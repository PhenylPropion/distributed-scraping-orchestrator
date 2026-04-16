#!/usr/bin/env python3
"""
deploy.py — scripts/sites.yml から Prefect Deployment を一括登録する

本番環境（Docker）でのみ使用する。開発環境では不要。

使い方:
    # Docker 環境で実行
    docker compose exec worker python bin/deploy.py

    # ドライラン（登録せずに内容を確認）
    docker compose exec worker python bin/deploy.py --dry-run

動作:
    1. scripts/sites.yml を読み込む
    2. enabled: true のサイトのみ Deployment を登録
    3. dso_system_ENV=prod → scraping-pool、dev → dev-pool に登録
    4. schedule が設定されていればスケジュールも自動設定
"""

import argparse
import asyncio
import os
import sys
from pathlib import Path

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterState,
    FlowRunFilterStateType,
)
from prefect.client.schemas.schedules import CronSchedule
from prefect.runner.storage import LocalStorage

root_path = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_path))

from src.utils.sites_config import load_sites

# run_flow.py, retry_and_cleanup.py の Flow 関数をインポート
from bin.run_flow import scrape_site_flow
from bin.retry_and_cleanup import maintenance_flow

def deploy_all(dry_run: bool = False):
    """全サイトの Deployment を一括登録する"""
    env = os.environ.get("dso_system_ENV", "dev")
    pool_name = "scraping-pool" if env == "prod" else "dev-pool"

    sites = load_sites(only_enabled=True)
    print(f"📋 {len(sites)} サイトを {pool_name} に登録します (env={env})")
    print("=" * 60)

    # ローカルマウントのコードを参照する形でデプロイ
    # Worker コンテナ内では /app にプロジェクトがマウントされている
    print("💻 ローカルマウントモードでデプロイします")
    scrape_source = scrape_site_flow.from_source(
        source=LocalStorage(path=str(root_path)),
        entrypoint="bin/run_flow.py:scrape_site_flow",
    )
    maintenance_source = maintenance_flow.from_source(
        source=LocalStorage(path=str(root_path)),
        entrypoint="bin/retry_and_cleanup.py:maintenance_flow",
    )

    deployments = []

    for site in sites:
        site_id = site["site_id"]
        name = site["name"]
        module = site["module"]
        url = site["url"]
        cron = site.get("schedule")

        deploy_name = f"{name} ({site_id})-{env}"
        print(f"\n🔧 {deploy_name}")
        print(f"   module: {module}")
        print(f"   url:    {url}")
        print(f"   pool:   {pool_name}")
        print(f"   cron:   {cron or '(手動実行のみ)'}")

        if dry_run:
            print("   → ドライラン: スキップ")
            continue

        schedules = []
        if cron:
            schedules.append(
                # AND semantics let us express "nth weekend of the month"
                # by combining day-of-month ranges with day-of-week.
                CronSchedule(cron=cron, timezone="Asia/Tokyo", day_or=False)
            )

        # Prefect Deployment 用の定義を生成してリストに追加
        deployments.append(
            scrape_source.to_deployment(
                name=deploy_name,
                work_pool_name=pool_name,
                parameters={
                    "site_id": site_id,
                    "site_name_ja": name,
                    "module_path": module,
                    "url": url,
                },
                schedules=schedules,
            )
        )

    # 管理用（再送とクリーンアップ）の Flow も登録
    maintenance_deploy_name = f"maintenance-{env}"
    print(f"\n🔧 {maintenance_deploy_name}: 定期メインテナンス (S3再送・クリーンアップ)")
    if dry_run:
        print("   → ドライラン: スキップ")
    else:
        deployments.append(
            maintenance_source.to_deployment(
                name=maintenance_deploy_name,
                work_pool_name=pool_name,
                schedules=[CronSchedule(cron="0 5 * * *", timezone="Asia/Tokyo", day_or=False)], # 毎日朝5時に実行
            )
        )

    if not dry_run and deployments:
        # デプロイ前に既存のデプロイメント名を取得（初回登録の判定に使用）
        try:
            existing_names: set[str] = asyncio.run(_get_existing_deployment_names())
        except Exception:
            existing_names = set()

        # 各デプロイメントを個別に Prefect API へ登録
        # (global deploy() はDocker/リモートストレージ前提のため apply() を使用)
        registered_names: set[str] = set()
        for d in deployments:
            d.apply()
            registered_names.add(d.name)
        print("\n" + "=" * 60)
        print(f"✅ 全 {len(registered_names)} 件の Deployment 登録完了")

        # 初回登録のサイトをワークプールに即時投入する
        new_names = {
            n for n in registered_names
            if n not in existing_names and not n.startswith("maintenance-")
        }
        if new_names:
            print(f"\n🚀 初回登録サイト ({len(new_names)} 件) をワークプールに投入中...")
            try:
                triggered = asyncio.run(_trigger_new_deployments(new_names))
                print(f"✅ {triggered} 件のフローランを作成しました")
            except Exception as e:
                print(f"⚠️  フローランの作成に失敗しました（登録自体は完了済み）: {e}")

        # sites.yml から削除・無効化されたデプロイメントを Prefect から削除する
        print("\n🧹 古い Deployment を削除中...")
        try:
            deleted = asyncio.run(_cleanup_stale_deployments(env, registered_names))
            if deleted:
                print(f"🗑  {deleted} 件の古い Deployment を削除しました")
            else:
                print("   削除対象なし")
        except Exception as e:
            print(f"⚠️  古い Deployment の削除に失敗しました（登録自体は完了済み）: {e}")


async def _get_existing_deployment_names() -> set[str]:
    """Prefect 上に現在登録されているデプロイメント名の一覧を返す"""
    async with get_client() as client:
        deployments = await client.read_deployments()
        return {dep.name for dep in deployments if dep.name}


async def _trigger_new_deployments(new_names: set[str]) -> int:
    """
    初回登録されたデプロイメントのフローランを即時作成してワークプールに投入する。
    """
    async with get_client() as client:
        all_deployments = await client.read_deployments()
        triggered = 0
        for dep in all_deployments:
            if dep.name in new_names:
                await client.create_flow_run_from_deployment(dep.id)
                print(f"   🚀 投入: {dep.name}")
                triggered += 1
        return triggered


async def _cleanup_stale_deployments(env: str, registered_names: set[str]) -> int:
    """
    Prefect 上に残っている古い Deployment を削除する。

    今回の deploy_all() で登録されなかった、同じ env のデプロイメントを
    Prefect API から取得して削除する。
    """
    async with get_client() as client:
        all_deployments = await client.read_deployments()
        deleted = 0
        for dep in all_deployments:
            # このenvのデプロイメントかつ今回登録されていないもの
            if dep.name and dep.name.endswith(f"-{env}") and dep.name not in registered_names:
                print(f"   🗑  削除: {dep.name}")
                await client.delete_deployment(dep.id)
                deleted += 1
        return deleted


async def _rename_scheduled_runs() -> int:
    """
    Scheduled 状態のフローランにデプロイメントのパラメータから日本語名を付与する。

    Prefect の仕様上、flow_run_name テンプレートはフロー実行開始時に解決されるため、
    スケジュール登録時点ではランダム名（例: capable-swan）が割り当てられる。
    この関数は Prefect API を使って Scheduled ランの名前を事後的に更新する。
    """
    async with get_client() as client:
        flow_runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(
                    type=FlowRunFilterStateType(any_=["SCHEDULED"]),
                ),
            ),
        )

        renamed = 0
        # デプロイメント情報のキャッシュ（同じデプロイメントを何度も取得しない）
        deployment_cache = {}

        for run in flow_runs:
            if run.deployment_id is None:
                continue

            # 既にリネーム済み（"/ scheduled" で終わる）ならスキップ
            if run.name and run.name.endswith("/ scheduled"):
                continue

            # デプロイメント情報を取得（キャッシュ利用）
            dep_id = str(run.deployment_id)
            if dep_id not in deployment_cache:
                deployment_cache[dep_id] = await client.read_deployment(run.deployment_id)
            deployment = deployment_cache[dep_id]
            params = deployment.parameters or {}

            # scrape_site_flow のスケジュールラン → 日本語名を付与
            site_name_ja = params.get("site_name_ja", "")
            site_id = params.get("site_id", "")
            if site_name_ja and site_id:
                new_name = f"{site_name_ja} ({site_id}) / scheduled"
            elif "maintenance" in (deployment.name or ""):
                new_name = "dso_system メンテナンス / scheduled"
            else:
                continue

            await client.set_flow_run_name(run.id, new_name)
            renamed += 1

        return renamed


def rename_scheduled_runs() -> int:
    """Scheduled フローランの名前を日本語化する（同期ラッパー）"""
    return asyncio.run(_rename_scheduled_runs())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="scripts/sites.yml から Prefect Deployment を一括登録する"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="登録せずに内容を確認するだけ",
    )
    args = parser.parse_args()
    deploy_all(dry_run=args.dry_run)

    # デプロイ後、スケジュール済みランのランダム名を日本語名に更新
    if not args.dry_run:
        print("\n📝 スケジュール済みフローランの名前を更新中...")
        try:
            count = rename_scheduled_runs()
            print(f"✅ {count} 件のスケジュール済みランの名前を更新しました")
        except Exception as e:
            print(f"⚠️  名前の更新に失敗しました（デプロイ自体は完了済み）: {e}")
