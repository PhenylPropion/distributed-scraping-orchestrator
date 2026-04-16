#!/usr/bin/env python3
"""
trigger.py — Prefect デプロイメントへの手動投入スクリプト

開発PCから本番（または開発）の Prefect Worker に、
特定のサイトのスクレイピングジョブを手動で投入するためのツール。

【run_flow.py との違い】
    run_flow.py  → このマシン上でFlowを直接実行する（Workerを使わない）
    trigger.py   → Prefect の Work Pool に投入し、Worker に実行させる

【事前条件】
    1. 対象環境で Docker 起動済み（Prefect Server + Worker）
    2. 対象Workerで deploy.py を実行済み（デプロイメント登録済み）

【使い方】
    # 1サイト指定
    python bin/trigger.py eaidem

    # 複数サイト指定
    python bin/trigger.py eaidem shinq_compass boxil

    # sites.yml の全 enabled サイトを一括投入
    python bin/trigger.py --all

    # 環境を明示指定（デフォルトは dso_system_ENV 環境変数、未設定なら dev）
    python bin/trigger.py eaidem --env prod
    python bin/trigger.py --all --env prod

    # 完了まで待機する（デフォルトは fire-and-forget）
    python bin/trigger.py eaidem --wait

【環境変数】
    PREFECT_API_URL  接続する Prefect サーバー（例: http://192.168.1.10:4200/api）
    dso_system_ENV   dev または prod（デプロイメント名のサフィックスに使用）

【本番サーバーに向ける場合の例】
    export PREFECT_API_URL=http://本番サーバーIP:4200/api
    export dso_system_ENV=prod
    python bin/trigger.py --all
"""

import argparse
import os
import sys
from pathlib import Path

# プロジェクトルートを sys.path に追加
root_path = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_path))

from prefect.deployments import run_deployment
from prefect.exceptions import PrefectHTTPStatusError

from src.utils.sites_config import load_sites, get_site_config


def trigger(site_ids: list[str], env: str, wait: bool) -> None:
    api_url = os.environ.get("PREFECT_API_URL", "http://localhost:4200/api")
    pool = "scraping-pool" if env == "prod" else "dev-pool"

    print(f"Prefect API : {api_url}")
    print(f"環境        : {env}  |  プール: {pool}")
    print()

    success = 0
    failed = 0

    for site_id in site_ids:
        # sites.yml に存在するか確認
        config = get_site_config(site_id)
        if config is None:
            print(f"⚠️  スキップ: '{site_id}' は sites.yml に存在しません")
            failed += 1
            continue

        name_ja = config.get("name", site_id)
        deployment_name = f"scrape_site_flow/{name_ja} ({site_id})-{env}"

        try:
            timeout = None if wait else 0  # None = 完了まで待機, 0 = fire-and-forget
            flow_run = run_deployment(name=deployment_name, timeout=timeout)

            run_id = flow_run.id if flow_run else "不明"
            # Prefect UI の Flow Run URL を構築
            # PREFECT_API_URL = http://host:port/api → UI は http://host:port
            ui_base = api_url.rstrip("/").removesuffix("/api")
            run_url = f"{ui_base}/flow-runs/flow-run/{run_id}"

            status = "完了" if wait else "投入完了"
            print(f"✅ {status}: {name_ja} ({site_id})-{env}")
            print(f"   → {run_url}")
            success += 1

        except PrefectHTTPStatusError as e:
            if "404" in str(e):
                print(f"❌ 失敗: '{deployment_name}' がPrefectに未登録です")
                print(f"   → 先に `docker compose exec worker python bin/deploy.py` を実行してください")
            else:
                print(f"❌ 失敗: {site_id} — {e}")
            failed += 1

        except Exception as e:
            err_msg = str(e) if str(e) and str(e) != "None" else type(e).__name__
            print(f"❌ 失敗: {site_id} — {err_msg}")
            print(f"   デプロイメント名: {deployment_name}")
            print(f"   → `docker compose exec worker python bin/deploy.py` で再登録してください")
            failed += 1

    print()
    print(f"{'=' * 50}")
    print(f"投入: {success}件  失敗/スキップ: {failed}件")
    if not wait and success > 0:
        ui_base = api_url.rstrip("/").removesuffix("/api")
        print(f"Prefect UI で進捗を確認: {ui_base}/flow-runs")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prefect デプロイメントに手動でジョブを投入する",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  python bin/trigger.py eaidem
  python bin/trigger.py eaidem shinq_compass --env prod
  python bin/trigger.py --all
  python bin/trigger.py --all --env prod --wait
        """,
    )
    parser.add_argument(
        "site_ids",
        nargs="*",
        help="投入するサイトのID（複数指定可）。--all と併用不可",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="sites.yml の全 enabled サイトを投入する",
    )
    parser.add_argument(
        "--env",
        default=os.environ.get("dso_system_ENV", "dev"),
        choices=["dev", "prod"],
        help="対象環境 (default: dso_system_ENV 環境変数、未設定なら dev)",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="全ジョブの完了まで待機する（デフォルトは投入のみで終了）",
    )

    args = parser.parse_args()

    # 引数バリデーション
    if args.all and args.site_ids:
        parser.error("--all と site_ids は同時に指定できません")

    if not args.all and not args.site_ids:
        parser.error("site_ids か --all のどちらかを指定してください")

    # 対象サイトを決定
    if args.all:
        sites = load_sites(only_enabled=True)
        site_ids = [s["site_id"] for s in sites]
        print(f"全 {len(site_ids)} サイトを投入します: {', '.join(site_ids)}")
        print()
    else:
        site_ids = args.site_ids

    trigger(site_ids, env=args.env, wait=args.wait)


if __name__ == "__main__":
    main()
