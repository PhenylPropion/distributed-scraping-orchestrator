"""
run_flow.py — Prefect Flow によるクローラー実行エントリーポイント
責務:
    - CLI 引数を受け取り、Prefect Flow を実行する
    - CLI 引数が不完全な場合、scripts/sites.yml から自動解決する
    - クローラーの実行、S3 へのアップロード、データカタログへの同期をタスクとして定義し、
    Flow でオーケストレーションする
    - クローラー実行結果を Microsoft Teams に通知する（成功/失敗/警告）

使い方:
    # CLI 直接実行（全パラメータ指定）
    python bin/run_flow.py --site-id eaidem --name イーアイデム --module jobs.eaidem --url "https://..."

    # CLI 簡易実行（scripts/sites.yml から自動解決）
    python bin/run_flow.py --site-id eaidem

    # Prefect Deployment から自動実行（deploy.py がパラメータを登録済み）

仕組み:
    1. site_id から scripts/sites.yml を参照し、name/module/url を自動解決（CLI省略時）
    2. module_path から scripts.sites.{module_path} を動的インポート
    3. Prefect の @task 内でクローラーを実行し、ダッシュボードで監視可能にする
    4. CSV ファイル名を site_name_ja (日本語) で出力
    5. データカタログには site_id (英語) を主キーとして保存
"""

import argparse
import importlib
import inspect
import os
import sys
import time
from pathlib import Path

from prefect import flow, task
from prefect.states import State, StateType
from prefect.artifacts import create_markdown_artifact
from prefect.logging import get_run_logger

# プロジェクトルートを sys.path に追加 (src パッケージのインポートを可能にする)
root_path = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_path))

from src.const.config import DEFAULT_DB_URL, setup_logging
from src.framework.base import BaseCrawler
from src.utils.catalog_repo import upsert_to_db, set_run_status
from src.utils.catalog_presenter import export_matrix_csv, generate_markdown_table
from src.utils.notify import notify_success, notify_failure, notify_warning
from src.utils.s3_uploader import upload_to_s3
from src.utils.git_sync import pull_scripts_repo_task
from src.utils.sites_config import get_site_config
from src.utils.scraper_loader import load_scraper_class

# ロギングをファイル + コンソールに初期化
setup_logging()


# _load_site_config() removed as it is now imported from src.utils.sites_config as get_site_config()==========================================================================
# Task: クローラーの実行
# ==========================================================================

@task(name="run_scraper", retries=0, task_run_name="{site_name_ja}")
def run_scraper_task(site_id: str, site_name_ja: str, module_path: str, url: str) -> dict:
    """
    指定されたサイトのクローラーを動的にインポートして実行する。

    Args:
        site_id:       サイトの英語ID (例: "eaidem")
        site_name_ja:  日本語表示名 (例: "イーアイデム") — CSV ファイル名に使用
        module_path:   モジュールパス (例: "jobs.eaidem" → scripts.sites.jobs.eaidem)
        url:           クロール対象の URL

    Returns:
        {"output_path": str, "observed_columns": list, "extra_columns": list, "item_count": int}
    """
    logger = get_run_logger()

    # 1. モジュールを動的ロードし、クローラークラスを取得 (専用ローダーに委譲)
    logger.debug("クローラーをロードします: %s", module_path)
    crawler_class = load_scraper_class(module_path)
    logger.debug("クローラー検出: %s (サイト名: %s)", crawler_class.__name__, site_name_ja)

    # 2. クローラークラスのインスタンス化
    scraper = crawler_class()
    # 日本語サイト名を設定 → setter が Pipeline にも自動反映
    if site_name_ja:
        scraper.site_name = site_name_ja
    scraper.site_id = site_id

    # 3. 実行
    scraper.execute(url)

    logger.info("出力ファイル: %s", scraper.output_filepath)
    logger.info("観測カラム: %s", scraper.observed_columns)
    logger.info("独自カラム: %s", scraper.extra_columns)
    logger.info("取得件数: %d", scraper.item_count)
    if scraper.error_count > 0:
        logger.warning("⚠ スキップされたエラー: %d 件", scraper.error_count)
    return {
        "output_path": scraper.output_filepath or "",
        "observed_columns": scraper.observed_columns,
        "extra_columns": scraper.extra_columns,
        "item_count": scraper.item_count,
        "error_count": scraper.error_count,
    }


# ==========================================================================
# Task: S3 ファイル転送
# ==========================================================================

@task(name="upload_to_s3", retries=2, retry_delay_seconds=10, task_run_name="S3: {site_id}")
def upload_to_s3_task(output_path: str, site_id: str, site_name_ja: str) -> str | None | State:
    """
    クローラーが生成した CSV ファイルを AWS S3 に転送する。
    """
    logger = get_run_logger()
    if not os.environ.get("S3_BUCKET_NAME"):
        logger.warning("S3_BUCKET_NAME が設定されていないため、S3転送タスクをスキップします。")
        return State(type=StateType.CANCELLED, message="S3_BUCKET_NAME が未設定です。")

    if not os.path.exists(output_path):
        logger.error("S3転送対象ファイルが存在しません: %s", output_path)
        return None
    try:
        s3_uri = upload_to_s3(output_path, site_name_ja=site_name_ja)
        
        if s3_uri:
            # 転送成功した場合、ファイルを uploaded フォルダに移動
            uploaded_dir = Path(output_path).parent / "uploaded"
            uploaded_dir.mkdir(parents=True, exist_ok=True)
            new_path = uploaded_dir / Path(output_path).name
            os.rename(output_path, new_path)
            logger.info("S3アップロード完了、ファイルを移動しました: %s", new_path)
            
        return s3_uri
    except Exception as e:
        logger.error("S3へのアップロード中にエラーが発生しました: %s", e)
        raise


# ==========================================================================
# Flow: オーケストレーション
# ==========================================================================

@flow(name="scrape_site_flow", flow_run_name="{site_name_ja} ({site_id}) / {worker_name}", log_prints=True)
def scrape_site_flow(
    site_id: str,
    site_name_ja: str = "",
    module_path: str = "",
    url: str = "",
    worker_name: str = os.getenv("WORKER_NAME", "localhost"),
) -> str:
    """
    クローラー実行の Prefect Flow。

    Args:
        site_id:       サイトの英語ID (必須)
        site_name_ja:  日本語表示名 (省略時は scripts/sites.yml から取得)
        module_path:   モジュールパス (省略時は scripts/sites.yml から取得)
        url:           クロール対象 URL (省略時は scripts/sites.yml から取得)
    """
    logger = get_run_logger()

    # scripts/sites.yml からの自動解決（パラメータ未指定時）
    if not site_name_ja or not module_path or not url:
        config = get_site_config(site_id)
        if config is None:
            raise ValueError(
                f"site_id '{site_id}' が scripts/sites.yml に見つかりません。\n"
                f"--name, --module, --url を全て指定するか、scripts/sites.yml にサイトを追加してください。"
            )
        site_name_ja = site_name_ja or config["name"]
        module_path = module_path or config["module"]
        url = url or config["url"]

    logger.info("===== Flow 開始: %s (%s) url=%s =====", site_name_ja, site_id, url)
    flow_start = time.monotonic()

    # DBに「実行中」を記録（Prefect Artifact で🔄 リアルタイム表示）
    try:
        set_run_status(site_id, "running")
    except Exception:
        pass  # DB 未接続などで失敗しても Flow は継続する

    try:
        # 0. Scripts リポジトリの最新化 (git pull)
        warnings = []  # スキップされたタスクの警告を収集
        git_state = pull_scripts_repo_task(root_path, return_state=True)
        if git_state.is_cancelled():
            warnings.append(f"Git Pull: {git_state.message}")

        # 1. クローラー実行
        result_state = run_scraper_task(site_id, site_name_ja, module_path, url, return_state=True)
        
        # 実行に失敗したか判定
        if result_state.is_failed():
            # エラーを発生させてFlowのexceptブロックに飛ばし、Teams通知させる
            raise result_state.result(raise_on_failure=False)
            
        result = result_state.result()
        output_path = result["output_path"]

        # スキップされたエラーがあれば警告に追加
        error_count = result.get("error_count", 0)
        if error_count > 0:
            warnings.append(f"スクレイピング中に {error_count} 件のエラーをスキップしました")

        # 2. S3 ファイル転送（出力ファイルがある場合のみ）
        if output_path:
            s3_state = upload_to_s3_task(output_path, site_id, site_name_ja, return_state=True)
            if s3_state.is_cancelled():
                warnings.append(f"S3: {s3_state.message}")
        else:
            logger.warning("出力ファイルなし: S3転送をスキップ")

        # 3. データカタログ同期（site_id をキーとして保存）
        sync_data_catalog_task(
            site_id,
            site_name_ja,
            result["observed_columns"],
            result["extra_columns"],
            result["item_count"],
            status="completed",
        )

        logger.info("===== Flow 完了: %s =====", output_path)

        # 4. Teams 通知（成功 / ✅ スキップエラーあり / ⚠️ 警告あり / ❌ 0件）
        elapsed = time.monotonic() - flow_start
        item_count = result["item_count"]

        if item_count == 0:
            # 取得件数0件 → サイト構造変更の可能性。失敗フォーマットで通知
            notify_failure(
                site_id=site_id,
                site_name=site_name_ja,
                error=ValueError(),
                elapsed_seconds=elapsed,
                is_zero_items=True,
            )
        elif warnings:
            notify_warning(
                site_id=site_id,
                site_name=site_name_ja,
                warnings=warnings,
                item_count=item_count,
                output_path=output_path or "",
                elapsed_seconds=elapsed,
                error_count=error_count,
            )
        else:
            notify_success(
                site_id=site_id,
                site_name=site_name_ja,
                item_count=item_count,
                output_path=output_path or "",
                elapsed_seconds=elapsed,
            )

        # 5. フロー状態を返却（警告・0件があれば Runs 一覧で識別可能に）
        if item_count == 0:
            return State(
                type=StateType.COMPLETED,
                name="Completed ⚠️",
                message="取得件数が0件でした。コードの再実装または放棄を検討してください。",
            )
        if warnings:
            return State(
                type=StateType.COMPLETED,
                name="Completed ⚠️",
                message=f"スキップされたタスク: {', '.join(warnings)}",
            )
        return output_path

    except BaseException as e:
        """BaseException をキャッチする理由:
        Pythonでは、通常のプログラムエラー（バグやネットワークエラーなど）は Exception クラスを継承して発生しますが、
        Ctrl+C で強制終了した際に発生する例外である KeyboardInterrupt&システム終了 SystemExitは
        Exception クラスではなく、さらに根本の BaseException クラスを継承しています。
        """
        # DBに「失敗」を記録
        try:
            set_run_status(site_id, "failed")
        except Exception:
            pass
        # Teams 通知（失敗）
        elapsed = time.monotonic() - flow_start
        notify_failure(
            site_id=site_id,
            site_name=site_name_ja,
            error=e,
            elapsed_seconds=elapsed,
        )
        raise  # Prefect にも失敗を伝える（ダッシュボード上 Failed 表示のため）


# ==========================================================================
# Task: データカタログ同期
# ==========================================================================

@task(name="sync_data_catalog", task_run_name="catalog: {site_id}")
def sync_data_catalog_task(
    site_id: str,
    site_name_ja: str,
    columns: list[str],
    extra_columns: list[str],
    item_count: int,
    status: str = "completed",
) -> None:
    """
    観測されたカラム情報をデータカタログに同期する。

    同期先 (同時更新):
        ① PostgreSQL (SSOT マスターデータ) — site_id を主キー
        ② CSV マトリクス (output/data_catalog.csv)
        ③ Prefect Dashboard (Markdown Artifact)
    """
    logger = get_run_logger()
    db_url = DEFAULT_DB_URL
    output_csv = str(root_path / "output" / "data_catalog.csv")

    # ① PostgreSQL UPSERT (site_id をキーに保存)
    logger.info("カタログ同期開始: site_id=%s, サイト名=%s, カラム=%s, 取得件数=%d",
                site_id, site_name_ja, columns, item_count)
    upsert_to_db(site_id, site_name_ja, columns, extra_columns, item_count, db_url, status=status)

    # ② CSV マトリクス出力
    export_matrix_csv(db_url, output_csv)

    # ③ Prefect Dashboard 可視化
    md = generate_markdown_table(db_url)
    create_markdown_artifact(md, key="data-catalog", description="サイト × カラム マトリクス")

    logger.info("カタログ同期完了")


# ==========================================================================
# CLI エントリーポイント
# ==========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Prefect Flow でクローラーを実行する"
    )
    parser.add_argument(
        "--site-id",
        required=True,
        help="サイトID (scripts/sites.yml の site_id。例: eaidem)",
    )
    parser.add_argument(
        "--name",
        default="",
        help="日本語サイト名 (省略時は scripts/sites.yml から取得)",
    )
    parser.add_argument(
        "--module",
        default="",
        help="モジュールパス (省略時は scripts/sites.yml から取得。例: jobs.eaidem)",
    )
    parser.add_argument(
        "--url",
        default="",
        help="クロール対象の URL (省略時は scripts/sites.yml から取得)",
    )

    args = parser.parse_args()

    # Flow の自動解決に委ねる。
    # ただし flow_run_name の表示のため、CLI 側でも scripts/sites.yml を解決する。
    site_name_ja = args.name
    module_path = args.module
    url = args.url

    if not all([site_name_ja, module_path, url]):
        config = get_site_config(args.site_id)
        if config is None:
            print(f"Error: site_id '{args.site_id}' が scripts/sites.yml に見つかりません。", file=sys.stderr)
            sys.exit(1)
        site_name_ja = site_name_ja or config.get("name", "")
        module_path = module_path or config.get("module", "")
        url = url or config.get("url", "")

    scrape_site_flow(
        site_id=args.site_id,
        site_name_ja=site_name_ja,
        module_path=module_path,
        url=url,
    )
