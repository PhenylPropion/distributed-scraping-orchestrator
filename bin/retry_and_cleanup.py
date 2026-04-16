"""
S3再送およびローカルCSVクリーンアップの定期実行スクリプト (bin/retry_and_cleanup.py)

概要:
    1. output/ 直下の未送信CSVを抽出し、ファイル名から site_id を推測して S3 へ再送を試みる。成功時は uploaded/ フォルダへ移動する。
    2. output/uploaded/ 内の古いCSVファイルを、.envで定義された CSV_RETENTION_DAYS を超過した場合に物理削除する。

実行方法:
    python bin/retry_and_cleanup.py
    または、Prefect Deployment 経由で定期実行
"""

import os
import re
import glob
import time
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict
from typing import List

from prefect import flow, task
from prefect.states import State, StateType
from prefect.logging import get_run_logger
from dotenv import load_dotenv

# プロジェクトルートパスを取得
_project_root = Path(__file__).resolve().parent.parent

# Prefect 以外の通常実行時に参照するライブラリ群
try:
    from src.utils.s3_uploader import upload_to_s3
    from src.utils.notify import send_notification
    from src.utils.git_sync import pull_scripts_repo_task
except ImportError:
    import sys
    sys.path.insert(0, str(_project_root))
    from src.utils.s3_uploader import upload_to_s3
    from src.utils.notify import send_notification
    from src.utils.git_sync import pull_scripts_repo_task


@task(name="retry_s3_uploads", task_run_name="S3 Retry Task")
def retry_s3_uploads_task(output_dir: Path) -> int | State:
    """
    output/ 配下で S3 に未送信のまま残っている CSV ファイルを全てアップロード試行する。
    """
    logger = get_run_logger()
    if not os.environ.get("S3_BUCKET_NAME"):
        logger.warning("S3_BUCKET_NAME が設定されていないため、再送タスクをスキップします。")
        return State(type=StateType.CANCELLED, message="S3_BUCKET_NAME が未設定です。")

    if not output_dir.exists():
        logger.warning("output ディレクトリが存在しないため、再送タスクをスキップします。")
        return State(type=StateType.CANCELLED, message="output ディレクトリが存在しません。")

    # 直下のCSVのみを対象とする
    candidate_files = [f for f in output_dir.glob("*.csv") if f.is_file()]
    
    if not candidate_files:
        logger.info("再送対象のCSVファイルはありませんでした。")
        return State(type=StateType.COMPLETED, message="再送対象のCSVファイルはありませんでした。")
        
    logger.info("%d件の未送信CSVファイルが見つかりました、再送を開始します。", len(candidate_files))
    
    success_count = 0
    uploaded_dir = output_dir / "uploaded"
    uploaded_dir.mkdir(parents=True, exist_ok=True)
    
    for filepath in candidate_files:
        # 新しい命名規則: {yyyymmdd}_{site_id}_{site_name_ja}_{count}件.csv
        # '_' で分割して2番目(index=1)を取得
        parts = filepath.name.split('_')
        if len(parts) >= 3:
            site_name_ja = parts[2]
        else:
            # 古い形式の場合は再送不可とする（サイト名が特定できないため）
            logger.warning("ファイル名からサイト名が特定できません: %s", filepath.name)
            continue

        logger.info("再送試行中: %s (推測サイト名: %s)", filepath.name, site_name_ja)
        try:
            # 再送
            s3_uri = upload_to_s3(str(filepath), site_name_ja=site_name_ja)
            if s3_uri:
                new_path = uploaded_dir / filepath.name
                os.rename(filepath, new_path)
                logger.info("再送成功、移動しました: %s -> %s", filepath.name, new_path)
                success_count += 1
        except Exception as e:
            logger.error("再送中にエラーが発生しました [%s]: %s", filepath.name, e)
            
    return success_count


@task(name="cleanup_local_files", task_run_name="Cleanup Local CSVs")
def cleanup_local_files_task(uploaded_dir: Path, retention_days: int) -> int:
    """
    output/uploaded/ ディレクトリ内のCSVファイルのうち、
    最終更新日時が retention_days を超過しているものを削除する。
    """
    logger = get_run_logger()
    
    if not uploaded_dir.exists():
        logger.info("uploaded ディレクトリが存在しません。クリーンアップをスキップします。")
        return 0
        
    candidate_files = [f for f in uploaded_dir.glob("*.csv") if f.is_file()]
    if not candidate_files:
        logger.info("クリーンアップ対象のCSVファイルはありませんでした。")
        return 0
        
    now = time.time()
    cutoff_time = now - (retention_days * 86400) # 86400秒 = 1日
    deleted_count = 0
    
    for filepath in candidate_files:
        file_mtime = os.path.getmtime(filepath)
        if file_mtime < cutoff_time:
            try:
                os.remove(filepath)
                logger.info("保持期間(%d日)を超過したため削除しました: %s", retention_days, filepath.name)
                deleted_count += 1
            except Exception as e:
                logger.error("ファイルの削除に失敗しました [%s]: %s", filepath.name, e)
                
    return deleted_count


@task(name="rename_scheduled_runs", task_run_name="Rename Scheduled Runs")
def rename_scheduled_runs_task() -> int:
    """
    Scheduled 状態のフローランにデプロイメントのパラメータから日本語名を付与する。

    Prefect の仕様上、flow_run_name テンプレートはフロー実行開始時に解決されるため、
    スケジュール登録時点ではランダム名（例: capable-swan）が割り当てられる。
    この関数は Prefect API を使って Scheduled ランの名前を事後的に更新する。
    """
    import asyncio
    from prefect.client.orchestration import get_client
    from prefect.client.schemas.filters import (
        FlowRunFilter,
        FlowRunFilterState,
        FlowRunFilterStateType,
    )

    logger = get_run_logger()

    async def _rename():
        async with get_client() as client:
            flow_runs = await client.read_flow_runs(
                flow_run_filter=FlowRunFilter(
                    state=FlowRunFilterState(
                        type=FlowRunFilterStateType(any_=["SCHEDULED"]),
                    ),
                ),
            )

            renamed = 0
            deployment_cache = {}

            for run in flow_runs:
                if run.deployment_id is None:
                    continue

                # 既にリネーム済み（"/ scheduled" で終わる）ならスキップ
                if run.name and run.name.endswith("/ scheduled"):
                    continue

                dep_id = str(run.deployment_id)
                if dep_id not in deployment_cache:
                    deployment_cache[dep_id] = await client.read_deployment(run.deployment_id)
                deployment = deployment_cache[dep_id]
                params = deployment.parameters or {}

                site_name_ja = params.get("site_name_ja", "")
                site_id = params.get("site_id", "")
                if site_name_ja and site_id:
                    new_name = f"{site_name_ja} ({site_id}) / scheduled"
                elif "maintenance" in (deployment.name or ""):
                    new_name = "dso_system メンテナンス / scheduled"
                else:
                    continue

                await client.set_flow_run_name(run.id, new_name)
                logger.debug("リネーム: %s → %s", run.name, new_name)
                renamed += 1

            return renamed

    return asyncio.run(_rename())


@flow(name="maintenance_flow", flow_run_name="dso_system Maintenance Default Flow", log_prints=True)
def maintenance_flow(retention_days: int = None):
    """
    定期メインテナンス用Flow
    1. S3再送タスク
    2. クリーンアップタスク
    """
    logger = get_run_logger()
    logger.info("定期メインテナンスフローを開始します")
    
    load_dotenv(override=True)
    
    if retention_days is None:
        try:
            retention_days = int(os.getenv("CSV_RETENTION_DAYS", "14"))
        except ValueError:
            retention_days = 14
            
    output_dir = _project_root / "output"
    uploaded_dir = output_dir / "uploaded"
    
    logger.info("保持期間 (CSV_RETENTION_DAYS): %d 日", retention_days)
    
    # 0. Scripts リポジトリの最新化 (git pull)
    pull_scripts_repo_task(_project_root, return_state=True)
    
    # 1. 未送信 CSV の S3 再送
    retry_result = retry_s3_uploads_task(output_dir, return_state=True)
    retry_count = retry_result.result() if retry_result.is_completed() else 0
    
    # 2. 古いファイルのクリーンアップ
    cleanup_count = cleanup_local_files_task(uploaded_dir, retention_days)
    logger.info("クリーンアップタスク完了: %d 件削除", cleanup_count)

    # 3. スケジュール済みフローランのランダム名を日本語名に更新
    rename_count = rename_scheduled_runs_task()
    logger.info("スケジュール済みラン名前更新: %d 件", rename_count)

    if retry_count > 0 or cleanup_count > 0:
        send_notification(
            title="🛠 定期メインテナンス (再送 / クリーンアップ) 完了",
            text=f"- S3再送成功件数: {retry_count} 件\n- ローカルファイル削除件数: {cleanup_count} 件\n\n正常に処理が完了しました。"
        )

if __name__ == "__main__":
    maintenance_flow()
