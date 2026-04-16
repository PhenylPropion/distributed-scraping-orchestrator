# src/const/config.py
"""
dso_system 共通設定値

全モジュールが参照する設定値を一元管理する。
環境変数による上書きが可能。
"""

import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path

# PostgreSQL 接続 URL
# docker-compose 環境: postgresql://dso_system:dso_system@database:5432/dso_system
DEFAULT_DB_URL = os.environ.get(
    "dso_system_DB_URL",
    "postgresql://dso_system:dso_system@database:5432/dso_system",
)

# ログディレクトリ（プロジェクトルート /app/logs、または環境変数で上書き可能）
_LOG_DIR = Path(os.environ.get("dso_system_LOG_DIR", "/app/logs"))


def setup_logging(level: int | None = None) -> None:
    """
    アプリケーション全体のロギングを初期化する。

    設定内容:
        - コンソール (stderr): INFO 以上を出力
        - ファイル (logs/dso_system_YYYYMMDD.log): INFO 以上を出力
          - 日次ローテーション（00:00 に切り替え）
          - 7世代分保持
        - フォーマット: %(asctime)s [%(levelname)s] %(name)s - %(message)s

    呼び出し方:
        # bin/run_flow.py や bin/init_db.py の冒頭で一度だけ呼ぶ
        from src.const.config import setup_logging
        setup_logging()

    Note:
        Prefect の get_run_logger() は Prefect 内部のロガーを使うため、
        このセットアップとは独立して動作する。コンソール出力は両方から出る。
    """
    if level is None:
        level_str = os.environ.get("dso_system_LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_str, logging.INFO)

    _LOG_DIR.mkdir(parents=True, exist_ok=True)

    log_filename = _LOG_DIR / f"dso_system_{datetime.now().strftime('%Y%m%d')}.log"

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --- コンソールハンドラ ---
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(fmt)

    # --- ファイルハンドラ（日次ローテーション、7日分保持）---
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_filename,
        when="midnight",
        backupCount=7,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)  # ファイルには常に全レベル記録
    file_handler.setFormatter(fmt)

    # ルートロガーに設定（全モジュールに伝播）
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)  # ルートは DEBUG（各ハンドラがフィルタ）

    # 重複追加を防ぐ（すでに同じ型のハンドラがないかチェック）
    has_console = any(type(h) is logging.StreamHandler for h in root_logger.handlers)
    has_file = any(isinstance(h, logging.handlers.TimedRotatingFileHandler) for h in root_logger.handlers)

    if not has_console:
        root_logger.addHandler(console_handler)
    if not has_file:
        root_logger.addHandler(file_handler)
