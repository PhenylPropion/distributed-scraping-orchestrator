#!/usr/bin/env python3
"""
データベース初期セットアップスクリプト

data_catalog テーブルなど、アプリケーション実行前に必要な
DDL を 1 回だけ実行する。

使い方:
    # ローカル
    python bin/init_db.py

    # Docker 環境
    docker compose exec worker python bin/init_db.py

    # カスタム接続先
    dso_system_DB_URL=postgresql://user:pass@host:5432/db python bin/init_db.py

注意:
    - CREATE TABLE IF NOT EXISTS を使用しているため、
      既にテーブルが存在していても安全に再実行可能。
    - ただし並行実行は避けること（PostgreSQL の DDL 競合リスク回避）。
"""

import logging
import sys
from pathlib import Path

import psycopg2

# プロジェクトルートを sys.path に追加
root_path = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_path))

from src.const.config import DEFAULT_DB_URL, setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# DEFAULT_DB_URL は src.const.config から取得済み

# ==========================================================================
# DDL 定義（最新スキーマ）
# 新規インストール時はこの DDL だけで全カラムが揃う。
# ==========================================================================

_TABLES = [
    # data_catalog: データカタログ (サイト × 取得カラム)
    """\
    CREATE TABLE IF NOT EXISTS data_catalog (
        site_name      TEXT PRIMARY KEY,
        site_name_ja   TEXT NOT NULL DEFAULT '',
        columns        JSONB NOT NULL DEFAULT '[]'::JSONB,
        extra_columns  JSONB NOT NULL DEFAULT '[]'::JSONB,
        item_count     INTEGER NOT NULL DEFAULT 0,
        last_run_at    TIMESTAMPTZ DEFAULT NOW(),
        updated_at     TIMESTAMPTZ DEFAULT NOW(),
        last_run_status TEXT NOT NULL DEFAULT 'unknown'
    );
    """,
]

# ==========================================================================
# マイグレーション定義（既存テーブルへの安全なカラム追加）
#
# 既に稼働中の環境を壊さないよう、IF NOT EXISTS パターンで冪等性を保証。
# 上の DDL に新カラムを追加した場合は、ここにもマイグレーションを追記すること。
#
# ヘルパー関数 _add_column_migration() で簡潔に記述する。
# ==========================================================================


def _add_column_migration(column_name: str, column_def: str) -> str:
    """カラム追加マイグレーション SQL を生成する（冪等）。"""
    return f"""\
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'data_catalog' AND column_name = '{column_name}'
        ) THEN
            ALTER TABLE data_catalog ADD COLUMN {column_name} {column_def};
        END IF;
    END $$;
    """


_MIGRATIONS = [
    # v0.2: extra_columns, item_count, last_run_at
    _add_column_migration("extra_columns", "JSONB NOT NULL DEFAULT '[]'::JSONB"),
    _add_column_migration("item_count", "INTEGER NOT NULL DEFAULT 0"),
    _add_column_migration("last_run_at", "TIMESTAMPTZ DEFAULT NOW()"),
    # v0.3: site_name_ja日本語表示名）
    _add_column_migration("site_name_ja", "TEXT NOT NULL DEFAULT ''"),
    # v0.4: last_run_status（実行ステータス）
    _add_column_migration("last_run_status", "TEXT NOT NULL DEFAULT 'unknown'"),
]


def init_db(db_url: str = None) -> None:
    """全テーブルを作成し、マイグレーションを実行する。既存テーブルはスキップされる。"""
    db_url = db_url or DEFAULT_DB_URL
    logger.info("DB 接続: %s", db_url.split("@")[-1])  # パスワードを隠す

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            for ddl in _TABLES:
                cur.execute(ddl)
            logger.info("テーブル作成完了 (%d テーブル)", len(_TABLES))

            for migration in _MIGRATIONS:
                cur.execute(migration)
            logger.info("マイグレーション完了 (%d 件)", len(_MIGRATIONS))

        conn.commit()
        logger.info("✅ データベース初期化完了")
    except Exception as e:
        conn.rollback()
        logger.error("❌ データベース初期化失敗: %s", e)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        init_db()
    except Exception as e:
        logger.error(f"致命的エラー: {e}")
        sys.exit(1)
