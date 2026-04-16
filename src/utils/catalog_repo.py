"""
catalog_repo.py — データカタログのリポジトリ（データベース操作層）

責務:
    - PostgreSQL へのデータ永続化 (UPSERT)
    - 実行ステータスの更新
    - 全カタログデータの取得（Dataclass として返却）

設計:
    - SRP (単一責任): データベース操作のみを担当
    - KISS (単純性): 複雑なタプルではなく Dataclass を使って型安全にデータをやり取りする
"""

import json
import logging
from dataclasses import dataclass
from datetime import datetime

import psycopg2
from psycopg2.extras import Json

from src.const.config import DEFAULT_DB_URL

logger = logging.getLogger(__name__)


@dataclass
class CatalogRecord:
    """各サイトのデータカタログ情報を保持するデータクラス"""
    site_name: str
    site_name_ja: str
    columns: list[str]
    extra_columns: list[str]
    item_count: int
    last_run_at: datetime | None
    last_run_status: str


def upsert_to_db(
    site_id: str,
    site_name_ja: str,
    columns: list[str],
    extra_columns: list[str],
    item_count: int,
    db_url: str = None,
    status: str = "completed",
) -> None:
    """
    data_catalog テーブルにサイトのカラム情報を UPSERT する。
    """
    db_url = db_url or DEFAULT_DB_URL

    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            if columns:
                cur.execute(
                    """
                    INSERT INTO data_catalog
                        (site_name, site_name_ja, columns, extra_columns, item_count, last_run_at, updated_at, last_run_status)
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW(), %s)
                    ON CONFLICT (site_name)
                    DO UPDATE SET site_name_ja     = EXCLUDED.site_name_ja,
                                  columns          = EXCLUDED.columns,
                                  extra_columns    = EXCLUDED.extra_columns,
                                  item_count       = EXCLUDED.item_count,
                                  last_run_at      = NOW(),
                                  updated_at       = NOW(),
                                  last_run_status  = EXCLUDED.last_run_status;
                    """,
                    (site_id, site_name_ja, Json(columns), Json(extra_columns), item_count, status),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO data_catalog
                        (site_name, site_name_ja, columns, extra_columns, item_count, last_run_at, updated_at, last_run_status)
                    VALUES (%s, %s, '[]'::JSONB, '[]'::JSONB, %s, NOW(), NOW(), %s)
                    ON CONFLICT (site_name)
                    DO UPDATE SET site_name_ja    = EXCLUDED.site_name_ja,
                                  item_count      = EXCLUDED.item_count,
                                  last_run_at     = NOW(),
                                  last_run_status = EXCLUDED.last_run_status;
                    """,
                    (site_id, site_name_ja, item_count, status),
                )
        conn.commit()
        logger.info(
            "DB UPSERT 完了: site_id=%s (%s), columns=%d件, extra=%d件, item_count=%d",
            site_id, site_name_ja, len(columns), len(extra_columns), item_count,
        )
    finally:
        conn.close()


def set_run_status(
    site_id: str,
    status: str,
    db_url: str = None,
) -> None:
    """
    data_catalog の last_run_status だけを更新する。
    """
    db_url = db_url or DEFAULT_DB_URL
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO data_catalog (site_name, last_run_status, last_run_at, updated_at)
                VALUES (%s, %s, NOW(), NOW())
                ON CONFLICT (site_name)
                DO UPDATE SET last_run_status = EXCLUDED.last_run_status,
                              updated_at      = NOW();
                """,
                (site_id, status),
            )
        conn.commit()
        logger.debug("ステータス更新: site_id=%s → %s", site_id, status)
    finally:
        conn.close()


def fetch_all_catalog(db_url: str = None) -> list[CatalogRecord]:
    """
    data_catalog テーブルから全レコードを取得する。

    Returns:
        [CatalogRecord, ...] のリスト (明示的で安全なデータ構造)
    """
    db_url = db_url or DEFAULT_DB_URL
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT site_name, site_name_ja, columns, extra_columns, item_count, last_run_at, last_run_status "
                "FROM data_catalog ORDER BY site_name_ja;"
            )
            result = []
            for row in cur.fetchall():
                site_name = row[0]
                site_name_ja = row[1] or row[0]
                columns = _parse_jsonb(row[2], site_name, "columns")
                extra_cols = _parse_jsonb(row[3], site_name, "extra_columns")
                
                record = CatalogRecord(
                    site_name=site_name,
                    site_name_ja=site_name_ja,
                    columns=columns,
                    extra_columns=extra_cols,
                    item_count=row[4],
                    last_run_at=row[5],
                    last_run_status=row[6],
                )
                result.append(record)
            return result
    finally:
        conn.close()


def _parse_jsonb(raw, site_name: str, field_name: str) -> list[str]:
    """JSONB 型安全チェック: 文字列で返された場合のフォールバック"""
    if isinstance(raw, list):
        return raw
    elif isinstance(raw, str):
        return json.loads(raw)
    else:
        logger.warning(
            "予期しない %s 型: %s (site=%s)", field_name, type(raw), site_name
        )
        return []
