"""
catalog_presenter.py — データカタログのプレゼンテーション層

責務:
    - 取得したカタログデータ（CatalogRecord のリスト）を元に表示用ファイルを生成
    - 営業用 CSV のエクスポート
    - Prefect 用 Markdown の生成

設計:
    - SRP (単一責任): 表示/フォーマットの変換のみを担当
"""

import csv
import logging
import os
import tempfile
from pathlib import Path

from src.const.config import DEFAULT_DB_URL
from src.const.schema import Schema
from src.utils.catalog_repo import fetch_all_catalog

logger = logging.getLogger(__name__)

# Schema カラムの正式順序リスト（カタログ表示用）
_SCHEMA_COLUMNS = list(Schema.COLUMNS)


def export_matrix_csv(db_url: str = None, output_path: str = None) -> str:
    """
    data_catalog テーブルから全レコードを読み込み、
    サイト × カラムの ◯✕ マトリクスを CSV で出力する。
    """
    if output_path is None:
        root = Path(__file__).resolve().parent.parent.parent
        output_path = str(root / "output" / "data_catalog.csv")

    records = fetch_all_catalog(db_url)
    if not records:
        logger.warning("data_catalog にレコードがありません。CSV 出力をスキップします。")
        return output_path

    # アトミック書き込み: 一時ファイル → os.replace()
    output_dir = Path(output_path).parent
    output_dir.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=str(output_dir), suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            # ヘッダー: サイト名 + Schema カラム + EXTRA カラム + 取得件数 + 最終実行日時
            writer.writerow(
                ["サイト名"] + _SCHEMA_COLUMNS + ["EXTRA カラム", "取得件数", "最終実行日時"]
            )

            for rec in records:
                col_set = set(rec.columns)
                row = (
                    [rec.site_name_ja]
                    + ["◯" if c in col_set else "✕" for c in _SCHEMA_COLUMNS]
                    + [", ".join(rec.extra_columns) if rec.extra_columns else ""]
                    + [rec.item_count, rec.last_run_at]
                )
                writer.writerow(row)

        os.replace(tmp_path, output_path)
    except Exception:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    logger.info(
        "CSV マトリクス出力完了: %s (%dサイト × %dカラム)",
        output_path, len(records), len(_SCHEMA_COLUMNS),
    )
    return output_path


def generate_markdown_table(db_url: str = None) -> str:
    """
    Prefect Dashboard の create_markdown_artifact に渡す
    Markdown テーブル文字列を生成する。
    """
    records = fetch_all_catalog(db_url)
    if not records:
        return "## データカタログ\n\n_レコードなし_"

    lines = [
        "## 📋 データカタログ（取得カラムマトリクス）",
        "",
        "| サイト名 | " + " | ".join(_SCHEMA_COLUMNS) + " | EXTRA | 取得件数 | 最終実行 | ステータス |",
        "|---|" + "|".join(["---"] * len(_SCHEMA_COLUMNS)) + "|---|---:|---|---|",
    ]

    for rec in records:
        col_set = set(rec.columns)
        cells = ["◯" if c in col_set else "✕" for c in _SCHEMA_COLUMNS]
        extra_display = ", ".join(rec.extra_columns) if rec.extra_columns else "-"
        count_display = f"⚠️ {rec.item_count}" if rec.item_count == 0 else str(rec.item_count)
        run_display = str(rec.last_run_at)[:16] if rec.last_run_at else "-"
        status_emoji = {"running": "🔄", "completed": "✅", "failed": "❌"}.get(rec.last_run_status, "❓")
        lines.append(
            f"| {rec.site_name_ja} | " + " | ".join(cells)
            + f" | {extra_display} | {count_display} | {run_display} | {status_emoji} |"
        )

    lines.append("")
    lines.append(f"_全 {len(records)} サイト × {len(_SCHEMA_COLUMNS)} Schema カラム_")

    return "\n".join(lines)
