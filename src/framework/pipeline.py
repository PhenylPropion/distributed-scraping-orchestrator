# src/framework/pipeline.py
"""
データ処理パイプライン

責務:
    - Schema に定義されたカラム名のみを許可するバリデーション
    - normalizer.py に正規化を委譲する
    - ローカル CSV ファイルへの書き込み

設計:
    - ストリーミング + ヘッダー後決定のハイブリッド方式
    - process_item() でアイテムをヘッダーなし一時 CSV にストリーミング書き込み
    - close() で観測されたカラムだけを抽出し、最終 CSV を生成
    - 100万行超でもメモリを圧迫しない（ファイルベースストリーミング）
    - JSONL 方式と比べ、キー名の繰り返しがなくファイルサイズが小さい
    - CSV パーサは JSON パーサより高速
"""

import csv
import logging
import os
import tempfile
from datetime import datetime
from pathlib import Path

from src.const.schema import Schema
from src.utils.normalizer import normalize, NormalizationDropWarning

logger = logging.getLogger(__name__)

# Schema クラスから全カラム名を自動収集（frozenset = 不変のセット）
_ALLOWED_COLUMNS = frozenset(Schema.COLUMNS)


class ItemPipeline:
    """
    parse() が yield した辞書データを受け取り、
    バリデーション → 正規化 → CSV 保存 する処理パイプライン。

    CSV ヘッダーの決定方式（ハイブリッド方式）:
        1. process_item(): 全カラム固定順でヘッダーなし一時 CSV に書き出し
        2. close(): 観測されたカラムだけを抽出し、最終 CSV を生成
        - 100万行超でもメモリ消費は一定（ファイルベースストリーミング）
    """

    def __init__(self, output_dir: Path, site_name: str, extra_columns: list = None, site_id: str = ""):
        """
        Args:
            output_dir:      CSV ファイルの出力先ディレクトリ
            site_name:       ファイル名に使用するサイト名 (例: "イーアイデム")
            extra_columns:   サイト固有のカスタムカラム名リスト。
                             Schema に定義されていない独自カラムを使用する場合に指定する。
            site_id:         サイトID（CSVファイル名に使用）
        """
        self._output_dir = output_dir
        self._site_name = site_name
        self._site_id = site_id

        # Schema カラム + クローラーが宣言したカスタムカラムの和集合
        self._extra_columns = list(extra_columns or [])
        self._allowed = _ALLOWED_COLUMNS | frozenset(self._extra_columns)

        # 全カラムの固定順序（一時 CSV の列順として使用）
        self._all_fieldnames = list(Schema.COLUMNS) + self._extra_columns

        # 実行中に観測されたカラム名を蓄積する（データカタログ用）
        self._observed_columns: set[str] = set()
        self._item_count: int = 0
        
        # 正規化によるドロップ（空文字変換）の記録: {カラム名: [警告メッセージのリスト]}
        self._drops: dict[str, list[str]] = {}

        # ストリーミング方式: ヘッダーなし一時 CSV にストリーミング書き出し
        self._tmp_fd, self._tmp_path = tempfile.mkstemp(
            suffix=".csv", prefix="pipeline_", dir=str(output_dir)
        )
        self._tmp_file = os.fdopen(self._tmp_fd, mode="w", encoding="utf-8", newline="")
        self._tmp_writer = csv.DictWriter(
            self._tmp_file,
            fieldnames=self._all_fieldnames,
            extrasaction="ignore",
        )
        # ヘッダーは書かない（close() で観測カラムだけのヘッダーを付ける）

        self.output_filepath = None

    # ==========================================================================
    # 公開メソッド
    # ==========================================================================

    def process_item(self, item: dict) -> dict:
        """
        1件のデータを検証 → 正規化 → 一時 CSV にストリーミング書き出し。

        Args:
            item: parse() から yield された辞書データ

        Returns:
            正規化済みの辞書データ

        Raises:
            ValueError: Schema に定義されていないカラム名が含まれている場合
        """
        # 0. 取得日時を自動挿入（開発者が手動で設定する必要はない）
        item[Schema.GET_TIME] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 1. Validation & Normalization
        clean_item = self._validate_and_normalize(item)

        # 2. カラム名を記録 + 件数カウント（データカタログ用）
        self._observed_columns.update(clean_item.keys())
        self._item_count += 1

        # 3. 一時 CSV にストリーミング書き出し（メモリ不使用）
        self._tmp_writer.writerow(clean_item)
        self._tmp_file.flush()  # 書き込みを即座にOSに反映（リアルタイム確認用）

        return clean_item

    @property
    def observed_columns(self) -> list[str]:
        """実行中に実際に観測されたカラム名のソート済みリスト。"""
        return sorted(self._observed_columns)

    @property
    def item_count(self) -> int:
        """処理したアイテムの総件数。"""
        return self._item_count

    @property
    def extra_columns(self) -> list[str]:
        """クローラーが宣言した EXTRA_COLUMNS のリスト。"""
        return list(self._extra_columns)

    def close(self):
        """
        一時 CSV から観測されたカラムだけを抽出し、最終 CSV を生成する。

        処理フロー:
            1. 一時ファイルを閉じる
            2. 観測カラムの列インデックスを特定
            3. 一時 CSV を csv.reader で高速に読み、該当列だけを final CSV に書き出す
            4. 一時ファイルを削除
        """
        self._tmp_file.close()

        try:
            if self._item_count == 0:
                logger.info("データなし: CSV ファイルは作成されません")
                return

            # 観測カラムだけで、定義順に最終ヘッダーを構成
            observed = self._observed_columns
            final_fieldnames = [c for c in self._all_fieldnames if c in observed]

            # 観測カラムの列インデックスを特定（位置ベースで高速抽出）
            col_indices = [self._all_fieldnames.index(c) for c in final_fieldnames]

            timestamp_day = datetime.now().strftime("%Y%m%d")
            if self._site_id:
                filename = f"{timestamp_day}_{self._site_id}_{self._site_name}_{self._item_count}件.csv"
            else:
                filename = f"{timestamp_day}_{self._site_name}_{self._item_count}件.csv"
                
            self.output_filepath = str(self._output_dir / filename)

            with open(self.output_filepath, mode="w", encoding="utf-8-sig", newline="") as csv_out, \
                 open(self._tmp_path, mode="r", encoding="utf-8", newline="") as tmp_in:

                writer = csv.writer(csv_out)
                writer.writerow(final_fieldnames)

                reader = csv.reader(tmp_in)
                for row in reader:
                    writer.writerow([row[i] for i in col_indices])

            logger.info("CSV 作成: %s (%d件, %dカラム)",
                        self.output_filepath, self._item_count, len(final_fieldnames))
            
            # ドロップサマリの出力
            if self._drops:
                logger.warning("=== 正規化ドロップサマリ ===")
                for col, warnings in self._drops.items():
                    logger.warning("カラム '%s': %d 件のデータが無効としてドロップ(空文字化)されました。", col, len(warnings))
                    # 最大5件まで詳細を表示
                    for w in warnings[:5]:
                        logger.warning("  - %s", w)
                    if len(warnings) > 5:
                        logger.warning("  - 他 %d 件...", len(warnings) - 5)
                logger.warning("============================")
                
        finally:
            # 一時ファイルを確実に削除
            if os.path.exists(self._tmp_path):
                os.unlink(self._tmp_path)

    # ==========================================================================
    # 内部メソッド
    # ==========================================================================

    def _validate_and_normalize(self, item: dict) -> dict:
        """
        Schema 検証 & 正規化

        1. 辞書のキーが Schema または EXTRA_COLUMNS に定義されているか検証する
        2. 正規化を normalizer モジュールに委譲する
        """
        clean_item = {}
        for key, value in item.items():
            if key not in self._allowed:
                raise ValueError(f"禁止されたカラム名です: '{key}'")
            try:
                clean_item[key] = normalize(key, value)
            except NormalizationDropWarning as e:
                clean_item[key] = ""
                if key not in self._drops:
                    self._drops[key] = []
                self._drops[key].append(str(e))
                
        return clean_item

    def __del__(self):
        """GC による安全策: close() が呼ばれなかった場合の一時ファイル削除"""
        try:
            if hasattr(self, "_tmp_file") and not self._tmp_file.closed:
                self._tmp_file.close()
            if hasattr(self, "_tmp_path") and os.path.exists(self._tmp_path):
                os.unlink(self._tmp_path)
        except Exception:
            pass  # GC 中の例外は無視
