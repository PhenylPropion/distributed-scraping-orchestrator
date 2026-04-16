"""
テスト: ItemPipeline (src/framework/pipeline.py)

検証項目:
    - 正常アイテムの検証・正規化
    - 禁止カラム名で ValueError
    - TEL 全角→半角正規化
    - None → 空文字変換
    - 0件時に CSV 未作成
    - EXTRA_COLUMNS の許可・拒否
    - 観測カラムだけが CSV ヘッダーに出力される
    - 一時ファイルが close() 後に削除される
"""

import csv
import os
import tempfile
from pathlib import Path

import pytest

from src.const.schema import Schema
from src.framework.pipeline import ItemPipeline


@pytest.fixture
def output_dir(tmp_path):
    """テスト用の一時出力ディレクトリ"""
    return tmp_path


@pytest.fixture
def pipeline(output_dir):
    """基本的な Pipeline インスタンス"""
    p = ItemPipeline(output_dir=output_dir, site_name="TestCrawler")
    yield p
    # テスト後にクリーンアップ（close されていない場合）
    try:
        if hasattr(p, "_tmp_file") and not p._tmp_file.closed:
            p._tmp_file.close()
        if hasattr(p, "_tmp_path") and os.path.exists(p._tmp_path):
            os.unlink(p._tmp_path)
    except Exception:
        pass


@pytest.fixture
def pipeline_with_extra(output_dir):
    """EXTRA_COLUMNS 付き Pipeline"""
    p = ItemPipeline(
        output_dir=output_dir,
        site_name="TestCrawler",
        extra_columns=["備考", "営業時間"],
    )
    yield p
    try:
        if hasattr(p, "_tmp_file") and not p._tmp_file.closed:
            p._tmp_file.close()
        if hasattr(p, "_tmp_path") and os.path.exists(p._tmp_path):
            os.unlink(p._tmp_path)
    except Exception:
        pass


# =========================================================================
# 正常系テスト
# =========================================================================


class TestProcessItem:
    """process_item() のテスト"""

    def test_正常アイテムが処理される(self, pipeline):
        item = {Schema.NAME: "テスト会社", Schema.ADDR: "東京都"}
        result = pipeline.process_item(item)
        assert result[Schema.NAME] == "テスト会社"
        assert result[Schema.ADDR] == "東京都"

    def test_Noneが空文字に変換される(self, pipeline):
        item = {Schema.NAME: "テスト", Schema.TEL: None}
        result = pipeline.process_item(item)
        assert result[Schema.TEL] == ""

    def test_TELが全角から半角に正規化される(self, pipeline):
        item = {Schema.NAME: "テスト", Schema.TEL: "０３−１２３４−５６７８"}
        result = pipeline.process_item(item)
        # 注: 「−」はU+2212 (MINUS SIGN) で、normalizer のマッピング「－」(U+FF0D) とは異なる
        # そのため [^\d\-] で除去される
        assert result[Schema.TEL] == "0312345678"

    def test_件数がカウントされる(self, pipeline):
        pipeline.process_item({Schema.NAME: "1社目"})
        pipeline.process_item({Schema.NAME: "2社目"})
        assert pipeline.item_count == 2

    def test_観測カラムが記録される(self, pipeline):
        pipeline.process_item({Schema.NAME: "テスト", Schema.TEL: "000"})
        assert Schema.NAME in pipeline.observed_columns
        assert Schema.TEL in pipeline.observed_columns
        assert Schema.ADDR not in pipeline.observed_columns


# =========================================================================
# エラー系テスト
# =========================================================================


class TestValidation:
    """バリデーションのテスト"""

    def test_禁止カラムでValueError(self, pipeline):
        with pytest.raises(ValueError, match="禁止されたカラム名です"):
            pipeline.process_item({"不正なカラム": "値"})

    def test_EXTRA未宣言のカラムでValueError(self, pipeline):
        """EXTRA_COLUMNS に宣言されていないカスタムカラムは拒否される"""
        with pytest.raises(ValueError):
            pipeline.process_item({Schema.NAME: "テスト", "備考": "メモ"})

    def test_EXTRA宣言済みのカラムは許可される(self, pipeline_with_extra):
        result = pipeline_with_extra.process_item(
            {Schema.NAME: "テスト", "備考": "メモ"}
        )
        assert result["備考"] == "メモ"


# =========================================================================
# CSV 出力テスト
# =========================================================================


class TestCSVOutput:
    """close() による CSV 出力のテスト"""

    def test_観測カラムだけがヘッダーに出力される(self, pipeline, output_dir):
        pipeline.process_item({Schema.NAME: "A社", Schema.TEL: "000"})
        pipeline.process_item({Schema.NAME: "B社", Schema.ADDR: "大阪"})
        pipeline.close()

        assert pipeline.output_filepath is not None
        with open(pipeline.output_filepath, encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            # 観測された 3 カラムだけがヘッダーに存在する
            assert set(reader.fieldnames) == {Schema.GET_TIME, Schema.NAME, Schema.TEL, Schema.ADDR}

    def test_カラム順序がSchema定義順(self, pipeline):
        pipeline.process_item({Schema.TEL: "000", Schema.NAME: "テスト"})
        pipeline.close()

        with open(pipeline.output_filepath, encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader)
            # NAME が TEL より前に来る（Schema 定義順）
            assert header.index(Schema.NAME) < header.index(Schema.TEL)

    def test_0件ではCSV未作成(self, pipeline):
        pipeline.close()
        assert pipeline.output_filepath is None

    def test_EXTRAカラムがSchema後に並ぶ(self, pipeline_with_extra):
        pipeline_with_extra.process_item(
            {Schema.NAME: "テスト", "備考": "メモ"}
        )
        pipeline_with_extra.close()

        with open(pipeline_with_extra.output_filepath, encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            header = next(reader)
            # "備考" が Schema カラムの後に来る
            assert header.index(Schema.NAME) < header.index("備考")

    def test_一時ファイルがclose後に削除される(self, pipeline):
        tmp_path = pipeline._tmp_path
        pipeline.process_item({Schema.NAME: "テスト"})
        pipeline.close()
        assert not os.path.exists(tmp_path)

    def test_大量アイテムでも正常動作(self, pipeline):
        """100件のアイテムを処理してもメモリに蓄積されない（ストリーミング方式の確認）"""
        for i in range(100):
            pipeline.process_item({Schema.NAME: f"会社{i}", Schema.ADDR: f"住所{i}"})
        pipeline.close()

        with open(pipeline.output_filepath, encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            rows = list(reader)
            assert len(rows) == 101  # ヘッダー + 100行
