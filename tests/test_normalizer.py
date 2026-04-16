"""
テスト: normalizer (src/utils/normalizer.py)

検証項目:
    - TEL 正規化（全角、半角、ハイフン混在）
    - None → 空文字変換
    - 正規化ルールなしのカラムはそのまま返る
    - 不正な郵便番号は NormalizationDropWarning を raise する
      (ItemPipeline がこれをキャッチして空文字として保存する)
"""

import pytest

from src.const.schema import Schema
from src.utils.normalizer import normalize, NormalizationDropWarning


class TestNormalize:
    """normalize() 関数のテスト"""

    def test_Noneは空文字に変換(self):
        assert normalize(Schema.NAME, None) == ""

    def test_正規化ルールなしのカラムはそのまま返す(self):
        assert normalize(Schema.NAME, "テスト会社") == "テスト会社"
        assert normalize(Schema.ADDR, "東京都千代田区") == "東京都千代田区"

    def test_空文字はそのまま返す(self):
        assert normalize(Schema.NAME, "") == ""


class TestTelNormalize:
    """TEL 正規化のテスト"""

    def test_全角数字が半角に変換される(self):
        assert normalize(Schema.TEL, "０３１２３４５６７８") == "0312345678"

    def test_全角ハイフンが半角に変換される(self):
        assert normalize(Schema.TEL, "０３－１２３４－５６７８") == "03-1234-5678"

    def test_半角はそのまま(self):
        assert normalize(Schema.TEL, "03-1234-5678") == "03-1234-5678"

    def test_括弧やスペースが除去される(self):
        assert normalize(Schema.TEL, "(03) 1234-5678") == "031234-5678"

    def test_数字とハイフン以外が除去される(self):
        assert normalize(Schema.TEL, "TEL: 03-1234-5678") == "03-1234-5678"


class TestPostCodeNormalize:
    """郵便番号 正規化のテスト"""

    def test_全角数字が半角に変換される(self):
        assert normalize(Schema.POST_CODE, "〒１０００００１") == "100-0001"

    def test_全角ハイフンが半角に変換される(self):
        assert normalize(Schema.POST_CODE, "〒１００－０００１") == "100-0001"

    def test_半角はそのまま(self):
        assert normalize(Schema.POST_CODE, "100-0001") == "100-0001"

    def test_括弧やスペースが除去される(self):
        assert normalize(Schema.POST_CODE, "(100) 0001") == "100-0001"

    def test_数字とハイフン以外が除去される(self):
        assert normalize(Schema.POST_CODE, "〒: 100-0001") == "100-0001"

    def test_7桁でハイフンなしがハイフン付きに変換される(self):
        assert normalize(Schema.POST_CODE, "1000001") == "100-0001"
    
    def test_8桁の場合はNormalizationDropWarningが発生する(self):
        """不正な桁数の郵便番号は NormalizationDropWarning を raise する。
        ItemPipeline がこれをキャッチして空文字として保存する。"""
        with pytest.raises(NormalizationDropWarning):
            normalize(Schema.POST_CODE, "10000011")

    def test_少なすぎる文字数の場合もNormalizationDropWarningが発生する(self):
        """桁数不足の郵便番号は NormalizationDropWarning を raise する。"""
        with pytest.raises(NormalizationDropWarning):
            normalize(Schema.POST_CODE, "100-00")