"""
テスト: Schema (src/const/schema.py)

検証項目:
    - COLUMNS リストの件数が期待値と一致（回帰チェック）
    - COLUMNS の全要素がクラス属性の値に含まれる
    - COLUMNS に重複がない
"""

from src.const.schema import Schema


class TestSchemaColumns:
    """Schema.COLUMNS の回帰テスト"""

    def test_カラム数(self):
        """Schema に新しいカラムが追加されたら、COLUMNS にも追加されているか検証"""
        assert len(Schema.COLUMNS) == 41

    def test_COLUMNSの全要素がクラス属性に含まれる(self):
        """COLUMNS にタイポや存在しない値が混入していないか検証"""
        attr_values = {v for k, v in vars(Schema).items()
                       if not k.startswith("_") and k != "COLUMNS" and isinstance(v, str)}
        for col in Schema.COLUMNS:
            assert col in attr_values, f"'{col}' は Schema のクラス属性に見つかりません"

    def test_全クラス属性がCOLUMNSに含まれる(self):
        """新しいカラム定数を追加したのに COLUMNS に追加し忘れた場合を検知"""
        attr_values = {v for k, v in vars(Schema).items()
                       if not k.startswith("_") and k != "COLUMNS" and isinstance(v, str)}
        columns_set = set(Schema.COLUMNS)
        for val in attr_values:
            assert val in columns_set, f"'{val}' が Schema.COLUMNS に含まれていません"

    def test_重複がない(self):
        assert len(Schema.COLUMNS) == len(set(Schema.COLUMNS))
