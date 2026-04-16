"""
正規化モジュール (src/utils/normalizer.py)

責務: Schema カラムに応じたデータ正規化を行う。
      新しいカラムの正規化ルールを追加する場合は、このモジュールに追加する。
      BaseCrawler を変更する必要はない。

設計原則:
    - 単一責任: 正規化ロジックだけに集中する
    - 開放閉鎖: 新ルールの追加に対して開いている (base.py の変更不要)
"""

import re
from src.const.schema import Schema

class NormalizationDropWarning(Warning):
    """
    正規化処理において、致命的なスキーマ違反ではないが
    要件を満たさないため空文字(DROP)にされた場合に発生する警告。
    (例: 郵便番号が7桁に満たないなど)
    """
    pass

# ---------------------------------------------------------------------------
# カラムごとの正規化関数
# 新しいカラムの正規化が必要になったら、ここに関数を追加し、
# _NORMALIZERS に登録するだけで反映される。
# ---------------------------------------------------------------------------

def _normalize_tel(value: str) -> str:
    """TEL: 全角→半角、数字とハイフン以外を除去"""
    if not value:
        return ""
    table = str.maketrans("０１２３４５６７８９－", "0123456789-")
    clean_val = re.sub(r"[^\d\-]", "", str(value).translate(table))
    # 元が空でないのに数字が一つも残らない場合は無効として扱う
    if not clean_val and str(value).strip():
        raise NormalizationDropWarning(f"無効な電話番号フォーマット (元データ: '{value}')。空文字として扱います。")
    return clean_val


def _normalize_post_code(value: str) -> str:
    """郵便番号: 〒マーク削除、全角→半角、7桁あれば xxx-xxxx 形式に整形。7桁でなければ空文字にする"""
    if not value:
        return ""
    table = str.maketrans("０１２３４５６７８９－", "0123456789-")
    val_half = str(value).translate(table)
    # 数字だけ抽出
    nums = re.sub(r"\D", "", val_half)
    if len(nums) == 7:
        return f"{nums[:3]}-{nums[3:]}"
    
    # 7桁でなければ無効な郵便番号として空にするが、
    # 開発者に通知するために警告を発生させる
    raise NormalizationDropWarning(f"無効な郵便番号フォーマット (抽出数字: '{nums}', 元データ: '{value}')。空文字として扱います。")


# カラム名 → 正規化関数 のマッピング
# 正規化が不要なカラムは登録しない (そのまま返す)
_NORMALIZERS = {
    Schema.TEL: _normalize_tel,
    Schema.POST_CODE: _normalize_post_code,
}


def normalize(key: str, value) -> any:
    """
    Schema キーに応じた正規化を実行する。

    Args:
        key:   Schema のカラム名 (例: Schema.TEL)
        value: 正規化対象の値

    Returns:
        正規化された値。None は空文字列に変換される。
        対応する正規化ルールがない場合はそのまま返す。
    """
    if value is None:
        return ""

    normalizer = _NORMALIZERS.get(key)
    if normalizer:
        return normalizer(value)
    return value
