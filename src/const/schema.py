class Schema:
    """
    システムの共通カラム定義クラス
    開発者は必ずこの定数を使用してカラムを当てはめること。
    = の後が実際の出力リストのカラム名となる。
    """
    # 取得日時はシステム側で自動生成する

    # --- 属性情報 ---
    GET_TIME = "取得日時"  # 取得日時
    URL = "取得URL"        # 取得ページのURL
    # --- STX基本カラム準拠 基本情報 ---
    NAME = "名称"             # 会社名、店舗名など
    NAME_KANA = "名称_カナ"  # 会社名、店舗名などの読み仮名
    PREF = "都道府県"         # 都道府県
    POST_CODE = "郵便番号"    # 郵便番号
    ADDR = "住所"            # 市区町村以降
    TEL = "TEL"             # 電話番号
    # --- 会社情報 ---
    CO_NUM = "法人番号"      # 法人番号
    REP_NM = "代表者名"      # 代表者名
    POS_NM = "役職"         # 代表者の役職
    EMP_NUM = "従業員数"     # 従業員数
    LOB = "事業内容"         # 事業内容など Line of Business
    CAP = "資本金"         # 資本金 Capital
    # --- 業種情報 ---
    CAT_LV1  = "大業種"     # 大業種カテゴリ
    CAT_LV2  = "中業種"     # 中業種カテゴリ
    CAT_LV3  = "小業種"     # 小業種カテゴリ
    CAT_NM   = "細業種"     # 細業種カテゴリ
    CAT_SITE = "サイト定義業種・ジャンル"  # 取得元サイトが定義した業種・ジャンル
    # --- SNS情報 ---
    LINE   = "Lineアカウント"      # LINE公式アカウント
    INSTA  = "Instagramアカウント" # Instagramアカウント
    X      = "Xアカウント"         # Xアカウント
    FB     = "Facebookアカウント"  # Facebookアカウント
    TIKTOK = "TikTokアカウント"    # TikTokアカウント
    # --- その他情報 ---
    HP = "HP"              # 会社・店舗のホームページURL

    # ---  GBP情報  ---
    FAC_NAME = "施設名"       # 例:イオンモール幕張新都心
    STS_NM   = "営業状態"     # 例:営業中、休業、閉店など
    HOLIDAY  = "定休日"       # 店舗の定休日
    TIME     = "営業時間"
    TIME_MON = "営業時間(月)"
    TIME_TUE = "営業時間(火)"
    TIME_WED = "営業時間(水)"
    TIME_THU = "営業時間(木)"
    TIME_FRI = "営業時間(金)"
    TIME_SAT = "営業時間(土)"
    TIME_SUN = "営業時間(日)"
    # --- その他の情報 ---
    SCORES   = "口コミ採点"   # 口コミの採点（例：Googleや食べログの評価）
    REV_SCR  = "口コミ件数"   # 口コミの件数（例：Googleや食べログの口コミ件数）
    OPEN_DATE = "設立年月日"  # YYYY-MM-DD 会社設立日、店舗開業日など
    PAYMENTS = "支払い方法" # クレジットカード、電子マネー、QRコード決済など
    
    # --- スクレイピングカラム ---
    SALES = "売上"            # 売上高

    # =========================================================================
    # カラムの正式順序（CSV ヘッダー、データカタログ等で使用）
    # ⚠ 新しいカラムを追加した場合は、上の定数定義とここの両方に追加すること。
    # =========================================================================
    COLUMNS = [
        GET_TIME, URL, NAME, NAME_KANA, PREF, POST_CODE, ADDR, TEL, CO_NUM,
        REP_NM, POS_NM, EMP_NUM, LOB, CAP,
        CAT_LV1, CAT_LV2, CAT_LV3, CAT_NM, CAT_SITE,
        LINE, INSTA, X, FB, TIKTOK, HP, OPEN_DATE, HOLIDAY, REV_SCR,
        FAC_NAME, STS_NM,
        TIME, TIME_MON, TIME_TUE, TIME_WED, TIME_THU, TIME_FRI, TIME_SAT, TIME_SUN,
        SCORES, PAYMENTS, SALES,
    ]