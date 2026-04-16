"""
StaticCrawler — 静的コンテンツ対応クローラーの基底クラス (src/framework/static.py)

責務: Requests セッションの管理と HTML 取得の便利関数を提供する。
      静的なサイトはこのクラスを継承して実装すること。
"""

import abc
import logging

import bs4
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.framework.base import BaseCrawler

logger = logging.getLogger(__name__)


class StaticCrawler(BaseCrawler, abc.ABC):
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.61 Safari/537.36"
    TIMEOUT = 20

    def __init__(self):
        """
        BaseCrawler と同様に Convention over Configuration に準拠。
        """
        super().__init__()        # 基底クラスの初期化処理
        self.session = None    # 後で _setup() で初期化されるセッションオブジェクトのプレースホルダ

    def _setup(self):
        """ クラス内部で使用する通信用セッション（self.session）を初期化・構築します。
        一時的なサーバーエラー（500, 502, 503, 504）に対する自動リトライ機能（最大3回、指数バックオフ）と、
        Bot判定を回避するためのUser-Agentヘッダーをセッションに適用します。

            このメソッドは値を返さず、インスタンス変数 `self.session` を直接更新します。
        """
        # API通信用のセッションを初期化し、一時的なエラーに対する自動リトライを設定する
        logger.debug("Requests通信セッション (自動リトライ) を初期化します...")
        # セッションを作成（TCP接続の使い回しによる高速化、クッキー等の維持のため）
        self.session = requests.Session()
        """
        リトライのルール:
        total=3: エラー発生時に最大3回まで再試行する
        backoff_factor=1: リトライ間隔を指数関数的に増やす (1回目1秒, 2回目2秒, 3回目4秒...)
        status_force list : サーバー側の一時的な障害を示すHTTPステータスコードのみリトライ対象とする
        """
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        # 作成したリトライルールを、http と https の両方の通信に適用（マウント）する
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        # 指定したユーザーエージェントを設定
        self.session.headers.update({"User-Agent": self.USER_AGENT})

    def _teardown_resources(self):
        """  使用した通信セッションを解放し、クリーンアップを行う。
        開いたままの通信コネクションを適切に閉じることで、
        メモリリークやシステムリソースの枯渇を防ぐ
        """
        # セッションが作成されていれば閉じて、リソースを解放する
        if self.session:
            self.session.close()
        # 親クラスで定義されている終了処理も忘れずに実行する
        super()._teardown_resources()

    def get_soup(self, url: str) -> bs4.BeautifulSoup | None:
        """指定したURLからHTMLを取得し、BeautifulSoupオブジェクトに変換して返す。

        設定済みのセッション（自動リトライ付き）を使用して安全に通信を行い、
        取得したページの文字コードを自動判定し、文字化けを防ぐ。

        Args:
            url (str): 取得先のWebページのURL

        Returns:
            bs4.BeautifulSoup | None: 解析済みのHTMLオブジェクト（パース用）。
                CONTINUE_ON_ERROR=True かつ通信エラー時は None を返す。

        Raises:
            requests.exceptions.RequestException: CONTINUE_ON_ERROR=False のとき、
                タイムアウトや404エラーなどの通信エラーが発生した場合
        """
        logger.info("取得中: %s", url)

        try:
            # タイムアウト付きでページを取得（無限に待機してフリーズするのを防ぐ）
            response = self.session.get(url, timeout=self.TIMEOUT)
            # HTTPステータスコードがエラー（404 Not Found など）の場合は例外を発生させる
            response.raise_for_status()
            # 文字化け対策: HTTPヘッダーの charset を最優先し、
            # 未指定の場合のみ chardet の推測（apparent_encoding）にフォールバックする。
            # ※ apparent_encoding は日本語 UTF-8 を誤検知することがあるため。
            content_type = response.headers.get("Content-Type", "")
            if "charset=" not in content_type.lower():
                response.encoding = response.apparent_encoding
            # HTML文字列を解析し、スクレイピングしやすいSoupオブジェクトにして返す
            return bs4.BeautifulSoup(response.text, "html.parser")

        except requests.exceptions.RequestException as e:
            if self.CONTINUE_ON_ERROR:
                # エラー耐性モード: ログに残して None を返し、処理を継続させる
                self.error_count += 1
                logger.warning("通信エラー (スキップして継続): %s — %s", url, e)
                return None
            # 従来モード: ログを残した後、呼び出し元にエラーを伝播させる
            logger.error("通信エラー: %s", e)
            raise