"""
DynamicCrawler — 動的コンテンツ対応クローラーの基底クラス (src/framework/dynamic.py)

責務: Playwright ブラウザの管理を提供する。
      JavaScript で生成されたコンテンツを取得するサイトはこのクラスを継承して実装すること。
"""

import abc
import logging

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from src.framework.base import BaseCrawler

logger = logging.getLogger(__name__)


class DynamicCrawler(BaseCrawler, abc.ABC):
    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

    def __init__(self):
        """ブラウザ操作用クローラーの初期化を行う。
        Convention over Configuration に準拠し、
        Playwrightで必要となる各リソースの変数を初期化する。
        """
        super().__init__() # 基底クラスの初期化処理

        # Playwrightの階層構造に合わせた変数を準備（セットアップ前はNone）
        self.playwright = None  # Playwrightのシステムマネージャー本体
        self.browser = None  # ブラウザ本体（Chromiumなど）
        self.context = None  # ブラウザのセッション（Cookie等を独立させる枠組み）
        self.page = None  # 実際に操作するブラウザのタブ

    def _setup(self):
        """Playwrightを起動し、スクレイピング用のブラウザとページを準備する。

        Docker環境での実行を想定し、デフォルトでヘッドレスモードで起動。
        また、Bot判定を避けるためにUser-Agentを適用する。
        """
        logger.debug("Playwright を起動します...")

        # 1. Playwright の起動
        logger.debug("Playwright を起動します...")
        self.playwright = sync_playwright().start()

        # 2. Chromiumブラウザを起動（Docker環境のため基本はheadless=True）
        # ローカルデバッグ時に実際の動きを見たい場合は headless=False に変更します
        self.browser = self.playwright.chromium.launch(headless=True)

        # 3. 新しいブラウザコンテキスト（独立したシークレットウィンドウのようなもの）を作成
        # ここで共通のUser-Agentを設定し、アクセス元の身元を偽装・統一します
        self.context = self.browser.new_context(user_agent=self.USER_AGENT)

        # 4. コンテキスト内に新しいタブ（ページ）を作成
        # 以降のスクレイピング操作（クリックや文字入力など）はこの self.page に対して行います
        self.page = self.context.new_page()

    def get_soup(self, url: str, wait_until: str = "domcontentloaded") -> BeautifulSoup | None:
        """Playwright でページを取得し、BeautifulSoup オブジェクトを返す。

        Args:
            url (str): 取得先のWebページのURL
            wait_until (str): Playwright の待機条件。デフォルト "domcontentloaded"。
                "load", "domcontentloaded", "networkidle", "commit" から選択。

        Returns:
            BeautifulSoup | None: 解析済みのHTMLオブジェクト。
                CONTINUE_ON_ERROR=True かつエラー時は None を返す。

        Raises:
            Exception: CONTINUE_ON_ERROR=False のとき、ページ取得に失敗した場合
        """
        logger.info("取得中 (Playwright): %s", url)
        try:
            self.page.goto(url, wait_until=wait_until)
            return BeautifulSoup(self.page.content(), "html.parser")
        except Exception as e:
            if self.CONTINUE_ON_ERROR:
                self.error_count += 1
                logger.warning("ページ取得エラー (スキップして継続): %s — %s", url, e)
                return None
            logger.error("ページ取得エラー: %s", e)
            raise

    def _teardown_resources(self):
        """Playwrightの全リソースを安全に解放・終了します。

        メモリリークや「ゾンビプロセス（見えないブラウザが裏で起動し続けるバグ）」を
        防ぐため、作成時とは【逆の順番】で確実に閉じていきます。
        """
        # 下位のリソースから順番に閉じる（ページ -> コンテキスト -> ブラウザ -> 本体）
        if self.page:
            self.page.close()
        if self.context:
            self.context.close()
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()

        logger.info("ブラウザを終了しました")

        # 親クラスで定義されている終了処理（あれば）も忘れずに実行する
        super()._teardown_resources()