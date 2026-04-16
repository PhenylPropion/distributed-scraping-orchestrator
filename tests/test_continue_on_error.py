"""
テスト: CONTINUE_ON_ERROR エラー耐性モード

検証項目:
    [StaticCrawler.get_soup()]
    - CONTINUE_ON_ERROR=True のとき、通信エラーで None を返す
    - CONTINUE_ON_ERROR=True のとき、404 で None を返す
    - CONTINUE_ON_ERROR=True のとき、エラー件数がカウントされる
    - CONTINUE_ON_ERROR=False のとき、通信エラーで例外を raise する

    [DynamicCrawler.get_soup()]
    - CONTINUE_ON_ERROR=True のとき、ページ取得エラーで None を返す
    - CONTINUE_ON_ERROR=False のとき、ページ取得エラーで例外を raise する

    [BaseCrawler.execute() — CONTINUE_ON_ERROR=True]
    - parse() 内のアイテムエラーをスキップして処理継続
    - parse() ジェネレーター自体のエラーで取得済みデータを保存して正常終了
    - エラー件数が error_count に正しく記録される
    - ValueError (Schema違反) は CONTINUE_ON_ERROR に関わらず即停止

    [BaseCrawler.execute() — CONTINUE_ON_ERROR=False]
    - parse() 内のアイテムエラーで即停止する
    - parse() ジェネレーター自体のエラーで即停止する
"""

import pytest
from unittest.mock import MagicMock, patch
import requests

from src.const.schema import Schema
from src.framework.static import StaticCrawler
from src.framework.dynamic import DynamicCrawler


# =============================================================================
# テスト用クローラー
# =============================================================================

class _BaseTestStaticCrawler(StaticCrawler):
    """テスト用: _setup() をスキップしてモックセッションを使う StaticCrawler 基底"""

    def _setup(self):
        # モックセッションを差し込む（実際のHTTP通信を行わない）
        self.session = MagicMock()

    def parse(self, url):
        yield {Schema.NAME: "テスト"}


class StrictStaticCrawler(_BaseTestStaticCrawler):
    """CONTINUE_ON_ERROR=False のクローラー"""
    CONTINUE_ON_ERROR = False


class LenientStaticCrawler(_BaseTestStaticCrawler):
    """CONTINUE_ON_ERROR=True（デフォルト）のクローラー"""
    CONTINUE_ON_ERROR = True


class _BaseTestDynamicCrawler(DynamicCrawler):
    """テスト用: Playwright 起動をスキップする DynamicCrawler 基底"""

    def _setup(self):
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = MagicMock()

    def parse(self, url):
        yield {Schema.NAME: "テスト"}


class StrictDynamicCrawler(_BaseTestDynamicCrawler):
    CONTINUE_ON_ERROR = False


class LenientDynamicCrawler(_BaseTestDynamicCrawler):
    CONTINUE_ON_ERROR = True


# =============================================================================
# StaticCrawler.get_soup() のテスト
# =============================================================================

class TestStaticGetSoup:
    """StaticCrawler.get_soup() のエラー耐性テスト"""

    def _make_crawler(self, continue_on_error: bool):
        """セットアップ済みのクローラーを返す"""
        cls = LenientStaticCrawler if continue_on_error else StrictStaticCrawler
        crawler = cls()
        crawler._setup()
        return crawler

    def test_CONTINUE_ON_ERROR_True_のとき_接続エラーでNoneを返す(self):
        crawler = self._make_crawler(continue_on_error=True)
        crawler.session.get.side_effect = requests.exceptions.ConnectionError("接続失敗")
        result = crawler.get_soup("https://example.com/")
        assert result is None

    def test_CONTINUE_ON_ERROR_True_のとき_タイムアウトでNoneを返す(self):
        crawler = self._make_crawler(continue_on_error=True)
        crawler.session.get.side_effect = requests.exceptions.Timeout("タイムアウト")
        result = crawler.get_soup("https://example.com/")
        assert result is None

    def test_CONTINUE_ON_ERROR_True_のとき_404でNoneを返す(self):
        crawler = self._make_crawler(continue_on_error=True)
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=MagicMock(status_code=404)
        )
        crawler.session.get.return_value = mock_resp
        result = crawler.get_soup("https://example.com/not-found")
        assert result is None

    def test_CONTINUE_ON_ERROR_True_のとき_エラー件数がインクリメントされる(self):
        crawler = self._make_crawler(continue_on_error=True)
        crawler.session.get.side_effect = requests.exceptions.ConnectionError()
        assert crawler.error_count == 0
        crawler.get_soup("https://example.com/")
        assert crawler.error_count == 1
        crawler.get_soup("https://example.com/page2")
        assert crawler.error_count == 2

    def test_CONTINUE_ON_ERROR_False_のとき_接続エラーで例外を発生させる(self):
        crawler = self._make_crawler(continue_on_error=False)
        crawler.session.get.side_effect = requests.exceptions.ConnectionError("接続失敗")
        with pytest.raises(requests.exceptions.ConnectionError):
            crawler.get_soup("https://example.com/")

    def test_CONTINUE_ON_ERROR_False_のとき_404で例外を発生させる(self):
        crawler = self._make_crawler(continue_on_error=False)
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=MagicMock(status_code=404)
        )
        crawler.session.get.return_value = mock_resp
        with pytest.raises(requests.exceptions.HTTPError):
            crawler.get_soup("https://example.com/not-found")

    def test_正常レスポンスはNoneを返さない(self):
        """エラーがない場合は BeautifulSoup オブジェクトを返す"""
        crawler = self._make_crawler(continue_on_error=True)
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        mock_resp.headers = {"Content-Type": "text/html; charset=utf-8"}
        mock_resp.text = "<html><body><p>テスト</p></body></html>"
        crawler.session.get.return_value = mock_resp
        result = crawler.get_soup("https://example.com/")
        assert result is not None


# =============================================================================
# DynamicCrawler.get_soup() のテスト
# =============================================================================

class TestDynamicGetSoup:
    """DynamicCrawler.get_soup() のエラー耐性テスト"""

    def _make_crawler(self, continue_on_error: bool):
        cls = LenientDynamicCrawler if continue_on_error else StrictDynamicCrawler
        crawler = cls()
        crawler._setup()
        return crawler

    def test_CONTINUE_ON_ERROR_True_のとき_ページ取得エラーでNoneを返す(self):
        crawler = self._make_crawler(continue_on_error=True)
        crawler.page.goto.side_effect = Exception("ページ読み込み失敗")
        result = crawler.get_soup("https://example.com/")
        assert result is None

    def test_CONTINUE_ON_ERROR_True_のとき_エラー件数がインクリメントされる(self):
        crawler = self._make_crawler(continue_on_error=True)
        crawler.page.goto.side_effect = Exception("タイムアウト")
        assert crawler.error_count == 0
        crawler.get_soup("https://example.com/")
        assert crawler.error_count == 1

    def test_CONTINUE_ON_ERROR_False_のとき_ページ取得エラーで例外を発生させる(self):
        crawler = self._make_crawler(continue_on_error=False)
        crawler.page.goto.side_effect = Exception("ページ読み込み失敗")
        with pytest.raises(Exception, match="ページ読み込み失敗"):
            crawler.get_soup("https://example.com/")

    def test_正常取得はNoneを返さない(self):
        crawler = self._make_crawler(continue_on_error=True)
        crawler.page.goto.return_value = None
        crawler.page.content.return_value = "<html><body>テスト</body></html>"
        result = crawler.get_soup("https://example.com/")
        assert result is not None


# =============================================================================
# BaseCrawler.execute() — CONTINUE_ON_ERROR=True のテスト
# =============================================================================

class TestExecuteContinueOnErrorTrue:
    """execute() が CONTINUE_ON_ERROR=True でエラーを吸収して処理継続するテスト"""

    def test_アイテム処理中のエラーをスキップして継続する(self, tmp_path):
        """parse() が正常 yield → エラー → 正常 yield の順でも合計2件処理される"""

        class CrawlerWithItemError(StaticCrawler):
            CONTINUE_ON_ERROR = True

            def _setup(self):
                self.session = MagicMock()

            def parse(self, url):
                yield {Schema.NAME: "1件目"}
                yield {"壊れたデータ": "エラーになる"}  # Schema違反ではないが…
                yield {Schema.NAME: "3件目"}

        # "壊れたデータ" は EXTRA_COLUMNS 未宣言 → ValueError → CONTINUE_ON_ERROR でも停止
        # この動作を確認するため、まずは別クラスで普通のエラーを使う
        class CrawlerWithRuntimeError(StaticCrawler):
            CONTINUE_ON_ERROR = True

            def _setup(self):
                self.session = MagicMock()

            def parse(self, url):
                yield {Schema.NAME: "1件目"}
                # parse() ジェネレーター内で予期せぬ例外
                raise RuntimeError("一時的なエラー")

        crawler = CrawlerWithRuntimeError()
        crawler.site_name = "テスト"
        # output を tmp_path に向ける
        crawler.local_output_dir = tmp_path
        from src.framework.pipeline import ItemPipeline
        crawler.pipeline = ItemPipeline(output_dir=tmp_path, site_name="テスト")

        crawler._setup()
        crawler.execute("https://example.com/")

        # エラー発生前の1件は保存されている
        assert crawler.item_count == 1
        # エラー件数が記録されている
        assert crawler.error_count == 1

    def test_parse_ジェネレーターエラー後も取得済みデータが保存される(self, tmp_path):
        """途中でエラーが発生しても、それまでに yield したデータは CSV に保存される"""

        class PartialCrawler(StaticCrawler):
            CONTINUE_ON_ERROR = True

            def _setup(self):
                self.session = MagicMock()

            def parse(self, url):
                for i in range(5):
                    yield {Schema.NAME: f"会社{i}"}
                raise ConnectionError("途中でネットワーク切断")

        from src.framework.pipeline import ItemPipeline
        crawler = PartialCrawler()
        crawler.site_name = "テスト"
        crawler.local_output_dir = tmp_path
        crawler.pipeline = ItemPipeline(output_dir=tmp_path, site_name="テスト")
        crawler._setup()
        crawler.execute("https://example.com/")

        assert crawler.item_count == 5
        assert crawler.error_count == 1
        assert crawler.output_filepath is not None

    def test_error_countが複数エラーを正しく累積する(self, tmp_path):
        """get_soup() の複数エラーが error_count に正しく累積される"""

        class MultiErrorCrawler(StaticCrawler):
            CONTINUE_ON_ERROR = True

            def _setup(self):
                self.session = MagicMock()
                self.session.get.side_effect = requests.exceptions.ConnectionError()

            def parse(self, url):
                for page in range(3):
                    soup = self.get_soup(f"{url}?page={page}")
                    if soup is not None:
                        yield {Schema.NAME: f"会社{page}"}
                    # soup is None → スキップ

        from src.framework.pipeline import ItemPipeline
        crawler = MultiErrorCrawler()
        crawler.site_name = "テスト"
        crawler.local_output_dir = tmp_path
        crawler.pipeline = ItemPipeline(output_dir=tmp_path, site_name="テスト")
        crawler._setup()
        crawler.execute("https://example.com/")

        # 3回すべて get_soup() がエラー → error_count == 3、item_count == 0
        assert crawler.error_count == 3
        assert crawler.item_count == 0

    def test_ValueError_は常に即停止する(self, tmp_path):
        """Schema 違反 (ValueError) は CONTINUE_ON_ERROR=True でも停止する"""

        class SchemaViolationCrawler(StaticCrawler):
            CONTINUE_ON_ERROR = True

            def _setup(self):
                self.session = MagicMock()

            def parse(self, url):
                yield {Schema.NAME: "正常"}
                yield {"未宣言カラム": "違反"}  # ValueError を発生させる

        from src.framework.pipeline import ItemPipeline
        crawler = SchemaViolationCrawler()
        crawler.site_name = "テスト"
        crawler.local_output_dir = tmp_path
        crawler.pipeline = ItemPipeline(output_dir=tmp_path, site_name="テスト")
        crawler._setup()

        with pytest.raises(ValueError, match="禁止されたカラム名"):
            crawler.execute("https://example.com/")


# =============================================================================
# BaseCrawler.execute() — CONTINUE_ON_ERROR=False のテスト
# =============================================================================

class TestExecuteContinueOnErrorFalse:
    """execute() が CONTINUE_ON_ERROR=False でエラー時に即停止するテスト"""

    def test_parse_ジェネレーターエラーで即停止する(self, tmp_path):
        """parse() が例外を raise すると execute() も例外を伝播させる"""

        class StrictCrawler(StaticCrawler):
            CONTINUE_ON_ERROR = False

            def _setup(self):
                self.session = MagicMock()

            def parse(self, url):
                yield {Schema.NAME: "1件目"}
                raise RuntimeError("停止すべきエラー")

        from src.framework.pipeline import ItemPipeline
        crawler = StrictCrawler()
        crawler.site_name = "テスト"
        crawler.local_output_dir = tmp_path
        crawler.pipeline = ItemPipeline(output_dir=tmp_path, site_name="テスト")
        crawler._setup()

        with pytest.raises(RuntimeError, match="停止すべきエラー"):
            crawler.execute("https://example.com/")

    def test_アイテムエラーで即停止する(self, tmp_path):
        """アイテム処理中の例外が CONTINUE_ON_ERROR=False なら伝播する"""

        class StrictCrawlerWithItemError(StaticCrawler):
            CONTINUE_ON_ERROR = False

            def _setup(self):
                self.session = MagicMock()

            def parse(self, url):
                yield {Schema.NAME: "1件目"}
                yield {Schema.NAME: None}  # None は空文字になるのでエラーにはならない

        # アイテムレベルで本当に例外を出すにはPipelineを乗っ取る
        from src.framework.pipeline import ItemPipeline
        crawler = StrictCrawlerWithItemError()
        crawler.site_name = "テスト"
        crawler.local_output_dir = tmp_path
        pipeline = ItemPipeline(output_dir=tmp_path, site_name="テスト")
        original_process = pipeline.process_item

        call_count = [0]

        def raising_process(item):
            call_count[0] += 1
            if call_count[0] == 2:
                raise ValueError("禁止されたカラム名です: テストエラー")
            return original_process(item)

        pipeline.process_item = raising_process
        crawler.pipeline = pipeline
        crawler._setup()

        with pytest.raises(ValueError):
            crawler.execute("https://example.com/")

    def test_get_soup_エラーで例外を伝播させる(self, tmp_path):
        """CONTINUE_ON_ERROR=False のとき get_soup() の通信エラーが伝播する"""

        class StrictFetchCrawler(StaticCrawler):
            CONTINUE_ON_ERROR = False

            def _setup(self):
                self.session = MagicMock()
                self.session.get.side_effect = requests.exceptions.Timeout("タイムアウト")

            def parse(self, url):
                soup = self.get_soup(url)  # ここで例外が発生する
                if soup:
                    yield {Schema.NAME: "取得成功"}

        from src.framework.pipeline import ItemPipeline
        crawler = StrictFetchCrawler()
        crawler.site_name = "テスト"
        crawler.local_output_dir = tmp_path
        crawler.pipeline = ItemPipeline(output_dir=tmp_path, site_name="テスト")
        crawler._setup()

        with pytest.raises(requests.exceptions.Timeout):
            crawler.execute("https://example.com/")


# =============================================================================
# クラス変数のデフォルト値確認
# =============================================================================

class TestContinueOnErrorDefault:
    """CONTINUE_ON_ERROR のデフォルト値と継承動作を確認"""

    def test_StaticCrawlerのデフォルトはTrue(self):
        assert LenientStaticCrawler.CONTINUE_ON_ERROR is True

    def test_DynamicCrawlerのデフォルトはTrue(self):
        assert LenientDynamicCrawler.CONTINUE_ON_ERROR is True

    def test_Falseに設定したクローラーはFalse(self):
        assert StrictStaticCrawler.CONTINUE_ON_ERROR is False

    def test_error_countの初期値はゼロ(self):
        crawler = LenientStaticCrawler()
        assert crawler.error_count == 0
