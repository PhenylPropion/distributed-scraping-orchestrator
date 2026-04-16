"""
=========================================================================
dso_system 実装例集
=========================================================================

このファイルは「コピペして改造するだけ」で新しいクローラーが作れる
テンプレート＆パターン集です。docs/開発マニュアル.md と併せてお読みください。

目次:
    1. 最小構成（StaticCrawler）
    2. 一覧→詳細パターン（ページネーション付き）
    3. 47都道府県ループ
    4. DynamicCrawler（JavaScript サイト）
    5. ログイン必須サイト（prepare フック）
    6. マルチスレッド並列処理
    7. ETA（残り時間）の設定方法
    8. EXTRA_COLUMNS（独自カラム）
"""


# =========================================================================
# 1. 最小構成 — 一番シンプルなクローラー
# =========================================================================
"""
1ページだけスクレイピングする最小パターン。
まずはこれをコピペして改造するのがおすすめ。

ポイント:
    - parse() の中で yield するだけ
    - Schema 定数でカラム名を指定
    - DELAY でサーバー負荷を軽減
"""

import sys
from pathlib import Path
root_path = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_path))

from src.framework.static import StaticCrawler
from src.const.schema import Schema


class MinimalScraper(StaticCrawler):
    """最小構成のクローラー"""
    DELAY = 1.0  # 1秒間隔

    def parse(self, url):
        soup = self.get_soup(url)
        for card in soup.select(".company-card"):
            yield {
                Schema.NAME: card.select_one(".name").text.strip(),
                Schema.ADDR: card.select_one(".address").text.strip(),
                Schema.TEL:  card.select_one(".tel").text.strip(),
                Schema.URL:  url,
            }


# =========================================================================
# 2. 一覧→詳細パターン（ページネーション付き）
# =========================================================================
"""
最もよくあるパターン:
    一覧ページ（ページネーション） → 詳細ページ URL 収集 → 各詳細ページからデータ取得

ポイント:
    - _collect_detail_urls() でリンクを全件収集
    - _scrape_detail() で1件分のデータを取得
    - try/except で1件失敗しても止まらない
"""

class ListDetailScraper(StaticCrawler):
    """一覧→詳細パターンのクローラー"""
    DELAY = 1.0

    def parse(self, url):
        # ① 全詳細ページの URL を収集
        detail_urls = self._collect_detail_urls(url)
        self.total_items = len(detail_urls)  # ETA 表示用
        self.logger.info("詳細 URL: %d 件", len(detail_urls))

        # ② 各詳細ページからデータ取得
        for detail_url in detail_urls:
            try:
                item = self._scrape_detail(detail_url)
                if item:
                    yield item
            except Exception as e:
                self.logger.warning("スキップ: %s (%s)", detail_url, e)
                continue  # 1件失敗しても次へ

    def _collect_detail_urls(self, base_url: str) -> list[str]:
        """一覧ページをページネーションして詳細 URL を全件収集"""
        urls = []
        page = 1
        while True:
            soup = self.get_soup(f"{base_url}?page={page}")
            links = soup.select("a.detail-link")
            if not links:
                break
            for link in links:
                href = link.get("href", "")
                if href and href not in urls:
                    urls.append(href)
            page += 1
        return urls

    def _scrape_detail(self, url: str) -> dict | None:
        """詳細ページから1件分のデータを取得"""
        soup = self.get_soup(url)
        name_el = soup.select_one("h1.company-name")
        if not name_el:
            return None
        return {
            Schema.NAME: name_el.text.strip(),
            Schema.ADDR: soup.select_one(".address").text.strip(),
            Schema.TEL:  soup.select_one(".tel").text.strip(),
            Schema.HP:   soup.select_one("a.website").get("href", ""),
            Schema.URL:  url,
        }


# =========================================================================
# 3. 47都道府県ループ
# =========================================================================
"""
サイトが都道府県別にページを分けている場合のパターン。
全国を一気にスクレイピングする。

ポイント:
    - 都道府県リストをループ
    - 各県で while True のページネーション
    - Schema.PREF に都道府県名をセット
"""

PREFECTURES = [
    "hokkaido", "aomori", "iwate", "miyagi", "akita", "yamagata", "fukushima",
    "ibaraki", "tochigi", "gunma", "saitama", "chiba", "tokyo", "kanagawa",
    "niigata", "toyama", "ishikawa", "fukui", "yamanashi", "nagano",
    "gifu", "shizuoka", "aichi", "mie",
    "shiga", "kyoto", "osaka", "hyogo", "nara", "wakayama",
    "tottori", "shimane", "okayama", "hiroshima", "yamaguchi",
    "tokushima", "kagawa", "ehime", "kochi",
    "fukuoka", "saga", "nagasaki", "kumamoto", "oita", "miyazaki", "kagoshima", "okinawa",
]

class AllJapanScraper(StaticCrawler):
    """47都道府県を巡回するクローラー"""
    DELAY = 1.0

    def parse(self, url):
        self.total_items = len(PREFECTURES) * 20  # 推定件数（1県20件と仮定）

        for pref in PREFECTURES:
            self.logger.info("都道府県: %s", pref)
            page = 1
            while True:
                list_url = f"{url}/prefectures/{pref}/?page={page}"
                soup = self.get_soup(list_url)

                items = soup.select(".company-item")
                if not items:
                    break  # この県のデータがなくなったら次の県へ

                for item in items:
                    detail_url = item.select_one("a")["href"]
                    detail = self.get_soup(detail_url)
                    yield {
                        Schema.NAME: detail.select_one(".name").text.strip(),
                        Schema.PREF: pref,
                        Schema.ADDR: detail.select_one(".address").text.strip(),
                        Schema.TEL:  detail.select_one(".tel").text.strip(),
                    }

                page += 1


# =========================================================================
# 4. DynamicCrawler（JavaScript サイト）
# =========================================================================
"""
JavaScript で描画されるサイト → Playwright（ブラウザ操作）を使う。

ポイント:
    - DynamicCrawler を継承（StaticCrawler ではない）
    - self.page で Playwright Page オブジェクトを操作
    - page.goto() → page.wait_for_selector() → page.query_selector_all()
    - get_soup() は使えない。self.page を直接操作する。
"""

from src.framework.dynamic import DynamicCrawler

class JSSiteScraper(DynamicCrawler):
    """JavaScript レンダリングが必要なサイト用クローラー"""
    DELAY = 2.0  # JS サイトは少し長めに待つ

    def parse(self, url):
        self.page.goto(url)
        self.page.wait_for_selector(".company-card")  # 要素が表示されるまで待機

        cards = self.page.query_selector_all(".company-card")
        self.total_items = len(cards)

        for card in cards:
            yield {
                Schema.NAME: card.query_selector(".name").inner_text(),
                Schema.ADDR: card.query_selector(".address").inner_text(),
                Schema.TEL:  card.query_selector(".tel").inner_text(),
                Schema.URL:  url,
            }


# =========================================================================
# 5. ログイン必須サイト（prepare フック）
# =========================================================================
"""
prepare() フックを使ってログイン処理を行う。
parse() が呼ばれる前に実行される。

ポイント:
    - prepare() はログイン等の前処理に使う
    - finalize() は後処理に使う
    - どちらも任意（書かなくてもOK）
"""

class LoginRequiredScraper(StaticCrawler):
    """ログインが必要なサイト用クローラー"""
    DELAY = 1.0

    def prepare(self):
        """ログイン処理（parse() の前に自動実行される）"""
        self.logger.info("ログイン中...")
        response = self.session.post("https://example.com/login", data={
            "username": "myuser",
            "password": "mypass",
        })
        if response.status_code != 200:
            raise RuntimeError("ログイン失敗")
        self.logger.info("ログイン成功")

    def parse(self, url):
        # ログイン済みセッションでアクセス可能
        soup = self.get_soup(url)
        for item in soup.select(".member-only-data"):
            yield {
                Schema.NAME: item.select_one(".name").text.strip(),
                Schema.TEL:  item.select_one(".tel").text.strip(),
            }

    def finalize(self):
        """後処理（parse() の後に自動実行される）"""
        self.logger.info("ログアウト中...")
        self.session.get("https://example.com/logout")


# DynamicCrawler でのログイン例
class LoginRequiredDynamicScraper(DynamicCrawler):
    """ブラウザでログインが必要なサイト"""
    DELAY = 2.0

    def prepare(self):
        """ブラウザでフォーム入力→ボタンクリックでログイン"""
        self.page.goto("https://example.com/login")
        self.page.fill("#username", "myuser")
        self.page.fill("#password", "mypass")
        self.page.click("#login-button")
        self.page.wait_for_selector(".dashboard")  # ログイン後の画面を待つ
        self.logger.info("ログイン成功")

    def parse(self, url):
        self.page.goto(url)
        self.page.wait_for_selector(".data-table")
        rows = self.page.query_selector_all(".data-table tr")
        for row in rows:
            cols = row.query_selector_all("td")
            if len(cols) >= 3:
                yield {
                    Schema.NAME: cols[0].inner_text(),
                    Schema.ADDR: cols[1].inner_text(),
                    Schema.TEL:  cols[2].inner_text(),
                }


# =========================================================================
# 6. マルチスレッド並列処理
# =========================================================================
"""
concurrent.futures を使って、URLアクセス部分だけを並列化する。

仕組み:
    URLのアクセスとHTMLの解析（重い処理）だけを並列で行い、
    終わったものから順番に yield で基盤に渡す。

3つの重要ポイント:
    1. 別スレッドでは yield せず return → メインスレッドで yield
       → CSV のファイル破壊を防ぐ

    2. コネクションプールの拡張
       requests.Session はデフォルトで同時10接続。
       並列数に合わせて pool_maxsize を拡張する。

    3. self.total_items の設定で ETA が自動表示される。

注意:
    - DELAY は 0.0 に設定する（並列なので個別待機は不要）
    - MAX_WORKERS はサイトの許容度に合わせて調整
"""
import concurrent.futures
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter


class ParallelScraper(StaticCrawler):
    """マルチスレッド並列処理クローラー"""
    DELAY = 0.0          # 並列時は待機不要
    MAX_WORKERS = 4      # 並列数（サイトの許容度に合わせて調整）
    EXTRA_COLUMNS = ["備考"]

    def _setup(self):
        """コネクションプールを並列数に合わせて拡張"""
        super()._setup()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=self.MAX_WORKERS,
            pool_maxsize=self.MAX_WORKERS,
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def parse(self, url: str):
        # 1. 対象 URL を収集
        target_urls = self._get_target_urls(url)
        self.total_items = len(target_urls)
        self.logger.info("%d 件を %d 並列で処理🚀", len(target_urls), self.MAX_WORKERS)

        # 2. 並列実行
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.MAX_WORKERS) as executor:
            future_to_url = {
                executor.submit(self._parse_single_page, u): u
                for u in target_urls
            }

            # 3. 完了順に yield（CSV 書き込みはメインスレッドで直列）
            for future in concurrent.futures.as_completed(future_to_url):
                try:
                    result = future.result()
                    if result:
                        yield result  # ← ここだけがメインスレッド。CSV安全。
                except Exception as e:
                    failed_url = future_to_url[future]
                    self.logger.error("失敗: %s (%s)", failed_url, e)

    def _get_target_urls(self, start_url: str) -> list[str]:
        """一覧ページから詳細 URL をかき集める"""
        # ここにURLリスト取得ロジックを書く
        return [f"{start_url}/item/{i}" for i in range(1, 101)]

    def _parse_single_page(self, url: str) -> dict:
        """1 URL 分の処理（別スレッドで動く → yield ではなく return）"""
        soup = self.get_soup(url)
        name = soup.find("h1").text if soup.find("h1") else "不明"
        return {
            Schema.NAME: name,
            Schema.URL:  url,
            "備考": "並列取得",
        }


# =========================================================================
# 7. ETA（残り時間）の設定方法 3パターン
# =========================================================================
"""
self.total_items に全件数をセットすると、
フレームワークが自動で ETA を計算して表示する。

設定済み → [50/200件] 残り約 00:07:30 (2.5件/秒)
未設定   → [50件] 経過 00:02:30
"""

class ETAExample1(StaticCrawler):
    """方法1: URL リストの len() を使う（最も正確）"""
    def parse(self, url):
        urls = self._collect_all_urls(url)
        self.total_items = len(urls)
        for u in urls:
            yield self._scrape(u)

class ETAExample2(StaticCrawler):
    """方法2: サイトに「全328件」と書いてあればパースする"""
    def parse(self, url):
        soup = self.get_soup(url)
        total_text = soup.select_one(".result-count").text  # "全328件"
        self.total_items = int("".join(c for c in total_text if c.isdigit()))
        # ...

class ETAExample3(StaticCrawler):
    """方法3: 概算する（47都道府県 × 推定20件/県）"""
    def parse(self, url):
        self.total_items = 47 * 20
        # ...


# =========================================================================
# 8. EXTRA_COLUMNS（独自カラム）
# =========================================================================
"""
Schema に定義されていない独自カラムを使いたい場合、
クラス変数で事前に宣言する。

宣言なしに使うと → ValueError（スキーマ違反）で即座に停止
宣言すれば      → CSV の末尾に追加される

ポイント:
    - Schema カラムが先、EXTRA カラムが後に CSV 出力される
    - EXTRA カラムはデータカタログにも反映される
"""

class ExtraColumnScraper(StaticCrawler):
    """独自カラムを使うクローラー"""
    DELAY = 1.0
    EXTRA_COLUMNS = ["企業概要", "設立年", "従業員数メモ"]

    def parse(self, url):
        soup = self.get_soup(url)
        for item in soup.select(".company"):
            yield {
                # Schema カラム
                Schema.NAME: item.select_one(".name").text.strip(),
                Schema.ADDR: item.select_one(".address").text.strip(),
                # EXTRA カラム（EXTRA_COLUMNS で宣言済み）
                "企業概要":    item.select_one(".description").text.strip(),
                "設立年":     item.select_one(".founded").text.strip(),
                # 「従業員数メモ」は今回使わない → 空欄で出力される（OK）
            }
