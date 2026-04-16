"""
BaseCrawler — クローラー基底クラス (src/framework/base.py)

責務: スクレイピングの実行フロー制御
      (Setup → Prepare → Parse Loop → Pipeline → Finalize)

責務外 (触れない):
    - データの検証・正規化・保存 → ItemPipeline に委譲
    - S3 への転送                → Prefect Flow が担当 (設計書 §3.5, §8.1)
    - メール通知                  → Prefect Flow が担当
    - 正規化ルール                → src/utils/normalizer.py に委譲

設計原則:
    - 単一責任: 実行フローの制御のみ。データ処理は Pipeline に委譲。
    - Convention over Configuration: 環境変数・設定ファイル不要で動作する。
    - Template Method: execute() が処理フローを固定し、サブクラスは parse() のみ実装する。
"""

import abc
import datetime
import logging
import time
from pathlib import Path
from typing import Generator, final

from src.framework.pipeline import ItemPipeline

# モジュール単位の logger (print() の代替)
# 呼び出し側で logging.basicConfig() を設定すれば、レベルやフォーマットを自由に制御できる。
logger = logging.getLogger(__name__)


class BaseCrawler(abc.ABC):
    """
    クローラーの基底クラス 各クローラーはこのクラスを継承して実装すること
    共通する処理はここで定義する
    主要なメソッド:
        - parse(url): 抽象メソッド。URLを受け取り、データアイテムを生成するジェネレーター。
        - execute(url): クローラのメイン実行メソッド。URLを受け取り、parseを呼び出し、結果をCSVに保存する。

    クラス変数:
        - EXTRA_COLUMNS: サイト固有のカスタムカラム名リスト。デフォルト []。
          Schema に定義されていない独自カラムを使用したい場合に宣言する。
        - CONTINUE_ON_ERROR: エラー耐性モード。True の場合、ネットワークエラー等が
          発生してもスクレイピングを中断せず継続する。デフォルト True。
          False に設定すると従来どおりエラーで即停止する。
    """
    DELAY: float = 0.0
    EXTRA_COLUMNS: list[str] = []
    CONTINUE_ON_ERROR: bool = True
    
    def __init_subclass__(cls, **kwargs):
        """
        * 重要 : クラス定義時のチェックを行うインスタンスメソッド
          サブクラスで禁止メソッドがオーバーライドされていないか検査する。

        * 改良メモ:
            - モジュール判定を startswith("src.framework") に変更。
              ファイル分割 (static.py, dynamic.py, pipeline.py) に対応するため。
            - 禁止メソッドリストから削除済みメソッドを除外。
        """
        super().__init_subclass__(**kwargs) # 基底クラスの初期化処理を呼び出す

        # 1. フレームワーク内部のクラスならチェック免除
        # (src/framework/ 配下のファイルで定義されたクラスはOK)
        if cls.__module__.startswith("src.framework"):
            return

        # 2. ユーザーコードに対する禁止メソッドリスト
        forbidden_methods = ["execute", "__init__"]

        for method in forbidden_methods:
            if method in cls.__dict__:
                raise TypeError(
                    f"違反検出: {cls.__name__} で '{method}' がオーバーライドされています。\n"
                    f"基盤の初期化処理が破損する恐れがあるため、このメソッドの変更は禁止です。\n"
                    f"変数の初期化を行いたい場合 -> 'prepare()' メソッドを使用してください。\n"
                    f"ロジックを書きたい場合 -> 'parse()' メソッドを使用してください。"
                )

    def __init__(self):
        """
        クローラを初期化し、一時保存領域を確保する。

        * 改良メモ (旧コードからの変更点):
            - is_test パラメータを削除しました。
              設計書 §3.5「Convention over Configuration」に基づき、
              Crawler は常に同一の挙動（ローカル書き込みのみ）とします。
              テスト/本番の違いは Crawler の外側 (Prefect Flow) で制御します。
            - データ処理 (検証・正規化・CSV保存) を ItemPipeline に委譲しました。
              BaseCrawler は実行フローの制御に専念します。
        """
        # --- ロガーの初期化 ---
        # Prefect の Flow/Task 内で実行中の場合は Prefect のロガーを使用し、
        # UI に INFO, WARNING, ERROR が全て表示されるようにする
        try:
            from prefect import get_run_logger
            self.logger = get_run_logger()
        except BaseException:
            self.logger = logging.getLogger(self.__class__.__name__)

        # スクレイピング中はローカルの一時フォルダ(output)に保存する。
        # S3 への転送は Prefect Flow が担当する。
        # 3つ上の階層(dso_system/) の直下の output フォルダを指定
        base_dir = Path(__file__).resolve().parent.parent.parent
        self.local_output_dir = base_dir / "output"
        self.local_output_dir.mkdir(parents=True, exist_ok=True)

        # サイト表示名（日本語OK）— run_flow.py から設定される。未設定時はクラス名を使用。
        self._site_name: str = self.__class__.__name__

        # サイトID — run_flow.py から設定される
        self._site_id: str = ""

        # データ処理パイプライン (検証 → 正規化 → CSV保存)
        self.pipeline = ItemPipeline(
            output_dir=self.local_output_dir,
            site_name=self._site_name,
            extra_columns=self.EXTRA_COLUMNS,
            site_id=self._site_id,
        )

        # 生成されたファイルのパス (完了後にアップローダーへ渡す)
        # Pipeline が CSV を作成した後、pipeline.output_filepath に設定される
        self.output_filepath = None

        # 実行後に観測されたカラム名のリスト（データカタログ用）
        # Pipeline が実際に処理したデータのキーから自動収集される
        self.observed_columns: list[str] = []
        self.item_count: int = 0
        self.extra_columns: list[str] = []

        # ETA / 進捗表示（開発者が parse() 内で self.total_items を設定すると ETA が表示される）
        self.total_items: int | None = None

        # エラー耐性モード用カウンター（スキップされたエラーの件数）
        self.error_count: int = 0

    # ===============================================
    # プロパティ
    # ===============================================

    @property
    def site_name(self) -> str:
        """サイト表示名（CSV ファイル名に使用）。"""
        return self._site_name

    @site_name.setter
    def site_name(self, value: str):
        """site_name を変更すると Pipeline にも自動反映される。"""
        self._site_name = value
        self.pipeline._site_name = value

    @property
    def site_id(self) -> str:
        """サイトID（CSV ファイル名に使用）。"""
        return self._site_id

    @site_id.setter
    def site_id(self, value: str):
        """site_id を変更すると Pipeline にも自動反映される。"""
        self._site_id = value
        self.pipeline._site_id = value

    # ===============================================
    # メイン実行メソッド : Template Method
    # ===============================================

    @final
    def execute(self, url: str):
        """
        このメソッドのオーバーライドは禁止されています。
        クローラのメイン実行メソッド。

        処理フロー:
            1. _setup()     — ブラウザ/セッションの起動 (サブクラスが実装)
            2. prepare()    — [Hook] 前処理 (ログインなど、任意実装)
            3. parse(url)   — メインループ: データ取得 → Pipeline で検証・保存
            4. finalize()   — [Hook] 後処理 (任意実装)
            5. pipeline.close() + _teardown_resources() — リソース解放

        Args:
            url (str): クロール対象のURL。
        """
        self.logger.debug("開始: %s (待機間隔: %s秒)", self.__class__.__name__, self.DELAY)

        try:
            # 1. セットアップ (ブラウザ/セッション起動)
            self._setup()

            # 2. [Hook] 前処理 (ログイン等はここに書く)
            self.prepare()

            # 3. メインループ
            count = 0
            start_time = time.monotonic()
            try:
                for item in self.parse(url):
                    try:
                        # Pipeline に検証・正規化・CSV書き込みを委譲
                        self.pipeline.process_item(item)
                        count += 1

                        # 進捗ログ (10件ごと)
                        if count % 10 == 0:
                            self._log_progress(count, start_time)

                        # [Wait] 負荷軽減
                        if self.DELAY > 0:
                            time.sleep(self.DELAY)

                    except ValueError as ve:
                        # Schema違反など、開発ミスは即座に停止
                        self.logger.error("致命的エラー (スキーマ違反): %s", ve)
                        raise

                    except Exception as item_error:
                        # 取得中の軽微なエラーはスキップ
                        if self.CONTINUE_ON_ERROR:
                            self.error_count += 1
                            self.logger.warning("アイテムをスキップしました (エラー: %s)", item_error)
                            continue
                        raise

            except (ValueError, KeyboardInterrupt):
                raise
            except Exception as parse_error:
                # parse() ジェネレーター内で捕捉されなかったエラー
                if self.CONTINUE_ON_ERROR:
                    self.error_count += 1
                    self.logger.warning(
                        "parse() 実行中にエラーが発生しました。取得済みデータで処理を継続します: %s",
                        parse_error,
                    )
                else:
                    raise

            # 4. [Hook] 後処理
            self.finalize()

            # 完了ログ（エラー件数があれば併記）
            if self.error_count > 0:
                self.logger.warning(
                    "完了: %d 件処理しました (⚠ %d 件のエラーをスキップ)",
                    count, self.error_count,
                )
            else:
                self.logger.info("完了: %d 件処理しました", count)

        except Exception as e:
            # 全体に関わる致命的なエラー
            self.logger.error("致命的エラー (システム): %s", e)
            raise

        finally:
            # Pipeline のリソースを確実にクローズ
            self.pipeline.close()
            # output_filepath を Pipeline から引き継ぐ
            # (外部の Prefect Flow 等が転送に使用する)
            self.output_filepath = self.pipeline.output_filepath
            # 観測されたカラム名を Pipeline から引き継ぐ（データカタログ用）
            self.observed_columns = self.pipeline.observed_columns
            self.item_count = self.pipeline.item_count
            self.extra_columns = self.pipeline.extra_columns
            # ブラウザ/セッションのリソース解放
            self._teardown_resources()

    # ===============================================
    # 開発者が実装すべきメソッド : Abstract
    # ===============================================

    @abc.abstractmethod
    def parse(self, url: str) -> Generator[dict, None, None]:
        """
        【必須】スクレイピングロジック本体。
        データを辞書形式で yield してください。
        """
        pass

    # ===============================================
    # 内部用メソッド : Abstract — サブクラスごとに異なるリソース管理
    # ===============================================

    @abc.abstractmethod
    def _setup(self):
        """ブラウザやセッションの起動処理。サブクラスが実装する。"""
        pass

    @abc.abstractmethod
    def _teardown_resources(self):
        """リソース解放。サブクラスが実装する。"""
        pass

    # ===============================================
    # フックメソッド : Override OK
    # ===============================================

    def prepare(self):
        """ [任意] スクレイピング前の準備
        例:
            - ログインセッションの確立
            - ブラウザの初期化
            - 一時ファイルの作成
        """
        pass

    def finalize(self):
        """ [任意] スクレイピング後の処理
        例:
            - ログインセッションのログアウト
            - ブラウザのクローズ
            - 一時ファイルの削除
        """
        pass

    # ===============================================
    # 内部ユーティリティ
    # ===============================================

    def _log_progress(self, count: int, start_time: float):
        """進捗ログを出力する。total_items が設定されていれば ETA も表示。"""
        elapsed = time.monotonic() - start_time
        elapsed_str = str(datetime.timedelta(seconds=int(elapsed)))
        rate = count / elapsed if elapsed > 0 else 0

        if self.total_items and self.total_items > 0:
            remaining = (self.total_items - count) / rate if rate > 0 else 0
            remaining_str = str(datetime.timedelta(seconds=int(remaining)))
            eta = datetime.datetime.now() + datetime.timedelta(seconds=remaining)
            eta_str = eta.strftime("%H:%M")
            self.logger.info(
                "📊 [%d/%d件] ⏱ 経過 %s | 残り約 %s | 完了予定 %s",
                count, self.total_items, elapsed_str, remaining_str, eta_str,
            )
        else:
            self.logger.info(
                "📊 [%d件] ⏱ 経過 %s | 速度 %.1f件/分",
                count, elapsed_str, rate * 60,
            )
