"""
scraper_loader.py — クローラーモジュールの動的読み込みローダー

責務:
    - 指定されたモジュールパスから Python ファイルを動的インポートする
    - インポートしたモジュール内から BaseCrawler のサブクラスを探索する
    - エラー発生時には、原因と対処方法をわかりやすく説明する例外を投げる

設計:
    - SRP (単一責任): クラス群の発見と読み込みだけを担当。実行・初期化は行わない。
"""

import importlib
import inspect
from typing import Type

from src.framework.base import BaseCrawler

def load_scraper_class(module_path: str) -> Type[BaseCrawler]:
    """
    指定されたモジュールパスから BaseCrawler のサブクラスを探索して返す。
    
    Args:
        module_path: sites.yml に記載された module (例: "jobs.eaidem")
        
    Returns:
        クローラークラス (インスタンス化されていない Type)
        
    Raises:
        ModuleNotFoundError: ファイルが見つからない場合
        RuntimeError: BaseCrawler を継承したクラスが見つからない場合
    """
    full_module_path = f"scripts.sites.{module_path}"
    
    try:
        module = importlib.import_module(full_module_path)
    except ImportError as e:
        raise ImportError(
            f"サイトモジュール '{full_module_path}' の読み込みに失敗しました。\n"
            f"【原因】該当ファイルが存在しない、またはスクリプト内のimport文でエラーが発生しています。\n"
            f"【確認事項】\n"
            f"  1. 'scripts/sites/{module_path.replace('.', '/')}.py' が正しいパスにあるか\n"
            f"  2. sites.yml の 'module: {module_path}' の記述（スペルミスや階層）が正しいか\n"
            f"  3. スクリプト内で存在しない外部ライブラリを import していないか\n"
            f"【発生エラー】{type(e).__name__}: {str(e)}"
        )

    # モジュール内で BaseCrawler を継承したクラスを探す
    for name, obj in inspect.getmembers(module, inspect.isclass):
        if issubclass(obj, BaseCrawler) and obj is not BaseCrawler:
            # 他ファイルからのインポートではなく、このファイル内で定義されたクラスかチェック
            if obj.__module__ == module.__name__:
                return obj

    raise RuntimeError(
        f"'{full_module_path}' 内にクローラーのクラス定義が見つかりませんでした。\n"
        f"【原因】フレームワークがクローラークラスを認識できていません。\n"
        f"【確認事項】\n"
        f"  1. 'StaticCrawler' または 'DynamicCrawler' を継承したクラスが定義されているか\n"
        f"     例: class MyScraper(StaticCrawler):\n"
        f"  2. モジュールロード時にクラス定義が実行される場所（グローバルスコープ）にあるか\n"
        f"  3. 別ファイルからの import ではなく、このファイルで直接クラスを実装しているか"
    )
