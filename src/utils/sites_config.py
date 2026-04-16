"""
config.py — 設定ファイル (scripts/sites.yml) の読み込みユーティリティ

責務:
    - scripts/sites.yml のパースと、必要に応じた探索
    - 各スクリプトにおける DRY 違反の解消
"""

import sys
import yaml
from pathlib import Path
from typing import Optional

def get_project_root() -> Path:
    """プロジェクトのルートディレクトリを返す"""
    return Path(__file__).resolve().parent.parent.parent

def load_sites(only_enabled: bool = False) -> list[dict]:
    """
    scripts/sites.yml を読み込み、サイト設定のリストを返す。

    Args:
        only_enabled (bool): True の場合、enabled: true のサイトのみを返す。
    
    Returns:
        list[dict]: サイト情報を格納した辞書のリスト。
    """
    root_path = get_project_root()
    sites_yml = root_path / "scripts" / "sites.yml"
    
    if not sites_yml.exists():
        print(f"Error: {sites_yml} が見つかりません。", file=sys.stderr)
        return []
        
    with open(sites_yml, encoding="utf-8") as f:
        data = yaml.safe_load(f)
        
    sites = data.get("sites", [])
    if only_enabled:
        sites = [s for s in sites if s.get("enabled", True)]
        
    return sites

def get_site_config(site_id: str) -> Optional[dict]:
    """
    指定された site_id に対応するサイト設定を取得する。
    """
    sites = load_sites()
    for site in sites:
        if site.get("site_id") == site_id:
            return site
    return None
