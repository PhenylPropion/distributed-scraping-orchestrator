"""
git_sync.py — 外部リポジトリの同期用ユーティリティ
"""

import subprocess
from pathlib import Path

from prefect import task
from prefect.states import State, StateType
from prefect.logging import get_run_logger

@task(name="pull_scripts_repo", task_run_name="Git Pull: scripts")
def pull_scripts_repo_task(project_root: Path) -> State:
    """
    指定されたプレジェクトルート配下の scripts/ ディレクトリ（別リポジトリ）の
    最新コードを Git から Pull して同期する。
    """
    logger = get_run_logger()
    scripts_dir = project_root / "scripts"
    
    if not (scripts_dir / ".git").exists():
        logger.warning("scripts/ は Git 管理されていません。Pull をスキップします。")
        return State(type=StateType.CANCELLED, message="scripts/ は Git 管理されていません。")

    logger.debug("scripts/ リポジトリの Git Pull を実行します...")
    try:
        result = subprocess.run(
            ["git", "-C", str(scripts_dir), "pull"],
            capture_output=True,
            text=True,
            check=True
        )
        logger.debug("Git Pull 成功:\n%s", result.stdout.strip())
        return True
    except subprocess.CalledProcessError as e:
        logger.error("Git Pull 失敗:\n[STDOUT] %s\n[STDERR] %s", e.stdout, e.stderr)
        raise RuntimeError(f"scripts リポジトリの同期に失敗しました: {e.stderr}")
    except FileNotFoundError:
        logger.error("git コマンドが見つかりませんでした。")
        raise RuntimeError("git コマンドを実行できません。")
