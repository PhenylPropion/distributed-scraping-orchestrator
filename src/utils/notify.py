# src/utils/notify.py
"""
通知ユーティリティ (Microsoft Teams Webhook)

責務: クローラー実行結果（成功/失敗）を Microsoft Teams に通知する。

設定方法:
    .env に以下を追加:
        TEAMS_WEBHOOK_URL=https://xxx.webhook.office.com/webhookb2/xxx

    未設定の場合、通知はスキップされる（エラーにはならない）。
"""

import json
import logging
import os
import traceback
import urllib.request
import urllib.error
from datetime import datetime

logger = logging.getLogger(__name__)




def _send_to_teams(payload: dict) -> bool:
    """
    Teams Webhook にペイロードを送信する。

    Returns:
        True: 送信成功
        False: 送信失敗（エラーはログに記録するが例外は投げない）
    """
    webhook_url = os.environ.get("TEAMS_WEBHOOK_URL", "")
    if not webhook_url:
        logger.debug("TEAMS_WEBHOOK_URL 未設定: 通知スキップ")
        return False

    try:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                logger.debug("Teams 通知送信成功")
                return True
            else:
                logger.warning("Teams 通知: 予期しないステータス %d", resp.status)
                return False

    except urllib.error.HTTPError as e:
        logger.warning("Teams 通知失敗 (HTTP %d): %s", e.code, e.reason)
        return False
    except urllib.error.URLError as e:
        logger.warning("Teams 通知失敗 (接続エラー): %s", e.reason)
        return False
    except Exception as e:
        # 通知の失敗でクローラー全体を落とさない
        logger.warning("Teams 通知失敗 (想定外): %s", e)
        return False


def notify_success(
    site_id: str,
    site_name: str,
    item_count: int,
    output_path: str,
    elapsed_seconds: float = 0,
) -> bool:
    """
    クローラー成功時の通知を送信する。

    Args:
        site_id: サイトID
        site_name: 日本語サイト名
        item_count: 取得件数
        output_path: 出力ファイルパス
        elapsed_seconds: 実行時間（秒）

    Returns:
        通知送信成功なら True
    """
    elapsed_str = _format_elapsed(elapsed_seconds)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    title = f"✅ {site_name} ({site_id}) — {item_count} 件取得"

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "00C851",  # 緑
        "summary": title,
        "sections": [{
            "activityTitle": title,
            "facts": [
                {"name": "サイトID", "value": site_id},
                {"name": "取得件数", "value": str(item_count)},
                {"name": "出力先", "value": output_path or "(なし)"},
                {"name": "実行時間", "value": elapsed_str},
                {"name": "完了日時", "value": now},
            ],
            "markdown": True,
        }],
    }
    return _send_to_teams(payload)


def notify_failure(
    site_id: str,
    site_name: str,
    error: BaseException,
    elapsed_seconds: float = 0,
    is_zero_items: bool = False,
) -> bool:
    """
    クローラー失敗時の通知を送信する。

    Args:
        site_id: サイトID
        site_name: 日本語サイト名
        error: 発生した例外
        elapsed_seconds: 実行時間（秒）
        is_zero_items: 取得件数0件による呼び出しの場合 True。
                       実際の例外ではないためトレースバック欄は表示しない。

    Returns:
        通知送信成功なら True
    """
    elapsed_str = _format_elapsed(elapsed_seconds)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if is_zero_items:
        # 取得件数0件 — 実行は完了したがデータが取れなかった。やや黄色っぽい赤で区別
        title = f"❌⚠️ {site_name} ({site_id}) — 取得件数 0 件"
        color = "FF4500"  # OrangeRed（やや黄色っぽい赤）
        facts = [
            {"name": "サイトID",   "value": site_id},
            {"name": "対応",       "value": "コードの再実装または放棄を検討してください"},
            {"name": "考えられる原因", "value": "サイト構造の変更、URL変更、アクセス制限など"},
            {"name": "実行時間",   "value": elapsed_str},
            {"name": "発生日時",   "value": now},
        ]
        payload = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": color,
            "summary": title,
            "sections": [{
                "activityTitle": title,
                "facts": facts,
                "markdown": True,
            }, {
                "activityTitle": "⚠️ 対応が必要です",
                "text": (
                    "スクレイピングは正常に完了しましたが、**取得件数が 0 件**でした。\n\n"
                    "以下をご確認ください：\n"
                    "- スクレイパーのコード・セレクタが現在のサイト構造と一致しているか\n"
                    "- 対象 URL が変更・移転されていないか\n"
                    "- アクセス制限（ログイン要求・IP ブロック等）が発生していないか\n\n"
                    "問題が解消しない場合は、スクレイパーの**再実装または運用停止**を検討してください。"
                ),
            }],
        }
        return _send_to_teams(payload)

    # 通常の実行失敗 — トレースバックつきで通知
    tb = traceback.format_exception(type(error), error, error.__traceback__)
    tb_short = "".join(tb[-5:])  # 末尾5行

    if isinstance(error, KeyboardInterrupt):
        title = f"🛑 {site_name} ({site_id}) — 手動キャンセルされました"
        color = "808080"  # グレー
        error_concept = "ユーザーによる手動停止 (Ctrl+C 等)"
    elif isinstance(error, SystemExit):
        title = f"🛑 {site_name} ({site_id}) — システム終了"
        color = "808080"  # グレー
        error_concept = "システムからの終了要求"
    else:
        title = f"❌ {site_name} ({site_id}) — 実行失敗"
        color = "FF0000"  # 赤
        error_concept = "プログラム実行エラー"

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": color,
        "summary": title,
        "sections": [{
            "activityTitle": title,
            "facts": [
                {"name": "サイトID",   "value": site_id},
                {"name": "エラー種別", "value": f"{type(error).__name__} ({error_concept})"},
                {"name": "エラー内容", "value": str(error)[:200] if str(error) else "(メッセージなし)"},
                {"name": "実行時間",   "value": elapsed_str},
                {"name": "発生日時",   "value": now},
            ],
            "markdown": True,
        }, {
            "activityTitle": "トレースバック（末尾5行）",
            "text": f"```\n{tb_short}\n```",
        }],
    }
    return _send_to_teams(payload)


def _format_elapsed(seconds: float) -> str:
    """秒数を「3分12秒」のような表示に変換する"""
    if seconds <= 0:
        return "—"
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}時間{minutes}分{secs}秒"
    elif minutes > 0:
        return f"{minutes}分{secs}秒"
    else:
        return f"{secs}秒"


def notify_warning(
    site_id: str,
    site_name: str,
    warnings: list[str],
    item_count: int,
    output_path: str,
    elapsed_seconds: float = 0,
    error_count: int = 0,
) -> bool:
    """
    フロー完了時に一部タスクがスキップされた場合の警告通知を送信する。

    error_count > 0 (ネットワークエラーによるスキップあり) の場合は、
    データ取得自体は成功しているため ✅ 黄緑色で通知する。
    それ以外の警告（S3スキップ・Gitスキップ等）は ⚠️ オレンジで通知する。

    Args:
        site_id: サイトID
        site_name: 日本語サイト名
        warnings: スキップされたタスクの警告メッセージリスト
        item_count: 取得件数
        output_path: 出力ファイルパス
        elapsed_seconds: 実行時間（秒）
        error_count: CONTINUE_ON_ERROR でスキップされたエラー件数

    Returns:
        通知送信成功なら True
    """
    elapsed_str = _format_elapsed(elapsed_seconds)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if error_count > 0:
        # ネットワークエラーのスキップあり → ✅ 黄緑で通知
        title = f"✅ {site_name} ({site_id}) — {item_count} 件取得 (スキップエラーあり)"
        theme_color = "9ACD32"  # 黄緑
        warning_section_title = "⚠️ スキップされた処理"
    else:
        # S3・Git など infrastructure 系のスキップ → ⚠️ オレンジで通知
        title = f"⚠️ {site_name} ({site_id}) — 完了（警告あり）"
        theme_color = "FF8C00"  # オレンジ
        warning_section_title = "⚠️ スキップされたタスク"

    warnings_text = "\n".join(f"- {w}" for w in warnings)

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": theme_color,
        "summary": title,
        "sections": [{
            "activityTitle": title,
            "facts": [
                {"name": "サイトID", "value": site_id},
                {"name": "取得件数", "value": str(item_count)},
                {"name": "出力先", "value": output_path or "(なし)"},
                {"name": "実行時間", "value": elapsed_str},
                {"name": "完了日時", "value": now},
            ],
            "markdown": True,
        }, {
            "activityTitle": warning_section_title,
            "text": warnings_text,
        }],
    }
    return _send_to_teams(payload)


def send_notification(title: str, text: str) -> bool:
    """汎用的なテキストメッセージをTeamsに送信する"""
    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "0078D7", # 青
        "summary": title,
        "sections": [{
            "activityTitle": title,
            "text": text,
            "markdown": True,
        }],
    }
    return _send_to_teams(payload)
