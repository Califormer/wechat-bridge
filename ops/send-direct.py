#!/usr/bin/env python3
"""Send a proactive (not task-reply) text message directly to the bound
WeChat user, without posting it in the Muse App.

Why this exists: bridge.py has no plain-text --to-user CLI (only
send-image/send-file/send-video take --to-user, with an optional caption).
A proactive alert therefore goes out as a synthetic task: it is created
with status waiting_send and result_text set, then attempt_send delivers
it through the same path as a normal reply (chunking, send_uncertain
handling, 30-day terminal-task cleanup). v3: the message carries the newest
context_token seen from the user (the official plugin echoes it on every
send; this script used to send none), and delivery is handed to the running
serve (bridge.deliver: SIGUSR1, warm keep-alive connection), falling back
to a direct send. attempt_send is status-guarded, so the message is
delivered exactly once either way. context_age_h tells how old that token
is; community SDKs assume a ~24 h context lifetime after the user's last
message (context_stale=true: delivery may be refused).

Usage:
    python3 ops/send-direct.py --message "微信桥接曾掉线、已自动恢复"
    python3 ops/send-direct.py --message-file /tmp/alert.txt
    python3 ops/send-direct.py --message "..." --dry-run   # no send, no task

Output is one JSON line with only ok/status/parts -- never a user id,
token, or message content echoed back (the content lives in the task
file, like any other bridge task).
"""
import argparse
import json
import secrets
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bridge  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description="Send a proactive message to WeChat")
    p.add_argument("--message", default=None)
    p.add_argument("--message-file", default=None)
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    if args.message_file:
        message = Path(args.message_file).read_text(encoding="utf-8").strip()
    else:
        message = (args.message or "").strip()
    if not message:
        print(json.dumps({"ok": False, "error": "empty message"}, ensure_ascii=False))
        return 2
    bridge.apply_private_umask()
    creds = bridge.load_creds()
    if not creds:
        print(json.dumps({"ok": False, "error": "no credentials"}, ensure_ascii=False))
        return 2
    to_user = creds.get("ilink_user_id")
    if not to_user:
        print(json.dumps({"ok": False, "error": "no bound user"}, ensure_ascii=False))
        return 2
    # v5: Markdown adapted like replies; a closed 24 h window is reported at
    # once (context_expired) instead of a send WeChat silently refuses.
    message = bridge.render_for_wechat(message)
    _, win_open, win_left = bridge.window_state(to_user)
    if win_open is False and not args.dry_run:
        print(json.dumps({"ok": False, "status": None, "context_expired": True,
                          "error": "24 h reply window closed: the user has not written "
                                   "for over 24 hours and WeChat refuses bot messages "
                                   "until they do - report this in the App instead"},
                         ensure_ascii=False))
        return 3
    if args.dry_run:
        print(json.dumps({"ok": True, "dry_run": True, "chars": len(message)},
                         ensure_ascii=False))
        return 0
    ctx, ctx_age_ms = bridge.latest_context_token(to_user)
    task_id = f"proactive-{bridge.now_ms()}-{secrets.token_hex(4)}"
    task = {
        "message_id": task_id,
        "from_user_id": to_user,
        "to_user_id": None,
        "session_id": None,
        "context_token": ctx,
        "create_time_ms": bridge.now_ms(),
        "received_at": bridge.now_ms(),
        "item_types": [1],
        "kind": "text",
        "text": "",
        "media_path": None,
        "media_bytes": None,
        "file_name": None,
        "play_length": None,
        "thumb_path": None,
        "media_error": None,
        "raw_items": [],
        "status": "waiting_send",
        "result_text": message,
        "execution": {"ok": True, "proactive": True,
                      "finished_at": bridge.now_ms()},
        "send": None,
    }
    with bridge.task_lock():
        bridge.save_task(task)
    bridge.PROXY_POOL.init()
    outcome = bridge.deliver(task_id)
    final = bridge.load_task(task_id) or {}
    age_h = round(ctx_age_ms / 3_600_000, 1) if ctx_age_ms is not None else None
    print(json.dumps({
        "ok": bool(outcome.get("ok")),
        "status": final.get("status"),
        "parts": outcome.get("parts"),
        "error": outcome.get("error"),
        "via": outcome.get("via", "direct"),
        "context_age_h": age_h,
        "context_stale": age_h is None or age_h >= 24,
        "context_expired": bool(outcome.get("context_expired")),
        "parked": bool(outcome.get("parked")),
        "reply_window_left_h": (round(win_left / 3_600_000, 1)
                                if win_left is not None else None),
    }, ensure_ascii=False))
    return 0 if outcome.get("ok") else 1


if __name__ == "__main__":
    sys.exit(main())
