"""Shared fixtures for the 2026-11-07 tests (relay + §8). Not a test itself:
tests/run_all.py only runs test_*.py. Builds on test_v3's isolated loader
(temp dir prefix v3-, registered in run_all.py)."""
import contextlib
import io
import json
import os
import sys
import time
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_v3 as h  # noqa: E402  (fixtures only)

fails: list = []


def check(cond, msg, detail=""):
    print(("PASS  " if cond else "FAIL  ") + msg
          + (f"\n      {str(detail)[:700]}" if detail and not cond else ""))
    if not cond:
        fails.append(msg)


class Clock:
    """now_ms of the module under test: real time plus an offset, so a test
    can let ten minutes pass in a millisecond."""
    def __init__(self):
        self.off = 0

    def __call__(self):
        return int(time.time() * 1000) + self.off

    def adv(self, minutes: float):
        self.off += int(minutes * 60_000)


def boot(name: str, src=None, **env):
    """An isolated bridge (its own temp dir), fake clock, fake sender: every
    text that would go to WeChat lands in b.SENT."""
    old = {k: os.environ.get(k) for k in env}
    os.environ.update(env)
    try:
        b = h.load(src or HERE / "bridge.py", name)
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    h.set_creds(b, token="<redacted>")
    clk = Clock()
    b.now_ms = clk
    b.SENT = []

    def fake(creds, to, text, ctx, client_id=None):
        b.SENT.append(text)
        return {"ret": 0, "_client_id": client_id, "message_id": f"m{len(b.SENT)}"}
    b.send_text = fake
    return b, clk


_seq = [0]


def say(b, text, mid=None):
    _seq[0] += 1
    mid = mid or f"u{_seq[0]}"
    b.ingest_message({"ilink_user_id": "owner"}, {
        "message_id": mid, "from_user_id": "owner", "message_type": 1,
        "context_token": f"CTX{_seq[0]}",
        "item_list": [{"type": 1, "text_item": {"text": text}}]})
    return mid


def flush(b):
    with contextlib.suppress(Exception):
        b.batch_flush_due(everything=True)


def run(fn, **kw):
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = fn(SimpleNamespace(**kw))
    txt = buf.getvalue().strip()
    if not txt:
        return rc, {}
    try:
        return rc, json.loads(txt)
    except ValueError:
        return rc, json.loads(txt.splitlines()[-1])


def claim(b, **kw):
    a = dict(limit=5, wait=0, settle_ms=None, full=False, context_since=None)
    a.update(kw)
    return run(b.cmd_claim, **a)


def progress(b, tid, **kw):
    a = dict(task_id=tid, note=None, stage=None, notify=False, hold_minutes=None,
             claim_id=None, plan=None, plan_from=None, step_done=None, artifact=[])
    a.update(kw)
    return run(b.cmd_progress, **a)


def complete(b, tid, **kw):
    a = dict(task_id=tid, result=None, result_file=None, error=None, attach=[],
             attach_as="auto", wait_attachments=0, then_wait=0, then_limit=5,
             force=False, merge_ids=None, raw=False, claim_id=None, cont=False,
             step_done=None, artifact=[], stage=None, background=False)
    a.update(kw)
    return run(b.cmd_complete, **a)


def stats(b, ids=None, recent=0):
    return run(b.cmd_stats, task_id=ids, recent=recent, hours=72)


def hook_keys(b):
    """The hook's wake filter (hooks/wechat-bridge-inbox.sh, jq) in Python:
    pending and not in an open batch, by wake_key."""
    out = []
    for f in sorted(b.INBOX_DIR.glob("*.json")):
        try:
            t = json.loads(f.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if t.get("status") == "pending_execution" and not t.get("batch_open"):
            out.append(t.get("wake_key") or t.get("message_id"))
    return out


def plan_json(steps, goal="测试"):
    return json.dumps({"goal": goal, "steps": steps}, ensure_ascii=False)


def lt(b, action, tid=None, **kw):
    """`long-tasks <action> [ID]` (v1.4 background queue)."""
    a = dict(action=action, id=tid, token=None, stage=None, note=None, notify=False,
             result=None, result_file=None, attach=[], error=None)
    a.update(kw)
    return run(b.cmd_long_tasks, **a)


def tick(b, age_s=0):
    """What the watch hook does every run: touch the liveness file."""
    b.BG_TICK.parent.mkdir(parents=True, exist_ok=True)
    b.BG_TICK.touch()
    if age_s:
        t = time.time() - age_s
        os.utime(b.BG_TICK, (t, t))


def notices(b):
    """Bridge-written messages (kind notice), oldest first: (task id, text)."""
    out = []
    for s in b.scan_task_index():
        if s.get("kind") == "notice":
            t = b.load_task(s["message_id"]) or {}
            out.append((s["message_id"], t.get("result_text") or t.get("text") or ""))
    return out


def finish(name: str) -> int:
    print()
    print(f"ALL {name} CHECKS PASSED" if not fails else f"{len(fails)} FAILED: {fails}")
    return 1 if fails else 0
