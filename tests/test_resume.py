#!/usr/bin/env python3
"""Resume + heartbeat tests (2026-11-02): a task reclaimed for silence
keeps its last stage/note as a checkpoint, the next claim surfaces
resume_from, and heartbeat renews the heartbeat without a user message.
Run: python3 tests/test_resume.py"""
import sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_v3 as h  # fixtures only

fails = []


def check(cond, msg, detail=""):
    print(("PASS  " if cond else "FAIL  ") + msg + (f"\n      {detail}" if detail else ""))
    if not cond:
        fails.append(msg)


def now_ms():
    return int(time.time() * 1000)


def cli(fn, **kw):
    return h.cli(fn, **kw)[1]


b = h.load(HERE / "bridge.py", "resume")
h.set_creds(b, token="<redacted>")

# say + claim
b.ingest_message({"ilink_user_id": "owner"}, {
    "message_id": "r1", "from_user_id": "owner", "message_type": 1,
    "context_token": "T1",
    "item_list": [{"type": 1, "text_item": {"text": "做一件长事"}}]})
out = cli(b.cmd_claim, limit=5, wait=0, settle_ms=0, full=False)
view = out["tasks"][0]
check("resume_from" not in view, "fresh claim has no resume_from", str(view))

# first executor: report stage, then go silent for >10 min
cli(b.cmd_progress, task_id="r1", note=None, stage="已做完第 1 步，正在做第 2 步", notify=False)
t = b.load_task("r1")
check(t["execution"].get("stage") == "已做完第 1 步，正在做第 2 步",
      "progress records stage", str(t["execution"]))
# backdate claim + heartbeat past the 10-min stale threshold
ex = dict(t["execution"])
ex["claimed_at"] = now_ms() - 11 * 60 * 1000
ex["heartbeat_at"] = now_ms() - 11 * 60 * 1000
t["execution"] = ex
b.save_task(t)
b._reclaim_stale()  # returns True only when a give-up wakes serve
check(True, "reclaim path runs (status asserted below)", "")
t = b.load_task("r1")
check(t["status"] == "pending_execution" and t["execution"].get("reclaims") == 1,
      "reclaim count 1, back to pending", str(t["execution"]))
ck = t["execution"].get("checkpoint") or {}
check(ck.get("stage") == "已做完第 1 步，正在做第 2 步",
      "reclaim snapshots checkpoint stage", str(ck))

# second executor claims: resume_from must surface, stage preserved
out = cli(b.cmd_claim, limit=5, wait=0, settle_ms=0, full=False)
view = out["tasks"][0]
check("已做完第 1 步" in (view.get("resume_from") or ""),
      "reclaimed claim surfaces resume_from with last stage", str(view))
check(view.get("resume_from", "").find("不要从头重做") >= 0,
      "resume_from says do not restart from scratch", str(view))

# heartbeat renews heartbeat_at, sends nothing to the user
before = b.load_task("r1")["execution"].get("heartbeat_at") or 0
out = cli(b.cmd_heartbeat, task_id="r1", stage="还在第 2 步")
after = b.load_task("r1")["execution"].get("heartbeat_at") or 0
check(out.get("ok") and after > before,
      "heartbeat renews heartbeat_at", f"before={before} after={after} out={out}")
check(b.load_task("r1")["execution"].get("stage") == "还在第 2 步",
      "heartbeat updates stage checkpoint", "")

print()
print("ALL RESUME CHECKS PASSED" if not fails else f"{len(fails)} FAILED: {fails}")
sys.exit(1 if fails else 0)
