#!/usr/bin/env python3
"""Round-2 tests (2026-11-02): silence-based auto_progress, long-step
lease, VM-recovery notice, outline, progress queue position, history
direct commands. Run: python3 tests/test_round2.py"""
import sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_v3 as h

fails = []


def check(cond, msg, detail=""):
    print(("PASS  " if cond else "FAIL  ") + msg + (f"\n      {detail}" if detail else ""))
    if not cond:
        fails.append(msg)


def now_ms():
    return int(time.time() * 1000)


def cli(fn, **kw):
    return h.cli(fn, **kw)[1]


def say(b, mid, text, tok="T1"):
    b.ingest_message({"ilink_user_id": "owner"}, {
        "message_id": mid, "from_user_id": "owner", "message_type": 1,
        "context_token": tok,
        "item_list": [{"type": 1, "text_item": {"text": text}}]})
    return b.load_task(mid)


def notices(b):
    return [x for x in b.scan_task_index() if x.get("kind") == "notice"]


b = h.load(HERE / "bridge.py", "round2")
h.set_creds(b, token="<redacted>")

# --- auto_progress: live heartbeat (100 s ago) -> no note ---
say(b, "a1", "长任务一")
cli(b.cmd_claim, limit=5, wait=0, settle_ms=0, full=False)
t = b.load_task("a1")
ex = dict(t["execution"]); ex["heartbeat_at"] = now_ms() - 100_000
t["execution"] = ex; b.save_task(t)
b.check_auto_progress(b.scan_task_index())
check(len(notices(b)) == 0, "auto_progress silent while heartbeat fresh (100 s)",
      str(notices(b)))

# --- auto_progress: silence 500 s after a heartbeat -> one warning ---
t = b.load_task("a1")
ex = dict(t["execution"]); ex["heartbeat_at"] = now_ms() - 500_000
t["execution"] = ex; b.save_task(t)
b.check_auto_progress(b.scan_task_index())
check(len(notices(b)) == 1, "auto_progress warns after 420 s silence",
      str(notices(b)))
b.check_auto_progress(b.scan_task_index())
check(len(notices(b)) == 1, "no duplicate warning for the same silence period",
      str(notices(b)))

# --- auto_progress: never heartbeated, 200 s old -> first stage note ---
say(b, "a2", "长任务二")
# a1 is still executing; claim a2 too (limit 5 claims both pending, a1 already claimed so only a2)
cli(b.cmd_claim, limit=5, wait=0, settle_ms=0, full=False)
t = b.load_task("a2")
ex = dict(t["execution"]); ex["claimed_at"] = now_ms() - 200_000
ex.pop("heartbeat_at", None)
t["execution"] = ex; b.save_task(t)
before = len(notices(b))
b.check_auto_progress(b.scan_task_index())
check(len(notices(b)) > before, "never-heartbeated task gets 180 s note",
      f"before={before} after={len(notices(b))}")

# --- long-step lease: stale but leased -> not reclaimed; lease expired -> reclaimed ---
say(b, "a3", "长任务三")
cli(b.cmd_claim, limit=5, wait=0, settle_ms=0, full=False)
cli(b.cmd_progress, task_id="a3", note=None, stage="跑整套测试",
    notify=False, hold_minutes=30)
t = b.load_task("a3")
check(int(t["execution"].get("hold_until") or 0) > now_ms(),
      "hold-minutes sets hold_until", str(t["execution"]))
ex = dict(t["execution"])
ex["claimed_at"] = now_ms() - 11 * 60_000
ex["heartbeat_at"] = now_ms() - 11 * 60_000
t["execution"] = ex; b.save_task(t)
b._reclaim_stale()
check(b.load_task("a3")["status"] == "executing",
      "leased task not reclaimed while lease runs", "")
ex = dict(b.load_task("a3")["execution"])
ex["hold_until"] = now_ms() - 1_000
t = b.load_task("a3"); t["execution"] = ex; b.save_task(t)
b._reclaim_stale()
check(b.load_task("a3")["status"] == "pending_execution",
      "task reclaimed after lease expires", "")

# --- VM recovery notice: gap + inflight -> one notice; small gap -> none ---
say(b, "a4", "在途任务")
cli(b.cmd_claim, limit=5, wait=0, settle_ms=0, full=False)
before = len(notices(b))
check(b._maybe_vm_recovery_notice({"gap_ms": 600_000}),
      "VM recovery notice created for inflight task after 10-min gap", "")
check(len(notices(b)) == before + 1, "exactly one recovery notice", "")
check(not b._maybe_vm_recovery_notice({"gap_ms": 60_000}),
      "no recovery notice for a normal short restart", "")

# --- outline ---
out = cli(b.cmd_outline, search="auto_progress")
check(out.get("count") == 1 and out["entries"][0]["name"] == "check_auto_progress",
      "outline finds check_auto_progress", str(out))

# --- progress queue position: two pending tasks ---
say(b, "q1", "排队任务一")
say(b, "q2", "排队任务二")
txt = b.progress_command("owner")
check("排队第" in txt and "前面还有" in txt,
      "progress shows queue position", txt[:300])

# --- history direct commands (notices excluded) ---
b.history_append("user", "备份测试原话", kind="text")
check("备份测试原话" in b.history_search_command("备份测试"),
      "history search finds on-disk turn", "")
check("备份测试原话" in b.recent_dialogue_command(),
      "recent dialogue includes on-disk turn", "")

print()
print("ALL ROUND2 CHECKS PASSED" if not fails else f"{len(fails)} FAILED: {fails}")
sys.exit(1 if fails else 0)
