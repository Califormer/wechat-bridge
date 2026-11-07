#!/usr/bin/env python3
"""Behavioural comparison of the old vs new hook script under a stub hook
runtime (wake/silent/log/state are faked; systemctl and cp are stubbed so
nothing touches the host). Run: python3 tests/test_hook.py"""
import json, os, shutil, subprocess, sys, tempfile, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OLD = ROOT / "backups/20261028-030105/hooks/wechat-bridge-inbox.sh"
NEW = ROOT / "hooks/wechat-bridge-inbox.sh"

# wake() exits like the real hook runtime (2026-11-05 audit §8-1: a stub that
# returned let the hook's post-wake woke=1 run here, while live it never ran).
RUNTIME = r'''
hook_state_get() { [[ -f "$HOOK_STATE" ]] && cat "$HOOK_STATE" || echo '{}'; }
hook_state_set() { printf '%s' "$1" > "$HOOK_STATE"; }
log()    { echo "LOG $1" >> "$HOOK_EVENTS"; }
wake()   { echo "WAKE $1 $(printf '%s' "$2" | tr -d ' \n')" >> "$HOOK_EVENTS"; exit 0; }
silent() { echo "SILENT $1" >> "$HOOK_EVENTS"; exit 0; }
'''


class Env:
    def __init__(self, script):
        self.script = script
        self.home = Path(tempfile.mkdtemp(prefix="hook-"))
        self.b = self.home / "workspace/wechat-bridge"
        for d in ("inbox", "state", "ops"):
            (self.b / d).mkdir(parents=True)
        (self.b / "state/credentials.json").write_text("{}")
        (self.b / "ops/backup.sh").write_text(
            "#!/usr/bin/env bash\ntouch \"$(dirname \"$0\")/../BACKUP_RAN\"\n")
        (self.b / "ops/backup.sh").chmod(0o755)
        (self.b / "wechat-bridge.service").write_text("[Unit]\n")
        self.stub = self.home / "stubbin"; self.stub.mkdir()
        import shutil as _sh
        if not _sh.which("jq"):
            (self.stub / "jq").write_text(f"#!/bin/sh\nexec {sys.executable} {ROOT}/tests/jqshim.py \"$@\"\n")
            (self.stub / "jq").chmod(0o755)
        for name in ("systemctl", "cp"):
            (self.stub / name).write_text(f"#!/bin/sh\necho {name} \"$@\" >> '{self.home}/stubcalls'\n")
            (self.stub / name).chmod(0o755)
        (self.home / "runtime.sh").write_text(RUNTIME)
        self.events = self.home / "events"; self.state = self.home / "hookstate.json"
        self.proc = None

    def serve(self, started_ago_ms=600_000, last_poll_ago_ms=1000, streak=0, fail_after_start=0):
        self.proc = subprocess.Popen(["bash", "-c", "exec -a 'python3 bridge.py serve' sleep 300"])
        (self.b / "state/serve.pid").write_text(str(self.proc.pid))
        now = int(time.time() * 1000)
        started = now - started_ago_ms
        hb = {"serve_pid": self.proc.pid, "serve_started_at": started,
              "last_poll_ok_at": now - last_poll_ago_ms if last_poll_ago_ms is not None else 0,
              "poll_fail_streak": streak, "last_poll_fail_at": started + fail_after_start}
        (self.b / "state/heartbeat.json").write_text(json.dumps(hb))

    def task(self, mid, status, **kw):
        t = {"message_id": str(mid), "status": status, "kind": "text", "text": "x",
             "raw_items": [{"type": 1}], "execution": None, "send": None}
        t.update(kw)
        (self.b / "inbox" / f"{mid}.json").write_text(json.dumps(t, indent=2))

    def run(self):
        env = {**os.environ, "HOME": str(self.home), "PATH": f"{self.stub}:{os.environ['PATH']}",
               "HATCH_HOOK_RUNTIME": str(self.home / "runtime.sh"),
               "HOOK_STATE": str(self.state), "HOOK_EVENTS": str(self.events)}
        if self.events.exists():
            self.events.unlink()
        t0 = time.perf_counter()
        r = subprocess.run(["bash", str(self.script)], env=env, capture_output=True, text=True, timeout=120)
        dt = time.perf_counter() - t0
        ev = self.events.read_text().splitlines() if self.events.exists() else []
        return [e for e in ev if not e.startswith("LOG")], dt, r.returncode, r.stderr

    def close(self):
        if self.proc: self.proc.kill()
        shutil.rmtree(self.home, ignore_errors=True)


# The pre-v2 hook only exists in a backup snapshot; without it, old-vs-new
# comparisons are skipped and the current hook is still fully exercised.
HAVE_OLD = OLD.exists()


def scenario(name, fn):
    out = {}
    for label, script in ((("old", OLD),) if HAVE_OLD else ()) + (("new", NEW),):
        e = Env(script)
        try:
            out[label] = fn(e)
        finally:
            e.close()
    return out


fails = []
def check(cond, msg):
    print(("PASS  " if cond else "FAIL  ") + msg)
    if not cond: fails.append(msg)


def wakes(ev): return [e for e in ev if e.startswith("WAKE")]


# S1 healthy / no tasks
def s1(e):
    e.serve(); ev, _, rc, err = e.run(); ev2, *_ = e.run()
    return ev, ev2, rc, (e.b / "BACKUP_RAN").exists(), err
r = scenario("healthy", s1)
check(all(r[k][0] == ["SILENT wechat bridge healthy"] and r[k][2] == 0 for k in r), "healthy, no tasks: silent exit (old & new)")
time.sleep(0.5)
r2 = scenario("backup", lambda e: (e.serve(), e.run(), time.sleep(0.6), (e.b / "BACKUP_RAN").exists(), e.run(), )[3])
check(r2["new"] and not r2.get("old"), "new: hourly backup kicked off by hook (zero tokens); old hook never did")

# S2 pending task + immediate re-tick
def s2(e):
    e.serve(); e.task(101, "pending_execution")
    a, *_ = e.run(); b, *_ = e.run()
    e.task(102, "pending_execution"); c, *_ = e.run()
    return a, b, c
r = scenario("pending", s2)
w = lambda ev: [x.split(" ", 2)[1:] for x in wakes(ev)]
check(w(r["new"][0]) == [["wechat_task", '{"task_ids":["101"],"count":1}']] and (not HAVE_OLD or w(r["new"][0]) == w(r["old"][0])),
      "pending task: identical wake payload on first tick")
check((not HAVE_OLD or len(wakes(r["old"][1])) == 1) and len(wakes(r["new"][1])) == 0,
      "re-tick 5s later: old re-wakes an executor (duplicate LLM run), new throttles")
# v5: 101 was woken seconds ago and is still unclaimed, so the executor that
# is starting claims 102 as well (claim --limit 5) - no second agent. Once
# 101 is claimed, a newcomer is woken at once (tests/test_v5.py, hook B).
v5_hook = "claim_waiter" in NEW.read_text()
check(len(wakes(r["new"][2])) == (0 if v5_hook else 1)
      and (v5_hook or '"102"' in wakes(r["new"][2])[0]),
      "new pending id while the woken executor has not claimed yet: "
      + ("left to it (v5 burst coalescing)" if v5_hook else "woken immediately despite throttle"))

# S3 corrupt file quarantine (incremental path, after a first tick)
def s3(e):
    e.serve(); e.task(1, "sent"); e.run()
    time.sleep(0.05)
    (e.b / "inbox/bad.json").write_text("{oops")
    ev, *_ = e.run()
    return (e.b / "inbox/bad.json.corrupt").exists(), (e.b / "inbox/bad.json").exists(), ev
r = scenario("corrupt", s3)
check(all(r[k][0] and not r[k][1] for k in r) and r["new"][2] == ["SILENT wechat bridge healthy"], "corrupt task JSON quarantined, hook keeps working (old & new)")

# S4 send_uncertain wakes exactly once
def s4(e):
    e.serve(); e.task(5, "send_uncertain"); a, *_ = e.run(); b, *_ = e.run(); return a, b
r = scenario("uncertain", s4)
check(all(len(wakes(r[k][0])) == 1 and "wechat_send_uncertain" in wakes(r[k][0])[0] and not wakes(r[k][1]) for k in r), "send_uncertain: one-time alert (old & new)")

# S5 stale executing is re-woken
def s5(e):
    e.serve(); e.task(7, "executing", execution={"claimed_at": int(time.time() * 1000) - 700_000}); ev, *_ = e.run(); return ev
r = scenario("stale", s5)
check(all(len(wakes(r[k])) == 1 and "wechat_task" in wakes(r[k])[0] for k in r), "executing >10min: re-woken (old & new)")
def s5b(e):
    e.serve(); e.task(8, "executing", execution={"claimed_at": int(time.time() * 1000) - 60_000}); ev, *_ = e.run(); return ev
r = scenario("fresh-exec", s5b)
check(all(not wakes(r[k]) for k in r), "executing <10min: no wake (old & new)")

# S6 startup grace vs dead-on-arrival (exercises the merged heartbeat read)
def s6(e):
    e.serve(started_ago_ms=10_000, last_poll_ago_ms=None); ev, *_ = e.run(); return ev
r = scenario("grace", s6)
check(all(r[k] == ["SILENT wechat bridge starting"] for k in r), "new serve <30s without poll: startup grace (old & new)")
def s7(e):
    e.serve(started_ago_ms=60_000, last_poll_ago_ms=None); ev, *_ = e.run()
    return ev, (e.home / "stubcalls").read_text() if (e.home / "stubcalls").exists() else ""
r = scenario("dead", s7)
check(all(r[k][0] == ["SILENT wechat bridge inline recovery attempted"] and "systemctl restart" in r[k][1] for k in r), "alive but never polled >30s: inline recovery restarts service (old & new)")
def s8(e):
    e.serve(started_ago_ms=60_000, last_poll_ago_ms=None, streak=5, fail_after_start=20_000)
    e.run(); return 0
def s8b(e):
    e.serve(started_ago_ms=20_000, last_poll_ago_ms=None, streak=5, fail_after_start=18_000)
    ev, *_ = e.run(); return ev, (e.home / "stubcalls").read_text() if (e.home / "stubcalls").exists() else ""
r = scenario("fastpath", s8b)
check(all("inline recovery" in r[k][0][0] and "systemctl restart" in r[k][1] for k in r), "poll_fail_streak>=4 over >15s: fast-path recovery before 30s grace (old & new)")

# S9 cost with a month of history
def s9(e):
    e.serve()
    for i in range(3000):
        e.task(10_000 + i, "sent")
    time.sleep(3)                      # history is old, as in production
    a = e.run(); b = e.run()           # first (full validation) + steady state
    return a[1], b[1], a[0], b[0]
r = scenario("bench", s9)
print(f"      hook run with 3000 historical tasks: old {r.get('old', (0, 0))[1]:.2f}s/tick -> new {r['new'][1]:.2f}s/tick "
      f"(first tick incl. full validation {r['new'][0]:.2f}s)")
check(r["new"][2] == ["SILENT wechat bridge healthy"] and r["new"][3] == ["SILENT wechat bridge healthy"], "history-heavy inbox: still healthy/silent")
check(not HAVE_OLD or r["new"][1] < r["old"][1] / 3, "steady-state hook tick >3x cheaper with large history")

# v4 scenarios
def s10(e):
    e.serve(); e.task(11, "send_uncertain", send={"notified": "notice-1"}); ev, *_ = e.run(); return ev
r = scenario("notified", s10)
check(all(not wakes(r[k]) for k in r), "v4: uncertain send the bridge already told the user about: no executor wake")
def s11(e):
    now = int(time.time() * 1000)
    e.serve(); e.task(12, "executing", execution={"claimed_at": now - 700_000, "heartbeat_at": now - 60_000})
    ev, *_ = e.run(); return ev
r = scenario("heartbeat", s11)
check(all(not wakes(r[k]) for k in r), "v4: executing >10 min but a recent progress heartbeat: not re-woken")
def s12(e):
    e.serve(); e.task(13, "send_uncertain"); first, *_ = e.run()
    e.task(13, "sent"); e.task(14, "send_uncertain"); second, *_ = e.run()
    return json.loads(e.state.read_text()).get("alerted_uncertain"), wakes(first), wakes(second)
r = scenario("prune", s12)
check(r["new"][0] == ["14"] and len(r["new"][1]) == 1 and len(r["new"][2]) == 1,
      "v4: alerted-uncertain list keeps only tasks still uncertain (no unbounded growth)")

print("\nALL HOOK CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED: {fails}")
sys.exit(1 if fails else 0)
