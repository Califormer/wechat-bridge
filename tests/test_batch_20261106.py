#!/usr/bin/env python3
"""2026-11-06 features, offline (stdlib only; the hook checks use tests/jqshim.py
when no jq is installed):
  1 batching   - state/pending.json: a burst of 5 messages -> one dispatch, one
                 claim, one round, one reply; a single message is dispatched 3 s
                 after it arrived (not 60); the 60 s cap dispatches a batch the
                 user never pauses in; boundary cases lose nothing (same-instant
                 dispatch/arrival both ways, a concurrent stress run, a lost
                 pending.json, a restart); one coalescing rule only (no claim
                 window, one batch per claim, limit never splits a batch).
  2 supersede  - A claimed, B arrives: A's complete / progress / heartbeat /
                 send-media exit 4 (real CLI processes) and send nothing; B's
                 round gets A+B and one reply goes out. Hook: no wake while a
                 batch is open, one wake per dispatch, and a superseded CLAIMED
                 task is woken for at once (wake_key). Batch = one unit
                 (heartbeat, lease, auto-progress). Commands and leased long
                 steps do not supersede.
  3 outbox     - state/outbox.json: a failed text is listed and resent within
                 3 s of the user's next message (serve's sender thread running);
                 a text that keeps failing is retried at most 3 times by
                 messages, under the original policy, without repeated notices;
                 maybe-delivered texts are never listed; strictly apart from the
                 media queue outbox/ (paths, code, behaviour); not in backups.
Run: python3 tests/test_batch_20261106.py
"""
import contextlib, importlib.util, inspect, io, json, os, random, shutil, subprocess, sys
import tempfile, threading, time
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
fails = []
_n = [0]
_made = []
_procs = []


def check(cond, name, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name + (f"\n      {detail}" if detail and not cond else ""))
    if not cond:
        fails.append(name)


def load(tag, home=None):
    """A private bridge copy in <tmp>/wb-batch-<tag>-XXXXXXXX/workspace/wechat-bridge
    (the layout the hook script expects), imported as a module. home= imports
    the same copy again: a restarted process."""
    _n[0] += 1
    if home is None:
        home = Path(tempfile.mkdtemp(prefix=f"wb-batch-{tag}-"))
        _made.append(home)
        d = home / "workspace" / "wechat-bridge"
        d.mkdir(parents=True)
        shutil.copy(HERE / "bridge.py", d / "bridge.py")
    d = home / "workspace" / "wechat-bridge"
    spec = importlib.util.spec_from_file_location(f"bb_{tag}_{_n[0]}", d / "bridge.py")
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b.LOGS = []
    b.log = lambda msg: b.LOGS.append(msg)
    for p in (b.STATE_DIR, b.INBOX_DIR, b.MEDIA_DIR, b.FILES_DIR, b.HISTORY_DIR,
              b.QUEUE_DIR, b.ATTACH_DIR):
        p.mkdir(parents=True, exist_ok=True)
    if not b.CREDS_FILE.exists():
        # loopback port 9 (discard): anything that really tried the network fails
        b.atomic_write_json(b.CREDS_FILE, {"bot_token": "T", "ilink_bot_id": "bot",
                                           "ilink_user_id": "owner",
                                           "baseurl": "http://127.0.0.1:9"})
    b.HOME = home
    return b


class Fake:
    """Replaces send_text (one sendmessage). fail = an ApiError to raise."""

    def __init__(self, b):
        self.sent, self.calls, self.fail = [], 0, None
        self.lock = threading.Lock()
        b.send_text = self.send

    def send(self, creds, to_user_id, text, context_token, client_id=None):
        with self.lock:
            self.calls += 1
            if self.fail is not None:
                raise self.fail
            self.sent.append(text)
            return {"ret": 0, "message_id": f"srv-{len(self.sent)}", "_client_id": client_id}


def say(b, mid, text, tok=None):
    b.ingest_message({"ilink_user_id": "owner"}, {
        "message_id": mid, "from_user_id": "owner", "message_type": 1,
        "context_token": tok or f"T-{mid}",
        "item_list": [{"type": 1, "text_item": {"text": text}}]})
    return b.load_task(mid)


def cli(fn, **kw):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = fn(SimpleNamespace(**kw))
    raw = buf.getvalue().strip()
    try:
        return rc, json.loads(raw) if raw else {}        # status prints indented JSON
    except ValueError:
        return rc, json.loads(raw.splitlines()[-1])


def complete(b, tid, **kw):
    a = dict(task_id=tid, result=None, result_file=None, error=None, attach=[], attach_as="auto",
             wait_attachments=0, force=False, merge_ids=None, raw=False, claim_id=None,
             then_wait=0, then_limit=5)
    a.update(kw)
    return cli(b.cmd_complete, **a)


def run_cli(b, *args):
    """The real CLI in its own process (real exit code), on the same state."""
    env = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
    r = subprocess.run([sys.executable, str(b.BASE_DIR / "bridge.py"), *args], cwd=b.BASE_DIR,
                       env=env, capture_output=True, text=True, timeout=60)
    lines = r.stdout.strip().splitlines()
    return r.returncode, (json.loads(lines[-1]) if lines else {}), r.stderr[-300:]


def start_thread(b, target):
    th = threading.Thread(target=target, daemon=True)
    th.start()
    return th


def stop_threads(b):
    b._stop.set()
    for ev in (b._batch_wake, b._send_wake):
        ev.set()


def wait_for(pred, secs, step=0.02):
    end = time.monotonic() + secs
    while time.monotonic() < end:
        if pred():
            return True
        time.sleep(step)
    return pred()


def notices(b):
    return [x for x in b.scan_task_index() if x.get("kind") == "notice"]


def outbox(b):
    return (b.read_json(b.TEXT_OUTBOX_FILE, {}) or {}).get("texts") or []


def clocked(b, t0=1_800_000_000_000):
    clock = {"t": t0}
    b.now_ms = lambda: clock["t"]
    return clock


# ------------------------------------------------------------------ hook harness
RUNTIME = r'''
hook_state_get() { [[ -f "$HOOK_STATE" ]] && cat "$HOOK_STATE" || echo '{}'; }
hook_state_set() { printf '%s' "$1" > "$HOOK_STATE"; }
log()    { echo "LOG $1" >> "$HOOK_EVENTS"; }
wake()   { echo "WAKE $1 $(printf '%s' "$2" | tr -d ' \n')" >> "$HOOK_EVENTS"; exit 0; }
silent() { echo "SILENT $1" >> "$HOOK_EVENTS"; exit 0; }
'''


def hook_env(b):
    home = b.HOME
    stub = home / "stub"
    stub.mkdir(exist_ok=True)
    if not shutil.which("jq"):
        (stub / "jq").write_text(f"#!/bin/sh\nexec {sys.executable} {HERE}/tests/jqshim.py \"$@\"\n")
        (stub / "jq").chmod(0o755)
    for name in ("systemctl", "cp"):
        (stub / name).write_text("#!/bin/sh\nexit 0\n")
        (stub / name).chmod(0o755)
    (home / "runtime.sh").write_text(RUNTIME)
    ops = b.BASE_DIR / "ops"
    ops.mkdir(exist_ok=True)
    (ops / "backup.sh").write_text("#!/usr/bin/env bash\n")
    (ops / "backup.sh").chmod(0o755)
    proc = subprocess.Popen(["bash", "-c", "exec -a 'python3 bridge.py serve' sleep 300"])
    _procs.append(proc)
    b.PID_FILE.write_text(str(proc.pid))
    return {**os.environ, "HOME": str(home), "PATH": f"{stub}:{os.environ['PATH']}",
            "HATCH_HOOK_RUNTIME": str(home / "runtime.sh"), "HOOK_STATE": str(home / "hookstate.json"),
            "HOOK_EVENTS": str(home / "events")}


def hook_healthy(b):
    now = int(time.time() * 1000)
    pid = int(b.PID_FILE.read_text())
    b.update_heartbeat(serve_pid=pid, serve_started_at=now - 600_000, last_poll_ok_at=now - 500)


def hook_run(b, env):
    hook_healthy(b)
    ev = Path(env["HOOK_EVENTS"])
    if ev.exists():
        ev.unlink()
    subprocess.run(["bash", str(HERE / "hooks/wechat-bridge-inbox.sh")], env=env,
                   capture_output=True, timeout=60)
    return [e for e in (ev.read_text().splitlines() if ev.exists() else []) if not e.startswith("LOG")]


def wakes(events):
    return [e for e in events if e.startswith("WAKE wechat_task")]


# ================================================================== 1 batching
def t_burst_one_round():
    b = load("burst")
    f = Fake(b)
    b.batching_on()
    start_thread(b, b.batch_loop)
    res = {}

    def executor():                  # a warm executor waiting for the next message
        res["out"] = b.claim_tasks(limit=5, wait_s=20)
        res["t"] = time.monotonic()
    th = start_thread(b, executor)
    time.sleep(0.3)
    ids = [f"m{i}" for i in range(1, 6)]
    held = []
    for i, mid in enumerate(ids):
        say(b, mid, f"第 {i + 1} 句")
        held.append(b._dispatch_group(5)[0] == [] and b.load_task(mid).get("batch_open") is True)
        if i < 4:
            time.sleep(0.4)
    t_last = time.monotonic()
    pj = b.batch_peek()
    th.join(20)
    out = res.get("out") or {}
    got = [x["message_id"] for x in out.get("tasks") or []]
    lag = res.get("t", 0) - t_last
    check(all(held) and len(pj) == 1 and [x["id"] for x in pj[0]["items"]] == ids,
          "5 quick messages wait together in ONE batch of state/pending.json; none is claimable meanwhile",
          json.dumps(pj, ensure_ascii=False)[:300])
    check(got == ids and "merge_hint" in out and out.get("batch_id") == pj[0]["id"] and 2.9 <= lag < 4.5,
          f"the batch is dispatched once, {lag:.2f}s after the last message (3 s pause), and ONE claim "
          "returns all 5 with one merge_hint", json.dumps(out, ensure_ascii=False)[:300])
    rc, res2 = complete(b, ids[-1], result="五句一起回答。", merge_ids=",".join(ids[:-1]),
                        claim_id=out.get("claim_id"))
    again = b.claim_tasks(limit=5, wait_s=0)
    check(rc == 0 and res2.get("merged") == ids[:-1] and len(f.sent) == 1
          and all(b.load_task(m)["status"] == "sent" for m in ids) and again["tasks"] == []
          and not b.batch_peek(),
          "agent runs ONE round: one complete --merge-ids, one reply sent, nothing left for a second round")
    disp = [m for m in b.LOGS if " dispatched: " in m]
    check(len(disp) == 1 and "5 message(s)" in disp[0] and "reason=quiet" in disp[0],
          "bridge log: one dispatch of 5 messages, reason=quiet", str(disp))
    stop_threads(b)


def t_single_after_3s():
    b = load("single")
    b.batching_on()
    start_thread(b, b.batch_loop)
    say(b, "s1", "在吗？")
    t0 = time.monotonic()
    time.sleep(2.6)
    held = b.load_task("s1").get("batch_open") is True and b._dispatch_group(5)[0] == []
    out = b.claim_tasks(limit=5, wait_s=0)         # a cold claim meanwhile waits for the dispatch
    dt = time.monotonic() - t0
    t = b.load_task("s1")
    lag = int(t.get("dispatched_at") or 0) - int(t.get("received_at") or 0)
    check(held and [x["message_id"] for x in out["tasks"]] == ["s1"] and 3000 <= lag < 3400
          and dt < 4.0 and "merge_hint" not in out and t.get("pending_at") == t.get("dispatched_at"),
          f"a single message is held 3 s, dispatched {lag} ms after it arrived (not at 60 s); a claim "
          f"started meanwhile gets it {dt:.2f}s after the message; pending_at = dispatch time")
    stop_threads(b)


def t_cap_60s():
    b = load("cap")
    b.batching_on()
    clock = clocked(b)
    t0 = clock["t"]
    ids, early = [], []
    for i in range(30):                    # one message every 2 s for 58 s: never a 3 s pause
        clock["t"] = t0 + i * 2000
        ids.append(f"c{i:02d}")
        say(b, ids[-1], f"第 {i} 句")
        clock["t"] += 1999                 # just before the next message: quiet not reached
        early += b.batch_flush_due()
    clock["t"] = t0 + 59_999
    before = b.batch_flush_due()
    clock["t"] = t0 + 60_000
    at_cap = b.batch_flush_due()
    out = b.claim_tasks(limit=5)
    check(early == [] and before == [] and len(at_cap) == 1 and at_cap[0]["reason"] == "max"
          and at_cap[0]["held_ms"] == 60_000 and at_cap[0]["ids"] == ids
          and [x["message_id"] for x in out["tasks"]] == ids,
          "60 s cap: a user who never pauses 3 s still gets the batch dispatched at exactly 60 s after "
          "its first message, all 30 messages in it, one claim", str(at_cap)[:300])
    clock["t"] = t0 + 60_500
    say(b, "c30", "又一句")
    nb = b.batch_peek()
    check(len(nb) == 1 and nb[0]["opened_at"] == t0 + 60_500 and nb[0]["due_at"] == t0 + 63_500
          and len(nb[0]["items"]) == 31,
          "after the cap a new message opens a new batch whose 3 s / 60 s count from IT (the 30 "
          "unanswered ones are carried into it, they do not shorten its window)",
          json.dumps(nb, ensure_ascii=False)[:300])


def t_boundary_same_instant():
    for order in ("dispatch-first", "message-first"):
        b = load("edge")
        b.batching_on()
        clock = clocked(b)
        t0 = clock["t"]
        say(b, "e1", "第一句")
        clock["t"] = t0 + 3000              # exactly e1's dispatch instant
        if order == "dispatch-first":
            first = b.batch_flush_due()
            say(b, "e2", "第二句")
        else:
            say(b, "e2", "第二句")
            first = b.batch_flush_due()
        clock["t"] = t0 + 6000
        b.batch_flush_due()
        out = b.claim_tasks(limit=5)
        got = [x["message_id"] for x in out["tasks"]]
        check(got == ["e1", "e2"] and "merge_hint" in out and not b.batch_peek()
              and len(first) == (1 if order == "dispatch-first" else 0)
              and all(b.load_task(m)["status"] == "executing" for m in ("e1", "e2")),
              f"boundary ({order}): a message arriving at the very dispatch instant is either in that "
              "batch or opens the next one (which takes the still-unclaimed first along) - both end up "
              "in one claim, nothing lost or doubled", f"{got} first={first}")


def t_boundary_stress():
    """Real threads (batch thread, a user, an executor that answers each claim
    at once), windows scaled down so there are dozens of dispatch boundaries
    and supersedes in a few seconds. Two seeds. Found a real race during
    development (a claim taking a message a new arrival had just pulled back
    into an open batch); any exit code other than 0 / 4 now fails."""
    for seed in (20261106, 7):
        b = load("stress")
        f = Fake(b)
        b.batching_on()
        b.BATCH_QUIET_MS, b.BATCH_MAX_MS = 60, 400
        start_thread(b, b.batch_loop)
        n = 120
        rnd = random.Random(seed)
        gaps = [rnd.choice((0, 5, 20, 50, 58, 60, 62, 70, 120)) / 1000 for _ in range(n)]
        done = threading.Event()

        def user():
            for i in range(n):
                say(b, f"x{i:03d}", f"消息 {i}")
                time.sleep(gaps[i])
            done.set()
        th = start_thread(b, user)
        covered, refused, other, rounds = [], 0, [], 0
        end = time.monotonic() + 90
        while time.monotonic() < end:
            out = b.claim_tasks(limit=5, wait_s=0.3)
            ids = [x["message_id"] for x in out["tasks"]]
            if not ids:
                if done.is_set() and not b.batch_peek() and not any(
                        x.get("status") in ("pending_execution", "executing") for x in b.scan_task_index()):
                    break
                continue
            rounds += 1
            rc, res = complete(b, ids[-1], result=f"第 {rounds} 轮的回复", merge_ids=",".join(ids[:-1]),
                               claim_id=out.get("claim_id"))
            if rc == b.EXIT_SUPERSEDED:
                refused += 1
            elif rc == 0:
                covered.append([ids[-1]] + list(res.get("merged") or []))
            else:
                other.append((rc, res.get("error")))
        th.join(30)
        flat = [m for c in covered for m in c]
        lost = [f"x{i:03d}" for i in range(n) if b.load_task(f"x{i:03d}")["status"] != "sent"]
        check(not lost and not other and sorted(flat) == [f"x{i:03d}" for i in range(n)]
              and len(f.sent) == len(covered) and refused > 0,
              f"stress (seed {seed}; 3 s/60 s scaled to 60/400 ms): {n} messages, {rounds} rounds, "
              f"{refused} superseded mid-round (exit 4), {len(covered)} replies - every message answered by "
              "exactly one reply, none lost, superseded rounds sent nothing, no other exit code",
              f"lost={lost[:5]} other={other[:3]} dup={len(flat) - len(set(flat))}")
        stop_threads(b)


def t_recover_and_restart():
    b = load("recover")
    b.batching_on()
    say(b, "r1", "第一句")
    b2 = load("recover", home=b.HOME)               # serve restarted while the batch was open
    early = b2.batch_flush_due()
    time.sleep(3.1)
    late = b2.batch_flush_due()
    check(early == [] and len(late) == 1 and late[0]["ids"] == ["r1"]
          and not b2.load_task("r1").get("batch_open"),
          "restart: the open batch survives in state/pending.json and the new process dispatches it when due")
    say(b, "r2", "第二句")                          # r1 (dispatched, unclaimed) joins r2's batch
    b.PENDING_BATCH_FILE.unlink()                   # crash between the two writes / file lost
    n = b.batch_recover()
    out = b.claim_tasks(limit=5)
    check(n == 2 and [x["message_id"] for x in out["tasks"]] == ["r1", "r2"],
          "held messages whose batch is gone are dispatched by batch_recover - nothing stays stuck")


def t_one_rule():
    b = load("rule")
    gone = [w for w in ("_settle", "_reads_complete", "_settled_pending", "_pending_now",
                        "CLAIM_SETTLE_MS", "CLAIM_SETTLE_SHORT_MS", "CLAIM_SETTLE_CAP_MS",
                        "_SENTENCE_ENDS") if hasattr(b, w)]
    check(not gone, "one coalescing rule: the claim-side burst window (settle functions and constants) "
          "is gone from bridge.py", str(gone))
    now = b.now_ms()

    def mk(mid, bid, age):
        b.save_task({"message_id": mid, "from_user_id": "owner", "kind": "text", "text": mid,
                     "status": "pending_execution", "received_at": now - age, "pending_at": now,
                     "execution": None, "send": None, "context_token": "CT", "batch_id": bid,
                     "dispatched_at": now})
    for i, mid in enumerate(("a1", "a2")):
        mk(mid, "bA", 9000 - i)
    mk("y1", "bB", 5000)
    for i in range(7):
        mk(f"z{i}", "bC", 4000 - i)
    o1 = b.claim_tasks(limit=5)
    o2 = b.claim_tasks(limit=5)
    t0 = time.monotonic()
    o3 = b.claim_tasks(limit=5, wait_s=0, settle_ms=5000)
    dt = time.monotonic() - t0
    ids = lambda o: [x["message_id"] for x in o["tasks"]]  # noqa: E731
    check(ids(o1) == ["a1", "a2"] and "merge_hint" in o1 and o1.get("batch_id") == "bA"
          and ids(o2) == ["y1"] and "merge_hint" not in o2,
          "a claim returns exactly one batch: merge_hint means 'this claim is one batch', never a mix")
    check(ids(o3) == [f"z{i}" for i in range(7)] and dt < 0.5,
          f"--limit 5 never splits a batch of 7; --settle-ms is ignored (claimed in {dt:.2f}s)")


def t_off_switch():
    b = load("off")                                  # not serve: batching not turned on
    say(b, "f1", "你好")
    t = b.load_task("f1")
    check(not t.get("batch_open") and not b.PENDING_BATCH_FILE.exists()
          and b._dispatch_group(5)[0] == ["f1"],
          "batching off (any process but serve, or WECHAT_BRIDGE_BATCH=0): claimable at once, no pending.json")
    os.environ["WECHAT_BRIDGE_BATCH"] = "0"
    try:
        b0 = load("off0")
    finally:
        os.environ.pop("WECHAT_BRIDGE_BATCH", None)
    check(b0.BATCH_ENABLED is False and b.BATCH_ENABLED is True and b.BATCH_QUIET_MS == 3000
          and b.BATCH_MAX_MS == 60_000,
          "WECHAT_BRIDGE_BATCH=0 is the switch serve reads; defaults 3000 ms / 60000 ms")


# ================================================================== 2 supersede
def t_supersede_exit4():
    b = load("sup")
    f = Fake(b)
    b.batching_on()
    say(b, "A", "帮我查一下明天的天气")
    b.batch_flush_due(everything=True)
    first = b.claim_tasks(limit=5)
    c1 = first.get("claim_id")
    say(b, "B", "是上海的")                          # while A is being worked on
    ta = b.load_task("A")
    exa = ta.get("execution") or {}
    check([x["message_id"] for x in first["tasks"]] == ["A"] and ta["status"] == "pending_execution"
          and ta.get("batch_open") and c1 in (exa.get("superseded_claim_ids") or [])
          and ta.get("wake_key") == "A~1" and exa.get("superseded_by") == "B",
          "B arrives while A is claimed: A goes back into B's batch, its claim is recorded as superseded")
    att = b.BASE_DIR / "draft.txt"
    att.write_text("草稿\n")
    rc_c, oc, err = run_cli(b, "complete", "--task-id", "A", "--claim-id", c1, "--result", "明天晴（旧回复）",
                            "--attach", str(att))
    rc_p, op, _ = run_cli(b, "progress", "--task-id", "A", "--claim-id", c1, "--stage", "查天气",
                          "--note", "在查", "--notify")
    rc_h, oh, _ = run_cli(b, "heartbeat", "--task-id", "A", "--claim-id", c1)
    rc_m, om, _ = run_cli(b, "send-media", "--task-id", "A", "--claim-id", c1, "--file", str(att),
                          "--wait", "0")
    rc_n, on_, _ = run_cli(b, "complete", "--task-id", "A", "--result", "不带 claim id 也不行")
    rc_f, of, _ = run_cli(b, "complete", "--task-id", "A", "--claim-id", c1, "--result", "x", "--force")
    check([rc_c, rc_p, rc_h, rc_m, rc_n, rc_f] == [4] * 6
          and all(o.get("superseded") is True and o.get("exit_code") == 4 for o in (oc, op, oh, om, on_, of)),
          "A's executor: complete / progress --notify / heartbeat / send-media / complete without "
          "--claim-id / complete --force all exit 4 (SUPERSEDED), as real processes",
          f"rcs={[rc_c, rc_p, rc_h, rc_m, rc_n, rc_f]} {err}")
    ta = b.load_task("A")
    check(f.calls == 0 and not notices(b) and not b.list_queue() and ta.get("result_text") is None
          and ta.get("status") == "pending_execution",
          "...and nothing at all was sent or queued for A: no text, no progress note, no file")
    time.sleep(3.1)
    second = b.claim_tasks(limit=5)
    c2 = second.get("claim_id")
    view = {x["message_id"]: x for x in second["tasks"]}
    check(list(view) == ["A", "B"] and "merge_hint" in second and "A" in second["merge_hint"].split("其中")[-1]
          and "作废" in view["A"].get("superseded", "") and "resume_from" not in view["A"] and c2 != c1,
          "B is processed normally (batch): the next claim returns A+B with one merge_hint that says A's "
          "earlier reply was dropped", json.dumps(second, ensure_ascii=False)[:300])
    rc, res = complete(b, "B", result="上海明天晴，最高 18 度。", merge_ids="A", claim_id=c2)
    rc_late, ol, _ = run_cli(b, "complete", "--task-id", "A", "--claim-id", c1, "--result", "迟到的旧回复")
    check(rc == 0 and res.get("merged") == ["A"] and f.sent == ["上海明天晴，最高 18 度。"]
          and b.load_task("A")["status"] == "sent" and rc_late == 4,
          "B gets its reply: ONE message for A+B; A's old executor still gets exit 4 afterwards")


def t_supersede_bounds():
    b = load("supb")
    Fake(b)
    b.batching_on()
    say(b, "L", "跑一遍全套测试")
    b.batch_flush_due(everything=True)
    o = b.claim_tasks(limit=5)
    rc, _ = cli(b.cmd_progress, task_id="L", note=None, stage="跑整套测试", notify=False,
                hold_minutes=20, claim_id=o.get("claim_id"))
    say(b, "L2", "顺便问一下几点了")
    c = say(b, "L3", "进度")
    tl = b.load_task("L")
    check(rc == 0 and tl["status"] == "executing" and not tl.get("batch_open")
          and c["kind"] == "command" and b.load_task("L2").get("batch_open")
          and [x["id"] for x in b.batch_peek()[0]["items"]] == ["L2"],
          "not superseded: a claim holding a long-step lease (progress --hold-minutes); a command "
          "(「进度」) supersedes nothing and is not batched")


def t_hook_batch():
    b = load("hook")
    b.batching_on()
    env = hook_env(b)
    say(b, "h1", "第一句")
    ev1 = hook_run(b, env)
    b.batch_flush_due(everything=True)
    ev2 = hook_run(b, env)
    o = b.claim_tasks(limit=5)
    say(b, "h2", "补充一句")
    ev3 = hook_run(b, env)
    b.batch_flush_due(everything=True)
    ev4 = hook_run(b, env)
    check(wakes(ev1) == [] and wakes(ev2) == ['WAKE wechat_task {"task_ids":["h1"],"count":1}']
          and o["tasks"] and wakes(ev3) == []
          and wakes(ev4) == ['WAKE wechat_task {"task_ids":["h1~1","h2"],"count":2}'],
          "hook: no wake while the batch is open, one wake per dispatch; a superseded CLAIMED message "
          "is woken for at once (wake_key h1~1), not held back by the 45 s rule", str([ev1, ev2, ev3, ev4]))
    c = load("hookc")
    c.batching_on()
    env = hook_env(c)
    say(c, "k1", "第一句")
    c.batch_flush_due(everything=True)
    w1 = hook_run(c, env)                             # an executor is woken for k1, still starting
    say(c, "k2", "第二句")                             # k1 unclaimed: carried, same key
    c.batch_flush_due(everything=True)
    w2 = hook_run(c, env)
    out = c.claim_tasks(limit=5)                      # the executor woken for k1 claims
    check(wakes(w1) and wakes(w2) == [] and [x["message_id"] for x in out["tasks"]] == ["k1", "k2"],
          "control: a superseded message nobody had claimed keeps its key - the executor already woken "
          "for it takes the new batch, no second agent", str([w1, w2]))
    cl = load("hookd")
    cl.batching_on()
    env = hook_env(cl)
    (cl.STATE_DIR / "tuning.env").write_text("WECHAT_BRIDGE_HOOK_LINGER_MS=3000\n")
    say(cl, "g1", "在吗")
    hook_healthy(cl)
    h = subprocess.Popen(["bash", str(HERE / "hooks/wechat-bridge-inbox.sh")], env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(0.6)
    t0 = time.monotonic()
    cl.batch_flush_due(everything=True)               # dispatch bumps .inbox_signal
    h.wait(30)
    took = time.monotonic() - t0
    ev = Path(env["HOOK_EVENTS"]).read_text().splitlines() if Path(env["HOOK_EVENTS"]).exists() else []
    check(any(e.startswith("WAKE wechat_task") for e in ev) and took < 1.5,
          f"a lingering hook wakes at the batch DISPATCH ({took:.2f}s) - the arrival itself no longer "
          "signals (so linger reruns stay = linger wakes)", str(ev))


def t_batch_is_one_unit():
    b = load("unit")
    Fake(b)
    b.batching_on()
    for i, mid in enumerate(("u1", "u2", "u3")):
        say(b, mid, f"第 {i} 句")
    b.batch_flush_due(everything=True)
    o = b.claim_tasks(limit=5)
    cid = o["claim_id"]
    with b.task_lock():
        for mid in ("u1", "u2", "u3"):
            t = b.load_task(mid)
            t["execution"]["claimed_at"] -= 11 * 60_000
            b.save_task(t)
    rc, _ = cli(b.cmd_heartbeat, task_id="u3", stage="写回复", claim_id=cid)
    b._reclaim_stale()
    hb = {m: (b.load_task(m)["execution"] or {}).get("heartbeat_at") for m in ("u1", "u2", "u3")}
    rc2, _ = cli(b.cmd_progress, task_id="u3", note=None, stage="长步骤", notify=False, hold_minutes=20,
                 claim_id=cid)
    leases = {m: (b.load_task(m)["execution"] or {}).get("hold_until") for m in ("u1", "u2", "u3")}
    check(rc == 0 and rc2 == 0 and all(b.load_task(m)["status"] == "executing" for m in hb)
          and len(set(hb.values())) == 1 and None not in hb.values() and len(set(leases.values())) == 1
          and None not in leases.values(),
          "a batch is one unit: a heartbeat / lease on its last task covers the whole claim, so 11 min "
          "later none of it is reclaimed (before: the others would be re-run - a duplicate reply)")
    v = load("unitv")
    Fake(v)
    v.batching_on()
    for i, mid in enumerate(("v1", "v2", "v3")):
        say(v, mid, f"第 {i} 句")
    v.batch_flush_due(everything=True)
    v.claim_tasks(limit=5)
    with v.task_lock():
        for mid in ("v1", "v2", "v3"):
            t = v.load_task(mid)
            t["execution"]["claimed_at"] -= 200_000
            v.save_task(t)
    v.check_auto_progress(v.scan_task_index())
    auto = [x for x in notices(v)]
    check(len(auto) == 1, "auto-progress: one note per batch (one round), not one per message", str(auto))


# ================================================================== 3 text outbox
def t_outbox_resend_within_3s():
    b = load("obx")
    f = Fake(b)
    start_thread(b, b.sender_loop)                    # serve's sender thread
    say(b, "o1", "帮我总结一下这份文档")
    o = b.claim_tasks(limit=5)
    f.fail = b.ApiError("connect", "connection refused")
    rc, res = complete(b, "o1", result="总结：第一，……第二，……", claim_id=o.get("claim_id"))
    t = b.load_task("o1")
    ob = outbox(b)
    listed = (rc == 0 and t["status"] == "send_failed" and len(ob) == 1 and ob[0]["task_id"] == "o1"
              and ob[0]["state"] == "retrying" and ob[0]["text"].startswith("总结：") and ob[0]["user"] == "owner")
    check(listed, "a reply that cannot be sent is listed in state/outbox.json (state, text, user)",
          json.dumps(ob, ensure_ascii=False)[:300])
    f.fail = None
    t0 = time.monotonic()
    say(b, "o2", "在吗")
    ok = wait_for(lambda: any(x.startswith("总结：") for x in f.sent), 3.0)
    dt = time.monotonic() - t0
    wait_for(lambda: not outbox(b), 2.0)
    check(ok and dt < 3.0 and b.load_task("o1")["status"] == "sent" and outbox(b) == [],
          f"the user's next message: the failed text goes out {dt:.2f}s later (< 3 s, before its own "
          "retry was due) and leaves the outbox")
    stop_threads(b)


def t_outbox_bounded():
    b = load("obb")
    f = Fake(b)
    say(b, "o1", "写个总结")
    o = b.claim_tasks(limit=5)
    f.fail = b.ApiError("business", "ret=-1", code=-1)
    complete(b, "o1", result="总结正文", claim_id=o.get("claim_id"))
    for _ in range(2):                                # the original retries (60 s, 180 s) run out
        t = b.load_task("o1")
        t["send"]["next_retry_at"] = 0
        b.save_task(t)
        b.flush_outbox()
    t = b.load_task("o1")
    n_notes = len(notices(b))
    ob = outbox(b)
    check(t["status"] == "send_uncertain" and t["send"].get("exhausted") and n_notes == 1
          and len(ob) == 1 and ob[0]["state"] == "exhausted",
          "out of retries: send_uncertain(exhausted) + one notice as before, and listed as exhausted")
    b.flush_outbox()                                  # the notice's own first try (also fails)
    attempts = []
    for k in range(1, 5):                             # the link stays broken, the user keeps writing
        say(b, f"n{k}", "？")
        b.flush_outbox()
        attempts.append(int(b.load_task("o1")["send"].get("attempts") or 0))
    t = b.load_task("o1")
    ob = outbox(b)
    check(attempts == [4, 5, 6, 6] and t["status"] == "send_uncertain" and t["send"].get("exhausted")
          and len(notices(b)) == n_notes and t["send"].get("notified") and ob and ob[0]["auto_resends"] == 3,
          "still failing: kept in the outbox, ONE attempt per message under the same policy (attempt "
          "count goes on, exhausted again), at most 3 messages, no repeated notice - no resend storm",
          f"attempts={attempts} notices={len(notices(b))} outbox={ob[:1]}")
    f.fail = None
    c = say(b, "n5", "重发")
    b.flush_outbox()
    check(c["kind"] == "command" and b.load_task("o1")["status"] == "sent" and "总结正文" in f.sent
          and outbox(b) == [],
          "「重发」 (a human decision) still resends it after the automatic cap; delivered -> out of the outbox")


def t_outbox_scope():
    b = load("obs")
    f = Fake(b)
    say(b, "u1", "问题一")
    o = b.claim_tasks(limit=5)
    f.fail = b.ApiError("timeout", "read timed out")
    complete(b, "u1", result="可能已经送达的回复", claim_id=o.get("claim_id"))
    t = b.load_task("u1")
    f.fail = None
    say(b, "u2", "？")
    b.flush_outbox()
    t2 = b.load_task("u1")
    check(t["status"] == "send_uncertain" and not t["send"].get("exhausted") and outbox(b) == []
          and t2["status"] == "send_uncertain" and "可能已经送达的回复" not in f.sent
          and t2["send"].get("attempts") == t["send"].get("attempts"),
          "maybe-delivered (timeout mid-send): never listed, never resent automatically (duplicate risk)",
          f"{t['status']} {t['send']} outbox={outbox(b)}")
    q = load("obq", )
    fq = Fake(q)
    big = "\n\n".join(f"第{i:02d}段 " + "内容" * 900 for i in range(24))
    say(q, "p1", "写一篇长文", "T1")
    oq = q.claim_tasks(limit=5)
    q.CTX_QUOTA = 10
    complete(q, "p1", result=big, claim_id=oq.get("claim_id"))
    parked = outbox(q)
    say(q, "p2", "继续", "T2")
    q.flush_outbox()
    check(parked and parked[0]["state"] == "parked" and q.load_task("p1")["status"] == "sent"
          and outbox(q) == [] and len(fq.sent) >= 11,
          "a reply parked by the reply budget is listed as parked and resumes on the next message "
          "(as before), then leaves the outbox", json.dumps(parked, ensure_ascii=False)[:200])


def t_outbox_separate_from_media_queue():
    b = load("obm")
    check(b.TEXT_OUTBOX_FILE == b.STATE_DIR / "outbox.json" and b.OUTBOX_DIR == b.BASE_DIR / "outbox"
          and b.OUTBOX_DIR not in b.TEXT_OUTBOX_FILE.parents and b.TEXT_OUTBOX_FILE.parent != b.QUEUE_DIR,
          "paths: texts in state/outbox.json, files in outbox/queue/ - different directories")
    text_fns = [b.text_outbox_resend, b.text_outbox_sync, b.text_outbox_reconcile, b._text_outbox_entry,
                b._text_outbox_state, b._text_outbox_load, b._text_outbox_save, b.text_outbox_summary]
    media_fns = [b.enqueue_attachment, b.process_queue_item, b._queue_failure, b._finish_qitem_raw,
                 b.load_qitem, b.save_qitem, b.list_queue, b.prune_queue, b.recover_queue, b._park_qitem,
                 b._mark_qitem_for_resend, b.queue_worker_loop, b.process_due_queue]
    t_bad = [fn.__name__ for fn in text_fns if any(w in inspect.getsource(fn) for w in (
        "OUTBOX_DIR", "QUEUE_DIR", "ATTACH_DIR", "qitem", "list_queue"))]
    m_bad = [fn.__name__ for fn in media_fns if any(w in inspect.getsource(fn) for w in (
        "TEXT_OUTBOX", "text_outbox", "outbox.json"))]
    check(not t_bad and not m_bad,
          "code: no text-outbox function touches the media queue, no media-queue function touches outbox.json",
          f"text->media {t_bad}, media->text {m_bad}")
    p = b.BASE_DIR / "song.mp3"
    p.write_bytes(b"ID3" + os.urandom(2000))
    it = b.enqueue_attachment("owner", p, kind="file", notify="background")
    with b.task_lock():
        cur = b.load_qitem(it["qid"])
        cur.update(status="failed", final_error="CDN upload failed")
        b.save_qitem(cur)
    say(b, "m1", "文件呢")
    b.text_outbox_sync()
    check(outbox(b) == [] and b.load_qitem(it["qid"])["status"] == "failed"
          and not any(x.name == "outbox.json" for x in b.OUTBOX_DIR.rglob("*")),
          "behaviour: a failed FILE stays in the media queue (outbox/queue) and never enters outbox.json; "
          "outbox/ never gets an outbox.json")
    d = Path(tempfile.mkdtemp(prefix="wb-batch-bk-"))
    _made.append(d)
    (d / "state").mkdir()
    for name in ("credentials.json", "pending.json", "outbox.json"):
        (d / "state" / name).write_text("{}")
    (d / "bridge.py").write_text("# fake\n")
    (d / "wechat-bridge.service").write_text("[Unit]\n")
    env = {**os.environ, "BRIDGE_DIR": str(d), "HOOKS_DIR": str(d / "nohooks"),
           "WECHAT_BRIDGE_CRON_SRC": str(d / "nocron.md")}
    r = subprocess.run(["bash", str(HERE / "ops/backup.sh")], env=env, capture_output=True, text=True,
                       timeout=120)
    snaps = [x for x in (d / "backups").iterdir() if x.is_dir()] if (d / "backups").exists() else []
    st = snaps[0] / "state" if len(snaps) == 1 else None
    check(r.returncode == 0 and st is not None and (st / "credentials.json").exists()
          and not (st / "pending.json").exists() and not (st / "outbox.json").exists(),
          "ops/backup.sh: hourly snapshots (kept forever) leave out pending.json and outbox.json "
          "(reply text; in-flight state rebuilt from inbox/)", (r.stdout + r.stderr)[-300:])


# ================================================================== visibility, docs
def t_status_and_docs():
    b = load("stat")
    b.batching_on()
    say(b, "w1", "稍等我再补一句")
    rc, st = cli(b.cmd_status)
    prog = b.progress_command("owner")
    check(rc == 0 and st.get("batch", {}).get("open") and st["batch"]["open"][0]["messages"] == 1
          and st.get("text_outbox", {}).get("file") == "state/outbox.json" and "合批中" in prog
          and "queue" in st and st["queue"] is not st["text_outbox"],
          "status shows the open batch and the text outbox (apart from the media `queue`); 「进度」 says 合批中")
    docs = {n: (HERE / n).read_text(encoding="utf-8") for n in
            ("README.md", "EXECUTOR.md", "OPS.md", "hooks/wechat-bridge-inbox.prompt.txt")}
    check(b.EXIT_SUPERSEDED == 4 and all("superseded" in s.lower() and "4" in s for s in docs.values())
          and "退出码 4" in docs["EXECUTOR.md"] and "退出码 4" in docs["hooks/wechat-bridge-inbox.prompt.txt"]
          and "EXIT_SUPERSEDED = 4" in (HERE / "bridge.py").read_text(encoding="utf-8"),
          "exit 4 (SUPERSEDED) is defined in the code and in README / EXECUTOR.md / OPS.md / the prompt")
    check(len((HERE / "EXECUTOR.md").read_text(encoding="utf-8").encode()) < 8000,
          "EXECUTOR.md still under 8000 bytes")


for fn in (t_burst_one_round, t_single_after_3s, t_cap_60s, t_boundary_same_instant, t_boundary_stress,
           t_recover_and_restart, t_one_rule, t_off_switch, t_supersede_exit4, t_supersede_bounds,
           t_hook_batch, t_batch_is_one_unit, t_outbox_resend_within_3s, t_outbox_bounded, t_outbox_scope,
           t_outbox_separate_from_media_queue, t_status_and_docs):
    try:
        fn()
    except Exception as e:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        check(False, f"{fn.__name__} raised {type(e).__name__}: {e}")

for p_ in _procs:
    with contextlib.suppress(Exception):
        p_.kill()
for d_ in _made:
    shutil.rmtree(d_, ignore_errors=True)
print("\nALL BATCH/SUPERSEDE/OUTBOX CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED: {fails}")
sys.exit(1 if fails else 0)
