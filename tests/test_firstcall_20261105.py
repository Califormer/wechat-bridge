#!/usr/bin/env python3
"""2026-11-05 round (Claude.md section 3, Step 0 + Step 1). Offline, stdlib only.

Step 0 - stats must not hide a late first progress:
  * the 15 to_first_call samples Muse measured on the live bridge after test 2
    (fake tasks, same numbers): p50 2782 / p95 5521 as before, max 132300 and
    the task named in to_first_call_top
  * a 132 s sample among 40 short ones: p95 hides it, max / top do not
  * `stats --task-id` / `--recent`: per-task calls, to_first_call, notices
Step 1 - long steps are reported before they start, whatever the estimate:
  * EXECUTOR.md and the hook prompt carry the rule (read-back), the prompt in
    hooks/wechat-bridge-inbox.prompt.txt is byte-identical to the JSON, and
    the hook settings (poll 5 s, timeout 600 s) are untouched
  * bridge side of the new rule: a progress --notify right after the claim is
    recorded as the first call and queues a notice about that task; during a
    147 s step nothing from the bridge duplicates it (no auto-progress)
The LLM's own compliance cannot be tested offline: that is the real-session
check in CHANGES. Run: python3 tests/test_firstcall_20261105.py
"""
import contextlib, hashlib, importlib.util, io, json, shutil, subprocess, sys, tempfile
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
fails = []
_n = [0]

# The live samples (ms) from Claude.md 2.4, test 2 (n=15); 132300 is task 2-3.
LIVE_TO_FIRST_CALL = [1485, 2120, 2427, 2527, 2601, 2636, 2709, 2782, 2819, 2931,
                      3193, 3590, 4046, 5521, 132300]
LONG_STEPS = ("改代码", "跑测试", "重启服务", "打包", "等外部结果")
# Added 2026-11-05 by 机主 (examples, not a checklist), plus the 60 s rule.
NEW_EXAMPLES = ("耗时脚本", "下载/上传大文件", "编译/安装", "批量处理", "网络抓取")
RULE_BITS = ("任何单步预计超过 60 秒", "不看预计时长", "举例，不是穷举", "领取后第一个动作",
             "只在开头带一次 --notify", "只报 --stage", "不算这类", "--notify")


def check(cond, name, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name + (f" ({detail})" if detail and not cond else ""))
    if not cond:
        fails.append(name)


def load(tag):
    _n[0] += 1
    d = Path(tempfile.mkdtemp(prefix=f"wb-first-{tag}-"))
    shutil.copy(HERE / "bridge.py", d / "bridge.py")
    spec = importlib.util.spec_from_file_location(f"bp_first_{tag}_{_n[0]}", d / "bridge.py")
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b.log = lambda msg: None
    for p in (b.STATE_DIR, b.INBOX_DIR, b.MEDIA_DIR, b.FILES_DIR, b.HISTORY_DIR,
              b.QUEUE_DIR, b.ATTACH_DIR):
        p.mkdir(parents=True, exist_ok=True)
    # Fake credentials and a fake base URL: nothing here may reach iLink.
    b.atomic_write_json(b.CREDS_FILE, {"bot_token": "T", "ilink_bot_id": "bot",
                                       "ilink_user_id": "owner",
                                       "baseurl": "https://127.0.0.1:9"})
    b.atomic_write_json(b.CONTEXT_TOKENS_FILE, {"owner": {"token": "CT", "at": b.now_ms()}})
    b.deliver = lambda tid: {"ok": False, "pending": True, "stub": "offline test: not sent"}
    return b, d


def quiet(fn, *a):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = fn(*a)
    return rc, buf.getvalue()


def answered(b, mid, to_first_call, exec_ms, received_ago_ms, warm=True):
    """A finished, sent task with one progress call `to_first_call` ms after
    its claim (None: no call at all)."""
    rec = b.now_ms() - received_ago_ms
    cl = rec + 1200
    fin = cl + exec_ms
    ex = {"claimed_at": cl, "finished_at": fin, "by": "muse", "warm": warm, "ok": True}
    if to_first_call is not None:
        ex["calls"] = [cl + to_first_call]
    b.save_task({"message_id": mid, "from_user_id": "owner", "kind": "text", "text": "q",
                 "status": "sent", "received_at": rec, "pending_at": rec,
                 "execution": ex, "send": {"ok": True, "at": fin + 700, "parts": 1},
                 "context_token": "CT"})


def stats_json(b, **kw):
    ns = SimpleNamespace(hours=72, **kw)
    rc, out = quiet(b.cmd_stats, ns)
    return rc, json.loads(out)


# ------------------------------------------------------------------ Step 0
def t_live_samples():
    b, _ = load("live")
    for i, ms in enumerate(LIVE_TO_FIRST_CALL):
        mid = "t2-3" if ms == 132300 else f"s{i:02d}"
        answered(b, mid, ms, 141_700 if ms == 132300 else ms + 20_000,
                 received_ago_ms=3_600_000 - i * 60_000)
    rc, st = stats_json(b)
    tf = st["evidence"]["exec"]["to_first_call"]
    top = st["evidence"]["exec"]["to_first_call_top"]
    check(rc == 0 and tf["n"] == 15 and tf["p50"] == 2782 and tf["p95"] == 5521,
          "live samples (n=15): p50 2782 / p95 5521 exactly as Muse read them (algorithm unchanged)", str(tf))
    check(tf["max"] == 132300 and top and top[0]["task_id"] == "t2-3"
          and top[0]["to_first_call_ms"] == 132300 and top[0]["exec_ms"] == 141_700,
          "the 132300 ms first progress is visible: to_first_call.max and to_first_call_top[0] = task 2-3",
          json.dumps(top, ensure_ascii=False))
    ewc = st["evidence"]["exec"]["exec_with_calls"]
    alc = st["evidence"]["exec"]["after_last_call"]
    check(ewc["max"] == 141_700 and alc["max"] == 20_000 and "max" in st["evidence"]["exec"]["calls"],
          "exec_with_calls / after_last_call carry max too", f"{ewc} {alc}")
    rc, one = stats_json(b, task_id=["t2-3"], recent=0)
    row = (one.get("tasks") or [{}])[0]
    check(rc == 0 and row.get("calls_ms") == [132300] and row.get("to_first_call_ms") == 132300
          and row.get("exec_ms") == 141_700 and len(row.get("calls") or []) == 1
          and row.get("after_last_call_ms") == 9_400,
          "stats --task-id t2-3: calls list, to_first_call 132300 ms, exec 141.7 s, 9.4 s after the call",
          json.dumps(row, ensure_ascii=False)[:300])


def t_hidden_in_p95():
    b, _ = load("p95")
    for i in range(39):
        answered(b, f"short{i:02d}", 2000 + 50 * i, 30_000, received_ago_ms=7_200_000 - i * 60_000)
    answered(b, "long1", 132_000, 147_000, received_ago_ms=60_000)
    ev = b.exec_breakdown()
    tf = ev["to_first_call"]
    check(tf["n"] == 40 and tf["p95"] < 10_000 and tf["max"] == 132_000
          and ev["to_first_call_top"][0]["task_id"] == "long1",
          "a 132 s sample among 40: p95 still shows a short value, max and the top list show 132 s",
          str(tf))
    answered(b, "silent1", None, 147_000, received_ago_ms=30_000)
    ev = b.exec_breakdown()
    check(ev["exec_no_calls"]["max"] == 147_000 and ev["exec_no_calls_top"][0]["task_id"] == "silent1",
          "a 147 s task with no progress at all is named in exec_no_calls_top", str(ev["exec_no_calls_top"]))


def t_per_task_cli():
    b, d = load("cli")
    answered(b, "t2-3", 132300, 141_700, received_ago_ms=600_000)
    answered(b, "t2-4", None, 30_700, received_ago_ms=300_000)
    rc, rec = stats_json(b, task_id=None, recent=2)
    ids = [r.get("task_id") for r in rec.get("tasks") or []]
    check(rc == 0 and ids == ["t2-4", "t2-3"] and rec["tasks"][0]["calls"] == []
          and rec["tasks"][0]["to_first_call_ms"] is None,
          "stats --recent 2: newest first, a task without calls shows calls=[] / to_first_call null",
          str(ids))
    rc, miss = stats_json(b, task_id=["nope"], recent=0)
    check(rc == 2 and miss["ok"] is False and miss["tasks"][0].get("error") == "task not found",
          "stats --task-id <unknown>: ok=false, rc 2")
    rc, plain = quiet(b.cmd_stats, SimpleNamespace(hours=72))
    check(rc == 0 and "evidence" in json.loads(plain),
          "stats with only --hours (older callers, doctor) still prints the evidence block")
    r = subprocess.run([sys.executable, "bridge.py", "stats", "--task-id", "t2-3,t2-4"],
                       cwd=d, capture_output=True, text=True, timeout=60)
    try:
        cli = json.loads(r.stdout)
    except ValueError:
        cli = {}
    check(r.returncode == 0 and [x["task_id"] for x in cli.get("tasks") or []] == ["t2-3", "t2-4"]
          and cli["tasks"][0]["to_first_call_ms"] == 132300,
          "CLI: `bridge.py stats --task-id a,b` parses and prints both timelines",
          (r.stdout + r.stderr)[-300:])


# ------------------------------------------------------------------ Step 1
def t_rule_texts():
    core = (HERE / "EXECUTOR.md").read_text(encoding="utf-8")
    beat = next((ln for ln in core.splitlines() if ln.startswith("- **长步骤先报")), "")
    check(len(core.encode()) < 8000, "EXECUTOR.md stays under 8 KB", f"{len(core.encode())} bytes")
    check(all(k in beat for k in LONG_STEPS + NEW_EXAMPLES + RULE_BITS),
          "EXECUTOR.md: any step over 60 s and the listed kinds (examples) are reported first; first "
          "action when the task names one; --notify once at the start, later steps --stage only; "
          "queries are not this kind", [k for k in LONG_STEPS + NEW_EXAMPLES + RULE_BITS if k not in beat])
    check("没有长步骤、预计 3 分钟内能答完的直接做" in core and "预计 3 分钟内能答完的直接做" not in
          core.replace("没有长步骤、预计 3 分钟内能答完的直接做", ""),
          "EXECUTOR.md: the 3-minute shortcut only applies to tasks without long steps")
    check("每 5 分钟至少一次" in core and "超过 10 分钟没心跳" in core and "--hold-minutes" in core
          and "有把握再加" not in core
          and "45" in core and "EXECUTOR-media.md" in core,
          "EXECUTOR.md keeps the 5-min heartbeat, 10-min reclaim, hold lease, 45 s note, media pointer")


def _prompt_parts():
    hook = json.loads((HERE / "hooks/wechat-bridge-inbox.json").read_text(encoding="utf-8"))
    txt = (HERE / "hooks/wechat-bridge-inbox.prompt.txt").read_text(encoding="utf-8")
    return hook, txt


def t_prompt_readback():
    hook, txt = _prompt_parts()
    pr = hook.get("prompt") or ""
    sha = hashlib.sha256(pr.encode("utf-8")).hexdigest()
    rec = (HERE / "hooks/wechat-bridge-inbox.prompt.sha256").read_text(encoding="utf-8").split()
    check(pr == txt, "prompt read-back: hooks/*.prompt.txt is byte-identical to the JSON prompt")
    check(rec and rec[0] == sha and int(rec[1]) == len(pr),
          "prompt read-back: recorded sha256 / length match the prompt", f"{sha} {len(pr)} vs {rec}")
    check(hook.get("poll_interval_secs") == 5 and hook.get("script_timeout_secs") == 600
          and hook.get("id") == "wechat-bridge-inbox" and hook.get("enabled") is True,
          "hook settings untouched: poll 5 s (never below), timeout 600 s, same id")
    line = next((ln for ln in pr.splitlines() if ln.startswith("- 长步骤先报")), "")
    check(all(k in line for k in LONG_STEPS + NEW_EXAMPLES + RULE_BITS) and "python3 wb progress" in line,
          "prompt: the long-step rule (60 s + examples, notify once) is in the prompt itself (the executor "
          "may not open EXECUTOR.md for a task it thinks is short)",
          [k for k in LONG_STEPS + NEW_EXAMPLES + RULE_BITS if k not in line])
    check("没有长步骤、预计 3 分钟内能答完的任务直接做" in pr and "预计 3 分钟内能答完的任务直接做" not in
          pr.replace("没有长步骤、预计 3 分钟内能答完的任务直接做", ""),
          "prompt: the short-task shortcut stays, limited to tasks without long steps")
    check(all(k in pr for k in ("--then-wait auto", "--claim-id <claim_id>", "stale_claim=true",
                                "cancelled=true", "python3 wb claim --limit 5", "merge_hint",
                                "bot_token")),
          "prompt: everything else of the deployed prompt is still there")
    check("WECHAT_BRIDGE_WARM_WAIT_S" in pr and "resolve" not in pr and "自动重发" not in pr,
          "prompt: tuning stays in state/tuning.env; no echo resolve / auto-resend")


def t_bridge_side_new_rule():
    """The new rule from the bridge's point of view, with a 147 s step:
    claim -> progress --stage --note --notify at once -> (147 s) -> complete."""
    b, _ = load("rule")
    b.save_task({"message_id": "r1", "from_user_id": "owner", "kind": "text",
                 "text": "在 /tmp 隔离目录解压性能包，跑 tests/run_all.py，22 套测试是否全过",
                 "status": "pending_execution", "received_at": b.now_ms(), "pending_at": b.now_ms(),
                 "execution": None, "send": None, "context_token": "CT"})
    got = b.claim_tasks(limit=5)
    cid = (got.get("tasks") or [{}])[0].get("claim_id") or got.get("claim_id")
    rc, out = quiet(b.cmd_progress, SimpleNamespace(
        task_id="r1", note="开始在 /tmp 隔离区跑整套测试，约 2 到 3 分钟，跑完把结果发你",
        stage="跑整套测试（tests/run_all.py）", notify=True, hold_minutes=None, claim_id=cid))
    ev = b.task_evidence("r1")
    check(rc == 0 and ev["to_first_call_ms"] is not None and ev["to_first_call_ms"] <= 10_000,
          "first progress recorded right after the claim (to_first_call <= 10 s)", str(ev)[:200])
    check(len(ev["notices"]) == 1 and ev["notices"][0]["reason"] == "progress"
          and ev["notices"][0]["queued_ms"] <= 10_000,
          "the --notify note is queued for the user within 10 s and is tied to the task",
          str(ev["notices"]))
    # 147 s later the step is still running: shift the clock of the task back.
    with b.task_lock():
        t = b.load_task("r1")
        shift = 147_000
        t["execution"]["claimed_at"] -= shift
        t["execution"]["heartbeat_at"] -= shift
        t["execution"]["calls"] = [x - shift for x in t["execution"]["calls"]]
        t["received_at"] -= shift
        b.save_task(t)
    before = sum(1 for s in b.scan_task_index() if s.get("kind") == "notice")
    b.check_auto_progress(b.scan_task_index())
    after = sum(1 for s in b.scan_task_index() if s.get("kind") == "notice")
    check(after == before, "during the 147 s step the bridge adds no auto-progress on top of the "
          "executor's note (no duplicate)", f"{before} -> {after}")
    # Control, the old behaviour of 2-3: no call for 141.7 s.
    b.save_task({"message_id": "r0", "from_user_id": "owner", "kind": "text", "text": "x",
                 "status": "executing", "received_at": b.now_ms() - 142_000,
                 "execution": {"claimed_at": b.now_ms() - 141_700, "by": "muse"},
                 "send": None, "context_token": "CT"})
    b.check_auto_progress(b.scan_task_index())
    ctl = b.task_evidence("r0")
    check(ctl["notices"] == [] and ctl["to_first_call_ms"] is None,
          "control (old 2-3 behaviour): 141.7 s without a call gets no note at all from the bridge "
          "either (first auto stage is 180 s) - the silence the new rule removes")


def t_doctor_tail_line():
    """doctor shows the tail (to_first_call max + task, total/exec p95, n) as
    information only: PASS however slow, one `stats` run shared with scale."""
    b, _ = load("doctor")
    for i, ms in enumerate(LIVE_TO_FIRST_CALL):
        mid = "t2-3" if ms == 132300 else f"s{i:02d}"
        answered(b, mid, ms, 141_700 if ms == 132300 else ms + 20_000,
                 received_ago_ms=3_600_000 - i * 60_000)
    rc, stats_out = quiet(b.cmd_stats, SimpleNamespace(hours=72))
    spec = importlib.util.spec_from_file_location("doctor_first", HERE / "ops" / "doctor.py")
    doc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(doc)
    calls = []
    doc.run = lambda *a: calls.append(a) or stats_out
    doc.check_scale()
    doc.check_latency_tail()
    rows = {r["check"]: r for r in doc.results}
    tail = rows.get("latency_tail") or {}
    st = json.loads(stats_out)
    want_total = f"{st['latency_ms']['total']['p95'] / 1000:.1f}s"
    want_exec = f"{st['latency_ms']['exec']['p95'] / 1000:.1f}s"
    check(tail.get("status") == "PASS" and "to_first_call max 132.3s (task t2-3), n=15" in tail.get("detail", "")
          and f"total p95 {want_total}" in tail.get("detail", "") and f"exec p95 {want_exec}" in tail.get("detail", "")
          and "n=15 " in tail.get("detail", "") and rows.get("scale", {}).get("status") == "PASS" and len(calls) == 1,
          "doctor: one info line with to_first_call max 132.3 s (task 2-3), total/exec p95 and n; "
          "stats run once for scale + tail", f"{tail} calls={len(calls)}")
    slow = json.loads(stats_out)
    slow["latency_ms"]["total"]["p95"] = 36_000_000
    slow["latency_ms"]["exec"]["p95"] = 36_000_000
    slow["evidence"]["exec"]["to_first_call"]["max"] = 9_000_000
    doc.results.clear()
    doc._STATS.clear()
    doc.run = lambda *a: json.dumps(slow)
    doc.check_latency_tail()
    doc._STATS.clear()
    doc.run = lambda *a: "<error timeout>"
    doc.check_latency_tail()
    check([r["status"] for r in doc.results] == ["PASS", "PASS"],
          "doctor: 10-hour p95 / 2.5-hour first call is still PASS (latency is not bridge health); "
          "unreadable stats does not FAIL either", str(doc.results))


for fn in (t_live_samples, t_hidden_in_p95, t_per_task_cli, t_rule_texts, t_prompt_readback,
           t_bridge_side_new_rule, t_doctor_tail_line):
    try:
        fn()
    except Exception as e:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        check(False, f"{fn.__name__} raised {type(e).__name__}: {e}")

print("\nALL FIRST-CALL CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED: {fails}")
sys.exit(1 if fails else 0)
