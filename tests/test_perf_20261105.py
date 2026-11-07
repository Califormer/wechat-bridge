#!/usr/bin/env python3
"""Latency / agent-throughput features (2026-11-05): (adaptive burst window:
replaced 2026-11-06 by the pending.json batches - t_claim_has_no_window),
fast warm pick-up, claim waiting for downloads (hook pre-wake), context
delta, `complete --then-wait`, delivery proof by WeChat's bot echo,
pre-digested media, the `wb` launcher, hook pre-wake and opt-in linger.
Offline, stdlib only; ffmpeg/pdftotext checks skip when missing.
Run: python3 tests/test_perf_20261105.py
"""
import contextlib, importlib.util, io, json, os, shutil, subprocess, sys, tempfile
import threading, time
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
fails = []
_n = [0]


def check(cond, name, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name + (f" ({detail})" if detail and not cond else ""))
    if not cond:
        fails.append(name)


def load(tag):
    _n[0] += 1
    d = Path(tempfile.mkdtemp(prefix=f"wb-perf-{tag}-"))
    shutil.copy(HERE / "bridge.py", d / "bridge.py")
    spec = importlib.util.spec_from_file_location(f"bp_{tag}_{_n[0]}", d / "bridge.py")
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b.log = lambda msg: None
    for p in (b.STATE_DIR, b.INBOX_DIR, b.MEDIA_DIR, b.FILES_DIR, b.HISTORY_DIR,
              b.QUEUE_DIR, b.ATTACH_DIR):
        p.mkdir(parents=True, exist_ok=True)
    b.atomic_write_json(b.CREDS_FILE, {"bot_token": "T", "ilink_bot_id": "bot",
                                       "ilink_user_id": "owner",
                                       "baseurl": "https://ilinkai.weixin.qq.com"})
    b.atomic_write_json(b.CONTEXT_TOKENS_FILE, {"owner": {"token": "CT", "at": b.now_ms()}})
    return b


def task(b, mid, text="", status="pending_execution", age_ms=0, **kw):
    t = {"message_id": mid, "from_user_id": "owner", "kind": kw.pop("kind", "text"),
         "text": text, "status": status, "received_at": b.now_ms() - age_ms,
         "pending_at": b.now_ms(), "execution": None, "send": None, "context_token": "CT"}
    t.update(kw)
    b.save_task(t)
    return t


def quiet(fn, *a):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = fn(*a)
    return rc, buf.getvalue()


# ---------------------------------------------------------------- claim path
def t_claim_has_no_window():
    """2026-11-06 (replaces t_adaptive_settle): the claim-side burst window -
    0.3 s for a finished sentence, 1.2 s for a fragment - is removed. Bursts
    are coalesced only by serve's batches (state/pending.json; see
    tests/test_batch_20261106.py), so a message that has been dispatched is
    claimed at once, however it reads."""
    b = load("settle")
    task(b, "s1", "帮我看看这段代码哪里有问题？")
    t0 = time.monotonic()
    out = b.claim_tasks(limit=5, wait_s=3)
    dt1 = time.monotonic() - t0
    task(b, "s2", "等一下")
    t0 = time.monotonic()
    out2 = b.claim_tasks(limit=5, wait_s=3)
    dt2 = time.monotonic() - t0
    check([t["message_id"] for t in out["tasks"]] == ["s1"] and dt1 < 0.8,
          f"a finished sentence is claimed at once ({dt1:.2f}s)")
    check([t["message_id"] for t in out2["tasks"]] == ["s2"] and dt2 < 0.8
          and not hasattr(b, "_settled_pending") and not hasattr(b, "CLAIM_SETTLE_MS"),
          f"a fragment too: no claim-side window any more, the batch is the only rule ({dt2:.2f}s)")


def t_fast_pickup_and_cursor():
    b = load("pickup")
    res = {}
    def waiter():
        res["out"] = b.claim_tasks(limit=5, wait_s=5)
        res["t"] = time.monotonic()
    th = threading.Thread(target=waiter)
    th.start()
    time.sleep(0.4)
    t0 = time.monotonic()
    task(b, "p1", "在吗？", age_ms=5000)            # old enough: no burst wait
    th.join(6)
    lag = res["t"] - t0
    check(res["out"]["tasks"] and lag < 0.3, f"a waiting executor picks a message up within {lag * 1000:.0f} ms")
    check(isinstance(res["out"].get("context_cursor"), int), "claim returns a context_cursor")
    t0 = time.monotonic()
    empty = b.claim_tasks(limit=5, wait_s=0)
    check(empty["tasks"] == [] and time.monotonic() - t0 < 0.3, "no task, no download: claim returns at once")


def t_claim_waits_for_download():
    b = load("dl")
    task(b, "d1", "", status="downloading", kind="image", age_ms=5000)
    res = {}
    th = threading.Thread(target=lambda: res.update(out=b.claim_tasks(limit=5, wait_s=0)))
    th.start()
    time.sleep(0.6)
    t = b.load_task("d1")
    t.update(status="pending_execution", media_path="/tmp/x.jpg")
    b.save_task(t)
    th.join(5)
    check(res.get("out") and [x["message_id"] for x in res["out"]["tasks"]] == ["d1"],
          "a claim started during the media download gets the task when it is ready (not an empty list)")


def t_media_question_together():
    b = load("hold")
    task(b, "h1", "", status="downloading", kind="video")
    time.sleep(0.05)
    task(b, "h2", "这个视频讲了什么？")
    def ready():
        t = b.load_task("h1")
        t.update(status="pending_execution", media_path="/tmp/v.mp4")
        b.save_task(t)
    threading.Timer(1.0, ready).start()
    out = b.claim_tasks(limit=5, wait_s=5)
    check([t["message_id"] for t in out["tasks"]] == ["h1", "h2"] and "merge_hint" in out,
          "a question sent right after a downloading video is claimed together with it")


def t_context_delta():
    b = load("delta")
    b.history_append("user", "旧问题", at=b.now_ms() - 60_000)
    b.history_append("bot", "旧回答", at=b.now_ms() - 59_000)
    b.memo_set("备忘：在改微信桥")
    time.sleep(0.02)
    cursor = b.now_ms()
    time.sleep(0.02)
    b.history_append("user", "新的追问")
    full = b.conversation_context(b.now_ms() + 1)
    delta = b.conversation_context(b.now_ms() + 1, since_ms=cursor)
    check(full and "memo" in full and len(full.get("recent", [])) == 3,
          "full context: memo + all recent turns")
    check(delta and delta.get("recent") and len(delta["recent"]) == 1 and "新的追问" in delta["recent"][0]
          and "memo" not in delta,
          "delta context: only the turn after the cursor, unchanged memo left out", str(delta))


def t_then_wait():
    b = load("then")
    b.deliver = lambda tid: {"ok": True, "via": "test"}
    task(b, "a1", "第一个问题？", age_ms=5000)
    claimed = b.claim_tasks(limit=1)
    cid = claimed.get("claim_id")
    threading.Timer(0.5, lambda: task(b, "a2", "追问：那第二步呢？", age_ms=5000)).start()
    args = SimpleNamespace(task_id="a1", result="第一步是这样。", result_file=None, error=None,
                           attach=[], attach_as="auto", wait_attachments=0, force=False,
                           merge_ids=None, raw=False, claim_id=cid, then_wait=4, then_limit=5)
    t0 = time.monotonic()
    rc, out = quiet(b.cmd_complete, args)
    res = json.loads(out)
    nxt = res.get("next") or {}
    check(rc == 0 and [t["message_id"] for t in nxt.get("tasks", [])] == ["a2"]
          and nxt.get("context_delta") and time.monotonic() - t0 < 3,
          "complete --then-wait delivers and returns the follow-up in the same call", out[:200])


# ---------------------------------------------------------------- echoes
def t_echo_resolution():
    os.environ["WECHAT_BRIDGE_ECHO_MODE"] = "resolve"
    try:
        b = load("echo")
    finally:
        os.environ.pop("WECHAT_BRIDGE_ECHO_MODE", None)
    text = "第一段\n\n" + "a" * 3500 + "\n\n" + "b" * 3500
    s, e, _p, _q = b._plan_parts(text, 0, b._chunk_limit())[0]
    task(b, "u1", "问题", status="send_uncertain", result_text=text,
         send={"pending": {"start": s, "end": e, "client_id": "openclaw-weixin:1-aa"},
               "done_upto": 0, "no_auto_retry": True, "uncertain": True, "attempts": 1})
    task(b, "u2", "问题2", status="send_uncertain", result_text="短回复",
         send={"pending": {"start": 0, "end": 3, "client_id": "openclaw-weixin:2-bb"},
               "done_upto": 0, "no_auto_retry": True, "uncertain": True, "attempts": 1})
    b.save_qitem({"qid": "q1", "status": "uncertain", "client_id": "openclaw-weixin:3-cc",
                  "to_user": "owner", "name": "chart.png", "kind": "image", "path": "/nonexistent",
                  "created_at": b.now_ms()})
    creds = b.load_creds()
    for n, cid in ((901, "openclaw-weixin:1-aa"), (902, "openclaw-weixin:2-bb"),
                   (903, "openclaw-weixin:3-cc")):
        b.ingest_message(creds, {"message_id": n, "message_type": 2, "client_id": cid,
                                 "from_user_id": "bot", "to_user_id": "owner",
                                 "item_list": [{"type": 1, "text_item": {"text": "回复"}}]})
    u1, u2 = b.load_task("u1"), b.load_task("u2")
    check(u1["status"] == "send_failed" and u1["send"]["done_upto"] == e
          and "pending" not in u1["send"] and not u1["send"].get("no_auto_retry"),
          "echo of an uncertain part: marked delivered, the rest of the reply resumes", str(u1["send"]))
    check(u2["status"] == "sent" and b.load_qitem("q1")["status"] == "sent",
          "echo of a single-part reply / a media message: sent")
    check(b.load_task("901") is None and b._echo_capable(), "echoes are not tasks; 3 echoes = echo-capable")


def t_echo_wait_notice():
    os.environ["WECHAT_BRIDGE_ECHO_MODE"] = "resolve"
    try:
        b = load("echowait")
    finally:
        os.environ.pop("WECHAT_BRIDGE_ECHO_MODE", None)
    b.atomic_write_json(b.ECHO_FILE, {"seen": 5, "last_at": b.now_ms()})
    task(b, "w1", "问题", status="waiting_send", result_text="好的", age_ms=1000,
         execution={"finished_at": b.now_ms()})
    def boom(*a, **k):
        raise b.ApiError("timeout", "request timeout")
    b.send_text = boom
    b.attempt_send("w1")
    t = b.load_task("w1")
    notices = [x for x in b.list_tasks() if x.get("kind") == "notice"]
    check(t["status"] == "send_uncertain" and t["send"].get("notified") == "echo-wait" and not notices,
          "echo-capable account: the 'maybe not delivered' notice waits for the echo")
    t["send"]["notify_after"] = b.now_ms() - 1
    b.save_task(t)
    b.check_echo_wait(b.scan_task_index())
    notices = [x for x in b.list_tasks() if x.get("kind") == "notice"]
    check(len(notices) == 1 and str(b.load_task("w1")["send"].get("notified", "")).startswith("notice-"),
          "no echo within the grace period: the user is told, once")


# ---------------------------------------------------------------- pre-digest
def _pdf(text: str) -> bytes:
    stream = f"BT /F1 24 Tf 20 100 Td ({text}) Tj ET".encode()
    objs = [b"<</Type/Catalog/Pages 2 0 R>>", b"<</Type/Pages/Kids[3 0 R]/Count 1>>",
            b"<</Type/Page/Parent 2 0 R/MediaBox[0 0 300 200]/Contents 4 0 R"
            b"/Resources<</Font<</F1 5 0 R>>>>>>",
            b"<</Length " + str(len(stream)).encode() + b">>stream\n" + stream + b"\nendstream",
            b"<</Type/Font/Subtype/Type1/BaseFont/Helvetica>>"]
    out, offs = bytearray(b"%PDF-1.4\n"), []
    for i, o in enumerate(objs, 1):
        offs.append(len(out))
        out += f"{i} 0 obj".encode() + o + b"endobj\n"
    x = len(out)
    out += f"xref\n0 {len(objs) + 1}\n0000000000 65535 f \n".encode()
    out += b"".join(f"{o:010d} 00000 n \n".encode() for o in offs)
    out += f"trailer<</Size {len(objs) + 1}/Root 1 0 R>>\nstartxref\n{x}\n%%EOF\n".encode()
    return bytes(out)


def t_predigest():
    b = load("digest")
    f1 = b.FILES_DIR / "m1__notes.md"
    f1.write_text("# 会议纪要\n\n- 第一项：上线\n", encoding="utf-8")
    f2 = b.FILES_DIR / "m2__old.txt"
    f2.write_bytes("中文旧编码文件".encode("gb18030"))
    f3 = b.FILES_DIR / "m3__big.log"
    f3.write_text("行\n" * 5000, encoding="utf-8")
    d1, d2, d3 = (b._predigest("file", {"media_path": str(f)}) for f in (f1, f2, f3))
    check(d1.get("text_inline", "").startswith("# 会议纪要") and d2.get("text_inline") == "中文旧编码文件"
          and len(d3.get("text_head", "")) == 2000 and d3.get("text_chars") == 10000,
          "text files: small ones inline (UTF-8 and GB18030), big ones as head + size")
    if shutil.which("pdftotext"):
        pdf = b.FILES_DIR / "m4__report.pdf"
        pdf.write_bytes(_pdf("Hello PDF"))
        d4 = b._predigest("file", {"media_path": str(pdf)})
        check("Hello PDF" in d4.get("text_head", "") and Path(d4.get("text_path", "/x")).is_file(),
              "PDF text extracted up front (pdftotext)")
    else:
        print("SKIP  PDF text: no pdftotext")
    ff = shutil.which("ffmpeg")
    if not ff:
        print("SKIP  image/video pre-digest: no ffmpeg")
        return
    img = b.MEDIA_DIR / "i1.jpg"
    subprocess.run([ff, "-nostdin", "-loglevel", "error", "-y", "-f", "lavfi", "-i",
                    "nullsrc=s=3000x2000,geq=random(1)*255:128:128", "-frames:v", "1",
                    "-q:v", "1", str(img)], check=True, timeout=120)
    di = b._predigest("image", {"media_path": str(img)})
    prev = Path(di.get("preview_path") or "/x")
    dims = subprocess.run(["ffprobe", "-v", "error", "-select_streams", "v", "-show_entries",
                           "stream=width,height", "-of", "csv=p=0", str(prev)],
                          capture_output=True, text=True).stdout.strip() if prev.is_file() else ""
    check(dims and max(int(x) for x in dims.split(",")) <= 1568,
          f"big image gets a <=1568 px preview ({dims or 'none'})")
    vid = b.MEDIA_DIR / "v1.mp4"
    subprocess.run([ff, "-nostdin", "-loglevel", "error", "-y", "-f", "lavfi", "-i",
                    "testsrc=size=640x360:rate=10", "-f", "lavfi", "-i", "sine=frequency=440",
                    "-t", "3", "-shortest", "-pix_fmt", "yuv420p", str(vid)], check=True, timeout=120)
    dv = b._predigest("video", {"media_path": str(vid)})
    mute = b.MEDIA_DIR / "v2.mp4"
    subprocess.run([ff, "-nostdin", "-loglevel", "error", "-y", "-f", "lavfi", "-i",
                    "testsrc=size=320x240:rate=10", "-t", "2", "-pix_fmt", "yuv420p", str(mute)],
                   check=True, timeout=120)
    dm = b._predigest("video", {"media_path": str(mute)})
    check(dv.get("frames") and Path(dv["frames"][0]).is_file() and Path(dv.get("audio_wav", "/x")).is_file()
          and dm.get("has_audio") is False,
          "video: key frames and the audio track ready before the claim; a silent video says so")
    view = b._executor_view({"message_id": "v1", "kind": "video", "text": "", "media_path": str(vid), **dv})
    check(view.get("frames") == dv["frames"] and "frames" in (view.get("hint") or ""),
          "claim view carries the pre-digested fields and says so in the hint")


# ---------------------------------------------------------------- wb launcher
def t_launcher():
    d = Path(tempfile.mkdtemp(prefix="wb-launch-"))
    shutil.copy(HERE / "bridge.py", d / "bridge.py")
    shutil.copy(HERE / "wb", d / "wb")
    env = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
    a = subprocess.run([sys.executable, "wb", "status"], cwd=d, env=env, capture_output=True, text=True, timeout=60)
    b_ = subprocess.run([sys.executable, "bridge.py", "status"], cwd=d, env=env, capture_output=True, text=True, timeout=60)
    ok = a.returncode == 0 and set(json.loads(a.stdout)) == set(json.loads(b_.stdout))
    check(ok and (d / "__pycache__").is_dir(), "wb runs the same CLI from cached bytecode")


# ---------------------------------------------------------------- hook
# wake() exits like the real hook runtime (2026-11-05 audit §8-1: a stub that
# returned let the hook's post-wake woke=1 run here, while live it never ran).
RUNTIME = r'''
hook_state_get() { [[ -f "$HOOK_STATE" ]] && cat "$HOOK_STATE" || echo '{}'; }
hook_state_set() { printf '%s' "$1" > "$HOOK_STATE"; }
log()    { echo "LOG $1" >> "$HOOK_EVENTS"; }
wake()   { echo "WAKE $1" >> "$HOOK_EVENTS"; exit 0; }
silent() { echo "SILENT $1" >> "$HOOK_EVENTS"; exit 0; }
'''


def hook_env():
    home = Path(tempfile.mkdtemp(prefix="hook-perf-"))
    br = home / "workspace/wechat-bridge"
    for sub in ("inbox", "state", "ops"):
        (br / sub).mkdir(parents=True)
    (br / "state/credentials.json").write_text("{}")
    (br / "ops/backup.sh").write_text("#!/usr/bin/env bash\n")
    (br / "ops/backup.sh").chmod(0o755)
    stub = home / "stub"
    stub.mkdir()
    if not shutil.which("jq"):
        (stub / "jq").write_text(f"#!/bin/sh\nexec {sys.executable} {HERE}/tests/jqshim.py \"$@\"\n")
        (stub / "jq").chmod(0o755)
    for name in ("systemctl", "cp"):
        (stub / name).write_text("#!/bin/sh\nexit 0\n")
        (stub / name).chmod(0o755)
    (home / "runtime.sh").write_text(RUNTIME)
    proc = subprocess.Popen(["bash", "-c", "exec -a 'python3 bridge.py serve' sleep 300"])
    (br / "state/serve.pid").write_text(str(proc.pid))
    now = int(time.time() * 1000)
    (br / "state/heartbeat.json").write_text(json.dumps(
        {"serve_pid": proc.pid, "serve_started_at": now - 600_000, "last_poll_ok_at": now - 1000}))
    env = {**os.environ, "HOME": str(home), "PATH": f"{stub}:{os.environ['PATH']}",
           "HATCH_HOOK_RUNTIME": str(home / "runtime.sh"), "HOOK_STATE": str(home / "state.json"),
           "HOOK_EVENTS": str(home / "events")}
    return home, br, env, proc


def hook_task(br, mid, status):
    (br / "inbox" / f"{mid}.json").write_text(json.dumps(
        {"message_id": mid, "status": status, "kind": "image", "text": "", "execution": None}, indent=2))


def events(home):
    f = home / "events"
    return [e for e in (f.read_text().splitlines() if f.exists() else []) if not e.startswith("LOG")]


def t_hook():
    script = HERE / "hooks/wechat-bridge-inbox.sh"
    home, br, env, proc = hook_env()
    try:
        hook_task(br, "77", "downloading")
        subprocess.run(["bash", str(script)], env=env, capture_output=True, timeout=60)
        check(any(e.startswith("WAKE wechat_task") for e in events(home)),
              "hook wakes the executor while the media is still downloading (cold start overlaps it)")
    finally:
        proc.kill()
    home, br, env, proc = hook_env()
    try:
        (br / "state/tuning.env").write_text("# test\nWECHAT_BRIDGE_HOOK_LINGER_MS=3000\n")
        t0 = time.monotonic()
        h = subprocess.Popen(["bash", str(script)], env=env, stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
        time.sleep(0.8)
        hook_task(br, "78", "pending_execution")
        t_msg = time.monotonic()
        (br / "state/.inbox_signal").write_text(str(int(time.time() * 1000)))
        h.wait(timeout=30)
        took = time.monotonic() - t_msg
        ev = events(home)
        check(any(e.startswith("WAKE wechat_task") for e in ev) and took < 1.5,
              f"opt-in linger: a message during the linger is woken for at once ({took:.2f}s)", str(ev))
        (home / "events").unlink()
        (br / "inbox/78.json").unlink()
        t0 = time.monotonic()
        subprocess.run(["bash", str(script)], env=env, capture_output=True, timeout=30)
        took = time.monotonic() - t0
        check(events(home) == ["SILENT wechat bridge healthy"] and 2.5 < took < 6,
              f"linger with no message: quiet exit after the window ({took:.1f}s)", str(events(home)))
        ticks = [ln.split() for ln in (br / "state/hook_ticks.log").read_text().splitlines()]
        check(len(ticks) >= 3 and all(len(x) == 6 for x in ticks)
              and any(x[4] == "1" and x[2] == "1" for x in ticks) and any(x[3] == "3000" for x in ticks),
              "every hook run logs one tick line (start end woke linger rerun sig)", str(ticks[-3:]))
    finally:
        proc.kill()


# ---------------------------------------------------------------- evidence (round 4)
def t_echo_observe_default():
    b = load("observe")
    task(b, "o1", "问题", status="send_uncertain", result_text="短回复",
         send={"pending": {"start": 0, "end": 3, "client_id": "openclaw-weixin:9-oo"},
               "done_upto": 0, "no_auto_retry": True, "uncertain": True, "attempts": 1})
    b.send_text = lambda *a, **k: {"ret": 0, "message_id": 1}
    b._send_part(b.load_creds(), "owner", "你好", "CT", "openclaw-weixin:8-ok")
    def boom(*a, **k):
        raise b.ApiError("timeout", "t")
    b.send_text = boom
    with contextlib.suppress(b.ApiError):
        b._send_part(b.load_creds(), "owner", "你好", "CT", "openclaw-weixin:9-oo")
    for n, cid in ((951, "openclaw-weixin:8-ok"), (952, "openclaw-weixin:9-oo")):
        b.ingest_message(b.load_creds(), {"message_id": n, "message_type": 2, "client_id": cid,
                                          "from_user_id": "bot", "to_user_id": "owner",
                                          "item_list": [{"type": 1, "text_item": {"text": "x"}}]})
    st = b.echo_stats()
    check(b.ECHO_MODE == "observe" and b.load_task("o1")["status"] == "send_uncertain"
          and st["sent"] == 1 and st["echoed"] == 1 and st["uncertain"] == 1 and st["uncertain_echoed"] == 1,
          "default observe mode: echoes are only recorded (ledger), nothing changes status", str(st))


def t_tuning_and_auto_wait():
    d = Path(tempfile.mkdtemp(prefix="wb-tune-"))
    (d / "state").mkdir()
    (d / "state/tuning.env").write_text("WECHAT_BRIDGE_WARM_WAIT_S=90\nLD_PRELOAD=/tmp/x.so\n"
                                        "# comment\nWECHAT_BRIDGE_ECHO_MODE='observe'\n")
    shutil.copy(HERE / "bridge.py", d / "bridge.py")
    for k in ("WECHAT_BRIDGE_WARM_WAIT_S", "WECHAT_BRIDGE_ECHO_MODE"):
        os.environ.pop(k, None)
    had = os.environ.get("LD_PRELOAD")
    try:
        spec = importlib.util.spec_from_file_location("b_tune", d / "bridge.py")
        b = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(b)
        ok = (b._wait_arg("auto") == 90.0 and b._wait_arg("30") == 30.0
              and os.environ.get("LD_PRELOAD") == had)
    finally:
        os.environ.pop("WECHAT_BRIDGE_WARM_WAIT_S", None)
        os.environ.pop("WECHAT_BRIDGE_ECHO_MODE", None)
    check(ok, "state/tuning.env sets WECHAT_BRIDGE_* knobs only; --wait auto follows it")


def t_exec_evidence():
    b = load("evid")
    res = {}
    th = threading.Thread(target=lambda: res.update(out=b.claim_tasks(limit=5, wait_s=3)))
    th.start()
    time.sleep(0.3)
    task(b, "e1", "warm 问题？", age_ms=0)
    th.join(5)
    task(b, "e2", "cold 问题？", age_ms=5000)
    b.claim_tasks(limit=5)
    rc, _ = quiet(b.cmd_heartbeat, SimpleNamespace(task_id="e2", stage="step", claim_id=None))
    b.send_text = lambda *a, **k: {"ret": 0, "message_id": 7}
    b.deliver = lambda tid: b.attempt_send(tid)
    for tid in ("e1", "e2"):
        quiet(b.cmd_complete, SimpleNamespace(task_id=tid, result="答", result_file=None, error=None,
                                              attach=[], attach_as="auto", wait_attachments=0,
                                              force=False, merge_ids=None, raw=False, claim_id=None,
                                              then_wait=0, then_limit=5))
    e1, e2 = b.load_task("e1"), b.load_task("e2")
    ev = b.exec_breakdown()
    check(e1["execution"].get("warm") is True and e2["execution"].get("warm") is False
          and len(e2["execution"].get("calls") or []) == 1,
          "claims are tagged warm/cold; the agent's CLI calls are timestamped")
    check(all(k in e1["send"] for k in ("t_start", "first_req_at", "net_ms"))
          and ev["warm_claims"] == 1 and ev["cold_claims"] == 1 and ev["network"]["n"] == 2,
          "deliver is split into pick-up / local work / network", str(ev)[:300])


def t_followup_and_ticks():
    b = load("gaps")
    base = b.now_ms() - 3_600_000
    for i, (role, off) in enumerate((("user", 0), ("bot", 50_000), ("user", 80_000),
                                     ("bot", 130_000), ("user", 400_000))):
        b.history_append(role, f"turn {i}", at=base + off)
    g = b.followup_gaps()
    check(g["n"] == 2 and g["within"]["45s"] == 0.5 and g["within"]["300s"] == 1.0,
          "follow-up gaps: share of user replies a 45/90/180/300 s warm wait would catch", str(g))
    now = b.now_ms()
    lines = []
    t = now - 200_000
    for i in range(30):                      # platform counting from the END: start = prev end + 5 s
        lines.append(f"{t} {t + 3000} 0 3000 0 -")
        t += 3000 + 5000
    lines.append(f"{t} {t + 100} 1 0 0 123")
    lines.append(f"{t + 8000} {t + 8100} 1 0 0 123")   # same ids woken again within 60 s
    b.HOOK_TICKS_FILE.write_text("\n".join(lines) + "\n")
    s = b.hook_tick_stats(hours=1)
    check(s["n"] == 32 and s["duplicate_wakes"] == 1 and s.get("schedule") == "from the END of a run",
          "hook tick stats tell the platform's schedule and count duplicate wakes", str(s))
    rc, out = quiet(b.cmd_stats, SimpleNamespace(hours=72))
    ev = json.loads(out).get("evidence") or {}
    check(rc == 0 and set(ev) >= {"exec", "followup_gaps", "hook_ticks", "echo", "tuning"},
          "`stats` carries the evidence block")


for fn in (t_echo_observe_default, t_tuning_and_auto_wait, t_exec_evidence, t_followup_and_ticks,
           t_claim_has_no_window, t_fast_pickup_and_cursor, t_claim_waits_for_download,
           t_media_question_together, t_context_delta, t_then_wait, t_echo_resolution, t_echo_wait_notice,
           t_predigest, t_launcher, t_hook):
    try:
        fn()
    except Exception as e:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        check(False, f"{fn.__name__} raised {type(e).__name__}: {e}")

print("\nALL PERF CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED: {fails}")
sys.exit(1 if fails else 0)
