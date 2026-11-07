#!/usr/bin/env python3
"""Regression tests for the 2026-11-04 code review fixes. Every check here
failed (or could not be expressed) against v5.0.5. Offline, stdlib only;
ffmpeg-dependent checks are skipped when ffmpeg is missing.
Run: python3 tests/test_review_20261104.py
"""
import contextlib, errno, http.server, importlib.util, io, json, os, shutil, stat
import sys, tempfile, threading, time
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
fails = []
_n = [0]


def check(cond, name, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name + (f" ({detail})" if detail and not cond else ""))
    if not cond:
        fails.append(name)


def load(tag: str):
    _n[0] += 1
    d = Path(tempfile.mkdtemp(prefix=f"wb-review-{tag}-"))
    shutil.copy(HERE / "bridge.py", d / "bridge.py")
    spec = importlib.util.spec_from_file_location(f"b_{tag}_{_n[0]}", d / "bridge.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.log = lambda msg: None
    for p in (mod.STATE_DIR, mod.INBOX_DIR, mod.MEDIA_DIR, mod.FILES_DIR,
              mod.HISTORY_DIR, mod.QUEUE_DIR, mod.ATTACH_DIR, mod.LOG_DIR):
        p.mkdir(parents=True, exist_ok=True)
    return mod


def set_creds(b, user="owner"):
    b.atomic_write_json(b.CREDS_FILE, {"bot_token": "TKN", "ilink_bot_id": "bot",
                                       "ilink_user_id": user,
                                       "baseurl": "https://ilinkai.weixin.qq.com"})
    b.atomic_write_json(b.CONTEXT_TOKENS_FILE, {user: {"token": "CT", "at": b.now_ms()}})


def quiet(fn, *a, **k):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = fn(*a, **k)
    return rc, buf.getvalue()


# --------------------------------------------------------------- 1. ids/paths
def t_safe_ids():
    b = load("ids")
    b._fetch_bytes = lambda url, **kw: b"\x89PNG\r\n\x1a\n" + b"x" * 32
    out = b.download_image({"media": {"encrypt_query_param": "q"}}, "../../ESCAPED")
    p = Path(out["media_path"]).resolve()
    check(p.parent == b.MEDIA_DIR.resolve(), "message_id '../../x' stays inside inbox_media",
          str(p))
    check(b.safe_name("1234567890") == "1234567890" and b.safe_name("notice-1-ab") == "notice-1-ab",
          "plain ids keep their on-disk names")
    check(b.task_path("a.b") != b.task_path("a_b"), "task_path no longer maps a.b and a_b to one file")
    # a task stored under the v5.0.5 name is still found, then migrated
    legacy = b.INBOX_DIR / "a_b.json"
    legacy.write_text(json.dumps({"message_id": "a.b", "status": "sent"}))
    t = b.load_task("a.b")
    check(t is not None and t["message_id"] == "a.b", "legacy-named task file still found")
    check(b.load_task("a_b") is None, "a_b does not read a.b's legacy file")
    b.save_task(t)
    check(b.task_path("a.b").exists() and not legacy.exists(), "save migrates to the new name")


# --------------------------------------------------------------- 2. private writes
def t_private_writes():
    b = load("perm")
    old = os.umask(0o002)
    seen = {}
    real_replace = os.replace
    def spy(src, dst):
        if ".tmp" in str(src):
            seen[Path(dst).name] = stat.S_IMODE(os.stat(src).st_mode)
        return real_replace(src, dst)
    os.replace = spy
    try:
        b.atomic_write_json(b.CREDS_FILE, {"bot_token": "SECRET"})
        b.atomic_write_text(b.DECISIONS_FILE, "- x\n")
    finally:
        os.replace = real_replace
        os.umask(old)
    check(seen.get("credentials.json") == 0o600 and seen.get("DECISIONS.md") == 0o600,
          "tmp files are 0600 before the rename (json and text alike)", str(seen))
    b.atomic_write_json(b.STATE_DIR / "s.json", {"t": "a\udc80b"})
    check(b.read_json(b.STATE_DIR / "s.json") == {"t": "a\udc80b"},
          "a lone surrogate round-trips instead of crashing the write")
    old = os.umask(0o002)
    try:
        b.apply_private_umask()
        f = b.STATE_DIR / "plain.txt"
        f.write_text("x")
        d = b.BASE_DIR / "loose"
        d.mkdir()
        os.chmod(d, 0o775)
        b.harden_dirs([d])
        check(stat.S_IMODE(f.stat().st_mode) == 0o600 and stat.S_IMODE(d.stat().st_mode) == 0o700,
              "private umask: new files 0600, existing dirs tightened to 0700")
    finally:
        os.umask(old)


# --------------------------------------------------------------- 3. cursor/ingest
def _run_receiver(b, responses, on_call=None):
    calls = {"n": 0}
    def fake(creds, cur, timeout_ms):
        calls["n"] += 1
        if on_call:
            on_call(calls["n"], cur, timeout_ms)
        if calls["n"] > len(responses):
            b._stop.set()                   # like SIGTERM during the next poll
            raise b.GracefulStop()
        return responses[calls["n"] - 1]
    b.get_updates = fake
    b.notify = lambda creds, start: True
    b.POLL_RETRY_DELAYS_SEC = (0,)
    b._stop.clear()
    try:
        b.receiver_loop(b.load_creds())
    except b.GracefulStop:
        pass
    return calls["n"]


def t_cursor_kept_on_storage_error():
    b = load("cursor")
    set_creds(b)
    b.atomic_write_json(b.CURSOR_FILE, {"get_updates_buf": "CUR1"})
    msg = {"message_id": 777, "from_user_id": "owner", "message_type": 1,
           "item_list": [{"type": 1, "text_item": {"text": "重要消息"}}]}
    batch = {"ret": 0, "get_updates_buf": "CUR2", "msgs": [msg]}
    real_save, state = b.save_task, {"fail": 1}
    def flaky(task):
        if state["fail"]:
            state["fail"] -= 1
            raise OSError(errno.ENOSPC, "No space left on device")
        return real_save(task)
    b.save_task = flaky
    seen = []
    _run_receiver(b, [batch, batch], on_call=lambda n, cur, t: seen.append(cur))
    check(seen[:2] == ["CUR1", "CUR1"], "after a storage error the same batch is polled again",
          str(seen))
    check(b.load_task("777") is not None and b.read_json(b.CURSOR_FILE)["get_updates_buf"] == "CUR2",
          "message stored on the retry, cursor advanced only then")


def t_client_timeouts():
    b = load("silent")
    set_creds(b)
    b.POLL_SILENT_FAIL_AFTER = 3
    timeout = {"ret": 0, "msgs": [], "get_updates_buf": "", "_client_timeout": True}
    snap = {}
    def on_call(n, cur, t):
        hb = b.read_json(b.HEARTBEAT_FILE, {}) or {}
        snap[n] = hb.get("last_poll_ok_at")
    _run_receiver(b, [dict(timeout) for _ in range(5)], on_call=on_call)
    hb = b.read_json(b.HEARTBEAT_FILE, {}) or {}
    check(hb.get("client_timeout_streak") == 5 and hb.get("poll_fail_streak") == 3,
          "unanswered long polls are counted; from the 3rd on as poll failures", str(hb))
    check(snap.get(4) == hb.get("last_poll_ok_at"),
          "last_poll_ok_at stops advancing once polls get no answer at all")


def t_longpoll_clamp():
    b = load("clamp")
    set_creds(b)
    got = []
    _run_receiver(b, [{"ret": 0, "msgs": [], "get_updates_buf": "C",
                       "longpolling_timeout_ms": 999_999_999},
                      {"ret": 0, "msgs": [], "get_updates_buf": "C"}],
                  on_call=lambda n, cur, t: got.append(t))
    check(got[1] == b.LONG_POLL_MAX_MS, "server long-poll hint clamped (watchdog-safe)", str(got))


# --------------------------------------------------------------- 4. login
def t_wait_login():
    b = load("login")
    seq = iter([{"status": "scaned_but_redirect", "redirect_host": "evil.example.com"},
                {"status": "expired"},
                {"status": "scaned_but_redirect", "redirect_host": "sz.ilinkai.weixin.qq.com"},
                {"status": "confirmed", "bot_token": "T", "ilink_bot_id": "botid123456",
                 "ilink_user_id": "user12345678", "baseurl": "https://evil.example.com"}])
    bases = []
    def fake_get(base, ep, timeout):
        bases.append(base)
        return next(seq)
    b.http_get = fake_get
    b.http_post = lambda *a, **k: {"qrcode": "Q2", "qrcode_img_content": "https://x/qr"}
    b.make_qr_png = lambda c: False
    b.atomic_write_json(b.LOGIN_FILE, {"qrcode": "Q1", "qrcode_img_content": "x"})
    real_sleep = time.sleep
    time.sleep = lambda s: None
    try:
        rc, out = quiet(b.cmd_wait_login, SimpleNamespace(timeout=30, verify_code=None))
    finally:
        time.sleep = real_sleep
    try:
        res = json.loads(out)
    except ValueError:
        res = None
    check(res is not None and res.get("connected") and res.get("qr_refreshes") == 1,
          "wait-login prints ONE JSON object, telling that the QR was refreshed", out[:200])
    check(bases[1] == b.FIXED_BASE_URL and bases[3] == "https://sz.ilinkai.weixin.qq.com",
          "redirect_host outside *.weixin.qq.com is ignored, an allowed one is followed",
          str(bases))
    check((b.load_creds() or {}).get("baseurl") == b.DEFAULT_BASE_URL,
          "a non-Tencent baseurl from the login response is not saved")
    try:
        b.api_base({"baseurl": "https://evil.example.com"})
        refused = False
    except b.ApiError as e:
        refused = e.kind == "config"
    os.environ["WECHAT_BRIDGE_API_ORIGINS"] = "https://hub.example.com"
    try:
        hub = b.api_base({"baseurl": "https://hub.example.com"})
    finally:
        os.environ.pop("WECHAT_BRIDGE_API_ORIGINS", None)
    check(refused and hub == "https://hub.example.com",
          "the bot token only goes to allowlisted API origins (env can add a hub)")


# --------------------------------------------------------------- 5. wire format
def t_http_body():
    b = load("body")
    cap = {}
    class Resp:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b'{"ret":0}'
    def fake_open(req, timeout):
        cap["data"] = req.data
        return Resp()
    b.PROXY_POOL.open = fake_open
    b.http_post("https://ilinkai.weixin.qq.com", "ilink/bot/sendmessage",
                {"msg": {"text": "中文" * 3 + "\udc80"}}, {}, 5)
    data = cap["data"]
    check("中文".encode() in data and b'": "' not in data and json.loads(data)["msg"]["text"].endswith("\udc80"),
          "request body is raw, compact UTF-8 (half the bytes for CJK); lone surrogate stays valid JSON",
          data[:60])


# --------------------------------------------------------------- 6. commands
def t_command_interception():
    b = load("cmd")
    check(b.parse_settings_command("设置一个明天早上8点的提醒") is None
          and b.parse_settings_command("设置一下服务器防火墙，只开 22 和 443") is None,
          "requests that merely start with 设置 go to Muse")
    check("没这个设置项" in (b.parse_settings_command("设置 不存在的东西") or "")
          and b.parse_settings_command("设置 媒体不等") and b.parse_settings_command("设置") ,
          "explicit settings commands still answered by the bridge")
    check(b.detect_command("查记录一下昨天的会议纪要有没有发出去") is None
          and b.detect_command("查记录 备份") == "history_search:备份"
          and b.detect_command("查记录") == "history_search:",
          "查记录 needs a separator; plain requests go to Muse")


# --------------------------------------------------------------- 7. history
def t_history():
    b = load("hist")
    b.HISTORY_MAX_BYTES = 20_000
    writes = {"n": 0}
    real_replace = os.replace
    def spy(src, dst):
        if str(dst).endswith("chat.jsonl"):
            writes["n"] += 1
        return real_replace(src, dst)
    os.replace = spy
    try:
        for i in range(300):
            b.history_append("bot", f"第{i}条 " + "回复" * 150)
    finally:
        os.replace = real_replace
    size = b.HISTORY_FILE.stat().st_size
    live = b.history_read()
    arch = b.history_read_all()
    check(writes["n"] <= 40 and size <= b.HISTORY_MAX_BYTES,
          f"rotation is byte-bounded: {writes['n']} rewrites for 300 appends, live {size} B")
    check(len(arch) == 300 and len(live) < 300, "nothing lost: live + archive hold every turn")
    hits, trunc = b.history_search("第3条", 5)
    check(hits and all("第3条" in h["text"] for h in hits) and not trunc,
          "history_search finds lines in archives, newest first, bounded")


# --------------------------------------------------------------- 8. decisions
def t_decisions():
    b = load("dec")
    text = ("规则：先读这里。\n\n## 进行中\n\n- 进行中事项\n\n## 已决定（口径）\n\n- 口径一\n\n"
            "## 已完成\n\n" + "- 完成事项很长很长\n" * 60 +
            "\n## v5.0.4 回灌\n\n" + "- 回灌细节\n" * 40 +
            "\n## 微信公式默认 Unicode（立规矩）\n\n- 公式用 Unicode\n")
    out = b.decisions_for_prompt(text, 600)
    check("进行中事项" in out and "口径一" in out and "公式用 Unicode" in out
          and "完成事项很长很长\n- 完成事项很长很长\n- 完成事项很长很长\n- 完成事项很长很长" not in out
          and "未带上" in out and len(out) <= 800,
          "over budget: open + standing-rule sections kept (incl. the newest rule), history left out",
          f"{len(out)} chars")
    rc, _ = quiet(b.cmd_decisions, SimpleNamespace(add="新决定\u2028## 已决定 注入\x85尾巴"))
    body = b.DECISIONS_FILE.read_text(encoding="utf-8")
    heads = [ln for ln in body.splitlines() if ln.startswith("## ")]
    check(rc == 0 and heads == ["## 进行中"] and "\u2028" not in body and "\x85" not in body,
          "decisions --add flattens every kind of line break", repr(heads))
    rc2, _ = quiet(b.cmd_decisions, SimpleNamespace(add="长" * 600))
    rc3, _ = quiet(b.cmd_decisions, SimpleNamespace(add="\u2028 \x00"))
    check(rc2 == 2 and rc3 == 2, "decisions --add refuses over-long and empty items")


# --------------------------------------------------------------- 9. claim fencing
def _complete_args(tid, **kw):
    a = dict(task_id=tid, result="答复", result_file=None, error=None, attach=[],
             attach_as="auto", wait_attachments=0, force=False, merge_ids=None,
             raw=False, claim_id=None)
    a.update(kw)
    return SimpleNamespace(**a)


def _pending(b, mid):
    b.save_task({"message_id": mid, "from_user_id": "owner", "kind": "text", "text": "问题",
                 "status": "pending_execution", "received_at": b.now_ms(),
                 "pending_at": b.now_ms(), "execution": None, "send": None,
                 "context_token": "CT"})


def _age_claim(b, mid):
    t = b.load_task(mid)
    t["execution"]["claimed_at"] = b.now_ms() - 11 * 60_000
    b.save_task(t)


def t_claim_fencing():
    b = load("claim")
    set_creds(b)
    b.deliver = lambda tid: {"ok": True, "via": "test"}
    _pending(b, "41")
    c1 = b._claim_pending(1)[0]["execution"]["claim_id"]
    _age_claim(b, "41")
    b._reclaim_stale()
    c2 = b._claim_pending(1)[0]["execution"]["claim_id"]
    rc, out = quiet(b.cmd_complete, _complete_args("41", claim_id=c1))
    rcp, outp = quiet(b.cmd_progress, SimpleNamespace(task_id="41", note=None, stage="x",
                                                      notify=False, hold_minutes=None,
                                                      claim_id=c1))
    check(c1 != c2 and rc == 2 and "stale claim" in out and rcp == 3 and "stale_claim" in outp,
          "a reclaimed executor's complete/progress is refused once another one claimed the task")
    rc, out = quiet(b.cmd_complete, _complete_args("41", claim_id=c2))
    check(rc == 0 and b.load_task("41")["status"] == "waiting_send",
          "the current claim holder completes normally")
    _pending(b, "42")
    c3 = b._claim_pending(1)[0]["execution"]["claim_id"]
    _age_claim(b, "42")
    b._reclaim_stale()
    rc, _ = quiet(b.cmd_heartbeat, SimpleNamespace(task_id="42", stage="还在跑", claim_id=c3))
    t = b.load_task("42")
    check(rc == 0 and t["status"] == "executing" and t["execution"].get("claim_id") == c3,
          "reclaimed but not yet re-claimed: the live executor's heartbeat re-adopts it")
    rc, out = quiet(b.cmd_claim, SimpleNamespace(limit=1, full=False, wait=0, settle_ms=0))
    _pending(b, "43")
    rc, out = quiet(b.cmd_claim, SimpleNamespace(limit=1, full=False, wait=0, settle_ms=0))
    res = json.loads(out)
    check(res.get("claim_id") and set(res["tasks"][0]) == {"message_id", "kind", "text"},
          "claim output carries one top-level claim_id; the slim task view is unchanged")


# --------------------------------------------------------------- 10. shutdown
def t_shutdown_mid_reply():
    b = load("stop")
    set_creds(b)
    text = "第一段\n\n" + "a" * 3500 + "\n\n" + "b" * 3500
    b.save_task({"message_id": "51", "from_user_id": "owner", "kind": "text",
                 "status": "waiting_send", "result_text": text, "received_at": b.now_ms(),
                 "context_token": "CT", "execution": {"finished_at": b.now_ms()}, "send": None})
    sent = []
    def fake_send(creds, to, body, ctx, client_id=None):
        sent.append(body)
        b._stop.set()                       # serve receives SIGTERM after part 1
        return {"ret": 0, "_client_id": client_id, "message_id": f"m{len(sent)}"}
    b.send_text = fake_send
    b._stop.clear()
    b.attempt_send("51")
    t = b.load_task("51")
    s = t["send"]
    check(len(sent) == 1 and t["status"] == "send_failed" and s.get("attempts") == 0
          and "serve stopped" in s.get("error", "") and not s.get("deferred"),
          "stop mid-reply: no new part started, not parked as quota, no attempt used", str(s))
    b._stop.clear()
    b.send_text = lambda creds, to, body, ctx, client_id=None: (
        sent.append(body) or {"ret": 0, "_client_id": client_id, "message_id": "m9"})
    b.flush_outbox()
    check(b.load_task("51")["status"] == "sent" and len(sent) == 2 and sent[1].startswith("b"),
          "after the restart only the remaining part goes out")


# --------------------------------------------------------------- 11. media integrity
class _BadRange(http.server.BaseHTTPRequestHandler):
    data = bytes(range(256)) * 64
    def log_message(self, *a): pass
    def do_GET(self):
        rng = self.headers.get("Range")
        self.send_response(206 if rng else 200)
        if rng:     # wrong: always serves from byte 0
            self.send_header("Content-Range", f"bytes 0-{len(self.data) - 1}/{len(self.data)}")
        self.send_header("Content-Length", str(len(self.data)))
        self.end_headers()
        self.wfile.write(self.data)


def t_content_range():
    b = load("range")
    os.environ.pop("HTTPS_PROXY", None); os.environ.pop("https_proxy", None)
    b.PROXY_POOL.init()
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _BadRange)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    try:
        dst = b.MEDIA_DIR / ".9.part"
        dst.write_bytes(b"\xee" * 1000)      # earlier partial download
        b._fetch_to_file(f"http://127.0.0.1:{srv.server_address[1]}/x", dst, 10, 10 ** 7)
        check(dst.read_bytes() == _BadRange.data,
              "a 206 that does not continue at our offset restarts from 0 (no spliced file)")
    finally:
        srv.shutdown()


def t_video_compress_gate():
    b = load("vid")
    big = b.BASE_DIR / "big.mp4"
    with open(big, "wb") as f:
        f.truncate(60 * 1024 * 1024)
    if shutil.which("ffmpeg"):
        plan = b.plan_attachment(big, "auto")
        check(plan["kind"] == "video" and "re-encoded" in (plan.get("note") or ""),
              "a 60 MB video is accepted for re-encoding instead of refused")
    else:
        print("SKIP  video compression gate: no ffmpeg")
    b.COMPRESS_ENABLED = False
    try:
        b.plan_attachment(big, "auto")
        refused = False
    except b.AttachError as e:
        refused = e.code == "too_large"
    check(refused, "with compression off, an over-limit video is still refused")


def t_encrypt_classification():
    b = load("enc")
    def boom(*a, **k):
        raise RuntimeError("no AES")
    b._aes_bytes = boom
    try:
        b._encrypt_aes_ecb(b"x", b"k" * 16)
        kind = None
    except b.ApiError as e:
        kind = e.kind
    check(kind == "business", "a local encryption failure is not retried as a network error")


# --------------------------------------------------------------- 12. auto-attach
def t_auto_attach():
    b = load("att")
    home = Path(tempfile.mkdtemp(prefix="wb-home-"))
    files = {".netrc": "x", ".docker/config.json": "{}", ".kube/config": "x",
             ".config/gh/hosts.yml": "x", "out/chart.png": "png"}
    for rel, body in files.items():
        p = home / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(body)
    b.HISTORY_FILE.write_text("{}\n")
    (b.INBOX_DIR / "1.json").write_text("{}")
    refs = [home / r for r in files] + [b.HISTORY_FILE, b.INBOX_DIR / "1.json"]
    refused = []
    _, found = b.extract_local_refs("\n".join(f"[f]({r})" for r in refs), refused)
    names = sorted(p.name for p in found)
    check(names == ["chart.png"] and len(refused) == 6,
          "auto-attach takes ordinary files only (hidden paths, bridge private dirs refused)",
          f"found={names} refused={refused}")
    os.environ["WECHAT_BRIDGE_AUTO_ATTACH_DIRS"] = str(home / "elsewhere")
    try:
        _, found2 = b.extract_local_refs(f"[c]({home / 'out/chart.png'})")
    finally:
        os.environ.pop("WECHAT_BRIDGE_AUTO_ATTACH_DIRS", None)
    check(found2 == [], "WECHAT_BRIDGE_AUTO_ATTACH_DIRS restricts auto-attach to those roots")


# --------------------------------------------------------------- 13. env / vendor / typing
def t_proxy_env():
    b = load("penv")
    saved = {k: os.environ.get(k) for k in b.PROXY_VARS}
    for k in b.PROXY_VARS:
        os.environ.pop(k, None)
    os.environ["HTTPS_PROXY"] = "http://p:3128\nLD_PRELOAD=/tmp/evil.so"
    os.environ["HTTP_PROXY"] = "http://p:3128"
    try:
        rc, out = quiet(b.cmd_write_proxy_env, None)
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
    body = b.PROXY_ENV_FILE.read_text()
    res = json.loads(out)
    check(rc == 0 and res.get("rejected_vars") == ["HTTPS_PROXY"] and "LD_PRELOAD" not in body
          and stat.S_IMODE(b.PROXY_ENV_FILE.stat().st_mode) == 0o600,
          "write-proxy-env refuses values that would inject variables into the root unit")


def t_vendor_path():
    b = load("vend")
    vend = b.BASE_DIR / "vendor" / "py"
    vend.mkdir(parents=True)
    b._add_vendor_paths()
    ok = str(vend) in sys.path and sys.path.index(str(vend)) > 0
    with contextlib.suppress(ValueError):
        sys.path.remove(str(vend))
    b2 = load("vend2")
    v2 = b2.BASE_DIR / "vendor" / "py"
    v2.mkdir(parents=True)
    os.chmod(v2, 0o777)
    b2._add_vendor_paths()
    check(ok and str(v2) not in sys.path,
          "vendor dirs are appended (never shadow the stdlib); a world-writable one is ignored")


def t_typing_errcode():
    b = load("typ")
    b.bot_post = lambda creds, ep, body, timeout: {"ret": 0, "errcode": -14}
    check(b.TYPING._send({}, "u", "tk", 1) is False and b.TYPING.sent == 0,
          "sendtyping honours errcode like every other send path")


# --------------------------------------------------------------- 14. notices / misc
def t_corrupt_notice():
    b = load("corrupt")
    set_creds(b)
    (b.INBOX_DIR / "x.json.corrupt").write_text("{bad")
    b.check_corrupt_tasks()
    b.check_corrupt_tasks()
    notices = [t for t in b.list_tasks() if t.get("kind") == "notice"]
    check(len(notices) == 1 and "损坏" in notices[0]["result_text"],
          "a quarantined task file is reported to the owner once")


def t_backup_size_dedupe():
    b = load("bk")
    snap = b.BASE_DIR / "backups" / "20261104-120000"
    (snap / "state").mkdir(parents=True)
    f = snap / "state" / "credentials.json"
    f.write_bytes(b"x" * 100_000)
    snap2 = b.BASE_DIR / "backups" / "20261104-130000" / "state"
    snap2.mkdir(parents=True)
    os.link(f, snap2 / "credentials.json")
    txt = b.backup_status_command()
    check("97.7 KB" in txt or "合计约 97.7 KB" in txt,
          "backup totals count hard-linked snapshot files once", txt.splitlines()[2] if len(txt.splitlines()) > 2 else txt)


def t_one_line():
    b = load("one")
    s = b.one_line("a\u2028b\u2029c\x85d\ve\u202ef\u200dg")
    check("\u2028" not in s and "\u202e" not in s and "\u200d" in s and "\n" not in s,
          "one_line removes all line-break kinds and bidi controls, keeps ZWJ", repr(s))


for fn in (t_safe_ids, t_private_writes, t_cursor_kept_on_storage_error, t_client_timeouts,
           t_longpoll_clamp, t_wait_login, t_http_body, t_command_interception, t_history,
           t_decisions, t_claim_fencing, t_shutdown_mid_reply, t_content_range,
           t_video_compress_gate, t_encrypt_classification, t_auto_attach, t_proxy_env,
           t_vendor_path, t_typing_errcode, t_corrupt_notice, t_backup_size_dedupe, t_one_line):
    try:
        fn()
    except Exception as e:  # noqa: BLE001 - report and continue
        import traceback
        traceback.print_exc()
        check(False, f"{fn.__name__} raised {type(e).__name__}: {e}")

print("\nALL REVIEW-FIX CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED: {fails}")
sys.exit(1 if fails else 0)
