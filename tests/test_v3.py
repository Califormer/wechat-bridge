#!/usr/bin/env python3
"""v3 tests (offline): transport (keep-alive, split timeouts), send
semantics, CDN upload, ingest, typing, reply handoff, re-login.
A local TLS server and a local CONNECT proxy stand in for iLink and the
egress proxy (with simulated latency, idle-close and black-hole modes).
Before/after numbers use bridge.py.v2 (the previous version) if present.
Run: python3 tests/test_v3.py"""
import hashlib, http.server, importlib.util, io, json, os, random, shutil
import signal, socket, socketserver, ssl, string, subprocess, sys, tempfile
import threading, time, tracemalloc, urllib.request
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
NEW, OLD = HERE / "bridge.py", HERE / "bridge.py.v2"
HAVE_OLD = OLD.exists()
fails = []
HDR = {"Content-Type": "application/json"}


def check(cond, msg, detail=""):
    print(("PASS  " if cond else "FAIL  ") + msg + (f"\n      {detail}" if detail else ""))
    if not cond:
        fails.append(msg)


def load(src, name):
    d = Path(tempfile.mkdtemp(prefix="v3-"))
    shutil.copy(src, d / "bridge.py")
    spec = importlib.util.spec_from_file_location(name, d / "bridge.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.logs = []
    mod.log = lambda m: mod.logs.append(m)
    for p in (mod.STATE_DIR, mod.INBOX_DIR, mod.LOG_DIR):
        p.mkdir(parents=True, exist_ok=True)
    mod._stop.clear()
    if hasattr(mod, "_ssl_context"):
        mod._ssl_context = lambda: CLIENT_CTX
    return mod


def now_ms():
    return int(time.time() * 1000)


def mk_task(b, mid, status, **kw):
    t = {"message_id": str(mid), "from_user_id": "owner", "to_user_id": "bot",
         "session_id": "s", "context_token": "CTX", "received_at": now_ms(),
         "kind": "text", "text": "q", "status": status, "raw_items": [],
         "result_text": None, "execution": None, "send": None}
    t.update(kw)
    b.save_task(t)
    return t


def set_creds(b, token="T"):
    b.atomic_write_json(b.CREDS_FILE, {"bot_token": token, "ilink_user_id": "owner",
                                       "ilink_bot_id": "bot", "baseurl": "https://localhost:9"})


def use_proxy(url):
    for k in ("http_proxy", "HTTP_PROXY", "https_proxy", "HTTPS_PROXY", "all_proxy", "ALL_PROXY"):
        os.environ.pop(k, None)
    os.environ["no_proxy"] = os.environ["NO_PROXY"] = ""
    if url:
        os.environ["https_proxy"] = os.environ["HTTPS_PROXY"] = url
    # urlopen() caches env proxies in a process-global opener on first use
    # (the v2 code path); drop it so the next scenario's proxy is picked up.
    urllib.request.install_opener(None)


def cli(fn, **kw):
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = fn(SimpleNamespace(**kw))
    return rc, json.loads(buf.getvalue())


# ---------------------------------------------------------------- fixtures
TMP = Path(tempfile.mkdtemp(prefix="v3tls-"))
subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", str(TMP / "k.pem"), "-out", str(TMP / "c.pem"), "-days", "1",
                "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"],
               check=True, capture_output=True)
SERVER_CTX = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
SERVER_CTX.load_cert_chain(TMP / "c.pem", TMP / "k.pem")
CLIENT_CTX = ssl.create_default_context(cafile=str(TMP / "c.pem"))
ssl._create_default_https_context = lambda: CLIENT_CTX     # urllib paths (v2, probes)


class Api(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n)
        self.server.requests.append((self.path, len(body), hashlib.md5(body).hexdigest()))
        if self.path.startswith("/upload"):
            self.send_response(200)
            self.send_header("x-encrypted-param", "P")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        out = json.dumps({"ret": 0, "msgs": []}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out)))
        self.end_headers()
        self.wfile.write(out)


class TLSServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True

    def __init__(self, idle=None):
        super().__init__(("127.0.0.1", 0), type("H", (Api,), {"timeout": idle}))
        self.socket = SERVER_CTX.wrap_socket(self.socket, server_side=True)
        self.requests = []
        self.base = f"https://localhost:{self.server_address[1]}"
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def handle_error(self, request, client_address):
        pass


class Proxy:
    """CONNECT proxy. delay: CONNECT setup time and the round-trip time of
    every request/response exchange in the tunnel (cross-border egress)."""

    def __init__(self, delay=0.0, blackhole=False):
        self.delay, self.blackhole, self.tunnels = delay, blackhole, 0
        self.s = socket.socket()
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.s.bind(("127.0.0.1", 0))
        self.s.listen(64)
        self.url = f"http://127.0.0.1:{self.s.getsockname()[1]}"
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while True:
            try:
                c, _ = self.s.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(c,), daemon=True).start()

    def _handle(self, c):
        try:
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = c.recv(4096)
                if not chunk:
                    return c.close()
                data += chunk
            if self.blackhole:
                time.sleep(30)
                return c.close()
            host, port = data.split(b"\r\n")[0].split()[1].decode().rsplit(":", 1)
            time.sleep(self.delay)
            u = socket.create_connection((host, int(port)))
            self.tunnels += 1
            c.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
        except OSError:
            return c.close()

        def fwd(src, dst):
            try:
                while True:
                    d = src.recv(65536)
                    if not d:
                        break
                    if self.delay:
                        time.sleep(self.delay / 2)
                    dst.sendall(d)
            except OSError:
                pass
            for s_ in (src, dst):
                try:
                    s_.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        t = threading.Thread(target=fwd, args=(u, c), daemon=True)
        t.start()
        fwd(c, u)
        t.join(5)
        c.close()
        u.close()


# ---------------------------------------------------------------- 1 transport
def g_keepalive():
    srv, px = TLSServer(), Proxy(delay=0.08)
    use_proxy(px.url)
    b = load(NEW, "ka")
    b.PROXY_POOL.init()
    n = 8
    t0 = time.perf_counter()
    for _ in range(n):
        assert b.http_post(srv.base, "ilink/bot/getupdates", {"get_updates_buf": ""}, HDR, 10) == {"ret": 0, "msgs": []}
    new_ms = (time.perf_counter() - t0) / n * 1000
    new_tunnels, px.tunnels = px.tunnels, 0
    t0 = time.perf_counter()
    if HAVE_OLD:
        o = load(OLD, "ka_old")
        for _ in range(n):
            o.http_post(srv.base, "ilink/bot/getupdates", {"get_updates_buf": ""}, HDR, 10)
    else:
        op = urllib.request.build_opener(urllib.request.ProxyHandler({"https": px.url}))
        for _ in range(n):
            with op.open(urllib.request.Request(srv.base + "/x", data=b"{}", headers=HDR), timeout=10) as r:
                r.read()
    old_ms = (time.perf_counter() - t0) / n * 1000
    check(new_tunnels == 1 and px.tunnels == n,
          f"keep-alive: {n} API calls through the proxy -> {new_tunnels} tunnel (v2: {px.tunnels})",
          f"mean latency per call, 80 ms-RTT proxy: v2 {old_ms:.0f} ms -> v3 {new_ms:.0f} ms "
          f"({old_ms / new_ms:.1f}x)")


def g_stale():
    srv, px = TLSServer(idle=0.6), Proxy()
    use_proxy(px.url)
    b = load(NEW, "stale")
    b.PROXY_POOL.init()
    b.http_post(srv.base, "ilink/bot/getupdates", {}, HDR, 10)
    time.sleep(1.0)                        # server idle-closed the connection
    ok1 = b.http_post(srv.base, "ilink/bot/getupdates", {}, HDR, 10) == {"ret": 0, "msgs": []}
    time.sleep(1.0)
    before = len(srv.requests)
    ok2 = b.http_post(srv.base, "ilink/bot/sendmessage", {"msg": {}}, HDR, 10) == {"ret": 0, "msgs": []}
    once = len(srv.requests) == before + 1
    check(ok1 and ok2 and once and px.tunnels == 3,
          "idle-closed keep-alive connection is detected before reuse: getupdates and sendmessage "
          "both succeed, sendmessage reaches the server exactly once",
          f"tunnels={px.tunnels} stale_retries={b._NET_STATS['stale_retries']}")
    srv2, px2 = TLSServer(), Proxy()
    use_proxy(px2.url)
    b2 = load(NEW, "cap")
    b2.PROXY_POOL.init()
    b2.IDLE_REUSE_MAX_SEC_UNSAFE = 0.3
    b2.http_post(srv2.base, "ilink/bot/sendtyping", {}, HDR, 10)
    b2.http_post(srv2.base, "ilink/bot/sendmessage", {}, HDR, 10)      # warm: reused
    warm = px2.tunnels
    time.sleep(0.5)
    b2.http_post(srv2.base, "ilink/bot/sendmessage", {}, HDR, 10)      # idle > cap: fresh
    b2.http_post(srv2.base, "ilink/bot/getconfig", {}, HDR, 10)        # idempotent: reuses
    check(warm == 1 and px2.tunnels == 2,
          "sendmessage reuses only warm connections (idle cap); idempotent calls reuse longer",
          f"tunnels after warm send={warm}, after idle send + getconfig={px2.tunnels}")


def g_connect_phase():
    bh = Proxy(blackhole=True)
    use_proxy(bh.url)
    b = load(NEW, "bh")
    b.CONNECT_TIMEOUT_SEC = 1
    b.PROXY_POOL.init()
    creds = {"bot_token": "t", "baseurl": "https://localhost:9"}
    t0 = time.perf_counter()
    try:
        b.get_updates(creds, "", 35_000)
        kind = "empty-ok"
    except b.ApiError as e:
        kind = e.kind
    dt = time.perf_counter() - t0
    detail = ""
    if HAVE_OLD:
        o = load(OLD, "bh_old")
        t1 = time.perf_counter()
        res = o.get_updates(creds, "", -8000)          # 2 s socket timeout
        detail = f"v2 on the same proxy: returned {res} after {time.perf_counter() - t1:.1f}s = 'healthy empty poll'"
    check(kind == "connect" and dt < 3,
          f"black-holed proxy: get_updates fails as kind=connect in {dt:.1f}s (counted, hook can see it)", detail)
    set_creds(b)
    mk_task(b, 50, "waiting_send", result_text="hello")
    b.attempt_send("50")
    t = b.load_task("50")
    s = t["send"]
    retry_s = (s.get("next_retry_at", 0) - now_ms()) / 1000
    detail = ""
    if HAVE_OLD:
        set_creds(o)
        mk_task(o, 50, "waiting_send", result_text="hello")
        real = o.http_post
        o.http_post = lambda base, ep, body, h, timeout: real(base, ep, body, h, 2)
        o.attempt_send("50")
        detail = f"v2: {o.load_task('50')['status']} (needs a human decision)"
    check(t["status"] == "send_failed" and s.get("not_delivered") and 5 < retry_s <= 10 and s.get("max_attempts") == 6,
          f"reply that cannot even connect: send_failed + auto-retry in {retry_s:.0f}s (6 attempts), not send_uncertain",
          detail)


# ---------------------------------------------------------------- 2 send semantics
def fake_sender(b, fail_at=(), exc=None, reject_over=None):
    calls = []

    def fake(creds, to, text, ctx, client_id=None):
        calls.append((text[:3], client_id, len(text)))
        if reject_over and len(text) > reject_over:
            raise b.ApiError("business", "sendmessage ret=-1 errmsg=too long", code=-1)
        if len(calls) in fail_at:
            raise exc
        return {"ret": 0, "_client_id": client_id, "message_id": "m"}
    b.send_text = fake
    return calls


def g_send():
    b = load(NEW, "send")
    set_creds(b)
    text = "\n".join(f"P{i}-" + "字" * 1500 for i in range(1, 7))   # 3 parts at 4000
    calls = fake_sender(b, fail_at=(2,), exc=b.ApiError("network", "reset"))
    mk_task(b, 1, "waiting_send", result_text=text)
    b.attempt_send("1")
    t = b.load_task("1")
    mid = (t["status"], t["send"]["done_upto"], t["send"]["pending"]["client_id"] == calls[1][1])
    rc, out = cli(b.cmd_send, task_id="1")
    order = [c[0] for c in calls]
    detail = ""
    if HAVE_OLD:
        o = load(OLD, "send_old")
        set_creds(o)
        mk_task(o, 1, "send_uncertain", result_text=text, send={"attempts": 1, "no_auto_retry": True})
        orc, oout = cli(o.cmd_send, task_id="1")
        detail = f"v2 `send` on the same task: rc={orc} {oout}"
    check(mid[0] == "send_uncertain" and mid[2] and rc == 0 and order == ["P1-", "P3-", "P3-", "P5-"]
          and calls[2][1] == calls[1][1] and b.load_task("1")["status"] == "sent",
          "human-approved resend (`send`) resumes at the unconfirmed part with its client_id; part 1 not repeated",
          detail)

    b2 = load(NEW, "exhaust")
    set_creds(b2)
    fake_sender(b2, fail_at=(1, 2, 3), exc=b2.ApiError("business", "ret=-1", code=-1))
    mk_task(b2, 2, "waiting_send", result_text="x")
    b2.attempt_send("2")
    for _ in range(2):
        t = b2.load_task("2")
        t["send"]["next_retry_at"] = 0
        b2.save_task(t)
        b2.flush_outbox()
    t = b2.load_task("2")
    check(t["status"] == "send_uncertain" and t["send"].get("exhausted") and t["send"].get("not_delivered"),
          "reply rejected 3 times: surfaced as send_uncertain(exhausted) for the hook, not parked silently",
          f"send record: attempts={t['send'].get('attempts')} error={t['send'].get('error')}")

    b3 = load(NEW, "fallback")
    set_creds(b3)
    calls = fake_sender(b3, reject_over=1800)
    mk_task(b3, 3, "waiting_send", result_text="y" * 3900)
    out = b3.attempt_send("3")
    check(out["ok"] and [c[2] for c in calls] == [3900, 1800, 1800, 300] and b3._chunk_limit() == 1800,
          "4000-char parts: if the server rejected one, it is re-split at 1800 and the limit kept for 24 h")

    b4 = load(NEW, "ratelimit")
    set_creds(b4)
    calls = fake_sender(b4, fail_at=(1,), exc=b4.ApiError("business", "ret=-2", code=-2))
    mk_task(b4, 4, "waiting_send", result_text="z")
    b4.attempt_send("4")
    retry_s = (b4.load_task("4")["send"]["next_retry_at"] - now_ms()) / 1000
    mk_task(b4, 5, "waiting_send", result_text="later")
    skipped = b4.flush_outbox() == [] and len(calls) == 1
    b4.ingest_message({"ilink_user_id": "owner"}, {"message_id": "u1", "from_user_id": "owner", "message_type": 1,
                                                   "item_list": [{"type": 1, "text_item": {"text": "hi"}}]})
    lifted = b4.rate_limited_until() == 0
    sent_after = [r["task_id"] for r in b4.flush_outbox()]
    # v5: the user's message also re-queues the reply that hit the limit,
    # so it goes out first instead of waiting out its own 90 s.
    expect_after = ["4", "5"] if hasattr(b4, "_resume_deferred") else ["5"]
    check(retry_s > 85 and skipped and lifted and sent_after == expect_after,
          f"-2 rate limit: retry in {retry_s:.0f}s, other sends wait, a user message lifts the deferral")

    rnd = random.Random(7)
    para = lambda: "".join(rnd.choice("微信桥接abcdef 。，") for _ in range(rnd.choice((40, 300, 900, 2500, 5000))))
    texts = ["\n".join(para() for _ in range(rnd.randint(1, 12))) for _ in range(300)]
    eq = cover = True
    for tx in texts:
        for lim in (1800, 4000):
            spans = b.split_spans(tx, lim)
            cover &= all(e - s <= lim for s, e in spans) and all(
                any(s <= i < e for s, e in spans) for i, ch in enumerate(tx) if ch != "\n")
            if HAVE_OLD:
                eq &= b.split_text(tx, lim) == o.split_text(tx, lim)
    typical = "\n".join("这是一段典型的回复内容，" * 25 for _ in range(30))   # 30 x 300 chars
    detail = (f"a typical {len(typical)}-char reply: {len(o.split_text(typical))} sendmessage calls in v2 -> "
              f"{len(b.split_text(typical))} now" if HAVE_OLD else "")
    check(eq and cover, "split_spans: identical parts to v2 at the same limit, every character covered, parts <= limit",
          detail)


# ---------------------------------------------------------------- 3 CDN upload
def g_cdn():
    d = Path(tempfile.mkdtemp(prefix="v3-"))   # 2026-11-05 §8-4: was unprefixed (tmp*)
    small = d / "a.jpg"
    small.write_bytes(os.urandom(300_000))
    creds = {"bot_token": "t"}
    b = load(NEW, "cdn")
    b.bot_post = lambda c, ep, body, timeout: {"upload_full_url": "https://cdn.qq.com/upload"}
    b.CDN_RETRY_DELAYS_SEC = (0.01, 0.01)
    calls = []

    def flaky(url, path, size):
        calls.append(size)
        if len(calls) < 3:
            raise b.ApiError("network", "CDN upload error: reset(ConnectionResetError)")
        return 200, {"x-encrypted-param": "P"}, b""
    b._cdn_post_file = flaky
    res = b._upload_media_to_cdn(creds, small, "owner", 1, 10 << 20)
    detail = ""
    if HAVE_OLD:
        o = load(OLD, "cdn_old")
        o.bot_post = b.bot_post
        ocalls = []

        def oflaky(url, data):
            ocalls.append(1)
            raise o.ApiError("network", "CDN upload error: ConnectionResetError")
        o._cdn_post_bytes = oflaky
        try:
            o._upload_media_to_cdn(creds, small, "owner", 1, 10 << 20)
            ores = "ok"
        except o.ApiError:
            ores = "failed"
        detail = f"v2: {ores} after {len(ocalls)} attempt (a single dropped connection aborted the send)"
    check(res["download_param"] == "P" and len(calls) == 3,
          "CDN upload survives two dropped connections (retried like upstream cdn-upload.ts)", detail)

    big = d / "v.mp4"
    big.write_bytes(os.urandom(24 << 20))

    def stream_ok(url, path, size):
        h = hashlib.md5()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return 200, {"x-encrypted-param": "P"}, b""
    b._cdn_post_file = stream_ok
    tracemalloc.start()
    b._upload_media_to_cdn(creds, big, "owner", 2, 50 << 20)
    peak_new = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    peak_old = None
    if HAVE_OLD:
        o._cdn_post_bytes = lambda url, data: (200, {"x-encrypted-param": "P"}, b"")
        tracemalloc.start()
        o._upload_media_to_cdn(creds, big, "owner", 2, 50 << 20)
        peak_old = tracemalloc.get_traced_memory()[1]
        tracemalloc.stop()
    check(peak_new < (4 << 20) and (peak_old is None or peak_old > (40 << 20)),
          f"24 MB video upload: Python heap peak {peak_old >> 20 if peak_old else '?'} MB (v2) -> "
          f"{peak_new / (1 << 20):.1f} MB (streamed md5 + openssl file-to-file + 64 KB upload blocks)")

    srv, px = TLSServer(), Proxy()
    use_proxy(px.url)
    b3 = load(NEW, "cdn_e2e")
    os.environ["WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL"] = "1"
    b3.PROXY_POOL.init()
    b3.bot_post = lambda c, ep, body, timeout: {"upload_full_url": srv.base + "/upload?q=1"}
    doc = d / "doc.pdf"
    doc.write_bytes(os.urandom(3 << 20))
    res = b3._upload_media_to_cdn(creds, doc, "owner", 3, 25 << 20)
    enc = subprocess.run(["openssl", "enc", "-aes-128-ecb", "-K", res["aeskey_hex"], "-in", str(doc)],
                         capture_output=True).stdout
    path_, n, md5_ = srv.requests[-1]
    check(n == res["file_size_ciphertext"] == len(enc) and md5_ == hashlib.md5(enc).hexdigest(),
          "streamed upload end to end (proxy + TLS): server received exactly the AES-128-ECB ciphertext")


# ---------------------------------------------------------------- 4 ingest
def g_ingest():
    b = load(NEW, "ing")
    creds = {"ilink_user_id": "owner"}

    def msg(mid, items):
        return {"message_id": mid, "from_user_id": "owner", "message_type": 1,
                "context_token": "CTX-9", "item_list": items}
    b.ingest_message(creds, msg("v1", [{"type": 3, "voice_item": {"text": "帮我查明天北京天气", "playtime": 2100}}]))
    b.ingest_message(creds, msg("v2", [{"type": 3, "voice_item": {"playtime": 900}}]))
    b.ingest_message(creds, msg("q1", [{"type": 1, "text_item": {"text": "这个改一下"},
                                        "ref_msg": {"title": "周报", "message_item": {"type": 1, "text_item": {"text": "本周完成A和B"}}}}]))
    b.ingest_message(creds, {"from_user_id": "owner", "message_type": 1,
                             "item_list": [{"type": 1, "msg_id": "777", "text_item": {"text": "hi"}}]})
    v1, v2, q1 = b.load_task("v1"), b.load_task("v2"), b.load_task("q1")
    view = b._executor_view(q1)
    detail = ""
    if HAVE_OLD:
        o = load(OLD, "ing_old")
        o.ingest_message(creds, msg("v1", [{"type": 3, "voice_item": {"text": "帮我查明天北京天气"}}]))
        o.ingest_message(creds, {"from_user_id": "owner", "message_type": 1,
                                 "item_list": [{"type": 1, "msg_id": "777", "text_item": {"text": "hi"}}]})
        detail = (f"v2: voice -> kind={o.load_task('v1')['kind']} (canned reply), "
                  f"id-less message stored={o.load_task('777') is not None}")
    check(v1["kind"] == "text" and v1["status"] == "pending_execution" and v1["text"].startswith("[语音转文字]")
          and v2["kind"] == "unsupported" and view.get("quote") == "周报 | 本周完成A和B"
          and b.load_task("777") is not None and b.latest_context_token("owner")[0] == "CTX-9",
          "ingest: voice transcript -> text task, quote kept for the executor, item msg_id fallback, context token kept",
          detail)


# ---------------------------------------------------------------- 5 typing
def g_typing():
    b = load(NEW, "typing")
    calls = []

    def fake(creds, ep, body, timeout):
        calls.append((ep.rsplit("/", 1)[1], body.get("status")))
        return {"ret": 0, "typing_ticket": "TK"} if ep.endswith("getconfig") else {"ret": 0}
    b.bot_post = fake
    b.TYPING_KEEPALIVE_SEC = 0.3
    tm, creds = b.TypingManager(), {"bot_token": "t"}
    mk_task(b, 1, "pending_execution")
    tm.tick(creds, b.scan_task_index())
    tm.tick(creds, b.scan_task_index())             # within keepalive: nothing
    time.sleep(0.35)
    tm.tick(creds, b.scan_task_index())             # keepalive
    t = b.load_task("1")
    t["status"] = "waiting_send"
    b.save_task(t)
    tm.tick(creds, b.scan_task_index())             # done: cancel
    seq = calls[:]
    b2 = load(NEW, "typing2")
    calls2 = []

    def down(creds, ep, body, timeout):
        calls2.append(ep)
        raise b2.ApiError("connect", "refused")
    b2.bot_post = down
    tm2 = b2.TypingManager()
    mk_task(b2, 1, "executing")
    tm2.tick(creds, b2.scan_task_index())
    tm2.tick(creds, b2.scan_task_index())           # getconfig in backoff: no retry storm
    check(seq == [("getconfig", None), ("sendtyping", 1), ("sendtyping", 1), ("sendtyping", 2)] and len(calls2) == 1,
          "typing: ticket fetched once, status 1 + 5 s keepalive while executing, status 2 when done; "
          "getconfig failures back off", f"calls={seq}")


# ---------------------------------------------------------------- 6 handoff
def g_handoff():
    b = load(NEW, "handoff")
    set_creds(b)
    me = os.getpid()
    b.update_heartbeat(serve_pid=me, handoff_pid=me)
    b.serve_alive = lambda: me
    who = []

    def fake(creds, to, text, ctx, client_id=None):
        who.append(threading.current_thread().name)
        return {"ret": 0, "_client_id": client_id, "message_id": "m"}
    b.send_text = fake
    prev = signal.signal(signal.SIGUSR1, lambda s, f: b._send_wake.set())
    th = threading.Thread(target=b.sender_loop, name="serve-sender", daemon=True)
    th.start()
    time.sleep(1.3)
    mk_task(b, 70, "waiting_send", result_text="reply")
    t0 = time.perf_counter()
    out = b.deliver("70")
    dt = time.perf_counter() - t0
    b._stop.set()
    b._send_wake.set()
    th.join(3)
    b._stop.clear()
    b.update_heartbeat(handoff_pid=12345)          # an older serve: never signal it
    mk_task(b, 71, "waiting_send", result_text="reply2")
    out2 = b.deliver("71")
    signal.signal(signal.SIGUSR1, prev)
    check(out.get("ok") and out.get("via") == "serve" and who == ["serve-sender", "MainThread"]
          and out2.get("ok") and dt < 0.5,
          f"reply handoff: serve's sender sent it {dt * 1000:.0f} ms after SIGUSR1 (warm connection); "
          "unadvertised serve -> direct send")


# ---------------------------------------------------------------- 7 re-login
def g_relogin():
    def prep(m):
        m.atomic_write_json(m.CREDS_FILE, {"bot_token": "A", "ilink_user_id": "owner"})
        m.atomic_write_json(m.CURSOR_FILE, {"get_updates_buf": "C-A"})
        m.atomic_write_json(m.SESSION_FILE, {"paused_until_ms": now_ms() + 3_600_000, "stale_count": 2})
        m.atomic_write_json(m.STATE_DIR / "auth_alert.json", {"at": 1})
    b = load(NEW, "relogin")
    prep(b)
    b.remember_context_token("owner", "CTX-A")
    b._save_credentials({"bot_token": "B", "ilink_bot_id": "bot", "ilink_user_id": "owner"})
    reset = (b.read_json(b.CURSOR_FILE)["get_updates_buf"] == "" and not b.SESSION_FILE.exists()
             and not (b.STATE_DIR / "auth_alert.json").exists() and b.latest_context_token("owner") == (None, None))
    detail = ""
    if HAVE_OLD:
        o = load(OLD, "relogin_old")
        prep(o)
        o._save_credentials({"bot_token": "B", "ilink_bot_id": "bot", "ilink_user_id": "owner"})
        left = ((o.read_json(o.SESSION_FILE) or {}).get("paused_until_ms", 0) - now_ms()) / 60000
        detail = f"v2 after a successful re-scan: still paused for {left:.0f} more minutes"
    b2 = load(NEW, "relogin2")
    prep(b2)
    b2.SESSION_FILE.unlink()
    seen = []

    def gu(creds, cursor, timeout_ms):
        seen.append((creds["bot_token"], cursor))
        if len(seen) == 1:
            b2._save_credentials({"bot_token": "B", "ilink_bot_id": "bot", "ilink_user_id": "owner"})
            return {"ret": 0, "msgs": [], "get_updates_buf": "C-A2"}   # late reply of the old session
        b2._stop.set()
        return {"ret": 0, "msgs": [], "get_updates_buf": "C-B1"}
    b2.get_updates, b2.notify = gu, (lambda c, start: True)
    b2.receiver_loop(b2.load_creds())
    check(reset and seen == [("A", "C-A"), ("B", "")],
          "re-login: cursor/-14 pause/alert/context tokens reset; running serve switches token without restart",
          detail)


if __name__ == "__main__":
    for g in (g_keepalive, g_stale, g_connect_phase, g_send, g_cdn, g_ingest, g_typing, g_handoff, g_relogin):
        try:
            g()
        except Exception as e:  # noqa: BLE001
            import traceback
            traceback.print_exc()
            check(False, f"{g.__name__} crashed: {type(e).__name__}: {e}")
    print("\nALL V3 CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED: {fails}")
    sys.exit(1 if fails else 0)
