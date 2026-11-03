#!/usr/bin/env python3
"""Offline test for the proxy failover in bridge.py (no real network).

Usage:  python3 test_proxy_failover.py [path/to/bridge.py]
Uses two throw-away local TCP servers as "proxies": one that drops every
connection, one that answers like a forward proxy.
"""
import importlib.util
import os
import re
import shutil
import socket
import sys
import tempfile
import threading
import time
import urllib.error
from pathlib import Path

SRC = Path(sys.argv[1] if len(sys.argv) > 1 else "bridge.py").resolve()
TARGET = "http://wechat.invalid"


def load(tmp: Path):
    shutil.copy(SRC, tmp / "bridge.py")
    spec = importlib.util.spec_from_file_location("bridge_p", tmp / "bridge.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for d in (mod.STATE_DIR, mod.INBOX_DIR, mod.LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)
    mod.logs = []
    mod.log = lambda m: mod.logs.append(m)
    mod.FIXED_BASE_URL = TARGET
    mod._stop.clear()
    for k in ("https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY"):
        os.environ.pop(k, None)
    os.environ["no_proxy"] = os.environ["NO_PROXY"] = ""
    return mod


def start_proxy(handler):
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)

    def loop():
        while True:
            try:
                c, _ = srv.accept()
            except OSError:
                return
            threading.Thread(target=handler, args=(c,), daemon=True).start()
    threading.Thread(target=loop, daemon=True).start()
    return srv, f"http://127.0.0.1:{srv.getsockname()[1]}"


def dead(c):
    c.close()


def good(c):
    try:
        c.settimeout(5)
        data = b""
        while b"\r\n\r\n" not in data:
            data += c.recv(4096)
        head, _, rest = data.partition(b"\r\n\r\n")
        m = re.search(rb"content-length: *(\d+)", head, re.I)
        while m and len(rest) < int(m.group(1)):
            rest += c.recv(4096)
        body = b'{"ret":0}'
        out = b"" if head.startswith(b"HEAD") else body
        c.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                  b"Content-Length: " + str(len(body)).encode() +
                  b"\r\nConnection: close\r\n\r\n" + out)
    finally:
        c.close()


def post(mod):
    return mod.http_post(TARGET, "x", {}, {}, 3)


def fails(mod, n):
    for _ in range(n):
        try:
            post(mod)
        except mod.ApiError:
            pass


def s_e2e(tmp):
    mod = load(tmp)
    d_srv, d_url = start_proxy(dead)
    g_srv, g_url = start_proxy(good)
    os.environ["https_proxy"] = d_url
    (mod.STATE_DIR / "proxy.env").write_text(f"https_proxy={g_url}\n")
    mod.PROXY_POOL.init()
    mod.PROXY_POOL.start_rotator()
    fails(mod, mod.PROXY_ROTATE_AFTER)
    t0 = time.time()
    while mod.PROXY_POOL.active != g_url and time.time() - t0 < 6:
        time.sleep(0.05)
    switched = mod.PROXY_POOL.active == g_url
    ok_after = switched and post(mod) == {"ret": 0}
    leaked = [l for l in mod.logs if "127.0.0.1" in l or "://" in l]
    hist = mod.read_json(mod.PROXY_POOL_FILE, {})["urls"]
    mode = oct(os.stat(mod.PROXY_POOL_FILE).st_mode & 0o777)
    mod._stop.set()
    d_srv.close(); g_srv.close()
    good_ = switched and ok_after and not leaked and hist[0] == g_url and mode == "0o600"
    return good_, f"switched={switched}, post_ok_after={ok_after}, url_in_logs={leaked}, " \
                  f"history_head_is_new={hist[0] == g_url}, file_mode={mode}"


def s_no_switch_when_all_dead(tmp):
    mod = load(tmp)
    a, a_url = start_proxy(dead)
    b, b_url = start_proxy(dead)
    os.environ["https_proxy"] = a_url
    (mod.STATE_DIR / "proxy-candidates.txt").write_text(f"# note\n{b_url}\n")
    mod.PROXY_POOL.init()
    fails(mod, mod.PROXY_ROTATE_AFTER)
    mod.PROXY_POOL._rotate_once()
    stayed = mod.PROXY_POOL.active == a_url and mod.PROXY_POOL.rotations == 0
    cooldown = mod.PROXY_POOL._next_rotation_at > time.time()
    a.close(); b.close()
    return stayed and cooldown, f"stayed={stayed}, cooldown_set={cooldown}, last_log={mod.logs[-1]!r}"


def s_accounting(tmp):
    mod = load(tmp)
    os.environ["https_proxy"] = "http://127.0.0.1:9"
    mod.PROXY_POOL.init()
    pool = mod.PROXY_POOL

    class Op:
        def __init__(self, exc): self.exc = exc
        def open(self, req, timeout): raise self.exc
    req = mod.urllib.request.Request(TARGET)

    def run(exc):
        pool._opener = Op(exc)
        try:
            pool.open(req, 1)
        except Exception:
            pass
        return pool._streak
    pool._streak = 0
    run(mod.urllib.error.URLError(TimeoutError("timed out")))      # connect timeout -> failure
    run(mod.http.client.RemoteDisconnected("x"))                   # mid-request drop -> failure
    two = pool._streak
    run(TimeoutError("read timed out"))                            # long-poll read timeout -> neutral
    neutral = pool._streak
    run(mod.urllib.error.HTTPError(TARGET, 404, "nf", {}, None))   # server answered -> reset
    return (two, neutral, pool._streak) == (2, 2, 0), f"streaks: after 2 failures={two}, after read-timeout={neutral}, after HTTP 404={pool._streak}"


def s_disabled_and_scrub(tmp):
    mod = load(tmp)
    os.environ["https_proxy"] = "socks5://127.0.0.1:1080"
    mod.PROXY_POOL.init()
    off = not mod.PROXY_POOL.enabled
    mod.PROXY_POOL.enabled = False
    # NOTE: since the v5.0.3 urllib redirect guard, the disabled-pool path
    # no longer calls urllib.request.urlopen directly; it builds an opener
    # with _SafeRedirectHandler and calls opener.open. Intercept the opener
    # path (urlopen patching would silently never fire).
    called = {}
    real_build = mod.urllib.request.build_opener

    class FakeOpener:
        def open(self, req, timeout=None):
            called["y"] = 1
            raise OSError()

    mod.urllib.request.build_opener = lambda *a, **k: FakeOpener()
    try:
        mod.PROXY_POOL.open(mod.urllib.request.Request(TARGET), 1)
    except OSError:
        pass
    finally:
        mod.urllib.request.build_opener = real_build
    scrubbed = mod._scrub("Tunnel failed for https://user:pw@10.0.0.1:8080/x end")
    return off and called and "pw" not in scrubbed and "<url>" in scrubbed, \
        f"socks_disables_pool={off}, passthrough_used={bool(called)}, scrub={scrubbed!r}"


def s_no_alternatives_logged_once(tmp):
    mod = load(tmp)
    os.environ["https_proxy"] = "http://127.0.0.1:9"
    mod.PROXY_POOL.init()
    mod.PROXY_POOL._rotate_once()
    mod.PROXY_POOL._rotate_once()
    n = sum("no alternative" in l for l in mod.logs)
    return n == 1, f"log lines={n}"


bad = 0
for name, fn in [("1 dead proxy -> fail over to a reachable one (end-to-end)", s_e2e),
                 ("2 no switch when no alternative is reachable", s_no_switch_when_all_dead),
                 ("3 failure accounting (connect/drop vs read-timeout vs HTTP)", s_accounting),
                 ("4 socks env disables pool; URLs scrubbed from log text", s_disabled_and_scrub),
                 ("5 'no alternative' warned only once", s_no_alternatives_logged_once)]:
    with tempfile.TemporaryDirectory() as d:
        try:
            ok, detail = fn(Path(d))
        except Exception as e:  # noqa: BLE001
            ok, detail = False, f"{type(e).__name__}: {e}"
    bad += not ok
    print(f"{'PASS' if ok else 'FAIL'}  {name}\n      {detail}")
sys.exit(1 if bad else 0)
