#!/usr/bin/env python3
"""Redirect-guard tests (v5.0.4 review follow-up): the pool redirect rules
and the urllib-fallback _SafeRedirectHandler must agree. Before this test
existed, no test touched _SafeRedirectHandler at all: test_proxy_failover
only proved the fallback path calls build_opener (via a fake opener), so
deleting the handler would not have turned anything red. These cases use
two local servers (A redirects, B records what it received) plus a TLS
server for the downgrade case:

- cross-origin + allowlisted target: Authorization/Cookie stripped
- cross-origin + non-allowlisted target: refused as MediaError("bad_url")
  in BOTH modes, so callers never retry it as a transient network error
- same-origin: credentials preserved
- https->http downgrade: refused as URLError in BOTH modes

Run: python3 tests/test_redirect_guard.py
"""
import importlib.util
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent.parent
fails = []
skipped = []


def check(cond, name, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name
          + (f" ({detail})" if detail and not cond else ""))
    if not cond:
        fails.append(name)


def load(src: Path, name: str):
    d = Path(tempfile.mkdtemp(prefix="wb-redir-"))
    shutil.copy(src, d / "bridge.py")
    spec = importlib.util.spec_from_file_location(name, d / "bridge.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for dd in (mod.STATE_DIR, mod.INBOX_DIR, mod.LOG_DIR):
        dd.mkdir(parents=True, exist_ok=True)
    mod.logs = []
    mod.log = lambda m: mod.logs.append(m)
    mod._stop.clear()
    for k in ("https_proxy", "HTTPS_PROXY", "http_proxy", "HTTP_PROXY"):
        os.environ.pop(k, None)
    os.environ["no_proxy"] = os.environ["NO_PROXY"] = ""
    os.environ["WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL"] = "1"
    return mod


def make_server(handler_cls):
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler_cls)
    srv.daemon_threads = True
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv, srv.server_address[1]


seen = {}
seen_lock = threading.Lock()


def record(name, handler):
    with seen_lock:
        seen[name] = {k: v for k, v in handler.headers.items()}


class EchoHandler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_GET(self):
        record("echo", self)
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        # Only records: a replayed POST reaching B would land here.
        record("echo-post", self)
        body = b"ok"
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def redirector(port_b):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _redir(self, loc):
            self.send_response(302)
            self.send_header("Location", loc)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self):
            if self.path == "/to-b":
                self._redir(f"http://127.0.0.1:{port_b}/echo")
            elif self.path == "/redir-post":
                self._redir(f"http://127.0.0.1:{port_b}/echo")
            elif self.path == "/to-same":
                self._redir("/echo")
            elif self.path == "/to-deny":
                self._redir("https://evil.invalid/x")
            elif self.path == "/echo":
                record("same", self)
                body = b"ok"
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            if n:
                self.rfile.read(n)
            if self.path == "/redir-post":
                self._redir(f"http://127.0.0.1:{port_b}/echo")
            else:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()

    return H


def main():
    mod = load(HERE / "bridge.py", "redir_guard")
    srv_b, port_b = make_server(EchoHandler)
    srv_a, port_a = make_server(redirector(port_b))
    pool = mod.PROXY_POOL

    def creds_req(url):
        return mod.urllib.request.Request(
            url, headers={"Authorization": "Bearer secret-token",
                          "Cookie": "session=abc123"})

    def fallback(url):
        pool.enabled = False
        pool.mode = "urllib"
        return pool.open(creds_req(url), timeout=10)

    def pooled(url):
        pool._opener = mod.KeepAliveOpener(None)
        pool.enabled = True
        pool.mode = "direct"
        return pool.open(creds_req(url), timeout=10)

    # 1. Fallback: cross-origin allowlisted target strips credentials.
    with seen_lock:
        seen.clear()
    with fallback(f"http://127.0.0.1:{port_a}/to-b") as resp:
        body = resp.read()
    got = seen.get("echo", {})
    check(body == b"ok" and "Authorization" not in got
          and "Cookie" not in got,
          "fallback cross-origin strips credentials", f"seen={got}")

    # 2. Fallback: cross-origin non-allowlisted target refused as MediaError.
    try:
        with fallback(f"http://127.0.0.1:{port_a}/to-deny") as resp:
            resp.read()
        check(False, "fallback non-allowlisted refused as MediaError")
    except Exception as e:
        check(isinstance(e, mod.MediaError)
              and getattr(e, "reason", None) == "bad_url",
              "fallback non-allowlisted refused as MediaError",
              f"{type(e).__name__}: {e}")

    # 3. Fallback: same-origin keeps credentials.
    with seen_lock:
        seen.clear()
    with fallback(f"http://127.0.0.1:{port_a}/to-same") as resp:
        body = resp.read()
    got = seen.get("same", {})
    check(body == b"ok" and got.get("Authorization") == "Bearer secret-token"
          and got.get("Cookie") == "session=abc123",
          "fallback same-origin keeps credentials", f"seen={got}")

    # 4. Pool: cross-origin allowlisted target strips credentials.
    with seen_lock:
        seen.clear()
    with pooled(f"http://127.0.0.1:{port_a}/to-b") as resp:
        body = resp.read()
    got = seen.get("echo", {})
    check(body == b"ok" and "Authorization" not in got
          and "Cookie" not in got,
          "pool cross-origin strips credentials", f"seen={got}")

    # 5. Pool: cross-origin non-allowlisted target refused as MediaError.
    try:
        with pooled(f"http://127.0.0.1:{port_a}/to-deny") as resp:
            resp.read()
        check(False, "pool non-allowlisted refused as MediaError")
    except Exception as e:
        check(isinstance(e, mod.MediaError)
              and getattr(e, "reason", None) == "bad_url",
              "pool non-allowlisted refused as MediaError",
              f"{type(e).__name__}: {e}")

    # 6. Pool: same-origin keeps credentials.
    with seen_lock:
        seen.clear()
    with pooled(f"http://127.0.0.1:{port_a}/to-same") as resp:
        body = resp.read()
    got = seen.get("same", {})
    check(body == b"ok" and got.get("Authorization") == "Bearer secret-token"
          and got.get("Cookie") == "session=abc123",
          "pool same-origin keeps credentials", f"seen={got}")

    # 7. POST redirects are NOT replayed: both modes raise HTTPError 302
    # and the target sees nothing. The stdlib handler would otherwise
    # silently replay a POST as a GET, so a redirected sendmessage would
    # look successful while its body never arrived.
    def post_via(url, mode):
        if mode == "fallback":
            pool.enabled = False
            pool.mode = "urllib"
        else:
            pool._opener = mod.KeepAliveOpener(None)
            pool.enabled = True
            pool.mode = "direct"
        req = mod.urllib.request.Request(
            url, data=b"sendmessage-body", method="POST")
        return pool.open(req, timeout=10)

    for name in ("fallback", "pool"):
        with seen_lock:
            seen.clear()
        try:
            with post_via(f"http://127.0.0.1:{port_a}/redir-post",
                          name) as resp:
                resp.read()
            check(False, f"{name} POST redirect raises HTTPError 302")
        except Exception as e:
            check(isinstance(e, mod.urllib.error.HTTPError)
                  and getattr(e, "code", None) == 302,
                  f"{name} POST redirect raises HTTPError 302",
                  f"{type(e).__name__}: {e}")
        with seen_lock:
            hit = "echo" in seen or "echo-post" in seen
        check(not hit, f"{name} POST redirect never reaches target",
              f"seen={dict(seen)}")

    # 8. Downgrade: refused as URLError in both modes. Needs a TLS server;
    # the bridge verifies certs, so the test cert is trusted via SSL_CERT_FILE.
    if not shutil.which("openssl"):
        skipped.append("downgrade (no openssl CLI)")
        print("SKIP  downgrade cases (no openssl CLI)")
    else:
        tmp = Path(tempfile.mkdtemp(prefix="wb-redir-tls-"))
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048",
                        "-keyout", str(tmp / "key.pem"),
                        "-out", str(tmp / "cert.pem"), "-days", "1",
                        "-nodes", "-subj", "/CN=127.0.0.1",
                        "-addext", "subjectAltName=IP:127.0.0.1"],
                       capture_output=True, check=True)

        class TLSRedirector(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                if self.path == "/to-downgrade":
                    self.send_response(302)
                    self.send_header(
                        "Location", f"http://127.0.0.1:{port_a}/echo")
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                else:
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()

        import ssl
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(str(tmp / "cert.pem"), str(tmp / "key.pem"))
        srv_t = ThreadingHTTPServer(("127.0.0.1", 0), TLSRedirector)
        srv_t.socket = ctx.wrap_socket(srv_t.socket, server_side=True)
        srv_t.daemon_threads = True
        threading.Thread(target=srv_t.serve_forever, daemon=True).start()
        port_t = srv_t.server_address[1]
        old_cert = os.environ.get("SSL_CERT_FILE")
        os.environ["SSL_CERT_FILE"] = str(tmp / "cert.pem")
        try:
            for name, opener in (("fallback", fallback), ("pool", pooled)):
                try:
                    with opener(f"https://127.0.0.1:{port_t}/to-downgrade") \
                            as resp:
                        resp.read()
                    check(False, f"{name} downgrade refused as URLError")
                except Exception as e:
                    check(isinstance(e, mod.urllib.error.URLError),
                          f"{name} downgrade refused as URLError",
                          f"{type(e).__name__}: {e}")
        finally:
            if old_cert is None:
                os.environ.pop("SSL_CERT_FILE", None)
            else:
                os.environ["SSL_CERT_FILE"] = old_cert
        srv_t.shutdown()

    srv_a.shutdown()
    srv_b.shutdown()
    print()
    if skipped:
        print(f"SKIPPED: {skipped}")
    if fails:
        print(f"{len(fails)} FAILED: {fails}")
        sys.exit(1)
    print("ALL REDIRECT-GUARD CHECKS PASSED")


if __name__ == "__main__":
    main()
