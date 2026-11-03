#!/usr/bin/env python3
"""End-to-end smoke test of the real `bridge.py serve` process against a
local fake iLink (TLS, HTTP/1.1 keep-alive) behind a local CONNECT proxy:
inbound message -> task -> typing -> claim/complete as separate CLI
processes -> reply handed to serve (SIGUSR1) -> sent over a reused
connection -> typing cancelled -> send-direct with the remembered context
token -> status -> graceful SIGTERM. Run: python3 tests/test_serve_e2e.py"""
import http.server, json, os, shutil, signal, socket, socketserver, ssl
import subprocess, sys, tempfile, threading, time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
fails = []


def check(c, m, d=""):
    print(("PASS  " if c else "FAIL  ") + m + (f"\n      {d}" if d else ""))
    if not c:
        fails.append(m)


TMP = Path(tempfile.mkdtemp(prefix="e2e-"))
subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(TMP / "k.pem"),
                "-out", str(TMP / "c.pem"), "-days", "1", "-subj", "/CN=localhost",
                "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"], check=True, capture_output=True)
SCTX = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
SCTX.load_cert_chain(TMP / "c.pem", TMP / "k.pem")


class ILink(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        ep = self.path.split("?")[0].rsplit("/", 1)[1]
        S = self.server
        S.calls.append((ep, body))
        if ep == "getupdates":
            if not S.delivered:
                S.delivered = True
                out = {"ret": 0, "get_updates_buf": "B1", "longpolling_timeout_ms": 3000,
                       "msgs": [{"message_id": 9001, "from_user_id": "owner", "to_user_id": "bot",
                                 "message_type": 1, "context_token": "CTX-E2E",
                                 "item_list": [{"type": 1, "text_item": {"text": "你好"}}]}]}
            else:
                time.sleep(2.0)                                  # long-poll hold
                out = {"ret": 0, "msgs": [], "get_updates_buf": "B1", "longpolling_timeout_ms": 3000}
        elif ep == "getconfig":
            out = {"ret": 0, "typing_ticket": "TK"}
        elif ep == "sendmessage":
            S.sent.append(body["msg"])
            out = {"ret": 0, "message_id": f"srv-{len(S.sent)}"}
        else:
            out = {"ret": 0}
        data = json.dumps(out).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), ILink)
        self.socket = SCTX.wrap_socket(self.socket, server_side=True)
        self.calls, self.sent, self.delivered = [], [], False
        self.base = f"https://localhost:{self.server_address[1]}"
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def handle_error(self, *a):
        pass


class Proxy:
    def __init__(self, delay=0.03):
        self.delay, self.tunnels = delay, 0
        self.s = socket.socket()
        self.s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.s.bind(("127.0.0.1", 0))
        self.s.listen(64)
        self.url = f"http://127.0.0.1:{self.s.getsockname()[1]}"
        threading.Thread(target=self._loop, daemon=True).start()

    def _loop(self):
        while True:
            c, _ = self.s.accept()
            threading.Thread(target=self._handle, args=(c,), daemon=True).start()

    def _handle(self, c):
        data = b""
        while b"\r\n\r\n" not in data:
            d = c.recv(4096)
            if not d:
                return c.close()
            data += d
        host, port = data.split(b"\r\n")[0].split()[1].decode().rsplit(":", 1)
        time.sleep(self.delay)
        u = socket.create_connection((host, int(port)))
        self.tunnels += 1
        c.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")

        def fwd(a, b):
            try:
                while True:
                    d = a.recv(65536)
                    if not d:
                        break
                    b.sendall(d)
            except OSError:
                pass
            for s_ in (a, b):
                try:
                    s_.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
        threading.Thread(target=fwd, args=(u, c), daemon=True).start()
        fwd(c, u)


srv, px = Server(), Proxy()
proj = TMP / "wechat-bridge"
(proj / "ops").mkdir(parents=True)
(proj / "state").mkdir()
shutil.copy(HERE / "bridge.py", proj / "bridge.py")
shutil.copy(HERE / "ops" / "send-direct.py", proj / "ops" / "send-direct.py")
(proj / "state" / "credentials.json").write_text(json.dumps(
    {"bot_token": "tok", "ilink_bot_id": "bot@im.bot", "ilink_user_id": "owner", "baseurl": srv.base}))
env = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
env.update(https_proxy=px.url, HTTPS_PROXY=px.url, no_proxy="", NO_PROXY="", SSL_CERT_FILE=str(TMP / "c.pem"))


def run(*args, script="bridge.py"):
    r = subprocess.run([sys.executable, script, *args], cwd=proj, env=env, capture_output=True, text=True, timeout=120)
    return json.loads(r.stdout or "{}")


def wait_for(pred, secs):
    t0 = time.time()
    while time.time() - t0 < secs:
        if pred():
            return True
        time.sleep(0.05)
    return False


serve = subprocess.Popen([sys.executable, "bridge.py", "serve"], cwd=proj, env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
try:
    task_file = proj / "inbox" / "9001.json"
    t0 = time.time()
    ok = wait_for(task_file.exists, 15)
    check(ok and json.loads(task_file.read_text())["status"] == "pending_execution",
          f"inbound message stored as a pending task {time.time() - t0:.1f}s after serve start")
    typing_on = lambda: any(ep == "sendtyping" and b.get("status") == 1 for ep, b in srv.calls)
    ok = wait_for(typing_on, 5)
    gc = [b for ep, b in srv.calls if ep == "getconfig"]
    check(ok and gc and gc[0].get("context_token") == "CTX-E2E",
          "typing indicator sent while the task waits (ticket from getconfig, with the context token)")
    time.sleep(5.6)                                       # one keepalive period
    claimed = run("claim", "--limit", "5")["tasks"]
    t0 = time.time()
    out = run("complete", "--task-id", "9001", "--result", "收到：你好")
    dt = time.time() - t0
    s = out.get("send") or {}
    check(claimed and claimed[0]["message_id"] == "9001" and s.get("ok") and s.get("via") == "serve"
          and len(srv.sent) == 1 and srv.sent[0]["item_list"][0]["text_item"]["text"] == "收到：你好"
          and srv.sent[0].get("context_token") == "CTX-E2E",
          f"claim/complete (CLI): reply handed to serve and sent exactly once in {dt * 1000:.0f} ms incl. CLI start")
    ok = wait_for(lambda: any(ep == "sendtyping" and b.get("status") == 2 for ep, b in srv.calls), 4)
    keepalives = sum(1 for ep, b in srv.calls if ep == "sendtyping" and b.get("status") == 1)
    check(ok and keepalives >= 2, f"typing: {keepalives} keepalives while working, cancelled after the reply")
    o = run("--message", "测试：主动提醒", script="ops/send-direct.py")
    check(o.get("ok") and o.get("via") == "serve" and srv.sent[-1].get("context_token") == "CTX-E2E"
          and o.get("context_stale") is False,
          "send-direct: proactive message carries the remembered context_token, sent by serve", f"{o}")
    n_calls = len(srv.calls)
    check(px.tunnels <= 4 and n_calls >= 10,
          f"{n_calls} iLink calls over {px.tunnels} proxy tunnels (v2 opened one tunnel per call)")
    st = run("status")
finally:
    serve.send_signal(signal.SIGTERM)
    try:
        rc = serve.wait(6)
    except subprocess.TimeoutExpired:
        rc = None
        serve.kill()
hb = json.loads((proj / "state" / "heartbeat.json").read_text())
check(st.get("serve_pid") and st.get("proxy_id") and st.get("send_chunk_limit") == 4000 and rc == 0
      and hb.get("handoff_pid") is None,
      "status shows proxy/transport fields; SIGTERM stops serve cleanly and withdraws the handoff",
      f"rc={rc} proxy_id={st.get('proxy_id')}")
log_lines = (proj / "logs" / "bridge.log").read_text().splitlines()
bogus = [ln for ln in log_lines if "unexpected error" in ln or "error (" in ln]
check(not bogus and any("serve stopped" in ln for ln in log_lines),
      "serve log clean: no poll errors and no bogus 'unexpected error' on SIGTERM", f"{bogus[:2]}")
if fails:
    print("---- serve log tail ----")
    print("\n".join((proj / "logs" / "bridge.log").read_text().splitlines()[-25:]))
print("\nALL E2E CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED")
sys.exit(1 if fails else 0)
