#!/usr/bin/env python3
"""v4 end to end: a real `bridge.py serve` against a local fake iLink + CDN
(TLS, keep-alive) behind a local CONNECT proxy, with CLI processes as the
executor. Replays the 2026-11-01 chat: a generated audio file whose CDN
upload is dropped repeatedly (RemoteDisconnected), a voice note, then the
chat commands 状态 / 帮助 and a follow-up that needs the earlier turns.
The SILK codec is a stand-in module in vendor/py (exercises the lookup path;
it does not produce real SILK). Run: python3 tests/test_v4_e2e.py"""
import http.server, json, os, shutil, signal, socket, socketserver, ssl
import subprocess, sys, tempfile, threading, time, wave
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
fails = []


def check(c, m, d=""):
    print(("PASS  " if c else "FAIL  ") + m + (f"\n      {d}" if d else ""))
    if not c:
        fails.append(m)


TMP = Path(tempfile.mkdtemp(prefix="e2e4-"))
subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", str(TMP / "k.pem"),
                "-out", str(TMP / "c.pem"), "-days", "1", "-subj", "/CN=localhost",
                "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"], check=True, capture_output=True)
SCTX = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
SCTX.load_cert_chain(TMP / "c.pem", TMP / "k.pem")


class ILink(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def reply(self, obj, headers=()):
        data = json.dumps(obj).encode() if obj is not None else b""
        self.send_response(200)
        for k, v in headers:
            self.send_header(k, v)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        S = self.server
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0))
        path = self.path.split("?")[0]
        if path.endswith("/upload"):                       # the CDN
            S.uploads += 1
            if S.drop_uploads > 0:
                S.drop_uploads -= 1
                self.close_connection = True               # no response at all:
                return                                     # client sees RemoteDisconnected
            return self.reply(None, (("x-encrypted-param", f"DL{S.uploads}"),))
        body = json.loads(raw or b"{}")
        ep = path.rsplit("/", 1)[1]
        S.calls.append((ep, body))
        if ep == "getupdates":
            if S.inbox:
                msgs, S.inbox = S.inbox, []
                return self.reply({"ret": 0, "msgs": msgs, "get_updates_buf": f"B{len(S.calls)}",
                                   "longpolling_timeout_ms": 3000})
            time.sleep(0.8)
            return self.reply({"ret": 0, "msgs": [], "get_updates_buf": "B", "longpolling_timeout_ms": 3000})
        if ep == "getuploadurl":
            S.upload_urls += 1
            return self.reply({"upload_full_url": f"{S.base}/upload?r={S.upload_urls}"})
        if ep == "getconfig":
            return self.reply({"ret": 0, "typing_ticket": "TK"})
        if ep == "sendmessage":
            S.sent.append(body["msg"])
            return self.reply({})
        return self.reply({"ret": 0})


class Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), ILink)
        self.socket = SCTX.wrap_socket(self.socket, server_side=True)
        self.calls, self.sent, self.inbox = [], [], []
        self.uploads = self.upload_urls = self.drop_uploads = 0
        self.base = f"https://localhost:{self.server_address[1]}"
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def handle_error(self, *a):
        pass

    def push(self, mid, text):
        self.inbox.append({"message_id": mid, "from_user_id": "owner", "to_user_id": "bot",
                           "message_type": 1, "context_token": "CTX-E2E",
                           "item_list": [{"type": 1, "text_item": {"text": text}}]})


class Proxy:
    def __init__(self):
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
        u = socket.create_connection((host, int(port)))
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


FAKE_SILK = '''"""Stand-in SILK codec for tests only - NOT real SILK."""
def encode(fi, fo, rate, bitrate, *a, **k):
    pcm = fi.read()
    fo.write(b"\\x02#!SILK_V3" + pcm[:20000])
def decode(fi, fo, rate, *a, **k):
    fi.read()
    fo.write(b"\\x00\\x00" * rate)
'''

srv, px = Server(), Proxy()
proj = TMP / "wechat-bridge"
(proj / "ops").mkdir(parents=True)
(proj / "state").mkdir()
(proj / "vendor" / "py").mkdir(parents=True)
shutil.copy(HERE / "bridge.py", proj / "bridge.py")
shutil.copy(HERE / "ops" / "send-direct.py", proj / "ops" / "send-direct.py")
(proj / "vendor" / "py" / "pysilk.py").write_text(FAKE_SILK)
(proj / "state" / "credentials.json").write_text(json.dumps(
    {"bot_token": "tok", "ilink_bot_id": "bot@im.bot", "ilink_user_id": "owner", "baseurl": srv.base}))
env = {k: v for k, v in os.environ.items() if not k.lower().endswith("_proxy")}
env.update(https_proxy=px.url, HTTPS_PROXY=px.url, no_proxy="", NO_PROXY="",
           SSL_CERT_FILE=str(TMP / "c.pem"), WECHAT_BRIDGE_QUEUE_DELAYS_MS="300,500,800,1000,1000")
# Native-voice scenario opts in explicitly (voice-file default since v5);
# the fake CDN is loopback, so the media-URL allowlist needs the test flag.
env.update(WECHAT_BRIDGE_NATIVE_VOICE="1", WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL="1")
song = TMP / "song.mp3"
song.write_bytes(os.urandom(200_000))
voice = TMP / "voice.wav"
with wave.open(str(voice), "wb") as w:
    w.setnchannels(1)
    w.setsampwidth(2)
    w.setframerate(24000)
    w.writeframes(b"\x10\x00\xf0\xff" * 36000)              # 3 s


def run(*args, script="bridge.py"):
    r = subprocess.run([sys.executable, script, *args], cwd=proj, env=env, capture_output=True,
                       text=True, timeout=180)
    try:
        return json.loads(r.stdout or "{}")
    except json.JSONDecodeError:
        return {"_raw": r.stdout, "_err": r.stderr[-500:]}


def wait_for(pred, secs):
    t0 = time.time()
    while time.time() - t0 < secs:
        if pred():
            return True
        time.sleep(0.05)
    return False


def texts():
    return [m["item_list"][0]["text_item"]["text"] for m in srv.sent if m["item_list"][0]["type"] == 1]


serve = subprocess.Popen([sys.executable, "bridge.py", "serve"], cwd=proj, env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
st = {}
try:
    srv.push(7001, "做一段语音和一个音频文件发给我")
    ok = wait_for(lambda: (proj / "inbox" / "7001.json").exists(), 15)
    claimed = run("claim", "--limit", "5").get("tasks") or []
    srv.drop_uploads = 8
    t0 = time.time()
    out = run("complete", "--task-id", "7001", "--result", "好的，语音和文件随后发来。",
              "--attach", str(song), "--wait-attachments", "0")
    first = (out.get("attachments") or [{}])[0]
    got_file = wait_for(lambda: any(m["item_list"][0]["type"] == 4 for m in srv.sent), 60)
    order = [m["item_list"][0]["type"] for m in srv.sent]
    fname = next((m["item_list"][0]["file_item"]["file_name"] for m in srv.sent
                  if m["item_list"][0]["type"] == 4), None)
    check(ok and claimed and out.get("send", {}).get("ok") and first.get("status") in ("pending", "sending")
          and got_file and order.index(1) < order.index(4) and fname == "song.mp3"
          and srv.upload_urls >= 2 and srv.uploads >= 9,
          f"complete --attach: CDN dropped 8 uploads (RemoteDisconnected) -> serve's queue kept going "
          f"with fresh upload params and delivered the file {time.time() - t0:.1f}s later, after the text",
          f"getuploadurl calls={srv.upload_urls}, CDN POSTs={srv.uploads}, message types in order={order}")

    vo = run("send-media", "--task-id", "7001", "--file", str(voice), "--as", "voice", "--wait", "20")
    vi = next((m["item_list"][0]["voice_item"] for m in srv.sent if m["item_list"][0]["type"] == 3), {})
    check(vo.get("ok") and vi.get("encode_type") == 6 and 2900 <= vi.get("playtime", 0) <= 3100
          and vi.get("sample_rate") == 24000,
          "send-media --as voice: codec found in vendor/py, voice note sent by serve's queue worker",
          f"{ {k: v for k, v in vi.items() if k != 'media'} }")

    n_before = len(srv.sent)
    srv.push(7002, "状态")
    got = wait_for(lambda: any(t.startswith("微信桥接状态") for t in texts()), 15)
    status_text = next((t for t in texts() if t.startswith("微信桥接状态")), "")
    task = json.loads((proj / "inbox" / "7002.json").read_text()) if (proj / "inbox" / "7002.json").exists() else {}
    check(got and task.get("kind") == "command" and task.get("status") == "sent"
          and "已连续运行" in status_text and "最近一次成功在" in status_text and "tok" not in status_text,
          "「状态」 answered by serve itself, no executor involved",
          status_text.replace("\n", "\n      "))
    srv.push(7003, "帮助")
    check(wait_for(lambda: any(t.startswith("可用的快捷指令") for t in texts()), 15),
          "「帮助」 answered by serve itself")

    srv.push(7004, "刚才那段语音说的是什么")
    wait_for(lambda: (proj / "inbox" / "7004.json").exists(), 15)
    c2 = run("claim", "--limit", "5")
    recent = (c2.get("conversation") or {}).get("recent") or []
    run("complete", "--task-id", "7004", "--result", "那段是测试用的提示音，没有内容。")
    joined = "\n".join(recent)
    check("用户: 做一段语音和一个音频文件发给我" in joined and "Muse: 好的，语音和文件随后发来。" in joined
          and "Muse: [文件] song.mp3" in joined and "Muse: [语音] voice.wav" in joined
          and "状态" not in joined,
          "follow-up claim carries the earlier turns, incl. the files that went out (commands left out)",
          joined.replace("\n", "\n      "))
    st = run("status")
finally:
    serve.send_signal(signal.SIGTERM)
    try:
        rc = serve.wait(8)
    except subprocess.TimeoutExpired:
        rc = None
        serve.kill()
hist = proj / "history" / "chat.jsonl"
check(rc == 0 and str(st.get("version") or "").startswith(("4.", "5.")) and st.get("voice_codec") == "pysilk"
      and (st.get("queue") or {}).get("sent") == 2 and hist.exists() and (hist.stat().st_mode & 0o777) == 0o600,
      "status reports version, codec and the media queue; history file is 0600; SIGTERM exits cleanly",
      f"rc={rc} queue={st.get('queue')} voice_codec={st.get('voice_codec')}")
log_lines = (proj / "logs" / "bridge.log").read_text().splitlines()
bad = [ln for ln in log_lines if any(x in ln for x in ("unexpected error", "worker error", "loop error",
                                                       "Traceback", "notice queued"))]
check(not bad and any("queue:" in ln and " sent " in ln for ln in log_lines),
      "serve log: no errors, and no failure notice was needed (the retries absorbed the drops)", f"{bad[:3]}")
if fails:
    print("---- serve log tail ----")
    print("\n".join(log_lines[-40:]))
print("\nALL V4 E2E CHECKS PASSED" if not fails else f"\n{len(fails)} FAILED")
sys.exit(1 if fails else 0)
