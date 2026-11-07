#!/usr/bin/env python3
"""v5 end-to-end: a real `serve` process and CLI processes against a local
TLS iLink stand-in that enforces the ClawBot reply budget (10 sendmessage per
context_token, then ret=-2), a real CONNECT proxy, and a systemd notify
socket. Covers: warm executor (claim --wait) + typing latency, a 12+-part
Markdown reply with a code block across the budget (parked, 「继续」 resumes),
a quote by server id, 「状态」, SIGHUP proxy reload without restart,
watchdog pings and a clean stop. Run: python3 tests/test_v5_e2e.py"""
import http.server, json, os, shutil, signal, socket, subprocess, sys, tempfile
import threading, time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_v3 as h  # noqa: E402  TLS cert + CONNECT proxy fixtures

fails = []


def check(c, m, d=""):
    print(("PASS  " if c else "FAIL  ") + m + (f"\n      {d}" if d else ""))
    if not c:
        fails.append(m)


def fences_ok(x):
    return sum(1 for ln in x.split("\n") if ln.strip().startswith("```")) % 2 == 0


class ILink(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        ep = self.path.split("?")[0].rsplit("/", 1)[1]
        s = self.server
        out = {"ret": 0}
        if ep == "getupdates":
            with s.cond:
                if not s.queue:
                    s.cond.wait(1.0)
                msgs, s.queue = s.queue, []
                s.polls += 1
            out = {"ret": 0, "msgs": msgs, "get_updates_buf": f"c{s.polls}",
                   "longpolling_timeout_ms": 1500}
        elif ep == "sendmessage":
            msg = body["msg"]
            tok = msg.get("context_token")
            with s.lock:
                if s.used.get(tok, 0) >= s.quota:
                    out = {"ret": -2}
                else:
                    s.used[tok] = s.used.get(tok, 0) + 1
                    s.seq += 1
                    s.sent.append({"tok": tok, "item": msg["item_list"][0],
                                   "id": f"srv-{s.seq}", "at": time.time()})
                    out = {"ret": 0, "message_id": f"srv-{s.seq}"}
        elif ep == "getconfig":
            out = {"ret": 0, "typing_ticket": "TT"}
        elif ep == "sendtyping":
            s.typing.append((time.time(), body.get("status")))
        raw = json.dumps(out).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self):
        super().__init__(("127.0.0.1", 0), ILink)
        self.socket = h.SERVER_CTX.wrap_socket(self.socket, server_side=True)
        self.queue, self.sent, self.typing, self.used = [], [], [], {}
        self.cond, self.lock = threading.Condition(), threading.Lock()
        self.quota, self.seq, self.polls = 10, 0, 0
        self.base = f"https://localhost:{self.server_address[1]}"
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def handle_error(self, *a):
        pass

    def push(self, mid, text, tok, extra=None):
        item = {"type": 1, "text_item": {"text": text}}
        item.update(extra or {})
        with self.cond:
            self.queue.append({"message_id": mid, "from_user_id": "owner", "message_type": 1,
                               "context_token": tok, "item_list": [item]})
            self.cond.notify_all()
        return time.time()

    def texts(self, tok):
        return [s["item"]["text_item"]["text"] for s in self.sent
                if s["tok"] == tok and s["item"]["type"] == 1]


def wait_for(pred, secs, step=0.05):
    end = time.time() + secs
    while time.time() < end:
        try:
            if pred():
                return True
        except (OSError, ValueError, KeyError):
            pass
        time.sleep(step)
    return False


proj = Path(tempfile.mkdtemp(prefix="v5e2e-"))
shutil.copy(HERE / "bridge.py", proj / "bridge.py")
srv = Server()
(proj / "state").mkdir()
(proj / "state/credentials.json").write_text(json.dumps(
    {"bot_token": "TKN", "ilink_user_id": "owner", "ilink_bot_id": "bot", "baseurl": srv.base}))
nsock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
nsock.bind(str(proj / "notify.sock"))
nsock.settimeout(0.05)
notes = []


def drain():
    while True:
        try:
            notes.append(nsock.recv(256).decode())
        except OSError:
            return True


env = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
env.update(SSL_CERT_FILE=str(h.TMP / "c.pem"), NOTIFY_SOCKET=str(proj / "notify.sock"),
           WECHAT_BRIDGE_AUTO_PROGRESS_SEC="0", PYTHONUNBUFFERED="1")


def run(*args, timeout=90):
    r = subprocess.run([sys.executable, "bridge.py", *args], cwd=proj, env=env,
                       capture_output=True, text=True, timeout=timeout)
    lines = r.stdout.strip().splitlines()
    return json.loads(lines[-1]) if lines else {"_rc": r.returncode, "_err": r.stderr[-300:]}


def hb():
    return json.loads((proj / "state/heartbeat.json").read_text())


def task(mid):
    return json.loads((proj / "inbox" / f"{mid}.json").read_text())


serve = subprocess.Popen([sys.executable, "bridge.py", "serve"], cwd=proj, env=env,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
px = None
try:
    ready = wait_for(lambda: drain() and any("READY=1" in n for n in notes), 15)
    polled = wait_for(lambda: (hb().get("last_poll_ok_at") or 0) > (hb().get("serve_started_at") or 0), 15)
    check(ready and polled and hb().get("transport_mode") == "direct",
          "serve: READY sent to systemd, polling, keep-alive pool running direct (no proxy configured)")

    # 1. warm executor + typing latency
    waiter = subprocess.Popen([sys.executable, "bridge.py", "claim", "--limit", "5", "--wait", "10"],
                              cwd=proj, env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    time.sleep(1.0)
    t_push = srv.push("m1", "写一篇带代码的长文", "T1")
    out, _ = waiter.communicate(timeout=30)
    t_claim = time.time()
    claimed = json.loads(out.strip().splitlines()[-1])
    typing_at = next((t for t, st in srv.typing if st == 1), None)
    # 2026-11-06: a message is dispatched after the batch pause (3 s without
    # a new one, state/pending.json) - before it was the 1.2 s claim window.
    check([x["message_id"] for x in claimed["tasks"]] == ["m1"] and 3.0 <= t_claim - t_push < 5.0
          and typing_at is not None and typing_at - t_push < 1.5,
          f"warm executor (claim --wait) had the message {t_claim - t_push:.1f}s after it was sent "
          f"(incl. the 3 s batch pause); 'typing...' shown after "
          f"{(typing_at or t_push + 99) - t_push:.2f}s")

    # 2. a reply bigger than the budget, with a code block across the parts
    code = "\n".join(f"step_{i:04d} = compute({i})  # padding padding padding" for i in range(1100))
    long = "说明如下：\n\n```python\n" + code + "\n```\n\n以上就是全部代码，最后这句必须在代码块外。"
    (proj / "reply.md").write_text(long)
    res = run("complete", "--task-id", "m1", "--result-file", str(proj / "reply.md"))
    wait_for(lambda: len(srv.texts("T1")) >= 10, 30)
    t1 = srv.texts("T1")
    check(len(t1) == 10 and all(fences_ok(x) for x in t1) and "回复任意内容" in t1[-1]
          and (res.get("send") or {}).get("parked") and task("m1")["send"].get("deferred") == "quota",
          "budget: 10 messages on the first token, every one valid Markdown, the 10th carries the note; "
          "complete reports parked=true", json.dumps(res.get("send"), ensure_ascii=False)[:200])
    srv.push("m2", "继续", "T2")
    done = wait_for(lambda: task("m1")["status"] == "sent", 30)
    t2 = srv.texts("T2")
    steps = [ln for x in t1 + t2 for ln in x.split("\n") if ln.startswith("step_")]
    check(done and t2 and len(steps) == 1100 and len(set(steps)) == 1100 and all(fences_ok(x) for x in t2)
          and t2[-1].rstrip().endswith("最后这句必须在代码块外。") and task("m2")["status"] == "sent"
          and not any(x.strip() == "继续" or "已" in x[:2] for x in t2[:1] if len(x) < 20),
          f"「继续」: the remaining {len(t2)} parts go out on the new message's budget, every code line "
          "exactly once, the closing sentence outside the code block, no reply of its own for 「继续」")

    # 3. quote by server id
    third = [s for s in srv.sent if s["tok"] == "T1"][2]
    srv.push("m3", "第三条里那段是干嘛的", "T3", {"ref_msg": {"svr_id": third["id"]}})
    wait_for(lambda: task("m3")["status"] == "pending_execution", 10)
    c3 = run("claim", "--limit", "5")
    q = ((c3.get("tasks") or [{}])[0]).get("quote") or ""
    first_step = next(ln for ln in third["item"]["text_item"]["text"].split("\n") if ln.startswith("step_"))
    check(first_step in q, "a quote carrying only the server id of the bot's 3rd message is restored "
          "for the executor", q[:120])
    run("complete", "--task-id", "m3", "--result", "那段在算第 N 步。")

    # 4. 状态
    srv.push("m4", "状态", "T4")
    wait_for(lambda: srv.texts("T4"), 15)
    st = (srv.texts("T4") or [""])[0]
    check("回复额度" in st and "主动消息窗口" in st and "直连" in st and "响应" in st,
          "「状态」 answered by the bridge: reply window, reply budget, transport, response times", st)

    # 5. SIGHUP: re-read proxy.env, move to the proxy without a restart
    px = h.Proxy()
    purl = getattr(px, "url", None) or f"http://127.0.0.1:{px.s.getsockname()[1]}"
    (proj / "state/proxy.env").write_text(f"https_proxy={purl}\n")
    os.kill(serve.pid, signal.SIGHUP)
    switched = wait_for(lambda: hb().get("transport_mode") == "proxy", 10)
    srv.push("m5", "切到代理之后还能收到吗", "T5")
    got = wait_for(lambda: task("m5")["status"] == "pending_execution", 15)
    # The switch takes effect on the next poll: a message pushed during
    # the switch may still be picked up by the in-flight direct poll, so
    # wait for a proxied poll to register before asserting tunnels > 0.
    tunneled = wait_for(lambda: px.tunnels > 0, 15)
    check(switched and got and tunneled and serve.poll() is None and hb().get("serve_pid") == serve.pid,
          f"SIGHUP: serve re-read proxy.env and polls through the proxy ({px.tunnels} tunnel(s)) - same "
          "process, no restart, no lost first poll; proxy takes effect by the next poll cycle")
finally:
    drain()
    wd = sum(1 for n in notes if "WATCHDOG=1" in n)
    serve.send_signal(signal.SIGTERM)
    try:
        rc = serve.wait(10)
    except subprocess.TimeoutExpired:
        rc = None
        serve.kill()
    time.sleep(0.2)
    drain()
log = (proj / "logs/bridge.log").read_text() if (proj / "logs/bridge.log").exists() else ""
bad = [ln for ln in log.splitlines() if "Traceback" in ln or "unexpected error" in ln or "loop error" in ln]
check(wd >= 1 and rc == 0 and any("STOPPING=1" in n for n in notes) and not bad,
      f"systemd: {wd} WATCHDOG ping(s) from the poll loop; SIGTERM -> STOPPING=1, clean exit; log has no errors",
      "\n      ".join(bad[:5]))
print()
print(f"{len(fails)} FAILED: {fails}" if fails else "ALL V5 E2E CHECKS PASSED")
sys.exit(1 if fails else 0)
