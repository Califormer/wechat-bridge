#!/usr/bin/env python3
"""2026-11-06 end-to-end: a real `serve` process (batching on, as deployed) and
real CLI processes against a local iLink stand-in (plain HTTP on loopback).
  1 five quick messages -> one dispatch, one claim (a warm `claim --wait`), one
    reply; pending.json empty afterwards
  2 a single message -> dispatched 3 s after it arrived; a cold claim gets it
  3 A claimed, B arrives -> A's `complete` exits 4 and sends nothing; the next
    claim has A+B; one reply
  4 a reply whose sendmessage fails -> state/outbox.json; the user's next
    message -> resent within 3 s, outbox empty
  5 the cap, scaled (WECHAT_BRIDGE_BATCH_MAX_MS=5000 on a second serve): a user
    who never pauses 3 s gets the batch dispatched at 5 s, reason=max
Run: python3 tests/test_batch_e2e_20261106.py
"""
import http.server, json, os, shutil, signal, subprocess, sys, tempfile, threading, time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
fails = []


def check(c, m, d=""):
    print(("PASS  " if c else "FAIL  ") + m + (f"\n      {d}" if d and not c else ""))
    if not c:
        fails.append(m)


class ILink(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(n) or b"{}")
        ep = self.path.split("?")[0].rsplit("/", 1)[-1]
        s = self.server
        out = {"ret": 0}
        if ep == "getupdates":
            with s.cond:
                if not s.queue:
                    s.cond.wait(1.0)
                msgs, s.queue = s.queue, []
                s.polls += 1
            out = {"ret": 0, "msgs": msgs, "get_updates_buf": f"c{s.polls}",
                   "longpolling_timeout_ms": 1000}
        elif ep == "sendmessage":
            item = body["msg"]["item_list"][0]
            with s.lock:
                if s.fail_sends > 0:
                    s.fail_sends -= 1
                    s.failed += 1
                    out = {"ret": -1, "errmsg": "system busy"}
                else:
                    s.seq += 1
                    s.sent.append({"item": item, "at": time.time()})
                    out = {"ret": 0, "message_id": f"srv-{s.seq}"}
        elif ep == "getconfig":
            out = {"ret": 0, "typing_ticket": "TT"}
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
        self.queue, self.sent = [], []
        self.cond, self.lock = threading.Condition(), threading.Lock()
        self.seq = self.polls = self.fail_sends = self.failed = 0
        self.base = f"http://127.0.0.1:{self.server_address[1]}"
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def handle_error(self, *a):
        pass

    def push(self, mid, text):
        with self.cond:
            self.queue.append({"message_id": mid, "from_user_id": "owner", "message_type": 1,
                               "context_token": f"T-{mid}",
                               "item_list": [{"type": 1, "text_item": {"text": text}}]})
            self.cond.notify_all()
        return time.time()

    def texts(self):
        return [x["item"]["text_item"]["text"] for x in self.sent if x["item"]["type"] == 1]


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


proj = Path(tempfile.mkdtemp(prefix="wbe2e6-"))
shutil.copy(HERE / "bridge.py", proj / "bridge.py")
srv = Server()
(proj / "state").mkdir()
(proj / "state/credentials.json").write_text(json.dumps(
    {"bot_token": "TKN", "ilink_user_id": "owner", "ilink_bot_id": "bot", "baseurl": srv.base}))
env = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
env.update(WECHAT_BRIDGE_AUTO_PROGRESS_SEC="0", PYTHONUNBUFFERED="1")


def run(*args, timeout=90):
    r = subprocess.run([sys.executable, "bridge.py", *args], cwd=proj, env=env,
                       capture_output=True, text=True, timeout=timeout)
    raw = r.stdout.strip()
    try:
        return r.returncode, (json.loads(raw) if raw else {"_err": r.stderr[-300:]})
    except ValueError:                       # one JSON object per line: take the last
        return r.returncode, json.loads(raw.splitlines()[-1])


def task(mid):
    return json.loads((proj / "inbox" / f"{mid}.json").read_text())


def hb():
    return json.loads((proj / "state/heartbeat.json").read_text())


def start_serve(extra=None):
    e = dict(env, **(extra or {}))
    p = subprocess.Popen([sys.executable, "bridge.py", "serve"], cwd=proj, env=e,
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ok = wait_for(lambda: (hb().get("serve_pid") == p.pid
                           and (hb().get("last_poll_ok_at") or 0) > (hb().get("serve_started_at") or 0)), 20)
    return p, ok


def stop_serve(p):
    p.send_signal(signal.SIGTERM)
    try:
        return p.wait(15)
    except subprocess.TimeoutExpired:
        p.kill()
        return None


serve, ok = start_serve()
rc_stop = None
try:
    check(ok, "serve is up and polling the stand-in (batching on, as deployed)")

    # 1 ---------------------------------------------------------------- burst
    waiter = subprocess.Popen([sys.executable, "bridge.py", "claim", "--limit", "5", "--wait", "25"],
                              cwd=proj, env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    time.sleep(1.0)
    ids = [f"b{i}" for i in range(1, 6)]
    for i, mid in enumerate(ids):
        t_last = srv.push(mid, f"第 {i + 1} 句")
        time.sleep(0.35)
    held = wait_for(lambda: all(task(m).get("batch_open") for m in ids), 5)
    pj = json.loads((proj / "state/pending.json").read_text()) if held else {}
    out, _ = waiter.communicate(timeout=40)
    t_claim = time.time()
    got = json.loads(out.strip().splitlines()[-1])
    lag = t_claim - t_last
    check(held and len(pj.get("batches") or []) == 1
          and [x["message_id"] for x in got.get("tasks") or []] == ids and "merge_hint" in got
          and 3.0 <= lag < 5.5,
          f"5 quick messages: held together in pending.json, then ONE claim returns all 5 "
          f"{lag:.1f}s after the last one (3 s pause)", json.dumps(got, ensure_ascii=False)[:300])
    n0 = len(srv.texts())
    rc, res = run("complete", "--task-id", ids[-1], "--merge-ids", ",".join(ids[:-1]),
                  "--claim-id", got.get("claim_id") or "", "--result", "五句合起来回答。")
    pj = json.loads((proj / "state/pending.json").read_text())
    check(rc == 0 and srv.texts()[n0:] == ["五句合起来回答。"] and not pj.get("batches")
          and all(task(m)["status"] == "sent" for m in ids),
          "one round, one reply for the 5; nothing left in pending.json")

    # 2 ---------------------------------------------------------------- single
    srv.push("s1", "在吗？")
    wait_for(lambda: (proj / "inbox/s1.json").exists(), 10)
    time.sleep(2.4)
    held = task("s1").get("batch_open") is True
    rc, c = run("claim", "--limit", "5")
    t = task("s1")
    d = int(t.get("dispatched_at") or 0) - int(t.get("received_at") or 0)
    check(held and [x["message_id"] for x in c.get("tasks") or []] == ["s1"] and 3000 <= d < 3600,
          f"a single message: still held at 2.4 s, dispatched {d} ms after it arrived; a cold claim gets it")
    run("complete", "--task-id", "s1", "--claim-id", c.get("claim_id") or "", "--result", "在的。")

    # 3 ---------------------------------------------------------------- supersede
    srv.push("A", "帮我查明天的天气")
    wait_for(lambda: (task("A").get("dispatched_at") or 0) > 0, 10)
    rc, ca = run("claim", "--limit", "5")
    c1 = ca.get("claim_id") or ""
    srv.push("B", "是上海的")
    wait_for(lambda: task("A").get("batch_open") is True, 10)
    n0 = len(srv.texts())
    rc4, o4 = run("complete", "--task-id", "A", "--claim-id", c1, "--result", "明天晴（旧回复）")
    time.sleep(0.5)
    check([x["message_id"] for x in ca.get("tasks") or []] == ["A"] and rc4 == 4 and o4.get("superseded")
          and len(srv.texts()) == n0,
          "A claimed, B arrives: A's `complete` exits 4 (SUPERSEDED) and nothing is sent", json.dumps(o4)[:200])
    rc, cb = run("claim", "--limit", "5")
    rc, rb = run("complete", "--task-id", "B", "--merge-ids", "A", "--claim-id", cb.get("claim_id") or "",
                 "--result", "上海明天晴。")
    check([x["message_id"] for x in cb.get("tasks") or []] == ["A", "B"] and "merge_hint" in cb
          and srv.texts()[n0:] == ["上海明天晴。"] and task("A")["status"] == "sent",
          "B is handled normally (batch): the next claim has A+B, one reply answers both")

    # 4 ---------------------------------------------------------------- outbox
    srv.push("o1", "总结一下")
    wait_for(lambda: (task("o1").get("dispatched_at") or 0) > 0, 10)
    rc, co = run("claim", "--limit", "5")
    srv.fail_sends = 1
    run("complete", "--task-id", "o1", "--claim-id", co.get("claim_id") or "", "--result", "总结：要点一、要点二。")
    listed = wait_for(lambda: any(e["task_id"] == "o1" for e in json.loads(
        (proj / "state/outbox.json").read_text())["texts"]), 10)
    st = task("o1")["status"]
    t0 = srv.push("o2", "在吗")
    resent = wait_for(lambda: "总结：要点一、要点二。" in srv.texts(), 8)
    dt = next((x["at"] for x in srv.sent if x["item"].get("text_item", {}).get("text") == "总结：要点一、要点二。"),
              t0 + 99) - t0
    empty = wait_for(lambda: not json.loads((proj / "state/outbox.json").read_text())["texts"], 5)
    check(listed and st == "send_failed" and srv.failed == 1 and resent and dt < 3.0 and empty
          and task("o1")["status"] == "sent",
          f"a reply whose send failed is listed in state/outbox.json; the user's next message gets it "
          f"resent {dt:.2f}s later (< 3 s) and the outbox is empty again")
    rc, c2 = run("claim", "--limit", "5")              # o2 is a normal message: answer it
    run("complete", "--task-id", "o2", "--claim-id", c2.get("claim_id") or "", "--result", "在。")
    rc, st_ = run("status")
    check(rc == 0 and [x["message_id"] for x in c2.get("tasks") or []] == ["o2"]
          and st_.get("batch", {}).get("enabled") is True and st_["batch"]["open"] == []
          and st_.get("text_outbox", {}).get("texts") == 0,
          "`status`: batching enabled, no open batch, text outbox empty", json.dumps(st_.get("batch")))
finally:
    rc_stop = stop_serve(serve)

# 5 -------------------------------------------------------------------- cap (scaled)
serve, ok = start_serve({"WECHAT_BRIDGE_BATCH_MAX_MS": "5000"})
try:
    first = srv.push("cap0", "一")
    for i in range(1, 4):
        time.sleep(1.5)
        srv.push(f"cap{i}", "接着说")
    time.sleep(2.5)
    log = (proj / "logs/bridge.log").read_text() if (proj / "logs/bridge.log").exists() else ""
    line = next((ln for ln in log.splitlines() if "dispatched: 4 message(s)" in ln and "cap0" in ln), "")
    t0 = task("cap0")
    d = int(t0.get("dispatched_at") or 0) - int(t0.get("received_at") or 0)
    check(ok and "reason=max" in line and 5000 <= d < 5600,
          f"cap (scaled to 5 s on this serve): messages 1.5 s apart, never a 3 s pause - dispatched at "
          f"{d} ms with reason=max", line[-160:])
finally:
    rc_stop2 = stop_serve(serve)

log = (proj / "logs/bridge.log").read_text() if (proj / "logs/bridge.log").exists() else ""
bad = [ln for ln in log.splitlines() if "Traceback" in ln or "loop error" in ln or "unexpected error" in ln]
check(rc_stop == 0 and rc_stop2 == 0 and not bad and "batch=3000/60000ms" in log,
      "both serves stopped cleanly; the log has no errors; serve logged batch=3000/60000ms at start",
      "\n      ".join(bad[:5]))
srv.shutdown()
shutil.rmtree(proj, ignore_errors=True)
print()
print(f"{len(fails)} FAILED: {fails}" if fails else "ALL BATCH E2E CHECKS PASSED")
sys.exit(1 if fails else 0)
