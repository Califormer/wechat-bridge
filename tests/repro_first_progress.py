#!/usr/bin/env python3
"""Repro of live test 2-3 (2026-11-05): a long blocking step (the full test run,
about 147 s) and when the user first hears anything. Isolated: a real
`bridge.py serve` per scenario against a local TLS stand-in for iLink, fake
credentials, temp dirs, nothing leaves 127.0.0.1. Not in run_all.py (it takes
about 150 s in real time); run it on purpose:

    python3 tests/repro_first_progress.py              # real time, 147 s step
    python3 tests/repro_first_progress.py --scale 0.05 # quick, same order of events

Two scenarios run side by side, each with a SCRIPTED executor (this script, not
an LLM: it shows what the bridge does under each behaviour, not whether the
model will follow the rule; that is the real-session check):
  old  = what 2-3 did: claim, run the step, first progress 132.3 s after the
         claim, complete 9.4 s later (Claude.md 2.3)
  new  = the Step 1 rule: claim, progress --stage --note --notify first, then the
         147 s step, then complete
Checks (each failed check prints BUG and counts):
  new: first progress <= 10 s after the claim; the note reaches the user (the
       stand-in) <= 10 s after the claim and before the step ends; no
       auto-progress on top; the final reply arrives; stats agrees
  old: the silence reproduces (nothing reaches the user before the answer) and
       Step 0 makes it visible (to_first_call.max / to_first_call_top / --task-id)
"""
import argparse, http.server, json, os, shutil, signal, subprocess, sys, tempfile
import threading, time
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))
import test_v3 as h  # noqa: E402  TLS certificate fixtures (local, self-signed)

STEP_S = 147.0                    # the blocking step (Claude.md 2.3)
OLD_FIRST_S, OLD_TAIL_S = 132.3, 9.4
LIMIT_MS = 10_000                 # Step 1 acceptance: first progress within 10 s
TASK_TEXT = "在 /tmp 隔离目录解压性能包，跑 tests/run_all.py，22 套测试是否全过，不动线上桥"
NOTE = "开始在 /tmp 隔离区解压性能包并跑整套测试，约 2 到 3 分钟，跑完把结果发你"
RESULT = "（复现脚本模拟）整套测试已跑完：全部通过。"


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
            item = body["msg"]["item_list"][0]
            with s.lock:
                s.seq += 1
                s.sent.append({"at": time.time(), "type": item.get("type"),
                               "text": (item.get("text_item") or {}).get("text", "")})
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
        self.socket = h.SERVER_CTX.wrap_socket(self.socket, server_side=True)
        self.queue, self.sent = [], []
        self.cond, self.lock = threading.Condition(), threading.Lock()
        self.seq = self.polls = 0
        self.base = f"https://localhost:{self.server_address[1]}"
        threading.Thread(target=self.serve_forever, daemon=True).start()

    def handle_error(self, *a):
        pass

    def push(self, mid, text, tok):
        with self.cond:
            self.queue.append({"message_id": mid, "from_user_id": "owner", "message_type": 1,
                               "context_token": tok,
                               "item_list": [{"type": 1, "text_item": {"text": text}}]})
            self.cond.notify_all()


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


class Project:
    """One isolated bridge: temp dir, fake credentials, its own serve."""

    def __init__(self, name):
        self.name = name
        self.dir = Path(tempfile.mkdtemp(prefix=f"wb-repro-{name}-"))
        for f in ("bridge.py", "wb"):
            shutil.copy(HERE / f, self.dir / f)
        self.srv = Server()
        (self.dir / "state").mkdir()
        (self.dir / "state/credentials.json").write_text(json.dumps(
            {"bot_token": "FAKE", "ilink_user_id": "owner", "ilink_bot_id": "bot",
             "baseurl": self.srv.base}))
        os.chmod(self.dir / "state/credentials.json", 0o600)
        self.env = {k: v for k, v in os.environ.items()
                    if "proxy" not in k.lower() and not k.startswith("WECHAT_BRIDGE_")}
        self.env.update(SSL_CERT_FILE=str(h.TMP / "c.pem"), PYTHONUNBUFFERED="1")
        self.serve = subprocess.Popen([sys.executable, "bridge.py", "serve"], cwd=self.dir,
                                      env=self.env, stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL)

    def wb(self, *args, timeout=120):
        r = subprocess.run([sys.executable, "wb", *args], cwd=self.dir, env=self.env,
                           capture_output=True, text=True, timeout=timeout)
        try:
            return json.loads(r.stdout)
        except ValueError:
            lines = r.stdout.strip().splitlines()
            try:
                return json.loads(lines[-1]) if lines else {"_rc": r.returncode}
            except ValueError:
                return {"_rc": r.returncode, "_out": r.stdout[-300:], "_err": r.stderr[-300:]}

    def task(self, mid):
        return json.loads((self.dir / "inbox" / f"{mid}.json").read_text())

    def stop(self):
        self.serve.send_signal(signal.SIGTERM)
        try:
            self.serve.wait(15)
        except subprocess.TimeoutExpired:
            self.serve.kill()


def run_scenario(p, mode, scale, res):
    """The scripted executor. mode 'new' follows the Step 1 rule, 'old' replays 2-3."""
    try:
        ok = wait_for(lambda: (p.serve.poll() is None and json.loads(
            (p.dir / "state/heartbeat.json").read_text()).get("last_poll_ok_at")), 20)
        if not ok:
            res["error"] = "serve did not start polling"
            return
        p.srv.push("m23", TASK_TEXT, "T23")
        if not wait_for(lambda: p.task("m23")["status"] == "pending_execution", 20):
            res["error"] = "message never became pending_execution"
            return
        got = p.wb("claim", "--limit", "5")
        tasks = got.get("tasks") or []
        cid = (tasks[0].get("claim_id") if tasks else None) or got.get("claim_id")
        res["claimed_ids"] = [t.get("message_id") for t in tasks]
        args = ["--task-id", "m23"] + (["--claim-id", cid] if cid else [])
        if mode == "new":
            res["progress"] = p.wb("progress", *args, "--stage", "跑整套测试（tests/run_all.py）",
                                   "--note", NOTE, "--notify")
            res["step_start"] = time.time()
            time.sleep(STEP_S * scale)                   # the blocking step
            res["step_end"] = time.time()
        else:
            res["step_start"] = time.time()
            time.sleep(OLD_FIRST_S * scale)              # ran the tests first
            res["progress"] = p.wb("progress", *args, "--stage", "测试跑完，整理结果")
            time.sleep(OLD_TAIL_S * scale)
            res["step_end"] = time.time()
        res["complete"] = p.wb("complete", *args, "--result", RESULT)
        wait_for(lambda: p.task("m23")["status"] == "sent", 30)
        res["one"] = (p.wb("stats", "--task-id", "m23").get("tasks") or [{}])[0]
        res["agg"] = ((p.wb("stats").get("evidence") or {}).get("exec") or {})
        res["sent"] = list(p.srv.sent)
        res["claimed_at_ms"] = int(p.task("m23")["execution"]["claimed_at"])
    except Exception as e:  # noqa: BLE001
        res["error"] = f"{type(e).__name__}: {e}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--scale", type=float, default=1.0,
                    help="time scale for the step lengths (1.0 = real 147 s)")
    ap.add_argument("--keep", action="store_true", help="keep the temp project dirs")
    a = ap.parse_args()
    bugs = []

    def check(cond, msg, detail=""):
        print(("OK    " if cond else "BUG   ") + msg + (f"\n      {detail}" if detail and not cond else ""))
        if not cond:
            bugs.append(msg)

    projs = {m: Project(m) for m in ("old", "new")}
    res = {m: {} for m in projs}
    t0 = time.time()
    try:
        th = [threading.Thread(target=run_scenario, args=(projs[m], m, a.scale, res[m]))
              for m in projs]
        for t in th:
            t.start()
        for t in th:
            t.join(STEP_S * a.scale + 240)
    finally:
        for p in projs.values():
            p.stop()
    print(f"scale {a.scale}: step {STEP_S * a.scale:.1f} s, wall {time.time() - t0:.1f} s\n")

    for m in ("old", "new"):
        r = res[m]
        if r.get("error") or "one" not in r:
            check(False, f"{m}: scenario ran to the end", r.get("error") or "no stats")
            continue
        cl = r["claimed_at_ms"] / 1000
        texts = [(s["at"] - cl, s["text"]) for s in r["sent"] if s["type"] == 1]
        first_text = min((dt for dt, x in texts), default=None)
        note_at = next((dt for dt, x in texts if x.strip() == NOTE), None)
        reply_at = next((dt for dt, x in texts if RESULT in x), None)
        one = r["one"]
        print(f"[{m}] after the claim: first progress {one.get('to_first_call_ms')} ms; "
              f"first message to the user {None if first_text is None else round(first_text, 1)} s; "
              f"step ended {round(r['step_end'] - cl, 1)} s; reply {None if reply_at is None else round(reply_at, 1)} s; "
              f"notices {[(n.get('reason'), n.get('sent_ms')) for n in one.get('notices') or []]}")
        check(r.get("claimed_ids") == ["m23"] and one.get("status") == "sent" and reply_at is not None
              and reply_at >= r["step_end"] - cl - 0.5,
              f"{m}: claimed, final reply delivered after the step", json.dumps(one, ensure_ascii=False)[:300])
        check(not any(n.get("reason") == "auto_progress" for n in one.get("notices") or []),
              f"{m}: no auto-progress note from the bridge", str(one.get("notices")))
        tf = one.get("to_first_call_ms")
        if m == "new":
            check(tf is not None and tf <= LIMIT_MS,
                  f"new: first progress {tf} ms after the claim (limit {LIMIT_MS} ms)")
            check(note_at is not None and note_at * 1000 <= LIMIT_MS and note_at < r["step_end"] - cl,
                  f"new: the user has the note {None if note_at is None else round(note_at, 1)} s after the "
                  "claim, while the step is still running", str(texts)[:300])
            check(one.get("first_notice_sent_ms") is not None
                  and one["first_notice_sent_ms"] <= LIMIT_MS
                  and [n.get("reason") for n in one.get("notices") or []] == ["progress"],
                  "new: stats --task-id shows the one progress notice and when it was sent",
                  str(one.get("notices")))
            agg = r["agg"].get("to_first_call") or {}
            check(agg.get("n") == 1 and agg.get("max") == tf,
                  "new: stats evidence agrees (to_first_call n=1, max = this task)", str(agg))
        else:
            want = OLD_FIRST_S * a.scale * 1000
            check(tf is not None and abs(tf - want) <= 3000 + want * 0.02,
                  f"old: first progress {tf} ms after the claim, as in 2-3 (expected about {want:.0f} ms)")
            check(first_text is not None and reply_at is not None and abs(first_text - reply_at) < 0.01,
                  "old: the silence reproduces - the first thing the user gets is the answer itself",
                  str(texts)[:300])
            agg = r["agg"].get("to_first_call") or {}
            top = (r["agg"].get("to_first_call_top") or [{}])[0]
            check(agg.get("max") == tf and top.get("task_id") == "m23" and one.get("calls_ms") == [tf],
                  "old: Step 0 shows it - to_first_call.max, to_first_call_top[0] and --task-id calls_ms",
                  f"{agg} {top}")
    if a.keep:
        print("\nkept:", ", ".join(str(p.dir) for p in projs.values()))
    else:
        for p in projs.values():
            shutil.rmtree(p.dir, ignore_errors=True)
    print(f"\nBUGS: {len(bugs)}" + (f" {bugs}" if bugs else ""))
    return 1 if bugs else 0


if __name__ == "__main__":
    sys.exit(main())
