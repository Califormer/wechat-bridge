#!/usr/bin/env python3
"""Bridge-side latency benchmark (2026-11-05). Starts a fake iLink server
(plain HTTP on loopback) and a real `serve` from a temp copy of the given
bridge.py, then measures only what the BRIDGE adds:
  inbound  : message available at the server -> task file on disk
  typing   : message available -> "typing..." sent
  warm     : message available -> a waiting `claim --wait` returns it
             (a sentence-like message and a fragment, which may be followed
             by more and is therefore coalesced longer)
  deliver  : `complete` started -> its sendmessage reaches the server
  complete : the whole `complete` CLI call (what the agent waits for)
  cli      : start-up of one CLI command (`status`)
The LLM's own time and the platform's cold start are NOT measured here.
Usage: python3 tests/bench_latency.py [path/to/bridge.py] [--rounds N]
Not run by tests/run_all.py (it is a benchmark, not a test).
"""
import json, os, shutil, statistics, subprocess, sys, tempfile, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


class Fake:
    def __init__(self):
        self.cv = threading.Condition()
        self.queue, self.sent, self.typing = [], [], []


def handler_for(fake):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *a):
            pass

        def _reply(self, obj):
            data = json.dumps(obj).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n) or b"{}")
            ep = self.path.split("?")[0].rsplit("/", 1)[-1]
            if ep == "getupdates":
                with fake.cv:
                    if not fake.queue:
                        fake.cv.wait(1.0)
                    msgs, fake.queue = fake.queue, []
                return self._reply({"ret": 0, "msgs": msgs, "longpolling_timeout_ms": 1000,
                                    "get_updates_buf": f"B{time.time():.6f}"})
            with fake.cv:
                if ep == "sendmessage":
                    fake.sent.append((time.perf_counter(), body))
                elif ep == "sendtyping":
                    fake.typing.append((time.perf_counter(), body.get("status")))
                fake.cv.notify_all()
            if ep == "getconfig":
                return self._reply({"ret": 0, "typing_ticket": "TT"})
            return self._reply({"ret": 0, "message_id": 900000 + len(fake.sent)})
    return H


def p50(xs):
    return f"{statistics.median(xs):7.0f} ms" if xs else "    n/a"


def main() -> int:
    argv = [a for a in sys.argv[1:]]
    rounds = 4
    if "--rounds" in argv:
        rounds = int(argv[argv.index("--rounds") + 1])
        del argv[argv.index("--rounds"):argv.index("--rounds") + 2]
    src = Path(argv[0]) if argv else Path(__file__).resolve().parent.parent / "bridge.py"
    d = Path(tempfile.mkdtemp(prefix="wb-bench-"))
    shutil.copy(src, d / "bridge.py")
    if (src.parent / "wb").exists():
        shutil.copy(src.parent / "wb", d / "wb")
    cli = ["wb"] if (d / "wb").exists() else ["bridge.py"]
    fake = Fake()
    srv = ThreadingHTTPServer(("127.0.0.1", 0), handler_for(fake))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    (d / "state").mkdir()
    (d / "state" / "credentials.json").write_text(json.dumps({
        "bot_token": "TKN", "ilink_bot_id": "bot", "ilink_user_id": "owner",
        "baseurl": f"http://127.0.0.1:{srv.server_address[1]}"}))
    env = {k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
    env["PYTHONUNBUFFERED"] = "1"
    py = sys.executable
    run = lambda *a, **k: subprocess.run([py, *cli, *a], cwd=d, env=env,  # noqa: E731
                                         capture_output=True, text=True, timeout=60, **k)
    run("status")                                   # warm the bytecode cache
    serve = subprocess.Popen([py, "bridge.py", "serve"], cwd=d, env=env,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    hb = d / "state" / "heartbeat.json"
    t_end = time.time() + 20
    while time.time() < t_end:
        try:
            if json.loads(hb.read_text()).get("last_poll_ok_at"):
                break
        except (OSError, ValueError):
            pass
        time.sleep(0.05)
    res = {k: [] for k in ("inbound", "typing", "warm_sentence", "warm_fragment",
                           "deliver", "complete", "cli")}
    mid = 5000
    try:
        for r in range(rounds):
            for kind, text in (("warm_sentence", "帮我看看这段代码哪里有问题？"),
                               ("warm_fragment", "等一下")):
                mid += 1
                claim = subprocess.Popen([py, *cli, "claim", "--limit", "5", "--wait", "15"],
                                         cwd=d, env=env, stdout=subprocess.PIPE, text=True)
                time.sleep(0.8)                     # the warm executor is waiting
                n_typing = len(fake.typing)
                msg = {"message_id": mid, "from_user_id": "owner", "to_user_id": "bot",
                       "message_type": 1, "context_token": f"CT{mid}",
                       "create_time_ms": int(time.time() * 1000),
                       "item_list": [{"type": 1, "text_item": {"text": text}}]}
                with fake.cv:
                    t0 = time.perf_counter()
                    fake.queue.append(msg)
                    fake.cv.notify_all()
                task_file = d / "inbox" / f"{mid}.json"
                while not task_file.exists() and time.perf_counter() - t0 < 10:
                    time.sleep(0.002)
                res["inbound"].append((time.perf_counter() - t0) * 1000)
                out, _ = claim.communicate(timeout=30)
                res[kind].append((time.perf_counter() - t0) * 1000)
                typ = [t for t, s in fake.typing[n_typing:] if s == 1]
                if typ:
                    res["typing"].append((typ[0] - t0) * 1000)
                got = [t["message_id"] for t in json.loads(out or "{}").get("tasks", [])]
                if str(mid) not in [str(x) for x in got]:
                    print(f"round {r}: claim did not return {mid}: {out[:200]}")
                    continue
                n_sent = len(fake.sent)
                t2 = time.perf_counter()
                c = run("complete", "--task-id", str(mid), "--result", "好的，这就看。")
                t4 = time.perf_counter()
                sent = [t for t, b in fake.sent[n_sent:]]
                if c.returncode == 0 and sent:
                    res["deliver"].append((sent[0] - t2) * 1000)
                    res["complete"].append((t4 - t2) * 1000)
        for _ in range(5):
            t0 = time.perf_counter()
            run("status")
            res["cli"].append((time.perf_counter() - t0) * 1000)
    finally:
        serve.terminate()
        with contextlib_suppress():
            serve.wait(timeout=20)
        srv.shutdown()
        shutil.rmtree(d, ignore_errors=True)
    labels = {"inbound": "inbound  消息 → 任务落盘", "typing": "typing   消息 → 正在输入",
              "warm_sentence": "warm     消息 → claim 返回（整句）",
              "warm_fragment": "warm     消息 → claim 返回（碎片，等连发）",
              "deliver": "deliver  complete 开始 → sendmessage 到达",
              "complete": "complete 整个 complete 调用", "cli": "cli      一条命令的启动+执行"}
    print(f"bench {src} ({' '.join(cli)}), {rounds} rounds, p50:")
    for k, label in labels.items():
        print(f"  {label:34} {p50(res[k])}")
    return 0


class contextlib_suppress:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return True


if __name__ == "__main__":
    sys.exit(main())
