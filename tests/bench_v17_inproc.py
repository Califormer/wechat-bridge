#!/usr/bin/env python3
"""v1.7 bench (in-process, bridge only, no network, no model): the same
operations on an old bridge.py and this one, alternating blocks so drift hits
both. Not run by run_all (not a test_* file).
Usage: python3 tests/bench_v17_inproc.py /path/to/old/bridge.py [blocks]"""
import statistics
import sys
import time

from relay_fixtures import boot, say, flush, claim, progress, complete, lt, tick

OLD = sys.argv[1]
BLOCKS = int(sys.argv[2]) if len(sys.argv) > 2 else 8
LONG = "把最新的微信桥去敏开源吧"


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(round(p * (len(xs) - 1))))]


def mk(tag, src):
    b, clk = boot(f"bench_{tag}", src, WECHAT_BRIDGE_BACKGROUND="1")
    b.batching_on(True)
    tick(b)
    return b


def short_round(b, res, n):
    for _ in range(n):
        m = say(b, "桥是干什么用的")
        flush(b)
        t0 = time.perf_counter()
        rc, out = claim(b)
        t1 = time.perf_counter()
        cid = out.get("claim_id")
        progress(b, m, claim_id=cid, stage="查")
        t2 = time.perf_counter()
        complete(b, m, claim_id=cid, result="把微信消息转给 Muse")
        t3 = time.perf_counter()
        res["claim"].append((t1 - t0) * 1e3)
        res["progress"].append((t2 - t1) * 1e3)
        res["complete"].append((t3 - t2) * 1e3)


def seed_done(b, n):
    """n finished queue entries (kept 14 days), like a busy fortnight."""
    now = b.now_ms()
    for i in range(n):
        b._bg_write({"v": 1, "message_id": f"old{i}", "message_ids": [f"old{i}"], "user": "owner",
                     "text": "旧任务" * 20, "texts": [], "state": "done", "queued_at": now - 86_400_000,
                     "finished_at": now - 3_600_000, "takes": 1, "wakes": 1,
                     "notes": [{"seq": 1, "text": "x" * 80}], "note_seq": 1, "notes_seen_seq": 1})


def bg_round(b, res, n, live):
    for _ in range(n):
        t0 = time.perf_counter()
        b._bg_lane()
        t1 = time.perf_counter()
        b.bg_position(live)
        t2 = time.perf_counter()
        b.check_background()
        t3 = time.perf_counter()
        res["lane"].append((t1 - t0) * 1e3)
        res["position"].append((t2 - t1) * 1e3)
        res["check_background"].append((t3 - t2) * 1e3)


def handoff_round(b, res, n):
    for _ in range(n):
        m = say(b, "把这 30 个文件逐项扫一遍")
        t0 = time.perf_counter()
        flush(b)
        res["handoff"].append((time.perf_counter() - t0) * 1e3)
        with b.task_lock():
            t = b.load_task(m)
            t["status"] = "cancelled"
            b._bg_close(t, "cancelled", b.now_ms())
            b.save_task(t)


def main():
    keys = ("claim", "progress", "complete", "lane", "position", "check_background", "handoff")
    res = {v: {k: [] for k in keys} for v in ("old", "new")}
    bs = {"old": mk("old", OLD), "new": mk("new", None)}
    bgs = {"old": mk("oldbg", OLD), "new": mk("newbg", None)}
    lives = {}
    for v, b in bgs.items():
        seed_done(b, 300)
        lives[v] = say(b, LONG)
        flush(b)
    for blk in range(BLOCKS):
        order = ("old", "new") if blk % 2 == 0 else ("new", "old")
        for v in order:
            short_round(bs[v], res[v], 20)
            bg_round(bgs[v], res[v], 25, lives[v])
            handoff_round(bgs[v], res[v], 4)
    print(f"{'metric (ms)':22} {'old p50/p95':>16} {'new p50/p95':>16}   n")
    for k in keys:
        o, n = res["old"][k], res["new"][k]
        print(f"{k:22} {pct(o, .5):7.2f}/{pct(o, .95):7.2f} {pct(n, .5):7.2f}/{pct(n, .95):7.2f}   {len(n)}")
    print("mean claim+progress+complete old/new:",
          round(statistics.mean(res['old']['claim']) + statistics.mean(res['old']['progress'])
                + statistics.mean(res['old']['complete']), 2), "/",
          round(statistics.mean(res['new']['claim']) + statistics.mean(res['new']['progress'])
                + statistics.mean(res['new']['complete']), 2))


if __name__ == "__main__":
    main()
