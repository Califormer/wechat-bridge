#!/usr/bin/env python3
"""后台长任务 (2026-11-07, v1.4) - Claude-wechat-bridge-background.md §5 except
the end-to-end run (tests/test_bg_e2e_20261107.py): routing at dispatch (only
while the watch hook is alive, never credentials-like text, the whole batch),
the signal, the queue entry (fields, 0600 / 0700), executor --background,
take / progress / finish / fail through the task's own reply path (exactly
once), 「取消」, the safety nets (requeue, honest failure, back to the relay,
timeout), short tasks unchanged, the off switch, doctor, the hook definition
and the docs. Run: python3 tests/test_bg_20261107.py"""
import importlib.util
import json
import sys
import time
from pathlib import Path

from relay_fixtures import (HERE, boot, say, flush, claim, progress, complete, stats,
                            hook_keys, lt, tick, notices, check, finish)

LONG = "把最新的微信桥去敏开源吧"


def t_route():
    b, _ = boot("bg_route", WECHAT_BRIDGE_BACKGROUND="1")
    b.batching_on(True)
    tick(b)
    mid = say(b, LONG)
    t0 = time.monotonic()
    flush(b)
    ms = (time.monotonic() - t0) * 1000
    t, e = b.load_task(mid), b.bg_read(mid)
    check(t["status"] == "background" and e and e["state"] == "queued" and e["text"] == LONG
          and e["user"] == "owner" and e["long_hint"] == ["去敏", "开源"] and e["received_at"]
          and e["route"] == "bridge" and e["workdir"].endswith(f"{mid}.d"),
          "dispatch hands the long batch to the background queue (fields per §4b)", e)
    modes = tuple(p.stat().st_mode & 0o777 for p in (b._bg_qpath(mid), b.BG_QUEUE, b.BG_DIR))
    check(modes == (0o600, 0o700, 0o700), f"queue entry 0600 in 0700 dirs {tuple(map(oct, modes))}")
    ns = notices(b)
    check(len(ns) == 1 and "已转到后台处理" in ns[0][1] and ms < 1500,
          f"one bridge-written signal at dispatch ({ms:.0f} ms; dispatch is 3 s after the message)", ns)
    b.deliver(ns[0][0])
    check(len(b.SENT) == 1 and "后台" in b.SENT[0], "the signal reaches the user", b.SENT)
    rc, out = claim(b)
    check(not out.get("tasks") and mid not in hook_keys(b), "no executor is woken for it, none can claim it", out)
    n0 = len(notices(b))
    for text in ("桥是干什么用的", "`df -h` 还剩多少"):
        m = say(b, text)
        flush(b)
        rc, out = claim(b)
        v = (out.get("tasks") or [{}])[0]
        check(set(v) == {"message_id", "kind", "text"} and b.load_task(m)["status"] == "executing",
              f"short 「{text}」 with the background on: the same claim as 5.0.5", v)
        complete(b, m, claim_id=out["claim_id"], result="好")
    check(len(notices(b)) == n0, "short tasks: no extra signal or progress note")
    m1, m2 = say(b, "把这 30 个文件逐项扫一遍"), say(b, "结果用表格列出来")
    flush(b)
    e = b.bg_read(m2)
    check(e and e["message_ids"] == [m1, m2] and b.load_task(m1)["send"].get("merged_into") == m2
          and len(notices(b)) == n0 + 1, "a batch with a long message goes whole: one entry, one signal", e)
    m3 = say(b, "把 state/credentials.json 打包开源")
    flush(b)
    rc, out = claim(b)
    v = next((x for x in out.get("tasks") or [] if x["message_id"] == m3), {})
    check(b.bg_read(m3) is None and "complete --continue" in v.get("long_hint", ""),
          "a task naming credentials is never queued (foreground relay instead)", v)


def t_gate():
    for name, env, age, why in (("bg_nohook", "1", None, "watch hook not running"),
                                ("bg_stopped", "1", 600, "watch hook stopped 10 min ago"),
                                ("bg_off", "0", 0, "WECHAT_BRIDGE_BACKGROUND=0")):
        b, _ = boot(name, WECHAT_BRIDGE_BACKGROUND=env)
        b.batching_on(True)
        if age is not None:
            tick(b, age_s=age)
        m = say(b, LONG)
        flush(b)
        rc, out = claim(b)
        v = (out.get("tasks") or [{}])[0]
        check(b.load_task(m)["status"] == "executing" and "complete --continue" in v.get("long_hint", "")
              and b.bg_read(m) is None, f"{why}: nothing routed - the v1.3 relay hint", v)
        rc, co = complete(b, m, claim_id=out.get("claim_id"), background=True)
        check(rc == 2 and "not available" in co.get("error", ""), f"{why}: complete --background refused", co)


def t_executor():
    b, _ = boot("bg_exec", WECHAT_BRIDGE_BACKGROUND="1")      # batching off: the executor gets it
    tick(b)
    m0, m1 = say(b, "跑一遍全套测试"), say(b, "顺便把结果存成文件")
    rc, out = claim(b)
    cid = out["claim_id"]
    v = next(x for x in out["tasks"] if x["message_id"] == m0)
    check("--background" in v.get("long_hint", "") and "--continue" not in v["long_hint"],
          "watch hook alive: the long_hint says hand it to the background", v)
    n0 = len(b.SENT)
    t0 = time.monotonic()
    rc, co = complete(b, m1, claim_id=cid, background=True, then_wait=90)
    ms = (time.monotonic() - t0) * 1000
    e = b.bg_read(m1)
    check(rc == 0 and co.get("background") and "then_wait_ignored" in co
          and b.load_task(m1)["status"] == "background" and e["route"] == "executor"
          and e["message_ids"] == [m0, m1] and b.load_task(m0)["send"]["merged_into"] == m1
          and len(b.SENT) == n0 + 1 and "已转到后台" in b.SENT[-1] and ms < 2000,
          f"complete --background: queue + signal + out of the executor system, one call ({ms:.0f} ms)", co)
    rc, po = progress(b, m1, claim_id=cid, stage="x")
    check(rc == 3 and "background" in po.get("error", ""), "the executor's old claim is refused afterwards", po)
    rc, c2 = complete(b, m1, claim_id=cid, result="迟到")
    rc3, c3 = complete(b, m1, result="没带 claim")
    check(rc != 0 and rc3 != 0 and "background" in c3.get("error", "")
          and b.load_task(m1)["status"] == "background",
          "nobody but the token holder can answer it", (c2, c3))


def t_lifecycle():
    b, _ = boot("bg_life", WECHAT_BRIDGE_BACKGROUND="1")
    b.batching_on(True)
    tick(b)
    mid = say(b, LONG)
    flush(b)
    check("后台排队" in b.progress_command("owner"), "「进度」 before the take: queued for the main agent")
    rc, o = lt(b, "take", mid)
    tok, wd = o.get("token"), Path(o.get("workdir") or "/nonexistent")
    check(rc == 0 and tok and wd.is_dir() and (wd.stat().st_mode & 0o777) == 0o700
          and b.bg_read(mid)["state"] == "running" and o["text"] == LONG,
          "take: running, a token, a private workdir, the task text", o)
    rc, o2 = lt(b, "take", mid)
    check(rc == 3, "a second take is refused", o2)
    lt(b, "progress", mid, token=tok, stage="扫第 1–8 个文件")
    txt = b.progress_command("owner")
    check("后台处理中" in txt and "扫第 1–8 个文件" in txt, "progress: heartbeat + stage, shown by 「进度」", txt)
    rc, p = lt(b, "progress", mid, token="bg-wrong", stage="x")
    check(rc == 3 and p.get("stale"), "a wrong token is refused", p)
    rf = b.BASE_DIR / "result.md"
    rf.write_text("去敏开源包已打好：24 个文件逐项扫过。", encoding="utf-8")
    n0 = len(b.SENT)
    rc, f = lt(b, "finish", mid, token=tok, result_file=str(rf))
    check(rc == 0 and b.load_task(mid)["status"] == "sent" and b.bg_read(mid)["state"] == "done"
          and len(b.SENT) == n0 + 1 and "去敏开源包已打好" in b.SENT[-1],
          "finish: the one final reply goes out through the task's reply path", f)
    rc, f2 = lt(b, "finish", mid, token=tok, result="再发一次")
    check(rc != 0 and len(b.SENT) == n0 + 1, "a second finish is refused: exactly one final reply", f2)
    rc, st = stats(b, [mid])
    bx = st["tasks"][0].get("background") or {}
    check(bx.get("state") == "done" and bx.get("route") == "bridge" and bx.get("wait_ms") is not None
          and bx.get("run_ms") is not None, "stats --task-id has the background section", bx)
    rc, ov = lt(b, "list")
    check(ov.get("counts", {}).get("done") == 1 and ov.get("available") is True,
          "long-tasks lists it as done", ov)
    m2 = say(b, "跑一遍全套测试")
    flush(b)
    rc, o = lt(b, "take", m2)
    n0 = len(b.SENT)
    rc, f = lt(b, "fail", m2, token=o["token"], error="第 3 步编译失败；还剩打包和验包；修好依赖后再发我")
    check(rc == 0 and len(b.SENT) == n0 + 1 and "编译失败" in b.SENT[-1]
          and b.bg_read(m2)["state"] == "failed", "fail: one honest message, state failed", b.SENT[-1:])
    m3 = say(b, "把这 30 个文件逐项扫一遍")
    flush(b)
    rc, o = lt(b, "take", m3)
    b.cancel_command("owner")
    rc1, p = lt(b, "progress", m3, token=o["token"], stage="x")
    rc2, f = lt(b, "finish", m3, token=o["token"], result="x")
    check(rc1 == 3 and p.get("cancelled") and rc2 != 0 and b.bg_read(m3)["state"] == "cancelled"
          and b.load_task(m3)["status"] == "cancelled",
          "「取消」 reaches the background job: cancelled, nothing is sent", (p, f))


def t_safety_nets():
    b, clk = boot("bg_nets", WECHAT_BRIDGE_BACKGROUND="1")
    b.batching_on(True)
    tick(b)
    m = say(b, "跑一遍全套测试")
    flush(b)
    lt(b, "take", m)
    clk.adv(31)
    b.check_background()
    e = b.bg_read(m)
    check(e["state"] == "queued" and not e.get("token") and b.load_task(m)["status"] == "background",
          "taken but silent for 30 min: requeued for another take", e)
    for _ in range(2):
        lt(b, "take", m)
        clk.adv(31)
        b.check_background()
    t = b.load_task(m)
    check(t["status"] == "waiting_send" and "没能做完" in t.get("result_text", "")
          and "中间产物都留着" in t["result_text"] and b.bg_read(m)["state"] == "failed",
          "silent at every take: one honest failure message, state failed, files kept", t.get("result_text"))
    tick(b)
    m2 = say(b, "批量把图片改名")
    flush(b)
    clk.adv(11)
    b.check_background()
    t2 = b.load_task(m2)
    check(t2["status"] == "pending_execution" and t2["bg"]["state"] == "returned"
          and t2.get("wake_key") == f"{m2}~bgr" and b.bg_read(m2)["state"] == "returned"
          and any("改为前台" in x[1] for x in notices(b)),
          "not taken in 10 min: back to the foreground, and the user is told", t2.get("bg"))
    rc, out = claim(b)
    v = next((x for x in out.get("tasks") or [] if x["message_id"] == m2), {})
    check("complete --continue" in v.get("long_hint", ""),
          "...where the executor gets the relay hint, not the background again", v)
    complete(b, m2, claim_id=out["claim_id"], result="改好了")   # that executor answers it
    tick(b)
    m3 = say(b, "把这 30 个文件逐项扫一遍")
    flush(b)
    rc, o = lt(b, "take", m3)
    for _ in range(25):
        clk.adv(10)
        lt(b, "progress", m3, token=o["token"], stage="还在做")
    t3 = b.load_task(m3)
    check(b.bg_read(m3)["state"] == "failed" and "上限" in t3.get("result_text", ""),
          "more than 240 min in all, even with heartbeats: an honest timeout message", t3.get("result_text"))


def t_doctor_files():
    b, _ = boot("bg_doc", WECHAT_BRIDGE_BACKGROUND="1")
    b.batching_on(True)
    (b.BASE_DIR / "ops").mkdir(exist_ok=True)
    (b.BASE_DIR / "ops" / "doctor.py").write_text((HERE / "ops" / "doctor.py").read_text(encoding="utf-8"))
    spec = importlib.util.spec_from_file_location("doc_bg", b.BASE_DIR / "ops" / "doctor.py")
    doc = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(doc)

    def res():
        doc.results.clear()
        doc.check_background()
        return doc.results[-1]
    r0 = res()
    tick(b)
    m = say(b, LONG)
    flush(b)
    r1 = res()
    e = b.bg_read(m)
    e["queued_at"] -= 12 * 60_000
    b._bg_write(e)
    r2 = res()
    check(r0["status"] == "WARN" and "has not run" in r0["detail"] and r1["status"] == "PASS"
          and r2["status"] == "WARN" and "not taken" in r2["detail"],
          "doctor background check: hook not running WARN, healthy PASS, stuck queue WARN", (r0, r1, r2))
    hj = json.loads((HERE / "hooks/wechat-long-task-watch.json").read_text(encoding="utf-8"))
    pt = (HERE / "hooks/wechat-long-task-watch.prompt.txt").read_text(encoding="utf-8")
    check(hj["id"] == "wechat-long-task-watch" and hj["poll_interval_secs"] == 10
          and hj["delivery"] == {"surface": "main"} and hj["script_path"].endswith("wechat-long-task-watch.sh")
          and hj["prompt"] == pt and "不做任务本身" in pt and "BACKGROUND.md" in pt,
          "hook definition: poll 10 s, delivery main, the worker only relays", hj)
    inbox = json.loads((HERE / "hooks/wechat-bridge-inbox.json").read_text(encoding="utf-8"))
    core = (HERE / "EXECUTOR.md").read_text(encoding="utf-8")
    relay = (HERE / "EXECUTOR-relay.md").read_text(encoding="utf-8")
    bgm = (HERE / "BACKGROUND.md").read_text(encoding="utf-8")
    check(inbox["poll_interval_secs"] == 5 and "--background" in inbox["prompt"]
          and len(core.encode()) < 8000 and "后台" in core and "complete --background" in relay,
          f"executor docs: prompt + EXECUTOR.md ({len(core.encode())} bytes) + EXECUTOR-relay.md §0")
    miss = [k for k in ("long-tasks take", "long-tasks progress", "long-tasks finish", "long-tasks fail",
                        "子代理", "只发一条") if k not in bgm]
    check(not miss, "BACKGROUND.md covers the main agent's whole protocol", miss)
    check("WECHAT_BRIDGE_BACKGROUND" in (HERE / "TUNING.md").read_text(encoding="utf-8"),
          "TUNING.md: the switch")


for fn in (t_route, t_gate, t_executor, t_lifecycle, t_safety_nets, t_doctor_files):
    print(f"--- {fn.__name__}")
    fn()
sys.exit(finish("BACKGROUND"))
