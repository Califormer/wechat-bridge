#!/usr/bin/env python3
"""End to end, Claude-wechat-bridge-background.md §5.1, in the sandbox: one
long message -> a signal within 20 s, the queue written, no executor turn ->
the real hooks/wechat-long-task-watch.sh (runtime stubbed as in
tests/test_hook.py) wakes the main agent on its next tick -> the main agent
takes it and a "sub-agent" works in short steps (artifacts in the workdir,
progress heartbeats) while the user sends a short message that the bridge
answers normally -> long-tasks finish -> exactly one final result. Then no
re-wake, the wake log, the list. Run: python3 tests/test_bg_e2e_20261107.py"""
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from relay_fixtures import HERE, boot, say, flush, claim, complete, lt, notices, check, finish

RUNTIME = r'''
hook_state_get() { [[ -f "$HOOK_STATE" ]] && cat "$HOOK_STATE" || echo '{}'; }
hook_state_set() { printf '%s' "$1" > "$HOOK_STATE"; }
log()    { echo "LOG $1" >> "$HOOK_EVENTS"; }
wake()   { echo "WAKE $1 $2" >> "$HOOK_EVENTS"; exit 0; }
silent() { echo "SILENT $1" >> "$HOOK_EVENTS"; exit 0; }
'''


def main():
    b, clk = boot("bg_e2e", WECHAT_BRIDGE_BACKGROUND="1")
    b.batching_on(True)
    shutil.copy(HERE / "wb", b.BASE_DIR / "wb")
    home = b.BASE_DIR / "home"
    home.mkdir()
    (home / "runtime.sh").write_text(RUNTIME)
    ev = home / "events.log"
    env = dict(os.environ, HOME=str(home), HATCH_HOOK_RUNTIME=str(home / "runtime.sh"),
               HOOK_EVENTS=str(ev), HOOK_STATE=str(home / "state.json"),
               WECHAT_BRIDGE_DIR=str(b.BASE_DIR))
    for k in [k for k in env if k.startswith("WECHAT_BRIDGE_") and k != "WECHAT_BRIDGE_DIR"]:
        env.pop(k)

    def hook():
        n = len(ev.read_text().splitlines()) if ev.exists() else 0
        t0 = time.monotonic()
        r = subprocess.run(["bash", str(HERE / "hooks/wechat-long-task-watch.sh")], env=env,
                           capture_output=True, text=True, timeout=60)
        ms = (time.monotonic() - t0) * 1000
        return r.returncode, (ev.read_text().splitlines()[n:] if ev.exists() else []), ms, r.stderr

    rc, new, ms_idle, err = hook()
    check(rc == 0 and not new and b.bg_available(),
          f"hook tick with nothing queued: no wake, liveness touched ({ms_idle:.0f} ms)", (new, err))
    t_msg = b.now_ms()
    mid = say(b, "把最新的微信桥去敏开源吧")
    clk.adv(3 / 60)                              # the batch closes 3 s after the message
    flush(b)
    ns = notices(b)
    sig_ms = (b.load_task(ns[0][0])["received_at"] - t_msg) if ns else None
    for nid, _ in ns:
        b.deliver(nid)
    check(b.load_task(mid)["status"] == "background" and sig_ms is not None and sig_ms <= 20_000
          and len(b.SENT) == 1 and "后台" in b.SENT[0],
          f"signal {(sig_ms or 0) / 1000:.1f} s after the message (<= 20 s); the task is in the queue", b.SENT)
    rc, out = claim(b)
    check(not out.get("tasks"), "no executor turn is spent on the long task", out)
    rc, new, ms_wake, err = hook()
    wakes = [x for x in new if x.startswith("WAKE wechat_long_task ")]
    payload = json.loads(wakes[0].split(" ", 2)[2]) if wakes else {}
    check(len(wakes) == 1 and (payload.get("tasks") or [{}])[0].get("id") == mid
          and "BACKGROUND.md" in payload.get("manual", ""),
          f"the next hook tick (<= 10 s later) wakes the main agent with the task ({ms_wake:.0f} ms)",
          (new, err))
    log = (b.BG_DIR / "watch.log").read_text()
    check(f" wake {mid}" in log, "the wake is in state/long-tasks/watch.log", log)
    rc, new, _, _ = hook()
    check(not any(x.startswith("WAKE") for x in new), "the following tick does not wake again", new)
    rc, o = lt(b, "take", mid)
    tok, wd = o.get("token"), Path(o.get("workdir") or "/nonexistent")
    check(rc == 0 and tok and wd.is_dir(), "the main agent takes it", o)
    for i, step in enumerate(("比对改动清单", "逐项扫敏感内容", "去敏打包并验包"), 1):
        (wd / f"step{i}.txt").write_text(f"{step}: ok\n", encoding="utf-8")
        rc, p = lt(b, "progress", mid, token=tok, stage=f"第 {i}/3 步：{step}")
        check(rc == 0 and p.get("ok"), f"sub-agent step {i}: artifact on disk, progress accepted")
        if i == 2:
            m2 = say(b, "桥是干什么用的")
            clk.adv(3 / 60)
            flush(b)
            rc, out = claim(b)
            check([x["message_id"] for x in out.get("tasks") or []] == [m2]
                  and "superseded" not in json.dumps(out, ensure_ascii=False),
                  "meanwhile a short message is claimed alone - nothing merged or interrupted", out)
            n0 = len(b.SENT)
            rc, co = complete(b, m2, claim_id=out["claim_id"], result="桥把微信消息转给 Muse 执行。")
            check(rc == 0 and len(b.SENT) == n0 + 1 and b.bg_read(mid)["state"] == "running",
                  "...and answered at once while the long task keeps running", co)
            txt = b.progress_command("owner")
            check("后台处理中" in txt and "第 2/3 步" in txt, "「进度」 shows the background step", txt)
    res = wd / "result.md"
    res.write_text("去敏开源包已打好：比对、逐项扫、去敏打包、验包都完成（产物见 workdir）。",
                   encoding="utf-8")
    n0 = len(b.SENT)
    rc, f = lt(b, "finish", mid, token=tok, result_file=str(res))
    finals = [s for s in b.SENT if "去敏开源包已打好" in s]
    check(rc == 0 and len(finals) == 1 and len(b.SENT) == n0 + 1 and b.load_task(mid)["status"] == "sent"
          and b.bg_read(mid)["state"] == "done",
          "the main agent sends exactly one final result through the task's reply path", f)
    check(len(b.SENT) == 3, "the user got 3 messages: the signal, the short answer, the final result", b.SENT)
    rc, new, _, _ = hook()
    check(not any(x.startswith("WAKE") for x in new), "finished: no more wakes", new)
    rc, ov = lt(b, "list")
    check(ov["counts"].get("done") == 1 and not ov["counts"].get("queued")
          and not ov["counts"].get("running"), "long-tasks: done 1, nothing pending", ov["counts"])


main()
sys.exit(finish("BACKGROUND E2E"))
