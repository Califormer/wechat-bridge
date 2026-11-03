#!/usr/bin/env python3
"""Minimal jq stand-in covering ONLY the expressions used by the bridge
hook scripts (old and new). Used by tests/test_hook.py when no real jq is
installed. Unknown programs fail loudly (exit 99)."""
import json, re, sys

args = sys.argv[1:]
flags, vars_, prog, files = set(), {}, None, []
i = 0
while i < len(args):
    a = args[i]
    if a == "--argjson":
        vars_[args[i + 1]] = json.loads(args[i + 2]); i += 3; continue
    if a.startswith("-") and len(a) > 1 and prog is None and a not in ("-"):
        flags.update(a[1:]); i += 1; continue
    if prog is None: prog = a
    else: files.append(a)
    i += 1
prog = " ".join(prog.split())

def load_inputs():
    if files:
        out = []
        for f in files:
            try: out.append(json.load(open(f, encoding="utf-8")))
            except Exception as e:
                sys.stderr.write(f"jq: error: {e}\n"); sys.exit(2)
        return out
    raw = sys.stdin.read()
    try: return [json.loads(raw)]
    except Exception as e:
        sys.stderr.write(f"jq: error: {e}\n"); sys.exit(2)

def emit(v):
    if "r" in flags and isinstance(v, str): print(v)
    elif "c" in flags: print(json.dumps(v, ensure_ascii=False, separators=(",", ":")))
    else: print(json.dumps(v, ensure_ascii=False, indent=2))

def rc_for(v):
    return 1 if ("e" in flags and (v is None or v is False)) else 0

def falsy_default(v, d): return d if v in (None, False) else v

def tostr(v): return v if isinstance(v, str) else json.dumps(v, ensure_ascii=False, separators=(",", ":"))

if "n" in flags:
    if prog == "$a - $b":
        v = [x for x in vars_["a"] if x not in vars_["b"]]
    elif prog == "$a + $n":
        v = vars_["a"] + vars_["n"]
    elif prog == "$a - ($a - $u)":
        v = [x for x in vars_["a"] if x in vars_["u"]]
    else: sys.stderr.write(f"jqshim: unsupported -n program: {prog}\n"); sys.exit(99)
    emit(v); sys.exit(0)

data = load_inputs()
if "s" in flags:
    docs = data
    m = re.search(r'select\( \.status == "pending_execution" or', prog)
    if prog.startswith("[ .[] | select(") and "pending_execution" in prog:
        now = vars_["now"]; out = []
        for t in docs:
            ex = (t.get("execution") or {}); ca = ex.get("claimed_at") or 0
            last = max(ca, ex.get("heartbeat_at") or 0) if "heartbeat_at" in prog else ca
            if t.get("status") == "pending_execution" or (t.get("status") == "executing" and ca > 0 and now - last > 600000):
                out.append(t["message_id"])
        emit(out); sys.exit(0)
    if prog == '[ .[] | select(.status == "send_uncertain" and ((.send.notified // false) | not)) | .message_id ]':
        emit([t["message_id"] for t in docs if t.get("status") == "send_uncertain"
              and not ((t.get("send") or {}).get("notified"))]); sys.exit(0)
    if prog == '[ .[] | select(.status == "send_uncertain") | .message_id ]':
        emit([t["message_id"] for t in docs if t.get("status") == "send_uncertain"]); sys.exit(0)
    sys.stderr.write(f"jqshim: unsupported -s program: {prog}\n"); sys.exit(99)

d = data[0]
if prog == ".":
    if len(data) > 1: sys.exit(rc_for(data[-1]))
    emit(d); sys.exit(rc_for(d))
if prog == 'type == "object"':
    v = isinstance(d, dict); emit(v); sys.exit(rc_for(v))
if prog == "length": emit(len(d)); sys.exit(0)
if prog == "sort": emit(sorted(d)); sys.exit(0)
if prog == ".alerted_uncertain // []": emit(falsy_default(d.get("alerted_uncertain"), [])); sys.exit(0)
m = re.fullmatch(r"\.(\w+) // 0", prog)
if m: emit(falsy_default(d.get(m.group(1)), 0)); sys.exit(0)
if prog.startswith("[(.last_poll_ok_at // 0)"):       # new merged heartbeat read
    vals = [falsy_default(d.get(k), 0) for k in ("last_poll_ok_at", "serve_started_at", "poll_fail_streak", "last_poll_fail_at")]
    print("\t".join(tostr(v) for v in vals)); sys.exit(0)
if prog.startswith("[((.last_task_wake_ids // []) | tostring)"):
    print(tostr(falsy_default(d.get("last_task_wake_ids"), [])) + "\t" + tostr(falsy_default(d.get("last_task_wake_ms"), 0))); sys.exit(0)
if re.fullmatch(r"\.\w+ = (\d+|\$\w+)( \| \.\w+ = (\d+|\$\w+))*", prog):
    for part in prog.split(" | "):
        k, v = re.fullmatch(r"\.(\w+) = (.+)", part).groups()
        d[k] = vars_[v[1:]] if v.startswith("$") else int(v)
    emit(d); sys.exit(0)
sys.stderr.write(f"jqshim: unsupported program: {prog}\n"); sys.exit(99)
