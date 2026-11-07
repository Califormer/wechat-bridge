#!/usr/bin/env python3
"""
Muse <-> WeChat ClawBot bridge, implementing Tencent's official
openclaw-weixin backend protocol directly (no OpenClaw runtime, no model).

Protocol source (verified 2026-10-27 against the live repo):
  - https://github.com/tencent/openclaw-weixin  (plugin version 2.4.10)
  - docs/protocol_zh_CN.md, src/api/api.ts, src/auth/login-qr.ts,
    src/monitor/monitor.ts, src/api/session-guard.ts, src/messaging/send.ts

Base URL : https://ilinkai.weixin.qq.com
QR login : POST /ilink/bot/get_bot_qrcode?bot_type=3
           GET  /ilink/bot/get_qrcode_status?qrcode=...
Updates  : POST /ilink/bot/getupdates     (long-poll, get_updates_buf cursor)
Send     : POST /ilink/bot/sendmessage
Lifecycle: POST /ilink/bot/msg/notifystart | notifystop

This program only transports messages and manages task state.
It never generates task answers itself. Tasks are claimed and executed
by a Muse agent (woken by a Muse hook), which calls `complete`
with the real result; this program then sends that result to WeChat.

Commands: login, wait-login, serve, send, send-image, send-file, send-video,
          send-media, flush, status, claim, complete, progress, history, memo,
          queue, stats, canary-dedupe, voice-to-wav, write-proxy-env, stop

v4 (2026-11-02): outbound media go through a persistent queue with
long-horizon background retries (a dropped CDN upload no longer needs the
user to say "resend"); `complete --attach`; voice bubbles (SILK, optional
codec) and inbound voice without WeChat's transcript; zero-LLM chat
commands (状态 / 重发 / 帮助); conversation history + memo handed to the
executor; explicit, bridge-written failure notices; send-result checks
that also honour `errcode`.

v5 (2026-11-02): the ClawBot channel's two hard limits are modelled - the
24 h reply window and the ~10-replies-per-user-message budget (both answered
with ret=-2, which v4 read as "rate limit") - with parking + resume on the
next user message; Markdown adapted to what the WeChat client renders and
fence/table-aware splitting in UTF-16 units (official textChunkLimit); quote
restoration by server id (official 2.4.9 quote cache); keep-alive pool also
without a proxy, in-process fast retries for undelivered sends, TCP keepalive,
DNS fallback, SIGHUP proxy reload, systemd watchdog; warm executors
(`claim --wait`), burst merging, 取消/进度/新话题/继续; Range-resumed
downloads and shrunk uploads.
"""
import argparse
import base64
import contextlib
import fcntl
import hashlib
import errno
import http.client
import importlib
import io
import itertools
import json
import os
import queue
import random
import re
import secrets
import select
import shutil
import signal
import socket
import sqlite3
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
TUNING_FILE = STATE_DIR / "tuning.env"


def _load_tuning() -> None:
    """state/tuning.env (2026-11-05): KEY=VALUE lines for WECHAT_BRIDGE_*
    knobs, read by serve and by every CLI call before the constants below
    are computed - tuning without touching the systemd unit or proxy.env
    (which write-proxy-env rewrites). Only WECHAT_BRIDGE_* names are taken;
    the process environment wins over the file."""
    try:
        lines = TUNING_FILE.read_text(encoding="utf-8").splitlines()
    except OSError:
        return
    for ln in lines:
        ln = ln.strip()
        if not ln or ln.startswith("#") or "=" not in ln:
            continue
        k, v = (x.strip() for x in ln.split("=", 1))
        if re.fullmatch(r"WECHAT_BRIDGE_[A-Z0-9_]+", k) and k not in os.environ:
            os.environ[k] = v.strip('"').strip("'")


_load_tuning()
INBOX_DIR = BASE_DIR / "inbox"
MEDIA_DIR = BASE_DIR / "inbox_media"
FILES_DIR = BASE_DIR / "inbox_files"
OUTBOX_DIR = BASE_DIR / "outbox"
LOG_DIR = BASE_DIR / "logs"
CREDS_FILE = STATE_DIR / "credentials.json"
CURSOR_FILE = STATE_DIR / "cursor.json"
SESSION_FILE = STATE_DIR / "session.json"
LOGIN_FILE = STATE_DIR / "login_session.json"
LOGIN_STATUS_FILE = STATE_DIR / "login_status.json"
HEARTBEAT_FILE = STATE_DIR / "heartbeat.json"
PID_FILE = STATE_DIR / "serve.pid"
LOCK_FILE = STATE_DIR / "tasks.lock"
QR_PNG = BASE_DIR / "qrcode.png"
QR_TXT = BASE_DIR / "qrcode.txt"
QUEUE_DIR = OUTBOX_DIR / "queue"        # v4: outbound media queue items
ATTACH_DIR = OUTBOX_DIR / "files"       # v4: private copies of attachments
HISTORY_DIR = BASE_DIR / "history"      # v4: conversation history (not backed up)
BRIDGE_VERSION = "5.0.5"

FIXED_BASE_URL = "https://ilinkai.weixin.qq.com"
DEFAULT_BASE_URL = FIXED_BASE_URL
BOT_TYPE = "3"
CHANNEL_VERSION = "2.4.10"          # matches current plugin package.json
ILINK_APP_ID = "bot"                # package.json ilink_appid
BOT_AGENT = "Muse-WeChatBridge/5.0"              # observability only, not auth
LONG_POLL_DEFAULT_MS = 35_000
# 2026-11-04 review: the server's longpolling_timeout_ms hint is clamped.
# Unbounded, a hint above ~175 s made one poll outlast WatchdogSec=180
# (the watchdog is pinged from the poll loop) and systemd killed serve.
LONG_POLL_MIN_MS = 1_000
LONG_POLL_MAX_MS = 60_000
STALE_ERRCODE = -14                 # session-guard.ts: pause 1 hour
SESSION_PAUSE_MS = 60 * 60 * 1000
MAX_CONSECUTIVE_FAILURES = 3
# Graduated poll-retry delays (seconds): getUpdates is idempotent, so
# retry fast and cap low - a long backoff only adds recovery latency
# when an upstream flap ends.
POLL_RETRY_DELAYS_SEC = (1, 2, 4, 8, 15)
# Business-error retry in receiver_loop (previously referenced but
# never defined, which would crash serve on a non--14 getUpdates error).
UNSUPPORTED_TEXT = ("这条消息的类型我暂时处理不了。目前支持：文字、图片、文件、视频，"
                    "以及微信能转成文字的语音。换成这些形式再发就行。")
VOICE_UNUSABLE_TEXT = ("这条语音微信没有给出文字转写，消息里也没有可下载的音频，我没法听。"
                       "请再说一遍（微信一般会自动转文字），或者直接打字。")

# Task statuses: received -> pending_execution -> executing ->
#                waiting_send -> sending -> sent | send_failed | send_uncertain
#                executing -> execution_failed (still reported to user)
#        (v5)    downloading/pending_execution/executing -> cancelled (「取消」)


def client_version_number() -> int:
    major, minor, patch = (int(p) for p in CHANNEL_VERSION.split("."))
    return ((major & 0xFF) << 16) | ((minor & 0xFF) << 8) | (patch & 0xFF)


def now_ms() -> int:
    return int(time.time() * 1000)


_LOG_TO_STDOUT = {"on": False}     # serve: stdout -> journald


def log(msg: str) -> None:
    """Log without tokens or message content. Rotates at ~5 MB. Only serve
    echoes to stdout; CLI commands echo to stderr, because their stdout is
    the JSON the executor parses (v3 mixed log lines into it whenever a CLI
    command sent something itself, e.g. complete without a running serve)."""
    line = f"{time.strftime('%Y-%m-%dT%H:%M:%S')} {msg}"
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = LOG_DIR / "bridge.log"
        try:
            if log_file.stat().st_size > 5_000_000:
                os.replace(log_file, LOG_DIR / "bridge.log.1")
        except OSError:
            pass
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        pass
    try:
        print(line, flush=True,
              file=sys.stdout if _LOG_TO_STDOUT["on"] else sys.stderr)
    except (OSError, ValueError):
        pass    # closed/broken stdout (journald restart, `| head`): never kill a thread


_tmp_seq = itertools.count()


def _fsync_dir(path: Path) -> None:
    """Make a rename durable (the directory entry itself). Best effort."""
    try:
        fd = os.open(str(path), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _atomic_write_bytes(path: Path, payload: bytes, mode: int = 0o600,
                        fsync: bool = True) -> None:
    """Write-then-rename with the tmp file private from its first byte.
    2026-11-04 review: v5.0.5 fixed the umask window for atomic_write_text
    only. atomic_write_json - credentials.json, token_history.json,
    proxy_pool.json (proxy passwords), every task file - still created its
    tmp file at the umask mode and chmodded it after the content was
    complete; with this host's umask the bot token sat in a 0664 file for
    that window (reproduced). Both now share this path: O_CREAT|O_EXCL|
    O_NOFOLLOW at 0600 (never follows or reuses an existing name), fchmod to
    the final mode before any byte is written, fsync, rename, and an fsync
    of the directory so the rename itself survives a power cut.
    The tmp name is unique per process+thread+call: the receiver and sender
    threads both rewrite heartbeat.json, and a shared tmp name once crashed
    os.replace with FileNotFoundError (which killed serve)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(
        path.suffix + f".tmp{os.getpid()}.{threading.get_ident()}.{next(_tmp_seq)}")
    flags = (os.O_WRONLY | os.O_CREAT | os.O_EXCL
             | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0))
    fd = os.open(tmp, flags, 0o600)
    try:
        try:
            os.fchmod(fd, mode)
            view = memoryview(payload)
            while view:
                view = view[os.write(fd, view):]
            if fsync:
                os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise
    if fsync:
        _fsync_dir(path.parent)


def atomic_write_json(path: Path, data, mode=0o600, fsync: bool = True) -> None:
    # backslashreplace: a lone surrogate (non-UTF-8 argv, a "\ud800" escape
    # in a server JSON) is written as the JSON escape it came from, so the
    # file stays valid UTF-8 and json.load returns the same string.
    _atomic_write_bytes(
        path, json.dumps(data, ensure_ascii=False, indent=2)
        .encode("utf-8", "backslashreplace"), mode, fsync)


def atomic_write_text(path: Path, text: str, mode=0o600,
                      fsync: bool = True) -> None:
    """atomic_write_json for plain text (e.g. DECISIONS.md)."""
    _atomic_write_bytes(path, text.encode("utf-8", "replace"), mode, fsync)


def read_json(path: Path, default=None):
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return default


_task_lock_local = threading.local()


@contextlib.contextmanager
def task_lock():
    """Cross-process task lock (flock). Re-entrant within a thread: v4 code
    paths (notices, resend, claim) nest it, and a second flock() on a fresh
    descriptor from the same thread would block forever. Other threads and
    processes still exclude each other."""
    depth = getattr(_task_lock_local, "depth", 0)
    if depth:
        _task_lock_local.depth = depth + 1
        try:
            yield
        finally:
            _task_lock_local.depth -= 1
        return
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    with open(LOCK_FILE, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        _task_lock_local.depth = 1
        try:
            yield
        finally:
            _task_lock_local.depth = 0
            fcntl.flock(f, fcntl.LOCK_UN)


def mask(value: str | None) -> str:
    if not value:
        return "(none)"
    if len(value) <= 10:
        return "***"
    return f"{value[:6]}...{value[-2:]}"


# --------------------------------------------------------------------------
# 2026-11-04 review: safe on-disk names, private files, one-line text
# --------------------------------------------------------------------------
_SAFE_NAME_RE = re.compile(r"[0-9A-Za-z_-]{1,64}")


def safe_name(value) -> str:
    """Filesystem-safe stem for an id that came from the network or a CLI.
    message_id is server-controlled: task_path() always sanitized it, but the
    media paths (inbox_media/<id>.jpg, inbox_files/<id>__name and their
    .part/.plain siblings) used it raw, so an id like "../../x" wrote outside
    the bridge directory - as root (reproduced in the review). Plain ids (all
    real iLink ids, notice-/proactive-/q ids) map to themselves, so no file
    name changes; anything else becomes an ASCII prefix plus a hash, which
    also ends the old task_path() collision of "a.b" and "a_b"."""
    s = str(value)
    if _SAFE_NAME_RE.fullmatch(s):
        return s
    head = re.sub(r"[^0-9A-Za-z_-]+", "_", s)[:40].strip("_") or "id"
    digest = hashlib.sha256(s.encode("utf-8", "surrogatepass")).hexdigest()[:12]
    return f"{head}-{digest}"


def _legacy_safe(value) -> str:
    """task_path()'s mapping up to v5.0.5 - only used to find old files."""
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in str(value))


@contextlib.contextmanager
def _flock(path: Path):
    """Exclusive advisory lock on `path` (created if missing)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _private_umask() -> int:
    raw = (os.environ.get("WECHAT_BRIDGE_UMASK") or "077").strip()
    try:
        return int(raw, 8) & 0o777
    except ValueError:
        return 0o077


def apply_private_umask() -> int:
    """Files and directories this process creates are private by default
    (WECHAT_BRIDGE_UMASK, octal, default 077). The service runs with
    systemd's 022 and this host's shells with 002/007, so inbound photos and
    files, logs, QR codes, lock files and every tmp file used to start out
    readable by others until a later chmod (if there was one)."""
    m = _private_umask()
    os.umask(m)
    return m


def harden_dirs(dirs) -> None:
    """Drop the group/other bits the current umask does not allow from
    existing bridge directories (never adds a bit)."""
    cur = os.umask(0)
    os.umask(cur)
    allowed = 0o777 & ~cur
    for d in dirs:
        with contextlib.suppress(OSError):
            mode = os.stat(d).st_mode & 0o777
            if mode & ~allowed:
                os.chmod(d, mode & allowed)


_LINEBREAKS_RE = re.compile("[\r\n\v\f\x1c\x1d\x1e\x85\u2028\u2029]+")


def one_line(text) -> str:
    """Everything a Markdown renderer, str.splitlines() or an LLM may read as
    a line break becomes a space; control and format characters (bidi
    overrides included, ZWJ kept for emoji) are dropped. v5.0.5's
    decisions --add only replaced \\r and \\n."""
    s = _LINEBREAKS_RE.sub(" ", str(text or ""))
    return "".join(ch for ch in s
                   if ch in "\t\u200d"
                   or (ord(ch) >= 32 and not 0x7f <= ord(ch) < 0xa0
                       and unicodedata.category(ch) != "Cf")).strip()


# --------------------------------------------------------------------------
# HTTP layer (faithful to src/api/api.ts)
# --------------------------------------------------------------------------

def random_wechat_uin() -> str:
    uint32 = int.from_bytes(secrets.token_bytes(4), "big")
    return base64.b64encode(str(uint32).encode()).decode()


def common_headers() -> dict:
    return {
        "iLink-App-Id": ILINK_APP_ID,
        "iLink-App-ClientVersion": str(client_version_number()),
    }


def post_headers(token: str | None = None, with_auth_type: bool = True) -> dict:
    h = {"Content-Type": "application/json", **common_headers()}
    if with_auth_type:
        h["AuthorizationType"] = "ilink_bot_token"
        h["X-WECHAT-UIN"] = random_wechat_uin()
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _sanitize_bot_agent(raw: str | None) -> str:
    """UA-style `Name/Version (comment)` tokens, ASCII only, at most 256
    bytes (official sanitizeBotAgent); "OpenClaw" when nothing valid is left.
    Observability only: the backend attributes traffic by it."""
    tok_re = re.compile(r"[A-Za-z0-9._+!#$%&'*^`|~-]+/[A-Za-z0-9._+!#$%&'*^`|~-]+"
                        r"(?: \([\x20-\x27\x2a-\x5b\x5d-\x7e]{0,120}\))?")
    out = ""
    for tok in tok_re.findall(raw or ""):
        cand = (out + " " + tok).strip()
        if len(cand) > 256:
            break
        out = cand
    return out or "OpenClaw"


def bot_agent() -> str:
    """v5: `Muse-WeChatBridge/5.0 (env=prod;transport=proxy)` - lets the
    backend tell this bridge's traffic apart (official 2.3.1 botAgent).
    WECHAT_BRIDGE_BOT_AGENT overrides it."""
    raw = os.environ.get("WECHAT_BRIDGE_BOT_AGENT") or (
        f"{BOT_AGENT} (env={os.environ.get('WECHAT_BRIDGE_ENV', 'prod')};"
        f"transport={getattr(PROXY_POOL, 'mode', 'urllib')})")
    return _sanitize_bot_agent(raw)


def base_info() -> dict:
    return {"channel_version": CHANNEL_VERSION, "bot_agent": bot_agent()}


class ApiError(Exception):
    def __init__(self, kind: str, message: str, code=None,
                 errmsg: str | None = None):
        super().__init__(message)
        # http | connect | network | timeout | business | config | local
        #   config  : refused locally before sending (e.g. a baseurl
        #             outside the allowlist) - never retried as a blip
        #   connect : failed before the request left this host (DNS, TCP,
        #             proxy CONNECT, TLS, writing it) -> never received
        #   network : failed after the request was (possibly) sent
        #   timeout : sent, but no response in time
        self.kind = kind
        self.code = code        # business ret/errcode when known
        self.errmsg = errmsg or ""   # v5: server text, used to classify -2


# --------------------------------------------------------------------------
# Transport: keep-alive connections, split timeouts, egress proxy failover
# --------------------------------------------------------------------------
# urllib opened a new connection for every request (it even sends
# "Connection: close"): TCP to the proxy, CONNECT, then a TLS handshake -
# several cross-border round trips in front of every long poll, every reply
# part and every presence/typing call. The community iLink clients all reuse
# connections (Go net/http in lib-x/ilink, reqwest in ilink-hub and
# wechat-ilink, the shared httpx client in wechat-clawbot); KeepAliveOpener
# does the same with the standard library only.
#
# Timeouts are split: connecting (TCP + CONNECT + TLS) gets
# CONNECT_TIMEOUT_SEC, the response gets the caller's timeout. urllib used one
# 45 s timeout for both, and a connect-phase timeout came back as "timeout",
# which get_updates treats as a normal empty long poll: a black-holed proxy
# looked like a healthy, quiet channel (the heartbeat kept advancing and the
# hook never noticed). Failures before the request left this host are now
# ConnectPhaseError -> ApiError kind "connect", which also tells the send
# path that the server cannot have received the message.

CONNECT_TIMEOUT_SEC = 12
POOL_MAX_IDLE_PER_HOST = 4
# Idle connections are reused only while this fresh. Idempotent requests are
# retried once on a new connection if a reused one turns out to be dead;
# sendmessage is not, so it only takes very warm connections.
IDLE_REUSE_MAX_SEC = 50
IDLE_REUSE_MAX_SEC_UNSAFE = 8
UPLOAD_BLOCKSIZE = 64 * 1024
_DEFAULT_UA = f"Python-urllib/{sys.version_info[0]}.{sys.version_info[1]}"
_NET_STATS = {"connects": 0, "reused": 0, "stale_retries": 0,
              "fast_retries": 0, "resumed": 0, "dns_fallbacks": 0}


class ConnectPhaseError(urllib.error.URLError):
    """The request never left this host (DNS, TCP, proxy CONNECT, TLS, or
    writing the request failed): the server cannot have processed it."""


def _ssl_context() -> ssl.SSLContext:
    return ssl.create_default_context()


def _conn_dropped(conn) -> bool:
    """True if an idle keep-alive connection is unusable: the peer closed it
    (EOF) or sent bytes nobody asked for."""
    sock = getattr(conn, "sock", None)
    if sock is None:
        return True
    try:
        readable, _, _ = select.select([sock], [], [], 0)
    except (OSError, ValueError):
        return True
    if not readable:
        return False
    prev = sock.gettimeout()
    try:
        sock.settimeout(0.0)
        sock.recv(1)
        return True
    except (ssl.SSLWantReadError, BlockingIOError):
        return False            # only TLS housekeeping (session tickets)
    except OSError:
        return True
    finally:
        with contextlib.suppress(OSError):
            sock.settimeout(prev)



# v5: socket tuning and a DNS fallback for every new connection. TCP
# keepalive turns a silently dead path (a proxy or NAT that dropped the flow
# during a long poll) into an error in ~35 s instead of the read timeout;
# TCP_NODELAY avoids Nagle delays on the small JSON requests. The resolver
# remembers the last good addresses per host and uses them when DNS fails
# (direct mode: the iLink/CDN hosts; proxy mode: the proxy itself).
TCP_KEEPIDLE_SEC, TCP_KEEPINTVL_SEC, TCP_KEEPCNT = 20, 5, 3
DNS_FALLBACK_MAX_AGE_SEC = 6 * 3600
_DNS_CACHE: dict = {}
_dns_lock = threading.Lock()


def _tune_socket(sock) -> None:
    opts = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)]
    for name, val in (("TCP_KEEPIDLE", TCP_KEEPIDLE_SEC),
                      ("TCP_KEEPINTVL", TCP_KEEPINTVL_SEC),
                      ("TCP_KEEPCNT", TCP_KEEPCNT)):
        if hasattr(socket, name):
            opts.append((socket.IPPROTO_TCP, getattr(socket, name), val))
    for level, opt, val in opts:
        with contextlib.suppress(OSError, ValueError):
            sock.setsockopt(level, opt, val)


def _create_connection_cached(address, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
                              source_address=None, *args, **kwargs):
    """socket.create_connection + keepalive tuning + last-good-address
    fallback when name resolution fails."""
    host, port = address[0], address[1]
    try:
        sock = socket.create_connection(address, timeout, source_address)
    except socket.gaierror:
        with _dns_lock:
            hit = _DNS_CACHE.get((host, port))
        if not hit or time.time() - hit[1] > DNS_FALLBACK_MAX_AGE_SEC:
            raise
        sock, last = None, None
        for ip in hit[0]:
            try:
                sock = socket.create_connection((ip, port), timeout,
                                                source_address)
                break
            except OSError as e:
                last = e
        if sock is None:
            raise last or OSError("no cached address reachable")
        _NET_STATS["dns_fallbacks"] += 1
    else:
        with contextlib.suppress(OSError, IndexError):
            peer = sock.getpeername()[0]
            with _dns_lock:
                old = (_DNS_CACHE.get((host, port)) or ([], 0))[0]
                _DNS_CACHE[(host, port)] = (
                    [peer] + [x for x in old if x != peer][:3], time.time())
    _tune_socket(sock)
    return sock


class _PooledResponse:
    """urlopen-style response. The connection goes back to the pool once the
    body has been read completely; otherwise it is closed."""

    def __init__(self, resp, conn, owner, key, url):
        self._resp, self._conn, self._owner, self._key = resp, conn, owner, key
        self.status = self.code = resp.status
        self.reason = resp.reason
        self.headers = self.msg = resp.headers
        self.url = url

    def read(self, amt=None):
        return self._resp.read() if amt is None else self._resp.read(amt)

    def getheader(self, name, default=None):
        return self._resp.getheader(name, default)

    def getcode(self):
        return self.status

    def close(self):
        conn, self._conn = self._conn, None
        if conn is None:
            return
        resp = self._resp
        if not resp.isclosed() and resp.length == 0:
            with contextlib.suppress(Exception):
                resp.read()
        if resp.isclosed() and not resp.will_close:
            self._owner._release(self._key, conn)
        else:
            with contextlib.suppress(Exception):
                resp.close()
            conn.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


class KeepAliveOpener:
    """`.open(req, timeout)` with urlopen semantics that keeps connections
    alive, through one HTTP proxy (CONNECT for https, absolute-form for
    http) or directly. A connection serves one request at a time."""

    def __init__(self, proxy_url: str | None = None):
        self.proxy_host = self.proxy_port = self.proxy_auth = None
        if proxy_url:
            p = urllib.parse.urlsplit(proxy_url)
            self.proxy_host = p.hostname
            self.proxy_port = p.port or 80
            if p.username is not None:
                raw = (urllib.parse.unquote(p.username) + ":"
                       + urllib.parse.unquote(p.password or ""))
                self.proxy_auth = "Basic " + base64.b64encode(
                    raw.encode()).decode()
        self._idle: dict = {}
        self._lock = threading.Lock()

    def _acquire(self, key, max_idle):
        now = time.monotonic()
        with self._lock:
            idle = self._idle.get(key) or []
            while idle:
                conn, since = idle.pop()
                if now - since <= max_idle and not _conn_dropped(conn):
                    return conn
                conn.close()
        return None

    def _release(self, key, conn) -> None:
        with self._lock:
            idle = self._idle.setdefault(key, [])
            idle.append((conn, time.monotonic()))
            while len(idle) > POOL_MAX_IDLE_PER_HOST:
                idle.pop(0)[0].close()

    def close_idle(self) -> None:
        with self._lock:
            conns = [c for idle in self._idle.values() for c, _ in idle]
            self._idle.clear()
        for c in conns:
            with contextlib.suppress(Exception):
                c.close()

    def _new_conn(self, scheme, host, port, direct):
        if scheme == "https":
            if direct:
                conn = http.client.HTTPSConnection(host, port,
                                                   context=_ssl_context())
            else:
                conn = http.client.HTTPSConnection(
                    self.proxy_host, self.proxy_port, context=_ssl_context())
                conn.set_tunnel(host, port, headers=(
                    {"Proxy-Authorization": self.proxy_auth}
                    if self.proxy_auth else None))
        elif direct:
            conn = http.client.HTTPConnection(host, port)
        else:
            conn = http.client.HTTPConnection(self.proxy_host, self.proxy_port)
        conn.blocksize = UPLOAD_BLOCKSIZE
        # v5: keepalive/NODELAY + DNS fallback on every new socket
        conn._create_connection = _create_connection_cached
        return conn

    def open(self, req, timeout=None):
        timeout = timeout or 60
        url = req.full_url
        parts = urllib.parse.urlsplit(url)
        scheme = (parts.scheme or "").lower()
        if scheme not in ("http", "https"):
            raise urllib.error.URLError(f"unsupported scheme: {scheme}")
        host = parts.hostname or ""
        port = parts.port or (443 if scheme == "https" else 80)
        direct = (not self.proxy_host
                  or bool(urllib.request.proxy_bypass(host)))
        if scheme == "http" and not direct:
            target = url                     # absolute-form, forward proxy
        else:
            target = (parts.path or "/") + (
                f"?{parts.query}" if parts.query else "")
        method = req.get_method()
        data = req.data
        headers = dict(req.header_items())
        present = {k.lower() for k in headers}
        if "user-agent" not in present:
            headers["User-Agent"] = _DEFAULT_UA
        if data is not None and "content-type" not in present:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        if scheme == "http" and not direct and self.proxy_auth:
            headers["Proxy-Authorization"] = self.proxy_auth
        idempotent = getattr(req, "idempotent", method in ("GET", "HEAD"))
        key = (scheme, host, port, direct)
        max_idle = (IDLE_REUSE_MAX_SEC if idempotent
                    else IDLE_REUSE_MAX_SEC_UNSAFE)
        for attempt in (0, 1):
            conn = self._acquire(key, max_idle) if attempt == 0 else None
            reused = conn is not None
            if attempt and hasattr(data, "seek"):
                data.seek(0)
            if conn is None:
                conn = self._new_conn(scheme, host, port, direct)
                conn.timeout = min(CONNECT_TIMEOUT_SEC, timeout)
                try:
                    conn.connect()
                except (OSError, http.client.HTTPException) as e:
                    conn.close()
                    raise ConnectPhaseError(e) from e
                _NET_STATS["connects"] += 1
            else:
                _NET_STATS["reused"] += 1
            try:
                conn.sock.settimeout(timeout)
                conn.request(method, target, body=data, headers=headers)
            except (OSError, http.client.HTTPException) as e:
                conn.close()
                if reused and attempt == 0:
                    # Writing failed, so the request cannot have been
                    # processed: even sendmessage may go again, fresh.
                    _NET_STATS["stale_retries"] += 1
                    continue
                raise ConnectPhaseError(e) from e
            try:
                resp = conn.getresponse()
            except (ConnectionResetError, BrokenPipeError,
                    ConnectionAbortedError):
                # incl. RemoteDisconnected: a reused connection the peer had
                # already dropped. Only idempotent requests may retry.
                conn.close()
                if reused and attempt == 0 and idempotent:
                    _NET_STATS["stale_retries"] += 1
                    continue
                raise
            except BaseException:
                conn.close()
                raise
            break
        pooled = _PooledResponse(resp, conn, self, key, url)
        if (resp.status in (301, 302, 303, 307, 308)
                and method in ("GET", "HEAD")):
            location = resp.getheader("Location")
            hops = getattr(req, "_redirects", 0)
            with contextlib.suppress(Exception):
                pooled.read(64 * 1024)      # bounded: a bigger body just
            pooled.close()                  # costs the connection, not RAM
            if location and hops < 4:
                new_url = urllib.parse.urljoin(url, location)
                old_parts = urllib.parse.urlsplit(url)
                new_parts = urllib.parse.urlsplit(new_url)
                if old_parts.scheme == "https" and new_parts.scheme == "http":
                    raise urllib.error.URLError(
                        "refusing https->http redirect downgrade")
                headers = dict(req.header_items())
                old_origin = (old_parts.scheme, old_parts.hostname,
                              old_parts.port)
                new_origin = (new_parts.scheme, new_parts.hostname,
                              new_parts.port)
                if old_origin != new_origin:
                    # Never leak caller credentials to another origin, and
                    # never let an open redirect on a trusted host carry
                    # the request to an arbitrary https host.
                    _validate_server_url(new_url, "redirect")
                    for k in list(headers):
                        if k.lower() in ("authorization", "cookie",
                                         "proxy-authorization"):
                            del headers[k]
                nxt = urllib.request.Request(
                    new_url, headers=headers, method=method)
                nxt._redirects = hops + 1
                nxt.idempotent = True
                return self.open(nxt, timeout)
        if resp.status >= 300:
            try:
                body = pooled.read(1 << 20)
            except Exception:  # noqa: BLE001
                body = b""
            pooled.close()
            raise urllib.error.HTTPError(url, resp.status, resp.reason,
                                         resp.headers, io.BytesIO(body))
        return pooled


# Egress proxy pool: fail over between candidate proxies. The active proxy
# is the one from the environment (state/proxy.env via systemd), exactly as
# before. After PROXY_ROTATE_AFTER consecutive transport failures a
# background thread probes the alternatives and switches only if one of them
# can actually reach the WeChat endpoint, so an upstream blip that every
# proxy shares does not cause flapping. Alternatives, in probe order:
#   1. the current value in state/proxy.env (the hook refreshes it)
#   2. state/proxy-candidates.txt (optional, one URL per line)
#   3. state/proxy_pool.json (recently used proxies, newest first)
# Proxy URLs can carry credentials: they are never logged (only a short
# hash), and both state files are 0600 and excluded from backups.

PROXY_POOL_FILE = STATE_DIR / "proxy_pool.json"
PROXY_CANDIDATES_FILE = STATE_DIR / "proxy-candidates.txt"
PROXY_ROTATE_AFTER = 3
PROXY_ROTATE_COOLDOWN_SEC = 30
PROXY_PROBE_TIMEOUT_SEC = 6
PROXY_PROBE_MAX = 3
PROXY_HISTORY_MAX = 4
_URL_RE = re.compile(r"\w+://\S+")
_IP_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}(?::\d+)?\b")


def _proxy_id(url: str | None) -> str:
    if not url:
        return "-"          # v5: direct mode
    return hashlib.sha256(url.encode()).hexdigest()[:6]


def _proxy_url_ok(url: str) -> bool:
    """Only plaintext http:// CONNECT proxies are supported by the pool.
    An https:// proxy would need a TLS handshake to the proxy itself,
    which the pool does not implement, and socks proxies are not spoken
    at all, so both are rejected here instead of failing confusingly
    later. Callers that cannot use the pool fall back to urllib."""
    try:
        parts = urllib.parse.urlsplit(url)
        return parts.scheme == "http" and bool(parts.hostname)
    except ValueError:
        return False


def _scrub(text) -> str:
    """Exception text made safe for the log: no URLs/IPs, bounded length."""
    return _IP_RE.sub("<ip>", _URL_RE.sub("<url>", str(text)))[:100]


def net_detail(e: BaseException) -> str:
    """Layer of a transport failure, like upstream classifyFetchError
    (dns / tcp / tls / timeout) plus proxy-specific classes, URL-free."""
    r = getattr(e, "reason", None)
    inner = r if isinstance(r, BaseException) else e
    if isinstance(inner, socket.gaierror):
        cls = "dns"
    elif isinstance(inner, ConnectionRefusedError):
        cls = "refused"
    elif isinstance(inner, TimeoutError):
        cls = "timeout"
    elif isinstance(inner, ssl.SSLError):
        cls = "tls"
    elif isinstance(inner, http.client.RemoteDisconnected):
        cls = "closed"
    elif isinstance(inner, (ConnectionResetError, BrokenPipeError,
                            ConnectionAbortedError)):
        cls = "reset"
    elif isinstance(inner, OSError) and "Tunnel connection failed" in str(inner):
        cls = "tunnel"
    elif isinstance(inner, OSError) and inner.errno in (errno.ENETUNREACH,
                                                        errno.EHOSTUNREACH):
        cls = "unreachable"
    elif isinstance(inner, http.client.HTTPException):
        cls = "protocol"
    else:
        cls = "other"
    return f"{cls}({type(inner).__name__}: {_scrub(inner)})"


def _https_proxy_from(mapping) -> str | None:
    for name in ("https_proxy", "HTTPS_PROXY"):
        value = (mapping.get(name) or "").strip()
        if value:
            return value
    return None


def _https_proxy_from_env_file() -> str | None:
    try:
        text = PROXY_ENV_FILE.read_text(encoding="utf-8")  # defined below
    except OSError:
        return None
    values = {}
    for line in text.splitlines():
        key, sep, value = line.strip().partition("=")
        if sep and not key.startswith("#"):
            values[key.strip()] = value.strip().strip("'\"")
    return _https_proxy_from(values)


class _SafeRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Mirror the pool's redirect rules for the urllib fallback path
    (used when the pool is disabled, e.g. https/socks proxies). The
    stdlib handler copies every header cross-origin, so without this
    an API Bearer token would leak to the redirect target. Validation
    failures surface as MediaError (exactly like the pool path), so
    callers treat a refused redirect as deterministic instead of
    retrying it as a transient network error. The https->http
    downgrade refusal stays URLError in both paths."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if req.get_method() not in ("GET", "HEAD"):
            # Mirror the pool path, which only follows redirects for
            # GET/HEAD and raises HTTPError for anything else: the stdlib
            # handler would otherwise silently replay a POST as a GET at
            # the new address, so a redirected sendmessage would look
            # successful while its body never arrived.
            raise urllib.error.HTTPError(req.full_url, code, msg,
                                         headers, fp)
        old = urllib.parse.urlsplit(req.full_url)
        new = urllib.parse.urlsplit(urllib.parse.urljoin(req.full_url, newurl))
        if old.scheme == "https" and new.scheme == "http":
            raise urllib.error.URLError(
                "refusing https->http redirect downgrade")
        old_origin = (old.scheme, old.hostname, old.port)
        new_origin = (new.scheme, new.hostname, new.port)
        if old_origin != new_origin:
            _validate_server_url(
                urllib.parse.urljoin(req.full_url, newurl), "redirect")
            stripped = {k: req.headers.pop(k) for k in list(req.headers)
                        if k.lower() in ("authorization", "cookie",
                                         "proxy-authorization")}
            try:
                return super().redirect_request(req, fp, code, msg,
                                                headers, newurl)
            finally:
                req.headers.update(stripped)
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class ProxyPool:
    def __init__(self) -> None:
        self.enabled = False
        self.active: str | None = None
        self.rotations = 0
        self._opener = None
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._streak = 0
        self._next_rotation_at = 0.0
        self._warned_no_alternatives = False
        self.mode = "urllib"          # v5: proxy | direct | urllib
        self._reload = threading.Event()

    # -- setup ------------------------------------------------------------
    def init(self) -> None:
        """Adopt the environment's https proxy. v5: WITHOUT one the pool
        still runs, with direct keep-alive connections - v4 fell back to
        plain urlopen (a fresh TCP+TLS handshake for every request), and the
        systemd unit treats proxy.env as optional, so a missing file silently
        cost every call a handshake. Only a proxy scheme the pool cannot
        speak (https, socks) is left to urllib, with a clear log line:
        urllib speaks plaintext CONNECT only, so such a proxy will fail
        there too unless it happens to accept plaintext CONNECT."""
        url = _https_proxy_from(os.environ)
        if url and _proxy_url_ok(url):
            self._activate(url)
            self.enabled, self.mode = True, "proxy"
        elif url:
            log("proxy scheme not supported by the pool (only http:// "
                "CONNECT proxies): pool disabled, urllib fallback")
            self.enabled, self.mode = False, "urllib"
        else:
            self._opener = KeepAliveOpener(None)
            self.active = None
            self.enabled, self.mode = True, "direct"

    def start_rotator(self) -> None:
        if not self.enabled:
            return
        self._remember(self.active)
        update_heartbeat(proxy_id=_proxy_id(self.active) if self.active else None,
                         proxy_rotations=0, transport_mode=self.mode)
        log(f"proxy pool ready: mode={self.mode} active={_proxy_id(self.active)} "
            f"alternatives={len(self._candidates())}")
        threading.Thread(target=self._rotator_loop, name="proxy-rotator",
                         daemon=True).start()

    def publish_stats(self) -> None:
        update_heartbeat(net_connects=_NET_STATS["connects"],
                         net_reused=_NET_STATS["reused"],
                         net_stale_retries=_NET_STATS["stale_retries"],
                         net_fast_retries=_NET_STATS["fast_retries"],
                         net_resumed=_NET_STATS["resumed"],
                         net_dns_fallbacks=_NET_STATS["dns_fallbacks"],
                         transport_mode=self.mode,
                         typing_sent=TYPING.sent,
                         threads_age_s=_thread_ages())

    # -- request path -----------------------------------------------------
    def open(self, req, timeout):
        """Drop-in for urllib.request.urlopen with failure accounting."""
        if not self.enabled:
            opener = urllib.request.build_opener(_SafeRedirectHandler)
            return opener.open(req, timeout=timeout)
        opener = self._opener
        try:
            resp = opener.open(req, timeout=timeout)
        except urllib.error.HTTPError:
            self._note_success()  # the server answered through this proxy
            raise
        except urllib.error.URLError:
            self._note_failure()  # connect/tunnel phase, incl. its timeouts
            raise
        except TimeoutError:
            raise  # read-phase timeout: a normal empty long poll
        except (OSError, http.client.HTTPException):
            self._note_failure()  # e.g. RemoteDisconnected mid-request
            raise
        self._note_success()
        return resp

    def _note_success(self) -> None:
        self._streak = 0

    def _note_failure(self) -> None:
        with self._lock:
            self._streak += 1
            due = (self._streak >= PROXY_ROTATE_AFTER
                   and time.time() >= self._next_rotation_at)
        if due:
            self._wake.set()

    def note_transport_failure(self) -> None:
        """A failure the opener cannot see itself (2026-11-04 review: long
        polls that get no answer at all count towards failover)."""
        self._note_failure()

    def close_idle(self) -> None:
        opener = self._opener
        if opener is not None and hasattr(opener, "close_idle"):
            opener.close_idle()

    # -- failover ---------------------------------------------------------
    def _activate(self, url: str) -> None:
        old = self._opener
        self._opener = KeepAliveOpener(url)
        self.active = url
        if old is not None and hasattr(old, "close_idle"):
            old.close_idle()

    def _history(self) -> list[str]:
        data = read_json(PROXY_POOL_FILE, {}) or {}
        return [u for u in (data.get("urls") or [])
                if isinstance(u, str) and _proxy_url_ok(u)]

    def _remember(self, url: str | None) -> None:
        if not url:
            return
        urls = [url] + [u for u in self._history() if u != url]
        try:
            atomic_write_json(PROXY_POOL_FILE,
                              {"urls": urls[:PROXY_HISTORY_MAX]})
        except OSError as e:
            log(f"proxy pool: could not save history: {type(e).__name__}")

    def _candidates(self) -> list[str]:
        sources = [_https_proxy_from_env_file()]
        try:
            for line in PROXY_CANDIDATES_FILE.read_text(
                    encoding="utf-8").splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    sources.append(line)
        except OSError:
            pass
        sources += self._history()
        out: list[str] = []
        for url in sources:
            if url and _proxy_url_ok(url) and url != self.active \
                    and url not in out:
                out.append(url)
        return out

    def _probe(self, url: str) -> bool:
        """Can this proxy reach the WeChat endpoint right now? Any HTTP
        answer counts; only transport failures do not."""
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": url, "https": url}))
        req = urllib.request.Request(FIXED_BASE_URL + "/", method="HEAD")
        try:
            with opener.open(req, timeout=PROXY_PROBE_TIMEOUT_SEC):
                return True
        except urllib.error.HTTPError:
            return True
        except (urllib.error.URLError, OSError, http.client.HTTPException):
            return False

    def _rotate_once(self) -> None:
        self._next_rotation_at = time.time() + PROXY_ROTATE_COOLDOWN_SEC
        candidates = self._candidates()
        if not candidates:
            if not self._warned_no_alternatives:
                self._warned_no_alternatives = True
                log("proxy failover: no alternative proxy configured")
            return
        for url in candidates[:PROXY_PROBE_MAX]:
            if _stop.is_set():
                return
            if not self._probe(url):
                continue
            old = self.active
            self._activate(url)
            self._remember(url)
            self.rotations += 1
            self._streak = 0
            self._next_rotation_at = time.time() + PROXY_ROTATE_COOLDOWN_SEC
            log(f"proxy failover: {_proxy_id(old)} -> {_proxy_id(url)} "
                f"(rotations={self.rotations})")
            update_heartbeat(proxy_id=_proxy_id(url),
                             proxy_rotations=self.rotations)
            return
        log(f"proxy failover: probed {min(len(candidates), PROXY_PROBE_MAX)} "
            f"alternative(s), none reachable; staying on "
            f"{_proxy_id(self.active)}")

    def request_reload(self) -> None:
        """SIGHUP handler: re-read state/proxy.env in the rotator thread."""
        self._reload.set()
        self._wake.set()

    def reload_from_env_file(self) -> bool:
        """v5: switch to the proxy now in state/proxy.env without a restart
        (threads, queue and the session survive; a restart costs 10-25 s of
        first-poll latency). Unchanged file: drop idle sockets only."""
        url = _https_proxy_from_env_file()
        if url and _proxy_url_ok(url) and url != self.active:
            old = self.active
            self._activate(url)
            self.enabled, self.mode = True, "proxy"
            self._remember(url)
            self._streak = 0
            self.rotations += 1
            log(f"proxy reload (SIGHUP): {_proxy_id(old)} -> {_proxy_id(url)}")
            update_heartbeat(proxy_id=_proxy_id(url), proxy_rotations=self.rotations,
                             transport_mode=self.mode)
            return True
        if self._opener is not None and hasattr(self._opener, "close_idle"):
            self._opener.close_idle()
        log("proxy reload (SIGHUP): proxy.env unchanged; idle connections dropped")
        return False

    def _rotator_loop(self) -> None:
        while not _stop.is_set():
            if not self._wake.wait(timeout=1.0):
                continue
            self._wake.clear()
            try:
                if self._reload.is_set():
                    self._reload.clear()
                    self.reload_from_env_file()
                    continue
                self._rotate_once()
            except Exception as e:  # noqa: BLE001 - never kill the thread
                log(f"proxy rotator error: {type(e).__name__}")


PROXY_POOL = ProxyPool()


def http_post(base_url: str, endpoint: str, body: dict | None,
              headers: dict, timeout: float) -> dict:
    url = base_url.rstrip("/") + "/" + endpoint
    # 2026-11-04 review: raw UTF-8 and compact separators, like the official
    # client's JSON.stringify. The json default (ensure_ascii) sent every CJK
    # character as a 6-byte \uXXXX escape: a 4000-unit Chinese part was 24 KB
    # instead of 12 KB (measured). backslashreplace keeps a lone surrogate as
    # the same JSON escape ensure_ascii produced for it.
    data = (json.dumps(body, ensure_ascii=False, separators=(",", ":"))
            .encode("utf-8", "backslashreplace") if body is not None else b"")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    # Only sendmessage must never be repeated blindly; everything else may be
    # retried on a fresh connection if a reused keep-alive one was stale.
    req.idempotent = not endpoint.startswith("ilink/bot/sendmessage")
    try:
        with PROXY_POOL.open(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        raise ApiError("http", f"HTTP {e.code}") from e
    except TimeoutError as e:
        raise ApiError("timeout", "request timeout") from e
    except urllib.error.URLError as e:
        # urllib wraps everything up to and including writing the request
        # (ConnectPhaseError is the keep-alive opener's equivalent): the
        # request never reached the server. This used to map a connect
        # timeout to "timeout", which get_updates reports as a healthy
        # empty poll - a black-holed proxy then looked like a quiet channel.
        raise ApiError("connect", net_detail(e)) from e
    except (ConnectionError, http.client.HTTPException, OSError) as e:
        # e.g. http.client.RemoteDisconnected when the peer/proxy closes a
        # long-held connection: a transient network failure, never fatal.
        raise ApiError("network", net_detail(e)) from e
    try:
        return json.loads(raw) if raw else {}
    except json.JSONDecodeError as e:
        raise ApiError("business", "invalid JSON response") from e


def http_get(base_url: str, endpoint: str, timeout: float) -> dict:
    url = base_url.rstrip("/") + "/" + endpoint
    req = urllib.request.Request(url, headers=common_headers(), method="GET")
    try:
        with PROXY_POOL.open(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        raise ApiError("http", f"HTTP {e.code}") from e
    except (TimeoutError, urllib.error.URLError, ConnectionError,
            http.client.HTTPException, OSError):
        # login-qr.ts: QR status fetch failures are treated as "wait"
        return {"status": "wait"}
    try:
        return json.loads(raw) if raw else {"status": "wait"}
    except json.JSONDecodeError:
        return {"status": "wait"}


def load_creds() -> dict | None:
    creds = read_json(CREDS_FILE)
    return creds if creds and creds.get("bot_token") else None


_API_HOST_SUFFIXES = ("weixin.qq.com",)
_LOOPBACK_HOSTS = ("127.0.0.1", "::1", "localhost")


def _api_base_allowed(url) -> bool:
    """Where the Bearer token may go. v5.0.5 allowlisted media/CDN URLs but
    sent the bot token to whatever `baseurl` (login response) or
    `redirect_host` (QR flow) said. Now: https on *.weixin.qq.com, a loopback
    host (local test servers, a hub on this machine), or an exact origin in
    WECHAT_BRIDGE_API_ORIGINS (comma separated, e.g. a self-hosted
    ilink-hub: "https://hub.example.com,http://10.0.0.5:8765")."""
    try:
        p = urllib.parse.urlsplit(str(url or ""))
        host = (p.hostname or "").lower()
        p.port                                  # raises on a bad port
    except ValueError:
        return False
    if not host or p.username is not None or p.password is not None:
        return False
    if p.scheme == "https" and any(host == s or host.endswith("." + s)
                                   for s in _API_HOST_SUFFIXES):
        return True
    if p.scheme in ("http", "https") and host in _LOOPBACK_HOSTS:
        return True
    extra = {x.strip().lower().rstrip("/")
             for x in os.environ.get("WECHAT_BRIDGE_API_ORIGINS", "").split(",")
             if x.strip()}
    return f"{p.scheme}://{p.netloc}".lower() in extra


def api_base(creds: dict | None) -> str:
    base = (creds or {}).get("baseurl") or DEFAULT_BASE_URL
    if not _api_base_allowed(base):
        raise ApiError("config", "baseurl not allowed (https *.weixin.qq.com, "
                                 "loopback, or WECHAT_BRIDGE_API_ORIGINS)")
    return base


def bot_post(creds: dict, endpoint: str, body: dict, timeout: float) -> dict:
    payload = {**body, "base_info": base_info()}
    return http_post(api_base(creds), endpoint, payload,
                     post_headers(token=creds["bot_token"]), timeout)


def bot_post_retry(creds: dict, endpoint: str, body: dict, timeout: float,
                   attempts: int = 3) -> dict:
    """bot_post for idempotent endpoints: retries transport failures."""
    for i in range(attempts):
        try:
            return bot_post(creds, endpoint, body, timeout)
        except ApiError as e:
            if e.kind not in ("connect", "network", "timeout") \
                    or i == attempts - 1:
                raise
            time.sleep(0.5 + i)
    raise AssertionError("unreachable")


def _check_resp(resp: dict, what: str) -> None:
    """Raise ApiError(business) if an iLink reply reports a failure in EITHER
    `ret` or `errcode`. v3 only looked at `ret` on the send paths, while the
    poll loop already treats `errcode` as authoritative: a sendmessage answer
    like {"errcode": -14, "errmsg": ...} was recorded as "sent" although
    nothing reached the user (a silent loss). A plain {} is success."""
    if not isinstance(resp, dict):
        raise ApiError("business", f"{what}: unexpected response type")
    ret, errcode = resp.get("ret"), resp.get("errcode")
    if ret in (None, 0) and errcode in (None, 0):
        return
    code = errcode if errcode not in (None, 0) else ret
    raise ApiError("business",
                   f"{what} ret={ret} errcode={errcode} "
                   f"errmsg={_scrub(resp.get('errmsg') or '')[:80]}",
                   code=code, errmsg=str(resp.get("errmsg") or "")[:200])


# --------------------------------------------------------------------------
# WeChat API operations
# --------------------------------------------------------------------------

def get_updates(creds: dict, cursor: str, timeout_ms: int) -> dict:
    try:
        return bot_post(creds, "ilink/bot/getupdates",
                        {"get_updates_buf": cursor or ""},
                        timeout=timeout_ms / 1000 + 5)
    except ApiError as e:
        if e.kind == "timeout":
            # api.ts: long-poll client timeout is a normal empty result. Flagged
            # (2026-11-04 review) so the receiver can tell "the server answered:
            # nothing new" from "nothing came back at all" - which includes a
            # keepalive-detected dead path (ETIMEDOUT is a TimeoutError).
            return {"ret": 0, "msgs": [], "get_updates_buf": cursor,
                    "_client_timeout": True}
        raise


def new_client_id() -> str:
    return f"openclaw-weixin:{now_ms()}-{secrets.token_hex(4)}"



def _fast_retry_delays() -> tuple:
    raw = os.environ.get("WECHAT_BRIDGE_FAST_RETRY_MS")
    if raw is None:
        return (0.3, 1.5, 5.0)
    if raw.strip().lower() in ("", "0", "off"):
        return ()
    try:
        return tuple(int(x) / 1000 for x in raw.split(",") if x.strip())
    except ValueError:
        return (0.3, 1.5, 5.0)


def _sendmessage(creds: dict, msg: dict, what: str) -> dict:
    """POST sendmessage, plus v5's in-process fast retries for failures
    BEFORE the request left this host (kind=connect: DNS/TCP/CONNECT/TLS/
    write - provably not delivered): after 0.3 s, 1.5 s and 5 s, with the
    SAME client_id. The egress drops connections in 1-1.5 s bursts and v4
    waited 10 s for its first (task-level) retry. A failure after the request
    left (timeout, reset) is never repeated here."""
    delays = _fast_retry_delays()
    attempt = 0
    while True:
        try:
            resp = bot_post(creds, "ilink/bot/sendmessage", {"msg": msg},
                            timeout=15)
            break
        except ApiError as e:
            if e.kind != "connect" or attempt >= len(delays) or _stop.is_set():
                raise
            _NET_STATS["fast_retries"] += 1
            log(f"{what}: connect failed ({_scrub(e)[:60]}); fast retry "
                f"{attempt + 1}/{len(delays)} in {delays[attempt]:g}s")
            if _stop.wait(delays[attempt]):
                raise           # serve is stopping: this part was not sent
            attempt += 1
    _check_resp(resp, what)
    return resp


def send_text(creds: dict, to_user_id: str, text: str,
              context_token: str | None, client_id: str | None = None) -> dict:
    """One single attempt. A timeout is reported as uncertain by the caller;
    we never silently re-send, to avoid duplicate replies. A resend of the
    same part reuses the client_id it was first sent with, so a server that
    de-duplicates by client_id can drop the duplicate."""
    client_id = client_id or new_client_id()
    msg = {
        "from_user_id": "",
        "to_user_id": to_user_id,
        "client_id": client_id,
        "message_type": 2,
        "message_state": 2,
        "item_list": [{"type": 1, "text_item": {"text": text}}],
    }
    if context_token:
        msg["context_token"] = context_token
    resp = _sendmessage(creds, msg, "sendmessage")
    resp["_client_id"] = client_id
    return resp


# --------------------------------------------------------------------------
# Outbound images (official CDN flow, mirroring the inbound side above:
# docs/protocol.md "CDN media flow" / src/messaging/send.ts /
# src/cdn/upload.ts / src/cdn/cdn-upload.ts)
#
#   1. read file, MD5 + AES-128-ECB padded size
#   2. POST ilink/bot/getuploadurl (media_type=1 image, no_need_thumb)
#   3. AES-128-ECB encrypt (openssl CLI; stdlib has no AES), POST the
#      ciphertext to the CDN URL (upload_full_url preferred, else
#      <cdn>/upload?encrypted_query_param=<upload_param>&filekey=...)
#      and read the x-encrypted-param response header
#   4. sendmessage with a single image item:
#      type=2, image_item.media={encrypt_query_param, aes_key:
#      base64(hex key string), encrypt_type:1}, mid_size=ciphertext size
#      (send.ts encodes the hex key string as base64, not the raw bytes)
# A caption, if given, is sent first as its own text message, exactly
# like upstream sendMediaItems. This command does NOT touch task
# status: an executor sends the image, then still completes the task
# with text (or vice versa).
# --------------------------------------------------------------------------

IMAGE_SEND_MAX_BYTES = 10 * 1024 * 1024
IMAGE_SEND_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}
CDN_UPLOAD_MAX_RETRIES = 3


# --- AES-128-ECB backends -------------------------------------------------
# stdlib has no AES. Preferred: cryptography (pip install cryptography).
# Next: libcrypto via ctypes (no new dependency, no key on a command
# line). Last resort: the openssl CLI, whose -K argument exposes the
# media key to other local users via ps for the lifetime of the child
# process; that fallback logs a one-time warning. The key is per-file,
# so a leak affects only that one media file, but prefer cryptography.

_AES_BACKEND = None
_AES_WARNED = False


def _aes_backend():
    global _AES_BACKEND, _AES_WARNED
    if _AES_BACKEND is not None:
        return _AES_BACKEND
    try:
        import cryptography  # noqa: F401
        _AES_BACKEND = "cryptography"
    except ImportError:
        try:
            import ctypes.util
            _AES_BACKEND = ("ctypes" if ctypes.util.find_library("crypto")
                            else "openssl")
        except Exception:  # noqa: BLE001
            _AES_BACKEND = "openssl"
    if _AES_BACKEND == "openssl" and not _AES_WARNED:
        _AES_WARNED = True
        try:
            log("AES backend: openssl CLI (key visible in process list "
                "to local users; pip install cryptography to avoid)")
        except Exception:  # noqa: BLE001
            pass
    return _AES_BACKEND


def _aes_cryptography(data: bytes, key: bytes, encrypt: bool) -> bytes:
    from cryptography.hazmat.primitives import padding
    from cryptography.hazmat.primitives.ciphers import (
        Cipher, algorithms, modes)
    cipher = Cipher(algorithms.AES(key), modes.ECB())
    if encrypt:
        padder = padding.PKCS7(128).padder()
        data = padder.update(data) + padder.finalize()
        ctx = cipher.encryptor()
        return ctx.update(data) + ctx.finalize()
    ctx = cipher.decryptor()
    plain = ctx.update(data) + ctx.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    return unpadder.update(plain) + unpadder.finalize()


_EVP_LIB = None


def _evp_lib():
    global _EVP_LIB
    if _EVP_LIB is not None:
        return _EVP_LIB
    import ctypes
    import ctypes.util
    lib = ctypes.CDLL(ctypes.util.find_library("crypto"))
    lib.EVP_CIPHER_CTX_new.restype = ctypes.c_void_p
    lib.EVP_CIPHER_CTX_free.argtypes = [ctypes.c_void_p]
    lib.EVP_aes_128_ecb.restype = ctypes.c_void_p
    for prefix in ("Encrypt", "Decrypt"):
        getattr(lib, f"EVP_{prefix}Init_ex").argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
            ctypes.c_char_p, ctypes.c_char_p]
        getattr(lib, f"EVP_{prefix}Update").argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_int),
            ctypes.c_char_p, ctypes.c_int]
        getattr(lib, f"EVP_{prefix}Final_ex").argtypes = [
            ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_int)]
    _EVP_LIB = lib
    return lib


def _aes_ctypes(data: bytes, key: bytes, encrypt: bool) -> bytes:
    import ctypes
    lib = _evp_lib()
    ctx = lib.EVP_CIPHER_CTX_new()
    if not ctx:
        raise MediaError("decrypt" if not encrypt else "encrypt", "EVP context allocation failed")
    try:
        init = lib.EVP_EncryptInit_ex if encrypt else lib.EVP_DecryptInit_ex
        update = lib.EVP_EncryptUpdate if encrypt else lib.EVP_DecryptUpdate
        final = lib.EVP_EncryptFinal_ex if encrypt else lib.EVP_DecryptFinal_ex
        if init(ctx, lib.EVP_aes_128_ecb(), None, key, None) <= 0:
            raise MediaError("decrypt" if not encrypt else "encrypt", "EVP init failed")
        out = ctypes.create_string_buffer(len(data) + 32)
        outlen = ctypes.c_int(0)
        if update(ctx, ctypes.addressof(out), ctypes.byref(outlen),
                  data, len(data)) <= 0:
            raise MediaError("decrypt" if not encrypt else "encrypt", "EVP update failed")
        n = outlen.value
        fin = ctypes.create_string_buffer(32)
        finlen = ctypes.c_int(0)
        if final(ctx, ctypes.addressof(fin), ctypes.byref(finlen)) <= 0:
            raise MediaError("decrypt" if not encrypt else "encrypt", "EVP final failed (bad padding?)")
        return out.raw[:n] + fin.raw[:finlen.value]
    finally:
        lib.EVP_CIPHER_CTX_free(ctx)


def _aes_bytes(data: bytes, key: bytes, encrypt: bool) -> bytes:
    backend = _aes_backend()
    if backend == "cryptography":
        return _aes_cryptography(data, key, encrypt)
    if backend == "ctypes":
        return _aes_ctypes(data, key, encrypt)
    args = ["openssl", "enc", "-aes-128-ecb", "-K", key.hex()]
    if not encrypt:
        args.insert(2, "-d")
    proc = subprocess.run(args, input=data, capture_output=True, timeout=30)
    if proc.returncode != 0:
        raise MediaError("decrypt" if not encrypt else "encrypt", "openssl AES failed")
    return proc.stdout


def _aes_stream(src: Path, key: bytes, dst: Path, encrypt: bool) -> None:
    """Streaming AES file transform (constant memory) for the
    cryptography and ctypes backends; openssl handles files itself."""
    backend = _aes_backend()
    if backend == "openssl":
        args = ["openssl", "enc", "-aes-128-ecb", "-K", key.hex(),
                "-in", str(src), "-out", str(dst)]
        if not encrypt:
            args.insert(2, "-d")
        proc = subprocess.run(args, stdin=subprocess.DEVNULL, capture_output=True, timeout=300)
        if proc.returncode != 0:
            raise MediaError("decrypt" if not encrypt else "encrypt", "openssl AES file failed")
        return
    if backend == "cryptography":
        from cryptography.hazmat.primitives import padding
        from cryptography.hazmat.primitives.ciphers import (
            Cipher, algorithms, modes)
        cipher = Cipher(algorithms.AES(key), modes.ECB())
        ctx = cipher.encryptor() if encrypt else cipher.decryptor()
        padder = padding.PKCS7(128).padder() if encrypt else None
        unpadder = padding.PKCS7(128).unpadder() if not encrypt else None
        with open(src, "rb") as f, open(dst, "wb") as out:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                if encrypt:
                    out.write(ctx.update(padder.update(chunk)))
                else:
                    out.write(unpadder.update(ctx.update(chunk)))
            if encrypt:
                out.write(ctx.update(padder.finalize()) + ctx.finalize())
            else:
                try:
                    tail = unpadder.update(ctx.finalize()) + unpadder.finalize()
                except ValueError as e:
                    raise MediaError(
                        "decrypt" if not encrypt else "encrypt",
                        "media decrypt failed" if not encrypt
                        else "media encrypt failed") from e
                out.write(tail)
        return
    import ctypes
    lib = _evp_lib()
    ctx = lib.EVP_CIPHER_CTX_new()
    if not ctx:
        raise MediaError("decrypt" if not encrypt else "encrypt", "EVP context allocation failed")
    try:
        init = lib.EVP_EncryptInit_ex if encrypt else lib.EVP_DecryptInit_ex
        update = lib.EVP_EncryptUpdate if encrypt else lib.EVP_DecryptUpdate
        final = lib.EVP_EncryptFinal_ex if encrypt else lib.EVP_DecryptFinal_ex
        if init(ctx, lib.EVP_aes_128_ecb(), None, key, None) <= 0:
            raise MediaError("decrypt" if not encrypt else "encrypt", "EVP init failed")
        out = ctypes.create_string_buffer(65536 + 32)
        outlen = ctypes.c_int(0)
        with open(src, "rb") as f, open(dst, "wb") as w:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                if update(ctx, ctypes.addressof(out), ctypes.byref(outlen),
                          chunk, len(chunk)) <= 0:
                    raise MediaError("decrypt" if not encrypt else "encrypt", "EVP update failed")
                w.write(out.raw[:outlen.value])
            fin = ctypes.create_string_buffer(32)
            finlen = ctypes.c_int(0)
            if final(ctx, ctypes.addressof(fin), ctypes.byref(finlen)) <= 0:
                raise MediaError("decrypt" if not encrypt else "encrypt", "EVP final failed (bad padding?)")
            w.write(fin.raw[:finlen.value])
    finally:
        lib.EVP_CIPHER_CTX_free(ctx)


def _encrypt_aes_ecb(data: bytes, key: bytes) -> bytes:
    """AES-128-ECB with PKCS7 padding (cryptography, else libcrypto
    via ctypes, else the openssl CLI)."""
    try:
        return _aes_bytes(data, key, True)
    except Exception as e:  # noqa: BLE001
        # business, not network: a local failure does not heal by retrying
        # (it used to burn the queue's six rounds); same as the file variant.
        raise ApiError("business", "media encrypt failed") from e


def _aes_ecb_padded_size(n: int) -> int:
    return ((n + 1 + 15) // 16) * 16


def _cdn_post_bytes(url: str, data: bytes) -> tuple[int, dict, bytes]:
    """POST raw bytes to the CDN; returns (status, headers, body).
    HTTP errors are returned, not raised, so the caller can abort on
    4xx and retry on 5xx/network exactly like upstream cdn-upload.ts."""
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/octet-stream"})
    try:
        with PROXY_POOL.open(req, timeout=45) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        try:
            body = e.read()
        except Exception:
            body = b""
        return e.code, dict(e.headers or {}), body
    except (TimeoutError, urllib.error.URLError, ConnectionError,
            http.client.HTTPException, OSError) as e:
        raise ApiError("network", f"CDN upload error: {type(e).__name__}") from e


CDN_UPLOAD_TIMEOUT_SEC = 45
CDN_RETRY_DELAYS_SEC = (1, 3)


def _encrypt_aes_ecb_file(src: Path, key: bytes, dst: Path) -> None:
    """File-to-file AES-128-ECB (PKCS7), streaming, constant memory."""
    try:
        _aes_stream(src, key, dst, True)
    except Exception as e:  # noqa: BLE001
        raise ApiError("business", "media encrypt failed") from e


def _cdn_post_file(url: str, path: Path, size: int) -> tuple[int, dict, bytes]:
    """Stream a file to the CDN in 64 KB blocks. Like _cdn_post_bytes: HTTP
    errors are returned, transport errors raised as ApiError."""
    with open(path, "rb") as f:
        req = urllib.request.Request(
            url, data=f, method="POST",
            headers={"Content-Type": "application/octet-stream",
                     "Content-Length": str(size)})
        req.idempotent = True       # nothing is delivered before sendmessage
        try:
            with PROXY_POOL.open(req, timeout=CDN_UPLOAD_TIMEOUT_SEC) as resp:
                return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as e:
            try:
                body = e.read()
            except Exception:  # noqa: BLE001
                body = b""
            return e.code, dict(e.headers or {}), body
        except (TimeoutError, urllib.error.URLError, ConnectionError,
                http.client.HTTPException, OSError) as e:
            raise ApiError("network",
                           f"CDN upload error: {net_detail(e)}") from e

FILE_SEND_MAX_BYTES = 25 * 1024 * 1024
VIDEO_SEND_MAX_BYTES = 50 * 1024 * 1024
VIDEO_RECV_MAX_BYTES = 100 * 1024 * 1024


def _upload_media_to_cdn(creds: dict, path: Path, to_user_id: str,
                         media_type: int, max_bytes: int) -> dict:
    """Shared CDN upload pipeline (getuploadurl -> AES-128-ECB encrypt
    -> POST ciphertext). media_type: 1=image, 2=video, 3=file.
    Streams end to end (md5, encryption and the upload are all chunked), so
    a 50 MB video no longer sits in RAM as plaintext plus ciphertext.
    Transport failures of the CDN POST are retried like upstream
    cdn-upload.ts (3 attempts, 4xx aborts at once): the old loop retried
    only HTTP 5xx, so one dropped connection failed the whole send.
    Returns filekey, download_param, aeskey (hex string), file_size and
    file_size_ciphertext. Keys and CDN parameters are never logged."""
    if not path.is_file():
        raise ApiError("business", f"file not found: {path}")
    rawsize = path.stat().st_size
    if not rawsize:
        raise ApiError("business", "file is empty")
    if rawsize > max_bytes:
        raise ApiError("business", f"file exceeds {max_bytes} bytes")
    rawfilemd5 = _md5_file(path)
    filesize = _aes_ecb_padded_size(rawsize)
    filekey = secrets.token_hex(16)
    aeskey = secrets.token_bytes(16)
    resp = bot_post_retry(creds, "ilink/bot/getuploadurl", {
        "filekey": filekey,
        "media_type": media_type,
        "to_user_id": to_user_id,
        "rawsize": rawsize,
        "rawfilemd5": rawfilemd5,
        "filesize": filesize,
        "no_need_thumb": True,
        "aeskey": aeskey.hex(),
    }, timeout=15)
    _check_resp(resp, "getuploadurl")
    upload_full_url = (resp.get("upload_full_url") or "").strip()
    upload_param = resp.get("upload_param") or ""
    if upload_full_url:
        cdn_url = _validate_server_url(upload_full_url, "upload")
    elif upload_param:
        # Fallback construction, mirroring the download URL pattern
        # (<cdn>/download?encrypted_query_param=...); upload_full_url
        # takes precedence whenever the server provides it.
        cdn_url = (f"{CDN_BASE_URL}/upload?encrypted_query_param="
                   f"{urllib.parse.quote(upload_param, safe='')}"
                   f"&filekey={filekey}")
    else:
        raise ApiError("business",
                       "getuploadurl returned no upload URL")
    OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
    enc_path = OUTBOX_DIR / f".upload-{filekey}.enc"
    try:
        _encrypt_aes_ecb_file(path, aeskey, enc_path)
        if enc_path.stat().st_size != filesize:
            raise ApiError("business", "AES ciphertext size mismatch")
        last_error = "CDN upload failed"
        for attempt in range(CDN_UPLOAD_MAX_RETRIES):
            if attempt:
                time.sleep(CDN_RETRY_DELAYS_SEC[
                    min(attempt - 1, len(CDN_RETRY_DELAYS_SEC) - 1)])
            try:
                status, headers, _body = _cdn_post_file(cdn_url, enc_path,
                                                        filesize)
            except ApiError as e:
                last_error = f"CDN upload {e.kind}: {e}"
                log(f"CDN upload attempt {attempt + 1}/{CDN_UPLOAD_MAX_RETRIES}"
                    f" failed ({e.kind}); retrying")
                continue
            lowered = {str(k).lower(): v for k, v in headers.items()}
            download_param = lowered.get("x-encrypted-param")
            if status == 200 and download_param:
                return {
                    "filekey": filekey,
                    "download_param": download_param,
                    "aeskey_hex": aeskey.hex(),
                    "file_size": rawsize,
                    "file_size_ciphertext": filesize,
                }
            if 400 <= status < 500:
                raise ApiError("business", f"CDN upload client error {status}")
            last_error = f"CDN upload status {status} (no download param)"
        raise ApiError("network", last_error)
    finally:
        enc_path.unlink(missing_ok=True)


def upload_image_to_cdn(creds: dict, path: Path,
                        to_user_id: str) -> dict:
    """Upload a local image file (media_type=1, image extensions only)."""
    if path.suffix.lower() not in IMAGE_SEND_EXTS:
        raise ApiError("business",
                       f"not an image file (expected one of "
                       f"{sorted(IMAGE_SEND_EXTS)}): {path.suffix}")
    return _upload_media_to_cdn(creds, path, to_user_id, media_type=1,
                                max_bytes=IMAGE_SEND_MAX_BYTES)


def upload_file_to_cdn(creds: dict, path: Path,
                       to_user_id: str) -> dict:
    """Upload a local file attachment (media_type=3, any file type)."""
    return _upload_media_to_cdn(creds, path, to_user_id, media_type=3,
                                max_bytes=FILE_SEND_MAX_BYTES)


def upload_video_to_cdn(creds: dict, path: Path,
                        to_user_id: str) -> dict:
    """Upload a local video (media_type=2, UploadMediaType.VIDEO)."""
    return _upload_media_to_cdn(creds, path, to_user_id, media_type=2,
                                max_bytes=VIDEO_SEND_MAX_BYTES)


# kind -> (getuploadurl media_type, message item type). send.ts /
# UploadMediaType: IMAGE=1 VIDEO=2 FILE=3 VOICE=4; item types: 2 image,
# 3 voice, 4 file, 5 video.
MEDIA_KINDS = {"image": (1, 2), "video": (2, 5), "file": (3, 4), "voice": (4, 3)}
VOICE_SEND_MAX_BYTES = 2 * 1024 * 1024      # a 60 s SILK note is ~180 KB
VIDEO_SEND_EXTS = {".mp4", ".m4v", ".mov"}
AUDIO_EXTS = {".mp3", ".wav", ".m4a", ".aac", ".ogg", ".oga", ".opus",
              ".amr", ".flac", ".silk", ".pcm", ".wma"}


def _media_limit(kind: str) -> int:
    return {"image": IMAGE_SEND_MAX_BYTES, "video": VIDEO_SEND_MAX_BYTES,
            "voice": VOICE_SEND_MAX_BYTES}.get(kind, FILE_SEND_MAX_BYTES)


def media_kind_for(path: Path, requested: str = "auto") -> str:
    """auto: images and mp4-family videos by extension, everything else
    (audio included) as a file attachment. A voice bubble is never chosen
    implicitly - it changes what the user gets, so it must be asked for."""
    if requested and requested != "auto":
        return requested
    ext = path.suffix.lower()
    if ext in IMAGE_SEND_EXTS:
        return "image"
    if ext in VIDEO_SEND_EXTS:
        return "video"
    return "file"


def _cdn_media_ref(uploaded: dict) -> dict:
    # send.ts encodes the hex key STRING as base64, not the raw key bytes
    return {"encrypt_query_param": uploaded["download_param"],
            "aes_key": base64.b64encode(
                uploaded["aeskey_hex"].encode("ascii")).decode(),
            "encrypt_type": 1}


def build_media_item(kind: str, uploaded: dict, file_name: str,
                     voice: dict | None = None) -> dict:
    """One sendmessage item for an uploaded file (send.ts shapes):
    image: mid_size = ciphertext size; video: video_size = ciphertext size;
    file: file_name + len (plaintext size, as a string); voice: SILK
    (encode_type 6) with sample_rate and playtime in ms - without playtime
    WeChat shows a 1-second bar (community finding, see CHANGELOG v4)."""
    media = _cdn_media_ref(uploaded)
    if kind == "image":
        return {"type": 2, "image_item": {
            "media": media, "mid_size": uploaded["file_size_ciphertext"]}}
    if kind == "video":
        return {"type": 5, "video_item": {
            "media": media, "video_size": uploaded["file_size_ciphertext"]}}
    if kind == "voice":
        v = voice or {}
        return {"type": 3, "voice_item": {
            "media": media, "encode_type": 6,
            "bits_per_sample": 16,
            "sample_rate": int(v.get("sample_rate") or VOICE_SAMPLE_RATE),
            "playtime": int(v.get("playtime_ms") or 0)}}
    return {"type": 4, "file_item": {
        "media": media, "file_name": file_name,
        "len": str(uploaded["file_size"])}}


def send_media_message(creds: dict, to_user_id: str, item: dict,
                       context_token: str | None,
                       client_id: str | None = None) -> dict:
    """One sendmessage carrying a single media item. Failures are ApiError
    with the usual kinds (connect = not delivered; network/timeout = maybe
    delivered; business carries the ret/errcode, e.g. -2)."""
    client_id = client_id or new_client_id()
    msg = {
        "from_user_id": "",
        "to_user_id": to_user_id,
        "client_id": client_id,
        "message_type": 2,
        "message_state": 2,
        "item_list": [item],
    }
    if context_token:
        msg["context_token"] = context_token
    resp = _sendmessage(creds, msg, "sendmessage(media)")
    resp["_client_id"] = client_id
    return resp


def upload_media(creds: dict, path: Path, to_user_id: str, kind: str) -> dict:
    media_type = MEDIA_KINDS[kind][0]
    if kind == "image" and path.suffix.lower() not in IMAGE_SEND_EXTS:
        raise ApiError("business",
                       f"not an image file (expected one of "
                       f"{sorted(IMAGE_SEND_EXTS)}): {path.suffix}")
    return _upload_media_to_cdn(creds, path, to_user_id,
                                media_type=media_type,
                                max_bytes=_media_limit(kind))


def send_media(creds: dict, to_user_id: str, path: Path, kind: str,
               context_token: str | None, caption: str | None = None) -> dict:
    """Synchronous upload + optional caption + media message (the v3
    send-image/send-file/send-video behaviour, now one code path). A voice
    request converts the audio to SILK first. Returns counts and ids only."""
    kind = media_kind_for(path, kind)
    if kind == "voice" and not _native_voice_enabled():
        # Native bot voice notes are silently dropped by the channel, so
        # go out as a file card (ported from live v4).
        kind = "file"
    voice = None
    send_path = path
    tmpdir = None
    try:
        if kind == "voice":
            OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
            tmpdir = Path(tempfile.mkdtemp(prefix="voice-", dir=str(OUTBOX_DIR)))
            send_path = tmpdir / (path.stem + ".silk")
            voice = audio_to_silk(path, send_path)
        uploaded = upload_media(creds, send_path, to_user_id, kind)
        sent = 0
        client_ids: list[str] = []
        if caption:
            resp = send_text(creds, to_user_id, caption, context_token)
            client_ids.append(resp.get("_client_id") or "")
            sent += 1
        item = build_media_item(kind, uploaded, path.name, voice)
        resp = send_media_message(creds, to_user_id, item, context_token)
        client_ids.append(resp["_client_id"])
        sent += 1
    finally:
        if tmpdir is not None:
            shutil.rmtree(tmpdir, ignore_errors=True)
    log(f"send {kind} to={mask(to_user_id)} name={path.name} "
        f"bytes={uploaded['file_size']} parts={sent}")
    out = {
        "ok": True,
        "kind": kind,
        "parts_sent": sent,
        "client_ids": client_ids,
        "server_message_id": resp.get("message_id"),
        "file_name": path.name,
        "file_size": uploaded["file_size"],
        "file_size_ciphertext": uploaded["file_size_ciphertext"],
    }
    if voice:
        out["playtime_ms"] = voice["playtime_ms"]
    return out


def send_image(creds: dict, to_user_id: str, path: Path,
               context_token: str | None, caption: str | None = None) -> dict:
    return send_media(creds, to_user_id, path, "image", context_token, caption)


def send_file(creds: dict, to_user_id: str, path: Path,
              context_token: str | None, caption: str | None = None) -> dict:
    return send_media(creds, to_user_id, path, "file", context_token, caption)


def send_video(creds: dict, to_user_id: str, path: Path,
               context_token: str | None, caption: str | None = None) -> dict:
    return send_media(creds, to_user_id, path, "video", context_token, caption)


# --------------------------------------------------------------------------
# Voice codec (v4). WeChat voice notes are Tencent-flavoured SILK v3: the
# standard "#!SILK_V3" stream with one 0x02 byte in front and no 0xFFFF end
# marker. Neither ffmpeg nor the standard library speaks SILK, so encoding
# and decoding use an optional module - silk-python (import pysilk) or pilk -
# looked up in vendor/py first, so one install survives VM replacement and
# is visible to both the root-run service and the CLI:
#     pip install --target ~/workspace/wechat-bridge/vendor/py silk-python
# ffmpeg turns any other audio into 24 kHz mono 16-bit PCM. Without a SILK
# module a requested voice bubble falls back to sending the audio as a file
# (the user still gets it), and an inbound untranscribed voice note is
# handed to the executor as the raw .silk with voice_wav=false.
# Pitfalls taken from community implementations: pysilk.encode needs FOUR
# positional args (input, output, sample_rate, bit_rate); its tencent=True
# default already adds the 0x02 byte - a second one makes WeChat play ~1 s;
# voice_item without playtime renders as a 1-second bar.
# --------------------------------------------------------------------------

VOICE_SAMPLE_RATE = 24000
VOICE_MAX_MS = 60_000                   # WeChat's own voice-note limit
SILK_STD_MAGIC = b"#!SILK_V3"


def _native_voice_enabled() -> bool:
    """Native bot voice notes are OFF by default (ported from live v4).

    Verified 2026-11-01 on a real device (and in Tencent/openclaw-weixin
    #91): the iLink API accepts a type-3 voice_item (ret/errcode both 0)
    yet no bubble ever appears in the WeChat client - the channel silently
    drops bot-direction VOICE. So a voice request goes out as an audio file
    card unless the operator explicitly opts back in with
    WECHAT_BRIDGE_NATIVE_VOICE=1 (e.g. if Tencent opens bot VOICE later).
    Inbound voice decoding is unaffected.
    """
    return os.environ.get("WECHAT_BRIDGE_NATIVE_VOICE", "") == "1"


class VoiceCodecError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code        # no_codec | no_ffmpeg | too_long | bad_audio


_VENDOR_WARNED: set = set()


def _dir_trusted(d: Path) -> bool:
    """A root service imports optional code from here: refuse a directory
    anyone may write to, or one owned by someone other than root, us, or the
    owner of the bridge directory itself."""
    try:
        st = d.stat()
        owners = {0, os.getuid(), BASE_DIR.stat().st_uid}
    except OSError:
        return False
    ok = not (st.st_mode & 0o002) and st.st_uid in owners
    if not ok and str(d) not in _VENDOR_WARNED:
        _VENDOR_WARNED.add(str(d))
        log(f"vendor dir {d.name} ignored: world-writable or foreign owner")
    return ok


def _add_vendor_paths() -> None:
    """vendor/py (pip --target) and legacy vendor/ are APPENDED to sys.path.
    v5.0.5 put them in FRONT (two different dirs in two places), so any
    file there shadowed the standard library for every later import."""
    for vend in (BASE_DIR / "vendor" / "py", BASE_DIR / "vendor"):
        s = str(vend)
        if s not in sys.path and vend.is_dir() and _dir_trusted(vend):
            sys.path.append(s)


def _import_optional(name: str):
    _add_vendor_paths()
    try:
        return importlib.import_module(name)
    except Exception:  # noqa: BLE001 - optional dependency
        return None


def silk_backend() -> tuple:
    """(name, module) of the first usable SILK codec, else (None, None).
    WECHAT_BRIDGE_SILK=off disables it (forces the file fallback)."""
    if os.environ.get("WECHAT_BRIDGE_SILK", "on") == "off":
        return None, None
    for name in ("pysilk", "pilk"):
        mod = _import_optional(name)
        if mod is not None and hasattr(mod, "encode") \
                and hasattr(mod, "decode"):
            return name, mod
    return None, None


def ffmpeg_path() -> str | None:
    return shutil.which("ffmpeg")


def _write_wav(pcm: Path, wav: Path, rate: int) -> None:
    import wave
    with open(pcm, "rb") as src, wave.open(str(wav), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(rate)
        for chunk in iter(lambda: src.read(1 << 16), b""):
            w.writeframes(chunk)


def _audio_to_pcm(src: Path, pcm: Path) -> None:
    """Any audio -> 24 kHz mono s16le PCM. .pcm is taken as-is; a WAV that
    already has that format is unpacked without ffmpeg."""
    ext = src.suffix.lower()
    if ext == ".pcm":
        shutil.copyfile(src, pcm)
        return
    if ext == ".wav":
        import wave
        try:
            with wave.open(str(src), "rb") as w:
                if (w.getnchannels(), w.getsampwidth(), w.getframerate()) \
                        == (1, 2, VOICE_SAMPLE_RATE):
                    with open(pcm, "wb") as out:
                        while True:
                            frames = w.readframes(1 << 15)
                            if not frames:
                                break
                            out.write(frames)
                    return
        except (wave.Error, EOFError, OSError):
            pass
    ff = ffmpeg_path()
    if not ff:
        raise VoiceCodecError("no_ffmpeg",
                              "ffmpeg not found (needed to convert audio)")
    proc = subprocess.run(
        [ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-i", str(src),
         "-f", "s16le", "-acodec", "pcm_s16le", "-ar", str(VOICE_SAMPLE_RATE),
         "-ac", "1", str(pcm)],
        stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
    if proc.returncode != 0 or not pcm.exists() or not pcm.stat().st_size:
        raise VoiceCodecError("bad_audio", "ffmpeg could not decode the audio")


def _normalize_tencent_silk(path: Path) -> None:
    data = path.read_bytes()
    core = data.lstrip(b"\x02")
    if not core.startswith(SILK_STD_MAGIC):
        raise VoiceCodecError("bad_audio", "encoder output is not SILK v3")
    if core.endswith(b"\xff\xff"):
        core = core[:-2]
    fixed = b"\x02" + core
    if fixed != data:
        path.write_bytes(fixed)


def audio_to_silk(src: Path, dst: Path) -> dict:
    """Encode an audio file as a WeChat voice note (Tencent SILK, 24 kHz).
    Returns {"playtime_ms", "sample_rate", "codec"}; raises VoiceCodecError."""
    name, mod = silk_backend()
    if mod is None:
        raise VoiceCodecError(
            "no_codec", "no SILK encoder installed (pip install --target "
                        "vendor/py silk-python)")
    if not src.is_file() or not src.stat().st_size:
        raise VoiceCodecError("bad_audio", "audio file missing or empty")
    pcm = dst.with_suffix(".pcm")
    try:
        _audio_to_pcm(src, pcm)
        playtime_ms = int(pcm.stat().st_size / (VOICE_SAMPLE_RATE * 2) * 1000)
        if playtime_ms < 300:
            raise VoiceCodecError("bad_audio", "audio is shorter than 0.3 s")
        if playtime_ms > VOICE_MAX_MS:
            raise VoiceCodecError(
                "too_long", f"audio is {playtime_ms / 1000:.0f}s; WeChat voice "
                            f"notes are limited to {VOICE_MAX_MS // 1000}s")
        try:
            if name == "pysilk":
                with open(pcm, "rb") as fi, open(dst, "wb") as fo:
                    mod.encode(fi, fo, VOICE_SAMPLE_RATE, VOICE_SAMPLE_RATE)
            else:
                try:
                    mod.encode(str(pcm), str(dst), pcm_rate=VOICE_SAMPLE_RATE,
                               tencent=True)
                except TypeError:
                    mod.encode(str(pcm), str(dst))
        except VoiceCodecError:
            raise
        except Exception as e:  # noqa: BLE001 - third-party codec
            raise VoiceCodecError("bad_audio",
                                  f"SILK encode failed: {type(e).__name__}") from e
        _normalize_tencent_silk(dst)
    finally:
        pcm.unlink(missing_ok=True)
    return {"playtime_ms": playtime_ms, "sample_rate": VOICE_SAMPLE_RATE,
            "codec": name}


def silk_to_wav(src: Path, dst: Path) -> dict:
    """Decode a WeChat SILK note to a 24 kHz mono WAV for speech-to-text.
    Returns {"playtime_ms", "codec"}; raises VoiceCodecError."""
    name, mod = silk_backend()
    if mod is None:
        raise VoiceCodecError("no_codec", "no SILK decoder installed")
    core = src.read_bytes().lstrip(b"\x02")
    if not core.startswith(SILK_STD_MAGIC):
        raise VoiceCodecError("bad_audio", "not a SILK v3 stream")
    std = dst.with_suffix(".std.silk")
    pcm = dst.with_suffix(".pcm")
    try:
        std.write_bytes(core)
        try:
            if name == "pysilk":
                with open(std, "rb") as fi, open(pcm, "wb") as fo:
                    mod.decode(fi, fo, VOICE_SAMPLE_RATE)
            else:
                try:
                    mod.decode(str(std), str(pcm), pcm_rate=VOICE_SAMPLE_RATE)
                except TypeError:
                    mod.decode(str(std), str(pcm))
        except Exception as e:  # noqa: BLE001 - third-party codec
            raise VoiceCodecError("bad_audio",
                                  f"SILK decode failed: {type(e).__name__}") from e
        if not pcm.exists() or not pcm.stat().st_size:
            raise VoiceCodecError("bad_audio", "SILK decode produced no audio")
        _write_wav(pcm, dst, VOICE_SAMPLE_RATE)
        playtime_ms = int(pcm.stat().st_size / (VOICE_SAMPLE_RATE * 2) * 1000)
    finally:
        std.unlink(missing_ok=True)
        pcm.unlink(missing_ok=True)
    return {"playtime_ms": playtime_ms, "codec": name}


def audio_to_wav(src: Path, dst: Path) -> bool:
    """Non-SILK inbound voice (amr/mp3/ogg) -> 16 kHz WAV via ffmpeg."""
    ff = ffmpeg_path()
    if not ff:
        return False
    proc = subprocess.run(
        [ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-i", str(src),
         "-ar", "16000", "-ac", "1", str(dst)], stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
    return proc.returncode == 0 and dst.exists() and dst.stat().st_size > 44


def notify(creds: dict, start: bool) -> bool:
    """Lifecycle presence signal. Returns True on success. The server
    drives the 'temporarily unable to connect' title in the user's
    WeChat from this + poll liveness, so a failed notifyStart must be
    retried by the caller - a one-shot attempt that hits a network flap
    otherwise leaves the bot showing offline indefinitely."""
    try:
        bot_post(creds, "ilink/bot/msg/notifystart" if start
                 else "ilink/bot/msg/notifystop", {}, timeout=10)
        return True
    except ApiError as e:
        log(f"notify{'Start' if start else 'Stop'} failed ({e.kind}) - non-fatal")
        return False


# --------------------------------------------------------------------------
# QR login
# --------------------------------------------------------------------------

def make_qr_png(content: str) -> bool:
    """The QR is a login credential while it is valid (whoever scans it first
    binds the bot), so both outputs are 0600."""
    _add_vendor_paths()
    try:
        import qrcode  # type: ignore
        img = qrcode.make(content)
        img.save(QR_PNG)
        with contextlib.suppress(OSError):
            os.chmod(QR_PNG, 0o600)
        return True
    except Exception as e:  # noqa: BLE001
        log(f"QR PNG generation unavailable: {type(e).__name__}")
        atomic_write_text(QR_TXT, content + "\n")
        return False



TOKEN_HISTORY_FILE = STATE_DIR / "token_history.json"


def _recent_tokens(creds: dict | None) -> list:
    """v5: up to 10 recent bot tokens for get_bot_qrcode's local_token_list
    (official 2.3.1), so the server can answer binded_redirect instead of
    opening a duplicate session."""
    toks = [creds["bot_token"]] if creds and creds.get("bot_token") else []
    for t in read_json(TOKEN_HISTORY_FILE, []) or []:
        if isinstance(t, str) and t and t not in toks:
            toks.append(t)
    return toks[:10]


def _remember_old_token(token: str | None) -> None:
    if not token:
        return
    old = [t for t in (read_json(TOKEN_HISTORY_FILE, []) or [])
           if isinstance(t, str) and t != token]
    with contextlib.suppress(OSError):
        atomic_write_json(TOKEN_HISTORY_FILE, [token] + old[:9])


def _start_login(force: bool) -> tuple:
    """Fetch a fresh login QR: (exit code, result). Shared by `login` and by
    wait-login's refresh, and it never prints: wait-login's stdout must stay
    ONE JSON object - v5.0.5 printed login's object in the middle of it on
    every QR refresh, so the caller's json.loads failed (reproduced)."""
    creds = load_creds()
    if creds and not force:
        return 2, {"ok": False,
                   "error": "credentials already exist; use --force to re-login"}
    local_tokens = _recent_tokens(creds)
    # api.ts: QR POST carries AuthorizationType + X-WECHAT-UIN, no token,
    # no base_info.
    try:
        resp = http_post(FIXED_BASE_URL,
                         f"ilink/bot/get_bot_qrcode?bot_type={BOT_TYPE}",
                         {"local_token_list": local_tokens},
                         post_headers(token=None), timeout=15)
    except ApiError as e:
        return 1, {"ok": False,
                   "error": f"could not fetch a QR code ({e.kind}): {_scrub(e)}"}
    qrcode_value = resp.get("qrcode")
    img_content = resp.get("qrcode_img_content")
    if not qrcode_value or not img_content:
        return 1, {"ok": False, "error": "server returned no qrcode"}
    atomic_write_json(LOGIN_FILE, {
        "qrcode": qrcode_value,
        "qrcode_img_content": img_content,
        "started_at": now_ms(),
        "current_base_url": FIXED_BASE_URL,
        "refresh_count": 0,
    })
    atomic_write_json(LOGIN_STATUS_FILE, {"status": "wait", "updated_at": now_ms()})
    png = make_qr_png(img_content)
    return 0, {"ok": True, "qr_png": str(QR_PNG) if png else None,
               "qr_txt": None if png else str(QR_TXT),
               "expires_in_sec": 300}


def cmd_login(args) -> int:
    rc, out = _start_login(bool(args.force))
    print(json.dumps(out, ensure_ascii=False))
    return rc


def _checked_baseurl(url) -> str:
    """The login response's baseurl if the bot token may go there, else the
    default host (see _api_base_allowed)."""
    if url and _api_base_allowed(url):
        return str(url)
    if url:
        log("login: baseurl outside the allowlist; using the default host")
    return DEFAULT_BASE_URL


def _save_credentials(status_resp: dict) -> dict:
    old_token = (load_creds() or {}).get("bot_token")
    creds = {
        "bot_token": status_resp.get("bot_token"),
        "ilink_bot_id": status_resp.get("ilink_bot_id"),
        "baseurl": _checked_baseurl(status_resp.get("baseurl")),
        "ilink_user_id": status_resp.get("ilink_user_id"),
        "saved_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    atomic_write_json(CREDS_FILE, creds, mode=0o600)
    os.chmod(CREDS_FILE, 0o600)
    if old_token != creds.get("bot_token"):
        _remember_old_token(old_token)
        _reset_session_state("new login")
    return creds


def cmd_wait_login(args) -> int:
    session = read_json(LOGIN_FILE)
    if not session:
        print(json.dumps({"ok": False, "error": "no active login; run login first"},
                         ensure_ascii=False))
        return 2
    deadline = time.time() + args.timeout
    current_base = session.get("current_base_url") or FIXED_BASE_URL
    refresh_count = int(session.get("refresh_count") or 0)
    verify_code = args.verify_code
    refreshed: dict = {}        # set once the QR was replaced: show the NEW one
    while time.time() < deadline:
        endpoint = ("ilink/bot/get_qrcode_status?qrcode="
                    + urllib.parse.quote(session["qrcode"], safe=""))
        if verify_code:
            endpoint += "&verify_code=" + urllib.parse.quote(verify_code, safe="")
        resp = http_get(current_base, endpoint, timeout=35)
        status = resp.get("status", "wait")
        atomic_write_json(LOGIN_STATUS_FILE,
                          {"status": status, "updated_at": now_ms(), **refreshed})
        if status == "wait":
            time.sleep(1)
            continue
        if status == "scaned":
            verify_code = None
            time.sleep(1)
            continue
        if status == "scaned_but_redirect":
            # 2026-11-04 review: the redirect target is where the confirmed
            # login (and its token) comes from - allowlisted like baseurl.
            host = str(resp.get("redirect_host") or "").strip()
            cand = (f"https://{host}"
                    if re.fullmatch(r"[A-Za-z0-9.-]+(?::\d{1,5})?", host) else "")
            if cand and _api_base_allowed(cand):
                current_base = cand
            elif host:
                log("login: redirect_host outside the allowlist; ignored")
            time.sleep(1)
            continue
        if status == "need_verifycode":
            if not args.verify_code:
                print(json.dumps({"ok": False, "status": "need_verifycode",
                                  "error": "phone shows a verify code; "
                                           "rerun wait-login --verify-code <code>",
                                  **refreshed}, ensure_ascii=False))
                return 3
            # wrong code loops back to need_verifycode with the same code;
            # require a fresh code from the user instead of spinning
            print(json.dumps({"ok": False, "status": "need_verifycode",
                              "error": "verify code not accepted; ask user for a new code",
                              **refreshed}, ensure_ascii=False))
            return 3
        if status in ("expired", "verify_code_blocked"):
            refresh_count += 1
            if refresh_count > 3:
                print(json.dumps({"ok": False, "status": status,
                                  "error": "QR refreshed 3 times; giving up",
                                  **refreshed}, ensure_ascii=False))
                return 1
            log(f"login QR {status}; refreshing ({refresh_count}/3)")
            rc_, fresh_qr = _start_login(True)
            if rc_ != 0:
                print(json.dumps({"ok": False, "status": status,
                                  "error": f"QR refresh failed: {fresh_qr.get('error')}",
                                  **refreshed}, ensure_ascii=False))
                return 1
            refreshed = {"qr_refreshes": refresh_count,
                         "qr_png": fresh_qr.get("qr_png"),
                         "qr_txt": fresh_qr.get("qr_txt")}
            log("login QR refreshed: the user has to scan the NEW code "
                f"({fresh_qr.get('qr_png') or fresh_qr.get('qr_txt')})")
            session = read_json(LOGIN_FILE) or {}
            session["refresh_count"] = refresh_count
            atomic_write_json(LOGIN_FILE, session)
            atomic_write_json(LOGIN_STATUS_FILE, {"status": "qr_refreshed",
                                                  "updated_at": now_ms(), **refreshed})
            current_base = FIXED_BASE_URL
            verify_code = None
            continue
        if status == "binded_redirect":
            creds = load_creds()
            if creds:
                print(json.dumps({"ok": True, "already_connected": True,
                                  "ilink_bot_id": mask(creds.get("ilink_bot_id")),
                                  "ilink_user_id": mask(creds.get("ilink_user_id")),
                                  **refreshed}, ensure_ascii=False))
                return 0
            print(json.dumps({"ok": False, "status": status,
                              "error": "bot already bound elsewhere; no local credentials",
                              **refreshed}, ensure_ascii=False))
            return 1
        if status == "confirmed":
            if not resp.get("ilink_bot_id"):
                print(json.dumps({"ok": False,
                                  "error": "confirmed but ilink_bot_id missing",
                                  **refreshed}, ensure_ascii=False))
                return 1
            creds = _save_credentials(resp)
            LOGIN_FILE.unlink(missing_ok=True)
            atomic_write_json(LOGIN_STATUS_FILE,
                              {"status": "confirmed", "updated_at": now_ms()})
            print(json.dumps({"ok": True, "connected": True,
                              "ilink_bot_id": mask(creds["ilink_bot_id"]),
                              "ilink_user_id": mask(creds["ilink_user_id"]),
                              "baseurl": creds["baseurl"], **refreshed},
                             ensure_ascii=False))
            return 0
        time.sleep(1)
    print(json.dumps({"ok": False, "error": "login timeout", **refreshed},
                     ensure_ascii=False))
    return 1


# --------------------------------------------------------------------------
# Task store
# --------------------------------------------------------------------------

def task_path(message_id: str) -> Path:
    return INBOX_DIR / f"{safe_name(message_id)}.json"


def _legacy_task_path(message_id: str) -> Path | None:
    """Where v5.0.5 stored this id, if that differs (ids with characters
    outside [0-9A-Za-z_-] - never seen from iLink, but possible)."""
    old = INBOX_DIR / f"{_legacy_safe(message_id)}.json"
    return None if old == task_path(message_id) else old


def load_task(message_id: str) -> dict | None:
    t = read_json(task_path(message_id))
    if isinstance(t, dict) and t.get("message_id") is not None \
            and str(t["message_id"]) != str(message_id):
        t = None        # a v5.0.5 file of a colliding id ("a.b" in a_b.json)
    if t is None:
        old = _legacy_task_path(message_id)
        if old is not None:
            t = read_json(old)
            if not isinstance(t, dict) or str(t.get("message_id")) != str(message_id):
                t = None        # the old mapping collided: not this task
    return t


def save_task(task: dict) -> None:
    atomic_write_json(task_path(task["message_id"]), task, mode=0o600)
    old = _legacy_task_path(task["message_id"])
    if old is not None:
        with contextlib.suppress(OSError):
            if str((read_json(old) or {}).get("message_id")) == str(task["message_id"]):
                old.unlink()    # moved to its collision-free name


CONTEXT_TOKENS_FILE = STATE_DIR / "context_tokens.json"


def remember_context_token(user: str, token: str | None) -> None:
    """Newest context_token per user (official plugin: contextTokenStore,
    persisted per account; "must be echoed verbatim in every outbound
    send"). Proactive sends - send-direct alerts, send-image --to-user -
    now echo it; they used to send none."""
    if not user or not token:
        return
    try:
        data = read_json(CONTEXT_TOKENS_FILE, {}) or {}
        if not isinstance(data, dict):
            data = {}
        data[user] = {"token": token, "at": now_ms()}
        atomic_write_json(CONTEXT_TOKENS_FILE, data, fsync=False)
    except OSError as e:
        log(f"context token save failed: {type(e).__name__}")


def latest_context_token(user: str | None) -> tuple:
    """(token, age_ms) of the newest context_token seen from `user`, else
    (None, None). Falls back to the newest stored task of that user."""
    if not user:
        return None, None
    entry = (read_json(CONTEXT_TOKENS_FILE, {}) or {}).get(user) or {}
    if isinstance(entry, dict) and entry.get("token"):
        return entry["token"], max(0, now_ms() - int(entry.get("at") or 0))
    for summ in reversed(scan_task_index()[-50:]):
        if summ.get("from_user_id") != user:
            continue
        t = load_task(summ["message_id"]) or {}
        if t.get("context_token"):
            return t["context_token"], max(
                0, now_ms() - int(t.get("received_at") or 0))
    return None, None


def _file_sig(path: Path):
    try:
        st = os.stat(path)
    except OSError:
        return None
    return (st.st_ino, st.st_mtime_ns, st.st_size)


def _reset_session_state(reason: str) -> None:
    """A new login invalidates everything tied to the old session: the
    get_updates_buf cursor, the -14 pause, the auth alert, context tokens and
    any rate-limit deferral (epiral/weixin-bot and wechat-ilink both clear
    cursor + context tokens on re-login). Before, a successful re-scan still
    sat out the rest of a 1-hour -14 pause."""
    with contextlib.suppress(OSError):
        atomic_write_json(CURSOR_FILE, {"get_updates_buf": ""})
    for p in (SESSION_FILE, STATE_DIR / "auth_alert.json",
              CONTEXT_TOKENS_FILE, RATE_LIMIT_FILE, QUOTA_FILE):
        with contextlib.suppress(OSError):
            p.unlink(missing_ok=True)
    log(f"session state reset ({reason}): cursor, -14 pause, auth alert, "
        "context tokens")


# --------------------------------------------------------------------------
# Conversation history + memo (v4). The executor used to see one message
# at a time, so "那个改一下" or "刚才那个文件" had no referent and every
# follow-up had to be re-explained. Every user message and every delivered
# bot message is appended to history/chat.jsonl (0600, outside state/ so it
# is not copied into backups); `claim` hands the executor the last few
# turns plus a short memo the executor maintains itself (`bridge.py memo`).
# `bridge.py history` lets Muse read the WeChat side from the App as well -
# the "shared memory" half of WeChat<->App; MEMORY.md stays Muse's own.
# Disable with WECHAT_BRIDGE_HISTORY=0 (and CONTEXT_TURNS=0 for claim).
# --------------------------------------------------------------------------

HISTORY_FILE = HISTORY_DIR / "chat.jsonl"
HISTORY_LOCK = HISTORY_DIR / ".lock"
HISTORY_MAX_BYTES = 1_000_000
HISTORY_KEEP_LINES = 600
HISTORY_TEXT_MAX = 1000
MEMO_FILE = STATE_DIR / "conversation_memo.json"
DECISIONS_FILE = BASE_DIR / "state" / "DECISIONS.md"  # runtime decisions (gitignored)
DECISIONS_LEGACY_FILE = BASE_DIR / "DECISIONS.md"  # pre-v5.0.2 location, read-only fallback


def _decisions_read() -> str:
    """Read the decision list: the runtime file first, the legacy repo
    file as fallback. v5.0.2 wired the fallback into claim only, so
    `decisions` show printed empty for legacy-only deployments and the
    first --add seeded the runtime file with just the new entry, after
    which claim stopped seeing the legacy decisions at all. All three
    paths (claim, show, --add) use this helper now, and --add seeds the
    runtime file from the legacy text when it first creates it."""
    try:
        if DECISIONS_FILE.exists():
            return DECISIONS_FILE.read_text(encoding="utf-8")
        return DECISIONS_LEGACY_FILE.read_text(encoding="utf-8")
    except OSError:
        return ""


_DECISION_LOW_PRIORITY = ("已完成", "回灌")


def decisions_for_prompt(text: str, budget: int | None = None) -> str:
    """The decision list as claim hands it to the executor. v5.0.5 sent
    text[:3000]: once the file outgrows that, the NEWEST standing rules -
    appended as their own "## ..." sections at the end - silently fall off
    (the file is at ~2300 chars and grows with every release). Over budget,
    the preamble and the open/standing sections (进行中, 已决定, every rule
    section) come first, then history sections (已完成, …回灌) newest first,
    in file order, and a last line says what was left out.
    WECHAT_BRIDGE_DECISIONS_CHARS moves the budget (default 3000)."""
    if budget is None:
        budget = _env_int("WECHAT_BRIDGE_DECISIONS_CHARS", 3000)
    if len(text) <= budget:
        return text
    chunks = re.split(r"(?m)^(?=## )", text)
    pre = "" if chunks[0].startswith("## ") else chunks.pop(0)
    pre = pre if len(pre) <= budget // 3 else pre[:budget // 3].rstrip() + "…\n"
    secs = [c for c in chunks if c]

    def title(c: str) -> str:
        return c.split("\n", 1)[0][3:].strip()

    low = [i for i, c in enumerate(secs)
           if any(w in title(c) for w in _DECISION_LOW_PRIORITY)]
    high = [i for i in range(len(secs)) if i not in low]
    room = budget - len(pre) - 120          # the omission note
    chosen: dict = {}
    for i in high + sorted(low, reverse=True):
        if len(secs[i]) <= room:
            chosen[i] = secs[i]
            room -= len(secs[i])
        elif i in high and room > 200:      # an over-long open section: cut
            chosen[i] = secs[i][:room - 20].rstrip() + "…（本节过长已截断）\n"
            room = 0
    out = pre + "".join(chosen[i] for i in sorted(chosen))
    left = [title(secs[i]) for i in range(len(secs)) if i not in chosen]
    if left:
        out = (out.rstrip("\n") + f"\n\n（另有 {len(left)} 节未带上："
               + "、".join(x[:16] for x in left[:4]) + ("等" if len(left) > 4 else "")
               + "；完整清单：python3 bridge.py decisions）\n")
    return out


MEMO_MAX_CHARS = 1500


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


HISTORY_ENABLED = os.environ.get("WECHAT_BRIDGE_HISTORY", "1") != "0"
CONTEXT_TURNS = _env_int("WECHAT_BRIDGE_CONTEXT_TURNS", 10)
CONTEXT_CHARS = _env_int("WECHAT_BRIDGE_CONTEXT_CHARS", 4000)
CONTEXT_WINDOW_MS = 24 * 3600 * 1000


def _user_tz():
    import datetime as _dt
    name = os.environ.get("WECHAT_BRIDGE_TZ", "Asia/Shanghai")
    try:
        from zoneinfo import ZoneInfo
        return ZoneInfo(name)
    except Exception:  # noqa: BLE001 - no tzdata: the owner is in UTC+8
        return _dt.timezone(_dt.timedelta(hours=8))


def user_clock(ms: int | None, with_date: bool = False) -> str:
    """Wall-clock time as the WeChat user sees it (Asia/Shanghai by
    default; the VM itself logs in UTC)."""
    import datetime as _dt
    if not ms:
        return "?"
    d = _dt.datetime.fromtimestamp(ms / 1000, _user_tz())
    if with_date:
        return d.strftime("%m-%d %H:%M")
    return d.strftime("%H:%M")


def fmt_duration(ms: int | float) -> str:
    sec = max(0, int(ms // 1000))
    if sec < 60:
        return f"{sec}秒"
    mins = sec // 60
    if mins < 60:
        return f"{mins}分钟"
    hours, mins = divmod(mins, 60)
    if hours < 48:
        return f"{hours}小时{mins}分" if mins else f"{hours}小时"
    days, hours = divmod(hours, 24)
    return f"{days}天{hours}小时" if hours else f"{days}天"


def fmt_size(n: int | None) -> str:
    n = int(n or 0)
    if n >= 1 << 20:
        return f"{n / (1 << 20):.1f} MB"
    if n >= 1 << 10:
        return f"{n / (1 << 10):.0f} KB"
    return f"{n} B"


def snippet(text: str | None, n: int = 24) -> str:
    t = " ".join(str(text or "").split())
    return t if len(t) <= n else t[:n] + "…"


def _history_keep_count(lines: list) -> int:
    """How many newest lines stay live after a rotation: at most
    HISTORY_KEEP_LINES and at most half of HISTORY_MAX_BYTES (at least one).
    2026-11-04 review: v5.0.5 kept 600 lines whatever their size; with long
    CJK replies those 600 lines were still over the cap, so EVERY later
    append re-read and rewrote the whole file (measured: 575 full rewrites
    in 900 appends, the live file stuck at 1.85 MB for a 1 MB cap)."""
    budget = max(1, HISTORY_MAX_BYTES // 2)
    used = n = 0
    for ln in reversed(lines):
        cost = len(ln.encode("utf-8", "replace")) + 1
        if n >= max(1, HISTORY_KEEP_LINES) or (n and used + cost > budget):
            break
        used += cost
        n += 1
    return n


def _append_private(path: Path, text: str) -> None:
    """Append, creating the file 0600 (no chmod-after window)."""
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND
                 | getattr(os, "O_CLOEXEC", 0), 0o600)
    with os.fdopen(fd, "a", encoding="utf-8", errors="backslashreplace") as f:
        f.write(text)


def _rotate_history() -> None:
    """Caller holds HISTORY_LOCK. 2026-11-02 六-3: archive, don't delete -
    dropped lines go to history/archive-YYYY-MM.jsonl (grouped by the line's
    own `at` month) so history --search still finds them."""
    import datetime as _dt
    lines = HISTORY_FILE.read_text(encoding="utf-8", errors="replace").splitlines()
    keep = _history_keep_count(lines)
    dropped, kept = lines[:len(lines) - keep], lines[len(lines) - keep:]
    if not dropped:
        return
    buckets: dict = {}
    for ln in dropped:
        try:
            at_ = int(json.loads(ln).get("at") or 0)
        except (ValueError, TypeError, AttributeError):
            at_ = 0
        month = (_dt.datetime.fromtimestamp(at_ / 1000, _user_tz()) if at_
                 else _dt.datetime.now(_user_tz())).strftime("%Y-%m")
        buckets.setdefault(month, []).append(ln)
    for month, ls in buckets.items():
        _append_private(HISTORY_DIR / f"archive-{month}.jsonl", "\n".join(ls) + "\n")
    atomic_write_text(HISTORY_FILE, "\n".join(kept) + "\n", fsync=False)


def history_append(role: str, text: str, *, mid: str | None = None,
                   kind: str | None = None, at: int | None = None) -> None:
    """Append one turn. Never raises: history is a convenience."""
    if not HISTORY_ENABLED or not (text or "").strip():
        return
    entry = {"at": at or now_ms(), "role": role, "id": mid, "kind": kind,
             "text": str(text)[:HISTORY_TEXT_MAX]}
    try:
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        with _flock(HISTORY_LOCK):
            _append_private(HISTORY_FILE,
                            json.dumps(entry, ensure_ascii=False) + "\n")
            if HISTORY_FILE.stat().st_size > HISTORY_MAX_BYTES:
                _rotate_history()
    except (OSError, UnicodeError) as e:
        log(f"history append failed: {type(e).__name__}")


HISTORY_SEARCH_MAX_BYTES = 32 * 1024 * 1024


def history_search(needle: str, limit: int = 5, *, skip_kinds=(),
                   max_bytes: int | None = HISTORY_SEARCH_MAX_BYTES) -> tuple:
    """(hits oldest first, truncated?): the live file newest first, then the
    archives newest month first, stopping at `limit` hits (0 = all) or after
    `max_bytes` read. v5.0.5's 「查记录」 loaded every archive ever written,
    in the poll thread, on each search."""
    needle = (needle or "").lower()
    if not needle:
        return [], False
    prefilter = not any(c in needle for c in '"\\') and all(ord(c) >= 32 for c in needle)
    try:
        archives = sorted(HISTORY_DIR.glob("archive-*.jsonl"), reverse=True)
    except OSError:
        archives = []
    hits: list = []
    scanned = 0
    order = lambda r: int(r.get("at") or 0)   # noqa: E731
    for f in [HISTORY_FILE] + archives:
        try:
            raw = f.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        scanned += len(raw)
        for line in reversed(raw.splitlines()):
            if prefilter and needle not in line.lower():
                continue
            try:
                e = json.loads(line)
            except ValueError:
                continue
            if (isinstance(e, dict) and e.get("kind") not in skip_kinds
                    and needle in str(e.get("text") or "").lower()):
                hits.append(e)
                if limit and len(hits) >= limit:
                    return sorted(hits, key=order), False
        if max_bytes is not None and scanned >= max_bytes:
            return sorted(hits, key=order), True
    return sorted(hits, key=order), False


def history_read(limit: int | None = None, since_ms: int | None = None,
                 before_ms: int | None = None) -> list[dict]:
    try:
        raw = HISTORY_FILE.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []
    out = []
    for line in raw.splitlines():
        try:
            e = json.loads(line)
        except ValueError:
            continue
        at = int(e.get("at") or 0)
        if since_ms is not None and at < since_ms:
            continue
        if before_ms is not None and at >= before_ms:
            continue
        out.append(e)
    return out[-limit:] if limit else out


HISTORY_ARCHIVE_MONTHS = _env_int("WECHAT_BRIDGE_HISTORY_ARCHIVE_MONTHS", 0)


def prune_history_archives() -> None:
    """Opt-in retention for history/archive-YYYY-MM.jsonl. Default 0 keeps
    everything (2026-11-02 六-3: archive, don't delete); N > 0 deletes the
    archives of months more than N months back."""
    if HISTORY_ARCHIVE_MONTHS <= 0:
        return
    import datetime as _dt
    now = _dt.datetime.now(_user_tz())
    cutoff = now.year * 12 + now.month - 1 - HISTORY_ARCHIVE_MONTHS
    for f in HISTORY_DIR.glob("archive-*.jsonl"):
        m = re.fullmatch(r"archive-(\d{4})-(\d{2})\.jsonl", f.name)
        if m and int(m.group(1)) * 12 + int(m.group(2)) - 1 < cutoff:
            with contextlib.suppress(OSError):
                f.unlink()
                log(f"pruned history archive {f.name}")


def history_read_all() -> list[dict]:
    """Current history + all archive-YYYY-MM.jsonl files, sorted by `at`.
    Only for history --search; conversation_context and topic digests
    keep using history_read() so a prompt never grows with age."""
    rows = history_read()
    try:
        files = sorted(HISTORY_DIR.glob("archive-*.jsonl"))
    except OSError:
        files = []
    for _af in files:
        try:
            raw = _af.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for line in raw.splitlines():
            try:
                e = json.loads(line)
            except ValueError:
                continue
            if isinstance(e, dict):
                rows.append(e)
    rows.sort(key=lambda e: int(e.get("at") or 0))
    return rows


def memo_get() -> str:
    return str((read_json(MEMO_FILE, {}) or {}).get("text") or "")


def memo_set(text: str) -> str:
    text = (text or "").strip()[:MEMO_MAX_CHARS]
    if text:
        atomic_write_json(MEMO_FILE, {"text": text, "updated_at": now_ms()})
    else:
        MEMO_FILE.unlink(missing_ok=True)
    return text


# --------------------------------------------------------------------------
# Topic summaries (2026-11-02, 三): claim only carries the last 10 turns,
# so a topic from days ago was unreachable ("那个文件" only worked same-day).
# 「新话题」 now files a one-line digest of the topic just ended (first and
# last user turns + turn count + time span), and claim carries the last 3
# digests. `history --search <词>` searches the whole kept history instead
# of only the last N turns. Digests are a convenience, never load-bearing.
# --------------------------------------------------------------------------

TOPIC_SUMMARIES_FILE = HISTORY_DIR / "topic_summaries.jsonl"
TOPIC_SUMMARIES_KEEP = 20


def topic_summaries_read() -> list[dict]:
    try:
        lines = TOPIC_SUMMARIES_FILE.read_text(
            encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []
    out = []
    for line in lines:
        try:
            e = json.loads(line)
        except ValueError:
            continue
        if isinstance(e, dict):
            out.append(e)
    return out


def topic_summary_append() -> dict | None:
    """Digest the topic segment since the last 「新话题」 marker. Returns
    the digest, or None when there is nothing worth keeping."""
    try:
        rows = history_read()
        cut = max((i for i, t in enumerate(rows) if t.get("kind") == "topic"),
                  default=-1)
        seg = [t for t in rows[cut + 1:]
               if t.get("kind") not in ("command", "topic")
               and (t.get("text") or "").strip()]
        users = [t for t in seg if t.get("role") == "user"]
        if len(seg) < 2 or not users:
            return None
        digest = {"at": now_ms(), "turns": len(seg),
                  "first": snippet(users[0].get("text"), 60),
                  "last": snippet(users[-1].get("text"), 60),
                  "span": (f"{user_clock(users[0].get('at'), with_date=True)}"
                           f"–{user_clock(users[-1].get('at'))}")}
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        kept = (topic_summaries_read() + [digest])[-TOPIC_SUMMARIES_KEEP:]
        atomic_write_text(TOPIC_SUMMARIES_FILE,
                          "".join(json.dumps(e, ensure_ascii=False) + "\n"
                                  for e in kept), fsync=False)
        return digest
    except OSError:
        return None


def _changed_since(path: Path, since_ms: int | None) -> bool:
    """True without a cursor, or if `path` was modified at/after it."""
    if not since_ms:
        return True
    try:
        return os.stat(path).st_mtime_ns // 1_000_000 >= int(since_ms)
    except OSError:
        return False


def conversation_context(before_ms: int | None,
                         since_ms: int | None = None) -> dict | None:
    """Recent turns before the claimed task(s) + the executor's memo, sized
    for an LLM prompt (turns are clipped; total bounded by CONTEXT_CHARS).
    2026-11-05: since_ms (claim --context-since <context_cursor>) gives a warm
    executor only what is NEW since its previous claim - the turns after it,
    and memo / decisions / topics only if they changed - instead of the
    whole context again on every follow-up (fewer tokens, faster answers)."""
    out: dict = {}
    if CONTEXT_TURNS > 0:
        lo = now_ms() - CONTEXT_WINDOW_MS
        rows = history_read(since_ms=max(lo, int(since_ms)) if since_ms else lo,
                            before_ms=before_ms)
        cut = max((i for i, t in enumerate(rows) if t.get("kind") == "topic"),
                  default=-1)          # v5: 「新话题」 starts a fresh context
        turns = [t for t in rows[cut + 1:]
                 if t.get("kind") not in ("command", "topic")][-CONTEXT_TURNS:]
        lines: list[str] = []
        used = 0
        for t in reversed(turns):
            who = "用户" if t.get("role") == "user" else "Muse"
            line = f"{user_clock(t.get('at'))} {who}: {snippet(t.get('text'), 400)}"
            if used + len(line) > CONTEXT_CHARS:
                break
            lines.insert(0, line)
            used += len(line)
        if lines:
            out["recent"] = lines
    memo = memo_get()
    if memo and _changed_since(MEMO_FILE, since_ms):
        out["memo"] = memo
    dec = _decisions_read()
    if dec.strip() and (_changed_since(DECISIONS_FILE, since_ms) or (
            not DECISIONS_FILE.exists()
            and _changed_since(DECISIONS_LEGACY_FILE, since_ms))):
        # opt batch item 2: decided matters first, do not re-ask
        out["decisions"] = decisions_for_prompt(dec)
    topics = topic_summaries_read()[-3:]
    if since_ms:
        topics = [t for t in topics if int(t.get("at") or 0) >= int(since_ms)]
    if topics:
        out["past_topics"] = [
            f"{t.get('span', '')} 约 {t.get('turns', 0)} 轮："
            f"「{t.get('first', '')}」…「{t.get('last', '')}」"
            for t in topics]
    return out or None



# --------------------------------------------------------------------------
# Quote store (v5). Newer WeChat clients may quote a message by its server id
# alone (ref_msg.svr_id, plus ref_msg.partial_text for a selected substring)
# without its text or media; the official plugin (2.4.9+) keeps a SQLite side
# store to restore them. v4 read only the inline quote, so such a quote
# reached the executor empty. Stored here: inbound messages (by message_id)
# and every delivered bot message (by the message_id sendmessage returns):
# text, kind and - for inbound media - the downloaded file, so "这张图里写的
# 什么" about a picture sent days ago just works. 30 days / 10,000 rows like
# upstream; never raises. WECHAT_BRIDGE_QUOTES=0 disables it.
# --------------------------------------------------------------------------

QUOTE_DB = HISTORY_DIR / "quotes.db"
QUOTES_ENABLED = os.environ.get("WECHAT_BRIDGE_QUOTES", "1") != "0"
QUOTE_RETENTION_MS = 30 * 24 * 3600 * 1000
QUOTE_MAX_ROWS = 10_000
QUOTE_TEXT_MAX = 8000
_QUOTE_KEYS = ("id", "user", "dir", "kind", "text", "media", "name", "task", "at")


def _quote_db():
    HISTORY_DIR.mkdir(parents=True, exist_ok=True)
    fresh = not QUOTE_DB.exists()
    conn = sqlite3.connect(str(QUOTE_DB), timeout=5)
    conn.execute("CREATE TABLE IF NOT EXISTS msgs (id TEXT PRIMARY KEY, user TEXT, "
                 "dir TEXT, kind TEXT, text TEXT, media TEXT, name TEXT, task TEXT, "
                 "at INTEGER)")
    # v1.7: the quoted-text fallback reads one user's recent rows
    conn.execute("CREATE INDEX IF NOT EXISTS msgs_user_at ON msgs (user, at)")
    if fresh:
        with contextlib.suppress(OSError):
            os.chmod(QUOTE_DB, 0o600)
    return conn


def _quote_exec(sql: str, args: tuple = (), fetch: bool = False):
    if not QUOTES_ENABLED:
        return None
    try:
        conn = _quote_db()
        try:
            with conn:
                cur = conn.execute(sql, args)
                return cur.fetchall() if fetch else None
        finally:
            conn.close()
    except sqlite3.Error as e:
        log(f"quote store error: {type(e).__name__}")
        return None


def quote_put(mid, *, user=None, direction="in", kind="text", text="",
              media=None, name=None, task=None) -> None:
    if mid:
        _quote_exec("INSERT OR REPLACE INTO msgs VALUES (?,?,?,?,?,?,?,?,?)",
                    (str(mid), user, direction, kind, str(text or "")[:QUOTE_TEXT_MAX],
                     media, name, task, now_ms()))


def quote_set_media(mid, path, name=None) -> None:
    if mid and path:
        # v1.7: also the item-id aliases of the same inbound message
        _quote_exec("UPDATE msgs SET media=?, name=COALESCE(?, name) WHERE id=? "
                    "OR (task=? AND dir='in')", (str(path), name, str(mid), str(mid)))


def quote_get(mid) -> dict | None:
    if not mid or not QUOTE_DB.exists():
        return None
    rows = _quote_exec("SELECT id,user,dir,kind,text,media,name,task,at FROM msgs "
                       "WHERE id=?", (str(mid),), fetch=True)
    if not rows:
        return None
    rec = dict(zip(_QUOTE_KEYS, rows[0]))
    return rec if now_ms() - int(rec.get("at") or 0) <= QUOTE_RETENTION_MS else None


def quote_count() -> int:
    if not QUOTE_DB.exists():
        return 0
    rows = _quote_exec("SELECT COUNT(*) FROM msgs", fetch=True)
    return int(rows[0][0]) if rows else -1


def quote_prune() -> None:
    if QUOTE_DB.exists():
        _quote_exec("DELETE FROM msgs WHERE at < ?", (now_ms() - QUOTE_RETENTION_MS,))
        _quote_exec("DELETE FROM msgs WHERE id IN (SELECT id FROM msgs ORDER BY at "
                    "DESC LIMIT -1 OFFSET ?)", (QUOTE_MAX_ROWS,))


def _prune_partials() -> None:
    """Partial downloads kept for a Range resume, abandoned for a day."""
    cutoff = time.time() - 86_400
    for d_ in (MEDIA_DIR, FILES_DIR):
        with contextlib.suppress(OSError):
            for f in d_.glob(".*.part"):
                if f.stat().st_mtime < cutoff:
                    f.unlink()


def _partial_quote(full: str, p: dict) -> str | None:
    """The selected substring of a partly quoted message. Indices may count
    code points or JS UTF-16 units and the end may be inclusive: the candidate
    whose md5 matches quotemd5 wins. No match: the client's own start/end
    snippets - never an invented text."""
    try:
        si, ei = int(p.get("startindex") or 0), int(p.get("endindex") or 0)
    except (TypeError, ValueError):
        si = ei = 0
    want = str(p.get("quotemd5") or "").lower()
    cands = []
    if full and 0 <= si <= ei:
        cands += [full[si:ei], full[si:ei + 1]]
        u = full.encode("utf-16-le")
        for end in (ei, ei + 1):
            if end * 2 <= len(u):
                cands.append(u[si * 2:end * 2].decode("utf-16-le", "ignore"))
    start, end_s = str(p.get("start") or ""), str(p.get("end") or "")
    if full and start and end_s:
        i = full.find(start)
        j = full.find(end_s, max(i, 0))
        if i >= 0 and j >= 0:
            cands.append(full[i:j + len(end_s)])
    for c in cands:
        if c and want and hashlib.md5(c.encode("utf-8")).hexdigest() == want:
            return c
    if not want:
        for c in cands:
            if c:
                return c
    if start or end_s:
        return f"{start}…{end_s}" if start and end_s and start != end_s else (start or end_s)
    return None


def _ref_ids(ref: dict) -> list:
    """Every id a quote may name the quoted message by, best first (v1.7):
    ref_msg.svr_id (official 2.4.9+ ID-only quotes); message_item.msg_id - the
    official inbound.ts falls back to it when there is no svr_id; then
    message_item.message_id / svr_id, message_item.extra.msg_id and a
    RefMessage-level msg_id (what ilink-hub's quote_route.rs and wechat-ilink
    read). v1.6 read svr_id only, so an inline quote that names its message
    by item id never reached the quote store or the background routing.
    Ids are uint64 on the wire: json gives exact Python ints; a float has
    already lost digits and is skipped, as are 0 and booleans."""
    out = []
    inline = ref.get("message_item") if isinstance(ref.get("message_item"), dict) else {}
    extra = inline.get("extra") if isinstance(inline.get("extra"), dict) else {}
    for v in (ref.get("svr_id"), inline.get("msg_id"), inline.get("message_id"),
              inline.get("svr_id"), extra.get("msg_id"), ref.get("msg_id")):
        if v is None or isinstance(v, (bool, float, dict, list)):
            continue
        s = str(v).strip()
        if s and s != "0" and s not in out:
            out.append(s)
    return out


def resolve_quote(items: list) -> dict:
    """{"text": "title | body" | None, "media": stored record with a local
    file | None, "svr_id", "ids", "from_store"}: inline quote first (older
    clients), then the local store by id, then partial_text. v1.7: every id
    the quote carries is tried (_ref_ids); "svr_id" is the first of them."""
    for it in items:
        ref = it.get("ref_msg")
        if not isinstance(ref, dict):
            continue
        title = str(ref.get("title") or "").strip()
        inline = ref.get("message_item") if isinstance(ref.get("message_item"), dict) else {}
        body = _item_text(inline) or _MEDIA_LABELS.get(inline.get("type"), "")
        ids = _ref_ids(ref)
        svr = ids[0] if ids else ""
        partial = ref.get("partial_text") if isinstance(ref.get("partial_text"), dict) else None
        rec = None
        for qid in ids:
            rec = quote_get(qid)
            if rec:
                break
        media = None
        if rec:
            full = str(rec.get("text") or "")
            if partial:
                body = _partial_quote(full, partial) or body or full
            elif not body:
                body = full
            if rec.get("media") and rec.get("kind") in KIND_LABELS \
                    and Path(rec["media"]).is_file():
                media = rec
        elif partial and not body:
            body = _partial_quote("", partial) or ""
        elif svr and not body:
            body = "（引用了一条桥接本地没有记录的消息，内容未知）"
        parts = [title] if title else []
        if body and body != title:
            parts.append(body)
        if parts:
            return {"text": " | ".join(parts)[:2000], "media": media,
                    "svr_id": svr or None, "ids": ids, "from_store": bool(rec)}
    return {"text": None, "media": None, "svr_id": None, "ids": [], "from_store": False}


def list_tasks() -> list[dict]:
    tasks = []
    for p in sorted(INBOX_DIR.glob("*.json")):
        t = read_json(p)
        if t and t.get("message_id"):
            tasks.append(t)
    return tasks


# Cheap task listing for the hot paths (sender loop runs every second,
# claim/status on demand). list_tasks() above opens and parses every inbox
# file; with a month of history that was two full parses per second,
# forever. This index only re-parses files whose (inode, mtime, size)
# changed, and when the directory itself has been quiet for 2s it skips
# even the per-file stat. Entries hold only the fields callers need.
SCAN_REVALIDATE_SEC = 300       # periodic full re-parse as a safety net
_SCAN_CACHE: dict[str, tuple] = {}
_scan_state: dict = {"dir_mtime": None, "out": [], "full_at": 0.0}
_scan_lock = threading.Lock()


def _settled_ns(mtime_ns: int, now_ns: int) -> bool:
    """v1.7: may a (inode, mtime, size) signature taken now stand for the
    content? Only if no later write can still carry the same mtime: file
    times come from the kernel's coarse clock (one tick, 1-10 ms), so 100 ms
    after the mtime every later write has a later one. A filesystem that
    keeps whole seconds only (mtime with no sub-second part) needs 2 s."""
    return now_ns - mtime_ns > (2_000_000_000 if mtime_ns % 1_000_000_000 == 0
                                else 100_000_000)
_SEND_KEYS = ("attempts", "no_auto_retry", "next_retry_at", "started_at",
              "max_attempts", "sender_pid", "progress_at", "notified",
              "exhausted", "not_delivered", "deferred", "notify_after")


def _task_summary(t: dict) -> dict:
    send = t.get("send") or {}
    ex = t.get("execution") or {}
    return {
        "message_id": t["message_id"],
        "status": t.get("status"),
        "kind": t.get("kind"),
        "from_user_id": t.get("from_user_id"),
        "received_at": t.get("received_at") or 0,
        "pending_at": t.get("pending_at") or 0,
        "claimed_at": ex.get("claimed_at"),
        "heartbeat_at": ex.get("heartbeat_at"),
        "overdue_notified": bool(t.get("overdue_notified")),
        "send": {k: send[k] for k in _SEND_KEYS if k in send},
        # 2026-11-06: batching (pending.json) and the claim group (one batch
        # = one claim_id = one round: heartbeats / auto-progress per group)
        "batch_id": t.get("batch_id"),
        "batch_open": bool(t.get("batch_open")),
        "dispatched_at": t.get("dispatched_at") or 0,
        "claim_id": ex.get("claim_id"),
        "hold_until": ex.get("hold_until") or 0,
        **({"relay_pending": True} if t.get("relay_pending") else {}),
        # v1.7: any quote (id or text) may be a follow-up for a background task
        **({"quoted": True} if (t.get("quote_svr_id") or t.get("quote_ids")
                                or t.get("quote")) else {}),
        **({"essential": True} if ex.get("essential") else {}),
    }


# v1.7: the summaries also persist in state/task_index.json, so a CLI process
# (claim / progress / complete / heartbeat - several per message, each a new
# process) parses only the task files that changed since serve last wrote it,
# not all of them: with a month of history (~1500 files) the first scan of
# every CLI call cost ~80-100 ms. Same idea as git's index: a pure cache,
# every entry checked against the file's (inode, mtime, size), only settled
# signatures stored (_settled_ns); missing, corrupt or another schema = not
# used. serve rewrites it at most once a minute; a scan that had to parse
# TASK_INDEX_SAVE_AFTER files rewrites it at once. Not in backups.
TASK_INDEX_FILE = STATE_DIR / "task_index.json"
TASK_INDEX_SCHEMA = "v1.7-1"        # bump whenever _task_summary changes
TASK_INDEX_SAVE_AFTER = 64
TASK_INDEX_ON = os.environ.get("WECHAT_BRIDGE_TASK_INDEX", "1").strip() != "0"
_tix = {"loaded": False, "dirty": 0}


def _task_index_load() -> bool:
    """Caller holds _scan_lock. Seeds _SCAN_CACHE from the persisted index."""
    _tix["loaded"] = True
    if not TASK_INDEX_ON:
        return False
    d = read_json(TASK_INDEX_FILE, None)
    if not isinstance(d, dict) or d.get("schema") != TASK_INDEX_SCHEMA \
            or not isinstance(d.get("files"), dict):
        return False
    for name, v in d["files"].items():
        if isinstance(v, list) and len(v) == 4 and name not in _SCAN_CACHE:
            with contextlib.suppress(TypeError, ValueError):
                _SCAN_CACHE[name] = ((int(v[0]), int(v[1]), int(v[2])),
                                     v[3] if isinstance(v[3], dict) else None)
    return True


def _task_index_write_locked() -> None:
    files = {n: [sig[0], sig[1], sig[2], summ] for n, (sig, summ) in _SCAN_CACHE.items() if sig}
    _tix["dirty"] = 0
    with contextlib.suppress(OSError):
        _atomic_write_bytes(TASK_INDEX_FILE, json.dumps(
            {"schema": TASK_INDEX_SCHEMA, "files": files}, ensure_ascii=False,
            separators=(",", ":")).encode("utf-8", "backslashreplace"), 0o600, fsync=False)


def task_index_persist() -> None:
    """serve (sender loop, once a minute): write the index if anything changed."""
    if TASK_INDEX_ON:
        with _scan_lock:
            if _tix["dirty"]:
                _task_index_write_locked()


def scan_task_index() -> list[dict]:
    """Summaries of all tasks, oldest first. Callers must not mutate them
    and must reload the real task (under task_lock) before acting."""
    with _scan_lock:
        try:
            dst = os.stat(INBOX_DIR)
        except OSError:
            _SCAN_CACHE.clear()
            _scan_state.update(dir_mtime=None, out=[])
            return []
        now_m = time.monotonic()
        if not _tix["loaded"] and _task_index_load():
            _scan_state["full_at"] = now_m      # entries are checked by signature below
        revalidate = now_m - _scan_state["full_at"] > SCAN_REVALIDATE_SEC
        settled = time.time_ns() - dst.st_mtime_ns > 2_000_000_000
        if (not revalidate and settled
                and dst.st_mtime_ns == _scan_state["dir_mtime"]):
            return list(_scan_state["out"])
        out, seen = [], set()
        now_ns = time.time_ns()
        try:
            it = os.scandir(INBOX_DIR)
        except OSError:
            return list(_scan_state["out"])
        with it:
            for e in it:
                name = e.name
                if not name.endswith(".json"):
                    continue
                try:
                    st = e.stat()
                except OSError:
                    continue
                seen.add(name)
                sig = (st.st_ino, st.st_mtime_ns, st.st_size)
                hit = _SCAN_CACHE.get(name)
                if hit and hit[0] == sig and not revalidate:
                    summ = hit[1]
                else:
                    t = read_json(INBOX_DIR / name)
                    summ = (_task_summary(t)
                            if isinstance(t, dict) and t.get("message_id")
                            else None)
                    # v1.7: a file written within one mtime tick of now is
                    # parsed again next time - two rewrites in one tick can
                    # reuse an inode and keep the size, so its signature
                    # alone could pin a stale summary (_settled_ns)
                    _SCAN_CACHE[name] = (sig if _settled_ns(st.st_mtime_ns, now_ns)
                                         else None, summ)
                    if _SCAN_CACHE[name][0]:
                        _tix["dirty"] += 1
                if summ:
                    out.append(summ)
        for name in [n for n in _SCAN_CACHE if n not in seen]:
            del _SCAN_CACHE[name]
            _tix["dirty"] += 1
        if TASK_INDEX_ON and _tix["dirty"] >= TASK_INDEX_SAVE_AFTER:
            _task_index_write_locked()
        out.sort(key=lambda x: (x["received_at"], str(x["message_id"])))
        # v1.7: the directory fast path only trusts an mtime that had already
        # settled when this pass ran (a rename in the same tick keeps it)
        _scan_state.update(dir_mtime=dst.st_mtime_ns if _settled_ns(dst.st_mtime_ns, now_ns)
                           else None, out=out)
        if revalidate:
            _scan_state["full_at"] = now_m
        return list(out)


HB_LOCK_FILE = STATE_DIR / "heartbeat.lock"


def update_heartbeat(**fields) -> None:
    """Merge fields into heartbeat.json. Serialized with flock (receiver
    thread, sender thread and CLI processes all write it, and the plain
    read-modify-write used to lose updates) and never raises: telemetry
    must not be able to take the poll loop down. No fsync: it is rewritten
    constantly and a lost beat after power loss is harmless."""
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        with open(HB_LOCK_FILE, "a+") as lk:
            fcntl.flock(lk, fcntl.LOCK_EX)
            try:
                hb = read_json(HEARTBEAT_FILE, {}) or {}
                hb.update(fields)
                atomic_write_json(HEARTBEAT_FILE, hb, fsync=False)
            finally:
                fcntl.flock(lk, fcntl.LOCK_UN)
    except OSError as e:
        log(f"heartbeat write failed: {type(e).__name__}")


# --------------------------------------------------------------------------
# Inbound media (official CDN flow: src/cdn/pic-decrypt.ts, media-download.ts)
# --------------------------------------------------------------------------

CDN_BASE_URL = "https://novac2c.cdn.weixin.qq.com/c2c"
MEDIA_MAX_BYTES = 25 * 1024 * 1024
MEDIA_TIMEOUT_SEC = 45
FILE_RECV_MAX_BYTES = 100 * 1024 * 1024   # v4: streamed, so no RAM concern
VOICE_RECV_MAX_BYTES = 5 * 1024 * 1024


class MediaError(ApiError):
    """A deterministic inbound-media failure. `reason` drives the explicit
    message the user gets (too_large / decrypt / no_ref / md5 / empty);
    being kind=business it is never retried as a network blip."""

    def __init__(self, reason: str, message: str, **info):
        super().__init__("business", message)
        self.reason = reason
        self.info = info


def _validate_server_url(url: str, what: str) -> str:
    """Allowlist for URLs the server hands us (media downloads, CDN
    uploads): https on Tencent CDN suffixes (qq.com, qpic.cn, qlogo.cn)
    only. The server is trusted, but a compromised or spoofed response
    must not turn the bridge into an SSRF client against arbitrary hosts.
    Tests may set WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL=1 to allow loopback URLs."""
    parts = urllib.parse.urlsplit(url or "")
    host = (parts.hostname or "").lower()
    if parts.scheme == "https" and any(
            host == suf or host.endswith("." + suf)
            for suf in ("qq.com", "qpic.cn", "qlogo.cn")):
        return url
    if (os.environ.get("WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL") == "1"
            and host in ("127.0.0.1", "::1", "localhost")):
        return url
    raise MediaError("bad_url",
                     f"{what} URL not allowed: {parts.scheme}://{host}")


def _media_url(media: dict, what: str) -> str:
    url = media.get("full_url")
    if url:
        return _validate_server_url(url, what)
    eqp = media.get("encrypt_query_param")
    if not eqp:
        raise MediaError("no_ref", f"{what} item has no CDN reference")
    return (f"{CDN_BASE_URL}/download?encrypted_query_param="
            f"{urllib.parse.quote(eqp, safe='')}")


def _int_or_none(v) -> int | None:
    try:
        n = int(v)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _parse_aes_key(image_item: dict) -> bytes | None:
    """AES-128 key for an inbound image: image_item.aeskey (hex) is
    preferred; otherwise media.aes_key is base64 of 16 raw bytes, or
    base64 of a 32-char hex string (pic-decrypt.ts parseAesKey)."""
    aeskey_hex = image_item.get("aeskey")
    if aeskey_hex:
        try:
            key = bytes.fromhex(aeskey_hex)
            if len(key) == 16:
                return key
        except ValueError:
            pass
    aes_key_b64 = ((image_item.get("media") or {}).get("aes_key")) or ""
    if aes_key_b64:
        # Some payloads carry the hex key string directly, unwrapped.
        if len(aes_key_b64) == 32 and all(
                c in "0123456789abcdefABCDEF" for c in aes_key_b64):
            return bytes.fromhex(aes_key_b64)
        try:
            decoded = base64.b64decode(aes_key_b64)
        except Exception:
            return None
        if len(decoded) == 16:
            return decoded
        if len(decoded) == 32:
            try:
                return bytes.fromhex(decoded.decode("ascii"))
            except Exception:
                return None
    return None



def _content_length(resp) -> int | None:
    try:
        v = str(resp.getheader("Content-Length") or "")
    except Exception:  # noqa: BLE001 - test doubles / exotic responses
        return None
    return int(v) if v.isdigit() else None


def _fetch_bytes(url: str, timeout: int = MEDIA_TIMEOUT_SEC,
                 max_bytes: int = MEDIA_MAX_BYTES) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "Muse/1.0"})
    with PROXY_POOL.open(req, timeout=timeout) as resp:
        data = resp.read(max_bytes + 1)
        expect = _content_length(resp)
    if len(data) > max_bytes:
        raise MediaError("too_large", "media exceeds size cap", cap=max_bytes)
    if expect is not None and len(data) < expect:
        # v5: see _fetch_to_file - a cut-off body is a network error, not data
        raise http.client.IncompleteRead(data, expect - len(data))
    return data


def _range_starts_at(resp, offset: int) -> bool:
    try:
        v = str(resp.getheader("Content-Range") or "")
    except Exception:  # noqa: BLE001 - test doubles / exotic responses
        return False
    m = re.match(r"\s*bytes\s+(\d+)-(\d+)/(\d+|\*)\s*$", v)
    return bool(m) and int(m.group(1)) == offset


def _fetch_to_file(url: str, dst: Path, timeout: int,
                   max_bytes: int, resume: bool | None = None) -> int:
    """Stream a download to disk in 1MB chunks (constant memory).
    v5: resume=True continues a partial `dst` with an HTTP Range request - a
    dropped connection on a big video no longer restarts at byte 0 (v4's six
    retries all started over inside the same burst of drops). A server that
    ignores Range (200 instead of 206) gets a fresh download; 416 as well.
    resume=None (default): resume exactly when `dst` is a `.part` file."""
    if resume is None:
        resume = dst.name.endswith(".part")
    have = 0
    if resume:
        with contextlib.suppress(OSError):
            have = dst.stat().st_size
    headers = {"User-Agent": "Muse/1.0"}
    if have:
        headers["Range"] = f"bytes={have}-"
    req = urllib.request.Request(url, headers=headers)
    try:
        resp = PROXY_POOL.open(req, timeout=timeout)
    except urllib.error.HTTPError as e:
        if have and e.code == 416:
            dst.unlink(missing_ok=True)
            return _fetch_to_file(url, dst, timeout, max_bytes)
        raise
    partial = bool(have) and getattr(resp, "status", 200) == 206
    if partial and not _range_starts_at(resp, have):
        # 2026-11-04 review: a 206 that does not continue exactly at our
        # offset would splice the wrong bytes in - and ECB decryption cannot
        # notice a misplaced middle block. Start over instead.
        with contextlib.suppress(Exception):
            resp.close()
        log("download: 206 with an unexpected Content-Range; restarting at byte 0")
        dst.unlink(missing_ok=True)
        return _fetch_to_file(url, dst, timeout, max_bytes, resume=False)
    with resp:
        total = have if partial else 0
        if partial:
            _NET_STATS["resumed"] += 1
        expect, got = _content_length(resp), 0
        with open(dst, "ab" if partial else "wb") as f:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                got += len(chunk)
                total += len(chunk)
                if total > max_bytes:
                    raise MediaError("too_large", "media exceeds size cap",
                                     cap=max_bytes)
                f.write(chunk)
        if expect is not None and got < expect:
            # http.client's read(n) returns b"" on a premature EOF instead of
            # raising. v4 took a cut-off download for a complete one; the
            # truncated ciphertext then failed to decrypt and the user was told
            # "WeChat CDN decrypt problem" (wrong, and never retried). Now it
            # is a transient network error: retried, resumed with Range.
            raise http.client.IncompleteRead(b"", expect - got)
    return total


def _fetch_decrypt(url: str, key: bytes, tmp_path: Path, plain_path: Path,
                   max_bytes: int, cap: int) -> None:
    """Download (resumable) + file-to-file decrypt. A transport failure
    keeps the partial ciphertext for a Range resume on the next attempt; a
    resumed copy that does not decrypt is fetched once more from byte 0."""
    try:
        r0 = _NET_STATS["resumed"]
        _fetch_to_file(url, tmp_path, timeout=120, max_bytes=max_bytes)
        try:
            _decrypt_aes_ecb_file(tmp_path, key, plain_path)
        except MediaError:
            if _NET_STATS["resumed"] == r0:
                raise
            tmp_path.unlink(missing_ok=True)
            _fetch_to_file(url, tmp_path, timeout=120, max_bytes=max_bytes)
            _decrypt_aes_ecb_file(tmp_path, key, plain_path)
    except MediaError as e:
        plain_path.unlink(missing_ok=True)
        tmp_path.unlink(missing_ok=True)
        if e.reason == "too_large":
            e.info.setdefault("cap", cap)
        raise
    except BaseException as e:
        plain_path.unlink(missing_ok=True)
        if not _transient_net_error(e):
            tmp_path.unlink(missing_ok=True)
        raise
    tmp_path.unlink(missing_ok=True)


def _decrypt_aes_ecb_file(src: Path, key: bytes, dst: Path) -> None:
    """File-to-file AES-128-ECB decrypt (streaming, so a
    100MB video never sits in RAM three times over). Raises
    MediaError("decrypt") on bad ciphertext, like _decrypt_aes_ecb;
    disk errors (OSError) pass through unchanged."""
    try:
        _aes_stream(src, key, dst, False)
    except (MediaError, OSError):
        raise
    except Exception as e:  # noqa: BLE001 - cryptography raises ValueError
        raise MediaError("decrypt", "media decrypt failed") from e


def _md5_file(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _decrypt_aes_ecb(data: bytes, key: bytes) -> bytes:
    """AES-128-ECB with PKCS7 unpadding. Raises MediaError on failure."""
    try:
        return _aes_bytes(data, key, False)
    except MediaError:
        raise
    except Exception as e:  # noqa: BLE001
        raise MediaError("decrypt", "media decrypt failed") from e


def _sniff_image_ext(data: bytes) -> str:
    if data[:3] == b"\xff\xd8\xff":
        return ".jpg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return ".gif"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return ".webp"
    if data[4:8] == b"ftyp":
        return ".heic"
    return ".bin"


def download_image(image_item: dict, mid: str) -> dict:
    """Download (and decrypt) an inbound image to MEDIA_DIR.
    Returns {"media_path", "media_bytes"}; raises ApiError on failure."""
    mid = safe_name(mid)
    url = _media_url(image_item.get("media") or {}, "image")
    raw = _fetch_bytes(url)
    key = _parse_aes_key(image_item)
    data = _decrypt_aes_ecb(raw, key) if key else raw
    if not data:
        raise MediaError("empty", "image decrypt produced no data")
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    path = MEDIA_DIR / f"{mid}{_sniff_image_ext(data)}"
    _atomic_write_bytes(path, data, 0o600, fsync=False)
    return {"media_path": str(path), "media_bytes": len(data)}


def _sanitize_filename(name: str) -> str:
    base = (name or "").replace("\\", "/").split("/")[-1].strip()
    base = "".join(c for c in base
                   if c not in '<>:"|?*' and ord(c) >= 32).strip()
    return base[:120] or "file.bin"


def download_file(file_item: dict, mid: str) -> dict:
    """Download (and decrypt) an inbound file attachment to FILES_DIR.
    Mirrors the FILE branch of media-download.ts (media.aes_key required,
    payload always AES-128-ECB). v4: streamed like video (disk -> openssl
    file-to-file) instead of holding ciphertext and plaintext in RAM, and
    the declared size (file_item.len) is checked BEFORE downloading, so an
    oversized file is refused at once with its real size.
    Returns {"media_path", "media_bytes", "file_name"}."""
    mid = safe_name(mid)
    url = _media_url(file_item.get("media") or {}, "file")
    key = _parse_aes_key(file_item)
    if key is None:
        raise MediaError("no_ref", "file item has no usable aes_key")
    fname = _sanitize_filename(file_item.get("file_name") or "")
    declared = _int_or_none(file_item.get("len"))
    if declared and declared > FILE_RECV_MAX_BYTES:
        raise MediaError("too_large", f"file declares {declared} bytes",
                         size=declared, cap=FILE_RECV_MAX_BYTES)
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = FILES_DIR / f".{mid}.part"
    plain_path = FILES_DIR / f".{mid}.plain"
    _fetch_decrypt(url, key, tmp_path, plain_path, FILE_RECV_MAX_BYTES + 16,
                   FILE_RECV_MAX_BYTES)
    size = plain_path.stat().st_size if plain_path.exists() else 0
    if not size:
        plain_path.unlink(missing_ok=True)
        raise MediaError("empty", "file decrypt produced no data")
    md5_want = str(file_item.get("md5") or "").lower()
    md5_ok = None
    if re.fullmatch(r"[0-9a-f]{32}", md5_want):
        # 2026-11-04 review, soft check: ECB + padding only proves the LAST
        # block, so a misplaced resumed chunk would decrypt "fine". The flag
        # reaches the executor; the file is kept either way.
        md5_ok = _md5_file(plain_path) == md5_want
        if not md5_ok:
            log(f"file md5 mismatch after decrypt id={mid} (kept, flagged)")
    path = FILES_DIR / f"{mid}__{fname}"
    os.replace(plain_path, path)
    os.chmod(path, 0o600)
    out = {"media_path": str(path), "media_bytes": size, "file_name": fname}
    if md5_ok is not None:
        out["file_md5_ok"] = md5_ok
    return out


def download_video(video_item: dict, mid: str) -> dict:
    """Download (and decrypt) an inbound video to MEDIA_DIR.
    Mirrors download_file (media.aes_key required, payload always
    AES-128-ECB encrypted), plus two video extras: the item carries a
    plaintext video_md5 we verify after decryption, and an encrypted
    thumb_media poster frame (same key) saved alongside best-effort.
    Returns {"media_path", "media_bytes", "play_length", ...}."""
    mid = safe_name(mid)
    url = _media_url(video_item.get("media") or {}, "video")
    key = _parse_aes_key(video_item)
    if key is None:
        raise MediaError("no_ref", "video item has no usable aes_key")
    declared = _int_or_none(video_item.get("video_size"))
    if declared and declared > VIDEO_RECV_MAX_BYTES + 16:
        raise MediaError("too_large", f"video declares {declared} bytes",
                         size=declared, cap=VIDEO_RECV_MAX_BYTES)
    # Streamed end to end (disk -> openssl file-to-file -> streamed
    # md5): a 100MB video must never be held in RAM on this small VM.
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = MEDIA_DIR / f".{mid}.part"
    plain_path = MEDIA_DIR / f"{mid}.plain"
    _fetch_decrypt(url, key, tmp_path, plain_path, VIDEO_RECV_MAX_BYTES,
                   VIDEO_RECV_MAX_BYTES)
    size = plain_path.stat().st_size if plain_path.exists() else 0
    if not size:
        plain_path.unlink(missing_ok=True)
        raise MediaError("empty", "video decrypt produced no data")
    md5_expect = (video_item.get("video_md5") or "").lower()
    md5_ok = None
    if md5_expect:
        md5_ok = _md5_file(plain_path) == md5_expect
        if not md5_ok:
            plain_path.unlink(missing_ok=True)
            raise MediaError("md5", "video md5 mismatch after decrypt")
    with open(plain_path, "rb") as f:
        head = f.read(16)
    ext = ".mp4" if head[4:8] == b"ftyp" else ".bin"
    path = MEDIA_DIR / f"{mid}{ext}"
    os.replace(plain_path, path)
    os.chmod(path, 0o600)
    out = {"media_path": str(path), "media_bytes": size,
           "play_length": video_item.get("play_length"),
           "video_md5_ok": md5_ok}
    thumb = video_item.get("thumb_media") or {}
    if thumb.get("encrypt_query_param") or thumb.get("full_url"):
        try:
            turl = (_validate_server_url(thumb["full_url"], "thumb")
                    if thumb.get("full_url") else
                    f"{CDN_BASE_URL}/download?encrypted_query_param="
                    f"{urllib.parse.quote(thumb.get('encrypt_query_param') or '', safe='')}")
            traw = _fetch_bytes(turl)
            tdata = _decrypt_aes_ecb(traw, key)
            if tdata:
                tpath = MEDIA_DIR / f"{mid}_thumb.jpg"
                _atomic_write_bytes(tpath, tdata, 0o600, fsync=False)
                out["thumb_path"] = str(tpath)
        except Exception as e:  # noqa: BLE001 - poster is best-effort
            log(f"video thumb download failed id={mid}: "
                f"{type(e).__name__}: {_scrub(e)}")
    return out


_VOICE_ENCODE_EXT = {1: "pcm", 2: "adpcm", 3: "feature", 4: "speex",
                     5: "amr", 6: "silk", 7: "mp3", 8: "ogg"}


def _sniff_audio(data: bytes, encode_type) -> str:
    if data[:12].lstrip(b"\x02").startswith(SILK_STD_MAGIC):
        return "silk"
    if data.startswith(b"#!AMR"):
        return "amr"
    if data[:3] == b"ID3" or data[:2] in (b"\xff\xfb", b"\xff\xf3",
                                          b"\xff\xf2"):
        return "mp3"
    if data[:4] == b"OggS":
        return "ogg"
    if data[:4] == b"RIFF" and data[8:12] == b"WAVE":
        return "wav"
    return _VOICE_ENCODE_EXT.get(encode_type, "bin")


def download_voice(voice_item: dict, mid: str) -> dict:
    """v4: a voice note WITHOUT WeChat's transcript (process-message.ts
    downloads those too). Saved raw; SILK is decoded to WAV when a SILK
    module is installed, amr/mp3/ogg via ffmpeg, so the executor can run
    speech-to-text. voice_wav=false + voice_note says why it could not."""
    mid = safe_name(mid)
    url = _media_url(voice_item.get("media") or {}, "voice")
    key = _parse_aes_key(voice_item)
    raw = _fetch_bytes(url, max_bytes=VOICE_RECV_MAX_BYTES)
    data = _decrypt_aes_ecb(raw, key) if key else raw
    if not data:
        raise MediaError("empty", "voice decrypt produced no data")
    fmt = _sniff_audio(data, voice_item.get("encode_type"))
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = MEDIA_DIR / f"{mid}.{fmt}"
    _atomic_write_bytes(raw_path, data, 0o600, fsync=False)
    playtime = _int_or_none(voice_item.get("playtime"))
    out = {"media_path": str(raw_path), "media_bytes": len(data),
           "voice_format": fmt, "voice_wav": fmt == "wav",
           "play_length": round(playtime / 1000, 1) if playtime else None}
    wav = MEDIA_DIR / f"{mid}.wav"
    if fmt == "silk":
        try:
            info = silk_to_wav(raw_path, wav)
            out.update(media_path=str(wav), voice_raw_path=str(raw_path),
                       voice_wav=True)
            if not out["play_length"]:
                out["play_length"] = round(info["playtime_ms"] / 1000, 1)
        except VoiceCodecError as e:
            out["voice_note"] = e.code
    elif fmt in ("amr", "mp3", "ogg", "speex"):
        if audio_to_wav(raw_path, wav):
            out.update(media_path=str(wav), voice_raw_path=str(raw_path),
                       voice_wav=True)
        else:
            out["voice_note"] = "no_ffmpeg" if not ffmpeg_path() else "bad_audio"
    if wav.exists():
        os.chmod(wav, 0o600)
    return out


def _split_media_items(items: list) -> tuple:
    image_item = next(((it.get("image_item") or {}) for it in items
                       if it.get("type") == 2), None)
    file_item = next(((it.get("file_item") or {}) for it in items
                      if it.get("type") == 4), None)
    video_item = next(((it.get("video_item") or {}) for it in items
                       if it.get("type") == 5), None)
    return image_item, file_item, video_item


VOICE_TEXT_PREFIX = "[语音转文字] "
_MEDIA_LABELS = {2: "[图片]", 3: "[语音]", 4: "[文件]", 5: "[视频]"}
_KIND_FIELDS = {"image": "image_item", "video": "video_item",
                "file": "file_item", "voice": "voice_item"}
_KIND_BY_TYPE = {2: "image", 5: "video", 4: "file", 3: "voice"}
KIND_LABELS = {"image": "图片", "video": "视频", "file": "文件", "voice": "语音"}
# v4: up to ~2 minutes of retries (the egress drops connections in bursts
# that outlast v3's 2 s + 5 s), then an explicit message instead of silence.
MEDIA_DOWNLOAD_ATTEMPTS = 6
MEDIA_RETRY_DELAYS_SEC = (2, 5, 15, 30, 60)
MEDIA_FAIL_DIRECT = os.environ.get("WECHAT_BRIDGE_MEDIA_FAIL_DIRECT", "1") != "0"
COMMANDS_ENABLED = os.environ.get("WECHAT_BRIDGE_COMMANDS", "1") != "0"
_COMMAND_WORDS = {
    "状态": "status", "/状态": "status", "/status": "status",
    "status": "status", "桥接状态": "status",
    "帮助": "help", "/帮助": "help", "/help": "help",
    "重发": "resend", "/重发": "resend", "/resend": "resend",
    # v5
    "取消": "cancel", "/取消": "cancel", "/cancel": "cancel", "停止": "cancel",
    "进度": "progress", "/进度": "progress", "/progress": "progress",
    "新话题": "newtopic", "/新话题": "newtopic", "新对话": "newtopic",
    "/new": "newtopic", "/reset": "newtopic",
    "继续": "continue", "/继续": "continue", "/continue": "continue",
    "接着发": "continue",
    # opt batch items 3+5: deterministic direct answers, no LLM run
    "最近文件": "recent_files", "/最近文件": "recent_files",
    "最近备份": "backup_status", "/最近备份": "backup_status",
    "备份状态": "backup_status", "/备份状态": "backup_status",
    "最近对话": "recent_dialogue", "/最近对话": "recent_dialogue",
    # 2026-11-07 §8.2.1: the numbers of `stats`, copied as they are
    **({"统计": "stats", "/统计": "stats", "/stats": "stats"}
       if os.environ.get("WECHAT_BRIDGE_CMD_STATS", "1").strip() != "0" else {}),
}


def _item_text(it: dict) -> str:
    """Text of a text item, or WeChat's own transcription of a voice item
    (voice_item.text, used as the body by inbound.ts bodyFromItemList)."""
    t = it.get("type")
    if t == 1:
        return str((it.get("text_item") or {}).get("text") or "")
    if t == 3:
        return str((it.get("voice_item") or {}).get("text") or "")
    return ""


def _quote_text(items: list) -> str | None:
    """'title | body' of a quoted message (inbound.ts inlineQuoteBody)."""
    for it in items:
        ref = it.get("ref_msg")
        if not isinstance(ref, dict):
            continue
        parts = []
        title = str(ref.get("title") or "").strip()
        if title:
            parts.append(title)
        quoted = ref.get("message_item") or {}
        body = _item_text(quoted) or _MEDIA_LABELS.get(quoted.get("type"), "")
        if body and body != title:
            parts.append(body)
        if parts:
            return " | ".join(parts)[:2000]
    return None


def _has_cdn_ref(sub) -> bool:
    m = (sub or {}).get("media") or {} if isinstance(sub, dict) else {}
    return bool(m.get("encrypt_query_param") or m.get("full_url"))


def _find_media(items: list) -> tuple:
    """(kind, {"type": t, "<kind>_item": {...}}, quoted) of the media to
    download, or (None, None, False). Order as in process-message.ts:
    image > video > file in the message itself, then an untranscribed voice
    note, then media the user QUOTED (v3 only kept the label "[图片]", so
    "这张图里写的什么" about a quoted picture had nothing to look at)."""
    for t in (2, 5, 4):
        for it in items:
            if it.get("type") == t:
                kind = _KIND_BY_TYPE[t]
                field = _KIND_FIELDS[kind]
                return kind, {"type": t, field: it.get(field) or {}}, False
    for it in items:
        if it.get("type") == 3 and not _item_text(it) \
                and _has_cdn_ref(it.get("voice_item")):
            return "voice", {"type": 3, "voice_item": it.get("voice_item")}, False
    for it in items:
        ref = it.get("ref_msg")
        if not isinstance(ref, dict):
            continue
        q = ref.get("message_item") or {}
        kind = _KIND_BY_TYPE.get(q.get("type"))
        if not kind or (kind == "voice" and _item_text(q)):
            continue
        sub = q.get(_KIND_FIELDS[kind]) or {}
        if _has_cdn_ref(sub):
            return kind, {"type": q.get("type"), _KIND_FIELDS[kind]: sub}, True
    return None, None, False


# --------------------------------------------------------------------------
# User settings (2026-11-02, 五): lightweight WeChat settings commands the
# bridge answers itself, so the user does not have to come back to the App
# to change a default. Single user (the bridge only accepts his messages).
# Stored in state/settings.json (0600, backed up with state/). Every
# change is answered with a confirmation naming old -> new, so an
# accidental tap is visible at once; unknown keys change nothing.
# --------------------------------------------------------------------------

SETTINGS_FILE = STATE_DIR / "settings.json"
SETTINGS_DEFAULTS = {"media_wait": True, "progress_notes": True,
                     "reply_style": "normal"}
_SETTINGS_WORDS = {
    "媒体等追问": ("media_wait", True), "媒体等": ("media_wait", True),
    "媒体等待": ("media_wait", True),
    "媒体不等追问": ("media_wait", False), "媒体不等": ("media_wait", False),
    "媒体马上回": ("media_wait", False),
    "进度开": ("progress_notes", True), "进度通知开": ("progress_notes", True),
    "进度关": ("progress_notes", False), "进度通知关": ("progress_notes", False),
    "简略": ("reply_style", "brief"), "详细": ("reply_style", "detailed"),
    "正常": ("reply_style", "normal"),
}
_SETTINGS_CN = {
    ("media_wait", True): "媒体等追问：开", ("media_wait", False): "媒体等追问：关",
    ("progress_notes", True): "进度提醒：开",
    ("progress_notes", False): "进度提醒：关",
    ("reply_style", "brief"): "回复详略：简略",
    ("reply_style", "detailed"): "回复详略：详细",
    ("reply_style", "normal"): "回复详略：正常",
}
_SETTINGS_EFFECT = {
    ("media_wait", True): "以后你发图片/视频/文件没附文字时，我会先看媒体、等你追问（最多 30 秒）再回完整答案。",
    ("media_wait", False): "以后媒体一到就直接回，不等追问。",
    ("progress_notes", True): "长任务到 180/600 秒、且阶段真的变了时最多再提醒 2 句在做什么。",
    ("progress_notes", False): "长任务不再发阶段提醒，只保留“正在输入”；想看进度时发「进度」。",
    ("reply_style", "brief"): "以后回复尽量简短，先给结论。",
    ("reply_style", "detailed"): "以后回复可以展开讲。",
    ("reply_style", "normal"): "恢复正常详略。",
}


def settings_get() -> dict:
    """Current settings merged over defaults; never raises."""
    raw = read_json(SETTINGS_FILE, {}) or {}
    out = dict(SETTINGS_DEFAULTS)
    for k in out:
        if k in raw and type(raw[k]) is type(out[k]):
            out[k] = raw[k]
    if out.get("reply_style") not in ("brief", "detailed", "normal"):
        out["reply_style"] = "normal"
    return out


def settings_set(updates: dict) -> dict:
    cur = settings_get()
    cur.update({k: v for k, v in updates.items() if k in SETTINGS_DEFAULTS})
    atomic_write_json(SETTINGS_FILE, cur)
    return cur


def settings_text(non_default_only: bool = False) -> str:
    cur = settings_get()
    parts = []
    for k in ("media_wait", "progress_notes", "reply_style"):
        if non_default_only and cur[k] == SETTINGS_DEFAULTS[k]:
            continue
        parts.append(_SETTINGS_CN[(k, cur[k])])
    return "；".join(parts)


def settings_usage() -> str:
    return ("当前设置：" + (settings_text() or "全部默认") + "\n"
            "可以这样改（改完我会回一句确认）：\n"
            "· 设置 媒体等追问 / 设置 媒体不等\n"
            "· 设置 进度开 / 设置 进度关\n"
            "· 设置 简略 / 设置 详细 / 设置 正常\n"
            "· 设置 恢复默认")


_SETTINGS_HINT_WORDS = ("媒体", "进度", "简略", "详细", "正常", "默认", "追问")


def parse_settings_command(text: str) -> str | None:
    """A settings command's bridge reply, or None if this is not one.
    2026-11-04 review: only an explicit settings command is intercepted - a
    known item, 「设置」 alone, or 「设置 <short word>」 with a separator (or a
    short text naming a setting) gets the usage text. v5.0.5 answered EVERY
    message starting with 设置 itself, so 「设置一个明天早上8点的提醒」 and
    「设置一下服务器防火墙」 got "没这个设置项" and never reached Muse."""
    t = (text or "").strip().strip("。.!！?？~～ \u3000")
    for prefix in ("/设置", "设置"):
        if t.startswith(prefix):
            after = t[len(prefix):]
            break
    else:
        return None
    sep = after[:1] in (" ", "\u3000", ":", "：")
    rest = after.strip(" \u3000:：").replace(" ", "").replace("\u3000", "")
    if not rest:
        return settings_usage()
    if rest in ("恢复默认", "重置", "默认"):
        settings_set(dict(SETTINGS_DEFAULTS))
        return ("已恢复默认：媒体等追问开、进度提醒开、回复正常详略。")
    hit = _SETTINGS_WORDS.get(rest)
    if not hit:
        if (sep and len(rest) <= 8) or (
                len(rest) <= 6 and any(w in rest for w in _SETTINGS_HINT_WORDS)):
            return "没这个设置项，什么都没改。\n" + settings_usage()
        return None             # an ordinary request that starts with 设置
    key, value = hit
    before = settings_get()
    if before.get(key) == value:
        return f"已经是这个设置了：{_SETTINGS_CN[(key, value)]}。"
    settings_set({key: value})
    return (f"已设置：{_SETTINGS_CN[(key, value)]}"
            f"（原来：{_SETTINGS_CN[(key, before.get(key))]}）。"
            f"{_SETTINGS_EFFECT[(key, value)]}")


def detect_command(text: str, user: str | None = None) -> str | None:
    """Exact-match chat commands the bridge answers itself (no LLM run):
    状态 / 重发 / 帮助, v5 取消 / 进度 / 新话题 / 继续. 「取消」 and 「继续」
    are intercepted only when they have something to act on (a task in
    flight; a reply parked by the reply budget) - otherwise they are ordinary
    messages for Muse (「继续」= "go on"). WECHAT_BRIDGE_COMMANDS=0: off."""
    if not COMMANDS_ENABLED:
        return None
    raw = (text or "").strip().strip("。.!！?？~～ \u3000")
    t = raw.lower()
    cmd = _COMMAND_WORDS.get(t)
    # 2026-11-04 review: 「查记录」 needs a separator (「查记录 备份」,
    # 「/查记录：备份」). v5.0.5 took any message starting with the three
    # characters, so 「查记录一下昨天的会议纪要有没有发出去」 became a keyword
    # search instead of a request for Muse (reproduced).
    for prefix in ("/查记录", "查记录"):
        if raw.startswith(prefix):
            after = raw[len(prefix):]
            if not after.strip(" \u3000:："):
                return "history_search:"
            if after[:1] in (" ", "\u3000", ":", "："):
                return "history_search:" + after.strip(" \u3000:：")
            break
    if cmd == "cancel" and user is not None and not _inflight_tasks(user):
        return None
    if cmd == "continue" and (user is None or not _has_deferred(user)):
        return None
    return cmd


def _transient_net_error(e: BaseException) -> bool:
    if isinstance(e, ApiError):
        return e.kind in ("connect", "network", "timeout")
    if isinstance(e, urllib.error.HTTPError):
        return e.code >= 500
    return isinstance(e, (urllib.error.URLError, TimeoutError,
                          ConnectionError, http.client.HTTPException))


def _history_label(kind: str, text: str, media_item: dict | None,
                   quoted: bool) -> str:
    if kind in ("image", "video", "file", "voice"):
        label = KIND_LABELS[kind]
        if kind == "file":
            name = ((media_item or {}).get("file_item") or {}).get("file_name")
            if name:
                label += f"《{_sanitize_filename(name)}》"
        tag = f"[引用的{label}]" if quoted else f"[{label}]"
        return (tag + " " + text).strip()
    return text


# --------------------------------------------------------------------------
# Message batching: state/pending.json (2026-11-06, feature 1) and
# superseding a reply in progress (feature 2, exit 4 - see EXIT_SUPERSEDED)
#
# A message for Muse is no longer claimable the moment it is stored. It joins
# its user's open batch, listed in state/pending.json, and the whole batch is
# DISPATCHED once - all its messages become claimable at the same instant,
# the hook wakes once, one claim returns all of them with one merge_hint, the
# agent runs one round - as soon as the user has paused BATCH_QUIET_MS (3 s
# without a new message) or BATCH_MAX_MS (60 s) after the batch opened,
# whichever comes first. A single message therefore goes out 3 s after it
# arrived, never at 60 s.
#   - The batch is the only coalescing rule. The claim-side burst window of
#     v5 (CLAIM_SETTLE_MS 1.2 s / CLAIM_SETTLE_SHORT_MS 0.3 s, cap 4 s) is
#     removed; merge_hint now just says "this claim is one batch".
#   - The message is still written to inbox/<id>.json before the cursor
#     moves (status pending_execution / downloading, as before); the batch
#     adds `batch_open: true` - the hook and claim skip such tasks - and lists
#     the id in pending.json. Enrolment, dispatch and supersede all run under
#     task_lock: a message arriving at the dispatch instant is either in the
#     batch being dispatched or opens the next one - never lost, never in
#     both. A task left with batch_open but no batch (crash between the two
#     writes, pending.json deleted) is dispatched by batch_recover().
#   - Commands (状态 / 取消 / ...), notices and messages the bridge answers
#     itself are not batched: answered at once, as before.
#   - Serve dispatches on time (batch thread); claim and the sender loop also
#     dispatch whatever is overdue, so nothing hangs on one thread.
#   - Feature 2: an agent-bound message that arrives while earlier messages
#     of the same user were dispatched but are not answered yet pulls them
#     back into the new batch (_batch_supersede): the reply being prepared
#     for them is abandoned - the executor's next complete / progress /
#     heartbeat / send-media for them exits 4 and sends nothing - and they
#     are dispatched again together with the new message.
# WECHAT_BRIDGE_BATCH=0 (state/tuning.env) turns batching and supersede off:
# every message is claimable at once (as before, minus the old claim window).
# --------------------------------------------------------------------------
PENDING_BATCH_FILE = STATE_DIR / "pending.json"
BATCH_ENABLED = os.environ.get("WECHAT_BRIDGE_BATCH", "1") != "0"
BATCH_QUIET_MS = max(0, _env_int("WECHAT_BRIDGE_BATCH_QUIET_MS", 3000))
BATCH_MAX_MS = max(0, _env_int("WECHAT_BRIDGE_BATCH_MAX_MS", 60_000))
_BATCH = {"on": False}          # enrolment on: serve (cmd_serve) or a test
_batch_wake = threading.Event()
_BATCH_PEEK: dict = {"sig": None, "batches": []}
_AGENT_BOUND = ("pending_execution", "downloading")
_SUPERSEDABLE = ("pending_execution", "downloading", "executing")


def batching_on(on: bool = True) -> None:
    """Enrol new agent-bound messages in pending.json (serve does this at
    start unless WECHAT_BRIDGE_BATCH=0). Off: ingest leaves them claimable at
    once and supersedes nothing."""
    _BATCH["on"] = bool(on)


def _batch_load() -> dict:
    d = read_json(PENDING_BATCH_FILE, None)
    if not isinstance(d, dict) or not isinstance(d.get("batches"), list):
        d = {"version": 1, "batches": []}
    d["batches"] = [b for b in d["batches"] if isinstance(b, dict) and b.get("id")]
    return d


def _batch_save(d: dict) -> None:
    d["version"] = 1
    d["updated_at"] = now_ms()
    # No fsync: the task files (fsynced) are the record; a lost pending.json
    # only means batch_recover() dispatches the held messages at once.
    atomic_write_json(PENDING_BATCH_FILE, d, fsync=False)


def batch_peek() -> list:
    """Open batches as last written - no lock, re-read only when the file
    changed; for the waiting loops (claim, batch thread)."""
    try:
        st = os.stat(PENDING_BATCH_FILE)
        sig = (st.st_ino, st.st_mtime_ns, st.st_size)
    except OSError:
        _BATCH_PEEK.update(sig=None, batches=[])
        return []
    if sig != _BATCH_PEEK["sig"]:
        _BATCH_PEEK.update(sig=sig, batches=_batch_load()["batches"])
    return list(_BATCH_PEEK["batches"])


def batch_next_due() -> int | None:
    """When the next open batch is due (epoch ms), None if there is none."""
    return min((int(b.get("due_at") or 0) for b in batch_peek()), default=None)


def _batch_enroll(task: dict) -> dict:
    """Pick the user's open batch (or open one) for a new agent-bound message
    and mark the task as held by it - before the task's first write, so it is
    never claimable outside its batch. Caller holds task_lock."""
    now = int(task.get("received_at") or now_ms())
    d = _batch_load()
    user = task.get("from_user_id")
    b = next((x for x in d["batches"] if x.get("user") == user), None)
    if b is None:
        b = {"id": f"b{now}-{secrets.token_hex(3)}", "user": user,
             "opened_at": now, "quiet_ms": BATCH_QUIET_MS,
             "max_ms": BATCH_MAX_MS, "items": []}
    task.update(batch_id=b["id"], batch_open=True, batched_at=now)
    return {"data": d, "batch": b, "now": now}


def _batch_commit(task: dict, ctx: dict) -> None:
    """Second half of the enrolment (task already saved, same task_lock):
    supersede what is still unanswered, list everything in pending.json and
    move the batch's due time (3 s after this message, capped at 60 s after
    the batch opened)."""
    d, b, now = ctx["data"], ctx["batch"], ctx["now"]
    carried = _batch_supersede(task.get("from_user_id"), task["message_id"],
                               b["id"], now)
    have = {it.get("id") for it in b["items"]}
    for mid in carried:
        if mid not in have:
            b["items"].append({"id": mid, "at": now, "carried": True})
    b["items"].append({"id": task["message_id"], "at": now})
    b["last_at"] = now
    b["due_at"] = min(now + int(b.get("quiet_ms", BATCH_QUIET_MS)),
                      int(b.get("opened_at") or now) + int(b.get("max_ms", BATCH_MAX_MS)))
    if not any(x.get("id") == b["id"] for x in d["batches"]):
        d["batches"].append(b)
    _batch_save(d)
    _batch_wake.set()


def _batch_supersede(user, new_mid: str, bid: str, now: int) -> list:
    """Feature 2. Messages of `user` already dispatched to Muse but not yet
    answered - pending_execution / downloading after their dispatch, or
    executing - go back into batch `bid`. The reply being prepared for them
    is abandoned: their claim ids are recorded as superseded, so the
    executor's next complete / progress / heartbeat / send-media for them
    exits 4 (SUPERSEDED) and sends nothing; they are dispatched again with
    the new message, so the next round answers all of them in one reply.
    Not superseded: a reply already handed to the bridge (waiting_send and
    later - it may be on its way), commands / notices, proactive sends, and
    a claim holding a long-step lease (progress --hold-minutes). A claimed
    task gets a new wake_key, so the hook wakes an executor for it at once
    (the one that had it will stop); an unclaimed one keeps it, so the
    executor already woken for it takes the new batch. Caller holds
    task_lock."""
    cands = [s_ for s_ in scan_task_index()
             if s_.get("from_user_id") == user and s_["message_id"] != new_mid
             and s_.get("status") in _SUPERSEDABLE and not s_.get("batch_open")
             and s_.get("kind") not in ("command", "notice")]
    if not cands:
        return []
    leased = {s_.get("claim_id") for s_ in cands
              if s_.get("status") == "executing" and s_.get("claim_id")
              and int(s_.get("hold_until") or 0) > now}
    carried = []
    for s_ in cands:
        t = load_task(s_["message_id"])
        if not t or t.get("status") not in _SUPERSEDABLE or t.get("batch_open"):
            continue
        ex = dict(t.get("execution") or {})
        if ex.get("proactive"):
            continue
        claimed = t.get("status") == "executing"
        if claimed and (ex.get("claim_id") in leased
                        or int(ex.get("hold_until") or 0) > now):
            continue
        if RELAY_ON and t.get("relay"):
            # 2026-11-07 relay: the running leg ends here (plan and done steps
            # stay); whoever answers the new batch needs a fresh turn
            if claimed:
                _relay_leg_end(t, plan_load(t["message_id"]), "superseded", now)
            t["relay_pending"] = True
        gone = {c for c in (ex.pop("claim_id", None), ex.pop("revoked_claim_id", None)) if c}
        n = int(ex.get("supersedes") or 0) + 1
        ex.update(superseded_claim_ids=sorted(set(ex.get("superseded_claim_ids") or []) | gone),
                  supersedes=n, superseded_at=now, superseded_by=new_mid)
        if ex.get("stage") or ex.get("note"):
            ex["checkpoint"] = {"stage": ex.get("stage", ""), "note": ex.get("note", ""),
                                "stage_at": ex.get("stage_at", 0)}
        for k in ("claimed_at", "heartbeat_at", "hold_until", "hold_minutes"):
            ex.pop(k, None)
        t["execution"] = ex
        if claimed:
            t["status"] = "pending_execution"
            t["wake_key"] = f"{t['message_id']}~{n}"
        t.update(batch_id=bid, batch_open=True, batched_at=now)
        save_task(t)
        carried.append(t["message_id"])
    if carried:
        log(f"batch {bid}: new message id={new_mid} supersedes {len(carried)} "
            f"unanswered message(s) {carried}: their reply is abandoned, they "
            "are dispatched again with it")
    return carried


def _batch_release(t: dict, now: int) -> None:
    """Dispatch one held task (caller holds task_lock and saves)."""
    t.pop("batch_open", None)
    t["dispatched_at"] = now
    t["dispatches"] = int(t.get("dispatches") or 0) + 1
    if t.get("status") == "pending_execution":
        t["pending_at"] = now


def _batch_dispatch(b: dict, now: int) -> dict:
    ids = []
    for it in b.get("items") or []:
        t = load_task(it.get("id"))
        if not t or t.get("batch_id") != b.get("id") or not t.get("batch_open"):
            continue
        _batch_release(t, now)
        save_task(t)
        if t.get("status") in _AGENT_BOUND:
            ids.append(t["message_id"])
    att = _bg_attach_quoted(ids, now) if BG_ON else []    # v1.5: quoted follow-ups
    ids = [x for x in ids if x not in att]
    bg = _bg_route_batch(ids, now) if BG_ON else []
    if bg:      # 2026-11-07 v1.4: a long batch goes to the background, not to an executor
        ids = [x for x in ids if x not in bg]
    bg = att + bg
    opened, last = int(b.get("opened_at") or now), int(b.get("last_at") or now)
    why = ("quiet" if now - last >= int(b.get("quiet_ms", BATCH_QUIET_MS))
           else "max" if now - opened >= int(b.get("max_ms", BATCH_MAX_MS)) else "forced")
    log(f"batch {b.get('id')} dispatched: {len(ids)} message(s) {ids} after "
        f"{now - opened} ms (reason={why})")
    return {"id": b.get("id"), "ids": ids, "reason": why, "held_ms": now - opened, "bg": bg}


def batch_flush_due(*, everything: bool = False) -> list:
    """Dispatch every open batch that is due (all of them with
    everything=True), then bump the inbox signal once. Any process may call
    it any time; without anything due it only stats pending.json."""
    if not everything:
        nd = batch_next_due()
        if nd is None or nd > now_ms():
            return []
    done = []
    with task_lock():
        now = now_ms()
        d = _batch_load()
        keep = []
        for b in d["batches"]:
            if everything or int(b.get("due_at") or 0) <= now:
                done.append(_batch_dispatch(b, now))
            else:
                keep.append(b)
        if done:
            d["batches"] = keep
            _batch_save(d)
    if any(x["ids"] for x in done):
        _signal_inbox()         # a lingering hook wakes for the batch at once
    if any(x.get("bg") for x in done):
        _wake_serve()           # 2026-11-07 v1.4: the background signal goes out now
    return done


def batch_recover() -> int:
    """Dispatch tasks still marked batch_open whose batch pending.json does
    not list (crash between the task write and pending.json, file lost)."""
    held = [s_ for s_ in scan_task_index() if s_.get("batch_open")]
    if not held:
        return 0
    n = 0
    with task_lock():
        known = {b["id"] for b in _batch_load()["batches"]}
        now = now_ms()
        for s_ in held:
            if s_.get("batch_id") in known:
                continue
            t = load_task(s_["message_id"])
            if not t or not t.get("batch_open") or t.get("batch_id") in known:
                continue
            _batch_release(t, now)
            save_task(t)
            n += 1
    if n:
        log(f"batch: dispatched {n} held message(s) whose batch was lost")
        _signal_inbox()
    return n


def batch_summary() -> dict:
    now = now_ms()
    return {"enabled": BATCH_ENABLED, "quiet_ms": BATCH_QUIET_MS, "max_ms": BATCH_MAX_MS,
            "open": [{"id": b.get("id"), "messages": len(b.get("items") or []),
                      "age_ms": now - int(b.get("opened_at") or now),
                      "due_in_ms": int(b.get("due_at") or now) - now}
                     for b in batch_peek()]}


def batch_loop() -> None:
    """serve's batch thread: dispatch each batch when it is due (sleeps until
    the next due time, woken by every enrolment); recovery every 5 s."""
    last_recover = 0.0
    while not _stop.is_set():
        _batch_wake.clear()
        _beat("batcher")
        nxt = None
        try:
            if time.monotonic() - last_recover >= 5:
                last_recover = time.monotonic()
                batch_recover()
            batch_flush_due()
            nxt = batch_next_due()
        except Exception as e:  # noqa: BLE001 - a worker must never die
            log(f"batch loop error: {type(e).__name__}: {_scrub(e)}")
        wait = 1.0 if nxt is None else min(1.0, max(0.005, (nxt - now_ms()) / 1000))
        _batch_wake.wait(wait)


def ingest_message(creds: dict, m: dict) -> None:
    """Validate and persist one inbound message. Never blocks on media:
    images/files/videos/voice are stored as a `downloading` task and fetched
    by the media workers. The task exists on disk before the cursor is
    persisted. v4: messages without message_type accepted; quoted media and
    untranscribed voice downloaded; chat commands answered locally; every
    turn in the history.
    v5: each message grants a fresh reply budget and re-opens the 24 h window
    (parked replies resume at once); quotes carrying only a server id are
    restored from the quote store (text, partial quotes, even the earlier
    picture/file itself); unknown item types no longer hide the text next to
    them; "typing..." starts immediately."""
    items = m.get("item_list") or []
    # inbound.ts getWeixinMessageId: top-level id, else the first item msg_id
    mid = str(m.get("message_id") or "") or next(
        (str(it["msg_id"]) for it in items if it.get("msg_id")), "")
    if not mid:
        log("inbound message without message_id; skipped")
        return
    allowed_user = creds.get("ilink_user_id")
    sender = m.get("from_user_id") or ""
    if m.get("group_id"):
        log(f"ignored group message id={mid}")
        return
    mtype = m.get("message_type")
    if mtype == 2:
        note_bot_echo(m, allowed_user)
        return
    if mtype not in (None, 1):
        log(f"ignored non-user message id={mid} type={mtype}")
        return
    if not allowed_user or sender != allowed_user:
        log(f"ignored unauthorized sender id={mid} sender={mask(sender)}")
        return
    if load_task(mid):
        log(f"duplicate message id={mid}; skipped")
        return
    ctx_token = m.get("context_token")
    remember_context_token(sender, ctx_token)
    quota_grant(sender, ctx_token, mid)
    types = [it.get("type") for it in items]
    texts = [_item_text(it) for it in items if it.get("type") in (1, 3)]
    transcribed = any(it.get("type") == 3 and _item_text(it) for it in items)
    mute_voice = any(it.get("type") == 3 and not _item_text(it)
                     for it in items)
    media_kind, media_item, quoted_media = _find_media(items)
    text = "".join(texts)
    qinfo = resolve_quote(items)
    quote = qinfo.get("text")
    stored = qinfo.get("media") if media_kind is None else None
    unknown = sorted({t for t in types if t not in (1, 2, 3, 4, 5)}, key=str)
    command = None
    result_text = None
    settings_reply = None
    if media_kind is not None:
        kind, status = media_kind, "downloading"
    elif stored is not None:
        kind, status = stored["kind"], "pending_execution"
    elif not mute_voice and (text.strip()
                             or (types and all(t in (1, 3) for t in types))):
        kind, status = "text", "pending_execution"
        command = detect_command(text, sender)
        if command is None:
            settings_reply = parse_settings_command(text)
        if transcribed:
            text = VOICE_TEXT_PREFIX + text
        if command or settings_reply:
            kind, status = "command", "waiting_send"
    elif mute_voice:
        kind, status, result_text = ("unsupported", "waiting_send",
                                     VOICE_UNUSABLE_TEXT)
    else:
        kind, status, result_text = ("unsupported", "waiting_send",
                                     UNSUPPORTED_TEXT)
    if command:
        try:
            result_text = run_command(command, sender, quote=quote)
        except Exception as e:  # noqa: BLE001 - always answer something
            log(f"command {command} failed: {type(e).__name__}: {_scrub(e)}")
            result_text = f"指令「{text.strip()}」执行出错了（{type(e).__name__}），请稍后再试。"
    elif settings_reply:
        result_text = settings_reply
    task = {
        "message_id": mid,
        "from_user_id": sender,
        "to_user_id": m.get("to_user_id"),
        "session_id": m.get("session_id"),
        "context_token": ctx_token,
        "create_time_ms": m.get("create_time_ms"),
        "received_at": now_ms(),
        "item_types": types,
        "kind": kind,
        "text": text,
        "quote": quote,
        "quote_svr_id": qinfo.get("svr_id"),      # v1.5: quote-bound follow-ups
        "voice_transcribed": bool(transcribed),
        "media_path": None,
        "media_bytes": None,
        "file_name": None,
        "play_length": None,
        "thumb_path": None,
        "media_error": None,
        "raw_items": items,
        "status": status,
        "pending_at": now_ms() if status == "pending_execution" else None,
        "result_text": result_text,
        "execution": ({"by": "bridge", "command": command or "settings",
                       "finished_at": now_ms()} if (command or settings_reply) else None),
        "send": None,
    }
    if unknown:
        task["unsupported_items"] = unknown
    if len(qinfo.get("ids") or []) > 1:
        task["quote_ids"] = qinfo["ids"][:6]      # v1.7: every id the quote names
    if media_item is not None:
        task["media_item"] = media_item
        task["quoted_media"] = bool(quoted_media)
    if stored is not None:
        task.update(media_path=stored.get("media"), file_name=stored.get("name"),
                    quoted_media=True, quote_from_store=True)
    if command and not result_text:
        # 「继续」: the parked replies resumed below ARE the answer - an
        # extra message would only cost a reply slot.
        task["status"] = "sent"
        task["send"] = {"ok": True, "no_text": True, "at": now_ms()}
    batched = None
    with task_lock():
        if load_task(mid):
            log(f"duplicate message id={mid}; skipped")
            return
        # 2026-11-06: an agent-bound message joins the user's batch
        # (pending.json) in the same lock as its first write - it is never
        # claimable on its own, and a dispatch cannot slip in between.
        if _BATCH["on"] and task["status"] in _AGENT_BOUND:
            batched = _batch_enroll(task)
        save_task(task)
        if batched is not None:
            _batch_commit(task, batched)
    if task["status"] in ("downloading", "pending_execution"):
        _typing_wake.set()          # "typing..." at once, before the bookkeeping
    if batched is None:
        _signal_inbox()             # batched: signalled when the batch is dispatched
    clear_rate_limit()   # a user message lifts the iLink send limit
    _resume_deferred(sender)
    update_heartbeat(last_inbound_at=now_ms())
    label = (_history_label(kind, text, media_item, bool(quoted_media or stored))
             or ("[语音]" if mute_voice else f"[{kind}]"))
    history_append("user", label, mid=mid, kind=kind)
    quote_put(mid, user=sender, direction="in",
              kind=kind if kind in KIND_LABELS else "text", text=label,
              media=task.get("media_path"), name=task.get("file_name"))
    # v1.7: a later quote may name this message by an item msg_id instead of
    # its message_id (ilink-hub / official inbound.ts fallback): alias rows
    # point at the task
    for alias in sorted({str(it.get("msg_id")) for it in items
                         if it.get("msg_id") not in (None, "", 0)} - {str(mid)}):
        quote_put(alias, user=sender, direction="in",
                  kind=kind if kind in KIND_LABELS else "text", text=label,
                  media=task.get("media_path"), name=task.get("file_name"), task=mid)
    log(f"task stored id={mid} kind={kind} chars={len(task['text'])}"
        + (" voice=transcribed" if transcribed else "")
        + (" quote=yes" if quote else "")
        + (" quote_from_store=yes" if qinfo.get("from_store") else "")
        + (" quoted_media=yes" if quoted_media or stored else "")
        + (f" unknown_items={unknown}" if unknown else "")
        + (f" command={command}" if command else "")
        + (" command=settings" if settings_reply else "")
        + (f" batch={task['batch_id']}" if batched is not None else ""))
    if status == "downloading":
        enqueue_media(mid)
    elif task["status"] == "waiting_send":
        _send_wake.set()
    if task["status"] in ("downloading", "pending_execution"):
        _typing_wake.set()


MEDIA_WORKERS = 2
_media_q: "queue.Queue[str]" = queue.Queue()
_media_queued: set = set()
_media_queued_lock = threading.Lock()


def enqueue_media(mid: str) -> None:
    with _media_queued_lock:
        if mid in _media_queued:
            return
        _media_queued.add(mid)
    _media_q.put(mid)


FILE_DECRYPT_HINT = ("这个文件之前上传过微信，触发了微信的已知解密问题（同一个文件会被复用旧的加密包、"
                     "却配了一把新密钥），原样重发多少次都解不开。请把文件压缩成 zip 再发，"
                     "或者在文档里随便改动一点重新保存后再发，我就能读了。")


def _media_label(kind: str, task: dict | None = None) -> str:
    label = KIND_LABELS.get(kind, "文件")
    if kind == "file":
        t = task or {}
        name = t.get("file_name") or (((t.get("media_item") or {})
                                       .get("file_item") or {}).get("file_name"))
        if name:
            label += f"《{_sanitize_filename(name)}》"
    return label


def classify_media_failure(kind: str, err: BaseException, task: dict | None,
                           attempts: int, elapsed_ms: float) -> tuple:
    """(code, message for the user) of an inbound-media failure. code is
    too_large / decrypt / md5 / no_ref / empty / network, or "other" (no
    canned message: the executor gets the raw error and decides)."""
    label = _media_label(kind, task)
    if isinstance(err, MediaError):
        r = err.reason
        if r == "too_large":
            size, cap = err.info.get("size"), err.info.get("cap")
            sz = f"有 {fmt_size(size)}" if size else "太大了"
            cap_s = f"（{fmt_size(cap)}）" if cap else ""
            extra = "，或者发个网盘链接" if kind == "file" else ""
            return "too_large", (f"这个{label}{sz}，超过了桥接能接收的上限{cap_s}。"
                                 f"可以压缩或拆分后再发{extra}。")
        if r == "decrypt":
            if kind == "file":
                return "decrypt", FILE_DECRYPT_HINT
            how = "截个图再发，或者换一张" if kind == "image" else "重新导出或压缩一下再发"
            return "decrypt", (f"这个{label}下载下来了但解不开（多半是微信 CDN 复用了以前上传过的"
                               f"同一份内容、却配了新密钥），原样重发通常还是不行，{how}试试。")
        if r == "md5":
            return "md5", f"这个{label}下载后校验对不上（内容不完整），请重发一次。"
        if r == "no_ref":
            return "no_ref", f"这条{label}消息里没有可下载的数据（微信没给下载地址），请重新发一次。"
        if r == "empty":
            return "empty", f"这个{label}解密后是空的，请重发一次。"
    if _transient_net_error(err):
        return "network", (f"这个{label}没能下载下来：网络连接反复中断（{fmt_duration(elapsed_ms)}"
                           f"内试了 {attempts} 次）。过一会儿重发一次就行。")
    return "other", None


def process_media_task(mid: str) -> None:
    """Download+decrypt the media of a `downloading` task, then flip it to
    pending_execution. v4: retries span ~2 minutes; a known failure (too
    large, CDN decrypt bug, network exhausted, ...) is answered by the bridge
    itself with the precise reason and what to do (WECHAT_BRIDGE_MEDIA_FAIL_
    DIRECT=0 restores v3: the executor gets media_error + media_error_hint).
    Tasks carrying user text, and unknown errors, still go to the executor."""
    task = load_task(mid)
    if not task or task.get("status") != "downloading":
        return
    kind = task.get("kind")
    wrapped = task.get("media_item")
    if isinstance(wrapped, dict) and kind in _KIND_FIELDS:
        src_item = wrapped.get(_KIND_FIELDS[kind])
    else:   # stored by v3 before the upgrade
        image_item, file_item, video_item = _split_media_items(
            task.get("raw_items") or [])
        src_item = {"image": image_item, "file": file_item,
                    "video": video_item}.get(kind)
    fetch = {"image": download_image, "file": download_file,
             "video": download_video, "voice": download_voice}.get(kind)
    media_info: dict = {}
    err = None
    attempts = 0
    t0 = time.monotonic()
    try:
        if src_item is None or fetch is None:
            raise MediaError("no_ref", "media item missing from stored message")
        for attempt in range(MEDIA_DOWNLOAD_ATTEMPTS):
            attempts = attempt + 1
            try:
                media_info = fetch(src_item, mid)
                break
            except Exception as e:  # noqa: BLE001 - classified below
                if (attempt == MEDIA_DOWNLOAD_ATTEMPTS - 1
                        or not _transient_net_error(e) or _stop.is_set()):
                    raise
                log(f"{kind} download attempt {attempt + 1} failed id={mid} "
                    f"({net_detail(e)}); retrying")
                _stop.wait(MEDIA_RETRY_DELAYS_SEC[
                    min(attempt, len(MEDIA_RETRY_DELAYS_SEC) - 1)])
        log(f"{kind} downloaded id={mid} bytes={media_info.get('media_bytes')}")
        media_info.update(_predigest(kind, media_info))
    except Exception as e:  # noqa: BLE001 - reported honestly below
        err = e
        media_fields = (sorted(((src_item or {}).get("media") or {}).keys())
                        if isinstance(src_item, dict) else [])
        log(f"{kind} download failed id={mid}: {type(e).__name__}: {_scrub(e)} "
            f"item_fields={sorted((src_item or {}).keys()) if isinstance(src_item, dict) else []} "
            f"media_fields={media_fields}")
    code = hint = None
    if err is not None:
        code, hint = classify_media_failure(kind, err, task, attempts,
                                            (time.monotonic() - t0) * 1000)
    direct = False
    with task_lock():
        fresh = load_task(mid)
        if not fresh or fresh.get("status") != "downloading":
            return
        fresh.update({
            "media_path": media_info.get("media_path"),
            "media_bytes": media_info.get("media_bytes"),
            "file_name": media_info.get("file_name") or fresh.get("file_name"),
            "play_length": media_info.get("play_length"),
            "thumb_path": media_info.get("thumb_path"),
            "media_error": (f"{type(err).__name__}: "
                            f"{_URL_RE.sub('<url>', str(err))}"[:200]
                            if err is not None else None),
        })
        for k in ("voice_format", "voice_wav", "voice_raw_path", "voice_note",
                  "video_md5_ok", "file_md5_ok", *_EXEC_DIGEST_FIELDS, "has_audio"):
            if k in media_info:
                fresh[k] = media_info[k]
        if err is not None:
            fresh["media_error_code"] = code
            if hint:
                fresh["media_error_hint"] = hint
        direct = bool(err is not None and MEDIA_FAIL_DIRECT and hint
                      and not (fresh.get("text") or "").strip())
        if direct:
            fresh["status"] = "waiting_send"
            fresh["result_text"] = hint
            fresh["execution"] = {"by": "bridge", "reason": f"media_{code}",
                                  "finished_at": now_ms()}
        else:
            fresh["status"] = "pending_execution"
            fresh["pending_at"] = now_ms()
        save_task(fresh)
    if direct:
        _send_wake.set()
    if err is None and media_info.get("media_path"):
        quote_set_media(mid, media_info.get("media_path"), media_info.get("file_name"))
    log(f"media task ready id={mid} kind={kind} ok={err is None}"
        + (f" error={code} direct={direct}" if err is not None else ""))


PREDIGEST = os.environ.get("WECHAT_BRIDGE_PREDIGEST", "1") != "0"
PREDIGEST_BUDGET_SEC = 30
PREDIGEST_FRAMES_MAX_BYTES = 60 * 1024 * 1024   # bigger: the executor runs video-frames
PREVIEW_OVER_BYTES = 1_500_000
PREVIEW_EDGE = 1568
TEXT_INLINE_MAX_CHARS = 4000
TEXT_FILE_EXTS = {
    ".txt", ".md", ".markdown", ".csv", ".tsv", ".json", ".jsonl", ".log", ".xml",
    ".yaml", ".yml", ".toml", ".ini", ".cfg", ".conf", ".py", ".js", ".ts", ".tsx",
    ".jsx", ".java", ".kt", ".go", ".rs", ".c", ".h", ".cc", ".cpp", ".hpp", ".cs",
    ".rb", ".php", ".sh", ".bash", ".zsh", ".sql", ".html", ".htm", ".css", ".scss",
    ".vue", ".swift", ".r", ".lua", ".pl", ".srt", ".vtt", ".tex", ".bib", ".diff",
    ".patch"}
_MEDIA_PATH_KEYS = ("media_path", "thumb_path", "voice_raw_path", "preview_path",
                    "audio_wav", "text_path")


def _decode_text(raw: bytes) -> str | None:
    for enc in ("utf-8-sig", "gb18030"):
        try:
            text = raw.decode(enc)
        except UnicodeDecodeError:
            continue
        return None if "\x00" in text else text
    return None


def _predigest(kind: str, info: dict) -> dict:
    """Pre-chewed media for the executor (2026-11-05), produced while its
    cold start runs (the hook wakes it as soon as the download starts), so it
    needs fewer tool calls before it can answer:
      image: preview_path - long edge <= 1568 px, what a vision model is given
             anyway - when the original is big;
      video: frames (first frame + scene changes, <= 6) and audio_wav
             (16 kHz mono, for speech-to-text), has_audio=false when silent;
      file : text_inline (whole small text file) or text_head (+ text_path
             from pdftotext for PDFs).
    Best effort within PREDIGEST_BUDGET_SEC: a failure only leaves a field
    out; WECHAT_BRIDGE_PREDIGEST=0 turns it off."""
    out: dict = {}
    p = Path(info.get("media_path") or "")
    if not PREDIGEST or not p.is_file():
        return out
    ff = ffmpeg_path()
    t_end = time.monotonic() + PREDIGEST_BUDGET_SEC

    def run(cmd) -> bool:
        left = t_end - time.monotonic()
        if left < 1:
            return False
        try:
            r = subprocess.run(cmd, stdin=subprocess.DEVNULL, capture_output=True,
                               timeout=left)
        except (OSError, subprocess.SubprocessError):
            return False
        return r.returncode == 0

    q = ["-nostdin", "-hide_banner", "-loglevel", "error", "-y"]
    try:
        if kind == "image" and ff and p.stat().st_size > PREVIEW_OVER_BYTES:
            prev = p.with_name(p.stem + ".preview.jpg")
            if run([ff, *q, "-i", str(p), "-vf",
                    f"scale='min({PREVIEW_EDGE},iw)':'min({PREVIEW_EDGE},ih)'"
                    ":force_original_aspect_ratio=decrease",
                    "-frames:v", "1", "-q:v", "3", str(prev)]) and prev.is_file():
                os.chmod(prev, 0o600)
                out["preview_path"] = str(prev)
        elif kind == "video" and ff:
            if p.stat().st_size <= PREDIGEST_FRAMES_MAX_BYTES:
                fdir = p.with_name(p.stem + ".frames")
                fdir.mkdir(mode=0o700, exist_ok=True)
                run([ff, *q, "-i", str(p), "-vf",
                     "select='eq(n,0)+gt(scene,0.35)',scale='min(1280,iw)':-2",
                     "-vsync", "vfr", "-frames:v", "6", str(fdir / "f-%02d.jpg")])
                frames = sorted(str(x) for x in fdir.glob("f-*.jpg"))
                if frames:
                    out["frames"] = frames
                else:
                    with contextlib.suppress(OSError):
                        fdir.rmdir()
            wav = p.with_suffix(".wav")
            if run([ff, *q, "-i", str(p), "-vn", "-ac", "1", "-ar", "16000", str(wav)]) \
                    and wav.is_file() and wav.stat().st_size > 1000:
                os.chmod(wav, 0o600)
                out["audio_wav"] = str(wav)
            else:
                wav.unlink(missing_ok=True)
                out["has_audio"] = False
        elif kind == "file":
            ext = p.suffix.lower()
            if ext in TEXT_FILE_EXTS and p.stat().st_size <= 256 * 1024:
                text = _decode_text(p.read_bytes())
                if text is not None:
                    if len(text) <= TEXT_INLINE_MAX_CHARS:
                        out["text_inline"] = text
                    else:
                        out["text_head"] = text[:2000]
                    out["text_chars"] = len(text)
            elif ext == ".pdf" and shutil.which("pdftotext"):
                txt = p.with_name(p.name + ".txt")
                if run(["pdftotext", "-layout", "-l", "30", str(p), str(txt)]) \
                        and txt.is_file():
                    text = txt.read_text(encoding="utf-8", errors="replace")
                    if text.strip():
                        os.chmod(txt, 0o600)
                        out.update(text_path=str(txt), text_head=text[:2000],
                                   text_chars=len(text))
                    else:
                        txt.unlink(missing_ok=True)
    except (OSError, ValueError) as e:
        log(f"predigest {kind} skipped: {type(e).__name__}")
    return out


def media_worker_loop() -> None:
    while not _stop.is_set():
        _beat()
        try:
            mid = _media_q.get(timeout=1)
        except queue.Empty:
            continue
        try:
            process_media_task(mid)
        except Exception as e:  # noqa: BLE001 - a worker must never die
            log(f"media worker error id={mid}: {type(e).__name__}")
        finally:
            with _media_queued_lock:
                _media_queued.discard(mid)


def recover_downloading() -> None:
    """After a restart, resume tasks that were still downloading."""
    n = 0
    for t in scan_task_index():
        if t.get("status") != "downloading":
            continue
        mid = str(t["message_id"])
        stem = safe_name(mid)
        # v5: .part (partial ciphertext) is kept for a Range resume
        for leftover in (MEDIA_DIR / f"{stem}.plain", FILES_DIR / f".{stem}.plain"):
            with contextlib.suppress(OSError):
                leftover.unlink()
        enqueue_media(mid)
        n += 1
    if n:
        log(f"resuming {n} interrupted media download(s)")


# --------------------------------------------------------------------------
# Bridge-written notices and chat commands (v4)
#
# A notice is a synthetic task (kind=notice, status waiting_send) sent by
# the normal reply path - same retries, chunking and context token - so the
# bridge can tell the user *itself*, in plain words, when something went
# wrong: a reply whose delivery is uncertain or that ran out of retries, a
# file that could not be uploaded, a message nobody picked up. Before, each
# of these either needed an LLM run (hook -> executor -> send-direct) or
# stayed silent. Notices never trigger notices; at most 12 per hour.
# Disable with WECHAT_BRIDGE_NOTICES=0 (the hook then alerts as in v3).
#
# Chat commands (状态 / 重发 / 帮助) are answered by the bridge without
# waking the executor: zero tokens, and they keep working when the executor
# pipeline is down - which is exactly when "状态" is most useful.
# --------------------------------------------------------------------------

NOTICES_ENABLED = os.environ.get("WECHAT_BRIDGE_NOTICES", "1") != "0"
NOTICE_HOURLY_CAP = 12
OVERDUE_PENDING_MS = 15 * 60 * 1000
RESEND_WINDOW_MS = 72 * 3600 * 1000
SERVE_STARTS_FILE = STATE_DIR / "serve_starts.json"
# 2026-11-02, 四: restart attribution. A restart reason is left by the
# restarter in serve_restart_reason.json just before restarting (repair /
# recover scripts do this; a bare `systemctl restart` or the hook leaves
# "unknown", which is reported as such, not guessed). Each start also
# records the gap since the last successful poll, so a repeat pattern
# (many starts, or starts <2 min apart = flapping) is visible instead of
# a bare count. Old entries were bare timestamps; they read as legacy.
SERVE_RESTART_REASON_FILE = STATE_DIR / "serve_restart_reason.json"


def serve_starts_read() -> list[dict]:
    out = []
    for x in (read_json(SERVE_STARTS_FILE, []) or []):
        if isinstance(x, int):
            out.append({"at": x, "reason": "legacy", "gap_ms": None,
                        "prev_uptime_ms": None})
        elif isinstance(x, dict) and x.get("at"):
            out.append({"at": int(x["at"]), "reason": str(x.get("reason") or "unknown"),
                        "gap_ms": x.get("gap_ms"),
                        "prev_uptime_ms": x.get("prev_uptime_ms")})
    return sorted(out, key=lambda e: e["at"])


def serve_start_entry(now: int) -> dict:
    """Build this start's entry from the PREVIOUS heartbeat - call before
    overwriting serve_started_at. Consumes a fresh reason file (<10 min)."""
    hb = read_json(HEARTBEAT_FILE, {}) or {}
    prev_started = int(hb.get("serve_started_at") or 0)
    last_poll = int(hb.get("last_poll_ok_at") or 0)
    reason = "unknown"
    rf = read_json(SERVE_RESTART_REASON_FILE, None)
    if isinstance(rf, dict) and rf.get("reason") \
            and now - int(rf.get("at") or 0) < 600_000:
        reason = str(rf["reason"])[:40]
    with contextlib.suppress(OSError):
        SERVE_RESTART_REASON_FILE.unlink(missing_ok=True)
    return {"at": now, "reason": reason,
            "gap_ms": (now - last_poll) if last_poll else None,
            "prev_uptime_ms": (last_poll - prev_started)
            if prev_started and last_poll > prev_started else None}
_POLL_FAIL_TIMES: list = []


def _note_poll_fail() -> None:
    _POLL_FAIL_TIMES.append(now_ms())
    if len(_POLL_FAIL_TIMES) > 500:
        del _POLL_FAIL_TIMES[:-500]


def create_notice(user: str | None, text: str, *, reason: str,
                  about: dict | None = None, essential: bool = False) -> str | None:
    """Queue a bridge-written message to `user`; returns its task id."""
    if not NOTICES_ENABLED or not user or not (text or "").strip():
        return None
    if window_state(user)[1] is False:
        log(f"notice not queued (24 h reply window closed) reason={reason}")
        return None
    now = now_ms()
    # v1.7: answers to a user message (essential) neither obey nor fill the cap -
    # v1.6 counted them, so a burst of them could mute the failure alerts
    recent = sum(1 for t in scan_task_index()
                 if t.get("kind") == "notice" and not t.get("essential")
                 and (t.get("received_at") or 0) > now - 3_600_000)
    if recent >= NOTICE_HOURLY_CAP and not essential:   # v1.6: answers to a message are not capped
        log(f"notice suppressed (hourly cap {NOTICE_HOURLY_CAP}) reason={reason}")
        return None
    tid = f"notice-{now}-{secrets.token_hex(3)}"
    task = {
        "message_id": tid, "from_user_id": user, "to_user_id": None,
        "session_id": None, "context_token": latest_context_token(user)[0],
        "create_time_ms": now, "received_at": now, "item_types": [],
        "kind": "notice", "text": "", "quote": None, "media_path": None,
        "media_bytes": None, "file_name": None, "play_length": None,
        "thumb_path": None, "media_error": None, "raw_items": [],
        "status": "waiting_send", "result_text": text.strip(),
        "execution": {"by": "bridge", "notice": reason, "about": about or {},
                      "finished_at": now, **({"essential": True} if essential else {})},
        "send": None,
    }
    with task_lock():
        save_task(task)
    _send_wake.set()
    log(f"notice queued id={tid} reason={reason}")
    return tid


INBOX_SIGNAL = STATE_DIR / ".inbox_signal"
ECHO_FILE = STATE_DIR / "echo.json"
ECHO_NOTICE_GRACE_MS = 20_000
ECHO_LEDGER_FILE = STATE_DIR / "echo_ledger.json"
ECHO_LEDGER_MAX = 600
# observe (default): echoes are only recorded (echo ledger + quote store) -
# status changes need data first. resolve: an echo also confirms an
# uncertain part and holds the uncertain notice for ECHO_NOTICE_GRACE_MS.
# off: ignored. There is no automatic resend in any mode.
ECHO_MODE = (os.environ.get("WECHAT_BRIDGE_ECHO_MODE") or "observe").strip().lower()


def _echo_ledger_update(cid: str, **fields) -> None:
    """One record per client_id: when it was sent, what, the outcome
    (True ok / None uncertain / False failed) and when its echo came."""
    if not cid or ECHO_MODE == "off":
        return
    try:
        with _flock(STATE_DIR / "echo.lock"):
            led = read_json(ECHO_LEDGER_FILE, {}) or {}
            rec = led.pop(cid, None) or {}
            for k, v in fields.items():
                if k == "echo_at" and rec.get("echo_at"):
                    continue                # keep the first echo
                rec[k] = v
            led[cid] = rec
            while len(led) > ECHO_LEDGER_MAX:
                led.pop(next(iter(led)))
            atomic_write_json(ECHO_LEDGER_FILE, led, fsync=False)
    except OSError:
        pass


def echo_stats(hours: float = 168) -> dict:
    """What the echo ledger shows: is an echo reliable proof of delivery on
    this account? (sends echoed, delay, by kind; uncertain sends echoed)."""
    led = read_json(ECHO_LEDGER_FILE, {}) or {}
    since = now_ms() - int(hours * 3_600_000)
    recs = [r for r in led.values() if isinstance(r, dict) and int(r.get("at") or 0) >= since]
    sent = [r for r in recs if r.get("ok") is True]
    unc = [r for r in recs if r.get("ok") is None and r.get("at")]
    ech = [r for r in sent if r.get("echo_at")]
    kinds: dict = {}
    for r in sent:
        k = kinds.setdefault(r.get("kind") or "text", {"sent": 0, "echoed": 0})
        k["sent"] += 1
        k["echoed"] += bool(r.get("echo_at"))
    delays = [int(r["echo_at"]) - int(r["at"]) for r in ech]
    return {"mode": ECHO_MODE, "sent": len(sent), "echoed": len(ech),
            "echo_rate": round(len(ech) / len(sent), 3) if sent else None,
            "delay_ms": {"p50": _pct(delays, 0.5), "p95": _pct(delays, 0.95)},
            "uncertain": len(unc), "uncertain_echoed": sum(1 for r in unc if r.get("echo_at")),
            "by_kind": kinds}


def _signal_inbox() -> None:
    """A new inbound task: bumps state/.inbox_signal, which a lingering hook
    (state/hook_linger_ms) watches to wake an executor at once."""
    with contextlib.suppress(OSError):
        atomic_write_text(INBOX_SIGNAL, str(now_ms()), fsync=False)


def _echo_capable() -> bool:
    """resolve mode and WeChat echoes this bot's own messages back through
    getupdates (message_type 2, same client_id) - seen 3+ times this week."""
    if ECHO_MODE != "resolve":
        return False
    e = read_json(ECHO_FILE, {}) or {}
    return (int(e.get("seen") or 0) >= 3
            and now_ms() - int(e.get("last_at") or 0) < 7 * 86_400_000)


def note_bot_echo(m: dict, owner: str | None) -> None:
    """2026-11-05, idea from ilink-hub: the bot copy of a sent message comes
    back through getupdates with the client_id it was sent with. That is
    proof of delivery: a part recorded as send_uncertain (timeout/reset after
    the request left) is marked delivered and the rest of the reply goes on -
    no false "不确定你收到没有" and no manual 「重发」. Copies are also put in
    the quote store under their server ids. Accounts that never echo just
    keep the v5 behaviour (`echo.json` stays empty)."""
    cid = str(m.get("client_id") or "")
    if ECHO_MODE == "off" or not cid or (owner and m.get("to_user_id") not in (None, "", owner)):
        return
    _echo_ledger_update(cid, echo_at=now_ms())
    with task_lock():
        e = read_json(ECHO_FILE, {}) or {}
        e.update(seen=int(e.get("seen") or 0) + 1, last_at=now_ms())
        with contextlib.suppress(OSError):
            atomic_write_json(ECHO_FILE, e, fsync=False)
    items = m.get("item_list") or []
    label = "".join(_item_text(it) for it in items if it.get("type") == 1) or \
        "".join(_MEDIA_LABELS.get(it.get("type"), "") for it in items)
    sids = {str(m.get("message_id") or "")} | {str(it.get("msg_id")) for it in items
                                                 if it.get("msg_id")}
    # v1.7 (ilink-hub quote_route.rs): the echo carries the server ids of a
    # message the bridge sent; the ledger knows which task sent that client id,
    # so a later quote of it routes back to the task (v1.6 stored no task)
    task = ((read_json(ECHO_LEDGER_FILE, {}) or {}).get(cid) or {}).get("task")
    for sid in sids - {"", "None", "0"}:
        old = quote_get(sid)
        if label and (not old or (task and not old.get("task"))):
            quote_put(sid, user=m.get("to_user_id"), direction="out", kind="text",
                      text=(old or {}).get("text") or label, task=task)
    n = _resolve_by_echo(cid) if ECHO_MODE == "resolve" else 0
    log(f"bot echo client_id=…{cid[-6:]}" + (f": resolved {n} uncertain send(s)" if n else ""))


def _resolve_by_echo(cid: str) -> int:
    n, done_texts = 0, []
    with task_lock():
        for s_ in scan_task_index():
            if s_.get("status") != "send_uncertain":
                continue
            t = load_task(s_["message_id"]) or {}
            send = dict(t.get("send") or {})
            pend = send.get("pending") or {}
            if t.get("status") != "send_uncertain" or pend.get("client_id") != cid:
                continue
            text = t.get("result_text") or ""
            done = int(pend.get("end") or send.get("done_upto") or 0)
            for k in ("pending", "uncertain", "no_auto_retry", "exhausted", "notify_after"):
                send.pop(k, None)
            if send.get("notified") == "echo-wait":
                send.pop("notified", None)
            send.update(done_upto=done, echo_confirmed_at=now_ms())
            if text.strip() and _plan_parts(text, _resume_offset(text, done), _chunk_limit()):
                attempts = int(send.get("attempts") or 0)
                send.update(next_retry_at=0, attempts=attempts,
                            max_attempts=max(SEND_MAX_ATTEMPTS, attempts + 1),
                            error="rest of the reply resumes (part confirmed by echo)")
                t["status"] = "send_failed"
            else:
                send.update(ok=True, error=None)
                t["status"] = "sent"
                done_texts.append((t["message_id"], text, t.get("kind")))
            t["send"] = send
            save_task(t)
            n += 1
        for it in list_queue():
            if it.get("status") == "uncertain" and it.get("client_id") == cid:
                it.pop("final_code", None)
                it.pop("final_error", None)
                _finish_qitem(it, "sent")
                n += 1
    for tid, text, kind in done_texts:
        update_heartbeat(last_send_ok_at=now_ms())
        if kind != "command" and text.strip():
            history_append("bot", text, mid=tid, kind=kind)
    if n:
        _send_wake.set()
    return n


def check_echo_wait(index: list) -> None:
    """Uncertain sends held back for an echo: alarm the user only if none
    came within ECHO_NOTICE_GRACE_MS."""
    now = now_ms()
    for s_ in index:
        send = s_.get("send") or {}
        if (s_.get("status") == "send_uncertain" and send.get("notified") == "echo-wait"
                and int(send.get("notify_after") or 0) <= now):
            _notify_send_problem(s_["message_id"])


def _reason_cn(kind: str, detail: str = "") -> str:
    d = (detail or "").lower()
    if kind == "connect":
        return "连不上微信服务器（出口代理或网络故障）"
    if "remotedisconnected" in d or "closed(" in d or "reset(" in d:
        return "连接被对方中断（RemoteDisconnected）" if "remotedisconnected" in d \
            else "连接被对方中断"
    if kind == "timeout" or "timeout" in d:
        return "网络超时"
    if kind == "network":
        return "网络连接中断"
    if kind == "http":
        return "微信服务端返回了错误"
    if kind == "business":
        m = re.search(r"(?:errcode|ret)=(-?\d+)", detail or "")
        if "client error" in d:
            return "微信 CDN 拒收了这个文件"
        if "exceeds" in d:
            return "超过了大小上限"
        return f"微信服务端拒绝（错误码 {m.group(1)}）" if m else "微信服务端拒绝"
    if kind == "local":
        return "本机处理出错"
    if kind == "config":
        return "本机配置不允许这次发送（例如 baseurl 不在允许列表里）"
    return "原因不明"


def _asked(t: dict) -> str:
    text = (t.get("text") or "").replace(VOICE_TEXT_PREFIX, "")
    if text.strip():
        return snippet(text, 18)
    return f"[{KIND_LABELS.get(t.get('kind'), '消息')}]"


def _notify_send_problem(task_id: str) -> None:
    """send_uncertain (maybe delivered) or exhausted (definitely not): tell
    the user directly and point at 「重发」. Marks send.notified so the hook
    does not also wake an executor for the same thing."""
    t = load_task(task_id)
    if not t or t.get("status") != "send_uncertain":
        return
    if t.get("kind") in ("notice", "command") \
            or (t.get("execution") or {}).get("proactive"):
        return
    send = t.get("send") or {}
    if send.get("notified") and send.get("notified") != "echo-wait":
        return
    when = user_clock(t.get("received_at"))
    if send.get("not_delivered") or send.get("exhausted"):
        err = str(send.get("error") or "")
        reason = _reason_cn(err.split(":", 1)[0].strip(), err)
        text = (f"你 {when} 发的「{_asked(t)}」，我的回复没能发出去：{reason}"
                f"（试了 {send.get('attempts') or '多'} 次）。回复「重发」我再发一次。")
        why = "send_failed"
    else:
        text = (f"你 {when} 发的「{_asked(t)}」，我的回复（「{snippet(t.get('result_text'), 14)}」开头）"
                "发送时网络中断，不确定你收到没有。没收到的话回复「重发」，我从断开的地方接着发。")
        why = "send_uncertain"
    nid = create_notice(t.get("from_user_id"), text, reason=why,
                        about={"task_id": task_id})
    with task_lock():
        fresh = load_task(task_id)
        if fresh and fresh.get("status") == "send_uncertain":
            rec = dict(fresh.get("send") or {})
            rec.pop("notify_after", None)
            if nid:
                rec["notified"] = nid
            else:
                rec.pop("notified", None)   # no notice possible: the hook steps in
            fresh["send"] = rec
            save_task(fresh)


def check_overdue(index: list) -> None:
    """Messages still unclaimed after 15 minutes mean the hook/executor
    pipeline is stuck: say so once - one notice for all of them, not one per
    message - instead of leaving the user guessing."""
    now = now_ms()
    # 2026-11-06: a message held in its batch is not waiting for Muse yet,
    # and one dispatched again (superseded) waits since its last dispatch.
    due = [s_ for s_ in index if s_.get("status") == "pending_execution"
           and not s_.get("overdue_notified") and not s_.get("batch_open")
           and now - max(s_.get("received_at") or now, s_.get("dispatched_at") or 0)
           >= OVERDUE_PENDING_MS]
    if not due:
        return
    marked = []
    with task_lock():
        for s_ in due:
            t = load_task(s_["message_id"])
            if not t or t.get("status") != "pending_execution" \
                    or t.get("overdue_notified"):
                continue
            t["overdue_notified"] = now
            save_task(t)
            marked.append(t)
    if not marked:
        return
    oldest = min(int(t.get("received_at") or now) for t in marked)
    if len(marked) == 1:
        what = f"你 {user_clock(oldest)} 发的「{_asked(marked[0])}」"
    else:
        what = (f"你从 {user_clock(oldest)} 起发的 {len(marked)} 条消息（"
                + "、".join(f"「{_asked(t)}」" for t in marked[:3])
                + ("等" if len(marked) > 3 else "") + "）")
    create_notice(marked[0].get("from_user_id"),
                  f"{what}还在排队：Muse 的执行者暂时没有接手（已经等了 "
                  f"{fmt_duration(now - oldest)}）。消息不会丢，恢复后会按顺序处理；"
                  "想确认桥接本身是否正常，可以发「状态」。",
                  reason="overdue", about={"task_ids": [t["message_id"] for t in marked]})


CORRUPT_REPORTED_FILE = STATE_DIR / "corrupt_reported.json"


def check_corrupt_tasks() -> None:
    """The hook quarantines unparsable inbox files as *.corrupt (one bad
    file used to stop every jq run). The message inside is lost unless
    somebody looks: tell the owner once per file (2026-11-04 review)."""
    try:
        names = sorted(p.name for p in INBOX_DIR.glob("*.corrupt"))
    except OSError:
        return
    seen = set(read_json(CORRUPT_REPORTED_FILE, []) or [])
    new = [n for n in names if n not in seen]
    if not new:
        return
    owner = (_SERVE_CREDS.get("creds") or load_creds() or {}).get("ilink_user_id")
    nid = create_notice(owner, (
        f"有 {len(new)} 条消息的本地记录损坏了，桥接没法处理（已隔离在 inbox/ 下的 "
        ".corrupt 文件里）。最近发的消息要是一直没回，请再发一次。"),
        reason="corrupt_task", about={"files": new[:10]})
    if nid:
        with contextlib.suppress(OSError):
            atomic_write_json(CORRUPT_REPORTED_FILE, sorted(seen | set(new))[-500:],
                              fsync=False)
    log(f"corrupt inbox files: {len(new)} new" + (" (owner told)" if nid else ""))


HELP_TEXT = ("可用的快捷指令（桥接直接回复，不经过 Muse）：\n"
             "· 状态 —— 桥接是否健康：登录、收发、积压、回复额度\n"
             "· 进度 —— 正在处理哪些消息、各用了多久\n"
             "· 取消 —— 停掉正在处理或排队的消息，结果不再发来\n"
             "· 继续 —— 回复因微信条数限制没发完时，接着发剩下的\n"
             "· 新话题 —— 不再带上之前的对话，从头开始\n"
             "· 重发 —— 把最近没发成功、或不确定送达的回复和文件再发一次"
             "（先引用那条失败提示再发「重发」，就只重发那一条）\n"
             "· 设置 —— 看当前设置；「设置 媒体不等」「设置 进度关」"
             "「设置 简略」等可以直接改，改完会回一句确认\n"
             "· 最近文件 —— 最近发过 / 收到 / 归档的文件（时间、文件名、大小）\n"
             "· 备份状态 —— 最新备份快照和最新归档包\n"
             "· 最近对话 —— 最近几轮对话原话\n"
             "· 统计 —— 最近 72 小时接手、处理、送达各用多久（数字原样来自 stats）\n"
             "· 查记录 <关键词> —— 在已落盘对话里找原话\n"
             "· 帮助 —— 显示这段说明\n"
             "其他消息照常交给 Muse 处理。")

_INFLIGHT = ("downloading", "pending_execution", "executing", "background")


def _inflight_tasks(user: str) -> list:
    return [s_ for s_ in scan_task_index()
            if s_.get("from_user_id") == user and s_.get("status") in _INFLIGHT
            and s_.get("kind") not in ("command", "notice")]


def cancel_command(user: str) -> str:
    """「取消」: stop what is in flight for this user. The tasks become
    `cancelled` (terminal): claim skips them, complete/progress tell the
    executor to stop, their queued files are dropped, typing stops."""
    done = []
    with task_lock():
        for s_ in _inflight_tasks(user):
            t = load_task(s_["message_id"])
            if not t or t.get("status") not in _INFLIGHT:
                continue
            ex = dict(t.get("execution") or {})
            ex.update(cancelled_at=now_ms(), cancelled_from=t.get("status"))
            t["execution"] = ex
            t["status"] = "cancelled"
            if t.get("bg"):          # 2026-11-07 v1.4: the background job sees it too
                _bg_close(t, "cancelled", now_ms())
            save_task(t)
            done.append(t)
    for t in done:
        for qid in t.get("attachments") or []:
            _cancel_qitem(qid)
    if not done:
        return "现在没有进行中的任务，不需要取消。"
    _typing_wake.set()
    names = "、".join(f"「{_asked(t)}」" for t in done[:3]) + ("等" if len(done) > 3 else "")
    log(f"cancelled {len(done)} task(s) on the user's request")
    return (f"好的，已取消 {len(done)} 条：{names}。已经开始的那一步可能还会跑完，"
            "但结果不会再发给你。")


def progress_command(user: str) -> str:
    """「进度」: what is in flight, for how long, and the executor's note."""
    now = now_ms()
    lines = []
    inflight = _inflight_tasks(user)
    for pos, s_ in enumerate(inflight):
        t = load_task(s_["message_id"]) or {}
        ex = t.get("execution") or {}
        since = int(ex.get("claimed_at") or t.get("received_at") or now)
        what = {"downloading": "下载中", "pending_execution": "排队等 Muse 接手",
                "executing": "处理中"}.get(t.get("status"), str(t.get("status")))
        if t.get("batch_open") and t.get("status") == "pending_execution":
            what = "合批中（你停顿几秒后和同批消息一起交给 Muse）"
        _bgx = t.get("bg") if t.get("status") == "background" else None
        if _bgx:            # 2026-11-07 v1.4: background - since it was queued
            since = int(_bgx.get("queued_at") or since)
            what = (f"后台排队（第 {bg_position(t['message_id'])} 位），等主代理接手"
                    if _bgx.get("state") == "queued"
                    else "后台处理中（主代理 + 子代理）")
            if _bgx.get("stage"):
                ex = {**ex, "stage": _bgx["stage"]}
            _nn = int((bg_read(t["message_id"]) or {}).get("note_seq") or 0)
            if _nn:         # v1.6: follow-ups are acknowledged once; the count is here
                what += f"（已收到 {_nn} 条补充）"
        rl = t.get("relay") if RELAY_ON else None
        if rl:              # 2026-11-07 relay: the whole chain, not this leg
            since = int(rl.get("started_at") or since)
            if t.get("relay_pending") and t.get("status") == "pending_execution":
                what = "交棒中（下一棒几秒内接手）"
        line = f"· 「{_asked(t)}」{what}，{fmt_duration(now - since)}"
        if t.get("status") == "pending_execution" and pos:
            line += f"（排队第 {pos + 1} 位，前面还有 {pos} 条）"
        where = _relay_where(t) if rl else ""
        if where:
            line += where
        elif ex.get("stage"):
            line += f"（正在：{snippet(ex['stage'], 40)}）"
        if ex.get("note"):
            line += f"（{snippet(ex['note'], 40)}）"
        lines.append(line)
    parked = _deferred_items(user)
    if parked:
        lines.append(f"· {parked} 项回复/文件因为微信的条数限制在等你下一条消息（发「继续」即可）")
    busy = [it for it in list_queue() if it.get("to_user") == user
            and it.get("status") in ("pending", "sending") and not it.get("deferred")]
    if busy:
        lines.append(f"· 文件：{len(busy)} 个在发送或等待重试")
    return ("进度：\n" + "\n".join(lines)) if lines else "现在没有进行中的任务。"


def newtopic_command(user: str) -> str:
    """「新话题」: the executor no longer gets the earlier turns and memo.
    2026-11-02: the ended topic first gets a one-line digest, so a later
    "那个" can still be traced back to it."""
    saved = topic_summary_append()
    memo_set("")
    history_append("system", "（新话题）", kind="topic")
    base = "好的，开始新话题：之前的对话和备忘不会再带给 Muse。"
    if saved:
        base += "上一话题已留一句摘要（约 %d 轮），以后提起来能找回。" % saved["turns"]
    return base


def status_summary_text(user: str | None = None) -> str:
    now = now_ms()
    creds = load_creds()
    hb = read_json(HEARTBEAT_FILE, {}) or {}
    session = read_json(SESSION_FILE, {}) or {}
    counts: dict = {}
    for t in scan_task_index():
        counts[t["status"]] = counts.get(t["status"], 0) + 1
    q = queue_counts()
    problems: list[str] = []
    lines = [f"微信桥接状态（v{BRIDGE_VERSION}，{user_clock(now, with_date=True)}）"]
    started = int(hb.get("serve_started_at") or 0)
    starts = [e for e in serve_starts_read() if e["at"] > now - 86_400_000]
    svc = f"运行中，已连续运行 {fmt_duration(now - started)}" if started else "运行中"
    if len(starts) > 1:
        by_reason: dict = {}
        for e in starts:
            by_reason[e["reason"]] = by_reason.get(e["reason"], 0) + 1
        breakdown = "、".join(f"{r} {n} 次" for r, n in sorted(by_reason.items()))
        rapid = sum(1 for a, b in zip(starts, starts[1:])
                    if b["at"] - a["at"] < 120_000)
        svc += f"（24 小时内启动过 {len(starts)} 次：{breakdown}"
        if rapid:
            svc += f"；其中间隔不到 2 分钟的快速重启 {rapid} 次"
        svc += "）"
    lines.append("· 服务：" + svc)
    paused = int(session.get("paused_until_ms") or 0)
    if not creds:
        login = "没有登录凭据"
        problems.append("需要重新扫码登录")
    elif (STATE_DIR / "auth_alert.json").exists():
        login = "可能已失效（连续收到会话过期）"
        problems.append("登录可能失效，需要重新扫码")
    elif paused > now:
        login = f"会话暂停中（-14），约 {fmt_duration(paused - now)}后恢复"
        problems.append("会话暂停中")
    else:
        saved = str(creds.get("saved_at") or "")
        login = "正常" + (f"（{saved[:10]} 扫码）" if saved else "")
    lines.append("· 登录：" + login)
    last_poll = int(hb.get("last_poll_ok_at") or 0)
    fails_1h = sum(1 for x in _POLL_FAIL_TIMES if x > now - 3_600_000)
    poll = (f"最近一次成功在 {fmt_duration(now - last_poll)}前" if last_poll
            else "还没有成功过")
    if fails_1h:
        poll += f"；近 1 小时失败 {fails_1h} 次"
    lines.append("· 收消息：" + poll)
    c, r = int(hb.get("net_connects") or 0), int(hb.get("net_reused") or 0)
    net = f"连接复用 {r}/{c + r}" if (c + r) else "暂无统计"
    rot = int(hb.get("proxy_rotations") or 0)
    if rot:
        net += f"，切换过 {rot} 次代理"
    mode_cn = {"proxy": "经代理", "direct": "直连", "urllib": "无连接池"}.get(
        str(hb.get("transport_mode") or ""), "")
    lines.append("· 网络：" + net + (f"，{mode_cn}" if mode_cn else ""))
    last_send = int(hb.get("last_send_ok_at") or 0)
    send = (f"最近一次成功在 {fmt_duration(now - last_send)}前" if last_send
            else "还没有发送记录")
    rl = rate_limited_until()
    if rl:
        send += f"；被限流，约 {fmt_duration(rl - now)}后解除"
        problems.append("发送被限流（-2）")
    lines.append("· 发消息：" + send)
    busy = [(counts.get(k, 0), name) for k, name in (
        ("downloading", "下载中"), ("pending_execution", "排队"),
        ("executing", "处理中"), ("waiting_send", "待发送"),
        ("send_failed", "等待重试"), ("send_uncertain", "不确定是否送达"))]
    lines.append("· 任务：" + ("、".join(f"{name} {n_}" for n_, name in busy if n_)
                                or "没有积压"))
    if counts.get("send_uncertain"):
        problems.append(f"{counts['send_uncertain']} 条回复不确定是否送达（回复「重发」可补发）")
    qs = "、".join(f"{name} {q[k]}" for k, name in (
        ("pending", "等待发送"), ("sending", "发送中"), ("failed", "失败"),
        ("uncertain", "不确定")) if q.get(k))
    if qs:
        lines.append("· 文件队列：" + qs)
    if q.get("failed") or q.get("uncertain"):
        problems.append("有文件没发成功（回复「重发」可再试）")
    if user:
        _, w_open, w_rem = window_state(user)
        if w_open is False:
            lines.append("· 主动消息窗口：已关闭（超过 24 小时没收到你的消息）")
            problems.append("主动提醒发不出去（回复任意内容即可恢复）")
        elif w_rem is not None:
            lines.append(f"· 主动消息窗口：还剩 {fmt_duration(w_rem)}")
        left = choose_context_token(user, None)[1]
        if left is not None:
            lines.append(f"· 回复额度：这条之后还能回 {max(0, left - 1)} 条"
                         f"（你每发一条消息，我最多能回 {CTX_QUOTA} 条）")
        parked = _deferred_items(user)
        if parked:
            lines.append(f"· 待续发：{parked} 项（发「继续」或任意内容就接着发）")
    lat = latency_summary()
    if lat:
        lines.append("· 响应：" + lat)
    lines.append("· 整体：" + ("正常" if not problems else "；".join(problems)))
    return "\n".join(lines)


def _quote_body(quote: str | None) -> str:
    if not quote:
        return ""
    return " ".join(quote.split(" | ", 1)[-1].split())[:40]


def _quote_matches(qbody: str, text: str | None) -> bool:
    t = " ".join((text or "").split())
    key = qbody[:30]
    return bool(key) and len(key) >= 4 and (t.startswith(key) or key in t[:600])


def _mark_task_for_resend(tid: str) -> bool:
    """A human-approved resend: a fresh attempt series that resumes at the
    first unconfirmed part (done_upto/pending are kept)."""
    with task_lock():
        t = load_task(tid)
        if not t or t.get("status") not in ("send_uncertain", "send_failed"):
            return False
        send = dict(t.get("send") or {})
        for k in ("no_auto_retry", "exhausted", "notified", "uncertain",
                  "deferred", "deferrals", "outbox_resends"):
            send.pop(k, None)
        send.update(attempts=0, max_attempts=SEND_MAX_ATTEMPTS_CONNECT,
                    next_retry_at=0,
                    manual_resends=int(send.get("manual_resends") or 0) + 1)
        t["status"] = "send_failed"
        t["send"] = send
        save_task(t)
    text_outbox_reconcile(tid)
    log(f"resend requested task={tid}")
    return True


def _mark_qitem_for_resend(qid: str) -> bool:
    with task_lock():
        it = load_qitem(qid)
        if not it or it.get("status") not in ("failed", "uncertain", "pending"):
            return False
        if not Path(it.get("path") or "").is_file():
            return False
        for k in ("final_error", "final_code", "notified", "finished_at",
                  "client_id"):
            it.pop(k, None)
        it.update(status="pending", rounds=0, next_at=0, deferrals=0,
                  waiter_until=0,
                  manual_resends=int(it.get("manual_resends") or 0) + 1)
        save_qitem(it)
    log(f"resend requested queue item={qid}")
    return True


def resend_command(user: str, quote: str | None = None) -> str:
    now = now_ms()
    tasks = [s_["message_id"] for s_ in scan_task_index()
             if s_.get("from_user_id") == user
             and s_.get("kind") not in ("command", "notice")
             and s_.get("status") in ("send_uncertain", "send_failed")
             and (s_.get("received_at") or 0) >= now - RESEND_WINDOW_MS]
    items = [it for it in list_queue()
             if it.get("to_user") == user
             and it.get("status") in ("failed", "uncertain")
             and int(it.get("created_at") or 0) >= now - QUEUE_KEEP_MS
             and Path(it.get("path") or "").is_file()]
    qbody = _quote_body(quote)
    if qbody:
        want_t, want_q = set(), set()
        for s_ in scan_task_index()[-300:]:
            if s_.get("from_user_id") != user:
                continue
            t = load_task(s_["message_id"]) or {}
            if not _quote_matches(qbody, t.get("result_text")):
                continue
            about = (t.get("execution") or {}).get("about") or {}
            if t.get("kind") == "notice":
                if about.get("task_id"):
                    want_t.add(about["task_id"])
                if about.get("qid"):
                    want_q.add(about["qid"])
            else:
                want_t.add(t["message_id"])
        if want_t or want_q:
            tasks = [x for x in tasks if x in want_t]
            items = [it for it in items
                     if it["qid"] in want_q or it.get("task_id") in want_t]
    lines = []
    for tid in tasks:
        if _mark_task_for_resend(tid):
            lines.append(f"回复「{snippet((load_task(tid) or {}).get('result_text'), 18)}」")
    for it in items:
        if _mark_qitem_for_resend(it["qid"]):
            label = "语音" if it.get("requested_kind") == "voice" \
                else KIND_LABELS.get(it.get("kind"), "文件")
            lines.append(f"{label}《{it.get('name')}》")
    if not lines:
        return ("现在没有需要重发的内容：最近 3 天的回复和文件都已经送达"
                "（或者正在自动重试）。")
    _send_wake.set()
    _queue_wake.set()
    body = "\n".join(f"{i}. {x}" for i, x in enumerate(lines, 1))
    return (f"好的，开始重发 {len(lines)} 项：\n{body}\n"
            "已经确认送达的部分不会重复发；还是发不出去的话我会再告诉你。")


def _fmt_size(n: int) -> str:
    for u in ("B", "KB", "MB", "GB"):
        if n < 1024 or u == "GB":
            return f"{n:.0f} {u}" if u == "B" else f"{n:.1f} {u}"
        n /= 1024
    return f"{n:.1f} GB"


def recent_dialogue_command() -> str:
    """Opt round 2 item 7: last dialogue turns, on-disk only, no LLM."""
    rows = [r for r in history_read()
            if r.get("kind") not in ("command", "topic", "notice")][-8:]
    if not rows:
        return "最近对话：没有已落盘的对话记录。"
    lines = ["最近对话（只读已落盘记录）："]
    for r in rows:
        who = "你" if r.get("role") == "user" else "Muse"
        lines.append(f"· {user_clock(r.get('at'), with_date=True)} {who}：{snippet(r.get('text') or '', 120)}")
    return "\n".join(lines)


def history_search_command(needle: str) -> str:
    """Opt round 2 item 7: keyword search over on-disk history, no LLM."""
    if not needle:
        return "查记录用法：查记录 <关键词>，例如“查记录 备份”。"
    hits, truncated = history_search(needle, 5,
                                     skip_kinds=("command", "topic", "notice"))
    if not hits:
        return (f"查记录“{needle}”：没有找到匹配的对话记录。"
                + ("（记录太多，只查了最近的一部分）" if truncated else ""))
    lines = [f"查记录“{needle}”（只读已落盘记录，最近 5 条）："]
    for r in hits:
        who = "你" if r.get("role") == "user" else "Muse"
        lines.append(f"· {user_clock(r.get('at'), with_date=True)} {who}：{snippet(r.get('text') or '', 120)}")
    return "\n".join(lines)


def recent_files_command() -> str:
    """Opt batch item 5: recent files, read only from on-disk records.
    Sent files come from the media queue (name/bytes/finished_at), received
    media from inbox_media, archives from the local archive folder
    (WECHAT_BRIDGE_ARCHIVE_DIR, default icloud-upload/). Never guesses."""
    entries = []  # (mtime_ms, label, size)
    try:
        for f in QUEUE_DIR.glob("*.json"):
            try:
                q = json.loads(f.read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            if q.get("status") == "sent" and q.get("name"):
                entries.append((int(q.get("finished_at") or q.get("created_at") or 0),
                                f"已发：{q['name']}", int(q.get("bytes") or 0)))
    except OSError:
        pass
    try:
        for f in MEDIA_DIR.glob("*"):
            # derived files (previews, extracted audio, thumbnails) are not
            # something the user sent
            if f.is_file() and ".preview." not in f.name and not f.name.endswith(
                    ("_thumb.jpg", ".plain")) and not (f.suffix == ".wav" and any(
                    f.with_suffix(x).exists() for x in (".mp4", ".bin", ".silk", ".amr"))):
                entries.append((int(f.stat().st_mtime * 1000),
                                f"收到：{f.name}", f.stat().st_size))
    except OSError:
        pass
    try:
        archive_dir = BASE_DIR / os.environ.get(
            "WECHAT_BRIDGE_ARCHIVE_DIR", "icloud-upload")
        for f in archive_dir.glob("*"):
            if f.is_file() and f.suffix != ".txt":
                entries.append((int(f.stat().st_mtime * 1000),
                                f"归档包：{f.name}", f.stat().st_size))
    except OSError:
        pass
    entries.sort(reverse=True)
    if not entries:
        return "最近文件：没有已落盘的文件记录。"
    lines = ["最近文件（时间、文件名、大小，只读已落盘记录）："]
    for ms, label, size in entries[:10]:
        lines.append(f"· {user_clock(ms, with_date=True)} {label}（{_fmt_size(size)}）")
    return "\n".join(lines)


def _tree_size(root: Path, budget: dict) -> int:
    """Size of a tree, hard links counted once, within a shared budget of
    files and time (「备份状态」 runs in the poll thread)."""
    total, stack = 0, [root]
    while stack:
        if budget["files"] <= 0 or time.monotonic() > budget["deadline"]:
            budget["partial"] = True
            break
        try:
            with os.scandir(stack.pop()) as it:
                for e in it:
                    try:
                        if e.is_dir(follow_symlinks=False):
                            stack.append(Path(e.path))
                        elif e.is_file(follow_symlinks=False):
                            st = e.stat(follow_symlinks=False)
                            budget["files"] -= 1
                            if (st.st_dev, st.st_ino) not in budget["seen"]:
                                budget["seen"].add((st.st_dev, st.st_ino))
                                total += st.st_size
                    except OSError:
                        continue
        except OSError:
            continue
    return total


def backup_status_command() -> str:
    """Opt batch item 3: latest backup / upload result, deterministic only."""
    bdir = BASE_DIR / "backups"
    dated, named = [], []
    total = 0
    budget = {"files": 50_000, "deadline": time.monotonic() + 2.0,
              "seen": set(), "partial": False}
    try:
        for d in bdir.iterdir():
            if not d.is_dir() or d.name.startswith("."):
                continue
            sz = _tree_size(d, budget)
            total += sz
            (dated if len(d.name) == 15 and d.name[8] == "-" else named).append((d.name, int(d.stat().st_mtime * 1000), sz))
    except OSError:
        pass
    dated.sort(reverse=True)
    lines = ["备份状态（只读落盘记录）："]
    if dated:
        n, ms, sz = dated[0]
        lines.append(f"· 最新带日期快照：{n}（{_fmt_size(sz)}）")
        lines.append(f"· 带日期快照共 {len(dated)} 份，命名快照 {len(named)} 份，合计约 {_fmt_size(total)}"
                     + ("（文件太多，只统计了一部分）" if budget["partial"] else ""))
        cand = bdir / ".prune-candidates.txt"
        if cand.exists():
            lines.append("· 超出 5 份的待确认清理清单在 backups/.prune-candidates.txt，尚未自动删除")
    else:
        lines.append("· 暂无带日期快照")
    try:
        ups = sorted((BASE_DIR / "icloud-upload").glob("*.zip"),
                     key=lambda f: f.stat().st_mtime, reverse=True)
        if ups:
            f = ups[0]
            lines.append(f"· 最新归档包：{f.name}（{_fmt_size(f.stat().st_size)}，{user_clock(int(f.stat().st_mtime * 1000), with_date=True)}）")
    except OSError:
        pass
    return "\n".join(lines)


def run_command(command: str, user: str, quote: str | None = None) -> str:
    if command == "status":
        return status_summary_text(user)
    if command == "resend":
        return resend_command(user, quote)
    if command == "cancel":
        return cancel_command(user)
    if command == "progress":
        return progress_command(user)
    if command == "newtopic":
        return newtopic_command(user)
    if command == "continue":
        return ""       # the parked replies resumed by this message answer it
    if command == "recent_files":
        return recent_files_command()
    if command == "backup_status":
        return backup_status_command()
    if command == "recent_dialogue":
        return recent_dialogue_command()
    if command == "stats":
        return stats_command()
    if command.startswith("history_search:"):
        return history_search_command(command[len("history_search:"):].strip())
    return HELP_TEXT


# --------------------------------------------------------------------------
# Sending (single flight per task; timeout => uncertain, never blind retry)
# --------------------------------------------------------------------------

SEND_CHUNK_LIMIT = 4000          # official plugin outbound textChunkLimit
SEND_CHUNK_SAFE = 1800           # the old limit: fallback if a long part is rejected
SEND_TUNING_FILE = STATE_DIR / "send_tuning.json"
SEND_TUNING_TTL_MS = 24 * 3600 * 1000
SEND_MAX_ATTEMPTS = 3
SEND_RETRY_DELAYS_MS = (60_000, 180_000)
# Failing before the request left this host is a guaranteed non-delivery
# (and the common failure on this flaky egress): retry sooner and more often.
SEND_MAX_ATTEMPTS_CONNECT = 6
SEND_CONNECT_RETRY_DELAYS_MS = (10_000, 30_000, 120_000, 300_000, 900_000)
# iLink ret/errcode -2: bot-wide rate limit (wechat-ilink: retry after 90 s,
# lifted early when the user sends a message). ilink-hub also saw -2
# "prepare failed" for media sent without iLink-App-* headers (we send them).
RATE_LIMIT_ERRCODE = -2
RATE_LIMIT_FILE = STATE_DIR / "ratelimit.json"
RATE_LIMIT_RETRY_MS = 90_000
RATE_LIMIT_POLL_RETRY_SEC = 30


def split_spans(text: str, limit: int = SEND_CHUNK_LIMIT) -> list[tuple[int, int]]:
    """(start, end) spans of `text` to send in order: paragraphs packed up
    to `limit` characters, over-long paragraphs hard-cut, newlines at span
    edges dropped - the same parts the old split_text produced, but as
    offsets, so a resend can continue after the last confirmed part."""
    n = len(text)
    if n <= limit:
        return [(0, n)]
    spans: list[tuple[int, int]] = []
    cur = None
    pos = 0
    while True:
        nl = text.find("\n", pos)
        pend = n if nl < 0 else nl
        if pend - pos <= limit:
            pieces = [(pos, pend)]
        else:
            pieces = [(i, min(i + limit, pend)) for i in range(pos, pend, limit)]
        for ps, pe in pieces:
            if cur is None:
                cur = (ps, pe)
            elif pe - cur[0] <= limit:
                cur = (cur[0], pe)
            else:
                spans.append(cur)
                cur = (ps, pe)
        if nl < 0:
            break
        pos = nl + 1
    if cur is not None:
        spans.append(cur)
    out = []
    for s, e in spans:
        while s < e and text[s] == "\n":
            s += 1
        while e > s and text[e - 1] == "\n":
            e -= 1
        if text[s:e].strip():
            out.append((s, e))
    return out or [(0, n)]


def split_text(text: str, limit: int = SEND_CHUNK_LIMIT) -> list[str]:
    """Split long replies into sequential chunks at natural boundaries."""
    return [text[s:e] for s, e in split_spans(text, limit)]


def _resume_offset(text: str, done_upto: int) -> int:
    i = max(0, min(int(done_upto or 0), len(text)))
    while 0 < i < len(text) and text[i] == "\n":
        i += 1
    return i


def _spans_from(text: str, start: int, limit: int) -> list[tuple[int, int]]:
    if start >= len(text):
        return [] if start else [(0, 0)]
    return [(s + start, e + start) for s, e in split_spans(text[start:], limit)]


def _chunk_limit() -> int:
    t = read_json(SEND_TUNING_FILE, {}) or {}
    lim = t.get("chunk_limit")
    if isinstance(lim, int) and now_ms() - int(t.get("at") or 0) < SEND_TUNING_TTL_MS:
        return max(200, min(lim, SEND_CHUNK_LIMIT))
    return SEND_CHUNK_LIMIT


def _downgrade_chunk_limit() -> None:
    with contextlib.suppress(OSError):
        atomic_write_json(SEND_TUNING_FILE,
                          {"chunk_limit": SEND_CHUNK_SAFE, "at": now_ms()},
                          fsync=False)


def rate_limited_until() -> int:
    until = int((read_json(RATE_LIMIT_FILE, {}) or {}).get("until_ms") or 0)
    return until if until > now_ms() else 0


RATE_LIMIT_MAX_MS = 10 * 60 * 1000


def note_rate_limit(n: int = 0) -> int:
    """Global send deferral after a genuine rate limit. v5: grows with
    consecutive deferrals (90 s, 3 min, 6 min, capped at 10 min) instead of
    probing every 90 s for up to an hour; a user message lifts it at once."""
    wait = min(RATE_LIMIT_RETRY_MS * (2 ** max(0, min(int(n or 0), 6))),
               RATE_LIMIT_MAX_MS)
    until = now_ms() + wait
    with contextlib.suppress(OSError):
        atomic_write_json(RATE_LIMIT_FILE, {"until_ms": until, "at": now_ms()},
                          fsync=False)
    log(f"iLink rate limit (-2): deferring sends for {wait // 1000}s")
    return until


def clear_rate_limit() -> None:
    if RATE_LIMIT_FILE.exists():
        with contextlib.suppress(OSError):
            RATE_LIMIT_FILE.unlink()


SEND_DEFER_MAX = 40          # -2 / -14 waits that do not use up retries
SESSION_DEFER_MS = 10 * 60 * 1000


def _record_send_progress(task_id: str, started_at: int, **fields) -> None:
    """Persist per-part progress of an in-flight send, plus progress_at so a
    long but live send is not mistaken for a dead one. No fsync: losing it
    only means a later resend repeats one part."""
    fields.setdefault("progress_at", now_ms())
    try:
        with task_lock():
            t = load_task(task_id)
            if not t or t.get("status") != "sending":
                return
            send = t.get("send") or {}
            if send.get("started_at") != started_at:
                return
            for k, v in fields.items():
                if v is None:
                    send.pop(k, None)
                else:
                    send[k] = v
            t["send"] = send
            atomic_write_json(task_path(task_id), t, mode=0o600, fsync=False)
    except OSError as e:
        log(f"send progress save failed: {type(e).__name__}")



# --------------------------------------------------------------------------
# Reply budget and the 24 h window (v5)
#
# The ClawBot channel has two hard limits, both answered with ret=-2:
#   - the 24 h window: WeChat only delivers bot messages within 24 h of the
#     user's last message (the ClawBot settings page says so; Tencent's reply
#     in openclaw-weixin#204). Outside it every send is refused until the user
#     writes again. v4 read -2 as "rate limit" and re-tried every 90 s for up
#     to an hour, and send-direct alerts silently went nowhere;
#   - a per-message reply budget: about 10 sendmessage calls per user message
#     (context_token); the 11th is refused (field reports: dsh-im#484, cc2wx,
#     tian-agent). Text parts, captions, files, notices and progress notes all
#     count.
# The bridge accounts the budget per context_token (granted by each inbound
# message), checks the window before sending, keeps the last slots for the
# final reply (progress notes/notices may not take them), and when a reply
# does not fit it sends what fits, ends the last part with a one-line note
# and PARKS the rest: the user's next message (any message, or 「继续」)
# brings a fresh budget and the parked parts/files go out first. A -2 is
# classified (window / budget / genuine rate limit) and each class waits
# differently; every -2 is logged with its context so the limit can be
# calibrated on this account (`bridge.py stats`).
# WECHAT_BRIDGE_CTX_QUOTA=0 turns local accounting off (the server decides).
# --------------------------------------------------------------------------

CTX_QUOTA = _env_int("WECHAT_BRIDGE_CTX_QUOTA", 10)
QUOTA_RESERVE = {"reply": 0, "notice": 1, "progress": 3}  # opt batch item 1: keep 3 slots for the final reply
QUOTA_FILE = STATE_DIR / "send_quota.json"
REPLY_WINDOW_MS = 24 * 3600 * 1000
REPLY_WINDOW_MARGIN_MS = 2 * 60 * 1000
QUOTA_PROBE_MS = (10 * 60_000, 30 * 60_000, 60 * 60_000)
FAR_FUTURE_MS = 1 << 52
_PARKED = ("quota", "window", "rate_limit")
_DEFER_REASON = {
    "quota": "reply budget of this user message used up; resumes on the next user message",
    "window": "24 h reply window closed; resumes when the user writes again",
    "rate_limit": "iLink rate limit",
    "session": "session paused (-14)",
}


def _tok_key(token: str | None) -> str | None:
    return hashlib.sha256(token.encode()).hexdigest()[:16] if token else None


def _quota_load() -> dict:
    q = read_json(QUOTA_FILE, {}) or {}
    if not isinstance(q.get("tokens"), dict):
        q["tokens"] = {}
    return q


def _quota_save(q: dict) -> None:
    with contextlib.suppress(OSError):
        atomic_write_json(QUOTA_FILE, q, fsync=False)


def quota_grant(user: str, token: str | None, mid: str | None = None) -> None:
    """A user message: a fresh reply budget on its context_token."""
    if CTX_QUOTA <= 0 or not token:
        return
    now = now_ms()
    with task_lock():
        q = _quota_load()
        toks = q["tokens"]
        toks[_tok_key(token)] = {"user": user, "at": now, "used": 0, "mid": mid}
        live = sorted(((k, v) for k, v in toks.items() if isinstance(v, dict)
                       and now - int(v.get("at") or 0) < REPLY_WINDOW_MS + 3_600_000),
                      key=lambda kv: -int(kv[1].get("at") or 0))[:64]
        q["tokens"] = dict(live)
        _quota_save(q)


def quota_remaining(token: str | None) -> int | None:
    """Sends left on this token; None = not accounted (accounting off, no
    token, or a token this bridge never saw arrive): the server decides."""
    if CTX_QUOTA <= 0 or not token:
        return None
    e = _quota_load()["tokens"].get(_tok_key(token))
    if not isinstance(e, dict):
        return None
    if now_ms() - int(e.get("at") or 0) >= REPLY_WINDOW_MS:
        return 0
    return max(0, CTX_QUOTA - int(e.get("used") or 0))


def quota_take(token: str | None, priority: str = "reply") -> bool:
    """Reserve one send. Progress notes and notices may not take the last
    slots, which stay for the final reply."""
    if CTX_QUOTA <= 0 or not token:
        return True
    with task_lock():
        q = _quota_load()
        e = q["tokens"].get(_tok_key(token))
        if not isinstance(e, dict):
            return True
        left = (0 if now_ms() - int(e.get("at") or 0) >= REPLY_WINDOW_MS
                else CTX_QUOTA - int(e.get("used") or 0))
        if left - QUOTA_RESERVE.get(priority, 0) <= 0:
            return False
        e["used"] = int(e.get("used") or 0) + 1
        _quota_save(q)
        return True


def quota_refund(token: str | None) -> None:
    """The send that took a slot provably did not go out."""
    if CTX_QUOTA <= 0 or not token:
        return
    with task_lock():
        q = _quota_load()
        e = q["tokens"].get(_tok_key(token))
        if isinstance(e, dict) and int(e.get("used") or 0) > 0:
            e["used"] = int(e["used"]) - 1
            _quota_save(q)


def quota_exhaust(token: str | None) -> None:
    """The server refused a send although the budget was not used up: trust
    the server, and remember what it allowed (calibration, `stats`)."""
    if CTX_QUOTA <= 0 or not token:
        return
    with task_lock():
        q = _quota_load()
        e = q["tokens"].get(_tok_key(token))
        if isinstance(e, dict):
            e["observed_limit"] = max(0, int(e.get("used") or 0) - 1)
            e["used"] = CTX_QUOTA
            _quota_save(q)


def window_state(user: str | None) -> tuple:
    """(last_inbound_ms, open, remaining_ms) of the 24 h reply window. open
    is None when this bridge never saw the user write (unknown: try)."""
    if not user:
        return None, None, None
    entry = (read_json(CONTEXT_TOKENS_FILE, {}) or {}).get(user) or {}
    at = int(entry.get("at") or 0) if isinstance(entry, dict) else 0
    if not at:
        return None, None, None
    rem = at + REPLY_WINDOW_MS - REPLY_WINDOW_MARGIN_MS - now_ms()
    return at, rem > 0, rem


def choose_context_token(user: str | None, task_ctx: str | None) -> tuple:
    """(context_token, slots_left or None). The task's own token first (the
    official plugin answers with the inbound message's token); when its
    budget is used up, the newest token of the same user."""
    latest = None
    if user:
        entry = (read_json(CONTEXT_TOKENS_FILE, {}) or {}).get(user) or {}
        latest = entry.get("token") if isinstance(entry, dict) else None
    cands = list(dict.fromkeys(t for t in (task_ctx, latest) if t))
    if not cands and user:
        tok = latest_context_token(user)[0]
        cands = [tok] if tok else []
    if not cands:
        return None, None
    first_left = None
    for i, tok in enumerate(cands):
        left = quota_remaining(tok)
        if i == 0:
            first_left = left
        if left is None or left > 0:
            return tok, left
    return cands[0], first_left


def classify_rejection(exc, user: str | None, token: str | None) -> str:
    """What a ret=-2 most likely means: "window" (24 h since the user's last
    message), "rate_limit" (the server says so, or nothing is accounted), else
    "quota" (this user message's reply budget is used up)."""
    _, open_, _ = window_state(user)
    msg = (getattr(exc, "errmsg", "") or "").lower()
    if open_ is False:
        why = "window"
    elif "rate" in msg or "limit" in msg or "频" in msg:
        why = "rate_limit"
    elif quota_remaining(token) is None:
        why = "rate_limit"
    else:
        why = "quota"
    e = (_quota_load()["tokens"].get(_tok_key(token)) or {}) if token else {}
    log(f"-2 from iLink classified as {why}: used_on_token={e.get('used')} "
        f"token_age_min={(now_ms() - int(e.get('at') or now_ms())) // 60000} "
        f"window_open={open_} errmsg={_scrub(msg)[:40] or '-'}")
    return why


def _defer_until(why: str, n: int) -> int:
    now = now_ms()
    if why == "rate_limit":
        return note_rate_limit(n)
    if why == "session":
        paused = int((read_json(SESSION_FILE, {}) or {}).get("paused_until_ms") or 0)
        return max(paused, now + SESSION_DEFER_MS)
    if why == "quota" and n < len(QUOTA_PROBE_MS):
        return now + QUOTA_PROBE_MS[n]
    return FAR_FUTURE_MS            # only the user's next message re-opens it


def _deferred_items(user: str | None) -> int:
    if not user:
        return 0
    n = sum(1 for s_ in scan_task_index()
            if s_.get("from_user_id") == user and s_.get("status") == "send_failed"
            and (s_.get("send") or {}).get("deferred") in ("quota", "window")
            and not (s_.get("send") or {}).get("no_auto_retry"))
    n += sum(1 for it in list_queue()
             if it.get("to_user") == user and it.get("status") == "pending"
             and it.get("deferred") in ("quota", "window"))
    return n


def _has_deferred(user: str | None) -> bool:
    return _deferred_items(user) > 0


def _resume_deferred(user: str) -> int:
    """A user message: undelivered reply TEXTS become due now - through
    state/outbox.json (text_outbox_resend, 2026-11-06; parked ones exactly as
    before, plus failed / exhausted ones) - and FILES parked by the reply
    budget, the window or a rate limit in the media queue (outbox/queue/,
    unchanged). The sender works oldest first."""
    n = text_outbox_resend(user)
    m = 0
    with task_lock():
        for it in list_queue():
            if (it.get("to_user") == user and it.get("status") == "pending"
                    and it.get("deferred") in _PARKED):
                it.update(next_at=0, deferrals=0)
                it.pop("deferred", None)
                save_qitem(it)
                m += 1
    if m:
        log(f"resuming {m} parked file send(s) after a new user message")
        _send_wake.set()
        _queue_wake.set()
    return n + m


# --------------------------------------------------------------------------
# Failed-text outbox: state/outbox.json (2026-11-06, feature 3)
#
# Reply TEXTS that could not be sent and were provably not delivered are
# listed in state/outbox.json. Only texts: outbound files keep their own
# queue under outbox/queue/ (v4) - a different file, directory and code path;
# neither reads or writes the other's (tests/test_batch_20261106.py checks).
# An entry is written when a send attempt ends undelivered:
#   parked    - reply budget / 24 h window / rate limit / -14 pause;
#   retrying  - failed, the next automatic retry is scheduled;
#   exhausted - every attempt failed (send_uncertain + exhausted).
# It is dropped when the text is delivered, cancelled or replaced, when it
# turns maybe-delivered (send_uncertain after a timeout: never resent
# automatically - duplicate risk, unchanged), or after 72 h (the 「重发」
# horizon). The user's next message - any message - makes every entry of
# theirs due at once (text_outbox_resend, called from ingest); the sender
# thread sends it right away, before the reply to the new message. Bounds,
# so a link that stays broken cannot turn messages into a resend storm:
#   - parked texts resume on every message, as since v5 (that is how a reply
#     longer than one message's budget continues; nothing new here);
#   - a failed or exhausted text gets at most TEXT_OUTBOX_AUTO_MAX (3)
#     message-triggered resends. Each is one more attempt under the original
#     policy (same attempt counter and backoff, exhausted again ->
#     send_uncertain); its "not delivered" notice is not repeated, and the
#     hook is not woken again for it;
#   - 「重发」 (a human decision) still starts a fresh series, as before.
# The task file stays the source of truth (text, done_upto - a resend
# continues at the first unconfirmed part); the entry carries the
# undelivered text so `cat state/outbox.json` shows what is waiting.
# --------------------------------------------------------------------------
TEXT_OUTBOX_FILE = STATE_DIR / "outbox.json"
TEXT_OUTBOX_AUTO_MAX = 3
TEXT_OUTBOX_KEEP_MS = RESEND_WINDOW_MS
_TEXT_OUTBOX_ABOUT = ("失败文本补发队列：只放发不出去的回复文字（文件/图片的出站队列是 "
                      "outbox/queue/，与本文件无关）。对方下一条消息到达即补发。")


def _text_outbox_state(t) -> str | None:
    """Why a task's text belongs in outbox.json ("parked" / "retrying" /
    "exhausted"), or None. Works on a task or an index summary. Same scope
    as 「重发」: replies, not commands or notices."""
    if not isinstance(t, dict) or t.get("kind") in ("command", "notice"):
        return None
    send = t.get("send") or {}
    if t.get("status") == "send_failed":
        if send.get("no_auto_retry"):
            return None     # e.g. proactive, window closed: reported in the App
        return "parked" if send.get("deferred") else "retrying"
    if t.get("status") == "send_uncertain" and send.get("exhausted") \
            and send.get("not_delivered"):
        return "exhausted"
    return None


def _text_outbox_load() -> dict:
    d = read_json(TEXT_OUTBOX_FILE, None)
    if not isinstance(d, dict) or not isinstance(d.get("texts"), list):
        d = {"version": 1, "texts": []}
    d["texts"] = [e for e in d["texts"] if isinstance(e, dict) and e.get("task_id")]
    return d


def _text_outbox_save(d: dict) -> None:
    d.update(version=1, about=_TEXT_OUTBOX_ABOUT, updated_at=now_ms())
    atomic_write_json(TEXT_OUTBOX_FILE, d, fsync=False)


def _text_outbox_entry(t: dict, state: str, old: dict | None, now: int) -> dict:
    text = t.get("result_text") or ""
    send = t.get("send") or {}
    left = text[_resume_offset(text, int(send.get("done_upto") or 0)):]
    e = dict(old or {})
    e.update(task_id=str(t["message_id"]), user=t.get("from_user_id"), state=state,
             reason=str(send.get("deferred") or send.get("error") or state)[:120],
             attempts=int(send.get("attempts") or 0), received_at=t.get("received_at"),
             auto_resends=int(send.get("outbox_resends") or 0),
             chars_left=len(left), text=left, updated_at=now)
    e.setdefault("added_at", now)
    return e


def text_outbox_reconcile(task_id: str) -> None:
    """After a send state change: list the task's undelivered text in
    outbox.json, refresh its entry, or drop it."""
    try:
        with task_lock():
            now = now_ms()
            t = load_task(task_id)
            st = (_text_outbox_state(t) if t and int(t.get("received_at") or 0)
                  >= now - TEXT_OUTBOX_KEEP_MS else None)
            d = _text_outbox_load()
            old = next((e for e in d["texts"] if e["task_id"] == str(task_id)), None)
            if st is None and old is None:
                return
            d["texts"] = [e for e in d["texts"] if e["task_id"] != str(task_id)]
            if st:
                d["texts"].append(_text_outbox_entry(t, st, old, now))
            _text_outbox_save(d)
    except OSError as e:
        log(f"text outbox update failed: {type(e).__name__}")
        return
    if st and not old:
        log(f"text outbox: task={task_id} listed ({st})")
    elif old and not st:
        log(f"text outbox: task={task_id} removed "
            f"(status {(t or {}).get('status', 'gone')})")


def text_outbox_sync() -> int:
    """Make outbox.json match the task files: list undelivered texts it
    misses (stored before this package, a crash between the two writes),
    drop entries that went out, were cancelled, turned uncertain or are older
    than 72 h. Index summaries only; a task file is read just for an entry
    that is added or whose state changed. Returns the number of entries."""
    now = now_ms()
    want = {}
    for s_ in scan_task_index():
        if int(s_.get("received_at") or 0) < now - TEXT_OUTBOX_KEEP_MS:
            continue
        st = _text_outbox_state(s_)
        if st:
            want[str(s_["message_id"])] = st
    with task_lock():
        d = _text_outbox_load()
        have = {e["task_id"]: e for e in d["texts"]}
        if not want and not have:
            return 0
        out, changed = [], False
        for mid, st in want.items():
            e = have.get(mid)
            if e and e.get("state") == st:
                out.append(e)
                continue
            t = load_task(mid)
            st2 = _text_outbox_state(t)
            if st2:
                out.append(_text_outbox_entry(t, st2, e, now))
            changed = True
        if set(have) - set(want):
            changed = True
        if changed:
            d["texts"] = sorted(out, key=lambda e: (int(e.get("received_at") or 0), e["task_id"]))
            _text_outbox_save(d)
    return len(out)


def text_outbox_resend(user: str) -> int:
    """The user wrote again: every outbox.json text of theirs is due now
    (the sender thread picks it up within ~1 s; oldest first, so it arrives
    before the reply to the new message). Parked ones resume as before;
    failed / exhausted ones get one more attempt each, at most
    TEXT_OUTBOX_AUTO_MAX times per text. Returns how many were made due."""
    if not user:
        return 0
    try:
        text_outbox_sync()
    except OSError as e:
        log(f"text outbox sync failed: {type(e).__name__}")
    armed: list = []
    now = now_ms()
    with task_lock():
        d = _text_outbox_load()
        for e in d["texts"]:
            if e.get("user") != user:
                continue
            t = load_task(e["task_id"])
            st = _text_outbox_state(t)
            if not st:
                continue
            send = dict(t.get("send") or {})
            auto = int(send.get("outbox_resends") or 0)
            counted = True
            if st == "parked":
                if send.get("deferred") not in _PARKED:
                    continue            # -14 session pause: it has its own wait
                send.update(next_retry_at=0, deferrals=0)
                counted = False         # v5 behaviour: every message resumes it
            elif auto >= TEXT_OUTBOX_AUTO_MAX:
                continue                # bounded: kept for 「重发」, no more automatic tries
            elif st == "retrying":
                if int(send.get("next_retry_at") or 0) <= now:
                    continue            # already due: the sender has it
                send["next_retry_at"] = 0
            else:                       # exhausted: one more attempt, same policy
                for k in ("no_auto_retry", "exhausted"):
                    send.pop(k, None)
                send.update(max_attempts=int(send.get("attempts") or 0) + 1,
                            next_retry_at=0)
                t["status"] = "send_failed"
            if counted:
                send["outbox_resends"] = auto + 1
                e.update(auto_resends=auto + 1, last_resend_at=now)
            t["send"] = send
            save_task(t)
            armed.append(e["task_id"])
        if armed:
            _text_outbox_save(d)
    if armed:
        log(f"text outbox: the user wrote again - {len(armed)} undelivered "
            f"text(s) due now {armed}")
        _send_wake.set()
    return len(armed)


def text_outbox_summary() -> dict:
    d = _text_outbox_load()
    now = now_ms()
    by = {}
    for e in d["texts"]:
        by[e.get("state")] = by.get(e.get("state"), 0) + 1
    oldest = min((int(e.get("added_at") or now) for e in d["texts"]), default=None)
    return {"file": "state/outbox.json", "texts": len(d["texts"]), "by_state": by,
            "oldest_age_min": round((now - oldest) / 60000) if oldest else None}


DEDUPE_FILE = STATE_DIR / "dedupe.json"


def _dedupe_confirmed() -> bool:
    """`canary-dedupe --confirm yes` recorded that WeChat drops a resend
    with the same client_id: uncertain parts may then be resent at once."""
    return bool((read_json(DEDUPE_FILE, {}) or {}).get("confirmed"))


# --------------------------------------------------------------------------
# Outbound Markdown (v5)
#
# The WeChat client renders standard Markdown in ClawBot chats (real-client
# test of 12 element types, cc-connect#1766, 2026-08-28: bold/italic/strike,
# inline code, fenced code with language label + highlighting + copy button,
# lists, quotes, TABLES, links, rules; headings with little size difference;
# task lists not rendered). The official StreamingMarkdownFilter therefore
# only removes what does not render: emphasis markers around CJK text, H5/H6
# markers, images. v4 sent the executor's Markdown untouched and - the real
# defect - its splitter could cut a code fence in half: the next part's
# leading ``` then opened a NEW block that swallowed the following text.
# render_for_wechat() applies the official rules (+ task lists -> ☐/☑, remote
# images -> links); _plan_parts() splits fence- and table-aware.
# WECHAT_BRIDGE_MARKDOWN=off sends text untouched; =plain strips Markdown.
# --------------------------------------------------------------------------

MARKDOWN_MODE = (os.environ.get("WECHAT_BRIDGE_MARKDOWN") or "filter").strip().lower()
AUTO_ATTACH = os.environ.get("WECHAT_BRIDGE_AUTO_ATTACH", "1") != "0"
AUTO_ATTACH_MAX = 9
SEND_TAIL_ROOM = 160
_CJK = ("\u2e80-\u2fff\u3000-\u303f\u3040-\u30ff\u3100-\u312f"
        "\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uff00-\uffef")
_FENCE_RE = re.compile(r"^ {0,3}(`{3,}|~{3,})")
_H56_RE = re.compile(r"^( {0,3})#{5,6}[ \t]+")
_TASK_RE = re.compile(r"^(\s*(?:[-*+]|\d+[.)])\s+)\[([ xX])\][ \t]+")
_IMG_RE = re.compile(r"!\[([^\]\n]*)\]\(\s*<?([^)\s>]+)>?(?:\s+\"[^\"\n]*\")?\s*\)")
_BI_CJK_RE = re.compile(rf"(?<![*_])(\*\*\*|___)(?=\S)([^*_\n]*?[{_CJK}][^*_\n]*?)(?<=\S)\1(?![*_])")
_I_STAR_CJK_RE = re.compile(rf"(?<!\*)\*(?![*\s])([^*\n]*?[{_CJK}][^*\n]*?)(?<![\s*])\*(?!\*)")
_I_UND_CJK_RE = re.compile(rf"(?<![\w_])_(?![_\s])([^_\n]*?[{_CJK}][^_\n]*?)(?<![\s_])_(?![\w_])")
_INLINE_CODE_SPLIT = re.compile(r"(`+[^`\n]*?`+)")
_LOCAL_REF_RE = re.compile(r"(!?)\[([^\]\n]{0,200})\]\(\s*<?(/[^)\s>]+|~/[^)\s>]+)>?\s*\)")
_ATTACH_DENY = ("credential", "secret", "password", "passwd", "token", ".env",
                "id_rsa", "id_ed25519", "id_ecdsa", ".pem", ".key", "proxy",
                "cookie", "netrc", "pgpass", "kubeconfig", "authorized_keys",
                "known_hosts", ".p12", ".pfx", ".jks", ".kdbx", "keystore",
                "shadow")
# Bridge directories whose files never go out implicitly (credentials, the
# chat log and quote store, task records with context tokens, backups ...).
_ATTACH_PRIVATE_DIRS = ("state", "inbox", "history", "backups", "logs",
                        "outbox", "vendor", "icloud-upload")


def _fence_closes(line: str, marker: str) -> bool:
    s = line.strip()
    return bool(s) and s[0] == marker[0] and len(s) >= len(marker) \
        and not s.strip(marker[0])


def _img_sub(m) -> str:
    alt, url = m.group(1).strip(), m.group(2)
    if url.startswith(("http://", "https://")):
        return f"[{alt or '图片'}]({url})"
    return alt


def _render_inline(seg: str, mode: str) -> str:
    seg = _IMG_RE.sub(_img_sub, seg)
    seg = _BI_CJK_RE.sub(r"\2", seg)
    seg = _I_STAR_CJK_RE.sub(r"\1", seg)
    seg = _I_UND_CJK_RE.sub(r"\1", seg)
    if mode == "plain":
        seg = re.sub(r"(\*\*|__)(?=\S)(.+?)(?<=\S)\1", r"\2", seg)
        seg = re.sub(r"(?<!\*)\*(?=\S)([^*\n]+?)(?<=\S)\*(?!\*)", r"\1", seg)
        seg = re.sub(r"~~(?=\S)(.+?)(?<=\S)~~", r"\1", seg)
        seg = re.sub(r"\[([^\]\n]+)\]\((https?://[^)\s]+)\)", r"\1（\2）", seg)
    return seg


def render_for_wechat(text: str, mode: str | None = None) -> str:
    """Adapt an executor's Markdown to what the WeChat client renders
    (official StreamingMarkdownFilter rules). Code is never touched."""
    mode = mode or MARKDOWN_MODE
    if mode == "off" or not text:
        return text
    out, fence = [], None
    for line in text.split("\n"):
        if fence is not None:
            if _fence_closes(line, fence):
                fence = None
                if mode == "plain":
                    continue
            out.append(line)
            continue
        m = _FENCE_RE.match(line)
        if m:
            fence = m.group(1)
            if mode != "plain":
                out.append(line)
            continue
        line = _H56_RE.sub(r"\1", line)
        line = _TASK_RE.sub(lambda mm: mm.group(1) + ("☑ " if mm.group(2) in "xX" else "☐ "), line)
        if mode == "plain":
            line = re.sub(r"^( {0,3})#{1,6}[ \t]+", r"\1", line)
        parts = _INLINE_CODE_SPLIT.split(line)
        out.append("".join(p if i % 2 else _render_inline(p, mode)
                           for i, p in enumerate(parts)))
    return "\n".join(out)


def _attachable(raw: str) -> Path | None:
    """Auto-attach is implicit (any [x](/path) in a reply), so it only takes
    ordinary files. 2026-11-04 review: v5.0.5's deny list let ~/.netrc,
    ~/.docker/config.json, ~/.kube/config, gh's hosts.yml, ~/.pgpass and the
    bridge's own chat log and task records through (reproduced): a
    prompt-injected reply could send them to WeChat. Also refused now: any
    hidden path component, the bridge's private directories, more
    secret-looking names and, if WECHAT_BRIDGE_AUTO_ATTACH_DIRS is set
    (":"-separated roots), everything outside those roots.
    `complete --attach` stays the explicit way to send any file."""
    try:
        p = Path(os.path.expanduser(raw)).resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    s = str(p)
    if not p.is_file() or s.startswith(("/proc/", "/sys/", "/dev/", "/etc/", "/boot/")):
        return None
    if any(part.startswith(".") for part in p.parts[1:]):
        return None             # ~/.ssh, ~/.netrc, ~/.config/..., .env, .git/...
    with contextlib.suppress(OSError, ValueError):
        base = BASE_DIR.resolve()
        if any(p.is_relative_to(base / d) for d in _ATTACH_PRIVATE_DIRS):
            return None         # credentials, chat log, task records ...
    roots = [r.strip() for r in
             os.environ.get("WECHAT_BRIDGE_AUTO_ATTACH_DIRS", "").split(":") if r.strip()]
    if roots:
        inside = False
        for r in roots:
            with contextlib.suppress(OSError, ValueError, RuntimeError):
                if p.is_relative_to(Path(os.path.expanduser(r)).resolve()):
                    inside = True
                    break
        if not inside:
            return None
    if any(x in p.name.lower() for x in _ATTACH_DENY):
        return None
    try:
        size = p.stat().st_size
    except OSError:
        return None
    return p if 0 < size <= max(FILE_SEND_MAX_BYTES, VIDEO_SEND_MAX_BYTES) else None


def extract_local_refs(text: str, refused: list | None = None) -> tuple:
    """(text, [paths]): ![说明](/abs/file) and [名字](/abs/file) pointing at
    a real local file become attachments - an LLM naturally "shows" a chart
    it made that way, and WeChat cannot open a local path. The text keeps the
    label. Code is left alone; secrets and bridge state never qualify."""
    found: list = []
    out, fence = [], None

    def sub(mm):
        if len(found) >= AUTO_ATTACH_MAX:
            return mm.group(0)
        p = _attachable(mm.group(3))
        if p is None:
            if refused is not None:
                with contextlib.suppress(OSError, ValueError):
                    if Path(os.path.expanduser(mm.group(3))).is_file():
                        refused.append(Path(mm.group(3)).name)
            return mm.group(0)
        if p not in found:
            found.append(p)
        label = mm.group(2).strip()
        return label if mm.group(1) else (label or p.name)

    for line in text.split("\n"):
        if fence is not None:
            out.append(line)
            if _fence_closes(line, fence):
                fence = None
            continue
        m = _FENCE_RE.match(line)
        if m:
            fence = m.group(1)
            out.append(line)
            continue
        parts = _INLINE_CODE_SPLIT.split(line)
        out.append("".join(p if i % 2 else _LOCAL_REF_RE.sub(sub, p)
                           for i, p in enumerate(parts)))
    return "\n".join(out), found


def _u16(s: str) -> int:
    """Length as the official textChunkLimit counts it (JS string length:
    UTF-16 code units, an emoji is 2). <= 3 UTF-8 bytes per unit, so a
    4000-unit part is <= 12,000 bytes: never more than upstream sends."""
    return len(s) + sum(1 for ch in s if ord(ch) > 0xFFFF)


def _md_context(text: str, upto: int) -> dict:
    """Open code fence (opening line, marker) and, inside a table body, the
    table's header lines - at offset `upto` (complete lines only)."""
    fence, table, pos = None, [], 0
    while pos < upto:
        nl = text.find("\n", pos, upto)
        if nl < 0:
            break
        line = text[pos:nl]
        if fence is not None:
            if _fence_closes(line, fence[1]):
                fence = None
        else:
            m = _FENCE_RE.match(line)
            if m:
                fence, table = (line.rstrip(), m.group(1)), []
            elif line.lstrip().startswith("|"):
                table.append(line)
            else:
                table = []
        pos = nl + 1
    hdr = ""
    if fence is None and len(table) >= 3 and re.fullmatch(
            r"\s*\|?[\s:|-]*-{3,}[\s:|-]*\|?\s*", table[1]):
        hdr = table[0] + "\n" + table[1] + "\n"
    return {"fence": fence, "table_hdr": hdr}


def _hard_cut(text: str, pos: int, cap: int) -> int:
    used, i, n = 0, pos, len(text)
    while i < n and text[i] != "\n":
        c = 2 if ord(text[i]) > 0xFFFF else 1
        if used + c > cap:
            break
        used += c
        i += 1
    if i < n and text[i] != "\n":
        lo = pos + max(1, int((i - pos) * 0.8))
        best = max(text.rfind(ch, lo, i) for ch in " ，。；！？、,.;!?")
        if best >= lo:
            i = best + 1
    return max(i, pos + 1)


def _plan_parts(text: str, start: int = 0, limit: int = SEND_CHUNK_LIMIT) -> list:
    """Parts of `text` from offset `start`: (s, e, prefix, suffix). text[s:e]
    is what a part carries (done_upto stays an offset into `text`); prefix/
    suffix repair Markdown cut by the split: an open code fence is closed at
    the end of a part and reopened, with its info string, in the next; a
    table continued in the next part repeats its header row. Breaks at a
    blank line when that keeps the part at least half full, else at a line
    end, else inside the line (after a space/punctuation if possible)."""
    n, out, pos = len(text), [], start
    while True:
        while pos < n and text[pos] == "\n":
            pos += 1
        if pos >= n or not text[pos:].strip():
            return out
        ctx = _md_context(text, pos)
        prefix = ""
        if ctx["fence"]:
            prefix = ctx["fence"][0] + "\n"
        elif ctx["table_hdr"] and text[pos:].lstrip(" ").startswith("|"):
            prefix = ctx["table_hdr"]
        window = text[pos:pos + limit + 1]
        reserve = 8 if (prefix or "```" in window or "~~~" in window) else 0
        cap = max(200, limit - _u16(prefix) - reserve)
        used, cur, para_cut, line_cut = 0, pos, None, None
        fence = ctx["fence"][1] if ctx["fence"] else None
        while cur < n:
            nl = text.find("\n", cur)
            nxt = n if nl < 0 else nl + 1
            cost = _u16(text[cur:nxt])
            if used + cost > cap:
                break
            used += cost
            line = text[cur:nxt].rstrip("\n")
            if fence is not None:
                if _fence_closes(line, fence):
                    fence = None
            else:
                m = _FENCE_RE.match(line)
                if m:
                    fence = m.group(1)
                elif not line.strip():
                    para_cut = nxt
            line_cut, cur = nxt, nxt
        if cur >= n:
            end = n
        elif line_cut is None:
            end = _hard_cut(text, pos, cap)
        elif para_cut is not None and _u16(text[pos:para_cut]) * 2 >= cap:
            end = para_cut
        else:
            end = line_cut
        s, e = pos, end
        while e > s and text[e - 1] == "\n":
            e -= 1
        suffix = ""
        if end < n:
            f_end = _md_context(text, end)["fence"]
            if f_end is not None:
                suffix = "\n" + f_end[1][0] * len(f_end[1])
        if text[s:e].strip():
            out.append((s, e, prefix, suffix))
        pos = end


def _send_priority(task: dict) -> str:
    if task.get("kind") != "notice":
        return "reply"
    why = str((task.get("execution") or {}).get("notice") or "")
    return "progress" if why in ("progress", "auto_progress") else "notice"


def _open_attachment_sends(qids: list) -> int:
    n = 0
    for qid in qids or []:
        it = load_qitem(qid)
        if it and it.get("status") in ("hold", "pending", "sending"):
            n += 1 + (1 if it.get("caption") and not it.get("caption_sent") else 0)
    return n


def _tail_note(chars_left: int, files_left: int) -> str:
    what = []
    if chars_left > 0:
        what.append(f"约 {chars_left} 字")
    if files_left > 0:
        what.append(f"{files_left} 个文件")
    return (f"\n\n（微信限制你每条消息之后我最多回 {CTX_QUOTA} 条，后面还有"
            + "和".join(what) + "没发。回复任意内容（比如「继续」）我接着发。）")


def _send_part_raw(creds: dict, to_user: str, body: str, ctx, client_id: str) -> dict:
    try:
        return send_text(creds, to_user, body, ctx, client_id=client_id)
    except ApiError as e:
        if e.kind not in ("timeout", "network") or not _dedupe_confirmed():
            raise
        log(f"send: {e.kind} after the request left; client_id de-dup is "
            "confirmed on this account - resending the part with its client_id")
        time.sleep(2)
        return send_text(creds, to_user, body, ctx, client_id=client_id)


def attempt_send(task_id: str, allow_retry: bool = False,
                 force: bool = False) -> dict:
    """Send a task's reply part by part, recording progress (`done_upto` =
    end offset of the last confirmed part, `pending` = the part in flight and
    its client_id), so a retry or a human-approved resend continues where
    delivery stopped instead of re-sending earlier parts.
      connect / business / http failure -> that part was NOT delivered:
          send_failed with automatic retries; out of retries ->
          send_uncertain(exhausted) + a bridge notice to the user.
      -2 (v5: classified) -> 24 h window closed / reply budget used up /
          rate limit; none uses up retries. Window and budget PARK the reply
          until the user's next message; a proactive message whose window is
          closed fails at once (context_expired) so the caller can use the App.
      -14 (session expired) -> waits without using up retries.
      network / timeout -> the part may have been delivered: send_uncertain,
          never retried automatically (unless `canary-dedupe` confirmed that
          WeChat de-duplicates by client_id on this account).
    v5: parts are planned fence/table-aware in UTF-16 units; the reply budget
    is checked per part; a reply that does not fit is cut at the budget with
    a one-line note on its last part and the rest is parked."""
    _SEND_TIMING.acc, _SEND_TIMING.t0 = [], now_ms()
    _SEND_TIMING.task = task_id     # v1.7: the echo ledger links client id -> task
    creds = load_creds()
    if not creds:
        return {"ok": False, "error": "no credentials"}
    with task_lock():
        task = load_task(task_id)
        if not task:
            return {"ok": False, "error": "task not found"}
        status = task["status"]
        prev = dict(task.get("send") or {})
        if status == "sent":
            return {"ok": True, "already_sent": True}
        if status == "cancelled":
            return {"ok": False, "cancelled": True,
                    "error": "task cancelled by the user"}
        retrying = (allow_retry and status == "send_failed"
                    and not prev.get("no_auto_retry")
                    and int(prev.get("attempts", 1))
                    < int(prev.get("max_attempts") or SEND_MAX_ATTEMPTS))
        forced = force and status in ("send_failed", "send_uncertain")
        if status != "waiting_send" and not retrying and not forced:
            return {"ok": False, "error": f"task status is {status}"}
        fresh = status == "waiting_send"
        prev_attempts = 0 if (fresh or forced) else int(prev.get("attempts", 0))
        deferrals = 0 if (fresh or forced) else int(prev.get("deferrals") or 0)
        done_upto = 0 if fresh else int(prev.get("done_upto") or 0)
        pending = {} if fresh else dict(prev.get("pending") or {})
        manual = int(prev.get("manual_resends") or 0) + (1 if forced else 0)
        started_at = now_ms()
        task["status"] = "sending"
        task["send"] = {"started_at": started_at, "attempts": prev_attempts,
                        "done_upto": done_upto, "sender_pid": os.getpid(),
                        "progress_at": started_at}
        if deferrals:
            task["send"]["deferrals"] = deferrals
        if manual:
            task["send"]["manual_resends"] = manual
        if pending:
            task["send"]["pending"] = pending
        save_task(task)
        text = task.get("result_text") or ""
        to_user = task["from_user_id"]
        task_ctx = task.get("context_token")
        kind = task.get("kind")
        proactive = bool((task.get("execution") or {}).get("proactive"))
        priority = _send_priority(task)
        attach_ids = list(task.get("attachments") or [])
    ctx, slots = choose_context_token(to_user, task_ctx)
    win_open = window_state(to_user)[1]
    limit = _chunk_limit()
    start = _resume_offset(text, done_upto)
    plan = _plan_parts(text, start, limit) if text.strip() else []
    tail = None
    if slots is not None and plan:
        extra = _open_attachment_sends(attach_ids)
        usable = slots - QUOTA_RESERVE.get(priority, 0)
        if 0 < usable < len(plan) + extra:
            plan2 = _plan_parts(text, start, max(400, limit - SEND_TAIL_ROOM))
            n_now = min(len(plan2), usable)
            chars_left = len("".join(text[a:b] for a, b, _p, _q
                                     in plan2[n_now:]).replace("\n", ""))
            files_left = max(0, extra - max(0, usable - len(plan2)))
            if chars_left or files_left:
                plan = plan2[:n_now]
                tail = _tail_note(chars_left, files_left)
    parts_sent = 0
    last_resp: dict = {}
    failure: ApiError | None = None
    stop_why = None
    downgraded = False
    while plan:
        s, e, prefix, suffix = plan[0]
        if _stop.is_set():
            # 2026-11-04 review: serve is stopping - do not start another
            # part; the reply resumes after the restart from done_upto.
            stop_why = "shutdown"
            break
        if win_open is False:
            stop_why = "window"
            break
        if not quota_take(ctx, priority):
            stop_why = "quota"
            break
        same = (pending.get("start"), pending.get("end")) == (s, e)
        pending = {"start": s, "end": e,
                   "client_id": (pending.get("client_id") if same else None)
                   or new_client_id()}
        _record_send_progress(task_id, started_at, pending=pending)
        body = prefix + text[s:e] + suffix
        sent_cid = pending["client_id"]
        if tail and len(plan) == 1:
            body += tail
        try:
            last_resp = _send_part(creds, to_user, body, ctx, pending["client_id"])
        except ApiError as exc:
            if exc.kind in ("connect", "http") or (
                    exc.kind == "business" and exc.code != RATE_LIMIT_ERRCODE):
                quota_refund(ctx)              # provably not delivered
            if (exc.kind == "business" and exc.code is not None
                    and exc.code not in (RATE_LIMIT_ERRCODE, STALE_ERRCODE)
                    and _u16(body) > SEND_CHUNK_SAFE and not downgraded):
                downgraded = True
                _downgrade_chunk_limit()
                log(f"send task={task_id}: {_u16(body)}-unit part rejected "
                    f"({_scrub(exc)}); re-splitting at {SEND_CHUNK_SAFE}")
                plan = _plan_parts(text, s, SEND_CHUNK_SAFE)
                tail = None
                pending = {}
                continue
            failure = exc
            break
        plan.pop(0)
        parts_sent += 1
        done_upto = e
        pending = {}
        _record_send_progress(task_id, started_at, done_upto=done_upto,
                              pending=None)
        smid = last_resp.get("message_id")
        # v1.7: registered even when sendmessage returns no message_id (only
        # 2.4.9+ servers do; older ones answer {ret, errmsg}): under the client
        # id, so an echo can add the server ids later and a quote can still be
        # matched by its text (_quote_text_target). v1.6 registered nothing
        # then, which is why quoting the "已转到后台" signal could not route.
        quote_put(str(smid) if smid not in (None, "", 0) else f"cid:{sent_cid}",
                  user=to_user, direction="out", kind="text", text=text[s:e], task=task_id)
    attempts = prev_attempts + 1
    parts = parts_sent + len(plan)
    rest = (failure is None and bool(text.strip())
            and bool(_plan_parts(text, _resume_offset(text, done_upto), limit)))
    why = None
    if stop_why == "shutdown":
        pass                    # not a deferral and not a failed attempt
    elif failure is not None and failure.kind == "business" \
            and failure.code == RATE_LIMIT_ERRCODE:
        why = classify_rejection(failure, to_user, ctx)
        if why == "quota":
            quota_exhaust(ctx)
        else:
            quota_refund(ctx)
    elif failure is not None and failure.kind == "business" \
            and failure.code == STALE_ERRCODE:
        why = "session"
    elif failure is None and (stop_why or rest):
        why = stop_why or "quota"
    if stop_why == "shutdown":
        outcome = {"ok": False, "not_delivered": True, "attempts": prev_attempts,
                   "parts_sent": parts_sent, "parts": parts, "next_retry_at": 0,
                   "max_attempts": max(SEND_MAX_ATTEMPTS, prev_attempts + 1,
                                       int(prev.get("max_attempts") or 0)),
                   "error": "serve stopped mid-reply; the rest goes out after "
                            "the restart"}
        if deferrals:
            outcome["deferrals"] = deferrals
        final_status = "send_failed"
    elif failure is None and not why:
        outcome = {"ok": True, "client_id": last_resp.get("_client_id"),
                   "server_message_id": last_resp.get("message_id"),
                   "parts": parts_sent, "attempts": attempts}
        if not text.strip():
            outcome["no_text"] = True
        final_status = "sent"
    elif why and (failure is None or deferrals < SEND_DEFER_MAX):
        until = _defer_until(why, deferrals)
        detail = (f"{failure.kind}: {_scrub(failure)}" if failure is not None
                  else _DEFER_REASON.get(why, why))
        outcome = {"ok": False, "error": detail, "attempts": prev_attempts,
                   "parts_sent": parts_sent, "parts": parts,
                   "not_delivered": True, "deferred": why,
                   "deferrals": deferrals + 1, "next_retry_at": until,
                   "max_attempts": max(SEND_MAX_ATTEMPTS,
                                       int(prev.get("max_attempts") or 0))}
        if why in ("quota", "window"):
            outcome["parked"] = True
        if why == "window":
            outcome["context_expired"] = True
            if proactive:
                outcome["no_auto_retry"] = True
        if tail and parts_sent:
            outcome["tail_note"] = True
        final_status = "send_failed"
    elif failure.kind in ("timeout", "network"):
        # Result unknown: the server may already have accepted the part
        # (observed 2026-10-27: a reply arrived 3 times). Never auto-retry.
        outcome = {"ok": False, "uncertain": True, "attempts": attempts,
                   "parts_sent": parts_sent, "parts": parts,
                   "no_auto_retry": True,
                   "error": f"send {failure.kind}; result unknown, not retried"}
        if _echo_capable():
            # WeChat echoes sends on this account: give the echo a moment to
            # prove delivery before alarming the user (note_bot_echo).
            outcome.update(notified="echo-wait",
                           notify_after=now_ms() + ECHO_NOTICE_GRACE_MS)
        final_status = "send_uncertain"
    else:
        connect = failure.kind == "connect"
        max_attempts = (SEND_MAX_ATTEMPTS_CONNECT if connect
                        else SEND_MAX_ATTEMPTS)
        if forced or retrying:          # keep a budget raised by 「重发」
            max_attempts = max(max_attempts, int(prev.get("max_attempts") or 0))
        outcome = {"ok": False, "error": f"{failure.kind}: {_scrub(failure)}",
                   "attempts": attempts, "parts_sent": parts_sent,
                   "parts": parts, "max_attempts": max_attempts,
                   "not_delivered": True}
        if deferrals:
            outcome["deferrals"] = deferrals
        if attempts < max_attempts:
            delays = (SEND_CONNECT_RETRY_DELAYS_MS if connect
                      else SEND_RETRY_DELAYS_MS)
            outcome["next_retry_at"] = now_ms() + delays[
                min(attempts - 1, len(delays) - 1)]
            final_status = "send_failed"
        else:
            outcome.update(no_auto_retry=True, exhausted=True)
            outcome["error"] += (f"; all {attempts} attempts failed, "
                                 "not delivered")
            final_status = "send_uncertain"
    with task_lock():
        task = load_task(task_id)
        if task is None:
            log(f"send task={task_id}: task vanished mid-send "
                f"(outcome {final_status})")
            return outcome
        rec = {**outcome, "at": now_ms(), "done_upto": done_upto}
        acc = getattr(_SEND_TIMING, "acc", None) or []
        if acc:     # where the time goes: pick-up -> first request, network
            rec.update(t_start=getattr(_SEND_TIMING, "t0", None),
                       first_req_at=acc[0][0], net_ms=sum(b - a for a, b in acc))
        _SEND_TIMING.acc = None
        if manual:
            rec["manual_resends"] = manual
        if failure is not None and pending:
            rec["pending"] = pending
        # 2026-11-06 text outbox: the per-text cap on message-triggered
        # resends survives the attempt, and so does the "already told the
        # user" marker of a text that failed again - no repeated notice, no
        # extra hook wake (「重发」 clears both: a human asked).
        if prev.get("outbox_resends"):
            rec["outbox_resends"] = prev["outbox_resends"]
            if prev.get("notified") and final_status == "send_uncertain":
                rec["notified"] = prev["notified"]
        task["status"] = final_status
        task["send"] = rec
        save_task(task)
    text_outbox_reconcile(task_id)
    if final_status == "sent":
        update_heartbeat(last_send_ok_at=now_ms())
        if kind != "command" and text.strip():
            history_append("bot", text, mid=task_id, kind=kind)
    log(f"send task={task_id} -> {final_status} "
        f"attempts={outcome.get('attempts', attempts)} parts={parts_sent}/{parts}"
        + (f" deferred={why}" if why else "")
        + (f" error={failure.kind}:{_scrub(failure)}" if failure else ""))
    if final_status == "send_uncertain" and outcome.get("notified") != "echo-wait":
        _notify_send_problem(task_id)
    return outcome


def _env_int_list(name: str, default: tuple) -> tuple:
    raw = os.environ.get(name)
    if not raw:
        return tuple(default)
    vals = []
    for part in raw.split(","):
        try:
            v = int(part.strip())
        except ValueError:
            continue
        if v > 0:
            vals.append(v)
    return tuple(sorted(set(vals))) or tuple(default)


# v5 patch 2026-11-02: staged auto-progress. The old single
# WECHAT_BRIDGE_AUTO_PROGRESS_SEC (default 180) is kept as a one-stage
# alias, and WECHAT_BRIDGE_AUTO_PROGRESS_SEC=0 still disables it.
# Otherwise stages come from WECHAT_BRIDGE_AUTO_PROGRESS_SECS, default
# 180/600 s (2026-11-02 opt batch, item 1): at most 2 notes per task, and
# only when the executor's stage actually changed since the last note
# (time-only notes still go out at the stage boundaries). Most tasks
# finish before 180 s, so most tasks now get no auto-progress note at
# all, and a note never eats the slots reserved for the final reply.
# A task that only appears already old gets one note (the highest due
# stage), never a burst of all past stages at once.
if os.environ.get("WECHAT_BRIDGE_AUTO_PROGRESS_SEC") not in (None, ""):
    _ap_sec = _env_int("WECHAT_BRIDGE_AUTO_PROGRESS_SEC", 180)
    AUTO_PROGRESS_STAGES = (_ap_sec,) if _ap_sec > 0 else ()
else:
    AUTO_PROGRESS_STAGES = _env_int_list("WECHAT_BRIDGE_AUTO_PROGRESS_SECS",
                                          (180, 600))
AUTO_PROGRESS_SEC = AUTO_PROGRESS_STAGES[0] if AUTO_PROGRESS_STAGES else 0


def check_auto_progress(index: list) -> None:
    """Staged "still working" notes while a task executes and the executor
    reported nothing itself. Costs a reply slot per note, so only while
    more than the reserved slots are left; executor progress/notes still
    suppress it, as before. 2026-11-02: the note never promises an
    imminent finish ("好了马上发你" was dishonest on the 22:46 task, which
    took 16+ minutes after three such notes). If the executor reported a
    stage, the note names it; otherwise it says plainly that the task is
    still running and how long it has taken. progress_notes off (设置
    进度关) suppresses these notes too."""
    if not AUTO_PROGRESS_STAGES:
        return
    if not settings_get().get("progress_notes", True):
        return
    now = now_ms()
    # 2026-11-06: a batch is one claim and one round: at most the newest task
    # of each claim speaks for it (before, a claimed burst of 5 could get 5).
    lead: dict = {}
    for s_ in index:
        if s_.get("status") == "executing" and s_.get("claim_id"):
            cur = lead.get(s_["claim_id"])
            if cur is None or (s_.get("received_at") or 0) >= (cur.get("received_at") or 0):
                lead[s_["claim_id"]] = s_
    for s_ in index:
        if s_.get("status") != "executing" or s_.get("kind") in ("command", "notice"):
            continue
        if s_.get("claim_id") and lead.get(s_["claim_id"]) is not s_:
            continue
        claimed = int(s_.get("claimed_at") or 0)
        if not claimed:
            continue
        hb = int(s_.get("heartbeat_at") or 0)
        # Opt round 2 item 1: silence-based, not "ever heartbeated".
        # The old rule (any heartbeat suppresses auto_progress forever)
        # broke once heartbeat became mandatory: an executor that
        # heartbeated once then went silent got no warning at all.
        # A live executor heartbeats every 5 min, so a warning only
        # fires after 420 s of silence (one missed heartbeat + 2 min
        # grace), well before the 10-min reclaim. Never-heartbeated
        # tasks keep the old 180/600 s stages.
        silence_warning = False
        if hb:
            if now - hb < 420_000:
                continue
            silence_warning = True
            due = 0
        else:
            due = sum(1 for sec in AUTO_PROGRESS_STAGES
                      if now - claimed >= sec * 1000)
            if due == 0:
                continue
        with task_lock():
            t = load_task(s_["message_id"])
            if not t or t.get("status") != "executing":
                continue
            ex = dict(t.get("execution") or {})
            sent = ex.get("auto_progress_stage")
            if sent is None:
                sent = (sum(1 for sec in AUTO_PROGRESS_STAGES if sec <= 180)
                        if ex.get("auto_progress_at") else 0)
            try:
                sent = int(sent)
            except (TypeError, ValueError):
                sent = 0
            if sent >= 2:
                continue  # max 2 auto_progress notes per task
            if silence_warning:
                warned_at = int(ex.get("auto_progress_silence_warned_at") or 0)
                if warned_at and warned_at >= hb:
                    continue  # already warned for this silence period
                due = sent + 1
                ex["auto_progress_silence_warned_at"] = now
            elif due <= sent:
                continue
            stage_now = (ex.get("stage") or "").strip()
            last_stage = ex.get("auto_progress_stage_text")
            if not silence_warning and stage_now and last_stage is not None \
                    and stage_now == last_stage:
                continue  # stage did not change: no new information to report
            ex["auto_progress_stage"] = due
            ex["auto_progress_at"] = now
            ex["auto_progress_stage_text"] = stage_now
            t["execution"] = ex
            save_task(t)
        user = t.get("from_user_id")
        left = choose_context_token(user, t.get("context_token"))[1]
        if left is not None and left <= QUOTA_RESERVE["progress"] + 1:
            continue
        took = fmt_duration(now - int(t.get("received_at") or claimed))
        stage = (ex.get("stage") or "").strip()
        if stage:
            text = f"「{_asked(t)}」还在处理：{stage}（已经 {took}）。"
        else:
            text = f"「{_asked(t)}」还在处理，已经 {took}了，还没做完，做完就发你。"
        create_notice(user, text,
                      reason="auto_progress", about={"task_id": t["message_id"]})


def _pct(vals: list, p: float):
    if not vals:
        return None
    vals = sorted(vals)
    return vals[min(len(vals) - 1, max(0, int(round(p * (len(vals) - 1)))))]


def latency_stats(hours: float = 72, limit: int = 300) -> dict:
    """Per-stage latency of recently answered messages (ms): wake = arrival
    -> claimed by an executor, exec = claimed -> complete, deliver = complete
    -> last part sent, total = arrival -> sent; plus parts per reply.
    2026-11-02: wake is split where the data exists - wake_queue = arrival
    -> pending_execution (bridge-side: media download / queueing) and
    wake_wait = pending_execution -> claimed (hook poll + executor cold
    start). Old tasks without pending_at only contribute to wake, so the
    split columns may cover fewer messages than wake itself."""
    since = now_ms() - int(hours * 3_600_000)
    cols: dict = {"wake": [], "exec": [], "deliver": [], "total": [], "parts": [],
                  "wake_queue": [], "wake_wait": []}
    for s_ in scan_task_index()[-limit:]:
        if s_.get("status") != "sent" or s_.get("kind") in ("command", "notice") \
                or int(s_.get("received_at") or 0) < since:
            continue
        t = load_task(s_["message_id"]) or {}
        ex, sd = t.get("execution") or {}, t.get("send") or {}
        rec, cl = int(t.get("received_at") or 0), int(ex.get("claimed_at") or 0)
        fin, at = int(ex.get("finished_at") or 0), int(sd.get("at") or 0)
        if ex.get("proactive") or ex.get("merged_into") or not (rec and cl and fin and at):
            continue
        cols["wake"].append(cl - rec)
        cols["exec"].append(fin - cl)
        cols["deliver"].append(max(0, at - fin))
        cols["total"].append(at - rec)
        cols["parts"].append(int(sd.get("parts") or 0))
        pen = int(t.get("pending_at") or s_.get("pending_at") or 0)
        if rec <= pen <= cl:
            cols["wake_queue"].append(pen - rec)
            cols["wake_wait"].append(cl - pen)
    out: dict = {"n": len(cols["total"])}
    for k, v in cols.items():
        out[k] = {"p50": _pct(v, 0.5), "p95": _pct(v, 0.95)}
    return out


def latency_summary() -> str:
    st = latency_stats()
    if not st["n"]:
        return ""
    def f(k):
        ms = int(st[k]["p50"] or 0)
        return f"{ms / 1000:.1f}秒" if ms < 10_000 else fmt_duration(ms)
    base = (f"最近 {st['n']} 条的中位数：接手 {f('wake')}、处理 {f('exec')}、"
            f"送达 {f('deliver')}，合计 {f('total')}")
    if st.get("wake_queue", {}).get("p50") is not None:
        base += (f"（接手分段：进队 {f('wake_queue')}、"
                 f"等接手 {f('wake_wait')}）")
    return base


def flush_outbox(index: list | None = None) -> list[dict]:
    results = []
    now = now_ms()
    deferred = rate_limited_until()
    for t in (scan_task_index() if index is None else index):
        if _stop.is_set():
            break           # serve is stopping: the rest goes after the restart
        send = t.get("send") or {}
        due_retry = (
            t["status"] == "send_failed"
            and not send.get("no_auto_retry")
            and int(send.get("attempts", 1))
            < int(send.get("max_attempts") or SEND_MAX_ATTEMPTS)
            and int(send.get("next_retry_at", 0)) <= now
        )
        if t["status"] == "waiting_send" or due_retry:
            if deferred:
                continue      # iLink -2: wait it out (a user message lifts it)
            results.append({"task_id": t["message_id"],
                            **attempt_send(t["message_id"],
                                           allow_retry=due_retry)})
    return results


# --------------------------------------------------------------------------
# serve: long-poll receiver thread + sender thread (single instance)
# --------------------------------------------------------------------------

_stop = threading.Event()
_send_wake = threading.Event()   # SIGUSR1 (reply handoff) wakes the sender
_SERVE_CREDS: dict = {}          # credentials currently used by serve


class GracefulStop(BaseException):
    # BaseException: as an Exception the receiver loop's `except
    # Exception` swallowed it, logging a bogus "getUpdates unexpected
    # error ... GracefulStop" (and a poll failure in the heartbeat) on
    # every stop/restart, which inflated report.py / doctor error counts.
    pass


# 2026-11-04 review: while a batch is being stored and the cursor saved,
# SIGTERM does not raise into it (the loop stops right after the batch).
_SIGNAL_STATE = {"critical": 0}
SHUTDOWN_GRACE_SEC = _env_int("WECHAT_BRIDGE_SHUTDOWN_GRACE_SEC", 12)
# Long polls in a row that get no answer at all before they count as poll
# failures (0 = never; v5.0.5 behaviour).
POLL_SILENT_FAIL_AFTER = _env_int("WECHAT_BRIDGE_POLL_SILENT_FAIL_AFTER", 3)
# A storage error that is not "disk full"-like is retried this many polls,
# then the message that causes it is skipped (so one poison message cannot
# stop the channel). ENOSPC / EDQUOT / EROFS / EIO / ENOMEM: retried forever.
INGEST_RETRY_MAX = 30
_STORAGE_TRANSIENT = {getattr(errno, n) for n in (
    "ENOSPC", "EDQUOT", "EROFS", "EIO", "ENOMEM", "EAGAIN", "EMFILE", "ENFILE")
    if hasattr(errno, n)}
_THREAD_BEAT: dict = {}


def _beat(name: str | None = None) -> None:
    _THREAD_BEAT[name or threading.current_thread().name] = time.time()


def _thread_ages() -> dict:
    now = time.time()
    return {k: int(now - v) for k, v in sorted(_THREAD_BEAT.items())}


def _ingest_batch(creds: dict, msgs: list, prior_failures: int) -> bool:
    """Store one getupdates batch. False = a message could not be stored
    for a storage reason: keep the old cursor and poll the batch again
    (message_id dedupe skips the ones already stored)."""
    for m in msgs:
        try:
            ingest_message(creds, m)
        except OSError as e:
            if e.errno in _STORAGE_TRANSIENT or prior_failures + 1 < INGEST_RETRY_MAX:
                log(f"ingest: could not store a message ({type(e).__name__}: "
                    f"{_scrub(e)}); cursor kept, the batch is polled again")
                return False
            log(f"ingest: gave up on one message after {prior_failures + 1} "
                f"tries ({type(e).__name__}); skipped")
        except Exception as e:  # noqa: BLE001 - one bad message must not block the rest
            log(f"ingest error (message skipped): {type(e).__name__}: {_scrub(e)}")
    return True


def receiver_loop(creds: dict) -> None:
    cursor = (read_json(CURSOR_FILE, {}) or {}).get("get_updates_buf", "") or ""
    creds_sig = _file_sig(CREDS_FILE)
    _SERVE_CREDS["creds"] = creds
    if cursor:
        log(f"resuming from saved cursor ({len(cursor)} bytes)")
    next_timeout = LONG_POLL_DEFAULT_MS
    saved_cursor = cursor
    failures = 0
    business_error_streak = 0
    client_timeouts = 0
    ingest_failures = 0
    # Presence maintenance: the "temporarily unable to connect" title
    # in WeChat is driven by notifyStart + poll liveness. A one-shot
    # notifyStart that fails during a network flap used to leave the
    # bot showing offline until the next process restart, so keep
    # retrying (<=1 attempt / 20s), refresh every 10 minutes, and
    # refresh immediately after any poll-failure streak recovers.
    presence_ok = False
    last_presence_attempt = 0.0
    last_presence_ok = 0.0
    while not _stop.is_set():
        _watchdog_ping()
        _beat("receiver")
        # A re-login (wait-login) rewrites credentials.json: switch to the
        # new token with a fresh cursor instead of polling with the dead one
        # until someone restarts the service.
        sig = _file_sig(CREDS_FILE)
        if sig != creds_sig:
            creds_sig = sig
            fresh = load_creds()
            if fresh and fresh.get("bot_token") != creds.get("bot_token"):
                creds = fresh
                _SERVE_CREDS["creds"] = fresh
                cursor = saved_cursor = ""
                with contextlib.suppress(OSError):
                    atomic_write_json(CURSOR_FILE, {"get_updates_buf": ""})
                presence_ok = False
                last_presence_attempt = 0.0
                failures = business_error_streak = 0
                log("credentials changed on disk: polling with the new "
                    "login, no restart needed")
        now_s = time.time()
        if (not presence_ok or now_s - last_presence_ok > 600) \
                and now_s - last_presence_attempt > 20:
            last_presence_attempt = now_s
            if notify(creds, start=True):
                presence_ok = True
                last_presence_ok = now_s
        session = read_json(SESSION_FILE, {}) or {}
        paused_until = session.get("paused_until_ms") or 0
        if paused_until > now_ms():
            wait = min(30, (paused_until - now_ms()) / 1000)
            log(f"session paused (stale token), waiting {int(wait)}s")
            _stop.wait(wait)
            continue
        try:
            resp = get_updates(creds, cursor, next_timeout)
        except ApiError as e:
            failures += 1
            # detail = failure layer (dns/refused/timeout/tls/closed/reset/
            # tunnel...), URL-free: ops/report.py tallies it per layer.
            log(f"getUpdates error ({failures}) kind={e.kind} "
                f"detail={_scrub(e)}")
            # Publish the failure streak so the hook can tell an
            # actively-failing serve (dead proxy) apart from a merely
            # slow first poll, and fast-fail it (see hook fast-path).
            _note_poll_fail()
            update_heartbeat(poll_fail_streak=failures,
                             last_poll_fail_at=now_ms())
            _stop.wait(POLL_RETRY_DELAYS_SEC[
                min(failures - 1, len(POLL_RETRY_DELAYS_SEC) - 1)])
            continue
        except Exception as e:  # noqa: BLE001 - the poll loop must never die
            failures += 1
            log(f"getUpdates unexpected error ({failures}) "
                f"type={type(e).__name__}")
            _note_poll_fail()
            update_heartbeat(poll_fail_streak=failures,
                             last_poll_fail_at=now_ms())
            _stop.wait(POLL_RETRY_DELAYS_SEC[
                min(failures - 1, len(POLL_RETRY_DELAYS_SEC) - 1)])
            continue
        ret = resp.get("ret")
        errcode = resp.get("errcode")
        if (ret not in (None, 0)) or (errcode not in (None, 0)):
            code = errcode if errcode not in (None, 0) else ret
            if code == STALE_ERRCODE:
                prev = read_json(SESSION_FILE, {}) or {}
                stale_count = int(prev.get("stale_count") or 0) + 1
                atomic_write_json(SESSION_FILE,
                                  {"paused_until_ms": now_ms() + SESSION_PAUSE_MS,
                                   "reason": "stale_token_-14",
                                   "stale_count": stale_count,
                                   "cursor_reset_at": prev.get("cursor_reset_at")})
                failures = 0
                log("getUpdates stale token (-14): pausing session for 1 hour")
                if stale_count >= 2:
                    # One pause did not heal it: the token is probably dead
                    # and a human re-login is needed. Alert via state file;
                    # the Muse hook turns this into a main-chat notice.
                    atomic_write_json(STATE_DIR / "auth_alert.json",
                                      {"at": now_ms(),
                                       "reason": "repeated_stale_token",
                                       "stale_count": stale_count})
                    log("auth alert raised: repeated stale token")
                continue
            if code == RATE_LIMIT_ERRCODE:
                # Rate limited: hammering makes it worse, and it says nothing
                # about the cursor - keep it out of the wedged-cursor reset.
                failures += 1
                _note_poll_fail()
                log(f"getUpdates rate limited (-2) ({failures}); retrying "
                    f"in {RATE_LIMIT_POLL_RETRY_SEC}s")
                _stop.wait(RATE_LIMIT_POLL_RETRY_SEC)
                continue
            business_error_streak += 1
            if business_error_streak >= 15:
                # A cursor the server no longer accepts can wedge polling
                # forever. Reset it once per hour at most; inbound dedupe
                # by message_id makes a reset safe against duplicates.
                prev = read_json(SESSION_FILE, {}) or {}
                last_reset = int(prev.get("cursor_reset_at") or 0)
                if now_ms() - last_reset > SESSION_PAUSE_MS:
                    cursor = saved_cursor = ""
                    atomic_write_json(CURSOR_FILE, {"get_updates_buf": ""})
                    prev["cursor_reset_at"] = now_ms()
                    atomic_write_json(SESSION_FILE, prev)
                    business_error_streak = 0
                    log("getUpdates: reset wedged cursor after repeated "
                        "business errors")
            failures += 1
            _note_poll_fail()
            log(f"getUpdates business error ret={ret} errcode={errcode} "
                f"errmsg={_scrub(resp.get('errmsg') or '')[:60]} ({failures})")
            _stop.wait(POLL_RETRY_DELAYS_SEC[
                min(failures - 1, len(POLL_RETRY_DELAYS_SEC) - 1)])
            continue
        if resp.get("_client_timeout"):
            # 2026-11-04 review: the request got NO answer at all (client
            # timeout - also how a keepalive-detected dead path surfaces).
            # v5.0.5 recorded it as a successful empty poll, so a server or
            # path that accepts connections but never answers looked healthy
            # forever. A few in a row are tolerated; from the
            # POLL_SILENT_FAIL_AFTER-th on they are poll failures: the
            # heartbeat stops advancing (hook and watchdog notice), idle
            # sockets are dropped and proxy failover is considered.
            client_timeouts += 1
            beat = {"client_timeout_streak": client_timeouts,
                    "last_client_timeout_at": now_ms()}
            if POLL_SILENT_FAIL_AFTER and client_timeouts >= POLL_SILENT_FAIL_AFTER:
                failures += 1
                _note_poll_fail()
                PROXY_POOL.note_transport_failure()
                PROXY_POOL.close_idle()
                log(f"getUpdates: {client_timeouts} long polls in a row got no "
                    f"answer at all; counted as a poll failure ({failures})")
                beat.update(poll_fail_streak=failures, last_poll_fail_at=now_ms())
            else:
                beat["last_poll_ok_at"] = now_ms()
            update_heartbeat(**beat)
            continue
        if client_timeouts:
            client_timeouts = 0
            update_heartbeat(client_timeout_streak=0)
        if failures > 0:
            # Polling just recovered: refresh presence right away so
            # the "unable to connect" title clears promptly.
            last_presence_ok = 0.0
        failures = 0
        business_error_streak = 0
        prev_session = read_json(SESSION_FILE, {}) or {}
        if prev_session.get("stale_count"):
            prev_session["stale_count"] = 0
            atomic_write_json(SESSION_FILE, prev_session)
            (STATE_DIR / "auth_alert.json").unlink(missing_ok=True)
        lp = resp.get("longpolling_timeout_ms")
        if lp:
            try:
                next_timeout = max(LONG_POLL_MIN_MS, min(int(lp), LONG_POLL_MAX_MS))
            except (TypeError, ValueError):
                pass
        new_cursor = resp.get("get_updates_buf")
        if new_cursor:
            cursor = new_cursor      # in memory now; persisted below
        update_heartbeat(last_poll_ok_at=now_ms(), poll_fail_streak=0)
        # At-least-once delivery: the cursor used to be saved BEFORE the
        # messages were stored, so a restart in between lost them for good
        # (the server never resends past the cursor). Now it is saved only
        # after the whole batch is on disk; message_id dedupe makes a
        # redelivery after a crash harmless.
        # 2026-11-04 review: ... and only if the batch really IS on disk.
        # v5.0.5 swallowed every ingest exception and still advanced the
        # cursor, so a message that could not be stored (disk full) was gone
        # for good (reproduced). A storage error now keeps the old cursor and
        # polls the same batch again.
        _SIGNAL_STATE["critical"] += 1
        try:
            stored = _ingest_batch(creds, resp.get("msgs") or [], ingest_failures)
            if stored and cursor != saved_cursor:
                try:
                    atomic_write_json(CURSOR_FILE, {"get_updates_buf": cursor})
                    saved_cursor = cursor
                except OSError as e:
                    log(f"cursor save failed: {type(e).__name__}")
        finally:
            _SIGNAL_STATE["critical"] -= 1
        if not stored:
            cursor = saved_cursor
            ingest_failures += 1
            update_heartbeat(ingest_fail_streak=ingest_failures,
                             last_ingest_fail_at=now_ms())
            _stop.wait(POLL_RETRY_DELAYS_SEC[
                min(ingest_failures - 1, len(POLL_RETRY_DELAYS_SEC) - 1)])
            continue
        if ingest_failures:
            ingest_failures = 0
            update_heartbeat(ingest_fail_streak=0)
    # No notifyStop here on purpose: serve exits are almost always
    # restarts (upgrades, crash recovery, VM replacement), and telling
    # the server "stopped" on every restart made WeChat show
    # "temporarily unable to connect" long after polling had resumed.
    # A deliberate full stop is signalled by `bridge.py stop` instead.


PRUNE_AFTER_MS = 30 * 24 * 3600 * 1000
_last_prune_epoch = 0.0


def prune_orphan_media() -> None:
    """Drop inbox_media files older than 30 days that no task references.

    Slim (2026-11-02): prune_old_tasks already deletes media linked to a
    pruned task, but a file with no task pointing at it (interrupted
    download, renamed task, manual copy) would otherwise live forever.
    Only MEDIA_DIR is swept here; outbox/files copies belong to queue
    items and are handled by prune_queue, never by this function.
    """
    referenced: set[str] = set()
    for t in list_tasks():
        for key in _MEDIA_PATH_KEYS:
            mp = t.get(key) or ""
            if mp:
                referenced.add(mp)
        for fr in t.get("frames") or []:
            referenced.add(str(Path(fr).parent))
    cutoff = time.time() - PRUNE_AFTER_MS / 1000
    try:
        entries = list(MEDIA_DIR.iterdir())
    except OSError:
        return
    for f in entries:
        if f.is_dir() and f.name.endswith(".frames") and str(f) not in referenced:
            with contextlib.suppress(OSError):
                if f.stat().st_mtime < cutoff:
                    shutil.rmtree(f, ignore_errors=True)
            continue
        if not f.is_file() or f.name.startswith("."):
            continue
        if str(f) in referenced:
            continue
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
                log(f"pruned orphan media {f.name}")
        except OSError:
            pass


def prune_old_tasks() -> None:
    """Drop terminal-state tasks (and their downloaded media) older
    than 30 days so the inbox cannot grow without bound."""
    cutoff = now_ms() - PRUNE_AFTER_MS
    for t in list_tasks():
        # send_uncertain is deliberately NOT pruned: it awaits a human
        # resend decision, however long that takes.
        if t.get("status") not in ("sent", "send_failed", "cancelled"):
            continue
        if (t.get("received_at") or 0) >= cutoff:
            continue
        for key in _MEDIA_PATH_KEYS:
            mp = t.get(key) or ""
            if mp.startswith((str(MEDIA_DIR), str(FILES_DIR))):
                try:
                    os.unlink(mp)
                except OSError:
                    pass
        for fd in {str(Path(fr).parent) for fr in (t.get("frames") or [])}:
            if fd.startswith(str(MEDIA_DIR)):
                shutil.rmtree(fd, ignore_errors=True)
        gone = False
        for p_ in (task_path(t["message_id"]), _legacy_task_path(t["message_id"])):
            if p_ is not None:
                try:
                    p_.unlink()
                    gone = True
                except OSError:
                    pass
        if gone:
            log(f"pruned old task id={t['message_id']}")


SENDING_STALE_MS = 120_000
SENDING_LIVE_MAX_MS = 15 * 60 * 1000


def _pid_alive_bridge(pid) -> bool:
    """True if `pid` is another live bridge process (CLI send, send-direct).
    Our own pid counts as not alive: callers run in the thread that would
    be doing the work, so nothing of ours can be in flight."""
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0 or pid == os.getpid():
        return False
    try:
        os.kill(pid, 0)
        cmd = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return False
    return b"bridge" in cmd or b"send-direct" in cmd


def recover_stale_sending(index: list | None = None) -> None:
    """A sender that dies mid-send leaves a task in "sending" - the API call
    may or may not have been accepted. After 120 s without progress it
    becomes send_uncertain (never auto-resent) and the user is told. v4: a
    send that is still making progress, or whose CLI process is alive (a
    long reply on a flaky link can take minutes), is left alone - v3 flipped
    it and raised a false alarm while it was still going."""
    now = now_ms()
    for t in (scan_task_index() if index is None else index):
        if t.get("status") != "sending":
            continue
        send = t.get("send") or {}
        started = int(send.get("started_at") or 0)
        last = max(started, int(send.get("progress_at") or 0))
        if not started or now - last < SENDING_STALE_MS:
            continue
        if _pid_alive_bridge(send.get("sender_pid")) \
                and now - last < SENDING_LIVE_MAX_MS:
            continue
        flipped = False
        with task_lock():
            fresh = load_task(t["message_id"])
            if fresh and fresh.get("status") == "sending" and int(
                    (fresh.get("send") or {}).get("started_at") or 0) == started:
                fresh["status"] = "send_uncertain"
                fresh["send"] = {
                    **(fresh.get("send") or {}),
                    "no_auto_retry": True,
                    "error": "sender interrupted mid-send; result "
                             "unknown, not retried",
                }
                save_task(fresh)
                flipped = True
        if flipped:
            log(f"recovered stale sending task={t['message_id']} "
                "-> send_uncertain")
            text_outbox_reconcile(t["message_id"])   # maybe delivered: not resent
            _notify_send_problem(t["message_id"])


def sender_loop() -> None:
    global _last_prune_epoch
    last_stats = 0.0
    last_overdue = 0.0
    last_auto = 0.0
    last_text_sync = 0.0
    while not _stop.is_set():
        _send_wake.clear()
        _beat("sender")
        try:
            # 2026-11-06: second line of defence for the batch thread - an
            # overdue batch is dispatched within a second either way.
            batch_flush_due()
            if time.monotonic() - last_text_sync > 60:
                last_text_sync = time.monotonic()
                text_outbox_sync()
                task_index_persist()        # v1.7: CLI calls start from it
            idx = scan_task_index()
            recover_stale_sending(idx)
            flush_outbox(idx)
            check_echo_wait(idx)
            if time.monotonic() - last_overdue > 60:
                last_overdue = time.monotonic()
                check_overdue(idx)
            if time.monotonic() - last_auto > 15:
                last_auto = time.monotonic()
                check_auto_progress(idx)
                check_background(idx)
        except Exception as e:  # noqa: BLE001
            log(f"sender loop error: {type(e).__name__}: {_scrub(e)}")
        if time.time() - _last_prune_epoch > 3600:
            _last_prune_epoch = time.time()
            try:
                prune_old_tasks()
                prune_orphan_media()
                prune_queue()
                quote_prune()
                _prune_partials()
                prune_history_archives()
                check_corrupt_tasks()
            except Exception as e:  # noqa: BLE001
                log(f"prune error: {type(e).__name__}: {_scrub(e)}")
        if time.monotonic() - last_stats > 60:
            last_stats = time.monotonic()
            with contextlib.suppress(Exception):
                PROXY_POOL.publish_stats()
        _send_wake.wait(1)       # SIGUSR1 from a reply handoff: no 1 s lag


# --------------------------------------------------------------------------
# Outbound media queue (v4)
#
# Observed 2026-11-01: an 18 s MP3 the executor generated failed with
# "CDN upload RemoteDisconnected" three times in a row and the user had to
# say "send it again" later. This egress drops connections in bursts
# (OPS.md: cut after ~1-1.5 s, in clusters); v3 retried the CDN POST 3 times
# (docs/OPERATIONS.md: cut after ~1-1.5 s, in clusters); v3 retried the CDN POST 3 times
# within ~5 s - all inside the same burst. Now every outbound file is a
# queue item under outbox/queue/ with a private copy under outbox/files/
# (the executor's /tmp does not survive a VM replacement, ~/workspace does):
#   - a round = getuploadurl (fresh upload params) + encrypt + CDN POST (3
#     quick tries) + sendmessage; failed rounds are retried 15 s, 45 s, 2 min,
#     5 min, 10 min later (6 rounds, ~18 min) by serve's queue worker;
#   - nothing is delivered before sendmessage, so upload failures are always
#     safe to retry; a dropped connection on the media sendmessage itself
#     is "uncertain" and never repeated automatically;
#   - -2 / -14 wait without using up rounds;
#   - attachments of a reply (`complete --attach`) go out after its text;
#   - a voice request whose bubble cannot be made (no SILK codec, too long)
#     or that the server refuses is sent as an audio FILE instead;
#   - final failure: the bridge tells the user why, and 「重发」 re-queues.
# --------------------------------------------------------------------------

def _env_delays_ms(name: str, default: tuple) -> tuple:
    raw = os.environ.get(name)
    if not raw:
        return default
    try:
        vals = tuple(int(x) for x in raw.split(",") if x.strip())
    except ValueError:
        return default
    return vals or default


QUEUE_RETRY_DELAYS_MS = _env_delays_ms(
    "WECHAT_BRIDGE_QUEUE_DELAYS_MS", (8_000, 30_000, 90_000, 300_000, 600_000))
QUEUE_MAX_ROUNDS = 6
QUEUE_GATE_MAX_MS = 120_000
QUEUE_KEEP_MS = 7 * 24 * 3600 * 1000
QUEUE_DEFER_MAX = 40
QUEUE_STALE_MS = 10 * 60 * 1000
QUEUE_HOLD_MAX_MS = 3600 * 1000
VOICE_SOURCE_MAX_BYTES = 30 * 1024 * 1024
VOICE_TUNING_FILE = STATE_DIR / "voice_tuning.json"
VOICE_REFUSED_TTL_MS = 24 * 3600 * 1000
_QUEUE_TERMINAL = ("sent", "failed", "uncertain", "cancelled")
_queue_wake = threading.Event()


class AttachError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _qpath(qid: str) -> Path:
    return QUEUE_DIR / f"{safe_name(qid)}.json"


def load_qitem(qid: str) -> dict | None:
    it = read_json(_qpath(qid))
    return it if isinstance(it, dict) and it.get("qid") else None


def save_qitem(item: dict) -> None:
    item["updated_at"] = now_ms()
    atomic_write_json(_qpath(item["qid"]), item)


def list_queue() -> list[dict]:
    try:
        names = os.listdir(QUEUE_DIR)
    except OSError:
        return []
    out = []
    for n in names:
        if n.endswith(".json"):
            it = read_json(QUEUE_DIR / n)
            if isinstance(it, dict) and it.get("qid"):
                out.append(it)
    out.sort(key=lambda x: (int(x.get("created_at") or 0), str(x["qid"])))
    return out


def queue_counts() -> dict:
    c: dict = {}
    for it in list_queue():
        c[it.get("status")] = c.get(it.get("status"), 0) + 1
    return c


def _voice_refused_recently() -> bool:
    at = int((read_json(VOICE_TUNING_FILE, {}) or {}).get("refused_at") or 0)
    return now_ms() - at < VOICE_REFUSED_TTL_MS


def plan_attachment(src: Path, requested: str = "auto") -> dict:
    """Validate one attachment and decide how it goes out (kind, size)."""
    if not src.is_file():
        raise AttachError("missing", f"file not found: {src}")
    size = src.stat().st_size
    if not size:
        raise AttachError("empty", f"file is empty: {src.name}")
    kind = media_kind_for(src, requested or "auto")
    if kind not in MEDIA_KINDS:
        raise AttachError("bad_kind", f"unknown attachment kind: {kind}")
    note = None
    if kind == "image" and src.suffix.lower() not in IMAGE_SEND_EXTS:
        raise AttachError("bad_type", f"not an image file ({src.suffix}); "
                                      "send it with --attach-as file")
    if kind == "image" and size > IMAGE_SEND_MAX_BYTES:
        kind, note = "file", (f"image larger than {fmt_size(IMAGE_SEND_MAX_BYTES)}: "
                              "sent as a file")
    elif kind == "video" and size > VIDEO_SEND_MAX_BYTES:
        # 2026-11-04 review: the queue re-encodes videos over
        # VIDEO_COMPRESS_OVER before the upload, but v5.0.5 refused anything
        # over the send limit right here - so that path never ran for the
        # files that needed it. Accept what ffmpeg can shrink; if the result
        # is still too big, the upload says so (permanent failure + notice).
        if COMPRESS_ENABLED and ffmpeg_path() and size <= VIDEO_COMPRESS_SOURCE_MAX:
            note = (f"video is {fmt_size(size)}, over the "
                    f"{fmt_size(VIDEO_SEND_MAX_BYTES)} limit: re-encoded "
                    "(1080p H.264) before the upload")
        else:
            raise AttachError("too_large", f"{src.name} is {fmt_size(size)}; videos are "
                                           f"limited to {fmt_size(VIDEO_SEND_MAX_BYTES)} "
                                           "- compress it first")
    elif kind == "voice":
        if size > VOICE_SOURCE_MAX_BYTES:
            raise AttachError("too_large", f"{src.name} is {fmt_size(size)}; too big "
                                           "for a voice note")
        if not _native_voice_enabled():
            # Default: file card. The native voice note would be silently
            # dropped by the channel (see _native_voice_enabled), so trying
            # it first only loses the audio; a file card is what the user
            # actually receives. The 60 s voice-note cap does not apply to
            # a file, so a longer clip is fine here (FILE_SEND_MAX_BYTES
            # below still applies).
            kind, note = "file", ("iLink silently drops bot voice notes "
                                  "(verified on a real device 2026-11-01): "
                                  "sent as an audio file")
        elif _voice_refused_recently():
            kind, note = "file", ("the server refused voice notes in the last "
                                  "24 h: sent as an audio file")
    if kind == "file" and size > FILE_SEND_MAX_BYTES:
        raise AttachError("too_large", f"{src.name} is {fmt_size(size)}; files are "
                                       f"limited to {fmt_size(FILE_SEND_MAX_BYTES)} "
                                       "- compress or split it")
    return {"kind": kind, "size": size, "note": note}


def enqueue_attachment(user: str, src, *, kind: str = "auto",
                       caption: str | None = None, task_id: str | None = None,
                       context_token: str | None = None, hold: bool = False,
                       claim: bool = False, gate: bool = False,
                       notify: str = "always", waiter_s: float = 0) -> dict:
    """Copy `src` into the bridge's outbox and create a queue item.
    hold: invisible to the worker until the caller commits it (complete);
    claim: created as already owned by this process (inline first round);
    gate: wait (<= 2 min) until the task's text reply has gone out;
    notify: "always" | "background" (not while a CLI is waiting on it,
    i.e. before waiter_until) | "never". Raises AttachError."""
    if not user:
        raise AttachError("no_user", "no recipient")
    src = Path(src).expanduser()
    plan = plan_attachment(src, kind)
    qid = f"q{now_ms()}-{secrets.token_hex(3)}"
    for d in (ATTACH_DIR, QUEUE_DIR):
        d.mkdir(parents=True, exist_ok=True)
    name = _sanitize_filename(src.name)
    dst = ATTACH_DIR / f"{qid}__{name}"
    shutil.copyfile(src, dst)
    os.chmod(dst, 0o600)
    now = now_ms()
    item = {
        "qid": qid, "task_id": task_id, "to_user": user,
        "context_token": context_token, "kind": plan["kind"],
        "requested_kind": kind or "auto", "path": str(dst), "name": name,
        "bytes": plan["size"], "caption": caption or None,
        "caption_sent": False,
        "status": "hold" if hold else ("sending" if claim else "pending"),
        "gate": bool(gate and task_id), "rounds": 0,
        "max_rounds": QUEUE_MAX_ROUNDS, "deferrals": 0, "next_at": 0,
        "created_at": now, "errors": [], "notify": notify,
        "waiter_until": now + int(waiter_s * 1000) if waiter_s else 0,
    }
    if plan.get("note"):
        item["note"] = plan["note"]
    if claim:
        item.update(sender_pid=os.getpid(), phase="prepare", claimed_at=now)
    if plan["kind"] == "voice":
        silk = ATTACH_DIR / f"{qid}.silk"
        try:
            info = audio_to_silk(dst, silk)
            item["voice"] = {**info, "silk_path": str(silk)}
        except VoiceCodecError as e:
            silk.unlink(missing_ok=True)
            if plan["size"] > FILE_SEND_MAX_BYTES:
                dst.unlink(missing_ok=True)
                raise AttachError("too_large", f"voice not possible ({e}) and "
                                               "the audio is too big for a file") from e
            item["kind"] = "file"
            item["fallback"] = f"voice->file ({e.code})"
            item["note"] = f"voice note not possible ({e}); the audio goes out as a file"
    save_qitem(item)
    log(f"queue: {qid} {item['kind']} name={name} bytes={plan['size']} "
        f"task={task_id or '-'} status={item['status']}"
        + (f" fallback={item['fallback']}" if item.get("fallback") else ""))
    return item


def _drop_qitem(qid: str) -> None:
    it = load_qitem(qid) or {}
    for pth in (it.get("path"), (it.get("voice") or {}).get("silk_path"),
                    it.get("compressed")):
        if pth:
            Path(pth).unlink(missing_ok=True)
    _qpath(qid).unlink(missing_ok=True)


def _cancel_qitem(qid: str) -> bool:
    with task_lock():
        it = load_qitem(qid)
        if not it or it.get("status") in ("sent", "cancelled"):
            return False
        it.update(status="cancelled", finished_at=now_ms())
        save_qitem(it)
    for pth in (it.get("path"), (it.get("voice") or {}).get("silk_path"),
                    it.get("compressed")):
        if pth:
            Path(pth).unlink(missing_ok=True)
    log(f"queue: {qid} cancelled")
    return True


def _task_text_settled(task_id: str) -> bool:
    t = load_task(task_id)
    if not t:
        return True
    st = t.get("status")
    if st in ("waiting_send", "sending"):
        return False
    if st == "send_failed" and not (t.get("send") or {}).get("no_auto_retry"):
        return False
    return True


def _claim_qitem(qid: str, owned: bool = False) -> dict | None:
    with task_lock():
        it = load_qitem(qid)
        if not it:
            return None
        if owned and it.get("status") == "sending" \
                and it.get("sender_pid") == os.getpid():
            return it
        if it.get("status") != "pending" or int(it.get("next_at") or 0) > now_ms():
            return None
        if (it.get("gate") and it.get("task_id")
                and (now_ms() - int(it.get("created_at") or 0) < QUEUE_GATE_MAX_MS
                     or _task_parked(it["task_id"]))
                and not _task_text_settled(it["task_id"])):
            return None
        it.update(status="sending", sender_pid=os.getpid(), phase="prepare",
                  claimed_at=now_ms())
        save_qitem(it)
        return it


def _qlabel(it: dict) -> str:
    if it.get("requested_kind") == "voice":
        return "语音"
    return KIND_LABELS.get(it.get("kind"), "文件")


def _notify_queue_problem(it: dict) -> None:
    if it.get("notify") == "never" or it.get("notified"):
        return
    if it.get("notify") == "background" \
            and now_ms() < int(it.get("waiter_until") or 0):
        return          # the CLI that queued it is still watching and reports
    last = (it.get("errors") or [{}])[-1]
    reason = _reason_cn(last.get("kind") or "", last.get("detail") or "")
    label, name = _qlabel(it), it.get("name")
    if it.get("status") == "uncertain":
        text = (f"{label}《{name}》发送时网络中断，不确定你收到没有。"
                "没收到的话回复「重发」。")
    elif it.get("final_code") == "missing":
        text = f"{label}《{name}》发不出去：桥接里保存的副本丢了，需要重新生成一次。"
    elif it.get("final_code") == "rejected":
        text = f"{label}《{name}》发不出去：{reason}。"
    else:
        took = fmt_duration(now_ms() - int(it.get("created_at") or now_ms()))
        text = (f"{label}《{name}》没能发出去：{reason}（{took}内试了 {it.get('rounds')} 轮）。"
                "文件还留着，回复「重发」我再试一次。")
    nid = create_notice(it.get("to_user"), text, reason=f"queue_{it.get('status')}",
                        about={"qid": it["qid"], "task_id": it.get("task_id")})
    if nid:
        it["notified"] = nid
        save_qitem(it)


def _finish_qitem_raw(it: dict, status: str, code: str | None = None,
                  error: str | None = None) -> dict:
    it["status"] = status
    it["finished_at"] = now_ms()
    if code:
        it["final_code"] = code
    if error:
        it["final_error"] = error
    save_qitem(it)
    if status == "sent":
        for pth in (it.get("path"), (it.get("voice") or {}).get("silk_path"),
                    it.get("compressed")):
            if pth:
                Path(pth).unlink(missing_ok=True)
        history_append("bot", f"[{_qlabel(it)}] {it.get('name')}",
                       mid=it.get("task_id"), kind=it.get("kind"))
        log(f"queue: {it['qid']} sent kind={it.get('kind')} rounds="
            f"{int(it.get('rounds') or 0) + 1}"
            + (f" fallback={it['fallback']}" if it.get("fallback") else ""))
    elif status in ("failed", "uncertain"):
        log(f"queue: {it['qid']} -> {status} ({code}): {_scrub(error or '')}")
        _notify_queue_problem(it)
    return it


def _queue_failure(it: dict, e: ApiError, phase: str) -> dict:
    now = now_ms()
    errs = list(it.get("errors") or [])[-4:]
    errs.append({"at": now, "phase": phase, "kind": e.kind, "code": e.code,
                 "detail": _scrub(e)[:120]})
    it["errors"] = errs
    log(f"queue: {it['qid']} {phase} failed ({e.kind}: {_scrub(e)})")
    if phase == "message" and e.kind in ("network", "timeout"):
        return _finish_qitem(it, "uncertain", code="uncertain",
                             error=f"{e.kind} while sending the media message; "
                                   "result unknown")
    if phase == "caption" and e.kind in ("network", "timeout"):
        it["caption_sent"] = True        # may have arrived: never repeat it
    if (e.kind == "business" and e.code in (RATE_LIMIT_ERRCODE, STALE_ERRCODE)
            and int(it.get("deferrals") or 0) < QUEUE_DEFER_MAX):
        if e.code == RATE_LIMIT_ERRCODE:
            sent_phase = phase in ("caption", "message")
            why = (classify_rejection(e, it.get("to_user"), it.get("ctx_used"))
                   if sent_phase else "rate_limit")
            if why in ("quota", "window"):
                if why == "quota":
                    quota_exhaust(it.get("ctx_used"))
                return _park_qitem(it, why)
            if sent_phase:
                quota_refund(it.get("ctx_used"))
            until = note_rate_limit(int(it.get("deferrals") or 0))
        else:
            paused = int((read_json(SESSION_FILE, {}) or {})
                         .get("paused_until_ms") or 0)
            until = max(paused, now + SESSION_DEFER_MS)
        it.update(status="pending", next_at=until,
                  deferrals=int(it.get("deferrals") or 0) + 1)
        save_qitem(it)
        return it
    if it.get("kind") == "voice" and phase == "message" \
            and e.kind in ("business", "http"):
        with contextlib.suppress(OSError):
            atomic_write_json(VOICE_TUNING_FILE, {"refused_at": now}, fsync=False)
        it.update(kind="file", status="pending", next_at=0,
                  fallback=f"voice->file (server refused the voice item: "
                           f"{e.code if e.code is not None else e.kind})")
        save_qitem(it)
        log(f"queue: {it['qid']} voice item refused; resending as a file")
        return it
    msg = str(e)
    permanent = e.kind in ("business", "config") and any(x in msg for x in (
        "file exceeds", "file not found", "file is empty", "not an image",
        "media encrypt failed", "ciphertext size mismatch", "baseurl not allowed"))
    rounds = int(it.get("rounds") or 0) + 1
    it["rounds"] = rounds
    if "client error" in msg and rounds >= 2:
        permanent = True                 # a 4xx twice, with fresh upload params
    if permanent:
        return _finish_qitem(it, "failed", code="rejected", error=msg[:160])
    if rounds >= int(it.get("max_rounds") or QUEUE_MAX_ROUNDS):
        return _finish_qitem(it, "failed", code="exhausted",
                             error=f"{rounds} rounds failed; last: {e.kind}: "
                                   f"{_scrub(e)[:100]}")
    delay = QUEUE_RETRY_DELAYS_MS[min(rounds - 1, len(QUEUE_RETRY_DELAYS_MS) - 1)]
    it.update(status="pending", next_at=now + delay)
    save_qitem(it)
    return it


def _task_parked(task_id: str | None) -> bool:
    t = load_task(task_id) if task_id else None
    return bool(t and t.get("status") == "send_failed"
                and (t.get("send") or {}).get("deferred") in ("quota", "window"))


def _park_qitem(it: dict, why: str) -> dict:
    """Not sent and not failed: waits for the user's next message, which
    brings a fresh reply budget and re-opens the 24 h window (v5)."""
    n = int(it.get("deferrals") or 0)
    it.update(status="pending", deferred=why, deferrals=n + 1,
              next_at=_defer_until(why, n), phase=None)
    save_qitem(it)
    log(f"queue: {it['qid']} parked ({why}) until the user writes again")
    return it


COMPRESS_ENABLED = os.environ.get("WECHAT_BRIDGE_COMPRESS", "1") != "0"
# 2026-11-02 detail-first change (user choice): only shrink close to the
# hard send limits, so ordinary large pictures/videos go out untouched.
# Old thresholds (2 MB / 20 MB) traded visible detail for fewer egress
# bytes; the queue already retries a cut upload for ~18 min, so the
# detail loss was the worse trade. Env overrides keep the old behaviour
# available without a code change.
IMAGE_COMPRESS_OVER = int(os.environ.get(
    "WECHAT_BRIDGE_IMAGE_COMPRESS_OVER_MB", "8")) * 1024 * 1024
VIDEO_COMPRESS_OVER = int(os.environ.get(
    "WECHAT_BRIDGE_VIDEO_COMPRESS_OVER_MB", "40")) * 1024 * 1024
# Largest source video accepted for re-encoding to fit the 50 MB send limit.
VIDEO_COMPRESS_SOURCE_MAX = _env_int(
    "WECHAT_BRIDGE_VIDEO_COMPRESS_SOURCE_MAX_MB", 300) * 1024 * 1024


def _maybe_compress(it: dict, path: Path, kind: str) -> Path:
    """v5: shrink only near-limit images (> 8 MB: max 4000 px; PNG stays
    PNG) and videos (> 40 MB: 1080p H.264, crf 23, audio 128k) once, before
    the first upload. Below those sizes the original goes out untouched,
    so no detail is lost to shrinking. The original is used when shrinking
    fails or does not help. WECHAT_BRIDGE_COMPRESS=0 disables it, and
    WECHAT_BRIDGE_IMAGE/VIDEO_COMPRESS_OVER_MB move the thresholds."""
    done_ = it.get("compressed")
    if done_ is not None:
        return Path(done_) if done_ and Path(done_).is_file() else path
    ff = ffmpeg_path() if COMPRESS_ENABLED else None
    try:
        size = path.stat().st_size
    except OSError:
        return path
    out, cmd, timeout = None, None, 90
    if ff and kind == "image" and size > IMAGE_COMPRESS_OVER \
            and path.suffix.lower() in (".jpg", ".jpeg", ".png", ".webp", ".bmp"):
        ext = ".png" if path.suffix.lower() == ".png" else ".jpg"
        out = ATTACH_DIR / f"{it['qid']}.small{ext}"
        cmd = [ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-i", str(path),
               "-vf", "scale='min(4000,iw)':'min(4000,ih)':force_original_aspect_ratio=decrease",
               *(["-q:v", "2"] if ext == ".jpg" else ["-compression_level", "9"]),
               "-frames:v", "1", str(out)]
    elif ff and kind == "video" and size > VIDEO_COMPRESS_OVER:
        out = ATTACH_DIR / f"{it['qid']}.small.mp4"
        cmd = [ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-y", "-i", str(path),
               "-vf", "scale=-2:'min(1080,ih)'", "-c:v", "libx264", "-preset",
               "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k",
               "-movflags", "+faststart", str(out)]
        timeout = 900
    result = ""
    if cmd:
        try:
            proc = subprocess.run(cmd, stdin=subprocess.DEVNULL, capture_output=True, timeout=timeout)
            small_ = out.stat().st_size if (proc.returncode == 0
                                             and out.is_file()) else 0
            limit_ = _media_limit(kind)
            # Smaller by 15 %+, or - for a source over the send limit - any
            # result that now fits under it.
            if 0 < small_ and (small_ < size * 0.85 or size > limit_ >= small_):
                os.chmod(out, 0o600)
                result = str(out)
                log(f"queue: {it['qid']} {kind} shrunk {fmt_size(size)} -> "
                    f"{fmt_size(out.stat().st_size)}")
        except (OSError, subprocess.SubprocessError):
            pass
        if not result and out is not None:
            out.unlink(missing_ok=True)
    it["compressed"] = result
    return Path(result) if result else path


def process_queue_item(qid: str, owned: bool = False) -> dict | None:
    """One round for one item; returns the item afterwards (None if it was
    not due / not ours). v5: the 24 h window and the reply budget are checked
    BEFORE uploading (a parked item waits for the user's next message instead
    of burning rounds); big images/videos are shrunk first; the delivered
    item is remembered for quotes."""
    it = _claim_qitem(qid, owned)
    if it is None:
        return None
    creds = _SERVE_CREDS.get("creds") or load_creds()
    if not creds:
        it.update(status="pending", next_at=now_ms() + 60_000)
        save_qitem(it)
        return it
    user = it["to_user"]
    task_ctx = it.get("context_token")
    if not task_ctx and it.get("task_id"):
        task_ctx = (load_task(it["task_id"]) or {}).get("context_token")
    ctx, left = choose_context_token(user, task_ctx)
    path = Path(it.get("path") or "")
    if not path.is_file():
        return _finish_qitem(it, "failed", code="missing",
                             error="the queued copy of the file is gone")
    need = 1 + (1 if it.get("caption") and not it.get("caption_sent") else 0)
    if window_state(user)[1] is False:
        return _park_qitem(it, "window")
    if left is not None and left < need:
        return _park_qitem(it, "quota")
    kind = it["kind"]
    send_path = path
    if kind == "voice":
        silk = Path((it.get("voice") or {}).get("silk_path") or "")
        if silk.is_file():
            send_path = silk
        else:
            kind = it["kind"] = "file"
            it["fallback"] = "voice->file (silk copy missing)"
    else:
        send_path = _maybe_compress(it, path, kind)
    it["ctx_used"] = ctx
    phase = "upload"
    t0 = time.monotonic()
    try:
        it["phase"] = "upload"
        save_qitem(it)
        uploaded = upload_media(creds, send_path, user, kind)
        if it.get("caption") and not it.get("caption_sent"):
            phase = "caption"
            if not quota_take(ctx):
                return _park_qitem(it, "quota")
            send_text(creds, user, it["caption"], ctx)
            it["caption_sent"] = True
        phase = "message"
        if not quota_take(ctx):
            return _park_qitem(it, "quota")
        it["phase"] = "message"
        it["client_id"] = new_client_id()
        save_qitem(it)
        media_item = build_media_item(kind, uploaded, it.get("name") or path.name,
                                      it.get("voice"))
        resp = send_media_message(creds, user, media_item, ctx,
                                  client_id=it["client_id"])
    except ApiError as e:
        if phase in ("caption", "message") and (
                e.kind in ("connect", "http")
                or (e.kind == "business" and e.code != RATE_LIMIT_ERRCODE)):
            quota_refund(ctx)
        return _queue_failure(it, e, phase)
    except (OSError, subprocess.SubprocessError, ValueError) as e:
        return _queue_failure(it, ApiError("local", f"{type(e).__name__}: {e}"),
                              phase)
    it["server_message_id"] = resp.get("message_id")
    it["upload_ms"] = int((time.monotonic() - t0) * 1000)
    it.pop("deferred", None)
    if it.get("server_message_id"):
        quote_put(str(it["server_message_id"]), user=user, direction="out",
                  kind=kind if kind in KIND_LABELS else "file",
                  text=f"[{_qlabel(it)}] {it.get('name')}", name=it.get("name"),
                  task=it.get("task_id"))
    return _finish_qitem(it, "sent")


def process_due_queue() -> int:
    n = 0
    for it in list_queue():
        if _stop.is_set() or rate_limited_until():
            break
        if it.get("status") != "pending" or int(it.get("next_at") or 0) > now_ms():
            continue
        if process_queue_item(it["qid"]) is not None:
            n += 1
    return n


def recover_queue() -> None:
    """Items left "sending" by a dead process: back to pending if they died
    before the media sendmessage (nothing can have been delivered), else
    uncertain. Stale holds (a `complete` that died midway) are dropped."""
    now = now_ms()
    for it in list_queue():
        st = it.get("status")
        if st == "hold" and now - int(it.get("created_at") or 0) > QUEUE_HOLD_MAX_MS:
            _drop_qitem(it["qid"])
            continue
        if st != "sending":
            continue
        if _pid_alive_bridge(it.get("sender_pid")) \
                and now - int(it.get("claimed_at") or 0) < QUEUE_STALE_MS:
            continue
        with task_lock():
            fresh = load_qitem(it["qid"])
            if not fresh or fresh.get("status") != "sending":
                continue
            if fresh.get("phase") == "message":
                _finish_qitem(fresh, "uncertain", code="interrupted",
                              error="interrupted while sending the media "
                                    "message; result unknown")
            else:
                fresh.update(status="pending", next_at=0)
                save_qitem(fresh)
                log(f"queue: {fresh['qid']} resumed after an interrupted round")


def prune_queue() -> None:
    cutoff = now_ms() - QUEUE_KEEP_MS
    for it in list_queue():
        if it.get("status") in _QUEUE_TERMINAL and int(
                it.get("finished_at") or it.get("updated_at") or 0) < cutoff:
            _drop_qitem(it["qid"])
    try:
        for f in ATTACH_DIR.iterdir():
            qid = f.name.split("__", 1)[0].split(".", 1)[0]
            if not _qpath(qid).exists() and f.stat().st_mtime * 1000 < cutoff:
                f.unlink()
    except OSError:
        pass


def _qitem_public(it: dict) -> dict:
    out = {"queue_id": it["qid"], "file_name": it.get("name"),
           "kind": it.get("kind"), "status": it.get("status")}
    for k in ("fallback", "note"):
        if it.get(k):
            out[k] = it[k]
    if it.get("status") == "pending":
        out["rounds"] = int(it.get("rounds") or 0)
        nxt = int(it.get("next_at") or 0)
        if nxt:
            out["next_retry_in_s"] = max(0, round((nxt - now_ms()) / 1000))
    errs = it.get("errors") or []
    if errs and it.get("status") != "sent":
        e = errs[-1]
        out["last_error"] = f"{e.get('phase')}: {e.get('kind')} {e.get('detail')}"
    if it.get("final_error"):
        out["error"] = it["final_error"]
    return out


def wait_queue_items(qids: list, wait_s: float) -> list[dict]:
    deadline = time.monotonic() + max(0.0, float(wait_s or 0))
    while True:
        items = [load_qitem(q) for q in qids]
        if all((x or {}).get("status") in _QUEUE_TERMINAL for x in items) \
                or time.monotonic() >= deadline:
            return [_qitem_public(x) if x else {"queue_id": q, "status": "missing"}
                    for q, x in zip(qids, items)]
        time.sleep(0.2)


def queue_worker_loop() -> None:
    last_recover = 0.0
    while not _stop.is_set():
        _queue_wake.clear()
        _beat("media-queue")
        try:
            if time.monotonic() - last_recover > 60:
                last_recover = time.monotonic()
                recover_queue()
            process_due_queue()
        except Exception as e:  # noqa: BLE001 - the worker must never die
            log(f"queue worker error: {type(e).__name__}: {_scrub(e)}")
        _queue_wake.wait(2)


# --------------------------------------------------------------------------
# "Typing..." indicator while a task is being worked on (official plugin:
# getconfig -> typing_ticket, cached per user with a random refresh inside
# 24 h and exponential retry 2 s .. 1 h; sendtyping status 1 with a 5 s
# keepalive, status 2 to cancel). The executor can take minutes; until now
# the user saw nothing at all in that time. Best effort only: failures are
# logged at most once a minute and never touch task state.
# Disable with WECHAT_BRIDGE_TYPING=0.
# --------------------------------------------------------------------------

TYPING_ENABLED = os.environ.get("WECHAT_BRIDGE_TYPING", "1") != "0"
TYPING_KEEPALIVE_SEC = 5
TYPING_MAX_MS = 10 * 60 * 1000
TYPING_STATUSES = ("downloading", "pending_execution", "executing")
CONFIG_TTL_SEC = 24 * 3600
CONFIG_RETRY_MIN_SEC = 2
CONFIG_RETRY_MAX_SEC = 3600


class TypingManager:
    def __init__(self) -> None:
        self._tickets: dict = {}
        self._typing: dict = {}
        self._quiet_until = 0.0
        self._last_err_log = 0.0
        self.sent = 0

    def _log_err(self, msg: str) -> None:
        if time.monotonic() - self._last_err_log > 60:
            self._last_err_log = time.monotonic()
            log(f"typing: {msg} (best effort; muted for 60s)")

    def _ticket(self, creds: dict, user: str) -> str:
        now = time.time()
        entry = self._tickets.get(user)
        if entry is not None and now < entry["next_fetch"]:
            return entry["ticket"]
        ok = False
        try:
            resp = bot_post(creds, "ilink/bot/getconfig",
                            {"ilink_user_id": user,
                             "context_token": latest_context_token(user)[0]},
                            timeout=10)
            ret, errcode = resp.get("ret"), resp.get("errcode")
            if errcode in (None, 0) and (
                    ret == 0 or (ret is None and resp.get("typing_ticket"))):
                self._tickets[user] = {
                    "ticket": resp.get("typing_ticket") or "",
                    "next_fetch": now + random.random() * CONFIG_TTL_SEC,
                    "retry": CONFIG_RETRY_MIN_SEC}
                ok = True
            else:
                self._log_err(f"getconfig ret={ret}")
        except ApiError as exc:
            self._log_err(f"getconfig failed ({exc.kind})")
        if not ok:
            prev = (entry or {}).get("retry", CONFIG_RETRY_MIN_SEC / 2)
            delay = min(prev * 2, CONFIG_RETRY_MAX_SEC)
            self._tickets[user] = {"ticket": (entry or {}).get("ticket", ""),
                                   "next_fetch": now + delay, "retry": delay}
        return self._tickets[user]["ticket"]

    def _send(self, creds: dict, user: str, ticket: str, status: int) -> bool:
        try:
            resp = bot_post(creds, "ilink/bot/sendtyping",
                            {"ilink_user_id": user, "typing_ticket": ticket,
                             "status": status}, timeout=10)
        except ApiError as exc:
            self._log_err(f"sendtyping failed ({exc.kind})")
            return False
        ret, errcode = resp.get("ret"), resp.get("errcode")
        code = errcode if errcode not in (None, 0) else ret
        if code not in (None, 0):           # errcode too, like _check_resp
            if code == RATE_LIMIT_ERRCODE:
                self._quiet_until = (time.monotonic()
                                     + RATE_LIMIT_RETRY_MS / 1000)
            self._log_err(f"sendtyping ret={ret} errcode={errcode}")
            return False
        self.sent += 1
        return True

    def tick(self, creds: dict | None, index: list) -> None:
        if not creds or not TYPING_ENABLED:
            return
        mono = time.monotonic()
        if mono < self._quiet_until or rate_limited_until():
            return
        session = read_json(SESSION_FILE, {}) or {}
        if int(session.get("paused_until_ms") or 0) > now_ms():
            return
        cutoff = now_ms() - TYPING_MAX_MS
        active = {t.get("from_user_id") for t in index
                  if t.get("from_user_id")
                  and t.get("status") in TYPING_STATUSES
                  and max(t.get("received_at") or 0,
                          t.get("heartbeat_at") or 0) >= cutoff}
        for user in active:
            last = self._typing.get(user)
            if last is not None and mono - last < TYPING_KEEPALIVE_SEC:
                continue
            ticket = self._ticket(creds, user)
            if not ticket:
                continue
            self._typing[user] = mono
            self._send(creds, user, ticket, 1)
        for user in [u for u in self._typing if u not in active]:
            del self._typing[user]
            ticket = (self._tickets.get(user) or {}).get("ticket")
            if ticket:
                self._send(creds, user, ticket, 2)


TYPING = TypingManager()


_typing_wake = threading.Event()     # v5: a new message starts "typing" at once


def typing_loop() -> None:
    """v4 ticked once a second (up to 1 s plus a getconfig round trip before
    "typing..." appeared). v5 wakes on every new message and fetches the
    typing ticket for the bound user at start-up."""
    warmed = False
    while not _stop.is_set():
        _typing_wake.clear()
        _beat("typing")
        creds = _SERVE_CREDS.get("creds")
        try:
            owner = (creds or {}).get("ilink_user_id")
            if not warmed and owner and TYPING_ENABLED \
                    and latest_context_token(owner)[0]:
                warmed = True       # getconfig wants the user's context token
                TYPING._ticket(creds, owner)
            TYPING.tick(creds, scan_task_index())
        except Exception as e:  # noqa: BLE001 - best effort, never dies
            log(f"typing loop error: {type(e).__name__}")
        _typing_wake.wait(1)


def _sd_notify(state: str) -> bool:
    """systemd notify protocol (Type=notify / WatchdogSec), stdlib only;
    a no-op outside systemd."""
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return False
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.connect(addr)
            s.sendall(state.encode())
        return True
    except OSError:
        return False


_WD = {"last": 0.0}


def _watchdog_ping() -> None:
    """WATCHDOG=1 from the poll loop (at most every 10 s). If the loop wedges,
    systemd restarts the process itself - even when the hook is down too."""
    now = time.monotonic()
    if now - _WD["last"] >= 10:
        _WD["last"] = now
        _sd_notify("WATCHDOG=1")


def _maybe_vm_recovery_notice(entry: dict) -> bool:
    """Opt round 2 item 5: after a real outage (poll gap > 5 min), if a
    task was in flight, tell the user once that the service recovered
    and the task is being resumed from its checkpoint. Silent when
    healthy: no inflight task, or a normal short restart gap."""
    try:
        gap = int(entry.get("gap_ms") or 0)
    except (TypeError, ValueError):
        return False
    if gap <= 300_000:
        return False
    inflight = [x for x in scan_task_index()
                if x.get("status") in ("executing", "pending_execution", "downloading")
                and x.get("kind") not in ("command", "notice")]
    if not inflight:
        return False
    t = load_task(inflight[-1]["message_id"]) or {}
    user = t.get("from_user_id")
    if not user:
        return False
    try:
        nid = create_notice(
            user,
            f"微信桥接服务刚恢复（之前中断了约 {fmt_duration(gap)}），你的任务还在，已断点续跑，不用重发。",
            reason="vm_recovery", about={"task_id": inflight[-1]["message_id"]})
    except Exception as exc:  # noqa: BLE001 - telemetry must not break serve start
        log(f"vm recovery notice failed: {type(exc).__name__}")
        return False
    return bool(nid)


def cmd_serve(args) -> int:
    creds = load_creds()
    if not creds:
        print(json.dumps({"ok": False,
                          "error": "no credentials; run login + wait-login first"},
                         ensure_ascii=False))
        return 2
    # single instance via PID lock
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    lock_fd = open(STATE_DIR / "serve.lock", "a+")
    try:
        fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(json.dumps({"ok": False, "error": "serve already running"},
                         ensure_ascii=False))
        return 2
    PID_FILE.write_text(str(os.getpid()))
    _LOG_TO_STDOUT["on"] = True
    # 2026-11-06: messages for Muse go through state/pending.json batches
    # (dispatched after a 3 s pause, 60 s at most); WECHAT_BRIDGE_BATCH=0 off.
    batching_on(BATCH_ENABLED)
    entry = serve_start_entry(now_ms())
    update_heartbeat(serve_pid=os.getpid(), serve_started_at=entry["at"])
    with contextlib.suppress(OSError):
        starts = serve_starts_read()[-29:]
        atomic_write_json(SERVE_STARTS_FILE, starts + [entry], fsync=False)

    # One recovery notice per outage restart, only if a task was in flight.
    try:
        _maybe_vm_recovery_notice(entry)
    except Exception as exc:  # noqa: BLE001
        log(f"vm recovery notice failed: {type(exc).__name__}")

    def handle_signal(signum, frame):
        _stop.set()
        if _SIGNAL_STATE["critical"]:
            # 2026-11-04 review: a batch is being stored / the cursor saved -
            # let it finish (milliseconds); the loop stops right after it.
            return
        # Raising here interrupts the blocking long-poll request at once
        # (PEP 475 retries only when the handler returns normally), so a
        # stop does not have to wait out the poll or be SIGKILLed.
        raise GracefulStop()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)
    # `complete` / send-direct hand replies to this process (warm keep-alive
    # connections) and wake the sender with SIGUSR1. Advertised in the
    # heartbeat so a CLI never signals an older serve without this handler
    # (SIGUSR1's default action would kill it).
    def _wake_workers(signum, frame):
        _send_wake.set()
        _queue_wake.set()
    signal.signal(signal.SIGUSR1, _wake_workers)
    # v5: SIGHUP re-reads state/proxy.env without a restart; advertised as
    # hup_pid so the hook only sends it to a serve that handles it.
    signal.signal(signal.SIGHUP, lambda signum, frame: PROXY_POOL.request_reload())
    update_heartbeat(handoff_pid=os.getpid(), hup_pid=os.getpid(),
                     transport_mode=PROXY_POOL.mode)
    _SERVE_CREDS["creds"] = creds
    log(f"serve starting pid={os.getpid()} bot={mask(creds.get('ilink_bot_id'))}"
        + (f" batch={BATCH_QUIET_MS}/{BATCH_MAX_MS}ms" if _BATCH["on"] else " batch=off"))
    PROXY_POOL.start_rotator()
    workers = [threading.Thread(target=sender_loop, name="sender", daemon=True)]
    workers += [threading.Thread(target=media_worker_loop, name=f"media-{i}",
                                 daemon=True) for i in range(MEDIA_WORKERS)]
    workers += [threading.Thread(target=typing_loop, name="typing", daemon=True),
                threading.Thread(target=queue_worker_loop, name="media-queue",
                                 daemon=True),
                threading.Thread(target=batch_loop, name="batcher", daemon=True)]
    for w in workers:
        w.start()
    _sd_notify("READY=1\nSTATUS=polling WeChat")
    try:
        recover_downloading()
    except Exception as e:  # noqa: BLE001
        log(f"recover_downloading error: {type(e).__name__}")
    try:
        receiver_loop(creds)
    except GracefulStop:
        log("serve stopping (signal)")
    finally:
        _stop.set()
        for ev in (_send_wake, _queue_wake, _typing_wake, _batch_wake):
            ev.set()
        _sd_notify("STOPPING=1")
        # 2026-11-04 review: let the workers finish the step they are in (a
        # reply part, an upload round) instead of being killed mid-request
        # at interpreter exit - that left tasks in "sending" and, after the
        # restart, a false "不确定你收到没有" notice to the user.
        deadline = time.monotonic() + SHUTDOWN_GRACE_SEC
        for w in workers:
            w.join(max(0.0, deadline - time.monotonic()))
        busy = [w.name for w in workers if w.is_alive()]
        if busy:
            log(f"serve stopping: still busy after {SHUTDOWN_GRACE_SEC}s: "
                + ", ".join(busy))
        PID_FILE.unlink(missing_ok=True)
        update_heartbeat(handoff_pid=None, hup_pid=None)
        log("serve stopped")
    return 0


# --------------------------------------------------------------------------
# claim / complete / send / status / stop
# --------------------------------------------------------------------------

STALE_EXECUTING_MS = 10 * 60 * 1000



# --------------------------------------------------------------------------
# 2026-11-07 长任务接力 (relay): long_hint, plan, complete --continue
# --------------------------------------------------------------------------
# One hook-woken executor turn is cut at 600 s by the platform (hooks/*.json
# script_timeout_secs - the bridge cannot extend it) and the bridge reclaims
# a task after 10 silent minutes. A long task used to hit the first wall,
# come back with a one-line checkpoint and redo its work until MAX_RECLAIMS
# gave up (2026-11-06 21:13, 「去敏开源」: 3 runs, the 24-file comparison and
# the scan started over each time). Now:
#   * the bridge flags likely-long tasks by rule (long_hint: plain Python, no
#     model call; a short task's claim output does not change by one byte);
#   * the executor writes a step plan (`progress --plan`, kept in
#     state/plans/<id>.json, 0600; every step <= RELAY_STEP_MAX_MIN minutes),
#     marks finished steps (`progress --step-done`) and hands the rest to a
#     fresh turn with `complete --continue`: the same task goes back to
#     pending_execution with a new wake_key - the hook wakes a new executor
#     at its next poll - and the next claim carries the plan (done steps and
#     their artifacts, the next step). No user message is needed; the user
#     gets progress notes and one final reply;
#   * the chain is bounded (legs / minutes / two legs in a row without a
#     finished step): at a bound the bridge tells the user where it stopped,
#     what is left and that 「继续」 picks it up (claim resume_plan).
# A leg killed at 600 s is still reclaimed by _reclaim_stale - and resumes
# from the plan. WECHAT_BRIDGE_RELAY=0 (state/tuning.env) turns it all off.
RELAY_ON = os.environ.get("WECHAT_BRIDGE_RELAY", "1").strip() != "0"
RELAY_STEP_MAX_MIN = max(1, min(9, _env_int("WECHAT_BRIDGE_RELAY_STEP_MAX_MIN", 7)))
RELAY_MAX_LEGS = max(2, min(20, _env_int("WECHAT_BRIDGE_RELAY_MAX_LEGS", 8)))
RELAY_MAX_MIN = max(10, min(240, _env_int("WECHAT_BRIDGE_RELAY_MAX_MIN", 60)))
RELAY_NOTIFY_GAP_MIN = max(0, min(120, _env_int("WECHAT_BRIDGE_RELAY_NOTIFY_GAP_MIN", 10)))
PLAN_KEEP_DAYS = max(1, _env_int("WECHAT_BRIDGE_PLAN_KEEP_DAYS", 14))
PLAN_DIR = STATE_DIR / "plans"
PLAN_MAX_STEPS = 20
PLAN_MAX_BYTES = 64 * 1024
LONG_HINT_WORDS = tuple(w.strip() for w in os.environ.get(
    "WECHAT_BRIDGE_LONG_HINT_WORDS",
    "打包,脱敏,去敏,开源,全套测试,全量测试,run_all,批量,逐项,逐个文件,扫描,"
    "所有文件,全部文件,每个文件").split(",") if w.strip())
LONG_HINT_MIN_FILES = max(2, _env_int("WECHAT_BRIDGE_LONG_HINT_MIN_FILES", 5))
LONG_HINT_FILE_MB = max(1, _env_int("WECHAT_BRIDGE_LONG_HINT_FILE_MB", 20))
_LH_COUNT = re.compile(r"(\d+|十几|几十|数十|上百|几百|数百|上千|[一二两三四五六七八九十]+)\s*个\s*"
                       r"(?:改动|修改|改过)?的?(?:文件|脚本|模块|仓库|项目|页面|网页|链接|测试)")
_LH_SCOPE = re.compile(r"(?:全部|所有|整个|每个|每一个)的?(?:文件|代码|测试|仓库|目录|项目|"
                       r"图片|视频|链接|网页|页面|日志|记录)")
_LH_ASKING = re.compile(r"是什么|什么意思|啥意思|为什么|为啥|怎么理解|有什么用|干什么用|"
                        r"是干嘛的|什么区别|啥区别|的区别")
_LH_DOING = re.compile(r"把|将|帮我|帮忙|给我|麻烦|请你|你去|去做|跑一遍|跑一下|做一下|"
                       r"弄一下|搞一下")
_LH_ARCHIVES = (".zip", ".tar", ".tgz", ".gz", ".7z", ".rar", ".xz", ".bz2")
_CN_DIGITS = {"一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5, "六": 6,
              "七": 7, "八": 8, "九": 9}
_RESUME_INTENT = re.compile(r"^(?:继续|接着做|继续做|接着来|接着干|继续干|继续上次的?|"
                            r"接着上次的?)(?:吧|啊|呀)?[。.!！~～]*$")
_SECRETISH = re.compile(r"credentials\.json|-----BEGIN [A-Z ]*PRIVATE KEY|"
                        r"(?i:\b(?:bot_token|access_token|refresh_token|api[_-]?key|"
                        r"secret|password|passwd)\b\s*[:=]\s*\S{6,})|"
                        # v1.6: free-text forms and well-known token shapes (after the
                        # gitleaks / detect-secrets rule sets) + the iLink bot token prefix
                        r"(?:密码|口令|密钥|令牌)\s*(?:是|为|[:：=])\s*[A-Za-z0-9!@#$%^&*()_+\-=.,?/]{4,}|"
                        r"\bilb_[A-Za-z0-9_-]{12,}|\bsk-[A-Za-z0-9_-]{20,}|\bgh[pousr]_[A-Za-z0-9]{30,}|"
                        r"\bAKIA[0-9A-Z]{16}\b|\bxox[abprs]-[A-Za-z0-9-]{10,}|"
                        r"\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}")
_LONG_HINT_WARM = ("这条是热接的（本回合已经用掉一段）：写完计划、发出通知就 "
                   "`complete --continue`，让新一棒用完整的回合做。")
_RELAY_HOW = {"continue": "上一棒做完手上的步骤后主动交棒",
              "reclaimed": "上一棒中途断了（10 分钟没心跳被收回，多半是回合到了 600 秒）",
              "superseded": "上一棒被用户的新消息打断（新消息也在这一批里，最后一起答）"}
_STEP_KEYS = ("step_id", "do", "inputs", "outputs", "est_min", "done_when")


def _cn_count(s: str) -> int:
    if s.isdigit():
        return int(s)
    if any(c in s for c in "几数百千"):
        return 100
    if "十" in s:
        a, _, b = s.partition("十")
        return _CN_DIGITS.get(a, 1) * 10 + _CN_DIGITS.get(b, 0)
    return _CN_DIGITS.get(s, 0)


def long_hint_rules(t: dict) -> list:
    """Why task `t` will probably not fit in one executor turn, or [] (the
    common case: short - the claim stays exactly as it was). Plain rules, far
    under a millisecond, no model call; words and thresholds are the
    WECHAT_BRIDGE_LONG_HINT_* knobs. Asking ABOUT 开源 / 打包 is not long."""
    if t.get("kind") in ("command", "notice"):
        return []
    text = (t.get("text") or "").strip()
    low = text.lower()
    why = [w for w in LONG_HINT_WORDS if w.lower() in low]
    for m in _LH_COUNT.finditer(text):
        if _cn_count(m.group(1)) >= LONG_HINT_MIN_FILES:
            why.append(re.sub(r"\s+", "", m.group(0)))
            break
    m = _LH_SCOPE.search(text)
    if m:
        why.append(m.group(0))
    if t.get("kind") == "file":
        name = str(t.get("file_name") or "").lower()
        try:
            size = int(t.get("media_bytes") or 0)
        except (TypeError, ValueError):
            size = 0
        if name.endswith(_LH_ARCHIVES):
            why.append("压缩包")
        elif size >= LONG_HINT_FILE_MB * 1024 * 1024:
            why.append(f"{size // 1048576} MB 的文件")
    if why and _LH_ASKING.search(text) and not _LH_DOING.search(text):
        return []
    return list(dict.fromkeys(why))


def _long_hint_text(why: list, t: dict | None = None) -> str:
    n = RELAY_STEP_MAX_MIN
    if bg_available() and not (t or {}).get("bg") \
            and not _SECRETISH.search((t or {}).get("text") or ""):
        # 2026-11-07 v1.4: the watch hook is alive - hand it to the background
        return (f"长任务（桥按规则判的：{'、'.join(why[:4])}），交给后台做：第一个动作就是 "
                "`complete --task-id <id> --claim-id <claim_id> --background`（桥替你回用户一条信号、"
                "先写队列再把任务移出前台；后台的主代理 + 子代理做完会把完整结果发回），然后立即"
                "结束本回合；不要在前台做，也不要再发别的。细则 EXECUTOR-relay.md §0。")
    return (f"长任务（桥按规则判的：{'、'.join(why[:4])}）。按 EXECUTOR-relay.md 接力：领取后第一个"
            f"动作就写计划并 `progress --task-id <id> --claim-id <claim_id> --plan <计划.json> "
            f"--notify`——计划先写粗的（每步一句话 + 预估分钟，每步 est_min ≤{n}），十几秒内发出，"
            f"之后可以再 --plan 细化，已完成的步骤保留；桥按计划给用户发大纲、步数和预估总时长。"
            f"一次只做一步，做完 `progress --step-done <step_id> --artifact <产物>`；从 claim 起约 "
            f"{n} 分钟、或下一步放不下时 `complete --continue` 交下一棒并立即结束本回合；全部步骤"
            f"做完才 complete 发唯一一条完整结果。")


def _relay_claim_extras(view: list, warm: bool) -> None:
    """claim: one batch with several files is long even when no single
    message is; a long task claimed by a warm (then-wait) turn only plans."""
    files = sum(1 for v in view if v.get("kind") == "file")
    if files >= 3 and not any("long_hint" in v or "plan" in v for v in view):
        view[-1]["long_hint"] = _long_hint_text([f"一批 {files} 个文件"],
                                                load_task(view[-1]["message_id"]))
    if warm:
        for v in view:
            if "complete --continue" in v.get("long_hint", ""):
                v["long_hint"] += _LONG_HINT_WARM


def _plan_name(task_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(task_id))[:120] or "_"


def _plan_path(task_id: str) -> Path:
    return PLAN_DIR / (_plan_name(task_id) + ".json")


def _plan_workdir(task_id: str) -> Path:
    return PLAN_DIR / (_plan_name(task_id) + ".d")


def plan_load(task_id: str) -> dict | None:
    pl = read_json(_plan_path(task_id), None)
    return pl if isinstance(pl, dict) and isinstance(pl.get("steps"), list) else None


_plan_pruned = [False]


def plan_save(pl: dict) -> None:
    """state/plans/<id>.json, 0600 in a 0700 directory: in-flight state, not
    in the hourly snapshot (ops/backup.sh) and never in a package (state/ is
    in .gitignore). Plans and work dirs idle PLAN_KEEP_DAYS days are pruned
    (once per process, on the first save)."""
    PLAN_DIR.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        os.chmod(PLAN_DIR, 0o700)
    pl["updated_at"] = now_ms()
    atomic_write_json(_plan_path(pl["task_id"]), pl, mode=0o600)
    if not _plan_pruned[0]:
        _plan_pruned[0] = True
        cut = time.time() - PLAN_KEEP_DAYS * 86400
        with contextlib.suppress(OSError):
            for p in list(PLAN_DIR.iterdir()):
                with contextlib.suppress(OSError):
                    if p.stat().st_mtime >= cut:
                        continue
                    if p.is_dir() and p.name.endswith(".d"):
                        shutil.rmtree(p, ignore_errors=True)
                    elif p.suffix == ".json":
                        p.unlink()


def _plan_from_raw(raw, task_id: str, old: dict | None) -> tuple:
    """(plan, None) or (None, why not) for the executor's plan JSON. A
    re-plan keeps the steps already done (same step_id)."""
    if isinstance(raw, list):
        raw = {"steps": raw}
    if not isinstance(raw, dict) or not isinstance(raw.get("steps"), list) or not raw["steps"]:
        return None, "plan must be a JSON object with a non-empty \"steps\" list"
    if len(raw["steps"]) > PLAN_MAX_STEPS:
        return None, f"plan has {len(raw['steps'])} steps (max {PLAN_MAX_STEPS}): merge small ones"
    blob = json.dumps(raw, ensure_ascii=False)
    if len(blob.encode("utf-8")) > PLAN_MAX_BYTES:
        return None, "plan too large (max 64 KB): keep long lists in the work files, not in the plan"
    if _SECRETISH.search(blob):
        return None, ("plan mentions credentials or a secret-looking value: a plan holds steps "
                      "and file paths only, never credentials, tokens or passwords")
    prev = {s.get("step_id"): s for s in (old or {}).get("steps") or [] if isinstance(s, dict)}
    steps, seen = [], set()
    for i, s in enumerate(raw["steps"], 1):
        if isinstance(s, str):
            s = {"do": s}
        if not isinstance(s, dict):
            return None, f"step {i}: must be an object"
        sid = str(s.get("step_id") or f"s{i}").strip()[:40]
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", sid) or sid in seen:
            return None, f"step {i}: step_id {sid!r} must be unique (letters, digits, . _ -)"
        seen.add(sid)
        do = str(s.get("do") or s.get("title") or "").strip()
        if not do:
            return None, f"step {sid}: \"do\" (what the step does) is required"
        try:
            est = float(s.get("est_min"))
        except (TypeError, ValueError):
            return None, f"step {sid}: est_min (minutes, at most {RELAY_STEP_MAX_MIN}) is required"
        if not 0 < est <= RELAY_STEP_MAX_MIN:
            return None, (f"step {sid}: est_min {est:g} is over {RELAY_STEP_MAX_MIN}: one turn ends "
                          f"at 600 s, so split it into steps of at most {RELAY_STEP_MAX_MIN} min "
                          "(e.g. a few files per step)")
        st = {"step_id": sid, "do": do[:300], "est_min": round(est, 1),
              "done_when": str(s.get("done_when") or "")[:300]}
        for k in ("inputs", "outputs"):
            v = s.get(k) or []
            if isinstance(v, str):
                v = [v]
            if not isinstance(v, list) or len(v) > 30:
                return None, f"step {sid}: {k} must be a list (at most 30)"
            st[k] = [str(x)[:300] for x in v]
        p = prev.get(sid) or {}
        if p.get("status") == "done":
            st.update({k: p[k] for k in ("status", "done_at", "leg", "artifacts") if k in p})
        else:
            st["status"] = "todo"
            if p.get("artifacts"):
                st["artifacts"] = p["artifacts"]
        steps.append(st)
    now = now_ms()
    return ({"v": 1, "task_id": task_id, "goal": str(raw.get("goal") or "")[:200],
             "steps": steps, "est_total_min": round(sum(x["est_min"] for x in steps), 1),
             "created_at": int((old or {}).get("created_at") or now), "state": "active"}, None)


def _plan_next(nx: dict) -> dict:
    v = {k: nx.get(k) for k in _STEP_KEYS}
    if nx.get("artifacts"):
        v["partial"] = [a.get("path") for a in nx["artifacts"]]
    return v


def _plan_summary(t: dict, pl: dict, now: int) -> dict:
    """What progress returns for a planned task: where the plan stands, how
    much of this leg is left, and whether to go on or hand off now."""
    steps = pl.get("steps") or []
    todo = [s for s in steps if s.get("status") != "done"]
    cl = int((t.get("execution") or {}).get("claimed_at") or now)
    left = max(0.0, round(RELAY_STEP_MAX_MIN - (now - cl) / 60000, 1))
    out = {"steps": len(steps), "done": len(steps) - len(todo),
           "todo": [s.get("step_id") for s in todo],
           "leg": len((t.get("relay") or {}).get("legs") or []) + 1,
           "leg_left_min": left, "workdir": str(_plan_workdir(t["message_id"]))}
    if not todo:
        out["advice"] = "计划里的步骤都做完了：用 complete --result 发唯一一条完整结果（不要 --continue）。"
        return out
    nx = todo[0]
    out["next"] = _plan_next(nx)
    out["next_fits"] = float(nx.get("est_min") or 0) <= left
    est = float(nx.get("est_min") or 0)
    out["advice"] = (f"接着做 {nx.get('step_id')}（预计 {est:g} 分钟，本棒还剩约 {left:g} 分钟）。"
                     if out["next_fits"] else
                     f"本棒还剩约 {left:g} 分钟，放不下 {nx.get('step_id')}（预计 {est:g} 分钟）："
                     "现在 complete --continue 交下一棒，然后结束本回合。")
    return out


def _plan_view(t: dict) -> dict | None:
    """claim: the plan for the leg that starts now - done steps and their
    artifacts, the next step, the limits - so the executor needs no extra
    read to pick up where the last leg stopped."""
    pl = plan_load(t["message_id"])
    if not pl:
        return None
    now = now_ms()
    rl = t.get("relay") or {}
    cl = int((t.get("execution") or {}).get("claimed_at") or now)
    steps = pl.get("steps") or []
    done = [s for s in steps if s.get("status") == "done"]
    todo = [s for s in steps if s.get("status") != "done"]
    leg = len(rl.get("legs") or []) + 1
    v = {"file": str(_plan_path(t["message_id"])), "workdir": str(_plan_workdir(t["message_id"])),
         "goal": pl.get("goal") or "", "leg": leg, "max_legs": RELAY_MAX_LEGS,
         "elapsed_min": round((now - int(rl.get("started_at") or cl)) / 60000, 1),
         "max_min": RELAY_MAX_MIN, "step_max_min": RELAY_STEP_MAX_MIN,
         "hand_off_by": _clock_s(cl + RELAY_STEP_MAX_MIN * 60_000),
         "done": [{"step_id": s.get("step_id"), "do": s.get("do"),
                   "artifacts": [a.get("path") for a in s.get("artifacts") or []]} for s in done],
         "next": _plan_next(todo[0]) if todo else None,
         "todo": [s.get("step_id") for s in todo]}
    if todo:
        v["relay"] = (f"接力第 {leg} 棒：done 里的步骤不要重做（产物以落盘文件为准）；开头先 "
                      f"`progress --stage \"第 {len(done) + 1}/{len(steps)} 步：…\"`，从 next 开始一次一步，"
                      "做完 `progress --step-done <step_id> --artifact <产物>`；到 hand_off_by 或下一步放"
                      "不下就 `complete --continue` 交棒并结束本回合；todo 都做完才 complete --result "
                      "发唯一一条完整结果。")
    else:
        v["relay"] = "计划里的步骤都做完了：complete --result 发唯一一条完整结果。"
    return v


def _plan_outline(t: dict, pl: dict) -> str:
    """The first progress note of a long task: outline, steps, time."""
    steps = pl.get("steps") or []
    todo = [s for s in steps if s.get("status") != "done"]
    rest = max(1, round(sum(float(s.get("est_min") or 0) for s in todo)))
    if len(todo) < len(steps):
        head = (f"「{_asked(t)}」接着做：共 {len(steps)} 步，前面 {len(steps) - len(todo)} 步已做完"
                f"（不重做），剩下的预计约 {rest} 分钟")
    else:
        head = f"「{_asked(t)}」是个长任务，分 {len(steps)} 步做，预计共约 {rest} 分钟"
    lines = [f"{i}. {s.get('do')}（约 {float(s.get('est_min') or 0):g} 分钟"
             + ("，已做完）" if s.get("status") == "done" else "）")
             for i, s in enumerate(steps[:12], 1)]
    if len(steps) > 12:
        lines.append(f"…（共 {len(steps)} 步）")
    return (head + "：\n" + "\n".join(lines) + "\n每步做完自动接着做，中途不用回我；全部做完发一条"
            "完整结果。想看进度回「进度」，要停回「取消」。")


def _relay_resume_text(t: dict, pv: dict, done_txt: str) -> str:
    legs = (t.get("relay") or {}).get("legs") or []
    how = _RELAY_HOW.get(legs[-1].get("how") if legs else "", "上一棒没做完")
    return (f"接力第 {pv['leg']} 棒：{how}；{done_txt}。按 plan 接着做：done 里的步骤不要重做，产物"
            "以落盘文件为准；next 那步如果上一棒做了一半（next.partial，或它的 outputs 已有内容），先读"
            "产物，已处理的项跳过。")


def _relay_where(t: dict) -> str:
    """「进度」 for a planned task: where the whole chain is, in one bracket."""
    pl = plan_load(t["message_id"])
    if not pl:
        return ""
    steps = pl.get("steps") or []
    todo = [s for s in steps if s.get("status") != "done"]
    leg = len((t.get("relay") or {}).get("legs") or []) + 1
    if not todo:
        return f"（{len(steps)} 步都做完了，正在写最终结果）"
    rest = max(1, round(sum(float(s.get("est_min") or 0) for s in todo)))
    return (f"（第 {len(steps) - len(todo) + 1}/{len(steps)} 步：{snippet(todo[0].get('do') or '', 30)}；"
            f"第 {leg} 棒，预计还要约 {rest} 分钟）")


def _relay_mark(t: dict, pl: dict, now: int, step_done, artifacts) -> str | None:
    """--step-done / --artifact: mark steps done in this leg and record
    artifact files (path + size, existence checked) on the last one marked,
    or - without --step-done - on the step in progress (partial results)."""
    ids = [x for x in re.split(r"[,\s]+", str(step_done or "")) if x]
    by_id = {s.get("step_id"): s for s in pl.get("steps") or []}
    bad = [x for x in ids if x not in by_id]
    if bad:
        return f"unknown step_id {', '.join(bad)} (the plan has {', '.join(map(str, by_id))})"
    arts = []
    for a in artifacts or []:
        a = str(a).strip()
        if not a:
            continue
        if _SECRETISH.search(a):
            return f"artifact {a!r} looks like a credentials file: never record credentials in a plan"
        rec: dict = {"path": a[:300]}
        try:
            p = Path(a).expanduser()
            if p.is_dir():
                rec["dir"] = True
            else:
                rec["bytes"] = p.stat().st_size
        except OSError:
            rec["missing"] = True
        arts.append(rec)
    leg = len((t.get("relay") or {}).get("legs") or []) + 1
    for x in ids:
        if by_id[x].get("status") != "done":
            by_id[x].update(status="done", done_at=now, leg=leg)
    target = by_id[ids[-1]] if ids else next(
        (s for s in pl.get("steps") or [] if s.get("status") != "done"), None)
    if arts and target is not None:
        have = {a.get("path") for a in target.get("artifacts") or []}
        target["artifacts"] = ((target.get("artifacts") or [])
                               + [a for a in arts if a["path"] not in have])[-30:]
    return None


def _relay_progress(t: dict, ex: dict, now: int, plan_raw, plan_from, step_done,
                    artifacts: list) -> tuple:
    """progress --plan / --plan-from / --step-done / --artifact: (info, None)
    or (None, why not). Writes nothing itself: cmd_progress saves
    info["_save"] together with the task once its own checks passed."""
    if not RELAY_ON:
        return None, "relay is off (WECHAT_BRIDGE_RELAY=0): report with --stage only"
    mid = t["message_id"]
    pl = plan_load(mid)
    save: list = []
    info: dict = {}
    if plan_from:
        if pl is not None:
            return None, "this task already has a plan (--plan-from adopts into a task without one)"
        old = plan_load(str(plan_from))
        if not old or old.get("user") != t.get("from_user_id") or old.get("state") != "stopped":
            return None, f"no stopped plan of this user under {plan_from}"
        pl = json.loads(json.dumps(old))
        for s in pl.get("steps") or []:
            if s.get("status") == "done":
                s["leg"] = 0                    # done by the earlier chain
        for k in ("stopped_reason", "stopped_at"):
            pl.pop(k, None)
        pl.update(task_id=mid, state="active", adopted_from=str(plan_from), created_at=now)
        old.update(state="adopted", adopted_by=mid)
        save.append(old)
        info["adopted_from"] = str(plan_from)
    if plan_raw:
        src = str(plan_raw).strip()
        try:
            if src.startswith(("{", "[")):
                raw = json.loads(src)
            elif src == "-":
                raw = json.loads(sys.stdin.read())
            else:
                raw = json.loads(Path(src).expanduser().read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            return None, f"--plan: not a readable JSON plan ({type(e).__name__}: {e})"[:300]
        new, err = _plan_from_raw(raw, mid, pl)
        if err:
            return None, err
        if pl and pl.get("adopted_from"):
            new["adopted_from"] = pl["adopted_from"]
        pl = new
        info["planned"] = True
    if pl is None:
        return None, "no plan yet: write one first (progress --plan FILE)"
    if pl.get("state", "active") != "active":
        return None, f"this task's plan is {pl.get('state')}: nothing more to record"
    err = _relay_mark(t, pl, now, step_done, artifacts)
    if err:
        return None, err
    pl["user"] = t.get("from_user_id")
    t.setdefault("relay", {}).setdefault("started_at", int(ex.get("claimed_at") or now))
    info["_save"] = [pl] + save
    steps = pl["steps"]
    info["summary"] = _plan_summary(t, pl, now)
    if info.get("planned") or info.get("adopted_from"):
        info["outline"] = _plan_outline(t, pl)
        info["stage"] = (f"计划：{len(steps)} 步，预计约 "
                         f"{round(sum(float(s.get('est_min') or 0) for s in steps))} 分钟")
    elif step_done:
        n_done = sum(1 for s in steps if s.get("status") == "done")
        last = [x for x in re.split(r"[,\s]+", str(step_done)) if x][-1]
        do = next((s.get("do") for s in steps if s.get("step_id") == last), "")
        info["stage"] = f"第 {n_done}/{len(steps)} 步做完：{do}"[:200]
    return info, None


def _relay_leg_end(t: dict, pl: dict | None, how: str, now: int) -> dict:
    """Close the running leg (caller saves t): when it started and ended,
    how, the steps it finished and its last stage - one row of
    `stats --task-id`."""
    ex = t.get("execution") or {}
    legs = t.setdefault("relay", {}).setdefault("legs", [])
    no = len(legs) + 1
    rec = {"leg": no, "claim_id": ex.get("claim_id") or ex.get("revoked_claim_id"),
           "claimed_at": int(ex.get("claimed_at") or 0), "ended_at": now, "how": how,
           "steps": [s.get("step_id") for s in (pl or {}).get("steps") or []
                     if s.get("status") == "done" and int(s.get("leg") or 0) == no],
           "stage": (ex.get("stage") or "")[:200], "calls": len(ex.get("calls") or [])}
    legs.append(rec)
    return rec


def _relay_close(t: dict, how: str, now: int) -> None:
    """The final complete of a planned task (caller holds task_lock)."""
    pl = plan_load(t["message_id"])
    _relay_leg_end(t, pl, how, now)
    t.pop("relay_pending", None)
    if pl is not None and pl.get("state", "active") == "active":
        pl["state"] = "done" if how == "complete" else "failed"
        plan_save(pl)


def _relay_stop(t: dict, pl: dict, reason: str, now: int) -> None:
    """Stop a chain at a bound (caller holds task_lock and saves t and pl):
    the user is told honestly where it got to, what is left and how to go
    on; done steps and their files stay for 「继续」. The message goes out
    like any reply (waiting_send)."""
    steps = pl.get("steps") or []
    done = [s for s in steps if s.get("status") == "done"]
    todo = [s for s in steps if s.get("status") != "done"]
    pl.update(state="stopped", stopped_reason=reason, stopped_at=now, user=t.get("from_user_id"))
    try:
        wd = str(_plan_workdir(t["message_id"]).relative_to(BASE_DIR))
    except ValueError:
        wd = str(_plan_workdir(t["message_id"]))
    lines = [f"「{_asked(t)}」这次没能做完，先停在这里：{reason}。",
             f"做到第 {len(done)}/{len(steps)} 步"
             + ("，已完成：" + "；".join(str(s.get("do")) for s in done[-6:]) if done else "") + "。"]
    if todo:
        lines.append("还剩：" + "；".join(str(s.get("do")) for s in todo[:6])
                     + ("……" if len(todo) > 6 else "") + "。")
    lines.append(f"已做完的步骤和中间产物都留着（{wd}），不会重做。要接着做就回我「继续」，"
                 "从没做完的那一步接着来。")
    t["result_text"] = "\n".join(lines)
    ex = dict(t.get("execution") or {})
    ex.update(finished_at=now, error=f"relay stopped: {reason}", by="bridge")
    t["execution"] = ex
    t["status"] = "waiting_send"
    t.pop("relay_pending", None)


def _relay_handoff_note(t: dict, pl: dict, now: int) -> str | None:
    """At a hand-off, at most every RELAY_NOTIFY_GAP_MIN minutes (0 = never):
    one line for the user - how far, what is next, how long so far."""
    if not RELAY_NOTIFY_GAP_MIN:
        return None
    rl = t.setdefault("relay", {})
    last = int(rl.get("notified_at") or rl.get("started_at") or 0)
    if last and now - last < RELAY_NOTIFY_GAP_MIN * 60_000:
        return None
    steps = pl.get("steps") or []
    done = sorted((s for s in steps if s.get("status") == "done"),
                  key=lambda s: int(s.get("done_at") or 0))
    todo = [s for s in steps if s.get("status") != "done"]
    if not todo:
        return None
    rl["notified_at"] = now
    rest = max(1, round(sum(float(s.get("est_min") or 0) for s in todo)))
    return (f"「{_asked(t)}」进度：{len(done)}/{len(steps)} 步做完"
            + (f"（刚做完：{done[-1].get('do')}）" if done else "")
            + f"，接着做「{todo[0].get('do')}」；已用 "
            f"{fmt_duration(now - int(t.get('received_at') or now))}，预计还要约 {rest} 分钟。"
            "中途不用回我，做完发完整结果。")


def _relay_send_notice(user, ctx_tok, task_id: str, text: str) -> dict:
    """A relay progress note under the rules of progress --notify: 设置 进度关
    silences it, and the last reply slots stay for the final answer."""
    if not settings_get().get("progress_notes", True):
        return {"ok": False, "skipped": "progress notes turned off by 设置 进度关"}
    left = choose_context_token(user, ctx_tok)[1]
    if left is not None and left <= QUOTA_RESERVE["progress"] + 1:
        return {"ok": False, "skipped": "only the last reply slots of this message are left: "
                                        "kept for the final answer"}
    nid = create_notice(user, render_for_wechat(text), reason="progress",
                        about={"task_id": task_id})
    return deliver(nid) if nid else {"ok": False, "error": "notice suppressed"}


def _relay_reclaim(t: dict, ex: dict, pl: dict, n: int, now: int) -> bool:
    """_reclaim_stale for a planned task (caller holds task_lock; `ex` is the
    copy with the claim already revoked). The silent leg most likely hit the
    600 s turn limit. A leg that finished a step made progress, so the
    no-progress counter (MAX_RECLAIMS) restarts; the chain itself stays
    bounded by the relay limits. True when the user is told it stopped."""
    rl = t.setdefault("relay", {})
    leg = len(rl.get("legs") or []) + 1
    did = any(s.get("status") == "done" and int(s.get("leg") or 0) == leg
              for s in pl.get("steps") or [])
    rec = _relay_leg_end(t, pl, "reclaimed", now)
    elapsed = now - int(rl.get("started_at") or now)
    reason = None
    if leg >= RELAY_MAX_LEGS:
        reason = f"已经接力了 {leg} 棒（上限 {RELAY_MAX_LEGS} 棒）"
    elif elapsed >= RELAY_MAX_MIN * 60_000:
        reason = f"已经用了 {fmt_duration(elapsed)}（上限 {RELAY_MAX_MIN} 分钟）"
    elif not did and n >= MAX_RECLAIMS:
        reason = f"有一步连续 {n + 1} 次做到一半就断了，可能这一步太大"
    t["execution"] = ex
    if reason:
        rec["stopped"] = True
        _relay_stop(t, pl, reason, now)
        save_task(t)
        plan_save(pl)
        log(f"relay task id={t['message_id']} stopped at leg {leg}: {reason}")
        return True
    ex.update(reclaimed_at=now, reclaim_reason="stale_executing", reclaims=0 if did else n + 1,
              checkpoint={"stage": ex.get("stage", ""), "note": ex.get("note", ""),
                          "stage_at": ex.get("stage_at", 0)}, checkpoint_at=now)
    t.update(status="pending_execution", pending_at=now, relay_pending=True,
             wake_key=f"{t['message_id']}~r{leg + 1}")
    save_task(t)
    log(f"relay task id={t['message_id']}: leg {leg} went silent; leg {leg + 1} queued")
    return False


def _resume_plan_hint(t: dict) -> str | None:
    """「继续」 / 「接着做」 after the bridge stopped a chain: point the executor
    at the user's newest stopped plan (24 h) - adopted, not redone."""
    if t.get("kind") != "text" or not _RESUME_INTENT.match((t.get("text") or "").strip()):
        return None
    if not PLAN_DIR.is_dir():
        return None
    user, now, best = t.get("from_user_id"), now_ms(), None
    for f in PLAN_DIR.glob("*.json"):
        pl = read_json(f, None)
        if not isinstance(pl, dict) or pl.get("state") != "stopped" or pl.get("user") != user:
            continue
        at = int(pl.get("stopped_at") or 0)
        if now - at <= 86_400_000 and (best is None or at > int(best.get("stopped_at") or 0)):
            best = pl
    if not best:
        return None
    steps = best.get("steps") or []
    todo = [s for s in steps if s.get("status") != "done"]
    return (f"用户多半是要接着做上次停下的长任务「{best.get('goal') or best.get('task_id')}」（停在第 "
            f"{len(steps) - len(todo)}/{len(steps)} 步：{best.get('stopped_reason') or ''}）。是的话先 "
            f"`progress --task-id {t['message_id']} --claim-id <claim_id> --plan-from "
            f"{best.get('task_id')} --notify` 接过计划（已做完的步骤和产物都保留），再按 plan 从 "
            f"{todo[0].get('step_id') if todo else '最后一步'} 接着做；不是这件事就忽略这条。")


def _complete_continue(args) -> int:
    """`complete --continue` (2026-11-07 relay): this leg is over. Marks
    --step-done, then hands the rest of the plan to a fresh executor turn:
    the task goes back to pending_execution with a new wake_key (the hook
    wakes a new executor at its next poll; a warm then-wait claim leaves it
    alone) and the next claim carries the whole plan. Sends nothing except,
    at most every RELAY_NOTIFY_GAP_MIN minutes, a one-line progress note.
    The other messages of the same claim are answered by the chain's final
    reply (closed as merged, as with --merge-ids). At a bound - legs,
    minutes, two legs in a row without a finished step - the chain stops and
    the bridge tells the user where it got to instead."""
    def emit(obj: dict, rc: int) -> int:
        print(json.dumps(obj, ensure_ascii=False))
        return rc
    claim_id = getattr(args, "claim_id", None)
    if not RELAY_ON:
        return emit({"ok": False, "error": "relay is off (WECHAT_BRIDGE_RELAY=0): finish "
                     "with complete --result or --error"}, 2)
    if getattr(args, "result", None) or getattr(args, "result_file", None) \
            or getattr(args, "attach", None) or getattr(args, "error", None):
        return emit({"ok": False, "error": "--continue sends nothing (no --result / "
                     "--result-file / --attach / --error): the one complete reply goes out "
                     "with the last leg's complete --result"}, 2)
    now = now_ms()
    stopped, note_text, merged, leg, todo = None, None, [], 0, []
    with task_lock():
        t = load_task(args.task_id)
        if not t:
            return emit({"ok": False, "error": "task not found"}, 2)
        if t.get("status") == "cancelled":
            return emit({"ok": False, "cancelled": True, "error": "the user cancelled this "
                         "task (「取消」): stop now; nothing is sent"}, 3)
        gone = _superseded_why(t, claim_id)
        if gone:
            return emit(_superseded_out(t, gone), EXIT_SUPERSEDED)
        why = _readopt_or_stale(t, claim_id, now)
        if why:
            return emit({"ok": False, "stale_claim": True, "error": why}, 3)
        if t.get("status") != "executing" or t.get("batch_open"):
            return emit({"ok": False, "error": f"task status is {t.get('status')}: "
                         "--continue hands off a task you are executing"}, 2)
        pl = plan_load(args.task_id)
        if not pl or pl.get("state", "active") != "active":
            return emit({"ok": False, "error": "no active plan for this task: write one "
                         "first (progress --plan FILE), or finish with complete --result"}, 2)
        ex = dict(t.get("execution") or {})
        ex["heartbeat_at"] = now
        _note_call(ex, now)
        stage = (getattr(args, "stage", None) or "").strip()
        if stage:
            ex["stage"], ex["stage_at"] = stage[:200], now
        err = _relay_mark(t, pl, now, getattr(args, "step_done", None),
                          list(getattr(args, "artifact", None) or []))
        if err:
            return emit({"ok": False, "error": err}, 2)
        t["execution"] = ex
        todo = [s for s in pl["steps"] if s.get("status") != "done"]
        if not todo:
            save_task(t)
            plan_save(pl)
            return emit({"ok": False, "all_done": True, "error": "every step of the plan is "
                         "done: send the one complete reply now with complete --result "
                         "(not --continue)"}, 2)
        rl = t.setdefault("relay", {})
        rl.setdefault("started_at", int(ex.get("claimed_at") or now))
        leg = len(rl.get("legs") or []) + 1
        did = [s.get("step_id") for s in pl["steps"]
               if s.get("status") == "done" and int(s.get("leg") or 0) == leg]
        rl["stalls"] = 0 if (did or leg == 1) else int(rl.get("stalls") or 0) + 1
        rec = _relay_leg_end(t, pl, "continue", now)
        elapsed = now - int(rl["started_at"])
        if leg >= RELAY_MAX_LEGS:
            stopped = f"已经接力了 {leg} 棒（上限 {RELAY_MAX_LEGS} 棒）"
        elif elapsed >= RELAY_MAX_MIN * 60_000:
            stopped = f"已经用了 {fmt_duration(elapsed)}（上限 {RELAY_MAX_MIN} 分钟）"
        elif rl["stalls"] >= 2:
            stopped = "连续两棒都没做完一步"
        if stopped:
            rec["stopped"] = True
            _relay_stop(t, pl, stopped, now)
        else:
            ex = dict(t["execution"])
            ex.pop("claim_id", None)
            ex.update(handed_off_at=now, checkpoint_at=now,
                      checkpoint={"stage": ex.get("stage", ""), "note": ex.get("note", ""),
                                  "stage_at": ex.get("stage_at", 0)})
            t["execution"] = ex
            t.update(status="pending_execution", pending_at=now, relay_pending=True,
                     wake_key=f"{t['message_id']}~r{leg + 1}")
            note_text = _relay_handoff_note(t, pl, now)
        save_task(t)
        plan_save(pl)
        group = [s_["message_id"] for s_ in scan_task_index()
                 if claim_id and s_.get("claim_id") == claim_id
                 and s_.get("status") == "executing" and s_["message_id"] != args.task_id]
        extra = [x for x in re.split(r"[,\s]+", getattr(args, "merge_ids", None) or "")
                 if x and x != args.task_id]
        if group or extra:
            merged = _merge_tasks(args.task_id, t, list(dict.fromkeys(group + extra)), claim_id)
        user, ctx_tok = t.get("from_user_id"), t.get("context_token")
        log(f"relay task id={args.task_id}: leg {leg} "
            + (f"stopped ({stopped})" if stopped else f"handed off, {len(todo)} step(s) left"))
    if not stopped:
        _signal_inbox()     # v1.4: a lingering inbox hook wakes the next leg at once
    if stopped:
        res = {"ok": True, "task_id": args.task_id, "continued": False, "stopped": True,
               "reason": stopped, "send": deliver(args.task_id),
               "note": "接力到了上限：桥已把做到哪一步、还剩什么、怎么继续发给用户。结束本回合，"
                       "不要再发。"}
    else:
        res = {"ok": True, "task_id": args.task_id, "continued": True, "leg": leg,
               "next_leg": leg + 1, "steps_left": [s.get("step_id") for s in todo],
               "note": f"已交棒：现在就结束本回合（不加 --then-wait，不要再 claim / progress / "
                       f"complete 这条）；桥会唤醒新一棒，从 {todo[0].get('step_id')} 接着做。"}
        if note_text:
            res["notice"] = _relay_send_notice(user, ctx_tok, args.task_id, note_text)
    if merged:
        res["merged"] = merged
    if float(getattr(args, "then_wait", 0) or 0) > 0:
        res["then_wait_ignored"] = ("after --continue the turn ends: the next leg gets a "
                                    "fresh 600 s turn")
    return emit(res, 0)


# --------------------------------------------------------------------------
# 2026-11-07 §8 (smarter / faster). Each item has its own switch in
# state/tuning.env, so each can be rolled back alone:
#   WECHAT_BRIDGE_PEEK=0       no file_peek in claims              (8.2.4)
#   WECHAT_BRIDGE_CMD_STATS=0  「统计」 goes to Muse like any text   (8.2.1)
#   WECHAT_BRIDGE_CLI_LOG=0    no logs/cli_calls.log               (8.3.3)
# --------------------------------------------------------------------------
PEEK_ON = os.environ.get("WECHAT_BRIDGE_PEEK", "1").strip() != "0"
PEEK_CHARS = max(200, min(4000, _env_int("WECHAT_BRIDGE_PEEK_CHARS", 1500)))
PEEK_LINES = 40
PEEK_MAX_FILES = 2
PEEK_COUNT_MAX_BYTES = 4 * 1024 * 1024
_PEEK_EXT = ("md|txt|py|sh|json|jsonl|yaml|yml|toml|ini|cfg|conf|service|log|csv|tsv|"
             "html|js|ts|css|sql|patch|diff")
_PEEK_PATH_RE = re.compile(r"(?<![A-Za-z0-9_./~-])((?:~|\.{1,2})?/?(?:[A-Za-z0-9_.-]+/)*"
                           r"[A-Za-z0-9_-][A-Za-z0-9_.-]*\.(?:" + _PEEK_EXT + r"))(?![A-Za-z0-9_])")
_PEEK_BARE_RE = re.compile(r"(?<![A-Za-z0-9_./-])(README|CHANGELOG|TUNING|EXECUTOR|MANIFEST)"
                           r"(?![A-Za-z0-9_.-])", re.I)
_PEEK_BARE = {"README": "README.md", "CHANGELOG": "CHANGELOG.md", "TUNING": "TUNING.md",
              "EXECUTOR": "EXECUTOR.md", "MANIFEST": "MANIFEST.txt"}
_PEEK_DENY_DIRS = {"state", "inbox", "inbox_media", "inbox_files", "outbox", "logs", "history",
                   "backups", "icloud-upload", ".git", ".ssh", ".gnupg", "__pycache__", "vendor"}
_PEEK_DENY_NAME = re.compile(r"(?i)credential|secret|token|passw|proxy|\.env$|\.pem$|\.key$|"
                             r"\.p12$|^id_[a-z0-9]+|\.sqlite$|\.db$")
CMD_STATS_ON = os.environ.get("WECHAT_BRIDGE_CMD_STATS", "1").strip() != "0"
CLI_LOG_ON = os.environ.get("WECHAT_BRIDGE_CLI_LOG", "1").strip() != "0"
CLI_LOG_FILE = LOG_DIR / "cli_calls.log"
_CLI_NOLOG = ("serve", "login", "wait-login")
_WARM_WINDOWS = (45, 60, 90, 120, 180, 300)


def _peek_roots() -> list:
    roots = [BASE_DIR.resolve()]
    for r in os.environ.get("WECHAT_BRIDGE_PEEK_ROOTS", "").split(":"):
        if r.strip():
            with contextlib.suppress(OSError, RuntimeError):
                roots.append(Path(r.strip()).expanduser().resolve())
    return roots


def _peek_one(raw: str) -> dict | None:
    try:
        p = Path(raw).expanduser()
        p = (p if p.is_absolute() else BASE_DIR / p).resolve()
    except (OSError, RuntimeError, ValueError):
        return None
    root = next((r for r in _peek_roots() if r == p or r in p.parents), None)
    if root is None:
        return None
    rel = p.relative_to(root)
    if any(x in _PEEK_DENY_DIRS for x in rel.parts[:-1]) or _PEEK_DENY_NAME.search(p.name):
        return None
    try:
        if not p.is_file():
            return None
        size = p.stat().st_size
        with open(p, "rb") as f:
            head = f.read(PEEK_CHARS * 4 + 4)
    except OSError:
        return None
    if b"\x00" in head:
        return None                       # binary
    shown = "\n".join(head.decode("utf-8", errors="ignore").splitlines()[:PEEK_LINES])[:PEEK_CHARS]
    if _SECRETISH.search(shown):
        return None
    out = {"path": str(rel) if root == BASE_DIR.resolve() else str(p), "bytes": size,
           "head": shown, "truncated": len(shown.encode("utf-8")) < size}
    if size <= PEEK_COUNT_MAX_BYTES:
        with contextlib.suppress(OSError):
            with open(p, "rb") as f:
                out["lines"] = sum(c.count(b"\n") for c in iter(lambda: f.read(1 << 16), b""))
    return out


def file_peeks(t: dict) -> list:
    """8.2.4: files the message names - a path, a file name with an extension,
    or the bridge's README / CHANGELOG / TUNING / EXECUTOR / MANIFEST - with
    the start of each (read-only snapshot), so 「读一下 README 开头」 needs no
    read call. Only regular text files under the bridge directory (plus
    WECHAT_BRIDGE_PEEK_ROOTS); never state/, inbox*, outbox/, logs/,
    history/, backups/, .git, credentials / key / env files, nothing
    secret-looking; at most 2 files, 40 lines / PEEK_CHARS characters each."""
    if not PEEK_ON or t.get("kind") in ("command", "notice"):
        return []
    text = t.get("text") or ""
    if not text or len(text) > 4000:
        return []                         # a pasted document, not a request to read
    cands = [m.group(1) for m in _PEEK_PATH_RE.finditer(text)]
    cands += [_PEEK_BARE[m.group(1).upper()] for m in _PEEK_BARE_RE.finditer(text)]
    out, seen = [], set()
    for c in cands:
        if len(out) >= PEEK_MAX_FILES:
            break
        pk = _peek_one(c)
        if pk and pk["path"] not in seen:
            seen.add(pk["path"])
            out.append(pk)
    return out


def _warm_wait_whatif(rows: list, gaps: list) -> dict:
    """8.3.1: the data to decide the warm wait on - per window W, the share of
    follow-ups a warm executor would catch, and what it costs: the seconds a
    warm executor sits waiting per reply (the gap, or all of W when no
    follow-up came within W)."""
    waits = []
    for i, a in enumerate(rows):
        if a.get("role") != "bot":
            continue
        b = rows[i + 1] if i + 1 < len(rows) else None
        if b is not None and b.get("role") == "bot":
            continue                      # another reply follows: nobody waits here
        g = int(b.get("at") or 0) - int(a.get("at") or 0) if b is not None else -1
        waits.append(g if 0 <= g <= 86_400_000 else None)
    n = len(gaps)
    return {"caught": {f"{s}s": (round(sum(1 for g in gaps if g <= s * 1000) / n, 3) if n else None)
                       for s in _WARM_WINDOWS},
            "wait_per_reply_s": {f"{s}s": (round(sum(min(w, s * 1000) if w is not None else s * 1000
                                                     for w in waits) / len(waits) / 1000, 1)
                                           if waits else None) for s in _WARM_WINDOWS},
            "replies": len(waits)}


def stats_command() -> str:
    """「统计」 (8.2.1): numbers `stats` already has, copied as they are - no
    model, no judgement: latency p50 / p95 per stage over 72 h, warm vs cold
    claims, and the follow-up gaps behind the warm wait."""
    st = latency_stats(hours=72)
    if not st.get("n"):
        return "最近 72 小时还没有答完的消息，暂无统计。"

    def f(ms):
        if ms is None:
            return "—"
        return f"{int(ms) / 1000:.1f} 秒" if int(ms) < 100_000 else fmt_duration(int(ms))
    lines = [f"最近 72 小时 {st['n']} 条（数字原样来自 stats，中位 / 95 分位）："]
    for label, k in (("合计", "total"), ("接手", "wake"), ("　其中进队", "wake_queue"),
                     ("　其中等接手", "wake_wait"), ("处理", "exec"), ("送达", "deliver")):
        v = st.get(k) or {}
        if v.get("p50") is not None:
            lines.append(f"· {label}：{f(v['p50'])} / {f(v.get('p95'))}")
    eb = exec_breakdown(hours=72)
    if eb.get("warm_claims") or eb.get("cold_claims"):
        lines.append(f"· 接手方式：热 {eb.get('warm_claims', 0)} 条、冷 {eb.get('cold_claims', 0)} 条")
    fg = followup_gaps()
    if fg.get("n"):
        w = max(5, min(CLAIM_WAIT_MAX_SEC, _env_int("WECHAT_BRIDGE_WARM_WAIT_S", 45)))
        share = ((fg.get("whatif") or {}).get("caught") or {}).get(f"{w}s")
        lines.append(f"· 追问间隔（30 天 {fg['n']} 次）：中位 {f(fg.get('p50_ms'))}"
                     + (f"，{w} 秒热等待接得住 {share:.0%}" if share is not None else ""))
    return "\n".join(lines)


def _cli_log(cmd, task_id, t0: float, rc) -> None:
    """8.3.3: one line per bridge CLI call - start (epoch ms), command, task
    id, duration ms, exit code; never arguments or text - so `stats
    --task-id` shows what one message really cost in bridge calls."""
    if not CLI_LOG_ON or cmd in _CLI_NOLOG:
        return
    try:
        ms = int((time.monotonic() - t0) * 1000)
        tid = task_id if isinstance(task_id, str) and task_id else "-"
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            if CLI_LOG_FILE.stat().st_size > 2 * 1024 * 1024:
                os.replace(CLI_LOG_FILE, LOG_DIR / "cli_calls.log.1")
        fd = os.open(CLI_LOG_FILE, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, f"{now_ms() - ms}\t{cmd}\t{tid}\t{ms}\t{rc}\n".encode())
        finally:
            os.close(fd)
    except OSError:
        pass


def cli_calls_between(a: int, z: int, task_id: str | None = None) -> list:
    """Bridge CLI calls that started in [a, z] (logs/cli_calls.log*): those
    tagged with task_id plus untagged ones (claim, history, stats ... - in a
    busy minute they may belong to another executor)."""
    rows = []
    for f in (LOG_DIR / "cli_calls.log.1", CLI_LOG_FILE):
        try:
            lines = f.read_text(encoding="utf-8", errors="replace").splitlines()[-20000:]
        except OSError:
            continue
        for ln in lines:
            p = ln.split("\t")
            if len(p) < 5 or not p[0].isdigit():
                continue
            at = int(p[0])
            if a <= at <= z and p[2] in ("-", task_id):
                rows.append({"at": at, "cmd": p[1], "tagged": p[2] != "-",
                             "ms": int(p[3]) if p[3].isdigit() else None})
    return sorted(rows, key=lambda r: r["at"])



# --------------------------------------------------------------------------
# 2026-11-07 (v1.4) 后台长任务 (background): queue -> watch hook -> main agent
# + sub-agent -> the task's own reply path
# --------------------------------------------------------------------------
# A long task no longer takes an executor turn. While the watch hook
# (hooks/wechat-long-task-watch.sh, registered by Muse: poll 10 s, delivery
# main) is alive - it touches state/long-tasks/.watch_tick every run - a
# dispatched batch that the long_hint rules call long is routed here by the
# bridge itself (_batch_dispatch, under task_lock, before the inbox hook can
# see it):
#   1. queue entry state/long-tasks/queue/<id>.json written and read back
#      FIRST (0600 in 0700 dirs; credentials-like tasks are never queued),
#   2. the task becomes status "background": not claimable, not superseded,
#      not reclaimed - 「进度」 and 「取消」 still see it,
#   3. one bridge-written signal to the user.
# Batches still downloading media (or batching off): the executor gets a
# long_hint telling it to call `complete --background` - the same 3 steps.
# The hook wakes the main agent; it runs `long-tasks take`, spawns a
# sub-agent, and answers with `long-tasks finish`: the task's normal reply
# path (chunks, attachments, retries, exactly once) instead of send-direct.
# Safety nets: untaken for BG_TAKE_MIN -> back to the foreground (v1.3
# relay); taken but silent for BG_STALE_MIN -> requeued (BG_MAX_TAKES takes),
# then an honest failure message; BG_MAX_MIN in all -> honest timeout.
# WECHAT_BRIDGE_BACKGROUND=0 turns it off (long tasks use the v1.3 relay).
BG_ON = os.environ.get("WECHAT_BRIDGE_BACKGROUND", "1").strip() != "0"
BG_DIR = STATE_DIR / "long-tasks"
BG_QUEUE = BG_DIR / "queue"
BG_TICK = BG_DIR / ".watch_tick"
BG_WATCH_LOG = BG_DIR / "watch.log"
BG_WATCH_FRESH_S = max(20, _env_int("WECHAT_BRIDGE_BG_WATCH_FRESH_S", 120))
BG_TAKE_MIN = max(2, _env_int("WECHAT_BRIDGE_BG_TAKE_MIN", 10))
BG_REWAKE_MIN = max(1, _env_int("WECHAT_BRIDGE_BG_REWAKE_MIN", 3))
BG_STALE_MIN = max(5, _env_int("WECHAT_BRIDGE_BG_STALE_MIN", 30))
BG_MAX_MIN = max(30, _env_int("WECHAT_BRIDGE_BG_MAX_MIN", 240))
BG_KEEP_DAYS = max(1, _env_int("WECHAT_BRIDGE_BG_KEEP_DAYS", 14))
BG_MAX_TAKES = 3
BG_MAX_WAKES = 3
BG_NOTIFY_GAP_MIN = 10
_BG_LIVE = ("queued", "running")


def bg_available() -> bool:
    """Route long tasks to the background only while its consumer is there:
    the watch hook touched BG_TICK within BG_WATCH_FRESH_S. Not registered
    yet, disabled or broken -> False -> the v1.3 relay, nothing gets stuck."""
    if not BG_ON:
        return False
    try:
        return time.time() - BG_TICK.stat().st_mtime <= BG_WATCH_FRESH_S
    except OSError:
        return False


def _bg_qpath(mid) -> Path:
    return BG_QUEUE / (_plan_name(mid) + ".json")


def _bg_workdir(mid) -> Path:
    return BG_DIR / (_plan_name(mid) + ".d")


def bg_read(mid) -> dict | None:
    e = read_json(_bg_qpath(mid), None)
    return e if isinstance(e, dict) and e.get("message_id") is not None else None


_bg_pruned = [False]
BG_PRUNE_MARK = BG_DIR / ".pruned"


def _bg_prune_due() -> bool:
    """v1.7: the 14-day sweep of the queue runs at most once an hour per host
    (a marker file), not in every short-lived CLI process that writes an
    entry - each long-tasks call used to re-read the whole queue for it."""
    try:
        if time.time() - BG_PRUNE_MARK.stat().st_mtime < 3600:
            return False
    except OSError:
        pass
    with contextlib.suppress(OSError):
        BG_PRUNE_MARK.touch()
    return True


def _bg_write(e: dict) -> bool:
    """Write a queue entry (0600, dirs 0700; indent=1 so the hook can grep
    `"state": "queued"`) and read it back. Prunes finished entries and their
    work dirs after BG_KEEP_DAYS (once per process)."""
    try:
        for d in (BG_DIR, BG_QUEUE):
            d.mkdir(parents=True, exist_ok=True)
            os.chmod(d, 0o700)
        e["updated_at"] = now_ms()
        atomic_write_text(_bg_qpath(e["message_id"]),
                          json.dumps(e, ensure_ascii=False, indent=1) + "\n", mode=0o600)
    except OSError as ex:
        log(f"background: queue write failed ({type(ex).__name__})")
        return False
    back = bg_read(e["message_id"])
    if not _bg_pruned[0] and _bg_prune_due():
        _bg_pruned[0] = True
        cut = now_ms() - BG_KEEP_DAYS * 86_400_000
        for p in list(BG_QUEUE.glob("*.json")):
            old = read_json(p, None)
            if isinstance(old, dict) and old.get("state") not in _BG_LIVE \
                    and int(old.get("updated_at") or 0) < cut:
                with contextlib.suppress(OSError):
                    p.unlink()
                shutil.rmtree(BG_DIR / (p.stem + ".d"), ignore_errors=True)
    return bool(back) and back.get("state") == e.get("state") \
        and str(back.get("message_id")) == str(e["message_id"])


def _bg_files(t: dict) -> list:
    """Files the task carries or names - paths and sizes only."""
    out = []
    if t.get("media_path") and t.get("kind") in ("file", "image", "video", "voice"):
        out.append({"path": str(t["media_path"]), "name": t.get("file_name"),
                    "bytes": t.get("media_bytes")})
    with contextlib.suppress(Exception):
        out += [{"path": pk["path"], "bytes": pk["bytes"]} for pk in file_peeks(t)]
    return out


def _bg_handoff(lead: dict, members: list, route: str, why: list, now: int) -> tuple:
    """The hand-off (caller holds task_lock): the queue entry is written and
    read back FIRST; only then does the task leave the executor system
    (status background) and the signal get queued. (notice id, None) or
    (None, why not) - on any refusal nothing has changed."""
    texts = [{"message_id": t["message_id"], "kind": t.get("kind"), "text": t.get("text") or "",
              "at": int(t.get("received_at") or now)} for t in members + [lead]]
    joined = "\n".join(x["text"] for x in texts if x["text"])
    if _SECRETISH.search(joined):
        return None, ("the task names credentials or a secret-looking value: not queued - "
                      "keep it in the foreground")
    files = []
    for t in members + [lead]:
        files += _bg_files(t)
    e = {"v": 1, "message_id": lead["message_id"], "message_ids": [x["message_id"] for x in texts],
         "user": lead.get("from_user_id"), "text": joined[:8000], "texts": texts[-10:],
         "long_hint": list(why)[:6], "received_at": min(x["at"] for x in texts),
         "files": files[:10], "state": "queued", "queued_at": now, "route": route,
         "takes": 0, "wakes": 0, "workdir": str(_bg_workdir(lead["message_id"]))}
    if not _bg_write(e):
        return None, "the queue entry could not be written and read back: not handed off"
    bg = {"state": "queued", "queued_at": now, "route": route, "takes": 0}
    ex = dict(lead.get("execution") or {})
    if ex.get("claim_id"):
        bg["from_claim"] = ex.pop("claim_id")
    ex["handed_to_background_at"] = now
    lead.update(status="background", bg=bg, execution=ex)
    lead.pop("relay_pending", None)
    save_task(lead)
    if members:
        _merge_tasks(lead["message_id"], lead, [t["message_id"] for t in members], bg.get("from_claim"))
    ahead = max(0, bg_position(lead["message_id"]) - 1)        # v1.5: the lane
    nid = create_notice(lead.get("from_user_id"), render_for_wechat(
        f"「{_asked(lead)}」是个长任务，已转到后台处理（不占前台，你照常发消息就行），做完把完整"
        "结果发你。" + (f"前面还有 {ahead} 个后台任务，轮到它就开始。" if ahead else "")
        + "想看进度回「进度」，要补充要求就引用这条回复，要停回「取消」。"),
        reason="progress", about={"task_id": lead["message_id"]}, essential=True)
    _bg_note_notice(e, nid)    # v1.5/v1.6: a quote of this notice routes to the task
    if not nid:                # v1.6: notices off / window closed - recorded, take says so
        e["signal"] = "suppressed"
        _bg_write(e)
        log(f"background: no signal could be queued for task id={lead['message_id']}")
    log(f"background: task id={lead['message_id']} queued by the {route} "
        f"({len(members)} message(s) merged into it)")
    return nid, None


def _bg_route_batch(ids: list, now: int) -> list:
    """_batch_dispatch (caller holds task_lock): a dispatched batch the
    long_hint rules call long goes to the background whole - one request,
    one final reply - while the watch hook is alive. Batches with media
    still downloading, relay legs and tasks that came back from the
    background stay on the executor path."""
    if not ids or not bg_available():
        return []
    ts = [load_task(i) for i in ids]
    if any(not t or t.get("status") != "pending_execution" or t.get("relay") or t.get("bg")
           or t.get("kind") in ("command", "notice") for t in ts):
        return []
    why: list = []
    for t in ts:
        why += [w for w in long_hint_rules(t) if w not in why]
    if not why:
        return []
    nid, err = _bg_handoff(ts[-1], ts[:-1], "bridge", why, now)
    if err:
        log(f"background: batch not routed ({err})")
        return []
    return list(ids)


def _complete_background(args) -> int:
    """`complete --background` (executor): the same hand-off, for a long task
    that reached an executor (media batch, batching off, or the rules missed
    it). The bridge sends the signal; the executor ends its turn."""
    def emit(obj: dict, rc: int) -> int:
        print(json.dumps(obj, ensure_ascii=False))
        return rc
    claim_id = getattr(args, "claim_id", None)
    if not bg_available():
        return emit({"ok": False, "error": "background not available (WECHAT_BRIDGE_BACKGROUND=0 "
                     "or the watch hook is not running): do it in the foreground - for long work "
                     "the relay (progress --plan ... complete --continue)"}, 2)
    if getattr(args, "result", None) or getattr(args, "result_file", None) \
            or getattr(args, "attach", None) or getattr(args, "error", None) \
            or getattr(args, "cont", False):
        return emit({"ok": False, "error": "--background sends nothing itself (the bridge sends "
                     "the signal): no --result / --attach / --error / --continue"}, 2)
    now = now_ms()
    with task_lock():
        t = load_task(args.task_id)
        if not t:
            return emit({"ok": False, "error": "task not found"}, 2)
        if t.get("status") == "cancelled":
            return emit({"ok": False, "cancelled": True, "error": "the user cancelled this task "
                         "(「取消」): stop now; nothing is sent"}, 3)
        gone = _superseded_why(t, claim_id)
        if gone:
            return emit(_superseded_out(t, gone), EXIT_SUPERSEDED)
        why = _readopt_or_stale(t, claim_id, now)
        if why:
            return emit({"ok": False, "stale_claim": True, "error": why}, 3)
        if t.get("status") != "executing" or t.get("batch_open"):
            return emit({"ok": False, "error": f"task status is {t.get('status')}: --background "
                         "hands off a task you are executing"}, 2)
        if (t.get("bg") or {}).get("state") == "returned":
            # v1.7: nobody took it in time once - queueing it again would only
            # repeat that (queue -> back -> queue ...)
            return emit({"ok": False, "error": "this task already came back from the background "
                         "(nobody took it in time): do it in the foreground - for long work the "
                         "relay (progress --plan ... complete --continue)"}, 2)
        ids = [s_["message_id"] for s_ in scan_task_index()
               if claim_id and s_.get("claim_id") == claim_id and s_.get("status") == "executing"
               and s_["message_id"] != args.task_id]
        ids += [x for x in re.split(r"[,\s]+", getattr(args, "merge_ids", None) or "")
                if x and x != args.task_id and x not in ids]
        members = [m for m in (load_task(i) for i in ids)
                   if m and m.get("status") in ("executing", "pending_execution")]
        nid, err = _bg_handoff(t, members, "executor", long_hint_rules(t) or ["执行者判长"], now)
        if err:
            return emit({"ok": False, "error": err}, 2)
    res = {"ok": True, "task_id": args.task_id, "background": True,
           "notice": deliver(nid) if nid else {"ok": False, "error": "notice suppressed"},
           "note": "已交给后台：现在就结束本回合（不加 --then-wait，不要再 claim / progress / "
                   "complete 这条）。主代理 + 子代理做完会把完整结果发回微信。"}
    if float(getattr(args, "then_wait", 0) or 0) > 0:
        res["then_wait_ignored"] = "after --background the turn ends"
    return emit(res, 0)


def _bg_close(t: dict, state: str, now: int) -> None:
    """Final state of a background task (caller holds task_lock, saves t)."""
    bg = dict(t.get("bg") or {})
    bg.update(state=state, finished_at=now)
    t["bg"] = bg
    e = bg_read(t["message_id"])
    if e and e.get("state") in _BG_LIVE:
        e.update(state=state, finished_at=now)
        _bg_write(e)


def _bg_emit(obj: dict, rc: int) -> int:
    print(json.dumps(obj, ensure_ascii=False))
    return rc


def _bg_fail_now(t: dict, e: dict, why: str, now: int) -> None:
    """Honest end of a background task the bridge had to give up on (caller
    holds task_lock): what happened, how far it got, where the files are,
    and (v1.6) any follow-up the user added that was never handed over.
    Goes out like any reply (waiting_send)."""
    try:
        wd = str(Path(e.get("workdir") or _bg_workdir(t["message_id"])).relative_to(BASE_DIR))
    except ValueError:
        wd = str(e.get("workdir") or "")
    unread = _bg_notes_take_new(e)
    t["result_text"] = (f"「{_asked(t)}」后台没能做完：{why}。"
                        + (f"最后做到：{e['stage']}。" if e.get("stage") else "")
                        + (f"你后来补充的「{'；'.join(snippet(n['text'], 30) for n in unread[:3])}」也还没做。"
                           if unread else "")
                        + f"已做的中间产物都留着（{wd}）。要接着做就再发我一次，会先看已有的产物、不从头重做。")
    ex = dict(t.get("execution") or {})
    ex.update(finished_at=now, error=f"background failed: {why}", by="bridge")
    t.update(execution=ex, status="waiting_send")
    bg = dict(t.get("bg") or {})
    bg.update(state="failed", finished_at=now, why=why)
    t["bg"] = bg
    save_task(t)
    e.update(state="failed", finished_at=now, why=why, run_ms=_bg_worked_ms(e, now))
    _bg_write(e)
    log(f"background: task id={t['message_id']} failed ({why})")


def check_background(idx=None) -> int:
    """The background's safety nets (serve every 15 s; `long-tasks` too).
      * queued: only a task whose turn it is (the first BG_MAX_RUNNING -
        running in FIFO order) can count as "not taken", from the moment its
        turn came (v1.6 - v1.5 also sent back tasks that were only waiting
        behind the head); not taken within BG_TAKE_MIN -> back to the
        foreground (relay);
      * running: no `long-tasks progress` for BG_STALE_MIN -> requeued (at
        most BG_MAX_TAKES takes), then an honest failure; more than
        BG_MAX_MIN of WORK (from the take, earlier takes added; v1.6 - queue
        time is not work time) -> honest timeout.
    Every notice it sends is registered on the task (v1.6), so a quote of
    it routes back. Entries whose task was cancelled or left are closed."""
    del idx
    if not BG_QUEUE.is_dir():
        return 0
    now, acted, notes, back = now_ms(), 0, [], False
    with task_lock():
        ents = _bg_entries()
        lane = _bg_lane(ents)
        turn = {str(x) for x in lane["eligible"]}
        for x in [x for x in ents if x.get("state") in _BG_LIVE]:
            e = bg_read(x["message_id"])       # v1.7: fresh - the cache only picks the live ones
            if not isinstance(e, dict) or e.get("state") not in _BG_LIVE:
                continue
            mid = e.get("message_id")
            t = load_task(mid)
            if not t or t.get("status") != "background":
                e.update(state="cancelled" if (t or {}).get("status") == "cancelled" else "failed",
                         why="the task left the background", finished_at=now)
                _bg_write(e)
                acted += 1
                continue
            bg = dict(t.get("bg") or {})
            if e["state"] == "queued":
                if str(mid) not in turn:
                    continue                        # waiting its turn, not being ignored
                if now - _bg_turn_since(e, lane) <= BG_TAKE_MIN * 60_000:
                    continue
                e.update(state="returned", why=f"not taken within {BG_TAKE_MIN} min of its turn",
                         finished_at=now)
                _bg_write(e)
                bg.update(state="returned", returned_at=now)
                t.update(bg=bg, status="pending_execution", pending_at=now, wake_key=f"{mid}~bgr")
                if e.get("notes"):      # v1.7: the foreground answers the follow-ups too
                    t["bg_notes"] = [{"message_id": n.get("message_id"), "text": n.get("text")}
                                     for n in e["notes"] if n.get("text")][-BG_NOTES_MAX_UNREAD:]
                save_task(t)
                notes.append((t, e, f"「{_asked(t)}」后台暂时没人接手，改为前台分段处理，不用重发。"))
                acted += 1
                back = True
                log(f"background: task id={mid} not taken in {BG_TAKE_MIN} min of its turn - "
                    "back to the foreground")
                continue
            silent = now - int(e.get("heartbeat_at") or e.get("taken_at") or now)
            worked = _bg_worked_ms(e, now)
            if worked > BG_MAX_MIN * 60_000:
                _bg_fail_now(t, e, f"已经动手做了 {fmt_duration(worked)}，超过 {BG_MAX_MIN} 分钟的上限", now)
                acted += 1
            elif silent > BG_STALE_MIN * 60_000:
                if int(e.get("takes") or 0) >= BG_MAX_TAKES:
                    _bg_fail_now(t, e, f"后台接手了 {e.get('takes')} 次，每次都中途没了动静", now)
                else:
                    e.update(state="queued", token=None, woken_at=None, wakes=0, requeued_at=now,
                             run_ms=worked, why=f"no progress for {BG_STALE_MIN} min")
                    _bg_write(e)
                    bg.update(state="queued", token=None)
                    t["bg"] = bg
                    save_task(t)
                    log(f"background: task id={mid} silent {BG_STALE_MIN} min - requeued")
                    notes.append((t, e, f"「{_asked(t)}」后台处理中断了一下（{BG_STALE_MIN} 分钟没有动静），"
                                        "已重新排队，会从已做完的地方接着做，不用重发。"))
                acted += 1
        for t, e, text in notes:
            nid = create_notice(t.get("from_user_id"), text, reason="progress",
                                about={"task_id": t["message_id"]})
            _bg_note_notice(e, nid)
    if acted:
        _wake_serve()
    if back:
        _signal_inbox()
    return acted


def bg_overview() -> dict:
    """`long-tasks` / `long-tasks list`: the queue by state, newest first."""
    now = now_ms()
    groups: dict = {k: [] for k in ("queued", "running", "done", "failed", "cancelled", "returned")}
    if BG_QUEUE.is_dir():
        for p in sorted(BG_QUEUE.glob("*.json"), key=lambda q: q.stat().st_mtime, reverse=True):
            e = read_json(p, None)
            if not isinstance(e, dict):
                continue
            row = {"id": e.get("message_id"), "text": snippet(e.get("text") or "", 40),
                   "route": e.get("route"),
                   "age_min": round((now - int(e.get("queued_at") or now)) / 60000, 1)}
            for k in ("stage", "takes", "wakes", "why", "note_seq"):
                if e.get(k):
                    row[k] = e[k]
            if e.get("state") == "running":
                row["silent_min"] = round((now - int(e.get("heartbeat_at") or now)) / 60000, 1)
                row["worked_min"] = round(_bg_worked_ms(e, now) / 60000, 1)
            groups.setdefault(e.get("state") or "?", []).append(row)
    try:
        tick_age = round(time.time() - BG_TICK.stat().st_mtime, 1)
    except OSError:
        tick_age = None
    looks = bg_quote_lookups()
    return {"ok": True, "available": bg_available(), "watch_tick_age_s": tick_age,
            "lane": {"max_running": BG_MAX_RUNNING, "running": len(groups.get("running") or [])},
            "last_quote_lookup": looks[-1] if looks else None,
            "counts": {k: len(v) for k, v in groups.items()},
            **{k: v[:20] for k, v in groups.items()}}


def _bg_wake_check() -> int:
    """For the watch hook only: whom to (re)wake the main agent for. Walks
    the queue in FIFO order while slots are free (v1.6):
      * a task whose turn came after its last wake gets a fresh set of wakes
        (v1.5 kept the old count, so it could sit until the take timeout);
      * one woken within BG_REWAKE_MIN holds its slot - the main agent is on
        it, and waking another one would only spam it;
      * one whose wakes are used up (BG_MAX_WAKES) no longer blocks the
        slot: the next queued task is woken (no head-of-line blocking).
    Prints QUIET, or WAKE / one-line JSON payload / ids."""
    check_background()
    now, due = now_ms(), []
    with task_lock():
        lane = _bg_lane()
        turn = {str(x) for x in lane["eligible"]}
        slots = lane["free"]
        for mid in lane["queued"]:
            if slots <= 0:
                break
            e, t = bg_read(mid), load_task(mid)
            if not e or e.get("state") != "queued" or not t or t.get("status") != "background":
                continue
            w = int(e.get("woken_at") or 0)
            # (+5 s: timestamps come from different processes)
            if str(mid) in turn and w and w + 5_000 < _bg_turn_since(e, lane):
                e.update(woken_at=None, wakes=0)
                w = 0
            if w and now - w < BG_REWAKE_MIN * 60_000:
                slots -= 1
                continue
            if int(e.get("wakes") or 0) >= BG_MAX_WAKES:
                continue
            e.update(woken_at=now, wakes=int(e.get("wakes") or 0) + 1)
            if _bg_write(e):
                due.append(e)
                slots -= 1
    if not due:
        print("QUIET")
        return 0
    payload = {"reason": "wechat_long_task", "count": len(due),
               "tasks": [{"id": e["message_id"], "text": snippet(e.get("text") or "", 120),
                          "queued_min": round((now - int(e.get("queued_at") or now)) / 60000, 1),
                          "wakes": e["wakes"], "notes": int(e.get("note_seq") or 0)} for e in due],
               "manual": "~/workspace/wechat-bridge/BACKGROUND.md",
               "how": ("主代理：cd ~/workspace/wechat-bridge && python3 wb long-tasks take <id> → 生成"
                       "子代理在后台做 → python3 wb long-tasks finish <id> --token <token> "
                       "--result-file <结果>（只发这一条）")}
    print("WAKE")
    print(json.dumps(payload, ensure_ascii=False))
    print(" ".join(str(e["message_id"]) for e in due))
    return 0


def _bg_take(mid: str) -> int:
    now = now_ms()
    with task_lock():
        e, t = bg_read(mid), load_task(mid)
        if not e or not t:
            return _bg_emit({"ok": False, "error": "no such long task"}, 2)
        if t.get("status") == "cancelled":
            return _bg_emit({"ok": False, "cancelled": True,
                             "error": "cancelled by the user (「取消」): nothing to do"}, 3)
        if e.get("state") != "queued" or t.get("status") != "background":
            return _bg_emit({"ok": False, "error": f"not waiting to be taken (queue {e.get('state')}, "
                             f"task {t.get('status')}): someone else has it, or it is over - do nothing"}, 3)
        lane = _bg_lane()
        if lane["free"] <= 0:
            return _bg_emit({"ok": False, "busy": True, "error": f"{lane['running']} background "
                             f"task(s) already running (WECHAT_BRIDGE_BG_MAX_RUNNING={BG_MAX_RUNNING}): "
                             "leave this one queued - the hook wakes you again when a slot frees"}, 3)
        tok = "bg-" + secrets.token_hex(6)
        _bg_notes_norm(e)
        e.update(state="running", taken_at=now, heartbeat_at=now, token=tok,
                 takes=int(e.get("takes") or 0) + 1, notes_seen_seq=int(e.get("note_seq") or 0))
        if not _bg_write(e):
            return _bg_emit({"ok": False, "error": "queue entry could not be updated"}, 2)
        bg = dict(t.get("bg") or {})
        bg.update(state="running", taken_at=now, heartbeat_at=now, token=tok, takes=e["takes"])
        t["bg"] = bg
        save_task(t)
        wd = _bg_workdir(mid)
        wd.mkdir(parents=True, exist_ok=True)
        with contextlib.suppress(OSError):
            os.chmod(wd, 0o700)
    nxt = ("生成子代理在后台做（BACKGROUND.md）：每做完一步、至少每 10 分钟 `python3 wb long-tasks "
           f"progress {mid} --token {tok} --stage \"…\"`；子代理交回结果后 `python3 wb long-tasks finish "
           f"{mid} --token {tok} --result-file <结果>`，只发这一条。")
    if e["takes"] > 1:
        nxt += "这条之前有人接过、中途没了动静：先看 workdir 里已有的产物，接着做，不要从头重做。"
    if e.get("notes"):
        nxt += "notes 里是用户引用这条任务补充的要求：原样贴进子代理的说明，一起照做，最终结果里一并回应。"
    if e.get("signal") == "suppressed":
        nxt += "用户没收到“已转到后台”的信号（通知被拦）：最终结果里简单说一句这是后台做完的。"
    return _bg_emit({"ok": True, "id": mid, "token": tok, "text": e.get("text"),
                     "texts": e.get("texts"), "notes": e.get("notes") or [], "files": e.get("files"),
                     "long_hint": e.get("long_hint"), "signal_sent": e.get("signal") != "suppressed",
                     "workdir": str(wd), "takes": e["takes"], "heartbeat_every_min": 10,
                     "stale_after_min": BG_STALE_MIN, "max_work_min": BG_MAX_MIN, "next": nxt}, 0)


def _bg_act(act: str, mid: str, args) -> int:
    """progress / finish / fail for the current taker (its token is the fence).
    v1.6: follow-ups are handed over by sequence number - progress returns
    the new ones; finish refuses while any is unseen (returns them instead
    and sends nothing); fail names them in the failure message."""
    tok = getattr(args, "token", None)
    now = now_ms()
    notify_text = user = ctx = None
    new_notes: list = []
    with task_lock():
        e, t = bg_read(mid), load_task(mid)
        if not e or not t:
            return _bg_emit({"ok": False, "error": "no such long task"}, 2)
        if t.get("status") == "cancelled":
            if e.get("state") in _BG_LIVE:
                e.update(state="cancelled", finished_at=now)
                _bg_write(e)
            return _bg_emit({"ok": False, "cancelled": True, "error": "the user cancelled this task "
                             "(「取消」): stop the sub-agent; nothing is sent"}, 3)
        if e.get("state") != "running" or not tok or tok != e.get("token") \
                or t.get("status") != "background":
            return _bg_emit({"ok": False, "stale": True, "error": "this token no longer holds the task "
                             "(requeued, finished or taken again): stop and send nothing"}, 3)
        new_notes = _bg_notes_take_new(e)
        if act == "progress":
            stage = (getattr(args, "stage", None) or "").strip()[:200]
            note = (getattr(args, "note", None) or "").strip()[:300]
            e["heartbeat_at"] = now
            bg = dict(t.get("bg") or {})
            bg["heartbeat_at"] = now
            if stage:
                e["stage"] = bg["stage"] = stage
            if getattr(args, "notify", False) and (note or stage) \
                    and now - int(bg.get("notified_at") or 0) >= BG_NOTIFY_GAP_MIN * 60_000:
                bg["notified_at"] = now
                notify_text = note or (f"「{_asked(t)}」后台还在做：{stage}（已经 "
                                       f"{fmt_duration(now - int(e.get('queued_at') or now))}）。")
            t["bg"] = bg
            save_task(t)
            _bg_write(e)
            user, ctx = t.get("from_user_id"), t.get("context_token")
        elif new_notes:
            _bg_write(e)
            if act == "finish":
                return _bg_emit({"ok": False, "unread_notes": new_notes,
                                 "error": "用户在你上次 progress 之后又补充了要求（unread_notes，现在已算交给你）："
                                          "先照着调整结果，再 finish；最终结果里要回应这些补充"}, 2)
    if act == "progress":
        out = {"ok": True, "id": mid, "heartbeat_at": now}
        if new_notes:
            out["notes"] = new_notes
            out["notes_hint"] = "用户引用这条任务补充了要求：照着调整，最终结果里一并回应。"
        if notify_text:
            out["notice"] = _relay_send_notice(user, ctx, mid, notify_text)
        elif getattr(args, "notify", False):
            out["notify_skipped"] = f"at most one progress note per {BG_NOTIFY_GAP_MIN} min"
        return _bg_emit(out, 0)
    err = (getattr(args, "error", None) or "后台处理没能完成") if act == "fail" else None
    if err and new_notes:
        err += f"（你后来补充的「{'；'.join(snippet(n['text'], 30) for n in new_notes[:3])}」也还没做）"
    # finish / fail: the task's own reply path (cmd_complete: chunks, attachments,
    # retries, failed-text outbox, exactly once); the token is the claim fence
    ns = argparse.Namespace(
        task_id=mid, result=getattr(args, "result", None) if act == "finish" else None,
        result_file=getattr(args, "result_file", None) if act == "finish" else None, error=err,
        attach=list(getattr(args, "attach", None) or []) if act == "finish" else [],
        attach_as="auto", wait_attachments=10, then_wait=0, then_limit=5, force=False,
        merge_ids=None, raw=False, claim_id=tok, cont=False, background=False,
        step_done=None, artifact=[], stage=None)
    return cmd_complete(ns)


def cmd_long_tasks(args) -> int:
    """`long-tasks [list|show|take|progress|finish|fail|wake-check] [ID]`: the
    background queue - for the main agent (BACKGROUND.md), the watch hook
    (wake-check) and anyone who wants to look (list / show)."""
    act = getattr(args, "action", None) or "list"
    if act == "wake-check":
        return _bg_wake_check()
    check_background()
    if act == "list":
        return _bg_emit(bg_overview(), 0)
    mid = getattr(args, "id", None)
    if not mid:
        return _bg_emit({"ok": False, "error": f"long-tasks {act} needs a task id"}, 2)
    if act == "show":
        e = bg_read(mid)
        looks = bg_quote_lookups()
        mine = [x for x in looks if str(x.get("target")) == str(mid)]
        return _bg_emit({"ok": bool(e), "entry": e,
                         "last_quote_lookup": looks[-1] if looks else None,
                         "quote_lookups_for_this_task": mine[-5:]}, 0 if e else 2)
    if act == "take":
        return _bg_take(mid)
    return _bg_act(act, mid, args)


# --------------------------------------------------------------------------
# 2026-11-07 v1.5 / v1.6: background lane + quote-bound follow-ups
# --------------------------------------------------------------------------
# After OpenClaw's sub-agent design - runs share one queue lane capped by
# agents.defaults.subagents.maxConcurrent ("a safety valve"), follow-ups in a
# thread route to the session bound to it, the requester is told when an
# interrupted run resumes - and ilink-hub's quote routing (a reply that quotes
# a bot message goes to the backend that wrote it):
#   * at most BG_MAX_RUNNING background tasks run at once (default 1). The
#     rest wait in FIFO order; whose turn it is ("eligible") is the first
#     free-slot count of the queue, and only those can time out (v1.6);
#   * a message that QUOTES a live background task's message is a follow-up
#     for that task: it is appended to the entry's notes, the main agent gets
#     it at take or the next progress, the one final reply answers it.
# v1.6 (review of v1.5): notes are a log with sequence numbers and the taker
# has an offset (Kafka consumer offsets / Redis Streams last-delivered-id) -
# unread notes are never trimmed, finish refuses while any is unseen; every
# quote lookup is recorded for diagnosis; only the first follow-up is
# acknowledged (reply slots); refusals are said, not silent.
BG_MAX_RUNNING = max(1, min(8, _env_int("WECHAT_BRIDGE_BG_MAX_RUNNING", 1)))
BG_NOTES_KEEP_READ = 30           # handed-over notes kept for a later re-take
BG_NOTES_MAX_UNREAD = 30          # beyond this a follow-up is refused, out loud
BG_LOOKUPS = BG_DIR / "quote_lookups.json"


_BG_CACHE: dict = {}             # file name -> (signature or None, entry)
_bg_cache_lock = threading.Lock()


def _bg_entries() -> list:
    """All queue entries, by file name. v1.7: a file is parsed again only when
    it changed - (inode, mtime, size), like scan_task_index - and a file
    written in the last 2 s is never served from the cache (two rewrites in
    one mtime tick can reuse an inode). v1.6 parsed every entry, done ones
    kept 14 days included, on every lane / position / quote check. Callers
    must not mutate the entries; decisions re-read the file (bg_read)."""
    try:
        it = os.scandir(BG_QUEUE)
    except OSError:
        with _bg_cache_lock:
            _BG_CACHE.clear()
        return []
    now_ns = time.time_ns()
    out, seen = [], set()
    with it, _bg_cache_lock:
        for d in sorted(it, key=lambda d: d.name):
            if not d.name.endswith(".json"):
                continue
            try:
                st = d.stat()
            except OSError:
                continue
            seen.add(d.name)
            sig = (st.st_ino, st.st_mtime_ns, st.st_size)
            hit = _BG_CACHE.get(d.name)
            if hit and hit[0] == sig:
                e = hit[1]
            else:
                e = read_json(Path(d.path), None)
                e = e if isinstance(e, dict) and e.get("message_id") is not None else None
                _BG_CACHE[d.name] = (sig if _settled_ns(st.st_mtime_ns, now_ns) else None, e)
            if e is not None:
                out.append(e)
        for name in [n for n in _BG_CACHE if n not in seen]:
            del _BG_CACHE[name]
    return out


def _bg_lane(entries=None) -> dict:
    """Running count, free slots, queued ids in FIFO order, the ones whose
    turn it is (the first `free` of them), and when a turn last opened: a
    task ended or left the queue (done, failed, cancelled, returned) or was
    requeued."""
    es = _bg_entries() if entries is None else entries
    running = sum(1 for e in es if e.get("state") == "running")
    queued = sorted((e for e in es if e.get("state") == "queued"),
                    key=lambda e: int(e.get("queued_at") or 0))
    freed = 0
    for e in es:
        if e.get("state") not in _BG_LIVE:
            freed = max(freed, int(e.get("finished_at") or 0))
        freed = max(freed, int(e.get("requeued_at") or 0))
    free = max(0, BG_MAX_RUNNING - running)
    ids = [e["message_id"] for e in queued]
    return {"running": running, "free": free, "queued": ids, "eligible": ids[:free],
            "freed_at": freed}


def _bg_turn_since(e: dict, lane: dict) -> int:
    """Since when it has been this queued task's turn."""
    return max(int(e.get("requeued_at") or e.get("queued_at") or 0), int(lane["freed_at"]))


def _bg_worked_ms(e: dict, now: int) -> int:
    """Work time: earlier takes plus the running one - never queue time."""
    run = int(e.get("run_ms") or 0)
    if e.get("state") == "running" and e.get("taken_at"):
        run += max(0, now - int(e["taken_at"]))
    return run


def bg_position(mid) -> int:
    """1-based place among live background tasks: running ones first, then
    the queue in FIFO order. 0 = not live."""
    es = [e for e in _bg_entries() if e.get("state") in _BG_LIVE]
    es.sort(key=lambda e: (e.get("state") != "running", int(e.get("queued_at") or 0)))
    for i, e in enumerate(es, 1):
        if str(e["message_id"]) == str(mid):
            return i
    return 0


def _bg_notes_norm(e: dict) -> None:
    """v1.5 entries: notes without sequence numbers, notes_seen an index."""
    notes = list(e.get("notes") or [])
    if any("seq" not in n for n in notes):
        for i, n in enumerate(notes, 1):
            n.setdefault("seq", i)
        e["notes"] = notes
    if "note_seq" not in e:
        e["note_seq"] = max([int(n.get("seq") or 0) for n in notes] or [0])
    if "notes_seen_seq" not in e:
        e["notes_seen_seq"] = min(int(e.get("notes_seen") or 0), int(e["note_seq"]))
    e.pop("notes_seen", None)


def _bg_notes_add(e: dict, note: dict) -> bool:
    """Append a follow-up (caller writes e). Only notes the taker already has
    are ever trimmed; False when BG_NOTES_MAX_UNREAD are still unseen."""
    _bg_notes_norm(e)
    seen = int(e.get("notes_seen_seq") or 0)
    notes = list(e.get("notes") or [])
    if sum(1 for n in notes if int(n["seq"]) > seen) >= BG_NOTES_MAX_UNREAD:
        return False
    e["note_seq"] = int(e.get("note_seq") or 0) + 1
    notes.append(dict(note, seq=e["note_seq"]))
    read = [n for n in notes if int(n["seq"]) <= seen][-BG_NOTES_KEEP_READ:]
    e["notes"] = read + [n for n in notes if int(n["seq"]) > seen]
    return True


def _bg_notes_take_new(e: dict) -> list:
    """The follow-ups the taker has not seen yet, now marked handed over
    (caller writes e when the list is not empty)."""
    _bg_notes_norm(e)
    seen = int(e.get("notes_seen_seq") or 0)
    new = [n for n in e.get("notes") or [] if int(n["seq"]) > seen]
    if new:
        e["notes_seen_seq"] = max(int(n["seq"]) for n in new)
    return new


def _bg_notes_at_close(mid) -> list:
    """Caller holds task_lock (cmd_complete closing a background task, v1.7):
    the follow-ups not handed over yet, now marked handed over. `long-tasks
    finish` checked them too, but it lets go of the lock before cmd_complete
    takes it again - a quoted follow-up in between was merged into the task,
    acknowledged and then never answered."""
    e = bg_read(mid)
    if not e or e.get("state") not in _BG_LIVE:
        return []
    new = _bg_notes_take_new(e)
    if new:
        _bg_write(e)
    return new


def _bg_note_notice(e: dict | None, nid) -> None:
    """Register a notice on the task (append, never overwrite), so that a
    quote of it routes back to the task."""
    if e and nid:
        e["notice_ids"] = ([x for x in e.get("notice_ids") or [] if x != nid] + [nid])[-30:]
        _bg_write(e)


def bg_quote_lookups() -> list:
    rows = read_json(BG_LOOKUPS, [])
    return rows if isinstance(rows, list) else []


def _bg_lookup_record(rec: dict) -> None:
    """The last 20 quote lookups (found or not) - `long-tasks show` / `list`."""
    try:
        BG_DIR.mkdir(parents=True, exist_ok=True)
        os.chmod(BG_DIR, 0o700)
        atomic_write_json(BG_LOOKUPS, (bg_quote_lookups() + [rec])[-20:], mode=0o600)
    except OSError:
        pass


def _bg_lead_of(x: dict | None, depth: int = 0) -> dict | None:
    """The live background task behind x: x itself, the task a notice was
    about (create_notice keeps `about` under execution - v1.5 read the top
    level and only worked through notice_ids), or the task x merged into."""
    if not x or depth > 3:
        return None
    if x.get("status") == "background":
        return x
    nxt = None
    if x.get("kind") == "notice":
        nxt = (((x.get("execution") or {}).get("about") or {}).get("task_id")
               or (x.get("about") or {}).get("task_id"))
        if not nxt:
            nxt = next((e["message_id"] for e in _bg_entries()
                        if x.get("message_id") in (e.get("notice_ids") or [])), None)
    else:
        nxt = (x.get("send") or {}).get("merged_into")
    return _bg_lead_of(load_task(nxt), depth + 1) if nxt else None


QUOTE_TEXT_MATCH_MIN = 6           # v1.7: shortest quoted text that may route
QUOTE_TEXT_MATCH_HOURS = 48
_QT_DROP = re.compile(r"[\s*_`#>|~=+\-—–:：，,。.!！?？;；、“”\"'‘’（）()\[\]【】「」《》…·]+")


def _qt_norm(s) -> str:
    """Quoted text as WeChat shows it vs. what the bridge sent: NFKC, no
    whitespace, punctuation or Markdown marks (the client renders them)."""
    return _QT_DROP.sub("", unicodedata.normalize("NFKC", str(s or "")))[:600]


def _quote_text_target(t: dict) -> tuple:
    """v1.7: the quote named no id the bridge knows - sendmessage returns no
    message_id on older servers and this account never echoes (TUNING.md:
    41 sends, 0 echoes) - but WeChat still shows the quoted text (ref_msg
    title / message_item). Match it against what the bridge sent to and got
    from this user in the last QUOTE_TEXT_MATCH_HOURS. Only a match that leads
    to exactly ONE live background task routes; several -> (None, None): an
    ordinary message, never a guess."""
    raw = str(t.get("quote") or "")
    user = t.get("from_user_id")
    if not raw or not user or raw.startswith("（引用了一条桥接本地没有记录的消息"):
        return None, None
    probes = [n for n in (_qt_norm(p) for p in raw.split(" | ")) if len(n) >= QUOTE_TEXT_MATCH_MIN]
    if not probes:
        return None, None
    rows = _quote_exec("SELECT id,user,dir,kind,text,media,name,task,at FROM msgs WHERE user=? "
                       "AND at>=? ORDER BY at DESC LIMIT 400",
                       (user, now_ms() - QUOTE_TEXT_MATCH_HOURS * 3_600_000), fetch=True) or []
    leads: dict = {}
    for row in rows:
        rec = dict(zip(_QUOTE_KEYS, row))
        body = _qt_norm(rec.get("text"))
        if len(body) < QUOTE_TEXT_MATCH_MIN or not any(
                p in body or body in p for p in probes):
            continue
        cand = rec.get("task") or (rec.get("id") if rec.get("dir") == "in" else None)
        lead = _bg_lead_of(load_task(cand)) if cand else None
        if lead and lead["message_id"] != t["message_id"] \
                and lead.get("from_user_id") == user:
            leads.setdefault(str(lead["message_id"]), (lead, rec))
    return next(iter(leads.values())) if len(leads) == 1 else (None, None)


def _bg_quote_target(t: dict) -> tuple:
    """(live background task the message quotes, which way it was found, the
    quote-store record). Every id of the quote (v1.7: not just svr_id), each
    tried in order: the record's task (a bot message the send path or an echo
    registered), the record's own id and the quoted id itself (the user's
    original request - the way that still works when no signal went out);
    then (v1.7) the quoted text (_quote_text_target, via "text")."""
    q = t.get("quote")
    ids = [str(x) for x in (t.get("quote_ids") or []) if x not in (None, "")]
    svr = t.get("quote_svr_id") or (q.get("svr_id") if isinstance(q, dict) else None)
    if svr and str(svr) not in ids:
        ids.insert(0, str(svr))
    first = None
    for qid in ids:
        rec = quote_get(qid) or {}
        if rec and first is None:
            first = rec
        for via, cand in (("task", rec.get("task")), ("id", rec.get("id")), ("svr_id", qid)):
            if not cand:
                continue
            lead = _bg_lead_of(load_task(cand))
            if lead and lead.get("from_user_id") == t.get("from_user_id") \
                    and lead["message_id"] != t["message_id"]:
                return lead, via, rec
    lead, rec = _quote_text_target(t)
    if lead:
        return lead, "text", rec
    return None, None, first


def _bg_user_live(user) -> bool:
    return any(e.get("state") in _BG_LIVE and e.get("user") == user for e in _bg_entries())


def _bg_attach_quoted(ids: list, now: int) -> list:
    """Caller holds task_lock (_batch_dispatch, and the non-batch claim path
    since v1.6): quoted follow-ups for a live background task go into its
    notes instead of becoming new tasks. While the user has a live background
    task every quote lookup is recorded (found or not) and a miss is logged;
    a follow-up that is refused (secret-looking, too many unseen) stays an
    ordinary message AND the user is told it was not merged."""
    if not BG_QUEUE.is_dir():
        return []
    out = []
    for i in ids:
        t = load_task(i)
        if not t or t.get("status") != "pending_execution" or t.get("kind") not in ("text", "voice"):
            continue
        svr, user = t.get("quote_svr_id"), t.get("from_user_id")
        text = (t.get("text") or "").strip()
        if not (svr or t.get("quote_ids") or t.get("quote")) or not text \
                or not _bg_user_live(user):
            continue
        lead, via, rec = _bg_quote_target(t)
        e = bg_read(lead["message_id"]) if lead else None
        look = {"at": now, "message_id": i, "quote_svr_id": svr, "store_record": bool(rec),
                **({"quote_ids": t["quote_ids"]} if t.get("quote_ids") else {}),
                "record_task": (rec or {}).get("task"), "via": via,
                "target": lead["message_id"] if lead else None,
                "found": bool(e and e.get("state") in _BG_LIVE)}
        if not look["found"]:
            _bg_lookup_record(dict(look, outcome="ordinary message: no live background task behind the quote"))
            log(f"background: message id={i} quotes svr_id=…{str(svr or '-')[-6:]} - no live background task "
                f"behind it (quote-store record: {'yes' if rec else 'no'}); handled as an ordinary message")
            continue
        refusal = None
        if _SECRETISH.search(text):
            refusal = ("secret-looking", f"这条看起来含敏感信息（密码、token 之类），没有并入后台的「{_asked(lead)}」，"
                                         "按普通消息处理。要补充的话请去掉敏感内容再引用。")
        elif not _bg_notes_add(e, {"at": now, "message_id": i, "text": text[:2000]}):
            refusal = ("too many unseen", f"「{_asked(lead)}」已经攒了 {BG_NOTES_MAX_UNREAD} 条还没处理的补充，"
                                          "这条没有并入，按普通消息处理。")
        if refusal:
            _bg_lookup_record(dict(look, outcome=f"refused ({refusal[0]}): ordinary message"))
            nid = create_notice(user, render_for_wechat(refusal[1]), reason="progress",
                                about={"task_id": lead["message_id"]}, essential=True)
            _bg_note_notice(e, nid)
            continue
        if not _bg_write(e):
            continue
        _merge_tasks(lead["message_id"], lead, [i], None)
        _bg_lookup_record(dict(look, outcome=f"note #{e['note_seq']}"))
        if int(e["note_seq"]) == 1:      # v1.6: only the first is acknowledged (reply slots)
            nid = create_notice(user, render_for_wechat(
                f"收到，已转给后台正在做的「{_asked(lead)}」：{snippet(text, 40)}。之后的补充也会一起"
                "处理（不再逐条回复），最后在同一条结果里回复。"), reason="progress",
                about={"task_id": lead["message_id"]}, essential=True)
            _bg_note_notice(e, nid)
        log(f"background: message id={i} quotes task id={lead['message_id']} ({via}) - note "
            f"#{e['note_seq']}")
        out.append(i)
    return out


_EXEC_MEDIA_FIELDS = ("media_path", "file_name", "play_length", "media_error")
_EXEC_OPTIONAL_FIELDS = ("media_bytes", "thumb_path")
_EXEC_V4_FIELDS = ("voice_format", "voice_note", "quoted_media",
                   "media_error_code", "media_error_hint",
                   "unsupported_items", "quote_from_store")


def _claim_hint_base(t: dict) -> str | None:
    """2026-11-02 六-2: one short per-kind hint on the claim itself, so a
    routine task does not need the full executor manual. Text tasks get
    no hint on purpose: the core manual already covers them, and adding
    one to every text task would cost tokens on the common path."""
    kind = t.get("kind")
    text = (t.get("text") or "").strip()
    if kind in ("image", "video", "file") and not text and not t.get("quote"):
        if settings_get().get("media_wait", True):
            return ("无文字媒体：先看媒体，等追问最多 30 秒再回完整答案；"
                    "30 秒内没追问只简短确认并问做什么。详见 EXECUTOR-media.md。")
        return ("无文字媒体且媒体等待已关：直接回，不等追问。"
                "详见 EXECUTOR-media.md。")
    if kind in ("image", "video", "file"):
        return "媒体任务：先实际看/读媒体再回。详见 EXECUTOR-media.md。"
    if kind == "voice":
        return ("语音：先转写并在开头简短复述，转不了如实说，不猜内容。"
                "详见 EXECUTOR-media.md。")
    if t.get("media_error"):
        return "媒体下载失败：详见 EXECUTOR-media.md 的 media_error 一节。"
    return None


def _claim_hint(t: dict) -> str | None:
    """_claim_hint_base plus what the bridge already prepared (2026-11-05)."""
    base = _claim_hint_base(t)
    extra = []
    if t.get("preview_path"):
        extra.append("先看 preview_path（≤1568px），细节不够再开原图")
    if t.get("frames"):
        extra.append("关键帧 frames" + ("和音轨 audio_wav" if t.get("audio_wav") else "")
                     + "已提取好，直接用，不必再跑 video-frames/video-audio")
    if t.get("text_inline") is not None:
        extra.append("文件全文已在 text_inline，不必再读文件")
    elif t.get("text_head"):
        extra.append("text_head 是文件开头" + ("，全文在 text_path" if t.get("text_path") else ""))
    if not extra:
        return base
    return ((base + " ") if base else "") + "；".join(extra) + "。"


_EXEC_DIGEST_FIELDS = ("preview_path", "frames", "audio_wav", "text_inline",
                       "text_head", "text_chars", "text_path")


def _executor_view(t: dict) -> dict:
    """What the executor agent needs, nothing more. The full task carries
    raw_items (CDN query params, AES keys), context_token and bookkeeping
    that only cost LLM tokens and put credential-adjacent values into the
    model context. `claim --full` returns the old complete record."""
    out = {"message_id": t["message_id"], "kind": t.get("kind"),
           "text": t.get("text") or ""}
    ex = t.get("execution") or {}
    sup = int(ex.get("supersedes") or 0)
    if sup:
        # 2026-11-06: dispatched again because the user wrote more while it
        # was being handled (not a crash): say so, and where it had got to.
        ck = ex.get("checkpoint") or {}
        out["superseded"] = (
            "这条之前已经在处理，用户又发了新消息，上一版回复已作废、没有发出；"
            "和这一批其他消息一起重新回答"
            + (f"（上次做到：{ck['stage']}，已落盘的产物可以接着用）" if ck.get("stage") else "")
            + "。")
    pv = _plan_view(t) if (RELAY_ON and t.get("relay")) else None
    if (ex.get("resumed") and not sup) or int(ex.get("reclaims") or 0):
        ck = ex.get("checkpoint") or {}
        stage, note = ck.get("stage", ""), ck.get("note", "")
        done = f"上次做到：{stage}" if stage else "上次没有留下阶段记录"
        if note:
            done += f"（{note}）"
        out["resume_from"] = _relay_resume_text(t, pv, done) if pv is not None else (
            f"断点续跑：这条任务之前已被执行过 {int(ex.get('reclaims') or 0)} 次，"
            f"因超过 10 分钟没心跳被收回。{done}。先按断点接着做，不要从头重做；"
            f"已做完的步骤不要重做一遍，步骤产物以落盘文件为准，不要只信内存。")
    _hint = _claim_hint(t)
    if _hint:
        out["hint"] = _hint
    if t.get("quote"):
        out["quote"] = t["quote"]      # the message the user is replying to
    if t.get("bg_notes"):
        # v1.7: follow-ups the user quoted onto a long task the background never
        # took; v1.6 dropped them when the task came back to the foreground
        out["followups"] = [n.get("text") for n in t["bg_notes"] if n.get("text")]
        out["hint"] = ((out["hint"] + " ") if out.get("hint") else "") + (
            "followups 是用户引用这条任务补充的要求（后台没接手，退回前台）：一起照做，"
            "最终结果里一并回应。")
    if t.get("kind") != "text":
        for k in _EXEC_MEDIA_FIELDS:
            out[k] = t.get(k)
        for k in _EXEC_OPTIONAL_FIELDS:
            if t.get(k) is not None:
                out[k] = t[k]
        for k in _EXEC_V4_FIELDS:
            if t.get(k) not in (None, False, ""):
                out[k] = t[k]
        if t.get("kind") == "voice":
            out["voice_wav"] = bool(t.get("voice_wav"))
        if t.get("file_md5_ok") is False:
            out["file_md5_ok"] = False       # content may be damaged: say so
        for k in _EXEC_DIGEST_FIELDS:
            if t.get(k) not in (None, "", []):
                out[k] = t[k]
        if t.get("has_audio") is False:
            out["has_audio"] = False
    if pv is not None:
        out["plan"] = pv            # 2026-11-07 relay: the leg that starts now
    elif RELAY_ON:
        _why = long_hint_rules(t)
        if _why:
            out["long_hint"] = _long_hint_text(_why, t)
        _rp = _resume_plan_hint(t)
        if _rp:
            out["resume_plan"] = _rp
    if PEEK_ON and pv is None:
        _pk = file_peeks(t)         # 2026-11-07 §8.2.4
        if _pk:
            out["file_peek"] = _pk
            out["hint"] = ((out["hint"] + " ") if out.get("hint") else "") + (
                "file_peek 是消息里提到的文件开头（只读快照），够答就不必再读文件。")
    return out


MAX_RECLAIMS = 2


# 2026-11-06: CLAIM_SETTLE_MS (the claim-side 1.2 s burst window) is gone -
# bursts are coalesced only by the batches in state/pending.json.
CLAIM_WAIT_MAX_SEC = 300
WAITER_FILE = STATE_DIR / "claim_waiter.txt"


def _write_waiter(deadline: float) -> None:
    until = now_ms() + int(max(0.0, deadline - time.monotonic()) * 1000)
    with contextlib.suppress(OSError):
        WAITER_FILE.write_text(f"{os.getpid()} {until}\n")


def _clear_waiter() -> None:
    with contextlib.suppress(OSError, IndexError):
        if WAITER_FILE.read_text().split()[0] == str(os.getpid()):
            WAITER_FILE.unlink()


def _waiter_alive() -> bool:
    try:
        pid, until = WAITER_FILE.read_text().split()[:2]
        os.kill(int(pid), 0)
    except (OSError, ValueError):
        return False
    return int(until) > now_ms()


def _reclaim_stale() -> bool:
    """Executing with no claim/`progress` heartbeat for 10+ minutes goes back
    to pending - at most MAX_RECLAIMS times, then the user is told."""
    woke = False
    with task_lock():
        now = now_ms()
        for s_ in scan_task_index():
            if s_["status"] != "executing":
                continue
            last = max(int(s_.get("claimed_at") or 0),
                       int(s_.get("heartbeat_at") or 0))
            if not last or now - last <= STALE_EXECUTING_MS:
                continue
            t = load_task(s_["message_id"])
            if not t or t.get("status") != "executing":
                continue
            ex = dict(t.get("execution") or {})
            if int(ex.get("hold_until") or 0) > now:
                continue  # long-step lease still running
            n = int(ex.get("reclaims") or 0)
            revoked = ex.pop("claim_id", None)
            if revoked:
                ex["revoked_claim_id"] = revoked
            if RELAY_ON and t.get("relay"):
                # 2026-11-07 relay: a planned leg resumes from its plan
                _pl = plan_load(t["message_id"])
                if _pl is not None and _pl.get("state", "active") == "active":
                    woke = _relay_reclaim(t, ex, _pl, n, now) or woke
                    continue
            if n >= MAX_RECLAIMS:
                t["status"] = "waiting_send"
                t["result_text"] = (
                    f"这条消息我前后试了 {n + 1} 次，每次都在处理中途断掉了，先停下来，"
                    "免得反复空转。需要的话换个说法，或者拆成小一点的任务再发给我。")
                ex.update(finished_at=now, error="reclaim limit reached", by="bridge")
                t["execution"] = ex
                save_task(t)
                woke = True
                log(f"task id={t['message_id']} given up after {n + 1} executor runs")
            else:
                # resume 2026-11-02: snapshot the last stage/note as the
                # checkpoint, so the next executor resumes instead of
                # restarting from scratch (the previous failure mode:
                # 3 runs, 0 heartbeats, all work redone and lost again).
                ex.update(reclaimed_at=now, reclaim_reason="stale_executing",
                          reclaims=n + 1,
                          checkpoint={"stage": ex.get("stage", ""),
                                      "note": ex.get("note", ""),
                                      "stage_at": ex.get("stage_at", 0)},
                          checkpoint_at=now)
                t["status"] = "pending_execution"
                t["pending_at"] = now
                t["execution"] = ex
                save_task(t)
                log(f"reclaimed stale executing task id={t['message_id']} "
                    f"({n + 1}/{MAX_RECLAIMS})")
    return woke


# CLI exit codes, as the executor sees them (2026-11-06: 4 added).
#   0  ok
#   1  a send did not go out (send, send-media, send-image/-file/-video)
#   2  bad arguments or a state that forbids the action (fail(): task not
#      found, empty result, reply already queued, ...)
#   3  stop: cancelled by 「取消」, stale claim (another executor has the
#      task, or this leg handed it on with complete --continue), and command-specific refusals (send-direct: 24 h window
#      closed; wait-login: verify code; canary-dedupe)
#   4  SUPERSEDED: the user sent a new message while this reply was being
#      prepared. The reply is abandoned: the bridge sent nothing for this
#      call and sends nothing for this claim any more. Stop working on it -
#      no retry, no --force, no send-media, no send-direct: the message is
#      dispatched again together with the new one (state/pending.json), and
#      the next round answers both. complete / progress / heartbeat /
#      send-media / send-image|file|video --task-id exit 4 with
#      {"ok": false, "superseded": true, "exit_code": 4, ...}.
EXIT_SUPERSEDED = 4


def _superseded_why(t: dict, claim_id: str | None) -> str | None:
    """Why an executor acting for task `t` with `claim_id` must stop because
    the task was superseded (feature 2), or None. A task that has been
    superseded only accepts calls carrying the claim id of the claim that
    returned it since: without --claim-id the bridge cannot tell the old
    executor from the new one, and sending the abandoned reply is the one
    thing that must not happen."""
    ex = t.get("execution") or {}
    gone = ex.get("superseded_claim_ids") or []
    base = ("superseded (exit 4): the user sent a new message while this reply "
            "was being prepared, so this reply is abandoned and nothing was sent. "
            "Stop now and send nothing for this task (no retry, no --force, no "
            "send-media, no send-direct): it is dispatched again together with "
            "the new message, and the next round answers both.")
    if claim_id and claim_id in gone:
        return base
    if not claim_id and int(ex.get("supersedes") or 0) > 0:
        return base + (" (This task was superseded before: only the executor "
                       "of its current claim may act for it, with --claim-id.)")
    return None


def _superseded_out(t: dict, why: str) -> dict:
    ex = t.get("execution") or {}
    return {"ok": False, "superseded": True, "exit_code": EXIT_SUPERSEDED,
            "task_id": t.get("message_id"), "superseded_by": ex.get("superseded_by"),
            "error": why}


def _claim_check(t: dict, claim_id: str | None) -> str | None:
    """None if an executor holding `claim_id` may act on task `t`, else why
    not (2026-11-04 review: v5.0.5 had no fencing token, so an executor whose
    task was reclaimed after 10 silent minutes could still complete it while
    its successor worked on the same message). No claim id: accepted, as
    before. A task reclaimed but not yet re-claimed (pending_execution) still
    takes its old executor's result: the work is done, no need to redo it."""
    if not claim_id:
        return None
    if any(lg.get("claim_id") == claim_id and lg.get("how") == "continue"
           for lg in (t.get("relay") or {}).get("legs") or []):
        return ("handed off: this leg ended with complete --continue and the next "
                "leg owns the task now - stop and send nothing")
    if claim_id == (t.get("bg") or {}).get("from_claim"):
        return ("handed to the background: the main agent does this task now - stop "
                "and send nothing")
    ex = t.get("execution") or {}
    if t.get("status") in ("executing", "waiting_send") \
            and ex.get("claim_id") not in (None, claim_id):
        return ("stale claim: this task was reclaimed (10 min without a "
                "heartbeat) and another executor is on it now - stop and send "
                "nothing")
    return None


def _readopt_or_stale(t: dict, claim_id: str | None, now: int) -> str | None:
    """progress / heartbeat with --claim-id: the stale-claim refusal, or -
    when the task was reclaimed for silence but nobody has claimed it again
    yet - hand it back to the executor that is evidently still alive, so the
    same work is not started a second time. Mutates `t` (caller saves)."""
    why = _claim_check(t, claim_id)
    if why or not claim_id:
        return why
    ex = dict(t.get("execution") or {})
    if t.get("status") == "pending_execution" and ex.get("revoked_claim_id") == claim_id:
        ex.pop("revoked_claim_id", None)
        ex.update(claim_id=claim_id, readopted_at=now,
                  claimed_at=int(ex.get("claimed_at") or now))
        t["execution"] = ex
        t["status"] = "executing"
        t.pop("relay_pending", None)
        log(f"task id={t.get('message_id')}: reclaimed executor is alive; re-adopted")
    return None


def _claim_pending(limit: int, only: set | None = None,
                   since_ms: int | None = None, batch_id=False) -> list:
    """Claim up to `limit` pending tasks, oldest first (only those in `only`
    when given: the snapshot _dispatch_group took). 2026-11-06: re-checked
    under the lock - a task a new message pulled back into an open batch
    since the snapshot (batch_open) is not claimed, nor one that moved to
    another batch (batch_id given): the caller looks again, so a batch is
    never split across two rounds and a held message never claimed."""
    claimed = []
    claim_id = secrets.token_hex(6)       # fencing token, shared by this claim
    with task_lock():
        pending = [x for x in scan_task_index() if x["status"] == "pending_execution"
                   and (only is None or x["message_id"] in only)]
        for s_ in pending[:limit]:
            t = load_task(s_["message_id"])
            if not t or t.get("status") != "pending_execution" or t.get("batch_open"):
                continue
            if batch_id is not False and t.get("batch_id") != batch_id:
                continue
            prev_ex = t.get("execution") or {}
            t["status"] = "executing"
            t["execution"] = {"claimed_at": now_ms(), "by": "muse",
                              "claim_id": claim_id,
                              # warm: it arrived while this claim was waiting
                              "warm": bool(since_ms and int(t.get("received_at") or 0) >= since_ms)}
            if t.pop("relay_pending", None):
                # 2026-11-07 relay: a new leg - its claim is its first heartbeat
                # (auto-progress then only warns after a long silence)
                t["execution"]["heartbeat_at"] = t["execution"]["claimed_at"]
            if prev_ex.get("reclaims"):
                t["execution"]["reclaims"] = prev_ex["reclaims"]
            # 2026-11-06 supersede: the abandoned claims stay refused (exit 4)
            # after the batch is claimed again
            for k in ("superseded_claim_ids", "supersedes", "superseded_at",
                      "superseded_by"):
                if prev_ex.get(k):
                    t["execution"][k] = prev_ex[k]
            # resume: keep the last stage/note as the checkpoint for the
            # new executor; a task reclaimed for silence is resumed, not
            # restarted. heartbeat_at is deliberately NOT carried over:
            # the new run must prove it is alive with its own first
            # progress/heartbeat call.
            ck = prev_ex.get("checkpoint") or {}
            if not ck and (prev_ex.get("stage") or prev_ex.get("note")):
                ck = {"stage": prev_ex.get("stage", ""),
                      "note": prev_ex.get("note", ""),
                      "stage_at": prev_ex.get("stage_at", 0)}
            if ck:
                t["execution"]["checkpoint"] = ck
                t["execution"]["resumed"] = True
            save_task(t)
            claimed.append(t)
    return claimed


CLAIM_POLL_SEC = 0.05
CLAIM_MEDIA_HOLD_MS = _env_int("WECHAT_BRIDGE_CLAIM_MEDIA_HOLD_MS", 15_000)
CLAIM_DOWNLOAD_WAIT_SEC = _env_int("WECHAT_BRIDGE_CLAIM_DOWNLOAD_WAIT_SEC", 60)
# 2026-11-06: _reads_complete / _settle / _settled_pending and
# CLAIM_SETTLE_SHORT_MS / CLAIM_SETTLE_CAP_MS (the adaptive claim window) are
# removed: a claim takes one dispatched batch, as the batch was formed.


def _dispatch_group(limit: int, skip_relay: bool = False) -> tuple:
    """(ids to claim now, holding_for_media, batch_id or None). The oldest
    dispatched message
    decides: its whole batch is claimed together - one round, one
    merge_hint, whatever `limit` says - once the batch's media is
    downloaded (at most CLAIM_MEDIA_HOLD_MS of waiting for it). Messages
    without a batch (stored before this package, or WECHAT_BRIDGE_BATCH=0)
    are claimed as before: oldest first, up to `limit`, holding for a
    picture/file sent just before the newest of them."""
    idx = scan_task_index()
    pend = [x for x in idx if x.get("status") == "pending_execution"
            and not x.get("batch_open")]
    if not pend:
        return [], False, None
    if skip_relay and any(x.get("relay_pending") for x in pend):
        return [], False, None     # 2026-11-07: a relay leg waits for a fresh turn
    now = now_ms()
    bid = pend[0].get("batch_id")
    if bid:
        group = [x for x in idx if x.get("batch_id") == bid and not x.get("batch_open")
                 and x.get("status") in _AGENT_BOUND]
        # hold for its media, counted from arrival - or from the dispatch for
        # one carried over from an earlier batch (superseded)
        dl = [max(int(x.get("received_at") or 0), int(x.get("dispatched_at") or 0))
              for x in group if x.get("status") == "downloading"]
        if dl and now - min(dl) < CLAIM_MEDIA_HOLD_MS:
            return [], True, bid
        return [x["message_id"] for x in group
                if x.get("status") == "pending_execution"], False, bid
    legacy = [x for x in pend if not x.get("batch_id")]
    # 2026-11-07 relay: a relay leg is claimed alone, never grouped with others
    legacy = (legacy[:1] if legacy[0].get("relay_pending")
              else [x for x in legacy if not x.get("relay_pending")])[:max(1, int(limit or 1))]
    if BG_ON and BG_QUEUE.is_dir() and any(x.get("quoted") for x in legacy):
        with task_lock():   # 2026-11-07 v1.6: quote routing without batching too
            att = _bg_attach_quoted([x["message_id"] for x in legacy if x.get("quoted")], now_ms())
        if att:
            _wake_serve()
            legacy = [x for x in legacy if x["message_id"] not in att]
            if not legacy:
                return [], False, None
    newest = max(int(x.get("received_at") or 0) for x in legacy)
    hold = [int(x.get("received_at") or 0) for x in idx
            if x.get("status") == "downloading" and not x.get("batch_id")
            and int(x.get("received_at") or 0) <= newest]
    if hold and now - min(hold) < CLAIM_MEDIA_HOLD_MS:
        return [], True, None
    return [x["message_id"] for x in legacy], False, None


def _inbox_mtime() -> int:
    try:
        return os.stat(INBOX_DIR).st_mtime_ns
    except OSError:
        return 0


def _downloading() -> int:
    return sum(1 for x in scan_task_index() if x.get("status") == "downloading")


def claim_tasks(limit: int = 1, wait_s: float = 0.0, settle_ms: int | None = None,
                full: bool = False, context_since: int | None = None) -> dict:
    """The claim logic behind `claim` and `complete --then-wait`; returns the
    JSON result. v5: `wait_s` blocks up to N s for the next message - a warm
    executor (conversation still in its context) takes follow-ups with no
    cold start and no second agent; the hook sees the waiter
    (state/claim_waiter.txt) and does not wake another one.
    2026-11-06: a claim returns ONE dispatched batch (pending.json) - all of
    it, with a merge_hint when it holds several messages - and nothing else;
    the claim-side burst window is gone (settle_ms is accepted and ignored).
    While a batch is still open (the user is typing) the claim keeps waiting
    for its dispatch, past wait_s if need be (a batch is due within
    BATCH_MAX_MS), and anything overdue is dispatched from here as well.
    Unchanged: the wait loop reads the inbox only when it changed (every
    50 ms; a full look once a second); a claim also waits (up to
    CLAIM_DOWNLOAD_WAIT_SEC) for media that is still downloading - the hook
    wakes executors for dispatched downloads, so cold start and download
    overlap; context_since returns only what is new."""
    del settle_ms               # kept in the signature for old callers
    wait_s = min(CLAIM_WAIT_MAX_SEC, max(0.0, float(wait_s or 0)))
    warm_turn = RELAY_ON and (wait_s > 0 or context_since is not None)
    if _reclaim_stale():
        _wake_serve()
    start = time.monotonic()
    started_ms = now_ms()
    deadline = start + wait_s
    dl_deadline = start + max(wait_s, CLAIM_DOWNLOAD_WAIT_SEC)
    # an open batch is due within BATCH_MAX_MS of opening: never wait longer
    batch_deadline = start + max(wait_s, BATCH_MAX_MS / 1000 + 5)
    registered = 0.0
    claimed: list = []
    seen, last_full = None, 0.0
    try:
        while True:
            try:
                batch_flush_due()           # overdue batch: dispatch it here
            except OSError as e:
                log(f"claim: batch dispatch failed ({type(e).__name__}); serve retries")
            m, now_m = _inbox_mtime(), time.monotonic()
            if m != seen or now_m - last_full >= 1.0:
                seen, last_full = m, now_m
                ids, _held, bid = _dispatch_group(limit, skip_relay=warm_turn)
                claimed = (_claim_pending(len(ids), set(ids), since_ms=started_ms,
                                          batch_id=bid) if ids else [])
                if claimed:
                    break
                if ids:
                    seen = None     # the group changed under us: look again now
                elif warm_turn and any(
                        x.get("relay_pending") and x.get("status") == "pending_execution"
                        and not x.get("batch_open") for x in scan_task_index()):
                    break   # 2026-11-07 relay: a leg waits for a fresh turn - the hook wakes one
            now_m = time.monotonic()
            until = deadline
            if _downloading():
                until = max(until, dl_deadline)
            due = batch_next_due()
            if due is not None:
                # an open batch: wait for its dispatch, then claim it
                until = max(until, min(batch_deadline,
                                       now_m + max(0.0, (due - now_ms()) / 1000) + 0.5))
            if now_m >= until:
                break
            if until > registered + 0.05:
                _write_waiter(until)
                registered = until
            time.sleep(CLAIM_POLL_SEC)
    finally:
        if registered:
            _clear_waiter()
    view = claimed if full else [_executor_view(t) for t in claimed]
    if RELAY_ON and not full and view:
        _relay_claim_extras(view, warm_turn)
    out = {"ok": True, "tasks": view}
    if claimed:
        ctx_at = now_ms()
        conv = conversation_context(min(int(t.get("received_at") or now_ms())
                                        for t in claimed), since_ms=context_since)
        if conv:
            out["conversation"] = conv
        st = settings_text(non_default_only=True)
        if st and _changed_since(SETTINGS_FILE, context_since):
            out.setdefault("conversation", {})["settings"] = st
        if context_since:
            out["context_delta"] = True     # only what changed since the cursor
        out["context_cursor"] = ctx_at      # pass back as --context-since
        ids = [t["message_id"] for t in claimed]
        cid = (claimed[-1].get("execution") or {}).get("claim_id")
        if cid:
            # One fencing token per claim (all tasks above share it): pass it
            # as --claim-id to complete / progress / heartbeat.
            out["claim_id"] = cid
        bids = {t.get("batch_id") for t in claimed}
        if len(bids) == 1 and None not in bids:
            out["batch_id"] = next(iter(bids))
        if len(ids) > 1 and len({t.get("from_user_id") for t in claimed}) == 1:
            out["merge_hint"] = (
                f"这 {len(ids)} 条是同一个人连着发的：合成一条完整回复，"
                f"complete --task-id {ids[-1]} --merge-ids {','.join(ids[:-1])}"
                + (f" --claim-id {cid}" if cid else "") + " ...")
            again = [t["message_id"] for t in claimed
                     if int((t.get("execution") or {}).get("supersedes") or 0)]
            if again:
                out["merge_hint"] += (
                    f"（其中 {','.join(again)} 之前处理到一半，用户又发了新消息，"
                    "上一版回复已作废、没有发出：这次一起回答。）")
        last = claimed[-1]
        left = choose_context_token(last.get("from_user_id"),
                                    last.get("context_token"))[1]
        if left is not None:
            out["reply_budget"] = left
    elif wait_s:
        out["waited_s"] = round(wait_s, 1)
    return out


def _wait_arg(v) -> float:
    """--wait / --then-wait: seconds, or "auto" = WECHAT_BRIDGE_WARM_WAIT_S
    (state/tuning.env; default 45, 5..CLAIM_WAIT_MAX_SEC) - the warm window
    can be tuned without editing the hook prompt."""
    if str(v).strip().lower() == "auto":
        return float(max(5, min(CLAIM_WAIT_MAX_SEC, _env_int("WECHAT_BRIDGE_WARM_WAIT_S", 45))))
    return float(v)


def cmd_claim(args) -> int:
    """`claim`: hand pending tasks to the executor (see claim_tasks)."""
    print(json.dumps(claim_tasks(
        limit=args.limit, wait_s=getattr(args, "wait", 0) or 0,
        settle_ms=getattr(args, "settle_ms", None), full=bool(args.full),
        context_since=getattr(args, "context_since", None)), ensure_ascii=False))
    return 0


HANDOFF_PICKUP_SEC = 8
HANDOFF_FINISH_SEC = 90
_TERMINAL_SEND = ("sent", "send_failed", "send_uncertain")


def _handoff_target() -> int | None:
    if os.environ.get("WECHAT_BRIDGE_HANDOFF", "1") == "0":
        return None
    pid = serve_alive()
    if not pid:
        return None
    hb = read_json(HEARTBEAT_FILE, {}) or {}
    if hb.get("handoff_pid") != pid or hb.get("serve_pid") != pid:
        return None       # an older serve: SIGUSR1 would kill it
    return pid


def _send_result(t: dict) -> dict:
    rec = {k: v for k, v in (t.get("send") or {}).items()
           if k not in ("pending", "at", "started_at", "done_upto")}
    rec["ok"] = t.get("status") == "sent"
    return rec


def _wake_serve() -> bool:
    """SIGUSR1 to a serve that advertised the handler: wakes its sender and
    its media-queue worker. False if there is no such serve."""
    pid = _handoff_target()
    if not pid:
        return False
    try:
        os.kill(pid, signal.SIGUSR1)
        return True
    except OSError:
        return False


def deliver(task_id: str) -> dict:
    """Send a waiting_send task, preferably through the running serve: its
    sender wakes on SIGUSR1 and reuses a warm keep-alive connection (the
    typing keepalive keeps one warm), so the reply skips a fresh
    TCP + CONNECT + TLS setup on the critical path. Falls back to sending
    from this process if the serve does not pick it up within 8 s.
    v4: during an iLink -2 deferral the reply is left to the serve, which
    sends it when the deferral ends (v3's CLI fallback ignored it)."""
    deadline = time.monotonic() + HANDOFF_FINISH_SEC
    pid = _handoff_target()
    if pid:
        try:
            os.kill(pid, signal.SIGUSR1)
        except OSError:
            pid = None
    rl = rate_limited_until()
    if pid and rl:
        return {"ok": False, "pending": True, "via": "serve",
                "deferred_until_ms": rl,
                "error": "iLink rate limit (-2): queued; serve sends it when "
                         "the deferral ends or the user writes"}
    if pid:
        pickup = time.monotonic() + HANDOFF_PICKUP_SEC
        while time.monotonic() < deadline:
            t = load_task(task_id) or {}
            if not t:
                return {"ok": False, "error": "task not found"}
            if t.get("status") in _TERMINAL_SEND:
                return {**_send_result(t), "via": "serve"}
            if t.get("status") == "waiting_send" and time.monotonic() > pickup:
                log(f"handoff: serve did not pick up task={task_id} in "
                    f"{HANDOFF_PICKUP_SEC}s; sending directly")
                break
            time.sleep(0.01)
    outcome = attempt_send(task_id)
    if not outcome.get("ok") and "status is sending" in str(outcome.get("error")):
        while time.monotonic() < deadline:       # the serve won the race
            t = load_task(task_id) or {}
            if t.get("status") in _TERMINAL_SEND:
                return {**_send_result(t), "via": "serve"}
            time.sleep(0.1)
        return {"ok": False, "pending": True,
                "error": "serve still sending; check status later"}
    return outcome



def _merge_tasks(main_id: str, main: dict, ids: list,
                 claim_id: str | None = None) -> list:
    """v5 `complete --merge-ids`: messages answered together with main_id (a
    burst of follow-ups) are closed WITHOUT a message of their own - one
    coherent reply instead of one per message, and fewer reply slots used."""
    done_, now = [], now_ms()
    for mid in ids:
        t = load_task(mid)
        if not t or t.get("from_user_id") != main.get("from_user_id") \
                or t.get("status") not in ("executing", "pending_execution") \
                or t.get("batch_open") \
                or _claim_check(t, claim_id) \
                or (claim_id and _superseded_why(t, claim_id)):
            continue
        ex = dict(t.get("execution") or {})
        ex.update(finished_at=now, ok=True, merged_into=main_id)
        t["execution"] = ex
        t["status"] = "sent"
        t["send"] = {"ok": True, "merged_into": main_id, "no_text": True, "at": now}
        save_task(t)
        done_.append(mid)
    if done_:
        log(f"complete task={main_id}: merged {len(done_)} message(s) into one reply")
    return done_


def cmd_complete(args) -> int:
    """Store the executor's reply and send it. v4: refuses an empty reply
    (v3 sent an empty message), refuses to clobber a reply that is being
    sent or partly delivered (v3 restarted it from part 1, duplicating what
    had arrived), and takes --attach files that go out after the text
    through the persistent media queue."""
    if getattr(args, "cont", False):    # 2026-11-07 relay: hand off, send nothing
        return _complete_continue(args)
    if getattr(args, "background", False):    # 2026-11-07 v1.4: hand to the background
        return _complete_background(args)
    def fail(msg: str, **kw) -> int:
        print(json.dumps({"ok": False, "error": msg, **kw}, ensure_ascii=False))
        return 2

    def superseded(t: dict, why: str) -> int:
        print(json.dumps(_superseded_out(t, why), ensure_ascii=False))
        return EXIT_SUPERSEDED
    claim_id = getattr(args, "claim_id", None)
    early = load_task(args.task_id)
    if early and early.get("status") != "cancelled":
        # 2026-11-06 (feature 2): before anything is read, rendered or queued
        why = _superseded_why(early, claim_id)
        if why:
            return superseded(early, why)
    if args.result_file:
        try:
            result_text = Path(args.result_file).read_text(encoding="utf-8",
                                                           errors="replace")
        except OSError as e:
            return fail(f"cannot read --result-file: {e.strerror or e}")
    else:
        result_text = args.result or ""
    attaches = list(getattr(args, "attach", None) or [])
    auto_attached: list = []
    auto_refused: list = []
    if result_text and not args.error and not getattr(args, "raw", False):
        # v5: ![说明](/abs/file) / [名字](/abs/file) pointing at a real local
        # file goes out as an attachment (the text keeps the label); then the
        # Markdown is adapted to what the WeChat client renders.
        if AUTO_ATTACH:
            result_text, found = extract_local_refs(result_text, auto_refused)
            for p_ in found:
                if str(p_) not in attaches:
                    attaches.append(str(p_))
                    auto_attached.append(str(p_))
        result_text = render_for_wechat(result_text)
    merge_ids = [x for x in re.split(r"[,\s]+", getattr(args, "merge_ids", None) or "")
                 if x and x != args.task_id]
    if not args.error and not result_text.strip() and not attaches:
        return fail("empty result: give the reply text (--result / "
                    "--result-file), attach a file (--attach) or report a "
                    "failure (--error). Every task needs a WeChat reply, even "
                    "when the real answer went to the App")
    task = load_task(args.task_id)
    if not task:
        return fail("task not found")
    held, problems = [], []
    for a in attaches:
        try:
            held.append(enqueue_attachment(
                task.get("from_user_id"), a,
                kind=("auto" if a in auto_attached
                      else getattr(args, "attach_as", None) or "auto"),
                task_id=args.task_id, context_token=task.get("context_token"),
                hold=True, gate=True, notify="always"))
        except (AttachError, OSError) as e:
            problems.append({"file": Path(a).name,
                             "code": getattr(e, "code", "error"),
                             "error": str(e)})
    if problems:
        for it in held:
            _drop_qitem(it["qid"])
        return fail("attachment rejected; nothing was sent - fix it and run "
                    "complete again", attachments=problems)
    guard = None
    guard_kw: dict = {}
    gone = None
    replaced = False
    merged: list = []
    with task_lock():
        task = load_task(args.task_id)
        status = (task or {}).get("status")
        if not task:
            guard = "task not found"
        elif status != "cancelled" and _superseded_why(task, claim_id):
            gone = _superseded_why(task, claim_id)   # superseded meanwhile
        elif task.get("batch_open"):
            guard = ("this message is waiting in its batch (state/pending.json) and "
                     "has not been dispatched to you: claim it first")
        elif status == "sent":
            guard = ("task already sent (to add a file afterwards: "
                     "send-media --task-id ID --file F)")
        elif status == "cancelled":
            guard = ("cancelled by the user (「取消」): nothing is sent - stop "
                     "working on this task")
        elif status == "sending":
            guard = ("this task's reply is being sent right now; check "
                     "`bridge.py status` in a moment")
        elif status in ("send_failed", "send_uncertain") \
                and not getattr(args, "force", False):
            guard = ("a reply for this task is already queued or partly "
                     "delivered: `bridge.py send --task-id ID` resumes it, "
                     "`complete --force` replaces it")
        elif status == "background" and (
                getattr(args, "claim_id", None) != (task.get("bg") or {}).get("token")
                or (task.get("bg") or {}).get("state") != "running"):
            guard = ("this long task is with the background: only its current taker may "
                     "answer it (long-tasks finish / fail with its token)")
        elif status == "background" and not args.error \
                and (late := _bg_notes_at_close(args.task_id)):
            guard = ("用户在你上次 progress 之后又补充了要求（unread_notes，现在已算交给你）："
                     "先照着调整结果，再 finish；最终结果里要回应这些补充")
            guard_kw["unread_notes"] = late
        elif _claim_check(task, getattr(args, "claim_id", None)):
            guard = _claim_check(task, getattr(args, "claim_id", None))
        else:
            ex = dict(task.get("execution") or {})
            ex["finished_at"] = now_ms()
            if args.error:
                ex["error"] = args.error
                task["result_text"] = f"任务执行失败：{args.error}"
                late = _bg_notes_at_close(args.task_id) if status == "background" else []
                if late:        # v1.7: follow-ups that came in after `long-tasks fail`
                    task["result_text"] += ("（你后来补充的「" + "；".join(
                        snippet(n["text"], 30) for n in late[:3]) + "」也还没做）")
            else:
                ex["ok"] = True
                task["result_text"] = result_text
            task["execution"] = ex
            if RELAY_ON and task.get("relay"):
                _relay_close(task, "error" if args.error else "complete", ex["finished_at"])
            if status == "background":      # 2026-11-07 v1.4: long-tasks finish / fail
                _bg_close(task, "failed" if args.error else "done", ex["finished_at"])
            if held:
                task["attachments"] = [it["qid"] for it in held]
            replaced = status in ("send_failed", "send_uncertain")
            task["status"] = "waiting_send"
            task["send"] = None
            save_task(task)
            merged = _merge_tasks(args.task_id, task, merge_ids,
                                  getattr(args, "claim_id", None))
            for it in held:
                it["status"] = "pending"
                save_qitem(it)
    if gone or guard:
        for it in held:
            _drop_qitem(it["qid"])
        return superseded(task, gone) if gone else fail(guard, **guard_kw)
    if replaced:
        text_outbox_reconcile(args.task_id)     # --force: the old text is not resent
    dup_warn = []
    if held:
        try:
            for it in held:
                for f in QUEUE_DIR.glob("*.json"):
                    try:
                        q = json.loads(f.read_text(encoding="utf-8"))
                    except (OSError, ValueError):
                        continue
                    if (q.get("qid") != it["qid"] and q.get("status") == "sent"
                            and q.get("name") == it.get("name")
                            and q.get("bytes") == it.get("bytes")
                            and now_ms() - int(q.get("finished_at") or 0) < 7 * 86400_000):
                        dup_warn.append(f"{it['name']}：同名同大小的文件 7 天内已发过，确认不是重复发送")
                        break
        except OSError:
            pass
    outcome = deliver(args.task_id)
    done_at = now_ms()
    out = {"ok": True, "task_id": args.task_id, "send": outcome}
    if dup_warn:
        out["duplicate_warning"] = dup_warn
    if merged:
        out["merged"] = merged
    if auto_attached:
        out["auto_attached"] = [Path(p_).name for p_ in auto_attached]
    if auto_refused:
        # Local files linked in the reply that auto-attach will not send
        # (hidden/private/secret-looking): send with --attach if intended.
        out["auto_attach_refused"] = auto_refused
    if (outcome or {}).get("parked"):
        out["send_note"] = ("超出微信对单条消息的回复条数限制：已发出的部分末尾带了提示，"
                            "剩下的会在用户下一条消息时自动续发，不要重发。")
    if held:
        qids = [it["qid"] for it in held]
        serve_up = _wake_serve()
        if not serve_up:
            for q in qids:
                process_queue_item(q)
        out["attachments"] = wait_queue_items(
            qids, float(getattr(args, "wait_attachments", 10) or 0))
        if any(a.get("status") not in _QUEUE_TERMINAL for a in out["attachments"]):
            out["attachments_note"] = (
                "未送达的附件由桥接在后台继续重试（约 18 分钟）；最终失败时桥接会直接"
                "告诉用户，执行者不用再另发消息。"
                + ("" if serve_up else "（serve 没在运行：它启动后才会继续重试）"))
    then_wait = float(getattr(args, "then_wait", 0) or 0)
    if then_wait > 0:
        # 2026-11-05: complete + wait for the follow-up in ONE call - the
        # agent saves a whole model round trip per turn, and the wait starts
        # the moment the reply is out. `next` is a claim result whose context
        # is only what is new since this reply.
        out["next"] = claim_tasks(limit=int(getattr(args, "then_limit", 5) or 5),
                                  wait_s=then_wait, context_since=done_at)
    print(json.dumps(out, ensure_ascii=False))
    return 0


def cmd_send(args) -> int:
    # A manual send is a human decision (EXECUTOR.md: resend only after the
    # user confirms), so it may resume send_uncertain / exhausted tasks.
    # Parts already confirmed are never sent again; the part that was in
    # flight goes out again with its original client_id.
    outcome = attempt_send(args.task_id, allow_retry=True, force=True)
    print(json.dumps(outcome, ensure_ascii=False))
    return 0 if outcome.get("ok") else 1


def _resolve_target(args) -> tuple:
    """(to_user, context_token, error) for the send-* commands. v4: with
    neither --task-id nor --to-user the bound user is the recipient."""
    creds = load_creds()
    if not creds:
        return None, None, "no credentials"
    to_user = getattr(args, "to_user", None)
    ctx = getattr(args, "context_token", None)
    if getattr(args, "task_id", None):
        task = load_task(args.task_id)
        if not task:
            return None, None, "task not found"
        to_user = to_user or task.get("from_user_id")
        ctx = ctx or task.get("context_token")
    to_user = to_user or creds.get("ilink_user_id")
    if not to_user:
        return None, None, "need --task-id or --to-user"
    if not ctx:
        ctx = latest_context_token(to_user)[0]
    return to_user, ctx, None


def _superseded_target(args) -> int | None:
    """send-media / send-image|file|video --task-id for a task whose reply
    was superseded (feature 2): print the exit-4 result, return 4; else
    None. With --claim-id the check is exact; without it a task that was
    ever superseded is refused (see _superseded_why)."""
    tid = getattr(args, "task_id", None)
    t = load_task(tid) if tid else None
    if not t or t.get("status") == "cancelled":
        return None
    why = _superseded_why(t, getattr(args, "claim_id", None))
    if not why:
        return None
    print(json.dumps(_superseded_out(t, why), ensure_ascii=False))
    return EXIT_SUPERSEDED


def _legacy_send(args, kind: str) -> int:
    """send-image / send-file / send-video (v3 commands, same arguments):
    one synchronous round through the media queue. If it fails before
    anything could have been delivered, the file stays queued for background
    retries (unless --no-queue) instead of being lost."""
    rc = _superseded_target(args)
    if rc is not None:
        return rc
    to_user, ctx, err = _resolve_target(args)
    if err:
        print(json.dumps({"ok": False, "error": err}, ensure_ascii=False))
        return 2
    try:
        it = enqueue_attachment(to_user, args.file, kind=kind,
                                caption=args.caption, task_id=args.task_id,
                                context_token=ctx, claim=True,
                                notify="background", waiter_s=600)
    except (AttachError, OSError) as e:
        print(json.dumps({"ok": False, "error": f"business: {e}"},
                         ensure_ascii=False))
        return 1
    it = process_queue_item(it["qid"], owned=True) or load_qitem(it["qid"]) or it
    pub = _qitem_public(it)
    if it.get("status") == "sent":
        print(json.dumps({"ok": True, **pub, "file_size": it.get("bytes")},
                         ensure_ascii=False))
        return 0
    if it.get("status") == "pending":
        if getattr(args, "no_queue", False):
            _cancel_qitem(it["qid"])
            print(json.dumps({"ok": False, "queued": False, **pub,
                              "status": "cancelled"}, ensure_ascii=False))
            return 1
        with task_lock():
            cur = load_qitem(it["qid"])
            if cur:
                cur["waiter_until"] = 0      # from now on the bridge reports
                save_qitem(cur)
        _wake_serve()
        print(json.dumps({"ok": False, "queued": True, **pub,
                          "note": "这一轮没发出去，已转入后台队列（约 18 分钟内多次重试）；"
                                  "最终失败时桥接会直接告诉用户。"}, ensure_ascii=False))
        return 1
    print(json.dumps({"ok": False, **pub}, ensure_ascii=False))
    return 1


def cmd_send_image(args) -> int:
    return _legacy_send(args, "image")


def cmd_send_file(args) -> int:
    return _legacy_send(args, "file")


def cmd_send_video(args) -> int:
    return _legacy_send(args, "video")


def cmd_send_media(args) -> int:
    """v4 one-stop outbound media: one or more files, kind auto-detected
    (images/videos by extension, anything else as a file) or forced with
    --as (voice = a WeChat voice note). Queued with background retries;
    waits up to --wait seconds and reports each file."""
    rc = _superseded_target(args)
    if rc is not None:
        return rc
    to_user, ctx, err = _resolve_target(args)
    if err:
        print(json.dumps({"ok": False, "error": err}, ensure_ascii=False))
        return 2
    wait_s = float(args.wait or 0)
    rejected, qids = [], []
    for i, f in enumerate(args.file):
        try:
            it = enqueue_attachment(to_user, f, kind=args.as_kind,
                                    caption=args.caption if i == 0 else None,
                                    task_id=args.task_id, context_token=ctx,
                                    notify="background", waiter_s=wait_s)
            qids.append(it["qid"])
        except (AttachError, OSError) as e:
            rejected.append({"file_name": Path(f).name, "status": "rejected",
                             "code": getattr(e, "code", "error"),
                             "error": str(e)})
    if qids and not _wake_serve():
        for q in qids:
            process_queue_item(q)
    items = wait_queue_items(qids, wait_s) if qids else []
    res = {"ok": bool(items) and not rejected
           and all(x.get("status") == "sent" for x in items),
           "items": items + rejected}
    if any(x.get("status") == "pending" for x in items):
        res["note"] = "未送达的在后台继续重试（约 18 分钟），最终失败时桥接会直接告诉用户。"
    print(json.dumps(res, ensure_ascii=False))
    return 0 if res["ok"] else 1


PROGRESS_NOTIFY_MIN_MS = 60_000


def _touch_claim_group(t: dict, now: int) -> int:
    """2026-11-06: a batch is claimed as one unit (one claim_id) and answered
    in one round, but progress / heartbeat name one task. Give the others of
    the same claim the same heartbeat and long-step lease - otherwise they
    would look silent and be reclaimed after 10 minutes while the round is
    alive (a duplicate reply), and auto-progress would warn for each. Caller
    holds task_lock and saves `t` itself."""
    ex0 = t.get("execution") or {}
    cid = ex0.get("claim_id")
    if not cid:
        return 0
    n = 0
    for s_ in scan_task_index():
        if s_.get("claim_id") != cid or s_["message_id"] == t.get("message_id") \
                or s_.get("status") != "executing":
            continue
        o = load_task(s_["message_id"])
        ex = dict((o or {}).get("execution") or {})
        if not o or o.get("status") != "executing" or ex.get("claim_id") != cid:
            continue
        ex["heartbeat_at"] = now
        if int(ex0.get("hold_until") or 0) > int(ex.get("hold_until") or 0):
            ex["hold_until"] = ex0["hold_until"]
            ex["hold_minutes"] = ex0.get("hold_minutes")
        o["execution"] = ex
        save_task(o)
        n += 1
    return n


def cmd_progress(args) -> int:
    """Executor heartbeat for a long task: keeps it from being reclaimed
    after 10 minutes (and keeps "typing..." on); --notify --note sends the
    note to the user (at most once a minute). 2026-11-02: --stage records
    what step the executor is on (e.g. "正在查 bridge.py 的进度逻辑");
    a notified stage note says what is happening and how long it has
    taken, never "好了马上发你" - the old auto-progress wording promised
    an imminent finish it could not keep."""
    note = (args.note or "").strip()
    stage = (getattr(args, "stage", None) or "").strip()
    hold_min = getattr(args, "hold_minutes", None)
    now = now_ms()
    with task_lock():
        t = load_task(args.task_id)
        if not t:
            print(json.dumps({"ok": False, "error": "task not found"},
                             ensure_ascii=False))
            return 2
        if t.get("status") == "cancelled":
            print(json.dumps({"ok": False, "cancelled": True,
                              "error": "the user cancelled this task (「取消」): stop "
                                       "now; no complete needed"}, ensure_ascii=False))
            return 3
        gone = _superseded_why(t, getattr(args, "claim_id", None))
        if gone:            # 2026-11-06: no note goes out for an abandoned reply
            print(json.dumps(_superseded_out(t, gone), ensure_ascii=False))
            return EXIT_SUPERSEDED
        why = _readopt_or_stale(t, getattr(args, "claim_id", None), now)
        if why:
            print(json.dumps({"ok": False, "stale_claim": True, "error": why},
                             ensure_ascii=False))
            return 3
        if t.get("status") != "executing":
            print(json.dumps({"ok": False, "error": f"task status is "
                              f"{t.get('status')} (progress applies while "
                              "executing)"}, ensure_ascii=False))
            return 2
        ex = dict(t.get("execution") or {})
        ex["heartbeat_at"] = now
        _note_call(ex, now)
        relay_info = None
        if any(getattr(args, k, None) for k in ("plan", "plan_from", "step_done", "artifact")):
            # 2026-11-07 relay (EXECUTOR-relay.md): plan / finished steps / artifacts
            relay_info, rerr = _relay_progress(
                t, ex, now, getattr(args, "plan", None), getattr(args, "plan_from", None),
                getattr(args, "step_done", None), list(getattr(args, "artifact", None) or []))
            if rerr:
                print(json.dumps({"ok": False, "error": rerr}, ensure_ascii=False))
                return 2
            stage = stage or relay_info.get("stage") or ""
        if note:
            ex["note"] = note[:300]
        if stage:
            ex["stage"] = stage[:200]
            ex["stage_at"] = now
        # Opt round 2 item 2: long-step lease (1-60 min, requires --stage).
        # Reclaim skips the task until the lease expires; afterwards the
        # normal 10-min rule applies from the last heartbeat.
        if hold_min is not None:
            if not stage:
                print(json.dumps({"ok": False, "error":
                                  "hold-minutes requires --stage"},
                                 ensure_ascii=False))
                return 2
            try:
                hold_min = max(1, min(60, int(hold_min)))
            except (TypeError, ValueError):
                print(json.dumps({"ok": False, "error":
                                  "hold-minutes must be an integer"},
                                 ensure_ascii=False))
                return 2
            ex["hold_until"] = now + hold_min * 60_000
            ex["hold_minutes"] = hold_min
        notify = bool(args.notify and (note or stage) and now - int(
            ex.get("progress_notified_at") or 0) > PROGRESS_NOTIFY_MIN_MS)
        if notify:
            ex["progress_notified_at"] = now
            if t.get("relay") is not None:
                t["relay"]["notified_at"] = now
        if relay_info:
            for p_ in relay_info.pop("_save", None) or []:
                plan_save(p_)
            with contextlib.suppress(OSError):
                _plan_workdir(args.task_id).mkdir(mode=0o700, exist_ok=True)
        t["execution"] = ex
        save_task(t)
        _touch_claim_group(t, now)
        user = t.get("from_user_id")
        ctx_tok = t.get("context_token")
    out = {"ok": True, "task_id": args.task_id, "heartbeat_at": now}
    if relay_info:
        out["plan"] = relay_info["summary"]
    _hold = (load_task(args.task_id) or {}).get("execution", {}).get("hold_until")
    if _hold:
        out["hold_until"] = _hold
    if args.notify and (note or stage) and not notify:
        out["notify_skipped"] = "at most one progress message per minute"
    if notify and not settings_get().get("progress_notes", True):
        notify = False
        out["notify_skipped"] = "progress notes turned off by 设置 进度关"
    if notify:
        left = choose_context_token(user, ctx_tok)[1]
        if left is not None and left <= QUOTA_RESERVE["progress"] + 1:
            notify = False
            out["notify_skipped"] = ("only the last reply slots of this message are "
                                     "left: kept for the final answer")
    if notify:
        if note:
            send_note = note
        elif relay_info and relay_info.get("outline"):
            send_note = relay_info["outline"]
        else:
            took = fmt_duration(now - int(
                (load_task(args.task_id) or {}).get("received_at") or now))
            send_note = f"还在处理：{stage}（已经 {took}），做完这步会继续，不会只说马上就好。"
        nid = create_notice(user, render_for_wechat(send_note), reason="progress",
                            about={"task_id": args.task_id})
        out["notice"] = deliver(nid) if nid else {"ok": False,
                                                  "error": "notice suppressed"}
    print(json.dumps(out, ensure_ascii=False))
    return 0


def cmd_heartbeat(args) -> int:
    """Resume 2026-11-02: cheap heartbeat, no message to the user.
    progress --stage already renews the heartbeat, but its name hides
    that job; heartbeat is the explicit 'I am still alive' call the
    hook prompt now requires at least every 5 minutes and before any
    long step. A task with no heartbeat for 10 minutes is reclaimed."""
    stage = (getattr(args, "stage", None) or "").strip()
    now = now_ms()
    with task_lock():
        t = load_task(args.task_id)
        if not t:
            print(json.dumps({"ok": False, "error": "task not found"},
                             ensure_ascii=False))
            return 2
        gone = _superseded_why(t, getattr(args, "claim_id", None))
        if gone and t.get("status") != "cancelled":
            print(json.dumps(_superseded_out(t, gone), ensure_ascii=False))
            return EXIT_SUPERSEDED
        why = _readopt_or_stale(t, getattr(args, "claim_id", None), now)
        if why:
            print(json.dumps({"ok": False, "stale_claim": True, "error": why},
                             ensure_ascii=False))
            return 3
        if t.get("status") != "executing":
            print(json.dumps({"ok": False, "error": f"task status is "
                              f"{t.get('status')} (heartbeat applies while "
                              f"executing)"}, ensure_ascii=False))
            return 2
        ex = dict(t.get("execution") or {})
        ex["heartbeat_at"] = now
        _note_call(ex, now)
        if stage:
            ex["stage"] = stage[:200]
            ex["stage_at"] = now
        t["execution"] = ex
        save_task(t)
        _touch_claim_group(t, now)
    print(json.dumps({"ok": True, "task_id": args.task_id,
                      "heartbeat_at": now,
                      "next_heartbeat_within_ms": 5 * 60 * 1000},
                     ensure_ascii=False))
    return 0


def cmd_outline(args) -> int:
    """Opt round 2 item 6: function/class index of bridge.py, so an
    executor locates code without reading all ~7900 lines."""
    import re as _re
    rows = []
    for i, line in enumerate(Path(__file__).read_text(encoding="utf-8").splitlines(), 1):
        m = _re.match(r"^(class|def) ([A-Za-z_][A-Za-z0-9_]*)", line)
        if m:
            rows.append({"line": i, "kind": m.group(1), "name": m.group(2)})
    if getattr(args, "search", None):
        needle = args.search.lower()
        rows = [r for r in rows if needle in r["name"].lower()]
    print(json.dumps({"ok": True, "count": len(rows), "entries": rows},
                     ensure_ascii=False))
    return 0


def cmd_history(args) -> int:
    if getattr(args, "search", None):
        needle = args.search.strip().lower()
        hits, _ = history_search(needle, args.limit or 0, max_bytes=None)
        if args.json:
            print(json.dumps({"ok": True, "search": args.search, "turns": hits},
                             ensure_ascii=False))
            return 0
        for r in hits:
            who = "用户" if r.get("role") == "user" else "Muse"
            print(f"{user_clock(r.get('at'), with_date=True)} {who}: "
                  f"{snippet(r.get('text'), 120)}")
        if not hits:
            print(f"(history 中没有含“{args.search}”的对话；"
                  "摘要里也可用 history --search 再找)")
        return 0
    since = now_ms() - int(args.hours * 3_600_000) if args.hours else None
    rows = history_read(limit=args.limit, since_ms=since)
    if args.json:
        print(json.dumps({"ok": True, "turns": rows}, ensure_ascii=False))
        return 0
    for r in rows:
        who = "用户" if r.get("role") == "user" else "Muse"
        print(f"{user_clock(r.get('at'), with_date=True)} {who}: {r.get('text')}")
    if not rows:
        print("(no history in this window)")
    return 0


DECISION_ITEM_MAX_CHARS = 500
DECISIONS_LOCK = STATE_DIR / "decisions.lock"


def cmd_decisions(args) -> int:
    """Decision list (opt batch item 2): show, or append one in-progress item."""
    if getattr(args, "add", None) is not None:
        # Decisions are single-line bullets: a line break inside --add could
        # smuggle in a "## ..." heading and break the file structure.
        # 2026-11-04 review: every kind of line break (v5.0.5 missed \v \f
        # \x85 \u2028 \u2029 ...), control and bidi characters, a length cap,
        # and a lock (two --add at once lost one of them).
        item = one_line(args.add)
        if not item:
            print(json.dumps({"ok": False, "error": "empty decision"},
                             ensure_ascii=False))
            return 2
        if len(item) > DECISION_ITEM_MAX_CHARS:
            print(json.dumps({"ok": False, "error": (
                f"decision too long ({len(item)} > {DECISION_ITEM_MAX_CHARS} "
                "chars): one line - details belong in the memo or a file")},
                ensure_ascii=False))
            return 2
        with _flock(DECISIONS_LOCK):
            text = _decisions_read()
            marker = "\n## 已决定"
            entry = f"\n- {item}\n"
            if marker in text:
                text = text.replace(marker, entry + marker, 1)
            else:
                text = text.rstrip() + "\n\n## 进行中\n" + entry
            atomic_write_text(DECISIONS_FILE, text)
        print(json.dumps({"ok": True, "added": item}, ensure_ascii=False))
        return 0
    print(_decisions_read(), end="")
    return 0


def cmd_memo(args) -> int:
    if args.clear:
        memo_set("")
        print(json.dumps({"ok": True, "memo": ""}, ensure_ascii=False))
        return 0
    if args.set is not None or args.set_file:
        try:
            text = (args.set if args.set is not None else
                    Path(args.set_file).read_text(encoding="utf-8",
                                                  errors="replace"))
        except OSError as e:
            print(json.dumps({"ok": False, "error": str(e)}, ensure_ascii=False))
            return 2
        saved = memo_set(text)
        print(json.dumps({"ok": True, "chars": len(saved),
                          "truncated": len(text.strip()) > len(saved)},
                         ensure_ascii=False))
        return 0
    print(json.dumps({"ok": True, "memo": memo_get()}, ensure_ascii=False))
    return 0


def cmd_queue(args) -> int:
    if args.retry:
        ok = _mark_qitem_for_resend(args.retry)
        if ok:
            _wake_serve()
        print(json.dumps({"ok": ok}, ensure_ascii=False))
        return 0 if ok else 1
    if args.cancel:
        ok = _cancel_qitem(args.cancel)
        print(json.dumps({"ok": ok}, ensure_ascii=False))
        return 0 if ok else 1
    items = []
    for it in list_queue():
        pub = _qitem_public(it)
        pub["created"] = user_clock(it.get("created_at"), with_date=True)
        if it.get("task_id"):
            pub["task_id"] = it["task_id"]
        items.append(pub)
    print(json.dumps({"ok": True, "counts": queue_counts(), "items": items},
                     ensure_ascii=False, indent=None if args.json else 1))
    return 0


def cmd_flush(args) -> int:
    print(json.dumps({"ok": True, "results": flush_outbox()}, ensure_ascii=False))
    return 0


def serve_alive() -> int | None:
    try:
        pid_val = int(PID_FILE.read_text().strip())
        os.kill(pid_val, 0)
        # Guard against PID reuse: the process must actually be our
        # serve, not an unrelated process that inherited the number.
        cmdline = Path(f"/proc/{pid_val}/cmdline").read_bytes() \
            .replace(b"\0", b" ").decode(errors="replace")
        if "bridge.py" not in cmdline or "serve" not in cmdline:
            return None
        return pid_val
    except (OSError, ValueError):
        return None



def _status_v5(creds, hb) -> dict:
    user = (creds or {}).get("ilink_user_id")
    last_in, open_, rem = window_state(user)
    left = choose_context_token(user, None)[1] if user else None
    return {
        "transport_mode": hb.get("transport_mode"),
        "markdown_mode": MARKDOWN_MODE,
        "reply_window": {"last_inbound_at": last_in, "open": open_,
                         "remaining_min": round(rem / 60000) if rem is not None else None},
        "reply_budget": {"per_message": CTX_QUOTA, "left_on_latest": left},
        "parked_sends": _deferred_items(user),
        "batch": batch_summary(),               # state/pending.json
        "text_outbox": text_outbox_summary(),   # state/outbox.json (texts only)
        "quote_store_rows": quote_count(),
        "claim_waiter": _waiter_alive(),
        "dedupe_confirmed": _dedupe_confirmed(),
        "net_fast_retries": hb.get("net_fast_retries"),
        "net_resumed_downloads": hb.get("net_resumed"),
        "net_dns_fallbacks": hb.get("net_dns_fallbacks"),
        "poll_client_timeouts": hb.get("client_timeout_streak"),
        "ingest_fail_streak": hb.get("ingest_fail_streak"),
        "threads_age_s": hb.get("threads_age_s"),
        "latency_ms": latency_stats(),
    }


def cmd_status(args) -> int:
    creds = load_creds()
    counts: dict[str, int] = {}
    for t in scan_task_index():
        counts[t["status"]] = counts.get(t["status"], 0) + 1
    hb = read_json(HEARTBEAT_FILE, {}) or {}
    session = read_json(SESSION_FILE, {}) or {}
    cursor = (read_json(CURSOR_FILE, {}) or {}).get("get_updates_buf", "") or ""
    status = {
        "version": BRIDGE_VERSION,
        "connected": bool(creds),
        "ilink_bot_id": mask(creds.get("ilink_bot_id")) if creds else None,
        "ilink_user_id": mask(creds.get("ilink_user_id")) if creds else None,
        "baseurl": (creds.get("baseurl") or DEFAULT_BASE_URL) if creds else None,
        "baseurl_allowed": (_api_base_allowed(creds.get("baseurl") or DEFAULT_BASE_URL)
                            if creds else None),
        "saved_at": creds.get("saved_at") if creds else None,
        "serve_pid": serve_alive(),
        "cursor_bytes": len(cursor),
        "session_paused_until_ms": session.get("paused_until_ms"),
        "tasks_by_status": counts,
        "last_poll_ok_at": hb.get("last_poll_ok_at"),
        "last_inbound_at": hb.get("last_inbound_at"),
        "last_send_ok_at": hb.get("last_send_ok_at"),
        "proxy_id": hb.get("proxy_id"),
        "proxy_rotations": hb.get("proxy_rotations"),
        "net_connects": hb.get("net_connects"),
        "net_reused": hb.get("net_reused"),
        "typing_sent": hb.get("typing_sent"),
        "rate_limited_until_ms": rate_limited_until() or None,
        "send_chunk_limit": _chunk_limit(),
        "queue": queue_counts(),
        "voice_codec": silk_backend()[0],
        "ffmpeg": bool(ffmpeg_path()),
        "memo_chars": len(memo_get()),
        "login_status": (read_json(LOGIN_STATUS_FILE, {}) or {}).get("status"),
        "auth_alert": read_json(STATE_DIR / "auth_alert.json"),
    }
    status.update(_status_v5(creds, hb))
    print(json.dumps(status, ensure_ascii=False, indent=2))
    return 0


PROXY_ENV_FILE = STATE_DIR / "proxy.env"
PROXY_VARS = ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
              "all_proxy", "ALL_PROXY", "no_proxy", "NO_PROXY")


def cmd_write_proxy_env(args) -> int:
    """Copy this process's proxy variables into state/proxy.env (0600)
    so the systemd service (which starts with a clean environment and
    cannot reach the internet directly in this runtime) can use the
    same egress proxy. Values are never printed."""
    lines, written, rejected = [], [], []
    for name in PROXY_VARS:
        value = os.environ.get(name)
        if not value:
            continue
        # 2026-11-04 review: proxy.env is the EnvironmentFile of a ROOT
        # service. A value with a line break (or a quote/backslash, which
        # systemd's parser interprets) could add arbitrary variables to it -
        # LD_PRELOAD=, PYTHONPATH= ... - so such values are refused.
        if not value.isprintable() or any(c in value for c in "\\\"'"):
            rejected.append(name)
            continue
        lines.append(f"{name}={value}")
        written.append(name)
    if not written:
        print(json.dumps({"ok": False,
                          "error": "no usable proxy variables in this environment",
                          **({"rejected_vars": rejected} if rejected else {})},
                         ensure_ascii=False))
        return 2
    atomic_write_text(PROXY_ENV_FILE, "\n".join(lines) + "\n")
    out = {"ok": True, "written_vars": written, "file": str(PROXY_ENV_FILE)}
    if rejected:
        out["rejected_vars"] = rejected
    print(json.dumps(out, ensure_ascii=False))
    return 0


def cmd_stop(args) -> int:
    pid_val = serve_alive()
    if not pid_val:
        print(json.dumps({"ok": True, "note": "serve not running"},
                         ensure_ascii=False))
        return 0
    os.kill(pid_val, signal.SIGTERM)
    for _ in range(50):
        if not serve_alive():
            break
        time.sleep(0.1)
    if not serve_alive():
        # Deliberate stop (not a restart): tell the server explicitly.
        creds = load_creds()
        if creds:
            notify(creds, start=False)
    print(json.dumps({"ok": True, "stopped_pid": pid_val,
                      "still_alive": bool(serve_alive())}, ensure_ascii=False))
    return 0



def scale_stats() -> dict:
    """2026-11-02 六-5: one scale line so '膨胀了' is a number, not a
    feeling. Read-only, never raises. claim_context_chars is the
    conversation_context() proxy for the next claim (recent turns clipped
    to CONTEXT_CHARS + memo + past topics); it is a character proxy, not
    a token-billing figure - there is no billing meter here."""
    out: dict = {"inbox_files": 0, "inbox_bytes": 0, "history_lines": 0,
                 "history_bytes": 0, "archive_files": 0, "archive_lines": 0,
                 "archive_bytes": 0, "quote_rows": 0, "backup_entries": 0,
                 "backup_bytes": 0, "claim_context_chars": 0}
    try:
        for p in INBOX_DIR.glob("*.json"):
            out["inbox_files"] += 1
            out["inbox_bytes"] += p.stat().st_size
    except OSError:
        pass
    try:
        if HISTORY_FILE.exists():
            raw = HISTORY_FILE.read_bytes()
            out["history_bytes"] = len(raw)
            out["history_lines"] = raw.count(b"\n")
    except OSError:
        pass
    try:
        for p in HISTORY_DIR.glob("archive-*.jsonl"):
            raw = p.read_bytes()
            out["archive_files"] += 1
            out["archive_bytes"] += len(raw)
            out["archive_lines"] += raw.count(b"\n")
    except OSError:
        pass
    try:
        out["quote_rows"] = max(0, int(quote_count()))
    except Exception:
        pass
    try:
        for p in (BASE_DIR / "backups").iterdir():
            out["backup_entries"] += 1
            if p.is_file():
                out["backup_bytes"] += p.stat().st_size
            else:
                for f in p.rglob("*"):
                    if f.is_file():
                        out["backup_bytes"] += f.stat().st_size
    except OSError:
        pass
    try:
        ctx = conversation_context(None) or {}
        chars = sum(len(x) for x in ctx.get("recent", []))
        chars += len(ctx.get("memo", ""))
        chars += sum(len(x) for x in ctx.get("past_topics", []))
        out["claim_context_chars"] = chars
        out["claim_context_limit_chars"] = CONTEXT_CHARS
    except Exception:
        pass
    return out


def cmd_stats(args) -> int:
    """Latency per stage (p50/p95, ms) and what the server actually allowed
    when it refused a send early (reply-budget calibration).
    `--task-id ID` / `--recent N` (Step 0, 2026-11-05): print the per-task
    execution timeline instead (task_evidence)."""
    ids = [x.strip() for raw in (getattr(args, "task_id", None) or [])
           for x in str(raw).split(",") if x.strip()]
    recent = int(getattr(args, "recent", 0) or 0)
    if ids or recent > 0:
        if recent > 0:
            ids += [i for i in recent_task_ids(recent) if i not in ids]
        rows = [task_evidence(i) for i in ids]
        ok = all("error" not in r for r in rows)
        print(json.dumps({"ok": ok, "tasks": rows}, ensure_ascii=False, indent=1))
        return 0 if ok else 2
    q = _quota_load()
    observed = sorted({int(v["observed_limit"]) for v in q["tokens"].values()
                       if isinstance(v, dict) and v.get("observed_limit") is not None})
    print(json.dumps({"ok": True, "latency_ms": latency_stats(hours=args.hours),
                      "reply_budget": {"per_message": CTX_QUOTA,
                                       "observed_limits": observed},
                      "scale": scale_stats(),
                      # 2026-11-05: data for tuning, not opinions
                      "evidence": {"exec": exec_breakdown(hours=args.hours),
                                   "followup_gaps": followup_gaps(),
                                   "hook_ticks": hook_tick_stats(hours=min(args.hours, 72)),
                                   "echo": echo_stats(),
                                   "tuning": {k: v for k, v in sorted(os.environ.items())
                                              if k.startswith("WECHAT_BRIDGE_")}}},
                     ensure_ascii=False, indent=1))
    return 0


def cmd_canary_dedupe(args) -> int:
    """Does WeChat drop a second sendmessage that reuses a client_id? Sends
    one test line twice with the SAME client_id (2 reply slots); the user
    says whether one or two arrived; `--confirm yes|no` records it. Only a
    confirmed yes lets the bridge resend an uncertain part by itself."""
    if args.confirm:
        ok = args.confirm == "yes"
        atomic_write_json(DEDUPE_FILE, {"confirmed": ok, "at": now_ms()})
        print(json.dumps({"ok": True, "dedupe_confirmed": ok}, ensure_ascii=False))
        return 0
    creds = load_creds()
    user = (creds or {}).get("ilink_user_id")
    if not user:
        print(json.dumps({"ok": False, "error": "no credentials"}, ensure_ascii=False))
        return 2
    if window_state(user)[1] is False:
        print(json.dumps({"ok": False, "context_expired": True,
                          "error": "24 h window closed: ask the user to send any "
                                   "message first"}, ensure_ascii=False))
        return 3
    ctx, left = choose_context_token(user, None)
    if left is not None and left < 3:
        print(json.dumps({"ok": False, "error": "not enough reply budget left (needs 2)"},
                         ensure_ascii=False))
        return 3
    cid = new_client_id()
    text = ("【桥接自检】去重测试：这一行用同一个 client_id 发了两次。"
            "只看到一条说明微信会自动去重，看到两条说明不会。")
    results = []
    for i in range(2):
        if not quota_take(ctx):
            break
        try:
            send_text(creds, user, text, ctx, client_id=cid)
            results.append("ok")
        except ApiError as e:
            if e.kind == "connect":
                quota_refund(ctx)
            results.append(f"{e.kind}:{e.code}")
        time.sleep(1.5)
    print(json.dumps({"ok": True, "sends": results,
                      "next": "ask the user: one line arrived -> `bridge.py canary-dedupe "
                              "--confirm yes`; two -> `--confirm no`"}, ensure_ascii=False))
    return 0


def cmd_voice_to_wav(args) -> int:
    """Executor helper: WeChat SILK (or amr/mp3/ogg) -> WAV for speech-to-text."""
    src = Path(args.input).absolute()   # never an ffmpeg "proto:" URL
    dst = Path(args.output).absolute() if args.output else src.with_suffix(".wav")
    try:
        with open(src, "rb") as f:
            head = f.read(12)
        if head.lstrip(b"\x02").startswith(SILK_STD_MAGIC):
            info = silk_to_wav(src, dst)
        elif audio_to_wav(src, dst):
            info = {}
        else:
            raise VoiceCodecError("no_ffmpeg" if not ffmpeg_path() else "bad_audio",
                                  "ffmpeg could not convert it")
    except (VoiceCodecError, OSError) as e:
        print(json.dumps({"ok": False, "code": getattr(e, "code", "error"),
                          "error": str(e)}, ensure_ascii=False))
        return 1
    print(json.dumps({"ok": True, "wav": str(dst), **info}, ensure_ascii=False))
    return 0


def cmd_video_audio(args) -> int:
    """Executor helper, 2026-11-02: extract a video's audio track to a
    16 kHz mono WAV for speech-to-text, instead of the old 'video sound
    cannot be transcribed' dead end. First probes for an audio stream so
    a silent video reports has_audio=false (not a failed transcription).
    The WAV itself is only the raw material: this VM has no STT engine
    installed, so transcription still needs one; do not claim a transcript
    from this command alone."""
    src = Path(args.input).absolute()   # never an ffmpeg "proto:" URL
    dst = Path(args.output).absolute() if args.output else src.with_suffix(".wav")
    ff = ffmpeg_path()
    if not ff:
        print(json.dumps({"ok": False, "code": "no_ffmpeg",
                          "error": "ffmpeg not found"}, ensure_ascii=False))
        return 1
    if not src.is_file():
        print(json.dumps({"ok": False, "code": "missing",
                          "error": f"no such file: {src}"}, ensure_ascii=False))
        return 1
    probe = shutil.which("ffprobe")
    has_audio = None
    if probe:
        try:
            p = subprocess.run([probe, "-hide_banner", "-loglevel", "error",
                                "-select_streams", "a", "-show_entries",
                                "stream=codec_name", "-of", "csv=p=0",
                                str(src)], stdin=subprocess.DEVNULL, capture_output=True, timeout=60)
            has_audio = bool(p.stdout.strip())
        except (OSError, subprocess.SubprocessError):
            has_audio = None
    if has_audio is False:
        print(json.dumps({"ok": True, "has_audio": False, "wav": None,
                          "note": "video has no audio stream"},
                         ensure_ascii=False))
        return 0
    try:
        p = subprocess.run([ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
                            "-i", str(src), "-vn", "-ac", "1", "-ar", "16000",
                            str(dst)], stdin=subprocess.DEVNULL, capture_output=True, timeout=600)
    except (OSError, subprocess.SubprocessError) as e:
        print(json.dumps({"ok": False, "code": "ffmpeg_error", "error": str(e)},
                         ensure_ascii=False))
        return 1
    if p.returncode != 0 or not dst.is_file() or dst.stat().st_size == 0:
        print(json.dumps({"ok": False, "code": "no_audio",
                          "error": "ffmpeg extracted no audio; the video "
                                   "may have no audio track"},
                         ensure_ascii=False))
        return 1
    os.chmod(dst, 0o600)
    print(json.dumps({"ok": True, "has_audio": True, "wav": str(dst),
                      "bytes": dst.stat().st_size}, ensure_ascii=False))
    return 0


def cmd_video_frames(args) -> int:
    """Executor helper, 2026-11-02 (二, minus local STT): keyframes of a
    video by scene change instead of a fixed 4-6 uniform frames, so long,
    fast-cut or subtitle-heavy videos lose less plot. If scene detection
    finds fewer than 3 frames, uniform frames fill up to --max-frames.
    Returns the frame paths for the executor to actually look at."""
    import tempfile
    src = Path(args.input).absolute()   # never an ffmpeg "proto:" URL
    ff = ffmpeg_path()
    if not ff:
        print(json.dumps({"ok": False, "code": "no_ffmpeg",
                          "error": "ffmpeg not found"}, ensure_ascii=False))
        return 1
    if not src.is_file():
        print(json.dumps({"ok": False, "code": "missing",
                          "error": f"no such file: {src}"}, ensure_ascii=False))
        return 1
    outdir = Path(args.out_dir).absolute() if args.out_dir else Path(
        tempfile.mkdtemp(prefix="vframes-"))
    outdir.mkdir(parents=True, exist_ok=True)
    duration = None
    probe = shutil.which("ffprobe")
    if probe:
        try:
            p = subprocess.run([probe, "-hide_banner", "-loglevel", "error",
                                "-show_entries", "format=duration",
                                "-of", "csv=p=0", str(src)],
                               stdin=subprocess.DEVNULL, capture_output=True, timeout=60)
            duration = float(p.stdout.decode().strip())
        except (OSError, subprocess.SubprocessError, ValueError):
            duration = None
    frames: list[str] = []
    try:
        p = subprocess.run([ff, "-nostdin", "-hide_banner", "-loglevel", "error", "-y",
                            "-i", str(src), "-vf",
                            f"select='gt(scene,{args.threshold})',scale=1280:-1",
                            "-vsync", "vfr", "-frames:v", str(args.max_frames),
                            str(outdir / "scene-%02d.jpg")],
                           stdin=subprocess.DEVNULL, capture_output=True, timeout=300)
        if p.returncode == 0:
            frames = sorted(str(x) for x in outdir.glob("scene-*.jpg"))
    except (OSError, subprocess.SubprocessError):
        frames = []
    scene_n = len(frames)
    if scene_n < 3 and duration:
        need = min(args.max_frames, 6) - scene_n
        for k in range(max(0, need)):
            ss = duration * (k + 0.5) / max(1, need)
            dst = outdir / f"uniform-{k + 1:02d}.jpg"
            try:
                p = subprocess.run([ff, "-nostdin", "-hide_banner", "-loglevel", "error",
                                    "-y", "-ss", str(ss), "-i", str(src),
                                    "-frames:v", "1", "-vf", "scale=1280:-1",
                                    str(dst)], stdin=subprocess.DEVNULL, capture_output=True, timeout=120)
                if p.returncode == 0 and dst.is_file():
                    frames.append(str(dst))
            except (OSError, subprocess.SubprocessError):
                continue
    frames = sorted(frames)
    print(json.dumps({"ok": True, "frames": frames, "scene_frames": scene_n,
                      "uniform_frames": len(frames) - scene_n,
                      "duration": duration, "out_dir": str(outdir)},
                     ensure_ascii=False))
    return 0 if frames else 1


ATTACH_KINDS = ("auto", "file", "image", "video", "voice")


def main() -> int:
    parser = argparse.ArgumentParser(description="Muse WeChat ClawBot bridge")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("login")
    p.add_argument("--force", action="store_true")
    p.set_defaults(fn=cmd_login)

    p = sub.add_parser("wait-login")
    p.add_argument("--timeout", type=int, default=480)
    p.add_argument("--verify-code", default=None)
    p.set_defaults(fn=cmd_wait_login)

    p = sub.add_parser("serve")
    p.set_defaults(fn=cmd_serve)

    p = sub.add_parser("send", help="human-approved resend of a task's reply")
    p.add_argument("--task-id", required=True)
    p.set_defaults(fn=cmd_send)

    for name, fn in (("send-image", cmd_send_image), ("send-file", cmd_send_file),
                     ("send-video", cmd_send_video)):
        p = sub.add_parser(name)
        p.add_argument("--task-id", default=None)
        p.add_argument("--to-user", default=None)
        p.add_argument("--file", required=True)
        p.add_argument("--caption", default=None)
        p.add_argument("--context-token", default=None)
        p.add_argument("--no-queue", action="store_true",
                       help="do not keep retrying in the background")
        p.add_argument("--claim-id", default=None,
                       help="the claim_id from claim (a superseded claim exits 4)")
        p.set_defaults(fn=fn)

    p = sub.add_parser("send-media",
                       help="send files (auto kind, or --as voice) with "
                            "background retries")
    p.add_argument("--task-id", default=None)
    p.add_argument("--to-user", default=None)
    p.add_argument("--file", action="append", required=True)
    p.add_argument("--as", dest="as_kind", default="auto", choices=ATTACH_KINDS)
    p.add_argument("--caption", default=None)
    p.add_argument("--context-token", default=None)
    p.add_argument("--wait", type=float, default=60)
    p.add_argument("--claim-id", default=None,
                   help="the claim_id from claim (a superseded claim exits 4)")
    p.set_defaults(fn=cmd_send_media)

    p = sub.add_parser("flush")
    p.set_defaults(fn=cmd_flush)

    p = sub.add_parser("status")
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser("claim")
    p.add_argument("--limit", type=int, default=1)
    p.add_argument("--full", action="store_true",
                   help="return complete task records (default: slim)")
    p.add_argument("--wait", type=_wait_arg, default=0,
                   help="block up to N s for the next message (warm executor); "
                        "auto = WECHAT_BRIDGE_WARM_WAIT_S (default 45)")
    p.add_argument("--settle-ms", type=int, default=None,
                   help="ignored since 2026-11-06: bursts are coalesced by serve's "
                        "batches (state/pending.json, 3 s pause / 60 s cap)")
    p.add_argument("--context-since", type=int, default=None,
                   help="context_cursor of the previous claim: only new context")
    p.set_defaults(fn=cmd_claim)

    p = sub.add_parser("complete")
    p.add_argument("--task-id", required=True)
    p.add_argument("--result", default=None)
    p.add_argument("--result-file", default=None)
    p.add_argument("--error", default=None)
    p.add_argument("--attach", action="append", default=[],
                   help="file to send after the reply (repeatable)")
    p.add_argument("--attach-as", default="auto", choices=ATTACH_KINDS)
    p.add_argument("--wait-attachments", type=float, default=10,
                   help="seconds to wait for attachments (they keep retrying "
                        "in the background either way)")
    p.add_argument("--then-wait", type=_wait_arg, default=0,
                   help="after sending, wait up to N s for the user's next "
                        "message and return it as `next` (one call per turn)")
    p.add_argument("--then-limit", type=int, default=5)
    p.add_argument("--force", action="store_true",
                   help="replace a reply that is queued or partly delivered")
    p.add_argument("--merge-ids", default=None,
                   help="other task ids this one reply also answers (comma separated)")
    p.add_argument("--raw", action="store_true",
                   help="send the text exactly as given (no Markdown adaptation, "
                        "no auto-attach)")
    p.add_argument("--claim-id", default=None,
                   help="the claim_id from claim: refused if another executor "
                        "has re-claimed the task")
    p.add_argument("--continue", dest="cont", action="store_true",
                   help="long task (EXECUTOR-relay.md): hand the rest of the plan to a "
                        "fresh executor turn; sends nothing - end the turn right after")
    p.add_argument("--step-done", default=None,
                   help="with --continue: plan step id(s) finished in this leg")
    p.add_argument("--artifact", action="append", default=[],
                   help="with --continue: an artifact file of that step (repeatable)")
    p.add_argument("--stage", default=None,
                   help="with --continue: the checkpoint the next leg sees")
    p.add_argument("--background", action="store_true",
                   help="long task: hand it to the background (main agent + sub-agent); the "
                        "bridge sends the signal - end the turn right after")
    p.set_defaults(fn=cmd_complete)

    p = sub.add_parser("progress", help="executor heartbeat for a long task")
    p.add_argument("--task-id", required=True)
    p.add_argument("--note", default=None)
    p.add_argument("--stage", default=None,
                   help="what step the task is on; notified honestly, never 'almost done'")
    p.add_argument("--notify", action="store_true",
                   help="also send the note to the user (max 1/min)")
    p.add_argument("--hold-minutes", type=int, default=None,
                   help="long-step lease (1-60 min, requires --stage): "
                        "reclaim skips this task until the lease expires")
    p.add_argument("--claim-id", default=None,
                   help="the claim_id from claim (stale claims are refused)")
    p.add_argument("--plan", default=None,
                   help="long task: the step plan (JSON file, - for stdin, or inline JSON), "
                        "kept as state/plans/<id>.json (EXECUTOR-relay.md)")
    p.add_argument("--plan-from", default=None,
                   help="adopt the stopped plan of an earlier task (claim resume_plan)")
    p.add_argument("--step-done", default=None,
                   help="plan step id(s) just finished (comma separated)")
    p.add_argument("--artifact", action="append", default=[],
                   help="an artifact file of the step (repeatable)")
    p.set_defaults(fn=cmd_progress)

    p = sub.add_parser("heartbeat", help="cheap executor heartbeat, no user message")
    p.add_argument("--task-id", required=True)
    p.add_argument("--stage", default=None,
                   help="what step the task is on; also recorded as the checkpoint")
    p.add_argument("--claim-id", default=None,
                   help="the claim_id from claim (stale claims are refused)")
    p.set_defaults(fn=cmd_heartbeat)

    p = sub.add_parser("outline", help="function/class index of bridge.py")
    p.add_argument("--search", default=None, help="filter by name substring")
    p.set_defaults(fn=cmd_outline)

    p = sub.add_parser("history", help="recent WeChat conversation")
    p.add_argument("--limit", type=int, default=30)
    p.add_argument("--hours", type=float, default=72)
    p.add_argument("--search", default=None,
                   help="search the whole kept history for a keyword")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_history)

    p = sub.add_parser("memo", help="show/set the conversation memo")
    p.add_argument("--set", default=None)
    p.add_argument("--set-file", default=None)
    p.add_argument("--clear", action="store_true")
    p.set_defaults(fn=cmd_memo)
    p = sub.add_parser("decisions", help="show/append the decision list")
    p.add_argument("--add", default=None, help="append one in-progress decision item")
    p.set_defaults(fn=cmd_decisions)

    p = sub.add_parser("queue", help="outbound media queue")
    p.add_argument("--retry", default=None, metavar="QUEUE_ID")
    p.add_argument("--cancel", default=None, metavar="QUEUE_ID")
    p.add_argument("--json", action="store_true")
    p.set_defaults(fn=cmd_queue)

    p = sub.add_parser("voice-to-wav", help="decode a voice note for STT")
    p.add_argument("--in", dest="input", required=True)
    p.add_argument("--out", dest="output", default=None)
    p.set_defaults(fn=cmd_voice_to_wav)

    p = sub.add_parser("video-audio", help="extract a video's audio to WAV for STT")
    p.add_argument("--in", dest="input", required=True)
    p.add_argument("--out", dest="output", default=None)
    p.set_defaults(fn=cmd_video_audio)

    p = sub.add_parser("video-frames", help="scene-change keyframes of a video")
    p.add_argument("--in", dest="input", required=True)
    p.add_argument("--out-dir", dest="out_dir", default=None)
    p.add_argument("--max-frames", dest="max_frames", type=int, default=8)
    p.add_argument("--threshold", type=float, default=0.35)
    p.set_defaults(fn=cmd_video_frames)

    p = sub.add_parser("long-tasks", help="background long tasks (BACKGROUND.md)")
    p.add_argument("action", nargs="?", default="list",
                   choices=("list", "show", "take", "progress", "finish", "fail", "wake-check"))
    p.add_argument("id", nargs="?", default=None)
    p.add_argument("--token", default=None, help="from long-tasks take")
    p.add_argument("--stage", default=None)
    p.add_argument("--note", default=None)
    p.add_argument("--notify", action="store_true", help="progress: also tell the user (max 1/10 min)")
    p.add_argument("--result", default=None)
    p.add_argument("--result-file", default=None)
    p.add_argument("--attach", action="append", default=[])
    p.add_argument("--error", default=None, help="fail: how far it got, what is left, how to go on")
    p.set_defaults(fn=cmd_long_tasks)

    p = sub.add_parser("stats", help="latency p50/p95 and reply-budget calibration")
    p.add_argument("--hours", type=float, default=72)
    p.add_argument("--task-id", action="append", default=None,
                   help="one task's timeline: CLI calls, to_first_call, progress "
                        "notices (repeat or comma-separate)")
    p.add_argument("--recent", type=int, default=0,
                   help="the same timeline for the last N claimed tasks (max 50)")
    p.set_defaults(fn=cmd_stats)

    p = sub.add_parser("canary-dedupe", help="test client_id de-duplication")
    p.add_argument("--confirm", choices=("yes", "no"), default=None)
    p.set_defaults(fn=cmd_canary_dedupe)

    p = sub.add_parser("stop")
    p.set_defaults(fn=cmd_stop)

    p = sub.add_parser("write-proxy-env")
    p.set_defaults(fn=cmd_write_proxy_env)

    args = parser.parse_args()
    apply_private_umask()
    dirs = (STATE_DIR, INBOX_DIR, OUTBOX_DIR, LOG_DIR, MEDIA_DIR,
            FILES_DIR, QUEUE_DIR, ATTACH_DIR, HISTORY_DIR)
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
    harden_dirs(dirs)
    with contextlib.suppress(OSError):
        os.chmod(HISTORY_DIR, 0o700)
        os.chmod(ATTACH_DIR, 0o700)
    PROXY_POOL.init()
    _t0 = time.monotonic()
    _rc = None
    try:
        _rc = args.fn(args)
        return _rc
    finally:
        _cli_log(args.cmd, getattr(args, "task_id", None), _t0, _rc)


# ---------------------------------------------------------------------------
# 2026-11-05: evidence for tuning (no behaviour change): send timing, echo
# ledger, warm/cold claims, the agent's CLI calls, follow-up gaps, hook ticks
# ---------------------------------------------------------------------------
_SEND_TIMING = threading.local()


def _send_part(creds: dict, to_user: str, body: str, ctx, client_id: str) -> dict:
    """_send_part_raw, timed (deliver breakdown) and recorded in the echo
    ledger: ok, or uncertain when the request may have reached WeChat."""
    t0 = now_ms()
    try:
        resp = _send_part_raw(creds, to_user, body, ctx, client_id)
    except ApiError as e:
        if e.kind in ("timeout", "network"):
            _echo_ledger_update(client_id, at=t0, kind="text", ok=None,
                                task=getattr(_SEND_TIMING, "task", None))
        raise
    finally:
        acc = getattr(_SEND_TIMING, "acc", None)
        if acc is not None:
            acc.append((t0, now_ms()))
    _echo_ledger_update(client_id, at=t0, kind="text", ok=True,
                        task=getattr(_SEND_TIMING, "task", None))
    return resp


def _finish_qitem(it: dict, status: str, *a, **k):
    if status in ("sent", "uncertain") and it.get("client_id"):
        _echo_ledger_update(it["client_id"], at=int(it.get("sent_at") or now_ms()),
                            kind=it.get("kind") or "media",
                            ok=True if status == "sent" else None, task=it.get("task_id"))
    return _finish_qitem_raw(it, status, *a, **k)


def _note_call(ex: dict, at: int) -> None:
    calls = list(ex.get("calls") or [])[-49:]
    calls.append(int(at))
    ex["calls"] = calls


EXEC_TOP_N = 3          # tasks named behind each long tail in `stats`


def exec_breakdown(hours: float = 72, limit: int = 300) -> dict:
    """Where exec time goes, seen from the bridge: the agent's CLI calls
    (progress/heartbeat) between claim and complete, warm vs cold claims,
    and the deliver split (pick-up, local work, network).
    Step 0 (2026-11-05): every column also reports `max`, and the slowest
    tasks are named (`to_first_call_top`, `exec_no_calls_top`). _pct picks
    index round(p * (n - 1)), so with n = 15 the p95 is the 14th value: the
    one 132 s first progress of test 2-3 hid behind p95 = 5.5 s. p50/p95
    are unchanged on purpose - they stay comparable with the numbers
    already recorded; `max` and the named tasks show the tail."""
    since = now_ms() - int(hours * 3_600_000)
    c: dict = {k: [] for k in ("calls", "to_first_call", "after_last_call",
                               "exec_no_calls", "exec_with_calls", "wake_warm",
                               "wake_cold", "pickup", "pre_send", "network", "post_send")}
    warm = cold = 0
    first_rows: list = []
    silent_rows: list = []
    for s_ in scan_task_index()[-limit:]:
        if s_.get("status") != "sent" or s_.get("kind") in ("command", "notice") \
                or int(s_.get("received_at") or 0) < since:
            continue
        t = load_task(s_["message_id"]) or {}
        ex, sd = t.get("execution") or {}, t.get("send") or {}
        rec, cl, fin = (int(t.get("received_at") or 0), int(ex.get("claimed_at") or 0),
                        int(ex.get("finished_at") or 0))
        if ex.get("proactive") or ex.get("merged_into") or not (rec and cl and fin):
            continue
        calls = [int(x) for x in ex.get("calls") or [] if cl <= int(x) <= fin]
        c["calls"].append(len(calls))
        if calls:
            c["to_first_call"].append(calls[0] - cl)
            c["after_last_call"].append(fin - calls[-1])
            c["exec_with_calls"].append(fin - cl)
            first_rows.append({"task_id": s_["message_id"], "to_first_call_ms": calls[0] - cl,
                               "exec_ms": fin - cl, "calls": len(calls)})
        else:
            c["exec_no_calls"].append(fin - cl)
            silent_rows.append({"task_id": s_["message_id"], "exec_ms": fin - cl})
        if "warm" in ex:
            if ex["warm"]:
                warm += 1
                c["wake_warm"].append(cl - rec)
            else:
                cold += 1
                c["wake_cold"].append(cl - rec)
        if sd.get("t_start") and sd.get("first_req_at"):
            c["pickup"].append(max(0, int(sd["t_start"]) - fin))
            c["pre_send"].append(max(0, int(sd["first_req_at"]) - int(sd["t_start"])))
            c["network"].append(int(sd.get("net_ms") or 0))
            c["post_send"].append(max(0, int(sd.get("at") or 0) - int(sd["first_req_at"])
                                      - int(sd.get("net_ms") or 0)))
    out: dict = {"n": len(c["calls"]), "warm_claims": warm, "cold_claims": cold}
    for k, v in c.items():
        out[k] = {"p50": _pct(v, 0.5), "p95": _pct(v, 0.95),
                  "max": max(v) if v else None, "n": len(v)}
    out["to_first_call_top"] = sorted(first_rows, key=lambda r: -r["to_first_call_ms"])[:EXEC_TOP_N]
    out["exec_no_calls_top"] = sorted(silent_rows, key=lambda r: -r["exec_ms"])[:EXEC_TOP_N]
    return out


def _clock_s(ms: int | None) -> str | None:
    """Wall clock with seconds, in the WeChat user's time zone."""
    import datetime as _dt
    if not ms:
        return None
    return _dt.datetime.fromtimestamp(int(ms) / 1000, _user_tz()).strftime("%m-%d %H:%M:%S")


def task_evidence(task_id: str) -> dict:
    """Step 0 (2026-11-05): one task's execution timeline as the bridge saw
    it - the executor's progress/heartbeat calls (epoch ms, and ms after
    the claim), to_first_call, and the progress notices the user got
    (executor `--notify` = reason progress, bridge = auto_progress) with
    when they were queued and sent - so a late first progress is looked up
    per task (`stats --task-id`) instead of read out of inbox files by hand.
    Read-only."""
    t = load_task(task_id)
    if not t:
        return {"task_id": task_id, "error": "task not found"}
    ex, sd = t.get("execution") or {}, t.get("send") or {}
    rec, cl = int(t.get("received_at") or 0), int(ex.get("claimed_at") or 0)
    fin = int(ex.get("finished_at") or 0)
    end = fin or now_ms()
    calls = sorted(int(x) for x in ex.get("calls") or [] if cl and cl <= int(x) <= end)
    out: dict = {
        "task_id": task_id, "kind": t.get("kind"), "status": t.get("status"),
        "warm": ex.get("warm"), "received": _clock_s(rec), "claimed": _clock_s(cl),
        "finished": _clock_s(fin), "sent": _clock_s(sd.get("at")),
        "wake_ms": cl - rec if rec and cl else None,
        "exec_ms": fin - cl if fin and cl else None,
        "running_ms": now_ms() - cl if cl and not fin else None,
        "calls": calls, "calls_ms": [x - cl for x in calls],
        "to_first_call_ms": calls[0] - cl if calls else None,
        "after_last_call_ms": fin - calls[-1] if calls and fin else None,
        "stage": ex.get("stage") or None,
    }
    for k in ("merged_into", "proactive", "resumed", "reclaims", "hold_minutes"):
        if ex.get(k):
            out[k] = ex[k]
    rl = t.get("relay") or {}
    if rl:
        # 2026-11-07 relay: one row per leg - start, end, how it ended, the
        # steps it finished and its last stage (what the next leg resumed from)
        pl = plan_load(task_id) or {}
        steps = pl.get("steps") or []
        legs = []
        for lg in rl.get("legs") or []:
            a, z = int(lg.get("claimed_at") or 0), int(lg.get("ended_at") or 0)
            legs.append({"leg": lg.get("leg"), "start": _clock_s(a), "end": _clock_s(z),
                         "ms": z - a if a and z else None, "how": lg.get("how"),
                         "steps_done": lg.get("steps") or [],
                         "checkpoint": lg.get("stage") or None,
                         **({"stopped": True} if lg.get("stopped") else {})})
        if t.get("status") == "executing" and cl:
            no = len(legs) + 1
            legs.append({"leg": no, "start": _clock_s(cl), "end": None, "ms": now_ms() - cl,
                         "how": "running",
                         "steps_done": [s.get("step_id") for s in steps if s.get("status") == "done"
                                        and int(s.get("leg") or 0) == no],
                         "checkpoint": ex.get("stage") or None})
        st0 = int(rl.get("started_at") or 0)
        out["relay"] = {"started": _clock_s(st0), "elapsed_ms": end - st0 if st0 else None,
                        "legs": legs,
                        "plan": ({"state": pl.get("state"), "steps": len(steps),
                                  "done": sum(1 for s in steps if s.get("status") == "done"),
                                  "next": next((s.get("step_id") for s in steps
                                                if s.get("status") != "done"), None)}
                                 if pl else None)}
    bgx = t.get("bg") or {}
    if bgx:                 # 2026-11-07 v1.4: the background part of this task
        q_, k_, f_ = (int(bgx.get(k) or 0) for k in ("queued_at", "taken_at", "finished_at"))
        out["background"] = {"route": bgx.get("route"), "state": bgx.get("state"),
                             "queued": _clock_s(q_), "taken": _clock_s(k_), "finished": _clock_s(f_),
                             "wait_ms": k_ - q_ if q_ and k_ else None,
                             "run_ms": f_ - k_ if k_ and f_ else None,
                             "takes": bgx.get("takes"), "stage": bgx.get("stage")}
    if cl:
        cc = cli_calls_between(cl - 2_000, end + 2_000, task_id)
        if cc:
            # 2026-11-07 §8.3.3: every bridge call in this task's window
            out["cli_calls"] = [{"cmd": r["cmd"], "at_ms": r["at"] - cl, "ms": r["ms"]}
                                for r in cc[:80]]
            out["cli_count"] = len(cc)
            cnt: dict = {}
            for r in cc:
                cnt[r["cmd"]] = cnt.get(r["cmd"], 0) + 1
            rep = {k: v for k, v in cnt.items() if v > 1 and k not in ("progress", "heartbeat")}
            if rep:
                out["cli_repeats"] = rep
    notices: list = []
    if cl:
        for s_ in scan_task_index():
            at = int(s_.get("received_at") or 0)
            if s_.get("kind") != "notice" or not cl <= at <= end + 5_000:
                continue
            n = load_task(s_["message_id"]) or {}
            nex = n.get("execution") or {}
            if str((nex.get("about") or {}).get("task_id")) != str(task_id):
                continue
            nat = int((n.get("send") or {}).get("at") or 0)
            notices.append({"id": s_["message_id"], "reason": nex.get("notice"),
                            "status": n.get("status"), "queued_ms": at - cl,
                            "sent_ms": nat - cl if nat and n.get("status") == "sent" else None})
    out["notices"] = notices
    sent = [x["sent_ms"] for x in notices if x["sent_ms"] is not None]
    out["first_notice_sent_ms"] = min(sent) if sent else None
    return out


def recent_task_ids(n: int) -> list:
    """The last `n` claimed tasks (no commands, no notices), newest first."""
    rows = [s_ for s_ in scan_task_index()
            if s_.get("kind") not in ("command", "notice") and s_.get("claimed_at")]
    return [s_["message_id"] for s_ in reversed(rows[-max(1, min(int(n), 50)):])]


def followup_gaps(days: int = 30) -> dict:
    """Bot reply -> the user's next message, from the chat history: how many
    follow-ups a warm executor waiting 45 / 90 / 180 / 300 s would catch."""
    since = now_ms() - days * 86_400_000
    rows = [r for r in history_read_all() if int(r.get("at") or 0) >= since
            and r.get("kind") not in ("command", "topic", "notice")]
    rows.sort(key=lambda r: int(r.get("at") or 0))
    gaps = []
    for a, b in zip(rows, rows[1:]):
        if a.get("role") == "bot" and b.get("role") == "user":
            g = int(b.get("at") or 0) - int(a.get("at") or 0)
            if 0 <= g <= 86_400_000:
                gaps.append(g)
    n = len(gaps)
    return {"n": n, "p50_ms": _pct(gaps, 0.5), "p90_ms": _pct(gaps, 0.9),
            "within": {f"{s}s": (round(sum(1 for g in gaps if g <= s * 1000) / n, 3) if n else None)
                       for s in (45, 90, 180, 300)},
            # 2026-11-07 §8.3.1: what 60 / 120 / 180 s would catch, and cost
            "whatif": _warm_wait_whatif(rows, gaps)}


HOOK_TICKS_FILE = STATE_DIR / "hook_ticks.log"
# 2026-11-05 audit (§8-2): the hook keeps the last 18000 lines (25 h at one
# run per 5 s, rotation at 1 MiB), so 24 h is the most the log can answer.
# It used to keep 2000 lines (2.8 h) and this reader the last 5000, while the
# window asked for 24 h (72 h from `stats`): most of the "window" was empty.
HOOK_TICKS_WINDOW_H = 24


def hook_tick_stats(hours: float = HOOK_TICKS_WINDOW_H) -> dict:
    """The platform's hook schedule as the hook itself logged it: interval
    start->start vs gap end->start (which one stays ~5 s tells whether the
    5 s count from the start or from the end of a run - decisive for
    WECHAT_BRIDGE_HOOK_LINGER_MS), run time, skipped ticks, duplicate wakes.
    The window is capped at HOOK_TICKS_WINDOW_H; `span_h` is how far back the
    lines in it really reach, `reruns` the linger re-runs (each should wake)."""
    try:
        lines = HOOK_TICKS_FILE.read_text(encoding="utf-8").splitlines()[-40000:]
    except OSError:
        return {"n": 0}
    hours = min(float(hours), HOOK_TICKS_WINDOW_H)
    now = now_ms()
    since = now - int(hours * 3_600_000)
    ticks: list = []
    reruns = 0
    for ln in lines:
        p = ln.split()
        if len(p) < 6 or not (p[0].isdigit() and p[1].isdigit()) or int(p[0]) < since:
            continue
        row = {"start": int(p[0]), "end": int(p[1]), "woke": p[2] == "1",
               "linger": int(p[3]) if p[3].isdigit() else 0, "sig": p[5]}
        reruns += p[4] == "1"
        if p[4] == "1" and ticks:           # a linger re-run belongs to the tick
            ticks[-1]["end"] = max(ticks[-1]["end"], row["end"])
            ticks[-1]["woke"] |= row["woke"]
            ticks[-1]["sig"] = row["sig"] if row["woke"] else ticks[-1]["sig"]
            ticks[-1]["rerun"] = True
            continue
        ticks.append(row)
    ss = [b["start"] - a["start"] for a, b in zip(ticks, ticks[1:])]
    es = [b["start"] - a["end"] for a, b in zip(ticks, ticks[1:])]
    run = [t["end"] - t["start"] for t in ticks]
    med = _pct(ss, 0.5) or 0
    dup, last = 0, {}
    for t in ticks:
        if t["woke"] and t["sig"] not in ("-", ""):
            if t["start"] - last.get(t["sig"], -10 ** 12) < 60_000:
                dup += 1
            last[t["sig"]] = t["start"]
    out = {"n": len(ticks), "window_h": hours,
           "span_h": round((now - ticks[0]["start"]) / 3_600_000, 1) if ticks else 0.0,
           "start_to_start_ms": {"p50": _pct(ss, 0.5), "p95": _pct(ss, 0.95)},
           "end_to_start_ms": {"p50": _pct(es, 0.5), "p95": _pct(es, 0.95)},
           "run_ms": {"p50": _pct(run, 0.5), "p95": _pct(run, 0.95)},
           "linger_ticks": sum(1 for t in ticks if t["linger"] > 0),
           "reruns": reruns,
           "linger_wakes": sum(1 for t in ticks if t.get("rerun") and t["woke"]),
           "wakes": sum(1 for t in ticks if t["woke"]), "duplicate_wakes": dup,
           "skipped_ticks": sum(1 for x in ss if med and x > 1.8 * med)}
    # Only runs that take a while (a linger) can tell the two apart: then
    # either start->start or end->start stays at the platform's interval.
    if (_pct(run, 0.5) or 0) >= 1000 and len(ss) >= 10:
        e50 = _pct(es, 0.5) or 0
        out["schedule"] = ("from the START of a run" if abs(med - 5000) <= 800 < abs(e50 - 5000)
                           else "from the END of a run" if abs(e50 - 5000) <= 800 < abs(med - 5000)
                           else "unclear")
    return out


if __name__ == "__main__":
    sys.exit(main())
