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
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
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
BRIDGE_VERSION = "5.0"

FIXED_BASE_URL = "https://ilinkai.weixin.qq.com"
DEFAULT_BASE_URL = FIXED_BASE_URL
BOT_TYPE = "3"
CHANNEL_VERSION = "2.4.10"          # matches current plugin package.json
ILINK_APP_ID = "bot"                # package.json ilink_appid
BOT_AGENT = "Muse-WeChatBridge/5.0"              # observability only, not auth
LONG_POLL_DEFAULT_MS = 35_000
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
    print(line, flush=True,
          file=sys.stdout if _LOG_TO_STDOUT["on"] else sys.stderr)


_tmp_seq = itertools.count()


def atomic_write_json(path: Path, data, mode=0o600, fsync: bool = True) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # The tmp name is unique per process+thread+call. It used to be per
    # process only, so the receiver and sender threads both rewriting
    # heartbeat.json shared one tmp file and could corrupt each other or
    # crash os.replace with FileNotFoundError (which killed serve).
    tmp = path.with_suffix(
        path.suffix + f".tmp{os.getpid()}.{threading.get_ident()}.{next(_tmp_seq)}")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            if fsync:
                os.fsync(f.fileno())
        os.chmod(tmp, mode)
        os.replace(tmp, path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp.unlink()
        raise


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
        # http | connect | network | timeout | business
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
                pooled.read()
            pooled.close()
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
    failures surface as URLError, which every caller already maps to
    a network error."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        old = urllib.parse.urlsplit(req.full_url)
        new = urllib.parse.urlsplit(urllib.parse.urljoin(req.full_url, newurl))
        if old.scheme == "https" and new.scheme == "http":
            raise urllib.error.URLError(
                "refusing https->http redirect downgrade")
        old_origin = (old.scheme, old.hostname, old.port)
        new_origin = (new.scheme, new.hostname, new.port)
        if old_origin != new_origin:
            try:
                _validate_server_url(
                    urllib.parse.urljoin(req.full_url, newurl), "redirect")
            except MediaError as e:
                raise urllib.error.URLError(str(e)) from e
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
                         typing_sent=TYPING.sent)

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
    data = json.dumps(body).encode() if body is not None else b""
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


def api_base(creds: dict | None) -> str:
    return (creds or {}).get("baseurl") or DEFAULT_BASE_URL


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
            # api.ts: long-poll client timeout is a normal empty result
            return {"ret": 0, "msgs": [], "get_updates_buf": cursor}
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
            time.sleep(delays[attempt])
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
        proc = subprocess.run(args, capture_output=True, timeout=300)
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
        raise ApiError("network", "media encrypt failed") from e


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


def _import_optional(name: str):
    vend = BASE_DIR / "vendor" / "py"
    if vend.is_dir() and str(vend) not in sys.path:
        sys.path.insert(0, str(vend))
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
        [ff, "-hide_banner", "-loglevel", "error", "-y", "-i", str(src),
         "-f", "s16le", "-acodec", "pcm_s16le", "-ar", str(VOICE_SAMPLE_RATE),
         "-ac", "1", str(pcm)],
        capture_output=True, timeout=120)
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
        [ff, "-hide_banner", "-loglevel", "error", "-y", "-i", str(src),
         "-ar", "16000", "-ac", "1", str(dst)], capture_output=True, timeout=120)
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
    for p in (BASE_DIR / "vendor",):
        if p.exists():
            sys.path.insert(0, str(p))
    try:
        import qrcode  # type: ignore
        img = qrcode.make(content)
        img.save(QR_PNG)
        return True
    except Exception as e:  # noqa: BLE001
        log(f"QR PNG generation unavailable: {e}")
        QR_TXT.write_text(content + "\n", encoding="utf-8")
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


def cmd_login(args) -> int:
    creds = load_creds()
    if creds and not args.force:
        print(json.dumps({"ok": False,
                          "error": "credentials already exist; use --force to re-login"},
                         ensure_ascii=False))
        return 2
    local_tokens = _recent_tokens(creds)
    # api.ts: QR POST carries AuthorizationType + X-WECHAT-UIN, no token,
    # no base_info.
    resp = http_post(FIXED_BASE_URL,
                     f"ilink/bot/get_bot_qrcode?bot_type={BOT_TYPE}",
                     {"local_token_list": local_tokens},
                     post_headers(token=None), timeout=15)
    qrcode_value = resp.get("qrcode")
    img_content = resp.get("qrcode_img_content")
    if not qrcode_value or not img_content:
        print(json.dumps({"ok": False, "error": "server returned no qrcode"},
                         ensure_ascii=False))
        return 1
    atomic_write_json(LOGIN_FILE, {
        "qrcode": qrcode_value,
        "qrcode_img_content": img_content,
        "started_at": now_ms(),
        "current_base_url": FIXED_BASE_URL,
        "refresh_count": 0,
    })
    atomic_write_json(LOGIN_STATUS_FILE, {"status": "wait", "updated_at": now_ms()})
    png = make_qr_png(img_content)
    print(json.dumps({"ok": True, "qr_png": str(QR_PNG) if png else None,
                      "qr_txt": None if png else str(QR_TXT),
                      "expires_in_sec": 300}, ensure_ascii=False))
    return 0


def _save_credentials(status_resp: dict) -> dict:
    old_token = (load_creds() or {}).get("bot_token")
    creds = {
        "bot_token": status_resp.get("bot_token"),
        "ilink_bot_id": status_resp.get("ilink_bot_id"),
        "baseurl": status_resp.get("baseurl") or DEFAULT_BASE_URL,
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
    while time.time() < deadline:
        endpoint = ("ilink/bot/get_qrcode_status?qrcode="
                    + urllib.parse.quote(session["qrcode"], safe=""))
        if verify_code:
            endpoint += "&verify_code=" + urllib.parse.quote(verify_code, safe="")
        resp = http_get(current_base, endpoint, timeout=35)
        status = resp.get("status", "wait")
        atomic_write_json(LOGIN_STATUS_FILE,
                          {"status": status, "updated_at": now_ms()})
        if status == "wait":
            time.sleep(1)
            continue
        if status == "scaned":
            verify_code = None
            time.sleep(1)
            continue
        if status == "scaned_but_redirect":
            host = resp.get("redirect_host")
            if host:
                current_base = f"https://{host}"
            time.sleep(1)
            continue
        if status == "need_verifycode":
            if not args.verify_code:
                print(json.dumps({"ok": False, "status": "need_verifycode",
                                  "error": "phone shows a verify code; "
                                           "rerun wait-login --verify-code <code>"},
                                 ensure_ascii=False))
                return 3
            # wrong code loops back to need_verifycode with the same code;
            # require a fresh code from the user instead of spinning
            print(json.dumps({"ok": False, "status": "need_verifycode",
                              "error": "verify code not accepted; ask user for a new code"},
                             ensure_ascii=False))
            return 3
        if status in ("expired", "verify_code_blocked"):
            refresh_count += 1
            if refresh_count > 3:
                print(json.dumps({"ok": False, "status": status,
                                  "error": "QR refreshed 3 times; giving up"},
                                 ensure_ascii=False))
                return 1
            log(f"login QR {status}; refreshing ({refresh_count}/3)")
            sub = argparse.Namespace(force=True)
            if cmd_login(sub) != 0:
                return 1
            session = read_json(LOGIN_FILE)
            session["refresh_count"] = refresh_count
            atomic_write_json(LOGIN_FILE, session)
            current_base = FIXED_BASE_URL
            verify_code = None
            continue
        if status == "binded_redirect":
            creds = load_creds()
            if creds:
                print(json.dumps({"ok": True, "already_connected": True,
                                  "ilink_bot_id": mask(creds.get("ilink_bot_id")),
                                  "ilink_user_id": mask(creds.get("ilink_user_id"))},
                                 ensure_ascii=False))
                return 0
            print(json.dumps({"ok": False, "status": status,
                              "error": "bot already bound elsewhere; no local credentials"},
                             ensure_ascii=False))
            return 1
        if status == "confirmed":
            if not resp.get("ilink_bot_id"):
                print(json.dumps({"ok": False,
                                  "error": "confirmed but ilink_bot_id missing"},
                                 ensure_ascii=False))
                return 1
            creds = _save_credentials(resp)
            LOGIN_FILE.unlink(missing_ok=True)
            atomic_write_json(LOGIN_STATUS_FILE,
                              {"status": "confirmed", "updated_at": now_ms()})
            print(json.dumps({"ok": True, "connected": True,
                              "ilink_bot_id": mask(creds["ilink_bot_id"]),
                              "ilink_user_id": mask(creds["ilink_user_id"]),
                              "baseurl": creds["baseurl"]}, ensure_ascii=False))
            return 0
        time.sleep(1)
    print(json.dumps({"ok": False, "error": "login timeout"}, ensure_ascii=False))
    return 1


# --------------------------------------------------------------------------
# Task store
# --------------------------------------------------------------------------

def task_path(message_id: str) -> Path:
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in str(message_id))
    return INBOX_DIR / f"{safe}.json"


def load_task(message_id: str) -> dict | None:
    return read_json(task_path(message_id))


def save_task(task: dict) -> None:
    atomic_write_json(task_path(task["message_id"]), task, mode=0o600)


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


def history_append(role: str, text: str, *, mid: str | None = None,
                   kind: str | None = None, at: int | None = None) -> None:
    """Append one turn. Never raises: history is a convenience."""
    if not HISTORY_ENABLED or not (text or "").strip():
        return
    entry = {"at": at or now_ms(), "role": role, "id": mid, "kind": kind,
             "text": str(text)[:HISTORY_TEXT_MAX]}
    try:
        HISTORY_DIR.mkdir(parents=True, exist_ok=True)
        with open(HISTORY_LOCK, "a+") as lk:
            fcntl.flock(lk, fcntl.LOCK_EX)
            try:
                fresh = not HISTORY_FILE.exists()
                with open(HISTORY_FILE, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry, ensure_ascii=False) + "\n")
                if fresh:
                    os.chmod(HISTORY_FILE, 0o600)
                if HISTORY_FILE.stat().st_size > HISTORY_MAX_BYTES:
                    lines = HISTORY_FILE.read_text(
                        encoding="utf-8", errors="replace").splitlines()
                    # 2026-11-02 六-3: archive, don't delete. Dropped lines
                    # go to history/archive-YYYY-MM.jsonl (grouped by the
                    # line's own `at` month) so history --search still
                    # finds them; the live file stays bounded.
                    dropped = lines[:-HISTORY_KEEP_LINES] if len(lines) > HISTORY_KEEP_LINES else []
                    if dropped:
                        import datetime as _dt
                        buckets: dict[str, list[str]] = {}
                        for _ln in dropped:
                            try:
                                _at = int(json.loads(_ln).get("at") or 0)
                            except (ValueError, TypeError):
                                _at = 0
                            if _at:
                                _m = _dt.datetime.fromtimestamp(_at / 1000, _user_tz()).strftime("%Y-%m")
                            else:
                                _m = _dt.datetime.now(_user_tz()).strftime("%Y-%m")
                            buckets.setdefault(_m, []).append(_ln)
                        for _m, _ls in buckets.items():
                            _af = HISTORY_DIR / f"archive-{_m}.jsonl"
                            _fresh = not _af.exists()
                            with open(_af, "a", encoding="utf-8") as _f:
                                _f.write("\n".join(_ls) + "\n")
                            if _fresh:
                                os.chmod(_af, 0o600)
                    tmp = HISTORY_FILE.with_suffix(".jsonl.tmp")
                    tmp.write_text("\n".join(lines[-HISTORY_KEEP_LINES:]) + "\n",
                                   encoding="utf-8")
                    os.chmod(tmp, 0o600)
                    os.replace(tmp, HISTORY_FILE)
            finally:
                fcntl.flock(lk, fcntl.LOCK_UN)
    except OSError as e:
        log(f"history append failed: {type(e).__name__}")


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
        tmp = TOPIC_SUMMARIES_FILE.with_suffix(".jsonl.tmp")
        tmp.write_text("".join(json.dumps(e, ensure_ascii=False) + "\n"
                               for e in kept), encoding="utf-8")
        os.chmod(tmp, 0o600)
        os.replace(tmp, TOPIC_SUMMARIES_FILE)
        return digest
    except OSError:
        return None


def conversation_context(before_ms: int | None) -> dict | None:
    """Recent turns before the claimed task(s) + the executor's memo, sized
    for an LLM prompt (turns are clipped; total bounded by CONTEXT_CHARS)."""
    out: dict = {}
    if CONTEXT_TURNS > 0:
        rows = history_read(since_ms=now_ms() - CONTEXT_WINDOW_MS,
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
    if memo:
        out["memo"] = memo
    dec = _decisions_read()
    if dec.strip():
        out["decisions"] = dec[:3000]  # opt batch item 2: decided matters first, do not re-ask
    topics = topic_summaries_read()[-3:]
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
        _quote_exec("UPDATE msgs SET media=?, name=COALESCE(?, name) WHERE id=?",
                    (str(path), name, str(mid)))


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


def resolve_quote(items: list) -> dict:
    """{"text": "title | body" | None, "media": stored record with a local
    file | None, "svr_id", "from_store"}: inline quote first (older clients),
    then the local store by svr_id, then partial_text."""
    for it in items:
        ref = it.get("ref_msg")
        if not isinstance(ref, dict):
            continue
        title = str(ref.get("title") or "").strip()
        inline = ref.get("message_item") or {}
        body = _item_text(inline) or _MEDIA_LABELS.get(inline.get("type"), "")
        svr = str(ref.get("svr_id") or "").strip()
        partial = ref.get("partial_text") if isinstance(ref.get("partial_text"), dict) else None
        rec = quote_get(svr) if svr else None
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
                    "svr_id": svr or None, "from_store": bool(rec)}
    return {"text": None, "media": None, "svr_id": None, "from_store": False}


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
_SEND_KEYS = ("attempts", "no_auto_retry", "next_retry_at", "started_at",
              "max_attempts", "sender_pid", "progress_at", "notified",
              "exhausted", "not_delivered", "deferred")


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
    }


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
        revalidate = now_m - _scan_state["full_at"] > SCAN_REVALIDATE_SEC
        settled = time.time_ns() - dst.st_mtime_ns > 2_000_000_000
        if (not revalidate and settled
                and dst.st_mtime_ns == _scan_state["dir_mtime"]):
            return list(_scan_state["out"])
        out, seen = [], set()
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
                    _SCAN_CACHE[name] = (sig, summ)
                if summ:
                    out.append(summ)
        for name in [n for n in _SCAN_CACHE if n not in seen]:
            del _SCAN_CACHE[name]
        out.sort(key=lambda x: (x["received_at"], str(x["message_id"])))
        _scan_state.update(dir_mtime=dst.st_mtime_ns, out=out)
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
    with resp:
        partial = bool(have) and getattr(resp, "status", 200) == 206
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
    url = _media_url(image_item.get("media") or {}, "image")
    raw = _fetch_bytes(url)
    key = _parse_aes_key(image_item)
    data = _decrypt_aes_ecb(raw, key) if key else raw
    if not data:
        raise MediaError("empty", "image decrypt produced no data")
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    path = MEDIA_DIR / f"{mid}{_sniff_image_ext(data)}"
    path.write_bytes(data)
    os.chmod(path, 0o600)
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
    path = FILES_DIR / f"{mid}__{fname}"
    os.replace(plain_path, path)
    os.chmod(path, 0o600)
    return {"media_path": str(path), "media_bytes": size, "file_name": fname}


def download_video(video_item: dict, mid: str) -> dict:
    """Download (and decrypt) an inbound video to MEDIA_DIR.
    Mirrors download_file (media.aes_key required, payload always
    AES-128-ECB encrypted), plus two video extras: the item carries a
    plaintext video_md5 we verify after decryption, and an encrypted
    thumb_media poster frame (same key) saved alongside best-effort.
    Returns {"media_path", "media_bytes", "play_length", ...}."""
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
                tpath.write_bytes(tdata)
                os.chmod(tpath, 0o600)
                out["thumb_path"] = str(tpath)
        except Exception as e:  # noqa: BLE001 - poster is best-effort
            log(f"video thumb download failed id={mid}: {e}")
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
    url = _media_url(voice_item.get("media") or {}, "voice")
    key = _parse_aes_key(voice_item)
    raw = _fetch_bytes(url, max_bytes=VOICE_RECV_MAX_BYTES)
    data = _decrypt_aes_ecb(raw, key) if key else raw
    if not data:
        raise MediaError("empty", "voice decrypt produced no data")
    fmt = _sniff_audio(data, voice_item.get("encode_type"))
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    raw_path = MEDIA_DIR / f"{mid}.{fmt}"
    raw_path.write_bytes(data)
    os.chmod(raw_path, 0o600)
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


def parse_settings_command(text: str) -> str | None:
    """A settings command's bridge reply, or None if this is not one.
    Unknown keys return the usage text and change nothing."""
    t = (text or "").strip().strip("。.!！?？~～ \u3000")
    for prefix in ("设置", "/设置"):
        if t.startswith(prefix):
            rest = t[len(prefix):].strip().replace(" ", "")
            break
    else:
        return None
    if not rest:
        return settings_usage()
    if rest in ("恢复默认", "重置", "默认"):
        settings_set(dict(SETTINGS_DEFAULTS))
        return ("已恢复默认：媒体等追问开、进度提醒开、回复正常详略。")
    hit = _SETTINGS_WORDS.get(rest)
    if not hit:
        return "没这个设置项，什么都没改。\n" + settings_usage()
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
    t = (text or "").strip().strip("。.!！?？~～ \u3000").lower()
    cmd = _COMMAND_WORDS.get(t)
    if t.startswith("查记录"):
        return "history_search:" + t[3:].strip()
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
            log(f"command {command} failed: {type(e).__name__}: {e}")
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
    with task_lock():
        if load_task(mid):
            log(f"duplicate message id={mid}; skipped")
            return
        save_task(task)
    clear_rate_limit()   # a user message lifts the iLink send limit
    _resume_deferred(sender)
    update_heartbeat(last_inbound_at=now_ms())
    label = (_history_label(kind, text, media_item, bool(quoted_media or stored))
             or ("[语音]" if mute_voice else f"[{kind}]"))
    history_append("user", label, mid=mid, kind=kind)
    quote_put(mid, user=sender, direction="in",
              kind=kind if kind in KIND_LABELS else "text", text=label,
              media=task.get("media_path"), name=task.get("file_name"))
    log(f"task stored id={mid} kind={kind} chars={len(task['text'])}"
        + (" voice=transcribed" if transcribed else "")
        + (" quote=yes" if quote else "")
        + (" quote_from_store=yes" if qinfo.get("from_store") else "")
        + (" quoted_media=yes" if quoted_media or stored else "")
        + (f" unknown_items={unknown}" if unknown else "")
        + (f" command={command}" if command else "")
        + (" command=settings" if settings_reply else ""))
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
            "media_error": (f"{type(err).__name__}: {err}"[:200]
                            if err is not None else None),
        })
        for k in ("voice_format", "voice_wav", "voice_raw_path", "voice_note",
                  "video_md5_ok"):
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


def media_worker_loop() -> None:
    while not _stop.is_set():
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
        # v5: .part (partial ciphertext) is kept for a Range resume
        for leftover in (MEDIA_DIR / f"{mid}.plain", FILES_DIR / f".{mid}.plain"):
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
                  about: dict | None = None) -> str | None:
    """Queue a bridge-written message to `user`; returns its task id."""
    if not NOTICES_ENABLED or not user or not (text or "").strip():
        return None
    if window_state(user)[1] is False:
        log(f"notice not queued (24 h reply window closed) reason={reason}")
        return None
    now = now_ms()
    recent = sum(1 for t in scan_task_index()
                 if t.get("kind") == "notice"
                 and (t.get("received_at") or 0) > now - 3_600_000)
    if recent >= NOTICE_HOURLY_CAP:
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
                      "finished_at": now},
        "send": None,
    }
    with task_lock():
        save_task(task)
    _send_wake.set()
    log(f"notice queued id={tid} reason={reason}")
    return tid


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
    if send.get("notified"):
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
    if nid:
        with task_lock():
            fresh = load_task(task_id)
            if fresh and fresh.get("status") == "send_uncertain":
                fresh["send"] = {**(fresh.get("send") or {}), "notified": nid}
                save_task(fresh)


def check_overdue(index: list) -> None:
    """Messages still unclaimed after 15 minutes mean the hook/executor
    pipeline is stuck: say so once - one notice for all of them, not one per
    message - instead of leaving the user guessing."""
    now = now_ms()
    due = [s_ for s_ in index if s_.get("status") == "pending_execution"
           and not s_.get("overdue_notified")
           and now - (s_.get("received_at") or now) >= OVERDUE_PENDING_MS]
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
             "· 查记录 <关键词> —— 在已落盘对话里找原话\n"
             "· 帮助 —— 显示这段说明\n"
             "其他消息照常交给 Muse 处理。")

_INFLIGHT = ("downloading", "pending_execution", "executing")


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
        line = f"· 「{_asked(t)}」{what}，{fmt_duration(now - since)}"
        if t.get("status") == "pending_execution" and pos:
            line += f"（排队第 {pos + 1} 位，前面还有 {pos} 条）"
        if ex.get("stage"):
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
                  "deferred", "deferrals"):
            send.pop(k, None)
        send.update(attempts=0, max_attempts=SEND_MAX_ATTEMPTS_CONNECT,
                    next_retry_at=0,
                    manual_resends=int(send.get("manual_resends") or 0) + 1)
        t["status"] = "send_failed"
        t["send"] = send
        save_task(t)
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
    hits = [r for r in history_read_all()
            if needle.lower() in (r.get("text") or "").lower()
            and r.get("kind") not in ("command", "topic", "notice")][-5:]
    if not hits:
        return f"查记录“{needle}”：没有找到匹配的对话记录。"
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
            if f.is_file():
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


def backup_status_command() -> str:
    """Opt batch item 3: latest backup / upload result, deterministic only."""
    bdir = BASE_DIR / "backups"
    dated, named = [], []
    total = 0
    try:
        for d in bdir.iterdir():
            if not d.is_dir() or d.name.startswith("."):
                continue
            sz = sum(f.stat().st_size for f in d.rglob("*") if f.is_file())
            total += sz
            (dated if len(d.name) == 15 and d.name[8] == "-" else named).append((d.name, int(d.stat().st_mtime * 1000), sz))
    except OSError:
        pass
    dated.sort(reverse=True)
    lines = ["备份状态（只读落盘记录）："]
    if dated:
        n, ms, sz = dated[0]
        lines.append(f"· 最新带日期快照：{n}（{_fmt_size(sz)}）")
        lines.append(f"· 带日期快照共 {len(dated)} 份，命名快照 {len(named)} 份，合计约 {_fmt_size(total)}")
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
    """A user message: everything parked by the reply budget, the window or
    a rate limit becomes due now (the sender works oldest first)."""
    n = 0
    with task_lock():
        for s_ in scan_task_index():
            send = s_.get("send") or {}
            if (s_.get("from_user_id") != user or s_.get("status") != "send_failed"
                    or send.get("deferred") not in _PARKED
                    or send.get("no_auto_retry")):
                continue
            t = load_task(s_["message_id"])
            if not t or t.get("status") != "send_failed":
                continue
            rec = dict(t.get("send") or {})
            rec.update(next_retry_at=0, deferrals=0)
            t["send"] = rec
            save_task(t)
            n += 1
        for it in list_queue():
            if (it.get("to_user") == user and it.get("status") == "pending"
                    and it.get("deferred") in _PARKED):
                it.update(next_at=0, deferrals=0)
                it.pop("deferred", None)
                save_qitem(it)
                n += 1
    if n:
        log(f"resuming {n} parked send(s) after a new user message")
        _send_wake.set()
        _queue_wake.set()
    return n


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
                "cookie")


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
    try:
        p = Path(os.path.expanduser(raw)).resolve(strict=True)
    except (OSError, RuntimeError):
        return None
    s = str(p)
    if not p.is_file() or s.startswith(("/proc/", "/sys/", "/dev/", "/etc/", "/boot/")) \
            or "/.ssh/" in s or "/.gnupg/" in s:
        return None
    with contextlib.suppress(OSError, ValueError):
        if p.is_relative_to(STATE_DIR.resolve()):
            return None         # credentials, proxy.env, ...
    if any(x in p.name.lower() for x in _ATTACH_DENY):
        return None
    try:
        size = p.stat().st_size
    except OSError:
        return None
    return p if 0 < size <= max(FILE_SEND_MAX_BYTES, VIDEO_SEND_MAX_BYTES) else None


def extract_local_refs(text: str) -> tuple:
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


def _send_part(creds: dict, to_user: str, body: str, ctx, client_id: str) -> dict:
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
        if tail and len(plan) == 1:
            body += tail
        try:
            last_resp = _send_part(creds, to_user, body, ctx, pending["client_id"])
        except ApiError as exc:
            if exc.kind in ("connect", "http") or (
                    exc.kind == "business" and exc.code != RATE_LIMIT_ERRCODE):
                quota_refund(ctx)              # provably not delivered
            if (exc.kind == "business"
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
        if smid:
            quote_put(str(smid), user=to_user, direction="out", kind="text",
                      text=text[s:e], task=task_id)
    attempts = prev_attempts + 1
    parts = parts_sent + len(plan)
    rest = (failure is None and bool(text.strip())
            and bool(_plan_parts(text, _resume_offset(text, done_upto), limit)))
    why = None
    if failure is not None and failure.kind == "business" \
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
    if failure is None and not why:
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
        if manual:
            rec["manual_resends"] = manual
        if failure is not None and pending:
            rec["pending"] = pending
        task["status"] = final_status
        task["send"] = rec
        save_task(task)
    if final_status == "sent":
        update_heartbeat(last_send_ok_at=now_ms())
        if kind != "command" and text.strip():
            history_append("bot", text, mid=task_id, kind=kind)
    log(f"send task={task_id} -> {final_status} "
        f"attempts={outcome.get('attempts', attempts)} parts={parts_sent}/{parts}"
        + (f" deferred={why}" if why else "")
        + (f" error={failure.kind}:{_scrub(failure)}" if failure else ""))
    if final_status == "send_uncertain":
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
    for s_ in index:
        if s_.get("status") != "executing" or s_.get("kind") in ("command", "notice"):
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
        if resp.get("longpolling_timeout_ms"):
            next_timeout = int(resp["longpolling_timeout_ms"])
        new_cursor = resp.get("get_updates_buf")
        if new_cursor:
            cursor = new_cursor      # in memory now; persisted below
        update_heartbeat(last_poll_ok_at=now_ms(), poll_fail_streak=0)
        for m in resp.get("msgs") or []:
            try:
                ingest_message(creds, m)
            except Exception as e:  # noqa: BLE001 - keep polling
                log(f"ingest error: {e}")
        # At-least-once delivery: the cursor used to be saved BEFORE the
        # messages were stored, so a restart in between lost them for good
        # (the server never resends past the cursor). Now it is saved only
        # after the whole batch is on disk; message_id dedupe makes a
        # redelivery after a crash harmless.
        if cursor != saved_cursor:
            try:
                atomic_write_json(CURSOR_FILE, {"get_updates_buf": cursor})
                saved_cursor = cursor
            except OSError as e:
                log(f"cursor save failed: {type(e).__name__}")
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
        for key in ("media_path", "thumb_path", "voice_raw_path"):
            mp = t.get(key) or ""
            if mp:
                referenced.add(mp)
    cutoff = time.time() - PRUNE_AFTER_MS / 1000
    try:
        entries = list(MEDIA_DIR.iterdir())
    except OSError:
        return
    for f in entries:
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
        for key in ("media_path", "thumb_path", "voice_raw_path"):
            mp = t.get(key) or ""
            if mp.startswith((str(MEDIA_DIR), str(FILES_DIR))):
                try:
                    os.unlink(mp)
                except OSError:
                    pass
        try:
            task_path(t["message_id"]).unlink()
            log(f"pruned old task id={t['message_id']}")
        except OSError:
            pass


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
            _notify_send_problem(t["message_id"])


def sender_loop() -> None:
    global _last_prune_epoch
    last_stats = 0.0
    last_overdue = 0.0
    last_auto = 0.0
    while not _stop.is_set():
        _send_wake.clear()
        try:
            idx = scan_task_index()
            recover_stale_sending(idx)
            flush_outbox(idx)
            if time.monotonic() - last_overdue > 60:
                last_overdue = time.monotonic()
                check_overdue(idx)
            if time.monotonic() - last_auto > 15:
                last_auto = time.monotonic()
                check_auto_progress(idx)
        except Exception as e:  # noqa: BLE001
            log(f"sender loop error: {e}")
        if time.time() - _last_prune_epoch > 3600:
            _last_prune_epoch = time.time()
            try:
                prune_old_tasks()
                prune_orphan_media()
                prune_queue()
                quote_prune()
                _prune_partials()
            except Exception as e:  # noqa: BLE001
                log(f"prune error: {e}")
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
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in str(qid))
    return QUEUE_DIR / f"{safe}.json"


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


def _finish_qitem(it: dict, status: str, code: str | None = None,
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
    permanent = e.kind == "business" and any(x in msg for x in (
        "file exceeds", "file not found", "file is empty", "not an image"))
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
        cmd = [ff, "-hide_banner", "-loglevel", "error", "-y", "-i", str(path),
               "-vf", "scale='min(4000,iw)':'min(4000,ih)':force_original_aspect_ratio=decrease",
               *(["-q:v", "2"] if ext == ".jpg" else ["-compression_level", "9"]),
               "-frames:v", "1", str(out)]
    elif ff and kind == "video" and size > VIDEO_COMPRESS_OVER:
        out = ATTACH_DIR / f"{it['qid']}.small.mp4"
        cmd = [ff, "-hide_banner", "-loglevel", "error", "-y", "-i", str(path),
               "-vf", "scale=-2:'min(1080,ih)'", "-c:v", "libx264", "-preset",
               "veryfast", "-crf", "23", "-c:a", "aac", "-b:a", "128k",
               "-movflags", "+faststart", str(out)]
        timeout = 900
    result = ""
    if cmd:
        try:
            proc = subprocess.run(cmd, capture_output=True, timeout=timeout)
            if proc.returncode == 0 and out.is_file() \
                    and 0 < out.stat().st_size < size * 0.85:
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
            ret = resp.get("ret")
            if ret == 0 or (ret is None and resp.get("typing_ticket")):
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
        ret = resp.get("ret")
        if ret not in (None, 0):
            if ret == RATE_LIMIT_ERRCODE:
                self._quiet_until = (time.monotonic()
                                     + RATE_LIMIT_RETRY_MS / 1000)
            self._log_err(f"sendtyping ret={ret}")
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

    class GracefulStop(BaseException):
        # BaseException: as an Exception the receiver loop's `except
        # Exception` swallowed it, logging a bogus "getUpdates unexpected
        # error ... GracefulStop" (and a poll failure in the heartbeat) on
        # every stop/restart, which inflated report.py / doctor error counts.
        pass

    def handle_signal(signum, frame):
        _stop.set()
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
    log(f"serve starting pid={os.getpid()} bot={mask(creds.get('ilink_bot_id'))}")
    PROXY_POOL.start_rotator()
    t_send = threading.Thread(target=sender_loop, daemon=True)
    t_send.start()
    for _ in range(MEDIA_WORKERS):
        threading.Thread(target=media_worker_loop, daemon=True).start()
    threading.Thread(target=typing_loop, name="typing", daemon=True).start()
    threading.Thread(target=queue_worker_loop, name="media-queue",
                     daemon=True).start()
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
        _send_wake.set()
        PID_FILE.unlink(missing_ok=True)
        update_heartbeat(handoff_pid=None, hup_pid=None)
        _sd_notify("STOPPING=1")
        log("serve stopped")
    return 0


# --------------------------------------------------------------------------
# claim / complete / send / status / stop
# --------------------------------------------------------------------------

STALE_EXECUTING_MS = 10 * 60 * 1000


_EXEC_MEDIA_FIELDS = ("media_path", "file_name", "play_length", "media_error")
_EXEC_OPTIONAL_FIELDS = ("media_bytes", "thumb_path")
_EXEC_V4_FIELDS = ("voice_format", "voice_note", "quoted_media",
                   "media_error_code", "media_error_hint",
                   "unsupported_items", "quote_from_store")


def _claim_hint(t: dict) -> str | None:
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


def _executor_view(t: dict) -> dict:
    """What the executor agent needs, nothing more. The full task carries
    raw_items (CDN query params, AES keys), context_token and bookkeeping
    that only cost LLM tokens and put credential-adjacent values into the
    model context. `claim --full` returns the old complete record."""
    out = {"message_id": t["message_id"], "kind": t.get("kind"),
           "text": t.get("text") or ""}
    ex = t.get("execution") or {}
    if ex.get("resumed") or int(ex.get("reclaims") or 0):
        ck = ex.get("checkpoint") or {}
        stage, note = ck.get("stage", ""), ck.get("note", "")
        done = f"上次做到：{stage}" if stage else "上次没有留下阶段记录"
        if note:
            done += f"（{note}）"
        out["resume_from"] = (
            f"断点续跑：这条任务之前已被执行过 {int(ex.get('reclaims') or 0)} 次，"
            f"因超过 10 分钟没心跳被收回。{done}。先按断点接着做，不要从头重做；"
            f"已做完的步骤不要重做一遍，步骤产物以落盘文件为准，不要只信内存。")
    _hint = _claim_hint(t)
    if _hint:
        out["hint"] = _hint
    if t.get("quote"):
        out["quote"] = t["quote"]      # the message the user is replying to
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
    return out


MAX_RECLAIMS = 2


CLAIM_SETTLE_MS = 1200
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


def _claim_pending(limit: int) -> list:
    claimed = []
    with task_lock():
        pending = [x for x in scan_task_index() if x["status"] == "pending_execution"]
        for s_ in pending[:limit]:
            t = load_task(s_["message_id"])
            if not t or t.get("status") != "pending_execution":
                continue
            prev_ex = t.get("execution") or {}
            t["status"] = "executing"
            t["execution"] = {"claimed_at": now_ms(), "by": "muse"}
            if prev_ex.get("reclaims"):
                t["execution"]["reclaims"] = prev_ex["reclaims"]
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


def _settle(settle_ms: int) -> None:
    """Coalesce a burst: while the newest waiting message is younger than
    settle_ms, wait (once) so quick follow-ups are claimed together."""
    if settle_ms <= 0:
        return
    newest = max((int(x.get("received_at") or 0) for x in scan_task_index()
                  if x["status"] == "pending_execution"), default=0)
    age = now_ms() - newest
    if newest and 0 <= age < settle_ms:
        time.sleep((settle_ms - age) / 1000)


def cmd_claim(args) -> int:
    """Hand pending tasks to the executor. v5: `--wait N` blocks up to N s for
    the next message - a warm executor (conversation still in its context)
    takes follow-ups in about a second, with no cold start and no second
    agent; the hook sees the waiter (state/claim_waiter.txt) and does not
    wake another one. A burst is claimed together (--settle-ms, default
    1.2 s when waiting) with a merge_hint for one combined reply."""
    wait_s = min(CLAIM_WAIT_MAX_SEC, max(0.0, float(getattr(args, "wait", 0) or 0)))
    settle = getattr(args, "settle_ms", None)
    settle = int(settle) if settle is not None else (CLAIM_SETTLE_MS if wait_s else 0)
    if _reclaim_stale():
        _wake_serve()
    deadline = time.monotonic() + wait_s
    registered = False
    claimed: list = []
    try:
        while True:
            _settle(settle)
            claimed = _claim_pending(args.limit)
            if claimed or time.monotonic() >= deadline:
                break
            if not registered:
                _write_waiter(deadline)
                registered = True
            time.sleep(0.2)
    finally:
        if registered:
            _clear_waiter()
    view = claimed if args.full else [_executor_view(t) for t in claimed]
    out = {"ok": True, "tasks": view}
    if claimed:
        conv = conversation_context(min(int(t.get("received_at") or now_ms())
                                        for t in claimed))
        if conv:
            out["conversation"] = conv
        st = settings_text(non_default_only=True)
        if st:
            out.setdefault("conversation", {})["settings"] = st
        ids = [t["message_id"] for t in claimed]
        if len(ids) > 1 and len({t.get("from_user_id") for t in claimed}) == 1:
            out["merge_hint"] = (
                f"这 {len(ids)} 条是同一个人连着发的：合成一条完整回复，"
                f"complete --task-id {ids[-1]} --merge-ids {','.join(ids[:-1])} ...")
        last = claimed[-1]
        left = choose_context_token(last.get("from_user_id"),
                                    last.get("context_token"))[1]
        if left is not None:
            out["reply_budget"] = left
    elif wait_s:
        out["waited_s"] = round(wait_s, 1)
    print(json.dumps(out, ensure_ascii=False))
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
            time.sleep(0.05)
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



def _merge_tasks(main_id: str, main: dict, ids: list) -> list:
    """v5 `complete --merge-ids`: messages answered together with main_id (a
    burst of follow-ups) are closed WITHOUT a message of their own - one
    coherent reply instead of one per message, and fewer reply slots used."""
    done_, now = [], now_ms()
    for mid in ids:
        t = load_task(mid)
        if not t or t.get("from_user_id") != main.get("from_user_id") \
                or t.get("status") not in ("executing", "pending_execution"):
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
    def fail(msg: str, **kw) -> int:
        print(json.dumps({"ok": False, "error": msg, **kw}, ensure_ascii=False))
        return 2
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
    if result_text and not args.error and not getattr(args, "raw", False):
        # v5: ![说明](/abs/file) / [名字](/abs/file) pointing at a real local
        # file goes out as an attachment (the text keeps the label); then the
        # Markdown is adapted to what the WeChat client renders.
        if AUTO_ATTACH:
            result_text, found = extract_local_refs(result_text)
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
    merged: list = []
    with task_lock():
        task = load_task(args.task_id)
        status = (task or {}).get("status")
        if not task:
            guard = "task not found"
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
        else:
            ex = dict(task.get("execution") or {})
            ex["finished_at"] = now_ms()
            if args.error:
                ex["error"] = args.error
                task["result_text"] = f"任务执行失败：{args.error}"
            else:
                ex["ok"] = True
                task["result_text"] = result_text
            task["execution"] = ex
            if held:
                task["attachments"] = [it["qid"] for it in held]
            task["status"] = "waiting_send"
            task["send"] = None
            save_task(task)
            merged = _merge_tasks(args.task_id, task, merge_ids)
            for it in held:
                it["status"] = "pending"
                save_qitem(it)
    if guard:
        for it in held:
            _drop_qitem(it["qid"])
        return fail(guard)
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
    out = {"ok": True, "task_id": args.task_id, "send": outcome}
    if dup_warn:
        out["duplicate_warning"] = dup_warn
    if merged:
        out["merged"] = merged
    if auto_attached:
        out["auto_attached"] = [Path(p_).name for p_ in auto_attached]
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
            qids, float(getattr(args, "wait_attachments", 45) or 0))
        if any(a.get("status") not in _QUEUE_TERMINAL for a in out["attachments"]):
            out["attachments_note"] = (
                "未送达的附件由桥接在后台继续重试（约 18 分钟）；最终失败时桥接会直接"
                "告诉用户，执行者不用再另发消息。"
                + ("" if serve_up else "（serve 没在运行：它启动后才会继续重试）"))
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


def _legacy_send(args, kind: str) -> int:
    """send-image / send-file / send-video (v3 commands, same arguments):
    one synchronous round through the media queue. If it fails before
    anything could have been delivered, the file stays queued for background
    retries (unless --no-queue) instead of being lost."""
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
        if t.get("status") != "executing":
            print(json.dumps({"ok": False, "error": f"task status is "
                              f"{t.get('status')} (progress applies while "
                              "executing)"}, ensure_ascii=False))
            return 2
        ex = dict(t.get("execution") or {})
        ex["heartbeat_at"] = now
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
        t["execution"] = ex
        save_task(t)
        user = t.get("from_user_id")
        ctx_tok = t.get("context_token")
    out = {"ok": True, "task_id": args.task_id, "heartbeat_at": now}
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
        if t.get("status") != "executing":
            print(json.dumps({"ok": False, "error": f"task status is "
                              f"{t.get('status')} (heartbeat applies while "
                              f"executing)"}, ensure_ascii=False))
            return 2
        ex = dict(t.get("execution") or {})
        ex["heartbeat_at"] = now
        if stage:
            ex["stage"] = stage[:200]
            ex["stage_at"] = now
        t["execution"] = ex
        save_task(t)
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
        hits = [r for r in history_read_all()
                if needle in (r.get("text") or "").lower()]
        hits = hits[-args.limit:] if args.limit else hits
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


def cmd_decisions(args) -> int:
    """Decision list (opt batch item 2): show, or append one in-progress item."""
    text = _decisions_read()
    if getattr(args, "add", None):
        item = args.add.strip()
        marker = "\n## 已决定"
        entry = f"\n- {item}\n"
        if marker in text:
            text = text.replace(marker, entry + marker, 1)
        else:
            text = text.rstrip() + "\n\n## 进行中\n" + entry
        DECISIONS_FILE.parent.mkdir(parents=True, exist_ok=True)
        DECISIONS_FILE.write_text(text, encoding="utf-8")
        print(json.dumps({"ok": True, "added": item}, ensure_ascii=False))
        return 0
    print(text, end="")
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
        "quote_store_rows": quote_count(),
        "claim_waiter": _waiter_alive(),
        "dedupe_confirmed": _dedupe_confirmed(),
        "net_fast_retries": hb.get("net_fast_retries"),
        "net_resumed_downloads": hb.get("net_resumed"),
        "net_dns_fallbacks": hb.get("net_dns_fallbacks"),
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
        "baseurl": api_base(creds) if creds else None,
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
    lines, written = [], []
    for name in PROXY_VARS:
        value = os.environ.get(name)
        if value:
            lines.append(f"{name}={value}")
            written.append(name)
    if not written:
        print(json.dumps({"ok": False,
                          "error": "no proxy variables in this environment"},
                         ensure_ascii=False))
        return 2
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = PROXY_ENV_FILE.with_suffix(".env.tmp")
    tmp.write_text("\n".join(lines) + "\n", encoding="utf-8")
    os.chmod(tmp, 0o600)
    os.replace(tmp, PROXY_ENV_FILE)
    print(json.dumps({"ok": True, "written_vars": written,
                      "file": str(PROXY_ENV_FILE)}, ensure_ascii=False))
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
    when it refused a send early (reply-budget calibration)."""
    q = _quota_load()
    observed = sorted({int(v["observed_limit"]) for v in q["tokens"].values()
                       if isinstance(v, dict) and v.get("observed_limit") is not None})
    print(json.dumps({"ok": True, "latency_ms": latency_stats(hours=args.hours),
                      "reply_budget": {"per_message": CTX_QUOTA,
                                       "observed_limits": observed},
                      "scale": scale_stats()},
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
    src = Path(args.input)
    dst = Path(args.output) if args.output else src.with_suffix(".wav")
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
    src = Path(args.input)
    dst = Path(args.output) if args.output else src.with_suffix(".wav")
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
                                str(src)], capture_output=True, timeout=60)
            has_audio = bool(p.stdout.strip())
        except (OSError, subprocess.SubprocessError):
            has_audio = None
    if has_audio is False:
        print(json.dumps({"ok": True, "has_audio": False, "wav": None,
                          "note": "video has no audio stream"},
                         ensure_ascii=False))
        return 0
    try:
        p = subprocess.run([ff, "-hide_banner", "-loglevel", "error", "-y",
                            "-i", str(src), "-vn", "-ac", "1", "-ar", "16000",
                            str(dst)], capture_output=True, timeout=600)
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
    src = Path(args.input)
    ff = ffmpeg_path()
    if not ff:
        print(json.dumps({"ok": False, "code": "no_ffmpeg",
                          "error": "ffmpeg not found"}, ensure_ascii=False))
        return 1
    if not src.is_file():
        print(json.dumps({"ok": False, "code": "missing",
                          "error": f"no such file: {src}"}, ensure_ascii=False))
        return 1
    outdir = Path(args.out_dir) if args.out_dir else Path(
        tempfile.mkdtemp(prefix="vframes-"))
    outdir.mkdir(parents=True, exist_ok=True)
    duration = None
    probe = shutil.which("ffprobe")
    if probe:
        try:
            p = subprocess.run([probe, "-hide_banner", "-loglevel", "error",
                                "-show_entries", "format=duration",
                                "-of", "csv=p=0", str(src)],
                               capture_output=True, timeout=60)
            duration = float(p.stdout.decode().strip())
        except (OSError, subprocess.SubprocessError, ValueError):
            duration = None
    frames: list[str] = []
    try:
        p = subprocess.run([ff, "-hide_banner", "-loglevel", "error", "-y",
                            "-i", str(src), "-vf",
                            f"select='gt(scene,{args.threshold})',scale=1280:-1",
                            "-vsync", "vfr", "-frames:v", str(args.max_frames),
                            str(outdir / "scene-%02d.jpg")],
                           capture_output=True, timeout=300)
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
                p = subprocess.run([ff, "-hide_banner", "-loglevel", "error",
                                    "-y", "-ss", str(ss), "-i", str(src),
                                    "-frames:v", "1", "-vf", "scale=1280:-1",
                                    str(dst)], capture_output=True, timeout=120)
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
    p.set_defaults(fn=cmd_send_media)

    p = sub.add_parser("flush")
    p.set_defaults(fn=cmd_flush)

    p = sub.add_parser("status")
    p.set_defaults(fn=cmd_status)

    p = sub.add_parser("claim")
    p.add_argument("--limit", type=int, default=1)
    p.add_argument("--full", action="store_true",
                   help="return complete task records (default: slim)")
    p.add_argument("--wait", type=float, default=0,
                   help="block up to N s for the next message (warm executor)")
    p.add_argument("--settle-ms", type=int, default=None,
                   help="coalesce a burst before claiming (default 1200 with --wait)")
    p.set_defaults(fn=cmd_claim)

    p = sub.add_parser("complete")
    p.add_argument("--task-id", required=True)
    p.add_argument("--result", default=None)
    p.add_argument("--result-file", default=None)
    p.add_argument("--error", default=None)
    p.add_argument("--attach", action="append", default=[],
                   help="file to send after the reply (repeatable)")
    p.add_argument("--attach-as", default="auto", choices=ATTACH_KINDS)
    p.add_argument("--wait-attachments", type=float, default=45)
    p.add_argument("--force", action="store_true",
                   help="replace a reply that is queued or partly delivered")
    p.add_argument("--merge-ids", default=None,
                   help="other task ids this one reply also answers (comma separated)")
    p.add_argument("--raw", action="store_true",
                   help="send the text exactly as given (no Markdown adaptation, "
                        "no auto-attach)")
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
    p.set_defaults(fn=cmd_progress)

    p = sub.add_parser("heartbeat", help="cheap executor heartbeat, no user message")
    p.add_argument("--task-id", required=True)
    p.add_argument("--stage", default=None,
                   help="what step the task is on; also recorded as the checkpoint")
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

    p = sub.add_parser("stats", help="latency p50/p95 and reply-budget calibration")
    p.add_argument("--hours", type=float, default=72)
    p.set_defaults(fn=cmd_stats)

    p = sub.add_parser("canary-dedupe", help="test client_id de-duplication")
    p.add_argument("--confirm", choices=("yes", "no"), default=None)
    p.set_defaults(fn=cmd_canary_dedupe)

    p = sub.add_parser("stop")
    p.set_defaults(fn=cmd_stop)

    p = sub.add_parser("write-proxy-env")
    p.set_defaults(fn=cmd_write_proxy_env)

    args = parser.parse_args()
    for d in (STATE_DIR, INBOX_DIR, OUTBOX_DIR, LOG_DIR, MEDIA_DIR,
              FILES_DIR, QUEUE_DIR, ATTACH_DIR, HISTORY_DIR):
        d.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        os.chmod(HISTORY_DIR, 0o700)
        os.chmod(ATTACH_DIR, 0o700)
    PROXY_POOL.init()
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
