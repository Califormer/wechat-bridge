#!/usr/bin/env python3
"""Open-source fix tests (2026-11-03, from an external review of the
published package): proxy scheme allowlist, server-URL allowlist, and
the AES backend (cryptography -> libcrypto ctypes -> openssl CLI).
Run: python3 tests/test_open_fixes_20261103.py
"""
import importlib.util, os, shutil, subprocess, sys, tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
fails = []


def check(cond, name, detail=""):
    print(("PASS  " if cond else "FAIL  ") + name + (f" ({detail})" if detail and not cond else ""))
    if not cond:
        fails.append(name)


def load(src: Path, name: str):
    d = Path(tempfile.mkdtemp(prefix="wb-test-"))
    shutil.copy(src, d / "bridge.py")
    spec = importlib.util.spec_from_file_location(name, d / "bridge.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.log = lambda msg: None
    return mod


b = load(HERE / "bridge.py", "open_fixes")

# 1. Proxy: only http:// is pool-capable.
check(b._proxy_url_ok("http://127.0.0.1:8080"), "proxy http ok")
check(not b._proxy_url_ok("https://127.0.0.1:443"), "proxy https rejected")
check(not b._proxy_url_ok("socks5://127.0.0.1:1080"), "proxy socks rejected")
check(not b._proxy_url_ok("http://"), "proxy no-host rejected")

# 2. Server URLs: https *.qq.com only; loopback only with the test flag.
for url, ok in [("https://cdn.qq.com/x", True), ("https://qq.com/x", True),
                ("https://mmbiz.qpic.cn/x", True), ("https://wx.qlogo.cn/x", True),
                ("http://cdn.qq.com/x", False), ("https://evil.com/x", False),
                ("https://cdn.qq.com.evil.com/x", False)]:
    try:
        b._validate_server_url(url, "test")
        got = True
    except Exception:
        got = False
    check(got == ok, f"server url {url} -> {ok}")
os.environ.pop("WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL", None)
try:
    b._validate_server_url("http://127.0.0.1:9/x", "test")
    check(False, "loopback rejected without flag")
except Exception:
    check(True, "loopback rejected without flag")
os.environ["WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL"] = "1"
try:
    b._validate_server_url("http://127.0.0.1:9/x", "test")
    check(True, "loopback allowed with flag")
except Exception:
    check(False, "loopback allowed with flag")
finally:
    os.environ.pop("WECHAT_BRIDGE_MEDIA_ALLOW_LOCAL", None)

# 3. AES: FIPS-197 vector, roundtrips, openssl cross-check, file roundtrip.
key = bytes.fromhex("000102030405060708090a0b0c0d0e0f")
plain = bytes.fromhex("00112233445566778899aabbccddeeff")
enc = b._encrypt_aes_ecb(plain, key)
check(enc[:16].hex() == "69c4e0d86a7b0430d8cdb78070b4c55a" and len(enc) == 32,
      "AES FIPS vector first block + PKCS7 padding block")
check(b._decrypt_aes_ecb(enc, key) == plain, "AES decrypt roundtrip")
ref = subprocess.run(["openssl", "enc", "-aes-128-ecb", "-K", key.hex()],
                     input=b"hello world, aes test data!",
                     capture_output=True).stdout
check(b._encrypt_aes_ecb(b"hello world, aes test data!", key) == ref,
      f"AES matches openssl CLI (backend={b._aes_backend()})")
data = os.urandom(100000)
d = Path(tempfile.mkdtemp(prefix="wb-aes-"))
(d / "a.bin").write_bytes(data)
b._encrypt_aes_ecb_file(d / "a.bin", key, d / "a.enc")
b._decrypt_aes_ecb_file(d / "a.enc", key, d / "a.out")
check((d / "a.out").read_bytes() == data, "AES file roundtrip 100KB")
try:
    b._decrypt_aes_ecb(b"not-a-block", key)
    check(False, "AES bad ciphertext rejected")
except Exception:
    check(True, "AES bad ciphertext rejected")

# 4. File decrypt failure must be MediaError("decrypt") on every backend
# (cryptography used to leak a raw ValueError here, skipping the
# FILE_DECRYPT_HINT mapping and the resume-redownload branch).
d2 = Path(tempfile.mkdtemp(prefix="wb-aes-bad-"))
(d2 / "bad.bin").write_bytes(b"\x00" * 4097)  # 4097 is not a multiple of
# the 16-byte block size, so the length itself is invalid on every backend,
# independent of the key (a fixed 4096-byte block only failed deterministically
# because the test key is fixed: with random keys ~0.5% of decryptions pass
# padding by chance, since the ciphertext bytes are not the padding bytes)
try:
    b._decrypt_aes_ecb_file(d2 / "bad.bin", key, d2 / "bad.out")
    check(False, "file decrypt failure raises")
except Exception as e:
    check(isinstance(e, b.MediaError) and getattr(e, "reason", None) == "decrypt",
          f"file decrypt failure is MediaError decrypt (backend={b._aes_backend()})")

# 5. Every available AES backend must behave the same. This guards the CI
# matrix: job 1 (no cryptography) must exercise ctypes, job 2 must exercise
# cryptography, and the openssl CLI backend (never the auto-detected one on
# a normal machine) gets exercised whenever the binary exists. Forcing the
# module global works because _aes_bytes/_aes_stream read it per call.
import ctypes.util
import shutil
available = []
try:
    import cryptography  # noqa: F401
    available.append("cryptography")
except ImportError:
    pass
if ctypes.util.find_library("crypto"):
    available.append("ctypes")
if shutil.which("openssl"):
    available.append("openssl")
check(bool(available), "at least one AES backend available",
      f"found={available}")
for backend in available:
    m = load(HERE / "bridge.py", f"open_fixes_{backend}")
    m._AES_BACKEND = backend
    enc = m._encrypt_aes_ecb(plain, key)
    check(enc[:16].hex() == "69c4e0d86a7b0430d8cdb78070b4c55a"
          and len(enc) == 32, f"AES vector via {backend}")
    check(m._decrypt_aes_ecb(enc, key) == plain,
          f"AES roundtrip via {backend}")
    d3 = Path(tempfile.mkdtemp(prefix="wb-aes-bad-"))
    (d3 / "bad.bin").write_bytes(b"\x00" * 4097)
    try:
        m._decrypt_aes_ecb_file(d3 / "bad.bin", key, d3 / "bad.out")
        check(False, f"bad-length file rejected via {backend}")
    except Exception as e:
        check(isinstance(e, m.MediaError)
              and getattr(e, "reason", None) == "decrypt",
              f"bad-length file is MediaError decrypt via {backend}")

print()
if fails:
    print(f"{len(fails)} FAILED: {fails}")
    sys.exit(1)
print("ALL OPEN-FIX CHECKS PASSED")
