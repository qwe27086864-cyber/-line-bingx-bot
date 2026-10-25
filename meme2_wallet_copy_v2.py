"""
MEME2 rate-limit hardening wrapper.

Keeps the existing Meme2 strategy/paper accounting unchanged and replaces only
its external request pacing.  Goals:
- keep all configured wallets (20 currently) without hammering public Solana RPC;
- stop GeckoTerminal 429 retry storms (Retry-After: 0 is never treated as instant);
- keep stale-copy protection: delayed wallet events are recorded but not entered;
- retain the existing executable-price/liquidity simulation and exit logic.

PAPER ONLY.
"""

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

import meme2_wallet_copy_job as core

ENGINE = "MEME2_RATE_LIMIT_V2"

# Conservative defaults for public endpoints. All can be overridden in Render.
RPC_MIN_INTERVAL_SEC = float(os.environ.get("MEME2_RPC_MIN_INTERVAL_SEC", "1.00"))
RPC_429_COOLDOWN_SEC = float(os.environ.get("MEME2_RPC_429_COOLDOWN_SEC", "12"))
GECKO_MIN_INTERVAL_SEC = float(os.environ.get("MEME2_GECKO_MIN_INTERVAL_SEC", "2.50"))
GECKO_429_COOLDOWN_SEC = float(os.environ.get("MEME2_GECKO_429_COOLDOWN_SEC", "15"))
RETRY_FLOOR_SEC = float(os.environ.get("MEME2_RETRY_FLOOR_SEC", "3.0"))

# Do not reduce wallet count. Lower request fan-out instead.
core.POLL_INTERVAL_SEC = float(os.environ.get("MEME2_POLL_INTERVAL_SEC", "30"))
core.HTTP_RETRIES = int(os.environ.get("MEME2_HTTP_RETRIES", "5"))
core.HTTP_RETRY_BASE_SEC = float(os.environ.get("MEME2_HTTP_RETRY_BASE_SEC", "3.0"))
core.SIG_LIMIT = int(os.environ.get("MEME2_SIG_LIMIT", "8"))
core.MARKET_CACHE_TTL_SEC = float(os.environ.get("MEME2_MARKET_CACHE_TTL_SEC", "180"))
core.MAX_SIGNAL_AGE_SEC = float(os.environ.get("MEME2_MAX_SIGNAL_AGE_SEC", "120"))

_rpc_lock = threading.Lock()
_rpc_next_at = 0.0
_gecko_lock = threading.Lock()
_gecko_next_at = 0.0


def _retry_delay(exc, attempt, cooldown):
    retry_after = exc.headers.get("Retry-After") if getattr(exc, "headers", None) else None
    try:
        header_delay = float(retry_after) if retry_after not in (None, "") else 0.0
    except Exception:
        header_delay = 0.0
    exponential = core.HTTP_RETRY_BASE_SEC * (2 ** attempt)
    # Critical fix: Retry-After: 0 must never mean immediate hammering.
    return max(RETRY_FLOOR_SEC, header_delay, exponential, cooldown if exc.code == 429 else 0.0)


def _wait_slot(kind):
    global _rpc_next_at, _gecko_next_at
    lock = _rpc_lock if kind == "rpc" else _gecko_lock
    interval = RPC_MIN_INTERVAL_SEC if kind == "rpc" else GECKO_MIN_INTERVAL_SEC
    with lock:
        now = time.monotonic()
        target = _rpc_next_at if kind == "rpc" else _gecko_next_at
        if target > now:
            time.sleep(target - now)
        next_at = time.monotonic() + max(0.0, interval)
        if kind == "rpc":
            _rpc_next_at = next_at
        else:
            _gecko_next_at = next_at


def _push_cooldown(kind, seconds):
    global _rpc_next_at, _gecko_next_at
    lock = _rpc_lock if kind == "rpc" else _gecko_lock
    with lock:
        target = time.monotonic() + max(0.0, seconds)
        if kind == "rpc":
            _rpc_next_at = max(_rpc_next_at, target)
        else:
            _gecko_next_at = max(_gecko_next_at, target)


def hardened_json_get(url, headers=None, timeout=12):
    h = {"User-Agent": core.USER_AGENT, "Accept": "application/json"}
    if headers:
        h.update(headers)
    last_exc = None
    for attempt in range(max(1, core.HTTP_RETRIES)):
        _wait_slot("gecko")
        req = urllib.request.Request(url, headers=h, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_exc = exc
            retryable = exc.code == 429 or 500 <= exc.code < 600
            if not retryable or attempt >= core.HTTP_RETRIES - 1:
                raise
            delay = _retry_delay(exc, attempt, GECKO_429_COOLDOWN_SEC)
            if exc.code == 429:
                _push_cooldown("gecko", delay)
            print(f"MEME2 GECKO HTTP {exc.code}; cooldown {delay:.1f}s", flush=True)
            time.sleep(delay)
        except Exception as exc:
            last_exc = exc
            if attempt >= core.HTTP_RETRIES - 1:
                raise
            delay = max(RETRY_FLOOR_SEC, core.HTTP_RETRY_BASE_SEC * (2 ** attempt))
            time.sleep(delay)
    raise RuntimeError(f"HTTP request failed: {last_exc}")


def hardened_rpc(method, params):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode("utf-8")
    last_exc = None
    for attempt in range(max(1, core.HTTP_RETRIES)):
        _wait_slot("rpc")
        req = urllib.request.Request(
            core.SOLANA_RPC,
            data=body,
            method="POST",
            headers={"Content-Type": "application/json", "User-Agent": core.USER_AGENT},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if payload.get("error"):
                raise RuntimeError(f"RPC {method} error: {payload['error']}")
            return payload.get("result")
        except urllib.error.HTTPError as exc:
            last_exc = exc
            retryable = exc.code == 429 or 500 <= exc.code < 600
            if not retryable or attempt >= core.HTTP_RETRIES - 1:
                raise
            delay = _retry_delay(exc, attempt, RPC_429_COOLDOWN_SEC)
            if exc.code == 429:
                _push_cooldown("rpc", delay)
            print(f"MEME2 RPC HTTP {exc.code}; cooldown {delay:.1f}s", flush=True)
            time.sleep(delay)
        except Exception as exc:
            last_exc = exc
            if attempt >= core.HTTP_RETRIES - 1:
                raise
            delay = max(RETRY_FLOOR_SEC, core.HTTP_RETRY_BASE_SEC * (2 ** attempt))
            time.sleep(delay)
    raise RuntimeError(f"RPC {method} failed: {last_exc}")


# Monkey-patch only transport. Strategy, wallet parsing, entries/exits and DB remain unchanged.
core._json_get = hardened_json_get
core.rpc = hardened_rpc
core.ENGINE = ENGINE


def health_summary():
    return (
        f"MEME2 transport V2 | wallets={len(core.WALLETS)} "
        f"poll={core.POLL_INTERVAL_SEC:.0f}s sig_limit={core.SIG_LIMIT} "
        f"rpc_gap={RPC_MIN_INTERVAL_SEC:.2f}s gecko_gap={GECKO_MIN_INTERVAL_SEC:.2f}s "
        f"market_cache={core.MARKET_CACHE_TTL_SEC:.0f}s stale_cutoff={core.MAX_SIGNAL_AGE_SEC:.0f}s"
    )


def run_once():
    print(health_summary(), flush=True)
    core.run_once()


def monitor_loop():
    core.init_database()
    core._start_health_server()
    print(health_summary(), flush=True)
    print(f"MEME2 monitor started interval={core.POLL_INTERVAL_SEC:.1f}s wallets={len(core.WALLETS)} port={core.HEALTH_PORT}", flush=True)
    while True:
        started = time.time()
        try:
            core.run_once()
        except Exception as exc:
            print(f"MEME2 monitor error: {type(exc).__name__}: {exc}", flush=True)
        elapsed = time.time() - started
        time.sleep(max(1.0, core.POLL_INTERVAL_SEC - elapsed))


if __name__ == "__main__":
    mode = os.environ.get("MEME2_MODE", "cron").strip().lower()
    if "--monitor" in sys.argv or mode == "monitor":
        monitor_loop()
    else:
        run_once()
