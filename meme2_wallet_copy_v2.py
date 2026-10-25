"""
MEME2 V3 — rate-limit hardening + wallet quality gate + verified exits.

PAPER ONLY.

Changes from V2:
1) Wallet quality gate:
   - after enough closed trades, wallets with persistently negative PF/PnL stop
     opening NEW copy positions;
   - ultra-short wallets whose median holding time is too short to cover copy
     costs are also blocked from NEW buys;
   - SELL events are never blocked, so existing positions can still mirror-exit.

2) Price verification:
   - suspicious one-tick price jumps are verified against the token's most
     liquid GeckoTerminal pool before they can update peak/last price;
   - every STOP/TP/TRAIL/MAX_HOLD candidate is re-evaluated using a fresh pool
     price before closing;
   - a stale/bad batch quote can no longer directly create a false stop/TP.

3) Keeps V2 public-endpoint pacing and stale-signal protection.

This file intentionally wraps meme2_wallet_copy_job.py instead of rewriting the
whole core, so the original accounting and wallet parser stay comparable.
"""

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

import meme2_wallet_copy_job as core

ENGINE = "MEME2_V3"

# ---------------------------------------------------------------------------
# Transport / rate-limit settings
# ---------------------------------------------------------------------------

RPC_MIN_INTERVAL_SEC = float(os.environ.get("MEME2_RPC_MIN_INTERVAL_SEC", "1.00"))
RPC_429_COOLDOWN_SEC = float(os.environ.get("MEME2_RPC_429_COOLDOWN_SEC", "12"))
GECKO_MIN_INTERVAL_SEC = float(os.environ.get("MEME2_GECKO_MIN_INTERVAL_SEC", "2.50"))
GECKO_429_COOLDOWN_SEC = float(os.environ.get("MEME2_GECKO_429_COOLDOWN_SEC", "15"))
RETRY_FLOOR_SEC = float(os.environ.get("MEME2_RETRY_FLOOR_SEC", "3.0"))

core.POLL_INTERVAL_SEC = float(os.environ.get("MEME2_POLL_INTERVAL_SEC", "30"))
core.HTTP_RETRIES = int(os.environ.get("MEME2_HTTP_RETRIES", "5"))
core.HTTP_RETRY_BASE_SEC = float(os.environ.get("MEME2_HTTP_RETRY_BASE_SEC", "3.0"))
core.SIG_LIMIT = int(os.environ.get("MEME2_SIG_LIMIT", "8"))
core.MARKET_CACHE_TTL_SEC = float(os.environ.get("MEME2_MARKET_CACHE_TTL_SEC", "180"))
core.MAX_SIGNAL_AGE_SEC = float(os.environ.get("MEME2_MAX_SIGNAL_AGE_SEC", "120"))

# MEME2_MAX_OPEN was temporarily set to 0 while V3 was prepared. V3 uses a
# separate variable so a stale safety-freeze env var cannot accidentally keep
# the strategy disabled after this file is deployed.
core.MAX_OPEN = int(os.environ.get("MEME2_ACTIVE_MAX_OPEN", "20"))

# ---------------------------------------------------------------------------
# Wallet quality gate
# ---------------------------------------------------------------------------

WALLET_GATE_ENABLED = os.environ.get("MEME2_WALLET_GATE_ENABLED", "true").lower() == "true"
WALLET_MIN_CLOSED = int(os.environ.get("MEME2_WALLET_MIN_CLOSED", "5"))
WALLET_MIN_PF = float(os.environ.get("MEME2_WALLET_MIN_PF", "0.70"))
WALLET_STATS_TTL_SEC = float(os.environ.get("MEME2_WALLET_STATS_TTL_SEC", "300"))

# Copyability guard. With current execution assumptions, a wallet that usually
# buys and sells within seconds and creates only tiny raw price movement cannot
# cover copy entry/exit costs.
SHORT_HOLD_MAX_SEC = float(os.environ.get("MEME2_SHORT_HOLD_MAX_SEC", "45"))
SHORT_HOLD_MIN_RAW_EDGE_PCT = float(os.environ.get("MEME2_SHORT_HOLD_MIN_RAW_EDGE_PCT", "6.0"))

# ---------------------------------------------------------------------------
# Market-data verification
# ---------------------------------------------------------------------------

MAX_UNVERIFIED_TICK_MULTIPLIER = float(
    os.environ.get("MEME2_MAX_UNVERIFIED_TICK_MULTIPLIER", "2.0")
)
MAX_EXIT_PRICE_DIVERGENCE_PCT = float(
    os.environ.get("MEME2_MAX_EXIT_PRICE_DIVERGENCE_PCT", "15")
)

_rpc_lock = threading.Lock()
_rpc_next_at = 0.0
_gecko_lock = threading.Lock()
_gecko_next_at = 0.0
_wallet_stats_cache = {}


def _retry_delay(exc, attempt, cooldown):
    retry_after = exc.headers.get("Retry-After") if getattr(exc, "headers", None) else None
    try:
        header_delay = float(retry_after) if retry_after not in (None, "") else 0.0
    except Exception:
        header_delay = 0.0
    exponential = core.HTTP_RETRY_BASE_SEC * (2 ** attempt)
    return max(
        RETRY_FLOOR_SEC,
        header_delay,
        exponential,
        cooldown if exc.code == 429 else 0.0,
    )


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
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    ).encode("utf-8")

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


def wallet_stats(wallet):
    now = time.monotonic()
    cached = _wallet_stats_cache.get(wallet)
    if cached and now - cached["ts"] <= WALLET_STATS_TTL_SEC:
        return cached["stats"]

    conn = core.db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status <> 'OPEN') AS closed,
                COUNT(*) FILTER (WHERE status <> 'OPEN' AND net_pnl_usdt > 0) AS wins,
                COALESCE(SUM(CASE WHEN status <> 'OPEN' THEN net_pnl_usdt ELSE 0 END),0) AS net_pnl,
                COALESCE(SUM(CASE WHEN status <> 'OPEN' AND net_pnl_usdt > 0 THEN net_pnl_usdt ELSE 0 END),0) AS gross_profit,
                ABS(COALESCE(SUM(CASE WHEN status <> 'OPEN' AND net_pnl_usdt < 0 THEN net_pnl_usdt ELSE 0 END),0)) AS gross_loss,
                PERCENTILE_CONT(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (closed_at-opened_at))
                ) FILTER (WHERE status <> 'OPEN') AS median_hold_sec,
                AVG(
                    CASE
                        WHEN status <> 'OPEN'
                         AND raw_entry_price > 0
                         AND last_price > 0
                        THEN ((last_price/raw_entry_price)-1.0)*100.0
                    END
                ) AS avg_raw_move_pct
            FROM meme2_positions
            WHERE source_wallet=%s;
            """,
            (wallet,),
        )
        row = cur.fetchone()
    finally:
        cur.close()
        conn.close()

    closed = int(row[0] or 0)
    wins = int(row[1] or 0)
    net_pnl = float(row[2] or 0.0)
    gross_profit = float(row[3] or 0.0)
    gross_loss = float(row[4] or 0.0)
    median_hold_sec = float(row[5]) if row[5] is not None else None
    avg_raw_move_pct = float(row[6]) if row[6] is not None else None

    pf = (gross_profit / gross_loss) if gross_loss > 0 else None
    win_rate = (wins / closed * 100.0) if closed > 0 else None

    stats = {
        "closed": closed,
        "wins": wins,
        "net_pnl": net_pnl,
        "gross_profit": gross_profit,
        "gross_loss": gross_loss,
        "pf": pf,
        "win_rate": win_rate,
        "median_hold_sec": median_hold_sec,
        "avg_raw_move_pct": avg_raw_move_pct,
    }
    _wallet_stats_cache[wallet] = {"ts": now, "stats": stats}
    return stats


def wallet_entry_allowed(wallet):
    if not WALLET_GATE_ENABLED:
        return True, "GATE_DISABLED", None

    s = wallet_stats(wallet)
    if s["closed"] < WALLET_MIN_CLOSED:
        return True, "INSUFFICIENT_SAMPLE", s

    pf = s["pf"]
    if s["net_pnl"] < 0 and pf is not None and pf < WALLET_MIN_PF:
        return False, "WALLET_NEGATIVE_EXPECTANCY", s

    hold = s["median_hold_sec"]
    raw_edge = s["avg_raw_move_pct"]
    if (
        hold is not None
        and raw_edge is not None
        and hold <= SHORT_HOLD_MAX_SEC
        and raw_edge < SHORT_HOLD_MIN_RAW_EDGE_PCT
    ):
        return False, "WALLET_UNCOPYABLE_SHORT_HOLD", s

    return True, "PASS", s


_original_process_change = core.process_change


def gated_process_change(wallet, signature, block_time_unix, ch):
    # Never block SELLs: current positions must remain fully manageable.
    if ch.get("side") != "BUY":
        return _original_process_change(wallet, signature, block_time_unix, ch)

    allowed, gate_reason, stats = wallet_entry_allowed(wallet)
    if allowed:
        return _original_process_change(wallet, signature, block_time_unix, ch)

    now = time.time()
    delay = max(0.0, now - float(block_time_unix or now))
    block_dt = (
        datetime.fromtimestamp(block_time_unix, tz=timezone.utc)
        if block_time_unix
        else None
    )

    pf = stats.get("pf")
    reason = (
        f"{gate_reason} "
        f"closed={stats['closed']} "
        f"pf={(pf if pf is not None else 0.0):.3f} "
        f"pnl={stats['net_pnl']:+.3f} "
        f"hold={(stats.get('median_hold_sec') or 0.0):.1f}s "
        f"raw={(stats.get('avg_raw_move_pct') or 0.0):+.2f}%"
    )

    core.log_signal(
        wallet,
        signature,
        block_dt,
        delay,
        ch,
        None,
        None,
        "SKIP",
        reason,
        {
            "wallet_gate": True,
            "gate_reason": gate_reason,
            "wallet_stats": stats,
        },
    )

    print(
        f"MEME2 BLOCK BUY wallet={wallet[:8]}.. {reason}",
        flush=True,
    )


def _fresh_verified_market(pos, batch_price, force=False):
    """Return a trusted market row for a position.

    Normal ticks use the fast batch quote unless it is an extreme one-tick jump.
    Extreme ticks and all exit candidates are checked against the most-liquid
    pool price.
    """
    batch_price = core._safe_float(batch_price)
    reference = (
        core._safe_float(pos.get("last_price"))
        or core._safe_float(pos.get("raw_entry_price"))
    )

    suspicious = False
    if batch_price <= 0:
        suspicious = True
    elif reference > 0:
        ratio = batch_price / reference
        limit = max(1.01, MAX_UNVERIFIED_TICK_MULTIPLIER)
        suspicious = ratio >= limit or ratio <= (1.0 / limit)

    if not suspicious and not force:
        return {
            "price": batch_price,
            "verified": False,
            "divergence_pct": 0.0,
            "market": None,
        }

    market = core.get_token_market_cached(pos["mint_address"], force=True)
    if not market or core._safe_float(market.get("price")) <= 0:
        return None

    fresh_price = core._safe_float(market["price"])
    divergence = (
        abs(fresh_price / batch_price - 1.0) * 100.0
        if batch_price > 0
        else 999.0
    )
    return {
        "price": fresh_price,
        "verified": True,
        "divergence_pct": divergence,
        "market": market,
    }


def verified_refresh_open_positions():
    opens = core.get_open_positions()
    if not opens:
        return

    prices = core.get_prices([p["mint_address"] for p in opens])

    for pos in opens:
        batch_price = prices.get(pos["mint_address"])
        trusted = _fresh_verified_market(pos, batch_price, force=False)

        if trusted is None:
            core.log_position_snapshot(
                pos,
                None,
                peak=float(pos.get("peak_price") or pos["raw_entry_price"]),
                reason_now="MARKET_DATA_MISSING",
                market=None,
            )
            continue

        price = trusted["price"]
        previous_peak = float(pos.get("peak_price") or pos["raw_entry_price"])
        peak = max(previous_peak, price)
        reason = core.price_exit_reason(pos, price)

        # Every exit decision gets a fresh same-pool verification.
        if reason:
            verified = _fresh_verified_market(pos, batch_price, force=True)
            if verified is None:
                core.log_position_snapshot(
                    pos,
                    price,
                    peak=previous_peak,
                    reason_now=f"EXIT_VERIFY_MISSING candidate={reason}",
                    market=None,
                )
                continue

            fresh_price = verified["price"]
            fresh_reason = core.price_exit_reason(pos, fresh_price)
            divergence = verified["divergence_pct"]
            fresh_market = dict(verified["market"] or {})
            fresh_market["price"] = fresh_price
            fresh_peak = max(previous_peak, fresh_price)

            # If the fast quote and pool quote disagree materially, or the fresh
            # pool price no longer supports the exit, quarantine the old quote.
            if (
                fresh_reason != reason
                or divergence > MAX_EXIT_PRICE_DIVERGENCE_PCT
            ):
                note = (
                    f"EXIT_QUARANTINE candidate={reason} fresh={fresh_reason or 'NONE'} "
                    f"divergence={divergence:.1f}%"
                )
                core.log_position_snapshot(
                    pos,
                    fresh_price,
                    peak=fresh_peak,
                    reason_now=note,
                    market=fresh_market,
                )
                core.update_mark(pos, fresh_price, fresh_peak)
                print(f"MEME2 {note} pos={pos['id']}", flush=True)
                continue

            core.log_position_snapshot(
                pos,
                fresh_price,
                peak=fresh_peak,
                reason_now=fresh_reason,
                market=fresh_market,
            )
            net, ret = core.close_position(pos, fresh_market, fresh_reason)
            print(
                f"MEME2 VERIFIED CLOSE #{pos['id']} {fresh_reason} "
                f"net={net:+.4f}U ret={ret:+.2f}%",
                flush=True,
            )
            continue

        # No exit. If the batch quote itself was suspicious and had to be
        # replaced by a pool quote, record that fact but do not punish the trade.
        market = core.get_token_market_cached(pos["mint_address"])
        snapshot_market = dict(market or {})
        snapshot_market["price"] = price

        note = None
        if trusted["verified"] and trusted["divergence_pct"] > MAX_EXIT_PRICE_DIVERGENCE_PCT:
            note = f"PRICE_QUARANTINE divergence={trusted['divergence_pct']:.1f}%"

        core.log_position_snapshot(
            pos,
            price,
            peak=peak,
            reason_now=note,
            market=snapshot_market,
        )
        core.update_mark(pos, price, peak)


# Install V3 patches.
core._json_get = hardened_json_get
core.rpc = hardened_rpc
core.process_change = gated_process_change
core.refresh_open_positions = verified_refresh_open_positions
core.ENGINE = ENGINE


def health_summary():
    return (
        f"MEME2 V3 | wallets={len(core.WALLETS)} "
        f"poll={core.POLL_INTERVAL_SEC:.0f}s sig_limit={core.SIG_LIMIT} "
        f"wallet_gate={WALLET_GATE_ENABLED} "
        f"min_closed={WALLET_MIN_CLOSED} min_pf={WALLET_MIN_PF:.2f} "
        f"short_hold={SHORT_HOLD_MAX_SEC:.0f}s "
        f"active_max_open={core.MAX_OPEN} "
        f"price_verify={MAX_EXIT_PRICE_DIVERGENCE_PCT:.0f}%"
    )


def run_once():
    print(health_summary(), flush=True)
    core.run_once()


def monitor_loop():
    core.init_database()
    core._start_health_server()
    print(health_summary(), flush=True)
    print(
        f"MEME2 monitor started interval={core.POLL_INTERVAL_SEC:.1f}s "
        f"wallets={len(core.WALLETS)} port={core.HEALTH_PORT}",
        flush=True,
    )

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
