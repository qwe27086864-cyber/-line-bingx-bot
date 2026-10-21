"""
R2 execution-aware paper trading core.

Design goals
------------
- Independent from the old R1 signal core.
- Paper only: never sends BingX private trade orders.
- Multi-timeframe direction (1m / 5m / 15m / 1h).
- Regime-aware (TREND / BREAKOUT / TRANSITION / RANGE).
- Execution-aware: fresh bid/ask, spread, signal-to-entry chase.
- Short-horizon continuation score decides whether a trade is worth taking.
- Dedicated R2 radar snapshots and decision log.
- Uses the existing conservative V4.2 1-minute replay for exits:
  TP1/TP2/TP3 = 2/4/8%, SL = 2.5%, TP1 -> BE, TP2 -> TP1 protection,
  timeout = 4 hours.
- 10 USDT margin x 8 leverage, max 3 concurrent R2 trades.
"""

import json
import math
import os
import time
from datetime import datetime, timezone

import app as base

ENGINE = "R2"

# ---------------------------------------------------------------------------
# R2 configuration
# ---------------------------------------------------------------------------

R2_MAX_OPEN = int(os.environ.get("R2_MAX_OPEN", "3"))
R2_MAX_NEW_PER_SCAN = int(os.environ.get("R2_MAX_NEW_PER_SCAN", "2"))
R2_COOLDOWN_HOURS = float(os.environ.get("R2_COOLDOWN_HOURS", "2"))

R2_RADAR_MIN_MOVE = float(os.environ.get("R2_RADAR_MIN_MOVE", "0.40"))
R2_RADAR_MAX_CANDIDATES = int(os.environ.get("R2_RADAR_MAX_CANDIDATES", "8"))
R2_MAX_DEEP_ANALYSIS = int(os.environ.get("R2_MAX_DEEP_ANALYSIS", "5"))
R2_MIN_QUOTE_VOLUME = float(os.environ.get("R2_MIN_QUOTE_VOLUME", "250000"))
R2_SCAN_MAX_SPREAD = float(os.environ.get("R2_SCAN_MAX_SPREAD", "0.80"))

R2_ENTER_MAX_SPREAD = float(os.environ.get("R2_ENTER_MAX_SPREAD", "0.60"))
R2_ENTER_MAX_CHASE = float(os.environ.get("R2_ENTER_MAX_CHASE", "0.75"))
R2_BREAKOUT_MAX_CHASE = float(os.environ.get("R2_BREAKOUT_MAX_CHASE", "1.20"))

R2_MIN_DIRECTION_SCORE = float(os.environ.get("R2_MIN_DIRECTION_SCORE", "70"))
R2_MIN_MICRO_SCORE = float(os.environ.get("R2_MIN_MICRO_SCORE", "58"))
R2_MIN_FORWARD_SCORE = float(os.environ.get("R2_MIN_FORWARD_SCORE", "72"))
R2_TRANSITION_MIN_FORWARD = float(os.environ.get("R2_TRANSITION_MIN_FORWARD", "80"))
R2_BREAKOUT_CHASE_FORWARD = float(os.environ.get("R2_BREAKOUT_CHASE_FORWARD", "88"))

R2_DAILY_MAX_LOSS_USDT = float(os.environ.get("R2_DAILY_MAX_LOSS_USDT", "20"))
R2_MAX_CONSECUTIVE_LOSSES = int(os.environ.get("R2_MAX_CONSECUTIVE_LOSSES", "4"))
R2_PAUSE_HOURS_AFTER_STREAK = float(os.environ.get("R2_PAUSE_HOURS_AFTER_STREAK", "6"))

# Keep phase-1 exits identical to the R1/R1M scaffold so the core can be
# evaluated without mixing in a new exit model at the same time.
base.SCANNER_ENGINE_VERSION = ENGINE
base.SCANNER_MAX_OPEN = R2_MAX_OPEN
base.SCANNER_MAX_NEW_PER_SCAN = R2_MAX_NEW_PER_SCAN
base.SCANNER_COOLDOWN_HOURS = R2_COOLDOWN_HOURS
base.SCANNER_TP1 = 2.0
base.SCANNER_TP2 = 4.0
base.SCANNER_TP3 = 8.0
base.SCANNER_SL = 2.5
base.V42_POSITION_TIMEOUT_HOURS = 4.0
base.SIM_MARGIN_USDT = 10.0
base.SIM_LEVERAGE = 8

# R2 is always paper-only. It only uses public market endpoints + Postgres.
base.LIVE_TRADING = False
if hasattr(base, "R1_LIVE_TRADING"):
    base.R1_LIVE_TRADING = False


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def db():
    return base.get_db_connection()


def init_r2_database():
    base.init_database()
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS r2_radar_snapshots (
                symbol VARCHAR(50) PRIMARY KEY,
                last_price DOUBLE PRECISION NOT NULL,
                bid_price DOUBLE PRECISION,
                ask_price DOUBLE PRECISION,
                quote_volume_24h DOUBLE PRECISION,
                price_change_24h DOUBLE PRECISION,
                captured_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS r2_decisions (
                id BIGSERIAL PRIMARY KEY,
                observed_at TIMESTAMPTZ DEFAULT NOW(),
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL,
                regime VARCHAR(20) NOT NULL,
                strategy VARCHAR(20) NOT NULL,
                radar_move_pct DOUBLE PRECISION,
                direction_score DOUBLE PRECISION,
                micro_score DOUBLE PRECISION,
                execution_score DOUBLE PRECISION,
                forward_score DOUBLE PRECISION,
                signal_price DOUBLE PRECISION,
                last_price DOUBLE PRECISION,
                bid_price DOUBLE PRECISION,
                ask_price DOUBLE PRECISION,
                market_entry_price DOUBLE PRECISION,
                spread_pct DOUBLE PRECISION,
                chase_pct DOUBLE PRECISION,
                decision VARCHAR(40) NOT NULL,
                reason TEXT,
                scanner_trade_id BIGINT,
                features JSONB
            );
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Market helpers
# ---------------------------------------------------------------------------

def valid_symbol(symbol):
    return base.valid_scanner_symbol(symbol)


def all_tickers():
    result = base.bingx_public_request("/openApi/swap/v2/quote/ticker")
    if result.get("code") != 0:
        raise RuntimeError(f"Ticker failed: {result}")

    data = result.get("data", [])
    if isinstance(data, dict):
        data = [data]

    out = []
    for item in data:
        try:
            symbol = str(item.get("symbol", "")).upper()
            if not valid_symbol(symbol):
                continue

            last = float(item.get("lastPrice", 0) or 0)
            bid = float(item.get("bidPrice", 0) or 0)
            ask = float(item.get("askPrice", 0) or 0)
            quote_volume = float(item.get("quoteVolume", 0) or 0)
            change_24h = float(item.get("priceChangePercent", 0) or 0)

            if last <= 0:
                continue

            spread = None
            if bid > 0 and ask > 0:
                mid = (bid + ask) / 2.0
                if mid > 0:
                    spread = (ask - bid) / mid * 100.0

            out.append({
                "symbol": symbol,
                "last": last,
                "bid": bid,
                "ask": ask,
                "spread_pct": spread,
                "quote_volume": quote_volume,
                "change_24h": change_24h,
            })
        except Exception:
            continue

    return out


def fresh_ticker(symbol):
    result = base.bingx_public_request(
        "/openApi/swap/v2/quote/ticker",
        {"symbol": symbol},
    )
    if result.get("code") != 0:
        raise RuntimeError(f"Fresh ticker failed: {result}")

    data = result.get("data", {})
    if isinstance(data, list):
        item = next(
            (x for x in data if str(x.get("symbol", "")).upper() == symbol.upper()),
            data[0] if data else {},
        )
    elif isinstance(data, dict):
        item = data
    else:
        item = {}

    bid = float(item.get("bidPrice", 0) or 0)
    ask = float(item.get("askPrice", 0) or 0)
    last = float(item.get("lastPrice", item.get("price", 0)) or 0)

    spread = None
    if bid > 0 and ask > 0:
        mid = (bid + ask) / 2.0
        if mid > 0:
            spread = (ask - bid) / mid * 100.0

    return {"bid": bid, "ask": ask, "last": last, "spread_pct": spread}


def get_klines_retry(symbol, interval, limit, attempts=3):
    last_error = None
    for attempt in range(attempts):
        try:
            return base.get_klines(symbol, interval, limit)
        except Exception as exc:
            last_error = exc
            if attempt + 1 >= attempts:
                break
            time.sleep(0.7 * (2 ** attempt))
    raise last_error


def load_snapshots():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT symbol, last_price, captured_at
            FROM r2_radar_snapshots;
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    return {
        symbol: {"price": float(price), "captured_at": captured_at}
        for symbol, price, captured_at in rows
    }


def save_snapshots(tickers):
    conn = db()
    cur = conn.cursor()
    try:
        for t in tickers:
            cur.execute(
                """
                INSERT INTO r2_radar_snapshots (
                    symbol, last_price, bid_price, ask_price,
                    quote_volume_24h, price_change_24h, captured_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (symbol)
                DO UPDATE SET
                    last_price=EXCLUDED.last_price,
                    bid_price=EXCLUDED.bid_price,
                    ask_price=EXCLUDED.ask_price,
                    quote_volume_24h=EXCLUDED.quote_volume_24h,
                    price_change_24h=EXCLUDED.price_change_24h,
                    captured_at=NOW();
                """,
                (
                    t["symbol"], t["last"], t["bid"], t["ask"],
                    t["quote_volume"], t["change_24h"],
                ),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Feature helpers
# ---------------------------------------------------------------------------

def clamp(x, lo=0.0, hi=100.0):
    return max(lo, min(hi, float(x)))


def pct(a, b):
    if not b:
        return 0.0
    return (a - b) / b * 100.0


def directional(side, value):
    return value if side == "LONG" else -value


def volume_ratio(volumes, lookback=20):
    if len(volumes) < lookback + 1:
        return 0.0
    prev = volumes[-(lookback + 1):-1]
    avg = sum(prev) / len(prev) if prev else 0
    return volumes[-1] / avg if avg > 0 else 0.0


def directional_extension(side, price, ema):
    if not ema:
        return 0.0
    raw = pct(price, ema)
    return raw if side == "LONG" else -raw


def aligned(side, price, fast_ema, slow_ema):
    if side == "LONG":
        return price > fast_ema > slow_ema
    return price < fast_ema < slow_ema


def trend_label(price, fast_ema, slow_ema):
    if price > fast_ema > slow_ema:
        return "UP"
    if price < fast_ema < slow_ema:
        return "DOWN"
    return "MIXED"


def build_candidates(current, previous):
    now = datetime.now(timezone.utc)
    candidates = []

    for t in current:
        old = previous.get(t["symbol"])
        if not old or old["price"] <= 0 or not old["captured_at"]:
            continue

        age_min = (now - old["captured_at"]).total_seconds() / 60.0
        if age_min < 2 or age_min > 20:
            continue

        spread = t["spread_pct"]
        if spread is None or spread > R2_SCAN_MAX_SPREAD:
            continue
        if t["quote_volume"] < R2_MIN_QUOTE_VOLUME:
            continue

        move = pct(t["last"], old["price"])
        abs_move = abs(move)
        if abs_move < R2_RADAR_MIN_MOVE:
            continue

        side = "LONG" if move > 0 else "SHORT"

        # Ranking only. Entry eligibility is decided later by the core.
        rank = 0.0
        rank += min(abs_move, 6.0) * 12.0
        if t["quote_volume"] >= 1_000_000:
            rank += 8
        if t["quote_volume"] >= 5_000_000:
            rank += 6
        if spread <= 0.20:
            rank += 12
        elif spread <= 0.35:
            rank += 7
        if directional(side, t["change_24h"]) > 0:
            rank += 5

        candidates.append({
            **t,
            "side": side,
            "radar_move_pct": move,
            "rank": rank,
            # The scan-time last price is the signal reference. A new BBO
            # is fetched only after the analysis finishes.
            "signal_price": t["last"],
        })

    candidates.sort(key=lambda x: (x["rank"], abs(x["radar_move_pct"])), reverse=True)
    return candidates[:R2_RADAR_MAX_CANDIDATES]


# ---------------------------------------------------------------------------
# R2 core
# ---------------------------------------------------------------------------

def analyze_candidate(radar):
    symbol = radar["symbol"]
    side = radar["side"]

    c1 = get_klines_retry(symbol, "1m", 90)
    c5 = get_klines_retry(symbol, "5m", 100)
    c15 = get_klines_retry(symbol, "15m", 100)
    c1h = get_klines_retry(symbol, "1h", 90)

    # Completed candles only: no unfinished-candle look-ahead.
    c1 = c1[:-1]
    c5 = c5[:-1]
    c15 = c15[:-1]
    c1h = c1h[:-1]

    if min(len(c1), len(c5), len(c15), len(c1h)) < 55:
        return None

    close1 = [x["close"] for x in c1]
    close5 = [x["close"] for x in c5]
    close15 = [x["close"] for x in c15]
    close1h = [x["close"] for x in c1h]

    high5 = [x["high"] for x in c5]
    low5 = [x["low"] for x in c5]
    vol1 = [x["volume"] for x in c1]
    vol5 = [x["volume"] for x in c5]
    vol15 = [x["volume"] for x in c15]

    p1 = close1[-1]
    p5 = close5[-1]
    p15 = close15[-1]
    p1h = close1h[-1]

    ema5_1 = base.calculate_ema(close1, 5)
    ema13_1 = base.calculate_ema(close1, 13)
    ema9_5 = base.calculate_ema(close5, 9)
    ema20_5 = base.calculate_ema(close5, 20)
    ema20_15 = base.calculate_ema(close15, 20)
    ema50_15 = base.calculate_ema(close15, 50)
    ema20_1h = base.calculate_ema(close1h, 20)
    ema50_1h = base.calculate_ema(close1h, 50)

    rsi1 = base.calculate_rsi(close1, 14)
    rsi5 = base.calculate_rsi(close5, 14)
    rsi15 = base.calculate_rsi(close15, 14)

    required = [
        ema5_1, ema13_1, ema9_5, ema20_5,
        ema20_15, ema50_15, ema20_1h, ema50_1h,
        rsi1, rsi5, rsi15,
    ]
    if any(x is None for x in required):
        return None

    mom1_3m = pct(close1[-1], close1[-4])
    mom5_15m = pct(close5[-1], close5[-4])
    mom15_1h = pct(close15[-1], close15[-5])
    mom1h_4h = pct(close1h[-1], close1h[-5])

    d_mom1 = directional(side, mom1_3m)
    d_mom5 = directional(side, mom5_15m)
    d_mom15 = directional(side, mom15_1h)
    d_mom1h = directional(side, mom1h_4h)

    vr1 = volume_ratio(vol1)
    vr5 = volume_ratio(vol5)
    vr15 = volume_ratio(vol15)

    previous_high = max(high5[-21:-1])
    previous_low = min(low5[-21:-1])
    breakout = p5 > previous_high if side == "LONG" else p5 < previous_low

    recent_high = max(high5[-7:])
    recent_low = min(low5[-7:])
    volatility_pct = (recent_high - recent_low) / p5 * 100.0 if p5 > 0 else 0.0

    ext5 = directional_extension(side, p5, ema20_5)
    ext1 = directional_extension(side, p1, ema13_1)

    align_1h = aligned(side, p1h, ema20_1h, ema50_1h)
    align_15 = aligned(side, p15, ema20_15, ema50_15)
    align_5 = aligned(side, p5, ema9_5, ema20_5)
    align_1 = aligned(side, p1, ema5_1, ema13_1)

    if align_1h and align_15 and align_5:
        regime = "TREND"
    elif breakout and vr5 >= 1.5 and d_mom5 >= 0.6:
        regime = "BREAKOUT"
    elif align_15 and align_5:
        regime = "TRANSITION"
    else:
        regime = "RANGE"

    # Direction score: "Is the larger structure genuinely pointed this way?"
    direction_score = 0.0
    direction_score += 25 if align_1h else 0
    direction_score += 20 if align_15 else 0
    direction_score += 15 if align_5 else 0

    if d_mom5 > 0.35:
        direction_score += 8
    if d_mom5 > 0.90:
        direction_score += 7

    if d_mom15 > 0.50:
        direction_score += 8
    if d_mom1h > 0.80:
        direction_score += 7

    if vr5 >= 1.2:
        direction_score += 5
    if breakout:
        direction_score += 5

    if side == "LONG":
        if 48 <= rsi5 <= 74:
            direction_score += 5
        if rsi5 >= 84:
            direction_score -= 15
    else:
        if 26 <= rsi5 <= 52:
            direction_score += 5
        if rsi5 <= 16:
            direction_score -= 15

    if ext5 > 3.5:
        direction_score -= 12
    if ext5 > 5.0:
        direction_score -= 10

    direction_score = clamp(direction_score)

    # Micro score: "Is the next few minutes still pushing with us?"
    micro_score = 0.0
    micro_score += 22 if align_1 else 0

    if d_mom1 > 0.03:
        micro_score += 12
    if d_mom1 > 0.12:
        micro_score += 10
    if d_mom1 > 0.30:
        micro_score += 6

    if vr1 >= 1.15:
        micro_score += 12
    if vr1 >= 1.8:
        micro_score += 8

    if side == "LONG":
        if 50 <= rsi1 <= 74:
            micro_score += 15
        if rsi1 >= 84:
            micro_score -= 18
    else:
        if 26 <= rsi1 <= 50:
            micro_score += 15
        if rsi1 <= 16:
            micro_score -= 18

    if ext1 <= 0.65:
        micro_score += 10
    elif ext1 > 1.20:
        micro_score -= 12

    if d_mom5 > 0:
        micro_score += 5

    micro_score = clamp(micro_score)

    # Fetch a new executable quote only after the analytical work is finished.
    q = fresh_ticker(symbol)
    bid, ask, last = q["bid"], q["ask"], q["last"]
    spread = q["spread_pct"]

    if side == "LONG":
        entry = ask if ask > 0 else last
        chase = pct(entry, radar["signal_price"])
    else:
        entry = bid if bid > 0 else last
        chase = pct(radar["signal_price"], entry)

    if entry <= 0:
        raise RuntimeError(f"No executable BBO for {symbol}")

    # Execution score: "Can the edge survive the price we can actually get?"
    if spread is None:
        execution_score = 0.0
    elif spread <= 0.10:
        execution_score = 100.0
    elif spread <= 0.20:
        execution_score = 90.0
    elif spread <= 0.35:
        execution_score = 75.0
    elif spread <= 0.50:
        execution_score = 55.0
    elif spread <= 0.60:
        execution_score = 35.0
    else:
        execution_score = 0.0

    # Negative chase is favorable (price came back toward us).
    adverse_chase = max(chase, 0.0)
    if adverse_chase <= 0.15:
        pass
    elif adverse_chase <= 0.35:
        execution_score -= 10
    elif adverse_chase <= 0.60:
        execution_score -= 25
    elif adverse_chase <= 0.75:
        execution_score -= 40
    else:
        execution_score -= 65
    execution_score = clamp(execution_score)

    regime_bonus = {
        "TREND": 6.0,
        "BREAKOUT": 6.0,
        "TRANSITION": 0.0,
        "RANGE": -12.0,
    }[regime]

    # This is a heuristic continuation score, NOT a calibrated probability.
    forward_score = clamp(
        0.45 * direction_score
        + 0.35 * micro_score
        + 0.20 * execution_score
        + regime_bonus
    )

    strategy = "BREAKOUT" if regime == "BREAKOUT" else "TREND"

    decision = "ENTER"
    reason = "R2 core passed"

    if spread is None or spread > R2_ENTER_MAX_SPREAD:
        decision = "REJECT_SPREAD"
        reason = f"spread {spread if spread is not None else 'NA'} > {R2_ENTER_MAX_SPREAD:.2f}%"
    elif regime == "RANGE":
        decision = "REJECT_RANGE"
        reason = "range regime"
    elif direction_score < R2_MIN_DIRECTION_SCORE:
        decision = "REJECT_DIRECTION"
        reason = f"direction {direction_score:.1f} < {R2_MIN_DIRECTION_SCORE:.1f}"
    elif micro_score < R2_MIN_MICRO_SCORE:
        decision = "REJECT_MICRO"
        reason = f"micro {micro_score:.1f} < {R2_MIN_MICRO_SCORE:.1f}"
    elif regime == "TRANSITION" and forward_score < R2_TRANSITION_MIN_FORWARD:
        decision = "REJECT_TRANSITION"
        reason = f"transition forward {forward_score:.1f} < {R2_TRANSITION_MIN_FORWARD:.1f}"
    elif adverse_chase > R2_ENTER_MAX_CHASE:
        # A strong breakout may be allowed to keep running, but only when
        # microstructure, spread and total score are exceptionally strong.
        breakout_exception = (
            regime == "BREAKOUT"
            and adverse_chase <= R2_BREAKOUT_MAX_CHASE
            and spread <= 0.20
            and micro_score >= 80
            and forward_score >= R2_BREAKOUT_CHASE_FORWARD
        )
        if not breakout_exception:
            decision = "REJECT_CHASE"
            reason = f"chase {adverse_chase:.2f}% too large"
    elif forward_score < R2_MIN_FORWARD_SCORE:
        decision = "REJECT_FORWARD"
        reason = f"forward {forward_score:.1f} < {R2_MIN_FORWARD_SCORE:.1f}"

    features = {
        "rsi_1m": rsi1,
        "rsi_5m": rsi5,
        "rsi_15m": rsi15,
        "volume_ratio_1m": vr1,
        "volume_ratio_5m": vr5,
        "volume_ratio_15m": vr15,
        "momentum_1m_3m": mom1_3m,
        "momentum_5m_15m": mom5_15m,
        "momentum_15m_1h": mom15_1h,
        "momentum_1h_4h": mom1h_4h,
        "directional_momentum_1m_3m": d_mom1,
        "directional_momentum_5m_15m": d_mom5,
        "directional_momentum_15m_1h": d_mom15,
        "directional_momentum_1h_4h": d_mom1h,
        "extension_1m": ext1,
        "extension_5m": ext5,
        "volatility_pct": volatility_pct,
        "breakout": breakout,
        "align_1m": align_1,
        "align_5m": align_5,
        "align_15m": align_15,
        "align_1h": align_1h,
    }

    return {
        "symbol": symbol,
        "side": side,
        "regime": regime,
        "strategy": strategy,
        "radar_move_pct": radar["radar_move_pct"],
        "signal_price": radar["signal_price"],
        "last_price": last,
        "bid_price": bid,
        "ask_price": ask,
        "entry_price": entry,
        "spread_pct": spread,
        "chase_pct": chase,
        "direction_score": direction_score,
        "micro_score": micro_score,
        "execution_score": execution_score,
        "forward_score": forward_score,
        "decision": decision,
        "reason": reason,
        "features": features,
        # fields required by scanner_trades
        "rsi": rsi15,
        "rsi_5m": rsi5,
        "volume_ratio": vr5,
        "momentum_pct": mom15_1h,
        "momentum_5m": mom5_15m,
        "volatility_pct": volatility_pct,
        "extension_pct": ext5,
        "trend_15m": trend_label(p15, ema20_15, ema50_15),
        "trend_1h": trend_label(p1h, ema20_1h, ema50_1h),
        "breakout": breakout,
    }


# ---------------------------------------------------------------------------
# R2 trade / decision persistence
# ---------------------------------------------------------------------------

def r2_open_count():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM scanner_trades
            WHERE engine_version=%s
              AND status='OPEN'
              AND strategy IN ('TREND','BREAKOUT');
            """,
            (ENGINE,),
        )
        return int(cur.fetchone()[0])
    finally:
        cur.close()
        conn.close()


def r2_can_open(symbol):
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1
            FROM scanner_trades
            WHERE engine_version=%s
              AND symbol=%s
              AND strategy IN ('TREND','BREAKOUT')
              AND (
                  status='OPEN'
                  OR signal_time > NOW() - (%s * INTERVAL '1 hour')
              )
            LIMIT 1;
            """,
            (ENGINE, symbol, R2_COOLDOWN_HOURS),
        )
        return cur.fetchone() is None
    finally:
        cur.close()
        conn.close()


def r2_risk_allows_new_trade():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COALESCE(SUM(sim_pnl_usdt),0)
            FROM scanner_trades
            WHERE engine_version=%s
              AND status IN ('WIN','LOSS')
              AND closed_at >= DATE_TRUNC('day', NOW());
            """,
            (ENGINE,),
        )
        daily = float(cur.fetchone()[0] or 0)

        if daily <= -abs(R2_DAILY_MAX_LOSS_USDT):
            return False, f"DAILY_LOSS_LIMIT {daily:.2f}U"

        cur.execute(
            """
            SELECT status, closed_at
            FROM scanner_trades
            WHERE engine_version=%s
              AND status IN ('WIN','LOSS')
            ORDER BY closed_at DESC, id DESC
            LIMIT %s;
            """,
            (ENGINE, max(R2_MAX_CONSECUTIVE_LOSSES, 1)),
        )
        rows = cur.fetchall()

        streak = 0
        latest_loss = None
        for status, closed_at in rows:
            if status == "LOSS":
                streak += 1
                if latest_loss is None:
                    latest_loss = closed_at
            else:
                break

        if streak >= R2_MAX_CONSECUTIVE_LOSSES and latest_loss is not None:
            elapsed = (datetime.now(timezone.utc) - latest_loss).total_seconds()
            if elapsed < R2_PAUSE_HOURS_AFTER_STREAK * 3600:
                return False, f"LOSS_STREAK_PAUSE {streak}"

        return True, "OK"
    finally:
        cur.close()
        conn.close()


def create_r2_trade(x):
    entry = float(x["entry_price"])
    side = x["side"]

    if side == "LONG":
        tp1 = entry * 1.02
        tp2 = entry * 1.04
        tp3 = entry * 1.08
        sl = entry * 0.975
    else:
        tp1 = entry * 0.98
        tp2 = entry * 0.96
        tp3 = entry * 0.92
        sl = entry * 1.025

    reasons = [
        f"R2 {x['regime']}",
        f"direction={x['direction_score']:.1f}",
        f"micro={x['micro_score']:.1f}",
        f"execution={x['execution_score']:.1f}",
        f"forward={x['forward_score']:.1f}",
        f"spread={x['spread_pct']:.3f}%",
        f"chase={x['chase_pct']:.3f}%",
    ]

    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO scanner_trades (
                symbol, side, strategy, engine_version, entry_price,
                score, trend_score, breakout_score,
                rsi, rsi_5m, volume_ratio, momentum_pct, momentum_5m,
                volatility_pct, extension_pct,
                trend_15m, trend_1h, breakout, reasons,
                tp1, tp2, tp3, sl,
                highest_price, lowest_price,
                sim_margin_usdt, sim_leverage,
                last_checked_at
            )
            VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,
                %s,%s,
                NOW()
            )
            RETURNING id;
            """,
            (
                x["symbol"], side, x["strategy"], ENGINE, entry,
                int(round(x["forward_score"])),
                int(round(x["direction_score"])),
                int(round(x["micro_score"])),
                x["rsi"], x["rsi_5m"], x["volume_ratio"],
                x["momentum_pct"], x["momentum_5m"],
                x["volatility_pct"], x["extension_pct"],
                x["trend_15m"], x["trend_1h"], x["breakout"],
                json.dumps(reasons, ensure_ascii=False),
                tp1, tp2, tp3, sl,
                entry, entry,
                10.0, 8,
            ),
        )
        trade_id = int(cur.fetchone()[0])
        conn.commit()
        return trade_id
    finally:
        cur.close()
        conn.close()


def log_decision(x, scanner_trade_id=None, override_decision=None, override_reason=None):
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO r2_decisions (
                symbol, side, regime, strategy, radar_move_pct,
                direction_score, micro_score, execution_score, forward_score,
                signal_price, last_price, bid_price, ask_price,
                market_entry_price, spread_pct, chase_pct,
                decision, reason, scanner_trade_id, features
            )
            VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s
            );
            """,
            (
                x["symbol"], x["side"], x["regime"], x["strategy"], x["radar_move_pct"],
                x["direction_score"], x["micro_score"], x["execution_score"], x["forward_score"],
                x["signal_price"], x["last_price"], x["bid_price"], x["ask_price"],
                x["entry_price"], x["spread_pct"], x["chase_pct"],
                override_decision or x["decision"],
                override_reason or x["reason"],
                scanner_trade_id,
                json.dumps(x["features"]),
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def r2_update_trades():
    prices = base.get_all_prices()
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                id, symbol, side, engine_version, signal_time, entry_price,
                tp1, tp2, tp3, sl,
                tp1_hit, tp2_hit, tp3_hit,
                highest_price, lowest_price,
                last_checked_at,
                sim_margin_usdt, sim_leverage
            FROM scanner_trades
            WHERE engine_version=%s
              AND status='OPEN'
              AND strategy IN ('TREND','BREAKOUT')
            ORDER BY id ASC;
            """,
            (ENGINE,),
        )
        rows = cur.fetchall()

        for row in rows:
            trade_id, symbol = row[0], row[1]
            if symbol not in prices:
                continue
            try:
                base.update_v42_trade_replay(cur, row, prices[symbol])
                conn.commit()
            except Exception as exc:
                conn.rollback()
                print("R2 UPDATE ERROR:", trade_id, symbol, str(exc), flush=True)
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    started = time.time()
    print("=" * 60, flush=True)
    print("R2 CORE ACTIVE | PAPER ONLY | execution-aware", flush=True)
    print(
        f"max_open={R2_MAX_OPEN} max_new={R2_MAX_NEW_PER_SCAN} "
        f"spread<={R2_ENTER_MAX_SPREAD:.2f}% chase<={R2_ENTER_MAX_CHASE:.2f}%",
        flush=True,
    )
    print("=" * 60, flush=True)

    init_r2_database()

    try:
        r2_update_trades()
    except Exception as exc:
        print("R2 TRADE UPDATE ERROR:", str(exc), flush=True)

    allow_new, risk_reason = r2_risk_allows_new_trade()
    print("R2 RISK:", allow_new, "|", risk_reason, flush=True)

    previous = load_snapshots()
    current = all_tickers()
    print("R2 MARKETS:", len(current), flush=True)

    if not previous:
        save_snapshots(current)
        print("R2 FIRST RUN: dedicated radar baseline created", flush=True)
        return

    candidates = build_candidates(current, previous)
    save_snapshots(current)

    print("R2 RADAR CANDIDATES:", len(candidates), flush=True)
    for c in candidates:
        print(
            f"R2 RADAR {c['symbol']} {c['side']} "
            f"move={c['radar_move_pct']:+.2f}% spread={c['spread_pct']:.3f}% "
            f"rank={c['rank']:.1f}",
            flush=True,
        )

    deep = candidates[:R2_MAX_DEEP_ANALYSIS]
    created = 0

    for radar in deep:
        try:
            x = analyze_candidate(radar)
            if not x:
                continue

            print(
                f"R2 DECISION {x['symbol']} {x['side']} {x['regime']} | "
                f"D={x['direction_score']:.1f} M={x['micro_score']:.1f} "
                f"E={x['execution_score']:.1f} F={x['forward_score']:.1f} | "
                f"spread={x['spread_pct']:.3f}% chase={x['chase_pct']:+.3f}% | "
                f"{x['decision']}",
                flush=True,
            )

            if x["decision"] != "ENTER":
                log_decision(x)
                continue

            if not allow_new:
                log_decision(x, override_decision="RISK_BLOCKED", override_reason=risk_reason)
                continue

            if created >= R2_MAX_NEW_PER_SCAN:
                log_decision(x, override_decision="NO_SLOT", override_reason="max new per scan")
                continue

            if r2_open_count() >= R2_MAX_OPEN:
                log_decision(x, override_decision="NO_SLOT", override_reason="max open reached")
                continue

            if not r2_can_open(x["symbol"]):
                log_decision(x, override_decision="COOLDOWN", override_reason="symbol cooldown/open")
                continue

            # Re-check the risk gate after every new trade.
            allow_new_now, risk_reason_now = r2_risk_allows_new_trade()
            if not allow_new_now:
                log_decision(x, override_decision="RISK_BLOCKED", override_reason=risk_reason_now)
                allow_new = False
                continue

            trade_id = create_r2_trade(x)
            log_decision(x, scanner_trade_id=trade_id)
            created += 1

            print(
                f"R2 PAPER ENTRY #{trade_id} {x['symbol']} {x['side']} "
                f"entry={x['entry_price']} F={x['forward_score']:.1f}",
                flush=True,
            )

        except Exception as exc:
            print("R2 ANALYSIS ERROR:", radar["symbol"], str(exc), flush=True)

    try:
        r2_update_trades()
    except Exception as exc:
        print("R2 POST UPDATE ERROR:", str(exc), flush=True)

    print(
        f"R2 RUN COMPLETE | created={created} open={r2_open_count()} "
        f"elapsed={time.time()-started:.2f}s",
        flush=True,
    )


if __name__ == "__main__":
    run()
