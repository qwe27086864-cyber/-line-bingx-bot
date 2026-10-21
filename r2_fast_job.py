"""
R2-Fast paper core.

Purpose
-------
A/B test against R2 without modifying R2 itself.

FAST path:
- Detect the same kind of whole-market 5m radar move.
- Evaluate only 1m + 5m structure plus BTC/ETH market alignment.
- Fetch a fresh executable BBO after the quick analysis.
- Enter immediately only when spread, chase, direction and micro momentum are all strong.
- Otherwise fall back to the full R2 analyzer.

Important:
- PAPER ONLY. No BingX private order endpoint is used.
- Engine version is R2F, stored separately from R2 and R1M.
- Exit model intentionally remains the same conservative 1m replay:
  TP 2/4/8%, SL 2.5%, TP1->BE, TP2->TP1 protection, 4h timeout.
"""

import json
import os
import time
from datetime import datetime, timezone

import app as base
import r2_job as slow

ENGINE = "R2F"

# ------------------------- configuration -------------------------

R2F_MAX_OPEN = int(os.environ.get("R2F_MAX_OPEN", "3"))
R2F_MAX_NEW_PER_SCAN = int(os.environ.get("R2F_MAX_NEW_PER_SCAN", "2"))
R2F_COOLDOWN_HOURS = float(os.environ.get("R2F_COOLDOWN_HOURS", "2"))

R2F_RADAR_MIN_MOVE = float(os.environ.get("R2F_RADAR_MIN_MOVE", "0.40"))
R2F_RADAR_MAX_CANDIDATES = int(os.environ.get("R2F_RADAR_MAX_CANDIDATES", "8"))
R2F_MAX_FAST_ANALYSIS = int(os.environ.get("R2F_MAX_FAST_ANALYSIS", "5"))
R2F_MIN_QUOTE_VOLUME = float(os.environ.get("R2F_MIN_QUOTE_VOLUME", "250000"))
R2F_SCAN_MAX_SPREAD = float(os.environ.get("R2F_SCAN_MAX_SPREAD", "0.80"))

# Strict fast-path execution rules.
R2F_FAST_MAX_SPREAD = float(os.environ.get("R2F_FAST_MAX_SPREAD", "0.25"))
R2F_FAST_MAX_CHASE = float(os.environ.get("R2F_FAST_MAX_CHASE", "0.35"))
R2F_FAST_MIN_DIRECTION = float(os.environ.get("R2F_FAST_MIN_DIRECTION", "72"))
R2F_FAST_MIN_MICRO = float(os.environ.get("R2F_FAST_MIN_MICRO", "72"))
R2F_FAST_MIN_MARKET = float(os.environ.get("R2F_FAST_MIN_MARKET", "45"))
R2F_FAST_MIN_SCORE = float(os.environ.get("R2F_FAST_MIN_SCORE", "78"))

R2F_DAILY_MAX_LOSS_USDT = float(os.environ.get("R2F_DAILY_MAX_LOSS_USDT", "20"))
R2F_MAX_CONSECUTIVE_LOSSES = int(os.environ.get("R2F_MAX_CONSECUTIVE_LOSSES", "4"))
R2F_PAUSE_HOURS_AFTER_STREAK = float(os.environ.get("R2F_PAUSE_HOURS_AFTER_STREAK", "6"))

# Same economic assumptions as R1M/R2 for a clean A/B test.
base.SCANNER_TP1 = 2.0
base.SCANNER_TP2 = 4.0
base.SCANNER_TP3 = 8.0
base.SCANNER_SL = 2.5
base.V42_POSITION_TIMEOUT_HOURS = 4.0
base.SIM_MARGIN_USDT = 10.0
base.SIM_LEVERAGE = 8

# Hard safety: simulation only.
base.LIVE_TRADING = False
if hasattr(base, "R1_LIVE_TRADING"):
    base.R1_LIVE_TRADING = False


def db():
    return base.get_db_connection()


def clamp(value, lo=0.0, hi=100.0):
    return max(lo, min(hi, float(value)))


def pct(a, b):
    if not b:
        return 0.0
    return (float(a) - float(b)) / float(b) * 100.0


def directional(side, value):
    return float(value) if side == "LONG" else -float(value)


def volume_ratio(volumes, lookback=20):
    if len(volumes) < lookback + 1:
        return 0.0
    prev = volumes[-(lookback + 1):-1]
    avg = sum(prev) / len(prev) if prev else 0.0
    return float(volumes[-1]) / avg if avg > 0 else 0.0


def aligned(side, price, fast_ema, slow_ema):
    if side == "LONG":
        return price > fast_ema > slow_ema
    return price < fast_ema < slow_ema


def directional_extension(side, price, ema):
    raw = pct(price, ema)
    return raw if side == "LONG" else -raw


# ------------------------- database -------------------------

def init_database():
    base.init_database()
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS r2_fast_radar_snapshots (
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
            CREATE TABLE IF NOT EXISTS r2_fast_decisions (
                id BIGSERIAL PRIMARY KEY,
                observed_at TIMESTAMPTZ DEFAULT NOW(),
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL,
                path VARCHAR(20) NOT NULL,
                radar_move_pct DOUBLE PRECISION,
                direction_score DOUBLE PRECISION,
                micro_score DOUBLE PRECISION,
                market_score DOUBLE PRECISION,
                execution_score DOUBLE PRECISION,
                fast_score DOUBLE PRECISION,
                signal_price DOUBLE PRECISION,
                pre_bid DOUBLE PRECISION,
                pre_ask DOUBLE PRECISION,
                post_bid DOUBLE PRECISION,
                post_ask DOUBLE PRECISION,
                market_entry_price DOUBLE PRECISION,
                spread_pct DOUBLE PRECISION,
                chase_pct DOUBLE PRECISION,
                analysis_latency_ms DOUBLE PRECISION,
                decision VARCHAR(50) NOT NULL,
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


# ------------------------- market snapshots -------------------------

def all_tickers():
    return slow.all_tickers()


def fresh_ticker(symbol):
    return slow.fresh_ticker(symbol)


def get_klines_retry(symbol, interval, limit, attempts=3):
    return slow.get_klines_retry(symbol, interval, limit, attempts)


def load_snapshots():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT symbol,last_price,captured_at
            FROM r2_fast_radar_snapshots;
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
                INSERT INTO r2_fast_radar_snapshots (
                    symbol,last_price,bid_price,ask_price,
                    quote_volume_24h,price_change_24h,captured_at
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
        if spread is None or spread > R2F_SCAN_MAX_SPREAD:
            continue
        if t["quote_volume"] < R2F_MIN_QUOTE_VOLUME:
            continue

        move = pct(t["last"], old["price"])
        if abs(move) < R2F_RADAR_MIN_MOVE:
            continue

        side = "LONG" if move > 0 else "SHORT"

        rank = min(abs(move), 6.0) * 12.0
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
            "signal_price": t["last"],
        })

    candidates.sort(
        key=lambda x: (x["rank"], abs(x["radar_move_pct"])),
        reverse=True,
    )
    return candidates[:R2F_RADAR_MAX_CANDIDATES]


def market_alignment_score(side, current_map, previous):
    score = 50.0
    details = {}

    for symbol, weight in (("BTC-USDT", 1.0), ("ETH-USDT", 1.0)):
        cur = current_map.get(symbol)
        old = previous.get(symbol)
        move5 = 0.0

        if cur and old and old.get("price", 0) > 0:
            move5 = pct(cur["last"], old["price"])

        change24 = float(cur["change_24h"]) if cur else 0.0
        d5 = directional(side, move5)
        d24 = directional(side, change24)

        if d5 >= 0.10:
            score += 12 * weight
        elif d5 >= 0.03:
            score += 6 * weight
        elif d5 <= -0.10:
            score -= 14 * weight
        elif d5 <= -0.03:
            score -= 7 * weight

        if d24 > 0:
            score += 4 * weight
        elif d24 < 0:
            score -= 4 * weight

        details[symbol] = {"move_5m_pct": move5, "change_24h_pct": change24}

    return clamp(score), details


# ------------------------- fast analyzer -------------------------

def execution_score(spread, adverse_chase):
    if spread is None:
        score = 0.0
    elif spread <= 0.08:
        score = 100.0
    elif spread <= 0.15:
        score = 94.0
    elif spread <= 0.25:
        score = 82.0
    elif spread <= 0.35:
        score = 68.0
    elif spread <= 0.50:
        score = 48.0
    elif spread <= 0.60:
        score = 30.0
    else:
        score = 0.0

    if adverse_chase <= 0.10:
        pass
    elif adverse_chase <= 0.20:
        score -= 5
    elif adverse_chase <= 0.35:
        score -= 12
    elif adverse_chase <= 0.50:
        score -= 28
    elif adverse_chase <= 0.75:
        score -= 48
    else:
        score -= 70

    return clamp(score)


def fast_analyze(radar, current_map, previous):
    started = time.perf_counter()
    symbol = radar["symbol"]
    side = radar["side"]

    # Snapshot BBO before analysis so latency can be measured explicitly.
    pre = fresh_ticker(symbol)

    c1 = get_klines_retry(symbol, "1m", 90)
    c5 = get_klines_retry(symbol, "5m", 100)

    # Completed candles only.
    c1 = c1[:-1]
    c5 = c5[:-1]
    if min(len(c1), len(c5)) < 55:
        return None

    close1 = [x["close"] for x in c1]
    close5 = [x["close"] for x in c5]
    high5 = [x["high"] for x in c5]
    low5 = [x["low"] for x in c5]
    vol1 = [x["volume"] for x in c1]
    vol5 = [x["volume"] for x in c5]

    p1 = close1[-1]
    p5 = close5[-1]

    ema5_1 = base.calculate_ema(close1, 5)
    ema13_1 = base.calculate_ema(close1, 13)
    ema9_5 = base.calculate_ema(close5, 9)
    ema20_5 = base.calculate_ema(close5, 20)
    rsi1 = base.calculate_rsi(close1, 14)
    rsi5 = base.calculate_rsi(close5, 14)

    if any(x is None for x in (ema5_1, ema13_1, ema9_5, ema20_5, rsi1, rsi5)):
        return None

    mom1_3m = pct(close1[-1], close1[-4])
    mom5_15m = pct(close5[-1], close5[-4])
    d_mom1 = directional(side, mom1_3m)
    d_mom5 = directional(side, mom5_15m)

    vr1 = volume_ratio(vol1)
    vr5 = volume_ratio(vol5)

    align1 = aligned(side, p1, ema5_1, ema13_1)
    align5 = aligned(side, p5, ema9_5, ema20_5)

    previous_high = max(high5[-21:-1])
    previous_low = min(low5[-21:-1])
    breakout = p5 > previous_high if side == "LONG" else p5 < previous_low

    ext1 = directional_extension(side, p1, ema13_1)
    ext5 = directional_extension(side, p5, ema20_5)

    direction_score = 0.0
    direction_score += 30 if align5 else 0
    if d_mom5 > 0.25:
        direction_score += 15
    if d_mom5 > 0.60:
        direction_score += 12
    if d_mom5 > 1.20:
        direction_score += 8
    if vr5 >= 1.2:
        direction_score += 10
    if vr5 >= 1.8:
        direction_score += 5
    if breakout:
        direction_score += 10

    if side == "LONG":
        if 48 <= rsi5 <= 76:
            direction_score += 10
        if rsi5 >= 84:
            direction_score -= 15
    else:
        if 24 <= rsi5 <= 52:
            direction_score += 10
        if rsi5 <= 16:
            direction_score -= 15

    if ext5 > 3.5:
        direction_score -= 12
    if ext5 > 5.0:
        direction_score -= 10
    direction_score = clamp(direction_score)

    micro_score = 0.0
    micro_score += 24 if align1 else 0
    if d_mom1 > 0.03:
        micro_score += 12
    if d_mom1 > 0.12:
        micro_score += 12
    if d_mom1 > 0.30:
        micro_score += 8
    if vr1 >= 1.15:
        micro_score += 14
    if vr1 >= 1.8:
        micro_score += 8

    if side == "LONG":
        if 50 <= rsi1 <= 74:
            micro_score += 15
        if rsi1 >= 84:
            micro_score -= 20
    else:
        if 26 <= rsi1 <= 50:
            micro_score += 15
        if rsi1 <= 16:
            micro_score -= 20

    if ext1 <= 0.55:
        micro_score += 12
    elif ext1 > 1.0:
        micro_score -= 14

    if d_mom5 > 0:
        micro_score += 7
    micro_score = clamp(micro_score)

    market_score, market_details = market_alignment_score(side, current_map, previous)

    # Executable price is measured only after the fast calculation finishes.
    post = fresh_ticker(symbol)
    bid = post["bid"]
    ask = post["ask"]
    last = post["last"]
    spread = post["spread_pct"]

    if side == "LONG":
        entry = ask if ask > 0 else last
        chase = pct(entry, radar["signal_price"])
    else:
        entry = bid if bid > 0 else last
        chase = pct(radar["signal_price"], entry)

    if entry <= 0:
        raise RuntimeError(f"No executable BBO for {symbol}")

    adverse_chase = max(chase, 0.0)
    exec_score = execution_score(spread, adverse_chase)

    fast_score = clamp(
        0.38 * direction_score
        + 0.34 * micro_score
        + 0.18 * exec_score
        + 0.10 * market_score
    )

    latency_ms = (time.perf_counter() - started) * 1000.0

    decision = "FAST_ENTER"
    reason = "fast path passed"

    if spread is None or spread > R2F_FAST_MAX_SPREAD:
        decision = "FAST_FALLBACK_SPREAD"
        reason = f"spread {spread if spread is not None else 'NA'} > {R2F_FAST_MAX_SPREAD:.2f}%"
    elif adverse_chase > R2F_FAST_MAX_CHASE:
        decision = "FAST_FALLBACK_CHASE"
        reason = f"chase {adverse_chase:.3f}% > {R2F_FAST_MAX_CHASE:.2f}%"
    elif direction_score < R2F_FAST_MIN_DIRECTION:
        decision = "FAST_FALLBACK_DIRECTION"
        reason = f"direction {direction_score:.1f} < {R2F_FAST_MIN_DIRECTION:.1f}"
    elif micro_score < R2F_FAST_MIN_MICRO:
        decision = "FAST_FALLBACK_MICRO"
        reason = f"micro {micro_score:.1f} < {R2F_FAST_MIN_MICRO:.1f}"
    elif market_score < R2F_FAST_MIN_MARKET:
        decision = "FAST_FALLBACK_MARKET"
        reason = f"market {market_score:.1f} < {R2F_FAST_MIN_MARKET:.1f}"
    elif fast_score < R2F_FAST_MIN_SCORE:
        decision = "FAST_FALLBACK_SCORE"
        reason = f"fast score {fast_score:.1f} < {R2F_FAST_MIN_SCORE:.1f}"
    elif ext1 > 1.10 or ext5 > 4.50:
        decision = "FAST_FALLBACK_EXTENSION"
        reason = f"extension too large 1m={ext1:.2f}% 5m={ext5:.2f}%"

    features = {
        "rsi_1m": rsi1,
        "rsi_5m": rsi5,
        "volume_ratio_1m": vr1,
        "volume_ratio_5m": vr5,
        "momentum_1m_3m": mom1_3m,
        "momentum_5m_15m": mom5_15m,
        "directional_momentum_1m_3m": d_mom1,
        "directional_momentum_5m_15m": d_mom5,
        "extension_1m": ext1,
        "extension_5m": ext5,
        "align_1m": align1,
        "align_5m": align5,
        "breakout_5m": breakout,
        "market": market_details,
    }

    return {
        "symbol": symbol,
        "side": side,
        "radar_move_pct": radar["radar_move_pct"],
        "signal_price": radar["signal_price"],
        "entry_price": entry,
        "spread_pct": spread,
        "chase_pct": chase,
        "direction_score": direction_score,
        "micro_score": micro_score,
        "market_score": market_score,
        "execution_score": exec_score,
        "fast_score": fast_score,
        "decision": decision,
        "reason": reason,
        "analysis_latency_ms": latency_ms,
        "pre_bid": pre["bid"],
        "pre_ask": pre["ask"],
        "post_bid": bid,
        "post_ask": ask,
        "features": features,
        # scanner_trades fields
        "strategy": "BREAKOUT" if breakout else "TREND",
        "score": int(round(fast_score)),
        "trend_score": int(round(direction_score)),
        "breakout_score": int(round(micro_score)),
        "rsi": rsi5,
        "rsi_5m": rsi5,
        "volume_ratio": vr5,
        "momentum_pct": mom5_15m,
        "momentum_5m": mom5_15m,
        "volatility_pct": (max(high5[-7:]) - min(low5[-7:])) / p5 * 100.0,
        "extension_pct": ext5,
        "trend_15m": "FAST_UNREAD",
        "trend_1h": "FAST_UNREAD",
        "breakout": breakout,
    }


# ------------------------- persistence / risk -------------------------

def open_count():
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


def can_open(symbol):
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
            (ENGINE, symbol, R2F_COOLDOWN_HOURS),
        )
        return cur.fetchone() is None
    finally:
        cur.close()
        conn.close()


def risk_allows_new_trade():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT COALESCE(SUM(sim_pnl_usdt),0)
            FROM scanner_trades
            WHERE engine_version=%s
              AND status IN ('WIN','LOSS')
              AND closed_at >= DATE_TRUNC('day',NOW());
            """,
            (ENGINE,),
        )
        daily = float(cur.fetchone()[0] or 0)
        if daily <= -abs(R2F_DAILY_MAX_LOSS_USDT):
            return False, f"DAILY_LOSS_LIMIT {daily:.2f}U"

        cur.execute(
            """
            SELECT status,closed_at
            FROM scanner_trades
            WHERE engine_version=%s
              AND status IN ('WIN','LOSS')
            ORDER BY closed_at DESC,id DESC
            LIMIT %s;
            """,
            (ENGINE, max(R2F_MAX_CONSECUTIVE_LOSSES, 1)),
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

        if streak >= R2F_MAX_CONSECUTIVE_LOSSES and latest_loss is not None:
            elapsed = (datetime.now(timezone.utc) - latest_loss).total_seconds()
            if elapsed < R2F_PAUSE_HOURS_AFTER_STREAK * 3600:
                return False, f"LOSS_STREAK_PAUSE {streak}"

        return True, "OK"
    finally:
        cur.close()
        conn.close()


def create_trade(x, path):
    entry = float(x["entry_price"])
    side = x["side"]

    if side == "LONG":
        tp1, tp2, tp3, sl = entry * 1.02, entry * 1.04, entry * 1.08, entry * 0.975
    else:
        tp1, tp2, tp3, sl = entry * 0.98, entry * 0.96, entry * 0.92, entry * 1.025

    reasons = [
        f"R2F {path}",
        f"direction={x.get('direction_score',0):.1f}",
        f"micro={x.get('micro_score',0):.1f}",
        f"execution={x.get('execution_score',0):.1f}",
        f"score={x.get('fast_score',x.get('forward_score',0)):.1f}",
        f"spread={x.get('spread_pct',0):.3f}%",
        f"chase={x.get('chase_pct',0):.3f}%",
    ]

    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO scanner_trades (
                symbol,side,strategy,engine_version,entry_price,
                score,trend_score,breakout_score,
                rsi,rsi_5m,volume_ratio,momentum_pct,momentum_5m,
                volatility_pct,extension_pct,
                trend_15m,trend_1h,breakout,reasons,
                tp1,tp2,tp3,sl,
                highest_price,lowest_price,
                sim_margin_usdt,sim_leverage,last_checked_at
            )
            VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,
                %s,%s,NOW()
            )
            RETURNING id;
            """,
            (
                x["symbol"], side, x.get("strategy","TREND"), ENGINE, entry,
                x.get("score", int(round(x.get("fast_score", x.get("forward_score", 0))))),
                int(round(x.get("direction_score",0))),
                int(round(x.get("micro_score",0))),
                x.get("rsi"), x.get("rsi_5m"), x.get("volume_ratio"),
                x.get("momentum_pct"), x.get("momentum_5m"),
                x.get("volatility_pct"), x.get("extension_pct"),
                x.get("trend_15m","UNKNOWN"), x.get("trend_1h","UNKNOWN"),
                bool(x.get("breakout",False)),
                json.dumps(reasons, ensure_ascii=False),
                tp1,tp2,tp3,sl,
                entry,entry,
                10.0,8,
            ),
        )
        trade_id = int(cur.fetchone()[0])
        conn.commit()
        return trade_id
    finally:
        cur.close()
        conn.close()


def log_decision(x, path, decision, reason, scanner_trade_id=None):
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO r2_fast_decisions (
                symbol,side,path,radar_move_pct,
                direction_score,micro_score,market_score,execution_score,fast_score,
                signal_price,pre_bid,pre_ask,post_bid,post_ask,
                market_entry_price,spread_pct,chase_pct,analysis_latency_ms,
                decision,reason,scanner_trade_id,features
            )
            VALUES (
                %s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,%s
            );
            """,
            (
                x["symbol"], x["side"], path, x.get("radar_move_pct"),
                x.get("direction_score"), x.get("micro_score"), x.get("market_score"),
                x.get("execution_score"), x.get("fast_score", x.get("forward_score")),
                x.get("signal_price"), x.get("pre_bid"), x.get("pre_ask"),
                x.get("post_bid"), x.get("post_ask"),
                x.get("entry_price"), x.get("spread_pct"), x.get("chase_pct"),
                x.get("analysis_latency_ms"),
                decision, reason, scanner_trade_id,
                json.dumps(x.get("features",{})),
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def update_trades():
    prices = base.get_all_prices()
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                id,symbol,side,engine_version,signal_time,entry_price,
                tp1,tp2,tp3,sl,
                tp1_hit,tp2_hit,tp3_hit,
                highest_price,lowest_price,last_checked_at,
                sim_margin_usdt,sim_leverage
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
                print("R2F UPDATE ERROR:", trade_id, symbol, str(exc), flush=True)
    finally:
        cur.close()
        conn.close()


def convert_slow_result(slow_result, radar):
    """
    Convert the existing full-R2 analyzer output into the R2F persistence shape.
    This does not modify R2; it only reuses its analysis function as fallback.
    """
    x = dict(slow_result)
    x["radar_move_pct"] = radar["radar_move_pct"]
    x["signal_price"] = radar["signal_price"]
    x["fast_score"] = x.get("forward_score")
    x["market_score"] = None
    x["analysis_latency_ms"] = None
    x["pre_bid"] = None
    x["pre_ask"] = None
    x["post_bid"] = x.get("bid_price")
    x["post_ask"] = x.get("ask_price")
    return x


# ------------------------- main -------------------------

def run():
    started = time.time()
    print("=" * 68, flush=True)
    print("R2-FAST ACTIVE | PAPER ONLY | FAST PATH + R2 FALLBACK", flush=True)
    print(
        f"fast spread<={R2F_FAST_MAX_SPREAD:.2f}% "
        f"fast chase<={R2F_FAST_MAX_CHASE:.2f}% "
        f"max_open={R2F_MAX_OPEN}",
        flush=True,
    )
    print("=" * 68, flush=True)

    init_database()

    try:
        update_trades()
    except Exception as exc:
        print("R2F TRADE UPDATE ERROR:", str(exc), flush=True)

    allow_new, risk_reason = risk_allows_new_trade()
    print("R2F RISK:", allow_new, "|", risk_reason, flush=True)

    previous = load_snapshots()
    current = all_tickers()
    current_map = {x["symbol"]: x for x in current}
    print("R2F MARKETS:", len(current), flush=True)

    if not previous:
        save_snapshots(current)
        print("R2F FIRST RUN: dedicated radar baseline created", flush=True)
        return

    candidates = build_candidates(current, previous)
    save_snapshots(current)

    print("R2F RADAR CANDIDATES:", len(candidates), flush=True)
    created = 0

    for radar in candidates[:R2F_MAX_FAST_ANALYSIS]:
        try:
            fast = fast_analyze(radar, current_map, previous)
            if not fast:
                continue

            print(
                f"R2F FAST {fast['symbol']} {fast['side']} | "
                f"D={fast['direction_score']:.1f} M={fast['micro_score']:.1f} "
                f"MKT={fast['market_score']:.1f} E={fast['execution_score']:.1f} "
                f"F={fast['fast_score']:.1f} | "
                f"spread={fast['spread_pct']:.3f}% chase={fast['chase_pct']:+.3f}% "
                f"latency={fast['analysis_latency_ms']:.0f}ms | {fast['decision']}",
                flush=True,
            )

            if fast["decision"] == "FAST_ENTER":
                if not allow_new:
                    log_decision(fast, "FAST", "RISK_BLOCKED", risk_reason)
                    continue
                if created >= R2F_MAX_NEW_PER_SCAN or open_count() >= R2F_MAX_OPEN:
                    log_decision(fast, "FAST", "NO_SLOT", "R2F position limit")
                    continue
                if not can_open(fast["symbol"]):
                    log_decision(fast, "FAST", "COOLDOWN", "symbol cooldown/open")
                    continue

                allow_now, reason_now = risk_allows_new_trade()
                if not allow_now:
                    allow_new = False
                    log_decision(fast, "FAST", "RISK_BLOCKED", reason_now)
                    continue

                trade_id = create_trade(fast, "FAST")
                log_decision(fast, "FAST", "ENTER", "fast path passed", trade_id)
                created += 1
                print(
                    f"R2F FAST PAPER ENTRY #{trade_id} {fast['symbol']} "
                    f"{fast['side']} entry={fast['entry_price']}",
                    flush=True,
                )
                continue

            # Fast conditions were not strict enough -> full R2 fallback.
            fast_reason = fast["reason"]
            try:
                slow_result = slow.analyze_candidate(radar)
            except Exception as exc:
                log_decision(
                    fast, "FAST", "FALLBACK_ERROR",
                    f"{fast_reason}; full R2 error: {exc}"
                )
                continue

            if not slow_result:
                log_decision(fast, "FAST", "FALLBACK_NO_RESULT", fast_reason)
                continue

            full = convert_slow_result(slow_result, radar)
            final_decision = slow_result.get("decision", "REJECT")

            print(
                f"R2F FULL FALLBACK {full['symbol']} {full['side']} | "
                f"D={full.get('direction_score',0):.1f} "
                f"M={full.get('micro_score',0):.1f} "
                f"E={full.get('execution_score',0):.1f} "
                f"F={full.get('forward_score',0):.1f} | {final_decision}",
                flush=True,
            )

            if final_decision != "ENTER":
                log_decision(
                    full, "FULL",
                    f"FULL_{final_decision}",
                    f"fast fallback: {fast_reason}; {slow_result.get('reason','')}",
                )
                continue

            if not allow_new:
                log_decision(full, "FULL", "RISK_BLOCKED", risk_reason)
                continue
            if created >= R2F_MAX_NEW_PER_SCAN or open_count() >= R2F_MAX_OPEN:
                log_decision(full, "FULL", "NO_SLOT", "R2F position limit")
                continue
            if not can_open(full["symbol"]):
                log_decision(full, "FULL", "COOLDOWN", "symbol cooldown/open")
                continue

            allow_now, reason_now = risk_allows_new_trade()
            if not allow_now:
                allow_new = False
                log_decision(full, "FULL", "RISK_BLOCKED", reason_now)
                continue

            trade_id = create_trade(full, "FULL")
            log_decision(
                full, "FULL", "ENTER",
                f"fast fallback: {fast_reason}; full R2 passed",
                trade_id,
            )
            created += 1
            print(
                f"R2F FULL PAPER ENTRY #{trade_id} {full['symbol']} "
                f"{full['side']} entry={full['entry_price']}",
                flush=True,
            )

        except Exception as exc:
            print("R2F ANALYSIS ERROR:", radar["symbol"], str(exc), flush=True)

    try:
        update_trades()
    except Exception as exc:
        print("R2F POST UPDATE ERROR:", str(exc), flush=True)

    print(
        f"R2F RUN COMPLETE | created={created} open={open_count()} "
        f"elapsed={time.time()-started:.2f}s",
        flush=True,
    )


if __name__ == "__main__":
    run()
