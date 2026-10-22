"""
R3 pullback/retest paper-trading research core.

Purpose
-------
R3 is deliberately NOT another "radar move -> score -> enter" engine.
It separates discovery from entry:

    MARKET REGIME -> BREAKOUT SETUP -> WATCHLIST -> PULLBACK ->
    RE-ACCELERATION TRIGGER -> EXECUTION CHECK -> PAPER ENTRY

Research rules
--------------
- PAPER ONLY. No private BingX order endpoints are used.
- LONG and SHORT use mirrored structure checks but are evaluated separately.
- A radar event can only create a watchlist setup; it can never enter directly.
- Entry requires a later pullback/retest and a later trigger.
- Phase 1 keeps exits identical to R2/R2F (TP 2/4/8%, SL 2.5%, 4h timeout)
  so the experiment isolates ENTRY quality. Structural exits can be tested later.
- R3 has its own radar snapshots, watchlist, decision log and engine_version.
- Rejections / invalidations are copied to the shared shadow_rejections table.
"""

import json
import os
import time
from datetime import datetime, timezone

import app as base
import r2_job as market
import shadow_tracker as shadow

ENGINE = "R3"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

R3_MAX_OPEN = int(os.environ.get("R3_MAX_OPEN", "5"))
R3_MAX_NEW_PER_SCAN = int(os.environ.get("R3_MAX_NEW_PER_SCAN", "2"))
R3_COOLDOWN_HOURS = float(os.environ.get("R3_COOLDOWN_HOURS", "2"))

R3_RADAR_MIN_MOVE = float(os.environ.get("R3_RADAR_MIN_MOVE", "0.65"))
R3_RADAR_MAX_CANDIDATES = int(os.environ.get("R3_RADAR_MAX_CANDIDATES", "10"))
R3_MAX_DEEP_ANALYSIS = int(os.environ.get("R3_MAX_DEEP_ANALYSIS", "6"))
R3_MIN_QUOTE_VOLUME = float(os.environ.get("R3_MIN_QUOTE_VOLUME", "500000"))
R3_SCAN_MAX_SPREAD = float(os.environ.get("R3_SCAN_MAX_SPREAD", "0.50"))

# Setup gates: breakout + higher-timeframe structure + volume + market regime.
R3_MIN_MARKET_ALIGNMENT = int(os.environ.get("R3_MIN_MARKET_ALIGNMENT", "2"))
R3_MIN_5M_VOLUME_RATIO = float(os.environ.get("R3_MIN_5M_VOLUME_RATIO", "1.15"))
R3_MIN_5M_DIRECTIONAL_MOM = float(os.environ.get("R3_MIN_5M_DIRECTIONAL_MOM", "0.35"))
R3_MAX_5M_EXTENSION = float(os.environ.get("R3_MAX_5M_EXTENSION", "4.00"))
R3_LONG_MAX_RSI5 = float(os.environ.get("R3_LONG_MAX_RSI5", "78"))
R3_SHORT_MIN_RSI5 = float(os.environ.get("R3_SHORT_MIN_RSI5", "22"))

# Pullback / retest state machine.
R3_PULLBACK_MIN_PCT = float(os.environ.get("R3_PULLBACK_MIN_PCT", "0.35"))
R3_PULLBACK_MAX_PCT = float(os.environ.get("R3_PULLBACK_MAX_PCT", "1.80"))
R3_REFERENCE_INVALIDATION_PCT = float(os.environ.get("R3_REFERENCE_INVALIDATION_PCT", "0.35"))
R3_SETUP_TTL_MIN = int(os.environ.get("R3_SETUP_TTL_MIN", "75"))

# Trigger / execution gates.
R3_TRIGGER_MIN_1M_MOM = float(os.environ.get("R3_TRIGGER_MIN_1M_MOM", "0.05"))
R3_TRIGGER_MIN_1M_VOLUME_RATIO = float(os.environ.get("R3_TRIGGER_MIN_1M_VOLUME_RATIO", "1.00"))
R3_ENTER_MAX_SPREAD = float(os.environ.get("R3_ENTER_MAX_SPREAD", "0.25"))
R3_ENTER_MAX_TRIGGER_CHASE = float(os.environ.get("R3_ENTER_MAX_TRIGGER_CHASE", "0.15"))

R3_DAILY_MAX_LOSS_USDT = float(os.environ.get("R3_DAILY_MAX_LOSS_USDT", "20"))
R3_MAX_CONSECUTIVE_LOSSES = int(os.environ.get("R3_MAX_CONSECUTIVE_LOSSES", "4"))
R3_PAUSE_HOURS_AFTER_STREAK = float(os.environ.get("R3_PAUSE_HOURS_AFTER_STREAK", "6"))

# Phase-1 exit model: intentionally identical to R2/R2F.
base.SCANNER_ENGINE_VERSION = ENGINE
base.SCANNER_MAX_OPEN = R3_MAX_OPEN
base.SCANNER_MAX_NEW_PER_SCAN = R3_MAX_NEW_PER_SCAN
base.SCANNER_COOLDOWN_HOURS = R3_COOLDOWN_HOURS
base.SCANNER_TP1 = 2.0
base.SCANNER_TP2 = 4.0
base.SCANNER_TP3 = 8.0
base.SCANNER_SL = 2.5
base.V42_POSITION_TIMEOUT_HOURS = 4.0
base.SIM_MARGIN_USDT = 10.0
base.SIM_LEVERAGE = 8
base.LIVE_TRADING = False
if hasattr(base, "R1_LIVE_TRADING"):
    base.R1_LIVE_TRADING = False


def db():
    return base.get_db_connection()


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
    return volumes[-1] / avg if avg > 0 else 0.0


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


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_r3_database():
    base.init_database()
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS r3_radar_snapshots (
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
            CREATE TABLE IF NOT EXISTS r3_watchlist (
                id BIGSERIAL PRIMARY KEY,
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL,
                stage VARCHAR(30) NOT NULL DEFAULT 'WATCH',
                status VARCHAR(30) NOT NULL DEFAULT 'ACTIVE',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                expires_at TIMESTAMPTZ NOT NULL,
                setup_price DOUBLE PRECISION NOT NULL,
                setup_extreme DOUBLE PRECISION NOT NULL,
                reference_level DOUBLE PRECISION NOT NULL,
                pullback_extreme DOUBLE PRECISION,
                trigger_level DOUBLE PRECISION,
                radar_move_pct DOUBLE PRECISION,
                regime VARCHAR(30),
                setup_strength DOUBLE PRECISION,
                market_alignment INTEGER,
                features JSONB,
                terminal_reason TEXT,
                scanner_trade_id BIGINT
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_r3_watch_active
            ON r3_watchlist(status, stage, expires_at);
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_r3_watch_symbol
            ON r3_watchlist(symbol, side, created_at DESC);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS r3_decisions (
                id BIGSERIAL PRIMARY KEY,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL,
                stage VARCHAR(30),
                decision VARCHAR(60) NOT NULL,
                reason TEXT,
                radar_move_pct DOUBLE PRECISION,
                regime VARCHAR(30),
                setup_strength DOUBLE PRECISION,
                market_alignment INTEGER,
                setup_price DOUBLE PRECISION,
                setup_extreme DOUBLE PRECISION,
                reference_level DOUBLE PRECISION,
                pullback_pct DOUBLE PRECISION,
                pullback_extreme DOUBLE PRECISION,
                trigger_level DOUBLE PRECISION,
                market_entry_price DOUBLE PRECISION,
                spread_pct DOUBLE PRECISION,
                chase_pct DOUBLE PRECISION,
                scanner_trade_id BIGINT,
                watchlist_id BIGINT,
                features JSONB
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_r3_decisions_time
            ON r3_decisions(observed_at DESC);
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    shadow.init_shadow_database()


# ---------------------------------------------------------------------------
# Market snapshot / radar
# ---------------------------------------------------------------------------

def all_tickers():
    return market.all_tickers()


def fresh_ticker(symbol):
    return market.fresh_ticker(symbol)


def get_klines_retry(symbol, interval, limit):
    return market.get_klines_retry(symbol, interval, limit)


def load_snapshots():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT symbol,last_price,captured_at
            FROM r3_radar_snapshots;
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
                INSERT INTO r3_radar_snapshots (
                    symbol,last_price,bid_price,ask_price,
                    quote_volume_24h,price_change_24h,captured_at
                ) VALUES (%s,%s,%s,%s,%s,%s,NOW())
                ON CONFLICT (symbol) DO UPDATE SET
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
    out = []
    for t in current:
        old = previous.get(t["symbol"])
        if not old or old["price"] <= 0 or not old["captured_at"]:
            continue
        age_min = (now - old["captured_at"]).total_seconds() / 60.0
        if age_min < 2 or age_min > 20:
            continue
        spread = t.get("spread_pct")
        if spread is None or spread > R3_SCAN_MAX_SPREAD:
            continue
        if t.get("quote_volume", 0) < R3_MIN_QUOTE_VOLUME:
            continue
        move = pct(t["last"], old["price"])
        if abs(move) < R3_RADAR_MIN_MOVE:
            continue
        side = "LONG" if move > 0 else "SHORT"
        rank = min(abs(move), 5.0) * 12.0
        if spread <= 0.10:
            rank += 12
        elif spread <= 0.20:
            rank += 8
        if t["quote_volume"] >= 2_000_000:
            rank += 8
        if directional(side, t.get("change_24h", 0)) > 0:
            rank += 5
        out.append({
            **t,
            "side": side,
            "radar_move_pct": move,
            "signal_price": t["last"],
            "rank": rank,
        })
    out.sort(key=lambda x: (x["rank"], abs(x["radar_move_pct"])), reverse=True)
    return out[:R3_RADAR_MAX_CANDIDATES]


# ---------------------------------------------------------------------------
# Global BTC / ETH regime gate
# ---------------------------------------------------------------------------

def market_context():
    details = {}
    bull = 0
    bear = 0
    for symbol in ("BTC-USDT", "ETH-USDT"):
        try:
            c15 = get_klines_retry(symbol, "15m", 80)[:-1]
            c1h = get_klines_retry(symbol, "1h", 80)[:-1]
            symbol_info = {}
            for label, candles in (("15m", c15), ("1h", c1h)):
                closes = [x["close"] for x in candles]
                if len(closes) < 55:
                    symbol_info[label] = "UNKNOWN"
                    continue
                e20 = base.calculate_ema(closes, 20)
                e50 = base.calculate_ema(closes, 50)
                p = closes[-1]
                if e20 is None or e50 is None:
                    state = "UNKNOWN"
                elif p > e20 > e50:
                    state = "UP"
                    bull += 1
                elif p < e20 < e50:
                    state = "DOWN"
                    bear += 1
                else:
                    state = "MIXED"
                symbol_info[label] = state
            details[symbol] = symbol_info
        except Exception as exc:
            details[symbol] = {"error": str(exc)}
    return {"bull": bull, "bear": bear, "details": details}


def market_alignment_for(side, ctx):
    return int(ctx["bull"] if side == "LONG" else ctx["bear"])


# ---------------------------------------------------------------------------
# Setup analysis: creates WATCH only, never a trade
# ---------------------------------------------------------------------------

def analyze_setup(radar, ctx):
    symbol = radar["symbol"]
    side = radar["side"]

    c5 = get_klines_retry(symbol, "5m", 100)[:-1]
    c15 = get_klines_retry(symbol, "15m", 100)[:-1]
    c1h = get_klines_retry(symbol, "1h", 90)[:-1]
    if min(len(c5), len(c15), len(c1h)) < 55:
        return None

    close5 = [x["close"] for x in c5]
    close15 = [x["close"] for x in c15]
    close1h = [x["close"] for x in c1h]
    high5 = [x["high"] for x in c5]
    low5 = [x["low"] for x in c5]
    vol5 = [x["volume"] for x in c5]

    p5, p15, p1h = close5[-1], close15[-1], close1h[-1]
    e9_5 = base.calculate_ema(close5, 9)
    e20_5 = base.calculate_ema(close5, 20)
    e20_15 = base.calculate_ema(close15, 20)
    e50_15 = base.calculate_ema(close15, 50)
    e20_1h = base.calculate_ema(close1h, 20)
    e50_1h = base.calculate_ema(close1h, 50)
    rsi5 = base.calculate_rsi(close5, 14)
    if any(x is None for x in (e9_5, e20_5, e20_15, e50_15, e20_1h, e50_1h, rsi5)):
        return None

    prev_high = max(high5[-21:-1])
    prev_low = min(low5[-21:-1])
    reference = prev_high if side == "LONG" else prev_low
    breakout = p5 > prev_high if side == "LONG" else p5 < prev_low

    align5 = aligned(side, p5, e9_5, e20_5)
    align15 = aligned(side, p15, e20_15, e50_15)
    align1h = aligned(side, p1h, e20_1h, e50_1h)

    mom5 = pct(close5[-1], close5[-4])
    d_mom5 = directional(side, mom5)
    vr5 = volume_ratio(vol5)
    ext5 = directional(side, pct(p5, e20_5))
    market_align = market_alignment_for(side, ctx)

    gates = {
        "breakout": breakout,
        "align_5m": align5,
        "align_15m": align15,
        "align_1h": align1h,
        "market_alignment": market_align,
        "volume_ratio_5m": vr5,
        "directional_momentum_5m": d_mom5,
        "extension_5m": ext5,
        "rsi_5m": rsi5,
    }

    # Hard-gate chain. setup_strength is research metadata only; it never
    # overrides a failed gate.
    passed = 0
    passed += int(breakout)
    passed += int(align5)
    passed += int(align15)
    passed += int(align1h)
    passed += int(market_align >= R3_MIN_MARKET_ALIGNMENT)
    passed += int(vr5 >= R3_MIN_5M_VOLUME_RATIO)
    passed += int(d_mom5 >= R3_MIN_5M_DIRECTIONAL_MOM)
    passed += int(ext5 <= R3_MAX_5M_EXTENSION)
    setup_strength = passed / 8.0 * 100.0

    decision = "WATCH"
    reason = "breakout setup passed; wait for pullback"

    if not breakout:
        decision, reason = "REJECT_NO_BREAKOUT", "5m close has not broken prior 20-bar structure"
    elif not align5 or not align15:
        decision, reason = "REJECT_STRUCTURE", "5m/15m structure not aligned"
    elif market_align < R3_MIN_MARKET_ALIGNMENT:
        decision, reason = "REJECT_MARKET", f"BTC/ETH alignment {market_align} < {R3_MIN_MARKET_ALIGNMENT}"
    elif vr5 < R3_MIN_5M_VOLUME_RATIO:
        decision, reason = "REJECT_VOLUME", f"5m volume ratio {vr5:.2f} < {R3_MIN_5M_VOLUME_RATIO:.2f}"
    elif d_mom5 < R3_MIN_5M_DIRECTIONAL_MOM:
        decision, reason = "REJECT_MOMENTUM", f"5m directional momentum {d_mom5:.2f}% too weak"
    elif ext5 > R3_MAX_5M_EXTENSION:
        decision, reason = "REJECT_EXTENSION", f"5m extension {ext5:.2f}% > {R3_MAX_5M_EXTENSION:.2f}%"
    elif side == "LONG" and rsi5 > R3_LONG_MAX_RSI5:
        decision, reason = "REJECT_RSI", f"LONG RSI5 {rsi5:.1f} > {R3_LONG_MAX_RSI5:.1f}"
    elif side == "SHORT" and rsi5 < R3_SHORT_MIN_RSI5:
        decision, reason = "REJECT_RSI", f"SHORT RSI5 {rsi5:.1f} < {R3_SHORT_MIN_RSI5:.1f}"

    q = fresh_ticker(symbol)
    last = q["last"] or radar["last"]
    entry_ref = q["ask"] if side == "LONG" else q["bid"]
    if not entry_ref or entry_ref <= 0:
        entry_ref = last

    return {
        "symbol": symbol,
        "side": side,
        "decision": decision,
        "reason": reason,
        "radar_move_pct": radar["radar_move_pct"],
        "setup_price": float(last),
        "entry_ref": float(entry_ref),
        "reference_level": float(reference),
        "regime": "BREAKOUT_PULLBACK",
        "setup_strength": setup_strength,
        "market_alignment": market_align,
        "spread_pct": q.get("spread_pct"),
        "rsi_5m": rsi5,
        "volume_ratio_5m": vr5,
        "momentum_5m": mom5,
        "directional_momentum_5m": d_mom5,
        "extension_5m": ext5,
        "trend_15m": trend_label(p15, e20_15, e50_15),
        "trend_1h": trend_label(p1h, e20_1h, e50_1h),
        "features": {
            **gates,
            "market_context": ctx,
            "prior_20_high_5m": prev_high,
            "prior_20_low_5m": prev_low,
        },
    }


# ---------------------------------------------------------------------------
# Persistence / shadow
# ---------------------------------------------------------------------------

def log_decision(x, decision, reason, stage=None, watchlist_id=None, scanner_trade_id=None,
                 pullback_pct=None, pullback_extreme=None, trigger_level=None,
                 market_entry_price=None, spread_pct=None, chase_pct=None):
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO r3_decisions (
                symbol,side,stage,decision,reason,
                radar_move_pct,regime,setup_strength,market_alignment,
                setup_price,setup_extreme,reference_level,
                pullback_pct,pullback_extreme,trigger_level,
                market_entry_price,spread_pct,chase_pct,
                scanner_trade_id,watchlist_id,features
            ) VALUES (
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,
                %s,%s,%s,
                %s,%s,%s
            ) RETURNING id;
            """,
            (
                x.get("symbol"), x.get("side"), stage, decision, reason,
                x.get("radar_move_pct"), x.get("regime"), x.get("setup_strength"), x.get("market_alignment"),
                x.get("setup_price"), x.get("setup_extreme", x.get("setup_price")), x.get("reference_level"),
                pullback_pct, pullback_extreme, trigger_level,
                market_entry_price, spread_pct, chase_pct,
                scanner_trade_id, watchlist_id,
                json.dumps(x.get("features", {}), ensure_ascii=False, default=str),
            ),
        )
        decision_id = int(cur.fetchone()[0])
        conn.commit()
        return decision_id
    finally:
        cur.close()
        conn.close()


def shadow_decision(decision_id, x, decision, reason, entry_price, extra=None):
    if not entry_price or float(entry_price) <= 0:
        return 0
    record = {
        "source_key": f"R3:r3_decisions:{decision_id}",
        "engine_version": ENGINE,
        "symbol": x["symbol"],
        "side": x["side"],
        "decision": decision,
        "reason": reason,
        "entry_price": float(entry_price),
        "signal_price": x.get("setup_price"),
        "observed_at": datetime.now(timezone.utc),
        "metadata": {
            "source_table": "r3_decisions",
            "source_id": decision_id,
            "stage": x.get("stage"),
            "radar_move_pct": x.get("radar_move_pct"),
            "regime": x.get("regime"),
            "setup_strength": x.get("setup_strength"),
            "market_alignment": x.get("market_alignment"),
            "reference_level": x.get("reference_level"),
            **(extra or {}),
        },
    }
    return shadow.record_shadow_batch([record])


def has_active_watch(symbol, side):
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1 FROM r3_watchlist
            WHERE symbol=%s AND side=%s AND status='ACTIVE'
            LIMIT 1;
            """,
            (symbol, side),
        )
        return cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


def add_watch(x):
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO r3_watchlist (
                symbol,side,stage,status,expires_at,
                setup_price,setup_extreme,reference_level,
                radar_move_pct,regime,setup_strength,market_alignment,features
            ) VALUES (
                %s,%s,'WATCH','ACTIVE',NOW() + (%s * INTERVAL '1 minute'),
                %s,%s,%s,%s,%s,%s,%s,%s
            ) RETURNING id;
            """,
            (
                x["symbol"], x["side"], R3_SETUP_TTL_MIN,
                x["setup_price"], x["setup_price"], x["reference_level"],
                x["radar_move_pct"], x["regime"], x["setup_strength"],
                x["market_alignment"], json.dumps(x["features"], default=str),
            ),
        )
        wid = int(cur.fetchone()[0])
        conn.commit()
        return wid
    finally:
        cur.close()
        conn.close()


def load_active_watches():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id,symbol,side,stage,created_at,updated_at,expires_at,
                   setup_price,setup_extreme,reference_level,
                   pullback_extreme,trigger_level,radar_move_pct,regime,
                   setup_strength,market_alignment,features
            FROM r3_watchlist
            WHERE status='ACTIVE'
            ORDER BY created_at ASC;
            """
        )
        rows = cur.fetchall()
    finally:
        cur.close()
        conn.close()

    keys = [
        "id","symbol","side","stage","created_at","updated_at","expires_at",
        "setup_price","setup_extreme","reference_level",
        "pullback_extreme","trigger_level","radar_move_pct","regime",
        "setup_strength","market_alignment","features",
    ]
    return [dict(zip(keys, row)) for row in rows]


def update_watch(wid, **fields):
    if not fields:
        return
    allowed = {
        "stage","status","setup_extreme","pullback_extreme","trigger_level",
        "terminal_reason","scanner_trade_id","features",
    }
    clean = {k: v for k, v in fields.items() if k in allowed}
    if not clean:
        return
    assignments = []
    params = []
    for k, v in clean.items():
        assignments.append(f"{k}=%s")
        if k == "features" and isinstance(v, (dict, list)):
            v = json.dumps(v, ensure_ascii=False, default=str)
        params.append(v)
    assignments.append("updated_at=NOW()")
    params.append(wid)

    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE r3_watchlist SET " + ",".join(assignments) + " WHERE id=%s;",
            params,
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Trade/risk helpers
# ---------------------------------------------------------------------------

def open_count():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT COUNT(*) FROM scanner_trades WHERE engine_version=%s AND status='OPEN';",
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
            SELECT 1 FROM scanner_trades
            WHERE engine_version=%s AND symbol=%s
              AND (status='OPEN' OR signal_time > NOW() - (%s * INTERVAL '1 hour'))
            LIMIT 1;
            """,
            (ENGINE, symbol, R3_COOLDOWN_HOURS),
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
            WHERE engine_version=%s AND status IN ('WIN','LOSS')
              AND closed_at >= DATE_TRUNC('day',NOW());
            """,
            (ENGINE,),
        )
        daily = float(cur.fetchone()[0] or 0)
        if daily <= -abs(R3_DAILY_MAX_LOSS_USDT):
            return False, f"DAILY_LOSS_LIMIT {daily:.2f}U"

        cur.execute(
            """
            SELECT status,closed_at FROM scanner_trades
            WHERE engine_version=%s AND status IN ('WIN','LOSS')
            ORDER BY closed_at DESC,id DESC LIMIT %s;
            """,
            (ENGINE, max(R3_MAX_CONSECUTIVE_LOSSES, 1)),
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
        if streak >= R3_MAX_CONSECUTIVE_LOSSES and latest_loss is not None:
            elapsed = (datetime.now(timezone.utc) - latest_loss).total_seconds()
            if elapsed < R3_PAUSE_HOURS_AFTER_STREAK * 3600:
                return False, f"LOSS_STREAK_PAUSE {streak}"
        return True, "OK"
    finally:
        cur.close()
        conn.close()


def create_trade(x, entry_price, trigger_meta):
    entry = float(entry_price)
    side = x["side"]
    if side == "LONG":
        tp1, tp2, tp3, sl = entry * 1.02, entry * 1.04, entry * 1.08, entry * 0.975
    else:
        tp1, tp2, tp3, sl = entry * 0.98, entry * 0.96, entry * 0.92, entry * 1.025

    reasons = [
        "R3 BREAKOUT_PULLBACK_RETRIGGER",
        f"setup_strength={x.get('setup_strength',0):.1f}",
        f"market_alignment={x.get('market_alignment',0)}",
        f"pullback={trigger_meta.get('pullback_pct',0):.3f}%",
        f"trigger_mom={trigger_meta.get('trigger_mom',0):.3f}%",
        f"trigger_vr={trigger_meta.get('trigger_vr',0):.2f}",
        f"spread={trigger_meta.get('spread_pct',0):.3f}%",
        f"trigger_chase={trigger_meta.get('chase_pct',0):.3f}%",
    ]

    features = x.get("features") or {}
    rsi5 = features.get("rsi_5m")
    vr5 = features.get("volume_ratio_5m")
    mom5 = features.get("directional_momentum_5m")
    ext5 = features.get("extension_5m")

    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO scanner_trades (
                symbol,side,strategy,engine_version,entry_price,
                score,trend_score,breakout_score,
                rsi,rsi_5m,volume_ratio,momentum_pct,momentum_5m,
                volatility_pct,extension_pct,trend_15m,trend_1h,breakout,reasons,
                tp1,tp2,tp3,sl,highest_price,lowest_price,
                sim_margin_usdt,sim_leverage,last_checked_at
            ) VALUES (
                %s,%s,'PULLBACK',%s,%s,
                %s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,TRUE,%s,
                %s,%s,%s,%s,%s,%s,
                10.0,8,NOW()
            ) RETURNING id;
            """,
            (
                x["symbol"], side, ENGINE, entry,
                int(round(x.get("setup_strength", 0))),
                int(round(x.get("setup_strength", 0))),
                int(round(trigger_meta.get("trigger_score", 0))),
                rsi5, rsi5, vr5, mom5, mom5,
                None, ext5,
                features.get("trend_15m", "UNKNOWN"),
                features.get("trend_1h", "UNKNOWN"),
                json.dumps(reasons, ensure_ascii=False),
                tp1, tp2, tp3, sl, entry, entry,
            ),
        )
        trade_id = int(cur.fetchone()[0])
        conn.commit()
        return trade_id
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
            SELECT id,symbol,side,engine_version,signal_time,entry_price,
                   tp1,tp2,tp3,sl,tp1_hit,tp2_hit,tp3_hit,
                   highest_price,lowest_price,last_checked_at,
                   sim_margin_usdt,sim_leverage
            FROM scanner_trades
            WHERE engine_version=%s AND status='OPEN'
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
                print("R3 UPDATE ERROR:", trade_id, symbol, str(exc), flush=True)
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------------------------
# Watchlist state machine
# ---------------------------------------------------------------------------

def _watch_as_signal(w):
    features = w.get("features") or {}
    if isinstance(features, str):
        try:
            features = json.loads(features)
        except Exception:
            features = {}
    return {
        "symbol": w["symbol"],
        "side": w["side"],
        "stage": w["stage"],
        "radar_move_pct": w.get("radar_move_pct"),
        "regime": w.get("regime"),
        "setup_strength": float(w.get("setup_strength") or 0),
        "market_alignment": int(w.get("market_alignment") or 0),
        "setup_price": float(w["setup_price"]),
        "setup_extreme": float(w["setup_extreme"]),
        "reference_level": float(w["reference_level"]),
        "features": features,
    }


def local_trigger(symbol, side):
    candles = get_klines_retry(symbol, "1m", 40)[:-1]
    if len(candles) < 25:
        return None
    closes = [x["close"] for x in candles]
    highs = [x["high"] for x in candles]
    lows = [x["low"] for x in candles]
    vols = [x["volume"] for x in candles]
    mom = directional(side, pct(closes[-1], closes[-4]))
    vr = volume_ratio(vols)
    # Trigger level is defined from completed candles only.
    level = max(highs[-4:-1]) if side == "LONG" else min(lows[-4:-1])
    return {"level": float(level), "mom": float(mom), "vr": float(vr)}


def process_watches(current_map, ctx):
    created = 0
    now = datetime.now(timezone.utc)
    watches = load_active_watches()

    for w in watches:
        x = _watch_as_signal(w)
        wid = w["id"]
        t = current_map.get(w["symbol"])
        if not t:
            continue
        price = float(t["last"])
        side = w["side"]
        reference = float(w["reference_level"])
        extreme = float(w["setup_extreme"])

        if side == "LONG":
            extreme = max(extreme, price)
            pullback_pct = (extreme - price) / extreme * 100.0
            reference_broken = price < reference * (1.0 - R3_REFERENCE_INVALIDATION_PCT / 100.0)
        else:
            extreme = min(extreme, price)
            pullback_pct = (price - extreme) / extreme * 100.0
            reference_broken = price > reference * (1.0 + R3_REFERENCE_INVALIDATION_PCT / 100.0)

        x["setup_extreme"] = extreme
        update_watch(wid, setup_extreme=extreme)

        if now >= w["expires_at"]:
            reason = f"setup expired after {R3_SETUP_TTL_MIN}m"
            update_watch(wid, status="EXPIRED", terminal_reason=reason)
            q = fresh_ticker(w["symbol"])
            entry_ref = q["ask"] if side == "LONG" else q["bid"]
            entry_ref = entry_ref or q["last"] or price
            did = log_decision(x, "EXPIRED", reason, stage=w["stage"], watchlist_id=wid,
                               pullback_pct=pullback_pct, pullback_extreme=w.get("pullback_extreme"),
                               trigger_level=w.get("trigger_level"), market_entry_price=entry_ref,
                               spread_pct=q.get("spread_pct"))
            shadow_decision(did, x, "EXPIRED", reason, entry_ref, {"pullback_pct": pullback_pct})
            continue

        if reference_broken or pullback_pct > R3_PULLBACK_MAX_PCT:
            reason = (
                "breakout reference invalidated" if reference_broken
                else f"pullback {pullback_pct:.2f}% > {R3_PULLBACK_MAX_PCT:.2f}%"
            )
            update_watch(wid, status="INVALIDATED", terminal_reason=reason)
            q = fresh_ticker(w["symbol"])
            entry_ref = q["ask"] if side == "LONG" else q["bid"]
            entry_ref = entry_ref or q["last"] or price
            did = log_decision(x, "INVALIDATED", reason, stage=w["stage"], watchlist_id=wid,
                               pullback_pct=pullback_pct, pullback_extreme=w.get("pullback_extreme"),
                               trigger_level=w.get("trigger_level"), market_entry_price=entry_ref,
                               spread_pct=q.get("spread_pct"))
            shadow_decision(did, x, "INVALIDATED", reason, entry_ref, {"pullback_pct": pullback_pct})
            continue

        if w["stage"] == "WATCH":
            if pullback_pct < R3_PULLBACK_MIN_PCT:
                continue
            trig = local_trigger(w["symbol"], side)
            if not trig:
                continue
            update_watch(
                wid,
                stage="PULLBACK",
                pullback_extreme=price,
                trigger_level=trig["level"],
            )
            log_decision(
                x, "PULLBACK_CONFIRMED", "valid retest depth; wait for re-acceleration",
                stage="PULLBACK", watchlist_id=wid, pullback_pct=pullback_pct,
                pullback_extreme=price, trigger_level=trig["level"],
            )
            print(
                f"R3 PULLBACK {w['symbol']} {side} depth={pullback_pct:.2f}% "
                f"trigger={trig['level']}",
                flush=True,
            )
            continue

        if w["stage"] != "PULLBACK":
            continue

        pullback_extreme = float(w["pullback_extreme"] or price)
        if side == "LONG":
            pullback_extreme = min(pullback_extreme, price)
        else:
            pullback_extreme = max(pullback_extreme, price)
        update_watch(wid, pullback_extreme=pullback_extreme)

        trig = local_trigger(w["symbol"], side)
        if not trig:
            continue

        # Preserve the original pullback trigger level unless a later completed
        # local structure gives an even more conservative trigger.
        old_trigger = float(w["trigger_level"] or trig["level"])
        if side == "LONG":
            trigger_level = max(old_trigger, trig["level"])
            crossed = price > trigger_level
        else:
            trigger_level = min(old_trigger, trig["level"])
            crossed = price < trigger_level
        update_watch(wid, trigger_level=trigger_level)

        if not crossed:
            continue
        if trig["mom"] < R3_TRIGGER_MIN_1M_MOM or trig["vr"] < R3_TRIGGER_MIN_1M_VOLUME_RATIO:
            continue

        market_align = market_alignment_for(side, ctx)
        if market_align < R3_MIN_MARKET_ALIGNMENT:
            continue

        q = fresh_ticker(w["symbol"])
        entry = q["ask"] if side == "LONG" else q["bid"]
        entry = entry or q["last"]
        spread = q.get("spread_pct")
        if not entry or entry <= 0:
            continue

        if side == "LONG":
            chase = pct(entry, trigger_level)
        else:
            chase = pct(trigger_level, entry)
        adverse_chase = max(chase, 0.0)

        trigger_score = 0.0
        trigger_score += 35 if trig["mom"] >= R3_TRIGGER_MIN_1M_MOM else 0
        trigger_score += 25 if trig["mom"] >= 0.15 else 0
        trigger_score += 20 if trig["vr"] >= 1.20 else 0
        trigger_score += 20 if spread is not None and spread <= 0.10 else 0

        meta = {
            "pullback_pct": pullback_pct,
            "trigger_mom": trig["mom"],
            "trigger_vr": trig["vr"],
            "trigger_score": trigger_score,
            "spread_pct": spread,
            "chase_pct": chase,
        }

        if spread is None or spread > R3_ENTER_MAX_SPREAD:
            continue
        if adverse_chase > R3_ENTER_MAX_TRIGGER_CHASE:
            continue

        allow, risk_reason = risk_allows_new_trade()
        if not allow:
            did = log_decision(x, "RISK_BLOCKED", risk_reason, stage="TRIGGER", watchlist_id=wid,
                               pullback_pct=pullback_pct, pullback_extreme=pullback_extreme,
                               trigger_level=trigger_level, market_entry_price=entry,
                               spread_pct=spread, chase_pct=chase)
            shadow_decision(did, x, "RISK_BLOCKED", risk_reason, entry, meta)
            continue
        if created >= R3_MAX_NEW_PER_SCAN or open_count() >= R3_MAX_OPEN:
            reason = "R3 position limit"
            did = log_decision(x, "NO_SLOT", reason, stage="TRIGGER", watchlist_id=wid,
                               pullback_pct=pullback_pct, pullback_extreme=pullback_extreme,
                               trigger_level=trigger_level, market_entry_price=entry,
                               spread_pct=spread, chase_pct=chase)
            shadow_decision(did, x, "NO_SLOT", reason, entry, meta)
            continue
        if not can_open(w["symbol"]):
            reason = "symbol cooldown/open"
            did = log_decision(x, "COOLDOWN", reason, stage="TRIGGER", watchlist_id=wid,
                               pullback_pct=pullback_pct, pullback_extreme=pullback_extreme,
                               trigger_level=trigger_level, market_entry_price=entry,
                               spread_pct=spread, chase_pct=chase)
            shadow_decision(did, x, "COOLDOWN", reason, entry, meta)
            continue

        # Store useful setup fields inside features for the trade record.
        x["features"]["rsi_5m"] = x["features"].get("rsi_5m")
        x["features"]["volume_ratio_5m"] = x["features"].get("volume_ratio_5m")
        x["features"]["directional_momentum_5m"] = x["features"].get("directional_momentum_5m")
        x["features"]["extension_5m"] = x["features"].get("extension_5m")
        x["features"]["trend_15m"] = x["features"].get("trend_15m", "UNKNOWN")
        x["features"]["trend_1h"] = x["features"].get("trend_1h", "UNKNOWN")

        trade_id = create_trade(x, entry, meta)
        update_watch(wid, status="ENTERED", scanner_trade_id=trade_id, terminal_reason="trigger passed")
        log_decision(x, "ENTER", "pullback + re-acceleration trigger passed", stage="TRIGGER",
                     watchlist_id=wid, scanner_trade_id=trade_id,
                     pullback_pct=pullback_pct, pullback_extreme=pullback_extreme,
                     trigger_level=trigger_level, market_entry_price=entry,
                     spread_pct=spread, chase_pct=chase)
        created += 1
        print(
            f"R3 PAPER ENTRY #{trade_id} {w['symbol']} {side} "
            f"entry={entry} pullback={pullback_pct:.2f}% trigger_mom={trig['mom']:.2f}%",
            flush=True,
        )

    return created


# ---------------------------------------------------------------------------
# New setup creation
# ---------------------------------------------------------------------------

def process_candidates(candidates, ctx):
    added = 0
    for radar in candidates[:R3_MAX_DEEP_ANALYSIS]:
        try:
            if has_active_watch(radar["symbol"], radar["side"]):
                continue
            x = analyze_setup(radar, ctx)
            if not x:
                continue
            print(
                f"R3 SETUP {x['symbol']} {x['side']} move={x['radar_move_pct']:+.2f}% "
                f"strength={x['setup_strength']:.0f} market={x['market_alignment']} "
                f"| {x['decision']}",
                flush=True,
            )
            if x["decision"] != "WATCH":
                did = log_decision(x, x["decision"], x["reason"], stage="SETUP",
                                   market_entry_price=x["entry_ref"],
                                   spread_pct=x.get("spread_pct"))
                shadow_decision(did, x, x["decision"], x["reason"], x["entry_ref"])
                continue

            wid = add_watch(x)
            log_decision(x, "WATCH", x["reason"], stage="WATCH", watchlist_id=wid)
            added += 1
            print(
                f"R3 WATCH #{wid} {x['symbol']} {x['side']} "
                f"setup={x['setup_price']} ref={x['reference_level']}",
                flush=True,
            )
        except Exception as exc:
            print("R3 SETUP ERROR:", radar.get("symbol"), str(exc), flush=True)
    return added


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run():
    started = time.time()
    print("=" * 72, flush=True)
    print("R3 ACTIVE | PAPER ONLY | BREAKOUT -> PULLBACK -> RETRIGGER", flush=True)
    print(
        f"radar>={R3_RADAR_MIN_MOVE:.2f}% pullback={R3_PULLBACK_MIN_PCT:.2f}-"
        f"{R3_PULLBACK_MAX_PCT:.2f}% spread<={R3_ENTER_MAX_SPREAD:.2f}% "
        f"trigger_chase<={R3_ENTER_MAX_TRIGGER_CHASE:.2f}% max_open={R3_MAX_OPEN}",
        flush=True,
    )
    print("=" * 72, flush=True)

    init_r3_database()

    try:
        update_trades()
    except Exception as exc:
        print("R3 TRADE UPDATE ERROR:", str(exc), flush=True)

    previous = load_snapshots()
    current = all_tickers()
    current_map = {x["symbol"]: x for x in current}
    print("R3 MARKETS:", len(current), flush=True)

    try:
        ctx = market_context()
    except Exception as exc:
        print("R3 MARKET CONTEXT ERROR:", str(exc), flush=True)
        ctx = {"bull": 0, "bear": 0, "details": {}}
    print("R3 MARKET CONTEXT:", json.dumps(ctx, ensure_ascii=False), flush=True)

    created = 0
    try:
        created += process_watches(current_map, ctx)
    except Exception as exc:
        print("R3 WATCH PROCESS ERROR:", str(exc), flush=True)

    if not previous:
        save_snapshots(current)
        print("R3 FIRST RUN: dedicated radar baseline created", flush=True)
    else:
        candidates = build_candidates(current, previous)
        save_snapshots(current)
        print("R3 RADAR CANDIDATES:", len(candidates), flush=True)
        process_candidates(candidates, ctx)

    try:
        update_trades()
    except Exception as exc:
        print("R3 POST UPDATE ERROR:", str(exc), flush=True)

    try:
        updated = shadow.update_shadow_results(ENGINE)
        print("R3 SHADOW UPDATE:", updated, flush=True)
    except Exception as exc:
        print("R3 SHADOW UPDATE ERROR:", str(exc), flush=True)

    elapsed = time.time() - started
    print(
        f"R3 RUN COMPLETE | created={created} open={open_count()} "
        f"watches={len(load_active_watches())} elapsed={elapsed:.2f}s",
        flush=True,
    )


if __name__ == "__main__":
    run()
