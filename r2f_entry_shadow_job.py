"""
R2FS Entry Shadow V1
====================

Forward-only entry A/B experiment for the existing R2F paper core.

Important:
- PAPER ONLY. Never sends a private order.
- Does NOT modify R2F logic or R2F historical trades.
- Consumes fresh rows already written by r2_fast_decisions.
- Writes its own engine_version='R2FS' trades into scanner_trades so the exact
  same V4.2 replay/TP/SL/cost model can be reused.
- First run seeds a watermark at the latest R2F decision and DOES NOT replay
  historical decisions at stale prices.

Hypothesis:
R2F currently over-rewards very strong momentum. R2FS reduces that bias and
puts more weight on clean execution, remaining room, non-overheated RSI and
confirmed-but-not-exhausted structure.
"""

import json
import os
from datetime import datetime, timezone

import app as base

ENGINE = "R2FS"
VARIANT = "ENTRY_V1"
PAPER_ONLY = True

# Independent paper limits. These do not consume R2F's slots.
MAX_OPEN = int(os.environ.get("R2FS_MAX_OPEN", "3"))
MAX_NEW_PER_RUN = int(os.environ.get("R2FS_MAX_NEW_PER_RUN", "2"))
COOLDOWN_HOURS = float(os.environ.get("R2FS_COOLDOWN_HOURS", "2"))

# Entry V1 gates.
MIN_SCORE = float(os.environ.get("R2FS_MIN_SCORE", "72"))
MIN_ROOM_SCORE = float(os.environ.get("R2FS_MIN_ROOM_SCORE", "55"))
MIN_STRUCTURE_SCORE = float(os.environ.get("R2FS_MIN_STRUCTURE_SCORE", "40"))
MAX_SPREAD_PCT = float(os.environ.get("R2FS_MAX_SPREAD_PCT", "0.18"))
MAX_ADVERSE_CHASE_PCT = float(os.environ.get("R2FS_MAX_ADVERSE_CHASE_PCT", "0.20"))
MAX_EXTENSION_1M = float(os.environ.get("R2FS_MAX_EXTENSION_1M", "1.20"))
MAX_EXTENSION_5M = float(os.environ.get("R2FS_MAX_EXTENSION_5M", "3.50"))
LONG_MAX_RSI1 = float(os.environ.get("R2FS_LONG_MAX_RSI1", "80"))
SHORT_MIN_RSI1 = float(os.environ.get("R2FS_SHORT_MIN_RSI1", "20"))

# Same trade economics as R2F.
TP1_PCT = 2.0
TP2_PCT = 4.0
TP3_PCT = 8.0
SL_PCT = 2.5
SIM_MARGIN_USDT = 10.0
SIM_LEVERAGE = 8

SOURCE_BATCH_LIMIT = int(os.environ.get("R2FS_SOURCE_BATCH_LIMIT", "500"))

# Hard safety.
base.LIVE_TRADING = False
if hasattr(base, "R1_LIVE_TRADING"):
    base.R1_LIVE_TRADING = False


def db():
    return base.get_db_connection()


def clamp(value, lo=0.0, hi=100.0):
    return max(lo, min(hi, float(value)))


def sf(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def init_database():
    base.init_database()
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS r2f_entry_shadow_state (
                singleton_id SMALLINT PRIMARY KEY DEFAULT 1,
                last_source_decision_id BIGINT NOT NULL DEFAULT 0,
                initialized_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                meta JSONB
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS r2f_entry_shadow_decisions (
                id BIGSERIAL PRIMARY KEY,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                variant VARCHAR(30) NOT NULL,
                source_decision_id BIGINT NOT NULL UNIQUE,
                source_observed_at TIMESTAMPTZ,
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL,
                source_path VARCHAR(20),
                source_decision VARCHAR(60),
                source_fast_score DOUBLE PRECISION,
                shadow_score DOUBLE PRECISION,
                room_score DOUBLE PRECISION,
                structure_score DOUBLE PRECISION,
                execution_score DOUBLE PRECISION,
                market_score DOUBLE PRECISION,
                spread_pct DOUBLE PRECISION,
                chase_pct DOUBLE PRECISION,
                extension_1m DOUBLE PRECISION,
                extension_5m DOUBLE PRECISION,
                rsi_1m DOUBLE PRECISION,
                rsi_5m DOUBLE PRECISION,
                decision VARCHAR(40) NOT NULL,
                reason TEXT,
                scanner_trade_id BIGINT,
                features JSONB
            );
            """
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_r2fs_shadow_time "
            "ON r2f_entry_shadow_decisions(observed_at DESC);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_r2fs_shadow_symbol "
            "ON r2f_entry_shadow_decisions(symbol, observed_at DESC);"
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def get_state():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT last_source_decision_id FROM r2f_entry_shadow_state "
            "WHERE singleton_id=1;"
        )
        row = cur.fetchone()
        return int(row[0]) if row else None
    finally:
        cur.close()
        conn.close()


def seed_state():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(MAX(id),0) FROM r2_fast_decisions;")
        latest = int(cur.fetchone()[0] or 0)
        cur.execute(
            """
            INSERT INTO r2f_entry_shadow_state(
                singleton_id,last_source_decision_id,meta
            )
            VALUES(1,%s,%s)
            ON CONFLICT(singleton_id) DO NOTHING;
            """,
            (
                latest,
                json.dumps(
                    {
                        "engine": ENGINE,
                        "variant": VARIANT,
                        "forward_only": True,
                        "seeded_at": datetime.now(timezone.utc).isoformat(),
                    }
                ),
            ),
        )
        conn.commit()
        return latest
    finally:
        cur.close()
        conn.close()


def advance_state(source_id):
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE r2f_entry_shadow_state
            SET last_source_decision_id=%s,updated_at=NOW()
            WHERE singleton_id=1;
            """,
            (int(source_id),),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def load_new_source_decisions(after_id):
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                id,observed_at,symbol,side,path,decision,
                direction_score,micro_score,market_score,execution_score,fast_score,
                market_entry_price,spread_pct,chase_pct,features
            FROM r2_fast_decisions
            WHERE id>%s
            ORDER BY id ASC
            LIMIT %s;
            """,
            (int(after_id), SOURCE_BATCH_LIMIT),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close()
        conn.close()


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
            (ENGINE, symbol, COOLDOWN_HOURS),
        )
        return cur.fetchone() is None
    finally:
        cur.close()
        conn.close()


def _extension_room(ext, one_minute):
    # Negative directional extension means the signal is not stretched in the
    # intended direction. Structure gates still decide whether it is usable.
    if ext <= 0:
        return 100.0
    if one_minute:
        if ext <= 0.45:
            return 100.0
        if ext <= 0.75:
            return 85.0
        if ext <= 1.00:
            return 65.0
        if ext <= 1.20:
            return 35.0
        return 0.0
    if ext <= 1.50:
        return 100.0
    if ext <= 2.50:
        return 85.0
    if ext <= 3.50:
        return 55.0
    if ext <= 4.50:
        return 25.0
    return 0.0


def _rsi_room(side, rsi1):
    if side == "LONG":
        if 50 <= rsi1 <= 72:
            return 100.0
        if 46 <= rsi1 < 50 or 72 < rsi1 <= 76:
            return 80.0
        if 42 <= rsi1 < 46 or 76 < rsi1 <= 80:
            return 50.0
        return 15.0

    if 28 <= rsi1 <= 50:
        return 100.0
    if 24 <= rsi1 < 28 or 50 < rsi1 <= 54:
        return 80.0
    if 20 <= rsi1 < 24 or 54 < rsi1 <= 58:
        return 50.0
    return 15.0


def score_entry(src):
    feat = src.get("features") or {}
    if isinstance(feat, str):
        try:
            feat = json.loads(feat)
        except Exception:
            feat = {}

    side = str(src["side"]).upper()
    ext1 = sf(feat.get("extension_1m"), 0.0)
    ext5 = sf(feat.get("extension_5m"), 0.0)
    rsi1 = sf(feat.get("rsi_1m"), 50.0)
    rsi5 = sf(feat.get("rsi_5m"), 50.0)
    dm1 = sf(feat.get("directional_momentum_1m_3m"), 0.0)
    dm5 = sf(feat.get("directional_momentum_5m_15m"), 0.0)

    align1 = bool(feat.get("align_1m"))
    align5 = bool(feat.get("align_5m"))
    align15 = bool(feat.get("align_15m"))
    align1h = bool(feat.get("align_1h"))
    breakout = bool(feat.get("breakout_5m", feat.get("breakout", False)))

    ext1_room = _extension_room(ext1, True)
    ext5_room = _extension_room(ext5, False)
    rsi_room = _rsi_room(side, rsi1)
    room_score = (ext1_room + ext5_room + rsi_room) / 3.0

    # Confirmation matters, but extreme momentum does not get extra reward.
    structure = 0.0
    structure += 25.0 if align5 else 0.0
    structure += 15.0 if align1 else 0.0
    structure += 10.0 if align15 else 0.0
    structure += 10.0 if align1h else 0.0
    structure += 15.0 if breakout else 0.0

    if 0.15 <= dm5 <= 1.20:
        structure += 15.0
    elif 0.05 <= dm5 < 0.15 or 1.20 < dm5 <= 1.80:
        structure += 7.0

    if 0.03 <= dm1 <= 0.30:
        structure += 10.0
    elif 0.00 < dm1 < 0.03 or 0.30 < dm1 <= 0.50:
        structure += 5.0

    structure_score = clamp(structure)

    execution_score = sf(src.get("execution_score"), 0.0)
    # FULL-path rows do not always store market_score. Use neutral 50 rather
    # than inventing bullish/bearish alignment.
    market_score = sf(src.get("market_score"), 50.0)
    if src.get("market_score") is None:
        market_score = 50.0

    shadow_score = clamp(
        0.30 * execution_score
        + 0.20 * market_score
        + 0.30 * room_score
        + 0.20 * structure_score
    )

    spread = src.get("spread_pct")
    chase = max(sf(src.get("chase_pct"), 0.0), 0.0)
    entry_price = sf(src.get("market_entry_price"), 0.0)

    decision = "ENTER"
    reason = "R2FS_ENTRY_V1_PASS"

    if entry_price <= 0:
        decision, reason = "REJECT_PRICE", "NO_EXECUTABLE_ENTRY_PRICE"
    elif spread is None or sf(spread, 999.0) > MAX_SPREAD_PCT:
        decision, reason = (
            "REJECT_SPREAD",
            f"spread={sf(spread,999):.3f}% > {MAX_SPREAD_PCT:.2f}%",
        )
    elif chase > MAX_ADVERSE_CHASE_PCT:
        decision, reason = (
            "REJECT_CHASE",
            f"adverse_chase={chase:.3f}% > {MAX_ADVERSE_CHASE_PCT:.2f}%",
        )
    elif ext1 > MAX_EXTENSION_1M or ext5 > MAX_EXTENSION_5M:
        decision, reason = (
            "REJECT_EXTENSION",
            f"ext1={ext1:.2f}% ext5={ext5:.2f}%",
        )
    elif side == "LONG" and rsi1 > LONG_MAX_RSI1:
        decision, reason = "REJECT_RSI", f"LONG rsi1={rsi1:.1f}"
    elif side == "SHORT" and rsi1 < SHORT_MIN_RSI1:
        decision, reason = "REJECT_RSI", f"SHORT rsi1={rsi1:.1f}"
    elif room_score < MIN_ROOM_SCORE:
        decision, reason = (
            "REJECT_ROOM",
            f"room={room_score:.1f} < {MIN_ROOM_SCORE:.1f}",
        )
    elif structure_score < MIN_STRUCTURE_SCORE:
        decision, reason = (
            "REJECT_STRUCTURE",
            f"structure={structure_score:.1f} < {MIN_STRUCTURE_SCORE:.1f}",
        )
    elif shadow_score < MIN_SCORE:
        decision, reason = (
            "REJECT_SCORE",
            f"score={shadow_score:.1f} < {MIN_SCORE:.1f}",
        )

    return {
        "features": feat,
        "shadow_score": shadow_score,
        "room_score": room_score,
        "structure_score": structure_score,
        "execution_score": execution_score,
        "market_score": market_score,
        "extension_1m": ext1,
        "extension_5m": ext5,
        "rsi_1m": rsi1,
        "rsi_5m": rsi5,
        "decision": decision,
        "reason": reason,
    }


def create_shadow_trade(src, scored):
    entry = sf(src["market_entry_price"])
    side = str(src["side"]).upper()

    if side == "LONG":
        tp1 = entry * (1.0 + TP1_PCT / 100.0)
        tp2 = entry * (1.0 + TP2_PCT / 100.0)
        tp3 = entry * (1.0 + TP3_PCT / 100.0)
        sl = entry * (1.0 - SL_PCT / 100.0)
    else:
        tp1 = entry * (1.0 - TP1_PCT / 100.0)
        tp2 = entry * (1.0 - TP2_PCT / 100.0)
        tp3 = entry * (1.0 - TP3_PCT / 100.0)
        sl = entry * (1.0 + SL_PCT / 100.0)

    feat = scored["features"]
    breakout = bool(feat.get("breakout_5m", feat.get("breakout", False)))
    strategy = "BREAKOUT" if breakout else "TREND"

    reasons = [
        f"{ENGINE} {VARIANT}",
        f"source_decision_id={src['id']}",
        f"source={src.get('path')}:{src.get('decision')}",
        f"shadow_score={scored['shadow_score']:.1f}",
        f"room={scored['room_score']:.1f}",
        f"structure={scored['structure_score']:.1f}",
        f"execution={scored['execution_score']:.1f}",
        f"market={scored['market_score']:.1f}",
        f"spread={sf(src.get('spread_pct')):.3f}%",
        f"chase={sf(src.get('chase_pct')):.3f}%",
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
                src["symbol"],
                side,
                strategy,
                ENGINE,
                entry,
                int(round(scored["shadow_score"])),
                int(round(scored["room_score"])),
                int(round(scored["structure_score"])),
                scored["rsi_1m"],
                scored["rsi_5m"],
                sf(feat.get("volume_ratio_5m"), 0.0),
                sf(feat.get("momentum_5m_15m"), 0.0),
                sf(feat.get("momentum_5m_15m"), 0.0),
                sf(feat.get("volatility_pct"), 0.0),
                scored["extension_5m"],
                "SHADOW",
                "SHADOW",
                breakout,
                json.dumps(reasons, ensure_ascii=False),
                tp1,
                tp2,
                tp3,
                sl,
                entry,
                entry,
                SIM_MARGIN_USDT,
                SIM_LEVERAGE,
            ),
        )
        trade_id = int(cur.fetchone()[0])
        conn.commit()
        return trade_id
    finally:
        cur.close()
        conn.close()


def log_shadow_decision(src, scored, decision=None, reason=None, trade_id=None):
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO r2f_entry_shadow_decisions (
                variant,source_decision_id,source_observed_at,
                symbol,side,source_path,source_decision,source_fast_score,
                shadow_score,room_score,structure_score,execution_score,market_score,
                spread_pct,chase_pct,extension_1m,extension_5m,rsi_1m,rsi_5m,
                decision,reason,scanner_trade_id,features
            )
            VALUES (
                %s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s
            )
            ON CONFLICT(source_decision_id) DO NOTHING;
            """,
            (
                VARIANT,
                src["id"],
                src.get("observed_at"),
                src["symbol"],
                src["side"],
                src.get("path"),
                src.get("decision"),
                src.get("fast_score"),
                scored["shadow_score"],
                scored["room_score"],
                scored["structure_score"],
                scored["execution_score"],
                scored["market_score"],
                src.get("spread_pct"),
                src.get("chase_pct"),
                scored["extension_1m"],
                scored["extension_5m"],
                scored["rsi_1m"],
                scored["rsi_5m"],
                decision or scored["decision"],
                reason or scored["reason"],
                trade_id,
                json.dumps(scored["features"]),
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def process_source(src, created_this_run):
    scored = score_entry(src)

    if scored["decision"] != "ENTER":
        log_shadow_decision(src, scored)
        return False

    if created_this_run >= MAX_NEW_PER_RUN or open_count() >= MAX_OPEN:
        log_shadow_decision(
            src,
            scored,
            decision="NO_SLOT",
            reason="R2FS_INDEPENDENT_POSITION_LIMIT",
        )
        return False

    if not can_open(src["symbol"]):
        log_shadow_decision(
            src,
            scored,
            decision="COOLDOWN",
            reason="R2FS_SYMBOL_COOLDOWN_OR_OPEN",
        )
        return False

    trade_id = create_shadow_trade(src, scored)
    log_shadow_decision(
        src,
        scored,
        decision="ENTER",
        reason="R2FS_ENTRY_V1_PASS",
        trade_id=trade_id,
    )

    print(
        f"R2FS ENTER #{trade_id} {src['symbol']} {src['side']} "
        f"entry={sf(src['market_entry_price']):.10g} "
        f"score={scored['shadow_score']:.1f} "
        f"room={scored['room_score']:.1f} "
        f"structure={scored['structure_score']:.1f}",
        flush=True,
    )
    return True


def update_shadow_trades():
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
            trade_id = row[0]
            symbol = row[1]
            if symbol not in prices:
                continue
            try:
                # Exact same replay/TP/SL/cost path used by the existing paper core.
                base.update_v42_trade_replay(cur, row, prices[symbol])
                conn.commit()
            except Exception as exc:
                conn.rollback()
                print(
                    "R2FS UPDATE ERROR:",
                    trade_id,
                    symbol,
                    str(exc),
                    flush=True,
                )
    finally:
        cur.close()
        conn.close()


def run():
    print("=" * 72, flush=True)
    print(
        "R2FS ENTRY SHADOW V1 | FORWARD ONLY | PAPER ONLY | R2F UNCHANGED",
        flush=True,
    )
    print(
        f"score>={MIN_SCORE:.1f} room>={MIN_ROOM_SCORE:.1f} "
        f"structure>={MIN_STRUCTURE_SCORE:.1f} "
        f"spread<={MAX_SPREAD_PCT:.2f}% chase<={MAX_ADVERSE_CHASE_PCT:.2f}%",
        flush=True,
    )
    print("=" * 72, flush=True)

    init_database()

    # Keep already-open shadow trades progressing even when no new source signal.
    try:
        update_shadow_trades()
    except Exception as exc:
        print("R2FS PRE UPDATE ERROR:", str(exc), flush=True)

    state = get_state()
    if state is None:
        latest = seed_state()
        print(
            f"R2FS FIRST RUN: forward watermark seeded at source decision #{latest}. "
            "Historical R2F decisions were NOT replayed.",
            flush=True,
        )
        return

    rows = load_new_source_decisions(state)
    print(
        f"R2FS SOURCE: after={state} new_decisions={len(rows)} open={open_count()}",
        flush=True,
    )

    created = 0
    for src in rows:
        try:
            if process_source(src, created):
                created += 1
        except Exception as exc:
            # Do not freeze the entire forward stream on one malformed source row.
            print(
                "R2FS SOURCE ERROR:",
                src.get("id"),
                src.get("symbol"),
                str(exc),
                flush=True,
            )
            try:
                fallback = {
                    "features": src.get("features") or {},
                    "shadow_score": 0.0,
                    "room_score": 0.0,
                    "structure_score": 0.0,
                    "execution_score": sf(src.get("execution_score"), 0.0),
                    "market_score": sf(src.get("market_score"), 50.0),
                    "extension_1m": 0.0,
                    "extension_5m": 0.0,
                    "rsi_1m": 50.0,
                    "rsi_5m": 50.0,
                    "decision": "ERROR",
                    "reason": str(exc)[:500],
                }
                log_shadow_decision(
                    src,
                    fallback,
                    decision="ERROR",
                    reason=str(exc)[:500],
                )
            except Exception:
                pass
        finally:
            advance_state(src["id"])

    try:
        update_shadow_trades()
    except Exception as exc:
        print("R2FS POST UPDATE ERROR:", str(exc), flush=True)

    print(
        f"R2FS RUN COMPLETE | processed={len(rows)} "
        f"created={created} open={open_count()}",
        flush=True,
    )


if __name__ == "__main__":
    run()
