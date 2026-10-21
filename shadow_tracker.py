"""Post-run shadow tracking for R1M / R2 / R2F.

No order placement. No threshold changes. No strategy decision changes.

R2/R2F:
    Their existing decision tables are copied into shadow_rejections only
    AFTER the scanner process finishes.

R1M:
    Rejected choose_strategy results are buffered in memory during the scan
    and flushed only AFTER the process finishes.

Older pending shadow rows are then evaluated from completed BingX 1m candles.
"""

import json
from datetime import datetime, timezone

import app as base


def db():
    return base.get_db_connection()


def init_shadow_database():
    try:
        conn = db()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS shadow_rejections (
                    id BIGSERIAL PRIMARY KEY,
                    source_key VARCHAR(160) UNIQUE,
                    engine_version VARCHAR(20) NOT NULL,
                    symbol VARCHAR(50) NOT NULL,
                    side VARCHAR(10) NOT NULL,
                    decision VARCHAR(80) NOT NULL,
                    reason TEXT,
                    entry_price DOUBLE PRECISION NOT NULL,
                    signal_price DOUBLE PRECISION,
                    metadata JSONB,
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    return_1m_pct DOUBLE PRECISION,
                    return_3m_pct DOUBLE PRECISION,
                    return_5m_pct DOUBLE PRECISION,
                    return_10m_pct DOUBLE PRECISION,
                    mfe_10m_pct DOUBLE PRECISION,
                    mae_10m_pct DOUBLE PRECISION,
                    tp1_hit_10m BOOLEAN,
                    sl_hit_10m BOOLEAN,
                    first_barrier_10m VARCHAR(10),
                    completed_at TIMESTAMPTZ
                );
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_shadow_pending
                ON shadow_rejections
                    (engine_version, completed_at, observed_at);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_shadow_reason
                ON shadow_rejections
                    (engine_version, decision, observed_at);
                """
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        return True
    except Exception as exc:
        print("SHADOW INIT ERROR:", str(exc), flush=True)
        return False


def record_shadow_batch(records):
    """Insert shadow records in one transaction; failures never propagate."""
    if not records:
        return 0

    try:
        conn = db()
        cur = conn.cursor()
        inserted = 0
        try:
            for r in records:
                entry = float(r.get("entry_price") or 0)
                if entry <= 0:
                    continue

                observed_at = r.get("observed_at")
                if observed_at is None:
                    observed_at = datetime.now(timezone.utc)

                metadata = json.dumps(
                    r.get("metadata") or {},
                    ensure_ascii=False,
                    default=str,
                )

                cur.execute(
                    """
                    INSERT INTO shadow_rejections (
                        source_key, engine_version, symbol, side,
                        decision, reason, entry_price, signal_price,
                        metadata, observed_at
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (source_key) DO NOTHING;
                    """,
                    (
                        r.get("source_key"),
                        str(r.get("engine_version") or ""),
                        str(r.get("symbol") or "").upper(),
                        str(r.get("side") or "").upper(),
                        str(r.get("decision") or "REJECT"),
                        str(r.get("reason") or ""),
                        entry,
                        (
                            float(r["signal_price"])
                            if r.get("signal_price")
                            else None
                        ),
                        metadata,
                        observed_at,
                    ),
                )
                inserted += max(cur.rowcount, 0)

            conn.commit()
            return inserted
        finally:
            cur.close()
            conn.close()
    except Exception as exc:
        print("SHADOW INSERT ERROR:", str(exc), flush=True)
        return 0


def import_existing_decisions(engine_version, since):
    """Copy existing R2/R2F final decisions into the shared shadow table."""
    engine = str(engine_version).upper()

    try:
        conn = db()
        cur = conn.cursor()
        try:
            if engine == "R2":
                cur.execute(
                    """
                    SELECT
                        id, observed_at, symbol, side, regime, strategy,
                        radar_move_pct, direction_score, micro_score,
                        execution_score, forward_score,
                        signal_price, market_entry_price,
                        spread_pct, chase_pct,
                        decision, reason, features
                    FROM r2_decisions
                    WHERE observed_at >= %s
                      AND decision <> 'ENTER'
                    ORDER BY id ASC;
                    """,
                    (since,),
                )
                rows = cur.fetchall()

                records = []
                for row in rows:
                    (
                        decision_id, observed_at, symbol, side,
                        regime, strategy, radar_move_pct,
                        direction_score, micro_score, execution_score,
                        forward_score, signal_price, entry_price,
                        spread_pct, chase_pct, decision, reason, features,
                    ) = row

                    records.append(
                        {
                            "source_key": f"R2:r2_decisions:{decision_id}",
                            "engine_version": "R2",
                            "symbol": symbol,
                            "side": side,
                            "decision": decision,
                            "reason": reason,
                            "entry_price": entry_price,
                            "signal_price": signal_price,
                            "observed_at": observed_at,
                            "metadata": {
                                "source_table": "r2_decisions",
                                "source_id": decision_id,
                                "regime": regime,
                                "strategy": strategy,
                                "radar_move_pct": radar_move_pct,
                                "direction_score": direction_score,
                                "micro_score": micro_score,
                                "execution_score": execution_score,
                                "forward_score": forward_score,
                                "spread_pct": spread_pct,
                                "chase_pct": chase_pct,
                                "features": features or {},
                                "entry_mode": "EXECUTABLE_BBO",
                            },
                        }
                    )

            elif engine == "R2F":
                cur.execute(
                    """
                    SELECT
                        id, observed_at, symbol, side, path,
                        radar_move_pct,
                        direction_score, micro_score, market_score,
                        execution_score, fast_score,
                        signal_price, market_entry_price,
                        spread_pct, chase_pct, analysis_latency_ms,
                        decision, reason, features
                    FROM r2_fast_decisions
                    WHERE observed_at >= %s
                      AND decision <> 'ENTER'
                    ORDER BY id ASC;
                    """,
                    (since,),
                )
                rows = cur.fetchall()

                records = []
                for row in rows:
                    (
                        decision_id, observed_at, symbol, side, path,
                        radar_move_pct, direction_score, micro_score,
                        market_score, execution_score, fast_score,
                        signal_price, entry_price, spread_pct,
                        chase_pct, latency_ms, decision, reason, features,
                    ) = row

                    records.append(
                        {
                            "source_key": (
                                f"R2F:r2_fast_decisions:{decision_id}"
                            ),
                            "engine_version": "R2F",
                            "symbol": symbol,
                            "side": side,
                            "decision": decision,
                            "reason": reason,
                            "entry_price": entry_price,
                            "signal_price": signal_price,
                            "observed_at": observed_at,
                            "metadata": {
                                "source_table": "r2_fast_decisions",
                                "source_id": decision_id,
                                "path": path,
                                "radar_move_pct": radar_move_pct,
                                "direction_score": direction_score,
                                "micro_score": micro_score,
                                "market_score": market_score,
                                "execution_score": execution_score,
                                "fast_score": fast_score,
                                "spread_pct": spread_pct,
                                "chase_pct": chase_pct,
                                "analysis_latency_ms": latency_ms,
                                "features": features or {},
                                "entry_mode": "EXECUTABLE_BBO",
                            },
                        }
                    )
            else:
                return 0
        finally:
            cur.close()
            conn.close()

        return record_shadow_batch(records)

    except Exception as exc:
        print(
            "SHADOW IMPORT ERROR:",
            engine,
            str(exc),
            flush=True,
        )
        return 0


def _candle_open_ms(candle):
    value = int(candle.get("time", 0) or 0)
    if 0 < value < 10**12:
        value *= 1000
    return value


def _candle_close_ms(candle):
    return _candle_open_ms(candle) + 60_000


def _directional_return(side, entry, price):
    if str(side).upper() == "SHORT":
        return (entry - price) / entry * 100.0
    return (price - entry) / entry * 100.0


def _return_at(candles, observed_ms, minutes, side, entry):
    target_ms = observed_ms + minutes * 60_000

    for candle in candles:
        close_ms = _candle_close_ms(candle)
        if close_ms < target_ms:
            continue

        # Don't bridge a large missing-data gap.
        if close_ms > target_ms + 90_000:
            return None

        return _directional_return(
            side,
            entry,
            float(candle["close"]),
        )

    return None


def _ten_minute_stats(candles, observed_ms, side, entry):
    end_ms = observed_ms + 10 * 60_000 + 90_000
    window = [
        c for c in candles
        if _candle_close_ms(c) > observed_ms
        and _candle_close_ms(c) <= end_ms
    ]
    if not window:
        return None, None, None

    high = max(float(c["high"]) for c in window)
    low = min(float(c["low"]) for c in window)
    side = str(side).upper()

    if side == "SHORT":
        mfe = (entry - low) / entry * 100.0
        mae = (entry - high) / entry * 100.0
    else:
        mfe = (high - entry) / entry * 100.0
        mae = (low - entry) / entry * 100.0

    # Conservative: if both barriers are inside one 1m candle,
    # count SL first because intrabar order is unknowable.
    first_barrier = "NONE"
    for candle in window:
        high1 = float(candle["high"])
        low1 = float(candle["low"])

        if side == "SHORT":
            tp1_hit = low1 <= entry * 0.98
            sl_hit = high1 >= entry * 1.025
        else:
            tp1_hit = high1 >= entry * 1.02
            sl_hit = low1 <= entry * 0.975

        if sl_hit:
            first_barrier = "SL"
            break
        if tp1_hit:
            first_barrier = "TP1"
            break

    return mfe, mae, first_barrier


def update_shadow_results(engine_version, limit=180, max_symbols=12):
    """Fill pending 1/3/5/10m results after the strategy process has run."""
    engine = str(engine_version).upper()

    try:
        conn = db()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT
                    id, symbol, side, entry_price, observed_at,
                    return_1m_pct, return_3m_pct,
                    return_5m_pct, return_10m_pct
                FROM shadow_rejections
                WHERE engine_version=%s
                  AND completed_at IS NULL
                  AND observed_at >= NOW() - INTERVAL '90 minutes'
                ORDER BY observed_at ASC
                LIMIT %s;
                """,
                (engine, int(limit)),
            )
            rows = cur.fetchall()
        finally:
            cur.close()
            conn.close()

        if not rows:
            return 0

        by_symbol = {}
        for row in rows:
            by_symbol.setdefault(row[1], []).append(row)

        updated = 0

        for symbol in list(by_symbol.keys())[: int(max_symbols)]:
            try:
                raw = base.get_klines(symbol, "1m", 120)
                candles = raw[:-1] if len(raw) > 1 else []
                if not candles:
                    continue
            except Exception as exc:
                print(
                    "SHADOW KLINE ERROR:",
                    engine,
                    symbol,
                    str(exc),
                    flush=True,
                )
                continue

            for row in by_symbol[symbol]:
                (
                    shadow_id,
                    _symbol,
                    side,
                    entry_price,
                    observed_at,
                    r1,
                    r3,
                    r5,
                    r10,
                ) = row

                entry = float(entry_price)
                observed_ms = int(observed_at.timestamp() * 1000)
                current = {1: r1, 3: r3, 5: r5, 10: r10}
                values = {}

                for minutes, column in (
                    (1, "return_1m_pct"),
                    (3, "return_3m_pct"),
                    (5, "return_5m_pct"),
                    (10, "return_10m_pct"),
                ):
                    if current[minutes] is not None:
                        continue

                    value = _return_at(
                        candles,
                        observed_ms,
                        minutes,
                        side,
                        entry,
                    )
                    if value is not None:
                        values[column] = value
                        current[minutes] = value

                if current[10] is not None:
                    mfe, mae, first_barrier = _ten_minute_stats(
                        candles,
                        observed_ms,
                        side,
                        entry,
                    )
                    if mfe is not None:
                        values["mfe_10m_pct"] = mfe
                        values["mae_10m_pct"] = mae
                        values["tp1_hit_10m"] = bool(mfe >= 2.0)
                        values["sl_hit_10m"] = bool(mae <= -2.5)
                        values["first_barrier_10m"] = first_barrier

                complete = all(
                    current[m] is not None
                    for m in (1, 3, 5, 10)
                )

                if not values and not complete:
                    continue

                conn2 = db()
                cur2 = conn2.cursor()
                try:
                    assignments = []
                    params = []

                    for key, value in values.items():
                        assignments.append(f"{key}=%s")
                        params.append(value)

                    if complete:
                        assignments.append("completed_at=NOW()")

                    if not assignments:
                        continue

                    params.append(shadow_id)
                    cur2.execute(
                        "UPDATE shadow_rejections SET "
                        + ", ".join(assignments)
                        + " WHERE id=%s;",
                        params,
                    )
                    conn2.commit()
                    updated += 1
                except Exception as exc:
                    conn2.rollback()
                    print(
                        "SHADOW RESULT ERROR:",
                        engine,
                        shadow_id,
                        str(exc),
                        flush=True,
                    )
                finally:
                    cur2.close()
                    conn2.close()

        return updated

    except Exception as exc:
        print(
            "SHADOW UPDATE ERROR:",
            engine,
            str(exc),
            flush=True,
        )
        return 0
