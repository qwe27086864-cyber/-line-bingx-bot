
"""V4.3R2 simulated-only market reversal manager."""

import os
from datetime import datetime, timezone

VERSION = "V4.3R2"
R1 = "V4.3R1"

R2_MIN_CONFIRM_SECONDS = int(
    os.getenv("R2_MIN_CONFIRM_SECONDS", "120")
)
R2_TRIGGER_MARKET_5M_PCT = float(
    os.getenv("R2_TRIGGER_MARKET_5M_PCT", "0.35")
)
R2_MIN_FAVORABLE_PCT = float(
    os.getenv("R2_MIN_FAVORABLE_PCT", "0.30")
)
R2_EARLY_EXIT_MAX_ADVERSE_PCT = float(
    os.getenv("R2_EARLY_EXIT_MAX_ADVERSE_PCT", "1.50")
)


def market_adverse(side, btc, eth):
    threshold = R2_TRIGGER_MARKET_5M_PCT
    if side == "LONG":
        return btc <= -threshold and eth <= -threshold
    return btc >= threshold and eth >= threshold


def favorable_pct(side, entry, high, low):
    if side == "SHORT":
        return (entry - low) / entry * 100
    return (high - entry) / entry * 100


def coin_weakness(
    side, close5, ema9, ema20,
    rsi, close15, ema20_15
):
    if side == "LONG":
        flags = [
            close5 < ema20,
            ema9 < ema20,
            rsi < 45,
            close15 < ema20_15,
        ]
    else:
        flags = [
            close5 > ema20,
            ema9 > ema20,
            rsi > 55,
            close15 > ema20_15,
        ]
    return sum(bool(flag) for flag in flags)


def install_r2(ns):
    if ns.get("_R2_INSTALLED"):
        return

    if ns.get("LIVE_TRADING"):
        raise RuntimeError(
            "R2 requires LIVE_TRADING=false"
        )

    ns["_R2_INSTALLED"] = True
    ns["SCANNER_ENGINE_VERSION"] = VERSION

    db_connect = ns["get_db_connection"]
    old_replay = ns["update_v43_trade_replay"]
    get_prices = ns["get_all_prices"]
    get_klines = ns["get_klines"]
    ema = ns["calculate_ema"]
    rsi_fn = ns["calculate_rsi"]
    finalize = ns["finalize_trade"]
    side_return = ns["side_return_pct"]

    def init_r2_table():
        conn = db_connect()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS
                    r2_position_states (
                        trade_id INTEGER PRIMARY KEY
                            REFERENCES scanner_trades(id),
                        position_state VARCHAR(12)
                            NOT NULL DEFAULT 'GREEN',
                        adverse_observations INTEGER
                            NOT NULL DEFAULT 0,
                        last_observation_at TIMESTAMPTZ,
                        last_market_bar_ms BIGINT,
                        first_warning_at TIMESTAMPTZ,
                        last_note TEXT,
                        updated_at TIMESTAMPTZ
                            NOT NULL DEFAULT NOW()
                    )
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS
                    idx_r2_trade_version_status
                    ON scanner_trades(
                        engine_version, status
                    )
                """)
            conn.commit()
        finally:
            conn.close()

    init_r2_table()

    def change_5m(symbol):
        candles = get_klines(symbol, "5m", 8)
        candles = candles[:-1]

        if len(candles) < 2:
            return None

        previous = candles[-2]["close"]
        if previous <= 0:
            return None

        change = (
            candles[-1]["close"] / previous - 1
        ) * 100

        return change, int(candles[-1]["time"])

    def weak_count(symbol, side):
        c5 = get_klines(
            symbol, "5m", 60
        )[:-1]

        c15 = get_klines(
            symbol, "15m", 60
        )[:-1]

        if len(c5) < 50 or len(c15) < 50:
            return None

        p5 = [c["close"] for c in c5]
        p15 = [c["close"] for c in c15]

        e9 = ema(p5, 9)
        e20 = ema(p5, 20)
        e20_15 = ema(p15, 20)
        rsi = rsi_fn(p5, 14)

        if None in (e9, e20, e20_15, rsi):
            return None

        return coin_weakness(
            side,
            p5[-1],
            e9,
            e20,
            rsi,
            p15[-1],
            e20_15,
        )

    def save_state(
        cur, trade_id, state, count,
        now, bar_ms, warning, note
    ):
        cur.execute("""
            INSERT INTO r2_position_states (
                trade_id,
                position_state,
                adverse_observations,
                last_observation_at,
                last_market_bar_ms,
                first_warning_at,
                last_note,
                updated_at
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s, NOW()
            )
            ON CONFLICT(trade_id)
            DO UPDATE SET
                position_state =
                    EXCLUDED.position_state,
                adverse_observations =
                    EXCLUDED.adverse_observations,
                last_observation_at =
                    EXCLUDED.last_observation_at,
                last_market_bar_ms =
                    EXCLUDED.last_market_bar_ms,
                first_warning_at =
                    EXCLUDED.first_warning_at,
                last_note =
                    EXCLUDED.last_note,
                updated_at = NOW()
        """, (
            trade_id,
            state,
            count,
            now,
            bar_ms,
            warning,
            note,
        ))

    def apply_r2(cur, row, current, majors):
        (
            tid, symbol, side, version,
            signal_time, entry,
            tp1, tp2, tp3, sl,
            tp1_hit, tp2_hit, tp3_hit,
            high, low, checked,
            margin, leverage,
            strategy, score
        ) = row

        if tp1_hit or entry is None or entry <= 0:
            return

        btc_data, eth_data = majors

        if btc_data is None or eth_data is None:
            return

        cur.execute("""
            SELECT
                status,
                highest_price,
                lowest_price,
                tp1_hit
            FROM scanner_trades
            WHERE id = %s
        """, (tid,))

        active = cur.fetchone()

        if not active:
            return

        if active[0] != "OPEN" or active[3]:
            return

        high = max(
            float(active[1] or entry),
            current
        )
        low = min(
            float(active[2] or entry),
            current
        )

        now = datetime.now(timezone.utc)
        btc = btc_data[0]
        eth = eth_data[0]
        bar_ms = min(
            btc_data[1],
            eth_data[1]
        )

        cur.execute("""
            SELECT
                adverse_observations,
                last_observation_at,
                last_market_bar_ms,
                first_warning_at
            FROM r2_position_states
            WHERE trade_id = %s
            FOR UPDATE
        """, (tid,))

        previous = cur.fetchone()

        count_old = previous[0] if previous else 0
        checked_old = previous[1] if previous else None
        bar_old = previous[2] if previous else None
        warning = previous[3] if previous else None

        if bar_old is not None and bar_ms <= bar_old:
            return

        if checked_old:
            elapsed = (
                now - checked_old
            ).total_seconds()

            if elapsed < R2_MIN_CONFIRM_SECONDS:
                return

        adverse = market_adverse(
            side, btc, eth
        )

        count = (
            count_old + 1 if adverse else 0
        )

        state = (
            "YELLOW" if adverse else "GREEN"
        )

        note = (
            f"BTC 5m={btc:+.2f}%, "
            f"ETH 5m={eth:+.2f}%"
        )

        if adverse and warning is None:
            warning = now

        if not adverse:
            warning = None

        if count >= 2:
            weakness = weak_count(
                symbol, side
            )

            peak = favorable_pct(
                side, entry, high, low
            )

            current_ret = side_return(
                side, entry, current
            )

            if weakness is not None and weakness >= 3:
                state = "RED"

                note += (
                    f"; weakness={weakness}"
                    f"; MFE={peak:.2f}%"
                    f"; current={current_ret:.2f}%"
                )

                if (
                    peak >= R2_MIN_FAVORABLE_PCT
                    and current_ret >=
                    -R2_EARLY_EXIT_MAX_ADVERSE_PCT
                ):
                    finalize(
                        cur,
                        tid,
                        current,
                        current_ret,
                        "R2_REVERSAL_EXIT",
                        False,
                        False,
                        False,
                        False,
                        high,
                        low,
                        margin,
                        leverage,
                        note,
                    )

        save_state(
            cur,
            tid,
            state,
            count,
            now,
            bar_ms,
            warning,
            note,
        )

    def r2_update_trades():
        prices = get_prices()

        try:
            majors = (
                change_5m("BTC-USDT"),
                change_5m("ETH-USDT")
            )
        except Exception as exc:
            print(
                "R2 MARKET DATA ERROR:",
                exc,
                flush=True
            )
            majors = (None, None)

        conn = db_connect()

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        id, symbol, side,
                        engine_version,
                        signal_time,
                        entry_price,
                        tp1, tp2, tp3, sl,
                        tp1_hit, tp2_hit, tp3_hit,
                        highest_price,
                        lowest_price,
                        last_checked_at,
                        sim_margin_usdt,
                        sim_leverage,
                        strategy,
                        score
                    FROM scanner_trades
                    WHERE status = 'OPEN'
                      AND strategy IN (
                          'TREND', 'BREAKOUT'
                      )
                    ORDER BY id
                """)

                rows = cur.fetchall()

                for row in rows:
                    tid = row[0]
                    symbol = row[1]
                    version = row[3]

                    if symbol not in prices:
                        continue

                    try:
                        if version in (VERSION, R1):
                            old_replay(
                                cur,
                                row,
                                prices[symbol]
                            )

                            if version == VERSION:
                                apply_r2(
                                    cur,
                                    row,
                                    float(prices[symbol]),
                                    majors,
                                )

                        elif version in (
                            "V4.2R1",
                            "V4.2",
                            "V4.2_PRE"
                        ):
                            ns["update_v42_trade_replay"](
                                cur, row, prices[symbol]
                            )

                        elif version == "V3":
                            ns["update_v3_trade_replay"](
                                cur, row, prices[symbol]
                            )

                        else:
                            ns["update_v2_trade_current_price"](
                                cur, row, prices[symbol]
                            )

                        conn.commit()

                    except Exception as exc:
                        conn.rollback()
                        print(
                            "R2 UPDATE ERROR:",
                            tid,
                            symbol,
                            exc,
                            flush=True
                        )
        finally:
            conn.close()

    ns["update_simulated_trades"] = r2_update_trades
    ns["R2_ENABLED"] = True

    print(
        "V4.3R2 SIMULATION INSTALLED",
        "CORE ENTRY UNCHANGED",
        "LIVE EXECUTION DISABLED",
        flush=True,
    )
