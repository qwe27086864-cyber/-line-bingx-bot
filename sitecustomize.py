"""
R1M live-like paper simulation patch for the existing V4.2R1 scanner.

Purpose
-------
Run the SAME R1 signal/exit core in paper mode, but make entries closer to
what a real taker order can get:
  * fresh BingX best ask for LONG / best bid for SHORT
  * keeps the existing simulated fee + slippage cost model
  * 10 USDT margin, 8x leverage
  * max 3 concurrent positions (matching the small live test)
  * same TP 2/4/8%, SL 2.5%, 35/40/25 exits, cooldown/risk rules
  * stores entry diagnostics in r1m_entry_diagnostics

This file is auto-imported by Python before scanner_job.py. It patches app.py
in memory only; it does NOT place real orders.
"""

import json
import time

try:
    import app as _app
except Exception as _e:
    print("R1M PATCH LOAD ERROR:", str(_e), flush=True)
else:
    # ------------------------------------------------------------------
    # Isolate this run from the historical V4.2R1 paper data.
    # ------------------------------------------------------------------
    _app.SCANNER_ENGINE_VERSION = "R1M"
    _app.SCANNER_MAX_OPEN = 30
    _app.SCANNER_MAX_NEW_PER_SCAN = 3

    # Explicitly force paper-only behavior for this simulation service.
    # (The backup paper service does not call the R1 live execution layer.)
    _app.R1_LIVE_TRADING = False

    # Keep the existing conservative transaction-cost model:
    # fee 0.05%/side + extra slippage 0.03%/side, while the entry itself
    # uses the executable BBO side (ask for LONG, bid for SHORT).
    _app.SIM_FEE_PCT = float(getattr(_app, "SIM_FEE_PCT", 0.05))
    _app.SIM_SLIPPAGE_PCT = float(getattr(_app, "SIM_SLIPPAGE_PCT", 0.03))

    _ORIGINAL_CREATE = _app.create_simulated_trade

    def _ticker_for_symbol(symbol):
        """Return (bid, ask, last) from the freshest public BingX ticker."""
        result = _app.bingx_public_request(
            "/openApi/swap/v2/quote/ticker",
            {"symbol": symbol},
        )
        if result.get("code") != 0:
            raise RuntimeError(f"R1M ticker failed: {result}")

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
        return bid, ask, last

    def _ensure_diag_table(cur):
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS r1m_entry_diagnostics (
                trade_id INTEGER PRIMARY KEY,
                engine_version VARCHAR(20) NOT NULL DEFAULT 'R1M',
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL,
                signal_price DOUBLE PRECISION,
                last_price DOUBLE PRECISION,
                bid_price DOUBLE PRECISION,
                ask_price DOUBLE PRECISION,
                market_entry_price DOUBLE PRECISION NOT NULL,
                spread_pct DOUBLE PRECISION,
                signal_to_entry_pct DOUBLE PRECISION,
                entry_mode VARCHAR(30) NOT NULL DEFAULT 'MARKET_BBO_SIM',
                fee_pct_per_side DOUBLE PRECISION,
                slippage_pct_per_side DOUBLE PRECISION,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

    def _record_diag(
        trade_id, symbol, side, signal_price, last_price,
        bid, ask, entry_price, spread_pct, signal_to_entry_pct,
    ):
        conn = _app.get_db_connection()
        cur = conn.cursor()
        try:
            _ensure_diag_table(cur)
            cur.execute(
                """
                INSERT INTO r1m_entry_diagnostics (
                    trade_id, symbol, side,
                    signal_price, last_price, bid_price, ask_price,
                    market_entry_price, spread_pct, signal_to_entry_pct,
                    fee_pct_per_side, slippage_pct_per_side
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (trade_id) DO NOTHING;
                """,
                (
                    trade_id, symbol, side,
                    signal_price, last_price, bid, ask,
                    entry_price, spread_pct, signal_to_entry_pct,
                    _app.SIM_FEE_PCT, _app.SIM_SLIPPAGE_PCT,
                ),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def r1m_create_simulated_trade(result):
        """
        Keep R1 selection logic unchanged, but replace its idealized entry
        with the current executable BBO side.
        """
        patched = dict(result)
        symbol = str(patched["symbol"]).upper()
        side = str(patched.get("side", "LONG")).upper()
        signal_price = float(patched.get("price") or 0)

        bid, ask, last = _ticker_for_symbol(symbol)

        if side == "LONG":
            entry_price = ask if ask > 0 else (last if last > 0 else bid)
        else:
            entry_price = bid if bid > 0 else (last if last > 0 else ask)

        if entry_price <= 0:
            raise RuntimeError(
                f"R1M no executable market price: {symbol} "
                f"bid={bid} ask={ask} last={last}"
            )

        spread_pct = None
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2.0
            if mid > 0:
                spread_pct = (ask - bid) / mid * 100.0

        signal_to_entry_pct = None
        if signal_price > 0:
            if side == "LONG":
                signal_to_entry_pct = (entry_price - signal_price) / signal_price * 100.0
            else:
                signal_to_entry_pct = (signal_price - entry_price) / signal_price * 100.0

        patched["price"] = entry_price

        trade_id = _ORIGINAL_CREATE(patched)
        _record_diag(
            trade_id, symbol, side, signal_price, last,
            bid, ask, entry_price, spread_pct, signal_to_entry_pct,
        )

        print(
            "R1M MARKET ENTRY:", trade_id, symbol, side,
            f"signal={signal_price}",
            f"bid={bid}", f"ask={ask}", f"last={last}",
            f"entry={entry_price}",
            f"spread={spread_pct if spread_pct is not None else 'NA'}%",
            f"signal_to_entry={signal_to_entry_pct if signal_to_entry_pct is not None else 'NA'}%",
            flush=True,
        )
        return trade_id

    def r1m_update_simulated_trades():
        """
        Update ONLY R1M rows so old V4.2R1 paper positions/history are not
        changed by this new experiment.
        """
        prices = _app.get_all_prices()
        conn = _app.get_db_connection()
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
                WHERE status = 'OPEN'
                  AND engine_version = 'R1M'
                  AND strategy IN ('TREND', 'BREAKOUT')
                ORDER BY id ASC;
                """
            )
            rows = cur.fetchall()

            for row in rows:
                trade_id = row[0]
                symbol = row[1]
                if symbol not in prices:
                    continue
                try:
                    _app.update_v42_trade_replay(cur, row, prices[symbol])
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    print("R1M UPDATE ERROR:", trade_id, symbol, str(exc), flush=True)
        finally:
            cur.close()
            conn.close()

    # Patch BEFORE scanner_job.py imports these symbols from app.
    _app.create_simulated_trade = r1m_create_simulated_trade
    _app.update_simulated_trades = r1m_update_simulated_trades

    print(
        "R1M PATCH ACTIVE | paper only | engine=R1M | max_open=30 | "
        "entry=best ask/bid | fees+slippage retained",
        flush=True,
    )


# ======================================================================
# SHADOW TRACKING V2 - R1M
# Diagnostic-only.  It buffers decisions in RAM and writes only at exit.
# ======================================================================
try:
    import atexit as _shadow_atexit
    import os as _shadow_os
    from datetime import datetime as _shadow_datetime
    from datetime import timedelta as _shadow_timedelta
    from datetime import timezone as _shadow_timezone

    _R1M_SHADOW_PROCESS_START = _shadow_datetime.now(_shadow_timezone.utc)
    _R1M_SHADOW_BUFFER = []
    _R1M_SHADOW_CHOSEN = {}
    _R1M_SHADOW_SEQ = 0

    _R1M_ORIGINAL_CHOOSE = _app.choose_strategy
    _R1M_ORIGINAL_CAN_OPEN = _app.scanner_can_open

    def _r1m_shadow_key():
        global _R1M_SHADOW_SEQ
        _R1M_SHADOW_SEQ += 1
        stamp = _R1M_SHADOW_PROCESS_START.isoformat()
        return f"R1M:{stamp}:{_R1M_SHADOW_SEQ}"

    def _r1m_shadow_metadata(result):
        return {
            "strategy": result.get("strategy"),
            "score": result.get("score"),
            "trend_score": result.get("trend_score"),
            "breakout_score": result.get("breakout_score"),
            "trend_blocked": result.get("trend_blocked"),
            "breakout_blocked": result.get("breakout_blocked"),
            "trend_15m": result.get("trend_15m"),
            "trend_1h": result.get("trend_1h"),
            "radar_move_pct": result.get("radar_move"),
            "radar_score": result.get("radar_score"),
            "rsi": result.get("rsi"),
            "rsi_5m": result.get("rsi_5m"),
            "volume_ratio": result.get("volume_ratio"),
            "volume_ratio_5m": result.get("volume_ratio_5m"),
            "momentum_5m": result.get("momentum_5m"),
            "entry_mode": "ANALYSIS_REFERENCE_NO_EXTRA_API",
        }

    def r1m_choose_strategy_shadow(result):
        chosen = _R1M_ORIGINAL_CHOOSE(result)

        try:
            snapshot = dict(result)
            symbol = str(snapshot.get("symbol") or "").upper()

            if chosen is None:
                price = float(snapshot.get("price") or 0)
                if symbol and price > 0:
                    _R1M_SHADOW_BUFFER.append(
                        {
                            "source_key": _r1m_shadow_key(),
                            "engine_version": "R1M",
                            "symbol": symbol,
                            "side": snapshot.get("side", "LONG"),
                            "decision": "CORE_REJECT",
                            "reason": "choose_strategy rejected",
                            "entry_price": price,
                            "signal_price": price,
                            "observed_at": _shadow_datetime.now(
                                _shadow_timezone.utc
                            ),
                            "metadata": _r1m_shadow_metadata(snapshot),
                        }
                    )
            else:
                _R1M_SHADOW_CHOSEN[symbol] = dict(chosen)

        except Exception as _shadow_e:
            print(
                "R1M SHADOW BUFFER ERROR:",
                str(_shadow_e),
                flush=True,
            )

        return chosen

    def r1m_scanner_can_open_shadow(symbol):
        allowed = _R1M_ORIGINAL_CAN_OPEN(symbol)

        if not allowed:
            try:
                key = str(symbol).upper()
                snapshot = _R1M_SHADOW_CHOSEN.get(key)
                if snapshot:
                    price = float(snapshot.get("price") or 0)
                    if price > 0:
                        _R1M_SHADOW_BUFFER.append(
                            {
                                "source_key": _r1m_shadow_key(),
                                "engine_version": "R1M",
                                "symbol": key,
                                "side": snapshot.get("side", "LONG"),
                                "decision": "COOLDOWN",
                                "reason": "symbol cooldown/open",
                                "entry_price": price,
                                "signal_price": price,
                                "observed_at": _shadow_datetime.now(
                                    _shadow_timezone.utc
                                ),
                                "metadata": _r1m_shadow_metadata(snapshot),
                            }
                        )
            except Exception as _shadow_e:
                print(
                    "R1M SHADOW COOLDOWN BUFFER ERROR:",
                    str(_shadow_e),
                    flush=True,
                )

        return allowed

    _app.choose_strategy = r1m_choose_strategy_shadow
    _app.scanner_can_open = r1m_scanner_can_open_shadow

    def _r1m_shadow_flush():
        script = _shadow_os.path.basename(
            str(__import__("sys").argv[0] or "")
        ).lower()

        if script != "scanner_job.py":
            return

        try:
            import shadow_tracker as _shadow

            if not _shadow.init_shadow_database():
                return

            inserted = _shadow.record_shadow_batch(
                _R1M_SHADOW_BUFFER
            )
            updated = _shadow.update_shadow_results("R1M")

            print(
                "R1M SHADOW POSTRUN:",
                f"buffered={len(_R1M_SHADOW_BUFFER)}",
                f"inserted={inserted}",
                f"updated={updated}",
                flush=True,
            )

        except Exception as _shadow_e:
            print(
                "R1M SHADOW POSTRUN ERROR:",
                str(_shadow_e),
                flush=True,
            )

    _shadow_atexit.register(_r1m_shadow_flush)

except Exception as _shadow_setup_e:
    print(
        "R1M SHADOW SETUP ERROR:",
        str(_shadow_setup_e),
        flush=True,
    )
