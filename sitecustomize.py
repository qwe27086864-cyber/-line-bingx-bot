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
    _app.SCANNER_MAX_OPEN = 3
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
        "R1M PATCH ACTIVE | paper only | engine=R1M | max_open=3 | "
        "entry=best ask/bid | fees+slippage retained",
        flush=True,
    )


# ======================================================================
# SHADOW TRACKING V2 - R2 / R2F
# No strategy function is wrapped. Existing decision rows are copied only
# after r2_job.py / r2_fast_job.py has completely finished.
# ======================================================================
try:
    import atexit as _shadow_atexit
    import os as _shadow_os
    from datetime import datetime as _shadow_datetime
    from datetime import timedelta as _shadow_timedelta
    from datetime import timezone as _shadow_timezone

    _SHADOW_PROCESS_START = _shadow_datetime.now(_shadow_timezone.utc)

    def _r2_shadow_postrun():
        script = _shadow_os.path.basename(
            str(__import__("sys").argv[0] or "")
        ).lower()

        if script == "r2_job.py":
            engine = "R2"
        elif script == "r2_fast_job.py":
            engine = "R2F"
        else:
            return

        try:
            import shadow_tracker as _shadow

            if not _shadow.init_shadow_database():
                return

            # Look back 90 minutes. source_key UNIQUE prevents duplicates and
            # also recovers rows if an earlier post-run import was interrupted.
            since = (
                _SHADOW_PROCESS_START
                - _shadow_timedelta(minutes=90)
            )

            inserted = _shadow.import_existing_decisions(
                engine,
                since,
            )
            updated = _shadow.update_shadow_results(engine)

            print(
                f"{engine} SHADOW POSTRUN:",
                f"inserted={inserted}",
                f"updated={updated}",
                flush=True,
            )

        except Exception as _shadow_e:
            print(
                "R2 SHADOW POSTRUN ERROR:",
                engine,
                str(_shadow_e),
                flush=True,
            )

    _shadow_atexit.register(_r2_shadow_postrun)

except Exception as _shadow_setup_e:
    print(
        "R2 SHADOW SETUP ERROR:",
        str(_shadow_setup_e),
        flush=True,
    )
