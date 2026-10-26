"""MEME2 V3 shadow-tracking patch.

Loaded through PYTHONPATH when meme2_wallet_copy_v2.py starts.
It does NOT open paper/live positions. It only turns eligible MAX_OPEN BUYs into
measured shadow candidates and tracks executable outcomes for 1/3/5/10 minutes.
"""

import hashlib
import json
import os
import sys
import threading
import time
from datetime import datetime, timezone

TARGET_SCRIPT = "meme2_wallet_copy_v2.py"
SHADOW_ENGINE = "MEME2_V3_SHADOW"
SHADOW_REFRESH_SEC = float(os.environ.get("MEME2_SHADOW_REFRESH_SEC", "30"))
SHADOW_MAX_PENDING_SEC = float(os.environ.get("MEME2_SHADOW_MAX_PENDING_SEC", "900"))
SHADOW_ENABLED = os.environ.get("MEME2_SHADOW_TRACKING_ENABLED", "true").lower() == "true"

_CORE = None
_MAIN = None


def _source_key(wallet, signature, mint):
    raw = f"{wallet}|{signature}|{mint}|BUY".encode("utf-8")
    return "MEME2:" + hashlib.sha1(raw).hexdigest()


def _init_shadow_table(core):
    conn = core.db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS shadow_rejections (
                id BIGSERIAL PRIMARY KEY,
                source_key VARCHAR(160) UNIQUE NOT NULL,
                engine_version VARCHAR(20) NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL,
                decision VARCHAR(80) NOT NULL,
                reason TEXT,
                entry_price DOUBLE PRECISION,
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
            "CREATE INDEX IF NOT EXISTS idx_shadow_pending "
            "ON shadow_rejections(engine_version, completed_at, observed_at);"
        )
        cur.execute(
            "CREATE INDEX IF NOT EXISTS idx_shadow_reason "
            "ON shadow_rejections(engine_version, decision, observed_at);"
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _exec_return_pct(core, raw_price, token_qty, notional, liquidity_usd):
    raw_price = core._safe_float(raw_price)
    token_qty = core._safe_float(token_qty)
    notional = core._safe_float(notional)
    liquidity_usd = core._safe_float(liquidity_usd)
    if raw_price <= 0 or token_qty <= 0 or notional <= 0 or liquidity_usd <= 0:
        return None
    exit_price, _ = core.executable_exit(raw_price, token_qty, liquidity_usd)
    gross_value = token_qty * exit_price
    exit_fee = gross_value * core.FEE_BPS_PER_SIDE / 10000.0
    proceeds = gross_value - exit_fee
    return (proceeds - notional) / notional * 100.0


def _record_candidate(core, wallet, signature, mint, delay, market):
    raw = core._safe_float((market or {}).get("price"))
    liq = core._safe_float((market or {}).get("liq"))
    if raw <= 0 or liq <= 0:
        return None

    notional = float(core.NOTIONAL_USDT)
    entry_price, entry_bps = core.executable_entry(raw, notional, liq)
    entry_fee = notional * core.FEE_BPS_PER_SIDE / 10000.0
    token_qty = max(0.0, notional - entry_fee) / entry_price if entry_price > 0 else 0.0
    initial_ret = _exec_return_pct(core, raw, token_qty, notional, liq)

    meta = {
        "wallet": wallet,
        "signature": signature,
        "mint": mint,
        "detection_delay_sec": float(delay or 0.0),
        "raw_entry_price": raw,
        "entry_price": entry_price,
        "entry_liquidity_usd": liq,
        "entry_execution_bps": entry_bps,
        "notional_usdt": notional,
        "token_qty": token_qty,
        "fee_bps_per_side": float(core.FEE_BPS_PER_SIDE),
        "exit_slippage_bps": float(core.EXIT_SLIPPAGE_BPS),
        "tp_pct": float(core.TAKE_PROFIT_PCT),
        "sl_pct": float(core.STOP_LOSS_PCT),
        "horizons_sec": [60, 180, 300, 600],
        "return_model": "executable_net_roi_with_entry_liquidity",
        "pool_address": (market or {}).get("pool_address"),
        "pool_name": (market or {}).get("pool_name"),
        "raw_mfe_pct": 0.0,
        "raw_mae_pct": 0.0,
    }

    conn = core.db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO shadow_rejections(
                source_key,engine_version,symbol,side,decision,reason,
                entry_price,signal_price,metadata,mfe_10m_pct,mae_10m_pct,
                tp1_hit_10m,sl_hit_10m
            )
            VALUES(%s,%s,%s,'LONG','WATCH','MAX_OPEN_SHADOW',%s,%s,%s,%s,%s,FALSE,FALSE)
            ON CONFLICT(source_key) DO NOTHING
            RETURNING id;
            """,
            (
                _source_key(wallet, signature, mint),
                SHADOW_ENGINE,
                mint,
                entry_price,
                raw,
                json.dumps(meta),
                initial_ret,
                initial_ret,
            ),
        )
        row = cur.fetchone()
        conn.commit()
        return int(row[0]) if row else None
    finally:
        cur.close()
        conn.close()


def _refresh_candidates(core):
    conn = core.db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id,symbol,signal_price,observed_at,
                   return_1m_pct,return_3m_pct,return_5m_pct,return_10m_pct,
                   mfe_10m_pct,mae_10m_pct,tp1_hit_10m,sl_hit_10m,
                   first_barrier_10m,metadata
            FROM shadow_rejections
            WHERE engine_version=%s AND completed_at IS NULL
            ORDER BY observed_at ASC
            LIMIT 200;
            """,
            (SHADOW_ENGINE,),
        )
        rows = cur.fetchall()
        if not rows:
            return

        prices = core.get_prices([r[1] for r in rows])
        now_dt = datetime.now(timezone.utc)

        for row in rows:
            (
                sid, mint, signal_price, observed_at,
                r1, r3, r5, r10, mfe, mae, tp_hit, sl_hit,
                first_barrier, meta,
            ) = row
            meta = dict(meta or {})
            age_sec = max(0.0, (now_dt - observed_at).total_seconds())
            raw_price = core._safe_float(prices.get(mint))

            if raw_price <= 0:
                if age_sec >= SHADOW_MAX_PENDING_SEC:
                    meta["shadow_incomplete"] = "NO_PRICE_BY_TIMEOUT"
                    meta["completed_age_sec"] = age_sec
                    cur.execute(
                        "UPDATE shadow_rejections SET metadata=%s::jsonb,completed_at=NOW() WHERE id=%s;",
                        (json.dumps(meta), sid),
                    )
                continue

            token_qty = core._safe_float(meta.get("token_qty"))
            notional = core._safe_float(meta.get("notional_usdt"))
            entry_liq = core._safe_float(meta.get("entry_liquidity_usd"))
            net_ret = _exec_return_pct(core, raw_price, token_qty, notional, entry_liq)
            if net_ret is None:
                continue

            raw_entry = core._safe_float(meta.get("raw_entry_price"), core._safe_float(signal_price))
            raw_ret = ((raw_price / raw_entry) - 1.0) * 100.0 if raw_entry > 0 else None

            new_mfe = net_ret if mfe is None else max(float(mfe), net_ret)
            new_mae = net_ret if mae is None else min(float(mae), net_ret)
            if raw_ret is not None:
                meta["raw_mfe_pct"] = max(float(meta.get("raw_mfe_pct", 0.0)), raw_ret)
                meta["raw_mae_pct"] = min(float(meta.get("raw_mae_pct", 0.0)), raw_ret)

            tp = bool(tp_hit)
            sl = bool(sl_hit)
            barrier = first_barrier
            if barrier is None:
                if net_ret >= float(core.TAKE_PROFIT_PCT):
                    tp = True
                    barrier = "TP"
                elif net_ret <= -float(core.STOP_LOSS_PCT):
                    sl = True
                    barrier = "SL"
            else:
                tp = tp or net_ret >= float(core.TAKE_PROFIT_PCT)
                sl = sl or net_ret <= -float(core.STOP_LOSS_PCT)

            nr1, nr3, nr5, nr10 = r1, r3, r5, r10
            samples = dict(meta.get("horizon_samples") or {})
            horizons = ((60, "1m"), (180, "3m"), (300, "5m"), (600, "10m"))
            for secs, name in horizons:
                current = {"1m": nr1, "3m": nr3, "5m": nr5, "10m": nr10}[name]
                if age_sec >= secs and current is None:
                    if name == "1m":
                        nr1 = net_ret
                    elif name == "3m":
                        nr3 = net_ret
                    elif name == "5m":
                        nr5 = net_ret
                    else:
                        nr10 = net_ret
                    samples[name] = {
                        "age_sec": age_sec,
                        "raw_price_usd": raw_price,
                        "raw_return_pct": raw_ret,
                        "executable_net_return_pct": net_ret,
                    }

            meta["horizon_samples"] = samples
            meta["last_shadow_sample_at"] = now_dt.isoformat()
            meta["last_shadow_age_sec"] = age_sec
            meta["last_raw_price_usd"] = raw_price
            meta["last_raw_return_pct"] = raw_ret
            meta["last_executable_net_return_pct"] = net_ret

            complete = age_sec >= 600 and nr10 is not None
            cur.execute(
                """
                UPDATE shadow_rejections
                SET return_1m_pct=%s,return_3m_pct=%s,return_5m_pct=%s,return_10m_pct=%s,
                    mfe_10m_pct=%s,mae_10m_pct=%s,tp1_hit_10m=%s,sl_hit_10m=%s,
                    first_barrier_10m=%s,metadata=%s::jsonb,
                    completed_at=CASE WHEN %s THEN NOW() ELSE completed_at END
                WHERE id=%s;
                """,
                (
                    nr1, nr3, nr5, nr10, new_mfe, new_mae, tp, sl, barrier,
                    json.dumps(meta), complete, sid,
                ),
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _shadow_refresh_loop(core):
    while True:
        try:
            _refresh_candidates(core)
        except Exception as exc:
            print(f"MEME2 SHADOW refresh error: {type(exc).__name__}: {exc}", flush=True)
        time.sleep(max(10.0, SHADOW_REFRESH_SEC))


def _install(main):
    global _CORE, _MAIN
    core = main.core
    _CORE = core
    _MAIN = main

    _init_shadow_table(core)
    previous = core.process_change

    def shadow_process_change(wallet, signature, block_time_unix, ch):
        if ch.get("side") != "BUY":
            return previous(wallet, signature, block_time_unix, ch)

        # Preserve V3 wallet gate. Rejected wallets stay rejected and are not
        # counted as eligible shadow entries.
        try:
            allowed, _, _ = main.wallet_entry_allowed(wallet)
        except Exception:
            return previous(wallet, signature, block_time_unix, ch)
        if not allowed:
            return previous(wallet, signature, block_time_unix, ch)

        # Only replace the capacity stop. If capacity is ever re-enabled, the
        # normal V3 paper logic resumes automatically.
        if len(core.get_open_positions()) < core.MAX_OPEN:
            return previous(wallet, signature, block_time_unix, ch)

        now = time.time()
        delay = max(0.0, now - float(block_time_unix or now))
        block_dt = (
            datetime.fromtimestamp(block_time_unix, tz=timezone.utc)
            if block_time_unix else None
        )

        # Preserve the same pre-entry gates as the core.
        if core.REQUIRE_SOL_FLOW and ch["sol_delta"] > -core.MIN_SOL_FLOW:
            return previous(wallet, signature, block_time_unix, ch)
        if delay > core.MAX_SIGNAL_AGE_SEC:
            return previous(wallet, signature, block_time_unix, ch)
        if core.find_open(wallet, ch["mint"]):
            return previous(wallet, signature, block_time_unix, ch)

        market = core.get_token_market_cached(ch["mint"], force=True)
        if not market or core._safe_float(market.get("price")) <= 0:
            core.log_signal(
                wallet, signature, block_dt, delay, ch, None, None,
                "WATCH", "MAX_OPEN",
                {"shadow_tracking": True, "shadow_eligible": False, "shadow_reason": "NO_MARKET_PRICE"},
            )
            return

        if core._safe_float(market.get("liq")) < core.MIN_LIQUIDITY_USD:
            core.log_signal(
                wallet, signature, block_dt, delay, ch, market["price"], market.get("liq"),
                "WATCH", "MAX_OPEN",
                {"shadow_tracking": True, "shadow_eligible": False, "shadow_reason": "LOW_LIQUIDITY"},
            )
            return

        shadow_id = _record_candidate(core, wallet, signature, ch["mint"], delay, market)
        core.log_signal(
            wallet, signature, block_dt, delay, ch, market["price"], market.get("liq"),
            "WATCH", "MAX_OPEN",
            {
                "shadow_tracking": True,
                "shadow_eligible": True,
                "shadow_id": shadow_id,
                "shadow_engine": SHADOW_ENGINE,
            },
        )
        print(
            f"MEME2 SHADOW BUY wallet={wallet[:8]}.. mint={ch['mint'][:8]}.. "
            f"px={market['price']:.10g} liq={core._safe_float(market.get('liq')):.0f}",
            flush=True,
        )

    core.process_change = shadow_process_change
    threading.Thread(target=_shadow_refresh_loop, args=(core,), daemon=True).start()
    print(
        "MEME2 SHADOW PATCH active | no positions opened | horizons=1m,3m,5m,10m | MFE/MAE + TP/SL",
        flush=True,
    )


def _install_when_ready():
    deadline = time.time() + 60.0
    while time.time() < deadline:
        main = sys.modules.get("__main__")
        if main is not None:
            core = getattr(main, "core", None)
            gated = getattr(main, "gated_process_change", None)
            if (
                core is not None
                and gated is not None
                and hasattr(main, "wallet_entry_allowed")
                and getattr(core, "process_change", None) is gated
            ):
                try:
                    _install(main)
                except Exception as exc:
                    print(f"MEME2 SHADOW install error: {type(exc).__name__}: {exc}", flush=True)
                return
        time.sleep(0.02)


if SHADOW_ENABLED and os.path.basename(sys.argv[0]) == TARGET_SCRIPT:
    threading.Thread(target=_install_when_ready, daemon=True).start()
