"""
MEME2 monitor RPC failover + forward-market validation wrapper.

PAPER ONLY.
- Keeps meme2_wallet_copy_v2.py strategy logic.
- Uses RPC failover.
- Adds a 20-second forward quote record for selected validation wallets.
- Never signs or sends a transaction.
"""

import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

import meme2_wallet_copy_v2 as v3


RPC_TIMEOUT_SEC = float(os.environ.get("MEME2_RPC_TIMEOUT_SEC", "6"))
RPC_FAILOVER_ROUNDS = max(
    1, int(os.environ.get("MEME2_RPC_FAILOVER_ROUNDS", "1"))
)
RPC_FAILOVER_PAUSE_SEC = float(
    os.environ.get("MEME2_RPC_FAILOVER_PAUSE_SEC", "1.0")
)


def build_rpc_endpoints():
    endpoints = []

    helius_key = os.environ.get("HELIUS_API_KEY", "").strip()
    if helius_key:
        endpoints.append(
            f"https://mainnet.helius-rpc.com/?api-key={helius_key}"
        )

    primary = (
        os.environ.get("MEME2_RPC_HTTP", "").strip()
        or str(v3.core.SOLANA_RPC or "").strip()
    )
    if primary:
        endpoints.append(primary)

    fallback = os.environ.get(
        "MEME2_RPC_FALLBACK_HTTP",
        "https://api.mainnet-beta.solana.com",
    ).strip()
    if fallback:
        endpoints.append(fallback)

    return list(dict.fromkeys(x for x in endpoints if x))


RPC_ENDPOINTS = build_rpc_endpoints()


def failover_rpc(method, params):
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        }
    ).encode("utf-8")

    if not RPC_ENDPOINTS:
        raise RuntimeError("MEME2 RPC endpoints are not configured")

    last_exc = None

    for round_no in range(RPC_FAILOVER_ROUNDS):
        for endpoint_no, endpoint in enumerate(RPC_ENDPOINTS, start=1):
            v3._wait_slot("rpc")

            req = urllib.request.Request(
                endpoint,
                data=body,
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "User-Agent": v3.core.USER_AGENT,
                },
            )

            try:
                with urllib.request.urlopen(
                    req,
                    timeout=RPC_TIMEOUT_SEC,
                ) as resp:
                    payload = json.loads(
                        resp.read().decode("utf-8")
                    )

                if payload.get("error"):
                    raise RuntimeError(
                        f"RPC {method} returned an RPC error"
                    )

                if endpoint_no > 1:
                    print(
                        "MEME2 RPC failover success "
                        f"method={method} "
                        f"endpoint={endpoint_no}/{len(RPC_ENDPOINTS)}",
                        flush=True,
                    )

                return payload.get("result")

            except urllib.error.HTTPError as exc:
                last_exc = exc
                print(
                    "MEME2 RPC endpoint failed "
                    f"method={method} "
                    f"endpoint={endpoint_no}/{len(RPC_ENDPOINTS)} "
                    f"type=HTTP{exc.code}",
                    flush=True,
                )

            except Exception as exc:
                last_exc = exc
                print(
                    "MEME2 RPC endpoint failed "
                    f"method={method} "
                    f"endpoint={endpoint_no}/{len(RPC_ENDPOINTS)} "
                    f"type={type(exc).__name__}",
                    flush=True,
                )

        if round_no + 1 < RPC_FAILOVER_ROUNDS:
            time.sleep(max(0.0, RPC_FAILOVER_PAUSE_SEC))

    print(
        "MEME2 RPC exhausted failover "
        f"method={method} "
        f"endpoints={len(RPC_ENDPOINTS)} "
        f"rounds={RPC_FAILOVER_ROUNDS}",
        flush=True,
    )

    raise RuntimeError(
        "RPC failed after failover: "
        f"{type(last_exc).__name__ if last_exc else 'unknown'}"
    )


v3.core.rpc = failover_rpc


# ---------------------------------------------------------------------------
# PAPER-ONLY forward validation
# ---------------------------------------------------------------------------

VALIDATION_ENABLED = os.environ.get(
    "MEME2_VALIDATION_ENABLED", "true"
).lower() == "true"

VALIDATION_DELAY_SEC = float(
    os.environ.get("MEME2_VALIDATION_DELAY_SEC", "20")
)

_DEFAULT_VALIDATION_WALLETS = (
    "3kxcF8wHKm4sEtnxjXXeUvNGeDjTmo47LgtSRg71YfG5,"
    "HzKH3s8Kf7f7pgvJZx2V6TzhkdHbZDurG53rXj61BVLR,"
    "An9sREpLnAXVi4KMaTGuGvgET51CyaukLUTMtxzmLYSB"
)

VALIDATION_WALLETS = {
    x.strip()
    for x in os.environ.get(
        "MEME2_VALIDATION_WALLETS",
        _DEFAULT_VALIDATION_WALLETS,
    ).split(",")
    if x.strip()
}

_strategy_process_change = v3.core.process_change
_validation_lock = threading.Lock()
_validation_pending = set()


def init_validation_table():
    if not VALIDATION_ENABLED:
        return

    conn = v3.core.db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS meme2_live_validation_quotes (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                wallet VARCHAR(80) NOT NULL,
                signature VARCHAR(120) NOT NULL,
                mint_address VARCHAR(80) NOT NULL,
                detection_delay_sec DOUBLE PRECISION,
                quote_delay_target_sec DOUBLE PRECISION NOT NULL,
                quote_delay_actual_sec DOUBLE PRECISION,
                detected_price_usd DOUBLE PRECISION,
                detected_liquidity_usd DOUBLE PRECISION,
                executable_entry_detected_usd DOUBLE PRECISION,
                quote_price_usd DOUBLE PRECISION,
                quote_liquidity_usd DOUBLE PRECISION,
                executable_entry_quote_usd DOUBLE PRECISION,
                raw_price_change_pct DOUBLE PRECISION,
                executable_price_change_pct DOUBLE PRECISION,
                detected_decision VARCHAR(40),
                detected_reason TEXT,
                market_data_ok BOOLEAN NOT NULL DEFAULT FALSE,
                meta JSONB,
                UNIQUE(
                    wallet,
                    signature,
                    mint_address,
                    quote_delay_target_sec
                )
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS
                idx_meme2_live_validation_wallet_time
            ON meme2_live_validation_quotes(wallet, created_at DESC);
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def read_detected_signal(wallet, signature, mint):
    conn = v3.core.db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT
                observed_at,
                detection_delay_sec,
                detected_price_usd,
                detected_liquidity_usd,
                decision,
                reason
            FROM meme2_signals
            WHERE wallet=%s
              AND signature=%s
              AND mint_address=%s
              AND side='BUY'
            ORDER BY id DESC
            LIMIT 1;
            """,
            (wallet, signature, mint),
        )
        row = cur.fetchone()

        if not row:
            return None

        return {
            "observed_at": row[0],
            "detection_delay_sec": (
                float(row[1]) if row[1] is not None else None
            ),
            "detected_price_usd": (
                float(row[2]) if row[2] is not None else None
            ),
            "detected_liquidity_usd": (
                float(row[3]) if row[3] is not None else None
            ),
            "decision": row[4],
            "reason": row[5],
        }
    finally:
        cur.close()
        conn.close()


def write_delayed_quote(wallet, signature, mint, signal):
    key = (wallet, signature, mint)

    try:
        scheduled_at = time.time()
        time.sleep(max(0.0, VALIDATION_DELAY_SEC))

        quote_started = time.time()
        market = v3.core.get_token_market(mint)
        quote_finished = time.time()

        detected_px = float(
            signal.get("detected_price_usd") or 0.0
        )
        detected_liq = float(
            signal.get("detected_liquidity_usd") or 0.0
        )
        quote_px = float(
            (market or {}).get("price") or 0.0
        )
        quote_liq = float(
            (market or {}).get("liq") or 0.0
        )

        detected_exec = None
        if detected_px > 0 and detected_liq > 0:
            detected_exec, _ = v3.core.executable_entry(
                detected_px,
                v3.core.NOTIONAL_USDT,
                detected_liq,
            )

        quote_exec = None
        if quote_px > 0 and quote_liq > 0:
            quote_exec, _ = v3.core.executable_entry(
                quote_px,
                v3.core.NOTIONAL_USDT,
                quote_liq,
            )

        raw_move = None
        if detected_px > 0 and quote_px > 0:
            raw_move = (
                quote_px / detected_px - 1.0
            ) * 100.0

        exec_move = None
        if (
            detected_exec is not None
            and detected_exec > 0
            and quote_exec is not None
            and quote_exec > 0
        ):
            exec_move = (
                quote_exec / detected_exec - 1.0
            ) * 100.0

        observed_at = signal.get("observed_at")
        if observed_at is not None:
            actual_delay = max(
                0.0,
                quote_finished - observed_at.timestamp(),
            )
        else:
            actual_delay = max(
                0.0,
                quote_finished - scheduled_at,
            )

        conn = v3.core.db()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO meme2_live_validation_quotes(
                    wallet,
                    signature,
                    mint_address,
                    detection_delay_sec,
                    quote_delay_target_sec,
                    quote_delay_actual_sec,
                    detected_price_usd,
                    detected_liquidity_usd,
                    executable_entry_detected_usd,
                    quote_price_usd,
                    quote_liquidity_usd,
                    executable_entry_quote_usd,
                    raw_price_change_pct,
                    executable_price_change_pct,
                    detected_decision,
                    detected_reason,
                    market_data_ok,
                    meta
                )
                VALUES(
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,%s,%s,%s,%s,%s,%s,%s
                )
                ON CONFLICT(
                    wallet,
                    signature,
                    mint_address,
                    quote_delay_target_sec
                ) DO NOTHING;
                """,
                (
                    wallet,
                    signature,
                    mint,
                    signal.get("detection_delay_sec"),
                    VALIDATION_DELAY_SEC,
                    actual_delay,
                    detected_px if detected_px > 0 else None,
                    detected_liq if detected_liq > 0 else None,
                    detected_exec,
                    quote_px if quote_px > 0 else None,
                    quote_liq if quote_liq > 0 else None,
                    quote_exec,
                    raw_move,
                    exec_move,
                    signal.get("decision"),
                    signal.get("reason"),
                    bool(quote_px > 0 and quote_liq > 0),
                    json.dumps(
                        {
                            "paper_only": True,
                            "notional_usdt": v3.core.NOTIONAL_USDT,
                            "quote_request_sec": (
                                quote_finished - quote_started
                            ),
                        }
                    ),
                ),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

        print(
            "MEME2_VALIDATION_QUOTE "
            f"wallet={wallet[:8]} "
            f"actual_delay={actual_delay:.1f}s "
            f"raw_move={(raw_move or 0.0):+.2f}% "
            f"exec_move={(exec_move or 0.0):+.2f}% "
            f"liq_usd={quote_liq:.0f} "
            f"ok={bool(quote_px > 0 and quote_liq > 0)}",
            flush=True,
        )

    except Exception as exc:
        print(
            "MEME2_VALIDATION_ERROR "
            f"wallet={wallet[:8]} "
            f"type={type(exc).__name__}: {exc}",
            flush=True,
        )

    finally:
        with _validation_lock:
            _validation_pending.discard(key)


def validation_process_change(
    wallet,
    signature,
    block_time_unix,
    ch,
):
    # Existing strategy stays unchanged and remains PAPER ONLY.
    result = _strategy_process_change(
        wallet,
        signature,
        block_time_unix,
        ch,
    )

    if (
        VALIDATION_ENABLED
        and wallet in VALIDATION_WALLETS
        and ch.get("side") == "BUY"
    ):
        mint = ch.get("mint")
        signal = read_detected_signal(
            wallet,
            signature,
            mint,
        )

        if (
            signal
            and float(signal.get("detected_price_usd") or 0.0) > 0
        ):
            key = (wallet, signature, mint)

            with _validation_lock:
                if key not in _validation_pending:
                    _validation_pending.add(key)
                    threading.Thread(
                        target=write_delayed_quote,
                        args=(
                            wallet,
                            signature,
                            mint,
                            signal,
                        ),
                        daemon=True,
                    ).start()

    return result


v3.core.process_change = validation_process_change


def main():
    init_validation_table()

    print(
        "MEME2_LIVE_VALIDATION_START "
        f"enabled={VALIDATION_ENABLED} "
        f"wallets={len(VALIDATION_WALLETS)} "
        f"delay={VALIDATION_DELAY_SEC:.0f}s "
        "paper_only=true",
        flush=True,
    )

    print(
        "MEME2_RPC_FAILOVER_PATCH "
        f"endpoints={len(RPC_ENDPOINTS)} "
        f"timeout={RPC_TIMEOUT_SEC:.1f}s "
        f"rounds={RPC_FAILOVER_ROUNDS} "
        "strategy_unchanged=true",
        flush=True,
    )

    mode = os.environ.get("MEME2_MODE", "monitor").strip().lower()

    if "--monitor" in sys.argv or mode == "monitor":
        v3.monitor_loop()
    else:
        v3.run_once()


if __name__ == "__main__":
    main()
