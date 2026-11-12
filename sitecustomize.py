"""
M2R3 runtime hotfix + persistent state.

Upload this file as `sitecustomize.py` to the ROOT of the
`meme2-development-branch` branch.

Required env vars on M2R3:
    M2R3_PRICE_SOURCE_FIX=1
    M2R3_PERSIST=1
    PYTHONPATH=.

Optional:
    M2R3_STATE_KEY=m2r3_hotfix_20261112

Behavior:
1) Entry/confirm/fallback market reads use the same Dexscreener
   highest-liquidity-pool selection rule used by M2R3 live monitoring.
2) M2R3 in-memory state is saved to the existing Render Postgres database.
3) On restart/redeploy, that state is restored before the main loop runs.

PAPER-TRADING INFRASTRUCTURE FIX ONLY.
"""

import importlib.util
import io
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request

_FIX_ENABLED = os.getenv("M2R3_PRICE_SOURCE_FIX", "").strip() == "1"
_PERSIST_ENABLED = os.getenv("M2R3_PERSIST", "").strip() == "1"

# During Render's build phase, sitecustomize may run before requirements are
# installed. Skip cleanly there; runtime Python will load this file again.
if (_FIX_ENABLED or _PERSIST_ENABLED) and importlib.util.find_spec("psycopg2") is not None:
    import psycopg2
    import meme2_wallet_copy_job as _old

    _DEX_BASE = "https://api.dexscreener.com/tokens/v1/solana/"
    _FIX_MAX_CACHE_SEC = 15.0

    # ------------------------------------------------------------------
    # M2R3 wallet filter: blocks NEW source events only.
    # Existing open positions remain managed until they close.
    # ------------------------------------------------------------------
    _BLOCK_WALLET_PREFIXES = [
        x.strip()
        for x in os.getenv("M2R3_BLOCK_WALLETS", "").split(",")
        if x.strip()
    ]
    if _BLOCK_WALLET_PREFIXES:
        _ORIG_URLOPEN = urllib.request.urlopen

        def _m2r3_filtered_urlopen(req, *args, **kwargs):
            url = getattr(req, "full_url", str(req))
            if str(url).startswith("https://m2n-3tfx.onrender.com"):
                with _ORIG_URLOPEN(req, *args, **kwargs) as resp:
                    raw = resp.read()
                try:
                    data = json.loads(raw.decode("utf-8"))
                    events = data.get("recent_events")
                    if isinstance(events, list):
                        data["recent_events"] = [
                            e for e in events
                            if not any(
                                str((e or {}).get("wallet") or "").startswith(prefix)
                                for prefix in _BLOCK_WALLET_PREFIXES
                            )
                        ]
                    raw = json.dumps(data, separators=(",", ":")).encode("utf-8")
                except Exception:
                    pass
                return io.BytesIO(raw)
            return _ORIG_URLOPEN(req, *args, **kwargs)

        urllib.request.urlopen = _m2r3_filtered_urlopen
        print(
            "M2R3_WALLET_FILTER active block="
            + ",".join(_BLOCK_WALLET_PREFIXES),
            flush=True,
        )

    def _safe_float(v, default=0.0):
        try:
            if v is None or v == "":
                return default
            return float(v)
        except (TypeError, ValueError):
            return default

    # ------------------------------------------------------------------
    # Price-source consistency hotfix
    # ------------------------------------------------------------------
    if _FIX_ENABLED:
        def _dex_rows_for_mint(mint):
            url = _DEX_BASE + urllib.parse.quote(str(mint))
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": "M2R3-price-source-fix/2.0",
                    "Accept": "application/json",
                },
                method="GET",
            )
            last_exc = None
            retries = max(1, int(getattr(_old, "HTTP_RETRIES", 4) or 4))
            base_delay = max(
                0.5,
                float(getattr(_old, "HTTP_RETRY_BASE_SEC", 1.0) or 1.0),
            )

            for attempt in range(retries):
                try:
                    with urllib.request.urlopen(req, timeout=20) as resp:
                        return json.loads(resp.read().decode("utf-8")) or []
                except Exception as exc:
                    last_exc = exc
                    if attempt >= retries - 1:
                        break
                    time.sleep(min(3.0, base_delay * (2 ** attempt)))

            raise RuntimeError(f"Dexscreener request failed: {last_exc}")

        def _best_market(mint):
            try:
                rows = _dex_rows_for_mint(mint)
            except Exception:
                return None

            best = None
            for row in rows:
                row = row or {}
                base = (row.get("baseToken") or {}).get("address")
                quote = (row.get("quoteToken") or {}).get("address")

                if mint not in (base, quote):
                    continue

                price = _safe_float(row.get("priceUsd"))
                liq = _safe_float((row.get("liquidity") or {}).get("usd"))

                if price <= 0:
                    continue

                market = {
                    "price": price,
                    "liq": liq,
                    "pool_address": row.get("pairAddress"),
                    "pool_name": row.get("dexId"),
                    "liquidity_source": "dexscreener",
                }

                if best is None or market["liq"] > best["liq"]:
                    best = market

            return best

        def get_token_market(mint):
            return _best_market(mint)

        def get_token_market_cached(mint, force=False):
            now = time.time()
            cached = _old._MARKET_CACHE.get(mint)

            configured_ttl = _safe_float(
                getattr(_old, "MARKET_CACHE_TTL_SEC", _FIX_MAX_CACHE_SEC),
                _FIX_MAX_CACHE_SEC,
            )
            effective_ttl = min(max(configured_ttl, 0.0), _FIX_MAX_CACHE_SEC)

            if (
                cached
                and not force
                and now - cached.get("ts", 0) <= effective_ttl
            ):
                row = dict(cached["market"])
                row["liquidity_age_sec"] = max(0.0, now - cached["ts"])
                row["liquidity_source"] = "dex_cache"
                return row

            market = _best_market(mint)
            if market:
                _old._MARKET_CACHE[mint] = {
                    "ts": now,
                    "market": dict(market),
                }
                row = dict(market)
                row["liquidity_age_sec"] = 0.0
                row["liquidity_source"] = "dex_fresh"
                return row

            if cached:
                row = dict(cached["market"])
                row["liquidity_age_sec"] = max(
                    0.0, now - cached.get("ts", now)
                )
                row["liquidity_source"] = "dex_stale_cache"
                return row

            return None

        def get_prices(mints):
            out = {}
            uniq = list(dict.fromkeys([m for m in mints if m]))
            for mint in uniq:
                market = _best_market(mint)
                if market and market["price"] > 0:
                    out[mint] = market["price"]
            return out

        _old.get_token_market = get_token_market
        _old.get_token_market_cached = get_token_market_cached
        _old.get_prices = get_prices

        print(
            "M2R3_PRICE_SOURCE_FIX active "
            "source=DEXSCREENER pool=highest_liquidity cache_max_sec=15",
            flush=True,
        )

    # ------------------------------------------------------------------
    # Persistent state hotfix
    # ------------------------------------------------------------------
    if _PERSIST_ENABLED:
        _DB_URL = os.getenv("DATABASE_URL", "").strip()
        _STATE_KEY = os.getenv(
            "M2R3_STATE_KEY", "m2r3_hotfix_20261112"
        ).strip()
        _SAVE_EVERY_SEC = 20.0
        _restored_once = False
        _saver_started = False

        def _to_jsonable(v):
            if isinstance(v, set):
                return list(v)
            if isinstance(v, dict):
                return {str(k): _to_jsonable(x) for k, x in v.items()}
            if isinstance(v, (list, tuple)):
                return [_to_jsonable(x) for x in v]
            if isinstance(v, (str, int, float, bool)) or v is None:
                return v
            return str(v)

        def _restore_shape(payload):
            if not isinstance(payload, dict):
                return {}
            out = dict(payload)
            out["seen"] = set(out.get("seen") or [])
            for key, default in (
                ("pending", {}),
                ("pos", []),
                ("closed", []),
                ("errors", []),
                ("last_open", {}),
                ("price_cache", {}),
            ):
                if key not in out or out[key] is None:
                    out[key] = default.copy() if isinstance(default, dict) else list(default)
            out["events"] = int(out.get("events") or 0)
            out["scans"] = int(out.get("scans") or 0)
            out["price_cache_t"] = float(out.get("price_cache_t") or 0)
            return out

        def _db_connect():
            return psycopg2.connect(_DB_URL, sslmode="require")

        def _ensure_table(conn):
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS m2r3_runtime_state (
                        state_key TEXT PRIMARY KEY,
                        payload JSONB NOT NULL,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                    """
                )
            conn.commit()

        def _load_state():
            conn = _db_connect()
            try:
                _ensure_table(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT payload FROM m2r3_runtime_state WHERE state_key=%s;",
                        (_STATE_KEY,),
                    )
                    row = cur.fetchone()
                return row[0] if row else None
            finally:
                conn.close()

        def _save_state(state_obj):
            payload = _to_jsonable(state_obj)
            conn = _db_connect()
            try:
                _ensure_table(conn)
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO m2r3_runtime_state(state_key,payload,updated_at)
                        VALUES(%s,%s::jsonb,NOW())
                        ON CONFLICT(state_key) DO UPDATE SET
                            payload=EXCLUDED.payload,
                            updated_at=NOW();
                        """,
                        (_STATE_KEY, json.dumps(payload, separators=(",", ":"))),
                    )
                conn.commit()
            finally:
                conn.close()

        def _save_loop(state_obj):
            while True:
                time.sleep(_SAVE_EVERY_SEC)
                try:
                    _save_state(state_obj)
                    print(
                        "M2R3_STATE_SAVE "
                        + json.dumps(
                            {
                                "key": _STATE_KEY,
                                "scans": int(state_obj.get("scans") or 0),
                                "events": int(state_obj.get("events") or 0),
                                "pending": len(state_obj.get("pending") or {}),
                                "open": sum(
                                    1
                                    for x in (state_obj.get("pos") or [])
                                    if x.get("status") == "OPEN"
                                ),
                                "closed": len(state_obj.get("closed") or []),
                            },
                            separators=(",", ":"),
                        ),
                        flush=True,
                    )
                except Exception as exc:
                    print(
                        "M2R3_STATE_SAVE_ERROR "
                        + json.dumps(
                            {"error": f"{type(exc).__name__}:{str(exc)[:180]}"},
                            separators=(",", ":"),
                        ),
                        flush=True,
                    )

        if not _DB_URL:
            print(
                "M2R3_PERSIST disabled reason=DATABASE_URL_missing",
                flush=True,
            )
        else:
            def _main_trace(frame, event, arg):
                global _restored_once, _saver_started

                if _restored_once:
                    return None

                if frame.f_code.co_filename != "<stdin>":
                    return None

                if event != "line":
                    return _main_trace

                g = frame.f_globals
                state_obj = g.get("S")

                # Detect M2R3's state object immediately after it is created.
                required = {
                    "seen",
                    "pending",
                    "pos",
                    "closed",
                    "events",
                    "scans",
                    "errors",
                    "last_open",
                    "price_cache",
                    "price_cache_t",
                }
                if not isinstance(state_obj, dict) or not required.issubset(state_obj):
                    return _main_trace

                try:
                    saved = _load_state()
                    if saved:
                        restored = _restore_shape(saved)
                        state_obj.clear()
                        state_obj.update(restored)
                        print(
                            "M2R3_STATE_RESTORE "
                            + json.dumps(
                                {
                                    "key": _STATE_KEY,
                                    "scans": int(state_obj.get("scans") or 0),
                                    "events": int(state_obj.get("events") or 0),
                                    "pending": len(state_obj.get("pending") or {}),
                                    "open": sum(
                                        1
                                        for x in (state_obj.get("pos") or [])
                                        if x.get("status") == "OPEN"
                                    ),
                                    "closed": len(state_obj.get("closed") or []),
                                },
                                separators=(",", ":"),
                            ),
                            flush=True,
                        )
                    else:
                        print(
                            "M2R3_STATE_RESTORE none key=" + _STATE_KEY,
                            flush=True,
                        )
                except Exception as exc:
                    print(
                        "M2R3_STATE_RESTORE_ERROR "
                        + json.dumps(
                            {"error": f"{type(exc).__name__}:{str(exc)[:180]}"},
                            separators=(",", ":"),
                        ),
                        flush=True,
                    )

                _restored_once = True
                sys.settrace(None)

                if not _saver_started:
                    _saver_started = True
                    threading.Thread(
                        target=_save_loop,
                        args=(state_obj,),
                        daemon=True,
                        name="m2r3-state-saver",
                    ).start()

                return None

            sys.settrace(_main_trace)
            print(
                "M2R3_PERSIST armed key=" + _STATE_KEY,
                flush=True,
            )
