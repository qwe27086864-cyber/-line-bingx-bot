"""
M2R3 price-source hotfix.

Upload this file as `sitecustomize.py` to the ROOT of the
`meme2-development-branch` branch.

It is inert unless the environment variable below is enabled:
    M2R3_PRICE_SOURCE_FIX=1

When enabled, it patches `meme2_wallet_copy_job` so entry/confirm/fallback
prices all use the same Dexscreener highest-liquidity-pool selection rule
as M2R3's live paper-position manager.

This avoids fake instant TP/SL events caused by mixing GeckoTerminal pool
prices with Dexscreener prices.

PAPER-TRADING INFRASTRUCTURE FIX ONLY.
"""

import json
import os
import time
import urllib.parse
import urllib.request

if os.getenv("M2R3_PRICE_SOURCE_FIX", "").strip() == "1":
    import meme2_wallet_copy_job as _old

    _DEX_BASE = "https://api.dexscreener.com/tokens/v1/solana/"
    _FIX_MAX_CACHE_SEC = 15.0

    def _safe_float(v, default=0.0):
        try:
            if v is None or v == "":
                return default
            return float(v)
        except (TypeError, ValueError):
            return default

    def _dex_rows_for_mint(mint):
        url = _DEX_BASE + urllib.parse.quote(str(mint))
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "M2R3-price-source-fix/1.0",
                "Accept": "application/json",
            },
            method="GET",
        )
        last_exc = None
        retries = max(1, int(getattr(_old, "HTTP_RETRIES", 4) or 4))
        base_delay = max(
            0.5, float(getattr(_old, "HTTP_RETRY_BASE_SEC", 1.0) or 1.0)
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

            # Intentionally mirrors M2R3's existing dex_prices() logic exactly.
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

        # M2R3 sets MARKET_CACHE_TTL_SEC=300, but a 5-minute entry-price cache
        # can create an artificial instant TP after the 90-second confirmation.
        # Cap the effective cache at 15 seconds for this hotfix.
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

        # If Dexscreener is temporarily unavailable, keep the last same-source
        # market rather than switching to a different provider mid-trade.
        if cached:
            row = dict(cached["market"])
            row["liquidity_age_sec"] = max(0.0, now - cached.get("ts", now))
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

    # Patch only this Python process.
    _old.get_token_market = get_token_market
    _old.get_token_market_cached = get_token_market_cached
    _old.get_prices = get_prices

    print(
        "M2R3_PRICE_SOURCE_FIX active "
        "source=DEXSCREENER pool=highest_liquidity cache_max_sec=15",
        flush=True,
    )
