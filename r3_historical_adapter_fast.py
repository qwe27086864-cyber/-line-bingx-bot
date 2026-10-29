"""
R3 Historical Fast Adapter

Drop-in performance wrapper around r3_historical_adapter.py.
Trading logic, thresholds, state machine, exits, costs, and output schema are
unchanged. Only historical lookup performance is optimized.

Key fixes:
- cache candle end timestamps instead of rebuilding them every scan
- use prefix sums for rolling 24h quote volume
- use bisect for 24h-ago price lookup
- preserve the original R3.1 historical replay semantics

Historical-only. No DATABASE_URL access. No orders.
"""
from __future__ import annotations

import argparse
import bisect
from pathlib import Path

import r3_historical_adapter as v1

_SERIES_ENDS = {}
_C1_META = {}


def _register_series(series):
    _SERIES_ENDS[id(series)] = [x["end"] for x in series]


def build_caches(c1_by, c5_by, c15_by, c1h_by):
    _SERIES_ENDS.clear()
    _C1_META.clear()

    for source in (c1_by, c5_by, c15_by, c1h_by):
        for rows in source.values():
            _register_series(rows)

    for symbol, rows in c1_by.items():
        ends = _SERIES_ENDS[id(rows)]
        prefix = [0.0]
        total = 0.0
        for x in rows:
            total += float(x.get("quote_volume") or 0.0)
            prefix.append(total)
        _C1_META[symbol] = {
            "rows": rows,
            "ends": ends,
            "prefix_quote": prefix,
        }


def completed_fast(series, scan_t, n=None):
    ends = _SERIES_ENDS.get(id(series))
    if ends is None:
        _register_series(series)
        ends = _SERIES_ENDS[id(series)]

    idx = bisect.bisect_right(ends, scan_t)
    if n is None:
        return series[:idx]
    return series[max(0, idx - n):idx]


def latest_completed_fast(series, scan_t):
    ends = _SERIES_ENDS.get(id(series))
    if ends is None:
        _register_series(series)
        ends = _SERIES_ENDS[id(series)]

    idx = bisect.bisect_right(ends, scan_t) - 1
    if idx < 0:
        return None
    return series[idx]


def rolling_quote_24h_fast(c1_by, symbol, scan_t):
    meta = _C1_META.get(symbol)
    if not meta:
        return 0.0

    ends = meta["ends"]
    prefix = meta["prefix_quote"]

    right = bisect.bisect_right(ends, scan_t)
    cutoff = scan_t - v1.timedelta(hours=24)
    left = bisect.bisect_right(ends, cutoff)

    return prefix[right] - prefix[left]


def price_24h_ago_fast(c1_by, symbol, scan_t):
    meta = _C1_META.get(symbol)
    if not meta:
        return None

    target = scan_t - v1.timedelta(hours=24)
    idx = bisect.bisect_right(meta["ends"], target) - 1
    if idx < 0:
        return None
    return meta["rows"][idx]["close"]


def ticker_proxy_fast(c1_by, symbol, scan_t, spread_pct):
    meta = _C1_META.get(symbol)
    if not meta:
        return None

    idx = bisect.bisect_right(meta["ends"], scan_t) - 1
    if idx < 0:
        return None

    p = float(meta["rows"][idx]["close"])
    half = spread_pct / 200.0

    old = price_24h_ago_fast(c1_by, symbol, scan_t)
    change24 = v1.pct(p, old if old else p)

    return {
        "last": p,
        "bid": p * (1.0 - half),
        "ask": p * (1.0 + half),
        "spread_pct": spread_pct,
        "quote_volume": rolling_quote_24h_fast(
            c1_by, symbol, scan_t
        ),
        "change_24h": change24,
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--candles-1m", required=True)
    p.add_argument("--candles-5m", required=True)
    p.add_argument("--candles-15m", required=True)
    p.add_argument("--candles-1h", required=True)
    p.add_argument(
        "--detail-out",
        default="data/r3_history_detail.csv",
    )
    p.add_argument(
        "--outcomes-out",
        default="data/r3_core_outcomes.csv",
    )
    p.add_argument(
        "--assumed-spread-pct",
        type=float,
        default=0.12,
    )
    args = p.parse_args()

    if args.assumed_spread_pct < 0:
        raise ValueError("spread proxy must be >= 0")

    c1 = v1.read_candles(args.candles_1m, 1)
    c5 = v1.read_candles(args.candles_5m, 5)
    c15 = v1.read_candles(args.candles_15m, 15)
    c1h = v1.read_candles(args.candles_1h, 60)

    build_caches(c1, c5, c15, c1h)

    # Monkey-patch only lookup helpers. Core strategy/state-machine logic
    # remains the original implementation.
    v1.completed = completed_fast
    v1.latest_completed = latest_completed_fast
    v1.rolling_quote_24h = rolling_quote_24h_fast
    v1.price_24h_ago = price_24h_ago_fast
    v1.ticker_proxy = ticker_proxy_fast

    print(
        "R3_FAST_CACHE_READY "
        f"symbols={len(c1)} "
        f"rows_1m={sum(len(x) for x in c1.values())}",
        flush=True,
    )

    details, trades = v1.run_replay(
        c1,
        c5,
        c15,
        c1h,
        args.assumed_spread_pct,
    )

    result = v1.write_outputs(
        details,
        trades,
        args.detail_out,
        args.outcomes_out,
    )

    print("R3_FAST_RESULT", result, flush=True)
    print("R3_FAST_STAGE_DONE", flush=True)


if __name__ == "__main__":
    main()
