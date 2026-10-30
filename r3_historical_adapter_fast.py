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
import json
from collections import Counter, defaultdict
from statistics import median
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



def _percentile(values, q):
    if not values:
        return 0.0
    xs = sorted(float(x) for x in values)
    if len(xs) == 1:
        return xs[0]
    pos = (len(xs) - 1) * float(q)
    lo = int(pos)
    hi = min(lo + 1, len(xs) - 1)
    frac = pos - lo
    return xs[lo] * (1.0 - frac) + xs[hi] * frac


def run_gate_diagnostic(c1, c5, c15, c1h, spread):
    """Count every pre-entry R3.1 gate without changing strategy behavior."""
    all_1m = [x for rows in c1.values() for x in rows]
    if not all_1m:
        return {"error": "no_1m_candles"}

    start = min(x["end"] for x in all_1m)
    end = max(x["end"] for x in all_1m)
    start = start.replace(second=0, microsecond=0)
    minute = start.minute - (start.minute % 5)
    scan = start.replace(minute=minute)
    if scan < start:
        scan += v1.timedelta(minutes=5)

    previous = {}
    counts = Counter()
    decisions = Counter()
    reasons = Counter()
    per_symbol = defaultdict(Counter)
    abs_moves = []
    quote_volumes = []
    max_abs_move = defaultdict(float)
    thresholds = (0.20, 0.35, 0.50, 0.65, 0.80, 1.00, 1.50, 2.00)

    while scan <= end:
        counts["scans"] += 1
        ctx = v1.market_context(c15, c1h, scan)
        current = {}
        for symbol in c1.keys():
            q = v1.ticker_proxy(c1, symbol, scan, spread)
            if q:
                current[symbol] = q
                counts["ticker_available"] += 1

        candidates = []
        for symbol, t in current.items():
            old = previous.get(symbol)
            if not old:
                counts["no_previous_snapshot"] += 1
                per_symbol[symbol]["no_previous_snapshot"] += 1
                continue

            age = (scan - old["captured_at"]).total_seconds() / 60.0
            if age < 2 or age > 20:
                counts["age_fail"] += 1
                per_symbol[symbol]["age_fail"] += 1
                continue
            counts["age_pass"] += 1

            sp = t.get("spread_pct")
            if sp is None or sp > v1.SCAN_MAX_SPREAD:
                counts["spread_fail"] += 1
                per_symbol[symbol]["spread_fail"] += 1
                continue
            counts["spread_pass"] += 1

            qv = float(t.get("quote_volume") or 0.0)
            quote_volumes.append(qv)
            if qv <= 0:
                counts["quote_volume_zero"] += 1
                per_symbol[symbol]["quote_volume_zero"] += 1
            if qv < v1.MIN_QUOTE_VOLUME:
                counts["volume_fail"] += 1
                per_symbol[symbol]["volume_fail"] += 1
                continue
            counts["volume_pass"] += 1

            move = v1.pct(t["last"], old["last"])
            a = abs(float(move))
            abs_moves.append(a)
            max_abs_move[symbol] = max(max_abs_move[symbol], a)
            for th in thresholds:
                if a >= th:
                    key = f"move_ge_{th:.2f}"
                    counts[key] += 1
                    per_symbol[symbol][key] += 1

            if a < v1.RADAR_MIN_MOVE:
                counts["radar_move_fail"] += 1
                per_symbol[symbol]["radar_move_fail"] += 1
                continue

            counts["radar_move_pass"] += 1
            per_symbol[symbol]["radar_move_pass"] += 1
            side = "LONG" if move > 0 else "SHORT"
            rank = min(a, 5.0) * 12.0
            if sp <= 0.10:
                rank += 12
            elif sp <= 0.20:
                rank += 8
            if qv >= 2_000_000:
                rank += 8
            if v1.directional(side, t.get("change_24h", 0)) > 0:
                rank += 5
            candidates.append((rank, a, symbol, side, move, t["last"]))

        counts["radar_candidates_raw"] += len(candidates)
        candidates.sort(reverse=True)
        selected = candidates[:v1.RADAR_MAX_CANDIDATES][:v1.MAX_DEEP_ANALYSIS]
        counts["radar_candidates_selected"] += len(selected)

        for _, __, symbol, side, move, live in selected:
            counts["analyze_setup_calls"] += 1
            x = v1.analyze_setup(
                symbol, side, move, float(live), scan, c5, c15, c1h, ctx
            )
            if not x:
                counts["analyze_setup_none"] += 1
                per_symbol[symbol]["analyze_setup_none"] += 1
                continue
            decision = str(x.get("decision") or "UNKNOWN")
            reason = str(x.get("reason") or "")
            decisions[decision] += 1
            reasons[reason] += 1
            per_symbol[symbol][f"decision_{decision}"] += 1
            counts["setup_objects"] += 1
            if decision in ("WATCH", "PENDING_BREAKOUT"):
                counts["setup_actionable"] += 1

        previous = {s: {**t, "captured_at": scan} for s, t in current.items()}
        scan += v1.timedelta(minutes=5)

    return {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "assumed_spread_pct": spread,
        "radar_min_move_pct": v1.RADAR_MIN_MOVE,
        "min_quote_volume": v1.MIN_QUOTE_VOLUME,
        "counts": dict(counts),
        "decision_counts": dict(decisions),
        "top_reasons": reasons.most_common(12),
        "move_stats": {
            "n": len(abs_moves),
            "median_abs_move_pct": round(median(abs_moves), 6) if abs_moves else 0.0,
            "p95_abs_move_pct": round(_percentile(abs_moves, 0.95), 6),
            "p99_abs_move_pct": round(_percentile(abs_moves, 0.99), 6),
            "max_abs_move_pct": round(max(abs_moves), 6) if abs_moves else 0.0,
        },
        "volume_stats": {
            "n": len(quote_volumes),
            "median_quote_volume_24h": round(median(quote_volumes), 3) if quote_volumes else 0.0,
            "p05_quote_volume_24h": round(_percentile(quote_volumes, 0.05), 3),
            "min_quote_volume_24h": round(min(quote_volumes), 3) if quote_volumes else 0.0,
            "max_quote_volume_24h": round(max(quote_volumes), 3) if quote_volumes else 0.0,
        },
        "max_abs_move_by_symbol_pct": {
            k: round(v, 6) for k, v in sorted(max_abs_move.items())
        },
        "per_symbol": {k: dict(v) for k, v in sorted(per_symbol.items())},
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

    diag = run_gate_diagnostic(
        c1, c5, c15, c1h, args.assumed_spread_pct
    )
    print(
        "R3_DIAG_SUMMARY "
        + json.dumps(diag, ensure_ascii=False, sort_keys=True),
        flush=True,
    )
    print("R3_DIAG_DONE", flush=True)

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
