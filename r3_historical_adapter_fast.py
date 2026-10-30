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
_QV_FALLBACK_ROWS = {}


def _register_series(series):
    _SERIES_ENDS[id(series)] = [x["end"] for x in series]


def build_caches(c1_by, c5_by, c15_by, c1h_by):
    _SERIES_ENDS.clear()
    _C1_META.clear()
    _QV_FALLBACK_ROWS.clear()

    for source in (c1_by, c5_by, c15_by, c1h_by):
        for rows in source.values():
            _register_series(rows)

    for symbol, rows in c1_by.items():
        ends = _SERIES_ENDS[id(rows)]
        prefix = [0.0]
        total = 0.0
        fallback_rows = 0
        for x in rows:
            qv = float(x.get("quote_volume") or 0.0)

            # BingX historical 1m responses can return quote_volume=0 even when
            # base volume and price are present. R3's live Radar gate is defined
            # in quote currency, so for historical-only replay reconstruct a
            # conservative per-candle quote-volume proxy from base volume × close.
            # This does NOT change the live strategy or write any database.
            if qv <= 0.0:
                base_vol = float(x.get("volume") or 0.0)
                close = float(x.get("close") or 0.0)
                if base_vol > 0.0 and close > 0.0:
                    qv = base_vol * close
                    fallback_rows += 1

            total += qv
            prefix.append(total)

        _QV_FALLBACK_ROWS[symbol] = fallback_rows
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
        "quote_volume_fallback_rows": dict(sorted(_QV_FALLBACK_ROWS.items())),
        "per_symbol": {k: dict(v) for k, v in sorted(per_symbol.items())},
    }

# ---------------------------------------------------------------------------
# Stage-2 lifecycle diagnostic wrappers.
# These wrappers count gates only; every original return value is preserved.
# ---------------------------------------------------------------------------
_STAGE2 = Counter()
_STAGE2_EXPECT_TRIGGER_MARKET = False
_STAGE2_EXPECT_CHASE = False
_STAGE2_TRIGGER_SIDE = None


def _install_stage2_wrappers():
    global _STAGE2_EXPECT_TRIGGER_MARKET, _STAGE2_EXPECT_CHASE, _STAGE2_TRIGGER_SIDE

    _STAGE2.clear()
    _STAGE2_EXPECT_TRIGGER_MARKET = False
    _STAGE2_EXPECT_CHASE = False
    _STAGE2_TRIGGER_SIDE = None

    orig_local_trigger = v1.local_trigger
    orig_recent_trigger_event = v1.recent_trigger_event
    orig_market_alignment_for = v1.market_alignment_for
    orig_pct = v1.pct
    orig_risk_allows = v1.risk_allows
    orig_symbol_blocked = v1.symbol_blocked
    orig_simulate_exit = v1.simulate_exit

    def local_trigger_wrapped(c1_by, symbol, side, scan_t):
        _STAGE2["local_trigger_calls"] += 1
        r = orig_local_trigger(c1_by, symbol, side, scan_t)
        if r is None:
            _STAGE2["local_trigger_none"] += 1
        else:
            _STAGE2["local_trigger_success"] += 1
        return r

    def recent_trigger_event_wrapped(c1_by, symbol, side, trigger_level, scan_t):
        global _STAGE2_EXPECT_TRIGGER_MARKET, _STAGE2_TRIGGER_SIDE
        _STAGE2["recent_trigger_calls"] += 1
        r = orig_recent_trigger_event(c1_by, symbol, side, trigger_level, scan_t)
        if r is None:
            _STAGE2["recent_trigger_none"] += 1
            return r

        _STAGE2["recent_trigger_found"] += 1
        mom = float(r.get("mom") or 0.0)
        vr = float(r.get("vr") or 0.0)

        if mom < v1.TRIGGER_MIN_1M_MOM:
            _STAGE2["trigger_momentum_fail"] += 1
        else:
            _STAGE2["trigger_momentum_pass"] += 1

        if vr < v1.TRIGGER_MIN_1M_VOLUME_RATIO:
            _STAGE2["trigger_volume_fail"] += 1
        else:
            _STAGE2["trigger_volume_pass"] += 1

        if mom >= v1.TRIGGER_MIN_1M_MOM and vr >= v1.TRIGGER_MIN_1M_VOLUME_RATIO:
            _STAGE2_EXPECT_TRIGGER_MARKET = True
            _STAGE2_TRIGGER_SIDE = side
        return r

    def market_alignment_for_wrapped(side, ctx):
        global _STAGE2_EXPECT_TRIGGER_MARKET, _STAGE2_EXPECT_CHASE, _STAGE2_TRIGGER_SIDE
        r = orig_market_alignment_for(side, ctx)
        if _STAGE2_EXPECT_TRIGGER_MARKET:
            _STAGE2["trigger_market_checks"] += 1
            if int(r) < v1.MIN_MARKET_ALIGNMENT:
                _STAGE2["trigger_market_fail"] += 1
                _STAGE2_EXPECT_CHASE = False
                _STAGE2_TRIGGER_SIDE = None
            else:
                _STAGE2["trigger_market_pass"] += 1
                # If the supportive/retrace gate passes, the next pct() call in
                # run_replay is the trigger-chase calculation.
                _STAGE2_EXPECT_CHASE = True
                _STAGE2_TRIGGER_SIDE = side
            _STAGE2_EXPECT_TRIGGER_MARKET = False
        return r

    def pct_wrapped(a, b):
        global _STAGE2_EXPECT_CHASE, _STAGE2_TRIGGER_SIDE
        r = orig_pct(a, b)
        if _STAGE2_EXPECT_CHASE:
            _STAGE2["trigger_chase_checks"] += 1
            adverse = max(float(r), 0.0)
            if adverse > v1.ENTER_MAX_TRIGGER_CHASE:
                _STAGE2["trigger_chase_fail"] += 1
            else:
                _STAGE2["trigger_chase_pass"] += 1
            _STAGE2_EXPECT_CHASE = False
            _STAGE2_TRIGGER_SIDE = None
        return r

    def risk_allows_wrapped(scan_t, trades):
        _STAGE2["risk_checks"] += 1
        r = orig_risk_allows(scan_t, trades)
        if r[0]:
            _STAGE2["risk_pass"] += 1
        else:
            _STAGE2["risk_fail"] += 1
            _STAGE2[f"risk_fail_{r[1]}"] += 1
        return r

    def symbol_blocked_wrapped(symbol, scan_t, trades):
        _STAGE2["symbol_block_checks"] += 1
        r = orig_symbol_blocked(symbol, scan_t, trades)
        if r:
            _STAGE2["symbol_blocked"] += 1
        else:
            _STAGE2["symbol_block_pass"] += 1
        return r

    def simulate_exit_wrapped(side, entry, entry_time, bars):
        _STAGE2["simulate_exit_calls"] += 1
        return orig_simulate_exit(side, entry, entry_time, bars)

    v1.local_trigger = local_trigger_wrapped
    v1.recent_trigger_event = recent_trigger_event_wrapped
    v1.market_alignment_for = market_alignment_for_wrapped
    v1.pct = pct_wrapped
    v1.risk_allows = risk_allows_wrapped
    v1.symbol_blocked = symbol_blocked_wrapped
    v1.simulate_exit = simulate_exit_wrapped


def _stage2_summary(details, trades):
    events = Counter(str(x.get("event") or "UNKNOWN") for x in details)
    reasons = Counter(str(x.get("reason") or "") for x in details)

    # If market passed but no chase calculation followed, the supportive
    # retrace gate blocked the setup before entry-price/chase evaluation.
    supportive_fail = max(
        int(_STAGE2.get("trigger_market_pass", 0))
        - int(_STAGE2.get("trigger_chase_checks", 0)),
        0,
    )

    return {
        "event_counts": dict(events),
        "top_event_reasons": reasons.most_common(20),
        "gate_counts": dict(_STAGE2),
        "derived": {
            "supportive_or_retrace_fail": supportive_fail,
            "pullback_confirmed_estimate": int(events.get("PULLBACK_CONFIRMED", 0)),
            "entered": int(events.get("ENTER", 0)),
            "trades": len(trades),
        },
    }


def _result_from_trades(trades):
    vals = [float(x["net_return_pct"]) for x in trades if x.get("resolved")]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    pf = sum(wins) / abs(sum(losses)) if losses else (999.0 if wins else 0.0)
    return {
        "entries": len(trades),
        "resolved_entries": len(vals),
        "win_rate_pct": round(100.0 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": round(pf, 3),
        "sum_net_return_pct": round(sum(vals), 3),
    }


def run_chase_sweep(c1, c5, c15, c1h, spread):
    """Historical-only counterfactual sweep. No DB, no orders, no live changes."""
    original = float(v1.ENTER_MAX_TRIGGER_CHASE)
    thresholds = [0.15, 0.20, 0.25, 0.30, 0.40, 0.50]
    results = {}
    try:
        for threshold in thresholds:
            v1.ENTER_MAX_TRIGGER_CHASE = float(threshold)
            details_i, trades_i = v1.run_replay(c1, c5, c15, c1h, spread)
            metrics = _result_from_trades(trades_i)
            metrics["events"] = len(details_i)
            results[f"{threshold:.2f}"] = metrics
    finally:
        v1.ENTER_MAX_TRIGGER_CHASE = original
    return {
        "baseline_live_default": original,
        "thresholds": results,
    }


def run_trigger_grid_sweep(c1, c5, c15, c1h, spread):
    """Historical-only small grid. No DB, no orders, no live parameter writes."""
    orig_mom = float(v1.TRIGGER_MIN_1M_MOM)
    orig_vr = float(v1.TRIGGER_MIN_1M_VOLUME_RATIO)
    orig_chase = float(v1.ENTER_MAX_TRIGGER_CHASE)

    moms = [0.00, 0.03, 0.05]
    vrs = [0.80, 0.90, 1.00]
    chases = [0.20, 0.25, 0.30]
    rows = []

    try:
        for mom in moms:
            for vr in vrs:
                for chase in chases:
                    v1.TRIGGER_MIN_1M_MOM = float(mom)
                    v1.TRIGGER_MIN_1M_VOLUME_RATIO = float(vr)
                    v1.ENTER_MAX_TRIGGER_CHASE = float(chase)

                    details_i, trades_i = v1.run_replay(
                        c1, c5, c15, c1h, spread
                    )
                    metrics = _result_from_trades(trades_i)
                    vals = [
                        float(x["net_return_pct"])
                        for x in trades_i
                        if x.get("resolved")
                    ]
                    wins = sum(1 for x in vals if x > 0)
                    losses = sum(1 for x in vals if x < 0)

                    rows.append({
                        "mom": round(mom, 2),
                        "vr": round(vr, 2),
                        "chase": round(chase, 2),
                        "events": len(details_i),
                        "entries": metrics["entries"],
                        "resolved_entries": metrics["resolved_entries"],
                        "wins": wins,
                        "losses": losses,
                        "win_rate_pct": metrics["win_rate_pct"],
                        "profit_factor": metrics["profit_factor"],
                        "sum_net_return_pct": metrics["sum_net_return_pct"],
                    })
    finally:
        v1.TRIGGER_MIN_1M_MOM = orig_mom
        v1.TRIGGER_MIN_1M_VOLUME_RATIO = orig_vr
        v1.ENTER_MAX_TRIGGER_CHASE = orig_chase

    ranked = sorted(
        rows,
        key=lambda r: (
            r["sum_net_return_pct"],
            r["profit_factor"],
            r["resolved_entries"],
        ),
        reverse=True,
    )

    return {
        "baseline": {
            "mom": orig_mom,
            "vr": orig_vr,
            "chase": orig_chase,
        },
        "tested": len(rows),
        "top10": ranked[:10],
        "all": rows,
    }


def run_shortlist_sweep(c1, c5, c15, c1h, spread):
    """Historical-only shortlist validation. No DB/orders/live parameter writes."""
    orig_mom = float(v1.TRIGGER_MIN_1M_MOM)
    orig_vr = float(v1.TRIGGER_MIN_1M_VOLUME_RATIO)
    orig_chase = float(v1.ENTER_MAX_TRIGGER_CHASE)

    configs = [
        {
            "name": "A_BALANCED",
            "mom": 0.03,
            "vr": 0.90,
            "chase": 0.30,
        },
        {
            "name": "B_CONSERVATIVE",
            "mom": 0.03,
            "vr": 1.00,
            "chase": 0.25,
        },
        {
            "name": "C_MINIMAL_CHANGE",
            "mom": 0.05,
            "vr": 0.90,
            "chase": 0.30,
        },
    ]

    rows = []
    try:
        for cfg in configs:
            v1.TRIGGER_MIN_1M_MOM = float(cfg["mom"])
            v1.TRIGGER_MIN_1M_VOLUME_RATIO = float(cfg["vr"])
            v1.ENTER_MAX_TRIGGER_CHASE = float(cfg["chase"])

            details_i, trades_i = v1.run_replay(
                c1, c5, c15, c1h, spread
            )
            metrics = _result_from_trades(trades_i)
            vals = [
                float(x["net_return_pct"])
                for x in trades_i
                if x.get("resolved")
            ]
            wins = sum(1 for x in vals if x > 0)
            losses = sum(1 for x in vals if x < 0)

            rows.append({
                **cfg,
                "events": len(details_i),
                "entries": metrics["entries"],
                "resolved_entries": metrics["resolved_entries"],
                "wins": wins,
                "losses": losses,
                "win_rate_pct": metrics["win_rate_pct"],
                "profit_factor": metrics["profit_factor"],
                "sum_net_return_pct": metrics["sum_net_return_pct"],
            })
    finally:
        v1.TRIGGER_MIN_1M_MOM = orig_mom
        v1.TRIGGER_MIN_1M_VOLUME_RATIO = orig_vr
        v1.ENTER_MAX_TRIGGER_CHASE = orig_chase

    return {
        "baseline": {
            "mom": orig_mom,
            "vr": orig_vr,
            "chase": orig_chase,
        },
        "candidates": rows,
    }


def _trade_detail_rows(candidate_name, trades):
    rows = []
    for t in trades:
        rows.append({
            "candidate": candidate_name,
            "symbol": str(t.get("symbol") or ""),
            "side": str(t.get("side") or ""),
            "entry_time": t.get("entry_time").isoformat() if t.get("entry_time") else None,
            "exit_time": t.get("exit_time").isoformat() if t.get("exit_time") else None,
            "entry_price": round(float(t.get("entry_price") or 0.0), 10),
            "exit_price": round(float(t.get("exit_price") or 0.0), 10),
            "exit_reason": str(t.get("exit_reason") or ""),
            "gross_return_pct": round(float(t.get("gross_return_pct") or 0.0), 6),
            "net_return_pct": round(float(t.get("net_return_pct") or 0.0), 6),
            "resolved": bool(t.get("resolved")),
        })
    return rows


def _trade_breakdown(rows):
    def bucket(key_fn):
        out = {}
        for r in rows:
            key = key_fn(r)
            d = out.setdefault(key, {
                "entries": 0, "wins": 0, "losses": 0,
                "sum_net_return_pct": 0.0,
            })
            d["entries"] += 1
            net = float(r["net_return_pct"])
            d["sum_net_return_pct"] += net
            if net > 0:
                d["wins"] += 1
            elif net < 0:
                d["losses"] += 1
        for d in out.values():
            d["sum_net_return_pct"] = round(d["sum_net_return_pct"], 6)
            d["win_rate_pct"] = round(
                100.0 * d["wins"] / d["entries"], 2
            ) if d["entries"] else 0.0
        return dict(sorted(out.items()))

    return {
        "by_symbol": bucket(lambda r: r["symbol"]),
        "by_side": bucket(lambda r: r["side"]),
        "by_exit_reason": bucket(lambda r: r["exit_reason"]),
        "by_symbol_side": bucket(lambda r: f'{r["symbol"]}|{r["side"]}'),
    }


def run_trade_detail_diagnostic(c1, c5, c15, c1h, spread):
    """
    Historical-only trade-level diagnostic for A/B shortlist.
    Does not write DB, send orders, or change live parameters.
    """
    orig_mom = float(v1.TRIGGER_MIN_1M_MOM)
    orig_vr = float(v1.TRIGGER_MIN_1M_VOLUME_RATIO)
    orig_chase = float(v1.ENTER_MAX_TRIGGER_CHASE)

    configs = [
        {
            "name": "A_BALANCED",
            "mom": 0.03,
            "vr": 0.90,
            "chase": 0.30,
        },
        {
            "name": "B_CONSERVATIVE",
            "mom": 0.03,
            "vr": 1.00,
            "chase": 0.25,
        },
    ]

    results = []
    try:
        for cfg in configs:
            v1.TRIGGER_MIN_1M_MOM = float(cfg["mom"])
            v1.TRIGGER_MIN_1M_VOLUME_RATIO = float(cfg["vr"])
            v1.ENTER_MAX_TRIGGER_CHASE = float(cfg["chase"])

            details_i, trades_i = v1.run_replay(
                c1, c5, c15, c1h, spread
            )

            rows = _trade_detail_rows(cfg["name"], trades_i)
            results.append({
                **cfg,
                "trade_count": len(rows),
                "trades": rows,
                "breakdown": _trade_breakdown(rows),
            })
    finally:
        v1.TRIGGER_MIN_1M_MOM = orig_mom
        v1.TRIGGER_MIN_1M_VOLUME_RATIO = orig_vr
        v1.ENTER_MAX_TRIGGER_CHASE = orig_chase

    return {
        "baseline": {
            "mom": orig_mom,
            "vr": orig_vr,
            "chase": orig_chase,
        },
        "candidates": results,
    }


def _metrics_from_trades(trades):
    vals = [float(x["net_return_pct"]) for x in trades if x.get("resolved")]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    pf = sum(wins) / abs(sum(losses)) if losses else (999.0 if wins else 0.0)
    return {
        "entries": len(trades),
        "resolved_entries": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100.0 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": round(pf, 3),
        "sum_net_return_pct": round(sum(vals), 3),
    }


def _breakdown(trades):
    def group(key_fn):
        out = {}
        for t in trades:
            if not t.get("resolved"):
                continue
            key = str(key_fn(t))
            d = out.setdefault(key, {"entries": 0, "wins": 0, "losses": 0, "sum_net_return_pct": 0.0})
            net = float(t["net_return_pct"])
            d["entries"] += 1
            d["sum_net_return_pct"] += net
            if net > 0:
                d["wins"] += 1
            elif net < 0:
                d["losses"] += 1
        for d in out.values():
            d["sum_net_return_pct"] = round(d["sum_net_return_pct"], 6)
        return dict(sorted(out.items()))

    return {
        "by_symbol": group(lambda t: t.get("symbol") or ""),
        "by_side": group(lambda t: t.get("side") or ""),
        "by_exit_reason": group(lambda t: t.get("exit_reason") or ""),
    }


def run_a_guard(c1, c5, c15, c1h, spread):
    """
    Historical-only chosen R3 candidate.
    Signal:
      momentum 0.03%
      1m volume ratio 0.90
      trigger chase 0.30%
    Portfolio guard:
      MAX_OPEN 2
      MAX_NEW_PER_SCAN 1
    No DB writes, no orders, no live configuration changes.
    """
    orig = {
        "mom": float(v1.TRIGGER_MIN_1M_MOM),
        "vr": float(v1.TRIGGER_MIN_1M_VOLUME_RATIO),
        "chase": float(v1.ENTER_MAX_TRIGGER_CHASE),
        "max_open": int(v1.MAX_OPEN),
        "max_new": int(v1.MAX_NEW_PER_SCAN),
    }

    try:
        v1.TRIGGER_MIN_1M_MOM = 0.03
        v1.TRIGGER_MIN_1M_VOLUME_RATIO = 0.90
        v1.ENTER_MAX_TRIGGER_CHASE = 0.30
        v1.MAX_OPEN = 2
        v1.MAX_NEW_PER_SCAN = 1

        details, trades = v1.run_replay(c1, c5, c15, c1h, spread)
        summary = {
            "candidate": "A_GUARD",
            "params": {
                "trigger_min_1m_mom": 0.03,
                "trigger_min_1m_volume_ratio": 0.90,
                "enter_max_trigger_chase": 0.30,
                "max_open": 2,
                "max_new_per_scan": 1,
            },
            "metrics": _metrics_from_trades(trades),
            "breakdown": _breakdown(trades),
        }
        return details, trades, summary
    finally:
        v1.TRIGGER_MIN_1M_MOM = orig["mom"]
        v1.TRIGGER_MIN_1M_VOLUME_RATIO = orig["vr"]
        v1.ENTER_MAX_TRIGGER_CHASE = orig["chase"]
        v1.MAX_OPEN = orig["max_open"]
        v1.MAX_NEW_PER_SCAN = orig["max_new"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--candles-1m", required=True)
    p.add_argument("--candles-5m", required=True)
    p.add_argument("--candles-15m", required=True)
    p.add_argument("--candles-1h", required=True)
    p.add_argument("--detail-out", default="data/r3_history_detail.csv")
    p.add_argument("--outcomes-out", default="data/r3_core_outcomes.csv")
    p.add_argument("--assumed-spread-pct", type=float, default=0.12)
    args = p.parse_args()

    if args.assumed_spread_pct < 0:
        raise ValueError("spread proxy must be >= 0")

    c1 = v1.read_candles(args.candles_1m, 1)
    c5 = v1.read_candles(args.candles_5m, 5)
    c15 = v1.read_candles(args.candles_15m, 15)
    c1h = v1.read_candles(args.candles_1h, 60)

    build_caches(c1, c5, c15, c1h)

    v1.completed = completed_fast
    v1.latest_completed = latest_completed_fast
    v1.rolling_quote_24h = rolling_quote_24h_fast
    v1.price_24h_ago = price_24h_ago_fast
    v1.ticker_proxy = ticker_proxy_fast

    print(
        "R3_QUOTE_VOLUME_FALLBACK "
        + json.dumps(dict(sorted(_QV_FALLBACK_ROWS.items())), sort_keys=True),
        flush=True,
    )
    print(
        "R3_FAST_CACHE_READY "
        f"symbols={len(c1)} rows_1m={sum(len(x) for x in c1.values())}",
        flush=True,
    )

    details, trades, summary = run_a_guard(
        c1, c5, c15, c1h, args.assumed_spread_pct
    )

    print(
        "R3_A_GUARD_SUMMARY "
        + json.dumps(summary, ensure_ascii=False, sort_keys=True),
        flush=True,
    )
    print("R3_A_GUARD_DONE", flush=True)

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
