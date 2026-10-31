"""
R3 A_GUARD v2 historical-only adapter.

Purpose:
- Preserve the original A_GUARD parameters and exits.
- Test entry-quality guards only:
  1) 1h trend must align with the trade direction.
  2) Trigger continuation must persist across recent 1m candles.
  3) ETH/XRP require stronger BTC/ETH market-regime confirmation.

No live/shadow/current DB access. No orders. No live parameter writes.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter

import r3_historical_adapter as v1
import r3_historical_adapter_fast as fast

# Fixed A_GUARD baseline parameters. Do not sweep in this file.
A_MOM = 0.03
A_VR = 0.90
A_CHASE = 0.30
A_MAX_OPEN = 2
A_MAX_NEW = 1

# V2 entry-quality guards. Fixed before the 360d A/B run.
V2_REQUIRE_1H_ALIGNMENT = True

V2_CONTINUATION_BARS = 3
V2_CONTINUATION_MIN_DIRECTIONAL_BARS = 2
V2_CONTINUATION_MIN_NET_MOM_PCT = 0.03
V2_CONTINUATION_MIN_VOLUME_RATIO = 0.80
V2_TRIGGER_HOLD_BUFFER_PCT = 0.02

V2_WEAK_SYMBOLS = {"ETH-USDT", "XRP-USDT"}
V2_WEAK_MIN_MARKET_ALIGNMENT = 3
V2_WEAK_REQUIRE_DOMINANCE = True

_REJECTS = Counter()

_ORIG_ANALYZE_SETUP = v1.analyze_setup
_ORIG_RECENT_TRIGGER_EVENT = v1.recent_trigger_event


def _reject_setup(x, code, reason):
    x = dict(x)
    x["decision"] = code
    x["reason"] = reason
    _REJECTS[code] += 1
    return x


def analyze_setup_v2(
    symbol,
    side,
    radar_move,
    live_price,
    scan_t,
    c5_by,
    c15_by,
    c1h_by,
    ctx,
):
    """
    Keep the original setup logic, then add only the V2 entry-quality guards.
    """
    x = _ORIG_ANALYZE_SETUP(
        symbol,
        side,
        radar_move,
        live_price,
        scan_t,
        c5_by,
        c15_by,
        c1h_by,
        ctx,
    )
    if not x:
        return x

    if x.get("decision") not in {"WATCH", "PENDING_BREAKOUT"}:
        return x

    if V2_REQUIRE_1H_ALIGNMENT and not bool(x.get("align_1h")):
        return _reject_setup(
            x,
            "REJECT_V2_1H_STRUCTURE",
            "V2 requires 1h structure aligned with trade direction",
        )

    if symbol in V2_WEAK_SYMBOLS:
        side_align = int(v1.market_alignment_for(side, ctx))
        opposite = int(ctx["bear"] if side == "LONG" else ctx["bull"])

        if side_align < V2_WEAK_MIN_MARKET_ALIGNMENT:
            return _reject_setup(
                x,
                "REJECT_V2_WEAK_REGIME",
                f"V2 {symbol} requires market alignment >= "
                f"{V2_WEAK_MIN_MARKET_ALIGNMENT}; got {side_align}",
            )

        if V2_WEAK_REQUIRE_DOMINANCE and side_align <= opposite:
            return _reject_setup(
                x,
                "REJECT_V2_WEAK_REGIME",
                f"V2 {symbol} requires directional market dominance "
                f"{side_align}>{opposite}",
            )

    return x


def recent_trigger_event_v2(
    c1_by,
    symbol,
    side,
    trigger_level,
    scan_t,
):
    """
    Original fixed-trigger event plus continuation/hold confirmation.

    This does not lower the original A_GUARD momentum/volume thresholds.
    It rejects a trigger when the latest 1m action looks like a one-candle spike
    that has already lost directional follow-through.
    """
    ev = _ORIG_RECENT_TRIGGER_EVENT(
        c1_by,
        symbol,
        side,
        trigger_level,
        scan_t,
    )
    if not ev:
        return None

    bars = v1.completed(
        c1_by.get(symbol, []),
        scan_t,
        30,
    )
    if len(bars) < 24:
        _REJECTS["TRIGGER_V2_INSUFFICIENT_1M"] += 1
        return None

    closes = [float(x["close"]) for x in bars]
    vols = [float(x.get("volume") or 0.0) for x in bars]

    n = V2_CONTINUATION_BARS
    recent_closes = closes[-(n + 1):]
    if len(recent_closes) < n + 1:
        _REJECTS["TRIGGER_V2_INSUFFICIENT_1M"] += 1
        return None

    directional_bars = 0
    for a, b in zip(recent_closes[:-1], recent_closes[1:]):
        r = v1.pct(b, a)
        if v1.directional(side, r) > 0:
            directional_bars += 1

    net_mom = v1.directional(
        side,
        v1.pct(recent_closes[-1], recent_closes[0]),
    )

    recent_vol = sum(vols[-n:]) / n
    prev = vols[-(n + 20):-n]
    prev_avg = (sum(prev) / len(prev)) if prev else 0.0
    continuation_vr = (
        recent_vol / prev_avg if prev_avg > 0 else 0.0
    )

    hold_pct = v1.directional(
        side,
        v1.pct(closes[-1], float(trigger_level)),
    )

    if directional_bars < V2_CONTINUATION_MIN_DIRECTIONAL_BARS:
        _REJECTS["TRIGGER_V2_DIRECTIONAL_BARS"] += 1
        return None

    if net_mom < V2_CONTINUATION_MIN_NET_MOM_PCT:
        _REJECTS["TRIGGER_V2_NET_MOM"] += 1
        return None

    if continuation_vr < V2_CONTINUATION_MIN_VOLUME_RATIO:
        _REJECTS["TRIGGER_V2_VOLUME_RETENTION"] += 1
        return None

    if hold_pct < V2_TRIGGER_HOLD_BUFFER_PCT:
        _REJECTS["TRIGGER_V2_NOT_HOLDING"] += 1
        return None

    out = dict(ev)
    out["v2_directional_bars"] = directional_bars
    out["v2_net_mom"] = net_mom
    out["v2_continuation_vr"] = continuation_vr
    out["v2_hold_pct"] = hold_pct
    _REJECTS["TRIGGER_V2_PASS"] += 1
    return out


def _metrics(trades):
    vals = [
        float(x["net_return_pct"])
        for x in trades
        if x.get("resolved")
    ]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gross_profit = sum(wins)
    gross_loss_abs = abs(sum(losses))
    pf = (
        gross_profit / gross_loss_abs
        if gross_loss_abs > 0
        else (999.0 if gross_profit > 0 else 0.0)
    )
    return {
        "entries": len(trades),
        "resolved_entries": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (
            round(100.0 * len(wins) / len(vals), 2)
            if vals else 0.0
        ),
        "profit_factor": round(pf, 3),
        "gross_profit_pct_points": round(gross_profit, 6),
        "gross_loss_abs_pct_points": round(gross_loss_abs, 6),
        "sum_net_return_pct": round(sum(vals), 3),
    }


def _breakdown(trades):
    def group(key_fn):
        out = {}
        for t in trades:
            if not t.get("resolved"):
                continue
            key = str(key_fn(t))
            d = out.setdefault(
                key,
                {
                    "entries": 0,
                    "wins": 0,
                    "losses": 0,
                    "sum_net_return_pct": 0.0,
                },
            )
            net = float(t["net_return_pct"])
            d["entries"] += 1
            d["sum_net_return_pct"] += net
            if net > 0:
                d["wins"] += 1
            elif net < 0:
                d["losses"] += 1
        for d in out.values():
            d["sum_net_return_pct"] = round(
                d["sum_net_return_pct"],
                6,
            )
        return dict(sorted(out.items()))

    return {
        "by_symbol": group(lambda t: t.get("symbol") or ""),
        "by_side": group(lambda t: t.get("side") or ""),
        "by_exit_reason": group(
            lambda t: t.get("exit_reason") or ""
        ),
    }


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--candles-1m", required=True)
    p.add_argument("--candles-5m", required=True)
    p.add_argument("--candles-15m", required=True)
    p.add_argument("--candles-1h", required=True)
    p.add_argument(
        "--detail-out",
        default="data/r3_v2_history_detail.csv",
    )
    p.add_argument(
        "--outcomes-out",
        default="data/r3_v2_core_outcomes.csv",
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

    fast.build_caches(c1, c5, c15, c1h)

    # Reuse the validated fast historical lookup layer.
    v1.completed = fast.completed_fast
    v1.latest_completed = fast.latest_completed_fast
    v1.rolling_quote_24h = fast.rolling_quote_24h_fast
    v1.price_24h_ago = fast.price_24h_ago_fast
    v1.ticker_proxy = fast.ticker_proxy_fast

    original = {
        "analyze_setup": v1.analyze_setup,
        "recent_trigger_event": v1.recent_trigger_event,
        "mom": float(v1.TRIGGER_MIN_1M_MOM),
        "vr": float(v1.TRIGGER_MIN_1M_VOLUME_RATIO),
        "chase": float(v1.ENTER_MAX_TRIGGER_CHASE),
        "max_open": int(v1.MAX_OPEN),
        "max_new": int(v1.MAX_NEW_PER_SCAN),
    }

    _REJECTS.clear()
    try:
        v1.analyze_setup = analyze_setup_v2
        v1.recent_trigger_event = recent_trigger_event_v2

        v1.TRIGGER_MIN_1M_MOM = A_MOM
        v1.TRIGGER_MIN_1M_VOLUME_RATIO = A_VR
        v1.ENTER_MAX_TRIGGER_CHASE = A_CHASE
        v1.MAX_OPEN = A_MAX_OPEN
        v1.MAX_NEW_PER_SCAN = A_MAX_NEW

        details, trades = v1.run_replay(
            c1,
            c5,
            c15,
            c1h,
            args.assumed_spread_pct,
        )
    finally:
        v1.analyze_setup = original["analyze_setup"]
        v1.recent_trigger_event = original["recent_trigger_event"]
        v1.TRIGGER_MIN_1M_MOM = original["mom"]
        v1.TRIGGER_MIN_1M_VOLUME_RATIO = original["vr"]
        v1.ENTER_MAX_TRIGGER_CHASE = original["chase"]
        v1.MAX_OPEN = original["max_open"]
        v1.MAX_NEW_PER_SCAN = original["max_new"]

    summary = {
        "candidate": "A_GUARD_V2_ENTRY_FILTERS",
        "baseline_params_unchanged": {
            "trigger_min_1m_mom": A_MOM,
            "trigger_min_1m_volume_ratio": A_VR,
            "enter_max_trigger_chase": A_CHASE,
            "max_open": A_MAX_OPEN,
            "max_new_per_scan": A_MAX_NEW,
        },
        "v2_guards": {
            "require_1h_alignment": V2_REQUIRE_1H_ALIGNMENT,
            "continuation_bars": V2_CONTINUATION_BARS,
            "continuation_min_directional_bars":
                V2_CONTINUATION_MIN_DIRECTIONAL_BARS,
            "continuation_min_net_mom_pct":
                V2_CONTINUATION_MIN_NET_MOM_PCT,
            "continuation_min_volume_ratio":
                V2_CONTINUATION_MIN_VOLUME_RATIO,
            "trigger_hold_buffer_pct":
                V2_TRIGGER_HOLD_BUFFER_PCT,
            "weak_symbols": sorted(V2_WEAK_SYMBOLS),
            "weak_min_market_alignment":
                V2_WEAK_MIN_MARKET_ALIGNMENT,
            "weak_require_dominance":
                V2_WEAK_REQUIRE_DOMINANCE,
        },
        "metrics": _metrics(trades),
        "breakdown": _breakdown(trades),
        "v2_reject_counts": dict(sorted(_REJECTS.items())),
    }

    print(
        "R3_A_GUARD_V2_SUMMARY "
        + json.dumps(
            summary,
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    print("R3_A_GUARD_V2_DONE", flush=True)

    result = v1.write_outputs(
        details,
        trades,
        args.detail_out,
        args.outcomes_out,
    )
    print("R3_V2_RESULT", result, flush=True)
    print("R3_V2_STAGE_DONE", flush=True)


if __name__ == "__main__":
    main()
