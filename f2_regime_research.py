from __future__ import annotations

import bisect
import csv
import json
import statistics
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

DATA_ROOT = Path("data/backtest_3y")
F1_ROOT = Path("data/f1_r0")
OUT_ROOT = Path("data/f2_regime")
OUT_ROOT.mkdir(parents=True, exist_ok=True)

SEGMENTS = 37
THRESHOLDS = tuple(round(0.25 + 0.025 * i, 3) for i in range(11))  # 0.25..0.50
EXTRA_COSTS = (0.00, 0.04, 0.06, 0.08)

# Architecture is intentionally frozen from F2 R1:
# event_type=REFERENCE, side=BOTH, align_min=0, ret24 disabled.
FIXED_RULE = {
    "event_type": "REFERENCE",
    "side": "BOTH",
    "align_min": 0,
    "ret24_min": -999.0,
}

def parse_dt(v):
    x = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)

def ema(vals, n):
    a = 2.0 / (n + 1.0)
    e = vals[0]
    for x in vals[1:]:
        e = a * x + (1.0 - a) * e
    return e

def pct(a, b):
    return (a / b - 1.0) * 100.0 if b else 0.0

def metrics(rows, extra_cost=0.0):
    vals = [float(r["net_return_pct"]) - extra_cost for r in rows]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl else (999.0 if gp else 0.0)

    eq = peak = dd = 0.0
    for x in vals:
        eq += x
        peak = max(peak, eq)
        dd = max(dd, peak - eq)

    segs = defaultdict(float)
    for r, v in zip(rows, vals):
        segs[int(r["segment"])] += v

    return {
        "trades": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": round(pf, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "avg_net_return_pct": round(sum(vals) / len(vals), 4) if vals else 0.0,
        "max_drawdown_pct_points": round(dd, 4),
        "positive_segments": sum(v > 0 for v in segs.values()),
        "active_segments": len(segs),
    }

def load_f1_reference():
    out = []
    seen = set()

    for i in range(1, SEGMENTS + 1):
        p = F1_ROOT / f"segment_{i:02d}" / "candidates.csv"
        if not p.exists():
            continue

        with p.open(newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                if r["trigger"] != "REFERENCE_BREAK":
                    continue

                key = (
                    r["timestamp"], r["symbol"], r["original_side"], r["side"],
                    r["exit_time"], r["net_return_pct"]
                )
                if key in seen:
                    continue
                seen.add(key)

                x = dict(r)
                x["segment"] = i
                x["timestamp_dt"] = parse_dt(x["timestamp"])
                out.append(x)

    return out

def load_market(interval):
    by = {s: [] for s in ("BTC-USDT", "ETH-USDT")}

    for i in range(1, SEGMENTS + 1):
        p = DATA_ROOT / f"segment_{i:02d}" / f"history_{interval}.csv"
        if not p.exists():
            continue

        with p.open(newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                s = r.get("symbol")
                if s not in by:
                    continue
                try:
                    by[s].append((parse_dt(r["timestamp"]), float(r["close"])))
                except Exception:
                    pass

    out = {}
    for s, rows in by.items():
        dedup = {t: c for t, c in rows}
        rows = sorted(dedup.items())
        out[s] = {
            "t": [x[0] for x in rows],
            "c": [x[1] for x in rows],
        }
    return out

def state_at(series, when):
    ts, cs = series["t"], series["c"]
    j = bisect.bisect_right(ts, when) - 1
    if j < 54:
        return "UNKNOWN"

    vals = cs[max(0, j - 79):j + 1]
    e20 = ema(vals[-55:], 20)
    e50 = ema(vals[-55:], 50)
    p = vals[-1]

    if p > e20 > e50:
        return "UP"
    if p < e20 < e50:
        return "DOWN"
    return "MIXED"

def h1_features(series, when):
    ts, cs = series["t"], series["c"]
    j = bisect.bisect_right(ts, when) - 1
    if j < 30:
        return None

    c = cs[j]
    r6 = pct(c, cs[j - 6])
    r24 = pct(c, cs[j - 24])

    rets = [
        pct(cs[k], cs[k - 1])
        for k in range(max(1, j - 23), j + 1)
    ]
    vol24 = statistics.pstdev(rets) if len(rets) >= 8 else 0.0
    return r6, r24, vol24

def enrich(rows, m15, m1h):
    out = []

    for r in rows:
        side = r["side"]
        want = "UP" if side == "LONG" else "DOWN"

        align = 0
        for symbol in ("BTC-USDT", "ETH-USDT"):
            align += int(state_at(m15[symbol], r["timestamp_dt"]) == want)
            align += int(state_at(m1h[symbol], r["timestamp_dt"]) == want)

        f_btc = h1_features(m1h["BTC-USDT"], r["timestamp_dt"])
        f_eth = h1_features(m1h["ETH-USDT"], r["timestamp_dt"])
        if not f_btc or not f_eth:
            continue

        d = 1.0 if side == "LONG" else -1.0

        x = dict(r)
        x["reverse_market_alignment"] = align
        x["market_6h_directional_pct"] = d * (f_btc[0] + f_eth[0]) / 2.0
        x["market_24h_directional_pct"] = d * (f_btc[1] + f_eth[1]) / 2.0
        x["market_vol24_pct"] = (f_btc[2] + f_eth[2]) / 2.0
        out.append(x)

    return out

def select(rows, threshold):
    return [
        r for r in rows
        if float(r["market_6h_directional_pct"]) >= threshold
    ]

def threshold_surface(rows, extra_cost=0.0):
    out = []
    for t in THRESHOLDS:
        m = metrics(select(rows, t), extra_cost=extra_cost)
        out.append({"threshold": t, **m})
    return out

def row_for(surface, t):
    for x in surface:
        if abs(x["threshold"] - t) < 1e-9:
            return x
    return None

def plateau_score(surface, t):
    here = row_for(surface, t)
    if not here or here["trades"] < 80 or here["sum_net_return_pct"] <= 0:
        return -1e9

    idx = THRESHOLDS.index(t)
    neighbors = [t]
    if idx > 0:
        neighbors.append(THRESHOLDS[idx - 1])
    if idx < len(THRESHOLDS) - 1:
        neighbors.append(THRESHOLDS[idx + 1])

    neighbor_rows = [row_for(surface, x) for x in neighbors]
    floor_pf = min(x["profit_factor"] for x in neighbor_rows if x)
    floor_net = min(x["sum_net_return_pct"] for x in neighbor_rows if x)
    min_trades = min(x["trades"] for x in neighbor_rows if x)

    # Favor a broad plateau, not a single-point spike.
    score = floor_pf
    score += 0.001 * min(floor_net, 100.0)
    score += 0.0002 * min(min_trades, 500)
    return score

def choose_threshold(prior_rows):
    surface = threshold_surface(prior_rows, extra_cost=0.0)

    scored = []
    for t in THRESHOLDS:
        scored.append((plateau_score(surface, t), t))

    scored.sort(reverse=True)
    best_score, best_t = scored[0]

    if best_score <= -1e8:
        # Fallback: still no future information; pick the threshold with
        # highest PF among thresholds with at least 60 prior trades.
        eligible = [
            x for x in surface
            if x["trades"] >= 60 and x["sum_net_return_pct"] > 0
        ]
        if not eligible:
            return None, surface
        eligible.sort(
            key=lambda x: (x["profit_factor"], x["sum_net_return_pct"], x["trades"]),
            reverse=True,
        )
        best_t = eligible[0]["threshold"]

    return best_t, surface

def seg_range(rows, lo, hi):
    return [r for r in rows if lo <= int(r["segment"]) <= hi]

def walk_forward(rows):
    # Each evaluation block is unseen when its threshold is selected.
    folds = [
        (1, 18, 19, 21),
        (1, 21, 22, 24),
        (1, 24, 25, 27),
        (1, 27, 28, 30),
        (1, 30, 31, 33),
        (1, 33, 34, 37),
    ]

    out = []
    combined_eval_rows = []

    for train_lo, train_hi, test_lo, test_hi in folds:
        prior = seg_range(rows, train_lo, train_hi)
        unseen = seg_range(rows, test_lo, test_hi)

        threshold, surface = choose_threshold(prior)
        if threshold is None:
            out.append({
                "train_segments": [train_lo, train_hi],
                "test_segments": [test_lo, test_hi],
                "threshold": None,
                "status": "NO_ELIGIBLE_THRESHOLD",
            })
            continue

        chosen = select(unseen, threshold)
        combined_eval_rows.extend(chosen)

        out.append({
            "train_segments": [train_lo, train_hi],
            "test_segments": [test_lo, test_hi],
            "threshold": threshold,
            "prior_metrics": row_for(surface, threshold),
            "test_metrics": metrics(chosen),
            "test_cost_004": metrics(chosen, 0.04),
            "test_cost_006": metrics(chosen, 0.06),
        })

    return out, combined_eval_rows

def frozen_oos(rows):
    pre_oos = seg_range(rows, 1, 27)
    oos = seg_range(rows, 28, 37)

    threshold, surface = choose_threshold(pre_oos)
    if threshold is None:
        return {
            "threshold": None,
            "status": "NO_ELIGIBLE_THRESHOLD",
        }

    chosen = select(oos, threshold)
    return {
        "threshold": threshold,
        "selection_surface_1_27": surface,
        "oos": metrics(chosen),
        "oos_cost_004": metrics(chosen, 0.04),
        "oos_cost_006": metrics(chosen, 0.06),
        "oos_cost_008": metrics(chosen, 0.08),
    }

def stability_table(rows):
    splits = {
        "train_1_18": seg_range(rows, 1, 18),
        "validation_19_27": seg_range(rows, 19, 27),
        "oos_28_37": seg_range(rows, 28, 37),
        "pre_oos_1_27": seg_range(rows, 1, 27),
        "all_1_37": seg_range(rows, 1, 37),
    }

    out = {}
    for name, part in splits.items():
        out[name] = {
            "base": threshold_surface(part, 0.0),
            "cost_004": threshold_surface(part, 0.04),
            "cost_006": threshold_surface(part, 0.06),
        }
    return out

def main():
    print(
        "F2_WF_START thresholds=0.25..0.50 step=0.025 "
        "architecture=REFERENCE_ONLY",
        flush=True,
    )

    if not (DATA_ROOT / "manifest.json").exists():
        raise RuntimeError("missing fixed 3Y dataset")
    if not F1_ROOT.exists():
        raise RuntimeError("missing F1 output")

    base = load_f1_reference()
    print(f"F2_WF_REFERENCE_ROWS rows={len(base)}", flush=True)

    m15 = load_market("15m")
    m1h = load_market("1h")
    rows = enrich(base, m15, m1h)
    print(f"F2_WF_FEATURE_ROWS rows={len(rows)}", flush=True)

    wf, wf_rows = walk_forward(rows)
    frozen = frozen_oos(rows)
    stability = stability_table(rows)

    wf_metrics = metrics(wf_rows)
    wf_cost_004 = metrics(wf_rows, 0.04)
    wf_cost_006 = metrics(wf_rows, 0.06)

    # Conservative pass:
    # - frozen OOS PF >= 1.05
    # - frozen OOS stays >=1 under +0.04% extra cost
    # - combined walk-forward PF >=1.05
    # - combined walk-forward stays >=1 under +0.04% extra cost
    conservative_pass = bool(
        frozen.get("oos", {}).get("trades", 0) >= 35
        and frozen.get("oos", {}).get("profit_factor", 0) >= 1.05
        and frozen.get("oos_cost_004", {}).get("profit_factor", 0) >= 1.00
        and wf_metrics["trades"] >= 80
        and wf_metrics["profit_factor"] >= 1.05
        and wf_cost_004["profit_factor"] >= 1.00
    )

    result = {
        "core": "F2_R2_FINE_GRID_WALK_FORWARD",
        "fixed_architecture": FIXED_RULE,
        "thresholds": THRESHOLDS,
        "selection_method": (
            "Choose threshold only from prior segments. "
            "Score uses the minimum PF/net/trades across the threshold and "
            "its immediate neighbors to favor plateaus over spikes."
        ),
        "walk_forward": wf,
        "walk_forward_combined": wf_metrics,
        "walk_forward_cost_004": wf_cost_004,
        "walk_forward_cost_006": wf_cost_006,
        "frozen_oos": frozen,
        "stability": stability,
        "conservative_pass": conservative_pass,
    }

    (OUT_ROOT / "f2_walkforward.json").write_text(
        json.dumps(result, indent=2),
        encoding="utf-8",
    )

    with (OUT_ROOT / "f2_threshold_surface.csv").open(
        "w", newline="", encoding="utf-8"
    ) as f:
        fields = [
            "split", "cost", "threshold", "trades", "profit_factor",
            "sum_net_return_pct", "avg_net_return_pct", "win_rate_pct",
            "max_drawdown_pct_points", "positive_segments", "active_segments",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()

        for split_name, cost_map in stability.items():
            for cost_name, surface in cost_map.items():
                for row in surface:
                    w.writerow({
                        "split": split_name,
                        "cost": cost_name,
                        **{k: row[k] for k in fields if k not in ("split", "cost")},
                    })

    summary = {
        "conservative_pass": conservative_pass,
        "frozen_threshold": frozen.get("threshold"),
        "frozen_oos": frozen.get("oos"),
        "frozen_oos_cost_004": frozen.get("oos_cost_004"),
        "wf_combined": wf_metrics,
        "wf_cost_004": wf_cost_004,
        "wf_cost_006": wf_cost_006,
        "folds": [
            {
                "test_segments": x.get("test_segments"),
                "threshold": x.get("threshold"),
                "test_metrics": x.get("test_metrics"),
            }
            for x in wf
        ],
    }

    print(
        "F2_WF_DONE " + json.dumps(summary, separators=(",", ":")),
        flush=True,
    )

    while True:
        time.sleep(3600)

if __name__ == "__main__":
    main()
