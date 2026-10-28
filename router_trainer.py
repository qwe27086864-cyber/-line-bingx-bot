"""
Historical core-router trainer.

Purpose:
  Learn which EXISTING core historically performs best in each market regime.
  This is a meta/router core, not another entry strategy.

Expected feature CSV:
  timestamp,symbol,...,regime

Expected outcome CSV:
  timestamp,symbol,core,net_return_pct

The outcome timestamp should represent the decision/entry time. Matching is
backward-only to the most recent feature snapshot for the same symbol, preventing
future feature leakage.

Chronological split:
  60% train / 20% validation / 20% blind test (defaults)
"""
from __future__ import annotations

import argparse
import bisect
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

CORES = ("R2FS", "R3", "AS1", "BF1", "MEME1", "MEME2")


def dt(s: str) -> datetime:
    x = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if x.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return x.astimezone(timezone.utc)


def load_features(path: str):
    by_symbol = defaultdict(list)
    with open(path, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            by_symbol[r["symbol"]].append((dt(r["timestamp"]), r))
    for symbol in by_symbol:
        by_symbol[symbol].sort(key=lambda x: x[0])
    return by_symbol


def attach_regime(outcomes, features_by_symbol, max_age_minutes=30):
    joined = []
    max_age_sec = max_age_minutes * 60
    for o in outcomes:
        core = o["core"].upper()
        if core not in CORES:
            continue
        symbol = o["symbol"]
        arr = features_by_symbol.get(symbol)
        if not arr:
            continue
        t = dt(o["timestamp"])
        times = [x[0] for x in arr]
        idx = bisect.bisect_right(times, t) - 1
        if idx < 0:
            continue
        ft, fr = arr[idx]
        if (t - ft).total_seconds() > max_age_sec:
            continue
        joined.append({
            "timestamp": t,
            "symbol": symbol,
            "core": core,
            "regime": fr["regime"],
            "net_return_pct": float(o["net_return_pct"]),
        })
    joined.sort(key=lambda x: x["timestamp"])
    return joined


def stats(values):
    vals = list(values)
    if not vals:
        return None
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gross_win = sum(wins)
    gross_loss = -sum(losses)
    pf = gross_win / gross_loss if gross_loss > 0 else (999.0 if gross_win > 0 else 0.0)
    return {
        "n": len(vals),
        "avg_pct": sum(vals) / len(vals),
        "win_rate": len(wins) / len(vals),
        "pf": pf,
        "sum_pct": sum(vals),
    }


def train_policy(rows, min_samples=20, min_pf=1.05, min_avg_pct=0.0):
    grouped = defaultdict(list)
    for r in rows:
        grouped[(r["regime"], r["core"])].append(r["net_return_pct"])

    policy = {}
    diagnostics = {}
    regimes = sorted({r["regime"] for r in rows})
    for regime in regimes:
        candidates = []
        for core in CORES:
            s = stats(grouped.get((regime, core), []))
            if not s:
                continue
            diagnostics[f"{regime}|{core}"] = s
            if s["n"] >= min_samples and s["pf"] >= min_pf and s["avg_pct"] > min_avg_pct:
                # Transparent score, intentionally simple for V1.
                score = s["avg_pct"] * min(s["pf"], 3.0) * (0.5 + s["win_rate"])
                candidates.append((score, core, s))
        candidates.sort(reverse=True)
        policy[regime] = candidates[0][1] if candidates else "OFF"
    return policy, diagnostics


def evaluate(rows, policy):
    selected = []
    skipped = 0
    for r in rows:
        target = policy.get(r["regime"], "OFF")
        if target == "OFF":
            skipped += 1
            continue
        if r["core"] == target:
            selected.append(r["net_return_pct"])
    s = stats(selected) or {"n": 0, "avg_pct": 0.0, "win_rate": 0.0, "pf": 0.0, "sum_pct": 0.0}
    s["skipped_rows"] = skipped
    return s


def chronological_split(rows, train_frac=0.60, val_frac=0.20):
    if not rows:
        return [], [], []
    n = len(rows)
    a = max(1, int(n * train_frac))
    b = max(a, int(n * (train_frac + val_frac)))
    return rows[:a], rows[a:b], rows[b:]


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--features", required=True)
    p.add_argument("--outcomes", required=True)
    p.add_argument("--out", default="data/router_v1_report.json")
    p.add_argument("--min-samples", type=int, default=20)
    p.add_argument("--max-feature-age-min", type=int, default=30)
    args = p.parse_args()

    features = load_features(args.features)
    with open(args.outcomes, newline="", encoding="utf-8-sig") as f:
        outcomes = list(csv.DictReader(f))

    joined = attach_regime(outcomes, features, args.max_feature_age_min)
    if len(joined) < 30:
        raise ValueError(f"only {len(joined)} joined outcomes; collect/generate more before trusting router")

    train, val, test = chronological_split(joined)
    policy, diagnostics = train_policy(train, min_samples=args.min_samples)

    report = {
        "version": "HISTORICAL_ROUTER_V1",
        "cores": list(CORES),
        "split": {"train": len(train), "validation": len(val), "blind_test": len(test)},
        "policy": policy,
        "train_diagnostics": diagnostics,
        "validation": evaluate(val, policy),
        "blind_test": evaluate(test, policy),
        "safety": {
            "live_orders": False,
            "writes_existing_core_tables": False,
            "default_when_unknown": "OFF",
        },
        "limitations": [
            "V1 learns only regime-to-core routing; it does not modify any core.",
            "Core outcomes must be generated with realistic fees/slippage and no future-data leakage.",
            "Meme and market-making cores require higher-fidelity historical inputs than ordinary OHLCV.",
            "A positive backtest is not sufficient for live deployment; forward shadow validation remains required.",
        ],
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps({
        "policy": policy,
        "validation": report["validation"],
        "blind_test": report["blind_test"],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
