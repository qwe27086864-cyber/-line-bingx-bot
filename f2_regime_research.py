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

def parse_dt(v):
    x = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)

def ema(vals, n):
    if not vals:
        return None
    a = 2.0 / (n + 1.0)
    e = vals[0]
    for x in vals[1:]:
        e = a * x + (1.0 - a) * e
    return e

def pct(a, b):
    return (a / b - 1.0) * 100.0 if b else 0.0

def metrics(rows):
    vals = [float(r["net_return_pct"]) for r in rows]
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
    for r in rows:
        segs[int(r["segment"])] += float(r["net_return_pct"])
    pos = sum(v > 0 for v in segs.values())
    return {
        "trades": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": round(pf, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "avg_net_return_pct": round(sum(vals) / len(vals), 4) if vals else 0.0,
        "max_drawdown_pct_points": round(dd, 4),
        "positive_segments": pos,
        "active_segments": len(segs),
    }

def load_f1_candidates():
    any_rows = []
    ref_keys = set()
    for i in range(1, SEGMENTS + 1):
        p = F1_ROOT / f"segment_{i:02d}" / "candidates.csv"
        if not p.exists():
            continue
        with p.open(newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                r["segment"] = i
                key = (r["timestamp"], r["symbol"], r["original_side"], r["side"], r["exit_time"], r["net_return_pct"])
                if r["trigger"] == "REFERENCE_BREAK":
                    ref_keys.add(key)
                elif r["trigger"] == "ANY_INVALIDATION":
                    any_rows.append((key, r))
    out = []
    seen = set()
    for key, r in any_rows:
        if key in seen:
            continue
        seen.add(key)
        x = dict(r)
        x["event_type"] = "REFERENCE" if key in ref_keys else "DEEP_PULLBACK"
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
        out[s] = {"t": [x[0] for x in rows], "c": [x[1] for x in rows]}
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
    rets = [pct(cs[k], cs[k - 1]) for k in range(max(1, j - 23), j + 1)]
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

def subset(rows, rule):
    out = []
    for r in rows:
        if rule["event_type"] != "ALL" and r["event_type"] != rule["event_type"]:
            continue
        if rule["side"] != "BOTH" and r["side"] != rule["side"]:
            continue
        if int(r["reverse_market_alignment"]) < rule["align_min"]:
            continue
        if float(r["market_6h_directional_pct"]) < rule["ret6_min"]:
            continue
        if float(r["market_24h_directional_pct"]) < rule["ret24_min"]:
            continue
        out.append(r)
    return out

def score_rule(mt, mv):
    if mt["trades"] < 80 or mv["trades"] < 35:
        return -1e9
    a = min(mt["profit_factor"], 5.0)
    b = min(mv["profit_factor"], 5.0)
    score = min(a, b)
    score += 0.002 * (mt["sum_net_return_pct"] + mv["sum_net_return_pct"])
    score += 0.03 * (mt["positive_segments"] + mv["positive_segments"])
    if mt["sum_net_return_pct"] <= 0:
        score -= 1.0
    if mv["sum_net_return_pct"] <= 0:
        score -= 1.0
    return score

def main():
    print("F2_START source=F1_R0 train=1-18 validation=19-27 oos=28-37", flush=True)
    if not (DATA_ROOT / "manifest.json").exists():
        raise RuntimeError("missing fixed 3Y dataset")
    if not F1_ROOT.exists():
        raise RuntimeError("missing F1 output")

    base = load_f1_candidates()
    print(f"F2_F1_ROWS unique_any_invalidation={len(base)}", flush=True)

    m15 = load_market("15m")
    m1h = load_market("1h")
    rows = enrich(base, m15, m1h)
    print(f"F2_FEATURE_ROWS enriched={len(rows)}", flush=True)

    train = [r for r in rows if int(r["segment"]) <= 18]
    valid = [r for r in rows if 19 <= int(r["segment"]) <= 27]
    oos = [r for r in rows if int(r["segment"]) >= 28]

    rules = []
    for event_type in ("ALL", "REFERENCE", "DEEP_PULLBACK"):
        for side in ("BOTH", "LONG", "SHORT"):
            for align_min in (0, 1, 2, 3, 4):
                for ret6_min in (-999.0, 0.0, 0.15, 0.30):
                    for ret24_min in (-999.0, 0.0, 0.50, 1.00):
                        rule = {
                            "event_type": event_type,
                            "side": side,
                            "align_min": align_min,
                            "ret6_min": ret6_min,
                            "ret24_min": ret24_min,
                        }
                        mt = metrics(subset(train, rule))
                        mv = metrics(subset(valid, rule))
                        rules.append({
                            "rule": rule,
                            "train": mt,
                            "validation": mv,
                            "selection_score": score_rule(mt, mv),
                        })

    rules.sort(key=lambda x: x["selection_score"], reverse=True)
    shortlist = rules[:30]
    final_rows = []

    for item in shortlist:
        rule = item["rule"]
        mo = metrics(subset(oos, rule))
        ma = metrics(subset(rows, rule))
        x = dict(item)
        x["oos"] = mo
        x["overall"] = ma
        x["strict_pass"] = bool(
            item["train"]["trades"] >= 80
            and item["validation"]["trades"] >= 35
            and mo["trades"] >= 35
            and ma["trades"] >= 200
            and item["train"]["profit_factor"] >= 1.0
            and item["validation"]["profit_factor"] >= 1.0
            and mo["profit_factor"] >= 1.0
            and item["train"]["sum_net_return_pct"] > 0
            and item["validation"]["sum_net_return_pct"] > 0
            and mo["sum_net_return_pct"] > 0
        )
        final_rows.append(x)

    best = next((x for x in final_rows if x["strict_pass"]), final_rows[0] if final_rows else None)

    result = {
        "core": "F2_R0_REGIME_FILTERED_FAILURE_REVERSAL",
        "historical_only": True,
        "selection_policy": "1-18 train, 19-27 validation; 28-37 held-out OOS and never used for ranking",
        "candidate_source": "unique F1 ANY_INVALIDATION reversals",
        "base_rows": len(base),
        "enriched_rows": len(rows),
        "train_rows": len(train),
        "validation_rows": len(valid),
        "oos_rows": len(oos),
        "strict_pass_count_in_shortlist": sum(x["strict_pass"] for x in final_rows),
        "best": best,
        "shortlist": final_rows,
    }

    (OUT_ROOT / "f2_final.json").write_text(json.dumps(result, indent=2), encoding="utf-8")

    with (OUT_ROOT / "top_rules.csv").open("w", newline="", encoding="utf-8") as f:
        fields = [
            "rank","event_type","side","align_min","ret6_min","ret24_min","selection_score",
            "train_trades","train_pf","train_net","val_trades","val_pf","val_net",
            "oos_trades","oos_pf","oos_net","overall_trades","overall_pf","overall_net","strict_pass",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for rank, x in enumerate(final_rows, 1):
            q=x["rule"]; tr=x["train"]; va=x["validation"]; oo=x["oos"]; al=x["overall"]
            w.writerow({
                "rank":rank, **q, "selection_score":round(x["selection_score"],6),
                "train_trades":tr["trades"],"train_pf":tr["profit_factor"],"train_net":tr["sum_net_return_pct"],
                "val_trades":va["trades"],"val_pf":va["profit_factor"],"val_net":va["sum_net_return_pct"],
                "oos_trades":oo["trades"],"oos_pf":oo["profit_factor"],"oos_net":oo["sum_net_return_pct"],
                "overall_trades":al["trades"],"overall_pf":al["profit_factor"],"overall_net":al["sum_net_return_pct"],
                "strict_pass":x["strict_pass"],
            })

    print("F2_DONE " + json.dumps({"strict_pass_count": result["strict_pass_count_in_shortlist"], "best": best}, separators=(",",":")), flush=True)

    while True:
        time.sleep(3600)

if __name__ == "__main__":
    main()
