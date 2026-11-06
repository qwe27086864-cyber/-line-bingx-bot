from __future__ import annotations

import csv
import importlib
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

DATA_ROOT = Path(os.environ.get("F1_DATA_ROOT", "data/backtest_3y"))
OUT_ROOT = Path(os.environ.get("F1_OUT_ROOT", "data/f1_r0"))
OUT_ROOT.mkdir(parents=True, exist_ok=True)

SPREAD = float(os.environ.get("F1_SPREAD_PCT", "0.12"))
SEGMENTS = int(os.environ.get("F1_SEGMENTS", "37"))

# Safety: refuse to run if the fixed 3Y dataset is not complete.
manifest_path = DATA_ROOT / "manifest.json"
if not manifest_path.exists():
    raise RuntimeError(f"Missing fixed-data manifest: {manifest_path}")
manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
if not manifest.get("complete"):
    raise RuntimeError("BACKTEST-3Y dataset is not marked complete")

# Patch only the ephemeral checkout so inactive watches do not accumulate.
adapter = Path("r3_historical_adapter.py")
text = adapter.read_text(encoding="utf-8")
old = "    while scan<=end:\n        ctx=market_context(c15_by,c1h_by,scan)\n"
new = (
    "    while scan<=end:\n"
    "        if watches:\n"
    "            watches=[w for w in watches if w.get('status')=='ACTIVE']\n"
    "        ctx=market_context(c15_by,c1h_by,scan)\n"
)
if old in text:
    adapter.write_text(text.replace(old, new, 1), encoding="utf-8")

import r3_historical_adapter as r3
r3 = importlib.reload(r3)
r3.CORE = "R3_BASELINE_FOR_F1"

CANDIDATE_FIELDS = [
    "timestamp", "symbol", "original_side", "side", "trigger",
    "entry_price", "exit_time", "exit_price", "exit_reason",
    "gross_return_pct", "net_return_pct",
]
BASE_FIELDS = [
    "timestamp", "symbol", "side", "exit_time", "exit_reason",
    "net_return_pct",
]


def parse_dt(value: str) -> datetime:
    x = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def merge_maps(paths: list[Path], minutes: int):
    out = defaultdict(list)
    for path in paths:
        part = r3.read_candles(path, minutes)
        for symbol, rows in part.items():
            out[symbol].extend(rows)
    for symbol in out:
        by_t = {x["t"]: x for x in out[symbol]}
        out[symbol] = sorted(by_t.values(), key=lambda x: x["t"])
    return out


def metrics(values):
    vals = [float(x) for x in values]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl else (999.0 if gp else 0.0)

    equity = 0.0
    peak = 0.0
    max_dd = 0.0
    for x in vals:
        equity += x
        peak = max(peak, equity)
        max_dd = max(max_dd, peak - equity)

    return {
        "trades": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": round(pf, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "avg_net_return_pct": round(sum(vals) / len(vals), 4) if vals else 0.0,
        "max_drawdown_pct_points": round(max_dd, 4),
    }


def proxy_entry(c1, symbol: str, side: str, when: datetime):
    q = r3.ticker_proxy(c1, symbol, when, SPREAD)
    if not q:
        return None
    return float(q["ask"] if side == "LONG" else q["bid"])


def simulate_reverse(c1, when: datetime, symbol: str, original_side: str):
    side = "SHORT" if original_side == "LONG" else "LONG"
    entry = proxy_entry(c1, symbol, side, when)
    if entry is None:
        return None

    result = r3.simulate_exit(side, entry, when, c1.get(symbol, []))
    exit_time, exit_price, exit_reason, gross, net, _, _, _, resolved = result
    if not resolved or not exit_time:
        return None

    return {
        "timestamp": when.isoformat(),
        "symbol": symbol,
        "original_side": original_side,
        "side": side,
        "entry_price": round(entry, 12),
        "exit_time": exit_time.isoformat(),
        "exit_price": round(float(exit_price), 12),
        "exit_reason": exit_reason,
        "gross_return_pct": round(float(gross), 6),
        "net_return_pct": round(float(net), 6),
    }


def write_csv(path: Path, fields, rows):
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def read_all_rows(filename: str):
    rows = []
    for i in range(1, SEGMENTS + 1):
        path = OUT_ROOT / f"segment_{i:02d}" / filename
        if not path.exists():
            continue
        with path.open(newline="", encoding="utf-8-sig") as f:
            rows.extend(csv.DictReader(f))
    return rows


def summarize(segment_summaries):
    candidate_rows = read_all_rows("candidates.csv")
    baseline_rows = read_all_rows("baseline.csv")

    overall = {
        "baseline_r3": metrics([x["net_return_pct"] for x in baseline_rows]),
    }
    yearly = {}

    for trigger in ("REFERENCE_BREAK", "ANY_INVALIDATION", "LOSING_TRADE_EXIT"):
        rows = [x for x in candidate_rows if x["trigger"] == trigger]
        overall[trigger] = metrics([x["net_return_pct"] for x in rows])

        by_year = defaultdict(list)
        for row in rows:
            by_year[str(parse_dt(row["timestamp"]).year)].append(
                float(row["net_return_pct"])
            )
        yearly[trigger] = {
            year: metrics(vals)
            for year, vals in sorted(by_year.items())
        }

    ranking = []
    for trigger in ("REFERENCE_BREAK", "ANY_INVALIDATION", "LOSING_TRADE_EXIT"):
        m = overall[trigger]
        ranking.append({
            "trigger": trigger,
            "trades": m["trades"],
            "profit_factor": m["profit_factor"],
            "sum_net_return_pct": m["sum_net_return_pct"],
            "avg_net_return_pct": m["avg_net_return_pct"],
            "eligible_200_trades": m["trades"] >= 200,
        })
    ranking.sort(
        key=lambda x: (
            x["eligible_200_trades"],
            x["profit_factor"],
            x["sum_net_return_pct"],
        ),
        reverse=True,
    )

    final = {
        "core": "F1_R0_FAILURE_REVERSAL_RESEARCH",
        "historical_only": True,
        "data_source": "BACKTEST-3Y fixed Bybit USDT perpetual",
        "spread_pct": SPREAD,
        "exit_model": "same R3 2/4/8 TP, 2.5 SL, 4h timeout, 0.16% round-trip cost",
        "signal_edge_only": True,
        "portfolio_limits_applied_to_f1": False,
        "completed_segments": len(segment_summaries),
        "overall": overall,
        "yearly": yearly,
        "ranking": ranking,
        "segments": segment_summaries,
    }
    (OUT_ROOT / "summary.json").write_text(
        json.dumps(final, indent=2), encoding="utf-8"
    )
    return final


def run_segment(index: int):
    seg_out = OUT_ROOT / f"segment_{index:02d}"
    seg_out.mkdir(parents=True, exist_ok=True)
    done = seg_out / "summary.json"

    if done.exists():
        try:
            obj = json.loads(done.read_text(encoding="utf-8"))
            if obj.get("complete"):
                print(f"F1_SEGMENT_SKIP segment={index}/{SEGMENTS}", flush=True)
                return obj
        except Exception:
            pass

    current = DATA_ROOT / f"segment_{index:02d}"
    meta = json.loads((current / "meta.json").read_text(encoding="utf-8"))
    roots = [current]
    if index > 1:
        previous = DATA_ROOT / f"segment_{index-1:02d}"
        if previous.exists():
            roots.insert(0, previous)

    c1 = merge_maps([x / "history_1m.csv" for x in roots], 1)
    c5 = merge_maps([x / "history_5m.csv" for x in roots], 5)
    c15 = merge_maps([x / "history_15m.csv" for x in roots], 15)
    c1h = merge_maps([x / "history_1h.csv" for x in roots], 60)

    start = parse_dt(meta["start"])
    end = parse_dt(meta["end"])
    cutoff = end - timedelta(hours=4)

    print(
        f"F1_REPLAY_START segment={index}/{SEGMENTS} "
        f"start={start.isoformat()} end={end.isoformat()}",
        flush=True,
    )

    details, trades = r3.run_replay(c1, c5, c15, c1h, SPREAD)

    candidates = []
    baseline_rows = []

    for trade in trades:
        if not trade.get("resolved") or not trade.get("exit_time"):
            continue
        if not (start <= trade["entry_time"] < end):
            continue
        baseline_rows.append({
            "timestamp": trade["entry_time"].isoformat(),
            "symbol": trade["symbol"],
            "side": trade["side"],
            "exit_time": trade["exit_time"].isoformat(),
            "exit_reason": trade["exit_reason"],
            "net_return_pct": round(float(trade["net_return_pct"]), 6),
        })

    # F1A: reverse only after the old breakout reference is invalidated.
    # F1B: reverse after any setup invalidation, including over-deep pullback.
    for event in details:
        if event.get("event") != "INVALIDATED":
            continue

        when = parse_dt(event["timestamp"])
        if not (start <= when < cutoff):
            continue

        reason = str(event.get("reason") or "")
        rev = simulate_reverse(c1, when, event["symbol"], event["side"])
        if not rev:
            continue

        row = dict(rev)
        row["trigger"] = "ANY_INVALIDATION"
        candidates.append(row)

        if "reference invalidated" in reason:
            row = dict(rev)
            row["trigger"] = "REFERENCE_BREAK"
            candidates.append(row)

    # F1C: literal old losing trade -> reverse after it proves wrong.
    for trade in trades:
        if not trade.get("resolved") or not trade.get("exit_time"):
            continue
        if float(trade.get("net_return_pct") or 0) >= 0:
            continue

        when = trade["exit_time"]
        if not (start <= when < cutoff):
            continue

        rev = simulate_reverse(c1, when, trade["symbol"], trade["side"])
        if not rev:
            continue

        row = dict(rev)
        row["trigger"] = "LOSING_TRADE_EXIT"
        candidates.append(row)

    write_csv(seg_out / "candidates.csv", CANDIDATE_FIELDS, candidates)
    write_csv(seg_out / "baseline.csv", BASE_FIELDS, baseline_rows)

    variants = {}
    for trigger in ("REFERENCE_BREAK", "ANY_INVALIDATION", "LOSING_TRADE_EXIT"):
        vals = [
            float(x["net_return_pct"])
            for x in candidates
            if x["trigger"] == trigger
        ]
        variants[trigger] = metrics(vals)

    obj = {
        "segment": index,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "complete": True,
        "baseline_r3": metrics([x["net_return_pct"] for x in baseline_rows]),
        "variants": variants,
    }

    done.write_text(json.dumps(obj, indent=2), encoding="utf-8")
    print("F1_SEGMENT_DONE " + json.dumps(obj, separators=(",", ":")), flush=True)
    return obj


def main():
    print(
        f"F1_START segments={SEGMENTS} spread={SPREAD} "
        f"data_root={DATA_ROOT} research_only=true",
        flush=True,
    )

    summaries = []
    for index in range(1, SEGMENTS + 1):
        summaries.append(run_segment(index))
        interim = summarize(summaries)
        leader = interim["ranking"][0] if interim["ranking"] else {}
        print(
            "F1_PROGRESS "
            + json.dumps({"segment": index, "leader": leader}, separators=(",", ":")),
            flush=True,
        )

    final = summarize(summaries)
    final["complete"] = True
    (OUT_ROOT / "summary.json").write_text(
        json.dumps(final, indent=2), encoding="utf-8"
    )

    print(
        "F1_DONE "
        + json.dumps(
            {"overall": final["overall"], "ranking": final["ranking"]},
            separators=(",", ":"),
        ),
        flush=True,
    )

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
