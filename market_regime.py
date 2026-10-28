"""
Leakage-safe market feature builder + simple regime labeler.

Only current/past candles are used. No future-return columns are used to create
features or regime labels.

Input CSV: output of historical_data.py
Output CSV: one row per symbol/candle after warmup
"""
from __future__ import annotations

import argparse
import csv
import math
import statistics
from collections import defaultdict, deque
from pathlib import Path


OUT_FIELDS = [
    "timestamp", "symbol", "interval", "close",
    "ret_fast_pct", "ret_slow_pct", "atr_pct", "volatility_pct",
    "volume_ratio", "trend_strength", "regime",
]


def pct(a: float, b: float) -> float:
    return (a / b - 1.0) * 100.0 if b else 0.0


def stdev(values) -> float:
    values = list(values)
    return statistics.pstdev(values) if len(values) >= 2 else 0.0


def classify(ret_fast: float, ret_slow: float, atr: float, vol: float, trend_strength: float) -> str:
    # Descriptive V1 buckets only. The router is trained on outcomes separately.
    if ret_fast <= -4.0 and atr >= 1.0:
        return "CRASH"
    if ret_fast >= 4.0 and atr >= 1.0:
        return "SURGE"
    if ret_fast > 1.0 and ret_slow > 2.0 and trend_strength >= 0.55:
        return "TREND_UP"
    if ret_fast < -1.0 and ret_slow < -2.0 and trend_strength >= 0.55:
        return "TREND_DOWN"
    if abs(ret_slow) <= 1.5 and atr >= 1.0:
        return "RANGE_HIGH_VOL"
    if abs(ret_slow) <= 1.5 and atr < 1.0:
        return "RANGE_LOW_VOL"
    if vol >= 1.5:
        return "VOLATILE_TRANSITION"
    return "TRANSITION"


def build_features(rows, fast=12, slow=48, atr_n=24, vol_n=24, volume_n=24):
    by_symbol = defaultdict(list)
    for r in rows:
        by_symbol[r["symbol"]].append(r)
    output = []

    warmup = max(slow, atr_n + 1, vol_n + 1, volume_n)
    for symbol, candles in by_symbol.items():
        candles.sort(key=lambda r: r["timestamp"])
        closes = deque(maxlen=slow + 1)
        true_ranges = deque(maxlen=atr_n)
        rets = deque(maxlen=vol_n)
        volumes = deque(maxlen=volume_n)
        prev_close = None

        for r in candles:
            o, h, l, c, v = (float(r[k]) for k in ("open", "high", "low", "close", "volume"))
            if min(o, h, l, c) <= 0:
                continue

            if prev_close is None:
                tr = h - l
            else:
                tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
                rets.append(pct(c, prev_close))
            true_ranges.append(tr)
            closes.append(c)

            if len(closes) >= 2:
                prev_vol_mean = statistics.fmean(volumes) if volumes else 0.0
                volumes.append(v)

                if len(closes) < warmup + 1:
                    prev_close = c
                    continue

                close_list = list(closes)
                ret_fast = pct(c, close_list[-fast - 1])
                ret_slow = pct(c, close_list[-slow - 1])
                atr_pct = statistics.fmean(true_ranges) / c * 100.0
                volatility_pct = stdev(rets)
                volume_ratio = (v / prev_vol_mean) if prev_vol_mean > 0 else 1.0

                # Fraction of last fast return signs agreeing with slow trend direction.
                rlist = list(rets)[-min(fast, len(rets)):]
                direction = 1 if ret_slow >= 0 else -1
                agreeing = sum(1 for x in rlist if (x >= 0) == (direction > 0))
                trend_strength = agreeing / len(rlist) if rlist else 0.0

                regime = classify(ret_fast, ret_slow, atr_pct, volatility_pct, trend_strength)
                output.append({
                    "timestamp": r["timestamp"],
                    "symbol": symbol,
                    "interval": r.get("interval", ""),
                    "close": round(c, 12),
                    "ret_fast_pct": round(ret_fast, 6),
                    "ret_slow_pct": round(ret_slow, 6),
                    "atr_pct": round(atr_pct, 6),
                    "volatility_pct": round(volatility_pct, 6),
                    "volume_ratio": round(volume_ratio, 6),
                    "trend_strength": round(trend_strength, 6),
                    "regime": regime,
                })
            else:
                volumes.append(v)

            prev_close = c

    output.sort(key=lambda x: (x["timestamp"], x["symbol"]))
    return output


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--candles", required=True)
    p.add_argument("--out", default="data/market_features.csv")
    args = p.parse_args()

    with open(args.candles, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise ValueError("empty candle CSV")

    result = build_features(rows)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_FIELDS)
        w.writeheader()
        w.writerows(result)
    print(f"wrote {len(result)} feature rows -> {out}")


if __name__ == "__main__":
    main()
