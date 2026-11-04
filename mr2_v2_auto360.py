from __future__ import annotations

import csv
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

CORE = "MR2_V3_STABLE_RANGE_ROTATION"
SYMBOLS = [
    x.strip().upper()
    for x in os.getenv(
        "MR2_SYMBOLS",
        "BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT,LTC-USDT,TRX-USDT,BCH-USDT,DOT-USDT,UNI-USDT",
    ).split(",")
    if x.strip()
]

SEGMENTS = int(os.getenv("MR2_SEGMENTS", "12"))
DAYS = int(os.getenv("MR2_SEGMENT_DAYS", "30"))
END = os.getenv("MR2_END", "2026-11-02T11:00:00Z")
ROOT = Path(os.getenv("MR2_OUT_DIR", "data/mr2_v3_auto360"))
ROOT.mkdir(parents=True, exist_ok=True)

CACHE_BASES = [
    x.strip().rstrip("/")
    for x in os.getenv(
        "MR2_CACHE_BASES",
        "https://lq1-auto360.onrender.com,https://mr1-mean-reversion-auto360.onrender.com",
    ).split(",")
    if x.strip()
]

MIN_COVERAGE = float(os.getenv("MR2_MIN_COVERAGE", "0.95"))
COST = float(os.getenv("MR2_ROUND_TRIP_COST_PCT", "0.12"))

# ---------- Mature stable-range regime ----------
RANGE_N = int(os.getenv("MR2_RANGE_N", "96"))
ATR_N = int(os.getenv("MR2_ATR_N", "24"))
ER_N = int(os.getenv("MR2_ER_N", "48"))
CROSS_N = int(os.getenv("MR2_CROSS_N", "48"))
VOL_N = int(os.getenv("MR2_VOL_N", "24"))

MAX_ER = float(os.getenv("MR2_MAX_ER", "0.27"))
MIN_RANGE_ATR = float(os.getenv("MR2_MIN_RANGE_ATR", "3.0"))
MAX_RANGE_ATR = float(os.getenv("MR2_MAX_RANGE_ATR", "9.5"))
MAX_MA_SEP_ATR = float(os.getenv("MR2_MAX_MA_SEP_ATR", "0.38"))
MIN_MEAN_CROSSES = int(os.getenv("MR2_MIN_MEAN_CROSSES", "3"))

# Range maturity/stability
MAX_EDGE_DRIFT_ATR = float(os.getenv("MR2_MAX_EDGE_DRIFT_ATR", "0.65"))
MAX_ATR_EXPANSION = float(os.getenv("MR2_MAX_ATR_EXPANSION", "1.35"))
MAX_15M_SLOPE_ATR = float(os.getenv("MR2_MAX_15M_SLOPE_ATR", "0.42"))
MAX_1H_SLOPE_ATR = float(os.getenv("MR2_MAX_1H_SLOPE_ATR", "0.62"))

# Range touches / edge ownership
TOUCH_TOL_ATR = float(os.getenv("MR2_TOUCH_TOL_ATR", "0.25"))
MIN_TOUCHES = int(os.getenv("MR2_MIN_TOUCHES", "2"))
EDGE_FRAC = float(os.getenv("MR2_EDGE_FRAC", "0.18"))
MAX_OUTSIDE_ATR = float(os.getenv("MR2_MAX_OUTSIDE_ATR", "0.08"))

# Edge-pressure filter
PRESSURE_N = int(os.getenv("MR2_PRESSURE_N", "8"))
MAX_EDGE_PRESSURE = int(os.getenv("MR2_MAX_EDGE_PRESSURE", "4"))
PRESSURE_EDGE_FRAC = float(os.getenv("MR2_PRESSURE_EDGE_FRAC", "0.20"))

# Rejection quality
REV_BODY_MIN = float(os.getenv("MR2_REV_BODY_MIN", "0.22"))
REJECTION_WICK_MIN = float(os.getenv("MR2_REJECTION_WICK_MIN", "0.25"))
REJECTION_VOL_MIN = float(os.getenv("MR2_REJECTION_VOL_MIN", "0.60"))
REJECTION_VOL_MAX = float(os.getenv("MR2_REJECTION_VOL_MAX", "1.65"))

# Confirmation
SECOND_HOLD_BARS = int(os.getenv("MR2_SECOND_HOLD_BARS", "3"))
SECOND_HOLD_FRAC = float(os.getenv("MR2_SECOND_HOLD_FRAC", "0.10"))
SECOND_VOL_MAX = float(os.getenv("MR2_SECOND_VOL_MAX", "1.50"))

# Risk / exits
STOP_BUF_ATR = float(os.getenv("MR2_STOP_BUF_ATR", "0.13"))
MIN_STOP_PCT = float(os.getenv("MR2_MIN_STOP_PCT", "0.12"))
MAX_STOP_PCT = float(os.getenv("MR2_MAX_STOP_PCT", "1.35"))
TP1_RANGE_FRAC = float(os.getenv("MR2_TP1_RANGE_FRAC", "0.38"))
TP1_WEIGHT = float(os.getenv("MR2_TP1_WEIGHT", "0.60"))
TP2_WEIGHT = 1.0 - TP1_WEIGHT
MIN_TP1_R = float(os.getenv("MR2_MIN_TP1_R", "0.75"))
MIN_MID_R = float(os.getenv("MR2_MIN_MID_R", "1.20"))
HOLD_BARS = int(os.getenv("MR2_HOLD_BARS", "36"))
COOLDOWN_BARS = int(os.getenv("MR2_COOLDOWN_BARS", "18"))


def dt(s):
    x = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    return x if x.tzinfo else x.replace(tzinfo=timezone.utc)


def mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def tr(b, pc):
    return max(
        b["high"] - b["low"],
        abs(b["high"] - pc),
        abs(b["low"] - pc),
    )


def pct(a, b):
    return (a / b - 1.0) * 100.0 if b else 0.0


def body_frac(b):
    rng = max(b["high"] - b["low"], 1e-12)
    return abs(b["close"] - b["open"]) / rng


def close_pos(b):
    rng = max(b["high"] - b["low"], 1e-12)
    return (b["close"] - b["low"]) / rng


def lower_wick_frac(b):
    rng = max(b["high"] - b["low"], 1e-12)
    return (min(b["open"], b["close"]) - b["low"]) / rng


def upper_wick_frac(b):
    rng = max(b["high"] - b["low"], 1e-12)
    return (b["high"] - max(b["open"], b["close"])) / rng


def load(path):
    by = defaultdict(list)

    with path.open(newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            try:
                by[r["symbol"].upper()].append(
                    {
                        "t": dt(r["timestamp"]),
                        "open": float(r["open"]),
                        "high": float(r["high"]),
                        "low": float(r["low"]),
                        "close": float(r["close"]),
                        "vol": float(r.get("quote_volume") or 0)
                        or float(r.get("volume") or 0) * float(r["close"]),
                    }
                )
            except Exception:
                pass

    for symbol in by:
        by[symbol].sort(key=lambda z: z["t"])

    return by


def atr(rows, i, n=ATR_N):
    return mean(
        [
            tr(rows[j], rows[j - 1]["close"])
            for j in range(i - n, i)
        ]
    )


def er(rows, i, n):
    net = abs(rows[i - 1]["close"] - rows[i - n]["close"])
    den = sum(
        abs(rows[j]["close"] - rows[j - 1]["close"])
        for j in range(i - n + 1, i)
    )
    return net / den if den else 1.0


def crossings(vals, level):
    return sum(
        1
        for a, b in zip(vals, vals[1:])
        if (a - level) * (b - level) < 0
    )


def sampled_slope(rows, i, step, count):
    vals = [
        rows[i - step * k - 1]["close"]
        for k in range(count)
    ][::-1]
    return vals[-1] - vals[0] if len(vals) >= 2 else 0.0


def regime(rows, i):
    need = max(RANGE_N, ATR_N, ER_N, CROSS_N, VOL_N, 120) + 3
    if i < need:
        return None

    base = rows[i - RANGE_N:i]
    a = atr(rows, i)
    if a <= 0:
        return None

    hi = max(x["high"] for x in base)
    lo = min(x["low"] for x in base)
    width = hi - lo
    if width <= 0:
        return None

    wr = width / a
    e = er(rows, i, ER_N)

    fast = mean([x["close"] for x in rows[i - 12:i]])
    slow = mean([x["close"] for x in rows[i - 48:i]])
    ma_sep = abs(fast - slow) / a

    mid = (hi + lo) / 2
    vals = [x["close"] for x in rows[i - CROSS_N:i]]
    cr = crossings(vals, mid)

    # Compare first and second half of the established range.
    first = base[: RANGE_N // 2]
    second = base[RANGE_N // 2:]
    hi1 = max(x["high"] for x in first)
    lo1 = min(x["low"] for x in first)
    hi2 = max(x["high"] for x in second)
    lo2 = min(x["low"] for x in second)
    edge_drift = max(abs(hi2 - hi1), abs(lo2 - lo1)) / a

    short_atr = atr(rows, i, 12)
    long_atr = atr(rows, i, 48)
    atr_expansion = short_atr / long_atr if long_atr > 0 else 999.0

    slope15 = abs(sampled_slope(rows, i, 3, 5)) / a
    slope1h = abs(sampled_slope(rows, i, 12, 4)) / a

    lt = sum(
        1
        for x in base[:-4]
        if x["low"] <= lo + TOUCH_TOL_ATR * a
    )
    ht = sum(
        1
        for x in base[:-4]
        if x["high"] >= hi - TOUCH_TOL_ATR * a
    )

    if e > MAX_ER:
        return None
    if ma_sep > MAX_MA_SEP_ATR:
        return None
    if not (MIN_RANGE_ATR <= wr <= MAX_RANGE_ATR):
        return None
    if cr < MIN_MEAN_CROSSES:
        return None
    if lt < MIN_TOUCHES or ht < MIN_TOUCHES:
        return None
    if edge_drift > MAX_EDGE_DRIFT_ATR:
        return None
    if atr_expansion > MAX_ATR_EXPANSION:
        return None
    if slope15 > MAX_15M_SLOPE_ATR:
        return None
    if slope1h > MAX_1H_SLOPE_ATR:
        return None

    vavg = mean(
        [rows[j]["vol"] for j in range(i - VOL_N, i)]
    )

    return {
        "hi": hi,
        "lo": lo,
        "mid": mid,
        "width": width,
        "atr": a,
        "er": e,
        "crosses": cr,
        "lt": lt,
        "ht": ht,
        "edge_drift": edge_drift,
        "atr_expansion": atr_expansion,
        "slope15": slope15,
        "slope1h": slope1h,
        "vavg": vavg,
    }


def edge_pressure(rows, i, rg):
    recent = rows[i - PRESSURE_N:i]
    low_edge = rg["lo"] + PRESSURE_EDGE_FRAC * rg["width"]
    high_edge = rg["hi"] - PRESSURE_EDGE_FRAC * rg["width"]

    low_pressure = sum(
        1 for x in recent if x["close"] <= low_edge
    )
    high_pressure = sum(
        1 for x in recent if x["close"] >= high_edge
    )

    return low_pressure, high_pressure


def setup(rows, i):
    rg = regime(rows, i)
    if not rg:
        return None

    b = rows[i]
    edge = EDGE_FRAC * rg["width"]

    if (
        b["close"] < rg["lo"] - MAX_OUTSIDE_ATR * rg["atr"]
        or b["close"] > rg["hi"] + MAX_OUTSIDE_ATR * rg["atr"]
    ):
        return None

    vr = b["vol"] / rg["vavg"] if rg["vavg"] > 0 else 0
    if not (REJECTION_VOL_MIN <= vr <= REJECTION_VOL_MAX):
        return None

    low_pressure, high_pressure = edge_pressure(rows, i, rg)

    # Long: range low rejection, but not while price is repeatedly pressing the low.
    if (
        low_pressure <= MAX_EDGE_PRESSURE
        and b["low"] <= rg["lo"] + edge
        and b["close"] > b["open"]
        and body_frac(b) >= REV_BODY_MIN
        and lower_wick_frac(b) >= REJECTION_WICK_MIN
        and close_pos(b) >= 0.60
    ):
        return {
            **rg,
            "side": "LONG",
            "ext": b["low"],
            "setup_i": i,
            "setup_vr": vr,
            "edge_pressure": low_pressure,
        }

    # Short: range high rejection, but not while price is repeatedly pressing the high.
    if (
        high_pressure <= MAX_EDGE_PRESSURE
        and b["high"] >= rg["hi"] - edge
        and b["close"] < b["open"]
        and body_frac(b) >= REV_BODY_MIN
        and upper_wick_frac(b) >= REJECTION_WICK_MIN
        and close_pos(b) <= 0.40
    ):
        return {
            **rg,
            "side": "SHORT",
            "ext": b["high"],
            "setup_i": i,
            "setup_vr": vr,
            "edge_pressure": high_pressure,
        }

    return None


def second_hold(rows, s):
    for j in range(
        s["setup_i"] + 1,
        min(
            len(rows),
            s["setup_i"] + 1 + SECOND_HOLD_BARS,
        ),
    ):
        b = rows[j]
        vr = b["vol"] / s["vavg"] if s["vavg"] > 0 else 0

        if vr > SECOND_VOL_MAX:
            continue

        if s["side"] == "LONG":
            s["ext"] = min(s["ext"], b["low"])

            if b["close"] < s["lo"] - MAX_OUTSIDE_ATR * s["atr"]:
                return None

            if (
                b["close"] > s["lo"] + SECOND_HOLD_FRAC * s["width"]
                and close_pos(b) >= 0.52
            ):
                return j, b["close"], vr

        else:
            s["ext"] = max(s["ext"], b["high"])

            if b["close"] > s["hi"] + MAX_OUTSIDE_ATR * s["atr"]:
                return None

            if (
                b["close"] < s["hi"] - SECOND_HOLD_FRAC * s["width"]
                and close_pos(b) <= 0.48
            ):
                return j, b["close"], vr

    return None


def simulate(rows, ei, entry, s):
    side = s["side"]

    tp1 = (
        s["lo"] + TP1_RANGE_FRAC * s["width"]
        if side == "LONG"
        else s["hi"] - TP1_RANGE_FRAC * s["width"]
    )
    tp2 = s["mid"]

    stop = (
        s["ext"] - STOP_BUF_ATR * s["atr"]
        if side == "LONG"
        else s["ext"] + STOP_BUF_ATR * s["atr"]
    )

    sp = abs(pct(stop, entry))
    risk = abs(entry - stop)

    d1 = (
        tp1 - entry
        if side == "LONG"
        else entry - tp1
    )
    d2 = (
        tp2 - entry
        if side == "LONG"
        else entry - tp2
    )

    if (
        risk <= 0
        or d1 <= 0
        or d2 <= d1
        or not (MIN_STOP_PCT <= sp <= MAX_STOP_PCT)
    ):
        return None

    r1 = d1 / risk
    r2 = d2 / risk

    if r1 < MIN_TP1_R or r2 < MIN_MID_R:
        return None

    hit_tp1 = False
    last = entry
    reason = "TIMEOUT"
    ret = None

    for j in range(
        ei + 1,
        min(len(rows), ei + 1 + HOLD_BARS),
    ):
        b = rows[j]
        last = b["close"]

        if side == "LONG":
            active_stop = entry if hit_tp1 else stop

            if b["low"] <= active_stop:
                if hit_tp1:
                    ret = TP1_WEIGHT * pct(tp1, entry)
                    reason = "BE_AFTER_TP1"
                else:
                    ret = -sp
                    reason = "STOP"
                break

            if not hit_tp1 and b["high"] >= tp1:
                hit_tp1 = True

            if b["high"] >= tp2:
                ret = (
                    TP1_WEIGHT * pct(tp1, entry)
                    + TP2_WEIGHT * pct(tp2, entry)
                )
                reason = "MID_TARGET"
                break

            if b["close"] < s["lo"] - MAX_OUTSIDE_ATR * s["atr"]:
                raw = pct(b["close"], entry)
                ret = (
                    TP1_WEIGHT * pct(tp1, entry)
                    + TP2_WEIGHT * raw
                    if hit_tp1
                    else raw
                )
                reason = "RANGE_LOST"
                break

        else:
            active_stop = entry if hit_tp1 else stop

            if b["high"] >= active_stop:
                if hit_tp1:
                    ret = TP1_WEIGHT * pct(entry, tp1)
                    reason = "BE_AFTER_TP1"
                else:
                    ret = -sp
                    reason = "STOP"
                break

            if not hit_tp1 and b["low"] <= tp1:
                hit_tp1 = True

            if b["low"] <= tp2:
                ret = (
                    TP1_WEIGHT * pct(entry, tp1)
                    + TP2_WEIGHT * pct(entry, tp2)
                )
                reason = "MID_TARGET"
                break

            if b["close"] > s["hi"] + MAX_OUTSIDE_ATR * s["atr"]:
                raw = pct(entry, b["close"])
                ret = (
                    TP1_WEIGHT * pct(entry, tp1)
                    + TP2_WEIGHT * raw
                    if hit_tp1
                    else raw
                )
                reason = "RANGE_LOST"
                break

    if ret is None:
        raw = (
            pct(last, entry)
            if side == "LONG"
            else pct(entry, last)
        )
        ret = (
            TP1_WEIGHT
            * (
                pct(tp1, entry)
                if side == "LONG"
                else pct(entry, tp1)
            )
            + TP2_WEIGHT * raw
            if hit_tp1
            else raw
        )

    return {
        "net": ret - COST,
        "reason": reason,
        "stop_pct": sp,
        "tp1_r": r1,
        "mid_r": r2,
    }


def run_symbol(rows):
    out = []
    cool = -1

    for i in range(len(rows)):
        if i <= cool:
            continue

        s = setup(rows, i)
        if not s:
            continue

        c = second_hold(rows, s)
        if not c:
            continue

        ei, entry, confirm_vr = c
        x = simulate(rows, ei, entry, s)

        if not x:
            continue

        out.append(
            {
                "side": s["side"],
                "entry_time": rows[ei]["t"].isoformat(),
                "net": x["net"],
                "reason": x["reason"],
                "stop_pct": x["stop_pct"],
                "tp1_r": x["tp1_r"],
                "mid_r": x["mid_r"],
                "er": s["er"],
                "crosses": s["crosses"],
                "edge_drift": s["edge_drift"],
                "atr_expansion": s["atr_expansion"],
                "slope15": s["slope15"],
                "slope1h": s["slope1h"],
                "setup_vr": s["setup_vr"],
                "confirm_vr": confirm_vr,
                "edge_pressure": s["edge_pressure"],
            }
        )

        cool = ei + COOLDOWN_BARS

    return out


def metrics(ts):
    vals = [x["net"] for x in ts]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]

    gp = sum(wins)
    gl = abs(sum(losses))

    return {
        "trades": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (
            round(100 * len(wins) / len(vals), 2)
            if vals
            else 0
        ),
        "profit_factor": (
            round(gp / gl, 6)
            if gl
            else (999 if gp else 0)
        ),
        "gross_profit_pct_points": round(gp, 6),
        "gross_loss_abs_pct_points": round(gl, 6),
        "sum_net_return_pct": round(sum(vals), 6),
    }


def fetch(idx):
    sd = ROOT / f"segment_{idx:02d}"
    sd.mkdir(parents=True, exist_ok=True)
    path = sd / "history_5m.csv"

    last = None

    for base in CACHE_BASES:
        for attempt in range(4):
            try:
                req = Request(
                    f"{base}/segment_{idx:02d}/history_5m.csv",
                    headers={"User-Agent": "mr2-v3/1.0"},
                )

                with urlopen(req, timeout=60) as r:
                    data = r.read()

                if len(data) > 100000:
                    path.write_bytes(data)
                else:
                    raise RuntimeError(
                        f"small cache {len(data)}"
                    )

                by = load(path)
                expected = int(DAYS * 24 * 12)
                minimum = int(expected * MIN_COVERAGE)

                counts = {
                    s: len(by.get(s, []))
                    for s in SYMBOLS
                }

                if any(
                    n < minimum
                    for n in counts.values()
                ):
                    raise RuntimeError(
                        f"coverage {counts}"
                    )

                print(
                    "MR2V3_CACHE_OK "
                    + json.dumps(
                        {
                            "segment": idx,
                            "base": base,
                            "counts": counts,
                        },
                        separators=(",", ":"),
                    ),
                    flush=True,
                )

                return path

            except Exception as e:
                last = e
                time.sleep(
                    min(3 * (attempt + 1), 10)
                )

    raise RuntimeError(
        f"cache unavailable {idx}: {last}"
    )


def main():
    end = dt(END)
    start = end - timedelta(
        days=SEGMENTS * DAYS
    )

    rows = []
    all_trades = []

    print(
        "MR2V3_START "
        + json.dumps(
            {
                "core": CORE,
                "historical_only": True,
                "architecture": (
                    "stable_mature_range_then_no_atr_expansion_"
                    "then_no_multiframe_trend_then_no_edge_pressure_"
                    "then_rejection_then_confirmation_then_partial_mean_rotation"
                ),
            },
            separators=(",", ":"),
        ),
        flush=True,
    )

    for idx in range(1, SEGMENTS + 1):
        by = load(fetch(idx))
        ts = []

        for sym in SYMBOLS:
            q = run_symbol(
                by.get(sym, [])
            )

            for x in q:
                x["symbol"] = sym

            ts += q

        z = metrics(ts)

        row = {
            "segment": idx,
            "start": (
                start
                + timedelta(
                    days=(idx - 1) * DAYS
                )
            ).isoformat(),
            "metrics": z,
        }

        rows.append(row)
        all_trades += ts

        print(
            "MR2V3_SEGMENT_DONE "
            + json.dumps(
                row,
                separators=(",", ":"),
            ),
            flush=True,
        )

    final = {
        "core": CORE,
        **metrics(all_trades),
        "positive_segments": sum(
            r["metrics"]["sum_net_return_pct"] > 0
            for r in rows
        ),
        "segments": SEGMENTS,
        "first_screen_pass": False,
        "segment_results": rows,
    }

    final["first_screen_pass"] = bool(
        final["trades"] >= 80
        and final["profit_factor"] >= 1.15
        and final["sum_net_return_pct"] > 0
        and final["positive_segments"] >= 7
    )

    (ROOT / "mr2_v3_final.json").write_text(
        json.dumps(final, indent=2)
    )

    fields = [
        "symbol",
        "side",
        "entry_time",
        "net",
        "reason",
        "stop_pct",
        "tp1_r",
        "mid_r",
        "er",
        "crosses",
        "edge_drift",
        "atr_expansion",
        "slope15",
        "slope1h",
        "setup_vr",
        "confirm_vr",
        "edge_pressure",
    ]

    with (
        ROOT / "mr2_v3_trades.csv"
    ).open(
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fields,
        )
        writer.writeheader()

        for x in all_trades:
            writer.writerow(
                {
                    k: x.get(k)
                    for k in fields
                }
            )

    print(
        "MR2V3_DONE "
        + json.dumps(
            final,
            separators=(",", ":"),
        ),
        flush=True,
    )

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
