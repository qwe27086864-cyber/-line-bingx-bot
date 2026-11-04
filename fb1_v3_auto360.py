from __future__ import annotations
import csv, json, os, time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

CORE = "FB1_V4_LIQUIDITY_SWEEP_REVERSAL"
SYMBOLS = [x.strip().upper() for x in os.getenv(
    "FB1_SYMBOLS",
    "BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT,LTC-USDT,TRX-USDT,BCH-USDT,DOT-USDT,UNI-USDT"
).split(",") if x.strip()]
SEGMENTS = int(os.getenv("FB1_SEGMENTS", "12"))
DAYS = int(os.getenv("FB1_SEGMENT_DAYS", "30"))
END = os.getenv("FB1_END", "2026-11-02T11:00:00Z")
ROOT = Path(os.getenv("FB1_OUT_DIR", "data/fb1_v4_auto360"))
ROOT.mkdir(parents=True, exist_ok=True)
CACHE_BASES = [x.strip().rstrip("/") for x in os.getenv(
    "FB1_CACHE_BASES",
    "https://lq1-auto360.onrender.com,https://mr1-mean-reversion-auto360.onrender.com"
).split(",") if x.strip()]
MIN_COVERAGE = float(os.getenv("FB1_MIN_COVERAGE", "0.95"))
COST = float(os.getenv("FB1_ROUND_TRIP_COST_PCT", "0.12"))

# ---- Range / regime filters ----
RANGE_N = int(os.getenv("FB1_RANGE_N", "72"))
ATR_N = int(os.getenv("FB1_ATR_N", "24"))
ER_N = int(os.getenv("FB1_ER_N", "48"))
VOL_N = int(os.getenv("FB1_VOL_N", "24"))

MAX_ER = float(os.getenv("FB1_MAX_ER", "0.34"))
MIN_RANGE_ATR = float(os.getenv("FB1_MIN_RANGE_ATR", "2.8"))
MAX_RANGE_ATR = float(os.getenv("FB1_MAX_RANGE_ATR", "9.0"))
MAX_MA_SEP_ATR = float(os.getenv("FB1_MAX_MA_SEP_ATR", "0.55"))
MAX_1H_SLOPE_ATR = float(os.getenv("FB1_MAX_1H_SLOPE_ATR", "0.55"))
MAX_15M_SLOPE_ATR = float(os.getenv("FB1_MAX_15M_SLOPE_ATR", "0.38"))
TOUCH_TOL_ATR = float(os.getenv("FB1_TOUCH_TOL_ATR", "0.28"))
MIN_TOUCHES = int(os.getenv("FB1_MIN_TOUCHES", "2"))

# ---- Liquidity sweep architecture ----
SWEEP_MIN_ATR = float(os.getenv("FB1_SWEEP_MIN_ATR", "0.10"))
SWEEP_MAX_ATR = float(os.getenv("FB1_SWEEP_MAX_ATR", "0.85"))
SWEEP_WICK_MIN = float(os.getenv("FB1_SWEEP_WICK_MIN", "0.38"))
SWEEP_CLOSE_INSIDE_ATR = float(os.getenv("FB1_SWEEP_CLOSE_INSIDE_ATR", "0.03"))
SWEEP_VOL_MIN = float(os.getenv("FB1_SWEEP_VOL_MIN", "0.85"))
SWEEP_VOL_MAX = float(os.getenv("FB1_SWEEP_VOL_MAX", "2.50"))

RECLAIM_BARS = int(os.getenv("FB1_RECLAIM_BARS", "2"))
RECLAIM_MIN_ATR = float(os.getenv("FB1_RECLAIM_MIN_ATR", "0.04"))

DISP_BARS = int(os.getenv("FB1_DISP_BARS", "3"))
DISP_BODY_MIN = float(os.getenv("FB1_DISP_BODY_MIN", "0.52"))
DISP_MOVE_ATR = float(os.getenv("FB1_DISP_MOVE_ATR", "0.18"))
DISP_VOL_MIN = float(os.getenv("FB1_DISP_VOL_MIN", "1.00"))

RETEST_BARS = int(os.getenv("FB1_RETEST_BARS", "6"))
RETEST_NEAR_ATR = float(os.getenv("FB1_RETEST_NEAR_ATR", "0.24"))
RETEST_HOLD_ATR = float(os.getenv("FB1_RETEST_HOLD_ATR", "0.04"))
RETEST_VOL_MAX_FRAC = float(os.getenv("FB1_RETEST_VOL_MAX_FRAC", "0.90"))

STOP_BUF_ATR = float(os.getenv("FB1_STOP_BUF_ATR", "0.10"))
MIN_STOP_PCT = float(os.getenv("FB1_MIN_STOP_PCT", "0.12"))
MAX_STOP_PCT = float(os.getenv("FB1_MAX_STOP_PCT", "1.35"))
MIN_MID_R = float(os.getenv("FB1_MIN_MID_R", "1.05"))
HOLD_BARS = int(os.getenv("FB1_HOLD_BARS", "48"))
COOLDOWN_BARS = int(os.getenv("FB1_COOLDOWN_BARS", "18"))
W_MID = float(os.getenv("FB1_W_MID", "0.65"))
W_FAR = 1.0 - W_MID


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
    r = max(b["high"] - b["low"], 1e-12)
    return abs(b["close"] - b["open"]) / r


def close_pos(b):
    r = max(b["high"] - b["low"], 1e-12)
    return (b["close"] - b["low"]) / r


def lower_wick_frac(b):
    r = max(b["high"] - b["low"], 1e-12)
    return (min(b["open"], b["close"]) - b["low"]) / r


def upper_wick_frac(b):
    r = max(b["high"] - b["low"], 1e-12)
    return (b["high"] - max(b["open"], b["close"])) / r


def load(path):
    by = defaultdict(list)
    with path.open(newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            try:
                by[r["symbol"].upper()].append({
                    "t": dt(r["timestamp"]),
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "vol": float(r.get("quote_volume") or 0)
                    or float(r.get("volume") or 0) * float(r["close"]),
                })
            except Exception:
                pass
    for s in by:
        by[s].sort(key=lambda z: z["t"])
    return by


def atr(rows, i):
    return mean([
        tr(rows[j], rows[j - 1]["close"])
        for j in range(i - ATR_N, i)
    ])


def er(rows, i, n):
    net = abs(rows[i - 1]["close"] - rows[i - n]["close"])
    den = sum(
        abs(rows[j]["close"] - rows[j - 1]["close"])
        for j in range(i - n + 1, i)
    )
    return net / den if den else 1.0


def sampled_slope(rows, i, step, count):
    vals = [rows[i - step * k - 1]["close"] for k in range(count)][::-1]
    return vals[-1] - vals[0] if len(vals) >= 2 else 0.0


def regime(rows, i):
    need = max(RANGE_N, ATR_N, ER_N, VOL_N, 72) + 3
    if i < need:
        return None

    base = rows[i - RANGE_N:i]
    a = atr(rows, i)
    if a <= 0:
        return None

    hi = max(x["high"] for x in base)
    lo = min(x["low"] for x in base)
    width = (hi - lo) / a
    e = er(rows, i, ER_N)

    fast = mean([x["close"] for x in rows[i - 12:i]])
    slow = mean([x["close"] for x in rows[i - 48:i]])
    ma_sep = abs(fast - slow) / a

    slope15 = abs(sampled_slope(rows, i, 3, 5)) / a
    slope1h = abs(sampled_slope(rows, i, 12, 4)) / a

    if not (MIN_RANGE_ATR <= width <= MAX_RANGE_ATR):
        return None
    if e > MAX_ER or ma_sep > MAX_MA_SEP_ATR:
        return None
    if slope15 > MAX_15M_SLOPE_ATR or slope1h > MAX_1H_SLOPE_ATR:
        return None

    low_touches = sum(
        1 for x in base[:-4]
        if x["low"] <= lo + TOUCH_TOL_ATR * a
    )
    high_touches = sum(
        1 for x in base[:-4]
        if x["high"] >= hi - TOUCH_TOL_ATR * a
    )

    vavg = mean([rows[j]["vol"] for j in range(i - VOL_N, i)])

    return {
        "hi": hi,
        "lo": lo,
        "atr": a,
        "er": e,
        "ma_sep": ma_sep,
        "slope15": slope15,
        "slope1h": slope1h,
        "low_touches": low_touches,
        "high_touches": high_touches,
        "vavg": vavg,
    }


def sweep_setup(rows, i):
    rg = regime(rows, i)
    if not rg:
        return None

    b = rows[i]
    a = rg["atr"]
    vr = b["vol"] / rg["vavg"] if rg["vavg"] > 0 else 0

    if not (SWEEP_VOL_MIN <= vr <= SWEEP_VOL_MAX):
        return None

    down = (rg["lo"] - b["low"]) / a
    up = (b["high"] - rg["hi"]) / a

    # Long: sweep below established range, leave lower wick, close back near/inside.
    if (
        rg["low_touches"] >= MIN_TOUCHES
        and SWEEP_MIN_ATR <= down <= SWEEP_MAX_ATR
        and lower_wick_frac(b) >= SWEEP_WICK_MIN
        and b["close"] >= rg["lo"] - SWEEP_CLOSE_INSIDE_ATR * a
        and close_pos(b) >= 0.42
    ):
        return {
            **rg,
            "side": "LONG",
            "sweep_i": i,
            "ext": b["low"],
            "sweep_vr": vr,
        }

    # Short: mirror image.
    if (
        rg["high_touches"] >= MIN_TOUCHES
        and SWEEP_MIN_ATR <= up <= SWEEP_MAX_ATR
        and upper_wick_frac(b) >= SWEEP_WICK_MIN
        and b["close"] <= rg["hi"] + SWEEP_CLOSE_INSIDE_ATR * a
        and close_pos(b) <= 0.58
    ):
        return {
            **rg,
            "side": "SHORT",
            "sweep_i": i,
            "ext": b["high"],
            "sweep_vr": vr,
        }

    return None


def reclaim(rows, s):
    level = s["lo"] if s["side"] == "LONG" else s["hi"]

    for j in range(
        s["sweep_i"],
        min(len(rows), s["sweep_i"] + 1 + RECLAIM_BARS),
    ):
        b = rows[j]

        if s["side"] == "LONG":
            s["ext"] = min(s["ext"], b["low"])
            if b["close"] >= level + RECLAIM_MIN_ATR * s["atr"]:
                return j
        else:
            s["ext"] = max(s["ext"], b["high"])
            if b["close"] <= level - RECLAIM_MIN_ATR * s["atr"]:
                return j

    return None


def displacement(rows, s, ri):
    level = s["lo"] if s["side"] == "LONG" else s["hi"]

    for j in range(
        ri + 1,
        min(len(rows), ri + 1 + DISP_BARS),
    ):
        b = rows[j]
        vr = b["vol"] / s["vavg"] if s["vavg"] > 0 else 0

        if s["side"] == "LONG":
            move = (b["close"] - level) / s["atr"]
            if (
                b["close"] > b["open"]
                and body_frac(b) >= DISP_BODY_MIN
                and move >= DISP_MOVE_ATR
                and vr >= DISP_VOL_MIN
                and close_pos(b) >= 0.65
            ):
                return j, vr

        else:
            move = (level - b["close"]) / s["atr"]
            if (
                b["close"] < b["open"]
                and body_frac(b) >= DISP_BODY_MIN
                and move >= DISP_MOVE_ATR
                and vr >= DISP_VOL_MIN
                and close_pos(b) <= 0.35
            ):
                return j, vr

    return None


def retest(rows, s, di, disp_vr):
    level = s["lo"] if s["side"] == "LONG" else s["hi"]

    for j in range(
        di + 1,
        min(len(rows), di + 1 + RETEST_BARS),
    ):
        b = rows[j]
        vr = b["vol"] / s["vavg"] if s["vavg"] > 0 else 0
        quiet = vr <= max(DISP_VOL_MIN, disp_vr * RETEST_VOL_MAX_FRAC)

        if s["side"] == "LONG":
            # Two closes below the edge again = likely real breakdown.
            if (
                j >= di + 2
                and rows[j - 1]["close"] < level
                and b["close"] < level
            ):
                return None

            near = b["low"] <= level + RETEST_NEAR_ATR * s["atr"]
            hold = (
                b["close"] >= level + RETEST_HOLD_ATR * s["atr"]
                and close_pos(b) >= 0.52
            )
            if near and hold and quiet:
                return j, b["close"], vr

        else:
            if (
                j >= di + 2
                and rows[j - 1]["close"] > level
                and b["close"] > level
            ):
                return None

            near = b["high"] >= level - RETEST_NEAR_ATR * s["atr"]
            hold = (
                b["close"] <= level - RETEST_HOLD_ATR * s["atr"]
                and close_pos(b) <= 0.48
            )
            if near and hold and quiet:
                return j, b["close"], vr

    return None


def simulate(rows, ei, entry, s):
    side = s["side"]
    a = s["atr"]
    mid = (s["hi"] + s["lo"]) / 2
    far = s["hi"] if side == "LONG" else s["lo"]

    stop = (
        s["ext"] - STOP_BUF_ATR * a
        if side == "LONG"
        else s["ext"] + STOP_BUF_ATR * a
    )

    sp = abs(pct(stop, entry))
    risk = abs(entry - stop)

    if risk <= 0 or not (MIN_STOP_PCT <= sp <= MAX_STOP_PCT):
        return None

    midr = (
        (mid - entry)
        if side == "LONG"
        else (entry - mid)
    ) / risk

    farr = (
        (far - entry)
        if side == "LONG"
        else (entry - far)
    ) / risk

    if midr < MIN_MID_R or farr <= midr:
        return None

    hit_mid = False
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
            active = entry if hit_mid else stop

            if b["low"] <= active:
                ret = (
                    W_MID * pct(mid, entry)
                    if hit_mid
                    else -sp
                )
                reason = "BE_AFTER_MID" if hit_mid else "STOP"
                break

            if not hit_mid and b["high"] >= mid:
                hit_mid = True

            if b["high"] >= far:
                ret = (
                    W_MID * pct(mid, entry)
                    + W_FAR * pct(far, entry)
                )
                reason = "FAR_BOUNDARY"
                break

        else:
            active = entry if hit_mid else stop

            if b["high"] >= active:
                ret = (
                    W_MID * pct(entry, mid)
                    if hit_mid
                    else -sp
                )
                reason = "BE_AFTER_MID" if hit_mid else "STOP"
                break

            if not hit_mid and b["low"] <= mid:
                hit_mid = True

            if b["low"] <= far:
                ret = (
                    W_MID * pct(entry, mid)
                    + W_FAR * pct(entry, far)
                )
                reason = "FAR_BOUNDARY"
                break

    if ret is None:
        raw = (
            pct(last, entry)
            if side == "LONG"
            else pct(entry, last)
        )
        ret = (
            W_MID * (
                pct(mid, entry)
                if side == "LONG"
                else pct(entry, mid)
            )
            + W_FAR * raw
            if hit_mid
            else raw
        )

    return {
        "net": ret - COST,
        "reason": reason,
        "stop_pct": sp,
        "mid_r": midr,
        "far_r": farr,
    }


def run_symbol(rows):
    out = []
    cool = -1

    for i in range(len(rows)):
        if i <= cool:
            continue

        s = sweep_setup(rows, i)
        if not s:
            continue

        ri = reclaim(rows, s)
        if ri is None:
            continue

        dp = displacement(rows, s, ri)
        if not dp:
            continue

        di, disp_vr = dp

        rt = retest(rows, s, di, disp_vr)
        if not rt:
            continue

        ei, entry, retest_vr = rt

        x = simulate(rows, ei, entry, s)
        if not x:
            continue

        out.append({
            "side": s["side"],
            "entry_time": rows[ei]["t"].isoformat(),
            "net": x["net"],
            "reason": x["reason"],
            "stop_pct": x["stop_pct"],
            "mid_r": x["mid_r"],
            "far_r": x["far_r"],
            "sweep_vr": s["sweep_vr"],
            "disp_vr": disp_vr,
            "retest_vr": retest_vr,
            "er": s["er"],
            "slope15": s["slope15"],
            "slope1h": s["slope1h"],
        })

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
    p = sd / "history_5m.csv"

    last = None

    for base in CACHE_BASES:
        for attempt in range(4):
            try:
                req = Request(
                    f"{base}/segment_{idx:02d}/history_5m.csv",
                    headers={"User-Agent": "fb1-v4/1.0"},
                )

                with urlopen(req, timeout=60) as r:
                    data = r.read()

                if len(data) > 100000:
                    p.write_bytes(data)
                else:
                    raise RuntimeError(f"small cache {len(data)}")

                by = load(p)
                expected = int(DAYS * 24 * 12)
                minimum = int(expected * MIN_COVERAGE)
                counts = {
                    s: len(by.get(s, []))
                    for s in SYMBOLS
                }

                if any(n < minimum for n in counts.values()):
                    raise RuntimeError(f"coverage {counts}")

                print(
                    "FB1V4_CACHE_OK "
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
                return p

            except Exception as e:
                last = e
                time.sleep(min(3 * (attempt + 1), 10))

    raise RuntimeError(
        f"cache unavailable {idx}: {last}"
    )


def main():
    end = dt(END)
    start = end - timedelta(days=SEGMENTS * DAYS)

    rows = []
    all_trades = []

    print(
        "FB1V4_START "
        + json.dumps(
            {
                "core": CORE,
                "historical_only": True,
                "architecture": (
                    "range_confirmed_then_liquidity_sweep_then_"
                    "reclaim_then_reverse_displacement_then_"
                    "low_volume_retest"
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
            q = run_symbol(by.get(sym, []))
            for x in q:
                x["symbol"] = sym
            ts += q

        z = metrics(ts)

        row = {
            "segment": idx,
            "start": (
                start
                + timedelta(days=(idx - 1) * DAYS)
            ).isoformat(),
            "metrics": z,
        }

        rows.append(row)
        all_trades += ts

        print(
            "FB1V4_SEGMENT_DONE "
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
        final["trades"] >= 100
        and final["profit_factor"] >= 1.15
        and final["sum_net_return_pct"] > 0
        and final["positive_segments"] >= 7
    )

    (ROOT / "fb1_v4_final.json").write_text(
        json.dumps(final, indent=2)
    )

    with (ROOT / "fb1_v4_trades.csv").open(
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        fields = [
            "symbol",
            "side",
            "entry_time",
            "net",
            "reason",
            "stop_pct",
            "mid_r",
            "far_r",
            "sweep_vr",
            "disp_vr",
            "retest_vr",
            "er",
            "slope15",
            "slope1h",
        ]
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()

        for x in all_trades:
            w.writerow({
                k: x.get(k)
                for k in fields
            })

    print(
        "FB1V4_DONE "
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
