"""
R3 A_GUARD baseline vs V2 360-day RESUME runner.

Purpose
-------
Resume the corrected 360-day R3 V2 run from segment 3/12 after the original
service stalled during segment 3 data download.

Fixes in this resume runner
---------------------------
1) Keeps the already-completed segment 1 and segment 2 metrics.
2) Starts new work from segment 3 by default.
3) Wraps every historical data source call in a hard watchdog
   (default 180 seconds). If a source stalls or repeatedly errors, it returns
   an empty result so the existing fallback chain can continue.
4) Retries a failed segment up to 2 times.
5) Aggregates all 12 segment metrics correctly using gross-profit/gross-loss
   sums, so the final PF is NOT an average of per-segment PF values.

Historical-only. No live/shadow/current DB writes. No orders.
"""

from __future__ import annotations

import csv
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import r3_historical_fast_job as base


END_DATES = [
    "2025-07-05T09:30:00Z",
    "2025-08-04T09:30:00Z",
    "2025-09-03T09:30:00Z",
    "2025-10-03T09:30:00Z",
    "2025-11-02T09:30:00Z",
    "2025-12-02T09:30:00Z",
    "2026-01-01T09:30:00Z",
    "2026-01-31T09:30:00Z",
    "2026-03-02T09:30:00Z",
    "2026-04-01T09:30:00Z",
    "2026-05-01T09:30:00Z",
    "2026-05-31T09:30:00Z",
]

ROOT = Path(os.environ.get("R3_V2_AUTO360_DIR", "data/r3_v2_auto360"))
ROOT.mkdir(parents=True, exist_ok=True)

START_SEGMENT = int(os.environ.get("R3_V2_START_SEGMENT", "3"))
SOURCE_TIMEOUT_SEC = int(os.environ.get("R3_V2_SOURCE_TIMEOUT_SEC", "180"))
MAX_SEGMENT_ATTEMPTS = int(os.environ.get("R3_V2_SEGMENT_ATTEMPTS", "2"))
ADAPTER_TIMEOUT_SEC = int(os.environ.get("R3_V2_ADAPTER_TIMEOUT_SEC", "600"))

PRECOMPLETED_SEGMENTS = [
    {
        "segment": 1,
        "start": "2025-06-05T09:30:00+00:00",
        "end": "2025-07-05T09:30:00+00:00",
        "baseline": {
            "trades": 9, "wins": 3, "losses": 6,
            "win_rate_pct": 33.33, "profit_factor": 0.446,
            "gross_profit_pct_points": 3.894951,
            "gross_loss_abs_pct_points": 8.740412,
            "sum_net_return_pct": -4.845,
        },
        "v2": {
            "trades": 4, "wins": 1, "losses": 3,
            "win_rate_pct": 25.0, "profit_factor": 0.157,
            "gross_profit_pct_points": 0.54,
            "gross_loss_abs_pct_points": 3.438527,
            "sum_net_return_pct": -2.899,
        },
        "delta_sum_net_return_pct": 1.946,
        "trade_retention_pct": 44.44,
    },
    {
        "segment": 2,
        "start": "2025-07-05T09:30:00+00:00",
        "end": "2025-08-04T09:30:00+00:00",
        "baseline": {
            "trades": 14, "wins": 6, "losses": 8,
            "win_rate_pct": 42.86, "profit_factor": 1.137,
            "gross_profit_pct_points": 13.874093,
            "gross_loss_abs_pct_points": 12.202044,
            "sum_net_return_pct": 1.672,
        },
        "v2": {
            "trades": 5, "wins": 2, "losses": 3,
            "win_rate_pct": 40.0, "profit_factor": 0.778,
            "gross_profit_pct_points": 3.192553,
            "gross_loss_abs_pct_points": 4.103596,
            "sum_net_return_pct": -0.911,
        },
        "delta_sum_net_return_pct": -2.583,
        "trade_retention_pct": 35.71,
    },
]


def _dt(s: str) -> datetime:
    d = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def _read_returns(path: Path) -> list[float]:
    vals = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            try:
                vals.append(float(row["net_return_pct"]))
            except Exception:
                continue
    return vals


def _metrics(vals: list[float]) -> dict:
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else (999.0 if gp > 0 else 0.0)
    return {
        "trades": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100.0 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": round(pf, 3),
        "gross_profit_pct_points": round(gp, 6),
        "gross_loss_abs_pct_points": round(gl, 6),
        "sum_net_return_pct": round(sum(vals), 3),
    }


def _aggregate_segment_metrics(segments: list[dict], key: str) -> dict:
    rows = [seg[key] for seg in segments]
    trades = sum(int(x["trades"]) for x in rows)
    wins = sum(int(x["wins"]) for x in rows)
    losses = sum(int(x["losses"]) for x in rows)
    gp = sum(float(x["gross_profit_pct_points"]) for x in rows)
    gl = sum(float(x["gross_loss_abs_pct_points"]) for x in rows)
    total = sum(float(x["sum_net_return_pct"]) for x in rows)
    pf = gp / gl if gl > 0 else (999.0 if gp > 0 else 0.0)
    return {
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(100.0 * wins / trades, 2) if trades else 0.0,
        "profit_factor": round(pf, 3),
        "gross_profit_pct_points": round(gp, 6),
        "gross_loss_abs_pct_points": round(gl, 6),
        "sum_net_return_pct": round(total, 3),
    }


def _bounded(label, fn):
    def wrapped(*args, **kwargs):
        if SOURCE_TIMEOUT_SEC <= 0 or not hasattr(signal, "SIGALRM"):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                print(
                    "R3_V2_SOURCE_FAIL "
                    f"label={label} error={type(exc).__name__}:{exc}",
                    flush=True,
                )
                return []

        old_handler = signal.getsignal(signal.SIGALRM)

        def alarm_handler(signum, frame):
            raise TimeoutError(f"{label} exceeded {SOURCE_TIMEOUT_SEC}s")

        try:
            signal.signal(signal.SIGALRM, alarm_handler)
            signal.setitimer(signal.ITIMER_REAL, SOURCE_TIMEOUT_SEC)
            return fn(*args, **kwargs)
        except Exception as exc:
            print(
                "R3_V2_SOURCE_FAIL "
                f"label={label} error={type(exc).__name__}:{exc}",
                flush=True,
            )
            return []
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)

    return wrapped


# Add watchdogs around the existing corrected historical source functions.
_ORIG_V3_DOWNLOAD = base.hd.download_symbol
_ORIG_MARK_DOWNLOAD = base.download_mark_price_symbol
_ORIG_SPOT_DOWNLOAD = base.download_spot_history_symbol

base.hd.download_symbol = _bounded("bingx_v3", _ORIG_V3_DOWNLOAD)
base.download_mark_price_symbol = _bounded("bingx_mark_price", _ORIG_MARK_DOWNLOAD)
base.download_spot_history_symbol = _bounded("bingx_historical_spot", _ORIG_SPOT_DOWNLOAD)


def _run_and_stream(cmd: list[str]) -> None:
    print("R3_V2_RUN", " ".join(cmd), flush=True)
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert p.stdout is not None

    started = time.monotonic()

    while True:
        if ADAPTER_TIMEOUT_SEC > 0 and time.monotonic() - started > ADAPTER_TIMEOUT_SEC:
            p.kill()
            raise TimeoutError(
                f"adapter exceeded {ADAPTER_TIMEOUT_SEC}s: {' '.join(cmd)}"
            )

        line = p.stdout.readline()
        if line:
            print(line.rstrip(), flush=True)

        code = p.poll()
        if code is not None:
            break

        if not line:
            time.sleep(0.1)

    for line in p.stdout:
        print(line.rstrip(), flush=True)

    if code != 0:
        raise subprocess.CalledProcessError(code, cmd)


def _run_segment(idx: int, end_iso: str) -> dict:
    end = _dt(end_iso)
    start = end - base.timedelta(days=30)

    seg = ROOT / f"segment_{idx:02d}"
    seg.mkdir(parents=True, exist_ok=True)

    base.DATA = seg
    base.DAYS = 30

    print(
        "R3_V2_AUTO360_SEGMENT_START "
        f"segment={idx}/12 start={start.isoformat()} end={end.isoformat()}",
        flush=True,
    )

    for interval in ("1m", "5m", "15m", "1h"):
        base.download_interval(interval, start, end)

    base.recover_interval_from_1m("5m", start, end)
    base.recover_interval_from_1m("15m", start, end)

    common = [
        "--candles-1m", str(seg / "history_1m.csv"),
        "--candles-5m", str(seg / "history_5m.csv"),
        "--candles-15m", str(seg / "history_15m.csv"),
        "--candles-1h", str(seg / "history_1h.csv"),
    ]

    baseline_out = seg / "baseline_outcomes.csv"
    baseline_detail = seg / "baseline_detail.csv"

    _run_and_stream([
        sys.executable,
        "r3_historical_adapter_fast.py",
        *common,
        "--detail-out", str(baseline_detail),
        "--outcomes-out", str(baseline_out),
    ])

    v2_out = seg / "v2_outcomes.csv"
    v2_detail = seg / "v2_detail.csv"

    _run_and_stream([
        sys.executable,
        "r3_historical_adapter_v2.py",
        *common,
        "--detail-out", str(v2_detail),
        "--outcomes-out", str(v2_out),
    ])

    bvals = _read_returns(baseline_out)
    vvals = _read_returns(v2_out)

    row = {
        "segment": idx,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "baseline": _metrics(bvals),
        "v2": _metrics(vvals),
    }
    row["delta_sum_net_return_pct"] = round(
        row["v2"]["sum_net_return_pct"] - row["baseline"]["sum_net_return_pct"],
        3,
    )
    row["trade_retention_pct"] = (
        round(
            100.0 * row["v2"]["trades"] / row["baseline"]["trades"],
            2,
        )
        if row["baseline"]["trades"] else 0.0
    )

    (seg / "segment_summary.json").write_text(
        json.dumps(row, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(
        "R3_V2_AUTO360_SEGMENT_DONE "
        + json.dumps(row, ensure_ascii=False, sort_keys=True),
        flush=True,
    )

    return row


def main() -> None:
    if START_SEGMENT not in (1, 3):
        raise ValueError(
            "R3_V2_START_SEGMENT supports 1 or 3 for this resume build"
        )

    segments = list(PRECOMPLETED_SEGMENTS) if START_SEGMENT == 3 else []

    print(
        "R3_V2_AUTO360_RESUME "
        f"start_segment={START_SEGMENT}/12 "
        f"precompleted={len(segments)} "
        f"source_timeout_sec={SOURCE_TIMEOUT_SEC} "
        f"segment_attempts={MAX_SEGMENT_ATTEMPTS}",
        flush=True,
    )

    for idx, end_iso in enumerate(END_DATES, start=1):
        if idx < START_SEGMENT:
            print(
                "R3_V2_AUTO360_SEGMENT_SKIP "
                f"segment={idx}/12 reason=precompleted",
                flush=True,
            )
            continue

        last_error = None

        for attempt in range(1, MAX_SEGMENT_ATTEMPTS + 1):
            try:
                print(
                    "R3_V2_AUTO360_ATTEMPT "
                    f"segment={idx}/12 attempt={attempt}/{MAX_SEGMENT_ATTEMPTS}",
                    flush=True,
                )
                row = _run_segment(idx, end_iso)
                segments.append(row)
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                print(
                    "R3_V2_AUTO360_SEGMENT_ERROR "
                    f"segment={idx}/12 attempt={attempt}/{MAX_SEGMENT_ATTEMPTS} "
                    f"error={type(exc).__name__}:{exc}",
                    flush=True,
                )
                if attempt < MAX_SEGMENT_ATTEMPTS:
                    time.sleep(5)

        if last_error is not None:
            raise RuntimeError(
                f"R3 segment {idx}/12 failed after "
                f"{MAX_SEGMENT_ATTEMPTS} attempts: {last_error}"
            )

    segments.sort(key=lambda x: int(x["segment"]))

    if len(segments) != 12:
        raise RuntimeError(f"R3 V2 incomplete segments={len(segments)}/12")

    baseline = _aggregate_segment_metrics(segments, "baseline")
    v2 = _aggregate_segment_metrics(segments, "v2")

    final = {
        "segments": 12,
        "start_segment_this_run": START_SEGMENT,
        "baseline": baseline,
        "v2": v2,
        "delta_sum_net_return_pct": round(
            v2["sum_net_return_pct"] - baseline["sum_net_return_pct"],
            3,
        ),
        "delta_pf": round(
            v2["profit_factor"] - baseline["profit_factor"],
            3,
        ),
        "trade_retention_pct": (
            round(100.0 * v2["trades"] / baseline["trades"], 2)
            if baseline["trades"] else 0.0
        ),
        "per_segment": segments,
    }

    (ROOT / "final_summary.json").write_text(
        json.dumps(final, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(
        "R3_V2_AUTO360_SUMMARY "
        + json.dumps(final, ensure_ascii=False, sort_keys=True),
        flush=True,
    )
    print("R3_V2_AUTO360_DONE segments=12/12", flush=True)


if __name__ == "__main__":
    main()
