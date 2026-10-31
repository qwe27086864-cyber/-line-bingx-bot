"""
R3 A_GUARD baseline vs v2 360-day A/B historical run.

Runs 12 isolated 30-day windows using the existing corrected historical data
retrieval/fallback path. For each window it runs:
  1) baseline A_GUARD (existing r3_historical_adapter_fast.py)
  2) A_GUARD v2 entry filters (r3_historical_adapter_v2.py)

Historical-only. No live/shadow/current DB writes. No orders.
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
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

ROOT = Path(
    os.environ.get(
        "R3_V2_AUTO360_DIR",
        "data/r3_v2_auto360",
    )
)
ROOT.mkdir(parents=True, exist_ok=True)


def _dt(s):
    x = s.replace("Z", "+00:00")
    d = datetime.fromisoformat(x)
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    return d.astimezone(timezone.utc)


def _read_returns(path):
    vals = []
    with open(
        path,
        newline="",
        encoding="utf-8-sig",
    ) as f:
        for row in csv.DictReader(f):
            try:
                vals.append(float(row["net_return_pct"]))
            except Exception:
                continue
    return vals


def _metrics(vals):
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = (
        gp / gl
        if gl > 0
        else (999.0 if gp > 0 else 0.0)
    )
    return {
        "trades": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (
            round(100.0 * len(wins) / len(vals), 2)
            if vals else 0.0
        ),
        "profit_factor": round(pf, 3),
        "gross_profit_pct_points": round(gp, 6),
        "gross_loss_abs_pct_points": round(gl, 6),
        "sum_net_return_pct": round(sum(vals), 3),
    }


def _run_and_stream(cmd):
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert p.stdout is not None
    for line in p.stdout:
        print(line.rstrip(), flush=True)
    code = p.wait()
    if code != 0:
        raise subprocess.CalledProcessError(code, cmd)


def main():
    all_base = []
    all_v2 = []
    segments = []

    for idx, end_iso in enumerate(END_DATES, start=1):
        end = _dt(end_iso)
        start = end - base.timedelta(days=30)

        seg = ROOT / f"segment_{idx:02d}"
        seg.mkdir(parents=True, exist_ok=True)

        # r3_historical_fast_job functions use module-global DATA.
        base.DATA = seg
        base.DAYS = 30

        print(
            "R3_V2_AUTO360_SEGMENT_START "
            f"segment={idx}/12 "
            f"start={start.isoformat()} "
            f"end={end.isoformat()}",
            flush=True,
        )

        for interval in ("1m", "5m", "15m", "1h"):
            base.download_interval(interval, start, end)

        base.recover_interval_from_1m(
            "5m",
            start,
            end,
        )
        base.recover_interval_from_1m(
            "15m",
            start,
            end,
        )

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
            "--detail-out",
            str(baseline_detail),
            "--outcomes-out",
            str(baseline_out),
        ])

        v2_out = seg / "v2_outcomes.csv"
        v2_detail = seg / "v2_detail.csv"

        _run_and_stream([
            sys.executable,
            "r3_historical_adapter_v2.py",
            *common,
            "--detail-out",
            str(v2_detail),
            "--outcomes-out",
            str(v2_out),
        ])

        bvals = _read_returns(baseline_out)
        vvals = _read_returns(v2_out)
        all_base.extend(bvals)
        all_v2.extend(vvals)

        row = {
            "segment": idx,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "baseline": _metrics(bvals),
            "v2": _metrics(vvals),
        }
        row["delta_sum_net_return_pct"] = round(
            row["v2"]["sum_net_return_pct"]
            - row["baseline"]["sum_net_return_pct"],
            3,
        )
        row["trade_retention_pct"] = (
            round(
                100.0
                * row["v2"]["trades"]
                / row["baseline"]["trades"],
                2,
            )
            if row["baseline"]["trades"]
            else 0.0
        )
        segments.append(row)

        (seg / "segment_summary.json").write_text(
            json.dumps(
                row,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        print(
            "R3_V2_AUTO360_SEGMENT_DONE "
            + json.dumps(
                row,
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )

    baseline = _metrics(all_base)
    v2 = _metrics(all_v2)

    final = {
        "segments": 12,
        "baseline": baseline,
        "v2": v2,
        "delta_sum_net_return_pct": round(
            v2["sum_net_return_pct"]
            - baseline["sum_net_return_pct"],
            3,
        ),
        "delta_pf": round(
            v2["profit_factor"]
            - baseline["profit_factor"],
            3,
        ),
        "trade_retention_pct": (
            round(
                100.0
                * v2["trades"]
                / baseline["trades"],
                2,
            )
            if baseline["trades"]
            else 0.0
        ),
        "per_segment": segments,
    }

    (ROOT / "final_summary.json").write_text(
        json.dumps(
            final,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(
        "R3_V2_AUTO360_SUMMARY "
        + json.dumps(
            final,
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )
    print("R3_V2_AUTO360_DONE segments=12/12", flush=True)


if __name__ == "__main__":
    main()
