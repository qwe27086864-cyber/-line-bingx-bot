"""
R2FS V3 Regime Filter Screening - 360 Day Post-Filter Sweep

Goal
----
Find whether R2FS V2's negative 360-day expectancy can be improved by only
tightening entry-quality gates, without changing TP/SL/exit logic.

This script:
1) Re-runs the same 12 x 30-day corrected R2FS V2 historical windows.
2) Reads each segment's resolved FAST_ENTER trades.
3) Post-filters those trades across a grid of:
   - market_score
   - direction_score
   - micro_score
   - fast_score
4) Aggregates PF correctly from gross profits / gross losses.
5) Reports how many of 12 segments are positive, so a variant cannot look
   good only because of one lucky month.

IMPORTANT
---------
This is a SCREENING pass, not the final exact V3 replay.
Because it filters an already-generated V2 trade set, it does not recreate
extra trades that might become available after earlier trades are rejected
and MAX_OPEN / cooldown slots are freed.

If a robust candidate appears, the next step is an exact replay with the
selected thresholds integrated before position allocation.

Historical-only. No DATABASE_URL. No orders. No live/shadow writes.
"""

from __future__ import annotations

import csv
import json
import math
import os
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(os.environ.get("R2FS_V3_DIR", "data/r2fs_v3_regime"))
ROOT.mkdir(parents=True, exist_ok=True)

SEGMENTS = [
    ("01", "2026-10-28T09:30:00Z"),
    ("02", "2026-09-28T09:30:00Z"),
    ("03", "2026-08-29T09:30:00Z"),
    ("04", "2026-07-30T09:30:00Z"),
    ("05", "2026-06-30T09:30:00Z"),
    ("06", "2026-05-31T09:30:00Z"),
    ("07", "2026-05-01T09:30:00Z"),
    ("08", "2026-04-01T09:30:00Z"),
    ("09", "2026-03-02T09:30:00Z"),
    ("10", "2026-01-31T09:30:00Z"),
    ("11", "2026-01-01T09:30:00Z"),
    ("12", "2025-12-02T09:30:00Z"),
]

# Baseline V2 gates were market>=45, direction>=72, micro>=72, fast>=78.
# Include baseline as a control plus tighter values.
MARKET_GATES = [45, 50, 55, 60, 65, 70]
DIRECTION_GATES = [72, 78, 84]
MICRO_GATES = [72, 78, 84]
FAST_GATES = [78, 82, 86]

MIN_ROBUST_TRADES = int(os.environ.get("R2FS_V3_MIN_TRADES", "200"))
MIN_POSITIVE_SEGMENTS = int(os.environ.get("R2FS_V3_MIN_POSITIVE_SEGMENTS", "6"))
MAX_SEGMENT_ATTEMPTS = int(os.environ.get("R2FS_V3_SEGMENT_ATTEMPTS", "2"))


def hold_forever():
    while True:
        time.sleep(3600)


def pf(gp: float, gl_abs: float) -> float:
    if gl_abs > 0:
        return gp / gl_abs
    if gp > 0:
        return 999.0
    return 0.0


def load_resolved_fast_entries(path: Path):
    rows = []
    with path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if str(row.get("decision", "")).strip() != "FAST_ENTER":
                continue
            if str(row.get("resolved", "")).strip().lower() not in {
                "true", "1", "yes"
            }:
                continue

            try:
                net = float(row["net_return_pct"])
                market = float(row["market_score"])
                direction = float(row["direction_score"])
                micro = float(row["micro_score"])
                fast = float(row["fast_score"])
            except Exception:
                continue

            rows.append({
                "timestamp": row.get("timestamp", ""),
                "symbol": row.get("symbol", ""),
                "side": row.get("side", ""),
                "net_return_pct": net,
                "market_score": market,
                "direction_score": direction,
                "micro_score": micro,
                "fast_score": fast,
                "exit_reason": row.get("exit_reason", ""),
            })
    return rows


def variant_name(market, direction, micro, fast):
    return f"M{market}_D{direction}_MI{micro}_F{fast}"


def build_variants():
    out = []
    for market in MARKET_GATES:
        for direction in DIRECTION_GATES:
            for micro in MICRO_GATES:
                for fast in FAST_GATES:
                    out.append({
                        "variant": variant_name(market, direction, micro, fast),
                        "market_min": market,
                        "direction_min": direction,
                        "micro_min": micro,
                        "fast_min": fast,
                    })
    return out


VARIANTS = build_variants()


def passes(row, v):
    return (
        row["market_score"] >= v["market_min"]
        and row["direction_score"] >= v["direction_min"]
        and row["micro_score"] >= v["micro_min"]
        and row["fast_score"] >= v["fast_min"]
    )


def stats(rows):
    vals = [float(x["net_return_pct"]) for x in rows]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    total = sum(vals)

    return {
        "trades": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100.0 * len(wins) / len(vals), 2) if vals else 0.0,
        "gross_profit_pct_points": round(gp, 9),
        "gross_loss_abs_pct_points": round(gl, 9),
        "profit_factor": round(pf(gp, gl), 6),
        "sum_net_return_pct": round(total, 9),
        "avg_net_return_pct": round(total / len(vals), 9) if vals else 0.0,
    }


def run_segment(seg_index: int, seg_id: str, end_iso: str):
    seg_dir = ROOT / f"segment_{seg_id}"
    seg_dir.mkdir(parents=True, exist_ok=True)

    detail_path = seg_dir / "r2fs_v2_history_detail.csv"

    # Resume within the same instance if a segment already finished.
    if detail_path.exists() and detail_path.stat().st_size > 100:
        rows = load_resolved_fast_entries(detail_path)
        if rows:
            print(
                f"R2FS_V3_SEGMENT_REUSE segment={seg_index}/12 "
                f"resolved_fast_entries={len(rows)}",
                flush=True,
            )
            return rows

    env = os.environ.copy()
    env.pop("DATABASE_URL", None)
    env["R2FS_V2_DAYS"] = "30"
    env["R2FS_V2_END_ISO"] = end_iso
    env["R2FS_V2_REQUEST_PAUSE_SEC"] = env.get(
        "R2FS_V2_REQUEST_PAUSE_SEC", "2.25"
    )
    env["R2FS_V2_DATA_DIR"] = str(seg_dir)

    print(
        f"R2FS_V3_SEGMENT_START segment={seg_index}/12 "
        f"end={end_iso}",
        flush=True,
    )

    proc = subprocess.Popen(
        [sys.executable, "r2fs_historical_v2_job.py"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    assert proc.stdout is not None
    for line in proc.stdout:
        print(line, end="", flush=True)

    rc = proc.wait()
    if rc != 0:
        raise RuntimeError(
            f"segment {seg_index}/12 V2 job failed rc={rc}"
        )

    if not detail_path.exists():
        raise RuntimeError(
            f"segment {seg_index}/12 missing {detail_path}"
        )

    rows = load_resolved_fast_entries(detail_path)
    print(
        f"R2FS_V3_SEGMENT_DATA_READY segment={seg_index}/12 "
        f"resolved_fast_entries={len(rows)}",
        flush=True,
    )
    return rows


def write_csv(path: Path, rows: list[dict]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main():
    os.environ.pop("DATABASE_URL", None)

    print(
        f"R2FS_V3_START segments=12 variants={len(VARIANTS)} "
        "mode=post_filter_screening historical_only=true",
        flush=True,
    )

    segment_trade_rows = {}

    for idx, (seg_id, end_iso) in enumerate(SEGMENTS, start=1):
        last_error = None

        for attempt in range(1, MAX_SEGMENT_ATTEMPTS + 1):
            try:
                print(
                    f"R2FS_V3_SEGMENT_ATTEMPT segment={idx}/12 "
                    f"attempt={attempt}/{MAX_SEGMENT_ATTEMPTS}",
                    flush=True,
                )
                segment_trade_rows[idx] = run_segment(
                    idx, seg_id, end_iso
                )
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                print(
                    f"R2FS_V3_SEGMENT_ERROR segment={idx}/12 "
                    f"attempt={attempt}/{MAX_SEGMENT_ATTEMPTS} "
                    f"error={type(exc).__name__}:{exc}",
                    flush=True,
                )
                if attempt < MAX_SEGMENT_ATTEMPTS:
                    time.sleep(10)

        if last_error is not None:
            raise RuntimeError(
                f"R2FS V3 segment {idx}/12 failed: {last_error}"
            )

    segment_results = []
    aggregate = {}

    for v in VARIANTS:
        name = v["variant"]
        total_rows = []
        per_segment = []

        for seg_idx in range(1, 13):
            filtered = [
                r for r in segment_trade_rows[seg_idx]
                if passes(r, v)
            ]
            s = stats(filtered)
            s["segment"] = seg_idx
            s["variant"] = name
            per_segment.append(s)
            total_rows.extend(filtered)

        overall = stats(total_rows)
        positive_segments = sum(
            1 for s in per_segment
            if s["sum_net_return_pct"] > 0
        )
        negative_segments = sum(
            1 for s in per_segment
            if s["sum_net_return_pct"] < 0
        )
        flat_segments = 12 - positive_segments - negative_segments

        segment_sums = [
            float(s["sum_net_return_pct"])
            for s in per_segment
        ]
        best_segment_sum = max(segment_sums) if segment_sums else 0.0
        worst_segment_sum = min(segment_sums) if segment_sums else 0.0

        positive_total = sum(x for x in segment_sums if x > 0)
        best_share = (
            best_segment_sum / positive_total
            if positive_total > 0 and best_segment_sum > 0
            else 0.0
        )

        record = {
            **v,
            **overall,
            "positive_segments": positive_segments,
            "negative_segments": negative_segments,
            "flat_segments": flat_segments,
            "best_segment_sum_pct_points": round(best_segment_sum, 6),
            "worst_segment_sum_pct_points": round(worst_segment_sum, 6),
            "best_segment_share_of_positive": round(best_share, 6),
            "robust_candidate": (
                overall["trades"] >= MIN_ROBUST_TRADES
                and overall["profit_factor"] > 1.0
                and overall["sum_net_return_pct"] > 0
                and positive_segments >= MIN_POSITIVE_SEGMENTS
            ),
        }
        aggregate[name] = {
            "summary": record,
            "per_segment": per_segment,
        }
        segment_results.extend(per_segment)

    summaries = [
        aggregate[v["variant"]]["summary"]
        for v in VARIANTS
    ]

    # Ranking rewards real positive expectancy and cross-segment consistency.
    def rank_key(r):
        robust_penalty = 0 if r["robust_candidate"] else 1
        positive_penalty = 0 if r["sum_net_return_pct"] > 0 else 1
        pf_penalty = 0 if r["profit_factor"] > 1.0 else 1
        sample_penalty = max(0, MIN_ROBUST_TRADES - r["trades"])
        segment_penalty = max(
            0,
            MIN_POSITIVE_SEGMENTS - r["positive_segments"]
        )

        return (
            robust_penalty,
            positive_penalty,
            pf_penalty,
            segment_penalty,
            sample_penalty,
            -r["positive_segments"],
            -r["profit_factor"],
            -r["sum_net_return_pct"],
            r["best_segment_share_of_positive"],
        )

    summaries.sort(key=rank_key)

    for rank, r in enumerate(summaries, start=1):
        r["rank"] = rank

    robust = [r for r in summaries if r["robust_candidate"]]

    summary_csv = ROOT / "r2fs_v3_regime_summary.csv"
    segment_csv = ROOT / "r2fs_v3_regime_per_segment.csv"
    summary_json = ROOT / "r2fs_v3_regime_summary.json"

    write_csv(summary_csv, summaries)
    write_csv(segment_csv, segment_results)

    report = {
        "mode": "post_filter_screening",
        "segments": 12,
        "variants": len(VARIANTS),
        "baseline_control": "M45_D72_MI72_F78",
        "minimum_robust_trades": MIN_ROBUST_TRADES,
        "minimum_positive_segments": MIN_POSITIVE_SEGMENTS,
        "robust_candidate_count": len(robust),
        "best": summaries[0] if summaries else None,
        "robust_candidates": robust,
        "all_results": summaries,
        "note": (
            "This screening filters the already-generated V2 FAST_ENTER trade "
            "set. A winning threshold combination must be re-run as an exact "
            "pre-entry filter before live/shadow use because removing trades "
            "can change MAX_OPEN/cooldown availability."
        ),
    }

    summary_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    if summaries:
        best = summaries[0]
        print(
            "R2FS_V3_BEST "
            + json.dumps(
                {
                    "variant": best["variant"],
                    "trades": best["trades"],
                    "wins": best["wins"],
                    "win_rate_pct": best["win_rate_pct"],
                    "profit_factor": best["profit_factor"],
                    "sum_net_return_pct": best["sum_net_return_pct"],
                    "positive_segments": best["positive_segments"],
                    "negative_segments": best["negative_segments"],
                    "worst_segment_sum_pct_points":
                        best["worst_segment_sum_pct_points"],
                    "robust_candidate": best["robust_candidate"],
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    print(
        f"R2FS_V3_ROBUST_COUNT {len(robust)}",
        flush=True,
    )

    print(
        f"R2FS_V3_DONE summary={summary_csv} "
        f"per_segment={segment_csv}",
        flush=True,
    )

    hold_forever()


if __name__ == "__main__":
    main()
