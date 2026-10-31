
"""
R2FS Exact Replay — Radar >= 0.80%, full 360-day validation.

Historical-only.
- No DATABASE_URL
- No live/shadow writes
- No orders
- Applies the Radar gate BEFORE scoring/entry, so MAX_OPEN/cooldown capacity
  is replayed exactly rather than post-filtered.
- Uses the existing corrected R2FS V2 replay engine unchanged otherwise.
"""

from __future__ import annotations

import csv
import json
import os
import time
from datetime import timedelta
from pathlib import Path

import r2fs_historical_adapter_v2 as adapter
import r2fs_historical_v2_job as job


ROOT = Path(
    os.environ.get(
        "R2FS_RADAR080_DIR",
        "data/r2fs_v2_auto360",
    )
)
ROOT.mkdir(parents=True, exist_ok=True)

RADAR_MIN_MOVE = float(
    os.environ.get("R2FS_EXACT_RADAR_MIN_MOVE", "0.80")
)
REQUEST_PAUSE = float(
    os.environ.get("R2FS_V2_REQUEST_PAUSE_SEC", "2.25")
)
MAX_ATTEMPTS = int(
    os.environ.get("R2FS_EXACT_SEGMENT_ATTEMPTS", "2")
)

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


def truthy(v):
    return str(v).strip().lower() in {
        "1", "true", "yes", "y"
    }


def read_resolved(path: Path):
    rows = []
    with path.open(
        newline="",
        encoding="utf-8-sig",
    ) as f:
        for row in csv.DictReader(f):
            if row.get("decision") != "FAST_ENTER":
                continue
            if not truthy(row.get("resolved")):
                continue
            try:
                net = float(row["net_return_pct"])
            except Exception:
                continue
            rows.append({
                "timestamp": row.get("timestamp", ""),
                "symbol": row.get("symbol", ""),
                "side": row.get("side", ""),
                "exit_reason": row.get("exit_reason", ""),
                "radar_move_pct": float(
                    row.get("radar_move_pct") or 0.0
                ),
                "net_return_pct": net,
            })
    return rows


def metrics(rows):
    vals = [
        float(x["net_return_pct"])
        for x in rows
    ]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]

    gp = sum(wins)
    gl = abs(sum(losses))
    pf = (
        gp / gl
        if gl > 0
        else (999.0 if gp > 0 else 0.0)
    )

    n = len(vals)

    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (
            round(100.0 * len(wins) / n, 2)
            if n else 0.0
        ),
        "gross_positive_net_pct": round(gp, 6),
        "gross_negative_abs_net_pct": round(gl, 6),
        "profit_factor": round(pf, 6),
        "sum_net_return_pct": round(sum(vals), 6),
        "avg_net_return_pct": (
            round(sum(vals) / n, 6)
            if n else 0.0
        ),
    }


def configure_segment(seg_dir: Path, end_iso: str):
    job.DATA = seg_dir
    job.DAYS = 30
    job.END_ISO = end_iso
    job.PAUSE = REQUEST_PAUSE

    job.recovery.DATA = seg_dir
    job.recovery.DAYS = 30
    job.recovery.REQUEST_PAUSE = REQUEST_PAUSE
    job.recovery.SYMBOLS = ",".join(job.SYMBOLS)


def run_segment(index: int, seg_id: str, end_iso: str):
    seg_dir = ROOT / f"radar080_segment_{seg_id}"
    seg_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    end = job.parse_dt(end_iso)
    start = end - timedelta(days=30)

    print(
        "R2FS_EXACT080_SEGMENT_START "
        f"segment={index}/12 "
        f"start={start.isoformat()} "
        f"end={end.isoformat()}",
        flush=True,
    )

    # Deterministic retry: remove this segment's previous outputs/candles.
    for name in (
        "history_1m.csv",
        "history_5m.csv",
        "r2fs_v2_history_detail.csv",
        "r2fs_v2_core_outcomes.csv",
        "segment_summary.json",
    ):
        p = seg_dir / name
        if p.exists():
            p.unlink()

    configure_segment(
        seg_dir,
        end_iso,
    )

    # Same corrected historical data path used by the completed V2 diagnostic.
    print(
        f"R2FS_EXACT080_STAGE segment={index}/12 stage=download_1m",
        flush=True,
    )
    job.recovery.download_interval(
        "1m",
        start,
        end,
    )
    print(
        f"R2FS_EXACT080_STAGE_DONE segment={index}/12 stage=download_1m",
        flush=True,
    )

    print(
        f"R2FS_EXACT080_STAGE segment={index}/12 stage=download_5m",
        flush=True,
    )
    job.recovery.download_interval(
        "5m",
        start,
        end,
    )
    job.recovery.recover_interval_from_1m(
        "5m",
        start,
        end,
    )
    print(
        f"R2FS_EXACT080_STAGE_DONE segment={index}/12 stage=download_5m",
        flush=True,
    )

    c1 = adapter.v1.read_candles(
        seg_dir / "history_1m.csv"
    )
    c5 = adapter.v1.read_candles(
        seg_dir / "history_5m.csv"
    )

    old_radar = float(
        adapter.v1.RADAR_MIN_MOVE
    )
    old_core = adapter.v1.CORE

    try:
        # This is the actual strategy change being validated.
        adapter.v1.RADAR_MIN_MOVE = RADAR_MIN_MOVE

        print(
            "R2FS_EXACT080_REPLAY "
            f"segment={index}/12 "
            f"radar_min_move={RADAR_MIN_MOVE:.2f} "
            "capacity_replayed=true",
            flush=True,
        )

        details = adapter.replay_v2(
            c1,
            c5,
            0.12,   # same assumed spread
            0.05,   # same adverse chase
            24.0,   # same runner cap
        )

        detail_path = (
            seg_dir
            / "r2fs_v2_history_detail.csv"
        )
        outcomes_path = (
            seg_dir
            / "r2fs_v2_core_outcomes.csv"
        )

        adapter.v1.CORE = (
            "R2FS_V2_EXACT_RADAR080"
        )
        result = adapter.v1.write_outputs(
            details,
            str(detail_path),
            str(outcomes_path),
        )

    finally:
        adapter.v1.RADAR_MIN_MOVE = old_radar
        adapter.v1.CORE = old_core

    rows = read_resolved(
        seg_dir
        / "r2fs_v2_history_detail.csv"
    )
    stat = metrics(rows)

    summary = {
        "segment": index,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "radar_min_move": RADAR_MIN_MOVE,
        **stat,
    }

    (
        seg_dir
        / "segment_summary.json"
    ).write_text(
        json.dumps(
            summary,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(
        "R2FS_EXACT080_SEGMENT_DONE "
        + json.dumps(
            summary,
            separators=(",", ":"),
        ),
        flush=True,
    )

    return rows, summary


def hold_forever():
    print(
        "R2FS_EXACT080_HOLD "
        "process_complete=true "
        "restart_prevention=true",
        flush=True,
    )
    while True:
        time.sleep(3600)


def main():
    os.environ.pop(
        "DATABASE_URL",
        None,
    )

    print(
        "R2FS_EXACT080_START "
        f"segments=12 "
        f"radar_min_move={RADAR_MIN_MOVE:.2f} "
        "historical_only=true "
        "live_shadow_untouched=true "
        "post_filter=false "
        "capacity_replayed=true",
        flush=True,
    )

    all_rows = []
    segment_summaries = []

    for index, (seg_id, end_iso) in enumerate(
        SEGMENTS,
        start=1,
    ):
        last_error = None

        for attempt in range(
            1,
            MAX_ATTEMPTS + 1,
        ):
            try:
                print(
                    "R2FS_EXACT080_ATTEMPT "
                    f"segment={index}/12 "
                    f"attempt={attempt}/{MAX_ATTEMPTS}",
                    flush=True,
                )

                rows, summary = run_segment(
                    index,
                    seg_id,
                    end_iso,
                )

                all_rows.extend(rows)
                segment_summaries.append(
                    summary
                )
                last_error = None
                break

            except Exception as exc:
                last_error = exc
                print(
                    "R2FS_EXACT080_SEGMENT_ERROR "
                    f"segment={index}/12 "
                    f"attempt={attempt}/{MAX_ATTEMPTS} "
                    f"error={type(exc).__name__}:{exc}",
                    flush=True,
                )

                if attempt < MAX_ATTEMPTS:
                    time.sleep(10)

        if last_error is not None:
            raise RuntimeError(
                f"R2FS exact080 stopped at "
                f"segment {index}/12: "
                f"{last_error}"
            )

    overall = metrics(
        all_rows
    )

    positive_segments = sum(
        1
        for x in segment_summaries
        if float(
            x["sum_net_return_pct"]
        ) > 0
    )
    negative_segments = sum(
        1
        for x in segment_summaries
        if float(
            x["sum_net_return_pct"]
        ) < 0
    )
    worst_segment = min(
        (
            float(x["sum_net_return_pct"])
            for x in segment_summaries
        ),
        default=0.0,
    )

    # Previously agreed R2FS keep criteria.
    keep_candidate = (
        overall["trades"] >= 200
        and overall["profit_factor"] >= 1.15
        and overall["sum_net_return_pct"] > 0
        and positive_segments >= 7
    )

    final = {
        "radar_min_move": RADAR_MIN_MOVE,
        **overall,
        "positive_segments": positive_segments,
        "negative_segments": negative_segments,
        "worst_segment_sum_net_return_pct":
            round(worst_segment, 6),
        "keep_candidate": keep_candidate,
        "criteria": {
            "min_trades": 200,
            "min_profit_factor": 1.15,
            "sum_net_return_positive": True,
            "min_positive_segments": 7,
        },
        "segments": segment_summaries,
    }

    (
        ROOT
        / "r2fs_exact080_final.json"
    ).write_text(
        json.dumps(
            final,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(
        "R2FS_EXACT080_SUMMARY "
        + json.dumps(
            final,
            separators=(",", ":"),
        ),
        flush=True,
    )
    print(
        "R2FS_EXACT080_DONE "
        f"segments=12/12 "
        f"trades={overall['trades']} "
        f"pf={overall['profit_factor']:.6f} "
        f"sum={overall['sum_net_return_pct']:+.6f} "
        f"positive_segments={positive_segments}/12 "
        f"keep_candidate={keep_candidate}",
        flush=True,
    )

    hold_forever()


if __name__ == "__main__":
    main()
