from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# R2FS V2 automatic 360-day historical runner.
# Historical-only:
# - 12 independent 30-day segments
# - each segment gets its own directory
# - no DATABASE_URL / no orders / no live-forward-shadow writes
# - retries a failed segment once, then stops safely without looping old results

ROOT = Path("data/r2fs_v2_auto360")
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

SUMMARY_PATH = ROOT / "r2fs_v2_360_summary.json"
RESULTS_PATH = ROOT / "r2fs_v2_360_segments.json"


def hold_forever():
    while True:
        time.sleep(3600)


def parse_result(line: str):
    prefix = "R2FS_V2_RESULT "
    if not line.startswith(prefix):
        return None
    raw = line[len(prefix):].strip()
    # Output is a Python dict with only primitive numeric values.
    import ast
    obj = ast.literal_eval(raw)
    return {
        "details": int(obj.get("details", 0)),
        "closed_fast_entries": int(obj.get("closed_fast_entries", 0)),
        "win_rate_pct": float(obj.get("win_rate_pct", 0.0)),
        "profit_factor": float(obj.get("profit_factor", 0.0)),
        "sum_net_return_pct": float(obj.get("sum_net_return_pct", 0.0)),
    }


def read_exact_outcome_stats(detail_csv: Path):
    trades = 0
    wins = 0
    positive = 0.0
    negative_abs = 0.0
    sum_net = 0.0

    if not detail_csv.exists():
        return {
            "trades": 0,
            "wins": 0,
            "gross_positive_net_pct": 0.0,
            "gross_negative_abs_net_pct": 0.0,
            "sum_net_return_pct": 0.0,
        }

    with detail_csv.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if str(row.get("decision", "")).strip() != "FAST_ENTER":
                continue
            resolved = str(row.get("resolved", "")).strip().lower()
            if resolved not in {"true", "1", "yes"}:
                continue
            raw = str(row.get("net_return_pct", "")).strip()
            if not raw:
                continue
            try:
                net = float(raw)
            except ValueError:
                continue

            trades += 1
            sum_net += net
            if net > 0:
                wins += 1
                positive += net
            elif net < 0:
                negative_abs += abs(net)

    return {
        "trades": trades,
        "wins": wins,
        "gross_positive_net_pct": round(positive, 9),
        "gross_negative_abs_net_pct": round(negative_abs, 9),
        "sum_net_return_pct": round(sum_net, 9),
    }


def save_results(results):
    RESULTS_PATH.write_text(
        json.dumps(results, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def run_segment(index: int, seg_id: str, end_iso: str):
    seg_dir = ROOT / f"segment_{seg_id}"
    seg_dir.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["R2FS_V2_DAYS"] = "30"
    env["R2FS_V2_END_ISO"] = end_iso
    env["R2FS_V2_REQUEST_PAUSE_SEC"] = env.get(
        "R2FS_V2_REQUEST_PAUSE_SEC", "2.25"
    )
    env["R2FS_V2_DATA_DIR"] = str(seg_dir)

    print(
        f"R2FS_V2_AUTO_SEGMENT_START segment={index}/12 "
        f"end={end_iso} dir={seg_dir}",
        flush=True,
    )

    result = None
    saw_done = False

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
        parsed = parse_result(line.strip())
        if parsed is not None:
            result = parsed
        if line.strip() == "R2FS_V2_STAGE1_DONE":
            saw_done = True

    rc = proc.wait()

    if rc != 0 or result is None or not saw_done:
        raise RuntimeError(
            f"segment {index}/12 failed rc={rc} "
            f"result={result is not None} done={saw_done}"
        )

    exact = read_exact_outcome_stats(
        seg_dir / "r2fs_v2_history_detail.csv"
    )

    record = {
        "segment": index,
        "end_iso": end_iso,
        "data_dir": str(seg_dir),
        "result": result,
        "exact_from_detail": exact,
    }

    print(
        f"R2FS_V2_AUTO_SEGMENT_DONE segment={index}/12 "
        f"trades={exact['trades']} wins={exact['wins']} "
        f"sum={exact['sum_net_return_pct']}",
        flush=True,
    )
    return record


def build_summary(results):
    trades = sum(
        x["exact_from_detail"]["trades"] for x in results
    )
    wins = sum(
        x["exact_from_detail"]["wins"] for x in results
    )
    positive = sum(
        x["exact_from_detail"]["gross_positive_net_pct"]
        for x in results
    )
    negative_abs = sum(
        x["exact_from_detail"]["gross_negative_abs_net_pct"]
        for x in results
    )
    sum_net = sum(
        x["exact_from_detail"]["sum_net_return_pct"]
        for x in results
    )

    win_rate = (wins / trades * 100.0) if trades else 0.0
    pf = (
        positive / negative_abs
        if negative_abs > 0
        else (999.0 if positive > 0 else 0.0)
    )

    return {
        "segments_complete": len(results),
        "segments_target": 12,
        "trades": trades,
        "wins": wins,
        "losses": max(0, trades - wins),
        "win_rate_pct": round(win_rate, 6),
        "profit_factor_exact_from_net_returns": round(pf, 6),
        "gross_positive_net_pct": round(positive, 9),
        "gross_negative_abs_net_pct": round(negative_abs, 9),
        "sum_net_return_pct": round(sum_net, 9),
        "note": (
            "sum_net_return_pct is the sum of per-trade percentage outcomes, "
            "not account equity return."
        ),
    }


def main():
    # Safety: this runner must not use the live DB.
    os.environ.pop("DATABASE_URL", None)

    print(
        "R2FS_V2_AUTO360_START segments=12 days_each=30 "
        "historical_only=true",
        flush=True,
    )

    results = []

    for idx, (seg_id, end_iso) in enumerate(SEGMENTS, start=1):
        success = False
        last_error = None

        for attempt in (1, 2):
            try:
                print(
                    f"R2FS_V2_AUTO_ATTEMPT segment={idx}/12 "
                    f"attempt={attempt}/2",
                    flush=True,
                )
                record = run_segment(idx, seg_id, end_iso)
                results.append(record)
                save_results(results)
                success = True
                break
            except Exception as exc:
                last_error = repr(exc)
                print(
                    f"R2FS_V2_AUTO_RETRYABLE_ERROR segment={idx}/12 "
                    f"attempt={attempt}/2 error={last_error}",
                    flush=True,
                )
                if attempt == 1:
                    time.sleep(20)

        if not success:
            print(
                f"R2FS_V2_AUTO360_STOPPED segment={idx}/12 "
                f"error={last_error}",
                flush=True,
            )
            # Hold the web service instead of exiting/restarting and
            # accidentally re-running earlier segments.
            hold_forever()

        if idx < 12:
            print(
                f"R2FS_V2_AUTO_NEXT from={idx}/12 to={idx+1}/12",
                flush=True,
            )
            time.sleep(5)

    summary = build_summary(results)
    SUMMARY_PATH.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        "R2FS_V2_AUTO360_SUMMARY " + json.dumps(
            summary, ensure_ascii=False, separators=(",", ":")
        ),
        flush=True,
    )
    print("R2FS_V2_AUTO360_DONE segments=12/12", flush=True)

    # Keep Render alive after completion; do not auto-repeat 360 days.
    hold_forever()


if __name__ == "__main__":
    main()
