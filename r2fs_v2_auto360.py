
"""
R2FS 360-day loss diagnostic.

Historical-only research:
- Re-runs the same 12 x 30-day corrected R2FS V2 windows.
- Does NOT connect to the live/shadow DB.
- Does NOT place orders.
- Breaks resolved FAST_ENTER trades down by:
    segment, symbol, side, symbol+side, exit reason,
    radar-move bins, market/direction/micro/fast score bins.
- Computes PF correctly from total gross positive / abs(total gross negative).
- Produces pre-entry filter diagnostics only as screening; exit-reason breakdown
  is diagnosis only and must NOT be used as a look-ahead filter.

Designed to temporarily replace r2fs_v2_auto360.py on the historical R2FS service.
Outputs are written into data/r2fs_v2_auto360 so the existing HTTP server can serve
the reports without changing Render's start command.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path

ROOT = Path(os.environ.get("R2FS_DIAG_DIR", "data/r2fs_v2_auto360"))
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

MAX_ATTEMPTS = int(os.environ.get("R2FS_DIAG_SEGMENT_ATTEMPTS", "2"))
REQUEST_PAUSE = os.environ.get("R2FS_V2_REQUEST_PAUSE_SEC", "2.25")


def fnum(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def truthy(v):
    return str(v).strip().lower() in {"1", "true", "yes", "y"}


def metrics(rows):
    vals = [fnum(x.get("net_return_pct")) for x in rows]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl > 0 else (999.0 if gp > 0 else 0.0)
    n = len(vals)
    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / n, 2) if n else 0.0,
        "gross_profit_pct_points": round(gp, 6),
        "gross_loss_abs_pct_points": round(gl, 6),
        "profit_factor": round(pf, 6),
        "sum_net_return_pct": round(sum(vals), 6),
        "avg_net_return_pct": round(sum(vals) / n, 6) if n else 0.0,
    }


def write_csv(path: Path, rows: list[dict]):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def load_resolved(detail_path: Path, segment: int):
    out = []
    with detail_path.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row.get("decision") != "FAST_ENTER":
                continue
            if not truthy(row.get("resolved")):
                continue
            try:
                net = float(row["net_return_pct"])
            except Exception:
                continue
            out.append({
                "segment": segment,
                "timestamp": row.get("timestamp", ""),
                "symbol": row.get("symbol", ""),
                "side": row.get("side", ""),
                "exit_reason": row.get("exit_reason", ""),
                "radar_move_pct": fnum(row.get("radar_move_pct")),
                "market_score": fnum(row.get("market_score")),
                "direction_score": fnum(row.get("direction_score")),
                "micro_score": fnum(row.get("micro_score")),
                "execution_score": fnum(row.get("execution_score")),
                "fast_score": fnum(row.get("fast_score")),
                "net_return_pct": net,
                "gross_return_pct": fnum(row.get("gross_return_pct")),
                "tp1_hit": truthy(row.get("tp1_hit")),
                "tp2_hit": truthy(row.get("tp2_hit")),
                "tp3_hit": truthy(row.get("tp3_hit")),
            })
    return out


def group_stats(rows, name, key_fn):
    buckets = defaultdict(list)
    for r in rows:
        buckets[str(key_fn(r))].append(r)

    result = []
    for key, vals in buckets.items():
        s = metrics(vals)
        result.append({"dimension": name, "bucket": key, **s})

    result.sort(
        key=lambda x: (
            x["sum_net_return_pct"],
            x["profit_factor"],
            -x["trades"],
        )
    )
    return result


def score_bin(v, cuts):
    for lo, hi, label in cuts:
        if lo <= v < hi:
            return label
    return cuts[-1][2]


RADAR_BINS = [
    (0.0, 0.6, "<0.60"),
    (0.6, 0.8, "0.60-0.79"),
    (0.8, 1.0, "0.80-0.99"),
    (1.0, 1.5, "1.00-1.49"),
    (1.5, 2.0, "1.50-1.99"),
    (2.0, 9999, ">=2.00"),
]

MARKET_BINS = [
    (0, 50, "<50"),
    (50, 55, "50-54.99"),
    (55, 60, "55-59.99"),
    (60, 65, "60-64.99"),
    (65, 70, "65-69.99"),
    (70, 1000, ">=70"),
]

DIRECTION_BINS = [
    (0, 75, "<75"),
    (75, 78, "75-77.99"),
    (78, 81, "78-80.99"),
    (81, 84, "81-83.99"),
    (84, 90, "84-89.99"),
    (90, 1000, ">=90"),
]

MICRO_BINS = DIRECTION_BINS

FAST_BINS = [
    (0, 80, "<80"),
    (80, 82, "80-81.99"),
    (82, 84, "82-83.99"),
    (84, 86, "84-85.99"),
    (86, 90, "86-89.99"),
    (90, 1000, ">=90"),
]


def preentry_rule_screen(rows):
    """
    Screen interpretable PRE-ENTRY exclusions.
    This is post-filter screening only; any promising rule must be replayed
    exactly later because rejecting a trade frees MAX_OPEN/cooldown capacity.
    """
    rules = []

    symbols = sorted({r["symbol"] for r in rows if r["symbol"]})
    for symbol in symbols:
        kept = [r for r in rows if r["symbol"] != symbol]
        rules.append({
            "rule": f"EXCLUDE_SYMBOL:{symbol}",
            "removed_trades": len(rows) - len(kept),
            **metrics(kept),
        })

    for side in ("LONG", "SHORT"):
        kept = [r for r in rows if r["side"] != side]
        rules.append({
            "rule": f"EXCLUDE_SIDE:{side}",
            "removed_trades": len(rows) - len(kept),
            **metrics(kept),
        })

    for label, fn in [
        ("RADAR_LT_0_60", lambda r: abs(r["radar_move_pct"]) >= 0.60),
        ("RADAR_LT_0_80", lambda r: abs(r["radar_move_pct"]) >= 0.80),
        ("MARKET_LT_50", lambda r: r["market_score"] >= 50),
        ("MARKET_LT_55", lambda r: r["market_score"] >= 55),
        ("MARKET_LT_60", lambda r: r["market_score"] >= 60),
        ("MARKET_LT_65", lambda r: r["market_score"] >= 65),
        ("MARKET_LT_70", lambda r: r["market_score"] >= 70),
        ("DIRECTION_LT_78", lambda r: r["direction_score"] >= 78),
        ("DIRECTION_LT_84", lambda r: r["direction_score"] >= 84),
        ("MICRO_LT_78", lambda r: r["micro_score"] >= 78),
        ("MICRO_LT_84", lambda r: r["micro_score"] >= 84),
        ("FAST_LT_82", lambda r: r["fast_score"] >= 82),
        ("FAST_LT_86", lambda r: r["fast_score"] >= 86),
    ]:
        kept = [r for r in rows if fn(r)]
        rules.append({
            "rule": label,
            "removed_trades": len(rows) - len(kept),
            **metrics(kept),
        })

    # Rank by actual remaining expectancy first, then PF, but retain sample size.
    rules.sort(
        key=lambda x: (
            x["sum_net_return_pct"] <= 0,
            x["profit_factor"] <= 1,
            -x["sum_net_return_pct"],
            -x["profit_factor"],
            -x["trades"],
        )
    )
    return rules


def run_segment(idx: int, seg_id: str, end_iso: str):
    seg_dir = ROOT / f"diag_segment_{seg_id}"
    seg_dir.mkdir(parents=True, exist_ok=True)

    detail = seg_dir / "r2fs_v2_history_detail.csv"
    outcomes = seg_dir / "r2fs_v2_core_outcomes.csv"

    env = os.environ.copy()
    env.pop("DATABASE_URL", None)
    env["R2FS_V2_DAYS"] = "30"
    env["R2FS_V2_END_ISO"] = end_iso
    env["R2FS_V2_REQUEST_PAUSE_SEC"] = REQUEST_PAUSE
    env["R2FS_V2_DATA_DIR"] = str(seg_dir)

    print(
        f"R2FS_DIAG_SEGMENT_START segment={idx}/12 end={end_iso}",
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
        raise RuntimeError(f"segment {idx}/12 failed rc={rc}")
    if not detail.exists():
        raise RuntimeError(f"segment {idx}/12 missing detail csv")

    rows = load_resolved(detail, idx)
    s = metrics(rows)
    print(
        "R2FS_DIAG_SEGMENT_DONE "
        + json.dumps(
            {"segment": idx, **s},
            separators=(",", ":"),
        ),
        flush=True,
    )
    return rows


def main():
    os.environ.pop("DATABASE_URL", None)

    print(
        "R2FS_DIAG_START segments=12 historical_only=true live_shadow_untouched=true",
        flush=True,
    )

    all_rows = []

    for idx, (seg_id, end_iso) in enumerate(SEGMENTS, start=1):
        last = None
        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                print(
                    f"R2FS_DIAG_ATTEMPT segment={idx}/12 attempt={attempt}/{MAX_ATTEMPTS}",
                    flush=True,
                )
                rows = run_segment(idx, seg_id, end_iso)
                all_rows.extend(rows)
                last = None
                break
            except Exception as exc:
                last = exc
                print(
                    f"R2FS_DIAG_SEGMENT_ERROR segment={idx}/12 "
                    f"attempt={attempt}/{MAX_ATTEMPTS} "
                    f"error={type(exc).__name__}:{exc}",
                    flush=True,
                )
                if attempt < MAX_ATTEMPTS:
                    time.sleep(10)
        if last is not None:
            raise RuntimeError(
                f"R2FS diagnostic stopped at segment {idx}/12: {last}"
            )

    baseline = metrics(all_rows)
    print(
        "R2FS_DIAG_BASELINE "
        + json.dumps(baseline, separators=(",", ":")),
        flush=True,
    )

    breakdown = []
    breakdown += group_stats(all_rows, "segment", lambda r: r["segment"])
    breakdown += group_stats(all_rows, "symbol", lambda r: r["symbol"])
    breakdown += group_stats(all_rows, "side", lambda r: r["side"])
    breakdown += group_stats(
        all_rows, "symbol_side", lambda r: f'{r["symbol"]}:{r["side"]}'
    )
    breakdown += group_stats(
        all_rows, "exit_reason", lambda r: r["exit_reason"]
    )
    breakdown += group_stats(
        all_rows, "radar_move_bin",
        lambda r: score_bin(abs(r["radar_move_pct"]), RADAR_BINS)
    )
    breakdown += group_stats(
        all_rows, "market_score_bin",
        lambda r: score_bin(r["market_score"], MARKET_BINS)
    )
    breakdown += group_stats(
        all_rows, "direction_score_bin",
        lambda r: score_bin(r["direction_score"], DIRECTION_BINS)
    )
    breakdown += group_stats(
        all_rows, "micro_score_bin",
        lambda r: score_bin(r["micro_score"], MICRO_BINS)
    )
    breakdown += group_stats(
        all_rows, "fast_score_bin",
        lambda r: score_bin(r["fast_score"], FAST_BINS)
    )

    screen = preentry_rule_screen(all_rows)

    write_csv(ROOT / "r2fs_diag_trades.csv", all_rows)
    write_csv(ROOT / "r2fs_diag_breakdown.csv", breakdown)
    write_csv(ROOT / "r2fs_diag_preentry_screen.csv", screen)

    report = {
        "baseline": baseline,
        "worst_breakdown_buckets": breakdown[:50],
        "preentry_screen_top": screen[:30],
        "notes": [
            "exit_reason is diagnostic only; it cannot be used as a pre-entry filter",
            "post-filter screens must be exact-replayed before strategy changes",
            "historical-only; live/shadow untouched",
        ],
    }
    (ROOT / "r2fs_diag_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    # Log the most important diagnosis so results are recoverable even if
    # the instance filesystem is later lost.
    for row in breakdown:
        if row["dimension"] in {
            "symbol", "side", "symbol_side", "exit_reason",
            "radar_move_bin", "market_score_bin",
            "direction_score_bin", "micro_score_bin", "fast_score_bin",
        }:
            print(
                "R2FS_DIAG_BUCKET "
                + json.dumps(row, separators=(",", ":")),
                flush=True,
            )

    for row in screen[:20]:
        print(
            "R2FS_DIAG_RULE "
            + json.dumps(row, separators=(",", ":")),
            flush=True,
        )

    print(
        "R2FS_DIAG_DONE "
        "breakdown=data/r2fs_v2_auto360/r2fs_diag_breakdown.csv "
        "screen=data/r2fs_v2_auto360/r2fs_diag_preentry_screen.csv "
        "report=data/r2fs_v2_auto360/r2fs_diag_report.json",
        flush=True,
    )


if __name__ == "__main__":
    main()
