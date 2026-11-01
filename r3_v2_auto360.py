
"""
R3 V2 360d resume runner — replay stall fix.

Historical-only. No live/shadow DB writes. No orders.

Fixes:
1) Patch the local Render checkout of r3_historical_adapter.py at runtime so
   non-ACTIVE watches are compacted once per scan. The original replay already
   ignores non-ACTIVE watches, so this is semantics-preserving but prevents
   historical watch-list growth from making later months progressively slower.
2) Replace the broken blocking-readline watchdog with communicate(timeout=...),
   so a hung adapter is actually killed after R3_V2_ADAPTER_TIMEOUT_SEC.
3) Resume from segment 5 by default; segments 1-4 are hardcoded completed.
4) Download only 1m candles; locally aggregate 5m/15m/1h.
"""

from __future__ import annotations

import csv
import json
import os
import signal
import subprocess
import sys
import time
from collections import defaultdict
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

START_SEGMENT = int(os.environ.get("R3_V2_START_SEGMENT", "5"))
SOURCE_TIMEOUT_SEC = int(os.environ.get("R3_V2_SOURCE_TIMEOUT_SEC", "240"))
MAX_SEGMENT_ATTEMPTS = int(os.environ.get("R3_V2_SEGMENT_ATTEMPTS", "2"))
ADAPTER_TIMEOUT_SEC = int(os.environ.get("R3_V2_ADAPTER_TIMEOUT_SEC", "600"))
MIN_COVERAGE = float(os.environ.get("R3_V2_MIN_COVERAGE", "0.95"))


PRECOMPLETED = {
    1: {
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
    2: {
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
    3: {
        "segment": 3,
        "start": "2025-08-04T09:30:00+00:00",
        "end": "2025-09-03T09:30:00+00:00",
        "baseline": {
            "trades": 12, "wins": 6, "losses": 6,
            "win_rate_pct": 50.0, "profit_factor": 0.604,
            "gross_profit_pct_points": 4.095461,
            "gross_loss_abs_pct_points": 6.778795,
            "sum_net_return_pct": -2.683,
        },
        "v2": {
            "trades": 5, "wins": 2, "losses": 3,
            "win_rate_pct": 40.0, "profit_factor": 0.408,
            "gross_profit_pct_points": 1.445069,
            "gross_loss_abs_pct_points": 3.539279,
            "sum_net_return_pct": -2.094,
        },
        "delta_sum_net_return_pct": 0.589,
        "trade_retention_pct": 41.67,
    },
    4: {
        "segment": 4,
        "start": "2025-09-03T09:30:00+00:00",
        "end": "2025-10-03T09:30:00+00:00",
        "baseline": {
            "trades": 3, "wins": 1, "losses": 2,
            "win_rate_pct": 33.33, "profit_factor": 0.959,
            "gross_profit_pct_points": 0.854298,
            "gross_loss_abs_pct_points": 0.890987,
            "sum_net_return_pct": -0.037,
        },
        "v2": {
            "trades": 1, "wins": 0, "losses": 1,
            "win_rate_pct": 0.0, "profit_factor": 0.0,
            "gross_profit_pct_points": 0.0,
            "gross_loss_abs_pct_points": 0.774128,
            "sum_net_return_pct": -0.774,
        },
        "delta_sum_net_return_pct": -0.737,
        "trade_retention_pct": 33.33,
    },
}


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
                pass
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
        "win_rate_pct": round(100 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": round(pf, 3),
        "gross_profit_pct_points": round(gp, 6),
        "gross_loss_abs_pct_points": round(gl, 6),
        "sum_net_return_pct": round(sum(vals), 3),
    }


def _aggregate_metrics(segments, key):
    rows = [x[key] for x in segments]
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
        "win_rate_pct": round(100 * wins / trades, 2) if trades else 0.0,
        "profit_factor": round(pf, 3),
        "gross_profit_pct_points": round(gp, 6),
        "gross_loss_abs_pct_points": round(gl, 6),
        "sum_net_return_pct": round(total, 3),
    }


def _bounded(label, fn):
    def wrapped(*args, **kwargs):
        if SOURCE_TIMEOUT_SEC <= 0 or not hasattr(signal, "SIGALRM"):
            return fn(*args, **kwargs)

        old_handler = signal.getsignal(signal.SIGALRM)

        def alarm_handler(signum, frame):
            raise TimeoutError(f"{label} exceeded {SOURCE_TIMEOUT_SEC}s")

        try:
            signal.signal(signal.SIGALRM, alarm_handler)
            signal.setitimer(signal.ITIMER_REAL, SOURCE_TIMEOUT_SEC)
            return fn(*args, **kwargs)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)
    return wrapped


base.hd.download_symbol = _bounded("bingx_1m", base.hd.download_symbol)
base.download_mark_price_symbol = _bounded(
    "bingx_mark_price", base.download_mark_price_symbol
)
base.download_spot_history_symbol = _bounded(
    "bingx_historical_spot", base.download_spot_history_symbol
)


def _symbols():
    return [
        x.strip().upper()
        for x in base.SYMBOLS.split(",")
        if x.strip()
    ]


def _verify_1m(path: Path, start: datetime, end: datetime):
    seen = {s: set() for s in _symbols()}
    start_ms = base.hd.utc_ms(start)
    end_ms = base.hd.utc_ms(end)

    with path.open(newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            sym = r["symbol"]
            if sym not in seen:
                continue
            ts = base.hd.utc_ms(datetime.fromisoformat(r["timestamp"]))
            if start_ms <= ts < end_ms:
                seen[sym].add(ts)

    counts = {s: len(v) for s, v in seen.items()}
    expected = int((end - start).total_seconds() // 60)
    minimum = int(expected * MIN_COVERAGE)

    print(
        f"R3_V2_1M_COVERAGE expected={expected} "
        f"minimum={minimum} counts={counts}",
        flush=True,
    )

    if any(v < minimum for v in counts.values()):
        raise RuntimeError(
            f"Insufficient 1m history: {counts}; "
            f"need >= {minimum} per symbol"
        )


def _aggregate_from_1m(
    source: Path,
    target: Path,
    interval: str,
    start: datetime,
    end: datetime,
):
    step_ms = base.hd.INTERVAL_MS[interval]
    start_ms = base.hd.utc_ms(start)
    end_ms = base.hd.utc_ms(end)
    groups = {}

    with source.open(newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            ts_ms = base.hd.utc_ms(
                datetime.fromisoformat(r["timestamp"])
            )
            if not (start_ms <= ts_ms < end_ms):
                continue

            bucket = (ts_ms // step_ms) * step_ms
            key = (r["symbol"], bucket)

            g = groups.get(key)
            if g is None:
                groups[key] = {
                    "first_ts": ts_ms,
                    "last_ts": ts_ms,
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": float(r["volume"] or 0),
                    "quote_volume": float(r["quote_volume"] or 0),
                    "trades": int(float(r["trades"] or 0)),
                    "taker_buy_base": float(r["taker_buy_base"] or 0),
                    "taker_buy_quote": float(r["taker_buy_quote"] or 0),
                    "count": 1,
                }
            else:
                if ts_ms < g["first_ts"]:
                    g["first_ts"] = ts_ms
                    g["open"] = float(r["open"])
                if ts_ms >= g["last_ts"]:
                    g["last_ts"] = ts_ms
                    g["close"] = float(r["close"])
                g["high"] = max(g["high"], float(r["high"]))
                g["low"] = min(g["low"], float(r["low"]))
                g["volume"] += float(r["volume"] or 0)
                g["quote_volume"] += float(r["quote_volume"] or 0)
                g["trades"] += int(float(r["trades"] or 0))
                g["taker_buy_base"] += float(r["taker_buy_base"] or 0)
                g["taker_buy_quote"] += float(r["taker_buy_quote"] or 0)
                g["count"] += 1

    rows = []
    expected_per_bucket = step_ms // 60_000

    for (symbol, bucket), g in sorted(
        groups.items(),
        key=lambda x: (x[0][0], x[0][1]),
    ):
        if g["count"] != expected_per_bucket:
            continue

        rows.append({
            "timestamp": base.hd.iso_utc(bucket),
            "symbol": symbol,
            "interval": interval,
            "open": g["open"],
            "high": g["high"],
            "low": g["low"],
            "close": g["close"],
            "volume": g["volume"],
            "quote_volume": g["quote_volume"],
            "trades": g["trades"],
            "taker_buy_base": g["taker_buy_base"],
            "taker_buy_quote": g["taker_buy_quote"],
        })

    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=base.hd.FIELDS)
        w.writeheader()
        w.writerows(rows)

    counts = {s: 0 for s in _symbols()}
    for r in rows:
        counts[r["symbol"]] += 1

    expected = int(
        (end - start).total_seconds()
        // (step_ms / 1000)
    )
    minimum = max(
        0,
        int(expected * MIN_COVERAGE) - 2,
    )

    print(
        f"R3_V2_LOCAL_AGG interval={interval} "
        f"rows={len(rows)} expected={expected} "
        f"minimum={minimum} counts={counts}",
        flush=True,
    )

    if any(v < minimum for v in counts.values()):
        raise RuntimeError(
            f"Insufficient locally aggregated {interval}: "
            f"{counts}; need >= {minimum} per symbol"
        )


def _patch_replay_source():
    """
    Patch only the ephemeral Render checkout.

    This does NOT modify GitHub and does NOT touch live/shadow code.
    Non-ACTIVE watches are already ignored by the original replay loop, so
    removing them between scans does not change strategy decisions.
    """
    path = Path("r3_historical_adapter.py")
    text = path.read_text(encoding="utf-8")

    marker = "R3_HISTORICAL_WATCH_COMPACTION_FIX"
    if marker in text:
        print(
            "R3_V2_REPLAY_PATCH already_applied=true",
            flush=True,
        )
        return

    old = (
        "    while scan<=end:\n"
        "        ctx=market_context(c15_by,c1h_by,scan)\n"
    )

    new = (
        "    while scan<=end:\n"
        f"        # {marker}\n"
        "        # Non-ACTIVE watches are never used again; compact them so\n"
        "        # historical replay cost does not grow with every old setup.\n"
        "        if watches:\n"
        "            watches = [w for w in watches "
        "if w.get(\"status\") == \"ACTIVE\"]\n"
        "\n"
        "        ctx=market_context(c15_by,c1h_by,scan)\n"
    )

    if old not in text:
        raise RuntimeError(
            "Cannot patch r3_historical_adapter.py: "
            "run_replay anchor not found"
        )

    text = text.replace(old, new, 1)
    path.write_text(text, encoding="utf-8")

    compile(
        text,
        str(path),
        "exec",
    )

    print(
        "R3_V2_REPLAY_PATCH "
        "watch_compaction=true "
        "semantics_preserved=true "
        "ephemeral_only=true",
        flush=True,
    )


def _run_and_stream(cmd):
    """
    Real watchdog.

    Previous implementation blocked inside stdout.readline(), which meant the
    timeout check never ran while an adapter was CPU-bound and silent.
    """
    print(
        "R3_V2_RUN",
        " ".join(cmd),
        flush=True,
    )

    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    try:
        if ADAPTER_TIMEOUT_SEC > 0:
            output, _ = p.communicate(
                timeout=ADAPTER_TIMEOUT_SEC
            )
        else:
            output, _ = p.communicate()
    except subprocess.TimeoutExpired:
        p.kill()
        output, _ = p.communicate()

        if output:
            for line in output.splitlines():
                print(line, flush=True)

        raise TimeoutError(
            f"adapter exceeded "
            f"{ADAPTER_TIMEOUT_SEC}s: "
            f"{' '.join(cmd)}"
        )

    if output:
        for line in output.splitlines():
            print(line, flush=True)

    if p.returncode != 0:
        raise subprocess.CalledProcessError(
            p.returncode,
            cmd,
        )


def _run_segment(idx: int, end_iso: str) -> dict:
    end = _dt(end_iso)
    start = end - base.timedelta(days=30)

    seg = ROOT / f"segment_{idx:02d}"
    seg.mkdir(
        parents=True,
        exist_ok=True,
    )

    base.DATA = seg
    base.DAYS = 30

    print(
        f"R3_V2_AUTO360_SEGMENT_START "
        f"segment={idx}/12 "
        f"start={start.isoformat()} "
        f"end={end.isoformat()}",
        flush=True,
    )

    # Deterministic retry.
    for name in (
        "history_1m.csv",
        "history_5m.csv",
        "history_15m.csv",
        "history_1h.csv",
        "baseline_outcomes.csv",
        "baseline_detail.csv",
        "v2_outcomes.csv",
        "v2_detail.csv",
        "segment_summary.json",
    ):
        p = seg / name
        if p.exists():
            p.unlink()

    print(
        f"R3_V2_STAGE segment={idx}/12 "
        "stage=download_1m",
        flush=True,
    )

    base.download_interval(
        "1m",
        start,
        end,
    )

    print(
        f"R3_V2_STAGE_DONE segment={idx}/12 "
        "stage=download_1m",
        flush=True,
    )

    one = seg / "history_1m.csv"
    _verify_1m(
        one,
        start,
        end,
    )

    for interval in (
        "5m",
        "15m",
        "1h",
    ):
        print(
            f"R3_V2_STAGE segment={idx}/12 "
            f"stage=local_aggregate_{interval}",
            flush=True,
        )

        _aggregate_from_1m(
            one,
            seg / f"history_{interval}.csv",
            interval,
            start,
            end,
        )

        print(
            f"R3_V2_STAGE_DONE segment={idx}/12 "
            f"stage=local_aggregate_{interval}",
            flush=True,
        )

    common = [
        "--candles-1m",
        str(seg / "history_1m.csv"),
        "--candles-5m",
        str(seg / "history_5m.csv"),
        "--candles-15m",
        str(seg / "history_15m.csv"),
        "--candles-1h",
        str(seg / "history_1h.csv"),
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

    row = {
        "segment": idx,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "baseline": _metrics(
            _read_returns(baseline_out)
        ),
        "v2": _metrics(
            _read_returns(v2_out)
        ),
    }

    row["delta_sum_net_return_pct"] = round(
        row["v2"]["sum_net_return_pct"]
        - row["baseline"]["sum_net_return_pct"],
        3,
    )

    row["trade_retention_pct"] = (
        round(
            100
            * row["v2"]["trades"]
            / row["baseline"]["trades"],
            2,
        )
        if row["baseline"]["trades"]
        else 0.0
    )

    (
        seg
        / "segment_summary.json"
    ).write_text(
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

    return row


def _load_previous(start_segment: int):
    segments = []

    for idx in range(
        1,
        start_segment,
    ):
        if idx in PRECOMPLETED:
            segments.append(
                PRECOMPLETED[idx]
            )
            continue

        path = (
            ROOT
            / f"segment_{idx:02d}"
            / "segment_summary.json"
        )

        if not path.exists():
            raise RuntimeError(
                f"Cannot resume at {start_segment}: "
                f"segment {idx} summary missing"
            )

        segments.append(
            json.loads(
                path.read_text(
                    encoding="utf-8"
                )
            )
        )

    return segments


def main():
    os.environ.pop(
        "DATABASE_URL",
        None,
    )

    if not (
        1
        <= START_SEGMENT
        <= 12
    ):
        raise ValueError(
            "R3_V2_START_SEGMENT "
            "must be 1..12"
        )

    _patch_replay_source()

    segments = _load_previous(
        START_SEGMENT
    )

    print(
        "R3_V2_AUTO360_RESUME "
        f"start_segment={START_SEGMENT}/12 "
        f"precompleted={len(segments)} "
        f"source_timeout_sec={SOURCE_TIMEOUT_SEC} "
        f"adapter_timeout_sec={ADAPTER_TIMEOUT_SEC} "
        f"segment_attempts={MAX_SEGMENT_ATTEMPTS} "
        "higher_intervals=local_from_1m "
        "watch_compaction=true "
        "watchdog=communicate_timeout",
        flush=True,
    )

    for idx, end_iso in enumerate(
        END_DATES,
        start=1,
    ):
        if idx < START_SEGMENT:
            print(
                "R3_V2_AUTO360_SEGMENT_SKIP "
                f"segment={idx}/12 "
                "reason=precompleted",
                flush=True,
            )
            continue

        last_error = None

        for attempt in range(
            1,
            MAX_SEGMENT_ATTEMPTS + 1,
        ):
            try:
                print(
                    "R3_V2_AUTO360_ATTEMPT "
                    f"segment={idx}/12 "
                    f"attempt={attempt}/"
                    f"{MAX_SEGMENT_ATTEMPTS}",
                    flush=True,
                )

                row = _run_segment(
                    idx,
                    end_iso,
                )
                segments.append(row)
                last_error = None
                break

            except Exception as exc:
                last_error = exc

                print(
                    "R3_V2_AUTO360_SEGMENT_ERROR "
                    f"segment={idx}/12 "
                    f"attempt={attempt}/"
                    f"{MAX_SEGMENT_ATTEMPTS} "
                    f"error={type(exc).__name__}:{exc}",
                    flush=True,
                )

                if attempt < MAX_SEGMENT_ATTEMPTS:
                    time.sleep(10)

        if last_error is not None:
            raise RuntimeError(
                f"R3 V2 auto360 stopped at "
                f"segment {idx}/12: "
                f"{last_error}"
            )

    baseline = _aggregate_metrics(
        segments,
        "baseline",
    )
    v2 = _aggregate_metrics(
        segments,
        "v2",
    )

    positive_v2 = sum(
        1
        for x in segments
        if float(
            x["v2"]["sum_net_return_pct"]
        ) > 0
    )

    negative_v2 = sum(
        1
        for x in segments
        if float(
            x["v2"]["sum_net_return_pct"]
        ) < 0
    )

    final = {
        "segments_complete": len(segments),
        "segments_target": 12,
        "baseline": baseline,
        "v2": v2,
        "v2_positive_segments": positive_v2,
        "v2_negative_segments": negative_v2,
        "delta_sum_net_return_pct": round(
            v2["sum_net_return_pct"]
            - baseline["sum_net_return_pct"],
            3,
        ),
        "trade_retention_pct": (
            round(
                100
                * v2["trades"]
                / baseline["trades"],
                2,
            )
            if baseline["trades"]
            else 0.0
        ),
        "segments": segments,
    }

    (
        ROOT
        / "r3_v2_auto360_summary.json"
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
        "R3_V2_AUTO360_SUMMARY "
        + json.dumps(
            final,
            ensure_ascii=False,
            sort_keys=True,
        ),
        flush=True,
    )

    print(
        "R3_V2_AUTO360_DONE "
        f"segments={len(segments)}/12 "
        f"baseline_pf={baseline['profit_factor']:.3f} "
        f"v2_pf={v2['profit_factor']:.3f} "
        f"v2_sum={v2['sum_net_return_pct']:+.3f} "
        f"v2_positive_segments={positive_v2}/12",
        flush=True,
    )

    # Keep the web service alive without accidentally restarting the whole job.
    print(
        "R3_V2_AUTO360_HOLD "
        "process_complete=true "
        "restart_prevention=true",
        flush=True,
    )
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
