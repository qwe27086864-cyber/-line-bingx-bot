from __future__ import annotations

import csv
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import historical_data as hd

SYMBOLS = os.environ.get(
    "HIST_SYMBOLS",
    "BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT",
)
DAYS = int(os.environ.get("HIST_DAYS", "30"))
END_ISO = os.environ.get("HIST_END_ISO", "").strip()
SEGMENTS = int(os.environ.get("HIST_SEGMENTS", "1"))
REQUEST_PAUSE = float(os.environ.get("HIST_REQUEST_PAUSE_SEC", "2.25"))
DATA = Path(os.environ.get("HIST_DATA_DIR", "data"))
RESULTS = DATA / "window_results"
DATA.mkdir(parents=True, exist_ok=True)
RESULTS.mkdir(parents=True, exist_ok=True)

WINDOW_FILES = [
    "history_1m.csv",
    "history_5m.csv",
    "history_15m.csv",
    "history_1h.csv",
    "market_features.csv",
    "r2fs_core_outcomes.csv",
    "r3_core_outcomes.csv",
    "all_core_outcomes.csv",
    "router_v1_report.json",
]


def run(cmd):
    print("RUN", " ".join(map(str, cmd)), flush=True)
    subprocess.run(list(map(str, cmd)), check=True)


def parse_end(value: str) -> datetime:
    if not value:
        return datetime.now(timezone.utc)
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def clean_window_files() -> None:
    """Prevent adjacent 30-day windows from being mixed together."""
    for name in WINDOW_FILES:
        p = DATA / name
        if p.exists():
            p.unlink()


def download_interval(interval: str, start: datetime, end: datetime) -> None:
    out = DATA / f"history_{interval}.csv"
    existing = hd.load_existing_keys(out)
    start_ms = hd.utc_ms(start)
    end_ms = hd.utc_ms(end)
    symbols = [s.strip().upper() for s in SYMBOLS.split(",") if s.strip()]
    total_new = 0

    print(
        f"WINDOW interval={interval} start={start.isoformat()} "
        f"end={end.isoformat()} days={DAYS}",
        flush=True,
    )

    for i, symbol in enumerate(symbols):
        rows = hd.download_symbol(
            symbol,
            interval,
            start_ms,
            end_ms,
            REQUEST_PAUSE,
        )
        fresh = [
            r for r in rows
            if (r["timestamp"], r["symbol"], r["interval"]) not in existing
        ]
        total_new += hd.append_rows(out, fresh)
        existing.update(
            (r["timestamp"], r["symbol"], r["interval"]) for r in fresh
        )
        print(
            f"{symbol}: fetched={len(rows)} new={len(fresh)} "
            f"interval={interval}",
            flush=True,
        )
        if i + 1 < len(symbols):
            time.sleep(max(0.0, REQUEST_PAUSE))

    print(f"saved {total_new} new rows -> {out}", flush=True)


def recover_5m_from_1m(start: datetime, end: datetime) -> None:
    source = DATA / "history_1m.csv"
    target = DATA / "history_5m.csv"
    if not source.exists():
        raise RuntimeError("Cannot recover 5m candles: 1m CSV is missing")

    existing = hd.load_existing_keys(target)
    recovered = []
    group_key = None
    group = []
    start_ms, end_ms = hd.utc_ms(start), hd.utc_ms(end)

    def flush() -> None:
        nonlocal group
        if not group:
            return
        symbol, bucket = group_key
        times = [hd.utc_ms(datetime.fromisoformat(r["timestamp"])) for r in group]
        if len(times) != 5 or times != [bucket + n * 60_000 for n in range(5)]:
            return
        ts = hd.iso_utc(bucket)
        key = (ts, symbol, "5m")
        if key in existing:
            return
        first, last = group[0], group[-1]
        recovered.append({
            "timestamp": ts,
            "symbol": symbol,
            "interval": "5m",
            "open": float(first["open"]),
            "high": max(float(r["high"]) for r in group),
            "low": min(float(r["low"]) for r in group),
            "close": float(last["close"]),
            "volume": sum(float(r["volume"]) for r in group),
            "quote_volume": sum(float(r["quote_volume"]) for r in group),
            "trades": sum(int(float(r["trades"])) for r in group),
            "taker_buy_base": sum(float(r["taker_buy_base"]) for r in group),
            "taker_buy_quote": sum(float(r["taker_buy_quote"]) for r in group),
        })
        existing.add(key)

    with source.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            ts_ms = hd.utc_ms(datetime.fromisoformat(row["timestamp"]))
            if ts_ms < start_ms or ts_ms >= end_ms:
                continue
            key = (row["symbol"], (ts_ms // 300_000) * 300_000)
            if key != group_key:
                flush()
                group_key, group = key, []
            group.append(row)
        flush()

    added = hd.append_rows(target, recovered)
    print(f"5m recovery: aggregated {added} complete bars from 1m", flush=True)

    counts = {s: 0 for s in (x.strip().upper() for x in SYMBOLS.split(",")) if s}
    with target.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row["symbol"] in counts:
                counts[row["symbol"]] += 1
    expected = int((end - start).total_seconds() // 300)
    minimum = int(expected * 0.95)
    print(f"5m coverage: expected={expected} minimum={minimum} counts={counts}", flush=True)
    if any(n < minimum for n in counts.values()):
        raise RuntimeError(
            f"Insufficient 5m history after 1m recovery: {counts}; "
            f"need >= {minimum} per symbol. Stop rather than backtest incomplete data."
        )


def archive_window(index: int, start: datetime, end: datetime) -> Path:
    tag = f"{index:02d}_{start:%Y%m%dT%H%M%SZ}_{end:%Y%m%dT%H%M%SZ}"
    dest = RESULTS / tag
    dest.mkdir(parents=True, exist_ok=True)

    saved = []
    for name in WINDOW_FILES:
        src = DATA / name
        if src.exists():
            shutil.copy2(src, dest / name)
            saved.append(name)

    meta = {
        "segment_index": index,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days": DAYS,
        "saved_files": saved,
    }
    (dest / "segment_meta.json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )
    return dest


def run_window(index: int, start: datetime, end: datetime) -> None:
    clean_window_files()
    print(
        f"SEGMENT_START index={index}/{SEGMENTS} "
        f"start={start.isoformat()} end={end.isoformat()} days={DAYS}",
        flush=True,
    )

    for interval in ("1m", "5m", "15m", "1h"):
        download_interval(interval, start, end)

    recover_5m_from_1m(start, end)

    run([
        sys.executable,
        "market_regime.py",
        "--candles",
        DATA / "history_5m.csv",
        "--out",
        DATA / "market_features.csv",
    ])

    if os.environ.get("DATABASE_URL"):
        run([
            sys.executable,
            "historical_db_forward_export.py",
            "--out-dir",
            DATA,
        ])
    else:
        print(
            "DATABASE_URL not set: BF1/AS1/MEME1/MEME2 DB export skipped",
            flush=True,
        )

    run([
        sys.executable,
        "historical_suite.py",
        "--data-dir",
        DATA,
        "--out",
        DATA / "all_core_outcomes.csv",
    ])

    out = DATA / "all_core_outcomes.csv"
    if out.exists() and out.stat().st_size > 0:
        run([
            sys.executable,
            "router_trainer.py",
            "--features",
            DATA / "market_features.csv",
            "--outcomes",
            out,
            "--out",
            DATA / "router_v1_report.json",
        ])

    if (DATA / "router_v1_report.json").exists():
        print(
            (DATA / "router_v1_report.json").read_text(encoding="utf-8"),
            flush=True,
        )
    else:
        print(
            "ROUTER REPORT NOT CREATED: insufficient joined core outcomes "
            "or missing source histories",
            flush=True,
        )

    archived = archive_window(index, start, end)
    print(
        f"SEGMENT_COMPLETE index={index}/{SEGMENTS} "
        f"start={start.isoformat()} end={end.isoformat()} "
        f"archive={archived}",
        flush=True,
    )


def main() -> None:
    if DAYS <= 0:
        raise ValueError("HIST_DAYS must be > 0")
    if SEGMENTS <= 0:
        raise ValueError("HIST_SEGMENTS must be > 0")

    first_end = parse_end(END_ISO)
    print(
        f"HIST_SERIES first_end={first_end.isoformat()} "
        f"days_per_segment={DAYS} segments={SEGMENTS}",
        flush=True,
    )

    current_end = first_end
    try:
        for index in range(1, SEGMENTS + 1):
            current_start = current_end - timedelta(days=DAYS)
            run_window(index, current_start, current_end)
            current_end = current_start
    except Exception as exc:
        print(
            f"HIST_SERIES_FAILED current_end={current_end.isoformat()} "
            f"error={type(exc).__name__}: {exc}",
            flush=True,
        )
        print(
            "HIST_SERVICE_HOLD reason=failed; keeping process alive to prevent "
            "Render from automatically restarting the same window.",
            flush=True,
        )
        while True:
            time.sleep(3600)

    print(
        f"HIST_SERIES_COMPLETE segments={SEGMENTS} "
        f"oldest_start={current_end.isoformat()} newest_end={first_end.isoformat()}",
        flush=True,
    )
    print(
        "HIST_SERVICE_HOLD reason=complete; keeping process alive to prevent "
        "Render from automatically restarting completed windows.",
        flush=True,
    )
    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
