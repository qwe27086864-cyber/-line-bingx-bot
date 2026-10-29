"""
R3 Historical Fast Job

Downloads an isolated 30-day historical window and runs the optimized
R3 historical adapter. Historical-only; no database export and no live writes.
"""
from __future__ import annotations

import csv
import os
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
END_ISO = os.environ.get(
    "HIST_END_ISO",
    "2026-10-28T09:30:00Z",
).strip()
REQUEST_PAUSE = float(
    os.environ.get("HIST_REQUEST_PAUSE_SEC", "2.25")
)
DATA = Path(
    os.environ.get("R3_FAST_DATA_DIR", "data/r3_fast")
)
DATA.mkdir(parents=True, exist_ok=True)


def parse_end(value: str) -> datetime:
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def download_interval(
    interval: str,
    start: datetime,
    end: datetime,
):
    out = DATA / f"history_{interval}.csv"
    existing = hd.load_existing_keys(out)

    start_ms = hd.utc_ms(start)
    end_ms = hd.utc_ms(end)
    symbols = [
        s.strip().upper()
        for s in SYMBOLS.split(",")
        if s.strip()
    ]

    total_new = 0

    print(
        f"R3_FAST_WINDOW interval={interval} "
        f"start={start.isoformat()} "
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
            if (
                r["timestamp"],
                r["symbol"],
                r["interval"],
            ) not in existing
        ]
        total_new += hd.append_rows(out, fresh)
        existing.update(
            (
                r["timestamp"],
                r["symbol"],
                r["interval"],
            )
            for r in fresh
        )

        print(
            f"R3_FAST_DATA {symbol} "
            f"interval={interval} "
            f"fetched={len(rows)} new={len(fresh)}",
            flush=True,
        )

        if i + 1 < len(symbols):
            time.sleep(max(0.0, REQUEST_PAUSE))

    print(
        f"R3_FAST_SAVED interval={interval} "
        f"new={total_new}",
        flush=True,
    )


def recover_interval_from_1m(
    interval: str,
    start: datetime,
    end: datetime,
):
    if interval not in {"5m", "15m"}:
        raise ValueError(interval)

    minutes = int(interval[:-1])
    bucket_ms = minutes * 60_000

    source = DATA / "history_1m.csv"
    target = DATA / f"history_{interval}.csv"

    existing = hd.load_existing_keys(target)
    recovered = []
    group_key = None
    group = []

    start_ms = hd.utc_ms(start)
    end_ms = hd.utc_ms(end)

    def flush():
        nonlocal group
        if not group:
            return

        symbol, bucket = group_key
        times = [
            hd.utc_ms(
                datetime.fromisoformat(r["timestamp"])
            )
            for r in group
        ]
        expected_times = [
            bucket + n * 60_000
            for n in range(minutes)
        ]

        if len(times) != minutes or times != expected_times:
            return

        ts = hd.iso_utc(bucket)
        key = (ts, symbol, interval)
        if key in existing:
            return

        first = group[0]
        last = group[-1]

        recovered.append({
            "timestamp": ts,
            "symbol": symbol,
            "interval": interval,
            "open": float(first["open"]),
            "high": max(float(r["high"]) for r in group),
            "low": min(float(r["low"]) for r in group),
            "close": float(last["close"]),
            "volume": sum(float(r["volume"]) for r in group),
            "quote_volume": sum(
                float(r["quote_volume"]) for r in group
            ),
            "trades": sum(
                int(float(r["trades"])) for r in group
            ),
            "taker_buy_base": sum(
                float(r["taker_buy_base"]) for r in group
            ),
            "taker_buy_quote": sum(
                float(r["taker_buy_quote"]) for r in group
            ),
        })
        existing.add(key)

    with source.open(
        newline="",
        encoding="utf-8-sig",
    ) as f:
        for row in csv.DictReader(f):
            ts_ms = hd.utc_ms(
                datetime.fromisoformat(row["timestamp"])
            )
            if ts_ms < start_ms or ts_ms >= end_ms:
                continue

            key = (
                row["symbol"],
                (ts_ms // bucket_ms) * bucket_ms,
            )

            if key != group_key:
                flush()
                group_key = key
                group = []

            group.append(row)

        flush()

    hd.append_rows(target, recovered)

    symbols = [
        s.strip().upper()
        for s in SYMBOLS.split(",")
        if s.strip()
    ]
    counts = {s: 0 for s in symbols}

    with target.open(
        newline="",
        encoding="utf-8-sig",
    ) as f:
        for row in csv.DictReader(f):
            if row["symbol"] in counts:
                counts[row["symbol"]] += 1

    expected = int(
        (end - start).total_seconds()
        // (minutes * 60)
    )
    minimum = int(expected * 0.95)

    print(
        f"R3_FAST_COVERAGE interval={interval} "
        f"expected={expected} minimum={minimum} "
        f"counts={counts}",
        flush=True,
    )

    if any(n < minimum for n in counts.values()):
        raise RuntimeError(
            f"Insufficient {interval} history: {counts}; "
            f"need >= {minimum} per symbol"
        )


def main():
    if DAYS <= 0:
        raise ValueError("HIST_DAYS must be > 0")

    end = parse_end(END_ISO)
    start = end - timedelta(days=DAYS)

    print(
        f"R3_FAST_HIST_WINDOW "
        f"start={start.isoformat()} "
        f"end={end.isoformat()} days={DAYS}",
        flush=True,
    )

    for interval in ("1m", "5m", "15m", "1h"):
        download_interval(interval, start, end)

    recover_interval_from_1m("5m", start, end)
    recover_interval_from_1m("15m", start, end)

    cmd = [
        sys.executable,
        "r3_historical_adapter_fast.py",
        "--candles-1m",
        str(DATA / "history_1m.csv"),
        "--candles-5m",
        str(DATA / "history_5m.csv"),
        "--candles-15m",
        str(DATA / "history_15m.csv"),
        "--candles-1h",
        str(DATA / "history_1h.csv"),
        "--detail-out",
        str(DATA / "r3_history_detail.csv"),
        "--outcomes-out",
        str(DATA / "r3_core_outcomes.csv"),
    ]

    print("R3_FAST_RUN", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)


if __name__ == "__main__":
    main()
