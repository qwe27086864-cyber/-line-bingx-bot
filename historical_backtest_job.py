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
END_ISO = os.environ.get("HIST_END_ISO", "").strip()
REQUEST_PAUSE = float(os.environ.get("HIST_REQUEST_PAUSE_SEC", "2.25"))
DATA = Path(os.environ.get("HIST_DATA_DIR", "data"))
DATA.mkdir(parents=True, exist_ok=True)


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


def recover_interval_from_1m(interval: str, start: datetime, end: datetime) -> None:
    if interval not in {"5m", "15m"}:
        raise ValueError(f"Unsupported recovery interval: {interval}")

    minutes = int(interval[:-1])
    bucket_ms = minutes * 60_000
    source = DATA / "history_1m.csv"
    target = DATA / f"history_{interval}.csv"

    if not source.exists():
        raise RuntimeError(
            f"Cannot recover {interval} candles: 1m CSV is missing"
        )

    existing = hd.load_existing_keys(target)
    recovered = []
    group_key = None
    group = []
    start_ms, end_ms = hd.utc_ms(start), hd.utc_ms(end)

    def flush() -> None:
        if not group:
            return

        symbol, bucket = group_key
        times = [
            hd.utc_ms(datetime.fromisoformat(r["timestamp"]))
            for r in group
        ]
        expected_times = [bucket + n * 60_000 for n in range(minutes)]
        if len(times) != minutes or times != expected_times:
            return

        ts = hd.iso_utc(bucket)
        key = (ts, symbol, interval)
        if key in existing:
            return

        first, last = group[0], group[-1]
        recovered.append({
            "timestamp": ts,
            "symbol": symbol,
            "interval": interval,
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

            key = (row["symbol"], (ts_ms // bucket_ms) * bucket_ms)
            if key != group_key:
                flush()
                group_key, group = key, []
            group.append(row)

        flush()

    added = hd.append_rows(target, recovered)
    print(
        f"{interval} recovery: aggregated {added} complete bars from 1m",
        flush=True,
    )

    counts = {
        s: 0
        for s in (x.strip().upper() for x in SYMBOLS.split(","))
        if s
    }
    with target.open(newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if row["symbol"] in counts:
                counts[row["symbol"]] += 1

    expected = int((end - start).total_seconds() // (minutes * 60))
    minimum = int(expected * 0.95)

    print(
        f"{interval} coverage: expected={expected} minimum={minimum} "
        f"counts={counts}",
        flush=True,
    )

    if any(n < minimum for n in counts.values()):
        raise RuntimeError(
            f"Insufficient {interval} history after 1m recovery: {counts}; "
            f"need >= {minimum} per symbol. Stop rather than backtest "
            "incomplete R3 data."
        )


if DAYS <= 0:
    raise ValueError("HIST_DAYS must be > 0")

end = parse_end(END_ISO)
start = end - timedelta(days=DAYS)

print(
    f"HIST_WINDOW start={start.isoformat()} end={end.isoformat()} "
    f"days={DAYS}",
    flush=True,
)

for interval in ("1m", "5m", "15m", "1h"):
    download_interval(interval, start, end)

recover_interval_from_1m("5m", start, end)
recover_interval_from_1m("15m", start, end)

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
