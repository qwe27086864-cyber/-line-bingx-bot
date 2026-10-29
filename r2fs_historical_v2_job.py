"""
Isolated R2FS Historical V2 job.

Downloads public BingX perpetual 1m + 5m OHLCV into a dedicated directory and
runs r2fs_historical_adapter_v2.py. No API key, no orders, no DATABASE_URL.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import historical_data as hd

SYMBOLS = [
    x.strip().upper()
    for x in os.getenv(
        "R2FS_V2_SYMBOLS",
        "BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT",
    ).split(",")
    if x.strip()
]
DAYS = int(os.getenv("R2FS_V2_DAYS", "30"))
END_ISO = os.getenv(
    "R2FS_V2_END_ISO",
    "2026-10-28T09:30:00Z",
)
PAUSE = float(
    os.getenv("R2FS_V2_REQUEST_PAUSE_SEC", "2.25")
)
DATA = Path(
    os.getenv("R2FS_V2_DATA_DIR", "data/r2fs_v2")
)
DATA.mkdir(parents=True, exist_ok=True)


def parse_dt(value: str) -> datetime:
    s = value[:-1] + "+00:00" if value.endswith("Z") else value
    x = datetime.fromisoformat(s)
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def download_interval(interval: str, start: datetime, end: datetime):
    out = DATA / f"history_{interval}.csv"
    start_ms = hd.utc_ms(start)
    end_ms = hd.utc_ms(end)

    # Dedicated V2 dir should be clean. If Render restarts the same job,
    # de-duplicate rather than corrupting the file.
    existing = hd.load_existing_keys(out)

    for i, symbol in enumerate(SYMBOLS):
        rows = hd.download_symbol(
            symbol,
            interval,
            start_ms,
            end_ms,
            PAUSE,
        )
        fresh = [
            r for r in rows
            if (
                r["timestamp"],
                r["symbol"],
                r["interval"],
            ) not in existing
        ]
        hd.append_rows(out, fresh)
        existing.update(
            (
                r["timestamp"],
                r["symbol"],
                r["interval"],
            )
            for r in fresh
        )
        print(
            f"R2FS_V2_DATA symbol={symbol} "
            f"interval={interval} rows={len(rows)} "
            f"new={len(fresh)}",
            flush=True,
        )
        if i + 1 < len(SYMBOLS):
            time.sleep(max(0.0, PAUSE))


def main():
    if DAYS <= 0:
        raise ValueError("R2FS_V2_DAYS must be > 0")

    end = parse_dt(END_ISO)
    start = end - timedelta(days=DAYS)

    print(
        f"R2FS_V2_WINDOW start={start.isoformat()} "
        f"end={end.isoformat()} days={DAYS}",
        flush=True,
    )

    for interval in ("1m", "5m"):
        download_interval(interval, start, end)

    cmd = [
        sys.executable,
        "r2fs_historical_adapter_v2.py",
        "--candles-1m",
        str(DATA / "history_1m.csv"),
        "--candles-5m",
        str(DATA / "history_5m.csv"),
        "--detail-out",
        str(DATA / "r2fs_v2_history_detail.csv"),
        "--outcomes-out",
        str(DATA / "r2fs_v2_core_outcomes.csv"),
    ]

    print("R2FS_V2_RUN", " ".join(cmd), flush=True)
    subprocess.run(cmd, check=True)
    print("R2FS_V2_STAGE1_DONE", flush=True)


if __name__ == "__main__":
    main()
