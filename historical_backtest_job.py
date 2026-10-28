from __future__ import annotations

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
