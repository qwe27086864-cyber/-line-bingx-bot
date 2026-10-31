"""
Isolated R2FS Historical V2 job - corrected deep-history data path.

Historical-only. No API key, no orders, no DATABASE_URL.

Data policy:
1) Try BingX perpetual futures candles first.
2) Try BingX mark-price candles when needed.
3) For old 1m windows that BingX futures endpoints no longer retain deeply,
   use the already-validated BingX historical spot 1m fallback from
   r3_historical_fast_job.py.
4) Rebuild 5m candles from complete 1m data and require >=95% 5m coverage
   for BTC/ETH/SOL/XRP/DOGE before running the R2FS V2 replay.

This reuses the same historical recovery path that successfully completed
R3 A_GUARD's oldest 30-day windows.
"""
from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import r3_historical_fast_job as recovery

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


def configure_recovery():
    """
    Point the validated R3 historical downloader at this isolated R2FS V2
    segment directory. This mutates only in-process historical module globals.
    """
    recovery.DATA = DATA
    recovery.DAYS = DAYS
    recovery.REQUEST_PAUSE = PAUSE
    recovery.SYMBOLS = ",".join(SYMBOLS)


def main():
    if DAYS <= 0:
        raise ValueError("R2FS_V2_DAYS must be > 0")

    # Safety: historical-only. Never inherit a live DB into this process.
    os.environ.pop("DATABASE_URL", None)

    end = parse_dt(END_ISO)
    start = end - timedelta(days=DAYS)

    print(
        f"R2FS_V2_WINDOW start={start.isoformat()} "
        f"end={end.isoformat()} days={DAYS}",
        flush=True,
    )
    print(
        "R2FS_V2_DATA_POLICY "
        "perpetual_first=true mark_price_fallback=true "
        "historical_spot_1m_fallback=true rebuild_5m_from_1m=true "
        "minimum_5m_coverage_pct=95",
        flush=True,
    )

    configure_recovery()

    # Download 1m first. For old windows the validated recovery module
    # automatically falls back to BingX Historical Spot Kline when perpetual
    # and mark-price retention are insufficient.
    recovery.download_interval("1m", start, end)

    # Try native/mark-price 5m as well. Older windows may return no rows.
    recovery.download_interval("5m", start, end)

    # Always fill missing 5m buckets from complete 1m and enforce >=95%
    # coverage for all five symbols before allowing the strategy replay.
    recovery.recover_interval_from_1m("5m", start, end)

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
