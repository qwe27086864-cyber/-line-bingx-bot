"""
Historical market-data downloader for the router research project.

- Public BingX perpetual kline endpoint only.
- No API keys, no orders, no DATABASE_URL writes.
- Stores raw history outside Postgres as CSV.
- Forward pagination with de-duplication.

Example:
  python historical_data.py --symbols BTC-USDT,ETH-USDT,SOL-USDT \
      --interval 5m --days 90 --out data/history_5m.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

API_BASE = "https://open-api.bingx.com"
KLINE_PATH = "/openApi/swap/v3/quote/klines"

INTERVAL_MS = {
    "1m": 60_000,
    "3m": 3 * 60_000,
    "5m": 5 * 60_000,
    "15m": 15 * 60_000,
    "30m": 30 * 60_000,
    "1h": 60 * 60_000,
    "2h": 2 * 60 * 60_000,
    "4h": 4 * 60 * 60_000,
    "6h": 6 * 60 * 60_000,
    "8h": 8 * 60 * 60_000,
    "12h": 12 * 60 * 60_000,
    "1d": 24 * 60 * 60_000,
}

FIELDS = [
    "timestamp", "symbol", "interval", "open", "high", "low", "close",
    "volume", "quote_volume", "trades", "taker_buy_base", "taker_buy_quote",
]


def utc_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        raise ValueError("datetime must be timezone-aware")
    return int(dt.timestamp() * 1000)


def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def row_ts(row) -> int:
    if isinstance(row, (list, tuple)):
        return int(row[0])
    if isinstance(row, dict):
        return int(row.get("time", row.get("openTime", row.get("open_time", 0))))
    raise TypeError(f"unsupported kline row type: {type(row).__name__}")


def fetch_page(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1440,
) -> list:
    if interval not in INTERVAL_MS:
        raise ValueError(f"unsupported interval: {interval}")
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": int(start_ms),
        "endTime": int(end_ms),
        "limit": min(int(limit), 1440),
    }
    url = f"{API_BASE}{KLINE_PATH}?{urlencode(params)}"
    req = Request(url, headers={"User-Agent": "historical-router-v1/1.0"})

    last_error = None
    for attempt in range(6):
        try:
            with urlopen(req, timeout=30) as resp:
                payload = json.loads(resp.read().decode("utf-8"))

            code = payload.get("code")
            if code in (0, "0", None):
                rows = payload.get("data") or []
                rows.sort(key=row_ts)
                return rows

            last_error = RuntimeError(f"BingX error: {payload}")

            # 100410 = temporary "please retry later" response observed in
            # production. Back off instead of aborting the whole history run.
            if str(code) == "100410":
                delay = min(3.0 * (2 ** attempt), 48.0)
                print(
                    f"BingX temporary 100410; retry {attempt + 1}/6 "
                    f"after {delay:.1f}s",
                    flush=True,
                )
                time.sleep(delay)
                continue

            raise last_error

        except Exception as exc:
            last_error = exc
            if attempt >= 5:
                break
            delay = min(3.0 * (2 ** attempt), 48.0)
            print(
                f"BingX request failed ({type(exc).__name__}: {exc}); "
                f"retry {attempt + 1}/6 after {delay:.1f}s",
                flush=True,
            )
            time.sleep(delay)

    raise RuntimeError(f"BingX request failed after retries: {last_error}")


def normalize_row(symbol: str, interval: str, row) -> dict:
    if isinstance(row, (list, tuple)):
        if len(row) < 6:
            raise ValueError(f"unexpected kline row: {row}")
        return {
            "timestamp": iso_utc(row_ts(row)),
            "symbol": symbol,
            "interval": interval,
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "volume": float(row[5]),
            "quote_volume": float(row[7]) if len(row) > 7 and row[7] not in (None, "") else 0.0,
            "trades": int(float(row[8])) if len(row) > 8 and row[8] not in (None, "") else 0,
            "taker_buy_base": float(row[9]) if len(row) > 9 and row[9] not in (None, "") else 0.0,
            "taker_buy_quote": float(row[10]) if len(row) > 10 and row[10] not in (None, "") else 0.0,
        }

    if isinstance(row, dict):
        def first(*keys, default=0):
            for key in keys:
                value = row.get(key)
                if value not in (None, ""):
                    return value
            return default

        return {
            "timestamp": iso_utc(row_ts(row)),
            "symbol": symbol,
            "interval": interval,
            "open": float(first("open")),
            "high": float(first("high")),
            "low": float(first("low")),
            "close": float(first("close")),
            "volume": float(first("volume")),
            "quote_volume": float(first("quoteVolume", "quote_volume", "quoteAssetVolume")),
            "trades": int(float(first("trades", "tradeCount", "count"))),
            "taker_buy_base": float(first("takerBuyBaseVolume", "taker_buy_base", "takerBuyBaseAssetVolume")),
            "taker_buy_quote": float(first("takerBuyQuoteVolume", "taker_buy_quote", "takerBuyQuoteAssetVolume")),
        }

    raise TypeError(f"unsupported kline row type: {type(row).__name__}")


def load_existing_keys(path: Path) -> set[tuple[str, str, str]]:
    if not path.exists():
        return set()
    keys = set()
    with path.open(newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            keys.add((r["timestamp"], r["symbol"], r["interval"]))
    return keys


def append_rows(path: Path, rows: Iterable[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists()
    count = 0
    with path.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if not exists:
            w.writeheader()
        for row in rows:
            w.writerow(row)
            count += 1
    return count


def download_symbol(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    request_pause_sec: float,
) -> list[dict]:
    """Download a complete historical range by paginating BACKWARD.

    BingX v3 klines returns the most recent rows inside the requested window.
    Forward pagination therefore stops after the first 1,440 candles. We move
    `endTime` backward to one interval before the earliest candle received.
    """
    step = INTERVAL_MS[interval]
    cursor_end = end_ms
    result: list[dict] = []
    seen = set()

    while cursor_end >= start_ms:
        page = fetch_page(symbol, interval, start_ms, cursor_end)
        if not page:
            break

        page_ts = [row_ts(raw) for raw in page if row_ts(raw) > 0]
        if not page_ts:
            break

        for raw in page:
            ts = row_ts(raw)
            if ts < start_ms or ts > end_ms or ts in seen:
                continue
            seen.add(ts)
            result.append(normalize_row(symbol, interval, raw))

        earliest_ts = min(page_ts)
        if earliest_ts <= start_ms:
            break

        next_end = earliest_ts - step
        if next_end >= cursor_end:
            break
        cursor_end = next_end

        if len(page) < 1440:
            break
        time.sleep(max(0.0, request_pause_sec))

    result.sort(key=lambda r: r["timestamp"])
    return result


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--symbols", required=True, help="Comma-separated, e.g. BTC-USDT,ETH-USDT")
    p.add_argument("--interval", default="5m", choices=sorted(INTERVAL_MS))
    p.add_argument("--days", type=int, default=90)
    p.add_argument("--out", default="data/history_5m.csv")
    p.add_argument("--request-pause-sec", type=float, default=2.25)
    args = p.parse_args()

    if args.days <= 0:
        raise ValueError("--days must be > 0")

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.days)
    start_ms, end_ms = utc_ms(start), utc_ms(end)

    out = Path(args.out)
    existing = load_existing_keys(out)
    total_new = 0

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    for i, symbol in enumerate(symbols):
        rows = download_symbol(symbol, args.interval, start_ms, end_ms, args.request_pause_sec)
        fresh = [
            r for r in rows
            if (r["timestamp"], r["symbol"], r["interval"]) not in existing
        ]
        total_new += append_rows(out, fresh)
        existing.update((r["timestamp"], r["symbol"], r["interval"]) for r in fresh)
        print(f"{symbol}: fetched={len(rows)} new={len(fresh)}", flush=True)
        if i + 1 < len(symbols):
            time.sleep(max(0.0, args.request_pause_sec))

    print(f"saved {total_new} new rows -> {out}", flush=True)


if __name__ == "__main__":
    main()
