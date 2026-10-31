"""
R3 Historical Fast Job

Downloads an isolated 30-day historical window and runs the optimized
R3 historical adapter. Historical-only; no database export and no live writes.
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

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

MARK_PRICE_PATH = "/openApi/swap/v1/market/markPriceKlines"


def parse_end(value: str) -> datetime:
    s = value
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _mark_row_ts(row) -> int:
    if isinstance(row, (list, tuple)):
        return int(float(row[0]))
    if isinstance(row, dict):
        return int(float(
            row.get("time",
            row.get("openTime",
            row.get("open_time", 0)))
        ))
    return 0


def _fetch_mark_page(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1440,
):
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": int(start_ms),
        "endTime": int(end_ms),
        "limit": min(int(limit), 1440),
    }
    url = (
        f"{hd.API_BASE}{MARK_PRICE_PATH}?"
        f"{urlencode(params)}"
    )
    req = Request(
        url,
        headers={"User-Agent": "r3-historical-mark-fallback/1.0"},
    )

    last_error = None
    for attempt in range(6):
        try:
            with urlopen(req, timeout=30) as resp:
                payload = json.loads(
                    resp.read().decode("utf-8")
                )

            code = payload.get("code")
            if code in (0, "0", None):
                rows = payload.get("data") or []
                rows.sort(key=_mark_row_ts)
                return rows

            last_error = RuntimeError(
                f"BingX mark-price error: {payload}"
            )
            if str(code) == "100410":
                delay = min(
                    3.0 * (2 ** attempt),
                    48.0,
                )
                print(
                    "BingX mark-price temporary 100410; "
                    f"retry {attempt + 1}/6 "
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
            delay = min(
                3.0 * (2 ** attempt),
                48.0,
            )
            print(
                "BingX mark-price request failed "
                f"({type(exc).__name__}: {exc}); "
                f"retry {attempt + 1}/6 "
                f"after {delay:.1f}s",
                flush=True,
            )
            time.sleep(delay)

    raise RuntimeError(
        "BingX mark-price request failed after retries: "
        f"{last_error}"
    )


def _normalize_mark_row(
    symbol: str,
    interval: str,
    row,
):
    if isinstance(row, (list, tuple)):
        if len(row) < 6:
            raise ValueError(
                f"unexpected mark-price kline row: {row}"
            )
        ts = _mark_row_ts(row)
        open_v = float(row[1])
        high_v = float(row[2])
        low_v = float(row[3])
        close_v = float(row[4])
        volume_v = float(row[5] or 0.0)
    elif isinstance(row, dict):
        ts = _mark_row_ts(row)
        open_v = float(row.get("open") or 0.0)
        high_v = float(row.get("high") or 0.0)
        low_v = float(row.get("low") or 0.0)
        close_v = float(row.get("close") or 0.0)
        volume_v = float(row.get("volume") or 0.0)
    else:
        raise TypeError(
            "unsupported mark-price kline row type: "
            f"{type(row).__name__}"
        )

    return {
        "timestamp": hd.iso_utc(ts),
        "symbol": symbol,
        "interval": interval,
        "open": open_v,
        "high": high_v,
        "low": low_v,
        "close": close_v,
        "volume": volume_v,
        "quote_volume": volume_v * close_v,
        "trades": 0,
        "taker_buy_base": 0.0,
        "taker_buy_quote": 0.0,
    }


def download_mark_price_symbol(
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    request_pause_sec: float,
):
    step = hd.INTERVAL_MS[interval]
    cursor_end = end_ms
    result = []
    seen = set()

    while cursor_end >= start_ms:
        page = _fetch_mark_page(
            symbol,
            interval,
            start_ms,
            cursor_end,
        )
        if not page:
            break

        page_ts = [
            _mark_row_ts(raw)
            for raw in page
            if _mark_row_ts(raw) > 0
        ]
        if not page_ts:
            break

        for raw in page:
            ts = _mark_row_ts(raw)
            if (
                ts < start_ms
                or ts > end_ms
                or ts in seen
            ):
                continue
            seen.add(ts)
            result.append(
                _normalize_mark_row(
                    symbol,
                    interval,
                    raw,
                )
            )

        earliest_ts = min(page_ts)
        if earliest_ts <= start_ms:
            break

        next_end = earliest_ts - step
        if next_end >= cursor_end:
            break
        cursor_end = next_end
        time.sleep(max(0.0, request_pause_sec))

    result.sort(key=lambda r: r["timestamp"])
    return result


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

        expected_rows = int(
            (end_ms - start_ms)
            // hd.INTERVAL_MS[interval]
        )
        minimum_rows = int(expected_rows * 0.95)

        if len(rows) < minimum_rows:
            original_count = len(rows)
            fallback = download_mark_price_symbol(
                symbol,
                interval,
                start_ms,
                end_ms,
                REQUEST_PAUSE,
            )
            if len(fallback) > len(rows):
                rows = fallback
            print(
                "R3_FAST_FALLBACK "
                "source=bingx_mark_price_klines "
                f"symbol={symbol} interval={interval} "
                f"v3_rows={original_count} "
                f"fallback_rows={len(fallback)} "
                f"selected_rows={len(rows)} "
                f"minimum={minimum_rows}",
                flush=True,
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

    regime_path = DATA / "market_features.csv"
    subprocess.run([
        sys.executable, "market_regime.py",
        "--candles", str(DATA / "history_5m.csv"),
        "--out", str(regime_path),
    ], check=True)
    print("R3_FAST_MARKET_REGIME_DONE", flush=True)

    r2_path = DATA / "r2fs_core_outcomes.csv"
    subprocess.run([
        sys.executable, "r2fs_historical_adapter.py",
        "--candles-1m", str(DATA / "history_1m.csv"),
        "--candles-5m", str(DATA / "history_5m.csv"),
        "--outcomes-out", str(r2_path),
    ], check=True)
    print("R3_FAST_R2FS_BASELINE_DONE", flush=True)

    fields = ["timestamp", "symbol", "core", "net_return_pct"]
    merged = []
    for source in (r2_path, DATA / "r3_core_outcomes.csv"):
        with source.open(newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                merged.append({key: row.get(key, "") for key in fields})

    merged_path = DATA / "all_core_outcomes.csv"
    with merged_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(merged)
    print(
        f"R3_FAST_MERGE merged_rows={len(merged)} "
        "historical_only=true",
        flush=True,
    )

    report_path = DATA / "router_v1_report.json"
    subprocess.run([
        sys.executable, "router_trainer.py",
        "--features", str(regime_path),
        "--outcomes", str(merged_path),
        "--out", str(report_path),
    ], check=True)
    if not report_path.exists() or report_path.stat().st_size == 0:
        raise RuntimeError("R3 fast router report missing")
    print("R3_FAST_ROUTER_REPORT_DONE", flush=True)
    print("R3_FAST_PIPELINE_DONE", flush=True)


if __name__ == "__main__":
    main()
