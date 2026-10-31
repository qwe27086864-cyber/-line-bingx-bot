"""
BF1 Historical V4 static-archive proxy runner.

Keeps the existing filename so the current Render historical service can run it
without changing its start command.

Historical-only. No DATABASE_URL, no orders, no live/shadow writes.

360-day research proxy:
- Binance Data Vision spot 15m klines
- Binance Data Vision USD-M perpetual 15m klines
- Binance Data Vision USD-M funding-rate history

This is NOT BingX execution performance. The existing BingX ~98-day exact
result remains the venue-fidelity check.
"""
from __future__ import annotations

import bisect
import csv
import io
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

os.environ.pop("DATABASE_URL", None)

OUT = Path(os.getenv("BF4_OUT_DIR", "data/bf1_v4_proxy"))
OUT.mkdir(parents=True, exist_ok=True)

SYMS = [
    x.strip().upper()
    for x in os.getenv("BF4_HIST_SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT").split(",")
    if x.strip()
]
INTERVAL = os.getenv("BF4_HIST_INTERVAL", "15m")
DAYS = int(os.getenv("BF4_HIST_DAYS", "360"))
END = datetime.fromisoformat(
    os.getenv("BF4_HIST_END_ISO", "2026-10-28T09:30:00Z").replace("Z", "+00:00")
).astimezone(timezone.utc)
START = END - timedelta(days=DAYS)
STEP_MS = 15 * 60 * 1000
START_MS = int(START.timestamp() * 1000)
END_MS = int(END.timestamp() * 1000)
EXPECTED = int((END_MS - START_MS) // STEP_MS)
MIN_COVERAGE = float(os.getenv("BF4_MIN_COVERAGE", "0.95"))
MIN_ROWS = int(EXPECTED * MIN_COVERAGE)
SPOT_SPREAD_BPS = float(os.getenv("BF4_PROXY_SPOT_SPREAD_BPS", "2.0"))
PERP_SPREAD_BPS = float(os.getenv("BF4_PROXY_PERP_SPREAD_BPS", "2.0"))
PAUSE = float(os.getenv("BF4_ARCHIVE_PAUSE_SEC", "0.03"))
BASE = "https://data.binance.vision/data"


def venue_symbol(symbol: str) -> str:
    return symbol.replace("-", "")


def norm_ts(value) -> int:
    x = int(float(value))
    # Binance spot archives use microseconds from 2025 onward; futures data is
    # generally milliseconds. Normalize both to milliseconds.
    if x > 100_000_000_000_000:
        x //= 1000
    return x


def fetch_zip_csv(url: str, retries: int = 6):
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(
                url,
                headers={"User-Agent": "bf1-static-archive/1.0"},
            )
            with urllib.request.urlopen(req, timeout=60) as resp:
                raw = resp.read()
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                names = [n for n in zf.namelist() if not n.endswith("/")]
                if not names:
                    raise RuntimeError(f"empty zip: {url}")
                with zf.open(names[0]) as f:
                    text = io.TextIOWrapper(f, encoding="utf-8-sig", newline="")
                    return list(csv.reader(text))
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code == 404:
                return None
            if exc.code not in (408, 418, 425, 429, 500, 502, 503, 504):
                raise
        except Exception as exc:
            last = exc
        if attempt + 1 < retries:
            delay = min(2 ** attempt, 20)
            print(
                f"BF4_ARCHIVE_RETRY attempt={attempt+1}/{retries} delay={delay}s "
                f"url={url} error={last}",
                flush=True,
            )
            time.sleep(delay)
    raise RuntimeError(f"archive download failed: {url}: {last}")


def month_iter(start: datetime, end: datetime):
    cur = date(start.year, start.month, 1)
    last = date(end.year, end.month, 1)
    while cur <= last:
        yield cur.year, cur.month
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)


def month_start_ms(year: int, month: int) -> int:
    return int(datetime(year, month, 1, tzinfo=timezone.utc).timestamp() * 1000)


def next_month_ms(year: int, month: int) -> int:
    if month == 12:
        dt = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        dt = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def daily_dates(year: int, month: int):
    cur = date(year, month, 1)
    if month == 12:
        nxt = date(year + 1, 1, 1)
    else:
        nxt = date(year, month + 1, 1)
    while cur < nxt:
        yield cur
        cur += timedelta(days=1)


def kline_urls(kind: str, symbol: str, year: int, month: int):
    s = venue_symbol(symbol)
    if kind == "spot":
        monthly = (
            f"{BASE}/spot/monthly/klines/{s}/{INTERVAL}/"
            f"{s}-{INTERVAL}-{year:04d}-{month:02d}.zip"
        )
        daily_tpl = (
            f"{BASE}/spot/daily/klines/{s}/{INTERVAL}/"
            f"{s}-{INTERVAL}-{{day}}.zip"
        )
    else:
        monthly = (
            f"{BASE}/futures/um/monthly/klines/{s}/{INTERVAL}/"
            f"{s}-{INTERVAL}-{year:04d}-{month:02d}.zip"
        )
        daily_tpl = (
            f"{BASE}/futures/um/daily/klines/{s}/{INTERVAL}/"
            f"{s}-{INTERVAL}-{{day}}.zip"
        )
    return monthly, daily_tpl


def funding_urls(symbol: str, year: int, month: int):
    s = venue_symbol(symbol)
    monthly = (
        f"{BASE}/futures/um/monthly/fundingRate/{s}/"
        f"{s}-fundingRate-{year:04d}-{month:02d}.zip"
    )
    daily_tpl = (
        f"{BASE}/futures/um/daily/fundingRate/{s}/"
        f"{s}-fundingRate-{{day}}.zip"
    )
    return monthly, daily_tpl


def parse_kline_rows(rows, start_ms: int, end_ms: int, out: dict[int, float]):
    if not rows:
        return 0
    added = 0
    for row in rows:
        if len(row) < 5:
            continue
        try:
            t = norm_ts(row[0])
            close = float(row[4])
        except Exception:
            # header row, if present
            continue
        if start_ms <= t < end_ms and close > 0:
            if t not in out:
                added += 1
            out[t] = close
    return added


def parse_funding_rows(rows, start_ms: int, end_ms: int, out: dict[int, float]):
    if not rows:
        return 0
    added = 0
    header = None
    if rows:
        maybe = [str(x).strip() for x in rows[0]]
        if any(not x.replace(".", "", 1).replace("-", "", 1).isdigit() for x in maybe[:2]):
            header = {name: i for i, name in enumerate(maybe)}
            rows = rows[1:]

    for row in rows:
        try:
            if header:
                if "calc_time" in header:
                    t = norm_ts(row[header["calc_time"]])
                    rate = float(row[header.get("last_funding_rate", 2)])
                elif "fundingTime" in header:
                    t = norm_ts(row[header["fundingTime"]])
                    rate = float(row[header.get("fundingRate", 1)])
                else:
                    t = norm_ts(row[0])
                    rate = float(row[-1])
            else:
                # Known Binance Data Vision funding archive layout commonly uses
                # calc_time,funding_interval_hours,last_funding_rate.
                t = norm_ts(row[0])
                rate = float(row[2] if len(row) >= 3 else row[1])
        except Exception:
            continue
        if start_ms <= t < end_ms:
            if t not in out:
                added += 1
            out[t] = rate
    return added


def load_month_kline(kind: str, symbol: str, year: int, month: int, out: dict[int, float]):
    monthly, daily_tpl = kline_urls(kind, symbol, year, month)
    rows = fetch_zip_csv(monthly)
    if rows is not None:
        n = parse_kline_rows(rows, START_MS, END_MS, out)
        print(
            f"BF4_ARCHIVE source={kind} mode=monthly symbol={symbol} month={year:04d}-{month:02d} "
            f"added={n} total={len(out)}",
            flush=True,
        )
        return

    # Current month may not have a monthly archive yet. Fall back to daily files.
    print(
        f"BF4_ARCHIVE_MONTHLY_MISSING source={kind} symbol={symbol} month={year:04d}-{month:02d} "
        "fallback=daily",
        flush=True,
    )
    for d in daily_dates(year, month):
        ds = d.isoformat()
        day_start = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
        if day_start >= END_MS:
            break
        if day_start + 86400000 <= START_MS:
            continue
        rows = fetch_zip_csv(daily_tpl.format(day=ds))
        if rows is None:
            print(f"BF4_ARCHIVE_DAILY_MISSING source={kind} symbol={symbol} day={ds}", flush=True)
            continue
        n = parse_kline_rows(rows, START_MS, END_MS, out)
        print(
            f"BF4_ARCHIVE source={kind} mode=daily symbol={symbol} day={ds} added={n} total={len(out)}",
            flush=True,
        )
        time.sleep(max(0.0, PAUSE))


def load_month_funding(symbol: str, year: int, month: int, out: dict[int, float]):
    monthly, daily_tpl = funding_urls(symbol, year, month)
    rows = fetch_zip_csv(monthly)
    if rows is not None:
        n = parse_funding_rows(rows, START_MS, END_MS, out)
        print(
            f"BF4_ARCHIVE source=funding mode=monthly symbol={symbol} month={year:04d}-{month:02d} "
            f"added={n} total={len(out)}",
            flush=True,
        )
        return

    print(
        f"BF4_ARCHIVE_MONTHLY_MISSING source=funding symbol={symbol} month={year:04d}-{month:02d} "
        "fallback=daily",
        flush=True,
    )
    for d in daily_dates(year, month):
        ds = d.isoformat()
        day_start = int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp() * 1000)
        if day_start >= END_MS:
            break
        if day_start + 86400000 <= START_MS:
            continue
        rows = fetch_zip_csv(daily_tpl.format(day=ds))
        if rows is None:
            print(f"BF4_ARCHIVE_DAILY_MISSING source=funding symbol={symbol} day={ds}", flush=True)
            continue
        n = parse_funding_rows(rows, START_MS, END_MS, out)
        print(
            f"BF4_ARCHIVE source=funding mode=daily symbol={symbol} day={ds} added={n} total={len(out)}",
            flush=True,
        )
        time.sleep(max(0.0, PAUSE))


def load_klines(kind: str, symbol: str):
    out: dict[int, float] = {}
    for year, month in month_iter(START, END):
        load_month_kline(kind, symbol, year, month, out)
        time.sleep(max(0.0, PAUSE))
    return out


def load_funding(symbol: str):
    out: dict[int, float] = {}
    for year, month in month_iter(START, END):
        load_month_funding(symbol, year, month, out)
        time.sleep(max(0.0, PAUSE))
    return sorted(out.items())


def quote(mid: float, spread_bps: float):
    half = max(0.0, spread_bps) / 20000.0
    return mid * (1.0 - half), mid * (1.0 + half)


def funding_at(rows, times, t: int):
    i = bisect.bisect_left(times, t)
    if i < len(rows):
        return rows[i][1], rows[i][0]
    return 0.0, 0


def run_adapter(snapshots: Path, outcomes: Path):
    subprocess.run(
        [
            sys.executable,
            "bf1_historical_adapter.py",
            "--snapshots",
            str(snapshots),
            "--out",
            str(outcomes),
        ],
        check=True,
    )


def summarize(outcomes: Path, quality: dict):
    with outcomes.open(newline="", encoding="utf-8-sig") as f:
        vals = [float(r["net_return_pct"]) for r in csv.DictReader(f)]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl else (999.0 if gp else 0.0)
    return {
        "closed": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": round(pf, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "mode": "360_CROSS_EXCHANGE_PROXY_STATIC_ARCHIVES",
        "venue": "Binance Data Vision spot + USD-M perpetual + funding",
        "days": DAYS,
        "symbols": SYMS,
        "coverage_rule": f">={MIN_COVERAGE:.0%}",
        "warning": (
            "Research proxy only; not BingX execution performance. "
            "Use the existing BingX exact-overlap result for venue fidelity."
        ),
        "quality": quality,
    }


def main():
    print(
        f"BF4_STATIC_WINDOW start={START.isoformat()} end={END.isoformat()} days={DAYS} "
        f"interval={INTERVAL} expected={EXPECTED} minimum={MIN_ROWS} historical_only=true",
        flush=True,
    )

    fields = [
        "timestamp",
        "symbol",
        "spot_bid",
        "spot_ask",
        "perp_bid",
        "perp_ask",
        "funding_rate",
        "next_funding_time_ms",
    ]
    snapshots = []
    quality = {}

    for symbol in SYMS:
        print(f"BF4_STATIC_SYMBOL_START symbol={symbol}", flush=True)
        spot = load_klines("spot", symbol)
        perp = load_klines("perp", symbol)
        funding = load_funding(symbol)
        ftimes = [x[0] for x in funding]
        times = sorted(set(spot) & set(perp))

        quality[symbol] = {
            "expected_rows": EXPECTED,
            "minimum_rows": MIN_ROWS,
            "spot_rows": len(spot),
            "perp_rows": len(perp),
            "aligned_rows": len(times),
            "coverage_pct": round(100 * len(times) / EXPECTED, 2) if EXPECTED else 0.0,
            "funding_events": len(funding),
        }
        print(
            "BF4_STATIC_QUALITY "
            + json.dumps({"symbol": symbol, **quality[symbol]}, separators=(",", ":")),
            flush=True,
        )

        for t in times:
            sb, sa = quote(spot[t], SPOT_SPREAD_BPS)
            pb, pa = quote(perp[t], PERP_SPREAD_BPS)
            rate, nft = funding_at(funding, ftimes, t)
            snapshots.append(
                {
                    "timestamp": datetime.fromtimestamp(t / 1000, tz=timezone.utc).isoformat(),
                    "symbol": symbol,
                    "spot_bid": sb,
                    "spot_ask": sa,
                    "perp_bid": pb,
                    "perp_ask": pa,
                    "funding_rate": rate,
                    "next_funding_time_ms": nft,
                }
            )

    (OUT / "bf1_v4_static_quality.json").write_text(json.dumps(quality, indent=2), encoding="utf-8")

    if any(q["aligned_rows"] < MIN_ROWS for q in quality.values()):
        print("BF4_STATIC_INVALID " + json.dumps(quality, separators=(",", ":")), flush=True)
        raise RuntimeError("BF4 static archive proxy coverage below 95%")

    snap_path = OUT / "bf1_v4_static_snapshots.csv"
    snapshots.sort(key=lambda r: (r["timestamp"], r["symbol"]))
    with snap_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(snapshots)

    out_path = OUT / "bf1_v4_static_outcomes.csv"
    run_adapter(snap_path, out_path)
    summary = summarize(out_path, quality)
    (OUT / "bf1_v4_static_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print("BF4_STATIC_RESULT " + json.dumps(summary, separators=(",", ":")), flush=True)
    print("BF4_STATIC_DONE", flush=True)


if __name__ == "__main__":
    main()
