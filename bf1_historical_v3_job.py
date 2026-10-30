"""
BF1 Historical V3

Historical-only research job. No DATABASE_URL, no orders, no live/forward/shadow writes.

Goals
-----
1) Produce an "exact overlap" BF1 backtest using BingX spot history + actual
   perpetual trade-price klines for whatever common history BingX exposes.
2) Produce a 360-day BF1 proxy backtest:
   - actual perpetual trade-price klines are preferred wherever available;
   - older missing perpetual candles are filled with BingX mark-price klines;
   - every result is explicitly labelled as a proxy and reports how many
     candles came from actual perp vs mark-price fallback.

This does NOT pretend mark price is executable perpetual trade price. It is a
historical proxy to test whether BF1's basis/funding logic has any plausible
edge before higher-fidelity forward validation.

Expected existing file:
  bf1_historical_adapter.py
"""

from __future__ import annotations

import bisect
import csv
import json
import os
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import historical_data as hd


BASES = (
    "https://open-api.bingx.com",
    "https://open-api.bingx.pro",
)

SPOT_HIST = "/openApi/market/his/v1/kline"
MARK_KLINE = "/openApi/swap/v1/market/markPriceKlines"
FUNDING = "/openApi/swap/v2/quote/fundingRate"

DAYS = int(os.getenv("BF3_HIST_DAYS", "360"))
END_ISO = os.getenv(
    "BF3_HIST_END_ISO",
    "2026-10-28T09:30:00Z",
)
SYMBOLS = [
    x.strip().upper()
    for x in os.getenv(
        "BF3_HIST_SYMBOLS",
        "BTC-USDT,ETH-USDT,SOL-USDT",
    ).split(",")
    if x.strip()
]
INTERVAL = os.getenv("BF3_HIST_INTERVAL", "15m")
SPOT_SPREAD_BPS = float(
    os.getenv("BF3_SPOT_SPREAD_BPS", "2.0")
)
PERP_SPREAD_BPS = float(
    os.getenv("BF3_PERP_SPREAD_BPS", "2.0")
)
PAUSE = float(
    os.getenv("BF3_REQUEST_PAUSE_SEC", "1.05")
)
MIN_COVERAGE = float(
    os.getenv("BF3_MIN_COVERAGE", "0.95")
)

OUT = Path(os.getenv("BF3_OUT_DIR", "data/bf1_v3"))
OUT.mkdir(parents=True, exist_ok=True)

STEP_MS = hd.INTERVAL_MS[INTERVAL]
FIELDS = [
    "timestamp",
    "symbol",
    "spot_bid",
    "spot_ask",
    "perp_bid",
    "perp_ask",
    "funding_rate",
    "next_funding_time_ms",
]


def parse_dt(value: str) -> datetime:
    s = value[:-1] + "+00:00" if value.endswith("Z") else value
    x = datetime.fromisoformat(s)
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def request_json(path: str, params: dict, tries: int = 7):
    query = urllib.parse.urlencode(params)
    last = None

    for attempt in range(tries):
        for base in BASES:
            try:
                req = urllib.request.Request(
                    base + path + "?" + query,
                    headers={
                        "User-Agent": "bf1-historical-v3/1.0",
                        "X-SOURCE-KEY": "BX-AI-SKILL",
                    },
                )
                with urllib.request.urlopen(req, timeout=30) as resp:
                    payload = json.loads(
                        resp.read().decode("utf-8")
                    )

                code = payload.get("code")
                if code in (0, "0", None):
                    return payload.get("data") or []

                last = RuntimeError(str(payload))
                if str(code) not in ("100410", "100500"):
                    raise last

            except Exception as exc:
                last = exc
                if base != BASES[-1]:
                    continue

        delay = min(2.0 * (2 ** attempt), 30.0)
        print(
            f"BF3 retry={attempt + 1}/{tries} "
            f"delay={delay:.1f}s error={last}",
            flush=True,
        )
        time.sleep(delay)

    raise RuntimeError(f"BingX request failed: {last}")


def row_ts(row) -> int:
    if isinstance(row, (list, tuple)):
        return int(float(row[0]))
    if isinstance(row, dict):
        for key in (
            "time",
            "openTime",
            "timestamp",
            "fundingTime",
        ):
            value = row.get(key)
            if value not in (None, ""):
                return int(float(value))
    return 0


def row_close(row) -> float:
    if isinstance(row, (list, tuple)):
        return float(row[4])
    if isinstance(row, dict):
        for key in ("close", "closePrice", "c"):
            value = row.get(key)
            if value not in (None, ""):
                return float(value)
    return 0.0


def spot_history(
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> dict[int, float]:
    """
    BingX spot historical endpoint is queried in bounded 500-candle windows.
    """
    cursor_end = end_ms
    out: dict[int, float] = {}
    page = 0

    while cursor_end >= start_ms:
        window_start = max(
            start_ms,
            cursor_end - STEP_MS * 500,
        )
        rows = request_json(
            SPOT_HIST,
            {
                "symbol": symbol,
                "interval": INTERVAL,
                "startTime": window_start,
                "endTime": cursor_end,
                "limit": 500,
            },
        )
        if isinstance(rows, dict):
            rows = (
                rows.get("data")
                or rows.get("klines")
                or []
            )

        stamps = []
        for row in rows:
            t = row_ts(row)
            if not t:
                continue
            stamps.append(t)
            if start_ms <= t < end_ms:
                c = row_close(row)
                if c > 0:
                    out[t] = c

        page += 1
        print(
            f"BF3 {symbol} spot page={page} "
            f"rows={len(rows)} total={len(out)}",
            flush=True,
        )

        if window_start <= start_ms:
            break

        if stamps:
            next_end = min(stamps) - 1
        else:
            next_end = window_start - 1

        if next_end >= cursor_end:
            break

        cursor_end = next_end
        time.sleep(max(PAUSE, 1.01))

    return out


def mark_price_history(
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> dict[int, float]:
    """
    Backward-paginate BingX mark-price klines in bounded 1440-candle windows.
    """
    cursor_end = end_ms
    out: dict[int, float] = {}
    page = 0
    page_span = STEP_MS * 1440

    while cursor_end >= start_ms:
        window_start = max(
            start_ms,
            cursor_end - page_span,
        )

        rows = request_json(
            MARK_KLINE,
            {
                "symbol": symbol,
                "interval": INTERVAL,
                "startTime": window_start,
                "endTime": cursor_end,
                "limit": 1440,
            },
        )

        if isinstance(rows, dict):
            rows = (
                rows.get("data")
                or rows.get("klines")
                or rows.get("list")
                or []
            )

        stamps = []
        for row in rows:
            t = row_ts(row)
            if not t:
                continue
            stamps.append(t)
            if start_ms <= t < end_ms:
                c = row_close(row)
                if c > 0:
                    out[t] = c

        page += 1
        print(
            f"BF3 {symbol} mark page={page} "
            f"rows={len(rows)} total={len(out)}",
            flush=True,
        )

        if window_start <= start_ms:
            break

        if stamps:
            next_end = min(stamps) - 1
        else:
            next_end = window_start - 1

        if next_end >= cursor_end:
            break

        cursor_end = next_end
        time.sleep(max(PAUSE, 1.01))

    return out


def actual_perp_history(
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> dict[int, float]:
    rows = hd.download_symbol(
        symbol,
        INTERVAL,
        start_ms,
        end_ms,
        max(PAUSE, 1.01),
    )
    out = {}
    for row in rows:
        t = int(
            datetime.fromisoformat(
                row["timestamp"]
            ).timestamp() * 1000
        )
        c = float(row["close"])
        if c > 0:
            out[t] = c

    print(
        f"BF3 {symbol} actual_perp rows={len(out)}",
        flush=True,
    )
    return out


def funding_history(
    symbol: str,
    start_ms: int,
    end_ms: int,
):
    cursor_end = end_ms
    out = {}
    page = 0

    while cursor_end >= start_ms:
        rows = request_json(
            FUNDING,
            {
                "symbol": symbol,
                "startTime": start_ms,
                "endTime": cursor_end,
                "limit": 100,
            },
        )

        if isinstance(rows, dict):
            rows = (
                rows.get("data")
                or rows.get("fundingRates")
                or rows.get("list")
                or []
            )

        stamps = []
        for row in rows:
            t = row_ts(row)
            if not t:
                continue
            stamps.append(t)

            if start_ms <= t <= end_ms:
                try:
                    rate = float(
                        row.get(
                            "fundingRate",
                            row.get(
                                "funding_rate",
                                row.get("rate", 0),
                            ),
                        )
                        or 0
                    )
                except Exception:
                    rate = 0.0

                out[t] = rate

        page += 1

        if not stamps:
            break

        earliest = min(stamps)
        if earliest <= start_ms:
            break

        next_end = earliest - 1
        if next_end >= cursor_end:
            break

        cursor_end = next_end
        time.sleep(max(PAUSE, 1.01))

    result = sorted(out.items())
    print(
        f"BF3 {symbol} funding events={len(result)}",
        flush=True,
    )
    return result


def quote(mid: float, spread_bps: float):
    half = max(0.0, spread_bps) / 20000.0
    return mid * (1.0 - half), mid * (1.0 + half)


def funding_at(
    funding_rows,
    funding_times,
    t: int,
):
    i = bisect.bisect_left(funding_times, t)
    if i < len(funding_rows):
        return funding_rows[i][1], funding_rows[i][0]
    return 0.0, 0


def make_snapshot(
    symbol: str,
    t: int,
    spot: float,
    perp: float,
    funding_rate: float,
    next_funding_time_ms: int,
):
    sb, sa = quote(spot, SPOT_SPREAD_BPS)
    pb, pa = quote(perp, PERP_SPREAD_BPS)

    return {
        "timestamp": datetime.fromtimestamp(
            t / 1000,
            tz=timezone.utc,
        ).isoformat(),
        "symbol": symbol,
        "spot_bid": sb,
        "spot_ask": sa,
        "perp_bid": pb,
        "perp_ask": pa,
        "funding_rate": funding_rate,
        "next_funding_time_ms": next_funding_time_ms,
    }


def write_snapshots(path: Path, rows: list[dict]):
    rows.sort(
        key=lambda r: (r["timestamp"], r["symbol"])
    )
    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as f:
        writer = csv.DictWriter(
            f,
            fieldnames=FIELDS,
        )
        writer.writeheader()
        writer.writerows(rows)


def analyze_outcomes(
    outcome_path: Path,
    extra: dict,
):
    with outcome_path.open(
        newline="",
        encoding="utf-8-sig",
    ) as f:
        vals = [
            float(row["net_return_pct"])
            for row in csv.DictReader(f)
        ]

    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))

    if gross_loss:
        pf = gross_profit / gross_loss
    elif gross_profit:
        pf = 999.0
    else:
        pf = 0.0

    return {
        "closed": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": (
            round(100 * len(wins) / len(vals), 2)
            if vals
            else 0.0
        ),
        "profit_factor": round(pf, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        **extra,
    }


def run_adapter(
    snapshots: Path,
    outcomes: Path,
):
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


def main():
    if DAYS <= 0:
        raise ValueError("BF3_HIST_DAYS must be > 0")

    end = parse_dt(END_ISO)
    start = end - timedelta(days=DAYS)

    start_ms = int(start.timestamp() * 1000)
    end_ms = int(end.timestamp() * 1000)

    expected = int(
        (end - start).total_seconds()
        // (STEP_MS / 1000)
    )
    minimum = int(expected * MIN_COVERAGE)

    print(
        f"BF3_WINDOW start={start.isoformat()} "
        f"end={end.isoformat()} "
        f"days={DAYS} interval={INTERVAL} "
        f"expected={expected} minimum={minimum}",
        flush=True,
    )

    exact_rows = []
    proxy_rows = []
    quality = {}

    for symbol in SYMBOLS:
        print(f"BF3 {symbol} spot history", flush=True)
        spot = spot_history(
            symbol,
            start_ms,
            end_ms,
        )

        print(
            f"BF3 {symbol} actual perp history",
            flush=True,
        )
        actual = actual_perp_history(
            symbol,
            start_ms,
            end_ms,
        )

        print(
            f"BF3 {symbol} mark price history",
            flush=True,
        )
        mark = mark_price_history(
            symbol,
            start_ms,
            end_ms,
        )

        funding = funding_history(
            symbol,
            start_ms,
            end_ms,
        )
        ftimes = [x[0] for x in funding]

        exact_times = sorted(
            set(spot) & set(actual)
        )

        proxy_times = sorted(
            set(spot) & (set(actual) | set(mark))
        )

        actual_used = 0
        mark_used = 0

        for t in exact_times:
            fr, next_ft = funding_at(
                funding,
                ftimes,
                t,
            )
            exact_rows.append(
                make_snapshot(
                    symbol,
                    t,
                    spot[t],
                    actual[t],
                    fr,
                    next_ft,
                )
            )

        for t in proxy_times:
            if t in actual:
                perp = actual[t]
                actual_used += 1
            else:
                perp = mark[t]
                mark_used += 1

            fr, next_ft = funding_at(
                funding,
                ftimes,
                t,
            )

            proxy_rows.append(
                make_snapshot(
                    symbol,
                    t,
                    spot[t],
                    perp,
                    fr,
                    next_ft,
                )
            )

        exact_span_days = 0.0
        if len(exact_times) >= 2:
            exact_span_days = (
                exact_times[-1] - exact_times[0]
            ) / 1000 / 86400

        quality[symbol] = {
            "expected_360_bars": expected,
            "minimum_95pct_bars": minimum,
            "spot_rows": len(spot),
            "actual_perp_rows": len(actual),
            "mark_price_rows": len(mark),
            "exact_aligned_rows": len(exact_times),
            "exact_overlap_span_days": round(
                exact_span_days,
                2,
            ),
            "proxy_aligned_rows": len(proxy_times),
            "proxy_coverage_pct": round(
                100 * len(proxy_times) / expected,
                2,
            ),
            "actual_perp_used_in_proxy": actual_used,
            "mark_price_used_in_proxy": mark_used,
            "funding_events": len(funding),
        }

        print(
            "BF3_QUALITY "
            + json.dumps(
                {
                    "symbol": symbol,
                    **quality[symbol],
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    quality_path = OUT / "bf1_v3_quality.json"
    quality_path.write_text(
        json.dumps(
            quality,
            indent=2,
        ),
        encoding="utf-8",
    )

    exact_snapshots = OUT / "bf1_exact_overlap_snapshots.csv"
    proxy_snapshots = OUT / "bf1_360_proxy_snapshots.csv"

    write_snapshots(
        exact_snapshots,
        exact_rows,
    )
    write_snapshots(
        proxy_snapshots,
        proxy_rows,
    )

    if not exact_rows:
        raise RuntimeError(
            "No exact spot/perp overlap snapshots"
        )

    proxy_ok = all(
        q["proxy_aligned_rows"] >= minimum
        for q in quality.values()
    )

    if not proxy_ok:
        print(
            "BF3_PROXY_INVALID_INCOMPLETE_COVERAGE "
            + json.dumps(
                quality,
                separators=(",", ":"),
            ),
            flush=True,
        )
        raise RuntimeError(
            "BF1 V3 proxy coverage below threshold"
        )

    exact_outcomes = OUT / "bf1_exact_overlap_outcomes.csv"
    proxy_outcomes = OUT / "bf1_360_proxy_outcomes.csv"

    print("BF3_RUN exact overlap adapter", flush=True)
    run_adapter(
        exact_snapshots,
        exact_outcomes,
    )

    exact_summary = analyze_outcomes(
        exact_outcomes,
        {
            "mode": "EXACT_ACTUAL_PERP_OVERLAP",
            "days_requested": DAYS,
            "symbols": SYMBOLS,
            "fidelity":
                "BINGX_SPOT_PLUS_ACTUAL_PERP_TRADE_KLINES",
        },
    )

    print(
        "BF3_EXACT_RESULT "
        + json.dumps(
            exact_summary,
            separators=(",", ":"),
        ),
        flush=True,
    )

    print("BF3_RUN 360 proxy adapter", flush=True)
    run_adapter(
        proxy_snapshots,
        proxy_outcomes,
    )

    proxy_summary = analyze_outcomes(
        proxy_outcomes,
        {
            "mode": "360_PROXY",
            "days": DAYS,
            "symbols": SYMBOLS,
            "coverage_rule": f">={MIN_COVERAGE:.0%}",
            "fidelity":
                "ACTUAL_PERP_WHERE_AVAILABLE_PLUS_MARK_PRICE_PROXY_FOR_OLDER_GAPS",
            "warning":
                "Mark price is not executable perp trade price; forward validation required",
        },
    )

    (OUT / "bf1_exact_overlap_summary.json").write_text(
        json.dumps(
            exact_summary,
            indent=2,
        ),
        encoding="utf-8",
    )
    (OUT / "bf1_360_proxy_summary.json").write_text(
        json.dumps(
            proxy_summary,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        "BF3_PROXY_RESULT "
        + json.dumps(
            proxy_summary,
            separators=(",", ":"),
        ),
        flush=True,
    )
    print("BF3_STAGE_DONE", flush=True)


if __name__ == "__main__":
    main()
