
#!/usr/bin/env python3
"""R1 September 2026 historical universe evidence probe.

Research only. No orders. No changes to R1.
Current BingX contracts are discovery seeds, NOT a verified
historical listing snapshot.
"""

import argparse
import csv
import json
import urllib.request

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urlencode


BASE = "https://open-api.bingx.com"

DATES = [
    "2026-09-01",
    "2026-09-10",
    "2026-09-20",
    "2026-09-30",
]


def api(path):
    request = urllib.request.Request(
        BASE + path,
        headers={
            "User-Agent": "R1-Historical-Universe/1.0"
        },
    )

    with urllib.request.urlopen(
        request,
        timeout=25,
    ) as response:
        return json.load(response)


def get_symbols():
    result = api(
        "/openApi/swap/v2/quote/contracts"
    )

    if str(result.get("code")) != "0":
        raise RuntimeError(result)

    contracts = result.get("data", [])

    if isinstance(contracts, dict):
        contracts = contracts.get(
            "contracts",
            contracts.get("list", []),
        )

    return sorted({
        item["symbol"]
        for item in contracts
        if isinstance(item, dict)
        and isinstance(item.get("symbol"), str)
        and item["symbol"].endswith("-USDT")
    })


def check_history(symbol, day):
    start = int(
        datetime.fromisoformat(
            day + "T12:00:00+00:00"
        ).timestamp() * 1000
    )

    end = start + 3600000

    params = urlencode({
        "symbol": symbol,
        "interval": "1m",
        "startTime": start,
        "endTime": end - 1,
        "limit": 60,
    })

    try:
        result = api(
            "/openApi/swap/v3/quote/klines?"
            + params
        )

        if str(result.get("code")) != "0":
            return {
                "symbol": symbol,
                "day": day,
                "status": "API_ERROR",
                "bars": 0,
                "detail": str(
                    result.get("msg", "")
                )[:100],
            }

        bars = result.get("data") or []
        timestamps = set()

        for bar in bars:
            if isinstance(bar, list):
                timestamp = bar[0]
            else:
                timestamp = bar.get(
                    "time",
                    bar.get("openTime"),
                )

            if timestamp is None:
                continue

            timestamp = int(timestamp)

            if timestamp < 10**11:
                timestamp *= 1000

            if start <= timestamp < end:
                timestamps.add(timestamp)

        return {
            "symbol": symbol,
            "day": day,
            "status": (
                "HAS_HISTORY"
                if timestamps
                else "NO_BARS"
            ),
            "bars": len(timestamps),
            "detail": "",
        }

    except Exception as error:
        return {
            "symbol": symbol,
            "day": day,
            "status": "ERROR",
            "bars": 0,
            "detail": str(error)[:100],
        }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--out",
        default="r1_september_universe",
    )

    parser.add_argument(
        "--max-symbols",
        type=int,
        default=0,
    )

    parser.add_argument(
        "--workers",
        type=int,
        default=5,
    )

    args = parser.parse_args()

    output = Path(args.out)
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    symbols = get_symbols()

    if args.max_symbols > 0:
        symbols = symbols[:args.max_symbols]

    results = []

    with ThreadPoolExecutor(
        max_workers=args.workers
    ) as executor:

        futures = [
            executor.submit(
                check_history,
                symbol,
                day,
            )
            for symbol in symbols
            for day in DATES
        ]

        for future in as_completed(futures):
            results.append(future.result())

    results.sort(
        key=lambda row: (
            row["symbol"],
            row["day"],
        )
    )

    with (
        output / "evidence.csv"
    ).open(
        "w",
        newline="",
        encoding="utf-8",
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "symbol",
                "day",
                "status",
                "bars",
                "detail",
            ],
        )

        writer.writeheader()
        writer.writerows(results)

    eligible = {}

    for day in DATES:
        eligible[day] = sorted(
            row["symbol"]
            for row in results
            if row["day"] == day
            and row["status"] == "HAS_HISTORY"
        )

    report = {
        "test": "R1 September universe probe",
        "total_symbols_checked": len(symbols),
        "checkpoint_counts": {
            day: len(eligible[day])
            for day in DATES
        },
        "eligible_by_checkpoint": eligible,
        "limitations": [
            "Current contract list is not a historical listing snapshot.",
            "Delisted symbols may be missing.",
            "One-hour samples do not prove full-month availability.",
            "Historical spread is not verified.",
            "No trades are executed.",
        ],
        "errors": [
            row
            for row in results
            if row["status"] in (
                "ERROR",
                "API_ERROR",
            )
        ],
    }

    (
        output / "universe_evidence.json"
    ).write_text(
        json.dumps(
            report,
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "checked": len(symbols),
                "counts": report[
                    "checkpoint_counts"
                ],
                "errors": len(
                    report["errors"]
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
