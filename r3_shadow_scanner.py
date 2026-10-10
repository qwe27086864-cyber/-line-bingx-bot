"""V4.3R3 candidate shadow evaluation, simulation-only.

Accepts JSON objects (one per line) and produces JSON diagnostics.
Never imports app.py, uses APIs, connects to a database, or opens trades.
This is not a live or simulated trade engine, and does not replace scanner_job.py.

Example:
  python r3_shadow_scanner.py --input candidates.jsonl
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

from r3_manager import VERSION, r3_shadow_long_trend_filter


def evaluate_candidate(candidate: dict[str, Any]) -> dict[str, Any]:
    """Read-only assessment; never mutates input candidate."""
    if not isinstance(candidate, dict):
        raise TypeError("Candidate must be a JSON object")
    assessment = r3_shadow_long_trend_filter(candidate)
    return {
        "engine": VERSION,
        "mode": "SHADOW_ONLY_NO_TRADING",
        "symbol": str(candidate.get("symbol", "")),
        "side": str(candidate.get("side", "")),
        "strategy": str(candidate.get("strategy", "")),
        "score": candidate.get("score"),
        "assessment": assessment,
    }


def process_stream(source, target):
    stats = Counter()
    for line_number, line in enumerate(source, 1):
        if not line.strip():
            continue
        try:
            candidate = json.loads(line)
            report = evaluate_candidate(candidate)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            print(f"Line {line_number}: invalid candidate: {exc}", file=sys.stderr)
            stats["invalid"] += 1
            continue
        target.write(json.dumps(report, ensure_ascii=False) + "\n")
        stats["read"] += 1
        if report["assessment"]["applicable"]:
            stats["long_trend"] += 1
            if report["assessment"]["would_block"]:
                stats["would_block"] += 1
    return dict(stats)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, help="Input JSONL candidates (default: stdin)")
    args = parser.parse_args(argv)
    if args.input:
        with args.input.open(encoding="utf-8") as source:
            stats = process_stream(source, sys.stdout)
    else:
        stats = process_stream(sys.stdin, sys.stdout)
    print(f"R3 SHADOW SUMMARY: {json.dumps(stats)}", file=sys.stderr)
    return 0 if not stats.get("invalid") else 2


if __name__ == "__main__":
    raise SystemExit(main())
