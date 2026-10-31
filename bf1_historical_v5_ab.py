"""
BF1 Historical V5 A/B Research Runner

Purpose
-------
Run multiple BF1 cash-and-carry variants over the SAME historical snapshot file
without touching live/shadow services or DATABASE_URL.

Default input:
    data/bf1_v4_proxy/bf1_v4_static_snapshots.csv

What it compares
----------------
A) Current taker-style baseline:
   - current funding counted once in entry edge
   - MIN_EDGE = 0.10%

B) Funding projection tests:
   - current funding rate repeated for 24h (3 funding events)
   - current funding rate repeated for 48h (6 funding events)
   IMPORTANT: this does NOT look ahead to future realized funding rates.
   It only projects the CURRENT funding rate forward.

C) Lower-cost / maker-like research model:
   - configurable lower fees and slippage
   - this is a hypothetical research model, NOT a claim about actual BingX fees

D) Edge thresholds:
   - 0.00%
   - 0.05%
   - 0.10%

Outputs
-------
data/bf1_v5_ab/bf1_v5_ab_summary.csv
data/bf1_v5_ab/bf1_v5_ab_trades.csv
data/bf1_v5_ab/bf1_v5_ab_rejects.csv
data/bf1_v5_ab/bf1_v5_ab_summary.json

Historical research only.
No private API.
No orders.
No DATABASE_URL.
No shadow/live writes.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

os.environ.pop("DATABASE_URL", None)

CORE = "BF1_V5_AB"

# Keep the structural rules identical to BF1 unless explicitly varied.
NOTIONAL = float(os.getenv("BF5_NOTIONAL", "100"))
MAX_OPEN = int(os.getenv("BF5_MAX_OPEN", "3"))
CLOSE_BASIS = float(os.getenv("BF5_CLOSE_BASIS_PCT", "0.05"))
MIN_FUNDING = float(os.getenv("BF5_MIN_FUNDING_RATE", "0.0"))
FUNDING_EXIT = float(os.getenv("BF5_FUNDING_EXIT_RATE", "-0.0002"))
MAX_HOURS = float(os.getenv("BF5_MAX_HOLD_HOURS", "48"))

# Existing BF1 taker-style cost assumptions.
TAKER_SPOT_FEE = float(os.getenv("BF5_TAKER_SPOT_FEE", "0.0010"))
TAKER_PERP_FEE = float(os.getenv("BF5_TAKER_PERP_FEE", "0.0005"))
TAKER_SLIP = float(os.getenv("BF5_TAKER_SLIP_PER_LEG", "0.0001"))

# Hypothetical lower-cost / maker-like model.
# These are RESEARCH assumptions only and can be overridden by env vars.
LOW_SPOT_FEE = float(os.getenv("BF5_LOW_SPOT_FEE", "0.0005"))
LOW_PERP_FEE = float(os.getenv("BF5_LOW_PERP_FEE", "0.0002"))
LOW_SLIP = float(os.getenv("BF5_LOW_SLIP_PER_LEG", "0.00005"))

EDGE_THRESHOLDS = [
    float(x.strip())
    for x in os.getenv("BF5_EDGE_THRESHOLDS", "0.00,0.05,0.10").split(",")
    if x.strip()
]

OUT_DIR = Path(os.getenv("BF5_OUT_DIR", "data/bf1_v5_ab"))
OUT_DIR.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Variant:
    name: str
    cost_model: str
    funding_projection_events: int
    min_edge_pct: float
    spot_fee: float
    perp_fee: float
    slip_per_leg: float


def dt(value: str) -> datetime:
    x = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def make_variants() -> list[Variant]:
    variants: list[Variant] = []

    horizons = [
        ("NOW", 1),   # current BF1 behavior: one funding payment in expected edge
        ("24H", 3),   # typical 8h funding * 3
        ("48H", 6),   # typical 8h funding * 6
    ]
    cost_models = [
        (
            "TAKER",
            TAKER_SPOT_FEE,
            TAKER_PERP_FEE,
            TAKER_SLIP,
        ),
        (
            "LOWCOST",
            LOW_SPOT_FEE,
            LOW_PERP_FEE,
            LOW_SLIP,
        ),
    ]

    for cost_name, spot_fee, perp_fee, slip in cost_models:
        for horizon_name, events in horizons:
            for threshold in EDGE_THRESHOLDS:
                variants.append(
                    Variant(
                        name=f"{cost_name}_{horizon_name}_E{threshold:.2f}",
                        cost_model=cost_name,
                        funding_projection_events=events,
                        min_edge_pct=threshold,
                        spot_fee=spot_fee,
                        perp_fee=perp_fee,
                        slip_per_leg=slip,
                    )
                )

    # Put the exact current-style baseline first.
    variants.sort(
        key=lambda v: (
            0 if (
                v.cost_model == "TAKER"
                and v.funding_projection_events == 1
                and abs(v.min_edge_pct - 0.10) < 1e-12
            ) else 1,
            v.cost_model,
            v.funding_projection_events,
            v.min_edge_pct,
        )
    )
    return variants


def parse_snapshot(raw: dict, variant: Variant) -> dict:
    sb = float(raw["spot_bid"])
    sa = float(raw["spot_ask"])
    pb = float(raw["perp_bid"])
    pa = float(raw["perp_ask"])
    fr = float(raw.get("funding_rate") or 0.0)

    open_basis = (pb / sa - 1.0) * 100.0
    close_basis = (pa / sb - 1.0) * 100.0

    # Round-trip fees: entry + exit on spot and perp.
    fee_pct = 2.0 * (variant.spot_fee + variant.perp_fee) * 100.0

    # Four legs total: spot in/out + perp in/out.
    slip_pct = 4.0 * variant.slip_per_leg * 100.0

    # No lookahead: repeat CURRENT funding estimate over assumed events.
    projected_funding_pct = (
        fr * 100.0 * variant.funding_projection_events
    )

    expected_net_edge = (
        open_basis
        - CLOSE_BASIS
        + projected_funding_pct
        - fee_pct
        - slip_pct
    )

    return {
        "t": dt(raw["timestamp"]),
        "symbol": raw["symbol"].upper(),
        "spot_bid": sb,
        "spot_ask": sa,
        "perp_bid": pb,
        "perp_ask": pa,
        "funding_rate": fr,
        "next_funding_time_ms": int(float(raw.get("next_funding_time_ms") or 0)),
        "open_basis_pct": open_basis,
        "close_basis_pct": close_basis,
        "projected_funding_pct": projected_funding_pct,
        "round_trip_fee_pct": fee_pct,
        "round_trip_slippage_pct": slip_pct,
        "expected_net_edge_pct": expected_net_edge,
    }


def open_reason(s: dict, open_count: int, variant: Variant) -> tuple[bool, str]:
    if open_count >= MAX_OPEN:
        return False, "MAX_OPEN"
    if s["funding_rate"] < MIN_FUNDING:
        return False, "FUNDING_BELOW_MIN"
    if s["open_basis_pct"] <= 0:
        return False, "NO_POSITIVE_PREMIUM"
    if s["expected_net_edge_pct"] < variant.min_edge_pct:
        return False, "NET_EDGE_TOO_SMALL"
    return True, "OPEN"


def close_reason(pos: dict, s: dict) -> str | None:
    if s["close_basis_pct"] <= CLOSE_BASIS:
        return "BASIS_CONVERGED"
    if s["funding_rate"] <= FUNDING_EXIT:
        return "FUNDING_TURNED_ADVERSE"
    age_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0
    if age_h >= MAX_HOURS:
        return "MAX_HOLD"
    return None


def close_calc(pos: dict, s: dict, variant: Variant) -> dict:
    qty = pos["qty"]

    gross = (
        qty * (s["spot_bid"] - pos["entry_spot"])
        + qty * (pos["entry_perp"] - s["perp_ask"])
        + pos["funding"]
    )

    fee = qty * (
        pos["entry_spot"] * variant.spot_fee
        + s["spot_bid"] * variant.spot_fee
        + pos["entry_perp"] * variant.perp_fee
        + s["perp_ask"] * variant.perp_fee
    )

    slip = qty * (
        pos["entry_spot"]
        + s["spot_bid"]
        + pos["entry_perp"]
        + s["perp_ask"]
    ) * variant.slip_per_leg

    net = gross - fee - slip
    capital = qty * pos["entry_spot"]
    ret = net / capital * 100.0 if capital > 0 else 0.0

    return {
        "gross_pnl_usdt": gross,
        "fee_usdt": fee,
        "slippage_usdt": slip,
        "net_pnl_usdt": net,
        "net_return_pct": ret,
    }


def run_variant(raw_rows: list[dict], variant: Variant):
    rows = [parse_snapshot(r, variant) for r in raw_rows]
    rows.sort(key=lambda x: (x["t"], x["symbol"]))

    opens: dict[str, dict] = {}
    trades: list[dict] = []
    rejects = Counter()
    opens_count = Counter()
    closes_count = Counter()

    for s in rows:
        symbol = s["symbol"]
        pos = opens.get(symbol)

        if pos:
            # Accrue actual historical funding when a funding timestamp is reached.
            nxt = int(pos.get("next_funding") or 0)
            last = int(pos.get("last_funding") or 0)

            if (
                nxt
                and int(s["t"].timestamp() * 1000) >= nxt
                and last < nxt
            ):
                perp_mid = (s["perp_bid"] + s["perp_ask"]) / 2.0
                pos["funding"] += pos["qty"] * perp_mid * s["funding_rate"]
                pos["funding_events"] += 1
                pos["last_funding"] = nxt

                # Snapshot's next_funding_time_ms should advance after the event.
                new_next = int(s["next_funding_time_ms"] or 0)
                if new_next > nxt:
                    pos["next_funding"] = new_next

            reason = close_reason(pos, s)
            if reason:
                result = close_calc(pos, s, variant)
                hold_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0

                trades.append(
                    {
                        "variant": variant.name,
                        "cost_model": variant.cost_model,
                        "funding_projection_events": variant.funding_projection_events,
                        "min_edge_pct": variant.min_edge_pct,
                        "symbol": symbol,
                        "opened_at": pos["opened_at"].isoformat(),
                        "closed_at": s["t"].isoformat(),
                        "hold_hours": round(hold_h, 4),
                        "entry_open_basis_pct": round(pos["entry_open_basis_pct"], 6),
                        "entry_funding_rate": round(pos["entry_funding_rate"], 10),
                        "entry_projected_funding_pct": round(pos["entry_projected_funding_pct"], 6),
                        "entry_expected_net_edge_pct": round(pos["entry_expected_net_edge_pct"], 6),
                        "realized_funding_events": pos["funding_events"],
                        "realized_funding_usdt": round(pos["funding"], 8),
                        "exit_close_basis_pct": round(s["close_basis_pct"], 6),
                        "exit_reason": reason,
                        **{k: round(v, 8) for k, v in result.items()},
                    }
                )
                closes_count[reason] += 1
                del opens[symbol]
                pos = None

        # Same bar can re-open after a close, matching the old adapter behavior.
        if symbol not in opens:
            ok, reason = open_reason(s, len(opens), variant)
            if not ok:
                rejects[reason] += 1
            else:
                qty = NOTIONAL / s["spot_ask"]
                opens[symbol] = {
                    "opened_at": s["t"],
                    "qty": qty,
                    "entry_spot": s["spot_ask"],
                    "entry_perp": s["perp_bid"],
                    "entry_open_basis_pct": s["open_basis_pct"],
                    "entry_funding_rate": s["funding_rate"],
                    "entry_projected_funding_pct": s["projected_funding_pct"],
                    "entry_expected_net_edge_pct": s["expected_net_edge_pct"],
                    "funding": 0.0,
                    "funding_events": 0,
                    "last_funding": 0,
                    "next_funding": int(s["next_funding_time_ms"] or 0),
                }
                opens_count[symbol] += 1

    return trades, rejects, opens_count, closes_count, len(opens)


def profit_factor(values: list[float]) -> float:
    gp = sum(x for x in values if x > 0)
    gl = abs(sum(x for x in values if x < 0))
    if gl > 0:
        return gp / gl
    if gp > 0:
        return math.inf
    return 0.0


def max_drawdown_pct(values: list[float]) -> float:
    # Trade-return-points equity curve; diagnostic only.
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for x in values:
        equity += x
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return abs(worst)


def summarize_variant(
    variant: Variant,
    trades: list[dict],
    rejects: Counter,
    opens_count: Counter,
    closes_count: Counter,
    open_left: int,
) -> dict:
    vals = [float(t["net_return_pct"]) for t in trades]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    pf = profit_factor(vals)

    by_symbol = defaultdict(list)
    for t in trades:
        by_symbol[t["symbol"]].append(float(t["net_return_pct"]))

    return {
        "variant": variant.name,
        "cost_model": variant.cost_model,
        "funding_projection_events": variant.funding_projection_events,
        "min_edge_pct": variant.min_edge_pct,
        "spot_fee": variant.spot_fee,
        "perp_fee": variant.perp_fee,
        "slip_per_leg": variant.slip_per_leg,
        "closed": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100.0 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": "INF" if math.isinf(pf) else round(pf, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "avg_net_return_pct": round(sum(vals) / len(vals), 6) if vals else 0.0,
        "best_trade_pct": round(max(vals), 6) if vals else 0.0,
        "worst_trade_pct": round(min(vals), 6) if vals else 0.0,
        "max_drawdown_trade_points_pct": round(max_drawdown_pct(vals), 4),
        "avg_hold_hours": round(
            sum(float(t["hold_hours"]) for t in trades) / len(trades), 4
        ) if trades else 0.0,
        "avg_realized_funding_events": round(
            sum(int(t["realized_funding_events"]) for t in trades) / len(trades), 4
        ) if trades else 0.0,
        "open_positions_left_at_end": open_left,
        "opens_BTC": int(opens_count.get("BTC-USDT", 0)),
        "opens_ETH": int(opens_count.get("ETH-USDT", 0)),
        "opens_SOL": int(opens_count.get("SOL-USDT", 0)),
        "reject_MAX_OPEN": int(rejects.get("MAX_OPEN", 0)),
        "reject_FUNDING_BELOW_MIN": int(rejects.get("FUNDING_BELOW_MIN", 0)),
        "reject_NO_POSITIVE_PREMIUM": int(rejects.get("NO_POSITIVE_PREMIUM", 0)),
        "reject_NET_EDGE_TOO_SMALL": int(rejects.get("NET_EDGE_TOO_SMALL", 0)),
        "close_BASIS_CONVERGED": int(closes_count.get("BASIS_CONVERGED", 0)),
        "close_FUNDING_TURNED_ADVERSE": int(closes_count.get("FUNDING_TURNED_ADVERSE", 0)),
        "close_MAX_HOLD": int(closes_count.get("MAX_HOLD", 0)),
        "BTC_sum_pct": round(sum(by_symbol["BTC-USDT"]), 4),
        "ETH_sum_pct": round(sum(by_symbol["ETH-USDT"]), 4),
        "SOL_sum_pct": round(sum(by_symbol["SOL-USDT"]), 4),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--snapshots",
        default="data/bf1_v4_proxy/bf1_v4_static_snapshots.csv",
        help="BF1 V4 static snapshot CSV",
    )
    ap.add_argument(
        "--out-dir",
        default=str(OUT_DIR),
        help="Output directory",
    )
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(args.snapshots, newline="", encoding="utf-8-sig") as f:
        raw_rows = list(csv.DictReader(f))

    if not raw_rows:
        raise RuntimeError("No historical snapshots found")

    variants = make_variants()
    all_trades: list[dict] = []
    summaries: list[dict] = []
    reject_rows: list[dict] = []

    print(
        f"BF5_AB_START snapshots={len(raw_rows)} variants={len(variants)} "
        f"source={args.snapshots} historical_only=true",
        flush=True,
    )

    for i, variant in enumerate(variants, 1):
        trades, rejects, opens_count, closes_count, open_left = run_variant(
            raw_rows, variant
        )
        summary = summarize_variant(
            variant, trades, rejects, opens_count, closes_count, open_left
        )
        summaries.append(summary)
        all_trades.extend(trades)

        reject_rows.append(
            {
                "variant": variant.name,
                "MAX_OPEN": rejects.get("MAX_OPEN", 0),
                "FUNDING_BELOW_MIN": rejects.get("FUNDING_BELOW_MIN", 0),
                "NO_POSITIVE_PREMIUM": rejects.get("NO_POSITIVE_PREMIUM", 0),
                "NET_EDGE_TOO_SMALL": rejects.get("NET_EDGE_TOO_SMALL", 0),
            }
        )

        print(
            "BF5_AB_RESULT "
            + json.dumps(
                {
                    "i": i,
                    "n": len(variants),
                    "variant": variant.name,
                    "closed": summary["closed"],
                    "wins": summary["wins"],
                    "losses": summary["losses"],
                    "win_rate_pct": summary["win_rate_pct"],
                    "profit_factor": summary["profit_factor"],
                    "sum_net_return_pct": summary["sum_net_return_pct"],
                    "rejects": dict(rejects),
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    # Rank useful candidates first. Avoid treating tiny samples as winners.
    def rank_key(s):
        closed = int(s["closed"])
        pf = s["profit_factor"]
        pf_num = 999.0 if pf == "INF" else float(pf)
        sample_penalty = 0 if closed >= 200 else (200 - closed)
        return (
            sample_penalty,
            -pf_num,
            -float(s["sum_net_return_pct"]),
            -closed,
        )

    ranked = sorted(summaries, key=rank_key)
    for rank, row in enumerate(ranked, 1):
        row["rank_sample_aware"] = rank

    # Write summary.
    summary_path = out_dir / "bf1_v5_ab_summary.csv"
    with summary_path.open("w", newline="", encoding="utf-8") as f:
        fields = list(ranked[0].keys())
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(ranked)

    # Write trades.
    trades_path = out_dir / "bf1_v5_ab_trades.csv"
    if all_trades:
        with trades_path.open("w", newline="", encoding="utf-8") as f:
            fields = list(all_trades[0].keys())
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            w.writerows(all_trades)
    else:
        trades_path.write_text("", encoding="utf-8")

    # Write rejects.
    rejects_path = out_dir / "bf1_v5_ab_rejects.csv"
    with rejects_path.open("w", newline="", encoding="utf-8") as f:
        fields = list(reject_rows[0].keys())
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(reject_rows)

    json_path = out_dir / "bf1_v5_ab_summary.json"
    json_path.write_text(
        json.dumps(
            {
                "source_snapshots": args.snapshots,
                "snapshot_rows": len(raw_rows),
                "historical_only": True,
                "reverse_cash_and_carry_enabled": False,
                "notes": [
                    "24h/48h funding projection repeats the current rate; no future funding lookahead.",
                    "LOWCOST is a hypothetical research execution-cost model, not an assertion of actual BingX fees.",
                    "Realized PnL accrues actual historical funding events from the snapshot stream.",
                    "PF=INF means no losing trades and must not be interpreted as infinite real-world quality.",
                    "Sample size remains a primary acceptance criterion.",
                ],
                "variants": [asdict(v) for v in variants],
                "ranked_results": ranked,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        f"BF5_AB_DONE summary={summary_path} trades={trades_path} rejects={rejects_path}",
        flush=True,
    )


if __name__ == "__main__":
    main()
