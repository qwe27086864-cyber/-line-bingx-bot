"""
BF1 V7 Two-Way Basis + Funding Historical Research
(replaces bf1_historical_v5_ab.py so the existing V4->handoff runner executes it)

Research goals
--------------
V6 proved that removing the positive-basis gate only increased the sample
from 2 trades to ~3 trades. V7 therefore tests the structural question:

A) CASH_AND_CARRY
   Buy spot + short perpetual.
   Benefits from perp premium and positive funding.

B) REVERSE_CARRY
   Short spot + long perpetual.
   Benefits from perp discount and negative funding.
   IMPORTANT: requires spot borrow/margin availability in real execution.

V7 also compares:
- current taker-style execution costs
- lower-cost / maker-like research costs
- zero exchange fee/slippage diagnostic (borrow cost still applied)

No future funding lookahead is used for ENTRY. The current funding rate is
projected over 24h or 48h.

Historical research only.
No DATABASE_URL.
No orders.
No live/shadow writes.
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

NOTIONAL = float(os.getenv("BF7_NOTIONAL", "100"))
MAX_OPEN = int(os.getenv("BF7_MAX_OPEN", "3"))
MAX_HOURS = float(os.getenv("BF7_MAX_HOLD_HOURS", "48"))

# Exit / risk parameters in percentage points.
CLOSE_BASIS = float(os.getenv("BF7_CLOSE_BASIS_PCT", "0.05"))
BASIS_ADVERSE_STOP = float(os.getenv("BF7_BASIS_ADVERSE_STOP_PCT", "0.25"))
BASIS_TAKE_PROFIT = float(os.getenv("BF7_BASIS_TAKE_PROFIT_PCT", "0.15"))

# Funding sign reversal exit threshold:
# cash short-perp exits if funding <= -threshold
# reverse long-perp exits if funding >= +threshold
FUNDING_ADVERSE_ABS = float(os.getenv("BF7_FUNDING_ADVERSE_ABS", "0.0002"))

# Reverse carry needs spot borrow. Default 10% APR research assumption.
BORROW_APR = float(os.getenv("BF7_SPOT_BORROW_APR", "0.10"))

EDGE_THRESHOLDS = [
    float(x.strip())
    for x in os.getenv("BF7_EDGE_THRESHOLDS", "0.00,0.05").split(",")
    if x.strip()
]

DEFAULT_OUT = "data/bf1_v5_ab"


@dataclass(frozen=True)
class CostModel:
    name: str
    spot_fee: float
    perp_fee: float
    slip_per_leg: float


@dataclass(frozen=True)
class Variant:
    name: str
    cost_name: str
    funding_projection_events: int
    projected_hold_hours: float
    min_edge_pct: float
    enable_cash: bool
    enable_reverse: bool


COST_MODELS = {
    "TAKER": CostModel(
        "TAKER",
        float(os.getenv("BF7_TAKER_SPOT_FEE", "0.0010")),
        float(os.getenv("BF7_TAKER_PERP_FEE", "0.0005")),
        float(os.getenv("BF7_TAKER_SLIP_PER_LEG", "0.0001")),
    ),
    "LOWCOST": CostModel(
        "LOWCOST",
        float(os.getenv("BF7_LOW_SPOT_FEE", "0.0005")),
        float(os.getenv("BF7_LOW_PERP_FEE", "0.0002")),
        float(os.getenv("BF7_LOW_SLIP_PER_LEG", "0.00005")),
    ),
    "ZEROEX": CostModel(
        "ZEROEX",
        0.0,
        0.0,
        0.0,
    ),
}


def parse_dt(s: str) -> datetime:
    x = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def build_variants() -> list[Variant]:
    out = [
        Variant(
            name="BASELINE_CURRENT",
            cost_name="TAKER",
            funding_projection_events=1,
            projected_hold_hours=8.0,
            min_edge_pct=0.10,
            enable_cash=True,
            enable_reverse=False,
        )
    ]

    for cost in ("TAKER", "LOWCOST", "ZEROEX"):
        for events, hours, label in ((3, 24.0, "24H"), (6, 48.0, "48H")):
            for edge in EDGE_THRESHOLDS:
                out.append(
                    Variant(
                        name=f"TWOWAY_{cost}_{label}_E{edge:.2f}",
                        cost_name=cost,
                        funding_projection_events=events,
                        projected_hold_hours=hours,
                        min_edge_pct=edge,
                        enable_cash=True,
                        enable_reverse=True,
                    )
                )
    return out


def quote_fields(raw: dict) -> dict:
    return {
        "t": parse_dt(raw["timestamp"]),
        "symbol": raw["symbol"].upper(),
        "spot_bid": float(raw["spot_bid"]),
        "spot_ask": float(raw["spot_ask"]),
        "perp_bid": float(raw["perp_bid"]),
        "perp_ask": float(raw["perp_ask"]),
        "funding_rate": float(raw.get("funding_rate") or 0.0),
        "next_funding_time_ms": int(float(raw.get("next_funding_time_ms") or 0)),
    }


def round_trip_exchange_cost_pct(cm: CostModel) -> float:
    return (
        2.0 * (cm.spot_fee + cm.perp_fee) * 100.0
        + 4.0 * cm.slip_per_leg * 100.0
    )


def projected_borrow_pct(hours: float) -> float:
    return BORROW_APR * (hours / (365.0 * 24.0)) * 100.0


def cash_metrics(s: dict, v: Variant, cm: CostModel) -> dict:
    # Entry: buy spot ask, short perp bid.
    entry_basis = (s["perp_bid"] / s["spot_ask"] - 1.0) * 100.0
    projected_funding = s["funding_rate"] * 100.0 * v.funding_projection_events
    exchange_cost = round_trip_exchange_cost_pct(cm)

    # Assume eventual close near CLOSE_BASIS residual.
    expected_edge = (
        entry_basis
        - CLOSE_BASIS
        + projected_funding
        - exchange_cost
    )

    return {
        "direction": "CASH",
        "entry_basis_pct": entry_basis,
        "projected_funding_pct": projected_funding,
        "projected_borrow_pct": 0.0,
        "exchange_cost_pct": exchange_cost,
        "expected_edge_pct": expected_edge,
    }


def reverse_metrics(s: dict, v: Variant, cm: CostModel) -> dict:
    # Entry: short spot bid, long perp ask.
    # Positive reverse basis means spot > perp.
    entry_basis = (s["spot_bid"] / s["perp_ask"] - 1.0) * 100.0

    # Negative funding benefits long perp.
    projected_funding = (-s["funding_rate"]) * 100.0 * v.funding_projection_events
    exchange_cost = round_trip_exchange_cost_pct(cm)
    borrow_pct = projected_borrow_pct(v.projected_hold_hours)

    expected_edge = (
        entry_basis
        - CLOSE_BASIS
        + projected_funding
        - exchange_cost
        - borrow_pct
    )

    return {
        "direction": "REVERSE",
        "entry_basis_pct": entry_basis,
        "projected_funding_pct": projected_funding,
        "projected_borrow_pct": borrow_pct,
        "exchange_cost_pct": exchange_cost,
        "expected_edge_pct": expected_edge,
    }


def choose_entry(s: dict, v: Variant, cm: CostModel):
    candidates = []

    if v.enable_cash:
        candidates.append(cash_metrics(s, v, cm))

    if v.enable_reverse:
        candidates.append(reverse_metrics(s, v, cm))

    if not candidates:
        return None, "NO_DIRECTION_ENABLED"

    best = max(candidates, key=lambda x: x["expected_edge_pct"])

    if best["expected_edge_pct"] < v.min_edge_pct:
        cash_edge = next((x["expected_edge_pct"] for x in candidates if x["direction"] == "CASH"), None)
        rev_edge = next((x["expected_edge_pct"] for x in candidates if x["direction"] == "REVERSE"), None)

        if cash_edge is not None and rev_edge is not None:
            if cash_edge < v.min_edge_pct and rev_edge < v.min_edge_pct:
                return None, "BOTH_EDGE_TOO_SMALL"
        return None, f'{best["direction"]}_EDGE_TOO_SMALL'

    return best, "OPEN"


def current_close_basis(s: dict, direction: str) -> float:
    if direction == "CASH":
        # Exit: sell spot bid, buy perp ask.
        return (s["perp_ask"] / s["spot_bid"] - 1.0) * 100.0

    # REVERSE exit: buy spot ask, sell perp bid.
    return (s["spot_ask"] / s["perp_bid"] - 1.0) * 100.0


def should_close(pos: dict, s: dict):
    age_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0
    now_basis = current_close_basis(s, pos["direction"])
    entry_basis = pos["entry_basis_pct"]

    favorable = entry_basis - now_basis
    adverse = now_basis - entry_basis

    if entry_basis > CLOSE_BASIS and now_basis <= CLOSE_BASIS:
        return "BASIS_CONVERGED", now_basis

    if favorable >= BASIS_TAKE_PROFIT:
        return "BASIS_TAKE_PROFIT", now_basis

    if adverse >= BASIS_ADVERSE_STOP:
        return "BASIS_ADVERSE_STOP", now_basis

    if pos["direction"] == "CASH":
        if s["funding_rate"] <= -FUNDING_ADVERSE_ABS:
            return "FUNDING_TURNED_ADVERSE", now_basis
    else:
        if s["funding_rate"] >= FUNDING_ADVERSE_ABS:
            return "FUNDING_TURNED_ADVERSE", now_basis

    if age_h >= MAX_HOURS:
        return "MAX_HOLD", now_basis

    return None, now_basis


def accrue_funding(pos: dict, s: dict):
    nxt = int(pos.get("next_funding") or 0)
    last = int(pos.get("last_funding") or 0)

    if not nxt:
        return

    if int(s["t"].timestamp() * 1000) < nxt:
        return

    if last >= nxt:
        return

    perp_mid = (s["perp_bid"] + s["perp_ask"]) / 2.0
    base = pos["qty"] * perp_mid * s["funding_rate"]

    if pos["direction"] == "CASH":
        # Short perp receives positive funding.
        funding_usdt = base
    else:
        # Long perp receives negative funding.
        funding_usdt = -base

    pos["funding"] += funding_usdt
    pos["funding_events"] += 1
    pos["last_funding"] = nxt

    new_next = int(s["next_funding_time_ms"] or 0)
    if new_next > nxt:
        pos["next_funding"] = new_next


def close_pnl(pos: dict, s: dict, cm: CostModel):
    q = pos["qty"]

    if pos["direction"] == "CASH":
        # Long spot + short perp.
        gross = (
            q * (s["spot_bid"] - pos["entry_spot"])
            + q * (pos["entry_perp"] - s["perp_ask"])
            + pos["funding"]
        )

        fee = q * (
            pos["entry_spot"] * cm.spot_fee
            + s["spot_bid"] * cm.spot_fee
            + pos["entry_perp"] * cm.perp_fee
            + s["perp_ask"] * cm.perp_fee
        )

        slip = q * (
            pos["entry_spot"]
            + s["spot_bid"]
            + pos["entry_perp"]
            + s["perp_ask"]
        ) * cm.slip_per_leg

        borrow = 0.0

    else:
        # Short spot + long perp.
        gross = (
            q * (pos["entry_spot"] - s["spot_ask"])
            + q * (s["perp_bid"] - pos["entry_perp"])
            + pos["funding"]
        )

        fee = q * (
            pos["entry_spot"] * cm.spot_fee
            + s["spot_ask"] * cm.spot_fee
            + pos["entry_perp"] * cm.perp_fee
            + s["perp_bid"] * cm.perp_fee
        )

        slip = q * (
            pos["entry_spot"]
            + s["spot_ask"]
            + pos["entry_perp"]
            + s["perp_bid"]
        ) * cm.slip_per_leg

        hold_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0
        borrow = (
            q
            * pos["entry_spot"]
            * BORROW_APR
            * hold_h
            / (365.0 * 24.0)
        )

    net = gross - fee - slip - borrow
    capital = q * pos["entry_spot"]
    ret = net / capital * 100.0 if capital > 0 else 0.0

    return gross, fee, slip, borrow, net, ret


def run_variant(raw_rows: list[dict], v: Variant):
    cm = COST_MODELS[v.cost_name]

    rows = [quote_fields(r) for r in raw_rows]
    rows.sort(key=lambda x: (x["t"], x["symbol"]))

    opens = {}
    trades = []
    rejects = Counter()
    closes = Counter()
    direction_entries = Counter()
    symbol_entries = Counter()

    for s in rows:
        sym = s["symbol"]
        pos = opens.get(sym)

        if pos:
            accrue_funding(pos, s)
            reason, exit_basis = should_close(pos, s)

            if reason:
                gross, fee, slip, borrow, net, ret = close_pnl(pos, s, cm)
                hold_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0

                trades.append({
                    "variant": v.name,
                    "cost_name": v.cost_name,
                    "direction": pos["direction"],
                    "symbol": sym,
                    "opened_at": pos["opened_at"].isoformat(),
                    "closed_at": s["t"].isoformat(),
                    "hold_hours": round(hold_h, 4),
                    "entry_basis_pct": round(pos["entry_basis_pct"], 6),
                    "entry_funding_rate": round(pos["entry_funding_rate"], 10),
                    "entry_projected_funding_pct": round(pos["entry_projected_funding_pct"], 6),
                    "entry_projected_borrow_pct": round(pos["entry_projected_borrow_pct"], 6),
                    "entry_exchange_cost_pct": round(pos["entry_exchange_cost_pct"], 6),
                    "entry_expected_edge_pct": round(pos["entry_expected_edge_pct"], 6),
                    "exit_basis_pct": round(exit_basis, 6),
                    "funding_events": pos["funding_events"],
                    "funding_usdt": round(pos["funding"], 8),
                    "borrow_usdt": round(borrow, 8),
                    "gross_pnl_usdt": round(gross, 8),
                    "fee_usdt": round(fee, 8),
                    "slippage_usdt": round(slip, 8),
                    "net_pnl_usdt": round(net, 8),
                    "net_return_pct": round(ret, 8),
                    "exit_reason": reason,
                })

                closes[reason] += 1
                del opens[sym]

        if sym not in opens:
            if len(opens) >= MAX_OPEN:
                rejects["MAX_OPEN"] += 1
                continue

            choice, reason = choose_entry(s, v, cm)
            if choice is None:
                rejects[reason] += 1
                continue

            direction = choice["direction"]

            if direction == "CASH":
                entry_spot = s["spot_ask"]
                entry_perp = s["perp_bid"]
            else:
                entry_spot = s["spot_bid"]
                entry_perp = s["perp_ask"]

            qty = NOTIONAL / max(entry_spot, 1e-12)

            opens[sym] = {
                "direction": direction,
                "opened_at": s["t"],
                "qty": qty,
                "entry_spot": entry_spot,
                "entry_perp": entry_perp,
                "entry_basis_pct": choice["entry_basis_pct"],
                "entry_funding_rate": s["funding_rate"],
                "entry_projected_funding_pct": choice["projected_funding_pct"],
                "entry_projected_borrow_pct": choice["projected_borrow_pct"],
                "entry_exchange_cost_pct": choice["exchange_cost_pct"],
                "entry_expected_edge_pct": choice["expected_edge_pct"],
                "funding": 0.0,
                "funding_events": 0,
                "last_funding": 0,
                "next_funding": int(s["next_funding_time_ms"] or 0),
            }

            direction_entries[direction] += 1
            symbol_entries[(direction, sym)] += 1

    return trades, rejects, closes, direction_entries, symbol_entries, len(opens)


def profit_factor(vals):
    gp = sum(x for x in vals if x > 0)
    gl = abs(sum(x for x in vals if x < 0))
    if gl > 0:
        return gp / gl
    if gp > 0:
        return math.inf
    return 0.0


def max_drawdown(vals):
    eq = 0.0
    peak = 0.0
    worst = 0.0

    for x in vals:
        eq += x
        peak = max(peak, eq)
        worst = min(worst, eq - peak)

    return abs(worst)


def make_summary(v, trades, rejects, closes, direction_entries, symbol_entries, open_left):
    vals = [float(t["net_return_pct"]) for t in trades]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    p = profit_factor(vals)

    by_dir = defaultdict(list)
    by_sym = defaultdict(list)

    for t in trades:
        by_dir[t["direction"]].append(float(t["net_return_pct"]))
        by_sym[t["symbol"]].append(float(t["net_return_pct"]))

    return {
        "variant": v.name,
        "cost_name": v.cost_name,
        "funding_projection_events": v.funding_projection_events,
        "projected_hold_hours": v.projected_hold_hours,
        "min_edge_pct": v.min_edge_pct,
        "closed": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": "INF" if math.isinf(p) else round(p, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "avg_net_return_pct": round(sum(vals) / len(vals), 6) if vals else 0.0,
        "best_trade_pct": round(max(vals), 6) if vals else 0.0,
        "worst_trade_pct": round(min(vals), 6) if vals else 0.0,
        "max_drawdown_trade_points_pct": round(max_drawdown(vals), 4),
        "avg_hold_hours": round(
            sum(float(t["hold_hours"]) for t in trades) / len(trades), 4
        ) if trades else 0.0,
        "open_left": open_left,
        "cash_entries": direction_entries.get("CASH", 0),
        "reverse_entries": direction_entries.get("REVERSE", 0),
        "cash_closed": len(by_dir["CASH"]),
        "reverse_closed": len(by_dir["REVERSE"]),
        "cash_sum_pct": round(sum(by_dir["CASH"]), 4),
        "reverse_sum_pct": round(sum(by_dir["REVERSE"]), 4),
        "BTC_sum_pct": round(sum(by_sym["BTC-USDT"]), 4),
        "ETH_sum_pct": round(sum(by_sym["ETH-USDT"]), 4),
        "SOL_sum_pct": round(sum(by_sym["SOL-USDT"]), 4),
        "cash_BTC_entries": symbol_entries.get(("CASH", "BTC-USDT"), 0),
        "cash_ETH_entries": symbol_entries.get(("CASH", "ETH-USDT"), 0),
        "cash_SOL_entries": symbol_entries.get(("CASH", "SOL-USDT"), 0),
        "reverse_BTC_entries": symbol_entries.get(("REVERSE", "BTC-USDT"), 0),
        "reverse_ETH_entries": symbol_entries.get(("REVERSE", "ETH-USDT"), 0),
        "reverse_SOL_entries": symbol_entries.get(("REVERSE", "SOL-USDT"), 0),
        "reject_MAX_OPEN": rejects.get("MAX_OPEN", 0),
        "reject_BOTH_EDGE_TOO_SMALL": rejects.get("BOTH_EDGE_TOO_SMALL", 0),
        "reject_CASH_EDGE_TOO_SMALL": rejects.get("CASH_EDGE_TOO_SMALL", 0),
        "reject_REVERSE_EDGE_TOO_SMALL": rejects.get("REVERSE_EDGE_TOO_SMALL", 0),
        "close_BASIS_CONVERGED": closes.get("BASIS_CONVERGED", 0),
        "close_BASIS_TAKE_PROFIT": closes.get("BASIS_TAKE_PROFIT", 0),
        "close_BASIS_ADVERSE_STOP": closes.get("BASIS_ADVERSE_STOP", 0),
        "close_FUNDING_TURNED_ADVERSE": closes.get("FUNDING_TURNED_ADVERSE", 0),
        "close_MAX_HOLD": closes.get("MAX_HOLD", 0),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--snapshots",
        default="data/bf1_v4_proxy/bf1_v4_static_snapshots.csv",
    )
    ap.add_argument(
        "--out-dir",
        default=DEFAULT_OUT,
    )
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(args.snapshots, newline="", encoding="utf-8-sig") as f:
        raw_rows = list(csv.DictReader(f))

    if not raw_rows:
        raise RuntimeError("BF7 no snapshots")

    vs = build_variants()
    all_trades = []
    summaries = []

    print(
        f"BF7_START snapshots={len(raw_rows)} variants={len(vs)} "
        f"borrow_apr={BORROW_APR:.4f} max_hold={MAX_HOURS} historical_only=true",
        flush=True,
    )

    for i, v in enumerate(vs, 1):
        trades, rejects, closes, direction_entries, symbol_entries, open_left = run_variant(
            raw_rows, v
        )

        s = make_summary(
            v,
            trades,
            rejects,
            closes,
            direction_entries,
            symbol_entries,
            open_left,
        )

        summaries.append(s)
        all_trades.extend(trades)

        print(
            "BF7_RESULT " + json.dumps(
                {
                    "i": i,
                    "n": len(vs),
                    "variant": v.name,
                    "closed": s["closed"],
                    "wins": s["wins"],
                    "losses": s["losses"],
                    "win_rate_pct": s["win_rate_pct"],
                    "profit_factor": s["profit_factor"],
                    "sum_net_return_pct": s["sum_net_return_pct"],
                    "max_dd": s["max_drawdown_trade_points_pct"],
                    "cash_closed": s["cash_closed"],
                    "reverse_closed": s["reverse_closed"],
                    "cash_sum_pct": s["cash_sum_pct"],
                    "reverse_sum_pct": s["reverse_sum_pct"],
                    "rejects": dict(rejects),
                    "closes": dict(closes),
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    def rank_key(x):
        n = int(x["closed"])
        deficit = max(0, 200 - n)
        p = 999.0 if x["profit_factor"] == "INF" else float(x["profit_factor"])

        return (
            deficit,
            -p,
            -float(x["sum_net_return_pct"]),
            float(x["max_drawdown_trade_points_pct"]),
            -n,
        )

    ranked = sorted(summaries, key=rank_key)

    for rank, row in enumerate(ranked, 1):
        row["rank_sample_aware"] = rank

    summary_csv = out_dir / "bf1_v7_summary.csv"
    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(ranked[0].keys()))
        w.writeheader()
        w.writerows(ranked)

    trades_csv = out_dir / "bf1_v7_trades.csv"
    if all_trades:
        with trades_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_trades[0].keys()))
            w.writeheader()
            w.writerows(all_trades)
    else:
        trades_csv.write_text("", encoding="utf-8")

    json_path = out_dir / "bf1_v7_summary.json"
    json_path.write_text(
        json.dumps(
            {
                "source": args.snapshots,
                "snapshot_rows": len(raw_rows),
                "borrow_apr": BORROW_APR,
                "notes": [
                    "Reverse carry requires real spot borrow/margin capability; this historical test does not prove executability.",
                    "Entry funding projection repeats the current funding rate only; it does not use future rates.",
                    "ZEROEX removes exchange fee/slippage only as a diagnostic and still charges reverse spot borrow cost.",
                    "This is Binance Data Vision proxy research, not BingX execution performance.",
                ],
                "cost_models": {k: asdict(v) for k, v in COST_MODELS.items()},
                "variants": [asdict(v) for v in vs],
                "ranked_results": ranked,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        f"BF7_DONE summary={summary_csv} trades={trades_csv}",
        flush=True,
    )


if __name__ == "__main__":
    main()
