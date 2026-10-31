"""
BF1 V8 Cost Break-Even Sweep
(replaces bf1_historical_v5_ab.py so existing V4 -> handoff runner executes it)

Purpose
-------
V7 showed:
- zero exchange-cost two-way 24h / E0.05 can be positive
- low-cost execution turns the same structural idea negative

V8 therefore answers one practical question:
"What maximum round-trip exchange cost can BF1 tolerate before PF falls below 1?"

This test:
- uses CASH + REVERSE carry
- uses 24h projected current funding (3 funding events)
- sweeps total round-trip exchange cost in basis points
- tests entry edge thresholds 0.05% and 0.10%
- keeps reverse spot borrow at a configurable APR (default 10%)
- uses actual historical funding accrual for realized PnL
- does NOT use future funding for entry

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

NOTIONAL = float(os.getenv("BF8_NOTIONAL", "100"))
MAX_OPEN = int(os.getenv("BF8_MAX_OPEN", "3"))
MAX_HOURS = float(os.getenv("BF8_MAX_HOLD_HOURS", "48"))

CLOSE_BASIS = float(os.getenv("BF8_CLOSE_BASIS_PCT", "0.05"))
BASIS_ADVERSE_STOP = float(os.getenv("BF8_BASIS_ADVERSE_STOP_PCT", "0.25"))
BASIS_TAKE_PROFIT = float(os.getenv("BF8_BASIS_TAKE_PROFIT_PCT", "0.15"))
FUNDING_ADVERSE_ABS = float(os.getenv("BF8_FUNDING_ADVERSE_ABS", "0.0002"))

BORROW_APR = float(os.getenv("BF8_SPOT_BORROW_APR", "0.10"))
FUNDING_EVENTS_PROJECTION = int(os.getenv("BF8_FUNDING_EVENTS_PROJECTION", "3"))
PROJECTED_HOLD_HOURS = float(os.getenv("BF8_PROJECTED_HOLD_HOURS", "24"))

COST_BPS = [
    float(x.strip())
    for x in os.getenv(
        "BF8_ROUNDTRIP_COST_BPS",
        "0,2,4,6,8,10,12,14,16,18,20,24,28,34"
    ).split(",")
    if x.strip()
]

EDGE_THRESHOLDS = [
    float(x.strip())
    for x in os.getenv("BF8_EDGE_THRESHOLDS", "0.05,0.10").split(",")
    if x.strip()
]

DEFAULT_OUT = "data/bf1_v5_ab"


@dataclass(frozen=True)
class Variant:
    name: str
    roundtrip_cost_bps: float
    min_edge_pct: float


def dt(s: str) -> datetime:
    x = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def variants() -> list[Variant]:
    out = []
    for bps in COST_BPS:
        for edge in EDGE_THRESHOLDS:
            out.append(
                Variant(
                    name=f"COST_{bps:.1f}BPS_E{edge:.2f}",
                    roundtrip_cost_bps=bps,
                    min_edge_pct=edge,
                )
            )
    return out


def base_snapshot(raw: dict) -> dict:
    return {
        "t": dt(raw["timestamp"]),
        "symbol": raw["symbol"].upper(),
        "spot_bid": float(raw["spot_bid"]),
        "spot_ask": float(raw["spot_ask"]),
        "perp_bid": float(raw["perp_bid"]),
        "perp_ask": float(raw["perp_ask"]),
        "funding_rate": float(raw.get("funding_rate") or 0.0),
        "next_funding_time_ms": int(float(raw.get("next_funding_time_ms") or 0)),
    }


def projected_borrow_pct(hours: float) -> float:
    return BORROW_APR * (hours / (365.0 * 24.0)) * 100.0


def metrics(s: dict, v: Variant):
    cost_pct = v.roundtrip_cost_bps / 100.0  # bps -> percentage points

    cash_basis = (s["perp_bid"] / s["spot_ask"] - 1.0) * 100.0
    cash_funding = s["funding_rate"] * 100.0 * FUNDING_EVENTS_PROJECTION
    cash_edge = cash_basis - CLOSE_BASIS + cash_funding - cost_pct

    reverse_basis = (s["spot_bid"] / s["perp_ask"] - 1.0) * 100.0
    reverse_funding = (-s["funding_rate"]) * 100.0 * FUNDING_EVENTS_PROJECTION
    reverse_borrow = projected_borrow_pct(PROJECTED_HOLD_HOURS)
    reverse_edge = (
        reverse_basis
        - CLOSE_BASIS
        + reverse_funding
        - cost_pct
        - reverse_borrow
    )

    return [
        {
            "direction": "CASH",
            "entry_basis_pct": cash_basis,
            "projected_funding_pct": cash_funding,
            "projected_borrow_pct": 0.0,
            "expected_edge_pct": cash_edge,
        },
        {
            "direction": "REVERSE",
            "entry_basis_pct": reverse_basis,
            "projected_funding_pct": reverse_funding,
            "projected_borrow_pct": reverse_borrow,
            "expected_edge_pct": reverse_edge,
        },
    ]


def choose_entry(s: dict, v: Variant):
    candidates = metrics(s, v)
    best = max(candidates, key=lambda x: x["expected_edge_pct"])

    if best["expected_edge_pct"] < v.min_edge_pct:
        return None, "BOTH_EDGE_TOO_SMALL"

    return best, "OPEN"


def current_close_basis(s: dict, direction: str) -> float:
    if direction == "CASH":
        return (s["perp_ask"] / s["spot_bid"] - 1.0) * 100.0

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

    now_ms = int(s["t"].timestamp() * 1000)
    if now_ms < nxt or last >= nxt:
        return

    perp_mid = (s["perp_bid"] + s["perp_ask"]) / 2.0
    base = pos["qty"] * perp_mid * s["funding_rate"]

    funding = base if pos["direction"] == "CASH" else -base

    pos["funding"] += funding
    pos["funding_events"] += 1
    pos["last_funding"] = nxt

    new_next = int(s["next_funding_time_ms"] or 0)
    if new_next > nxt:
        pos["next_funding"] = new_next


def close_pnl(pos: dict, s: dict, v: Variant):
    q = pos["qty"]

    if pos["direction"] == "CASH":
        gross = (
            q * (s["spot_bid"] - pos["entry_spot"])
            + q * (pos["entry_perp"] - s["perp_ask"])
            + pos["funding"]
        )
        borrow = 0.0
    else:
        gross = (
            q * (pos["entry_spot"] - s["spot_ask"])
            + q * (s["perp_bid"] - pos["entry_perp"])
            + pos["funding"]
        )
        hold_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0
        borrow = (
            q * pos["entry_spot"] * BORROW_APR * hold_h / (365.0 * 24.0)
        )

    capital = q * pos["entry_spot"]

    # V8 uses a direct total round-trip cost diagnostic.
    exchange_cost = capital * (v.roundtrip_cost_bps / 10000.0)

    net = gross - borrow - exchange_cost
    ret = net / capital * 100.0 if capital > 0 else 0.0

    return gross, borrow, exchange_cost, net, ret


def run_variant(raw_rows: list[dict], v: Variant):
    rows = [base_snapshot(r) for r in raw_rows]
    rows.sort(key=lambda x: (x["t"], x["symbol"]))

    opens = {}
    trades = []
    rejects = Counter()
    closes = Counter()
    direction_entries = Counter()

    for s in rows:
        sym = s["symbol"]
        pos = opens.get(sym)

        if pos:
            accrue_funding(pos, s)
            reason, exit_basis = should_close(pos, s)

            if reason:
                gross, borrow, exchange_cost, net, ret = close_pnl(pos, s, v)
                hold_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0

                trades.append({
                    "variant": v.name,
                    "roundtrip_cost_bps": v.roundtrip_cost_bps,
                    "min_edge_pct": v.min_edge_pct,
                    "direction": pos["direction"],
                    "symbol": sym,
                    "opened_at": pos["opened_at"].isoformat(),
                    "closed_at": s["t"].isoformat(),
                    "hold_hours": round(hold_h, 4),
                    "entry_basis_pct": round(pos["entry_basis_pct"], 6),
                    "entry_funding_rate": round(pos["entry_funding_rate"], 10),
                    "entry_projected_funding_pct": round(
                        pos["entry_projected_funding_pct"], 6
                    ),
                    "entry_projected_borrow_pct": round(
                        pos["entry_projected_borrow_pct"], 6
                    ),
                    "entry_expected_edge_pct": round(
                        pos["entry_expected_edge_pct"], 6
                    ),
                    "exit_basis_pct": round(exit_basis, 6),
                    "funding_events": pos["funding_events"],
                    "funding_usdt": round(pos["funding"], 8),
                    "borrow_usdt": round(borrow, 8),
                    "exchange_cost_usdt": round(exchange_cost, 8),
                    "gross_pnl_usdt": round(gross, 8),
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

            choice, reason = choose_entry(s, v)
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
                "entry_expected_edge_pct": choice["expected_edge_pct"],
                "funding": 0.0,
                "funding_events": 0,
                "last_funding": 0,
                "next_funding": int(s["next_funding_time_ms"] or 0),
            }

            direction_entries[direction] += 1

    return trades, rejects, closes, direction_entries, len(opens)


def pf(vals):
    gp = sum(x for x in vals if x > 0)
    gl = abs(sum(x for x in vals if x < 0))

    if gl > 0:
        return gp / gl
    if gp > 0:
        return math.inf
    return 0.0


def max_dd(vals):
    eq = peak = worst = 0.0
    for x in vals:
        eq += x
        peak = max(peak, eq)
        worst = min(worst, eq - peak)
    return abs(worst)


def make_summary(v, trades, rejects, closes, direction_entries, open_left):
    vals = [float(t["net_return_pct"]) for t in trades]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    p = pf(vals)

    by_dir = defaultdict(list)
    for t in trades:
        by_dir[t["direction"]].append(float(t["net_return_pct"]))

    return {
        "variant": v.name,
        "roundtrip_cost_bps": v.roundtrip_cost_bps,
        "min_edge_pct": v.min_edge_pct,
        "closed": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": "INF" if math.isinf(p) else round(p, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "avg_net_return_pct": round(sum(vals) / len(vals), 6) if vals else 0.0,
        "max_drawdown_trade_points_pct": round(max_dd(vals), 4),
        "cash_closed": len(by_dir["CASH"]),
        "reverse_closed": len(by_dir["REVERSE"]),
        "cash_sum_pct": round(sum(by_dir["CASH"]), 4),
        "reverse_sum_pct": round(sum(by_dir["REVERSE"]), 4),
        "cash_entries": direction_entries.get("CASH", 0),
        "reverse_entries": direction_entries.get("REVERSE", 0),
        "open_left": open_left,
        "reject_BOTH_EDGE_TOO_SMALL": rejects.get("BOTH_EDGE_TOO_SMALL", 0),
        "reject_MAX_OPEN": rejects.get("MAX_OPEN", 0),
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
        raise RuntimeError("BF8 no snapshots")

    vs = variants()
    all_trades = []
    summaries = []

    print(
        f"BF8_START snapshots={len(raw_rows)} variants={len(vs)} "
        f"borrow_apr={BORROW_APR:.4f} projection_events={FUNDING_EVENTS_PROJECTION} "
        f"historical_only=true",
        flush=True,
    )

    for i, v in enumerate(vs, 1):
        trades, rejects, closes, direction_entries, open_left = run_variant(
            raw_rows, v
        )

        s = make_summary(
            v, trades, rejects, closes, direction_entries, open_left
        )

        summaries.append(s)
        all_trades.extend(trades)

        print(
            "BF8_RESULT " + json.dumps(
                {
                    "i": i,
                    "n": len(vs),
                    "cost_bps": v.roundtrip_cost_bps,
                    "edge": v.min_edge_pct,
                    "closed": s["closed"],
                    "wins": s["wins"],
                    "losses": s["losses"],
                    "win_rate_pct": s["win_rate_pct"],
                    "profit_factor": s["profit_factor"],
                    "sum_net_return_pct": s["sum_net_return_pct"],
                    "max_dd": s["max_drawdown_trade_points_pct"],
                    "cash_closed": s["cash_closed"],
                    "reverse_closed": s["reverse_closed"],
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    # Determine practical break-even envelope.
    viable = []
    for s in summaries:
        p = 999.0 if s["profit_factor"] == "INF" else float(s["profit_factor"])
        if p > 1.0 and float(s["sum_net_return_pct"]) > 0:
            viable.append(s)

    max_viable_cost = {}
    for edge in EDGE_THRESHOLDS:
        rows = [
            s for s in viable
            if abs(float(s["min_edge_pct"]) - edge) < 1e-12
        ]
        max_viable_cost[f"{edge:.2f}"] = (
            max((float(s["roundtrip_cost_bps"]) for s in rows), default=None)
        )

    # Sample-aware rank.
    def rank_key(s):
        n = int(s["closed"])
        deficit = max(0, 200 - n)
        p = 999.0 if s["profit_factor"] == "INF" else float(s["profit_factor"])
        return (
            deficit,
            -p,
            -float(s["sum_net_return_pct"]),
            float(s["max_drawdown_trade_points_pct"]),
        )

    ranked = sorted(summaries, key=rank_key)
    for rank, row in enumerate(ranked, 1):
        row["rank_sample_aware"] = rank

    summary_csv = out_dir / "bf1_v8_cost_sweep.csv"
    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(ranked[0].keys()))
        w.writeheader()
        w.writerows(ranked)

    trades_csv = out_dir / "bf1_v8_trades.csv"
    if all_trades:
        with trades_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_trades[0].keys()))
            w.writeheader()
            w.writerows(all_trades)
    else:
        trades_csv.write_text("", encoding="utf-8")

    report = {
        "source": args.snapshots,
        "snapshot_rows": len(raw_rows),
        "borrow_apr": BORROW_APR,
        "projected_hold_hours": PROJECTED_HOLD_HOURS,
        "funding_projection_events": FUNDING_EVENTS_PROJECTION,
        "cost_bps_sweep": COST_BPS,
        "edge_thresholds": EDGE_THRESHOLDS,
        "max_viable_roundtrip_cost_bps_by_edge": max_viable_cost,
        "notes": [
            "Round-trip cost is modeled directly in bps of spot capital.",
            "Reverse carry includes realized spot borrow cost at configured APR.",
            "Entry uses current funding projected for 24h; no future funding lookahead.",
            "A cost level is called viable only when PF > 1 and summed trade return > 0.",
            "This is Binance Data Vision proxy research, not BingX venue-fidelity performance.",
        ],
        "variants": [asdict(v) for v in vs],
        "ranked_results": ranked,
    }

    json_path = out_dir / "bf1_v8_cost_sweep.json"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(
        "BF8_BREAK_EVEN "
        + json.dumps(max_viable_cost, separators=(",", ":")),
        flush=True,
    )
    print(
        f"BF8_DONE summary={summary_csv} trades={trades_csv}",
        flush=True,
    )


if __name__ == "__main__":
    main()
