"""
BF1 V6 Structural Historical Test
(replaces bf1_historical_v5_ab.py so the existing V4->V5 runner can execute it)

Goal
----
Test the structural bottleneck found in V5:
- Current BF1 rejects most rows because open_basis must be > 0
- Current BF1 also hard-requires funding >= 0
- MIN_EDGE / lower-cost changes did NOT materially increase trade count

V6 tests:
1) BASELINE: current BF1 behavior.
2) FLEX_POS_FUND:
   - basis may be negative down to BASIS_FLOOR
   - funding must still be >= 0
   - entry is based on projected total economics
3) FLEX_ANY_FUND:
   - basis may be negative down to BASIS_FLOOR
   - funding sign is not hard-gated
   - projected total economics must still pass MIN_EDGE

Funding projection:
- 24h = current funding rate * 3 events
- 48h = current funding rate * 6 events
No future funding lookahead is used for ENTRY.

V6 exit logic for FLEX variants:
- positive-basis entries can close when basis converges
- close if funding turns materially adverse
- close on max hold
- close on adverse basis widening stop
- take profit if basis improves enough versus entry

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

NOTIONAL = float(os.getenv("BF6_NOTIONAL", "100"))
MAX_OPEN = int(os.getenv("BF6_MAX_OPEN", "3"))

SPOT_FEE = float(os.getenv("BF6_SPOT_FEE", "0.0010"))
PERP_FEE = float(os.getenv("BF6_PERP_FEE", "0.0005"))
SLIP = float(os.getenv("BF6_SLIP_PER_LEG", "0.0001"))

CLOSE_BASIS = float(os.getenv("BF6_CLOSE_BASIS_PCT", "0.05"))
FUNDING_EXIT = float(os.getenv("BF6_FUNDING_EXIT_RATE", "-0.0002"))
MAX_HOURS = float(os.getenv("BF6_MAX_HOLD_HOURS", "48"))

# Structural risk guards for flexible carry entries.
BASIS_FLOOR = float(os.getenv("BF6_BASIS_FLOOR_PCT", "-0.30"))
BASIS_ADVERSE_STOP = float(os.getenv("BF6_BASIS_ADVERSE_STOP_PCT", "0.25"))
BASIS_TAKE_PROFIT = float(os.getenv("BF6_BASIS_TAKE_PROFIT_PCT", "0.15"))

EDGE_THRESHOLDS = [
    float(x.strip())
    for x in os.getenv("BF6_EDGE_THRESHOLDS", "0.00,0.05,0.10").split(",")
    if x.strip()
]

DEFAULT_OUT = "data/bf1_v5_ab"


@dataclass(frozen=True)
class Variant:
    name: str
    mode: str
    funding_projection_events: int
    min_edge_pct: float
    require_positive_funding: bool
    require_positive_basis: bool
    flexible_exit: bool


def parse_dt(s: str) -> datetime:
    x = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def variants() -> list[Variant]:
    out = [
        Variant(
            name="BASELINE_CURRENT",
            mode="BASELINE",
            funding_projection_events=1,
            min_edge_pct=0.10,
            require_positive_funding=True,
            require_positive_basis=True,
            flexible_exit=False,
        )
    ]

    for events, horizon in [(3, "24H"), (6, "48H")]:
        for edge in EDGE_THRESHOLDS:
            out.append(
                Variant(
                    name=f"FLEX_POS_FUND_{horizon}_E{edge:.2f}",
                    mode="FLEX_POS_FUND",
                    funding_projection_events=events,
                    min_edge_pct=edge,
                    require_positive_funding=True,
                    require_positive_basis=False,
                    flexible_exit=True,
                )
            )
            out.append(
                Variant(
                    name=f"FLEX_ANY_FUND_{horizon}_E{edge:.2f}",
                    mode="FLEX_ANY_FUND",
                    funding_projection_events=events,
                    min_edge_pct=edge,
                    require_positive_funding=False,
                    require_positive_basis=False,
                    flexible_exit=True,
                )
            )
    return out


def snap(raw: dict, v: Variant) -> dict:
    sb = float(raw["spot_bid"])
    sa = float(raw["spot_ask"])
    pb = float(raw["perp_bid"])
    pa = float(raw["perp_ask"])
    fr = float(raw.get("funding_rate") or 0.0)

    open_basis = (pb / sa - 1.0) * 100.0
    close_basis = (pa / sb - 1.0) * 100.0

    fee_pct = 2.0 * (SPOT_FEE + PERP_FEE) * 100.0
    slip_pct = 4.0 * SLIP * 100.0
    projected_funding_pct = fr * 100.0 * v.funding_projection_events

    expected_edge = (
        open_basis
        - CLOSE_BASIS
        + projected_funding_pct
        - fee_pct
        - slip_pct
    )

    return {
        "t": parse_dt(raw["timestamp"]),
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
        "expected_net_edge_pct": expected_edge,
    }


def should_open(s: dict, n_open: int, v: Variant):
    if n_open >= MAX_OPEN:
        return False, "MAX_OPEN"

    if v.require_positive_funding and s["funding_rate"] < 0:
        return False, "FUNDING_BELOW_ZERO"

    if v.require_positive_basis and s["open_basis_pct"] <= 0:
        return False, "NO_POSITIVE_PREMIUM"

    if not v.require_positive_basis and s["open_basis_pct"] < BASIS_FLOOR:
        return False, "BASIS_BELOW_FLOOR"

    if s["expected_net_edge_pct"] < v.min_edge_pct:
        return False, "NET_EDGE_TOO_SMALL"

    return True, "OPEN"


def should_close(pos: dict, s: dict, v: Variant):
    age_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0

    if not v.flexible_exit:
        if s["close_basis_pct"] <= CLOSE_BASIS:
            return "BASIS_CONVERGED"
        if s["funding_rate"] <= FUNDING_EXIT:
            return "FUNDING_TURNED_ADVERSE"
        if age_h >= MAX_HOURS:
            return "MAX_HOLD"
        return None

    # FLEX logic:
    entry_basis = pos["entry_open_basis_pct"]
    basis_change_favorable = entry_basis - s["close_basis_pct"]
    basis_change_adverse = s["close_basis_pct"] - entry_basis

    # If this was a classic positive-basis cash-and-carry entry, preserve
    # the convergence exit.
    if entry_basis > CLOSE_BASIS and s["close_basis_pct"] <= CLOSE_BASIS:
        return "BASIS_CONVERGED"

    # For negative/low basis carry entries, convergence upward can hurt.
    # Exit if basis widens upward too far versus entry.
    if basis_change_adverse >= BASIS_ADVERSE_STOP:
        return "BASIS_ADVERSE_STOP"

    # If basis improves materially in our favor, crystallize the spread gain.
    if basis_change_favorable >= BASIS_TAKE_PROFIT:
        return "BASIS_TAKE_PROFIT"

    if s["funding_rate"] <= FUNDING_EXIT:
        return "FUNDING_TURNED_ADVERSE"

    if age_h >= MAX_HOURS:
        return "MAX_HOLD"

    return None


def close_calc(pos: dict, s: dict):
    q = pos["qty"]

    gross = (
        q * (s["spot_bid"] - pos["entry_spot"])
        + q * (pos["entry_perp"] - s["perp_ask"])
        + pos["funding"]
    )

    fee = q * (
        pos["entry_spot"] * SPOT_FEE
        + s["spot_bid"] * SPOT_FEE
        + pos["entry_perp"] * PERP_FEE
        + s["perp_ask"] * PERP_FEE
    )

    slip = q * (
        pos["entry_spot"] + s["spot_bid"]
        + pos["entry_perp"] + s["perp_ask"]
    ) * SLIP

    net = gross - fee - slip
    capital = q * pos["entry_spot"]
    ret = net / capital * 100.0 if capital > 0 else 0.0
    return gross, fee, slip, net, ret


def run_one(raw_rows: list[dict], v: Variant):
    rows = [snap(r, v) for r in raw_rows]
    rows.sort(key=lambda x: (x["t"], x["symbol"]))

    opens = {}
    trades = []
    rejects = Counter()
    closes = Counter()
    entry_symbols = Counter()

    for s in rows:
        sym = s["symbol"]
        pos = opens.get(sym)

        if pos:
            nxt = int(pos.get("next_funding") or 0)
            last = int(pos.get("last_funding") or 0)

            if nxt and int(s["t"].timestamp() * 1000) >= nxt and last < nxt:
                perp_mid = (s["perp_bid"] + s["perp_ask"]) / 2.0
                pos["funding"] += pos["qty"] * perp_mid * s["funding_rate"]
                pos["funding_events"] += 1
                pos["last_funding"] = nxt

                new_next = int(s["next_funding_time_ms"] or 0)
                if new_next > nxt:
                    pos["next_funding"] = new_next

            reason = should_close(pos, s, v)
            if reason:
                gross, fee, slip, net, ret = close_calc(pos, s)
                hold_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0

                trades.append({
                    "variant": v.name,
                    "mode": v.mode,
                    "symbol": sym,
                    "opened_at": pos["opened_at"].isoformat(),
                    "closed_at": s["t"].isoformat(),
                    "hold_hours": round(hold_h, 4),
                    "entry_open_basis_pct": round(pos["entry_open_basis_pct"], 6),
                    "entry_funding_rate": round(pos["entry_funding_rate"], 10),
                    "entry_projected_funding_pct": round(pos["entry_projected_funding_pct"], 6),
                    "entry_expected_net_edge_pct": round(pos["entry_expected_net_edge_pct"], 6),
                    "exit_close_basis_pct": round(s["close_basis_pct"], 6),
                    "funding_events": pos["funding_events"],
                    "funding_usdt": round(pos["funding"], 8),
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
            ok, reason = should_open(s, len(opens), v)
            if not ok:
                rejects[reason] += 1
            else:
                qty = NOTIONAL / s["spot_ask"]
                opens[sym] = {
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
                entry_symbols[sym] += 1

    return trades, rejects, closes, entry_symbols, len(opens)


def pf(vals):
    gp = sum(x for x in vals if x > 0)
    gl = abs(sum(x for x in vals if x < 0))
    if gl > 0:
        return gp / gl
    if gp > 0:
        return math.inf
    return 0.0


def drawdown(vals):
    eq = peak = worst = 0.0
    for x in vals:
        eq += x
        peak = max(peak, eq)
        worst = min(worst, eq - peak)
    return abs(worst)


def summary(v, trades, rejects, closes, entries, open_left):
    vals = [float(x["net_return_pct"]) for x in trades]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    p = pf(vals)

    sym = defaultdict(list)
    for t in trades:
        sym[t["symbol"]].append(float(t["net_return_pct"]))

    return {
        "variant": v.name,
        "mode": v.mode,
        "funding_projection_events": v.funding_projection_events,
        "min_edge_pct": v.min_edge_pct,
        "require_positive_funding": v.require_positive_funding,
        "require_positive_basis": v.require_positive_basis,
        "closed": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) * 100 / len(vals), 2) if vals else 0.0,
        "profit_factor": "INF" if math.isinf(p) else round(p, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "avg_net_return_pct": round(sum(vals) / len(vals), 6) if vals else 0.0,
        "worst_trade_pct": round(min(vals), 6) if vals else 0.0,
        "best_trade_pct": round(max(vals), 6) if vals else 0.0,
        "max_drawdown_trade_points_pct": round(drawdown(vals), 4),
        "avg_hold_hours": round(
            sum(float(x["hold_hours"]) for x in trades) / len(trades), 4
        ) if trades else 0.0,
        "open_left": open_left,
        "entries_BTC": entries.get("BTC-USDT", 0),
        "entries_ETH": entries.get("ETH-USDT", 0),
        "entries_SOL": entries.get("SOL-USDT", 0),
        "BTC_sum_pct": round(sum(sym["BTC-USDT"]), 4),
        "ETH_sum_pct": round(sum(sym["ETH-USDT"]), 4),
        "SOL_sum_pct": round(sum(sym["SOL-USDT"]), 4),
        "reject_MAX_OPEN": rejects.get("MAX_OPEN", 0),
        "reject_FUNDING_BELOW_ZERO": rejects.get("FUNDING_BELOW_ZERO", 0),
        "reject_NO_POSITIVE_PREMIUM": rejects.get("NO_POSITIVE_PREMIUM", 0),
        "reject_BASIS_BELOW_FLOOR": rejects.get("BASIS_BELOW_FLOOR", 0),
        "reject_NET_EDGE_TOO_SMALL": rejects.get("NET_EDGE_TOO_SMALL", 0),
        "close_BASIS_CONVERGED": closes.get("BASIS_CONVERGED", 0),
        "close_BASIS_ADVERSE_STOP": closes.get("BASIS_ADVERSE_STOP", 0),
        "close_BASIS_TAKE_PROFIT": closes.get("BASIS_TAKE_PROFIT", 0),
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
        raise RuntimeError("BF6 no snapshots")

    vs = variants()
    all_trades = []
    summaries = []

    print(
        f"BF6_START snapshots={len(raw_rows)} variants={len(vs)} "
        f"basis_floor={BASIS_FLOOR} adverse_stop={BASIS_ADVERSE_STOP} "
        f"take_profit={BASIS_TAKE_PROFIT} historical_only=true",
        flush=True,
    )

    for i, v in enumerate(vs, 1):
        trades, rejects, closes, entries, open_left = run_one(raw_rows, v)
        s = summary(v, trades, rejects, closes, entries, open_left)
        all_trades.extend(trades)
        summaries.append(s)

        print(
            "BF6_RESULT " + json.dumps(
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
                    "rejects": dict(rejects),
                    "closes": dict(closes),
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    # Sample-aware ranking: prefer >=200 closed trades, then PF, then return.
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

    summary_csv = out_dir / "bf1_v6_summary.csv"
    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(ranked[0].keys()))
        w.writeheader()
        w.writerows(ranked)

    trades_csv = out_dir / "bf1_v6_trades.csv"
    if all_trades:
        with trades_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_trades[0].keys()))
            w.writeheader()
            w.writerows(all_trades)
    else:
        trades_csv.write_text("", encoding="utf-8")

    json_path = out_dir / "bf1_v6_summary.json"
    json_path.write_text(
        json.dumps(
            {
                "source": args.snapshots,
                "snapshot_rows": len(raw_rows),
                "basis_floor_pct": BASIS_FLOOR,
                "basis_adverse_stop_pct": BASIS_ADVERSE_STOP,
                "basis_take_profit_pct": BASIS_TAKE_PROFIT,
                "notes": [
                    "ENTRY funding projection uses current rate only; no future funding lookahead.",
                    "Realized PnL accrues historical funding events from snapshots.",
                    "FLEX modes remove the hard positive-basis requirement.",
                    "FLEX_ANY_FUND also removes the hard positive-funding sign requirement.",
                    "This remains a Binance proxy research test, not BingX execution performance.",
                ],
                "variants": [asdict(v) for v in vs],
                "ranked_results": ranked,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    print(
        f"BF6_DONE summary={summary_csv} trades={trades_csv}",
        flush=True,
    )


if __name__ == "__main__":
    main()
