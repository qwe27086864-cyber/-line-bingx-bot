"""
BF1 V9 Maker Execution Feasibility Test
(replaces bf1_historical_v5_ab.py so the existing V4 -> handoff runner executes it)

Why V9
------
V8 found BF1's approximate break-even total round-trip cost near 8 bps.
V9 tests whether passive execution can realistically stay below that level.

Execution model
---------------
Entry signal:
- two-way carry (CASH and REVERSE)
- 24h projected CURRENT funding (3 funding events)
- entry threshold 0.05%

Passive entry:
- CASH: buy spot at bid + short perp at ask
- REVERSE: short spot at ask + long perp at bid
- maker fill is only credited when a later BBO snapshot crosses the resting limit
- wait windows: 15 / 30 / 60 minutes
- if at least one maker leg fills but the pair is incomplete at timeout,
  remaining quantity is completed with taker hedge
- if neither leg fills by timeout, the signal is cancelled (no trade)
- partial-fill stress: 100% or 50% of target quantity gets maker price when crossed;
  remainder is completed as taker when the pair resolves
- adverse-selection stress is charged as extra bps on maker-filled quantity

Exit:
- basis convergence / take-profit exits use a conservative maker-attempt proxy:
  one 15m step to cross; otherwise taker fallback
- adverse-stop / funding-adverse / max-hold exits use immediate taker execution

Cost profiles
-------------
TARGET_2 / TARGET_4 / TARGET_6 / TARGET_8:
- target total maker-only round-trip exchange cost of 2/4/6/8 bps
- maker cost is split across four legs
- taker fallback adds configurable extra bps per taker leg

REGULAR_PROXY:
- research proxy for regular-user execution:
  spot maker/taker defaults 10/10 bps
  perp maker/taker defaults 2/5 bps
- all values can be overridden by environment variables

Important
---------
The snapshot file has no order-book queue position, trade prints, or volume.
Therefore this is an execution PROXY, not proof of fill probability.
A cross of the resting price is treated as evidence that a passive order
could have filled.

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

NOTIONAL = float(os.getenv("BF9_NOTIONAL", "100"))
MAX_OPEN = int(os.getenv("BF9_MAX_OPEN", "3"))
MAX_HOURS = float(os.getenv("BF9_MAX_HOLD_HOURS", "48"))

CLOSE_BASIS = float(os.getenv("BF9_CLOSE_BASIS_PCT", "0.05"))
BASIS_ADVERSE_STOP = float(os.getenv("BF9_BASIS_ADVERSE_STOP_PCT", "0.25"))
BASIS_TAKE_PROFIT = float(os.getenv("BF9_BASIS_TAKE_PROFIT_PCT", "0.15"))
FUNDING_ADVERSE_ABS = float(os.getenv("BF9_FUNDING_ADVERSE_ABS", "0.0002"))

BORROW_APR = float(os.getenv("BF9_SPOT_BORROW_APR", "0.10"))
FUNDING_EVENTS_PROJECTION = int(os.getenv("BF9_FUNDING_EVENTS_PROJECTION", "3"))
PROJECTED_HOLD_HOURS = float(os.getenv("BF9_PROJECTED_HOLD_HOURS", "24"))
ENTRY_EDGE_PCT = float(os.getenv("BF9_ENTRY_EDGE_PCT", "0.05"))

TARGET_COSTS_BPS = [
    float(x.strip())
    for x in os.getenv("BF9_TARGET_MAKER_ROUNDTRIP_BPS", "2,4,6,8").split(",")
    if x.strip()
]
WAIT_MINUTES = [
    int(x.strip())
    for x in os.getenv("BF9_WAIT_MINUTES", "15,30,60").split(",")
    if x.strip()
]
PARTIAL_FILL_RATIOS = [
    float(x.strip())
    for x in os.getenv("BF9_PARTIAL_FILL_RATIOS", "1.0,0.5").split(",")
    if x.strip()
]

TAKER_FALLBACK_EXTRA_BPS = float(os.getenv("BF9_TAKER_FALLBACK_EXTRA_BPS", "5"))
ADVERSE_SELECTION_BPS = float(os.getenv("BF9_ADVERSE_SELECTION_BPS", "1"))

# Regular-user proxy defaults. Override if the actual account fee schedule differs.
REG_SPOT_MAKER_BPS = float(os.getenv("BF9_REG_SPOT_MAKER_BPS", "10"))
REG_SPOT_TAKER_BPS = float(os.getenv("BF9_REG_SPOT_TAKER_BPS", "10"))
REG_PERP_MAKER_BPS = float(os.getenv("BF9_REG_PERP_MAKER_BPS", "2"))
REG_PERP_TAKER_BPS = float(os.getenv("BF9_REG_PERP_TAKER_BPS", "5"))

DEFAULT_OUT = "data/bf1_v5_ab"


@dataclass(frozen=True)
class FeeProfile:
    name: str
    spot_maker_bps: float
    spot_taker_bps: float
    perp_maker_bps: float
    perp_taker_bps: float


@dataclass(frozen=True)
class Variant:
    name: str
    profile: str
    wait_minutes: int
    partial_fill_ratio: float
    adverse_selection_bps: float


def dt(s: str) -> datetime:
    x = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def snapshot(raw: dict) -> dict:
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


def target_profile(cost_bps: float) -> FeeProfile:
    # Four passive trade legs in a full round trip:
    # spot entry, perp entry, spot exit, perp exit.
    per_leg = cost_bps / 4.0
    return FeeProfile(
        name=f"TARGET_{cost_bps:.0f}",
        spot_maker_bps=per_leg,
        spot_taker_bps=per_leg + TAKER_FALLBACK_EXTRA_BPS,
        perp_maker_bps=per_leg,
        perp_taker_bps=per_leg + TAKER_FALLBACK_EXTRA_BPS,
    )


def build_profiles() -> dict[str, FeeProfile]:
    out = {}
    for x in TARGET_COSTS_BPS:
        p = target_profile(x)
        out[p.name] = p

    out["REGULAR_PROXY"] = FeeProfile(
        name="REGULAR_PROXY",
        spot_maker_bps=REG_SPOT_MAKER_BPS,
        spot_taker_bps=REG_SPOT_TAKER_BPS,
        perp_maker_bps=REG_PERP_MAKER_BPS,
        perp_taker_bps=REG_PERP_TAKER_BPS,
    )
    return out


PROFILES = build_profiles()


def build_variants() -> list[Variant]:
    out = []

    for pname in sorted(PROFILES):
        for wait in WAIT_MINUTES:
            for ratio in PARTIAL_FILL_RATIOS:
                out.append(
                    Variant(
                        name=f"{pname}_W{wait}_F{ratio:.2f}",
                        profile=pname,
                        wait_minutes=wait,
                        partial_fill_ratio=ratio,
                        adverse_selection_bps=ADVERSE_SELECTION_BPS,
                    )
                )

    return out


def projected_borrow_pct(hours: float) -> float:
    return BORROW_APR * (hours / (365.0 * 24.0)) * 100.0


def ideal_maker_roundtrip_cost_pct(profile: FeeProfile) -> float:
    return (
        profile.spot_maker_bps
        + profile.perp_maker_bps
        + profile.spot_maker_bps
        + profile.perp_maker_bps
    ) / 100.0


def entry_metrics(s: dict, profile: FeeProfile):
    maker_cost_pct = ideal_maker_roundtrip_cost_pct(profile)

    cash_basis = (s["perp_bid"] / s["spot_ask"] - 1.0) * 100.0
    cash_funding = s["funding_rate"] * 100.0 * FUNDING_EVENTS_PROJECTION
    cash_edge = (
        cash_basis
        - CLOSE_BASIS
        + cash_funding
        - maker_cost_pct
    )

    reverse_basis = (s["spot_bid"] / s["perp_ask"] - 1.0) * 100.0
    reverse_funding = (-s["funding_rate"]) * 100.0 * FUNDING_EVENTS_PROJECTION
    reverse_borrow = projected_borrow_pct(PROJECTED_HOLD_HOURS)
    reverse_edge = (
        reverse_basis
        - CLOSE_BASIS
        + reverse_funding
        - maker_cost_pct
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


def choose_signal(s: dict, profile: FeeProfile):
    candidates = entry_metrics(s, profile)
    best = max(candidates, key=lambda x: x["expected_edge_pct"])

    if best["expected_edge_pct"] < ENTRY_EDGE_PCT:
        return None

    return best


def maker_entry_limits(s: dict, direction: str):
    if direction == "CASH":
        return {
            "spot_side": "BUY",
            "spot_limit": s["spot_bid"],
            "perp_side": "SELL",
            "perp_limit": s["perp_ask"],
        }

    return {
        "spot_side": "SELL",
        "spot_limit": s["spot_ask"],
        "perp_side": "BUY",
        "perp_limit": s["perp_bid"],
    }


def maker_crossed(s: dict, side: str, limit_px: float, market: str) -> bool:
    if market == "spot":
        bid, ask = s["spot_bid"], s["spot_ask"]
    else:
        bid, ask = s["perp_bid"], s["perp_ask"]

    if side == "BUY":
        # Conservative proxy: ask must cross down through our resting bid.
        return ask <= limit_px

    # SELL: bid must cross up through our resting ask.
    return bid >= limit_px


def taker_px(s: dict, side: str, market: str) -> float:
    if market == "spot":
        return s["spot_ask"] if side == "BUY" else s["spot_bid"]
    return s["perp_ask"] if side == "BUY" else s["perp_bid"]


def fee_usdt(notional_usdt: float, bps: float) -> float:
    return notional_usdt * bps / 10000.0


def weighted_entry_price(
    maker_px: float,
    taker_px_value: float,
    maker_ratio: float,
    side: str,
    adverse_bps: float,
):
    maker_ratio = max(0.0, min(1.0, maker_ratio))
    taker_ratio = 1.0 - maker_ratio

    # Extra adverse-selection penalty on maker-filled quantity.
    adverse = adverse_bps / 10000.0
    if side == "BUY":
        maker_eff = maker_px * (1.0 + adverse)
    else:
        maker_eff = maker_px * (1.0 - adverse)

    return maker_eff * maker_ratio + taker_px_value * taker_ratio


def current_close_basis(s: dict, direction: str) -> float:
    if direction == "CASH":
        return (s["perp_ask"] / s["spot_bid"] - 1.0) * 100.0
    return (s["spot_ask"] / s["perp_bid"] - 1.0) * 100.0


def close_reason(pos: dict, s: dict):
    age_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0
    now_basis = current_close_basis(s, pos["direction"])
    entry_basis = pos["entry_basis_pct"]

    favorable = entry_basis - now_basis
    adverse = now_basis - entry_basis

    if entry_basis > CLOSE_BASIS and now_basis <= CLOSE_BASIS:
        return "BASIS_CONVERGED", now_basis, "MAKER_ATTEMPT"

    if favorable >= BASIS_TAKE_PROFIT:
        return "BASIS_TAKE_PROFIT", now_basis, "MAKER_ATTEMPT"

    if adverse >= BASIS_ADVERSE_STOP:
        return "BASIS_ADVERSE_STOP", now_basis, "TAKER"

    if pos["direction"] == "CASH":
        if s["funding_rate"] <= -FUNDING_ADVERSE_ABS:
            return "FUNDING_TURNED_ADVERSE", now_basis, "TAKER"
    else:
        if s["funding_rate"] >= FUNDING_ADVERSE_ABS:
            return "FUNDING_TURNED_ADVERSE", now_basis, "TAKER"

    if age_h >= MAX_HOURS:
        return "MAX_HOLD", now_basis, "TAKER"

    return None, now_basis, None


def accrue_funding(pos: dict, s: dict):
    nxt = int(pos.get("next_funding") or 0)
    last = int(pos.get("last_funding") or 0)

    if not nxt:
        return

    if int(s["t"].timestamp() * 1000) < nxt or last >= nxt:
        return

    perp_mid = (s["perp_bid"] + s["perp_ask"]) / 2.0
    base = pos["qty"] * perp_mid * s["funding_rate"]
    pos["funding"] += base if pos["direction"] == "CASH" else -base
    pos["funding_events"] += 1
    pos["last_funding"] = nxt

    new_next = int(s["next_funding_time_ms"] or 0)
    if new_next > nxt:
        pos["next_funding"] = new_next


def exit_sides(direction: str):
    if direction == "CASH":
        return "SELL", "BUY"
    return "BUY", "SELL"


def open_position_from_pending(p: dict, s: dict, v: Variant, profile: FeeProfile):
    spot_cross = p["spot_crossed"]
    perp_cross = p["perp_crossed"]

    if not spot_cross and not perp_cross:
        return None, "NO_FILL_CANCEL"

    spot_ratio = v.partial_fill_ratio if spot_cross else 0.0
    perp_ratio = v.partial_fill_ratio if perp_cross else 0.0

    spot_taker_now = taker_px(s, p["spot_side"], "spot")
    perp_taker_now = taker_px(s, p["perp_side"], "perp")

    entry_spot = weighted_entry_price(
        p["spot_limit"],
        spot_taker_now,
        spot_ratio,
        p["spot_side"],
        v.adverse_selection_bps if spot_cross else 0.0,
    )
    entry_perp = weighted_entry_price(
        p["perp_limit"],
        perp_taker_now,
        perp_ratio,
        p["perp_side"],
        v.adverse_selection_bps if perp_cross else 0.0,
    )

    qty = NOTIONAL / max(entry_spot, 1e-12)

    spot_fee = fee_usdt(
        NOTIONAL,
        profile.spot_maker_bps * spot_ratio
        + profile.spot_taker_bps * (1.0 - spot_ratio),
    )
    perp_fee = fee_usdt(
        NOTIONAL,
        profile.perp_maker_bps * perp_ratio
        + profile.perp_taker_bps * (1.0 - perp_ratio),
    )

    if spot_cross and perp_cross:
        execution_type = "BOTH_MAKER_THEN_COMPLETE"
    else:
        execution_type = "ONE_LEG_MAKER_TAKER_HEDGE"

    pos = {
        "direction": p["direction"],
        "opened_at": s["t"],
        "qty": qty,
        "entry_spot": entry_spot,
        "entry_perp": entry_perp,
        "entry_basis_pct": p["entry_basis_pct"],
        "entry_funding_rate": p["entry_funding_rate"],
        "entry_projected_funding_pct": p["projected_funding_pct"],
        "entry_expected_edge_pct": p["expected_edge_pct"],
        "entry_execution_type": execution_type,
        "entry_spot_maker_ratio": spot_ratio,
        "entry_perp_maker_ratio": perp_ratio,
        "entry_fee_usdt": spot_fee + perp_fee,
        "funding": 0.0,
        "funding_events": 0,
        "last_funding": 0,
        "next_funding": int(s["next_funding_time_ms"] or 0),
    }
    return pos, execution_type


def close_pnl(
    pos: dict,
    s: dict,
    profile: FeeProfile,
    exit_mode: str,
    maker_exit_filled: bool,
    adverse_bps: float,
):
    q = pos["qty"]
    spot_side, perp_side = exit_sides(pos["direction"])

    if exit_mode == "MAKER_ATTEMPT" and maker_exit_filled:
        spot_exit = s["spot_ask"] if spot_side == "SELL" else s["spot_bid"]
        perp_exit = s["perp_ask"] if perp_side == "SELL" else s["perp_bid"]

        adverse = adverse_bps / 10000.0
        if spot_side == "BUY":
            spot_exit *= (1.0 + adverse)
        else:
            spot_exit *= (1.0 - adverse)

        if perp_side == "BUY":
            perp_exit *= (1.0 + adverse)
        else:
            perp_exit *= (1.0 - adverse)

        spot_fee = fee_usdt(NOTIONAL, profile.spot_maker_bps)
        perp_fee = fee_usdt(NOTIONAL, profile.perp_maker_bps)
        exit_exec = "MAKER"
    else:
        spot_exit = taker_px(s, spot_side, "spot")
        perp_exit = taker_px(s, perp_side, "perp")
        spot_fee = fee_usdt(NOTIONAL, profile.spot_taker_bps)
        perp_fee = fee_usdt(NOTIONAL, profile.perp_taker_bps)
        exit_exec = "TAKER"

    if pos["direction"] == "CASH":
        gross = (
            q * (spot_exit - pos["entry_spot"])
            + q * (pos["entry_perp"] - perp_exit)
            + pos["funding"]
        )
        borrow = 0.0
    else:
        gross = (
            q * (pos["entry_spot"] - spot_exit)
            + q * (perp_exit - pos["entry_perp"])
            + pos["funding"]
        )
        hold_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0
        borrow = (
            q * pos["entry_spot"] * BORROW_APR * hold_h / (365.0 * 24.0)
        )

    fees = pos["entry_fee_usdt"] + spot_fee + perp_fee
    net = gross - borrow - fees
    capital = q * pos["entry_spot"]
    ret = net / capital * 100.0 if capital > 0 else 0.0

    effective_cost_bps = fees / max(capital, 1e-12) * 10000.0

    return {
        "gross_pnl_usdt": gross,
        "borrow_usdt": borrow,
        "fee_usdt": fees,
        "net_pnl_usdt": net,
        "net_return_pct": ret,
        "effective_fee_bps": effective_cost_bps,
        "exit_execution": exit_exec,
    }


def run_variant(raw_rows: list[dict], v: Variant):
    profile = PROFILES[v.profile]
    rows = [snapshot(r) for r in raw_rows]
    rows.sort(key=lambda x: (x["t"], x["symbol"]))

    wait_steps = max(1, int(round(v.wait_minutes / 15.0)))

    opens = {}
    pending = {}
    trades = []
    stats = Counter()
    closes = Counter()

    # For maker-exit attempt, store one-step pending exit marker.
    exit_pending = {}

    for s in rows:
        sym = s["symbol"]

        # 1) Resolve pending exits first.
        if sym in exit_pending and sym in opens:
            ep = exit_pending[sym]
            pos = opens[sym]

            spot_side, perp_side = exit_sides(pos["direction"])

            if pos["direction"] == "CASH":
                spot_limit = ep["spot_limit"]
                perp_limit = ep["perp_limit"]
            else:
                spot_limit = ep["spot_limit"]
                perp_limit = ep["perp_limit"]

            both_cross = (
                maker_crossed(s, spot_side, spot_limit, "spot")
                and maker_crossed(s, perp_side, perp_limit, "perp")
            )

            result = close_pnl(
                pos,
                s,
                profile,
                "MAKER_ATTEMPT",
                maker_exit_filled=both_cross,
                adverse_bps=v.adverse_selection_bps,
            )

            hold_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0

            trades.append({
                "variant": v.name,
                "profile": v.profile,
                "direction": pos["direction"],
                "symbol": sym,
                "opened_at": pos["opened_at"].isoformat(),
                "closed_at": s["t"].isoformat(),
                "hold_hours": round(hold_h, 4),
                "entry_execution_type": pos["entry_execution_type"],
                "entry_spot_maker_ratio": pos["entry_spot_maker_ratio"],
                "entry_perp_maker_ratio": pos["entry_perp_maker_ratio"],
                "exit_execution": result["exit_execution"],
                "entry_basis_pct": round(pos["entry_basis_pct"], 6),
                "entry_expected_edge_pct": round(pos["entry_expected_edge_pct"], 6),
                "funding_events": pos["funding_events"],
                "funding_usdt": round(pos["funding"], 8),
                "exit_reason": ep["reason"],
                **{k: round(val, 8) if isinstance(val, float) else val
                   for k, val in result.items()},
            })

            closes[ep["reason"]] += 1
            stats[f"exit_{result['exit_execution']}"] += 1
            del opens[sym]
            del exit_pending[sym]

        # 2) Accrue funding / consider exits.
        if sym in opens and sym not in exit_pending:
            pos = opens[sym]
            accrue_funding(pos, s)
            reason, exit_basis, mode = close_reason(pos, s)

            if reason:
                if mode == "MAKER_ATTEMPT":
                    spot_side, perp_side = exit_sides(pos["direction"])
                    if pos["direction"] == "CASH":
                        spot_limit = s["spot_ask"]   # sell maker
                        perp_limit = s["perp_bid"]   # buy maker
                    else:
                        spot_limit = s["spot_bid"]   # buy maker
                        perp_limit = s["perp_ask"]   # sell maker

                    exit_pending[sym] = {
                        "reason": reason,
                        "spot_limit": spot_limit,
                        "perp_limit": perp_limit,
                    }
                    stats["exit_maker_attempts"] += 1
                else:
                    result = close_pnl(
                        pos,
                        s,
                        profile,
                        "TAKER",
                        maker_exit_filled=False,
                        adverse_bps=0.0,
                    )
                    hold_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0

                    trades.append({
                        "variant": v.name,
                        "profile": v.profile,
                        "direction": pos["direction"],
                        "symbol": sym,
                        "opened_at": pos["opened_at"].isoformat(),
                        "closed_at": s["t"].isoformat(),
                        "hold_hours": round(hold_h, 4),
                        "entry_execution_type": pos["entry_execution_type"],
                        "entry_spot_maker_ratio": pos["entry_spot_maker_ratio"],
                        "entry_perp_maker_ratio": pos["entry_perp_maker_ratio"],
                        "exit_execution": result["exit_execution"],
                        "entry_basis_pct": round(pos["entry_basis_pct"], 6),
                        "entry_expected_edge_pct": round(pos["entry_expected_edge_pct"], 6),
                        "funding_events": pos["funding_events"],
                        "funding_usdt": round(pos["funding"], 8),
                        "exit_reason": reason,
                        **{k: round(val, 8) if isinstance(val, float) else val
                           for k, val in result.items()},
                    })

                    closes[reason] += 1
                    stats["exit_TAKER"] += 1
                    del opens[sym]

        # 3) Advance pending entries.
        if sym in pending and sym not in opens:
            p = pending[sym]
            p["age_steps"] += 1

            if not p["spot_crossed"]:
                if maker_crossed(s, p["spot_side"], p["spot_limit"], "spot"):
                    p["spot_crossed"] = True
                    stats["spot_maker_cross"] += 1

            if not p["perp_crossed"]:
                if maker_crossed(s, p["perp_side"], p["perp_limit"], "perp"):
                    p["perp_crossed"] = True
                    stats["perp_maker_cross"] += 1

            both = p["spot_crossed"] and p["perp_crossed"]
            timeout = p["age_steps"] >= wait_steps

            if both or timeout:
                pos, exec_type = open_position_from_pending(
                    p, s, v, profile
                )
                del pending[sym]

                if pos is None:
                    stats["entry_no_fill_cancel"] += 1
                else:
                    opens[sym] = pos
                    stats["entries_opened"] += 1
                    stats[f"entry_{exec_type}"] += 1

        # 4) Create new signal only if symbol is completely idle.
        if (
            sym not in opens
            and sym not in pending
            and sym not in exit_pending
            and len(opens) < MAX_OPEN
        ):
            signal = choose_signal(s, profile)
            if signal:
                limits = maker_entry_limits(s, signal["direction"])
                pending[sym] = {
                    "direction": signal["direction"],
                    "signal_at": s["t"],
                    "age_steps": 0,
                    "entry_basis_pct": signal["entry_basis_pct"],
                    "entry_funding_rate": s["funding_rate"],
                    "projected_funding_pct": signal["projected_funding_pct"],
                    "expected_edge_pct": signal["expected_edge_pct"],
                    "spot_side": limits["spot_side"],
                    "spot_limit": limits["spot_limit"],
                    "perp_side": limits["perp_side"],
                    "perp_limit": limits["perp_limit"],
                    "spot_crossed": False,
                    "perp_crossed": False,
                }
                stats["signals"] += 1

    return trades, stats, closes, len(opens), len(pending)


def profit_factor(vals):
    gp = sum(x for x in vals if x > 0)
    gl = abs(sum(x for x in vals if x < 0))
    if gl > 0:
        return gp / gl
    if gp > 0:
        return math.inf
    return 0.0


def max_drawdown(vals):
    eq = peak = worst = 0.0
    for x in vals:
        eq += x
        peak = max(peak, eq)
        worst = min(worst, eq - peak)
    return abs(worst)


def summary(v, trades, stats, closes, open_left, pending_left):
    vals = [float(t["net_return_pct"]) for t in trades]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    p = profit_factor(vals)

    fees = [float(t["effective_fee_bps"]) for t in trades]

    return {
        "variant": v.name,
        "profile": v.profile,
        "wait_minutes": v.wait_minutes,
        "partial_fill_ratio": v.partial_fill_ratio,
        "adverse_selection_bps": v.adverse_selection_bps,
        "closed": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": "INF" if math.isinf(p) else round(p, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "avg_net_return_pct": round(sum(vals) / len(vals), 6) if vals else 0.0,
        "max_drawdown_trade_points_pct": round(max_drawdown(vals), 4),
        "avg_effective_fee_bps": round(sum(fees) / len(fees), 4) if fees else 0.0,
        "signals": stats.get("signals", 0),
        "entries_opened": stats.get("entries_opened", 0),
        "entry_no_fill_cancel": stats.get("entry_no_fill_cancel", 0),
        "entry_both_maker": stats.get("entry_BOTH_MAKER_THEN_COMPLETE", 0),
        "entry_one_leg_hedge": stats.get("entry_ONE_LEG_MAKER_TAKER_HEDGE", 0),
        "exit_maker": stats.get("exit_MAKER", 0),
        "exit_taker": stats.get("exit_TAKER", 0),
        "spot_maker_cross": stats.get("spot_maker_cross", 0),
        "perp_maker_cross": stats.get("perp_maker_cross", 0),
        "open_left": open_left,
        "pending_left": pending_left,
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
        raise RuntimeError("BF9 no snapshots")

    vs = build_variants()
    all_trades = []
    summaries = []

    print(
        f"BF9_START snapshots={len(raw_rows)} variants={len(vs)} "
        f"entry_edge={ENTRY_EDGE_PCT} borrow_apr={BORROW_APR:.4f} "
        f"historical_only=true",
        flush=True,
    )

    for i, v in enumerate(vs, 1):
        trades, stats, closes, open_left, pending_left = run_variant(raw_rows, v)
        s = summary(v, trades, stats, closes, open_left, pending_left)

        summaries.append(s)
        all_trades.extend(trades)

        print(
            "BF9_RESULT " + json.dumps(
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
                    "avg_effective_fee_bps": s["avg_effective_fee_bps"],
                    "signals": s["signals"],
                    "entries_opened": s["entries_opened"],
                    "no_fill": s["entry_no_fill_cancel"],
                    "both_maker": s["entry_both_maker"],
                    "one_leg_hedge": s["entry_one_leg_hedge"],
                    "exit_maker": s["exit_maker"],
                    "exit_taker": s["exit_taker"],
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    # Viability: enough sample to be informative, PF>1, positive return,
    # and realized average fee <= V8's approximate break-even 8 bps.
    viable = []
    for s in summaries:
        p = 999.0 if s["profit_factor"] == "INF" else float(s["profit_factor"])
        if (
            int(s["closed"]) >= 20
            and p > 1.0
            and float(s["sum_net_return_pct"]) > 0
            and float(s["avg_effective_fee_bps"]) <= 8.0
        ):
            viable.append(s)

    def rank_key(s):
        n = int(s["closed"])
        p = 999.0 if s["profit_factor"] == "INF" else float(s["profit_factor"])
        fee_penalty = max(0.0, float(s["avg_effective_fee_bps"]) - 8.0)
        sample_penalty = max(0, 50 - n)
        return (
            fee_penalty,
            sample_penalty,
            -p,
            -float(s["sum_net_return_pct"]),
            float(s["max_drawdown_trade_points_pct"]),
        )

    ranked = sorted(summaries, key=rank_key)
    for rank, row in enumerate(ranked, 1):
        row["rank_execution_aware"] = rank

    summary_csv = out_dir / "bf1_v9_execution_summary.csv"
    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(ranked[0].keys()))
        w.writeheader()
        w.writerows(ranked)

    trades_csv = out_dir / "bf1_v9_execution_trades.csv"
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
        "entry_edge_pct": ENTRY_EDGE_PCT,
        "borrow_apr": BORROW_APR,
        "funding_projection_events": FUNDING_EVENTS_PROJECTION,
        "v8_break_even_reference_bps": 8.0,
        "profiles": {k: asdict(v) for k, v in PROFILES.items()},
        "variants": [asdict(v) for v in vs],
        "viable_execution_variants": viable,
        "ranked_results": ranked,
        "notes": [
            "Maker fill requires a later 15m BBO snapshot to cross the resting limit.",
            "The data has no queue position, trades, or depth, so maker fill is only a proxy.",
            "If one maker leg fills and the pair remains incomplete at timeout, the remainder is taker-hedged.",
            "No-fill pairs are cancelled and do not become trades.",
            "Risk exits use taker execution; favorable exits attempt maker for one 15m step then fall back to taker.",
            "This remains Binance Data Vision proxy research, not BingX venue-fidelity performance.",
        ],
    }

    json_path = out_dir / "bf1_v9_execution_summary.json"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    best = ranked[0]
    print(
        "BF9_BEST "
        + json.dumps(
            {
                "variant": best["variant"],
                "closed": best["closed"],
                "profit_factor": best["profit_factor"],
                "sum_net_return_pct": best["sum_net_return_pct"],
                "avg_effective_fee_bps": best["avg_effective_fee_bps"],
                "entries_opened": best["entries_opened"],
                "no_fill": best["entry_no_fill_cancel"],
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    print(
        "BF9_VIABLE_COUNT "
        + str(len(viable)),
        flush=True,
    )
    print(
        f"BF9_DONE summary={summary_csv} trades={trades_csv}",
        flush=True,
    )


if __name__ == "__main__":
    main()
