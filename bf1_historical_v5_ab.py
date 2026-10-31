"""
BF1 V11 Execution-Aware Entry Gate
(replaces bf1_historical_v5_ab.py so the existing V4 -> handoff runner executes it)

Purpose
-------
V8 showed unrestricted BF1 breaks down when realized round-trip cost rises
above ~8 bps. V9/V10 showed that practical maker-first execution often lands
around 17+ bps because:
- one leg fills maker
- the other leg is completed with taker
- exits frequently fall back to taker

V11 therefore does NOT assume ideal maker-only cost at signal time.
It estimates the likely execution path BEFORE entry and only opens when:

    gross carry edge
    - estimated execution cost
    - safety margin
    >= 0

Execution model
---------------
- two-way carry: CASH and REVERSE
- both entry legs rest as maker
- first maker fill triggers IMMEDIATE taker hedge of the other leg
- if both cross same snapshot, both are maker
- if neither fills within 30 minutes, cancel
- favorable exit: one-snapshot maker attempt, then taker fallback
- risk exit: immediate taker

Funding lookahead defense
-------------------------
V4 snapshots may carry a funding rate whose exact publication semantics are
ambiguous. V11 therefore uses an 8-hour LAGGED funding rate for:
- entry funding projection
- realized funding accrual proxy

This is deliberately conservative and avoids using the currently stamped
funding value as if it were certainly known before settlement.

Variants
--------
Profiles:
- TARGET_2 / TARGET_4 / TARGET_6 / TARGET_8
- REGULAR_PROXY

Safety margin:
- 0 / 2 / 5 bps

Maker partial-fill stress:
- 100% / 50%

Total: 5 * 3 * 2 = 30 variants.

Historical research only.
No DATABASE_URL.
No live/shadow writes.
No orders.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
from collections import Counter
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

os.environ.pop("DATABASE_URL", None)

NOTIONAL = float(os.getenv("BF11_NOTIONAL", "100"))
MAX_OPEN = int(os.getenv("BF11_MAX_OPEN", "3"))
MAX_HOURS = float(os.getenv("BF11_MAX_HOLD_HOURS", "48"))

CLOSE_BASIS = float(os.getenv("BF11_CLOSE_BASIS_PCT", "0.05"))
BASIS_ADVERSE_STOP = float(os.getenv("BF11_BASIS_ADVERSE_STOP_PCT", "0.25"))
BASIS_TAKE_PROFIT = float(os.getenv("BF11_BASIS_TAKE_PROFIT_PCT", "0.15"))
FUNDING_ADVERSE_ABS = float(os.getenv("BF11_FUNDING_ADVERSE_ABS", "0.0002"))

BORROW_APR = float(os.getenv("BF11_SPOT_BORROW_APR", "0.10"))
FUNDING_EVENTS_PROJECTION = int(os.getenv("BF11_FUNDING_EVENTS_PROJECTION", "3"))
PROJECTED_HOLD_HOURS = float(os.getenv("BF11_PROJECTED_HOLD_HOURS", "24"))

# 15m snapshots -> 32 steps = 8 hours.
FUNDING_LAG_STEPS = int(os.getenv("BF11_FUNDING_LAG_STEPS", "32"))

WAIT_MINUTES = int(os.getenv("BF11_WAIT_MINUTES", "30"))
ADVERSE_SELECTION_BPS = float(os.getenv("BF11_ADVERSE_SELECTION_BPS", "1"))

TARGET_COSTS_BPS = [
    float(x.strip())
    for x in os.getenv("BF11_TARGET_MAKER_ROUNDTRIP_BPS", "2,4,6,8").split(",")
    if x.strip()
]
SAFETY_MARGIN_BPS = [
    float(x.strip())
    for x in os.getenv("BF11_SAFETY_MARGIN_BPS", "0,2,5").split(",")
    if x.strip()
]
PARTIAL_FILL_RATIOS = [
    float(x.strip())
    for x in os.getenv("BF11_PARTIAL_FILL_RATIOS", "1.0,0.5").split(",")
    if x.strip()
]

TAKER_FALLBACK_EXTRA_BPS = float(os.getenv("BF11_TAKER_FALLBACK_EXTRA_BPS", "5"))

# Research proxy defaults; override with actual account fees if known.
REG_SPOT_MAKER_BPS = float(os.getenv("BF11_REG_SPOT_MAKER_BPS", "10"))
REG_SPOT_TAKER_BPS = float(os.getenv("BF11_REG_SPOT_TAKER_BPS", "10"))
REG_PERP_MAKER_BPS = float(os.getenv("BF11_REG_PERP_MAKER_BPS", "2"))
REG_PERP_TAKER_BPS = float(os.getenv("BF11_REG_PERP_TAKER_BPS", "5"))

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
    safety_margin_bps: float
    partial_fill_ratio: float


def parse_dt(s: str) -> datetime:
    x = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def target_profile(cost_bps: float) -> FeeProfile:
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

    for cost in TARGET_COSTS_BPS:
        p = target_profile(cost)
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
        for margin in SAFETY_MARGIN_BPS:
            for ratio in PARTIAL_FILL_RATIOS:
                out.append(
                    Variant(
                        name=f"{pname}_M{margin:.0f}_F{ratio:.2f}",
                        profile=pname,
                        safety_margin_bps=margin,
                        partial_fill_ratio=ratio,
                    )
                )
    return out


def base_snap(raw: dict) -> dict:
    return {
        "t": parse_dt(raw["timestamp"]),
        "symbol": raw["symbol"].upper(),
        "spot_bid": float(raw["spot_bid"]),
        "spot_ask": float(raw["spot_ask"]),
        "perp_bid": float(raw["perp_bid"]),
        "perp_ask": float(raw["perp_ask"]),
        "raw_funding_rate": float(raw.get("funding_rate") or 0.0),
        "next_funding_time_ms": int(float(raw.get("next_funding_time_ms") or 0)),
    }


def attach_lagged_funding(rows: list[dict]) -> list[dict]:
    by_symbol = {}
    for r in rows:
        by_symbol.setdefault(r["symbol"], []).append(r)

    out = []
    for sym, arr in by_symbol.items():
        arr.sort(key=lambda x: x["t"])
        for i, r in enumerate(arr):
            lag_i = max(0, i - FUNDING_LAG_STEPS)
            x = dict(r)
            x["known_funding_rate"] = arr[lag_i]["raw_funding_rate"]
            out.append(x)

    out.sort(key=lambda x: (x["t"], x["symbol"]))
    return out


def borrow_pct(hours: float) -> float:
    return BORROW_APR * (hours / (365.0 * 24.0)) * 100.0


def estimated_roundtrip_fee_bps(profile: FeeProfile, maker_ratio: float) -> float:
    """
    Conservative path:
    - one entry leg is maker-filled at maker_ratio
    - remaining portion of that leg is taker
    - the OTHER entry leg is fully taker (immediate hedge)
    - both exit legs are taker

    We compute both possible first-fill paths and take the WORSE fee path.
    """
    r = max(0.0, min(1.0, maker_ratio))

    # Spot maker first, perp taker hedge.
    path_spot_first = (
        profile.spot_maker_bps * r
        + profile.spot_taker_bps * (1.0 - r)
        + profile.perp_taker_bps
        + profile.spot_taker_bps
        + profile.perp_taker_bps
    )

    # Perp maker first, spot taker hedge.
    path_perp_first = (
        profile.perp_maker_bps * r
        + profile.perp_taker_bps * (1.0 - r)
        + profile.spot_taker_bps
        + profile.spot_taker_bps
        + profile.perp_taker_bps
    )

    return max(path_spot_first, path_perp_first)


def gross_entry_candidates(s: dict):
    fr = s["known_funding_rate"]

    cash_basis = (s["perp_bid"] / s["spot_ask"] - 1.0) * 100.0
    cash_funding = fr * 100.0 * FUNDING_EVENTS_PROJECTION
    cash_gross = cash_basis - CLOSE_BASIS + cash_funding

    reverse_basis = (s["spot_bid"] / s["perp_ask"] - 1.0) * 100.0
    reverse_funding = (-fr) * 100.0 * FUNDING_EVENTS_PROJECTION
    reverse_borrow = borrow_pct(PROJECTED_HOLD_HOURS)
    reverse_gross = reverse_basis - CLOSE_BASIS + reverse_funding - reverse_borrow

    return [
        {
            "direction": "CASH",
            "entry_basis_pct": cash_basis,
            "projected_funding_pct": cash_funding,
            "projected_borrow_pct": 0.0,
            "gross_edge_pct": cash_gross,
        },
        {
            "direction": "REVERSE",
            "entry_basis_pct": reverse_basis,
            "projected_funding_pct": reverse_funding,
            "projected_borrow_pct": reverse_borrow,
            "gross_edge_pct": reverse_gross,
        },
    ]


def choose_signal(s: dict, v: Variant, profile: FeeProfile):
    est_fee_bps = estimated_roundtrip_fee_bps(profile, v.partial_fill_ratio)
    adverse_bps = ADVERSE_SELECTION_BPS * max(0.0, min(1.0, v.partial_fill_ratio))
    required_bps = est_fee_bps + adverse_bps + v.safety_margin_bps
    required_pct = required_bps / 100.0

    cands = gross_entry_candidates(s)
    best = max(cands, key=lambda x: x["gross_edge_pct"])

    best["estimated_exec_fee_bps"] = est_fee_bps
    best["estimated_adverse_bps"] = adverse_bps
    best["required_edge_bps"] = required_bps
    best["net_after_gate_pct"] = best["gross_edge_pct"] - required_pct

    if best["net_after_gate_pct"] < 0:
        return None, best

    return best, best


def maker_limits(s: dict, direction: str):
    if direction == "CASH":
        return "BUY", s["spot_bid"], "SELL", s["perp_ask"]
    return "SELL", s["spot_ask"], "BUY", s["perp_bid"]


def crossed(s: dict, market: str, side: str, limit_px: float) -> bool:
    if market == "spot":
        bid, ask = s["spot_bid"], s["spot_ask"]
    else:
        bid, ask = s["perp_bid"], s["perp_ask"]

    if side == "BUY":
        return ask <= limit_px
    return bid >= limit_px


def taker_price(s: dict, market: str, side: str) -> float:
    if market == "spot":
        return s["spot_ask"] if side == "BUY" else s["spot_bid"]
    return s["perp_ask"] if side == "BUY" else s["perp_bid"]


def maker_effective_price(limit_px: float, side: str, adverse_bps: float) -> float:
    a = adverse_bps / 10000.0
    if side == "BUY":
        return limit_px * (1.0 + a)
    return limit_px * (1.0 - a)


def weighted_leg(
    maker_px: float,
    taker_px: float,
    maker_ratio: float,
    side: str,
):
    r = max(0.0, min(1.0, maker_ratio))
    maker_eff = maker_effective_price(maker_px, side, ADVERSE_SELECTION_BPS)
    return maker_eff * r + taker_px * (1.0 - r)


def fee_usdt(notional: float, bps: float) -> float:
    return notional * bps / 10000.0


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
        return "BASIS_CONVERGED", "MAKER_ATTEMPT"

    if favorable >= BASIS_TAKE_PROFIT:
        return "BASIS_TAKE_PROFIT", "MAKER_ATTEMPT"

    if adverse >= BASIS_ADVERSE_STOP:
        return "BASIS_ADVERSE_STOP", "TAKER"

    fr = s["known_funding_rate"]

    if pos["direction"] == "CASH":
        if fr <= -FUNDING_ADVERSE_ABS:
            return "FUNDING_TURNED_ADVERSE", "TAKER"
    else:
        if fr >= FUNDING_ADVERSE_ABS:
            return "FUNDING_TURNED_ADVERSE", "TAKER"

    if age_h >= MAX_HOURS:
        return "MAX_HOLD", "TAKER"

    return None, None


def accrue_funding(pos: dict, s: dict):
    nxt = int(pos.get("next_funding") or 0)
    last = int(pos.get("last_funding") or 0)
    now_ms = int(s["t"].timestamp() * 1000)

    if not nxt or now_ms < nxt or last >= nxt:
        return

    perp_mid = (s["perp_bid"] + s["perp_ask"]) / 2.0
    fr = s["known_funding_rate"]
    base = pos["qty"] * perp_mid * fr

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


def open_from_pending(pend: dict, s: dict, v: Variant, profile: FeeProfile):
    spot_hit = crossed(s, "spot", pend["spot_side"], pend["spot_limit"])
    perp_hit = crossed(s, "perp", pend["perp_side"], pend["perp_limit"])

    if not spot_hit and not perp_hit:
        return None, None

    r = v.partial_fill_ratio

    if spot_hit and perp_hit:
        spot_maker_ratio = r
        perp_maker_ratio = r
        execution = "BOTH_MAKER_SAME_SNAPSHOT"
    elif spot_hit:
        spot_maker_ratio = r
        perp_maker_ratio = 0.0
        execution = "SPOT_MAKER_PERP_IMMEDIATE_TAKER"
    else:
        spot_maker_ratio = 0.0
        perp_maker_ratio = r
        execution = "PERP_MAKER_SPOT_IMMEDIATE_TAKER"

    spot_taker = taker_price(s, "spot", pend["spot_side"])
    perp_taker = taker_price(s, "perp", pend["perp_side"])

    entry_spot = weighted_leg(
        pend["spot_limit"], spot_taker, spot_maker_ratio, pend["spot_side"]
    )
    entry_perp = weighted_leg(
        pend["perp_limit"], perp_taker, perp_maker_ratio, pend["perp_side"]
    )

    qty = NOTIONAL / max(entry_spot, 1e-12)

    spot_fee_bps = (
        profile.spot_maker_bps * spot_maker_ratio
        + profile.spot_taker_bps * (1.0 - spot_maker_ratio)
    )
    perp_fee_bps = (
        profile.perp_maker_bps * perp_maker_ratio
        + profile.perp_taker_bps * (1.0 - perp_maker_ratio)
    )

    pos = {
        "direction": pend["direction"],
        "opened_at": s["t"],
        "qty": qty,
        "entry_spot": entry_spot,
        "entry_perp": entry_perp,
        "entry_basis_pct": pend["entry_basis_pct"],
        "gross_edge_pct": pend["gross_edge_pct"],
        "estimated_exec_fee_bps": pend["estimated_exec_fee_bps"],
        "required_edge_bps": pend["required_edge_bps"],
        "entry_execution_type": execution,
        "entry_spot_maker_ratio": spot_maker_ratio,
        "entry_perp_maker_ratio": perp_maker_ratio,
        "entry_fee_usdt": fee_usdt(NOTIONAL, spot_fee_bps + perp_fee_bps),
        "funding": 0.0,
        "funding_events": 0,
        "last_funding": 0,
        "next_funding": int(s["next_funding_time_ms"] or 0),
    }

    return pos, execution


def finalize_close(pos, s, spot_px, perp_px, total_fee, exit_exec):
    q = pos["qty"]

    if pos["direction"] == "CASH":
        gross = (
            q * (spot_px - pos["entry_spot"])
            + q * (pos["entry_perp"] - perp_px)
            + pos["funding"]
        )
        borrow = 0.0
    else:
        gross = (
            q * (pos["entry_spot"] - spot_px)
            + q * (perp_px - pos["entry_perp"])
            + pos["funding"]
        )
        hold_h = (s["t"] - pos["opened_at"]).total_seconds() / 3600.0
        borrow = q * pos["entry_spot"] * BORROW_APR * hold_h / (365.0 * 24.0)

    net = gross - borrow - total_fee
    capital = q * pos["entry_spot"]
    ret = net / capital * 100.0 if capital > 0 else 0.0
    eff_bps = total_fee / max(capital, 1e-12) * 10000.0

    return {
        "gross_pnl_usdt": gross,
        "borrow_usdt": borrow,
        "fee_usdt": total_fee,
        "net_pnl_usdt": net,
        "net_return_pct": ret,
        "effective_fee_bps": eff_bps,
        "exit_execution": exit_exec,
    }


def close_taker(pos: dict, s: dict, profile: FeeProfile):
    spot_side, perp_side = exit_sides(pos["direction"])
    spot_px = taker_price(s, "spot", spot_side)
    perp_px = taker_price(s, "perp", perp_side)

    exit_fee = fee_usdt(
        NOTIONAL,
        profile.spot_taker_bps + profile.perp_taker_bps,
    )

    return finalize_close(
        pos, s, spot_px, perp_px, pos["entry_fee_usdt"] + exit_fee, "TAKER"
    )


def run_variant(raw_rows: list[dict], v: Variant):
    profile = PROFILES[v.profile]
    rows = attach_lagged_funding([base_snap(r) for r in raw_rows])

    wait_steps = max(1, int(round(WAIT_MINUTES / 15.0)))

    opens = {}
    pending = {}
    exit_pending = {}

    trades = []
    stats = Counter()
    closes = Counter()

    for s in rows:
        sym = s["symbol"]

        # Resolve favorable exit maker attempt.
        if sym in exit_pending and sym in opens:
            ep = exit_pending[sym]
            pos = opens[sym]
            spot_side, perp_side = exit_sides(pos["direction"])

            spot_hit = crossed(s, "spot", spot_side, ep["spot_limit"])
            perp_hit = crossed(s, "perp", perp_side, ep["perp_limit"])

            if spot_hit and perp_hit:
                spot_px = maker_effective_price(
                    ep["spot_limit"], spot_side, ADVERSE_SELECTION_BPS
                )
                perp_px = maker_effective_price(
                    ep["perp_limit"], perp_side, ADVERSE_SELECTION_BPS
                )
                exit_fee = fee_usdt(
                    NOTIONAL,
                    profile.spot_maker_bps + profile.perp_maker_bps,
                )
                result = finalize_close(
                    pos,
                    s,
                    spot_px,
                    perp_px,
                    pos["entry_fee_usdt"] + exit_fee,
                    "MAKER",
                )
                stats["exit_maker"] += 1
            else:
                result = close_taker(pos, s, profile)
                stats["exit_taker_fallback"] += 1

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
                "entry_gross_edge_pct": round(pos["gross_edge_pct"], 6),
                "entry_estimated_exec_fee_bps": round(pos["estimated_exec_fee_bps"], 4),
                "entry_required_edge_bps": round(pos["required_edge_bps"], 4),
                "exit_reason": ep["reason"],
                "funding_events": pos["funding_events"],
                "funding_usdt": round(pos["funding"], 8),
                **{
                    k: round(val, 8) if isinstance(val, float) else val
                    for k, val in result.items()
                },
            })

            closes[ep["reason"]] += 1
            del opens[sym]
            del exit_pending[sym]

        # Funding + exits.
        if sym in opens and sym not in exit_pending:
            pos = opens[sym]
            accrue_funding(pos, s)
            reason, mode = close_reason(pos, s)

            if reason:
                if mode == "MAKER_ATTEMPT":
                    if pos["direction"] == "CASH":
                        spot_limit = s["spot_ask"]
                        perp_limit = s["perp_bid"]
                    else:
                        spot_limit = s["spot_bid"]
                        perp_limit = s["perp_ask"]

                    exit_pending[sym] = {
                        "reason": reason,
                        "spot_limit": spot_limit,
                        "perp_limit": perp_limit,
                    }
                    stats["exit_maker_attempt"] += 1
                else:
                    result = close_taker(pos, s, profile)
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
                        "entry_gross_edge_pct": round(pos["gross_edge_pct"], 6),
                        "entry_estimated_exec_fee_bps": round(
                            pos["estimated_exec_fee_bps"], 4
                        ),
                        "entry_required_edge_bps": round(
                            pos["required_edge_bps"], 4
                        ),
                        "exit_reason": reason,
                        "funding_events": pos["funding_events"],
                        "funding_usdt": round(pos["funding"], 8),
                        **{
                            k: round(val, 8) if isinstance(val, float) else val
                            for k, val in result.items()
                        },
                    })

                    closes[reason] += 1
                    stats["exit_taker"] += 1
                    del opens[sym]

        # Pending entry: first fill => immediate hedge.
        if sym in pending and sym not in opens:
            p = pending[sym]
            p["age_steps"] += 1

            pos, execution = open_from_pending(p, s, v, profile)

            if pos is not None:
                opens[sym] = pos
                del pending[sym]
                stats["entries_opened"] += 1
                stats[f"entry_{execution}"] += 1
            elif p["age_steps"] >= wait_steps:
                del pending[sym]
                stats["entry_no_fill_cancel"] += 1

        # Fresh signal with execution-aware gate.
        if (
            sym not in opens
            and sym not in pending
            and sym not in exit_pending
            and len(opens) < MAX_OPEN
        ):
            stats["candidate_rows"] += 1
            signal, diag = choose_signal(s, v, profile)

            if signal is None:
                stats["gate_reject"] += 1
                continue

            stats["gate_pass"] += 1

            spot_side, spot_limit, perp_side, perp_limit = maker_limits(
                s, signal["direction"]
            )

            pending[sym] = {
                "direction": signal["direction"],
                "signal_at": s["t"],
                "age_steps": 0,
                "entry_basis_pct": signal["entry_basis_pct"],
                "gross_edge_pct": signal["gross_edge_pct"],
                "estimated_exec_fee_bps": signal["estimated_exec_fee_bps"],
                "required_edge_bps": signal["required_edge_bps"],
                "spot_side": spot_side,
                "spot_limit": spot_limit,
                "perp_side": perp_side,
                "perp_limit": perp_limit,
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


def max_dd(vals):
    eq = peak = worst = 0.0
    for x in vals:
        eq += x
        peak = max(peak, eq)
        worst = min(worst, eq - peak)
    return abs(worst)


def summarize(v, trades, stats, closes, open_left, pending_left):
    vals = [float(t["net_return_pct"]) for t in trades]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    pf = profit_factor(vals)

    fees = [float(t["effective_fee_bps"]) for t in trades]
    est = [float(t["entry_estimated_exec_fee_bps"]) for t in trades]

    return {
        "variant": v.name,
        "profile": v.profile,
        "safety_margin_bps": v.safety_margin_bps,
        "partial_fill_ratio": v.partial_fill_ratio,
        "closed": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": "INF" if math.isinf(pf) else round(pf, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "avg_net_return_pct": round(sum(vals) / len(vals), 6) if vals else 0.0,
        "max_drawdown_trade_points_pct": round(max_dd(vals), 4),
        "avg_effective_fee_bps": round(sum(fees) / len(fees), 4) if fees else 0.0,
        "avg_estimated_entry_cost_bps": round(sum(est) / len(est), 4) if est else 0.0,
        "candidate_rows": stats.get("candidate_rows", 0),
        "gate_pass": stats.get("gate_pass", 0),
        "gate_reject": stats.get("gate_reject", 0),
        "signals": stats.get("signals", 0),
        "entries_opened": stats.get("entries_opened", 0),
        "entry_no_fill_cancel": stats.get("entry_no_fill_cancel", 0),
        "entry_both_maker": stats.get("entry_BOTH_MAKER_SAME_SNAPSHOT", 0),
        "entry_spot_maker_perp_taker": stats.get(
            "entry_SPOT_MAKER_PERP_IMMEDIATE_TAKER", 0
        ),
        "entry_perp_maker_spot_taker": stats.get(
            "entry_PERP_MAKER_SPOT_IMMEDIATE_TAKER", 0
        ),
        "exit_maker": stats.get("exit_maker", 0),
        "exit_taker": stats.get("exit_taker", 0),
        "exit_taker_fallback": stats.get("exit_taker_fallback", 0),
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
        raise RuntimeError("BF11 no snapshots")

    vs = build_variants()
    all_trades = []
    summaries = []

    print(
        f"BF11_START snapshots={len(raw_rows)} variants={len(vs)} "
        f"funding_lag_steps={FUNDING_LAG_STEPS} wait_minutes={WAIT_MINUTES} "
        f"historical_only=true",
        flush=True,
    )

    for i, v in enumerate(vs, 1):
        trades, stats, closes, open_left, pending_left = run_variant(raw_rows, v)
        s = summarize(v, trades, stats, closes, open_left, pending_left)

        summaries.append(s)
        all_trades.extend(trades)

        print(
            "BF11_RESULT "
            + json.dumps(
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
                    "avg_estimated_entry_cost_bps": s["avg_estimated_entry_cost_bps"],
                    "gate_pass": s["gate_pass"],
                    "gate_reject": s["gate_reject"],
                    "entries_opened": s["entries_opened"],
                    "no_fill": s["entry_no_fill_cancel"],
                    "both_maker": s["entry_both_maker"],
                    "spot_maker_perp_taker": s["entry_spot_maker_perp_taker"],
                    "perp_maker_spot_taker": s["entry_perp_maker_spot_taker"],
                    "exit_maker": s["exit_maker"],
                    "exit_taker": s["exit_taker"],
                    "exit_taker_fallback": s["exit_taker_fallback"],
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    viable20 = []
    viable10 = []

    for s in summaries:
        p = 999.0 if s["profit_factor"] == "INF" else float(s["profit_factor"])
        good = p > 1.0 and float(s["sum_net_return_pct"]) > 0

        if good and int(s["closed"]) >= 20:
            viable20.append(s)
        if good and int(s["closed"]) >= 10:
            viable10.append(s)

    def rank_key(s):
        p = 999.0 if s["profit_factor"] == "INF" else float(s["profit_factor"])
        sample_penalty = max(0, 20 - int(s["closed"]))
        positive_penalty = 0 if float(s["sum_net_return_pct"]) > 0 else 1
        pf_penalty = 0 if p > 1.0 else 1

        return (
            positive_penalty,
            pf_penalty,
            sample_penalty,
            -p,
            -float(s["sum_net_return_pct"]),
            float(s["max_drawdown_trade_points_pct"]),
        )

    ranked = sorted(summaries, key=rank_key)
    for rank, row in enumerate(ranked, 1):
        row["rank_execution_gate"] = rank

    summary_csv = out_dir / "bf1_v11_execution_gate_summary.csv"
    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(ranked[0].keys()))
        w.writeheader()
        w.writerows(ranked)

    trades_csv = out_dir / "bf1_v11_execution_gate_trades.csv"
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
        "funding_lag_steps": FUNDING_LAG_STEPS,
        "funding_lag_hours": FUNDING_LAG_STEPS * 0.25,
        "wait_minutes": WAIT_MINUTES,
        "profiles": {k: asdict(v) for k, v in PROFILES.items()},
        "variants": [asdict(v) for v in vs],
        "viable_20plus": viable20,
        "viable_10plus": viable10,
        "ranked_results": ranked,
        "notes": [
            "Entry gate uses worst of spot-maker-first and perp-maker-first execution fee paths.",
            "Entry gate assumes the other entry leg is immediate taker and both exit legs are taker.",
            "Safety margin and adverse-selection allowance are added before entry.",
            "Funding projection and funding accrual use an 8-hour lagged funding rate to avoid lookahead risk.",
            "This is a conservative Binance Data Vision proxy, not BingX venue-fidelity performance.",
        ],
    }

    json_path = out_dir / "bf1_v11_execution_gate_summary.json"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")

    best = ranked[0]

    print(
        "BF11_BEST "
        + json.dumps(
            {
                "variant": best["variant"],
                "closed": best["closed"],
                "profit_factor": best["profit_factor"],
                "sum_net_return_pct": best["sum_net_return_pct"],
                "avg_effective_fee_bps": best["avg_effective_fee_bps"],
                "gate_pass": best["gate_pass"],
                "gate_reject": best["gate_reject"],
            },
            separators=(",", ":"),
        ),
        flush=True,
    )
    print("BF11_VIABLE_20PLUS " + str(len(viable20)), flush=True)
    print("BF11_VIABLE_10PLUS " + str(len(viable10)), flush=True)
    print(
        f"BF11_DONE summary={summary_csv} trades={trades_csv}",
        flush=True,
    )


if __name__ == "__main__":
    main()
