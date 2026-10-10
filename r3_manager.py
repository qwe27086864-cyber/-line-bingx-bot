"""V4.3R3 Stage 2: simulation-only, read-only diagnostics and entry shadow filter.

This module deliberately does NOT install or replace any trading engine.
It cannot open, update, close, or execute a trade.
"""
from __future__ import annotations

from dataclasses import dataclass, asdict
from math import isfinite
from typing import Any, Mapping

VERSION = "V4.3R3"


@dataclass(frozen=True)
class R3FilterSettings:
    # Research parameters, NOT optimized or validated trading thresholds.
    max_long_ema20_distance_pct: float = 2.0
    min_long_rsi5: float = 48.0
    max_long_rsi5: float = 75.0
    market_btc_5m_floor_pct: float = -0.35
    market_eth_5m_floor_pct: float = -0.35


def install_r3(ns: dict[str, Any]) -> None:
    """Opt-in diagnostic install. Does not change SCANNER_ENGINE_VERSION."""
    if ns.get("LIVE_TRADING", False):
        raise RuntimeError("R3 diagnostics require LIVE_TRADING=false")
    required = ("get_db_connection", "update_simulated_trades",
                "create_simulated_trade", "finalize_trade")
    missing = [name for name in required if not callable(ns.get(name))]
    if missing:
        raise RuntimeError("R3 missing functions: " + ", ".join(missing))
    ns["_R3_INSTALLED"] = True
    ns["R3_DIAGNOSTICS_ENABLED"] = True
    print("R3 READ-ONLY DIAGNOSTICS READY; R2 CORE UNCHANGED", flush=True)


def _number(context: Mapping[str, Any], name: str) -> float | None:
    value = context.get(name)
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if isfinite(result) else None


def r3_shadow_long_trend_filter(
    context: Mapping[str, Any],
    settings: R3FilterSettings = R3FilterSettings(),
) -> dict[str, Any]:
    """Shadow-only assessment of LONG TREND. Never decides or executes a trade.

    Required context: side, strategy, price, ema20_5m, rsi5,
    btc_change_5m_pct, eth_change_5m_pct.
    Fail-closed: unknown/missing data yields WOULD_BLOCK, not approval.
    """
    if str(context.get("side", "")).upper() != "LONG" or str(
        context.get("strategy", "")
    ).upper() != "TREND":
        return {"applicable": False, "would_block": False,
                "reasons": [], "missing_fields": []}

    fields = ("price", "ema20_5m", "rsi5", "btc_change_5m_pct",
              "eth_change_5m_pct")
    values = {key: _number(context, key) for key in fields}
    missing = [key for key, value in values.items() if value is None]
    reasons = []
    if missing:
        reasons.append("MISSING_OR_INVALID_DATA")
    else:
        price, ema = values["price"], values["ema20_5m"]
        rsi = values["rsi5"]
        if price <= 0 or ema <= 0:
            reasons.append("INVALID_PRICE_OR_EMA")
        else:
            distance = (price / ema - 1.0) * 100.0
            if distance > settings.max_long_ema20_distance_pct:
                reasons.append("LONG_EMA20_OVEREXTENDED")
        if not settings.min_long_rsi5 <= rsi <= settings.max_long_rsi5:
            reasons.append("LONG_RSI_OUT_OF_RANGE")
        if values["btc_change_5m_pct"] <= settings.market_btc_5m_floor_pct:
            reasons.append("BTC_SHORT_TERM_WEAKNESS")
        if values["eth_change_5m_pct"] <= settings.market_eth_5m_floor_pct:
            reasons.append("ETH_SHORT_TERM_WEAKNESS")
    return {"applicable": True, "would_block": bool(reasons),
            "reasons": reasons, "missing_fields": missing,
            "settings": asdict(settings)}


def r3_diagnostic_report(get_db_connection):
    """Read-only summary of R1/R2, with sample count and basic win rate."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT engine_version, side, strategy,
                       COUNT(*) AS closed,
                       COUNT(*) FILTER (WHERE status='WIN') AS wins,
                       COUNT(*) FILTER (WHERE exit_reason='SL_EXIT') AS stops,
                       COUNT(*) FILTER (WHERE exit_reason='EARLY_WEAK_EXIT') AS early_exits,
                       COALESCE(AVG(net_result_pct),0) AS average_net_pct,
                       COALESCE(SUM(sim_pnl_usdt),0) AS realized_pnl_usdt
                FROM scanner_trades
                WHERE engine_version IN ('V4.3R1','V4.3R2')
                  AND strategy IN ('TREND','BREAKOUT')
                  AND status IN ('WIN','LOSS')
                GROUP BY engine_version, side, strategy
                ORDER BY engine_version, side, strategy
            """)
            rows = cur.fetchall()
        return [{"version": r[0], "side": r[1], "strategy": r[2],
                 "closed": int(r[3]), "wins": int(r[4]),
                 "win_rate_pct": round(100 * int(r[4]) / int(r[3]), 2)
                 if r[3] else 0.0,
                 "stops": int(r[5]), "early_exits": int(r[6]),
                 "average_net_pct": round(float(r[7]), 4),
                 "realized_pnl_usdt": round(float(r[8]), 4)} for r in rows]
    finally:
        conn.close()
