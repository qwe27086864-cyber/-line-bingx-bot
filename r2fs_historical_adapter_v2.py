"""
R2FS Historical V2 - Stage 1 fidelity replay.

Historical-only research adapter. It DOES NOT connect to Postgres and DOES NOT
send orders. It reuses the current R2FS fast-path scoring rules from
r2fs_historical_adapter.py, but fixes three replay-fidelity issues:

1) completed-candle timing (no current candle close/high/low/volume look-ahead),
2) executable-entry proxy using half-spread + adverse chase,
3) live-like max-open / max-new-per-scan / symbol-open / cooldown constraints.

Strategy thresholds and TP/SL remain unchanged in Stage 1 so the result can be
compared against the existing 360-day baseline without mixing strategy changes
into the replay-engine changes.

This is still FAST-path historical research. The full-R2 fallback remains a
separate Stage 2 fidelity task.
"""
from __future__ import annotations

import argparse
import bisect
from datetime import timedelta

import r2fs_historical_adapter as v1

CORE = "R2FS_V2_FIDELITY"
MAX_OPEN = 3
MAX_NEW_PER_SCAN = 2
COOLDOWN_HOURS = 2.0


def rolling_ticker_context_completed(c5):
    """Expose a 5m candle only at its close time."""
    out = {}
    from collections import deque

    q = deque()
    qsum = 0.0
    for i, row in enumerate(c5):
        t_open = row["t"]
        t_available = t_open + timedelta(minutes=5)
        cutoff = t_open.timestamp() - 24 * 3600

        q.append(row)
        qsum += row["quote_volume"]
        while q and q[0]["t"].timestamp() < cutoff:
            qsum -= q.popleft()["quote_volume"]

        change24 = 0.0
        if i >= 288:
            change24 = v1.pct(row["close"], c5[i - 288]["close"])

        out[t_available] = {
            "last": row["close"],
            "quote_volume": qsum,
            "change_24h": change24,
        }
    return out


def bars_up_to_completed(series, t, n, interval_minutes=None):
    """
    Return only candles fully completed by signal time t.

    v1.fast_scores calls this with only (series, t, n), so infer timeframe by
    looking at adjacent candle timestamps. This keeps the existing scoring code
    unchanged while removing look-ahead.
    """
    if not series:
        return []

    if interval_minutes is None:
        if len(series) >= 2:
            delta = (series[1]["t"] - series[0]["t"]).total_seconds() / 60.0
            interval_minutes = max(1, int(round(delta)))
        else:
            interval_minutes = 1

    cutoff = t - timedelta(minutes=interval_minutes)
    times = [x["t"] for x in series]
    idx = bisect.bisect_right(times, cutoff)
    if idx <= 0:
        return []
    return series[max(0, idx - n):idx]


def simulate_exit_v2(side, entry, entry_time, bars, max_runner_hours=24):
    tp1 = v1.level(entry, side, v1.TP1)
    tp2 = v1.level(entry, side, v1.TP2)
    tp3 = v1.level(entry, side, v1.TP3)
    sl = v1.level(entry, side, -v1.SL)

    h1 = h2 = h3 = False
    last = None

    for bar in bars:
        if bar["t"] <= entry_time:
            continue

        last = bar
        age = (bar["t"] - entry_time).total_seconds() / 3600.0

        if (not h1) and age >= 4.0:
            gross = v1.side_return(side, entry, bar["open"])
            return (
                bar["open"], "TIMEOUT_EXIT", gross,
                h1, h2, h3, True, bar["t"]
            )

        if h1 and age >= max_runner_hours:
            gross = v1.gross_stop(side, entry, tp1, tp2, h1, h2, bar["open"])
            return (
                bar["open"], "RUNNER_RESEARCH_CAP", gross,
                h1, h2, h3, True, bar["t"]
            )

        if h2:
            active_stop = tp1
            reason = "TP2_PROTECT_EXIT"
        elif h1:
            active_stop = entry
            reason = "BREAKEVEN_EXIT"
        else:
            active_stop = sl
            reason = "SL_EXIT"

        # Keep v1's conservative intrabar ordering: stop before new TP.
        if v1.stop_hit(side, bar, active_stop):
            gross = v1.gross_stop(
                side, entry, tp1, tp2, h1, h2, active_stop
            )
            return (
                active_stop, reason, gross,
                h1, h2, h3, True, bar["t"]
            )

        if (not h1) and v1.tp_hit(side, bar, tp1):
            h1 = True
            if v1.stop_hit(side, bar, entry):
                gross = v1.gross_stop(
                    side, entry, tp1, tp2, True, False, entry
                )
                return (
                    entry, "BREAKEVEN_EXIT", gross,
                    True, False, False, True, bar["t"]
                )

        if (not h2) and v1.tp_hit(side, bar, tp2):
            h1 = h2 = True
            if v1.stop_hit(side, bar, tp1):
                gross = v1.gross_stop(
                    side, entry, tp1, tp2, True, True, tp1
                )
                return (
                    tp1, "TP2_PROTECT_EXIT", gross,
                    True, True, False, True, bar["t"]
                )

        if (not h3) and v1.tp_hit(side, bar, tp3):
            h1 = h2 = True
            if v1.stop_hit(side, bar, tp1):
                gross = v1.gross_stop(
                    side, entry, tp1, tp2, True, True, tp1
                )
                return (
                    tp1, "TP2_PROTECT_EXIT", gross,
                    True, True, False, True, bar["t"]
                )
            h3 = True
            gross = v1.W1 * v1.TP1 + v1.W2 * v1.TP2 + v1.W3 * v1.TP3
            return (
                tp3, "TP3_EXIT", gross,
                True, True, True, True, bar["t"]
            )

    return (
        last["close"] if last else entry,
        "UNRESOLVED",
        0.0,
        h1, h2, h3,
        False,
        last["t"] if last else None,
    )


def replay_v2(
    c1_by,
    c5_by,
    assumed_spread,
    assumed_chase,
    max_runner_hours,
):
    # Monkey-patch only the in-process historical helper; repository code is
    # not modified.
    v1.bars_up_to = bars_up_to_completed

    contexts = {
        s: rolling_ticker_context_completed(rows)
        for s, rows in c5_by.items()
    }
    all_times = sorted({t for ctx in contexts.values() for t in ctx})

    cooldown_until = {}
    details = []
    active = []

    for t in all_times:
        # Settle positions that have already closed before this scan.
        active = [
            x for x in active
            if x["exit_time"] is None or x["exit_time"] > t
        ]
        created_this_scan = 0
        candidates = []

        for symbol in c5_by:
            ctx = contexts[symbol]
            cur = ctx.get(t)
            if not cur:
                continue

            times = sorted(ctx)
            idx = bisect.bisect_left(times, t)
            if idx <= 0:
                continue

            prev = ctx[times[idx - 1]]
            move = v1.pct(cur["last"], prev["last"])

            if abs(move) < v1.RADAR_MIN_MOVE:
                continue
            if (
                cur["quote_volume"]
                and cur["quote_volume"] < v1.MIN_QUOTE_VOLUME_24H
            ):
                continue

            side = "LONG" if move > 0 else "SHORT"
            rank = min(abs(move), 6.0) * 12.0

            if cur["quote_volume"] >= 1_000_000:
                rank += 8
            if cur["quote_volume"] >= 5_000_000:
                rank += 6
            if assumed_spread <= 0.20:
                rank += 12
            elif assumed_spread <= 0.35:
                rank += 7
            if v1.directional(side, cur["change_24h"]) > 0:
                rank += 5

            candidates.append(
                (rank, abs(move), symbol, side, move, cur["last"])
            )

        candidates.sort(reverse=True)

        for (
            _,
            __,
            symbol,
            side,
            move,
            signal_price,
        ) in candidates[:v1.RADAR_MAX_CANDIDATES][:v1.MAX_FAST_ANALYSIS]:

            if cooldown_until.get(symbol) and t < cooldown_until[symbol]:
                continue

            scores = v1.fast_scores(
                symbol,
                side,
                t,
                c1_by,
                c5_by,
                contexts,
                assumed_spread,
                assumed_chase,
            )
            if not scores:
                continue

            row = {
                "timestamp": t.isoformat(),
                "symbol": symbol,
                "core": CORE,
                "side": side,
                "decision": scores["decision"],
                "reason": scores["reason"],
                "radar_move_pct": round(move, 6),
                "direction_score": round(scores["direction_score"], 3),
                "micro_score": round(scores["micro_score"], 3),
                "market_score": round(scores["market_score"], 3),
                "execution_score": round(scores["execution_score"], 3),
                "fast_score": round(scores["fast_score"], 3),
                "assumed_spread_pct": assumed_spread,
                "assumed_chase_pct": assumed_chase,
                "entry_price": "",
                "exit_price": "",
                "exit_reason": "",
                "gross_return_pct": "",
                "net_return_pct": "",
                "sim_pnl_usdt": "",
                "tp1_hit": False,
                "tp2_hit": False,
                "tp3_hit": False,
                "resolved": False,
                "history_quality":
                    "OHLCV_FAST_V2_COMPLETED_CANDLE_EXEC_PROXY",
            }

            if scores["decision"] != "FAST_ENTER":
                details.append(row)
                continue

            # Live-like position controls.
            if created_this_scan >= MAX_NEW_PER_SCAN:
                row["decision"] = "RISK_BLOCKED_SCAN_CAP"
                row["reason"] = "max new per scan"
                details.append(row)
                continue

            if len(active) >= MAX_OPEN:
                row["decision"] = "RISK_BLOCKED_MAX_OPEN"
                row["reason"] = "max concurrent open"
                details.append(row)
                continue

            if any(x["symbol"] == symbol for x in active):
                row["decision"] = "RISK_BLOCKED_SYMBOL_OPEN"
                row["reason"] = "symbol already open"
                details.append(row)
                continue

            # Entry proxy:
            # - half-spread approximates crossing from mid/last to bid/ask
            # - adverse chase approximates signal->execution movement
            adverse_entry = (
                max(float(assumed_spread), 0.0) / 2.0
                + max(float(assumed_chase), 0.0)
            )

            if side == "LONG":
                entry = float(signal_price) * (1.0 + adverse_entry / 100.0)
            else:
                entry = float(signal_price) * (1.0 - adverse_entry / 100.0)

            future = c1_by.get(symbol, [])
            (
                exit_price,
                reason,
                gross,
                h1,
                h2,
                h3,
                resolved,
                exit_time,
            ) = simulate_exit_v2(
                side,
                entry,
                t,
                future,
                max_runner_hours,
            )

            # Preserve approximately the old 0.16%-point total execution-cost
            # assumption without double-counting the adverse entry proxy.
            residual_cost = max(
                0.0,
                v1.ROUND_TRIP_COST_PCT - adverse_entry,
            )
            net = gross - residual_cost if resolved else 0.0
            pnl = (
                v1.SIM_MARGIN_USDT
                * v1.SIM_LEVERAGE
                * net
                / 100.0
                if resolved
                else 0.0
            )

            row.update({
                "entry_price": round(entry, 12),
                "exit_price": round(exit_price, 12),
                "exit_reason": reason,
                "gross_return_pct": round(gross, 6),
                "net_return_pct": round(net, 6),
                "sim_pnl_usdt": round(pnl, 6),
                "tp1_hit": h1,
                "tp2_hit": h2,
                "tp3_hit": h3,
                "resolved": resolved,
            })
            details.append(row)

            created_this_scan += 1
            active.append({
                "symbol": symbol,
                "exit_time": exit_time,
            })
            cooldown_until[symbol] = (
                t + timedelta(hours=COOLDOWN_HOURS)
            )

    return details


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--candles-1m", required=True)
    p.add_argument("--candles-5m", required=True)
    p.add_argument(
        "--detail-out",
        default="data/r2fs_v2_history_detail.csv",
    )
    p.add_argument(
        "--outcomes-out",
        default="data/r2fs_v2_core_outcomes.csv",
    )
    p.add_argument(
        "--assumed-spread-pct",
        type=float,
        default=0.12,
    )
    p.add_argument(
        "--assumed-chase-pct",
        type=float,
        default=0.05,
    )
    p.add_argument(
        "--max-runner-hours",
        type=float,
        default=24.0,
    )
    args = p.parse_args()

    c1 = v1.read_candles(args.candles_1m)
    c5 = v1.read_candles(args.candles_5m)

    details = replay_v2(
        c1,
        c5,
        args.assumed_spread_pct,
        args.assumed_chase_pct,
        args.max_runner_hours,
    )

    old_core = v1.CORE
    v1.CORE = CORE
    try:
        result = v1.write_outputs(
            details,
            args.detail_out,
            args.outcomes_out,
        )
    finally:
        v1.CORE = old_core

    print("R2FS_V2_RESULT", result, flush=True)


if __name__ == "__main__":
    main()
