#!/usr/bin/env python3
"""Phase 5: offline, causally timed TP2 trend weakness exit approximation.
Uses V4.2R1 weakness rules (5m/15m EMA, RSI, momentum) on completed bars.
Historical close prices approximate live ticks. Research only; never places orders.
"""
from datetime import timedelta
from v42_execution_phase4 import evaluate as phase4_evaluate
from v42_historical_backtest import exit_one
from v42_original_signal_bridge import aggregate


def weakness_at(rows, side, at, calculate_ema, calculate_rsi):
    c5 = aggregate(rows, 5, at)
    c15 = aggregate(rows, 15, at)
    if len(c5) < 30 or len(c15) < 30:
        return False, []
    # Synthetic forming candle is removed, as in the original signal code.
    close5 = [x['close'] for x in c5[:-1]]
    close15 = [x['close'] for x in c15[:-1]]
    last5 = close5[-1]
    ema9 = calculate_ema(close5, 9)
    ema20 = calculate_ema(close5, 20)
    ema15 = calculate_ema(close15, 20)
    rsi = calculate_rsi(close5, 14)
    if None in (ema9, ema20, ema15, rsi):
        return False, []
    momentum = (close5[-1] - close5[-4]) / close5[-4] * 100
    weaknesses = []
    if side == 'SHORT':
        if last5 > ema20:
            weaknesses.append('5m_close_above_ema20')
        if ema9 > ema20:
            weaknesses.append('5m_ema9_above_ema20')
        if close15[-1] > ema15:
            weaknesses.append('15m_close_above_ema20')
        if momentum > 0:
            weaknesses.append('15min_positive_momentum')
        if rsi > 52:
            weaknesses.append('5m_rsi_above_52')
    else:
        if last5 < ema20:
            weaknesses.append('5m_close_below_ema20')
        if ema9 < ema20:
            weaknesses.append('5m_ema9_below_ema20')
        if close15[-1] < ema15:
            weaknesses.append('15m_close_below_ema20')
        if momentum < 0:
            weaknesses.append('15min_negative_momentum')
        if rsi < 48:
            weaknesses.append('5m_rsi_below_48')
    return len(weaknesses) >= 2, weaknesses


def replay_with_trend(signal, candles, full_rows, ema, rsi):
    """Replay original-style TP protection; timeout only applies before TP1."""
    from v42_historical_backtest import execution_price, MAX_HOLD_MIN

    start = signal['time']
    future = [bar for bar in candles if bar[0] > start]
    if not future:
        return None, 'NO_FUTURE_BARS'
    if future[0][0] - start > timedelta(minutes=2):
        return None, 'ENTRY_GAP'

    first = future[0]
    raw = first[1]
    side = signal['side']
    direction = 1 if side == 'LONG' else -1
    pos = dict(
        symbol=signal['symbol'], side=side, score=signal['score'],
        regime=signal['regime'], signal_time=start.isoformat(),
        entry_time=first[0].isoformat(),
        entry_fill=execution_price(raw, side, True),
        remaining=1.0, net=0.0, legs=[]
    )
    tps = [raw * (1 + direction * x) for x in (0.02, 0.04, 0.08)]
    stop = raw * (1 - direction * 0.025)
    reason = 'SL_EXIT'
    hit1 = False
    hit2 = False
    prev = None
    end = first[0] + timedelta(minutes=MAX_HOLD_MIN)

    for t, o, h, l, c in future:
        # V4.2R1: four-hour timeout only applies if TP1 was never reached.
        # After TP1, keep replaying until a real exit or the history ends.
        if t > end and not hit1:
            break
        if prev is not None and t - prev != timedelta(minutes=1):
            return None, 'MISSING_1M_CANDLE'
        prev = t

        def touched(price):
            return l <= price if side == 'LONG' else h >= price

        def target(price):
            return h >= price if side == 'LONG' else l <= price

        # Conservative same-candle handling: active stop is checked first.
        if touched(stop):
            exit_one(pos, pos['remaining'], stop, t, reason)
            break

        if not hit1 and target(tps[0]):
            hit1 = True
            exit_one(pos, 0.35, tps[0], t, 'TP1')
            stop = raw
            reason = 'BREAKEVEN_EXIT'
            if touched(stop):
                exit_one(pos, pos['remaining'], stop, t, reason)
                break

        if not hit2 and target(tps[1]):
            if not hit1:
                hit1 = True
                exit_one(pos, 0.35, tps[0], t, 'TP1')
            hit2 = True
            exit_one(pos, 0.40, tps[1], t, 'TP2')
            stop = tps[0]
            reason = 'TP2_PROTECT_EXIT'
            if touched(stop):
                exit_one(pos, pos['remaining'], stop, t, reason)
                break

        if target(tps[2]):
            if not hit1:
                hit1 = True
                exit_one(pos, 0.35, tps[0], t, 'TP1')
            if not hit2:
                hit2 = True
                exit_one(pos, 0.40, tps[1], t, 'TP2')
            exit_one(pos, pos['remaining'], tps[2], t, 'TP3_EXIT')
            break

        # At each completed 5-minute boundary, check TP2 trend weakness.
        at = t + timedelta(minutes=1)
        if hit2 and int(at.timestamp()) % 300 == 0:
            fire, weakness = weakness_at(full_rows, side, at, ema, rsi)
            if fire:
                # Historical last close is a proxy for the live price.
                px = max(c, tps[0]) if side == 'LONG' else min(c, tps[0])
                exit_one(pos, pos['remaining'], px, t, 'TREND_EXIT')
                pos['trend_weakness'] = weakness
                break

        if at >= end and not hit1:
            exit_one(pos, pos['remaining'], c, t, 'TIMEOUT_EXIT')
            break

    # Do not fabricate a close on the last available historical candle.
    if pos['remaining'] > 1e-8:
        return None, 'INCOMPLETE_HISTORY'
    pos['exit_time'] = pos['legs'][-1]['time']
    pos['net_usdt'] = round(pos.pop('net'), 6)
    pos['exit_reason'] = pos['legs'][-1]['reason']
    pos.pop('remaining')
    return pos, None


def evaluate(market, signals, history, ema, rsi):
    """Use the phase-4 chronological gate with a scoped phase-5 replay."""
    import v42_execution_phase4 as phase4
    original_replay = phase4.replay

    def hooked(signal, candles):
        return replay_with_trend(
            signal, candles, history[signal['symbol']], ema, rsi
        )

    try:
        phase4.replay = hooked
        return phase4_evaluate(market, signals)
    finally:
        phase4.replay = original_replay
