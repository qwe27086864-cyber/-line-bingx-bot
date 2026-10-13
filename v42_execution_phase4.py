#!/usr/bin/env python3
"""Historical chronological risk gate for V4.2R1, simulation-only.

Preserves unfinished positions in capacity calculations without falsely
classifying partial realized legs as completed trades. No trading APIs.
"""
from collections import defaultdict
from datetime import timedelta
from v42_historical_backtest import (
    ts, pnl, execution_price, exit_one, load_market, load_signals, stats, report,
    MAX_HOLD_MIN, COOLDOWN_MIN, MAX_OPEN, MAX_NEW_PER_SCAN, BALANCE, MARGIN,
)


def replay(signal, candles):
    start = signal['time']
    future = [b for b in candles if b[0] > start]
    if not future:
        return None, 'NO_FUTURE_BARS'
    first = future[0]
    if first[0] - start > timedelta(minutes=2):
        return None, 'ENTRY_GAP'
    entry_time = first[0]
    raw = first[1]
    side = signal['side']
    direction = 1 if side == 'LONG' else -1
    pos = dict(symbol=signal['symbol'], side=side, score=signal['score'],
               regime=signal['regime'], signal_time=start.isoformat(),
               entry_time=entry_time.isoformat(),
               entry_fill=execution_price(raw, side, True),
               remaining=1.0, net=0.0, legs=[])
    tp = [raw * (1 + direction * x) for x in (.02, .04, .08)]
    stop = raw * (1 - direction * .025)
    reason = 'SL_EXIT'
    hit1 = hit2 = False
    previous = None
    # Original signal-time deadline; timeout applies only before TP1.
    end = start + timedelta(minutes=MAX_HOLD_MIN)
    for t, o, h, l, c in future:
        if previous is not None and t-previous != timedelta(minutes=1):
            return None, 'MISSING_1M_CANDLE'
        previous = t
        def touched(px): return l <= px if side == 'LONG' else h >= px
        def target(px): return h >= px if side == 'LONG' else l <= px
        if touched(stop):
            exit_one(pos, pos['remaining'], stop, t, reason)
            break
        if not hit1 and target(tp[0]):
            hit1 = True
            exit_one(pos, .35, tp[0], t, 'TP1')
            stop, reason = raw, 'BREAKEVEN_EXIT'
            if touched(stop):
                exit_one(pos, pos['remaining'], stop, t, reason)
                break
        if not hit2 and target(tp[1]):
            if not hit1:
                hit1 = True
                exit_one(pos, .35, tp[0], t, 'TP1')
            hit2 = True
            exit_one(pos, .40, tp[1], t, 'TP2')
            stop, reason = tp[0], 'TP2_PROTECT_EXIT'
            if touched(stop):
                exit_one(pos, pos['remaining'], stop, t, reason)
                break
        if target(tp[2]):
            if not hit1:
                hit1 = True
                exit_one(pos, .35, tp[0], t, 'TP1')
            if not hit2:
                hit2 = True
                exit_one(pos, .40, tp[1], t, 'TP2')
            exit_one(pos, pos['remaining'], tp[2], t, 'TP3_EXIT')
            break
        at = t + timedelta(minutes=1)
        if at >= end and not hit1:
            exit_one(pos, pos['remaining'], c, t, 'TIMEOUT_EXIT')
            break
    if pos['remaining'] > 1e-8:
        pos['status'] = 'OPEN'
        pos['last_seen_time'] = future[-1][0].isoformat()
        return pos, 'INCOMPLETE_HISTORY'
    pos['exit_time'] = pos['legs'][-1]['time']
    pos['net_usdt'] = round(pos.pop('net'), 6)
    pos['exit_reason'] = pos['legs'][-1]['reason']
    pos.pop('remaining')
    return pos, None


def evaluate(market, signals):
    trades = []
    pending = []
    rejects = defaultdict(int)
    last_entry = {}
    per_scan = defaultdict(int)
    for s in sorted(signals, key=lambda x: (x['time'], -x['score'], x['symbol'])):
        moment = s['time']
        if per_scan[moment] >= MAX_NEW_PER_SCAN:
            rejects['SCAN_NEW_LIMIT'] += 1; continue
        if s['symbol'] not in market:
            rejects['NO_CANDLES'] += 1; continue
        if s['symbol'] in last_entry and moment-last_entry[s['symbol']] < timedelta(minutes=COOLDOWN_MIN):
            rejects['COOLDOWN'] += 1; continue
        closed = [t for t in trades if ts(t['exit_time']) <= moment]
        open_now = [t for t in trades if ts(t['entry_time']) <= moment < ts(t['exit_time'])]
        open_now.extend(t for t in pending if ts(t['entry_time']) <= moment)
        if len(open_now) >= MAX_OPEN:
            rejects['MAX_OPEN'] += 1; continue
        realized = sum(t['net_usdt'] for t in closed)
        if BALANCE + realized - len(open_now)*MARGIN < MARGIN:
            rejects['INSUFFICIENT_BALANCE'] += 1; continue
        daily = sum(t['net_usdt'] for t in closed if ts(t['exit_time']).date() == moment.date())
        if daily <= -20:
            rejects['DAILY_LOSS_LIMIT'] += 1; continue
        recent_losses = sorted(closed, key=lambda t: t['exit_time'], reverse=True)[:4]
        if len(recent_losses) == 4 and all(t['net_usdt'] <= 0 for t in recent_losses):
            if moment - ts(recent_losses[0]['exit_time']) < timedelta(hours=6):
                rejects['LOSS_STREAK_PAUSE'] += 1; continue
        result, reason = replay(s, market[s['symbol']])
        if reason == 'INCOMPLETE_HISTORY' and result is not None:
            pending.append(result)
            rejects['INCOMPLETE_HISTORY'] += 1
        elif reason:
            rejects[reason] += 1
            continue
        else:
            trades.append(result)
        last_entry[s['symbol']] = moment
        per_scan[moment] += 1
    # Pending trades are intentionally not counted in completed-trade PF / win rate.
    rejects['UNFINISHED_POSITIONS'] = len(pending)
    return trades, dict(rejects)
