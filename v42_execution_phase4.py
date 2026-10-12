#!/usr/bin/env python3
"""Offline V4.2R1 execution upgrade: protective stops and chronological entry risk gates.
Not a fully verified reproduction: 1m candle intrabar order and TREND exits remain approximate.
"""
from collections import defaultdict
from datetime import timedelta
from v42_historical_backtest import (ts, pnl, execution_price, exit_one, load_market,
    load_signals, stats, report, MAX_HOLD_MIN, COOLDOWN_MIN, MAX_OPEN,
    MAX_NEW_PER_SCAN, BALANCE, MARGIN)


def replay(signal, candles):
    start=signal['time']
    future=[b for b in candles if b[0]>start]
    if not future:return None,'NO_FUTURE_BARS'
    first=future[0]
    if first[0]-start>timedelta(minutes=2):return None,'ENTRY_GAP'
    entry_time=first[0]
    raw=first[1]
    side=signal['side']; direction=1 if side=='LONG' else -1
    pos=dict(symbol=signal['symbol'],side=side,score=signal['score'],regime=signal['regime'],
        signal_time=start.isoformat(),entry_time=entry_time.isoformat(),
        entry_fill=execution_price(raw,side,True),remaining=1.0,net=0.0,legs=[])
    tp=[raw*(1+direction*x) for x in (.02,.04,.08)]
    initial_stop=raw*(1-direction*.025)
    stop=initial_stop; reason='SL_EXIT'; hit1=False; hit2=False
    previous=None; end=entry_time+timedelta(minutes=MAX_HOLD_MIN)
    for t,o,h,l,c in future:
        if t>end:break
        if previous is not None and t-previous!=timedelta(minutes=1):return None,'MISSING_1M_CANDLE'
        previous=t
        def touched(px):return l<=px if side=='LONG' else h>=px
        def target(px):return h>=px if side=='LONG' else l<=px
        # Original legacy replay checks active stop before new targets, and
        # protects same-minute excursions conservatively once a TP is reached.
        if touched(stop):
            exit_one(pos,pos['remaining'],stop,t,reason);break
        if not hit1 and target(tp[0]):
            hit1=True;exit_one(pos,.35,tp[0],t,'TP1')
            stop=raw;reason='BREAKEVEN_EXIT'
            if touched(stop):
                exit_one(pos,pos['remaining'],stop,t,reason);break
        if not hit2 and target(tp[1]):
            if not hit1:
                hit1=True;exit_one(pos,.35,tp[0],t,'TP1')
            hit2=True;exit_one(pos,.40,tp[1],t,'TP2')
            stop=tp[0];reason='TP2_PROTECT_EXIT'
            if touched(stop):
                exit_one(pos,pos['remaining'],stop,t,reason);break
        if target(tp[2]):
            if not hit1:exit_one(pos,.35,tp[0],t,'TP1');hit1=True
            if not hit2:exit_one(pos,.40,tp[1],t,'TP2');hit2=True
            # Same-bar TP3 stop conflicts are conservatively handled above.
            exit_one(pos,pos['remaining'],tp[2],t,'TP3_EXIT');break
        if t>=end:
            exit_one(pos,pos['remaining'],c,t,'TIMEOUT_EXIT');break
    if pos['remaining']>1e-8:return None,'INCOMPLETE_HISTORY'
    pos['exit_time']=pos['legs'][-1]['time'];pos['net_usdt']=round(pos.pop('net'),6)
    pos['exit_reason']=pos['legs'][-1]['reason'];pos.pop('remaining')
    return pos,None


def evaluate(market,signals):
    trades=[]; rejects=defaultdict(int);last_entry={};per_scan=defaultdict(int)
    for s in sorted(signals,key=lambda x:(x['time'],-x['score'],x['symbol'])):
        moment=s['time']
        if per_scan[moment]>=MAX_NEW_PER_SCAN:rejects['SCAN_NEW_LIMIT']+=1;continue
        if s['symbol'] not in market:rejects['NO_CANDLES']+=1;continue
        if s['symbol'] in last_entry and moment-last_entry[s['symbol']]<timedelta(minutes=COOLDOWN_MIN):
            rejects['COOLDOWN']+=1;continue
        closed=[t for t in trades if ts(t['exit_time'])<=moment]
        open_now=[t for t in trades if ts(t['entry_time'])<=moment<ts(t['exit_time'])]
        if len(open_now)>=MAX_OPEN:rejects['MAX_OPEN']+=1;continue
        # Original risk gates use realized PnL, not future outcomes.
        realized=sum(t['net_usdt'] for t in closed)
        if BALANCE+realized-len(open_now)*MARGIN<MARGIN:
            rejects['INSUFFICIENT_BALANCE']+=1;continue
        daily=sum(t['net_usdt'] for t in closed if ts(t['exit_time']).date()==moment.date())
        if daily<=-20:rejects['DAILY_LOSS_LIMIT']+=1;continue
        recent_losses=sorted(closed,key=lambda t:t['exit_time'],reverse=True)[:4]
        if len(recent_losses)==4 and all(t['net_usdt']<=0 for t in recent_losses):
            if moment-ts(recent_losses[0]['exit_time'])<timedelta(hours=6):
                rejects['LOSS_STREAK_PAUSE']+=1;continue
        result,reason=replay(s,market[s['symbol']])
        if reason:rejects[reason]+=1;continue
        trades.append(result);last_entry[s['symbol']]=moment;per_scan[moment]+=1
    return trades,dict(rejects)
