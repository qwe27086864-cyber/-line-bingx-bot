#!/usr/bin/env python3
"""Phase 5: offline, causally timed TP2 trend weakness exit approximation.
Uses actual V4.2R1 weakness rules (5m/15m EMA, RSI, momentum), evaluated on
COMPLETED historical candles only at 5-minute boundaries. Exit uses most recent
completed 1m CLOSE as a historical ticker proxy. This is NOT tick-level parity.
"""
from datetime import timedelta
from v42_execution_phase4 import evaluate as phase4_evaluate
from v42_historical_backtest import exit_one
from v42_original_signal_bridge import aggregate


def weakness_at(rows, side, at, calculate_ema, calculate_rsi):
    c5=aggregate(rows,5,at);c15=aggregate(rows,15,at)
    if len(c5)<30 or len(c15)<30:return False,[]
    # aggregate supplies a synthetic forming candle, matching original [:-1].
    close5=[x['close'] for x in c5[:-1]]
    close15=[x['close'] for x in c15[:-1]]
    last5=close5[-1]
    ema9=calculate_ema(close5,9);ema20=calculate_ema(close5,20)
    ema15=calculate_ema(close15,20);rsi=calculate_rsi(close5,14)
    if None in (ema9,ema20,ema15,rsi):return False,[]
    momentum=(close5[-1]-close5[-4])/close5[-4]*100
    w=[]
    if side=='SHORT':
        if last5>ema20:w.append('5m_close_above_ema20')
        if ema9>ema20:w.append('5m_ema9_above_ema20')
        if close15[-1]>ema15:w.append('15m_close_above_ema20')
        if momentum>0:w.append('15min_positive_momentum')
        if rsi>52:w.append('5m_rsi_above_52')
    else:
        if last5<ema20:w.append('5m_close_below_ema20')
        if ema9<ema20:w.append('5m_ema9_below_ema20')
        if close15[-1]<ema15:w.append('15m_close_below_ema20')
        if momentum<0:w.append('15min_negative_momentum')
        if rsi<48:w.append('5m_rsi_below_48')
    return len(w)>=2,w


def replay_with_trend(signal,candles,full_rows,ema,rsi):
    # Use phase4 original execution machinery with a synthetic immutable 
    # stop triggered at minute after the trend decision; since phase4 cannot
    # interrupt its loop, this standalone replay follows the same stop-first flow.
    from v42_historical_backtest import execution_price,MAX_HOLD_MIN
    start=signal['time'];future=[b for b in candles if b[0]>start]
    if not future:return None,'NO_FUTURE_BARS'
    if future[0][0]-start>timedelta(minutes=2):return None,'ENTRY_GAP'
    first=future[0];raw=first[1];side=signal['side'];direction=1 if side=='LONG' else -1
    pos=dict(symbol=signal['symbol'],side=side,score=signal['score'],regime=signal['regime'],
             signal_time=start.isoformat(),entry_time=first[0].isoformat(),
             entry_fill=execution_price(raw,side,True),remaining=1.,net=0.,legs=[])
    tps=[raw*(1+direction*x) for x in (.02,.04,.08)]
    stop=raw*(1-direction*.025);reason='SL_EXIT';hit1=hit2=False
    prev=None;end=first[0]+timedelta(minutes=MAX_HOLD_MIN)
    for t,o,h,l,c in future:
        if t>end:break
        if prev is not None and t-prev!=timedelta(minutes=1):return None,'MISSING_1M_CANDLE'
        prev=t
        touched=lambda p: l<=p if side=='LONG' else h>=p
        target=lambda p: h>=p if side=='LONG' else l<=p
        if touched(stop):exit_one(pos,pos['remaining'],stop,t,reason);break
        if not hit1 and target(tps[0]):
            hit1=True;exit_one(pos,.35,tps[0],t,'TP1');stop=raw;reason='BREAKEVEN_EXIT'
            if touched(stop):exit_one(pos,pos['remaining'],stop,t,reason);break
        if not hit2 and target(tps[1]):
            if not hit1:hit1=True;exit_one(pos,.35,tps[0],t,'TP1')
            hit2=True;exit_one(pos,.40,tps[1],t,'TP2');stop=tps[0];reason='TP2_PROTECT_EXIT'
            if touched(stop):exit_one(pos,pos['remaining'],stop,t,reason);break
        if target(tps[2]):
            if not hit1:hit1=True;exit_one(pos,.35,tps[0],t,'TP1')
            if not hit2:hit2=True;exit_one(pos,.40,tps[1],t,'TP2')
            exit_one(pos,pos['remaining'],tps[2],t,'TP3_EXIT');break
        # At cron boundary t+1m divisible by 5, completed candle is now known.
        at=t+timedelta(minutes=1)
        if hit2 and int(at.timestamp())%300==0:
            fire,w=weakness_at(full_rows,side,at,ema,rsi)
            if fire:
                # Original: LONG max(current price, TP1), SHORT min(current, TP1)
                px=max(c,tps[0]) if side=='LONG' else min(c,tps[0])
                exit_one(pos,pos['remaining'],px,t,'TREND_EXIT')
                pos['trend_weakness']=w
                break
        if at>=end:
            exit_one(pos,pos['remaining'],c,t,'TIMEOUT_EXIT');break
    if pos['remaining']>1e-8:return None,'INCOMPLETE_HISTORY'
    pos['exit_time']=pos['legs'][-1]['time'];pos['net_usdt']=round(pos.pop('net'),6)
    pos['exit_reason']=pos['legs'][-1]['reason'];pos.pop('remaining')
    return pos,None


def evaluate(market,signals,history,ema,rsi):
    """Use same chronological gate as phase4, replacing its replay through a scoped proxy."""
    import v42_execution_phase4 as p
    old=p.replay
    def hooked(signal,candles):
        return replay_with_trend(signal,candles,history[signal['symbol']],ema,rsi)
    try:
        p.replay=hooked
        return phase4_evaluate(market,signals)
    finally:
        p.replay=old
