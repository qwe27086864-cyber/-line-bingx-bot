#!/usr/bin/env python3
"""Isolated historical execution backtester for V4.2R1 signal exports.

IMPORTANT: This is an execution and accounting harness, NOT the original entry
scanner. Feed historical signals produced WITHOUT future-bar leakage by the
original V4.2R1 logic. CSV timestamps UTC, candle times are 1-minute OPEN times.
No exchange credentials, trading, network access, or databases.
"""
import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from pathlib import Path

TP = [(0.02, 0.35, 'TP1'), (0.04, 0.40, 'TP2'), (0.08, 0.25, 'TP3')]
SL = 0.025
MAX_HOLD_MIN = 240
MAX_OPEN = 20
MAX_NEW_PER_SCAN = 3
COOLDOWN_MIN = 240
MARGIN = 10.0
LEVERAGE = 8.0
FEE_PER_SIDE = 0.0005
SLIPPAGE_PER_SIDE = 0.0003
BALANCE = 1000.0


def ts(s):
    s = str(s).strip().replace('Z', '+00:00')
    d = datetime.fromisoformat(s)
    if d.tzinfo is None:
        raise ValueError('Timestamp requires timezone: ' + s)
    return d.astimezone(timezone.utc)


def load_csv(path, required):
    with open(path, newline='', encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    if not rows or not required.issubset(set(rows[0])):
        raise ValueError(f'{path}: missing columns {sorted(required)} or empty file')
    return rows


def load_market(folder):
    bars = defaultdict(list)
    for path in sorted(Path(folder).glob('*.csv')):
        for r in load_csv(path, {'time','symbol','open','high','low','close'}):
            symbol = r['symbol'].strip().upper()
            o, h, l, c = (float(r[k]) for k in ('open','high','low','close'))
            if min(o,h,l,c) <= 0 or l > min(o,c) or h < max(o,c) or l>h:
                raise ValueError(f'{path}: invalid OHLC at {r["time"]}')
            bars[symbol].append((ts(r['time']),o,h,l,c))
    if not bars:
        raise ValueError('No candle CSV files found')
    for symbol, items in bars.items():
        items.sort()
        times=[b[0] for b in items]
        if len(times)!=len(set(times)):
            raise ValueError('Duplicate bars for ' + symbol)
    return bars


def load_signals(path):
    out=[]
    for r in load_csv(path, {'time','symbol','side','score'}):
        side=r['side'].strip().upper()
        if side not in ('LONG','SHORT'):
            raise ValueError('Side must be LONG or SHORT')
        score=int(r['score'])
        if score<85:
            continue
        out.append({'time':ts(r['time']),'symbol':r['symbol'].strip().upper(),
                    'side':side,'score':score,'regime':r.get('regime','unknown') or 'unknown'})
    return sorted(out,key=lambda x:(x['time'],-x['score'],x['symbol']))


def pnl(side, entry, exit_price):
    return (exit_price / entry - 1.0) * (1 if side=='LONG' else -1)


def execution_price(price, side, entering):
    # Adverse slippage on both entry and exit.
    sign=(1 if side=='LONG' else -1) * (1 if entering else -1)
    return price*(1 + sign*SLIPPAGE_PER_SIDE)


def exit_one(pos, frac, px, when, reason):
    fill=execution_price(px,pos['side'],False)
    # Net USDT on 10 USDT margin x 8 notional, including entry/exit fees for this fraction.
    net=MARGIN*LEVERAGE*frac*(pnl(pos['side'],pos['entry_fill'],fill)-2*FEE_PER_SIDE)
    pos['net']+=net
    pos['remaining']=round(pos['remaining']-frac,10)
    pos['legs'].append({'time':when.isoformat(),'reason':reason,'fraction':frac,'fill':fill,'net_usdt':round(net,6)})


def replay(sig, candles):
    start=sig['time']
    # Entry only at the NEXT candle open after signal timestamp. Do not look ahead.
    future=[b for b in candles if b[0] > start]
    if not future:
        return None,'NO_FUTURE_BARS'
    first=future[0]
    if first[0]-start > timedelta(minutes=2):
        return None,'ENTRY_GAP'
    entry_time=first[0]
    pos={'symbol':sig['symbol'],'side':sig['side'],'score':sig['score'],
         'regime':sig['regime'],'signal_time':start.isoformat(),
         'entry_time':entry_time.isoformat(),'entry_fill':execution_price(first[1],sig['side'],True),
         'remaining':1.0,'net':0.0,'legs':[]}
    raw_entry=first[1]
    side=sig['side']
    reached=[False,False,False]
    prev_time=None
    end=entry_time+timedelta(minutes=MAX_HOLD_MIN)
    for t,o,h,l,c in future:
        if t < entry_time:continue
        if t > end:break
        if prev_time is not None and t-prev_time!=timedelta(minutes=1):
            return None,'MISSING_1M_CANDLE'
        prev_time=t
        # Conservative same-bar ambiguity: SL wins before TP.
        stop=raw_entry*(1-SL if side=='LONG' else 1+SL)
        stop_hit = l<=stop if side=='LONG' else h>=stop
        if stop_hit:
            exit_one(pos,pos['remaining'],stop,t,'SL')
            break
        for i,(target,weight,label) in enumerate(TP):
            px=raw_entry*(1+target if side=='LONG' else 1-target)
            triggered=h>=px if side=='LONG' else l<=px
            if not reached[i] and triggered:
                reached[i]=True
                exit_one(pos,min(weight,pos['remaining']),px,t,label)
        if pos['remaining']<1e-9:break
        if t >= end:
            exit_one(pos,pos['remaining'],c,t,'TIMEOUT')
            break
    if pos['remaining']>1e-9:
        # Incomplete price history: never invent end-of-data exit.
        return None,'INCOMPLETE_HISTORY'
    pos['exit_time']=pos['legs'][-1]['time']
    pos['net_usdt']=round(pos.pop('net'),6)
    pos.pop('remaining')
    return pos,None


def evaluate(market,signals):
    trades=[]; rejects=defaultdict(int); last_entry={}; per_scan=defaultdict(int)
    for s in signals:
        key=(s['time'],s['symbol'])
        if per_scan[s['time']]>=MAX_NEW_PER_SCAN:
            rejects['SCAN_NEW_LIMIT']+=1; continue
        if s['symbol'] not in market:
            rejects['NO_CANDLES']+=1;continue
        recent=last_entry.get(s['symbol'])
        if recent and s['time']-recent<timedelta(minutes=COOLDOWN_MIN):
            rejects['COOLDOWN']+=1;continue
        active=sum(ts(t['entry_time'])<=s['time']<ts(t['exit_time']) for t in trades)
        if active>=MAX_OPEN:
            rejects['MAX_OPEN']+=1;continue
        trade, reason=replay(s,market[s['symbol']])
        if reason:
            rejects[reason]+=1;continue
        trades.append(trade);last_entry[s['symbol']]=s['time'];per_scan[s['time']]+=1
    return trades,dict(rejects)


def stats(rows):
    count=len(rows);wins=sum(t['net_usdt']>0 for t in rows)
    gains=sum(max(t['net_usdt'],0) for t in rows)
    losses=-sum(min(t['net_usdt'],0) for t in rows)
    return {'trades':count,'wins':wins,'win_rate_pct':round(100*wins/count,2) if count else None,
            'profit_factor':round(gains/losses,4) if losses else (None if not gains else 'inf'),
            'net_usdt':round(sum(t['net_usdt'] for t in rows),4)}


def report(trades,rejects):
    groups=defaultdict(list)
    for t in trades:
        groups[t['regime']].append(t)
    # Closed-trade only drawdown; not intratrade mark-to-market.
    equity=BALANCE;peak=equity;maxdd=0
    for t in sorted(trades,key=lambda t:t['exit_time']):
        equity+=t['net_usdt'];peak=max(equity,peak)
        maxdd=max(maxdd,(peak-equity)/peak*100)
    return {'strategy_source':'V4.2R1 historical signals (externally generated, not yet integrated)',
            'engine':'backtest-v0.1','assumptions':{'sl_priority_same_bar':True,
            'max_hold_minutes':MAX_HOLD_MIN,'max_open':MAX_OPEN,'max_new_per_timestamp':MAX_NEW_PER_SCAN,
            'margin_usdt':MARGIN,'leverage':LEVERAGE,'fee_per_side_pct':FEE_PER_SIDE*100,
            'slippage_per_side_pct':SLIPPAGE_PER_SIDE*100},'overall':stats(trades),
            'by_regime':{k:stats(v) for k,v in sorted(groups.items())},
            'by_side':{k:stats([t for t in trades if t['side']==k]) for k in ('LONG','SHORT')},
            'max_closed_equity_drawdown_pct':round(maxdd,2),'rejected':rejects}


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--candles',required=True,help='Directory containing 1m OHLC CSV files')
    p.add_argument('--signals',required=True,help='Time-stamped, no-lookahead historical signal CSV')
    p.add_argument('--out',default='backtest_results')
    args=p.parse_args()
    market=load_market(args.candles)
    signals=load_signals(args.signals)
    trades,rejects=evaluate(market,signals)
    r=report(trades,rejects)
    out=Path(args.out);out.mkdir(parents=True,exist_ok=True)
    (out/'report.json').write_text(json.dumps(r,indent=2,ensure_ascii=False),encoding='utf-8')
    with (out/'trades.json').open('w',encoding='utf-8') as f:
        json.dump(trades,f,indent=2,ensure_ascii=False)
    print(json.dumps(r,indent=2,ensure_ascii=False))

if __name__=='__main__':
    main()
