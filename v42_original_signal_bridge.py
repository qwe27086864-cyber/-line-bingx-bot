#!/usr/bin/env python3
"""Historical signal generator using UNMODIFIED V4.2R1 function ASTs.

Requires original `app.py` and `scanner_job.py` downloaded from GitHub
branch backup-v42r1-best. Input: complete 1m OHLCV including `volume`,
plus a `spread_pct` column (actual observed historical spread), all UTC.
Each emitted signal uses only bars closed at or before its scan timestamp.
This does NOT recreate the historical ticker spread or exact exchange execution.
No internet, database, exchange keys, or trading functions are executed.
"""
import argparse
import ast
import csv
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import math

from v42_historical_backtest import ts

APP_FUNCS={'calculate_ema','calculate_rsi','_fast_direction_pack','analyze_fast_5m',
           'analyze_15m_detail','confirm_1h','_score_tier_allows','choose_strategy'}
RADAR_FUNCS={'build_radar_candidates'}
BASE={'SCANNER_MIN_TREND_SCORE':85,'SCANNER_MIN_BREAKOUT_SCORE':85,
      'V42_MAIN_SCORE':90,'V42_CONFIRM_SCORE':85,'V42_ENABLE_80_84':False,
      'RADAR_MIN_5M_MOVE':0.6,'RADAR_MAX_CANDIDATES':12,
      'RADAR_MIN_24H_QUOTE_VOLUME':50000,'RADAR_MAX_SPREAD_PCT':2.0}


def extract_exact(path, allowed, ns):
    source=Path(path).read_text(encoding='utf-8')
    tree=ast.parse(source,path)
    selected=[node for node in tree.body if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)) and node.name in allowed]
    names={n.name for n in selected}
    if names != allowed:
        raise RuntimeError(f'{path} missing original functions: {sorted(allowed-names)}')
    for node in selected:
        if node.decorator_list: raise RuntimeError(f'Unexpected decorator: {node.name}')
    code=compile(ast.Module(body=selected,type_ignores=[]),str(path),'exec')
    exec(code,ns,ns)


def load_history(folder):
    data=defaultdict(list)
    for path in Path(folder).glob('*.csv'):
        with path.open(newline='',encoding='utf-8-sig') as f:
            reader=csv.DictReader(f)
            required={'time','symbol','open','high','low','close','volume','spread_pct'}
            if not required.issubset(set(reader.fieldnames or [])):
                raise ValueError(f'{path} requires {sorted(required)}')
            for row in reader:
                sym=row['symbol'].strip().upper()
                bar={'time':ts(row['time']),'symbol':sym}
                for k in ('open','high','low','close','volume','spread_pct'):
                    bar[k]=float(row[k])
                if not all(math.isfinite(v) for k,v in bar.items() if isinstance(v,float)):
                    raise ValueError('Nonfinite input')
                if min(bar[k] for k in ('open','high','low','close'))<=0 or bar['volume']<0 or not 0<=bar['spread_pct']<=100:
                    raise ValueError('Invalid OHLCV/spread')
                if bar['low']>min(bar['open'],bar['close']) or bar['high']<max(bar['open'],bar['close']):
                    raise ValueError('Invalid candle bounds')
                data[sym].append(bar)
    if not data: raise ValueError('No history CSV')
    for sym,rows in data.items():
        rows.sort(key=lambda x:x['time'])
        if len({r['time'] for r in rows})!=len(rows):raise ValueError('Duplicate time: '+sym)
    return data


def aggregate(rows,minutes,asof):
    """Return only complete period bars, and a dummy forming candle as original API expected."""
    sec=minutes*60
    buckets=defaultdict(list)
    for b in rows:
        t=b['time']
        if t+timedelta(minutes=1)>asof:continue
        idx=int(t.timestamp())//sec
        # Exclude partial multi-minute periods at the current asof time.
        if (idx+1)*sec>int(asof.timestamp()):continue
        buckets[idx].append(b)
    result=[]
    for idx,items in sorted(buckets.items()):
        if len(items)!=minutes:continue
        if items[0]['time'].timestamp()!=idx*sec:continue
        if any(items[j+1]['time']-items[j]['time']!=timedelta(minutes=1) for j in range(len(items)-1)):continue
        result.append({'time':idx*sec*1000,'open':items[0]['open'],
                       'high':max(x['high'] for x in items),'low':min(x['low'] for x in items),
                       'close':items[-1]['close'],'volume':sum(x['volume'] for x in items)})
    if not result:return []
    # Original code removes candles[-1] as the still-forming candle.
    # Synthetic sentinel holds last close, contains no future data.
    last=result[-1]
    return result+[{'time':int(asof.timestamp()*1000),'open':last['close'],'high':last['close'],
                    'low':last['close'],'close':last['close'],'volume':0}]


def build_signals(history,app_path,scanner_path,start=None,end=None):
    ns={'__builtins__':__builtins__,'datetime':datetime,'timezone':timezone,**BASE}
    extract_exact(app_path,APP_FUNCS,ns)
    extract_exact(scanner_path,RADAR_FUNCS,ns)
    out=[];diagnostics=defaultdict(int)
    first=min(row[0]['time'] for row in history.values() if row)
    last=max(row[-1]['time'] for row in history.values() if row)
    begin=start or first+timedelta(hours=61)
    finish=end or last
    epoch=int(begin.timestamp());epoch+=(-epoch)%300
    previous=None
    ticks={sym:{r['time']:i for i,r in enumerate(rows)} for sym,rows in history.items()}
    while epoch<=int(finish.timestamp()):
        at=datetime.fromtimestamp(epoch,tz=timezone.utc)
        current=[]
        for sym,rows in history.items():
            # Strictly require the minute immediately preceding the scan timestamp.
            idx=ticks[sym].get(at-timedelta(minutes=1))
            if idx is None or idx<1440:continue
            recent=rows[idx-1439:idx+1]
            if len(recent)!=1440 or recent[-1]['time']-recent[0]['time']!=timedelta(minutes=1439):continue
            px=recent[-1]['close']; old=recent[0]['close']
            current.append({'symbol':sym,'price':px,'quote_volume':sum(b['volume']*b['close'] for b in recent),
                            'change_24h':(px/old-1)*100,'spread_pct':recent[-1]['spread_pct']})
        diagnostics['scans']+=1
        if previous is not None:
            class Clock:
                @staticmethod
                def now(tz=None):return at
            ns['datetime']=Clock
            radar=ns['build_radar_candidates'](current,previous)
            diagnostics['radar_candidates']+=len(radar)
            qualified=[]
            for r in radar:
                symbol=r['symbol'];side=r['side'];hist=history[symbol]
                ns['get_klines']=lambda s,interval,limit=100: aggregate(hist,{'5m':5,'15m':15,'1h':60}[interval],at)[-limit:]
                fast=ns['analyze_fast_5m'](symbol,side_hint=side)
                if not fast:diagnostics['fast_missing']+=1;continue
                detail=ns['analyze_15m_detail'](fast)
                if not detail:diagnostics['detail_missing']+=1;continue
                ns['confirm_1h'](detail)
                chosen=ns['choose_strategy'](detail)
                if not chosen:diagnostics['strategy_rejected']+=1;continue
                qualified.append((chosen,r))
            qualified.sort(key=lambda q:(q[0]['score'],q[0]['strategy']=='BREAKOUT',abs(q[1]['radar_move'])),reverse=True)
            # Do NOT pre-truncate to 3 here: execution engine applies capacity and cooldown.
            for picked,r in qualified:
                out.append({'time':at.isoformat(),'symbol':r['symbol'],'side':r['side'],
                            'score':picked['score'],'regime':'unknown','strategy':picked['strategy']})
            diagnostics['qualified']+=len(qualified)
        previous={r['symbol']:{'price':r['price'],'captured_at':at} for r in current}
        epoch+=300
    return out,dict(diagnostics)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--candles',required=True)
    p.add_argument('--original-app',required=True)
    p.add_argument('--original-scanner',required=True)
    p.add_argument('--out',required=True)
    p.add_argument('--start',help='UTC ISO timestamp')
    p.add_argument('--end',help='UTC ISO timestamp')
    args=p.parse_args()
    signals,diag=build_signals(load_history(args.candles),args.original_app,args.original_scanner,
                               ts(args.start) if args.start else None,ts(args.end) if args.end else None)
    target=Path(args.out);target.parent.mkdir(parents=True,exist_ok=True)
    with target.open('w',newline='',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=['time','symbol','side','score','regime','strategy'])
        writer.writeheader();writer.writerows(signals)
    print({'signals':len(signals),'diagnostics':diag,'out':str(target)})

if __name__=='__main__':main()
