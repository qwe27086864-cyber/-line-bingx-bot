#!/usr/bin/env python3
"""Strict independent audit of historical 1m CSV input and its provenance.
Rejects absent/estimated spreads unless explicit observed-spread evidence provided.
"""
import argparse,csv,hashlib,json,math
from pathlib import Path
from collections import defaultdict
from datetime import timedelta
from v42_historical_backtest import ts

REQUIRED={'time','symbol','open','high','low','close','volume','spread_pct'}

def audit(directory, min_days=4, provenance=None):
    folder=Path(directory)
    paths=sorted(folder.glob('*.csv'))
    if not paths:raise ValueError('No CSV files')
    if provenance is None:raise ValueError('Historical spread source evidence required: --provenance manifest.json')
    doc=json.loads(Path(provenance).read_text(encoding='utf-8'))
    if doc.get('spread_method')!='observed':raise ValueError('Spreads must be genuinely observed, not estimated or synthetic')
    for field in ('exchange','spread_source','collection_method'):
        if not doc.get(field):raise ValueError(f'Missing provenance: {field}')
    entries=defaultdict(list); hashes=[]
    for path in paths:
        b=path.read_bytes()
        hashes.append({'file':path.name,'sha256':hashlib.sha256(b).hexdigest()})
        with path.open(newline='',encoding='utf-8-sig') as f:
            reader=csv.DictReader(f)
            if not REQUIRED.issubset(set(reader.fieldnames or [])):raise ValueError(f'Missing required fields: {path.name}')
            for line,r in enumerate(reader,2):
                try:
                    symbol=r['symbol'].strip().upper(); t=ts(r['time'])
                    vals={k:float(r[k]) for k in ('open','high','low','close','volume','spread_pct')}
                    if not symbol or any(not math.isfinite(x) for x in vals.values()):raise ValueError('invalid symbol/nonfinite')
                    if min(vals[k] for k in ('open','high','low','close'))<=0 or vals['volume']<0 or not 0<=vals['spread_pct']<=100:raise ValueError('invalid values')
                    if vals['low']>min(vals['open'],vals['close']) or vals['high']<max(vals['open'],vals['close']):raise ValueError('OHLC bounds')
                    if t.second or t.microsecond:raise ValueError('not 1 minute timestamp')
                    entries[symbol].append(t)
                except Exception as exc:raise ValueError(f'{path.name}:{line}: {exc}') from exc
    summary=[]
    for symbol,times in sorted(entries.items()):
        times.sort()
        for x,y in zip(times,times[1:]):
            if y-x!=timedelta(minutes=1):raise ValueError(f'{symbol}: duplicate or missing 1m candle at {x.isoformat()} => {y.isoformat()}')
        duration=(times[-1]-times[0]).total_seconds()/86400
        if duration<min_days:raise ValueError(f'{symbol}: only {duration:.2f} days; need >= {min_days}')
        summary.append({'symbol':symbol,'bars':len(times),'first':times[0].isoformat(),'last':times[-1].isoformat(),'days':round(duration,2)})
    return {'validated':True,'spread_provenance':doc,'files':hashes,'symbols':summary}

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--candles',required=True);p.add_argument('--provenance',required=True);p.add_argument('--min-days',type=float,default=4);p.add_argument('--out',default='history_audit.json');a=p.parse_args()
    result=audit(a.candles,a.min_days,a.provenance)
    Path(a.out).write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(result,indent=2,ensure_ascii=False))
