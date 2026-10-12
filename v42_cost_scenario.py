#!/usr/bin/env python3
"""Create explicitly ASSUMED-spread sensitivity datasets from OHLCV; NEVER claim observed spread.

This is a research-only bridge, not a verified historical execution backtest.
Keeps original source files untouched, never calls networks/trading APIs.
"""
import argparse,csv,hashlib,json,math
from pathlib import Path
from datetime import datetime,timezone
from collections import defaultdict
FIELDS=('time','symbol','open','high','low','close','volume','spread_pct')

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def run(source,dest,spread_pct,allow_gaps=False):
    spread_pct = float(spread_pct)
    if not math.isfinite(spread_pct) or not 0<=spread_pct<=2:
        raise ValueError('Assumed spread must be 0..2 percent')
    source,dest=Path(source).resolve(),Path(dest).resolve()
    if dest==source:raise ValueError('Output must not overwrite source')
    dest.mkdir(parents=True,exist_ok=True)
    manifest={'label':'RESEARCH SENSITIVITY ONLY — ASSUMED SPREAD, NOT OBSERVED','spread_assumption_pct':spread_pct,
              'do_not_treat_as_live_or_historical_spread':True,'files':[]}
    found=False
    for p in sorted(source.glob('*.csv')):
        if p.resolve().parent == dest:continue
        with p.open(newline='',encoding='utf-8-sig') as f:
            r=csv.DictReader(f)
            required=set(FIELDS)-{'spread_pct'}
            if not required.issubset(r.fieldnames or []):continue
            if 'spread_pct' in r.fieldnames:raise ValueError(f'{p.name} already has spread — refusing to replace observations')
            rows=list(r)
        if not rows:raise ValueError(f'No rows in {p}')
        times=defaultdict(list)
        for row in rows:
            t=datetime.fromisoformat(row['time'].replace('Z','+00:00'))
            if t.tzinfo is None:raise ValueError('Naive timestamp')
            t=t.astimezone(timezone.utc)
            symbol=row['symbol'].upper()
            o,h,l,c,v=[float(row[k]) for k in ('open','high','low','close','volume')]
            if not all(map(math.isfinite,(o,h,l,c,v))) or min(o,h,l,c)<=0 or v<0 or l>min(o,c) or h<max(o,c) or h<l:
                raise ValueError('Invalid OHLCV')
            times[symbol].append(t)
            row['spread_pct']=str(spread_pct)
        gaps=0
        for sym,stamps in times.items():
            stamps.sort()
            if len(set(stamps))!=len(stamps):raise ValueError('Duplicate timestamps')
            gaps+=sum((b-a).total_seconds()!=60 for a,b in zip(stamps,stamps[1:]))
        if gaps and not allow_gaps:raise ValueError(f'{p.name}: {gaps} discontinuities; fix history before testing')
        out=dest/p.name
        if out.resolve()==p.resolve():raise ValueError('Output would overwrite input')
        with out.open('w',newline='',encoding='utf-8') as f:
            w=csv.DictWriter(f,fieldnames=FIELDS,extrasaction='ignore');w.writeheader();w.writerows(rows)
        manifest['files'].append({'file':p.name,'rows':len(rows),'source_sha256':sha(p),'output_sha256':sha(out),'gaps':gaps})
        found=True
    if not found:raise ValueError('No valid OHLCV CSV files found')
    (dest/'ASSUMED_SPREAD_MANIFEST.json').write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    return manifest

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--source',required=True);p.add_argument('--out',required=True)
    p.add_argument('--spread-pct',type=float,required=True)
    args=p.parse_args();print(json.dumps(run(args.source,args.out,args.spread_pct),indent=2))
