#!/usr/bin/env python3
"""V4.2R1 offline historical research bootstrap; simulation only.
Fetches pinned public source + public OHLCV; NEVER sends orders or accesses DB.
No profitability claim. Historical spread scenarios are assumptions.
"""
import argparse, hashlib, json, sys
from datetime import datetime, timezone
from pathlib import Path
from v42_prepare_sources import retrieve
from v42_download_bingx_history import parse_utc, download, FIELDS, BASE
from v42_research_runner import main as run_study
import csv


def plan(symbols,start,end,work):
    t0,t1=parse_utc(start),parse_utc(end)
    if t1<=t0:raise ValueError('End must follow start')
    if t1>int(datetime.now(timezone.utc).timestamp()*1000):raise ValueError('Future end date prohibited')
    from datetime import timedelta
    days=(t1-t0)/86400000
    if days<4:raise ValueError('At least 4 days required to warm up 1h indicators and scan')
    if days>365:raise ValueError('For safety download up to 365 days per run; split larger jobs')
    if len(symbols)>30:raise ValueError('Limit symbols to 30 in one job')
    return {'mode':'OFFLINE_HISTORICAL_RESEARCH_ONLY','symbols':symbols,'start':start,'end':end,'days':round(days,2),'expected_1m_bars_per_symbol':int((t1-t0)/60000),'workdir':str(work),'dataset':'BingX public swap 1m OHLCV','spread':'HYPOTHETICAL scenarios, not observed historical bid/ask'}


def execute(argv=None):
    ap=argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--symbols',nargs='+',required=True)
    ap.add_argument('--start',required=True,help='UTC, e.g. 2026-06-01T00:00:00Z')
    ap.add_argument('--end',required=True)
    ap.add_argument('--workdir',default='v42_study')
    ap.add_argument('--spreads',nargs='+',type=float,default=[0.05,0.15,0.30])
    ap.add_argument('--plan-only',action='store_true')
    a=ap.parse_args(argv)
    symbols=[s.upper() for s in a.symbols]
    import re
    if len(set(symbols))!=len(symbols) or any(not re.fullmatch(r'[A-Z0-9]{2,25}-USDT',s) for s in symbols):
        raise ValueError('Invalid or repeated trading pair')
    work=Path(a.workdir).resolve(); p=plan(symbols,a.start,a.end,work)
    if not a.spreads or any(x<0 or x>2 for x in a.spreads):raise ValueError('Spreads must be between 0 and 2 percent')
    print(json.dumps(p,indent=2,ensure_ascii=False),flush=True)
    if a.plan_only:return p
    source=work/'original_sources'; data_dir=work/'ohlcv'; output=work/'results'
    if source.exists() and (source/'source_manifest.json').exists():
        manifest=json.loads((source/'source_manifest.json').read_text())
        for name,details in manifest['files'].items():
            if hashlib.sha256((source/name).read_bytes()).hexdigest()!=details['sha256']:
                raise RuntimeError(f'Pinned original source has changed: {name}')
        print('PINNED SOURCE: verified cached files',flush=True)
    else:
        retrieve(source)
        print('PINNED SOURCE: downloaded',flush=True)
    data_dir.mkdir(parents=True,exist_ok=True)
    t0,t1=parse_utc(a.start),parse_utc(a.end)
    expected=(t1-t0)//60000
    candles=[]
    for sym in symbols:
        path=data_dir/(sym+'.csv')
        if path.exists():
            import v42_validate_history as validator
            # We still validate cached data below, never silently overwrite it.
            with path.open(newline='',encoding='utf-8-sig') as f:
                cached=list(csv.DictReader(f))
            if len(cached)!=expected:raise RuntimeError(f'{path}: cached data row count differs; inspect manually')
            if cached[0]['time'].replace('+00:00','Z')!=datetime.fromtimestamp(t0/1000,timezone.utc).isoformat().replace('+00:00','Z'):
                raise RuntimeError(f'{path}: cached start mismatch')
            print('DATA CACHED:',sym,len(cached),flush=True)
        else:
            rows,missing=download(sym,t0,t1)
            if missing:raise RuntimeError(f'{sym}: missing {len(missing)} historical candles; refusing incomplete backtest')
            with path.open('w',newline='',encoding='utf-8') as f:
                writer=csv.DictWriter(f,fieldnames=FIELDS);writer.writeheader();writer.writerows(rows)
            print('DATA DOWNLOADED:',sym,len(rows),flush=True)
        candles.append({'symbol':sym,'file':path.name,'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
    (data_dir/'ohlcv_manifest.json').write_text(json.dumps({'dataset':'BingX public swap 1m OHLCV','endpoint':BASE,'start':a.start,'end':a.end,'candles':candles,'spread':'not observed'},indent=2),encoding='utf-8')
    args=['--ohlcv',str(data_dir),'--sources',str(source),'--out',str(output),'--spreads',*map(str,a.spreads)]
    result=run_study(args)
    print('SUCCESS: output',str(output/'SCENARIO_SUMMARY.json'),flush=True)
    return result

if __name__=='__main__':
    try:execute()
    except Exception as exc:
        print('STOPPED WITHOUT TRADING:',type(exc).__name__,str(exc),file=sys.stderr)
        sys.exit(1)
