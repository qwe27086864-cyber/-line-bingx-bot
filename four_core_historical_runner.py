"""Run four independent historical adapters only with genuine historical inputs.

Place in repository root alongside existing *_historical_adapter.py files.
No historical data is fabricated; absent or insufficient data is reported as BLOCKED.
"""
from __future__ import annotations
import argparse
import csv
import json
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

CONFIG = {
    'AS1': ('as1_historical_adapter.py', 'as1_events.csv', '--events', ['timestamp','event','bid','ask','trade_price','trade_qty','buyer_maker']),
    'BF1': ('bf1_historical_adapter.py', 'bf1_snapshots.csv', '--snapshots', ['timestamp','symbol','spot_bid','spot_ask','perp_bid','perp_ask','funding_rate','next_funding_time_ms']),
    'MEME1': ('meme1_historical_adapter.py', 'meme1_snapshots.csv', '--snapshots', ['timestamp','pool_address','pool_created_at','mint_address','price_usd','liquidity_usd','fdv_usd','volume_h1_usd','buys_m5','sells_m5','price_change_m5_pct','rug_score']),
    'MEME2': ('meme2_historical_adapter.py', 'meme2_events.csv', '--events', ['timestamp','wallet','side','mint_address','price_usd','liquidity_usd']),
}

def parse_time(s):
    t=datetime.fromisoformat(s.replace('Z','+00:00'))
    return t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc)

def inspect(path, fields, start, end):
    if not path.exists(): return None, 'MISSING_INPUT'
    with path.open(newline='',encoding='utf-8-sig') as f:
        reader=csv.DictReader(f)
        missing=sorted(set(fields)-set(reader.fieldnames or []))
        if missing:return None, 'MISSING_COLUMNS: '+','.join(missing)
        count=0; earliest=None; latest=None; invalid=0; outside=0
        for row in reader:
            try:t=parse_time(row['timestamp'])
            except (ValueError,KeyError,TypeError):invalid+=1;continue
            if not start<=t<end:outside+=1;continue
            count+=1
            earliest=t if earliest is None or t<earliest else earliest
            latest=t if latest is None or t>latest else latest
    if invalid:return None,f'INVALID_TIMESTAMPS: {invalid}'
    if count==0:return None,'NO_EVENTS_IN_WINDOW'
    # Full-window coverage is essential; input with only one day must not be labeled 30d.
    if (earliest-start)>timedelta(days=1) or (end-latest)>timedelta(days=1):
        return None,f'INSUFFICIENT_WINDOW_COVERAGE first={earliest.isoformat()} last={latest.isoformat()}'
    return {'rows':count,'first':earliest.isoformat(),'last':latest.isoformat(),'outside_window_rows':outside},None

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--data-dir',default='data')
    p.add_argument('--end-iso',required=True)
    p.add_argument('--days',type=int,default=30)
    p.add_argument('--report',default='data/four_core_historical_report.json')
    a=p.parse_args()
    if a.days<=0:p.error('--days must be positive')
    end=parse_time(a.end_iso);start=end-timedelta(days=a.days)
    d=Path(a.data_dir);d.mkdir(parents=True,exist_ok=True)
    report={'start':start.isoformat(),'end':end.isoformat(),'days':a.days,'cores':{},'warning':'Input coverage checks are necessary but not proof of event completeness or point-in-time integrity. Verify source provenance, missing intervals, and execution realism before trusting PnL.'}
    for core,(script,name,flag,fields) in CONFIG.items():
        path=d/name;meta,error=inspect(path,fields,start,end)
        if error:
            report['cores'][core]={'status':'BLOCKED','reason':error,'input':str(path)}
            continue
        # Prevent accidental mixing of adjacent historical windows: make a scoped file.
        scoped=d/f'{core.lower()}_window_input.csv'
        with path.open(newline='',encoding='utf-8-sig') as f,scoped.open('w',newline='',encoding='utf-8') as o:
            rd=csv.DictReader(f);wr=csv.DictWriter(o,fieldnames=rd.fieldnames);wr.writeheader()
            for row in rd:
                try:t=parse_time(row['timestamp'])
                except (ValueError,TypeError,KeyError):continue
                if start<=t<end:wr.writerow(row)
        out=d/f'{core.lower()}_core_outcomes.csv'
        cmd=[sys.executable,script,flag,str(scoped),'--out',str(out)]
        result=subprocess.run(cmd,capture_output=True,text=True)
        if result.returncode:
            report['cores'][core]={'status':'FAILED','reason':result.stderr[-1600:],'input':str(path),'coverage':meta}
            continue
        with out.open(newline='',encoding='utf-8') as f: trades=list(csv.DictReader(f))
        report['cores'][core]={'status':'RAN_HISTORICAL_UNVALIDATED','coverage':meta,'outcomes':len(trades),'output':str(out),'stdout':result.stdout[-800:]}
    dest=Path(a.report);dest.parent.mkdir(parents=True,exist_ok=True)
    dest.write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps(report,indent=2,ensure_ascii=False))
    return 0 if all(v['status'].startswith('RAN_') for v in report['cores'].values()) else 2

if __name__=='__main__':raise SystemExit(main())
