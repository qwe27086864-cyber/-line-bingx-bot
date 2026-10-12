#!/usr/bin/env python3
"""Offline, reproducible V4.2R1 research workflow (NOT validated trading performance).

Accepts historical OHLCV CSV and pinned original sources. Generates separate
assumed-spread scenarios, independently evaluating signals and trades per case.
No live APIs, database operations, or trading endpoints are called.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
from datetime import datetime, timezone
from v42_cost_scenario import run as add_assumed_spread
from v42_run_offline_phase5 import run as run_backtest

ORIGINAL_NAMES=('original_app.py','original_scanner_job.py')

def check_source(path, needed):
    import ast
    content=Path(path).read_text(encoding='utf-8')
    tree=ast.parse(content)
    funcs={n.name for n in tree.body if isinstance(n,ast.FunctionDef)}
    absent=set(needed)-funcs
    if absent: raise ValueError(f'{path}: missing V4.2 functions {sorted(absent)}')
    return hashlib.sha256(content.encode()).hexdigest()

def main(argv=None):
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--ohlcv',required=True,help='Directory containing historical one-minute OHLCV CSVs, without spread_pct')
    p.add_argument('--sources',required=True,help='Directory containing original_app.py and original_scanner_job.py')
    p.add_argument('--out',required=True)
    p.add_argument('--spreads',nargs='+',type=float,default=[0.05,0.15,0.30])
    a=p.parse_args(argv)
    spreads=list(dict.fromkeys(a.spreads))
    if not spreads or any(not math.isfinite(n) or n<0 or n>2 for n in spreads):raise ValueError('Spreads must be 0..2%')
    source=Path(a.sources)
    app=source/ORIGINAL_NAMES[0]; scanner=source/ORIGINAL_NAMES[1]
    hashes={str(app.name):check_source(app,{'analyze_fast_5m','analyze_15m_detail','confirm_1h','choose_strategy'}),
            str(scanner.name):check_source(scanner,{'build_radar_candidates'})}
    inp=Path(a.ohlcv).resolve(); dest=Path(a.out).resolve()
    if inp==dest or inp in dest.parents or dest in inp.parents:
        raise ValueError('Output and input must be separate directory trees')
    # Preflight source files and CSV, before writing any output.
    paths=sorted(inp.glob('*.csv'))
    if not paths:raise ValueError('No input OHLCV CSV files')
    if any('spread_pct' in p.read_text(encoding='utf-8-sig').splitlines()[0].split(',') for p in paths):
        raise ValueError('Input already has spread_pct; do not relabel observed quotes as assumed')
    dest.mkdir(parents=True,exist_ok=True)
    results=[]
    for spread in spreads:
        slug=f'{spread:.4f}'.replace('.','p')
        adjusted=dest/f'input_assumed_spread_{slug}'
        manifest=add_assumed_spread(inp,adjusted,spread)
        report_dir=dest/f'backtest_spread_{slug}'
        result=run_backtest(str(app),str(scanner),str(adjusted),str(report_dir))
        results.append({'assumed_spread_pct':spread,'dataset_manifest':str(adjusted/'ASSUMED_SPREAD_MANIFEST.json'),
                        'report_path':str(report_dir/'report.json'),
                        'signals_generated':result.get('signal_diagnostics',{}).get('qualified',0),
                        'overall':result.get('overall',{}),
                        'source_sha256':hashes,
                        'source_type':'OHLCV+ASSUMED_SPREAD; research only',
                        'data_files':manifest['files']})
    summary={'status':'RESEARCH_ONLY_UNVERIFIED','source_hashes':hashes,'scenarios':results,
             'limitations':['Historical ticker bid/ask spread not observed',
                            'Radar volume and 24h change derived from 1m bars, not archived ticker snapshots',
                            'Unverified parity with real V4.2R1 forward trade results',
                            'No demonstration of profitability or original PF 2.6']}
    (dest/'SCENARIO_SUMMARY.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'status':summary['status'],'scenarios':len(results),
                      'summary':str(dest/'SCENARIO_SUMMARY.json')},ensure_ascii=False))
    return summary

if __name__=='__main__':main()
