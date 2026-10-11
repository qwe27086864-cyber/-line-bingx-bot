"""V5 SAFE offline contract & logic checks. NO DATABASE CALLS, NO ORDERS.

Upload to V5 and run: python v5_integration_check.py
PASS is only a local static/unit check, NOT PostgreSQL integration verification.
"""
from __future__ import annotations
import ast
import os
import sys
import types
from pathlib import Path
from datetime import datetime, timezone, timedelta

BASE = Path(__file__).resolve().parent
FILES = ('v5_recorder.py', 'v5_market.py', 'v5_manager.py', 'v5_shadow.py')

def check(condition, label):
    if not condition:
        raise AssertionError(label)
    print('PASS:', label)

def main():
    os.environ['LIVE_TRADING'] = 'false'
    source = {}
    trees = {}
    for filename in FILES:
        path = BASE / filename
        check(path.exists(), f'{filename} exists')
        source[filename] = path.read_text(encoding='utf-8')
        trees[filename] = ast.parse(source[filename], filename=filename)
        check(True, f'{filename} parses')
        check('bingx_private_request' not in source[filename], f'{filename} has no private order API')
    # Allow pure offline tests on machines without psycopg2; NEVER connect DB.
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        stub=types.ModuleType('psycopg2')
        extras=types.ModuleType('psycopg2.extras')
        extras.Json=lambda value: value
        stub.extras=extras
        sys.modules['psycopg2']=stub
        sys.modules['psycopg2.extras']=extras
        print('INFO: psycopg2 absent; using import-only stub, database NOT tested')
    import v5_recorder as recorder
    import v5_market as market
    import v5_manager as manager
    import v5_shadow as shadow
    check(manager.VERSION == 'V5' and manager.MAX_OPEN == 30, 'V5 only / max 30 positions')
    check(manager.MAX_NEW_PER_SCAN == 3, 'max three entries per scan')
    check(manager.STOP_PCT == 2.5, '2.5% hard stop')
    check(manager.LEVERAGE == 8 and manager.MARGIN_USDT == 10, '8x and 10U simulation')
    check('v5_sim_trades' in manager.DDL and 'REFERENCES v5_runs(run_id)' in manager.DDL, 'separate V5 trade schema')
    check('CREATE TABLE IF NOT EXISTS v5_runs' in source['v5_recorder.py'], 'recorder defines v5_runs')
    check('CREATE TABLE IF NOT EXISTS v5_events' in source['v5_recorder.py'], 'recorder defines v5_events')
    check('CREATE TABLE IF NOT EXISTS v5_position_snapshots' in source['v5_recorder.py'], 'recorder defines v5_position_snapshots')
    check('CREATE TABLE IF NOT EXISTS v5_line_signals' in source['v5_recorder.py'], 'LINE signal schema exists')
    check('CREATE TABLE IF NOT EXISTS v5_shadow_events' in source['v5_recorder.py'], 'shadow evidence schema exists')
    check(all(callable(getattr(recorder,n,None)) for n in ('init_v5_records','register_run','append_event','append_market_snapshot','append_position_snapshot','append_shadow_event','append_line_signal','append_line_relation_event')), 'recorder integration API callable')
    for direction in ('LONG','SHORT'):
        levels=manager._levels(100.0,direction)
        check((levels[3]<100<levels[0]<levels[1]<levels[2]) if direction=='LONG' else (levels[2]<levels[1]<levels[0]<100<levels[3]), f'{direction} TP/SL ordering')
    now=datetime(2026,10,11,3,0,tzinfo=timezone.utc)
    result=market.classify_market(lambda symbol,interval,limit:[], observed_at=now)
    check(result['regime']=='UNKNOWN' and result['missing'], 'missing market candles marked UNKNOWN')
    entry=now-timedelta(minutes=20)
    early=now
    common=dict(symbol='DEMO-USDT',side='LONG',entry_price=100,entry_time=entry,
                early_exit_time=early,early_exit_price=99.5,tp1=102,tp2=104,tp3=108,
                sl=97.5,tp1_hit=False,tp2_hit=False)
    start_ms=int(early.timestamp()*1000)
    bars=[{'time':start_ms,'open':99.5,'high':99.7,'low':97.3,'close':97.4}]
    got=shadow.compare_early_exit(**common,completed_bars=bars)
    check(got['status']=='RESOLVED' and got['baseline_exit_reason']=='SL_EXIT' and got['delta_early_minus_hold_pct']>0, 'early exit protects vs future SL')
    future=[
        {'time':start_ms,'open':99.5,'high':101.5,'low':99.4,'close':101.0},
        {'time':start_ms+60_000,'open':101.0,'high':102.5,'low':100.5,'close':102.2},
        {'time':start_ms+120_000,'open':102.2,'high':104.5,'low':102.1,'close':104.2},
        {'time':start_ms+180_000,'open':104.2,'high':108.5,'low':104.1,'close':108.1},
    ]
    got=shadow.compare_early_exit(**common,completed_bars=future)
    check(got['status']=='RESOLVED' and got['baseline_exit_reason']=='TP3_EXIT' and got['delta_early_minus_hold_pct']<0, 'early exit missed TP3')
    got=shadow.compare_early_exit(**common,completed_bars=[])
    check(got['status']=='INCOMPLETE', 'missing shadow candles are not guessed')
    print('\nOFFLINE CHECKS PASSED. DB integration, market calibration, LINE receiver, and portfolio replay NOT YET TESTED.')

if __name__=='__main__':
    main()
