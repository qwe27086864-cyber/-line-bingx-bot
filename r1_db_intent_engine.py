"""R1 persistent intent reservation. No exchange orders; staging only.

Usage:
 python r1_db_intent_engine.py --self-test
 python r1_db_intent_engine.py --db-check

This program NEVER changes live_risk_state and NEVER submits orders.
It does not accept a live reservation command: future execution must be reviewed.
"""
import argparse
import os
from decimal import Decimal, InvalidOperation

ALLOWED_STATES = {'PENDING','SUBMITTING','ACKNOWLEDGED','PROTECTED','CLOSING','UNKNOWN'}

def decimal_value(value):
    try:
        result = Decimal(str(value))
    except (ValueError, InvalidOperation, TypeError) as exc:
        raise ValueError('Invalid numeric value') from exc
    if not result.is_finite():
        raise ValueError('Nonfinite numeric value')
    return result

def validate_r1_intent(symbol, side, entry, stop, margin, leverage):
    if not isinstance(symbol,str) or not symbol.endswith('-USDT') or len(symbol) > 40:
        raise ValueError('Invalid symbol')
    if side not in ('LONG','SHORT'):
        raise ValueError('Invalid side')
    entry,stop,margin,leverage = map(decimal_value,(entry,stop,margin,leverage))
    if not (entry > 0 and stop > 0 and 0 < margin <= 10 and leverage == 8):
        raise ValueError('Invalid size or price')
    if not (stop < entry if side == 'LONG' else stop > entry):
        raise ValueError('Invalid stop direction')
    if abs(stop/entry-1) > Decimal('0.0251') or abs(stop/entry-1) < Decimal('0.0249'):
        raise ValueError('R1 stop not 2.5%')
    return True

def assess_database(conn):
    """One read-only transaction: verify ledger schema, locked state, and active intents.
    Does not assert stop coverage or grant trade authorization.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name IN ('live_order_intents','live_order_events','live_risk_state')")
        names = {row[0] for row in cur.fetchall()}
        if names != {'live_order_intents','live_order_events','live_risk_state'}:
            return {'status':'BLOCKED','reason':'MISSING_TABLES'}
        cur.execute('SELECT enabled, kill_switch FROM live_risk_state WHERE singleton=TRUE')
        row=cur.fetchone()
        if row != (False,True):
            return {'status':'BLOCKED','reason':'UNSAFE_RISK_STATE'}
        cur.execute("SELECT state, COUNT(*) FROM live_order_intents GROUP BY state")
        counts=dict(cur.fetchall())
        unknown = set(counts)-ALLOWED_STATES-{'CLOSED','REJECTED'}
        if unknown:
            return {'status':'BLOCKED','reason':'UNKNOWN_LEDGER_STATE'}
        return {'status':'LOCKED','active_intents':sum(counts.get(s,0) for s in ALLOWED_STATES),
                'trade_authorized':False,'exchange_stops_verified':False}

def db_check():
    if os.environ.get('LIVE_TRADING','false').lower()!='false':
        raise RuntimeError('LIVE_TRADING must be false')
    dsn=os.environ.get('DATABASE_URL')
    if not dsn:
        raise RuntimeError('DATABASE_URL missing')
    import psycopg2
    with psycopg2.connect(dsn,connect_timeout=10) as conn:
        conn.set_session(readonly=True)
        return assess_database(conn)

def self_test():
    assert validate_r1_intent('BTC-USDT','LONG','100','97.5','10',8)
    assert validate_r1_intent('BTC-USDT','SHORT','100','102.5','10',8)
    for args in [ ('BTC-USDT','LONG',100,102.5,10,8),
                  ('BTC-USDT','SHORT',100,97.5,10,8),
                  ('BTC-USDT','LONG',100,97.5,11,8),
                  ('BTC-USDT','LONG',100,97.5,10,9),
                  ('BTC-USDT','LONG',100,'NaN',10,8),
                  ('BTC-USDT','LONG',100,98,10,8) ]:
        try: validate_r1_intent(*args)
        except ValueError: pass
        else: raise AssertionError('Unsafe intent accepted')
    print('PASS: R1 staging ledger read-only gate and 8 intent validation cases')
    print('NOT LIVE READY: no live orders, no atomic reservation, no exchange stops or daily PnL enforcement')

if __name__=='__main__':
    p=argparse.ArgumentParser()
    g=p.add_mutually_exclusive_group(required=True)
    g.add_argument('--self-test',action='store_true')
    g.add_argument('--db-check',action='store_true')
    args=p.parse_args()
    if args.self_test:self_test()
    else:
        result=db_check()
        print(result)
        if result['status']!='LOCKED':raise SystemExit(1)
