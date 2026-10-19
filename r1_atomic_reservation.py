"""R1 staging: atomic Postgres signal reservation. NEVER submits exchange orders.

Usage: python r1_atomic_reservation.py --self-test
       python r1_atomic_reservation.py --reserve --signal-id EXAMPLE --symbol BTC-USDT --side LONG --entry 70000 --stop 68250

All writes require LIVE_TRADING=false. Creates only PENDING intent; risk switch remains OFF.
This is NOT a live executor: no exchange fills, protective stops, realized PnL, or recovery.
"""
import argparse
import os
import re
from decimal import Decimal, InvalidOperation

ACTIVE = ('PENDING','SUBMITTING','ACKNOWLEDGED','PROTECTED','CLOSING','UNKNOWN')


def positive(value):
    try:
        n = Decimal(str(value))
        if not n.is_finite() or n <= 0: raise ValueError('invalid positive number')
        return n
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError('invalid positive number') from exc


def validate(signal_id, symbol, side, entry, stop):
    if not isinstance(signal_id,str) or not 1 <= len(signal_id) <= 200:
        raise ValueError('invalid signal ID')
    if not isinstance(symbol,str) or not re.fullmatch(r'[A-Z0-9]{2,25}-USDT',symbol):
        raise ValueError('invalid symbol')
    if side not in ('LONG','SHORT'): raise ValueError('invalid side')
    e,s = positive(entry),positive(stop)
    if side == 'LONG' and s != e*Decimal('0.975'):
        raise ValueError('R1 long SL must be 2.5% below entry')
    if side == 'SHORT' and s != e*Decimal('1.025'):
        raise ValueError('R1 short SL must be 2.5% above entry')
    return e,s


def reserve(dsn, signal_id, symbol, side, entry, stop):
    """Serialize staging reservations. Duplicates never create a second intent."""
    if os.getenv('LIVE_TRADING','false').strip().lower() != 'false':
        raise RuntimeError('BLOCKED: LIVE_TRADING must be false')
    if not dsn: raise RuntimeError('DATABASE_URL missing')
    e,s=validate(signal_id,symbol,side,entry,stop)
    import psycopg2
    with psycopg2.connect(dsn,connect_timeout=10) as conn:
        with conn.cursor() as cur:
            # Serialize with one global lock; protects concurrent staging reservations.
            cur.execute('SELECT enabled,kill_switch FROM live_risk_state WHERE singleton=TRUE FOR UPDATE')
            risk=cur.fetchone()
            if risk != (False,True): raise RuntimeError('risk switch must remain locked')
            cur.execute('SELECT id,state FROM live_order_intents WHERE source=%s AND signal_id=%s',('R1',signal_id))
            existing=cur.fetchone()
            if existing:
                return {'intent_id':existing[0],'state':existing[1],'duplicate':True,'orders_submitted':False}
            cur.execute('SELECT COUNT(*) FROM live_order_intents WHERE source=%s AND state=ANY(%s)',('R1',list(ACTIVE)))
            if cur.fetchone()[0] >= 5: raise RuntimeError('R1 staging limit 5 reached')
            cur.execute('''INSERT INTO live_order_intents
              (source,signal_id,symbol,position_side,intended_margin_usdt,leverage,entry_price,stop_price,state)
              VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'PENDING') RETURNING id''',
              ('R1',signal_id,symbol,side,Decimal('10'),8,e,s))
            intent_id=cur.fetchone()[0]
            cur.execute('INSERT INTO live_order_events (intent_id,event_type,event_data) VALUES (%s,%s,%s::jsonb)',
                        (intent_id,'STAGING_RESERVED','{"orders_submitted":false}'))
            return {'intent_id':intent_id,'state':'PENDING','duplicate':False,'orders_submitted':False}


def self_test():
    assert validate('x','BTC-USDT','LONG','70000','68250')==(Decimal('70000'),Decimal('68250'))
    assert validate('x','BTC-USDT','SHORT','70000','71750')[1]==Decimal('71750')
    for args in [('x','BTC-USDT','LONG',70000,68000),('x','BTC-USDT','SHORT',70000,68250),
                 ('x','BTC-USDT','LONG','NaN',68250),('x','BAD','LONG',70000,68250)]:
        try: validate(*args)
        except ValueError: pass
        else: raise AssertionError('invalid signal accepted')
    assert len(ACTIVE)==6
    print('PASS: R1 staging reservation validation; duplicate protection uses PostgreSQL transaction')
    print('NOT LIVE READY: database concurrency and exchange-side stops must be integration tested')

if __name__=='__main__':
    p=argparse.ArgumentParser()
    g=p.add_mutually_exclusive_group(required=True)
    g.add_argument('--self-test',action='store_true')
    g.add_argument('--reserve',action='store_true')
    for arg in ('signal-id','symbol','side','entry','stop'): p.add_argument('--'+arg)
    a=p.parse_args()
    if a.self_test: self_test()
    else:
        import json
        if not all((a.signal_id,a.symbol,a.side,a.entry,a.stop)): p.error('missing reservation fields')
        print(json.dumps(reserve(os.getenv('DATABASE_URL'),a.signal_id,a.symbol,a.side,a.entry,a.stop)))
