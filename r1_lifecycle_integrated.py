"""R1 live-readiness lifecycle validator, DRY-RUN ONLY.

Single-file integration: validates fill-driven state transitions, original-size
TP allocation, exchange-side stop coverage and fail-closed restart policy.
Never sends orders or changes database or risk switch.

python r1_lifecycle_integrated.py --self-test
python r1_lifecycle_integrated.py --account-audit  # read-only, requires keys
"""
import argparse
import json
import os
from decimal import Decimal, InvalidOperation

ACTIVE = ('PENDING','SUBMITTING','ACKNOWLEDGED','PROTECTED','CLOSING','UNKNOWN')
TRANSITIONS = {
    'PENDING': {'SUBMITTING','REJECTED','UNKNOWN'},
    'SUBMITTING': {'ACKNOWLEDGED','UNKNOWN','REJECTED'},
    'ACKNOWLEDGED': {'PROTECTED','CLOSING','UNKNOWN'},
    'PROTECTED': {'CLOSING','UNKNOWN'},
    'CLOSING': {'CLOSED','UNKNOWN'},
    'UNKNOWN': {'CLOSING','CLOSED','REJECTED'},
    'REJECTED': set(), 'CLOSED': set(),
}

def num(v):
    try:
        n = Decimal(str(v))
        if not n.is_finite(): raise ValueError()
        return n
    except (ValueError, TypeError, InvalidOperation):
        raise ValueError('Invalid decimal')

def transition(current, nxt, *, exchange_confirmed=False, protective_stop_confirmed=False):
    if nxt not in TRANSITIONS.get(current, ()):
        raise ValueError('Invalid state transition')
    if nxt == 'ACKNOWLEDGED' and exchange_confirmed is not True:
        raise ValueError('Exchange acknowledgement required')
    if nxt == 'PROTECTED' and protective_stop_confirmed is not True:
        raise ValueError('Verified exchange-side stop required')
    if nxt == 'CLOSED' and exchange_confirmed is not True:
        raise ValueError('Exchange flat confirmation required')
    return nxt

def allocate(original_qty, step_sizes=('0.30','0.40','0.30')):
    q = num(original_qty)
    weights = [num(w) for w in step_sizes]
    if q <= 0 or len(weights)!=3 or any(w<=0 for w in weights) or sum(weights)!=1:
        raise ValueError('Invalid quantity/weights')
    return (q*weights[0],q*weights[1],q-q*weights[0]-q*weights[1])

def coverage(position, stop_orders):
    """Input is NORMALIZED, NOT raw BingX API data. Never authorizes live orders."""
    if not isinstance(position,dict) or not isinstance(stop_orders,list):
        return False,'SNAPSHOT_MISSING'
    required = {'symbol','side','quantity','stop_price'}
    if not required.issubset(position) or position['side'] not in ('LONG','SHORT'):
        return False,'POSITION_INVALID'
    try:
        q, sl = num(position['quantity']),num(position['stop_price'])
        if q<=0 or sl<=0: return False,'POSITION_INVALID'
    except ValueError: return False,'POSITION_INVALID'
    covered=Decimal('0')
    ids=set()
    for o in stop_orders:
        if not isinstance(o,dict): return False,'ORDER_SHAPE_UNKNOWN'
        if not {'id','symbol','side','qty','trigger','type','status','reduce_only'}.issubset(o):
            return False,'ORDER_FIELDS_UNKNOWN'
        if o['symbol']!=position['symbol'] or o['side']!=position['side']: continue
        if o['type']!='STOP_MARKET' or o['status']!='ACTIVE' or o['reduce_only'] is not True: continue
        if not isinstance(o['id'],str) or not o['id'] or o['id'] in ids:
            return False,'STOP_ID_INVALID'
        ids.add(o['id'])
        try:
            oq, trigger = num(o['qty']),num(o['trigger'])
        except ValueError: return False,'STOP_INVALID'
        if oq<=0 or trigger<=0: return False,'STOP_INVALID'
        if position['side']=='LONG' and trigger<sl: continue
        if position['side']=='SHORT' and trigger>sl: continue
        covered+=oq
    return (covered>=q, 'NORMALIZED_COVERAGE_ONLY' if covered>=q else 'STOP_COVERAGE_INSUFFICIENT')

def restart_policy(ledger, positions, open_orders, stop_orders):
    """Explicitly blocks new entries even if snapshots seem complete.

    Real exchange adapter, stable IDs, fills and conditional-order endpoints
    have not been validated. Never infer empty positions from unknown shapes.
    """
    result={'permit_new_entries':False,'status':'REVIEW_REQUIRED','alerts':[]}
    if any(not isinstance(x,list) for x in (ledger,positions,open_orders,stop_orders)):
        result['alerts'].append('MISSING_OR_UNKNOWN_SNAPSHOT')
        return result
    if any(not isinstance(x,dict) for group in (ledger,positions,open_orders,stop_orders) for x in group):
        result['alerts'].append('INVALID_RECORD_SHAPE')
        return result
    ids=[(x.get('source'),x.get('signal_id')) for x in ledger]
    if any(None in key for key in ids) or len(ids)!=len(set(ids)):
        result['alerts'].append('INVALID_OR_DUPLICATE_LEDGER_IDS')
    if any(x.get('state') in ('SUBMITTING','UNKNOWN') for x in ledger):
        result['alerts'].append('UNCERTAIN_ORDER_STATE')
    result['alerts'].append('REAL_EXCHANGE_STOP_AND_FILL_RECONCILIATION_NOT_IMPLEMENTED')
    return result

def account_audit():
    if os.getenv('LIVE_TRADING','false').strip().lower()!='false':
        raise RuntimeError('LIVE_TRADING must be false')
    from bingx_readonly import signed_get
    positions=signed_get('/openApi/swap/v2/user/positions')
    orders=signed_get('/openApi/swap/v2/trade/openOrders')
    from r1_exchange_snapshot_parser import inspect
    report=inspect(positions,orders)
    report['permit_new_entries']=False
    print(json.dumps(report,ensure_ascii=False,sort_keys=True))
    print('NOT LIVE READY: conditional stops and actual fills not verified')

def self_test():
    tests=0
    def ok(condition):
        nonlocal tests
        assert condition
        tests+=1
    def rejects(fn):
        nonlocal tests
        try: fn()
        except ValueError: tests+=1
        else: raise AssertionError('Expected rejection')
    ok(transition('PENDING','SUBMITTING')=='SUBMITTING')
    rejects(lambda:transition('SUBMITTING','ACKNOWLEDGED'))
    ok(transition('SUBMITTING','ACKNOWLEDGED',exchange_confirmed=True)=='ACKNOWLEDGED')
    rejects(lambda:transition('ACKNOWLEDGED','PROTECTED'))
    ok(transition('ACKNOWLEDGED','PROTECTED',protective_stop_confirmed=True)=='PROTECTED')
    rejects(lambda:transition('PROTECTED','CLOSED',exchange_confirmed=True))
    rejects(lambda:transition('CLOSING','CLOSED'))
    ok(transition('CLOSING','CLOSED',exchange_confirmed=True)=='CLOSED')
    rejects(lambda:transition('CLOSED','SUBMITTING'))
    ok(allocate('10')==(Decimal('3.00'),Decimal('4.00'),Decimal('3.00')))
    rejects(lambda:allocate('NaN'))
    rejects(lambda:allocate(10,('0.3','0.3','0.3')))
    p={'symbol':'BTC-USDT','side':'LONG','quantity':'1','stop_price':'97500'}
    s={'id':'s1','symbol':'BTC-USDT','side':'LONG','qty':'1','trigger':'97500','type':'STOP_MARKET','status':'ACTIVE','reduce_only':True}
    ok(coverage(p,[s])[0])
    ok(not coverage(p,[])[0])
    ok(not coverage(p,[{**s,'reduce_only':False}])[0])
    ok(not coverage(p,[{**s,'qty':'0.5'}])[0])
    ok(not coverage(p,[{**s,'trigger':'97000'}])[0])
    ok(not coverage(p,[s,s])[0])
    ok(not coverage(p,[{}])[0])
    ok(not restart_policy([],[],[],[])['permit_new_entries'])
    ok('MISSING_OR_UNKNOWN_SNAPSHOT' in restart_policy([],None,[],[])['alerts'])
    ok('UNCERTAIN_ORDER_STATE' in restart_policy([{'source':'R1','signal_id':'x','state':'UNKNOWN'}],[],[],[])['alerts'])
    ok('INVALID_OR_DUPLICATE_LEDGER_IDS' in restart_policy([{'source':'R1','signal_id':'x'},{'source':'R1','signal_id':'x'}],[],[],[])['alerts'])
    print(f'PASS: {tests} lifecycle, TP sizing, stop coverage and restart fail-closed tests')
    print('NOT LIVE READY: no exchange order adapter, live protective stops or actual fill reconciliation')

if __name__=='__main__':
    ap=argparse.ArgumentParser()
    g=ap.add_mutually_exclusive_group(required=True)
    g.add_argument('--self-test',action='store_true')
    g.add_argument('--account-audit',action='store_true')
    args=ap.parse_args()
    if args.self_test: self_test()
    else: account_audit()
