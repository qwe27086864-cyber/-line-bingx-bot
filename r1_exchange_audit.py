"""BingX R1/LINE READ-ONLY exchange snapshot audit. No orders or database writes.

Usage: python r1_exchange_audit.py --self-test
       python r1_exchange_audit.py --account-check

The account check intentionally DOES NOT claim protective stops are verified:
BingX conditional/trigger orders may not be included in openOrders.
All new entries remain blocked until exchange-specific stop verification exists.
"""
import argparse
import json
import os
from bingx_readonly import signed_get

ENDPOINTS = (
    ('balance', '/openApi/swap/v2/user/balance'),
    ('positions', '/openApi/swap/v2/user/positions'),
    ('open_orders', '/openApi/swap/v2/trade/openOrders'),
)

def classify_snapshot(data, kind):
    """Return non-sensitive shape diagnostics; never turn unknown shapes into empty."""
    if isinstance(data, list):
        if not all(isinstance(x, dict) for x in data):
            return {'status':'UNKNOWN_SHAPE', 'count':None}
        return {'status':'PARSED_LIST', 'count':len(data)}
    if isinstance(data, dict):
        # Balance is normally a nested object. Orders/positions may use envelopes;
        # their exact schema must be verified before any trade decisions.
        return {'status':'UNVERIFIED_OBJECT_SHAPE', 'count':None,
                'keys': sorted(str(k) for k in data.keys())[:20]}
    return {'status':'UNKNOWN_SHAPE', 'count':None}

def audit(fetch=signed_get):
    if os.getenv('LIVE_TRADING', 'false').strip().lower() == 'true':
        raise RuntimeError('BLOCKED: LIVE_TRADING must remain false')
    result = {'checks':{}, 'protective_stops_verified':False,
              'permit_new_entries':False, 'status':'REVIEW_REQUIRED'}
    for name,path in ENDPOINTS:
        try:
            result['checks'][name] = classify_snapshot(fetch(path), name)
        except Exception as exc:
            # Never print credentials, raw responses, or signed URLs.
            result['checks'][name] = {'status':'FAILED', 'error_type':type(exc).__name__}
            break
    return result

def self_test():
    assert classify_snapshot([], 'positions') == {'status':'PARSED_LIST','count':0}
    assert classify_snapshot({'orders':[]},'open_orders')['count'] is None
    assert classify_snapshot(None,'positions')['status']=='UNKNOWN_SHAPE'
    assert classify_snapshot([42],'positions')['status']=='UNKNOWN_SHAPE'
    fake = lambda path: []
    r=audit(fake)
    assert len(r['checks'])==3 and not r['permit_new_entries']
    assert not r['protective_stops_verified']
    def fail(path): raise RuntimeError('test')
    assert audit(fail)['checks']['balance']['status']=='FAILED'
    print('PASS: read-only audit shape checks; unknown snapshots and unverified stops never authorize trading.')
    print('NOT LIVE READY: stop/trigger orders, order IDs, position sides and fills are not reconciled.')

def main():
    p=argparse.ArgumentParser()
    modes=p.add_mutually_exclusive_group(required=True)
    modes.add_argument('--self-test', action='store_true')
    modes.add_argument('--account-check', action='store_true')
    args=p.parse_args()
    if args.self_test: self_test()
    else: print(json.dumps(audit(),ensure_ascii=False,sort_keys=True))

if __name__=='__main__': main()
