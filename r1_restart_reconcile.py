"""R1/LINE restart reconciliation DRY-RUN prototype (NO NETWORK, NO ORDERS).

Run: python r1_restart_reconcile.py --self-test
Input: local JSON files of ledger intents, positions, open orders.
Run: python r1_restart_reconcile.py --ledger ledger.json --positions positions.json --orders orders.json
Never interprets a missing/invalid exchange snapshot as zero positions.
This is a diagnostic prototype, NOT a live-trading safety guarantee.
"""
import argparse
import json
import os
from collections import Counter

ACTIVE = {'PENDING','SUBMITTING','ACKNOWLEDGED','PROTECTED','CLOSING','UNKNOWN'}
TERMINAL = {'CLOSED','REJECTED'}

def _records(obj, kind):
    if not isinstance(obj, list):
        raise ValueError(f'{kind} snapshot must be a list; do not assume empty')
    if any(not isinstance(row, dict) for row in obj):
        raise ValueError(f'{kind} contains non-object records')
    return obj

def reconcile(ledger, positions, orders):
    """Fail closed: return diagnostics and permit_new_entries=False always."""
    ledger = _records(ledger, 'ledger')
    positions = _records(positions, 'positions')
    orders = _records(orders, 'orders')
    alerts = []
    ids = []
    for intent in ledger:
        for field in ('source','signal_id','symbol','position_side','state'):
            if not isinstance(intent.get(field), str) or not intent[field].strip():
                raise ValueError(f'ledger intent missing {field}')
        if intent['source'] not in ('R1','LINE') or intent['position_side'] not in ('LONG','SHORT'):
            raise ValueError('invalid intent source or side')
        if intent['state'] not in ACTIVE | TERMINAL:
            raise ValueError('unknown ledger state')
        ids.append((intent['source'],intent['signal_id']))
    if len(ids) != len(set(ids)):
        alerts.append('DUPLICATE_LEDGER_SIGNAL')
    for row in positions:
        if not isinstance(row.get('symbol'), str) or not row['symbol']:
            raise ValueError('position missing symbol')
        # Exchange shape is deliberately NOT normalized here: manual review required.
    for row in orders:
        if not isinstance(row.get('symbol'), str) or not row['symbol']:
            raise ValueError('order missing symbol')
    active_symbols = {x['symbol'] for x in ledger if x['state'] in ACTIVE}
    exchange_symbols = {x['symbol'] for x in positions} | {x['symbol'] for x in orders}
    for symbol in sorted(exchange_symbols - active_symbols):
        alerts.append('UNMATCHED_EXCHANGE_SYMBOL:' + symbol)
    for intent in ledger:
        if intent['state'] in ACTIVE and intent['symbol'] not in exchange_symbols:
            alerts.append('UNVERIFIED_LEDGER_INTENT:' + intent['source'] + ':' + intent['signal_id'])
        if intent['state'] in ('SUBMITTING','UNKNOWN'):
            alerts.append('UNCERTAIN_ORDER_STATE:' + intent['source'] + ':' + intent['signal_id'])
    # No claim that symbol-level matches are adequate: exchange order IDs,
    # position side, fills and protective stop coverage MUST be reconciled later.
    alerts.append('ORDER_ID_SIDE_FILL_AND_STOP_VERIFICATION_NOT_IMPLEMENTED')
    return {'permit_new_entries':False,'status':'REVIEW_REQUIRED',
            'ledger_intents':len(ledger),'exchange_positions':len(positions),
            'exchange_orders':len(orders),'alerts':alerts}

def self_test():
    base=[{'source':'R1','signal_id':'s1','symbol':'BTC-USDT','position_side':'LONG','state':'ACKNOWLEDGED'}]
    result=reconcile(base,[],[])
    assert not result['permit_new_entries'] and any('UNVERIFIED' in a for a in result['alerts'])
    result=reconcile([], [{'symbol':'ETH-USDT'}], [])
    assert 'UNMATCHED_EXCHANGE_SYMBOL:ETH-USDT' in result['alerts']
    result=reconcile([{**base[0],'state':'UNKNOWN'}],[],[])
    assert any('UNCERTAIN' in a for a in result['alerts'])
    result=reconcile(base+base,[],[])
    assert 'DUPLICATE_LEDGER_SIGNAL' in result['alerts']
    for bad in (None, {}, '[]'):
        try: reconcile([],bad,[])
        except ValueError: pass
        else: raise AssertionError('invalid snapshot accepted')
    assert not reconcile([],[],[])['permit_new_entries']
    print('PASS: restart reconciliation DRY_RUN diagnostics; ambiguous states block new entries.')
    print('NOT LIVE READY: real exchange ID/side/stop matching and execution are not implemented.')

def main():
    if os.getenv('LIVE_TRADING','false').strip().lower() == 'true':
        raise RuntimeError('Refusing to run with LIVE_TRADING=true')
    p=argparse.ArgumentParser()
    p.add_argument('--self-test',action='store_true')
    p.add_argument('--ledger');p.add_argument('--positions');p.add_argument('--orders')
    args=p.parse_args()
    if args.self_test:
        self_test();return
    if not all((args.ledger,args.positions,args.orders)):
        p.error('all three snapshot files required; missing files are not empty positions')
    def load(path):
        with open(path,encoding='utf-8') as f:return json.load(f)
    print(json.dumps(reconcile(load(args.ledger),load(args.positions),load(args.orders)),ensure_ascii=False,indent=2))
if __name__=='__main__':main()
