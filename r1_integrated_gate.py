"""R1 integration gate. Offline only: NO API calls and NO orders.

Usage:
  python r1_integrated_gate.py --self-test
  python r1_integrated_gate.py --input r1_gate_input.json

Input JSON: {"signal": {...}, "exchange": {"price": "...", "quantity_step": "...",
"min_quantity": "..."}, "risk": {"r1_open": 0, "line_open": 0,
"unique_signal": true, "exchange_snapshot_fresh": true,
"position_mode_verified": true, "leverage_verified": true,
"ledger_locked": true, "daily_risk_verified": true,
"stop_api_verified": true, "tp_api_verified": true},
"snapshot": {"intent": {...}, "orders": [], "positions": [], "stops": []}}

All checks are evidence supplied by the caller; this script cannot verify their truth.
"""
import argparse
import json
from r1_execution_preflight import plan
from r1_order_reconcile import reconcile

RISK_KEYS = ('r1_open', 'line_open', 'unique_signal', 'exchange_snapshot_fresh',
             'position_mode_verified', 'leverage_verified', 'ledger_locked',
             'daily_risk_verified', 'stop_api_verified', 'tp_api_verified')


def inspect(payload):
    if not isinstance(payload, dict):
        raise ValueError('Expected JSON object')
    signal = payload['signal']
    exchange = payload['exchange']
    risk = payload['risk']
    snapshot = payload['snapshot']
    if not all(isinstance(x, dict) for x in (signal, exchange, risk, snapshot)):
        raise ValueError('Invalid payload sections')
    if any(key not in risk for key in RISK_KEYS):
        raise ValueError('Missing risk evidence')
    pre = plan(signal,
               exchange_price=exchange['price'],
               exchange_quantity_step=exchange['quantity_step'],
               exchange_min_quantity=exchange['min_quantity'],
               **{key: risk[key] for key in RISK_KEYS})
    rec = reconcile(snapshot)
    if snapshot['intent'].get('symbol') != signal['symbol'] or snapshot['intent'].get('side') != signal['side']:
        raise ValueError('Signal and order snapshot identity mismatch')
    return {'mode': 'OFFLINE_ONLY', 'permit_live_order': False,
            'signal_id': signal['signal_id'], 'preflight': pre,
            'reconciliation': rec,
            'blocking': ['NO_VERIFIED_EXCHANGE_STOP_COVERAGE',
                         'NO_LIVE_ORDER_EXECUTOR',
                         'NO_ATOMIC_DB_DEDUPLICATION',
                         'NO_VERIFIED_ACCOUNT_RISK_ENFORCEMENT']}


def self_test():
    from r1_execution_preflight import sample, sample_kwargs
    s = sample()
    k = sample_kwargs()
    payload = {'signal': s,
               'exchange': {'price': k.pop('exchange_price'),
                            'quantity_step': k.pop('exchange_quantity_step'),
                            'min_quantity': k.pop('exchange_min_quantity')},
               'risk': k,
               'snapshot': {'intent': {'symbol': s['symbol'], 'side': s['side'],
                                       'client_order_id': 'r1-test', 'quantity': '0.8'},
                            'orders': [], 'positions': [], 'stops': []}}
    result = inspect(payload)
    assert result['permit_live_order'] is False
    assert result['preflight']['permit_live_order'] is False
    assert result['reconciliation']['permit_live_orders'] is False
    try:
        inspect({**payload, 'snapshot': {**payload['snapshot'], 'intent': {**payload['snapshot']['intent'], 'side': 'SHORT'}}})
    except ValueError:
        pass
    else:
        raise AssertionError('Mismatched snapshot accepted')
    print('PASS: R1 integrated offline gate; live orders prohibited')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--self-test', action='store_true')
    mode.add_argument('--input')
    args = parser.parse_args()
    if args.self_test:
        self_test()
    else:
        with open(args.input, encoding='utf-8') as f:
            print(json.dumps(inspect(json.load(f)), indent=2))
