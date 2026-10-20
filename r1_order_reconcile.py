"""R1 offline order/stop reconciliation. No network, no exchange writes.

Usage: python r1_order_reconcile.py --self-test
       python r1_order_reconcile.py --snapshot snapshot.json

Input JSON: {"intent": {"symbol": "BTC-USDT", "side": "LONG",
"client_order_id": "...", "quantity": "0.1"}, "orders": [...],
"positions": [...], "stops": [...]}. Unrecognized states fail closed.
This module never permits live trading: manual review remains required.
"""
import argparse
import json
from decimal import Decimal, InvalidOperation

FINAL = {'FILLED', 'CANCELED', 'CANCELLED', 'REJECTED', 'EXPIRED'}
KNOWN = FINAL | {'NEW', 'PARTIALLY_FILLED', 'PENDING', 'UNKNOWN'}


def num(value):
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError('Invalid quantity') from exc
    if not d.is_finite() or d < 0:
        raise ValueError('Invalid quantity')
    return d


def reconcile(snapshot):
    if not isinstance(snapshot, dict):
        raise ValueError('Snapshot must be an object')
    intent = snapshot.get('intent')
    orders, positions, stops = (snapshot.get(k) for k in ('orders', 'positions', 'stops'))
    if not isinstance(intent, dict) or not all(isinstance(x, list) for x in (orders, positions, stops)):
        raise ValueError('Missing intent or exchange collections; never assume empty')
    symbol, side, client_id = (intent.get(k) for k in ('symbol', 'side', 'client_order_id'))
    if not isinstance(symbol, str) or not symbol.endswith('-USDT') or side not in ('LONG', 'SHORT') or not isinstance(client_id, str) or not client_id:
        raise ValueError('Invalid intent identity')
    qty = num(intent.get('quantity'))
    if qty == 0:
        raise ValueError('Zero requested quantity')
    warnings = []
    matching = []
    for row in orders:
        if not isinstance(row, dict):
            raise ValueError('Invalid order record')
        if row.get('clientOrderId') == client_id:
            matching.append(row)
    if len(matching) != 1:
        warnings.append('ENTRY_ORDER_NOT_UNIQUELY_IDENTIFIED')
    else:
        order = matching[0]
        if order.get('symbol') != symbol or order.get('positionSide') != side:
            warnings.append('ENTRY_ORDER_IDENTITY_MISMATCH')
        status = order.get('status')
        if status not in KNOWN:
            warnings.append('UNKNOWN_ENTRY_STATUS')
        elif status != 'FILLED':
            warnings.append('ENTRY_NOT_FULLY_FILLED:' + status)
        try:
            executed = num(order.get('executedQty'))
            if executed > qty:
                warnings.append('OVERFILLED_OR_QUANTITY_MISMATCH')
        except ValueError:
            warnings.append('EXECUTED_QUANTITY_UNKNOWN')
    # A stop must be linked by exchange order ID and have an appropriate
    # trigger, quantity, reduce-only/close semantics, and active status.
    # The current prototype does not yet verify these exchange-specific fields.
    warnings.append('EXCHANGE_STOP_COVERAGE_NOT_IMPLEMENTED')
    warnings.append('POSITION_FILL_AND_STOP_ID_RECONCILIATION_NOT_IMPLEMENTED')
    return {'status': 'REVIEW_REQUIRED', 'permit_live_orders': False,
            'entry_matches': len(matching), 'position_records': len(positions),
            'stop_records': len(stops), 'warnings': warnings}


def self_test():
    intent = {'symbol': 'BTC-USDT', 'side': 'LONG', 'client_order_id': 'r1-1', 'quantity': '0.1'}
    order = {'clientOrderId': 'r1-1', 'symbol': 'BTC-USDT', 'positionSide': 'LONG', 'status': 'FILLED', 'executedQty': '0.1'}
    result = reconcile({'intent': intent, 'orders': [order], 'positions': [], 'stops': []})
    assert result['entry_matches'] == 1 and not result['permit_live_orders']
    assert 'EXCHANGE_STOP_COVERAGE_NOT_IMPLEMENTED' in result['warnings']
    assert 'ENTRY_ORDER_NOT_UNIQUELY_IDENTIFIED' in reconcile({'intent': intent, 'orders': [], 'positions': [], 'stops': []})['warnings']
    assert any('PARTIALLY_FILLED' in x for x in reconcile({'intent': intent, 'orders': [{**order, 'status': 'PARTIALLY_FILLED'}], 'positions': [], 'stops': []})['warnings'])
    for bad in ({'intent': intent, 'orders': [], 'positions': []}, {'intent': intent, 'orders': None, 'positions': [], 'stops': []}):
        try:
            reconcile(bad)
        except ValueError:
            pass
        else:
            raise AssertionError('Missing exchange data accepted')
    print('PASS: order identity, partial fill, missing snapshot fail closed; NO ORDERS')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--self-test', action='store_true')
    group.add_argument('--snapshot')
    args = parser.parse_args()
    if args.self_test:
        self_test()
    else:
        with open(args.snapshot, encoding='utf-8') as f:
            print(json.dumps(reconcile(json.load(f)), indent=2))
