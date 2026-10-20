"""R1 real-execution preflight contract (OFFLINE ONLY, no network/orders).

Run: python r1_execution_preflight.py --self-test
     python r1_execution_preflight.py --example

No API credentials needed. This is an independent gate for the future BingX
executor, not a live executor or an authorization to trade.
"""
import argparse
import json
from decimal import Decimal, InvalidOperation

MAX_R1 = 20
MAX_LINE = 5
MAX_MARGIN = Decimal('10')
LEVERAGE = 8
TP_WEIGHTS = (Decimal('0.35'), Decimal('0.40'), Decimal('0.25'))


def number(value):
    try:
        n = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError('Invalid numeric input') from exc
    if not n.is_finite() or n <= 0:
        raise ValueError('Numeric input must be finite and positive')
    return n


def plan(signal, *, exchange_price, exchange_quantity_step, exchange_min_quantity,
         r1_open, line_open, unique_signal, exchange_snapshot_fresh,
         position_mode_verified, leverage_verified, ledger_locked,
         daily_risk_verified, stop_api_verified, tp_api_verified):
    """Return a NON-EXECUTABLE plan; no side effects or network access."""
    required = ('source', 'signal_id', 'symbol', 'side', 'entry', 'sl',
                'tp1', 'tp2', 'tp3', 'margin_usdt', 'leverage', 'executable')
    if not isinstance(signal, dict) or any(k not in signal for k in required):
        raise ValueError('Incomplete signal')
    if signal['source'] != 'R1' or not str(signal['signal_id']).startswith('V4.2R1:'):
        raise ValueError('Only R1 signals accepted')
    if signal['side'] not in ('LONG', 'SHORT') or not isinstance(signal['symbol'], str) or not signal['symbol'].endswith('-USDT'):
        raise ValueError('Invalid symbol or direction')
    if signal['leverage'] != LEVERAGE or number(signal['margin_usdt']) > MAX_MARGIN:
        raise ValueError('Size policy violation')
    if type(r1_open) is not int or type(line_open) is not int or r1_open < 0 or line_open < 0:
        raise ValueError('Unknown position counts')
    e, sl, a, b, c = [number(signal[k]) for k in ('entry', 'sl', 'tp1', 'tp2', 'tp3')]
    if not (sl < e < a < b < c if signal['side'] == 'LONG' else c < b < a < e < sl):
        raise ValueError('TP/SL direction invalid')
    price, step, minimum = map(number, (exchange_price, exchange_quantity_step, exchange_min_quantity))
    raw_quantity = number(signal['margin_usdt']) * LEVERAGE / price
    quantity = raw_quantity // step * step
    reasons = []
    if quantity < minimum or quantity <= 0:
        reasons.append('QUANTITY_BELOW_EXCHANGE_MINIMUM')
    if r1_open >= MAX_R1 or line_open > MAX_LINE or r1_open + line_open >= MAX_R1 + MAX_LINE:
        reasons.append('POSITION_LIMIT')
    flags = dict(unique_signal=unique_signal, exchange_snapshot_fresh=exchange_snapshot_fresh,
                 position_mode_verified=position_mode_verified, leverage_verified=leverage_verified,
                 ledger_locked=ledger_locked, daily_risk_verified=daily_risk_verified,
                 stop_api_verified=stop_api_verified, tp_api_verified=tp_api_verified)
    reasons.extend(k.upper() + '_NOT_VERIFIED' for k, v in flags.items() if v is not True)
    # The bridge explicitly marks its signals non-executable. This preflight
    # must not reinterpret them as authorization to place live orders.
    if signal['executable'] is not False:
        raise ValueError('Bridge contract unexpectedly changed')
    reasons.append('LIVE_EXECUTOR_NOT_IMPLEMENTED')
    return {'mode': 'OFFLINE_PREFLIGHT', 'permit_live_order': False,
            'source': 'R1', 'signal_id': signal['signal_id'],
            'symbol': signal['symbol'], 'side': signal['side'],
            'margin_usdt': str(number(signal['margin_usdt'])), 'leverage': LEVERAGE,
            'estimated_quantity': str(quantity), 'quantity_step': str(step),
            'tp_weights': [str(x) for x in TP_WEIGHTS], 'blocks': reasons}


def sample():
    return dict(source='R1', signal_id='V4.2R1:101', symbol='BTC-USDT',
                side='LONG', entry='100', sl='97.5', tp1='102', tp2='104',
                tp3='108', margin_usdt='10', leverage=8, executable=False)


def sample_kwargs():
    return dict(exchange_price='100', exchange_quantity_step='0.001',
                exchange_min_quantity='0.001', r1_open=0, line_open=0,
                unique_signal=True, exchange_snapshot_fresh=True,
                position_mode_verified=True, leverage_verified=True,
                ledger_locked=True, daily_risk_verified=True,
                stop_api_verified=True, tp_api_verified=True)


def self_test():
    s = sample()
    p = plan(s, **sample_kwargs())
    assert not p['permit_live_order'] and 'LIVE_EXECUTOR_NOT_IMPLEMENTED' in p['blocks']
    assert p['estimated_quantity'] == '0.800'
    assert 'POSITION_LIMIT' in plan(s, **{**sample_kwargs(), 'r1_open': 20})['blocks']
    assert 'STOP_API_VERIFIED_NOT_VERIFIED' in plan(s, **{**sample_kwargs(), 'stop_api_verified': False})['blocks']
    assert 'QUANTITY_BELOW_EXCHANGE_MINIMUM' in plan(s, **{**sample_kwargs(), 'exchange_min_quantity': '2'})['blocks']
    short = {**s, 'side': 'SHORT', 'sl': '102.5', 'tp1': '98', 'tp2': '96', 'tp3': '92'}
    assert plan(short, **sample_kwargs())['side'] == 'SHORT'
    for bad in ({**s, 'sl': '110'}, {**s, 'margin_usdt': '11'}, {**s, 'executable': True}):
        try:
            plan(bad, **sample_kwargs())
        except ValueError:
            pass
        else:
            raise AssertionError('Unsafe input accepted')
    print('PASS: R1 execution preflight; size, direction, capacity, stop flags; NO ORDERS')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument('--self-test', action='store_true')
    group.add_argument('--example', action='store_true')
    args = p.parse_args()
    if args.self_test:
        self_test()
    else:
        print(json.dumps(plan(sample(), **sample_kwargs()), indent=2))
