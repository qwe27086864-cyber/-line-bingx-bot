"""R1 simulation-to-execution signal bridge (READ ONLY; NO ORDERS).

Run: python r1_signal_bridge.py --self-test
     python r1_signal_bridge.py --inspect

Reads existing R1 scanner trades and reports candidate signals without
modifying simulation data. Never authorizes live trading or places orders.
"""
import argparse
import json
import os
from decimal import Decimal, InvalidOperation

VERSION = 'V4.2R1'
MAX_R1_OPEN = 20
MAX_LINE_OPEN = 5
MARGIN_USDT = Decimal('10')
LEVERAGE = 8


def positive(value):
    try:
        n = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError('Invalid price') from exc
    if not n.is_finite() or n <= 0:
        raise ValueError('Invalid price')
    return n


def candidate(row):
    """Normalize one scanner_trades row to a non-executable signal."""
    keys = ('id', 'symbol', 'side', 'engine_version', 'strategy',
            'status', 'entry_price', 'tp1', 'tp2', 'tp3', 'sl', 'signal_time')
    if not isinstance(row, dict) or any(k not in row for k in keys):
        raise ValueError('Incomplete R1 scanner row')
    if row['engine_version'] != VERSION or row['strategy'] not in ('TREND', 'BREAKOUT'):
        return None
    if row['status'] != 'OPEN':
        return None
    side = row['side']
    if side not in ('LONG', 'SHORT'):
        raise ValueError('Unknown position direction')
    symbol = row['symbol']
    if not isinstance(symbol, str) or not symbol.endswith('-USDT'):
        raise ValueError('Invalid market symbol')
    entry, tp1, tp2, tp3, sl = map(positive, (row['entry_price'], row['tp1'],
                                               row['tp2'], row['tp3'], row['sl']))
    if not (sl < entry < tp1 < tp2 < tp3 if side == 'LONG' else
            tp3 < tp2 < tp1 < entry < sl):
        raise ValueError('Incorrect TP/SL direction')
    signal_id = int(row['id'])
    if signal_id <= 0:
        raise ValueError('Invalid signal id')
    return {'source': 'R1', 'signal_id': f'{VERSION}:{signal_id}',
            'symbol': symbol, 'side': side, 'entry': str(entry),
            'sl': str(sl), 'tp1': str(tp1), 'tp2': str(tp2),
            'tp3': str(tp3), 'margin_usdt': str(MARGIN_USDT),
            'leverage': LEVERAGE, 'signal_time': row['signal_time'].isoformat()
            if hasattr(row['signal_time'], 'isoformat') else str(row['signal_time']),
            'executable': False}


def inspect():
    if os.getenv('LIVE_TRADING', 'false').strip().lower() != 'false':
        raise RuntimeError('READ-ONLY BRIDGE BLOCKS LIVE_TRADING')
    dsn = os.getenv('DATABASE_URL')
    if not dsn:
        raise RuntimeError('DATABASE_URL is required')
    import psycopg2
    from psycopg2.extras import RealDictCursor
    conn = psycopg2.connect(dsn, connect_timeout=10)
    try:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute('''SELECT id,symbol,side,engine_version,strategy,status,
                           entry_price,tp1,tp2,tp3,sl,signal_time
                           FROM scanner_trades
                           WHERE engine_version=%s AND status='OPEN'
                           ORDER BY id DESC LIMIT 30''', (VERSION,))
            signals = [x for x in (candidate(dict(r)) for r in cur.fetchall()) if x]
        print(json.dumps({'mode': 'READ_ONLY', 'live_orders_enabled': False,
                          'signal_count': len(signals), 'signals': signals},
                         ensure_ascii=False, indent=2))
    finally:
        conn.close()


def self_test():
    row = {'id': 101, 'symbol': 'BTC-USDT', 'side': 'LONG',
           'engine_version': VERSION, 'strategy': 'TREND', 'status': 'OPEN',
           'entry_price': 100, 'sl': 97.5, 'tp1': 102, 'tp2': 104,
           'tp3': 108, 'signal_time': '2026-10-20T00:00:00Z'}
    assert candidate(row)['signal_id'] == 'V4.2R1:101'
    assert candidate(row)['executable'] is False
    assert candidate({**row, 'engine_version': 'V6'}) is None
    assert candidate({**row, 'status': 'CLOSED'}) is None
    assert candidate({**row, 'side': 'SHORT', 'sl': 102.5,
                      'tp1': 98, 'tp2': 96, 'tp3': 92})['side'] == 'SHORT'
    try:
        candidate({**row, 'sl': 110})
    except ValueError:
        pass
    else:
        raise AssertionError('Invalid stop accepted')
    print('PASS: R1-only signal bridge, long/short, TP/SL direction, no orders')


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    modes = p.add_mutually_exclusive_group(required=True)
    modes.add_argument('--self-test', action='store_true')
    modes.add_argument('--inspect', action='store_true')
    args = p.parse_args()
    self_test() if args.self_test else inspect()
