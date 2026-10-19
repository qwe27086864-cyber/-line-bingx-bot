"""Offline R1/LINE safety regression tests. Never connects to BingX or places orders.
Run: python r1_line_safety_tests.py
Requires r1_line_executor.py in the same directory.
"""
import os
import sqlite3
import tempfile
from unittest.mock import patch
from r1_line_executor import stage, validate

R1 = ('R1','r1-001','BTC-USDT','LONG','70000','68250','71400','72800','75600')
LINE = ('LINE','line-001','BTC-USDT','SHORT','70000','72000','69000','68000','67000')

def rejected(fn, exception=Exception):
    try:
        fn()
    except exception:
        return
    raise AssertionError('Expected rejection')

def run():
    with tempfile.TemporaryDirectory() as temp:
        db = os.path.join(temp, 'test.sqlite3')
        with patch.dict(os.environ, {'LIVE_TRADING':'false'}):
            first = stage(db,*R1)
            assert first['state'] == 'DRY_RUN'
            assert stage(db,*R1)['duplicate'] is True
            assert stage(db,*LINE)['state'] == 'DRY_RUN'
            with sqlite3.connect(db) as con:
                assert con.execute('SELECT COUNT(*) FROM order_intents').fetchone()[0] == 2
            rejected(lambda: stage(db,'R1','bad-tp','BTC-USDT','LONG','70000','68250','71500','72800','75600'), ValueError)
            rejected(lambda: stage(db,'LINE','bad-side','BTC-USDT','SIDEWAYS','70000','72000','69000','68000','67000'), ValueError)
            rejected(lambda: stage(db,'LINE','bad-margin','BTC-USDT','SHORT','70000','72000','69000','68000','67000',margin='11'), ValueError)
            rejected(lambda: stage(db,'LINE','bad-leverage','BTC-USDT','SHORT','70000','72000','69000','68000','67000',leverage=9), ValueError)
            rejected(lambda: stage(db,'LINE','bad-order','BTC-USDT','LONG','70000','68000','69000','71000','72000'), ValueError)
            for i in range(2,6):
                stage(db,'R1',f'r1-{i}','BTC-USDT','LONG','70000','68250','71400','72800','75600')
            rejected(lambda: stage(db,'R1','over-cap','BTC-USDT','LONG','70000','68250','71400','72800','75600'), RuntimeError)
            for i in range(2,4):
                stage(db,'LINE',f'line-{i}','BTC-USDT','SHORT','70000','72000','69000','68000','67000')
            rejected(lambda: stage(db,'LINE','over-cap','BTC-USDT','SHORT','70000','72000','69000','68000','67000'), RuntimeError)
        with patch.dict(os.environ, {'LIVE_TRADING':'true'}):
            rejected(lambda: stage(db,'R1','blocked','BTC-USDT','LONG','70000','68250','71400','72800','75600'), RuntimeError)
    print('PASS: offline DRY_RUN safety checks (dedup, R1 TP/SL, LINE separation, limits, invalid signals, live block).')
    print('NOT LIVE READY: exchange stops, restart reconciliation, cloud ledger and daily loss enforcement not implemented.')

if __name__ == '__main__':
    run()
