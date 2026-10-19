"""One-shot R1/LINE ledger schema installation; never submits orders.
Usage: python r1_ledger_migrate.py
Requires DATABASE_URL, psycopg2 and r1_live_ledger.sql alongside this file.
"""
import os
from pathlib import Path
import psycopg2

SQL = Path(__file__).with_name('r1_live_ledger.sql')
EXPECTED = ('live_order_intents', 'live_order_events', 'live_risk_state')

def main():
    if os.getenv('LIVE_TRADING', 'false').strip().lower() == 'true':
        raise RuntimeError('Refusing migration while LIVE_TRADING=true')
    url = os.getenv('DATABASE_URL')
    if not url:
        raise RuntimeError('DATABASE_URL missing; no changes made')
    schema = SQL.read_text(encoding='utf-8')
    with psycopg2.connect(url, sslmode='require', connect_timeout=10) as conn:
        with conn.cursor() as cur:
            cur.execute(schema)
            cur.execute('SELECT enabled, kill_switch FROM live_risk_state WHERE singleton=TRUE')
            row = cur.fetchone()
            if row != (False, True):
                raise RuntimeError('Unexpected live risk state; transaction rolled back')
            cur.execute('SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema() AND table_name = ANY(%s)', (list(EXPECTED),))
            names = {r[0] for r in cur.fetchall()}
            if names != set(EXPECTED):
                raise RuntimeError('Missing ledger tables; transaction rolled back')
    print('PASS: isolated live ledger schema installed; trading disabled; kill switch ON.')
    print('NOT LIVE READY: no live order submission, protective stops, restart reconciliation, or daily loss enforcement.')

if __name__ == '__main__':
    main()
