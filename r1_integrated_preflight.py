"""R1/LINE consolidated READ-ONLY preflight. Never submits or modifies orders.

Run: python r1_integrated_preflight.py --offline
     python r1_integrated_preflight.py --account-check
     python r1_integrated_preflight.py --db-check
     python r1_integrated_preflight.py --all

All modes require LIVE_TRADING=false. Nonzero exit means NOT LIVE READY.
"""
import argparse
import json
import os
import sys

EXPECTED = ('live_order_intents', 'live_order_events', 'live_risk_state')


def ensure_disabled():
    if os.getenv('LIVE_TRADING', 'false').strip().lower() != 'false':
        raise RuntimeError('LIVE_TRADING must be explicitly false')


def offline():
    from r1_restart_reconcile import self_test as restart_test
    from r1_exchange_audit import self_test as audit_test
    from bingx_readonly import self_test as readonly_test
    restart_test()
    audit_test()
    readonly_test()
    print('PASS: consolidated offline diagnostics')


def account_check():
    from r1_exchange_audit import audit
    report = audit()
    print(json.dumps(report, ensure_ascii=False, sort_keys=True))
    if any(v.get('status') == 'FAILED' for v in report['checks'].values()):
        raise RuntimeError('Exchange read failed')
    if len(report['checks']) != 3:
        raise RuntimeError('Exchange snapshot incomplete')
    print('READ ONLY: API reachable; stop protection remains UNVERIFIED')


def db_check():
    import psycopg2
    dsn = os.environ.get('DATABASE_URL')
    if not dsn:
        raise RuntimeError('DATABASE_URL missing')
    with psycopg2.connect(dsn, connect_timeout=10) as conn:
        conn.set_session(readonly=True)
        with conn.cursor() as cur:
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name = ANY(%s)", (list(EXPECTED),))
            tables = {r[0] for r in cur.fetchall()}
            if tables != set(EXPECTED):
                raise RuntimeError('Live ledger schema incomplete: ' + ','.join(sorted(set(EXPECTED)-tables)))
            cur.execute('SELECT enabled, kill_switch FROM live_risk_state WHERE singleton = TRUE')
            row = cur.fetchone()
            if row != (False, True):
                raise RuntimeError('Unsafe or missing risk state; expected enabled=false, kill_switch=true')
    print('PASS: ledger exists; trading disabled and kill switch enabled')


def main():
    p = argparse.ArgumentParser()
    group = p.add_mutually_exclusive_group(required=True)
    for name in ('offline', 'account-check', 'db-check', 'all'):
        group.add_argument('--' + name, action='store_true')
    args = p.parse_args()
    try:
        ensure_disabled()
        if args.offline or args.all: offline()
        if args.account_check or args.all: account_check()
        if args.db_check or args.all: db_check()
        print('NOT LIVE READY: no exchange-side stop-order matching, restart execution, or daily loss enforcement')
        return 0
    except Exception as exc:
        print('BLOCKED:', type(exc).__name__, str(exc) if isinstance(exc, RuntimeError) else 'unexpected error')
        return 1


if __name__ == '__main__':
    sys.exit(main())
