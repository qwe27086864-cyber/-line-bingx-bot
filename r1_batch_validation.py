"""Single command offline suite for R1 staging. Never places orders.
Run: python r1_batch_validation.py
"""
import os
import sys


def main():
    if os.getenv('LIVE_TRADING', 'false').strip().lower() != 'false':
        raise RuntimeError('BLOCKED: LIVE_TRADING must be false')
    from r1_risk_gate import self_test as risk_test
    from r1_ledger_guard import self_test as ledger_test
    from r1_exchange_snapshot_parser import self_test as snapshot_test
    from r1_execution_simulator import self_test as fills_test
    from r1_line_executor import self_test as intent_test
    from r1_restart_reconcile import self_test as reconcile_test
    for name, test in [('risk',risk_test),('ledger',ledger_test),('snapshot',snapshot_test),('fills',fills_test),('intent',intent_test),('reconcile',reconcile_test)]:
        print('TEST:', name, flush=True)
        test()
    print('PASS: six staging modules passed offline checks')
    print('NOT LIVE READY: no order submission, protective stop verification, restart execution, or daily PnL enforcement')

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('BLOCKED:', type(e).__name__, file=sys.stderr)
        sys.exit(1)
