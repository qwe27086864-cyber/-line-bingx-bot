"""R1 staging database guard: read-only verification, no schema writes or orders.
Usage: python r1_ledger_guard.py --self-test
       python r1_ledger_guard.py --db-check
"""
import argparse
import os

TABLES = {"live_order_intents", "live_order_events", "live_risk_state"}

def validate(tables, risk_rows):
    if set(tables) != TABLES:
        return False, "LEDGER_TABLES_MISSING"
    if len(risk_rows) != 1:
        return False, "RISK_STATE_MISSING_OR_DUPLICATE"
    enabled, kill_switch = risk_rows[0]
    if enabled is not False or kill_switch is not True:
        return False, "RISK_SWITCH_NOT_LOCKED"
    return True, "LEDGER_PRESENT_AND_LOCKED"

def db_check():
    if os.getenv("LIVE_TRADING", "false").lower() != "false":
        raise RuntimeError("LIVE_TRADING must be false")
    import psycopg2
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL missing")
    conn = psycopg2.connect(dsn, connect_timeout=10)
    try:
        conn.set_session(readonly=True, autocommit=False)
        with conn.cursor() as cur:
            cur.execute("""SELECT table_name FROM information_schema.tables
                           WHERE table_schema='public' AND table_name LIKE 'live_%'""")
            tables = [row[0] for row in cur.fetchall()]
            if set(tables) != TABLES:
                return validate(tables, [])
            cur.execute("SELECT enabled,kill_switch FROM live_risk_state")
            return validate(tables, cur.fetchall())
    finally:
        conn.close()

def self_test():
    assert validate(TABLES, [(False, True)])[0]
    for tables,rows in (([],[]),(TABLES,[]),(TABLES,[(True,False)]),
                        (TABLES,[(False,False)]),
                        (TABLES,[(False,True),(False,True)])):
        assert not validate(tables,rows)[0]
    print("PASS: 6 database guard cases")
    print("READ ONLY: no database changes or trading")

if __name__ == "__main__":
    parser=argparse.ArgumentParser()
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--self-test",action="store_true")
    group.add_argument("--db-check",action="store_true")
    args=parser.parse_args()
    if args.self_test:
        self_test()
    else:
        ok,reason=db_check()
        print(("PASS" if ok else "BLOCKED")+": "+reason)
        raise SystemExit(0 if ok else 1)
