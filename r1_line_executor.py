"""R1 + LINE isolated execution staging engine.

This module prepares, validates and persists order intents, but NEVER sends
orders. It is intentionally fail-closed until exchange reconciliation,
protective stop handling and restart recovery are implemented and tested.

Usage: python r1_line_executor.py --self-test
       python r1_line_executor.py --db staging_orders.sqlite3 --source R1 --signal-id demo-1 --symbol BTC-USDT --side LONG --entry 70000 --sl 68250 --tp1 71400 --tp2 72800 --tp3 75600
"""
import argparse
import json
import os
import sqlite3
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

MAX_MARGIN = Decimal("10")
MAX_LEVERAGE = 8
MAX_R1_OPEN = 5
MAX_LINE_OPEN = 3
TEST_BUDGET = Decimal("500")
DAILY_STOP = Decimal("-10")

SCHEMA = """
CREATE TABLE IF NOT EXISTS order_intents (
 id INTEGER PRIMARY KEY,
 source TEXT NOT NULL CHECK(source IN ('R1','LINE')),
 signal_id TEXT NOT NULL,
 symbol TEXT NOT NULL,
 side TEXT NOT NULL CHECK(side IN ('LONG','SHORT')),
 entry TEXT NOT NULL,
 sl TEXT NOT NULL,
 tp1 TEXT NOT NULL,
 tp2 TEXT NOT NULL,
 tp3 TEXT NOT NULL,
 margin TEXT NOT NULL,
 leverage INTEGER NOT NULL,
 state TEXT NOT NULL DEFAULT 'DRY_RUN',
 created_at TEXT NOT NULL,
 UNIQUE(source,signal_id)
);
"""

def dec(v):
    try:
        value = Decimal(str(v))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError("Invalid price/margin") from exc
    if not value.is_finite() or value <= 0:
        raise ValueError("Prices and margin must be positive finite numbers")
    return value

def validate(source, symbol, side, entry, sl, tp1, tp2, tp3, margin, leverage):
    if source not in ("R1", "LINE") or side not in ("LONG", "SHORT"):
        raise ValueError("Invalid strategy or side")
    if not symbol.endswith("-USDT") or not symbol[:-5].replace("-", "").isalnum():
        raise ValueError("Invalid USDT symbol")
    e, s, a, b, c, m = map(dec, (entry, sl, tp1, tp2, tp3, margin))
    if m > MAX_MARGIN or leverage != MAX_LEVERAGE:
        raise ValueError("Margin/leverage exceeds staging policy")
    if side == "LONG" and not (s < e < a < b < c):
        raise ValueError("LONG requires SL < entry < TP1 < TP2 < TP3")
    if side == "SHORT" and not (c < b < a < e < s):
        raise ValueError("SHORT requires TP3 < TP2 < TP1 < entry < SL")
    if source == "R1":
        targets = ((a / e - 1) * 100, (b / e - 1) * 100, (c / e - 1) * 100,
                   (1 - s / e) * 100) if side == "LONG" else (
                   (1 - a / e) * 100, (1 - b / e) * 100, (1 - c / e) * 100,
                   (s / e - 1) * 100)
        for actual, expected in zip(targets, map(Decimal, ("2", "4", "8", "2.5"))):
            if abs(actual - expected) > Decimal("0.02"):
                raise ValueError("R1 TP/SL does not match unmodified V4.2R1 core")
    return (e, s, a, b, c, m)

def stage(db, source, signal_id, symbol, side, entry, sl, tp1, tp2, tp3,
          margin="10", leverage=8):
    if os.getenv("LIVE_TRADING", "false").strip().lower() == "true":
        raise RuntimeError("BLOCKED: staging executor refuses LIVE_TRADING=true")
    if not signal_id or len(signal_id) > 200:
        raise ValueError("Invalid signal ID")
    vals = validate(source, symbol, side, entry, sl, tp1, tp2, tp3, margin, leverage)
    with sqlite3.connect(db, timeout=20) as con:
        con.execute(SCHEMA)
        count = con.execute(
            "SELECT count(*) FROM order_intents WHERE source=? AND state NOT IN ('CANCELLED','CLOSED')",
            (source,)).fetchone()[0]
        existing = con.execute(
            "SELECT id,state FROM order_intents WHERE source=? AND signal_id=?",
            (source, signal_id)).fetchone()
        if existing:
            return {"id": existing[0], "state": existing[1], "duplicate": True}
        limit = MAX_R1_OPEN if source == "R1" else MAX_LINE_OPEN
        if count >= limit:
            raise RuntimeError(f"Staging {source} pool full ({count}/{limit})")
        total = con.execute(
            "SELECT margin FROM order_intents WHERE state NOT IN ('CANCELLED','CLOSED')"
        ).fetchall()
        if sum((Decimal(x[0]) for x in total), Decimal("0")) + vals[-1] > TEST_BUDGET:
            raise RuntimeError("Staging capital budget exceeded")
        now = datetime.now(timezone.utc).isoformat()
        cur = con.execute(
            """INSERT INTO order_intents
            (source,signal_id,symbol,side,entry,sl,tp1,tp2,tp3,margin,leverage,state,created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,'DRY_RUN',?)""",
            (source,signal_id,symbol,side,*map(str, vals[:5]),str(vals[5]),leverage,now))
        return {"id":cur.lastrowid,"state":"DRY_RUN","duplicate":False}

def self_test():
    import tempfile
    with tempfile.TemporaryDirectory() as folder:
        db = os.path.join(folder, "test.sqlite3")
        args = ("R1","test-1","BTC-USDT","LONG","70000","68250","71400","72800","75600")
        one = stage(db,*args)
        two = stage(db,*args)
        assert one["state"] == "DRY_RUN" and two["duplicate"]
        three = stage(db,"LINE","line-1","BTC-USDT","SHORT",
                      "70000","72000","69000","68000","67000")
        assert three["state"] == "DRY_RUN"
        try:
            stage(db,"R1","bad","BTC-USDT","LONG","70000","65000","71400","72800","75600")
        except ValueError:
            pass
        else:
            raise AssertionError("R1 TP/SL integrity check failed")
    print("PASS: R1/LINE separate, deduplication, direction checks, DRY_RUN only")
    print("NOT READY FOR LIVE: no exchange adapter, persistent cloud ledger, exchange-side stops, or reconciliation.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--self-test",action="store_true")
    parser.add_argument("--db",default="staging_orders.sqlite3")
    for name in ("source","signal-id","symbol","side","entry","sl","tp1","tp2","tp3"):
        parser.add_argument("--"+name,required=False)
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    required = ("source","signal_id","symbol","side","entry","sl","tp1","tp2","tp3")
    if any(getattr(args,k) is None for k in required):
        parser.error("Missing required signal arguments")
    result = stage(args.db,*(getattr(args,k) for k in required))
    print(json.dumps(result,ensure_ascii=False))

if __name__ == "__main__":
    main()
