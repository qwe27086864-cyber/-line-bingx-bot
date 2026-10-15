"""V6 isolated forward paper scanner. No live orders. Uses R1 signal analysis.
Requires DATABASE_URL and runs every five minutes; independent V6 tables.
V6 exit rules are experimental, not a byte-for-byte R1 execution replica.
"""
import os
os.environ['LIVE_TRADING'] = 'false'
import json
from datetime import datetime, timezone
from app import get_db_connection
from scanner_job import get_all_tickers, build_radar_candidates, deep_analyze

VERSION = 'V6'
MAX_OPEN = 20
MAX_NEW = 3
FEE_ROUNDTRIP_PCT = float(os.getenv('V6_FEE_ROUNDTRIP_PCT', '0.16'))
SLIPPAGE_ROUNDTRIP_PCT = float(os.getenv('V6_SLIPPAGE_ROUNDTRIP_PCT', '0.10'))


def setup(cur):
    cur.execute('''CREATE TABLE IF NOT EXISTS v6_radar (
        symbol TEXT PRIMARY KEY, price DOUBLE PRECISION NOT NULL,
        captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW())''')
    cur.execute('''CREATE TABLE IF NOT EXISTS v6_paper_trades (
        id BIGSERIAL PRIMARY KEY, symbol TEXT NOT NULL, side TEXT NOT NULL,
        strategy TEXT NOT NULL, score DOUBLE PRECISION, entry_price DOUBLE PRECISION NOT NULL,
        entry_time TIMESTAMPTZ NOT NULL DEFAULT NOW(), exit_price DOUBLE PRECISION,
        exit_time TIMESTAMPTZ, exit_reason TEXT, net_pct DOUBLE PRECISION,
        status TEXT NOT NULL DEFAULT 'OPEN', reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
        highest_price DOUBLE PRECISION, lowest_price DOUBLE PRECISION,
        last_checked_at TIMESTAMPTZ, CHECK (side IN ('LONG','SHORT')),
        CHECK (status IN ('OPEN','CLOSED')))''')
    cur.execute('''CREATE UNIQUE INDEX IF NOT EXISTS v6_one_open_per_symbol
        ON v6_paper_trades(symbol) WHERE status='OPEN' ''')


def update_trades(cur, prices):
    cur.execute("SELECT id,symbol,side,entry_price,entry_time,highest_price,lowest_price FROM v6_paper_trades WHERE status='OPEN' FOR UPDATE")
    for tid,sym,side,entry,started,hi,lo in cur.fetchall():
        p = prices.get(sym)
        if not p or p <= 0: continue
        hi,lo=max(hi or entry,p),min(lo or entry,p)
        direction=1 if side=='LONG' else -1
        gross=direction*(p/entry-1)*100
        hours=(datetime.now(timezone.utc)-started).total_seconds()/3600
        reason='SL' if gross<=-2.5 else ('TP3' if gross>=8 else ('TIMEOUT' if hours>=4 else None))
        if reason:
            net=gross-FEE_ROUNDTRIP_PCT-SLIPPAGE_ROUNDTRIP_PCT
            cur.execute('''UPDATE v6_paper_trades SET status='CLOSED',exit_price=%s,
                exit_time=NOW(),exit_reason=%s,net_pct=%s,highest_price=%s,
                lowest_price=%s,last_checked_at=NOW() WHERE id=%s''',(p,reason,net,hi,lo,tid))
            print('V6 PAPER CLOSE',tid,sym,reason,round(net,4),flush=True)
        else:
            cur.execute('''UPDATE v6_paper_trades SET highest_price=%s,lowest_price=%s,
                last_checked_at=NOW() WHERE id=%s''',(hi,lo,tid))


def run():
    if not os.getenv('DATABASE_URL'):
        raise RuntimeError('DATABASE_URL missing: refusing to run without persistent storage')
    conn=get_db_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT pg_try_advisory_xact_lock(660006)')
                if not cur.fetchone()[0]:
                    print('V6 prior scan running; skip',flush=True)
                    return
                setup(cur)
                tickers=get_all_tickers()
                prices={x['symbol']:x['price'] for x in tickers}
                update_trades(cur,prices)
                cur.execute('SELECT symbol,price,captured_at FROM v6_radar')
                previous={s:{'price':p,'captured_at':t} for s,p,t in cur.fetchall()}
                candidates=build_radar_candidates(tickers,previous) if previous else []
                for t in tickers:
                    cur.execute('''INSERT INTO v6_radar(symbol,price,captured_at)
                        VALUES(%s,%s,NOW()) ON CONFLICT(symbol) DO UPDATE
                        SET price=EXCLUDED.price,captured_at=EXCLUDED.captured_at''',
                        (t['symbol'],t['price']))
                cur.execute("SELECT COUNT(*) FROM v6_paper_trades WHERE status='OPEN'")
                available=max(0,min(MAX_NEW,MAX_OPEN-cur.fetchone()[0]))
                qualified=[]
                for radar in candidates:
                    try:
                        signal=deep_analyze(radar)
                        if signal: qualified.append(signal)
                    except Exception as exc:
                        print('V6 analysis error',radar['symbol'],str(exc),flush=True)
                qualified.sort(key=lambda x:x['score'],reverse=True)
                created=0
                for signal in qualified:
                    if created>=available: break
                    sym=signal['symbol']
                    cur.execute('''SELECT 1 FROM v6_paper_trades WHERE symbol=%s
                        AND (status='OPEN' OR entry_time>NOW()-INTERVAL '4 hours') LIMIT 1''',(sym,))
                    if cur.fetchone(): continue
                    entry=float(signal['price'])
                    if entry<=0: continue
                    cur.execute('''INSERT INTO v6_paper_trades
                        (symbol,side,strategy,score,entry_price,highest_price,lowest_price,reasons)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s::jsonb)''',
                        (sym,signal['side'],signal['strategy'],signal['score'],entry,entry,
                         entry,json.dumps(signal.get('reasons',[]),ensure_ascii=False)))
                    created+=1
                    print('V6 PAPER OPEN',sym,signal['side'],signal['score'],flush=True)
                print('V6 scan complete',len(tickers),'markets',len(candidates),'candidates',created,'opens',flush=True)
    finally:
        conn.close()

if __name__=='__main__':
    run()
