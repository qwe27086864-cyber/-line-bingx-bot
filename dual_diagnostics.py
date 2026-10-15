"""R1/V6 independent PAPER diagnostic recorder. No orders, no strategy changes.
Run every five minutes as a Render cron job: python dual_diagnostics.py
Requires DATABASE_URL. Retains all old trades; records only post-cutoff entries.
"""
import os
from datetime import datetime, timezone, timedelta
import psycopg2
from psycopg2.extras import Json

os.environ['LIVE_TRADING'] = 'false'
from scanner_job import get_all_tickers

CUTOFF = datetime.fromisoformat(os.getenv('COMPARE_START_UTC','2026-10-15T08:44:47Z').replace('Z','+00:00'))
if CUTOFF.tzinfo is None:
    raise ValueError('COMPARE_START_UTC must include timezone')

DDL = """
CREATE TABLE IF NOT EXISTS dual_trade_diagnostics (
 version TEXT NOT NULL, trade_id BIGINT NOT NULL, observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 symbol TEXT NOT NULL, side TEXT NOT NULL, status TEXT NOT NULL,
 entry_time TIMESTAMPTZ NOT NULL, entry_price DOUBLE PRECISION NOT NULL,
 market_price DOUBLE PRECISION, unrealized_gross_pct DOUBLE PRECISION,
 btc_change_24h DOUBLE PRECISION, eth_change_24h DOUBLE PRECISION,
 symbol_change_24h DOUBLE PRECISION, symbol_quote_volume DOUBLE PRECISION,
 spread_pct DOUBLE PRECISION, holding_minutes DOUBLE PRECISION,
 exit_reason TEXT, realized_net_pct DOUBLE PRECISION,
 entry_score DOUBLE PRECISION, strategy TEXT, entry_reasons TEXT,
 PRIMARY KEY (version, trade_id, observed_at)
);
CREATE INDEX IF NOT EXISTS dual_diag_trade_lookup ON dual_trade_diagnostics(version, trade_id, observed_at);
CREATE TABLE IF NOT EXISTS dual_shadow_observations (
 version TEXT NOT NULL, trade_id BIGINT NOT NULL,
 checkpoint_hours INTEGER NOT NULL, observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
 exit_time TIMESTAMPTZ NOT NULL, exit_price DOUBLE PRECISION NOT NULL,
 shadow_price DOUBLE PRECISION NOT NULL,
 hold_vs_exit_pct DOUBLE PRECISION NOT NULL,
 PRIMARY KEY(version, trade_id, checkpoint_hours)
);
"""

def main():
    url=os.getenv('DATABASE_URL')
    if not url: raise RuntimeError('DATABASE_URL missing; refusing to run')
    tickers=get_all_tickers()
    prices={x['symbol']:x for x in tickers}
    btc=prices.get('BTC-USDT',{})
    eth=prices.get('ETH-USDT',{})
    conn=psycopg2.connect(url,sslmode='require',connect_timeout=10)
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT pg_try_advisory_xact_lock(660007)')
                if not cur.fetchone()[0]:
                    print('diagnostic job already running');return
                cur.execute(DDL)
                cur.execute("""SELECT id,symbol,side,status,signal_time,entry_price,closed_at,exit_price,
                  exit_reason,net_result_pct,score,strategy,reasons FROM scanner_trades
                  WHERE engine_version='V4.2R1' AND signal_time >= %s
                    AND (status='OPEN' OR closed_at >= NOW()-INTERVAL '5 hours')""",(CUTOFF,))
                r1=cur.fetchall()
                cur.execute("""SELECT id,symbol,side,status,entry_time,entry_price,exit_time,exit_price,
                  exit_reason,net_pct,score,strategy,reasons::text FROM v6_paper_trades
                  WHERE entry_time >= %s
                    AND (status='OPEN' OR exit_time >= NOW()-INTERVAL '5 hours')""",(CUTOFF,))
                v6=cur.fetchall()
                now=datetime.now(timezone.utc)
                count=0;shadows=0
                for version,rows in [('R1',r1),('V6',v6)]:
                    for (tid,sym,side,status,entry_time,entry_price,exit_time,exit_price,exit_reason,net_pct,score,strategy,reasons) in rows:
                        ticker=prices.get(sym)
                        price=ticker['price'] if ticker else None
                        direction=-1 if str(side).upper()=='SHORT' else 1
                        gross=direction*(price/entry_price-1)*100 if price and entry_price else None
                        minutes=(now-entry_time).total_seconds()/60
                        cur.execute("""INSERT INTO dual_trade_diagnostics
                          (version,trade_id,symbol,side,status,entry_time,entry_price,market_price,
                           unrealized_gross_pct,btc_change_24h,eth_change_24h,symbol_change_24h,
                           symbol_quote_volume,spread_pct,holding_minutes,exit_reason,
                           realized_net_pct,entry_score,strategy,entry_reasons)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                          (version,tid,sym,side,status,entry_time,entry_price,price,gross,
                           btc.get('change_24h'),eth.get('change_24h'),
                           ticker.get('change_24h') if ticker else None,
                           ticker.get('quote_volume') if ticker else None,
                           ticker.get('spread_pct') if ticker else None,minutes,exit_reason,
                           net_pct,score,strategy,str(reasons) if reasons is not None else None))
                        count+=1
                        if status in ('CLOSED', 'WIN', 'LOSS') and exit_time and exit_price and price:
                            elapsed=(now-exit_time).total_seconds()/3600
                            for hours in (1,2,4):
                                # First observation at or after checkpoint, never claim exact checkpoint price.
                                if elapsed>=hours:
                                    delta=direction*(price/exit_price-1)*100
                                    cur.execute("""INSERT INTO dual_shadow_observations
                                      (version,trade_id,checkpoint_hours,exit_time,exit_price,shadow_price,hold_vs_exit_pct)
                                      VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                                      (version,tid,hours,exit_time,exit_price,price,delta))
                                    shadows+=cur.rowcount
                print(f'DIAGNOSTICS OK: {count} snapshots; {shadows} shadow checkpoints; {len(tickers)} markets',flush=True)
    finally:conn.close()

if __name__=='__main__':main()
