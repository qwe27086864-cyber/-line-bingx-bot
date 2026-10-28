from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path

import psycopg2


def write_rows(path: Path, fieldnames: list[str], rows) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k) for k in fieldnames})
            n += 1
    return n


def from_env(out: Path) -> dict:
    raw = os.getenv("FORWARD_OUTCOMES_JSON")
    if not raw:
        raise SystemExit("FORWARD_OUTCOMES_JSON missing")
    p = json.loads(raw)
    fields = ["timestamp","symbol","core","net_return_pct"]
    report = {}
    report["as1_forward_outcomes"] = write_rows(out/"as1_forward_outcomes.csv", fields, p.get("as1", []))
    report["meme1_forward_outcomes"] = write_rows(out/"meme1_forward_outcomes.csv", fields, p.get("meme1", []))
    report["meme2_forward_outcomes"] = write_rows(out/"meme2_forward_outcomes.csv", fields, p.get("meme2", []))

    bf = p.get("bf1", {})
    (out/"bf1_forward_status.json").write_text(json.dumps(bf, indent=2), encoding="utf-8")
    report["bf1"] = bf
    report["source"] = "FORWARD_OUTCOMES_JSON"
    return report


def fetch_dicts(cur, sql: str):
    cur.execute(sql)
    cols = [d.name for d in cur.description]
    for row in cur.fetchall():
        yield dict(zip(cols, row))


def from_db(out: Path, db: str, as1_variant: str) -> dict:
    report = {}
    conn = psycopg2.connect(db)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            bf_sql = """
                SELECT observed_at AS timestamp,symbol,spot_bid,spot_ask,perp_bid,perp_ask,
                       funding_rate,next_funding_time_ms
                FROM bf_observations
                WHERE spot_bid>0 AND spot_ask>0 AND perp_bid>0 AND perp_ask>0
                ORDER BY observed_at
            """
            bf_fields=["timestamp","symbol","spot_bid","spot_ask","perp_bid","perp_ask","funding_rate","next_funding_time_ms"]
            report["bf1_snapshots"]=write_rows(out/"bf1_snapshots.csv",bf_fields,fetch_dicts(cur,bf_sql))

            as1_sql = """
                WITH x AS (
                  SELECT observed_at,symbol,shadow_variant,net_pnl_usdt,date_trunc('day',observed_at) d,
                    first_value(net_pnl_usdt) OVER (
                      PARTITION BY symbol,shadow_variant,date_trunc('day',observed_at) ORDER BY observed_at
                    ) first_pnl,
                    first_value(net_pnl_usdt) OVER (
                      PARTITION BY symbol,shadow_variant,date_trunc('day',observed_at) ORDER BY observed_at DESC
                    ) last_pnl,
                    row_number() OVER (
                      PARTITION BY symbol,shadow_variant,date_trunc('day',observed_at) ORDER BY observed_at DESC
                    ) rn
                  FROM as_mm_shadow_snapshots WHERE shadow_variant=%s
                )
                SELECT d timestamp,symbol,'AS1'::text core,
                       ((last_pnl-first_pnl)/1000.0)*100.0 net_return_pct
                FROM x WHERE rn=1 ORDER BY d,symbol
            """
            cur.execute(as1_sql,(as1_variant,))
            cols=[d.name for d in cur.description]
            rows=(dict(zip(cols,r)) for r in cur.fetchall())
            report["as1_forward_outcomes"]=write_rows(out/"as1_forward_outcomes.csv",
                ["timestamp","symbol","core","net_return_pct"],rows)

            m1_sql = """
                SELECT opened_at timestamp,mint_address symbol,'MEME1'::text core,net_return_pct
                FROM meme1v3_positions
                WHERE variant='SURVIVAL_V3' AND status='CLOSED' AND net_return_pct IS NOT NULL
                ORDER BY opened_at
            """
            report["meme1_forward_outcomes"]=write_rows(out/"meme1_forward_outcomes.csv",
                ["timestamp","symbol","core","net_return_pct"],fetch_dicts(cur,m1_sql))

            m2_sql = """
                SELECT opened_at timestamp,mint_address symbol,'MEME2'::text core,net_return_pct
                FROM meme2_positions
                WHERE engine_version='MEME2_V3' AND status='CLOSED' AND net_return_pct IS NOT NULL
                ORDER BY opened_at
            """
            report["meme2_forward_outcomes"]=write_rows(out/"meme2_forward_outcomes.csv",
                ["timestamp","symbol","core","net_return_pct"],fetch_dicts(cur,m2_sql))
    finally:
        conn.close()
    report["source"]="DATABASE_URL"
    return report


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--out-dir",default="data")
    ap.add_argument("--as1-variant",default=os.getenv("AS1_FORWARD_VARIANT","H10"))
    args=ap.parse_args()
    out=Path(args.out_dir);out.mkdir(parents=True,exist_ok=True)

    db=os.getenv("DATABASE_URL","")
    if db and db != "ENV_ONLY":
        report=from_db(out,db,args.as1_variant)
    else:
        report=from_env(out)

    report["note"]="AS1/MEME1/MEME2 are forward validation; BF1 may be no-trade forward evidence."
    (out/"db_forward_export_report.json").write_text(json.dumps(report,indent=2,default=str),encoding="utf-8")
    print(json.dumps(report,indent=2,default=str),flush=True)

if __name__=="__main__":
    main()
