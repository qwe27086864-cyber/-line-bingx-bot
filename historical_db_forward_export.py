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
            if not isinstance(row, dict):
                row = dict(zip(fieldnames, row))
            w.writerow({k: row.get(k) for k in fieldnames})
            n += 1
    return n


def fetch_dicts(cur, sql: str):
    cur.execute(sql)
    cols = [d.name for d in cur.description]
    for row in cur.fetchall():
        yield dict(zip(cols, row))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default="data")
    ap.add_argument("--as1-variant", default=os.getenv("AS1_FORWARD_VARIANT", "H10"))
    args = ap.parse_args()

    db = os.getenv("DATABASE_URL")
    if not db:
        raise SystemExit("DATABASE_URL is required for historical_db_forward_export.py")

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    report = {}

    conn = psycopg2.connect(db)
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            # BF1: these observations contain the exact BBO/basis/funding inputs
            # required by bf1_historical_adapter.py.
            bf_fields = [
                "timestamp","symbol","spot_bid","spot_ask","perp_bid","perp_ask",
                "funding_rate","next_funding_time_ms"
            ]
            bf_sql = """
                SELECT
                    observed_at AS timestamp,
                    symbol,
                    spot_bid,
                    spot_ask,
                    perp_bid,
                    perp_ask,
                    funding_rate,
                    next_funding_time_ms
                FROM bf_observations
                WHERE spot_bid > 0 AND spot_ask > 0
                  AND perp_bid > 0 AND perp_ask > 0
                ORDER BY observed_at
            """
            report["bf1_snapshots"] = write_rows(
                out / "bf1_snapshots.csv", bf_fields, fetch_dicts(cur, bf_sql)
            )

            # AS1: the DB does not contain the raw historic trade aggressor stream,
            # so do NOT fabricate as1_events.csv. Use the actually observed shadow
            # equity path instead. Produce one daily outcome for a designated variant.
            as1_fields = ["timestamp","symbol","core","net_return_pct"]
            as1_sql = """
                WITH x AS (
                  SELECT
                    observed_at,
                    symbol,
                    shadow_variant,
                    net_pnl_usdt,
                    date_trunc('day', observed_at) AS d,
                    first_value(net_pnl_usdt) OVER (
                      PARTITION BY symbol, shadow_variant, date_trunc('day', observed_at)
                      ORDER BY observed_at
                    ) AS first_pnl,
                    first_value(net_pnl_usdt) OVER (
                      PARTITION BY symbol, shadow_variant, date_trunc('day', observed_at)
                      ORDER BY observed_at DESC
                    ) AS last_pnl,
                    row_number() OVER (
                      PARTITION BY symbol, shadow_variant, date_trunc('day', observed_at)
                      ORDER BY observed_at DESC
                    ) AS rn
                  FROM as_mm_shadow_snapshots
                  WHERE shadow_variant = %s
                )
                SELECT
                  d AS timestamp,
                  symbol,
                  'AS1'::text AS core,
                  ((last_pnl - first_pnl) / 1000.0) * 100.0 AS net_return_pct
                FROM x
                WHERE rn = 1
                ORDER BY d, symbol
            """
            cur.execute(as1_sql, (args.as1_variant,))
            cols = [d.name for d in cur.description]
            report["as1_forward_outcomes"] = write_rows(
                out / "as1_forward_outcomes.csv",
                as1_fields,
                (dict(zip(cols, r)) for r in cur.fetchall()),
            )

            # Meme1: use the current SURVIVAL_V3 realised forward positions directly.
            m1_fields = ["timestamp","symbol","core","net_return_pct"]
            m1_sql = """
                SELECT
                  opened_at AS timestamp,
                  mint_address AS symbol,
                  'MEME1'::text AS core,
                  net_return_pct
                FROM meme1v3_positions
                WHERE variant = 'SURVIVAL_V3'
                  AND status = 'CLOSED'
                  AND net_return_pct IS NOT NULL
                ORDER BY opened_at
            """
            report["meme1_forward_outcomes"] = write_rows(
                out / "meme1_forward_outcomes.csv",
                m1_fields,
                fetch_dicts(cur, m1_sql),
            )

            # Meme2: use the current V3 realised forward positions directly.
            m2_fields = ["timestamp","symbol","core","net_return_pct"]
            m2_sql = """
                SELECT
                  opened_at AS timestamp,
                  mint_address AS symbol,
                  'MEME2'::text AS core,
                  net_return_pct
                FROM meme2_positions
                WHERE engine_version = 'MEME2_V3'
                  AND status = 'CLOSED'
                  AND net_return_pct IS NOT NULL
                ORDER BY opened_at
            """
            report["meme2_forward_outcomes"] = write_rows(
                out / "meme2_forward_outcomes.csv",
                m2_fields,
                fetch_dicts(cur, m2_sql),
            )

    finally:
        conn.close()

    report["as1_variant"] = args.as1_variant
    report["note"] = (
        "BF1 is replay input. AS1/MEME1/MEME2 are forward-shadow outcomes from DB, "
        "not fabricated 30-day historical microstructure/on-chain replays."
    )
    (out / "db_forward_export_report.json").write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8"
    )
    print(json.dumps(report, indent=2, default=str), flush=True)


if __name__ == "__main__":
    main()
