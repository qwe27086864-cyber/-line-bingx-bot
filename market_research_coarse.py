import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg

PORT = int(os.getenv("PORT", "10000"))
DBURL = os.getenv("DATABASE_URL", "").strip()
REFRESH_SEC = int(os.getenv("COARSE_REFRESH_SEC", "60"))
SERVICE = "market-research-coarse-v2-persistent"

STATE = {
    "service": SERVICE,
    "status": "starting",
    "db_enabled": bool(DBURL),
    "updated_at": None,
    "outcomes": 0,
    "main": [],
    "adjustments": {},
    "persistent_write": False,
    "errors": [],
}

DIMENSIONS = {
    "trend": 2,
    "volume": 3,
    "spread": 4,
    "regime": 5,
    "atr": 6,
    "session": 7,
}

CREATE_SQL = [
    """
    CREATE TABLE IF NOT EXISTS market_research_coarse_stats (
        event_type text PRIMARY KEY,
        n integer NOT NULL,
        wins integer NOT NULL,
        win_pct double precision,
        avg_net_pct double precision,
        sum_net_pct double precision,
        updated_at timestamptz NOT NULL DEFAULT now()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_research_coarse_modifiers (
        event_type text NOT NULL,
        dimension text NOT NULL,
        value text NOT NULL,
        n integer NOT NULL,
        wins integer NOT NULL,
        win_pct double precision,
        avg_net_pct double precision,
        base_avg_net_pct double precision,
        delta_vs_base_net_pct double precision,
        updated_at timestamptz NOT NULL DEFAULT now(),
        PRIMARY KEY (event_type, dimension, value)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS market_research_coarse_meta (
        key text PRIMARY KEY,
        value jsonb NOT NULL,
        updated_at timestamptz NOT NULL DEFAULT now()
    )
    """,
]

MAIN_SQL = """
SELECT
    event_type,
    count(*)::int AS n,
    sum(CASE WHEN success THEN 1 ELSE 0 END)::int AS wins,
    avg(CASE WHEN success THEN 1.0 ELSE 0.0 END) * 100.0 AS win_pct,
    avg(net_pct) AS avg_net_pct,
    sum(net_pct) AS sum_net_pct
FROM market_research_v8_outcomes
WHERE event_type <> 'NORMAL_CONTROL'
GROUP BY event_type
ORDER BY n DESC
"""

def dim_sql(position: int) -> str:
    return f"""
    SELECT
        event_type,
        split_part(pattern_key,'|',{position}) AS value,
        count(*)::int AS n,
        sum(CASE WHEN success THEN 1 ELSE 0 END)::int AS wins,
        avg(CASE WHEN success THEN 1.0 ELSE 0.0 END) * 100.0 AS win_pct,
        avg(net_pct) AS avg_net_pct
    FROM market_research_v8_outcomes
    WHERE event_type <> 'NORMAL_CONTROL'
    GROUP BY event_type, split_part(pattern_key,'|',{position})
    HAVING count(*) >= 30
    ORDER BY event_type, n DESC
    """

def iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def safe_float(v):
    return None if v is None else float(v)

def rowdicts(cur):
    cols = [d.name for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def ensure_tables(cur):
    for sql in CREATE_SQL:
        cur.execute(sql)

def write_main(cur, main_rows):
    sql = """
    INSERT INTO market_research_coarse_stats
        (event_type, n, wins, win_pct, avg_net_pct, sum_net_pct, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s,now())
    ON CONFLICT (event_type) DO UPDATE SET
        n=EXCLUDED.n,
        wins=EXCLUDED.wins,
        win_pct=EXCLUDED.win_pct,
        avg_net_pct=EXCLUDED.avg_net_pct,
        sum_net_pct=EXCLUDED.sum_net_pct,
        updated_at=now()
    """
    for r in main_rows:
        cur.execute(sql, (
            r["event_type"], r["n"], r["wins"],
            safe_float(r["win_pct"]),
            safe_float(r["avg_net_pct"]),
            safe_float(r["sum_net_pct"]),
        ))

def write_modifiers(cur, adjustments, base_map):
    sql = """
    INSERT INTO market_research_coarse_modifiers
        (event_type, dimension, value, n, wins, win_pct, avg_net_pct,
         base_avg_net_pct, delta_vs_base_net_pct, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
    ON CONFLICT (event_type, dimension, value) DO UPDATE SET
        n=EXCLUDED.n,
        wins=EXCLUDED.wins,
        win_pct=EXCLUDED.win_pct,
        avg_net_pct=EXCLUDED.avg_net_pct,
        base_avg_net_pct=EXCLUDED.base_avg_net_pct,
        delta_vs_base_net_pct=EXCLUDED.delta_vs_base_net_pct,
        updated_at=now()
    """
    for dimension, rows in adjustments.items():
        for r in rows:
            base = base_map.get(r["event_type"])
            avg_net = safe_float(r["avg_net_pct"])
            delta = None if base is None or avg_net is None else avg_net - base
            cur.execute(sql, (
                r["event_type"], dimension, r["value"],
                r["n"], r["wins"],
                safe_float(r["win_pct"]),
                avg_net, base, delta
            ))

def write_meta(cur, outcomes):
    payload = {
        "service": SERVICE,
        "source_table": "market_research_v8_outcomes",
        "outcomes": outcomes,
        "main_layer": "event_type",
        "secondary_adjustments": list(DIMENSIONS.keys()),
        "min_adjustment_sample": 30,
        "raw_fine_patterns_preserved": True,
        "refreshed_at": iso(),
    }
    cur.execute(
        """
        INSERT INTO market_research_coarse_meta(key,value,updated_at)
        VALUES ('learning_state', %s::jsonb, now())
        ON CONFLICT(key) DO UPDATE SET
            value=EXCLUDED.value,
            updated_at=now()
        """,
        (json.dumps(payload, separators=(",", ":")),)
    )

def refresh():
    if not DBURL:
        STATE["status"] = "error"
        STATE["persistent_write"] = False
        STATE["errors"] = [{"time": iso(), "error": "DATABASE_URL missing"}]
        return

    try:
        with psycopg.connect(DBURL, connect_timeout=10) as con:
            with con.cursor() as cur:
                ensure_tables(cur)

                cur.execute("SELECT count(*)::int FROM market_research_v8_outcomes")
                outcomes = cur.fetchone()[0]

                cur.execute(MAIN_SQL)
                main_rows = rowdicts(cur)

                adjustments = {}
                for name, pos in DIMENSIONS.items():
                    cur.execute(dim_sql(pos))
                    adjustments[name] = rowdicts(cur)

                base_map = {
                    r["event_type"]: safe_float(r["avg_net_pct"])
                    for r in main_rows
                }

                write_main(cur, main_rows)
                write_modifiers(cur, adjustments, base_map)
                write_meta(cur, outcomes)

            con.commit()

        STATE["outcomes"] = outcomes
        STATE["main"] = main_rows
        STATE["adjustments"] = adjustments
        STATE["status"] = "ok"
        STATE["persistent_write"] = True
        STATE["updated_at"] = iso()
        STATE["errors"] = []

    except Exception as exc:
        STATE["status"] = "error"
        STATE["persistent_write"] = False
        STATE["errors"] = [{
            "time": iso(),
            "error": f"{type(exc).__name__}: {str(exc)[:500]}"
        }]

def loop():
    while True:
        refresh()
        time.sleep(max(15, REFRESH_SEC))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({
            **STATE,
            "persistent_tables": [
                "market_research_coarse_stats",
                "market_research_coarse_modifiers",
                "market_research_coarse_meta",
            ],
            "learning_model": {
                "main_layer": "event_type",
                "main_classes": [
                    "DOWN_IMPULSE",
                    "UP_IMPULSE",
                    "BREAKOUT_DOWN",
                    "BREAKOUT_UP",
                    "UPPER_WICK",
                    "LOWER_WICK",
                    "BOTH_WICK",
                ],
                "secondary_adjustments": list(DIMENSIONS.keys()),
                "min_adjustment_sample": 30,
                "raw_fine_patterns_preserved": True,
                "write_mode": "UPSERT_FROM_FULL_HISTORY",
                "deletes_source_data": False,
            }
        }, ensure_ascii=False, separators=(",", ":"), default=str).encode()

        self.send_response(200 if STATE["status"] != "error" else 503)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass

if __name__ == "__main__":
    refresh()
    threading.Thread(target=loop, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
