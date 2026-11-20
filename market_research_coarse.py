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
SERVICE = "market-research-coarse-v1"

STATE = {
    "service": SERVICE,
    "status": "starting",
    "db_enabled": bool(DBURL),
    "updated_at": None,
    "outcomes": 0,
    "main": [],
    "adjustments": {},
    "errors": [],
}

MAIN_SQL = """
WITH x AS (
  SELECT
    event_type,
    split_part(pattern_key,'|',2) AS trend,
    split_part(pattern_key,'|',3) AS volume_bucket,
    split_part(pattern_key,'|',4) AS spread_bucket,
    split_part(pattern_key,'|',5) AS regime,
    split_part(pattern_key,'|',6) AS atr_bucket,
    split_part(pattern_key,'|',7) AS session,
    success,
    net_pct
  FROM market_research_v8_outcomes
  WHERE event_type <> 'NORMAL_CONTROL'
)
SELECT
  event_type,
  count(*)::int AS n,
  sum(CASE WHEN success THEN 1 ELSE 0 END)::int AS wins,
  round(avg(CASE WHEN success THEN 1.0 ELSE 0.0 END)::numeric * 100, 2) AS win_pct,
  round(avg(net_pct)::numeric, 4) AS avg_net_pct,
  round(sum(net_pct)::numeric, 4) AS sum_net_pct
FROM x
GROUP BY event_type
ORDER BY n DESC
"""

DIM_SQL = {
    "trend": """
      SELECT event_type, split_part(pattern_key,'|',2) value,
             count(*)::int n,
             round(avg(CASE WHEN success THEN 1.0 ELSE 0.0 END)::numeric*100,2) win_pct,
             round(avg(net_pct)::numeric,4) avg_net_pct
      FROM market_research_v8_outcomes
      WHERE event_type <> 'NORMAL_CONTROL'
      GROUP BY event_type, split_part(pattern_key,'|',2)
      HAVING count(*) >= 30
      ORDER BY event_type,n DESC
    """,
    "volume": """
      SELECT event_type, split_part(pattern_key,'|',3) value,
             count(*)::int n,
             round(avg(CASE WHEN success THEN 1.0 ELSE 0.0 END)::numeric*100,2) win_pct,
             round(avg(net_pct)::numeric,4) avg_net_pct
      FROM market_research_v8_outcomes
      WHERE event_type <> 'NORMAL_CONTROL'
      GROUP BY event_type, split_part(pattern_key,'|',3)
      HAVING count(*) >= 30
      ORDER BY event_type,n DESC
    """,
    "spread": """
      SELECT event_type, split_part(pattern_key,'|',4) value,
             count(*)::int n,
             round(avg(CASE WHEN success THEN 1.0 ELSE 0.0 END)::numeric*100,2) win_pct,
             round(avg(net_pct)::numeric,4) avg_net_pct
      FROM market_research_v8_outcomes
      WHERE event_type <> 'NORMAL_CONTROL'
      GROUP BY event_type, split_part(pattern_key,'|',4)
      HAVING count(*) >= 30
      ORDER BY event_type,n DESC
    """,
    "regime": """
      SELECT event_type, split_part(pattern_key,'|',5) value,
             count(*)::int n,
             round(avg(CASE WHEN success THEN 1.0 ELSE 0.0 END)::numeric*100,2) win_pct,
             round(avg(net_pct)::numeric,4) avg_net_pct
      FROM market_research_v8_outcomes
      WHERE event_type <> 'NORMAL_CONTROL'
      GROUP BY event_type, split_part(pattern_key,'|',5)
      HAVING count(*) >= 30
      ORDER BY event_type,n DESC
    """,
    "atr": """
      SELECT event_type, split_part(pattern_key,'|',6) value,
             count(*)::int n,
             round(avg(CASE WHEN success THEN 1.0 ELSE 0.0 END)::numeric*100,2) win_pct,
             round(avg(net_pct)::numeric,4) avg_net_pct
      FROM market_research_v8_outcomes
      WHERE event_type <> 'NORMAL_CONTROL'
      GROUP BY event_type, split_part(pattern_key,'|',6)
      HAVING count(*) >= 30
      ORDER BY event_type,n DESC
    """,
    "session": """
      SELECT event_type, split_part(pattern_key,'|',7) value,
             count(*)::int n,
             round(avg(CASE WHEN success THEN 1.0 ELSE 0.0 END)::numeric*100,2) win_pct,
             round(avg(net_pct)::numeric,4) avg_net_pct
      FROM market_research_v8_outcomes
      WHERE event_type <> 'NORMAL_CONTROL'
      GROUP BY event_type, split_part(pattern_key,'|',7)
      HAVING count(*) >= 30
      ORDER BY event_type,n DESC
    """,
}

def iso():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

def rows(cur):
    cols = [d.name for d in cur.description]
    out = []
    for row in cur.fetchall():
        item = {}
        for k, v in zip(cols, row):
            if hasattr(v, "as_tuple"):
                v = float(v)
            item[k] = v
        out.append(item)
    return out

def refresh():
    if not DBURL:
        STATE["status"] = "error"
        STATE["errors"] = [{"time": iso(), "error": "DATABASE_URL missing"}]
        return

    try:
        with psycopg.connect(DBURL, connect_timeout=10) as con:
            with con.cursor() as cur:
                cur.execute("select count(*)::int from market_research_v8_outcomes")
                STATE["outcomes"] = cur.fetchone()[0]

                cur.execute(MAIN_SQL)
                STATE["main"] = rows(cur)

                adjustments = {}
                for name, sql in DIM_SQL.items():
                    cur.execute(sql)
                    adjustments[name] = rows(cur)

                STATE["adjustments"] = adjustments

        STATE["status"] = "ok"
        STATE["updated_at"] = iso()
        STATE["errors"] = []

    except Exception as exc:
        STATE["status"] = "error"
        STATE["errors"] = [{
            "time": iso(),
            "error": f"{type(exc).__name__}: {str(exc)[:300]}"
        }]

def loop():
    while True:
        refresh()
        time.sleep(max(15, REFRESH_SEC))

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps({
            **STATE,
            "learning_model": {
                "main_layer": "event_type",
                "main_classes": [
                    "DOWN_IMPULSE",
                    "UP_IMPULSE",
                    "BREAKOUT_DOWN",
                    "BREAKOUT_UP",
                    "UPPER_WICK",
                    "LOWER_WICK",
                    "BOTH_WICK"
                ],
                "secondary_adjustments": [
                    "trend",
                    "volume",
                    "spread",
                    "regime",
                    "atr",
                    "session"
                ],
                "rule": (
                    "secondary dimensions are score adjustments, "
                    "not separate primary patterns"
                ),
                "min_adjustment_sample": 30,
                "raw_fine_patterns_preserved": True
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
    threading.Thread(target=loop, daemon=True).start()
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
