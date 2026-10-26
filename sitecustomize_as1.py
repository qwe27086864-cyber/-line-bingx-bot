"""AS1 paper-simulation safety patch.

Fixes one structural observability bug only:
when primary AS1 inventory reaches its notional cap, one quote side is disabled.
The original persist_snapshot() returned immediately whenever either quote side
was None, freezing primary snapshots/state/PnL even though the service and the
remaining unwind quote were still active.

This patch keeps the strategy/risk parameters unchanged. It makes snapshot quote
columns nullable and allows one-sided snapshots to be persisted.
"""

import os
import sys
from pathlib import Path


def _patch_as1_source():
    if Path(str(sys.argv[0] or "")).name.lower() != "as_mm_job.py":
        return

    source = Path(__file__).resolve().with_name("as_mm_job.py")
    if not source.exists():
        print("AS1 SNAPSHOT PATCH: as_mm_job.py not found", flush=True)
        return

    text = source.read_text(encoding="utf-8")
    original = text

    # Future/new DB creation: one-sided market-making quotes are valid states.
    text = text.replace(
        "reservation_price DOUBLE PRECISION NOT NULL, bid_quote DOUBLE PRECISION NOT NULL,\n"
        "                ask_quote DOUBLE PRECISION NOT NULL, model_half_spread_bps DOUBLE PRECISION NOT NULL,",
        "reservation_price DOUBLE PRECISION NOT NULL, bid_quote DOUBLE PRECISION,\n"
        "                ask_quote DOUBLE PRECISION, model_half_spread_bps DOUBLE PRECISION NOT NULL,",
    )

    # Core fix: q itself must exist, but bid OR ask may legitimately be None at
    # the inventory limit. Persist the snapshot/state anyway so PnL stays live.
    text = text.replace(
        '        if not q or q["bid"] is None or q["ask"] is None: return\n',
        '        if not q: return\n',
    )

    if text != original:
        source.write_text(text, encoding="utf-8")
        print("AS1 SNAPSHOT SOURCE PATCHED | one-sided snapshots enabled", flush=True)
    else:
        print("AS1 SNAPSHOT SOURCE PATCH already present / marker not found", flush=True)

    # Existing table migration. Do it after checking the target script so this
    # never touches DB during unrelated Python/pip processes.
    database_url = os.environ.get("DATABASE_URL", "")
    if not database_url:
        print("AS1 SNAPSHOT PATCH: DATABASE_URL missing; schema migration skipped", flush=True)
        return

    try:
        import psycopg2
        conn = psycopg2.connect(database_url, sslmode="require")
        cur = conn.cursor()
        try:
            cur.execute("ALTER TABLE IF EXISTS as_mm_snapshots ALTER COLUMN bid_quote DROP NOT NULL;")
            cur.execute("ALTER TABLE IF EXISTS as_mm_snapshots ALTER COLUMN ask_quote DROP NOT NULL;")
            conn.commit()
        finally:
            cur.close()
            conn.close()
        print("AS1 SNAPSHOT PATCH ACTIVE | schema nullable | strategy unchanged", flush=True)
    except Exception as exc:
        print(f"AS1 SNAPSHOT PATCH DB ERROR: {type(exc).__name__}: {exc}", flush=True)


try:
    _patch_as1_source()
except Exception as exc:
    print(f"AS1 SNAPSHOT PATCH ERROR: {type(exc).__name__}: {exc}", flush=True)
