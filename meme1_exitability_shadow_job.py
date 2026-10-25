"""MEME1X Exitability Shadow V1 — forward-only, paper-only.

Reads only fresh MEME1 observations that the main core accepted with decision='OPEN'.
The original MEME1 is untouched. Candidates must survive an exitability confirmation
window before MEME1X opens an independent paper position.
"""

import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import meme1_new_pool_job as base

ENGINE = "MEME1X"
VARIANT = "EXITABILITY_V2"
PAPER_ONLY = True

POLL_SEC = float(os.environ.get("MEME1X_POLL_SEC", "15"))
HEALTH_PORT = int(os.environ.get("PORT", "10000"))
NOTIONAL_USDT = float(os.environ.get("MEME1X_NOTIONAL_USDT", str(base.NOTIONAL_USDT)))
MAX_OPEN = int(os.environ.get("MEME1X_MAX_OPEN", "10"))

MIN_CONFIRM_LIQUIDITY_USD = float(os.environ.get("MEME1X_MIN_CONFIRM_LIQUIDITY_USD", "15000"))
BASE_CONFIRM_SEC = float(os.environ.get("MEME1X_BASE_CONFIRM_SEC", "60"))
RISKY_LP_CONFIRM_SEC = float(os.environ.get("MEME1X_RISKY_LP_CONFIRM_SEC", "180"))
UNKNOWN_LP_CONFIRM_SEC = float(os.environ.get("MEME1X_UNKNOWN_LP_CONFIRM_SEC", "120"))
MAX_PENDING_SEC = float(os.environ.get("MEME1X_MAX_PENDING_SEC", "300"))
MIN_CURRENT_LIQ_RATIO = float(os.environ.get("MEME1X_MIN_CURRENT_LIQ_RATIO", "0.80"))
MIN_PATH_LIQ_RATIO = float(os.environ.get("MEME1X_MIN_PATH_LIQ_RATIO", "0.70"))
HARD_COLLAPSE_RATIO = float(os.environ.get("MEME1X_HARD_COLLAPSE_RATIO", "0.40"))
MAX_EXIT_EXECUTION_BPS = float(os.environ.get("MEME1X_MAX_EXIT_EXECUTION_BPS", "400"))
MIN_IMMEDIATE_ROUND_TRIP_PCT = float(os.environ.get("MEME1X_MIN_IMMEDIATE_ROUND_TRIP_PCT", "-8"))
MIN_CONFIRM_SAMPLES = int(os.environ.get("MEME1X_MIN_CONFIRM_SAMPLES", "4"))
LP_LOCKED_SAFE_PCT = float(os.environ.get("MEME1X_LP_LOCKED_SAFE_PCT", "50"))
SOURCE_BATCH_LIMIT = int(os.environ.get("MEME1X_SOURCE_BATCH_LIMIT", "500"))

STOP_LOSS_PCT = float(os.environ.get("MEME1X_STOP_LOSS_PCT", str(base.STOP_LOSS_PCT)))
TAKE_PROFIT_PCT = float(os.environ.get("MEME1X_TAKE_PROFIT_PCT", str(base.TAKE_PROFIT_PCT)))
TRAIL_ACTIVATE_PCT = float(os.environ.get("MEME1X_TRAIL_ACTIVATE_PCT", str(base.TRAIL_ACTIVATE_PCT)))
TRAIL_DISTANCE_PCT = float(os.environ.get("MEME1X_TRAIL_DISTANCE_PCT", str(base.TRAIL_DISTANCE_PCT)))
MAX_HOLD_MIN = float(os.environ.get("MEME1X_MAX_HOLD_MIN", str(base.MAX_HOLD_MIN)))
LIQUIDITY_COLLAPSE_RATIO = float(os.environ.get("MEME1X_LIQUIDITY_COLLAPSE_RATIO", str(base.LIQUIDITY_COLLAPSE_RATIO)))
RAPID_LIQUIDITY_DROP_RATIO = float(os.environ.get("MEME1X_RAPID_LIQUIDITY_DROP_RATIO", str(base.RAPID_LIQUIDITY_DROP_RATIO)))
FEE_BPS_PER_SIDE = base.FEE_BPS_PER_SIDE


def db():
    return base.db()


def sf(v, default=0.0):
    return base._safe_float(v, default)


def utcnow():
    return datetime.now(timezone.utc)


def parse_json(v):
    if isinstance(v, dict):
        return v
    if not v:
        return {}
    try:
        return json.loads(v)
    except Exception:
        return {}


def init_database():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meme1x_state (
                singleton_id SMALLINT PRIMARY KEY DEFAULT 1,
                last_source_observation_id BIGINT NOT NULL DEFAULT 0,
                initialized_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                meta JSONB
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meme1x_candidates (
                id BIGSERIAL PRIMARY KEY,
                source_observation_id BIGINT NOT NULL UNIQUE,
                source_observed_at TIMESTAMPTZ NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_checked_at TIMESTAMPTZ,
                status VARCHAR(30) NOT NULL DEFAULT 'PENDING',
                decision_reason VARCHAR(120),
                pool_address VARCHAR(80) NOT NULL,
                mint_address VARCHAR(80) NOT NULL,
                pool_name TEXT,
                dex_id VARCHAR(80),
                signal_price_usd DOUBLE PRECISION,
                signal_liquidity_usd DOUBLE PRECISION,
                signal_market_score DOUBLE PRECISION,
                signal_rug_score DOUBLE PRECISION,
                lp_locked_pct DOUBLE PRECISION,
                unlocked_lp BOOLEAN NOT NULL DEFAULT FALSE,
                low_liquidity_danger BOOLEAN NOT NULL DEFAULT FALSE,
                confirm_required_sec DOUBLE PRECISION NOT NULL,
                sample_count INTEGER NOT NULL DEFAULT 0,
                min_liquidity_usd DOUBLE PRECISION,
                max_liquidity_usd DOUBLE PRECISION,
                current_liquidity_usd DOUBLE PRECISION,
                current_price_usd DOUBLE PRECISION,
                current_exit_execution_bps DOUBLE PRECISION,
                immediate_round_trip_pct DOUBLE PRECISION,
                opened_position_id BIGINT,
                meta JSONB
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme1x_candidate_status ON meme1x_candidates(status, created_at);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meme1x_positions (
                id BIGSERIAL PRIMARY KEY,
                engine_version VARCHAR(20) NOT NULL,
                variant VARCHAR(40) NOT NULL,
                candidate_id BIGINT NOT NULL,
                source_observation_id BIGINT NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
                pool_address VARCHAR(80) NOT NULL,
                mint_address VARCHAR(80) NOT NULL,
                pool_name TEXT,
                dex_id VARCHAR(80),
                opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                closed_at TIMESTAMPTZ,
                notional_usdt DOUBLE PRECISION NOT NULL,
                token_qty DOUBLE PRECISION NOT NULL,
                raw_entry_price DOUBLE PRECISION NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                entry_fee_usdt DOUBLE PRECISION NOT NULL,
                entry_liquidity_usd DOUBLE PRECISION,
                entry_execution_bps DOUBLE PRECISION,
                peak_price DOUBLE PRECISION NOT NULL,
                last_price DOUBLE PRECISION,
                last_liquidity_usd DOUBLE PRECISION,
                exit_price DOUBLE PRECISION,
                exit_fee_usdt DOUBLE PRECISION,
                exit_execution_bps DOUBLE PRECISION,
                gross_pnl_usdt DOUBLE PRECISION,
                net_pnl_usdt DOUBLE PRECISION,
                net_return_pct DOUBLE PRECISION,
                exit_reason VARCHAR(80),
                meta JSONB
            );
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_meme1x_open_pool ON meme1x_positions(pool_address) WHERE status='OPEN';")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme1x_pos_time ON meme1x_positions(opened_at DESC);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meme1x_position_snapshots (
                id BIGSERIAL PRIMARY KEY,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                engine_version VARCHAR(20) NOT NULL,
                variant VARCHAR(40) NOT NULL,
                position_id BIGINT NOT NULL,
                pool_address VARCHAR(80) NOT NULL,
                mint_address VARCHAR(80) NOT NULL,
                raw_price_usd DOUBLE PRECISION,
                executable_exit_price_usd DOUBLE PRECISION,
                liquidity_usd DOUBLE PRECISION,
                exit_execution_bps DOUBLE PRECISION,
                raw_roi_pct DOUBLE PRECISION,
                executable_net_roi_pct DOUBLE PRECISION,
                peak_price_usd DOUBLE PRECISION,
                peak_roi_pct DOUBLE PRECISION,
                trail_floor_price_usd DOUBLE PRECISION,
                holding_minutes DOUBLE PRECISION,
                exit_reason_now VARCHAR(80),
                market_data_ok BOOLEAN NOT NULL DEFAULT TRUE,
                meta JSONB
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme1x_snap_pos_time ON meme1x_position_snapshots(position_id, observed_at DESC);")
        conn.commit()
    finally:
        cur.close(); conn.close()


def get_state():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT last_source_observation_id FROM meme1x_state WHERE singleton_id=1;")
        row = cur.fetchone()
        return int(row[0]) if row else None
    finally:
        cur.close(); conn.close()


def seed_state():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(MAX(id),0) FROM meme1_observations;")
        latest = int(cur.fetchone()[0] or 0)
        cur.execute("""
            INSERT INTO meme1x_state(singleton_id,last_source_observation_id,meta)
            VALUES(1,%s,%s) ON CONFLICT(singleton_id) DO NOTHING;
        """, (latest, json.dumps({"engine":ENGINE,"variant":VARIANT,"forward_only":True,"seeded_at":utcnow().isoformat()})))
        conn.commit(); return latest
    finally:
        cur.close(); conn.close()


def advance_state(source_id):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("UPDATE meme1x_state SET last_source_observation_id=%s,updated_at=NOW() WHERE singleton_id=1;", (int(source_id),))
        conn.commit()
    finally:
        cur.close(); conn.close()


def load_new_source_rows(after_id):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id,observed_at,pool_address,mint_address,pool_name,dex_id,
                   price_usd,liquidity_usd,market_score,rug_score,rug_level,
                   decision,reason,meta
            FROM meme1_observations WHERE id>%s ORDER BY id ASC LIMIT %s;
        """, (int(after_id), SOURCE_BATCH_LIMIT))
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close(); conn.close()


def risk_flags(meta):
    risk = (parse_json(meta).get("risk") or {})
    risks = risk.get("risks") or []
    lp_locked = None
    raw = risk.get("lp_locked_pct")
    if raw not in (None, ""):
        try: lp_locked = float(raw)
        except Exception: lp_locked = None
    unlocked = False; low_liq_danger = False; normalized = []
    for item in risks:
        if not isinstance(item, dict): continue
        name = str(item.get("name") or "")
        level = str(item.get("level") or "").lower()
        normalized.append({"name":name,"level":level,"value":item.get("value"),"description":item.get("description")})
        if name == "Large Amount of LP Unlocked": unlocked = True
        if name == "Low Liquidity" and level == "danger": low_liq_danger = True
    return lp_locked, unlocked, low_liq_danger, normalized


def confirmation_seconds(lp_locked, unlocked):
    if unlocked: return RISKY_LP_CONFIRM_SEC
    if lp_locked is None: return UNKNOWN_LP_CONFIRM_SEC
    if lp_locked < LP_LOCKED_SAFE_PCT: return RISKY_LP_CONFIRM_SEC
    return BASE_CONFIRM_SEC


def ingest_source_row(src):
    if src.get("decision") != "OPEN": return False
    lp_locked, unlocked, low_liq_danger, risks = risk_flags(src.get("meta"))
    required = confirmation_seconds(lp_locked, unlocked)
    signal_liq = sf(src.get("liquidity_usd"))
    status, reason = "PENDING", "WAIT_CONFIRMATION"
    if low_liq_danger:
        status, reason = "REJECTED", "RUGCHECK_DANGER_LOW_LIQUIDITY"
    elif signal_liq < MIN_CONFIRM_LIQUIDITY_USD:
        status, reason = "REJECTED", "SIGNAL_LIQUIDITY_TOO_LOW"
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO meme1x_candidates(
                source_observation_id,source_observed_at,status,decision_reason,
                pool_address,mint_address,pool_name,dex_id,
                signal_price_usd,signal_liquidity_usd,signal_market_score,signal_rug_score,
                lp_locked_pct,unlocked_lp,low_liquidity_danger,confirm_required_sec,
                min_liquidity_usd,max_liquidity_usd,current_liquidity_usd,current_price_usd,meta
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(source_observation_id) DO NOTHING;
        """, (
            src["id"],src["observed_at"],status,reason,src["pool_address"],src["mint_address"],src.get("pool_name"),src.get("dex_id"),
            src.get("price_usd"),src.get("liquidity_usd"),src.get("market_score"),src.get("rug_score"),lp_locked,unlocked,low_liq_danger,required,
            signal_liq if signal_liq>0 else None,signal_liq if signal_liq>0 else None,signal_liq if signal_liq>0 else None,src.get("price_usd"),
            json.dumps({"paper_only":True,"variant":VARIANT,"rug_level":src.get("rug_level"),"rug_risks":risks,"source_meta":parse_json(src.get("meta"))})
        ))
        conn.commit()
    finally:
        cur.close(); conn.close()
    print(f"MEME1X SOURCE #{src['id']} {src.get('pool_name')} status={status} reason={reason} confirm={required:.0f}s liq=${signal_liq:.0f} locked={lp_locked}", flush=True)
    return True


def load_pending_candidates():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM meme1x_candidates WHERE status='PENDING' ORDER BY id ASC;")
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close(); conn.close()


def shadow_open_count():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM meme1x_positions WHERE status='OPEN';")
        return int(cur.fetchone()[0])
    finally:
        cur.close(); conn.close()


def round_trip_metrics(raw_price, liquidity_usd):
    entry_price, entry_bps = base.executable_entry(raw_price, NOTIONAL_USDT, liquidity_usd)
    entry_fee = NOTIONAL_USDT * FEE_BPS_PER_SIDE / 10000.0
    spend_after_fee = max(0.0, NOTIONAL_USDT - entry_fee)
    qty = spend_after_fee / entry_price if entry_price > 0 else 0.0
    exit_price, exit_bps = base.executable_exit(raw_price, qty, liquidity_usd)
    gross_value = qty * exit_price
    exit_fee = gross_value * FEE_BPS_PER_SIDE / 10000.0
    net = gross_value - exit_fee - NOTIONAL_USDT
    ret = net / NOTIONAL_USDT * 100.0 if NOTIONAL_USDT > 0 else -100.0
    return {"entry_price":entry_price,"entry_bps":entry_bps,"qty":qty,"exit_price":exit_price,"exit_bps":exit_bps,"round_trip_pct":ret}


def mark_candidate(candidate_id,status,reason,sample_count,min_liq,max_liq,current_liq,current_price,exit_bps,round_trip_pct,opened_position_id=None):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE meme1x_candidates SET status=%s,decision_reason=%s,last_checked_at=NOW(),sample_count=%s,
                min_liquidity_usd=%s,max_liquidity_usd=%s,current_liquidity_usd=%s,current_price_usd=%s,
                current_exit_execution_bps=%s,immediate_round_trip_pct=%s,
                opened_position_id=COALESCE(%s,opened_position_id)
            WHERE id=%s;
        """, (status,reason,sample_count,min_liq,max_liq,current_liq,current_price,exit_bps,round_trip_pct,opened_position_id,candidate_id))
        conn.commit()
    finally:
        cur.close(); conn.close()


def create_shadow_position(c,p,m):
    raw = sf(p["price"]); liq = sf(p["liq"])
    entry_fee = NOTIONAL_USDT * FEE_BPS_PER_SIDE / 10000.0
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO meme1x_positions(
                engine_version,variant,candidate_id,source_observation_id,pool_address,mint_address,pool_name,dex_id,
                notional_usdt,token_qty,raw_entry_price,entry_price,entry_fee_usdt,entry_liquidity_usd,entry_execution_bps,
                peak_price,last_price,last_liquidity_usd,meta
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
        """, (
            ENGINE,VARIANT,c["id"],c["source_observation_id"],c["pool_address"],c["mint_address"],c.get("pool_name"),c.get("dex_id"),
            NOTIONAL_USDT,m["qty"],raw,m["entry_price"],entry_fee,liq,m["entry_bps"],raw,raw,liq,
            json.dumps({"paper_only":True,"variant":VARIANT,"confirm_required_sec":c["confirm_required_sec"],"confirmation_samples":c["sample_count"]+1,
                        "signal_liquidity_usd":c["signal_liquidity_usd"],"lp_locked_pct":c["lp_locked_pct"],"unlocked_lp":c["unlocked_lp"],
                        "immediate_round_trip_pct":m["round_trip_pct"],"entry_exit_execution_bps":m["exit_bps"],"execution_model":"constant_product_liquidity_impact_v2"})
        ))
        pid = int(cur.fetchone()[0]); conn.commit(); return pid
    finally:
        cur.close(); conn.close()


def evaluate_pending_candidates():
    pending = load_pending_candidates()
    if not pending: return 0,0,0
    snapshots = base.fetch_pools_by_addresses([c["pool_address"] for c in pending])
    opened = 0; rejected = 0
    for c in pending:
        p = snapshots.get(c["pool_address"])
        age_sec = max(0.0,(utcnow()-c["created_at"]).total_seconds())
        if not p or sf(p.get("price")) <= 0:
            if age_sec >= MAX_PENDING_SEC:
                mark_candidate(c["id"],"EXPIRED","MARKET_DATA_MISSING_TIMEOUT",c["sample_count"],c["min_liquidity_usd"],c["max_liquidity_usd"],c["current_liquidity_usd"],c["current_price_usd"],c["current_exit_execution_bps"],c["immediate_round_trip_pct"])
                rejected += 1
            continue
        current_liq = sf(p["liq"]); current_price = sf(p["price"]); signal_liq = max(sf(c["signal_liquidity_usd"]),1e-9)
        min_liq = min(sf(c["min_liquidity_usd"],current_liq), current_liq)
        max_liq = max(sf(c["max_liquidity_usd"],current_liq), current_liq)
        samples = int(c["sample_count"] or 0)+1
        m = round_trip_metrics(current_price,current_liq)
        current_ratio = current_liq/signal_liq; path_ratio = min_liq/signal_liq
        status, reason = "PENDING", "WAIT_CONFIRMATION"
        if current_liq < MIN_CONFIRM_LIQUIDITY_USD:
            status, reason = "REJECTED", "CONFIRM_LIQUIDITY_TOO_LOW"
        elif current_ratio < HARD_COLLAPSE_RATIO:
            status, reason = "REJECTED", "LIQUIDITY_COLLAPSED_DURING_CONFIRM"
        elif m["exit_bps"] > MAX_EXIT_EXECUTION_BPS:
            status, reason = "REJECTED", "EXIT_IMPACT_TOO_HIGH"
        elif m["round_trip_pct"] < MIN_IMMEDIATE_ROUND_TRIP_PCT:
            status, reason = "REJECTED", "ROUND_TRIP_TOO_EXPENSIVE"
        elif age_sec >= MAX_PENDING_SEC:
            status, reason = "EXPIRED", "CONFIRMATION_TIMEOUT"
        elif age_sec >= sf(c["confirm_required_sec"]) and samples >= MIN_CONFIRM_SAMPLES:
            if current_ratio < MIN_CURRENT_LIQ_RATIO:
                status, reason = "REJECTED", "CURRENT_LIQUIDITY_UNSTABLE"
            elif path_ratio < MIN_PATH_LIQ_RATIO:
                status, reason = "REJECTED", "PATH_LIQUIDITY_UNSTABLE"
            elif shadow_open_count() >= MAX_OPEN:
                status, reason = "PENDING", "MAX_OPEN_WAIT"
            else:
                try:
                    pid = create_shadow_position(c,p,m)
                    mark_candidate(c["id"],"OPENED","EXITABILITY_PASS",samples,min_liq,max_liq,current_liq,current_price,m["exit_bps"],m["round_trip_pct"],pid)
                    opened += 1
                    print(f"MEME1X OPEN #{pid} {c.get('pool_name')} confirm={age_sec:.0f}s samples={samples} liq=${current_liq:.0f} ratio={current_ratio:.2f} exit_bps={m['exit_bps']:.0f} rt={m['round_trip_pct']:+.2f}%", flush=True)
                    continue
                except Exception as exc:
                    status, reason = "REJECTED", f"OPEN_ERROR:{type(exc).__name__}"
        mark_candidate(c["id"],status,reason,samples,min_liq,max_liq,current_liq,current_price,m["exit_bps"],m["round_trip_pct"])
        if status in ("REJECTED","EXPIRED"):
            rejected += 1
            print(f"MEME1X {status} {c.get('pool_name')} reason={reason} age={age_sec:.0f}s liq=${current_liq:.0f} ratio={current_ratio:.2f} exit_bps={m['exit_bps']:.0f}", flush=True)
    return len(pending), opened, rejected


def load_open_positions():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM meme1x_positions WHERE status='OPEN' ORDER BY id ASC;")
        cols=[d.name for d in cur.description]
        return [dict(zip(cols,row)) for row in cur.fetchall()]
    finally:
        cur.close(); conn.close()


def position_exit_reason(pos,p):
    entry=sf(pos["raw_entry_price"]); price=sf(p["price"])
    if entry<=0 or price<=0: return None
    peak=max(sf(pos.get("peak_price"),entry),price)
    peak_roi=(peak/entry-1.0)*100.0
    trail_floor=peak*(1.0-TRAIL_DISTANCE_PCT/100.0)
    age_min=(utcnow()-pos["opened_at"]).total_seconds()/60.0
    entry_liq=sf(pos.get("entry_liquidity_usd"))
    last_liq=sf(pos.get("last_liquidity_usd"),entry_liq)
    current_liq=sf(p.get("liq"))

    if entry_liq>0 and current_liq < entry_liq*LIQUIDITY_COLLAPSE_RATIO:
        return "LIQUIDITY_COLLAPSE"
    if (entry_liq>0 and last_liq>0 and
            current_liq < last_liq*RAPID_LIQUIDITY_DROP_RATIO and
            current_liq < entry_liq*0.90):
        return "LIQUIDITY_DRAIN"

    exit_price,_=base.executable_exit(price,pos["token_qty"],current_liq)
    gross_value=sf(pos["token_qty"])*exit_price
    exit_fee=gross_value*FEE_BPS_PER_SIDE/10000.0
    notional=sf(pos["notional_usdt"])
    executable_net_roi=(gross_value-exit_fee-notional)/notional*100.0 if notional>0 else -100.0
    if executable_net_roi <= -STOP_LOSS_PCT:
        return "STOP_LOSS_EXECUTABLE"

    raw_roi=(price/entry-1.0)*100.0
    if peak_roi >= TRAIL_ACTIVATE_PCT and price <= trail_floor: return "TRAILING_STOP"
    if raw_roi >= TAKE_PROFIT_PCT: return "TAKE_PROFIT"
    if age_min >= MAX_HOLD_MIN: return "MAX_HOLD"
    return None


def log_position_snapshot(pos,p=None,peak=None,reason_now=None):
    raw_price=sf((p or {}).get("price"),0.0); liq=sf((p or {}).get("liq"),0.0)
    entry=sf(pos.get("raw_entry_price")); qty=sf(pos.get("token_qty")); notional=sf(pos.get("notional_usdt")); market_ok=raw_price>0
    if peak is None: peak=max(sf(pos.get("peak_price"),entry), raw_price if raw_price>0 else 0.0)
    raw_roi=((raw_price/entry)-1.0)*100.0 if entry>0 and raw_price>0 else None
    peak_roi=((peak/entry)-1.0)*100.0 if entry>0 and peak>0 else None
    trail_floor=peak*(1.0-TRAIL_DISTANCE_PCT/100.0) if peak>0 else None
    holding_min=max(0.0,(utcnow()-pos["opened_at"]).total_seconds()/60.0)
    executable_exit_price=None; execution_bps=None; executable_net_roi=None
    if market_ok and qty>0 and notional>0:
        executable_exit_price,execution_bps=base.executable_exit(raw_price,qty,liq)
        gross_value=qty*executable_exit_price; exit_fee=gross_value*FEE_BPS_PER_SIDE/10000.0
        executable_net_roi=(gross_value-exit_fee-notional)/notional*100.0
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            INSERT INTO meme1x_position_snapshots(
                engine_version,variant,position_id,pool_address,mint_address,raw_price_usd,executable_exit_price_usd,liquidity_usd,
                exit_execution_bps,raw_roi_pct,executable_net_roi_pct,peak_price_usd,peak_roi_pct,trail_floor_price_usd,holding_minutes,
                exit_reason_now,market_data_ok,meta
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (ENGINE,VARIANT,pos["id"],pos["pool_address"],pos["mint_address"],raw_price if raw_price>0 else None,executable_exit_price,liq if liq>0 else None,
                execution_bps,raw_roi,executable_net_roi,peak if peak>0 else None,peak_roi,trail_floor,holding_min,reason_now,market_ok,
                json.dumps({"paper_only":True,"variant":VARIANT,"execution_model":"constant_product_liquidity_impact_v2"})))
        conn.commit()
    finally:
        cur.close(); conn.close()


def update_position_mark(pos,p,peak):
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("UPDATE meme1x_positions SET last_price=%s,last_liquidity_usd=%s,peak_price=%s WHERE id=%s AND status='OPEN';", (p["price"],p["liq"],peak,pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()


def close_position(pos,p,reason):
    raw_exit=sf(p["price"]); liq=sf(p["liq"])
    exit_price,execution_bps=base.executable_exit(raw_exit,pos["token_qty"],liq)
    gross_value=sf(pos["token_qty"])*exit_price; exit_fee=gross_value*FEE_BPS_PER_SIDE/10000.0
    net=gross_value-exit_fee-sf(pos["notional_usdt"]); ret=net/sf(pos["notional_usdt"])*100.0
    gross=sf(pos["token_qty"])*raw_exit-sf(pos["notional_usdt"])
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            UPDATE meme1x_positions SET status='CLOSED',closed_at=NOW(),last_price=%s,last_liquidity_usd=%s,
                exit_price=%s,exit_fee_usdt=%s,exit_execution_bps=%s,gross_pnl_usdt=%s,net_pnl_usdt=%s,net_return_pct=%s,exit_reason=%s,
                meta=COALESCE(meta,'{}'::jsonb) || %s::jsonb WHERE id=%s AND status='OPEN';
        """, (raw_exit,liq,exit_price,exit_fee,execution_bps,gross,net,ret,reason,
                json.dumps({"exit_execution_bps":execution_bps,"execution_model":"constant_product_liquidity_impact_v2"}),pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return net,ret


def monitor_positions():
    opens=load_open_positions()
    if not opens: return 0,0
    snapshots=base.fetch_pools_by_addresses([p["pool_address"] for p in opens]); closed=0
    for pos in opens:
        p=snapshots.get(pos["pool_address"])
        if not p or sf(p.get("price"))<=0:
            log_position_snapshot(pos,None,peak=sf(pos.get("peak_price"),sf(pos["raw_entry_price"])),reason_now="MARKET_DATA_MISSING"); continue
        peak=max(sf(pos.get("peak_price"),sf(p["price"])),sf(p["price"])); reason=position_exit_reason(pos,p)
        log_position_snapshot(pos,p,peak=peak,reason_now=reason)
        if reason:
            net,ret=close_position(pos,p,reason); closed+=1
            print(f"MEME1X CLOSE #{pos['id']} {pos.get('pool_name')} {reason} net={net:+.4f}U ret={ret:+.2f}%", flush=True)
        else:
            update_position_mark(pos,p,peak)
    return len(opens),closed


def ingest_new_sources():
    state=get_state()
    if state is None:
        latest=seed_state(); print(f"MEME1X FIRST RUN: watermark seeded at observation #{latest}. Historical MEME1 signals were NOT replayed.", flush=True); return 0
    rows=load_new_source_rows(state); accepted=0
    for src in rows:
        try:
            if ingest_source_row(src): accepted+=1
        except Exception as exc:
            print("MEME1X SOURCE ERROR:",src.get("id"),src.get("pool_name"),type(exc).__name__,str(exc),flush=True)
        finally:
            advance_state(src["id"])
    return accepted


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/","/health"):
            body=json.dumps({"status":"ok","engine":ENGINE,"variant":VARIANT,"paper_only":True,"poll_sec":POLL_SEC,"open_positions":shadow_open_count()}).encode()
            self.send_response(200); self.send_header("Content-Type","application/json"); self.send_header("Content-Length",str(len(body))); self.end_headers(); self.wfile.write(body)
        else:
            self.send_response(404); self.end_headers()
    def log_message(self,fmt,*args): return


def start_health_server():
    server=ThreadingHTTPServer(("0.0.0.0",HEALTH_PORT),HealthHandler)
    threading.Thread(target=server.serve_forever,daemon=True).start(); return server


def run_monitor():
    init_database(); start_health_server()
    print("="*78,flush=True)
    print("MEME1X EXITABILITY V2 | FORWARD ONLY | PAPER ONLY | CONFIRMED ENTRY + EXECUTABLE EXIT",flush=True)
    print(f"base_confirm={BASE_CONFIRM_SEC:.0f}s risky_lp_confirm={RISKY_LP_CONFIRM_SEC:.0f}s min_liq=${MIN_CONFIRM_LIQUIDITY_USD:.0f} min_current_ratio={MIN_CURRENT_LIQ_RATIO:.2f} min_path_ratio={MIN_PATH_LIQ_RATIO:.2f} max_exit_bps={MAX_EXIT_EXECUTION_BPS:.0f}",flush=True)
    print("="*78,flush=True)
    while True:
        started=time.time()
        try:
            sources=ingest_new_sources(); pending,opened,rejected=evaluate_pending_candidates(); open_count,closed=monitor_positions()
            if sources or pending or opened or rejected or open_count or closed:
                print(f"MEME1X LOOP sources={sources} pending={pending} opened={opened} rejected={rejected} open_positions={open_count} closed={closed}",flush=True)
        except Exception as exc:
            print(f"MEME1X LOOP ERROR {type(exc).__name__}: {exc}",flush=True)
        time.sleep(max(1.0,POLL_SEC-(time.time()-started)))


def run_once():
    init_database(); ingest_new_sources(); evaluate_pending_candidates(); monitor_positions()


if __name__ == "__main__":
    if "--once" in sys.argv: run_once()
    else: run_monitor()
