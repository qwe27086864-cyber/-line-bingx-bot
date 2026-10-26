"""
MEME1 shadow monitor: EXITABILITY_V2 baseline + SURVIVAL_V3 forward-only shadow.

PAPER ONLY.
- V2 keeps the existing exitability gate and existing meme1x_* tables.
- V3 is independent and writes to meme1v3_* tables.
- Both consume fresh MEME1 observations with decision='OPEN'.
- V3 changes ENTRY only; exit/execution logic stays the same as V2 so the
  comparison isolates whether the survival-confirmation entry improves expectancy.
"""

import json
import os
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import meme1_new_pool_job as base

PAPER_ONLY = True
POLL_SEC = float(os.environ.get("MEME1X_POLL_SEC", "15"))
HEALTH_PORT = int(os.environ.get("PORT", "10000"))
SOURCE_BATCH_LIMIT = int(os.environ.get("MEME1X_SOURCE_BATCH_LIMIT", "500"))
FETCH_CACHE_TTL_SEC = float(os.environ.get("MEME1X_FETCH_CACHE_TTL_SEC", "8"))

FEE_BPS_PER_SIDE = base.FEE_BPS_PER_SIDE


def envf(name, default):
    return float(os.environ.get(name, str(default)))


def envi(name, default):
    return int(os.environ.get(name, str(default)))


V2 = {
    "engine": "MEME1X",
    "variant": "EXITABILITY_V2",
    "prefix": "meme1x",
    "notional": envf("MEME1X_NOTIONAL_USDT", base.NOTIONAL_USDT),
    "max_open": envi("MEME1X_MAX_OPEN", 10),
    "min_confirm_liq": envf("MEME1X_MIN_CONFIRM_LIQUIDITY_USD", 15000),
    "base_confirm_sec": envf("MEME1X_BASE_CONFIRM_SEC", 60),
    "risky_confirm_sec": envf("MEME1X_RISKY_LP_CONFIRM_SEC", 180),
    "unknown_confirm_sec": envf("MEME1X_UNKNOWN_LP_CONFIRM_SEC", 120),
    "max_pending_sec": envf("MEME1X_MAX_PENDING_SEC", 300),
    "min_current_liq_ratio": envf("MEME1X_MIN_CURRENT_LIQ_RATIO", 0.80),
    "min_path_liq_ratio": envf("MEME1X_MIN_PATH_LIQ_RATIO", 0.70),
    "hard_collapse_ratio": envf("MEME1X_HARD_COLLAPSE_RATIO", 0.40),
    "max_exit_bps": envf("MEME1X_MAX_EXIT_EXECUTION_BPS", 400),
    "min_roundtrip_pct": envf("MEME1X_MIN_IMMEDIATE_ROUND_TRIP_PCT", -8),
    "min_samples": envi("MEME1X_MIN_CONFIRM_SAMPLES", 4),
    "lp_locked_safe_pct": envf("MEME1X_LP_LOCKED_SAFE_PCT", 50),
    "stop_loss_pct": envf("MEME1X_STOP_LOSS_PCT", base.STOP_LOSS_PCT),
    "take_profit_pct": envf("MEME1X_TAKE_PROFIT_PCT", base.TAKE_PROFIT_PCT),
    "trail_activate_pct": envf("MEME1X_TRAIL_ACTIVATE_PCT", base.TRAIL_ACTIVATE_PCT),
    "trail_distance_pct": envf("MEME1X_TRAIL_DISTANCE_PCT", base.TRAIL_DISTANCE_PCT),
    "max_hold_min": envf("MEME1X_MAX_HOLD_MIN", base.MAX_HOLD_MIN),
    "liq_collapse_ratio": envf("MEME1X_LIQUIDITY_COLLAPSE_RATIO", base.LIQUIDITY_COLLAPSE_RATIO),
    "rapid_liq_drop_ratio": envf("MEME1X_RAPID_LIQUIDITY_DROP_RATIO", base.RAPID_LIQUIDITY_DROP_RATIO),
    "survival_gate": False,
}

V3 = {
    "engine": "MEME1V3",
    "variant": "SURVIVAL_V3",
    "prefix": "meme1v3",
    "notional": envf("MEME1V3_NOTIONAL_USDT", base.NOTIONAL_USDT),
    "max_open": envi("MEME1V3_MAX_OPEN", 10),
    "min_confirm_liq": envf("MEME1V3_MIN_CONFIRM_LIQUIDITY_USD", 15000),
    "base_confirm_sec": envf("MEME1V3_BASE_CONFIRM_SEC", 180),
    "risky_confirm_sec": envf("MEME1V3_RISKY_LP_CONFIRM_SEC", 300),
    "unknown_confirm_sec": envf("MEME1V3_UNKNOWN_LP_CONFIRM_SEC", 240),
    "max_pending_sec": envf("MEME1V3_MAX_PENDING_SEC", 360),
    "min_current_liq_ratio": envf("MEME1V3_MIN_CURRENT_LIQ_RATIO", 0.90),
    "min_path_liq_ratio": envf("MEME1V3_MIN_PATH_LIQ_RATIO", 0.85),
    "hard_collapse_ratio": envf("MEME1V3_HARD_COLLAPSE_RATIO", 0.40),
    "max_exit_bps": envf("MEME1V3_MAX_EXIT_EXECUTION_BPS", 400),
    "min_roundtrip_pct": envf("MEME1V3_MIN_IMMEDIATE_ROUND_TRIP_PCT", -8),
    "min_samples": envi("MEME1V3_MIN_CONFIRM_SAMPLES", 8),
    "lp_locked_safe_pct": envf("MEME1V3_LP_LOCKED_SAFE_PCT", 50),
    "min_buy_sell_ratio": envf("MEME1V3_MIN_BUY_SELL_RATIO", 2.0),
    "min_vol5_liq_ratio": envf("MEME1V3_MIN_VOL5_LIQ_RATIO", 0.60),
    "min_price_ratio": envf("MEME1V3_MIN_PRICE_RATIO", 0.75),
    "max_price_ratio": envf("MEME1V3_MAX_PRICE_RATIO", 2.50),
    "stop_loss_pct": envf("MEME1V3_STOP_LOSS_PCT", V2["stop_loss_pct"]),
    "take_profit_pct": envf("MEME1V3_TAKE_PROFIT_PCT", V2["take_profit_pct"]),
    "trail_activate_pct": envf("MEME1V3_TRAIL_ACTIVATE_PCT", V2["trail_activate_pct"]),
    "trail_distance_pct": envf("MEME1V3_TRAIL_DISTANCE_PCT", V2["trail_distance_pct"]),
    "max_hold_min": envf("MEME1V3_MAX_HOLD_MIN", V2["max_hold_min"]),
    "liq_collapse_ratio": envf("MEME1V3_LIQUIDITY_COLLAPSE_RATIO", V2["liq_collapse_ratio"]),
    "rapid_liq_drop_ratio": envf("MEME1V3_RAPID_LIQUIDITY_DROP_RATIO", V2["rapid_liq_drop_ratio"]),
    "survival_gate": True,
}

ENGINES = (V2, V3)


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


def tn(cfg, suffix):
    return f"{cfg['prefix']}_{suffix}"


_fetch_cache = {}
_fetch_lock = threading.Lock()


def fetch_pools_cached(addresses):
    unique = [a for a in dict.fromkeys(addresses) if a]
    if not unique:
        return {}
    now = time.time()
    out = {}
    missing = []
    with _fetch_lock:
        for addr in unique:
            item = _fetch_cache.get(addr)
            if item and now - item[0] <= FETCH_CACHE_TTL_SEC:
                out[addr] = item[1]
            else:
                missing.append(addr)
    if missing:
        fresh = base.fetch_pools_by_addresses(missing)
        now2 = time.time()
        with _fetch_lock:
            for addr, pool in fresh.items():
                _fetch_cache[addr] = (now2, pool)
                out[addr] = pool
    return out


def init_database(cfg):
    state = tn(cfg, "state")
    candidates = tn(cfg, "candidates")
    positions = tn(cfg, "positions")
    snaps = tn(cfg, "position_snapshots")
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {state} (
                singleton_id SMALLINT PRIMARY KEY DEFAULT 1,
                last_source_observation_id BIGINT NOT NULL DEFAULT 0,
                initialized_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                meta JSONB
            );
        """)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {candidates} (
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
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{cfg['prefix']}_candidate_status ON {candidates}(status, created_at);")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {positions} (
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
        cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{cfg['prefix']}_open_pool ON {positions}(pool_address) WHERE status='OPEN';")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{cfg['prefix']}_pos_time ON {positions}(opened_at DESC);")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {snaps} (
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
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{cfg['prefix']}_snap_pos_time ON {snaps}(position_id, observed_at DESC);")
        if cfg["survival_gate"]:
            samples = tn(cfg, "candidate_samples")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {samples} (
                    id BIGSERIAL PRIMARY KEY,
                    candidate_id BIGINT NOT NULL,
                    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    age_sec DOUBLE PRECISION,
                    liquidity_usd DOUBLE PRECISION,
                    current_liq_ratio DOUBLE PRECISION,
                    path_liq_ratio DOUBLE PRECISION,
                    price_usd DOUBLE PRECISION,
                    price_ratio DOUBLE PRECISION,
                    volume_m5_usd DOUBLE PRECISION,
                    vol5_liq_ratio DOUBLE PRECISION,
                    buys_m5 INTEGER,
                    sells_m5 INTEGER,
                    buy_sell_ratio DOUBLE PRECISION,
                    exit_execution_bps DOUBLE PRECISION,
                    immediate_round_trip_pct DOUBLE PRECISION
                );
            """)
            cur.execute(f"CREATE INDEX IF NOT EXISTS idx_{cfg['prefix']}_sample_candidate_time ON {samples}(candidate_id, observed_at DESC);")
        conn.commit()
    finally:
        cur.close(); conn.close()


def get_state(cfg):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"SELECT last_source_observation_id FROM {tn(cfg,'state')} WHERE singleton_id=1;")
        row = cur.fetchone()
        return int(row[0]) if row else None
    finally:
        cur.close(); conn.close()


def seed_state(cfg):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("SELECT COALESCE(MAX(id),0) FROM meme1_observations;")
        latest = int(cur.fetchone()[0] or 0)
        cur.execute(f"""
            INSERT INTO {tn(cfg,'state')}(singleton_id,last_source_observation_id,meta)
            VALUES(1,%s,%s) ON CONFLICT(singleton_id) DO NOTHING;
        """, (latest, json.dumps({"engine":cfg["engine"],"variant":cfg["variant"],"forward_only":True,"seeded_at":utcnow().isoformat()})))
        conn.commit(); return latest
    finally:
        cur.close(); conn.close()


def advance_state(cfg, source_id):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"UPDATE {tn(cfg,'state')} SET last_source_observation_id=%s,updated_at=NOW() WHERE singleton_id=1;", (int(source_id),))
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
        try:
            lp_locked = float(raw)
        except Exception:
            lp_locked = None
    unlocked = False
    low_liq_danger = False
    normalized = []
    for item in risks:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "")
        level = str(item.get("level") or "").lower()
        normalized.append({"name":name,"level":level,"value":item.get("value"),"description":item.get("description")})
        if name == "Large Amount of LP Unlocked":
            unlocked = True
        if name == "Low Liquidity" and level == "danger":
            low_liq_danger = True
    return lp_locked, unlocked, low_liq_danger, normalized


def confirmation_seconds(cfg, lp_locked, unlocked):
    if unlocked:
        return cfg["risky_confirm_sec"]
    if lp_locked is None:
        return cfg["unknown_confirm_sec"]
    if lp_locked < cfg["lp_locked_safe_pct"]:
        return cfg["risky_confirm_sec"]
    return cfg["base_confirm_sec"]


def ingest_source_row(cfg, src):
    if src.get("decision") != "OPEN":
        return False
    lp_locked, unlocked, low_liq_danger, risks = risk_flags(src.get("meta"))
    required = confirmation_seconds(cfg, lp_locked, unlocked)
    signal_liq = sf(src.get("liquidity_usd"))
    status, reason = "PENDING", "WAIT_SURVIVAL_CONFIRMATION" if cfg["survival_gate"] else "WAIT_CONFIRMATION"
    if low_liq_danger:
        status, reason = "REJECTED", "RUGCHECK_DANGER_LOW_LIQUIDITY"
    elif signal_liq < cfg["min_confirm_liq"]:
        status, reason = "REJECTED", "SIGNAL_LIQUIDITY_TOO_LOW"
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {tn(cfg,'candidates')}(
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
            json.dumps({"paper_only":True,"variant":cfg["variant"],"rug_level":src.get("rug_level"),"rug_risks":risks,"source_meta":parse_json(src.get("meta"))})
        ))
        conn.commit()
    finally:
        cur.close(); conn.close()
    print(f"{cfg['engine']} SOURCE #{src['id']} {src.get('pool_name')} status={status} reason={reason} confirm={required:.0f}s liq=${signal_liq:.0f}", flush=True)
    return True


def ingest_new_sources(cfg):
    state = get_state(cfg)
    if state is None:
        latest = seed_state(cfg)
        print(f"{cfg['engine']} FIRST RUN: watermark seeded at observation #{latest}. Historical signals NOT replayed.", flush=True)
        return 0
    rows = load_new_source_rows(state)
    accepted = 0
    for src in rows:
        try:
            if ingest_source_row(cfg, src):
                accepted += 1
        except Exception as exc:
            print(f"{cfg['engine']} SOURCE ERROR #{src.get('id')}: {type(exc).__name__}: {exc}", flush=True)
        finally:
            advance_state(cfg, src["id"])
    return accepted


def load_pending(cfg):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {tn(cfg,'candidates')} WHERE status='PENDING' ORDER BY id ASC;")
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        cur.close(); conn.close()


def open_count(cfg):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {tn(cfg,'positions')} WHERE status='OPEN';")
        return int(cur.fetchone()[0])
    finally:
        cur.close(); conn.close()


def round_trip_metrics(cfg, raw_price, liquidity_usd):
    notional = cfg["notional"]
    entry_price, entry_bps = base.executable_entry(raw_price, notional, liquidity_usd)
    entry_fee = notional * FEE_BPS_PER_SIDE / 10000.0
    spend_after_fee = max(0.0, notional - entry_fee)
    qty = spend_after_fee / entry_price if entry_price > 0 else 0.0
    exit_price, exit_bps = base.executable_exit(raw_price, qty, liquidity_usd)
    gross_value = qty * exit_price
    exit_fee = gross_value * FEE_BPS_PER_SIDE / 10000.0
    net = gross_value - exit_fee - notional
    ret = net / notional * 100.0 if notional > 0 else -100.0
    return {"entry_price":entry_price,"entry_bps":entry_bps,"qty":qty,"exit_price":exit_price,"exit_bps":exit_bps,"round_trip_pct":ret}


def mark_candidate(cfg, candidate_id, status, reason, sample_count, min_liq, max_liq, current_liq, current_price, exit_bps, round_trip_pct, opened_position_id=None):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"""
            UPDATE {tn(cfg,'candidates')} SET status=%s,decision_reason=%s,last_checked_at=NOW(),sample_count=%s,
                min_liquidity_usd=%s,max_liquidity_usd=%s,current_liquidity_usd=%s,current_price_usd=%s,
                current_exit_execution_bps=%s,immediate_round_trip_pct=%s,
                opened_position_id=COALESCE(%s,opened_position_id)
            WHERE id=%s;
        """, (status,reason,sample_count,min_liq,max_liq,current_liq,current_price,exit_bps,round_trip_pct,opened_position_id,candidate_id))
        conn.commit()
    finally:
        cur.close(); conn.close()


def log_survival_sample(cfg, c, p, age_sec, current_ratio, path_ratio, price_ratio, buy_sell_ratio, vol5_liq_ratio, m):
    if not cfg["survival_gate"]:
        return
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {tn(cfg,'candidate_samples')}(
                candidate_id,age_sec,liquidity_usd,current_liq_ratio,path_liq_ratio,price_usd,price_ratio,
                volume_m5_usd,vol5_liq_ratio,buys_m5,sells_m5,buy_sell_ratio,
                exit_execution_bps,immediate_round_trip_pct
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            c["id"],age_sec,sf(p.get("liq")),current_ratio,path_ratio,sf(p.get("price")),price_ratio,
            sf(p.get("vol_m5")),vol5_liq_ratio,int(sf(p.get("buys_m5"))),int(sf(p.get("sells_m5"))),buy_sell_ratio,
            m["exit_bps"],m["round_trip_pct"]
        ))
        conn.commit()
    finally:
        cur.close(); conn.close()


def create_position(cfg, c, p, m):
    raw = sf(p["price"]); liq = sf(p["liq"])
    entry_fee = cfg["notional"] * FEE_BPS_PER_SIDE / 10000.0
    meta = {
        "paper_only": True,
        "variant": cfg["variant"],
        "confirm_required_sec": c["confirm_required_sec"],
        "confirmation_samples": int(c["sample_count"] or 0) + 1,
        "signal_liquidity_usd": c["signal_liquidity_usd"],
        "lp_locked_pct": c["lp_locked_pct"],
        "unlocked_lp": c["unlocked_lp"],
        "immediate_round_trip_pct": m["round_trip_pct"],
        "entry_exit_execution_bps": m["exit_bps"],
        "execution_model": "constant_product_liquidity_impact_v2",
    }
    for k in ("buy_sell_ratio","vol5_liq_ratio","price_ratio","current_liq_ratio","path_liq_ratio"):
        if k in m:
            meta[k] = m[k]
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {tn(cfg,'positions')}(
                engine_version,variant,candidate_id,source_observation_id,pool_address,mint_address,pool_name,dex_id,
                notional_usdt,token_qty,raw_entry_price,entry_price,entry_fee_usdt,entry_liquidity_usd,entry_execution_bps,
                peak_price,last_price,last_liquidity_usd,meta
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
        """, (
            cfg["engine"],cfg["variant"],c["id"],c["source_observation_id"],c["pool_address"],c["mint_address"],c.get("pool_name"),c.get("dex_id"),
            cfg["notional"],m["qty"],raw,m["entry_price"],entry_fee,liq,m["entry_bps"],raw,raw,liq,json.dumps(meta)
        ))
        pid = int(cur.fetchone()[0]); conn.commit(); return pid
    finally:
        cur.close(); conn.close()


def evaluate_pending(cfg):
    pending = load_pending(cfg)
    if not pending:
        return 0,0,0
    snapshots = fetch_pools_cached([c["pool_address"] for c in pending])
    opened = 0; rejected = 0
    for c in pending:
        p = snapshots.get(c["pool_address"])
        age_sec = max(0.0, (utcnow() - c["created_at"]).total_seconds())
        if not p or sf(p.get("price")) <= 0:
            if age_sec >= cfg["max_pending_sec"]:
                mark_candidate(cfg,c["id"],"EXPIRED","MARKET_DATA_MISSING_TIMEOUT",c["sample_count"],c["min_liquidity_usd"],c["max_liquidity_usd"],c["current_liquidity_usd"],c["current_price_usd"],c["current_exit_execution_bps"],c["immediate_round_trip_pct"])
                rejected += 1
            continue

        current_liq = sf(p["liq"]); current_price = sf(p["price"])
        signal_liq = max(sf(c["signal_liquidity_usd"]), 1e-9)
        min_liq = min(sf(c["min_liquidity_usd"], current_liq), current_liq)
        max_liq = max(sf(c["max_liquidity_usd"], current_liq), current_liq)
        samples = int(c["sample_count"] or 0) + 1
        m = round_trip_metrics(cfg, current_price, current_liq)
        current_ratio = current_liq / signal_liq
        path_ratio = min_liq / signal_liq
        signal_price = max(sf(c.get("signal_price_usd")), 1e-12)
        price_ratio = current_price / signal_price
        buys = int(sf(p.get("buys_m5"))); sells = int(sf(p.get("sells_m5")))
        buy_sell_ratio = (buys + 1.0) / (sells + 1.0)
        vol5_liq_ratio = sf(p.get("vol_m5")) / max(current_liq, 1e-9)
        m.update({
            "buy_sell_ratio":buy_sell_ratio,
            "vol5_liq_ratio":vol5_liq_ratio,
            "price_ratio":price_ratio,
            "current_liq_ratio":current_ratio,
            "path_liq_ratio":path_ratio,
        })
        log_survival_sample(cfg,c,p,age_sec,current_ratio,path_ratio,price_ratio,buy_sell_ratio,vol5_liq_ratio,m)

        status = "PENDING"
        reason = "WAIT_SURVIVAL_CONFIRMATION" if cfg["survival_gate"] else "WAIT_CONFIRMATION"
        if current_liq < cfg["min_confirm_liq"]:
            status, reason = "REJECTED", "CONFIRM_LIQUIDITY_TOO_LOW"
        elif current_ratio < cfg["hard_collapse_ratio"]:
            status, reason = "REJECTED", "LIQUIDITY_COLLAPSED_DURING_CONFIRM"
        elif m["exit_bps"] > cfg["max_exit_bps"]:
            status, reason = "REJECTED", "EXIT_IMPACT_TOO_HIGH"
        elif m["round_trip_pct"] < cfg["min_roundtrip_pct"]:
            status, reason = "REJECTED", "ROUND_TRIP_TOO_EXPENSIVE"
        elif age_sec >= cfg["max_pending_sec"]:
            status, reason = "EXPIRED", "CONFIRMATION_TIMEOUT"
        elif age_sec >= sf(c["confirm_required_sec"]) and samples >= cfg["min_samples"]:
            if current_ratio < cfg["min_current_liq_ratio"]:
                status, reason = "REJECTED", "CURRENT_LIQUIDITY_UNSTABLE"
            elif path_ratio < cfg["min_path_liq_ratio"]:
                status, reason = "REJECTED", "PATH_LIQUIDITY_UNSTABLE"
            elif cfg["survival_gate"] and buy_sell_ratio < cfg["min_buy_sell_ratio"]:
                status, reason = "REJECTED", "BUY_PRESSURE_WEAK"
            elif cfg["survival_gate"] and vol5_liq_ratio < cfg["min_vol5_liq_ratio"]:
                status, reason = "REJECTED", "TURNOVER_WEAK"
            elif cfg["survival_gate"] and price_ratio < cfg["min_price_ratio"]:
                status, reason = "REJECTED", "PRICE_FADED_DURING_CONFIRM"
            elif cfg["survival_gate"] and price_ratio > cfg["max_price_ratio"]:
                status, reason = "REJECTED", "PRICE_CHASE_TOO_LATE"
            elif open_count(cfg) >= cfg["max_open"]:
                status, reason = "PENDING", "MAX_OPEN_WAIT"
            else:
                try:
                    pid = create_position(cfg,c,p,m)
                    pass_reason = "SURVIVAL_PASS" if cfg["survival_gate"] else "EXITABILITY_PASS"
                    mark_candidate(cfg,c["id"],"OPENED",pass_reason,samples,min_liq,max_liq,current_liq,current_price,m["exit_bps"],m["round_trip_pct"],pid)
                    opened += 1
                    print(f"{cfg['engine']} OPEN #{pid} {c.get('pool_name')} age={age_sec:.0f}s liq_ratio={current_ratio:.2f} path={path_ratio:.2f} bs={buy_sell_ratio:.2f} v5/liq={vol5_liq_ratio:.2f}", flush=True)
                    continue
                except Exception as exc:
                    status, reason = "REJECTED", f"OPEN_ERROR:{type(exc).__name__}"
        mark_candidate(cfg,c["id"],status,reason,samples,min_liq,max_liq,current_liq,current_price,m["exit_bps"],m["round_trip_pct"])
        if status in ("REJECTED","EXPIRED"):
            rejected += 1
            print(f"{cfg['engine']} {status} {c.get('pool_name')} reason={reason} age={age_sec:.0f}s liq_ratio={current_ratio:.2f} bs={buy_sell_ratio:.2f} v5/liq={vol5_liq_ratio:.2f}", flush=True)
    return len(pending), opened, rejected


def load_open_positions(cfg):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM {tn(cfg,'positions')} WHERE status='OPEN' ORDER BY id ASC;")
        cols = [d.name for d in cur.description]
        return [dict(zip(cols,row)) for row in cur.fetchall()]
    finally:
        cur.close(); conn.close()


def position_exit_reason(cfg, pos, p):
    entry = sf(pos["raw_entry_price"]); price = sf(p["price"])
    if entry <= 0 or price <= 0:
        return None
    peak = max(sf(pos.get("peak_price"), entry), price)
    peak_roi = (peak / entry - 1.0) * 100.0
    trail_floor = peak * (1.0 - cfg["trail_distance_pct"] / 100.0)
    age_min = (utcnow() - pos["opened_at"]).total_seconds() / 60.0
    entry_liq = sf(pos.get("entry_liquidity_usd"))
    last_liq = sf(pos.get("last_liquidity_usd"), entry_liq)
    current_liq = sf(p.get("liq"))

    if entry_liq > 0 and current_liq < entry_liq * cfg["liq_collapse_ratio"]:
        return "LIQUIDITY_COLLAPSE"
    if (entry_liq > 0 and last_liq > 0 and current_liq < last_liq * cfg["rapid_liq_drop_ratio"] and current_liq < entry_liq * 0.90):
        return "LIQUIDITY_DRAIN"

    exit_price, _ = base.executable_exit(price, pos["token_qty"], current_liq)
    gross_value = sf(pos["token_qty"]) * exit_price
    exit_fee = gross_value * FEE_BPS_PER_SIDE / 10000.0
    notional = sf(pos["notional_usdt"])
    executable_net_roi = (gross_value - exit_fee - notional) / notional * 100.0 if notional > 0 else -100.0
    if executable_net_roi <= -cfg["stop_loss_pct"]:
        return "STOP_LOSS_EXECUTABLE"

    raw_roi = (price / entry - 1.0) * 100.0
    if peak_roi >= cfg["trail_activate_pct"] and price <= trail_floor:
        return "TRAILING_STOP"
    if raw_roi >= cfg["take_profit_pct"]:
        return "TAKE_PROFIT"
    if age_min >= cfg["max_hold_min"]:
        return "MAX_HOLD"
    return None


def log_position_snapshot(cfg, pos, p=None, peak=None, reason_now=None):
    raw_price = sf((p or {}).get("price"),0.0); liq = sf((p or {}).get("liq"),0.0)
    entry = sf(pos.get("raw_entry_price")); qty = sf(pos.get("token_qty")); notional = sf(pos.get("notional_usdt")); market_ok = raw_price > 0
    if peak is None:
        peak = max(sf(pos.get("peak_price"),entry), raw_price if raw_price > 0 else 0.0)
    raw_roi = ((raw_price / entry) - 1.0) * 100.0 if entry > 0 and raw_price > 0 else None
    peak_roi = ((peak / entry) - 1.0) * 100.0 if entry > 0 and peak > 0 else None
    trail_floor = peak * (1.0 - cfg["trail_distance_pct"] / 100.0) if peak > 0 else None
    holding_min = max(0.0,(utcnow()-pos["opened_at"]).total_seconds()/60.0)
    executable_exit_price = None; execution_bps = None; executable_net_roi = None
    if market_ok and qty > 0 and notional > 0:
        executable_exit_price, execution_bps = base.executable_exit(raw_price, qty, liq)
        gross_value = qty * executable_exit_price
        exit_fee = gross_value * FEE_BPS_PER_SIDE / 10000.0
        executable_net_roi = (gross_value - exit_fee - notional) / notional * 100.0
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"""
            INSERT INTO {tn(cfg,'position_snapshots')}(
                engine_version,variant,position_id,pool_address,mint_address,raw_price_usd,executable_exit_price_usd,liquidity_usd,
                exit_execution_bps,raw_roi_pct,executable_net_roi_pct,peak_price_usd,peak_roi_pct,trail_floor_price_usd,holding_minutes,
                exit_reason_now,market_data_ok,meta
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (cfg["engine"],cfg["variant"],pos["id"],pos["pool_address"],pos["mint_address"],raw_price if raw_price>0 else None,executable_exit_price,liq if liq>0 else None,
                execution_bps,raw_roi,executable_net_roi,peak if peak>0 else None,peak_roi,trail_floor,holding_min,reason_now,market_ok,
                json.dumps({"paper_only":True,"variant":cfg["variant"],"execution_model":"constant_product_liquidity_impact_v2"})))
        conn.commit()
    finally:
        cur.close(); conn.close()


def update_position_mark(cfg, pos, p, peak):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"UPDATE {tn(cfg,'positions')} SET last_price=%s,last_liquidity_usd=%s,peak_price=%s WHERE id=%s AND status='OPEN';", (p["price"],p["liq"],peak,pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()


def close_position(cfg, pos, p, reason):
    raw_exit = sf(p["price"]); liq = sf(p["liq"])
    exit_price, execution_bps = base.executable_exit(raw_exit,pos["token_qty"],liq)
    gross_value = sf(pos["token_qty"]) * exit_price
    exit_fee = gross_value * FEE_BPS_PER_SIDE / 10000.0
    net = gross_value - exit_fee - sf(pos["notional_usdt"])
    ret = net / sf(pos["notional_usdt"]) * 100.0
    gross = sf(pos["token_qty"]) * raw_exit - sf(pos["notional_usdt"])
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(f"""
            UPDATE {tn(cfg,'positions')} SET status='CLOSED',closed_at=NOW(),last_price=%s,last_liquidity_usd=%s,
                exit_price=%s,exit_fee_usdt=%s,exit_execution_bps=%s,gross_pnl_usdt=%s,net_pnl_usdt=%s,net_return_pct=%s,exit_reason=%s,
                meta=COALESCE(meta,'{{}}'::jsonb) || %s::jsonb WHERE id=%s AND status='OPEN';
        """, (raw_exit,liq,exit_price,exit_fee,execution_bps,gross,net,ret,reason,
                json.dumps({"exit_execution_bps":execution_bps,"execution_model":"constant_product_liquidity_impact_v2"}),pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return net,ret


def monitor_positions(cfg):
    opens = load_open_positions(cfg)
    if not opens:
        return 0,0
    snapshots = fetch_pools_cached([p["pool_address"] for p in opens])
    closed = 0
    for pos in opens:
        p = snapshots.get(pos["pool_address"])
        if not p or sf(p.get("price")) <= 0:
            log_position_snapshot(cfg,pos,None,peak=sf(pos.get("peak_price"),sf(pos["raw_entry_price"])),reason_now="MARKET_DATA_MISSING")
            continue
        peak = max(sf(pos.get("peak_price"),sf(p["price"])),sf(p["price"]))
        reason = position_exit_reason(cfg,pos,p)
        log_position_snapshot(cfg,pos,p,peak=peak,reason_now=reason)
        if reason:
            net,ret = close_position(cfg,pos,p,reason); closed += 1
            print(f"{cfg['engine']} CLOSE #{pos['id']} {pos.get('pool_name')} {reason} net={net:+.4f}U ret={ret:+.2f}%", flush=True)
        else:
            update_position_mark(cfg,pos,p,peak)
    return len(opens),closed


class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/","/health"):
            body = json.dumps({
                "status":"ok",
                "paper_only":True,
                "poll_sec":POLL_SEC,
                "v2":{"variant":V2["variant"],"open_positions":open_count(V2)},
                "v3":{"variant":V3["variant"],"open_positions":open_count(V3)},
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",str(len(body)))
            self.end_headers(); self.wfile.write(body)
        else:
            self.send_response(404); self.end_headers()

    def log_message(self,fmt,*args):
        return


def start_health_server():
    server = ThreadingHTTPServer(("0.0.0.0",HEALTH_PORT),HealthHandler)
    threading.Thread(target=server.serve_forever,daemon=True).start()
    return server


def run_monitor():
    for cfg in ENGINES:
        init_database(cfg)
    start_health_server()
    print("="*88, flush=True)
    print("MEME1 SHADOW DUAL RUN | V2 BASELINE + V3 SURVIVAL ENTRY | PAPER ONLY", flush=True)
    print(f"V2 confirm={V2['base_confirm_sec']:.0f}/{V2['unknown_confirm_sec']:.0f}/{V2['risky_confirm_sec']:.0f}s liq={V2['min_current_liq_ratio']:.2f}/{V2['min_path_liq_ratio']:.2f}", flush=True)
    print(f"V3 confirm={V3['base_confirm_sec']:.0f}/{V3['unknown_confirm_sec']:.0f}/{V3['risky_confirm_sec']:.0f}s liq={V3['min_current_liq_ratio']:.2f}/{V3['min_path_liq_ratio']:.2f} buy/sell>={V3['min_buy_sell_ratio']:.2f} vol5/liq>={V3['min_vol5_liq_ratio']:.2f}", flush=True)
    print("="*88, flush=True)

    while True:
        started = time.time()
        for cfg in ENGINES:
            try:
                sources = ingest_new_sources(cfg)
                pending,opened,rejected = evaluate_pending(cfg)
                open_n,closed = monitor_positions(cfg)
                if sources or pending or opened or rejected or open_n or closed:
                    print(f"{cfg['engine']} LOOP sources={sources} pending={pending} opened={opened} rejected={rejected} open_positions={open_n} closed={closed}", flush=True)
            except Exception as exc:
                print(f"{cfg['engine']} LOOP ERROR {type(exc).__name__}: {exc}", flush=True)
        time.sleep(max(1.0, POLL_SEC - (time.time()-started)))


def run_once():
    for cfg in ENGINES:
        init_database(cfg)
        ingest_new_sources(cfg)
        evaluate_pending(cfg)
        monitor_positions(cfg)


if __name__ == "__main__":
    import sys
    if "--once" in sys.argv:
        run_once()
    else:
        run_monitor()
