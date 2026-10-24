"""
MEME1 — Solana new-pool / memecoin research core.

PAPER ONLY. Uses public market/risk data and never signs or sends a transaction.

Research idea:
- discover newly created Solana pools from GeckoTerminal;
- score liquidity, turnover, buy pressure, activity, momentum and age;
- optionally gate candidates with RugCheck risk data;
- paper-enter only after conservative costs;
- paper-exit on stop, trailing stop, take-profit, liquidity collapse or timeout.

This core is intentionally independent from R2F/R3/BF1/AS1.
"""

import json
import math
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg2

ENGINE = "MEME1"
PAPER_ONLY = True
DATABASE_URL = os.environ.get("DATABASE_URL", "")

GECKO_BASE = "https://api.geckoterminal.com/api/v2"
GECKO_VERSION = "20230203"
RUGCHECK_BASE = "https://api.rugcheck.xyz/v1"

NOTIONAL_USDT = float(os.environ.get("MEME1_NOTIONAL_USDT", "20"))
MAX_OPEN = int(os.environ.get("MEME1_MAX_OPEN", "10"))
MAX_CANDIDATES_PER_RUN = int(os.environ.get("MEME1_MAX_CANDIDATES_PER_RUN", "8"))

MIN_LIQUIDITY_USD = float(os.environ.get("MEME1_MIN_LIQUIDITY_USD", "8000"))
MAX_FDV_LIQ_RATIO = float(os.environ.get("MEME1_MAX_FDV_LIQ_RATIO", "80"))
MAX_POOL_AGE_MIN = float(os.environ.get("MEME1_MAX_POOL_AGE_MIN", "180"))
MIN_SCORE = float(os.environ.get("MEME1_MIN_SCORE", "70"))
MAX_RUG_SCORE = float(os.environ.get("MEME1_MAX_RUG_SCORE", "60"))
REQUIRE_RUGCHECK = os.environ.get("MEME1_REQUIRE_RUGCHECK", "true").lower() == "true"

ENTRY_SLIPPAGE_BPS = float(os.environ.get("MEME1_ENTRY_SLIPPAGE_BPS", "150"))
EXIT_SLIPPAGE_BPS = float(os.environ.get("MEME1_EXIT_SLIPPAGE_BPS", "200"))
FEE_BPS_PER_SIDE = float(os.environ.get("MEME1_FEE_BPS_PER_SIDE", "100"))

STOP_LOSS_PCT = float(os.environ.get("MEME1_STOP_LOSS_PCT", "25"))
TAKE_PROFIT_PCT = float(os.environ.get("MEME1_TAKE_PROFIT_PCT", "100"))
TRAIL_ACTIVATE_PCT = float(os.environ.get("MEME1_TRAIL_ACTIVATE_PCT", "25"))
TRAIL_DISTANCE_PCT = float(os.environ.get("MEME1_TRAIL_DISTANCE_PCT", "12"))
MAX_HOLD_MIN = float(os.environ.get("MEME1_MAX_HOLD_MIN", "240"))
LIQUIDITY_COLLAPSE_RATIO = float(os.environ.get("MEME1_LIQUIDITY_COLLAPSE_RATIO", "0.40"))

# High-frequency open-position monitor. New-pool discovery stays on the 5-minute cron.
MONITOR_INTERVAL_SEC = float(os.environ.get("MEME1_MONITOR_INTERVAL_SEC", "15"))
HEALTH_PORT = int(os.environ.get("PORT", "10000"))

# Execution realism: pool reserve_in_usd is total pool liquidity. For a balanced AMM,
# roughly half is on the quote side. We use that reserve to estimate market impact.
LIQUIDITY_SIDE_FRACTION = float(os.environ.get("MEME1_LIQUIDITY_SIDE_FRACTION", "0.50"))
HTTP_RETRIES = int(os.environ.get("MEME1_HTTP_RETRIES", "4"))
HTTP_RETRY_BASE_SEC = float(os.environ.get("MEME1_HTTP_RETRY_BASE_SEC", "2.0"))

USER_AGENT = "MEME1-paper-research/2.0"


def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def _safe_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _json_get(url, headers=None, timeout=12):
    req_headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    last_exc = None
    for attempt in range(max(1, HTTP_RETRIES)):
        req = urllib.request.Request(url, headers=req_headers, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            last_exc = exc
            retryable = exc.code == 429 or 500 <= exc.code < 600
            if not retryable or attempt >= HTTP_RETRIES - 1:
                raise
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            try:
                delay = float(retry_after) if retry_after else HTTP_RETRY_BASE_SEC * (2 ** attempt)
            except Exception:
                delay = HTTP_RETRY_BASE_SEC * (2 ** attempt)
            print(f"MEME1 HTTP {exc.code}; retry in {delay:.1f}s", flush=True)
            time.sleep(max(0.5, delay))
        except Exception as exc:
            last_exc = exc
            if attempt >= HTTP_RETRIES - 1:
                raise
            delay = HTTP_RETRY_BASE_SEC * (2 ** attempt)
            time.sleep(max(0.5, delay))
    raise RuntimeError(f"HTTP request failed: {last_exc}")


def gecko_get(path):
    return _json_get(
        GECKO_BASE + path,
        headers={"Accept": f"application/json;version={GECKO_VERSION}"},
    )


def _quote_side_reserve_usd(liquidity_usd):
    liq = max(0.0, _safe_float(liquidity_usd))
    return max(1e-9, liq * max(0.05, min(0.95, LIQUIDITY_SIDE_FRACTION)))


def executable_entry(raw_price, notional_usdt, liquidity_usd):
    """Conservative AMM-style market-buy estimate.

    For a constant-product pool, average execution price rises approximately by
    (1 + quote_in / quote_reserve). We then apply the configured extra slippage.
    """
    raw = max(0.0, _safe_float(raw_price))
    if raw <= 0:
        return 0.0, 0.0
    reserve = _quote_side_reserve_usd(liquidity_usd)
    impact_ratio = max(0.0, _safe_float(notional_usdt)) / reserve
    fixed = max(0.0, ENTRY_SLIPPAGE_BPS) / 10000.0
    price = raw * (1.0 + impact_ratio) * (1.0 + fixed)
    total_bps = (price / raw - 1.0) * 10000.0
    return price, total_bps


def executable_exit(raw_price, token_qty, liquidity_usd):
    """Conservative AMM-style market-sell estimate.

    The raw marked value is discounted by constant-product price impact and by
    the configured extra exit slippage. If liquidity vanishes, execution value
    naturally approaches zero rather than assuming the screen price is fillable.
    """
    raw = max(0.0, _safe_float(raw_price))
    qty = max(0.0, _safe_float(token_qty))
    if raw <= 0 or qty <= 0:
        return 0.0, 10000.0
    reserve = _quote_side_reserve_usd(liquidity_usd)
    raw_value = qty * raw
    impact_ratio = raw_value / reserve
    fixed = max(0.0, min(0.95, EXIT_SLIPPAGE_BPS / 10000.0))
    price = raw * (1.0 - fixed) / (1.0 + impact_ratio)
    total_bps = max(0.0, (1.0 - price / raw) * 10000.0)
    return max(0.0, price), total_bps


def init_database():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meme1_observations (
                id BIGSERIAL PRIMARY KEY,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                engine_version VARCHAR(20) NOT NULL,
                pool_address VARCHAR(80) NOT NULL,
                mint_address VARCHAR(80) NOT NULL,
                pool_name TEXT,
                dex_id VARCHAR(80),
                pool_created_at TIMESTAMPTZ,
                pool_age_min DOUBLE PRECISION,
                price_usd DOUBLE PRECISION,
                liquidity_usd DOUBLE PRECISION,
                volume_m5_usd DOUBLE PRECISION,
                volume_h1_usd DOUBLE PRECISION,
                buys_m5 INTEGER,
                sells_m5 INTEGER,
                buyers_m5 INTEGER,
                sellers_m5 INTEGER,
                price_change_m5_pct DOUBLE PRECISION,
                price_change_h1_pct DOUBLE PRECISION,
                fdv_usd DOUBLE PRECISION,
                market_cap_usd DOUBLE PRECISION,
                fdv_liq_ratio DOUBLE PRECISION,
                market_score DOUBLE PRECISION,
                rug_score DOUBLE PRECISION,
                rug_level VARCHAR(40),
                decision VARCHAR(40) NOT NULL,
                reason TEXT,
                meta JSONB
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme1_obs_time ON meme1_observations(observed_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme1_obs_mint ON meme1_observations(mint_address, observed_at DESC);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meme1_positions (
                id BIGSERIAL PRIMARY KEY,
                engine_version VARCHAR(20) NOT NULL,
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
                entry_score DOUBLE PRECISION,
                entry_rug_score DOUBLE PRECISION,
                peak_price DOUBLE PRECISION NOT NULL,
                last_price DOUBLE PRECISION,
                last_liquidity_usd DOUBLE PRECISION,
                exit_price DOUBLE PRECISION,
                exit_fee_usdt DOUBLE PRECISION,
                gross_pnl_usdt DOUBLE PRECISION,
                net_pnl_usdt DOUBLE PRECISION,
                net_return_pct DOUBLE PRECISION,
                exit_reason VARCHAR(80),
                meta JSONB
            );
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_meme1_one_open_pool ON meme1_positions(pool_address) WHERE status='OPEN';")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme1_pos_time ON meme1_positions(opened_at DESC);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meme1_position_snapshots (
                id BIGSERIAL PRIMARY KEY,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                engine_version VARCHAR(20) NOT NULL,
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme1_snap_pos_time ON meme1_position_snapshots(position_id, observed_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme1_snap_mint_time ON meme1_position_snapshots(mint_address, observed_at DESC);")
        conn.commit()
    finally:
        cur.close(); conn.close()


def _parse_iso(ts):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
    except Exception:
        return None


def _mint_from_relationship(rel):
    rid = (((rel or {}).get("data") or {}).get("id") or "")
    if rid.startswith("solana_"):
        return rid[len("solana_"):]
    return rid


def parse_pool(item):
    a = item.get("attributes") or {}
    rel = item.get("relationships") or {}
    tx = a.get("transactions") or {}
    tx5 = tx.get("m5") or {}
    vol = a.get("volume_usd") or {}
    pc = a.get("price_change_percentage") or {}
    created = _parse_iso(a.get("pool_created_at"))
    now = datetime.now(timezone.utc)
    age_min = (now - created).total_seconds() / 60.0 if created else 999999.0
    liq = _safe_float(a.get("reserve_in_usd"))
    fdv = _safe_float(a.get("fdv_usd"))
    mint = _mint_from_relationship(rel.get("base_token"))
    return {
        "pool_address": a.get("address") or item.get("id", "").split("_")[-1],
        "mint_address": mint,
        "pool_name": a.get("name") or "",
        "dex_id": (((rel.get("dex") or {}).get("data") or {}).get("id") or ""),
        "created_at": created,
        "age_min": age_min,
        "price": _safe_float(a.get("base_token_price_usd")),
        "liq": liq,
        "vol_m5": _safe_float(vol.get("m5")),
        "vol_h1": _safe_float(vol.get("h1")),
        "buys_m5": int(_safe_float(tx5.get("buys"))),
        "sells_m5": int(_safe_float(tx5.get("sells"))),
        "buyers_m5": int(_safe_float(tx5.get("buyers"))),
        "sellers_m5": int(_safe_float(tx5.get("sellers"))),
        "pc_m5": _safe_float(pc.get("m5")),
        "pc_h1": _safe_float(pc.get("h1")),
        "fdv": fdv,
        "mcap": _safe_float(a.get("market_cap_usd")),
        "fdv_liq": fdv / liq if liq > 0 and fdv > 0 else 0.0,
        "raw": a,
    }


def score_pool(p):
    reasons = []
    if not p["mint_address"]:
        reasons.append("NO_MINT")
    if p["price"] <= 0:
        reasons.append("NO_PRICE")
    if p["liq"] < MIN_LIQUIDITY_USD:
        reasons.append("LOW_LIQUIDITY")
    if p["age_min"] < 0 or p["age_min"] > MAX_POOL_AGE_MIN:
        reasons.append("AGE_OUTSIDE_WINDOW")
    if p["fdv_liq"] > MAX_FDV_LIQ_RATIO:
        reasons.append("FDV_LIQ_TOO_HIGH")

    # 0..20 liquidity. Full points around 5x minimum.
    liq_score = min(20.0, max(0.0, 20.0 * math.log10(max(p["liq"], 1.0) / MIN_LIQUIDITY_USD + 1.0) / math.log10(6.0)))

    # 0..20 turnover. New tokens need actual flow, not only a funded pool.
    turnover = p["vol_h1"] / p["liq"] if p["liq"] > 0 else 0.0
    turnover_score = min(20.0, turnover * 12.0)

    buys, sells = p["buys_m5"], p["sells_m5"]
    total = buys + sells
    buy_ratio = buys / total if total else 0.0
    pressure_score = max(0.0, min(20.0, (buy_ratio - 0.40) / 0.35 * 20.0))

    activity_score = min(15.0, total / 40.0 * 15.0)

    # Reward positive acceleration but cap chase-heavy pumps.
    m5 = p["pc_m5"]
    if m5 <= -10:
        momentum_score = 0.0
    elif m5 <= 5:
        momentum_score = (m5 + 10.0) / 15.0 * 8.0
    elif m5 <= 35:
        momentum_score = 8.0 + (m5 - 5.0) / 30.0 * 7.0
    elif m5 <= 80:
        momentum_score = 15.0 - (m5 - 35.0) / 45.0 * 6.0
    else:
        momentum_score = 3.0

    age_score = max(0.0, 10.0 * (1.0 - min(p["age_min"], MAX_POOL_AGE_MIN) / MAX_POOL_AGE_MIN))

    score = liq_score + turnover_score + pressure_score + activity_score + momentum_score + age_score
    return round(max(0.0, min(100.0, score)), 2), reasons, {
        "liq_score": round(liq_score, 2),
        "turnover_score": round(turnover_score, 2),
        "pressure_score": round(pressure_score, 2),
        "activity_score": round(activity_score, 2),
        "momentum_score": round(momentum_score, 2),
        "age_score": round(age_score, 2),
        "turnover_h1": turnover,
        "buy_ratio_m5": buy_ratio,
    }


def rugcheck_summary(mint):
    try:
        data = _json_get(f"{RUGCHECK_BASE}/tokens/{urllib.parse.quote(mint)}/report/summary", timeout=10)
        score = data.get("score_normalised")
        if score is None:
            score = data.get("scoreNormalized")
        if score is None:
            score = data.get("score")
        score = None if score is None else _safe_float(score)
        risks = data.get("risks") or []
        level = "UNKNOWN"
        if score is not None:
            level = "LOW" if score <= 30 else ("MEDIUM" if score <= 60 else "HIGH")
        return score, level, {
            "risks": risks[:12],
            "lp_locked_pct": data.get("lpLockedPct"),
            "rug_raw_keys": sorted(list(data.keys()))[:30],
        }
    except Exception as exc:
        return None, "UNAVAILABLE", {"rug_error": str(exc)[:300]}


def fetch_new_pools():
    payload = gecko_get("/networks/solana/new_pools?page=1")
    return [parse_pool(x) for x in (payload.get("data") or [])]


def fetch_pools_by_addresses(addresses):
    out = {}
    for i in range(0, len(addresses), 20):
        chunk = addresses[i:i+20]
        if not chunk:
            continue
        path = "/networks/solana/pools/multi/" + ",".join(urllib.parse.quote(x) for x in chunk)
        payload = gecko_get(path)
        for item in payload.get("data") or []:
            p = parse_pool(item)
            out[p["pool_address"]] = p
        if i + 20 < len(addresses):
            time.sleep(1.0)
    return out


def get_open_positions():
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT id,pool_address,mint_address,pool_name,dex_id,opened_at,notional_usdt,
                   token_qty,raw_entry_price,entry_price,entry_fee_usdt,entry_liquidity_usd,
                   entry_score,entry_rug_score,peak_price,last_price,last_liquidity_usd
            FROM meme1_positions WHERE status='OPEN' ORDER BY id;
        """)
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        cur.close(); conn.close()


def log_observation(p, score, rug_score, rug_level, decision, reason, meta):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO meme1_observations (
                engine_version,pool_address,mint_address,pool_name,dex_id,pool_created_at,pool_age_min,
                price_usd,liquidity_usd,volume_m5_usd,volume_h1_usd,buys_m5,sells_m5,buyers_m5,sellers_m5,
                price_change_m5_pct,price_change_h1_pct,fdv_usd,market_cap_usd,fdv_liq_ratio,
                market_score,rug_score,rug_level,decision,reason,meta
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            ENGINE,p["pool_address"],p["mint_address"],p["pool_name"],p["dex_id"],p["created_at"],p["age_min"],
            p["price"],p["liq"],p["vol_m5"],p["vol_h1"],p["buys_m5"],p["sells_m5"],p["buyers_m5"],p["sellers_m5"],
            p["pc_m5"],p["pc_h1"],p["fdv"],p["mcap"],p["fdv_liq"],score,rug_score,rug_level,decision,reason,json.dumps(meta)
        ))
        conn.commit()
    finally:
        cur.close(); conn.close()


def open_position(p, score, rug_score, meta):
    raw = p["price"]
    entry_price, execution_bps = executable_entry(raw, NOTIONAL_USDT, p["liq"])
    entry_fee = NOTIONAL_USDT * FEE_BPS_PER_SIDE / 10000.0
    spend_after_fee = max(0.0, NOTIONAL_USDT - entry_fee)
    qty = spend_after_fee / entry_price if entry_price > 0 else 0.0
    meta = dict(meta or {})
    meta["entry_execution_bps"] = execution_bps
    meta["execution_model"] = "constant_product_liquidity_impact_v2"
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO meme1_positions (
                engine_version,pool_address,mint_address,pool_name,dex_id,notional_usdt,token_qty,
                raw_entry_price,entry_price,entry_fee_usdt,entry_liquidity_usd,entry_score,entry_rug_score,
                peak_price,last_price,last_liquidity_usd,meta
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
        """, (
            ENGINE,p["pool_address"],p["mint_address"],p["pool_name"],p["dex_id"],NOTIONAL_USDT,qty,
            raw,entry_price,entry_fee,p["liq"],score,rug_score,raw,raw,p["liq"],json.dumps(meta)
        ))
        pid = int(cur.fetchone()[0]); conn.commit(); return pid
    finally:
        cur.close(); conn.close()


def update_position_mark(pos, price, liq, peak):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("UPDATE meme1_positions SET last_price=%s,last_liquidity_usd=%s,peak_price=%s WHERE id=%s AND status='OPEN';",
                    (price, liq, peak, pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()


def log_position_snapshot(pos, p=None, peak=None, reason_now=None):
    raw_price = _safe_float((p or {}).get("price"), 0.0)
    liq = _safe_float((p or {}).get("liq"), 0.0)
    entry = _safe_float(pos.get("raw_entry_price"), 0.0)
    qty = _safe_float(pos.get("token_qty"), 0.0)
    notional = _safe_float(pos.get("notional_usdt"), 0.0)
    market_ok = raw_price > 0

    if peak is None:
        peak = max(_safe_float(pos.get("peak_price"), entry), raw_price if raw_price > 0 else 0.0)
    raw_roi = ((raw_price / entry) - 1.0) * 100.0 if entry > 0 and raw_price > 0 else None
    peak_roi = ((peak / entry) - 1.0) * 100.0 if entry > 0 and peak > 0 else None
    trail_floor = peak * (1.0 - TRAIL_DISTANCE_PCT / 100.0) if peak > 0 else None
    holding_min = max(0.0, (datetime.now(timezone.utc) - pos["opened_at"]).total_seconds() / 60.0)

    executable_exit_price = None
    execution_bps = None
    executable_net_roi = None
    if market_ok and qty > 0 and notional > 0:
        executable_exit_price, execution_bps = executable_exit(raw_price, qty, liq)
        gross_value = qty * executable_exit_price
        exit_fee = gross_value * FEE_BPS_PER_SIDE / 10000.0
        executable_net = gross_value - exit_fee - notional
        executable_net_roi = executable_net / notional * 100.0

    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO meme1_position_snapshots(
                engine_version,position_id,pool_address,mint_address,raw_price_usd,
                executable_exit_price_usd,liquidity_usd,exit_execution_bps,raw_roi_pct,
                executable_net_roi_pct,peak_price_usd,peak_roi_pct,trail_floor_price_usd,
                holding_minutes,exit_reason_now,market_data_ok,meta
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (
            ENGINE,pos["id"],pos["pool_address"],pos["mint_address"],
            raw_price if raw_price > 0 else None,
            executable_exit_price,liq if liq > 0 else None,execution_bps,raw_roi,
            executable_net_roi,peak if peak > 0 else None,peak_roi,trail_floor,
            holding_min,reason_now,market_ok,
            json.dumps({
                "paper_only": True,
                "monitor_interval_sec": MONITOR_INTERVAL_SEC,
                "stop_loss_pct": STOP_LOSS_PCT,
                "take_profit_pct": TAKE_PROFIT_PCT,
                "trail_activate_pct": TRAIL_ACTIVATE_PCT,
                "trail_distance_pct": TRAIL_DISTANCE_PCT,
                "liquidity_collapse_ratio": LIQUIDITY_COLLAPSE_RATIO,
                "execution_model": "constant_product_liquidity_impact_v2",
            })
        ))
        conn.commit()
    finally:
        cur.close(); conn.close()


def close_position(pos, p, reason):
    raw_exit = p["price"]
    exit_price, execution_bps = executable_exit(raw_exit, pos["token_qty"], p["liq"])
    gross_value = float(pos["token_qty"]) * exit_price
    exit_fee = gross_value * FEE_BPS_PER_SIDE / 10000.0
    proceeds = gross_value - exit_fee
    net = proceeds - float(pos["notional_usdt"])
    ret = net / float(pos["notional_usdt"]) * 100.0
    gross = float(pos["token_qty"]) * raw_exit - float(pos["notional_usdt"])
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE meme1_positions SET status='CLOSED',closed_at=NOW(),last_price=%s,last_liquidity_usd=%s,
                exit_price=%s,exit_fee_usdt=%s,gross_pnl_usdt=%s,net_pnl_usdt=%s,net_return_pct=%s,exit_reason=%s,
                meta=COALESCE(meta,'{}'::jsonb) || %s::jsonb
            WHERE id=%s AND status='OPEN';
        """, (raw_exit,p["liq"],exit_price,exit_fee,gross,net,ret,reason,
              json.dumps({"exit_slippage_bps":EXIT_SLIPPAGE_BPS,"fee_bps_per_side":FEE_BPS_PER_SIDE,
                          "exit_execution_bps":execution_bps,
                          "execution_model":"constant_product_liquidity_impact_v2"}),pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return net, ret


def exit_reason(pos, p):
    entry = float(pos["raw_entry_price"])
    price = p["price"]
    if entry <= 0 or price <= 0:
        return None
    roi = (price / entry - 1.0) * 100.0
    peak = max(float(pos.get("peak_price") or entry), price)
    peak_roi = (peak / entry - 1.0) * 100.0
    trail_floor = peak * (1.0 - TRAIL_DISTANCE_PCT / 100.0)
    age_min = (datetime.now(timezone.utc) - pos["opened_at"]).total_seconds() / 60.0
    entry_liq = float(pos.get("entry_liquidity_usd") or 0.0)

    if roi <= -STOP_LOSS_PCT:
        return "STOP_LOSS"
    if entry_liq > 0 and p["liq"] < entry_liq * LIQUIDITY_COLLAPSE_RATIO:
        return "LIQUIDITY_COLLAPSE"
    if peak_roi >= TRAIL_ACTIVATE_PCT and price <= trail_floor:
        return "TRAILING_STOP"
    if roi >= TAKE_PROFIT_PCT:
        return "TAKE_PROFIT"
    if age_min >= MAX_HOLD_MIN:
        return "MAX_HOLD"
    return None


def monitor_open_positions_once():
    opens = get_open_positions()
    if not opens:
        return 0, 0
    snapshots = fetch_pools_by_addresses([p["pool_address"] for p in opens])
    closed = 0
    for pos in opens:
        p = snapshots.get(pos["pool_address"])
        if not p or p["price"] <= 0:
            log_position_snapshot(pos, None, peak=float(pos.get("peak_price") or pos["raw_entry_price"]),
                                  reason_now="MARKET_DATA_MISSING")
            continue
        peak = max(float(pos.get("peak_price") or p["price"]), p["price"])
        reason = exit_reason(pos, p)
        # Persist the full 15-second path BEFORE any close/update, so every trade can be replayed.
        log_position_snapshot(pos, p, peak=peak, reason_now=reason)
        if reason:
            net, ret = close_position(pos, p, reason)
            closed += 1
            print(f"MEME1 CLOSE #{pos['id']} {pos['pool_name']} {reason} net={net:+.4f}U ret={ret:+.2f}%", flush=True)
        else:
            update_position_mark(pos, p["price"], p["liq"], peak)
    return len(opens), closed


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            body = json.dumps({"status":"ok","engine":ENGINE,"mode":"position_monitor",
                               "paper_only":True,"interval_sec":MONITOR_INTERVAL_SEC}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers(); self.wfile.write(body)
        else:
            self.send_response(404); self.end_headers()

    def log_message(self, fmt, *args):
        return


def _start_health_server():
    server = ThreadingHTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def monitor_loop():
    init_database()
    _start_health_server()
    print(f"MEME1 monitor started interval={MONITOR_INTERVAL_SEC:.1f}s port={HEALTH_PORT}", flush=True)
    while True:
        started = time.time()
        try:
            open_count, closed = monitor_open_positions_once()
            if open_count or closed:
                print(f"MEME1 monitor open={open_count} closed={closed}", flush=True)
        except Exception as exc:
            # Never kill the continuous risk monitor for one transient market-data failure.
            print(f"MEME1 monitor error: {type(exc).__name__}: {exc}", flush=True)
        elapsed = time.time() - started
        time.sleep(max(1.0, MONITOR_INTERVAL_SEC - elapsed))


def run_once():
    init_database()

    # 1) Safety backup: cron also marks/closes positions once, while the dedicated
    # monitor service handles them every ~15 seconds.
    try:
        monitor_open_positions_once()
    except Exception as exc:
        print(f"MEME1 cron monitor warning: {type(exc).__name__}: {exc}", flush=True)

    # 2) Discover and score new pools.
    opens = get_open_positions()
    open_pools = {p["pool_address"] for p in opens}
    room = max(0, MAX_OPEN - len(opens))
    pools = fetch_new_pools()

    scored = []
    for p in pools:
        score, hard_reasons, detail = score_pool(p)
        scored.append((score, p, hard_reasons, detail))
    scored.sort(key=lambda x: x[0], reverse=True)

    risk_checks = 0
    for score, p, hard_reasons, detail in scored:
        if not p["pool_address"] or not p["mint_address"]:
            continue

        rug_score = None; rug_level = "NOT_CHECKED"; rug_meta = {}
        market_candidate = not hard_reasons and score >= MIN_SCORE
        if market_candidate and risk_checks < MAX_CANDIDATES_PER_RUN:
            rug_score, rug_level, rug_meta = rugcheck_summary(p["mint_address"])
            risk_checks += 1

        decision = "REJECT"
        reason = ",".join(hard_reasons) if hard_reasons else ""
        if not hard_reasons and score < MIN_SCORE:
            reason = f"SCORE_BELOW_MIN {score:.2f}<{MIN_SCORE:.2f}"
        elif market_candidate:
            if REQUIRE_RUGCHECK and rug_score is None:
                decision = "WATCH"
                reason = "RUGCHECK_UNAVAILABLE"
            elif rug_score is not None and rug_score > MAX_RUG_SCORE:
                decision = "REJECT"
                reason = f"RUG_SCORE_HIGH {rug_score:.1f}>{MAX_RUG_SCORE:.1f}"
            elif p["pool_address"] in open_pools:
                decision = "HOLD_EXISTING"
                reason = "ALREADY_OPEN"
            elif room <= 0:
                decision = "WATCH"
                reason = "MAX_OPEN"
            else:
                decision = "OPEN"
                reason = "MARKET_AND_RISK_PASS"

        meta = {
            "paper_only": True,
            "score_detail": detail,
            "risk": rug_meta,
            "cost_model": {
                "entry_slippage_bps": ENTRY_SLIPPAGE_BPS,
                "exit_slippage_bps": EXIT_SLIPPAGE_BPS,
                "fee_bps_per_side": FEE_BPS_PER_SIDE,
                "liquidity_side_fraction": LIQUIDITY_SIDE_FRACTION,
                "execution_model": "constant_product_liquidity_impact_v2",
            },
        }
        log_observation(p, score, rug_score, rug_level, decision, reason, meta)

        if decision == "OPEN":
            pid = open_position(p, score, rug_score, meta)
            open_pools.add(p["pool_address"]); room -= 1
            print(f"MEME1 OPEN #{pid} {p['pool_name']} score={score:.1f} rug={rug_score} liq=${p['liq']:.0f}", flush=True)

    print(f"MEME1 done pools={len(pools)} open={len(get_open_positions())} rug_checks={risk_checks}", flush=True)


if __name__ == "__main__":
    mode = os.environ.get("MEME1_MODE", "cron").strip().lower()
    if "--monitor" in sys.argv or mode == "monitor":
        monitor_loop()
    else:
        run_once()
