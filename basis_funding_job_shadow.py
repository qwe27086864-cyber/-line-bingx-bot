"""
Basis + Funding Arbitrage research core for BingX.

PAPER ONLY. Uses public market-data endpoints only. Never sends private orders.

Core idea:
    BUY SPOT + SHORT PERPETUAL when the executable perp premium plus expected
    funding compensation exceeds realistic round-trip fees/slippage and a
    required net edge. Close when the executable basis converges, risk rules
    fire, or the holding timeout is reached.

This is intentionally conservative:
- spot/perp entries and exits are valued at executable top-of-book prices;
- default cost model assumes taker fees on all four legs;
- same base-asset quantity is used on both legs (delta neutral at entry);
- reverse cash-and-carry (short spot + long perp) is disabled because ordinary
  spot shorting is not assumed to be available.
"""

import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

import psycopg2

ENGINE = "BF1"
PAPER_ONLY = True

BASE_URLS = (
    "https://open-api.bingx.com",
    "https://open-api.bingx.pro",
)
DATABASE_URL = os.environ.get("DATABASE_URL", "")

SYMBOLS = [
    x.strip().upper()
    for x in os.environ.get("BF_SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT").split(",")
    if x.strip()
]

NOTIONAL_USDT = float(os.environ.get("BF_NOTIONAL_USDT", "100"))
MAX_OPEN = int(os.environ.get("BF_MAX_OPEN", "3"))
API_DELAY_SEC = float(os.environ.get("BF_API_DELAY_SEC", "1.05"))

SPOT_TAKER_FEE = float(os.environ.get("BF_SPOT_TAKER_FEE", "0.0010"))
PERP_TAKER_FEE = float(os.environ.get("BF_PERP_TAKER_FEE", "0.0005"))
EXTRA_SLIPPAGE_PER_LEG = float(os.environ.get("BF_EXTRA_SLIPPAGE_PER_LEG", "0.0001"))

CLOSE_BASIS_PCT = float(os.environ.get("BF_CLOSE_BASIS_PCT", "0.05"))
MIN_NET_EDGE_PCT = float(os.environ.get("BF_MIN_NET_EDGE_PCT", "0.10"))
MIN_FUNDING_RATE = float(os.environ.get("BF_MIN_FUNDING_RATE", "0.0"))
FUNDING_EXIT_RATE = float(os.environ.get("BF_FUNDING_EXIT_RATE", "-0.0002"))
MAX_HOLD_HOURS = float(os.environ.get("BF_MAX_HOLD_HOURS", "48"))

# Shadow policies run beside BF1 without affecting the primary paper portfolio.
SHADOW_MIN_EDGE_PCTS = sorted({
    float(x.strip())
    for x in os.environ.get("BF_SHADOW_MIN_EDGE_PCTS", "0.00,0.05").split(",")
    if x.strip()
})


def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def _json_get(path, params):
    query = urllib.parse.urlencode(params)
    last_exc = None
    for base in BASE_URLS:
        url = f"{base}{path}" + (f"?{query}" if query else "")
        req = urllib.request.Request(
            url,
            method="GET",
            headers={"X-SOURCE-KEY": "BX-AI-SKILL", "User-Agent": "BF1-paper-research"},
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if int(payload.get("code", -1)) != 0:
                raise RuntimeError(f"BingX error {payload.get('code')}: {payload.get('msg')}")
            return payload.get("data")
        except Exception as exc:
            last_exc = exc
    raise RuntimeError(f"BingX request failed for {path}: {last_exc}")


def _first(data):
    if isinstance(data, list):
        return data[0] if data else {}
    return data or {}


def market_snapshot(symbol):
    spot_depth = _json_get("/openApi/spot/v1/market/depth", {"symbol": symbol, "limit": 5})
    perp_depth = _json_get("/openApi/swap/v2/quote/depth", {"symbol": symbol, "limit": 5})
    funding = _first(_json_get("/openApi/swap/v2/quote/fundingRate", {"symbol": symbol, "limit": 1}))

    spot_bids = (spot_depth or {}).get("bids") or []
    spot_asks = (spot_depth or {}).get("asks") or []
    perp_bids = (perp_depth or {}).get("bids") or []
    perp_asks = (perp_depth or {}).get("asks") or []

    if not spot_bids or not spot_asks:
        raise ValueError(f"empty spot depth for {symbol}: {spot_depth}")
    if not perp_bids or not perp_asks:
        raise ValueError(f"empty perp depth for {symbol}: {perp_depth}")

    spot_bid = float(spot_bids[0][0])
    spot_ask = float(spot_asks[0][0])
    perp_bid = float(perp_bids[0][0])
    perp_ask = float(perp_asks[0][0])
    funding_rate = float(funding.get("fundingRate") or 0.0)
    next_funding_time = int(funding.get("nextFundingTime") or 0)

    if min(spot_bid, spot_ask, perp_bid, perp_ask) <= 0:
        raise ValueError(f"invalid book for {symbol}")

    open_basis_pct = (perp_bid / spot_ask - 1.0) * 100.0
    close_basis_pct = (perp_ask / spot_bid - 1.0) * 100.0
    spot_spread_pct = (spot_ask / spot_bid - 1.0) * 100.0
    perp_spread_pct = (perp_ask / perp_bid - 1.0) * 100.0

    round_trip_fee_pct = 2.0 * (SPOT_TAKER_FEE + PERP_TAKER_FEE) * 100.0
    round_trip_slippage_pct = 4.0 * EXTRA_SLIPPAGE_PER_LEG * 100.0
    expected_funding_pct = funding_rate * 100.0
    expected_net_edge_pct = (
        open_basis_pct - CLOSE_BASIS_PCT + expected_funding_pct
        - round_trip_fee_pct - round_trip_slippage_pct
    )

    return {
        "symbol": symbol, "spot_bid": spot_bid, "spot_ask": spot_ask,
        "perp_bid": perp_bid, "perp_ask": perp_ask, "funding_rate": funding_rate,
        "next_funding_time_ms": next_funding_time, "open_basis_pct": open_basis_pct,
        "close_basis_pct": close_basis_pct, "spot_spread_pct": spot_spread_pct,
        "perp_spread_pct": perp_spread_pct, "round_trip_fee_pct": round_trip_fee_pct,
        "round_trip_slippage_pct": round_trip_slippage_pct,
        "expected_funding_pct": expected_funding_pct,
        "expected_net_edge_pct": expected_net_edge_pct,
    }


def init_database():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bf_observations (
                id BIGSERIAL PRIMARY KEY, observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                engine_version VARCHAR(20) NOT NULL, symbol VARCHAR(50) NOT NULL,
                spot_bid DOUBLE PRECISION NOT NULL, spot_ask DOUBLE PRECISION NOT NULL,
                perp_bid DOUBLE PRECISION NOT NULL, perp_ask DOUBLE PRECISION NOT NULL,
                open_basis_pct DOUBLE PRECISION NOT NULL, close_basis_pct DOUBLE PRECISION NOT NULL,
                funding_rate DOUBLE PRECISION NOT NULL, next_funding_time_ms BIGINT,
                expected_net_edge_pct DOUBLE PRECISION NOT NULL, decision VARCHAR(40) NOT NULL,
                reason TEXT, meta JSONB
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bf_obs_symbol_time ON bf_observations(symbol, observed_at DESC);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bf_positions (
                id BIGSERIAL PRIMARY KEY, engine_version VARCHAR(20) NOT NULL,
                symbol VARCHAR(50) NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
                opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), closed_at TIMESTAMPTZ,
                base_qty DOUBLE PRECISION NOT NULL, entry_spot_price DOUBLE PRECISION NOT NULL,
                entry_perp_price DOUBLE PRECISION NOT NULL, entry_basis_pct DOUBLE PRECISION NOT NULL,
                entry_funding_rate DOUBLE PRECISION NOT NULL, next_funding_time_ms BIGINT,
                last_funding_time_ms BIGINT, funding_accrued_usdt DOUBLE PRECISION NOT NULL DEFAULT 0,
                funding_events INTEGER NOT NULL DEFAULT 0, exit_spot_price DOUBLE PRECISION,
                exit_perp_price DOUBLE PRECISION, exit_basis_pct DOUBLE PRECISION,
                gross_pnl_usdt DOUBLE PRECISION, fee_usdt DOUBLE PRECISION,
                slippage_usdt DOUBLE PRECISION, net_pnl_usdt DOUBLE PRECISION,
                net_return_pct DOUBLE PRECISION, exit_reason VARCHAR(60), meta JSONB
            );
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_bf_one_open_per_symbol ON bf_positions(symbol) WHERE status='OPEN';")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bf_shadow_positions (
                id BIGSERIAL PRIMARY KEY, engine_version VARCHAR(20) NOT NULL,
                shadow_variant VARCHAR(40) NOT NULL, min_net_edge_pct DOUBLE PRECISION NOT NULL,
                symbol VARCHAR(50) NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
                opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), closed_at TIMESTAMPTZ,
                base_qty DOUBLE PRECISION NOT NULL, entry_spot_price DOUBLE PRECISION NOT NULL,
                entry_perp_price DOUBLE PRECISION NOT NULL, entry_basis_pct DOUBLE PRECISION NOT NULL,
                entry_funding_rate DOUBLE PRECISION NOT NULL,
                entry_expected_net_edge_pct DOUBLE PRECISION NOT NULL,
                next_funding_time_ms BIGINT, last_funding_time_ms BIGINT,
                funding_accrued_usdt DOUBLE PRECISION NOT NULL DEFAULT 0,
                funding_events INTEGER NOT NULL DEFAULT 0,
                exit_spot_price DOUBLE PRECISION, exit_perp_price DOUBLE PRECISION,
                exit_basis_pct DOUBLE PRECISION, gross_pnl_usdt DOUBLE PRECISION,
                fee_usdt DOUBLE PRECISION, slippage_usdt DOUBLE PRECISION,
                net_pnl_usdt DOUBLE PRECISION, net_return_pct DOUBLE PRECISION,
                exit_reason VARCHAR(60), meta JSONB
            );
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_bf_shadow_one_open ON bf_shadow_positions(shadow_variant, symbol) WHERE status='OPEN';")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_bf_shadow_variant_time ON bf_shadow_positions(shadow_variant, opened_at DESC);")
        conn.commit()
    finally:
        cur.close(); conn.close()


def _rows(sql, params=()):
    conn = db(); cur = conn.cursor()
    try:
        cur.execute(sql, params)
        rows = cur.fetchall(); cols = [x.name for x in cur.description]
        return [dict(zip(cols, row)) for row in rows]
    finally:
        cur.close(); conn.close()


def get_open_positions():
    return _rows("""
        SELECT id,symbol,opened_at,base_qty,entry_spot_price,entry_perp_price,
               entry_basis_pct,entry_funding_rate,next_funding_time_ms,
               last_funding_time_ms,funding_accrued_usdt,funding_events
        FROM bf_positions WHERE status='OPEN' ORDER BY id;
    """)


def shadow_variant_name(threshold):
    return f"EDGE_{threshold:.3f}"


def get_open_shadow_positions():
    return _rows("""
        SELECT id,shadow_variant,min_net_edge_pct,symbol,opened_at,base_qty,
               entry_spot_price,entry_perp_price,entry_basis_pct,entry_funding_rate,
               entry_expected_net_edge_pct,next_funding_time_ms,last_funding_time_ms,
               funding_accrued_usdt,funding_events
        FROM bf_shadow_positions WHERE status='OPEN' ORDER BY id;
    """)


def log_observation(s, decision, reason):
    meta = {"paper_only": True, "notional_usdt": NOTIONAL_USDT,
            "spot_taker_fee": SPOT_TAKER_FEE, "perp_taker_fee": PERP_TAKER_FEE,
            "extra_slippage_per_leg": EXTRA_SLIPPAGE_PER_LEG,
            "close_basis_pct": CLOSE_BASIS_PCT, "min_net_edge_pct": MIN_NET_EDGE_PCT}
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bf_observations (
                engine_version,symbol,spot_bid,spot_ask,perp_bid,perp_ask,
                open_basis_pct,close_basis_pct,funding_rate,next_funding_time_ms,
                expected_net_edge_pct,decision,reason,meta
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
        """, (ENGINE,s["symbol"],s["spot_bid"],s["spot_ask"],s["perp_bid"],s["perp_ask"],
              s["open_basis_pct"],s["close_basis_pct"],s["funding_rate"],s["next_funding_time_ms"],
              s["expected_net_edge_pct"],decision,reason,json.dumps(meta)))
        conn.commit()
    finally:
        cur.close(); conn.close()


def open_position(s):
    qty = NOTIONAL_USDT / s["spot_ask"]
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bf_positions (
                engine_version,symbol,base_qty,entry_spot_price,entry_perp_price,
                entry_basis_pct,entry_funding_rate,next_funding_time_ms,last_funding_time_ms,meta
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
        """, (ENGINE,s["symbol"],qty,s["spot_ask"],s["perp_bid"],s["open_basis_pct"],
              s["funding_rate"],s["next_funding_time_ms"],None,
              json.dumps({"paper_only": True, "entry_expected_net_edge_pct": s["expected_net_edge_pct"]})))
        pid = int(cur.fetchone()[0]); conn.commit(); return pid
    finally:
        cur.close(); conn.close()


def _accrue_funding_if_due(pos, snap):
    next_ms = int(pos.get("next_funding_time_ms") or 0)
    if next_ms <= 0: return pos
    now_ms = int(time.time() * 1000)
    last_ms = int(pos.get("last_funding_time_ms") or 0)
    if now_ms < next_ms or last_ms >= next_ms: return pos
    qty = float(pos["base_qty"])
    perp_mid = (snap["perp_bid"] + snap["perp_ask"]) / 2.0
    funding_usdt = qty * perp_mid * snap["funding_rate"]
    new_total = float(pos.get("funding_accrued_usdt") or 0) + funding_usdt
    new_events = int(pos.get("funding_events") or 0) + 1
    conn = db(); cur = conn.cursor()
    try:
        cur.execute("""
            UPDATE bf_positions SET funding_accrued_usdt=%s,funding_events=%s,
                last_funding_time_ms=%s,next_funding_time_ms=%s,
                meta=COALESCE(meta,'{}'::jsonb) || %s::jsonb
            WHERE id=%s AND status='OPEN';
        """, (new_total,new_events,next_ms,snap["next_funding_time_ms"],
              json.dumps({"last_funding_rate": snap["funding_rate"], "last_funding_usdt": funding_usdt}), pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()
    pos.update(funding_accrued_usdt=new_total,funding_events=new_events,last_funding_time_ms=next_ms,
               next_funding_time_ms=snap["next_funding_time_ms"])
    return pos


def _calc_close(pos, snap):
    qty = float(pos["base_qty"]); entry_spot = float(pos["entry_spot_price"]); entry_perp = float(pos["entry_perp_price"])
    exit_spot = snap["spot_bid"]; exit_perp = snap["perp_ask"]
    funding = float(pos.get("funding_accrued_usdt") or 0)
    gross = qty*(exit_spot-entry_spot) + qty*(entry_perp-exit_perp) + funding
    fee = qty*(entry_spot*SPOT_TAKER_FEE + exit_spot*SPOT_TAKER_FEE + entry_perp*PERP_TAKER_FEE + exit_perp*PERP_TAKER_FEE)
    slip = qty*(entry_spot+exit_spot+entry_perp+exit_perp)*EXTRA_SLIPPAGE_PER_LEG
    net = gross-fee-slip; capital=qty*entry_spot; ret_pct=net/capital*100.0 if capital>0 else 0.0
    return exit_spot,exit_perp,gross,fee,slip,net,ret_pct,funding


def close_position(pos, snap, reason):
    exit_spot,exit_perp,gross,fee,slip,net,ret_pct,funding = _calc_close(pos,snap)
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            UPDATE bf_positions SET status='CLOSED',closed_at=NOW(),exit_spot_price=%s,
                exit_perp_price=%s,exit_basis_pct=%s,gross_pnl_usdt=%s,fee_usdt=%s,
                slippage_usdt=%s,net_pnl_usdt=%s,net_return_pct=%s,exit_reason=%s,
                meta=COALESCE(meta,'{}'::jsonb) || %s::jsonb
            WHERE id=%s AND status='OPEN';
        """, (exit_spot,exit_perp,snap["close_basis_pct"],gross,fee,slip,net,ret_pct,reason,
              json.dumps({"funding_accrued_usdt": funding, "funding_events": pos.get("funding_events",0)}), pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return net,ret_pct


def should_open(s, open_count):
    if open_count >= MAX_OPEN: return False,"MAX_OPEN"
    if s["funding_rate"] < MIN_FUNDING_RATE: return False,f"FUNDING_BELOW_MIN {s['funding_rate']:.6f}"
    if s["open_basis_pct"] <= 0: return False,f"NO_POSITIVE_PREMIUM {s['open_basis_pct']:.4f}%"
    if s["expected_net_edge_pct"] < MIN_NET_EDGE_PCT: return False,f"NET_EDGE_TOO_SMALL {s['expected_net_edge_pct']:.4f}%"
    return True,"OPEN_CASH_AND_CARRY"


def should_close(pos, s):
    age_h=(datetime.now(timezone.utc)-pos["opened_at"]).total_seconds()/3600.0
    if s["close_basis_pct"] <= CLOSE_BASIS_PCT: return True,"BASIS_CONVERGED"
    if s["funding_rate"] <= FUNDING_EXIT_RATE: return True,"FUNDING_TURNED_ADVERSE"
    if age_h >= MAX_HOLD_HOURS: return True,"MAX_HOLD"
    return False,"HOLD"


def shadow_should_open(s, threshold):
    if s["funding_rate"] < MIN_FUNDING_RATE: return False,"FUNDING_BELOW_MIN"
    if s["open_basis_pct"] <= 0: return False,"NO_POSITIVE_PREMIUM"
    if s["expected_net_edge_pct"] < threshold: return False,"EDGE_BELOW_SHADOW_THRESHOLD"
    return True,"SHADOW_OPEN"


def open_shadow_position(s, threshold):
    variant=shadow_variant_name(threshold); qty=NOTIONAL_USDT/s["spot_ask"]
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            INSERT INTO bf_shadow_positions (
                engine_version,shadow_variant,min_net_edge_pct,symbol,base_qty,entry_spot_price,
                entry_perp_price,entry_basis_pct,entry_funding_rate,entry_expected_net_edge_pct,
                next_funding_time_ms,last_funding_time_ms,meta
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
        """,(ENGINE,variant,threshold,s["symbol"],qty,s["spot_ask"],s["perp_bid"],s["open_basis_pct"],
             s["funding_rate"],s["expected_net_edge_pct"],s["next_funding_time_ms"],None,
             json.dumps({"paper_only":True,"shadow":True})))
        pid=int(cur.fetchone()[0]); conn.commit(); return pid
    finally:
        cur.close(); conn.close()


def accrue_shadow_funding_if_due(pos,snap):
    next_ms=int(pos.get("next_funding_time_ms") or 0)
    if next_ms<=0: return pos
    now_ms=int(time.time()*1000); last_ms=int(pos.get("last_funding_time_ms") or 0)
    if now_ms<next_ms or last_ms>=next_ms: return pos
    qty=float(pos["base_qty"]); perp_mid=(snap["perp_bid"]+snap["perp_ask"])/2.0
    funding_usdt=qty*perp_mid*snap["funding_rate"]
    new_total=float(pos.get("funding_accrued_usdt") or 0)+funding_usdt; new_events=int(pos.get("funding_events") or 0)+1
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            UPDATE bf_shadow_positions SET funding_accrued_usdt=%s,funding_events=%s,
                last_funding_time_ms=%s,next_funding_time_ms=%s,
                meta=COALESCE(meta,'{}'::jsonb) || %s::jsonb
            WHERE id=%s AND status='OPEN';
        """,(new_total,new_events,next_ms,snap["next_funding_time_ms"],
             json.dumps({"last_funding_rate":snap["funding_rate"],"last_funding_usdt":funding_usdt}),pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()
    pos.update(funding_accrued_usdt=new_total,funding_events=new_events,last_funding_time_ms=next_ms,
               next_funding_time_ms=snap["next_funding_time_ms"])
    return pos


def close_shadow_position(pos,snap,reason):
    exit_spot,exit_perp,gross,fee,slip,net,ret_pct,funding=_calc_close(pos,snap)
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            UPDATE bf_shadow_positions SET status='CLOSED',closed_at=NOW(),exit_spot_price=%s,
                exit_perp_price=%s,exit_basis_pct=%s,gross_pnl_usdt=%s,fee_usdt=%s,
                slippage_usdt=%s,net_pnl_usdt=%s,net_return_pct=%s,exit_reason=%s,
                meta=COALESCE(meta,'{}'::jsonb) || %s::jsonb
            WHERE id=%s AND status='OPEN';
        """,(exit_spot,exit_perp,snap["close_basis_pct"],gross,fee,slip,net,ret_pct,reason,
             json.dumps({"funding_accrued_usdt":funding,"funding_events":pos.get("funding_events",0)}),pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return net,ret_pct


def process_shadow_snapshot(snap,shadow_opens):
    symbol=snap["symbol"]
    for threshold in SHADOW_MIN_EDGE_PCTS:
        variant=shadow_variant_name(threshold); key=(variant,symbol); pos=shadow_opens.get(key)
        if pos:
            pos=accrue_shadow_funding_if_due(pos,snap); close,reason=should_close(pos,snap)
            if close:
                net,ret=close_shadow_position(pos,snap,reason); shadow_opens.pop(key,None)
                print(f"BF1 SHADOW CLOSE {variant} {symbol} {reason} net={net:.4f}U ret={ret:.4f}%",flush=True)
        else:
            ok,_=shadow_should_open(snap,threshold)
            if ok:
                pid=open_shadow_position(snap,threshold)
                shadow_opens[key]={"id":pid,"shadow_variant":variant,"min_net_edge_pct":threshold,
                    "symbol":symbol,"opened_at":datetime.now(timezone.utc),"base_qty":NOTIONAL_USDT/snap["spot_ask"],
                    "entry_spot_price":snap["spot_ask"],"entry_perp_price":snap["perp_bid"],
                    "entry_basis_pct":snap["open_basis_pct"],"entry_funding_rate":snap["funding_rate"],
                    "entry_expected_net_edge_pct":snap["expected_net_edge_pct"],
                    "next_funding_time_ms":snap["next_funding_time_ms"],"last_funding_time_ms":None,
                    "funding_accrued_usdt":0.0,"funding_events":0}
                print(f"BF1 SHADOW OPEN {variant} #{pid} {symbol} edge={snap['expected_net_edge_pct']:.4f}%",flush=True)


def run_once():
    init_database()
    opens={p["symbol"]:p for p in get_open_positions()}
    shadow_opens={(p["shadow_variant"],p["symbol"]):p for p in get_open_shadow_positions()}
    for idx,symbol in enumerate(SYMBOLS):
        try:
            snap=market_snapshot(symbol); pos=opens.get(symbol)
            if pos:
                pos=_accrue_funding_if_due(pos,snap); close,reason=should_close(pos,snap)
                if close:
                    net,ret=close_position(pos,snap,reason); log_observation(snap,"CLOSE",f"{reason}; net={net:.4f}U ret={ret:.4f}%")
                    opens.pop(symbol,None); print(f"BF1 CLOSE {symbol} {reason} net={net:.4f}U ret={ret:.4f}%",flush=True)
                else:
                    log_observation(snap,"HOLD",reason)
                    print(f"BF1 HOLD {symbol} openBasis={snap['open_basis_pct']:.4f}% closeBasis={snap['close_basis_pct']:.4f}% funding={snap['funding_rate']:.6f}",flush=True)
            else:
                ok,reason=should_open(snap,len(opens))
                if ok:
                    pid=open_position(snap); log_observation(snap,"OPEN",reason); opens[symbol]={"id":pid,"symbol":symbol}
                    print(f"BF1 OPEN #{pid} {symbol} basis={snap['open_basis_pct']:.4f}% funding={snap['funding_rate']:.6f} netEdge={snap['expected_net_edge_pct']:.4f}%",flush=True)
                else:
                    log_observation(snap,"SKIP",reason)
                    print(f"BF1 SKIP {symbol} basis={snap['open_basis_pct']:.4f}% funding={snap['funding_rate']:.6f} netEdge={snap['expected_net_edge_pct']:.4f}% {reason}",flush=True)
            process_shadow_snapshot(snap,shadow_opens)
        except Exception as exc:
            print(f"BF1 ERROR {symbol}: {exc}",flush=True)
        if idx != len(SYMBOLS)-1: time.sleep(API_DELAY_SEC)


if __name__ == "__main__":
    if not PAPER_ONLY:
        raise RuntimeError("BF1 must remain paper-only in this research branch")
    run_once()
