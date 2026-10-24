"""
MEME2 — Solana smart-wallet copy research core.

PAPER ONLY. Reads public Solana RPC + public market data. Never signs or sends a
transaction and never needs a private key.

Research idea:
- watch explicitly configured public wallet addresses;
- detect fresh token buys/sells from pre/post token balances;
- require meaningful SOL flow to reduce airdrop/transfer false positives;
- simulate our copy entry at the price available when WE detect the trade;
- model liquidity-dependent market impact + slippage + fees;
- support a continuous low-latency monitor mode;
- mirror target sells and also enforce independent stop/trailing/TP/timeout exits.
"""

import json
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

ENGINE = "MEME2"
PAPER_ONLY = True
DATABASE_URL = os.environ.get("DATABASE_URL", "")
SOLANA_RPC = os.environ.get("MEME2_RPC_HTTP", "https://api.mainnet.solana.com").strip()
GECKO_BASE = "https://api.geckoterminal.com/api/v2"
GECKO_VERSION = "20230203"
USER_AGENT = "MEME2-paper-research/2.0"

WALLETS = [x.strip() for x in os.environ.get("MEME2_WALLETS", "").split(",") if x.strip()]
MAX_WALLETS = int(os.environ.get("MEME2_MAX_WALLETS", "5"))
WALLETS = WALLETS[:max(0, MAX_WALLETS)]
POLL_INTERVAL_SEC = float(os.environ.get("MEME2_POLL_INTERVAL_SEC", "15"))
HEALTH_PORT = int(os.environ.get("PORT", "10000"))
HTTP_RETRIES = int(os.environ.get("MEME2_HTTP_RETRIES", "4"))
HTTP_RETRY_BASE_SEC = float(os.environ.get("MEME2_HTTP_RETRY_BASE_SEC", "2.0"))
LIQUIDITY_SIDE_FRACTION = float(os.environ.get("MEME2_LIQUIDITY_SIDE_FRACTION", "0.50"))
SIG_LIMIT = int(os.environ.get("MEME2_SIG_LIMIT", "20"))
MAX_TX_VERSION = int(os.environ.get("MEME2_MAX_TX_VERSION", "1"))
MAX_SIGNAL_AGE_SEC = float(os.environ.get("MEME2_MAX_SIGNAL_AGE_SEC", "180"))
MIN_SOL_FLOW = float(os.environ.get("MEME2_MIN_SOL_FLOW", "0.0001"))
REQUIRE_SOL_FLOW = os.environ.get("MEME2_REQUIRE_SOL_FLOW", "true").lower() == "true"

NOTIONAL_USDT = float(os.environ.get("MEME2_NOTIONAL_USDT", "20"))
MAX_OPEN = int(os.environ.get("MEME2_MAX_OPEN", "20"))
MIN_LIQUIDITY_USD = float(os.environ.get("MEME2_MIN_LIQUIDITY_USD", "10000"))
ENTRY_SLIPPAGE_BPS = float(os.environ.get("MEME2_ENTRY_SLIPPAGE_BPS", "150"))
EXIT_SLIPPAGE_BPS = float(os.environ.get("MEME2_EXIT_SLIPPAGE_BPS", "200"))
FEE_BPS_PER_SIDE = float(os.environ.get("MEME2_FEE_BPS_PER_SIDE", "100"))
STOP_LOSS_PCT = float(os.environ.get("MEME2_STOP_LOSS_PCT", "25"))
TAKE_PROFIT_PCT = float(os.environ.get("MEME2_TAKE_PROFIT_PCT", "100"))
TRAIL_ACTIVATE_PCT = float(os.environ.get("MEME2_TRAIL_ACTIVATE_PCT", "25"))
TRAIL_DISTANCE_PCT = float(os.environ.get("MEME2_TRAIL_DISTANCE_PCT", "12"))
MAX_HOLD_MIN = float(os.environ.get("MEME2_MAX_HOLD_MIN", "240"))

WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
IGNORE_MINTS = {WSOL, USDC, USDT}
LAMPORTS_PER_SOL = 1_000_000_000.0


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
    h = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    if headers:
        h.update(headers)
    last_exc = None
    for attempt in range(max(1, HTTP_RETRIES)):
        req = urllib.request.Request(url, headers=h, method="GET")
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
            print(f"MEME2 HTTP {exc.code}; retry in {delay:.1f}s", flush=True)
            time.sleep(max(0.5, delay))
        except Exception as exc:
            last_exc = exc
            if attempt >= HTTP_RETRIES - 1:
                raise
            time.sleep(max(0.5, HTTP_RETRY_BASE_SEC * (2 ** attempt)))
    raise RuntimeError(f"HTTP request failed: {last_exc}")


def gecko_get(path):
    return _json_get(GECKO_BASE + path, {"Accept": f"application/json;version={GECKO_VERSION}"})


def rpc(method, params):
    body = json.dumps({"jsonrpc":"2.0","id":1,"method":method,"params":params}).encode("utf-8")
    last_exc = None
    for attempt in range(max(1, HTTP_RETRIES)):
        req = urllib.request.Request(
            SOLANA_RPC, data=body, method="POST",
            headers={"Content-Type":"application/json","User-Agent":USER_AGENT},
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            if payload.get("error"):
                raise RuntimeError(f"RPC {method} error: {payload['error']}")
            return payload.get("result")
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
            print(f"MEME2 RPC HTTP {exc.code}; retry in {delay:.1f}s", flush=True)
            time.sleep(max(0.5, delay))
        except Exception as exc:
            last_exc = exc
            if attempt >= HTTP_RETRIES - 1:
                raise
            time.sleep(max(0.5, HTTP_RETRY_BASE_SEC * (2 ** attempt)))
    raise RuntimeError(f"RPC {method} failed: {last_exc}")


def _quote_side_reserve_usd(liquidity_usd):
    liq = max(0.0, _safe_float(liquidity_usd))
    return max(1e-9, liq * max(0.05, min(0.95, LIQUIDITY_SIDE_FRACTION)))


def executable_entry(raw_price, notional_usdt, liquidity_usd):
    raw = max(0.0, _safe_float(raw_price))
    if raw <= 0:
        return 0.0, 0.0
    reserve = _quote_side_reserve_usd(liquidity_usd)
    impact_ratio = max(0.0, _safe_float(notional_usdt)) / reserve
    fixed = max(0.0, ENTRY_SLIPPAGE_BPS) / 10000.0
    price = raw * (1.0 + impact_ratio) * (1.0 + fixed)
    return price, (price / raw - 1.0) * 10000.0


def executable_exit(raw_price, token_qty, liquidity_usd):
    raw = max(0.0, _safe_float(raw_price))
    qty = max(0.0, _safe_float(token_qty))
    if raw <= 0 or qty <= 0:
        return 0.0, 10000.0
    reserve = _quote_side_reserve_usd(liquidity_usd)
    raw_value = qty * raw
    impact_ratio = raw_value / reserve
    fixed = max(0.0, min(0.95, EXIT_SLIPPAGE_BPS / 10000.0))
    price = raw * (1.0 - fixed) / (1.0 + impact_ratio)
    return max(0.0, price), max(0.0, (1.0 - price / raw) * 10000.0)


def init_database():
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meme2_wallet_state (
                wallet VARCHAR(80) PRIMARY KEY,
                initialized_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_signature VARCHAR(120),
                meta JSONB
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meme2_signals (
                id BIGSERIAL PRIMARY KEY,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                engine_version VARCHAR(20) NOT NULL,
                wallet VARCHAR(80) NOT NULL,
                signature VARCHAR(120) NOT NULL,
                block_time TIMESTAMPTZ,
                detection_delay_sec DOUBLE PRECISION,
                side VARCHAR(10) NOT NULL,
                mint_address VARCHAR(80) NOT NULL,
                token_delta DOUBLE PRECISION,
                pre_token_balance DOUBLE PRECISION,
                sol_delta DOUBLE PRECISION,
                detected_price_usd DOUBLE PRECISION,
                detected_liquidity_usd DOUBLE PRECISION,
                decision VARCHAR(40) NOT NULL,
                reason TEXT,
                meta JSONB,
                UNIQUE(wallet,signature,mint_address,side)
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme2_signal_time ON meme2_signals(observed_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme2_signal_wallet ON meme2_signals(wallet, observed_at DESC);")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS meme2_positions (
                id BIGSERIAL PRIMARY KEY,
                engine_version VARCHAR(20) NOT NULL,
                source_wallet VARCHAR(80) NOT NULL,
                source_signature VARCHAR(120) NOT NULL,
                mint_address VARCHAR(80) NOT NULL,
                status VARCHAR(20) NOT NULL DEFAULT 'OPEN',
                opened_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                closed_at TIMESTAMPTZ,
                notional_usdt DOUBLE PRECISION NOT NULL,
                token_qty DOUBLE PRECISION NOT NULL,
                raw_entry_price DOUBLE PRECISION NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                entry_fee_usdt DOUBLE PRECISION NOT NULL,
                entry_liquidity_usd DOUBLE PRECISION,
                detection_delay_sec DOUBLE PRECISION,
                peak_price DOUBLE PRECISION NOT NULL,
                last_price DOUBLE PRECISION,
                exit_price DOUBLE PRECISION,
                exit_fee_usdt DOUBLE PRECISION,
                gross_pnl_usdt DOUBLE PRECISION,
                net_pnl_usdt DOUBLE PRECISION,
                net_return_pct DOUBLE PRECISION,
                exit_reason VARCHAR(80),
                exit_source_signature VARCHAR(120),
                meta JSONB
            );
        """)
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_meme2_one_open_wallet_mint ON meme2_positions(source_wallet,mint_address) WHERE status='OPEN';")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_meme2_pos_time ON meme2_positions(opened_at DESC);")
        conn.commit()
    finally:
        cur.close(); conn.close()


def wallet_state(wallet):
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("SELECT last_signature FROM meme2_wallet_state WHERE wallet=%s;",(wallet,))
        row=cur.fetchone(); return row[0] if row else None
    finally:
        cur.close(); conn.close()


def set_wallet_state(wallet, signature, meta=None):
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            INSERT INTO meme2_wallet_state(wallet,last_signature,meta)
            VALUES(%s,%s,%s)
            ON CONFLICT(wallet) DO UPDATE SET last_signature=EXCLUDED.last_signature,updated_at=NOW(),
                meta=COALESCE(meme2_wallet_state.meta,'{}'::jsonb) || EXCLUDED.meta;
        """,(wallet,signature,json.dumps(meta or {})))
        conn.commit()
    finally:
        cur.close(); conn.close()


def get_open_positions():
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            SELECT id,source_wallet,source_signature,mint_address,opened_at,notional_usdt,token_qty,
                   raw_entry_price,entry_price,entry_fee_usdt,entry_liquidity_usd,detection_delay_sec,
                   peak_price,last_price
            FROM meme2_positions WHERE status='OPEN' ORDER BY id;
        """)
        cols=[d.name for d in cur.description]
        return [dict(zip(cols,r)) for r in cur.fetchall()]
    finally:
        cur.close(); conn.close()


def token_amount(b):
    ui=(b or {}).get("uiTokenAmount") or {}
    s=ui.get("uiAmountString")
    if s is not None:
        return _safe_float(s)
    raw=_safe_float(ui.get("amount")); dec=int(_safe_float(ui.get("decimals")))
    return raw/(10**dec) if dec>=0 else 0.0


def parse_wallet_changes(wallet, tx):
    meta=(tx or {}).get("meta") or {}
    message=(((tx or {}).get("transaction") or {}).get("message") or {})
    keys=message.get("accountKeys") or []
    pubkeys=[]
    for k in keys:
        pubkeys.append(k.get("pubkey") if isinstance(k,dict) else k)

    sol_delta=0.0
    try:
        idx=pubkeys.index(wallet)
        pre=(meta.get("preBalances") or [])[idx]
        post=(meta.get("postBalances") or [])[idx]
        sol_delta=(float(post)-float(pre))/LAMPORTS_PER_SOL
    except Exception:
        pass

    pre_map={}
    for b in meta.get("preTokenBalances") or []:
        if b.get("owner") == wallet:
            pre_map[b.get("mint")]=token_amount(b)
    post_map={}
    for b in meta.get("postTokenBalances") or []:
        if b.get("owner") == wallet:
            post_map[b.get("mint")]=token_amount(b)

    changes=[]
    for mint in set(pre_map)|set(post_map):
        if not mint or mint in IGNORE_MINTS:
            continue
        pre=pre_map.get(mint,0.0); post=post_map.get(mint,0.0); delta=post-pre
        if abs(delta) <= 1e-15:
            continue
        side="BUY" if delta>0 else "SELL"
        changes.append({"mint":mint,"side":side,"delta":delta,"pre":pre,"post":post,"sol_delta":sol_delta})
    return changes


def fetch_signatures(wallet):
    return rpc("getSignaturesForAddress",[wallet,{"commitment":"confirmed","limit":SIG_LIMIT}]) or []


def fetch_transaction(signature):
    return rpc("getTransaction",[
        signature,
        {"commitment":"confirmed","encoding":"jsonParsed","maxSupportedTransactionVersion":MAX_TX_VERSION},
    ])


def get_token_market(mint):
    try:
        payload=gecko_get(f"/networks/solana/tokens/{urllib.parse.quote(mint)}/pools?page=1")
        best=None
        for item in payload.get("data") or []:
            a=item.get("attributes") or {}
            price=_safe_float(a.get("base_token_price_usd"))
            liq=_safe_float(a.get("reserve_in_usd"))
            if price<=0:
                continue
            row={"price":price,"liq":liq,"pool_address":a.get("address"),"pool_name":a.get("name")}
            if best is None or row["liq"]>best["liq"]:
                best=row
        return best
    except Exception:
        return None


def get_prices(mints):
    out={}
    uniq=list(dict.fromkeys([m for m in mints if m]))
    for i in range(0,len(uniq),30):
        chunk=uniq[i:i+30]
        if not chunk:
            continue
        try:
            path="/simple/networks/solana/token_price/"+",".join(urllib.parse.quote(x) for x in chunk)
            payload=gecko_get(path)
            prices=(((payload or {}).get("data") or {}).get("attributes") or {}).get("token_prices") or {}
            for mint,val in prices.items():
                px=_safe_float(val)
                if px>0: out[mint]=px
        except Exception as exc:
            print(f"MEME2 price batch failed: {exc}",flush=True)
        if i+30<len(uniq): time.sleep(1.0)
    return out


def log_signal(wallet, signature, block_time, delay, ch, price, liq, decision, reason, meta=None):
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            INSERT INTO meme2_signals(
                engine_version,wallet,signature,block_time,detection_delay_sec,side,mint_address,
                token_delta,pre_token_balance,sol_delta,detected_price_usd,detected_liquidity_usd,
                decision,reason,meta
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(wallet,signature,mint_address,side) DO NOTHING;
        """,(ENGINE,wallet,signature,block_time,delay,ch["side"],ch["mint"],ch["delta"],ch["pre"],ch["sol_delta"],
             price,liq,decision,reason,json.dumps(meta or {})))
        conn.commit()
    finally:
        cur.close(); conn.close()


def open_position(wallet, signature, mint, market, delay):
    raw=market["price"]
    entry, impact_bps = executable_entry(raw, NOTIONAL_USDT, market["liq"])
    fee=NOTIONAL_USDT*FEE_BPS_PER_SIDE/10000.0
    qty=max(0.0,NOTIONAL_USDT-fee)/entry
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            INSERT INTO meme2_positions(
                engine_version,source_wallet,source_signature,mint_address,notional_usdt,token_qty,
                raw_entry_price,entry_price,entry_fee_usdt,entry_liquidity_usd,detection_delay_sec,
                peak_price,last_price,meta
            ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id;
        """,(ENGINE,wallet,signature,mint,NOTIONAL_USDT,qty,raw,entry,fee,market["liq"],delay,raw,raw,
             json.dumps({"paper_only":True,"pool_address":market.get("pool_address"),"pool_name":market.get("pool_name"),
                         "entry_execution_bps":impact_bps,"execution_model":"constant_product_liquidity_impact_v2"})))
        pid=int(cur.fetchone()[0]); conn.commit(); return pid
    finally:
        cur.close(); conn.close()


def close_position(pos, market, reason, exit_signature=None):
    raw_price=market["price"]
    exit_price, impact_bps = executable_exit(raw_price, pos["token_qty"], market.get("liq", 0.0))
    gross_value=float(pos["token_qty"])*exit_price
    exit_fee=gross_value*FEE_BPS_PER_SIDE/10000.0
    proceeds=gross_value-exit_fee
    net=proceeds-float(pos["notional_usdt"])
    ret=net/float(pos["notional_usdt"])*100.0
    gross=float(pos["token_qty"])*raw_price-float(pos["notional_usdt"])
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("""
            UPDATE meme2_positions SET status='CLOSED',closed_at=NOW(),last_price=%s,exit_price=%s,
                exit_fee_usdt=%s,gross_pnl_usdt=%s,net_pnl_usdt=%s,net_return_pct=%s,
                exit_reason=%s,exit_source_signature=%s,
                meta=COALESCE(meta,'{}'::jsonb) || %s::jsonb
            WHERE id=%s AND status='OPEN';
        """,(raw_price,exit_price,exit_fee,gross,net,ret,reason,exit_signature,
             json.dumps({"exit_execution_bps":impact_bps,"exit_slippage_bps":EXIT_SLIPPAGE_BPS,
                         "fee_bps_per_side":FEE_BPS_PER_SIDE,"exit_liquidity_usd":market.get("liq"),
                         "execution_model":"constant_product_liquidity_impact_v2"}),pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()
    return net,ret


def update_mark(pos, price, peak):
    conn=db(); cur=conn.cursor()
    try:
        cur.execute("UPDATE meme2_positions SET last_price=%s,peak_price=%s WHERE id=%s AND status='OPEN';",
                    (price,peak,pos["id"]))
        conn.commit()
    finally:
        cur.close(); conn.close()


def price_exit_reason(pos, price):
    entry=float(pos["raw_entry_price"])
    if entry<=0 or price<=0:
        return None
    roi=(price/entry-1.0)*100.0
    peak=max(float(pos.get("peak_price") or entry),price)
    peak_roi=(peak/entry-1.0)*100.0
    trail_floor=peak*(1.0-TRAIL_DISTANCE_PCT/100.0)
    age=(datetime.now(timezone.utc)-pos["opened_at"]).total_seconds()/60.0
    if roi<=-STOP_LOSS_PCT: return "STOP_LOSS"
    if peak_roi>=TRAIL_ACTIVATE_PCT and price<=trail_floor: return "TRAILING_STOP"
    if roi>=TAKE_PROFIT_PCT: return "TAKE_PROFIT"
    if age>=MAX_HOLD_MIN: return "MAX_HOLD"
    return None


def refresh_open_positions():
    opens=get_open_positions()
    if not opens:
        return
    prices=get_prices([p["mint_address"] for p in opens])
    for pos in opens:
        price=prices.get(pos["mint_address"])
        if not price:
            continue
        peak=max(float(pos.get("peak_price") or price),price)
        reason=price_exit_reason(pos,price)
        if reason:
            market=get_token_market(pos["mint_address"])
            if market and market["price"]>0:
                net,ret=close_position(pos,market,reason)
                print(f"MEME2 CLOSE #{pos['id']} {reason} net={net:+.4f}U ret={ret:+.2f}%",flush=True)
        else:
            update_mark(pos,price,peak)


def find_open(wallet,mint):
    for p in get_open_positions():
        if p["source_wallet"]==wallet and p["mint_address"]==mint:
            return p
    return None


def process_change(wallet, signature, block_time_unix, ch):
    now=time.time()
    delay=max(0.0,now-float(block_time_unix or now))
    block_dt=datetime.fromtimestamp(block_time_unix,tz=timezone.utc) if block_time_unix else None

    # SOL-flow gate reduces airdrops, plain transfers and account housekeeping.
    flow_ok=True
    if REQUIRE_SOL_FLOW:
        if ch["side"]=="BUY": flow_ok=ch["sol_delta"] <= -MIN_SOL_FLOW
        else: flow_ok=ch["sol_delta"] >= MIN_SOL_FLOW
    if not flow_ok:
        log_signal(wallet,signature,block_dt,delay,ch,None,None,"SKIP","NO_MATCHING_SOL_FLOW")
        return
    if delay>MAX_SIGNAL_AGE_SEC:
        log_signal(wallet,signature,block_dt,delay,ch,None,None,"SKIP","STALE_SIGNAL")
        return

    if ch["side"]=="SELL":
        pos=find_open(wallet,ch["mint"])
        market=get_token_market(ch["mint"]) if pos else None
        if not pos:
            log_signal(wallet,signature,block_dt,delay,ch,None,None,"OBSERVE","SELL_WITHOUT_OPEN_COPY")
            return
        if not market or market["price"]<=0:
            log_signal(wallet,signature,block_dt,delay,ch,None,None,"WATCH","NO_EXIT_PRICE")
            return
        net,ret=close_position(pos,market,"MIRROR_SELL",signature)
        log_signal(wallet,signature,block_dt,delay,ch,market["price"],market["liq"],"CLOSE","MIRROR_SELL",
                   {"position_id":pos["id"],"net_pnl_usdt":net,"net_return_pct":ret})
        print(f"MEME2 MIRROR CLOSE #{pos['id']} wallet={wallet[:6]}.. ret={ret:+.2f}%",flush=True)
        return

    # BUY
    if find_open(wallet,ch["mint"]):
        log_signal(wallet,signature,block_dt,delay,ch,None,None,"SKIP","ALREADY_OPEN")
        return
    if len(get_open_positions())>=MAX_OPEN:
        log_signal(wallet,signature,block_dt,delay,ch,None,None,"WATCH","MAX_OPEN")
        return
    market=get_token_market(ch["mint"])
    if not market or market["price"]<=0:
        log_signal(wallet,signature,block_dt,delay,ch,None,None,"SKIP","NO_MARKET_PRICE")
        return
    if market["liq"]<MIN_LIQUIDITY_USD:
        log_signal(wallet,signature,block_dt,delay,ch,market["price"],market["liq"],"SKIP","LOW_LIQUIDITY")
        return
    pid=open_position(wallet,signature,ch["mint"],market,delay)
    log_signal(wallet,signature,block_dt,delay,ch,market["price"],market["liq"],"OPEN","COPY_BUY",
               {"position_id":pid,"paper_only":True})
    print(f"MEME2 OPEN #{pid} wallet={wallet[:6]}.. delay={delay:.1f}s liq=${market['liq']:.0f}",flush=True)


def process_wallet(wallet):
    sigs=fetch_signatures(wallet)
    if not sigs:
        return
    last=wallet_state(wallet)
    latest=sigs[0].get("signature")
    if not last:
        # Critical safety: do not replay historical trades on first startup.
        set_wallet_state(wallet,latest,{"seeded":True,"seed_count":len(sigs)})
        print(f"MEME2 seed wallet={wallet[:8]}.. latest={latest[:10]}.. no historical replay",flush=True)
        return

    fresh=[]
    for s in sigs:
        if s.get("signature")==last:
            break
        if not s.get("err"):
            fresh.append(s)
    # Oldest to newest preserves the target wallet's event order.
    for s in reversed(fresh):
        signature=s.get("signature")
        try:
            tx=fetch_transaction(signature)
            if not tx or (tx.get("meta") or {}).get("err"):
                continue
            for ch in parse_wallet_changes(wallet,tx):
                process_change(wallet,signature,tx.get("blockTime") or s.get("blockTime"),ch)
        except Exception as exc:
            print(f"MEME2 tx error wallet={wallet[:6]} sig={signature[:10]}: {exc}",flush=True)
        time.sleep(0.15)
    set_wallet_state(wallet,latest,{"fresh_processed":len(fresh)})


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            body = json.dumps({
                "status":"ok", "engine":ENGINE, "mode":"wallet_monitor",
                "paper_only":True, "poll_interval_sec":POLL_INTERVAL_SEC,
                "wallets":len(WALLETS)
            }).encode()
            self.send_response(200)
            self.send_header("Content-Type","application/json")
            self.send_header("Content-Length",str(len(body)))
            self.end_headers(); self.wfile.write(body)
        else:
            self.send_response(404); self.end_headers()

    def log_message(self, fmt, *args):
        return


def _start_health_server():
    server=ThreadingHTTPServer(("0.0.0.0",HEALTH_PORT),_HealthHandler)
    thread=threading.Thread(target=server.serve_forever,daemon=True)
    thread.start(); return server


def run_once():
    init_database()
    refresh_open_positions()
    if not WALLETS:
        print("MEME2 idle: set MEME2_WALLETS to one or more public Solana wallet addresses",flush=True)
        return
    for wallet in WALLETS:
        try:
            process_wallet(wallet)
        except Exception as exc:
            print(f"MEME2 wallet error {wallet[:8]}..: {exc}",flush=True)
        time.sleep(0.5)
    print(f"MEME2 done wallets={len(WALLETS)} open={len(get_open_positions())}",flush=True)


def monitor_loop():
    init_database()
    _start_health_server()
    print(f"MEME2 monitor started interval={POLL_INTERVAL_SEC:.1f}s wallets={len(WALLETS)} port={HEALTH_PORT}", flush=True)
    while True:
        started=time.time()
        try:
            run_once()
        except Exception as exc:
            print(f"MEME2 monitor error: {type(exc).__name__}: {exc}", flush=True)
        elapsed=time.time()-started
        time.sleep(max(1.0,POLL_INTERVAL_SEC-elapsed))


if __name__ == "__main__":
    mode=os.environ.get("MEME2_MODE","cron").strip().lower()
    if "--monitor" in sys.argv or mode=="monitor":
        monitor_loop()
    else:
        run_once()
