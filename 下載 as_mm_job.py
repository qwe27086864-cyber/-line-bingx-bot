"""
Avellaneda-Stoikov inspired market-making research core for BingX perpetuals.

PAPER ONLY. Public WebSocket market data only; no private order endpoints.

The simulator:
- consumes real-time bookTicker + trade streams;
- estimates short-horizon log-return variance from mid prices;
- computes reservation price + optimal spread from the AS model;
- applies an explicit maker-fee-aware spread floor;
- simulates maker fills only when matching taker trade flow reaches the quote;
- models queue-ahead by requiring cumulative touched trade quantity before fill;
- marks inventory to market and stores snapshots/fills/equity in Postgres.

This is a research implementation, not a claim of profitability.
"""

import gzip
import json
import math
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import statistics
import threading
import time
import uuid
from collections import deque
from datetime import datetime, timezone

import psycopg2
import websocket

ENGINE = "AS1"
PAPER_ONLY = True
DATABASE_URL = os.environ.get("DATABASE_URL", "")

SYMBOL = os.environ.get("AS_SYMBOL", "BTC-USDT").strip().upper()
WS_URL = "wss://open-api-swap.bingx.com/swap-market"

START_CASH = float(os.environ.get("AS_START_CASH", "1000"))
ORDER_NOTIONAL_USDT = float(os.environ.get("AS_ORDER_NOTIONAL_USDT", "20"))
MAX_INVENTORY_NOTIONAL = float(os.environ.get("AS_MAX_INVENTORY_NOTIONAL", "100"))
MAKER_FEE = float(os.environ.get("AS_MAKER_FEE", "0.0002"))

# AS parameters in log-price space.
GAMMA = float(os.environ.get("AS_GAMMA", "0.10"))
KAPPA = float(os.environ.get("AS_KAPPA", "2500"))
HORIZON_SEC = float(os.environ.get("AS_HORIZON_SEC", "300"))
MIN_HALF_SPREAD_BPS = float(os.environ.get("AS_MIN_HALF_SPREAD_BPS", "4.0"))
MAX_HALF_SPREAD_BPS = float(os.environ.get("AS_MAX_HALF_SPREAD_BPS", "40.0"))

VOL_WINDOW = int(os.environ.get("AS_VOL_WINDOW", "300"))
REQUOTE_SEC = float(os.environ.get("AS_REQUOTE_SEC", "2.0"))
SNAPSHOT_SEC = float(os.environ.get("AS_SNAPSHOT_SEC", "10.0"))
QUEUE_AHEAD_MULT = float(os.environ.get("AS_QUEUE_AHEAD_MULT", "1.5"))
QUOTE_CHANGE_RESET_BPS = float(os.environ.get("AS_QUOTE_CHANGE_RESET_BPS", "0.5"))


def db():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_database():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS as_mm_state (
                symbol VARCHAR(50) PRIMARY KEY,
                engine_version VARCHAR(20) NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                cash_usdt DOUBLE PRECISION NOT NULL,
                inventory_base DOUBLE PRECISION NOT NULL,
                fees_usdt DOUBLE PRECISION NOT NULL,
                buy_fills INTEGER NOT NULL,
                sell_fills INTEGER NOT NULL,
                last_mid DOUBLE PRECISION,
                equity_usdt DOUBLE PRECISION,
                net_pnl_usdt DOUBLE PRECISION,
                meta JSONB
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS as_mm_fills (
                id BIGSERIAL PRIMARY KEY,
                filled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                engine_version VARCHAR(20) NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                qty DOUBLE PRECISION NOT NULL,
                notional_usdt DOUBLE PRECISION NOT NULL,
                maker_fee_usdt DOUBLE PRECISION NOT NULL,
                inventory_after DOUBLE PRECISION NOT NULL,
                cash_after DOUBLE PRECISION NOT NULL,
                mid_at_fill DOUBLE PRECISION NOT NULL,
                quote_age_sec DOUBLE PRECISION,
                touched_qty DOUBLE PRECISION,
                meta JSONB
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_as_fills_time
            ON as_mm_fills(symbol, filled_at DESC);
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS as_mm_snapshots (
                id BIGSERIAL PRIMARY KEY,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                engine_version VARCHAR(20) NOT NULL,
                symbol VARCHAR(50) NOT NULL,
                best_bid DOUBLE PRECISION NOT NULL,
                best_ask DOUBLE PRECISION NOT NULL,
                mid DOUBLE PRECISION NOT NULL,
                market_spread_bps DOUBLE PRECISION NOT NULL,
                sigma2_horizon DOUBLE PRECISION NOT NULL,
                inventory_base DOUBLE PRECISION NOT NULL,
                inventory_notional DOUBLE PRECISION NOT NULL,
                inventory_norm DOUBLE PRECISION NOT NULL,
                reservation_price DOUBLE PRECISION NOT NULL,
                bid_quote DOUBLE PRECISION NOT NULL,
                ask_quote DOUBLE PRECISION NOT NULL,
                model_half_spread_bps DOUBLE PRECISION NOT NULL,
                applied_half_spread_bps DOUBLE PRECISION NOT NULL,
                cash_usdt DOUBLE PRECISION NOT NULL,
                equity_usdt DOUBLE PRECISION NOT NULL,
                net_pnl_usdt DOUBLE PRECISION NOT NULL,
                fees_usdt DOUBLE PRECISION NOT NULL,
                buy_fills INTEGER NOT NULL,
                sell_fills INTEGER NOT NULL,
                meta JSONB
            );
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_as_snapshots_time
            ON as_mm_snapshots(symbol, observed_at DESC);
            """
        )
        cur.execute(
            """
            INSERT INTO as_mm_state (
                symbol,engine_version,cash_usdt,inventory_base,fees_usdt,
                buy_fills,sell_fills,equity_usdt,net_pnl_usdt,meta
            ) VALUES (%s,%s,%s,0,0,0,0,%s,0,%s)
            ON CONFLICT (symbol) DO NOTHING;
            """,
            (SYMBOL, ENGINE, START_CASH, START_CASH, json.dumps({"paper_only": True})),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def load_state():
    conn = db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT cash_usdt,inventory_base,fees_usdt,buy_fills,sell_fills
            FROM as_mm_state WHERE symbol=%s;
            """,
            (SYMBOL,),
        )
        row = cur.fetchone()
        if not row:
            return START_CASH, 0.0, 0.0, 0, 0
        return float(row[0]), float(row[1]), float(row[2]), int(row[3]), int(row[4])
    finally:
        cur.close()
        conn.close()


def bps(a, b):
    if b <= 0:
        return 0.0
    return (a / b - 1.0) * 10000.0


class ASMarketMaker:
    def __init__(self):
        self.lock = threading.RLock()
        self.best_bid = None
        self.best_ask = None
        self.last_book_ts = None
        self.mid_history = deque(maxlen=max(VOL_WINDOW, 30))
        self.return_history = deque(maxlen=max(VOL_WINDOW, 30))
        self.cash, self.inventory, self.fees, self.buy_fills, self.sell_fills = load_state()

        self.bid_quote = None
        self.ask_quote = None
        self.quote_created_at = None
        self.bid_touched_qty = 0.0
        self.ask_touched_qty = 0.0
        self.last_quote_time = 0.0
        self.last_snapshot_time = 0.0

    def mid(self):
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2.0

    def update_book(self, bid, ask, ts_ms=None):
        if bid <= 0 or ask <= 0 or ask <= bid:
            return
        with self.lock:
            old_mid = self.mid()
            self.best_bid = float(bid)
            self.best_ask = float(ask)
            self.last_book_ts = ts_ms
            new_mid = self.mid()
            if old_mid and new_mid and old_mid > 0:
                r = math.log(new_mid / old_mid)
                if math.isfinite(r) and abs(r) < 0.10:
                    self.return_history.append(r)
            if new_mid:
                self.mid_history.append((time.time(), new_mid))

            now = time.time()
            if now - self.last_quote_time >= REQUOTE_SEC:
                self.requote(now)
            if now - self.last_snapshot_time >= SNAPSHOT_SEC:
                self.persist_snapshot(now)

    def sigma2_horizon(self):
        vals = list(self.return_history)
        if len(vals) < 20:
            return 0.0
        var_per_tick = statistics.pvariance(vals)
        # bookTicker is ~200ms for BTC; convert observed tick variance to horizon variance
        observed_sec = max(REQUOTE_SEC / max(1.0, REQUOTE_SEC / 0.2), 0.2)
        var_per_sec = var_per_tick / observed_sec
        return max(0.0, var_per_sec * HORIZON_SEC)

    def quote_model(self):
        mid = self.mid()
        if not mid:
            return None
        sigma2_h = self.sigma2_horizon()
        inv_notional = self.inventory * mid
        inv_norm = max(-1.0, min(1.0, inv_notional / MAX_INVENTORY_NOTIONAL))

        # Log-price AS form with rolling horizon.
        reservation_log_shift = -inv_norm * GAMMA * sigma2_h
        reservation = mid * math.exp(reservation_log_shift)

        model_total_log_spread = (
            GAMMA * sigma2_h
            + (2.0 / GAMMA) * math.log(1.0 + GAMMA / KAPPA)
        )
        model_half_log = max(0.0, model_total_log_spread / 2.0)
        model_half_bps = model_half_log * 10000.0

        # Fee-aware floor: a round trip needs at least 2*maker fee, plus a small buffer.
        fee_floor_half_bps = MAKER_FEE * 10000.0 + MIN_HALF_SPREAD_BPS
        applied_half_bps = max(model_half_bps, fee_floor_half_bps)
        applied_half_bps = min(applied_half_bps, MAX_HALF_SPREAD_BPS)
        half_log = applied_half_bps / 10000.0

        bid = reservation * math.exp(-half_log)
        ask = reservation * math.exp(half_log)

        # Never simulate a maker quote that crosses the current opposite touch.
        eps = mid * 1e-8
        bid = min(bid, self.best_ask - eps)
        ask = max(ask, self.best_bid + eps)
        if bid >= ask:
            bid = min(self.best_bid, mid * (1 - half_log))
            ask = max(self.best_ask, mid * (1 + half_log))

        # Inventory hard limits: disable the side that would increase excess inventory.
        if inv_notional >= MAX_INVENTORY_NOTIONAL:
            bid = None
        if inv_notional <= -MAX_INVENTORY_NOTIONAL:
            ask = None

        return {
            "mid": mid,
            "sigma2_h": sigma2_h,
            "inv_notional": inv_notional,
            "inv_norm": inv_norm,
            "reservation": reservation,
            "bid": bid,
            "ask": ask,
            "model_half_bps": model_half_bps,
            "applied_half_bps": applied_half_bps,
        }

    def requote(self, now):
        q = self.quote_model()
        if not q:
            return
        old_bid, old_ask = self.bid_quote, self.ask_quote
        new_bid, new_ask = q["bid"], q["ask"]

        def changed(old, new):
            if old is None or new is None:
                return old != new
            return abs(bps(new, old)) >= QUOTE_CHANGE_RESET_BPS

        if changed(old_bid, new_bid):
            self.bid_touched_qty = 0.0
        if changed(old_ask, new_ask):
            self.ask_touched_qty = 0.0
        self.bid_quote = new_bid
        self.ask_quote = new_ask
        self.quote_created_at = now
        self.last_quote_time = now

    def on_trade(self, price, qty, buyer_maker, trade_ts_ms=None):
        with self.lock:
            if not self.bid_quote and not self.ask_quote:
                return
            price = float(price)
            qty = float(qty)
            if qty <= 0:
                return

            # buyer_maker=True -> seller was taker -> resting bids can be hit.
            if buyer_maker and self.bid_quote is not None and price <= self.bid_quote:
                self.bid_touched_qty += qty
                self.maybe_fill("BUY", self.bid_quote, self.bid_touched_qty, trade_ts_ms)

            # buyer_maker=False -> buyer was taker -> resting asks can be lifted.
            if (not buyer_maker) and self.ask_quote is not None and price >= self.ask_quote:
                self.ask_touched_qty += qty
                self.maybe_fill("SELL", self.ask_quote, self.ask_touched_qty, trade_ts_ms)

    def maybe_fill(self, side, quote_price, touched_qty, trade_ts_ms):
        mid = self.mid()
        if not mid or quote_price <= 0:
            return
        order_qty = ORDER_NOTIONAL_USDT / quote_price
        required = order_qty * QUEUE_AHEAD_MULT
        if touched_qty < required:
            return

        inv_after = self.inventory + order_qty if side == "BUY" else self.inventory - order_qty
        if abs(inv_after * mid) > MAX_INVENTORY_NOTIONAL * 1.02:
            return

        notional = order_qty * quote_price
        fee = notional * MAKER_FEE
        if side == "BUY":
            self.cash -= notional + fee
            self.inventory += order_qty
            self.buy_fills += 1
            self.bid_touched_qty = 0.0
        else:
            self.cash += notional - fee
            self.inventory -= order_qty
            self.sell_fills += 1
            self.ask_touched_qty = 0.0
        self.fees += fee

        quote_age = time.time() - self.quote_created_at if self.quote_created_at else None
        self.persist_fill(side, quote_price, order_qty, fee, mid, quote_age, touched_qty, trade_ts_ms)
        self.persist_state(mid)
        self.requote(time.time())

    def equity(self, mid):
        return self.cash + self.inventory * mid

    def persist_fill(self, side, price, qty, fee, mid, quote_age, touched_qty, trade_ts_ms):
        conn = db()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO as_mm_fills (
                    engine_version,symbol,side,price,qty,notional_usdt,maker_fee_usdt,
                    inventory_after,cash_after,mid_at_fill,quote_age_sec,touched_qty,meta
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (
                    ENGINE,SYMBOL,side,price,qty,qty*price,fee,self.inventory,self.cash,mid,
                    quote_age,touched_qty,
                    json.dumps({"paper_only": True, "trade_ts_ms": trade_ts_ms, "queue_ahead_mult": QUEUE_AHEAD_MULT}),
                ),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        print(
            f"AS1 FILL {side} {SYMBOL} px={price:.4f} qty={qty:.8f} "
            f"inv={self.inventory:.8f} equity={self.equity(mid):.4f}",
            flush=True,
        )

    def persist_state(self, mid):
        eq = self.equity(mid)
        pnl = eq - START_CASH
        conn = db()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                UPDATE as_mm_state SET
                    engine_version=%s,updated_at=NOW(),cash_usdt=%s,inventory_base=%s,
                    fees_usdt=%s,buy_fills=%s,sell_fills=%s,last_mid=%s,
                    equity_usdt=%s,net_pnl_usdt=%s,meta=%s
                WHERE symbol=%s;
                """,
                (
                    ENGINE,self.cash,self.inventory,self.fees,self.buy_fills,self.sell_fills,
                    mid,eq,pnl,json.dumps({"paper_only": True}),SYMBOL,
                ),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    def persist_snapshot(self, now):
        q = self.quote_model()
        if not q or q["bid"] is None or q["ask"] is None:
            return
        mid = q["mid"]
        eq = self.equity(mid)
        pnl = eq - START_CASH
        market_spread_bps = bps(self.best_ask, self.best_bid)
        conn = db()
        cur = conn.cursor()
        try:
            cur.execute(
                """
                INSERT INTO as_mm_snapshots (
                    engine_version,symbol,best_bid,best_ask,mid,market_spread_bps,
                    sigma2_horizon,inventory_base,inventory_notional,inventory_norm,
                    reservation_price,bid_quote,ask_quote,model_half_spread_bps,
                    applied_half_spread_bps,cash_usdt,equity_usdt,net_pnl_usdt,
                    fees_usdt,buy_fills,sell_fills,meta
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
                """,
                (
                    ENGINE,SYMBOL,self.best_bid,self.best_ask,mid,market_spread_bps,
                    q["sigma2_h"],self.inventory,q["inv_notional"],q["inv_norm"],
                    q["reservation"],q["bid"],q["ask"],q["model_half_bps"],
                    q["applied_half_bps"],self.cash,eq,pnl,self.fees,self.buy_fills,self.sell_fills,
                    json.dumps({"paper_only": True, "book_ts_ms": self.last_book_ts}),
                ),
            )
            conn.commit()
        finally:
            cur.close()
            conn.close()
        self.persist_state(mid)
        self.last_snapshot_time = now



class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/health"):
            body = b"AS1 paper-only market-making service running\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        return


def start_health_server():
    port = int(os.environ.get("PORT", "10000"))
    server = ThreadingHTTPServer(("0.0.0.0", port), HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"AS1 health server listening on :{port}", flush=True)
    return server

def decode_message(message):
    if isinstance(message, bytes):
        try:
            return gzip.decompress(message).decode("utf-8")
        except Exception:
            try:
                return message.decode("utf-8")
            except Exception:
                return ""
    return str(message)


def main():
    if not PAPER_ONLY:
        raise RuntimeError("AS1 must remain paper-only in this research branch")
    init_database()
    start_health_server()
    mm = ASMarketMaker()

    def on_open(ws):
        for channel in (f"{SYMBOL}@bookTicker", f"{SYMBOL}@trade"):
            ws.send(json.dumps({"id": str(uuid.uuid4()), "reqType": "sub", "dataType": channel}))
        print(f"AS1 connected {SYMBOL}; paper-only", flush=True)

    def on_message(ws, message):
        text = decode_message(message)
        if not text:
            return
        if text == "Ping" or "ping" in text.lower() and text.strip().lower() == "ping":
            ws.send("Pong")
            return
        try:
            obj = json.loads(text)
        except Exception:
            return

        dtype = obj.get("dataType", "")
        data = obj.get("data")
        if dtype.endswith("@bookTicker") and isinstance(data, dict):
            try:
                mm.update_book(float(data["b"]), float(data["a"]), int(data.get("T") or 0))
            except Exception as exc:
                print("AS1 BOOK ERROR", exc, flush=True)
        elif dtype.endswith("@trade") and isinstance(data, list):
            for t in data:
                try:
                    mm.on_trade(float(t["p"]), float(t["q"]), bool(t["m"]), int(t.get("T") or 0))
                except Exception as exc:
                    print("AS1 TRADE ERROR", exc, flush=True)

    def on_error(ws, error):
        print("AS1 WS ERROR", error, flush=True)

    def on_close(ws, code, reason):
        print("AS1 WS CLOSED", code, reason, flush=True)

    while True:
        ws = websocket.WebSocketApp(
            WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        ws.run_forever(ping_interval=None)
        time.sleep(3)


if __name__ == "__main__":
    main()
