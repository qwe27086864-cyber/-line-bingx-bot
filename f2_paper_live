from __future__ import annotations

import csv
import json
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import r3_historical_adapter as r3

BASE_URL = "https://open-api.bingx.com"
ROOT = Path(os.environ.get("F2_PAPER_ROOT", "data/f2_paper"))
ROOT.mkdir(parents=True, exist_ok=True)
STATE_PATH = ROOT / "state.json"
TRADES_PATH = ROOT / "trades.csv"
EVENTS_PATH = ROOT / "events.csv"
STATUS_PATH = ROOT / "status.json"
ROOT_INDEX = Path("data/index.html")

THRESHOLD_6H = float(os.environ.get("F2_PAPER_RET6_MIN", "0.425"))
ROUND_TRIP_COST = float(os.environ.get("F2_PAPER_COST_PCT", "0.16"))
EXTRA_COST_04 = 0.04
EXTRA_COST_06 = 0.06
SIM_MARGIN_USDT = float(os.environ.get("F2_PAPER_MARGIN_USDT", "10"))
SIM_LEVERAGE = float(os.environ.get("F2_PAPER_LEVERAGE", "8"))
LOOP_SECONDS = float(os.environ.get("F2_PAPER_LOOP_SECONDS", "20"))
API_DELAY = float(os.environ.get("F2_PAPER_API_DELAY", "0.12"))

TP1, TP2, TP3, SL = 2.0, 4.0, 8.0, 2.5
W1, W2, W3 = 0.35, 0.40, 0.25

lock = threading.RLock()
state = {
    "started_at": None,
    "last_scan_at": None,
    "last_scan_key": None,
    "previous": {},
    "watches": [],
    "trades": [],
    "errors": [],
    "scan_count": 0,
    "api_calls": 0,
}


def now_utc():
    return datetime.now(timezone.utc)


def iso(dt):
    return dt.astimezone(timezone.utc).isoformat() if isinstance(dt, datetime) else dt


def parse_dt(value):
    if not value:
        return None
    x = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if x.tzinfo is None:
        x = x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)


def pct(a, b):
    return (float(a) / float(b) - 1.0) * 100.0 if b else 0.0


def safe_float(value, default=0.0):
    try:
        x = float(value)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def public_request(path, params=None, timeout=20):
    params = params or {}
    query = urllib.parse.urlencode(params)
    url = BASE_URL + path + (("?" + query) if query else "")
    req = urllib.request.Request(url, method="GET", headers={"User-Agent": "Mozilla/5.0"})
    with lock:
        state["api_calls"] = int(state.get("api_calls", 0)) + 1
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            obj = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"BingX HTTP {e.code}: {body[:500]}")
    if isinstance(obj, dict) and obj.get("code") not in (None, 0):
        raise RuntimeError(f"BingX API error: {obj}")
    return obj


def normalize_rows(data):
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("data", "tickers", "list"):
            if isinstance(data.get(key), list):
                return data[key]
        return [data]
    return []


def get_all_tickers():
    obj = public_request("/openApi/swap/v2/quote/ticker")
    data = obj.get("data", []) if isinstance(obj, dict) else []
    rows = data if isinstance(data, list) else [data]
    out = {}
    for item in rows:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper()
        if not symbol.endswith("-USDT"):
            continue
        last = safe_float(item.get("lastPrice", item.get("price", item.get("last"))), 0)
        if last <= 0:
            continue
        quote_volume = safe_float(
            item.get("quoteVolume", item.get("quoteVol", item.get("quoteVolume24h"))), 0
        )
        if quote_volume <= 0:
            base_volume = safe_float(item.get("volume", item.get("baseVolume")), 0)
            quote_volume = base_volume * last
        change = safe_float(
            item.get("priceChangePercent", item.get("changePercent", item.get("priceChangeRate"))),
            0,
        )
        open_price = safe_float(item.get("openPrice", item.get("open")), 0)
        if change == 0 and open_price > 0:
            change = pct(last, open_price)
        out[symbol] = {
            "last": last,
            "quote_volume": quote_volume,
            "change_24h": change,
        }
    return out


def get_book(symbol):
    obj = public_request("/openApi/swap/v2/quote/bookTicker", {"symbol": symbol})
    data = obj.get("data", {}) if isinstance(obj, dict) else {}
    if isinstance(data, list):
        data = data[0] if data else {}
    bid = safe_float(data.get("bidPrice", data.get("bid")), 0)
    ask = safe_float(data.get("askPrice", data.get("ask")), 0)
    if bid <= 0 or ask <= 0 or ask < bid:
        return None
    mid = (bid + ask) / 2.0
    return {
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "spread_pct": (ask - bid) / mid * 100.0 if mid else 0.0,
    }


def get_klines(symbol, interval, limit=100):
    obj = public_request(
        "/openApi/swap/v3/quote/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )
    data = obj.get("data", []) if isinstance(obj, dict) else []
    mins = {"1m": 1, "5m": 5, "15m": 15, "1h": 60}[interval]
    rows = []
    for item in data if isinstance(data, list) else []:
        try:
            if isinstance(item, list):
                ms = int(item[0])
                op, hi, lo, cl, vol = map(float, item[1:6])
            else:
                ms = int(item.get("time", item.get("openTime", 0)))
                op = float(item["open"])
                hi = float(item["high"])
                lo = float(item["low"])
                cl = float(item["close"])
                vol = float(item.get("volume", 0))
            t = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            rows.append({
                "t": t,
                "end": t + timedelta(minutes=mins),
                "symbol": symbol,
                "open": op,
                "high": hi,
                "low": lo,
                "close": cl,
                "volume": vol,
                "quote_volume": 0.0,
            })
        except Exception:
            continue
    rows.sort(key=lambda x: x["t"])
    return rows


def market_data(scan_t):
    c15 = {}
    c1h = {}
    for s in ("BTC-USDT", "ETH-USDT"):
        c15[s] = get_klines(s, "15m", 90)
        time.sleep(API_DELAY)
        c1h[s] = get_klines(s, "1h", 90)
        time.sleep(API_DELAY)
    ctx = r3.market_context(c15, c1h, scan_t)
    return ctx, c15, c1h


def directional_6h(reverse_side, c1h, scan_t):
    values = []
    for symbol in ("BTC-USDT", "ETH-USDT"):
        bars = r3.completed(c1h.get(symbol, []), scan_t, 20)
        closes = [x["close"] for x in bars]
        if len(closes) < 7:
            return None
        values.append(pct(closes[-1], closes[-7]))
    raw = sum(values) / len(values)
    return raw if reverse_side == "LONG" else -raw


def analyze_candidate(symbol, side, move, live, scan_t, ctx):
    c5 = {symbol: get_klines(symbol, "5m", 100)}
    time.sleep(API_DELAY)
    c15 = {symbol: get_klines(symbol, "15m", 100)}
    time.sleep(API_DELAY)
    c1h = {symbol: get_klines(symbol, "1h", 90)}
    time.sleep(API_DELAY)
    return r3.analyze_setup(symbol, side, move, live, scan_t, c5, c15, c1h, ctx)


def add_event(event, **kwargs):
    row = {
        "timestamp": iso(now_utc()),
        "event": event,
        **kwargs,
    }
    fields = [
        "timestamp", "event", "symbol", "original_side", "side", "reason",
        "reference_level", "price", "market_6h_directional_pct", "threshold",
        "entry_price", "spread_pct", "trade_id",
    ]
    exists = EVENTS_PATH.exists()
    with EVENTS_PATH.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if not exists:
            w.writeheader()
        w.writerow(row)
    print("F2_PAPER_EVENT " + json.dumps(row, separators=(",", ":"), default=str), flush=True)


def trade_id(symbol, when):
    return f"{when.strftime('%Y%m%d%H%M%S')}-{symbol.replace('-', '')}"


def enter_paper(symbol, original_side, reverse_side, momentum6h, signal_t):
    book = get_book(symbol)
    if not book:
        add_event(
            "ENTRY_SKIPPED", symbol=symbol, original_side=original_side, side=reverse_side,
            reason="bookTicker unavailable", market_6h_directional_pct=momentum6h,
            threshold=THRESHOLD_6H,
        )
        return None
    entry = book["ask"] if reverse_side == "LONG" else book["bid"]
    tid = trade_id(symbol, signal_t)
    trade = {
        "trade_id": tid,
        "symbol": symbol,
        "original_side": original_side,
        "side": reverse_side,
        "signal_time": iso(signal_t),
        "entry_time": iso(now_utc()),
        "entry_price": entry,
        "entry_bid": book["bid"],
        "entry_ask": book["ask"],
        "entry_spread_pct": book["spread_pct"],
        "market_6h_directional_pct": momentum6h,
        "threshold": THRESHOLD_6H,
        "tp1": r3.level(entry, reverse_side, TP1),
        "tp2": r3.level(entry, reverse_side, TP2),
        "tp3": r3.level(entry, reverse_side, TP3),
        "sl": r3.level(entry, reverse_side, -SL),
        "hit_tp1": False,
        "hit_tp2": False,
        "status": "OPEN",
        "last_bar_end": iso(signal_t),
        "exit_time": None,
        "exit_price": None,
        "exit_reason": None,
        "gross_return_pct": None,
        "net_return_pct": None,
        "net_return_cost04_pct": None,
        "net_return_cost06_pct": None,
        "sim_pnl_usdt": None,
    }
    with lock:
        state["trades"].append(trade)
        persist_state()
    add_event(
        "ENTER", symbol=symbol, original_side=original_side, side=reverse_side,
        reason="REFERENCE_BREAK + 6h momentum filter passed",
        market_6h_directional_pct=momentum6h, threshold=THRESHOLD_6H,
        entry_price=entry, spread_pct=book["spread_pct"], trade_id=tid,
    )
    return trade


def close_trade(trade, exit_price, reason, gross, exit_time=None):
    net = gross - ROUND_TRIP_COST
    trade["status"] = "CLOSED"
    trade["exit_time"] = iso(exit_time or now_utc())
    trade["exit_price"] = float(exit_price)
    trade["exit_reason"] = reason
    trade["gross_return_pct"] = round(float(gross), 6)
    trade["net_return_pct"] = round(float(net), 6)
    trade["net_return_cost04_pct"] = round(float(net - EXTRA_COST_04), 6)
    trade["net_return_cost06_pct"] = round(float(net - EXTRA_COST_06), 6)
    trade["sim_pnl_usdt"] = round(SIM_MARGIN_USDT * SIM_LEVERAGE * net / 100.0, 6)
    add_event(
        "EXIT", symbol=trade["symbol"], original_side=trade["original_side"],
        side=trade["side"], reason=reason, price=exit_price, trade_id=trade["trade_id"],
    )


def process_bar(trade, bar):
    if trade["status"] != "OPEN":
        return
    side = trade["side"]
    entry = float(trade["entry_price"])
    tp1, tp2, tp3 = float(trade["tp1"]), float(trade["tp2"]), float(trade["tp3"])
    h1, h2 = bool(trade["hit_tp1"]), bool(trade["hit_tp2"])
    entry_time = parse_dt(trade["entry_time"])
    age = (bar["end"] - entry_time).total_seconds() / 3600.0

    if (not h1) and age >= 4.0:
        gross = r3.side_return(side, entry, bar["open"])
        close_trade(trade, bar["open"], "TIMEOUT_EXIT", gross, bar["end"])
        return

    if h2:
        active_stop, stop_reason = tp1, "TP2_PROTECT_EXIT"
    elif h1:
        active_stop, stop_reason = entry, "BREAKEVEN_EXIT"
    else:
        active_stop, stop_reason = float(trade["sl"]), "SL_EXIT"

    if r3.stop_hit(side, bar, active_stop):
        gross = r3.gross_stop(side, entry, tp1, tp2, h1, h2, active_stop)
        close_trade(trade, active_stop, stop_reason, gross, bar["end"])
        return

    if (not h1) and r3.tp_hit(side, bar, tp1):
        trade["hit_tp1"] = True
        h1 = True
        if r3.stop_hit(side, bar, entry):
            gross = r3.gross_stop(side, entry, tp1, tp2, True, False, entry)
            close_trade(trade, entry, "BREAKEVEN_EXIT", gross, bar["end"])
            return

    if trade["status"] != "OPEN":
        return

    if (not h2) and r3.tp_hit(side, bar, tp2):
        trade["hit_tp1"] = True
        trade["hit_tp2"] = True
        h1 = h2 = True
        if r3.stop_hit(side, bar, tp1):
            gross = r3.gross_stop(side, entry, tp1, tp2, True, True, tp1)
            close_trade(trade, tp1, "TP2_PROTECT_EXIT", gross, bar["end"])
            return

    if trade["status"] != "OPEN":
        return

    if r3.tp_hit(side, bar, tp3):
        gross = W1 * TP1 + W2 * TP2 + W3 * TP3
        close_trade(trade, tp3, "TP3_EXIT", gross, bar["end"])


def manage_open_trades():
    open_trades = []
    with lock:
        open_trades = [t for t in state["trades"] if t.get("status") == "OPEN"]
    for trade in open_trades:
        try:
            symbol = trade["symbol"]
            bars = get_klines(symbol, "1m", 12)
            now = now_utc()
            completed = [b for b in bars if b["end"] <= now]
            last_end = parse_dt(trade.get("last_bar_end")) or parse_dt(trade["entry_time"])
            for bar in completed:
                if bar["end"] <= last_end:
                    continue
                if bar["end"] <= parse_dt(trade["entry_time"]):
                    continue
                process_bar(trade, bar)
                trade["last_bar_end"] = iso(bar["end"])
                last_end = bar["end"]
                if trade["status"] != "OPEN":
                    break
            with lock:
                persist_state()
        except Exception as e:
            record_error(f"manage {trade.get('symbol')}: {e}")
        time.sleep(API_DELAY)


def record_error(message):
    row = {"timestamp": iso(now_utc()), "message": str(message)[:800]}
    with lock:
        state["errors"] = (state.get("errors", []) + [row])[-20:]
        persist_state()
    print("F2_PAPER_ERROR " + json.dumps(row, separators=(",", ":")), flush=True)


def process_watch(w, current, scan_t, ctx, c1h_market):
    symbol = w["symbol"]
    side = w["side"]
    t = current.get(symbol)
    if not t:
        return
    price = float(t["last"])

    if w["stage"] == "BREAKOUT_PENDING":
        if scan_t >= parse_dt(w["expires_at"]):
            w["status"] = "EXPIRED"
            add_event("PENDING_EXPIRED", symbol=symbol, original_side=side,
                      reason="5m breakout confirmation not received within 15m")
            return
        c5 = get_klines(symbol, "5m", 100)
        time.sleep(API_DELAY)
        bars = r3.completed(c5, scan_t, 5)
        if not bars:
            return
        close = float(bars[-1]["close"])
        ref = float(w["reference_level"])
        confirmed = close > ref if side == "LONG" else close < ref
        if not confirmed:
            return
        checked = analyze_candidate(symbol, side, w["radar_move_pct"], price, scan_t, ctx)
        if not checked:
            return
        checked = r3.recheck_stored_breakout(checked, ref)
        if checked["decision"] != "WATCH":
            w["status"] = "REJECTED"
            add_event(checked["decision"], symbol=symbol, original_side=side,
                      reason=checked["reason"], reference_level=ref, price=price)
            return
        w.update({k: v for k, v in checked.items() if isinstance(v, (str, int, float, bool, type(None)))})
        w["stage"] = "WATCH"
        w["setup_price"] = price
        w["setup_extreme"] = price
        w["expires_at"] = iso(scan_t + timedelta(minutes=r3.SETUP_TTL_MIN))
        add_event("WATCH", symbol=symbol, original_side=side,
                  reason="stored breakout confirmed", reference_level=ref, price=price)
        return

    reference = float(w["reference_level"])
    extreme = float(w.get("setup_extreme") or price)
    if side == "LONG":
        extreme = max(extreme, price)
        pullback = (extreme - price) / extreme * 100.0
        reference_broken = price < reference * (1.0 - r3.REFERENCE_INVALIDATION_PCT / 100.0)
    else:
        extreme = min(extreme, price)
        pullback = (price - extreme) / extreme * 100.0
        reference_broken = price > reference * (1.0 + r3.REFERENCE_INVALIDATION_PCT / 100.0)
    w["setup_extreme"] = extreme

    if scan_t >= parse_dt(w["expires_at"]):
        w["status"] = "EXPIRED"
        add_event("EXPIRED", symbol=symbol, original_side=side, reason="setup expired after 75m",
                  reference_level=reference, price=price)
        return

    if reference_broken:
        w["status"] = "INVALIDATED"
        reverse_side = "SHORT" if side == "LONG" else "LONG"
        mom6 = directional_6h(reverse_side, c1h_market, scan_t)
        add_event("REFERENCE_BREAK", symbol=symbol, original_side=side, side=reverse_side,
                  reason="breakout reference invalidated", reference_level=reference, price=price,
                  market_6h_directional_pct=mom6, threshold=THRESHOLD_6H)
        if mom6 is not None and mom6 >= THRESHOLD_6H:
            active_same = any(
                t.get("status") == "OPEN" and t.get("symbol") == symbol and t.get("side") == reverse_side
                for t in state["trades"]
            )
            if not active_same:
                enter_paper(symbol, side, reverse_side, mom6, scan_t)
            else:
                add_event("ENTRY_SKIPPED", symbol=symbol, original_side=side, side=reverse_side,
                          reason="same symbol/side paper trade already open",
                          market_6h_directional_pct=mom6, threshold=THRESHOLD_6H)
        else:
            add_event("FILTERED", symbol=symbol, original_side=side, side=reverse_side,
                      reason="6h momentum below threshold",
                      market_6h_directional_pct=mom6, threshold=THRESHOLD_6H)
        return

    if pullback > r3.PULLBACK_MAX_PCT:
        w["status"] = "INVALIDATED_DEEP_PULLBACK"
        add_event("DEEP_PULLBACK_INVALIDATION", symbol=symbol, original_side=side,
                  reason=f"pullback {pullback:.3f}% > {r3.PULLBACK_MAX_PCT:.2f}%",
                  reference_level=reference, price=price)


def floor_5m(dt):
    return dt.replace(minute=dt.minute - dt.minute % 5, second=0, microsecond=0)


def run_scan(scan_t):
    print(f"F2_PAPER_SCAN_START {scan_t.isoformat()}", flush=True)
    current = get_all_tickers()
    if not current:
        raise RuntimeError("no BingX ticker rows")

    ctx, _, c1h_market = market_data(scan_t)

    with lock:
        active_watches = [w for w in state["watches"] if w.get("status") == "ACTIVE"]

    for w in active_watches:
        try:
            process_watch(w, current, scan_t, ctx, c1h_market)
        except Exception as e:
            record_error(f"watch {w.get('symbol')}: {e}")
        time.sleep(API_DELAY)

    with lock:
        previous = dict(state.get("previous") or {})

    candidates = []
    for symbol, t in current.items():
        old = previous.get(symbol)
        if not old:
            continue
        old_t = parse_dt(old.get("captured_at"))
        if not old_t:
            continue
        age = (scan_t - old_t).total_seconds() / 60.0
        if age < 2 or age > 20:
            continue
        if t["quote_volume"] < r3.MIN_QUOTE_VOLUME:
            continue
        move = pct(t["last"], old["last"])
        if abs(move) < r3.RADAR_MIN_MOVE:
            continue
        side = "LONG" if move > 0 else "SHORT"
        rank = min(abs(move), 5.0) * 12.0 + 8.0  # matches historical 0.12% spread rank
        if t["quote_volume"] >= 2_000_000:
            rank += 8.0
        if r3.directional(side, t["change_24h"]) > 0:
            rank += 5.0
        candidates.append((rank, abs(move), symbol, side, move, t["last"]))

    candidates.sort(reverse=True)
    active_keys = {(w.get("symbol"), w.get("side")) for w in active_watches if w.get("status") == "ACTIVE"}

    for _, __, symbol, side, move, live in candidates[:r3.RADAR_MAX_CANDIDATES][:r3.MAX_DEEP_ANALYSIS]:
        if (symbol, side) in active_keys:
            continue
        try:
            x = analyze_candidate(symbol, side, move, float(live), scan_t, ctx)
            if not x:
                continue
            clean = {k: v for k, v in x.items() if isinstance(v, (str, int, float, bool, type(None)))}
            if x["decision"] == "PENDING_BREAKOUT":
                clean.update({
                    "stage": "BREAKOUT_PENDING",
                    "status": "ACTIVE",
                    "expires_at": iso(scan_t + timedelta(minutes=r3.PENDING_TTL_MIN)),
                    "setup_extreme": x["setup_price"],
                })
                with lock:
                    state["watches"].append(clean)
                active_keys.add((symbol, side))
                add_event("PENDING_BREAKOUT", symbol=symbol, original_side=side,
                          reason=x["reason"], reference_level=x["reference_level"], price=live)
            elif x["decision"] == "WATCH":
                clean.update({
                    "stage": "WATCH",
                    "status": "ACTIVE",
                    "expires_at": iso(scan_t + timedelta(minutes=r3.SETUP_TTL_MIN)),
                    "setup_extreme": x["setup_price"],
                })
                with lock:
                    state["watches"].append(clean)
                active_keys.add((symbol, side))
                add_event("WATCH", symbol=symbol, original_side=side,
                          reason=x["reason"], reference_level=x["reference_level"], price=live)
        except Exception as e:
            record_error(f"candidate {symbol}: {e}")
        time.sleep(API_DELAY)

    snapshot = {
        s: {**t, "captured_at": iso(scan_t)}
        for s, t in current.items()
    }
    with lock:
        state["previous"] = snapshot
        state["last_scan_at"] = iso(scan_t)
        state["last_scan_key"] = scan_t.strftime("%Y-%m-%dT%H:%M")
        state["scan_count"] = int(state.get("scan_count", 0)) + 1
        state["watches"] = state["watches"][-500:]
        persist_state()
    print(f"F2_PAPER_SCAN_DONE symbols={len(current)} candidates={len(candidates)} active_watches={sum(w.get('status')=='ACTIVE' for w in state['watches'])}", flush=True)


def metrics(field="net_return_pct"):
    vals = [safe_float(t.get(field), 0) for t in state["trades"] if t.get("status") == "CLOSED" and t.get(field) is not None]
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    gp = sum(wins)
    gl = abs(sum(losses))
    pf = gp / gl if gl else (999.0 if gp else 0.0)
    eq = peak = dd = 0.0
    for x in vals:
        eq += x
        peak = max(peak, eq)
        dd = max(dd, peak - eq)
    return {
        "trades": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / len(vals), 2) if vals else 0.0,
        "profit_factor": round(pf, 4),
        "sum_net_return_pct": round(sum(vals), 4),
        "avg_net_return_pct": round(sum(vals) / len(vals), 4) if vals else 0.0,
        "max_drawdown_pct_points": round(dd, 4),
    }


def write_trades_csv():
    fields = [
        "trade_id", "symbol", "original_side", "side", "signal_time", "entry_time",
        "entry_price", "entry_bid", "entry_ask", "entry_spread_pct",
        "market_6h_directional_pct", "threshold", "exit_time", "exit_price",
        "exit_reason", "gross_return_pct", "net_return_pct",
        "net_return_cost04_pct", "net_return_cost06_pct", "sim_pnl_usdt", "status",
    ]
    with TRADES_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for row in state["trades"]:
            w.writerow(row)


def status_payload():
    active_watches = [w for w in state["watches"] if w.get("status") == "ACTIVE"]
    open_trades = [t for t in state["trades"] if t.get("status") == "OPEN"]
    closed = [t for t in state["trades"] if t.get("status") == "CLOSED"]
    return {
        "core": "F2-PAPER",
        "mode": "LIVE_FORWARD_PAPER_NO_ORDERS",
        "rule": {
            "trigger": "R3 REFERENCE_BREAK reverse",
            "btc_eth_6h_directional_min_pct": THRESHOLD_6H,
            "tp_pct": [TP1, TP2, TP3],
            "tp_weights": [W1, W2, W3],
            "sl_pct": SL,
            "timeout_hours_before_tp1": 4,
            "round_trip_cost_pct": ROUND_TRIP_COST,
        },
        "started_at": state.get("started_at"),
        "last_scan_at": state.get("last_scan_at"),
        "scan_count": state.get("scan_count", 0),
        "api_calls": state.get("api_calls", 0),
        "active_watches": len(active_watches),
        "open_trades": len(open_trades),
        "closed_trades": len(closed),
        "base": metrics("net_return_pct"),
        "extra_cost_004": metrics("net_return_cost04_pct"),
        "extra_cost_006": metrics("net_return_cost06_pct"),
        "recent_trades": state["trades"][-20:],
        "recent_watches": active_watches[-20:],
        "errors": state.get("errors", [])[-10:],
        "updated_at": iso(now_utc()),
    }


def write_dashboard():
    html = """<!doctype html><html><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>F2 PAPER</title><style>body{font-family:system-ui,-apple-system,sans-serif;max-width:980px;margin:28px auto;padding:0 16px;background:#0d1117;color:#e6edf3}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}.card{background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px}h1,h2{margin:.3em 0}table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;border-bottom:1px solid #30363d;padding:7px;white-space:nowrap}.ok{color:#3fb950}.muted{color:#8b949e}.scroll{overflow:auto}</style></head><body><h1>F2-PAPER · Live Forward Simulation</h1><p class='muted'>BingX public market data only · no order endpoint · threshold 6h ≥ 0.425%</p><div id='cards' class='grid'></div><h2>Recent trades</h2><div class='scroll'><table><thead><tr><th>Time</th><th>Symbol</th><th>Side</th><th>Status</th><th>6h</th><th>Net</th><th>Exit</th></tr></thead><tbody id='trades'></tbody></table></div><h2>Errors</h2><pre id='errors' class='muted'></pre><script>async function load(){try{const r=await fetch('f2_paper/status.json?'+Date.now());const s=await r.json();const b=s.base||{};document.getElementById('cards').innerHTML=[['Last scan',s.last_scan_at||'-'],['Watches',s.active_watches],['Open',s.open_trades],['Closed',s.closed_trades],['PF',b.profit_factor??0],['Win rate',(b.win_rate_pct??0)+'%'],['Net',(b.sum_net_return_pct??0)+'%'],['DD',(b.max_drawdown_pct_points??0)+'%']].map(x=>`<div class='card'><div class='muted'>${x[0]}</div><b>${x[1]}</b></div>`).join('');document.getElementById('trades').innerHTML=(s.recent_trades||[]).slice().reverse().map(t=>`<tr><td>${t.entry_time||''}</td><td>${t.symbol||''}</td><td>${t.side||''}</td><td>${t.status||''}</td><td>${Number(t.market_6h_directional_pct||0).toFixed(3)}%</td><td>${t.net_return_pct==null?'':Number(t.net_return_pct).toFixed(3)+'%'}</td><td>${t.exit_reason||''}</td></tr>`).join('');document.getElementById('errors').textContent=JSON.stringify(s.errors||[],null,2)}catch(e){document.getElementById('errors').textContent=String(e)}}load();setInterval(load,15000);</script></body></html>"""
    ROOT_INDEX.write_text(html, encoding="utf-8")


def persist_state():
    serial = dict(state)
    STATE_PATH.write_text(json.dumps(serial, indent=2, ensure_ascii=False), encoding="utf-8")
    write_trades_csv()
    STATUS_PATH.write_text(json.dumps(status_payload(), indent=2, ensure_ascii=False), encoding="utf-8")
    write_dashboard()


def load_state():
    if not STATE_PATH.exists():
        return
    try:
        obj = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        if isinstance(obj, dict):
            state.update(obj)
    except Exception as e:
        print(f"F2_PAPER_STATE_LOAD_ERROR {e}", flush=True)


def main():
    load_state()
    with lock:
        if not state.get("started_at"):
            state["started_at"] = iso(now_utc())
        persist_state()
    print(
        f"F2_PAPER_START threshold={THRESHOLD_6H} margin={SIM_MARGIN_USDT} leverage={SIM_LEVERAGE} no_orders=true",
        flush=True,
    )
    while True:
        try:
            manage_open_trades()
            now = now_utc()
            scan_t = floor_5m(now)
            key = scan_t.strftime("%Y-%m-%dT%H:%M")
            if state.get("last_scan_key") != key and now >= scan_t + timedelta(seconds=5):
                run_scan(scan_t)
            with lock:
                persist_state()
        except Exception as e:
            record_error(f"main loop: {e}")
        time.sleep(LOOP_SECONDS)


if __name__ == "__main__":
    main()
