from flask import Flask, request, abort
import os
import hmac
import hashlib
import base64
import re
import time
import json
import math
import threading
import urllib.request
import urllib.error
import urllib.parse
import psycopg2

app = Flask(__name__)

# =========================================================
# ENVIRONMENT
# =========================================================

LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
BINGX_API_KEY = os.environ.get("BINGX_API_KEY", "")
BINGX_SECRET_KEY = os.environ.get("BINGX_SECRET_KEY", "")
DATABASE_URL = os.environ.get("DATABASE_URL", "")

LIVE_TRADING = os.environ.get("LIVE_TRADING", "false").strip().lower() == "true"
ORDER_USDT = float(os.environ.get("ORDER_USDT", "10"))
MAX_LIVE_ORDER_USDT = float(os.environ.get("MAX_LIVE_ORDER_USDT", "10"))

TP1_PCT = float(os.environ.get("TP1_PCT", "30"))
TP2_PCT = float(os.environ.get("TP2_PCT", "40"))
TP3_PCT = float(os.environ.get("TP3_PCT", "30"))

LEVERAGE = int(os.environ.get("LEVERAGE", "8"))
BINGX_POSITION_MODE = os.environ.get("BINGX_POSITION_MODE", "HEDGE").strip().upper()
ENTRY_TIMEOUT_SEC = int(os.environ.get("ENTRY_TIMEOUT_SEC", "600"))
ORDER_POLL_SEC = float(os.environ.get("ORDER_POLL_SEC", "2"))
BINGX_BASE_URL = "https://open-api.bingx.com"

# =========================================================
# SCANNER V3 SETTINGS
# =========================================================

SCANNER_MIN_TREND_SCORE = int(os.environ.get("SCANNER_MIN_TREND_SCORE", "75"))
SCANNER_MIN_BREAKOUT_SCORE = int(os.environ.get("SCANNER_MIN_BREAKOUT_SCORE", "75"))
SCANNER_MAX_NEW_PER_SCAN = int(os.environ.get("SCANNER_MAX_NEW_PER_SCAN", "3"))
SCANNER_MAX_OPEN = int(os.environ.get("SCANNER_MAX_OPEN", "10"))
SCANNER_COOLDOWN_HOURS = int(os.environ.get("SCANNER_COOLDOWN_HOURS", "4"))

SCANNER_TP1 = float(os.environ.get("SCANNER_TP1", "2"))
SCANNER_TP2 = float(os.environ.get("SCANNER_TP2", "4"))
SCANNER_TP3 = float(os.environ.get("SCANNER_TP3", "8"))
SCANNER_SL = float(os.environ.get("SCANNER_SL", "3"))

SCANNER_API_DELAY = float(os.environ.get("SCANNER_API_DELAY", "1.05"))
SIM_MONITOR_SEC = int(os.environ.get("SIM_MONITOR_SEC", "30"))

# V3 simulated money/risk assumptions
SIM_START_BALANCE = float(os.environ.get("SIM_START_BALANCE", "1000"))
SIM_MARGIN_USDT = float(os.environ.get("SIM_MARGIN_USDT", "10"))
SIM_LEVERAGE = int(os.environ.get("SIM_LEVERAGE", str(LEVERAGE)))

# Estimated per-side trading friction on notional.
# 0.05% fee + 0.03% slippage per side by default.
SIM_FEE_PCT = float(os.environ.get("SIM_FEE_PCT", "0.05"))
SIM_SLIPPAGE_PCT = float(os.environ.get("SIM_SLIPPAGE_PCT", "0.03"))

# Simulation risk controls (V3 only)
SIM_DAILY_MAX_LOSS_USDT = float(os.environ.get("SIM_DAILY_MAX_LOSS_USDT", "20"))
SIM_MAX_CONSECUTIVE_LOSSES = int(os.environ.get("SIM_MAX_CONSECUTIVE_LOSSES", "4"))
SIM_PAUSE_HOURS_AFTER_STREAK = int(os.environ.get("SIM_PAUSE_HOURS_AFTER_STREAK", "6"))

# V3 TP/SL replay
SIM_REPLAY_1M_LIMIT = int(os.environ.get("SIM_REPLAY_1M_LIMIT", "240"))

# Optional web-service monitor. Keep false when Render Cron is the manager.
ENABLE_WEB_SIM_MONITOR = os.environ.get(
    "ENABLE_WEB_SIM_MONITOR", "false"
).strip().lower() == "true"

# Old full-scan route settings retained for compatibility
FAST_DETAIL_LIMIT = int(os.environ.get("FAST_DETAIL_LIMIT", "120"))
BREAKOUT_FORCE_SCORE = int(os.environ.get("BREAKOUT_FORCE_SCORE", "45"))
MAX_DETAIL_SYMBOLS = int(os.environ.get("MAX_DETAIL_SYMBOLS", "180"))
ONE_HOUR_CONFIRM_LIMIT = int(os.environ.get("ONE_HOUR_CONFIRM_LIMIT", "40"))

scanner_lock = threading.Lock()
scanner_status = {
    "running": False,
    "phase": "",
    "started_at": None,
    "finished_at": None,
    "total": 0,
    "processed": 0,
    "detail_total": 0,
    "detail_processed": 0,
    "trend_candidates": 0,
    "breakout_candidates": 0,
    "new_trades": 0,
    "current_symbol": "",
    "error": "",
    "top_results": [],
}


# =========================================================
# DATABASE
# =========================================================

def get_db_connection():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL not set")
    return psycopg2.connect(DATABASE_URL, sslmode="require")


def init_database():
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS scanner_trades (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(50) NOT NULL,
                side VARCHAR(10) NOT NULL DEFAULT 'LONG',
                strategy VARCHAR(30),
                engine_version VARCHAR(10),
                signal_time TIMESTAMPTZ DEFAULT NOW(),
                entry_price DOUBLE PRECISION NOT NULL,

                score INTEGER,
                trend_score INTEGER,
                breakout_score INTEGER,

                rsi DOUBLE PRECISION,
                rsi_5m DOUBLE PRECISION,
                volume_ratio DOUBLE PRECISION,
                momentum_pct DOUBLE PRECISION,
                momentum_5m DOUBLE PRECISION,
                volatility_pct DOUBLE PRECISION,
                extension_pct DOUBLE PRECISION,
                trend_15m VARCHAR(20),
                trend_1h VARCHAR(20),
                breakout BOOLEAN DEFAULT FALSE,
                reasons TEXT,

                tp1 DOUBLE PRECISION,
                tp2 DOUBLE PRECISION,
                tp3 DOUBLE PRECISION,
                sl DOUBLE PRECISION,

                status VARCHAR(30) DEFAULT 'OPEN',
                tp1_hit BOOLEAN DEFAULT FALSE,
                tp2_hit BOOLEAN DEFAULT FALSE,
                tp3_hit BOOLEAN DEFAULT FALSE,
                sl_hit BOOLEAN DEFAULT FALSE,

                highest_price DOUBLE PRECISION,
                lowest_price DOUBLE PRECISION,

                exit_price DOUBLE PRECISION,
                result_pct DOUBLE PRECISION,
                net_result_pct DOUBLE PRECISION,
                sim_pnl_usdt DOUBLE PRECISION,
                sim_margin_usdt DOUBLE PRECISION,
                sim_leverage INTEGER,

                exit_reason VARCHAR(30),
                exit_detail TEXT,
                last_checked_at TIMESTAMPTZ,

                closed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

        migrations = [
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS strategy VARCHAR(30);",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS engine_version VARCHAR(10);",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS trend_score INTEGER;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS breakout_score INTEGER;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS rsi_5m DOUBLE PRECISION;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS momentum_5m DOUBLE PRECISION;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS volatility_pct DOUBLE PRECISION;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS extension_pct DOUBLE PRECISION;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS momentum_pct DOUBLE PRECISION;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS reasons TEXT;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS exit_reason VARCHAR(30);",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS exit_detail TEXT;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS net_result_pct DOUBLE PRECISION;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS sim_pnl_usdt DOUBLE PRECISION;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS sim_margin_usdt DOUBLE PRECISION;",
            "ALTER TABLE scanner_trades ADD COLUMN IF NOT EXISTS sim_leverage INTEGER;",
        ]
        for sql in migrations:
            cur.execute(sql)

        # Preserve old records without mixing them into V3.
        cur.execute(
            """
            UPDATE scanner_trades
            SET strategy = 'LEGACY_V1'
            WHERE strategy IS NULL;
            """
        )
        cur.execute(
            """
            UPDATE scanner_trades
            SET engine_version = 'V1'
            WHERE strategy = 'LEGACY_V1'
              AND engine_version IS NULL;
            """
        )
        cur.execute(
            """
            UPDATE scanner_trades
            SET engine_version = 'V2'
            WHERE strategy IN ('TREND', 'BREAKOUT')
              AND engine_version IS NULL;
            """
        )

        conn.commit()
        print("DATABASE READY | V3 MIGRATIONS OK", flush=True)
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


# =========================================================
# BINGX REQUESTS
# =========================================================

def bingx_public_request(path, params=None):
    params = params or {}
    query = urllib.parse.urlencode(params)
    url = BINGX_BASE_URL + path
    if query:
        url += "?" + query

    req = urllib.request.Request(
        url,
        method="GET",
        headers={"User-Agent": "Mozilla/5.0"},
    )

    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"BingX Public HTTP {e.code}: {body}")


def build_canonical(params):
    return "&".join(f"{k}={v}" for k, v in sorted(params.items()))


def bingx_private_request(method, path, params=None):
    if not BINGX_API_KEY:
        raise RuntimeError("BINGX_API_KEY not set")
    if not BINGX_SECRET_KEY:
        raise RuntimeError("BINGX_SECRET_KEY not set")

    params = dict(params or {})
    params["recvWindow"] = 5000
    params["timestamp"] = int(time.time() * 1000)

    canonical = build_canonical(params)
    signature = hmac.new(
        BINGX_SECRET_KEY.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    signed = canonical + "&signature=" + signature
    headers = {
        "X-BX-APIKEY": BINGX_API_KEY,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    if method == "GET":
        url = BINGX_BASE_URL + path + "?" + signed
        data = None
    else:
        url = BINGX_BASE_URL + path
        data = signed.encode("utf-8")

    req = urllib.request.Request(url, headers=headers, data=data, method=method)

    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"BingX HTTP {e.code}: {body}")


# =========================================================
# LINE SIGNAL PARSER / LIVE MANUAL COPY-TRADE
# =========================================================

def parse_signal(text):
    result = {
        "symbol": None, "side": None, "entry": None,
        "tp1": None, "tp2": None, "tp3": None, "sl": None,
    }

    symbol = re.search(r"å¹£ç¨®\s*[:ï¼]\s*([A-Za-z0-9]+)", text)
    side = re.search(r"æ¹å\s*[:ï¼]\s*(å¤|ç©º|LONG|SHORT|Long|Short)", text)
    entry = re.search(r"é²å ´(?:å¹ä½)?\s*[:ï¼]\s*([0-9.]+)", text)
    tp1 = re.search(r"TP1\s*[:ï¼]\s*([0-9.]+)", text, re.I)
    tp2 = re.search(r"TP2\s*[:ï¼]\s*([0-9.]+)", text, re.I)
    tp3 = re.search(r"TP3\s*[:ï¼]\s*([0-9.]+)", text, re.I)
    sl = re.search(r"(?:SL|æ­¢æ)\s*[:ï¼]\s*([0-9.]+)", text, re.I)

    if symbol:
        result["symbol"] = symbol.group(1).upper()
    if side:
        s = side.group(1).upper()
        result["side"] = "LONG" if s in ["å¤", "LONG"] else "SHORT"
    if entry:
        result["entry"] = entry.group(1)
    if tp1:
        result["tp1"] = tp1.group(1)
    if tp2:
        result["tp2"] = tp2.group(1)
    if tp3:
        result["tp3"] = tp3.group(1)
    if sl:
        result["sl"] = sl.group(1)

    return result


def validate_signal(signal):
    for key in ["symbol", "side", "entry", "tp1", "tp2", "tp3", "sl"]:
        if not signal.get(key):
            return False, f"Missing field: {key}"

    try:
        prices = [
            float(signal["entry"]),
            float(signal["tp1"]),
            float(signal["tp2"]),
            float(signal["tp3"]),
            float(signal["sl"]),
        ]
    except ValueError:
        return False, "Invalid price"

    if min(prices) <= 0:
        return False, "Prices must be > 0"

    if abs(TP1_PCT + TP2_PCT + TP3_PCT - 100) > 0.0001:
        return False, "TP percentages must total 100"

    return True, None


def set_leverage(symbol, position_side):
    leverage_side = position_side if BINGX_POSITION_MODE == "HEDGE" else "BOTH"
    result = bingx_private_request(
        "POST",
        "/openApi/swap/v2/trade/leverage",
        {"symbol": symbol, "side": leverage_side, "leverage": LEVERAGE},
    )
    if result.get("code") != 0:
        raise RuntimeError(f"Set leverage failed: {result}")
    return result


def get_contract_info(symbol):
    result = bingx_public_request("/openApi/swap/v2/quote/contracts")
    if result.get("code") != 0:
        raise RuntimeError(f"Contract query failed: {result}")

    for item in result.get("data", []):
        if item.get("symbol") == symbol:
            return item

    raise RuntimeError(f"Contract not found: {symbol}")


def floor_number(value, precision):
    factor = 10 ** precision
    return math.floor(value * factor) / factor


def format_number(value, precision):
    return f"{value:.{precision}f}"


def client_id(event_id, suffix):
    raw = str(event_id) + "|" + str(suffix)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return "line" + digest[:24] + suffix


def build_limit_entry(signal, event_id):
    symbol = signal["symbol"] + "-USDT"
    contract = get_contract_info(symbol)

    quantity_precision = int(contract.get("quantityPrecision", 0))
    price_precision = int(contract.get("pricePrecision", 8))
    min_qty = float(contract.get("tradeMinQuantity", 0))
    min_usdt = float(contract.get("tradeMinUSDT", 0))

    entry_price = floor_number(float(signal["entry"]), price_precision)
    if entry_price <= 0:
        raise RuntimeError("Invalid entry price")

    quantity = floor_number(ORDER_USDT / entry_price, quantity_precision)
    if quantity <= 0:
        raise RuntimeError("Entry quantity became zero")
    if min_qty > 0 and quantity < min_qty:
        raise RuntimeError(f"Entry quantity {quantity} below minimum {min_qty}")

    notional = quantity * entry_price
    if min_usdt > 0 and notional < min_usdt:
        raise RuntimeError(f"Entry value {notional} USDT below minimum {min_usdt}")

    if LIVE_TRADING and ORDER_USDT > MAX_LIVE_ORDER_USDT:
        raise RuntimeError(
            f"LIVE ORDER BLOCKED: ORDER_USDT={ORDER_USDT} > MAX={MAX_LIVE_ORDER_USDT}"
        )

    side = "BUY" if signal["side"] == "LONG" else "SELL"
    position_side = signal["side"] if BINGX_POSITION_MODE == "HEDGE" else "BOTH"

    params = {
        "symbol": symbol,
        "side": side,
        "positionSide": position_side,
        "type": "LIMIT",
        "quantity": format_number(quantity, quantity_precision),
        "price": format_number(entry_price, price_precision),
        "timeInForce": "GTC",
        "clientOrderId": client_id(event_id, "entry"),
    }
    return params, contract, quantity


def place_limit_entry(signal, event_id):
    params, contract, quantity = build_limit_entry(signal, event_id)
    symbol = params["symbol"]

    if LIVE_TRADING:
        set_leverage(symbol, signal["side"])
    else:
        print("TEST MODE LEVERAGE:", f"{LEVERAGE}x", signal["side"], flush=True)

    path = (
        "/openApi/swap/v2/trade/order"
        if LIVE_TRADING
        else "/openApi/swap/v2/trade/order/test"
    )
    result = bingx_private_request("POST", path, params)
    return result, params, contract, quantity


def query_order(symbol, order_id=None, client_order_id=None):
    params = {"symbol": symbol}
    if order_id:
        params["orderId"] = order_id
    elif client_order_id:
        params["clientOrderId"] = client_order_id
    else:
        raise RuntimeError("Missing order ID")

    return bingx_private_request("GET", "/openApi/swap/v2/trade/order", params)


def cancel_order(symbol, order_id=None, client_order_id=None):
    params = {"symbol": symbol}
    if order_id:
        params["orderId"] = order_id
    else:
        params["clientOrderId"] = client_order_id

    return bingx_private_request("DELETE", "/openApi/swap/v2/trade/order", params)


def build_exit_order(
    symbol, position_side, quantity, stop_price, order_type,
    event_id, suffix, quantity_precision, price_precision
):
    close_side = "SELL" if position_side == "LONG" else "BUY"
    params = {
        "symbol": symbol,
        "side": close_side,
        "positionSide": position_side if BINGX_POSITION_MODE == "HEDGE" else "BOTH",
        "type": order_type,
        "quantity": format_number(quantity, quantity_precision),
        "stopPrice": format_number(stop_price, price_precision),
        "workingType": "MARK_PRICE",
    }
    if BINGX_POSITION_MODE == "ONEWAY":
        params["reduceOnly"] = "true"
    return params


def place_tp_sl(signal, event_id, filled_qty, contract):
    symbol = signal["symbol"] + "-USDT"
    quantity_precision = int(contract.get("quantityPrecision", 0))
    price_precision = int(contract.get("pricePrecision", 8))
    min_qty = float(contract.get("tradeMinQuantity", 0))
    min_usdt = float(contract.get("tradeMinUSDT", 0))

    q1 = floor_number(filled_qty * TP1_PCT / 100, quantity_precision)
    q2 = floor_number(filled_qty * TP2_PCT / 100, quantity_precision)
    q3 = floor_number(filled_qty - q1 - q2, quantity_precision)

    checks = [
        ("TP1", q1, float(signal["tp1"])),
        ("TP2", q2, float(signal["tp2"])),
        ("TP3", q3, float(signal["tp3"])),
    ]

    for name, qty, trigger_price in checks:
        if qty <= 0:
            raise RuntimeError(f"{name} qty became zero")
        if min_qty > 0 and qty < min_qty:
            raise RuntimeError(f"{name} qty {qty} below minimum {min_qty}")
        if min_usdt > 0 and qty * trigger_price < min_usdt:
            raise RuntimeError(f"{name} value below minimum {min_usdt}")

    tp_results = []
    for name, qty, trigger_price in checks:
        params = build_exit_order(
            symbol, signal["side"], qty, trigger_price,
            "TAKE_PROFIT_MARKET", event_id, name.lower(),
            quantity_precision, price_precision,
        )
        result = bingx_private_request("POST", "/openApi/swap/v2/trade/order", params)
        if result.get("code") != 0:
            raise RuntimeError(f"{name} failed: {result}")
        tp_results.append(result)

    sl_params = build_exit_order(
        symbol, signal["side"], filled_qty, float(signal["sl"]),
        "STOP_MARKET", event_id, "sl",
        quantity_precision, price_precision,
    )
    sl_result = bingx_private_request("POST", "/openApi/swap/v2/trade/order", sl_params)
    if sl_result.get("code") != 0:
        raise RuntimeError(f"SL failed: {sl_result}")

    return {"tp": tp_results, "sl": sl_result}


def monitor_limit_order(signal, event_id, order_id, client_order_id, contract):
    symbol = signal["symbol"] + "-USDT"
    start = time.time()

    while True:
        try:
            result = query_order(symbol, order_id, client_order_id)
            if result.get("code") != 0:
                raise RuntimeError(str(result))

            order = result.get("data", {})
            if isinstance(order, dict) and "order" in order:
                order = order["order"]

            status = str(order.get("status", "")).upper()
            executed_qty = float(order.get("executedQty", 0) or 0)

            if status == "FILLED":
                if executed_qty <= 0:
                    raise RuntimeError("FILLED but executedQty=0")
                place_tp_sl(signal, event_id, executed_qty, contract)
                return

            if status in ["CANCELED", "EXPIRED"]:
                return

            if time.time() - start >= ENTRY_TIMEOUT_SEC:
                cancel_order(symbol, order_id, client_order_id)
                return

        except Exception as e:
            print("MONITOR ERROR:", str(e), flush=True)

        time.sleep(ORDER_POLL_SEC)


# =========================================================
# INDICATORS / KLINES
# =========================================================

def calculate_ema(values, period):
    if len(values) < period:
        return None

    ema_value = sum(values[:period]) / period
    multiplier = 2 / (period + 1)

    for price in values[period:]:
        ema_value = (price - ema_value) * multiplier + ema_value

    return ema_value


def calculate_rsi(closes, period=14):
    if len(closes) < period + 1:
        return None

    gains = []
    losses = []

    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        if change > 0:
            gains.append(change)
            losses.append(0)
        else:
            gains.append(0)
            losses.append(abs(change))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        return 100.0

    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def get_klines(symbol, interval, limit=100):
    result = bingx_public_request(
        "/openApi/swap/v3/quote/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )

    if result.get("code") != 0:
        raise RuntimeError(f"Kline failed: {result}")

    candles = []
    for item in result.get("data", []):
        try:
            if isinstance(item, list):
                candle = {
                    "time": int(item[0]),
                    "open": float(item[1]),
                    "high": float(item[2]),
                    "low": float(item[3]),
                    "close": float(item[4]),
                    "volume": float(item[5]),
                }
            else:
                candle = {
                    "time": int(item.get("time", item.get("openTime", 0))),
                    "open": float(item["open"]),
                    "high": float(item["high"]),
                    "low": float(item["low"]),
                    "close": float(item["close"]),
                    "volume": float(item["volume"]),
                }
            candles.append(candle)
        except Exception:
            continue

    candles.sort(key=lambda x: x["time"])
    return candles


# =========================================================
# SCANNER ENTRY ANALYSIS
# =========================================================

def valid_scanner_symbol(symbol):
    symbol = str(symbol).upper()
    match = re.fullmatch(r"([A-Z0-9]{1,24})-USDT", symbol)
    if not match:
        return False
    return not match.group(1).endswith("USDT")


def get_all_usdt_contracts():
    result = bingx_public_request("/openApi/swap/v2/quote/contracts")
    if result.get("code") != 0:
        raise RuntimeError(f"Contract list failed: {result}")

    symbols = []
    for item in result.get("data", []):
        symbol = str(item.get("symbol", "")).upper()
        if valid_scanner_symbol(symbol):
            symbols.append(symbol)

    return sorted(set(symbols))


def analyze_fast_5m(symbol):
    candles = get_klines(symbol, "5m", 90)
    if len(candles) < 60:
        return None

    completed = candles[:-1]
    if len(completed) < 55:
        return None

    closes = [x["close"] for x in completed]
    highs = [x["high"] for x in completed]
    lows = [x["low"] for x in completed]
    volumes = [x["volume"] for x in completed]

    price = closes[-1]
    ema9 = calculate_ema(closes, 9)
    ema20 = calculate_ema(closes, 20)
    rsi = calculate_rsi(closes, 14)

    if ema9 is None or ema20 is None or rsi is None:
        return None

    momentum_15m = (price - closes[-4]) / closes[-4] * 100
    momentum_1h = (price - closes[-13]) / closes[-13] * 100

    previous_volumes = volumes[-21:-1]
    avg_volume = sum(previous_volumes) / len(previous_volumes) if previous_volumes else 0
    volume_ratio = volumes[-1] / avg_volume if avg_volume > 0 else 0

    previous_high = max(highs[-21:-1])
    breakout = price > previous_high

    recent_high = max(highs[-6:])
    recent_low = min(lows[-6:])
    volatility_pct = (recent_high - recent_low) / price * 100
    extension_pct = (price - ema20) / ema20 * 100

    breakout_score = 0
    breakout_reasons = []

    if volume_ratio >= 1.5:
        breakout_score += 15
    if volume_ratio >= 2.0:
        breakout_score += 15
        breakout_reasons.append(f"5méè½{volume_ratio:.2f}x")
    if volume_ratio >= 3.0:
        breakout_score += 10

    if momentum_15m >= 0.8:
        breakout_score += 10
    if momentum_15m >= 1.5:
        breakout_score += 15
        breakout_reasons.append(f"15åé+{momentum_15m:.2f}%")
    if momentum_15m >= 3.0:
        breakout_score += 10

    if breakout:
        breakout_score += 20
        breakout_reasons.append("5mçªç ´20æ ¹é«é»")

    if 55 <= rsi <= 85:
        breakout_score += 10
    if volatility_pct >= 2:
        breakout_score += 5
    if volatility_pct >= 4:
        breakout_score += 5
        breakout_reasons.append(f"æ³¢åæ´å¼µ{volatility_pct:.2f}%")
    if extension_pct > 10:
        breakout_score -= 15
    if rsi > 92:
        breakout_score -= 20

    trend_seed = 0
    trend_reasons = []

    if price > ema9:
        trend_seed += 10
    if ema9 > ema20:
        trend_seed += 15
        trend_reasons.append("5m EMA9>EMA20")
    if momentum_1h > 0.5:
        trend_seed += 10
    if 50 <= rsi <= 75:
        trend_seed += 10
    if volume_ratio >= 1.2:
        trend_seed += 5

    return {
        "symbol": symbol,
        "price": price,
        "rsi_5m": rsi,
        "volume_ratio_5m": volume_ratio,
        "momentum_5m": momentum_15m,
        "momentum_1h_fast": momentum_1h,
        "volatility_pct": volatility_pct,
        "extension_pct": extension_pct,
        "breakout_5m": breakout,
        "breakout_score_fast": breakout_score,
        "trend_seed": trend_seed,
        "fast_score": max(trend_seed, breakout_score),
        "breakout_reasons": breakout_reasons,
        "trend_reasons": trend_reasons,
    }


def analyze_15m_detail(fast):
    symbol = fast["symbol"]
    candles = get_klines(symbol, "15m", 100)
    if len(candles) < 60:
        return None

    completed = candles[:-1]
    closes = [x["close"] for x in completed]
    highs = [x["high"] for x in completed]
    volumes = [x["volume"] for x in completed]

    price = closes[-1]
    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)

    if ema20 is None or ema50 is None or rsi is None:
        return None

    momentum = (price - closes[-6]) / closes[-6] * 100
    previous_volumes = volumes[-21:-1]
    avg_volume = sum(previous_volumes) / len(previous_volumes) if previous_volumes else 0
    volume_ratio = volumes[-1] / avg_volume if avg_volume > 0 else 0

    previous_high = max(highs[-21:-1])
    breakout_15m = price > previous_high
    extension_15m = (price - ema20) / ema20 * 100

    trend_score = 0
    trend_reasons = list(fast["trend_reasons"])

    if price > ema20:
        trend_score += 15
        trend_reasons.append("15må¹æ ¼>EMA20")
    if ema20 > ema50:
        trend_score += 20
        trend_reasons.append("15m EMA20>EMA50")
    if 50 <= rsi <= 68:
        trend_score += 15
        trend_reasons.append(f"15m RSI={rsi:.1f}")
    elif 68 < rsi < 80:
        trend_score += 5
    if momentum > 0.3:
        trend_score += 5
    if momentum > 1:
        trend_score += 10
        trend_reasons.append(f"15måè½+{momentum:.2f}%")
    if volume_ratio >= 1.3:
        trend_score += 10
    if volume_ratio >= 1.8:
        trend_score += 5
        trend_reasons.append(f"15méè½{volume_ratio:.2f}x")
    if breakout_15m:
        trend_score += 15
        trend_reasons.append("15mçªç ´")

    trend_blocked = False
    if rsi >= 80:
        trend_blocked = True
        trend_reasons.append("è¶¨å¢ç­ç¥é»æ: RSI>=80")
    if extension_15m >= 10:
        trend_blocked = True
        trend_reasons.append("è¶¨å¢ç­ç¥é»æ: ä¹é¢éå¤§")

    breakout_score = int(fast["breakout_score_fast"])
    breakout_reasons = list(fast["breakout_reasons"])

    if volume_ratio >= 1.5:
        breakout_score += 10
    if breakout_15m:
        breakout_score += 10
        breakout_reasons.append("15måæ­¥çªç ´")
    if momentum > 1:
        breakout_score += 5
    if price > ema20:
        breakout_score += 5

    breakout_blocked = False
    if fast["rsi_5m"] > 90:
        breakout_blocked = True
        breakout_reasons.append("çç¼ç­ç¥é»æ: 5m RSI>90")
    if fast["extension_pct"] > 9:
        breakout_blocked = True
        breakout_reasons.append("çç¼ç­ç¥é»æ: 5mä¹é¢éå¤§")
    if fast["volume_ratio_5m"] < 1.8:
        breakout_blocked = True
    if fast["momentum_5m"] < 1.0:
        breakout_blocked = True
    if not fast["breakout_5m"]:
        breakout_blocked = True

    result = dict(fast)
    result.update({
        "price": price,
        "rsi": rsi,
        "volume_ratio": volume_ratio,
        "momentum_pct": momentum,
        "breakout": fast["breakout_5m"] or breakout_15m,
        "trend_15m": "UP" if (price > ema20 and ema20 > ema50) else "DOWN",
        "trend_score": trend_score,
        "breakout_score": breakout_score,
        "trend_blocked": trend_blocked,
        "breakout_blocked": breakout_blocked,
        "trend_reasons_final": trend_reasons,
        "breakout_reasons_final": breakout_reasons,
        "extension_15m": extension_15m,
    })
    return result


def confirm_1h(result):
    candles = get_klines(result["symbol"], "1h", 100)
    if len(candles) < 60:
        result["trend_1h"] = "UNKNOWN"
        return result

    closes = [x["close"] for x in candles[:-1]]
    price = closes[-1]
    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)

    if ema20 is None or ema50 is None:
        result["trend_1h"] = "UNKNOWN"
        return result

    if price > ema20 and ema20 > ema50:
        result["trend_1h"] = "UP"
        result["trend_score"] += 15
        result["trend_reasons_final"].append("1hå¤é ­ç¢ºèª")
        result["breakout_score"] += 5
    elif price > ema20:
        result["trend_1h"] = "WEAK_UP"
        result["trend_score"] += 5
    else:
        result["trend_1h"] = "DOWN"
        result["trend_score"] -= 10

    return result


def choose_strategy(result):
    trend_ok = (
        not result.get("trend_blocked", True)
        and result["trend_score"] >= SCANNER_MIN_TREND_SCORE
        and result.get("trend_1h", "UNKNOWN") in ["UP", "WEAK_UP"]
    )

    breakout_ok = (
        not result.get("breakout_blocked", True)
        and result["breakout_score"] >= SCANNER_MIN_BREAKOUT_SCORE
    )

    if trend_ok and breakout_ok:
        strategy = "BREAKOUT" if result["breakout_score"] > result["trend_score"] else "TREND"
    elif trend_ok:
        strategy = "TREND"
    elif breakout_ok:
        strategy = "BREAKOUT"
    else:
        return None

    result["strategy"] = strategy
    if strategy == "TREND":
        result["score"] = result["trend_score"]
        result["reasons"] = result["trend_reasons_final"]
    else:
        result["score"] = result["breakout_score"]
        result["reasons"] = result["breakout_reasons_final"]

    return result


# =========================================================
# SCANNER DB / RISK
# =========================================================

def scanner_open_count():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM scanner_trades
        WHERE status = 'OPEN'
          AND engine_version = 'V3'
          AND strategy IN ('TREND', 'BREAKOUT');
        """
    )
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


def scanner_can_open(symbol):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM scanner_trades
        WHERE symbol = %s
          AND engine_version = 'V3'
          AND strategy IN ('TREND', 'BREAKOUT')
          AND (
              status = 'OPEN'
              OR signal_time > NOW() - (%s * INTERVAL '1 hour')
          )
        ORDER BY signal_time DESC
        LIMIT 1;
        """,
        (symbol, SCANNER_COOLDOWN_HOURS),
    )
    found = cur.fetchone()
    cur.close()
    conn.close()
    return found is None


def get_v3_closed_pnl():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(sim_pnl_usdt), 0)
        FROM scanner_trades
        WHERE engine_version = 'V3'
          AND status IN ('WIN', 'LOSS')
          AND sim_pnl_usdt IS NOT NULL;
        """
    )
    pnl = float(cur.fetchone()[0] or 0)
    cur.close()
    conn.close()
    return pnl


def scanner_risk_allows_new_trade():
    conn = get_db_connection()
    cur = conn.cursor()

    # Daily realized PnL (UTC; intentionally deterministic for the backend).
    cur.execute(
        """
        SELECT COALESCE(SUM(sim_pnl_usdt), 0)
        FROM scanner_trades
        WHERE engine_version = 'V3'
          AND status IN ('WIN', 'LOSS')
          AND closed_at >= DATE_TRUNC('day', NOW());
        """
    )
    daily_pnl = float(cur.fetchone()[0] or 0)

    if daily_pnl <= -abs(SIM_DAILY_MAX_LOSS_USDT):
        cur.close()
        conn.close()
        return False, f"DAILY_LOSS_LIMIT {daily_pnl:.2f}U"

    # Consecutive losses from most recent closed trades.
    cur.execute(
        """
        SELECT status, closed_at
        FROM scanner_trades
        WHERE engine_version = 'V3'
          AND status IN ('WIN', 'LOSS')
        ORDER BY closed_at DESC, id DESC
        LIMIT %s;
        """,
        (max(SIM_MAX_CONSECUTIVE_LOSSES, 1),),
    )
    rows = cur.fetchall()

    streak = 0
    most_recent_loss_time = None
    for status, closed_at in rows:
        if status == "LOSS":
            streak += 1
            if most_recent_loss_time is None:
                most_recent_loss_time = closed_at
        else:
            break

    if streak >= SIM_MAX_CONSECUTIVE_LOSSES and most_recent_loss_time is not None:
        cur.execute(
            "SELECT NOW() - %s;",
            (most_recent_loss_time,),
        )
        elapsed = cur.fetchone()[0]
        if elapsed.total_seconds() < SIM_PAUSE_HOURS_AFTER_STREAK * 3600:
            cur.close()
            conn.close()
            return False, f"LOSS_STREAK_PAUSE {streak}"

    cur.close()
    conn.close()

    balance = SIM_START_BALANCE + get_v3_closed_pnl()
    reserved = scanner_open_count() * SIM_MARGIN_USDT
    free_balance = balance - reserved

    if free_balance < SIM_MARGIN_USDT:
        return False, f"INSUFFICIENT_SIM_BALANCE {free_balance:.2f}U"

    return True, "OK"


def create_simulated_trade(result):
    entry = float(result["price"])
    tp1 = entry * (1 + SCANNER_TP1 / 100)
    tp2 = entry * (1 + SCANNER_TP2 / 100)
    tp3 = entry * (1 + SCANNER_TP3 / 100)
    sl = entry * (1 - SCANNER_SL / 100)

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO scanner_trades (
            symbol, side, strategy, engine_version,
            entry_price,

            score, trend_score, breakout_score,
            rsi, rsi_5m,
            volume_ratio, momentum_pct, momentum_5m,
            volatility_pct, extension_pct,
            trend_15m, trend_1h,
            breakout, reasons,

            tp1, tp2, tp3, sl,
            highest_price, lowest_price,

            sim_margin_usdt, sim_leverage,
            last_checked_at
        )
        VALUES (
            %s, 'LONG', %s, 'V3',
            %s,

            %s, %s, %s,
            %s, %s,
            %s, %s, %s,
            %s, %s,
            %s, %s,
            %s, %s,

            %s, %s, %s, %s,
            %s, %s,

            %s, %s,
            NOW()
        )
        RETURNING id;
        """,
        (
            result["symbol"], result["strategy"], entry,

            result["score"], result["trend_score"], result["breakout_score"],
            result["rsi"], result["rsi_5m"],
            result["volume_ratio"], result["momentum_pct"], result["momentum_5m"],
            result["volatility_pct"], result["extension_pct"],
            result["trend_15m"], result.get("trend_1h", "UNKNOWN"),
            result["breakout"], json.dumps(result["reasons"], ensure_ascii=False),

            tp1, tp2, tp3, sl,
            entry, entry,

            SIM_MARGIN_USDT, SIM_LEVERAGE,
        )
    )

    trade_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()

    print(
        "SIMULATED V3 TRADE:",
        trade_id, result["strategy"], result["symbol"], result["score"],
        flush=True,
    )
    return trade_id


# =========================================================
# PRICE / RESULT HELPERS
# =========================================================

def get_all_prices():
    result = bingx_public_request("/openApi/swap/v1/ticker/price")
    if result.get("code") != 0:
        raise RuntimeError(f"Price query failed: {result}")

    data = result.get("data", [])
    if isinstance(data, dict):
        data = [data]

    prices = {}
    for item in data:
        try:
            prices[item["symbol"]] = float(item["price"])
        except Exception:
            continue
    return prices


def estimated_round_trip_cost_pct():
    return 2.0 * (SIM_FEE_PCT + SIM_SLIPPAGE_PCT)


def calculate_net_result(gross_result_pct, margin_usdt=None, leverage=None):
    margin_usdt = float(margin_usdt or SIM_MARGIN_USDT)
    leverage = int(leverage or SIM_LEVERAGE)

    net_result_pct = gross_result_pct - estimated_round_trip_cost_pct()
    pnl_usdt = margin_usdt * leverage * net_result_pct / 100.0
    return net_result_pct, pnl_usdt


def gross_result_for_stop(tp1_hit, tp2_hit, stop_return_pct):
    realized = 0.0
    remaining = 1.0

    if tp1_hit:
        realized += (TP1_PCT / 100) * SCANNER_TP1
        remaining -= TP1_PCT / 100

    if tp2_hit:
        realized += (TP2_PCT / 100) * SCANNER_TP2
        remaining -= TP2_PCT / 100

    return realized + remaining * stop_return_pct


def full_tp3_gross_result():
    return (
        (TP1_PCT / 100) * SCANNER_TP1
        + (TP2_PCT / 100) * SCANNER_TP2
        + (TP3_PCT / 100) * SCANNER_TP3
    )


def finalize_v3_trade(
    cur, trade_id, exit_price, gross_result_pct, exit_reason,
    tp1_hit, tp2_hit, tp3_hit, sl_hit,
    highest_price, lowest_price, margin_usdt, leverage,
    exit_detail=""
):
    net_result_pct, pnl_usdt = calculate_net_result(
        gross_result_pct, margin_usdt, leverage
    )
    status = "WIN" if net_result_pct > 0 else "LOSS"

    cur.execute(
        """
        UPDATE scanner_trades
        SET
            tp1_hit = %s,
            tp2_hit = %s,
            tp3_hit = %s,
            sl_hit = %s,

            highest_price = %s,
            lowest_price = %s,

            exit_price = %s,
            result_pct = %s,
            net_result_pct = %s,
            sim_pnl_usdt = %s,

            status = %s,
            exit_reason = %s,
            exit_detail = %s,

            last_checked_at = NOW(),
            closed_at = NOW()
        WHERE id = %s
          AND status = 'OPEN';
        """,
        (
            tp1_hit, tp2_hit, tp3_hit, sl_hit,
            highest_price, lowest_price,
            exit_price, gross_result_pct, net_result_pct, pnl_usdt,
            status, exit_reason, exit_detail,
            trade_id,
        ),
    )

    print(
        "V3 CLOSED:",
        trade_id,
        exit_reason,
        f"gross={gross_result_pct:.2f}%",
        f"net={net_result_pct:.2f}%",
        f"pnl={pnl_usdt:.4f}U",
        flush=True,
    )


# =========================================================
# V2 LEGACY MONITOR (keeps old test logic intact)
# =========================================================

def update_v2_trade_current_price(cur, row, current_price):
    (
        trade_id, symbol, engine_version, signal_time, entry,
        tp1, tp2, tp3, sl,
        tp1_hit, tp2_hit, tp3_hit,
        highest_price, lowest_price,
        last_checked_at, margin_usdt, leverage
    ) = row

    highest_price = max(highest_price or entry, current_price)
    lowest_price = min(lowest_price or entry, current_price)

    new_tp1 = bool(tp1_hit or current_price >= tp1)
    new_tp2 = bool(tp2_hit or current_price >= tp2)
    new_tp3 = bool(tp3_hit or current_price >= tp3)
    if new_tp3:
        new_tp1 = True
        new_tp2 = True
    if new_tp2:
        new_tp1 = True

    sl_hit = current_price <= sl

    if new_tp3:
        gross = full_tp3_gross_result()
        cur.execute(
            """
            UPDATE scanner_trades
            SET tp1_hit=TRUE, tp2_hit=TRUE, tp3_hit=TRUE,
                highest_price=%s, lowest_price=%s,
                exit_price=%s, result_pct=%s,
                status='WIN',
                exit_reason=COALESCE(exit_reason,'TP3_EXIT'),
                closed_at=NOW()
            WHERE id=%s AND status='OPEN';
            """,
            (highest_price, lowest_price, current_price, gross, trade_id),
        )
    elif sl_hit:
        remaining = 1.0
        realized = 0.0
        if new_tp1:
            realized += (TP1_PCT / 100) * SCANNER_TP1
            remaining -= TP1_PCT / 100
        if new_tp2:
            realized += (TP2_PCT / 100) * SCANNER_TP2
            remaining -= TP2_PCT / 100
        gross = realized - remaining * SCANNER_SL
        status = "WIN" if gross > 0 else "LOSS"
        cur.execute(
            """
            UPDATE scanner_trades
            SET tp1_hit=%s, tp2_hit=%s, tp3_hit=%s, sl_hit=TRUE,
                highest_price=%s, lowest_price=%s,
                exit_price=%s, result_pct=%s,
                status=%s,
                exit_reason=COALESCE(exit_reason,'SL_EXIT'),
                closed_at=NOW()
            WHERE id=%s AND status='OPEN';
            """,
            (
                new_tp1, new_tp2, new_tp3,
                highest_price, lowest_price,
                current_price, gross, status, trade_id,
            ),
        )
    else:
        cur.execute(
            """
            UPDATE scanner_trades
            SET tp1_hit=%s, tp2_hit=%s, tp3_hit=%s,
                highest_price=%s, lowest_price=%s
            WHERE id=%s AND status='OPEN';
            """,
            (
                new_tp1, new_tp2, new_tp3,
                highest_price, lowest_price, trade_id,
            ),
        )


# =========================================================
# V3 1-MINUTE REPLAY + PROTECTIVE STOPS
# =========================================================

def update_v3_trade_replay(cur, row, current_price):
    (
        trade_id, symbol, engine_version, signal_time, entry,
        tp1, tp2, tp3, sl,
        tp1_hit, tp2_hit, tp3_hit,
        highest_price, lowest_price,
        last_checked_at, margin_usdt, leverage
    ) = row

    highest_price = highest_price or entry
    lowest_price = lowest_price or entry
    new_tp1 = bool(tp1_hit)
    new_tp2 = bool(tp2_hit)
    new_tp3 = bool(tp3_hit)

    candles = get_klines(symbol, "1m", SIM_REPLAY_1M_LIMIT)

    start_time = last_checked_at or signal_time
    start_ms = int(start_time.timestamp() * 1000) if start_time else 0

    relevant = [c for c in candles if int(c["time"]) >= start_ms]
    relevant.sort(key=lambda x: x["time"])

    if not relevant:
        relevant = [{
            "time": int(time.time() * 1000),
            "open": current_price,
            "high": current_price,
            "low": current_price,
            "close": current_price,
            "volume": 0,
        }]

    for candle in relevant:
        high = float(candle["high"])
        low = float(candle["low"])

        highest_price = max(highest_price, high)
        lowest_price = min(lowest_price, low)

        # Stop active BEFORE this candle's new TP hits.
        if new_tp2:
            active_stop = tp1
            stop_return = SCANNER_TP1
            stop_reason = "TP2_PROTECT_EXIT"
        elif new_tp1:
            active_stop = entry
            stop_return = 0.0
            stop_reason = "BREAKEVEN_EXIT"
        else:
            active_stop = sl
            stop_return = -SCANNER_SL
            stop_reason = "SL_EXIT"

        if low <= active_stop:
            gross = gross_result_for_stop(new_tp1, new_tp2, stop_return)
            finalize_v3_trade(
                cur, trade_id, active_stop, gross, stop_reason,
                new_tp1, new_tp2, new_tp3,
                stop_reason == "SL_EXIT",
                highest_price, lowest_price,
                margin_usdt, leverage,
                "1m replay: protective stop hit before new TP credit",
            )
            return

        # Determine new targets touched inside this candle.
        hit_tp1_now = (not new_tp1 and high >= tp1)
        hit_tp2_now = (not new_tp2 and high >= tp2)
        hit_tp3_now = (not new_tp3 and high >= tp3)

        # Conservative ambiguity handling:
        # after TP1 becomes active, if same candle also trades back to entry,
        # close at breakeven rather than assuming the favorable path.
        if hit_tp1_now:
            new_tp1 = True
            if low <= entry:
                gross = gross_result_for_stop(True, False, 0.0)
                finalize_v3_trade(
                    cur, trade_id, entry, gross, "BREAKEVEN_EXIT",
                    True, False, False, False,
                    highest_price, lowest_price,
                    margin_usdt, leverage,
                    "same 1m candle touched TP1 and breakeven; conservative sequence",
                )
                return

        if hit_tp2_now:
            new_tp1 = True
            new_tp2 = True
            if low <= tp1:
                gross = gross_result_for_stop(True, True, SCANNER_TP1)
                finalize_v3_trade(
                    cur, trade_id, tp1, gross, "TP2_PROTECT_EXIT",
                    True, True, False, False,
                    highest_price, lowest_price,
                    margin_usdt, leverage,
                    "same 1m candle touched TP2 and TP1 protection; conservative sequence",
                )
                return

        if hit_tp3_now:
            # Only credit TP3 if the candle did not also violate the TP2 protection.
            new_tp1 = True
            new_tp2 = True
            if low <= tp1:
                gross = gross_result_for_stop(True, True, SCANNER_TP1)
                finalize_v3_trade(
                    cur, trade_id, tp1, gross, "TP2_PROTECT_EXIT",
                    True, True, False, False,
                    highest_price, lowest_price,
                    margin_usdt, leverage,
                    "same 1m candle touched TP3 and TP1 protection; conservative sequence",
                )
                return

            new_tp3 = True
            gross = full_tp3_gross_result()
            finalize_v3_trade(
                cur, trade_id, tp3, gross, "TP3_EXIT",
                True, True, True, False,
                highest_price, lowest_price,
                margin_usdt, leverage,
                "1m replay TP3",
            )
            return

    # Current price fallback after replay.
    highest_price = max(highest_price, current_price)
    lowest_price = min(lowest_price, current_price)

    if new_tp2 and current_price <= tp1:
        gross = gross_result_for_stop(True, True, SCANNER_TP1)
        finalize_v3_trade(
            cur, trade_id, tp1, gross, "TP2_PROTECT_EXIT",
            True, True, False, False,
            highest_price, lowest_price,
            margin_usdt, leverage,
            "current price below TP2 protection",
        )
        return

    if new_tp1 and not new_tp2 and current_price <= entry:
        gross = gross_result_for_stop(True, False, 0.0)
        finalize_v3_trade(
            cur, trade_id, entry, gross, "BREAKEVEN_EXIT",
            True, False, False, False,
            highest_price, lowest_price,
            margin_usdt, leverage,
            "current price below breakeven protection",
        )
        return

    if not new_tp1 and current_price <= sl:
        gross = -SCANNER_SL
        finalize_v3_trade(
            cur, trade_id, sl, gross, "SL_EXIT",
            False, False, False, True,
            highest_price, lowest_price,
            margin_usdt, leverage,
            "current price below original SL",
        )
        return

    cur.execute(
        """
        UPDATE scanner_trades
        SET tp1_hit=%s, tp2_hit=%s, tp3_hit=%s,
            highest_price=%s, lowest_price=%s,
            last_checked_at=NOW()
        WHERE id=%s AND status='OPEN';
        """,
        (
            new_tp1, new_tp2, new_tp3,
            highest_price, lowest_price,
            trade_id,
        ),
    )


def update_simulated_trades():
    prices = get_all_prices()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            id, symbol, engine_version, signal_time, entry_price,
            tp1, tp2, tp3, sl,
            tp1_hit, tp2_hit, tp3_hit,
            highest_price, lowest_price,
            last_checked_at,
            sim_margin_usdt, sim_leverage
        FROM scanner_trades
        WHERE status = 'OPEN'
          AND strategy IN ('TREND', 'BREAKOUT')
        ORDER BY id ASC;
        """
    )

    rows = cur.fetchall()

    for row in rows:
        trade_id = row[0]
        symbol = row[1]
        engine_version = row[2] or "V2"

        if symbol not in prices:
            continue

        try:
            if engine_version == "V3":
                update_v3_trade_replay(cur, row, prices[symbol])
            else:
                update_v2_trade_current_price(cur, row, prices[symbol])

            # Commit each trade so one later API error does not lose all progress.
            conn.commit()

        except Exception as e:
            conn.rollback()
            print("SIM UPDATE ERROR:", trade_id, symbol, str(e), flush=True)

    cur.close()
    conn.close()


# =========================================================
# OPTIONAL WEB MONITOR
# =========================================================

def simulation_monitor_loop():
    print("SIMULATION MONITOR STARTED", flush=True)
    while True:
        try:
            update_simulated_trades()
        except Exception as e:
            print("SIM MONITOR ERROR:", str(e), flush=True)
        time.sleep(SIM_MONITOR_SEC)


def start_simulation_monitor():
    thread = threading.Thread(target=simulation_monitor_loop, daemon=True)
    thread.start()


# =========================================================
# PERFORMANCE STATS
# =========================================================

def calculate_performance_stats(results):
    if not results:
        return {
            "closed": 0, "wins": 0, "losses": 0, "win_rate": 0,
            "avg_result": 0, "avg_win": 0, "avg_loss": 0,
            "reward_risk": 0, "profit_factor": 0, "expectancy": 0,
            "total_result": 0, "max_winning_streak": 0,
            "max_losing_streak": 0,
        }

    wins_list = [x for x in results if x > 0]
    losses_list = [x for x in results if x <= 0]
    closed = len(results)

    avg_win = sum(wins_list) / len(wins_list) if wins_list else 0
    avg_loss = sum(losses_list) / len(losses_list) if losses_list else 0
    win_rate_decimal = len(wins_list) / closed

    gross_profit = sum(wins_list)
    gross_loss = abs(sum(losses_list))

    current_win = current_loss = 0
    max_win = max_loss = 0

    for x in results:
        if x > 0:
            current_win += 1
            current_loss = 0
            max_win = max(max_win, current_win)
        else:
            current_loss += 1
            current_win = 0
            max_loss = max(max_loss, current_loss)

    return {
        "closed": closed,
        "wins": len(wins_list),
        "losses": len(losses_list),
        "win_rate": win_rate_decimal * 100,
        "avg_result": sum(results) / closed,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "reward_risk": avg_win / abs(avg_loss) if avg_loss < 0 else 0,
        "profit_factor": (
            gross_profit / gross_loss
            if gross_loss > 0
            else (float("inf") if gross_profit > 0 else 0)
        ),
        "expectancy": win_rate_decimal * avg_win + (1 - win_rate_decimal) * avg_loss,
        "total_result": sum(results),
        "max_winning_streak": max_win,
        "max_losing_streak": max_loss,
    }


def calculate_equity_metrics(pnls):
    equity = SIM_START_BALANCE
    peak = equity
    max_drawdown_usdt = 0.0
    max_drawdown_pct = 0.0

    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        dd = equity - peak
        dd_pct = (dd / peak * 100) if peak > 0 else 0
        max_drawdown_usdt = min(max_drawdown_usdt, dd)
        max_drawdown_pct = min(max_drawdown_pct, dd_pct)

    return equity, max_drawdown_usdt, max_drawdown_pct


# =========================================================
# WEB PAGES
# =========================================================

@app.route("/db-test", methods=["GET"])
def db_test():
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT NOW();")
        now = cur.fetchone()[0]
        return f"DATABASE OK | {now}", 200
    except Exception as e:
        return f"DATABASE ERROR | {str(e)}", 500
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


@app.route("/scanner-trades", methods=["GET"])
def scanner_trades_page():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            id, symbol, strategy, engine_version, signal_time,
            entry_price, score, trend_score, breakout_score,
            tp1, tp2, tp3, sl,
            tp1_hit, tp2_hit, tp3_hit, sl_hit,
            status, result_pct, net_result_pct, sim_pnl_usdt,
            exit_reason, exit_detail
        FROM scanner_trades
        ORDER BY id DESC
        LIMIT 150;
        """
    )
    rows = cur.fetchall()
    cur.close()
    conn.close()

    lines = [
        "SIMULATED TRADES",
        "============================================================",
    ]

    for row in rows:
        (
            trade_id, symbol, strategy, version, signal_time,
            entry, score, trend_score, breakout_score,
            tp1, tp2, tp3, sl,
            tp1_hit, tp2_hit, tp3_hit, sl_hit,
            status, gross, net, pnl,
            exit_reason, exit_detail
        ) = row

        lines.append(
            f"#{trade_id} {symbol} | {version or '-'} | {strategy} | {status}"
        )
        lines.append(f"Time: {signal_time}")
        lines.append(
            f"Entry: {entry} | Score {score} | Trend {trend_score} | Breakout {breakout_score}"
        )
        lines.append(
            f"TP1 {'YES' if tp1_hit else 'NO'} | "
            f"TP2 {'YES' if tp2_hit else 'NO'} | "
            f"TP3 {'YES' if tp3_hit else 'NO'} | "
            f"SL {'YES' if sl_hit else 'NO'}"
        )
        lines.append(
            f"Gross: {gross if gross is not None else '-'}% | "
            f"Net est.: {net if net is not None else '-'}% | "
            f"PnL: {pnl if pnl is not None else '-'} U"
        )
        lines.append(f"Exit: {exit_reason or '-'}")
        if exit_detail:
            lines.append(f"Detail: {exit_detail}")
        lines.append("------------------------------------------------------------")

    return "<pre>" + "\n".join(lines) + "</pre>", 200


@app.route("/scanner-stats", methods=["GET"])
def scanner_stats_page():
    conn = get_db_connection()
    cur = conn.cursor()

    # V3 top-level counts
    cur.execute(
        """
        SELECT
            COUNT(*),
            SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END)
        FROM scanner_trades
        WHERE engine_version='V3'
          AND strategy IN ('TREND','BREAKOUT');
        """
    )
    total, open_count = cur.fetchone()
    open_count = int(open_count or 0)

    cur.execute(
        """
        SELECT
            id, strategy, score, status,
            result_pct, net_result_pct, sim_pnl_usdt,
            tp1_hit, tp2_hit, tp3_hit,
            COALESCE(exit_reason,'UNKNOWN'),
            closed_at
        FROM scanner_trades
        WHERE engine_version='V3'
          AND strategy IN ('TREND','BREAKOUT')
          AND status IN ('WIN','LOSS')
        ORDER BY closed_at ASC, id ASC;
        """
    )
    rows = cur.fetchall()

    net_results = [float(r[5] or 0) for r in rows]
    gross_results = [float(r[4] or 0) for r in rows]
    pnls = [float(r[6] or 0) for r in rows]

    net_stats = calculate_performance_stats(net_results)
    gross_stats = calculate_performance_stats(gross_results)
    balance, max_dd_u, max_dd_pct = calculate_equity_metrics(pnls)

    closed = len(rows)
    tp1_hits = sum(1 for r in rows if r[7])
    tp2_hits = sum(1 for r in rows if r[8])
    tp3_hits = sum(1 for r in rows if r[9])

    # Strategy breakdown V3
    cur.execute(
        """
        SELECT
            strategy,
            COUNT(*) AS total,
            SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) AS open_count,
            SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END) AS losses,
            COALESCE(AVG(CASE WHEN net_result_pct IS NOT NULL THEN net_result_pct END),0)
        FROM scanner_trades
        WHERE engine_version='V3'
          AND strategy IN ('TREND','BREAKOUT')
        GROUP BY strategy
        ORDER BY strategy;
        """
    )
    strategy_rows = cur.fetchall()

    # Score buckets V3
    cur.execute(
        """
        SELECT
            CASE
                WHEN score >= 90 THEN '90+'
                WHEN score >= 85 THEN '85-89'
                WHEN score >= 80 THEN '80-84'
                ELSE '75-79'
            END AS bucket,
            COUNT(*) AS closed,
            SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END) AS wins,
            SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END) AS losses,
            COALESCE(AVG(net_result_pct),0)
        FROM scanner_trades
        WHERE engine_version='V3'
          AND strategy IN ('TREND','BREAKOUT')
          AND status IN ('WIN','LOSS')
        GROUP BY bucket
        ORDER BY bucket DESC;
        """
    )
    score_rows = cur.fetchall()

    # Exit reasons V3
    cur.execute(
        """
        SELECT
            COALESCE(exit_reason,'UNKNOWN'),
            COUNT(*),
            SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END),
            COALESCE(AVG(net_result_pct),0)
        FROM scanner_trades
        WHERE engine_version='V3'
          AND status IN ('WIN','LOSS')
        GROUP BY COALESCE(exit_reason,'UNKNOWN')
        ORDER BY COUNT(*) DESC;
        """
    )
    exit_rows = cur.fetchall()

    # V2 preserved summary
    cur.execute(
        """
        SELECT
            COUNT(*),
            SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='WIN' THEN 1 ELSE 0 END),
            SUM(CASE WHEN status='LOSS' THEN 1 ELSE 0 END)
        FROM scanner_trades
        WHERE engine_version='V2'
          AND strategy IN ('TREND','BREAKOUT');
        """
    )
    v2_total, v2_open, v2_wins, v2_losses = cur.fetchone()

    cur.close()
    conn.close()

    def rate(hit, denominator):
        return hit / denominator * 100 if denominator else 0

    pf = (
        "INF"
        if net_stats["profit_factor"] == float("inf")
        else f"{net_stats['profit_factor']:.2f}"
    )

    roi = (balance - SIM_START_BALANCE) / SIM_START_BALANCE * 100 if SIM_START_BALANCE else 0
    open_margin = open_count * SIM_MARGIN_USDT
    open_notional = open_margin * SIM_LEVERAGE

    lines = [
        "BINGX SCANNER V3 - PRODUCTION SIMULATION",
        "============================================================",
        f"V3 Total signals: {int(total or 0)}",
        f"Open: {open_count}",
        f"Closed: {closed}",
        f"Wins: {net_stats['wins']}",
        f"Losses: {net_stats['losses']}",
        "",
        "NET PERFORMANCE (estimated fees + slippage included)",
        "============================================================",
        f"Win rate: {net_stats['win_rate']:.2f}%",
        f"Average net result: {net_stats['avg_result']:.2f}%",
        f"Average net win: {net_stats['avg_win']:.2f}%",
        f"Average net loss: {net_stats['avg_loss']:.2f}%",
        f"Reward / Risk: {net_stats['reward_risk']:.2f}",
        f"Profit Factor: {pf}",
        f"Expectancy / trade: {net_stats['expectancy']:.2f}%",
        f"Max winning streak: {net_stats['max_winning_streak']}",
        f"Max losing streak: {net_stats['max_losing_streak']}",
        "",
        "SIMULATED ACCOUNT",
        "============================================================",
        f"Starting balance: {SIM_START_BALANCE:.2f} U",
        f"Current realized balance: {balance:.2f} U",
        f"Realized PnL: {sum(pnls):+.2f} U",
        f"Realized ROI: {roi:+.2f}%",
        f"Max drawdown: {max_dd_u:.2f} U ({max_dd_pct:.2f}%)",
        f"Margin per trade: {SIM_MARGIN_USDT:.2f} U",
        f"Sim leverage: {SIM_LEVERAGE}x",
        f"Open reserved margin: {open_margin:.2f} U",
        f"Open notional exposure: {open_notional:.2f} U",
        "",
        "COST ASSUMPTIONS",
        "============================================================",
        f"Fee per side: {SIM_FEE_PCT:.3f}%",
        f"Slippage per side: {SIM_SLIPPAGE_PCT:.3f}%",
        f"Estimated round-trip friction: {estimated_round_trip_cost_pct():.3f}%",
        "",
        "GROSS STRATEGY RESULT (before estimated costs)",
        "============================================================",
        f"Average gross result: {gross_stats['avg_result']:.2f}%",
        f"Cumulative gross result: {gross_stats['total_result']:.2f}%",
        "",
        "TP HIT RATE - CLOSED V3 TRADES",
        "============================================================",
        f"TP1: {tp1_hits}/{closed} | {rate(tp1_hits, closed):.2f}%",
        f"TP2: {tp2_hits}/{closed} | {rate(tp2_hits, closed):.2f}%",
        f"TP3: {tp3_hits}/{closed} | {rate(tp3_hits, closed):.2f}%",
        "",
        "BY STRATEGY",
        "============================================================",
    ]

    for strategy, stotal, sopen, swins, slosses, savg in strategy_rows:
        resolved = int(swins or 0) + int(slosses or 0)
        wr = int(swins or 0) / resolved * 100 if resolved else 0
        lines.append(
            f"{strategy}: {stotal} total | {int(sopen or 0)} open | "
            f"{int(swins or 0)}W/{int(slosses or 0)}L | "
            f"Win {wr:.2f}% | Avg net {float(savg):.2f}%"
        )

    lines += ["", "BY SCORE", "============================================================"]
    for bucket, btotal, bwins, blosses, bavg in score_rows:
        resolved = int(bwins or 0) + int(blosses or 0)
        wr = int(bwins or 0) / resolved * 100 if resolved else 0
        lines.append(
            f"{bucket}: {btotal} closed | {int(bwins or 0)}W/{int(blosses or 0)}L | "
            f"Win {wr:.2f}% | Avg net {float(bavg):.2f}%"
        )

    lines += ["", "EXIT REASONS", "============================================================"]
    for reason, count, ewins, elosses, eavg in exit_rows:
        resolved = int(ewins or 0) + int(elosses or 0)
        wr = int(ewins or 0) / resolved * 100 if resolved else 0
        lines.append(
            f"{reason}: {count} | {int(ewins or 0)}W/{int(elosses or 0)}L | "
            f"Win {wr:.2f}% | Avg net {float(eavg):.2f}%"
        )

    lines += [
        "",
        "RISK CONTROLS",
        "============================================================",
        f"Max open trades: {SCANNER_MAX_OPEN}",
        f"Daily loss stop: {SIM_DAILY_MAX_LOSS_USDT:.2f} U",
        f"Loss-streak pause: {SIM_MAX_CONSECUTIVE_LOSSES} losses",
        f"Pause length: {SIM_PAUSE_HOURS_AFTER_STREAK} hours",
        "",
        "HISTORICAL V2 (excluded from V3 performance)",
        "============================================================",
        f"V2 total: {int(v2_total or 0)} | "
        f"Open {int(v2_open or 0)} | "
        f"{int(v2_wins or 0)}W/{int(v2_losses or 0)}L",
        "",
        "V3 target: collect 100 CLOSED trades without changing strategy rules.",
    ]

    return "<pre>" + "\n".join(lines) + "</pre>", 200


@app.route("/scanner-status", methods=["GET"])
def scanner_status_page():
    with scanner_lock:
        status = dict(scanner_status)

    ok, risk_reason = scanner_risk_allows_new_trade()

    lines = [
        "BingX Scanner V3",
        "================================",
        f"Web full-scan running: {status['running']}",
        f"Phase: {status['phase']}",
        f"Current: {status['current_symbol']}",
        f"Simulation risk allows new trade: {ok}",
        f"Risk reason: {risk_reason}",
        "",
        "Primary scanner: Render Cron -> scanner_job.py every 5 minutes",
    ]
    return "<pre>" + "\n".join(lines) + "</pre>", 200


@app.route("/scan-now", methods=["GET"])
def scan_now():
    # Retained only as a compatibility message.
    # Fast V3 radar is intentionally run by Render Cron to avoid web-sleep issues.
    return (
        "<pre>"
        "V3 FAST RADAR IS CRON-MANAGED\n"
        "Use Render Cron: scanner_job.py\n"
        "Schedule: */5 * * * *\n"
        "</pre>",
        200,
    )


@app.route("/", methods=["GET"])
def home():
    mode = "LIVE" if LIVE_TRADING else "TEST"
    return (
        "<pre>"
        "LINE BingX Bot + Scanner V3\n"
        "================================\n"
        f"LINE manual trading: {mode}\n"
        f"LINE leverage: {LEVERAGE}x\n"
        "Autonomous scanner: SIMULATION ONLY\n"
        "Scanner version: V3\n"
        "Scanner manager: Render Cron every 5 minutes\n\n"
        "Pages:\n"
        "/db-test\n"
        "/scanner-status\n"
        "/scanner-trades\n"
        "/scanner-stats\n"
        "</pre>",
        200,
    )


# =========================================================
# LINE WEBHOOK
# =========================================================

@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_data(as_text=True)
    signature = request.headers.get("X-Line-Signature", "")

    if not LINE_CHANNEL_SECRET:
        return "LINE_CHANNEL_SECRET not set", 500

    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    expected_signature = base64.b64encode(digest).decode("utf-8")

    if not hmac.compare_digest(signature, expected_signature):
        abort(400)

    data = request.get_json(silent=True) or {}

    for event in data.get("events", []):
        if event.get("type") != "message":
            continue

        message = event.get("message", {})
        if message.get("type") != "text":
            continue

        text = message.get("text", "")
        event_id = event.get("webhookEventId", str(time.time_ns()))

        print("LINE MESSAGE:", text, flush=True)
        signal = parse_signal(text)
        print("PARSED SIGNAL:", signal, flush=True)

        if not signal["symbol"]:
            print("NOT A SIGNAL", flush=True)
            continue

        valid, error = validate_signal(signal)
        if not valid:
            print("SIGNAL REJECTED:", error, flush=True)
            continue

        try:
            entry_result, params, contract, expected_qty = place_limit_entry(
                signal, event_id
            )

            print(
                "LIMIT ENTRY RESULT:",
                json.dumps(entry_result, ensure_ascii=False),
                flush=True,
            )

            if entry_result.get("code") != 0:
                raise RuntimeError(f"Entry failed: {entry_result}")

            if not LIVE_TRADING:
                print("LINE TEST MODE: NO REAL ORDER", flush=True)
                continue

            data_result = entry_result.get("data", {})
            order = data_result.get("order", data_result)
            order_id = order.get("orderId")
            client_order_id = params.get("clientOrderId")

            if not order_id:
                raise RuntimeError("No orderId returned")

            thread = threading.Thread(
                target=monitor_limit_order,
                args=(signal, event_id, order_id, client_order_id, contract),
                daemon=True,
            )
            thread.start()

        except Exception as e:
            print("TRADING ERROR:", str(e), flush=True)

    return "OK", 200


# =========================================================
# STARTUP
# =========================================================

try:
    init_database()
except Exception as e:
    print("STARTUP DATABASE ERROR:", str(e), flush=True)

# Important: disabled by default because Render Cron owns V3 monitoring.
# This avoids app import -> duplicate daemon monitor inside scanner_job.
if DATABASE_URL and ENABLE_WEB_SIM_MONITOR:
    try:
        start_simulation_monitor()
    except Exception as e:
        print("MONITOR START ERROR:", str(e), flush=True)
