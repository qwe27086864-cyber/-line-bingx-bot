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
import psycopg2


app = Flask(__name__)


# =========================================================
# ENVIRONMENT VARIABLES
# =========================================================

LINE_CHANNEL_SECRET = os.environ.get(
    "LINE_CHANNEL_SECRET",
    ""
)

BINGX_API_KEY = os.environ.get(
    "BINGX_API_KEY",
    ""
)

BINGX_SECRET_KEY = os.environ.get(
    "BINGX_SECRET_KEY",
    ""
)

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    ""
)

LIVE_TRADING = (
    os.environ.get(
        "LIVE_TRADING",
        "false"
    ).strip().lower()
    == "true"
)

ORDER_USDT = float(
    os.environ.get(
        "ORDER_USDT",
        "10"
    )
)

MAX_LIVE_ORDER_USDT = float(
    os.environ.get(
        "MAX_LIVE_ORDER_USDT",
        "10"
    )
)

TP1_PCT = float(
    os.environ.get(
        "TP1_PCT",
        "30"
    )
)

TP2_PCT = float(
    os.environ.get(
        "TP2_PCT",
        "40"
    )
)

TP3_PCT = float(
    os.environ.get(
        "TP3_PCT",
        "30"
    )
)

LEVERAGE = int(
    os.environ.get(
        "LEVERAGE",
        "8"
    )
)

BINGX_POSITION_MODE = (
    os.environ.get(
        "BINGX_POSITION_MODE",
        "HEDGE"
    ).strip().upper()
)

ENTRY_TIMEOUT_SEC = int(
    os.environ.get(
        "ENTRY_TIMEOUT_SEC",
        "600"
    )
)

ORDER_POLL_SEC = float(
    os.environ.get(
        "ORDER_POLL_SEC",
        "2"
    )
)

BINGX_BASE_URL = (
    "https://open-api.bingx.com"
)


# =========================================================
# SCANNER SETTINGS
# =========================================================

SCANNER_MIN_SCORE = int(
    os.environ.get(
        "SCANNER_MIN_SCORE",
        "75"
    )
)

SCANNER_MAX_NEW_PER_SCAN = int(
    os.environ.get(
        "SCANNER_MAX_NEW_PER_SCAN",
        "3"
    )
)

SCANNER_MAX_OPEN = int(
    os.environ.get(
        "SCANNER_MAX_OPEN",
        "10"
    )
)

SCANNER_COOLDOWN_HOURS = int(
    os.environ.get(
        "SCANNER_COOLDOWN_HOURS",
        "4"
    )
)

SCANNER_TP1 = float(
    os.environ.get(
        "SCANNER_TP1",
        "2"
    )
)

SCANNER_TP2 = float(
    os.environ.get(
        "SCANNER_TP2",
        "4"
    )
)

SCANNER_TP3 = float(
    os.environ.get(
        "SCANNER_TP3",
        "8"
    )
)

SCANNER_SL = float(
    os.environ.get(
        "SCANNER_SL",
        "3"
    )
)

SCANNER_API_DELAY = float(
    os.environ.get(
        "SCANNER_API_DELAY",
        "1.05"
    )
)

SIM_MONITOR_SEC = int(
    os.environ.get(
        "SIM_MONITOR_SEC",
        "30"
    )
)


# =========================================================
# GLOBAL SCANNER STATUS
# =========================================================

scanner_lock = threading.Lock()

scanner_status = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "total": 0,
    "processed": 0,
    "candidates": 0,
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

        raise RuntimeError(
            "DATABASE_URL not set"
        )

    return psycopg2.connect(
        DATABASE_URL,
        sslmode="require"
    )


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
                signal_time TIMESTAMPTZ DEFAULT NOW(),
                entry_price DOUBLE PRECISION NOT NULL,

                score INTEGER,
                rsi DOUBLE PRECISION,
                volume_ratio DOUBLE PRECISION,
                momentum_pct DOUBLE PRECISION,

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

                closed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            """
        )

        cur.execute(
            """
            ALTER TABLE scanner_trades
            ADD COLUMN IF NOT EXISTS momentum_pct
            DOUBLE PRECISION;
            """
        )

        cur.execute(
            """
            ALTER TABLE scanner_trades
            ADD COLUMN IF NOT EXISTS reasons TEXT;
            """
        )

        conn.commit()

        print(
            "DATABASE READY",
            flush=True
        )

    except Exception as e:

        print(
            "DATABASE INIT ERROR:",
            str(e),
            flush=True
        )

        raise

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()


# =========================================================
# SIGNAL PARSER
# =========================================================

def parse_signal(text):

    result = {
        "symbol": None,
        "side": None,
        "entry": None,
        "tp1": None,
        "tp2": None,
        "tp3": None,
        "sl": None,
    }

    symbol = re.search(
        r"幣種\s*[:：]\s*([A-Za-z0-9]+)",
        text
    )

    side = re.search(
        r"方向\s*[:：]\s*(多|空|LONG|SHORT|Long|Short)",
        text
    )

    entry = re.search(
        r"進場(?:價位)?\s*[:：]\s*([0-9.]+)",
        text
    )

    tp1 = re.search(
        r"TP1\s*[:：]\s*([0-9.]+)",
        text,
        re.I
    )

    tp2 = re.search(
        r"TP2\s*[:：]\s*([0-9.]+)",
        text,
        re.I
    )

    tp3 = re.search(
        r"TP3\s*[:：]\s*([0-9.]+)",
        text,
        re.I
    )

    sl = re.search(
        r"(?:SL|止損)\s*[:：]\s*([0-9.]+)",
        text,
        re.I
    )

    if symbol:
        result["symbol"] = (
            symbol.group(1).upper()
        )

    if side:

        s = side.group(1).upper()

        result["side"] = (
            "LONG"
            if s in ["多", "LONG"]
            else "SHORT"
        )

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


# =========================================================
# SIGNAL VALIDATION
# =========================================================

def validate_signal(signal):

    required = [
        "symbol",
        "side",
        "entry",
        "tp1",
        "tp2",
        "tp3",
        "sl",
    ]

    for key in required:

        if not signal.get(key):

            return (
                False,
                f"Missing field: {key}"
            )

    try:

        entry = float(signal["entry"])
        tp1 = float(signal["tp1"])
        tp2 = float(signal["tp2"])
        tp3 = float(signal["tp3"])
        sl = float(signal["sl"])

    except ValueError:

        return (
            False,
            "Invalid price"
        )

    if min(
        entry,
        tp1,
        tp2,
        tp3,
        sl
    ) <= 0:

        return (
            False,
            "Prices must be > 0"
        )

    if abs(
        TP1_PCT
        + TP2_PCT
        + TP3_PCT
        - 100
    ) > 0.0001:

        return (
            False,
            "TP percentages must total 100"
        )

    return True, None


# =========================================================
# BINGX SIGNING
# =========================================================

def build_canonical(params):

    return "&".join(
        f"{k}={v}"
        for k, v in sorted(
            params.items()
        )
    )


def bingx_private_request(
    method,
    path,
    params=None
):

    if not BINGX_API_KEY:

        raise RuntimeError(
            "BINGX_API_KEY not set"
        )

    if not BINGX_SECRET_KEY:

        raise RuntimeError(
            "BINGX_SECRET_KEY not set"
        )

    params = dict(
        params or {}
    )

    params["recvWindow"] = 5000

    params["timestamp"] = int(
        time.time() * 1000
    )

    canonical = build_canonical(
        params
    )

    signature = hmac.new(
        BINGX_SECRET_KEY.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    signed = (
        canonical
        + "&signature="
        + signature
    )

    headers = {
        "X-BX-APIKEY":
            BINGX_API_KEY,

        "Content-Type":
            "application/x-www-form-urlencoded"
    }

    if method == "GET":

        url = (
            BINGX_BASE_URL
            + path
            + "?"
            + signed
        )

        data = None

    else:

        url = (
            BINGX_BASE_URL
            + path
        )

        data = signed.encode(
            "utf-8"
        )

    req = urllib.request.Request(
        url,
        headers=headers,
        data=data,
        method=method
    )

    try:

        with urllib.request.urlopen(
            req,
            timeout=20
        ) as response:

            body = (
                response
                .read()
                .decode("utf-8")
            )

            return json.loads(body)

    except urllib.error.HTTPError as e:

        body = (
            e.read()
            .decode(
                "utf-8",
                errors="replace"
            )
        )

        raise RuntimeError(
            f"BingX HTTP "
            f"{e.code}: "
            f"{body}"
        )


# =========================================================
# SET LEVERAGE
# =========================================================

def set_leverage(
    symbol,
    position_side
):

    leverage_side = (
        position_side
        if BINGX_POSITION_MODE == "HEDGE"
        else "BOTH"
    )

    params = {
        "symbol": symbol,
        "side": leverage_side,
        "leverage": LEVERAGE,
    }

    result = bingx_private_request(
        "POST",
        "/openApi/swap/v2/trade/leverage",
        params
    )

    if result.get("code") != 0:

        raise RuntimeError(
            f"Set leverage failed: "
            f"{result}"
        )

    return result


# =========================================================
# CONTRACT INFO
# =========================================================

def get_contract_info(symbol):

    result = bingx_private_request(
        "GET",
        "/openApi/swap/v2/quote/contracts",
        {
            "symbol": symbol
        }
    )

    if result.get("code") != 0:

        raise RuntimeError(
            f"Contract query failed: "
            f"{result}"
        )

    data = result.get(
        "data",
        []
    )

    if isinstance(data, dict):
        data = [data]

    for item in data:

        if item.get(
            "symbol"
        ) == symbol:

            return item

    raise RuntimeError(
        f"Contract not found: "
        f"{symbol}"
    )


# =========================================================
# PRECISION
# =========================================================

def floor_number(
    value,
    precision
):

    factor = (
        10 ** precision
    )

    return (
        math.floor(
            value * factor
        )
        / factor
    )


def format_number(
    value,
    precision
):

    return (
        f"{value:.{precision}f}"
    )


# =========================================================
# CLIENT ORDER ID
# =========================================================

def client_id(
    event_id,
    suffix
):

    raw = (
        str(event_id)
        + "|"
        + str(suffix)
    )

    digest = hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()

    return (
        "line"
        + digest[:24]
        + suffix
    )


# =========================================================
# BUILD LIMIT ENTRY
# =========================================================

def build_limit_entry(
    signal,
    event_id
):

    symbol = (
        signal["symbol"]
        + "-USDT"
    )

    contract = get_contract_info(
        symbol
    )

    quantity_precision = int(
        contract.get(
            "quantityPrecision",
            0
        )
    )

    price_precision = int(
        contract.get(
            "pricePrecision",
            8
        )
    )

    min_qty = float(
        contract.get(
            "tradeMinQuantity",
            0
        )
    )

    min_usdt = float(
        contract.get(
            "tradeMinUSDT",
            0
        )
    )

    entry_price = float(
        signal["entry"]
    )

    entry_price = floor_number(
        entry_price,
        price_precision
    )

    if entry_price <= 0:

        raise RuntimeError(
            "Invalid entry price"
        )

    raw_quantity = (
        ORDER_USDT
        / entry_price
    )

    quantity = floor_number(
        raw_quantity,
        quantity_precision
    )

    if quantity <= 0:

        raise RuntimeError(
            "Entry quantity became zero"
        )

    if (
        min_qty > 0
        and quantity < min_qty
    ):

        raise RuntimeError(
            f"Entry quantity "
            f"{quantity} below "
            f"minimum {min_qty}"
        )

    notional = (
        quantity
        * entry_price
    )

    if (
        min_usdt > 0
        and notional < min_usdt
    ):

        raise RuntimeError(
            f"Entry value "
            f"{notional} USDT "
            f"below minimum "
            f"{min_usdt}"
        )

    if (
        LIVE_TRADING
        and ORDER_USDT
        > MAX_LIVE_ORDER_USDT
    ):

        raise RuntimeError(
            "LIVE ORDER BLOCKED: "
            f"ORDER_USDT="
            f"{ORDER_USDT} "
            f"> MAX="
            f"{MAX_LIVE_ORDER_USDT}"
        )

    side = (
        "BUY"
        if signal["side"]
        == "LONG"
        else "SELL"
    )

    position_side = (
        signal["side"]
        if BINGX_POSITION_MODE
        == "HEDGE"
        else "BOTH"
    )

    params = {
        "symbol":
            symbol,

        "side":
            side,

        "positionSide":
            position_side,

        "type":
            "LIMIT",

        "quantity":
            format_number(
                quantity,
                quantity_precision
            ),

        "price":
            format_number(
                entry_price,
                price_precision
            ),

        "timeInForce":
            "GTC",

        "clientOrderId":
            client_id(
                event_id,
                "entry"
            ),
    }

    return (
        params,
        contract,
        quantity
    )


# =========================================================
# PLACE LIMIT ENTRY
# =========================================================

def place_limit_entry(
    signal,
    event_id
):

    (
        params,
        contract,
        quantity
    ) = build_limit_entry(
        signal,
        event_id
    )

    symbol = params["symbol"]

    if LIVE_TRADING:

        set_leverage(
            symbol,
            signal["side"]
        )

    else:

        print(
            "TEST MODE LEVERAGE:",
            f"{LEVERAGE}x",
            signal["side"],
            flush=True
        )

    print(
        "LIMIT ENTRY ORDER:",
        params,
        flush=True
    )

    path = (
        "/openApi/swap/v2/trade/order"
        if LIVE_TRADING
        else
        "/openApi/swap/v2/trade/order/test"
    )

    result = bingx_private_request(
        "POST",
        path,
        params
    )

    return (
        result,
        params,
        contract,
        quantity
    )


# =========================================================
# QUERY ORDER
# =========================================================

def query_order(
    symbol,
    order_id=None,
    client_order_id=None
):

    params = {
        "symbol": symbol
    }

    if order_id:

        params["orderId"] = order_id

    elif client_order_id:

        params["clientOrderId"] = (
            client_order_id
        )

    else:

        raise RuntimeError(
            "Missing order ID"
        )

    return bingx_private_request(
        "GET",
        "/openApi/swap/v2/trade/order",
        params
    )


# =========================================================
# CANCEL ORDER
# =========================================================

def cancel_order(
    symbol,
    order_id=None,
    client_order_id=None
):

    params = {
        "symbol": symbol
    }

    if order_id:

        params["orderId"] = order_id

    else:

        params[
            "clientOrderId"
        ] = client_order_id

    return bingx_private_request(
        "DELETE",
        "/openApi/swap/v2/trade/order",
        params
    )


# =========================================================
# BUILD EXIT ORDER
# =========================================================

def build_exit_order(
    symbol,
    position_side,
    quantity,
    stop_price,
    order_type,
    event_id,
    suffix,
    quantity_precision,
    price_precision
):

    close_side = (
        "SELL"
        if position_side
        == "LONG"
        else "BUY"
    )

    params = {
        "symbol":
            symbol,

        "side":
            close_side,

        "positionSide":
            (
                position_side
                if BINGX_POSITION_MODE
                == "HEDGE"
                else "BOTH"
            ),

        "type":
            order_type,

        "quantity":
            format_number(
                quantity,
                quantity_precision
            ),

        "stopPrice":
            format_number(
                stop_price,
                price_precision
            ),

        "workingType":
            "MARK_PRICE",
    }

    if (
        BINGX_POSITION_MODE
        == "ONEWAY"
    ):

        params[
            "reduceOnly"
        ] = "true"

    return params


# =========================================================
# TP / SL
# =========================================================

def place_tp_sl(
    signal,
    event_id,
    filled_qty,
    contract
):

    symbol = (
        signal["symbol"]
        + "-USDT"
    )

    quantity_precision = int(
        contract.get(
            "quantityPrecision",
            0
        )
    )

    price_precision = int(
        contract.get(
            "pricePrecision",
            8
        )
    )

    min_qty = float(
        contract.get(
            "tradeMinQuantity",
            0
        )
    )

    min_usdt = float(
        contract.get(
            "tradeMinUSDT",
            0
        )
    )

    q1 = floor_number(
        filled_qty
        * TP1_PCT
        / 100,
        quantity_precision
    )

    q2 = floor_number(
        filled_qty
        * TP2_PCT
        / 100,
        quantity_precision
    )

    q3 = floor_number(
        filled_qty
        - q1
        - q2,
        quantity_precision
    )

    checks = [
        (
            "TP1",
            q1,
            float(signal["tp1"])
        ),
        (
            "TP2",
            q2,
            float(signal["tp2"])
        ),
        (
            "TP3",
            q3,
            float(signal["tp3"])
        ),
    ]

    for (
        name,
        qty,
        trigger_price
    ) in checks:

        if qty <= 0:

            raise RuntimeError(
                f"{name} qty became zero"
            )

        if (
            min_qty > 0
            and qty < min_qty
        ):

            raise RuntimeError(
                f"{name} qty "
                f"{qty} below "
                f"minimum "
                f"{min_qty}"
            )

        value = (
            qty
            * trigger_price
        )

        if (
            min_usdt > 0
            and value < min_usdt
        ):

            raise RuntimeError(
                f"{name} value "
                f"{value} USDT "
                f"below minimum "
                f"{min_usdt}"
            )

    tp_results = []

    for (
        name,
        qty,
        trigger_price
    ) in checks:

        params = build_exit_order(
            symbol,
            signal["side"],
            qty,
            trigger_price,
            "TAKE_PROFIT_MARKET",
            event_id,
            name.lower(),
            quantity_precision,
            price_precision
        )

        result = bingx_private_request(
            "POST",
            "/openApi/swap/v2/trade/order",
            params
        )

        if result.get(
            "code"
        ) != 0:

            raise RuntimeError(
                f"{name} failed: "
                f"{result}"
            )

        tp_results.append(
            result
        )

    sl_params = build_exit_order(
        symbol,
        signal["side"],
        filled_qty,
        float(signal["sl"]),
        "STOP_MARKET",
        event_id,
        "sl",
        quantity_precision,
        price_precision
    )

    sl_result = bingx_private_request(
        "POST",
        "/openApi/swap/v2/trade/order",
        sl_params
    )

    if sl_result.get(
        "code"
    ) != 0:

        raise RuntimeError(
            f"SL failed: "
            f"{sl_result}"
        )

    return {
        "tp": tp_results,
        "sl": sl_result
    }


# =========================================================
# MONITOR LIMIT ORDER
# =========================================================

def monitor_limit_order(
    signal,
    event_id,
    order_id,
    client_order_id,
    contract
):

    symbol = (
        signal["symbol"]
        + "-USDT"
    )

    start = time.time()

    while True:

        try:

            result = query_order(
                symbol,
                order_id,
                client_order_id
            )

            if result.get(
                "code"
            ) != 0:

                raise RuntimeError(
                    str(result)
                )

            order = result.get(
                "data",
                {}
            )

            if (
                isinstance(
                    order,
                    dict
                )
                and "order"
                in order
            ):

                order = order["order"]

            status = str(
                order.get(
                    "status",
                    ""
                )
            ).upper()

            executed_qty = float(
                order.get(
                    "executedQty",
                    0
                )
                or 0
            )

            if status == "FILLED":

                if executed_qty <= 0:

                    raise RuntimeError(
                        "FILLED but "
                        "executedQty=0"
                    )

                place_tp_sl(
                    signal,
                    event_id,
                    executed_qty,
                    contract
                )

                return

            if status in [
                "CANCELED",
                "EXPIRED"
            ]:

                return

            if (
                time.time()
                - start
                >= ENTRY_TIMEOUT_SEC
            ):

                cancel_order(
                    symbol,
                    order_id,
                    client_order_id
                )

                return

        except Exception as e:

            print(
                "MONITOR ERROR:",
                str(e),
                flush=True
            )

        time.sleep(
            ORDER_POLL_SEC
        )


# =========================================================
# EMA
# =========================================================

def calculate_ema(
    values,
    period
):

    if len(values) < period:

        return None

    seed = (
        sum(
            values[:period]
        )
        / period
    )

    multiplier = (
        2
        / (
            period
            + 1
        )
    )

    ema_value = seed

    for price in values[
        period:
    ]:

        ema_value = (
            price
            - ema_value
        ) * multiplier + ema_value

    return ema_value


# =========================================================
# RSI
# =========================================================

def calculate_rsi(
    closes,
    period=14
):

    if len(closes) < (
        period + 1
    ):

        return None

    gains = []
    losses = []

    for i in range(
        1,
        len(closes)
    ):

        change = (
            closes[i]
            - closes[i - 1]
        )

        if change > 0:

            gains.append(change)
            losses.append(0)

        else:

            gains.append(0)
            losses.append(
                abs(change)
            )

    avg_gain = (
        sum(
            gains[-period:]
        )
        / period
    )

    avg_loss = (
        sum(
            losses[-period:]
        )
        / period
    )

    if avg_loss == 0:

        return 100.0

    rs = (
        avg_gain
        / avg_loss
    )

    return (
        100
        - (
            100
            / (
                1 + rs
            )
        )
    )


# =========================================================
# GET KLINES
# =========================================================

def get_klines(
    symbol,
    interval,
    limit=100
):

    result = bingx_private_request(
        "GET",
        "/openApi/swap/v3/quote/klines",
        {
            "symbol":
                symbol,

            "interval":
                interval,

            "limit":
                limit,
        }
    )

    if result.get(
        "code"
    ) != 0:

        raise RuntimeError(
            f"Kline failed: "
            f"{result}"
        )

    data = result.get(
        "data",
        []
    )

    candles = []

    for item in data:

        try:

            if isinstance(
                item,
                list
            ):

                candle = {
                    "time":
                        int(item[0]),

                    "open":
                        float(item[1]),

                    "high":
                        float(item[2]),

                    "low":
                        float(item[3]),

                    "close":
                        float(item[4]),

                    "volume":
                        float(item[5]),
                }

            else:

                candle = {
                    "time":
                        int(
                            item.get(
                                "time",
                                item.get(
                                    "openTime",
                                    0
                                )
                            )
                        ),

                    "open":
                        float(
                            item["open"]
                        ),

                    "high":
                        float(
                            item["high"]
                        ),

                    "low":
                        float(
                            item["low"]
                        ),

                    "close":
                        float(
                            item["close"]
                        ),

                    "volume":
                        float(
                            item["volume"]
                        ),
                }

            candles.append(
                candle
            )

        except Exception:

            continue

    candles.sort(
        key=lambda x:
            x["time"]
    )

    return candles


# =========================================================
# GET ALL CONTRACTS
# =========================================================

def get_all_usdt_contracts():

    result = bingx_private_request(
        "GET",
        "/openApi/swap/v2/quote/contracts",
        {}
    )

    if result.get(
        "code"
    ) != 0:

        raise RuntimeError(
            f"Contract list failed: "
            f"{result}"
        )

    symbols = []

    for item in result.get(
        "data",
        []
    ):

        symbol = str(
            item.get(
                "symbol",
                ""
            )
        ).upper()

        if not symbol.endswith(
            "-USDT"
        ):

            continue

        status = item.get(
            "status",
            None
        )

        if (
            status is not None
            and str(status)
            in [
                "0",
                "false",
                "False"
            ]
        ):

            continue

        symbols.append(
            symbol
        )

    return sorted(
        list(
            set(symbols)
        )
    )


# =========================================================
# ANALYZE 15M
# =========================================================

def analyze_15m(
    symbol
):

    candles = get_klines(
        symbol,
        "15m",
        100
    )

    if len(candles) < 60:

        return None

    # Ignore current unfinished candle
    completed = candles[:-1]

    if len(completed) < 55:

        return None

    closes = [
        x["close"]
        for x in completed
    ]

    highs = [
        x["high"]
        for x in completed
    ]

    volumes = [
        x["volume"]
        for x in completed
    ]

    price = closes[-1]

    ema20 = calculate_ema(
        closes,
        20
    )

    ema50 = calculate_ema(
        closes,
        50
    )

    rsi = calculate_rsi(
        closes,
        14
    )

    if (
        ema20 is None
        or ema50 is None
        or rsi is None
    ):

        return None

    if len(closes) < 6:

        return None

    momentum_pct = (
        (
            price
            - closes[-6]
        )
        / closes[-6]
        * 100
    )

    previous_volumes = (
        volumes[-21:-1]
    )

    avg_volume = (
        sum(previous_volumes)
        / len(previous_volumes)
        if previous_volumes
        else 0
    )

    last_volume = volumes[-1]

    volume_ratio = (
        last_volume
        / avg_volume
        if avg_volume > 0
        else 0
    )

    previous_high = max(
        highs[-21:-1]
    )

    breakout = (
        price
        > previous_high
    )

    score = 0
    reasons = []

    if price > ema20:

        score += 15

        reasons.append(
            "15m價格>EMA20"
        )

    if ema20 > ema50:

        score += 20

        reasons.append(
            "15m EMA20>EMA50"
        )

    if (
        rsi >= 50
        and rsi <= 68
    ):

        score += 10

        reasons.append(
            f"RSI={rsi:.1f}"
        )

    elif rsi > 75:

        score -= 10

        reasons.append(
            f"RSI過熱={rsi:.1f}"
        )

    if momentum_pct > 1:

        score += 10

        reasons.append(
            f"短線+{momentum_pct:.2f}%"
        )

    elif momentum_pct > 0.3:

        score += 5

    if volume_ratio >= 1.5:

        score += 15

        reasons.append(
            f"量能={volume_ratio:.2f}x"
        )

    if volume_ratio >= 2:

        score += 5

    if breakout:

        score += 15

        reasons.append(
            "突破20根高點"
        )

    return {
        "symbol": symbol,
        "price": price,
        "score": score,
        "rsi": rsi,
        "volume_ratio":
            volume_ratio,
        "momentum_pct":
            momentum_pct,
        "breakout":
            breakout,
        "ema20":
            ema20,
        "ema50":
            ema50,
        "reasons":
            reasons,
    }


# =========================================================
# CONFIRM 1H
# =========================================================

def confirm_1h(
    result
):

    candles = get_klines(
        result["symbol"],
        "1h",
        100
    )

    if len(candles) < 60:

        result[
            "trend_1h"
        ] = "UNKNOWN"

        return result

    completed = candles[:-1]

    closes = [
        x["close"]
        for x in completed
    ]

    price = closes[-1]

    ema20 = calculate_ema(
        closes,
        20
    )

    ema50 = calculate_ema(
        closes,
        50
    )

    if (
        ema20 is None
        or ema50 is None
    ):

        result[
            "trend_1h"
        ] = "UNKNOWN"

        return result

    if (
        price > ema20
        and ema20 > ema50
    ):

        result[
            "trend_1h"
        ] = "UP"

        result[
            "score"
        ] += 10

        result[
            "reasons"
        ].append(
            "1h多頭確認"
        )

    elif price > ema20:

        result[
            "trend_1h"
        ] = "WEAK_UP"

        result[
            "score"
        ] += 5

    else:

        result[
            "trend_1h"
        ] = "DOWN"

    return result


# =========================================================
# DB HELPERS
# =========================================================

def scanner_open_count():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM scanner_trades
        WHERE status = 'OPEN';
        """
    )

    count = cur.fetchone()[0]

    cur.close()
    conn.close()

    return count


def scanner_can_open(
    symbol
):

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT id
        FROM scanner_trades
        WHERE symbol = %s
        AND (
            status = 'OPEN'
            OR signal_time >
                NOW()
                - (%s * INTERVAL '1 hour')
        )
        ORDER BY signal_time DESC
        LIMIT 1;
        """,
        (
            symbol,
            SCANNER_COOLDOWN_HOURS
        )
    )

    found = cur.fetchone()

    cur.close()
    conn.close()

    return (
        found is None
    )


def create_simulated_trade(
    result
):

    entry = float(
        result["price"]
    )

    tp1 = entry * (
        1
        + SCANNER_TP1
        / 100
    )

    tp2 = entry * (
        1
        + SCANNER_TP2
        / 100
    )

    tp3 = entry * (
        1
        + SCANNER_TP3
        / 100
    )

    sl = entry * (
        1
        - SCANNER_SL
        / 100
    )

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO scanner_trades (
            symbol,
            side,
            entry_price,
            score,
            rsi,
            volume_ratio,
            momentum_pct,
            trend_15m,
            trend_1h,
            breakout,
            reasons,
            tp1,
            tp2,
            tp3,
            sl,
            highest_price,
            lowest_price
        )
        VALUES (
            %s,
            'LONG',
            %s,
            %s,
            %s,
            %s,
            %s,
            'UP',
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        )
        RETURNING id;
        """,
        (
            result["symbol"],
            entry,
            result["score"],
            result["rsi"],
            result[
                "volume_ratio"
            ],
            result[
                "momentum_pct"
            ],
            result.get(
                "trend_1h",
                "UNKNOWN"
            ),
            result[
                "breakout"
            ],
            json.dumps(
                result[
                    "reasons"
                ],
                ensure_ascii=False
            ),
            tp1,
            tp2,
            tp3,
            sl,
            entry,
            entry,
        )
    )

    trade_id = (
        cur.fetchone()[0]
    )

    conn.commit()
    cur.close()
    conn.close()

    print(
        "SIMULATED TRADE CREATED:",
        trade_id,
        result["symbol"],
        result["score"],
        flush=True
    )

    return trade_id


# =========================================================
# FULL MARKET SCANNER
# =========================================================

def run_market_scan():

    global scanner_status

    try:

        with scanner_lock:

            scanner_status[
                "running"
            ] = True

            scanner_status[
                "started_at"
            ] = time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            scanner_status[
                "finished_at"
            ] = None

            scanner_status[
                "processed"
            ] = 0

            scanner_status[
                "candidates"
            ] = 0

            scanner_status[
                "new_trades"
            ] = 0

            scanner_status[
                "error"
            ] = ""

            scanner_status[
                "top_results"
            ] = []

        symbols = (
            get_all_usdt_contracts()
        )

        with scanner_lock:

            scanner_status[
                "total"
            ] = len(symbols)

        print(
            "SCANNER START:",
            len(symbols),
            "symbols",
            flush=True
        )

        results = []

        for index, symbol in enumerate(
            symbols
        ):

            with scanner_lock:

                scanner_status[
                    "current_symbol"
                ] = symbol

                scanner_status[
                    "processed"
                ] = index

            try:

                result = analyze_15m(
                    symbol
                )

                if result:

                    results.append(
                        result
                    )

            except Exception as e:

                print(
                    "SCAN SYMBOL ERROR:",
                    symbol,
                    str(e),
                    flush=True
                )

            with scanner_lock:

                scanner_status[
                    "processed"
                ] = index + 1

            time.sleep(
                SCANNER_API_DELAY
            )

        results.sort(
            key=lambda x:
                x["score"],
            reverse=True
        )

        # Only spend extra API calls
        # on strongest 15m candidates
        top_for_1h = results[:30]

        confirmed = []

        for result in top_for_1h:

            try:

                result = confirm_1h(
                    result
                )

                confirmed.append(
                    result
                )

            except Exception as e:

                print(
                    "1H CONFIRM ERROR:",
                    result["symbol"],
                    str(e),
                    flush=True
                )

            time.sleep(
                SCANNER_API_DELAY
            )

        confirmed.sort(
            key=lambda x:
                x["score"],
            reverse=True
        )

        qualified = [
            x
            for x in confirmed
            if x[
                "score"
            ] >= SCANNER_MIN_SCORE
        ]

        with scanner_lock:

            scanner_status[
                "candidates"
            ] = len(qualified)

            scanner_status[
                "top_results"
            ] = qualified[:10]

        created = 0

        for result in qualified:

            if (
                created
                >= SCANNER_MAX_NEW_PER_SCAN
            ):

                break

            if (
                scanner_open_count()
                >= SCANNER_MAX_OPEN
            ):

                break

            if not scanner_can_open(
                result["symbol"]
            ):

                continue

            create_simulated_trade(
                result
            )

            created += 1

        with scanner_lock:

            scanner_status[
                "new_trades"
            ] = created

            scanner_status[
                "finished_at"
            ] = time.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        print(
            "SCANNER FINISHED:",
            "created",
            created,
            flush=True
        )

    except Exception as e:

        print(
            "SCANNER FATAL ERROR:",
            str(e),
            flush=True
        )

        with scanner_lock:

            scanner_status[
                "error"
            ] = str(e)

    finally:

        with scanner_lock:

            scanner_status[
                "running"
            ] = False

            scanner_status[
                "current_symbol"
            ] = ""


# =========================================================
# GET ALL CURRENT PRICES
# =========================================================

def get_all_prices():

    result = bingx_private_request(
        "GET",
        "/openApi/swap/v1/ticker/price",
        {}
    )

    if result.get(
        "code"
    ) != 0:

        raise RuntimeError(
            f"Price query failed: "
            f"{result}"
        )

    data = result.get(
        "data",
        []
    )

    if isinstance(
        data,
        dict
    ):

        data = [data]

    prices = {}

    for item in data:

        try:

            symbol = item[
                "symbol"
            ]

            price = float(
                item["price"]
            )

            prices[
                symbol
            ] = price

        except Exception:

            continue

    return prices


# =========================================================
# SIMULATED TRADE MONITOR
# =========================================================

def update_simulated_trades():

    prices = get_all_prices()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            id,
            symbol,
            entry_price,
            tp1,
            tp2,
            tp3,
            sl,
            tp1_hit,
            tp2_hit,
            tp3_hit,
            highest_price,
            lowest_price
        FROM scanner_trades
        WHERE status = 'OPEN';
        """
    )

    trades = cur.fetchall()

    for trade in trades:

        (
            trade_id,
            symbol,
            entry,
            tp1,
            tp2,
            tp3,
            sl,
            tp1_hit,
            tp2_hit,
            tp3_hit,
            highest_price,
            lowest_price
        ) = trade

        if symbol not in prices:

            continue

        price = prices[symbol]

        highest_price = max(
            highest_price
            or entry,
            price
        )

        lowest_price = min(
            lowest_price
            or entry,
            price
        )

        new_tp1 = (
            tp1_hit
            or price >= tp1
        )

        new_tp2 = (
            tp2_hit
            or price >= tp2
        )

        new_tp3 = (
            tp3_hit
            or price >= tp3
        )

        # If TP3 is hit,
        # TP1 and TP2 were necessarily crossed
        if new_tp3:

            new_tp1 = True
            new_tp2 = True

        if new_tp2:

            new_tp1 = True

        sl_hit = (
            price <= sl
        )

        closed = False
        result_pct = None
        status = "OPEN"

        if new_tp3:

            # 30% at +2%
            # 40% at +4%
            # 30% at +8%
            result_pct = (
                (
                    TP1_PCT
                    / 100
                )
                * SCANNER_TP1
                +
                (
                    TP2_PCT
                    / 100
                )
                * SCANNER_TP2
                +
                (
                    TP3_PCT
                    / 100
                )
                * SCANNER_TP3
            )

            status = "WIN"
            closed = True

        elif sl_hit:

            remaining = 1.0

            realized = 0.0

            if new_tp1:

                realized += (
                    TP1_PCT
                    / 100
                ) * SCANNER_TP1

                remaining -= (
                    TP1_PCT
                    / 100
                )

            if new_tp2:

                realized += (
                    TP2_PCT
                    / 100
                ) * SCANNER_TP2

                remaining -= (
                    TP2_PCT
                    / 100
                )

            result_pct = (
                realized
                - (
                    remaining
                    * SCANNER_SL
                )
            )

            status = (
                "WIN"
                if result_pct > 0
                else "LOSS"
            )

            closed = True

        if closed:

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
                    status = %s,
                    closed_at = NOW()
                WHERE id = %s;
                """,
                (
                    new_tp1,
                    new_tp2,
                    new_tp3,
                    sl_hit,
                    highest_price,
                    lowest_price,
                    price,
                    result_pct,
                    status,
                    trade_id,
                )
            )

        else:

            cur.execute(
                """
                UPDATE scanner_trades
                SET
                    tp1_hit = %s,
                    tp2_hit = %s,
                    tp3_hit = %s,
                    highest_price = %s,
                    lowest_price = %s
                WHERE id = %s;
                """,
                (
                    new_tp1,
                    new_tp2,
                    new_tp3,
                    highest_price,
                    lowest_price,
                    trade_id,
                )
            )

    conn.commit()
    cur.close()
    conn.close()


def simulation_monitor_loop():

    print(
        "SIMULATION MONITOR STARTED",
        flush=True
    )

    while True:

        try:

            update_simulated_trades()

        except Exception as e:

            print(
                "SIM MONITOR ERROR:",
                str(e),
                flush=True
            )

        time.sleep(
            SIM_MONITOR_SEC
        )


def start_simulation_monitor():

    thread = threading.Thread(
        target=
            simulation_monitor_loop,
        daemon=True
    )

    thread.start()


# =========================================================
# DATABASE TEST
# =========================================================

@app.route(
    "/db-test",
    methods=["GET"]
)
def db_test():

    conn = None
    cur = None

    try:

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            "SELECT NOW();"
        )

        now = cur.fetchone()[0]

        return (
            f"DATABASE OK | {now}",
            200
        )

    except Exception as e:

        return (
            f"DATABASE ERROR | {str(e)}",
            500
        )

    finally:

        if cur:
            cur.close()

        if conn:
            conn.close()


# =========================================================
# START SCAN
# =========================================================

@app.route(
    "/scan-now",
    methods=["GET"]
)
def scan_now():

    with scanner_lock:

        if scanner_status[
            "running"
        ]:

            return (
                "SCANNER ALREADY RUNNING | "
                f"{scanner_status['processed']}/"
                f"{scanner_status['total']} | "
                f"{scanner_status['current_symbol']}",
                200
            )

        scanner_status[
            "running"
        ] = True

    thread = threading.Thread(
        target=
            run_market_scan,
        daemon=True
    )

    thread.start()

    return (
        "SCANNER STARTED | "
        "This is simulation only. "
        "No real scanner order will be placed.",
        200
    )


# =========================================================
# SCANNER STATUS
# =========================================================

@app.route(
    "/scanner-status",
    methods=["GET"]
)
def scanner_status_page():

    with scanner_lock:

        status = dict(
            scanner_status
        )

    lines = []

    lines.append(
        "BingX Scanner"
    )

    lines.append(
        "===================="
    )

    lines.append(
        f"Running: "
        f"{status['running']}"
    )

    lines.append(
        f"Progress: "
        f"{status['processed']}/"
        f"{status['total']}"
    )

    lines.append(
        f"Current: "
        f"{status['current_symbol']}"
    )

    lines.append(
        f"Candidates: "
        f"{status['candidates']}"
    )

    lines.append(
        f"New simulated trades: "
        f"{status['new_trades']}"
    )

    lines.append(
        f"Started: "
        f"{status['started_at']}"
    )

    lines.append(
        f"Finished: "
        f"{status['finished_at']}"
    )

    if status[
        "error"
    ]:

        lines.append(
            f"ERROR: "
            f"{status['error']}"
        )

    lines.append("")
    lines.append(
        "TOP RESULTS"
    )

    lines.append(
        "===================="
    )

    for item in status[
        "top_results"
    ]:

        lines.append(
            f"{item['symbol']} | "
            f"Score {item['score']} | "
            f"RSI {item['rsi']:.1f} | "
            f"Volume {item['volume_ratio']:.2f}x | "
            f"Momentum {item['momentum_pct']:.2f}% | "
            f"1H {item.get('trend_1h')}"
        )

    return (
        "<pre>"
        + "\n".join(lines)
        + "</pre>",
        200
    )


# =========================================================
# SCANNER TRADES
# =========================================================

@app.route(
    "/scanner-trades",
    methods=["GET"]
)
def scanner_trades_page():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            id,
            symbol,
            signal_time,
            entry_price,
            score,
            rsi,
            volume_ratio,
            momentum_pct,
            tp1,
            tp2,
            tp3,
            sl,
            tp1_hit,
            tp2_hit,
            tp3_hit,
            sl_hit,
            status,
            result_pct
        FROM scanner_trades
        ORDER BY id DESC
        LIMIT 100;
        """
    )

    rows = cur.fetchall()

    cur.close()
    conn.close()

    lines = []

    lines.append(
        "SIMULATED TRADES"
    )

    lines.append(
        "=================================================="
    )

    for row in rows:

        (
            trade_id,
            symbol,
            signal_time,
            entry,
            score,
            rsi,
            volume_ratio,
            momentum,
            tp1,
            tp2,
            tp3,
            sl,
            tp1_hit,
            tp2_hit,
            tp3_hit,
            sl_hit,
            status,
            result_pct
        ) = row

        lines.append(
            f"#{trade_id} "
            f"{symbol} | "
            f"{status}"
        )

        lines.append(
            f"Time: "
            f"{signal_time}"
        )

        lines.append(
            f"Entry: "
            f"{entry}"
        )

        lines.append(
            f"Score: "
            f"{score} | "
            f"RSI: "
            f"{rsi:.1f} | "
            f"Vol: "
            f"{volume_ratio:.2f}x | "
            f"Momentum: "
            f"{momentum:.2f}%"
        )

        lines.append(
            f"TP1 {tp1}: "
            f"{'YES' if tp1_hit else 'NO'}"
        )

        lines.append(
            f"TP2 {tp2}: "
            f"{'YES' if tp2_hit else 'NO'}"
        )

        lines.append(
            f"TP3 {tp3}: "
            f"{'YES' if tp3_hit else 'NO'}"
        )

        lines.append(
            f"SL {sl}: "
            f"{'YES' if sl_hit else 'NO'}"
        )

        lines.append(
            f"Result: "
            f"{result_pct if result_pct is not None else '-'}%"
        )

        lines.append(
            "--------------------------------------------------"
        )

    return (
        "<pre>"
        + "\n".join(lines)
        + "</pre>",
        200
    )


# =========================================================
# SCANNER STATS
# =========================================================

@app.route(
    "/scanner-stats",
    methods=["GET"]
)
def scanner_stats_page():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*)
        FROM scanner_trades;
        """
    )

    total = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM scanner_trades
        WHERE status = 'OPEN';
        """
    )

    open_count = (
        cur.fetchone()[0]
    )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM scanner_trades
        WHERE status = 'WIN';
        """
    )

    wins = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(*)
        FROM scanner_trades
        WHERE status = 'LOSS';
        """
    )

    losses = cur.fetchone()[0]

    cur.execute(
        """
        SELECT
            COALESCE(
                AVG(result_pct),
                0
            )
        FROM scanner_trades
        WHERE result_pct
        IS NOT NULL;
        """
    )

    avg_result = float(
        cur.fetchone()[0]
        or 0
    )

    cur.execute(
        """
        SELECT
            COALESCE(
                SUM(result_pct),
                0
            )
        FROM scanner_trades
        WHERE result_pct
        IS NOT NULL;
        """
    )

    total_result = float(
        cur.fetchone()[0]
        or 0
    )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM scanner_trades
        WHERE tp1_hit = TRUE;
        """
    )

    tp1_hits = (
        cur.fetchone()[0]
    )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM scanner_trades
        WHERE tp2_hit = TRUE;
        """
    )

    tp2_hits = (
        cur.fetchone()[0]
    )

    cur.execute(
        """
        SELECT COUNT(*)
        FROM scanner_trades
        WHERE tp3_hit = TRUE;
        """
    )

    tp3_hits = (
        cur.fetchone()[0]
    )

    cur.execute(
        """
        SELECT
            CASE
                WHEN score >= 90
                    THEN '90+'
                WHEN score >= 85
                    THEN '85-89'
                WHEN score >= 80
                    THEN '80-84'
                ELSE '75-79'
            END AS bucket,
            COUNT(*) AS total,
            SUM(
                CASE
                    WHEN status = 'WIN'
                    THEN 1
                    ELSE 0
                END
            ) AS wins,
            SUM(
                CASE
                    WHEN status = 'LOSS'
                    THEN 1
                    ELSE 0
                END
            ) AS losses
        FROM scanner_trades
        WHERE status
        IN (
            'WIN',
            'LOSS'
        )
        GROUP BY bucket
        ORDER BY bucket DESC;
        """
    )

    buckets = cur.fetchall()

    cur.close()
    conn.close()

    closed = (
        wins + losses
    )

    win_rate = (
        (
            wins
            / closed
            * 100
        )
        if closed > 0
        else 0
    )

    lines = []

    lines.append(
        "BINGX SCANNER STATISTICS"
    )

    lines.append(
        "=============================="
    )

    lines.append(
        f"Total signals: {total}"
    )

    lines.append(
        f"Open: {open_count}"
    )

    lines.append(
        f"Closed: {closed}"
    )

    lines.append(
        f"Wins: {wins}"
    )

    lines.append(
        f"Losses: {losses}"
    )

    lines.append(
        f"Win rate: "
        f"{win_rate:.2f}%"
    )

    lines.append(
        f"Average result: "
        f"{avg_result:.2f}%"
    )

    lines.append(
        f"Sum result: "
        f"{total_result:.2f}%"
    )

    lines.append("")

    lines.append(
        f"TP1 hits: "
        f"{tp1_hits}"
    )

    lines.append(
        f"TP2 hits: "
        f"{tp2_hits}"
    )

    lines.append(
        f"TP3 hits: "
        f"{tp3_hits}"
    )

    lines.append("")

    lines.append(
        "SCORE WIN RATE"
    )

    lines.append(
        "=============================="
    )

    for (
        bucket,
        bucket_total,
        bucket_wins,
        bucket_losses
    ) in buckets:

        resolved = (
            bucket_wins
            + bucket_losses
        )

        rate = (
            bucket_wins
            / resolved
            * 100
            if resolved > 0
            else 0
        )

        lines.append(
            f"{bucket}: "
            f"{bucket_wins}W/"
            f"{bucket_losses}L | "
            f"{rate:.2f}%"
        )

    return (
        "<pre>"
        + "\n".join(lines)
        + "</pre>",
        200
    )


# =========================================================
# HOME
# =========================================================

@app.route(
    "/",
    methods=["GET"]
)
def home():

    mode = (
        "LIVE"
        if LIVE_TRADING
        else "TEST"
    )

    return (
        "<pre>"
        "LINE BingX Bot\n"
        "========================\n"
        f"LINE trading: {mode}\n"
        f"Leverage: {LEVERAGE}x\n"
        f"Database: "
        f"{'ON' if DATABASE_URL else 'OFF'}\n\n"
        "Scanner: SIMULATION ONLY\n\n"
        "Pages:\n"
        "/db-test\n"
        "/scan-now\n"
        "/scanner-status\n"
        "/scanner-trades\n"
        "/scanner-stats\n"
        "</pre>",
        200
    )


# =========================================================
# LINE WEBHOOK
# =========================================================

@app.route(
    "/webhook",
    methods=["POST"]
)
def webhook():

    body = request.get_data(
        as_text=True
    )

    signature = request.headers.get(
        "X-Line-Signature",
        ""
    )

    if not LINE_CHANNEL_SECRET:

        return (
            "LINE_CHANNEL_SECRET "
            "not set",
            500
        )

    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode(
            "utf-8"
        ),
        body.encode(
            "utf-8"
        ),
        hashlib.sha256
    ).digest()

    expected_signature = (
        base64
        .b64encode(
            digest
        )
        .decode(
            "utf-8"
        )
    )

    if not hmac.compare_digest(
        signature,
        expected_signature
    ):

        abort(400)

    data = (
        request.get_json(
            silent=True
        )
        or {}
    )

    for event in data.get(
        "events",
        []
    ):

        if event.get(
            "type"
        ) != "message":

            continue

        message = event.get(
            "message",
            {}
        )

        if message.get(
            "type"
        ) != "text":

            continue

        text = message.get(
            "text",
            ""
        )

        event_id = event.get(
            "webhookEventId",
            str(
                time.time_ns()
            )
        )

        print(
            "LINE MESSAGE:",
            text,
            flush=True
        )

        signal = parse_signal(
            text
        )

        print(
            "PARSED SIGNAL:",
            signal,
            flush=True
        )

        if not signal[
            "symbol"
        ]:

            print(
                "NOT A SIGNAL",
                flush=True
            )

            continue

        (
            valid,
            error
        ) = validate_signal(
            signal
        )

        if not valid:

            print(
                "SIGNAL REJECTED:",
                error,
                flush=True
            )

            continue

        try:

            (
                entry_result,
                params,
                contract,
                expected_qty
            ) = place_limit_entry(
                signal,
                event_id
            )

            print(
                "LIMIT ENTRY RESULT:",
                json.dumps(
                    entry_result,
                    ensure_ascii=False
                ),
                flush=True
            )

            if entry_result.get(
                "code"
            ) != 0:

                raise RuntimeError(
                    f"Entry failed: "
                    f"{entry_result}"
                )

            if not LIVE_TRADING:

                print(
                    "TEST MODE: "
                    "NO REAL ORDER",
                    flush=True
                )

                continue

            data_result = (
                entry_result.get(
                    "data",
                    {}
                )
            )

            order = data_result.get(
                "order",
                data_result
            )

            order_id = order.get(
                "orderId"
            )

            client_order_id = (
                params.get(
                    "clientOrderId"
                )
            )

            if not order_id:

                raise RuntimeError(
                    "No orderId returned"
                )

            thread = threading.Thread(
                target=
                    monitor_limit_order,

                args=(
                    signal,
                    event_id,
                    order_id,
                    client_order_id,
                    contract
                ),

                daemon=True
            )

            thread.start()

        except Exception as e:

            print(
                "TRADING ERROR:",
                str(e),
                flush=True
            )

    return "OK", 200


# =========================================================
# STARTUP
# =========================================================

try:

    init_database()

except Exception as e:

    print(
        "STARTUP DATABASE ERROR:",
        str(e),
        flush=True
    )


if DATABASE_URL:

    try:

        start_simulation_monitor()

    except Exception as e:

        print(
            "MONITOR START ERROR:",
            str(e),
            flush=True
        )
