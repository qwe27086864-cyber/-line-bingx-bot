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

LIVE_TRADING = os.environ.get(

    "LIVE_TRADING",

    "false"

).strip().lower() == "true"

ORDER_USDT = float(

    os.environ.get("ORDER_USDT", "10")

)

MAX_LIVE_ORDER_USDT = float(

    os.environ.get("MAX_LIVE_ORDER_USDT", "10")

)

TP1_PCT = 35.0

TP2_PCT = 40.0

TP3_PCT = 25.0

LEVERAGE = int(

    os.environ.get("LEVERAGE", "8")

)

BINGX_POSITION_MODE = os.environ.get(

    "BINGX_POSITION_MODE",

    "HEDGE"

).strip().upper()

ENTRY_TIMEOUT_SEC = int(

    os.environ.get("ENTRY_TIMEOUT_SEC", "600")

)

ORDER_POLL_SEC = float(

    os.environ.get("ORDER_POLL_SEC", "2")

)

LINE_MAX_OPEN = 5

BINGX_BASE_URL = "https://open-api.bingx.com"

# =========================================================

# SCANNER V4.3R1 SETTINGS

# =========================================================

SCANNER_ENGINE_VERSION = "V4.3R1"

SCANNER_MIN_TREND_SCORE = 85

SCANNER_MIN_BREAKOUT_SCORE = 85

# 最多每次掃描新增 3 單

SCANNER_MAX_NEW_PER_SCAN = 3

# 30 是最大容量，不是一定要塞滿

SCANNER_MAX_OPEN = 30

SCANNER_COOLDOWN_HOURS = 4

SCANNER_TP1 = 2.0

SCANNER_TP2 = 4.0

SCANNER_TP3 = 8.0

SCANNER_SL = 2.5

SCANNER_API_DELAY = float(

    os.environ.get("SCANNER_API_DELAY", "1.05")

)

SIM_MONITOR_SEC = int(

    os.environ.get("SIM_MONITOR_SEC", "30")

)

# =========================================================

# V4.3 SCORE SETTINGS

# =========================================================

V43_MAIN_SCORE = 90

V43_CONFIRM_SCORE = 85

# V4.3R1 暫時不開放 80-84

V43_ENABLE_80_84 = False

# =========================================================

# V4.3 SIDEWAYS FILTER

# =========================================================

V43_SIDEWAYS_CAUTION = 40

V43_SIDEWAYS_BLOCK = 60

# 橫盤中仍允許真正放量突破

V43_BREAKOUT_EXCEPTION_VOLUME = 2.5

V43_BREAKOUT_EXCEPTION_MOMENTUM = 1.5

# =========================================================

# V4.3 DYNAMIC TIMEOUT

# =========================================================

# 普通 TREND

V43_TIMEOUT_TREND_HOURS = 3.0

# 95+ 強 TREND

V43_TIMEOUT_STRONG_TREND_HOURS = 3.5

# BREAKOUT

V43_TIMEOUT_BREAKOUT_HOURS = 4.0

# 至少持倉 2 小時才開始判斷弱勢提前退出

V43_EARLY_WEAK_MIN_HOURS = 2.0

# 至少同時出現 3 個弱勢條件才提前退出

V43_EARLY_WEAKNESS_COUNT = 3

# =========================================================

# V4.2 LEGACY SETTINGS

# =========================================================

# 舊 V4.2 持倉繼續照原本 4 小時規則跑完

V42_POSITION_TIMEOUT_HOURS = 4.0

# =========================================================

# SIMULATION

# =========================================================

SIM_START_BALANCE = 1000.0

SIM_MARGIN_USDT = 10.0

SIM_LEVERAGE = 8

SIM_FEE_PCT = float(

    os.environ.get("SIM_FEE_PCT", "0.05")

)

SIM_SLIPPAGE_PCT = float(

    os.environ.get("SIM_SLIPPAGE_PCT", "0.03")

)

SIM_DAILY_MAX_LOSS_USDT = float(

    os.environ.get("SIM_DAILY_MAX_LOSS_USDT", "20")

)

SIM_MAX_CONSECUTIVE_LOSSES = int(

    os.environ.get("SIM_MAX_CONSECUTIVE_LOSSES", "4")

)

SIM_PAUSE_HOURS_AFTER_STREAK = int(

    os.environ.get("SIM_PAUSE_HOURS_AFTER_STREAK", "6")

)

SIM_REPLAY_1M_LIMIT = int(

    os.environ.get("SIM_REPLAY_1M_LIMIT", "240")

)

ENABLE_WEB_SIM_MONITOR = os.environ.get(

    "ENABLE_WEB_SIM_MONITOR",

    "false"

).strip().lower() == "true"

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

                strategy VARCHAR(30),

                engine_version VARCHAR(20),

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

                sideways_score INTEGER,

                sideways_state VARCHAR(20),

                resonance BOOLEAN DEFAULT FALSE,

                resonance_source VARCHAR(30),

                management_mode VARCHAR(30)

                    DEFAULT 'SCANNER',

                tp1 DOUBLE PRECISION,

                tp2 DOUBLE PRECISION,

                tp3 DOUBLE PRECISION,

                sl DOUBLE PRECISION,

                status VARCHAR(30)

                    DEFAULT 'OPEN',

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

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            side VARCHAR(10) NOT NULL DEFAULT 'LONG';

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            strategy VARCHAR(30);

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            engine_version VARCHAR(20);

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            trend_score INTEGER;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            breakout_score INTEGER;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            rsi_5m DOUBLE PRECISION;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            momentum_5m DOUBLE PRECISION;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            volatility_pct DOUBLE PRECISION;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            extension_pct DOUBLE PRECISION;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            momentum_pct DOUBLE PRECISION;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            reasons TEXT;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            sideways_score INTEGER;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            sideways_state VARCHAR(20);

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            resonance BOOLEAN DEFAULT FALSE;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            resonance_source VARCHAR(30);

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            management_mode VARCHAR(30)

            DEFAULT 'SCANNER';

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            exit_reason VARCHAR(30);

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            exit_detail TEXT;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            last_checked_at TIMESTAMPTZ;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            net_result_pct DOUBLE PRECISION;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            sim_pnl_usdt DOUBLE PRECISION;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            sim_margin_usdt DOUBLE PRECISION;

            """,

            """

            ALTER TABLE scanner_trades

            ADD COLUMN IF NOT EXISTS

            sim_leverage INTEGER;

            """,

        ]

        for sql in migrations:

            cur.execute(sql)

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

        # 舊的 pre-lock V4.2 分開保存

        cur.execute(

            """

            UPDATE scanner_trades

            SET engine_version = 'V4.2_PRE'

            WHERE engine_version = 'V4.2';

            """

        )

        cur.execute(

            """

            CREATE TABLE IF NOT EXISTS

            line_signal_events (

                id SERIAL PRIMARY KEY,

                event_id VARCHAR(120) UNIQUE,

                symbol VARCHAR(50) NOT NULL,

                side VARCHAR(10) NOT NULL,

                entry_price DOUBLE PRECISION,

                tp1 DOUBLE PRECISION,

                tp2 DOUBLE PRECISION,

                tp3 DOUBLE PRECISION,

                sl DOUBLE PRECISION,

                relation VARCHAR(30) NOT NULL,

                scanner_trade_id INTEGER,

                order_status VARCHAR(30)

                    DEFAULT 'RECEIVED',

                note TEXT,

                created_at TIMESTAMPTZ DEFAULT NOW()

            );

            """

        )

        conn.commit()

        print(

            "DATABASE READY | V4.3R1 MIGRATIONS OK",

            flush=True

        )

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

        headers={

            "User-Agent": "Mozilla/5.0"

        },

    )

    try:

        with urllib.request.urlopen(

            req,

            timeout=20

        ) as response:

            return json.loads(

                response.read().decode("utf-8")

            )

    except urllib.error.HTTPError as e:

        body = e.read().decode(

            "utf-8",

            errors="replace"

        )

        raise RuntimeError(

            f"BingX Public HTTP {e.code}: {body}"

        )

def build_canonical(params):

    return "&".join(

        f"{k}={v}"

        for k, v in sorted(params.items())

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

    params = dict(params or {})

    params["recvWindow"] = 5000

    params["timestamp"] = int(

        time.time() * 1000

    )

    canonical = build_canonical(params)

    signature = hmac.new(

        BINGX_SECRET_KEY.encode("utf-8"),

        canonical.encode("utf-8"),

        hashlib.sha256,

    ).hexdigest()

    signed = (

        canonical +

        "&signature=" +

        signature

    )

    headers = {

        "X-BX-APIKEY": BINGX_API_KEY,

        "Content-Type":

            "application/x-www-form-urlencoded",

    }

    if method == "GET":

        url = (

            BINGX_BASE_URL +

            path +

            "?" +

            signed

        )

        data = None

    else:

        url = BINGX_BASE_URL + path

        data = signed.encode("utf-8")

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

            return json.loads(

                response.read().decode("utf-8")

            )

    except urllib.error.HTTPError as e:

        body = e.read().decode(

            "utf-8",

            errors="replace"

        )

        raise RuntimeError(

            f"BingX HTTP {e.code}: {body}"

        )

# =========================================================

# LINE SIGNAL

# =========================================================

#

# V4.3R1 暫時保留舊 LINE 系統。

# LINE 中文 parser + optional TP4

# 會放在 V4.3R2。

#

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

        r"å¹£ç¨®\s*[:ï¼]\s*([A-Za-z0-9]+)",

        text,

        re.I

    )

    side = re.search(

        r"æ¹å\s*[:ï¼]\s*(å¤|ç©º|LONG|SHORT)",

        text,

        re.I

    )

    entry = re.search(

        r"é²å ´(?:å¹ä½)?\s*[:ï¼]\s*([0-9.]+)",

        text,

        re.I

    )

    tp1 = re.search(

        r"TP1\s*[:ï¼]\s*([0-9.]+)",

        text,

        re.I

    )

    tp2 = re.search(

        r"TP2\s*[:ï¼]\s*([0-9.]+)",

        text,

        re.I

    )

    tp3 = re.search(

        r"TP3\s*[:ï¼]\s*([0-9.]+)",

        text,

        re.I

    )

    sl = re.search(

        r"(?:SL|æ­¢æ)\s*[:ï¼]\s*([0-9.]+)",

        text,

        re.I

    )

    if symbol:

        result["symbol"] = (

            symbol.group(1).upper()

        )

    if side:

        side_text = (

            side.group(1).upper()

        )

        result["side"] = (

            "LONG"

            if side_text in [

                "å¤",

                "LONG"

            ]

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

def validate_signal(signal):

    for key in [

        "symbol",

        "side",

        "entry",

        "tp1",

        "tp2",

        "tp3",

        "sl"

    ]:

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

        return False, "Invalid price"

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

    if signal["side"] == "LONG":

        if not (

            sl <

            entry <

            tp1 <

            tp2 <

            tp3

        ):

            return (

                False,

                "Invalid LONG price order"

            )

    else:

        if not (

            tp3 <

            tp2 <

            tp1 <

            entry <

            sl

        ):

            return (

                False,

                "Invalid SHORT price order"

            )

    if abs(

        TP1_PCT +

        TP2_PCT +

        TP3_PCT -

        100

    ) > 0.0001:

        return (

            False,

            "TP percentages must total 100"

        )

    return True, None

def set_leverage(

    symbol,

    position_side

):

    leverage_side = (

        position_side

        if BINGX_POSITION_MODE == "HEDGE"

        else "BOTH"

    )

    result = bingx_private_request(

        "POST",

        "/openApi/swap/v2/trade/leverage",

        {

            "symbol": symbol,

            "side": leverage_side,

            "leverage": LEVERAGE

        },

    )

    if result.get("code") != 0:

        raise RuntimeError(

            f"Set leverage failed: {result}"

        )

    return result

def get_contract_info(symbol):

    result = bingx_public_request(

        "/openApi/swap/v2/quote/contracts"

    )

    if result.get("code") != 0:

        raise RuntimeError(

            f"Contract query failed: {result}"

        )

    for item in result.get("data", []):

        if item.get("symbol") == symbol:

            return item

    raise RuntimeError(

        f"Contract not found: {symbol}"

    )

def floor_number(

    value,

    precision

):

    factor = 10 ** precision

    return (

        math.floor(

            value * factor

        ) / factor

    )

def format_number(

    value,

    precision

):

    return f"{value:.{precision}f}"

def client_id(

    event_id,

    suffix

):

    raw = (

        str(event_id) +

        "|" +

        str(suffix)

    )

    digest = hashlib.sha256(

        raw.encode("utf-8")

    ).hexdigest()

    return (

        "line" +

        digest[:24] +

        suffix

    )

def build_limit_entry(

    signal,

    event_id

):

    symbol = (

        signal["symbol"] +

        "-USDT"

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

    entry_price = floor_number(

        float(signal["entry"]),

        price_precision

    )

    if entry_price <= 0:

        raise RuntimeError(

            "Invalid entry price"

        )

    quantity = floor_number(

        ORDER_USDT / entry_price,

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

            f"Entry quantity {quantity} "

            f"below minimum {min_qty}"

        )

    notional = (

        quantity *

        entry_price

    )

    if (

        min_usdt > 0

        and notional < min_usdt

    ):

        raise RuntimeError(

            f"Entry value {notional} USDT "

            f"below minimum {min_usdt}"

        )

    if (

        LIVE_TRADING

        and ORDER_USDT >

        MAX_LIVE_ORDER_USDT

    ):

        raise RuntimeError(

            "LIVE ORDER BLOCKED: "

            f"ORDER_USDT={ORDER_USDT} "

            f"> MAX={MAX_LIVE_ORDER_USDT}"

        )

    side = (

        "BUY"

        if signal["side"] == "LONG"

        else "SELL"

    )

    position_side = (

        signal["side"]

        if BINGX_POSITION_MODE == "HEDGE"

        else "BOTH"

    )

    params = {

        "symbol": symbol,

        "side": side,

        "positionSide": position_side,

        "type": "LIMIT",

        "quantity": format_number(

            quantity,

            quantity_precision

        ),

        "price": format_number(

            entry_price,

            price_precision

        ),

        "timeInForce": "GTC",

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

        params["clientOrderId"] = (

            client_order_id

        )

    return bingx_private_request(

        "DELETE",

        "/openApi/swap/v2/trade/order",

        params

    )

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

        if position_side == "LONG"

        else "BUY"

    )

    params = {

        "symbol": symbol,

        "side": close_side,

        "positionSide": (

            position_side

            if BINGX_POSITION_MODE == "HEDGE"

            else "BOTH"

        ),

        "type": order_type,

        "quantity": format_number(

            quantity,

            quantity_precision

        ),

        "stopPrice": format_number(

            stop_price,

            price_precision

        ),

        "workingType": "MARK_PRICE",

    }

    if BINGX_POSITION_MODE == "ONEWAY":

        params["reduceOnly"] = "true"

    return params

def place_tp_sl(

    signal,

    event_id,

    filled_qty,

    contract

):

    symbol = (

        signal["symbol"] +

        "-USDT"

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

        filled_qty *

        TP1_PCT /

        100,

        quantity_precision

    )

    q2 = floor_number(

        filled_qty *

        TP2_PCT /

        100,

        quantity_precision

    )

    q3 = floor_number(

        filled_qty -

        q1 -

        q2,

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

                f"{name} qty {qty} "

                f"below minimum {min_qty}"

            )

        if (

            min_usdt > 0

            and

            qty * trigger_price <

            min_usdt

        ):

            raise RuntimeError(

                f"{name} value "

                f"below minimum {min_usdt}"

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

            price_precision,

        )

        result = bingx_private_request(

            "POST",

            "/openApi/swap/v2/trade/order",

            params

        )

        if result.get("code") != 0:

            raise RuntimeError(

                f"{name} failed: {result}"

            )

        tp_results.append(result)

    sl_params = build_exit_order(

        symbol,

        signal["side"],

        filled_qty,

        float(signal["sl"]),

        "STOP_MARKET",

        event_id,

        "sl",

        quantity_precision,

        price_precision,

    )

    sl_result = bingx_private_request(

        "POST",

        "/openApi/swap/v2/trade/order",

        sl_params

    )

    if sl_result.get("code") != 0:

        raise RuntimeError(

            f"SL failed: {sl_result}"

        )

    return {

        "tp": tp_results,

        "sl": sl_result

    }

def monitor_limit_order(

    signal,

    event_id,

    order_id,

    client_order_id,

    contract

):

    symbol = (

        signal["symbol"] +

        "-USDT"

    )

    start = time.time()

    while True:

        try:

            result = query_order(

                symbol,

                order_id,

                client_order_id

            )

            if result.get("code") != 0:

                raise RuntimeError(

                    str(result)

                )

            order = result.get(

                "data",

                {}

            )

            if (

                isinstance(order, dict)

                and "order" in order

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

                ) or 0

            )

            if status == "FILLED":

                if executed_qty <= 0:

                    raise RuntimeError(

                        "FILLED but executedQty=0"

                    )

                place_tp_sl(

                    signal,

                    event_id,

                    executed_qty,

                    contract

                )

                try:

                    update_line_event_status(

                        event_id,

                        "FILLED"

                    )

                except Exception as db_error:

                    print(

                        "LINE EVENT STATUS ERROR:",

                        str(db_error),

                        flush=True

                    )

                return

            if status in [

                "CANCELED",

                "EXPIRED"

            ]:

                try:

                    update_line_event_status(

                        event_id,

                        status

                    )

                except Exception as db_error:

                    print(

                        "LINE EVENT STATUS ERROR:",

                        str(db_error),

                        flush=True

                    )

                return

            if (

                time.time() - start >=

                ENTRY_TIMEOUT_SEC

            ):

                cancel_order(

                    symbol,

                    order_id,

                    client_order_id

                )

                try:

                    update_line_event_status(

                        event_id,

                        "ENTRY_TIMEOUT"

                    )

                except Exception as db_error:

                    print(

                        "LINE EVENT STATUS ERROR:",

                        str(db_error),

                        flush=True

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

# INDICATORS / KLINES

# =========================================================

def calculate_ema(

    values,

    period

):

    if len(values) < period:

        return None

    ema_value = (

        sum(values[:period]) /

        period

    )

    multiplier = (

        2 /

        (period + 1)

    )

    for price in values[period:]:

        ema_value = (

            (price - ema_value)

            * multiplier

            + ema_value

        )

    return ema_value

def calculate_rsi(

    closes,

    period=14

):

    if len(closes) < period + 1:

        return None

    gains = []

    losses = []

    for i in range(

        1,

        len(closes)

    ):

        change = (

            closes[i] -

            closes[i - 1]

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

        ) /

        period

    )

    avg_loss = (

        sum(

            losses[-period:]

        ) /

        period

    )

    if avg_loss == 0:

        return 100.0

    rs = (

        avg_gain /

        avg_loss

    )

    return (

        100 -

        (

            100 /

            (1 + rs)

        )

    )

def get_klines(

    symbol,

    interval,

    limit=100

):

    result = bingx_public_request(

        "/openApi/swap/v3/quote/klines",

        {

            "symbol": symbol,

            "interval": interval,

            "limit": limit

        },

    )

    if result.get("code") != 0:

        raise RuntimeError(

            f"Kline failed: {result}"

        )

    candles = []

    for item in result.get(

        "data",

        []

    ):

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

        key=lambda x: x["time"]

    )

    return candles

# =========================================================

# V4.3R1 ENTRY ANALYSIS

# =========================================================

def valid_scanner_symbol(

    symbol

):

    symbol = str(

        symbol

    ).upper()

    match = re.fullmatch(

        r"([A-Z0-9]{1,24})-USDT",

        symbol

    )

    if not match:

        return False

    return not (

        match.group(1)

        .endswith("USDT")

    )

def get_all_usdt_contracts():

    result = bingx_public_request(

        "/openApi/swap/v2/quote/contracts"

    )

    if result.get("code") != 0:

        raise RuntimeError(

            f"Contract list failed: {result}"

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

        if valid_scanner_symbol(

            symbol

        ):

            symbols.append(

                symbol

            )

    return sorted(

        set(symbols)

    )

def _fast_direction_pack(

    side,

    price,

    ema9,

    ema20,

    rsi,

    momentum_15m,

    momentum_1h,

    volume_ratio,

    volatility_pct,

    previous_high,

    previous_low

):

    is_long = (

        side == "LONG"

    )

    breakout = (

        price > previous_high

        if is_long

        else price < previous_low

    )

    extension_pct = (

        (

            price - ema20

        ) /

        ema20 *

        100

        if is_long

        else (

            ema20 - price

        ) /

        ema20 *

        100

    )

    directional_m15 = (

        momentum_15m

        if is_long

        else -momentum_15m

    )

    directional_m1h = (

        momentum_1h

        if is_long

        else -momentum_1h

    )

    breakout_score = 0

    breakout_reasons = []

    if volume_ratio >= 1.5:

        breakout_score += 15

    if volume_ratio >= 2.0:

        breakout_score += 15

        breakout_reasons.append(

            f"5m volume {volume_ratio:.2f}x"

        )

    if volume_ratio >= 3.0:

        breakout_score += 10

    if directional_m15 >= 0.8:

        breakout_score += 10

    if directional_m15 >= 1.5:

        breakout_score += 15

        breakout_reasons.append(

            "15m directional momentum "

            f"{directional_m15:.2f}%"

        )

    if directional_m15 >= 3.0:

        breakout_score += 10

    if breakout:

        breakout_score += 20

        breakout_reasons.append(

            "5m 20-bar breakout"

        )

    if is_long:

        if 55 <= rsi <= 85:

            breakout_score += 10

        if rsi > 92:

            breakout_score -= 20

    else:

        if 15 <= rsi <= 45:

            breakout_score += 10

        if rsi < 8:

            breakout_score -= 20

    if volatility_pct >= 2:

        breakout_score += 5

    if volatility_pct >= 4:

        breakout_score += 5

        breakout_reasons.append(

            "volatility expansion "

            f"{volatility_pct:.2f}%"

        )

    if extension_pct > 10:

        breakout_score -= 15

    trend_seed = 0

    trend_reasons = []

    price_ema9_ok = (

        price > ema9

        if is_long

        else price < ema9

    )

    ema_stack_ok = (

        ema9 > ema20

        if is_long

        else ema9 < ema20

    )

    if price_ema9_ok:

        trend_seed += 10

    if ema_stack_ok:

        trend_seed += 15

        trend_reasons.append(

            "5m EMA aligned"

        )

    if directional_m1h > 0.5:

        trend_seed += 10

    if (

        is_long

        and 50 <= rsi <= 75

    ):

        trend_seed += 10

    if (

        not is_long

        and 25 <= rsi <= 50

    ):

        trend_seed += 10

    if volume_ratio >= 1.2:

        trend_seed += 5

    ema_gap_pct = (

        abs(

            ema9 - ema20

        ) /

        ema20 *

        100

        if ema20

        else 0

    )

    return {

        "breakout":

            breakout,

        "extension_pct":

            extension_pct,

        "breakout_score":

            breakout_score,

        "breakout_reasons":

            breakout_reasons,

        "trend_seed":

            trend_seed,

        "trend_reasons":

            trend_reasons,

        "ema_gap_5m_pct":

            ema_gap_pct,

    }

def analyze_fast_5m(

    symbol,

    side_hint=None

):

    candles = get_klines(

        symbol,

        "5m",

        90

    )

    if len(candles) < 60:

        return None

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

    lows = [

        x["low"]

        for x in completed

    ]

    volumes = [

        x["volume"]

        for x in completed

    ]

    price = closes[-1]

    ema9 = calculate_ema(

        closes,

        9

    )

    ema20 = calculate_ema(

        closes,

        20

    )

    rsi = calculate_rsi(

        closes,

        14

    )

    if (

        ema9 is None

        or ema20 is None

        or rsi is None

    ):

        return None

    momentum_15m = (

        (

            price -

            closes[-4]

        ) /

        closes[-4] *

        100

    )

    momentum_1h = (

        (

            price -

            closes[-13]

        ) /

        closes[-13] *

        100

    )

    previous_volumes = (

        volumes[-21:-1]

    )

    avg_volume = (

        sum(previous_volumes)

        /

        len(previous_volumes)

        if previous_volumes

        else 0

    )

    volume_ratio = (

        volumes[-1] /

        avg_volume

        if avg_volume > 0

        else 0

    )

    previous_high = max(

        highs[-21:-1]

    )

    previous_low = min(

        lows[-21:-1]

    )

    recent_high = max(

        highs[-6:]

    )

    recent_low = min(

        lows[-6:]

    )

    volatility_pct = (

        (

            recent_high -

            recent_low

        ) /

        price *

        100

    )

    sides = (

        [side_hint]

        if side_hint

        in [

            "LONG",

            "SHORT"

        ]

        else [

            "LONG",

            "SHORT"

        ]

    )

    candidates = []

    for side in sides:

        pack = _fast_direction_pack(

            side,

            price,

            ema9,

            ema20,

            rsi,

            momentum_15m,

            momentum_1h,

            volume_ratio,

            volatility_pct,

            previous_high,

            previous_low,

        )

        candidates.append({

            "symbol":

                symbol,

            "side":

                side,

            "price":

                price,

            "rsi_5m":

                rsi,

            "volume_ratio_5m":

                volume_ratio,

            "momentum_5m":

                momentum_15m,

            "momentum_1h_fast":

                momentum_1h,

            "volatility_pct":

                volatility_pct,

            "extension_pct":

                pack[

                    "extension_pct"

                ],

            "ema_gap_5m_pct":

                pack[

                    "ema_gap_5m_pct"

                ],

            "breakout_5m":

                pack[

                    "breakout"

                ],

            "breakout_score_fast":

                max(

                    0,

                    pack[

                        "breakout_score"

                    ]

                ),

            "trend_seed":

                max(

                    0,

                    pack[

                        "trend_seed"

                    ]

                ),

            "fast_score":

                max(

                    pack[

                        "trend_seed"

                    ],

                    pack[

                        "breakout_score"

                    ]

                ),

            "breakout_reasons":

                pack[

                    "breakout_reasons"

                ],

            "trend_reasons":

                pack[

                    "trend_reasons"

                ],

        })

    candidates.sort(

        key=lambda x:

            x["fast_score"],

        reverse=True

    )

    return (

        candidates[0]

        if candidates

        else None

    )

def analyze_15m_detail(

    fast

):

    symbol = fast["symbol"]

    side = fast["side"]

    is_long = (

        side == "LONG"

    )

    candles = get_klines(

        symbol,

        "15m",

        100

    )

    if len(candles) < 60:

        return None

    completed = candles[:-1]

    closes = [

        x["close"]

        for x in completed

    ]

    highs = [

        x["high"]

        for x in completed

    ]

    lows = [

        x["low"]

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

    momentum = (

        (

            price -

            closes[-6]

        ) /

        closes[-6] *

        100

    )

    directional_momentum = (

        momentum

        if is_long

        else -momentum

    )

    previous_volumes = (

        volumes[-21:-1]

    )

    avg_volume = (

        sum(previous_volumes)

        /

        len(previous_volumes)

        if previous_volumes

        else 0

    )

    volume_ratio = (

        volumes[-1] /

        avg_volume

        if avg_volume > 0

        else 0

    )

    previous_high = max(

        highs[-21:-1]

    )

    previous_low = min(

        lows[-21:-1]

    )

    breakout_15m = (

        price > previous_high

        if is_long

        else price < previous_low

    )

    extension_15m = (

        (

            price -

            ema20

        ) /

        ema20 *

        100

        if is_long

        else (

            ema20 -

            price

        ) /

        ema20 *

        100

    )

    ema_gap_15m_pct = (

        abs(

            ema20 -

            ema50

        ) /

        ema50 *

        100

        if ema50

        else 0

    )

    trend_score = int(

        fast["trend_seed"]

    )

    trend_reasons = list(

        fast["trend_reasons"]

    )

    price_ema20_ok = (

        price > ema20

        if is_long

        else price < ema20

    )

    ema_stack_ok = (

        ema20 > ema50

        if is_long

        else ema20 < ema50

    )

    if price_ema20_ok:

        trend_score += 15

        trend_reasons.append(

            "15m price/EMA20 aligned"

        )

    if ema_stack_ok:

        trend_score += 20

        trend_reasons.append(

            "15m EMA20/EMA50 aligned"

        )

    if is_long:

        if 50 <= rsi <= 68:

            trend_score += 15

            trend_reasons.append(

                f"15m RSI={rsi:.1f}"

            )

        elif 68 < rsi < 78:

            trend_score += 5

    else:

        if 32 <= rsi <= 50:

            trend_score += 15

            trend_reasons.append(

                f"15m RSI={rsi:.1f}"

            )

        elif 22 < rsi < 32:

            trend_score += 5

    if directional_momentum > 0.3:

        trend_score += 5

    if directional_momentum > 1:

        trend_score += 10

        trend_reasons.append(

            "15m directional momentum "

            f"{directional_momentum:.2f}%"

        )

    if volume_ratio >= 1.3:

        trend_score += 10

    if volume_ratio >= 1.8:

        trend_score += 5

        trend_reasons.append(

            f"15m volume {volume_ratio:.2f}x"

        )

    if breakout_15m:

        trend_score += 15

        trend_reasons.append(

            "15m breakout"

        )

    trend_blocked = False

    if (

        is_long

        and rsi >= 78

    ):

        trend_blocked = True

        trend_reasons.append(

            "TREND_BLOCK: LONG RSI too hot"

        )

    if (

        not is_long

        and rsi <= 22

    ):

        trend_blocked = True

        trend_reasons.append(

            "TREND_BLOCK: SHORT RSI too cold"

        )

    if extension_15m >= 8:

        trend_blocked = True

        trend_reasons.append(

            "TREND_BLOCK: "

            "15m extension too large"

        )

    breakout_score = int(

        fast[

            "breakout_score_fast"

        ]

    )

    breakout_reasons = list(

        fast[

            "breakout_reasons"

        ]

    )

    if volume_ratio >= 1.5:

        breakout_score += 10

    if breakout_15m:

        breakout_score += 10

        breakout_reasons.append(

            "15m synchronized breakout"

        )

    if directional_momentum > 1:

        breakout_score += 5

    if price_ema20_ok:

        breakout_score += 5

    breakout_blocked = False

    if (

        is_long

        and fast["rsi_5m"] > 90

    ):

        breakout_blocked = True

        breakout_reasons.append(

            "BREAKOUT_BLOCK: "

            "5m RSI>90"

        )

    if (

        not is_long

        and fast["rsi_5m"] < 10

    ):

        breakout_blocked = True

        breakout_reasons.append(

            "BREAKOUT_BLOCK: "

            "5m RSI<10"

        )

    if fast["extension_pct"] > 9:

        breakout_blocked = True

        breakout_reasons.append(

            "BREAKOUT_BLOCK: "

            "5m extension too large"

        )

    if (

        fast[

            "volume_ratio_5m"

        ] < 1.8

    ):

        breakout_blocked = True

    directional_fast_momentum = (

        fast[

            "momentum_5m"

        ]

        if is_long

        else -fast[

            "momentum_5m"

        ]

    )

    if directional_fast_momentum < 1.0:

        breakout_blocked = True

    if not fast["breakout_5m"]:

        breakout_blocked = True

    result = dict(fast)

    result.update({

        "price":

            price,

        "rsi":

            rsi,

        "volume_ratio":

            volume_ratio,

        "momentum_pct":

            momentum,

        "breakout":

            (

                fast["breakout_5m"]

                or breakout_15m

            ),

        "breakout_15m":

            breakout_15m,

        "ema_gap_15m_pct":

            ema_gap_15m_pct,

        "trend_15m":

            (

                "UP"

                if (

                    is_long

                    and price > ema20

                    and ema20 > ema50

                )

                else "DOWN"

                if (

                    not is_long

                    and price < ema20

                    and ema20 < ema50

                )

                else "MIXED"

            ),

        "trend_score":

            max(

                0,

                min(

                    100,

                    trend_score

                )

            ),

        "breakout_score":

            max(

                0,

                min(

                    100,

                    breakout_score

                )

            ),

        "trend_blocked":

            trend_blocked,

        "breakout_blocked":

            breakout_blocked,

        "trend_reasons_final":

            trend_reasons,

        "breakout_reasons_final":

            breakout_reasons,

        "extension_15m":

            extension_15m,

    })

    return result

def confirm_1h(

    result

):

    candles = get_klines(

        result["symbol"],

        "1h",

        100

    )

    if len(candles) < 60:

        result["trend_1h"] = (

            "UNKNOWN"

        )

        result[

            "ema_gap_1h_pct"

        ] = None

        return result

    closes = [

        x["close"]

        for x in candles[:-1]

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

        result["trend_1h"] = (

            "UNKNOWN"

        )

        result[

            "ema_gap_1h_pct"

        ] = None

        return result

    result[

        "ema_gap_1h_pct"

    ] = (

        abs(

            ema20 -

            ema50

        ) /

        ema50 *

        100

        if ema50

        else None

    )

    is_long = (

        result["side"] ==

        "LONG"

    )

    if is_long:

        if (

            price > ema20

            and ema20 > ema50

        ):

            result[

                "trend_1h"

            ] = "UP"

            result[

                "trend_score"

            ] += 15

            result[

                "trend_reasons_final"

            ].append(

                "1h bullish confirmation"

            )

            result[

                "breakout_score"

            ] += 5

        elif price > ema20:

            result[

                "trend_1h"

            ] = "WEAK_UP"

            result[

                "trend_score"

            ] += 5

        else:

            result[

                "trend_1h"

            ] = "DOWN"

            result[

                "trend_score"

            ] -= 10

    else:

        if (

            price < ema20

            and ema20 < ema50

        ):

            result[

                "trend_1h"

            ] = "DOWN"

            result[

                "trend_score"

            ] += 15

            result[

                "trend_reasons_final"

            ].append(

                "1h bearish confirmation"

            )

            result[

                "breakout_score"

            ] += 5

        elif price < ema20:

            result[

                "trend_1h"

            ] = "WEAK_DOWN"

            result[

                "trend_score"

            ] += 5

        else:

            result[

                "trend_1h"

            ] = "UP"

            result[

                "trend_score"

            ] -= 10

    result[

        "trend_score"

    ] = max(

        0,

        min(

            100,

            int(

                result[

                    "trend_score"

                ]

            )

        )

    )

    result[

        "breakout_score"

    ] = max(

        0,

        min(

            100,

            int(

                result[

                    "breakout_score"

                ]

            )

        )

    )

    return result

# =========================================================

# V4.3 SIDEWAYS FILTER

# =========================================================

def apply_v43_sideways_filter(

    result

):

    sideways_score = 0

    sideways_reasons = []

    # 5m EMA9 / EMA20 太接近

    if (

        result.get(

            "ema_gap_5m_pct",

            999

        ) < 0.25

    ):

        sideways_score += 30

        sideways_reasons.append(

            "5m EMA compression"

        )

    # 15m 動能太弱

    if (

        abs(

            float(

                result.get(

                    "momentum_pct",

                    0

                ) or 0

            )

        ) < 0.40

    ):

        sideways_score += 25

        sideways_reasons.append(

            "15m momentum weak"

        )

    # 最近 5m 波動太小

    if (

        float(

            result.get(

                "volatility_pct",

                0

            ) or 0

        ) < 1.20

    ):

        sideways_score += 20

        sideways_reasons.append(

            "5m volatility low"

        )

    # 5m 量能弱

    if (

        float(

            result.get(

                "volume_ratio_5m",

                0

            ) or 0

        ) < 1.20

    ):

        sideways_score += 15

        sideways_reasons.append(

            "5m volume weak"

        )

    # 1h 無資料

    if (

        result.get(

            "trend_1h"

        ) == "UNKNOWN"

    ):

        sideways_score += 10

        sideways_reasons.append(

            "1h direction unknown"

        )

    else:

        ema_gap_1h = result.get(

            "ema_gap_1h_pct"

        )

        # FINAL 修正版：

        # 0.0 不會再被錯誤轉成 999

        if (

            ema_gap_1h is not None

            and float(

                ema_gap_1h

            ) < 0.35

        ):

            sideways_score += 10

            sideways_reasons.append(

                "1h EMA compression"

            )

    if (

        sideways_score >=

        V43_SIDEWAYS_BLOCK

    ):

        sideways_state = "BLOCK"

    elif (

        sideways_score >=

        V43_SIDEWAYS_CAUTION

    ):

        sideways_state = "CAUTION"

    else:

        sideways_state = "NORMAL"

    is_long = (

        result.get("side") ==

        "LONG"

    )

    directional_momentum = (

        float(

            result.get(

                "momentum_5m",

                0

            ) or 0

        )

        if is_long

        else -float(

            result.get(

                "momentum_5m",

                0

            ) or 0

        )

    )

    # 真突破例外

    breakout_exception = (

        result.get(

            "breakout_5m",

            False

        )

        and float(

            result.get(

                "volume_ratio_5m",

                0

            ) or 0

        ) >= (

            V43_BREAKOUT_EXCEPTION_VOLUME

        )

        and directional_momentum >= (

            V43_BREAKOUT_EXCEPTION_MOMENTUM

        )

    )

    result[

        "sideways_score"

    ] = sideways_score

    result[

        "sideways_state"

    ] = sideways_state

    result[

        "sideways_reasons"

    ] = sideways_reasons

    result[

        "breakout_exception"

    ] = breakout_exception

    return result

# =========================================================

# V4.3 SCORE TIERS

# =========================================================

def _score_tier_allows(

    result,

    strategy

):

    score = (

        result["trend_score"]

        if strategy == "TREND"

        else result[

            "breakout_score"

        ]

    )

    if score >= V43_MAIN_SCORE:

        return True

    side = result["side"]

    is_long = (

        side == "LONG"

    )

    directional_momentum = (

        result["momentum_5m"]

        if is_long

        else -result[

            "momentum_5m"

        ]

    )

    aligned_1h = (

        result.get(

            "trend_1h"

        )

        in [

            "UP",

            "WEAK_UP"

        ]

        if is_long

        else result.get(

            "trend_1h"

        )

        in [

            "DOWN",

            "WEAK_DOWN"

        ]

    )

    if score >= V43_CONFIRM_SCORE:

        if strategy == "BREAKOUT":

            return (

                aligned_1h

                and result.get(

                    "breakout",

                    False

                )

                and result.get(

                    "volume_ratio_5m",

                    0

                ) >= 2.0

                and

                directional_momentum

                >= 1.2

            )

        aligned_15m = (

            result.get(

                "trend_15m"

            ) == "UP"

            if is_long

            else result.get(

                "trend_15m"

            ) == "DOWN"

        )

        return (

            aligned_1h

            and aligned_15m

            and result.get(

                "volume_ratio",

                0

            ) >= 1.3

            and

            directional_momentum

            >= 0.6

        )

    if (

        80 <= score <= 84

        and V43_ENABLE_80_84

    ):

        strong_1h = (

            result.get(

                "trend_1h"

            ) == "UP"

            if is_long

            else result.get(

                "trend_1h"

            ) == "DOWN"

        )

        return (

            strategy == "BREAKOUT"

            and strong_1h

            and result.get(

                "breakout",

                False

            )

            and result.get(

                "volume_ratio_5m",

                0

            ) >= 2.5

            and

            directional_momentum

            >= 1.5

        )

    return False

# =========================================================

# V4.3 STRATEGY SELECTION

# =========================================================

def choose_strategy(

    result

):

    result = (

        apply_v43_sideways_filter(

            result

        )

    )

    is_long = (

        result["side"] ==

        "LONG"

    )

    trend_alignment_ok = (

        result.get(

            "trend_1h"

        )

        in [

            "UP",

            "WEAK_UP"

        ]

        if is_long

        else result.get(

            "trend_1h"

        )

        in [

            "DOWN",

            "WEAK_DOWN"

        ]

    )

    trend_ok = (

        not result.get(

            "trend_blocked",

            True

        )

        and result[

            "trend_score"

        ] >= SCANNER_MIN_TREND_SCORE

        and trend_alignment_ok

        and _score_tier_allows(

            result,

            "TREND"

        )

    )

    breakout_ok = (

        not result.get(

            "breakout_blocked",

            True

        )

        and result[

            "breakout_score"

        ] >= SCANNER_MIN_BREAKOUT_SCORE

        and _score_tier_allows(

            result,

            "BREAKOUT"

        )

    )

    # -----------------------------------------------------

    # SIDEWAYS BLOCK

    # -----------------------------------------------------

    if (

        result[

            "sideways_state"

        ] == "BLOCK"

    ):

        # 橫盤 TREND 直接擋

        trend_ok = False

        # 只有真正強突破例外才放行

        if not result.get(

            "breakout_exception",

            False

        ):

            breakout_ok = False

    # -----------------------------------------------------

    # SIDEWAYS CAUTION

    # -----------------------------------------------------

    elif (

        result[

            "sideways_state"

        ] == "CAUTION"

    ):

        # CAUTION 中 TREND 至少 90

        if (

            result[

                "trend_score"

            ] < V43_MAIN_SCORE

        ):

            trend_ok = False

        # BREAKOUT 至少 90

        # 或符合強突破例外

        if (

            result[

                "breakout_score"

            ] < V43_MAIN_SCORE

            and not result.get(

                "breakout_exception",

                False

            )

        ):

            breakout_ok = False

    # -----------------------------------------------------

    # SHORT TREND EXTRA FILTER

    # -----------------------------------------------------

    if (

        not is_long

        and trend_ok

    ):

        directional_fast = (

            -float(

                result.get(

                    "momentum_5m",

                    0

                ) or 0

            )

        )

        short_trend_extra_ok = (

            result[

                "trend_score"

            ] >= 90

            and result.get(

                "trend_15m"

            ) == "DOWN"

            and result.get(

                "trend_1h"

            ) == "DOWN"

            and float(

                result.get(

                    "volume_ratio",

                    0

                ) or 0

            ) >= 1.3

            and

            directional_fast

            >= 0.8

        )

        if not short_trend_extra_ok:

            trend_ok = False

    # -----------------------------------------------------

    # CHOOSE TREND / BREAKOUT

    # -----------------------------------------------------

    if (

        trend_ok

        and breakout_ok

    ):

        strategy = (

            "BREAKOUT"

            if result[

                "breakout_score"

            ] >= result[

                "trend_score"

            ]

            else "TREND"

        )

    elif breakout_ok:

        strategy = "BREAKOUT"

    elif trend_ok:

        strategy = "TREND"

    else:

        return None

    result[

        "strategy"

    ] = strategy

    if strategy == "TREND":

        result[

            "score"

        ] = result[

            "trend_score"

        ]

        result[

            "reasons"

        ] = list(

            result[

                "trend_reasons_final"

            ]

        )

    else:

        result[

            "score"

        ] = result[

            "breakout_score"

        ]

        result[

            "reasons"

        ] = list(

            result[

                "breakout_reasons_final"

            ]

        )

    result[

        "reasons"

    ].append(

        "SIDEWAYS="

        f"{result['sideways_score']} "

        f"{result['sideways_state']}"

    )

    if result.get(

        "breakout_exception"

    ):

        result[

            "reasons"

        ].append(

            "BREAKOUT_EXCEPTION"

        )

    return result
    # =========================================================

# SCANNER DB / RISK

# =========================================================

def scanner_open_count(side=None):

    conn = get_db_connection()

    cur = conn.cursor()

    if side in ("LONG", "SHORT"):

        cur.execute(

            """

            SELECT COUNT(*)

            FROM scanner_trades

            WHERE status = 'OPEN'

              AND engine_version = %s

              AND strategy IN ('TREND', 'BREAKOUT')

              AND side = %s;

            """,

            (

                SCANNER_ENGINE_VERSION,

                side

            ),

        )

    else:

        cur.execute(

            """

            SELECT COUNT(*)

            FROM scanner_trades

            WHERE status = 'OPEN'

              AND engine_version = %s

              AND strategy IN ('TREND', 'BREAKOUT');

            """,

            (

                SCANNER_ENGINE_VERSION,

            ),

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

          AND engine_version = %s

          AND strategy IN ('TREND', 'BREAKOUT')

          AND (

              status = 'OPEN'

              OR signal_time >

                 NOW() - (%s * INTERVAL '1 hour')

          )

        ORDER BY signal_time DESC

        LIMIT 1;

        """,

        (

            symbol,

            SCANNER_ENGINE_VERSION,

            SCANNER_COOLDOWN_HOURS

        ),

    )

    found = cur.fetchone()

    cur.close()

    conn.close()

    return found is None

def get_engine_closed_pnl(

    engine_version=SCANNER_ENGINE_VERSION

):

    conn = get_db_connection()

    cur = conn.cursor()

    cur.execute(

        """

        SELECT

            COALESCE(

                SUM(sim_pnl_usdt),

                0

            )

        FROM scanner_trades

        WHERE engine_version = %s

          AND status IN ('WIN', 'LOSS')

          AND sim_pnl_usdt IS NOT NULL;

        """,

        (

            engine_version,

        ),

    )

    pnl = float(

        cur.fetchone()[0] or 0

    )

    cur.close()

    conn.close()

    return pnl

def get_v3_closed_pnl():

    return get_engine_closed_pnl(

        "V3"

    )

def scanner_risk_allows_new_trade():

    conn = get_db_connection()

    cur = conn.cursor()

    cur.execute(

        """

        SELECT

            COALESCE(

                SUM(sim_pnl_usdt),

                0

            )

        FROM scanner_trades

        WHERE engine_version = %s

          AND status IN ('WIN', 'LOSS')

          AND closed_at >=

              DATE_TRUNC(

                  'day',

                  NOW()

              );

        """,

        (

            SCANNER_ENGINE_VERSION,

        ),

    )

    daily_pnl = float(

        cur.fetchone()[0] or 0

    )

    if (

        daily_pnl <=

        -abs(

            SIM_DAILY_MAX_LOSS_USDT

        )

    ):

        cur.close()

        conn.close()

        return (

            False,

            f"DAILY_LOSS_LIMIT "

            f"{daily_pnl:.2f}U"

        )

    cur.execute(

        """

        SELECT

            status,

            closed_at

        FROM scanner_trades

        WHERE engine_version = %s

          AND status IN ('WIN', 'LOSS')

        ORDER BY

            closed_at DESC,

            id DESC

        LIMIT %s;

        """,

        (

            SCANNER_ENGINE_VERSION,

            max(

                SIM_MAX_CONSECUTIVE_LOSSES,

                1

            )

        ),

    )

    rows = cur.fetchall()

    streak = 0

    most_recent_loss_time = None

    for (

        status,

        closed_at

    ) in rows:

        if status == "LOSS":

            streak += 1

            if (

                most_recent_loss_time

                is None

            ):

                most_recent_loss_time = (

                    closed_at

                )

        else:

            break

    if (

        streak >=

        SIM_MAX_CONSECUTIVE_LOSSES

        and

        most_recent_loss_time

        is not None

    ):

        cur.execute(

            """

            SELECT NOW() - %s;

            """,

            (

                most_recent_loss_time,

            ),

        )

        elapsed = (

            cur.fetchone()[0]

        )

        if (

            elapsed.total_seconds()

            <

            SIM_PAUSE_HOURS_AFTER_STREAK

            * 3600

        ):

            cur.close()

            conn.close()

            return (

                False,

                f"LOSS_STREAK_PAUSE "

                f"{streak}"

            )

    cur.close()

    conn.close()

    balance = (

        SIM_START_BALANCE

        +

        get_engine_closed_pnl()

    )

    reserved = (

        scanner_open_count()

        *

        SIM_MARGIN_USDT

    )

    free_balance = (

        balance -

        reserved

    )

    if (

        free_balance <

        SIM_MARGIN_USDT

    ):

        return (

            False,

            "INSUFFICIENT_SIM_BALANCE "

            f"{free_balance:.2f}U"

        )

    return True, "OK"

# =========================================================

# LINE / SCANNER RELATION

# =========================================================

def register_line_signal(

    signal,

    event_id

):

    symbol = (

        signal["symbol"] +

        "-USDT"

    )

    side = signal["side"]

    conn = get_db_connection()

    cur = conn.cursor()

    try:

        cur.execute(

            """

            SELECT

                id,

                side

            FROM scanner_trades

            WHERE engine_version = %s

              AND status = 'OPEN'

              AND strategy IN (

                  'TREND',

                  'BREAKOUT'

              )

              AND symbol = %s

            ORDER BY

                signal_time DESC,

                id DESC

            LIMIT 1;

            """,

            (

                SCANNER_ENGINE_VERSION,

                symbol

            ),

        )

        row = cur.fetchone()

        relation = "LINE_ONLY"

        scanner_trade_id = None

        note = ""

        if row:

            scanner_trade_id = int(

                row[0]

            )

            scanner_side = str(

                row[1] or "LONG"

            ).upper()

            if (

                scanner_side ==

                side

            ):

                relation = "RESONANCE"

                note = (

                    "Same symbol and "

                    "same direction; "

                    "LINE TP/SL takes priority."

                )

                cur.execute(

                    """

                    UPDATE scanner_trades

                    SET

                        resonance = TRUE,

                        resonance_source =

                            'MAX_CRYPTO',

                        management_mode =

                            'LINE_TPSL',

                        tp1 = %s,

                        tp2 = %s,

                        tp3 = %s,

                        sl = %s

                    WHERE id = %s

                      AND status = 'OPEN';

                    """,

                    (

                        float(

                            signal["tp1"]

                        ),

                        float(

                            signal["tp2"]

                        ),

                        float(

                            signal["tp3"]

                        ),

                        float(

                            signal["sl"]

                        ),

                        scanner_trade_id,

                    ),

                )

            else:

                relation = "OPPOSITE"

                note = (

                    "Same symbol but "

                    "opposite direction; "

                    "scanner direction kept. "

                    "LINE signal recorded only."

                )

        cur.execute(

            """

            INSERT INTO

            line_signal_events (

                event_id,

                symbol,

                side,

                entry_price,

                tp1,

                tp2,

                tp3,

                sl,

                relation,

                scanner_trade_id,

                order_status,

                note

            )

            VALUES (

                %s,

                %s,

                %s,

                %s,

                %s,

                %s,

                %s,

                %s,

                %s,

                %s,

                'RECEIVED',

                %s

            )

            ON CONFLICT (event_id)

            DO NOTHING;

            """,

            (

                str(event_id),

                symbol,

                side,

                float(

                    signal["entry"]

                ),

                float(

                    signal["tp1"]

                ),

                float(

                    signal["tp2"]

                ),

                float(

                    signal["tp3"]

                ),

                float(

                    signal["sl"]

                ),

                relation,

                scanner_trade_id,

                note,

            ),

        )

        conn.commit()

        return (

            relation,

            scanner_trade_id

        )

    finally:

        cur.close()

        conn.close()

def update_line_event_status(

    event_id,

    status,

    note=None

):

    conn = get_db_connection()

    cur = conn.cursor()

    try:

        cur.execute(

            """

            UPDATE line_signal_events

            SET

                order_status = %s,

                note =

                    COALESCE(

                        %s,

                        note

                    )

            WHERE event_id = %s;

            """,

            (

                status,

                note,

                str(event_id)

            ),

        )

        conn.commit()

    finally:

        cur.close()

        conn.close()

def get_live_open_position_count():

    if not LIVE_TRADING:

        return 0

    result = bingx_private_request(

        "GET",

        "/openApi/swap/v2/user/positions",

        {},

    )

    if result.get("code") != 0:

        raise RuntimeError(

            "Position query failed: "

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

        data = data.get(

            "positions",

            data.get(

                "data",

                [data]

            )

        )

    if not isinstance(

        data,

        list

    ):

        data = []

    count = 0

    for item in data:

        try:

            qty = item.get(

                "positionAmt",

                item.get(

                    "positionAmount",

                    item.get(

                        "availableAmt",

                        0

                    )

                )

            )

            if (

                abs(

                    float(

                        qty or 0

                    )

                ) > 0

            ):

                count += 1

        except Exception:

            continue

    return count

# =========================================================

# CREATE V4.3 SIM TRADE

# =========================================================

def create_simulated_trade(

    result

):

    entry = float(

        result["price"]

    )

    side = result.get(

        "side",

        "LONG"

    ).upper()

    if side == "LONG":

        tp1 = (

            entry *

            (

                1 +

                SCANNER_TP1 /

                100

            )

        )

        tp2 = (

            entry *

            (

                1 +

                SCANNER_TP2 /

                100

            )

        )

        tp3 = (

            entry *

            (

                1 +

                SCANNER_TP3 /

                100

            )

        )

        sl = (

            entry *

            (

                1 -

                SCANNER_SL /

                100

            )

        )

    else:

        tp1 = (

            entry *

            (

                1 -

                SCANNER_TP1 /

                100

            )

        )

        tp2 = (

            entry *

            (

                1 -

                SCANNER_TP2 /

                100

            )

        )

        tp3 = (

            entry *

            (

                1 -

                SCANNER_TP3 /

                100

            )

        )

        sl = (

            entry *

            (

                1 +

                SCANNER_SL /

                100

            )

        )

    conn = get_db_connection()

    cur = conn.cursor()

    cur.execute(

        """

        INSERT INTO scanner_trades (

            symbol,

            side,

            strategy,

            engine_version,

            entry_price,

            score,

            trend_score,

            breakout_score,

            rsi,

            rsi_5m,

            volume_ratio,

            momentum_pct,

            momentum_5m,

            volatility_pct,

            extension_pct,

            trend_15m,

            trend_1h,

            breakout,

            reasons,

            sideways_score,

            sideways_state,

            tp1,

            tp2,

            tp3,

            sl,

            highest_price,

            lowest_price,

            sim_margin_usdt,

            sim_leverage,

            last_checked_at

        )

        VALUES (

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            %s,

            NOW()

        )

        RETURNING id;

        """,

        (

            result["symbol"],

            side,

            result["strategy"],

            SCANNER_ENGINE_VERSION,

            entry,

            result["score"],

            result["trend_score"],

            result["breakout_score"],

            result["rsi"],

            result["rsi_5m"],

            result["volume_ratio"],

            result["momentum_pct"],

            result["momentum_5m"],

            result["volatility_pct"],

            result["extension_pct"],

            result["trend_15m"],

            result.get(

                "trend_1h",

                "UNKNOWN"

            ),

            result["breakout"],

            json.dumps(

                result["reasons"],

                ensure_ascii=False

            ),

            result.get(

                "sideways_score",

                0

            ),

            result.get(

                "sideways_state",

                "UNKNOWN"

            ),

            tp1,

            tp2,

            tp3,

            sl,

            entry,

            entry,

            SIM_MARGIN_USDT,

            SIM_LEVERAGE,

        )

    )

    trade_id = (

        cur.fetchone()[0]

    )

    conn.commit()

    cur.close()

    conn.close()

    print(

        f"SIMULATED "

        f"{SCANNER_ENGINE_VERSION} "

        f"TRADE:",

        trade_id,

        side,

        result["strategy"],

        result["symbol"],

        result["score"],

        "| sideways",

        result.get(

            "sideways_score",

            0

        ),

        result.get(

            "sideways_state",

            ""

        ),

        flush=True,

    )

    return trade_id

# =========================================================

# PRICE / RESULT HELPERS

# =========================================================

def get_all_prices():

    result = bingx_public_request(

        "/openApi/swap/v1/ticker/price"

    )

    if result.get("code") != 0:

        raise RuntimeError(

            f"Price query failed: {result}"

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

            prices[

                item["symbol"]

            ] = float(

                item["price"]

            )

        except Exception:

            continue

    return prices

def estimated_round_trip_cost_pct():

    return (

        2.0 *

        (

            SIM_FEE_PCT +

            SIM_SLIPPAGE_PCT

        )

    )

def calculate_net_result(

    gross_result_pct,

    margin_usdt=None,

    leverage=None

):

    margin_usdt = float(

        margin_usdt

        or SIM_MARGIN_USDT

    )

    leverage = int(

        leverage

        or SIM_LEVERAGE

    )

    net_result_pct = (

        gross_result_pct -

        estimated_round_trip_cost_pct()

    )

    pnl_usdt = (

        margin_usdt *

        leverage *

        net_result_pct /

        100.0

    )

    return (

        net_result_pct,

        pnl_usdt

    )

def side_return_pct(

    side,

    entry,

    exit_price

):

    if side == "SHORT":

        return (

            (

                entry -

                exit_price

            ) /

            entry *

            100

        )

    return (

        (

            exit_price -

            entry

        ) /

        entry *

        100

    )

def gross_result_for_stop_levels(

    side,

    entry,

    tp1,

    tp2,

    tp1_hit,

    tp2_hit,

    stop_price

):

    realized = 0.0

    remaining = 1.0

    if tp1_hit:

        realized += (

            TP1_PCT /

            100

        ) * side_return_pct(

            side,

            entry,

            tp1

        )

        remaining -= (

            TP1_PCT /

            100

        )

    if tp2_hit:

        realized += (

            TP2_PCT /

            100

        ) * side_return_pct(

            side,

            entry,

            tp2

        )

        remaining -= (

            TP2_PCT /

            100

        )

    return (

        realized

        +

        remaining

        *

        side_return_pct(

            side,

            entry,

            stop_price

        )

    )

def full_tp3_gross_result_levels(

    side,

    entry,

    tp1,

    tp2,

    tp3

):

    return (

        (

            TP1_PCT /

            100

        )

        *

        side_return_pct(

            side,

            entry,

            tp1

        )

        +

        (

            TP2_PCT /

            100

        )

        *

        side_return_pct(

            side,

            entry,

            tp2

        )

        +

        (

            TP3_PCT /

            100

        )

        *

        side_return_pct(

            side,

            entry,

            tp3

        )

    )

def gross_result_for_stop(

    tp1_hit,

    tp2_hit,

    stop_return_pct

):

    realized = 0.0

    remaining = 1.0

    if tp1_hit:

        realized += (

            TP1_PCT /

            100

        ) * SCANNER_TP1

        remaining -= (

            TP1_PCT /

            100

        )

    if tp2_hit:

        realized += (

            TP2_PCT /

            100

        ) * SCANNER_TP2

        remaining -= (

            TP2_PCT /

            100

        )

    return (

        realized

        +

        remaining

        *

        stop_return_pct

    )

def full_tp3_gross_result():

    return (

        (

            TP1_PCT /

            100

        ) * SCANNER_TP1

        +

        (

            TP2_PCT /

            100

        ) * SCANNER_TP2

        +

        (

            TP3_PCT /

            100

        ) * SCANNER_TP3

    )

def finalize_trade(

    cur,

    trade_id,

    exit_price,

    gross_result_pct,

    exit_reason,

    tp1_hit,

    tp2_hit,

    tp3_hit,

    sl_hit,

    highest_price,

    lowest_price,

    margin_usdt,

    leverage,

    exit_detail=""

):

    (

        net_result_pct,

        pnl_usdt

    ) = calculate_net_result(

        gross_result_pct,

        margin_usdt,

        leverage

    )

    status = (

        "WIN"

        if net_result_pct > 0

        else "LOSS"

    )

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

            tp1_hit,

            tp2_hit,

            tp3_hit,

            sl_hit,

            highest_price,

            lowest_price,

            exit_price,

            gross_result_pct,

            net_result_pct,

            pnl_usdt,

            status,

            exit_reason,

            exit_detail,

            trade_id,

        ),

    )

    print(

        "TRADE CLOSED:",

        trade_id,

        exit_reason,

        f"gross="

        f"{gross_result_pct:.2f}%",

        f"net="

        f"{net_result_pct:.2f}%",

        f"pnl="

        f"{pnl_usdt:.4f}U",

        flush=True,

    )

def finalize_v3_trade(

    *args,

    **kwargs

):

    return finalize_trade(

        *args,

        **kwargs

    )

# =========================================================

# V2 LEGACY MONITOR

# =========================================================

def update_v2_trade_current_price(

    cur,

    row,

    current_price

):

    (

        trade_id,

        symbol,

        side,

        engine_version,

        signal_time,

        entry,

        tp1,

        tp2,

        tp3,

        sl,

        tp1_hit,

        tp2_hit,

        tp3_hit,

        highest_price,

        lowest_price,

        last_checked_at,

        margin_usdt,

        leverage,

        strategy,

        score

    ) = row

    highest_price = max(

        highest_price or entry,

        current_price

    )

    lowest_price = min(

        lowest_price or entry,

        current_price

    )

    new_tp1 = bool(

        tp1_hit

        or current_price >= tp1

    )

    new_tp2 = bool(

        tp2_hit

        or current_price >= tp2

    )

    new_tp3 = bool(

        tp3_hit

        or current_price >= tp3

    )

    if new_tp3:

        new_tp1 = True

        new_tp2 = True

    elif new_tp2:

        new_tp1 = True

    sl_hit = (

        current_price <= sl

    )

    if new_tp3:

        gross = (

            full_tp3_gross_result()

        )

        cur.execute(

            """

            UPDATE scanner_trades

            SET

                tp1_hit = TRUE,

                tp2_hit = TRUE,

                tp3_hit = TRUE,

                highest_price = %s,

                lowest_price = %s,

                exit_price = %s,

                result_pct = %s,

                status = 'WIN',

                exit_reason =

                    COALESCE(

                        exit_reason,

                        'TP3_EXIT'

                    ),

                closed_at = NOW()

            WHERE id = %s

              AND status = 'OPEN';

            """,

            (

                highest_price,

                lowest_price,

                current_price,

                gross,

                trade_id

            ),

        )

    elif sl_hit:

        remaining = 1.0

        realized = 0.0

        if new_tp1:

            realized += (

                TP1_PCT /

                100

            ) * SCANNER_TP1

            remaining -= (

                TP1_PCT /

                100

            )

        if new_tp2:

            realized += (

                TP2_PCT /

                100

            ) * SCANNER_TP2

            remaining -= (

                TP2_PCT /

                100

            )

        gross = (

            realized

            -

            remaining

            *

            SCANNER_SL

        )

        status = (

            "WIN"

            if gross > 0

            else "LOSS"

        )

        cur.execute(

            """

            UPDATE scanner_trades

            SET

                tp1_hit = %s,

                tp2_hit = %s,

                tp3_hit = %s,

                sl_hit = TRUE,

                highest_price = %s,

                lowest_price = %s,

                exit_price = %s,

                result_pct = %s,

                status = %s,

                exit_reason =

                    COALESCE(

                        exit_reason,

                        'SL_EXIT'

                    ),

                closed_at = NOW()

            WHERE id = %s

              AND status = 'OPEN';

            """,

            (

                new_tp1,

                new_tp2,

                new_tp3,

                highest_price,

                lowest_price,

                current_price,

                gross,

                status,

                trade_id,

            ),

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

            WHERE id = %s

              AND status = 'OPEN';

            """,

            (

                new_tp1,

                new_tp2,

                new_tp3,

                highest_price,

                lowest_price,

                trade_id,

            ),

        )

# =========================================================

# V3 LEGACY REPLAY

# =========================================================

def update_v3_trade_replay(

    cur,

    row,

    current_price

):

    (

        trade_id,

        symbol,

        side,

        engine_version,

        signal_time,

        entry,

        tp1,

        tp2,

        tp3,

        sl,

        tp1_hit,

        tp2_hit,

        tp3_hit,

        highest_price,

        lowest_price,

        last_checked_at,

        margin_usdt,

        leverage,

        strategy,

        score

    ) = row

    highest_price = (

        highest_price

        or entry

    )

    lowest_price = (

        lowest_price

        or entry

    )

    new_tp1 = bool(

        tp1_hit

    )

    new_tp2 = bool(

        tp2_hit

    )

    new_tp3 = bool(

        tp3_hit

    )

    candles = get_klines(

        symbol,

        "1m",

        SIM_REPLAY_1M_LIMIT

    )

    start_time = (

        last_checked_at

        or signal_time

    )

    start_ms = (

        int(

            start_time.timestamp()

            * 1000

        )

        if start_time

        else 0

    )

    relevant = [

        c

        for c in candles

        if int(

            c["time"]

        ) >= start_ms

    ]

    relevant.sort(

        key=lambda x:

            x["time"]

    )

    if not relevant:

        relevant = [{

            "time":

                int(

                    time.time()

                    * 1000

                ),

            "open":

                current_price,

            "high":

                current_price,

            "low":

                current_price,

            "close":

                current_price,

            "volume":

                0,

        }]

    for candle in relevant:

        high = float(

            candle["high"]

        )

        low = float(

            candle["low"]

        )

        highest_price = max(

            highest_price,

            high

        )

        lowest_price = min(

            lowest_price,

            low

        )

        if new_tp2:

            active_stop = tp1

            stop_return = SCANNER_TP1

            stop_reason = (

                "TP2_PROTECT_EXIT"

            )

        elif new_tp1:

            active_stop = entry

            stop_return = 0.0

            stop_reason = (

                "BREAKEVEN_EXIT"

            )

        else:

            active_stop = sl

            stop_return = (

                -SCANNER_SL

            )

            stop_reason = (

                "SL_EXIT"

            )

        if low <= active_stop:

            gross = (

                gross_result_for_stop(

                    new_tp1,

                    new_tp2,

                    stop_return

                )

            )

            finalize_trade(

                cur,

                trade_id,

                active_stop,

                gross,

                stop_reason,

                new_tp1,

                new_tp2,

                new_tp3,

                stop_reason

                == "SL_EXIT",

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V3 legacy 1m replay",

            )

            return

        hit_tp1_now = (

            not new_tp1

            and high >= tp1

        )

        hit_tp2_now = (

            not new_tp2

            and high >= tp2

        )

        hit_tp3_now = (

            not new_tp3

            and high >= tp3

        )

        if hit_tp1_now:

            new_tp1 = True

            if low <= entry:

                gross = (

                    gross_result_for_stop(

                        True,

                        False,

                        0.0

                    )

                )

                finalize_trade(

                    cur,

                    trade_id,

                    entry,

                    gross,

                    "BREAKEVEN_EXIT",

                    True,

                    False,

                    False,

                    False,

                    highest_price,

                    lowest_price,

                    margin_usdt,

                    leverage,

                    "V3 same candle "

                    "TP1 + breakeven",

                )

                return

        if hit_tp2_now:

            new_tp1 = True

            new_tp2 = True

            if low <= tp1:

                gross = (

                    gross_result_for_stop(

                        True,

                        True,

                        SCANNER_TP1

                    )

                )

                finalize_trade(

                    cur,

                    trade_id,

                    tp1,

                    gross,

                    "TP2_PROTECT_EXIT",

                    True,

                    True,

                    False,

                    False,

                    highest_price,

                    lowest_price,

                    margin_usdt,

                    leverage,

                    "V3 same candle "

                    "TP2 + protection",

                )

                return

        if hit_tp3_now:

            new_tp1 = True

            new_tp2 = True

            if low <= tp1:

                gross = (

                    gross_result_for_stop(

                        True,

                        True,

                        SCANNER_TP1

                    )

                )

                finalize_trade(

                    cur,

                    trade_id,

                    tp1,

                    gross,

                    "TP2_PROTECT_EXIT",

                    True,

                    True,

                    False,

                    False,

                    highest_price,

                    lowest_price,

                    margin_usdt,

                    leverage,

                    "V3 same candle "

                    "TP3 + protection",

                )

                return

            new_tp3 = True

            gross = (

                full_tp3_gross_result()

            )

            finalize_trade(

                cur,

                trade_id,

                tp3,

                gross,

                "TP3_EXIT",

                True,

                True,

                True,

                False,

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V3 1m replay TP3",

            )

            return

    highest_price = max(

        highest_price,

        current_price

    )

    lowest_price = min(

        lowest_price,

        current_price

    )

    if (

        new_tp2

        and current_price <= tp1

    ):

        gross = (

            gross_result_for_stop(

                True,

                True,

                SCANNER_TP1

            )

        )

        finalize_trade(

            cur,

            trade_id,

            tp1,

            gross,

            "TP2_PROTECT_EXIT",

            True,

            True,

            False,

            False,

            highest_price,

            lowest_price,

            margin_usdt,

            leverage,

            "V3 current price "

            "protection",

        )

        return

    if (

        new_tp1

        and not new_tp2

        and current_price <= entry

    ):

        gross = (

            gross_result_for_stop(

                True,

                False,

                0.0

            )

        )

        finalize_trade(

            cur,

            trade_id,

            entry,

            gross,

            "BREAKEVEN_EXIT",

            True,

            False,

            False,

            False,

            highest_price,

            lowest_price,

            margin_usdt,

            leverage,

            "V3 current price "

            "breakeven",

        )

        return

    if (

        not new_tp1

        and current_price <= sl

    ):

        gross = (

            -SCANNER_SL

        )

        finalize_trade(

            cur,

            trade_id,

            sl,

            gross,

            "SL_EXIT",

            False,

            False,

            False,

            True,

            highest_price,

            lowest_price,

            margin_usdt,

            leverage,

            "V3 current price SL",

        )

        return

    cur.execute(

        """

        UPDATE scanner_trades

        SET

            tp1_hit = %s,

            tp2_hit = %s,

            tp3_hit = %s,

            highest_price = %s,

            lowest_price = %s,

            last_checked_at = NOW()

        WHERE id = %s

          AND status = 'OPEN';

        """,

        (

            new_tp1,

            new_tp2,

            new_tp3,

            highest_price,

            lowest_price,

            trade_id,

        ),

    )

# =========================================================

# V4.2 LEGACY LONG / SHORT REPLAY

# =========================================================

def _v42_stop_hit(

    side,

    candle_high,

    candle_low,

    stop_price

):

    if side == "SHORT":

        return (

            candle_high >=

            stop_price

        )

    return (

        candle_low <=

        stop_price

    )

def _v42_tp_hit(

    side,

    candle_high,

    candle_low,

    target

):

    if side == "SHORT":

        return (

            candle_low <=

            target

        )

    return (

        candle_high >=

        target

    )

def update_v42_trade_replay(

    cur,

    row,

    current_price

):

    (

        trade_id,

        symbol,

        side,

        engine_version,

        signal_time,

        entry,

        tp1,

        tp2,

        tp3,

        sl,

        tp1_hit,

        tp2_hit,

        tp3_hit,

        highest_price,

        lowest_price,

        last_checked_at,

        margin_usdt,

        leverage,

        strategy,

        score

    ) = row

    side = (

        side or "LONG"

    ).upper()

    highest_price = (

        highest_price

        or entry

    )

    lowest_price = (

        lowest_price

        or entry

    )

    new_tp1 = bool(tp1_hit)

    new_tp2 = bool(tp2_hit)

    new_tp3 = bool(tp3_hit)

    # V4.2 原本規則維持不變

    if (

        not new_tp1

        and signal_time

        is not None

    ):

        age_hours = (

            time.time()

            -

            signal_time.timestamp()

        ) / 3600.0

        if (

            age_hours >=

            V42_POSITION_TIMEOUT_HOURS

        ):

            highest_price = max(

                highest_price,

                current_price

            )

            lowest_price = min(

                lowest_price,

                current_price

            )

            gross = (

                side_return_pct(

                    side,

                    entry,

                    current_price

                )

            )

            finalize_trade(

                cur,

                trade_id,

                current_price,

                gross,

                "TIMEOUT_EXIT",

                False,

                False,

                False,

                False,

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.2 timeout after "

                f"{age_hours:.2f}h "

                "without TP1",

            )

            return

    candles = get_klines(

        symbol,

        "1m",

        SIM_REPLAY_1M_LIMIT

    )

    start_time = (

        last_checked_at

        or signal_time

    )

    start_ms = (

        int(

            start_time.timestamp()

            * 1000

        )

        if start_time

        else 0

    )

    relevant = [

        c

        for c in candles

        if int(

            c["time"]

        ) >= start_ms

    ]

    relevant.sort(

        key=lambda x:

            x["time"]

    )

    if not relevant:

        relevant = [{

            "time":

                int(

                    time.time()

                    * 1000

                ),

            "open":

                current_price,

            "high":

                current_price,

            "low":

                current_price,

            "close":

                current_price,

            "volume":

                0,

        }]

    for candle in relevant:

        high = float(

            candle["high"]

        )

        low = float(

            candle["low"]

        )

        highest_price = max(

            highest_price,

            high

        )

        lowest_price = min(

            lowest_price,

            low

        )

        if new_tp2:

            active_stop = tp1

            stop_reason = (

                "TP2_PROTECT_EXIT"

            )

        elif new_tp1:

            active_stop = entry

            stop_reason = (

                "BREAKEVEN_EXIT"

            )

        else:

            active_stop = sl

            stop_reason = (

                "SL_EXIT"

            )

        if _v42_stop_hit(

            side,

            high,

            low,

            active_stop

        ):

            gross = (

                gross_result_for_stop_levels(

                    side,

                    entry,

                    tp1,

                    tp2,

                    new_tp1,

                    new_tp2,

                    active_stop

                )

            )

            finalize_trade(

                cur,

                trade_id,

                active_stop,

                gross,

                stop_reason,

                new_tp1,

                new_tp2,

                new_tp3,

                stop_reason

                == "SL_EXIT",

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.2 conservative "

                "1m replay",

            )

            return

        hit_tp1_now = (

            not new_tp1

            and

            _v42_tp_hit(

                side,

                high,

                low,

                tp1

            )

        )

        hit_tp2_now = (

            not new_tp2

            and

            _v42_tp_hit(

                side,

                high,

                low,

                tp2

            )

        )

        hit_tp3_now = (

            not new_tp3

            and

            _v42_tp_hit(

                side,

                high,

                low,

                tp3

            )

        )

        if hit_tp1_now:

            new_tp1 = True

            if _v42_stop_hit(

                side,

                high,

                low,

                entry

            ):

                gross = (

                    gross_result_for_stop_levels(

                        side,

                        entry,

                        tp1,

                        tp2,

                        True,

                        False,

                        entry

                    )

                )

                finalize_trade(

                    cur,

                    trade_id,

                    entry,

                    gross,

                    "BREAKEVEN_EXIT",

                    True,

                    False,

                    False,

                    False,

                    highest_price,

                    lowest_price,

                    margin_usdt,

                    leverage,

                    "V4.2 same candle "

                    "TP1 + breakeven",

                )

                return

        if hit_tp2_now:

            new_tp1 = True

            new_tp2 = True

            if _v42_stop_hit(

                side,

                high,

                low,

                tp1

            ):

                gross = (

                    gross_result_for_stop_levels(

                        side,

                        entry,

                        tp1,

                        tp2,

                        True,

                        True,

                        tp1

                    )

                )

                finalize_trade(

                    cur,

                    trade_id,

                    tp1,

                    gross,

                    "TP2_PROTECT_EXIT",

                    True,

                    True,

                    False,

                    False,

                    highest_price,

                    lowest_price,

                    margin_usdt,

                    leverage,

                    "V4.2 same candle "

                    "TP2 + TP1 protection",

                )

                return

        if hit_tp3_now:

            new_tp1 = True

            new_tp2 = True

            if _v42_stop_hit(

                side,

                high,

                low,

                tp1

            ):

                gross = (

                    gross_result_for_stop_levels(

                        side,

                        entry,

                        tp1,

                        tp2,

                        True,

                        True,

                        tp1

                    )

                )

                finalize_trade(

                    cur,

                    trade_id,

                    tp1,

                    gross,

                    "TP2_PROTECT_EXIT",

                    True,

                    True,

                    False,

                    False,

                    highest_price,

                    lowest_price,

                    margin_usdt,

                    leverage,

                    "V4.2 same candle "

                    "TP3 + TP1 protection",

                )

                return

            new_tp3 = True

            gross = (

                full_tp3_gross_result_levels(

                    side,

                    entry,

                    tp1,

                    tp2,

                    tp3

                )

            )

            finalize_trade(

                cur,

                trade_id,

                tp3,

                gross,

                "TP3_EXIT",

                True,

                True,

                True,

                False,

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.2 1m replay TP3",

            )

            return

    highest_price = max(

        highest_price,

        current_price

    )

    lowest_price = min(

        lowest_price,

        current_price

    )

    if new_tp2:

        protection_hit = (

            current_price >= tp1

            if side == "SHORT"

            else current_price <= tp1

        )

        if protection_hit:

            gross = (

                gross_result_for_stop_levels(

                    side,

                    entry,

                    tp1,

                    tp2,

                    True,

                    True,

                    tp1

                )

            )

            finalize_trade(

                cur,

                trade_id,

                tp1,

                gross,

                "TP2_PROTECT_EXIT",

                True,

                True,

                False,

                False,

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.2 current price "

                "TP2 protection",

            )

            return

    if (

        new_tp1

        and not new_tp2

    ):

        breakeven_hit = (

            current_price >= entry

            if side == "SHORT"

            else current_price <= entry

        )

        if breakeven_hit:

            gross = (

                gross_result_for_stop_levels(

                    side,

                    entry,

                    tp1,

                    tp2,

                    True,

                    False,

                    entry

                )

            )

            finalize_trade(

                cur,

                trade_id,

                entry,

                gross,

                "BREAKEVEN_EXIT",

                True,

                False,

                False,

                False,

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.2 current price "

                "breakeven",

            )

            return

    if not new_tp1:

        sl_now = (

            current_price >= sl

            if side == "SHORT"

            else current_price <= sl

        )

        if sl_now:

            gross = (

                side_return_pct(

                    side,

                    entry,

                    sl

                )

            )

            finalize_trade(

                cur,

                trade_id,

                sl,

                gross,

                "SL_EXIT",

                False,

                False,

                False,

                True,

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.2 current price SL",

            )

            return

    cur.execute(

        """

        UPDATE scanner_trades

        SET

            tp1_hit = %s,

            tp2_hit = %s,

            tp3_hit = %s,

            highest_price = %s,

            lowest_price = %s,

            last_checked_at = NOW()

        WHERE id = %s

          AND status = 'OPEN';

        """,

        (

            new_tp1,

            new_tp2,

            new_tp3,

            highest_price,

            lowest_price,

            trade_id,

        ),

    )

# =========================================================

# V4.3 DYNAMIC TIMEOUT

# =========================================================

def v43_timeout_hours(

    strategy,

    score

):

    strategy = (

        strategy

        or "TREND"

    ).upper()

    score = int(

        score or 0

    )

    if strategy == "BREAKOUT":

        return (

            V43_TIMEOUT_BREAKOUT_HOURS

        )

    if score >= 95:

        return (

            V43_TIMEOUT_STRONG_TREND_HOURS

        )

    return (

        V43_TIMEOUT_TREND_HOURS

    )

# =========================================================

# V4.3 EARLY WEAK EXIT

# =========================================================

def v43_early_weakness(

    symbol,

    side

):

    candles_5m = get_klines(

        symbol,

        "5m",

        60

    )

    time.sleep(

        SCANNER_API_DELAY

    )

    candles_15m = get_klines(

        symbol,

        "15m",

        60

    )

    if (

        len(candles_5m) < 30

        or

        len(candles_15m) < 30

    ):

        return False, []

    c5 = candles_5m[:-1]

    c15 = candles_15m[:-1]

    close5 = [

        x["close"]

        for x in c5

    ]

    close15 = [

        x["close"]

        for x in c15

    ]

    vol5 = [

        x["volume"]

        for x in c5

    ]

    last5 = close5[-1]

    ema9_5 = calculate_ema(

        close5,

        9

    )

    ema20_5 = calculate_ema(

        close5,

        20

    )

    ema20_15 = calculate_ema(

        close15,

        20

    )

    ema50_15 = calculate_ema(

        close15,

        50

    )

    rsi5 = calculate_rsi(

        close5,

        14

    )

    if (

        ema9_5 is None

        or ema20_5 is None

        or ema20_15 is None

        or ema50_15 is None

        or rsi5 is None

    ):

        return False, []

    momentum_15m = (

        (

            close5[-1]

            -

            close5[-4]

        )

        /

        close5[-4]

        *

        100

    )

    previous_volumes = (

        vol5[-21:-1]

    )

    avg_volume = (

        sum(previous_volumes)

        /

        len(previous_volumes)

        if previous_volumes

        else 0

    )

    volume_ratio = (

        vol5[-1]

        /

        avg_volume

        if avg_volume > 0

        else 0

    )

    weakness = []

    side = (

        side or "LONG"

    ).upper()

    if side == "LONG":

        if last5 < ema20_5:

            weakness.append(

                "5m below EMA20"

            )

        if ema9_5 < ema20_5:

            weakness.append(

                "5m EMA bearish cross"

            )

        if close15[-1] < ema20_15:

            weakness.append(

                "15m below EMA20"

            )

        if ema20_15 < ema50_15:

            weakness.append(

                "15m bearish EMA structure"

            )

        if momentum_15m < -0.20:

            weakness.append(

                "15m momentum "

                f"{momentum_15m:.2f}%"

            )

        if rsi5 < 45:

            weakness.append(

                f"5m RSI {rsi5:.1f}"

            )

    else:

        if last5 > ema20_5:

            weakness.append(

                "5m above EMA20"

            )

        if ema9_5 > ema20_5:

            weakness.append(

                "5m EMA bullish cross"

            )

        if close15[-1] > ema20_15:

            weakness.append(

                "15m above EMA20"

            )

        if ema20_15 > ema50_15:

            weakness.append(

                "15m bullish EMA structure"

            )

        if momentum_15m > 0.20:

            weakness.append(

                "15m rebound momentum +"

                f"{momentum_15m:.2f}%"

            )

        if rsi5 > 55:

            weakness.append(

                f"5m RSI {rsi5:.1f}"

            )

    if volume_ratio < 0.80:

        weakness.append(

            "5m volume weak "

            f"{volume_ratio:.2f}x"

        )

    return (

        len(weakness)

        >=

        V43_EARLY_WEAKNESS_COUNT,

        weakness

    )

# =========================================================

# V4.3R1 REPLAY

# =========================================================

def update_v43_trade_replay(

    cur,

    row,

    current_price

):

    (

        trade_id,

        symbol,

        side,

        engine_version,

        signal_time,

        entry,

        tp1,

        tp2,

        tp3,

        sl,

        tp1_hit,

        tp2_hit,

        tp3_hit,

        highest_price,

        lowest_price,

        last_checked_at,

        margin_usdt,

        leverage,

        strategy,

        score

    ) = row

    side = (

        side or "LONG"

    ).upper()

    strategy = (

        strategy or "TREND"

    ).upper()

    highest_price = (

        highest_price

        or entry

    )

    lowest_price = (

        lowest_price

        or entry

    )

    new_tp1 = bool(tp1_hit)

    new_tp2 = bool(tp2_hit)

    new_tp3 = bool(tp3_hit)

    # -----------------------------------------------------

    # 先 replay，再判斷 TIMEOUT

    # -----------------------------------------------------

    candles = get_klines(

        symbol,

        "1m",

        SIM_REPLAY_1M_LIMIT

    )

    start_time = (

        last_checked_at

        or signal_time

    )

    start_ms = (

        int(

            start_time.timestamp()

            * 1000

        )

        if start_time

        else 0

    )

    relevant = [

        c

        for c in candles

        if int(

            c["time"]

        ) >= start_ms

    ]

    relevant.sort(

        key=lambda x:

            x["time"]

    )

    if not relevant:

        relevant = [{

            "time":

                int(

                    time.time()

                    * 1000

                ),

            "open":

                current_price,

            "high":

                current_price,

            "low":

                current_price,

            "close":

                current_price,

            "volume":

                0,

        }]

    for candle in relevant:

        high = float(

            candle["high"]

        )

        low = float(

            candle["low"]

        )

        highest_price = max(

            highest_price,

            high

        )

        lowest_price = min(

            lowest_price,

            low

        )

        if new_tp2:

            active_stop = tp1

            stop_reason = (

                "TP2_PROTECT_EXIT"

            )

        elif new_tp1:

            active_stop = entry

            stop_reason = (

                "BREAKEVEN_EXIT"

            )

        else:

            active_stop = sl

            stop_reason = (

                "SL_EXIT"

            )

        # 同根 1m K 同時碰 stop + TP 時

        # 仍採保守路徑：先算 stop

        if _v42_stop_hit(

            side,

            high,

            low,

            active_stop

        ):

            gross = (

                gross_result_for_stop_levels(

                    side,

                    entry,

                    tp1,

                    tp2,

                    new_tp1,

                    new_tp2,

                    active_stop

                )

            )

            finalize_trade(

                cur,

                trade_id,

                active_stop,

                gross,

                stop_reason,

                new_tp1,

                new_tp2,

                new_tp3,

                stop_reason

                == "SL_EXIT",

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.3R1 conservative "

                "1m replay",

            )

            return

        hit_tp1_now = (

            not new_tp1

            and

            _v42_tp_hit(

                side,

                high,

                low,

                tp1

            )

        )

        hit_tp2_now = (

            not new_tp2

            and

            _v42_tp_hit(

                side,

                high,

                low,

                tp2

            )

        )

        hit_tp3_now = (

            not new_tp3

            and

            _v42_tp_hit(

                side,

                high,

                low,

                tp3

            )

        )

        if hit_tp1_now:

            new_tp1 = True

            if _v42_stop_hit(

                side,

                high,

                low,

                entry

            ):

                gross = (

                    gross_result_for_stop_levels(

                        side,

                        entry,

                        tp1,

                        tp2,

                        True,

                        False,

                        entry

                    )

                )

                finalize_trade(

                    cur,

                    trade_id,

                    entry,

                    gross,

                    "BREAKEVEN_EXIT",

                    True,

                    False,

                    False,

                    False,

                    highest_price,

                    lowest_price,

                    margin_usdt,

                    leverage,

                    "V4.3R1 same candle "

                    "TP1 + breakeven",

                )

                return

        if hit_tp2_now:

            new_tp1 = True

            new_tp2 = True

            if _v42_stop_hit(

                side,

                high,

                low,

                tp1

            ):

                gross = (

                    gross_result_for_stop_levels(

                        side,

                        entry,

                        tp1,

                        tp2,

                        True,

                        True,

                        tp1

                    )

                )

                finalize_trade(

                    cur,

                    trade_id,

                    tp1,

                    gross,

                    "TP2_PROTECT_EXIT",

                    True,

                    True,

                    False,

                    False,

                    highest_price,

                    lowest_price,

                    margin_usdt,

                    leverage,

                    "V4.3R1 same candle "

                    "TP2 + TP1 protection",

                )

                return

        if hit_tp3_now:

            new_tp1 = True

            new_tp2 = True

            if _v42_stop_hit(

                side,

                high,

                low,

                tp1

            ):

                gross = (

                    gross_result_for_stop_levels(

                        side,

                        entry,

                        tp1,

                        tp2,

                        True,

                        True,

                        tp1

                    )

                )

                finalize_trade(

                    cur,

                    trade_id,

                    tp1,

                    gross,

                    "TP2_PROTECT_EXIT",

                    True,

                    True,

                    False,

                    False,

                    highest_price,

                    lowest_price,

                    margin_usdt,

                    leverage,

                    "V4.3R1 same candle "

                    "TP3 + TP1 protection",

                )

                return

            new_tp3 = True

            gross = (

                full_tp3_gross_result_levels(

                    side,

                    entry,

                    tp1,

                    tp2,

                    tp3

                )

            )

            finalize_trade(

                cur,

                trade_id,

                tp3,

                gross,

                "TP3_EXIT",

                True,

                True,

                True,

                False,

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.3R1 1m replay TP3",

            )

            return

    # -----------------------------------------------------

    # CURRENT PRICE CHECK

    # -----------------------------------------------------

    highest_price = max(

        highest_price,

        current_price

    )

    lowest_price = min(

        lowest_price,

        current_price

    )

    if new_tp2:

        protection_hit = (

            current_price >= tp1

            if side == "SHORT"

            else current_price <= tp1

        )

        if protection_hit:

            gross = (

                gross_result_for_stop_levels(

                    side,

                    entry,

                    tp1,

                    tp2,

                    True,

                    True,

                    tp1

                )

            )

            finalize_trade(

                cur,

                trade_id,

                tp1,

                gross,

                "TP2_PROTECT_EXIT",

                True,

                True,

                False,

                False,

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.3R1 current price "

                "TP2 protection",

            )

            return

    if (

        new_tp1

        and not new_tp2

    ):

        breakeven_hit = (

            current_price >= entry

            if side == "SHORT"

            else current_price <= entry

        )

        if breakeven_hit:

            gross = (

                gross_result_for_stop_levels(

                    side,

                    entry,

                    tp1,

                    tp2,

                    True,

                    False,

                    entry

                )

            )

            finalize_trade(

                cur,

                trade_id,

                entry,

                gross,

                "BREAKEVEN_EXIT",

                True,

                False,

                False,

                False,

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.3R1 current "

                "price breakeven",

            )

            return

    if not new_tp1:

        sl_now = (

            current_price >= sl

            if side == "SHORT"

            else current_price <= sl

        )

        if sl_now:

            gross = (

                side_return_pct(

                    side,

                    entry,

                    sl

                )

            )

            finalize_trade(

                cur,

                trade_id,

                sl,

                gross,

                "SL_EXIT",

                False,

                False,

                False,

                True,

                highest_price,

                lowest_price,

                margin_usdt,

                leverage,

                "V4.3R1 current price SL",

            )

            return

        # -------------------------------------------------

        # EARLY WEAK / DYNAMIC TIMEOUT

        # 只有 TP1 尚未命中才使用

        # -------------------------------------------------

        if signal_time is not None:

            age_hours = (

                time.time()

                -

                signal_time.timestamp()

            ) / 3600.0

            if (

                age_hours >=

                V43_EARLY_WEAK_MIN_HOURS

            ):

                try:

                    (

                        weak,

                        weakness

                    ) = v43_early_weakness(

                        symbol,

                        side

                    )

                except Exception as e:

                    print(

                        "EARLY WEAK CHECK ERROR:",

                        symbol,

                        str(e),

                        flush=True,

                    )

                    weak = False

                    weakness = []

                if weak:

                    gross = (

                        side_return_pct(

                            side,

                            entry,

                            current_price

                        )

                    )

                    finalize_trade(

                        cur,

                        trade_id,

                        current_price,

                        gross,

                        "EARLY_WEAK_EXIT",

                        False,

                        False,

                        False,

                        False,

                        highest_price,

                        lowest_price,

                        margin_usdt,

                        leverage,

                        "V4.3R1 early weak "

                        f"after {age_hours:.2f}h | "

                        +

                        ", ".join(

                            weakness

                        ),

                    )

                    return

            timeout_hours = (

                v43_timeout_hours(

                    strategy,

                    score

                )

            )

            if (

                age_hours >=

                timeout_hours

            ):

                gross = (

                    side_return_pct(

                        side,

                        entry,

                        current_price

                    )

                )

                finalize_trade(

                    cur,

                    trade_id,

                    current_price,

                    gross,

                    "TIMEOUT_EXIT",

                    False,

                    False,

                    False,

                    False,

                    highest_price,

                    lowest_price,

                    margin_usdt,

                    leverage,

                    "V4.3R1 dynamic timeout "

                    f"{timeout_hours:.1f}h | "

                    f"age {age_hours:.2f}h",

                )

                return

    cur.execute(

        """

        UPDATE scanner_trades

        SET

            tp1_hit = %s,

            tp2_hit = %s,

            tp3_hit = %s,

            highest_price = %s,

            lowest_price = %s,

            last_checked_at = NOW()

        WHERE id = %s

          AND status = 'OPEN';

        """,

        (

            new_tp1,

            new_tp2,

            new_tp3,

            highest_price,

            lowest_price,

            trade_id,

        ),

    )

# =========================================================

# UPDATE ALL SIMULATED TRADES

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

            side,

            engine_version,

            signal_time,

            entry_price,

            tp1,

            tp2,

            tp3,

            sl,

            tp1_hit,

            tp2_hit,

            tp3_hit,

            highest_price,

            lowest_price,

            last_checked_at,

            sim_margin_usdt,

            sim_leverage,

            strategy,

            score

        FROM scanner_trades

        WHERE status = 'OPEN'

          AND strategy IN (

              'TREND',

              'BREAKOUT'

          )

        ORDER BY id ASC;

        """

    )

    rows = cur.fetchall()

    for row in rows:

        trade_id = row[0]

        symbol = row[1]

        engine_version = (

            row[3] or "V2"

        )

        if symbol not in prices:

            continue

        try:

            # V4.3 新邏輯

            if (

                engine_version ==

                SCANNER_ENGINE_VERSION

            ):

                update_v43_trade_replay(

                    cur,

                    row,

                    prices[symbol]

                )

            # V4.2 舊倉保持舊規則

            elif engine_version in (

                "V4.2R1",

                "V4.2",

                "V4.2_PRE"

            ):

                update_v42_trade_replay(

                    cur,

                    row,

                    prices[symbol]

                )

            elif (

                engine_version ==

                "V3"

            ):

                update_v3_trade_replay(

                    cur,

                    row,

                    prices[symbol]

                )

            else:

                update_v2_trade_current_price(

                    cur,

                    row,

                    prices[symbol]

                )

            conn.commit()

        except Exception as e:

            conn.rollback()

            print(

                "SIM UPDATE ERROR:",

                trade_id,

                symbol,

                str(e),

                flush=True,

            )

    cur.close()

    conn.close()

# =========================================================

# OPTIONAL WEB MONITOR

# =========================================================

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

                flush=True,

            )

        time.sleep(

            SIM_MONITOR_SEC

        )

def start_simulation_monitor():

    thread = threading.Thread(

        target=simulation_monitor_loop,

        daemon=True

    )

    thread.start()

# =========================================================

# PERFORMANCE STATS

# =========================================================

def calculate_performance_stats(

    results

):

    if not results:

        return {

            "closed": 0,

            "wins": 0,

            "losses": 0,

            "win_rate": 0,

            "avg_result": 0,

            "avg_win": 0,

            "avg_loss": 0,

            "reward_risk": 0,

            "profit_factor": 0,

            "expectancy": 0,

            "total_result": 0,

            "max_winning_streak": 0,

            "max_losing_streak": 0,

        }

    wins_list = [

        x

        for x in results

        if x > 0

    ]

    losses_list = [

        x

        for x in results

        if x <= 0

    ]

    closed = len(

        results

    )

    avg_win = (

        sum(wins_list)

        /

        len(wins_list)

        if wins_list

        else 0

    )

    avg_loss = (

        sum(losses_list)

        /

        len(losses_list)

        if losses_list

        else 0

    )

    win_rate_decimal = (

        len(wins_list)

        /

        closed

    )

    gross_profit = sum(

        wins_list

    )

    gross_loss = abs(

        sum(

            losses_list

        )

    )

    current_win = 0

    current_loss = 0

    max_win = 0

    max_loss = 0

    for x in results:

        if x > 0:

            current_win += 1

            current_loss = 0

            max_win = max(

                max_win,

                current_win

            )

        else:

            current_loss += 1

            current_win = 0

            max_loss = max(

                max_loss,

                current_loss

            )

    return {

        "closed":

            closed,

        "wins":

            len(wins_list),

        "losses":

            len(losses_list),

        "win_rate":

            win_rate_decimal

            * 100,

        "avg_result":

            sum(results)

            / closed,

        "avg_win":

            avg_win,

        "avg_loss":

            avg_loss,

        "reward_risk":

            (

                avg_win

                /

                abs(avg_loss)

                if avg_loss < 0

                else 0

            ),

        "profit_factor":

            (

                gross_profit

                /

                gross_loss

                if gross_loss > 0

                else (

                    float("inf")

                    if gross_profit > 0

                    else 0

                )

            ),

        "expectancy":

            (

                win_rate_decimal

                * avg_win

                +

                (

                    1 -

                    win_rate_decimal

                )

                * avg_loss

            ),

        "total_result":

            sum(results),

        "max_winning_streak":

            max_win,

        "max_losing_streak":

            max_loss,

    }

def calculate_equity_metrics(

    pnls

):

    equity = (

        SIM_START_BALANCE

    )

    peak = equity

    max_drawdown_usdt = 0.0

    max_drawdown_pct = 0.0

    for pnl in pnls:

        equity += pnl

        peak = max(

            peak,

            equity

        )

        dd = (

            equity -

            peak

        )

        dd_pct = (

            dd /

            peak *

            100

            if peak > 0

            else 0

        )

        max_drawdown_usdt = min(

            max_drawdown_usdt,

            dd

        )

        max_drawdown_pct = min(

            max_drawdown_pct,

            dd_pct

        )

    return (

        equity,

        max_drawdown_usdt,

        max_drawdown_pct

    )

# =========================================================

# WEB PAGES

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

        now = (

            cur.fetchone()[0]

        )

        return (

            f"DATABASE OK | {now}",

            200

        )

    except Exception as e:

        return (

            "DATABASE ERROR | "

            f"{str(e)}",

            500

        )

    finally:

        if cur:

            cur.close()

        if conn:

            conn.close()

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

            side,

            strategy,

            engine_version,

            signal_time,

            entry_price,

            score,

            trend_score,

            breakout_score,

            sideways_score,

            sideways_state,

            resonance,

            resonance_source,

            management_mode,

            tp1,

            tp2,

            tp3,

            sl,

            tp1_hit,

            tp2_hit,

            tp3_hit,

            sl_hit,

            status,

            result_pct,

            net_result_pct,

            sim_pnl_usdt,

            exit_reason,

            exit_detail

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

            trade_id,

            symbol,

            side,

            strategy,

            version,

            signal_time,

            entry,

            score,

            trend_score,

            breakout_score,

            sideways_score,

            sideways_state,

            resonance,

            resonance_source,

            management_mode,

            tp1,

            tp2,

            tp3,

            sl,

            tp1_hit,

            tp2_hit,

            tp3_hit,

            sl_hit,

            status,

            gross,

            net,

            pnl,

            exit_reason,

            exit_detail

        ) = row

        lines.append(

            f"#{trade_id} "

            f"{symbol} | "

            f"{version or '-'} | "

            f"{side or 'LONG'} | "

            f"{strategy} | "

            f"{status}"

        )

        lines.append(

            f"Time: {signal_time}"

        )

        lines.append(

            f"Entry: {entry} | "

            f"Score {score} | "

            f"Trend {trend_score} | "

            f"Breakout {breakout_score} | "

            f"Sideways "

            f"{sideways_score if sideways_score is not None else '-'} "

            f"{sideways_state or '-'}"

        )

        if resonance:

            lines.append(

                "RESONANCE: YES | "

                f"Source "

                f"{resonance_source or '-'} | "

                f"Management "

                f"{management_mode or 'SCANNER'}"

            )

        lines.append(

            f"TP1 "

            f"{'YES' if tp1_hit else 'NO'} | "

            f"TP2 "

            f"{'YES' if tp2_hit else 'NO'} | "

            f"TP3 "

            f"{'YES' if tp3_hit else 'NO'} | "

            f"SL "

            f"{'YES' if sl_hit else 'NO'}"

        )

        lines.append(

            f"Gross: "

            f"{gross if gross is not None else '-'}% | "

            f"Net est.: "

            f"{net if net is not None else '-'}% | "

            f"PnL: "

            f"{pnl if pnl is not None else '-'} U"

        )

        lines.append(

            f"Exit: "

            f"{exit_reason or '-'}"

        )

        if exit_detail:

            lines.append(

                f"Detail: "

                f"{exit_detail}"

            )

        lines.append(

            "------------------------------------------------------------"

        )

    return (

        "<pre>"

        +

        "\n".join(lines)

        +

        "</pre>",

        200

    )

def _load_engine_rows(

    cur,

    version

):

    cur.execute(

        """

        SELECT

            id,

            side,

            strategy,

            score,

            status,

            result_pct,

            net_result_pct,

            sim_pnl_usdt,

            tp1_hit,

            tp2_hit,

            tp3_hit,

            COALESCE(

                exit_reason,

                'UNKNOWN'

            ),

            closed_at

        FROM scanner_trades

        WHERE engine_version = %s

          AND strategy IN (

              'TREND',

              'BREAKOUT'

          )

          AND status IN (

              'WIN',

              'LOSS'

          )

        ORDER BY

            closed_at ASC,

            id ASC;

        """,

        (

            version,

        ),

    )

    return cur.fetchall()

@app.route(

    "/scanner-stats",

    methods=["GET"]

)

def scanner_stats_page():

    conn = get_db_connection()

    cur = conn.cursor()

    cur.execute(

        """

        SELECT

            COUNT(*),

            SUM(

                CASE

                    WHEN status='OPEN'

                    THEN 1

                    ELSE 0

                END

            )

        FROM scanner_trades

        WHERE engine_version = %s

          AND strategy IN (

              'TREND',

              'BREAKOUT'

          );

        """,

        (

            SCANNER_ENGINE_VERSION,

        ),

    )

    (

        total,

        open_count

    ) = cur.fetchone()

    open_count = int(

        open_count or 0

    )

    rows = _load_engine_rows(

        cur,

        SCANNER_ENGINE_VERSION

    )

    net_results = [

        float(

            r[6] or 0

        )

        for r in rows

    ]

    pnls = [

        float(

            r[7] or 0

        )

        for r in rows

    ]

    net_stats = (

        calculate_performance_stats(

            net_results

        )

    )

    (

        balance,

        max_dd_u,

        max_dd_pct

    ) = calculate_equity_metrics(

        pnls

    )

    closed = len(rows)

    tp1_hits = sum(

        1

        for r in rows

        if r[8]

    )

    tp2_hits = sum(

        1

        for r in rows

        if r[9]

    )

    tp3_hits = sum(

        1

        for r in rows

        if r[10]

    )

    cur.execute(

        """

        SELECT

            side,

            strategy,

            COUNT(*) AS total,

            SUM(

                CASE

                    WHEN status='OPEN'

                    THEN 1

                    ELSE 0

                END

            ) AS open_count,

            SUM(

                CASE

                    WHEN status='WIN'

                    THEN 1

                    ELSE 0

                END

            ) AS wins,

            SUM(

                CASE

                    WHEN status='LOSS'

                    THEN 1

                    ELSE 0

                END

            ) AS losses,

            COALESCE(

                AVG(

                    CASE

                        WHEN net_result_pct

                             IS NOT NULL

                        THEN net_result_pct

                    END

                ),

                0

            )

        FROM scanner_trades

        WHERE engine_version = %s

          AND strategy IN (

              'TREND',

              'BREAKOUT'

          )

        GROUP BY

            side,

            strategy

        ORDER BY

            side,

            strategy;

        """,

        (

            SCANNER_ENGINE_VERSION,

        ),

    )

    strategy_rows = (

        cur.fetchall()

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

                ELSE '<80'

            END

            AS bucket,

            COUNT(*) AS closed,

            SUM(

                CASE

                    WHEN status='WIN'

                    THEN 1

                    ELSE 0

                END

            ) AS wins,

            SUM(

                CASE

                    WHEN status='LOSS'

                    THEN 1

                    ELSE 0

                END

            ) AS losses,

            COALESCE(

                AVG(net_result_pct),

                0

            )

        FROM scanner_trades

        WHERE engine_version = %s

          AND strategy IN (

              'TREND',

              'BREAKOUT'

          )

          AND status IN (

              'WIN',

              'LOSS'

          )

        GROUP BY bucket

        ORDER BY bucket DESC;

        """,

        (

            SCANNER_ENGINE_VERSION,

        ),

    )

    score_rows = (

        cur.fetchall()

    )

    cur.execute(

        """

        SELECT

            COALESCE(

                exit_reason,

                'UNKNOWN'

            ),

            COUNT(*),

            SUM(

                CASE

                    WHEN status='WIN'

                    THEN 1

                    ELSE 0

                END

            ),

            SUM(

                CASE

                    WHEN status='LOSS'

                    THEN 1

                    ELSE 0

                END

            ),

            COALESCE(

                AVG(net_result_pct),

                0

            )

        FROM scanner_trades

        WHERE engine_version = %s

          AND status IN (

              'WIN',

              'LOSS'

          )

        GROUP BY

            COALESCE(

                exit_reason,

                'UNKNOWN'

            )

        ORDER BY

            COUNT(*) DESC;

        """,

        (

            SCANNER_ENGINE_VERSION,

        ),

    )

    exit_rows = (

        cur.fetchall()

    )

    cur.execute(

        """

        SELECT

            COALESCE(

                sideways_state,

                'UNKNOWN'

            ),

            COUNT(*),

            SUM(

                CASE

                    WHEN status='WIN'

                    THEN 1

                    ELSE 0

                END

            ),

            SUM(

                CASE

                    WHEN status='LOSS'

                    THEN 1

                    ELSE 0

                END

            ),

            COALESCE(

                AVG(

                    CASE

                        WHEN status

                             IN ('WIN','LOSS')

                        THEN net_result_pct

                    END

                ),

                0

            )

        FROM scanner_trades

        WHERE engine_version = %s

        GROUP BY

            COALESCE(

                sideways_state,

                'UNKNOWN'

            )

        ORDER BY

            COUNT(*) DESC;

        """,

        (

            SCANNER_ENGINE_VERSION,

        ),

    )

    sideways_rows = (

        cur.fetchall()

    )

    # V4.2R1 benchmark

    v42_rows = _load_engine_rows(

        cur,

        "V4.2R1"

    )

    v42_net = [

        float(

            r[6] or 0

        )

        for r in v42_rows

    ]

    v42_stats = (

        calculate_performance_stats(

            v42_net

        )

    )

    cur.close()

    conn.close()

    def rate(

        hit,

        denominator

    ):

        return (

            hit /

            denominator *

            100

            if denominator

            else 0

        )

    pf = (

        "INF"

        if net_stats[

            "profit_factor"

        ] == float("inf")

        else (

            f"{net_stats['profit_factor']:.2f}"

        )

    )

    v42_pf = (

        "INF"

        if v42_stats[

            "profit_factor"

        ] == float("inf")

        else (

            f"{v42_stats['profit_factor']:.2f}"

        )

    )

    roi = (

        (

            balance -

            SIM_START_BALANCE

        )

        /

        SIM_START_BALANCE

        *

        100

        if SIM_START_BALANCE

        else 0

    )

    open_margin = (

        open_count *

        SIM_MARGIN_USDT

    )

    open_notional = (

        open_margin *

        SIM_LEVERAGE

    )

    lines = [

        "BINGX SCANNER V4.3R1 - PRODUCTION SIMULATION",

        "============================================================",

        f"V4.3R1 Total signals: "

        f"{int(total or 0)}",

        f"Open: {open_count}",

        f"Closed: {closed}",

        f"Wins: "

        f"{net_stats['wins']}",

        f"Losses: "

        f"{net_stats['losses']}",

        "",

        "NET PERFORMANCE",

        "============================================================",

        f"Win rate: "

        f"{net_stats['win_rate']:.2f}%",

        f"Average net result: "

        f"{net_stats['avg_result']:.2f}%",

        f"Average net win: "

        f"{net_stats['avg_win']:.2f}%",

        f"Average net loss: "

        f"{net_stats['avg_loss']:.2f}%",

        f"Reward / Risk: "

        f"{net_stats['reward_risk']:.2f}",

        f"Profit Factor: {pf}",

        f"Expectancy / trade: "

        f"{net_stats['expectancy']:.2f}%",

        f"Max winning streak: "

        f"{net_stats['max_winning_streak']}",

        f"Max losing streak: "

        f"{net_stats['max_losing_streak']}",

        "",

        "SIMULATED ACCOUNT",

        "============================================================",

        f"Starting balance: "

        f"{SIM_START_BALANCE:.2f} U",

        f"Current realized balance: "

        f"{balance:.2f} U",

        f"Realized PnL: "

        f"{sum(pnls):+.2f} U",

        f"Realized ROI: "

        f"{roi:+.2f}%",

        f"Max drawdown: "

        f"{max_dd_u:.2f} U "

        f"({max_dd_pct:.2f}%)",

        f"Margin per trade: "

        f"{SIM_MARGIN_USDT:.2f} U",

        f"Sim leverage: "

        f"{SIM_LEVERAGE}x",

        f"Open reserved margin: "

        f"{open_margin:.2f} U",

        f"Open notional exposure: "

        f"{open_notional:.2f} U",

        "",

        "TP HIT RATE - CLOSED V4.3R1 TRADES",

        "============================================================",

        f"TP1: "

        f"{tp1_hits}/{closed} | "

        f"{rate(tp1_hits, closed):.2f}%",

        f"TP2: "

        f"{tp2_hits}/{closed} | "

        f"{rate(tp2_hits, closed):.2f}%",

        f"TP3: "

        f"{tp3_hits}/{closed} | "

        f"{rate(tp3_hits, closed):.2f}%",

        "",

        "BY SIDE / STRATEGY",

        "============================================================",

    ]

    for (

        side,

        strategy,

        stotal,

        sopen,

        swins,

        slosses,

        savg

    ) in strategy_rows:

        resolved = (

            int(swins or 0)

            +

            int(slosses or 0)

        )

        wr = (

            int(swins or 0)

            /

            resolved

            *

            100

            if resolved

            else 0

        )

        lines.append(

            f"{side} {strategy}: "

            f"{stotal} total | "

            f"{int(sopen or 0)} open | "

            f"{int(swins or 0)}W/"

            f"{int(slosses or 0)}L | "

            f"Win {wr:.2f}% | "

            f"Avg net "

            f"{float(savg):.2f}%"

        )

    lines += [

        "",

        "BY SCORE",

        "============================================================",

    ]

    for (

        bucket,

        btotal,

        bwins,

        blosses,

        bavg

    ) in score_rows:

        resolved = (

            int(bwins or 0)

            +

            int(blosses or 0)

        )

        wr = (

            int(bwins or 0)

            /

            resolved

            *

            100

            if resolved

            else 0

        )

        lines.append(

            f"{bucket}: "

            f"{btotal} closed | "

            f"{int(bwins or 0)}W/"

            f"{int(blosses or 0)}L | "

            f"Win {wr:.2f}% | "

            f"Avg net "

            f"{float(bavg):.2f}%"

        )

    lines += [

        "",

        "EXIT REASONS",

        "============================================================",

    ]

    for (

        reason,

        count,

        ewins,

        elosses,

        eavg

    ) in exit_rows:

        resolved = (

            int(ewins or 0)

            +

            int(elosses or 0)

        )

        wr = (

            int(ewins or 0)

            /

            resolved

            *

            100

            if resolved

            else 0

        )

        lines.append(

            f"{reason}: "

            f"{count} | "

            f"{int(ewins or 0)}W/"

            f"{int(elosses or 0)}L | "

            f"Win {wr:.2f}% | "

            f"Avg net "

            f"{float(eavg):.2f}%"

        )

    lines += [

        "",

        "SIDEWAYS STATE",

        "============================================================",

    ]

    for (

        state,

        count,

        wins,

        losses,

        avg

    ) in sideways_rows:

        resolved = (

            int(wins or 0)

            +

            int(losses or 0)

        )

        wr = (

            int(wins or 0)

            /

            resolved

            *

            100

            if resolved

            else 0

        )

        lines.append(

            f"{state}: "

            f"{count} total | "

            f"{int(wins or 0)}W/"

            f"{int(losses or 0)}L | "

            f"Win {wr:.2f}% | "

            f"Avg net "

            f"{float(avg):.2f}%"

        )

    lines += [

        "",

        "V4.2R1 BENCHMARK",

        "============================================================",

        f"V4.2R1 closed: "

        f"{v42_stats['closed']}",

        f"V4.2R1 win rate: "

        f"{v42_stats['win_rate']:.2f}%",

        f"V4.2R1 average net: "

        f"{v42_stats['avg_result']:.2f}%",

        f"V4.2R1 profit factor: "

        f"{v42_pf}",

        "",

        "V4.3R1 RULES",

        "============================================================",

        f"90+ direct tier: "

        f">= {V43_MAIN_SCORE}",

        f"Confirm tier: "

        f">= {V43_CONFIRM_SCORE}",

        f"80-84 enabled: "

        f"{V43_ENABLE_80_84}",

        f"Sideways caution: "

        f">= {V43_SIDEWAYS_CAUTION}",

        f"Sideways block: "

        f">= {V43_SIDEWAYS_BLOCK}",

        f"Breakout exception volume: "

        f">= "

        f"{V43_BREAKOUT_EXCEPTION_VOLUME:.1f}x",

        f"Breakout exception momentum: "

        f">= "

        f"{V43_BREAKOUT_EXCEPTION_MOMENTUM:.1f}%",

        f"Trend timeout: "

        f"{V43_TIMEOUT_TREND_HOURS:.1f}h",

        f"Strong trend timeout: "

        f"{V43_TIMEOUT_STRONG_TREND_HOURS:.1f}h",

        f"Breakout timeout: "

        f"{V43_TIMEOUT_BREAKOUT_HOURS:.1f}h",

        f"Early weak check starts: "

        f"{V43_EARLY_WEAK_MIN_HOURS:.1f}h",

        f"Scanner max open trades: "

        f"{SCANNER_MAX_OPEN}",

        f"TP split: "

        f"{TP1_PCT:.0f}/"

        f"{TP2_PCT:.0f}/"

        f"{TP3_PCT:.0f}",

        f"Scanner targets: "

        f"{SCANNER_TP1:.1f}% / "

        f"{SCANNER_TP2:.1f}% / "

        f"{SCANNER_TP3:.1f}%",

        f"Scanner initial SL: "

        f"{SCANNER_SL:.1f}%",

        f"Daily loss stop: "

        f"{SIM_DAILY_MAX_LOSS_USDT:.2f} U",

    ]

    return (

        "<pre>"

        +

        "\n".join(lines)

        +

        "</pre>",

        200

    )

@app.route(

    "/scanner-status",

    methods=["GET"]

)

def scanner_status_page():

    with scanner_lock:

        status = dict(

            scanner_status

        )

    (

        ok,

        risk_reason

    ) = scanner_risk_allows_new_trade()

    lines = [

        "BingX Scanner V4.3R1",

        "================================",

        f"Web full-scan running: "

        f"{status['running']}",

        f"Phase: "

        f"{status['phase']}",

        f"Current: "

        f"{status['current_symbol']}",

        f"Simulation risk allows "

        f"new trade: {ok}",

        f"Risk reason: "

        f"{risk_reason}",

        "",

        "Primary scanner: "

        "Render Cron -> "

        "scanner_job.py "

        "every 5 minutes",

    ]

    return (

        "<pre>"

        +

        "\n".join(lines)

        +

        "</pre>",

        200

    )

@app.route(

    "/scan-now",

    methods=["GET"]

)

def scan_now():

    return (

        "<pre>"

        "V4.3R1 FAST RADAR "

        "IS CRON-MANAGED\n"

        "Use Render Cron: "

        "scanner_job.py\n"

        "Schedule: "

        "*/5 * * * *\n"

        "</pre>",

        200,

    )

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

        "LINE BingX Bot + "

        "Scanner V4.3R1\n"

        "================================\n"

        f"LINE manual trading: "

        f"{mode}\n"

        f"LINE leverage: "

        f"{LEVERAGE}x\n"

        "Autonomous scanner: "

        "SIMULATION ONLY\n"

        "Scanner version: "

        "V4.3R1\n"

        "Scanner manager: "

        "Render Cron every 5 minutes\n\n"

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

@app.route(

    "/webhook",

    methods=["POST"]

)

def webhook():

    body = request.get_data(

        as_text=True

    )

    signature = (

        request.headers.get(

            "X-Line-Signature",

            ""

        )

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

        hashlib.sha256,

    ).digest()

    expected_signature = (

        base64.b64encode(

            digest

        ).decode(

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

        if (

            event.get("type")

            != "message"

        ):

            continue

        message = event.get(

            "message",

            {}

        )

        if (

            message.get("type")

            != "text"

        ):

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

        if not signal["symbol"]:

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

                relation,

                scanner_trade_id

            ) = register_line_signal(

                signal,

                event_id

            )

            print(

                "LINE RELATION:",

                relation,

                "| scanner trade",

                scanner_trade_id,

                flush=True,

            )

            if (

                relation ==

                "OPPOSITE"

            ):

                update_line_event_status(

                    event_id,

                    "RECORDED_ONLY",

                    "Opposite to open "

                    "scanner trade; "

                    "no LINE order opened.",

                )

                print(

                    "OPPOSITE SIGNAL "

                    "RECORDED ONLY:",

                    signal["symbol"],

                    signal["side"],

                    flush=True,

                )

                continue

            if LIVE_TRADING:

                line_open = (

                    get_live_open_position_count()

                )

                if (

                    line_open >=

                    LINE_MAX_OPEN

                ):

                    update_line_event_status(

                        event_id,

                        "POOL_FULL",

                        "LINE pool full: "

                        f"{line_open}/"

                        f"{LINE_MAX_OPEN}",

                    )

                    print(

                        "LINE POOL FULL:",

                        line_open,

                        "/",

                        LINE_MAX_OPEN,

                        flush=True,

                    )

                    continue

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

                flush=True,

            )

            if (

                entry_result.get(

                    "code"

                ) != 0

            ):

                update_line_event_status(

                    event_id,

                    "ENTRY_ERROR",

                    json.dumps(

                        entry_result,

                        ensure_ascii=False

                    ),

                )

                raise RuntimeError(

                    "Entry failed: "

                    f"{entry_result}"

                )

            if not LIVE_TRADING:

                update_line_event_status(

                    event_id,

                    "TEST_ONLY"

                )

                print(

                    "LINE TEST MODE: "

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

                update_line_event_status(

                    event_id,

                    "NO_ORDER_ID"

                )

                raise RuntimeError(

                    "No orderId returned"

                )

            update_line_event_status(

                event_id,

                "ENTRY_PLACED"

            )

            thread = threading.Thread(

                target=monitor_limit_order,

                args=(

                    signal,

                    event_id,

                    order_id,

                    client_order_id,

                    contract

                ),

                daemon=True,

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

if (

    DATABASE_URL

    and

    ENABLE_WEB_SIM_MONITOR

):

    try:

        start_simulation_monitor()

    except Exception as e:

        print(

            "MONITOR START ERROR:",

            str(e),

            flush=True

        )

from r2_manager import install_r2

install_r2(globals())
