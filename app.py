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

                side VARCHAR(10)
                    NOT NULL
                    DEFAULT 'LONG',

                signal_time TIMESTAMPTZ
                    DEFAULT NOW(),

                entry_price DOUBLE PRECISION
                    NOT NULL,

                score INTEGER,

                rsi DOUBLE PRECISION,

                volume_ratio DOUBLE PRECISION,

                trend_15m VARCHAR(20),

                trend_1h VARCHAR(20),

                breakout BOOLEAN
                    DEFAULT FALSE,

                tp1 DOUBLE PRECISION,

                tp2 DOUBLE PRECISION,

                tp3 DOUBLE PRECISION,

                sl DOUBLE PRECISION,

                status VARCHAR(30)
                    DEFAULT 'OPEN',

                tp1_hit BOOLEAN
                    DEFAULT FALSE,

                tp2_hit BOOLEAN
                    DEFAULT FALSE,

                tp3_hit BOOLEAN
                    DEFAULT FALSE,

                sl_hit BOOLEAN
                    DEFAULT FALSE,

                highest_price DOUBLE PRECISION,

                lowest_price DOUBLE PRECISION,

                exit_price DOUBLE PRECISION,

                result_pct DOUBLE PRECISION,

                closed_at TIMESTAMPTZ,

                created_at TIMESTAMPTZ
                    DEFAULT NOW()
            );
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

        result["entry"] = (
            entry.group(1)
        )

    if tp1:

        result["tp1"] = (
            tp1.group(1)
        )

    if tp2:

        result["tp2"] = (
            tp2.group(1)
        )

    if tp3:

        result["tp3"] = (
            tp3.group(1)
        )

    if sl:

        result["sl"] = (
            sl.group(1)
        )

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

        entry = float(
            signal["entry"]
        )

        tp1 = float(
            signal["tp1"]
        )

        tp2 = float(
            signal["tp2"]
        )

        tp3 = float(
            signal["tp3"]
        )

        sl = float(
            signal["sl"]
        )

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
        (
            TP1_PCT
            + TP2_PCT
            + TP3_PCT
        )
        - 100
    ) > 0.0001:

        return (
            False,
            "TP percentages must total 100"
        )

    return (
        True,
        None
    )


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
        time.time()
        * 1000
    )

    canonical = build_canonical(
        params
    )

    signature = hmac.new(
        BINGX_SECRET_KEY.encode(
            "utf-8"
        ),
        canonical.encode(
            "utf-8"
        ),
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
            timeout=15
        ) as response:

            body = (
                response
                .read()
                .decode("utf-8")
            )

            return json.loads(
                body
            )

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

    if BINGX_POSITION_MODE == "HEDGE":

        leverage_side = (
            position_side
        )

    else:

        leverage_side = (
            "BOTH"
        )

    params = {
        "symbol": symbol,
        "side": leverage_side,
        "leverage": LEVERAGE,
    }

    print(
        "SETTING LEVERAGE:",
        params,
        flush=True
    )

    result = (
        bingx_private_request(
            "POST",
            "/openApi/swap/v2/trade/leverage",
            params
        )
    )

    print(
        "LEVERAGE RESULT:",
        json.dumps(
            result,
            ensure_ascii=False
        ),
        flush=True
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

    url = (
        BINGX_BASE_URL
        + "/openApi/swap/v2/quote/contracts"
    )

    with urllib.request.urlopen(
        url,
        timeout=15
    ) as response:

        result = json.loads(
            response
            .read()
            .decode("utf-8")
        )

    if result.get("code") != 0:

        raise RuntimeError(
            f"Contract query failed: "
            f"{result}"
        )

    for item in result.get(
        "data",
        []
    ):

        if item.get(
            "symbol"
        ) == symbol:

            return item

    raise RuntimeError(
        f"Contract not found: "
        f"{symbol}"
    )


# =========================================================
# PRECISION HELPERS
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

    digest = (
        hashlib
        .sha256(
            raw.encode(
                "utf-8"
            )
        )
        .hexdigest()
    )

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

    contract = (
        get_contract_info(
            symbol
        )
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

    symbol = (
        params["symbol"]
    )

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

    if LIVE_TRADING:

        path = (
            "/openApi/swap/v2/trade/order"
        )

    else:

        path = (
            "/openApi/swap/v2/trade/order/test"
        )

    result = (
        bingx_private_request(
            "POST",
            path,
            params
        )
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
        "symbol":
            symbol
    }

    if order_id:

        params[
            "orderId"
        ] = order_id

    elif client_order_id:

        params[
            "clientOrderId"
        ] = client_order_id

    else:

        raise RuntimeError(
            "Missing order ID"
        )

    return (
        bingx_private_request(
            "GET",
            "/openApi/swap/v2/trade/order",
            params
        )
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
        "symbol":
            symbol
    }

    if order_id:

        params[
            "orderId"
        ] = order_id

    else:

        params[
            "clientOrderId"
        ] = client_order_id

    return (
        bingx_private_request(
            "DELETE",
            "/openApi/swap/v2/trade/order",
            params
        )
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

    print(
        "FILLED QTY:",
        filled_qty,
        flush=True
    )

    print(
        "TP SPLIT:",
        {
            "TP1": q1,
            "TP2": q2,
            "TP3": q3,
        },
        flush=True
    )

    checks = [
        (
            "TP1",
            q1,
            float(
                signal["tp1"]
            )
        ),

        (
            "TP2",
            q2,
            float(
                signal["tp2"]
            )
        ),

        (
            "TP3",
            q3,
            float(
                signal["tp3"]
            )
        ),
    ]

    for (
        name,
        qty,
        trigger_price
    ) in checks:

        if qty <= 0:

            raise RuntimeError(
                f"{name} qty "
                f"became zero"
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

        params = (
            build_exit_order(
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
        )

        print(
            name + " ORDER:",
            params,
            flush=True
        )

        result = (
            bingx_private_request(
                "POST",
                "/openApi/swap/v2/trade/order",
                params
            )
        )

        if (
            result.get("code")
            != 0
        ):

            raise RuntimeError(
                f"{name} failed: "
                f"{result}"
            )

        tp_results.append(
            result
        )

    sl_params = (
        build_exit_order(
            symbol,
            signal["side"],
            filled_qty,
            float(
                signal["sl"]
            ),
            "STOP_MARKET",
            event_id,
            "sl",
            quantity_precision,
            price_precision
        )
    )

    print(
        "SL ORDER:",
        sl_params,
        flush=True
    )

    sl_result = (
        bingx_private_request(
            "POST",
            "/openApi/swap/v2/trade/order",
            sl_params
        )
    )

    if (
        sl_result.get("code")
        != 0
    ):

        raise RuntimeError(
            f"SL failed: "
            f"{sl_result}"
        )

    return {
        "tp":
            tp_results,

        "sl":
            sl_result
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

    print(
        "MONITORING LIMIT ORDER:",
        order_id,
        flush=True
    )

    while True:

        try:

            result = (
                query_order(
                    symbol,
                    order_id,
                    client_order_id
                )
            )

            print(
                "ORDER STATUS RESULT:",
                json.dumps(
                    result,
                    ensure_ascii=False
                ),
                flush=True
            )

            if (
                result.get("code")
                != 0
            ):

                raise RuntimeError(
                    str(result)
                )

            order = (
                result.get(
                    "data",
                    {}
                )
            )

            if (
                isinstance(
                    order,
                    dict
                )
                and "order"
                in order
            ):

                order = (
                    order["order"]
                )

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

            print(
                "LIMIT STATUS:",
                status,
                "EXECUTED:",
                executed_qty,
                flush=True
            )

            if status == "FILLED":

                if (
                    executed_qty
                    <= 0
                ):

                    raise RuntimeError(
                        "FILLED but "
                        "executedQty=0"
                    )

                print(
                    "LIMIT FILLED",
                    flush=True
                )

                exits = place_tp_sl(
                    signal,
                    event_id,
                    executed_qty,
                    contract
                )

                print(
                    "EXIT RESULT:",
                    json.dumps(
                        exits,
                        ensure_ascii=False
                    ),
                    flush=True
                )

                return

            if status in [
                "CANCELED",
                "EXPIRED"
            ]:

                print(
                    "ENTRY NO LONGER ACTIVE:",
                    status,
                    flush=True
                )

                return

            if (
                time.time()
                - start
                >= ENTRY_TIMEOUT_SEC
            ):

                print(
                    "ENTRY TIMEOUT - "
                    "CANCELLING LIMIT ORDER",
                    flush=True
                )

                cancel_result = (
                    cancel_order(
                        symbol,
                        order_id,
                        client_order_id
                    )
                )

                print(
                    "CANCEL RESULT:",
                    json.dumps(
                        cancel_result,
                        ensure_ascii=False
                    ),
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
        f"LINE BingX LIMIT Bot "
        f"{mode} mode | "
        f"{LEVERAGE}x | "
        f"DB={'ON' if DATABASE_URL else 'OFF'}",
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

        if (
            event.get("type")
            != "message"
        ):

            continue

        message = (
            event.get(
                "message",
                {}
            )
        )

        if (
            message.get("type")
            != "text"
        ):

            continue

        text = (
            message.get(
                "text",
                ""
            )
        )

        event_id = (
            event.get(
                "webhookEventId",
                str(
                    time.time_ns()
                )
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

            if (
                entry_result.get(
                    "code"
                )
                != 0
            ):

                raise RuntimeError(
                    f"Entry failed: "
                    f"{entry_result}"
                )

            if not LIVE_TRADING:

                print(
                    "TEST MODE: "
                    f"LIMIT VALIDATED | "
                    f"LEVERAGE={LEVERAGE}x | "
                    "NO REAL ORDER CREATED",
                    flush=True
                )

                continue

            data_result = (
                entry_result.get(
                    "data",
                    {}
                )
            )

            order = (
                data_result.get(
                    "order",
                    data_result
                )
            )

            order_id = (
                order.get(
                    "orderId"
                )
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

            print(
                "LIMIT MONITOR STARTED",
                flush=True
            )

        except Exception as e:

            print(
                "TRADING ERROR:",
                str(e),
                flush=True
            )

    return "OK", 200


# =========================================================
# INITIALIZE DATABASE
# =========================================================

try:

    init_database()

except Exception as e:

    print(
        "STARTUP DATABASE ERROR:",
        str(e),
        flush=True
    )
