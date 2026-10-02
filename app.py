from flask import Flask, request, abort
import os
import hmac
import hashlib
import base64
import re
import time
import json
import math
import urllib.request
import urllib.error

app = Flask(__name__)


# =========================================================
# Environment Variables
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
        "5"
    )
)

MAX_LIVE_ORDER_USDT = float(
    os.environ.get(
        "MAX_LIVE_ORDER_USDT",
        "5"
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

BINGX_POSITION_MODE = (
    os.environ.get(
        "BINGX_POSITION_MODE",
        "HEDGE"
    )
    .strip()
    .upper()
)

BINGX_BASE_URL = (
    "https://open-api.bingx.com"
)


# =========================================================
# Signal Parser
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
# Validation
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

        sl = float(
            signal["sl"]
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

    except ValueError:

        return (
            False,
            "Invalid number"
        )

    if min(
        entry,
        sl,
        tp1,
        tp2,
        tp3
    ) <= 0:

        return (
            False,
            "Prices must be > 0"
        )

    if (
        TP1_PCT
        + TP2_PCT
        + TP3_PCT
    ) != 100:

        return (
            False,
            "TP percentages must total 100"
        )

    return (
        True,
        None
    )


# =========================================================
# BingX Signing
# =========================================================

def build_canonical(params):

    items = sorted(
        params.items(),
        key=lambda x: x[0]
    )

    return "&".join(
        f"{k}={v}"
        for k, v in items
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

    if params is None:
        params = {}

    params = dict(params)

    params["recvWindow"] = 5000

    params["timestamp"] = int(
        time.time() * 1000
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
            f"BingX HTTP {e.code}: {body}"
        )


# =========================================================
# Public Contract Info
# =========================================================

def get_contract_info(symbol):

    params = {
        "symbol": symbol,
        "timestamp": int(
            time.time() * 1000
        ),
    }

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

    url = (
        BINGX_BASE_URL
        + "/openApi/swap/v2/quote/contracts?"
        + canonical
        + "&signature="
        + signature
    )

    req = urllib.request.Request(
        url,
        headers={
            "X-BX-APIKEY":
            BINGX_API_KEY
        },
        method="GET"
    )

    with urllib.request.urlopen(
        req,
        timeout=15
    ) as response:

        data = json.loads(
            response
            .read()
            .decode("utf-8")
        )

    if data.get("code") != 0:

        raise RuntimeError(
            str(data)
        )

    contracts = data.get(
        "data",
        []
    )

    for item in contracts:

        if (
            item.get("symbol")
            == symbol
        ):

            return item

    raise RuntimeError(
        f"Contract not found: {symbol}"
    )


# =========================================================
# Quantity Precision
# =========================================================

def floor_quantity(
    quantity,
    precision
):

    factor = 10 ** precision

    return (
        math.floor(
            quantity * factor
        )
        / factor
    )


# =========================================================
# Order IDs
# =========================================================

def client_id(
    event_id,
    suffix
):

    raw = (
        str(event_id)
        + str(suffix)
    )

    digest = hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()

    return (
        "line"
        + digest[:25]
        + suffix
    )


# =========================================================
# Main Entry Order
# =========================================================

def place_entry(
    signal,
    event_id
):

    symbol = (
        signal["symbol"]
        + "-USDT"
    )

    side = (
        "BUY"
        if signal["side"] == "LONG"
        else "SELL"
    )

    position_side = (
        signal["side"]
        if BINGX_POSITION_MODE
        == "HEDGE"
        else "BOTH"
    )

    params = {
        "symbol": symbol,
        "side": side,
        "positionSide":
        position_side,

        "type": "MARKET",

        "quoteOrderQty":
        ORDER_USDT,

        "clientOrderId":
        client_id(
            event_id,
            "entry"
        ),
    }

    if LIVE_TRADING:

        if (
            ORDER_USDT
            > MAX_LIVE_ORDER_USDT
        ):

            raise RuntimeError(
                "LIVE ORDER BLOCKED: "
                f"{ORDER_USDT} USDT exceeds "
                f"MAX_LIVE_ORDER_USDT="
                f"{MAX_LIVE_ORDER_USDT}"
            )

        path = (
            "/openApi/swap/v2/trade/order"
        )

    else:

        path = (
            "/openApi/swap/v2/trade/order/test"
        )

    print(
        "ENTRY ORDER:",
        params,
        flush=True
    )

    return (
        bingx_private_request(
            "POST",
            path,
            params
        )
    )


# =========================================================
# Exit Order Builder
# =========================================================

def build_exit_order(
    symbol,
    position_side,
    quantity,
    stop_price,
    order_type,
    event_id,
    suffix
):

    close_side = (
        "SELL"
        if position_side == "LONG"
        else "BUY"
    )

    params = {
        "symbol": symbol,

        "side": close_side,

        "positionSide":
        (
            position_side
            if BINGX_POSITION_MODE
            == "HEDGE"
            else "BOTH"
        ),

        "type": order_type,

        "quantity": quantity,

        "stopPrice":
        stop_price,

        "workingType":
        "MARK_PRICE",

        "clientOrderId":
        client_id(
            event_id,
            suffix
        ),
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
# Split TP + SL
# =========================================================

def place_tp_sl(
    signal,
    event_id,
    filled_qty
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

    precision = int(
        contract.get(
            "quantityPrecision",
            0
        )
    )

    min_qty = float(
        contract.get(
            "tradeMinQuantity",
            0
        )
    )

    q1 = floor_quantity(
        filled_qty
        * TP1_PCT
        / 100,
        precision
    )

    q2 = floor_quantity(
        filled_qty
        * TP2_PCT
        / 100,
        precision
    )

    # Remainder goes to TP3
    q3 = floor_quantity(
        filled_qty
        - q1
        - q2,
        precision
    )

    for name, qty in [
        ("TP1", q1),
        ("TP2", q2),
        ("TP3", q3),
    ]:

        if qty <= 0:

            raise RuntimeError(
                f"{name} quantity "
                f"became zero"
            )

        if (
            min_qty > 0
            and qty < min_qty
        ):

            raise RuntimeError(
                f"{name} quantity "
                f"{qty} below minimum "
                f"{min_qty}"
            )

    tp_orders = [
        (
            q1,
            float(
                signal["tp1"]
            ),
            "tp1"
        ),

        (
            q2,
            float(
                signal["tp2"]
            ),
            "tp2"
        ),

        (
            q3,
            float(
                signal["tp3"]
            ),
            "tp3"
        ),
    ]

    results = []

    for (
        qty,
        price,
        suffix
    ) in tp_orders:

        params = (
            build_exit_order(
                symbol,
                signal["side"],
                qty,
                price,
                "TAKE_PROFIT_MARKET",
                event_id,
                suffix
            )
        )

        print(
            f"{suffix.upper()} ORDER:",
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

        results.append(
            result
        )

    # Full-position SL
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
            "sl"
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

    return {
        "tp_results":
        results,

        "sl_result":
        sl_result
    }


# =========================================================
# Home
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
        f"LINE BingX Bot "
        f"{mode} mode",
        200
    )


# =========================================================
# LINE Webhook
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
            "LINE_CHANNEL_SECRET not set",
            500
        )

    digest = hmac.new(
        LINE_CHANNEL_SECRET
        .encode("utf-8"),

        body.encode("utf-8"),

        hashlib.sha256
    ).digest()

    expected_signature = (
        base64
        .b64encode(digest)
        .decode("utf-8")
    )

    if not hmac.compare_digest(
        signature,
        expected_signature
    ):

        abort(400)

    data = request.get_json(
        silent=True
    ) or {}

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

        if not signal[
            "symbol"
        ]:

            print(
                "NOT A SIGNAL",
                flush=True
            )

            continue

        valid, error = (
            validate_signal(
                signal
            )
        )

        if not valid:

            print(
                "SIGNAL REJECTED:",
                error,
                flush=True
            )

            continue

        try:

            entry_result = (
                place_entry(
                    signal,
                    event_id
                )
            )

            print(
                "ENTRY RESULT:",
                json.dumps(
                    entry_result,
                    ensure_ascii=False
                ),
                flush=True
            )

            # TEST MODE stops here
            if not LIVE_TRADING:

                print(
                    "TEST MODE: "
                    "NO REAL TP/SL ORDERS SENT",
                    flush=True
                )

                continue

            if (
                entry_result.get(
                    "code"
                )
                != 0
            ):

                raise RuntimeError(
                    "Entry order failed"
                )

            order_data = (
                entry_result
                .get(
                    "data",
                    {}
                )
                .get(
                    "order",
                    {}
                )
            )

            filled_qty = float(
                order_data.get(
                    "executedQty",
                    0
                )
            )

            if filled_qty <= 0:

                raise RuntimeError(
                    "Entry filled quantity "
                    "not available"
                )

            exit_results = (
                place_tp_sl(
                    signal,
                    event_id,
                    filled_qty
                )
            )

            print(
                "EXIT ORDERS RESULT:",
                json.dumps(
                    exit_results,
                    ensure_ascii=False
                ),
                flush=True
            )

        except Exception as e:

            print(
                "TRADING ERROR:",
                str(e),
                flush=True
            )

    return (
        "OK",
        200
    )
