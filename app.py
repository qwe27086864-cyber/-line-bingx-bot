from flask import Flask, request, abort

import os

import hmac

import hashlib

import base64

import re

import time

import json

import urllib.request

import urllib.error

app = Flask(__name__)

# =========================================================

# Environment Variables

# =========================================================

LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")

BINGX_API_KEY = os.environ.get("BINGX_API_KEY", "")

BINGX_SECRET_KEY = os.environ.get("BINGX_SECRET_KEY", "")

LIVE_TRADING = (

    os.environ.get("LIVE_TRADING", "false")

    .strip()

    .lower()

    == "true"

)

ORDER_USDT_RAW = os.environ.get("ORDER_USDT", "5")

BINGX_POSITION_MODE = (

    os.environ.get("BINGX_POSITION_MODE", "HEDGE")

    .strip()

    .upper()

)

BINGX_BASE_URLS = [

    "https://open-api.bingx.com",

    "https://open-api.bingx.pro",

]

# =========================================================

# Parse LINE Trading Signal

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

        result["symbol"] = symbol.group(1).upper()

    if side:

        s = side.group(1).upper()

        if s in ["多", "LONG"]:

            result["side"] = "LONG"

        else:

            result["side"] = "SHORT"

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

# Validate Signal

# =========================================================

def validate_signal(signal):

    required = [

        "symbol",

        "side",

        "entry",

        "sl",

    ]

    for field in required:

        if not signal.get(field):

            return False, f"Missing field: {field}"

    try:

        entry = float(signal["entry"])

        sl = float(signal["sl"])

        if entry <= 0:

            return False, "Entry must be > 0"

        if sl <= 0:

            return False, "SL must be > 0"

        for name in ["tp1", "tp2", "tp3"]:

            if signal.get(name):

                if float(signal[name]) <= 0:

                    return False, f"{name} must be > 0"

    except ValueError:

        return False, "Invalid numeric value"

    return True, None

# =========================================================

# Order Amount

# =========================================================

def get_order_usdt():

    try:

        value = float(ORDER_USDT_RAW)

    except ValueError:

        raise RuntimeError(

            "ORDER_USDT must be a number"

        )

    if value <= 0:

        raise RuntimeError(

            "ORDER_USDT must be > 0"

        )

    return value

# =========================================================

# BingX Canonical Signing String

# =========================================================

def build_canonical(params):

    items = sorted(

        params.items(),

        key=lambda x: x[0]

    )

    return "&".join(

        f"{key}={value}"

        for key, value in items

    )

# =========================================================

# BingX Private Request

# =========================================================

def bingx_private_request(method, path, params=None):

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

        BINGX_SECRET_KEY.encode("utf-8"),

        canonical.encode("utf-8"),

        hashlib.sha256

    ).hexdigest()

    signed_string = (

        canonical

        + "&signature="

        + signature

    )

    headers = {

        "X-BX-APIKEY": BINGX_API_KEY,

        "X-SOURCE-KEY": "BX-AI-SKILL",

    }

    last_error = None

    for base_url in BINGX_BASE_URLS:

        try:

            if method == "GET":

                url = (

                    base_url

                    + path

                    + "?"

                    + signed_string

                )

                req = urllib.request.Request(

                    url,

                    headers=headers,

                    method="GET"

                )

            elif method == "POST":

                url = (

                    base_url

                    + path

                )

                post_headers = dict(headers)

                post_headers[

                    "Content-Type"

                ] = "application/x-www-form-urlencoded"

                req = urllib.request.Request(

                    url,

                    headers=post_headers,

                    data=signed_string.encode(

                        "utf-8"

                    ),

                    method="POST"

                )

            else:

                raise RuntimeError(

                    f"Unsupported method: {method}"

                )

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

        except Exception as e:

            last_error = e

            continue

    raise RuntimeError(

        f"BingX request failed: {last_error}"

    )

# =========================================================

# BingX Balance Test

# =========================================================

def bingx_get_balance():

    return bingx_private_request(

        "GET",

        "/openApi/swap/v2/user/balance",

        {}

    )

# =========================================================

# Unique Client Order ID

# =========================================================

def make_client_order_id(

    event_id,

    signal

):

    source = (

        str(event_id)

        + "|"

        + str(signal["symbol"])

        + "|"

        + str(signal["side"])

        + "|"

        + str(signal["entry"])

    )

    digest = hashlib.sha256(

        source.encode("utf-8")

    ).hexdigest()

    return (

        "line"

        + digest[:28]

    )

# =========================================================

# Build BingX Order Parameters

# =========================================================

def build_order_params(

    signal,

    event_id

):

    order_usdt = get_order_usdt()

    symbol = (

        f"{signal['symbol']}-USDT"

    )

    if signal["side"] == "LONG":

        side = "BUY"

    else:

        side = "SELL"

    if BINGX_POSITION_MODE == "ONEWAY":

        position_side = "BOTH"

    else:

        position_side = signal["side"]

    params = {

        "symbol": symbol,

        "side": side,

        "positionSide": position_side,

        "type": "MARKET",

        "quoteOrderQty": order_usdt,

        "clientOrderId": (

            make_client_order_id(

                event_id,

                signal

            )

        ),

    }

    # Stop Loss

    if signal.get("sl"):

        stop_loss = {

            "type": "STOP_MARKET",

            "stopPrice": float(

                signal["sl"]

            ),

            "workingType": "MARK_PRICE",

            "stopGuaranteed": False,

        }

        params["stopLoss"] = json.dumps(

            stop_loss,

            separators=(",", ":")

        )

    # TP1 only for now

    if signal.get("tp1"):

        take_profit = {

            "type": "TAKE_PROFIT_MARKET",

            "stopPrice": float(

                signal["tp1"]

            ),

            "workingType": "MARK_PRICE",

            "stopGuaranteed": False,

        }

        params["takeProfit"] = json.dumps(

            take_profit,

            separators=(",", ":")

        )

    return params

# =========================================================

# Submit Test / Live Order

# =========================================================

def submit_order(

    signal,

    event_id

):

    params = build_order_params(

        signal,

        event_id

    )

    print(

        "ORDER PARAMETERS:",

        json.dumps(

            params,

            ensure_ascii=False

        ),

        flush=True

    )

    if LIVE_TRADING:

        print(

            "!!! LIVE TRADING ENABLED !!!",

            flush=True

        )

        path = (

            "/openApi/swap/v2/trade/order"

        )

    else:

        print(

            "TEST ORDER ONLY - NO REAL TRADE",

            flush=True

        )

        path = (

            "/openApi/swap/v2/trade/order/test"

        )

    result = bingx_private_request(

        "POST",

        path,

        params

    )

    return result

# =========================================================

# Home

# =========================================================

@app.route("/", methods=["GET"])

def home():

    if LIVE_TRADING:

        mode = "LIVE"

    else:

        mode = "TEST"

    return (

        f"LINE BingX Bot running - {mode} mode",

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

    signature = request.headers.get(

        "X-Line-Signature",

        ""

    )

    if not LINE_CHANNEL_SECRET:

        return (

            "LINE_CHANNEL_SECRET not set",

            500

        )

    # Verify LINE signature

    digest = hmac.new(

        LINE_CHANNEL_SECRET.encode(

            "utf-8"

        ),

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

            str(time.time_ns())

        )

        print(

            "LINE MESSAGE:",

            text,

            flush=True

        )

        # =================================================

        # BingX connection test

        # =================================================

        if (

            text.strip().upper()

            == "BINGX TEST"

        ):

            try:

                result = (

                    bingx_get_balance()

                )

                print(

                    "BINGX TEST RESULT:",

                    json.dumps(

                        result,

                        ensure_ascii=False

                    ),

                    flush=True

                )

            except Exception as e:

                print(

                    "BINGX TEST ERROR:",

                    str(e),

                    flush=True

                )

            continue

        # =================================================

        # Parse Trading Signal

        # =================================================

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

                "NOT A TRADING SIGNAL",

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

        # =================================================

        # BingX Test / Live Order

        # =================================================

        try:

            result = submit_order(

                signal,

                event_id

            )

            print(

                "BINGX ORDER RESULT:",

                json.dumps(

                    result,

                    ensure_ascii=False

                ),

                flush=True

            )

            if signal.get("tp2"):

                print(

                    "TP2 SAVED ONLY:",

                    signal["tp2"],

                    flush=True

                )

            if signal.get("tp3"):

                print(

                    "TP3 SAVED ONLY:",

                    signal["tp3"],

                    flush=True

                )

        except Exception as e:

            print(

                "BINGX ORDER ERROR:",

                str(e),

                flush=True

            )

    return "OK", 200
