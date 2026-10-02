from flask import Flask, request, abort
import os
import hmac
import hashlib
import base64
import re
import time
import json
import urllib.request
import urllib.parse

app = Flask(__name__)

# =========================
# Environment Variables
# =========================

LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")
BINGX_API_KEY = os.environ.get("BINGX_API_KEY", "")
BINGX_SECRET_KEY = os.environ.get("BINGX_SECRET_KEY", "")


# =========================
# Parse LINE Trading Signal
# =========================

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


# =========================
# Check Required Fields
# =========================

def valid_signal(signal):
    required = [
        "symbol",
        "side",
        "entry",
        "sl"
    ]

    for field in required:
        if not signal.get(field):
            return False

    return True


# =========================
# Create DRY RUN Order
# =========================

def build_dry_run_order(signal):

    symbol = signal["symbol"]

    # BingX perpetual symbol format
    bingx_symbol = f"{symbol}-USDT"

    side = signal["side"]

    if side == "LONG":
        order_side = "BUY"
        position_side = "LONG"

    else:
        order_side = "SELL"
        position_side = "SHORT"

    order = {
        "dry_run": True,
        "symbol": bingx_symbol,
        "side": order_side,
        "positionSide": position_side,
        "orderType": "MARKET",
        "entry_reference": signal["entry"],
        "takeProfit1": signal["tp1"],
        "takeProfit2": signal["tp2"],
        "takeProfit3": signal["tp3"],
        "stopLoss": signal["sl"],
    }

    return order


# =========================
# BingX API Signature
# =========================

def bingx_signature(params):

    query_string = urllib.parse.urlencode(
        sorted(params.items())
    )

    signature = hmac.new(
        BINGX_SECRET_KEY.encode("utf-8"),
        query_string.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    return query_string, signature


# =========================
# BingX Balance Test
# =========================

def bingx_get_balance():

    if not BINGX_API_KEY:
        return {
            "success": False,
            "error": "BINGX_API_KEY not set"
        }

    if not BINGX_SECRET_KEY:
        return {
            "success": False,
            "error": "BINGX_SECRET_KEY not set"
        }

    base_url = "https://open-api.bingx.com"

    path = "/openApi/swap/v2/user/balance"

    params = {
        "recvWindow": 5000,
        "timestamp": int(time.time() * 1000)
    }

    query_string, signature = bingx_signature(params)

    url = (
        base_url
        + path
        + "?"
        + query_string
        + "&signature="
        + signature
    )

    headers = {
        "X-BX-APIKEY": BINGX_API_KEY
    }

    req = urllib.request.Request(
        url,
        headers=headers,
        method="GET"
    )

    try:

        with urllib.request.urlopen(
            req,
            timeout=10
        ) as response:

            body = response.read().decode("utf-8")

            data = json.loads(body)

            return {
                "success": True,
                "response": data
            }

    except Exception as e:

        return {
            "success": False,
            "error": str(e)
        }


# =========================
# Home
# =========================

@app.route("/", methods=["GET"])
def home():

    return "LINE BingX Bot DRY RUN is running", 200


# =========================
# LINE Webhook
# =========================

@app.route("/webhook", methods=["POST"])
def webhook():

    body = request.get_data(as_text=True)

    signature = request.headers.get(
        "X-Line-Signature",
        ""
    )

    if not LINE_CHANNEL_SECRET:
        return "LINE_CHANNEL_SECRET not set", 500


    # Verify LINE signature

    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha256
    ).digest()

    expected_signature = base64.b64encode(
        digest
    ).decode("utf-8")

    if not hmac.compare_digest(
        signature,
        expected_signature
    ):
        abort(400)


    data = request.get_json(
        silent=True
    ) or {}


    for event in data.get("events", []):

        if event.get("type") != "message":
            continue

        message = event.get(
            "message",
            {}
        )

        if message.get("type") != "text":
            continue

        text = message.get(
            "text",
            ""
        )

        print(
            "LINE MESSAGE:",
            text,
            flush=True
        )


        # =========================
        # BingX Connection Test
        # =========================

        if text.strip().upper() == "BINGX TEST":

            print(
                "STARTING BINGX API TEST...",
                flush=True
            )

            result = bingx_get_balance()

            print(
                "BINGX TEST RESULT:",
                json.dumps(
                    result,
                    ensure_ascii=False
                ),
                flush=True
            )

            continue


        # =========================
        # Parse Signal
        # =========================

        signal = parse_signal(text)

        print(
            "PARSED SIGNAL:",
            signal,
            flush=True
        )


        # Ignore normal messages
        if not signal["symbol"]:
            print(
                "NOT A TRADING SIGNAL",
                flush=True
            )

            continue


        # Check required fields
        if not valid_signal(signal):

            print(
                "SIGNAL INCOMPLETE - NO ACTION",
                signal,
                flush=True
            )

            continue


        # =========================
        # DRY RUN
        # =========================

        dry_run_order = build_dry_run_order(
            signal
        )

        print(
            "==============================",
            flush=True
        )

        print(
            "DRY RUN ORDER - NO REAL TRADE",
            flush=True
        )

        print(
            json.dumps(
                dry_run_order,
                ensure_ascii=False,
                indent=2
            ),
            flush=True
        )

        print(
            "==============================",
            flush=True
        )


    return "OK", 200
