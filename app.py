from flask import Flask, request, abort
import os
import hmac
import hashlib
import base64
import re

app = Flask(__name__)

LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")


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

    symbol = re.search(r"幣種\s*[:：]\s*([A-Za-z0-9]+)", text)
    side = re.search(r"方向\s*[:：]\s*(多|空|LONG|SHORT|Long|Short)", text)
    entry = re.search(r"進場(?:價位)?\s*[:：]\s*([0-9.]+)", text)

    tp1 = re.search(r"TP1\s*[:：]\s*([0-9.]+)", text, re.I)
    tp2 = re.search(r"TP2\s*[:：]\s*([0-9.]+)", text, re.I)
    tp3 = re.search(r"TP3\s*[:：]\s*([0-9.]+)", text, re.I)

    sl = re.search(r"(?:SL|止損)\s*[:：]\s*([0-9.]+)", text, re.I)

    if symbol:
        result["symbol"] = symbol.group(1).upper()

    if side:
        s = side.group(1).upper()
        result["side"] = "LONG" if s in ["多", "LONG"] else "SHORT"

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


@app.route("/", methods=["GET"])
def home():
    return "LINE BingX Bot is running", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    body = request.get_data(as_text=True)
    signature = request.headers.get("X-Line-Signature", "")

    if not LINE_CHANNEL_SECRET:
        return "LINE_CHANNEL_SECRET not set", 500

    digest = hmac.new(
        LINE_CHANNEL_SECRET.encode("utf-8"),
        body.encode("utf-8"),
        hashlib.sha256
    ).digest()

    expected_signature = base64.b64encode(digest).decode("utf-8")

    if not hmac.compare_digest(signature, expected_signature):
        abort(400)

    data = request.get_json(silent=True) or {}

    for event in data.get("events", []):
        if event.get("type") == "message":
            message = event.get("message", {})

            if message.get("type") == "text":
                text = message.get("text", "")

                print("LINE MESSAGE:", text)

                signal = parse_signal(text)

                print("PARSED SIGNAL:", signal)

    return "OK", 200
