from flask import Flask, request, abort
import os
import hmac
import hashlib
import base64

app = Flask(__name__)

LINE_CHANNEL_SECRET = os.environ.get("LINE_CHANNEL_SECRET", "")

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
                print("LINE MESSAGE:", message.get("text"))

    return "OK", 200
