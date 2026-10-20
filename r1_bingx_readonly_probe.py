"""R1 BingX read-only API connectivity check. NEVER submits or cancels orders.

Usage:
    python r1_bingx_readonly_probe.py --self-test
    python r1_bingx_readonly_probe.py --check

Requires BINGX_API_KEY, BINGX_SECRET_KEY for --check.
Do not commit credentials to Git or paste them into chat.
"""
import argparse
import hashlib
import hmac
import json
import os
import time
import urllib.parse
import urllib.request

BASE_URL = "https://open-api.bingx.com"
READ_ONLY_PATHS = {
    "/openApi/swap/v2/user/balance",
    "/openApi/swap/v2/user/positions",
    "/openApi/swap/v2/trade/openOrders",
}


def make_signed_url(path, api_key, secret_key, timestamp_ms):
    if path not in READ_ONLY_PATHS:
        raise ValueError("Endpoint is not on the read-only allowlist")
    if not api_key or not secret_key:
        raise ValueError("BingX API credentials are missing")
    params = urllib.parse.urlencode({"timestamp": int(timestamp_ms), "recvWindow": 5000})
    signature = hmac.new(secret_key.encode("utf-8"), params.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{BASE_URL}{path}?{params}&signature={signature}"


def fetch_readonly(path):
    if os.getenv("LIVE_TRADING", "false").strip().lower() != "false":
        raise RuntimeError("Blocked: LIVE_TRADING must be false for this diagnostic")
    key = os.getenv("BINGX_API_KEY", "")
    secret = os.getenv("BINGX_SECRET_KEY", "")
    url = make_signed_url(path, key, secret, int(time.time() * 1000))
    request = urllib.request.Request(url, headers={"X-BX-APIKEY": key}, method="GET")
    with urllib.request.urlopen(request, timeout=10) as response:
        payload = json.load(response)
    if not isinstance(payload, dict) or payload.get("code") not in (0, "0"):
        raise RuntimeError("BingX read request failed (response details withheld)")
    if "data" not in payload:
        raise RuntimeError("BingX response is missing data")
    return payload["data"]


def self_test():
    url = make_signed_url("/openApi/swap/v2/user/positions", "example", "secret", 123456789)
    assert "signature=" in url and "secret" not in url
    for forbidden in ("/openApi/swap/v2/trade/order", "/openApi/swap/v2/trade/cancel"):
        try:
            make_signed_url(forbidden, "example", "secret", 123456789)
        except ValueError:
            pass
        else:
            raise AssertionError("A write endpoint passed the allowlist")
    print("PASS: signed URL construction and write-endpoint rejection")


def check():
    for path, label in (
        ("/openApi/swap/v2/user/balance", "balance"),
        ("/openApi/swap/v2/user/positions", "positions"),
        ("/openApi/swap/v2/trade/openOrders", "open_orders"),
    ):
        data = fetch_readonly(path)
        print(f"PASS: {label} response type={type(data).__name__}; data redacted")
    print("READ ONLY: no orders submitted, modified, or cancelled")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--self-test", action="store_true")
    mode.add_argument("--check", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
    else:
        check()
