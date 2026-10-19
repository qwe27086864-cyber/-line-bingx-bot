"""BingX perpetual futures READ-ONLY connectivity and account reconciliation probe.

No order, cancel, leverage, or position-mode changes. Defaults to OFFLINE self-test.
Usage:
  python bingx_readonly.py --self-test
  python bingx_readonly.py --account-check   # requires BINGX_API_KEY / BINGX_SECRET_KEY
Do not publish keys, logs with account details, or screenshots with credentials.
"""
import argparse
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://open-api.bingx.com"
ALLOWED_PATHS = frozenset({
    "/openApi/swap/v2/user/balance",
    "/openApi/swap/v2/user/positions",
    "/openApi/swap/v2/trade/openOrders",
})

def signed_get(path, params=None):
    if path not in ALLOWED_PATHS:
        raise ValueError("Blocked: endpoint not on read-only allowlist")
    key = os.environ.get("BINGX_API_KEY", "")
    secret = os.environ.get("BINGX_SECRET_KEY", "")
    if not key or not secret:
        raise RuntimeError("Missing BingX API credentials")
    payload = dict(params or {})
    payload.update(timestamp=int(time.time() * 1000), recvWindow=5000)
    canonical = urllib.parse.urlencode(sorted(payload.items()))
    signature = hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).hexdigest()
    url = BASE + path + "?" + canonical + "&signature=" + signature
    request = urllib.request.Request(url, headers={"X-BX-APIKEY":key}, method="GET")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            result = json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"BingX HTTP {exc.code} (response suppressed)") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError("BingX connection failed (details suppressed)") from exc
    if str(result.get("code")) not in ("0",):
        raise RuntimeError(f"BingX returned non-success code: {result.get('code')}")
    return result.get("data")

def summarize(data):
    if isinstance(data, list):
        return {"records":len(data)}
    if isinstance(data, dict):
        return {"records":len(data), "shape":"object"}
    return {"shape":type(data).__name__}

def account_check():
    if os.getenv("LIVE_TRADING","false").lower() == "true":
        raise RuntimeError("Refusing account check while LIVE_TRADING=true")
    for name,path in (
        ("balance","/openApi/swap/v2/user/balance"),
        ("positions","/openApi/swap/v2/user/positions"),
        ("open_orders","/openApi/swap/v2/trade/openOrders"),
    ):
        try:
            result = signed_get(path)
            print(json.dumps({"check":name,"status":"OK",**summarize(result)}))
        except Exception as exc:
            print(json.dumps({"check":name,"status":"FAILED","reason":str(exc)}))
            return 1
    print("READ ONLY COMPLETE: no trading actions performed.")
    return 0

def self_test():
    assert all("/user/" in x or x.endswith("/openOrders") for x in ALLOWED_PATHS)
    for path in ("/openApi/swap/v2/trade/order", "/openApi/swap/v2/trade/order/test"):
        try:
            signed_get(path)
        except ValueError:
            pass
        else:
            raise AssertionError("Trading endpoint was not blocked")
    assert summarize([1,2]) == {"records":2}
    print("PASS: endpoint allowlist blocks trade operations; no network calls made.")
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--self-test",action="store_true")
    mode.add_argument("--account-check",action="store_true")
    args = parser.parse_args()
    raise SystemExit(self_test() if args.self_test else account_check())
