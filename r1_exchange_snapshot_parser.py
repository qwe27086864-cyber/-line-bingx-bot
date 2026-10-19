"""Read-only BingX response parser. NO orders, NO trading authorization.
Run: python r1_exchange_snapshot_parser.py --self-test
"""
import argparse
import json
import os
from decimal import Decimal, InvalidOperation

def _items(data, key):
    if isinstance(data, list):
        rows = data
    elif isinstance(data, dict) and isinstance(data.get(key), list):
        rows = data[key]
    else:
        return None
    return rows if all(isinstance(x, dict) for x in rows) else None

def _nonzero(value):
    try:
        x = Decimal(str(value))
        return x.is_finite() and x != 0
    except (InvalidOperation, ValueError, TypeError):
        return False

def inspect(positions, orders):
    pos = _items(positions, "positions")
    ords = _items(orders, "orders")
    result = {"status":"REVIEW_REQUIRED", "permit_new_entries":False,
              "protective_stops_verified":False,
              "position_records":None if pos is None else len(pos),
              "open_order_records":None if ords is None else len(ords),
              "warnings":[]}
    if pos is None: result["warnings"].append("UNKNOWN_POSITION_ENVELOPE")
    if ords is None: result["warnings"].append("UNKNOWN_ORDER_ENVELOPE")
    if pos is not None:
        for item in pos:
            if not {"symbol","positionSide"}.issubset(item):
                result["warnings"].append("POSITION_FIELDS_UNVERIFIED")
                break
            if "positionAmt" not in item and "positionAmount" not in item:
                result["warnings"].append("POSITION_QUANTITY_UNVERIFIED")
                break
    if ords is not None:
        for item in ords:
            if not {"symbol","orderId","type"}.issubset(item):
                result["warnings"].append("ORDER_FIELDS_UNVERIFIED")
                break
    result["warnings"].append("CONDITIONAL_STOP_ENDPOINT_NOT_VERIFIED")
    return result

def account_check():
    if os.getenv("LIVE_TRADING","false").strip().lower() != "false":
        raise RuntimeError("LIVE_TRADING must be false")
    from bingx_readonly import signed_get
    pos = signed_get("/openApi/swap/v2/user/positions")
    orders = signed_get("/openApi/swap/v2/trade/openOrders")
    print(json.dumps(inspect(pos,orders), sort_keys=True))
    print("READ ONLY; not sufficient to authorize trading")

def self_test():
    assert inspect([],{"orders":[]})["open_order_records"] == 0
    assert inspect([],{"orders":[]})["position_records"] == 0
    assert inspect([],{"orders":[]})["permit_new_entries"] is False
    assert inspect([],{"orders":[]})["protective_stops_verified"] is False
    assert inspect([],{"wrong":[]})["open_order_records"] is None
    assert "UNKNOWN_POSITION_ENVELOPE" in inspect({"x":[]},[])["warnings"]
    assert "ORDER_FIELDS_UNVERIFIED" in inspect([],{"orders":[{}]})["warnings"]
    assert "POSITION_FIELDS_UNVERIFIED" in inspect([{}],[])["warnings"]
    print("PASS: 8 read-only parser checks; unknown data fails closed")

if __name__ == "__main__":
    ap=argparse.ArgumentParser()
    group=ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--self-test",action="store_true")
    group.add_argument("--account-check",action="store_true")
    args=ap.parse_args()
    self_test() if args.self_test else account_check()
