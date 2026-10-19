"""Fail-closed R1 exchange-side stop coverage verifier (offline).
Does NOT fetch orders, submit orders, or authorize live trading.
Only accepts normalized, independently verified exchange observations.
"""
from decimal import Decimal, InvalidOperation

def decimal(v):
    try:
        x=Decimal(str(v))
        if not x.is_finite(): raise ValueError()
        return x
    except (InvalidOperation,ValueError,TypeError):
        raise ValueError("invalid decimal")

def verify(position, stop_orders):
    """Require matching symbol, side, stop trigger, reduce-only/close-position, quantity.
    Input must be normalized by a separately tested BingX adapter.
    """
    errors=[]
    if not isinstance(position,dict) or not isinstance(stop_orders,list):
        return {"protected":False,"reason":"UNKNOWN_SNAPSHOT"}
    required=("symbol","side","quantity","stop_price")
    if any(k not in position for k in required):
        return {"protected":False,"reason":"MISSING_POSITION_FIELDS"}
    try:
        qty=decimal(position["quantity"])
        stop=decimal(position["stop_price"])
        if qty<=0 or stop<=0 or position["side"] not in ("LONG","SHORT"):
            raise ValueError()
    except ValueError:
        return {"protected":False,"reason":"INVALID_POSITION"}
    covered=Decimal(0)
    seen=set()
    for order in stop_orders:
        if not isinstance(order,dict):
            return {"protected":False,"reason":"UNKNOWN_ORDER_SHAPE"}
        if not all(k in order for k in ("order_id","symbol","position_side","trigger_price","quantity","status","type","reduce_only")):
            return {"protected":False,"reason":"MISSING_STOP_FIELDS"}
        if order["symbol"]!=position["symbol"] or order["position_side"]!=position["side"]:
            continue
        if order["type"] not in ("STOP_MARKET","STOP_LOSS_MARKET") or order["status"] not in ("NEW","ACTIVE"):
            continue
        if order["reduce_only"] is not True:
            continue
        oid=str(order["order_id"])
        if not oid or oid in seen:
            return {"protected":False,"reason":"DUPLICATE_STOP_ID"}
        seen.add(oid)
        try:
            trigger=decimal(order["trigger_price"])
            amount=decimal(order["quantity"])
        except ValueError:
            return {"protected":False,"reason":"INVALID_STOP_VALUES"}
        if amount<=0 or trigger<=0:
            return {"protected":False,"reason":"INVALID_STOP_VALUES"}
        if position["side"]=="LONG" and trigger<stop:
            continue
        if position["side"]=="SHORT" and trigger>stop:
            continue
        covered+=amount
    if covered<qty:
        return {"protected":False,"reason":"INSUFFICIENT_STOP_COVERAGE","covered":str(covered)}
    return {"protected":True,"reason":"NORMALIZED_STOP_COVERAGE_ONLY","covered":str(covered)}

def self_test():
    p={"symbol":"BTC-USDT","side":"LONG","quantity":"0.01","stop_price":"97500"}
    s={"order_id":"stop-1","symbol":"BTC-USDT","position_side":"LONG","trigger_price":"97500",
       "quantity":"0.01","status":"ACTIVE","type":"STOP_MARKET","reduce_only":True}
    assert verify(p,[s])["protected"]
    for bad in ([],[{**s,"quantity":"0.005"}],[{**s,"reduce_only":False}],
                [{**s,"status":"FILLED"}],[{**s,"trigger_price":"97000"}],
                [{**s,"position_side":"SHORT"}],[{**s,"order_id":""}],
                [{**s,"quantity":"NaN"}],[{}]):
        assert not verify(p,bad)["protected"],bad
    assert not verify(None,[])["protected"]
    assert not verify(p,[s,s])["protected"]
    print("PASS: 12 offline stop coverage cases")
    print("NOT LIVE READY: BingX conditional order endpoint and normalized fill/stop adapter unverified")

if __name__=="__main__":
    self_test()
