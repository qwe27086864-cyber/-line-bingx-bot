"""R1 TP/SL fill accounting simulator. OFFLINE ONLY. No API calls.
Usage: python r1_execution_simulator.py --self-test
"""
from decimal import Decimal

def D(x):
    v=Decimal(str(x))
    if not v.is_finite() or v <= 0:
        raise ValueError("invalid positive number")
    return v

def simulate(side, entry, quantity, stop, targets, weights, events):
    """Process ordered observed fill events. TP quantities based on ORIGINAL size."""
    if side not in ("LONG","SHORT"):
        raise ValueError("invalid side")
    e,q,sl=D(entry),D(quantity),D(stop)
    t=[D(x) for x in targets]
    w=[D(x) for x in weights]
    if len(t)!=3 or len(w)!=3 or sum(w)!=1 or any(x<=0 for x in w):
        raise ValueError("invalid targets/weights")
    if not (sl<e<t[0]<t[1]<t[2] if side=="LONG" else t[2]<t[1]<t[0]<e<sl):
        raise ValueError("invalid stop/targets")
    remaining=q
    realized=Decimal(0)
    used=set()
    fills=[]
    for event in events:
        if not isinstance(event,dict) or event.get("kind") not in ("TP1","TP2","TP3","SL"):
            raise ValueError("unknown event")
        kind=event["kind"]
        if kind in used or remaining<=0:
            raise ValueError("duplicate or post-close fill")
        used.add(kind)
        price=D(event["price"])
        if kind=="SL":
            qty=remaining
            if (side=="LONG" and price>sl) or (side=="SHORT" and price<sl):
                raise ValueError("stop fill above/below configured trigger requires explicit review")
        else:
            index=int(kind[-1])-1
            if any("TP"+str(i) not in used for i in range(1,index+1)):
                raise ValueError("out-of-order TP")
            if (side=="LONG" and price<t[index]) or (side=="SHORT" and price>t[index]):
                raise ValueError("TP price not reached")
            qty=remaining if index==2 else min(remaining,q*w[index])
        realized+=(price-e)*qty*(1 if side=="LONG" else -1)
        remaining-=qty
        fills.append({"kind":kind,"qty":str(qty),"price":str(price)})
    return {"remaining":str(remaining),"gross_pnl":str(realized),"fills":fills,
            "net_pnl_verified":False,"exchange_stops_verified":False,
            "permit_live_orders":False}

def self_test():
    a=simulate("LONG",100,8,97.5,[102,104,108],[.3,.4,.3],
               [{"kind":"TP1","price":102},{"kind":"TP2","price":104},{"kind":"TP3","price":108}])
    assert a["remaining"]=="0.0" and not a["permit_live_orders"]
    b=simulate("SHORT",100,8,102.5,[98,96,92],["0.3","0.4","0.3"],
               [{"kind":"TP1","price":98},{"kind":"SL","price":103}])
    assert Decimal(b["remaining"])==0 and len(b["fills"])==2
    try:
        simulate("LONG",100,8,97.5,[102,104,108],["0.3","0.4","0.3"],
                 [{"kind":"TP2","price":104}])
    except ValueError:
        pass
    else: raise AssertionError("out-of-order fill accepted")
    print("PASS: offline R1 TP1/TP2/TP3, stop, partial-close accounting")
    print("NOT LIVE READY: no fees, funding, slippage, exchange order handling or actual stop placement")

if __name__=="__main__":
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument("--self-test",action="store_true",required=True)
    ap.parse_args()
    self_test()
