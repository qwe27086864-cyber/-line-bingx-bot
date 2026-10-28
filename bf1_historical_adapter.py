from __future__ import annotations
import argparse,csv
from datetime import datetime,timezone
from pathlib import Path

CORE="BF1"
NOTIONAL=100.0
MAX_OPEN=3
SPOT_FEE=0.0010
PERP_FEE=0.0005
SLIP=0.0001
CLOSE_BASIS=0.05
MIN_EDGE=0.10
MIN_FUNDING=0.0
FUNDING_EXIT=-0.0002
MAX_HOURS=48.0

def dt(s):
    x=datetime.fromisoformat(s.replace("Z","+00:00"))
    return x if x.tzinfo else x.replace(tzinfo=timezone.utc)

def snap(r):
    sb,sa,pb,pa=[float(r[k]) for k in ("spot_bid","spot_ask","perp_bid","perp_ask")]
    fr=float(r.get("funding_rate") or 0)
    ob=(pb/sa-1)*100
    cb=(pa/sb-1)*100
    fee=2*(SPOT_FEE+PERP_FEE)*100
    slip=4*SLIP*100
    edge=ob-CLOSE_BASIS+fr*100-fee-slip
    return dict(t=dt(r["timestamp"]),symbol=r["symbol"].upper(),spot_bid=sb,spot_ask=sa,
                perp_bid=pb,perp_ask=pa,funding_rate=fr,
                next_funding_time_ms=int(float(r.get("next_funding_time_ms") or 0)),
                open_basis_pct=ob,close_basis_pct=cb,expected_net_edge_pct=edge)

def should_open(s,n):
    return n<MAX_OPEN and s["funding_rate"]>=MIN_FUNDING and s["open_basis_pct"]>0 and s["expected_net_edge_pct"]>=MIN_EDGE

def should_close(p,s):
    if s["close_basis_pct"]<=CLOSE_BASIS:return "BASIS_CONVERGED"
    if s["funding_rate"]<=FUNDING_EXIT:return "FUNDING_TURNED_ADVERSE"
    if (s["t"]-p["opened_at"]).total_seconds()/3600>=MAX_HOURS:return "MAX_HOLD"
    return None

def close_calc(p,s):
    q=p["qty"]
    gross=q*(s["spot_bid"]-p["entry_spot"])+q*(p["entry_perp"]-s["perp_ask"])+p["funding"]
    fee=q*(p["entry_spot"]*SPOT_FEE+s["spot_bid"]*SPOT_FEE+p["entry_perp"]*PERP_FEE+s["perp_ask"]*PERP_FEE)
    slip=q*(p["entry_spot"]+s["spot_bid"]+p["entry_perp"]+s["perp_ask"])*SLIP
    net=gross-fee-slip
    return net/(q*p["entry_spot"])*100 if q*p["entry_spot"]>0 else 0.0

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--snapshots",required=True,help="timestamp,symbol,spot_bid,spot_ask,perp_bid,perp_ask,funding_rate,next_funding_time_ms")
    ap.add_argument("--out",default="data/bf1_core_outcomes.csv")
    args=ap.parse_args()
    with open(args.snapshots,newline="",encoding="utf-8-sig") as f:
        rows=sorted((snap(r) for r in csv.DictReader(f)),key=lambda x:x["t"])
    opens={}; out=[]
    for s in rows:
        p=opens.get(s["symbol"])
        if p:
            nxt=p["next_funding"]
            if nxt and int(s["t"].timestamp()*1000)>=nxt and p["last_funding"]<nxt:
                mid=(s["perp_bid"]+s["perp_ask"])/2
                p["funding"]+=p["qty"]*mid*s["funding_rate"]
                p["last_funding"]=nxt
                p["next_funding"]=s["next_funding_time_ms"]
            reason=should_close(p,s)
            if reason:
                ret=close_calc(p,s)
                out.append({"timestamp":p["opened_at"].isoformat(),"symbol":s["symbol"],"core":CORE,"net_return_pct":round(ret,6)})
                del opens[s["symbol"]]
        if s["symbol"] not in opens and should_open(s,len(opens)):
            opens[s["symbol"]]=dict(opened_at=s["t"],qty=NOTIONAL/s["spot_ask"],
                entry_spot=s["spot_ask"],entry_perp=s["perp_bid"],funding=0.0,
                last_funding=0,next_funding=s["next_funding_time_ms"])
    Path(args.out).parent.mkdir(parents=True,exist_ok=True)
    with open(args.out,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["timestamp","symbol","core","net_return_pct"]);w.writeheader();w.writerows(out)
    print({"closed":len(out),"sum_net_return_pct":round(sum(float(x["net_return_pct"]) for x in out),3)})
if __name__=="__main__":main()
