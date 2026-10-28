from __future__ import annotations
import argparse,csv
from collections import defaultdict
from datetime import datetime,timezone
from pathlib import Path
CORE="MEME2";NOTIONAL=20.;MAX_OPEN=20;MIN_LIQ=10000.;ENTRY_BPS=150.;EXIT_BPS=200.;FEE_BPS=100.;SL=25.;TP=100.;TRAIL_ON=25.;TRAIL_DIST=12.;MAX_HOLD=240.;SIDE=.50
WALLET_MIN_CLOSED=5;WALLET_MIN_PF=.70;SHORT_HOLD=45.;SHORT_EDGE=6.
def dt(s):
    x=datetime.fromisoformat(s.replace("Z","+00:00"));return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
def sf(x,d=0):
    try:return float(x)
    except:return d
def exec_entry(raw,liq):
    reserve=max(1e-9,liq*SIDE);return raw*(1+NOTIONAL/reserve)*(1+ENTRY_BPS/10000)
def exec_exit(raw,qty,liq):
    reserve=max(1e-9,liq*SIDE);v=qty*raw;return max(0,raw*(1-min(.95,EXIT_BPS/10000))/(1+v/reserve))
def stats(hist):
    if not hist:return 0,None,None,None
    gp=sum(max(0,x["pnl"]) for x in hist);gl=abs(sum(min(0,x["pnl"]) for x in hist));pf=gp/gl if gl else None
    hs=sorted(x["hold"] for x in hist);med=hs[len(hs)//2];raw=sum(x["raw"] for x in hist)/len(hist)
    return len(hist),pf,med,raw
def allowed(wallet,hist):
    n,pf,med,raw=stats(hist[wallet])
    if n<WALLET_MIN_CLOSED:return True
    if sum(x["pnl"] for x in hist[wallet])<0 and pf is not None and pf<WALLET_MIN_PF:return False
    if med is not None and raw is not None and med<=SHORT_HOLD and raw<SHORT_EDGE:return False
    return True
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--events",required=True,help="timestamp,wallet,side,mint_address,price_usd,liquidity_usd")
    ap.add_argument("--out",default="data/meme2_core_outcomes.csv");args=ap.parse_args()
    with open(args.events,newline="",encoding="utf-8-sig") as f:rows=sorted(csv.DictReader(f),key=lambda x:x["timestamp"])
    opens={};hist=defaultdict(list);out=[]
    for r in rows:
        t=dt(r["timestamp"]);w=r["wallet"];mint=r["mint_address"];side=r["side"].upper();raw=sf(r["price_usd"]);liq=sf(r["liquidity_usd"])
        key=(w,mint)
        # manage every observed mark for existing position
        p=opens.get(key)
        if p:
            p["peak"]=max(p["peak"],raw);age=(t-p["opened"]).total_seconds()/60
            xp=exec_exit(raw,p["qty"],liq);value=p["qty"]*xp;fee=value*FEE_BPS/10000;net=value-fee-NOTIONAL;ret=net/NOTIONAL*100;reason=None
            if side=="SELL":reason="MIRROR_SELL"
            elif ret<=-SL:reason="STOP_LOSS"
            elif (p["peak"]/p["raw_entry"]-1)*100>=TRAIL_ON and raw<=p["peak"]*(1-TRAIL_DIST/100):reason="TRAILING_STOP"
            elif (raw/p["raw_entry"]-1)*100>=TP:reason="TAKE_PROFIT"
            elif age>=MAX_HOLD:reason="MAX_HOLD"
            if reason:
                hold=(t-p["opened"]).total_seconds();rawmove=(raw/p["raw_entry"]-1)*100
                hist[w].append({"pnl":net,"hold":hold,"raw":rawmove})
                out.append({"timestamp":p["opened"].isoformat(),"symbol":mint,"core":CORE,"net_return_pct":round(ret,6)})
                del opens[key];p=None
        if side!="BUY" or key in opens or len(opens)>=MAX_OPEN or raw<=0 or liq<MIN_LIQ or not allowed(w,hist):continue
        ep=exec_entry(raw,liq);fee=NOTIONAL*FEE_BPS/10000;qty=max(0,NOTIONAL-fee)/ep
        opens[key]={"opened":t,"raw_entry":raw,"qty":qty,"peak":raw}
    Path(args.out).parent.mkdir(parents=True,exist_ok=True)
    with open(args.out,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["timestamp","symbol","core","net_return_pct"]);w.writeheader();w.writerows(out)
    print({"closed":len(out),"sum_net_return_pct":round(sum(float(x["net_return_pct"]) for x in out),3)})
if __name__=="__main__":main()
