from __future__ import annotations
import argparse,csv,math
from datetime import datetime,timezone
from pathlib import Path
CORE="MEME1";NOTIONAL=20.;MAX_OPEN=10;MAX_CAND=8;MIN_LIQ=8000.;MAX_FDV_LIQ=80.;MAX_AGE=180.;MIN_SCORE=70.;MAX_RUG=60.
ENTRY_BPS=150.;EXIT_BPS=200.;FEE_BPS=100.;SL=25.;TP=100.;TRAIL_ON=25.;TRAIL_DIST=12.;MAX_HOLD=240.;LIQ_COLLAPSE=.70;RAPID_DROP=.80;SIDE=.50;MAX_PROFIT_BPS=5000.
def dt(s):
    x=datetime.fromisoformat(s.replace("Z","+00:00"));return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
def sf(x,d=0):
    try:return float(x)
    except:return d
def exec_entry(raw,liq):
    reserve=max(1e-9,liq*SIDE);p=raw*(1+NOTIONAL/reserve)*(1+ENTRY_BPS/10000);return p
def exec_exit(raw,qty,liq):
    reserve=max(1e-9,liq*SIDE);v=qty*raw;p=raw*(1-min(.95,EXIT_BPS/10000))/(1+v/reserve);return max(0,p),(1-max(0,p)/raw)*10000 if raw>0 else 10000
def score(r,t):
    created=dt(r["pool_created_at"]);age=(t-created).total_seconds()/60
    liq=sf(r["liquidity_usd"]);fdv=sf(r["fdv_usd"]);ratio=fdv/liq if liq>0 and fdv>0 else 0
    reasons=[]
    if liq<MIN_LIQ:reasons.append("LOW_LIQUIDITY")
    if age<0 or age>MAX_AGE:reasons.append("AGE_OUTSIDE_WINDOW")
    if ratio>MAX_FDV_LIQ:reasons.append("FDV_LIQ_TOO_HIGH")
    ls=min(20,max(0,20*math.log10(max(liq,1)/MIN_LIQ+1)/math.log10(6)))
    turn=sf(r["volume_h1_usd"])/liq if liq>0 else 0;ts=min(20,turn*12)
    b,s=int(sf(r["buys_m5"])),int(sf(r["sells_m5"]));tot=b+s;br=b/tot if tot else 0
    ps=max(0,min(20,(br-.40)/.35*20));act=min(15,tot/40*15);m=sf(r["price_change_m5_pct"])
    if m<=-10:ms=0
    elif m<=5:ms=(m+10)/15*8
    elif m<=35:ms=8+(m-5)/30*7
    elif m<=80:ms=15-(m-35)/45*6
    else:ms=3
    ages=max(0,10*(1-min(age,MAX_AGE)/MAX_AGE))
    return min(100,max(0,ls+ts+ps+act+ms+ages)),reasons,age
def main():
    ap=argparse.ArgumentParser();ap.add_argument("--snapshots",required=True);ap.add_argument("--out",default="data/meme1_core_outcomes.csv");args=ap.parse_args()
    with open(args.snapshots,newline="",encoding="utf-8-sig") as f: rows=sorted(csv.DictReader(f),key=lambda x:x["timestamp"])
    opens={};out=[]
    for r in rows:
        t=dt(r["timestamp"]);pool=r["pool_address"];raw=sf(r["price_usd"]);liq=sf(r["liquidity_usd"])
        p=opens.get(pool)
        if p:
            p["peak"]=max(p["peak"],raw);age=(t-p["opened"]).total_seconds()/60;reason=None
            if p["entry_liq"]>0 and liq<p["entry_liq"]*LIQ_COLLAPSE:reason="LIQUIDITY_COLLAPSE"
            elif p["last_liq"]>0 and liq<p["last_liq"]*RAPID_DROP and liq<p["entry_liq"]*.90:reason="LIQUIDITY_DRAIN"
            xp,xb=exec_exit(raw,p["qty"],liq);gross=p["qty"]*xp;exitfee=gross*FEE_BPS/10000;ret=(gross-exitfee-NOTIONAL)/NOTIONAL*100
            peakroi=(p["peak"]/p["raw_entry"]-1)*100
            if reason is None and ret<=-SL:reason="STOP_LOSS_EXECUTABLE"
            if reason is None and peakroi>=TRAIL_ON and raw<=p["peak"]*(1-TRAIL_DIST/100):reason="TRAILING_STOP"
            if reason is None and (raw/p["raw_entry"]-1)*100>=TP:reason="TAKE_PROFIT"
            if reason is None and age>=MAX_HOLD:reason="MAX_HOLD"
            if reason in ("TAKE_PROFIT","TRAILING_STOP") and xb>MAX_PROFIT_BPS:reason=None
            p["last_liq"]=liq
            if reason:
                out.append({"timestamp":p["opened"].isoformat(),"symbol":p["mint"],"core":CORE,"net_return_pct":round(ret,6)});del opens[pool]
                continue
        if pool in opens or len(opens)>=MAX_OPEN:continue
        sc,reasons,age=score(r,t);rug=sf(r.get("rug_score"),999)
        if reasons or sc<MIN_SCORE or rug>MAX_RUG or raw<=0:continue
        ep=exec_entry(raw,liq);fee=NOTIONAL*FEE_BPS/10000;qty=max(0,NOTIONAL-fee)/ep
        opens[pool]={"opened":t,"mint":r["mint_address"],"raw_entry":raw,"qty":qty,"entry_liq":liq,"last_liq":liq,"peak":raw}
    Path(args.out).parent.mkdir(parents=True,exist_ok=True)
    with open(args.out,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["timestamp","symbol","core","net_return_pct"]);w.writeheader();w.writerows(out)
    print({"closed":len(out),"sum_net_return_pct":round(sum(float(x["net_return_pct"]) for x in out),3)})
if __name__=="__main__":main()
