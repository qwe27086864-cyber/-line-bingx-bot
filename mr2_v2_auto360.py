from __future__ import annotations
import csv,json,math,os,time
from collections import defaultdict
from datetime import datetime,timedelta,timezone
from pathlib import Path
from urllib.request import Request,urlopen

CORE="MR2_V2_MATURE_RANGE_EDGE_ROTATION"
SYMBOLS=[x.strip().upper() for x in os.getenv("MR2_SYMBOLS","BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT,LTC-USDT,TRX-USDT,BCH-USDT,DOT-USDT,UNI-USDT").split(",") if x.strip()]
SEGMENTS=int(os.getenv("MR2_SEGMENTS","12"));DAYS=int(os.getenv("MR2_SEGMENT_DAYS","30"));END=os.getenv("MR2_END","2026-11-02T11:00:00Z")
ROOT=Path(os.getenv("MR2_OUT_DIR","data/mr2_v2_auto360"));ROOT.mkdir(parents=True,exist_ok=True)
CACHE_BASES=[x.strip().rstrip("/") for x in os.getenv("MR2_CACHE_BASES","https://lq1-auto360.onrender.com,https://mr1-mean-reversion-auto360.onrender.com").split(",") if x.strip()]
MIN_COVERAGE=float(os.getenv("MR2_MIN_COVERAGE","0.95"));COST=float(os.getenv("MR2_ROUND_TRIP_COST_PCT","0.12"))

RANGE_N=int(os.getenv("MR2_RANGE_N","96"));ATR_N=int(os.getenv("MR2_ATR_N","24"));ER_N=int(os.getenv("MR2_ER_N","48"))
MAX_ER=float(os.getenv("MR2_MAX_ER","0.22"));MIN_RANGE_ATR=float(os.getenv("MR2_MIN_RANGE_ATR","3.5"));MAX_RANGE_ATR=float(os.getenv("MR2_MAX_RANGE_ATR","9.0"))
MAX_MA_SEP_ATR=float(os.getenv("MR2_MAX_MA_SEP_ATR","0.28"));MIN_MEAN_CROSSES=int(os.getenv("MR2_MIN_MEAN_CROSSES","4"));CROSS_N=int(os.getenv("MR2_CROSS_N","48"))
TOUCH_TOL_ATR=float(os.getenv("MR2_TOUCH_TOL_ATR","0.22"));MIN_TOUCHES=int(os.getenv("MR2_MIN_TOUCHES","2"));EDGE_FRAC=float(os.getenv("MR2_EDGE_FRAC","0.16"))
MAX_OUTSIDE_ATR=float(os.getenv("MR2_MAX_OUTSIDE_ATR","0.08"));REV_BODY_MIN=float(os.getenv("MR2_REV_BODY_MIN","0.38"));SECOND_HOLD_BARS=int(os.getenv("MR2_SECOND_HOLD_BARS","2"))
STOP_BUF_ATR=float(os.getenv("MR2_STOP_BUF_ATR","0.15"));MIN_STOP_PCT=float(os.getenv("MR2_MIN_STOP_PCT","0.15"));MAX_STOP_PCT=float(os.getenv("MR2_MAX_STOP_PCT","1.40"));MIN_TARGET_R=float(os.getenv("MR2_MIN_TARGET_R","1.30"))
HOLD_BARS=int(os.getenv("MR2_HOLD_BARS","36"));COOLDOWN_BARS=int(os.getenv("MR2_COOLDOWN_BARS","24"))

def dt(s):
    x=datetime.fromisoformat(str(s).replace("Z","+00:00"));return x if x.tzinfo else x.replace(tzinfo=timezone.utc)
def mean(x):return sum(x)/len(x) if x else 0.0
def tr(b,pc):return max(b["high"]-b["low"],abs(b["high"]-pc),abs(b["low"]-pc))
def pct(a,b):return (a/b-1)*100 if b else 0.0
def body_frac(b):return abs(b["close"]-b["open"])/max(b["high"]-b["low"],1e-12)
def close_pos(b):return (b["close"]-b["low"])/max(b["high"]-b["low"],1e-12)
def load(p):
    by=defaultdict(list)
    with p.open(newline="",encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            try:by[r["symbol"].upper()].append({"t":dt(r["timestamp"]),"open":float(r["open"]),"high":float(r["high"]),"low":float(r["low"]),"close":float(r["close"]),"vol":float(r.get("quote_volume") or 0) or float(r.get("volume") or 0)*float(r["close"])})
            except Exception:pass
    for s in by:by[s].sort(key=lambda z:z["t"])
    return by
def atr(rows,i):return mean([tr(rows[j],rows[j-1]["close"]) for j in range(i-ATR_N,i)])
def er(rows,i,n):
    net=abs(rows[i-1]["close"]-rows[i-n]["close"]);den=sum(abs(rows[j]["close"]-rows[j-1]["close"]) for j in range(i-n+1,i));return net/den if den else 1.0
def crossings(vals,level):
    return sum(1 for a,b in zip(vals,vals[1:]) if (a-level)*(b-level)<0)
def regime(rows,i):
    need=max(RANGE_N,ATR_N,ER_N,CROSS_N,48)+2
    if i<need:return None
    base=rows[i-RANGE_N:i];a=atr(rows,i)
    if a<=0:return None
    hi=max(x["high"] for x in base);lo=min(x["low"] for x in base);width=hi-lo
    if width<=0:return None
    e=er(rows,i,ER_N);f=mean([x["close"] for x in rows[i-12:i]]);s=mean([x["close"] for x in rows[i-48:i]]);ma_sep=abs(f-s)/a
    wr=width/a
    mid=(hi+lo)/2;vals=[x["close"] for x in rows[i-CROSS_N:i]];cr=crossings(vals,mid)
    lt=sum(1 for x in base[:-4] if x["low"]<=lo+TOUCH_TOL_ATR*a);ht=sum(1 for x in base[:-4] if x["high"]>=hi-TOUCH_TOL_ATR*a)
    if e>MAX_ER or ma_sep>MAX_MA_SEP_ATR or not(MIN_RANGE_ATR<=wr<=MAX_RANGE_ATR) or cr<MIN_MEAN_CROSSES or lt<MIN_TOUCHES or ht<MIN_TOUCHES:return None
    return {"hi":hi,"lo":lo,"mid":mid,"width":width,"atr":a,"er":e,"crosses":cr,"lt":lt,"ht":ht}
def setup(rows,i):
    rg=regime(rows,i)
    if not rg:return None
    b=rows[i];edge=EDGE_FRAC*rg["width"]
    # MR2 owns INSIDE-range edge reversals only. Any meaningful close outside belongs to FB1.
    if b["close"]<rg["lo"]-MAX_OUTSIDE_ATR*rg["atr"] or b["close"]>rg["hi"]+MAX_OUTSIDE_ATR*rg["atr"]:return None
    if b["low"]<=rg["lo"]+edge and b["close"]>b["open"] and body_frac(b)>=REV_BODY_MIN and close_pos(b)>=0.58:
        return {**rg,"side":"LONG","ext":b["low"],"setup_i":i}
    if b["high"]>=rg["hi"]-edge and b["close"]<b["open"] and body_frac(b)>=REV_BODY_MIN and close_pos(b)<=0.42:
        return {**rg,"side":"SHORT","ext":b["high"],"setup_i":i}
    return None
def second_hold(rows,s):
    for j in range(s["setup_i"]+1,min(len(rows),s["setup_i"]+1+SECOND_HOLD_BARS)):
        b=rows[j]
        if s["side"]=="LONG":
            s["ext"]=min(s["ext"],b["low"])
            if b["close"]<s["lo"]-MAX_OUTSIDE_ATR*s["atr"]:return None
            if b["close"]>s["lo"]+0.12*s["width"] and close_pos(b)>=0.52:return j,b["close"]
        else:
            s["ext"]=max(s["ext"],b["high"])
            if b["close"]>s["hi"]+MAX_OUTSIDE_ATR*s["atr"]:return None
            if b["close"]<s["hi"]-0.12*s["width"] and close_pos(b)<=0.48:return j,b["close"]
    return None
def simulate(rows,ei,entry,s):
    side=s["side"];target=s["mid"];stop=s["ext"]-STOP_BUF_ATR*s["atr"] if side=="LONG" else s["ext"]+STOP_BUF_ATR*s["atr"]
    sp=abs(pct(stop,entry));risk=abs(entry-stop);dist=(target-entry) if side=="LONG" else (entry-target)
    if risk<=0 or dist<=0 or not(MIN_STOP_PCT<=sp<=MAX_STOP_PCT):return None
    rr=dist/risk
    if rr<MIN_TARGET_R:return None
    last=entry;reason="TIMEOUT";ret=None
    for j in range(ei+1,min(len(rows),ei+1+HOLD_BARS)):
        b=rows[j];last=b["close"]
        if side=="LONG":
            if b["low"]<=stop:ret=-sp;reason="STOP";break
            if b["high"]>=target:ret=pct(target,entry);reason="MEAN_TARGET";break
            if b["close"]<s["lo"]-MAX_OUTSIDE_ATR*s["atr"]:ret=pct(b["close"],entry);reason="RANGE_LOST";break
        else:
            if b["high"]>=stop:ret=-sp;reason="STOP";break
            if b["low"]<=target:ret=pct(entry,target);reason="MEAN_TARGET";break
            if b["close"]>s["hi"]+MAX_OUTSIDE_ATR*s["atr"]:ret=pct(entry,b["close"]);reason="RANGE_LOST";break
    if ret is None:ret=pct(last,entry) if side=="LONG" else pct(entry,last)
    return {"net":ret-COST,"reason":reason,"stop_pct":sp,"target_r":rr}
def run_symbol(rows):
    out=[];cool=-1
    for i in range(len(rows)):
        if i<=cool:continue
        s=setup(rows,i)
        if not s:continue
        c=second_hold(rows,s)
        if not c:continue
        ei,entry=c;x=simulate(rows,ei,entry,s)
        if not x:continue
        out.append({"side":s["side"],"entry_time":rows[ei]["t"].isoformat(),"net":x["net"],"reason":x["reason"],"stop_pct":x["stop_pct"],"target_r":x["target_r"],"er":s["er"],"crosses":s["crosses"]});cool=ei+COOLDOWN_BARS
    return out
def metrics(ts):
    vals=[x["net"] for x in ts];w=[x for x in vals if x>0];l=[x for x in vals if x<0];gp=sum(w);gl=abs(sum(l))
    return {"trades":len(vals),"wins":len(w),"losses":len(l),"win_rate_pct":round(100*len(w)/len(vals),2) if vals else 0,"profit_factor":round(gp/gl,6) if gl else (999 if gp else 0),"gross_profit_pct_points":round(gp,6),"gross_loss_abs_pct_points":round(gl,6),"sum_net_return_pct":round(sum(vals),6)}
def fetch(idx):
    sd=ROOT/f"segment_{idx:02d}";sd.mkdir(parents=True,exist_ok=True);p=sd/"history_5m.csv";last=None
    for base in CACHE_BASES:
        for a in range(4):
            try:
                req=Request(f"{base}/segment_{idx:02d}/history_5m.csv",headers={"User-Agent":"mr2-v2/1.0"})
                with urlopen(req,timeout=60) as r:data=r.read()
                if len(data)>100000:p.write_bytes(data)
                else:raise RuntimeError(f"small cache {len(data)}")
                by=load(p);expected=int(DAYS*24*12);minimum=int(expected*MIN_COVERAGE);counts={s:len(by.get(s,[])) for s in SYMBOLS}
                if any(n<minimum for n in counts.values()):raise RuntimeError(f"coverage {counts}")
                print("MR2V2_CACHE_OK "+json.dumps({"segment":idx,"base":base,"counts":counts},separators=(",",":")),flush=True);return p
            except Exception as e:last=e;time.sleep(min(3*(a+1),10))
    raise RuntimeError(f"cache unavailable {idx}: {last}")
def main():
    end=dt(END);start=end-timedelta(days=SEGMENTS*DAYS);rows=[];all_trades=[]
    print("MR2V2_START "+json.dumps({"core":CORE,"historical_only":True,"architecture":"mature_range_inside_edge_rotation"},separators=(",",":")),flush=True)
    for idx in range(1,SEGMENTS+1):
        by=load(fetch(idx));ts=[]
        for sym in SYMBOLS:
            q=run_symbol(by.get(sym,[]))
            for x in q:x["symbol"]=sym
            ts+=q
        z=metrics(ts);row={"segment":idx,"start":(start+timedelta(days=(idx-1)*DAYS)).isoformat(),"metrics":z};rows.append(row);all_trades+=ts
        print("MR2V2_SEGMENT_DONE "+json.dumps(row,separators=(",",":")),flush=True)
    final={"core":CORE,**metrics(all_trades),"positive_segments":sum(r["metrics"]["sum_net_return_pct"]>0 for r in rows),"segments":SEGMENTS,"first_screen_pass":False,"segment_results":rows}
    final["first_screen_pass"]=bool(final["trades"]>=120 and final["profit_factor"]>=1.15 and final["sum_net_return_pct"]>0 and final["positive_segments"]>=7)
    (ROOT/"mr2_v2_final.json").write_text(json.dumps(final,indent=2));print("MR2V2_DONE "+json.dumps(final,separators=(",",":")),flush=True)
    while True:time.sleep(3600)
if __name__=="__main__":main()
