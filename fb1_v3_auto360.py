from __future__ import annotations
import csv, json, math, os, time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

CORE = "FB1_V3_TRUE_BREAK_FAIL_RETEST"
SYMBOLS = [x.strip().upper() for x in os.getenv("FB1_SYMBOLS", "BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT,LTC-USDT,TRX-USDT,BCH-USDT,DOT-USDT,UNI-USDT").split(",") if x.strip()]
SEGMENTS = int(os.getenv("FB1_SEGMENTS", "12"))
DAYS = int(os.getenv("FB1_SEGMENT_DAYS", "30"))
END = os.getenv("FB1_END", "2026-11-02T11:00:00Z")
ROOT = Path(os.getenv("FB1_OUT_DIR", "data/fb1_v3_auto360")); ROOT.mkdir(parents=True, exist_ok=True)
CACHE_BASES = [x.strip().rstrip("/") for x in os.getenv("FB1_CACHE_BASES", "https://lq1-auto360.onrender.com,https://mr1-mean-reversion-auto360.onrender.com").split(",") if x.strip()]
MIN_COVERAGE = float(os.getenv("FB1_MIN_COVERAGE", "0.95"))
COST = float(os.getenv("FB1_ROUND_TRIP_COST_PCT", "0.12"))

# Regime / structure
RANGE_N = int(os.getenv("FB1_RANGE_N", "72"))
ATR_N = int(os.getenv("FB1_ATR_N", "24"))
ER_N = int(os.getenv("FB1_ER_N", "48"))
VOL_N = int(os.getenv("FB1_VOL_N", "24"))
MAX_ER = float(os.getenv("FB1_MAX_ER", "0.30"))
MIN_RANGE_ATR = float(os.getenv("FB1_MIN_RANGE_ATR", "3.2"))
MAX_RANGE_ATR = float(os.getenv("FB1_MAX_RANGE_ATR", "10.0"))
MAX_MA_SEP_ATR = float(os.getenv("FB1_MAX_MA_SEP_ATR", "0.45"))
TOUCH_TOL_ATR = float(os.getenv("FB1_TOUCH_TOL_ATR", "0.22"))
MIN_TOUCHES = int(os.getenv("FB1_MIN_TOUCHES", "2"))

# Three-stage false-break state machine
BREAK_MIN_ATR = float(os.getenv("FB1_BREAK_MIN_ATR", "0.12"))
BREAK_MAX_ATR = float(os.getenv("FB1_BREAK_MAX_ATR", "0.90"))
BREAK_VOL_MIN = float(os.getenv("FB1_BREAK_VOL_MIN", "1.20"))
BREAK_BODY_MIN = float(os.getenv("FB1_BREAK_BODY_MIN", "0.45"))
RECLAIM_BARS = int(os.getenv("FB1_RECLAIM_BARS", "3"))
RECLAIM_IN_ATR = float(os.getenv("FB1_RECLAIM_IN_ATR", "0.08"))
RECLAIM_BODY_MIN = float(os.getenv("FB1_RECLAIM_BODY_MIN", "0.45"))
RECLAIM_VOL_MIN = float(os.getenv("FB1_RECLAIM_VOL_MIN", "0.85"))
RETEST_BARS = int(os.getenv("FB1_RETEST_BARS", "4"))
RETEST_NEAR_ATR = float(os.getenv("FB1_RETEST_NEAR_ATR", "0.18"))
RETEST_HOLD_ATR = float(os.getenv("FB1_RETEST_HOLD_ATR", "0.06"))
STOP_BUF_ATR = float(os.getenv("FB1_STOP_BUF_ATR", "0.12"))
MIN_STOP_PCT = float(os.getenv("FB1_MIN_STOP_PCT", "0.15"))
MAX_STOP_PCT = float(os.getenv("FB1_MAX_STOP_PCT", "1.50"))
MIN_MID_R = float(os.getenv("FB1_MIN_MID_R", "1.25"))
HOLD_BARS = int(os.getenv("FB1_HOLD_BARS", "48"))
COOLDOWN_BARS = int(os.getenv("FB1_COOLDOWN_BARS", "24"))
W_MID = float(os.getenv("FB1_W_MID", "0.65"))
W_FAR = 1.0 - W_MID


def dt(s):
    x = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    return x if x.tzinfo else x.replace(tzinfo=timezone.utc)

def mean(xs): return sum(xs)/len(xs) if xs else 0.0

def tr(b, pc): return max(b["high"]-b["low"], abs(b["high"]-pc), abs(b["low"]-pc))

def pct(a,b): return (a/b-1.0)*100.0 if b else 0.0

def body_frac(b):
    r=max(b["high"]-b["low"],1e-12); return abs(b["close"]-b["open"])/r

def close_pos(b):
    r=max(b["high"]-b["low"],1e-12); return (b["close"]-b["low"])/r

def load(path):
    by=defaultdict(list)
    with path.open(newline="",encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            try:
                by[r["symbol"].upper()].append({"t":dt(r["timestamp"]),"open":float(r["open"]),"high":float(r["high"]),"low":float(r["low"]),"close":float(r["close"]),"vol":float(r.get("quote_volume") or 0) or float(r.get("volume") or 0)*float(r["close"])})
            except Exception: pass
    for s in by: by[s].sort(key=lambda z:z["t"])
    return by

def atr(rows,i): return mean([tr(rows[j],rows[j-1]["close"]) for j in range(i-ATR_N,i)])

def er(rows,i,n):
    net=abs(rows[i-1]["close"]-rows[i-n]["close"])
    den=sum(abs(rows[j]["close"]-rows[j-1]["close"]) for j in range(i-n+1,i))
    return net/den if den else 1.0

def regime(rows,i):
    need=max(RANGE_N,ATR_N,ER_N,VOL_N,48)+2
    if i<need: return None
    base=rows[i-RANGE_N:i]
    a=atr(rows,i)
    if a<=0:return None
    hi=max(x["high"] for x in base);lo=min(x["low"] for x in base)
    width=(hi-lo)/a
    e=er(rows,i,ER_N)
    f=mean([x["close"] for x in rows[i-12:i]]);s=mean([x["close"] for x in rows[i-48:i]])
    ma_sep=abs(f-s)/a
    if not(MIN_RANGE_ATR<=width<=MAX_RANGE_ATR) or e>MAX_ER or ma_sep>MAX_MA_SEP_ATR:return None
    low_touches=sum(1 for x in base[:-4] if x["low"]<=lo+TOUCH_TOL_ATR*a)
    high_touches=sum(1 for x in base[:-4] if x["high"]>=hi-TOUCH_TOL_ATR*a)
    vols=[rows[j]["vol"] for j in range(i-VOL_N,i)];vavg=mean(vols)
    return {"hi":hi,"lo":lo,"atr":a,"er":e,"ma_sep":ma_sep,"low_touches":low_touches,"high_touches":high_touches,"vavg":vavg}

def break_setup(rows,i):
    rg=regime(rows,i)
    if not rg:return None
    b=rows[i];a=rg["atr"];vr=b["vol"]/rg["vavg"] if rg["vavg"]>0 else 0
    if vr<BREAK_VOL_MIN or body_frac(b)<BREAK_BODY_MIN:return None
    # A true fake breakout must first CLOSE outside the established range.
    down=(rg["lo"]-b["close"])/a
    up=(b["close"]-rg["hi"])/a
    if rg["low_touches"]>=MIN_TOUCHES and BREAK_MIN_ATR<=down<=BREAK_MAX_ATR and close_pos(b)<=0.45:
        return {**rg,"side":"LONG","break_i":i,"ext":b["low"],"break_vr":vr}
    if rg["high_touches"]>=MIN_TOUCHES and BREAK_MIN_ATR<=up<=BREAK_MAX_ATR and close_pos(b)>=0.55:
        return {**rg,"side":"SHORT","break_i":i,"ext":b["high"],"break_vr":vr}
    return None

def reclaim(rows,s):
    for j in range(s["break_i"]+1,min(len(rows),s["break_i"]+1+RECLAIM_BARS)):
        b=rows[j]; vr=b["vol"]/s["vavg"] if s["vavg"]>0 else 0
        if s["side"]=="LONG":
            s["ext"]=min(s["ext"],b["low"])
            if b["close"]>=s["lo"]+RECLAIM_IN_ATR*s["atr"] and b["close"]>b["open"] and body_frac(b)>=RECLAIM_BODY_MIN and vr>=RECLAIM_VOL_MIN and close_pos(b)>=0.58:
                return j
        else:
            s["ext"]=max(s["ext"],b["high"])
            if b["close"]<=s["hi"]-RECLAIM_IN_ATR*s["atr"] and b["close"]<b["open"] and body_frac(b)>=RECLAIM_BODY_MIN and vr>=RECLAIM_VOL_MIN and close_pos(b)<=0.42:
                return j
    return None

def retest(rows,s,ri):
    for j in range(ri+1,min(len(rows),ri+1+RETEST_BARS)):
        b=rows[j]
        if s["side"]=="LONG":
            if b["close"]<s["lo"]-0.04*s["atr"]:return None
            near=b["low"]<=s["lo"]+RETEST_NEAR_ATR*s["atr"]
            hold=b["close"]>=s["lo"]+RETEST_HOLD_ATR*s["atr"] and close_pos(b)>=0.55
            if near and hold:return j,b["close"]
        else:
            if b["close"]>s["hi"]+0.04*s["atr"]:return None
            near=b["high"]>=s["hi"]-RETEST_NEAR_ATR*s["atr"]
            hold=b["close"]<=s["hi"]-RETEST_HOLD_ATR*s["atr"] and close_pos(b)<=0.45
            if near and hold:return j,b["close"]
    return None

def simulate(rows,ei,entry,s):
    side=s["side"];a=s["atr"];mid=(s["hi"]+s["lo"])/2;far=s["hi"] if side=="LONG" else s["lo"]
    stop=s["ext"]-STOP_BUF_ATR*a if side=="LONG" else s["ext"]+STOP_BUF_ATR*a
    sp=abs(pct(stop,entry));risk=abs(entry-stop)
    if risk<=0 or not(MIN_STOP_PCT<=sp<=MAX_STOP_PCT):return None
    midr=((mid-entry) if side=="LONG" else (entry-mid))/risk
    farr=((far-entry) if side=="LONG" else (entry-far))/risk
    if midr<MIN_MID_R or farr<=midr:return None
    hit_mid=False;last=entry;reason="TIMEOUT";ret=None
    for j in range(ei+1,min(len(rows),ei+1+HOLD_BARS)):
        b=rows[j];last=b["close"]
        if side=="LONG":
            active=entry if hit_mid else stop
            if b["low"]<=active:
                ret=(W_MID*pct(mid,entry) if hit_mid else -sp);reason="BE_AFTER_MID" if hit_mid else "STOP";break
            if not hit_mid and b["high"]>=mid:hit_mid=True
            if b["high"]>=far:
                ret=W_MID*pct(mid,entry)+W_FAR*pct(far,entry);reason="FAR_BOUNDARY";break
        else:
            active=entry if hit_mid else stop
            if b["high"]>=active:
                ret=(W_MID*pct(entry,mid) if hit_mid else -sp);reason="BE_AFTER_MID" if hit_mid else "STOP";break
            if not hit_mid and b["low"]<=mid:hit_mid=True
            if b["low"]<=far:
                ret=W_MID*pct(entry,mid)+W_FAR*pct(entry,far);reason="FAR_BOUNDARY";break
    if ret is None:
        raw=pct(last,entry) if side=="LONG" else pct(entry,last)
        ret=W_MID*(pct(mid,entry) if side=="LONG" else pct(entry,mid))+W_FAR*raw if hit_mid else raw
    return {"net":ret-COST,"reason":reason,"stop_pct":sp,"mid_r":midr,"far_r":farr}

def run_symbol(rows):
    out=[];cool=-1
    for i in range(len(rows)):
        if i<=cool:continue
        s=break_setup(rows,i)
        if not s:continue
        ri=reclaim(rows,s)
        if ri is None:continue
        rt=retest(rows,s,ri)
        if not rt:continue
        ei,entry=rt;x=simulate(rows,ei,entry,s)
        if not x:continue
        out.append({"side":s["side"],"entry_time":rows[ei]["t"].isoformat(),"net":x["net"],"reason":x["reason"],"stop_pct":x["stop_pct"],"mid_r":x["mid_r"],"break_vr":s["break_vr"],"er":s["er"]})
        cool=ei+COOLDOWN_BARS
    return out

def metrics(ts):
    vals=[x["net"] for x in ts];w=[x for x in vals if x>0];l=[x for x in vals if x<0];gp=sum(w);gl=abs(sum(l))
    return {"trades":len(vals),"wins":len(w),"losses":len(l),"win_rate_pct":round(100*len(w)/len(vals),2) if vals else 0,"profit_factor":round(gp/gl,6) if gl else (999 if gp else 0),"gross_profit_pct_points":round(gp,6),"gross_loss_abs_pct_points":round(gl,6),"sum_net_return_pct":round(sum(vals),6)}

def fetch(idx):
    sd=ROOT/f"segment_{idx:02d}";sd.mkdir(parents=True,exist_ok=True);p=sd/"history_5m.csv"
    last=None
    for base in CACHE_BASES:
        for a in range(4):
            try:
                req=Request(f"{base}/segment_{idx:02d}/history_5m.csv",headers={"User-Agent":"fb1-v3/1.0"})
                with urlopen(req,timeout=60) as r:data=r.read()
                if len(data)>100000:p.write_bytes(data)
                else:raise RuntimeError(f"small cache {len(data)}")
                by=load(p);expected=int(DAYS*24*12);minimum=int(expected*MIN_COVERAGE);counts={s:len(by.get(s,[])) for s in SYMBOLS}
                if any(n<minimum for n in counts.values()):raise RuntimeError(f"coverage {counts}")
                print("FB1V3_CACHE_OK "+json.dumps({"segment":idx,"base":base,"counts":counts},separators=(",",":")),flush=True)
                return p
            except Exception as e:
                last=e;time.sleep(min(3*(a+1),10))
    raise RuntimeError(f"cache unavailable {idx}: {last}")

def main():
    end=dt(END);start=end-timedelta(days=SEGMENTS*DAYS);rows=[];all_trades=[]
    print("FB1V3_START "+json.dumps({"core":CORE,"historical_only":True,"architecture":"confirmed_close_break_then_reclaim_then_inside_retest"},separators=(",",":")),flush=True)
    for idx in range(1,SEGMENTS+1):
        by=load(fetch(idx));ts=[]
        for sym in SYMBOLS:
            q=run_symbol(by.get(sym,[]))
            for x in q:x["symbol"]=sym
            ts+=q
        z=metrics(ts);row={"segment":idx,"start":(start+timedelta(days=(idx-1)*DAYS)).isoformat(),"metrics":z};rows.append(row);all_trades+=ts
        print("FB1V3_SEGMENT_DONE "+json.dumps(row,separators=(",",":")),flush=True)
    final={"core":CORE,**metrics(all_trades),"positive_segments":sum(r["metrics"]["sum_net_return_pct"]>0 for r in rows),"segments":SEGMENTS,"first_screen_pass":False,"segment_results":rows}
    final["first_screen_pass"]=bool(final["trades"]>=120 and final["profit_factor"]>=1.15 and final["sum_net_return_pct"]>0 and final["positive_segments"]>=7)
    (ROOT/"fb1_v3_final.json").write_text(json.dumps(final,indent=2));print("FB1V3_DONE "+json.dumps(final,separators=(",",":")),flush=True)
    while True:time.sleep(3600)
if __name__=="__main__":main()
