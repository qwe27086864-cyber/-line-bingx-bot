"""
BF1 Historical V4 proxy runner (keeps the existing filename so the current
Render historical service can run it without changing its start command).

Historical-only. No DATABASE_URL, no orders, no live/shadow writes.

This 360-day result uses Binance spot + USD-M perpetual + funding history as a
cross-exchange research proxy. It is NOT BingX execution performance. The
existing BingX ~98-day exact-overlap result remains the venue-fidelity check.
"""
from __future__ import annotations
import bisect,csv,json,os,subprocess,sys,time,urllib.error,urllib.parse,urllib.request
from datetime import datetime,timedelta,timezone
from pathlib import Path

os.environ.pop("DATABASE_URL",None)
OUT=Path(os.getenv("BF4_OUT_DIR","data/bf1_v4_proxy")); OUT.mkdir(parents=True,exist_ok=True)
SYMS=[x.strip().upper() for x in os.getenv("BF4_HIST_SYMBOLS","BTC-USDT,ETH-USDT,SOL-USDT").split(",") if x.strip()]
IV=os.getenv("BF4_HIST_INTERVAL","15m")
STEP={"1m":60000,"3m":180000,"5m":300000,"15m":900000,"30m":1800000,"1h":3600000}[IV]
DAYS=int(os.getenv("BF4_HIST_DAYS","360"))
END=datetime.fromisoformat(os.getenv("BF4_HIST_END_ISO","2026-10-28T09:30:00Z").replace("Z","+00:00"))
START=END-timedelta(days=DAYS)
S=int(START.timestamp()*1000); E=int(END.timestamp()*1000)
EXPECTED=(E-S)//STEP; MIN=int(EXPECTED*float(os.getenv("BF4_MIN_COVERAGE","0.95")))
PAUSE=float(os.getenv("BF4_REQUEST_PAUSE_SEC","0.10"))
SPOT_BPS=float(os.getenv("BF4_PROXY_SPOT_SPREAD_BPS","2.0"))
PERP_BPS=float(os.getenv("BF4_PROXY_PERP_SPREAD_BPS","2.0"))
SPOT_BASES=("https://data-api.binance.vision","https://api.binance.com")
FUT_BASES=("https://fapi.binance.com",)
SPOT="/api/v3/klines"; PERP="/fapi/v1/klines"; FUND="/fapi/v1/fundingRate"

def req(bases,path,params,tries=7):
    q=urllib.parse.urlencode(params); last=None
    for a in range(tries):
        for base in bases:
            try:
                r=urllib.request.Request(base+path+"?"+q,headers={"User-Agent":"bf1-v4-proxy/1.0"})
                with urllib.request.urlopen(r,timeout=30) as x: p=json.loads(x.read().decode())
                if isinstance(p,list): return p
                raise RuntimeError(str(p))
            except urllib.error.HTTPError as e:
                last=e
                if e.code not in (408,418,425,429,500,502,503,504): raise
            except Exception as e: last=e
        if a+1<tries:
            d=min(2**a,20); print(f"BF4_RETRY path={path} attempt={a+1}/{tries} delay={d}s error={last}",flush=True); time.sleep(d)
    raise RuntimeError(f"BF4 request failed path={path}: {last}")

def klines(bases,path,sym):
    out={}; cur=S; page=0; rsym=sym.replace("-","")
    while cur<E:
        rows=req(bases,path,{"symbol":rsym,"interval":IV,"startTime":cur,"endTime":E-1,"limit":1000})
        if not rows: break
        stamps=[]
        for r in rows:
            if not isinstance(r,(list,tuple)) or len(r)<5: continue
            t=int(r[0]); stamps.append(t)
            if S<=t<E:
                c=float(r[4])
                if c>0: out[t]=c
        page+=1; print(f"BF4_KLINE path={path} symbol={sym} page={page} rows={len(rows)} total={len(out)}",flush=True)
        if not stamps: break
        nxt=max(stamps)+STEP
        if nxt<=cur: raise RuntimeError(f"BF4 kline pagination stalled {sym} {path}")
        cur=nxt; time.sleep(PAUSE)
    return out

def funding(sym):
    out={}; cur=S; page=0; rsym=sym.replace("-","")
    while cur<E:
        rows=req(FUT_BASES,FUND,{"symbol":rsym,"startTime":cur,"endTime":E-1,"limit":1000})
        if not rows: break
        stamps=[]
        for r in rows:
            if not isinstance(r,dict): continue
            t=int(r.get("fundingTime") or 0)
            if not t: continue
            stamps.append(t)
            if S<=t<E: out[t]=float(r.get("fundingRate") or 0)
        page+=1; print(f"BF4_FUNDING symbol={sym} page={page} rows={len(rows)} total={len(out)}",flush=True)
        if not stamps: break
        nxt=max(stamps)+1
        if nxt<=cur: raise RuntimeError(f"BF4 funding pagination stalled {sym}")
        cur=nxt; time.sleep(PAUSE)
    return sorted(out.items())

def quote(mid,bps):
    h=max(0.0,bps)/20000
    return mid*(1-h),mid*(1+h)

def fund_at(fr,ft,t):
    i=bisect.bisect_left(ft,t)
    return (fr[i][1],fr[i][0]) if i<len(fr) else (0.0,0)

fields=["timestamp","symbol","spot_bid","spot_ask","perp_bid","perp_ask","funding_rate","next_funding_time_ms"]
rows=[]; quality={}
print(f"BF4_PROXY_WINDOW start={START.isoformat()} end={END.isoformat()} days={DAYS} interval={IV} expected={EXPECTED} minimum={MIN} historical_only=true",flush=True)
for sym in SYMS:
    sp=klines(SPOT_BASES,SPOT,sym); pp=klines(FUT_BASES,PERP,sym); fr=funding(sym); ft=[x[0] for x in fr]
    ts=sorted(set(sp)&set(pp))
    quality[sym]={"expected":EXPECTED,"minimum":MIN,"spot_rows":len(sp),"perp_rows":len(pp),"aligned_rows":len(ts),"coverage_pct":round(len(ts)*100/EXPECTED,2) if EXPECTED else 0.0,"funding_events":len(fr)}
    print("BF4_PROXY_QUALITY "+json.dumps({"symbol":sym,**quality[sym]},separators=(",",":")),flush=True)
    for t in ts:
        sb,sa=quote(sp[t],SPOT_BPS); pb,pa=quote(pp[t],PERP_BPS); rate,nft=fund_at(fr,ft,t)
        rows.append({"timestamp":datetime.fromtimestamp(t/1000,tz=timezone.utc).isoformat(),"symbol":sym,"spot_bid":sb,"spot_ask":sa,"perp_bid":pb,"perp_ask":pa,"funding_rate":rate,"next_funding_time_ms":nft})
(OUT/"bf1_v4_proxy_quality.json").write_text(json.dumps(quality,indent=2),encoding="utf-8")
if any(q["aligned_rows"]<MIN for q in quality.values()):
    print("BF4_PROXY_INVALID "+json.dumps(quality,separators=(",",":")),flush=True); raise RuntimeError("BF4 proxy coverage below threshold")
snap=OUT/"bf1_v4_proxy_snapshots.csv"; rows.sort(key=lambda r:(r["timestamp"],r["symbol"]))
with snap.open("w",newline="",encoding="utf-8") as f:
    w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(rows)
out=OUT/"bf1_v4_proxy_outcomes.csv"
subprocess.run([sys.executable,"bf1_historical_adapter.py","--snapshots",str(snap),"--out",str(out)],check=True)
with out.open(newline="",encoding="utf-8-sig") as f: vals=[float(r["net_return_pct"]) for r in csv.DictReader(f)]
wins=[x for x in vals if x>0]; losses=[x for x in vals if x<0]; gp=sum(wins); gl=abs(sum(losses)); pf=gp/gl if gl else (999.0 if gp else 0.0)
summary={"closed":len(vals),"wins":len(wins),"losses":len(losses),"win_rate_pct":round(len(wins)*100/len(vals),2) if vals else 0.0,"profit_factor":round(pf,4),"sum_net_return_pct":round(sum(vals),4),"mode":"360_CROSS_EXCHANGE_PROXY","venue":"Binance spot + USD-M perpetual","days":DAYS,"symbols":SYMS,"coverage_rule":">=95%","warning":"Research proxy only; not BingX execution performance. Existing BingX exact-overlap result remains the venue-fidelity check.","quality":quality}
(OUT/"bf1_v4_proxy_summary.json").write_text(json.dumps(summary,indent=2),encoding="utf-8")
print("BF4_PROXY_RESULT "+json.dumps(summary,separators=(",",":")),flush=True); print("BF4_PROXY_DONE",flush=True)
while True: time.sleep(3600)
