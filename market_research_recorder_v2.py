import os,json,time,statistics,threading,urllib.request,urllib.parse,urllib.error,random,psycopg
from collections import defaultdict,deque
from datetime import datetime,timezone
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer

PORT=int(os.getenv("PORT","10000")); BASE="https://open-api.bingx.com"
DBURL=os.getenv("DATABASE_URL","").strip(); TICK=int(os.getenv("TICK_SEC","15"))
FOLLOW=(1,3,5,15,30,60,120,240); FEE=float(os.getenv("SIM_FEE_PER_SIDE_PCT","0.05"))/100
MAX_ACTIVE=int(os.getenv("MAX_ACTIVE","1200")); DEEP=int(os.getenv("DEEP_PER_MIN","5")); NEG=int(os.getenv("NEGATIVE_PER_MIN","3")); WICK_PER_MIN=int(os.getenv("WICK_PER_MIN","20"))
COARSE_CACHE_SEC=int(os.getenv("COARSE_CACHE_SEC","60"))
H=defaultdict(lambda:deque(maxlen=320)); ACTIVE={}; LAST={}; LAST_WICK_BAR={}; PAT=defaultdict(lambda:{"n":0,"wins":0,"sum_net":0.0,"sum_mfe":0.0,"sum_mae":0.0})
REC=deque(maxlen=200); FIN=deque(maxlen=100); CTRL=deque(maxlen=100); FOLLOW_CACHE={}; CONTRACTS=[]; LAST_CONTRACT=0; ROT=0; DB=None
COARSE={"loaded":0.0,"main":{},"mods":{}}
STATE={"service":"market-research-recorder-final","mode":"RESEARCH_ONLY_NO_ORDERS","live_orders":False,"paper_orders":False,"db_enabled":bool(DBURL),"ticks":0,"events":0,"controls":0,"final":0,"wick_symbols_scanned":0,"errors":[],"started":None,
       "coarse_learning":True,"coarse_cache_loaded":False,"coarse_last_refresh":None}
def iso(ts=None): return datetime.fromtimestamp(time.time() if ts is None else ts,timezone.utc).isoformat().replace("+00:00","Z")
STATE["started"]=iso()
def log(t,o): print(t+" "+json.dumps(o,separators=(",",":"),ensure_ascii=False),flush=True)
def error(w,e):
    x={"time":iso(),"where":w,"error":type(e).__name__+":"+str(e)[:180]}; STATE["errors"]=(STATE["errors"]+[x])[-50:]; log("FINAL_ERROR",x)

def getj(path,p=None,tries=3):
    u=BASE+path+("?" + urllib.parse.urlencode(p) if p else ""); last=None
    for i in range(tries):
        t=time.time(); req=urllib.request.Request(u,headers={"User-Agent":"MRFINAL/1.0","Accept":"application/json"})
        try:
            with urllib.request.urlopen(req,timeout=12) as r:return json.loads(r.read().decode()),int((time.time()-t)*1000)
        except urllib.error.HTTPError as e:
            last=e
            if e.code in (429,500,502,503):time.sleep(min(1+i,3));continue
            raise
        except Exception as e:
            last=e;time.sleep(min(1+i,3))
    raise last
def data(o):
    d=o.get("data",o) if isinstance(o,dict) else o
    if isinstance(d,dict):
        for k in ("list","data","symbols","contracts","tickers"):
            if isinstance(d.get(k),list):return d[k]
    return d
def num(d,*ks):
    for k in ks:
        try:
            if d.get(k) is not None:return float(d[k])
        except:pass
    return 0.0
def refresh_contracts():
    global CONTRACTS,LAST_CONTRACT
    if CONTRACTS and time.time()-LAST_CONTRACT<3600:return
    co,_=getj("/openApi/swap/v2/quote/contracts"); out=[]
    for x in data(co) or []:
        sym=str(x.get("symbol","")).upper(); st=str(x.get("status") or x.get("tradeStatus") or "").upper()
        if sym.endswith("-USDT") and not sym.startswith(("NC","NCSK")) and st not in ("0","OFFLINE","SUSPEND","SUSPENDED","CLOSED"):out.append(sym)
    CONTRACTS=sorted(set(out));LAST_CONTRACT=time.time()

def ticker():
    refresh_contracts(); allowed=set(CONTRACTS)
    o,lat=getj("/openApi/swap/v2/quote/ticker"); out={}
    for x in data(o) or []:
        sym=str(x.get("symbol","")).upper()
        if sym not in allowed:continue
        p=num(x,"lastPrice","price","close")
        if p<=0:continue
        qv=num(x,"quoteVolume","quoteVol","amount"); qv=qv or num(x,"volume","baseVolume")*p
        out[sym]={"p":p,"qv":qv}
    return out,lat
def kl(s,iv,n):
    o,lat=getj("/openApi/swap/v3/quote/klines",{"symbol":s,"interval":iv,"limit":n}); r=[]
    for x in data(o) or []:
        try:
            if isinstance(x,dict):t=float(x.get("time") or x.get("timestamp") or x.get("openTime"));a=(float(x["open"]),float(x["high"]),float(x["low"]),float(x["close"]),float(x.get("volume") or 0))
            else:t=float(x[0]);a=tuple(map(float,x[1:6]))
            if t>1e11:t/=1000
            r.append((t,*a))
        except:pass
    r=sorted({x[0]:x for x in r}.values()); sec=60 if iv=="1m" else 300; now=time.time()
    return [x for x in r if x[0]+sec<=now],lat
def atr(r,n=14):
    if len(r)<n+1:return 0
    tr=[max(r[i][2]-r[i][3],abs(r[i][2]-r[i-1][4]),abs(r[i][3]-r[i-1][4])) for i in range(1,len(r))]
    return statistics.mean(tr[-n:])
def rh(s,sec):
    h=H.get(s)
    if not h:return None
    target=h[-1][0]-sec
    for t,p in reversed(h):
        if t<=target:return h[-1][1]/p-1
    return None
def depth(s):
    o,lat=getj("/openApi/swap/v2/quote/depth",{"symbol":s,"limit":20}); d=data(o); bids=(d or {}).get("bids",[]) if isinstance(d,dict) else []; asks=(d or {}).get("asks",[]) if isinstance(d,dict) else []
    def pp(a):
        z=[]
        for x in a:
            try:z.append((float(x[0] if not isinstance(x,dict) else x["price"]),float(x[1] if not isinstance(x,dict) else x.get("quantity") or x.get("qty"))))
            except:pass
        return z
    b=sorted(pp(bids),reverse=True); a=sorted(pp(asks))
    if not b or not a:return {"spread_pct":None,"imbalance_10":None,"slip_buy_100":None,"slip_sell_100":None},lat
    mid=(b[0][0]+a[0][0])/2; db=sum(p*q for p,q in b[:10]); da=sum(p*q for p,q in a[:10])
    def slip(levels,side):
        rem=100; cost=qty=0
        for p,q in levels:
            take=min(rem,p*q); qty+=take/p; cost+=take; rem-=take
            if rem<=0:break
        if rem>0 or qty<=0:return None
        av=cost/qty; return ((av/mid-1) if side=="buy" else (mid/av-1))*100
    return {"best_bid":b[0][0],"best_ask":a[0][0],"spread_pct":(a[0][0]-b[0][0])/mid*100,"imbalance_10":(db-da)/(db+da) if db+da else 0,"depth_bid_usd_10":db,"depth_ask_usd_10":da,"slip_buy_100":slip(a,"buy"),"slip_sell_100":slip(b,"sell")},lat
def oi_fun(s):
    oi=fr=None; loi=lfr=None
    try:o,loi=getj("/openApi/swap/v2/quote/openInterest",{"symbol":s}); d=data(o); d=d[0] if isinstance(d,list) and d else d; oi=num(d or {},"openInterest","openInterestValue","amount")
    except:pass
    try:o,lfr=getj("/openApi/swap/v2/quote/premiumIndex",{"symbol":s}); d=data(o); d=d[0] if isinstance(d,list) and d else d; fr=num(d or {},"lastFundingRate","fundingRate")
    except:pass
    return oi,fr,loi,lfr

def dbc():
    global DB
    if not DBURL:return None
    try:
        if DB is None or DB.closed:DB=psycopg.connect(DBURL,autocommit=True,connect_timeout=5)
        return DB
    except Exception as e:DB=None;error("db_connect",e);return None
def q(sql,args=()):
    global DB
    c=dbc()
    if not c:return None
    try:return c.execute(sql,args)
    except Exception as e:
        try:DB.close()
        except:pass
        DB=None;error("db_exec",e);return None

def coarse_tables():
    if not DBURL:return
    for sql in [
      """create table if not exists market_research_coarse_stats(
           event_type text primary key,n integer not null,wins integer not null,
           win_pct double precision,avg_net_pct double precision,sum_net_pct double precision,
           updated_at timestamptz not null default now())""",
      """create table if not exists market_research_coarse_modifiers(
           event_type text not null,dimension text not null,value text not null,
           n integer not null,wins integer not null,win_pct double precision,
           avg_net_pct double precision,base_avg_net_pct double precision,
           delta_vs_base_net_pct double precision,updated_at timestamptz not null default now(),
           primary key(event_type,dimension,value))""",
      """create table if not exists market_research_coarse_meta(
           key text primary key,value jsonb not null,updated_at timestamptz not null default now())"""
    ]:
        q(sql)

def coarse_reload(force=False):
    if not DBURL:return
    if not force and time.time()-COARSE["loaded"]<COARSE_CACHE_SEC:return
    try:
        main={};mods={}
        c=q("select event_type,n,wins,win_pct,avg_net_pct,sum_net_pct from market_research_coarse_stats")
        if c:
            for et,n,w,wr,av,sn in c.fetchall():
                main[et]={"n":int(n),"wins":int(w),"win_pct":float(wr or 0),"avg_net_pct":float(av or 0),"sum_net_pct":float(sn or 0)}
        c=q("select event_type,dimension,value,n,wins,win_pct,avg_net_pct,base_avg_net_pct,delta_vs_base_net_pct from market_research_coarse_modifiers")
        if c:
            for et,dim,val,n,w,wr,av,bav,delta in c.fetchall():
                mods[(et,dim,val)]={"n":int(n),"wins":int(w),"win_pct":float(wr or 0),"avg_net_pct":float(av or 0),
                                    "base_avg_net_pct":float(bav or 0),"delta_vs_base_net_pct":float(delta or 0)}
        COARSE["main"]=main;COARSE["mods"]=mods;COARSE["loaded"]=time.time()
        STATE["coarse_cache_loaded"]=True;STATE["coarse_last_refresh"]=iso()
    except Exception as ex:error("coarse_reload",ex)

def coarse_update_event(e,net_pct,success):
    if not DBURL or e.get("event_type")=="NORMAL_CONTROL":return
    try:
        et=e["event_type"];win=1 if success else 0
        c=q("""insert into market_research_coarse_stats(event_type,n,wins,win_pct,avg_net_pct,sum_net_pct,updated_at)
               values(%s,1,%s,%s,%s,%s,now())
               on conflict(event_type) do update set
                 n=market_research_coarse_stats.n+1,
                 wins=market_research_coarse_stats.wins+excluded.wins,
                 sum_net_pct=market_research_coarse_stats.sum_net_pct+excluded.sum_net_pct,
                 win_pct=100.0*(market_research_coarse_stats.wins+excluded.wins)/(market_research_coarse_stats.n+1),
                 avg_net_pct=(market_research_coarse_stats.sum_net_pct+excluded.sum_net_pct)/(market_research_coarse_stats.n+1),
                 updated_at=now()
               returning n,wins,win_pct,avg_net_pct,sum_net_pct""",
            (et,win,100.0 if success else 0.0,net_pct,net_pct))
        base=c.fetchone() if c else None
        base_avg=float(base[3]) if base else 0.0

        parts=(e.get("pattern_key") or "").split("|")
        dims={
          "trend":parts[1] if len(parts)>1 else None,
          "volume":parts[2] if len(parts)>2 else None,
          "spread":parts[3] if len(parts)>3 else None,
          "regime":parts[4] if len(parts)>4 else None,
          "atr":parts[5] if len(parts)>5 else None,
          "session":parts[6] if len(parts)>6 else None,
        }
        for dim,val in dims.items():
            if not val:continue
            q("""insert into market_research_coarse_modifiers(event_type,dimension,value,n,wins,win_pct,avg_net_pct,base_avg_net_pct,delta_vs_base_net_pct,updated_at)
                 values(%s,%s,%s,1,%s,%s,%s,%s,%s,now())
                 on conflict(event_type,dimension,value) do update set
                   n=market_research_coarse_modifiers.n+1,
                   wins=market_research_coarse_modifiers.wins+excluded.wins,
                   win_pct=100.0*(market_research_coarse_modifiers.wins+excluded.wins)/(market_research_coarse_modifiers.n+1),
                   avg_net_pct=(market_research_coarse_modifiers.avg_net_pct*market_research_coarse_modifiers.n+excluded.avg_net_pct)/(market_research_coarse_modifiers.n+1),
                   base_avg_net_pct=excluded.base_avg_net_pct,
                   delta_vs_base_net_pct=((market_research_coarse_modifiers.avg_net_pct*market_research_coarse_modifiers.n+excluded.avg_net_pct)/(market_research_coarse_modifiers.n+1))-excluded.base_avg_net_pct,
                   updated_at=now()""",
              (et,dim,val,win,100.0 if success else 0.0,net_pct,base_avg,net_pct-base_avg))
        meta={"service":STATE["service"],"mode":"INCREMENTAL_PER_NEW_OUTCOME","last_event_id":e.get("id"),
              "last_event_type":et,"raw_fine_patterns_preserved":True,"updated_at":iso()}
        q("""insert into market_research_coarse_meta(key,value,updated_at) values('learning_state',%s::jsonb,now())
             on conflict(key) do update set value=market_research_coarse_meta.value || excluded.value,updated_at=now()""",
          (json.dumps(meta,separators=(",",":")),))
        COARSE["loaded"]=0
    except Exception as ex:error("coarse_update_event",ex)

def coarse_hint(event_type,pattern_key):
    coarse_reload()
    base=COARSE["main"].get(event_type)
    if not base:
        return {"hint":"COARSE_INSUFFICIENT","n":0,"base_avg_net_pct":None,"base_win_pct":None,
                "modifier_adjustment":0.0,"score":None,"modifiers":[]}
    parts=(pattern_key or "").split("|")
    dims=[
      ("trend",parts[1] if len(parts)>1 else None),
      ("volume",parts[2] if len(parts)>2 else None),
      ("spread",parts[3] if len(parts)>3 else None),
      ("regime",parts[4] if len(parts)>4 else None),
      ("atr",parts[5] if len(parts)>5 else None),
      ("session",parts[6] if len(parts)>6 else None),
    ]
    contrib=[];adj=0.0
    for dim,val in dims:
        m=COARSE["mods"].get((event_type,dim,val))
        if not m or m["n"]<30:continue
        shrink=m["n"]/(m["n"]+50.0)
        delta=max(-3.0,min(3.0,m["delta_vs_base_net_pct"]))
        c=delta*shrink*0.15
        adj+=c
        contrib.append({"dimension":dim,"value":val,"n":m["n"],"delta":round(m["delta_vs_base_net_pct"],4),
                        "shrink":round(shrink,4),"contribution":round(c,4)})
    adj=max(-1.5,min(1.5,adj))
    score=base["avg_net_pct"]+adj
    n=base["n"];wr=base["win_pct"]
    if n<100:hint="COARSE_INSUFFICIENT"
    elif score>=0.35 and wr>=52.0:hint="COARSE_FAVOR"
    elif score<=-0.35 or wr<=45.0:hint="COARSE_AVOID"
    else:hint="COARSE_NEUTRAL"
    return {"hint":hint,"n":n,"base_avg_net_pct":round(base["avg_net_pct"],4),"base_win_pct":round(wr,2),
            "modifier_adjustment":round(adj,4),"score":round(score,4),"modifiers":contrib}

def dbinit():
    if not DBURL:log("FINAL_DB_DISABLED",{"reason":"DATABASE_URL_missing"});return
    for x in [
      """create table if not exists market_research_v8_events(event_id text primary key,created_at timestamptz default now(),updated_at timestamptz default now(),symbol text,event_type text,cluster_id text,payload jsonb not null,finalized boolean default false)""",
      """create table if not exists market_research_v8_outcomes(event_id text primary key,finalized_at timestamptz default now(),symbol text,event_type text,pattern_key text,success boolean,net_pct double precision,mfe_pct double precision,mae_pct double precision,best_entry text,best_horizon_min integer,failure_reasons jsonb,payload jsonb not null)""",
      """create table if not exists market_research_v8_pattern_stats(pattern_key text primary key,n integer,wins integer,sum_net double precision,sum_mfe double precision,sum_mae double precision,updated_at timestamptz default now())""",
      """create table if not exists market_research_v8_heartbeat(ts timestamptz default now(),service text,active integer,payload jsonb)"""]:
        if q(x) is None:return
    coarse_tables()
    c=q("select pattern_key,n,wins,sum_net,sum_mfe,sum_mae from market_research_v8_pattern_stats")
    if c:
        for k,n,w,sn,sm,sa in c.fetchall():PAT[k]={"n":n,"wins":w,"sum_net":sn,"sum_mfe":sm,"sum_mae":sa}
    c=q("select payload from market_research_v8_events where finalized=false and updated_at>now()-interval '5 hours'")
    restored=0
    if c:
        for (e,) in c.fetchall():
            if isinstance(e,dict) and e.get("id") and e.get("_t0"):ACTIVE[e["id"]]=e; restored+=1
    coarse_reload(True)
    log("FINAL_DB_READY",{"patterns":len(PAT),"active_restored":restored,"coarse_main":len(COARSE["main"]),"coarse_mods":len(COARSE["mods"])})

def save(e,final=False):
    if not DBURL:return
    q("""insert into market_research_v8_events(event_id,symbol,event_type,cluster_id,payload,finalized) values(%s,%s,%s,%s,%s::jsonb,%s)
         on conflict(event_id) do update set updated_at=now(),payload=excluded.payload,finalized=(market_research_v8_events.finalized or excluded.finalized)""",
      (e["id"],e["symbol"],e["event_type"],e.get("cluster_id"),json.dumps(e,separators=(",",":"),ensure_ascii=False),final))
def finish(e):
    save(e,True); en=e["entries"]["market"]; pk=e["pattern_key"]; bh=e.get("best_exit_horizon_min_by_entry") or {}; hz=bh.get(e.get("best_entry_240m"))
    c=q("""insert into market_research_v8_outcomes(event_id,symbol,event_type,pattern_key,success,net_pct,mfe_pct,mae_pct,best_entry,best_horizon_min,failure_reasons,payload)
         values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb) on conflict(event_id) do nothing returning event_id""",
      (e["id"],e["symbol"],e["event_type"],pk,e["success"],en["net"],en["mfe"],en["mae"],e.get("best_entry_240m"),hz,json.dumps(e.get("failure_reasons") or []),json.dumps(e,separators=(",",":"),ensure_ascii=False)))
    inserted=c.fetchone() if c else None
    p=PAT[pk];q("""insert into market_research_v8_pattern_stats values(%s,%s,%s,%s,%s,%s,now()) on conflict(pattern_key) do update set n=excluded.n,wins=excluded.wins,sum_net=excluded.sum_net,sum_mfe=excluded.sum_mfe,sum_mae=excluded.sum_mae,updated_at=now()""",(pk,p["n"],p["wins"],p["sum_net"],p["sum_mfe"],p["sum_mae"]))
    if inserted:coarse_update_event(e,en["net"],e["success"])

def pattern_hint(pk):
    p=PAT.get(pk) or {"n":0,"wins":0,"sum_net":0.0}; n=int(p.get("n",0) or 0); wins=int(p.get("wins",0) or 0)
    wr=(wins/n) if n else None; avg=(p.get("sum_net",0)/n) if n else None
    ci=None
    if n:
        z=1.96; den=1+z*z/n; center=(wr+z*z/(2*n))/den; half=z*((wr*(1-wr)/n+z*z/(4*n*n))**0.5)/den;ci=[max(0,center-half),min(1,center+half)]
    if n<30:hint="INSUFFICIENT_SAMPLE"
    elif n<80:hint="PRELIM_FAVOR" if wr>=.60 and avg>0 else ("PRELIM_AVOID" if wr<=.40 or avg<0 else "PRELIM_NEUTRAL")
    else:hint="FAVOR" if wr>=.60 and avg>0 else ("AVOID" if wr<=.40 or avg<0 else "NEUTRAL")
    out={"n":n,"win_rate":wr,"win_rate_95ci":ci,"avg_net_pct":avg,"hint":hint,"best_entry":None,"best_horizon_min":None,"top_failure_reasons":[]}
    if DBURL and n:
        try:
            c=q("select best_entry,count(*) c from market_research_v8_outcomes where pattern_key=%s and best_entry is not null group by best_entry order by c desc limit 1",(pk,)); r=c.fetchone() if c else None
            if r:out["best_entry"]={"value":r[0],"count":int(r[1])}
            c=q("select best_horizon_min,count(*) c from market_research_v8_outcomes where pattern_key=%s and best_horizon_min is not null group by best_horizon_min order by c desc limit 1",(pk,));r=c.fetchone() if c else None
            if r:out["best_horizon_min"]={"value":int(r[0]),"count":int(r[1])}
            c=q("""select x,count(*) c from market_research_v8_outcomes,lateral jsonb_array_elements_text(failure_reasons) x
                   where pattern_key=%s group by x order by c desc limit 3""",(pk,))
            if c:out["top_failure_reasons"]=[{"reason":r[0],"count":int(r[1])} for r in c.fetchall()]
        except Exception as ex:error("pattern_hint",ex)
    return out

def follow_ctx(s):
    key=(s,int(time.time()//45))
    if key in FOLLOW_CACHE:return FOLLOW_CACHE[key]
    r1,l1=kl(s,"1m",25); d,ld=depth(s); oi,fr,loi,lfr=oi_fun(s)
    vr=None
    if len(r1)>=12:
        vm=statistics.median([x[5] for x in r1[-12:-2] if x[5]>0] or [1]); vr=r1[-1][5]/vm if vm else None
    out={"time":iso(),"oi":oi,"funding":fr,"volume_ratio":vr,"book_imbalance":d.get("imbalance_10"),"spread_pct":d.get("spread_pct"),
         "api_latency_ms":{"kline1":l1,"depth":ld,"oi":loi,"funding":lfr}}
    FOLLOW_CACHE.clear(); FOLLOW_CACHE[key]=out
    return out

def feat(s,tick,r1,r5,breadth,l1=None,l5=None):
    p=tick[s]["p"]; a=atr(r1); prev=r1[-21:-1]; sup=min(x[3] for x in prev); res=max(x[2] for x in prev); vm=statistics.median([x[5] for x in prev if x[5]>0] or [1]); d,ld=depth(s); oi,fr,loi,lfr=oi_fun(s)
    c5=[x[4] for x in r5]; slope5=(c5[-1]/c5[-4]-1) if len(c5)>=4 else 0
    trend5="UP" if slope5>.001 else ("DOWN" if slope5<-.001 else "FLAT")
    h=datetime.now(timezone.utc).hour; session="ASIA" if h<8 else ("EUROPE" if h<16 else "US")
    flags=[]
    if d.get("spread_pct") is None:flags.append("BOOK_MISSING")
    if oi is None:flags.append("OI_MISSING")
    lat=[x for x in (l1,l5,ld,loi,lfr) if x is not None]
    if any(x>1500 for x in lat):flags.append("API_SLOW")
    atrp=a/p if p else 0
    return {"atr":a,"atr_pct":atrp,"support20":sup,"resistance20":res,"volume_ratio":r1[-1][5]/vm,
      "r1":r1[-1][4]/r1[-2][4]-1,"r3":r1[-1][4]/r1[-4][4]-1,"r5":r1[-1][4]/r1[-6][4]-1,
      "r15":r1[-1][4]/r1[-16][4]-1,"r30":r1[-1][4]/r1[-31][4]-1,"r60":r1[-1][4]/r1[-61][4]-1,
      "trend5":trend5,"market_regime":"RISK_ON" if breadth>=.65 else ("RISK_OFF" if breadth<=.35 else "MIXED"),
      "vol_regime":"HIGH" if atrp>=.01 else ("LOW" if atrp<.003 else "NORMAL"),
      "btc_r3":rh("BTC-USDT",180),"eth_r3":rh("ETH-USDT",180),"breadth":breadth,"qv24h":tick[s]["qv"],
      "oi":oi,"funding":fr,"hour_utc":h,"weekday_utc":datetime.now(timezone.utc).weekday(),"session":session,"book":d,
      "liquidity_tier":"HIGH" if tick[s]["qv"]>=10000000 else ("MEDIUM" if tick[s]["qv"]>=1000000 else "LOW"),"low_liquidity_flag":tick[s]["qv"]<500000,
      "api_latency_ms":{"kline1":l1,"kline5":l5,"depth":ld,"oi":loi,"funding":lfr},
      "data_quality_flags":flags,"data_quality_score":max(0,100-20*len(flags))}
def wick(r):
    b=r[-1]; a=atr(r[:-1]); rng=b[2]-b[3]
    if not a or rng<1.5*a:return []
    _,o,h,l,c,v=b; lo=max(0,min(o,c)-l); up=max(0,h-max(o,c)); out=[]
    if lo/rng>=.3 and up/rng>=.3 and rng>=2*a:out=["BOTH_WICK"]
    else:
        if lo/rng>=.45 and lo>=.75*a:out.append("LOWER_WICK")
        if up/rng>=.45 and up>=.75*a:out.append("UPPER_WICK")
    return out
def ent(px,side,kind="market",filled=True):return {"px":px,"side":side,"kind":kind,"filled":filled,"fill_time":iso() if filled else None,"fill_confidence":"snapshot_market" if filled else None,"net":0.0,"mfe":0.0,"mae":0.0}
def make(s,typ,side,tick,r1,r5,breadth,l1=None,l5=None):
    now=time.time(); p=tick[s]["p"]; f=feat(s,tick,r1,r5,breadth,l1,l5); a=f["atr"]
    ex_time=r1[-1][0]+60;recv=time.time();e={"id":f"{int(now*1000)}-{s.replace('-','')}-{typ}","symbol":s,"event_type":typ,"side":side,"cluster_id":f"{s}-{int(now//900)}","schema_version":"8.1-coarse","time":iso(),
       "exchange_event_time":iso(ex_time),"received_at":iso(recv),"decision_time":iso(),"detection_delay_ms":max(0,int((recv-ex_time)*1000)),
       "features":f,"entries":{},"follow":{},"thresholds":{"fav0.5":None,"fav1":None,"fav2":None,"adv0.5":None,"adv1":None,"adv2":None},"cost_model":{"fee_per_side_pct":FEE*100,"slippage":"initial_orderbook_estimate","limit_fill":"snapshot_cross_not_queue_proof"},"_t0":now,"_next":0}
    e["entries"]["market"]=ent(p,side); e["entries"]["opposite"]=ent(p,"SHORT" if side=="LONG" else "LONG")
    e["entries"]["delay1"]=ent(None,side,filled=False);e["entries"]["delay3"]=ent(None,side,filled=False);e["entries"]["delay5"]=ent(None,side,filled=False)
    e["entries"]["atr025"]=ent(p-.25*a if side=="LONG" else p+.25*a,side,"limit",False);e["entries"]["atr050"]=ent(p-.5*a if side=="LONG" else p+.5*a,side,"limit",False)
    if "BREAKOUT" in typ:
        level=f["resistance20"] if side=="LONG" else f["support20"];e["entries"]["retest"]=ent(level,side,"limit",False)
        e["breakout"]={"level":level,"returned_inside_15m":False,"return_inside_min":None,"label":None}
    if "WICK" in typ:
        _,o,h,l,c,v=r1[-1]; span=(c-l) if side=="LONG" else (h-c)
        e["entries"]["wick25"]=ent(c-.25*span if side=="LONG" else c+.25*span,side,"limit",False);e["entries"]["wick50"]=ent(c-.5*span if side=="LONG" else c+.5*span,side,"limit",False)
    atrb="ATRH" if f["atr_pct"]>=.01 else ("ATRL" if f["atr_pct"]<.003 else "ATRM")
    e["pattern_key"]="|".join([typ,f["trend5"],"VH" if f["volume_ratio"]>=2 else "VL","SPH" if (f["book"].get("spread_pct") or 0)>=.15 else "SPL",f["market_regime"],atrb,f["session"]])
    e["memory_before"]=pattern_hint(e["pattern_key"])
    e["coarse_memory_before"]=coarse_hint(typ,e["pattern_key"]) if typ!="NORMAL_CONTROL" else {"hint":"CONTROL_BASELINE"}
    ch=e["coarse_memory_before"].get("hint")
    e["research_hint"]=e["memory_before"]["hint"] if ch=="COARSE_INSUFFICIENT" else ch
    return e
def upd(e,p):
    elapsed=(time.time()-e["_t0"])/60
    for m in (1,3,5):
        z=e["entries"][f"delay{m}"]
        if elapsed>=m and not z["filled"]:z["px"]=p;z["filled"]=True;z["fill_time"]=iso();z["fill_confidence"]="scheduled_snapshot"
    for k,z in e["entries"].items():
        if not z or z["px"] is None:continue
        if not z["filled"] and z["kind"]=="limit":
            if (z["side"]=="LONG" and p<=z["px"]) or (z["side"]=="SHORT" and p>=z["px"]):z["filled"]=True;z["fill_time"]=iso();z["fill_confidence"]="snapshot_cross_not_queue_proof"
        if not z["filled"]:continue
        gross=(p/z["px"]-1) if z["side"]=="LONG" else (z["px"]/p-1)
        slip=(e["features"]["book"].get("slip_buy_100") if z["side"]=="LONG" else e["features"]["book"].get("slip_sell_100")) or 0
        net=gross*100-2*FEE*100-slip;z["net"]=net;z["mfe"]=max(z["mfe"],net);z["mae"]=min(z["mae"],net)
    b=e.get("breakout")
    if b and elapsed<=15 and not b["returned_inside_15m"]:
        inside=(e["side"]=="LONG" and p<b["level"]) or (e["side"]=="SHORT" and p>b["level"])
        if inside:b["returned_inside_15m"]=True;b["return_inside_min"]=round(elapsed,3)
    mkt=e["entries"]["market"]; g=mkt["net"]; th=e["thresholds"]
    for v,k in ((.5,"fav0.5"),(1,"fav1"),(2,"fav2")):
        if g>=v and th[k] is None:th[k]=round(elapsed,3)
    for v,k in ((-.5,"adv0.5"),(-1,"adv1"),(-2,"adv2")):
        if g<=v and th[k] is None:th[k]=round(elapsed,3)
    while e["_next"]<len(FOLLOW) and elapsed>=FOLLOW[e["_next"]]:
        m=FOLLOW[e["_next"]]
        snap={"entries":{k:{"filled":z["filled"],"net":z["net"],"mfe":z["mfe"],"mae":z["mae"]} for k,z in e["entries"].items()}}
        if m in (1,3,5,15):
            fc=follow_ctx(e["symbol"]); init=e["features"]; ioi=init.get("oi"); coi=fc.get("oi")
            snap["market_change"]={"oi":coi,"oi_delta_pct":((coi/ioi-1)*100 if ioi and coi else None),
              "volume_ratio":fc.get("volume_ratio"),"volume_ratio_delta":((fc.get("volume_ratio") or 0)-(init.get("volume_ratio") or 0)) if fc.get("volume_ratio") is not None else None,
              "volume_persistence_ratio":(fc.get("volume_ratio")/(init.get("volume_ratio") or 1)) if fc.get("volume_ratio") is not None else None,
              "book_imbalance":fc.get("book_imbalance"),"book_imbalance_delta":((fc.get("book_imbalance") or 0)-(init.get("book",{}).get("imbalance_10") or 0)) if fc.get("book_imbalance") is not None else None,
              "spread_pct":fc.get("spread_pct"),"api_latency_ms":fc.get("api_latency_ms")}
        e["follow"][str(m)]=snap
        if b and m==15:b["label"]="FAKE_BREAKOUT" if b["returned_inside_15m"] else "TRUE_BREAKOUT"
        save(e);e["_next"]+=1
    return elapsed>=240
def finalize(e):
    best=max([(z["net"],k) for k,z in e["entries"].items() if z["filled"]])[1];hz={}
    for k in e["entries"]:
        vals=[(snap["entries"][k]["net"],int(m)) for m,snap in e["follow"].items() if "entries" in snap and k in snap["entries"] and snap["entries"][k]["filled"]]
        hz[k]=max(vals)[1] if vals else None
    e["best_entry_240m"]=best;e["best_exit_horizon_min_by_entry"]=hz;e["success"]=e["entries"]["market"]["net"]>0 and e["entries"]["market"]["mfe"]>=.5
    if e.get("breakout") and not e["breakout"].get("label"):e["breakout"]["label"]="FAKE_BREAKOUT" if e["breakout"].get("returned_inside_15m") else "TRUE_BREAKOUT"
    reasons=[]
    if e.get("breakout",{}).get("label")=="FAKE_BREAKOUT":reasons.append("FAKEOUT_RETURN_INSIDE")
    if (e["features"]["book"].get("spread_pct") or 0)>.15:reasons.append("WIDE_SPREAD")
    if e["features"]["volume_ratio"]<.8:reasons.append("WEAK_VOLUME")
    if (e["side"]=="LONG" and e["features"]["breadth"]<.4) or (e["side"]=="SHORT" and e["features"]["breadth"]>.6):reasons.append("MARKET_AGAINST")
    f5=e["follow"].get("5",{}).get("market_change",{})
    if f5.get("oi_delta_pct") is not None and f5["oi_delta_pct"]<0:reasons.append("OI_DECREASED_5M")
    if f5.get("book_imbalance_delta") is not None:
        if (e["side"]=="LONG" and f5["book_imbalance_delta"]<-.20) or (e["side"]=="SHORT" and f5["book_imbalance_delta"]>.20):reasons.append("ORDERBOOK_TURNED_AGAINST")
    e["diagnostic_tags"]=reasons;e["failure_reasons"]=[] if e["success"] else (reasons or ["UNCLASSIFIED"])
    p=PAT[e["pattern_key"]];p["n"]+=1;p["wins"]+=1 if e["success"] else 0;p["sum_net"]+=e["entries"]["market"]["net"];p["sum_mfe"]+=e["entries"]["market"]["mfe"];p["sum_mae"]+=e["entries"]["market"]["mae"]
    finish(e);FIN.append({k:v for k,v in e.items() if not k.startswith("_")});STATE["final"]+=1

def scan(tick):
    global ROT
    vals=[rh(sym,60) for sym in tick];vv=[x for x in vals if x is not None];breadth=sum(x>0 for x in vv)/len(vv) if vv else .5
    cand=[]
    for sym in tick:
        if time.time()-LAST.get(sym,0)<600:continue
        a=rh(sym,60);b=rh(sym,180)
        if a is None:continue
        if a>=.0035 or (b or 0)>=.006:cand.append((abs(a)+abs(b or 0),sym,"LONG"))
        elif a<=-.0035 or (b or 0)<=-.006:cand.append((abs(a)+abs(b or 0),sym,"SHORT"))
    chosen=set()
    for _,sym,side in sorted(cand,reverse=True)[:DEEP]:
        try:
            r1,l1=kl(sym,"1m",80);r5,l5=kl(sym,"5m",60)
            if len(r1)<61 or len(r5)<20:continue
            types=["UP_IMPULSE" if side=="LONG" else "DOWN_IMPULSE"];prev=r1[-21:-1];res=max(x[2] for x in prev);sup=min(x[3] for x in prev);c=r1[-1][4]
            if c>=res*1.001:types.append("BREAKOUT_UP")
            if c<=sup*.999:types.append("BREAKOUT_DOWN")
            types+=wick(r1)
            for typ in dict.fromkeys(types):
                sd="LONG" if typ in ("UP_IMPULSE","BREAKOUT_UP","LOWER_WICK") else ("SHORT" if typ in ("DOWN_IMPULSE","BREAKOUT_DOWN","UPPER_WICK") else side)
                e=make(sym,typ,sd,tick,r1,r5,breadth,l1,l5);ACTIVE[e["id"]]=e;REC.append({k:v for k,v in e.items() if not k.startswith("_")});STATE["events"]+=1;save(e)
            LAST[sym]=time.time();chosen.add(sym)
        except Exception as ex:error("scan_"+sym,ex)
    cs=[x for x in CONTRACTS if x in tick]
    if cs:
        start=ROT%len(cs); batch=[cs[(start+i)%len(cs)] for i in range(min(WICK_PER_MIN,len(cs)))];ROT=(start+len(batch))%len(cs)
        for sym in batch:
            STATE["wick_symbols_scanned"]+=1
            if sym in chosen:continue
            try:
                r1,l1=kl(sym,"1m",80)
                if len(r1)<61:continue
                bar=r1[-1][0]
                if LAST_WICK_BAR.get(sym)==bar:continue
                LAST_WICK_BAR[sym]=bar;types=wick(r1)
                if not types:continue
                r5,l5=kl(sym,"5m",60)
                if len(r5)<20:continue
                for typ in types:
                    sd="LONG" if typ=="LOWER_WICK" else ("SHORT" if typ=="UPPER_WICK" else ("LONG" if r1[-1][4]>=r1[-1][1] else "SHORT"))
                    e=make(sym,typ,sd,tick,r1,r5,breadth,l1,l5);ACTIVE[e["id"]]=e;REC.append({k:v for k,v in e.items() if not k.startswith("_")});STATE["events"]+=1;save(e)
            except Exception as ex:error("wick_"+sym,ex)
    normals=[sym for sym in tick if sym not in chosen and time.time()-LAST.get(sym,0)>600];random.shuffle(normals)
    for sym in normals[:NEG]:
        try:
            r1,l1=kl(sym,"1m",80);r5,l5=kl(sym,"5m",60)
            if len(r1)<61 or len(r5)<20:continue
            e=make(sym,"NORMAL_CONTROL","LONG",tick,r1,r5,breadth,l1,l5);ACTIVE[e["id"]]=e;CTRL.append({k:v for k,v in e.items() if not k.startswith("_")});STATE["controls"]+=1;save(e);LAST[sym]=time.time()
        except Exception as ex:error("control_"+sym,ex)

def pub():
    return {"status":"ok","service":STATE["service"],"db_enabled":bool(DBURL),"state":STATE,"schema_version":"8.1-coarse","followups_min":list(FOLLOW),"captures":["UP/DOWN_IMPULSE","BREAKOUT_UP/DOWN","LOWER_WICK","UPPER_WICK","BOTH_WICK","NORMAL_CONTROL"],"entries":["market","delay1/3/5","ATR0.25/0.5","breakout retest","wick retrace25/50","opposite"],
      "learning":["persistent fine pattern memory","persistent coarse memory","incremental coarse update per new outcome","coarse FAVOR/AVOID/NEUTRAL as primary research hint","fine pattern fallback when coarse sample insufficient","best entry/horizon","failure taxonomy"],
      "change_features":["OI delta 1/3/5/15m","volume persistence 1/3/5/15m","orderbook imbalance delta 1/3/5/15m","formal fake breakout label","full-universe rotating wick scan"],
      "quality":["exchange/receive/decision timestamps","detection latency","5m trend","market regime","vol regime","weekday/hour/session","liquidity tier","API latency","data quality score","fill confidence"],
      "recent_events":list(REC)[-30:],"recent_controls":list(CTRL)[-20:],"recent_final":list(FIN)[-30:]}
class W(BaseHTTPRequestHandler):
    def do_GET(self):
        b=json.dumps(pub(),separators=(",",":"),ensure_ascii=False).encode();self.send_response(200);self.send_header("Content-Type","application/json");self.send_header("Content-Length",str(len(b)));self.end_headers();self.wfile.write(b)
    def log_message(self,*a):pass

threading.Thread(target=lambda:ThreadingHTTPServer(("0.0.0.0",PORT),W).serve_forever(),daemon=True).start()
dbinit();log("FINAL_START",{"time":STATE["started"],"db_enabled":bool(DBURL),"coarse_learning":True})
lastscan=0
while True:
    t0=time.time()
    try:
        tick,lat=ticker();now=time.time()
        for s,x in tick.items():H[s].append((now,x["p"]))
        done=[]
        for eid,e in list(ACTIVE.items()):
            x=tick.get(e["symbol"])
            if x and upd(e,x["p"]):finalize(e);done.append(eid)
        for eid in done:ACTIVE.pop(eid,None)
        if now-lastscan>=60:scan(tick);lastscan=now
        STATE["ticks"]+=1
        if STATE["ticks"]%4==0:log("FINAL_STATUS",{"ticks":STATE["ticks"],"universe":len(tick),"events":STATE["events"],"controls":STATE["controls"],"active":len(ACTIVE),"final":STATE["final"],"wick_symbols_scanned":STATE["wick_symbols_scanned"],"errors":len(STATE["errors"]),"db":bool(DBURL),"coarse_main":len(COARSE["main"]),"coarse_mods":len(COARSE["mods"])})
        if DBURL and STATE["ticks"]%40==0:q("insert into market_research_v8_heartbeat(service,active,payload) values(%s,%s,%s::jsonb)",(STATE["service"],len(ACTIVE),json.dumps({"ticks":STATE["ticks"],"events":STATE["events"],"final":STATE["final"],"coarse_learning":True})))
    except Exception as ex:error("main",ex)
    time.sleep(max(3,TICK-(time.time()-t0)))
