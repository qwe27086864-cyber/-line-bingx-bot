from __future__ import annotations
import argparse, hashlib, json, os, random, time, urllib.error, urllib.request
from datetime import datetime, timezone
from pathlib import Path

DEFAULT_WALLETS = ['3kxcF8wHKm4sEtnxjXXeUvNGeDjTmo47LgtSRg71YfG5', '6tzAKfHtQA4zLwgaXn97kU5C1dYsKcRy8XZhZUWmWU23', '7ufmve7ZSFCzuNcKRunYrGtyb2Ka1MXzkWwf7jZhVsmL', '81HHNStHsNMN8hrLbD2LFxzaikvzueZp4XWzvvmGePC1', '8MoW9mtbEz6z3gPuAdYb1yWhjCAxQSYqpcTb1CQgN5qb', '92cwTJoGDFKSnp6Zr2taFr3sFWqs8jRvtU73qbut2win', '9XQLDx1MV5Ssv48DfFfBwFErh8ds6jfXfHZBJKj1FqXY', 'A7ZmFuTUEb14ArzoYf7mBJUPmdvbLbj1J8kvKAaJAWek', 'An9sREpLnAXVi4KMaTGuGvgET51CyaukLUTMtxzmLYSB', 'BNnN2MqfWLvgThYBsv6v8JQaYZXYKYahC5YCy27Ct1cX', 'CJgBunSv2nagagWqBuPtQmrsPmPajewsZjvGyZhpNSzb', 'CQGxdVAvrRvb8TvrGmMYQCcHjCCbFuhKgTWSEVqFJgfm', 'DAtJJbckkqADvpt7bhCqNHMpitYBVHfFP3ZsisNvbBTC', 'DS5ZrUiQwSNd1Y14nFKTTwaAxH4PR7gAzE5ac6aLp8cS', 'dtrzJPj7yDdvm6eRqBAgxsK2sMJeD9HhBEBB3XMedXy', 'EsJfAbSsMUqxF6vqymkLDLgzz6biHQEAjnmyEf3BQTWs', 'EvbPo7eCCmLxb95WSbS4P7JZvc6mxLxyG8CJUBydteN2', 'G318mttEeAmdDuVkwA1Dse76QhnuKrM2w4FPkG724adW', 'GUkwZGEfDQuKSSyQy9225swpLAzy3Ssko4u3zNbRYQzx', 'HzKH3s8Kf7f7pgvJZx2V6TzhkdHbZDurG53rXj61BVLR', 'kCtFJ3xQWahz63HkPGwNfXRk21MBrz4sdeFBUeznUkC']
DEFAULT_START = "2025-11-02T09:30:00Z"
DEFAULT_END = "2026-10-28T09:30:00Z"

def dt(s):
    if s.endswith("Z"): s=s[:-1]+"+00:00"
    x=datetime.fromisoformat(s)
    if x.tzinfo is None: x=x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)

def endpoint():
    u=os.getenv("MEME2_HIST_RPC_HTTP","").strip()
    if u: return u
    k=os.getenv("HELIUS_API_KEY","").strip()
    if not k:
        raise SystemExit("Set HELIUS_API_KEY or MEME2_HIST_RPC_HTTP")
    return "https://mainnet.helius-rpc.com/?api-key="+k

def call(url,payload,retries=8):
    body=json.dumps(payload,separators=(",",":")).encode()
    last=None
    for i in range(retries):
        req=urllib.request.Request(url,data=body,method="POST",
            headers={"Content-Type":"application/json","User-Agent":"meme2-hist-backfill/1.0"})
        try:
            with urllib.request.urlopen(req,timeout=90) as r:
                ans=json.loads(r.read().decode())
            if ans.get("error"): raise RuntimeError(ans["error"])
            return ans
        except urllib.error.HTTPError as e:
            last=e
            if e.code not in (408,425,429,500,502,503,504): raise
        except Exception as e:
            last=e
        if i==retries-1: break
        delay=min(60,2**i)+random.random()
        time.sleep(delay)
    raise RuntimeError(f"RPC failed: {last}")

def load(p):
    if not p.exists(): return {}
    try: return json.loads(p.read_text())
    except: return {}

def save(p,obj):
    t=p.with_suffix(".tmp")
    t.write_text(json.dumps(obj,indent=2))
    t.replace(p)

def fetch_wallet(url,wallet,start_ts,end_ts,out_dir,limit,pause):
    tag=hashlib.sha1(wallet.encode()).hexdigest()[:12]
    raw=out_dir/f"{tag}.jsonl"
    ck=out_dir/f"{tag}.checkpoint.json"
    st=load(ck)
    if st.get("complete"): return st
    token=st.get("paginationToken")
    rows=int(st.get("rows",0)); pages=int(st.get("pages",0))
    while True:
        cfg={
            "transactionDetails":"full",
            "encoding":"jsonParsed",
            "maxSupportedTransactionVersion":1,
            "commitment":"finalized",
            "sortOrder":"asc",
            "limit":limit,
            "filters":{"tokenAccounts":"all","blockTime":{"gte":start_ts,"lt":end_ts},"status":"succeeded"}
        }
        if token: cfg["paginationToken"]=token
        ans=call(url,{"jsonrpc":"2.0","id":"meme2-hist","method":"getTransactionsForAddress","params":[wallet,cfg]})
        res=ans.get("result") or {}
        data=res.get("data") or []
        token=res.get("paginationToken")
        with raw.open("a",encoding="utf-8") as f:
            for tx in data:
                f.write(json.dumps({"wallet":wallet,"tx":tx},separators=(",",":"))+"\n")
        rows+=len(data); pages+=1
        st={"wallet":wallet,"rows":rows,"pages":pages,"paginationToken":token,
            "complete":not bool(token),"raw_file":str(raw),
            "start_ts":start_ts,"end_ts":end_ts,
            "updated_at":datetime.now(timezone.utc).isoformat()}
        save(ck,st)
        print(f"{wallet[:8]}.. page={pages} added={len(data)} total={rows} next={bool(token)}",flush=True)
        if not token: return st
        time.sleep(max(0,pause))

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--start",default=DEFAULT_START)
    ap.add_argument("--end",default=DEFAULT_END)
    ap.add_argument("--out-dir",default="data/historical_360/meme2/raw")
    ap.add_argument("--wallet",action="append",default=[])
    ap.add_argument("--limit",type=int,default=500)
    ap.add_argument("--pause",type=float,default=.15)
    a=ap.parse_args()
    start,end=dt(a.start),dt(a.end)
    if end<=start: raise SystemExit("bad window")
    wallets=list(dict.fromkeys(a.wallet or DEFAULT_WALLETS))
    out=Path(a.out_dir); out.mkdir(parents=True,exist_ok=True)
    url=endpoint()
    manifest={"purpose":"MEME2 isolated 360-day raw history","live_db_reads":False,
        "live_db_writes":False,"trading_actions":False,
        "start":start.isoformat(),"end":end.isoformat(),"wallet_count":len(wallets),"wallets":{}}
    for i,w in enumerate(wallets,1):
        print(f"=== {i}/{len(wallets)} {w[:8]}.. ===",flush=True)
        try:
            manifest["wallets"][w]=fetch_wallet(url,w,int(start.timestamp()),int(end.timestamp()),
                out,max(1,min(a.limit,1000)),a.pause)
        except Exception as e:
            manifest["wallets"][w]={"complete":False,"error":f"{type(e).__name__}: {e}"}
        (out/"manifest.json").write_text(json.dumps(manifest,indent=2))
    done=sum(1 for x in manifest["wallets"].values() if x.get("complete"))
    rows=sum(int(x.get("rows",0)) for x in manifest["wallets"].values())
    manifest["summary"]={"complete_wallets":done,"wallets":len(wallets),"rows":rows}
    (out/"manifest.json").write_text(json.dumps(manifest,indent=2))
    print(json.dumps(manifest["summary"],indent=2))

if __name__=="__main__":
    main()
