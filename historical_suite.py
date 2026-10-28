from __future__ import annotations
import argparse,csv,json,subprocess,sys
from pathlib import Path

def run(cmd):
    print("RUN:", " ".join(cmd), flush=True)
    p=subprocess.run(cmd,check=False)
    if p.returncode!=0: raise SystemExit(p.returncode)

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--data-dir",default="data")
    ap.add_argument("--out",default="data/all_core_outcomes.csv")
    args=ap.parse_args();d=Path(args.data_dir);outs=[]
    jobs=[
      ("R2FS",[sys.executable,"r2fs_historical_adapter.py","--candles-1m",str(d/"history_1m.csv"),"--candles-5m",str(d/"history_5m.csv"),"--outcomes-out",str(d/"r2fs_core_outcomes.csv")],d/"r2fs_core_outcomes.csv",[(d/"history_1m.csv"),(d/"history_5m.csv")]),
      ("R3",[sys.executable,"r3_historical_adapter.py","--candles-1m",str(d/"history_1m.csv"),"--candles-5m",str(d/"history_5m.csv"),"--candles-15m",str(d/"history_15m.csv"),"--candles-1h",str(d/"history_1h.csv"),"--outcomes-out",str(d/"r3_core_outcomes.csv")],d/"r3_core_outcomes.csv",[(d/"history_1m.csv"),(d/"history_5m.csv"),(d/"history_15m.csv"),(d/"history_1h.csv")]),
      ("BF1",[sys.executable,"bf1_historical_adapter.py","--snapshots",str(d/"bf1_snapshots.csv"),"--out",str(d/"bf1_core_outcomes.csv")],d/"bf1_core_outcomes.csv",[d/"bf1_snapshots.csv"]),
      ("AS1",[sys.executable,"as1_historical_adapter.py","--events",str(d/"as1_events.csv"),"--out",str(d/"as1_core_outcomes.csv")],d/"as1_core_outcomes.csv",[d/"as1_events.csv"]),
      ("MEME1",[sys.executable,"meme1_historical_adapter.py","--snapshots",str(d/"meme1_pool_history.csv"),"--out",str(d/"meme1_core_outcomes.csv")],d/"meme1_core_outcomes.csv",[d/"meme1_pool_history.csv"]),
      ("MEME2",[sys.executable,"meme2_historical_adapter.py","--events",str(d/"meme2_wallet_history.csv"),"--out",str(d/"meme2_core_outcomes.csv")],d/"meme2_core_outcomes.csv",[d/"meme2_wallet_history.csv"]),
    ]
    status={}
    for name,cmd,out,needs in jobs:
        if all(x.exists() for x in needs):
            run(cmd);outs.append(out);status[name]="RAN"
        else: status[name]="WAITING_DATA: "+",".join(str(x) for x in needs if not x.exists())
    merged=[]
    for p in outs:
        if p.exists():
            with p.open(newline="",encoding="utf-8-sig") as f:merged.extend(csv.DictReader(f))
    Path(args.out).parent.mkdir(parents=True,exist_ok=True)
    with open(args.out,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["timestamp","symbol","core","net_return_pct"]);w.writeheader();w.writerows(merged)
    print(json.dumps({"status":status,"merged_rows":len(merged),"out":args.out},indent=2))
if __name__=="__main__":main()
