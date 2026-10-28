from __future__ import annotations
import argparse,csv,json,subprocess,sys
from pathlib import Path

def run(cmd):
    print("RUN:", " ".join(map(str,cmd)), flush=True)
    p=subprocess.run(list(map(str,cmd)),check=False)
    if p.returncode!=0: raise SystemExit(p.returncode)

def append_outcome_file(path, outs, status, name, label):
    if path.exists() and path.stat().st_size > 0:
        outs.append(path)
        status[name]=label
        return True
    return False

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--data-dir",default="data")
    ap.add_argument("--out",default="data/all_core_outcomes.csv")
    args=ap.parse_args();d=Path(args.data_dir);outs=[]

    status={}

    # True historical candle replay.
    r2out=d/"r2fs_core_outcomes.csv"
    if (d/"history_1m.csv").exists() and (d/"history_5m.csv").exists():
        run([sys.executable,"r2fs_historical_adapter.py",
             "--candles-1m",str(d/"history_1m.csv"),
             "--candles-5m",str(d/"history_5m.csv"),
             "--outcomes-out",str(r2out)])
        outs.append(r2out);status["R2FS"]="RAN_HISTORICAL"
    else:
        status["R2FS"]="WAITING_DATA"

    r3out=d/"r3_core_outcomes.csv"
    r3need=[d/"history_1m.csv",d/"history_5m.csv",d/"history_15m.csv",d/"history_1h.csv"]
    if all(x.exists() for x in r3need):
        run([sys.executable,"r3_historical_adapter.py",
             "--candles-1m",str(d/"history_1m.csv"),
             "--candles-5m",str(d/"history_5m.csv"),
             "--candles-15m",str(d/"history_15m.csv"),
             "--candles-1h",str(d/"history_1h.csv"),
             "--outcomes-out",str(r3out)])
        outs.append(r3out);status["R3"]="RAN_HISTORICAL"
    else:
        status["R3"]="WAITING_DATA"

    # BF1 can be replayed from the DB observation stream.
    bfout=d/"bf1_core_outcomes.csv"
    if (d/"bf1_snapshots.csv").exists():
        run([sys.executable,"bf1_historical_adapter.py",
             "--snapshots",str(d/"bf1_snapshots.csv"),
             "--out",str(bfout)])
        outs.append(bfout);status["BF1"]="RAN_DB_REPLAY"
    else:
        status["BF1"]="WAITING_DB_EXPORT"

    # AS1 raw tick/trade history is not stored. Never fabricate it.
    if not append_outcome_file(
        d/"as1_forward_outcomes.csv", outs, status, "AS1", "RAN_FORWARD_DB_H10"
    ):
        status["AS1"]="WAITING_FORWARD_DB_EXPORT"

    # Use current strategy versions from realised DB forward positions.
    if not append_outcome_file(
        d/"meme1_forward_outcomes.csv", outs, status, "MEME1", "RAN_FORWARD_DB_SURVIVAL_V3"
    ):
        status["MEME1"]="WAITING_FORWARD_DB_EXPORT"

    if not append_outcome_file(
        d/"meme2_forward_outcomes.csv", outs, status, "MEME2", "RAN_FORWARD_DB_V3"
    ):
        status["MEME2"]="WAITING_FORWARD_DB_EXPORT"

    merged=[]
    for p in outs:
        if p.exists():
            with p.open(newline="",encoding="utf-8-sig") as f:
                merged.extend(csv.DictReader(f))

    Path(args.out).parent.mkdir(parents=True,exist_ok=True)
    with open(args.out,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=["timestamp","symbol","core","net_return_pct"])
        w.writeheader()
        for r in merged:
            w.writerow({k:r.get(k,"") for k in ["timestamp","symbol","core","net_return_pct"]})

    print(json.dumps({
        "status":status,
        "merged_rows":len(merged),
        "out":args.out,
        "warning":"AS1/MEME1/MEME2 DB outputs are forward validation, not 30-day historical replay."
    },indent=2),flush=True)

if __name__=="__main__":
    main()
