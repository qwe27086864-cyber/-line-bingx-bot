from __future__ import annotations
import os, subprocess, sys
from pathlib import Path

SYMBOLS=os.environ.get("HIST_SYMBOLS","BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT")
DAYS=int(os.environ.get("HIST_DAYS","90"))
DATA=Path(os.environ.get("HIST_DATA_DIR","data"))
DATA.mkdir(parents=True,exist_ok=True)

def run(cmd):
    print("RUN", " ".join(map(str,cmd)), flush=True)
    subprocess.run(list(map(str,cmd)),check=True)

for interval in ("1m","5m","15m","1h"):
    run([sys.executable,"historical_data.py","--symbols",SYMBOLS,"--interval",interval,
         "--days",DAYS,"--out",DATA/f"history_{interval}.csv"])

run([sys.executable,"market_regime.py","--candles",DATA/"history_5m.csv",
     "--out",DATA/"market_features.csv"])

run([sys.executable,"historical_suite.py","--data-dir",DATA,
     "--out",DATA/"all_core_outcomes.csv"])

out=DATA/"all_core_outcomes.csv"
if out.exists() and out.stat().st_size>0:
    run([sys.executable,"router_trainer.py","--features",DATA/"market_features.csv",
         "--outcomes",out,"--out",DATA/"router_v1_report.json"])

if (DATA/"router_v1_report.json").exists():
    print((DATA/"router_v1_report.json").read_text(encoding="utf-8"),flush=True)
else:
    print("ROUTER REPORT NOT CREATED: insufficient joined core outcomes or missing source histories",flush=True)
