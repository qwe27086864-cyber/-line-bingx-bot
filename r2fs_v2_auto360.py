"""
R2FS V4 Breakout-Confirm 360d backtest.
Historical-only: no DATABASE_URL, no live/shadow writes, no orders.

V4 redesign:
- Radar only discovers setups; it never enters directly.
- Require 5m trend + real structure breakout + volume/momentum + BTC/ETH regime.
- Then wait 2-20 minutes for 1m hold/continuation confirmation.
- Reject invalidated or over-chased breakouts.
- Keep V2 TP/SL/partials/cost model unchanged for clean A/B comparison.
"""

from __future__ import annotations

import bisect
import csv
import json
import os
import time
from datetime import timedelta
from pathlib import Path

import r2fs_historical_adapter as base
import r2fs_historical_adapter_v2 as exit_model
import r2fs_historical_v2_job as job

CORE = "R2FS_V4_BREAKOUT_CONFIRM"
ROOT = Path(os.environ.get("R2FS_V4_DIR", "data/r2fs_v2_auto360"))
ROOT.mkdir(parents=True, exist_ok=True)

SEGMENTS = [
    ("01","2026-10-28T09:30:00Z"),("02","2026-09-28T09:30:00Z"),
    ("03","2026-08-29T09:30:00Z"),("04","2026-07-30T09:30:00Z"),
    ("05","2026-06-30T09:30:00Z"),("06","2026-05-31T09:30:00Z"),
    ("07","2026-05-01T09:30:00Z"),("08","2026-04-01T09:30:00Z"),
    ("09","2026-03-02T09:30:00Z"),("10","2026-01-31T09:30:00Z"),
    ("11","2026-01-01T09:30:00Z"),("12","2025-12-02T09:30:00Z"),
]

REQUEST_PAUSE = float(os.environ.get("R2FS_V4_REQUEST_PAUSE_SEC", "2.25"))
MAX_ATTEMPTS = int(os.environ.get("R2FS_V4_SEGMENT_ATTEMPTS", "2"))

RADAR_MIN_MOVE = float(os.environ.get("R2FS_V4_RADAR_MIN_MOVE", "0.40"))
MIN_QV24 = float(os.environ.get("R2FS_V4_MIN_QV24", "250000"))
MIN_MOM5 = float(os.environ.get("R2FS_V4_MIN_MOM5", "0.25"))
MIN_VR5 = float(os.environ.get("R2FS_V4_MIN_VR5", "1.15"))
MAX_EXT5 = float(os.environ.get("R2FS_V4_MAX_EXT5", "2.50"))
MIN_ATR5 = float(os.environ.get("R2FS_V4_MIN_ATR5", "0.10"))
MAX_ATR5 = float(os.environ.get("R2FS_V4_MAX_ATR5", "1.80"))
BREAKOUT_BUFFER = float(os.environ.get("R2FS_V4_BREAKOUT_BUFFER", "0.03"))
MIN_MARKET_SCORE = float(os.environ.get("R2FS_V4_MIN_MARKET_SCORE", "60"))

TTL_MIN = int(os.environ.get("R2FS_V4_TTL_MIN", "20"))
MIN_AGE_MIN = int(os.environ.get("R2FS_V4_MIN_AGE_MIN", "2"))
INVALIDATION = float(os.environ.get("R2FS_V4_INVALIDATION", "0.20"))
HOLD_BUFFER = float(os.environ.get("R2FS_V4_HOLD_BUFFER", "0.03"))
MAX_BREAKOUT_CHASE = float(os.environ.get("R2FS_V4_MAX_BREAKOUT_CHASE", "0.90"))
MIN_MOM1 = float(os.environ.get("R2FS_V4_MIN_MOM1", "0.03"))
MIN_VR1 = float(os.environ.get("R2FS_V4_MIN_VR1", "0.85"))
MAX_EXT1 = float(os.environ.get("R2FS_V4_MAX_EXT1", "0.80"))

SPREAD = float(os.environ.get("R2FS_V4_SPREAD", "0.12"))
ADVERSE_CHASE = float(os.environ.get("R2FS_V4_ADVERSE_CHASE", "0.05"))
MAX_OPEN = int(os.environ.get("R2FS_V4_MAX_OPEN", "3"))
MAX_NEW = int(os.environ.get("R2FS_V4_MAX_NEW_PER_MIN", "1"))
COOLDOWN_HOURS = float(os.environ.get("R2FS_V4_COOLDOWN_HOURS", "2"))
MAX_RUNNER_HOURS = float(os.environ.get("R2FS_V4_MAX_RUNNER_HOURS", "24"))

DETAIL_FIELDS = [
    "timestamp","symbol","core","side","decision","reason","setup_time",
    "radar_move_pct","reference_level","signal_price","market_score",
    "mom5_pct","volume_ratio_5m","atr5_pct","extension5_pct","rsi5",
    "dir_1m_bars","mom1_pct","volume_ratio_1m","extension1_pct",
    "breakout_chase_pct","entry_price","exit_price","exit_reason",
    "gross_return_pct","net_return_pct","sim_pnl_usdt",
    "tp1_hit","tp2_hit","tp3_hit","resolved","history_quality",
]
OUTCOME_FIELDS = ["timestamp","symbol","core","net_return_pct"]


def pct(a,b):
    return (float(a)/float(b)-1.0)*100.0 if b else 0.0

def d(side,x):
    return float(x) if side == "LONG" else -float(x)

def qv(row):
    x = float(row.get("quote_volume") or 0.0)
    if x > 0:
        return x
    return float(row.get("volume") or 0.0) * float(row.get("close") or 0.0)

def aligned(side,p,fast,slow):
    return p > fast > slow if side == "LONG" else p < fast < slow

def ext(side,p,e):
    return d(side, pct(p,e))

def atr_pct(rows):
    if len(rows) < 15:
        return 0.0
    vals = []
    for prev, cur in zip(rows[-15:-1], rows[-14:]):
        h,l,pc = float(cur["high"]),float(cur["low"]),float(prev["close"])
        vals.append(max(h-l, abs(h-pc), abs(l-pc)))
    p = float(rows[-1]["close"])
    return (sum(vals)/len(vals))/p*100.0 if p and vals else 0.0


class Idx:
    def __init__(self, by, minutes):
        self.by = by
        self.times = {}
        self.qprefix = {}
        dt = timedelta(minutes=minutes)
        for s, rows in by.items():
            self.times[s] = [r["t"] + dt for r in rows]
            pref=[0.0]; total=0.0
            for r in rows:
                total += qv(r); pref.append(total)
            self.qprefix[s]=pref

    def completed(self,s,t,n=None):
        rows=self.by.get(s,[]); ts=self.times.get(s,[])
        i=bisect.bisect_right(ts,t)
        return rows[:i] if n is None else rows[max(0,i-n):i]

    def qv24(self,s,t):
        ts=self.times.get(s,[]); pref=self.qprefix.get(s,[0.0])
        r=bisect.bisect_right(ts,t)
        l=bisect.bisect_right(ts,t-timedelta(hours=24))
        return pref[r]-pref[l]


def market_score(side,t,i5):
    score=50.0
    for s in ("BTC-USDT","ETH-USDT"):
        bars=i5.completed(s,t,300)
        if len(bars)<60:
            continue
        c=[float(x["close"]) for x in bars]
        e12=base.ema(c,12); e36=base.ema(c,36)
        if e12 is None or e36 is None:
            continue
        if aligned(side,c[-1],e12,e36):
            score += 16
        elif aligned("SHORT" if side=="LONG" else "LONG", c[-1],e12,e36):
            score -= 16
        if len(c)>=289:
            x=d(side,pct(c[-1],c[-289]))
            score += 8 if x>0.50 else 4 if x>0 else -8 if x<-0.50 else -4
    return max(0.0,min(100.0,score))


def make_setup(symbol,t,i5):
    bars=i5.completed(symbol,t,320)
    if len(bars)<80:
        return None
    cur,prev=bars[-1],bars[-2]
    move=pct(cur["close"],prev["close"])
    if abs(move)<RADAR_MIN_MOVE:
        return None
    side="LONG" if move>0 else "SHORT"
    if i5.qv24(symbol,t) < MIN_QV24:
        return None

    closes=[float(x["close"]) for x in bars]
    highs=[float(x["high"]) for x in bars]
    lows=[float(x["low"]) for x in bars]
    vols=[float(x.get("volume") or 0.0) for x in bars]
    p=closes[-1]
    e12=base.ema(closes,12); e36=base.ema(closes,36); r=base.rsi(closes,14)
    if e12 is None or e36 is None or r is None:
        return None
    if not aligned(side,p,e12,e36):
        return None

    mom5=d(side,pct(closes[-1],closes[-4]))
    vr5=base.volume_ratio(vols)
    if mom5<MIN_MOM5 or vr5<MIN_VR5:
        return None

    ref=max(highs[-21:-1]) if side=="LONG" else min(lows[-21:-1])
    if d(side,pct(p,ref)) < BREAKOUT_BUFFER:
        return None

    e5=ext(side,p,e12)
    a5=atr_pct(bars)
    if e5<0 or e5>MAX_EXT5 or not (MIN_ATR5<=a5<=MAX_ATR5):
        return None
    if side=="LONG" and not (45<=r<=82):
        return None
    if side=="SHORT" and not (18<=r<=55):
        return None

    mkt=market_score(side,t,i5)
    if mkt<MIN_MARKET_SCORE:
        return None

    return {
        "symbol":symbol,"side":side,"created":t,"expires":t+timedelta(minutes=TTL_MIN),
        "reference":ref,"move":move,"p5":p,"mkt":mkt,"mom5":mom5,
        "vr5":vr5,"atr5":a5,"ext5":e5,"rsi5":r,
    }


def confirm(setup,t,i1,i5):
    bars=i1.completed(setup["symbol"],t,40)
    if len(bars)<30:
        return None,"WAIT"
    age=(t-setup["created"]).total_seconds()/60.0
    if age<MIN_AGE_MIN:
        return None,"WAIT"
    if t>setup["expires"]:
        return None,"EXPIRED"

    p=float(bars[-1]["close"]); ref=float(setup["reference"])
    chase=d(setup["side"],pct(p,ref))
    if chase < -INVALIDATION:
        return None,"INVALIDATED"
    if chase < HOLD_BUFFER:
        return None,"WAIT"
    if chase > MAX_BREAKOUT_CHASE:
        return None,"CHASE"

    closes=[float(x["close"]) for x in bars]
    vols=[float(x.get("volume") or 0.0) for x in bars]
    e5=base.ema(closes,5); e13=base.ema(closes,13)
    if e5 is None or e13 is None or not aligned(setup["side"],p,e5,e13):
        return None,"WAIT"

    recent=closes[-4:]
    dbars=sum(1 for a,b in zip(recent[:-1],recent[1:]) if d(setup["side"],pct(b,a))>0)
    mom1=d(setup["side"],pct(recent[-1],recent[0]))
    recent_vol=sum(vols[-3:])/3.0
    prev=vols[-23:-3]
    prev_avg=sum(prev)/len(prev) if prev else 0.0
    vr1=recent_vol/prev_avg if prev_avg>0 else 0.0
    e1=ext(setup["side"],p,e13)

    if dbars<2 or mom1<MIN_MOM1 or vr1<MIN_VR1 or e1<0 or e1>MAX_EXT1:
        return None,"WAIT"
    mkt=market_score(setup["side"],t,i5)
    if mkt<MIN_MARKET_SCORE:
        return None,"MARKET"

    return {
        "price":p,"mkt":mkt,"dbars":dbars,"mom1":mom1,
        "vr1":vr1,"ext1":e1,"chase":chase,
    },"OK"


def row0(t,s,decision,reason):
    return {
        "timestamp":t.isoformat(),"symbol":s["symbol"],"core":CORE,"side":s["side"],
        "decision":decision,"reason":reason,"setup_time":s["created"].isoformat(),
        "radar_move_pct":round(s["move"],6),"reference_level":round(s["reference"],12),
        "signal_price":"","market_score":round(s["mkt"],3),"mom5_pct":round(s["mom5"],6),
        "volume_ratio_5m":round(s["vr5"],6),"atr5_pct":round(s["atr5"],6),
        "extension5_pct":round(s["ext5"],6),"rsi5":round(s["rsi5"],3),
        "dir_1m_bars":"","mom1_pct":"","volume_ratio_1m":"","extension1_pct":"",
        "breakout_chase_pct":"","entry_price":"","exit_price":"","exit_reason":"",
        "gross_return_pct":"","net_return_pct":"","sim_pnl_usdt":"",
        "tp1_hit":False,"tp2_hit":False,"tp3_hit":False,"resolved":False,
        "history_quality":"OHLCV_V4_BREAKOUT_CONFIRM_EXEC_PROXY",
    }


def replay(c1,c5):
    i1=Idx(c1,1); i5=Idx(c5,5)
    times=sorted({t for xs in i1.times.values() for t in xs})
    t5=set(t for xs in i5.times.values() for t in xs)
    symbols=sorted(c5.keys())
    pending={}; active=[]; cooldown={}; details=[]

    for t in times:
        active=[x for x in active if x["exit_time"] is None or x["exit_time"]>t]
        new_count=0

        if t in t5:
            for symbol in symbols:
                if cooldown.get(symbol) and t<cooldown[symbol]:
                    continue
                s=make_setup(symbol,t,i5)
                if not s:
                    continue
                old=pending.get(symbol)
                if old and old["side"]==s["side"]:
                    oldage=(t-old["created"]).total_seconds()/60.0
                    if oldage<TTL_MIN/2:
                        continue
                pending[symbol]=s
                x=row0(t,s,"SETUP_CREATED","5m breakout passed; wait for 1m hold/continuation")
                x["signal_price"]=round(s["p5"],12)
                details.append(x)

        for symbol,s in list(pending.items()):
            if t>s["expires"]:
                details.append(row0(t,s,"SETUP_EXPIRED","confirmation window expired"))
                pending.pop(symbol,None); continue

            c,state=confirm(s,t,i1,i5)
            if state=="INVALIDATED":
                details.append(row0(t,s,"SETUP_INVALIDATED","breakout reference failed"))
                pending.pop(symbol,None); continue
            if state=="CHASE":
                details.append(row0(t,s,"SETUP_CHASE_REJECTED","confirmation too far from breakout"))
                pending.pop(symbol,None); continue
            if state=="MARKET":
                details.append(row0(t,s,"SETUP_MARKET_REJECTED","market regime lost alignment"))
                pending.pop(symbol,None); continue
            if not c:
                continue

            x=row0(t,s,"V4_ENTER","5m breakout + 1m continuation confirmed")
            x.update({
                "signal_price":round(c["price"],12),"market_score":round(c["mkt"],3),
                "dir_1m_bars":c["dbars"],"mom1_pct":round(c["mom1"],6),
                "volume_ratio_1m":round(c["vr1"],6),"extension1_pct":round(c["ext1"],6),
                "breakout_chase_pct":round(c["chase"],6),
            })

            if new_count>=MAX_NEW:
                x["decision"]="RISK_BLOCKED_SCAN_CAP"; x["reason"]="max new per minute"
                details.append(x); continue
            if len(active)>=MAX_OPEN:
                x["decision"]="RISK_BLOCKED_MAX_OPEN"; x["reason"]="max concurrent open"
                details.append(x); continue
            if any(a["symbol"]==symbol for a in active):
                x["decision"]="RISK_BLOCKED_SYMBOL_OPEN"; x["reason"]="symbol already open"
                details.append(x); continue
            if cooldown.get(symbol) and t<cooldown[symbol]:
                x["decision"]="RISK_BLOCKED_COOLDOWN"; x["reason"]="symbol cooldown"
                details.append(x); continue

            adverse=SPREAD/2.0+ADVERSE_CHASE
            signal=float(c["price"])
            entry=signal*(1+adverse/100.0) if s["side"]=="LONG" else signal*(1-adverse/100.0)

            out=exit_model.simulate_exit_v2(s["side"],entry,t,c1.get(symbol,[]),MAX_RUNNER_HOURS)
            exit_price,reason,gross,h1,h2,h3,resolved,exit_time=out
            residual=max(0.0,base.ROUND_TRIP_COST_PCT-adverse)
            net=gross-residual if resolved else 0.0
            pnl=base.SIM_MARGIN_USDT*base.SIM_LEVERAGE*net/100.0 if resolved else 0.0

            x.update({
                "entry_price":round(entry,12),"exit_price":round(float(exit_price),12),
                "exit_reason":reason,"gross_return_pct":round(float(gross),6),
                "net_return_pct":round(float(net),6),"sim_pnl_usdt":round(float(pnl),6),
                "tp1_hit":bool(h1),"tp2_hit":bool(h2),"tp3_hit":bool(h3),"resolved":bool(resolved),
            })
            details.append(x)
            active.append({"symbol":symbol,"exit_time":exit_time})
            cooldown[symbol]=t+timedelta(hours=COOLDOWN_HOURS)
            pending.pop(symbol,None)
            new_count+=1

    return details


def write_outputs(details,detail_path,out_path):
    with open(detail_path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=DETAIL_FIELDS); w.writeheader(); w.writerows(details)
    rows=[]
    for x in details:
        if x.get("decision")=="V4_ENTER" and x.get("resolved"):
            rows.append({
                "timestamp":x["timestamp"],"symbol":x["symbol"],"core":CORE,
                "net_return_pct":float(x["net_return_pct"]),
            })
    with open(out_path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=OUTCOME_FIELDS); w.writeheader(); w.writerows(rows)
    return rows


def metrics(rows):
    vals=[float(x["net_return_pct"]) for x in rows]
    wins=[x for x in vals if x>0]; losses=[x for x in vals if x<0]
    gp=sum(wins); gl=abs(sum(losses))
    pf=gp/gl if gl>0 else (999.0 if gp>0 else 0.0)
    return {
        "trades":len(vals),"wins":len(wins),"losses":len(losses),
        "win_rate_pct":round(100*len(wins)/len(vals),2) if vals else 0.0,
        "gross_positive_net_pct":round(gp,6),"gross_negative_abs_net_pct":round(gl,6),
        "profit_factor":round(pf,6),"sum_net_return_pct":round(sum(vals),6),
    }


def configure(seg,end_iso):
    job.DATA=seg; job.DAYS=30; job.END_ISO=end_iso; job.PAUSE=REQUEST_PAUSE
    job.recovery.DATA=seg; job.recovery.DAYS=30
    job.recovery.REQUEST_PAUSE=REQUEST_PAUSE
    job.recovery.SYMBOLS=",".join(job.SYMBOLS)


def run_segment(index,seg_id,end_iso):
    seg=ROOT/f"v4_segment_{seg_id}"; seg.mkdir(parents=True,exist_ok=True)
    end=job.parse_dt(end_iso); start=end-timedelta(days=30)
    print(f"R2FS_V4_SEGMENT_START segment={index}/12 start={start.isoformat()} end={end.isoformat()}",flush=True)

    for name in ("history_1m.csv","history_5m.csv","r2fs_v4_detail.csv","r2fs_v4_outcomes.csv","segment_summary.json"):
        p=seg/name
        if p.exists(): p.unlink()

    configure(seg,end_iso)
    print(f"R2FS_V4_STAGE segment={index}/12 stage=download_1m",flush=True)
    job.recovery.download_interval("1m",start,end)
    print(f"R2FS_V4_STAGE_DONE segment={index}/12 stage=download_1m",flush=True)

    print(f"R2FS_V4_STAGE segment={index}/12 stage=download_5m",flush=True)
    job.recovery.download_interval("5m",start,end)
    job.recovery.recover_interval_from_1m("5m",start,end)
    print(f"R2FS_V4_STAGE_DONE segment={index}/12 stage=download_5m",flush=True)

    c1=base.read_candles(seg/"history_1m.csv")
    c5=base.read_candles(seg/"history_5m.csv")
    print(f"R2FS_V4_REPLAY segment={index}/12 architecture=breakout_confirm capacity_replayed=true",flush=True)
    details=replay(c1,c5)
    rows=write_outputs(details,seg/"r2fs_v4_detail.csv",seg/"r2fs_v4_outcomes.csv")
    stat=metrics(rows)
    summary={"segment":index,"start":start.isoformat(),"end":end.isoformat(),**stat}
    (seg/"segment_summary.json").write_text(json.dumps(summary,indent=2,sort_keys=True),encoding="utf-8")
    print("R2FS_V4_SEGMENT_DONE "+json.dumps(summary,separators=(",",":")),flush=True)
    return summary


def main():
    os.environ.pop("DATABASE_URL",None)
    print("R2FS_V4_START segments=12 historical_only=true live_shadow_untouched=true entry_redesign=true tp_sl_unchanged=true",flush=True)
    summaries=[]
    for index,(seg_id,end_iso) in enumerate(SEGMENTS,start=1):
        err=None
        for attempt in range(1,MAX_ATTEMPTS+1):
            try:
                print(f"R2FS_V4_ATTEMPT segment={index}/12 attempt={attempt}/{MAX_ATTEMPTS}",flush=True)
                summaries.append(run_segment(index,seg_id,end_iso)); err=None; break
            except Exception as exc:
                err=exc
                print(f"R2FS_V4_SEGMENT_ERROR segment={index}/12 attempt={attempt}/{MAX_ATTEMPTS} error={type(exc).__name__}:{exc}",flush=True)
                if attempt<MAX_ATTEMPTS: time.sleep(10)
        if err is not None:
            raise RuntimeError(f"R2FS V4 stopped at segment {index}/12: {err}")

    trades=sum(x["trades"] for x in summaries)
    wins=sum(x["wins"] for x in summaries)
    losses=sum(x["losses"] for x in summaries)
    gp=sum(x["gross_positive_net_pct"] for x in summaries)
    gl=sum(x["gross_negative_abs_net_pct"] for x in summaries)
    total=sum(x["sum_net_return_pct"] for x in summaries)
    pf=gp/gl if gl>0 else (999.0 if gp>0 else 0.0)
    positive=sum(1 for x in summaries if x["sum_net_return_pct"]>0)
    negative=sum(1 for x in summaries if x["sum_net_return_pct"]<0)
    keep=trades>=200 and pf>=1.15 and total>0 and positive>=7

    final={
        "version":CORE,"trades":trades,"wins":wins,"losses":losses,
        "win_rate_pct":round(100*wins/trades,2) if trades else 0.0,
        "gross_positive_net_pct":round(gp,6),"gross_negative_abs_net_pct":round(gl,6),
        "profit_factor":round(pf,6),"sum_net_return_pct":round(total,6),
        "positive_segments":positive,"negative_segments":negative,"keep_candidate":keep,
        "criteria":{"min_trades":200,"min_profit_factor":1.15,"sum_net_return_positive":True,"min_positive_segments":7},
        "segments":summaries,
    }
    (ROOT/"r2fs_v4_final.json").write_text(json.dumps(final,indent=2,sort_keys=True),encoding="utf-8")
    print("R2FS_V4_SUMMARY "+json.dumps(final,separators=(",",":")),flush=True)
    print(f"R2FS_V4_DONE segments=12/12 trades={trades} pf={pf:.6f} sum={total:+.6f} positive_segments={positive}/12 keep_candidate={keep}",flush=True)
    print("R2FS_V4_HOLD process_complete=true restart_prevention=true",flush=True)
    while True:
        time.sleep(3600)


if __name__=="__main__":
    main()
