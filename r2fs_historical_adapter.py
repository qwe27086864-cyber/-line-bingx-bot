"""
R2FS historical replay adapter (FAST path V1).

Matches the currently deployed r2-development/r2_fast_job.py FAST path as
closely as OHLCV history permits:

- 5m radar move threshold: 0.40%
- max 8 radar candidates, deep-analyze top 5
- 1m + 5m EMA/RSI/momentum/volume structure
- BTC/ETH market-alignment score
- direction >= 72, micro >= 72, market >= 45, fast score >= 78
- extension limits
- TP 2/4/8%, SL 2.5%
- partials 35/40/25
- TP1 -> breakeven; TP2 -> TP1 protection
- timeout at 4h ONLY if TP1 was never hit
- estimated round-trip cost: 0.16 percentage points
- simulated margin 10U, leverage 8x

Historical limitation:
BingX OHLCV does not contain the historical executable BBO used by live R2FS.
Therefore spread and chase are explicit configurable PROXIES. Results must be
treated as research estimates and later validated with forward shadow data.

Input:
  --candles-1m CSV from historical_data.py
  --candles-5m CSV from historical_data.py

Outputs:
  detailed CSV
  router-compatible outcomes CSV:
    timestamp,symbol,core,net_return_pct
"""
from __future__ import annotations

import argparse
import bisect
import csv
import math
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

CORE = "R2FS"

RADAR_MIN_MOVE = 0.40
RADAR_MAX_CANDIDATES = 8
MAX_FAST_ANALYSIS = 5
MIN_QUOTE_VOLUME_24H = 250_000.0

FAST_MAX_SPREAD = 0.25
FAST_MAX_CHASE = 0.35
FAST_MIN_DIRECTION = 72.0
FAST_MIN_MICRO = 72.0
FAST_MIN_MARKET = 45.0
FAST_MIN_SCORE = 78.0

TP1 = 2.0
TP2 = 4.0
TP3 = 8.0
SL = 2.5
W1, W2, W3 = 0.35, 0.40, 0.25

ROUND_TRIP_COST_PCT = 0.16
SIM_MARGIN_USDT = 10.0
SIM_LEVERAGE = 8

DETAIL_FIELDS = [
    "timestamp","symbol","core","side","decision","reason",
    "radar_move_pct","direction_score","micro_score","market_score",
    "execution_score","fast_score","assumed_spread_pct","assumed_chase_pct",
    "entry_price","exit_price","exit_reason","gross_return_pct",
    "net_return_pct","sim_pnl_usdt","tp1_hit","tp2_hit","tp3_hit",
    "resolved","history_quality",
]
OUTCOME_FIELDS = ["timestamp","symbol","core","net_return_pct"]


def parse_dt(s: str) -> datetime:
    x = datetime.fromisoformat(s.replace("Z","+00:00"))
    if x.tzinfo is None:
        raise ValueError("timestamps must be timezone-aware")
    return x.astimezone(timezone.utc)


def pct(a: float, b: float) -> float:
    return (float(a) / float(b) - 1.0) * 100.0 if b else 0.0


def clamp(x: float, lo=0.0, hi=100.0) -> float:
    return max(lo, min(hi, float(x)))


def directional(side: str, value: float) -> float:
    return float(value) if side == "LONG" else -float(value)


def ema(values, period: int):
    vals = [float(x) for x in values]
    if len(vals) < period:
        return None
    alpha = 2.0 / (period + 1.0)
    e = sum(vals[:period]) / period
    for x in vals[period:]:
        e = alpha * x + (1.0-alpha) * e
    return e


def rsi(values, period=14):
    vals = [float(x) for x in values]
    if len(vals) < period + 1:
        return None
    gains, losses = [], []
    for a,b in zip(vals[-(period+1):-1], vals[-period:]):
        d = b-a
        gains.append(max(d,0.0))
        losses.append(max(-d,0.0))
    ag = sum(gains)/period
    al = sum(losses)/period
    if al == 0:
        return 100.0 if ag > 0 else 50.0
    rs = ag/al
    return 100.0 - 100.0/(1.0+rs)


def volume_ratio(volumes, lookback=20):
    if len(volumes) < lookback + 1:
        return 0.0
    prev = [float(x) for x in volumes[-(lookback+1):-1]]
    avg = sum(prev)/len(prev) if prev else 0.0
    return float(volumes[-1])/avg if avg > 0 else 0.0


def aligned(side, price, fast_ema, slow_ema):
    if side == "LONG":
        return price > fast_ema > slow_ema
    return price < fast_ema < slow_ema


def directional_extension(side, price, e):
    raw = pct(price, e)
    return raw if side == "LONG" else -raw


def execution_score(spread, adverse_chase):
    if spread is None:
        score = 0.0
    elif spread <= 0.08: score = 100.0
    elif spread <= 0.15: score = 94.0
    elif spread <= 0.25: score = 82.0
    elif spread <= 0.35: score = 68.0
    elif spread <= 0.50: score = 48.0
    elif spread <= 0.60: score = 30.0
    else: score = 0.0

    if adverse_chase <= 0.10: pass
    elif adverse_chase <= 0.20: score -= 5
    elif adverse_chase <= 0.35: score -= 12
    elif adverse_chase <= 0.50: score -= 28
    elif adverse_chase <= 0.75: score -= 48
    else: score -= 70
    return clamp(score)


def read_candles(path: str):
    by_symbol = defaultdict(list)
    with open(path, newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            item = {
                "t": parse_dt(r["timestamp"]),
                "symbol": r["symbol"].upper(),
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r.get("volume") or 0),
                "quote_volume": float(r.get("quote_volume") or 0),
            }
            if min(item["open"],item["high"],item["low"],item["close"]) <= 0:
                continue
            by_symbol[item["symbol"]].append(item)
    for s in by_symbol:
        by_symbol[s].sort(key=lambda x:x["t"])
    return by_symbol


def rolling_ticker_context(c5):
    """Precompute OHLCV proxies corresponding to the current all_tickers snapshot."""
    out = {}
    q = deque()
    qsum = 0.0
    for i,row in enumerate(c5):
        t = row["t"]
        cutoff = t.timestamp() - 24*3600
        q.append(row)
        qsum += row["quote_volume"]
        while q and q[0]["t"].timestamp() < cutoff:
            qsum -= q.popleft()["quote_volume"]

        change24 = 0.0
        if i >= 288:
            change24 = pct(row["close"], c5[i-288]["close"])
        out[t] = {
            "last": row["close"],
            "quote_volume": qsum,
            "change_24h": change24,
        }
    return out


def market_alignment(side, t, contexts):
    score = 50.0
    for symbol in ("BTC-USDT","ETH-USDT"):
        ctx = contexts.get(symbol, {})
        cur = ctx.get(t)
        if not cur:
            continue
        times = sorted(ctx)
        idx = bisect.bisect_left(times,t)
        if idx <= 0:
            continue
        prev = ctx[times[idx-1]]
        move5 = pct(cur["last"], prev["last"])
        d5 = directional(side, move5)
        d24 = directional(side, cur["change_24h"])
        if d5 >= 0.10: score += 12
        elif d5 >= 0.03: score += 6
        elif d5 <= -0.10: score -= 14
        elif d5 <= -0.03: score -= 7
        if d24 > 0: score += 4
        elif d24 < 0: score -= 4
    return clamp(score)


def bars_up_to(series, t, n):
    times = [x["t"] for x in series]
    idx = bisect.bisect_right(times,t)
    if idx <= 0:
        return []
    return series[max(0,idx-n):idx]


def fast_scores(symbol, side, t, c1_by, c5_by, contexts, assumed_spread, assumed_chase):
    c1 = bars_up_to(c1_by.get(symbol,[]), t, 90)
    c5 = bars_up_to(c5_by.get(symbol,[]), t, 100)
    if min(len(c1),len(c5)) < 55:
        return None

    close1=[x["close"] for x in c1]
    close5=[x["close"] for x in c5]
    high5=[x["high"] for x in c5]
    low5=[x["low"] for x in c5]
    vol1=[x["volume"] for x in c1]
    vol5=[x["volume"] for x in c5]
    p1,p5=close1[-1],close5[-1]

    ema5_1,ema13_1=ema(close1,5),ema(close1,13)
    ema9_5,ema20_5=ema(close5,9),ema(close5,20)
    rsi1,rsi5=rsi(close1,14),rsi(close5,14)
    if any(x is None for x in (ema5_1,ema13_1,ema9_5,ema20_5,rsi1,rsi5)):
        return None

    mom1=pct(close1[-1],close1[-4])
    mom5=pct(close5[-1],close5[-4])
    d1,d5=directional(side,mom1),directional(side,mom5)
    vr1,vr5=volume_ratio(vol1),volume_ratio(vol5)
    a1=aligned(side,p1,ema5_1,ema13_1)
    a5=aligned(side,p5,ema9_5,ema20_5)

    prev_high=max(high5[-21:-1]); prev_low=min(low5[-21:-1])
    breakout = p5 > prev_high if side=="LONG" else p5 < prev_low
    ext1=directional_extension(side,p1,ema13_1)
    ext5=directional_extension(side,p5,ema20_5)

    ds=30 if a5 else 0
    if d5>0.25: ds+=15
    if d5>0.60: ds+=12
    if d5>1.20: ds+=8
    if vr5>=1.2: ds+=10
    if vr5>=1.8: ds+=5
    if breakout: ds+=10
    if side=="LONG":
        if 48<=rsi5<=76: ds+=10
        if rsi5>=84: ds-=15
    else:
        if 24<=rsi5<=52: ds+=10
        if rsi5<=16: ds-=15
    if ext5>3.5: ds-=12
    if ext5>5.0: ds-=10
    ds=clamp(ds)

    ms=24 if a1 else 0
    if d1>0.03: ms+=12
    if d1>0.12: ms+=12
    if d1>0.30: ms+=8
    if vr1>=1.15: ms+=14
    if vr1>=1.8: ms+=8
    if side=="LONG":
        if 50<=rsi1<=74: ms+=15
        if rsi1>=84: ms-=20
    else:
        if 26<=rsi1<=50: ms+=15
        if rsi1<=16: ms-=20
    if ext1<=0.55: ms+=12
    elif ext1>1.0: ms-=14
    if d5>0: ms+=7
    ms=clamp(ms)

    mkt=market_alignment(side,t,contexts)
    chase=max(float(assumed_chase),0.0)
    es=execution_score(float(assumed_spread),chase)
    fs=clamp(0.38*ds+0.34*ms+0.18*es+0.10*mkt)

    decision="FAST_ENTER"; reason="fast path passed"
    if assumed_spread>FAST_MAX_SPREAD:
        decision="FAST_FALLBACK_SPREAD"; reason="historical spread proxy over gate"
    elif chase>FAST_MAX_CHASE:
        decision="FAST_FALLBACK_CHASE"; reason="historical chase proxy over gate"
    elif ds<FAST_MIN_DIRECTION:
        decision="FAST_FALLBACK_DIRECTION"; reason=f"direction {ds:.1f} < {FAST_MIN_DIRECTION:.1f}"
    elif ms<FAST_MIN_MICRO:
        decision="FAST_FALLBACK_MICRO"; reason=f"micro {ms:.1f} < {FAST_MIN_MICRO:.1f}"
    elif mkt<FAST_MIN_MARKET:
        decision="FAST_FALLBACK_MARKET"; reason=f"market {mkt:.1f} < {FAST_MIN_MARKET:.1f}"
    elif fs<FAST_MIN_SCORE:
        decision="FAST_FALLBACK_SCORE"; reason=f"fast score {fs:.1f} < {FAST_MIN_SCORE:.1f}"
    elif ext1>1.10 or ext5>4.50:
        decision="FAST_FALLBACK_EXTENSION"; reason=f"extension 1m={ext1:.2f} 5m={ext5:.2f}"

    return {
        "direction_score":ds,"micro_score":ms,"market_score":mkt,
        "execution_score":es,"fast_score":fs,"decision":decision,
        "reason":reason,"breakout":breakout,
    }


def side_return(side, entry, x):
    return (entry-x)/entry*100.0 if side=="SHORT" else (x-entry)/entry*100.0


def level(entry, side, pct_move):
    return entry*(1+pct_move/100.0) if side=="LONG" else entry*(1-pct_move/100.0)


def stop_hit(side, bar, target):
    return bar["high"] >= target if side=="SHORT" else bar["low"] <= target


def tp_hit(side, bar, target):
    return bar["low"] <= target if side=="SHORT" else bar["high"] >= target


def gross_stop(side, entry, tp1, tp2, h1, h2, stop):
    realized=0.0; remaining=1.0
    if h1:
        realized += W1*side_return(side,entry,tp1); remaining -= W1
    if h2:
        realized += W2*side_return(side,entry,tp2); remaining -= W2
    return realized + remaining*side_return(side,entry,stop)


def simulate_exit(side, entry, entry_time, bars, max_runner_hours=24):
    tp1=level(entry,side,TP1); tp2=level(entry,side,TP2); tp3=level(entry,side,TP3)
    sl=level(entry,side,-SL)
    h1=h2=h3=False
    last=None

    for bar in bars:
        if bar["t"] <= entry_time:
            continue
        last=bar
        age=(bar["t"]-entry_time).total_seconds()/3600.0

        if (not h1) and age>=4.0:
            gross=side_return(side,entry,bar["open"])
            return bar["open"],"TIMEOUT_EXIT",gross,h1,h2,h3,True

        if h1 and age>=max_runner_hours:
            gross=gross_stop(side,entry,tp1,tp2,h1,h2,bar["open"])
            return bar["open"],"RUNNER_RESEARCH_CAP",gross,h1,h2,h3,True

        if h2:
            active=tp1; reason="TP2_PROTECT_EXIT"
        elif h1:
            active=entry; reason="BREAKEVEN_EXIT"
        else:
            active=sl; reason="SL_EXIT"

        # exact conservative order used by current replay: stop before new TP
        if stop_hit(side,bar,active):
            gross=gross_stop(side,entry,tp1,tp2,h1,h2,active)
            return active,reason,gross,h1,h2,h3,True

        if (not h1) and tp_hit(side,bar,tp1):
            h1=True
            if stop_hit(side,bar,entry):
                gross=gross_stop(side,entry,tp1,tp2,True,False,entry)
                return entry,"BREAKEVEN_EXIT",gross,True,False,False,True

        if (not h2) and tp_hit(side,bar,tp2):
            h1=h2=True
            if stop_hit(side,bar,tp1):
                gross=gross_stop(side,entry,tp1,tp2,True,True,tp1)
                return tp1,"TP2_PROTECT_EXIT",gross,True,True,False,True

        if (not h3) and tp_hit(side,bar,tp3):
            h1=h2=True
            if stop_hit(side,bar,tp1):
                gross=gross_stop(side,entry,tp1,tp2,True,True,tp1)
                return tp1,"TP2_PROTECT_EXIT",gross,True,True,False,True
            h3=True
            gross=W1*TP1+W2*TP2+W3*TP3
            return tp3,"TP3_EXIT",gross,True,True,True,True

    return (last["close"] if last else entry),"UNRESOLVED",0.0,h1,h2,h3,False


def replay(c1_by,c5_by,assumed_spread,assumed_chase,max_runner_hours):
    contexts={s:rolling_ticker_context(rows) for s,rows in c5_by.items()}
    all_times=sorted({r["t"] for rows in c5_by.values() for r in rows})
    cooldown_until={}
    details=[]

    for t in all_times:
        candidates=[]
        for symbol,rows in c5_by.items():
            ctx=contexts[symbol]
            cur=ctx.get(t)
            if not cur:
                continue
            times=sorted(ctx)
            idx=bisect.bisect_left(times,t)
            if idx<=0:
                continue
            prev=ctx[times[idx-1]]
            move=pct(cur["last"],prev["last"])
            if abs(move)<RADAR_MIN_MOVE:
                continue
            if cur["quote_volume"] and cur["quote_volume"]<MIN_QUOTE_VOLUME_24H:
                continue
            side="LONG" if move>0 else "SHORT"
            rank=min(abs(move),6.0)*12.0
            if cur["quote_volume"]>=1_000_000: rank+=8
            if cur["quote_volume"]>=5_000_000: rank+=6
            if assumed_spread<=0.20: rank+=12
            elif assumed_spread<=0.35: rank+=7
            if directional(side,cur["change_24h"])>0: rank+=5
            candidates.append((rank,abs(move),symbol,side,move,cur["last"]))

        candidates.sort(reverse=True)
        for _,__,symbol,side,move,signal_price in candidates[:RADAR_MAX_CANDIDATES][:MAX_FAST_ANALYSIS]:
            if cooldown_until.get(symbol) and t < cooldown_until[symbol]:
                continue
            scores=fast_scores(symbol,side,t,c1_by,c5_by,contexts,assumed_spread,assumed_chase)
            if not scores:
                continue

            row={
                "timestamp":t.isoformat(),"symbol":symbol,"core":CORE,"side":side,
                "decision":scores["decision"],"reason":scores["reason"],
                "radar_move_pct":round(move,6),
                "direction_score":round(scores["direction_score"],3),
                "micro_score":round(scores["micro_score"],3),
                "market_score":round(scores["market_score"],3),
                "execution_score":round(scores["execution_score"],3),
                "fast_score":round(scores["fast_score"],3),
                "assumed_spread_pct":assumed_spread,
                "assumed_chase_pct":assumed_chase,
                "entry_price":"","exit_price":"","exit_reason":"",
                "gross_return_pct":"","net_return_pct":"","sim_pnl_usdt":"",
                "tp1_hit":False,"tp2_hit":False,"tp3_hit":False,
                "resolved":False,
                "history_quality":"OHLCV_FAST_PATH_PROXY_BBO",
            }

            if scores["decision"]!="FAST_ENTER":
                details.append(row)
                continue

            entry=float(signal_price)
            future=c1_by.get(symbol,[])
            exit_price,reason,gross,h1,h2,h3,resolved=simulate_exit(
                side,entry,t,future,max_runner_hours
            )
            net=gross-ROUND_TRIP_COST_PCT if resolved else 0.0
            pnl=SIM_MARGIN_USDT*SIM_LEVERAGE*net/100.0 if resolved else 0.0
            row.update({
                "entry_price":round(entry,12),"exit_price":round(exit_price,12),
                "exit_reason":reason,"gross_return_pct":round(gross,6),
                "net_return_pct":round(net,6),"sim_pnl_usdt":round(pnl,6),
                "tp1_hit":h1,"tp2_hit":h2,"tp3_hit":h3,"resolved":resolved,
            })
            details.append(row)

            # Current R2FS uses a 2h symbol cooldown from signal/open.
            from datetime import timedelta
            cooldown_until[symbol]=t+timedelta(hours=2)

    return details


def write_outputs(details,detail_path,outcome_path):
    Path(detail_path).parent.mkdir(parents=True,exist_ok=True)
    with open(detail_path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=DETAIL_FIELDS)
        w.writeheader(); w.writerows(details)

    outcomes=[
        {
            "timestamp":r["timestamp"],"symbol":r["symbol"],"core":CORE,
            "net_return_pct":r["net_return_pct"],
        }
        for r in details
        if r["decision"]=="FAST_ENTER" and r["resolved"]
    ]
    with open(outcome_path,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=OUTCOME_FIELDS)
        w.writeheader(); w.writerows(outcomes)

    vals=[float(x["net_return_pct"]) for x in outcomes]
    wins=[x for x in vals if x>0]; losses=[x for x in vals if x<0]
    pf=(sum(wins)/abs(sum(losses))) if losses else (999.0 if wins else 0.0)
    return {
        "details":len(details),
        "closed_fast_entries":len(vals),
        "win_rate_pct":round(100*len(wins)/len(vals),2) if vals else 0.0,
        "profit_factor":round(pf,3),
        "sum_net_return_pct":round(sum(vals),3),
    }


def main():
    p=argparse.ArgumentParser()
    p.add_argument("--candles-1m",required=True)
    p.add_argument("--candles-5m",required=True)
    p.add_argument("--detail-out",default="data/r2fs_history_detail.csv")
    p.add_argument("--outcomes-out",default="data/r2fs_core_outcomes.csv")
    p.add_argument("--assumed-spread-pct",type=float,default=0.12)
    p.add_argument("--assumed-chase-pct",type=float,default=0.05)
    p.add_argument("--max-runner-hours",type=float,default=24.0)
    args=p.parse_args()

    c1=read_candles(args.candles_1m)
    c5=read_candles(args.candles_5m)
    details=replay(
        c1,c5,args.assumed_spread_pct,args.assumed_chase_pct,args.max_runner_hours
    )
    print(write_outputs(details,args.detail_out,args.outcomes_out))


if __name__=="__main__":
    main()
