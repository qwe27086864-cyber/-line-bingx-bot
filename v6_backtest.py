"""V6 offline, research-only event-driven backtest of supplied entry signals.
CSV candles: timestamp,symbol,open,high,low,close (UTC ISO timestamps).
CSV signals: timestamp,symbol,side (LONG or SHORT),strategy (R1 or V6).
Signals formed at candle close fill at NEXT candle open. No exchange/order calls.
This is an execution harness, NOT a reconstruction of the R1 scanner.
"""
import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def read_csv(path, required):
    with open(path, newline='', encoding='utf-8-sig') as f:
        rows = list(csv.DictReader(f))
    if not rows or not set(required).issubset(rows[0]):
        raise ValueError(f'{path}: required columns {required}, nonempty file')
    return rows


def dt(s):
    x = datetime.fromisoformat(s.replace('Z', '+00:00'))
    if x.tzinfo is None:
        raise ValueError('timestamps must have explicit timezone')
    return x.astimezone(timezone.utc)


def run(candles, signals, tp_pct=8.0, sl_pct=2.5, max_bars=48,
        fee_per_side=0.0008, slippage_per_side=0.0005):
    if min(tp_pct, sl_pct, max_bars) <= 0 or min(fee_per_side, slippage_per_side) < 0:
        raise ValueError('invalid parameters')
    series = defaultdict(list)
    for c in candles:
        t = dt(c['timestamp'])
        o, h, l, cl = (float(c[k]) for k in ('open','high','low','close'))
        if min(o,h,l,cl) <= 0 or l > min(o,cl) or h < max(o,cl):
            raise ValueError('invalid OHLC candle')
        series[c['symbol']].append((t,o,h,l,cl))
    for symbol in series:
        series[symbol].sort()
        if len(set(c[0] for c in series[symbol])) != len(series[symbol]):
            raise ValueError(f'duplicate candles: {symbol}')
    results=[]
    for s in signals:
        symbol, side = s['symbol'], s['side'].upper()
        if side not in ('LONG','SHORT'):
            raise ValueError('invalid side')
        start = dt(s['timestamp'])
        bars = series.get(symbol, [])
        idx = next((i for i,b in enumerate(bars) if b[0] > start), None)
        if idx is None:
            results.append({'symbol':symbol,'strategy':s['strategy'],'status':'NO_NEXT_BAR'})
            continue
        direction = 1 if side=='LONG' else -1
        entry_raw = bars[idx][1]
        entry = entry_raw * (1 + direction * slippage_per_side)
        end = min(len(bars), idx + int(max_bars))
        exit_raw = None
        reason = 'TIMEOUT'
        for j in range(idx, end):
            t,o,h,l,cl = bars[j]
            # Gap at open gets filled at open, not at a better stop price.
            adverse_open = direction*(o/entry_raw-1)*100 <= -sl_pct
            favorable_open = direction*(o/entry_raw-1)*100 >= tp_pct
            if adverse_open or favorable_open:
                exit_raw=o
                reason='SL_GAP' if adverse_open else 'TP_GAP'
                break
            stop=entry_raw*(1-direction*sl_pct/100)
            target=entry_raw*(1+direction*tp_pct/100)
            stop_hit = l <= stop if direction==1 else h >= stop
            target_hit = h >= target if direction==1 else l <= target
            if stop_hit or target_hit:
                # OHLC does not establish intrabar sequence; assume adverse fill first.
                exit_raw=stop if stop_hit else target
                reason='SL' if stop_hit else 'TP'
                break
        if exit_raw is None:
            exit_raw=bars[end-1][4]
            j=end-1
        exit_price=exit_raw*(1-direction*slippage_per_side)
        net_pct=(direction*(exit_price/entry-1)-2*fee_per_side)*100
        results.append({'symbol':symbol,'strategy':s['strategy'],'side':side,
                        'signal_time':start.isoformat(),'entry_time':bars[idx][0].isoformat(),
                        'exit_time':bars[j][0].isoformat(),'entry':entry,'exit':exit_price,
                        'exit_reason':reason,'net_pct_unlevered':round(net_pct,6),'status':'CLOSED'})
    return results


def report(rows):
    groups=defaultdict(list)
    for r in rows:
        if r['status']=='CLOSED':
            groups[(r['strategy'],r['entry_time'][:7])].append(r['net_pct_unlevered'])
    stats=[]
    for (strategy,month), vals in sorted(groups.items()):
        gain=sum(x for x in vals if x>0)
        loss=-sum(x for x in vals if x<0)
        stats.append({'strategy':strategy,'month':month,'trades':len(vals),
                      'win_rate_pct':round(100*sum(x>0 for x in vals)/len(vals),2),
                      'profit_factor':round(gain/loss,4) if loss else None,
                      'sum_trade_returns_pct_unlevered':round(sum(vals),5)})
    return {'summary':stats,'trades':rows,'limitations':[
        'Requires externally generated timestamped R1/V6 signals; does not reproduce scanner.',
        'Single-target exits; does not yet model R1 partial TP, portfolio capital, funding or concurrent-position limits.',
        'Intrabar TP/SL collision assumes stop first; missing bars are not automatically filled.',
        'Summed per-trade percentages are not portfolio profit or compounded returns.']}


def main():
    p=argparse.ArgumentParser()
    p.add_argument('--candles',required=True)
    p.add_argument('--signals',required=True)
    p.add_argument('--output',default='v6_backtest_report.json')
    p.add_argument('--tp',type=float,default=8)
    p.add_argument('--sl',type=float,default=2.5)
    p.add_argument('--max-bars',type=int,default=48)
    args=p.parse_args()
    candles=read_csv(args.candles,['timestamp','symbol','open','high','low','close'])
    signals=read_csv(args.signals,['timestamp','symbol','side','strategy'])
    output=report(run(candles,signals,args.tp,args.sl,args.max_bars))
    Path(args.output).write_text(json.dumps(output,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(output['summary'],indent=2))

if __name__=='__main__':
    main()
