#!/usr/bin/env python3
"""One-command OFFLINE V4.2R1 backtest orchestration; never contacts exchanges."""
import argparse, json, csv, hashlib
from pathlib import Path
from datetime import timedelta
from v42_original_signal_bridge import load_history, build_signals
from v42_historical_backtest import load_market, load_signals, report
from v42_execution_phase4 import evaluate

def sha256(p):
    return hashlib.sha256(Path(p).read_bytes()).hexdigest()

def run(app, scanner, candles, out, start=None, end=None):
    output=Path(out);output.mkdir(parents=True, exist_ok=True)
    h=load_history(candles)
    signals, diagnostics=build_signals(h,app,scanner,start,end)
    path=output/'signals.csv'
    with path.open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=['time','symbol','side','score','regime','strategy'])
        w.writeheader();w.writerows(signals)
    trades,rejections=evaluate(load_market(candles),load_signals(path)) if signals else ([],{})
    result=report(trades,rejections)
    result['provenance']={'app_sha256':sha256(app),'scanner_sha256':sha256(scanner),
                          'input_files':[{'name':p.name,'sha256':sha256(p)} for p in sorted(Path(candles).glob('*.csv'))]}
    result['signal_diagnostics']=diagnostics
    result['limitations']=['Not validated versus original live V4.2R1 trades',
      'Historical radar reconstructs ticker snapshots from 1m OHLCV (not identical)',
      'Requires observed historical spread_pct, not a made-up constant',
      'TP1 breakeven and TP2 protection approximated; original TREND_EXIT not yet integrated',
      'Risk gates use chronological realized PnL; partial original position margin logic approximated',
      'Intraminute TP/SL ordering unknown; stop-first is a conservative assumption',
      'Not a live trading system and does not verify past PF claims']
    (output/'report.json').write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
    (output/'trades.json').write_text(json.dumps(trades,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'signals':len(signals),'closed_trades':len(trades),'diagnostics':diagnostics,
                      'overall':result['overall'],'out':str(output)},ensure_ascii=False,indent=2))
    return result

def main():
    p=argparse.ArgumentParser(description=__doc__)
    for v in ('original-app','original-scanner','candles','out'):
        p.add_argument('--'+v,required=True)
    a=p.parse_args()
    run(a.original_app,a.original_scanner,a.candles,a.out)
if __name__=='__main__':main()
