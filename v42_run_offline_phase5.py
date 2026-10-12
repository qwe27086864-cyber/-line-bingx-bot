#!/usr/bin/env python3
"""Offline V4.2R1 backtest with TP2 trend exit, historical proxy explicitly labelled."""
import argparse,csv,json
from pathlib import Path
from v42_original_signal_bridge import load_history,build_signals,extract_exact
from v42_historical_backtest import load_market,load_signals,report
from v42_execution_phase5 import evaluate
from v42_run_offline_phase4 import sha256

def run(app,scanner,candles,out,start=None,end=None):
    dest=Path(out);dest.mkdir(parents=True,exist_ok=True)
    history=load_history(candles)
    signals,diagnostics=build_signals(history,app,scanner,start,end)
    with (dest/'signals.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=['time','symbol','side','score','regime','strategy'])
        w.writeheader();w.writerows(signals)
    ns={'__builtins__':__builtins__}
    extract_exact(app,{'calculate_ema','calculate_rsi'},ns)
    trades,errors=evaluate(load_market(candles),load_signals(dest/'signals.csv'),history,
                           ns['calculate_ema'],ns['calculate_rsi']) if signals else ([],{})
    result=report(trades,errors)
    result['provenance']={'app_sha256':sha256(app),'scanner_sha256':sha256(scanner),
        'inputs':[{'file':p.name,'sha256':sha256(p)} for p in sorted(Path(candles).glob('*.csv'))]}
    result['signal_diagnostics']=diagnostics
    result['limitations']=[
      'Not validated against original V4.2R1 forward trades or PF 2.6 claim',
      'Historical radar approximates ticker snapshots using minute OHLCV',
      'Historical spread must be observed, never fabricated',
      'Trend exit follows original weakness rules but uses last closed 1m price proxy for live ticker',
      'TP/SL ordering inside one 1m candle unknowable: stop-first conservative assumption',
      'Trend decisions evaluated at 5m boundaries AFTER minute close (causal)',
      'Original live partial-fill/position accounting remains approximate',
      'No live orders, database writes or exchange connections']
    (dest/'report.json').write_text(json.dumps(result,indent=2,ensure_ascii=False),encoding='utf-8')
    (dest/'trades.json').write_text(json.dumps(trades,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'signals':len(signals),'closed_trades':len(trades),'overall':result['overall'],
                      'diagnostics':diagnostics,'out':str(dest)},ensure_ascii=False,indent=2))
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser()
    for name in ('original-app','original-scanner','candles','out'):p.add_argument('--'+name,required=True)
    a=p.parse_args();run(a.original_app,a.original_scanner,a.candles,a.out)
