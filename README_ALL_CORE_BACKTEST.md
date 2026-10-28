# All-core historical backtest inputs

All adapters output the same Router schema:

`timestamp,symbol,core,net_return_pct`

## Directly backtestable from BingX candles
- R2FS: `history_1m.csv`, `history_5m.csv`
- R3.1: `history_1m.csv`, `history_5m.csv`, `history_15m.csv`, `history_1h.csv`

## BF1
Needs historical executable spot/perp BBO + funding snapshots:
`bf1_snapshots.csv`
columns:
`timestamp,symbol,spot_bid,spot_ask,perp_bid,perp_ask,funding_rate,next_funding_time_ms`

## AS1
Needs tick/BBO history. OHLCV is not sufficient for maker queue simulation.
`as1_events.csv`
columns:
`timestamp,event,bid,ask,trade_price,trade_qty,buyer_maker`
`event` is `BOOK` or `TRADE`.

## MEME1
Needs historical Solana pool snapshots at the time they were observable:
`meme1_pool_history.csv`
columns:
`timestamp,pool_address,mint_address,pool_created_at,price_usd,liquidity_usd,volume_h1_usd,buys_m5,sells_m5,price_change_m5_pct,fdv_usd,rug_score`

## MEME2
Needs historical wallet events with contemporaneous market state:
`meme2_wallet_history.csv`
columns:
`timestamp,wallet,side,mint_address,price_usd,liquidity_usd`

Do not reconstruct MEME1/MEME2 from only tokens that later survived. That creates survivorship bias.

Run all available datasets:
`python historical_suite.py --data-dir data`

The suite skips cores whose required history is missing rather than fabricating results.
