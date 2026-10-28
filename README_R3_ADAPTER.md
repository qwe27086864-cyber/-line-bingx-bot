# R3.1 Historical Adapter

This adapter replays the **currently deployed R3.1** state machine. It is not a
generic breakout backtest.

Required historical files:

```bash
python historical_data.py --symbols BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT \
  --interval 1m --days 90 --out data/history_1m.csv

python historical_data.py --symbols BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT \
  --interval 5m --days 90 --out data/history_5m.csv

python historical_data.py --symbols BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT \
  --interval 15m --days 90 --out data/history_15m.csv

python historical_data.py --symbols BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT \
  --interval 1h --days 90 --out data/history_1h.csv
```

Run:

```bash
python r3_historical_adapter.py \
  --candles-1m data/history_1m.csv \
  --candles-5m data/history_5m.csv \
  --candles-15m data/history_15m.csv \
  --candles-1h data/history_1h.csv \
  --assumed-spread-pct 0.12 \
  --outcomes-out data/r3_core_outcomes.csv
```

The output `r3_core_outcomes.csv` matches the Historical Router input format.

## Important limitation

Historical OHLCV has no executable historical BBO. The adapter therefore uses an
explicit spread proxy and adjusts entry by half that spread. This is suitable for
research/ranking, not proof of live profitability.

The adapter uses completed candles only for indicators and structure, while the
radar/live-price proxy comes from the latest completed 1m candle. This is designed
to avoid future-data leakage while still reproducing R3.1's pending-breakout timing
behavior.
