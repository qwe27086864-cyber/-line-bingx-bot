# R2FS Historical Adapter

This is the first core adapter for Historical Router V1.

It reproduces the currently deployed R2FS FAST-path scoring and the current
conservative partial-TP replay logic. It does **not** claim exact historical BBO
execution, because ordinary OHLCV history does not contain the executable bid/ask
snapshot that live R2FS reads.

Use the same symbol universe for both files:

```bash
python historical_data.py --symbols BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT   --interval 1m --days 90 --out data/history_1m.csv

python historical_data.py --symbols BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT   --interval 5m --days 90 --out data/history_5m.csv

python r2fs_historical_adapter.py   --candles-1m data/history_1m.csv   --candles-5m data/history_5m.csv   --outcomes-out data/r2fs_core_outcomes.csv
```

`r2fs_core_outcomes.csv` can then be fed into `router_trainer.py`.

Important: this V1 replays only the R2FS **FAST_ENTER** branch. The full-R2 fallback
branch is intentionally excluded until its historical adapter is implemented.
