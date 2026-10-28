# Historical Router V1

This package starts a **new meta/router core** without changing R2FS, R3, AS1,
BF1, MEME1 or MEME2.

## Goal

Use historical data to answer:

> In this market regime, which existing core has historically had a verified edge?

Unknown or weak regimes route to **OFF**.

## Phase 1 (included)

1. `historical_data.py`
   - downloads public BingX perpetual OHLCV
   - saves raw data to CSV, not Postgres
   - no API key / no trading

2. `market_regime.py`
   - produces leakage-safe market features
   - labels descriptive regimes:
     `TREND_UP`, `TREND_DOWN`, `RANGE_LOW_VOL`, `RANGE_HIGH_VOL`,
     `SURGE`, `CRASH`, `VOLATILE_TRANSITION`, `TRANSITION`

3. `router_trainer.py`
   - joins historical core outcomes to the latest prior market feature
   - chronological 60/20/20 split
   - trains only on first 60%
   - validates on next 20%
   - reports final 20% as blind test
   - routes to `OFF` when evidence is insufficient

## Important

This V1 intentionally **does not deploy or trade**. It is a research harness.

The current 1 GB Render Postgres should keep summaries/results, not large raw
history. Historical OHLCV should live as files. Later, Parquet/object storage can
replace CSV without changing the router logic.

## First data run

Start small:

```bash
python historical_data.py \
  --symbols BTC-USDT,ETH-USDT,SOL-USDT \
  --interval 5m \
  --days 90 \
  --out data/history_5m.csv

python market_regime.py \
  --candles data/history_5m.csv \
  --out data/market_features.csv
```

## Outcome input format

`router_trainer.py` expects a CSV like:

```text
timestamp,symbol,core,net_return_pct
2026-01-01T00:05:00+00:00,BTC-USDT,R2FS,1.25
2026-01-01T00:15:00+00:00,BTC-USDT,R3,-0.80
```

These rows must come from historical replay of each existing core with realistic
fees/slippage. Do not create outcomes using future information.

Then:

```bash
python router_trainer.py \
  --features data/market_features.csv \
  --outcomes data/core_outcomes.csv \
  --out data/router_v1_report.json
```

## Next phase

Build one adapter per existing core to generate `core_outcomes.csv` from historical
replay:

- R2FS adapter
- R3 adapter
- AS1 adapter (needs higher-fidelity quote/fill assumptions)
- BF1 adapter (needs funding + spot/perp basis history)
- MEME1 adapter (needs historical pool/liquidity data)
- MEME2 adapter (needs wallet transaction history + contemporaneous market data)

Only after the adapters are validated should the router be considered for forward
shadow testing.
