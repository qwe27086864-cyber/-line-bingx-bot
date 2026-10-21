R1M live-like paper simulation patch

Upload sitecustomize.py to the repository root of branch: backup-v42r1-best
Do not rename it.

The existing Render cron 'bingx-v5-test' runs python scanner_job.py every 5 minutes.
Python auto-imports sitecustomize.py before scanner_job.py, so this patch will:
- force paper engine_version = R1M
- max 3 concurrent paper positions
- keep R1 signal/TP/SL/BE/timeout core
- enter LONG at fresh best ask, SHORT at fresh best bid
- keep simulated fee 0.05%/side + slippage 0.03%/side
- keep old V4.2R1 rows untouched
- record entry diagnostics in r1m_entry_diagnostics

Expected Render log after deploy:
R1M PATCH ACTIVE | paper only | engine=R1M | max_open=3 | entry=best ask/bid | fees+slippage retained

This patch does not submit BingX orders.
