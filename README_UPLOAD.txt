R1 staging batch, 2026-10-19

All Python files are offline/read-only staging utilities. This is NOT a live-trading bot.

Existing files already on GitHub (no need to re-upload unless missing):
- r1_risk_gate.py
- r1_ledger_guard.py
- r1_exchange_snapshot_parser.py

New files to upload together to branch r1-line-live-staging:
- r1_execution_simulator.py
- r1_batch_validation.py

On iPhone: extract ZIP in Files app; GitHub branch > Add file > Upload files;
select both NEW .py files, commit once.

After deploy, run: python r1_batch_validation.py
Keep LIVE_TRADING=false and database kill_switch=true.
No trading API keys needed for these tests.
