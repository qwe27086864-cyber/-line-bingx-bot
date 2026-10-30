from __future__ import annotations
import subprocess
import sys

# Isolated Meme2 historical smoke test:
# - one wallet only
# - full 360-day window
# - 1000 rows/page
# - separate output directory
# - no live DB / no orders / no shadow writes

WALLET = "HzKH3s8Kf7f7pgvJZx2V6TzhkdHbZDurG53rXj61BVLR"

cmd = [
    sys.executable,
    "meme2_historical_backfill.py",
    "--start", "2025-11-02T09:30:00Z",
    "--end", "2026-10-28T09:30:00Z",
    "--out-dir", "data/historical_360/meme2_test_one/raw",
    "--wallet", WALLET,
    "--limit", "1000",
    "--pause", "0.15",
]

print("MEME2_ONE_WALLET_TEST_START", flush=True)
print("wallet=" + WALLET[:8] + ".. limit=1000 isolated=true", flush=True)
subprocess.run(cmd, check=True)
print("MEME2_ONE_WALLET_TEST_DONE", flush=True)
