from __future__ import annotations
import subprocess
import sys

# Isolated Meme2 historical batch test:
# - five wallets
# - full 360-day window
# - 1000 rows/page
# - separate output directory
# - no live DB / no orders / no shadow writes

WALLETS = [
    "HzKH3s8Kf7f7pgvJZx2V6TzhkdHbZDurG53rXj61BVLR",
    "kCtFJ3xQWahz63HkPGwNfXRk21MBrz4sdeFBUeznUkC",
    "An9sREpLnAXVi4KMaTGuGvgET51CyaukLUTMtxzmLYSB",
    "G318mttEeAmdDuVkwA1Dse76QhnuKrM2w4FPkG724adW",
    "7ufmve7ZSFCzuNcKRunYrGtyb2Ka1MXzkWwf7jZhVsmL",
]

cmd = [
    sys.executable,
    "meme2_historical_backfill.py",
    "--start", "2025-11-02T09:30:00Z",
    "--end", "2026-10-28T09:30:00Z",
    "--out-dir", "data/historical_360/meme2_test_five/raw",
    "--limit", "1000",
    "--pause", "0.15",
]

for wallet in WALLETS:
    cmd.extend(["--wallet", wallet])

print("MEME2_FIVE_WALLET_TEST_START", flush=True)
print("wallets=5 limit=1000 isolated=true", flush=True)
subprocess.run(cmd, check=True)
print("MEME2_FIVE_WALLET_TEST_DONE", flush=True)
