from __future__ import annotations
import json
import subprocess
import sys
from pathlib import Path

# Isolated Meme2 20-wallet historical batch:
# - 20 wallets
# - full 360-day window
# - 1000 rows/page
# - separate output directory
# - no live DB / no orders / no shadow writes
# - success marker only when manifest confirms 20/20 complete

WALLETS = [
    "HzKH3s8Kf7f7pgvJZx2V6TzhkdHbZDurG53rXj61BVLR",
    "kCtFJ3xQWahz63HkPGwNfXRk21MBrz4sdeFBUeznUkC",
    "An9sREpLnAXVi4KMaTGuGvgET51CyaukLUTMtxzmLYSB",
    "G318mttEeAmdDuVkwA1Dse76QhnuKrM2w4FPkG724adW",
    "7ufmve7ZSFCzuNcKRunYrGtyb2Ka1MXzkWwf7jZhVsmL",
    "BNnN2MqfWLvgThYBsv6v8JQaYZXYKYahC5YCy27Ct1cX",
    "81HHNStHsNMN8hrLbD2LFxzaikvzueZp4XWzvvmGePC1",
    "6tzAKfHtQA4zLwgaXn97kU5C1dYsKcRy8XZhZUWmWU23",
    "3kxcF8wHKm4sEtnxjXXeUvNGeDjTmo47LgtSRg71YfG5",
    "DS5ZrUiQwSNd1Y14nFKTTwaAxH4PR7gAzE5ac6aLp8cS",
    "92cwTJoGDFKSnp6Zr2taFr3sFWqs8jRvtU73qbut2win",
    "CJgBunSv2nagagWqBuPtQmrsPmPajewsZjvGyZhpNSzb",
    "CQGxdVAvrRvb8TvrGmMYQCcHjCCbFuhKgTWSEVqFJgfm",
    "8MoW9mtbEz6z3gPuAdYb1yWhjCAxQSYqpcTb1CQgN5qb",
    "DAtJJbckkqADvpt7bhCqNHMpitYBVHfFP3ZsisNvbBTC",
    "A7ZmFuTUEb14ArzoYf7mBJUPmdvbLbj1J8kvKAaJAWek",
    "EvbPo7eCCmLxb95WSbS4P7JZvc6mxLxyG8CJUBydteN2",
    "9XQLDx1MV5Ssv48DfFfBwFErh8ds6jfXfHZBJKj1FqXY",
    "dtrzJPj7yDdvm6eRqBAgxsK2sMJeD9HhBEBB3XMedXy",
    "GUkwZGEfDQuKSSyQy9225swpLAzy3Ssko4u3zNbRYQzx",
]

OUT_DIR = Path("data/historical_360/meme2_test_twenty/raw")
MANIFEST = OUT_DIR / "manifest.json"

cmd = [
    sys.executable,
    "meme2_historical_backfill.py",
    "--start", "2025-11-02T09:30:00Z",
    "--end", "2026-10-28T09:30:00Z",
    "--out-dir", str(OUT_DIR),
    "--limit", "1000",
    "--pause", "0.15",
]

for wallet in WALLETS:
    cmd.extend(["--wallet", wallet])

print("MEME2_TWENTY_WALLET_TEST_START", flush=True)
print("wallets=20 limit=1000 isolated=true", flush=True)

subprocess.run(cmd, check=True)

if not MANIFEST.exists():
    raise SystemExit("MEME2_TWENTY_MANIFEST_MISSING")

manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
summary = manifest.get("summary") or {}
complete = int(summary.get("complete_wallets", 0))
wallets = int(summary.get("wallets", 0))
rows = int(summary.get("rows", 0))

print(
    f"MEME2_TWENTY_VERIFY complete_wallets={complete} wallets={wallets} rows={rows}",
    flush=True,
)

if complete != 20 or wallets != 20:
    raise SystemExit(
        f"MEME2_TWENTY_INCOMPLETE complete_wallets={complete} wallets={wallets}"
    )

print("MEME2_TWENTY_WALLET_TEST_DONE", flush=True)
