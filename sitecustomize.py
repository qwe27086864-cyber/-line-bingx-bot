# sitecustomize.py
# Safe bootstrap for M2 Ledger V4.
# Python imports sitecustomize automatically on startup when present on sys.path.
# It only activates on the Ledger service when M2_LEDGER_V4_BOOTSTRAP=1.

import os
import sys

if (
    os.getenv("M2_LEDGER_V4_BOOTSTRAP", "") == "1"
    and os.getenv("M2_LEDGER_V4_ACTIVE", "") != "1"
):
    target = os.path.join(os.path.dirname(__file__), "m2_ledger_v4_helius.py")
    if not os.path.exists(target):
        raise RuntimeError("M2 Ledger V4 bootstrap target missing: " + target)

    # Prevent an exec loop when V4 itself starts a new Python interpreter/imports modules.
    os.environ["M2_LEDGER_V4_ACTIVE"] = "1"
    os.execv(sys.executable, [sys.executable, target])
