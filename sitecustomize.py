# sitecustomize.py
# Safe bootstrap for M2 Ledger V4.
# This repo is put on PYTHONPATH only for the Ledger service.
# Render also imports sitecustomize during build, so bootstrap ONLY when
# the service's existing runtime command is `python -` (sys.argv[0] == "-").

import os
import sys

if (
    sys.argv
    and sys.argv[0] == "-"
    and os.getenv("M2_LEDGER_V4_BOOTSTRAP", "") == "1"
    and os.getenv("M2_LEDGER_V4_ACTIVE", "") != "1"
):
    target = os.path.join(os.path.dirname(__file__), "m2_ledger_v4_helius.py")
    if not os.path.exists(target):
        raise RuntimeError("M2 Ledger V4 bootstrap target missing: " + target)

    os.environ["M2_LEDGER_V4_ACTIVE"] = "1"
    os.execv(sys.executable, [sys.executable, target])
