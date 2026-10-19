"""R1 + LINE staging guard. NO API CALLS, NO ORDER PLACEMENT.
Usage: python live_staging_guard.py
Read-only source audit; fails closed on unsafe existing live paths.
"""
from pathlib import Path
import ast
import os
import sys

ROOT = Path(__file__).resolve().parent
ISSUES = []

def issue(message):
    ISSUES.append(message)
    print("BLOCK:", message)

def check_file(name):
    path = ROOT / name
    if not path.exists():
        issue(f"Missing source file: {name}")
        return ""
    source = path.read_text(encoding="utf-8")
    try:
        ast.parse(source, filename=name)
    except SyntaxError as exc:
        issue(f"{name}: syntax error: {exc}")
    return source

def audit():
    print("R1 + LINE LIVE STAGING SOURCE AUDIT — READ ONLY")
    app = check_file("app.py")
    scanner = check_file("scanner_job.py")
    if os.getenv("LIVE_TRADING", "false").strip().lower() == "true":
        issue("LIVE_TRADING=true is prohibited during staging.")
    if app:
        if 'SCANNER_ENGINE_VERSION = "V4.2R1"' not in app:
            issue("R1 strategy identity changed or not found.")
        for setting in ("SCANNER_TP1 = 2.0", "SCANNER_TP2 = 4.0",
                        "SCANNER_TP3 = 8.0", "SCANNER_SL = 2.5"):
            if setting not in app:
                issue(f"R1 core value changed: {setting}")
        if "def register_line_signal" in app and "management_mode = 'LINE_TPSL'" in app:
            issue("LINE signal currently changes open R1 simulation TP/SL; isolate accounting before live.")
        if "if relation == \"OPPOSITE\"" in app:
            issue("LINE opposite signal is skipped; conflicts with independent R1/LINE live pools.")
        if "threading.Thread(" in app and "monitor_limit_order" in app:
            issue("LINE protection depends on in-process thread; recovery after restart is not implemented.")
        if "def place_tp_sl" in app and "for name, qty, trigger_price in checks:" in app:
            issue("LINE TP orders precede stop-loss; unsafe window if TP/SL placement fails.")
        if "ORDER_USDT / entry_price" in app:
            issue("LINE entry sizing uses margin amount as notional; verify leverage/quantity intent.")
    if scanner and "create_simulated_trade(result)" in scanner:
        issue("R1 scanner only creates simulated trades; no dedicated live execution pipeline.")
    print(f"\nRESULT: {'BLOCKED' if ISSUES else 'SOURCE CHECK PASS'} ({len(ISSUES)} issue(s))")
    print("This does not validate BingX exchange behavior, positions, or real orders.")
    return 1 if ISSUES else 0

if __name__ == "__main__":
    sys.exit(audit())
