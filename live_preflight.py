"""R1 + LINE live staging preflight. READ-ONLY: never sends orders."""
import os
import sys

def check():
    problems = []
    warnings = []
    def required(name):
        if not os.getenv(name):
            problems.append(f"Missing {name}")
    required("DATABASE_URL")
    required("BINGX_API_KEY")
    required("BINGX_SECRET_KEY")
    if os.getenv("LIVE_TRADING", "false").lower() == "true":
        problems.append("LIVE_TRADING=true: staging preflight requires false")
    if os.getenv("BINGX_POSITION_MODE", "HEDGE").upper() != "HEDGE":
        problems.append("BINGX_POSITION_MODE must be HEDGE")
    try:
        leverage = int(os.getenv("LEVERAGE", "8"))
        margin = float(os.getenv("ORDER_USDT", "10"))
        ceiling = float(os.getenv("MAX_LIVE_ORDER_USDT", "10"))
        if leverage != 8:
            warnings.append(f"Leverage is {leverage}, expected 8")
        if margin <= 0 or margin > 10 or margin > ceiling:
            problems.append("ORDER_USDT invalid or exceeds 10 USDT cap")
    except ValueError:
        problems.append("Invalid numeric leverage or order limit")
    print("R1 + LINE STAGING PREFLIGHT — NO ORDERS")
    for item in problems:
        print("BLOCK:", item)
    for item in warnings:
        print("WARN:", item)
    print("PASS: configuration only" if not problems else "FAIL: do not enable live trading")
    print("NOT CHECKED: exchange fills, protective stops, idempotency, R1 live execution, account positions.")
    return 1 if problems else 0

if __name__ == "__main__":
    sys.exit(check())
