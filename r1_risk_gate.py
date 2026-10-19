"""R1 staging risk precheck; does not place orders or authorize live trading."""
from decimal import Decimal, InvalidOperation

def check(*, enabled=False, kill_switch=True, ledger_ok=False,
          reconciled=False, stops_verified=False, unique=False,
          daily_pnl=None, open_r1=None, margin=None, leverage=None):
    if enabled is not True or kill_switch is not False:
        return False, "TRADING_DISABLED"
    if not all(x is True for x in (ledger_ok, reconciled, stops_verified, unique)):
        return False, "SAFETY_NOT_VERIFIED"
    try:
        pnl = Decimal(str(daily_pnl))
        m = Decimal(str(margin))
        lev = Decimal(str(leverage))
        if not all(x.is_finite() for x in (pnl, m, lev)):
            raise ValueError("nonfinite")
        if type(open_r1) is not int or open_r1 < 0:
            raise ValueError("position count")
    except (ValueError, InvalidOperation, TypeError):
        return False, "INVALID_INPUT"
    if pnl <= -10:
        return False, "DAILY_LOSS_LIMIT"
    if open_r1 >= 5:
        return False, "POSITION_LIMIT"
    if not (0 < m <= 10 and 0 < lev <= 8):
        return False, "SIZE_LIMIT"
    return True, "PRECHECK_ONLY"

def self_test():
    valid=dict(enabled=True,kill_switch=False,ledger_ok=True,reconciled=True,
               stops_verified=True,unique=True,daily_pnl=0,open_r1=0,margin=10,leverage=8)
    assert check(**valid)[0]
    for patch in (dict(enabled=False),dict(kill_switch=True),dict(ledger_ok=False),
                  dict(reconciled=False),dict(stops_verified=False),dict(unique=False),
                  dict(daily_pnl=-10),dict(daily_pnl="NaN"),dict(open_r1=5),
                  dict(open_r1=-1),dict(margin=11),dict(leverage=9)):
        assert not check(**{**valid,**patch})[0], patch
    print("PASS: 12 fail-closed risk gate cases")
    print("NOT LIVE READY: no DB locking, real PnL or exchange stop enforcement")

if __name__ == "__main__":
    self_test()
