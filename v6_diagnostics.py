"""V6 research-only trade diagnostics.

No exchange requests, order placement, database writes, or live trading.
Returns are unleveraged percentage returns, net of supplied costs.
"""
from dataclasses import dataclass, asdict
from typing import Optional, Iterable
import json


@dataclass(frozen=True)
class TradeObservation:
    symbol: str
    side: str
    entry_time: str
    entry_price: float
    exit_time: str
    exit_price: float
    regime_entry: str = "unknown"
    regime_exit: str = "unknown"
    exit_reason: str = "unknown"
    shadow_exit_price: Optional[float] = None
    fee_rate_roundtrip: float = 0.0016
    slippage_rate_roundtrip: float = 0.0

    def metrics(self):
        if self.entry_price <= 0 or self.exit_price <= 0:
            raise ValueError("Entry and exit prices must be positive")
        if self.side.upper() not in ("LONG", "SHORT"):
            raise ValueError("side must be LONG or SHORT")
        if self.fee_rate_roundtrip < 0 or self.slippage_rate_roundtrip < 0:
            raise ValueError("Trading costs cannot be negative")
        direction = 1 if self.side.upper() == "LONG" else -1
        cost = self.fee_rate_roundtrip + self.slippage_rate_roundtrip
        net = direction * (self.exit_price / self.entry_price - 1) - cost
        shadow_net = None
        if self.shadow_exit_price is not None:
            if self.shadow_exit_price <= 0:
                raise ValueError("Shadow exit price must be positive")
            shadow_net = direction * (self.shadow_exit_price / self.entry_price - 1) - cost
        return {
            **asdict(self),
            "net_return_pct_unlevered": round(net * 100, 6),
            "shadow_net_return_pct_unlevered": None if shadow_net is None else round(shadow_net * 100, 6),
            "actual_minus_shadow_pct_points": None if shadow_net is None else round((net - shadow_net) * 100, 6),
        }


def summarize(records: Iterable[TradeObservation]):
    rows = [record.metrics() for record in records]
    profits = [row["net_return_pct_unlevered"] for row in rows if row["net_return_pct_unlevered"] > 0]
    losses = [row["net_return_pct_unlevered"] for row in rows if row["net_return_pct_unlevered"] < 0]
    gross_profit = sum(profits)
    gross_loss = -sum(losses)
    return {
        "count": len(rows),
        "win_rate_pct": round(100 * len(profits) / len(rows), 2) if rows else None,
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss else None,
        "profit_factor_note": "undefined: no losing trades" if not gross_loss else None,
        "sum_trade_returns_pct_unlevered": round(sum(row["net_return_pct_unlevered"] for row in rows), 6),
        "trades": rows,
    }


if __name__ == "__main__":
    # Example only; no trading, historical download, or live data access.
    example = TradeObservation(
        symbol="EXAMPLE-USDT", side="LONG", entry_time="2026-10-01T00:00:00Z",
        entry_price=100, exit_time="2026-10-01T01:00:00Z", exit_price=102,
        regime_entry="sideways", regime_exit="uptrend", exit_reason="TP1",
        shadow_exit_price=101,
    )
    print(json.dumps(summarize([example]), indent=2))
