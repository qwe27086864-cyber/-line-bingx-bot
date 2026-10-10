
"""
V4.3R3 - Simulation Safety & Diagnostics
Stage 1: Read-only diagnostic module.

Does not create, modify, or close trades.
Does not enable live trading.
"""

VERSION = "V4.3R3"


def install_r3(ns):
    if ns.get("_R3_INSTALLED", False):
        return

    if ns.get("LIVE_TRADING", False):
        raise RuntimeError(
            "R3 simulation requires LIVE_TRADING=false"
        )

    required = (
        "get_db_connection",
        "update_simulated_trades",
        "create_simulated_trade",
        "finalize_trade",
    )

    missing = [
        name for name in required
        if not callable(ns.get(name))
    ]

    if missing:
        raise RuntimeError(
            "R3 missing functions: "
            + ", ".join(missing)
        )

    ns["_R3_INSTALLED"] = True
    ns["R3_DIAGNOSTICS_ENABLED"] = True

    print(
        "V4.3R3 DIAGNOSTICS INSTALLED | "
        "READ ONLY | NO TRADING CHANGES",
        flush=True,
    )


def r3_diagnostic_report(get_db_connection):
    """Read-only summary of recorded trade results."""

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    engine_version,
                    side,
                    strategy,
                    COUNT(*) AS closed,
                    COUNT(*) FILTER (
                        WHERE status = 'WIN'
                    ) AS wins,
                    COUNT(*) FILTER (
                        WHERE exit_reason = 'SL_EXIT'
                    ) AS stops,
                    COUNT(*) FILTER (
                        WHERE exit_reason = 'EARLY_WEAK_EXIT'
                    ) AS early_exits,
                    COALESCE(AVG(net_result_pct), 0)
                FROM scanner_trades
                WHERE engine_version IN (
                    'V4.3R1', 'V4.3R2'
                )
                  AND strategy IN ('TREND', 'BREAKOUT')
                  AND status IN ('WIN', 'LOSS')
                GROUP BY
                    engine_version, side, strategy
                ORDER BY
                    engine_version, side, strategy
            """)

            rows = cur.fetchall()

        return [
            {
                "version": r[0],
                "side": r[1],
                "strategy": r[2],
                "closed": int(r[3]),
                "wins": int(r[4]),
                "stops": int(r[5]),
                "early_exits": int(r[6]),
                "average_net_pct": round(float(r[7]), 4),
            }
            for r in rows
        ]

    finally:
        conn.close()
