import time
from datetime import datetime, timezone

from app import (
    get_db_connection,
    init_database,
    bingx_public_request,
    valid_scanner_symbol,
    analyze_fast_5m,
    analyze_15m_detail,
    confirm_1h,
    choose_strategy,
    scanner_open_count,
    scanner_can_open,
    create_simulated_trade,
    update_simulated_trades,

    SCANNER_MAX_NEW_PER_SCAN,
    SCANNER_MAX_OPEN,
)


# =========================================================
# FAST RADAR SETTINGS
# =========================================================

RADAR_MIN_5M_MOVE = 0.60
RADAR_STRONG_5M_MOVE = 1.20

RADAR_MAX_CANDIDATES = 12

RADAR_MIN_24H_QUOTE_VOLUME = 50000

API_DELAY = 1.10


# =========================================================
# RADAR DATABASE
# =========================================================

def init_radar_database():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS radar_snapshots (
            symbol VARCHAR(50) PRIMARY KEY,

            last_price DOUBLE PRECISION NOT NULL,

            quote_volume_24h DOUBLE PRECISION,

            price_change_24h DOUBLE PRECISION,

            captured_at TIMESTAMPTZ
            DEFAULT NOW()
        );
        """
    )

    conn.commit()

    cur.close()
    conn.close()

    print(
        "RADAR DATABASE READY",
        flush=True
    )


# =========================================================
# GET ALL MARKET TICKERS
# =========================================================

def get_all_tickers():

    result = bingx_public_request(
        "/openApi/swap/v2/quote/ticker"
    )

    if result.get("code") != 0:

        raise RuntimeError(
            f"Ticker failed: {result}"
        )

    data = result.get(
        "data",
        []
    )

    if isinstance(
        data,
        dict
    ):

        data = [data]

    tickers = []

    for item in data:

        try:

            symbol = str(
                item.get(
                    "symbol",
                    ""
                )
            ).upper()

            if not valid_scanner_symbol(
                symbol
            ):

                continue

            last_price = float(
                item.get(
                    "lastPrice",
                    0
                )
                or 0
            )

            quote_volume = float(
                item.get(
                    "quoteVolume",
                    0
                )
                or 0
            )

            change_24h = float(
                item.get(
                    "priceChangePercent",
                    0
                )
                or 0
            )

            bid = float(
                item.get(
                    "bidPrice",
                    0
                )
                or 0
            )

            ask = float(
                item.get(
                    "askPrice",
                    0
                )
                or 0
            )

            if last_price <= 0:

                continue

            spread_pct = 0

            if (
                bid > 0
                and ask > 0
            ):

                mid = (
                    bid + ask
                ) / 2

                spread_pct = (
                    (
                        ask - bid
                    )
                    / mid
                    * 100
                )

            tickers.append(
                {
                    "symbol":
                        symbol,

                    "price":
                        last_price,

                    "quote_volume":
                        quote_volume,

                    "change_24h":
                        change_24h,

                    "spread_pct":
                        spread_pct,
                }
            )

        except Exception:

            continue

    return tickers


# =========================================================
# LOAD PREVIOUS SNAPSHOTS
# =========================================================

def load_previous_snapshots():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            symbol,
            last_price,
            quote_volume_24h,
            price_change_24h,
            captured_at

        FROM radar_snapshots;
        """
    )

    rows = cur.fetchall()

    cur.close()
    conn.close()

    result = {}

    for row in rows:

        (
            symbol,
            price,
            volume,
            change_24h,
            captured_at
        ) = row

        result[
            symbol
        ] = {
            "price":
                float(price),

            "quote_volume":
                float(
                    volume or 0
                ),

            "change_24h":
                float(
                    change_24h or 0
                ),

            "captured_at":
                captured_at,
        }

    return result


# =========================================================
# SAVE SNAPSHOTS
# =========================================================

def save_snapshots(
    tickers
):

    conn = get_db_connection()
    cur = conn.cursor()

    for item in tickers:

        cur.execute(
            """
            INSERT INTO radar_snapshots (
                symbol,
                last_price,
                quote_volume_24h,
                price_change_24h,
                captured_at
            )

            VALUES (
                %s,
                %s,
                %s,
                %s,
                NOW()
            )

            ON CONFLICT (symbol)

            DO UPDATE SET

                last_price =
                    EXCLUDED.last_price,

                quote_volume_24h =
                    EXCLUDED.quote_volume_24h,

                price_change_24h =
                    EXCLUDED.price_change_24h,

                captured_at =
                    NOW();
            """,
            (
                item["symbol"],
                item["price"],
                item[
                    "quote_volume"
                ],
                item[
                    "change_24h"
                ],
            )
        )

    conn.commit()

    cur.close()
    conn.close()


# =========================================================
# FAST MARKET RADAR
# =========================================================

def build_radar_candidates(
    current,
    previous
):

    candidates = []

    now = datetime.now(
        timezone.utc
    )

    for item in current:

        symbol = item[
            "symbol"
        ]

        old = previous.get(
            symbol
        )

        if not old:

            continue

        old_price = old[
            "price"
        ]

        if old_price <= 0:

            continue

        captured_at = old[
            "captured_at"
        ]

        if not captured_at:

            continue

        age_minutes = (
            now
            - captured_at
        ).total_seconds() / 60

        # Previous snapshot should roughly
        # represent the prior 5-minute run.
        if age_minutes < 2:

            continue

        if age_minutes > 20:

            continue

        move_pct = (
            (
                item["price"]
                - old_price
            )
            / old_price
            * 100
        )

        # Avoid extremely illiquid markets.
        # Kept deliberately low so small coins
        # are not automatically excluded.
        if (
            item["quote_volume"]
            < RADAR_MIN_24H_QUOTE_VOLUME
        ):

            continue

        # Very large spreads are dangerous
        # even in simulation.
        if (
            item["spread_pct"]
            > 2.0
        ):

            continue

        radar_score = 0

        reasons = []

        if move_pct >= 0.30:

            radar_score += 10

        if move_pct >= 0.60:

            radar_score += 20

            reasons.append(
                f"5分鐘+{move_pct:.2f}%"
            )

        if move_pct >= 1.20:

            radar_score += 25

            reasons.append(
                "短線快速加速"
            )

        if move_pct >= 2.00:

            radar_score += 20

        if (
            item["change_24h"]
            > 0
        ):

            radar_score += 5

        if (
            item["change_24h"]
            >= 5
        ):

            radar_score += 5

        if (
            item["spread_pct"]
            <= 0.5
        ):

            radar_score += 5

        if (
            move_pct
            >= RADAR_MIN_5M_MOVE
        ):

            candidates.append(
                {
                    "symbol":
                        symbol,

                    "radar_move":
                        move_pct,

                    "radar_score":
                        radar_score,

                    "radar_price":
                        item["price"],

                    "quote_volume":
                        item[
                            "quote_volume"
                        ],

                    "change_24h":
                        item[
                            "change_24h"
                        ],

                    "spread_pct":
                        item[
                            "spread_pct"
                        ],

                    "radar_reasons":
                        reasons,
                }
            )

    candidates.sort(
        key=lambda x: (
            x["radar_score"],
            x["radar_move"]
        ),
        reverse=True
    )

    return candidates[
        :RADAR_MAX_CANDIDATES
    ]


# =========================================================
# DEEP ANALYSIS
# =========================================================

def deep_analyze(
    radar
):

    symbol = radar[
        "symbol"
    ]

    print(
        "DEEP ANALYSIS:",
        symbol,
        "5m move:",
        f"{radar['radar_move']:.2f}%",
        flush=True
    )

    # 5m Kline
    fast = analyze_fast_5m(
        symbol
    )

    if not fast:

        return None

    time.sleep(
        API_DELAY
    )

    # Add radar information
    fast[
        "radar_move"
    ] = radar[
        "radar_move"
    ]

    fast[
        "radar_score"
    ] = radar[
        "radar_score"
    ]

    # 15m analysis
    detailed = (
        analyze_15m_detail(
            fast
        )
    )

    if not detailed:

        return None

    time.sleep(
        API_DELAY
    )

    # 1h confirmation
    detailed = confirm_1h(
        detailed
    )

    time.sleep(
        API_DELAY
    )

    chosen = choose_strategy(
        detailed
    )

    if not chosen:

        print(
            "REJECTED AFTER DEEP ANALYSIS:",
            symbol,
            flush=True
        )

        return None

    chosen[
        "reasons"
    ].insert(
        0,
        f"雷達5m+"
        f"{radar['radar_move']:.2f}%"
    )

    return chosen


# =========================================================
# MAIN SCAN
# =========================================================

def run_fast_radar():

    print(
        "========================================",
        flush=True
    )

    print(
        "BINGX 5-MINUTE RADAR START",
        flush=True
    )

    print(
        "========================================",
        flush=True
    )

    init_database()

    init_radar_database()

    # Update existing simulated trades first.
    try:

        update_simulated_trades()

    except Exception as e:

        print(
            "TRADE UPDATE ERROR:",
            str(e),
            flush=True
        )

    previous = (
        load_previous_snapshots()
    )

    current = (
        get_all_tickers()
    )

    print(
        "MARKETS RECEIVED:",
        len(current),
        flush=True
    )

    # First run only creates the baseline.
    if not previous:

        save_snapshots(
            current
        )

        print(
            "FIRST RADAR RUN:"
            " BASELINE CREATED",
            flush=True
        )

        return

    candidates = (
        build_radar_candidates(
            current,
            previous
        )
    )

    # Always save newest snapshot
    # before deep analysis.
    save_snapshots(
        current
    )

    print(
        "FAST RADAR CANDIDATES:",
        len(candidates),
        flush=True
    )

    for item in candidates:

        print(
            item["symbol"],
            "|",
            f"5m {item['radar_move']:.2f}%",
            "|",
            f"Radar {item['radar_score']}",
            flush=True
        )

    qualified = []

    for radar in candidates:

        try:

            result = deep_analyze(
                radar
            )

            if result:

                qualified.append(
                    result
                )

        except Exception as e:

            print(
                "DEEP ANALYSIS ERROR:",
                radar["symbol"],
                str(e),
                flush=True
            )

    qualified.sort(
        key=lambda x:
            x["score"],
        reverse=True
    )

    created = 0

    for result in qualified:

        if (
            created
            >= SCANNER_MAX_NEW_PER_SCAN
        ):

            break

        if (
            scanner_open_count()
            >= SCANNER_MAX_OPEN
        ):

            print(
                "MAX OPEN TRADES REACHED",
                flush=True
            )

            break

        if not scanner_can_open(
            result["symbol"]
        ):

            print(
                "COOLDOWN:",
                result["symbol"],
                flush=True
            )

            continue

        create_simulated_trade(
            result
        )

        created += 1

    print(
        "QUALIFIED:",
        len(qualified),
        flush=True
    )

    print(
        "NEW SIMULATED TRADES:",
        created,
        flush=True
    )

    try:

        update_simulated_trades()

    except Exception as e:

        print(
            "POST UPDATE ERROR:",
            str(e),
            flush=True
        )

    print(
        "========================================",
        flush=True
    )

    print(
        "BINGX 5-MINUTE RADAR COMPLETE",
        flush=True
    )

    print(
        "========================================",
        flush=True
    )


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    start = time.time()

    try:

        run_fast_radar()

    except Exception as e:

        print(
            "RADAR FATAL ERROR:",
            str(e),
            flush=True
        )

        raise

    finally:

        elapsed = (
            time.time()
            - start
        )

        print(
            f"TOTAL RUN TIME: "
            f"{elapsed:.2f} seconds",
            flush=True
        )
