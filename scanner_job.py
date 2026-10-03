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
    get_klines,
    calculate_ema,
    calculate_rsi,

    SCANNER_MAX_NEW_PER_SCAN,
    SCANNER_MAX_OPEN,
    TP1_PCT,
    TP2_PCT,
    TP3_PCT,
    SCANNER_TP1,
    SCANNER_TP2,
    SCANNER_TP3,
)


# =========================================================
# FAST RADAR SETTINGS
# =========================================================

RADAR_MIN_5M_MOVE = 0.60
RADAR_MAX_CANDIDATES = 12

# 24h quote volume minimum
# deliberately kept low so smaller coins can still qualify
RADAR_MIN_24H_QUOTE_VOLUME = 50000

API_DELAY = 1.10


# =========================================================
# DATABASE MIGRATION
# =========================================================

def init_extra_database():

    conn = get_db_connection()
    cur = conn.cursor()

    # Fast-radar snapshots
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS radar_snapshots (
            symbol VARCHAR(50) PRIMARY KEY,

            last_price DOUBLE PRECISION NOT NULL,

            quote_volume_24h DOUBLE PRECISION,

            price_change_24h DOUBLE PRECISION,

            captured_at TIMESTAMPTZ DEFAULT NOW()
        );
        """
    )

    # Record how the trade finally closed
    cur.execute(
        """
        ALTER TABLE scanner_trades
        ADD COLUMN IF NOT EXISTS exit_reason
        VARCHAR(30);
        """
    )

    conn.commit()

    cur.close()
    conn.close()

    print(
        "RADAR / EXIT DATABASE READY",
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

                if mid > 0:

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

        # Ignore snapshots that are too recent
        # or clearly stale.
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

        if (
            item["quote_volume"]
            < RADAR_MIN_24H_QUOTE_VOLUME
        ):

            continue

        # Extremely wide spread is dangerous
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
# DEEP ENTRY ANALYSIS
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
        "| 5m move:",
        f"{radar['radar_move']:.2f}%",
        flush=True
    )

    # 5m
    fast = analyze_fast_5m(
        symbol
    )

    if not fast:

        return None

    time.sleep(
        API_DELAY
    )

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

    # 15m
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

    # 1h
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
# TREND EXIT ANALYSIS
# =========================================================

def should_exit_after_tp2(
    symbol
):

    """
    Only used after TP2 has already been hit.

    We require at least TWO weakness signals
    before closing the remaining 30%.
    """

    candles_5m = get_klines(
        symbol,
        "5m",
        60
    )

    time.sleep(
        API_DELAY
    )

    candles_15m = get_klines(
        symbol,
        "15m",
        60
    )

    if (
        len(candles_5m) < 30
        or len(candles_15m) < 30
    ):

        return (
            False,
            [],
            None
        )

    # Ignore unfinished candle
    c5 = candles_5m[:-1]
    c15 = candles_15m[:-1]

    close5 = [
        x["close"]
        for x in c5
    ]

    close15 = [
        x["close"]
        for x in c15
    ]

    current_price = (
        close5[-1]
    )

    ema9_5 = calculate_ema(
        close5,
        9
    )

    ema20_5 = calculate_ema(
        close5,
        20
    )

    ema20_15 = calculate_ema(
        close15,
        20
    )

    rsi5 = calculate_rsi(
        close5,
        14
    )

    if (
        ema9_5 is None
        or ema20_5 is None
        or ema20_15 is None
        or rsi5 is None
    ):

        return (
            False,
            [],
            current_price
        )

    weakness = []

    # Signal 1:
    # price loses 5m EMA20
    if (
        current_price
        < ema20_5
    ):

        weakness.append(
            "5m跌破EMA20"
        )

    # Signal 2:
    # short EMA turns bearish
    if (
        ema9_5
        < ema20_5
    ):

        weakness.append(
            "5m EMA9<EMA20"
        )

    # Signal 3:
    # 15m structure starts weakening
    if (
        close15[-1]
        < ema20_15
    ):

        weakness.append(
            "15m跌破EMA20"
        )

    # Signal 4:
    # momentum is no longer positive
    momentum_15m = (
        (
            close5[-1]
            - close5[-4]
        )
        / close5[-4]
        * 100
    )

    if momentum_15m < 0:

        weakness.append(
            f"15分鐘動能{momentum_15m:.2f}%"
        )

    # Signal 5:
    # RSI has clearly weakened
    if rsi5 < 48:

        weakness.append(
            f"5m RSI降至{rsi5:.1f}"
        )

    # Need two weakness signals.
    exit_now = (
        len(weakness)
        >= 2
    )

    return (
        exit_now,
        weakness,
        current_price
    )


# =========================================================
# CLOSE LAST 30% ON TREND WEAKNESS
# =========================================================

def manage_tp2_trend_exits():

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            id,
            symbol,
            entry_price

        FROM scanner_trades

        WHERE status = 'OPEN'

        AND strategy
        IN ('TREND', 'BREAKOUT')

        AND tp2_hit = TRUE

        AND tp3_hit = FALSE;
        """
    )

    trades = cur.fetchall()

    cur.close()
    conn.close()

    if not trades:

        print(
            "TP2 TREND MONITOR: "
            "NO POSITIONS",
            flush=True
        )

        return

    print(
        "TP2 TREND MONITOR:",
        len(trades),
        "position(s)",
        flush=True
    )

    for (
        trade_id,
        symbol,
        entry_price
    ) in trades:

        try:

            (
                exit_now,
                weakness,
                current_price
            ) = should_exit_after_tp2(
                symbol
            )

            if (
                not exit_now
                or current_price is None
            ):

                print(
                    "KEEP RUNNING:",
                    symbol,
                    "|",
                    ", ".join(
                        weakness
                    )
                    if weakness
                    else
                    "trend still healthy",
                    flush=True
                )

                continue

            remaining_return_pct = (
                (
                    current_price
                    - entry_price
                )
                / entry_price
                * 100
            )

            # TP1 = 30%
            # TP2 = 40%
            # final remaining 30% exits at market
            result_pct = (
                (
                    TP1_PCT
                    / 100
                )
                * SCANNER_TP1

                +

                (
                    TP2_PCT
                    / 100
                )
                * SCANNER_TP2

                +

                (
                    TP3_PCT
                    / 100
                )
                * remaining_return_pct
            )

            status = (
                "WIN"
                if result_pct > 0
                else "LOSS"
            )

            conn = get_db_connection()
            cur = conn.cursor()

            cur.execute(
                """
                UPDATE scanner_trades

                SET
                    exit_price = %s,

                    result_pct = %s,

                    status = %s,

                    exit_reason =
                        'TREND_EXIT',

                    closed_at =
                        NOW()

                WHERE id = %s

                AND status = 'OPEN';
                """,
                (
                    current_price,
                    result_pct,
                    status,
                    trade_id,
                )
            )

            conn.commit()

            cur.close()
            conn.close()

            print(
                "TREND EXIT:",
                symbol,
                "| Price:",
                current_price,
                "| Final result:",
                f"{result_pct:.2f}%",
                "| Weakness:",
                ", ".join(
                    weakness
                ),
                flush=True
            )

        except Exception as e:

            print(
                "TREND EXIT ERROR:",
                symbol,
                str(e),
                flush=True
            )


# =========================================================
# MAIN 5-MINUTE RADAR
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

    init_extra_database()

    # -----------------------------------------------------
    # 1. Update existing TP / SL results
    # -----------------------------------------------------

    try:

        update_simulated_trades()

    except Exception as e:

        print(
            "TRADE UPDATE ERROR:",
            str(e),
            flush=True
        )

    # -----------------------------------------------------
    # 2. If TP2 has already been reached,
    #    check whether last 30% should exit early
    # -----------------------------------------------------

    try:

        manage_tp2_trend_exits()

    except Exception as e:

        print(
            "TREND EXIT MANAGER ERROR:",
            str(e),
            flush=True
        )

    # -----------------------------------------------------
    # 3. Load previous market snapshot
    # -----------------------------------------------------

    previous = (
        load_previous_snapshots()
    )

    # -----------------------------------------------------
    # 4. Get current whole-market snapshot
    # -----------------------------------------------------

    current = (
        get_all_tickers()
    )

    print(
        "MARKETS RECEIVED:",
        len(current),
        flush=True
    )

    # First run only creates baseline
    if not previous:

        save_snapshots(
            current
        )

        print(
            "FIRST RADAR RUN: "
            "BASELINE CREATED",
            flush=True
        )

        return

    # -----------------------------------------------------
    # 5. Find fast-moving markets
    # -----------------------------------------------------

    candidates = (
        build_radar_candidates(
            current,
            previous
        )
    )

    # Save current prices immediately
    # for next 5-minute comparison
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
            f"5m "
            f"{item['radar_move']:.2f}%",
            "|",
            f"Radar "
            f"{item['radar_score']}",
            flush=True
        )

    # -----------------------------------------------------
    # 6. Deep analyze only radar candidates
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # 7. Create simulation trades
    # -----------------------------------------------------

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

    # -----------------------------------------------------
    # 8. One final result update
    # -----------------------------------------------------

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
