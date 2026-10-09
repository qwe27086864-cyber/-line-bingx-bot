
import time
from datetime import datetime, timezone

import app as trading_app

from app import (
    get_db_connection,
    init_database,
    bingx_public_request,
    valid_scanner_symbol,
    analyze_fast_5m,
    analyze_15m_detail,
    confirm_1h,
    choose_strategy,
    scanner_risk_allows_new_trade,
    create_simulated_trade,
    update_simulated_trades,
    get_all_prices,
    get_klines,
    calculate_ema,
    calculate_rsi,
    side_return_pct,
    finalize_trade,
    SCANNER_ENGINE_VERSION,
    SCANNER_MAX_NEW_PER_SCAN,
    SCANNER_MAX_OPEN,
    SCANNER_COOLDOWN_HOURS,
    TP1_PCT,
    TP2_PCT,
    TP3_PCT,
)

# ==================================================
# V4.3R2 SIMULATION SAFETY
# ==================================================

if trading_app.LIVE_TRADING:
    raise RuntimeError(
        "R2 SIMULATION ONLY: Set LIVE_TRADING=false"
    )

if SCANNER_ENGINE_VERSION != "V4.3R2":
    raise RuntimeError(
        "R2 not installed. Current engine: "
        + SCANNER_ENGINE_VERSION
    )

if not getattr(trading_app, "R2_ENABLED", False):
    raise RuntimeError("R2 manager not enabled")

ENGINE = "V4.3R2"
ACTIVE_ENGINES = ("V4.3R1", "V4.3R2")

# ==================================================
# ORIGINAL R1 RADAR SETTINGS - UNCHANGED
# ==================================================

RADAR_MIN_5M_MOVE = 0.60
RADAR_MAX_CANDIDATES = 20
RADAR_MIN_24H_QUOTE_VOLUME = 50000
RADAR_MAX_SPREAD_PCT = 2.0
API_DELAY = 1.10

TREND_EXIT_WEAKNESS_COUNT = 2

# ==================================================
# DATABASE
# ==================================================

def init_extra_database():
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS
                radar_snapshots (
                    symbol VARCHAR(50) PRIMARY KEY,
                    last_price DOUBLE PRECISION NOT NULL,
                    quote_volume_24h DOUBLE PRECISION,
                    price_change_24h DOUBLE PRECISION,
                    captured_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn.commit()
    finally:
        conn.close()

    print("R2 RADAR DATABASE READY", flush=True)


def active_open_count(side=None):
    """
    R1 + R2 share the same simulation capacity.
    """

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            if side in ("LONG", "SHORT"):
                cur.execute("""
                    SELECT COUNT(*)
                    FROM scanner_trades
                    WHERE status = 'OPEN'
                      AND engine_version IN (
                          'V4.3R1', 'V4.3R2'
                      )
                      AND strategy IN (
                          'TREND', 'BREAKOUT'
                      )
                      AND side = %s
                """, (side,))
            else:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM scanner_trades
                    WHERE status = 'OPEN'
                      AND engine_version IN (
                          'V4.3R1', 'V4.3R2'
                      )
                      AND strategy IN (
                          'TREND', 'BREAKOUT'
                      )
                """)

            return int(cur.fetchone()[0] or 0)
    finally:
        conn.close()


def active_symbol_blocked(symbol):
    """
    Prevent R2 from duplicating open R1 trades.
    Also preserve the original 4-hour cooldown.
    """

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1
                    FROM scanner_trades
                    WHERE symbol = %s
                      AND engine_version IN (
                          'V4.3R1', 'V4.3R2'
                      )
                      AND strategy IN (
                          'TREND', 'BREAKOUT'
                      )
                      AND (
                          status = 'OPEN'
                          OR signal_time >
                              NOW() -
                              (%s * INTERVAL '1 hour')
                      )
                )
            """, (
                symbol,
                SCANNER_COOLDOWN_HOURS,
            ))

            return bool(cur.fetchone()[0])
    finally:
        conn.close()


def combined_capacity_allows():
    """
    R1 and R2 share one 1000U simulation account
    for capacity purposes during transition.
    """

    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COALESCE(
                    SUM(sim_pnl_usdt), 0
                )
                FROM scanner_trades
                WHERE engine_version IN (
                    'V4.3R1', 'V4.3R2'
                )
                  AND status IN ('WIN', 'LOSS')
            """)

            pnl = float(cur.fetchone()[0] or 0)

            cur.execute("""
                SELECT COUNT(*)
                FROM scanner_trades
                WHERE engine_version IN (
                    'V4.3R1', 'V4.3R2'
                )
                  AND status = 'OPEN'
            """)

            opened = int(cur.fetchone()[0] or 0)
    finally:
        conn.close()

    capital = (
        trading_app.SIM_START_BALANCE
        + pnl
        - opened * trading_app.SIM_MARGIN_USDT
    )

    return (
        capital >= trading_app.SIM_MARGIN_USDT,
        capital
    )


# ==================================================
# WHOLE MARKET SNAPSHOT
# ==================================================

def get_all_tickers():
    result = bingx_public_request(
        "/openApi/swap/v2/quote/ticker"
    )

    if result.get("code") != 0:
        raise RuntimeError(
            f"Ticker failed: {result}"
        )

    data = result.get("data", [])

    if isinstance(data, dict):
        data = [data]

    tickers = []

    for item in data:
        try:
            symbol = str(
                item.get("symbol", "")
            ).upper()

            if not valid_scanner_symbol(symbol):
                continue

            price = float(
                item.get("lastPrice", 0) or 0
            )

            volume = float(
                item.get("quoteVolume", 0) or 0
            )

            change = float(
                item.get(
                    "priceChangePercent", 0
                ) or 0
            )

            bid = float(
                item.get("bidPrice", 0) or 0
            )

            ask = float(
                item.get("askPrice", 0) or 0
            )

            if price <= 0:
                continue

            # Missing bid/ask must not be
            # mistaken for a perfect spread.
            if bid > 0 and ask > 0 and ask >= bid:
                mid = (bid + ask) / 2
                spread = (ask - bid) / mid * 100
            else:
                spread = None

            tickers.append({
                "symbol": symbol,
                "price": price,
                "quote_volume": volume,
                "change_24h": change,
                "spread_pct": spread,
            })

        except (TypeError, ValueError, ZeroDivisionError):
            continue

    return tickers


def load_previous_snapshots():
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol,
                       last_price,
                       quote_volume_24h,
                       price_change_24h,
                       captured_at
                FROM radar_snapshots
            """)
            rows = cur.fetchall()
    finally:
        conn.close()

    return {
        symbol: {
            "price": float(price),
            "quote_volume": float(volume or 0),
            "change_24h": float(change or 0),
            "captured_at": captured,
        }
        for symbol, price, volume, change, captured
        in rows
    }


def save_snapshots(tickers):
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            for item in tickers:
                cur.execute("""
                    INSERT INTO radar_snapshots (
                        symbol,
                        last_price,
                        quote_volume_24h,
                        price_change_24h,
                        captured_at
                    )
                    VALUES (%s, %s, %s, %s, NOW())
                    ON CONFLICT(symbol)
                    DO UPDATE SET
                        last_price =
                            EXCLUDED.last_price,
                        quote_volume_24h =
                            EXCLUDED.quote_volume_24h,
                        price_change_24h =
                            EXCLUDED.price_change_24h,
                        captured_at = NOW()
                """, (
                    item["symbol"],
                    item["price"],
                    item["quote_volume"],
                    item["change_24h"],
                ))
        conn.commit()
    finally:
        conn.close()


# ==================================================
# R1 FAST RADAR LOGIC - UNCHANGED
# ==================================================

def build_radar_candidates(current, previous):
    candidates = []
    now = datetime.now(timezone.utc)

    for item in current:
        symbol = item["symbol"]
        old = previous.get(symbol)

        if not old:
            continue

        old_price = old["price"]
        captured_at = old["captured_at"]

        if old_price <= 0 or not captured_at:
            continue

        age = (
            now - captured_at
        ).total_seconds() / 60

        if age < 2 or age > 20:
            continue

        move = (
            (item["price"] - old_price)
            / old_price * 100
        )

        magnitude = abs(move)

        if (
            item["quote_volume"]
            < RADAR_MIN_24H_QUOTE_VOLUME
        ):
            continue

        spread = item["spread_pct"]

        # Skip candidates without verifiable
        # spread data rather than assuming 0%.
        if spread is None:
            continue

        if spread > RADAR_MAX_SPREAD_PCT:
            continue

        if magnitude < RADAR_MIN_5M_MOVE:
            continue

        side = "LONG" if move > 0 else "SHORT"

        score = 0
        reasons = []

        if magnitude >= 0.30:
            score += 10

        if magnitude >= 0.60:
            score += 20
            reasons.append(
                f"Radar move {magnitude:.2f}% {side}"
            )

        if magnitude >= 1.20:
            score += 25
            reasons.append("short-term acceleration")

        if magnitude >= 2.00:
            score += 20

        if side == "LONG":
            if item["change_24h"] > 0:
                score += 5
            if item["change_24h"] >= 5:
                score += 5
        else:
            if item["change_24h"] < 0:
                score += 5
            if item["change_24h"] <= -5:
                score += 5

        if spread <= 0.5:
            score += 5

        candidates.append({
            "symbol": symbol,
            "side": side,
            "radar_move": move,
            "radar_abs_move": magnitude,
            "radar_score": score,
            "radar_price": item["price"],
            "quote_volume": item["quote_volume"],
            "change_24h": item["change_24h"],
            "spread_pct": spread,
            "radar_reasons": reasons,
        })

    candidates.sort(
        key=lambda x: (
            x["radar_score"],
            x["radar_abs_move"],
        ),
        reverse=True,
    )

    # Same R1 candidate limit.
    return candidates[:RADAR_MAX_CANDIDATES]


def deep_analyze(radar):
    symbol = radar["symbol"]
    side = radar["side"]

    fast = analyze_fast_5m(
        symbol,
        side_hint=side
    )

    if not fast:
        return None

    time.sleep(API_DELAY)

    fast["radar_move"] = radar["radar_move"]
    fast["radar_score"] = radar["radar_score"]

    detailed = analyze_15m_detail(fast)

    if not detailed:
        return None

    time.sleep(API_DELAY)

    detailed = confirm_1h(detailed)

    time.sleep(API_DELAY)

    chosen = choose_strategy(detailed)

    if not chosen:
        return None

    chosen["reasons"].insert(
        0,
        f"Radar {radar['radar_move']:+.2f}% "
        f"| {side}"
    )

    print(
        "QUALIFIED DETAIL:",
        symbol,
        side,
        chosen["strategy"],
        "score",
        chosen["score"],
        flush=True
    )

    return chosen


# ==================================================
# R1 BTC / ETH MARKET BIAS - UNCHANGED
# ==================================================

def _market_bias_score(symbol):
    candles = get_klines(symbol, "1h", 90)

    if len(candles) < 60:
        return 0

    closes = [
        x["close"] for x in candles[:-1]
    ]

    last = closes[-1]
    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)

    if None in (ema20, ema50, rsi):
        return 0

    score = 0

    score += 1 if last > ema20 else -1
    score += 1 if ema20 > ema50 else -1

    if rsi >= 55:
        score += 1
    elif rsi <= 45:
        score -= 1

    momentum_4h = (
        (last - closes[-5])
        / closes[-5] * 100
    )

    if momentum_4h >= 1.0:
        score += 1
    elif momentum_4h <= -1.0:
        score -= 1

    return score


def determine_market_quota():
    try:
        btc = _market_bias_score("BTC-USDT")
        time.sleep(API_DELAY)

        eth = _market_bias_score("ETH-USDT")
        time.sleep(API_DELAY)

        bias = btc + eth

        if bias >= 6:
            ratio = 0.80
            regime = "VERY_STRONG_BULL"
        elif bias >= 3:
            ratio = 0.70
            regime = "BULL"
        elif bias >= 1:
            ratio = 0.60
            regime = "MILD_BULL"
        elif bias <= -6:
            ratio = 0.20
            regime = "VERY_STRONG_BEAR"
        elif bias <= -3:
            ratio = 0.30
            regime = "BEAR"
        elif bias <= -1:
            ratio = 0.40
            regime = "MILD_BEAR"
        else:
            ratio = 0.50
            regime = "NEUTRAL"

        long_quota = int(round(
            SCANNER_MAX_OPEN * ratio
        ))

        return {
            "regime": regime,
            "bias_score": bias,
            "btc_score": btc,
            "eth_score": eth,
            "long_quota": long_quota,
            "short_quota":
                SCANNER_MAX_OPEN - long_quota,
        }

    except Exception as exc:
        print(
            "MARKET BIAS ERROR:",
            exc,
            flush=True
        )

        half = SCANNER_MAX_OPEN // 2

        return {
            "regime": "FALLBACK_NEUTRAL",
            "bias_score": 0,
            "btc_score": 0,
            "eth_score": 0,
            "long_quota": half,
            "short_quota": SCANNER_MAX_OPEN - half,
        }


# ==================================================
# TP2 RUNNER: SAME WEAKNESS SIGNALS AS R1
# ==================================================

def should_exit_after_tp2(symbol, side):
    candles5 = get_klines(symbol, "5m", 60)
    time.sleep(API_DELAY)
    candles15 = get_klines(symbol, "15m", 60)

    if len(candles5) < 30 or len(candles15) < 30:
        return False, []

    c5 = candles5[:-1]
    c15 = candles15[:-1]

    p5 = [c["close"] for c in c5]
    p15 = [c["close"] for c in c15]

    last5 = p5[-1]

    ema9 = calculate_ema(p5, 9)
    ema20 = calculate_ema(p5, 20)
    ema20_15 = calculate_ema(p15, 20)
    rsi5 = calculate_rsi(p5, 14)

    if None in (ema9, ema20, ema20_15, rsi5):
        return False, []

    momentum = (
        (p5[-1] - p5[-4])
        / p5[-4] * 100
    )

    weakness = []

    if side == "SHORT":
        if last5 > ema20:
            weakness.append("5m above EMA20")
        if ema9 > ema20:
            weakness.append("5m bullish EMA")
        if p15[-1] > ema20_15:
            weakness.append("15m above EMA20")
        if momentum > 0:
            weakness.append("15m rebound momentum")
        if rsi5 > 52:
            weakness.append("RSI rebound")
    else:
        if last5 < ema20:
            weakness.append("5m below EMA20")
        if ema9 < ema20:
            weakness.append("5m bearish EMA")
        if p15[-1] < ema20_15:
            weakness.append("15m below EMA20")
        if momentum < 0:
            weakness.append("15m falling momentum")
        if rsi5 < 48:
            weakness.append("RSI weakness")

    return (
        len(weakness) >= TREND_EXIT_WEAKNESS_COUNT,
        weakness
    )


def manage_tp2_trend_exits():
    """
    Does not call update_simulated_trades()
    again. Main radar updates positions first.

    Uses observed current price when closing
    a runner. Does not invent a better fill.
    """

    prices = get_all_prices()
    conn = get_db_connection()

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id,
                    symbol,
                    side,
                    engine_version,
                    entry_price,
                    tp1,
                    tp2,
                    highest_price,
                    lowest_price,
                    sim_margin_usdt,
                    sim_leverage
                FROM scanner_trades
                WHERE status = 'OPEN'
                  AND engine_version IN (
                      'V3',
                      'V4.2_PRE',
                      'V4.2R1',
                      'V4.3R1',
                      'V4.3R2'
                  )
                  AND strategy IN (
                      'TREND', 'BREAKOUT'
                  )
                  AND tp2_hit = TRUE
                  AND tp3_hit = FALSE
                ORDER BY id
            """)

            runners = cur.fetchall()

            print(
                "TP2 RUNNERS:",
                len(runners),
                flush=True
            )

            for row in runners:
                (
                    trade_id,
                    symbol,
                    side,
                    version,
                    entry,
                    tp1,
                    tp2,
                    high,
                    low,
                    margin,
                    leverage,
                ) = row

                try:
                    if symbol not in prices:
                        continue

                    side = (side or "LONG").upper()

                    exit_now, weakness = (
                        should_exit_after_tp2(
                            symbol, side
                        )
                    )

                    if not exit_now:
                        continue

                    current = float(prices[symbol])
                    entry = float(entry)
                    tp1 = float(tp1)
                    tp2 = float(tp2)

                    if min(
                        current, entry, tp1, tp2
                    ) <= 0:
                        continue

                    # Use observed current price.
                    # No optimistic TP1 fill.
                    runner_return = side_return_pct(
                        side,
                        entry,
                        current
                    )

                    gross = (
                        TP1_PCT / 100
                        * side_return_pct(
                            side, entry, tp1
                        )
                        +
                        TP2_PCT / 100
                        * side_return_pct(
                            side, entry, tp2
                        )
                        +
                        TP3_PCT / 100
                        * runner_return
                    )

                    high = max(
                        float(high or entry),
                        current
                    )

                    low = min(
                        float(low or entry),
                        current
                    )

                    finalize_trade(
                        cur,
                        trade_id,
                        current,
                        gross,
                        "TREND_EXIT",
                        True,
                        True,
                        False,
                        False,
                        high,
                        low,
                        margin,
                        leverage,
                        "Observed-price runner exit | "
                        + ", ".join(weakness)
                    )

                    conn.commit()

                    print(
                        "TP2 TREND EXIT:",
                        version,
                        symbol,
                        side,
                        current,
                        flush=True
                    )

                except Exception as exc:
                    conn.rollback()

                    print(
                        "TP2 TREND ERROR:",
                        trade_id,
                        str(exc),
                        flush=True
                    )
    finally:
        conn.close()


# ==================================================
# R2 MAIN RADAR
# ==================================================

def run_fast_radar():
    print("=" * 50, flush=True)
    print(
        "BINGX V4.3R2 RADAR START",
        flush=True
    )
    print(
        "SIMULATION ONLY | CORE R1 ENTRY",
        flush=True
    )
    print("=" * 50, flush=True)

    init_database()
    init_extra_database()

    # 1. Update old R1 and new R2 trades.
    # The app-installed R2 manager is used.
    try:
        update_simulated_trades()
    except Exception as exc:
        print(
            "TRADE UPDATE FATAL:",
            exc,
            flush=True
        )
        raise

    # 2. Manage TP2 remaining runners.
    try:
        manage_tp2_trend_exits()
    except Exception as exc:
        print(
            "RUNNER MANAGER ERROR:",
            exc,
            flush=True
        )

    # 3. Existing R2 risk gate.
    allow_new, risk_reason = (
        scanner_risk_allows_new_trade()
    )

    print(
        "R2 RISK GATE:",
        allow_new,
        risk_reason,
        flush=True
    )

    # 4. Check shared R1/R2 capital.
    shared_ok, shared_free = (
        combined_capacity_allows()
    )

    print(
        "SHARED FREE SIM BALANCE:",
        f"{shared_free:.2f}U",
        flush=True
    )

    # 5. Original BTC/ETH market quota.
    quota = determine_market_quota()

    print(
        "MARKET REGIME:",
        quota["regime"],
        "| BTC",
        quota["btc_score"],
        "| ETH",
        quota["eth_score"],
        "| LONG",
        quota["long_quota"],
        "| SHORT",
        quota["short_quota"],
        flush=True
    )

    # 6. Market snapshot.
    previous = load_previous_snapshots()
    current = get_all_tickers()

    print(
        "MARKETS RECEIVED:",
        len(current),
        flush=True
    )

    if not previous:
        save_snapshots(current)
        print(
            "RADAR BASELINE CREATED",
            flush=True
        )
        return

    candidates = build_radar_candidates(
        current, previous
    )

    save_snapshots(current)

    print(
        "FAST RADAR CANDIDATES:",
        len(candidates),
        flush=True
    )

    radar_longs = sum(
        c["side"] == "LONG" for c in candidates
    )

    radar_shorts = sum(
        c["side"] == "SHORT" for c in candidates
    )

    print(
        "RADAR DIRECTION:",
        radar_longs,
        "LONG /",
        radar_shorts,
        "SHORT",
        flush=True
    )

    for item in candidates:
        print(
            item["symbol"],
            item["side"],
            f"{item['radar_move']:+.2f}%",
            "score",
            item["radar_score"],
            flush=True
        )

    # Do not open trades when risk blocks.
    if not allow_new or not shared_ok:
        print(
            "NEW ENTRY BLOCKED:",
            risk_reason,
            "| shared capital",
            shared_free,
            flush=True
        )
        return

    # 7. Deep analysis: identical core.
    qualified = []

    for radar in candidates:
        try:
            result = deep_analyze(radar)

            if result:
                qualified.append(result)

        except Exception as exc:
            print(
                "DEEP ANALYSIS ERROR:",
                radar["symbol"],
                str(exc),
                flush=True
            )

    # 8. Original sorting.
    qualified.sort(
        key=lambda x: (
            x["score"],
            1 if x["strategy"] == "BREAKOUT"
            else 0,
            abs(x.get("radar_move", 0)),
        ),
        reverse=True,
    )

    print(
        "QUALIFIED:",
        len(qualified),
        flush=True
    )

    # 9. Create R2 simulation trades.
    created = 0

    for result in qualified:

        if created >= SCANNER_MAX_NEW_PER_SCAN:
            break

        # Combined R1 + R2 position limit.
        total_open = active_open_count()

        if total_open >= SCANNER_MAX_OPEN:
            print(
                "COMBINED MAX OPEN:",
                total_open,
                flush=True
            )
            break

        side = result["side"]
        side_open = active_open_count(side)

        side_limit = (
            quota["long_quota"]
            if side == "LONG"
            else quota["short_quota"]
        )

        if side_open >= side_limit:
            print(
                "SIDE CEILING:",
                side,
                f"{side_open}/{side_limit}",
                flush=True
            )
            continue

        # Existing daily loss / streak rules.
        risk_ok, risk_reason = (
            scanner_risk_allows_new_trade()
        )

        if not risk_ok:
            print(
                "RISK STOP:",
                risk_reason,
                flush=True
            )
            break

        capital_ok, free = (
            combined_capacity_allows()
        )

        if not capital_ok:
            print(
                "CAPITAL STOP:",
                f"{free:.2f}U",
                flush=True
            )
            break

        # R1 and R2 share symbol cooldown.
        if active_symbol_blocked(
            result["symbol"]
        ):
            print(
                "CROSS-VERSION COOLDOWN:",
                result["symbol"],
                flush=True
            )
            continue

        trade_id = create_simulated_trade(
            result
        )

        created += 1

        print(
            "NEW R2 SIM TRADE:",
            trade_id,
            result["symbol"],
            side,
            result["strategy"],
            result["score"],
            flush=True
        )

    print(
        "NEW V4.3R2 SIMULATED TRADES:",
        created,
        flush=True
    )

    # 10. Update again after new entries.
    if created:
        try:
            update_simulated_trades()
        except Exception as exc:
            print(
                "POST UPDATE ERROR:",
                exc,
                flush=True
            )
            raise

    print("=" * 50, flush=True)
    print(
        "BINGX V4.3R2 RADAR COMPLETE",
        flush=True
    )
    print("=" * 50, flush=True)


# ==================================================
# RUN
# ==================================================

if __name__ == "__main__":
    start = time.time()

    try:
        run_fast_radar()

    except Exception as exc:
        print(
            "R2 RADAR FATAL:",
            str(exc),
            flush=True
        )
        raise

    finally:
        print(
            "TOTAL RUN TIME:",
            f"{time.time() - start:.2f}s",
            flush=True
        )
