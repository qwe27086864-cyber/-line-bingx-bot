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
    TP1_PCT,
    TP2_PCT,
    TP3_PCT,
    SCANNER_TP1,
    SCANNER_TP2,
    SCANNER_TP3,
)

# =========================================================
# V4.2 FAST RADAR SETTINGS
# =========================================================

RADAR_MIN_5M_MOVE = 0.60
RADAR_MAX_CANDIDATES = 20
RADAR_MIN_24H_QUOTE_VOLUME = 50000
RADAR_MAX_SPREAD_PCT = 2.0
API_DELAY = 1.10

# TP2 runner trend-exit requires this many weakness signals.
TREND_EXIT_WEAKNESS_COUNT = 2


# =========================================================
# DATABASE
# =========================================================

def init_extra_database():
    conn = get_db_connection()
    cur = conn.cursor()

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

    conn.commit()
    cur.close()
    conn.close()

    print("V4.2 LOCKED TEST RADAR DATABASE READY", flush=True)


# =========================================================
# WHOLE MARKET SNAPSHOT
# =========================================================

def get_all_tickers():
    result = bingx_public_request("/openApi/swap/v2/quote/ticker")

    if result.get("code") != 0:
        raise RuntimeError(f"Ticker failed: {result}")

    data = result.get("data", [])
    if isinstance(data, dict):
        data = [data]

    tickers = []

    for item in data:
        try:
            symbol = str(item.get("symbol", "")).upper()

            if not valid_scanner_symbol(symbol):
                continue

            last_price = float(item.get("lastPrice", 0) or 0)
            quote_volume = float(item.get("quoteVolume", 0) or 0)
            change_24h = float(item.get("priceChangePercent", 0) or 0)
            bid = float(item.get("bidPrice", 0) or 0)
            ask = float(item.get("askPrice", 0) or 0)

            if last_price <= 0:
                continue

            spread_pct = 0.0

            if bid > 0 and ask > 0:
                mid = (bid + ask) / 2
                if mid > 0:
                    spread_pct = (ask - bid) / mid * 100

            tickers.append({
                "symbol": symbol,
                "price": last_price,
                "quote_volume": quote_volume,
                "change_24h": change_24h,
                "spread_pct": spread_pct,
            })

        except Exception:
            continue

    return tickers


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

    for symbol, price, volume, change_24h, captured_at in rows:
        result[symbol] = {
            "price": float(price),
            "quote_volume": float(volume or 0),
            "change_24h": float(change_24h or 0),
            "captured_at": captured_at,
        }

    return result


def save_snapshots(tickers):
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
            VALUES (%s,%s,%s,%s,NOW())
            ON CONFLICT (symbol)
            DO UPDATE SET
                last_price=EXCLUDED.last_price,
                quote_volume_24h=EXCLUDED.quote_volume_24h,
                price_change_24h=EXCLUDED.price_change_24h,
                captured_at=NOW();
            """,
            (
                item["symbol"],
                item["price"],
                item["quote_volume"],
                item["change_24h"],
            ),
        )

    conn.commit()
    cur.close()
    conn.close()


# =========================================================
# V4.2 LONG / SHORT FAST RADAR
# =========================================================

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

        age_minutes = (now - captured_at).total_seconds() / 60

        # Cron normally runs every 5m, but allow delayed runs.
        if age_minutes < 2 or age_minutes > 20:
            continue

        move_pct = (item["price"] - old_price) / old_price * 100
        abs_move = abs(move_pct)

        if item["quote_volume"] < RADAR_MIN_24H_QUOTE_VOLUME:
            continue

        if item["spread_pct"] > RADAR_MAX_SPREAD_PCT:
            continue

        if abs_move < RADAR_MIN_5M_MOVE:
            continue

        side = "LONG" if move_pct > 0 else "SHORT"

        radar_score = 0
        reasons = []

        if abs_move >= 0.30:
            radar_score += 10

        if abs_move >= 0.60:
            radar_score += 20
            reasons.append(
                f"5åé{'ä¸æ¼²' if side == 'LONG' else 'ä¸è·'} {abs_move:.2f}%"
            )

        if abs_move >= 1.20:
            radar_score += 25
            reasons.append("ç­ç·å¿«éå é")

        if abs_move >= 2.00:
            radar_score += 20

        # 24h direction agrees with radar direction.
        if side == "LONG":
            if item["change_24h"] > 0:
                radar_score += 5
            if item["change_24h"] >= 5:
                radar_score += 5
        else:
            if item["change_24h"] < 0:
                radar_score += 5
            if item["change_24h"] <= -5:
                radar_score += 5

        if item["spread_pct"] <= 0.5:
            radar_score += 5

        candidates.append({
            "symbol": symbol,
            "side": side,
            "radar_move": move_pct,
            "radar_abs_move": abs_move,
            "radar_score": radar_score,
            "radar_price": item["price"],
            "quote_volume": item["quote_volume"],
            "change_24h": item["change_24h"],
            "spread_pct": item["spread_pct"],
            "radar_reasons": reasons,
        })

    candidates.sort(
        key=lambda x: (x["radar_score"], x["radar_abs_move"]),
        reverse=True,
    )

    return candidates[:RADAR_MAX_CANDIDATES]


def deep_analyze(radar):
    symbol = radar["symbol"]
    side = radar["side"]

    print(
        "DEEP ANALYSIS:",
        symbol,
        "|",
        side,
        "| radar move",
        f"{radar['radar_move']:+.2f}%",
        flush=True,
    )

    # Lock analysis to the radar direction.
    fast = analyze_fast_5m(symbol, side_hint=side)

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
        print("REJECTED:", symbol, side, flush=True)
        return None

    chosen["reasons"].insert(
        0,
        f"é·é5m {radar['radar_move']:+.2f}% | {side}"
    )

    return chosen


# =========================================================
# V4.2 DYNAMIC MARKET BIAS / 20-SLOT QUOTA
# =========================================================

def _market_bias_score(symbol):
    """
    Returns roughly -4..+4 for one major coin.
    Uses completed 1h candles only.
    """
    candles = get_klines(symbol, "1h", 90)
    if len(candles) < 60:
        return 0

    completed = candles[:-1]
    closes = [x["close"] for x in completed]

    last = closes[-1]
    ema20 = calculate_ema(closes, 20)
    ema50 = calculate_ema(closes, 50)
    rsi = calculate_rsi(closes, 14)

    if ema20 is None or ema50 is None or rsi is None:
        return 0

    score = 0

    score += 1 if last > ema20 else -1
    score += 1 if ema20 > ema50 else -1

    if rsi >= 55:
        score += 1
    elif rsi <= 45:
        score -= 1

    momentum_4h = (last - closes[-5]) / closes[-5] * 100

    if momentum_4h >= 1.0:
        score += 1
    elif momentum_4h <= -1.0:
        score -= 1

    return score


def determine_market_quota():
    """
    Scanner has 20 total slots, but LONG/SHORT allocation is dynamic.

    Examples:
      very strong bull -> 16 / 4
      bull             -> 14 / 6
      mild bull        -> 12 / 8
      neutral          -> 10 / 10
      mild bear        -> 8 / 12
      bear             -> 6 / 14
      very strong bear -> 4 / 16
    """
    try:
        btc = _market_bias_score("BTC-USDT")
        time.sleep(API_DELAY)
        eth = _market_bias_score("ETH-USDT")
        time.sleep(API_DELAY)

        bias = btc + eth

        if bias >= 6:
            long_quota, short_quota, regime = 16, 4, "VERY_STRONG_BULL"
        elif bias >= 3:
            long_quota, short_quota, regime = 14, 6, "BULL"
        elif bias >= 1:
            long_quota, short_quota, regime = 12, 8, "MILD_BULL"
        elif bias <= -6:
            long_quota, short_quota, regime = 4, 16, "VERY_STRONG_BEAR"
        elif bias <= -3:
            long_quota, short_quota, regime = 6, 14, "BEAR"
        elif bias <= -1:
            long_quota, short_quota, regime = 8, 12, "MILD_BEAR"
        else:
            long_quota, short_quota, regime = 10, 10, "NEUTRAL"

        # Respect SCANNER_MAX_OPEN even if user changes it from 20 later.
        total = long_quota + short_quota
        if total != SCANNER_MAX_OPEN and total > 0:
            long_quota = int(round(SCANNER_MAX_OPEN * long_quota / total))
            short_quota = SCANNER_MAX_OPEN - long_quota

        return {
            "regime": regime,
            "bias_score": bias,
            "btc_score": btc,
            "eth_score": eth,
            "long_quota": long_quota,
            "short_quota": short_quota,
        }

    except Exception as e:
        print("MARKET BIAS ERROR:", str(e), flush=True)

        half = SCANNER_MAX_OPEN // 2
        return {
            "regime": "FALLBACK_NEUTRAL",
            "bias_score": 0,
            "btc_score": 0,
            "eth_score": 0,
            "long_quota": half,
            "short_quota": SCANNER_MAX_OPEN - half,
        }


# =========================================================
# TP2 RUNNER TREND EXIT - LONG / SHORT
# =========================================================

def should_exit_after_tp2(symbol, side):
    candles_5m = get_klines(symbol, "5m", 60)
    time.sleep(API_DELAY)

    candles_15m = get_klines(symbol, "15m", 60)

    if len(candles_5m) < 30 or len(candles_15m) < 30:
        return False, []

    c5 = candles_5m[:-1]
    c15 = candles_15m[:-1]

    close5 = [x["close"] for x in c5]
    close15 = [x["close"] for x in c15]

    last5 = close5[-1]
    ema9_5 = calculate_ema(close5, 9)
    ema20_5 = calculate_ema(close5, 20)
    ema20_15 = calculate_ema(close15, 20)
    rsi5 = calculate_rsi(close5, 14)

    if None in (ema9_5, ema20_5, ema20_15, rsi5):
        return False, []

    momentum_15m = (close5[-1] - close5[-4]) / close5[-4] * 100
    weakness = []

    if side == "SHORT":
        if last5 > ema20_5:
            weakness.append("5mç«åEMA20")

        if ema9_5 > ema20_5:
            weakness.append("5m EMA9>EMA20")

        if close15[-1] > ema20_15:
            weakness.append("15mç«åEMA20")

        if momentum_15m > 0:
            weakness.append(f"15åéåå½åè½ +{momentum_15m:.2f}%")

        if rsi5 > 52:
            weakness.append(f"5m RSIååè³ {rsi5:.1f}")

    else:
        if last5 < ema20_5:
            weakness.append("5mè·ç ´EMA20")

        if ema9_5 < ema20_5:
            weakness.append("5m EMA9<EMA20")

        if close15[-1] < ema20_15:
            weakness.append("15mè·ç ´EMA20")

        if momentum_15m < 0:
            weakness.append(f"15åéåè½ {momentum_15m:.2f}%")

        if rsi5 < 48:
            weakness.append(f"5m RSIéè³ {rsi5:.1f}")

    return len(weakness) >= TREND_EXIT_WEAKNESS_COUNT, weakness


def manage_tp2_trend_exits():
    # First run TP/SL/breakeven/TIMEOUT management.
    update_simulated_trades()

    prices = get_all_prices()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
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
        WHERE status='OPEN'
          AND engine_version IN ('V3', 'V4.2_PRE', %s)
          AND strategy IN ('TREND','BREAKOUT')
          AND tp2_hit=TRUE
          AND tp3_hit=FALSE
        ORDER BY id ASC;
        """,
        (SCANNER_ENGINE_VERSION,),
    )

    trades = cur.fetchall()

    if not trades:
        print("TP2 TREND MONITOR: NO RUNNERS", flush=True)
        cur.close()
        conn.close()
        return

    print("TP2 TREND MONITOR:", len(trades), "runner(s)", flush=True)

    for (
        trade_id,
        symbol,
        side,
        engine_version,
        entry_price,
        tp1,
        tp2,
        highest_price,
        lowest_price,
        margin_usdt,
        leverage,
    ) in trades:

        try:
            if symbol not in prices:
                continue

            side = (side or "LONG").upper()

            exit_now, weakness = should_exit_after_tp2(symbol, side)

            if not exit_now:
                print(
                    "KEEP RUNNING:",
                    symbol,
                    side,
                    "|",
                    ", ".join(weakness) if weakness else "trend healthy",
                    flush=True,
                )
                continue

            current_price = float(prices[symbol])
            entry_price = float(entry_price)
            tp1 = float(tp1)

            # After TP2, TP1 is the protection level.
            if side == "SHORT":
                effective_exit = min(current_price, tp1)
                runner_return_pct = (
                    (entry_price - effective_exit)
                    / entry_price
                    * 100
                )
            else:
                effective_exit = max(current_price, tp1)
                runner_return_pct = (
                    (effective_exit - entry_price)
                    / entry_price
                    * 100
                )

            tp1_return_pct = side_return_pct(
                side,
                entry_price,
                tp1,
            )

            tp2_return_pct = side_return_pct(
                side,
                entry_price,
                float(tp2),
            )

            gross_result = (
                (TP1_PCT / 100) * tp1_return_pct
                + (TP2_PCT / 100) * tp2_return_pct
                + (TP3_PCT / 100) * runner_return_pct
            )

            highest_price = max(
                float(highest_price or entry_price),
                effective_exit,
            )
            lowest_price = min(
                float(lowest_price or entry_price),
                effective_exit,
            )

            finalize_trade(
                cur,
                trade_id,
                effective_exit,
                gross_result,
                "TREND_EXIT",
                True,
                True,
                False,
                False,
                highest_price,
                lowest_price,
                margin_usdt,
                leverage,
                ", ".join(weakness),
            )

            conn.commit()

            print(
                "TREND EXIT:",
                symbol,
                side,
                "| price",
                effective_exit,
                "| weakness",
                ", ".join(weakness),
                flush=True,
            )

        except Exception as e:
            conn.rollback()
            print(
                "TREND EXIT ERROR:",
                symbol,
                str(e),
                flush=True,
            )

    cur.close()
    conn.close()


# =========================================================
# MAIN V4.2 RADAR
# =========================================================

def run_fast_radar():
    print("========================================", flush=True)
    print("BINGX V4.2 LOCKED TEST 5-MINUTE RADAR START", flush=True)
    print("========================================", flush=True)

    init_database()
    init_extra_database()

    # 1. Update all old/current open simulated trades.
    try:
        update_simulated_trades()
    except Exception as e:
        print("TRADE UPDATE ERROR:", str(e), flush=True)

    # 2. Manage TP2 runners.
    try:
        manage_tp2_trend_exits()
    except Exception as e:
        print("TREND EXIT MANAGER ERROR:", str(e), flush=True)

    # 3. V4.2 risk gate.
    allow_new, risk_reason = scanner_risk_allows_new_trade()

    print(
        "RISK GATE:",
        allow_new,
        "|",
        risk_reason,
        flush=True,
    )

    market_quota = determine_market_quota()

    print(
        "MARKET REGIME:",
        market_quota["regime"],
        "| bias",
        market_quota["bias_score"],
        "| BTC",
        market_quota["btc_score"],
        "| ETH",
        market_quota["eth_score"],
        "| LONG",
        market_quota["long_quota"],
        "| SHORT",
        market_quota["short_quota"],
        flush=True,
    )

    previous = load_previous_snapshots()
    current = get_all_tickers()

    print("MARKETS RECEIVED:", len(current), flush=True)

    if not previous:
        save_snapshots(current)
        print("FIRST V4.2 RADAR RUN: BASELINE CREATED", flush=True)
        return

    candidates = build_radar_candidates(current, previous)

    # Save early so next cron has a stable comparison baseline.
    save_snapshots(current)

    print("FAST RADAR CANDIDATES:", len(candidates), flush=True)

    for item in candidates:
        print(
            item["symbol"],
            "|",
            item["side"],
            "| 5m",
            f"{item['radar_move']:+.2f}%",
            "| Radar",
            item["radar_score"],
            flush=True,
        )

    if not allow_new:
        print(
            "NEW ENTRY BLOCKED BY RISK CONTROL:",
            risk_reason,
            flush=True,
        )
        print("BINGX V4.2 RADAR COMPLETE", flush=True)
        return

    qualified = []

    for radar in candidates:
        try:
            result = deep_analyze(radar)

            if result:
                qualified.append(result)

        except Exception as e:
            print(
                "DEEP ANALYSIS ERROR:",
                radar["symbol"],
                radar["side"],
                str(e),
                flush=True,
            )

    # Prefer score, then BREAKOUT, then stronger absolute radar move.
    qualified.sort(
        key=lambda x: (
            x["score"],
            1 if x["strategy"] == "BREAKOUT" else 0,
            abs(x.get("radar_move", 0)),
        ),
        reverse=True,
    )

    created = 0

    for result in qualified:
        if created >= SCANNER_MAX_NEW_PER_SCAN:
            break

        if scanner_open_count() >= SCANNER_MAX_OPEN:
            print("MAX OPEN TRADES REACHED", flush=True)
            break

        side = result["side"]
        side_open = scanner_open_count(side)

        if side == "LONG":
            side_limit = market_quota["long_quota"]
        else:
            side_limit = market_quota["short_quota"]

        if side_open >= side_limit:
            print(
                "SIDE QUOTA FULL:",
                side,
                f"{side_open}/{side_limit}",
                "| regime",
                market_quota["regime"],
                flush=True,
            )
            continue

        allow_new, risk_reason = scanner_risk_allows_new_trade()

        if not allow_new:
            print("RISK CONTROL STOP:", risk_reason, flush=True)
            break

        if not scanner_can_open(result["symbol"]):
            print(
                "COOLDOWN:",
                result["symbol"],
                result["side"],
                flush=True,
            )
            continue

        create_simulated_trade(result)
        created += 1

    print("QUALIFIED:", len(qualified), flush=True)
    print("NEW V4.2 SIMULATED TRADES:", created, flush=True)

    # Final update after new trades are inserted.
    try:
        update_simulated_trades()
    except Exception as e:
        print("POST UPDATE ERROR:", str(e), flush=True)

    print("========================================", flush=True)
    print("BINGX V4.2 LOCKED TEST 5-MINUTE RADAR COMPLETE", flush=True)
    print("========================================", flush=True)


if __name__ == "__main__":
    start = time.time()

    try:
        run_fast_radar()

    except Exception as e:
        print("RADAR FATAL ERROR:", str(e), flush=True)
        raise

    finally:
        elapsed = time.time() - start
        print(
            f"TOTAL RUN TIME: {elapsed:.2f} seconds",
            flush=True,
        )
