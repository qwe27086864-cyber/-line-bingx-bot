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
    calculate_net_result,
    finalize_v3_trade,

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
# V3 FAST RADAR SETTINGS
# =========================================================

RADAR_MIN_5M_MOVE = 0.60
RADAR_MAX_CANDIDATES = 12
RADAR_MIN_24H_QUOTE_VOLUME = 50000
RADAR_MAX_SPREAD_PCT = 2.0
API_DELAY = 1.10

# Trend-exit requires at least this many weakness signals.
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

    print("V3 RADAR DATABASE READY", flush=True)


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
# FAST RADAR
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

        # Cron should be about 5m, but allow delayed executions.
        if age_minutes < 2 or age_minutes > 20:
            continue

        move_pct = (item["price"] - old_price) / old_price * 100

        if item["quote_volume"] < RADAR_MIN_24H_QUOTE_VOLUME:
            continue

        if item["spread_pct"] > RADAR_MAX_SPREAD_PCT:
            continue

        radar_score = 0
        reasons = []

        if move_pct >= 0.30:
            radar_score += 10
        if move_pct >= 0.60:
            radar_score += 20
            reasons.append(f"5åé+{move_pct:.2f}%")
        if move_pct >= 1.20:
            radar_score += 25
            reasons.append("ç­ç·å¿«éå é")
        if move_pct >= 2.00:
            radar_score += 20

        if item["change_24h"] > 0:
            radar_score += 5
        if item["change_24h"] >= 5:
            radar_score += 5
        if item["spread_pct"] <= 0.5:
            radar_score += 5

        if move_pct >= RADAR_MIN_5M_MOVE:
            candidates.append({
                "symbol": symbol,
                "radar_move": move_pct,
                "radar_score": radar_score,
                "radar_price": item["price"],
                "quote_volume": item["quote_volume"],
                "change_24h": item["change_24h"],
                "spread_pct": item["spread_pct"],
                "radar_reasons": reasons,
            })

    candidates.sort(
        key=lambda x: (x["radar_score"], x["radar_move"]),
        reverse=True,
    )
    return candidates[:RADAR_MAX_CANDIDATES]


def deep_analyze(radar):
    symbol = radar["symbol"]

    print(
        "DEEP ANALYSIS:",
        symbol,
        "| radar move",
        f"{radar['radar_move']:.2f}%",
        flush=True,
    )

    fast = analyze_fast_5m(symbol)
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
        print("REJECTED:", symbol, flush=True)
        return None

    chosen["reasons"].insert(
        0,
        f"é·é5m+{radar['radar_move']:.2f}%"
    )

    return chosen


# =========================================================
# TP2 RUNNER TREND EXIT
# =========================================================

def should_exit_after_tp2(symbol):
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

    weakness = []

    if last5 < ema20_5:
        weakness.append("5mè·ç ´EMA20")

    if ema9_5 < ema20_5:
        weakness.append("5m EMA9<EMA20")

    if close15[-1] < ema20_15:
        weakness.append("15mè·ç ´EMA20")

    momentum_15m = (close5[-1] - close5[-4]) / close5[-4] * 100
    if momentum_15m < 0:
        weakness.append(f"15åéåè½{momentum_15m:.2f}%")

    if rsi5 < 48:
        weakness.append(f"5m RSIéè³{rsi5:.1f}")

    return len(weakness) >= TREND_EXIT_WEAKNESS_COUNT, weakness


def manage_tp2_trend_exits():
    # First run the 1m replay/protective-stop engine.
    # This guarantees a position that already hit protection is closed
    # before the trend-exit logic sees it.
    update_simulated_trades()

    prices = get_all_prices()

    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT
            id,
            symbol,
            entry_price,
            tp1,
            highest_price,
            lowest_price,
            sim_margin_usdt,
            sim_leverage
        FROM scanner_trades
        WHERE status='OPEN'
          AND engine_version='V3'
          AND strategy IN ('TREND','BREAKOUT')
          AND tp2_hit=TRUE
          AND tp3_hit=FALSE
        ORDER BY id ASC;
        """
    )

    trades = cur.fetchall()

    if not trades:
        print("TP2 TREND MONITOR: NO V3 RUNNERS", flush=True)
        cur.close()
        conn.close()
        return

    print("TP2 TREND MONITOR:", len(trades), "runner(s)", flush=True)

    for (
        trade_id, symbol, entry_price, tp1,
        highest_price, lowest_price,
        margin_usdt, leverage
    ) in trades:
        try:
            if symbol not in prices:
                continue

            exit_now, weakness = should_exit_after_tp2(symbol)

            if not exit_now:
                print(
                    "KEEP RUNNING:",
                    symbol,
                    "|",
                    ", ".join(weakness) if weakness else "trend healthy",
                    flush=True,
                )
                continue

            current_price = prices[symbol]

            # Protection is already TP1 after TP2.
            # Never let trend-exit simulate below the protection floor.
            effective_exit = max(float(current_price), float(tp1))

            runner_return_pct = (
                (effective_exit - float(entry_price))
                / float(entry_price)
                * 100
            )

            gross_result = (
                (TP1_PCT / 100) * SCANNER_TP1
                + (TP2_PCT / 100) * SCANNER_TP2
                + (TP3_PCT / 100) * runner_return_pct
            )

            highest_price = max(float(highest_price or entry_price), effective_exit)
            lowest_price = min(float(lowest_price or entry_price), effective_exit)

            finalize_v3_trade(
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
                "| price",
                effective_exit,
                "| weakness",
                ", ".join(weakness),
                flush=True,
            )

        except Exception as e:
            conn.rollback()
            print("TREND EXIT ERROR:", symbol, str(e), flush=True)

    cur.close()
    conn.close()


# =========================================================
# MAIN V3 RADAR
# =========================================================

def run_fast_radar():
    print("========================================", flush=True)
    print("BINGX V3 5-MINUTE RADAR START", flush=True)
    print("========================================", flush=True)

    init_database()
    init_extra_database()

    # 1. Update V2/V3 open trades. V3 gets 1m replay + protection.
    try:
        update_simulated_trades()
    except Exception as e:
        print("TRADE UPDATE ERROR:", str(e), flush=True)

    # 2. Manage V3 TP2 runner exits.
    try:
        manage_tp2_trend_exits()
    except Exception as e:
        print("TREND EXIT MANAGER ERROR:", str(e), flush=True)

    # 3. Risk gate before looking for new entries.
    allow_new, risk_reason = scanner_risk_allows_new_trade()
    print("RISK GATE:", allow_new, "|", risk_reason, flush=True)

    previous = load_previous_snapshots()
    current = get_all_tickers()

    print("MARKETS RECEIVED:", len(current), flush=True)

    if not previous:
        save_snapshots(current)
        print("FIRST RADAR RUN: BASELINE CREATED", flush=True)
        return

    candidates = build_radar_candidates(current, previous)

    # Save snapshot early so the next cron has a stable comparison baseline.
    save_snapshots(current)

    print("FAST RADAR CANDIDATES:", len(candidates), flush=True)

    for item in candidates:
        print(
            item["symbol"],
            "| 5m",
            f"{item['radar_move']:.2f}%",
            "| Radar",
            item["radar_score"],
            flush=True,
        )

    if not allow_new:
        print("NEW ENTRY BLOCKED BY RISK CONTROL:", risk_reason, flush=True)
        print("BINGX V3 RADAR COMPLETE", flush=True)
        return

    qualified = []

    for radar in candidates:
        try:
            result = deep_analyze(radar)
            if result:
                qualified.append(result)
        except Exception as e:
            print("DEEP ANALYSIS ERROR:", radar["symbol"], str(e), flush=True)

    qualified.sort(key=lambda x: x["score"], reverse=True)

    created = 0

    for result in qualified:
        if created >= SCANNER_MAX_NEW_PER_SCAN:
            break

        if scanner_open_count() >= SCANNER_MAX_OPEN:
            print("MAX OPEN TRADES REACHED", flush=True)
            break

        allow_new, risk_reason = scanner_risk_allows_new_trade()
        if not allow_new:
            print("RISK CONTROL STOP:", risk_reason, flush=True)
            break

        if not scanner_can_open(result["symbol"]):
            print("COOLDOWN:", result["symbol"], flush=True)
            continue

        create_simulated_trade(result)
        created += 1

    print("QUALIFIED:", len(qualified), flush=True)
    print("NEW V3 SIMULATED TRADES:", created, flush=True)

    # Final update after new trades are inserted.
    try:
        update_simulated_trades()
    except Exception as e:
        print("POST UPDATE ERROR:", str(e), flush=True)

    print("========================================", flush=True)
    print("BINGX V3 5-MINUTE RADAR COMPLETE", flush=True)
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
        print(f"TOTAL RUN TIME: {elapsed:.2f} seconds", flush=True)
