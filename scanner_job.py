import time

from app import (
    init_database,
    update_simulated_trades,
    run_market_scan,
)


def main():

    print(
        "========================================",
        flush=True
    )

    print(
        "BINGX SCANNER CRON JOB START",
        flush=True
    )

    print(
        "========================================",
        flush=True
    )

    # 確認資料庫存在
    init_database()

    # 先更新之前還沒結束的模擬單
    try:

        print(
            "UPDATING EXISTING SIMULATED TRADES...",
            flush=True
        )

        update_simulated_trades()

        print(
            "EXISTING TRADES UPDATED",
            flush=True
        )

    except Exception as e:

        print(
            "PRE-SCAN TRADE UPDATE ERROR:",
            str(e),
            flush=True
        )

    # 開始跑完整市場掃描
    print(
        "STARTING MARKET SCAN...",
        flush=True
    )

    start_time = time.time()

    run_market_scan()

    elapsed = (
        time.time()
        - start_time
    )

    print(
        "MARKET SCAN FINISHED",
        flush=True
    )

    print(
        f"SCAN TIME: "
        f"{elapsed / 60:.2f} minutes",
        flush=True
    )

    # 掃描結束後，再更新一次模擬單狀態
    try:

        print(
            "UPDATING SIMULATED TRADES AFTER SCAN...",
            flush=True
        )

        update_simulated_trades()

        print(
            "POST-SCAN TRADES UPDATED",
            flush=True
        )

    except Exception as e:

        print(
            "POST-SCAN TRADE UPDATE ERROR:",
            str(e),
            flush=True
        )

    print(
        "========================================",
        flush=True
    )

    print(
        "BINGX SCANNER CRON JOB COMPLETE",
        flush=True
    )

    print(
        "========================================",
        flush=True
    )


if __name__ == "__main__":

    main()
