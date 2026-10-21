R2 PAPER CORE — 安裝說明

這版 R2 是「重做核心」，不是 R1 改分數。

核心設計：
1. 1m / 5m / 15m / 1h 多週期重新判斷方向。
2. 先判斷市場 Regime：TREND / BREAKOUT / TRANSITION / RANGE。
3. Micro Score 判斷接下來幾分鐘是否仍有同方向動能。
4. Execution Score 直接把 Bid / Ask / Spread / 訊號後追價幅度算進核心。
5. Forward Score 是短線延續「分數」，不是保證或校準機率。
6. Spread 過大、追價太遠、盤整、方向或微結構不夠強，直接不進。
7. 所有深度分析後的 ENTER / REJECT 都寫入 r2_decisions，後續可拿來訓練真正的預測模型。
8. R2 純模擬，不會送 BingX 私有交易訂單。

第一階段故意保留相同出場框架：
TP1 +2%、TP2 +4%、TP3 +8%、SL -2.5%
TP1 後保本、TP2 後以 TP1 保護、未碰 TP1 最長 4 小時。
這樣先測「新核心本身」到底有沒有優勢，不同時混入新的出場模型。

部署：
A. 在 GitHub 專案 qwe27086864-cyber/-line-bingx-bot 建立新分支：
   r2-development
   建議從 backup-v42r1-best 建立即可。

B. 把 r2_job.py 上傳到 r2-development 的專案根目錄，
   跟 app.py、scanner_job.py 同一層。

C. 不要改現在的 R1M 服務。
   R2 需要另外開一個 Render Cron Job。

D. R2 Cron Job：
   Branch: r2-development
   Build: pip install -r requirements.txt
   Start: python r2_job.py
   Schedule: */5 * * * *
   Region: Singapore
   DATABASE_URL 使用目前同一個 bingx_bot_db。

E. R2 是 paper-only；不需要 LIVE_TRADING=true。
   建議明確設：
   LIVE_TRADING=false

可選環境變數：
R2_MAX_OPEN=3
R2_MAX_NEW_PER_SCAN=2
R2_MIN_QUOTE_VOLUME=250000
R2_ENTER_MAX_SPREAD=0.60
R2_ENTER_MAX_CHASE=0.75
R2_MIN_DIRECTION_SCORE=70
R2_MIN_MICRO_SCORE=58
R2_MIN_FORWARD_SCORE=72

資料：
- scanner_trades.engine_version='R2'：真正進入模擬倉位的交易。
- r2_decisions：包含所有深度分析後的進場/拒絕決策與特徵。
- r2_radar_snapshots：R2 自己的雷達基準，不和 R1/R1M 共用。

注意：
若 r2-development 是從目前含 sitecustomize.py 的 R1M 分支建立，沒關係。
r2_job.py 不使用被 R1M patch 替換的 create_simulated_trade / update_simulated_trades，
它會自行寫入及管理 engine_version='R2' 的資料，因此 R2 和 R1M 資料仍分開。
