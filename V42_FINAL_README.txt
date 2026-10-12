V4.2R1 歷史回測研究整合包（非實盤）
======================================
這是一套可在有網路的 Python 電腦/伺服器上執行的研究流程，
不是已證明獲利的策略，也沒有宣稱已取得真正 500 筆歷史交易。

使用前準備：Python 3.10+；能連線公開 GitHub 與 BingX K 線 API。
請將 ZIP 解壓，進入檔案資料夾。

只檢查工作量，不下載、不修改任何外部資源：
python v42_one_shot.py --symbols BTC-USDT ETH-USDT --start 2026-06-01T00:00:00Z --end 2026-06-08T00:00:00Z --workdir v42_study --plan-only

真正開始歷史研究（建議先少量日期及少數幣測試）：
python v42_one_shot.py --symbols BTC-USDT ETH-USDT --start 2026-06-01T00:00:00Z --end 2026-06-08T00:00:00Z --workdir v42_study

輸出：v42_study/results/SCENARIO_SUMMARY.json
不同假設 spread 比較：0.05%, 0.15%, 0.30%
原始策略來源：GitHub qwe27086864-cyber/-line-bingx-bot commit 0169359859f4126f1d54b842ef4d44d9c6150aaa
來源程式只讀，透過 AST 抽取進場函式，不執行原版 Web、BingX 下單或 DB。

重要限制：
- 我們的執行環境無法連接 BingX/原始 GitHub，因此目前無法直接下載資料並做真實回測。
- 這套使用回算的 24h ticker / 5min radar 並非當年錄製的 ticker 快照。
- 歷史 spread 是假設而非真實歸檔買賣價差；滑價亦為模型。
- 1m K 線不保證能辨識同一分鐘 TP/SL 先後順序。
- 趨勢出場依已完成 5m/15m K 線近似，不等同原服務逐筆成交。
- 原始交易最優 PF 約 2.6 尚未驗證；500 筆樣本尚未達成。
- 回測沒有評估未交易的歷史下架/失效交易對的存活者偏差。
- 千萬不要把回測高 PF 直接當成可實盤的證據。

此包完全不呼叫私有下單 API、不寫 Render DB、不修改 V5、不修改原始 GitHub。
如果下載失敗，停止而不是虛構或補齊 K 線。
