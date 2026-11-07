import csv
import io
import json
import os
import urllib.request
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from flask import Flask, Response, render_template_string

app = Flask(__name__)
SYDNEY = ZoneInfo("Australia/Sydney")
F2_PAPER_URL = os.getenv(
    "F2_PAPER_URL",
    "https://bingx-historical-router-backtest-v3.onrender.com",
).rstrip("/")

PAGE = r"""
<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>F2 核心儀表板</title>
<style>
:root{
  color-scheme:dark;
  --bg:#0d1117;--panel:#161b22;--panel2:#11161d;--line:#30363d;
  --text:#e6edf3;--muted:#8b949e;--good:#3fb950;--bad:#f85149;
  --warn:#d29922;--blue:#58a6ff
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:920px;margin:auto;padding:18px}
h1{font-size:27px;margin:0 0 5px}
.sub{color:var(--muted);font-size:13px;margin-bottom:16px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:16px;padding:16px}
.head{display:flex;justify-content:space-between;gap:14px;align-items:flex-start}
.name{font-size:23px;font-weight:850}
.desc{color:var(--muted);font-size:13px;margin-top:4px}
.badge{font-size:12px;font-weight:800;padding:6px 10px;border:1px solid var(--line);border-radius:999px}
.badge.good{color:var(--good)}.badge.warn{color:var(--warn)}.badge.bad{color:var(--bad)}
.metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px;margin-top:16px}
.metric{background:var(--panel2);border:1px solid var(--line);border-radius:11px;padding:11px}
.label{color:var(--muted);font-size:11px}
.value{font-size:18px;font-weight:800;margin-top:4px;overflow-wrap:anywhere}
.pnl.good{color:var(--good)}.pnl.bad{color:var(--bad)}
.rule,.action,.positions{margin-top:13px;background:var(--panel2);border:1px solid var(--line);border-radius:11px;padding:11px 12px}
.rule{font-size:12px;line-height:1.55;color:var(--muted)}
.action .title{font-size:14px;font-weight:850}
.action.good .title{color:var(--good)}.action.warn .title{color:var(--warn)}.action.bad .title{color:var(--bad)}
.action .detail{font-size:12px;color:var(--muted);margin-top:4px}
.positions h2{font-size:14px;margin:0 0 8px}
table{width:100%;border-collapse:collapse;font-size:12px}
th,td{text-align:left;padding:8px 6px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--muted);font-weight:700}
.empty{color:var(--muted);font-size:12px}
.foot{margin-top:13px;color:var(--muted);font-size:11px;display:flex;justify-content:space-between;gap:10px}
@media(max-width:700px){
  main{padding:12px}
  .metrics{grid-template-columns:repeat(2,minmax(0,1fr))}
  .positions{overflow:auto}
}
</style>
</head>
<body>
<main>
<h1>F2 核心儀表板</h1>
<div class="sub">只顯示目前驗證中的 F2 Forward 影子持倉｜不下真單｜每 30 秒更新｜{{ now }}</div>

<div class="card">
  <div class="head">
    <div>
      <div class="name">F2</div>
      <div class="desc">REFERENCE_BREAK 反向 · Forward Shadow</div>
    </div>
    <div class="badge {{ status_class }}">{{ status }}</div>
  </div>

  <div class="metrics">
    <div class="metric"><div class="label">已平倉 / 持倉</div><div class="value">{{ closed }} / {{ open_count }}</div></div>
    <div class="metric"><div class="label">勝率</div><div class="value">{{ win_rate }}</div></div>
    <div class="metric"><div class="label">PF</div><div class="value">{{ pf }}</div></div>
    <div class="metric"><div class="label">Net PnL</div><div class="value pnl {{ 'good' if net>0 else 'bad' if net<0 else '' }}">{{ money(net) }}</div></div>
    <div class="metric"><div class="label">24h PnL</div><div class="value pnl {{ 'good' if pnl24>0 else 'bad' if pnl24<0 else '' }}">{{ money(pnl24) }}</div></div>
    <div class="metric"><div class="label">最大 DD</div><div class="value">{{ money(dd) }}</div></div>
    <div class="metric"><div class="label">掃描次數</div><div class="value">{{ scan_count }}</div></div>
    <div class="metric"><div class="label">Watch</div><div class="value">{{ watches }}</div></div>
  </div>

  <div class="rule">
    固定規則：REFERENCE_BREAK 失敗反向｜BTC/ETH 6h 反向動能 ≥ {{ threshold }}%｜
    TP 2% / 4% / 8%｜SL 2.5%｜模擬 10U × 8x｜只用 BingX 公開行情。
  </div>

  <div class="action {{ action_class }}">
    <div class="title">{{ action }}</div>
    <div class="detail">{{ progress }}</div>
  </div>

  <div class="positions">
    <h2>目前影子持倉</h2>
    {% if open_trades %}
    <table>
      <thead><tr><th>幣種</th><th>方向</th><th>進場</th><th>6h</th><th>時間</th></tr></thead>
      <tbody>
      {% for t in open_trades %}
        <tr>
          <td>{{ t.symbol }}</td>
          <td>{{ t.side }}</td>
          <td>{{ t.entry_price }}</td>
          <td>{{ t.market_6h }}%</td>
          <td>{{ t.entry_time }}</td>
        </tr>
      {% endfor %}
      </tbody>
    </table>
    {% else %}
    <div class="empty">目前沒有 F2 影子持倉。</div>
    {% endif %}
  </div>

  <div class="foot">
    <span>最後掃描：{{ last_scan }}</span>
    <span>模式：LIVE FORWARD PAPER · NO ORDERS</span>
  </div>
</div>
</main>
</body>
</html>
"""


def f(v, default=0.0):
    try:
        return default if v is None else float(v)
    except (TypeError, ValueError):
        return default


def money(v):
    return f"{float(v):+.2f} U"


def parse_utc(v):
    if not v:
        return None
    dt = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_sydney(v):
    dt = parse_utc(v)
    return dt.astimezone(SYDNEY).strftime("%m/%d %H:%M:%S") if dt else "—"


def fetch_text(url, timeout=8):
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "f2-only-dashboard/1.0",
            "Cache-Control": "no-cache",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


def snapshot():
    status = json.loads(fetch_text(F2_PAPER_URL + "/f2_paper/status.json"))
    rows = list(csv.DictReader(io.StringIO(
        fetch_text(F2_PAPER_URL + "/f2_paper/trades.csv")
    )))

    closed = [r for r in rows if (r.get("status") or "").upper() == "CLOSED"]
    open_rows = [r for r in rows if (r.get("status") or "").upper() == "OPEN"]

    gains = losses = net = pnl24 = 0.0
    wins = 0
    eq = peak = dd = 0.0
    cutoff = datetime.now(timezone.utc).timestamp() - 86400

    for r in closed:
        p = f(r.get("sim_pnl_usdt"))
        net += p
        if p > 0:
            wins += 1
            gains += p
        elif p < 0:
            losses += -p

        eq += p
        peak = max(peak, eq)
        dd = max(dd, peak - eq)

        t = parse_utc(r.get("exit_time"))
        if t and t.timestamp() >= cutoff:
            pnl24 += p

    pf = gains / losses if losses > 0 else (float("inf") if gains > 0 else None)
    win_rate = 100 * wins / len(closed) if closed else None

    sample = len(closed)
    if sample < 30:
        action, action_class, progress = (
            "繼續收 F2 影子",
            "warn",
            f"第一輪看 30 筆，還差 {30-sample} 筆",
        )
    elif sample < 50:
        if net < 0 and pf is not None and pf < 0.80:
            action, action_class, progress = (
                "F2 需要檢查",
                "bad",
                f"{sample} 筆影子已偏弱（PF {pf:.2f}）",
            )
        else:
            action, action_class, progress = (
                "繼續收 F2 影子",
                "warn",
                f"確認門檻 50 筆，還差 {50-sample} 筆",
            )
    elif net > 0 and (pf is None or pf >= 1.00):
        action, action_class, progress = (
            "F2 影子通過初步驗證",
            "good",
            f"{sample} 筆 Forward 仍維持正期望",
        )
    else:
        action, action_class, progress = (
            "F2 需要修正",
            "bad",
            f"{sample} 筆 Forward 未維持正期望",
        )

    if sample < 20:
        status_text, status_class = "Forward 樣本不足", "warn"
    elif net > 0 and (pf is None or pf >= 1.0):
        status_text, status_class = "Forward 正向", "good"
    elif net < 0:
        status_text, status_class = "Forward 負期望", "bad"
    else:
        status_text, status_class = "Forward 觀察", "warn"

    open_trades = []
    for r in open_rows[-20:]:
        open_trades.append({
            "symbol": r.get("symbol") or "—",
            "side": r.get("side") or "—",
            "entry_price": r.get("entry_price") or "—",
            "market_6h": f"{f(r.get('market_6h_directional_pct')):.3f}",
            "entry_time": fmt_sydney(r.get("entry_time")),
        })

    rule = status.get("rule") or {}

    return {
        "closed": len(closed),
        "open_count": len(open_rows),
        "wins": wins,
        "win_rate": "—" if win_rate is None else f"{win_rate:.1f}%",
        "pf": "—" if pf is None else ("∞" if pf == float("inf") else f"{pf:.2f}"),
        "net": net,
        "pnl24": pnl24,
        "dd": dd,
        "scan_count": int(status.get("scan_count") or 0),
        "watches": int(status.get("active_watches") or 0),
        "threshold": f"{f(rule.get('btc_eth_6h_directional_min_pct'), 0.425):.3f}",
        "last_scan": fmt_sydney(status.get("last_scan_at")),
        "open_trades": open_trades,
        "action": action,
        "action_class": action_class,
        "progress": progress,
        "status": status_text,
        "status_class": status_class,
    }


@app.get("/health")
def health():
    try:
        s = snapshot()
        return {
            "status": "ok",
            "core": "F2",
            "closed": s["closed"],
            "open": s["open_count"],
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}, 503


@app.get("/")
def home():
    try:
        s = snapshot()
        return render_template_string(
            PAGE,
            now=datetime.now(SYDNEY).strftime("%m/%d %H:%M:%S"),
            money=money,
            **s,
        )
    except Exception as e:
        app.logger.exception("F2 dashboard read failed")
        return Response(f"F2 影子資料讀取失敗: {e}", 503)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
