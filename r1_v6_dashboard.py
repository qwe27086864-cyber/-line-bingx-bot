"""Read-only research dashboard: R2F + R3 + BF1 + AS1. PAPER ONLY."""
import os
from datetime import datetime, timezone

from flask import Flask, Response, render_template_string, request
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
DIRECTIONAL = ("R2F", "R3")

PAGE = r"""<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="60">
<title>四核心交易研究儀表板</title>
<style>
:root{color-scheme:dark;--bg:#0a101c;--panel:#141e2d;--line:#2a3a50;--muted:#9eafc7;--text:#e9f2ff;--a:#77b9ff;--good:#68e2b0;--bad:#ff9297}
*{box-sizing:border-box}body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Noto Sans TC",sans-serif;margin:0}
header,main{max-width:1380px;margin:auto;padding:18px 14px}header{padding-bottom:0}
h1{font-size:24px;margin:0 0 8px}h2{font-size:18px;margin:0 0 12px}h3{font-size:15px;margin:14px 0 9px}
p,.muted{color:var(--muted);font-size:13px;line-height:1.65}.row{display:flex;gap:9px;flex-wrap:wrap;align-items:center}
.pill{border:1px solid #3b526f;border-radius:20px;padding:5px 10px;color:#bed1eb;font-size:12px}
.panel,.metric{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:15px;min-width:0}
.section{margin-top:15px}.columns{display:grid;grid-template-columns:1fr;gap:15px}
.metrics{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;margin:12px 0}
.metric .name{font-size:12px;color:var(--muted)}.metric .value{font-size:21px;font-weight:750;margin-top:4px;overflow-wrap:anywhere}
.good{color:var(--good)}.bad{color:var(--bad)}.scroll{overflow:auto}
table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:9px 8px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}
th{color:var(--muted);font-weight:500}td.wrap{white-space:normal;min-width:180px}.kicker{color:var(--a);font-size:12px;font-weight:700;letter-spacing:.08em}
.headline{display:flex;justify-content:space-between;align-items:center;gap:10px}.empty{color:var(--muted)}
select{background:#1c293c;border:1px solid #4a607e;border-radius:8px;color:white;padding:8px}
.note{border-left:3px solid #6a94cb;padding-left:10px}details summary{cursor:pointer;color:var(--a)}
@media(min-width:850px){.columns{grid-template-columns:repeat(2,minmax(0,1fr))}.metrics{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(min-width:1180px){.columns.four{grid-template-columns:repeat(4,minmax(0,1fr))}}
</style>
</head>
<body>
<header>
<h1>四核心交易研究儀表板</h1>
<div class="row"><span class="pill">PAPER ONLY</span><span class="pill">唯讀</span><span class="pill">60 秒更新</span><span class="pill">{{now}}</span></div>
<p>目前主研究線：R2F（方向基準）、R3（突破回踩）、BF1（Basis + Funding 套利）、AS1（Avellaneda–Stoikov 做市）。不同核心使用不同績效指標，避免用勝率硬比較不同策略。</p>
</header>
<main>

<section class="panel">
<div class="headline"><h2>四核心總覽</h2>
<form method="get"><label class="muted">統計範圍
<select name="period" onchange="this.form.submit()">
<option value="all" {% if period=="all" %}selected{% endif %}>全部</option>
<option value="24h" {% if period=="24h" %}selected{% endif %}>最近 24 小時</option>
<option value="7d" {% if period=="7d" %}selected{% endif %}>最近 7 天</option>
</select></label></form></div>
<div class="scroll"><table>
<tr><th>核心</th><th>樣本</th><th>持倉 / Inventory</th><th>主要績效</th><th>淨利</th><th>狀態</th></tr>
<tr><td><strong>R2F</strong></td><td>{{stats.R2F.closed_count}} 平倉</td><td>{{stats.R2F.open_count}} 持倉</td><td>WR {{stats.R2F.win_rate}} · PF {{stats.R2F.pf}}</td><td class="{{'good' if stats.R2F.net>=0 else 'bad'}}">{{money(stats.R2F.net)}}</td><td>方向基準</td></tr>
<tr><td><strong>R3</strong></td><td>{{stats.R3.closed_count}} 平倉</td><td>{{stats.R3.open_count}} 持倉</td><td>WR {{stats.R3.win_rate}} · PF {{stats.R3.pf}}</td><td class="{{'good' if stats.R3.net>=0 else 'bad'}}">{{money(stats.R3.net)}}</td><td>突破回踩</td></tr>
<tr><td><strong>BF1</strong></td><td>{{bf.obs_count}} 掃描</td><td>{{bf.open_count}} 套利倉</td><td>{{bf.closed_count}} 已平倉</td><td class="{{'good' if bf.net>=0 else 'bad'}}">{{money(bf.net)}}</td><td>Basis + Funding</td></tr>
<tr><td><strong>AS1</strong></td><td>{{as1.snapshot_count}} snapshots · {{as1.fill_count}} fills</td><td>{{number(as1.inventory_base)}} BTC</td><td>Equity {{money_plain(as1.equity)}}</td><td class="{{'good' if as1.net_pnl>=0 else 'bad'}}">{{money(as1.net_pnl)}}</td><td>Market Making</td></tr>
</table></div>
</section>

<div class="columns four section">
{% for v in directional %}{% set s=stats[v] %}
<section class="panel">
<div class="kicker">{{v}} CORE</div><h2>{{"快速方向基準" if v=="R2F" else "突破回踩 · 再加速"}}</h2>
<div class="metrics">
<div class="metric"><div class="name">勝率</div><div class="value">{{s.win_rate}}</div></div>
<div class="metric"><div class="name">PF</div><div class="value">{{s.pf}}</div></div>
<div class="metric"><div class="name">淨利</div><div class="value {{'good' if s.net>=0 else 'bad'}}">{{money(s.net)}}</div></div>
<div class="metric"><div class="name">平倉 / 持倉</div><div class="value">{{s.closed_count}} / {{s.open_count}}</div></div>
<div class="metric"><div class="name">最大回撤</div><div class="value">{{money_plain(s.drawdown)}}</div></div>
<div class="metric"><div class="name">平均每單</div><div class="value">{{money(s.avg) if s.closed_count else '—'}}</div></div>
</div>
</section>
{% endfor %}

<section class="panel">
<div class="kicker">BF1 CORE</div><h2>Basis + Funding 套利</h2>
<div class="metrics">
<div class="metric"><div class="name">掃描樣本</div><div class="value">{{bf.obs_count}}</div></div>
<div class="metric"><div class="name">目前套利倉</div><div class="value">{{bf.open_count}}</div></div>
<div class="metric"><div class="name">已平倉</div><div class="value">{{bf.closed_count}}</div></div>
<div class="metric"><div class="name">已實現淨利</div><div class="value {{'good' if bf.net>=0 else 'bad'}}">{{money(bf.net)}}</div></div>
<div class="metric"><div class="name">Funding 累積</div><div class="value">{{money(bf.funding)}}</div></div>
<div class="metric"><div class="name">最近資料</div><div class="value" style="font-size:14px">{{dt(bf.last_obs)}}</div></div>
</div>
</section>

<section class="panel">
<div class="kicker">AS1 CORE</div><h2>Avellaneda–Stoikov 做市</h2>
<div class="metrics">
<div class="metric"><div class="name">Snapshots</div><div class="value">{{as1.snapshot_count}}</div></div>
<div class="metric"><div class="name">Fills</div><div class="value">{{as1.fill_count}}</div></div>
<div class="metric"><div class="name">Equity</div><div class="value">{{money_plain(as1.equity)}}</div></div>
<div class="metric"><div class="name">Net PnL</div><div class="value {{'good' if as1.net_pnl>=0 else 'bad'}}">{{money(as1.net_pnl)}}</div></div>
<div class="metric"><div class="name">Inventory</div><div class="value">{{number(as1.inventory_base)}} BTC</div></div>
<div class="metric"><div class="name">Maker Fees</div><div class="value">{{money_plain(as1.fees)}}</div></div>
</div>
</section>
</div>

<div class="columns section">
<section class="panel">
<div class="headline"><div><div class="kicker">BF1 LIVE RESEARCH</div><h2>最新 Basis / Funding 掃描</h2></div><span class="pill">{{bf.obs_count}} 筆</span></div>
<div class="scroll"><table>
<tr><th>UTC</th><th>幣種</th><th>Open Basis</th><th>Close Basis</th><th>Funding</th><th>預估淨 Edge</th><th>決策</th><th>原因</th></tr>
{% for x in bf_recent %}
<tr><td>{{dt(x.observed_at)}}</td><td>{{x.symbol}}</td><td>{{percent(x.open_basis_pct)}}</td><td>{{percent(x.close_basis_pct)}}</td><td>{{rate_percent(x.funding_rate)}}</td><td class="{{'good' if x.expected_net_edge_pct is not none and x.expected_net_edge_pct>=0 else 'bad'}}">{{percent(x.expected_net_edge_pct)}}</td><td>{{x.decision}}</td><td class="wrap">{{x.reason or '—'}}</td></tr>
{% else %}<tr><td colspan="8" class="empty">等待 BF1 第一次有效掃描資料</td></tr>{% endfor %}
</table></div>
<h3>BF1 目前 / 最近套利倉</h3>
<div class="scroll"><table>
<tr><th>幣種</th><th>Status</th><th>開倉 UTC</th><th>Entry Basis</th><th>Funding 累積</th><th>Net PnL</th><th>出場原因</th></tr>
{% for x in bf_positions %}
<tr><td>{{x.symbol}}</td><td>{{x.status}}</td><td>{{dt(x.opened_at)}}</td><td>{{percent(x.entry_basis_pct)}}</td><td>{{money(x.funding_accrued_usdt)}}</td><td class="{{'good' if x.net_pnl_usdt is not none and x.net_pnl_usdt>=0 else 'bad' if x.net_pnl_usdt is not none else ''}}">{{money(x.net_pnl_usdt)}}</td><td>{{x.exit_reason or '—'}}</td></tr>
{% else %}<tr><td colspan="7" class="empty">尚無 BF1 套利倉</td></tr>{% endfor %}
</table></div>
</section>

<section class="panel">
<div class="headline"><div><div class="kicker">AS1 LIVE RESEARCH</div><h2>做市狀態</h2></div><span class="pill">BTC-USDT</span></div>
{% if as_state %}
<div class="metrics">
<div class="metric"><div class="name">Buy / Sell fills</div><div class="value">{{as_state.buy_fills}} / {{as_state.sell_fills}}</div></div>
<div class="metric"><div class="name">Cash</div><div class="value">{{money_plain(as_state.cash_usdt)}}</div></div>
<div class="metric"><div class="name">Last Mid</div><div class="value">{{price(as_state.last_mid)}}</div></div>
<div class="metric"><div class="name">Inventory</div><div class="value">{{number(as_state.inventory_base)}} BTC</div></div>
<div class="metric"><div class="name">Fees</div><div class="value">{{money_plain(as_state.fees_usdt)}}</div></div>
<div class="metric"><div class="name">更新時間</div><div class="value" style="font-size:14px">{{dt(as_state.updated_at)}}</div></div>
</div>
{% else %}<p class="empty">AS1 state 尚未建立</p>{% endif %}
<h3>最近報價 Snapshot</h3>
<div class="scroll"><table>
<tr><th>UTC</th><th>Mid</th><th>市場 Spread</th><th>Bid Quote</th><th>Ask Quote</th><th>Inventory</th><th>Equity</th><th>Net PnL</th></tr>
{% for x in as_recent %}
<tr><td>{{dt(x.observed_at)}}</td><td>{{price(x.mid)}}</td><td>{{bps(x.market_spread_bps)}}</td><td>{{price(x.bid_quote)}}</td><td>{{price(x.ask_quote)}}</td><td>{{number(x.inventory_base)}}</td><td>{{money_plain(x.equity_usdt)}}</td><td class="{{'good' if x.net_pnl_usdt>=0 else 'bad'}}">{{money(x.net_pnl_usdt)}}</td></tr>
{% else %}<tr><td colspan="8" class="empty">等待 AS1 snapshot</td></tr>{% endfor %}
</table></div>
<h3>最近模擬 Fills</h3>
<div class="scroll"><table>
<tr><th>UTC</th><th>Side</th><th>Price</th><th>Notional</th><th>Fee</th><th>Inventory After</th><th>Quote Age</th></tr>
{% for x in as_fills %}
<tr><td>{{dt(x.filled_at)}}</td><td>{{x.side}}</td><td>{{price(x.price)}}</td><td>{{money_plain(x.notional_usdt)}}</td><td>{{money_plain(x.maker_fee_usdt)}}</td><td>{{number(x.inventory_after)}}</td><td>{{seconds(x.quote_age_sec)}}</td></tr>
{% else %}<tr><td colspan="7" class="empty">尚無 AS1 模擬成交</td></tr>{% endfor %}
</table></div>
</section>
</div>

<div class="columns section">
<section class="panel">
<h2>R2F 最新決策</h2>
<div class="scroll"><table><tr><th>UTC</th><th>幣種</th><th>路徑</th><th>D</th><th>M</th><th>Exec</th><th>Fast</th><th>Spread</th><th>追價</th><th>決策</th></tr>
{% for d in r2f_decisions %}<tr><td>{{dt(d.observed_at)}}</td><td>{{d.symbol}}</td><td>{{d.path}}</td><td>{{number(d.direction_score)}}</td><td>{{number(d.micro_score)}}</td><td>{{number(d.execution_score)}}</td><td>{{number(d.fast_score)}}</td><td>{{percent(d.spread_pct)}}</td><td>{{percent(d.chase_pct)}}</td><td>{{d.decision}}</td></tr>
{% else %}<tr><td colspan="10" class="empty">尚無 R2F 決策</td></tr>{% endfor %}</table></div>
</section>
<section class="panel">
<h2>R3 Setup / Pullback / Trigger</h2>
<div class="metrics">
<div class="metric"><div class="name">Watchlist</div><div class="value">{{r3_watch_total}}</div></div>
<div class="metric"><div class="name">Active</div><div class="value">{{r3_watch_active}}</div></div>
<div class="metric"><div class="name">Entered</div><div class="value">{{r3_entered}}</div></div>
</div>
<div class="scroll"><table><tr><th>UTC</th><th>幣種</th><th>方向</th><th>Stage</th><th>決策</th><th>Strength</th><th>Pullback</th><th>原因</th></tr>
{% for d in r3_decisions %}<tr><td>{{dt(d.observed_at)}}</td><td>{{d.symbol}}</td><td>{{d.side}}</td><td>{{d.stage or '—'}}</td><td>{{d.decision}}</td><td>{{number(d.setup_strength)}}</td><td>{{percent(d.pullback_pct)}}</td><td class="wrap">{{d.reason or '—'}}</td></tr>
{% else %}<tr><td colspan="8" class="empty">等待 R3 候選</td></tr>{% endfor %}</table></div>
</section>
</div>

<section class="panel section">
<h2>R2F / R3 影子研究</h2>
<div class="scroll"><table><tr><th>版本</th><th>影子筆數</th><th>完成 10m</th><th>10m 平均</th><th>10m 正收益率</th><th>MFE</th><th>MAE</th><th>TP1</th><th>SL</th></tr>
{% for v in directional %}{% set sh=shadow_stats[v] %}
<tr><td><strong>{{v}}</strong></td><td>{{sh.total}}</td><td>{{sh.completed}}</td><td>{{percent(sh.avg_10m)}}</td><td>{{sh.positive_10m_rate}}</td><td>{{percent(sh.avg_mfe)}}</td><td>{{percent(sh.avg_mae)}}</td><td>{{sh.tp1_hits}}</td><td>{{sh.sl_hits}}</td></tr>
{% endfor %}
</table></div>
</section>

<p class="note">四核心仍全部是模擬研究。R2F/R3 的淨利是已平倉模擬交易；BF1 是已關閉套利倉的淨利；AS1 是目前 equity 相對起始 1000U 的即時模擬 PnL。這些數字用途不同，不應只看單一排名就判定核心優劣。</p>
</main></body></html>"""


def f(v):
    return None if v is None else float(v)

def money(v):
    return "—" if v is None else f"{float(v):+.4f} U"

def money_plain(v):
    return "—" if v is None else f"{float(v):.4f} U"

def percent(v):
    return "—" if v is None else f"{float(v):+.3f}%"

def rate_percent(v):
    return "—" if v is None else f"{float(v)*100:+.4f}%"

def bps(v):
    return "—" if v is None else f"{float(v):.2f} bps"

def number(v):
    return "—" if v is None else f"{float(v):.6f}".rstrip("0").rstrip(".")

def seconds(v):
    return "—" if v is None else f"{float(v):.1f}s"

def price(v):
    if v is None:
        return "—"
    x = float(v)
    if abs(x) < 1:
        return f"{x:.8f}".rstrip("0").rstrip(".")
    return f"{x:.6f}".rstrip("0").rstrip(".")

def dt(v):
    return v.strftime("%m/%d %H:%M") if v else "—"

def calc_stats(trades):
    active = [t for t in trades if t["status"] == "OPEN"]
    closed = sorted([t for t in trades if t["status"] != "OPEN" and t.get("closed_at")], key=lambda x:(x["closed_at"],x["id"]))
    pnl = [float(t["sim_pnl_usdt"]) for t in closed if t.get("sim_pnl_usdt") is not None]
    gains = sum(max(0,x) for x in pnl)
    losses = -sum(min(0,x) for x in pnl)
    net = sum(pnl)
    bal = peak = 1000.0
    dd = 0.0
    for x in pnl:
        bal += x
        peak = max(peak, bal)
        dd = max(dd, peak-bal)
    return {
        "total":len(trades),
        "open_count":len(active),
        "closed_count":len(closed),
        "win_rate":f"{sum(x>0 for x in pnl)/len(pnl)*100:.1f}%" if pnl else "—",
        "pf":f"{gains/losses:.2f}" if losses else ("∞" if gains else "—"),
        "net":net,
        "avg":net/len(pnl) if pnl else 0.0,
        "drawdown":dd,
    }

def calc_shadow(rows):
    completed = [r for r in rows if r.get("return_10m_pct") is not None]
    def avg(field):
        vals = [float(r[field]) for r in rows if r.get(field) is not None]
        return sum(vals)/len(vals) if vals else None
    return {
        "total":len(rows),
        "completed":len(completed),
        "avg_10m":avg("return_10m_pct"),
        "avg_mfe":avg("mfe_10m_pct"),
        "avg_mae":avg("mae_10m_pct"),
        "positive_10m_rate":f"{sum(float(r['return_10m_pct'])>0 for r in completed)/len(completed)*100:.1f}%" if completed else "—",
        "tp1_hits":sum(r.get("tp1_hit_10m") is True for r in rows),
        "sl_hits":sum(r.get("sl_hit_10m") is True for r in rows),
    }

@app.get("/health")
def health():
    return {"status":"ok","mode":"read_only","cores":["R2F","R3","BF1","AS1"]}

@app.get("/")
def home():
    if os.getenv("LIVE_TRADING","false").lower() != "false":
        return Response("Read-only dashboard requires LIVE_TRADING=false", status=503)
    url = os.getenv("DATABASE_URL","")
    if not url:
        return Response("DATABASE_URL is missing", status=503)
    period = request.args.get("period","all")
    if period not in ("all","24h","7d"):
        period = "all"
    interval = {"all":None,"24h":"24 hours","7d":"7 days"}[period]

    try:
        with psycopg2.connect(url, sslmode="require", connect_timeout=8) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET TRANSACTION READ ONLY")

                cur.execute("""SELECT id,symbol,side,signal_time,status,closed_at,sim_pnl_usdt,engine_version
                              FROM scanner_trades
                              WHERE engine_version IN ('R2F','R3')
                                AND (%s::text IS NULL OR signal_time >= NOW() - (%s::text)::interval)
                              ORDER BY signal_time DESC LIMIT 30000""",(interval,interval))
                trades = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT observed_at,symbol,path,direction_score,micro_score,execution_score,fast_score,
                                     spread_pct,chase_pct,analysis_latency_ms,decision
                              FROM r2_fast_decisions
                              WHERE (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval)
                              ORDER BY observed_at DESC LIMIT 25000""",(interval,interval))
                r2f_all = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT observed_at,symbol,side,stage,decision,reason,setup_strength,pullback_pct
                              FROM r3_decisions
                              WHERE (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval)
                              ORDER BY observed_at DESC LIMIT 25000""",(interval,interval))
                r3_all = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT status,stage FROM r3_watchlist
                              WHERE (%s::text IS NULL OR created_at >= NOW() - (%s::text)::interval)""",(interval,interval))
                r3_watches = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT engine_version,return_10m_pct,mfe_10m_pct,mae_10m_pct,tp1_hit_10m,sl_hit_10m
                              FROM shadow_rejections
                              WHERE engine_version IN ('R2F','R3')
                                AND (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval)
                              ORDER BY observed_at DESC LIMIT 30000""",(interval,interval))
                shadow_rows = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT observed_at,symbol,open_basis_pct,close_basis_pct,funding_rate,
                                     expected_net_edge_pct,decision,reason
                              FROM bf_observations
                              WHERE (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval)
                              ORDER BY observed_at DESC LIMIT 5000""",(interval,interval))
                bf_obs = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT symbol,status,opened_at,closed_at,entry_basis_pct,funding_accrued_usdt,
                                     net_pnl_usdt,exit_reason
                              FROM bf_positions
                              WHERE (%s::text IS NULL OR opened_at >= NOW() - (%s::text)::interval)
                              ORDER BY opened_at DESC LIMIT 500""",(interval,interval))
                bf_positions = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT symbol,updated_at,cash_usdt,inventory_base,fees_usdt,buy_fills,sell_fills,
                                     last_mid,equity_usdt,net_pnl_usdt
                              FROM as_mm_state WHERE symbol='BTC-USDT' LIMIT 1""")
                row = cur.fetchone()
                as_state = dict(row) if row else None

                cur.execute("""SELECT observed_at,mid,market_spread_bps,bid_quote,ask_quote,inventory_base,equity_usdt,net_pnl_usdt
                              FROM as_mm_snapshots
                              WHERE (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval)
                              ORDER BY observed_at DESC LIMIT 5000""",(interval,interval))
                as_snaps = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT filled_at,side,price,notional_usdt,maker_fee_usdt,inventory_after,quote_age_sec
                              FROM as_mm_fills
                              WHERE (%s::text IS NULL OR filled_at >= NOW() - (%s::text)::interval)
                              ORDER BY filled_at DESC LIMIT 5000""",(interval,interval))
                as_fills_all = [dict(x) for x in cur.fetchall()]

    except psycopg2.Error:
        app.logger.exception("Dashboard read failed")
        return Response("資料庫查詢失敗：請檢查 DATABASE_URL 與資料表", status=503)

    by_version = {v:[] for v in DIRECTIONAL}
    for t in trades:
        if t["engine_version"] in by_version:
            by_version[t["engine_version"]].append(t)
    stats = {v:calc_stats(by_version[v]) for v in DIRECTIONAL}

    shadow_by = {v:[] for v in DIRECTIONAL}
    for r in shadow_rows:
        if r["engine_version"] in shadow_by:
            shadow_by[r["engine_version"]].append(r)
    shadow_stats = {v:calc_shadow(shadow_by[v]) for v in DIRECTIONAL}

    bf_closed = [x for x in bf_positions if x.get("status") != "OPEN" and x.get("net_pnl_usdt") is not None]
    bf = {
        "obs_count":len(bf_obs),
        "open_count":sum(1 for x in bf_positions if x.get("status")=="OPEN"),
        "closed_count":len(bf_closed),
        "net":sum(float(x["net_pnl_usdt"]) for x in bf_closed),
        "funding":sum(float(x.get("funding_accrued_usdt") or 0) for x in bf_positions),
        "last_obs":bf_obs[0]["observed_at"] if bf_obs else None,
    }

    as1 = {
        "snapshot_count":len(as_snaps),
        "fill_count":len(as_fills_all),
        "equity":f(as_state["equity_usdt"]) if as_state else None,
        "net_pnl":f(as_state["net_pnl_usdt"]) if as_state else 0.0,
        "inventory_base":f(as_state["inventory_base"]) if as_state else 0.0,
        "fees":f(as_state["fees_usdt"]) if as_state else 0.0,
    }

    return render_template_string(
        PAGE,
        directional=DIRECTIONAL, stats=stats, period=period,
        now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        bf=bf, bf_recent=bf_obs[:40], bf_positions=bf_positions[:30],
        as1=as1, as_state=as_state, as_recent=as_snaps[:40], as_fills=as_fills_all[:40],
        r2f_decisions=r2f_all[:40],
        r3_decisions=r3_all[:40], r3_watch_total=len(r3_watches),
        r3_watch_active=sum(1 for w in r3_watches if w.get("status")=="ACTIVE"),
        r3_entered=sum(1 for w in r3_watches if w.get("status")=="ENTERED"),
        shadow_stats=shadow_stats,
        money=money, money_plain=money_plain, percent=percent, rate_percent=rate_percent,
        bps=bps, number=number, seconds=seconds, price=price, dt=dt,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","10000")))
