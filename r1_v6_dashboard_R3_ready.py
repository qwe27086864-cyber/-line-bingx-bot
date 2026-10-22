"""Read-only R2 vs R2F vs R3 research dashboard. PAPER ONLY. No database writes."""
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone

from flask import Flask, Response, render_template_string, request
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
VERSIONS = ("R2", "R2F", "R3")

PAGE = r"""<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="60">
<title>R2 × R2-Fast × R3 模擬研究儀表板</title>
<style>
:root{color-scheme:dark;--bg:#0a101c;--panel:#141e2d;--line:#2a3a50;--muted:#9eafc7;--text:#e9f2ff;--a:#77b9ff;--good:#68e2b0;--bad:#ff9297}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Noto Sans TC",sans-serif;margin:0}
header,main{max-width:1320px;margin:auto;padding:18px 14px}
header{padding-bottom:0}
h1{font-size:24px;margin:0 0 8px}h2{font-size:18px;margin:0 0 12px}h3{font-size:15px;margin:14px 0 9px}
p,.muted{color:var(--muted);font-size:13px;line-height:1.65}
.row{display:flex;gap:9px;flex-wrap:wrap;align-items:center}
.pill{border:1px solid #3b526f;border-radius:20px;padding:5px 10px;color:#bed1eb;font-size:12px}
.panel,.metric{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:15px;min-width:0}
.section{margin-top:15px}
.columns{display:grid;grid-template-columns:1fr;gap:15px}
.metrics{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;margin:12px 0}
.metric .name{font-size:12px;color:var(--muted)}.metric .value{font-size:21px;font-weight:750;margin-top:4px;overflow-wrap:anywhere}
.good{color:var(--good)}.bad{color:var(--bad)}
.scroll{overflow:auto}
table{border-collapse:collapse;width:100%;font-size:12px}
th,td{padding:9px 8px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}
th{color:var(--muted);font-weight:500}
td.wrap{white-space:normal;min-width:180px}
.kicker{color:var(--a);font-size:12px;font-weight:700;letter-spacing:.08em}
.headline{display:flex;justify-content:space-between;align-items:center;gap:10px}
.empty{color:var(--muted)}
select{background:#1c293c;border:1px solid #4a607e;border-radius:8px;color:white;padding:8px}
.note{border-left:3px solid #6a94cb;padding-left:10px}
details summary{cursor:pointer;color:var(--a)}pre{font-size:11px;white-space:pre-wrap;word-break:break-word}
@media(min-width:850px){.columns{grid-template-columns:repeat(2,minmax(0,1fr))}.metrics{grid-template-columns:repeat(3,minmax(0,1fr))}}
@media(min-width:1180px){.columns.core{grid-template-columns:repeat(3,minmax(0,1fr))}}
</style>
</head>
<body>
<header>
<h1>R2 × R2-Fast × R3 交易研究儀表板</h1>
<div class="row"><span class="pill">PAPER ONLY</span><span class="pill">唯讀</span><span class="pill">60 秒更新</span><span class="pill">{{now}}</span></div>
<p>R2：完整執行感知核心；R2-Fast：FAST + FULL 雙路徑；R3：突破 Setup → Watchlist → Pullback / Retest → Re-acceleration Trigger。三版獨立統計，R3 不會看到雷達異動就直接進場。</p>
</header>
<main>

<section class="panel">
<div class="headline">
<h2>三核心總覽</h2>
<form method="get"><label class="muted">統計範圍
<select name="period" onchange="this.form.submit()">
<option value="all" {% if period=="all" %}selected{% endif %}>全部</option>
<option value="24h" {% if period=="24h" %}selected{% endif %}>最近 24 小時</option>
<option value="7d" {% if period=="7d" %}selected{% endif %}>最近 7 天</option>
</select></label></form>
</div>
<div class="scroll">
<table>
<tr><th>版本</th><th>總交易</th><th>已平倉</th><th>持倉</th><th>勝率</th><th>PF</th><th>已實現淨利</th><th>平均每單</th><th>最大回撤</th></tr>
{% for v in versions %}{% set s=stats[v] %}
<tr><td><strong>{{v}}</strong></td><td>{{s.total}}</td><td>{{s.closed_count}}</td><td>{{s.open_count}}</td><td>{{s.win_rate}}</td><td>{{s.pf}}</td><td class="{{'good' if s.net>=0 else 'bad'}}">{{money(s.net) if s.closed_count else '—'}}</td><td>{{money(s.avg) if s.closed_count else '—'}}</td><td>{{money(s.drawdown) if s.closed_count else '—'}}</td></tr>
{% endfor %}
</table>
</div>
</section>

<div class="columns core section">
{% for v in versions %}{% set s=stats[v] %}
<section class="panel">
<div class="headline"><div><div class="kicker">{{v}} CORE</div>
<h2>{{"執行感知核心" if v=="R2" else ("快速進場 · A/B 測試" if v=="R2F" else "突破回踩 · 再加速")}}</h2></div>
<span class="pill">{{s.total}} 筆</span></div>
<div class="metrics">
<div class="metric"><div class="name">勝率</div><div class="value">{{s.win_rate}}</div></div>
<div class="metric"><div class="name">PF</div><div class="value">{{s.pf}}</div></div>
<div class="metric"><div class="name">已實現淨利</div><div class="value {{'good' if s.net>=0 else 'bad'}}">{{money(s.net) if s.closed_count else '—'}}</div></div>
<div class="metric"><div class="name">已平倉 / 持倉</div><div class="value">{{s.closed_count}} / {{s.open_count}}</div></div>
<div class="metric"><div class="name">贏 / 輸 / 平</div><div class="value">{{s.wins}} / {{s.losses}} / {{s.flats}}</div></div>
<div class="metric"><div class="name">平均 Spread</div><div class="value">{{percent(s.avg_spread)}}</div></div>
<div class="metric"><div class="name">平均不利追價</div><div class="value">{{percent(s.avg_chase)}}</div></div>
<div class="metric"><div class="name">TP1 / TP2 / TP3</div><div class="value">{{s.tp1}} / {{s.tp2}} / {{s.tp3}}</div></div>
<div class="metric"><div class="name">診斷樣本</div><div class="value">{{s.diagnostic_count}}</div></div>
</div>

<h3>目前持倉</h3>
<div class="scroll"><table><tr><th>幣種</th><th>方向</th><th>策略</th><th>進場價</th><th>分數</th><th>持倉時間</th></tr>
{% for t in s.active %}<tr><td>{{t.symbol}}</td><td>{{t.side}}</td><td>{{t.strategy or '—'}}</td><td>{{price(t.entry_price)}}</td><td>{{t.score if t.score is not none else '—'}}</td><td>{{age(t.signal_time)}}</td></tr>
{% else %}<tr><td colspan="6" class="empty">目前沒有持倉</td></tr>{% endfor %}</table></div>

<h3>最近 20 筆平倉</h3>
<div class="scroll"><table><tr><th>幣種</th><th>方向</th><th>淨利 U</th><th>淨利 %</th><th>出場</th><th>詳細</th></tr>
{% for t in s.recent %}<tr><td>{{t.symbol}}</td><td>{{t.side}}</td><td class="{{'good' if t.sim_pnl_usdt is not none and t.sim_pnl_usdt>=0 else 'bad'}}">{{money(t.sim_pnl_usdt)}}</td><td>{{percent(t.net_result_pct)}}</td><td>{{t.exit_reason or '—'}}</td><td><details><summary>查看</summary><pre>進場 UTC：{{dt(t.signal_time)}}
平倉 UTC：{{dt(t.closed_at)}}
策略：{{t.strategy or '—'}}
進場價：{{price(t.entry_price)}}
出場價：{{price(t.exit_price)}}
TP1/TP2/TP3：{{t.tp1_hit}} / {{t.tp2_hit}} / {{t.tp3_hit}}
{% if t.diag %}Spread：{{percent(t.diag.spread_pct)}}
追價：{{percent(t.diag.chase_pct)}}
參考訊號價：{{price(t.diag.signal_price)}}{% endif %}</pre></details></td></tr>
{% else %}<tr><td colspan="6" class="empty">尚無已平倉交易</td></tr>{% endfor %}
</table></div>
</section>
{% endfor %}
</div>

<div class="columns section">
<section class="panel">
<h2>R2 最新決策</h2>
<div class="metrics"><div class="metric"><div class="name">決策總數</div><div class="value">{{r2_total}}</div></div><div class="metric"><div class="name">ENTER</div><div class="value">{{r2_enter}}</div></div><div class="metric"><div class="name">未進場</div><div class="value">{{r2_total-r2_enter}}</div></div></div>
<div class="scroll"><table><tr><th>UTC</th><th>幣種</th><th>方向</th><th>Regime</th><th>D</th><th>M</th><th>E</th><th>F</th><th>Spread</th><th>追價</th><th>決策</th></tr>
{% for d in r2_decisions %}<tr><td>{{dt(d.observed_at)}}</td><td>{{d.symbol}}</td><td>{{d.side}}</td><td>{{d.regime}}</td><td>{{number(d.direction_score)}}</td><td>{{number(d.micro_score)}}</td><td>{{number(d.execution_score)}}</td><td>{{number(d.forward_score)}}</td><td>{{percent(d.spread_pct)}}</td><td>{{percent(d.chase_pct)}}</td><td>{{d.decision}}</td></tr>
{% else %}<tr><td colspan="11" class="empty">尚無 R2 決策</td></tr>{% endfor %}</table></div>
</section>

<section class="panel">
<h2>R2-Fast 最新決策</h2>
<div class="metrics"><div class="metric"><div class="name">決策總數</div><div class="value">{{r2f_total}}</div></div><div class="metric"><div class="name">ENTER</div><div class="value">{{r2f_enter}}</div></div><div class="metric"><div class="name">平均耗時</div><div class="value">{{latency_ms(r2f_latency)}}</div></div></div>
<div class="scroll"><table><tr><th>UTC</th><th>幣種</th><th>路徑</th><th>D</th><th>M</th><th>Exec</th><th>Fast</th><th>Spread</th><th>追價</th><th>決策</th></tr>
{% for d in r2f_decisions %}<tr><td>{{dt(d.observed_at)}}</td><td>{{d.symbol}}</td><td>{{d.path}}</td><td>{{number(d.direction_score)}}</td><td>{{number(d.micro_score)}}</td><td>{{number(d.execution_score)}}</td><td>{{number(d.fast_score)}}</td><td>{{percent(d.spread_pct)}}</td><td>{{percent(d.chase_pct)}}</td><td>{{d.decision}}</td></tr>
{% else %}<tr><td colspan="10" class="empty">尚無 R2-Fast 決策</td></tr>{% endfor %}</table></div>
</section>
</div>

<div class="columns section">
<section class="panel">
<div class="headline"><div><div class="kicker">R3 WATCHLIST</div><h2>Setup 狀態機</h2></div><span class="pill">{{r3_watch_total}} 累計</span></div>
<div class="metrics">
<div class="metric"><div class="name">Active</div><div class="value">{{r3_watch_active}}</div></div>
<div class="metric"><div class="name">Pullback</div><div class="value">{{r3_pullback_active}}</div></div>
<div class="metric"><div class="name">Entered</div><div class="value">{{r3_entered}}</div></div>
</div>
<div class="scroll"><table><tr><th>建立 UTC</th><th>幣種</th><th>方向</th><th>Stage</th><th>Status</th><th>Setup</th><th>Reference</th><th>Pullback Extreme</th><th>原因</th></tr>
{% for w in r3_watches %}<tr><td>{{dt(w.created_at)}}</td><td>{{w.symbol}}</td><td>{{w.side}}</td><td>{{w.stage}}</td><td>{{w.status}}</td><td>{{price(w.setup_price)}}</td><td>{{price(w.reference_level)}}</td><td>{{price(w.pullback_extreme)}}</td><td class="wrap">{{w.terminal_reason or '—'}}</td></tr>
{% else %}<tr><td colspan="9" class="empty">尚無 R3 Watchlist Setup</td></tr>{% endfor %}</table></div>
</section>

<section class="panel">
<div class="headline"><div><div class="kicker">R3 DECISIONS</div><h2>Setup / Pullback / Trigger</h2></div><span class="pill">{{r3_decision_total}} 決策</span></div>
<div class="scroll"><table><tr><th>UTC</th><th>幣種</th><th>方向</th><th>Stage</th><th>決策</th><th>Strength</th><th>市場</th><th>Pullback</th><th>Spread</th><th>追價</th><th>原因</th></tr>
{% for d in r3_decisions %}<tr><td>{{dt(d.observed_at)}}</td><td>{{d.symbol}}</td><td>{{d.side}}</td><td>{{d.stage or '—'}}</td><td>{{d.decision}}</td><td>{{number(d.setup_strength)}}</td><td>{{d.market_alignment if d.market_alignment is not none else '—'}}</td><td>{{percent(d.pullback_pct)}}</td><td>{{percent(d.spread_pct)}}</td><td>{{percent(d.chase_pct)}}</td><td class="wrap">{{d.reason or '—'}}</td></tr>
{% else %}<tr><td colspan="11" class="empty">等待 R3 產生候選</td></tr>{% endfor %}</table></div>
</section>
</div>

<section class="panel section">
<div class="headline"><div><div class="kicker">SHADOW RESEARCH</div><h2>影子策略總覽</h2></div><span class="pill">不下單 · 不占倉</span></div>
<p class="muted">R2 / R2-Fast 追蹤未進場決策；R3 追蹤 Setup 拒絕、Watch 過期/失效，以及 Trigger 後被風控、名額或冷卻擋下的訊號。統一記錄 1m / 3m / 5m / 10m、MFE、MAE、TP1/SL。</p>
<div class="scroll"><table><tr><th>版本</th><th>影子筆數</th><th>完成 10m</th><th>待完成</th><th>1m 平均</th><th>3m 平均</th><th>5m 平均</th><th>10m 平均</th><th>10m 正收益率</th><th>MFE</th><th>MAE</th><th>TP1</th><th>SL</th></tr>
{% for v in versions %}{% set sh=shadow_stats[v] %}
<tr><td><strong>{{v}}</strong></td><td>{{sh.total}}</td><td>{{sh.completed}}</td><td>{{sh.pending}}</td><td>{{percent(sh.avg_1m)}}</td><td>{{percent(sh.avg_3m)}}</td><td>{{percent(sh.avg_5m)}}</td><td class="{{'good' if sh.avg_10m is not none and sh.avg_10m>=0 else 'bad' if sh.avg_10m is not none else ''}}">{{percent(sh.avg_10m)}}</td><td>{{sh.positive_10m_rate}}</td><td class="good">{{percent(sh.avg_mfe)}}</td><td class="bad">{{percent(sh.avg_mae)}}</td><td>{{sh.tp1_hits}}</td><td>{{sh.sl_hits}}</td></tr>
{% endfor %}
</table></div>
</section>

<section class="panel section">
<h2>最新 60 筆影子訊號</h2>
<div class="scroll"><table><tr><th>UTC</th><th>版本</th><th>幣種</th><th>方向</th><th>決策</th><th>原因</th><th>1m</th><th>3m</th><th>5m</th><th>10m</th><th>MFE</th><th>MAE</th><th>先碰</th><th>狀態</th></tr>
{% for x in shadow_recent %}<tr><td>{{dt(x.observed_at)}}</td><td><strong>{{x.engine_version}}</strong></td><td>{{x.symbol}}</td><td>{{x.side}}</td><td>{{x.decision}}</td><td class="wrap">{{x.reason or '—'}}</td><td>{{percent(x.return_1m_pct)}}</td><td>{{percent(x.return_3m_pct)}}</td><td>{{percent(x.return_5m_pct)}}</td><td class="{{'good' if x.return_10m_pct is not none and x.return_10m_pct>=0 else 'bad' if x.return_10m_pct is not none else ''}}">{{percent(x.return_10m_pct)}}</td><td>{{percent(x.mfe_10m_pct)}}</td><td>{{percent(x.mae_10m_pct)}}</td><td>{{x.first_barrier_10m or '—'}}</td><td>{{'完成' if x.completed_at else '等待'}}</td></tr>
{% else %}<tr><td colspan="14" class="empty">尚無影子資料</td></tr>{% endfor %}
</table></div>
</section>

<p class="note">PF = 正獲利總和 ÷ 虧損絕對值總和。PnL 使用 scanner_trades.sim_pnl_usdt。R2 / R2F / R3 都是模擬研究，不代表交易所實際績效；目前 R3 第一階段刻意維持與 R2/R2F 相同的 TP 2/4/8%、SL 2.5%，先比較進場品質。</p>
</main></body></html>"""


def money(v):
    return "—" if v is None else f"{float(v):+.3f} U"

def percent(v):
    return "—" if v is None else f"{float(v):+.2f}%"

def number(v):
    return "—" if v is None else f"{float(v):.1f}"

def latency_ms(v):
    return "—" if v is None else f"{float(v):.0f} ms"

def price(v):
    if v is None:
        return "—"
    x = float(v)
    if abs(x) < 1:
        return f"{x:.8f}".rstrip("0").rstrip(".")
    return f"{x:.6f}".rstrip("0").rstrip(".")

def dt(v):
    return v.strftime("%m/%d %H:%M") if v else "—"

def age(v):
    if not v:
        return "—"
    m = max(0, int((datetime.now(timezone.utc) - v).total_seconds() // 60))
    return f"{m//60}h {m%60}m"

def calc_stats(trades):
    active = sorted([t for t in trades if t["status"] == "OPEN"], key=lambda x:x["signal_time"], reverse=True)
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
    diags = [t["diag"] for t in trades if t.get("diag")]
    spreads = [float(d["spread_pct"]) for d in diags if d.get("spread_pct") is not None]
    chases = [float(d["chase_pct"]) for d in diags if d.get("chase_pct") is not None]
    return {
        "total":len(trades),
        "open_count":len(active),
        "closed_count":len(closed),
        "wins":sum(x>0 for x in pnl),
        "losses":sum(x<0 for x in pnl),
        "flats":sum(x==0 for x in pnl),
        "win_rate":f"{sum(x>0 for x in pnl)/len(pnl)*100:.1f}%" if pnl else "—",
        "pf":f"{gains/losses:.2f}" if losses else ("∞" if gains else "—"),
        "net":net,
        "avg":net/len(pnl) if pnl else 0.0,
        "drawdown":dd,
        "tp1":sum(t.get("tp1_hit") is True for t in trades),
        "tp2":sum(t.get("tp2_hit") is True for t in trades),
        "tp3":sum(t.get("tp3_hit") is True for t in trades),
        "avg_spread":sum(spreads)/len(spreads) if spreads else None,
        "avg_chase":sum(chases)/len(chases) if chases else None,
        "diagnostic_count":len(diags),
        "active":active,
        "recent":list(reversed(closed))[:20],
    }

def calc_shadow(rows):
    completed = [r for r in rows if r.get("return_10m_pct") is not None]
    def avg(field):
        vals = [float(r[field]) for r in rows if r.get(field) is not None]
        return sum(vals)/len(vals) if vals else None
    return {
        "total":len(rows),
        "completed":len(completed),
        "pending":len(rows)-len(completed),
        "avg_1m":avg("return_1m_pct"),
        "avg_3m":avg("return_3m_pct"),
        "avg_5m":avg("return_5m_pct"),
        "avg_10m":avg("return_10m_pct"),
        "avg_mfe":avg("mfe_10m_pct"),
        "avg_mae":avg("mae_10m_pct"),
        "positive_10m_rate":f"{sum(float(r['return_10m_pct'])>0 for r in completed)/len(completed)*100:.1f}%" if completed else "—",
        "tp1_hits":sum(r.get("tp1_hit_10m") is True for r in rows),
        "sl_hits":sum(r.get("sl_hit_10m") is True for r in rows),
    }

@app.get("/health")
def health():
    return {"status":"ok","mode":"read_only","versions":list(VERSIONS)}

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

                cur.execute("""SELECT id,symbol,side,signal_time,entry_price,score,strategy,status,exit_reason,closed_at,exit_price,
                                     sim_pnl_usdt,net_result_pct,reasons,highest_price,lowest_price,tp1_hit,tp2_hit,tp3_hit,engine_version
                              FROM scanner_trades
                              WHERE engine_version IN ('R2','R2F','R3')
                                AND (%s::text IS NULL OR signal_time >= NOW() - (%s::text)::interval)
                              ORDER BY signal_time DESC LIMIT 30000""",(interval,interval))
                trades = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT id,observed_at,symbol,side,regime,direction_score,micro_score,execution_score,forward_score,
                                     signal_price,bid_price,ask_price,market_entry_price,spread_pct,chase_pct,decision,reason,scanner_trade_id
                              FROM r2_decisions
                              WHERE (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval)
                              ORDER BY observed_at DESC LIMIT 25000""",(interval,interval))
                r2_all = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT id,observed_at,symbol,side,path,direction_score,micro_score,market_score,execution_score,fast_score,
                                     signal_price,post_bid,post_ask,market_entry_price,spread_pct,chase_pct,analysis_latency_ms,decision,reason,scanner_trade_id
                              FROM r2_fast_decisions
                              WHERE (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval)
                              ORDER BY observed_at DESC LIMIT 25000""",(interval,interval))
                r2f_all = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT id,observed_at,symbol,side,stage,decision,reason,radar_move_pct,regime,setup_strength,market_alignment,
                                     setup_price,reference_level,pullback_pct,pullback_extreme,trigger_level,market_entry_price,
                                     spread_pct,chase_pct,scanner_trade_id,watchlist_id
                              FROM r3_decisions
                              WHERE (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval)
                              ORDER BY observed_at DESC LIMIT 25000""",(interval,interval))
                r3_all = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT id,symbol,side,stage,status,created_at,updated_at,expires_at,setup_price,setup_extreme,
                                     reference_level,pullback_extreme,trigger_level,radar_move_pct,regime,setup_strength,
                                     market_alignment,terminal_reason,scanner_trade_id
                              FROM r3_watchlist
                              WHERE (%s::text IS NULL OR created_at >= NOW() - (%s::text)::interval)
                              ORDER BY created_at DESC LIMIT 25000""",(interval,interval))
                r3_watches = [dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT id,engine_version,symbol,side,decision,reason,entry_price,signal_price,observed_at,
                                     return_1m_pct,return_3m_pct,return_5m_pct,return_10m_pct,mfe_10m_pct,mae_10m_pct,
                                     tp1_hit_10m,sl_hit_10m,first_barrier_10m,completed_at
                              FROM shadow_rejections
                              WHERE engine_version IN ('R2','R2F','R3')
                                AND (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval)
                              ORDER BY observed_at DESC LIMIT 30000""",(interval,interval))
                shadow_rows = [dict(x) for x in cur.fetchall()]

    except psycopg2.Error:
        app.logger.exception("Dashboard read failed")
        return Response("資料庫查詢失敗：請檢查 DATABASE_URL 與資料表", status=503)

    r2_by = {d["scanner_trade_id"]:d for d in r2_all if d.get("scanner_trade_id") is not None}
    r2f_by = {d["scanner_trade_id"]:d for d in r2f_all if d.get("scanner_trade_id") is not None}
    r3_by = {d["scanner_trade_id"]:d for d in r3_all if d.get("scanner_trade_id") is not None}

    by_version = {v:[] for v in VERSIONS}
    for t in trades:
        v = t["engine_version"]
        if v == "R2":
            d = r2_by.get(t["id"])
            sig = d.get("signal_price") if d else None
        elif v == "R2F":
            d = r2f_by.get(t["id"])
            sig = d.get("signal_price") if d else None
        else:
            d = r3_by.get(t["id"])
            sig = d.get("setup_price") if d else None
        t["diag"] = ({
            "spread_pct":d.get("spread_pct"),
            "chase_pct":d.get("chase_pct"),
            "signal_price":sig,
        } if d else None)
        by_version[v].append(t)

    stats = {v:calc_stats(by_version[v]) for v in VERSIONS}

    latency_vals = [float(d["analysis_latency_ms"]) for d in r2f_all if d.get("analysis_latency_ms") is not None]
    r2f_latency = sum(latency_vals)/len(latency_vals) if latency_vals else None

    shadow_by = {v:[] for v in VERSIONS}
    for r in shadow_rows:
        if r["engine_version"] in shadow_by:
            shadow_by[r["engine_version"]].append(r)
    shadow_stats = {v:calc_shadow(shadow_by[v]) for v in VERSIONS}

    r3_watch_active = sum(1 for w in r3_watches if w.get("status") == "ACTIVE")
    r3_pullback_active = sum(1 for w in r3_watches if w.get("status") == "ACTIVE" and w.get("stage") == "PULLBACK")
    r3_entered = sum(1 for w in r3_watches if w.get("status") == "ENTERED")

    return render_template_string(
        PAGE,
        versions=VERSIONS, stats=stats, period=period,
        now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        r2_decisions=r2_all[:40], r2_total=len(r2_all),
        r2_enter=sum(1 for d in r2_all if d.get("decision")=="ENTER"),
        r2f_decisions=r2f_all[:40], r2f_total=len(r2f_all),
        r2f_enter=sum(1 for d in r2f_all if d.get("decision")=="ENTER"),
        r2f_latency=r2f_latency,
        r3_decisions=r3_all[:40], r3_decision_total=len(r3_all),
        r3_watches=r3_watches[:40], r3_watch_total=len(r3_watches),
        r3_watch_active=r3_watch_active, r3_pullback_active=r3_pullback_active, r3_entered=r3_entered,
        shadow_stats=shadow_stats, shadow_recent=shadow_rows[:60],
        money=money, percent=percent, number=number, latency_ms=latency_ms,
        price=price, dt=dt, age=age,
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT","10000")))
