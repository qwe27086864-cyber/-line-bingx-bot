import os
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, Response, render_template_string, request

app = Flask(__name__)
DIRECTIONAL = ("R2F", "R3")

PAGE = r"""
<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>六核心交易研究儀表板</title>
<style>
:root{color-scheme:dark;--bg:#0d1117;--panel:#161b22;--line:#30363d;--muted:#8b949e;--txt:#e6edf3;--good:#3fb950;--bad:#f85149;--accent:#58a6ff;--warn:#d29922}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:1550px;margin:auto;padding:20px}.top{display:flex;justify-content:space-between;gap:15px;align-items:flex-end;flex-wrap:wrap}
.muted,.note{color:var(--muted)}h1{margin:0;font-size:28px}h2{margin:4px 0 14px;font-size:20px}h3{margin:18px 0 8px;font-size:16px}
.grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.two{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:12px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:15px;margin-top:12px}.kicker{font-size:12px;color:var(--accent);font-weight:700;letter-spacing:.08em}
.metrics{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.metric{border:1px solid var(--line);border-radius:9px;padding:10px}.name{font-size:12px;color:var(--muted)}
.value{font-size:20px;font-weight:700;margin-top:4px}.good{color:var(--good)}.bad{color:var(--bad)}.warn{color:var(--warn)}.scroll{overflow:auto}
table{border-collapse:collapse;width:100%;font-size:13px}th,td{padding:8px;border-bottom:1px solid var(--line);white-space:nowrap;text-align:left}th{color:var(--muted);font-weight:600}
.wrap{white-space:normal;min-width:180px}.pill{display:inline-block;padding:5px 9px;border:1px solid var(--line);border-radius:999px;color:var(--muted);text-decoration:none}
.controls a{margin-left:6px}.active{color:var(--txt);border-color:var(--accent)}.empty{color:var(--muted);text-align:center;padding:20px}.note{font-size:13px;line-height:1.6}
.headline{display:flex;justify-content:space-between;align-items:center;gap:10px}.mono{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
@media(max-width:1050px){.grid{grid-template-columns:repeat(2,minmax(0,1fr))}.two{grid-template-columns:1fr}}
@media(max-width:560px){main{padding:12px}.grid{grid-template-columns:1fr}.value{font-size:18px}}
</style></head><body><main>
<div class="top">
  <div><div class="kicker">PAPER ONLY · READ ONLY</div><h1>六核心交易研究儀表板</h1>
  <div class="muted">R2F · R3 · BF1 · AS1 · MEME1 · MEME2｜更新 {{now}}</div></div>
  <div class="controls"><a class="pill {{'active' if period=='all' else ''}}" href="/?period=all">全部</a><a class="pill {{'active' if period=='24h' else ''}}" href="/?period=24h">24h</a><a class="pill {{'active' if period=='7d' else ''}}" href="/?period=7d">7d</a></div>
</div>

<div class="grid">
{% for v in directional %}{% set s=stats[v] %}
<section class="panel"><div class="kicker">{{v}} CORE</div><h2>{{"快速方向基準" if v=="R2F" else "突破回踩 · 再加速"}}</h2>
<div class="metrics"><div class="metric"><div class="name">勝率</div><div class="value">{{s.win_rate}}</div></div><div class="metric"><div class="name">PF</div><div class="value">{{s.pf}}</div></div>
<div class="metric"><div class="name">淨利</div><div class="value {{'good' if s.net>=0 else 'bad'}}">{{money(s.net)}}</div></div><div class="metric"><div class="name">平倉 / 持倉</div><div class="value">{{s.closed_count}} / {{s.open_count}}</div></div></div></section>{% endfor %}

<section class="panel"><div class="kicker">BF1 CORE</div><h2>Basis + Funding 套利</h2><div class="metrics">
<div class="metric"><div class="name">掃描樣本</div><div class="value">{{bf.obs_count}}</div></div><div class="metric"><div class="name">目前套利倉</div><div class="value">{{bf.open_count}}</div></div>
<div class="metric"><div class="name">已平倉</div><div class="value">{{bf.closed_count}}</div></div><div class="metric"><div class="name">已實現淨利</div><div class="value {{'good' if bf.net>=0 else 'bad'}}">{{money(bf.net)}}</div></div></div></section>

<section class="panel"><div class="kicker">AS1 CORE</div><h2>Avellaneda–Stoikov 做市</h2><div class="metrics">
<div class="metric"><div class="name">Snapshots</div><div class="value">{{as1.snapshot_count}}</div></div><div class="metric"><div class="name">Fills</div><div class="value">{{as1.fill_count}}</div></div>
<div class="metric"><div class="name">Equity</div><div class="value">{{money_plain(as1.equity)}}</div></div><div class="metric"><div class="name">Net PnL</div><div class="value {{'good' if as1.net_pnl>=0 else 'bad'}}">{{money(as1.net_pnl)}}</div></div></div></section>

<section class="panel"><div class="kicker">MEME1 CORE</div><h2>Solana 新池金狗研究</h2><div class="metrics">
<div class="metric"><div class="name">觀察池</div><div class="value">{{meme1.obs_count}}</div></div><div class="metric"><div class="name">交易 / 持倉</div><div class="value">{{meme1.total_positions}} / {{meme1.open_count}}</div></div>
<div class="metric"><div class="name">勝率 / PF</div><div class="value">{{meme1.win_rate}} / {{meme1.pf}}</div></div><div class="metric"><div class="name">Net PnL</div><div class="value {{'good' if meme1.net>=0 else 'bad'}}">{{money(meme1.net)}}</div></div></div></section>

<section class="panel"><div class="kicker">MEME2 CORE</div><h2>聰明錢包影子跟單</h2><div class="metrics">
<div class="metric"><div class="name">監控錢包</div><div class="value">{{meme2.wallet_count}}</div></div><div class="metric"><div class="name">偵測訊號</div><div class="value">{{meme2.signal_count}}</div></div>
<div class="metric"><div class="name">交易 / 持倉</div><div class="value">{{meme2.total_positions}} / {{meme2.open_count}}</div></div><div class="metric"><div class="name">Net PnL</div><div class="value {{'good' if meme2.net>=0 else 'bad'}}">{{money(meme2.net)}}</div></div></div>
<div class="note" style="margin-top:10px">平均偵測延遲：{{seconds(meme2.avg_delay)}}｜勝率：{{meme2.win_rate}}｜PF：{{meme2.pf}}</div></section>
</div>

<div class="two">
<section class="panel"><div class="headline"><div><div class="kicker">BF1 SHADOW</div><h2>Edge 門檻影子持倉</h2></div><span class="pill">不影響主 BF1</span></div>
<div class="scroll"><table><tr><th>Variant</th><th>門檻</th><th>Open</th><th>Closed</th><th>勝率</th><th>PF</th><th>Net PnL</th><th>Funding</th></tr>
{% for x in bf_shadow %}<tr><td><strong>{{x.shadow_variant}}</strong></td><td>{{percent(x.min_net_edge_pct)}}</td><td>{{x.open_count}}</td><td>{{x.closed_count}}</td><td>{{x.win_rate}}</td><td>{{x.pf}}</td><td class="{{'good' if x.net>=0 else 'bad'}}">{{money(x.net)}}</td><td>{{money(x.funding)}}</td></tr>{% else %}<tr><td colspan="8" class="empty">等待 BF1 shadow table / position</td></tr>{% endfor %}</table></div></section>

<section class="panel"><div class="headline"><div><div class="kicker">AS1 SHADOW</div><h2>不同報價寬度比較</h2></div><span class="pill">同一份真實行情</span></div>
<div class="scroll"><table><tr><th>Variant</th><th>Half Spread</th><th>Buy/Sell</th><th>Fills</th><th>Inventory</th><th>Equity</th><th>Net PnL</th><th>Fees</th></tr>
{% for x in as_shadow %}<tr><td><strong>{{x.shadow_variant}}</strong></td><td>{{bpsfmt(x.half_spread_bps)}}</td><td>{{x.buy_fills}} / {{x.sell_fills}}</td><td>{{x.fill_count}}</td><td>{{number(x.inventory_base)}}</td><td>{{money_plain(x.equity_usdt)}}</td><td class="{{'good' if x.net_pnl_usdt>=0 else 'bad'}}">{{money(x.net_pnl_usdt)}}</td><td>{{money_plain(x.fees_usdt)}}</td></tr>{% else %}<tr><td colspan="8" class="empty">等待 AS1 shadow state</td></tr>{% endfor %}</table></div></section>
</div>

<div class="two">
<section class="panel"><div class="kicker">MEME1 RESEARCH</div><h2>最新新池判斷</h2>
<div class="scroll"><table><tr><th>UTC</th><th>Pool</th><th>Age</th><th>Liq</th><th>Score</th><th>Rug</th><th>Decision</th><th>Reason</th></tr>
{% for x in meme1_recent_obs %}<tr><td>{{dt(x.observed_at)}}</td><td class="wrap">{{x.pool_name or short(x.mint_address)}}</td><td>{{mins(x.pool_age_min)}}</td><td>{{usd(x.liquidity_usd)}}</td><td>{{num1(x.market_score)}}</td><td>{{num1(x.rug_score)}}</td><td>{{x.decision}}</td><td class="wrap">{{x.reason or '—'}}</td></tr>
{% else %}<tr><td colspan="8" class="empty">等待 MEME1 observation</td></tr>{% endfor %}</table></div>

<h3>最近 MEME1 Positions</h3>
<div class="scroll"><table><tr><th>ID</th><th>Pool</th><th>Status</th><th>開倉</th><th>Net</th><th>Return</th><th>Exit</th></tr>
{% for x in meme1_recent_pos %}<tr><td>#{{x.id}}</td><td>{{x.pool_name}}</td><td>{{x.status}}</td><td>{{dt(x.opened_at)}}</td><td class="{{'good' if (x.net_pnl_usdt or 0)>=0 else 'bad'}}">{{money(x.net_pnl_usdt)}}</td><td>{{percent(x.net_return_pct)}}</td><td>{{x.exit_reason or '—'}}</td></tr>
{% else %}<tr><td colspan="7" class="empty">尚無 MEME1 position</td></tr>{% endfor %}</table></div></section>

<section class="panel"><div class="kicker">MEME1 15S PATH</div><h2>最近持倉路徑快照</h2>
<div class="scroll"><table><tr><th>UTC</th><th>Pos</th><th>Price</th><th>Exec Exit</th><th>Liq</th><th>Impact</th><th>Raw ROI</th><th>Net ROI</th><th>Peak ROI</th><th>Now</th></tr>
{% for x in meme1_snapshots %}<tr><td>{{dtsec(x.observed_at)}}</td><td>#{{x.position_id}}</td><td>{{tinyprice(x.raw_price_usd)}}</td><td>{{tinyprice(x.executable_exit_price_usd)}}</td><td>{{usd(x.liquidity_usd)}}</td><td>{{bpsfmt(x.exit_execution_bps)}}</td><td>{{percent(x.raw_roi_pct)}}</td><td>{{percent(x.executable_net_roi_pct)}}</td><td>{{percent(x.peak_roi_pct)}}</td><td>{{x.exit_reason_now or 'HOLD'}}</td></tr>
{% else %}<tr><td colspan="10" class="empty">目前沒有 MEME1 持倉快照；有新持倉後每 15 秒寫入</td></tr>{% endfor %}</table></div></section>
</div>

<div class="two">
<section class="panel"><div class="kicker">MEME2 RESEARCH</div><h2>最新聰明錢包訊號</h2>
<div class="scroll"><table><tr><th>UTC</th><th>Wallet</th><th>Side</th><th>Mint</th><th>Delay</th><th>Price</th><th>Liq</th><th>Decision</th><th>Reason</th></tr>
{% for x in meme2_recent_signals %}<tr><td>{{dtsec(x.observed_at)}}</td><td class="mono">{{short(x.wallet)}}</td><td>{{x.side}}</td><td class="mono">{{short(x.mint_address)}}</td><td>{{seconds(x.detection_delay_sec)}}</td><td>{{tinyprice(x.detected_price_usd)}}</td><td>{{usd(x.detected_liquidity_usd)}}</td><td>{{x.decision}}</td><td class="wrap">{{x.reason or '—'}}</td></tr>
{% else %}<tr><td colspan="9" class="empty">等待 MEME2 新訊號</td></tr>{% endfor %}</table></div>

<h3>錢包表現</h3>
<div class="scroll"><table><tr><th>Wallet</th><th>Trades</th><th>Open</th><th>Win</th><th>PF</th><th>Net PnL</th><th>Avg Delay</th></tr>
{% for x in meme2_wallet_stats %}<tr><td class="mono">{{short(x.wallet)}}</td><td>{{x.trades}}</td><td>{{x.open_count}}</td><td>{{x.win_rate}}</td><td>{{x.pf}}</td><td class="{{'good' if x.net>=0 else 'bad'}}">{{money(x.net)}}</td><td>{{seconds(x.avg_delay)}}</td></tr>
{% else %}<tr><td colspan="7" class="empty">已有 5 個監控錢包；尚未累積可比較的跟單交易</td></tr>{% endfor %}</table></div></section>

<section class="panel"><div class="kicker">MEME2 15S PATH</div><h2>最近跟單持倉路徑快照</h2>
<div class="scroll"><table><tr><th>UTC</th><th>Pos</th><th>Wallet</th><th>Price</th><th>Exec Exit</th><th>Liq</th><th>Liq Age</th><th>Impact</th><th>Net ROI</th><th>Now</th></tr>
{% for x in meme2_snapshots %}<tr><td>{{dtsec(x.observed_at)}}</td><td>#{{x.position_id}}</td><td class="mono">{{short(x.source_wallet)}}</td><td>{{tinyprice(x.raw_price_usd)}}</td><td>{{tinyprice(x.executable_exit_price_usd)}}</td><td>{{usd(x.liquidity_usd)}}</td><td>{{seconds(x.liquidity_age_sec)}}</td><td>{{bpsfmt(x.exit_execution_bps)}}</td><td>{{percent(x.executable_net_roi_pct)}}</td><td>{{x.exit_reason_now or 'HOLD'}}</td></tr>
{% else %}<tr><td colspan="10" class="empty">目前沒有 MEME2 持倉快照；跟單開倉後每 15 秒寫入</td></tr>{% endfor %}</table></div>

<h3>最近 MEME2 Positions</h3>
<div class="scroll"><table><tr><th>ID</th><th>Wallet</th><th>Mint</th><th>Status</th><th>Delay</th><th>Net</th><th>Return</th><th>Exit</th></tr>
{% for x in meme2_recent_pos %}<tr><td>#{{x.id}}</td><td class="mono">{{short(x.source_wallet)}}</td><td class="mono">{{short(x.mint_address)}}</td><td>{{x.status}}</td><td>{{seconds(x.detection_delay_sec)}}</td><td class="{{'good' if (x.net_pnl_usdt or 0)>=0 else 'bad'}}">{{money(x.net_pnl_usdt)}}</td><td>{{percent(x.net_return_pct)}}</td><td>{{x.exit_reason or '—'}}</td></tr>
{% else %}<tr><td colspan="8" class="empty">尚無 MEME2 position</td></tr>{% endfor %}</table></div></section>
</div>

<div class="two">
<section class="panel"><div class="kicker">BF1 LIVE RESEARCH</div><h2>最新 Basis / Funding 掃描</h2><div class="scroll"><table><tr><th>UTC</th><th>幣種</th><th>Open Basis</th><th>Funding</th><th>預估淨 Edge</th><th>決策</th><th>原因</th></tr>
{% for x in bf_recent %}<tr><td>{{dt(x.observed_at)}}</td><td>{{x.symbol}}</td><td>{{percent(x.open_basis_pct)}}</td><td>{{rate_percent(x.funding_rate)}}</td><td>{{percent(x.expected_net_edge_pct)}}</td><td>{{x.decision}}</td><td class="wrap">{{x.reason or '—'}}</td></tr>{% else %}<tr><td colspan="7" class="empty">等待資料</td></tr>{% endfor %}</table></div></section>

<section class="panel"><div class="kicker">AS1 LIVE RESEARCH</div><h2>主做市狀態</h2>{% if as_state %}<div class="metrics"><div class="metric"><div class="name">Buy / Sell fills</div><div class="value">{{as_state.buy_fills}} / {{as_state.sell_fills}}</div></div><div class="metric"><div class="name">Inventory</div><div class="value">{{number(as_state.inventory_base)}} BTC</div></div><div class="metric"><div class="name">Cash</div><div class="value">{{money_plain(as_state.cash_usdt)}}</div></div><div class="metric"><div class="name">更新</div><div class="value" style="font-size:14px">{{dt(as_state.updated_at)}}</div></div></div>{% else %}<p class="empty">AS1 state 尚未建立</p>{% endif %}
<h3>最近主模擬 Fills</h3><div class="scroll"><table><tr><th>UTC</th><th>Side</th><th>Price</th><th>Notional</th><th>Fee</th><th>Inventory</th></tr>{% for x in as_fills %}<tr><td>{{dt(x.filled_at)}}</td><td>{{x.side}}</td><td>{{price(x.price)}}</td><td>{{money_plain(x.notional_usdt)}}</td><td>{{money_plain(x.maker_fee_usdt)}}</td><td>{{number(x.inventory_after)}}</td></tr>{% else %}<tr><td colspan="6" class="empty">尚無主 AS1 fill</td></tr>{% endfor %}</table></div></section>
</div>

<section class="panel"><h2>R2F / R3 影子研究</h2><div class="scroll"><table><tr><th>版本</th><th>影子筆數</th><th>完成10m</th><th>10m平均</th><th>正收益率</th><th>MFE</th><th>MAE</th><th>TP1</th><th>SL</th></tr>
{% for v in directional %}{% set sh=directional_shadow[v] %}<tr><td>{{v}}</td><td>{{sh.total}}</td><td>{{sh.completed}}</td><td>{{percent(sh.avg_10m)}}</td><td>{{sh.positive_rate}}</td><td>{{percent(sh.avg_mfe)}}</td><td>{{percent(sh.avg_mae)}}</td><td>{{sh.tp1}}</td><td>{{sh.sl}}</td></tr>{% endfor %}</table></div></section>

<p class="note">所有核心與影子皆為模擬研究。MEME1 / MEME2 的 market price 來自公開市場資料，但成交價仍是保守的 paper execution estimate，不代表真實錢包一定能以相同價格成交。</p>
</main></body></html>
"""


def f(v): return None if v is None else float(v)
def money(v): return "—" if v is None else f"{float(v):+.4f} U"
def money_plain(v): return "—" if v is None else f"{float(v):.4f} U"
def percent(v): return "—" if v is None else f"{float(v):+.3f}%"
def rate_percent(v): return "—" if v is None else f"{float(v)*100:+.4f}%"
def bpsfmt(v): return "—" if v is None else f"{float(v):.1f} bps"
def number(v): return "—" if v is None else f"{float(v):.6f}".rstrip("0").rstrip(".")
def num1(v): return "—" if v is None else f"{float(v):.1f}"
def mins(v): return "—" if v is None else f"{float(v):.0f}m"
def seconds(v): return "—" if v is None else f"{float(v):.1f}s"
def usd(v): return "—" if v is None else f"${float(v):,.0f}"
def price(v): return "—" if v is None else f"{float(v):.6f}".rstrip("0").rstrip(".")
def tinyprice(v):
    if v is None: return "—"
    x=float(v)
    if x==0: return "0"
    if abs(x)>=1: return f"{x:.6f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")
def dt(v): return v.strftime("%m/%d %H:%M") if v else "—"
def dtsec(v): return v.strftime("%m/%d %H:%M:%S") if v else "—"
def short(v):
    if not v: return "—"
    s=str(v)
    return s if len(s)<=14 else f"{s[:6]}…{s[-5:]}"


def calc_stats(trades):
    active=[t for t in trades if t["status"]=="OPEN"]
    closed=sorted([t for t in trades if t["status"]!="OPEN" and t.get("closed_at")],key=lambda x:(x["closed_at"],x["id"]))
    pnl=[float(t["sim_pnl_usdt"]) for t in closed if t.get("sim_pnl_usdt") is not None]
    gains=sum(max(0,x) for x in pnl); losses=-sum(min(0,x) for x in pnl); net=sum(pnl)
    return {"open_count":len(active),"closed_count":len(closed),"net":net,
            "win_rate":f"{sum(x>0 for x in pnl)/len(pnl)*100:.1f}%" if pnl else "—",
            "pf":f"{gains/losses:.2f}" if losses else ("∞" if gains else "—")}


def calc_position_stats(rows):
    opens=[x for x in rows if x["status"]=="OPEN"]
    closed=[x for x in rows if x["status"]!="OPEN" and x.get("net_pnl_usdt") is not None]
    pnl=[float(x["net_pnl_usdt"]) for x in closed]
    gains=sum(max(0,x) for x in pnl); losses=-sum(min(0,x) for x in pnl)
    return {
        "total_positions":len(rows),"open_count":len(opens),"closed_count":len(closed),
        "net":sum(pnl),
        "win_rate":f"{sum(x>0 for x in pnl)/len(pnl)*100:.1f}%" if pnl else "—",
        "pf":f"{gains/losses:.2f}" if losses else ("∞" if gains else "—"),
    }


def calc_dir_shadow(rows):
    done=[r for r in rows if r.get("return_10m_pct") is not None]
    def avg(field):
        vals=[float(r[field]) for r in rows if r.get(field) is not None]
        return sum(vals)/len(vals) if vals else None
    return {"total":len(rows),"completed":len(done),"avg_10m":avg("return_10m_pct"),
            "avg_mfe":avg("mfe_10m_pct"),"avg_mae":avg("mae_10m_pct"),
            "positive_rate":f"{sum(float(r['return_10m_pct'])>0 for r in done)/len(done)*100:.1f}%" if done else "—",
            "tp1":sum(r.get("tp1_hit_10m") is True for r in rows),
            "sl":sum(r.get("sl_hit_10m") is True for r in rows)}


def calc_wallet_stats(wallets, positions):
    grouped={w:[] for w in wallets}
    for p in positions:
        grouped.setdefault(p["source_wallet"],[]).append(p)
    out=[]
    for wallet, rows in grouped.items():
        closed=[x for x in rows if x["status"]!="OPEN" and x.get("net_pnl_usdt") is not None]
        pnl=[float(x["net_pnl_usdt"]) for x in closed]
        gains=sum(max(0,x) for x in pnl); losses=-sum(min(0,x) for x in pnl)
        delays=[float(x["detection_delay_sec"]) for x in rows if x.get("detection_delay_sec") is not None]
        out.append({
            "wallet":wallet,"trades":len(rows),"open_count":sum(x["status"]=="OPEN" for x in rows),
            "net":sum(pnl),
            "win_rate":f"{sum(x>0 for x in pnl)/len(pnl)*100:.1f}%" if pnl else "—",
            "pf":f"{gains/losses:.2f}" if losses else ("∞" if gains else "—"),
            "avg_delay":sum(delays)/len(delays) if delays else None,
        })
    return sorted(out,key=lambda x:(x["trades"],x["net"]),reverse=True)


def table_exists(cur,name):
    cur.execute("SELECT to_regclass(%s) AS regclass",(f"public.{name}",))
    row=cur.fetchone()
    return bool(row and row.get("regclass") is not None)


@app.get("/health")
def health():
    return {"status":"ok","mode":"read_only",
            "cores":["R2F","R3","BF1","AS1","MEME1","MEME2"],
            "shadows":["R2F_REJECTION","R3_REJECTION","BF1_EDGE","AS1_SPREAD","MEME1_PATH","MEME2_PATH"]}


@app.get("/")
def home():
    if os.getenv("LIVE_TRADING","false").lower()!="false":
        return Response("Read-only dashboard requires LIVE_TRADING=false",503)
    url=os.getenv("DATABASE_URL","")
    if not url:
        return Response("DATABASE_URL is missing",503)
    period=request.args.get("period","all")
    period=period if period in ("all","24h","7d") else "all"
    interval={"all":None,"24h":"24 hours","7d":"7 days"}[period]

    try:
        with psycopg2.connect(url,sslmode="require",connect_timeout=8) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET TRANSACTION READ ONLY")

                cur.execute("""SELECT id,symbol,side,signal_time,status,closed_at,sim_pnl_usdt,engine_version
                               FROM scanner_trades
                               WHERE engine_version IN ('R2F','R3')
                                 AND (%s::text IS NULL OR signal_time>=NOW()-(%s::text)::interval)
                               ORDER BY signal_time DESC LIMIT 30000""",(interval,interval))
                trades=[dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT engine_version,return_10m_pct,mfe_10m_pct,mae_10m_pct,tp1_hit_10m,sl_hit_10m
                               FROM shadow_rejections
                               WHERE engine_version IN ('R2F','R3')
                                 AND (%s::text IS NULL OR observed_at>=NOW()-(%s::text)::interval)
                               ORDER BY observed_at DESC LIMIT 30000""",(interval,interval))
                dshadow=[dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT observed_at,symbol,open_basis_pct,funding_rate,expected_net_edge_pct,decision,reason
                               FROM bf_observations
                               WHERE (%s::text IS NULL OR observed_at>=NOW()-(%s::text)::interval)
                               ORDER BY observed_at DESC LIMIT 5000""",(interval,interval))
                bf_obs=[dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT status,opened_at,net_pnl_usdt,funding_accrued_usdt
                               FROM bf_positions
                               WHERE (%s::text IS NULL OR opened_at>=NOW()-(%s::text)::interval)
                               ORDER BY opened_at DESC LIMIT 5000""",(interval,interval))
                bf_pos=[dict(x) for x in cur.fetchall()]

                cur.execute("""SELECT symbol,updated_at,cash_usdt,inventory_base,fees_usdt,buy_fills,sell_fills,last_mid,equity_usdt,net_pnl_usdt
                               FROM as_mm_state WHERE symbol='BTC-USDT' LIMIT 1""")
                row=cur.fetchone(); as_state=dict(row) if row else None

                cur.execute("""SELECT COUNT(*) c FROM as_mm_snapshots
                               WHERE (%s::text IS NULL OR observed_at>=NOW()-(%s::text)::interval)""",(interval,interval))
                as_snap_count=int(cur.fetchone()["c"])

                cur.execute("""SELECT filled_at,side,price,notional_usdt,maker_fee_usdt,inventory_after
                               FROM as_mm_fills
                               WHERE (%s::text IS NULL OR filled_at>=NOW()-(%s::text)::interval)
                               ORDER BY filled_at DESC LIMIT 5000""",(interval,interval))
                as_fills_all=[dict(x) for x in cur.fetchall()]

                bf_shadow=[]; bf_shadow_recent=[]
                if table_exists(cur,"bf_shadow_positions"):
                    cur.execute("""SELECT shadow_variant,min_net_edge_pct,status,opened_at,entry_expected_net_edge_pct,symbol,net_pnl_usdt,funding_accrued_usdt,exit_reason
                                   FROM bf_shadow_positions
                                   WHERE (%s::text IS NULL OR opened_at>=NOW()-(%s::text)::interval)
                                   ORDER BY opened_at DESC LIMIT 10000""",(interval,interval))
                    bs=[dict(x) for x in cur.fetchall()]; bf_shadow_recent=bs[:40]
                    variants={}
                    for x in bs:
                        v=variants.setdefault(x["shadow_variant"],{"shadow_variant":x["shadow_variant"],"min_net_edge_pct":f(x["min_net_edge_pct"]),"open_count":0,"closed_count":0,"pnl":[],"funding":0.0})
                        v["funding"]+=float(x.get("funding_accrued_usdt") or 0)
                        if x["status"]=="OPEN": v["open_count"]+=1
                        elif x.get("net_pnl_usdt") is not None:
                            v["closed_count"]+=1; v["pnl"].append(float(x["net_pnl_usdt"]))
                    for v in variants.values():
                        gains=sum(max(0,x) for x in v["pnl"]); losses=-sum(min(0,x) for x in v["pnl"])
                        v["net"]=sum(v["pnl"])
                        v["win_rate"]=f"{sum(x>0 for x in v['pnl'])/len(v['pnl'])*100:.1f}%" if v["pnl"] else "—"
                        v["pf"]=f"{gains/losses:.2f}" if losses else ("∞" if gains else "—")
                    bf_shadow=sorted(variants.values(),key=lambda x:x["min_net_edge_pct"] or 0)

                as_shadow=[]; as_shadow_fills=[]
                if table_exists(cur,"as_mm_shadow_state"):
                    cur.execute("""SELECT shadow_variant,half_spread_bps,buy_fills,sell_fills,inventory_base,equity_usdt,net_pnl_usdt,fees_usdt
                                   FROM as_mm_shadow_state WHERE symbol='BTC-USDT' ORDER BY half_spread_bps""")
                    as_shadow=[dict(x) for x in cur.fetchall()]
                    if table_exists(cur,"as_mm_shadow_fills"):
                        cur.execute("""SELECT filled_at,shadow_variant,side,price,notional_usdt,inventory_after
                                       FROM as_mm_shadow_fills
                                       WHERE (%s::text IS NULL OR filled_at>=NOW()-(%s::text)::interval)
                                       ORDER BY filled_at DESC LIMIT 5000""",(interval,interval))
                        allsf=[dict(x) for x in cur.fetchall()]; as_shadow_fills=allsf[:40]
                        counts={}
                        for x in allsf: counts[x["shadow_variant"]]=counts.get(x["shadow_variant"],0)+1
                        for x in as_shadow: x["fill_count"]=counts.get(x["shadow_variant"],0)
                    else:
                        for x in as_shadow: x["fill_count"]=0

                meme1_obs=[]; meme1_pos=[]; meme1_snapshots=[]
                meme1_agg={"obs_count":0,"total_positions":0,"open_count":0,"closed_count":0,"net":0.0,"wins":0,"gains":0.0,"losses":0.0}
                if table_exists(cur,"meme1_observations"):
                    cur.execute("""SELECT COUNT(*) AS obs_count
                                   FROM meme1_observations
                                   WHERE (%s::text IS NULL OR observed_at>=NOW()-(%s::text)::interval)""",(interval,interval))
                    meme1_agg["obs_count"]=int(cur.fetchone()["obs_count"])
                    cur.execute("""SELECT observed_at,pool_name,mint_address,pool_age_min,liquidity_usd,market_score,rug_score,decision,reason
                                   FROM meme1_observations
                                   WHERE (%s::text IS NULL OR observed_at>=NOW()-(%s::text)::interval)
                                   ORDER BY observed_at DESC LIMIT 40""",(interval,interval))
                    meme1_obs=[dict(x) for x in cur.fetchall()]
                if table_exists(cur,"meme1_positions"):
                    cur.execute("""SELECT COUNT(*) AS total_positions,
                                          COUNT(*) FILTER (WHERE status='OPEN') AS open_count,
                                          COUNT(*) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL) AS closed_count,
                                          COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL),0) AS net,
                                          COUNT(*) FILTER (WHERE status!='OPEN' AND net_pnl_usdt>0) AS wins,
                                          COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt>0),0) AS gains,
                                          COALESCE(-SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt<0),0) AS losses
                                   FROM meme1_positions
                                   WHERE (%s::text IS NULL OR opened_at>=NOW()-(%s::text)::interval)""",(interval,interval))
                    meme1_agg.update(dict(cur.fetchone()))
                    cur.execute("""SELECT id,pool_name,mint_address,status,opened_at,closed_at,net_pnl_usdt,net_return_pct,exit_reason
                                   FROM meme1_positions
                                   WHERE (%s::text IS NULL OR opened_at>=NOW()-(%s::text)::interval)
                                   ORDER BY opened_at DESC LIMIT 40""",(interval,interval))
                    meme1_pos=[dict(x) for x in cur.fetchall()]
                if table_exists(cur,"meme1_position_snapshots"):
                    cur.execute("""SELECT observed_at,position_id,raw_price_usd,executable_exit_price_usd,liquidity_usd,exit_execution_bps,
                                          raw_roi_pct,executable_net_roi_pct,peak_roi_pct,exit_reason_now
                                   FROM meme1_position_snapshots
                                   WHERE (%s::text IS NULL OR observed_at>=NOW()-(%s::text)::interval)
                                   ORDER BY observed_at DESC LIMIT 120""",(interval,interval))
                    meme1_snapshots=[dict(x) for x in cur.fetchall()]

                meme2_wallets=[]; meme2_signals=[]; meme2_pos=[]; meme2_snapshots=[]; meme2_wallet_stats=[]
                meme2_agg={"wallet_count":0,"signal_count":0,"avg_delay":None,"total_positions":0,"open_count":0,"closed_count":0,"net":0.0,"wins":0,"gains":0.0,"losses":0.0}
                if table_exists(cur,"meme2_wallet_state"):
                    cur.execute("SELECT wallet FROM meme2_wallet_state ORDER BY wallet")
                    meme2_wallets=[x["wallet"] for x in cur.fetchall()]
                    meme2_agg["wallet_count"]=len(meme2_wallets)
                if table_exists(cur,"meme2_signals"):
                    cur.execute("""SELECT COUNT(*) AS signal_count, AVG(detection_delay_sec) AS avg_delay
                                   FROM meme2_signals
                                   WHERE (%s::text IS NULL OR observed_at>=NOW()-(%s::text)::interval)""",(interval,interval))
                    x=dict(cur.fetchone()); meme2_agg["signal_count"]=int(x["signal_count"]); meme2_agg["avg_delay"]=f(x["avg_delay"])
                    cur.execute("""SELECT observed_at,wallet,side,mint_address,detection_delay_sec,detected_price_usd,detected_liquidity_usd,decision,reason
                                   FROM meme2_signals
                                   WHERE (%s::text IS NULL OR observed_at>=NOW()-(%s::text)::interval)
                                   ORDER BY observed_at DESC LIMIT 40""",(interval,interval))
                    meme2_signals=[dict(x) for x in cur.fetchall()]
                if table_exists(cur,"meme2_positions"):
                    cur.execute("""SELECT COUNT(*) AS total_positions,
                                          COUNT(*) FILTER (WHERE status='OPEN') AS open_count,
                                          COUNT(*) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL) AS closed_count,
                                          COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL),0) AS net,
                                          COUNT(*) FILTER (WHERE status!='OPEN' AND net_pnl_usdt>0) AS wins,
                                          COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt>0),0) AS gains,
                                          COALESCE(-SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt<0),0) AS losses
                                   FROM meme2_positions
                                   WHERE (%s::text IS NULL OR opened_at>=NOW()-(%s::text)::interval)""",(interval,interval))
                    meme2_agg.update(dict(cur.fetchone()))
                    cur.execute("""SELECT id,source_wallet,mint_address,status,opened_at,closed_at,detection_delay_sec,net_pnl_usdt,net_return_pct,exit_reason
                                   FROM meme2_positions
                                   WHERE (%s::text IS NULL OR opened_at>=NOW()-(%s::text)::interval)
                                   ORDER BY opened_at DESC LIMIT 40""",(interval,interval))
                    meme2_pos=[dict(x) for x in cur.fetchall()]
                    if table_exists(cur,"meme2_wallet_state"):
                        cur.execute("""SELECT w.wallet,
                                              COUNT(p.id) AS trades,
                                              COUNT(p.id) FILTER (WHERE p.status='OPEN') AS open_count,
                                              COUNT(p.id) FILTER (WHERE p.status!='OPEN' AND p.net_pnl_usdt IS NOT NULL) AS closed_count,
                                              COUNT(p.id) FILTER (WHERE p.status!='OPEN' AND p.net_pnl_usdt>0) AS wins,
                                              COALESCE(SUM(p.net_pnl_usdt) FILTER (WHERE p.status!='OPEN' AND p.net_pnl_usdt IS NOT NULL),0) AS net,
                                              COALESCE(SUM(p.net_pnl_usdt) FILTER (WHERE p.status!='OPEN' AND p.net_pnl_usdt>0),0) AS gains,
                                              COALESCE(-SUM(p.net_pnl_usdt) FILTER (WHERE p.status!='OPEN' AND p.net_pnl_usdt<0),0) AS losses,
                                              AVG(p.detection_delay_sec) AS avg_delay
                                       FROM meme2_wallet_state w
                                       LEFT JOIN meme2_positions p ON p.source_wallet=w.wallet
                                         AND (%s::text IS NULL OR p.opened_at>=NOW()-(%s::text)::interval)
                                       GROUP BY w.wallet
                                       ORDER BY trades DESC, net DESC""",(interval,interval))
                        for row in cur.fetchall():
                            x=dict(row); closed=int(x["closed_count"] or 0); wins=int(x["wins"] or 0)
                            gains=float(x["gains"] or 0); losses=float(x["losses"] or 0)
                            x["net"]=float(x["net"] or 0); x["trades"]=int(x["trades"] or 0); x["open_count"]=int(x["open_count"] or 0)
                            x["win_rate"]=f"{wins/closed*100:.1f}%" if closed else "—"
                            x["pf"]=f"{gains/losses:.2f}" if losses else ("∞" if gains else "—")
                            x["avg_delay"]=f(x["avg_delay"])
                            meme2_wallet_stats.append(x)
                if table_exists(cur,"meme2_position_snapshots"):
                    cur.execute("""SELECT observed_at,position_id,source_wallet,raw_price_usd,executable_exit_price_usd,liquidity_usd,liquidity_age_sec,
                                          exit_execution_bps,executable_net_roi_pct,exit_reason_now
                                   FROM meme2_position_snapshots
                                   WHERE (%s::text IS NULL OR observed_at>=NOW()-(%s::text)::interval)
                                   ORDER BY observed_at DESC LIMIT 120""",(interval,interval))
                    meme2_snapshots=[dict(x) for x in cur.fetchall()]

    except psycopg2.Error:
        app.logger.exception("Dashboard read failed")
        return Response("資料庫查詢失敗",503)

    byv={v:[] for v in DIRECTIONAL}
    for t in trades:
        if t["engine_version"] in byv: byv[t["engine_version"]].append(t)
    stats={v:calc_stats(byv[v]) for v in DIRECTIONAL}

    dsby={v:[] for v in DIRECTIONAL}
    for r in dshadow:
        if r["engine_version"] in dsby: dsby[r["engine_version"]].append(r)
    directional_shadow={v:calc_dir_shadow(dsby[v]) for v in DIRECTIONAL}

    bf_closed=[x for x in bf_pos if x["status"]!="OPEN" and x.get("net_pnl_usdt") is not None]
    bf={"obs_count":len(bf_obs),"open_count":sum(x["status"]=="OPEN" for x in bf_pos),
        "closed_count":len(bf_closed),"net":sum(float(x["net_pnl_usdt"]) for x in bf_closed)}

    as1={"snapshot_count":as_snap_count,"fill_count":len(as_fills_all),
         "equity":f(as_state["equity_usdt"]) if as_state else None,
         "net_pnl":f(as_state["net_pnl_usdt"]) if as_state else 0.0}

    def finalize_agg(x):
        closed=int(x.get("closed_count") or 0); wins=int(x.get("wins") or 0)
        gains=float(x.get("gains") or 0); losses=float(x.get("losses") or 0)
        x["total_positions"]=int(x.get("total_positions") or 0); x["open_count"]=int(x.get("open_count") or 0)
        x["closed_count"]=closed; x["net"]=float(x.get("net") or 0)
        x["win_rate"]=f"{wins/closed*100:.1f}%" if closed else "—"
        x["pf"]=f"{gains/losses:.2f}" if losses else ("∞" if gains else "—")
        return x

    meme1=finalize_agg(meme1_agg)
    meme2=finalize_agg(meme2_agg)

    return render_template_string(
        PAGE,
        directional=DIRECTIONAL,stats=stats,directional_shadow=directional_shadow,
        period=period,now=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        bf=bf,bf_recent=bf_obs[:40],bf_shadow=bf_shadow,bf_shadow_recent=bf_shadow_recent,
        as1=as1,as_state=as_state,as_fills=as_fills_all[:40],as_shadow=as_shadow,as_shadow_fills=as_shadow_fills,
        meme1=meme1,meme1_recent_obs=meme1_obs[:40],meme1_recent_pos=meme1_pos[:40],meme1_snapshots=meme1_snapshots,
        meme2=meme2,meme2_recent_signals=meme2_signals[:40],meme2_recent_pos=meme2_pos[:40],
        meme2_snapshots=meme2_snapshots,meme2_wallet_stats=meme2_wallet_stats,
        money=money,money_plain=money_plain,percent=percent,rate_percent=rate_percent,bpsfmt=bpsfmt,number=number,
        num1=num1,mins=mins,seconds=seconds,usd=usd,price=price,tinyprice=tinyprice,dt=dt,dtsec=dtsec,short=short
    )


if __name__=="__main__":
    app.run(host="0.0.0.0",port=int(os.getenv("PORT","10000")))
