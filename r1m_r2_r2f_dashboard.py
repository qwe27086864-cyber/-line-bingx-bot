"""Read-only R1M vs R2 vs R2F research dashboard. No order execution or database writes."""
import os
from collections import defaultdict, Counter
from datetime import datetime, timezone
from flask import Flask, Response, render_template_string, request
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
VERSIONS = ('R1M', 'R2', 'R2F')

PAGE = r'''<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="60"><title>R1M × R2 × R2-Fast 模擬研究儀表板</title><style>
:root{color-scheme:dark;--bg:#0a101c;--panel:#141e2d;--line:#2a3a50;--muted:#9eafc7;--text:#e9f2ff;--a:#77b9ff;--good:#68e2b0;--bad:#ff9297}*{box-sizing:border-box}body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Noto Sans TC",sans-serif;margin:0}main,header{max-width:1250px;margin:auto;padding:18px 14px}header{padding-bottom:0}h1{font-size:24px;margin:0 0 8px}h2{font-size:18px;margin:0 0 13px}h3{font-size:15px;margin:0 0 10px}p,.muted{color:var(--muted);font-size:13px;line-height:1.65}.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center}.pill{border:1px solid #3b526f;border-radius:20px;padding:5px 10px;color:#bed1eb;font-size:12px}.columns{display:grid;grid-template-columns:1fr;gap:15px}.panel,.metric{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:15px;min-width:0}.metrics{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;margin:12px 0}.metric .name{font-size:12px;color:var(--muted)}.metric .value{font-size:21px;font-weight:750;margin-top:5px;overflow-wrap:anywhere}.good{color:var(--good)}.bad{color:var(--bad)}.section{margin-top:15px}.scroll{overflow:auto}table{border-collapse:collapse;width:100%;font-size:12px}th,td{padding:10px 8px;border-bottom:1px solid var(--line);text-align:left;white-space:nowrap}th{color:var(--muted);font-weight:500}td.wrapcell{white-space:normal;min-width:150px}details summary{cursor:pointer;color:var(--a)}pre{font-size:11px;white-space:pre-wrap;word-break:break-word}select{background:#1c293c;border:1px solid #4a607e;border-radius:8px;color:white;padding:9px}a{color:var(--a)}.empty{color:var(--muted);font-size:13px}.note{border-left:3px solid #6a94cb;padding-left:10px}.headline{display:flex;justify-content:space-between;align-items:center;gap:10px}.kicker{color:var(--a);font-size:12px;font-weight:700;letter-spacing:.08em}@media(min-width:860px){.columns{grid-template-columns:repeat(2,minmax(0,1fr))}.metrics{grid-template-columns:repeat(3,minmax(0,1fr))}}@media(min-width:1180px){.columns{grid-template-columns:repeat(3,minmax(0,1fr))}}
</style></head><body><header><h1>R1M × R2 × R2-Fast 交易研究儀表板</h1><div class="row"><span class="pill">PAPER ONLY</span><span class="pill">唯讀</span><span class="pill">每 60 秒更新</span><span class="pill">{{now}}</span></div><p>R1M：舊 R1 訊號 + 更貼近成交的進場；R2：完整執行感知核心；R2-Fast：先跑快速 1m/5m 進場判斷，不夠強才回完整 R2。三者只比較已平倉損益，持倉浮盈虧不包含在內。</p></header><main>
<div class="panel"><div class="headline"><h2>三核心總覽</h2><form method="get"><label class="muted">統計範圍 <select name="period" onchange="this.form.submit()"><option value="all" {% if period=='all' %}selected{% endif %}>全部</option><option value="24h" {% if period=='24h' %}selected{% endif %}>最近 24 小時</option><option value="7d" {% if period=='7d' %}selected{% endif %}>最近 7 天</option></select></label></form></div><div class="scroll"><table><tr><th>版本</th><th>總交易</th><th>已平倉</th><th>持倉</th><th>勝率</th><th>PF</th><th>已實現淨利</th><th>平均每單</th><th>最大已平倉回撤</th></tr>{% for v in versions %}{% set s=stats[v] %}<tr><td><strong>{{v}}</strong></td><td>{{s.total}}</td><td>{{s.closed_count}}</td><td>{{s.open_count}}</td><td>{{s.win_rate}}</td><td>{{s.pf}}</td><td class="{{'good' if s.net>=0 else 'bad'}}">{{money(s.net) if s.closed_count else '—'}}</td><td>{{money(s.avg) if s.closed_count else '—'}}</td><td>{{money(s.drawdown) if s.closed_count else '—'}}</td></tr>{% endfor %}</table></div></div>
<div class="columns section">{% for v in versions %}{% set s=stats[v] %}<section class="panel"><div class="headline"><div><div class="kicker">{{v}} CORE</div><h2>{{'真實成交條件基準' if v=='R1M' else ('新核心 · 執行感知' if v=='R2' else '快速進場 · A/B 測試')}}</h2></div><span class="pill">{{s.total}} 筆</span></div><div class="metrics"><div class="metric"><div class="name">勝率</div><div class="value">{{s.win_rate}}</div></div><div class="metric"><div class="name">PF</div><div class="value">{{s.pf}}</div></div><div class="metric"><div class="name">已實現淨利</div><div class="value {{'good' if s.net>=0 else 'bad'}}">{{money(s.net) if s.closed_count else '—'}}</div></div><div class="metric"><div class="name">已平倉 / 持倉</div><div class="value">{{s.closed_count}} / {{s.open_count}}</div></div><div class="metric"><div class="name">贏 / 輸 / 平</div><div class="value">{{s.wins}} / {{s.losses}} / {{s.flats}}</div></div><div class="metric"><div class="name">平均每筆淨利</div><div class="value">{{money(s.avg) if s.closed_count else '—'}}</div></div><div class="metric"><div class="name">TP1 / TP2 / TP3 觸發</div><div class="value">{{s.tp1}} / {{s.tp2}} / {{s.tp3}}</div></div><div class="metric"><div class="name">平均 Spread</div><div class="value">{{percent(s.avg_spread)}}</div></div><div class="metric"><div class="name">平均不利追價</div><div class="value">{{percent(s.avg_chase)}}</div></div></div>
<p class="muted">Spread / 追價僅統計有對應診斷資料的進場，樣本 {{s.diagnostic_count}} 筆。R1M 為 signal→entry 方向性偏差；R2 / R2-Fast 為 chase_pct。正值代表不利。</p>
<h3>目前持倉</h3><div class="scroll"><table><tr><th>幣種</th><th>方向</th><th>進場價</th><th>分數</th><th>持倉時間</th></tr>{% for t in s.active %}<tr><td>{{t.symbol}}</td><td>{{t.side}}</td><td>{{price(t.entry_price)}}</td><td>{{t.score if t.score is not none else '—'}}</td><td>{{age(t.signal_time)}}</td></tr>{% else %}<tr><td colspan="5" class="empty">目前沒有持倉</td></tr>{% endfor %}</table></div>
<h3 class="section">最近 20 筆平倉</h3><div class="scroll"><table><tr><th>幣種</th><th>方向</th><th>淨利 U</th><th>淨利 %</th><th>出場</th><th>詳細</th></tr>{% for t in s.recent %}<tr><td>{{t.symbol}}</td><td>{{t.side}}</td><td class="{{'good' if t.sim_pnl_usdt is not none and t.sim_pnl_usdt>=0 else 'bad'}}">{{money(t.sim_pnl_usdt)}}</td><td>{{percent(t.net_result_pct)}}</td><td>{{t.exit_reason or '—'}}</td><td><details><summary>查看</summary><pre>進場時間 UTC：{{dt(t.signal_time)}}
平倉時間 UTC：{{dt(t.closed_at)}}
策略：{{t.strategy or '—'}}
進場評分：{{t.score if t.score is not none else '—'}}
進場價格：{{price(t.entry_price)}}
出場價格：{{price(t.exit_price)}}
TP1 / TP2 / TP3：{{t.tp1_hit}} / {{t.tp2_hit}} / {{t.tp3_hit}}
最高 / 最低：{{price(t.highest_price)}} / {{price(t.lowest_price)}}
進場理由：{{t.reasons or '—'}}
{% if t.diag %}Spread：{{percent(t.diag.spread_pct)}}
追價：{{percent(t.diag.chase_pct)}}
訊號價：{{price(t.diag.signal_price)}}
Bid / Ask：{{price(t.diag.bid_price)}} / {{price(t.diag.ask_price)}}
{% endif %}</pre></details></td></tr>{% else %}<tr><td colspan="6" class="empty">尚無已平倉交易</td></tr>{% endfor %}</table></div>
<h3 class="section">依方向 / 策略 / 出場原因</h3><div class="scroll"><table><tr><th>類別</th><th>筆數</th><th>勝率</th><th>PF</th><th>淨利 U</th></tr>{% for g in s.groups %}<tr><td>{{g.name}}</td><td>{{g.count}}</td><td>{{g.win_rate}}</td><td>{{g.pf}}</td><td class="{{'good' if g.net>=0 else 'bad'}}">{{money(g.net)}}</td></tr>{% else %}<tr><td colspan="5" class="empty">尚無分類數據</td></tr>{% endfor %}</table></div></section>{% endfor %}</div>
<div class="columns section"><section class="panel"><h2>R1M 成交診斷（最近 30 筆）</h2><div class="scroll"><table><tr><th>時間 UTC</th><th>幣種</th><th>方向</th><th>訊號價</th><th>可成交價</th><th>Spread</th><th>不利追價</th><th>模式</th></tr>{% for d in r1m_diagnostics %}<tr><td>{{dt(d.created_at)}}</td><td>{{d.symbol}}</td><td>{{d.side}}</td><td>{{price(d.signal_price)}}</td><td>{{price(d.market_entry_price)}}</td><td>{{percent(d.spread_pct)}}</td><td>{{percent(d.signal_to_entry_pct)}}</td><td>{{d.entry_mode or '—'}}</td></tr>{% else %}<tr><td colspan="8" class="empty">尚無診斷資料</td></tr>{% endfor %}</table></div></section><section class="panel"><h2>R2 進場 / 拒絕統計</h2><div class="metrics"><div class="metric"><div class="name">已分析決策</div><div class="value">{{decision_total}}</div></div><div class="metric"><div class="name">實際進場</div><div class="value">{{decision_enter}}</div></div><div class="metric"><div class="name">拒絕 / 未進場</div><div class="value">{{decision_total-decision_enter}}</div></div></div><p class="muted">這是 R2 深度分析後記錄的決策數，不是所有市場掃描數；NO_SLOT / COOLDOWN / RISK_BLOCKED 也計入未進場。</p><div class="scroll"><table><tr><th>決策</th><th>次數</th></tr>{% for name,n in decision_counts %}<tr><td>{{name}}</td><td>{{n}}</td></tr>{% else %}<tr><td colspan="2" class="empty">尚無 R2 決策</td></tr>{% endfor %}</table></div></section></div>
<section class="panel section"><h2>R2 最新 40 筆決策與分數</h2><div class="scroll"><table><tr><th>時間 UTC</th><th>幣種</th><th>方向</th><th>Regime</th><th>Direction</th><th>Micro</th><th>Execution</th><th>Forward</th><th>Spread</th><th>追價</th><th>決策</th><th>原因</th></tr>{% for d in decisions %}<tr><td>{{dt(d.observed_at)}}</td><td>{{d.symbol}}</td><td>{{d.side}}</td><td>{{d.regime}}</td><td>{{number(d.direction_score)}}</td><td>{{number(d.micro_score)}}</td><td>{{number(d.execution_score)}}</td><td>{{number(d.forward_score)}}</td><td>{{percent(d.spread_pct)}}</td><td>{{percent(d.chase_pct)}}</td><td>{{d.decision}}</td><td class="wrapcell">{{d.reason or '—'}}</td></tr>{% else %}<tr><td colspan="12" class="empty">等待 R2 下一輪深度分析</td></tr>{% endfor %}</table></div></section>
<div class="columns section">
<section class="panel"><h2>R2-Fast 路徑統計</h2>
<div class="metrics">
<div class="metric"><div class="name">已記錄決策</div><div class="value">{{fast_total}}</div></div>
<div class="metric"><div class="name">成功進場</div><div class="value">{{fast_enter}}</div></div>
<div class="metric"><div class="name">平均分析耗時</div><div class="value">{{latency_ms(fast_avg_latency)}}</div></div>
<div class="metric"><div class="name">FAST 直接進場</div><div class="value">{{fast_direct_enter}}</div></div>
<div class="metric"><div class="name">FULL 回完整 R2</div><div class="value">{{fast_full_count}}</div></div>
<div class="metric"><div class="name">其他 / 拒絕</div><div class="value">{{fast_total-fast_enter}}</div></div>
</div>
<p class="muted">FAST = 快速 1m/5m 路徑；FULL = 快速條件不足後回完整 R2。平均分析耗時只統計有 latency 記錄的 FAST 判斷。</p>
<div class="scroll"><table><tr><th>決策</th><th>次數</th></tr>{% for name,n in fast_decision_counts %}<tr><td>{{name}}</td><td>{{n}}</td></tr>{% else %}<tr><td colspan="2" class="empty">等待 R2-Fast 決策資料</td></tr>{% endfor %}</table></div>
</section>
<section class="panel"><h2>R2-Fast 最新 30 筆執行診斷</h2>
<div class="scroll"><table><tr><th>時間 UTC</th><th>幣種</th><th>路徑</th><th>Direction</th><th>Micro</th><th>Execution</th><th>Fast</th><th>Spread</th><th>追價</th><th>耗時</th><th>決策</th></tr>{% for d in fast_decisions %}<tr><td>{{dt(d.observed_at)}}</td><td>{{d.symbol}}</td><td>{{d.path}}</td><td>{{number(d.direction_score)}}</td><td>{{number(d.micro_score)}}</td><td>{{number(d.execution_score)}}</td><td>{{number(d.fast_score)}}</td><td>{{percent(d.spread_pct)}}</td><td>{{percent(d.chase_pct)}}</td><td>{{latency_ms(d.analysis_latency_ms)}}</td><td>{{d.decision}}</td></tr>{% else %}<tr><td colspan="11" class="empty">等待 R2-Fast 下一輪分析</td></tr>{% endfor %}</table></div>
</section>
</div>
<p class="note">說明：R2 Forward Score 與 R2-Fast Fast Score 都是規則分數，不是經過校準的上漲機率。模擬損益依資料庫已記錄的 sim_pnl_usdt，含程式預設交易成本假設，不代表交易所實際結算；資金費未完整計入。PF = 正獲利總和 ÷ 虧損絕對值總和。最大回撤只依已平倉結果計算。所有資料唯讀，不會下單。</p>
</main></body></html>'''

def money(value):
    return '—' if value is None else f'{float(value):+.3f} U'

def percent(value):
    return '—' if value is None else f'{float(value):+.2f}%'

def number(value):
    return '—' if value is None else f'{float(value):.1f}'

def latency_ms(value):
    return '—' if value is None else f'{float(value):.0f} ms'

def price(value):
    if value is None: return '—'
    v = float(value)
    return f'{v:.8f}'.rstrip('0').rstrip('.') if abs(v) < 1 else f'{v:.6f}'.rstrip('0').rstrip('.')

def dt(value):
    return value.strftime('%m/%d %H:%M') if value else '—'

def age(value):
    if not value: return '—'
    minutes = max(0, int((datetime.now(timezone.utc) - value).total_seconds() // 60))
    return f'{minutes // 60}h {minutes % 60}m'

def stats_for(trades):
    active = [t for t in trades if t['status'] == 'OPEN']
    closed = sorted([t for t in trades if t['status'] != 'OPEN' and t['closed_at'] is not None], key=lambda t:(t['closed_at'],t['id']))
    pnl = [float(t['sim_pnl_usdt']) for t in closed if t['sim_pnl_usdt'] is not None]
    gains = sum(max(0,x) for x in pnl)
    losses_abs = -sum(min(0,x) for x in pnl)
    net = sum(pnl)
    bal=1000.0;peak=bal;dd=0.0
    for t in closed:
        if t['sim_pnl_usdt'] is None: continue
        bal += float(t['sim_pnl_usdt'])
        peak=max(peak,bal)
        dd=max(dd,peak-bal)
    diagnostics=[t['diag'] for t in trades if t.get('diag')]
    spreads=[float(d['spread_pct']) for d in diagnostics if d['spread_pct'] is not None]
    chases=[float(d['chase_pct']) for d in diagnostics if d['chase_pct'] is not None]
    groups=[]
    for label, key in [('方向','side'),('策略','strategy'),('出場','exit_reason')]:
        buckets=defaultdict(list)
        for t in closed:
            if t['sim_pnl_usdt'] is not None: buckets[f'{label} · {t.get(key) or "UNKNOWN"}'].append(float(t['sim_pnl_usdt']))
        for name,values in buckets.items():
            gp=sum(max(0,x) for x in values);gl=-sum(min(0,x) for x in values)
            groups.append(dict(name=name,count=len(values),win_rate=f'{sum(x>0 for x in values)/len(values)*100:.1f}%',pf=f'{gp/gl:.2f}' if gl else ('∞' if gp else '—'),net=sum(values)))
    groups.sort(key=lambda g:(-g['count'],g['name']))
    return dict(total=len(trades),open_count=len(active),closed_count=len(closed),wins=sum(x>0 for x in pnl),losses=sum(x<0 for x in pnl),flats=sum(x==0 for x in pnl),win_rate=f'{sum(x>0 for x in pnl)/len(pnl)*100:.1f}%' if pnl else '—',pf=f'{gains/losses_abs:.2f}' if losses_abs else ('∞' if gains else '—'),net=net,avg=net/len(pnl) if pnl else 0,drawdown=dd,tp1=sum(t['tp1_hit'] is True for t in trades),tp2=sum(t['tp2_hit'] is True for t in trades),tp3=sum(t['tp3_hit'] is True for t in trades),avg_spread=sum(spreads)/len(spreads) if spreads else None,avg_chase=sum(chases)/len(chases) if chases else None,diagnostic_count=len(diagnostics),active=sorted(active,key=lambda t:t['signal_time'],reverse=True),recent=list(reversed(closed))[:20],groups=groups[:35])

@app.get('/health')
def health():
    return {'status':'ok','mode':'read_only','versions':list(VERSIONS)}

@app.get('/')
def home():
    if os.getenv('LIVE_TRADING','false').lower() != 'false':
        return Response('Read-only dashboard requires LIVE_TRADING=false',status=503)
    url=os.getenv('DATABASE_URL','')
    if not url: return Response('DATABASE_URL is missing',status=503)
    period=request.args.get('period','all')
    if period not in ('all','24h','7d'): period='all'
    interval={'all':None,'24h':'24 hours','7d':'7 days'}[period]
    try:
        with psycopg2.connect(url,sslmode='require',connect_timeout=8) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute('SET TRANSACTION READ ONLY')
                cur.execute('''SELECT id,symbol,side,signal_time,entry_price,score,strategy,status,exit_reason,closed_at,exit_price,sim_pnl_usdt,net_result_pct,reasons,highest_price,lowest_price,tp1_hit,tp2_hit,tp3_hit,engine_version FROM scanner_trades WHERE engine_version IN ('R1M','R2','R2F') AND (%s::text IS NULL OR signal_time >= NOW() - (%s::text)::interval) ORDER BY signal_time DESC LIMIT 25000''',(interval,interval))
                rows=[dict(x) for x in cur.fetchall()]
                cur.execute('''SELECT trade_id,symbol,side,signal_price,market_entry_price,bid_price,ask_price,spread_pct,signal_to_entry_pct,entry_mode,created_at FROM r1m_entry_diagnostics WHERE (%s::text IS NULL OR created_at >= NOW() - (%s::text)::interval) ORDER BY created_at DESC LIMIT 25000''',(interval,interval))
                r1m_diagnostics=[dict(x) for x in cur.fetchall()]
                cur.execute('''SELECT id,observed_at,symbol,side,regime,direction_score,micro_score,execution_score,forward_score,signal_price,bid_price,ask_price,market_entry_price,spread_pct,chase_pct,decision,reason,scanner_trade_id FROM r2_decisions WHERE (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval) ORDER BY observed_at DESC LIMIT 25000''',(interval,interval))
                all_decisions=[dict(x) for x in cur.fetchall()]
                cur.execute('''SELECT id,observed_at,symbol,side,path,radar_move_pct,direction_score,micro_score,market_score,execution_score,fast_score,signal_price,pre_bid,pre_ask,post_bid,post_ask,market_entry_price,spread_pct,chase_pct,analysis_latency_ms,decision,reason,scanner_trade_id FROM r2_fast_decisions WHERE (%s::text IS NULL OR observed_at >= NOW() - (%s::text)::interval) ORDER BY observed_at DESC LIMIT 25000''',(interval,interval))
                all_fast_decisions=[dict(x) for x in cur.fetchall()]
    except psycopg2.Error:
        app.logger.exception('Dashboard read failed')
        return Response('資料庫查詢失敗：請檢查 DATABASE_URL 與資料表',status=503)
    by_version={v:[] for v in VERSIONS}
    r1m_by_id={d['trade_id']:d for d in r1m_diagnostics}
    r2_by_id={}
    for d in all_decisions:
        if d['scanner_trade_id'] is not None:
            r2_by_id.setdefault(d['scanner_trade_id'], d)
    r2f_by_id={}
    for d in all_fast_decisions:
        if d['scanner_trade_id'] is not None:
            r2f_by_id.setdefault(d['scanner_trade_id'], d)
    for t in rows:
        v=t['engine_version']
        if v=='R1M':
            d=r1m_by_id.get(t['id'])
        elif v=='R2':
            d=r2_by_id.get(t['id'])
        else:
            d=r2f_by_id.get(t['id'])
        if d:
            bid_value=d.get('bid_price') if v!='R2F' else d.get('post_bid')
            ask_value=d.get('ask_price') if v!='R2F' else d.get('post_ask')
            t['diag']={'spread_pct':d.get('spread_pct'),'chase_pct':d.get('signal_to_entry_pct') if v=='R1M' else d.get('chase_pct'),'signal_price':d.get('signal_price'),'bid_price':bid_value,'ask_price':ask_value}
        else:
            t['diag']=None
        by_version[v].append(t)
    stats={v:stats_for(by_version[v]) for v in VERSIONS}
    counts=Counter(d['decision'] for d in all_decisions)
    fast_counts=Counter(d['decision'] for d in all_fast_decisions)
    latency_values=[float(d['analysis_latency_ms']) for d in all_fast_decisions if d['analysis_latency_ms'] is not None]
    fast_avg_latency=sum(latency_values)/len(latency_values) if latency_values else None
    fast_enter=fast_counts.get('ENTER',0)
    fast_direct_enter=sum(1 for d in all_fast_decisions if d.get('path')=='FAST' and d.get('decision')=='ENTER')
    fast_full_count=sum(1 for d in all_fast_decisions if d.get('path')=='FULL')
    return render_template_string(PAGE,versions=VERSIONS,stats=stats,period=period,now=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),r1m_diagnostics=r1m_diagnostics[:30],decisions=all_decisions[:40],decision_total=len(all_decisions),decision_enter=counts.get('ENTER',0),decision_counts=counts.most_common(),fast_decisions=all_fast_decisions[:30],fast_total=len(all_fast_decisions),fast_enter=fast_enter,fast_direct_enter=fast_direct_enter,fast_full_count=fast_full_count,fast_avg_latency=fast_avg_latency,fast_decision_counts=fast_counts.most_common(),money=money,percent=percent,number=number,latency_ms=latency_ms,price=price,dt=dt,age=age)

if __name__ == '__main__':
    app.run(host='0.0.0.0',port=int(os.getenv('PORT','10000')))
