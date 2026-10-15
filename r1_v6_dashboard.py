"""Read-only R1/V6 paper-trading dashboard. Does not place orders or mutate data."""
import os
from collections import defaultdict
from datetime import datetime, timezone
from flask import Flask, Response, render_template_string, request
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
PAGE = '''<!doctype html><html lang="zh-Hant"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta http-equiv="refresh" content="60"><title>R1 / V6 交易研究儀表板</title><style>
:root{color-scheme:dark}*{box-sizing:border-box}body{background:#0c101a;color:#edf3ff;font-family:-apple-system,BlinkMacSystemFont,"Noto Sans TC",sans-serif;margin:0}header,.wrap{max-width:1100px;margin:auto;padding:18px 14px}h1{font-size:24px;margin:0 0 8px}p,.muted{color:#9fb0ca;font-size:13px}.cards{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.card,.panel{background:#151d2b;border:1px solid #2c3950;border-radius:14px;padding:15px}.label{font-size:12px;color:#9fb0ca}.num{font-size:24px;font-weight:750;overflow-wrap:anywhere;margin-top:5px}.green{color:#68e5ad}.red{color:#ff8b91}.grid{display:grid;grid-template-columns:1fr;gap:12px;margin-top:14px}h2{font-size:17px;margin:0 0 14px}.scroll{overflow-x:auto}table{border-collapse:collapse;width:100%;font-size:12px}th,td{text-align:left;padding:10px 8px;border-bottom:1px solid #2c3950;white-space:nowrap}th{color:#9fb0ca}a{color:#9ac4ff}select{background:#1d293a;border:1px solid #52627b;color:white;padding:9px;border-radius:9px}.tag{display:inline-block;border:1px solid #52627b;border-radius:25px;padding:5px 10px}.wrap{padding-top:0}details{max-width:340px}summary{cursor:pointer;color:#a6c7ff}pre{white-space:pre-wrap;word-break:break-word;font-size:11px}@media(min-width:800px){.cards{grid-template-columns:repeat(4,1fr)}.grid{grid-template-columns:repeat(2,1fr)}}
</style></head><body><header><h1>交易研究儀表板</h1><p>SIMULATION ONLY · 唯讀 · 60 秒自動更新 · {{now}}</p><form method="get"><label>模擬版本　<select name="version" onchange="this.form.submit()"><option value="R1" {% if version=='R1' %}selected{% endif %}>R1</option><option value="V6" {% if version=='V6' %}selected{% endif %}>V6</option></select></label></form><p><span class="tag">非實盤</span> <span class="tag">{{version}}</span> <span class="tag">{{scope}}</span></p></header><div class="wrap"><div class="cards">{% for label,value,color in cards %}<div class="card"><div class="label">{{label}}</div><div class="num {{color}}">{{value}}</div></div>{% endfor %}</div><div class="grid">
<section class="panel"><h2>目前持倉</h2><div class="scroll"><table><tr><th>幣種</th><th>方向</th><th>進場</th><th>分數</th><th>持倉時間</th><th>開倉 UTC</th></tr>{% for t in active %}<tr><td>{{t.symbol}}</td><td>{{t.side}}</td><td>{{fmt(t.entry_price,6)}}</td><td>{{t.score if t.score is not none else '—'}}</td><td>{{age(t.entry_time)}}</td><td>{{dt(t.entry_time)}}</td></tr>{% else %}<tr><td colspan="6">目前沒有持倉</td></tr>{% endfor %}</table></div></section>
<section class="panel"><h2>TP 階段統計（新批次）</h2>
<p>統計範圍：新批次全部 R1 交易（包含持倉中）。旗標表示交易程式已標記觸發，不等於逐筆成交紀錄。</p>
<div class="cards">
<div><div class="label">TP1 觸發</div><div class="num">{{tp_stats.tp1}}</div></div>
<div><div class="label">TP2 觸發</div><div class="num">{{tp_stats.tp2}}</div></div>
<div><div class="label">TP3 觸發</div><div class="num">{{tp_stats.tp3}}</div></div>
<div><div class="label">有紀錄交易</div><div class="num">{{tp_stats.total}}</div></div>
</div>
<p class="muted">舊批次不納入。階段時間、各段實際平倉數量和分段獲利尚無完整事件紀錄，不能由旗標推算。</p>
<p class="muted">{{'R1 實際 TP 旗標' if version=='R1' else 'V6 影子 TP 旗標（只觀察、不分批平倉）'}}；不代表逐筆成交。</p>
</section>
<section class="panel"><h2>最近 30 筆已平倉</h2><div class="scroll"><table><tr><th>幣種</th><th>方向</th><th>淨利 U</th><th>淨利 %</th><th>出場原因</th><th>診斷</th></tr>{% for t in recent %}<tr><td>{{t.symbol}}</td><td>{{t.side}}</td><td class="{{'green' if t.pnl_u is not none and t.pnl_u>=0 else 'red'}}">{{signed(t.pnl_u)}}</td><td>{{signed(t.net_pct)}}</td><td>{{t.exit_reason or '—'}}</td><td><details><summary>查看</summary><pre>進場評分：{{t.score if t.score is not none else '待收集'}}
策略：{{t.strategy or '—'}}
進場：{{dt(t.entry_time)}}
平倉：{{dt(t.exit_time)}}
最新觀察：{{dt(t.diagnostic.observed_at) if t.diagnostic else "待收集"}}
觀察價：{{fmt(t.diagnostic.market_price,6) if t.diagnostic else "待收集"}}
BTC 24h：{{signed(t.diagnostic.btc_change_24h) if t.diagnostic else "待收集"}}%
ETH 24h：{{signed(t.diagnostic.eth_change_24h) if t.diagnostic else "待收集"}}%
幣種 24h：{{signed(t.diagnostic.symbol_change_24h) if t.diagnostic else "待收集"}}%
觀察快照：{{t.snapshot_count}} 筆
影子追蹤：{% if t.shadows %}{% for sh in t.shadows %}{{sh.checkpoint_hours}}h: {{signed(sh.hold_vs_exit_pct)}}%（實際觀察 {{dt(sh.observed_at)}}）; {% endfor %}{% else %}待收集{% endif %}
TP1：{{'已觸發' if t.tp1_hit else ('未觸發' if t.tp1_hit is not none else '待收集')}}（目標 {{fmt(t.tp1,6)}}）
TP2：{{'已觸發' if t.tp2_hit else ('未觸發' if t.tp2_hit is not none else '待收集')}}（目標 {{fmt(t.tp2,6)}}）
TP3：{{'已觸發' if t.tp3_hit else ('未觸發' if t.tp3_hit is not none else '待收集')}}（目標 {{fmt(t.tp3,6)}}）
最高價：{{fmt(t.highest_price,6)}}
最低價：{{fmt(t.lowest_price,6)}}
進場理由：{{t.reasons if t.reasons else '待收集'}}
市場環境分類：待收集（目前只有 BTC/ETH 24h 漲跌，不足以分類）</pre></details></td></tr>{% else %}<tr><td colspan="6">尚無平倉交易</td></tr>{% endfor %}</table></div></section>
{% for heading,rows in groups %}<section class="panel"><h2>{{heading}}</h2><div class="scroll"><table><tr><th>分類</th><th>筆數</th><th>勝率</th><th>PF</th><th>淨利 U</th></tr>{% for g in rows %}<tr><td>{{g.name}}</td><td>{{g.n}}</td><td>{{'%.1f%%'|format(g.win_rate) if g.has_usdt else '待收集'}}</td><td>{{g.pf if g.has_usdt else '待收集'}}</td><td class="{{'green' if g.net>=0 else 'red'}}">{{signed(g.net) if g.has_usdt else '待收集'}}</td></tr>{% else %}<tr><td colspan="5">無資料</td></tr>{% endfor %}</table></div></section>{% endfor %}
<section class="panel"><h2>診斷快照（最近 30 筆）</h2><div class="scroll"><table><tr><th>觀察 UTC</th><th>幣種</th><th>方向</th><th>狀態</th><th>浮動毛利 %</th><th>BTC 24h %</th><th>ETH 24h %</th><th>幣種 24h %</th><th>持倉分鐘</th></tr>{% for d in observations %}<tr><td>{{dt(d.observed_at)}}</td><td>{{d.symbol}}</td><td>{{d.side}}</td><td>{{d.status}}</td><td>{{signed(d.unrealized_gross_pct)}}</td><td>{{signed(d.btc_change_24h)}}</td><td>{{signed(d.eth_change_24h)}}</td><td>{{signed(d.symbol_change_24h)}}</td><td>{{fmt(d.holding_minutes,0)}}</td></tr>{% else %}<tr><td colspan="9">尚無診斷資料</td></tr>{% endfor %}</table></div></section><section class="panel"><h2>影子持倉（最近 30 筆）</h2><div class="scroll"><table><tr><th>幣種</th><th>觀察 UTC</th><th>平倉後</th><th>繼續持有差異 %</th></tr>{% for sh in shadow_rows %}<tr><td>{{sh.symbol}}</td><td>{{dt(sh.observed_at)}}</td><td>{{sh.checkpoint_hours}}h</td><td>{{signed(sh.hold_vs_exit_pct)}}</td></tr>{% else %}<tr><td colspan="4">尚無影子觀察</td></tr>{% endfor %}</table></div></section><section class="panel"><h2>診斷說明</h2><p>目前已接入持倉快照、BTC／ETH 與幣種 24h 漲跌、平倉後影子觀察。影子觀察價格是檢查當下價格，不保證精確等於平倉後 1/2/4 小時價格。</p><p class="muted">行情分類、錯失機會及小幣橫盤判斷仍待開發；V6 USDT 損益尚未入庫，不以百分比冒充 U。</p></section>
</div><p class="muted">R1 舊紀錄不刪除。{{note}} 模擬淨利只計已平倉交易；回撤不含未實現損益。V6 目前尚未儲存 USDT 金額，故不會把百分比誤當成 USDT。</p></div></body></html>'''

def fmt(value, digits=2):
    return '—' if value is None else f'{float(value):,.{digits}f}'
def signed(value):
    return '待收集' if value is None else f'{float(value):+.3f}'
def dt(value):
    return value.strftime('%m/%d %H:%M') if value else '—'
def age(value):
    if not value:return '—'
    minutes=max(0,int((datetime.now(timezone.utc)-value).total_seconds()//60))
    return f'{minutes//60}h {minutes%60}m'
def group_stats(trades, key):
    bins=defaultdict(list)
    for t in trades:bins[str(key(t) or 'UNKNOWN')].append(t)
    rows=[]
    for name,items in bins.items():
        pnl=[float(t['pnl_u']) for t in items if t['pnl_u'] is not None]
        gains=sum(max(0,x) for x in pnl);losses=-sum(min(0,x) for x in pnl)
        rows.append(dict(name=name,n=len(items),has_usdt=len(pnl)==len(items),win_rate=100*sum(x>0 for x in pnl)/len(pnl) if pnl else 0,pf=f'{gains/losses:.2f}' if losses else ('∞' if gains else '—'),net=sum(pnl)))
    return sorted(rows,key=lambda x:(-x['n'],x['name']))[:30]

def normalize(row,version):
    d=dict(row)
    if version=='R1':
        return dict(id=d['id'],symbol=d['symbol'],side=d['side'],strategy=d['strategy'],score=d['score'],entry_price=d['entry_price'],entry_time=d['signal_time'],exit_time=d['closed_at'],status='OPEN' if d['status']=='OPEN' else 'CLOSED',exit_reason=d['exit_reason'],pnl_u=float(d['sim_pnl_usdt']) if d['sim_pnl_usdt'] is not None else None,net_pct=d['net_result_pct'],highest_price=d.get('highest_price'),lowest_price=d.get('lowest_price'),reasons=d.get('reasons'),tp1_hit=d.get('tp1_hit'),tp2_hit=d.get('tp2_hit'),tp3_hit=d.get('tp3_hit'),tp1=d.get('tp1'),tp2=d.get('tp2'),tp3=d.get('tp3'),diagnostic=None,snapshot_count=0,shadows=[])
    return dict(id=d['id'],symbol=d['symbol'],side=d['side'],strategy=d['strategy'],score=d['score'],entry_price=d['entry_price'],entry_time=d['entry_time'],exit_time=d['exit_time'],status=d['status'],exit_reason=d['exit_reason'],pnl_u=None,net_pct=d['net_pct'],highest_price=d['highest_price'],lowest_price=d['lowest_price'],reasons=d['reasons'],tp1_hit=d.get('shadow_tp1_hit'),tp2_hit=d.get('shadow_tp2_hit'),tp3_hit=d.get('shadow_tp3_hit'),tp1=2.0,tp2=4.0,tp3=8.0,diagnostic=None,snapshot_count=0,shadows=[])

@app.get('/')
def home():
    if os.environ.get('LIVE_TRADING','false').lower()!='false':return Response('Simulation-only dashboard',status=503)
    url=os.environ.get('DATABASE_URL','')
    if not url:return Response('DATABASE_URL is missing',status=503)
    version=request.args.get('version','R1')
    if version not in ('R1','V6'):version='R1'
    # Optional UTC start time, shared by both versions; never delete earlier records.
    start=os.environ.get('COMPARE_START_UTC','').strip()
    cutoff=None
    if start:
        try:
            cutoff=datetime.fromisoformat(start.replace('Z','+00:00'))
            if cutoff.tzinfo is None:raise ValueError('timezone required')
        except ValueError:return Response('Invalid COMPARE_START_UTC: use ISO8601 UTC',status=503)
    try:
        with psycopg2.connect(url,sslmode='require',connect_timeout=8) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute('SET TRANSACTION READ ONLY')
                if version=='R1':
                    cur.execute('''SELECT id,symbol,side,strategy,score,entry_price,signal_time,closed_at,status,exit_reason,sim_pnl_usdt,net_result_pct,reasons,highest_price,lowest_price,tp1,tp2,tp3,tp1_hit,tp2_hit,tp3_hit FROM scanner_trades WHERE engine_version=%s AND (%s::timestamptz IS NULL OR signal_time >= %s::timestamptz) ORDER BY id DESC LIMIT 20000''',('V4.2R1',cutoff,cutoff))
                else:
                    cur.execute('''SELECT id,symbol,side,strategy,score,entry_price,entry_time,exit_time,status,exit_reason,net_pct,highest_price,lowest_price,reasons,shadow_tp1_hit,shadow_tp2_hit,shadow_tp3_hit FROM v6_paper_trades WHERE (%s::timestamptz IS NULL OR entry_time >= %s::timestamptz) ORDER BY id DESC LIMIT 20000''',(cutoff,cutoff))
                trades=[normalize(r,version) for r in cur.fetchall()]
                observations=[];shadow_rows=[]
                # Separate read-only diagnostic tables; missing tables are treated as not-yet-collected.
                cur.execute('SAVEPOINT diagnostic_read')
                try:
                    cur.execute('''SELECT version,trade_id,observed_at,symbol,side,status,market_price,
                        unrealized_gross_pct,btc_change_24h,eth_change_24h,symbol_change_24h,
                        holding_minutes FROM dual_trade_diagnostics
                        WHERE version=%s AND (%s::timestamptz IS NULL OR entry_time >= %s::timestamptz)
                        ORDER BY observed_at DESC LIMIT 10000''',(version,cutoff,cutoff))
                    rows=cur.fetchall()
                    latest={};counts=defaultdict(int)
                    for d in rows:
                        tid=d['trade_id'];counts[tid]+=1
                        if tid not in latest:latest[tid]=dict(d)
                    observations=rows[:30]
                    cur.execute('''SELECT sh.trade_id,sh.checkpoint_hours,sh.observed_at,
                        sh.hold_vs_exit_pct,t.symbol FROM dual_shadow_observations sh
                        JOIN dual_trade_diagnostics t ON t.version=sh.version AND t.trade_id=sh.trade_id
                        WHERE sh.version=%s AND (%s::timestamptz IS NULL OR t.entry_time >= %s::timestamptz)
                        GROUP BY sh.trade_id,sh.checkpoint_hours,sh.observed_at,sh.hold_vs_exit_pct,t.symbol
                        ORDER BY sh.observed_at DESC LIMIT 200''',(version,cutoff,cutoff))
                    all_shadows=cur.fetchall();shadow_rows=all_shadows[:30]
                    shadows=defaultdict(list)
                    for sh in all_shadows:shadows[sh['trade_id']].append(dict(sh))
                    for t in trades:
                        t['diagnostic']=latest.get(t['id'])
                        t['snapshot_count']=counts[t['id']]
                        t['shadows']=shadows[t['id']]
                except psycopg2.Error:
                    cur.execute('ROLLBACK TO SAVEPOINT diagnostic_read')
                    app.logger.warning('Diagnostic tables unavailable; dashboard will show pending diagnostics')
                finally:
                    cur.execute('RELEASE SAVEPOINT diagnostic_read')
    except psycopg2.Error:
        app.logger.exception('Dashboard read query failed')
        return Response('資料庫查詢失敗，請確認 R1/V6 資料表存在',status=503)
    tp_stats=dict(total=len(trades),tp1=sum(t['tp1_hit'] is True for t in trades),tp2=sum(t['tp2_hit'] is True for t in trades),tp3=sum(t['tp3_hit'] is True for t in trades))
    active=[t for t in trades if t['status']=='OPEN']
    closed=[t for t in trades if t['status']=='CLOSED' and t['exit_time'] is not None]
    closed.sort(key=lambda t:(t['exit_time'],t['id']),reverse=True)
    pnl=[t['pnl_u'] for t in closed if t['pnl_u'] is not None]
    complete=bool(pnl) and len(pnl)==len(closed)
    gains=sum(max(0,p) for p in pnl);losses=-sum(min(0,p) for p in pnl);net=sum(pnl)
    balance=1000;peak=1000;dd=0
    for t in reversed(closed):
        if t['pnl_u'] is None:continue
        balance+=t['pnl_u'];peak=max(peak,balance);dd=max(dd,peak-balance)
    pf=f'{gains/losses:.2f}' if losses else ('∞' if gains else '—')
    cards=[('已平倉',str(len(closed)),''),('目前持倉',str(len(active)),''),('勝率',f'{100*sum(x>0 for x in pnl)/len(pnl):.2f}%' if complete else '待收集',''),('PF',pf if complete else '待收集',''),('累計淨利 U',f'{net:+.2f} U' if complete else '待收集','green' if net>=0 else 'red'),('模擬帳面起始 1000 U',f'{1000+net:.2f} U' if complete else '待收集',''),('已實現 ROI',f'{net/10:+.2f}%' if complete else '待收集',''),('最大已平倉回撤',f'{dd:.2f} U' if complete else '待收集','red'),('贏 / 輸',f'{sum(x>0 for x in pnl)} / {sum(x<=0 for x in pnl)}' if complete else '待收集',''),('平均淨利/單',f'{net/len(pnl):+.3f} U' if complete else '待收集','')]
    groups=[('依方向',group_stats(closed,lambda t:t['side'])),('依策略',group_stats(closed,lambda t:t['strategy'])),('依出場原因',group_stats(closed,lambda t:t['exit_reason'])),('依幣種',group_stats(closed,lambda t:t['symbol'])),('依進場評分',group_stats(closed,lambda t:'90+' if (t['score'] or 0)>=90 else ('85–89' if (t['score'] or 0)>=85 else '<85')))]
    return render_template_string(PAGE,version=version,scope='同期新批次' if cutoff else '現有全部模擬紀錄',note='共同起跑時間尚未設定；目前顯示各版本既有資料。' if not cutoff else f'共同起跑 UTC：{cutoff.isoformat()}',now=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),cards=cards,active=active,recent=closed[:30],groups=groups,observations=observations,shadow_rows=shadow_rows,tp_stats=tp_stats,fmt=fmt,dt=dt,age=age,signed=signed)

if __name__=='__main__':app.run(host='0.0.0.0',port=int(os.environ.get('PORT','10000')))
