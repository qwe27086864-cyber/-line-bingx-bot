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
<section class="panel"><h2>最近 30 筆已平倉</h2><div class="scroll"><table><tr><th>幣種</th><th>方向</th><th>淨利 U</th><th>淨利 %</th><th>出場原因</th><th>診斷</th></tr>{% for t in recent %}<tr><td>{{t.symbol}}</td><td>{{t.side}}</td><td class="{{'green' if t.pnl_u is not none and t.pnl_u>=0 else 'red'}}">{{signed(t.pnl_u)}}</td><td>{{signed(t.net_pct)}}</td><td>{{t.exit_reason or '—'}}</td><td><details><summary>查看</summary><pre>進場評分：{{t.score if t.score is not none else '待收集'}}
策略：{{t.strategy or '—'}}
進場：{{dt(t.entry_time)}}
平倉：{{dt(t.exit_time)}}
最高價：{{fmt(t.highest_price,6)}}
最低價：{{fmt(t.lowest_price,6)}}
進場理由：{{t.reasons if t.reasons else '待收集'}}
市場環境：待收集
影子持倉：待收集</pre></details></td></tr>{% else %}<tr><td colspan="6">尚無平倉交易</td></tr>{% endfor %}</table></div></section>
{% for heading,rows in groups %}<section class="panel"><h2>{{heading}}</h2><div class="scroll"><table><tr><th>分類</th><th>筆數</th><th>勝率</th><th>PF</th><th>淨利 U</th></tr>{% for g in rows %}<tr><td>{{g.name}}</td><td>{{g.n}}</td><td>{{'%.1f%%'|format(g.win_rate) if g.has_usdt else '待收集'}}</td><td>{{g.pf if g.has_usdt else '待收集'}}</td><td class="{{'green' if g.net>=0 else 'red'}}">{{signed(g.net) if g.has_usdt else '待收集'}}</td></tr>{% else %}<tr><td colspan="5">無資料</td></tr>{% endfor %}</table></div></section>{% endfor %}
<section class="panel"><h2>交易診斷紀錄</h2><p>已可顯示：進場評分、策略、進出場時間、出場原因；V6 已有的進場理由及價格高低點也會顯示在平倉詳情。</p><p class="muted">市場環境變化、影子持倉、錯失機會與小幣橫盤診斷尚未接入交易紀錄；沒有資料時明確顯示「待收集」，不捏造統計。</p></section>
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
        return dict(id=d['id'],symbol=d['symbol'],side=d['side'],strategy=d['strategy'],score=d['score'],entry_price=d['entry_price'],entry_time=d['signal_time'],exit_time=d['closed_at'],status='OPEN' if d['status']=='OPEN' else 'CLOSED',exit_reason=d['exit_reason'],pnl_u=float(d['sim_pnl_usdt']) if d['sim_pnl_usdt'] is not None else None,net_pct=d['net_result_pct'],highest_price=None,lowest_price=None,reasons=None)
    return dict(id=d['id'],symbol=d['symbol'],side=d['side'],strategy=d['strategy'],score=d['score'],entry_price=d['entry_price'],entry_time=d['entry_time'],exit_time=d['exit_time'],status=d['status'],exit_reason=d['exit_reason'],pnl_u=None,net_pct=d['net_pct'],highest_price=d['highest_price'],lowest_price=d['lowest_price'],reasons=d['reasons'])

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
                    cur.execute('''SELECT id,symbol,side,strategy,score,entry_price,signal_time,closed_at,status,exit_reason,sim_pnl_usdt,net_result_pct FROM scanner_trades WHERE engine_version=%s AND (%s::timestamptz IS NULL OR signal_time >= %s::timestamptz) ORDER BY id DESC LIMIT 20000''',('V4.2R1',cutoff,cutoff))
                else:
                    cur.execute('''SELECT id,symbol,side,strategy,score,entry_price,entry_time,exit_time,status,exit_reason,net_pct,highest_price,lowest_price,reasons FROM v6_paper_trades WHERE (%s::timestamptz IS NULL OR entry_time >= %s::timestamptz) ORDER BY id DESC LIMIT 20000''',(cutoff,cutoff))
                trades=[normalize(r,version) for r in cur.fetchall()]
    except psycopg2.Error:
        app.logger.exception('Dashboard read query failed')
        return Response('資料庫查詢失敗，請確認 R1/V6 資料表存在',status=503)
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
    return render_template_string(PAGE,version=version,scope='同期新批次' if cutoff else '現有全部模擬紀錄',note='共同起跑時間尚未設定；目前顯示各版本既有資料。' if not cutoff else f'共同起跑 UTC：{cutoff.isoformat()}',now=datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC'),cards=cards,active=active,recent=closed[:30],groups=groups,fmt=fmt,dt=dt,age=age,signed=signed)

if __name__=='__main__':app.run(host='0.0.0.0',port=int(os.environ.get('PORT','10000')))
