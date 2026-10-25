import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, Response, render_template_string

app = Flask(__name__)

DATABASE_URL = os.getenv("DATABASE_URL", "")
MEME2_EXPECTED_WALLETS = int(os.getenv("MEME2_EXPECTED_WALLETS", "20"))
SYDNEY = ZoneInfo("Australia/Sydney")

PAGE = r"""
<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>核心交易儀表板</title>
<style>
:root{color-scheme:dark;--bg:#0d1117;--panel:#161b22;--line:#30363d;--text:#e6edf3;--muted:#8b949e;--good:#3fb950;--bad:#f85149;--warn:#d29922}
*{box-sizing:border-box} body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:980px;margin:auto;padding:16px} h1{font-size:25px;margin:0 0 4px}.sub{color:var(--muted);font-size:13px;margin-bottom:14px}
.summary{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px;margin-bottom:12px}.summary-card,.core{background:var(--panel);border:1px solid var(--line);border-radius:14px}.summary-card{padding:11px}.summary-card .label,.metric .label{color:var(--muted);font-size:11px}.summary-card .big{font-size:17px;font-weight:750;margin-top:5px;overflow-wrap:anywhere}
.cores{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:11px}.core{padding:14px;min-width:0}.head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}.name{font-size:19px;font-weight:800}.desc{color:var(--muted);font-size:12px;margin-top:3px}.badge{font-size:12px;font-weight:700;padding:5px 8px;border-radius:999px;border:1px solid var(--line);white-space:nowrap}.badge.good{color:var(--good)}.badge.bad{color:var(--bad)}.badge.warn{color:var(--warn)}
.metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:13px}.metric{min-width:0}.metric .value{font-size:17px;font-weight:750;margin-top:3px;overflow-wrap:anywhere}.pnl.good{color:var(--good)}.pnl.bad{color:var(--bad)}.foot{display:flex;justify-content:space-between;gap:10px;color:var(--muted);font-size:11px;border-top:1px solid var(--line);padding-top:10px;margin-top:11px}.note{margin-top:12px;color:var(--muted);font-size:11px;line-height:1.5}
@media(max-width:700px){main{padding:12px}.summary{grid-template-columns:repeat(2,minmax(0,1fr))}.cores{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(3,minmax(0,1fr))}}
</style>
</head>
<body>
<main>
  <h1>核心交易儀表板</h1>
  <div class="sub">只看決策用數據｜每 30 秒自動更新｜{{ now }}</div>
  <section class="summary">
    <div class="summary-card"><div class="label">目前最好</div><div class="big">{{ best }}</div></div>
    <div class="summary-card"><div class="label">目前最差</div><div class="big">{{ worst }}</div></div>
    <div class="summary-card"><div class="label">樣本不足</div><div class="big">{{ watch }}</div></div>
    <div class="summary-card"><div class="label">系統狀態</div><div class="big">{{ system }}</div></div>
  </section>
  <section class="cores">
  {% for c in cores %}
    <article class="core">
      <div class="head"><div><div class="name">{{ c.name }}</div><div class="desc">{{ c.desc }}</div></div><span class="badge {{ c.status_class }}">{{ c.status }}</span></div>
      <div class="metrics">
        <div class="metric"><div class="label">交易 / 持倉</div><div class="value">{{ c.trades }} / {{ c.open }}</div></div>
        <div class="metric"><div class="label">勝率</div><div class="value">{{ c.win_rate }}</div></div>
        <div class="metric"><div class="label">PF</div><div class="value">{{ c.pf }}</div></div>
        <div class="metric"><div class="label">Net PnL</div><div class="value pnl {{ 'good' if c.net > 0 else 'bad' if c.net < 0 else '' }}">{{ money(c.net) }}</div></div>
        <div class="metric"><div class="label">24h PnL</div><div class="value pnl {{ 'good' if c.pnl24 > 0 else 'bad' if c.pnl24 < 0 else '' }}">{{ money(c.pnl24) }}</div></div>
        <div class="metric"><div class="label">{{ c.extra_label }}</div><div class="value">{{ c.extra }}</div></div>
      </div>
      <div class="foot"><span>更新 {{ c.updated }}</span><span>{{ c.note }}</span></div>
    </article>
  {% endfor %}
  </section>
  <div class="note">MEME1X 只顯示 EXITABILITY_V2 新版資料；舊 MEME1 / V1 不混入主績效。MEME2 監控錢包只計最近 5 分鐘仍持續更新的錢包。</div>
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
    return "—" if v is None else f"{float(v):+.2f} U"


def pf_text(gains, losses):
    gains, losses = f(gains), f(losses)
    if losses > 0:
        return f"{gains/losses:.2f}"
    if gains > 0:
        return "∞"
    return "—"


def fmt_time(v):
    if not v:
        return "—"
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.astimezone(SYDNEY).strftime("%H:%M:%S")


def table_exists(cur, name):
    cur.execute("SELECT to_regclass(%s) AS regclass", (f"public.{name}",))
    r = cur.fetchone()
    return bool(r and r["regclass"])


def make_core(name, desc, closed, opens, wins, gains, losses, net, pnl24, updated,
              extra_label="樣本", extra=None, sample_count=None, forced_issue=None, note=""):
    closed, opens, wins = int(closed or 0), int(opens or 0), int(wins or 0)
    gains, losses, net, pnl24 = f(gains), f(losses), f(net), f(pnl24)
    sample = closed if sample_count is None else int(sample_count or 0)
    win_rate = f"{wins/closed*100:.1f}%" if closed else "—"
    pf = pf_text(gains, losses)

    if forced_issue:
        status, status_class = forced_issue, "bad"
    elif sample < 20:
        status, status_class = "樣本不足", "warn"
    elif net > 0 and (losses == 0 or gains / max(losses, 1e-12) >= 1.0):
        status, status_class = "正向", "good"
    elif net < 0:
        status, status_class = "負期望", "bad"
    else:
        status, status_class = "觀察", "warn"

    return {
        "name": name, "desc": desc, "trades": closed, "open": opens,
        "win_rate": win_rate, "pf": pf, "net": net, "pnl24": pnl24,
        "updated": fmt_time(updated), "status": status, "status_class": status_class,
        "extra_label": extra_label, "extra": extra if extra is not None else str(sample),
        "sample_count": sample, "note": note,
    }


@app.get("/health")
def health():
    return {"status": "ok", "mode": "simple_read_only", "cores": ["R2F", "R2FS", "R3", "AS1", "BF1", "MEME1X V2", "MEME2"]}


@app.get("/")
def home():
    if os.getenv("LIVE_TRADING", "false").lower() != "false":
        return Response("Dashboard requires LIVE_TRADING=false", 503)
    if not DATABASE_URL:
        return Response("DATABASE_URL is missing", 503)

    cores, system_issues = [], []
    try:
        with psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=8) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET TRANSACTION READ ONLY")

                # Directional cores: R2F / R2FS / R3
                cur.execute("""
                    SELECT engine_version,
                           COUNT(*) FILTER (WHERE status='OPEN') AS open_count,
                           COUNT(*) FILTER (WHERE status!='OPEN' AND sim_pnl_usdt IS NOT NULL) AS closed_count,
                           COUNT(*) FILTER (WHERE status!='OPEN' AND sim_pnl_usdt>0) AS wins,
                           COALESCE(SUM(sim_pnl_usdt) FILTER (WHERE status!='OPEN' AND sim_pnl_usdt>0),0) AS gains,
                           COALESCE(-SUM(sim_pnl_usdt) FILTER (WHERE status!='OPEN' AND sim_pnl_usdt<0),0) AS losses,
                           COALESCE(SUM(sim_pnl_usdt) FILTER (WHERE status!='OPEN' AND sim_pnl_usdt IS NOT NULL),0) AS net,
                           COALESCE(SUM(sim_pnl_usdt) FILTER (WHERE status!='OPEN' AND sim_pnl_usdt IS NOT NULL AND closed_at >= NOW()-INTERVAL '24 hours'),0) AS pnl24,
                           MAX(COALESCE(last_checked_at,closed_at,signal_time)) AS updated
                    FROM scanner_trades
                    WHERE engine_version IN ('R2F','R2FS','R3')
                    GROUP BY engine_version
                """)
                directional = {r["engine_version"]: dict(r) for r in cur.fetchall()}
                for eng, desc in (("R2F", "快速方向交易"), ("R2FS", "R2F 影子 · 進場過濾"), ("R3", "突破回踩 · 再加速")):
                    r = directional.get(eng, {})
                    cores.append(make_core(eng, desc, r.get("closed_count"), r.get("open_count"), r.get("wins"), r.get("gains"), r.get("losses"), r.get("net"), r.get("pnl24"), r.get("updated"), extra_label="已平倉", extra=str(int(r.get("closed_count") or 0))))

                # AS1
                if table_exists(cur, "as_mm_state"):
                    cur.execute("SELECT updated_at,inventory_base,buy_fills,sell_fills,net_pnl_usdt FROM as_mm_state WHERE symbol='BTC-USDT' LIMIT 1")
                    s = cur.fetchone()
                    if s:
                        s = dict(s)
                        fill_count = int(s["buy_fills"] or 0) + int(s["sell_fills"] or 0)
                        pnl24 = 0.0
                        if table_exists(cur, "as_mm_snapshots"):
                            cur.execute("SELECT net_pnl_usdt FROM as_mm_snapshots WHERE symbol='BTC-USDT' AND observed_at>=NOW()-INTERVAL '24 hours' ORDER BY observed_at ASC LIMIT 1")
                            old = cur.fetchone()
                            pnl24 = f(s["net_pnl_usdt"]) - f(old["net_pnl_usdt"]) if old else 0.0
                        inv = abs(f(s["inventory_base"]))
                        c = make_core("AS1", "BTC 做市 / 橫盤核心", fill_count, 1 if inv > 1e-10 else 0, 0, 0, 0, s["net_pnl_usdt"], pnl24, s["updated_at"], extra_label="Fills", extra=str(fill_count), sample_count=fill_count)
                        c["win_rate"], c["pf"] = "—", "—"
                        if fill_count >= 20:
                            c["status"], c["status_class"] = (("正向", "good") if c["net"] > 0 else ("負期望", "bad") if c["net"] < 0 else ("觀察", "warn"))
                        cores.append(c)

                # BF1
                if table_exists(cur, "bf_positions"):
                    cur.execute("""
                        SELECT COUNT(*) FILTER (WHERE status='OPEN') AS open_count,
                               COUNT(*) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL) AS closed_count,
                               COUNT(*) FILTER (WHERE status!='OPEN' AND net_pnl_usdt>0) AS wins,
                               COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt>0),0) AS gains,
                               COALESCE(-SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt<0),0) AS losses,
                               COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL),0) AS net,
                               COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL AND closed_at>=NOW()-INTERVAL '24 hours'),0) AS pnl24,
                               MAX(COALESCE(closed_at,opened_at)) AS updated
                        FROM bf_positions
                    """)
                    r = dict(cur.fetchone())
                    if table_exists(cur, "bf_observations"):
                        cur.execute("SELECT MAX(observed_at) AS t FROM bf_observations")
                        t = cur.fetchone()["t"]
                        if t: r["updated"] = t
                    cores.append(make_core("BF1", "Basis + Funding 套利", r["closed_count"], r["open_count"], r["wins"], r["gains"], r["losses"], r["net"], r["pnl24"], r["updated"], extra_label="已平倉", extra=str(int(r["closed_count"] or 0))))

                # MEME1X V2 only. Old MEME1 and V1 are intentionally excluded.
                if table_exists(cur, "meme1x_positions"):
                    cur.execute("""
                        SELECT COUNT(*) FILTER (WHERE status='OPEN') AS open_count,
                               COUNT(*) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL) AS closed_count,
                               COUNT(*) FILTER (WHERE status!='OPEN' AND net_pnl_usdt>0) AS wins,
                               COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt>0),0) AS gains,
                               COALESCE(-SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt<0),0) AS losses,
                               COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL),0) AS net,
                               COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL AND closed_at>=NOW()-INTERVAL '24 hours'),0) AS pnl24,
                               MAX(COALESCE(closed_at,opened_at)) AS updated
                        FROM meme1x_positions
                        WHERE variant='EXITABILITY_V2'
                    """)
                    r = dict(cur.fetchone())
                    cores.append(make_core("MEME1X V2", "Solana 新池 · 出場能力確認", r["closed_count"], r["open_count"], r["wins"], r["gains"], r["losses"], r["net"], r["pnl24"], r["updated"], extra_label="V2 已平倉", extra=str(int(r["closed_count"] or 0)), note="只計 EXITABILITY_V2"))

                # MEME2
                active_wallets, signal_count, avg_delay, last_wallet_update = 0, 0, None, None
                if table_exists(cur, "meme2_wallet_state"):
                    cur.execute("SELECT COUNT(*) FILTER (WHERE updated_at>=NOW()-INTERVAL '5 minutes') AS active,MAX(updated_at) AS updated FROM meme2_wallet_state")
                    w = dict(cur.fetchone()); active_wallets = int(w["active"] or 0); last_wallet_update = w["updated"]
                if table_exists(cur, "meme2_signals"):
                    cur.execute("SELECT COUNT(*) AS c,AVG(detection_delay_sec) FILTER (WHERE observed_at>=NOW()-INTERVAL '24 hours') AS avg_delay FROM meme2_signals")
                    sg = dict(cur.fetchone()); signal_count = int(sg["c"] or 0); avg_delay = f(sg["avg_delay"], None)
                if table_exists(cur, "meme2_positions"):
                    cur.execute("""
                        SELECT COUNT(*) FILTER (WHERE status='OPEN') AS open_count,
                               COUNT(*) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL) AS closed_count,
                               COUNT(*) FILTER (WHERE status!='OPEN' AND net_pnl_usdt>0) AS wins,
                               COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt>0),0) AS gains,
                               COALESCE(-SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt<0),0) AS losses,
                               COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL),0) AS net,
                               COALESCE(SUM(net_pnl_usdt) FILTER (WHERE status!='OPEN' AND net_pnl_usdt IS NOT NULL AND closed_at>=NOW()-INTERVAL '24 hours'),0) AS pnl24
                        FROM meme2_positions
                    """)
                    r = dict(cur.fetchone())
                    issue = None
                    if active_wallets < MEME2_EXPECTED_WALLETS:
                        issue = f"監控異常 {active_wallets}/{MEME2_EXPECTED_WALLETS}"; system_issues.append(issue)
                    elif avg_delay is not None and avg_delay > 30:
                        issue = "偵測延遲偏高"; system_issues.append(issue)
                    note = f"訊號 {signal_count}｜延遲 {avg_delay:.1f}s" if avg_delay is not None else f"訊號 {signal_count}"
                    cores.append(make_core("MEME2", "聰明錢包跟單", r["closed_count"], r["open_count"], r["wins"], r["gains"], r["losses"], r["net"], r["pnl24"], last_wallet_update, extra_label="監控錢包", extra=str(active_wallets), forced_issue=issue, note=note))

    except psycopg2.Error:
        app.logger.exception("Dashboard read failed")
        return Response("資料庫查詢失敗", 503)

    order = {"R2F":0,"R2FS":1,"R3":2,"AS1":3,"BF1":4,"MEME1X V2":5,"MEME2":6}
    cores.sort(key=lambda x: order.get(x["name"], 99))

    comparable = [c for c in cores if c["sample_count"] >= 20]
    if comparable:
        best_core = max(comparable, key=lambda x: x["net"]); worst_core = min(comparable, key=lambda x: x["net"])
        best, worst = f"{best_core['name']} {money(best_core['net'])}", f"{worst_core['name']} {money(worst_core['net'])}"
    else:
        best, worst = "樣本不足", "樣本不足"

    insufficient = [c["name"] for c in cores if c["sample_count"] < 20]
    watch = "、".join(insufficient) if insufficient else "無"
    system = "正常" if not system_issues else "、".join(system_issues)

    return render_template_string(PAGE, cores=cores, best=best, worst=worst, watch=watch, system=system,
                                  now=datetime.now(SYDNEY).strftime("%m/%d %H:%M:%S"), money=money)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
