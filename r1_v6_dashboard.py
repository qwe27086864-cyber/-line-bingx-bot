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

# 每次核心有重大修正，只要更新對應 Render env 的時間點即可。
# 儀表板會把該時間前後的資料完全分開，不再把舊績效混入目前判斷。
DEFAULT_REVISIONS = {
    "R2F": "2026-10-24T20:42:35Z",
    "R2FS": "2026-10-24T20:42:35Z",
    "R3": "2026-10-22T18:45:04Z",
    "AS1": "2026-10-24T11:09:19Z",
    "BF1": "2026-10-24T11:08:08Z",
    "MEME1X": "2026-10-25T15:33:40Z",
    "MEME2": "2026-10-25T21:59:28Z",
}

REVISION_ENV = {
    "R2F": "REVISION_R2F",
    "R2FS": "REVISION_R2FS",
    "R3": "REVISION_R3",
    "AS1": "REVISION_AS1",
    "BF1": "REVISION_BF1",
    "MEME1X": "REVISION_MEME1X",
    "MEME2": "REVISION_MEME2",
}

PAGE = r"""
<!doctype html>
<html lang="zh-Hant">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>核心交易儀表板</title>
<style>
:root{color-scheme:dark;--bg:#0d1117;--panel:#161b22;--panel2:#11161d;--line:#30363d;--text:#e6edf3;--muted:#8b949e;--good:#3fb950;--bad:#f85149;--warn:#d29922;--blue:#58a6ff}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
main{max-width:1040px;margin:auto;padding:16px}h1{font-size:25px;margin:0 0 4px}.sub{color:var(--muted);font-size:13px;margin-bottom:14px}
.summary{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:9px;margin-bottom:12px}.summary-card,.core{background:var(--panel);border:1px solid var(--line);border-radius:14px}.summary-card{padding:11px}.summary-card .label,.metric .label{color:var(--muted);font-size:11px}.summary-card .big{font-size:17px;font-weight:750;margin-top:5px;overflow-wrap:anywhere}
.cores{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:11px}.core{padding:14px;min-width:0}.head{display:flex;justify-content:space-between;align-items:flex-start;gap:10px}.name{font-size:19px;font-weight:800}.desc{color:var(--muted);font-size:12px;margin-top:3px}.revision{color:var(--blue);font-size:11px;font-weight:700;margin-top:6px}.badge{font-size:12px;font-weight:700;padding:5px 8px;border-radius:999px;border:1px solid var(--line);white-space:nowrap}.badge.good{color:var(--good)}.badge.bad{color:var(--bad)}.badge.warn{color:var(--warn)}
.section-tag{margin-top:12px;font-size:11px;font-weight:800;color:var(--good);letter-spacing:.04em}.metrics{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:7px}.metric{min-width:0}.metric .value{font-size:17px;font-weight:750;margin-top:3px;overflow-wrap:anywhere}.pnl.good{color:var(--good)}.pnl.bad{color:var(--bad)}
.compare{margin-top:12px;background:var(--panel2);border:1px solid var(--line);border-radius:10px;padding:9px 10px;display:grid;gap:6px;font-size:11px}.compare-row{display:flex;gap:8px;align-items:center;justify-content:space-between}.compare-label{font-weight:800;white-space:nowrap}.compare-label.before{color:var(--muted)}.compare-label.after{color:var(--good)}.compare-data{color:var(--muted);text-align:right;overflow-wrap:anywhere}.compare-row.after .compare-data{color:var(--text)}
.foot{display:flex;justify-content:space-between;gap:10px;color:var(--muted);font-size:11px;border-top:1px solid var(--line);padding-top:10px;margin-top:11px}.note{margin-top:12px;color:var(--muted);font-size:11px;line-height:1.5}
@media(max-width:700px){main{padding:12px}.summary{grid-template-columns:repeat(2,minmax(0,1fr))}.cores{grid-template-columns:1fr}.metrics{grid-template-columns:repeat(3,minmax(0,1fr))}}
</style>
</head>
<body>
<main>
  <h1>核心交易儀表板</h1>
  <div class="sub">修正前 / 修正後完全分離｜主判斷只看修正後｜每 30 秒自動更新｜{{ now }}</div>
  <section class="summary">
    <div class="summary-card"><div class="label">修正後最好</div><div class="big">{{ best }}</div></div>
    <div class="summary-card"><div class="label">修正後最差</div><div class="big">{{ worst }}</div></div>
    <div class="summary-card"><div class="label">修正後樣本不足</div><div class="big">{{ watch }}</div></div>
    <div class="summary-card"><div class="label">系統狀態</div><div class="big">{{ system }}</div></div>
  </section>
  <section class="cores">
  {% for c in cores %}
    <article class="core">
      <div class="head">
        <div>
          <div class="name">{{ c.name }}</div>
          <div class="desc">{{ c.desc }}</div>
          <div class="revision">修正切點 {{ c.revision }}</div>
        </div>
        <span class="badge {{ c.status_class }}">{{ c.status }}</span>
      </div>

      <div class="section-tag">修正後</div>
      <div class="metrics">
        <div class="metric"><div class="label">交易 / 持倉</div><div class="value">{{ c.trades }} / {{ c.open }}</div></div>
        <div class="metric"><div class="label">勝率</div><div class="value">{{ c.win_rate }}</div></div>
        <div class="metric"><div class="label">PF</div><div class="value">{{ c.pf }}</div></div>
        <div class="metric"><div class="label">Net PnL</div><div class="value pnl {{ 'good' if c.net > 0 else 'bad' if c.net < 0 else '' }}">{{ money(c.net) }}</div></div>
        <div class="metric"><div class="label">24h PnL</div><div class="value pnl {{ 'good' if c.pnl24 > 0 else 'bad' if c.pnl24 < 0 else '' }}">{{ money(c.pnl24) }}</div></div>
        <div class="metric"><div class="label">{{ c.extra_label }}</div><div class="value">{{ c.extra }}</div></div>
      </div>

      <div class="compare">
        <div class="compare-row"><span class="compare-label before">修正前</span><span class="compare-data">{{ c.before_summary }}</span></div>
        <div class="compare-row after"><span class="compare-label after">修正後</span><span class="compare-data">{{ c.after_summary }}</span></div>
      </div>
      <div class="foot"><span>更新 {{ c.updated }}</span><span>{{ c.note }}</span></div>
    </article>
  {% endfor %}
  </section>
  <div class="note">切分規則：方向核心以 signal_time、其他持倉核心以 opened_at、AS1 以累積快照差值切分。修正前資料保留供比較，但不再計入目前最好/最差、PF、勝率與狀態判斷。MEME2 影子資料另行累積，不會混入實際/模擬持倉績效。</div>
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


def parse_revision(core):
    raw = os.getenv(REVISION_ENV[core], DEFAULT_REVISIONS[core]).strip()
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        raw = DEFAULT_REVISIONS[core].replace("Z", "+00:00")
        return datetime.fromisoformat(raw).astimezone(timezone.utc)


def fmt_time(v):
    if not v:
        return "—"
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.astimezone(SYDNEY).strftime("%H:%M:%S")


def fmt_revision(v):
    if v.tzinfo is None:
        v = v.replace(tzinfo=timezone.utc)
    return v.astimezone(SYDNEY).strftime("%m/%d %H:%M 起")


def table_exists(cur, name):
    cur.execute("SELECT to_regclass(%s) AS regclass", (f"public.{name}",))
    r = cur.fetchone()
    return bool(r and r["regclass"])


def norm_stats(row=None):
    row = row or {}
    return {
        "closed": int(row.get("closed_count") or row.get("closed") or 0),
        "open": int(row.get("open_count") or row.get("open") or 0),
        "wins": int(row.get("wins") or 0),
        "gains": f(row.get("gains")),
        "losses": f(row.get("losses")),
        "net": f(row.get("net")),
        "pnl24": f(row.get("pnl24")),
    }


def stat_summary(s, show_pf=True):
    pf = pf_text(s["gains"], s["losses"]) if show_pf else "—"
    return f"{s['closed']} 筆｜PF {pf}｜PnL {money(s['net'])}"


def make_core(name, desc, before, after, revision, updated,
              extra_label="樣本", extra=None, sample_count=None,
              forced_issue=None, note="", show_wr_pf=True):
    before, after = norm_stats(before), norm_stats(after)
    sample = after["closed"] if sample_count is None else int(sample_count or 0)

    win_rate = f"{after['wins']/after['closed']*100:.1f}%" if after["closed"] else "—"
    pf = pf_text(after["gains"], after["losses"])
    if not show_wr_pf:
        win_rate, pf = "—", "—"

    if forced_issue:
        status, status_class = forced_issue, "bad"
    elif sample < 20:
        status, status_class = "修正後樣本不足", "warn"
    elif after["net"] > 0 and (after["losses"] == 0 or after["gains"] / max(after["losses"], 1e-12) >= 1.0):
        status, status_class = "修正後正向", "good"
    elif after["net"] < 0:
        status, status_class = "修正後負期望", "bad"
    else:
        status, status_class = "修正後觀察", "warn"

    return {
        "name": name,
        "desc": desc,
        "trades": after["closed"],
        "open": after["open"],
        "win_rate": win_rate,
        "pf": pf,
        "net": after["net"],
        "pnl24": after["pnl24"],
        "updated": fmt_time(updated),
        "status": status,
        "status_class": status_class,
        "extra_label": extra_label,
        "extra": extra if extra is not None else str(sample),
        "sample_count": sample,
        "note": note,
        "revision": fmt_revision(revision),
        "before_summary": stat_summary(before, show_wr_pf),
        "after_summary": stat_summary(after, show_wr_pf),
    }


def split_directional(cur, engine, cutoff):
    cur.execute("""
        SELECT
          COUNT(*) FILTER (WHERE signal_time < %(cutoff)s AND status='OPEN') AS pre_open,
          COUNT(*) FILTER (WHERE signal_time < %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt IS NOT NULL) AS pre_closed,
          COUNT(*) FILTER (WHERE signal_time < %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt>0) AS pre_wins,
          COALESCE(SUM(sim_pnl_usdt) FILTER (WHERE signal_time < %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt>0),0) AS pre_gains,
          COALESCE(-SUM(sim_pnl_usdt) FILTER (WHERE signal_time < %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt<0),0) AS pre_losses,
          COALESCE(SUM(sim_pnl_usdt) FILTER (WHERE signal_time < %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt IS NOT NULL),0) AS pre_net,

          COUNT(*) FILTER (WHERE signal_time >= %(cutoff)s AND status='OPEN') AS post_open,
          COUNT(*) FILTER (WHERE signal_time >= %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt IS NOT NULL) AS post_closed,
          COUNT(*) FILTER (WHERE signal_time >= %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt>0) AS post_wins,
          COALESCE(SUM(sim_pnl_usdt) FILTER (WHERE signal_time >= %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt>0),0) AS post_gains,
          COALESCE(-SUM(sim_pnl_usdt) FILTER (WHERE signal_time >= %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt<0),0) AS post_losses,
          COALESCE(SUM(sim_pnl_usdt) FILTER (WHERE signal_time >= %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt IS NOT NULL),0) AS post_net,
          COALESCE(SUM(sim_pnl_usdt) FILTER (WHERE signal_time >= %(cutoff)s AND status!='OPEN' AND sim_pnl_usdt IS NOT NULL AND closed_at >= NOW()-INTERVAL '24 hours'),0) AS post_pnl24,
          MAX(COALESCE(last_checked_at,closed_at,signal_time)) AS updated
        FROM scanner_trades
        WHERE engine_version=%(engine)s
    """, {"cutoff": cutoff, "engine": engine})
    r = dict(cur.fetchone())
    before = {"open_count":r["pre_open"],"closed_count":r["pre_closed"],"wins":r["pre_wins"],"gains":r["pre_gains"],"losses":r["pre_losses"],"net":r["pre_net"],"pnl24":0}
    after = {"open_count":r["post_open"],"closed_count":r["post_closed"],"wins":r["post_wins"],"gains":r["post_gains"],"losses":r["post_losses"],"net":r["post_net"],"pnl24":r["post_pnl24"]}
    return before, after, r["updated"]


def split_positions(cur, table, cutoff, extra_where="", params=None):
    params = dict(params or {})
    params["cutoff"] = cutoff
    where = f"WHERE 1=1 {extra_where}"
    cur.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE opened_at < %(cutoff)s AND status='OPEN') AS pre_open,
          COUNT(*) FILTER (WHERE opened_at < %(cutoff)s AND status!='OPEN' AND net_pnl_usdt IS NOT NULL) AS pre_closed,
          COUNT(*) FILTER (WHERE opened_at < %(cutoff)s AND status!='OPEN' AND net_pnl_usdt>0) AS pre_wins,
          COALESCE(SUM(net_pnl_usdt) FILTER (WHERE opened_at < %(cutoff)s AND status!='OPEN' AND net_pnl_usdt>0),0) AS pre_gains,
          COALESCE(-SUM(net_pnl_usdt) FILTER (WHERE opened_at < %(cutoff)s AND status!='OPEN' AND net_pnl_usdt<0),0) AS pre_losses,
          COALESCE(SUM(net_pnl_usdt) FILTER (WHERE opened_at < %(cutoff)s AND status!='OPEN' AND net_pnl_usdt IS NOT NULL),0) AS pre_net,

          COUNT(*) FILTER (WHERE opened_at >= %(cutoff)s AND status='OPEN') AS post_open,
          COUNT(*) FILTER (WHERE opened_at >= %(cutoff)s AND status!='OPEN' AND net_pnl_usdt IS NOT NULL) AS post_closed,
          COUNT(*) FILTER (WHERE opened_at >= %(cutoff)s AND status!='OPEN' AND net_pnl_usdt>0) AS post_wins,
          COALESCE(SUM(net_pnl_usdt) FILTER (WHERE opened_at >= %(cutoff)s AND status!='OPEN' AND net_pnl_usdt>0),0) AS post_gains,
          COALESCE(-SUM(net_pnl_usdt) FILTER (WHERE opened_at >= %(cutoff)s AND status!='OPEN' AND net_pnl_usdt<0),0) AS post_losses,
          COALESCE(SUM(net_pnl_usdt) FILTER (WHERE opened_at >= %(cutoff)s AND status!='OPEN' AND net_pnl_usdt IS NOT NULL),0) AS post_net,
          COALESCE(SUM(net_pnl_usdt) FILTER (WHERE opened_at >= %(cutoff)s AND status!='OPEN' AND net_pnl_usdt IS NOT NULL AND closed_at >= NOW()-INTERVAL '24 hours'),0) AS post_pnl24,
          MAX(COALESCE(closed_at,opened_at)) AS updated
        FROM {table}
        {where}
    """, params)
    r = dict(cur.fetchone())
    before = {"open_count":r["pre_open"],"closed_count":r["pre_closed"],"wins":r["pre_wins"],"gains":r["pre_gains"],"losses":r["pre_losses"],"net":r["pre_net"],"pnl24":0}
    after = {"open_count":r["post_open"],"closed_count":r["post_closed"],"wins":r["post_wins"],"gains":r["post_gains"],"losses":r["post_losses"],"net":r["post_net"],"pnl24":r["post_pnl24"]}
    return before, after, r["updated"]


@app.get("/health")
def health():
    return {"status":"ok","mode":"revision_split_read_only","cores":["R2F","R2FS","R3","AS1","BF1","MEME1X V2","MEME2"]}


@app.get("/")
def home():
    if os.getenv("LIVE_TRADING", "false").lower() != "false":
        return Response("Dashboard requires LIVE_TRADING=false", 503)
    if not DATABASE_URL:
        return Response("DATABASE_URL is missing", 503)

    revisions = {k: parse_revision(k) for k in DEFAULT_REVISIONS}
    cores, system_issues = [], []

    try:
        with psycopg2.connect(DATABASE_URL, sslmode="require", connect_timeout=8) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SET TRANSACTION READ ONLY")

                # Directional cores
                for eng, desc in (("R2F","快速方向交易"),("R2FS","R2F 影子 · 進場過濾"),("R3","突破回踩 · 再加速")):
                    before, after, updated = split_directional(cur, eng, revisions[eng])
                    cores.append(make_core(eng, desc, before, after, revisions[eng], updated,
                                           extra_label="修正後已平倉", extra=str(norm_stats(after)["closed"])))

                # AS1: use cumulative snapshot immediately before the revision as baseline.
                if table_exists(cur, "as_mm_state") and table_exists(cur, "as_mm_snapshots"):
                    cutoff = revisions["AS1"]
                    cur.execute("SELECT updated_at,inventory_base,buy_fills,sell_fills,net_pnl_usdt FROM as_mm_state WHERE symbol='BTC-USDT' LIMIT 1")
                    state = cur.fetchone()
                    if state:
                        state = dict(state)
                        cur.execute("""
                            SELECT observed_at,net_pnl_usdt,buy_fills,sell_fills
                            FROM as_mm_snapshots
                            WHERE symbol='BTC-USDT' AND observed_at < %s
                            ORDER BY observed_at DESC LIMIT 1
                        """, (cutoff,))
                        base = cur.fetchone()
                        base = dict(base) if base else {"net_pnl_usdt":0,"buy_fills":0,"sell_fills":0}
                        pre_fills = int(base.get("buy_fills") or 0) + int(base.get("sell_fills") or 0)
                        total_fills = int(state.get("buy_fills") or 0) + int(state.get("sell_fills") or 0)
                        post_fills = max(0, total_fills - pre_fills)
                        pre_net = f(base.get("net_pnl_usdt"))
                        post_net = f(state.get("net_pnl_usdt")) - pre_net
                        cur.execute("""
                            SELECT net_pnl_usdt FROM as_mm_snapshots
                            WHERE symbol='BTC-USDT' AND observed_at>=GREATEST(%s,NOW()-INTERVAL '24 hours')
                            ORDER BY observed_at ASC LIMIT 1
                        """, (cutoff,))
                        old24 = cur.fetchone()
                        post24 = f(state.get("net_pnl_usdt")) - f(old24["net_pnl_usdt"]) if old24 else 0.0
                        before = {"closed_count":pre_fills,"open_count":0,"wins":0,"gains":0,"losses":0,"net":pre_net,"pnl24":0}
                        after = {"closed_count":post_fills,"open_count":1 if abs(f(state.get("inventory_base")))>1e-10 else 0,"wins":0,"gains":0,"losses":0,"net":post_net,"pnl24":post24}
                        cores.append(make_core("AS1","BTC 做市 / 橫盤核心",before,after,cutoff,state.get("updated_at"),
                                               extra_label="修正後 Fills",extra=str(post_fills),sample_count=post_fills,show_wr_pf=False))

                # BF1
                if table_exists(cur, "bf_positions"):
                    before, after, updated = split_positions(cur, "bf_positions", revisions["BF1"])
                    if table_exists(cur, "bf_observations"):
                        cur.execute("SELECT MAX(observed_at) AS t FROM bf_observations")
                        t = cur.fetchone()["t"]
                        if t:
                            updated = t
                    cores.append(make_core("BF1","Basis + Funding 套利",before,after,revisions["BF1"],updated,
                                           extra_label="修正後已平倉",extra=str(norm_stats(after)["closed"])))

                # MEME1X V2 only
                if table_exists(cur, "meme1x_positions"):
                    before, after, updated = split_positions(
                        cur,"meme1x_positions",revisions["MEME1X"],
                        "AND variant=%(variant)s",{"variant":"EXITABILITY_V2"}
                    )
                    cores.append(make_core("MEME1X V2","Solana 新池 · 出場能力確認",before,after,revisions["MEME1X"],updated,
                                           extra_label="修正後已平倉",extra=str(norm_stats(after)["closed"]),
                                           note="只計 EXITABILITY_V2"))

                # MEME2
                active_wallets, signal_count, avg_delay, last_wallet_update = 0, 0, None, None
                if table_exists(cur, "meme2_wallet_state"):
                    cur.execute("SELECT COUNT(*) FILTER (WHERE updated_at>=NOW()-INTERVAL '5 minutes') AS active,MAX(updated_at) AS updated FROM meme2_wallet_state")
                    w = dict(cur.fetchone())
                    active_wallets = int(w["active"] or 0)
                    last_wallet_update = w["updated"]
                if table_exists(cur, "meme2_signals"):
                    cur.execute("""
                        SELECT COUNT(*) FILTER (WHERE observed_at >= %s) AS c,
                               AVG(detection_delay_sec) FILTER (WHERE observed_at >= GREATEST(%s,NOW()-INTERVAL '24 hours')) AS avg_delay
                        FROM meme2_signals
                    """, (revisions["MEME2"], revisions["MEME2"]))
                    sg = dict(cur.fetchone())
                    signal_count = int(sg["c"] or 0)
                    avg_delay = f(sg["avg_delay"], None)
                if table_exists(cur, "meme2_positions"):
                    before, after, updated = split_positions(cur, "meme2_positions", revisions["MEME2"])
                    issue = None
                    if active_wallets < MEME2_EXPECTED_WALLETS:
                        issue = f"監控異常 {active_wallets}/{MEME2_EXPECTED_WALLETS}"
                        system_issues.append(issue)
                    elif avg_delay is not None and avg_delay > 30:
                        issue = "偵測延遲偏高"
                        system_issues.append(issue)
                    note = f"修正後訊號 {signal_count}｜延遲 {avg_delay:.1f}s" if avg_delay is not None else f"修正後訊號 {signal_count}"
                    cores.append(make_core("MEME2","聰明錢包跟單",before,after,revisions["MEME2"],last_wallet_update or updated,
                                           extra_label="監控錢包",extra=str(active_wallets),forced_issue=issue,note=note))

    except psycopg2.Error:
        app.logger.exception("Dashboard read failed")
        return Response("資料庫查詢失敗", 503)

    order = {"R2F":0,"R2FS":1,"R3":2,"AS1":3,"BF1":4,"MEME1X V2":5,"MEME2":6}
    cores.sort(key=lambda x: order.get(x["name"], 99))

    comparable = [c for c in cores if c["sample_count"] >= 20]
    if comparable:
        best_core = max(comparable, key=lambda x: x["net"])
        worst_core = min(comparable, key=lambda x: x["net"])
        best = f"{best_core['name']} {money(best_core['net'])}"
        worst = f"{worst_core['name']} {money(worst_core['net'])}"
    else:
        best, worst = "樣本不足", "樣本不足"

    insufficient = [c["name"] for c in cores if c["sample_count"] < 20]
    watch = "、".join(insufficient) if insufficient else "無"
    system = "正常" if not system_issues else "、".join(system_issues)

    return render_template_string(
        PAGE,
        cores=cores,
        best=best,
        worst=worst,
        watch=watch,
        system=system,
        now=datetime.now(SYDNEY).strftime("%m/%d %H:%M:%S"),
        money=money,
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "10000")))
