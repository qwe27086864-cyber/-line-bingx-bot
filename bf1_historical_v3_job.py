
"""
Meme2 final robustness validation.

Historical-only. No live/shadow DB. No orders.

Goals:
1) Rebuild paired trades for the 8 surviving Meme2 wallet candidates.
2) Require hold_sec > 20 seconds (copyability gate).
3) Apply conservative total round-trip cost = 11.5 percentage points
   (6.5% base + 5% extra adverse-cost stress).
4) Stress extreme-return dependence by capping raw per-trade return at:
   100%, 200%, 500%, and uncapped.
5) Check month-by-month stability over the 360-day window.
6) Measure concentration: how much gross profit comes from top 1 / top 10 trades.
7) Produce FINAL_PASS / WATCH / FAIL.

Important:
- This still cannot reconstruct exact delayed DEX prices because synchronized
  historical liquidity/price snapshots were not archived.
- This is a robustness screen, not proof of live-equivalent PF.
- After completion the process intentionally sleeps forever so Render does NOT
  restart the service and repeat the job.
"""

from __future__ import annotations

import csv
import io
import json
import math
import socket
import statistics
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

BASE = "https://meme2-historical-360.onrender.com"
RAW_ROOT = "data/historical_360/meme2_test_twenty/raw"
MANIFEST_URL = f"{BASE}/{RAW_ROOT}/manifest.json"

OUT = Path("data/bf1_v3")
OUT.mkdir(parents=True, exist_ok=True)

TARGET_WALLETS = [
    "BNnN2MqfWLvgThYBsv6v8JQaYZXYKYahC5YCy27Ct1cX",
    "8MoW9mtbEz6z3gPuAdYb1yWhjCAxQSYqpcTb1CQgN5qb",
    "CQGxdVAvrRvb8TvrGmMYQCcHjCCbFuhKgTWSEVqFJgfm",
    "6tzAKfHtQA4zLwgaXn97kU5C1dYsKcRy8XZhZUWmWU23",
    "A7ZmFuTUEb14ArzoYf7mBJUPmdvbLbj1J8kvKAaJAWek",
    "HzKH3s8Kf7f7pgvJZx2V6TzhkdHbZDurG53rXj61BVLR",
    "3kxcF8wHKm4sEtnxjXXeUvNGeDjTmo47LgtSRg71YfG5",
    "An9sREpLnAXVi4KMaTGuGvgET51CyaukLUTMtxzmLYSB",
]

DELAY_SEC = 20
BASE_COST_PCT = 6.5
EXTRA_COST_PCT = 5.0
TOTAL_COST_PCT = BASE_COST_PCT + EXTRA_COST_PCT
RETURN_CAPS = [100.0, 200.0, 500.0, None]

MIN_SOL_FLOW = 0.0001
HTTP_TIMEOUT_SEC = 45
MAX_ATTEMPTS = 4
RETRY_PAUSE_SEC = 5

WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
IGNORE_MINTS = {WSOL, USDC, USDT}
LAMPORTS_PER_SOL = 1_000_000_000.0


def hold_forever():
    print("MEME2_FINAL_HOLD process_complete=true restart_prevention=true", flush=True)
    while True:
        time.sleep(3600)


def get_json(url: str):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "meme2-final-validation/1.0", "Connection": "close"},
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as r:
        return json.loads(r.read().decode("utf-8"))


def token_amount(b):
    ui = (b or {}).get("uiTokenAmount") or {}
    s = ui.get("uiAmountString")
    if s is not None:
        try:
            return float(s)
        except Exception:
            return 0.0
    try:
        raw = float(ui.get("amount") or 0)
        dec = int(float(ui.get("decimals") or 0))
        return raw / (10 ** dec)
    except Exception:
        return 0.0


def wallet_change(wallet: str, tx: dict):
    meta = (tx or {}).get("meta") or {}
    if meta.get("err"):
        return None

    message = (((tx or {}).get("transaction") or {}).get("message") or {})
    keys = message.get("accountKeys") or []
    pubkeys = [k.get("pubkey") if isinstance(k, dict) else k for k in keys]

    try:
        idx = pubkeys.index(wallet)
        pre = (meta.get("preBalances") or [])[idx]
        post = (meta.get("postBalances") or [])[idx]
        sol_delta = (float(post) - float(pre)) / LAMPORTS_PER_SOL
    except Exception:
        return None

    pre_map = {}
    for b in meta.get("preTokenBalances") or []:
        if b.get("owner") == wallet and b.get("mint"):
            pre_map[b["mint"]] = token_amount(b)

    post_map = {}
    for b in meta.get("postTokenBalances") or []:
        if b.get("owner") == wallet and b.get("mint"):
            post_map[b["mint"]] = token_amount(b)

    changes = []
    for mint in set(pre_map) | set(post_map):
        if not mint or mint in IGNORE_MINTS:
            continue
        delta = post_map.get(mint, 0.0) - pre_map.get(mint, 0.0)
        if abs(delta) <= 1e-15:
            continue
        changes.append({
            "mint": mint,
            "side": "BUY" if delta > 0 else "SELL",
            "delta": delta,
            "sol_delta": sol_delta,
        })

    if len(changes) != 1:
        return None

    ch = changes[0]
    if ch["side"] == "BUY" and ch["sol_delta"] > -MIN_SOL_FLOW:
        return None
    if ch["side"] == "SELL" and ch["sol_delta"] < MIN_SOL_FLOW:
        return None

    qty = abs(float(ch["delta"]))
    if qty <= 0:
        return None

    px = abs(float(ch["sol_delta"])) / qty
    if not math.isfinite(px) or px <= 0:
        return None

    ch["price_sol_per_token"] = px
    return ch


def pf(vals):
    gp = sum(x for x in vals if x > 0)
    gl = abs(sum(x for x in vals if x < 0))
    if gl > 0:
        return gp / gl
    return 999.0 if gp > 0 else 0.0


def metrics(vals):
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    total = sum(vals)
    n = len(vals)
    return {
        "trades": n,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(100 * len(wins) / n, 2) if n else 0.0,
        "profit_factor": round(pf(vals), 6),
        "sum_net_return_pct": round(total, 6),
        "avg_net_return_pct": round(total / n, 6) if n else 0.0,
    }


def scan_wallet_once(wallet: str, raw_url: str):
    opens = {}
    pairs = []
    counters = defaultdict(int)

    req = urllib.request.Request(
        raw_url,
        headers={"User-Agent": "meme2-final-validation/1.0", "Connection": "close"},
    )

    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as r:
        stream = io.TextIOWrapper(r, encoding="utf-8")
        for line in stream:
            counters["raw_rows"] += 1
            if counters["raw_rows"] % 25000 == 0:
                print(
                    f"MEME2_FINAL_PROGRESS wallet={wallet[:8]} rows={counters['raw_rows']}",
                    flush=True,
                )

            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
                tx = obj.get("tx") or {}
            except Exception:
                counters["bad_json"] += 1
                continue

            ch = wallet_change(wallet, tx)
            if not ch:
                continue

            block_time = tx.get("blockTime")
            if not block_time:
                continue

            t = datetime.fromtimestamp(float(block_time), tz=timezone.utc)
            key = (wallet, ch["mint"])

            if ch["side"] == "BUY":
                if key in opens:
                    continue
                opens[key] = {
                    "opened": t,
                    "entry_px_sol": ch["price_sol_per_token"],
                    "mint": ch["mint"],
                }
                continue

            pos = opens.get(key)
            if not pos:
                continue

            exit_px = ch["price_sol_per_token"]
            raw_ret = (exit_px / pos["entry_px_sol"] - 1.0) * 100.0
            hold_sec = max(0.0, (t - pos["opened"]).total_seconds())

            pairs.append({
                "wallet": wallet,
                "mint": ch["mint"],
                "opened_at": pos["opened"],
                "closed_at": t,
                "hold_sec": hold_sec,
                "raw_return_pct": raw_ret,
            })
            del opens[key]

    return pairs


def scan_wallet_with_retry(wallet: str, raw_url: str):
    last = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(
                f"MEME2_FINAL_ATTEMPT wallet={wallet[:8]} "
                f"attempt={attempt}/{MAX_ATTEMPTS}",
                flush=True,
            )
            return scan_wallet_once(wallet, raw_url)
        except (
            TimeoutError,
            socket.timeout,
            urllib.error.URLError,
            ConnectionError,
            OSError,
        ) as exc:
            last = exc
            print(
                f"MEME2_FINAL_RETRY wallet={wallet[:8]} "
                f"attempt={attempt}/{MAX_ATTEMPTS} "
                f"error={type(exc).__name__}:{exc}",
                flush=True,
            )
            if attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_PAUSE_SEC * attempt)
    raise RuntimeError(f"{wallet[:8]} failed after retries: {last}")


def adjusted_values(pairs, cap):
    out = []
    for x in pairs:
        raw = float(x["raw_return_pct"])
        if cap is not None:
            raw = min(raw, cap)
        out.append(raw - TOTAL_COST_PCT)
    return out


def monthly_stats(pairs, cap):
    buckets = defaultdict(list)
    for x in pairs:
        month = x["opened_at"].strftime("%Y-%m")
        raw = float(x["raw_return_pct"])
        if cap is not None:
            raw = min(raw, cap)
        buckets[month].append(raw - TOTAL_COST_PCT)

    rows = []
    for month in sorted(buckets):
        m = metrics(buckets[month])
        rows.append({"month": month, **m})
    return rows


def concentration(vals):
    pos = sorted((x for x in vals if x > 0), reverse=True)
    gp = sum(pos)
    if gp <= 0:
        return 0.0, 0.0
    top1 = 100.0 * sum(pos[:1]) / gp
    top10 = 100.0 * sum(pos[:10]) / gp
    return round(top1, 2), round(top10, 2)


def classify(primary, months, top1_share, top10_share):
    positive_months = sum(1 for m in months if m["sum_net_return_pct"] > 0)
    month_pfs = [m["profit_factor"] for m in months if m["trades"] > 0]
    median_month_pf = statistics.median(month_pfs) if month_pfs else 0.0

    # Conservative FINAL_PASS uses capped-100% returns.
    if (
        primary["trades"] >= 200
        and primary["profit_factor"] >= 1.15
        and primary["sum_net_return_pct"] > 0
        and positive_months >= 8
        and median_month_pf > 1.0
        and top1_share <= 15.0
        and top10_share <= 40.0
    ):
        return "FINAL_PASS"

    if (
        primary["trades"] >= 150
        and primary["profit_factor"] > 1.0
        and primary["sum_net_return_pct"] > 0
        and positive_months >= 6
    ):
        return "WATCH"

    return "FAIL"


def write_csv(path: Path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)


def main():
    manifest = get_json(MANIFEST_URL)
    summary = manifest.get("summary") or {}
    if int(summary.get("complete_wallets", 0)) != 20:
        raise RuntimeError(f"raw history incomplete: {summary}")

    m_wallets = manifest.get("wallets") or {}

    print(
        "MEME2_FINAL_START "
        f"wallets={len(TARGET_WALLETS)} delay_sec={DELAY_SEC} "
        f"total_cost_pct={TOTAL_COST_PCT} caps={RETURN_CAPS} "
        "historical_only=true",
        flush=True,
    )

    final_rows = []
    cap_rows = []
    month_rows = []

    for idx, wallet in enumerate(TARGET_WALLETS, 1):
        meta = m_wallets.get(wallet)
        if not meta:
            raise RuntimeError(f"wallet missing from manifest: {wallet}")

        raw_file = str(meta.get("raw_file") or "")
        raw_url = f"{BASE}/{raw_file.lstrip('/')}"

        print(
            f"MEME2_FINAL_WALLET_START wallet={idx}/{len(TARGET_WALLETS)} "
            f"{wallet[:8]}.. raw_rows={meta.get('rows')}",
            flush=True,
        )

        pairs = scan_wallet_with_retry(wallet, raw_url)
        followable = [x for x in pairs if float(x["hold_sec"]) > DELAY_SEC]
        followable_rate = (
            100.0 * len(followable) / len(pairs) if pairs else 0.0
        )

        cap_results = {}
        for cap in RETURN_CAPS:
            vals = adjusted_values(followable, cap)
            m = metrics(vals)
            label = "uncapped" if cap is None else f"cap_{int(cap)}"
            cap_results[label] = m

            cap_rows.append({
                "wallet": wallet,
                "cap": label,
                "followable_rate_pct": round(followable_rate, 2),
                **m,
            })

        # Primary robustness result = raw return capped at +100% per trade.
        primary_vals = adjusted_values(followable, 100.0)
        primary = metrics(primary_vals)
        months = monthly_stats(followable, 100.0)
        top1, top10 = concentration(primary_vals)

        for m in months:
            month_rows.append({
                "wallet": wallet,
                "cap": "cap_100",
                **m,
            })

        positive_months = sum(
            1 for m in months if m["sum_net_return_pct"] > 0
        )
        negative_months = sum(
            1 for m in months if m["sum_net_return_pct"] < 0
        )
        month_pfs = [m["profit_factor"] for m in months if m["trades"] > 0]
        median_month_pf = (
            round(statistics.median(month_pfs), 6)
            if month_pfs else 0.0
        )
        worst_month_sum = (
            min((m["sum_net_return_pct"] for m in months), default=0.0)
        )

        classification = classify(
            primary, months, top1, top10
        )

        row = {
            "wallet": wallet,
            "classification": classification,
            "paired_trades_total": len(pairs),
            "followable_trades_20s": len(followable),
            "followable_rate_20s_pct": round(followable_rate, 2),
            "median_hold_sec": round(
                statistics.median([x["hold_sec"] for x in pairs]), 2
            ) if pairs else None,
            "cap100_trades": primary["trades"],
            "cap100_pf": primary["profit_factor"],
            "cap100_sum_net_return_pct": primary["sum_net_return_pct"],
            "positive_months": positive_months,
            "negative_months": negative_months,
            "median_month_pf": median_month_pf,
            "worst_month_sum_pct": round(worst_month_sum, 6),
            "top1_share_gross_profit_pct": top1,
            "top10_share_gross_profit_pct": top10,
            "uncapped_pf": cap_results["uncapped"]["profit_factor"],
            "cap200_pf": cap_results["cap_200"]["profit_factor"],
            "cap500_pf": cap_results["cap_500"]["profit_factor"],
        }
        final_rows.append(row)

        print(
            "MEME2_FINAL_WALLET_DONE "
            + json.dumps({
                "wallet": wallet[:8],
                "classification": classification,
                "followable_rate_20s_pct": row["followable_rate_20s_pct"],
                "cap100_trades": row["cap100_trades"],
                "cap100_pf": row["cap100_pf"],
                "positive_months": row["positive_months"],
                "median_month_pf": row["median_month_pf"],
                "top1_share_pct": row["top1_share_gross_profit_pct"],
                "top10_share_pct": row["top10_share_gross_profit_pct"],
            }, separators=(",", ":")),
            flush=True,
        )

    order = {"FINAL_PASS": 0, "WATCH": 1, "FAIL": 2}
    final_rows.sort(
        key=lambda r: (
            order.get(r["classification"], 9),
            -float(r["cap100_pf"]),
            -int(r["cap100_trades"]),
        )
    )

    write_csv(OUT / "meme2_final_wallets.csv", final_rows)
    write_csv(OUT / "meme2_final_caps.csv", cap_rows)
    write_csv(OUT / "meme2_final_monthly.csv", month_rows)

    report = {
        "delay_sec": DELAY_SEC,
        "base_cost_pct": BASE_COST_PCT,
        "extra_cost_pct": EXTRA_COST_PCT,
        "total_cost_pct": TOTAL_COST_PCT,
        "primary_return_cap_pct": 100.0,
        "limitations": [
            "Exact delayed DEX prices/liquidity are not available in the archive.",
            "20-second delay is modeled as a followability gate.",
            "Primary PF caps each raw trade return at +100% to reduce outlier dependence.",
            "This is robustness screening, not live-equivalent proof.",
        ],
        "wallets": final_rows,
    }
    (OUT / "meme2_final_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    for row in final_rows:
        print(
            "MEME2_FINAL_CLASSIFICATION "
            + json.dumps(row, separators=(",", ":")),
            flush=True,
        )

    print(
        "MEME2_FINAL_DONE "
        "wallets=data/bf1_v3/meme2_final_wallets.csv "
        "caps=data/bf1_v3/meme2_final_caps.csv "
        "monthly=data/bf1_v3/meme2_final_monthly.csv "
        "report=data/bf1_v3/meme2_final_report.json",
        flush=True,
    )

    hold_forever()


if __name__ == "__main__":
    main()
