
"""
Meme2 resume runner: wallets 19-20 only, with HTTP timeout/retry.

Historical-only. No DB writes. No orders. No live/shadow changes.
Designed to replace bf1_historical_v3_job.py temporarily on the retired BF1
historical service.

It resumes the 20-wallet screening from wallet 19/20 and writes results into
data/bf1_v3, which is already served by the existing Render start command.
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

WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
IGNORE_MINTS = {WSOL, USDC, USDT}
LAMPORTS_PER_SOL = 1_000_000_000.0

COST_SCENARIOS_PCT = [5.37, 6.5, 8.0, 10.0]
MIN_SOL_FLOW = 0.0001
HTTP_TIMEOUT_SEC = 30
MAX_ATTEMPTS = 4
RETRY_PAUSE_SEC = 5


def get_json(url: str):
    req = urllib.request.Request(url, headers={"User-Agent": "meme2-resume19/1.0"})
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
        pre = pre_map.get(mint, 0.0)
        post = post_map.get(mint, 0.0)
        delta = post - pre
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


def percentile(vals, p):
    if not vals:
        return None
    s = sorted(vals)
    if len(s) == 1:
        return s[0]
    x = (len(s) - 1) * p
    lo = int(math.floor(x))
    hi = int(math.ceil(x))
    if lo == hi:
        return s[lo]
    f = x - lo
    return s[lo] * (1 - f) + s[hi] * f


def scan_wallet_once(wallet: str, raw_url: str):
    opens = {}
    pairs = []
    counters = defaultdict(int)

    req = urllib.request.Request(
        raw_url,
        headers={
            "User-Agent": "meme2-resume19/1.0",
            "Connection": "close",
        },
    )

    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as r:
        stream = io.TextIOWrapper(r, encoding="utf-8")
        for line in stream:
            counters["raw_rows"] += 1
            if counters["raw_rows"] % 25000 == 0:
                print(
                    f"MEME2_RESUME19_PROGRESS wallet={wallet[:8]} "
                    f"rows={counters['raw_rows']}",
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
                counters["not_simple_swap"] += 1
                continue

            block_time = tx.get("blockTime")
            if not block_time:
                counters["missing_time"] += 1
                continue

            t = datetime.fromtimestamp(float(block_time), tz=timezone.utc)
            key = (wallet, ch["mint"])

            if ch["side"] == "BUY":
                counters["simple_buys"] += 1
                if key in opens:
                    counters["extra_buy_while_open"] += 1
                    continue
                opens[key] = {
                    "opened": t,
                    "entry_px_sol": ch["price_sol_per_token"],
                    "mint": ch["mint"],
                }
                continue

            counters["simple_sells"] += 1
            pos = opens.get(key)
            if not pos:
                counters["sell_without_open"] += 1
                continue

            exit_px = ch["price_sol_per_token"]
            raw_ret = (exit_px / pos["entry_px_sol"] - 1.0) * 100.0
            hold_sec = max(0.0, (t - pos["opened"]).total_seconds())

            pairs.append({
                "wallet": wallet,
                "mint": ch["mint"],
                "opened_at": pos["opened"].isoformat(),
                "closed_at": t.isoformat(),
                "hold_sec": hold_sec,
                "entry_sol_per_token": pos["entry_px_sol"],
                "exit_sol_per_token": exit_px,
                "raw_return_pct": raw_ret,
            })
            del opens[key]

    counters["unclosed"] = len(opens)
    return pairs, dict(counters)


def scan_wallet_with_retry(wallet: str, raw_url: str):
    last = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(
                f"MEME2_RESUME19_ATTEMPT wallet={wallet[:8]} "
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
                f"MEME2_RESUME19_RETRY wallet={wallet[:8]} "
                f"attempt={attempt}/{MAX_ATTEMPTS} "
                f"error={type(exc).__name__}:{exc}",
                flush=True,
            )
            if attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_PAUSE_SEC * attempt)

    raise RuntimeError(
        f"wallet {wallet[:8]} failed after {MAX_ATTEMPTS} attempts: {last}"
    )


def summarize(wallet, pairs, counters):
    raw = [float(x["raw_return_pct"]) for x in pairs]
    holds = [float(x["hold_sec"]) for x in pairs]

    row = {
        "wallet": wallet,
        "raw_rows": int(counters.get("raw_rows", 0)),
        "paired_trades": len(pairs),
        "simple_buys": int(counters.get("simple_buys", 0)),
        "simple_sells": int(counters.get("simple_sells", 0)),
        "unclosed": int(counters.get("unclosed", 0)),
        "raw_win_rate_pct": round(
            100.0 * sum(1 for x in raw if x > 0) / len(raw), 2
        ) if raw else 0.0,
        "raw_pf": round(pf(raw), 4),
        "raw_sum_return_pct": round(sum(raw), 4),
        "median_hold_sec": round(statistics.median(holds), 2) if holds else None,
        "p25_hold_sec": round(percentile(holds, 0.25), 2) if holds else None,
        "p75_hold_sec": round(percentile(holds, 0.75), 2) if holds else None,
        "median_raw_return_pct": round(statistics.median(raw), 4) if raw else None,
    }

    for cost in COST_SCENARIOS_PCT:
        vals = [x - cost for x in raw]
        tag = str(cost).replace(".", "_")
        row[f"pf_cost_{tag}"] = round(pf(vals), 4)
        row[f"sum_cost_{tag}"] = round(sum(vals), 4)
        row[f"win_rate_cost_{tag}"] = round(
            100.0 * sum(1 for x in vals if x > 0) / len(vals), 2
        ) if vals else 0.0

    return row


def main():
    manifest = get_json(MANIFEST_URL)
    summary = manifest.get("summary") or {}
    if int(summary.get("complete_wallets", 0)) != 20:
        raise RuntimeError(f"20-wallet raw history incomplete: {summary}")

    wallets = list((manifest.get("wallets") or {}).items())
    targets = wallets[18:20]

    print(
        "MEME2_RESUME19_START "
        f"targets=19-20/20 total_raw_rows={summary.get('rows')} "
        f"timeout={HTTP_TIMEOUT_SEC}s retries={MAX_ATTEMPTS}",
        flush=True,
    )

    rows_out = []
    all_pairs = []

    for absolute_idx, (wallet, meta) in enumerate(targets, start=19):
        raw_file = str(meta.get("raw_file") or "")
        raw_url = f"{BASE}/{raw_file.lstrip('/')}"

        print(
            f"MEME2_RESUME19_WALLET_START wallet={absolute_idx}/20 "
            f"{wallet[:8]}.. expected_rows={meta.get('rows')}",
            flush=True,
        )

        pairs, counters = scan_wallet_with_retry(wallet, raw_url)
        row = summarize(wallet, pairs, counters)
        rows_out.append(row)
        all_pairs.extend(pairs)

        print(
            "MEME2_RESUME19_WALLET_DONE "
            + json.dumps({
                "wallet_no": absolute_idx,
                "wallet": wallet[:8],
                "raw_rows": row["raw_rows"],
                "paired_trades": row["paired_trades"],
                "raw_pf": row["raw_pf"],
                "pf_cost_6_5": row["pf_cost_6_5"],
                "sum_cost_6_5": row["sum_cost_6_5"],
                "median_hold_sec": row["median_hold_sec"],
            }, separators=(",", ":")),
            flush=True,
        )

    csv_path = OUT / "meme2_resume19_wallet_summary.csv"
    if rows_out:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows_out[0].keys()))
            w.writeheader()
            w.writerows(rows_out)

    pairs_path = OUT / "meme2_resume19_pairs.csv"
    if all_pairs:
        with pairs_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_pairs[0].keys()))
            w.writeheader()
            w.writerows(all_pairs)

    report = {
        "resume_wallets": [19, 20],
        "cost_scenarios_pct": COST_SCENARIOS_PCT,
        "wallets": rows_out,
        "note": (
            "Copyability screening only. Historical liquidity snapshots were not "
            "stored, so this is not final live-equivalent PF."
        ),
    }
    (OUT / "meme2_resume19_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        "MEME2_RESUME19_DONE "
        + json.dumps({
            "wallets_completed": len(rows_out),
            "paired_trades": len(all_pairs),
        }, separators=(",", ":")),
        flush=True,
    )


if __name__ == "__main__":
    main()
