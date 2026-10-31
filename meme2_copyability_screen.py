"""
Meme2 20-wallet historical copyability screen.

Purpose
-------
Safely analyze the already-completed 360-day raw Helius history without
redeploying the Meme2 backfill service.

Input source
------------
Streams manifest.json and each wallet's .jsonl from the existing public
Render HTTP server:
  https://meme2-historical-360.onrender.com/
  data/historical_360/meme2_test_twenty/raw/

Method
------
- Parse wallet-owned non-stable token balance changes with the SAME basic
  pre/post token-balance logic as the live Meme2 core.
- Require matching SOL flow, matching the live signal guard:
    BUY  -> wallet SOL delta < 0
    SELL -> wallet SOL delta > 0
- For high-confidence "simple swap" transactions only, require exactly one
  non-ignored token mint change in that wallet transaction.
- Approximate wallet execution price in SOL/token as:
    abs(wallet SOL delta) / abs(token delta)
  This is NOT a USD market quote and does not reconstruct historical pool
  liquidity. It is useful for copyability / wallet-edge screening only.
- Mirror live behavior at the wallet/mint level:
    first BUY opens, next SELL closes; extra BUYs while open are ignored.
- Test several conservative round-trip cost assumptions.

IMPORTANT
---------
This is NOT the final Meme2 live-equivalent PF because historical liquidity and
historical GeckoTerminal quotes were not stored. It is a screening layer to
identify wallets whose raw edge is large enough to survive realistic costs.

Historical-only. No DB. No orders. No private keys.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import math
import statistics
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

BASE = "https://meme2-historical-360.onrender.com"
RAW_ROOT = "data/historical_360/meme2_test_twenty/raw"
MANIFEST_URL = f"{BASE}/{RAW_ROOT}/manifest.json"

OUT = Path("data/meme2_copyability")
OUT.mkdir(parents=True, exist_ok=True)

WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
IGNORE_MINTS = {WSOL, USDC, USDT}
LAMPORTS_PER_SOL = 1_000_000_000.0

# Live fixed-cost assumptions alone imply ~5.37% loss on a flat-price round trip:
# entry slippage 1.5%, exit slippage 2.0%, fee 1.0% each side.
# Since historical liquidity impact is unknown, screen additional harsher cases.
COST_SCENARIOS_PCT = [5.37, 6.5, 8.0, 10.0]

MIN_SOL_FLOW = 0.0001
HTTP_TIMEOUT = 120


def get_json(url: str):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "meme2-copyability-screen/1.0"},
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
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
    """Return one high-confidence non-stable token change or None."""
    meta = (tx or {}).get("meta") or {}
    if meta.get("err"):
        return None

    message = (((tx or {}).get("transaction") or {}).get("message") or {})
    keys = message.get("accountKeys") or []
    pubkeys = []
    for k in keys:
        pubkeys.append(k.get("pubkey") if isinstance(k, dict) else k)

    sol_delta = 0.0
    try:
        idx = pubkeys.index(wallet)
        pre = (meta.get("preBalances") or [])[idx]
        post = (meta.get("postBalances") or [])[idx]
        sol_delta = (float(post) - float(pre)) / LAMPORTS_PER_SOL
    except Exception:
        return None

    pre_map = {}
    for b in meta.get("preTokenBalances") or []:
        if b.get("owner") == wallet:
            mint = b.get("mint")
            if mint:
                pre_map[mint] = token_amount(b)

    post_map = {}
    for b in meta.get("postTokenBalances") or []:
        if b.get("owner") == wallet:
            mint = b.get("mint")
            if mint:
                post_map[mint] = token_amount(b)

    changes = []
    for mint in set(pre_map) | set(post_map):
        if not mint or mint in IGNORE_MINTS:
            continue
        pre = pre_map.get(mint, 0.0)
        post = post_map.get(mint, 0.0)
        delta = post - pre
        if abs(delta) <= 1e-15:
            continue
        changes.append(
            {
                "mint": mint,
                "side": "BUY" if delta > 0 else "SELL",
                "delta": delta,
                "pre": pre,
                "post": post,
                "sol_delta": sol_delta,
            }
        )

    # High-confidence only: one non-stable token changed in the wallet.
    # Multi-token transactions can make one wallet-level SOL delta ambiguous.
    if len(changes) != 1:
        return None

    ch = changes[0]
    if ch["side"] == "BUY":
        if ch["sol_delta"] > -MIN_SOL_FLOW:
            return None
    else:
        if ch["sol_delta"] < MIN_SOL_FLOW:
            return None

    qty = abs(float(ch["delta"]))
    if qty <= 0:
        return None

    # Approximate wallet's own transaction price in SOL/token.
    px_sol = abs(float(ch["sol_delta"])) / qty
    if not math.isfinite(px_sol) or px_sol <= 0:
        return None

    ch["price_sol_per_token"] = px_sol
    return ch


def raw_return_pct(entry_px: float, exit_px: float) -> float:
    if entry_px <= 0:
        return 0.0
    return (exit_px / entry_px - 1.0) * 100.0


def net_after_flat_cost(raw_ret_pct: float, cost_pct: float) -> float:
    # Screening approximation: apply conservative total round-trip cost as
    # percentage points. Final live-equivalent replay must use historical
    # liquidity-aware executable prices.
    return raw_ret_pct - cost_pct


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
    frac = x - lo
    return s[lo] * (1 - frac) + s[hi] * frac


def scan_wallet(wallet: str, raw_url: str):
    opens = {}
    pairs = []
    counters = defaultdict(int)

    req = urllib.request.Request(
        raw_url,
        headers={"User-Agent": "meme2-copyability-screen/1.0"},
    )
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        # TextIOWrapper lets us stream line-by-line instead of loading giant files.
        stream = io.TextIOWrapper(r, encoding="utf-8")
        for line in stream:
            counters["raw_rows"] += 1
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
            mint = ch["mint"]
            key = (wallet, mint)

            if ch["side"] == "BUY":
                counters["simple_buys"] += 1
                if key in opens:
                    counters["extra_buy_while_open"] += 1
                    continue
                opens[key] = {
                    "opened": t,
                    "entry_px_sol": ch["price_sol_per_token"],
                    "mint": mint,
                }
                continue

            counters["simple_sells"] += 1
            p = opens.get(key)
            if not p:
                counters["sell_without_open"] += 1
                continue

            exit_px = ch["price_sol_per_token"]
            raw_ret = raw_return_pct(p["entry_px_sol"], exit_px)
            hold_sec = max(0.0, (t - p["opened"]).total_seconds())

            pairs.append(
                {
                    "wallet": wallet,
                    "mint": mint,
                    "opened_at": p["opened"].isoformat(),
                    "closed_at": t.isoformat(),
                    "hold_sec": hold_sec,
                    "entry_sol_per_token": p["entry_px_sol"],
                    "exit_sol_per_token": exit_px,
                    "raw_return_pct": raw_ret,
                }
            )
            del opens[key]

    counters["unclosed"] = len(opens)
    return pairs, dict(counters)


def wallet_summary(wallet: str, pairs: list[dict], counters: dict):
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
        vals = [net_after_flat_cost(x, cost) for x in raw]
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
        raise RuntimeError(
            f"backfill not complete: {summary}"
        )

    wallets_meta = manifest.get("wallets") or {}

    print(
        "MEME2_COPYABILITY_START "
        f"wallets={len(wallets_meta)} "
        f"raw_rows={summary.get('rows')} "
        f"cost_scenarios={COST_SCENARIOS_PCT}",
        flush=True,
    )

    wallet_rows = []
    all_pairs = []

    for i, (wallet, meta) in enumerate(wallets_meta.items(), start=1):
        raw_file = str(meta.get("raw_file") or "")
        if not raw_file:
            tag = hashlib.sha1(wallet.encode()).hexdigest()[:12]
            raw_file = f"{RAW_ROOT}/{tag}.jsonl"

        # Manifest stores repo-relative path.
        raw_url = f"{BASE}/{raw_file.lstrip('/')}"
        print(
            f"MEME2_COPYABILITY_WALLET_START "
            f"wallet={i}/{len(wallets_meta)} {wallet[:8]}.. "
            f"raw_rows={meta.get('rows')}",
            flush=True,
        )

        pairs, counters = scan_wallet(wallet, raw_url)
        row = wallet_summary(wallet, pairs, counters)
        wallet_rows.append(row)
        all_pairs.extend(pairs)

        print(
            "MEME2_COPYABILITY_WALLET_DONE "
            + json.dumps(
                {
                    "wallet": wallet[:8],
                    "paired_trades": row["paired_trades"],
                    "raw_pf": row["raw_pf"],
                    "pf_cost_6_5": row.get("pf_cost_6_5"),
                    "median_hold_sec": row["median_hold_sec"],
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    # Aggregate across wallets.
    aggregate_raw = [float(x["raw_return_pct"]) for x in all_pairs]
    aggregate = {
        "wallets": len(wallet_rows),
        "paired_trades": len(all_pairs),
        "raw_pf": round(pf(aggregate_raw), 4),
        "raw_sum_return_pct": round(sum(aggregate_raw), 4),
        "raw_win_rate_pct": round(
            100.0 * sum(1 for x in aggregate_raw if x > 0)
            / len(aggregate_raw),
            2,
        ) if aggregate_raw else 0.0,
    }

    for cost in COST_SCENARIOS_PCT:
        vals = [net_after_flat_cost(x, cost) for x in aggregate_raw]
        tag = str(cost).replace(".", "_")
        aggregate[f"pf_cost_{tag}"] = round(pf(vals), 4)
        aggregate[f"sum_cost_{tag}"] = round(sum(vals), 4)
        aggregate[f"win_rate_cost_{tag}"] = round(
            100.0 * sum(1 for x in vals if x > 0) / len(vals),
            2,
        ) if vals else 0.0

    # Ranking: use 6.5% cost scenario as a conservative central screen.
    wallet_rows.sort(
        key=lambda r: (
            r.get("paired_trades", 0) >= 20,
            r.get("pf_cost_6_5", 0.0),
            r.get("sum_cost_6_5", 0.0),
        ),
        reverse=True,
    )

    wallet_csv = OUT / "meme2_wallet_copyability.csv"
    pair_csv = OUT / "meme2_copyability_pairs.csv"
    report_json = OUT / "meme2_copyability_report.json"

    if wallet_rows:
        with wallet_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(wallet_rows[0].keys()))
            w.writeheader()
            w.writerows(wallet_rows)

    if all_pairs:
        with pair_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_pairs[0].keys()))
            w.writeheader()
            w.writerows(all_pairs)

    report = {
        "method": "simple_swap_sol_flow_copyability_screen",
        "limitations": [
            "No historical liquidity snapshot; cannot reproduce live liquidity gate or impact exactly.",
            "SOL/token transaction-price proxy is used instead of historical USD pool price.",
            "Only simple one-nonstable-token-change transactions are retained to avoid ambiguous multi-token SOL-flow attribution.",
            "PF is a screening metric only; a final liquidity-aware replay is required before live use.",
        ],
        "aggregate": aggregate,
        "wallets": wallet_rows,
    }
    report_json.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    print(
        "MEME2_COPYABILITY_SUMMARY "
        + json.dumps(aggregate, separators=(",", ":")),
        flush=True,
    )

    print("MEME2_COPYABILITY_TOP", flush=True)
    for r in wallet_rows[:10]:
        print(
            json.dumps(
                {
                    "wallet": r["wallet"][:8],
                    "trades": r["paired_trades"],
                    "pf6_5": r.get("pf_cost_6_5"),
                    "sum6_5": r.get("sum_cost_6_5"),
                    "median_hold_sec": r.get("median_hold_sec"),
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    print("MEME2_COPYABILITY_DONE", flush=True)


if __name__ == "__main__":
    main()
