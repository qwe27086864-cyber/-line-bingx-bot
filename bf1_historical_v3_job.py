
"""
Meme2 copyability delay/slippage stress test.

Purpose
-------
Historical-only screening for the 8 remaining Meme2 wallet candidates.

What this CAN test:
1) Whether a copied trade would still be open after a detection/execution delay
   of 5 / 10 / 20 / 30 seconds.
2) Whether surviving trades retain positive expectancy after a base round-trip
   cost plus extra adverse cost stress.

What this CANNOT honestly reconstruct:
- The exact market price 5/10/20/30 seconds after the wallet's transaction,
  because the raw historical archive contains wallet transactions but not
  synchronized historical DEX price/liquidity snapshots at those delayed times.

Therefore:
- Trades with hold_sec <= delay are treated as UNFOLLOWABLE and excluded.
- Surviving trades keep the wallet's observed raw return, then we subtract:
    BASE_COST_PCT + EXTRA_COST_PCT
- EXTRA_COST_PCT is stressed independently at 0 / 2 / 5 / 10 percentage points.
  It is NOT claimed that a 10-second delay equals any specific cost.

Historical-only:
- no DATABASE_URL
- no live/shadow writes
- no orders
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

# 8 candidates from the completed 20-wallet copyability screen.
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

DELAY_SECONDS = [5, 10, 20, 30]

# The earlier screen used 6.5% round-trip cost as a conservative baseline.
BASE_COST_PCT = 6.5

# Extra round-trip adverse-cost stress, independent of delay.
EXTRA_COST_PCTS = [0.0, 2.0, 5.0, 10.0]

MIN_SOL_FLOW = 0.0001
HTTP_TIMEOUT_SEC = 45
MAX_ATTEMPTS = 4
RETRY_PAUSE_SEC = 5

WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
IGNORE_MINTS = {WSOL, USDC, USDT}
LAMPORTS_PER_SOL = 1_000_000_000.0


def get_json(url: str):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "meme2-delay-stress/1.0", "Connection": "close"},
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

    # Same high-confidence simple-swap rule used in the earlier copyability screen.
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


def pctile(vals, p):
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
    return s[lo] * (1.0 - f) + s[hi] * f


def scan_wallet_once(wallet: str, raw_url: str):
    opens = {}
    pairs = []
    counters = defaultdict(int)

    req = urllib.request.Request(
        raw_url,
        headers={
            "User-Agent": "meme2-delay-stress/1.0",
            "Connection": "close",
        },
    )

    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT_SEC) as r:
        stream = io.TextIOWrapper(r, encoding="utf-8")
        for line in stream:
            counters["raw_rows"] += 1

            if counters["raw_rows"] % 25000 == 0:
                print(
                    f"MEME2_STRESS_PROGRESS wallet={wallet[:8]} "
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
                f"MEME2_STRESS_ATTEMPT wallet={wallet[:8]} "
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
                f"MEME2_STRESS_RETRY wallet={wallet[:8]} "
                f"attempt={attempt}/{MAX_ATTEMPTS} "
                f"error={type(exc).__name__}:{exc}",
                flush=True,
            )
            if attempt < MAX_ATTEMPTS:
                time.sleep(RETRY_PAUSE_SEC * attempt)

    raise RuntimeError(
        f"wallet {wallet[:8]} failed after {MAX_ATTEMPTS} attempts: {last}"
    )


def metrics(vals):
    wins = [x for x in vals if x > 0]
    losses = [x for x in vals if x < 0]
    total = sum(vals)

    return {
        "trades": len(vals),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(
            100.0 * len(wins) / len(vals), 2
        ) if vals else 0.0,
        "profit_factor": round(pf(vals), 6),
        "sum_net_return_pct": round(total, 6),
        "avg_net_return_pct": round(
            total / len(vals), 6
        ) if vals else 0.0,
    }


def classify_wallet(rows):
    """
    Practical screening label, not a live-trading approval.

    PASS:
      - at 20s delay, >=50% of paired trades remain followable
      - at 20s delay + base 6.5% + extra 5% cost:
          >=200 trades, PF > 1.10, sum > 0
      - at 30s delay + base 6.5% + extra 10% cost:
          PF >= 0.90 (does not collapse completely)

    BORDERLINE:
      - at 20s delay + extra 2%:
          >=100 trades, PF > 1.0, sum > 0

    Otherwise FAIL.
    """
    by_key = {
        (int(r["delay_sec"]), float(r["extra_cost_pct"])): r
        for r in rows
    }

    a = by_key.get((20, 5.0))
    b = by_key.get((30, 10.0))
    c = by_key.get((20, 2.0))

    if (
        a and b
        and a["followable_rate_pct"] >= 50.0
        and a["trades"] >= 200
        and a["profit_factor"] > 1.10
        and a["sum_net_return_pct"] > 0
        and b["profit_factor"] >= 0.90
    ):
        return "PASS"

    if (
        c
        and c["trades"] >= 100
        and c["profit_factor"] > 1.0
        and c["sum_net_return_pct"] > 0
    ):
        return "BORDERLINE"

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
        "MEME2_STRESS_START "
        f"wallets={len(TARGET_WALLETS)} "
        f"delays={DELAY_SECONDS} "
        f"base_cost_pct={BASE_COST_PCT} "
        f"extra_costs={EXTRA_COST_PCTS} "
        "historical_only=true",
        flush=True,
    )

    all_summary = []
    wallet_classifications = []

    for idx, wallet in enumerate(TARGET_WALLETS, start=1):
        meta = m_wallets.get(wallet)
        if not meta:
            raise RuntimeError(f"wallet missing from manifest: {wallet}")

        raw_file = str(meta.get("raw_file") or "")
        raw_url = f"{BASE}/{raw_file.lstrip('/')}"

        print(
            f"MEME2_STRESS_WALLET_START wallet={idx}/{len(TARGET_WALLETS)} "
            f"{wallet[:8]}.. raw_rows={meta.get('rows')}",
            flush=True,
        )

        pairs, counters = scan_wallet_with_retry(wallet, raw_url)
        total_pairs = len(pairs)
        holds = [float(x["hold_sec"]) for x in pairs]

        wallet_rows = []

        for delay in DELAY_SECONDS:
            followable = [
                x for x in pairs
                if float(x["hold_sec"]) > float(delay)
            ]

            followable_rate = (
                100.0 * len(followable) / total_pairs
                if total_pairs else 0.0
            )

            for extra_cost in EXTRA_COST_PCTS:
                total_cost = BASE_COST_PCT + extra_cost
                vals = [
                    float(x["raw_return_pct"]) - total_cost
                    for x in followable
                ]
                m = metrics(vals)

                row = {
                    "wallet": wallet,
                    "delay_sec": delay,
                    "base_cost_pct": BASE_COST_PCT,
                    "extra_cost_pct": extra_cost,
                    "total_cost_pct": total_cost,
                    "paired_trades_total": total_pairs,
                    "followable_trades": len(followable),
                    "unfollowable_trades": total_pairs - len(followable),
                    "followable_rate_pct": round(followable_rate, 2),
                    "median_hold_sec": round(
                        statistics.median(holds), 2
                    ) if holds else None,
                    "p25_hold_sec": round(
                        pctile(holds, 0.25), 2
                    ) if holds else None,
                    **m,
                }

                wallet_rows.append(row)
                all_summary.append(row)

                print(
                    "MEME2_STRESS_RESULT "
                    + json.dumps(
                        {
                            "wallet": wallet[:8],
                            "delay_sec": delay,
                            "extra_cost_pct": extra_cost,
                            "followable_rate_pct": row["followable_rate_pct"],
                            "trades": row["trades"],
                            "pf": row["profit_factor"],
                            "sum_net_return_pct": row["sum_net_return_pct"],
                        },
                        separators=(",", ":"),
                    ),
                    flush=True,
                )

        classification = classify_wallet(wallet_rows)
        key20_5 = next(
            r for r in wallet_rows
            if r["delay_sec"] == 20 and r["extra_cost_pct"] == 5.0
        )

        wallet_classifications.append({
            "wallet": wallet,
            "classification": classification,
            "paired_trades_total": total_pairs,
            "median_hold_sec": key20_5["median_hold_sec"],
            "followable_rate_20s_pct": key20_5["followable_rate_pct"],
            "trades_20s_extra5": key20_5["trades"],
            "pf_20s_extra5": key20_5["profit_factor"],
            "sum_20s_extra5": key20_5["sum_net_return_pct"],
        })

        print(
            "MEME2_STRESS_WALLET_DONE "
            + json.dumps(
                {
                    "wallet": wallet[:8],
                    "classification": classification,
                    "paired_trades": total_pairs,
                    "median_hold_sec": key20_5["median_hold_sec"],
                    "followable_rate_20s_pct": key20_5[
                        "followable_rate_pct"
                    ],
                    "pf_20s_extra5": key20_5["profit_factor"],
                    "sum_20s_extra5": key20_5["sum_net_return_pct"],
                },
                separators=(",", ":"),
            ),
            flush=True,
        )

    # Sort PASS -> BORDERLINE -> FAIL, then stronger 20s/+5% PF.
    order = {"PASS": 0, "BORDERLINE": 1, "FAIL": 2}
    wallet_classifications.sort(
        key=lambda r: (
            order.get(r["classification"], 9),
            -float(r["pf_20s_extra5"]),
            -int(r["trades_20s_extra5"]),
        )
    )

    write_csv(
        OUT / "meme2_delay_stress_summary.csv",
        all_summary,
    )
    write_csv(
        OUT / "meme2_delay_stress_wallet_classification.csv",
        wallet_classifications,
    )

    report = {
        "purpose": "Meme2 delay/cost robustness screening",
        "limitations": [
            "No exact delayed historical DEX price snapshots are available.",
            "Delay is modeled first as followability: hold_sec must exceed delay.",
            "Additional adverse cost is stressed independently from delay.",
            "Results are screening only, not final live-equivalent PF.",
        ],
        "delays_sec": DELAY_SECONDS,
        "base_cost_pct": BASE_COST_PCT,
        "extra_cost_pct": EXTRA_COST_PCTS,
        "wallets": wallet_classifications,
    }

    (OUT / "meme2_delay_stress_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    for row in wallet_classifications:
        print(
            "MEME2_STRESS_CLASSIFICATION "
            + json.dumps(row, separators=(",", ":")),
            flush=True,
        )

    print(
        "MEME2_STRESS_DONE "
        "summary=data/bf1_v3/meme2_delay_stress_summary.csv "
        "classification=data/bf1_v3/"
        "meme2_delay_stress_wallet_classification.csv "
        "report=data/bf1_v3/meme2_delay_stress_report.json",
        flush=True,
    )


if __name__ == "__main__":
    main()
