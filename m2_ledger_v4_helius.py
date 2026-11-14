#!/usr/bin/env python3
# m2_ledger_v4_helius.py
#
# M2 wallet-vs-copy execution ledger
# - Fresh-only dataset cutoff
# - Helius Parsed Events first (Jupiter/multi-hop/inner instructions)
# - Raw Solana RPC fallback
# - No secret hardcoded: HELIUS_API_KEY must be set in Render env
# - Price quality is explicit; proxy USD prices are never labeled exact
#
# Render Start Command:
#   python m2_ledger_v4_helius.py
#
# Required env:
#   HELIUS_API_KEY=<your Helius key>
#
# Optional env:
#   M2N_URL=https://m2n-3tfx.onrender.com
#   M2_COPY_URL=https://m2qq-fast-v3.onrender.com
#   M2_LEDGER_POLL_SEC=5
#   M2_DATASET_START_TS=2026-11-14T21:53:07Z
#   MEME2_RPC_HTTP=https://solana-rpc.publicnode.com

import json
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import meme2_wallet_copy_job as old

PORT = int(os.getenv("PORT", "10000"))
M2N = os.getenv("M2N_URL", "https://m2n-3tfx.onrender.com")
COPY = os.getenv("M2_COPY_URL", "https://m2qq-fast-v3.onrender.com")
POLL = float(os.getenv("M2_LEDGER_POLL_SEC", "5"))
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY", "").strip()
HELIUS_MIN_GAP_SEC = float(os.getenv("HELIUS_MIN_GAP_SEC", "0.35"))
HELIUS_RETRIES = int(os.getenv("HELIUS_RETRIES", "4"))
HELIUS_RETRY_BASE_SEC = float(os.getenv("HELIUS_RETRY_BASE_SEC", "1.0"))
DATASET_START_ISO = os.getenv("M2_DATASET_START_TS", "2026-11-14T21:53:07Z")

_HELIUS_RATE_LOCK = threading.Lock()
_HELIUS_LAST_AT = 0.0

old.RPC_HTTP = os.getenv("MEME2_RPC_HTTP", "https://solana-rpc.publicnode.com")
old.HTTP_RETRIES = int(os.getenv("MEME2_HTTP_RETRIES", "3"))
old.HTTP_RETRY_BASE_SEC = float(os.getenv("MEME2_HTTP_RETRY_BASE_SEC", "1.0"))

WSOL = old.WSOL
USDC = old.USDC
USDT = old.USDT

ROOT = Path("data/m2_ledger_v4")
ROOT.mkdir(parents=True, exist_ok=True)
STATUS = ROOT / "status.json"

S = {
    "seen": set(),
    "events": {},
    "wallet_open": {},
    "rows": {},
    "errors": [],
    "scans": 0,
    "helius_ok": 0,
    "helius_fail": 0,
    "helius_retry_429": 0,
    "fallback_ok": 0,
    "cutoff_reject": 0,
}

def now():
    return time.time()

def iso(ts=None):
    d = datetime.fromtimestamp(float(ts), tz=timezone.utc) if ts is not None else datetime.now(timezone.utc)
    return d.isoformat().replace("+00:00", "Z")

def parse_iso(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()

DATASET_START_TS = parse_iso(DATASET_START_ISO)

def finite_float(x, default=None):
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default

def fetch_json(url, method="GET", body=None, headers=None, timeout=20):
    h = {"User-Agent": "M2LEDGER-V4/1.0", "Accept": "application/json"}
    if headers:
        h.update(headers)
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        h["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, method=method, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))

def fetch(url):
    return fetch_json(url)

def err(where, e):
    x = {"time": iso(), "where": where, "error": type(e).__name__ + ":" + str(e)[:220]}
    S["errors"] = (S["errors"] + [x])[-30:]
    print("M2LEDGER_V4_ERROR " + json.dumps(x, separators=(",", ":")), flush=True)

def tokamt(b):
    ui = (b or {}).get("uiTokenAmount") or {}
    try:
        if ui.get("uiAmountString") is not None:
            return float(ui["uiAmountString"])
        return float(ui.get("amount") or 0) / (10 ** int(ui.get("decimals") or 0))
    except Exception:
        return 0.0

def mint_decimals_from_tx(tx):
    out = {WSOL: 9, USDC: 6, USDT: 6}
    meta = (tx or {}).get("meta") or {}
    for side in ("preTokenBalances", "postTokenBalances"):
        for b in meta.get(side) or []:
            mint = b.get("mint")
            ui = b.get("uiTokenAmount") or {}
            if mint and ui.get("decimals") is not None:
                try:
                    out[mint] = int(ui["decimals"])
                except Exception:
                    pass
    return out

def raw_to_ui(raw, decimals):
    try:
        return float(raw) / (10 ** int(decimals))
    except Exception:
        return None

def owned_deltas(wallet, tx):
    meta = (tx or {}).get("meta") or {}
    msg = (((tx or {}).get("transaction") or {}).get("message") or {})
    keys = msg.get("accountKeys") or []
    pubs = [k.get("pubkey") if isinstance(k, dict) else k for k in keys]

    native = 0.0
    try:
        i = pubs.index(wallet)
        native = (float((meta.get("postBalances") or [])[i]) -
                  float((meta.get("preBalances") or [])[i])) / 1e9
    except Exception:
        pass

    pre, post = {}, {}
    for b in meta.get("preTokenBalances") or []:
        if b.get("owner") == wallet and b.get("mint"):
            pre[b["mint"]] = tokamt(b)
    for b in meta.get("postTokenBalances") or []:
        if b.get("owner") == wallet and b.get("mint"):
            post[b["mint"]] = tokamt(b)

    d = {m: float(post.get(m, 0)) - float(pre.get(m, 0)) for m in set(pre) | set(post)}
    return native, d

def jup_prices(mints):
    ids = [m for m in dict.fromkeys(mints) if m]
    if not ids:
        return {}
    try:
        url = "https://api.jup.ag/price/v3?ids=" + urllib.parse.quote(",".join(ids), safe=",")
        d = fetch_json(url, timeout=15)
        out = {}
        for m in ids:
            try:
                p = float((d.get(m) or {}).get("usdPrice") or 0)
                if p > 0:
                    out[m] = p
            except Exception:
                pass
        return out
    except Exception:
        return {}

def normalize_helius_result(payload):
    # Parsed Events Parse Transactions can be returned as:
    #   [tx, ...]
    #   {"data":[tx,...]}
    #   {"result":[tx,...]}
    # Keep this tolerant while the API is in open beta.
    if isinstance(payload, list):
        return payload[0] if payload else None
    if isinstance(payload, dict):
        for k in ("data", "result", "transactions"):
            v = payload.get(k)
            if isinstance(v, list):
                return v[0] if v else None
            if isinstance(v, dict) and v:
                return v
        if payload.get("summary") or payload.get("signature"):
            return payload
    return None

def helius_parsed_event(signature):
    global _HELIUS_LAST_AT

    if not HELIUS_API_KEY:
        return None

    url = (
        "https://mainnet.helius-rpc.com/v1/parsed-events/transactions"
        "?api-key=" + urllib.parse.quote(HELIUS_API_KEY, safe="")
    )

    # Serialize Parsed Events calls and keep a minimum gap between requests.
    # This mainly protects service restarts, when recent events can arrive as a burst.
    with _HELIUS_RATE_LOCK:
        gap = max(0.0, HELIUS_MIN_GAP_SEC - (time.time() - _HELIUS_LAST_AT))
        if gap > 0:
            time.sleep(gap)

        last_exc = None
        for attempt in range(max(1, HELIUS_RETRIES)):
            try:
                payload = fetch_json(
                    url,
                    method="POST",
                    body={"transactions": [signature]},
                    timeout=25,
                )
                _HELIUS_LAST_AT = time.time()
                return normalize_helius_result(payload)

            except urllib.error.HTTPError as exc:
                last_exc = exc
                _HELIUS_LAST_AT = time.time()

                if exc.code != 429 or attempt >= HELIUS_RETRIES - 1:
                    raise

                retry_after = exc.headers.get("Retry-After") if exc.headers else None
                try:
                    delay = float(retry_after) if retry_after else HELIUS_RETRY_BASE_SEC * (2 ** attempt)
                except Exception:
                    delay = HELIUS_RETRY_BASE_SEC * (2 ** attempt)

                delay = max(HELIUS_MIN_GAP_SEC, delay)
                S["helius_retry_429"] += 1
                print(
                    "M2LEDGER_V4_HELIUS_429 "
                    + json.dumps(
                        {
                            "attempt": attempt + 1,
                            "retry_in_sec": round(delay, 3),
                            "signature": signature[:10],
                        },
                        separators=(",", ":"),
                    ),
                    flush=True,
                )
                time.sleep(delay)

            except Exception as exc:
                last_exc = exc
                _HELIUS_LAST_AT = time.time()

                if attempt >= HELIUS_RETRIES - 1:
                    raise

                delay = max(
                    HELIUS_MIN_GAP_SEC,
                    HELIUS_RETRY_BASE_SEC * (2 ** attempt),
                )
                time.sleep(delay)

        if last_exc:
            raise last_exc
        return None

def pick_swap_parsed_data(h):
    if not isinstance(h, dict):
        return None

    # Preferred transaction-level summary.
    summary = h.get("summary") or {}
    pd = summary.get("parsedData") or summary.get("parsed_data")
    if isinstance(pd, dict):
        if pd.get("input_mint") and pd.get("output_mint"):
            return pd

    # Open-beta schema tolerance: recursively find a Jupiter/swap-like parsedData.
    stack = [h]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            for k, v in cur.items():
                if k in ("parsedData", "parsed_data") and isinstance(v, dict):
                    if v.get("input_mint") and v.get("output_mint"):
                        return v
                if isinstance(v, (dict, list)):
                    stack.append(v)
        elif isinstance(cur, list):
            stack.extend(cur)
    return None

def parsed_field(d, *names):
    for n in names:
        if n in d and d[n] is not None:
            return d[n]
    return None

def block_time_from_helius(h, fallback):
    for k in ("blockTime", "block_time", "timestamp"):
        v = h.get(k) if isinstance(h, dict) else None
        fv = finite_float(v)
        if fv is not None and fv > 0:
            return fv
    return fallback

def exact_price_record(base, quote_mint, quote_qty, token_qty, quote_kind, quality, usd_method=None):
    if not quote_qty or not token_qty or quote_qty <= 0 or token_qty <= 0:
        return None
    px_quote = quote_qty / token_qty
    px_usd = None
    method = usd_method

    if quote_mint in (USDC, USDT) or quote_kind == "USD_STABLE":
        px_usd = px_quote
        method = "EXACT_STABLE"
    elif quote_mint == WSOL or quote_kind == "SOL":
        solusd = jup_prices([WSOL]).get(WSOL)
        px_usd = px_quote * solusd if solusd else None
        method = "SOL_X_NEARBY_JUPITER" if px_usd else None
    elif quote_mint:
        qusd = jup_prices([quote_mint]).get(quote_mint)
        px_usd = px_quote * qusd if qusd else None
        method = "QUOTE_TOKEN_X_NEARBY_JUPITER" if px_usd else None

    return {
        **base,
        "price_status": quality,
        "price_quality": quality,
        "quote_kind": quote_kind,
        "quote_mint": quote_mint,
        "quote_qty": quote_qty,
        "token_qty": token_qty,
        "wallet_price_quote": px_quote,
        "wallet_price_usd": px_usd,
        "usd_price_method": method,
    }

def parse_from_helius(ev, h, raw_tx=None):
    w = str(ev.get("wallet") or "")
    target_mint = str(ev.get("mint") or "")
    side = str(ev.get("side") or "").upper()
    fallback_ts = float((raw_tx or {}).get("blockTime") or ev.get("t") or now())
    bt = block_time_from_helius(h or {}, fallback_ts)

    base = {
        "wallet": w,
        "mint": target_mint,
        "side": side,
        "signature": ev.get("signature"),
        "block_ts": bt,
        "block_time": iso(bt),
        "feed_ts": float(ev.get("t") or 0),
        "feed_seen_delay_sec": round(float(ev.get("t") or bt) - bt, 3),
        "parser": "HELIUS_PARSED_EVENTS",
    }

    pd = pick_swap_parsed_data(h)
    if not pd:
        return None

    in_mint = str(parsed_field(pd, "input_mint", "inputMint") or "")
    out_mint = str(parsed_field(pd, "output_mint", "outputMint") or "")
    in_raw = parsed_field(pd, "in_amount", "inAmount", "input_amount", "inputAmount")
    out_raw = parsed_field(pd, "actual_out_amount", "actualOutAmount", "out_amount", "outAmount")

    if not in_mint or not out_mint or in_raw is None or out_raw is None:
        return None

    dec = mint_decimals_from_tx(raw_tx or {})
    # Known decimals first. For target/route tokens, raw Solana tx normally supplies decimals.
    in_dec = dec.get(in_mint)
    out_dec = dec.get(out_mint)
    if in_dec is None or out_dec is None:
        return {
            **base,
            "price_status": "HELIUS_SWAP_DECIMALS_MISSING",
            "price_quality": "UNRESOLVED",
            "input_mint": in_mint,
            "output_mint": out_mint,
        }

    in_qty = raw_to_ui(in_raw, in_dec)
    out_qty = raw_to_ui(out_raw, out_dec)
    if not in_qty or not out_qty:
        return None

    # BUY target: quote -> target.
    if side == "BUY" and out_mint == target_mint:
        quote_mint = in_mint
        quote_qty = in_qty
        token_qty = out_qty
    # SELL target: target -> quote.
    elif side == "SELL" and in_mint == target_mint:
        quote_mint = out_mint
        quote_qty = out_qty
        token_qty = in_qty
    else:
        # Some routers wrap/unwrap SOL or report an adjacent route leg.
        # Do not guess a price if the target is not the actual input/output.
        return {
            **base,
            "price_status": "HELIUS_SWAP_TARGET_NOT_EDGE",
            "price_quality": "UNRESOLVED",
            "input_mint": in_mint,
            "output_mint": out_mint,
            "in_qty": in_qty,
            "out_qty": out_qty,
        }

    if quote_mint in (USDC, USDT):
        kind, quality = "USD_STABLE", "EXACT_STABLE"
    elif quote_mint == WSOL:
        kind, quality = "SOL", "EXACT_SOL"
    else:
        kind, quality = "TOKEN", "EXACT_ROUTED"

    rec = exact_price_record(
        base, quote_mint, quote_qty, token_qty, kind, quality
    )
    if rec:
        rec.update({
            "input_mint": in_mint,
            "output_mint": out_mint,
            "input_qty": in_qty,
            "output_qty": out_qty,
            "protocol": parsed_field(pd, "protocol"),
        })
    return rec

def parse_wallet_fill_raw(ev, tx):
    w = str(ev.get("wallet") or "")
    m = str(ev.get("mint") or "")
    side = str(ev.get("side") or "").upper()
    native, d = owned_deltas(w, tx)
    target = float(d.get(m, 0))
    qty = abs(target)
    block_ts = float((tx or {}).get("blockTime") or ev.get("t") or now())
    base = {
        "wallet": w,
        "mint": m,
        "side": side,
        "signature": ev.get("signature"),
        "block_ts": block_ts,
        "block_time": iso(block_ts),
        "token_qty": qty,
        "target_delta": target,
        "feed_ts": float(ev.get("t") or 0),
        "feed_seen_delay_sec": round(float(ev.get("t") or block_ts) - block_ts, 3),
        "parser": "RAW_RPC_FALLBACK",
    }
    if qty <= 0:
        return {**base, "price_status": "UNRESOLVED_NO_TARGET", "price_quality": "UNRESOLVED"}

    stable = float(d.get(USDC, 0)) + float(d.get(USDT, 0))
    sol = native + float(d.get(WSOL, 0))
    want_neg = side == "BUY"
    okstable = stable < 0 if want_neg else stable > 0
    oksol = sol < 0 if want_neg else sol > 0

    if okstable:
        return exact_price_record(
            base, "USD", abs(stable), qty, "USD_STABLE", "RAW_EXACT_STABLE"
        )

    if oksol:
        return exact_price_record(
            base, WSOL, abs(sol), qty, "SOL", "RAW_EXACT_SOL"
        )

    c = []
    for qm, qd in d.items():
        if qm in (m, USDC, USDT, WSOL) or abs(qd) <= 1e-12:
            continue
        if (side == "BUY" and qd < 0) or (side == "SELL" and qd > 0):
            c.append((qm, qd))

    if len(c) == 1:
        qm, qd = c[0]
        return exact_price_record(
            base, qm, abs(qd), qty, "TOKEN", "RAW_EXACT_ROUTED"
        )

    return {
        **base,
        "price_status": "UNRESOLVED",
        "price_quality": "UNRESOLVED",
        "quote_candidates": len(c),
    }

def parse_wallet_fill(ev, tx):
    sig = str(ev.get("signature") or "")

    # Helius first.
    if HELIUS_API_KEY and sig:
        try:
            h = helius_parsed_event(sig)
            if h:
                rec = parse_from_helius(ev, h, tx)
                if rec and rec.get("price_quality") != "UNRESOLVED":
                    S["helius_ok"] += 1
                    return rec
                if rec:
                    # Keep reason around but still try raw fallback.
                    helius_reason = rec.get("price_status")
                else:
                    helius_reason = "HELIUS_NO_SWAP"
            else:
                helius_reason = "HELIUS_EMPTY"
        except Exception as e:
            S["helius_fail"] += 1
            helius_reason = "HELIUS_ERROR:" + type(e).__name__
            err("helius_" + sig[:10], e)
    else:
        helius_reason = "HELIUS_DISABLED"

    rec = parse_wallet_fill_raw(ev, tx or {})
    if rec.get("price_quality") != "UNRESOLVED":
        S["fallback_ok"] += 1
    rec["helius_fallback_reason"] = helius_reason
    return rec

def event_key(ev):
    return "|".join([
        str(ev.get("signature") or ""),
        str(ev.get("wallet") or ""),
        str(ev.get("mint") or ""),
        str(ev.get("side") or "").upper(),
    ])

def ingest_wallet_events(m2n):
    evs = sorted(m2n.get("recent_events") or [], key=lambda x: float(x.get("t") or 0))

    for ev in evs:
        sig = str(ev.get("signature") or "")
        side = str(ev.get("side") or "").upper()
        if not sig or side not in ("BUY", "SELL"):
            continue

        ek = event_key(ev)
        if ek in S["seen"]:
            continue

        # Fixed dataset cutoff: anything observed before the official start is never ingested.
        feed_ts = float(ev.get("t") or 0)
        if feed_ts and feed_ts < DATASET_START_TS:
            S["seen"].add(ek)
            S["cutoff_reject"] += 1
            continue

        try:
            tx = old.fetch_transaction(sig)
            if not tx:
                fill = {
                    "wallet": ev.get("wallet"),
                    "mint": ev.get("mint"),
                    "side": side,
                    "signature": sig,
                    "price_status": "TX_MISSING",
                    "price_quality": "UNRESOLVED",
                    "block_ts": feed_ts or now(),
                }
            else:
                block_ts = finite_float(tx.get("blockTime"), feed_ts or now())
                if block_ts < DATASET_START_TS:
                    S["seen"].add(ek)
                    S["cutoff_reject"] += 1
                    continue
                fill = parse_wallet_fill(ev, tx)

            S["seen"].add(ek)
            S["events"][ek] = fill

            key = str(ev.get("wallet") or "") + "|" + str(ev.get("mint") or "")
            if side == "BUY":
                S["wallet_open"][key] = ek
                row = S["rows"].setdefault(
                    ek,
                    {
                        "source_buy_event_key": ek,
                        "source_buy_signature": sig,
                        "wallet": ev.get("wallet"),
                        "mint": ev.get("mint"),
                    },
                )
                row["wallet_buy"] = fill
            else:
                bek = S["wallet_open"].get(key)
                if bek:
                    row = S["rows"].setdefault(
                        bek,
                        {
                            "source_buy_event_key": bek,
                            "wallet": ev.get("wallet"),
                            "mint": ev.get("mint"),
                        },
                    )
                    row["wallet_sell"] = fill
                    if bek in S["events"]:
                        row["wallet_buy"] = S["events"][bek]
                    S["wallet_open"].pop(key, None)

            print(
                "M2LEDGER_V4_WALLET "
                + json.dumps(
                    {
                        "side": side,
                        "wallet": str(ev.get("wallet") or "")[:8],
                        "mint": str(ev.get("mint") or "")[:8],
                        "status": fill.get("price_status"),
                        "quality": fill.get("price_quality"),
                        "price_quote": fill.get("wallet_price_quote"),
                        "price_usd": fill.get("wallet_price_usd"),
                        "quote": str(fill.get("quote_mint") or "")[:8],
                        "block_ts": fill.get("block_ts"),
                    },
                    separators=(",", ":"),
                ),
                flush=True,
            )
        except Exception as e:
            err("wallet_event_" + sig[:10], e)

def find_row_by_buy_signature(bsig):
    if not bsig:
        return None
    for r in S["rows"].values():
        if str(r.get("source_buy_signature") or "") == bsig:
            return r
    return None

def sync_copy(copy):
    ps = list(copy.get("open_positions") or []) + list(copy.get("closed_recent") or [])
    for p in ps:
        bsig = str(p.get("source_buy_signature") or "")
        if not bsig:
            continue

        row = find_row_by_buy_signature(bsig)
        if row is None:
            # Do not fabricate a wallet row from copy-only history.
            # This prevents old M2QQ state from creating polluted source samples.
            continue

        row["copy_buy"] = {
            "time": p.get("entry_t"),
            "ts": p.get("entry_ts"),
            "price_raw": p.get("entry_raw"),
            "price_exec": p.get("entry"),
            "feed_delay_sec": p.get("buy_delay_from_feed_sec"),
        }

        if p.get("status") == "CLOSED":
            row["copy_sell"] = {
                "time": p.get("exit_t"),
                "ts": p.get("exit_ts"),
                "price_exec": p.get("exit"),
                "reason": p.get("reason"),
                "net_pct": p.get("net_pct"),
                "source_sell_signature": p.get("exit_signature"),
                "feed_delay_sec": p.get("sell_delay_from_feed_sec"),
            }

        enrich(row)

def enrich(r):
    wb = r.get("wallet_buy") or {}
    ws = r.get("wallet_sell") or {}
    cb = r.get("copy_buy") or {}
    cs = r.get("copy_sell") or {}

    try:
        if cb.get("ts") and wb.get("block_ts") is not None:
            r["buy_delay_sec"] = round(float(cb["ts"]) - float(wb["block_ts"]), 3)
    except Exception:
        pass

    try:
        if cs.get("ts") and ws.get("block_ts") is not None:
            r["sell_delay_sec"] = round(float(cs["ts"]) - float(ws["block_ts"]), 3)
    except Exception:
        pass

    # Prefer same-quote return: avoids contaminating wallet return with a current USD proxy.
    if (
        wb.get("quote_mint")
        and wb.get("quote_mint") == ws.get("quote_mint")
        and wb.get("wallet_price_quote")
        and ws.get("wallet_price_quote")
    ):
        try:
            r["wallet_return_pct"] = round(
                (float(ws["wallet_price_quote"]) / float(wb["wallet_price_quote"]) - 1) * 100,
                4,
            )
            r["wallet_return_method"] = "SAME_QUOTE_EXACT"
        except Exception:
            pass
    elif wb.get("wallet_price_usd") and ws.get("wallet_price_usd"):
        # Can be exact for stable quote; proxy for SOL/routed token USD conversion.
        try:
            r["wallet_return_pct"] = round(
                (float(ws["wallet_price_usd"]) / float(wb["wallet_price_usd"]) - 1) * 100,
                4,
            )
            r["wallet_return_method"] = "USD_CONVERTED"
        except Exception:
            pass

    if cb.get("price_raw") and cs.get("price_exec"):
        try:
            raw_exit = float(cs["price_exec"]) / 0.98
            r["copy_raw_pct"] = round(
                (raw_exit / float(cb["price_raw"]) - 1) * 100, 4
            )
        except Exception:
            pass

    if cs.get("net_pct") is not None:
        r["copy_net_pct"] = cs.get("net_pct")

    if r.get("wallet_return_pct") is not None and r.get("copy_net_pct") is not None:
        r["edge_gap_pct"] = round(
            float(r["copy_net_pct"]) - float(r["wallet_return_pct"]), 4
        )

    r["price_quality_buy"] = wb.get("price_quality")
    r["price_quality_sell"] = ws.get("price_quality")
    r["source_complete"] = bool(
        wb
        and ws
        and wb.get("price_quality") != "UNRESOLVED"
        and ws.get("price_quality") != "UNRESOLVED"
        and r.get("wallet_return_pct") is not None
    )
    r["copy_complete"] = bool(cb and cs)
    r["complete"] = bool(r["source_complete"] and r["copy_complete"])

def persist(copy):
    for r in S["rows"].values():
        enrich(r)

    rows = sorted(
        S["rows"].values(),
        key=lambda x: float(
            ((x.get("copy_buy") or {}).get("ts")
             or (x.get("wallet_buy") or {}).get("block_ts")
             or 0)
        ),
        reverse=True,
    )
    source_complete = [r for r in rows if r.get("source_complete")]
    complete = [r for r in rows if r.get("complete")]

    d = {
        "status": "ok",
        "core": "M2_LEDGER_V4_HELIUS",
        "mode": "WALLET_VS_COPY_EXECUTION_LEDGER",
        "paper_only": True,
        "updated": iso(),
        "dataset_start_utc": DATASET_START_ISO,
        "helius_enabled": bool(HELIUS_API_KEY),
        "scans": S["scans"],
        "wallet_events": len(S["events"]),
        "rows": len(rows),
        "source_complete_rows": len(source_complete),
        "complete_rows": len(complete),
        "helius_ok": S["helius_ok"],
        "helius_fail": S["helius_fail"],
        "helius_retry_429": S["helius_retry_429"],
        "fallback_ok": S["fallback_ok"],
        "cutoff_reject": S["cutoff_reject"],
        "copy_open": copy.get("open", 0),
        "copy_closed": copy.get("closed", 0),
        "errors": S["errors"][-10:],
        "recent_rows": rows[:100],
    }

    STATUS.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    return d

class H(BaseHTTPRequestHandler):
    def do_GET(self):
        b = (
            STATUS.read_bytes()
            if STATUS.exists()
            else b'{"status":"starting","core":"M2_LEDGER_V4_HELIUS"}'
        )
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def log_message(self, *args):
        pass

threading.Thread(
    target=lambda: ThreadingHTTPServer(("0.0.0.0", PORT), H).serve_forever(),
    daemon=True,
).start()

print(
    "M2LEDGER_V4_START "
    + json.dumps(
        {
            "source": "M2N",
            "copy": "M2QQ_FAST_V3",
            "helius_enabled": bool(HELIUS_API_KEY),
            "dataset_start_utc": DATASET_START_ISO,
            "fields": [
                "wallet_buy_price",
                "copy_buy_price",
                "buy_delay",
                "wallet_sell_price",
                "copy_sell_price",
                "sell_delay",
                "wallet_return",
                "copy_raw",
                "copy_net",
                "price_quality",
            ],
        },
        separators=(",", ":"),
    ),
    flush=True,
)

while True:
    st = now()
    try:
        m2n = fetch(M2N)
        copy = fetch(COPY)
        ingest_wallet_events(m2n)
        sync_copy(copy)
        S["scans"] += 1
        d = persist(copy)
        print(
            "M2LEDGER_V4_STATUS "
            + json.dumps(
                {
                    "scans": d["scans"],
                    "events": d["wallet_events"],
                    "rows": d["rows"],
                    "source_complete": d["source_complete_rows"],
                    "complete": d["complete_rows"],
                    "helius_ok": d["helius_ok"],
                    "helius_fail": d["helius_fail"],
                    "helius_retry_429": d["helius_retry_429"],
                    "fallback_ok": d["fallback_ok"],
                    "copy_open": d["copy_open"],
                    "copy_closed": d["copy_closed"],
                    "errors": len(d["errors"]),
                },
                separators=(",", ":"),
            ),
            flush=True,
        )
    except Exception as e:
        err("loop", e)

    time.sleep(max(3, POLL - (now() - st)))
