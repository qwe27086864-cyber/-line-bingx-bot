from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import historical_data as hd

NAME = "BACKTEST-3Y"
ROOT = Path(os.environ.get("BACKTEST3_DATA_DIR", "data/backtest_3y"))
ROOT.mkdir(parents=True, exist_ok=True)

SYMBOLS = [
    s.strip().upper()
    for s in os.environ.get(
        "BACKTEST3_SYMBOLS",
        "BTC-USDT,ETH-USDT,SOL-USDT,XRP-USDT,DOGE-USDT,LTC-USDT,TRX-USDT,BCH-USDT,DOT-USDT,UNI-USDT",
    ).split(",")
    if s.strip()
]

DAYS = int(os.environ.get("BACKTEST3_DAYS", "1096"))
SEGMENT_DAYS = int(os.environ.get("BACKTEST3_SEGMENT_DAYS", "30"))
REQUEST_PAUSE = float(os.environ.get("BACKTEST3_REQUEST_PAUSE_SEC", "0.8"))
MIN_COVERAGE = float(os.environ.get("BACKTEST3_MIN_COVERAGE", "0.95"))
END_ISO = os.environ.get("BACKTEST3_END_ISO", "2026-11-02T11:00:00+00:00").strip()


def dt(value: str) -> datetime:
    s = value.replace("Z", "+00:00")
    out = datetime.fromisoformat(s)
    if out.tzinfo is None:
        out = out.replace(tzinfo=timezone.utc)
    return out.astimezone(timezone.utc)


END = dt(END_ISO)
START = END - timedelta(days=DAYS)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def rowcount(path: Path) -> int:
    with path.open(newline="", encoding="utf-8-sig") as f:
        return max(sum(1 for _ in f) - 1, 0)


def ranges():
    out = []
    a = START
    while a < END:
        b = min(a + timedelta(days=SEGMENT_DAYS), END)
        out.append((a, b))
        a = b
    return out


def verify_1m(path: Path, start: datetime, end: datetime) -> dict[str, int]:
    seen = {s: set() for s in SYMBOLS}
    sm, em = hd.utc_ms(start), hd.utc_ms(end)
    with path.open(newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            s = r["symbol"]
            if s not in seen:
                continue
            ts = hd.utc_ms(datetime.fromisoformat(r["timestamp"]))
            if sm <= ts < em:
                seen[s].add(ts)

    counts = {s: len(v) for s, v in seen.items()}
    expected = int((end - start).total_seconds() // 60)
    minimum = int(expected * MIN_COVERAGE)
    print(
        f"BACKTEST3_1M_COVERAGE expected={expected} minimum={minimum} counts={counts}",
        flush=True,
    )
    if any(n < minimum for n in counts.values()):
        raise RuntimeError(
            f"Insufficient 1m history: {counts}; need >= {minimum} per symbol"
        )
    return counts


def aggregate_from_1m(
    source: Path,
    target: Path,
    interval: str,
    start: datetime,
    end: datetime,
) -> dict[str, int]:
    step_ms = hd.INTERVAL_MS[interval]
    sm, em = hd.utc_ms(start), hd.utc_ms(end)
    groups = {}

    with source.open(newline="", encoding="utf-8-sig") as f:
        for r in csv.DictReader(f):
            ts = hd.utc_ms(datetime.fromisoformat(r["timestamp"]))
            if not (sm <= ts < em):
                continue
            bucket = (ts // step_ms) * step_ms
            key = (r["symbol"], bucket)
            g = groups.get(key)
            if g is None:
                groups[key] = {
                    "first_ts": ts,
                    "last_ts": ts,
                    "open": float(r["open"]),
                    "high": float(r["high"]),
                    "low": float(r["low"]),
                    "close": float(r["close"]),
                    "volume": float(r["volume"] or 0),
                    "quote_volume": float(r["quote_volume"] or 0),
                    "trades": int(float(r["trades"] or 0)),
                    "taker_buy_base": float(r["taker_buy_base"] or 0),
                    "taker_buy_quote": float(r["taker_buy_quote"] or 0),
                    "count": 1,
                }
            else:
                if ts < g["first_ts"]:
                    g["first_ts"] = ts
                    g["open"] = float(r["open"])
                if ts >= g["last_ts"]:
                    g["last_ts"] = ts
                    g["close"] = float(r["close"])
                g["high"] = max(g["high"], float(r["high"]))
                g["low"] = min(g["low"], float(r["low"]))
                g["volume"] += float(r["volume"] or 0)
                g["quote_volume"] += float(r["quote_volume"] or 0)
                g["trades"] += int(float(r["trades"] or 0))
                g["taker_buy_base"] += float(r["taker_buy_base"] or 0)
                g["taker_buy_quote"] += float(r["taker_buy_quote"] or 0)
                g["count"] += 1

    expected_per_bucket = step_ms // 60_000
    rows = []
    for (symbol, bucket), g in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1])):
        if g["count"] != expected_per_bucket:
            continue
        rows.append({
            "timestamp": hd.iso_utc(bucket),
            "symbol": symbol,
            "interval": interval,
            "open": g["open"],
            "high": g["high"],
            "low": g["low"],
            "close": g["close"],
            "volume": g["volume"],
            "quote_volume": g["quote_volume"],
            "trades": g["trades"],
            "taker_buy_base": g["taker_buy_base"],
            "taker_buy_quote": g["taker_buy_quote"],
        })

    with target.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=hd.FIELDS)
        w.writeheader()
        w.writerows(rows)

    counts = {s: 0 for s in SYMBOLS}
    for r in rows:
        counts[r["symbol"]] += 1

    expected = int((end - start).total_seconds() // (step_ms / 1000))
    minimum = max(0, int(expected * MIN_COVERAGE) - 2)
    print(
        f"BACKTEST3_AGG interval={interval} rows={len(rows)} "
        f"expected={expected} minimum={minimum} counts={counts}",
        flush=True,
    )
    if any(n < minimum for n in counts.values()):
        raise RuntimeError(
            f"Insufficient aggregated {interval}: {counts}; need >= {minimum} per symbol"
        )
    return counts


def load_complete_segment(seg: Path):
    meta = seg / "meta.json"
    if not meta.exists():
        return None
    try:
        obj = json.loads(meta.read_text(encoding="utf-8"))
    except Exception:
        return None
    for iv in ("1m", "5m", "15m", "1h"):
        p = seg / f"history_{iv}.csv"
        if not p.exists() or p.stat().st_size == 0:
            return None
    return obj


def run_segment(idx: int, total: int, start: datetime, end: datetime) -> dict:
    seg = ROOT / f"segment_{idx:02d}"
    cached = load_complete_segment(seg)
    if cached:
        print(f"BACKTEST3_SEGMENT_SKIP segment={idx}/{total} reason=complete", flush=True)
        return cached

    if seg.exists():
        shutil.rmtree(seg)
    seg.mkdir(parents=True, exist_ok=True)

    one = seg / "history_1m.csv"
    print(
        f"BACKTEST3_SEGMENT_START segment={idx}/{total} "
        f"start={start.isoformat()} end={end.isoformat()}",
        flush=True,
    )

    sm, em = hd.utc_ms(start), hd.utc_ms(end)
    for n, symbol in enumerate(SYMBOLS, 1):
        rows = hd.download_symbol(symbol, "1m", sm, em, REQUEST_PAUSE)
        hd.append_rows(one, rows)
        print(
            f"BACKTEST3_DOWNLOAD segment={idx}/{total} "
            f"symbol={n}/{len(SYMBOLS)} {symbol} bars={len(rows)}",
            flush=True,
        )
        if n < len(SYMBOLS):
            time.sleep(max(0.0, REQUEST_PAUSE))

    verify_1m(one, start, end)

    for iv in ("5m", "15m", "1h"):
        aggregate_from_1m(one, seg / f"history_{iv}.csv", iv, start, end)

    meta = {
        "segment": idx,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "files": {},
    }
    for iv in ("1m", "5m", "15m", "1h"):
        p = seg / f"history_{iv}.csv"
        meta["files"][iv] = {
            "rows": rowcount(p),
            "sha256": sha256(p),
            "path": f"segment_{idx:02d}/history_{iv}.csv",
        }

    (seg / "meta.json").write_text(
        json.dumps(meta, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print("BACKTEST3_SEGMENT_DONE " + json.dumps(meta, separators=(",", ":")), flush=True)
    return meta


def main():
    spans = ranges()
    manifest = {
        "name": NAME,
        "version": "v1",
        "source": "BingX",
        "historical_only": True,
        "period": {
            "start": START.isoformat(),
            "end": END.isoformat(),
            "days": DAYS,
        },
        "segment_days": SEGMENT_DAYS,
        "segments_target": len(spans),
        "symbols": SYMBOLS,
        "intervals": ["1m", "5m", "15m", "1h"],
        "segments": [],
        "complete": False,
    }
    (ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(
        "BACKTEST3_START "
        + json.dumps(
            {
                "start": START.isoformat(),
                "end": END.isoformat(),
                "days": DAYS,
                "segments": len(spans),
                "symbols": len(SYMBOLS),
                "intervals": ["1m", "5m", "15m", "1h"],
                "higher_intervals": "local_from_1m",
            },
            separators=(",", ":"),
        ),
        flush=True,
    )

    for idx, (start, end) in enumerate(spans, 1):
        meta = run_segment(idx, len(spans), start, end)
        manifest["segments"].append(meta)
        (ROOT / "manifest.json").write_text(
            json.dumps(manifest, indent=2),
            encoding="utf-8",
        )

    manifest["complete"] = True
    (ROOT / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(
        "BACKTEST3_DONE "
        + json.dumps(
            {"segments": len(manifest["segments"]), "complete": True},
            separators=(",", ":"),
        ),
        flush=True,
    )

    while True:
        time.sleep(3600)


if __name__ == "__main__":
    main()
