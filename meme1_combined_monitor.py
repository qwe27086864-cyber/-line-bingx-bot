"""Run MEME1's existing monitor and the V2/V3 shadow engines together.

PAPER / SHADOW ONLY.

This wrapper also applies the same market-data sanity checks used by the main
MEME1 core to the V2/V3 shadow position monitor:
- a >=2x or <=0.5x one-tick move is quarantined;
- the anomalous price must repeat near the same level before it is trusted;
- positive exits are blocked if modeled exit impact is too high.

This preserves real meme pumps while preventing a single API spike from becoming
realized paper PnL.
"""

import threading
import time
import traceback

import meme1_new_pool_job as meme1
import meme1_exitability_shadow_job as meme1x


_price_anomaly_state = {}


def _shadow_price_tick_ok(cfg, pos, p):
    """Require confirmation for an extreme one-tick price move."""
    price = meme1x.sf((p or {}).get("price"), 0.0)
    reference = (
        meme1x.sf(pos.get("last_price"), 0.0)
        or meme1x.sf(pos.get("raw_entry_price"), 0.0)
    )
    if price <= 0 or reference <= 0:
        return False, "MARKET_DATA_MISSING"

    limit = max(1.01, float(getattr(meme1, "MAX_TICK_PRICE_MULTIPLIER", 2.0)))
    ratio = price / reference
    suspicious = ratio >= limit or ratio <= (1.0 / limit)
    key = (cfg["engine"], int(pos["id"]))

    if not suspicious:
        _price_anomaly_state.pop(key, None)
        return True, None

    previous = _price_anomaly_state.get(key)
    hits = 1
    if previous:
        candidate = meme1x.sf(previous.get("price"), 0.0)
        if candidate > 0:
            delta_pct = abs(price / candidate - 1.0) * 100.0
            band = max(
                1.0,
                float(getattr(meme1, "ANOMALY_CONFIRM_BAND_PCT", 25.0)),
            )
            if delta_pct <= band:
                hits = int(previous.get("hits", 1)) + 1

    _price_anomaly_state[key] = {"price": price, "hits": hits}
    required = max(
        2,
        int(getattr(meme1, "ANOMALY_CONFIRMATIONS_REQUIRED", 2)),
    )

    if hits >= required:
        _price_anomaly_state.pop(key, None)
        return True, f"ANOMALOUS_PRICE_CONFIRMED_{hits}"

    return (
        False,
        f"ANOMALOUS_PRICE_TICK ratio={ratio:.2f}x "
        f"confirmation={hits}/{required}",
    )


def _profit_exit_ok(pos, p, reason):
    """Reject a paper profit exit whose modeled market impact is unfillable."""
    if reason not in ("TAKE_PROFIT", "TRAILING_STOP"):
        return True, None

    _, execution_bps = meme1.executable_exit(
        p["price"],
        pos["token_qty"],
        p["liq"],
    )
    max_bps = float(
        getattr(meme1, "MAX_PROFIT_EXIT_EXECUTION_BPS", 5000.0)
    )
    if execution_bps > max_bps:
        return (
            False,
            f"EXIT_IMPACT_TOO_HIGH {execution_bps:.1f}bps > {max_bps:.1f}bps",
        )
    return True, None


def safe_monitor_positions(cfg):
    """Monitor V2/V3 positions with one-tick anomaly protection."""
    opens = meme1x.load_open_positions(cfg)
    if not opens:
        return 0, 0

    snapshots = meme1x.fetch_pools_cached(
        [p["pool_address"] for p in opens]
    )
    closed = 0

    for pos in opens:
        p = snapshots.get(pos["pool_address"])
        if not p or meme1x.sf(p.get("price")) <= 0:
            meme1x.log_position_snapshot(
                cfg,
                pos,
                None,
                peak=meme1x.sf(
                    pos.get("peak_price"),
                    meme1x.sf(pos["raw_entry_price"]),
                ),
                reason_now="MARKET_DATA_MISSING",
            )
            continue

        tick_ok, tick_note = _shadow_price_tick_ok(cfg, pos, p)
        if not tick_ok:
            meme1x.log_position_snapshot(
                cfg,
                pos,
                p,
                peak=meme1x.sf(
                    pos.get("peak_price"),
                    meme1x.sf(pos["raw_entry_price"]),
                ),
                reason_now=tick_note,
            )
            print(
                f"{cfg['engine']} PRICE QUARANTINE #{pos['id']} "
                f"{pos.get('pool_name')} {tick_note}",
                flush=True,
            )
            continue

        peak = max(
            meme1x.sf(pos.get("peak_price"), meme1x.sf(p["price"])),
            meme1x.sf(p["price"]),
        )
        reason = meme1x.position_exit_reason(cfg, pos, p)

        if reason:
            exit_ok, exit_note = _profit_exit_ok(pos, p, reason)
            if not exit_ok:
                meme1x.log_position_snapshot(
                    cfg,
                    pos,
                    p,
                    peak=peak,
                    reason_now=exit_note,
                )
                meme1x.update_position_mark(cfg, pos, p, peak)
                print(
                    f"{cfg['engine']} EXIT QUARANTINE #{pos['id']} "
                    f"{pos.get('pool_name')} {exit_note}",
                    flush=True,
                )
                continue

        snapshot_reason = reason or tick_note
        meme1x.log_position_snapshot(
            cfg,
            pos,
            p,
            peak=peak,
            reason_now=snapshot_reason,
        )

        if reason:
            net, ret = meme1x.close_position(cfg, pos, p, reason)
            closed += 1
            print(
                f"{cfg['engine']} CLOSE #{pos['id']} "
                f"{pos.get('pool_name')} {reason} "
                f"net={net:+.4f}U ret={ret:+.2f}%"
                + (f" [{tick_note}]" if tick_note else ""),
                flush=True,
            )
        else:
            meme1x.update_position_mark(cfg, pos, p, peak)

    return len(opens), closed


def meme1x_loop():
    """Run both V2 and V3 shadow engines without binding their own HTTP port."""
    for cfg in meme1x.ENGINES:
        meme1x.init_database(cfg)

    variants = ", ".join(
        f"{cfg['engine']}={cfg['variant']}" for cfg in meme1x.ENGINES
    )
    print(
        f"MEME1 embedded shadow started interval={meme1x.POLL_SEC:.1f}s "
        f"{variants} | anomaly_guard=ON",
        flush=True,
    )

    while True:
        started = time.time()

        for cfg in meme1x.ENGINES:
            try:
                sources = meme1x.ingest_new_sources(cfg)
                pending, opened, rejected = meme1x.evaluate_pending(cfg)
                open_count, closed = safe_monitor_positions(cfg)

                if sources or pending or opened or rejected or open_count or closed:
                    print(
                        f"{cfg['engine']} embedded "
                        f"sources={sources} pending={pending} opened={opened} "
                        f"rejected={rejected} open_positions={open_count} "
                        f"closed={closed}",
                        flush=True,
                    )

            except Exception as exc:
                print(
                    f"{cfg['engine']} embedded loop error: "
                    f"{type(exc).__name__}: {exc}",
                    flush=True,
                )
                traceback.print_exc()

        elapsed = time.time() - started
        time.sleep(max(1.0, meme1x.POLL_SEC - elapsed))


def main():
    thread = threading.Thread(
        target=meme1x_loop,
        name="meme1-v2-v3-shadow",
        daemon=True,
    )
    thread.start()

    meme1.monitor_loop()


if __name__ == "__main__":
    main()
