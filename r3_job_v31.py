"""
R3.1 timing-fix wrapper for the existing R3 paper-trading engine.

Fixes two timing problems without changing R2/R2F or R3 exit parameters:
1) A live Radar breakout is carried forward briefly so the next completed 5m
   candle can confirm it instead of immediately becoming REJECT_NO_BREAKOUT.
2) The pullback trigger is frozen when the pullback is confirmed. A recent
   completed 1m close crossing that fixed level can trigger entry, so the 5m
   cron does not miss a short re-acceleration between scans.

Engine version intentionally remains "R3" so the existing dashboard keeps
working. New rows are tagged features.variant = "R3.1".
PAPER ONLY.
"""

import json
import os
from datetime import datetime, timezone

import r3_job as core


_orig_init_r3_database = core.init_r3_database
_orig_analyze_setup = core.analyze_setup

R31_PENDING_TTL_MIN = int(os.environ.get("R31_PENDING_TTL_MIN", "15"))
R31_TRIGGER_LOOKBACK_MIN = int(os.environ.get("R31_TRIGGER_LOOKBACK_MIN", "6"))
R31_TRIGGER_RETRACE_TOL_PCT = float(os.environ.get("R31_TRIGGER_RETRACE_TOL_PCT", "0.08"))


def _utc_iso():
    return datetime.now(timezone.utc).isoformat()


def _feature_dict(raw):
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
            return value if isinstance(value, dict) else {}
        except Exception:
            return {}
    return {}


def _tag_variant(x):
    features = _feature_dict(x.get("features"))
    features["variant"] = "R3.1"
    # Preserve values create_trade later expects inside features.
    for k in (
        "rsi_5m",
        "volume_ratio_5m",
        "directional_momentum_5m",
        "extension_5m",
        "trend_15m",
        "trend_1h",
    ):
        if k in x and x.get(k) is not None:
            features[k] = x.get(k)
    x["features"] = features
    return x


def init_r31_database():
    _orig_init_r3_database()
    # No schema change is required. BREAKOUT_PENDING reuses r3_watchlist.
    conn = core.db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_r3_watch_stage_status
            ON r3_watchlist(stage,status,expires_at);
            """
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def _closed_5m_breakout(symbol, side, reference):
    candles = core.get_klines_retry(symbol, "5m", 5)[:-1]
    if not candles:
        return False, None
    close = float(candles[-1]["close"])
    if side == "LONG":
        return close > float(reference), close
    return close < float(reference), close


def _recheck_after_stored_breakout(x, reference):
    """Re-evaluate setup gates while using the stored Radar-time reference."""
    x = _tag_variant(x)
    x["reference_level"] = float(reference)
    f = x["features"]
    f["breakout"] = True
    f["stored_breakout_reference"] = float(reference)
    f["breakout_confirmed_after_radar"] = True

    align5 = bool(f.get("align_5m"))
    align15 = bool(f.get("align_15m"))
    align1h = bool(f.get("align_1h"))
    market_align = int(x.get("market_alignment") or 0)
    vr5 = float(x.get("volume_ratio_5m") or f.get("volume_ratio_5m") or 0)
    dmom5 = float(x.get("directional_momentum_5m") or f.get("directional_momentum_5m") or 0)
    ext5 = float(x.get("extension_5m") or f.get("extension_5m") or 0)
    rsi5 = float(x.get("rsi_5m") or f.get("rsi_5m") or 0)

    passed = 1
    passed += int(align5)
    passed += int(align15)
    passed += int(align1h)
    passed += int(market_align >= core.R3_MIN_MARKET_ALIGNMENT)
    passed += int(vr5 >= core.R3_MIN_5M_VOLUME_RATIO)
    passed += int(dmom5 >= core.R3_MIN_5M_DIRECTIONAL_MOM)
    passed += int(ext5 <= core.R3_MAX_5M_EXTENSION)
    x["setup_strength"] = passed / 8.0 * 100.0

    decision = "WATCH"
    reason = "stored Radar breakout confirmed by completed 5m close; wait for pullback"
    if not align5 or not align15:
        decision, reason = "REJECT_STRUCTURE", "5m/15m structure not aligned after breakout confirmation"
    elif market_align < core.R3_MIN_MARKET_ALIGNMENT:
        decision, reason = "REJECT_MARKET", f"BTC/ETH alignment {market_align} < {core.R3_MIN_MARKET_ALIGNMENT}"
    elif vr5 < core.R3_MIN_5M_VOLUME_RATIO:
        decision, reason = "REJECT_VOLUME", f"5m volume ratio {vr5:.2f} < {core.R3_MIN_5M_VOLUME_RATIO:.2f}"
    elif dmom5 < core.R3_MIN_5M_DIRECTIONAL_MOM:
        decision, reason = "REJECT_MOMENTUM", f"5m directional momentum {dmom5:.2f}% too weak"
    elif ext5 > core.R3_MAX_5M_EXTENSION:
        decision, reason = "REJECT_EXTENSION", f"5m extension {ext5:.2f}% > {core.R3_MAX_5M_EXTENSION:.2f}%"
    elif x["side"] == "LONG" and rsi5 > core.R3_LONG_MAX_RSI5:
        decision, reason = "REJECT_RSI", f"LONG RSI5 {rsi5:.1f} > {core.R3_LONG_MAX_RSI5:.1f}"
    elif x["side"] == "SHORT" and rsi5 < core.R3_SHORT_MIN_RSI5:
        decision, reason = "REJECT_RSI", f"SHORT RSI5 {rsi5:.1f} < {core.R3_SHORT_MIN_RSI5:.1f}"

    x["decision"] = decision
    x["reason"] = reason
    return x


def analyze_setup_v31(radar, ctx):
    x = _orig_analyze_setup(radar, ctx)
    if not x:
        return None
    x = _tag_variant(x)

    if x["decision"] == "REJECT_NO_BREAKOUT":
        reference = float(x["reference_level"])
        live = float(radar.get("last") or x.get("setup_price") or 0)
        live_crossed = live > reference if x["side"] == "LONG" else live < reference
        x["features"]["radar_live_price"] = live
        x["features"]["radar_live_crossed_reference"] = bool(live_crossed)
        if live_crossed:
            x["decision"] = "PENDING_BREAKOUT"
            x["reason"] = (
                "live Radar price crossed prior 20-bar structure; "
                "wait for completed 5m close confirmation"
            )
    return x


def add_pending(x):
    conn = core.db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO r3_watchlist (
                symbol,side,stage,status,expires_at,
                setup_price,setup_extreme,reference_level,
                radar_move_pct,regime,setup_strength,market_alignment,features
            ) VALUES (
                %s,%s,'BREAKOUT_PENDING','ACTIVE',NOW() + (%s * INTERVAL '1 minute'),
                %s,%s,%s,%s,%s,%s,%s,%s
            ) RETURNING id;
            """,
            (
                x["symbol"], x["side"], R31_PENDING_TTL_MIN,
                x["setup_price"], x["setup_price"], x["reference_level"],
                x["radar_move_pct"], x["regime"], x["setup_strength"],
                x["market_alignment"], json.dumps(x["features"], default=str),
            ),
        )
        wid = int(cur.fetchone()[0])
        conn.commit()
        return wid
    finally:
        cur.close()
        conn.close()


def promote_pending_to_watch(wid, x, reference):
    features = _feature_dict(x.get("features"))
    features["variant"] = "R3.1"
    q = core.fresh_ticker(x["symbol"])
    setup_price = float(q.get("last") or x.get("setup_price"))

    conn = core.db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE r3_watchlist SET
                stage='WATCH', status='ACTIVE', updated_at=NOW(),
                expires_at=NOW() + (%s * INTERVAL '1 minute'),
                setup_price=%s, setup_extreme=%s, reference_level=%s,
                pullback_extreme=NULL, trigger_level=NULL,
                regime=%s, setup_strength=%s, market_alignment=%s,
                features=%s, terminal_reason=NULL
            WHERE id=%s;
            """,
            (
                core.R3_SETUP_TTL_MIN,
                setup_price, setup_price, float(reference),
                x.get("regime"), x.get("setup_strength"), x.get("market_alignment"),
                json.dumps(features, ensure_ascii=False, default=str), wid,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()

    x["setup_price"] = setup_price
    x["setup_extreme"] = setup_price
    x["reference_level"] = float(reference)
    x["features"] = features
    return x


def _set_diag(wid, x, block, **extra):
    features = _feature_dict(x.get("features"))
    diag = features.get("r31_diag")
    if not isinstance(diag, dict):
        diag = {}
    diag.update({"last_block": block, "last_block_at": _utc_iso(), **extra})
    features["r31_diag"] = diag
    features["variant"] = "R3.1"
    x["features"] = features
    core.update_watch(wid, features=features)


def recent_trigger_event(symbol, side, trigger_level):
    """Find a qualifying completed 1m trigger close inside the recent scan gap."""
    candles = core.get_klines_retry(symbol, "1m", 60)[:-1]
    if len(candles) < 25:
        return None

    closes = [float(x["close"]) for x in candles]
    vols = [float(x["volume"]) for x in candles]
    start = max(20, len(candles) - max(R31_TRIGGER_LOOKBACK_MIN, 1))
    latest_cross = None

    for i in range(start, len(candles)):
        close_i = closes[i]
        crossed = close_i > trigger_level if side == "LONG" else close_i < trigger_level
        if not crossed:
            continue

        j = max(0, i - 3)
        mom = core.directional(side, core.pct(close_i, closes[j]))
        prev = vols[max(0, i - 20):i]
        avg = sum(prev) / len(prev) if prev else 0.0
        vr = vols[i] / avg if avg > 0 else 0.0
        latest_cross = {
            "level": float(trigger_level),
            "close": close_i,
            "mom": float(mom),
            "vr": float(vr),
            "bars_ago": len(candles) - 1 - i,
        }

    return latest_cross


def _process_pending(w, current_map, ctx, now):
    wid = w["id"]
    side = w["side"]
    symbol = w["symbol"]
    reference = float(w["reference_level"])
    x = core._watch_as_signal(w)
    x = _tag_variant(x)
    t = current_map.get(symbol)
    if not t:
        return

    if now >= w["expires_at"]:
        reason = f"5m breakout confirmation not received within {R31_PENDING_TTL_MIN}m"
        core.update_watch(wid, status="EXPIRED", terminal_reason=reason)
        q = core.fresh_ticker(symbol)
        entry_ref = q["ask"] if side == "LONG" else q["bid"]
        entry_ref = entry_ref or q["last"] or t["last"]
        did = core.log_decision(
            x, "PENDING_EXPIRED", reason, stage="BREAKOUT_PENDING", watchlist_id=wid,
            market_entry_price=entry_ref, spread_pct=q.get("spread_pct"),
        )
        core.shadow_decision(did, x, "PENDING_EXPIRED", reason, entry_ref)
        return

    confirmed, closed_price = _closed_5m_breakout(symbol, side, reference)
    if not confirmed:
        _set_diag(wid, x, "WAIT_5M_BREAKOUT_CLOSE", closed_5m=closed_price, reference=reference)
        return

    radar = {
        "symbol": symbol,
        "side": side,
        "radar_move_pct": float(w.get("radar_move_pct") or 0),
        "last": float(t["last"]),
    }
    checked = _orig_analyze_setup(radar, ctx)
    if not checked:
        _set_diag(wid, x, "SETUP_DATA_UNAVAILABLE_AFTER_CONFIRM")
        return
    checked = _recheck_after_stored_breakout(checked, reference)

    if checked["decision"] == "WATCH":
        checked = promote_pending_to_watch(wid, checked, reference)
        core.log_decision(
            checked, "WATCH", checked["reason"], stage="WATCH", watchlist_id=wid,
        )
        print(
            f"R3.1 BREAKOUT CONFIRMED #{wid} {symbol} {side} "
            f"close={closed_price} ref={reference}",
            flush=True,
        )
        return

    reason = checked["reason"]
    core.update_watch(wid, status="REJECTED", terminal_reason=reason)
    q = core.fresh_ticker(symbol)
    entry_ref = q["ask"] if side == "LONG" else q["bid"]
    entry_ref = entry_ref or q["last"] or t["last"]
    did = core.log_decision(
        checked, checked["decision"], reason, stage="BREAKOUT_CONFIRMED", watchlist_id=wid,
        market_entry_price=entry_ref, spread_pct=q.get("spread_pct"),
    )
    core.shadow_decision(did, checked, checked["decision"], reason, entry_ref)


def process_watches_v31(current_map, ctx):
    created = 0
    now = datetime.now(timezone.utc)
    watches = core.load_active_watches()

    for w in watches:
        if w["stage"] == "BREAKOUT_PENDING":
            try:
                _process_pending(w, current_map, ctx, now)
            except Exception as exc:
                print("R3.1 PENDING ERROR:", w.get("symbol"), str(exc), flush=True)
            continue

        x = _tag_variant(core._watch_as_signal(w))
        wid = w["id"]
        t = current_map.get(w["symbol"])
        if not t:
            continue
        price = float(t["last"])
        side = w["side"]
        reference = float(w["reference_level"])
        extreme = float(w["setup_extreme"])

        if side == "LONG":
            extreme = max(extreme, price)
            pullback_pct = (extreme - price) / extreme * 100.0
            reference_broken = price < reference * (1.0 - core.R3_REFERENCE_INVALIDATION_PCT / 100.0)
        else:
            extreme = min(extreme, price)
            pullback_pct = (price - extreme) / extreme * 100.0
            reference_broken = price > reference * (1.0 + core.R3_REFERENCE_INVALIDATION_PCT / 100.0)

        x["setup_extreme"] = extreme
        core.update_watch(wid, setup_extreme=extreme)

        if now >= w["expires_at"]:
            reason = f"setup expired after {core.R3_SETUP_TTL_MIN}m"
            core.update_watch(wid, status="EXPIRED", terminal_reason=reason)
            q = core.fresh_ticker(w["symbol"])
            entry_ref = q["ask"] if side == "LONG" else q["bid"]
            entry_ref = entry_ref or q["last"] or price
            did = core.log_decision(
                x, "EXPIRED", reason, stage=w["stage"], watchlist_id=wid,
                pullback_pct=pullback_pct, pullback_extreme=w.get("pullback_extreme"),
                trigger_level=w.get("trigger_level"), market_entry_price=entry_ref,
                spread_pct=q.get("spread_pct"),
            )
            core.shadow_decision(did, x, "EXPIRED", reason, entry_ref, {"pullback_pct": pullback_pct})
            continue

        if reference_broken or pullback_pct > core.R3_PULLBACK_MAX_PCT:
            reason = (
                "breakout reference invalidated" if reference_broken
                else f"pullback {pullback_pct:.2f}% > {core.R3_PULLBACK_MAX_PCT:.2f}%"
            )
            core.update_watch(wid, status="INVALIDATED", terminal_reason=reason)
            q = core.fresh_ticker(w["symbol"])
            entry_ref = q["ask"] if side == "LONG" else q["bid"]
            entry_ref = entry_ref or q["last"] or price
            did = core.log_decision(
                x, "INVALIDATED", reason, stage=w["stage"], watchlist_id=wid,
                pullback_pct=pullback_pct, pullback_extreme=w.get("pullback_extreme"),
                trigger_level=w.get("trigger_level"), market_entry_price=entry_ref,
                spread_pct=q.get("spread_pct"),
            )
            core.shadow_decision(did, x, "INVALIDATED", reason, entry_ref, {"pullback_pct": pullback_pct})
            continue

        if w["stage"] == "WATCH":
            if pullback_pct < core.R3_PULLBACK_MIN_PCT:
                _set_diag(wid, x, "WAIT_PULLBACK", pullback_pct=round(pullback_pct, 4))
                continue
            trig = core.local_trigger(w["symbol"], side)
            if not trig:
                _set_diag(wid, x, "PULLBACK_TRIGGER_DATA_UNAVAILABLE")
                continue

            # R3.1 freezes this level. It will NOT be moved farther away later.
            features = _feature_dict(x["features"])
            features["variant"] = "R3.1"
            features["fixed_trigger_level"] = float(trig["level"])
            features["pullback_confirmed_at"] = _utc_iso()
            x["features"] = features
            core.update_watch(
                wid,
                stage="PULLBACK",
                pullback_extreme=price,
                trigger_level=trig["level"],
                features=features,
            )
            core.log_decision(
                x, "PULLBACK_CONFIRMED", "valid retest depth; fixed trigger armed",
                stage="PULLBACK", watchlist_id=wid, pullback_pct=pullback_pct,
                pullback_extreme=price, trigger_level=trig["level"],
            )
            print(
                f"R3.1 PULLBACK {w['symbol']} {side} depth={pullback_pct:.2f}% "
                f"fixed_trigger={trig['level']}",
                flush=True,
            )
            continue

        if w["stage"] != "PULLBACK":
            continue

        pullback_extreme = float(w["pullback_extreme"] or price)
        if side == "LONG":
            pullback_extreme = min(pullback_extreme, price)
        else:
            pullback_extreme = max(pullback_extreme, price)
        core.update_watch(wid, pullback_extreme=pullback_extreme)

        trigger_level = float(w["trigger_level"] or 0)
        if trigger_level <= 0:
            trig0 = core.local_trigger(w["symbol"], side)
            if not trig0:
                _set_diag(wid, x, "TRIGGER_LEVEL_MISSING")
                continue
            trigger_level = float(trig0["level"])
            core.update_watch(wid, trigger_level=trigger_level)

        event = recent_trigger_event(w["symbol"], side, trigger_level)
        if not event:
            _set_diag(wid, x, "WAIT_FIXED_TRIGGER_CROSS", trigger_level=trigger_level)
            continue
        if event["mom"] < core.R3_TRIGGER_MIN_1M_MOM:
            _set_diag(
                wid, x, "TRIGGER_MOMENTUM_FAIL", trigger_level=trigger_level,
                trigger_mom=round(event["mom"], 5), trigger_vr=round(event["vr"], 4),
            )
            continue
        if event["vr"] < core.R3_TRIGGER_MIN_1M_VOLUME_RATIO:
            _set_diag(
                wid, x, "TRIGGER_VOLUME_FAIL", trigger_level=trigger_level,
                trigger_mom=round(event["mom"], 5), trigger_vr=round(event["vr"], 4),
            )
            continue

        market_align = core.market_alignment_for(side, ctx)
        if market_align < core.R3_MIN_MARKET_ALIGNMENT:
            _set_diag(wid, x, "TRIGGER_MARKET_FAIL", market_alignment=market_align)
            continue

        # A recent crossed 1m close is remembered across the 5m cron gap, but
        # do not enter after the market has clearly failed back through it.
        if side == "LONG":
            supportive = price >= trigger_level * (1.0 - R31_TRIGGER_RETRACE_TOL_PCT / 100.0)
        else:
            supportive = price <= trigger_level * (1.0 + R31_TRIGGER_RETRACE_TOL_PCT / 100.0)
        if not supportive:
            _set_diag(
                wid, x, "RETRACED_AFTER_TRIGGER", trigger_level=trigger_level,
                live_price=price, tolerance_pct=R31_TRIGGER_RETRACE_TOL_PCT,
            )
            continue

        q = core.fresh_ticker(w["symbol"])
        entry = q["ask"] if side == "LONG" else q["bid"]
        entry = entry or q["last"]
        spread = q.get("spread_pct")
        if not entry or entry <= 0:
            _set_diag(wid, x, "ENTRY_PRICE_UNAVAILABLE")
            continue

        if side == "LONG":
            chase = core.pct(entry, trigger_level)
        else:
            chase = core.pct(trigger_level, entry)
        adverse_chase = max(chase, 0.0)

        trigger_score = 0.0
        trigger_score += 35 if event["mom"] >= core.R3_TRIGGER_MIN_1M_MOM else 0
        trigger_score += 25 if event["mom"] >= 0.15 else 0
        trigger_score += 20 if event["vr"] >= 1.20 else 0
        trigger_score += 20 if spread is not None and spread <= 0.10 else 0

        meta = {
            "pullback_pct": pullback_pct,
            "trigger_mom": event["mom"],
            "trigger_vr": event["vr"],
            "trigger_score": trigger_score,
            "spread_pct": spread,
            "chase_pct": chase,
            "trigger_bars_ago": event["bars_ago"],
            "variant": "R3.1",
        }

        if spread is None or spread > core.R3_ENTER_MAX_SPREAD:
            _set_diag(wid, x, "SPREAD_FAIL", spread_pct=spread, trigger_level=trigger_level)
            continue
        if adverse_chase > core.R3_ENTER_MAX_TRIGGER_CHASE:
            _set_diag(
                wid, x, "CHASE_FAIL", chase_pct=round(adverse_chase, 5),
                max_chase_pct=core.R3_ENTER_MAX_TRIGGER_CHASE,
            )
            continue

        allow, risk_reason = core.risk_allows_new_trade()
        if not allow:
            did = core.log_decision(
                x, "RISK_BLOCKED", risk_reason, stage="TRIGGER", watchlist_id=wid,
                pullback_pct=pullback_pct, pullback_extreme=pullback_extreme,
                trigger_level=trigger_level, market_entry_price=entry,
                spread_pct=spread, chase_pct=chase,
            )
            core.shadow_decision(did, x, "RISK_BLOCKED", risk_reason, entry, meta)
            continue
        if created >= core.R3_MAX_NEW_PER_SCAN or core.open_count() >= core.R3_MAX_OPEN:
            reason = "R3 position limit"
            did = core.log_decision(
                x, "NO_SLOT", reason, stage="TRIGGER", watchlist_id=wid,
                pullback_pct=pullback_pct, pullback_extreme=pullback_extreme,
                trigger_level=trigger_level, market_entry_price=entry,
                spread_pct=spread, chase_pct=chase,
            )
            core.shadow_decision(did, x, "NO_SLOT", reason, entry, meta)
            continue
        if not core.can_open(w["symbol"]):
            reason = "symbol cooldown/open"
            did = core.log_decision(
                x, "COOLDOWN", reason, stage="TRIGGER", watchlist_id=wid,
                pullback_pct=pullback_pct, pullback_extreme=pullback_extreme,
                trigger_level=trigger_level, market_entry_price=entry,
                spread_pct=spread, chase_pct=chase,
            )
            core.shadow_decision(did, x, "COOLDOWN", reason, entry, meta)
            continue

        x = _tag_variant(x)
        trade_id = core.create_trade(x, entry, meta)
        core.update_watch(
            wid, status="ENTERED", scanner_trade_id=trade_id,
            terminal_reason="R3.1 fixed trigger passed",
        )
        core.log_decision(
            x, "ENTER", "R3.1 pullback + fixed re-acceleration trigger passed",
            stage="TRIGGER", watchlist_id=wid, scanner_trade_id=trade_id,
            pullback_pct=pullback_pct, pullback_extreme=pullback_extreme,
            trigger_level=trigger_level, market_entry_price=entry,
            spread_pct=spread, chase_pct=chase,
        )
        created += 1
        print(
            f"R3.1 PAPER ENTRY #{trade_id} {w['symbol']} {side} entry={entry} "
            f"pullback={pullback_pct:.2f}% trigger_mom={event['mom']:.2f}% "
            f"bars_ago={event['bars_ago']}",
            flush=True,
        )

    return created


def process_candidates_v31(candidates, ctx):
    added = 0
    for radar in candidates[:core.R3_MAX_DEEP_ANALYSIS]:
        try:
            if core.has_active_watch(radar["symbol"], radar["side"]):
                continue
            x = analyze_setup_v31(radar, ctx)
            if not x:
                continue

            print(
                f"R3.1 SETUP {x['symbol']} {x['side']} move={x['radar_move_pct']:+.2f}% "
                f"strength={x['setup_strength']:.0f} market={x['market_alignment']} "
                f"| {x['decision']}",
                flush=True,
            )

            if x["decision"] == "PENDING_BREAKOUT":
                wid = add_pending(x)
                core.log_decision(
                    x, "PENDING_BREAKOUT", x["reason"],
                    stage="BREAKOUT_PENDING", watchlist_id=wid,
                )
                added += 1
                print(
                    f"R3.1 PENDING #{wid} {x['symbol']} {x['side']} "
                    f"live={x['setup_price']} ref={x['reference_level']}",
                    flush=True,
                )
                continue

            if x["decision"] != "WATCH":
                did = core.log_decision(
                    x, x["decision"], x["reason"], stage="SETUP",
                    market_entry_price=x["entry_ref"], spread_pct=x.get("spread_pct"),
                )
                core.shadow_decision(did, x, x["decision"], x["reason"], x["entry_ref"])
                continue

            wid = core.add_watch(x)
            core.log_decision(x, "WATCH", x["reason"], stage="WATCH", watchlist_id=wid)
            added += 1
            print(
                f"R3.1 WATCH #{wid} {x['symbol']} {x['side']} "
                f"setup={x['setup_price']} ref={x['reference_level']}",
                flush=True,
            )
        except Exception as exc:
            print("R3.1 SETUP ERROR:", radar.get("symbol"), str(exc), flush=True)
    return added


def install_patch():
    core.init_r3_database = init_r31_database
    core.process_watches = process_watches_v31
    core.process_candidates = process_candidates_v31
    # Useful if another module calls analyze_setup directly after importing us.
    core.analyze_setup = analyze_setup_v31


if __name__ == "__main__":
    install_patch()
    print(
        "R3.1 PATCH ACTIVE | PAPER ONLY | pending 5m confirmation + fixed trigger + recent 1m cross",
        flush=True,
    )
    core.run()
