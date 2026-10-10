"""V4.3 R3 SHADOW diagnostics wrapper around the unchanged R2 radar.

Does not run by being uploaded to GitHub. Run explicitly ONLY in a safe,
separate simulation environment, with LIVE_TRADING=false.

The R2 scan and opening logic remain unchanged. This wrapper observes
qualified signals; it does not modify, block, or create R3 orders.
"""

import os


def attach_shadow_diagnostics(scanner_module, evaluate):
    """Attach read-only logging to an imported R2 scanner module.

    This function does not execute the radar or import the trading engine.
    """
    if scanner_module.ENGINE != "V4.3R2":
        raise RuntimeError("Expected the unchanged V4.3R2 scanner")
    if getattr(scanner_module, "_R3_SHADOW_ATTACHED", False):
        raise RuntimeError("R3 shadow wrapper already attached")

    original = scanner_module.deep_analyze

    def observed_deep_analyze(radar):
        result = original(radar)
        if result is not None:
            try:
                # Only provide a shallow copy to the read-only evaluator.
                # The original signal and R2 trade flow stay untouched.
                report = evaluate(dict(result))
                print(
                    "R3 SHADOW CANDIDATE:",
                    report.get("symbol", ""),
                    report.get("assessment", {}),
                    flush=True,
                )
            except Exception as exc:
                print("R3 SHADOW DIAGNOSTIC ERROR:", repr(exc), flush=True)
        return result

    scanner_module.deep_analyze = observed_deep_analyze
    scanner_module._R3_SHADOW_ATTACHED = True


def main():
    live = os.environ.get("LIVE_TRADING", "false").strip().lower()
    if live not in ("false", "0", "off", "no"):
        raise RuntimeError("Simulation only: LIVE_TRADING must be false")

    # Important: this imports the EXISTING R2 engine and its dependencies.
    # Running it executes R2 radar and can create R2 simulated trades.
    import scanner_job as scanner
    from r3_shadow_scanner import evaluate_candidate

    if scanner.trading_app.LIVE_TRADING:
        raise RuntimeError("Refusing to run with LIVE_TRADING=true")
    attach_shadow_diagnostics(scanner, evaluate_candidate)
    scanner.run_fast_radar()


if __name__ == "__main__":
    main()
