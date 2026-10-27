"""Run MEME1's existing monitor and the V2/V3 shadow engines together.

Why this wrapper exists:
- bingx-meme1-monitor is an always-on Render web service.
- The separate shadow web service may sleep on the free plan.
- This wrapper keeps both MEME1X V2 and MEME1V3 running every MEME1X_POLL_SEC
  without changing MEME1's trading logic and without binding another health port.

PAPER / SHADOW ONLY.
"""

import threading
import time
import traceback

import meme1_new_pool_job as meme1
import meme1_exitability_shadow_job as meme1x


def meme1x_loop():
    """Run both V2 and V3 shadow engines without binding their own HTTP port."""
    for cfg in meme1x.ENGINES:
        meme1x.init_database(cfg)

    variants = ", ".join(
        f"{cfg['engine']}={cfg['variant']}" for cfg in meme1x.ENGINES
    )
    print(
        f"MEME1 embedded shadow started interval={meme1x.POLL_SEC:.1f}s "
        f"{variants}",
        flush=True,
    )

    while True:
        started = time.time()

        for cfg in meme1x.ENGINES:
            try:
                sources = meme1x.ingest_new_sources(cfg)
                pending, opened, rejected = meme1x.evaluate_pending(cfg)
                open_count, closed = meme1x.monitor_positions(cfg)

                if sources or pending or opened or rejected or open_count or closed:
                    print(
                        f"{cfg['engine']} embedded "
                        f"sources={sources} pending={pending} opened={opened} "
                        f"rejected={rejected} open_positions={open_count} closed={closed}",
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
    # V2/V3 shadow processing runs in a daemon thread.
    # The existing MEME1 monitor remains the main process and serves Render's
    # HTTP port exactly as before.
    thread = threading.Thread(
        target=meme1x_loop,
        name="meme1-v2-v3-shadow",
        daemon=True,
    )
    thread.start()

    # Existing MEME1 behavior is unchanged.
    meme1.monitor_loop()


if __name__ == "__main__":
    main()
