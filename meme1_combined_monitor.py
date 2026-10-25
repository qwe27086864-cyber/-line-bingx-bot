"""Run MEME1's existing monitor and MEME1X exitability shadow together.

Why this wrapper exists:
- bingx-meme1-monitor is already an always-on Render web service.
- MEME1X was on a free web service and could sleep when there was no inbound traffic.
- This wrapper keeps MEME1X running every MEME1X_POLL_SEC seconds without changing
  MEME1's trading logic and without starting a second health server/port.

PAPER / SHADOW ONLY.
"""

import threading
import time
import traceback

import meme1_new_pool_job as meme1
import meme1_exitability_shadow_job as meme1x


def meme1x_loop():
    """Run the MEME1X shadow loop without binding its own HTTP health port."""
    meme1x.init_database()

    print(
        f"MEME1X embedded shadow started interval={meme1x.POLL_SEC:.1f}s "
        f"variant={meme1x.VARIANT}",
        flush=True,
    )

    while True:
        started = time.time()
        try:
            sources = meme1x.ingest_new_sources()
            pending, opened, rejected = meme1x.evaluate_pending_candidates()
            open_count, closed = meme1x.monitor_positions()

            if sources or pending or opened or rejected or open_count or closed:
                print(
                    "MEME1X embedded "
                    f"sources={sources} pending={pending} opened={opened} "
                    f"rejected={rejected} open_positions={open_count} closed={closed}",
                    flush=True,
                )

        except Exception as exc:
            print(
                f"MEME1X embedded loop error: {type(exc).__name__}: {exc}",
                flush=True,
            )
            traceback.print_exc()

        elapsed = time.time() - started
        time.sleep(max(1.0, meme1x.POLL_SEC - elapsed))


def main():
    # MEME1X runs in a daemon thread.
    # The existing MEME1 monitor remains the main process and continues serving
    # Render's HTTP port exactly as before.
    thread = threading.Thread(
        target=meme1x_loop,
        name="meme1x-exitability-shadow",
        daemon=True,
    )
    thread.start()

    # Existing MEME1 behavior is unchanged.
    meme1.monitor_loop()


if __name__ == "__main__":
    main()
