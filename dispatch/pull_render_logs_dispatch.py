"""
Dispatch script for the per-call server-log puller.

Enqueues one sweep of the Azure voice-ai service's Render logs, splitting the
~ct~ audio-timeline lines by call and attaching an artifact to each
conversation. See docs/plans/call-server-log-capture.md in speako-workspace.

Intended cadence: every 5 minutes. The sweep is idempotent, so a missed run
costs nothing beyond a wider window on the next one — up to the MAX_WINDOW
clamp in the task, after which unswept logs stay in Render until retention
expires (>=14 days on this workspace).

Usage:
    python dispatch/pull_render_logs_dispatch.py            # prod service
    python dispatch/pull_render_logs_dispatch.py --dev      # dev service
    python dispatch/pull_render_logs_dispatch.py --sync     # run inline, no worker
"""

import argparse
import json
import sys
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

from tasks.pull_render_logs import pull_render_logs


def main():
    parser = argparse.ArgumentParser(description="Sweep Render logs into per-call artifacts")
    parser.add_argument("--dev", action="store_true",
                        help="read the dev azure service and write the dev R2 bucket")
    parser.add_argument("--window-minutes", type=int, default=None,
                        help="override the sweep width (ignored when a cursor exists)")
    parser.add_argument("--sync", action="store_true",
                        help="run inline instead of enqueueing (for manual verification)")
    args = parser.parse_args()

    print("=" * 80)
    print("[DISPATCH] Render server-log sweep")
    print(f"[DISPATCH] env={'dev' if args.dev else 'prod'} at {datetime.now().isoformat()}")
    print("=" * 80)

    kwargs = {"is_dev": args.dev, "window_minutes": args.window_minutes}

    if args.sync:
        result = pull_render_logs(**kwargs)
        print(json.dumps(result, indent=2))
        # Exit non-zero on a configuration failure so Render marks the cron run
        # RED. Without this a missing RENDER_API_KEY would return cleanly and
        # every run would look green while nothing was ever captured — the
        # exact silent failure that would waste a multi-day soak.
        if not result.get("success"):
            print(f"[ERROR] sweep did not run: {result.get('error')}")
            sys.exit(1)
        # A sweep that found work and completed none of it is a broken sweep,
        # even though the task itself "succeeded". The classic cause is a
        # misconfigured R2 bucket variable: parsing works, every upload throws,
        # and the run would otherwise exit 0 and show green while capturing
        # nothing. Isolated failures (one bad call among many) still pass.
        if result.get("failed") and not result.get("uploaded"):
            print(f"[ERROR] every artifact failed to store "
                  f"({result['failed']} call(s), 0 uploaded) — check R2 config")
            sys.exit(1)
    else:
        async_result = pull_render_logs.apply_async(kwargs=kwargs)
        print(f"[DISPATCHED] Task ID = {async_result.id}")


if __name__ == "__main__":
    main()
