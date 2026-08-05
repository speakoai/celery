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
    else:
        async_result = pull_render_logs.apply_async(kwargs=kwargs)
        print(f"[DISPATCHED] Task ID = {async_result.id}")


if __name__ == "__main__":
    main()
