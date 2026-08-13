#!/usr/bin/env python3
"""
Entry point for the audio verification sweep (Phase 8).

Checks findings that say STT produced nothing against the call recording, and
records whether the caller was actually speaking at that moment.

Usage:
    python dispatch/verify_call_findings_dispatch.py --dev --sync
    python dispatch/verify_call_findings_dispatch.py --sync            # prod

Runs `--sync` in its own cron container for the same reason the log puller does:
the celery worker is a single memory-constrained process shared with
availability regeneration, and decoding audio has no business occupying one of
its slots.

Exits non-zero only on a CONFIGURATION failure. Individual abstentions (mono
audio, a recording not yet uploaded, an ambiguous window) are normal outcomes
and must not turn the cron red — this is a diagnostic, and a red cron should
mean the diagnostic is broken, not that a call was inconclusive.
"""

import argparse
import json
import sys
from datetime import datetime

from tasks.verify_call_findings import verify_call_findings


def main():
    ap = argparse.ArgumentParser(description="Verify STT findings against call audio")
    ap.add_argument("--dev", action="store_true", help="run against the dev database")
    ap.add_argument("--sync", action="store_true", help="run inline instead of enqueueing")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    print("=" * 80)
    print("[DISPATCH] Call-audio verification")
    print(f"[DISPATCH] env={'dev' if args.dev else 'prod'} at {datetime.now().isoformat()}")
    print("=" * 80)

    kwargs = {"is_dev": args.dev, "limit": args.limit}
    if not args.sync:
        print(f"[DISPATCHED] Task ID = {verify_call_findings.apply_async(kwargs=kwargs).id}")
        return

    result = verify_call_findings(**kwargs)
    print(json.dumps(result, indent=2))
    if not result.get("success"):
        print(f"[ERROR] verification did not run: {result.get('error')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
