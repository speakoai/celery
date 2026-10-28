"""
Availability cache golden check — restaurant booking maturity S4 (reused by S6–S10).

Proves a change to the availability generator is (or is not) behaviour-neutral:
rebuild fixed dates, snapshot what the cache holds for them, deploy, rebuild
again, snapshot, diff. The cache carries no timestamps, so identical code and
data give byte-identical snapshots.

Usage (DEV only — the script refuses any Redis that isn't speako-cache-dev):

  # env: REDIS_URL (speako-cache-dev), CELERY_API_URL + CELERY_API_KEY (app-dev)
  python tests/availability_golden.py rebuild  --loc 2:34 --loc 2:35 --tz Australia/Sydney --date 2026-11-24 --date 2026-11-28
  python tests/availability_golden.py snapshot before.json --loc 2:34 --loc 2:35 --date 2026-11-24 --date 2026-11-28
  python tests/availability_golden.py diff before.json after.json

Only bookings on the chosen dates must not change between the two snapshots.
"""

import argparse
import datetime as dt
import json
import os
import sys
import time
import urllib.request

DEV_CACHE_ID = "red-d5ngvi14tr6s73d7dci0"  # speako-cache-dev


def _redis():
    import redis  # noqa: WPS433 — only needed for snapshot

    url = os.environ.get("REDIS_URL", "")
    if DEV_CACHE_ID not in url:
        sys.exit("Refusing: REDIS_URL is not speako-cache-dev")
    return redis.Redis.from_url(url, decode_responses=True)


def _parse_locs(values):
    out = []
    for v in values:
        tenant, loc = v.split(":")
        out.append((int(tenant), int(loc)))
    return out


def rebuild(args):
    base = os.environ["CELERY_API_URL"].rstrip("/")
    key = os.environ["CELERY_API_KEY"]
    for tenant, loc in _parse_locs(args.loc):
        for day in args.date:
            body = json.dumps({
                "tenant_id": str(tenant),
                "location_id": str(loc),
                "location_tz": args.tz,
                "affected_date": day,
                "business_type": "rest",
            }).encode()
            req = urllib.request.Request(
                f"{base}/api/availability/generate",
                data=body,
                headers={"Content-Type": "application/json", "x-api-key": key},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=30) as res:
                print(f"rebuild {tenant}:{loc} {day} -> {res.status}")
    print(f"waiting {args.wait}s for the worker…")
    time.sleep(args.wait)


def _day_entries(r, tenant, loc, day):
    """The cache entry holding `day` (chunks hold 3 consecutive days)."""
    target = dt.date.fromisoformat(day)
    for back in range(0, 3):
        start = (target - dt.timedelta(days=back)).isoformat()
        raw = r.get(f"availability:tenant_{tenant}:location_{loc}:start_date_{start}")
        if not raw:
            continue
        data = json.loads(raw)
        for entry in data.get("availabilities", []):
            if entry.get("date") == day:
                return {
                    "chunk": start,
                    "services": data.get("services"),
                    "location_zone_tags": data.get("location_zone_tags"),
                    "day": entry,
                }
    return None


def snapshot(args):
    r = _redis()
    out = {}
    for tenant, loc in _parse_locs(args.loc):
        for day in args.date:
            out[f"{tenant}:{loc}:{day}"] = _day_entries(r, tenant, loc, day)
    with open(args.out, "w") as f:
        json.dump(out, f, indent=1, sort_keys=True)
    missing = [k for k, v in out.items() if v is None]
    print(f"wrote {args.out}: {len(out)} location-days" + (f", MISSING {missing}" if missing else ""))


def diff(args):
    with open(args.a) as f:
        a = json.load(f)
    with open(args.b) as f:
        b = json.load(f)
    if json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True):
        print("IDENTICAL")
        return 0
    for k in sorted(set(a) | set(b)):
        if json.dumps(a.get(k), sort_keys=True) != json.dumps(b.get(k), sort_keys=True):
            print(f"DIFFERS: {k}")
    return 1


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    rb = sub.add_parser("rebuild")
    rb.add_argument("--loc", action="append", required=True)
    rb.add_argument("--date", action="append", required=True)
    rb.add_argument("--tz", default="Australia/Sydney")
    rb.add_argument("--wait", type=int, default=30)
    sn = sub.add_parser("snapshot")
    sn.add_argument("out")
    sn.add_argument("--loc", action="append", required=True)
    sn.add_argument("--date", action="append", required=True)
    df = sub.add_parser("diff")
    df.add_argument("a")
    df.add_argument("b")
    args = p.parse_args()
    if args.cmd == "rebuild":
        rebuild(args)
    elif args.cmd == "snapshot":
        snapshot(args)
    else:
        sys.exit(diff(args))


if __name__ == "__main__":
    main()
