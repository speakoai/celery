"""
Persistence for call-quality findings.

Phase 3 of docs/plans/call-quality-issues.md (speako-workspace).

Separated from tasks/utils/call_quality_rules.py — that module is pure and
unit-tested; this one owns the SQL. Both are kept out of
tasks/pull_render_logs.py so the sweep body stays about sweeping.

The catalogue is loaded once per process. It is a small file read from disk, but
the sweep runs in a short-lived cron container and would otherwise re-read and
re-index it for every call in the window.
"""

import json
import os

from tasks.utils.call_quality_rules import index_catalogue

_CATALOGUE_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "call_issue_catalogue.json",
)

_catalogue = None


def load_catalogue():
    """Indexed catalogue, cached per process. Returns None if unreadable —
    analysis is then skipped rather than crashing the sweep."""
    global _catalogue
    if _catalogue is None:
        with open(_CATALOGUE_PATH) as handle:
            _catalogue = index_catalogue(json.load(handle))
    return _catalogue


def insert_findings(conn, *, tenant_id, location_id, conversation_id, call_sid,
                    findings):
    """Write findings for one call. Returns the number of NEW rows.

    ``ON CONFLICT DO NOTHING`` against UNIQUE (location_conversation_id,
    rule_id) makes re-analysis idempotent AND preserves triage state: a finding
    the admin already marked `solved` must not be resurrected to `open` because
    an overlapping sweep window re-read the same call.

    A recurrence of the same problem on a DIFFERENT call inserts a new row with
    status='open', which is how a solved issue resurfaces — see the plan's D0.
    """
    if not findings:
        return 0

    rows = [
        (
            tenant_id, location_id, conversation_id, call_sid,
            f["rule_id"], f["severity"], f["detector"], f["confidence"],
            f["title"], f["explanation"], f["suggested_fix"],
            json.dumps(f["evidence"]),
            f["analyzer_version"], f["catalogue_version"],
        )
        for f in findings
    ]

    inserted = 0
    with conn.cursor() as cur:
        for row in rows:
            cur.execute(
                """
                INSERT INTO call_quality_findings (
                    tenant_id, location_id, location_conversation_id, call_sid,
                    rule_id, severity, detector, confidence,
                    title, explanation, suggested_fix,
                    evidence, analyzer_version, catalogue_version
                ) VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s::jsonb,%s,%s)
                ON CONFLICT (location_conversation_id, rule_id) DO NOTHING
                """,
                row,
            )
            inserted += cur.rowcount
    return inserted
