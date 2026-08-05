"""
Redaction helpers for anything that might reach a log.

Render retains application logs for ~14 days and they are readable by anyone
with dashboard access or a `RENDER_API_KEY`, so a connection string printed
once is a credential disclosed for a fortnight.

Truncation is NOT redaction: `postgresql://speako_ai_dev_db_user:` is already
35 characters, so a "first 50 chars" preview still exposes the first ~15
characters of the password.
"""

from urllib.parse import urlsplit, urlunsplit


def redact_url(url: str | None, placeholder: str = "NOT SET") -> str:
    """
    Mask the password in a URL while keeping the parts that make it useful
    for diagnostics — scheme, user, host, port and path.

        postgresql://user:s3cret@host:5432/db  ->  postgresql://user:***@host:5432/db
        redis://red-abc:6379                   ->  redis://red-abc:6379

    Never raises: an unparseable value is reported as redacted rather than
    echoed, because the failure mode of this function must not be "print the
    secret anyway".
    """
    if not url:
        return placeholder
    try:
        parts = urlsplit(url)
        if not parts.netloc or "@" not in parts.netloc:
            return url  # nothing that looks like credentials
        userinfo, _, hostinfo = parts.netloc.rpartition("@")
        user = userinfo.split(":", 1)[0]
        netloc = f"{user}:***@{hostinfo}" if user else f"***@{hostinfo}"
        return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
    except Exception:
        return "<unparseable url, redacted>"
