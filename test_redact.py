"""
Tests for tasks/utils/redact.py.

Security-relevant: this function's failure mode must never be "print the
secret anyway", so the unparseable case is covered explicitly.

Run:  python -m pytest test_redact.py -q
"""

import pytest

from tasks.utils.redact import redact_url

PASSWORD = "bJYzm2wZmS0s3fcJoS9yTQ"


def test_masks_the_password_in_a_postgres_url():
    out = redact_url(f"postgresql://speako_user:{PASSWORD}@dpg-abc123-a/speako_db")
    assert PASSWORD not in out
    assert out == "postgresql://speako_user:***@dpg-abc123-a/speako_db"


def test_keeps_the_parts_that_make_it_useful():
    """Redaction shouldn't destroy the diagnostic value — which host and which
    database is exactly what these log lines exist to answer."""
    out = redact_url(f"postgresql://speako_user:{PASSWORD}@dpg-abc123-a:5432/speako_db")
    assert "speako_user" in out and "dpg-abc123-a" in out
    assert "5432" in out and "speako_db" in out


def test_truncation_would_not_have_been_enough():
    """The old app.py used url[:50]. The prefix before the password is 35
    chars, so that leaked ~15 characters of it."""
    url = f"postgresql://speako_ai_dev_db_user:{PASSWORD}xxxxxxxxxx@dpg-abc/db"
    assert PASSWORD[:10] in url[:50]          # the old behaviour leaked
    assert PASSWORD[:10] not in redact_url(url)  # the new one does not


def test_redis_url_without_credentials_is_left_alone():
    url = "redis://red-d5ngvi14tr6s73d7dci0:6379"
    assert redact_url(url) == url


def test_redis_url_with_password_is_masked():
    out = redact_url(f"redis://:{PASSWORD}@red-abc:6379")
    assert PASSWORD not in out
    assert "red-abc:6379" in out


@pytest.mark.parametrize("value,expected", [
    (None, "NOT SET"),
    ("", "NOT SET"),
])
def test_missing_values_report_a_placeholder(value, expected):
    assert redact_url(value) == expected


def test_placeholder_is_overridable():
    assert redact_url(None, placeholder="") == ""


def test_unparseable_input_is_never_echoed():
    """If parsing fails we must not fall back to returning the raw value."""
    class Hostile(str):
        def __getattribute__(self, name):
            if name in ("encode", "find", "rfind"):
                raise RuntimeError("boom")
            return str.__getattribute__(self, name)

    out = redact_url(Hostile(f"postgresql://u:{PASSWORD}@h/db"))
    assert PASSWORD not in out
