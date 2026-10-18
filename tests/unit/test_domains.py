"""Unit tests for the internal-domain helpers."""

import pytest

from slack_chat_migrator.utils.domains import (
    internal_domains,
    is_external_email,
    normalize_domain,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (" Example.COM ", "example.com"),
        ("@corp.example.net", "corp.example.net"),
        (" @ corp.example.net", "corp.example.net"),
        ("", ""),
        (None, ""),
    ],
)
def test_normalize_domain(raw, expected):
    assert normalize_domain(raw) == expected


def test_internal_domains_joins_admin_and_listed_domains():
    assert internal_domains("Example.com", [" corp.example.net", "", "@x.io"]) == {
        "example.com",
        "corp.example.net",
        "x.io",
    }


@pytest.mark.parametrize("admin_domain", [None, "", " "])
def test_internal_domains_without_admin_domain(admin_domain):
    """Without the primary domain nobody is external (#89)."""
    domains = internal_domains(admin_domain, ["corp.example.net"])

    assert domains == frozenset()
    assert is_external_email("alice@example.com", domains) is False


@pytest.mark.parametrize(
    ("email", "external"),
    [
        ("a@example.com", False),
        (" A@Corp.Example.NET ", False),
        ("x@other.org", True),
        ("x@sub.example.com", True),  # subdomains aren't listed, so external
        ("", False),
        (None, False),
    ],
)
def test_is_external_email(email, external):
    domains = internal_domains("example.com", ["corp.example.net"])
    assert is_external_email(email, domains) is external


def test_nobody_is_external_without_known_domains():
    assert is_external_email("x@other.org", frozenset()) is False
