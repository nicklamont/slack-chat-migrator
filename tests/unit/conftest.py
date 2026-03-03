"""Shared fixtures and data builders for unit tests.

Provides a single ``make_ctx`` factory for ``MigrationContext``, a fresh
``MigrationState``, and data builders for common dict shapes used across
tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from slack_migrator.core.config import MigrationConfig
from slack_migrator.core.context import MigrationContext
from slack_migrator.core.state import MigrationState

# ---------------------------------------------------------------------------
# MigrationContext factory
# ---------------------------------------------------------------------------


def _make_ctx(**overrides: Any) -> MigrationContext:
    """Build a MigrationContext with sensible test defaults.

    Any keyword argument that matches a MigrationContext field will
    override the default value.
    """
    defaults: dict[str, Any] = {
        "export_root": Path("/fake/export"),
        "creds_path": "/fake/creds.json",
        "workspace_admin": "admin@example.com",
        "workspace_domain": "example.com",
        "dry_run": False,
        "update_mode": False,
        "verbose": False,
        "debug_api": False,
        "config": MigrationConfig(),
        "user_map": {},
        "users_without_email": [],
        "channels_meta": {},
        "channel_id_to_name": {},
        "channel_name_to_id": {},
    }
    defaults.update(overrides)
    return MigrationContext(**defaults)


@pytest.fixture()
def make_ctx():
    """Fixture that returns the ``_make_ctx`` factory.

    Usage::

        def test_something(make_ctx):
            ctx = make_ctx(dry_run=True, user_map={"U1": "a@b.com"})
    """
    return _make_ctx


@pytest.fixture()
def fresh_state():
    """Return a fresh MigrationState instance."""
    return MigrationState()


# ---------------------------------------------------------------------------
# Data builders — common dict shapes
# ---------------------------------------------------------------------------


def make_space_dict(
    display_name: str = "Slack #general",
    space_name: str = "spaces/abc123",
    **overrides: Any,
) -> dict[str, Any]:
    """Build a Google Chat space dict matching the API response shape."""
    d: dict[str, Any] = {
        "name": space_name,
        "displayName": display_name,
        "spaceType": "SPACE",
    }
    d.update(overrides)
    return d


def make_message_dict(
    ts: str = "1609459200.000000",
    user: str = "U001",
    text: str = "hello",
    **overrides: Any,
) -> dict[str, Any]:
    """Build a Slack message dict matching the export JSON shape."""
    d: dict[str, Any] = {
        "ts": ts,
        "user": user,
        "text": text,
        "type": "message",
    }
    d.update(overrides)
    return d


def make_channel_meta(
    name: str = "general",
    channel_id: str = "C001",
    members: list[str] | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    """Build a Slack channel metadata dict."""
    d: dict[str, Any] = {
        "id": channel_id,
        "name": name,
        "members": members or ["U001"],
        "purpose": {"value": ""},
        "topic": {"value": ""},
    }
    d.update(overrides)
    return d
