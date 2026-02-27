"""Unit test configuration and shared fixtures."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from slack_migrator.core.config import MigrationConfig
from slack_migrator.core.state import MigrationState, _default_migration_summary

# ---------------------------------------------------------------------------
# Shared mock migrator factory
# ---------------------------------------------------------------------------


def _build_mock_migrator(**kwargs: Any) -> MagicMock:
    """Build a MagicMock that behaves like SlackToChatMigrator.

    Accepts any keyword argument. Keys matching ``MigrationState`` dataclass
    fields are set on ``m.state.<field>``, everything else is set directly on
    the mock (e.g. ``dry_run``, ``user_map``, ``workspace_admin``).

    Comes pre-wired with:
    - ``state`` = fresh ``MigrationState()``
    - ``config`` = default ``MigrationConfig()``
    - ``state.migration_summary`` = zeroed summary
    - ``dry_run = False``
    - ``update_mode = False``
    - ``workspace_admin = "admin@example.com"``
    - ``workspace_domain = "example.com"``
    - ``user_map = {}``
    """
    m = MagicMock()
    m.state = MigrationState()
    m.config = MigrationConfig()
    m.state.migration_summary = _default_migration_summary()
    m.dry_run = False
    m.update_mode = False
    m.workspace_admin = "admin@example.com"
    m.workspace_domain = "example.com"
    m.user_map = {}

    _state_fields = {f for f in MigrationState.__dataclass_fields__}
    for key, value in kwargs.items():
        if key in _state_fields:
            setattr(m.state, key, value)
        else:
            setattr(m, key, value)
    return m


@pytest.fixture()
def make_mock_migrator():
    """Factory fixture — call with kwargs to get a configured mock migrator.

    Usage in tests::

        def test_something(make_mock_migrator):
            m = make_mock_migrator(dry_run=True, current_channel="general")
    """
    return _build_mock_migrator


# ---------------------------------------------------------------------------
# Data builders
# ---------------------------------------------------------------------------


def make_space_dict(
    display_name: str = "general",
    space_name: str = "spaces/SPACE1",
    **overrides: Any,
) -> dict[str, Any]:
    """Build a dict resembling a Google Chat Space API response."""
    d: dict[str, Any] = {
        "displayName": display_name,
        "name": space_name,
        "spaceType": "SPACE",
        "createTime": "2024-01-01T00:00:00Z",
    }
    d.update(overrides)
    return d


def make_message_dict(
    ts: str = "1609459200.000000",
    user: str = "U001",
    text: str = "hello",
    **overrides: Any,
) -> dict[str, Any]:
    """Build a dict resembling a Slack message from a JSON export."""
    d: dict[str, Any] = {
        "type": "message",
        "ts": ts,
        "user": user,
        "text": text,
    }
    d.update(overrides)
    return d
