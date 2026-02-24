"""Shared test fixtures for the slack_migrator test suite."""

import json

import pytest

from slack_migrator.core.config import MigrationConfig


@pytest.fixture()
def sample_users():
    """Return a list of sample Slack user dicts."""
    return [
        {
            "id": "U001",
            "name": "alice",
            "real_name": "Alice Smith",
            "profile": {"email": "alice@example.com", "real_name": "Alice Smith"},
            "is_bot": False,
            "deleted": False,
        },
        {
            "id": "U002",
            "name": "bob",
            "real_name": "Bob Jones",
            "profile": {"email": "bob@example.com", "real_name": "Bob Jones"},
            "is_bot": False,
            "deleted": False,
        },
        {
            "id": "B001",
            "name": "testbot",
            "real_name": "Test Bot",
            "profile": {},
            "is_bot": True,
            "deleted": False,
        },
    ]


@pytest.fixture()
def sample_channels():
    """Return a list of sample Slack channel dicts."""
    return [
        {
            "id": "C001",
            "name": "general",
            "members": ["U001", "U002"],
            "purpose": {"value": "General discussion"},
            "topic": {"value": "Welcome"},
        },
        {
            "id": "C002",
            "name": "random",
            "members": ["U001"],
            "purpose": {"value": "Random stuff"},
            "topic": {"value": ""},
        },
    ]


@pytest.fixture()
def mock_config():
    """Return a MigrationConfig with all defaults populated."""
    return MigrationConfig()


@pytest.fixture()
def minimal_export_dir(tmp_path, sample_users, sample_channels):
    """Create a minimal Slack export directory with users.json, channels.json, and channel dirs.

    Returns the tmp_path so tests can add more files as needed.
    """
    (tmp_path / "users.json").write_text(json.dumps(sample_users))
    (tmp_path / "channels.json").write_text(json.dumps(sample_channels))

    for ch in sample_channels:
        ch_dir = tmp_path / ch["name"]
        ch_dir.mkdir(exist_ok=True)

    return tmp_path
