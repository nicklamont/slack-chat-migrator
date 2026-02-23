"""Unit tests for the migrator module."""

import json

import pytest

from slack_migrator.core.migrator import SlackToChatMigrator


def _setup_export(tmp_path, users=None, channels=None):
    """Set up a minimal export directory structure for testing."""
    users = users or [
        {"id": "U001", "name": "alice", "profile": {"email": "alice@example.com"}}
    ]
    channels = channels or [{"id": "C001", "name": "general", "members": ["U001"]}]

    (tmp_path / "users.json").write_text(json.dumps(users))
    (tmp_path / "channels.json").write_text(json.dumps(channels))

    # Create at least one channel directory
    for ch in channels:
        ch_dir = tmp_path / ch["name"]
        ch_dir.mkdir(exist_ok=True)


class TestEmailValidation:
    """Tests for workspace_admin email validation in __init__."""

    def test_valid_email(self, tmp_path):
        _setup_export(tmp_path)
        # Should not raise
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="admin@example.com",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.workspace_admin == "admin@example.com"

    def test_invalid_email_no_at(self, tmp_path):
        _setup_export(tmp_path)
        with pytest.raises(ValueError, match="Invalid workspace_admin email"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="not-an-email",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )

    def test_invalid_email_empty(self, tmp_path):
        _setup_export(tmp_path)
        with pytest.raises(ValueError, match="Invalid workspace_admin email"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )

    def test_invalid_email_at_only(self, tmp_path):
        _setup_export(tmp_path)
        with pytest.raises(ValueError, match="Invalid workspace_admin email"):
            SlackToChatMigrator(
                creds_path="fake_creds.json",
                export_path=str(tmp_path),
                workspace_admin="@",
                config_path=str(tmp_path / "config.yaml"),
                dry_run=True,
            )

    def test_whitespace_stripped(self, tmp_path):
        _setup_export(tmp_path)
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="  admin@example.com  ",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.workspace_admin == "admin@example.com"


class TestIsExternalUser:
    """Tests for _is_external_user()."""

    def _make_migrator(self, tmp_path, domain="example.com"):
        _setup_export(tmp_path)
        return SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin=f"admin@{domain}",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )

    def test_internal_user(self, tmp_path):
        m = self._make_migrator(tmp_path, "example.com")
        assert m._is_external_user("user@example.com") is False

    def test_external_user(self, tmp_path):
        m = self._make_migrator(tmp_path, "example.com")
        assert m._is_external_user("user@other.com") is True

    def test_case_insensitive(self, tmp_path):
        m = self._make_migrator(tmp_path, "example.com")
        assert m._is_external_user("user@EXAMPLE.COM") is False

    def test_none_email(self, tmp_path):
        m = self._make_migrator(tmp_path, "example.com")
        assert m._is_external_user(None) is False

    def test_empty_email(self, tmp_path):
        m = self._make_migrator(tmp_path, "example.com")
        assert m._is_external_user("") is False


class TestExportPathValidation:
    """Tests for export path validation."""

    def test_valid_export_path(self, tmp_path):
        _setup_export(tmp_path)
        # Should not raise
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="admin@example.com",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.export_root == tmp_path

    def test_workspace_domain_extraction(self, tmp_path):
        _setup_export(tmp_path)
        m = SlackToChatMigrator(
            creds_path="fake_creds.json",
            export_path=str(tmp_path),
            workspace_admin="admin@mycompany.com",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
        )
        assert m.workspace_domain == "mycompany.com"
