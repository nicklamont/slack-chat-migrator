"""Unit tests for MigrationContext."""

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from slack_migrator.core.config import MigrationConfig
from slack_migrator.core.context import MigrationContext


def _make_ctx(**overrides) -> MigrationContext:
    """Build a MigrationContext with sensible test defaults."""
    defaults = {
        "export_root": Path("/fake/export"),
        "creds_path": "creds.json",
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


class TestMigrationContextCreation:
    """Test MigrationContext construction and field access."""

    def test_all_fields_accessible(self):
        ctx = _make_ctx(dry_run=True, verbose=True)
        assert ctx.dry_run is True
        assert ctx.verbose is True
        assert ctx.export_root == Path("/fake/export")
        assert ctx.creds_path == "creds.json"
        assert ctx.workspace_admin == "admin@example.com"
        assert ctx.workspace_domain == "example.com"

    def test_user_data_stored(self):
        user_map = {"U001": "alice@example.com"}
        users_without = [{"id": "U999", "name": "bot"}]
        ctx = _make_ctx(user_map=user_map, users_without_email=users_without)
        assert ctx.user_map == {"U001": "alice@example.com"}
        assert ctx.users_without_email == [{"id": "U999", "name": "bot"}]

    def test_channel_metadata_stored(self):
        meta = {"general": {"id": "C001", "name": "general"}}
        id_to_name = {"C001": "general"}
        name_to_id = {"general": "C001"}
        ctx = _make_ctx(
            channels_meta=meta,
            channel_id_to_name=id_to_name,
            channel_name_to_id=name_to_id,
        )
        assert ctx.channels_meta["general"]["id"] == "C001"
        assert ctx.channel_id_to_name["C001"] == "general"
        assert ctx.channel_name_to_id["general"] == "C001"

    def test_config_stored(self):
        config = MigrationConfig(max_retries=5, ignore_bots=True)
        ctx = _make_ctx(config=config)
        assert ctx.config.max_retries == 5
        assert ctx.config.ignore_bots is True


class TestMigrationContextFrozen:
    """Test that MigrationContext is truly immutable."""

    def test_cannot_set_scalar_field(self):
        ctx = _make_ctx()
        with pytest.raises(FrozenInstanceError):
            ctx.dry_run = True  # type: ignore[misc]

    def test_cannot_set_path_field(self):
        ctx = _make_ctx()
        with pytest.raises(FrozenInstanceError):
            ctx.export_root = Path("/other")  # type: ignore[misc]

    def test_cannot_set_config_field(self):
        ctx = _make_ctx()
        with pytest.raises(FrozenInstanceError):
            ctx.config = MigrationConfig()  # type: ignore[misc]

    def test_cannot_replace_dict_field(self):
        ctx = _make_ctx()
        with pytest.raises(FrozenInstanceError):
            ctx.user_map = {"new": "value"}  # type: ignore[misc]


class TestMigrationContextProperties:
    """Test derived properties on MigrationContext."""

    def test_import_mode_true_when_not_update(self):
        ctx = _make_ctx(update_mode=False)
        assert ctx.import_mode is True

    def test_import_mode_false_when_update(self):
        ctx = _make_ctx(update_mode=True)
        assert ctx.import_mode is False

    def test_progress_file(self):
        ctx = _make_ctx(export_root=Path("/data/export"))
        assert ctx.progress_file == Path("/data/export/.migration_progress.json")

    def test_log_prefix_no_flags(self):
        ctx = _make_ctx(dry_run=False, update_mode=False)
        assert ctx.log_prefix == ""

    def test_log_prefix_dry_run(self):
        ctx = _make_ctx(dry_run=True, update_mode=False)
        assert ctx.log_prefix == "[DRY RUN] "

    def test_log_prefix_update_mode(self):
        ctx = _make_ctx(dry_run=False, update_mode=True)
        assert ctx.log_prefix == "[UPDATE MODE] "

    def test_log_prefix_both(self):
        ctx = _make_ctx(dry_run=True, update_mode=True)
        assert ctx.log_prefix == "[DRY RUN] [UPDATE MODE] "


class TestMigrationContextIntegrationWithMigrator:
    """Test that MigrationContext is properly wired into the migrator."""

    def test_migrator_creates_ctx(self, tmp_path):
        """Verify the migrator creates a ctx that matches its own attributes."""
        import json

        from slack_migrator.core.migrator import SlackToChatMigrator

        # Set up minimal export
        users = [
            {"id": "U001", "name": "alice", "profile": {"email": "alice@example.com"}}
        ]
        channels = [{"id": "C001", "name": "general", "members": ["U001"]}]
        (tmp_path / "users.json").write_text(json.dumps(users))
        (tmp_path / "channels.json").write_text(json.dumps(channels))
        (tmp_path / "general").mkdir()

        m = SlackToChatMigrator(
            creds_path="fake.json",
            export_path=str(tmp_path),
            workspace_admin="admin@test.com",
            config_path=str(tmp_path / "config.yaml"),
            dry_run=True,
            verbose=True,
            update_mode=False,
            debug_api=True,
        )

        # ctx fields match migrator attributes
        assert m.ctx.dry_run == m.dry_run
        assert m.ctx.verbose == m.verbose
        assert m.ctx.debug_api == m.debug_api
        assert m.ctx.update_mode == m.update_mode
        assert m.ctx.export_root == m.export_root
        assert m.ctx.creds_path == m.creds_path
        assert m.ctx.workspace_admin == m.workspace_admin
        assert m.ctx.workspace_domain == m.workspace_domain
        assert m.ctx.config is m.config
        assert m.ctx.user_map is m.user_map
        assert m.ctx.channels_meta is m.channels_meta
        assert m.ctx.channel_id_to_name is m.channel_id_to_name
        assert m.ctx.channel_name_to_id is m.channel_name_to_id

        # Derived properties work
        assert m.ctx.import_mode is True
        assert m.ctx.log_prefix == "[DRY RUN] "
        assert m.ctx.progress_file == tmp_path / ".migration_progress.json"
