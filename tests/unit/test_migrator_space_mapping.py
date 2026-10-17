"""The config file's space_mapping reaches SlackToChatMigrator (issue #87)."""

from __future__ import annotations

from pathlib import Path

import pytest

from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.core.migrator import SlackToChatMigrator
from slack_chat_migrator.exceptions import ConfigError


def _migrator(
    export: Path, config_text: str, *, config: MigrationConfig | None = None
) -> SlackToChatMigrator:
    config_path = export / "config.yaml"
    config_path.write_text(config_text)
    return SlackToChatMigrator(
        creds_path=None,
        export_path=str(export),
        workspace_admin="admin@example.com",
        config_path=str(config_path),
        dry_run=True,
        config=config,
    )


@pytest.fixture()
def export(minimal_export_dir: Path) -> Path:
    return minimal_export_dir


class TestSpaceMappingReachesTheMigrator:
    @pytest.mark.parametrize("passed_config", [None, MigrationConfig()])
    def test_mapping_is_loaded_into_state(
        self, export: Path, passed_config: MigrationConfig | None
    ) -> None:
        """Both the CLI path (config loaded here) and a library caller's own
        MigrationConfig load space_mapping from the config file."""
        m = _migrator(
            export,
            'space_mapping:\n  "#general": spaces/AAAA111\n  random: BBBB222\n',
            config=passed_config,
        )
        assert m.state.spaces.space_mapping == {
            "general": "AAAA111",
            "random": "BBBB222",
        }

    @pytest.mark.parametrize(
        ("text", "match"),
        [
            ("space_mapping: [general]\n", "must be a mapping"),
            ("space_mapping:\n  general: AAA\n  '#general': BBB\n", "more than once"),
            ("space_mapping:\n  general: AAA\n  general: BBB\n", "duplicate key"),
            ("space_mapping:\n  general:\n", "must be a space ID"),
        ],
    )
    def test_malformed_mapping_raises_with_a_passed_config(
        self, export: Path, text: str, match: str
    ) -> None:
        """A library caller that skips load_config still gets the error."""
        with pytest.raises(ConfigError, match=match):
            _migrator(export, text, config=MigrationConfig())

    def test_malformed_mapping_raises_when_the_migrator_loads_the_config(
        self, export: Path
    ) -> None:
        with pytest.raises(ConfigError, match="space_mapping"):
            _migrator(export, "space_mapping: [general]\n")
