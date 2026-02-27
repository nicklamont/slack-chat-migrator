"""Tests for slack_migrator.cli.report module."""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import yaml

from slack_migrator.cli.report import generate_report, print_dry_run_summary
from slack_migrator.core.state import _default_migration_summary
from slack_migrator.types import FailedMessage, MigrationSummary
from tests.unit.conftest import _build_mock_migrator


def _make_summary(**overrides: object) -> MigrationSummary:
    """Return a MigrationSummary with defaults, applying any overrides."""
    summary = _default_migration_summary()
    summary.update(overrides)  # type: ignore[typeddict-item]
    return summary


def _make_failed(
    channel: str = "general",
    ts: str = "1234.56",
    error: str = "test error",
    error_details: str = "",
    payload: dict[str, object] | None = None,
) -> FailedMessage:
    """Return a FailedMessage with sensible defaults."""
    return FailedMessage(
        channel=channel,
        ts=ts,
        error=error,
        error_details=error_details,
        payload=payload if payload is not None else {},
    )


def _make_migrator(**overrides):
    """Build a MagicMock that behaves like SlackToChatMigrator for report tests."""
    defaults: dict = dict(
        migration_summary=_make_summary(
            channels_processed=["general", "random"],
            spaces_created=2,
            messages_created=50,
            reactions_created=10,
            files_created=5,
        ),
        user_map={"U001": "alice@example.com", "U002": "bob@example.com"},
        dry_run=True,
        export_root="/tmp/slack_export",
        created_spaces={"general": "spaces/abc", "random": "spaces/def"},
        users_without_email=[],
    )
    defaults.update(overrides)
    m = _build_mock_migrator(**defaults)
    m.user_resolver.is_external_user.return_value = False
    # No file_handler by default — tests that need it can set it
    del m.file_handler
    return m


# ---------------------------------------------------------------------------
# print_dry_run_summary tests
# ---------------------------------------------------------------------------


class TestPrintDryRunSummary:
    """Tests for the print_dry_run_summary function."""

    def test_basic_summary_output(self, capsys):
        migrator = _make_migrator()
        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert "DRY RUN SUMMARY" in out
        assert "Channels processed: 2" in out
        assert "Spaces that would be created: 2" in out
        assert "Messages that would be migrated: 50" in out
        assert "Reactions that would be migrated: 10" in out
        assert "Files that would be migrated: 5" in out
        assert "migration_report.yaml" in out
        assert "run again without --dry-run" in out

    def test_zero_stats(self, capsys):
        migrator = _make_migrator()
        migrator.state.migration_summary = _default_migration_summary()
        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert "Channels processed: 0" in out
        assert "Spaces that would be created: 0" in out
        assert "Messages that would be migrated: 0" in out

    def test_report_file_override(self, capsys):
        migrator = _make_migrator()
        print_dry_run_summary(migrator, report_file="/custom/path/report.yaml")
        out = capsys.readouterr().out

        assert "/custom/path/report.yaml" in out

    def test_default_report_path_uses_output_dir(self, capsys):
        migrator = _make_migrator(output_dir="/my/output")
        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert os.path.join("/my/output", "migration_report.yaml") in out

    def test_default_report_path_when_output_dir_is_none(self, capsys):
        migrator = _make_migrator(output_dir=None)
        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert os.path.join(".", "migration_report.yaml") in out

    def test_users_without_email_shown(self, capsys):
        migrator = _make_migrator(
            users_without_email=[
                {"id": "U099", "name": "bot1"},
                {"id": "U100", "name": "bot2"},
            ]
        )
        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert "Users without email: 2" in out
        assert "mapped in config.yaml" in out

    def test_external_users_shown(self, capsys):
        migrator = _make_migrator()
        migrator.user_map = {
            "U001": "alice@example.com",
            "U002": "ext@other.com",
        }
        migrator.user_resolver.is_external_user.side_effect = lambda email: (
            email == "ext@other.com"
        )
        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert "External users detected: 1" in out
        assert "external user support" in out

    def test_no_external_users_hides_section(self, capsys):
        migrator = _make_migrator()
        migrator.user_resolver.is_external_user.return_value = False
        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert "External users detected" not in out

    def test_file_statistics_shown(self, capsys):
        migrator = _make_migrator()
        # Add file_handler with stats
        file_handler = MagicMock()
        file_handler.get_file_statistics.return_value = {
            "total_files_processed": 10,
            "successful_uploads": 8,
            "failed_uploads": 2,
            "drive_uploads": 6,
            "direct_uploads": 2,
            "external_user_files": 1,
            "ownership_transferred": 5,
            "success_rate": 80.0,
        }
        migrator.file_handler = file_handler

        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert "File Upload Details:" in out
        assert "Total files processed: 10" in out
        assert "Successful uploads: 8" in out
        assert "Failed uploads: 2" in out
        assert "Drive uploads: 6" in out
        assert "Direct uploads: 2" in out
        assert "External user files: 1" in out
        assert "Ownership transferred: 5" in out
        assert "Success rate: 80.0%" in out

    def test_file_statistics_zero_files_hides_details(self, capsys):
        migrator = _make_migrator()
        file_handler = MagicMock()
        file_handler.get_file_statistics.return_value = {
            "total_files_processed": 0,
        }
        migrator.file_handler = file_handler

        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert "File Upload Details:" not in out

    def test_file_statistics_exception_handled(self, capsys):
        migrator = _make_migrator()
        file_handler = MagicMock()
        file_handler.get_file_statistics.side_effect = RuntimeError("stats unavailable")
        migrator.file_handler = file_handler

        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert "Could not retrieve detailed file statistics" in out

    def test_no_users_without_email_hides_section(self, capsys):
        migrator = _make_migrator(users_without_email=[])
        print_dry_run_summary(migrator)
        out = capsys.readouterr().out

        assert "Users without email" not in out


# ---------------------------------------------------------------------------
# generate_report tests
# ---------------------------------------------------------------------------


class TestGenerateReport:
    """Tests for the generate_report function."""

    @patch("slack_migrator.cli.report.log_with_context")
    def test_basic_report_generation(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        result = generate_report(migrator)

        assert result == os.path.join(str(tmp_path), "migration_report.yaml")
        assert os.path.exists(result)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert report["migration_summary"]["channels_processed"] == 2
        assert report["migration_summary"]["spaces_created"] == 2
        assert report["migration_summary"]["messages_migrated"] == 50
        assert report["migration_summary"]["reactions_migrated"] == 10
        assert report["migration_summary"]["files_migrated"] == 5
        assert report["migration_summary"]["dry_run"] is True
        assert report["migration_summary"]["workspace_admin"] == "admin@example.com"
        assert report["migration_summary"]["export_path"] == "/tmp/slack_export"
        assert report["migration_summary"]["failed_messages_count"] == 0
        assert report["migration_summary"]["channels_with_failures"] == 0

    @patch("slack_migrator.cli.report.log_with_context")
    def test_report_uses_dot_when_output_dir_none(
        self, mock_log, tmp_path, monkeypatch
    ):
        """When output_dir is None, report defaults to current directory."""
        monkeypatch.chdir(tmp_path)
        migrator = _make_migrator(output_dir=None)
        result = generate_report(migrator)

        assert result == os.path.join(".", "migration_report.yaml")
        assert os.path.exists(result)

    @patch("slack_migrator.cli.report.log_with_context")
    def test_report_contains_spaces_section(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.channel_stats = {
            "general": {"message_count": 30, "reaction_count": 5, "file_count": 2},
            "random": {"message_count": 20, "reaction_count": 5, "file_count": 3},
        }
        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert "general" in report["spaces"]
        assert report["spaces"]["general"]["messages_migrated"] == 30
        assert report["spaces"]["general"]["reactions_migrated"] == 5
        assert report["spaces"]["general"]["files_migrated"] == 2
        assert "random" in report["spaces"]

    @patch("slack_migrator.cli.report.log_with_context")
    def test_skipped_channels_when_no_space_created(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        # "random" is processed but has no created space
        migrator.state.created_spaces = {"general": "spaces/abc"}
        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert "random" in report["skipped_channels"]
        assert "random" not in report["spaces"]

    @patch("slack_migrator.cli.report.log_with_context")
    def test_failed_messages_grouped_by_channel(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.failed_messages = [
            _make_failed(channel="general", ts="1234.56", error="timeout"),
            _make_failed(channel="general", ts="1234.57", error="rate limit"),
            _make_failed(channel="random", ts="1234.58", error="unknown"),
        ]
        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert report["migration_summary"]["failed_messages_count"] == 3
        assert report["migration_summary"]["channels_with_failures"] == 2
        assert "general" in report["failed_channels"]
        assert "random" in report["failed_channels"]
        assert report["spaces"]["general"]["failed_messages"] == 2
        assert report["spaces"]["random"]["failed_messages"] == 1

    @patch("slack_migrator.cli.report.log_with_context")
    def test_failed_messages_writes_channel_logs(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.failed_messages = [
            _make_failed(
                channel="general",
                ts="1234.56",
                error="timeout",
                payload={"text": "hello"},
            ),
        ]
        generate_report(migrator)

        log_file = os.path.join(str(tmp_path), "channel_logs", "general_migration.log")
        assert os.path.exists(log_file)

        with open(log_file) as f:
            content = f.read()
        assert "FAILED MESSAGES DETAILS" in content
        assert "Timestamp: 1234.56" in content
        assert "Error: timeout" in content
        assert '"text": "hello"' in content

    @patch("slack_migrator.cli.report.log_with_context")
    def test_failed_messages_with_no_payload(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.failed_messages = [
            _make_failed(channel="general", ts="1234.56", error="timeout"),
        ]
        generate_report(migrator)

        log_file = os.path.join(str(tmp_path), "channel_logs", "general_migration.log")
        assert os.path.exists(log_file)

        with open(log_file) as f:
            content = f.read()
        assert "Timestamp: 1234.56" in content
        # "Payload:" should not appear since payload is empty dict
        assert "Payload:" not in content

    @patch("slack_migrator.cli.report.log_with_context")
    def test_failed_messages_unlisted_channel(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.failed_messages = [
            _make_failed(channel="unlisted", ts="1234.56", error="timeout"),
        ]
        generate_report(migrator)

        with open(os.path.join(str(tmp_path), "migration_report.yaml")) as f:
            report = yaml.safe_load(f)

        assert "unlisted" in report["failed_channels"]

    @patch("slack_migrator.cli.report.log_with_context")
    def test_external_users_in_report(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.user_map = {
            "U001": "alice@example.com",
            "U002": "ext@other.com",
        }
        migrator.user_resolver.is_external_user.side_effect = lambda email: (
            email == "ext@other.com"
        )

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert report["users"]["external_user_count"] == 1
        assert "U002" in report["users"]["external_users"]
        assert report["users"]["external_users"]["U002"] == "ext@other.com"

        # Check external user recommendation
        rec_types = [r["type"] for r in report["recommendations"]]
        assert "external_users" in rec_types

        # Check external_user_mappings_for_config section
        assert "external_user_mappings_for_config" in report
        mappings = report["external_user_mappings_for_config"]
        assert any("user_mapping_overrides:" in line for line in mappings)

    @patch("slack_migrator.cli.report.log_with_context")
    def test_no_external_users_no_recommendation(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.user_resolver.is_external_user.return_value = False

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert report["users"]["external_user_count"] == 0
        rec_types = [r["type"] for r in report["recommendations"]]
        assert "external_users" not in rec_types
        assert "external_user_mappings_for_config" not in report

    @patch("slack_migrator.cli.report.log_with_context")
    def test_users_without_email_in_report(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.users_without_email = [
            {
                "id": "U099",
                "name": "slackbot",
                "real_name": "Slack Bot",
                "is_bot": True,
            },
            {"id": "U100", "name": "olduser", "real_name": "Old User"},
        ]

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        users_data = report["users"]["users_without_email"]
        assert "U099" in users_data
        assert users_data["U099"]["type"] == "Bot"
        assert users_data["U099"]["name"] == "slackbot"
        assert "U100" in users_data
        assert users_data["U100"]["type"] == "User"
        assert report["users"]["users_without_email_count"] == 2

        rec_types = [r["type"] for r in report["recommendations"]]
        assert "users_without_email" in rec_types

    @patch("slack_migrator.cli.report.log_with_context")
    def test_users_without_email_app_user(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.users_without_email = [
            {
                "id": "U200",
                "name": "myapp",
                "real_name": "My App",
                "is_app_user": True,
            },
        ]

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert report["users"]["users_without_email"]["U200"]["type"] == "Bot"

    @patch("slack_migrator.cli.report.log_with_context")
    def test_users_without_email_missing_id_skipped(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.users_without_email = [
            {"name": "noid_user"},  # no "id" key
        ]

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        # Should be empty since the user had no id
        assert report["users"]["users_without_email"] == {}

    @patch("slack_migrator.cli.report.log_with_context")
    def test_high_failure_rate_channels_recommendation(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.high_failure_rate_channels = {
            "general": {"failure_rate": 25.0, "failed": 5, "total": 20}
        }
        migrator.config.max_failure_percentage = 10

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert "general" in report["high_failure_rate_channels"]
        rec_types = [r["type"] for r in report["recommendations"]]
        assert "high_failure_rate" in rec_types

    @patch("slack_migrator.cli.report.log_with_context")
    def test_empty_high_failure_rate_channels_no_recommendation(
        self, mock_log, tmp_path
    ):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.high_failure_rate_channels = {}

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        rec_types = [r["type"] for r in report["recommendations"]]
        assert "high_failure_rate" not in rec_types

    @patch("slack_migrator.cli.report.log_with_context")
    def test_channel_conflicts_recommendation(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.channel_conflicts = {"general", "random"}

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert len(report["duplicate_space_conflicts"]) == 2
        rec_types = [r["type"] for r in report["recommendations"]]
        assert "duplicate_space_conflicts" in rec_types

    @patch("slack_migrator.cli.report.log_with_context")
    def test_no_channel_conflicts_no_recommendation(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.channel_conflicts = set()

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert report["duplicate_space_conflicts"] == []
        rec_types = [r["type"] for r in report["recommendations"]]
        assert "duplicate_space_conflicts" not in rec_types

    @patch("slack_migrator.cli.report.log_with_context")
    def test_skipped_reactions_recommendation(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.skipped_reactions = [
            {
                "user_id": "U999",
                "reaction": "thumbsup",
                "message_ts": "123.456",
                "channel": "general",
            },
        ]

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert len(report["skipped_reactions"]) == 1
        rec_types = [r["type"] for r in report["recommendations"]]
        assert "skipped_reactions" in rec_types

    @patch("slack_migrator.cli.report.log_with_context")
    def test_file_statistics_in_report(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        file_handler = MagicMock()
        file_handler.get_file_statistics.return_value = {
            "total_files_processed": 10,
            "successful_uploads": 8,
            "failed_uploads": 2,
        }
        migrator.file_handler = file_handler

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert report["file_upload_details"]["total_files_processed"] == 10
        assert report["file_upload_details"]["successful_uploads"] == 8

    @patch("slack_migrator.cli.report.log_with_context")
    def test_file_statistics_exception_produces_empty_dict(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        file_handler = MagicMock()
        file_handler.get_file_statistics.side_effect = RuntimeError("boom")
        migrator.file_handler = file_handler

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        # file_upload_details should be empty dict since exception occurred
        assert (
            report["file_upload_details"] is None or report["file_upload_details"] == {}
        )

    @patch("slack_migrator.cli.report.log_with_context")
    def test_active_users_by_channel_internal_and_external(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.user_map = {
            "U001": "alice@example.com",
            "U002": "ext@other.com",
            "U003": "bob@example.com",
        }
        migrator.user_resolver.is_external_user.side_effect = lambda email: (
            email == "ext@other.com"
        )
        migrator.state.active_users_by_channel = {
            "general": ["U001", "U002", "U003"],
        }

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        general_space = report["spaces"]["general"]
        assert "alice@example.com" in general_space["internal_users"]
        assert "bob@example.com" in general_space["internal_users"]
        assert "ext@other.com" in general_space["external_users"]

    @patch("slack_migrator.cli.report.log_with_context")
    def test_active_users_unmapped_user_skipped(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.user_map = {"U001": "alice@example.com"}
        migrator.state.active_users_by_channel = {
            "general": ["U001", "U999"],  # U999 not in user_map
        }

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        general_space = report["spaces"]["general"]
        assert (
            len(general_space["internal_users"]) + len(general_space["external_users"])
            == 1
        )

    @patch("slack_migrator.cli.report.log_with_context")
    def test_spaces_with_external_users_flag(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.spaces_with_external_users = {"spaces/abc": True}

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert report["spaces"]["general"]["external_users_allowed"] is True
        assert report["spaces"]["random"]["external_users_allowed"] is False

    @patch("slack_migrator.cli.report.log_with_context")
    def test_migration_issues_in_report(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.migration_issues = {"general": ["issue1", "issue2"]}

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert report["channel_issues"] == {"general": ["issue1", "issue2"]}

    @patch("slack_migrator.cli.report.log_with_context")
    def test_report_log_message(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        generate_report(migrator)

        # Check that log_with_context was called with a message about the report
        log_messages = [str(call) for call in mock_log.call_args_list]
        assert any("Migration report generated" in msg for msg in log_messages)

    @patch("slack_migrator.cli.report.log_with_context")
    def test_empty_channels_processed(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.migration_summary["channels_processed"] = []

        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert report["migration_summary"]["channels_processed"] == 0
        assert report["spaces"] == {}

    @patch("slack_migrator.cli.report.log_with_context")
    def test_failed_messages_channel_log_write_failure(self, mock_log, tmp_path):
        """When writing channel logs fails, it should log an error but not crash."""
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.failed_messages = [
            _make_failed(channel="general", ts="1234.56", error="timeout"),
        ]

        # Patch open to raise when writing channel logs
        original_open = open

        def patched_open(path, mode="r", *args, **kwargs):
            if "channel_logs" in str(path) and mode in ("w", "a"):
                raise OSError("Simulated write failure")
            return original_open(path, mode, *args, **kwargs)

        with patch("builtins.open", side_effect=patched_open):
            generate_report(migrator)

    @patch("slack_migrator.cli.report.log_with_context")
    def test_failed_message_payload_not_serializable(self, mock_log, tmp_path):
        """When payload can't be JSON-serialized, it should use repr fallback."""
        migrator = _make_migrator(output_dir=str(tmp_path))

        # Create an object that can't be JSON-serialized
        class Unserializable:
            def __repr__(self):
                return "<Unserializable>"

        migrator.state.failed_messages = [
            FailedMessage(
                channel="general",
                ts="1234.56",
                error="timeout",
                error_details="",
                payload=Unserializable(),  # type: ignore[typeddict-item]
            ),
        ]

        generate_report(migrator)

        log_file = os.path.join(str(tmp_path), "channel_logs", "general_migration.log")
        with open(log_file) as f:
            content = f.read()
        assert "<Unserializable>" in content

    @patch("slack_migrator.cli.report.log_with_context")
    def test_no_failed_messages_no_channel_logs(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        migrator.state.failed_messages = []

        generate_report(migrator)

        logs_dir = os.path.join(str(tmp_path), "channel_logs")
        assert not os.path.exists(logs_dir)

    @patch("slack_migrator.cli.report.log_with_context")
    def test_timestamp_in_report(self, mock_log, tmp_path):
        migrator = _make_migrator(output_dir=str(tmp_path))
        result = generate_report(migrator)

        with open(result) as f:
            report = yaml.safe_load(f)

        assert "timestamp" in report["migration_summary"]
        # Should be a valid ISO format timestamp
        assert "T" in report["migration_summary"]["timestamp"]
