"""Tests for the click-based CLI."""

import logging
from unittest.mock import MagicMock, patch

import click
from click.testing import CliRunner

from slack_chat_migrator.cli.commands import cli, handle_exception
from slack_chat_migrator.cli.common import deprecated_command, deprecated_option
from slack_chat_migrator.core.config import MigrationConfig
from slack_chat_migrator.exceptions import (
    ConfigError,
    MigrationAbortedError,
    MigratorError,
    PermissionCheckError,
)
from slack_chat_migrator.services.chat_adapter import ChatAdapter


class TestCLIGroup:
    """Tests for the CLI group structure."""

    def test_cli_has_all_subcommands(self):
        expected = {"migrate", "check-permissions", "validate", "cleanup"}
        assert set(cli.commands.keys()) == expected

    def test_version_flag(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "slack-chat-migrator" in result.output

    def test_help_output(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "migrate" in result.output
        assert "check-permissions" in result.output
        assert "validate" in result.output
        assert "cleanup" in result.output

    def test_short_help_flag(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["-h"])
        assert result.exit_code == 0
        assert "migrate" in result.output


class TestMigrateCommand:
    """Tests for the migrate subcommand."""

    def test_help_shows_all_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate", "--help"])
        assert result.exit_code == 0
        for opt in [
            "--creds_path",
            "--export_path",
            "--workspace_admin",
            "--config",
            "--dry_run",
            "--update_mode",
            "--verbose",
            "--debug_api",
            "--skip_permission_check",
        ]:
            assert opt in result.output

    def test_missing_export_path(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "export_path" in result.output


class TestCheckPermissionsCommand:
    """Tests for the check-permissions subcommand."""

    def test_help_shows_correct_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["check-permissions", "--help"])
        assert result.exit_code == 0
        assert "--creds_path" in result.output
        assert "--workspace_admin" in result.output
        # Should NOT require --export_path
        assert "--export_path" not in result.output

    def test_missing_required_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["check-permissions"])
        assert result.exit_code != 0

    @patch("slack_chat_migrator.cli.permissions_cmd.check_permissions_standalone")
    @patch("slack_chat_migrator.cli.permissions_cmd.load_config")
    def test_invokes_standalone_function(self, mock_load_config, mock_check):
        mock_load_config.return_value = MigrationConfig()
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "check-permissions",
                "--creds_path",
                "fake.json",
                "--workspace_admin",
                "a@b.com",
            ],
        )
        assert result.exit_code == 0
        mock_check.assert_called_once_with(
            creds_path="fake.json",
            workspace_admin="a@b.com",
            max_retries=3,
            retry_delay=2,
        )


class TestValidateCommand:
    """Tests for the validate subcommand."""

    def test_help_shows_correct_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["validate", "--help"])
        assert result.exit_code == 0
        assert "--creds_path" in result.output
        assert "--export_path" in result.output
        assert "--workspace_admin" in result.output

    def test_missing_required_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["validate"])
        assert result.exit_code != 0

    @patch("slack_chat_migrator.cli.validate_cmd.MigrationOrchestrator")
    @patch("slack_chat_migrator.cli.validate_cmd.setup_logger")
    @patch("slack_chat_migrator.cli.validate_cmd.create_migration_output_directory")
    def test_always_sets_dry_run_true(self, mock_outdir, mock_log, mock_orch_cls):
        """Validate always passes dry_run=True to the orchestrator."""
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
        mock_orch_cls.return_value = mock_orch

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "validate",
                "--creds_path",
                "fake.json",
                "--export_path",
                "fake",
                "--workspace_admin",
                "a@b.com",
            ],
        )
        assert result.exit_code == 0
        # The args namespace passed to MigrationOrchestrator must have dry_run=True
        args = mock_orch_cls.call_args[0][0]
        assert args.dry_run is True


class TestCleanupCommand:
    """Tests for the cleanup subcommand."""

    def test_help_shows_correct_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["cleanup", "--help"])
        assert result.exit_code == 0
        assert "--creds_path" in result.output
        assert "--workspace_admin" in result.output
        # Should NOT require --export_path
        assert "--export_path" not in result.output

    def test_missing_required_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["cleanup"])
        assert result.exit_code != 0

    @patch("slack_chat_migrator.cli.cleanup_cmd.cleanup_import_mode_spaces")
    @patch("slack_chat_migrator.cli.cleanup_cmd.get_gcp_service")
    @patch("slack_chat_migrator.cli.cleanup_cmd.load_config")
    def test_invokes_standalone_cleanup(self, mock_config, mock_svc, mock_cleanup):
        mock_config.return_value = MigrationConfig()
        mock_chat = MagicMock()
        mock_svc.return_value = mock_chat

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "cleanup",
                "--creds_path",
                "fake.json",
                "--workspace_admin",
                "a@b.com",
                "--yes",
            ],
        )
        assert result.exit_code == 0
        mock_cleanup.assert_called_once()
        adapter = mock_cleanup.call_args[0][0]
        assert isinstance(adapter, ChatAdapter)
        assert adapter._svc is mock_chat

    @patch("slack_chat_migrator.cli.cleanup_cmd.cleanup_import_mode_spaces")
    @patch("slack_chat_migrator.cli.cleanup_cmd.get_gcp_service")
    @patch("slack_chat_migrator.cli.cleanup_cmd.load_config")
    def test_prompts_for_confirmation_without_yes(
        self, mock_config, mock_svc, mock_cleanup
    ):
        """Without --yes, cleanup should prompt and abort on 'n'."""
        mock_config.return_value = MigrationConfig()
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "cleanup",
                "--creds_path",
                "fake.json",
                "--workspace_admin",
                "a@b.com",
            ],
            input="n\n",
        )
        assert result.exit_code == 0
        assert "Cleanup cancelled" in result.output
        mock_cleanup.assert_not_called()


class TestBackwardsCompatibility:
    """Tests that flags without a subcommand route to migrate."""

    def test_flags_without_subcommand_route_to_migrate(self):
        """When args start with -- (no subcommand), DefaultGroup prepends migrate."""
        runner = CliRunner()
        # This should route to migrate, which will fail due to missing --export_path
        # but the error should reference migrate's required options, not
        # an unknown-command error.
        result = runner.invoke(cli, ["--creds_path", "fake.json"])
        assert result.exit_code != 0
        # The error should be about a missing required option, not about
        # an invalid subcommand
        assert "Missing option" in result.output or "export_path" in result.output


class TestHandleException:
    """Tests for handle_exception()."""

    @patch("slack_chat_migrator.cli.common.log_with_context")
    def test_handles_migrator_error(self, mock_log):
        handle_exception(MigratorError("test error"))
        mock_log.assert_called_once_with(logging.ERROR, "test error")

    @patch("slack_chat_migrator.cli.common.log_with_context")
    def test_handles_config_error(self, mock_log):
        handle_exception(ConfigError("bad config"))
        mock_log.assert_called_once_with(logging.ERROR, "bad config")

    @patch("slack_chat_migrator.cli.common.log_with_context")
    def test_handles_permission_check_error(self, mock_log):
        handle_exception(PermissionCheckError("missing scope"))
        mock_log.assert_called_once_with(logging.ERROR, "missing scope")

    @patch("slack_chat_migrator.cli.common.log_with_context")
    def test_handles_migration_aborted_error(self, mock_log):
        handle_exception(MigrationAbortedError("aborted"))
        mock_log.assert_called_once_with(logging.ERROR, "aborted")

    @patch("slack_chat_migrator.cli.common.log_with_context")
    def test_handles_file_not_found(self, mock_log):
        handle_exception(FileNotFoundError("missing.json"))
        assert mock_log.call_count == 2
        assert "missing.json" in str(mock_log.call_args_list[0])

    @patch("slack_chat_migrator.cli.common.log_with_context")
    def test_handles_generic_exception(self, mock_log):
        handle_exception(RuntimeError("unexpected"))
        mock_log.assert_called_once_with(
            logging.ERROR, "Migration failed: unexpected", exc_info=True
        )


class TestMigrateExceptionPaths:
    """Tests for exception paths in the migrate command."""

    @patch("slack_chat_migrator.cli.migrate_cmd.show_security_warning")
    @patch("slack_chat_migrator.cli.migrate_cmd.create_migration_output_directory")
    @patch("slack_chat_migrator.cli.migrate_cmd.setup_logger")
    @patch("slack_chat_migrator.cli.migrate_cmd.MigrationOrchestrator")
    def test_config_error_exits_with_code_1(
        self, mock_orch_cls, mock_logger, mock_outdir, mock_warn
    ):
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
        mock_orch.validate_prerequisites.side_effect = ConfigError("creds not found")
        mock_orch_cls.return_value = mock_orch

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "migrate",
                "--creds_path",
                "fake.json",
                "--export_path",
                "fake",
                "--workspace_admin",
                "a@b.com",
            ],
        )
        assert result.exit_code == 1

    @patch("slack_chat_migrator.cli.migrate_cmd.show_security_warning")
    @patch("slack_chat_migrator.cli.migrate_cmd.create_migration_output_directory")
    @patch("slack_chat_migrator.cli.migrate_cmd.setup_logger")
    @patch("slack_chat_migrator.cli.migrate_cmd.MigrationOrchestrator")
    def test_migration_aborted_exits_with_code_1(
        self, mock_orch_cls, mock_logger, mock_outdir, mock_warn
    ):
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
        mock_orch.validate_prerequisites.return_value = None
        mock_orch.run_migration.side_effect = MigrationAbortedError("unmapped users")
        mock_orch_cls.return_value = mock_orch

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "migrate",
                "--creds_path",
                "fake.json",
                "--export_path",
                "fake",
                "--workspace_admin",
                "a@b.com",
            ],
        )
        assert result.exit_code == 1


class TestCredentialFreeDryRun:
    """Tests for credential-free dry-run mode."""

    @patch("slack_chat_migrator.cli.migrate_cmd.show_security_warning")
    @patch("slack_chat_migrator.cli.migrate_cmd.create_migration_output_directory")
    @patch("slack_chat_migrator.cli.migrate_cmd.setup_logger")
    @patch("slack_chat_migrator.cli.migrate_cmd.MigrationOrchestrator")
    def test_dry_run_without_credentials(
        self, mock_orch_cls, mock_logger, mock_outdir, mock_warn
    ):
        """Dry-run mode accepts missing --creds_path and --workspace_admin."""
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
        mock_orch_cls.return_value = mock_orch

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "migrate",
                "--export_path",
                "fake",
                "--dry_run",
            ],
        )
        assert result.exit_code == 0
        mock_orch.validate_prerequisites.assert_called_once()
        mock_orch.run_migration.assert_called_once()

    @patch("slack_chat_migrator.cli.migrate_cmd.show_security_warning")
    @patch("slack_chat_migrator.cli.migrate_cmd.create_migration_output_directory")
    @patch("slack_chat_migrator.cli.migrate_cmd.setup_logger")
    @patch("slack_chat_migrator.cli.migrate_cmd.MigrationOrchestrator")
    def test_live_run_requires_creds_path(
        self, mock_orch_cls, mock_logger, mock_outdir, mock_warn
    ):
        """Live migration (no --dry_run) rejects missing --creds_path."""
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
        mock_orch.validate_prerequisites.side_effect = click.UsageError(
            "--creds_path is required for live migration"
        )
        mock_orch_cls.return_value = mock_orch

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "migrate",
                "--export_path",
                "fake",
            ],
        )
        assert result.exit_code != 0

    @patch("slack_chat_migrator.cli.validate_cmd.MigrationOrchestrator")
    @patch("slack_chat_migrator.cli.validate_cmd.setup_logger")
    @patch("slack_chat_migrator.cli.validate_cmd.create_migration_output_directory")
    def test_validate_without_credentials(self, mock_outdir, mock_log, mock_orch_cls):
        """Validate command works without --creds_path or --workspace_admin."""
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
        mock_orch_cls.return_value = mock_orch

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "validate",
                "--export_path",
                "fake",
            ],
        )
        assert result.exit_code == 0
        args = mock_orch_cls.call_args[0][0]
        assert args.dry_run is True
        assert args.creds_path is None
        assert args.workspace_admin is None

    def test_cleanup_requires_creds_path(self):
        """Cleanup command rejects missing --creds_path."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["cleanup", "--yes"],
        )
        assert result.exit_code != 0

    def test_check_permissions_requires_creds_path(self):
        """Check-permissions command rejects missing --creds_path."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["check-permissions"],
        )
        assert result.exit_code != 0

    def test_dry_run_with_nonexistent_creds_raises(self):
        """Dry-run with a supplied but nonexistent creds file raises ConfigError."""
        from types import SimpleNamespace

        from slack_chat_migrator.cli.migrate_cmd import MigrationOrchestrator
        from slack_chat_migrator.exceptions import ConfigError

        args = SimpleNamespace(
            creds_path="/nonexistent/fake_creds.json",
            export_path="fake",
            workspace_admin="a@b.com",
            config="config.yaml",
            verbose=False,
            debug_api=False,
            dry_run=True,
            update_mode=False,
            skip_permission_check=False,
        )
        orchestrator = MigrationOrchestrator(args)
        import pytest

        with pytest.raises(ConfigError, match="Credentials file not found"):
            orchestrator.validate_prerequisites()


class TestDeprecatedOption:
    """Tests for the deprecated_option helper.

    ``deprecated_option`` creates a hidden Click option that forwards its
    value to the canonical option when used, emitting a deprecation warning.
    The canonical option must be declared separately.
    """

    def test_deprecated_flag_emits_warning(self):
        """Using the deprecated flag emits a warning to stderr."""

        @click.command()
        @click.option("--resume", is_flag=True, default=False)
        @deprecated_option("--update_mode", "--resume", is_flag=True, default=False)
        def cmd(resume: bool) -> None:
            click.echo(f"resume={resume}")

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(cmd, ["--update_mode"])
        assert result.exit_code == 0
        assert "deprecated" in result.stderr.lower()
        assert "--resume" in result.stderr
        assert "resume=True" in result.output

    def test_deprecated_flag_passes_value(self):
        """Deprecated flag value is forwarded to the canonical parameter."""

        @click.command()
        @click.option("--output", default="default.yaml")
        @deprecated_option("--old_output", "--output", default=None)
        def cmd(output: str) -> None:
            click.echo(f"output={output}")

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(cmd, ["--old_output", "custom.yaml"])
        assert result.exit_code == 0
        assert "output=custom.yaml" in result.output
        assert "deprecated" in result.stderr.lower()

    def test_no_warning_when_new_flag_used(self):
        """No warning when the canonical flag is used."""

        @click.command()
        @click.option("--resume", is_flag=True, default=False)
        @deprecated_option("--update_mode", "--resume", is_flag=True, default=False)
        def cmd(resume: bool) -> None:
            click.echo(f"resume={resume}")

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(cmd, ["--resume"])
        assert result.exit_code == 0
        assert result.stderr == ""
        assert "resume=True" in result.output

    def test_no_warning_when_flag_not_used(self):
        """No warning when neither flag is used."""

        @click.command()
        @click.option("--resume", is_flag=True, default=False)
        @deprecated_option("--update_mode", "--resume", is_flag=True, default=False)
        def cmd(resume: bool) -> None:
            click.echo(f"resume={resume}")

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(cmd, [])
        assert result.exit_code == 0
        assert result.stderr == ""
        assert "resume=False" in result.output

    def test_new_flag_wins_when_both_provided(self):
        """When both old and new flags are given, the new flag takes priority."""

        @click.command()
        @click.option("--resume", is_flag=True, default=False)
        @deprecated_option("--update_mode", "--resume", is_flag=True, default=False)
        def cmd(resume: bool) -> None:
            click.echo(f"resume={resume}")

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(cmd, ["--update_mode", "--resume"])
        assert result.exit_code == 0
        assert "deprecated" in result.stderr.lower()
        # New flag was explicitly provided, so its value prevails
        assert "resume=True" in result.output


class TestDeprecatedCommand:
    """Tests for the deprecated_command helper."""

    def test_deprecated_command_emits_warning(self):
        """Running a deprecated command emits a warning to stderr."""

        @click.group()
        def grp() -> None:
            pass

        @grp.command("old-cmd")
        @deprecated_command("old-cmd", "Use 'new-cmd' instead.")
        def old_cmd() -> None:
            click.echo("ran old command")

        runner = CliRunner(mix_stderr=False)
        result = runner.invoke(grp, ["old-cmd"])
        assert result.exit_code == 0
        assert "deprecated" in result.stderr.lower()
        assert "new-cmd" in result.stderr
        assert "ran old command" in result.output
