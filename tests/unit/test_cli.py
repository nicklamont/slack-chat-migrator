"""Tests for the click-based CLI."""

import logging
from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from slack_migrator.cli.commands import cli, handle_exception
from slack_migrator.core.config import MigrationConfig
from slack_migrator.exceptions import (
    ConfigError,
    MigrationAbortedError,
    MigratorError,
    PermissionCheckError,
)


class TestCLIGroup:
    """Tests for the CLI group structure."""

    def test_cli_has_all_subcommands(self):
        expected = {"migrate", "check-permissions", "validate", "cleanup"}
        assert set(cli.commands.keys()) == expected

    def test_version_flag(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert "slack-migrator" in result.output

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

    def test_missing_required_options(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["migrate"])
        assert result.exit_code != 0
        assert "Missing option" in result.output or "Error" in result.output


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

    @patch("slack_migrator.cli.commands.check_permissions_standalone")
    @patch("slack_migrator.cli.commands.load_config")
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

    @patch("slack_migrator.cli.commands.MigrationOrchestrator")
    @patch("slack_migrator.cli.commands.setup_logger")
    @patch("slack_migrator.cli.commands.create_migration_output_directory")
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

    @patch("slack_migrator.cli.commands.cleanup_import_mode_spaces")
    @patch("slack_migrator.cli.commands.get_gcp_service")
    @patch("slack_migrator.cli.commands.load_config")
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
        mock_cleanup.assert_called_once_with(mock_chat)

    @patch("slack_migrator.cli.commands.cleanup_import_mode_spaces")
    @patch("slack_migrator.cli.commands.get_gcp_service")
    @patch("slack_migrator.cli.commands.load_config")
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
        # This should route to migrate, which will fail due to missing options
        # but the error should reference migrate's required options, not
        # an unknown-command error.
        result = runner.invoke(
            cli, ["--creds_path", "fake.json", "--export_path", "fake"]
        )
        # It should NOT be exit code 0 (missing workspace_admin)
        assert result.exit_code != 0
        # The error should be about a missing required option, not about
        # an invalid subcommand
        assert "Missing option" in result.output or "workspace_admin" in result.output


class TestHandleException:
    """Tests for handle_exception()."""

    @patch("slack_migrator.cli.commands.log_with_context")
    def test_handles_migrator_error(self, mock_log):
        handle_exception(MigratorError("test error"))
        mock_log.assert_called_once_with(logging.ERROR, "test error")

    @patch("slack_migrator.cli.commands.log_with_context")
    def test_handles_config_error(self, mock_log):
        handle_exception(ConfigError("bad config"))
        mock_log.assert_called_once_with(logging.ERROR, "bad config")

    @patch("slack_migrator.cli.commands.log_with_context")
    def test_handles_permission_check_error(self, mock_log):
        handle_exception(PermissionCheckError("missing scope"))
        mock_log.assert_called_once_with(logging.ERROR, "missing scope")

    @patch("slack_migrator.cli.commands.log_with_context")
    def test_handles_migration_aborted_error(self, mock_log):
        handle_exception(MigrationAbortedError("aborted"))
        mock_log.assert_called_once_with(logging.ERROR, "aborted")

    @patch("slack_migrator.cli.commands.log_with_context")
    def test_handles_file_not_found(self, mock_log):
        handle_exception(FileNotFoundError("missing.json"))
        assert mock_log.call_count == 2
        assert "missing.json" in str(mock_log.call_args_list[0])

    @patch("slack_migrator.cli.commands.log_with_context")
    def test_handles_generic_exception(self, mock_log):
        handle_exception(RuntimeError("unexpected"))
        mock_log.assert_called_once_with(
            logging.ERROR, "Migration failed: unexpected", exc_info=True
        )


class TestMigrateExceptionPaths:
    """Tests for exception paths in the migrate command."""

    @patch("slack_migrator.cli.commands.show_security_warning")
    @patch("slack_migrator.cli.commands.create_migration_output_directory")
    @patch("slack_migrator.cli.commands.setup_logger")
    @patch("slack_migrator.cli.commands.MigrationOrchestrator")
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

    @patch("slack_migrator.cli.commands.show_security_warning")
    @patch("slack_migrator.cli.commands.create_migration_output_directory")
    @patch("slack_migrator.cli.commands.setup_logger")
    @patch("slack_migrator.cli.commands.MigrationOrchestrator")
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
