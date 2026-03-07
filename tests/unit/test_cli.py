"""Tests for the click-based CLI."""

import logging
from unittest.mock import MagicMock, patch

import click
import pytest
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
        expected = {
            "migrate",
            "check-permissions",
            "validate",
            "cleanup",
            "init",
            "setup",
        }
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
            "--resume",
            "--complete",
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

    @patch("slack_chat_migrator.cli.permissions_cmd.check_permissions_standalone")
    @patch("slack_chat_migrator.cli.permissions_cmd.load_config")
    def test_emits_deprecation_warning(self, mock_load_config, mock_check):
        """check-permissions emits a deprecation warning."""
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
        assert "deprecated" in result.output.lower()
        assert "validate" in result.output.lower()


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
    def test_always_sets_dry_run_true(
        self, mock_outdir, mock_log, mock_orch_cls, tmp_path
    ):
        """Validate always passes dry_run=True to the orchestrator."""
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
        mock_orch_cls.return_value = mock_orch
        config_file = tmp_path / "config.yaml"
        config_file.write_text("{}")

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
                "--config",
                str(config_file),
            ],
        )
        assert result.exit_code == 0, result.output
        # The args namespace passed to MigrationOrchestrator must have dry_run=True
        args = mock_orch_cls.call_args[0][0]
        assert args.dry_run is True

    @patch("slack_chat_migrator.cli.validate_cmd.MigrationOrchestrator")
    @patch("slack_chat_migrator.cli.validate_cmd.setup_logger")
    @patch("slack_chat_migrator.cli.validate_cmd.create_migration_output_directory")
    def test_delegates_to_orchestrator_validate_prerequisites(
        self, mock_outdir, mock_log, mock_orch_cls, tmp_path
    ):
        """Validate command delegates permission checks to orchestrator."""
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
        mock_orch_cls.return_value = mock_orch
        config_file = tmp_path / "config.yaml"
        config_file.write_text("{}")

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
                "--config",
                str(config_file),
            ],
        )
        assert result.exit_code == 0, result.output
        mock_orch.validate_prerequisites.assert_called_once()
        mock_orch.run_migration.assert_called_once()


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
        assert "Missing option" in result.output or "export_path" in result.output

    def test_implicit_migrate_emits_deprecation_warning(self):
        """DefaultGroup emits deprecation when implicitly routing to migrate."""
        runner = CliRunner()
        result = runner.invoke(cli, ["--creds_path", "fake.json"])
        assert result.exit_code != 0
        assert "deprecated" in result.output.lower()
        assert "migrate" in result.output.lower()


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
    def test_validate_without_credentials(
        self, mock_outdir, mock_log, mock_orch_cls, tmp_path
    ):
        """Validate command works without --creds_path or --workspace_admin."""
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
        mock_orch_cls.return_value = mock_orch
        config_file = tmp_path / "config.yaml"
        config_file.write_text("{}")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "validate",
                "--export_path",
                "fake",
                "--config",
                str(config_file),
            ],
        )
        assert result.exit_code == 0, result.output
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


class TestMigrateResumeFlag:
    """Tests for --resume flag and --update_mode deprecation."""

    @patch("slack_chat_migrator.cli.migrate_cmd.show_security_warning")
    @patch("slack_chat_migrator.cli.migrate_cmd.create_migration_output_directory")
    @patch("slack_chat_migrator.cli.migrate_cmd.setup_logger")
    @patch("slack_chat_migrator.cli.migrate_cmd.MigrationOrchestrator")
    def test_resume_maps_to_update_mode(
        self, mock_orch_cls, mock_logger, mock_outdir, mock_warn
    ):
        """--resume sets update_mode=True in the args namespace."""
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
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
                "--resume",
            ],
        )
        assert result.exit_code == 0
        args = mock_orch_cls.call_args[0][0]
        assert args.update_mode is True

    @patch("slack_chat_migrator.cli.migrate_cmd.show_security_warning")
    @patch("slack_chat_migrator.cli.migrate_cmd.create_migration_output_directory")
    @patch("slack_chat_migrator.cli.migrate_cmd.setup_logger")
    @patch("slack_chat_migrator.cli.migrate_cmd.MigrationOrchestrator")
    def test_update_mode_emits_deprecation(
        self, mock_orch_cls, mock_logger, mock_outdir, mock_warn
    ):
        """--update_mode still works but emits a deprecation warning."""
        mock_outdir.return_value = "/tmp/fake"
        mock_orch = MagicMock()
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
                "--update_mode",
            ],
        )
        assert result.exit_code == 0
        assert "deprecated" in result.output.lower()
        assert "--resume" in result.output
        args = mock_orch_cls.call_args[0][0]
        assert args.update_mode is True


class TestMigrateCompleteFlag:
    """Tests for --complete flag."""

    def test_complete_runs_cleanup(self):
        """--complete runs cleanup_import_mode_spaces instead of migration."""
        runner = CliRunner()
        # Patch at source — these are deferred imports inside _run_complete_mode
        with (
            patch(
                "slack_chat_migrator.services.spaces.space_creator.cleanup_import_mode_spaces"
            ) as mock_cleanup,
            patch(
                "slack_chat_migrator.utils.api.get_gcp_service",
                return_value=MagicMock(),
            ),
            patch(
                "slack_chat_migrator.core.config.load_config",
                return_value=MigrationConfig(),
            ),
        ):
            result = runner.invoke(
                cli,
                [
                    "migrate",
                    "--export_path",
                    "fake",
                    "--creds_path",
                    "fake.json",
                    "--workspace_admin",
                    "a@b.com",
                    "--complete",
                ],
            )
            assert result.exit_code == 0
            mock_cleanup.assert_called_once()

    def test_complete_requires_creds(self):
        """--complete requires --creds_path."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "migrate",
                "--export_path",
                "fake",
                "--complete",
            ],
        )
        assert result.exit_code != 0


class TestCleanupDeprecation:
    """Tests for cleanup command deprecation."""

    @patch("slack_chat_migrator.cli.cleanup_cmd.cleanup_import_mode_spaces")
    @patch("slack_chat_migrator.cli.cleanup_cmd.get_gcp_service")
    @patch("slack_chat_migrator.cli.cleanup_cmd.load_config")
    def test_cleanup_emits_deprecation(self, mock_config, mock_svc, mock_cleanup):
        """cleanup command emits deprecation warning pointing to migrate --complete."""
        mock_config.return_value = MigrationConfig()
        mock_svc.return_value = MagicMock()

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
        assert "deprecated" in result.output.lower()
        assert "migrate --complete" in result.output


class TestInterruptHandler:
    """Tests for the InterruptHandler context manager."""

    def test_no_effect_on_normal_exit(self):
        """InterruptHandler does nothing when no exception occurs."""
        from slack_chat_migrator.cli.common import InterruptHandler

        with InterruptHandler(export_path="fake"):
            pass  # should not raise

    def test_non_keyboard_interrupt_propagates(self):
        """Non-KeyboardInterrupt exceptions propagate unchanged."""
        import pytest

        from slack_chat_migrator.cli.common import InterruptHandler

        with pytest.raises(ValueError, match="boom"):
            with InterruptHandler(export_path="fake"):
                raise ValueError("boom")

    def test_keyboard_interrupt_propagates(self):
        """KeyboardInterrupt propagates after printing the summary."""
        import pytest

        from slack_chat_migrator.cli.common import InterruptHandler

        with pytest.raises(KeyboardInterrupt):
            with InterruptHandler(export_path="fake"):
                raise KeyboardInterrupt()


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

        runner = CliRunner()
        result = runner.invoke(cmd, ["--update_mode"])
        assert result.exit_code == 0
        assert "deprecated" in result.output.lower()
        assert "--resume" in result.output
        assert "resume=True" in result.output

    def test_deprecated_flag_passes_value(self):
        """Deprecated flag value is forwarded to the canonical parameter."""

        @click.command()
        @click.option("--output", default="default.yaml")
        @deprecated_option("--old_output", "--output", default=None)
        def cmd(output: str) -> None:
            click.echo(f"output={output}")

        runner = CliRunner()
        result = runner.invoke(cmd, ["--old_output", "custom.yaml"])
        assert result.exit_code == 0
        assert "output=custom.yaml" in result.output
        assert "deprecated" in result.output.lower()

    def test_no_warning_when_new_flag_used(self):
        """No warning when the canonical flag is used."""

        @click.command()
        @click.option("--resume", is_flag=True, default=False)
        @deprecated_option("--update_mode", "--resume", is_flag=True, default=False)
        def cmd(resume: bool) -> None:
            click.echo(f"resume={resume}")

        runner = CliRunner()
        result = runner.invoke(cmd, ["--resume"])
        assert result.exit_code == 0
        assert "deprecated" not in result.output.lower()
        assert "resume=True" in result.output

    def test_no_warning_when_flag_not_used(self):
        """No warning when neither flag is used."""

        @click.command()
        @click.option("--resume", is_flag=True, default=False)
        @deprecated_option("--update_mode", "--resume", is_flag=True, default=False)
        def cmd(resume: bool) -> None:
            click.echo(f"resume={resume}")

        runner = CliRunner()
        result = runner.invoke(cmd, [])
        assert result.exit_code == 0
        assert "deprecated" not in result.output.lower()
        assert "resume=False" in result.output

    def test_new_flag_wins_when_both_provided(self):
        """When both old and new flags are given, the new flag takes priority."""

        @click.command()
        @click.option("--resume", is_flag=True, default=False)
        @deprecated_option("--update_mode", "--resume", is_flag=True, default=False)
        def cmd(resume: bool) -> None:
            click.echo(f"resume={resume}")

        runner = CliRunner()
        result = runner.invoke(cmd, ["--update_mode", "--resume"])
        assert result.exit_code == 0
        assert "deprecated" in result.output.lower()
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

        runner = CliRunner()
        result = runner.invoke(grp, ["old-cmd"])
        assert result.exit_code == 0
        assert "deprecated" in result.output.lower()
        assert "new-cmd" in result.output
        assert "ran old command" in result.output


class TestQuietConsole:
    """Tests for _quiet_console context manager."""

    def test_suppresses_console_handler(self):
        """_quiet_console raises console handler level to ERROR."""
        from slack_chat_migrator.cli.migrate_cmd import _quiet_console

        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        test_logger = logging.getLogger("test_quiet_console")
        test_logger.addHandler(handler)

        # Temporarily add to root logger to be detected
        root = logging.getLogger()
        root.addHandler(handler)
        try:
            with _quiet_console():
                assert handler.level == logging.ERROR
            assert handler.level == logging.DEBUG
        finally:
            root.removeHandler(handler)
            test_logger.removeHandler(handler)

    def test_restores_level_on_exception(self):
        """_quiet_console restores level even if body raises."""
        from slack_chat_migrator.cli.migrate_cmd import _quiet_console

        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        root = logging.getLogger()
        root.addHandler(handler)
        try:
            with pytest.raises(RuntimeError):
                with _quiet_console():
                    raise RuntimeError("boom")
            assert handler.level == logging.INFO
        finally:
            root.removeHandler(handler)

    def test_noop_when_no_console_handler(self):
        """_quiet_console is a no-op when there are no console handlers."""
        from slack_chat_migrator.cli.migrate_cmd import _quiet_console

        # Just ensure it doesn't error
        with _quiet_console():
            pass


class TestPrintConfigPanel:
    """Tests for _print_config_panel."""

    @patch("sys.stdout")
    def test_prints_panel_on_tty(self, mock_stdout):
        """_print_config_panel prints Rich panel when TTY."""
        mock_stdout.isatty.return_value = True
        from types import SimpleNamespace

        from slack_chat_migrator.cli.migrate_cmd import _print_config_panel

        args = SimpleNamespace(
            export_path="/tmp/export",
            workspace_admin="admin@example.com",
            config="config.yaml",
            dry_run=True,
            update_mode=False,
            verbose=False,
            debug_api=False,
        )
        # Should not raise
        _print_config_panel(args, "/tmp/logs")

    @patch("sys.stdout")
    def test_no_panel_on_non_tty(self, mock_stdout):
        """_print_config_panel skips panel when not TTY."""
        mock_stdout.isatty.return_value = False
        from types import SimpleNamespace

        from slack_chat_migrator.cli.migrate_cmd import _print_config_panel

        args = SimpleNamespace(
            export_path="/tmp/export",
            workspace_admin="admin@example.com",
            config="config.yaml",
            dry_run=False,
            update_mode=False,
            verbose=False,
            debug_api=False,
        )
        # Should not raise (just logs)
        _print_config_panel(args, "/tmp/logs")
