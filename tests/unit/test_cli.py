"""Tests for the click-based CLI."""

from unittest.mock import MagicMock, patch

from click.testing import CliRunner

from slack_migrator.cli.commands import cli


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
    def test_invokes_standalone_function(self, mock_check):
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
            config_path="config.yaml",
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
        runner.invoke(
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
    @patch("slack_migrator.utils.api.get_gcp_service")
    @patch("slack_migrator.core.config.load_config")
    def test_invokes_standalone_cleanup(self, mock_config, mock_svc, mock_cleanup):
        mock_config.return_value = {}
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
            ],
        )
        assert result.exit_code == 0
        mock_cleanup.assert_called_once_with(mock_chat)


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
