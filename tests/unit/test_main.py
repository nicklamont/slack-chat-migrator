"""Unit tests for the __main__ module."""

from unittest.mock import patch

from slack_migrator import __main__


class TestMainModule:
    """Tests for __main__.py entry point."""

    def test_main_function_is_imported_from_cli(self):
        """Verify main is imported from slack_migrator.cli.commands."""
        from slack_migrator.cli.commands import main

        assert __main__.main is main

    @patch("slack_migrator.cli.commands.main")
    def test_runpy_invokes_main(self, mock_main):
        """Verify running the package via runpy triggers main()."""
        import runpy

        # run_module re-executes __main__.py with __name__ == '__main__'
        runpy.run_module("slack_migrator", run_name="__main__", alter_sys=False)
        mock_main.assert_called_once()

    @patch("slack_migrator.__main__.main")
    def test_main_not_called_on_import(self, mock_main):
        """Verify importing the module does not call main()."""
        # After import, main should not have been called because __name__
        # is 'slack_migrator.__main__', not '__main__'.
        mock_main.assert_not_called()

    @patch("slack_migrator.cli.commands.cli")
    def test_main_delegates_to_cli(self, mock_cli):
        """Verify main() delegates to the click cli group."""
        from slack_migrator.cli.commands import main

        main()
        mock_cli.assert_called_once()
