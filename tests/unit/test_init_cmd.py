"""Tests for the init command."""

from __future__ import annotations

import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from slack_chat_migrator.cli.commands import cli


def _make_export(tmp_path: Path) -> Path:
    """Create a minimal valid export structure."""
    (tmp_path / "users.json").write_text(
        json.dumps(
            [
                {
                    "id": "U1",
                    "name": "alice",
                    "profile": {"email": "alice@example.com"},
                },
                {
                    "id": "U2",
                    "name": "bob",
                    "profile": {},
                },
                {
                    "id": "U3",
                    "name": "mybot",
                    "is_bot": True,
                    "profile": {"email": "bot@example.com"},
                },
            ]
        )
    )
    (tmp_path / "channels.json").write_text(
        json.dumps(
            [
                {"id": "C1", "name": "general"},
                {"id": "C2", "name": "random"},
            ]
        )
    )

    general = tmp_path / "general"
    general.mkdir()
    (general / "2024-01-15.json").write_text(
        json.dumps([{"type": "message", "ts": "1.0", "text": "hello"}])
    )

    random_ch = tmp_path / "random"
    random_ch.mkdir()
    (random_ch / "2024-02-20.json").write_text(
        json.dumps([{"type": "message", "ts": "2.0", "text": "world"}])
    )

    return tmp_path


class TestInitCommand:
    """Tests for the init subcommand."""

    def test_generates_default_config(self, tmp_path: Path) -> None:
        """init produces a valid config.yaml with default answers."""
        export = _make_export(tmp_path)
        output = tmp_path / "config.yaml"

        runner = CliRunner()
        # Provide interactive answers:
        # channel mode: all (default), ignore bots: y,
        # add user mappings: n, abort on error: n (default),
        # max failure %: 10 (default), strategy: skip_on_error (default),
        # drive name: default
        result = runner.invoke(
            cli,
            ["init", "--export_path", str(export), "--output", str(output)],
            input="\n\ny\nn\n\n\n\n\n",
        )
        assert result.exit_code == 0, result.output
        assert output.exists()

        config = yaml.safe_load(output.read_text())
        assert config["ignore_bots"] is True
        assert config["shared_drive"]["name"] == "Imported Slack Attachments"

    def test_exclude_channels(self, tmp_path: Path) -> None:
        """init writes exclude_channels when user selects exclude mode."""
        export = _make_export(tmp_path)
        output = tmp_path / "config.yaml"

        runner = CliRunner()
        # channel mode: exclude, channels: random,
        # ignore bots: y, add mappings: n,
        # abort: n, max%: 10, strategy: skip_on_error, drive: default
        result = runner.invoke(
            cli,
            ["init", "--export_path", str(export), "--output", str(output)],
            input="exclude\nrandom\ny\nn\n\n\n\n\n",
        )
        assert result.exit_code == 0, result.output

        config = yaml.safe_load(output.read_text())
        assert config["exclude_channels"] == ["random"]

    def test_include_channels(self, tmp_path: Path) -> None:
        """init writes include_channels when user selects include mode."""
        export = _make_export(tmp_path)
        output = tmp_path / "config.yaml"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["init", "--export_path", str(export), "--output", str(output)],
            input="include\ngeneral\ny\nn\n\n\n\n\n",
        )
        assert result.exit_code == 0, result.output

        config = yaml.safe_load(output.read_text())
        assert config["include_channels"] == ["general"]

    def test_user_mapping_overrides(self, tmp_path: Path) -> None:
        """init records user mapping overrides when provided."""
        export = _make_export(tmp_path)
        output = tmp_path / "config.yaml"

        runner = CliRunner()
        # channel: all, ignore bots: y, add mappings: y,
        # bob email: bob@example.com, abort: n, max%: 10,
        # strategy: skip_on_error, drive: default
        result = runner.invoke(
            cli,
            ["init", "--export_path", str(export), "--output", str(output)],
            input="\n\ny\nbob@example.com\n\n\n\n\n",
        )
        assert result.exit_code == 0, result.output

        config = yaml.safe_load(output.read_text())
        assert config["user_mapping_overrides"]["U2"] == "bob@example.com"

    def test_refuses_overwrite_without_confirm(self, tmp_path: Path) -> None:
        """init aborts if output file exists and user declines overwrite."""
        export = _make_export(tmp_path)
        output = tmp_path / "config.yaml"
        output.write_text("existing: true")

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["init", "--export_path", str(export), "--output", str(output)],
            input="n\n",
        )
        assert result.exit_code == 0
        assert "Aborted" in result.output
        # File should be unchanged
        assert yaml.safe_load(output.read_text()) == {"existing": True}

    def test_shows_export_summary(self, tmp_path: Path) -> None:
        """init prints export summary statistics."""
        export = _make_export(tmp_path)
        output = tmp_path / "out.yaml"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["init", "--export_path", str(export), "--output", str(output)],
            input="\n\ny\nn\n\n\n\n\n",
        )
        # Rich table renders metrics with spacing, not "Key: Value" format
        assert "Channels" in result.output
        assert "Users" in result.output
        assert "Messages" in result.output
        assert "Export Summary" in result.output

    def test_shows_structure_issues(self, tmp_path: Path) -> None:
        """init reports structure issues and lets user abort."""
        # Export with no channel dirs
        (tmp_path / "users.json").write_text("[]")
        (tmp_path / "channels.json").write_text("[]")
        output = tmp_path / "config.yaml"

        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["init", "--export_path", str(tmp_path), "--output", str(output)],
            input="n\n",  # don't continue
        )
        assert result.exit_code == 1
        assert "No channel" in result.output

    def test_abort_on_error_config(self, tmp_path: Path) -> None:
        """init sets abort_on_error when user selects it."""
        export = _make_export(tmp_path)
        output = tmp_path / "config.yaml"

        runner = CliRunner()
        # channel: all, ignore bots: y, add mappings: n,
        # abort on error: y, strategy: skip_on_error, drive: default
        result = runner.invoke(
            cli,
            ["init", "--export_path", str(export), "--output", str(output)],
            input="\n\ny\nn\ny\n\n\n",
        )
        assert result.exit_code == 0, result.output

        config = yaml.safe_load(output.read_text())
        assert config["abort_on_error"] is True

    def test_nonexistent_export_path_exits_immediately(self, tmp_path: Path) -> None:
        """init exits with error if export path does not exist."""
        output = tmp_path / "config.yaml"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["init", "--export_path", "/nonexistent/path", "--output", str(output)],
        )
        assert result.exit_code == 1
        assert "does not exist" in result.output

    def test_file_as_export_path_exits_immediately(self, tmp_path: Path) -> None:
        """init exits with error if export path is a file, not a directory."""
        some_file = tmp_path / "not_a_dir.txt"
        some_file.write_text("hello")
        output = tmp_path / "config.yaml"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            ["init", "--export_path", str(some_file), "--output", str(output)],
        )
        assert result.exit_code == 1
        assert "not a directory" in result.output

    def test_registered_in_cli(self) -> None:
        """init command is registered on the CLI group."""
        assert "init" in cli.commands
