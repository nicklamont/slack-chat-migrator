"""Tests for the ExportInspector."""

from __future__ import annotations

import json
from pathlib import Path

from slack_chat_migrator.services.export_inspector import ExportInspector


def _write_json(path: Path, data: object) -> None:
    path.write_text(json.dumps(data))


def _make_export(tmp_path: Path) -> Path:
    """Create a minimal valid export structure."""
    _write_json(
        tmp_path / "users.json",
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
        ],
    )
    _write_json(
        tmp_path / "channels.json",
        [
            {"id": "C1", "name": "general"},
            {"id": "C2", "name": "random"},
        ],
    )

    # Channel: general — 2 messages
    general = tmp_path / "general"
    general.mkdir()
    _write_json(
        general / "2024-01-15.json",
        [
            {"type": "message", "ts": "1.0", "text": "hello"},
            {"type": "message", "ts": "2.0", "text": "world"},
        ],
    )

    # Channel: random — 1 message with a file
    random_ch = tmp_path / "random"
    random_ch.mkdir()
    _write_json(
        random_ch / "2024-02-20.json",
        [
            {
                "type": "message",
                "ts": "3.0",
                "text": "file here",
                "files": [{"id": "F1", "name": "doc.pdf"}],
            },
        ],
    )

    return tmp_path


class TestExportInspector:
    """Tests for ExportInspector accessors."""

    def test_get_channel_count(self, tmp_path: Path) -> None:
        export = _make_export(tmp_path)
        inspector = ExportInspector(export)
        assert inspector.get_channel_count() == 2

    def test_get_user_count(self, tmp_path: Path) -> None:
        export = _make_export(tmp_path)
        inspector = ExportInspector(export)
        assert inspector.get_user_count() == 3

    def test_get_message_counts(self, tmp_path: Path) -> None:
        export = _make_export(tmp_path)
        inspector = ExportInspector(export)
        counts = inspector.get_message_counts()
        assert counts["general"] == 2
        assert counts["random"] == 1

    def test_get_total_message_count(self, tmp_path: Path) -> None:
        export = _make_export(tmp_path)
        inspector = ExportInspector(export)
        assert inspector.get_total_message_count() == 3

    def test_get_total_file_count(self, tmp_path: Path) -> None:
        export = _make_export(tmp_path)
        inspector = ExportInspector(export)
        assert inspector.get_total_file_count() == 1

    def test_get_export_date_range(self, tmp_path: Path) -> None:
        export = _make_export(tmp_path)
        inspector = ExportInspector(export)
        result = inspector.get_export_date_range()
        assert result == ("2024-01-15", "2024-02-20")

    def test_get_export_date_range_empty(self, tmp_path: Path) -> None:
        tmp_path.mkdir(exist_ok=True)
        inspector = ExportInspector(tmp_path)
        assert inspector.get_export_date_range() is None

    def test_get_users_without_email(self, tmp_path: Path) -> None:
        export = _make_export(tmp_path)
        inspector = ExportInspector(export)
        no_email = inspector.get_users_without_email()
        assert len(no_email) == 1
        assert no_email[0]["name"] == "bob"

    def test_get_bot_users(self, tmp_path: Path) -> None:
        export = _make_export(tmp_path)
        inspector = ExportInspector(export)
        bots = inspector.get_bot_users()
        assert len(bots) == 1
        assert bots[0]["name"] == "mybot"


class TestStructureIssues:
    """Tests for ExportInspector.get_structure_issues()."""

    def test_valid_export_no_issues(self, tmp_path: Path) -> None:
        export = _make_export(tmp_path)
        inspector = ExportInspector(export)
        assert inspector.get_structure_issues() == []

    def test_missing_users_json(self, tmp_path: Path) -> None:
        _write_json(tmp_path / "channels.json", [])
        ch = tmp_path / "general"
        ch.mkdir()
        _write_json(ch / "2024-01-01.json", [])

        inspector = ExportInspector(tmp_path)
        issues = inspector.get_structure_issues()
        assert any("users.json" in i for i in issues)

    def test_missing_channels_json(self, tmp_path: Path) -> None:
        _write_json(tmp_path / "users.json", [])
        ch = tmp_path / "general"
        ch.mkdir()
        _write_json(ch / "2024-01-01.json", [])

        inspector = ExportInspector(tmp_path)
        issues = inspector.get_structure_issues()
        assert any("channels.json" in i for i in issues)

    def test_no_channel_dirs(self, tmp_path: Path) -> None:
        _write_json(tmp_path / "users.json", [])
        _write_json(tmp_path / "channels.json", [])

        inspector = ExportInspector(tmp_path)
        issues = inspector.get_structure_issues()
        assert any("No channel" in i for i in issues)

    def test_channel_without_json(self, tmp_path: Path) -> None:
        _write_json(tmp_path / "users.json", [])
        _write_json(tmp_path / "channels.json", [])
        (tmp_path / "empty_channel").mkdir()

        inspector = ExportInspector(tmp_path)
        issues = inspector.get_structure_issues()
        assert any("empty_channel" in i for i in issues)

    def test_not_a_directory(self, tmp_path: Path) -> None:
        fake = tmp_path / "not_a_dir"
        fake.write_text("nope")

        inspector = ExportInspector(fake)
        issues = inspector.get_structure_issues()
        assert any("not a directory" in i for i in issues)

    def test_bad_json_in_channel_handled(self, tmp_path: Path) -> None:
        """Corrupted JSON files don't crash get_message_counts()."""
        _write_json(tmp_path / "users.json", [])
        _write_json(tmp_path / "channels.json", [])
        ch = tmp_path / "broken"
        ch.mkdir()
        (ch / "2024-01-01.json").write_text("{bad json")

        inspector = ExportInspector(tmp_path)
        counts = inspector.get_message_counts()
        assert counts["broken"] == 0
