"""Tests for UTF-8 encoding in file I/O."""

from __future__ import annotations

import json
from pathlib import Path

from slack_chat_migrator.services.export_inspector import ExportInspector


class TestUtf8Encoding:
    """Verify that files with non-ASCII content are read correctly."""

    def test_export_inspector_reads_utf8_messages(self, tmp_path: Path) -> None:
        """ExportInspector handles UTF-8 characters in message files."""
        (tmp_path / "users.json").write_text("[]", encoding="utf-8")
        (tmp_path / "channels.json").write_text("[]", encoding="utf-8")

        ch_dir = tmp_path / "general"
        ch_dir.mkdir()
        # U+2019 RIGHT SINGLE QUOTATION MARK — encoded as 0xE2 0x80 0x99 in UTF-8
        # Byte 0x9D in Windows-1252 maps to this, causing charmap codec errors
        msg = [{"type": "message", "ts": "1.0", "text": "it\u2019s a test"}]
        (ch_dir / "2024-01-01.json").write_text(json.dumps(msg), encoding="utf-8")

        inspector = ExportInspector(tmp_path)
        counts = inspector.get_message_counts()
        assert counts["general"] == 1

    def test_export_inspector_reads_utf8_users(self, tmp_path: Path) -> None:
        """ExportInspector handles UTF-8 characters in users.json."""
        users = [
            {
                "id": "U1",
                "name": "Jos\u00e9",
                "profile": {"email": "jose@example.com"},
            }
        ]
        (tmp_path / "users.json").write_text(json.dumps(users), encoding="utf-8")
        (tmp_path / "channels.json").write_text("[]", encoding="utf-8")

        inspector = ExportInspector(tmp_path)
        assert inspector.get_user_count() == 1
