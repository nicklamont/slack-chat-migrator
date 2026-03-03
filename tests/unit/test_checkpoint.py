"""Unit tests for the checkpoint persistence module."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

from slack_migrator.core.checkpoint import (
    CHECKPOINT_SCHEMA_VERSION,
    CheckpointData,
    clear_checkpoint,
    load_checkpoint,
    save_checkpoint,
)


class TestLoadCheckpoint:
    """Tests for load_checkpoint()."""

    def test_returns_none_when_file_missing(self, tmp_path: Path) -> None:
        result = load_checkpoint(tmp_path / "nonexistent.json")
        assert result is None

    def test_loads_valid_checkpoint(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        data = {
            "schema_version": CHECKPOINT_SCHEMA_VERSION,
            "completed_channels": {"general": "1700000000.0", "random": "1700001000.0"},
            "started_at": "2025-01-01T00:00:00+00:00",
            "last_updated": "2025-01-01T01:00:00+00:00",
        }
        cp_path.write_text(json.dumps(data))

        result = load_checkpoint(cp_path)

        assert result is not None
        assert result.schema_version == CHECKPOINT_SCHEMA_VERSION
        assert result.completed_channels == {
            "general": "1700000000.0",
            "random": "1700001000.0",
        }
        assert result.started_at == "2025-01-01T00:00:00+00:00"
        assert result.last_updated == "2025-01-01T01:00:00+00:00"

    def test_returns_none_on_corrupt_json(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        cp_path.write_text("{invalid json content!!")

        result = load_checkpoint(cp_path)
        assert result is None

    def test_returns_none_on_wrong_schema_version(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        data = {
            "schema_version": 99,
            "completed_channels": {},
        }
        cp_path.write_text(json.dumps(data))

        result = load_checkpoint(cp_path)
        assert result is None

    def test_returns_none_on_non_dict_json(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        cp_path.write_text("[1, 2, 3]")

        result = load_checkpoint(cp_path)
        assert result is None

    def test_handles_os_error(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        # Create the file so path.exists() returns True
        cp_path.write_text("{}")

        with patch.object(Path, "read_text", side_effect=OSError("disk error")):
            result = load_checkpoint(cp_path)

        assert result is None


class TestSaveCheckpoint:
    """Tests for save_checkpoint()."""

    def test_creates_file(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        data = CheckpointData(
            completed_channels={"general": "1700000000.0"},
            started_at="2025-01-01T00:00:00+00:00",
        )

        save_checkpoint(cp_path, data)

        assert cp_path.exists()
        loaded = json.loads(cp_path.read_text())
        assert loaded["completed_channels"] == {"general": "1700000000.0"}
        assert loaded["started_at"] == "2025-01-01T00:00:00+00:00"
        assert loaded["schema_version"] == CHECKPOINT_SCHEMA_VERSION

    def test_sets_last_updated(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        data = CheckpointData()

        save_checkpoint(cp_path, data)

        assert data.last_updated is not None
        loaded = json.loads(cp_path.read_text())
        assert loaded["last_updated"] is not None

    def test_atomic_write(self, tmp_path: Path) -> None:
        """Verify the .tmp file is renamed to the final path (no leftover .tmp)."""
        cp_path = tmp_path / "checkpoint.json"
        tmp_file = cp_path.with_suffix(".tmp")
        data = CheckpointData(completed_channels={"general": "123"})

        save_checkpoint(cp_path, data)

        assert cp_path.exists()
        assert not tmp_file.exists()

    def test_handles_os_error(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        data = CheckpointData()

        with patch.object(Path, "write_text", side_effect=OSError("disk full")):
            # Should not raise
            save_checkpoint(cp_path, data)


class TestClearCheckpoint:
    """Tests for clear_checkpoint()."""

    def test_removes_file(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        cp_path.write_text("{}")

        clear_checkpoint(cp_path)

        assert not cp_path.exists()

    def test_noop_on_missing_file(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "nonexistent.json"

        # Should not raise
        clear_checkpoint(cp_path)

    def test_handles_os_error(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        cp_path.write_text("{}")

        with patch.object(Path, "unlink", side_effect=OSError("permission denied")):
            # Should not raise
            clear_checkpoint(cp_path)


class TestRoundTrip:
    """Tests for save -> load round-trip consistency."""

    def test_save_then_load(self, tmp_path: Path) -> None:
        cp_path = tmp_path / "checkpoint.json"
        original = CheckpointData(
            completed_channels={
                "general": "1700000000.0",
                "random": "1700001000.0",
                "engineering": "1700002000.0",
            },
            started_at="2025-01-01T00:00:00+00:00",
        )

        save_checkpoint(cp_path, original)
        loaded = load_checkpoint(cp_path)

        assert loaded is not None
        assert loaded.completed_channels == original.completed_channels
        assert loaded.started_at == original.started_at
        assert loaded.schema_version == original.schema_version
        # last_updated is set by save_checkpoint
        assert loaded.last_updated == original.last_updated

    def test_channel_skipping(self, tmp_path: Path) -> None:
        """Verify channels A and B are in completed_channels after save/load."""
        cp_path = tmp_path / "checkpoint.json"
        data = CheckpointData(
            completed_channels={"channel_a": "100.0", "channel_b": "200.0"},
        )

        save_checkpoint(cp_path, data)
        loaded = load_checkpoint(cp_path)

        assert loaded is not None
        assert "channel_a" in loaded.completed_channels
        assert "channel_b" in loaded.completed_channels
        assert loaded.completed_channels["channel_a"] == "100.0"
        assert loaded.completed_channels["channel_b"] == "200.0"
