"""Checkpoint persistence for resumable migrations."""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path

from slack_migrator.utils.logging import log_with_context

CHECKPOINT_SCHEMA_VERSION = 1


@dataclass
class CheckpointData:
    """Serializable snapshot of migration progress."""

    schema_version: int = CHECKPOINT_SCHEMA_VERSION
    completed_channels: dict[str, str] = field(
        default_factory=dict
    )  # channel_name -> ISO 8601 completion timestamp
    started_at: str | None = None
    last_updated: str | None = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_checkpoint(path: Path) -> CheckpointData | None:
    """Load a checkpoint from disk, returning None if absent or corrupt."""
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text())
        if not isinstance(raw, dict):
            log_with_context(
                logging.WARNING,
                f"Checkpoint file {path} has invalid format, ignoring",
            )
            return None
        version = raw.get("schema_version", 0)
        if version != CHECKPOINT_SCHEMA_VERSION:
            log_with_context(
                logging.WARNING,
                f"Checkpoint schema version {version} != {CHECKPOINT_SCHEMA_VERSION}, ignoring",
            )
            return None
        return CheckpointData(
            schema_version=raw.get("schema_version", CHECKPOINT_SCHEMA_VERSION),
            completed_channels=raw.get("completed_channels", {}),
            started_at=raw.get("started_at"),
            last_updated=raw.get("last_updated"),
        )
    except (json.JSONDecodeError, OSError) as e:
        log_with_context(logging.WARNING, f"Failed to read checkpoint {path}: {e}")
        return None


def save_checkpoint(path: Path, data: CheckpointData) -> None:
    """Atomically save checkpoint to disk (write .tmp + rename).

    Uses ``dataclasses.replace()`` to avoid mutating the caller's *data*.
    """
    save_data = replace(data, last_updated=now_iso())
    tmp = path.with_suffix(".tmp")
    try:
        tmp.write_text(json.dumps(asdict(save_data), indent=2) + "\n")
        tmp.replace(path)
    except OSError as e:
        log_with_context(logging.ERROR, f"Failed to write checkpoint {path}: {e}")


def clear_checkpoint(path: Path) -> None:
    """Remove the checkpoint file after successful migration."""
    try:
        path.unlink(missing_ok=True)
    except OSError as e:
        log_with_context(logging.WARNING, f"Failed to remove checkpoint {path}: {e}")
