"""Temporary storage for attachment downloads.

Each file handler streams Slack downloads into its own directory, under
``<output_dir>/.attachment_tmp/`` when the run's output directory is known
and under the system temp dir otherwise.  The directory holds a lock file
that stays locked for the life of the process.  The operating system drops
that lock when the process dies, even on SIGKILL or an out-of-memory kill,
so the next run can tell abandoned directories from ones still in use and
delete them.
"""

from __future__ import annotations

import atexit
import logging
import os
import re
import shutil
import sys
import tempfile
import uuid
from collections.abc import Callable
from pathlib import Path

from slack_chat_migrator.constants import (
    ATTACHMENT_TEMP_DIRNAME,
    ATTACHMENT_TEMP_FALLBACK_DIRNAME,
)
from slack_chat_migrator.utils.logging import log_with_context

logger = logging.getLogger("slack_chat_migrator")

_LOCK_FILENAME = ".lock"


def safe_temp_suffix(name: str) -> str:
    """Sanitize a filename for use as a temp file suffix.

    Args:
        name: Original file name.

    Returns:
        A suffix of at most 65 characters made of ``[A-Za-z0-9._-]``.
    """
    return "_" + re.sub(r"[^A-Za-z0-9._-]", "_", name)[:64]


def _try_lock(fd: int) -> bool:
    """Take an exclusive, non-blocking OS lock on *fd*.

    Returns:
        True if the lock was acquired, False if another process holds it.
    """
    try:
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
        else:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        return False
    return True


def _fallback_root() -> Path:
    """Return the system-temp parent directory used when no output dir is known."""
    return Path(tempfile.gettempdir(), ATTACHMENT_TEMP_FALLBACK_DIRNAME)


def _base_dir(output_dir: str | None) -> Path:
    """Return the parent of per-handler temp dirs for *output_dir*."""
    if output_dir:
        return Path(output_dir).resolve().joinpath(ATTACHMENT_TEMP_DIRNAME)
    return _fallback_root()


def _candidate_dirs(output_dir: str | None) -> list[Path]:
    """List temp directories that earlier runs may have left behind.

    Args:
        output_dir: The current run's output directory, if known.  The
            directories of earlier runs next to it are searched too.

    Returns:
        Existing per-handler temp directories, live or abandoned.
    """
    bases = [_fallback_root()]
    if output_dir:
        parent = Path(output_dir).resolve().parent
        if parent.is_dir():
            bases.extend(
                run_dir / ATTACHMENT_TEMP_DIRNAME for run_dir in parent.iterdir()
            )
    candidates: list[Path] = []
    for base in bases:
        if base.is_dir():
            candidates.extend(p for p in base.iterdir() if p.is_dir())
    return candidates


def sweep_stale_temp_dirs(output_dir: str | None = None) -> int:
    """Delete attachment temp directories abandoned by runs that were killed.

    A directory is removed only when its lock file can be locked, which
    means no live process owns it.  Directories without a lock file are left
    alone.

    Args:
        output_dir: The current run's output directory, if known.

    Returns:
        The number of directories removed.
    """
    removed = 0
    try:
        candidates = _candidate_dirs(output_dir)
    except OSError:
        logger.debug("Could not list attachment temp dirs", exc_info=True)
        return 0

    for directory in candidates:
        lock_path = directory / _LOCK_FILENAME
        if not lock_path.exists():
            continue
        try:
            fd = os.open(str(lock_path), os.O_RDWR)
        except OSError:
            continue
        try:
            if not _try_lock(fd):
                continue  # still in use by a live run
        finally:
            os.close(fd)
        shutil.rmtree(directory, ignore_errors=True)
        if not directory.exists():
            removed += 1
        try:
            directory.parent.rmdir()  # only succeeds once the base dir is empty
        except OSError:
            pass

    if removed:
        log_with_context(
            logging.INFO,
            f"Removed {removed} leftover attachment temp director"
            f"{'y' if removed == 1 else 'ies'} from an earlier run",
        )
    return removed


class AttachmentTempDir:
    """The current run's directory for attachment downloads.

    The directory is created on first use and removed by :meth:`cleanup`,
    which also runs at interpreter exit.
    """

    def __init__(self, output_dir_getter: Callable[[], str | None]) -> None:
        """Initialize without touching the filesystem.

        Args:
            output_dir_getter: Returns the run's output directory, or None.
                It is read when the directory is first needed, because the
                output directory may be set after construction.
        """
        self._output_dir_getter = output_dir_getter
        self._path: Path | None = None
        self._lock_fd: int | None = None

    def path(self) -> str:
        """Return the directory, creating and locking it on first use.

        Returns:
            Absolute path of the directory.
        """
        if self._path is not None and self._path.is_dir():
            return str(self._path)

        base = _base_dir(self._output_dir_getter())
        directory = base / f"run-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)

        self._release_lock()
        fd = os.open(str(directory / _LOCK_FILENAME), os.O_RDWR | os.O_CREAT, 0o600)
        _try_lock(fd)  # a fresh, uniquely named directory: nobody else holds it
        self._lock_fd = fd
        if self._path is None:
            atexit.register(self.cleanup)
        self._path = directory
        return str(directory)

    def _release_lock(self) -> None:
        """Close the lock file, which releases the OS lock."""
        if self._lock_fd is not None:
            try:
                os.close(self._lock_fd)
            except OSError:
                pass
            self._lock_fd = None

    def cleanup(self) -> None:
        """Delete the directory and everything left in it."""
        self._release_lock()
        if self._path is not None:
            shutil.rmtree(self._path, ignore_errors=True)
            try:
                self._path.parent.rmdir()  # only succeeds once it is empty
            except OSError:
                pass


def remove_temp_file(path: str | None) -> None:
    """Delete a temp file, ignoring a file that is already gone.

    Args:
        path: File to delete, or None.
    """
    if not path:
        return
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
    except OSError:
        logger.debug("Failed to clean up temp file %s", path, exc_info=True)
