"""Unit tests for attachment temp-file storage and the start-up sweep."""

from __future__ import annotations

import os
import signal
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from slack_chat_migrator.constants import ATTACHMENT_TEMP_DIRNAME
from slack_chat_migrator.services.files import temp_files
from slack_chat_migrator.services.files.temp_files import (
    AttachmentTempDir,
    remove_temp_file,
    sweep_stale_temp_dirs,
)

SRC_DIR = Path(temp_files.__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def isolated_fallback(tmp_path, monkeypatch):
    """Point the system-temp fallback at a private directory."""
    fallback = tmp_path / "systmp"
    monkeypatch.setattr(temp_files, "_fallback_root", lambda: fallback)
    return fallback


def _abandoned_dir(base: Path) -> Path:
    """Create a temp dir with an unlocked lock file, as a killed run leaves it."""
    directory = base / "run-1-dead"
    directory.mkdir(parents=True)
    (directory / ".lock").write_bytes(b"")
    (directory / "dl_big.mov").write_bytes(b"x" * 100)
    return directory


class TestAttachmentTempDir:
    def test_created_lazily_under_output_dir(self, tmp_path):
        output_dir = tmp_path / "migration_logs" / "run_1"
        output_dir.mkdir(parents=True)
        temp_dir = AttachmentTempDir(lambda: str(output_dir))
        assert not (output_dir / ATTACHMENT_TEMP_DIRNAME).exists()

        path = Path(temp_dir.path())

        assert path.parent == (output_dir / ATTACHMENT_TEMP_DIRNAME).resolve()
        assert (path / ".lock").exists()
        assert temp_dir.path() == str(path)  # stable while it exists

    def test_falls_back_to_system_temp(self, isolated_fallback):
        path = Path(AttachmentTempDir(lambda: None).path())
        assert path.parent == isolated_fallback

    def test_output_dir_read_at_first_use(self, tmp_path):
        holder: dict[str, str | None] = {"dir": None}
        temp_dir = AttachmentTempDir(lambda: holder["dir"])
        holder["dir"] = str(tmp_path)
        assert Path(temp_dir.path()).parent.name == ATTACHMENT_TEMP_DIRNAME

    def test_cleanup_removes_directory(self, tmp_path):
        temp_dir = AttachmentTempDir(lambda: str(tmp_path))
        path = Path(temp_dir.path())
        (path / "dl_file").write_bytes(b"data")

        temp_dir.cleanup()

        assert not path.exists()
        assert not (tmp_path / ATTACHMENT_TEMP_DIRNAME).exists()

    def test_recreated_after_cleanup(self, tmp_path):
        temp_dir = AttachmentTempDir(lambda: str(tmp_path))
        temp_dir.path()
        temp_dir.cleanup()
        assert Path(temp_dir.path()).is_dir()
        temp_dir.cleanup()


class TestSweep:
    def test_removes_abandoned_dir_of_earlier_run(self, tmp_path):
        logs = tmp_path / "migration_logs"
        old = _abandoned_dir(logs / "run_old" / ATTACHMENT_TEMP_DIRNAME)
        current = logs / "run_new"
        current.mkdir(parents=True)

        assert sweep_stale_temp_dirs(str(current)) == 1
        assert not old.exists()

    def test_removes_abandoned_fallback_dir(self, isolated_fallback):
        old = _abandoned_dir(isolated_fallback)
        assert sweep_stale_temp_dirs(None) == 1
        assert not old.exists()

    def test_keeps_dir_of_live_handler(self, tmp_path):
        live = AttachmentTempDir(lambda: str(tmp_path / "run_a"))
        live_path = Path(live.path())

        assert sweep_stale_temp_dirs(str(tmp_path / "run_b")) == 0
        assert live_path.exists()
        live.cleanup()

    def test_ignores_dirs_without_lock_file(self, isolated_fallback):
        unrelated = isolated_fallback / "something"
        unrelated.mkdir(parents=True)
        assert sweep_stale_temp_dirs(None) == 0
        assert unrelated.exists()

    def test_nothing_to_sweep(self, tmp_path):
        assert sweep_stale_temp_dirs(str(tmp_path / "missing" / "run")) == 0

    @pytest.mark.skipif(sys.platform == "win32", reason="uses POSIX signals")
    def test_files_of_killed_run_are_removed_on_next_start(self, tmp_path):
        """A SIGKILLed migration's downloads are swept by the next run (I28)."""
        logs = tmp_path / "migration_logs"
        script = textwrap.dedent(
            f"""
            import sys, time
            from slack_chat_migrator.services.files.temp_files import AttachmentTempDir
            d = AttachmentTempDir(lambda: {str(logs / "run_1")!r})
            path = d.path()
            open(path + "/dl_big.mov", "wb").write(b"x" * 1024)
            print(path, flush=True)
            time.sleep(60)
            """
        )
        env = dict(os.environ, PYTHONPATH=str(SRC_DIR))
        child = subprocess.Popen(  # noqa: S603 — fixed interpreter and script
            [sys.executable, "-c", script], stdout=subprocess.PIPE, text=True, env=env
        )
        try:
            assert child.stdout is not None
            path = Path(child.stdout.readline().strip())
            assert (path / "dl_big.mov").exists()
            # Still running: the next run must leave it alone.
            assert sweep_stale_temp_dirs(str(logs / "run_2")) == 0
            child.send_signal(signal.SIGKILL)
            child.wait(timeout=10)
        finally:
            if child.poll() is None:
                child.kill()
            if child.stdout is not None:
                child.stdout.close()

        assert path.exists()  # SIGKILL skipped every cleanup handler
        assert sweep_stale_temp_dirs(str(logs / "run_2")) == 1
        assert not path.exists()


def test_remove_temp_file_tolerates_missing(tmp_path):
    target = tmp_path / "f"
    target.write_bytes(b"x")
    remove_temp_file(str(target))
    remove_temp_file(str(target))
    remove_temp_file(None)
    assert not target.exists()
