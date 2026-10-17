"""Driving the migrator as a library, outside the CLI."""

from __future__ import annotations

import signal
import threading
from pathlib import Path

import pytest

from tests.integration.conftest import make_migrator

pytestmark = pytest.mark.integration


class TestMigrateFromWorkerThread:
    """migrate() works when called from a non-main thread (issue #119)."""

    def test_dry_run_in_worker_thread(self, single_channel_export: Path) -> None:
        m = make_migrator(single_channel_export)
        outcome: list[object] = []

        def worker() -> None:
            try:
                outcome.append(m.migrate())
            except BaseException as e:
                outcome.append(e)

        handler_before = signal.getsignal(signal.SIGINT)
        thread = threading.Thread(target=worker)
        thread.start()
        thread.join(timeout=60)

        assert outcome, "migrate() did not finish"
        assert not isinstance(outcome[0], BaseException), repr(outcome[0])
        assert m.state.progress.migration_summary["messages_created"] == 3
        # The worker never touched the process's SIGINT handler
        assert signal.getsignal(signal.SIGINT) is handler_before

    def test_main_thread_restores_sigint_handler(
        self, single_channel_export: Path
    ) -> None:
        m = make_migrator(single_channel_export)
        handler_before = signal.getsignal(signal.SIGINT)
        m.migrate()
        assert signal.getsignal(signal.SIGINT) is handler_before
