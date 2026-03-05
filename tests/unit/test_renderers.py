"""Tests for progress renderers."""

from __future__ import annotations

import io
from unittest.mock import patch

from slack_chat_migrator.cli.renderers import create_renderer
from slack_chat_migrator.cli.renderers.plain_renderer import PlainProgressRenderer
from slack_chat_migrator.cli.renderers.rich_renderer import RichProgressRenderer
from slack_chat_migrator.core.progress import EventType, ProgressEvent, ProgressTracker


class TestPlainProgressRenderer:
    """Tests for the plain-text renderer."""

    def test_start_prints_message(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output)
        renderer.start()

        assert "Migration started" in output.getvalue()

    def test_stop_prints_summary(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output)
        renderer.start()
        renderer.stop()

        text = output.getvalue()
        assert "Migration finished" in text

    def test_phase_change_prints(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output)
        renderer.start()

        tracker.phase_change("validation")

        assert "Phase: validation" in output.getvalue()

    def test_channel_start_prints(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output)
        renderer.start()

        tracker.channel_start("general")

        assert "Processing channel: general" in output.getvalue()

    def test_channel_complete_prints(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output)
        renderer.start()

        tracker.channel_complete("general")

        assert "Completed channel: general" in output.getvalue()

    def test_message_failed_prints(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output)
        renderer.start()

        tracker.message_failed("general", detail="timeout")

        assert "timeout" in output.getvalue()

    def test_status_line_includes_timestamp(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output)
        renderer.start()

        tracker.phase_change("test")

        lines = output.getvalue().strip().split("\n")
        for line in lines:
            # Each line should start with a timestamp like [00:00]
            assert line.startswith("[")

    def test_counters_accumulate(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output, interval=0)
        renderer.start()

        tracker.message_sent("general")
        tracker.message_sent("general")
        tracker.message_failed("general", detail="err")

        renderer.stop()
        text = output.getvalue()
        assert "2 sent" in text
        assert "1 failed" in text


class TestRichProgressRenderer:
    """Tests for the Rich renderer (unit-level, no Live display)."""

    def test_handle_event_updates_counters(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)

        tracker.emit(
            ProgressEvent(event_type=EventType.MESSAGE_SENT, channel="general")
        )
        tracker.emit(
            ProgressEvent(event_type=EventType.MESSAGE_SENT, channel="general")
        )
        tracker.emit(
            ProgressEvent(event_type=EventType.MESSAGE_FAILED, channel="general")
        )
        tracker.emit(
            ProgressEvent(event_type=EventType.FILE_UPLOADED, channel="general")
        )

        assert renderer._messages_sent == 2
        assert renderer._messages_failed == 1
        assert renderer._files_uploaded == 1

    def test_phase_change_updates_state(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)

        tracker.phase_change("cleanup")

        assert renderer._current_phase == "cleanup"

    def test_channel_lifecycle(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)

        tracker.channel_start("general", total_messages=10)
        assert renderer._current_channel == "general"
        assert renderer._channel_msg_total == 10

        tracker.channel_complete("general")
        assert renderer._channels_complete == 1


class TestRendererFactory:
    """Tests for the create_renderer factory function."""

    @patch("sys.stdout")
    def test_returns_rich_for_tty(self, mock_stdout):
        mock_stdout.isatty.return_value = True
        tracker = ProgressTracker()
        renderer = create_renderer(tracker)
        assert isinstance(renderer, RichProgressRenderer)

    @patch("sys.stdout")
    def test_returns_plain_for_non_tty(self, mock_stdout):
        mock_stdout.isatty.return_value = False
        tracker = ProgressTracker()
        renderer = create_renderer(tracker)
        assert isinstance(renderer, PlainProgressRenderer)
