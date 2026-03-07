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

        tracker.channel_start("general")
        assert renderer._current_channel == "general"

        # Message bar is created by MESSAGE_PHASE_START, not channel_start
        tracker.message_phase_start("general", total=10)
        assert renderer._channel_msg_total == 10

        tracker.channel_complete("general")
        assert renderer._channels_complete == 1


class TestRichProgressRendererDryRun:
    """Tests for dry-run mode in the Rich renderer."""

    def test_dry_run_changes_header_title(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker, dry_run=True)
        panel = renderer._build_header_panel()

        assert panel.title is not None
        # Rich wraps title in Text objects; convert to plain string
        title_str = str(panel.title)
        assert "Validating Slack Export" in title_str

    def test_live_mode_header_title(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker, dry_run=False)
        panel = renderer._build_header_panel()

        title_str = str(panel.title)
        assert "Migrating" in title_str

    def test_dry_run_header_has_blue_border(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker, dry_run=True)
        panel = renderer._build_header_panel()

        assert str(panel.border_style) == "blue"

    def test_live_mode_header_has_green_border(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker, dry_run=False)
        panel = renderer._build_header_panel()

        assert str(panel.border_style) == "green"

    def test_dry_run_phase_has_prefix(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker, dry_run=True)
        tracker.phase_change("Processing messages")
        panel = renderer._build_header_panel()
        # Render the panel to a string to check content
        from io import StringIO

        from rich.console import Console

        buf = StringIO()
        Console(file=buf, width=80).print(panel)
        content = buf.getvalue()
        assert "[DRY RUN]" in content


class TestRichMemberBar:
    """Tests for the member sub-bar in the Rich renderer."""

    def test_member_bar_created_on_phase_start(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)

        tracker.member_phase_start("general", total=10)

        assert renderer._member_task is not None
        assert renderer._member_done == 0

    def test_member_bar_advances_on_member_added(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)

        tracker.member_phase_start("general", total=10)
        tracker.member_added("general")
        tracker.member_added("general")

        assert renderer._member_done == 2
        assert renderer._members_added == 2

    def test_member_bar_removed_on_channel_complete(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker, total_channels=1)

        tracker.channel_start("general", total_messages=5)
        tracker.member_phase_start("general", total=10)
        tracker.channel_complete("general")

        assert renderer._member_task is None
        assert renderer._member_done == 0

    def test_member_bar_replaced_on_new_phase_start(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)

        tracker.member_phase_start("general", total=10)
        first_task = renderer._member_task

        tracker.member_phase_start("random", total=5)
        assert renderer._member_task is not None
        assert renderer._member_task != first_task
        assert renderer._member_done == 0


class TestRichMessageBar:
    """Tests for the message sub-bar in the Rich renderer."""

    def test_message_bar_created_on_phase_start(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)

        tracker.message_phase_start("general", total=50)

        assert renderer._message_task is not None
        assert renderer._channel_msg_done == 0
        assert renderer._channel_msg_total == 50

    def test_message_bar_advances_on_message_sent(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)

        tracker.message_phase_start("general", total=10)
        tracker.message_sent("general")
        tracker.message_sent("general")

        assert renderer._channel_msg_done == 2
        assert renderer._messages_sent == 2

    def test_message_bar_removed_on_channel_complete(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker, total_channels=1)

        tracker.channel_start("general")
        tracker.message_phase_start("general", total=5)
        tracker.channel_complete("general")

        assert renderer._message_task is None

    def test_message_bar_replaced_on_new_channel(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)

        tracker.message_phase_start("general", total=10)
        first_task = renderer._message_task

        # Starting a new channel clears the old bar
        tracker.channel_start("random")
        assert renderer._message_task is None

        # New phase start creates a fresh bar
        tracker.message_phase_start("random", total=20)
        assert renderer._message_task is not None
        assert renderer._message_task != first_task
        assert renderer._channel_msg_total == 20


class TestRichThroughputAndErrors:
    """Tests for throughput and error rate display."""

    def test_throughput_in_stats_when_messages_sent(self):
        from collections import deque

        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)
        renderer._start_time = 1.0  # Fixed start time
        renderer._messages_sent = 100
        # Populate rolling window with recent timestamps
        renderer._recent_msg_times = deque([5.0 + i * 0.1 for i in range(50)])

        import time

        with patch.object(time, "time", return_value=11.0):
            table = renderer._build_stats_table()

        # Table should contain a "Throughput" row
        # Rich tables store row data; check the rendered output
        from io import StringIO

        from rich.console import Console

        buf = StringIO()
        Console(file=buf, width=80).print(table)
        output = buf.getvalue()
        assert "Throughput" in output
        assert "msgs/sec" in output

    def test_error_rate_percentage_shown(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)
        renderer._messages_sent = 90
        renderer._messages_failed = 10
        renderer._start_time = 1.0

        import time

        with patch.object(time, "time", return_value=2.0):
            table = renderer._build_stats_table()

        from io import StringIO

        from rich.console import Console

        buf = StringIO()
        Console(file=buf, width=80).print(table)
        output = buf.getvalue()
        assert "10.0%" in output

    def test_no_throughput_when_no_messages(self):
        tracker = ProgressTracker()
        renderer = RichProgressRenderer(tracker)
        renderer._start_time = 1.0
        renderer._messages_sent = 0

        import time

        with patch.object(time, "time", return_value=11.0):
            table = renderer._build_stats_table()

        from io import StringIO

        from rich.console import Console

        buf = StringIO()
        Console(file=buf, width=80).print(table)
        output = buf.getvalue()
        assert "Throughput" not in output


class TestPlainRendererDryRun:
    """Tests for dry-run mode in the plain renderer."""

    def test_dry_run_prefix_in_output(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output, dry_run=True)
        renderer.start()

        assert "[DRY RUN]" in output.getvalue()

    def test_no_prefix_when_not_dry_run(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output, dry_run=False)
        renderer.start()

        assert "[DRY RUN]" not in output.getvalue()

    def test_member_phase_start_prints(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output)
        renderer.start()

        tracker.member_phase_start("general", total=24)

        text = output.getvalue()
        assert "Adding members to #general" in text
        assert "24 users" in text

    def test_message_phase_start_prints(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output)
        renderer.start()

        tracker.message_phase_start("general", total=100)

        text = output.getvalue()
        assert "Sending messages in #general" in text
        assert "100 messages" in text

    def test_throughput_in_status(self):
        tracker = ProgressTracker()
        output = io.StringIO()
        renderer = PlainProgressRenderer(tracker, output=output, interval=0)
        renderer.start()

        # Send a message to trigger status
        tracker.message_sent("general")

        text = output.getvalue()
        assert "Messages:" in text


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
