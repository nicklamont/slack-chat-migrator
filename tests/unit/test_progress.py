"""Tests for the ProgressTracker event emitter."""

from slack_chat_migrator.core.progress import EventType, ProgressEvent, ProgressTracker


class TestProgressTracker:
    """Tests for subscribe/emit and convenience methods."""

    def test_subscribe_and_emit(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        event = ProgressEvent(event_type=EventType.MESSAGE_SENT, channel="general")
        tracker.emit(event)

        assert len(received) == 1
        assert received[0] is event

    def test_multiple_subscribers(self):
        tracker = ProgressTracker()
        received_a: list[ProgressEvent] = []
        received_b: list[ProgressEvent] = []
        tracker.subscribe(received_a.append)
        tracker.subscribe(received_b.append)

        tracker.emit(ProgressEvent(event_type=EventType.PHASE_CHANGE, detail="test"))

        assert len(received_a) == 1
        assert len(received_b) == 1

    def test_no_subscribers_does_not_error(self):
        tracker = ProgressTracker()
        tracker.emit(ProgressEvent(event_type=EventType.MESSAGE_SENT))

    def test_channel_start_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.channel_start("general", total_messages=42)

        assert len(received) == 1
        assert received[0].event_type == EventType.CHANNEL_START
        assert received[0].channel == "general"
        assert received[0].total == 42

    def test_channel_complete_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.channel_complete("general")

        assert received[0].event_type == EventType.CHANNEL_COMPLETE
        assert received[0].channel == "general"

    def test_message_sent_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.message_sent("general", count=5, total=10)

        assert received[0].event_type == EventType.MESSAGE_SENT
        assert received[0].count == 5
        assert received[0].total == 10

    def test_message_failed_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.message_failed("general", detail="API error")

        assert received[0].event_type == EventType.MESSAGE_FAILED
        assert received[0].detail == "API error"

    def test_file_uploaded_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.file_uploaded("general")

        assert received[0].event_type == EventType.FILE_UPLOADED

    def test_reaction_added_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.reaction_added("general")

        assert received[0].event_type == EventType.REACTION_ADDED

    def test_space_created_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.space_created("general")

        assert received[0].event_type == EventType.SPACE_CREATED

    def test_member_added_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.member_added("general")

        assert received[0].event_type == EventType.MEMBER_ADDED

    def test_member_phase_start_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.member_phase_start("general", total=24)

        assert received[0].event_type == EventType.MEMBER_PHASE_START
        assert received[0].channel == "general"
        assert received[0].total == 24

    def test_message_phase_start_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.message_phase_start("general", total=100)

        assert received[0].event_type == EventType.MESSAGE_PHASE_START
        assert received[0].channel == "general"
        assert received[0].total == 100

    def test_phase_change_convenience(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.phase_change("migration")

        assert received[0].event_type == EventType.PHASE_CHANGE
        assert received[0].detail == "migration"

    def test_event_has_timestamp(self):
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []
        tracker.subscribe(received.append)

        tracker.message_sent("general")

        assert received[0].timestamp > 0

    def test_failing_subscriber_does_not_block_others(self):
        """A subscriber that raises should not prevent other subscribers."""
        tracker = ProgressTracker()
        received: list[ProgressEvent] = []

        def bad_subscriber(event: ProgressEvent) -> None:
            raise RuntimeError("boom")

        tracker.subscribe(bad_subscriber)
        tracker.subscribe(received.append)

        tracker.message_sent("general")

        assert len(received) == 1
        assert received[0].event_type == EventType.MESSAGE_SENT
