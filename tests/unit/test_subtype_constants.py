"""Tests for the Slack non-content subtype constants."""

from slack_chat_migrator import constants


def test_channel_events_cover_the_system_subtypes() -> None:
    assert constants.SYSTEM_SUBTYPES <= constants.CHANNEL_EVENT_SUBTYPES


def test_channel_events_include_topic_purpose_name_archive_and_pins() -> None:
    for subtype in (
        "channel_topic",
        "channel_purpose",
        "channel_name",
        "channel_archive",
        "pinned_item",
        "group_join",
        "group_leave",
    ):
        assert subtype in constants.CHANNEL_EVENT_SUBTYPES


def test_edit_log_subtypes() -> None:
    assert constants.EDIT_LOG_SUBTYPES == {"message_changed", "message_deleted"}
    assert not constants.EDIT_LOG_SUBTYPES & constants.CHANNEL_EVENT_SUBTYPES
    assert constants.TOMBSTONE_SUBTYPE not in constants.CHANNEL_EVENT_SUBTYPES
