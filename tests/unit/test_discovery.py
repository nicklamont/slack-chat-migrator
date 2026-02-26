"""Unit tests for the discovery module."""

from unittest.mock import MagicMock, patch

from googleapiclient.errors import HttpError
from httplib2 import Response

from slack_migrator.core.state import MigrationState
from slack_migrator.services.discovery import (
    discover_existing_spaces,
    get_last_message_timestamp,
    should_process_message,
)


def _make_space(
    display_name, space_name, space_type="SPACE", create_time="2024-01-01T00:00:00Z"
):
    """Helper to build a space dict as returned by the Chat API."""
    return {
        "displayName": display_name,
        "name": space_name,
        "spaceType": space_type,
        "createTime": create_time,
    }


def _setup_list_response(chat, pages):
    """
    Configure chat.spaces().list().execute() to return pages in sequence.

    Args:
        chat: The mock Chat API service.
        pages: A list of dicts, each representing a page response.
    """
    list_mock = MagicMock()
    list_mock.execute = MagicMock(side_effect=[p for p in pages])
    chat.spaces.return_value.list.return_value = list_mock


class TestDiscoverExistingSpaces:
    """Tests for discover_existing_spaces()."""

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_single_space_found(self, mock_sleep):
        """A single matching space is discovered and mapped correctly."""
        chat = MagicMock()
        state = MigrationState()
        channel_name_to_id = {"general": "C001"}
        _setup_list_response(
            chat,
            [
                {
                    "spaces": [
                        _make_space("Slack #general", "spaces/abc123"),
                    ],
                }
            ],
        )

        space_mappings, duplicate_spaces = discover_existing_spaces(
            chat, channel_name_to_id, state
        )

        assert space_mappings == {"general": "spaces/abc123"}
        assert duplicate_spaces == {}
        assert state.channel_id_to_space_id["C001"] == "abc123"

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_empty_response(self, mock_sleep):
        """No spaces returned from the API."""
        chat = MagicMock()
        state = MigrationState()
        _setup_list_response(chat, [{"spaces": []}])

        space_mappings, duplicate_spaces = discover_existing_spaces(chat, {}, state)

        assert space_mappings == {}
        assert duplicate_spaces == {}

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_no_spaces_key(self, mock_sleep):
        """API response with no 'spaces' key at all."""
        chat = MagicMock()
        state = MigrationState()
        _setup_list_response(chat, [{}])

        space_mappings, duplicate_spaces = discover_existing_spaces(chat, {}, state)

        assert space_mappings == {}
        assert duplicate_spaces == {}

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_non_matching_spaces_ignored(self, mock_sleep):
        """Spaces without the 'Slack #' prefix are ignored."""
        chat = MagicMock()
        state = MigrationState()
        _setup_list_response(
            chat,
            [
                {
                    "spaces": [
                        _make_space("Engineering Team", "spaces/eng1"),
                        _make_space("Marketing", "spaces/mkt1"),
                    ],
                }
            ],
        )

        space_mappings, duplicate_spaces = discover_existing_spaces(chat, {}, state)

        assert space_mappings == {}
        assert duplicate_spaces == {}

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_pagination_two_pages(self, mock_sleep):
        """Spaces spread across two pages are both discovered."""
        chat = MagicMock()
        state = MigrationState()
        channel_name_to_id = {"general": "C001", "random": "C002"}
        _setup_list_response(
            chat,
            [
                {
                    "spaces": [_make_space("Slack #general", "spaces/abc")],
                    "nextPageToken": "token_page2",
                },
                {
                    "spaces": [_make_space("Slack #random", "spaces/def")],
                },
            ],
        )

        space_mappings, duplicate_spaces = discover_existing_spaces(
            chat, channel_name_to_id, state
        )

        assert space_mappings == {
            "general": "spaces/abc",
            "random": "spaces/def",
        }
        assert duplicate_spaces == {}
        assert state.channel_id_to_space_id == {"C001": "abc", "C002": "def"}
        # Verify sleep was called between pages
        mock_sleep.assert_called_once_with(0.2)

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_pagination_three_pages(self, mock_sleep):
        """Three pages of results are all processed."""
        chat = MagicMock()
        state = MigrationState()
        channel_name_to_id = {"a": "C1", "b": "C2", "c": "C3"}
        _setup_list_response(
            chat,
            [
                {
                    "spaces": [_make_space("Slack #a", "spaces/s1")],
                    "nextPageToken": "tok2",
                },
                {
                    "spaces": [_make_space("Slack #b", "spaces/s2")],
                    "nextPageToken": "tok3",
                },
                {
                    "spaces": [_make_space("Slack #c", "spaces/s3")],
                },
            ],
        )

        space_mappings, _ = discover_existing_spaces(chat, channel_name_to_id, state)

        assert len(space_mappings) == 3
        assert mock_sleep.call_count == 2

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_duplicate_spaces_detected(self, mock_sleep):
        """Multiple spaces with the same channel name are flagged as duplicates."""
        chat = MagicMock()
        state = MigrationState()
        channel_name_to_id = {"general": "C001"}

        # Set up members list mock for duplicate space disambiguation
        members_mock = MagicMock()
        members_mock.execute.return_value = {
            "memberships": [{"name": "m1"}, {"name": "m2"}]
        }
        chat.spaces.return_value.members.return_value.list.return_value = members_mock

        _setup_list_response(
            chat,
            [
                {
                    "spaces": [
                        _make_space(
                            "Slack #general",
                            "spaces/abc",
                            create_time="2024-01-01T00:00:00Z",
                        ),
                        _make_space(
                            "Slack #general",
                            "spaces/def",
                            create_time="2024-02-01T00:00:00Z",
                        ),
                    ],
                }
            ],
        )

        space_mappings, duplicate_spaces = discover_existing_spaces(
            chat, channel_name_to_id, state
        )

        # The first space is used as default mapping
        assert space_mappings["general"] == "spaces/abc"
        # Duplicates are reported
        assert "general" in duplicate_spaces
        assert len(duplicate_spaces["general"]) == 2
        # Channel ID mapping should be removed for ambiguous channels
        assert "C001" not in state.channel_id_to_space_id

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_duplicate_spaces_member_count(self, mock_sleep):
        """Member count is populated for duplicate spaces."""
        chat = MagicMock()
        state = MigrationState()
        channel_name_to_id = {"general": "C001"}

        members_mock = MagicMock()
        members_mock.execute.return_value = {
            "memberships": [{"name": "m1"}],
            "nextPageToken": "more",
        }
        chat.spaces.return_value.members.return_value.list.return_value = members_mock

        _setup_list_response(
            chat,
            [
                {
                    "spaces": [
                        _make_space("Slack #general", "spaces/abc"),
                        _make_space("Slack #general", "spaces/def"),
                    ],
                }
            ],
        )

        _, duplicate_spaces = discover_existing_spaces(chat, channel_name_to_id, state)

        # When nextPageToken is present, member_count should have "+" suffix
        for space_info in duplicate_spaces["general"]:
            assert space_info["member_count"] == "1+"

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_duplicate_spaces_member_fetch_error(self, mock_sleep):
        """Errors fetching member counts for duplicates are handled gracefully."""
        chat = MagicMock()
        state = MigrationState()
        channel_name_to_id = {"general": "C001"}

        members_mock = MagicMock()
        members_mock.execute.side_effect = HttpError(
            Response({"status": "500"}), b"API error"
        )
        chat.spaces.return_value.members.return_value.list.return_value = members_mock

        _setup_list_response(
            chat,
            [
                {
                    "spaces": [
                        _make_space("Slack #general", "spaces/abc"),
                        _make_space("Slack #general", "spaces/def"),
                    ],
                }
            ],
        )

        # Should not raise
        _space_mappings, duplicate_spaces = discover_existing_spaces(
            chat, channel_name_to_id, state
        )
        assert "general" in duplicate_spaces
        # Member count stays at default 0
        for space_info in duplicate_spaces["general"]:
            assert space_info["member_count"] == 0

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_http_error_handled(self, mock_sleep):
        """HttpError from the API is caught and returns empty results."""
        chat = MagicMock()
        state = MigrationState()
        http_error = HttpError(
            resp=MagicMock(status=403),
            content=b"Forbidden",
        )
        list_mock = MagicMock()
        list_mock.execute.side_effect = http_error
        chat.spaces.return_value.list.return_value = list_mock

        space_mappings, duplicate_spaces = discover_existing_spaces(chat, {}, state)

        assert space_mappings == {}
        assert duplicate_spaces == {}

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_channel_id_to_space_id_stays_empty_with_no_matches(self, mock_sleep):
        """channel_id_to_space_id remains empty when no spaces match."""
        chat = MagicMock()
        state = MigrationState()
        _setup_list_response(chat, [{"spaces": []}])

        discover_existing_spaces(chat, {}, state)

        assert state.channel_id_to_space_id == {}

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_channel_id_to_space_id_preserved_when_exists(self, mock_sleep):
        """Existing channel_id_to_space_id entries are preserved."""
        chat = MagicMock()
        state = MigrationState()
        state.channel_id_to_space_id = {"C999": "existing"}
        _setup_list_response(chat, [{"spaces": []}])

        discover_existing_spaces(chat, {}, state)

        assert state.channel_id_to_space_id["C999"] == "existing"

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_space_without_display_name_ignored(self, mock_sleep):
        """Spaces with empty or missing displayName are skipped."""
        chat = MagicMock()
        state = MigrationState()
        _setup_list_response(
            chat,
            [
                {
                    "spaces": [
                        {"name": "spaces/x", "spaceType": "SPACE"},
                        {"displayName": "", "name": "spaces/y", "spaceType": "SPACE"},
                    ],
                }
            ],
        )

        space_mappings, duplicate_spaces = discover_existing_spaces(chat, {}, state)

        assert space_mappings == {}
        assert duplicate_spaces == {}

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_space_with_prefix_only_ignored(self, mock_sleep):
        """A space named exactly 'Slack #' with no channel name after prefix is ignored."""
        chat = MagicMock()
        state = MigrationState()
        _setup_list_response(
            chat,
            [
                {
                    "spaces": [_make_space("Slack #", "spaces/empty")],
                }
            ],
        )

        space_mappings, _ = discover_existing_spaces(chat, {}, state)

        # "Slack #" with nothing after it -> channel_name is empty string -> skipped
        assert space_mappings == {}

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_channel_without_id_mapping(self, mock_sleep):
        """Space is mapped by name even when channel has no ID in channel_name_to_id."""
        chat = MagicMock()
        state = MigrationState()
        _setup_list_response(
            chat,
            [
                {
                    "spaces": [_make_space("Slack #orphan", "spaces/xyz")],
                }
            ],
        )

        space_mappings, _ = discover_existing_spaces(chat, {}, state)

        assert space_mappings == {"orphan": "spaces/xyz"}

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_mixed_matching_and_nonmatching_spaces(self, mock_sleep):
        """Only spaces with the migration prefix are included in results."""
        chat = MagicMock()
        state = MigrationState()
        channel_name_to_id = {"general": "C001"}
        _setup_list_response(
            chat,
            [
                {
                    "spaces": [
                        _make_space("Team Chat", "spaces/team"),
                        _make_space("Slack #general", "spaces/gen"),
                        _make_space("Other Space", "spaces/other"),
                    ],
                }
            ],
        )

        space_mappings, _ = discover_existing_spaces(chat, channel_name_to_id, state)

        assert space_mappings == {"general": "spaces/gen"}

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_space_id_extraction_from_name(self, mock_sleep):
        """space_id is correctly extracted from 'spaces/{id}' format."""
        chat = MagicMock()
        state = MigrationState()
        channel_name_to_id = {"dev": "C010"}
        _setup_list_response(
            chat,
            [
                {
                    "spaces": [
                        _make_space("Slack #dev", "spaces/AAAA1234"),
                    ],
                }
            ],
        )

        discover_existing_spaces(chat, channel_name_to_id, state)

        assert state.channel_id_to_space_id["C010"] == "AAAA1234"

    @patch("slack_migrator.services.discovery.time.sleep")
    def test_duplicate_channel_id_first_occurrence_wins_in_pagination(self, mock_sleep):
        """When the same channel appears on different pages, first occurrence sets the ID mapping."""
        chat = MagicMock()
        state = MigrationState()
        channel_name_to_id = {"general": "C001"}

        # Members mock for duplicate disambiguation
        members_mock = MagicMock()
        members_mock.execute.return_value = {"memberships": []}
        chat.spaces.return_value.members.return_value.list.return_value = members_mock

        _setup_list_response(
            chat,
            [
                {
                    "spaces": [_make_space("Slack #general", "spaces/first")],
                    "nextPageToken": "page2",
                },
                {
                    "spaces": [_make_space("Slack #general", "spaces/second")],
                },
            ],
        )

        _space_mappings, duplicate_spaces = discover_existing_spaces(
            chat, channel_name_to_id, state
        )

        # Duplicates detected
        assert "general" in duplicate_spaces
        # Ambiguous mapping removed
        assert "C001" not in state.channel_id_to_space_id


class TestGetLastMessageTimestamp:
    """Tests for get_last_message_timestamp()."""

    def _make_chat(self, execute_return):
        """Create a mock chat service with messages().list() configured."""
        chat = MagicMock()
        messages_mock = MagicMock()
        messages_mock.execute.return_value = execute_return
        chat.spaces.return_value.messages.return_value.list.return_value = messages_mock
        return chat

    def test_returns_timestamp_with_z_suffix(self):
        """Parses RFC3339 timestamp ending with Z."""
        chat = self._make_chat({"messages": [{"createTime": "2024-06-15T12:30:00Z"}]})

        result = get_last_message_timestamp(chat, "general", "spaces/abc")

        assert result > 0
        import datetime

        expected = datetime.datetime(
            2024, 6, 15, 12, 30, 0, tzinfo=datetime.timezone.utc
        )
        assert result == expected.timestamp()

    def test_returns_timestamp_with_offset(self):
        """Parses RFC3339 timestamp with explicit timezone offset."""
        chat = self._make_chat(
            {"messages": [{"createTime": "2024-06-15T12:30:00+00:00"}]}
        )

        result = get_last_message_timestamp(chat, "general", "spaces/abc")

        assert result > 0

    def test_returns_timestamp_without_timezone(self):
        """Parses timestamp with no timezone info (assumes UTC)."""
        chat = self._make_chat({"messages": [{"createTime": "2024-06-15T12:30:00"}]})

        result = get_last_message_timestamp(chat, "general", "spaces/abc")

        assert result > 0

    def test_no_messages_returns_zero(self):
        """Empty messages list returns 0."""
        chat = self._make_chat({"messages": []})

        result = get_last_message_timestamp(chat, "general", "spaces/abc")

        assert result == 0.0

    def test_no_messages_key_returns_zero(self):
        """Response with no 'messages' key returns 0."""
        chat = self._make_chat({})

        result = get_last_message_timestamp(chat, "general", "spaces/abc")

        assert result == 0.0

    def test_message_without_create_time_returns_zero(self):
        """Message with no createTime field returns 0 and logs warning."""
        chat = self._make_chat({"messages": [{"text": "hello"}]})

        result = get_last_message_timestamp(chat, "general", "spaces/abc")

        assert result == 0.0

    def test_message_with_empty_create_time_returns_zero(self):
        """Message with empty string createTime returns 0."""
        chat = self._make_chat({"messages": [{"createTime": ""}]})

        result = get_last_message_timestamp(chat, "general", "spaces/abc")

        assert result == 0.0

    def test_http_error_returns_zero(self):
        """HttpError is caught and returns 0."""
        chat = MagicMock()
        http_error = HttpError(
            resp=MagicMock(status=404),
            content=b"Not Found",
        )
        messages_mock = MagicMock()
        messages_mock.execute.side_effect = http_error
        chat.spaces.return_value.messages.return_value.list.return_value = messages_mock

        result = get_last_message_timestamp(chat, "general", "spaces/abc")

        assert result == 0.0

    def test_negative_offset_timestamp(self):
        """Parses timestamp with negative UTC offset."""
        chat = self._make_chat(
            {"messages": [{"createTime": "2024-06-15T07:30:00-05:00"}]}
        )

        result = get_last_message_timestamp(chat, "general", "spaces/abc")

        # 07:30 -05:00 = 12:30 UTC
        import datetime

        expected = datetime.datetime(
            2024, 6, 15, 12, 30, 0, tzinfo=datetime.timezone.utc
        )
        assert result == expected.timestamp()


class TestShouldProcessMessage:
    """Tests for should_process_message()."""

    def test_newer_message_processed(self):
        """Message newer than last timestamp should be processed."""
        assert should_process_message(1609459200.0, "1609459300.000000") is True

    def test_older_message_skipped(self):
        """Message older than last timestamp should not be processed."""
        assert should_process_message(1609459200.0, "1609459100.000000") is False

    def test_same_timestamp_skipped(self):
        """Message with same timestamp as last should not be processed."""
        assert should_process_message(1609459200.0, "1609459200.000000") is False

    def test_zero_last_timestamp_processes_all(self):
        """When last timestamp is 0, all messages should be processed."""
        assert should_process_message(0, "1609459200.000000") is True

    def test_invalid_timestamp_processed(self):
        """Invalid timestamp strings default to processing the message."""
        assert should_process_message(1609459200.0, "not_a_number") is True

    def test_empty_timestamp_processed(self):
        """Empty timestamp string defaults to processing the message."""
        assert should_process_message(1609459200.0, "") is True

    def test_timestamp_with_no_decimal(self):
        """Slack timestamp without decimal part is handled."""
        assert should_process_message(1609459200.0, "1609459300") is True

    def test_timestamp_uses_integer_part_only(self):
        """Only the integer part of the Slack timestamp is used for comparison."""
        # 1609459200.999999 -> integer part 1609459200, which is not > 1609459200
        assert should_process_message(1609459200.0, "1609459200.999999") is False
