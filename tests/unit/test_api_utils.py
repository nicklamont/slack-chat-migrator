"""Unit tests for the API utilities module."""

from slack_migrator.utils.api import slack_ts_to_rfc3339


class TestSlackTsToRfc3339:
    """Tests for slack_ts_to_rfc3339()."""

    def test_basic_conversion(self):
        result = slack_ts_to_rfc3339("1609459200.000000")
        assert result == "2021-01-01T00:00:00.000000Z"

    def test_with_microseconds(self):
        result = slack_ts_to_rfc3339("1609459200.123456")
        assert result == "2021-01-01T00:00:00.123456Z"

    def test_preserves_microsecond_precision(self):
        result = slack_ts_to_rfc3339("1609459200.000001")
        assert result.endswith(".000001Z")

    def test_nonzero_time(self):
        # 2023-06-15T12:10:45 UTC
        result = slack_ts_to_rfc3339("1686831045.000000")
        assert result == "2023-06-15T12:10:45.000000Z"

    def test_result_ends_with_z(self):
        result = slack_ts_to_rfc3339("0.000000")
        assert result.endswith("Z")

    def test_result_is_rfc3339_format(self):
        result = slack_ts_to_rfc3339("1609459200.000000")
        # RFC3339: YYYY-MM-DDTHH:MM:SS.xxxxxxZ
        assert "T" in result
        assert result.endswith("Z")
        date_part, time_part = result.split("T")
        assert len(date_part.split("-")) == 3
