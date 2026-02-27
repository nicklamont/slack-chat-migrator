"""Unit tests for the API utilities module."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httplib2
import pytest
from googleapiclient.errors import HttpError

from slack_migrator.utils.api import (
    RetryWrapper,
    _service_cache,
    escape_drive_query_value,
    get_gcp_service,
    slack_ts_to_rfc3339,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_http_error(status: int, reason: str = "error") -> HttpError:
    """Create an HttpError with the given status code."""
    resp = httplib2.Response({"status": status})
    resp.reason = reason
    return HttpError(resp, b"error body")


# ---------------------------------------------------------------------------
# slack_ts_to_rfc3339
# ---------------------------------------------------------------------------


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
        date_part, _time_part = result.split("T")
        assert len(date_part.split("-")) == 3


# ---------------------------------------------------------------------------
# RetryWrapper — basic attribute delegation
# ---------------------------------------------------------------------------


class TestRetryWrapperDelegation:
    """Verify RetryWrapper transparently proxies attributes."""

    def test_non_callable_attribute_is_returned_directly(self):
        inner = SimpleNamespace(value=42)
        wrapper = RetryWrapper(inner)
        assert wrapper.value == 42

    def test_callable_non_execute_returns_retry_wrapper(self):
        """Calling a method that returns an object with `execute` re-wraps."""
        inner_result = MagicMock()
        inner_result.execute = MagicMock(return_value="ok")

        inner = MagicMock()
        inner.spaces.return_value = inner_result

        wrapper = RetryWrapper(inner)
        result = wrapper.spaces()
        assert isinstance(result, RetryWrapper)

    def test_callable_returning_plain_value_not_wrapped(self):
        """Methods whose return has no execute/list/create are not wrapped."""
        inner = MagicMock()
        inner.plain_method.return_value = "plain_string"

        wrapper = RetryWrapper(inner)
        result = wrapper.plain_method()
        assert result == "plain_string"

    def test_channel_context_getter_passed_to_child_wrappers(self):
        """Child RetryWrappers inherit the channel_context_getter."""
        inner_result = MagicMock()
        inner_result.execute = MagicMock(return_value="ok")

        inner = MagicMock()
        inner.messages.return_value = inner_result

        def getter():
            return "general"

        wrapper = RetryWrapper(inner, channel_context_getter=getter)
        child = wrapper.messages()
        assert isinstance(child, RetryWrapper)
        assert child._channel_context_getter is getter

    def test_retry_params_passed_to_child_wrappers(self):
        inner_result = MagicMock()
        inner_result.execute = MagicMock(return_value="ok")

        inner = MagicMock()
        inner.members.return_value = inner_result

        wrapper = RetryWrapper(inner, max_retries=5, retry_delay=10)
        child = wrapper.members()
        assert isinstance(child, RetryWrapper)
        assert child._max_retries == 5
        assert child._retry_delay == 10


# ---------------------------------------------------------------------------
# RetryWrapper — execute() happy path
# ---------------------------------------------------------------------------


class TestRetryWrapperExecuteSuccess:
    """Execute calls that succeed immediately."""

    @patch("slack_migrator.utils.api.time.sleep")
    def test_execute_returns_result_on_success(self, _sleep):
        inner = MagicMock()
        inner.execute.return_value = {"name": "spaces/abc"}

        wrapper = RetryWrapper(inner)
        result = wrapper.execute()
        assert result == {"name": "spaces/abc"}
        inner.execute.assert_called_once()

    @patch("slack_migrator.utils.api.time.sleep")
    def test_execute_uses_args_and_kwargs(self, _sleep):
        inner = MagicMock()
        inner.execute.return_value = "ok"

        wrapper = RetryWrapper(inner)
        result = wrapper.execute(num_retries=0)
        assert result == "ok"
        inner.execute.assert_called_once_with(num_retries=0)


# ---------------------------------------------------------------------------
# RetryWrapper — retry on retryable errors
# ---------------------------------------------------------------------------


class TestRetryWrapperRetries:
    """Execute calls that fail then succeed (retryable errors)."""

    @patch("slack_migrator.utils.api.time.sleep")
    def test_retries_on_429_then_succeeds(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(429, "Too Many Requests"),
            {"ok": True},
        ]
        wrapper = RetryWrapper(inner)
        result = wrapper.execute()
        assert result == {"ok": True}
        assert inner.execute.call_count == 2
        mock_sleep.assert_called_once()

    @patch("slack_migrator.utils.api.time.sleep")
    def test_retries_on_500_then_succeeds(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(500, "Internal Server Error"),
            "ok",
        ]
        wrapper = RetryWrapper(inner)
        result = wrapper.execute()
        assert result == "ok"
        assert inner.execute.call_count == 2

    @patch("slack_migrator.utils.api.time.sleep")
    def test_retries_on_503_then_succeeds(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(503, "Service Unavailable"),
            "done",
        ]
        wrapper = RetryWrapper(inner)
        result = wrapper.execute()
        assert result == "done"
        assert inner.execute.call_count == 2

    @patch("slack_migrator.utils.api.time.sleep")
    def test_retries_on_generic_exception_then_succeeds(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = [
            ConnectionError("network"),
            "recovered",
        ]
        wrapper = RetryWrapper(inner)
        result = wrapper.execute()
        assert result == "recovered"
        assert inner.execute.call_count == 2

    @patch("slack_migrator.utils.api.time.sleep")
    def test_retries_on_attribute_error_create(self, mock_sleep):
        """AttributeError mentioning 'create' is retried."""
        inner = MagicMock()
        inner.execute.side_effect = [
            AttributeError("Resource object has no attribute 'create'"),
            "ok",
        ]
        wrapper = RetryWrapper(inner)
        result = wrapper.execute()
        assert result == "ok"
        assert inner.execute.call_count == 2


# ---------------------------------------------------------------------------
# RetryWrapper — non-retryable errors
# ---------------------------------------------------------------------------


class TestRetryWrapperNonRetryable:
    """Errors that should NOT be retried."""

    @patch("slack_migrator.utils.api.time.sleep")
    def test_400_not_retried(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(400, "Bad Request")

        wrapper = RetryWrapper(inner)
        with pytest.raises(HttpError):
            wrapper.execute()
        inner.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("slack_migrator.utils.api.time.sleep")
    def test_403_not_retried(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(403, "Forbidden")

        wrapper = RetryWrapper(inner)
        with pytest.raises(HttpError):
            wrapper.execute()
        inner.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("slack_migrator.utils.api.time.sleep")
    def test_404_not_retried(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(404, "Not Found")

        wrapper = RetryWrapper(inner)
        with pytest.raises(HttpError):
            wrapper.execute()
        inner.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("slack_migrator.utils.api.time.sleep")
    def test_attribute_error_without_create_not_retried(self, mock_sleep):
        """AttributeError NOT mentioning 'create' is raised immediately."""
        inner = MagicMock()
        inner.execute.side_effect = AttributeError("no attribute 'foobar'")

        wrapper = RetryWrapper(inner)
        with pytest.raises(AttributeError):
            wrapper.execute()
        inner.execute.assert_called_once()
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# RetryWrapper — max retries exhaustion
# ---------------------------------------------------------------------------


class TestRetryWrapperMaxRetries:
    """Verify the wrapper gives up after max_retries."""

    @patch("slack_migrator.utils.api.time.sleep")
    def test_raises_after_default_max_retries(self, mock_sleep):
        """Default max_retries is 3 → 4 total attempts (0..3)."""
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(500, "Server Error")

        wrapper = RetryWrapper(inner)
        with pytest.raises(HttpError):
            wrapper.execute()
        # initial + 3 retries = 4 calls
        assert inner.execute.call_count == 4

    @patch("slack_migrator.utils.api.time.sleep")
    def test_raises_after_custom_max_retries(self, mock_sleep):
        """Honour max_retries parameter."""
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(500, "Server Error")

        wrapper = RetryWrapper(inner, max_retries=1, retry_delay=1)
        with pytest.raises(HttpError):
            wrapper.execute()
        # initial + 1 retry = 2 calls
        assert inner.execute.call_count == 2

    @patch("slack_migrator.utils.api.time.sleep")
    def test_zero_retries_raises_immediately(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(500, "Server Error")

        wrapper = RetryWrapper(inner, max_retries=0, retry_delay=1)
        with pytest.raises(HttpError):
            wrapper.execute()
        assert inner.execute.call_count == 1
        mock_sleep.assert_not_called()

    @patch("slack_migrator.utils.api.time.sleep")
    def test_max_retries_exhaustion_generic_exception(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = RuntimeError("always fails")

        wrapper = RetryWrapper(inner, max_retries=2, retry_delay=1)
        with pytest.raises(RuntimeError, match="always fails"):
            wrapper.execute()
        assert inner.execute.call_count == 3

    @patch("slack_migrator.utils.api.time.sleep")
    def test_max_retries_exhaustion_attribute_error_create(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = AttributeError(
            "Resource object has no attribute 'create'"
        )

        wrapper = RetryWrapper(inner, max_retries=1, retry_delay=1)
        with pytest.raises(AttributeError):
            wrapper.execute()
        assert inner.execute.call_count == 2


# ---------------------------------------------------------------------------
# RetryWrapper — backoff timing
# ---------------------------------------------------------------------------


class TestRetryWrapperBackoff:
    """Verify exponential backoff sleep durations."""

    @patch("slack_migrator.utils.api.time.sleep")
    def test_backoff_increases_exponentially(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(500, "err"),
            _make_http_error(500, "err"),
            _make_http_error(500, "err"),
            "ok",
        ]
        # initial_delay=1, backoff_factor=2.0 → sleeps: 1, 2, 4
        wrapper = RetryWrapper(inner)
        result = wrapper.execute()
        assert result == "ok"
        sleep_values = [call.args[0] for call in mock_sleep.call_args_list]
        assert sleep_values == [1.0, 2.0, 4.0]

    @patch("slack_migrator.utils.api.time.sleep")
    def test_backoff_respects_custom_initial_delay(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(500, "err"),
            "ok",
        ]
        wrapper = RetryWrapper(inner, max_retries=3, retry_delay=5)
        result = wrapper.execute()
        assert result == "ok"
        # first attempt (attempt=0): delay * backoff^0 = 5 * 1 = 5
        mock_sleep.assert_called_once_with(5.0)

    @patch("slack_migrator.utils.api.time.sleep")
    def test_backoff_capped_at_max_delay(self, mock_sleep):
        """Sleep time should never exceed 60 seconds."""
        inner = MagicMock()
        # With initial_delay=30, backoff=2: attempt 0→30, attempt 1→60, attempt 2→60 (capped)
        inner.execute.side_effect = [
            _make_http_error(500, "err"),
            _make_http_error(500, "err"),
            _make_http_error(500, "err"),
            "ok",
        ]
        wrapper = RetryWrapper(inner, max_retries=3, retry_delay=30)
        result = wrapper.execute()
        assert result == "ok"
        sleep_values = [call.args[0] for call in mock_sleep.call_args_list]
        assert all(v <= 60 for v in sleep_values)


# ---------------------------------------------------------------------------
# RetryWrapper — channel context
# ---------------------------------------------------------------------------


class TestRetryWrapperChannelContext:
    """Verify channel context getter is invoked and tolerant of failures."""

    @patch("slack_migrator.utils.api.time.sleep")
    def test_channel_context_getter_called(self, _sleep):
        getter = MagicMock(return_value="general")
        inner = MagicMock()
        inner.execute.return_value = "ok"

        wrapper = RetryWrapper(inner, channel_context_getter=getter)
        wrapper.execute()
        getter.assert_called()

    @patch("slack_migrator.utils.api.time.sleep")
    def test_channel_context_getter_exception_ignored(self, _sleep):
        """If the getter raises, the call should still succeed."""
        getter = MagicMock(side_effect=RuntimeError("boom"))
        inner = MagicMock()
        inner.execute.return_value = "ok"

        wrapper = RetryWrapper(inner, channel_context_getter=getter)
        result = wrapper.execute()
        assert result == "ok"

    @patch("slack_migrator.utils.api.time.sleep")
    def test_no_channel_context_getter(self, _sleep):
        """Works fine with no getter at all."""
        inner = MagicMock()
        inner.execute.return_value = "ok"
        wrapper = RetryWrapper(inner)
        assert wrapper.execute() == "ok"


# ---------------------------------------------------------------------------
# RetryWrapper — method chaining
# ---------------------------------------------------------------------------


class TestRetryWrapperChaining:
    """Verify that chained calls like service.spaces().messages().execute() work."""

    @patch("slack_migrator.utils.api.time.sleep")
    def test_deep_chain_execute(self, _sleep):
        """Simulate service.spaces().messages().list().execute()."""
        # Build the chain from inside out
        final_request = MagicMock()
        final_request.execute.return_value = {"messages": []}

        list_method = MagicMock()
        list_method.list.return_value = final_request

        messages_method = MagicMock()
        messages_method.messages.return_value = list_method

        spaces_method = MagicMock()
        spaces_method.spaces.return_value = messages_method

        wrapper = RetryWrapper(spaces_method)
        result = wrapper.spaces().messages().list().execute()
        assert result == {"messages": []}


# ---------------------------------------------------------------------------
# RetryWrapper — _extract_request_details
# ---------------------------------------------------------------------------


class TestExtractRequestDetails:
    """Tests for _extract_request_details."""

    def test_extracts_method_and_uri(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(
            method="POST", uri="https://chat.googleapis.com/v1/spaces"
        )

        wrapper = RetryWrapper(MagicMock())
        details = wrapper._extract_request_details(execute_method)
        assert details["method"] == "POST"
        assert details["uri"] == "https://chat.googleapis.com/v1/spaces"

    def test_extracts_body(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(
            method="POST",
            uri="https://example.com",
            body='{"key": "value"}',
        )

        wrapper = RetryWrapper(MagicMock())
        details = wrapper._extract_request_details(execute_method)
        assert details["body"] == '{"key": "value"}'

    def test_no_self_returns_none(self):
        execute_method = MagicMock(spec=[])
        # No __self__ attribute
        wrapper = RetryWrapper(MagicMock())
        details = wrapper._extract_request_details(execute_method)
        assert details is None

    def test_infers_post_from_create_in_uri(self):
        """When method is not set, infer POST from 'create' in URI."""
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(
            uri="https://example.com/v1/spaces/create",
        )

        wrapper = RetryWrapper(MagicMock())
        details = wrapper._extract_request_details(execute_method)
        assert details["method"] == "POST"

    def test_infers_get_from_list_in_uri(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(
            uri="https://example.com/v1/spaces/list",
        )

        wrapper = RetryWrapper(MagicMock())
        details = wrapper._extract_request_details(execute_method)
        assert details["method"] == "GET"

    def test_infers_delete_from_delete_in_uri(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(
            uri="https://example.com/v1/spaces/delete",
        )

        wrapper = RetryWrapper(MagicMock())
        details = wrapper._extract_request_details(execute_method)
        assert details["method"] == "DELETE"

    def test_infers_put_from_update_in_uri(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(
            uri="https://example.com/v1/spaces/update",
        )

        wrapper = RetryWrapper(MagicMock())
        details = wrapper._extract_request_details(execute_method)
        assert details["method"] == "PUT"

    def test_method_id_fallback(self):
        """Falls back to methodId when uri is not set."""
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(
            methodId="chat.spaces.messages.create",
        )

        wrapper = RetryWrapper(MagicMock())
        details = wrapper._extract_request_details(execute_method)
        assert "chat/spaces/messages/create" in details["uri"]

    def test_returns_fallback_on_exception(self):
        """If extraction fails entirely, return a minimal dict."""
        execute_method = MagicMock()
        # Make __self__ raise on any attribute access
        bad_self = MagicMock()
        bad_self.method = property(lambda self: (_ for _ in ()).throw(RuntimeError))
        execute_method.__self__ = bad_self

        wrapper = RetryWrapper(MagicMock())
        # Should not raise, should return something
        details = wrapper._extract_request_details(execute_method)
        assert details is not None
        assert "method" in details
        assert "uri" in details


# ---------------------------------------------------------------------------
# RetryWrapper — _extract_status_code
# ---------------------------------------------------------------------------


class TestExtractStatusCode:
    """Tests for _extract_status_code."""

    def test_extracts_from_response_attribute(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(_response=SimpleNamespace(status=201))

        wrapper = RetryWrapper(MagicMock())
        status = wrapper._extract_status_code(execute_method, {})
        assert status == 201

    def test_extracts_from_status_code_attribute(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(
            _response=SimpleNamespace(status_code=204)
        )

        wrapper = RetryWrapper(MagicMock())
        status = wrapper._extract_status_code(execute_method, {})
        assert status == 204

    def test_extracts_from_result_dict(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace()

        wrapper = RetryWrapper(MagicMock())
        status = wrapper._extract_status_code(execute_method, {"status": 200})
        assert status == 200

    def test_extracts_from_result_dict_string_status(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace()

        wrapper = RetryWrapper(MagicMock())
        status = wrapper._extract_status_code(execute_method, {"status": "201"})
        assert status == 201

    def test_infers_201_for_post(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(method="POST")

        wrapper = RetryWrapper(MagicMock())
        status = wrapper._extract_status_code(execute_method, {})
        assert status == 201

    def test_infers_204_for_delete(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(method="DELETE")

        wrapper = RetryWrapper(MagicMock())
        status = wrapper._extract_status_code(execute_method, {})
        assert status == 204

    def test_infers_200_for_get(self):
        execute_method = MagicMock()
        execute_method.__self__ = SimpleNamespace(method="GET")

        wrapper = RetryWrapper(MagicMock())
        status = wrapper._extract_status_code(execute_method, {})
        assert status == 200

    def test_fallback_returns_200(self):
        """When nothing can be extracted, return 200."""
        execute_method = MagicMock(spec=[])

        wrapper = RetryWrapper(MagicMock())
        status = wrapper._extract_status_code(execute_method, None)
        assert status == 200

    def test_extracts_from_httplib2_response_tuple(self):
        execute_method = MagicMock()
        resp_obj = SimpleNamespace(status=202)
        execute_method.__self__ = SimpleNamespace(response=(resp_obj, b"content"))

        wrapper = RetryWrapper(MagicMock())
        status = wrapper._extract_status_code(execute_method, {})
        assert status == 202


# ---------------------------------------------------------------------------
# RetryWrapper — API logging (debug mode)
# ---------------------------------------------------------------------------


class TestRetryWrapperApiLogging:
    """Verify _log_api_request / _log_api_response behaviour."""

    @patch("slack_migrator.utils.api.time.sleep")
    def test_log_api_request_called_when_debug_enabled(self, _sleep):
        """When debug API is enabled, _log_api_request should be called."""
        inner = MagicMock()
        inner.execute.return_value = "ok"
        inner.execute.__self__ = SimpleNamespace(
            method="GET", uri="https://example.com"
        )

        wrapper = RetryWrapper(inner)
        with (
            patch.object(wrapper, "_log_api_request") as mock_log_req,
            patch.object(wrapper, "_log_api_response"),
        ):
            wrapper.execute()
            mock_log_req.assert_called_once()

    @patch("slack_migrator.utils.api.time.sleep")
    def test_log_api_response_called_on_success(self, _sleep):
        """Response logging is called on successful execute."""
        inner = MagicMock()
        inner.execute.return_value = {"name": "spaces/123"}
        inner.execute.__self__ = SimpleNamespace(
            method="POST", uri="https://example.com"
        )

        wrapper = RetryWrapper(inner)
        with (
            patch.object(wrapper, "_log_api_request"),
            patch.object(wrapper, "_log_api_response") as mock_log_resp,
        ):
            wrapper.execute()
            mock_log_resp.assert_called_once()

    def test_log_api_request_no_crash_when_debug_disabled(self):
        """_log_api_request silently returns when debug is disabled."""
        wrapper = RetryWrapper(MagicMock())
        with patch(
            "slack_migrator.utils.logging.is_debug_api_enabled", return_value=False
        ):
            # Should not raise
            wrapper._log_api_request(
                {"method": "GET", "uri": "https://x.com", "body": None}, None
            )

    def test_log_api_request_handles_json_body(self):
        """_log_api_request handles JSON string body."""
        wrapper = RetryWrapper(MagicMock())
        with (
            patch(
                "slack_migrator.utils.logging.is_debug_api_enabled", return_value=True
            ),
            patch("slack_migrator.utils.logging.log_api_request") as mock_log,
        ):
            wrapper._log_api_request(
                {
                    "method": "POST",
                    "uri": "https://x.com",
                    "body": '{"key": "val"}',
                },
                "general",
            )
            mock_log.assert_called_once()
            call_kwargs = mock_log.call_args
            assert call_kwargs[1]["data"] == {"key": "val"}

    def test_log_api_request_handles_dict_body(self):
        wrapper = RetryWrapper(MagicMock())
        with (
            patch(
                "slack_migrator.utils.logging.is_debug_api_enabled", return_value=True
            ),
            patch("slack_migrator.utils.logging.log_api_request") as mock_log,
        ):
            wrapper._log_api_request(
                {
                    "method": "POST",
                    "uri": "https://x.com",
                    "body": {"key": "val"},
                },
                None,
            )
            mock_log.assert_called_once()
            call_kwargs = mock_log.call_args
            assert call_kwargs[1]["data"] == {"key": "val"}

    def test_log_api_request_handles_invalid_json_body(self):
        wrapper = RetryWrapper(MagicMock())
        with (
            patch(
                "slack_migrator.utils.logging.is_debug_api_enabled", return_value=True
            ),
            patch("slack_migrator.utils.logging.log_api_request") as mock_log,
        ):
            wrapper._log_api_request(
                {
                    "method": "POST",
                    "uri": "https://x.com",
                    "body": "not-json{{{",
                },
                None,
            )
            mock_log.assert_called_once()
            call_kwargs = mock_log.call_args
            # Fallback to truncated string representation
            assert "body" in call_kwargs[1]["data"]

    def test_log_api_response_no_crash_when_debug_disabled(self):
        wrapper = RetryWrapper(MagicMock())
        with patch(
            "slack_migrator.utils.logging.is_debug_api_enabled", return_value=False
        ):
            wrapper._log_api_response(
                200,
                {"method": "GET", "uri": "https://x.com"},
                None,
                None,
            )

    def test_log_api_response_calls_log_function(self):
        wrapper = RetryWrapper(MagicMock())
        with (
            patch(
                "slack_migrator.utils.logging.is_debug_api_enabled", return_value=True
            ),
            patch("slack_migrator.utils.logging.log_api_response") as mock_log,
        ):
            wrapper._log_api_response(
                201,
                {"method": "POST", "uri": "https://x.com"},
                {"name": "spaces/123"},
                "general",
            )
            mock_log.assert_called_once_with(
                status_code=201,
                url="https://x.com",
                response_data={"name": "spaces/123"},
                channel="general",
            )


# ---------------------------------------------------------------------------
# get_gcp_service — credential loading and caching
# ---------------------------------------------------------------------------


class TestGetGcpService:
    """Tests for get_gcp_service()."""

    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        """Ensure the service cache is empty before and after each test."""
        _service_cache.clear()
        yield
        _service_cache.clear()

    @patch("slack_migrator.utils.api.build")
    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_creates_service_and_returns_retry_wrapper(self, mock_creds, mock_build):
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_delegated = MagicMock()
        mock_cred_instance.with_subject.return_value = mock_delegated
        mock_build.return_value = MagicMock()

        result = get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")

        assert isinstance(result, RetryWrapper)
        mock_creds.assert_called_once_with(
            "/path/creds.json",
            scopes=pytest.approx(
                [
                    "https://www.googleapis.com/auth/chat.import",
                    "https://www.googleapis.com/auth/chat.spaces",
                    "https://www.googleapis.com/auth/chat.messages",
                    "https://www.googleapis.com/auth/chat.spaces.readonly",
                    "https://www.googleapis.com/auth/chat.memberships.readonly",
                    "https://www.googleapis.com/auth/drive",
                ],
                abs=0,
            ),
        )
        mock_cred_instance.with_subject.assert_called_once_with("user@example.com")
        mock_build.assert_called_once_with(
            "chat", "v1", credentials=mock_delegated, cache_discovery=False
        )

    @patch("slack_migrator.utils.api.build")
    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_caches_service_by_key(self, mock_creds, mock_build):
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_cred_instance.with_subject.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        svc1 = get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")
        svc2 = get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")

        assert svc1 is svc2
        # build should only be called once — second call uses cache
        mock_build.assert_called_once()

    @patch("slack_migrator.utils.api.build")
    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_different_args_not_cached(self, mock_creds, mock_build):
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_cred_instance.with_subject.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        svc1 = get_gcp_service("/path/creds.json", "user1@example.com", "chat", "v1")
        svc2 = get_gcp_service("/path/creds.json", "user2@example.com", "chat", "v1")

        assert svc1 is not svc2
        assert mock_build.call_count == 2

    @patch("slack_migrator.utils.api.build")
    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_different_api_not_cached(self, mock_creds, mock_build):
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_cred_instance.with_subject.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        svc1 = get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")
        svc2 = get_gcp_service("/path/creds.json", "user@example.com", "drive", "v3")

        assert svc1 is not svc2
        assert mock_build.call_count == 2

    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_file_not_found_error(self, mock_creds):
        mock_creds.side_effect = FileNotFoundError("no such file")

        with pytest.raises(FileNotFoundError, match="Credential file not found"):
            get_gcp_service("/bad/path.json", "user@example.com", "chat", "v1")

    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_invalid_credential_file(self, mock_creds):
        mock_creds.side_effect = ValueError("bad json")

        with pytest.raises(ValueError, match="Invalid credential file format"):
            get_gcp_service("/bad/creds.json", "user@example.com", "chat", "v1")

    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_json_decode_error(self, mock_creds):
        mock_creds.side_effect = json.JSONDecodeError("err", "doc", 0)

        with pytest.raises(ValueError, match="Invalid credential file format"):
            get_gcp_service("/bad/creds.json", "user@example.com", "chat", "v1")

    @patch("slack_migrator.utils.api.build")
    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_build_failure_propagates(self, mock_creds, mock_build):
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_cred_instance.with_subject.return_value = MagicMock()
        mock_build.side_effect = Exception("discovery failed")

        with pytest.raises(Exception, match="discovery failed"):
            get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")

    @patch("slack_migrator.utils.api.build")
    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_passes_retry_params_to_wrapper(self, mock_creds, mock_build):
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_cred_instance.with_subject.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        result = get_gcp_service(
            "/path/creds.json",
            "user@example.com",
            "chat",
            "v1",
            max_retries=7,
            retry_delay=5,
        )
        assert isinstance(result, RetryWrapper)
        assert result._max_retries == 7
        assert result._retry_delay == 5

    @patch("slack_migrator.utils.api.build")
    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_channel_context_set_in_wrapper(self, mock_creds, mock_build):
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_cred_instance.with_subject.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        result = get_gcp_service(
            "/path/creds.json", "user@example.com", "chat", "v1", channel="general"
        )
        assert isinstance(result, RetryWrapper)
        # The channel context getter should return the channel passed
        assert result._channel_context_getter() == "general"

    @patch("slack_migrator.utils.api.build")
    @patch(
        "slack_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_no_channel_produces_none_context(self, mock_creds, mock_build):
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_cred_instance.with_subject.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        result = get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")
        assert isinstance(result, RetryWrapper)
        # channel defaults to None
        assert result._channel_context_getter() is None


# ---------------------------------------------------------------------------
# escape_drive_query_value
# ---------------------------------------------------------------------------


class TestEscapeDriveQueryValue:
    """Tests for escape_drive_query_value()."""

    def test_clean_name_passthrough(self):
        assert escape_drive_query_value("general") == "general"

    def test_single_quote_escaped(self):
        assert escape_drive_query_value("team's channel") == "team\\'s channel"

    def test_backslash_escaped(self):
        assert escape_drive_query_value("path\\folder") == "path\\\\folder"

    def test_combined_backslash_and_quote(self):
        result = escape_drive_query_value("it\\'s")
        assert result == "it\\\\\\'s"

    def test_empty_string(self):
        assert escape_drive_query_value("") == ""

    def test_multiple_quotes(self):
        assert escape_drive_query_value("a'b'c") == "a\\'b\\'c"

    def test_no_mutation_of_safe_characters(self):
        safe = "channel-name_123 (archived)"
        assert escape_drive_query_value(safe) == safe
