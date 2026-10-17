"""Unit tests for the API utilities module."""

import copy
import datetime
import email.utils
import http.client
import json
import socket
import ssl
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httplib2
import pytest
from googleapiclient.discovery import Resource
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest, HttpRequest

from slack_chat_migrator.utils.api import (
    CUSTOM_EMOJI_SCOPE,
    MAX_BACKOFF_SECONDS,
    RATE_LIMIT_BUDGET_SECONDS,
    RATE_LIMIT_MAX_ATTEMPTS,
    REQUIRED_SCOPES,
    SERVICE_CACHE_MAX_ENTRIES,
    RetryWrapper,
    _credentials_cache,
    _service_cache,
    clear_service_cache,
    escape_drive_query_value,
    get_gcp_service,
    http_error_reasons,
    is_rate_limit_error,
    retry_after_seconds,
    scopes_for,
    slack_ts_to_rfc3339,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_http_error(
    status: int,
    reason: str = "error",
    content: bytes = b"error body",
    headers: dict[str, str] | None = None,
) -> HttpError:
    """Create an HttpError with the given status code."""
    resp = httplib2.Response({"status": status, **(headers or {})})
    resp.reason = reason
    return HttpError(resp, content)


def _drive_403(reason: str) -> HttpError:
    """A Drive-style 403 with an ``error.errors[].reason`` body."""
    body = {
        "error": {
            "code": 403,
            "message": reason,
            "errors": [{"domain": "usageLimits", "reason": reason, "message": reason}],
        }
    }
    return _make_http_error(403, "Forbidden", json.dumps(body).encode())


def _typed_mock(cls: type) -> MagicMock:
    """A MagicMock that passes ``isinstance(mock, cls)``."""
    mock = MagicMock()
    mock.__class__ = cls
    return mock


def _max_jitter(low: float, high: float) -> float:
    """Deterministic stand-in for random.uniform: the top of the range."""
    return high


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

    def test_integer_only_timestamp(self):
        """Dotless timestamp (no microseconds) should not crash."""
        result = slack_ts_to_rfc3339("1609459200")
        assert result == "2021-01-01T00:00:00.000000Z"


# ---------------------------------------------------------------------------
# RetryWrapper — basic attribute delegation
# ---------------------------------------------------------------------------


class TestRetryWrapperDelegation:
    """Verify RetryWrapper transparently proxies attributes."""

    def test_non_callable_attribute_is_returned_directly(self):
        inner = SimpleNamespace(value=42)
        wrapper = RetryWrapper(inner)
        assert wrapper.value == 42

    @pytest.mark.parametrize("cls", [Resource, HttpRequest, BatchHttpRequest])
    def test_googleapiclient_results_are_wrapped(self, cls):
        """Resources and requests are wrapped by type, not attribute names."""
        inner = MagicMock()
        inner.spaces.return_value = _typed_mock(cls)

        wrapper = RetryWrapper(inner)
        result = wrapper.spaces()
        assert isinstance(result, RetryWrapper)

    def test_media_resource_is_wrapped(self):
        """chat.media() has no execute/list/create but is still wrapped."""
        media = _typed_mock(Resource)
        del media.execute, media.list, media.create
        inner = MagicMock()
        inner.media.return_value = media

        assert isinstance(RetryWrapper(inner).media(), RetryWrapper)

    def test_other_objects_with_execute_not_wrapped(self):
        """Duck-typed objects are not mistaken for API requests."""
        inner = MagicMock()
        inner.helper.return_value = SimpleNamespace(execute=lambda: "x")

        assert not isinstance(RetryWrapper(inner).helper(), RetryWrapper)

    def test_callable_returning_plain_value_not_wrapped(self):
        """Methods returning plain values are not wrapped."""
        inner = MagicMock()
        inner.plain_method.return_value = "plain_string"

        wrapper = RetryWrapper(inner)
        result = wrapper.plain_method()
        assert result == "plain_string"

    def test_channel_context_getter_passed_to_child_wrappers(self):
        """Child RetryWrappers inherit the channel_context_getter."""
        inner_result = _typed_mock(HttpRequest)
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
        inner_result = _typed_mock(Resource)
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

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_execute_returns_result_on_success(self, _sleep):
        inner = MagicMock()
        inner.execute.return_value = {"name": "spaces/abc"}

        wrapper = RetryWrapper(inner)
        result = wrapper.execute()
        assert result == {"name": "spaces/abc"}
        inner.execute.assert_called_once()

    @patch("slack_chat_migrator.utils.api.time.sleep")
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

    @patch("slack_chat_migrator.utils.api.time.sleep")
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

    @patch("slack_chat_migrator.utils.api.time.sleep")
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

    @patch("slack_chat_migrator.utils.api.time.sleep")
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

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_retries_on_connection_error_then_succeeds(self, mock_sleep):
        """ConnectionError (subclass of OSError) is retried as transient."""
        inner = MagicMock()
        inner.execute.side_effect = [
            ConnectionError("network"),
            "recovered",
        ]
        wrapper = RetryWrapper(inner)
        result = wrapper.execute()
        assert result == "recovered"
        assert inner.execute.call_count == 2

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_retries_on_transport_error_then_succeeds(self, mock_sleep):
        """TransportError is retried as a transient network error."""
        from google.auth.exceptions import TransportError

        inner = MagicMock()
        inner.execute.side_effect = [
            TransportError("connection reset"),
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

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_400_not_retried(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(400, "Bad Request")

        wrapper = RetryWrapper(inner)
        with pytest.raises(HttpError):
            wrapper.execute()
        inner.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_403_not_retried(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(403, "Forbidden")

        wrapper = RetryWrapper(inner)
        with pytest.raises(HttpError):
            wrapper.execute()
        inner.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_404_not_retried(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(404, "Not Found")

        wrapper = RetryWrapper(inner)
        with pytest.raises(HttpError):
            wrapper.execute()
        inner.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_attribute_error_not_retried(self, mock_sleep):
        """AttributeError is not a transient error and should not be retried."""
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

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_raises_after_default_max_retries(self, mock_sleep):
        """Default max_retries is 3 → 4 total attempts (0..3)."""
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(500, "Server Error")

        wrapper = RetryWrapper(inner)
        with pytest.raises(HttpError):
            wrapper.execute()
        # initial + 3 retries = 4 calls
        assert inner.execute.call_count == 4

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_raises_after_custom_max_retries(self, mock_sleep):
        """Honour max_retries parameter."""
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(500, "Server Error")

        wrapper = RetryWrapper(inner, max_retries=1, retry_delay=1)
        with pytest.raises(HttpError):
            wrapper.execute()
        # initial + 1 retry = 2 calls
        assert inner.execute.call_count == 2

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_zero_retries_raises_immediately(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(500, "Server Error")

        wrapper = RetryWrapper(inner, max_retries=0, retry_delay=1)
        with pytest.raises(HttpError):
            wrapper.execute()
        assert inner.execute.call_count == 1
        mock_sleep.assert_not_called()

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_non_transient_exception_not_retried(self, mock_sleep):
        """Non-transient exceptions (e.g. RuntimeError) are raised immediately."""
        inner = MagicMock()
        inner.execute.side_effect = RuntimeError("always fails")

        wrapper = RetryWrapper(inner, max_retries=2, retry_delay=1)
        with pytest.raises(RuntimeError, match="always fails"):
            wrapper.execute()
        assert inner.execute.call_count == 1
        mock_sleep.assert_not_called()

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_max_retries_exhaustion_transport_error(self, mock_sleep):
        from google.auth.exceptions import TransportError

        inner = MagicMock()
        inner.execute.side_effect = TransportError("connection reset")

        wrapper = RetryWrapper(inner, max_retries=1, retry_delay=1)
        with pytest.raises(TransportError):
            wrapper.execute()
        assert inner.execute.call_count == 2


# ---------------------------------------------------------------------------
# RetryWrapper — backoff timing
# ---------------------------------------------------------------------------


class TestRetryWrapperBackoff:
    """Verify full-jitter exponential backoff."""

    @patch("slack_chat_migrator.utils.api.random.uniform", side_effect=_max_jitter)
    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_backoff_ceiling_doubles(self, mock_sleep, _uniform):
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(500, "err"),
            _make_http_error(500, "err"),
            _make_http_error(500, "err"),
            "ok",
        ]
        # retry_delay=1 → jitter ceilings 1, 2, 4
        wrapper = RetryWrapper(inner)
        result = wrapper.execute()
        assert result == "ok"
        sleep_values = [call.args[0] for call in mock_sleep.call_args_list]
        assert sleep_values == [1.0, 2.0, 4.0]

    @patch("slack_chat_migrator.utils.api.random.uniform", side_effect=_max_jitter)
    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_backoff_respects_custom_initial_delay(self, mock_sleep, _uniform):
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(500, "err"),
            "ok",
        ]
        wrapper = RetryWrapper(inner, max_retries=3, retry_delay=5)
        result = wrapper.execute()
        assert result == "ok"
        mock_sleep.assert_called_once_with(5.0)

    @patch("slack_chat_migrator.utils.api.random.uniform", side_effect=_max_jitter)
    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_backoff_capped_at_max_delay(self, mock_sleep, _uniform):
        """No single sleep exceeds 64 seconds."""
        inner = MagicMock()
        inner.execute.side_effect = [_make_http_error(500, "err")] * 3 + ["ok"]
        wrapper = RetryWrapper(inner, max_retries=3, retry_delay=30)
        assert wrapper.execute() == "ok"
        sleep_values = [call.args[0] for call in mock_sleep.call_args_list]
        assert sleep_values == [30.0, 60.0, 64.0]

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_jitter_applied(self, mock_sleep):
        """Sleeps are drawn from uniform(0, ceiling), not fixed values."""
        with patch(
            "slack_chat_migrator.utils.api.random.uniform", return_value=0.25
        ) as uniform:
            inner = MagicMock()
            inner.execute.side_effect = [_make_http_error(503, "err"), "ok"]
            assert RetryWrapper(inner, retry_delay=2).execute() == "ok"
        uniform.assert_called_once_with(0, 2.0)
        mock_sleep.assert_called_once_with(0.25)


# ---------------------------------------------------------------------------
# RetryWrapper — channel context
# ---------------------------------------------------------------------------


class TestRetryWrapperChannelContext:
    """Verify channel context getter is invoked and tolerant of failures."""

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_channel_context_getter_called(self, _sleep):
        getter = MagicMock(return_value="general")
        inner = MagicMock()
        inner.execute.return_value = "ok"

        wrapper = RetryWrapper(inner, channel_context_getter=getter)
        wrapper.execute()
        getter.assert_called()

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_channel_context_getter_exception_ignored(self, _sleep):
        """If the getter raises, the call should still succeed."""
        getter = MagicMock(side_effect=RuntimeError("boom"))
        inner = MagicMock()
        inner.execute.return_value = "ok"

        wrapper = RetryWrapper(inner, channel_context_getter=getter)
        result = wrapper.execute()
        assert result == "ok"

    @patch("slack_chat_migrator.utils.api.time.sleep")
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

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_deep_chain_execute(self, _sleep):
        """Simulate service.spaces().messages().list().execute() with a retry."""
        # Build the chain from inside out
        final_request = _typed_mock(HttpRequest)
        final_request.execute.side_effect = [
            _make_http_error(503, "Unavailable"),
            {"messages": []},
        ]

        list_method = _typed_mock(Resource)
        list_method.list.return_value = final_request

        messages_method = _typed_mock(Resource)
        messages_method.messages.return_value = list_method

        spaces_method = MagicMock()
        spaces_method.spaces.return_value = messages_method

        wrapper = RetryWrapper(spaces_method)
        result = wrapper.spaces().messages().list().execute()
        assert result == {"messages": []}
        assert final_request.execute.call_count == 2


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
        assert isinstance(details, dict)
        assert set(details.keys()) == {"method", "uri", "body"}


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

    @patch("slack_chat_migrator.utils.api.time.sleep")
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

    @patch("slack_chat_migrator.utils.api.time.sleep")
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
            "slack_chat_migrator.utils.logging.is_debug_api_enabled", return_value=False
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
                "slack_chat_migrator.utils.logging.is_debug_api_enabled",
                return_value=True,
            ),
            patch("slack_chat_migrator.utils.logging.log_api_request") as mock_log,
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
                "slack_chat_migrator.utils.logging.is_debug_api_enabled",
                return_value=True,
            ),
            patch("slack_chat_migrator.utils.logging.log_api_request") as mock_log,
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
                "slack_chat_migrator.utils.logging.is_debug_api_enabled",
                return_value=True,
            ),
            patch("slack_chat_migrator.utils.logging.log_api_request") as mock_log,
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
            "slack_chat_migrator.utils.logging.is_debug_api_enabled", return_value=False
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
                "slack_chat_migrator.utils.logging.is_debug_api_enabled",
                return_value=True,
            ),
            patch("slack_chat_migrator.utils.logging.log_api_response") as mock_log,
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
        clear_service_cache()
        yield
        clear_service_cache()

    @patch("slack_chat_migrator.utils.api.build")
    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
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

    @patch("slack_chat_migrator.utils.api.build")
    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
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

    @patch("slack_chat_migrator.utils.api.build")
    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
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

    @patch("slack_chat_migrator.utils.api.build")
    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
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
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_file_not_found_error(self, mock_creds):
        mock_creds.side_effect = FileNotFoundError("no such file")

        with pytest.raises(FileNotFoundError, match="Credential file not found"):
            get_gcp_service("/bad/path.json", "user@example.com", "chat", "v1")

    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_invalid_credential_file(self, mock_creds):
        mock_creds.side_effect = ValueError("bad json")

        with pytest.raises(ValueError, match="Invalid credential file format"):
            get_gcp_service("/bad/creds.json", "user@example.com", "chat", "v1")

    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_json_decode_error(self, mock_creds):
        mock_creds.side_effect = json.JSONDecodeError("err", "doc", 0)

        with pytest.raises(ValueError, match="Invalid credential file format"):
            get_gcp_service("/bad/creds.json", "user@example.com", "chat", "v1")

    @patch("slack_chat_migrator.utils.api.build")
    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_build_failure_propagates(self, mock_creds, mock_build):
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_cred_instance.with_subject.return_value = MagicMock()
        mock_build.side_effect = Exception("discovery failed")

        with pytest.raises(Exception, match="discovery failed"):
            get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")

    @patch("slack_chat_migrator.utils.api.build")
    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
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

    @patch("slack_chat_migrator.utils.api.build")
    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
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

    @patch("slack_chat_migrator.utils.api.build")
    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
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


# ---------------------------------------------------------------------------
# Service cache TTL and 401 eviction
# ---------------------------------------------------------------------------


class TestServiceCacheTTL:
    """Tests for service cache TTL expiry."""

    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        """Ensure the service cache is empty before and after each test."""
        clear_service_cache()
        yield
        clear_service_cache()

    @patch("slack_chat_migrator.utils.api.build")
    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_ttl_expiry_evicts_cached_service(self, mock_creds, mock_build):
        """A cached service older than _SERVICE_CACHE_TTL should be evicted."""
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_cred_instance.with_subject.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        svc1 = get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")
        assert mock_build.call_count == 1

        # Artificially age the cache entry past TTL
        cache_key = (
            "/path/creds.json",
            "user@example.com",
            "chat",
            "v1",
            tuple(REQUIRED_SCOPES),
        )
        old_service, _ = _service_cache[cache_key]
        _service_cache[cache_key] = (old_service, 0.0)  # epoch = very old

        svc2 = get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")
        # build should have been called a second time due to TTL eviction
        assert mock_build.call_count == 2
        assert svc1 is not svc2

    @patch("slack_chat_migrator.utils.api.build")
    @patch(
        "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"
    )
    def test_fresh_cache_entry_not_evicted(self, mock_creds, mock_build):
        """A cache entry within TTL should be returned without rebuilding."""
        mock_cred_instance = MagicMock()
        mock_creds.return_value = mock_cred_instance
        mock_cred_instance.with_subject.return_value = MagicMock()
        mock_build.return_value = MagicMock()

        svc1 = get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")
        svc2 = get_gcp_service("/path/creds.json", "user@example.com", "chat", "v1")

        assert svc1 is svc2
        assert mock_build.call_count == 1


class TestServiceCache401Eviction:
    """Tests for 401 error clearing the service cache."""

    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        clear_service_cache()
        yield
        clear_service_cache()

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_401_clears_service_cache(self, _sleep):
        """A 401 HttpError in _wrap_execute should clear the service cache."""
        # Pre-populate the cache with a fake entry
        _service_cache[("c", "u", "chat", "v1", ())] = (MagicMock(), 999999999.0)
        assert len(_service_cache) == 1

        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(401, "Unauthorized")

        wrapper = RetryWrapper(inner)
        with pytest.raises(HttpError):
            wrapper.execute()

        # Cache should have been cleared by the 401 handler
        assert len(_service_cache) == 0

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_403_does_not_clear_service_cache(self, _sleep):
        """A 403 HttpError should NOT clear the service cache."""
        _service_cache[("c", "u", "chat", "v1", ())] = (MagicMock(), 999999999.0)
        assert len(_service_cache) == 1

        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(403, "Forbidden")

        wrapper = RetryWrapper(inner)
        with pytest.raises(HttpError):
            wrapper.execute()

        # Cache should NOT have been cleared
        assert len(_service_cache) == 1


# ---------------------------------------------------------------------------
# Retry policy (issue #111)
# ---------------------------------------------------------------------------


class TestRetryClassification:
    """Which errors are retried."""

    @pytest.mark.parametrize(
        "reason",
        [
            "rateLimitExceeded",
            "userRateLimitExceeded",
            "sharingRateLimitExceeded",
            "quotaExceeded",
        ],
    )
    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_drive_rate_limit_403_retried(self, mock_sleep, reason):
        inner = MagicMock()
        inner.execute.side_effect = [_drive_403(reason), {"id": "f1"}]
        assert RetryWrapper(inner).execute() == {"id": "f1"}
        assert inner.execute.call_count == 2

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_rate_limit_reason_in_rpc_details(self, mock_sleep):
        body = {
            "error": {
                "code": 403,
                "status": "PERMISSION_DENIED",
                "details": [{"@type": "x", "reason": "rateLimitExceeded"}],
            }
        }
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(403, "Forbidden", json.dumps(body).encode()),
            "ok",
        ]
        assert RetryWrapper(inner).execute() == "ok"

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_plain_403_not_retried(self, mock_sleep):
        body = {"error": {"errors": [{"reason": "insufficientPermissions"}]}}
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(
            403, "Forbidden", json.dumps(body).encode()
        )
        with pytest.raises(HttpError):
            RetryWrapper(inner).execute()
        inner.execute.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_408_retried(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = [_make_http_error(408, "Timeout"), "ok"]
        assert RetryWrapper(inner).execute() == "ok"

    @pytest.mark.parametrize(
        "exc",
        [
            httplib2.ServerNotFoundError("Unable to find the server at chat"),
            http.client.IncompleteRead(b"partial"),
            http.client.RemoteDisconnected("closed"),
            TimeoutError("timed out"),
            socket.timeout("timed out"),
            ssl.SSLError("bad record mac"),
            ConnectionResetError("reset"),
        ],
    )
    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_network_errors_retried(self, mock_sleep, exc):
        inner = MagicMock()
        inner.execute.side_effect = [exc, "ok"]
        assert RetryWrapper(inner).execute() == "ok"
        assert inner.execute.call_count == 2

    @pytest.mark.parametrize(
        "exc", [PermissionError("denied"), FileNotFoundError("missing"), OSError(5)]
    )
    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_local_os_errors_not_retried(self, mock_sleep, exc):
        inner = MagicMock()
        inner.execute.side_effect = exc
        with pytest.raises(type(exc)):
            RetryWrapper(inner).execute()
        inner.execute.assert_called_once()
        mock_sleep.assert_not_called()

    def test_is_rate_limit_error(self):
        assert is_rate_limit_error(_make_http_error(429))
        assert is_rate_limit_error(_drive_403("userRateLimitExceeded"))
        assert not is_rate_limit_error(_make_http_error(403))
        assert not is_rate_limit_error(_make_http_error(503))
        assert not is_rate_limit_error(ValueError("x"))

    def test_http_error_reasons_tolerates_garbage(self):
        assert http_error_reasons(_make_http_error(403, content=b"\xff\xfe")) == set()
        assert http_error_reasons(_make_http_error(403, content=b"[]")) == set()


class TestRetryAfter:
    """Retry-After is honoured."""

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_retry_after_seconds_honoured(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(429, "Too Many", headers={"retry-after": "30"}),
            "ok",
        ]
        assert RetryWrapper(inner, max_retries=3, retry_delay=2).execute() == "ok"
        mock_sleep.assert_called_once_with(30.0)

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_retry_after_http_date_honoured(self, mock_sleep):
        when = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(
            seconds=20
        )
        header = email.utils.format_datetime(when, usegmt=True)
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(503, "Unavailable", headers={"retry-after": header}),
            "ok",
        ]
        assert RetryWrapper(inner).execute() == "ok"
        slept = mock_sleep.call_args.args[0]
        assert 15 <= slept <= 21

    def test_retry_after_parsing(self):
        assert retry_after_seconds(_make_http_error(429)) is None
        err = _make_http_error(429, headers={"retry-after": "nonsense"})
        assert retry_after_seconds(err) is None
        err = _make_http_error(429, headers={"retry-after": "-5"})
        assert retry_after_seconds(err) == 0.0
        past = "Wed, 21 Oct 2015 07:28:00 GMT"
        err = _make_http_error(429, headers={"retry-after": past})
        assert retry_after_seconds(err) == 0.0

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_server_error_retry_after_capped(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = [
            _make_http_error(503, "Unavailable", headers={"retry-after": "3600"}),
            "ok",
        ]
        assert RetryWrapper(inner).execute() == "ok"
        mock_sleep.assert_called_once_with(MAX_BACKOFF_SECONDS)


class TestRateLimitBudget:
    """Rate limits get a time budget independent of max_retries."""

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_rate_limits_retried_beyond_max_retries(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = [_make_http_error(429)] * 6 + ["ok"]
        assert RetryWrapper(inner, max_retries=1, retry_delay=2).execute() == "ok"
        assert inner.execute.call_count == 7

    @patch("slack_chat_migrator.utils.api.random.uniform", side_effect=_max_jitter)
    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_rate_limit_budget_exhausted(self, mock_sleep, _uniform):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(429)
        with pytest.raises(HttpError):
            RetryWrapper(inner, max_retries=3, retry_delay=2).execute()
        slept = [c.args[0] for c in mock_sleep.call_args_list]
        assert sum(slept) <= RATE_LIMIT_BUDGET_SECONDS
        assert max(slept) <= MAX_BACKOFF_SECONDS
        # 2+4+8+16+32+64+64+64 = 254; the next 64 would exceed 300 s
        assert slept == [2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 64.0, 64.0]

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_zero_retry_after_cannot_loop_forever(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(429, headers={"retry-after": "0"})
        with pytest.raises(HttpError):
            RetryWrapper(inner).execute()
        assert inner.execute.call_count == RATE_LIMIT_MAX_ATTEMPTS + 1

    @patch("slack_chat_migrator.utils.api.random.uniform", side_effect=_max_jitter)
    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_zero_retry_delay_still_backs_off_on_rate_limits(
        self, mock_sleep, _uniform
    ):
        inner = MagicMock()
        inner.execute.side_effect = [_make_http_error(429), "ok"]
        assert RetryWrapper(inner, retry_delay=0).execute() == "ok"
        mock_sleep.assert_called_once_with(1.0)

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_server_errors_limited_by_max_retries(self, mock_sleep):
        inner = MagicMock()
        inner.execute.side_effect = _make_http_error(503)
        with pytest.raises(HttpError):
            RetryWrapper(inner, max_retries=2).execute()
        assert inner.execute.call_count == 3


class TestRetryWrapperCopy:
    """copy.copy and attribute writes go through to the wrapped object."""

    def test_copy_does_not_recurse(self):
        inner = SimpleNamespace(uri="https://x/1")
        wrapper = RetryWrapper(inner, max_retries=4)
        clone = copy.copy(wrapper)
        assert isinstance(clone, RetryWrapper)
        assert clone._max_retries == 4
        clone.uri = "https://x/2"
        assert clone.uri == "https://x/2"
        assert wrapper.uri == "https://x/1"

    def test_deepcopy_does_not_recurse(self):
        wrapper = RetryWrapper(SimpleNamespace(value=[1]))
        clone = copy.deepcopy(wrapper)
        assert clone.value == [1]

    def test_setattr_forwards_to_wrapped(self):
        inner = SimpleNamespace(uri="a")
        wrapper = RetryWrapper(inner)
        wrapper.uri = "b"
        assert inner.uri == "b"

    def test_dunder_lookup_not_forwarded(self):
        wrapper = RetryWrapper(SimpleNamespace())
        assert not hasattr(wrapper, "__setstate__")


class TestRealDiscoveryService:
    """RetryWrapper around a real discovery-built Chat client (mocked HTTP)."""

    @staticmethod
    def _chat(responses):
        from googleapiclient.discovery import build
        from googleapiclient.http import HttpMockSequence

        http_mock = HttpMockSequence(
            [
                ({"status": str(status)}, json.dumps(body).encode())
                for status, body in responses
            ]
        )
        return RetryWrapper(build("chat", "v1", http=http_mock, static_discovery=True))

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_list_next_paginates(self, _sleep):
        chat = self._chat(
            [
                (503, {"error": {"code": 503}}),
                (200, {"spaces": [{"name": "spaces/A"}], "nextPageToken": "tok"}),
                (200, {"spaces": [{"name": "spaces/B"}]}),
            ]
        )
        request = chat.spaces().list(pageSize=1)
        page1 = request.execute()
        page2_request = chat.spaces().list_next(request, page1)
        assert isinstance(page2_request, RetryWrapper)
        assert "pageToken=tok" in page2_request.uri
        assert "pageToken" not in request.uri
        assert page2_request.execute() == {"spaces": [{"name": "spaces/B"}]}
        assert chat.spaces().list_next(page2_request, {"spaces": []}) is None

    @patch("slack_chat_migrator.utils.api.time.sleep")
    def test_media_upload_is_retried(self, mock_sleep):
        from googleapiclient.http import MediaInMemoryUpload

        from slack_chat_migrator.services.chat_adapter import ChatAdapter

        chat = self._chat(
            [
                (503, {"error": {"code": 503}}),
                (200, {"attachmentDataRef": {"resourceName": "ref"}}),
            ]
        )
        result = ChatAdapter(chat).upload_media(
            parent="spaces/A",
            media_body=MediaInMemoryUpload(b"hello", mimetype="text/plain"),
            body={"filename": "a.txt"},
        )
        assert result == {"attachmentDataRef": {"resourceName": "ref"}}
        mock_sleep.assert_called_once()


# ---------------------------------------------------------------------------
# Bounded caches, credential reuse and optional scopes (issue #119)
# ---------------------------------------------------------------------------

_FROM_FILE = "slack_chat_migrator.utils.api.service_account.Credentials.from_service_account_file"


class TestBoundedServiceCache:
    """The per-user client cache is an LRU with a fixed size."""

    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        clear_service_cache()
        yield
        clear_service_cache()

    @patch("slack_chat_migrator.utils.api.build")
    @patch(_FROM_FILE)
    def test_cache_is_bounded(self, mock_creds, mock_build):
        mock_build.side_effect = lambda *a, **k: MagicMock()
        for i in range(SERVICE_CACHE_MAX_ENTRIES + 50):
            get_gcp_service("/c.json", f"u{i}@example.com", "chat", "v1")
        assert len(_service_cache) == SERVICE_CACHE_MAX_ENTRIES
        # The oldest entries were evicted, the newest kept
        users = {key[1] for key in _service_cache}
        assert "u0@example.com" not in users
        assert f"u{SERVICE_CACHE_MAX_ENTRIES + 49}@example.com" in users

    @patch("slack_chat_migrator.utils.api.build")
    @patch(_FROM_FILE)
    def test_recently_used_entry_survives_eviction(self, mock_creds, mock_build):
        mock_build.side_effect = lambda *a, **k: MagicMock()
        first = get_gcp_service("/c.json", "keep@example.com", "chat", "v1")
        for i in range(SERVICE_CACHE_MAX_ENTRIES - 1):
            get_gcp_service("/c.json", f"u{i}@example.com", "chat", "v1")
        # Touch the first entry, then overflow the cache
        assert get_gcp_service("/c.json", "keep@example.com", "chat", "v1") is first
        get_gcp_service("/c.json", "new@example.com", "chat", "v1")
        assert get_gcp_service("/c.json", "keep@example.com", "chat", "v1") is first
        users = {key[1] for key in _service_cache}
        assert "u0@example.com" not in users

    @patch("slack_chat_migrator.utils.api.build")
    @patch(_FROM_FILE)
    def test_credentials_loaded_once_per_file(self, mock_creds, mock_build):
        base = mock_creds.return_value
        get_gcp_service("/c.json", "a@example.com", "chat", "v1")
        get_gcp_service("/c.json", "b@example.com", "chat", "v1")
        get_gcp_service("/c.json", "a@example.com", "drive", "v3")
        mock_creds.assert_called_once()
        assert base.with_subject.call_count == 3

    @patch("slack_chat_migrator.utils.api.build")
    @patch(_FROM_FILE)
    def test_clear_service_cache_clears_credentials(self, mock_creds, mock_build):
        get_gcp_service("/c.json", "a@example.com", "chat", "v1")
        clear_service_cache()
        assert not _service_cache and not _credentials_cache
        get_gcp_service("/c.json", "a@example.com", "chat", "v1")
        assert mock_creds.call_count == 2
        assert mock_build.call_count == 2


class TestOptionalScopes:
    """scopes_for() adds optional scopes only when the config needs them."""

    @pytest.fixture(autouse=True)
    def _clear_cache(self):
        clear_service_cache()
        yield
        clear_service_cache()

    def test_default_scopes_unchanged(self):
        from slack_chat_migrator.core.config import MigrationConfig

        assert scopes_for(MigrationConfig()) == REQUIRED_SCOPES
        assert scopes_for(None) == REQUIRED_SCOPES
        assert CUSTOM_EMOJI_SCOPE not in REQUIRED_SCOPES

    def test_custom_emoji_scope_added(self):
        from slack_chat_migrator.core.config import MigrationConfig

        scopes = scopes_for(MigrationConfig(use_chat_custom_emoji=True))
        assert scopes == [*REQUIRED_SCOPES, CUSTOM_EMOJI_SCOPE]

    def test_scopes_for_returns_a_copy(self):
        scopes_for(None).append("x")
        assert "x" not in REQUIRED_SCOPES

    @patch("slack_chat_migrator.utils.api.build")
    @patch(_FROM_FILE)
    def test_get_gcp_service_requests_given_scopes(self, mock_creds, mock_build):
        scopes = [*REQUIRED_SCOPES, CUSTOM_EMOJI_SCOPE]
        default = get_gcp_service("/c.json", "a@example.com", "chat", "v1")
        extended = get_gcp_service(
            "/c.json", "a@example.com", "chat", "v1", scopes=scopes
        )
        assert default is not extended
        requested = [c.kwargs["scopes"] for c in mock_creds.call_args_list]
        assert requested == [REQUIRED_SCOPES, scopes]
