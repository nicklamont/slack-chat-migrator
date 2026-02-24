"""Unit tests for the logging module."""

import json
import logging
import os
from unittest.mock import patch

import pytest

import slack_migrator.utils.logging as log_module
from slack_migrator.utils.logging import (
    EnhancedFormatter,
    JsonFormatter,
    _extract_api_operation,
    ensure_channel_log_created,
    get_logger,
    is_debug_api_enabled,
    log_api_request,
    log_api_response,
    log_failed_message,
    log_with_context,
    setup_channel_logger,
    setup_logger,
    setup_main_log_file,
)


@pytest.fixture(autouse=True)
def _clean_logger():
    """Remove all handlers from the slack_migrator logger before and after each test."""
    logger = logging.getLogger("slack_migrator")
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    yield
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    # Reset the module-level debug flag
    log_module._DEBUG_API_ENABLED = False


# --- JsonFormatter tests ---


class TestJsonFormatter:
    """Tests for JsonFormatter."""

    def test_basic_format_contains_required_keys(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="test message",
            args=(),
            exc_info=None,
        )
        result = json.loads(formatter.format(record))
        assert result["level"] == "INFO"
        assert result["message"] == "test message"
        assert result["module"] == "test"
        assert "time" in result

    def test_excludes_standard_record_attributes(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        result = json.loads(formatter.format(record))
        # Standard attributes that should be excluded from the extra data
        for key in ("args", "exc_info", "lineno", "pathname", "funcName"):
            assert key not in result

    def test_includes_extra_attributes(self):
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="hello",
            args=(),
            exc_info=None,
        )
        record.channel = "general"
        record.custom_field = "custom_value"
        result = json.loads(formatter.format(record))
        assert result["channel"] == "general"
        assert result["custom_field"] == "custom_value"


# --- EnhancedFormatter tests ---


class TestEnhancedFormatter:
    """Tests for EnhancedFormatter."""

    def _make_record(
        self, msg="test message", level=logging.INFO, name="slack_migrator"
    ):
        return logging.LogRecord(
            name=name,
            level=level,
            pathname="test.py",
            lineno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )

    def test_default_format(self):
        formatter = EnhancedFormatter()
        record = self._make_record()
        result = formatter.format(record)
        assert "INFO" in result
        assert "test message" in result

    def test_verbose_format_includes_module_and_line(self):
        formatter = EnhancedFormatter(verbose=True)
        record = self._make_record()
        result = formatter.format(record)
        # Verbose format includes module and lineno
        assert "test" in result  # module name
        assert ":1]" in result  # line number

    def test_custom_fmt_used_when_not_verbose(self):
        formatter = EnhancedFormatter(fmt="%(levelname)s|%(message)s")
        record = self._make_record()
        result = formatter.format(record)
        assert result == "INFO|test message"

    def test_verbose_overrides_custom_fmt(self):
        formatter = EnhancedFormatter(fmt="%(message)s", verbose=True)
        record = self._make_record()
        result = formatter.format(record)
        # When verbose=True, the verbose format overrides the supplied fmt
        assert "slack_migrator" in result
        assert ":1]" in result

    def test_api_details_not_included_by_default(self):
        formatter = EnhancedFormatter()
        record = self._make_record()
        record.api_data = '{"key": "value"}'
        record.response = '{"status": "ok"}'
        result = formatter.format(record)
        assert "--- API Request Data ---" not in result
        assert "--- API Response Data ---" not in result

    def test_api_details_included_when_enabled(self):
        formatter = EnhancedFormatter(include_api_details=True)
        record = self._make_record()
        record.api_data = '{"key": "value"}'
        record.response = '{"status": "ok"}'
        result = formatter.format(record)
        assert "--- API Request Data ---" in result
        assert '{"key": "value"}' in result
        assert "--- API Response Data ---" in result
        assert '{"status": "ok"}' in result

    def test_api_details_skipped_when_empty(self):
        formatter = EnhancedFormatter(include_api_details=True)
        record = self._make_record()
        record.api_data = ""
        record.response = ""
        result = formatter.format(record)
        assert "--- API Request Data ---" not in result
        assert "--- API Response Data ---" not in result

    def test_http_client_header_formatting(self):
        formatter = EnhancedFormatter()
        record = self._make_record(
            msg="Header: Content-Type: application/json",
            level=logging.DEBUG,
            name="http.client",
        )
        result = formatter.format(record)
        assert "HTTP" in result
        assert "Header:" in result

    def test_http_client_authorization_header_redacted(self):
        formatter = EnhancedFormatter()
        record = self._make_record(
            msg="Header: authorization: 'Bearer secret-token-123'",
            level=logging.DEBUG,
            name="http.client",
        )
        result = formatter.format(record)
        assert "HTTP" in result
        assert "[REDACTED]" in result

    def test_http_client_sending_request_with_bearer_redacted(self):
        formatter = EnhancedFormatter()
        record = self._make_record(
            msg="Sending request: authorization: Bearer my-token-abc",
            level=logging.DEBUG,
            name="http.client",
        )
        result = formatter.format(record)
        assert "HTTP" in result
        assert "Bearer [REDACTED]" in result
        assert "my-token-abc" not in result

    def test_non_http_debug_log_unaffected(self):
        formatter = EnhancedFormatter()
        record = self._make_record(msg="just a debug message", level=logging.DEBUG)
        result = formatter.format(record)
        assert "just a debug message" in result
        assert "HTTP" not in result


# --- setup_logger tests ---


class TestSetupLogger:
    """Tests for setup_logger()."""

    def test_returns_logger_instance(self):
        result = setup_logger()
        assert isinstance(result, logging.Logger)
        assert result.name == "slack_migrator"

    def test_logger_level_is_debug(self):
        result = setup_logger()
        assert result.level == logging.DEBUG

    def test_console_handler_info_level_by_default(self):
        result = setup_logger()
        console_handlers = [
            h for h in result.handlers if isinstance(h, logging.StreamHandler)
        ]
        assert len(console_handlers) == 1
        assert console_handlers[0].level == logging.INFO

    def test_console_handler_debug_level_when_verbose(self):
        result = setup_logger(verbose=True)
        console_handlers = [
            h for h in result.handlers if isinstance(h, logging.StreamHandler)
        ]
        assert len(console_handlers) == 1
        assert console_handlers[0].level == logging.DEBUG

    def test_clears_existing_handlers(self):
        logger = logging.getLogger("slack_migrator")
        logger.addHandler(logging.StreamHandler())
        logger.addHandler(logging.StreamHandler())
        assert len(logger.handlers) == 2
        setup_logger()
        # After setup, should have exactly 1 console handler
        assert len(logger.handlers) == 1

    def test_debug_api_sets_module_flag(self):
        assert log_module._DEBUG_API_ENABLED is False
        setup_logger(debug_api=True)
        assert log_module._DEBUG_API_ENABLED is True

    def test_debug_api_enables_http_client_logger(self):
        setup_logger(debug_api=True)
        http_logger = logging.getLogger("http.client")
        assert http_logger.level == logging.DEBUG

    def test_output_dir_creates_file_handler(self, tmp_path):
        result = setup_logger(output_dir=str(tmp_path))
        file_handlers = [
            h for h in result.handlers if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) == 1
        assert os.path.exists(os.path.join(str(tmp_path), "migration.log"))

    def test_formatter_is_enhanced(self):
        result = setup_logger()
        handler = result.handlers[0]
        assert isinstance(handler.formatter, EnhancedFormatter)


# --- log_with_context tests ---


class TestLogWithContext:
    """Tests for log_with_context()."""

    def test_logs_message_at_specified_level(self):
        logger = setup_logger()
        with patch.object(logger, "log") as mock_log:
            log_with_context(logging.WARNING, "test warning")
            mock_log.assert_called_once()
            args = mock_log.call_args
            assert args[0][0] == logging.WARNING
            assert args[0][1] == "test warning"

    def test_filters_none_values(self):
        logger = setup_logger()
        with patch.object(logger, "log") as mock_log:
            log_with_context(logging.INFO, "msg", channel="general", extra_val=None)
            extras = mock_log.call_args[1]["extra"]
            assert "channel" in extras
            assert "extra_val" not in extras

    def test_filters_reserved_attributes(self):
        logger = setup_logger()
        with patch.object(logger, "log") as mock_log:
            log_with_context(
                logging.INFO, "msg", channel="general", name="conflict", lineno=99
            )
            extras = mock_log.call_args[1]["extra"]
            assert "channel" in extras
            assert "name" not in extras
            assert "lineno" not in extras

    def test_api_data_gets_defaults(self):
        logger = setup_logger()
        with patch.object(logger, "log") as mock_log:
            log_with_context(logging.DEBUG, "api log", api_data='{"test": 1}')
            extras = mock_log.call_args[1]["extra"]
            assert extras["api_data"] == '{"test": 1}'
            # response should get a default empty string value
            assert extras["response"] == ""

    def test_non_api_kwargs_no_defaults(self):
        logger = setup_logger()
        with patch.object(logger, "log") as mock_log:
            log_with_context(logging.INFO, "msg", channel="general")
            extras = mock_log.call_args[1]["extra"]
            assert "channel" in extras
            # api_data and response defaults should NOT be added for non-API logs
            assert "api_data" not in extras
            assert "response" not in extras


# --- _extract_api_operation tests ---


class TestExtractApiOperation:
    """Tests for _extract_api_operation()."""

    def test_chat_spaces_create(self):
        result = _extract_api_operation(
            "POST", "https://chat.googleapis.com/v1/spaces?key=abc"
        )
        assert result == "POST chat.spaces.create"

    def test_chat_spaces_members_create(self):
        result = _extract_api_operation(
            "POST",
            "https://chat.googleapis.com/v1/spaces/abc123/members?key=xyz",
        )
        assert result == "POST chat.spaces.members.create"

    def test_chat_spaces_messages_create(self):
        result = _extract_api_operation(
            "POST",
            "https://chat.googleapis.com/v1/spaces/abc123/messages?key=xyz",
        )
        assert result == "POST chat.spaces.messages.create"

    def test_chat_spaces_messages_get(self):
        result = _extract_api_operation(
            "GET",
            "https://chat.googleapis.com/v1/spaces/abc123/messages/msg456?key=xyz",
        )
        assert result == "GET chat.spaces.messages.get"

    def test_chat_media_upload(self):
        result = _extract_api_operation(
            "POST", "https://chat.googleapis.com/upload/v1/media/spaces/abc123"
        )
        assert result == "POST chat.media.upload"

    def test_chat_media_download(self):
        result = _extract_api_operation(
            "GET", "https://chat.googleapis.com/v1/media/spaces/abc123/attachments/xyz"
        )
        assert result == "GET chat.media.download"

    def test_fallback_short_url(self):
        result = _extract_api_operation("GET", "https://example.com/api/v1/data")
        assert result == "GET https://example.com/api/v1/data"

    def test_fallback_long_url_truncated(self):
        long_path = "/a" * 60
        result = _extract_api_operation("GET", f"https://example.com{long_path}")
        assert result.startswith("GET ...")
        assert len(result.split(" ", 1)[1]) <= 50

    def test_url_query_params_stripped_in_fallback(self):
        result = _extract_api_operation(
            "GET", "https://example.com/short?param=value&other=2"
        )
        assert "?" not in result
        assert result == "GET https://example.com/short"

    def test_non_google_chat_url(self):
        result = _extract_api_operation("PUT", "https://api.slack.com/chat.postMessage")
        assert result == "PUT https://api.slack.com/chat.postMessage"


# --- is_debug_api_enabled tests ---


class TestIsDebugApiEnabled:
    """Tests for is_debug_api_enabled()."""

    def test_default_is_false(self):
        log_module._DEBUG_API_ENABLED = False
        assert is_debug_api_enabled() is False

    def test_true_after_setup_with_debug_api(self):
        setup_logger(debug_api=True)
        assert is_debug_api_enabled() is True

    def test_false_after_setup_without_debug_api(self):
        setup_logger(debug_api=False)
        assert is_debug_api_enabled() is False


# --- log_api_request tests ---


class TestLogApiRequest:
    """Tests for log_api_request()."""

    def test_no_log_when_debug_api_disabled(self):
        setup_logger(debug_api=False)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_request("GET", "https://example.com/api")
            mock_ctx.assert_not_called()

    def test_logs_when_debug_api_enabled(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_request("POST", "https://chat.googleapis.com/v1/spaces?key=abc")
            mock_ctx.assert_called_once()
            args = mock_ctx.call_args
            assert args[0][0] == logging.DEBUG
            assert "API Request" in args[0][1]
            assert "chat.spaces.create" in args[0][1]

    def test_sensitive_fields_redacted_in_data(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_request(
                "POST",
                "https://example.com/api",
                data={"token": "secret123", "name": "test"},
            )
            call_kwargs = mock_ctx.call_args[1]
            api_data = json.loads(call_kwargs["api_data"])
            assert api_data["token"] == "[REDACTED]"  # noqa: S105
            assert api_data["name"] == "test"

    def test_passes_extra_kwargs(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_request("GET", "https://example.com", channel="general")
            call_kwargs = mock_ctx.call_args[1]
            assert call_kwargs["channel"] == "general"

    def test_handles_none_data(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_request("GET", "https://example.com", data=None)
            mock_ctx.assert_called_once()
            call_kwargs = mock_ctx.call_args[1]
            assert "api_data" not in call_kwargs


# --- log_api_response tests ---


class TestLogApiResponse:
    """Tests for log_api_response()."""

    def test_no_log_when_debug_api_disabled(self):
        setup_logger(debug_api=False)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com")
            mock_ctx.assert_not_called()

    def test_logs_success_response(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com/api")
            mock_ctx.assert_called_once()
            msg = mock_ctx.call_args[0][1]
            assert "200" in msg

    def test_includes_dict_response_data(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com", response_data={"ok": True})
            call_kwargs = mock_ctx.call_args[1]
            assert "response" in call_kwargs
            assert '"ok": true' in call_kwargs["response"]

    def test_includes_list_response_data(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com", response_data=[1, 2, 3])
            call_kwargs = mock_ctx.call_args[1]
            assert "response" in call_kwargs

    def test_truncates_long_dict_response(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            large_data = {"key": "x" * 3000}
            log_api_response(200, "https://example.com", response_data=large_data)
            call_kwargs = mock_ctx.call_args[1]
            assert "... [truncated]" in call_kwargs["response"]

    def test_truncates_long_string_response(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com", response_data="x" * 2000)
            call_kwargs = mock_ctx.call_args[1]
            assert "... [truncated]" in call_kwargs["response"]

    def test_handles_unserializable_response(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            # An object that will fail json.dumps but has a str representation
            class BadJson:
                def __str__(self):
                    return "bad-json-obj"

            log_api_response(200, "https://example.com", response_data=BadJson())
            call_kwargs = mock_ctx.call_args[1]
            assert "bad-json-obj" in call_kwargs["response"]

    def test_error_status_code_in_message(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(404, "https://example.com")
            msg = mock_ctx.call_args[0][1]
            assert "404" in msg

    def test_passes_extra_kwargs(self):
        setup_logger(debug_api=True)
        with patch("slack_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com", channel="dev")
            call_kwargs = mock_ctx.call_args[1]
            assert call_kwargs["channel"] == "dev"


# --- log_failed_message tests ---


class TestLogFailedMessage:
    """Tests for log_failed_message()."""

    def test_logs_error_with_ts_and_error(self):
        logger = setup_logger()
        with patch.object(logger, "error") as mock_error:
            log_failed_message(
                "general",
                {"ts": "12345.6", "error": "rate_limited", "payload": {}},
            )
            mock_error.assert_called_once()
            call_msg = mock_error.call_args[0][0]
            assert "12345.6" in call_msg
            assert "rate_limited" in call_msg
            call_extra = mock_error.call_args[1]["extra"]
            assert call_extra["channel"] == "general"

    def test_logs_debug_payload(self):
        logger = setup_logger()
        with patch.object(logger, "debug") as mock_debug:
            log_failed_message(
                "general",
                {"ts": "1", "error": "err", "payload": {"text": "hello"}},
            )
            mock_debug.assert_called()
            # At least one debug call should have the payload
            payload_logged = any(
                "hello" in str(call) for call in mock_debug.call_args_list
            )
            assert payload_logged

    def test_handles_non_serializable_payload(self):
        logger = setup_logger()
        # A set is not JSON serializable
        with patch.object(logger, "debug") as mock_debug:
            log_failed_message(
                "general",
                {"ts": "1", "error": "err", "payload": {1, 2, 3}},
            )
            # Should still log without raising
            mock_debug.assert_called()


# --- get_logger tests ---


class TestGetLogger:
    """Tests for get_logger()."""

    def test_returns_slack_migrator_logger(self):
        result = get_logger()
        assert result.name == "slack_migrator"

    def test_creates_handler_if_none_exist(self):
        logger = logging.getLogger("slack_migrator")
        for h in logger.handlers[:]:
            logger.removeHandler(h)
        result = get_logger()
        assert len(result.handlers) >= 1

    def test_does_not_duplicate_handlers(self):
        # First call creates a handler
        get_logger()
        handler_count = len(logging.getLogger("slack_migrator").handlers)
        # Second call should not add more handlers
        get_logger()
        assert len(logging.getLogger("slack_migrator").handlers) == handler_count


# --- setup_main_log_file tests ---


class TestSetupMainLogFile:
    """Tests for setup_main_log_file()."""

    def test_creates_log_file(self, tmp_path):
        setup_main_log_file(str(tmp_path))
        log_file = tmp_path / "migration.log"
        assert log_file.exists()

    def test_returns_file_handler(self, tmp_path):
        result = setup_main_log_file(str(tmp_path))
        assert isinstance(result, logging.FileHandler)

    def test_handler_level_is_debug(self, tmp_path):
        result = setup_main_log_file(str(tmp_path))
        assert result.level == logging.DEBUG

    def test_handler_uses_enhanced_formatter(self, tmp_path):
        result = setup_main_log_file(str(tmp_path))
        assert isinstance(result.formatter, EnhancedFormatter)

    def test_creates_output_dir_if_missing(self, tmp_path):
        new_dir = tmp_path / "subdir" / "logs"
        setup_main_log_file(str(new_dir))
        assert new_dir.exists()

    def test_main_log_filter_passes_errors(self, tmp_path):
        handler = setup_main_log_file(str(tmp_path))
        # Error-level log with a channel should still pass the main filter
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.ERROR,
            pathname="test.py",
            lineno=1,
            msg="critical error",
            args=(),
            exc_info=None,
        )
        record.channel = "general"
        assert all(f.filter(record) for f in handler.filters)

    def test_main_log_filter_passes_no_channel_logs(self, tmp_path):
        handler = setup_main_log_file(str(tmp_path))
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="migration started",
            args=(),
            exc_info=None,
        )
        # No channel attribute set
        assert all(f.filter(record) for f in handler.filters)

    def test_main_log_filter_excludes_channel_info_logs(self, tmp_path):
        handler = setup_main_log_file(str(tmp_path))
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="channel message",
            args=(),
            exc_info=None,
        )
        record.channel = "general"
        # INFO-level channel logs should be excluded from main log
        assert not all(f.filter(record) for f in handler.filters)

    def test_main_log_filter_excludes_api_logs_without_debug(self, tmp_path):
        handler = setup_main_log_file(str(tmp_path), debug_api=False)
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.DEBUG,
            pathname="test.py",
            lineno=1,
            msg="API Request: POST something",
            args=(),
            exc_info=None,
        )
        record.channel = "general"
        assert not all(f.filter(record) for f in handler.filters)

    def test_main_log_filter_passes_empty_channel(self, tmp_path):
        handler = setup_main_log_file(str(tmp_path))
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="some event",
            args=(),
            exc_info=None,
        )
        record.channel = ""
        assert all(f.filter(record) for f in handler.filters)


# --- setup_channel_logger tests ---


class TestSetupChannelLogger:
    """Tests for setup_channel_logger()."""

    def test_creates_channel_log_file(self, tmp_path):
        setup_channel_logger(str(tmp_path), "general")
        log_file = tmp_path / "channel_logs" / "general_migration.log"
        assert log_file.exists()

    def test_returns_file_handler(self, tmp_path):
        result = setup_channel_logger(str(tmp_path), "general")
        assert isinstance(result, logging.FileHandler)

    def test_handler_level_is_debug(self, tmp_path):
        result = setup_channel_logger(str(tmp_path), "general")
        assert result.level == logging.DEBUG

    def test_channel_filter_passes_matching_channel(self, tmp_path):
        handler = setup_channel_logger(str(tmp_path), "general")
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="channel event",
            args=(),
            exc_info=None,
        )
        record.channel = "general"
        assert all(f.filter(record) for f in handler.filters)

    def test_channel_filter_excludes_different_channel(self, tmp_path):
        handler = setup_channel_logger(str(tmp_path), "general")
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="channel event",
            args=(),
            exc_info=None,
        )
        record.channel = "random"
        assert not all(f.filter(record) for f in handler.filters)

    def test_channel_filter_excludes_no_channel(self, tmp_path):
        handler = setup_channel_logger(str(tmp_path), "general")
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="no channel log",
            args=(),
            exc_info=None,
        )
        assert not all(f.filter(record) for f in handler.filters)

    def test_channel_filter_http_client_with_debug_api(self, tmp_path):
        handler = setup_channel_logger(str(tmp_path), "general", debug_api=True)
        record = logging.LogRecord(
            name="http.client",
            level=logging.DEBUG,
            pathname="test.py",
            lineno=1,
            msg="Header: Content-Type",
            args=(),
            exc_info=None,
        )
        record.channel = "general"
        assert all(f.filter(record) for f in handler.filters)

    def test_channel_filter_http_client_wrong_channel(self, tmp_path):
        handler = setup_channel_logger(str(tmp_path), "general", debug_api=True)
        record = logging.LogRecord(
            name="http.client",
            level=logging.DEBUG,
            pathname="test.py",
            lineno=1,
            msg="Header: Content-Type",
            args=(),
            exc_info=None,
        )
        record.channel = "random"
        assert not all(f.filter(record) for f in handler.filters)

    def test_channel_filter_api_data_matching_channel(self, tmp_path):
        handler = setup_channel_logger(str(tmp_path), "general", debug_api=True)
        record = logging.LogRecord(
            name="slack_migrator",
            level=logging.DEBUG,
            pathname="test.py",
            lineno=1,
            msg="api debug",
            args=(),
            exc_info=None,
        )
        record.api_data = '{"key": "value"}'
        record.channel = "general"
        assert all(f.filter(record) for f in handler.filters)


# --- ensure_channel_log_created tests ---


class TestEnsureChannelLogCreated:
    """Tests for ensure_channel_log_created()."""

    def test_creates_log_file(self, tmp_path):
        ensure_channel_log_created(str(tmp_path), "general")
        log_file = tmp_path / "channel_logs" / "general_migration.log"
        assert log_file.exists()

    def test_dry_run_header(self, tmp_path):
        ensure_channel_log_created(str(tmp_path), "general", dry_run=True)
        log_file = tmp_path / "channel_logs" / "general_migration.log"
        content = log_file.read_text()
        assert "DRY RUN" in content

    def test_normal_run_header(self, tmp_path):
        ensure_channel_log_created(str(tmp_path), "general", dry_run=False)
        log_file = tmp_path / "channel_logs" / "general_migration.log"
        content = log_file.read_text()
        assert "Channel migration log for general" in content
        assert "DRY RUN" not in content

    def test_does_not_overwrite_existing_file(self, tmp_path):
        logs_dir = tmp_path / "channel_logs"
        logs_dir.mkdir(parents=True)
        log_file = logs_dir / "general_migration.log"
        log_file.write_text("existing content")
        ensure_channel_log_created(str(tmp_path), "general")
        assert log_file.read_text() == "existing content"

    def test_creates_channel_logs_directory(self, tmp_path):
        ensure_channel_log_created(str(tmp_path), "dev")
        assert (tmp_path / "channel_logs").is_dir()

    def test_handles_write_error_gracefully(self, tmp_path):
        with patch("builtins.open", side_effect=PermissionError("no permission")):
            # Should not raise, just print a warning
            ensure_channel_log_created(str(tmp_path), "general")


# --- _enable_http_client_debug tests ---


class TestEnableHttpClientDebug:
    """Tests for _enable_http_client_debug()."""

    def test_patches_putheader(self):
        import http.client

        original = http.client.HTTPConnection.putheader
        try:
            setup_logger(debug_api=True)
            # putheader should be patched
            assert http.client.HTTPConnection.putheader is not original
        finally:
            # Restore original to avoid side effects
            http.client.HTTPConnection.putheader = original
