"""Unit tests for the logging module."""

import http.client
import json
import logging
import os
from unittest.mock import patch

import pytest

import slack_chat_migrator.utils.logging as log_module
from slack_chat_migrator.utils.logging import (
    EnhancedFormatter,
    ImmediateFlushHandler,
    RedactingFilter,
    _extract_api_operation,
    create_output_directory,
    ensure_channel_log_created,
    get_logger,
    is_debug_api_enabled,
    log_api_request,
    log_api_response,
    log_with_context,
    make_private_dir,
    open_private,
    sanitize_for_log,
    setup_channel_logger,
    setup_logger,
    setup_main_log_file,
)

# Capture the stdlib putheader before any test has a chance to patch it.
_STDLIB_PUTHEADER = http.client.HTTPConnection.putheader


@pytest.fixture(autouse=True)
def _clean_logger():
    """Remove all handlers from the slack_chat_migrator logger before and after each test."""
    logger = logging.getLogger("slack_chat_migrator")
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    yield
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    # Reset the module-level debug flag
    log_module._DEBUG_API_ENABLED.set(False)


# --- EnhancedFormatter tests ---


class TestEnhancedFormatter:
    """Tests for EnhancedFormatter."""

    def _make_record(
        self, msg="test message", level=logging.INFO, name="slack_chat_migrator"
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
        assert "slack_chat_migrator" in result
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
        assert result.name == "slack_chat_migrator"

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
        logger = logging.getLogger("slack_chat_migrator")
        logger.addHandler(logging.StreamHandler())
        logger.addHandler(logging.StreamHandler())
        assert len(logger.handlers) == 2
        setup_logger()
        # After setup, should have exactly 1 console handler
        assert len(logger.handlers) == 1

    def test_debug_api_sets_module_flag(self):
        assert log_module._DEBUG_API_ENABLED.get() is False
        setup_logger(debug_api=True)
        assert log_module._DEBUG_API_ENABLED.get() is True

    def test_debug_api_enables_http_client_logger(self):
        setup_logger(debug_api=True)
        http_logger = logging.getLogger("http.client")
        assert http_logger.level == logging.DEBUG

    def test_output_dir_creates_file_handler(self, tmp_path):
        result = setup_logger(output_dir=str(tmp_path))
        file_handlers = [
            h for h in result.handlers if isinstance(h, ImmediateFlushHandler)
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
        log_module._DEBUG_API_ENABLED.set(False)
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
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_request("GET", "https://example.com/api")
            mock_ctx.assert_not_called()

    def test_logs_when_debug_api_enabled(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_request("POST", "https://chat.googleapis.com/v1/spaces?key=abc")
            mock_ctx.assert_called_once()
            args = mock_ctx.call_args
            assert args[0][0] == logging.DEBUG
            assert "API Request" in args[0][1]
            assert "chat.spaces.create" in args[0][1]

    def test_sensitive_fields_redacted_in_data(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
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
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_request("GET", "https://example.com", channel="general")
            call_kwargs = mock_ctx.call_args[1]
            assert call_kwargs["channel"] == "general"

    def test_handles_none_data(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_request("GET", "https://example.com", data=None)
            mock_ctx.assert_called_once()
            call_kwargs = mock_ctx.call_args[1]
            assert "api_data" not in call_kwargs


# --- log_api_response tests ---


class TestLogApiResponse:
    """Tests for log_api_response()."""

    def test_no_log_when_debug_api_disabled(self):
        setup_logger(debug_api=False)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com")
            mock_ctx.assert_not_called()

    def test_logs_success_response(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com/api")
            mock_ctx.assert_called_once()
            msg = mock_ctx.call_args[0][1]
            assert "200" in msg

    def test_includes_dict_response_data(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com", response_data={"ok": True})
            call_kwargs = mock_ctx.call_args[1]
            assert "response" in call_kwargs
            assert '"ok": true' in call_kwargs["response"]

    def test_includes_list_response_data(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com", response_data=[1, 2, 3])
            call_kwargs = mock_ctx.call_args[1]
            assert "response" in call_kwargs

    def test_truncates_long_dict_response(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            large_data = {"key": "x" * 3000}
            log_api_response(200, "https://example.com", response_data=large_data)
            call_kwargs = mock_ctx.call_args[1]
            assert "... [truncated]" in call_kwargs["response"]

    def test_truncates_long_string_response(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com", response_data="x" * 2000)
            call_kwargs = mock_ctx.call_args[1]
            assert "... [truncated]" in call_kwargs["response"]

    def test_handles_unserializable_response(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            # An object that will fail json.dumps but has a str representation
            class BadJson:
                def __str__(self):
                    return "bad-json-obj"

            log_api_response(200, "https://example.com", response_data=BadJson())
            call_kwargs = mock_ctx.call_args[1]
            assert "bad-json-obj" in call_kwargs["response"]

    def test_error_status_code_in_message(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(404, "https://example.com")
            msg = mock_ctx.call_args[0][1]
            assert "404" in msg

    def test_passes_extra_kwargs(self):
        setup_logger(debug_api=True)
        with patch("slack_chat_migrator.utils.logging.log_with_context") as mock_ctx:
            log_api_response(200, "https://example.com", channel="dev")
            call_kwargs = mock_ctx.call_args[1]
            assert call_kwargs["channel"] == "dev"


# --- get_logger tests ---


class TestGetLogger:
    """Tests for get_logger()."""

    def test_returns_slack_chat_migrator_logger(self):
        result = get_logger()
        assert result.name == "slack_chat_migrator"

    def test_creates_handler_if_none_exist(self):
        logger = logging.getLogger("slack_chat_migrator")
        for h in logger.handlers[:]:
            logger.removeHandler(h)
        result = get_logger()
        assert len(result.handlers) >= 1

    def test_does_not_duplicate_handlers(self):
        # First call creates a handler
        get_logger()
        handler_count = len(logging.getLogger("slack_chat_migrator").handlers)
        # Second call should not add more handlers
        get_logger()
        assert len(logging.getLogger("slack_chat_migrator").handlers) == handler_count


# --- setup_main_log_file tests ---


class TestSetupMainLogFile:
    """Tests for setup_main_log_file()."""

    def test_creates_log_file(self, tmp_path):
        setup_main_log_file(str(tmp_path))
        log_file = tmp_path / "migration.log"
        assert log_file.exists()

    def test_returns_immediate_flush_handler(self, tmp_path):
        result = setup_main_log_file(str(tmp_path))
        assert isinstance(result, ImmediateFlushHandler)

    def test_handler_level_is_info_by_default(self, tmp_path):
        """DEBUG detail (message payloads) only reaches disk with --debug_api."""
        result = setup_main_log_file(str(tmp_path))
        assert result.level == logging.INFO

    def test_handler_level_is_debug_with_debug_api(self, tmp_path):
        result = setup_main_log_file(str(tmp_path), debug_api=True)
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
            name="slack_chat_migrator",
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
            name="slack_chat_migrator",
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
            name="slack_chat_migrator",
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
            name="slack_chat_migrator",
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
            name="slack_chat_migrator",
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

    def test_returns_immediate_flush_handler(self, tmp_path):
        result = setup_channel_logger(str(tmp_path), "general")
        assert isinstance(result, ImmediateFlushHandler)

    def test_handler_level_is_info_by_default(self, tmp_path):
        """Message payloads are DEBUG and only reach disk with --debug_api."""
        result = setup_channel_logger(str(tmp_path), "general")
        assert result.level == logging.INFO

    def test_handler_level_is_debug_with_debug_api(self, tmp_path):
        result = setup_channel_logger(str(tmp_path), "general", debug_api=True)
        assert result.level == logging.DEBUG

    def test_debug_payload_not_written_without_debug_api(self, tmp_path):
        handler = setup_channel_logger(str(tmp_path), "general")
        log_with_context(
            logging.DEBUG,
            "Complete message payload for 1.0: {'text': 'secret plans'}",
            channel="general",
        )
        handler.close()
        log_file = tmp_path / "channel_logs" / "general_migration.log"
        assert "secret plans" not in log_file.read_text()

    def test_channel_filter_passes_matching_channel(self, tmp_path):
        handler = setup_channel_logger(str(tmp_path), "general")
        record = logging.LogRecord(
            name="slack_chat_migrator",
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
            name="slack_chat_migrator",
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
            name="slack_chat_migrator",
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
            name="slack_chat_migrator",
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

    @pytest.fixture(autouse=True)
    def _restore_putheader(self):
        """Restore the stdlib putheader before and after each test."""
        http.client.HTTPConnection.putheader = _STDLIB_PUTHEADER  # type: ignore[assignment]
        yield
        http.client.HTTPConnection.putheader = _STDLIB_PUTHEADER  # type: ignore[assignment]

    def test_patches_putheader(self):
        original = http.client.HTTPConnection.putheader
        assert not getattr(original, "_patched_for_debug", False)

        setup_logger(debug_api=True)
        patched = http.client.HTTPConnection.putheader
        # putheader should be patched
        assert patched is not original
        assert getattr(patched, "_patched_for_debug", False) is True

    def test_double_patch_guard_prevents_stacking(self):
        setup_logger(debug_api=True)
        first_patch = http.client.HTTPConnection.putheader
        # Call setup_logger again with debug_api=True
        setup_logger(debug_api=True)
        second_patch = http.client.HTTPConnection.putheader
        # The patched function should be the same object (not re-wrapped)
        assert first_patch is second_patch


# --- sanitize_for_log tests ---


class TestSanitizeForLog:
    """Tests for sanitize_for_log()."""

    def test_redacts_bearer_token(self):
        text = "Authorization: Bearer ya29.abc123XYZ-_~+/def456=="
        result = sanitize_for_log(text)
        assert "ya29" not in result
        assert "Bearer [REDACTED]" in result

    def test_redacts_bearer_case_insensitive(self):
        text = "header: bearer ya29.secret-token-here"
        result = sanitize_for_log(text)
        assert "ya29" not in result
        assert "Bearer [REDACTED]" in result

    def test_redacts_token_url_param(self):
        text = "https://example.com/api?token=xoxb-123-456-abc&other=val"
        result = sanitize_for_log(text)
        assert "xoxb" not in result
        assert "token=[REDACTED]" in result
        assert "other=val" in result

    def test_redacts_access_token_param(self):
        text = "url?access_token=secret123"
        result = sanitize_for_log(text)
        assert "secret123" not in result
        assert "access_token=[REDACTED]" in result

    def test_redacts_key_param(self):
        text = "url?key=AIzaSyABC123def456"
        result = sanitize_for_log(text)
        assert "AIzaSy" not in result
        assert "key=[REDACTED]" in result

    def test_case_insensitive_param_redaction(self):
        text = "url?TOKEN=mysecret123&Key=otherkey456"
        result = sanitize_for_log(text)
        assert "mysecret" not in result
        assert "otherkey" not in result

    def test_no_redaction_for_clean_text(self):
        text = "Successfully uploaded file report.pdf to Drive"
        result = sanitize_for_log(text)
        assert result == text

    def test_empty_string(self):
        assert sanitize_for_log("") == ""


class TestEnhancedFormatterSanitization:
    """Tests that EnhancedFormatter applies sanitize_for_log."""

    def _make_record(self, msg="test message", level=logging.INFO):
        return logging.LogRecord(
            name="slack_chat_migrator",
            level=level,
            pathname="test.py",
            lineno=1,
            msg=msg,
            args=(),
            exc_info=None,
        )

    def test_bearer_token_redacted_in_output(self):
        formatter = EnhancedFormatter()
        record = self._make_record(
            msg="Got response with Bearer ya29.secret-token-here"
        )
        result = formatter.format(record)
        assert "ya29" not in result
        assert "Bearer [REDACTED]" in result

    def test_url_token_param_redacted_in_output(self):
        formatter = EnhancedFormatter()
        record = self._make_record(
            msg="Requesting https://api.example.com?token=xoxb-secret"
        )
        result = formatter.format(record)
        assert "xoxb-secret" not in result
        assert "token=[REDACTED]" in result


# --- Secrets, permissions and handler hygiene (issues #112, #119) ---

SLACK_FILE_URL = (
    "https://files.slack.com/files-pri/T0ABC-F0ABC/report.pdf?t=xoxe-1234567890-abcdef"
)


class TestSlackTokenRedaction:
    """sanitize_for_log() and the RedactingFilter remove Slack tokens."""

    def test_redacts_file_url_token(self):
        result = sanitize_for_log(SLACK_FILE_URL)
        assert "xoxe-1234567890" not in result
        assert "?t=[REDACTED]" in result
        assert "files.slack.com/files-pri/T0ABC-F0ABC/report.pdf" in result

    def test_redacts_t_param_after_ampersand(self):
        result = sanitize_for_log("https://x.slack.com/f?a=1&t=abc.def-123&b=2")
        assert "abc.def-123" not in result
        assert "&b=2" in result

    @pytest.mark.parametrize(
        "token",
        [
            "xoxp-1234-5678-abcd",
            "xoxb-12345-67890-AbCdEf",
            "xoxp-1-abc",
            "xoxa-2-abc",
            "xoxs-12-abc",
            "xoxr-3-abc",
            "xapp-1-A0-123-abc",
        ],
    )
    def test_redacts_bare_tokens(self, token):
        result = sanitize_for_log(f"token value {token} end")
        assert token not in result
        assert "[REDACTED]" in result
        assert result.endswith(" end")

    def test_redacts_token_in_json(self):
        result = sanitize_for_log(f'{{"url_private": "{SLACK_FILE_URL}"}}')
        assert "1234567890-abcdef" not in result

    def test_redacts_rotating_token(self):
        result = sanitize_for_log("refresh xoxe.xoxp-1-Mi0yLTE2 done")
        assert "Mi0yLTE2" not in result

    def test_does_not_touch_ordinary_words(self):
        text = "Processing channel xoxo-party and taxonomy=fine"
        assert sanitize_for_log(text) == text

    def test_filter_redacts_message_args_and_extras(self):
        record = logging.LogRecord(
            "slack_chat_migrator",
            logging.INFO,
            __file__,
            1,
            "url=%s",
            (SLACK_FILE_URL,),
            None,
        )
        record.error_details = f"failed {SLACK_FILE_URL}"
        assert RedactingFilter().filter(record) is True
        assert "xoxe-1234567890" not in record.getMessage()
        assert "xoxe-1234567890" not in record.error_details

    def test_no_token_in_any_log_file(self, tmp_path):
        """End to end: main log, channel log and console are all redacted."""
        import io

        setup_logger(output_dir=str(tmp_path), debug_api=True)
        stream = io.StringIO()
        console = logging.getLogger("slack_chat_migrator").handlers[0]
        console.setStream(stream)
        channel_handler = setup_channel_logger(str(tmp_path), "general", debug_api=True)
        log_with_context(logging.ERROR, f"Failed to download {SLACK_FILE_URL}")
        log_with_context(
            logging.DEBUG, f"Downloading {SLACK_FILE_URL}", channel="general"
        )
        log_with_context(
            logging.DEBUG,
            "API call",
            channel="general",
            api_data=f'{{"u": "{SLACK_FILE_URL}"}}',
        )
        channel_handler.close()
        texts = [
            (tmp_path / "migration.log").read_text(),
            (tmp_path / "channel_logs" / "general_migration.log").read_text(),
            stream.getvalue(),
        ]
        for text in texts:
            assert "xoxe-1234567890" not in text
        assert "Failed to download" in texts[0]
        assert "Downloading" in texts[1]


@pytest.mark.skipif(os.name == "nt", reason="POSIX permissions")
class TestPrivatePermissions:
    """Output directories are 0700 and log files 0600, whatever the umask."""

    @pytest.fixture(autouse=True)
    def _permissive_umask(self):
        old = os.umask(0o022)
        yield
        os.umask(old)

    def test_create_output_directory_is_private(self, tmp_path):
        out = create_output_directory(str(tmp_path / "migration_logs"))
        assert os.stat(out).st_mode & 0o777 == 0o700
        assert os.stat(os.path.join(out, "channel_logs")).st_mode & 0o777 == 0o700
        assert os.stat(tmp_path / "migration_logs").st_mode & 0o777 == 0o700
        assert os.path.basename(out).startswith("run_")

    def test_log_files_are_private(self, tmp_path):
        setup_main_log_file(str(tmp_path))
        handler = setup_channel_logger(str(tmp_path), "general")
        handler.close()
        ensure_channel_log_created(str(tmp_path), "random")
        for path in (
            tmp_path / "migration.log",
            tmp_path / "channel_logs" / "general_migration.log",
            tmp_path / "channel_logs" / "random_migration.log",
        ):
            assert os.stat(path).st_mode & 0o777 == 0o600, path

    def test_open_private_tightens_existing_file(self, tmp_path):
        path = tmp_path / "report.yaml"
        path.write_text("old")
        os.chmod(path, 0o644)
        with open_private(str(path), "a") as f:
            f.write(" new")
        assert os.stat(path).st_mode & 0o777 == 0o600
        assert path.read_text() == "old new"

    def test_make_private_dir_tightens_existing_dir(self, tmp_path):
        target = tmp_path / "logs"
        target.mkdir(mode=0o755)
        make_private_dir(str(target))
        assert os.stat(target).st_mode & 0o777 == 0o700


class TestLogWithContextExcInfo:
    """exc_info/stack_info reach the logger (tracebacks are kept)."""

    def _capture(self):
        records: list[logging.LogRecord] = []

        class _Collect(logging.Handler):
            def emit(self, record):
                records.append(record)

        logger = logging.getLogger("slack_chat_migrator")
        logger.setLevel(logging.DEBUG)
        logger.addHandler(_Collect())
        return records

    def test_exc_info_true_records_traceback(self):
        records = self._capture()
        try:
            raise RuntimeError("boom")
        except RuntimeError:
            log_with_context(logging.ERROR, "Migration failed: boom", exc_info=True)
        assert records[-1].exc_info is not None
        assert records[-1].exc_info[0] is RuntimeError

    def test_traceback_in_formatted_output(self, tmp_path):
        setup_logger(output_dir=str(tmp_path))
        try:
            raise ValueError("kaboom")
        except ValueError:
            log_with_context(logging.ERROR, "Migration failed: kaboom", exc_info=True)
        text = (tmp_path / "migration.log").read_text()
        assert "Traceback (most recent call last)" in text
        assert "ValueError: kaboom" in text

    def test_stack_info_passed_through(self):
        records = self._capture()
        log_with_context(logging.WARNING, "where am I", stack_info=True)
        assert records[-1].stack_info

    def test_record_attributed_to_caller(self):
        records = self._capture()
        log_with_context(logging.INFO, "caller check")
        assert records[-1].funcName == "test_record_attributed_to_caller"

    def test_no_exc_info_by_default(self):
        records = self._capture()
        log_with_context(logging.INFO, "plain")
        assert records[-1].exc_info is None


class TestSetupLoggerClosesHandlers:
    """Calling setup_logger() twice closes the old file handlers."""

    def test_old_file_handler_closed(self, tmp_path):
        first = tmp_path / "first"
        setup_logger(output_dir=str(first))
        old = [
            h
            for h in logging.getLogger("slack_chat_migrator").handlers
            if isinstance(h, ImmediateFlushHandler)
        ]
        assert old and old[0].stream is not None and not old[0].stream.closed
        stream = old[0].stream
        setup_logger(output_dir=str(tmp_path / "second"))
        assert stream.closed
        assert old[0] not in logging.getLogger("slack_chat_migrator").handlers
