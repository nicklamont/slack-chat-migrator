"""
Logging module for the Slack to Google Chat migration tool
"""

from __future__ import annotations

import contextvars
import json
import logging
import os
import re
from typing import Any

# Module-level flag to track if API debug logging is enabled (thread-safe)
_DEBUG_API_ENABLED: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "_DEBUG_API_ENABLED", default=False
)

RESPONSE_MAX_LENGTH = 2000
RESPONSE_FALLBACK_LENGTH = 1000


class JsonFormatter(logging.Formatter):
    """Logging formatter that outputs records as single-line JSON objects."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as a JSON string.

        Args:
            record: The log record to format.

        Returns:
            A JSON-encoded string containing the log data.
        """
        data = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
        }
        # Include any additional attributes from the record
        for key, value in record.__dict__.items():
            if key not in (
                "args",
                "asctime",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "id",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
            ):
                data[key] = value
        return json.dumps(data)


def setup_main_log_file(
    output_dir: str, debug_api: bool = False
) -> logging.FileHandler:
    """
    Set up a file handler for the main log file that contains non-channel-specific logs.

    Args:
        output_dir: The output directory path
        debug_api: If True, enable detailed API request/response logging

    Returns:
        The file handler for the main log file
    """
    # Create the logs directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create the log file path
    log_file = os.path.join(output_dir, "migration.log")

    # Create file handler
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setLevel(logging.DEBUG)  # Always use DEBUG level for file handlers

    # Enable immediate flushing to ensure logs are written to disk promptly
    # This is important in case the migration fails and we want to preserve logs
    if hasattr(file_handler, "flush"):
        # Ensure the file handler flushes immediately on each log write
        old_emit = file_handler.emit

        def immediate_flush_emit(record: logging.LogRecord) -> None:
            """Emit the record and flush immediately.

            Args:
                record: The log record to emit.
            """
            old_emit(record)
            file_handler.flush()

        file_handler.emit = immediate_flush_emit  # type: ignore[method-assign]

    # Create formatter - always use EnhancedFormatter but conditionally include API details
    formatter = EnhancedFormatter(
        "%(asctime)s - %(levelname)s - %(message)s", include_api_details=debug_api
    )
    file_handler.setFormatter(formatter)

    # Create a filter to include logs that should go to the main migration log:
    # 1. All ERROR level and above (critical errors always go to main log)
    # 2. All logs without a channel attribute (migration-level events)
    # 3. API logs when debug_api is enabled AND they have no channel context
    class MainLogFilter(logging.Filter):
        """Filter that routes records to the main migration log file."""

        def filter(self, record: logging.LogRecord) -> bool:
            """Return True if *record* belongs in the main log.

            Args:
                record: The log record to evaluate.

            Returns:
                True if the record should appear in the main log.
            """
            # Check if the record has a channel attribute
            record_channel = getattr(record, "channel", None)

            # Always include critical errors (ERROR level and above) in main log
            if record.levelno >= logging.ERROR:
                return True

            # Include ALL logs without a channel attribute or with empty/None channel
            # This covers all migration-level operations by default
            if record_channel is None or record_channel == "":
                return True

            # For logs that DO have a channel attribute, exclude them from main log
            # (they should go to channel-specific logs instead)
            # Exception: if debug_api is disabled, API logs go nowhere
            message = record.getMessage()
            has_api_indicators = any(
                indicator in message
                for indicator in [
                    "API Request:",
                    "API Response:",
                    "🔄",
                    "✅",
                    "--- API Request Data ---",
                    "--- API Response Data ---",
                ]
            )

            # If this is an API log but debug_api is disabled, exclude it entirely
            if has_api_indicators and not debug_api:
                return False

            # All other logs with channel context should go to channel logs only
            return False

    # Add the filter to the handler
    main_filter = MainLogFilter()
    file_handler.addFilter(main_filter)

    # Add handler to the logger
    logger = logging.getLogger("slack_migrator")
    logger.addHandler(file_handler)

    logger.info(f"Main log file created at: {log_file}")
    return file_handler


# Define an enhanced formatter class that can handle both verbose formatting and API details
class EnhancedFormatter(logging.Formatter):
    """
    Custom formatter that supports both verbose mode (with additional context information)
    and API debug mode (with request/response data)
    """

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: str = "%",
        verbose: bool = False,
        include_api_details: bool = False,
    ) -> None:
        # Use more detailed format for verbose mode
        if verbose:
            fmt = "%(asctime)s - %(name)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s"
        elif not fmt:
            fmt = "%(asctime)s - %(levelname)s - %(message)s"

        super().__init__(fmt, datefmt)
        self.include_api_details = include_api_details

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record, optionally appending API request/response data.

        Args:
            record: The log record to format.

        Returns:
            The formatted log string.
        """
        # First apply the base format
        result = super().format(record)

        # Only include API details if explicitly enabled
        if self.include_api_details:
            # Add API data if present (for structured API requests)
            if hasattr(record, "api_data") and record.api_data:
                api_data = record.api_data
                result += f"\n--- API Request Data ---\n{api_data}\n--- End API Request Data ---"

            # Add response data if present (for structured API responses)
            if hasattr(record, "response") and record.response:
                response_data = record.response
                result += f"\n--- API Response Data ---\n{response_data}\n--- End API Response Data ---"

        # For HTTP client debug messages, improve formatting
        if record.name == "http.client" and record.levelname == "DEBUG":
            if "Header:" in record.getMessage():
                # Format HTTP headers more cleanly
                msg = record.getMessage()
                if "authorization:" in msg.lower():
                    msg = msg.replace(
                        msg.split("authorization:")[1].split("'")[1], "[REDACTED]"
                    )
                result = f"{record.asctime if hasattr(record, 'asctime') else ''} - HTTP - {msg}"
            elif "Sending request:" in record.getMessage():
                # Format HTTP requests more cleanly
                msg = record.getMessage()
                # Redact authorization tokens in request logs
                if "authorization: Bearer" in msg:
                    msg = re.sub(
                        r"authorization: Bearer [^\r\n]+",
                        "authorization: Bearer [REDACTED]",
                        msg,
                    )
                result = f"{record.asctime if hasattr(record, 'asctime') else ''} - HTTP - {msg}"

        return result


def setup_logger(
    verbose: bool = False, debug_api: bool = False, output_dir: str | None = None
) -> logging.Logger:
    """
    Set up and return the logger with appropriate formatting.

    Args:
        verbose: If True, set console handler to DEBUG level; otherwise INFO level
        debug_api: If True, enable detailed API request/response logging
        output_dir: Optional output directory for the main log file

    Returns:
        Configured logger instance
    """
    _DEBUG_API_ENABLED.set(debug_api)

    logger = logging.getLogger("slack_migrator")

    # Clear any existing handlers to prevent duplicate messages
    if logger.handlers:
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

    logger.setLevel(logging.DEBUG)  # Always set logger to DEBUG to capture all logs

    # Create console handler with appropriate level based on verbose flag
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Create the enhanced formatter that handles both verbose and API details
    formatter = EnhancedFormatter(verbose=verbose, include_api_details=debug_api)

    # Set the formatter for the console handler
    console_handler.setFormatter(formatter)

    logger.addHandler(console_handler)

    # Set up main log file if output directory is provided
    if output_dir:
        setup_main_log_file(output_dir, debug_api)

    # Configure API debugging if enabled
    if debug_api:
        # Enable httplib (http.client) debug logging
        http_logger = logging.getLogger("http.client")
        http_logger.setLevel(logging.DEBUG)
        http_logger.propagate = True

        # Enable http.client debug logging via stdlib debuglevel
        _enable_http_client_debug()

        logger.info(
            "API debug logging enabled - Channel-specific API requests/responses will be logged to channel logs only"
        )

    return logger


def _enable_http_client_debug() -> None:
    """
    Enable http.client debug logging by routing debug output through the
    logging module with auth token redaction.

    Only patches the `putheader` method to intercept headers (for redaction).
    This is only used when debug_api=True.
    """
    import http.client

    _orig_putheader = http.client.HTTPConnection.putheader

    http_logger = logging.getLogger("http.client")

    def _debug_putheader(self: Any, header: str, *values: str) -> None:
        if header and values:
            header_value = ", ".join(str(v) for v in values)
            if header.lower() == "authorization":
                header_value = re.sub(r"(Bearer\s+)\S+", r"\1[REDACTED]", header_value)
            http_logger.debug("Header: %s: %s", header, header_value)
        return _orig_putheader(self, header, *values)

    http.client.HTTPConnection.putheader = _debug_putheader  # type: ignore[assignment]


def setup_channel_logger(
    output_dir: str, channel: str, verbose: bool = False, debug_api: bool = False
) -> logging.FileHandler:
    """
    Set up a file handler for channel-specific logging.

    Args:
        output_dir: The output directory path
        channel: The channel name
        verbose: If True, set file handler to DEBUG level; otherwise INFO level

    Returns:
        The file handler for the channel log
    """
    # Create the channel logs directory if it doesn't exist
    logs_dir = os.path.join(output_dir, "channel_logs")
    os.makedirs(logs_dir, exist_ok=True)

    # Create the log file path
    log_file = os.path.join(logs_dir, f"{channel}_migration.log")

    # Create file handler
    file_handler = logging.FileHandler(log_file, mode="w")
    file_handler.setLevel(logging.DEBUG)  # Always use DEBUG level for file handlers

    # Enable immediate flushing to ensure logs are written to disk promptly
    # This is important in case the migration fails and we want to preserve logs
    if hasattr(file_handler, "flush"):
        # Ensure the file handler flushes immediately on each log write
        # This sacrifices some performance for data safety
        old_emit = file_handler.emit

        def immediate_flush_emit(record: logging.LogRecord) -> None:
            """Emit the record and flush immediately.

            Args:
                record: The log record to emit.
            """
            old_emit(record)
            file_handler.flush()

        file_handler.emit = immediate_flush_emit  # type: ignore[method-assign]

    # Create formatter - use EnhancedFormatter with appropriate settings
    formatter = EnhancedFormatter(verbose=verbose, include_api_details=debug_api)
    file_handler.setFormatter(formatter)

    # Create a filter to only include logs for this specific channel and related API calls
    class ChannelFilter(logging.Filter):
        """Filter that only passes records matching a specific channel."""

        def filter(self, record: logging.LogRecord) -> bool:
            """Return True if *record* belongs to this channel.

            Args:
                record: The log record to evaluate.

            Returns:
                True if the record matches this channel.
            """
            # Always include logs that have a channel attribute matching this channel
            record_channel = getattr(record, "channel", None)
            if record_channel == channel:
                return True

            # For API debug logs, only include them if they have the matching channel context
            if debug_api and (
                hasattr(record, "api_data") or hasattr(record, "response")
            ):
                # Only include API logs that have a channel context matching this channel
                return record_channel == channel

            # Include HTTP client logs only when API debug is enabled AND they have matching channel context
            if debug_api and record.name == "http.client":
                return record_channel == channel

            return False

    # Add the filter to the handler
    channel_filter = ChannelFilter()
    file_handler.addFilter(channel_filter)

    # Add handler to the logger
    logger = logging.getLogger("slack_migrator")
    logger.addHandler(file_handler)

    logger.info(f"Channel log file created at: {log_file}", extra={"channel": channel})
    return file_handler


def ensure_channel_log_created(
    output_dir: str, channel: str, dry_run: bool = False
) -> None:
    """
    Ensure a channel log file is created even in dry run mode.

    Args:
        output_dir: The output directory path
        channel: The channel name
        dry_run: Whether this is a dry run
    """
    # Create the channel logs directory if it doesn't exist
    logs_dir = os.path.join(output_dir, "channel_logs")
    os.makedirs(logs_dir, exist_ok=True)

    # Create the log file path
    log_file = os.path.join(logs_dir, f"{channel}_migration.log")

    # Create an empty log file or write a header if it doesn't exist
    if not os.path.exists(log_file):
        try:
            with open(log_file, "w") as f:
                if dry_run:
                    f.write(f"# Channel migration log for {channel} (DRY RUN)\n")
                    f.write(
                        f"# Created at {logging.Formatter().formatTime(logging.LogRecord('', 0, '', 0, '', (), None))}\n"
                    )
                    f.write("# This is a dry run. No actual changes were made.\n\n")
                else:
                    f.write(f"# Channel migration log for {channel}\n")
                    f.write(
                        f"# Created at {logging.Formatter().formatTime(logging.LogRecord('', 0, '', 0, '', (), None))}\n\n"
                    )
                # Ensure content is written to disk immediately
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            # Use print instead of logging to avoid potential recursion issues
            print(f"Warning: Failed to create channel log file for {channel}: {e}")
            return

    logger.debug(
        f"{'[DRY RUN] ' if dry_run else ''}Channel log file ensured at: {log_file}"
    )


def log_with_context(level: int, message: str, **kwargs: Any) -> None:
    """
    Log a message with additional context information.

    Args:
        level: The logging level (e.g., logging.INFO)
        message: The log message
        **kwargs: Additional context to include in the log record
    """
    # Filter out None values from kwargs
    filtered_kwargs = {k: v for k, v in kwargs.items() if v is not None}

    # Filter out reserved logging attributes to avoid conflicts
    reserved_attributes = {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "getMessage",
        "message",
        "asctime",
    }

    # Remove any reserved attributes from kwargs
    filtered_kwargs = {
        k: v for k, v in filtered_kwargs.items() if k not in reserved_attributes
    }

    # Make sure extra attributes don't cause issues with standard formatters
    # by ensuring all potentially missing attributes have default values
    default_extras = {"api_data": "", "response": ""}

    # Only add defaults for API-related logs to avoid unnecessary processing
    if "api_data" in filtered_kwargs or "response" in filtered_kwargs:
        extras = {**default_extras, **filtered_kwargs}
    else:
        extras = filtered_kwargs

    logger = logging.getLogger("slack_migrator")
    logger.log(level, message, extra=extras)


def _extract_api_operation(method: str, url: str) -> str:
    """
    Extract a meaningful API operation description from method and URL.

    Args:
        method: HTTP method (GET, POST, etc.)
        url: The API endpoint URL

    Returns:
        A formatted string describing the API operation
    """
    try:
        # Extract the path and operation from common Google Chat API URLs
        if "chat.googleapis.com" in url:
            if "/spaces?" in url and method == "POST":
                return f"{method} chat.spaces.create"
            elif "/spaces/" in url and "/members?" in url and method == "POST":
                return f"{method} chat.spaces.members.create"
            elif "/spaces/" in url and "/messages?" in url and method == "POST":
                return f"{method} chat.spaces.messages.create"
            elif "/spaces/" in url and "/messages/" in url and method == "GET":
                return f"{method} chat.spaces.messages.get"
            elif "/media/" in url:
                if method == "POST":
                    return f"{method} chat.media.upload"
                elif method == "GET":
                    return f"{method} chat.media.download"

        # Fallback to basic method + simplified URL
        simplified_url = url.split("?")[0]  # Remove query parameters
        if len(simplified_url) > 50:
            simplified_url = "..." + simplified_url[-47:]

        return f"{method} {simplified_url}"

    except Exception:
        # If anything goes wrong, just return the basic info
        return f"{method} {url}"


def log_api_request(
    method: str, url: str, data: dict[str, Any] | None = None, **kwargs: Any
) -> None:
    """
    Log an API request with appropriate detail level based on debug mode.

    Args:
        method: HTTP method (GET, POST, etc.)
        url: The API endpoint URL
        data: Optional request data/payload
        **kwargs: Additional context to include in the log record
    """
    # Only log detailed API requests if in debug mode
    if not is_debug_api_enabled():
        return

    # Always log the basic request info
    log_context = kwargs.copy()

    # Extract API operation from URL for better context
    api_operation = _extract_api_operation(method, url)

    # Add detailed data if available
    if data and isinstance(data, dict):
        data_copy = data.copy()
        # Redact any tokens or sensitive fields
        for key in data_copy:
            if any(
                sensitive in key.lower()
                for sensitive in ["token", "auth", "password", "secret", "key"]
            ):
                data_copy[key] = "[REDACTED]"

        # Add API data to the log context
        log_context["api_data"] = json.dumps(data_copy, indent=2)

    # Log with all available context
    log_with_context(logging.DEBUG, f"🔄 API Request: {api_operation}", **log_context)


def log_api_response(
    status_code: int, url: str, response_data: Any = None, **kwargs: Any
) -> None:
    """
    Log an API response with appropriate detail level based on debug mode.

    Args:
        status_code: HTTP status code
        url: The API endpoint URL
        response_data: Optional response data
        **kwargs: Additional context to include in the log record
    """
    # Only log detailed API responses if in debug mode
    if not is_debug_api_enabled():
        return

    # Always log the basic response info
    log_context = kwargs.copy()

    # Extract API operation from URL for better context
    api_operation = _extract_api_operation("", url).replace(" ", " ").strip()

    # Add status emoji for better readability
    status_emoji = (
        "✅" if 200 <= status_code < 300 else "❌" if status_code >= 400 else "⚠️"
    )

    # Process and add response data if available
    if response_data:
        try:
            if isinstance(response_data, dict) or isinstance(response_data, list):
                # For dict/list, convert to formatted JSON string
                response_str = json.dumps(response_data, indent=2)
                # Truncate if too long
                if len(response_str) > RESPONSE_MAX_LENGTH:
                    response_str = (
                        response_str[:RESPONSE_MAX_LENGTH] + "... [truncated]"
                    )
            else:
                # For other types, use string representation
                response_str = str(response_data)
                if len(response_str) > RESPONSE_FALLBACK_LENGTH:
                    response_str = (
                        response_str[:RESPONSE_FALLBACK_LENGTH] + "... [truncated]"
                    )

            # Add response data to the log context
            log_context["response"] = response_str
        except Exception as e:
            # If there's an error formatting the response, include that info
            log_context["response"] = f"Error formatting response: {e}"

    # Log with all available context
    log_with_context(
        logging.DEBUG,
        f"{status_emoji} API Response: {status_code} from {api_operation}",
        **log_context,
    )


def log_failed_message(channel: str, failed_msg: dict[str, Any]) -> None:
    """
    Log details of a failed message to the channel log.

    Args:
        channel: The channel name
        failed_msg: The failed message data
    """
    logger.error(
        f"Failed to send message: TS={failed_msg.get('ts')}, Error={failed_msg.get('error')}",
        extra={"channel": channel},
    )

    # Log payload details at debug level
    try:
        payload_str = json.dumps(failed_msg.get("payload", {}), indent=2)
        logger.debug(
            f"Failed message payload: {payload_str}", extra={"channel": channel}
        )
    except Exception:
        logger.debug(
            f"Failed message payload (not JSON serializable): {failed_msg.get('payload', {})!r}",
            extra={"channel": channel},
        )


def is_debug_api_enabled() -> bool:
    """Check if API debug logging is enabled.

    Returns:
        True if the ``--debug_api`` flag was set at startup.
    """
    return _DEBUG_API_ENABLED.get()


def get_logger() -> logging.Logger:
    """Get the slack_migrator logger, creating it with defaults if needed.

    Returns:
        The ``slack_migrator`` logger instance.
    """
    slack_logger = logging.getLogger("slack_migrator")
    if not slack_logger.handlers:
        # If no handlers, set up a basic logger
        slack_logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setLevel(logging.INFO)
        formatter = EnhancedFormatter()
        handler.setFormatter(formatter)
        slack_logger.addHandler(handler)
    return slack_logger


# Module logger - will be properly initialized when setup_logger is called
logger = get_logger()
