"""
Logging module for the Slack to Google Chat migration tool
"""

import json
import logging
import os
from typing import Any, Dict, Optional

# Module-level flag to track if API debug logging is enabled
_DEBUG_API_ENABLED = False


class JsonFormatter(logging.Formatter):
    def format(self, record):
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

    # Create formatter - always use EnhancedFormatter but conditionally include API details
    formatter = EnhancedFormatter(
        "%(asctime)s - %(levelname)s - %(message)s", include_api_details=debug_api
    )
    file_handler.setFormatter(formatter)

    # Create a filter to include logs that don't have a channel attribute
    # or the channel attribute is empty
    class MainLogFilter(logging.Filter):
        def filter(self, record):
            # Check if the record has a channel attribute
            record_channel = getattr(record, "channel", None)

            # If no channel attribute or empty channel, include in main log
            if record_channel is None or record_channel == "":
                return True

            # If record has a non-empty channel attribute, exclude from main log
            # Channel-specific logs should go to their respective channel log files
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
        fmt=None,
        datefmt=None,
        style="%",
        verbose=False,
        include_api_details=False,
    ):
        # Use more detailed format for verbose mode
        if verbose:
            fmt = "%(asctime)s - %(name)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s"
        elif not fmt:
            fmt = "%(asctime)s - %(levelname)s - %(message)s"

        super().__init__(fmt, datefmt, style)
        self.include_api_details = include_api_details

    def format(self, record):
        # First apply the base format
        result = super().format(record)

        # Only include API details if explicitly enabled
        if self.include_api_details:
            # Add API data if present
            if hasattr(record, "api_data") and record.api_data:
                result += f"\nAPI Data: {record.api_data}"

            # Add response data if present
            if hasattr(record, "response") and record.response:
                result += f"\nResponse: {record.response}"

        return result


def setup_logger(
    verbose: bool = False, debug_api: bool = False, output_dir: Optional[str] = None
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
    global _DEBUG_API_ENABLED
    _DEBUG_API_ENABLED = debug_api

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

        # Add a handler to log HTTP traffic to a separate file if output_dir is provided
        if output_dir:
            # Create formatter for API logs with API details always enabled
            api_formatter = EnhancedFormatter(include_api_details=True)

            # Set up API debug log file for HTTP client logs
            api_log_file = os.path.join(output_dir, "api_debug.log")
            api_handler = logging.FileHandler(api_log_file, mode="w")
            api_handler.setLevel(logging.DEBUG)
            api_handler.setFormatter(api_formatter)
            http_logger.addHandler(api_handler)

            # Create a separate file specifically for our structured API logs
            structured_api_log_file = os.path.join(
                output_dir, "structured_api_debug.log"
            )
            structured_api_handler = logging.FileHandler(
                structured_api_log_file, mode="w"
            )
            structured_api_handler.setLevel(logging.DEBUG)
            structured_api_handler.setFormatter(api_formatter)

            # Only include records that have api_data or response attributes
            def api_filter(record):
                return hasattr(record, "api_data") or hasattr(record, "response")

            structured_api_handler.addFilter(api_filter)
            logger.addHandler(structured_api_handler)

            logger.info(
                f"API debug logging enabled, writing to {api_log_file} and {structured_api_log_file}"
            )

            # Patch http.client to log complete request/response data
            _patch_http_client_for_debug()

            logger.info(f"API debug logging enabled, writing to {api_log_file}")
        else:
            logger.info("API debug logging enabled, writing to console")

    return logger


def _patch_http_client_for_debug():
    """
    Patch http.client to log complete request/response data.
    This is only used when debug_api=True.
    """
    import http.client

    # Save the original methods
    _orig_send = http.client.HTTPConnection.send
    _orig_putheader = http.client.HTTPConnection.putheader

    http_logger = logging.getLogger("http.client")

    def _debug_send(self, data):
        if hasattr(self, "_http_vsn_str") and self._http_vsn_str:
            http_logger.debug(f"Sending request: {data[:1024]}")
        return _orig_send(self, data)

    def _debug_putheader(self, header, *values):
        if header and values:
            header_value = ", ".join(str(v) for v in values)
            # Don't log Authorization headers with tokens
            if header.lower() == "authorization":
                http_logger.debug(f"Header: {header}: [REDACTED]")
            else:
                http_logger.debug(f"Header: {header}: {header_value}")
        return _orig_putheader(self, header, *values)

    # Replace the methods with debug versions
    http.client.HTTPConnection.send = _debug_send
    http.client.HTTPConnection.putheader = _debug_putheader


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

    # Create formatter - use EnhancedFormatter with appropriate settings
    formatter = EnhancedFormatter(verbose=verbose, include_api_details=debug_api)
    file_handler.setFormatter(formatter)

    # Create a filter to only include logs for this specific channel
    class ChannelFilter(logging.Filter):
        def filter(self, record):
            # Only include log messages that have a channel attribute matching this channel
            record_channel = getattr(record, "channel", None)
            return record_channel == channel

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


def log_api_request(
    method: str, url: str, data: Optional[Dict] = None, **kwargs: Any
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
    log_with_context(logging.DEBUG, f"API Request: {method} {url}", **log_context)


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

    # Process and add response data if available
    if response_data:
        try:
            if isinstance(response_data, dict) or isinstance(response_data, list):
                # For dict/list, convert to formatted JSON string
                response_str = json.dumps(response_data, indent=2)
                # Truncate if too long
                if len(response_str) > 2000:
                    response_str = response_str[:2000] + "... [truncated]"
            else:
                # For other types, use string representation
                response_str = str(response_data)
                if len(response_str) > 1000:
                    response_str = response_str[:1000] + "... [truncated]"

            # Add response data to the log context
            log_context["response"] = response_str
        except Exception as e:
            # If there's an error formatting the response, include that info
            log_context["response"] = f"Error formatting response: {e}"

    # Log with all available context
    log_with_context(
        logging.DEBUG, f"API Response: {status_code} from {url}", **log_context
    )


def log_failed_message(channel: str, failed_msg: Dict[str, Any]) -> None:
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
    except:
        logger.debug(
            f"Failed message payload (not JSON serializable): {repr(failed_msg.get('payload', {}))}",
            extra={"channel": channel},
        )


def is_debug_api_enabled() -> bool:
    """Check if API debug logging is enabled."""
    return _DEBUG_API_ENABLED


def get_logger():
    """Get the slack_migrator logger, creating it with defaults if needed."""
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
