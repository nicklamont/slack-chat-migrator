"""
API utilities for the Slack to Google Chat migration tool
"""

import functools
import inspect
import logging
import time
from typing import Any, Dict, Optional

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from slack_migrator.utils.logging import log_with_context

REQUIRED_SCOPES = [
    "https://www.googleapis.com/auth/chat.import",
    "https://www.googleapis.com/auth/chat.spaces",
    "https://www.googleapis.com/auth/chat.messages",
    "https://www.googleapis.com/auth/chat.spaces.readonly",
    "https://www.googleapis.com/auth/chat.memberships.readonly",  # For reading space member lists
    "https://www.googleapis.com/auth/drive",  # Full Drive scope covers all drive.file permissions plus shared drives
]

# Cache for service instances
_service_cache: Dict[str, Any] = {}


class RetryWrapper:
    """Wrapper that adds retry logic to any object's methods."""

    def __init__(self, wrapped_obj, channel_context_getter=None, retry_config=None):
        self._wrapped_obj = wrapped_obj
        self._channel_context_getter = channel_context_getter
        self._retry_config = retry_config or {}

    def __getattr__(self, name):
        attr = getattr(self._wrapped_obj, name)

        # If this is a callable method, wrap it with retry logic
        if callable(attr):
            if name == "execute":
                # This is an execute method - wrap it with retry
                return self._wrap_execute(attr)
            else:
                # For other methods, return a new wrapper that maintains the chain
                def wrapped_method(*args, **kwargs):
                    result = attr(*args, **kwargs)
                    # If the result has methods that might need retry, wrap it too
                    if (
                        hasattr(result, "execute")
                        or hasattr(result, "list")
                        or hasattr(result, "create")
                    ):
                        return RetryWrapper(
                            result, self._channel_context_getter, self._retry_config
                        )
                    return result

                return wrapped_method

        return attr

    def _wrap_execute(self, execute_method):
        """Wrap an execute method with retry logic and automatic API logging."""

        @functools.wraps(execute_method)
        def wrapper(*args, **kwargs):
            # Get retry config from configuration or use defaults
            max_retries = self._retry_config.get("max_retries", 3)
            initial_delay = self._retry_config.get("retry_delay", 1)
            max_delay = 60
            backoff_factor = 2.0

            # Try to get channel context
            channel_context = None
            if self._channel_context_getter and callable(self._channel_context_getter):
                try:
                    channel_context = self._channel_context_getter()
                except Exception:
                    pass

            log_kwargs = {"component": "http"}
            if channel_context and isinstance(channel_context, str):
                log_kwargs["channel"] = channel_context

            # Extract request details for automatic API logging
            request_details = self._extract_request_details(execute_method)

            # Log API request automatically if debug mode is enabled
            if request_details:
                self._log_api_request(request_details, channel_context)

            delay = initial_delay
            last_exception = None
            request_logged = False

            for attempt in range(max_retries + 1):
                try:
                    result = execute_method(*args, **kwargs)

                    # Log successful API response automatically if debug mode is enabled (only on final success)
                    if request_details and not request_logged:
                        # Extract actual status code from the response
                        status_code = self._extract_status_code(execute_method, result)
                        self._log_api_response(
                            status_code, request_details, result, channel_context
                        )

                    return result
                except HttpError as e:
                    last_exception = e

                    # Log failed API response automatically if debug mode is enabled (only on final failure)
                    if (
                        request_details
                        and not request_logged
                        and attempt == max_retries
                    ):
                        self._log_api_response(
                            e.resp.status, request_details, None, channel_context
                        )

                    # Don't retry client errors (4xx) except rate limits (429)
                    if e.resp.status // 100 == 4 and e.resp.status != 429:
                        log_with_context(
                            logging.WARNING,
                            f"Client error ({e.resp.status}) not retried: {e}",
                            **log_kwargs,
                        )
                        raise

                    log_with_context(
                        logging.WARNING,
                        f"Encountered {e.resp.status} {e.resp.reason}",
                        **log_kwargs,
                    )

                    if attempt < max_retries:
                        sleep_time = min(delay * (backoff_factor**attempt), max_delay)
                        log_with_context(
                            logging.INFO,
                            f"Retrying in {sleep_time:.1f} seconds...",
                            **log_kwargs,
                        )
                        time.sleep(sleep_time)
                    else:
                        log_with_context(
                            logging.ERROR,
                            f"Max retries reached. Last error: {e}",
                            **log_kwargs,
                        )
                        raise
                except AttributeError as e:
                    # Special handling for 'Resource' object has no attribute 'create'
                    last_exception = e
                    if "has no attribute 'create'" in str(e):
                        log_with_context(
                            logging.WARNING,
                            f"API client error: {e}",
                            **log_kwargs,
                        )
                        if attempt < max_retries:
                            sleep_time = min(
                                delay * (backoff_factor**attempt), max_delay
                            )
                            log_with_context(
                                logging.INFO,
                                f"Retrying in {sleep_time:.1f} seconds...",
                                **log_kwargs,
                            )
                            time.sleep(sleep_time)
                        else:
                            log_with_context(
                                logging.ERROR,
                                f"Max retries reached. Last error: {e}",
                                **log_kwargs,
                            )
                            raise
                    else:
                        # Re-raise other attribute errors
                        raise
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        sleep_time = min(delay * (backoff_factor**attempt), max_delay)
                        log_with_context(
                            logging.INFO,
                            f"Retrying in {sleep_time:.1f} seconds...",
                            **log_kwargs,
                        )
                        time.sleep(sleep_time)
                    else:
                        log_with_context(
                            logging.ERROR,
                            f"Max retries reached. Last error: {e}",
                            **log_kwargs,
                        )
                        raise

            if last_exception:
                raise last_exception
            raise RuntimeError("Exited retry loop unexpectedly.")

        return wrapper

    def _extract_request_details(self, execute_method):
        """Extract request details from the API method for logging purposes."""
        try:
            # Try to get the underlying HttpRequest object
            method_self = (
                execute_method.__self__ if hasattr(execute_method, "__self__") else None
            )

            if not method_self:
                return None

            # Common attributes we can extract from GoogleAPI HttpRequest objects
            http_method = getattr(
                method_self, "method", getattr(method_self, "_method", None)
            )
            uri = getattr(method_self, "uri", getattr(method_self, "_uri", None))
            body = getattr(method_self, "body", getattr(method_self, "_body", None))

            # If we don't have basic info, try to infer from method chain
            if not uri and hasattr(method_self, "methodId"):
                # This is a GoogleAPI service method - we can get some info
                method_id = method_self.methodId
                uri = f"googleapis.com/{method_id.replace('.', '/')}"

            # If we still don't have an HTTP method, try to infer it
            if not http_method:
                # Look for clues in the URI or method name
                if uri and any(
                    keyword in uri.lower() for keyword in ["create", "insert"]
                ):
                    http_method = "POST"
                elif uri and any(
                    keyword in uri.lower() for keyword in ["update", "patch"]
                ):
                    http_method = (
                        "PUT"  # or PATCH, but PUT is more common in Google APIs
                    )
                elif uri and any(
                    keyword in uri.lower() for keyword in ["delete", "remove"]
                ):
                    http_method = "DELETE"
                elif uri and any(
                    keyword in uri.lower() for keyword in ["list", "get", "search"]
                ):
                    http_method = "GET"
                else:
                    # Default to POST for Google APIs when we can't determine the method
                    http_method = "POST"

            return {
                "method": http_method,
                "uri": uri or "unknown_endpoint",
                "body": body,
            }
        except Exception:
            # If extraction fails, return minimal info
            return {
                "method": "UNKNOWN",  # Don't assume POST
                "uri": "google_api_call",
                "body": None,
            }

    def _extract_status_code(self, execute_method, result):
        """Extract the actual HTTP status code from the response."""
        try:
            # Try to get the status code from the underlying HTTP response
            method_self = (
                execute_method.__self__ if hasattr(execute_method, "__self__") else None
            )

            if method_self:
                # Look for common attributes that contain status information
                # Google API client libraries sometimes store this in different places
                if hasattr(method_self, "_response") and method_self._response:
                    if hasattr(method_self._response, "status"):
                        return int(method_self._response.status)
                    elif hasattr(method_self._response, "status_code"):
                        return int(method_self._response.status_code)

                # Some clients store it in the httplib2 response object
                if hasattr(method_self, "response") and method_self.response:
                    if hasattr(method_self.response, "status"):
                        return int(method_self.response.status)
                    elif (
                        isinstance(method_self.response, tuple)
                        and len(method_self.response) > 0
                    ):
                        # httplib2 returns (response, content) tuple
                        resp = method_self.response[0]
                        if hasattr(resp, "status"):
                            return int(resp.status)

                # Check if the result itself contains status information
                if isinstance(result, dict) and "status" in result:
                    status = result["status"]
                    if isinstance(status, (int, str)) and str(status).isdigit():
                        return int(status)

            # If we can't extract the actual status code, infer from the HTTP method
            # This is a reasonable fallback based on REST conventions
            if hasattr(method_self, "method") or hasattr(method_self, "_method"):
                method = getattr(
                    method_self, "method", getattr(method_self, "_method", "POST")
                )
                method = method.upper() if method else "POST"

                # Standard HTTP status codes for successful operations
                if method == "POST":
                    return 201  # Created
                elif method == "PUT":
                    return 200  # OK
                elif method == "DELETE":
                    return 204  # No Content
                elif method == "PATCH":
                    return 200  # OK
                else:  # GET and others
                    return 200  # OK

        except (ValueError, TypeError, AttributeError):
            pass

        # Final fallback - assume 200 OK for successful responses
        return 200

    def _log_api_request(self, request_details, channel_context):
        """Log API request automatically if debug mode is enabled."""
        try:
            # Import here to avoid circular imports
            from slack_migrator.utils.logging import is_debug_api_enabled

            if not is_debug_api_enabled():
                return

            # Import the actual logging function only when needed
            from slack_migrator.utils.logging import log_api_request

            # Prepare request data for logging
            request_data = None
            if request_details.get("body"):
                try:
                    # Try to parse body as JSON if it's a string
                    import json

                    if isinstance(request_details["body"], str):
                        request_data = json.loads(request_details["body"])
                    elif isinstance(request_details["body"], dict):
                        request_data = request_details["body"]
                except (json.JSONDecodeError, TypeError):
                    # If parsing fails, just use string representation
                    request_data = {"body": str(request_details["body"])[:500]}

            log_api_request(
                method=request_details["method"],
                url=request_details["uri"],
                data=request_data,
                channel=channel_context,
            )
        except (ImportError, AttributeError, Exception):
            # Silently ignore logging errors to not break API calls
            pass

    def _log_api_response(
        self, status_code, request_details, response_data, channel_context
    ):
        """Log API response automatically if debug mode is enabled."""
        try:
            # Import here to avoid circular imports
            from slack_migrator.utils.logging import is_debug_api_enabled

            if not is_debug_api_enabled():
                return

            # Import the actual logging function only when needed
            from slack_migrator.utils.logging import log_api_response

            log_api_response(
                status_code=status_code,
                url=request_details["uri"],
                response_data=response_data,
                channel=channel_context,
            )
        except (ImportError, AttributeError, Exception):
            # Silently ignore logging errors to not break API calls
            pass


def slack_ts_to_rfc3339(ts: str) -> str:
    """Convert Slack timestamp to RFC3339 format."""
    secs, micros = ts.split(".")
    base = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(int(secs)))
    return f"{base}.{micros}Z"


def get_gcp_service(
    creds_path: str,
    user_email: str,
    api: str,
    version: str,
    channel: Optional[str] = None,
    retry_config: Optional[Dict[str, Any]] = None,
) -> Any:
    """Get a Google API client service using service account impersonation."""
    cache_key = f"{creds_path}:{user_email}:{api}:{version}"
    if cache_key in _service_cache:
        log_with_context(
            logging.DEBUG,
            f"Using cached service for {api} as {user_email}",
            channel=channel,
        )
        return _service_cache[cache_key]

    try:
        log_with_context(
            logging.DEBUG,
            f"Creating new service for {api} as {user_email} with required scopes.",
            channel=channel,
        )

        # This is the critical step: The code must explicitly request the
        # scopes that you authorized in the Admin Console.
        creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=REQUIRED_SCOPES
        )

        # Impersonate the target user
        delegated = creds.with_subject(user_email)

        # Build the API service object
        service = build(api, version, credentials=delegated, cache_discovery=False)

        # Wrap the service with retry logic
        # Create a channel context getter that tries multiple sources
        def get_channel_context():
            # First try the explicitly passed channel
            if channel:
                return channel

            # Try to get from current call stack or global state
            # This is a fallback for when channel isn't explicitly passed
            try:
                frame = inspect.currentframe()
                while frame:
                    local_vars = frame.f_locals
                    # Look for common channel variable names
                    for var_name in ["channel", "current_channel"]:
                        if var_name in local_vars and isinstance(
                            local_vars[var_name], str
                        ):
                            return local_vars[var_name]

                    # Look for migrator object with current_channel
                    if "migrator" in local_vars:
                        migrator = local_vars["migrator"]
                        if (
                            hasattr(migrator, "current_channel")
                            and migrator.current_channel
                        ):
                            return migrator.current_channel

                    # Look for self with migrator or current channel
                    if "self" in local_vars:
                        self_obj = local_vars["self"]
                        if hasattr(self_obj, "migrator") and hasattr(
                            self_obj.migrator, "current_channel"
                        ):
                            return self_obj.migrator.current_channel
                        elif hasattr(self_obj, "_get_current_channel"):
                            return self_obj._get_current_channel()

                    frame = frame.f_back
            except Exception:
                pass

            return None

        wrapped_service = RetryWrapper(service, get_channel_context, retry_config)

        _service_cache[cache_key] = wrapped_service
        return wrapped_service
    except Exception as e:
        log_with_context(
            logging.ERROR,
            f"Failed to create {api} service: {e}",
            user_email=user_email,
            api=api,
            version=version,
        )
        raise
