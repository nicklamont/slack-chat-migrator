"""
API utilities for the Slack to Google Chat migration tool
"""

from __future__ import annotations

import copy
import datetime
import email.utils
import functools
import http.client
import json
import logging
import math
import random
import ssl
import threading
import time
from collections import OrderedDict
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import httplib2
from google.auth.exceptions import TransportError
from google.oauth2 import service_account
from googleapiclient.discovery import Resource, build
from googleapiclient.errors import HttpError
from googleapiclient.http import BatchHttpRequest, HttpRequest

from slack_chat_migrator.constants import (
    HTTP_FORBIDDEN,
    HTTP_RATE_LIMIT,
    HTTP_SERVER_ERROR_MIN,
    HTTP_UNAUTHORIZED,
)
from slack_chat_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_chat_migrator.core.config import MigrationConfig

logger = logging.getLogger("slack_chat_migrator")


def escape_drive_query_value(value: str) -> str:
    """Escape a string value for use in a Drive API query parameter.

    The Drive API ``q`` parameter uses single-quoted string literals.
    Backslashes and single quotes inside the value must be escaped to
    prevent query injection.

    Args:
        value: Raw string to embed in a Drive query.

    Returns:
        The escaped string safe for interpolation into ``q='...'``.
    """
    return value.replace("\\", "\\\\").replace("'", "\\'")


REQUIRED_SCOPES = [
    "https://www.googleapis.com/auth/chat.import",
    "https://www.googleapis.com/auth/chat.spaces",
    "https://www.googleapis.com/auth/chat.messages",
    "https://www.googleapis.com/auth/chat.spaces.readonly",
    "https://www.googleapis.com/auth/chat.memberships.readonly",  # For reading space member lists
    "https://www.googleapis.com/auth/drive",  # Full Drive scope covers all drive.file permissions plus shared drives
]

#: Lists custom emoji, so reactions can use them (``use_chat_custom_emoji``).
#: Optional: only requested when that option is on, so existing domain-wide
#: delegation entries keep working.
CUSTOM_EMOJI_SCOPE = "https://www.googleapis.com/auth/chat.customemojis.readonly"


def scopes_for(config: MigrationConfig | None) -> list[str]:
    """Return the OAuth scopes a migration with *config* needs.

    Args:
        config: The migration config, or None for the defaults.

    Returns:
        :data:`REQUIRED_SCOPES`, plus :data:`CUSTOM_EMOJI_SCOPE` when
        ``use_chat_custom_emoji`` is enabled.
    """
    scopes = list(REQUIRED_SCOPES)
    if config is not None and config.use_chat_custom_emoji:
        scopes.append(CUSTOM_EMOJI_SCOPE)
    return scopes


#: Most API clients kept at once.  Each holds its own httplib2 connection
#: pool (about 0.8 MB), and there is one per impersonated user, so the cache
#: is a bounded LRU rather than one entry per user in the workspace.
SERVICE_CACHE_MAX_ENTRIES = 256

_ServiceKey = tuple[str, str, str, str, tuple[str, ...]]

# Cache for service instances: maps key → (service, created_at), LRU order
_service_cache: OrderedDict[_ServiceKey, tuple[Any, float]] = OrderedDict()
# Parsed service-account credentials, per (creds_path, scopes); each user's
# client is derived from them with ``with_subject``.
_credentials_cache: dict[tuple[str, tuple[str, ...]], Any] = {}
_service_cache_lock = threading.Lock()
_SERVICE_CACHE_TTL = 2700  # 45 minutes


def clear_service_cache() -> None:
    """Clear the cached GCP service instances and credentials.

    Call this after authentication failures (401) to force
    re-creation of service objects (including per-user delegates)
    on next use.
    """
    with _service_cache_lock:
        _service_cache.clear()
        _credentials_cache.clear()


# ---------------------------------------------------------------------------
# Retry policy
# ---------------------------------------------------------------------------

#: Upper bound for one backoff sleep (Google recommends capping at 32-64 s).
MAX_BACKOFF_SECONDS = 64.0

#: Total time one request may spend waiting out rate limits (429 and the
#: rate-limit 403s), independent of ``max_retries``.  Chat and Drive quotas
#: are per-minute windows, so a handful of short retries is not enough.
RATE_LIMIT_BUDGET_SECONDS = 300.0

#: Safety valve on rate-limit attempts (e.g. a server sending
#: ``Retry-After: 0`` forever).
RATE_LIMIT_MAX_ATTEMPTS = 50

#: Smallest base delay for rate-limit backoff, so ``retry_delay: 0`` can't
#: hammer a throttled API.
_RATE_LIMIT_MIN_DELAY_SECONDS = 1.0

HTTP_REQUEST_TIMEOUT = 408

#: Error reasons that turn a 403 into "slow down" rather than "forbidden".
#: Drive reports its rate limits this way.
RATE_LIMIT_REASONS = frozenset(
    {
        "rateLimitExceeded",
        "userRateLimitExceeded",
        "sharingRateLimitExceeded",
        "quotaExceeded",
    }
)

#: Network-level failures worth retrying.  Deliberately not a bare
#: ``OSError``: local errors such as ``PermissionError`` or
#: ``FileNotFoundError`` won't go away on retry.
RETRYABLE_NETWORK_ERRORS: tuple[type[BaseException], ...] = (
    TransportError,
    ConnectionError,
    TimeoutError,  # also socket.timeout
    ssl.SSLError,
    http.client.IncompleteRead,
    http.client.RemoteDisconnected,
    httplib2.ServerNotFoundError,
    httplib2.HttpLib2Error,
)

#: googleapiclient objects whose methods lead to an ``execute()`` call.
_WRAPPABLE_TYPES = (Resource, HttpRequest, BatchHttpRequest)

#: RetryWrapper's own attributes (never forwarded to the wrapped object).
_OWN_ATTRS = frozenset(
    {"_wrapped_obj", "_channel_context_getter", "_max_retries", "_retry_delay"}
)


def http_error_reasons(error: HttpError) -> set[str]:
    """Return the ``reason`` values in an API error response.

    Reads both ``HttpError.error_details`` and the JSON body, which may use
    the Drive shape (``error.errors[].reason``) or the google.rpc shape
    (``error.details[].reason``).

    Args:
        error: The HTTP error to inspect.

    Returns:
        The set of reason strings (empty if none could be parsed).
    """
    reasons: set[str] = set()

    def _collect(items: Any) -> None:
        if isinstance(items, list):
            for item in items:
                if isinstance(item, dict) and isinstance(item.get("reason"), str):
                    reasons.add(item["reason"])

    _collect(getattr(error, "error_details", None))
    try:
        data = json.loads(error.content.decode("utf-8"))
    except (AttributeError, UnicodeDecodeError, ValueError):
        data = None
    if isinstance(data, list) and data:
        data = data[0]
    body = data.get("error") if isinstance(data, dict) else None
    if isinstance(body, dict):
        _collect(body.get("errors"))
        _collect(body.get("details"))
    return reasons


def is_rate_limit_error(error: BaseException) -> bool:
    """Return True for 429s and 403s whose reason is a rate limit or quota.

    Args:
        error: Any exception.

    Returns:
        True if *error* is an ``HttpError`` asking the client to slow down.
    """
    if not isinstance(error, HttpError):
        return False
    status = error.resp.status
    if status == HTTP_RATE_LIMIT:
        return True
    return status == HTTP_FORBIDDEN and bool(
        http_error_reasons(error) & RATE_LIMIT_REASONS
    )


def is_retryable_http_error(error: HttpError) -> bool:
    """Return True for 408, 429, 5xx and rate-limit 403 responses.

    Args:
        error: The HTTP error to classify.

    Returns:
        True if the request is worth retrying.
    """
    status = error.resp.status
    return (
        status in (HTTP_REQUEST_TIMEOUT, HTTP_RATE_LIMIT)
        or status >= HTTP_SERVER_ERROR_MIN
        or is_rate_limit_error(error)
    )


def retry_after_seconds(error: HttpError) -> float | None:
    """Parse the ``Retry-After`` header (seconds or an HTTP date).

    Args:
        error: The HTTP error whose response headers to read.

    Returns:
        Seconds to wait (>= 0), or None if the header is absent or invalid.
    """
    resp = getattr(error, "resp", None)
    if resp is None or not hasattr(resp, "get"):
        return None
    raw = resp.get("retry-after")
    if raw is None:
        return None
    value = str(raw).strip()
    try:
        seconds = float(value)
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=datetime.timezone.utc)
        seconds = (when - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


def backoff_seconds(base_delay: float, attempt: int) -> float:
    """Full-jitter exponential backoff: ``uniform(0, min(cap, base * 2**n))``.

    Args:
        base_delay: Delay for the first retry, in seconds.
        attempt: Zero-based retry number.

    Returns:
        Seconds to sleep before the next attempt.
    """
    ceiling = min(MAX_BACKOFF_SECONDS, base_delay * (2.0 ** min(attempt, 32)))
    # Jitter spreads retries out; it has no security role.
    return random.uniform(0, ceiling)  # noqa: S311


class RetryWrapper:
    """Wrapper that adds retry logic to a googleapiclient service.

    Calls that return a googleapiclient ``Resource``, ``HttpRequest`` or
    ``BatchHttpRequest`` (``spaces()``, ``media()``, ``list(...)``,
    ``upload(...)``, ...) return wrapped objects, so every ``execute()`` in
    the chain is retried:

    - 408, 429, 5xx and 403 rate-limit/quota errors, and network errors
      (connection resets, timeouts, DNS failures, truncated responses) are
      retried with full-jitter exponential backoff capped at
      :data:`MAX_BACKOFF_SECONDS`, honouring ``Retry-After``.
    - Rate-limit errors have their own budget of
      :data:`RATE_LIMIT_BUDGET_SECONDS`; other errors are retried
      ``max_retries`` times.
    - Other 4xx errors are raised at once (a 401 also clears the service
      cache).  409s are left to the callers.
    """

    def __init__(
        self,
        wrapped_obj: Any,
        channel_context_getter: Any = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        self._wrapped_obj = wrapped_obj
        self._channel_context_getter = channel_context_getter
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    def _wrap(self, obj: Any) -> RetryWrapper:
        """Wrap *obj* with this wrapper's retry settings."""
        return RetryWrapper(
            obj, self._channel_context_getter, self._max_retries, self._retry_delay
        )

    def __getattr__(self, name: str) -> Any:
        # Guard against recursion while the instance is half-built (copy,
        # pickle) and keep Python's protocol lookups off the wrapped object.
        if name in _OWN_ATTRS or (name.startswith("__") and name.endswith("__")):
            raise AttributeError(name)

        attr = getattr(self._wrapped_obj, name)

        # If this is a callable method, wrap it with retry logic
        if callable(attr):
            if name == "execute":
                # This is an execute method - wrap it with retry
                return self._wrap_execute(attr)

            # For other methods, return a new wrapper that maintains the chain
            def wrapped_method(*args: Any, **kwargs: Any) -> Any:
                """Invoke the method and wrap googleapiclient results.

                Returns:
                    The method result, wrapped in a RetryWrapper if it is a
                    Resource, HttpRequest or BatchHttpRequest.
                """
                result = attr(*args, **kwargs)
                if isinstance(result, _WRAPPABLE_TYPES):
                    return self._wrap(result)
                return result

            return wrapped_method

        return attr

    def __setattr__(self, name: str, value: Any) -> None:
        # Public attribute writes (e.g. ``request.uri = ...`` in
        # ``list_next``) go to the wrapped object; private names stay on the
        # wrapper.
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._wrapped_obj, name, value)

    def __copy__(self) -> RetryWrapper:
        """Copy the wrapped object too, as ``list_next`` expects."""
        return self._wrap(copy.copy(self._wrapped_obj))

    def _build_request_log_context(
        self, execute_method: Any
    ) -> tuple[str | None, dict[str, str], dict[str, str | None] | None]:
        """Build channel context and log kwargs for a retry-wrapped execute call.

        Returns:
            (channel_context, log_kwargs, request_details) tuple.
        """
        channel_context = None
        if self._channel_context_getter and callable(self._channel_context_getter):
            try:
                channel_context = self._channel_context_getter()
            except Exception:
                logger.debug("Failed to get channel context", exc_info=True)

        log_kwargs: dict[str, str] = {"component": "http"}
        if channel_context and isinstance(channel_context, str):
            log_kwargs["channel"] = channel_context

        request_details = self._extract_request_details(execute_method)
        if request_details:
            self._log_api_request(request_details, channel_context)

        return channel_context, log_kwargs, request_details

    def _log_retry(
        self, error: BaseException, delay: float, log_kwargs: dict[str, str]
    ) -> None:
        """Log a retryable error and the upcoming sleep."""
        if isinstance(error, HttpError):
            reasons = ", ".join(sorted(http_error_reasons(error)))
            detail = f" ({reasons})" if reasons else ""
            log_with_context(
                logging.WARNING,
                f"Encountered {error.resp.status} {error.resp.reason}{detail}",
                **log_kwargs,
            )
        else:
            log_with_context(
                logging.WARNING,
                f"Network error: {type(error).__name__}: {error}",
                **log_kwargs,
            )
        log_with_context(
            logging.INFO,
            f"Retrying in {delay:.1f} seconds...",
            **log_kwargs,
        )

    def _retry_delay_for(
        self, error: BaseException, attempt: int, rate_limit_attempt: int
    ) -> float:
        """Return how long to wait before retrying *error*."""
        retry_after = (
            retry_after_seconds(error) if isinstance(error, HttpError) else None
        )
        if is_rate_limit_error(error):
            if retry_after is not None:
                return retry_after
            base = max(self._retry_delay, _RATE_LIMIT_MIN_DELAY_SECONDS)
            return backoff_seconds(base, rate_limit_attempt)
        if retry_after is not None:
            return min(retry_after, MAX_BACKOFF_SECONDS)
        return backoff_seconds(self._retry_delay, attempt)

    def _wrap_execute(self, execute_method: Any) -> Any:
        """Wrap an execute method with retry logic and automatic API logging."""

        @functools.wraps(execute_method)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """Execute the API call, retrying transient failures.

            Returns:
                The result of the underlying ``execute()`` call.
            """
            channel_context, log_kwargs, request_details = (
                self._build_request_log_context(execute_method)
            )

            attempt = 0  # retries of non-rate-limit errors
            rate_limit_attempt = 0
            rate_limit_waited = 0.0

            while True:
                try:
                    result = execute_method(*args, **kwargs)
                    if request_details:
                        status_code = self._extract_status_code(execute_method, result)
                        self._log_api_response(
                            status_code, request_details, result, channel_context
                        )
                    return result
                except HttpError as e:
                    if not is_retryable_http_error(e):
                        if e.resp.status == HTTP_UNAUTHORIZED:
                            clear_service_cache()
                        if request_details:
                            self._log_api_response(
                                e.resp.status, request_details, None, channel_context
                            )
                        log_with_context(
                            logging.WARNING,
                            f"Client error ({e.resp.status}) not retried: {e}",
                            **log_kwargs,
                        )
                        raise
                    error: BaseException = e
                except RETRYABLE_NETWORK_ERRORS as e:
                    error = e

                delay = self._retry_delay_for(error, attempt, rate_limit_attempt)
                if is_rate_limit_error(error):
                    exhausted = (
                        rate_limit_waited + delay > RATE_LIMIT_BUDGET_SECONDS
                        or rate_limit_attempt >= RATE_LIMIT_MAX_ATTEMPTS
                    )
                    rate_limit_attempt += 1
                    rate_limit_waited += delay
                else:
                    exhausted = attempt >= self._max_retries
                    attempt += 1

                if exhausted:
                    if request_details and isinstance(error, HttpError):
                        self._log_api_response(
                            error.resp.status, request_details, None, channel_context
                        )
                    log_with_context(
                        logging.ERROR,
                        f"Max retries reached. Last error: {error}",
                        **log_kwargs,
                    )
                    raise error

                self._log_retry(error, delay, log_kwargs)
                time.sleep(delay)

        return wrapper

    def _extract_request_details(
        self, execute_method: Any
    ) -> dict[str, str | None] | None:
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
            logger.debug("Failed to extract API request details", exc_info=True)
            # If extraction fails, return minimal info
            return {
                "method": "UNKNOWN",  # Don't assume POST
                "uri": "google_api_call",
                "body": None,
            }

    @staticmethod
    def _try_status_from_response(method_self: Any, result: Any) -> int | None:
        """Try to extract a status code from response object attributes.

        Returns the status code if found, or ``None``.
        """
        if hasattr(method_self, "_response") and method_self._response:
            if hasattr(method_self._response, "status"):
                return int(method_self._response.status)
            if hasattr(method_self._response, "status_code"):
                return int(method_self._response.status_code)

        if hasattr(method_self, "response") and method_self.response:
            if hasattr(method_self.response, "status"):
                return int(method_self.response.status)
            if (
                isinstance(method_self.response, tuple)
                and len(method_self.response) > 0
            ):
                resp = method_self.response[0]
                if hasattr(resp, "status"):
                    return int(resp.status)

        if isinstance(result, dict) and "status" in result:
            status = result["status"]
            if isinstance(status, (int, str)) and str(status).isdigit():
                return int(status)

        return None

    @staticmethod
    def _infer_status_from_http_verb(method_self: Any) -> int:
        """Infer a conventional HTTP status code from the request method.

        Falls back to 200 when the HTTP verb cannot be determined.
        """
        _VERB_TO_STATUS = {"POST": 201, "DELETE": 204}
        if not (hasattr(method_self, "method") or hasattr(method_self, "_method")):
            return 200
        method = getattr(method_self, "method", getattr(method_self, "_method", "POST"))
        method = method.upper() if method else "POST"
        return _VERB_TO_STATUS.get(method, 200)

    def _extract_status_code(self, execute_method: Any, result: Any) -> int:
        """Extract the actual HTTP status code from the response."""
        try:
            method_self = (
                execute_method.__self__ if hasattr(execute_method, "__self__") else None
            )
            if method_self:
                status = self._try_status_from_response(method_self, result)
                if status is not None:
                    return status
                return self._infer_status_from_http_verb(method_self)
        except (ValueError, TypeError, AttributeError):
            logging.debug(
                "Could not extract status code from response, defaulting to 200"
            )
        return 200

    def _log_api_request(
        self, request_details: dict[str, str | None], channel_context: str | None
    ) -> None:
        """Log API request automatically if debug mode is enabled."""
        try:
            # Import here to avoid circular imports
            from slack_chat_migrator.utils.logging import is_debug_api_enabled

            if not is_debug_api_enabled():
                return

            # Import the actual logging function only when needed
            from slack_chat_migrator.utils.logging import log_api_request

            # Prepare request data for logging
            request_data = None
            if request_details.get("body"):
                try:
                    # Try to parse body as JSON if it's a string
                    if isinstance(request_details["body"], str):
                        request_data = json.loads(request_details["body"])
                    elif isinstance(request_details["body"], dict):
                        request_data = request_details["body"]
                except (json.JSONDecodeError, TypeError):
                    # If parsing fails, just use string representation
                    request_data = {"body": str(request_details["body"])[:500]}

            method = request_details.get("method")
            uri = request_details.get("uri")
            if method is not None and uri is not None:
                log_api_request(
                    method=method,
                    url=uri,
                    data=request_data,
                    channel=channel_context,
                )
        except Exception:
            logger.debug("API logging failed", exc_info=True)

    def _log_api_response(
        self,
        status_code: int,
        request_details: dict[str, str | None],
        response_data: Any,
        channel_context: str | None,
    ) -> None:
        """Log API response automatically if debug mode is enabled."""
        try:
            # Import here to avoid circular imports
            from slack_chat_migrator.utils.logging import is_debug_api_enabled

            if not is_debug_api_enabled():
                return

            # Import the actual logging function only when needed
            from slack_chat_migrator.utils.logging import log_api_response

            uri = request_details.get("uri")
            if uri is not None:
                log_api_response(
                    status_code=status_code,
                    url=uri,
                    response_data=response_data,
                    channel=channel_context,
                )
        except Exception:
            logger.debug("API logging failed", exc_info=True)


def slack_ts_to_rfc3339(ts: str) -> str:
    """Convert Slack timestamp to RFC3339 format.

    Args:
        ts: Slack timestamp string (e.g. ``"1609459200.000000"``).

    Returns:
        An RFC 3339 datetime string ending in ``Z``.
    """
    parts = ts.split(".", 1)
    secs = parts[0]
    micros = parts[1] if len(parts) > 1 else "000000"
    base = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(int(secs)))
    return f"{base}.{micros}Z"


def _service_account_credentials(creds_path: str, scopes: tuple[str, ...]) -> Any:
    """Load (once) the service-account credentials for *creds_path*.

    Raises:
        FileNotFoundError: If *creds_path* does not exist.
        ValueError: If the credentials file has an invalid format.
    """
    key = (creds_path, scopes)
    with _service_cache_lock:
        cached = _credentials_cache.get(key)
    if cached is not None:
        return cached
    # This is the critical step: The code must explicitly request the
    # scopes that you authorized in the Admin Console.
    try:
        creds = service_account.Credentials.from_service_account_file(
            creds_path, scopes=list(scopes)
        )
    except FileNotFoundError as e:
        raise FileNotFoundError(f"Credential file not found: {creds_path}") from e
    except (ValueError, json.JSONDecodeError) as e:
        raise ValueError(f"Invalid credential file format in {creds_path}: {e}") from e
    with _service_cache_lock:
        _credentials_cache[key] = creds
    return creds


def get_gcp_service(
    creds_path: str,
    user_email: str,
    api: str,
    version: str,
    channel: str | None = None,
    max_retries: int = 3,
    retry_delay: float = 1.0,
    scopes: Sequence[str] | None = None,
) -> Any:
    """Get a Google API client service using service account impersonation.

    Clients are cached per (credentials, user, API, scopes) in a bounded LRU
    cache with a 45-minute TTL; :func:`clear_service_cache` empties it.  The
    service-account key file is parsed once and each user's credentials are
    derived with ``with_subject``.

    Args:
        creds_path: Path to the service account JSON key file.
        user_email: Email of the user to impersonate via domain-wide delegation.
        api: Google API name (e.g. ``"chat"``, ``"drive"``).
        version: API version string (e.g. ``"v1"``).
        channel: Optional channel name for contextual log messages.
        max_retries: Maximum retry attempts for transient errors.
        retry_delay: Initial delay in seconds between retries.
        scopes: OAuth scopes to request (default :data:`REQUIRED_SCOPES`;
            see :func:`scopes_for`).

    Returns:
        A RetryWrapper-wrapped Google API service object.

    Raises:
        FileNotFoundError: If *creds_path* does not exist.
        ValueError: If the credentials file has an invalid format.
    """
    scope_key = tuple(scopes) if scopes is not None else tuple(REQUIRED_SCOPES)
    cache_key: _ServiceKey = (creds_path, user_email, api, version, scope_key)
    with _service_cache_lock:
        if cache_key in _service_cache:
            cached_service, created_at = _service_cache[cache_key]
            if time.time() - created_at > _SERVICE_CACHE_TTL:
                del _service_cache[cache_key]
                log_with_context(
                    logging.DEBUG,
                    f"Evicted stale cached service for {api} as {user_email}",
                    channel=channel,
                )
            else:
                _service_cache.move_to_end(cache_key)
                return cached_service

    # Credential creation intentionally happens outside the lock to avoid
    # blocking other threads. Double-init is harmless (last writer wins).
    try:
        log_with_context(
            logging.DEBUG,
            f"Creating new service for {api} as {user_email} with required scopes.",
            channel=channel,
        )

        creds = _service_account_credentials(creds_path, scope_key)

        # Impersonate the target user
        delegated = creds.with_subject(user_email)

        # Build the API service object
        service = build(api, version, credentials=delegated, cache_discovery=False)

        # Wrap the service with retry logic
        # Use the explicitly passed channel parameter for context
        def get_channel_context() -> str | None:
            """Return the channel name bound at service-creation time.

            Returns:
                The channel name, or None.
            """
            return channel

        wrapped_service = RetryWrapper(
            service, get_channel_context, max_retries, retry_delay
        )

        with _service_cache_lock:
            _service_cache[cache_key] = (wrapped_service, time.time())
            _service_cache.move_to_end(cache_key)
            while len(_service_cache) > SERVICE_CACHE_MAX_ENTRIES:
                _service_cache.popitem(last=False)
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
