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

from slack_migrator.utils.logging import log_with_context, logger

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
        """Wrap an execute method with retry logic."""

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

            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return execute_method(*args, **kwargs)
                except HttpError as e:
                    last_exception = e
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
