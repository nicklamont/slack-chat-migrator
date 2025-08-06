"""
API utilities for the Slack to Google Chat migration tool
"""

import functools
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

# Global config for retry settings
_retry_config = None


def set_global_retry_config(config, channel=None):
    """
    Set global retry configuration for all retry decorators.

    Args:
        config: The configuration dictionary
        channel: Optional channel name for logging
    """
    global _retry_config
    _retry_config = config
    if channel:
        log_with_context(
            logging.DEBUG,
            f"Set global retry config: max_retries={config.get('max_retries', 3)}, retry_delay={config.get('retry_delay', 2)}",
            channel=channel,
        )
    else:
        logger.debug(
            f"Set global retry config: max_retries={config.get('max_retries', 3)}, retry_delay={config.get('retry_delay', 2)}"
        )


def retry(
    max_retries: int = 3,
    initial_delay: float = 1,
    max_delay: float = 60,
    backoff_factor: float = 2.0,
    config: Optional[Dict[str, Any]] = None,
):
    """Decorator for retrying API calls with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        backoff_factor: Factor by which the delay increases
        config: Configuration dictionary that may contain retry settings
    """
    # Use config values if provided, or fall back to global config
    global _retry_config
    effective_config = config or _retry_config

    if effective_config:
        max_retries = effective_config.get("max_retries", max_retries)
        initial_delay = effective_config.get("retry_delay", initial_delay)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except HttpError as e:
                    last_exception = e
                    # Don't retry client errors (4xx) except rate limits (429)
                    if e.resp.status // 100 == 4 and e.resp.status != 429:
                        logger.warning(
                            f"Client error ({e.resp.status}) not retried: {e}"
                        )
                        raise

                    log_with_context(
                        logging.WARNING,
                        f"Encountered {e.resp.status} {e.resp.reason}",
                        component="http",
                    )

                    if attempt < max_retries:
                        sleep_time = min(delay * (backoff_factor**attempt), max_delay)
                        logger.info(f"Retrying in {sleep_time:.1f} seconds...")
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"Max retries reached. Last error: {e}")
                        raise
                except AttributeError as e:
                    # Special handling for 'Resource' object has no attribute 'create'
                    last_exception = e
                    if "has no attribute 'create'" in str(e):
                        log_with_context(
                            logging.WARNING,
                            f"API client error: {e}",
                            component="http",
                        )
                        if attempt < max_retries:
                            sleep_time = min(
                                delay * (backoff_factor**attempt), max_delay
                            )
                            logger.info(f"Retrying in {sleep_time:.1f} seconds...")
                            time.sleep(sleep_time)
                        else:
                            logger.error(f"Max retries reached. Last error: {e}")
                            raise
                    else:
                        # Re-raise other attribute errors
                        raise
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        sleep_time = min(delay * (backoff_factor**attempt), max_delay)
                        logger.info(f"Retrying in {sleep_time:.1f} seconds...")
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"Max retries reached. Last error: {e}")
                        raise

            if last_exception:
                raise last_exception
            raise RuntimeError("Exited retry loop unexpectedly.")

        return wrapper

    return decorator


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

        _service_cache[cache_key] = service
        return service
    except Exception as e:
        logger.error(f"Failed to create {api} service: {e}", exc_info=True)
        raise
