"""
API utilities for the Slack to Google Chat migration tool
"""

import functools
import logging
import time
from typing import Callable, Any, Dict

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from slack_migrator.utils.logging import logger, log_with_context

# THE FIX: This is the definitive list of scopes the application MUST request.
# It now includes 'chat.spaces.readonly' to guarantee the GetSpace method is allowed.
REQUIRED_SCOPES = [
    "https://www.googleapis.com/auth/chat.import",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/chat.spaces",
    "https://www.googleapis.com/auth/chat.messages",
    "https://www.googleapis.com/auth/chat.spaces.readonly",
]

# Cache for service instances
_service_cache: Dict[str, Any] = {}


def retry(
    max_retries: int = 3,
    initial_delay: float = 1,
    max_delay: float = 60,
    backoff_factor: float = 2.0,
):
    """Decorator for retrying API calls with exponential backoff."""

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except HttpError as e:
                    if e.resp.status // 100 == 4 and e.resp.status != 429:
                        logger.warning(
                            f"Client error ({e.resp.status}) not retried: {e}"
                        )
                        raise
                    log_with_context(
                        logging.WARNING,
                        f"Encountered {e.resp.status} {e.resp.reason}",
                        module="http",
                    )
                    if attempt < max_retries:
                        sleep_time = min(delay * (backoff_factor**attempt), max_delay)
                        logger.info(f"Retrying in {sleep_time:.1f} seconds...")
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"Max retries reached. Last error: {e}")
                        raise
                except Exception as e:
                    if attempt < max_retries:
                        sleep_time = min(delay * (backoff_factor**attempt), max_delay)
                        logger.info(f"Retrying in {sleep_time:.1f} seconds...")
                        time.sleep(sleep_time)
                    else:
                        logger.error(f"Max retries reached. Last error: {e}")
                        raise
            raise RuntimeError("Exited retry loop unexpectedly.")

        return wrapper

    return decorator


def slack_ts_to_rfc3339(ts: str) -> str:
    """Convert Slack timestamp to RFC3339 format."""
    secs, micros = ts.split(".")
    base = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(int(secs)))
    return f"{base}.{micros}Z"


def get_gcp_service(creds_path: str, user_email: str, api: str, version: str) -> Any:
    """Get a Google API client service using service account impersonation."""
    cache_key = f"{creds_path}:{user_email}:{api}:{version}"
    if cache_key in _service_cache:
        logger.debug(f"Using cached service for {api} as {user_email}")
        return _service_cache[cache_key]

    try:
        logger.debug(
            f"Creating new service for {api} as {user_email} with required scopes."
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
