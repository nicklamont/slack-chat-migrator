"""Integration test configuration.

These tests require external service credentials and are skipped by default.
Set the GOOGLE_APPLICATION_CREDENTIALS environment variable to a service
account JSON file to enable them.
"""

import os

import pytest

skip_no_creds = pytest.mark.skipif(
    not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"),
    reason="Integration tests require GOOGLE_APPLICATION_CREDENTIALS env var",
)
