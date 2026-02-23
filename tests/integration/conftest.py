"""Integration test configuration.

These tests require external service credentials and are skipped by default.
"""

import pytest

skip_no_creds = pytest.mark.skipif(
    True,  # TODO: check for actual credential env var
    reason="Integration tests require service account credentials",
)
