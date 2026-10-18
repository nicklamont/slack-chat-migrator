"""Custom exception hierarchy for the Slack to Google Chat migration tool."""


class MigratorError(Exception):
    """Base exception for all migration-related errors."""


class ConfigError(MigratorError, ValueError):
    """Raised when configuration is invalid or missing.

    Also a ``ValueError``, so callers that validated config values by catching
    ``ValueError`` keep working.
    """


class ExportError(MigratorError):
    """Raised when the Slack export data is invalid or unreadable."""


class APIError(MigratorError):
    """Raised when a Google API call fails in an unrecoverable way."""


class PermissionCheckError(MigratorError):
    """Raised when required API permissions are missing."""


class UserMappingError(MigratorError):
    """Raised when user mapping fails (no valid users, missing files, etc.)."""


class DriveQuotaError(MigratorError):
    """Raised when Drive says a quota is used up; it stops the run.

    Every later Drive call would fail the same way, as with a full disk, so
    the run stops instead of failing each file in turn.  The file being
    handled gets no result, so it isn't marked as failed: a later run
    (``migrate --resume``) brings it over once the quota allows.

    Attributes:
        reason: Drive's error reason, e.g. ``dailyLimitExceeded``.
    """

    def __init__(self, message: str, reason: str) -> None:
        super().__init__(message)
        self.reason = reason


class MigrationAbortedError(MigratorError):
    """Raised when the migration is aborted due to errors exceeding thresholds."""


class ConfirmationRequiredError(MigratorError):
    """Raised when a confirmation prompt is needed but stdin isn't interactive."""


class SpacePermissionError(APIError):
    """Raised when space creation fails due to a 403 permission error."""
