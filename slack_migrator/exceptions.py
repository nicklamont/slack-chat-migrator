"""Custom exception hierarchy for the Slack to Google Chat migration tool."""


class MigratorError(Exception):
    """Base exception for all migration-related errors."""


class ConfigError(MigratorError):
    """Raised when configuration is invalid or missing."""


class ExportError(MigratorError):
    """Raised when the Slack export data is invalid or unreadable."""


class APIError(MigratorError):
    """Raised when a Google API call fails in an unrecoverable way."""


class PermissionCheckError(MigratorError):
    """Raised when required API permissions are missing."""


class UserMappingError(MigratorError):
    """Raised when user mapping fails (no valid users, missing files, etc.)."""


class MigrationAbortedError(MigratorError):
    """Raised when the migration is aborted due to errors exceeding thresholds."""
