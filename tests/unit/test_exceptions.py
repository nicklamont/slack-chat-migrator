"""Tests for the custom exception hierarchy."""

import pytest

from slack_migrator.exceptions import (
    APIError,
    ConfigError,
    ExportError,
    MigrationAbortedError,
    MigratorError,
    PermissionCheckError,
    UserMappingError,
)

EXCEPTION_CLASSES = [
    ConfigError,
    ExportError,
    APIError,
    PermissionCheckError,
    UserMappingError,
    MigrationAbortedError,
]


class TestExceptionHierarchy:
    """Tests for exception types, inheritance, and message handling."""

    @pytest.mark.parametrize("exc_class", EXCEPTION_CLASSES)
    def test_each_exception_can_be_raised_and_caught_by_own_type(self, exc_class):
        with pytest.raises(exc_class):
            raise exc_class("test error")

    @pytest.mark.parametrize("exc_class", EXCEPTION_CLASSES)
    def test_each_exception_is_caught_by_migrator_error(self, exc_class):
        with pytest.raises(MigratorError):
            raise exc_class("caught by base")

    @pytest.mark.parametrize("exc_class", EXCEPTION_CLASSES)
    def test_each_exception_inherits_from_builtin_exception(self, exc_class):
        assert issubclass(exc_class, Exception)

    @pytest.mark.parametrize("exc_class", EXCEPTION_CLASSES)
    def test_each_exception_inherits_from_migrator_error(self, exc_class):
        assert issubclass(exc_class, MigratorError)

    def test_migrator_error_inherits_from_exception(self):
        assert issubclass(MigratorError, Exception)

    def test_migrator_error_can_be_raised_and_caught(self):
        with pytest.raises(MigratorError):
            raise MigratorError("base error")

    @pytest.mark.parametrize("exc_class", [MigratorError, *EXCEPTION_CLASSES])
    def test_message_is_preserved(self, exc_class):
        msg = f"specific message for {exc_class.__name__}"
        with pytest.raises(exc_class, match=msg):
            raise exc_class(msg)

    @pytest.mark.parametrize("exc_class", [MigratorError, *EXCEPTION_CLASSES])
    def test_str_returns_message(self, exc_class):
        msg = "check str output"
        exc = exc_class(msg)
        assert str(exc) == msg

    def test_catching_migrator_error_does_not_catch_unrelated_exceptions(self):
        with pytest.raises(ValueError):
            try:
                raise ValueError("unrelated")
            except MigratorError:
                pytest.fail("MigratorError should not catch ValueError")
