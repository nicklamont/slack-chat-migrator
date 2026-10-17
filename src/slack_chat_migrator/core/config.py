"""
Configuration module for the Slack to Google Chat migration tool.

This module loads and validates configuration settings from YAML files and
determines which Slack channels should be processed based on the
configuration.
"""

from __future__ import annotations

import datetime
import difflib
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from enum import Enum
from pathlib import Path
from typing import Any

import yaml

from slack_chat_migrator.exceptions import ConfigError
from slack_chat_migrator.utils.logging import log_with_context

#: Slack conversation types, in the order they are listed in the export.
CONVERSATION_TYPES = ("public", "private", "mpim", "dm")

#: Who becomes the space manager after ``completeImport``.
SPACE_MANAGER_CHOICES = ("admin", "creator")

#: Shared drive name used when neither ``shared_drive.id`` nor
#: ``shared_drive.name`` is set (a configured ``id`` wins over any name).
DEFAULT_SHARED_DRIVE_NAME = "Imported Slack Attachments"


class ImportCompletionStrategy(str, Enum):
    """Strategy for completing import mode when errors occur."""

    SKIP_ON_ERROR = "skip_on_error"
    FORCE_COMPLETE = "force_complete"


@dataclass
class SharedDriveConfig:
    """Configuration for the shared Google Drive used for file attachments.

    ``name`` is never blank: an empty name means
    :data:`DEFAULT_SHARED_DRIVE_NAME`.  ``id`` is stripped, and an empty or
    whitespace-only ``id`` means "not set" (None).
    """

    name: str = DEFAULT_SHARED_DRIVE_NAME
    id: str | None = None

    def __post_init__(self) -> None:
        """Apply the default to a blank name and check the field types.

        Raises:
            ConfigError: If ``name`` or ``id`` isn't a string.
        """
        if not isinstance(self.name, str):
            raise ConfigError(
                f"shared_drive.name must be a string, got {_describe(self.name)}"
            )
        if not self.name.strip():
            self.name = DEFAULT_SHARED_DRIVE_NAME
        if self.id is not None and not isinstance(self.id, str):
            raise ConfigError(
                f"shared_drive.id must be a string, got {_describe(self.id)}"
            )
        if self.id is not None:
            self.id = self.id.strip() or None

    @classmethod
    def from_dict(cls, data: Any) -> SharedDriveConfig:
        """Create a SharedDriveConfig from a raw config dictionary.

        Args:
            data: Dictionary with optional ``name`` and ``id`` keys, or None.

        Returns:
            A SharedDriveConfig instance with values from *data* or defaults.

        Raises:
            ConfigError: If *data* is not a mapping or a value isn't a string.
        """
        if data is None:
            return cls()
        if not isinstance(data, Mapping):
            raise ConfigError(
                "Config option 'shared_drive' must be a mapping with 'name'"
                f" and/or 'id', got {_describe(data)}"
            )
        unknown = sorted(str(k) for k in data if k not in ("name", "id"))
        if unknown:
            log_with_context(
                logging.WARNING,
                "Ignoring unknown shared_drive option(s): " + ", ".join(unknown),
            )
        name = _get_str(data, "name", DEFAULT_SHARED_DRIVE_NAME, prefix="shared_drive.")
        drive_id = _get_str(data, "id", "", prefix="shared_drive.") or None
        return cls(name=name, id=drive_id)


@dataclass
class MigrationConfig:
    """Typed configuration for the migration tool.

    All fields have sensible defaults matching the previous dict-based config.
    """

    # Channel filtering
    exclude_channels: list[str] = field(default_factory=list)
    include_channels: list[str] = field(default_factory=list)

    # User mapping
    user_mapping_overrides: dict[str, str] = field(default_factory=dict)
    email_domain_override: str = ""
    ignore_bots: bool = False

    # Error handling
    abort_on_error: bool = False
    max_failure_percentage: int = 10
    import_completion_strategy: ImportCompletionStrategy = (
        ImportCompletionStrategy.SKIP_ON_ERROR
    )
    cleanup_on_error: bool = False

    # Retry
    max_retries: int = 3
    retry_delay: float = 2

    # Shared drive
    shared_drive: SharedDriveConfig = field(default_factory=SharedDriveConfig)

    # Identity: Google Workspace domains whose users are "internal" (can be
    # impersonated).  Empty means "the workspace admin's domain only".
    workspace_domains: list[str] = field(default_factory=list)

    # Optional Slack token (``files:read``) used as a Bearer header when
    # downloading export files.  Falls back to the ``SLACK_TOKEN`` env var.
    # Kept out of repr(), so the repr-based views (print(config), pdb's
    # ``p config``, tracebacks with locals) don't show it; the attribute
    # itself stays readable.
    slack_token: str = field(default="", repr=False)

    # Durable migration state (created spaces, import deadlines, resume
    # points, uploaded files).  Empty means
    # ``<export_root>/.slack_chat_migrator_state.json``.
    state_file: str = ""

    # Slack conversation types to migrate: public channels, private
    # channels, group DMs (mpim) and direct messages (dm).
    conversation_types: list[str] = field(
        default_factory=lambda: list(CONVERSATION_TYPES)
    )

    # Performance
    max_parallel_channels: int = 1
    file_workers: int = 4

    # Optional features
    custom_emoji_map: dict[str, str] = field(default_factory=dict)
    use_chat_custom_emoji: bool = False
    quote_shared_messages: bool = False
    migrate_pins: bool = True
    convert_canvases: bool = True
    restrict_private_channel_folders: bool = True
    space_manager: str = "admin"
    public_channel_audience: str = ""
    announcement_channels: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Validate and normalise the values after initialization.

        Checks every field's type (so direct construction fails like
        ``from_dict`` does), coerces ``import_completion_strategy`` to its
        enum, checks the ranges, and normalises ``custom_emoji_map`` keys
        (see :func:`_normalize_emoji_map`).

        Raises:
            ConfigError: If a value has the wrong type or is out of range.
        """
        _check_field_types(self)
        self.import_completion_strategy = _parse_completion_strategy(
            self.import_completion_strategy
        )
        if self.max_parallel_channels < 1:
            raise ConfigError(
                f"max_parallel_channels must be >= 1, got {self.max_parallel_channels}"
            )
        if self.file_workers < 1:
            raise ConfigError(f"file_workers must be >= 1, got {self.file_workers}")
        if self.space_manager not in SPACE_MANAGER_CHOICES:
            raise ConfigError(
                f"space_manager must be one of {', '.join(SPACE_MANAGER_CHOICES)},"
                f" got {self.space_manager!r}"
            )
        if not self.conversation_types:
            raise ConfigError(
                "conversation_types must list at least one of"
                f" {', '.join(CONVERSATION_TYPES)} (leave it out to migrate"
                " every type)"
            )
        unknown_types = set(self.conversation_types) - set(CONVERSATION_TYPES)
        if unknown_types:
            raise ConfigError(
                f"Unknown conversation_types: {', '.join(sorted(unknown_types))}."
                f" Valid values: {', '.join(CONVERSATION_TYPES)}"
            )
        if self.max_retries < 0:
            raise ConfigError(f"max_retries must be >= 0, got {self.max_retries}")
        if not (0 <= self.max_failure_percentage <= 100):
            raise ConfigError(
                f"max_failure_percentage must be between 0 and 100,"
                f" got {self.max_failure_percentage}"
            )
        if self.retry_delay < 0:
            raise ConfigError(f"retry_delay must be >= 0, got {self.retry_delay}")
        self.custom_emoji_map = _normalize_emoji_map(self.custom_emoji_map)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> MigrationConfig:
        """Create a MigrationConfig from a raw config dictionary.

        Every value is type-checked.  List options also accept a single
        string (treated as a one-item list, with a warning) or an empty
        value; channel names are coerced to strings and a leading ``#`` is
        stripped.  Booleans and integers must be real YAML booleans and
        integers.  Unknown keys that look like a typo of a known option are
        rejected; other unknown keys are reported with a warning.

        Args:
            data: Dictionary loaded from the YAML config file.

        Returns:
            A fully populated MigrationConfig with defaults for missing keys.

        Raises:
            ConfigError: If a value has the wrong type, a key looks like a
                misspelled option, or a value fails validation.
        """
        if not isinstance(data, Mapping):
            raise ConfigError(
                "The config file must contain a mapping of option: value pairs"
                f" at the top level, got {_describe(data)}"
            )
        _check_unknown_keys(data, cls)

        # ``space_mapping`` lives on MigrationState, but validate it here so a
        # malformed value fails before any API call.
        parse_space_mapping(data.get("space_mapping"))

        try:
            return cls(
                exclude_channels=_get_list(data, "exclude_channels", channels=True),
                include_channels=_get_list(data, "include_channels", channels=True),
                user_mapping_overrides=_get_mapping(data, "user_mapping_overrides"),
                email_domain_override=_get_str(data, "email_domain_override", ""),
                ignore_bots=_get_bool(data, "ignore_bots", False),
                abort_on_error=_get_bool(data, "abort_on_error", False),
                max_failure_percentage=_get_int(data, "max_failure_percentage", 10),
                import_completion_strategy=_parse_completion_strategy(
                    _get_str(data, "import_completion_strategy", "skip_on_error")
                ),
                cleanup_on_error=_get_bool(data, "cleanup_on_error", False),
                max_retries=_get_int(data, "max_retries", 3),
                retry_delay=_get_number(data, "retry_delay", 2),
                shared_drive=SharedDriveConfig.from_dict(data.get("shared_drive")),
                workspace_domains=_get_list(data, "workspace_domains"),
                slack_token=_get_str(data, "slack_token", "")
                or os.environ.get("SLACK_TOKEN", ""),
                state_file=_get_str(data, "state_file", ""),
                conversation_types=_get_conversation_types(data),
                max_parallel_channels=_get_int(data, "max_parallel_channels", 1),
                file_workers=_get_int(data, "file_workers", 4),
                custom_emoji_map=_get_mapping(data, "custom_emoji_map"),
                use_chat_custom_emoji=_get_bool(data, "use_chat_custom_emoji", False),
                quote_shared_messages=_get_bool(data, "quote_shared_messages", False),
                migrate_pins=_get_bool(data, "migrate_pins", True),
                convert_canvases=_get_bool(data, "convert_canvases", True),
                restrict_private_channel_folders=_get_bool(
                    data, "restrict_private_channel_folders", True
                ),
                space_manager=_get_str(data, "space_manager", "admin"),
                public_channel_audience=_get_str(data, "public_channel_audience", ""),
                announcement_channels=_get_list(
                    data, "announcement_channels", channels=True
                ),
            )
        except ConfigError:
            raise
        except ValueError as e:
            raise ConfigError(str(e)) from e


# ---------------------------------------------------------------------------
# Typed accessors used by ``from_dict``
# ---------------------------------------------------------------------------

#: Top-level keys accepted in config.yaml that are not MigrationConfig fields.
_EXTRA_KEYS = frozenset({"space_mapping"})

#: An unknown key at least this similar to a known option is treated as a typo.
_TYPO_SIMILARITY = 0.8


def _describe(value: Any) -> str:
    """Describe a raw YAML value for an error message."""
    if value is None:
        return "an empty value"
    return f"{type(value).__name__} {value!r}"


def _check_unknown_keys(data: Mapping[str, Any], config_cls: type) -> None:
    """Reject misspelled options and warn about other unknown keys.

    Args:
        data: The raw top-level config mapping.
        config_cls: The dataclass whose fields are the known options.

    Raises:
        ConfigError: If an unknown key closely matches a known option.
    """
    known = {f.name for f in fields(config_cls)} | _EXTRA_KEYS
    unknown = sorted(str(k) for k in data if str(k) not in known)
    if not unknown:
        return

    typos: list[str] = []
    others: list[str] = []
    for key in unknown:
        match = difflib.get_close_matches(key, known, n=1, cutoff=0.6)
        if not match:
            others.append(key)
        elif difflib.SequenceMatcher(None, key, match[0]).ratio() >= _TYPO_SIMILARITY:
            typos.append(f"'{key}' (did you mean '{match[0]}'?)")
        else:
            others.append(f"{key} (did you mean '{match[0]}'?)")

    if typos:
        raise ConfigError("Unknown config option(s): " + ", ".join(typos))
    log_with_context(
        logging.WARNING,
        "Ignoring unknown config option(s): " + ", ".join(others),
    )


def _get_bool(data: Mapping[str, Any], key: str, default: bool) -> bool:
    """Return a boolean option, rejecting strings and numbers."""
    if key not in data:
        return default
    value = data[key]
    if isinstance(value, bool):
        return value
    raise ConfigError(
        f"Config option '{key}' must be true or false, got {_describe(value)}"
        + (" (remove the quotes)" if isinstance(value, str) else "")
    )


def _get_int(data: Mapping[str, Any], key: str, default: int) -> int:
    """Return an integer option, rejecting strings, floats and empty values."""
    if key not in data:
        return default
    value = data[key]
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    raise ConfigError(
        f"Config option '{key}' must be a whole number, got {_describe(value)}"
    )


def _get_number(data: Mapping[str, Any], key: str, default: float) -> float:
    """Return a numeric (int or float) option, rejecting strings and empty values."""
    if key not in data:
        return default
    value = data[key]
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    raise ConfigError(f"Config option '{key}' must be a number, got {_describe(value)}")


def _get_str(
    data: Mapping[str, Any], key: str, default: str, *, prefix: str = ""
) -> str:
    """Return a string option; an empty value means the default."""
    value = data.get(key)
    if value is None:
        return default
    if isinstance(value, str):
        return value
    raise ConfigError(
        f"Config option '{prefix}{key}' must be a string, got {_describe(value)}"
    )


def _get_list(
    data: Mapping[str, Any], key: str, *, channels: bool = False
) -> list[str]:
    """Return a list-of-strings option.

    A single string becomes a one-item list (with a warning) and an empty
    value becomes an empty list.  Scalar items are coerced with ``str()`` so
    numeric channel names such as ``2024`` work.

    Args:
        data: The raw config mapping.
        key: The option name.
        channels: If True, items are channel names: surrounding whitespace
            and a leading ``#`` are stripped.

    Returns:
        The list of strings.

    Raises:
        ConfigError: If the value is not a list, a string or empty, or an
            item is a nested list or mapping.
    """
    value = data.get(key)
    if value is None:
        return []
    if isinstance(value, str):
        log_with_context(
            logging.WARNING,
            f"Config option '{key}' should be a list; treating {value!r} as"
            f" [{value!r}]",
        )
        value = [value]
    if not isinstance(value, list):
        raise ConfigError(
            f"Config option '{key}' must be a list, got {_describe(value)}"
        )

    items: list[str] = []
    for item in value:
        if item is None:
            continue
        if isinstance(item, (list, dict)):
            raise ConfigError(
                f"Config option '{key}' must be a list of names, got an item"
                f" {_describe(item)}"
            )
        text = normalize_channel_name(str(item)) if channels else str(item).strip()
        if text:
            items.append(text)
    return items


def _get_mapping(data: Mapping[str, Any], key: str) -> dict[str, str]:
    """Return a string-to-string mapping option with keys coerced to ``str``."""
    value = data.get(key)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(
            f"Config option '{key}' must be a mapping, got {_describe(value)}"
        )
    result: dict[str, str] = {}
    for raw_key, raw_value in value.items():
        if not isinstance(raw_value, (str, int, float)) or isinstance(raw_value, bool):
            raise ConfigError(
                f"Config option '{key}' entry {raw_key!r} must be a string,"
                f" got {_describe(raw_value)}"
            )
        result[_mapping_key(raw_key, f"Config option '{key}'")] = str(raw_value)
    return result


#: What each annotated field type must hold, for direct construction.
_FIELD_TYPE_CHECKS: dict[str, tuple[str, Any]] = {
    "bool": ("true or false", lambda v: isinstance(v, bool)),
    "int": (
        "a whole number",
        lambda v: isinstance(v, int) and not isinstance(v, bool),
    ),
    "float": (
        "a number",
        lambda v: isinstance(v, (int, float)) and not isinstance(v, bool),
    ),
    "str": ("a string", lambda v: isinstance(v, str)),
    "list[str]": (
        "a list of strings",
        lambda v: isinstance(v, list) and all(isinstance(i, str) for i in v),
    ),
    "dict[str, str]": (
        "a mapping of strings",
        lambda v: (
            isinstance(v, Mapping) and all(isinstance(i, str) for i in v.values())
        ),
    ),
    "SharedDriveConfig": (
        "a SharedDriveConfig",
        lambda v: isinstance(v, SharedDriveConfig),
    ),
}


def _check_field_types(config: Any) -> None:
    """Check each dataclass field of *config* against its annotation.

    Fields whose annotation isn't in :data:`_FIELD_TYPE_CHECKS` (such as the
    completion-strategy enum, which is coerced instead) are skipped.

    Raises:
        ConfigError: If a field holds the wrong type.
    """
    for f in fields(config):
        check = _FIELD_TYPE_CHECKS.get(str(f.type))
        value = getattr(config, f.name)
        if check is not None and not check[1](value):
            raise ConfigError(f"{f.name} must be {check[0]}, got {_describe(value)}")


def _mapping_key(raw_key: Any, option: str) -> str:
    """Return a mapping key from the config as a string.

    YAML turns some unquoted keys into other types: ``yes``/``no``/``on``/
    ``off`` into booleans and ``2024-01-01`` into a date.  Their text can't
    be recovered, so they are rejected with a hint to quote them.

    Args:
        raw_key: The key as YAML parsed it.
        option: The option name, for the message.

    Returns:
        The key: strings as they are, integers as their digits.

    Raises:
        ConfigError: For any other key type.
    """
    if isinstance(raw_key, str):
        return raw_key
    if isinstance(raw_key, bool):
        raise ConfigError(
            f"{option} has a key that YAML read as the boolean {raw_key}"
            " (written as yes, no, on, off, true or false); quote it, for"
            " example 'yes': ..."
        )
    if isinstance(raw_key, int):
        return str(raw_key)
    if isinstance(raw_key, (datetime.date, datetime.datetime)):
        raise ConfigError(
            f"{option} has a key that YAML read as the date"
            f" {raw_key.isoformat()}; quote it, for example"
            f" '{raw_key.isoformat()}': ..."
        )
    raise ConfigError(
        f"{option} key {raw_key!r} isn't a name; quote it, for example '{raw_key}': ..."
    )


def _get_conversation_types(data: Mapping[str, Any]) -> list[str]:
    """Return ``conversation_types``.

    An absent or null value (``conversation_types:`` with nothing after it)
    means all types.  ``[]`` or ``""`` is passed on as an empty list, so
    ``__post_init__`` rejects it rather than reading it as "all".
    """
    if data.get("conversation_types") is None:
        return list(CONVERSATION_TYPES)
    return _get_list(data, "conversation_types")


def _normalize_emoji_map(mapping: Mapping[Any, str]) -> dict[str, str]:
    """Normalise ``custom_emoji_map`` keys to bare, lowercase emoji names.

    Emoji are looked up by the bare lowercase name Slack uses, so
    ``":PartyParrot:"`` becomes ``"partyparrot"``; otherwise such a key
    would silently never match.

    Raises:
        ConfigError: If a key isn't a name (a YAML boolean or date), is
            empty after stripping, or two keys name the same emoji (also
            when they differ only in case or colons).
    """
    result: dict[str, str] = {}
    for raw_key, value in mapping.items():
        key = _mapping_key(raw_key, "custom_emoji_map")
        name = key.strip().strip(":").strip().lower()
        if not name:
            raise ConfigError(f"custom_emoji_map key {raw_key!r} is not an emoji name")
        if name in result:
            raise ConfigError(
                f"custom_emoji_map lists the emoji {name!r} more than once"
            )
        result[name] = value
    return result


def normalize_channel_name(name: str) -> str:
    """Normalize a channel name from config: trim whitespace and a leading ``#``.

    Args:
        name: Channel name as written by the user (e.g. ``"#general"``).

    Returns:
        The bare channel name (e.g. ``"general"``).
    """
    return name.strip().lstrip("#").strip()


def _parse_completion_strategy(
    value: str | ImportCompletionStrategy,
) -> ImportCompletionStrategy:
    """Convert a raw string to an ImportCompletionStrategy enum member.

    Args:
        value: The string from YAML config.

    Returns:
        The matching enum member.

    Raises:
        ConfigError: If the string doesn't match a known strategy.
    """
    try:
        return ImportCompletionStrategy(value)
    except ValueError:
        valid = ", ".join(s.value for s in ImportCompletionStrategy)
        raise ConfigError(
            f"Invalid import_completion_strategy: {value!r}. Must be one of: {valid}"
        ) from None


def load_config(config_path: Path, *, required: bool = False) -> MigrationConfig:
    """Load and validate the configuration from a YAML file.

    A missing file falls back to the default settings, with a warning, only
    when *required* is False (the user didn't name a config file).  An
    unreadable or invalid file is always an error, so a typo can't silently
    turn into "migrate everything with no user overrides".

    Args:
        config_path: Path to the config YAML file.
        required: True when the user passed ``--config`` explicitly; a
            missing file is then an error.

    Returns:
        MigrationConfig with all necessary defaults applied.

    Raises:
        ConfigError: If the file is required but missing, can't be read,
            isn't valid YAML, or contains invalid options.
    """
    if not config_path.exists():
        if required:
            raise ConfigError(
                f"Config file not found: {config_path}. Check the --config path,"
                " or create one with 'slack-chat-migrator init'."
            )
        log_with_context(
            logging.WARNING,
            f"Config file {config_path} not found, using default settings"
            " (all channels, no user mapping overrides).",
        )
        return MigrationConfig.from_dict({})

    loaded = _read_yaml(config_path)
    try:
        config = MigrationConfig.from_dict({} if loaded is None else loaded)
    except ConfigError as e:
        raise ConfigError(f"Invalid config file {config_path}: {e}") from e

    log_with_context(logging.INFO, f"Loaded configuration from {config_path}")
    return config


class _UniqueKeyLoader(yaml.SafeLoader):
    """A ``SafeLoader`` that rejects a key repeated in the same mapping.

    PyYAML keeps the last of two identical keys without a word, so pasting
    the duplicate-space block the tool logs (one ``"general": ...`` line per
    candidate space) or writing an option twice would silently pick one.
    Keys pulled in with a ``<<`` merge may still be overridden.
    """

    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False
    ) -> dict[Any, Any]:
        """Construct a mapping, raising on a repeated key.

        Raises:
            yaml.constructor.ConstructorError: If a key appears twice.
        """
        first_line: dict[Any, int] = {}
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            key = self.construct_object(key_node, deep=deep)
            try:
                seen = key in first_line
            except TypeError:  # unhashable; the base class reports it
                continue
            if seen:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r} (first on line"
                    f" {first_line[key]}); remove one of the entries",
                    key_node.start_mark,
                )
            first_line[key] = key_node.start_mark.line + 1
        return super().construct_mapping(node, deep=deep)


def _read_yaml(config_path: Path) -> Any:
    """Read and parse a YAML config file, rejecting duplicate keys.

    Raises:
        ConfigError: If the file can't be read or isn't valid YAML, which
            includes a key repeated in the same mapping.
    """
    try:
        with open(config_path, encoding="utf-8") as f:
            # A SafeLoader subclass: it constructs only plain YAML types.
            return yaml.load(f, Loader=_UniqueKeyLoader)  # noqa: S506  # nosec B506
    except yaml.YAMLError as e:
        raise ConfigError(f"Config file {config_path} is not valid YAML: {e}") from e
    except (OSError, UnicodeDecodeError) as e:
        raise ConfigError(f"Could not read config file {config_path}: {e}") from e


#: Prefix of Chat space resource names; ``space_mapping`` values are bare IDs.
_SPACE_NAME_PREFIX = "spaces/"


def parse_space_mapping(value: Any) -> dict[str, str]:
    """Validate a raw ``space_mapping`` value (channel name to space ID).

    Channel names are coerced to strings (so ``123:`` matches channel
    ``"123"``) and lose a leading ``#``.  Space IDs are the bare ID shown in
    the duplicate-space report (``AAAAAgcE123``); a ``spaces/`` prefix is
    removed.

    Args:
        value: The ``space_mapping`` value from the config file.

    Returns:
        A dict mapping channel names to space IDs (empty for an empty value).

    Raises:
        ConfigError: If *value* isn't a mapping, a channel name is empty or
            not a name, a space ID is missing or malformed, or a channel is
            listed twice.
    """
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(
            "Config option 'space_mapping' must be a mapping of channel name:"
            f" space ID, got {_describe(value)}"
        )
    result: dict[str, str] = {}
    for raw_channel, raw_space in value.items():
        channel = normalize_channel_name(_mapping_key(raw_channel, "space_mapping"))
        if not channel:
            raise ConfigError(
                f"space_mapping key {raw_channel!r} is not a channel name"
            )
        if not isinstance(raw_space, str) or not raw_space.strip():
            raise ConfigError(
                f"space_mapping entry {channel!r} must be a space ID such as"
                f" 'AAAAAgcE123', got {_describe(raw_space)}"
            )
        space_id = raw_space.strip()
        if space_id.startswith(_SPACE_NAME_PREFIX):
            space_id = space_id[len(_SPACE_NAME_PREFIX) :]
        if not space_id or "/" in space_id or any(c.isspace() for c in space_id):
            raise ConfigError(
                f"space_mapping entry {channel!r} has an invalid space ID"
                f" {raw_space!r}; use the ID shown in the duplicate-space"
                " report, such as 'AAAAAgcE123'"
            )
        if channel in result:
            raise ConfigError(f"space_mapping lists channel {channel!r} more than once")
        result[channel] = space_id
    return result


def load_space_mapping(config_path: Path) -> dict[str, str]:
    """Load space_mapping overrides from a YAML config file.

    ``space_mapping`` is runtime state used to resolve duplicate-space
    conflicts during migration.  It is stored on
    :class:`~slack_chat_migrator.core.state.MigrationState`, not on
    :class:`MigrationConfig`, but is still authored by users in their
    config YAML.  See :func:`parse_space_mapping` for the accepted shape.

    Args:
        config_path: Path to the config YAML file.

    Returns:
        A dict mapping channel names to space IDs; empty when the file
        doesn't exist or has no ``space_mapping``.

    Raises:
        ConfigError: If the file can't be read, isn't valid YAML (duplicate
            keys included), its top level isn't a mapping, or its
            ``space_mapping`` is malformed.
    """
    if not config_path.exists():
        return {}
    raw = _read_yaml(config_path)
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ConfigError(
            f"Invalid config file {config_path}: the top level must be a"
            f" mapping of option: value pairs, got {_describe(raw)}"
        )
    try:
        return parse_space_mapping(raw.get("space_mapping"))
    except ConfigError as e:
        raise ConfigError(f"Invalid config file {config_path}: {e}") from e


def should_process_channel(channel_name: str, config: MigrationConfig) -> bool:
    """
    Determine if a Slack channel should be processed based on configuration filters.

    This function applies inclusion and exclusion rules from the configuration:
    1. If an include_channels list is specified, only those channels are processed
    2. If no include_channels list is specified, all channels are processed except
       those in the exclude_channels list

    Args:
        channel_name: The name of the Slack channel
        config: The MigrationConfig instance

    Returns:
        True if the channel should be processed, False if it should be skipped
    """
    # Check include list (if specified, only those channels are processed)
    # Normalize: strip '#' prefix that users may accidentally include
    include_channels = {normalize_channel_name(ch) for ch in config.include_channels}
    if include_channels:
        should_process = channel_name in include_channels
        log_with_context(
            logging.DEBUG,
            f"Channel '{channel_name}' {'in' if should_process else 'not in'} include list",
            channel=channel_name,
        )
        return should_process

    # Check exclude list
    exclude_channels = {normalize_channel_name(ch) for ch in config.exclude_channels}
    if channel_name in exclude_channels:
        log_with_context(
            logging.DEBUG,
            f"Channel '{channel_name}' is in exclude list, skipping",
            channel=channel_name,
        )
        return False

    return True
