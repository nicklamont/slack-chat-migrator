"""
Configuration module for the Slack to Google Chat migration tool.

This module provides functions for loading and manipulating configuration
settings from YAML files, creating default configurations, and determining
which Slack channels should be processed based on the configuration.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from slack_migrator.utils.logging import log_with_context


@dataclass
class SharedDriveConfig:
    """Configuration for the shared Google Drive used for file attachments."""

    name: str = "Imported Slack Attachments"
    id: str | None = None

    @classmethod
    def from_dict(cls, data: dict[str, Any] | None) -> SharedDriveConfig:
        if not data:
            return cls()
        return cls(
            name=data.get("name", cls.name),
            id=data.get("id"),
        )


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
    import_completion_strategy: str = "skip_on_error"
    cleanup_on_error: bool = False

    # Retry
    max_retries: int = 3
    retry_delay: int = 2

    # Shared drive
    shared_drive: SharedDriveConfig = field(default_factory=SharedDriveConfig)

    # Runtime state (set during migration, not from YAML)
    space_mapping: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MigrationConfig:
        """Create a MigrationConfig from a raw config dictionary."""
        shared_drive = SharedDriveConfig.from_dict(data.get("shared_drive"))
        return cls(
            exclude_channels=data.get("exclude_channels", []),
            include_channels=data.get("include_channels", []),
            user_mapping_overrides=data.get("user_mapping_overrides") or {},
            email_domain_override=data.get("email_domain_override", ""),
            ignore_bots=data.get("ignore_bots", False),
            abort_on_error=data.get("abort_on_error", False),
            max_failure_percentage=data.get("max_failure_percentage", 10),
            import_completion_strategy=data.get(
                "import_completion_strategy", "skip_on_error"
            ),
            cleanup_on_error=data.get("cleanup_on_error", False),
            max_retries=data.get("max_retries", 3),
            retry_delay=data.get("retry_delay", 2),
            shared_drive=shared_drive,
            space_mapping=data.get("space_mapping") or {},
        )


def load_config(config_path: Path) -> MigrationConfig:
    """
    Load configuration from YAML file and apply default values.

    Loads the configuration from the specified YAML file and applies default
    values for any missing configuration options. If the file doesn't exist
    or is invalid, appropriate warnings are logged and default settings are used.

    Args:
        config_path: Path to the config YAML file

    Returns:
        MigrationConfig with all necessary defaults applied
    """
    raw: dict[str, Any] = {}

    if config_path.exists():
        try:
            with open(config_path) as f:
                loaded_config = yaml.safe_load(f)
                # Handle None result from empty file
                if loaded_config is not None:
                    raw = loaded_config
            log_with_context(logging.INFO, f"Loaded configuration from {config_path}")
        except (yaml.YAMLError, OSError) as e:
            log_with_context(
                logging.WARNING, f"Failed to load config file {config_path}: {e}"
            )
    else:
        log_with_context(
            logging.WARNING,
            f"Config file {config_path} not found, using default settings",
        )

    return MigrationConfig.from_dict(raw)


def create_default_config(output_path: Path) -> bool:
    """
    Create a default configuration file with recommended settings.

    This function creates a new configuration file with sensible defaults at
    the specified location. It includes all supported configuration options
    with example values and comments. The function will not overwrite an
    existing configuration file.

    Args:
        output_path: Path where the default config should be saved

    Returns:
        True if the config file was created successfully, False otherwise
    """
    if output_path.exists():
        log_with_context(
            logging.WARNING,
            f"Config file {output_path} already exists, not overwriting",
        )
        return False

    default_config = {
        # Shared drive configuration replaces the old attachments_folder approach
        "shared_drive": {"name": "Imported Slack Attachments"},
        "exclude_channels": ["random", "shitposting"],
        "include_channels": [],
        "email_domain_override": "",
        "user_mapping_overrides": {
            "UEXAMPLE1": "user1@example.com",
            "UEXAMPLE2": "user2@example.com",
            "UEXAMPLE3": "work@company.com",  # Example of mapping an external email
        },
        # Error handling options
        "abort_on_error": False,
        "max_failure_percentage": 10,
        "import_completion_strategy": "skip_on_error",
        "cleanup_on_error": False,
        # Retry options
        "max_retries": 3,
        "retry_delay": 2,
    }

    try:
        with open(output_path, "w") as f:
            yaml.safe_dump(default_config, f, default_flow_style=False)
        log_with_context(logging.INFO, f"Created default config file at {output_path}")
        return True
    except Exception as e:
        log_with_context(logging.ERROR, f"Failed to create default config file: {e}")
        return False


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
    log_with_context(
        logging.DEBUG,
        f"CHANNEL CHECK: Checking if channel '{channel_name}' should be processed",
        channel=None,
    )
    log_with_context(
        logging.DEBUG,
        f"CHANNEL CHECK: include_channels={config.include_channels}, exclude_channels={config.exclude_channels}",
        channel=channel_name,
    )

    # Check include list (if specified, only these channels are processed)
    include_channels = set(config.include_channels)
    if include_channels:
        if channel_name in include_channels:
            log_with_context(
                logging.DEBUG,
                f"CHANNEL CHECK: Channel '{channel_name}' is in include list, will process",
                channel=None,
            )
            return True
        else:
            log_with_context(
                logging.DEBUG,
                f"CHANNEL CHECK: Channel '{channel_name}' not in include list, skipping",
                channel=None,
            )
            return False

    # Check exclude list
    exclude_channels = set(config.exclude_channels)
    if channel_name in exclude_channels:
        log_with_context(
            logging.DEBUG,
            f"CHANNEL CHECK: Channel '{channel_name}' is in exclude list, skipping",
            channel=None,
        )
        return False

    log_with_context(
        logging.DEBUG,
        f"CHANNEL CHECK: Channel '{channel_name}' not in any list, will process",
        channel=None,
    )
    return True
