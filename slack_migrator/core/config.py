"""
Configuration module for the Slack to Google Chat migration tool.

This module provides functions for loading and manipulating configuration
settings from YAML files, creating default configurations, and determining
which Slack channels should be processed based on the configuration.
"""

import logging
from pathlib import Path
from typing import Any, Dict

import yaml

from slack_migrator.utils.logging import log_with_context


def load_config(config_path: Path) -> Dict[str, Any]:
    """
    Load configuration from YAML file and apply default values.

    Loads the configuration from the specified YAML file and applies default
    values for any missing configuration options. If the file doesn't exist
    or is invalid, appropriate warnings are logged and default settings are used.

    Args:
        config_path: Path to the config YAML file

    Returns:
        Dictionary of configuration settings with all necessary defaults applied
    """
    config = {}

    if config_path.exists():
        try:
            with open(config_path) as f:
                loaded_config = yaml.safe_load(f)
                # Handle None result from empty file
                if loaded_config is not None:
                    config = loaded_config
            log_with_context(logging.INFO, f"Loaded configuration from {config_path}")
        except Exception as e:
            log_with_context(
                logging.WARNING, f"Failed to load config file {config_path}: {e}"
            )
    else:
        log_with_context(
            logging.WARNING,
            f"Config file {config_path} not found, using default settings",
        )

    # Ensure expected dictionaries exist
    config.setdefault("exclude_channels", [])
    config.setdefault("include_channels", [])
    config.setdefault("user_mapping_overrides", {})

    # Set default values (attachments_folder no longer needed with shared drive approach)
    config.setdefault("email_domain_override", "")

    # Set default values for error handling options
    config.setdefault("abort_on_error", False)
    config.setdefault("max_failure_percentage", 10)
    config.setdefault("import_completion_strategy", "skip_on_error")
    config.setdefault("cleanup_on_error", False)

    # Set default values for retry options
    config.setdefault("max_retries", 3)
    config.setdefault("retry_delay", 2)

    return config


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


def should_process_channel(channel_name: str, config: Dict[str, Any]) -> bool:
    """
    Determine if a Slack channel should be processed based on configuration filters.

    This function applies inclusion and exclusion rules from the configuration:
    1. If an include_channels list is specified, only those channels are processed
    2. If no include_channels list is specified, all channels are processed except
       those in the exclude_channels list

    Args:
        channel_name: The name of the Slack channel
        config: The configuration dictionary containing include_channels and exclude_channels lists

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
        f"CHANNEL CHECK: include_channels={config.get('include_channels', [])}, exclude_channels={config.get('exclude_channels', [])}",
        channel=channel_name,
    )

    # Check include list (if specified, only these channels are processed)
    include_channels = set(config.get("include_channels", []))
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
    exclude_channels = set(config.get("exclude_channels", []))
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
