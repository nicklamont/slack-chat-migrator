"""Unit tests for the config module."""

import os
import tempfile
from pathlib import Path

import pytest
import yaml

from slack_migrator.core.config import load_config, should_process_channel


def test_load_config_with_empty_file():
    """Test loading config from an empty file."""
    with tempfile.NamedTemporaryFile(suffix=".yaml") as temp_file:
        config = load_config(Path(temp_file.name))
        
        # Check default values
        assert "exclude_channels" in config
        assert "include_channels" in config
        assert "user_mapping_overrides" in config
        assert config["attachments_folder"] == "Slack Attachments"
        assert config["email_domain_override"] == ""


def test_load_config_with_values():
    """Test loading config with specific values."""
    with tempfile.NamedTemporaryFile(suffix=".yaml") as temp_file:
        # Write test config
        config_data = {
            "attachments_folder": "Custom Folder",
            "email_domain_override": "example.com",
            "exclude_channels": ["random", "general"],
            "include_channels": ["important"],
            "user_mapping_overrides": {"U123": "user@example.com"}
        }
        
        with open(temp_file.name, "w") as f:
            yaml.dump(config_data, f)
        
        # Load the config
        config = load_config(Path(temp_file.name))
        
        # Check values
        assert config["attachments_folder"] == "Custom Folder"
        assert config["email_domain_override"] == "example.com"
        assert "random" in config["exclude_channels"]
        assert "general" in config["exclude_channels"]
        assert "important" in config["include_channels"]
        assert config["user_mapping_overrides"]["U123"] == "user@example.com"


def test_should_process_channel():
    """Test channel processing logic."""
    # Test with include list
    config = {"include_channels": ["channel1", "channel2"]}
    assert should_process_channel("channel1", config) is True
    assert should_process_channel("channel3", config) is False
    
    # Test with exclude list
    config = {"exclude_channels": ["channel1", "channel2"]}
    assert should_process_channel("channel1", config) is False
    assert should_process_channel("channel3", config) is True
    
    # Test with both include and exclude (include takes precedence)
    config = {
        "include_channels": ["channel1", "channel2"],
        "exclude_channels": ["channel1", "channel3"]
    }
    assert should_process_channel("channel1", config) is True  # In include list
    assert should_process_channel("channel2", config) is True  # In include list
    assert should_process_channel("channel3", config) is False  # Not in include list
    assert should_process_channel("channel4", config) is False  # Not in include list 