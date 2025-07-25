"""
Configuration module for the Slack to Google Chat migration tool
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional

from slack_migrator.utils.logging import logger


def load_config(config_path: Path) -> Dict[str, Any]:
    """Load configuration from YAML file.
    
    Args:
        config_path: Path to the config YAML file
        
    Returns:
        Dictionary of configuration settings
    """
    config = {}
    
    if config_path.exists():
        try:
            with open(config_path) as f:
                loaded_config = yaml.safe_load(f)
                # Handle None result from empty file
                if loaded_config is not None:
                    config = loaded_config
            logger.info(f"Loaded configuration from {config_path}")
        except Exception as e:
            logger.warning(f"Failed to load config file {config_path}: {e}")
    else:
        logger.warning(f"Config file {config_path} not found, using default settings")
    
    # Ensure expected dictionaries exist
    config.setdefault('exclude_channels', [])
    config.setdefault('include_channels', [])
    config.setdefault('user_mapping_overrides', {})
    
    # Set default values
    config.setdefault('attachments_folder', 'Slack Attachments')
    config.setdefault('email_domain_override', '')
    
    return config


def create_default_config(output_path: Path) -> bool:
    """Create a default configuration file.
    
    Args:
        output_path: Path where the default config should be saved
        
    Returns:
        True if successful, False otherwise
    """
    if output_path.exists():
        logger.warning(f"Config file {output_path} already exists, not overwriting")
        return False
        
    default_config = {
        "attachments_folder": "Slack Attachments",
        "exclude_channels": [
            "random",
            "shitposting"
        ],
        "include_channels": [],
        "email_domain_override": "",
        "user_mapping_overrides": {
            "UEXAMPLE1": "user1@example.com",
            "UEXAMPLE2": "user2@example.com",
            "UEXAMPLE3": "work@company.com"  # Example of mapping an external email
        }
    }
    
    try:
        with open(output_path, 'w') as f:
            yaml.safe_dump(default_config, f, default_flow_style=False)
        logger.info(f"Created default config file at {output_path}")
        return True
    except Exception as e:
        logger.error(f"Failed to create default config file: {e}")
        return False


def should_process_channel(channel_name: str, config: Dict[str, Any]) -> bool:
    """Determine if a channel should be processed based on config.
    
    Args:
        channel_name: The name of the Slack channel
        config: The configuration dictionary
        
    Returns:
        True if the channel should be processed, False otherwise
    """
    logger.debug(f"CHANNEL CHECK: Checking if channel '{channel_name}' should be processed")
    logger.debug(f"CHANNEL CHECK: include_channels={config.get('include_channels', [])}, exclude_channels={config.get('exclude_channels', [])}")
    
    # Check include list (if specified, only these channels are processed)
    include_channels = set(config.get('include_channels', []))
    if include_channels:
        if channel_name in include_channels:
            logger.debug(f"CHANNEL CHECK: Channel '{channel_name}' is in include list, will process")
            return True
        else:
            logger.debug(f"CHANNEL CHECK: Channel '{channel_name}' not in include list, skipping")
            return False
    
    # Check exclude list
    exclude_channels = set(config.get('exclude_channels', []))
    if channel_name in exclude_channels:
        logger.debug(f"CHANNEL CHECK: Channel '{channel_name}' is in exclude list, skipping")
        return False
    
    logger.debug(f"CHANNEL CHECK: Channel '{channel_name}' not in any list, will process")
    return True 