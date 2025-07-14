#!/usr/bin/env python3
"""
Main execution module for the Slack to Google Chat migration tool
"""

import sys
from slack_migrator.cli.commands import main

if __name__ == "__main__":
    sys.exit(main()) 