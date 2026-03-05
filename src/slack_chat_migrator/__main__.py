#!/usr/bin/env python3
"""Main execution module for the Slack to Google Chat migration tool."""

import warnings

# Suppress deprecation warnings from third-party libraries about
# Python version support — these are not actionable by users.
warnings.filterwarnings("ignore", category=FutureWarning, module=r"google\.")
warnings.filterwarnings("ignore", category=DeprecationWarning, module=r"google\.")
warnings.filterwarnings("ignore", message=r"urllib3 v2 only supports OpenSSL")

from slack_chat_migrator.cli.commands import main  # noqa: E402

if __name__ == "__main__":
    main()
