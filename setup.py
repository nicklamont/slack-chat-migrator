"""
Setup script for the slack-chat-migrator package.

This file manages the package installation process, including dependencies,
entry points, metadata, and other package configuration.
"""

from setuptools import setup, find_packages
import os
import re
from typing import List, Optional

# Read version from __init__.py without importing the package
def get_version() -> str:
    """Extract version from the package __init__.py file."""
    with open("slack_migrator/__init__.py", "r", encoding="utf-8") as f:
        version_match = re.search(r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]', f.read())
        return version_match.group(1) if version_match else "0.1.0"

# Read long description from README.md
def get_long_description() -> str:
    """Read and return the content of README.md file."""
    with open("README.md", "r", encoding="utf-8") as fh:
        return fh.read()

# Read requirements from requirements.txt
def get_requirements() -> List[str]:
    """Parse requirements.txt file and return a list of required packages."""
    with open("requirements.txt", "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]

version = get_version()
long_description = get_long_description()
requirements = get_requirements()

setup(
    # Package metadata
    name="slack-chat-migrator",
    version=version,
    author="Nick Lamont",
    author_email="nick@nicklamont.com",
    description="Tool for migrating Slack exports to Google Chat",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nicklamont/slack-chat-migrator",
    keywords="slack, google-chat, migration, workspace, chat, data-migration",
    
    # Package configuration
    packages=find_packages(),
    install_requires=requirements,
    python_requires=">=3.9",
    include_package_data=True,
    package_data={
        "slack_migrator": ["py.typed"],  # Include type information
    },
    
    # Command line tools
    entry_points={
        'console_scripts': [
            'slack-migrator=slack_migrator.__main__:main',
        ],
    },
    
    # Classifiers for package indexing
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Communications :: Chat",
        "Topic :: Utilities",
    ],
    
    # Additional URLs
    project_urls={
        "Bug Tracker": "https://github.com/nicklamont/slack-chat-migrator/issues",
        "Documentation": "https://github.com/nicklamont/slack-chat-migrator",
        "Source Code": "https://github.com/nicklamont/slack-chat-migrator",
    },
) 