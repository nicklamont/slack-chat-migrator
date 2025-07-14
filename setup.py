from setuptools import setup, find_packages
import os
import re

# Read version from __init__.py without importing the package
with open("slack_migrator/__init__.py", "r", encoding="utf-8") as f:
    version_match = re.search(r'__version__\s*=\s*[\'"]([^\'"]*)[\'"]', f.read())
    version = version_match.group(1) if version_match else "0.1.0"

# Read long description from README.md
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

# Read requirements from requirements.txt
with open("requirements.txt", "r", encoding="utf-8") as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith("#") and not line.startswith("# ")]

setup(
    name="slack-chat-migrator",
    version=version,
    packages=find_packages(),
    install_requires=requirements,
    entry_points={
        'console_scripts': [
            'slack-migrator=slack_migrator.__main__:main',
            'slack-migrator-check-permissions=slack_migrator.cli.permission:main',
        ],
    },
    author="Nick Lamont",
    author_email="nick@nicklamont.com",
    description="Tool for migrating Slack exports to Google Chat",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/nicklamont/slack-chat-migrator",
    keywords="slack, google-chat, migration, workspace, chat",
    python_requires=">=3.9",
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
    ],
    include_package_data=True,
    package_data={
        "slack_migrator": ["py.typed"],
    },
    project_urls={
        "Bug Tracker": "https://github.com/nicklamont/slack-chat-migrator/issues",
        "Documentation": "https://github.com/nicklamont/slack-chat-migrator",
        "Source Code": "https://github.com/nicklamont/slack-chat-migrator",
    },
) 