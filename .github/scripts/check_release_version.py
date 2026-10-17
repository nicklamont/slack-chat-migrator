"""Fail the release when the pushed tag doesn't match the package version.

Usage: python3 .github/scripts/check_release_version.py v1.2.3
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

INIT = (
    Path(__file__).resolve().parents[2] / "src" / "slack_chat_migrator" / "__init__.py"
)


def package_version(init_file: Path = INIT) -> str:
    """Return ``__version__`` from the package's ``__init__.py``."""
    match = re.search(
        r"""^__version__\s*=\s*["']([^"']+)["']""",
        init_file.read_text(encoding="utf-8"),
        re.MULTILINE,
    )
    if not match:
        raise SystemExit(f"No __version__ found in {init_file}")
    return match.group(1)


def main(argv: list[str]) -> int:
    """Compare the tag in *argv* with ``__version__``; return the exit code."""
    if len(argv) != 2:
        print(__doc__, file=sys.stderr)
        return 2
    tag = argv[1]
    version = package_version()
    if tag != f"v{version}":
        print(
            f"::error::Tag {tag} does not match __version__ {version}"
            f" (expected tag v{version})",
            file=sys.stderr,
        )
        return 1
    print(f"Tag {tag} matches __version__ {version}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
