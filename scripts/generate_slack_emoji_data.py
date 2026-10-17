#!/usr/bin/env python3
"""Generate ``src/slack_chat_migrator/utils/slack_emoji_data.py``.

Slack names emoji with its own short names (``man-shrugging``, ``face_palm``,
``flag-england``), taken from iamcal/emoji-data, the MIT-licensed table Slack
uses.  The ``emoji`` package resolves most of them through its ``alias``
language, but not all, and a few resolve to a different emoji than Slack's.

This script keeps only the names the installed ``emoji`` package cannot
resolve, or resolves differently, and writes them as a small Python dict.
Two-letter ``flag-xx`` country flags are left out, because
``slack_chat_migrator.utils.emoji`` derives them from the ISO code.

Usage (from the repository root, with the package's dependencies installed)::

    python scripts/generate_slack_emoji_data.py [path/to/emoji.json]

Without an argument the script downloads ``emoji.json`` from GitHub.  Run
``ruff format`` on the output afterwards.
"""

from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path
from typing import Any

import emoji

EMOJI_DATA_URL = "https://raw.githubusercontent.com/iamcal/emoji-data/master/emoji.json"
OUTPUT_PATH = (
    Path(__file__).resolve().parent.parent
    / "src"
    / "slack_chat_migrator"
    / "utils"
    / "slack_emoji_data.py"
)

#: Slack-only names that emoji-data doesn't list (Slack draws them itself),
#: mapped to the closest Unicode emoji, as code points.
EXTRA_NAMES = {
    "simple_smile": "1F642",
}

LICENSE_TEXT = """\
The MIT License (MIT)

Copyright (c) 2013 Cal Henderson

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE."""


def _chars(codepoints: str) -> str:
    """Convert ``"1F937-200D-2642-FE0F"`` into the emoji string."""
    return "".join(chr(int(part, 16)) for part in codepoints.split("-"))


def _regional_flag(code: str) -> str:
    """Return the regional-indicator flag for a two-letter ISO code."""
    return "".join(chr(0x1F1E6 + ord(c) - ord("a")) for c in code)


def _same(a: str, b: str) -> bool:
    """Compare two emoji strings, ignoring variation selector 16."""
    return a.replace("\ufe0f", "") == b.replace("\ufe0f", "")


def _package_resolves(name: str) -> str | None:
    """Return what the ``emoji`` package's alias table makes of ``:name:``."""
    out = emoji.emojize(f":{name}:", language="alias")
    if out == f":{name}:" or not emoji.is_emoji(out):
        return None
    return str(out)


def _escape(value: str) -> str:
    """Render *value* as a Python string literal body with escapes."""
    parts = []
    for ch in value:
        code = ord(ch)
        if 0x20 <= code < 0x7F and ch not in '\\"':
            parts.append(ch)
        elif code <= 0xFFFF:
            parts.append(f"\\u{code:04x}")
        else:
            parts.append(f"\\U{code:08x}")
    return "".join(parts)


def build_table(entries: list[dict[str, Any]]) -> dict[str, str]:
    """Return the Slack names the ``emoji`` package can't resolve correctly."""
    table: dict[str, str] = {}
    for entry in entries:
        target = _chars(entry["unified"])
        for name in entry.get("short_names", []):
            if (
                name.startswith("flag-")
                and len(name) == 7
                and _same(_regional_flag(name[5:]), target)
            ):
                continue
            resolved = _package_resolves(name)
            if resolved is None or not _same(resolved, target):
                table[name] = target
    for name, codepoints in EXTRA_NAMES.items():
        if _package_resolves(name) is None:
            table[name] = _chars(codepoints)
    return dict(sorted(table.items()))


def render_module(table: dict[str, str]) -> str:
    """Render the generated module source."""
    license_block = "\n".join(
        f"    {line}" if line else "" for line in LICENSE_TEXT.splitlines()
    )
    lines = [
        '"""Slack emoji short names that the ``emoji`` package does not resolve.',
        "",
        "GENERATED FILE: do not edit by hand.  Regenerate it with",
        "``python scripts/generate_slack_emoji_data.py``.",
        "",
        "Source: iamcal/emoji-data (https://github.com/iamcal/emoji-data), the",
        "emoji table Slack uses, plus a few Slack-only names listed in the",
        "script.  Only names the ``emoji`` package (``language='alias'``) can't",
        "resolve, or resolves to a different emoji than Slack, are kept.",
        "Two-letter ``flag-xx`` country flags are derived at runtime.",
        "",
        "emoji-data is distributed under the MIT license:",
        "",
        license_block,
        '"""',
        "",
        "from __future__ import annotations",
        "",
        f"#: Slack short name -> Unicode emoji ({len(table)} entries).",
        "SLACK_EMOJI: dict[str, str] = {",
    ]
    lines.extend(f'    "{name}": "{_escape(value)}",' for name, value in table.items())
    lines.append("}")
    return "\n".join(lines) + "\n"


def main(argv: list[str]) -> int:
    """Generate the module from a local or downloaded ``emoji.json``."""
    if len(argv) > 1:
        raw = Path(argv[1]).read_text(encoding="utf-8")
    else:
        with urllib.request.urlopen(EMOJI_DATA_URL, timeout=60) as resp:
            raw = resp.read().decode("utf-8")
    table = build_table(json.loads(raw))
    OUTPUT_PATH.write_text(render_module(table), encoding="utf-8")
    print(f"Wrote {len(table)} names to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
