# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| 0.3.x   | :white_check_mark: |
| < 0.3   | :x:                |

## Reporting a Vulnerability

If you discover a security vulnerability in this project, please report it
responsibly:

1. **Do not** open a public issue.
2. Email **nick@nicklamont.com** with:
   - A description of the vulnerability
   - Steps to reproduce
   - Potential impact
3. You will receive an acknowledgement within 72 hours.
4. A fix will be developed privately and released as a patch version.

## Scope

This tool handles Google Workspace service account credentials and processes
Slack export archives that may contain authentication tokens in file URLs.
Vulnerabilities related to credential handling, token exposure, or data leakage
are especially relevant.
