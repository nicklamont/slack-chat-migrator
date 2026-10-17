"""Who counts as internal: users on the workspace's Google Workspace domains.

The internal domains are the workspace admin's domain plus
``config.workspace_domains``.  :meth:`UserResolver.is_external_user` uses
these helpers and makes the decision for the whole tool; FileHandler and
FolderManager call them directly only when they have no resolver.
"""

from __future__ import annotations

from collections.abc import Iterable


def normalize_domain(domain: object) -> str:
    """Return a domain lowercased, without spaces or a leading ``@``."""
    if not isinstance(domain, str):
        return ""
    return domain.strip().lower().lstrip("@").strip()


def internal_domains(
    admin_domain: str | None, workspace_domains: Iterable[str] = ()
) -> frozenset[str]:
    """The admin's domain plus *workspace_domains*, normalised.

    Args:
        admin_domain: The workspace admin's domain (may be empty).
        workspace_domains: Further internal domains (``config.workspace_domains``).

    Returns:
        The non-empty normalised domains.
    """
    domains = (
        normalize_domain(admin_domain),
        *map(normalize_domain, workspace_domains),
    )
    return frozenset(d for d in domains if d)


def is_external_email(email: str | None, domains: frozenset[str]) -> bool:
    """Whether *email* is outside every internal domain.

    Case and surrounding whitespace don't matter.  With no email, or no
    known internal domain (a dry run without an admin), nobody is external.

    Args:
        email: The address to check.
        domains: Internal domains, from :func:`internal_domains`.

    Returns:
        True if the address is on none of *domains*.
    """
    address = email.strip().lower() if isinstance(email, str) else ""
    if not address or not domains:
        return False
    return address.rsplit("@", 1)[-1] not in domains
