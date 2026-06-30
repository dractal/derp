"""Module loading utilities for Derp ORM."""

from __future__ import annotations

from derp._loader import deduplicate_tables, load_table_subclasses
from derp.orm.table import Table


def load_tables(module_path: str) -> list[type[Table]]:
    """Load Table subclasses from a Python module, directory, or glob pattern.

    Args:
        module_path: One of:
            - Path to a Python file (e.g., "src/myapp/schema.py")
            - Path to a directory (loads all .py files recursively)
            - Glob pattern (e.g., "src/**/models.py", "src/**/*.py")

    Returns:
        List of Table subclasses found in matching modules
    """
    return load_table_subclasses(module_path, Table)


def _deduplicate_tables(tables: list[type[Table]]) -> list[type[Table]]:
    """Keep only the youngest table in each inheritance chain."""
    return deduplicate_tables(tables, Table)


def discover_tables(
    schema_path: str,
    *,
    auth_config: object | None = None,
) -> list[type[Table]]:
    """Load user tables and optionally inject framework tables, then dedup.

    Args:
        schema_path: Path/glob/directory for user schema files.
        auth_config: An ``AuthConfig`` instance (or ``None``).  The
            active backend determines which auth tables are injected:

            - **native**: AuthUser, AuthSession, AuthOrganization, AuthOrgMember
            - **supabase**: AuthOrganization, SupabaseOrgMember (users in Supabase)
            - **workos**: WorkOSOrganization (users + memberships in WorkOS;
              local table maps WorkOS org id ↔ local UUID + slug)
            - **gcip**: AuthOrganization, GCIPOrgMember, AuthInvitation (users in
              GCIP; derp owns the org graph and projects it into token claims)

    Returns:
        Deduplicated list of Table subclasses.
    """
    tables = load_tables(schema_path)

    if auth_config is not None:
        native = getattr(auth_config, "native", None)
        supabase = getattr(auth_config, "supabase", None)
        workos = getattr(auth_config, "workos", None)
        gcip = getattr(auth_config, "gcip", None)

        auth_tables: list[type[Table]] = []

        if native is not None:
            from derp.auth.models import (
                AuthOrganization,
                AuthOrgMember,
                AuthSession,
                AuthUser,
            )

            auth_tables = [
                AuthUser,
                AuthSession,
                AuthOrganization,
                AuthOrgMember,
            ]

        elif supabase is not None:
            from derp.auth.models import AuthOrganization, SupabaseOrgMember

            auth_tables = [AuthOrganization, SupabaseOrgMember]

        elif workos is not None:
            from derp.auth.models import WorkOSOrganization

            auth_tables = [WorkOSOrganization]

        elif gcip is not None:
            from derp.auth.models import AuthInvitation, AuthOrganization, GCIPOrgMember

            auth_tables = [AuthOrganization, GCIPOrgMember, AuthInvitation]

        for auth_table in auth_tables:
            if not any(issubclass(t, auth_table) for t in tables):
                tables.append(auth_table)

    return _deduplicate_tables(tables)
