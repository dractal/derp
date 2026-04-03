"""Statement filters for migration generation.

Provides filtering functions that remove specific statement types from
migration change lists based on configuration flags.
"""

from __future__ import annotations

from derp.orm.migrations.statements.types import (
    AlterPolicyStatement,
    CreatePolicyStatement,
    DisableRLSStatement,
    DropPolicyStatement,
    EnableRLSStatement,
    Statement,
)

_RLS_STATEMENT_TYPES = (
    EnableRLSStatement,
    DisableRLSStatement,
    CreatePolicyStatement,
    DropPolicyStatement,
    AlterPolicyStatement,
)


def filter_rls_statements(statements: list[Statement]) -> list[Statement]:
    """Remove RLS and policy statements from a statement list.

    Filters out EnableRLS, DisableRLS, CreatePolicy, DropPolicy,
    and AlterPolicy statements.  Used when ``ignore_rls = true``
    is set in the ``[database]`` section of ``derp.toml``.
    """
    return [s for s in statements if not isinstance(s, _RLS_STATEMENT_TYPES)]
