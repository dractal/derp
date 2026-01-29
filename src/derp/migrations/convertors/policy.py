"""Row-Level Security (RLS) policy operation convertors."""

from __future__ import annotations

from derp.migrations.convertors.base import (
    ConvertorRegistry,
    StatementConvertor,
    quote_identifier,
    quote_schema_table,
)
from derp.migrations.statements.types import (
    AlterPolicyStatement,
    CreatePolicyStatement,
    DisableRLSStatement,
    DropPolicyStatement,
    EnableRLSStatement,
)


class EnableRLSConvertor(StatementConvertor[EnableRLSStatement]):
    """Convert ENABLE ROW LEVEL SECURITY statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "enable_rls"

    def convert(self, statement: EnableRLSStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        sql = f"ALTER TABLE {table_ref} ENABLE ROW LEVEL SECURITY;"

        if statement.force:
            sql += f"\nALTER TABLE {table_ref} FORCE ROW LEVEL SECURITY;"

        return sql


class DisableRLSConvertor(StatementConvertor[DisableRLSStatement]):
    """Convert DISABLE ROW LEVEL SECURITY statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "disable_rls"

    def convert(self, statement: DisableRLSStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        return f"ALTER TABLE {table_ref} DISABLE ROW LEVEL SECURITY;"


class CreatePolicyConvertor(StatementConvertor[CreatePolicyStatement]):
    """Convert CREATE POLICY statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_policy"

    def convert(self, statement: CreatePolicyStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        policy_name = quote_identifier(statement.name)

        parts = [f"CREATE POLICY {policy_name} ON {table_ref}"]

        # AS PERMISSIVE or AS RESTRICTIVE
        if not statement.permissive:
            parts.append("AS RESTRICTIVE")

        # FOR command
        if statement.command and statement.command.upper() != "ALL":
            parts.append(f"FOR {statement.command.upper()}")

        # TO roles
        if statement.roles:
            roles = ", ".join(quote_identifier(r) for r in statement.roles)
            parts.append(f"TO {roles}")

        # USING expression
        if statement.using:
            parts.append(f"USING ({statement.using})")

        # WITH CHECK expression
        if statement.with_check:
            parts.append(f"WITH CHECK ({statement.with_check})")

        return " ".join(parts) + ";"


class DropPolicyConvertor(StatementConvertor[DropPolicyStatement]):
    """Convert DROP POLICY statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_policy"

    def convert(self, statement: DropPolicyStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        policy_name = quote_identifier(statement.name)
        return f"DROP POLICY IF EXISTS {policy_name} ON {table_ref};"


class AlterPolicyConvertor(StatementConvertor[AlterPolicyStatement]):
    """Convert ALTER POLICY statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "alter_policy"

    def convert(self, statement: AlterPolicyStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        policy_name = quote_identifier(statement.name)

        parts = [f"ALTER POLICY {policy_name} ON {table_ref}"]

        if statement.roles is not None:
            roles = ", ".join(quote_identifier(r) for r in statement.roles)
            parts.append(f"TO {roles}")

        if statement.using is not None:
            parts.append(f"USING ({statement.using})")

        if statement.with_check is not None:
            parts.append(f"WITH CHECK ({statement.with_check})")

        return " ".join(parts) + ";"


# Register convertors
ConvertorRegistry.register(EnableRLSConvertor())
ConvertorRegistry.register(DisableRLSConvertor())
ConvertorRegistry.register(CreatePolicyConvertor())
ConvertorRegistry.register(DropPolicyConvertor())
ConvertorRegistry.register(AlterPolicyConvertor())
