"""Role and grant operation convertors."""

from __future__ import annotations

from derp.migrations.convertors.base import (
    ConvertorRegistry,
    StatementConvertor,
    quote_identifier,
    quote_schema_table,
)
from derp.migrations.statements.types import (
    CreateRoleStatement,
    DropRoleStatement,
    GrantStatement,
    RevokeStatement,
)


class CreateRoleConvertor(StatementConvertor[CreateRoleStatement]):
    """Convert CREATE ROLE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_role"

    def convert(self, statement: CreateRoleStatement) -> str:
        role_name = quote_identifier(statement.name)
        options: list[str] = []

        if statement.superuser:
            options.append("SUPERUSER")
        else:
            options.append("NOSUPERUSER")

        if statement.create_db:
            options.append("CREATEDB")
        else:
            options.append("NOCREATEDB")

        if statement.create_role:
            options.append("CREATEROLE")
        else:
            options.append("NOCREATEROLE")

        if statement.inherit:
            options.append("INHERIT")
        else:
            options.append("NOINHERIT")

        if statement.login:
            options.append("LOGIN")
        else:
            options.append("NOLOGIN")

        if statement.replication:
            options.append("REPLICATION")
        else:
            options.append("NOREPLICATION")

        if statement.bypass_rls:
            options.append("BYPASSRLS")
        else:
            options.append("NOBYPASSRLS")

        if statement.connection_limit != -1:
            options.append(f"CONNECTION LIMIT {statement.connection_limit}")

        if statement.password:
            # Note: In production, use a safer method for passwords
            options.append(f"PASSWORD '{statement.password}'")

        if statement.valid_until:
            options.append(f"VALID UNTIL '{statement.valid_until}'")

        sql = f"CREATE ROLE {role_name}"
        if options:
            sql += " WITH " + " ".join(options)
        sql += ";"

        # IN ROLE (member of other roles)
        if statement.in_roles:
            for parent_role in statement.in_roles:
                sql += f"\nGRANT {quote_identifier(parent_role)} TO {role_name};"

        return sql


class DropRoleConvertor(StatementConvertor[DropRoleStatement]):
    """Convert DROP ROLE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_role"

    def convert(self, statement: DropRoleStatement) -> str:
        role_name = quote_identifier(statement.name)
        return f"DROP ROLE IF EXISTS {role_name};"


class GrantConvertor(StatementConvertor[GrantStatement]):
    """Convert GRANT statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "grant"

    def convert(self, statement: GrantStatement) -> str:
        privileges = ", ".join(statement.privileges)
        grantee = quote_identifier(statement.grantee)

        # Build object reference
        if statement.object_type.upper() == "TABLE":
            obj_ref = quote_schema_table(statement.object_schema, statement.object_name)
            on_clause = f"ON TABLE {obj_ref}"
        elif statement.object_type.upper() == "SCHEMA":
            on_clause = f"ON SCHEMA {quote_identifier(statement.object_name)}"
        elif statement.object_type.upper() == "SEQUENCE":
            obj_ref = quote_schema_table(statement.object_schema, statement.object_name)
            on_clause = f"ON SEQUENCE {obj_ref}"
        elif statement.object_type.upper() == "FUNCTION":
            obj_ref = quote_schema_table(statement.object_schema, statement.object_name)
            on_clause = f"ON FUNCTION {obj_ref}"
        else:
            obj_ref = quote_schema_table(statement.object_schema, statement.object_name)
            on_clause = f"ON {statement.object_type.upper()} {obj_ref}"

        sql = f"GRANT {privileges} {on_clause} TO {grantee}"

        if statement.with_grant_option:
            sql += " WITH GRANT OPTION"

        return sql + ";"


class RevokeConvertor(StatementConvertor[RevokeStatement]):
    """Convert REVOKE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "revoke"

    def convert(self, statement: RevokeStatement) -> str:
        privileges = ", ".join(statement.privileges)
        grantee = quote_identifier(statement.grantee)

        # Build object reference
        if statement.object_type.upper() == "TABLE":
            obj_ref = quote_schema_table(statement.object_schema, statement.object_name)
            on_clause = f"ON TABLE {obj_ref}"
        elif statement.object_type.upper() == "SCHEMA":
            on_clause = f"ON SCHEMA {quote_identifier(statement.object_name)}"
        elif statement.object_type.upper() == "SEQUENCE":
            obj_ref = quote_schema_table(statement.object_schema, statement.object_name)
            on_clause = f"ON SEQUENCE {obj_ref}"
        elif statement.object_type.upper() == "FUNCTION":
            obj_ref = quote_schema_table(statement.object_schema, statement.object_name)
            on_clause = f"ON FUNCTION {obj_ref}"
        else:
            obj_ref = quote_schema_table(statement.object_schema, statement.object_name)
            on_clause = f"ON {statement.object_type.upper()} {obj_ref}"

        sql = f"REVOKE {privileges} {on_clause} FROM {grantee}"

        if statement.cascade:
            sql += " CASCADE"

        return sql + ";"


# Register convertors
ConvertorRegistry.register(CreateRoleConvertor())
ConvertorRegistry.register(DropRoleConvertor())
ConvertorRegistry.register(GrantConvertor())
ConvertorRegistry.register(RevokeConvertor())
