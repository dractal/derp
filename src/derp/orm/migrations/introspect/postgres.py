"""PostgreSQL database introspection.

Queries the PostgreSQL system catalogs and information_schema to build
a complete SchemaSnapshot representing the current database state.
"""

from __future__ import annotations

from collections.abc import Sequence

import asyncpg

from derp.orm.migrations.snapshot.models import (
    CheckConstraintSnapshot,
    ColumnSnapshot,
    EnumSnapshot,
    ForeignKeyAction,
    ForeignKeySnapshot,
    IdentityConfig,
    IdentityGeneration,
    IndexMethod,
    IndexSnapshot,
    PolicyCommand,
    PolicySnapshot,
    PrimaryKeySnapshot,
    SchemaSnapshot,
    SequenceSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
)


class PostgresIntrospector:
    """Introspect a PostgreSQL database schema.

    This class queries PostgreSQL system catalogs to extract complete
    schema information matching Drizzle's introspection capabilities.
    """

    def __init__(self, pool: asyncpg.Pool):
        """Initialize the introspector.

        Args:
            pool: asyncpg connection pool
        """
        self.pool = pool

    async def introspect(
        self, *, schemas: Sequence[str], exclude_tables: Sequence[str]
    ) -> SchemaSnapshot:
        """Introspect the database and return a SchemaSnapshot.

        Args:
            schemas: List of schemas to introspect.
            exclude_tables: Table names to exclude.

        Returns:
            SchemaSnapshot representing the database state
        """
        snapshot = SchemaSnapshot(schemas=schemas)

        async with self.pool.acquire() as conn:
            # Introspect enums first (columns depend on them)
            snapshot.enums = await self._get_enums(conn, schemas)

            # Introspect sequences
            snapshot.sequences = await self._get_sequences(conn, schemas)

            # Introspect tables
            snapshot.tables = await self._get_tables(conn, schemas, exclude_tables)

            # Introspect policies
            snapshot.policies = await self._get_policies(conn, schemas, exclude_tables)

            # Introspect roles (optional, can be slow on large systems)
            # snapshot.roles = await self._get_roles(conn)

        return snapshot

    async def _get_enums(
        self, conn: asyncpg.Connection, schemas: Sequence[str]
    ) -> dict[str, EnumSnapshot]:
        """Get all enum types."""
        enums: dict[str, EnumSnapshot] = {}

        rows = await conn.fetch(
            """
            SELECT
                n.nspname AS schema_name,
                t.typname AS enum_name,
                array_agg(e.enumlabel ORDER BY e.enumsortorder) AS values
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            JOIN pg_enum e ON e.enumtypid = t.oid
            WHERE t.typtype = 'e'
              AND n.nspname = ANY($1::text[])
            GROUP BY n.nspname, t.typname
            ORDER BY n.nspname, t.typname
            """,
            schemas,
        )

        for row in rows:
            key = row["enum_name"]
            if row["schema_name"] != "public":
                key = f"{row['schema_name']}.{row['enum_name']}"

            enums[key] = EnumSnapshot(
                name=row["enum_name"],
                schema_name=row["schema_name"],
                values=list(row["values"]),
            )

        return enums

    async def _get_sequences(
        self, conn: asyncpg.Connection, schemas: Sequence[str]
    ) -> dict[str, SequenceSnapshot]:
        """Get all sequences."""
        sequences: dict[str, SequenceSnapshot] = {}

        rows = await conn.fetch(
            """
            SELECT
                schemaname,
                sequencename,
                start_value,
                increment_by,
                min_value,
                max_value,
                cache_size,
                cycle
            FROM pg_sequences
            WHERE schemaname = ANY($1::text[])
            ORDER BY schemaname, sequencename
            """,
            schemas,
        )

        for row in rows:
            key = row["sequencename"]
            if row["schemaname"] != "public":
                key = f"{row['schemaname']}.{row['sequencename']}"

            sequences[key] = SequenceSnapshot(
                name=row["sequencename"],
                schema_name=row["schemaname"],
                start=row["start_value"] or 1,
                increment=row["increment_by"] or 1,
                min_value=row["min_value"],
                max_value=row["max_value"],
                cache=row["cache_size"] or 1,
                cycle=row["cycle"] or False,
            )

        return sequences

    async def _get_tables(
        self,
        conn: asyncpg.Connection,
        schemas: Sequence[str],
        exclude_tables: Sequence[str],
    ) -> dict[str, TableSnapshot]:
        """Get all tables with their columns and constraints."""
        tables: dict[str, TableSnapshot] = {}

        # Get table list
        table_rows = await conn.fetch(
            """
            SELECT
                n.nspname AS schema_name,
                c.relname AS table_name,
                c.relrowsecurity AS rls_enabled,
                c.relforcerowsecurity AS rls_forced
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind = 'r'
              AND n.nspname = ANY($1::text[])
              AND c.relname != ALL($2::text[])
            ORDER BY n.nspname, c.relname
            """,
            schemas,
            exclude_tables,
        )

        for table_row in table_rows:
            schema_name = table_row["schema_name"]
            table_name = table_row["table_name"]

            key = table_name
            if schema_name != "public":
                key = f"{schema_name}.{table_name}"

            # Get columns
            columns = await self._get_columns(conn, schema_name, table_name)

            # Get primary key
            primary_key = await self._get_primary_key(conn, schema_name, table_name)

            # Get foreign keys
            foreign_keys = await self._get_foreign_keys(conn, schema_name, table_name)

            # Get unique constraints
            unique_constraints = await self._get_unique_constraints(
                conn, schema_name, table_name
            )

            # Get check constraints
            check_constraints = await self._get_check_constraints(
                conn, schema_name, table_name
            )

            # Get indexes
            indexes = await self._get_indexes(conn, schema_name, table_name)

            tables[key] = TableSnapshot(
                name=table_name,
                schema_name=schema_name,
                columns=columns,
                primary_key=primary_key,
                foreign_keys=foreign_keys,
                unique_constraints=unique_constraints,
                check_constraints=check_constraints,
                indexes=indexes,
                rls_enabled=table_row["rls_enabled"],
                rls_forced=table_row["rls_forced"],
            )

        return tables

    async def _get_columns(
        self, conn: asyncpg.Connection, schema: str, table: str
    ) -> dict[str, ColumnSnapshot]:
        """Get columns for a table."""
        columns: dict[str, ColumnSnapshot] = {}

        rows = await conn.fetch(
            """
            SELECT
                a.attname AS column_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                a.attnotnull AS not_null,
                pg_get_expr(d.adbin, d.adrelid) AS column_default,
                a.attidentity AS identity,
                a.attgenerated AS generated
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            schema,
            table,
        )

        # Get primary key columns for this table
        pk_cols = await self._get_pk_columns(conn, schema, table)

        for row in rows:
            col_name = row["column_name"]

            # Parse identity column
            identity = None
            if row["identity"]:
                identity = IdentityConfig(
                    generation=(
                        IdentityGeneration.ALWAYS
                        if row["identity"] == "a"
                        else IdentityGeneration.BY_DEFAULT
                    ),
                )

            columns[col_name] = ColumnSnapshot(
                name=col_name,
                type=row["data_type"].lower(),
                primary_key=col_name in pk_cols,
                not_null=row["not_null"],
                unique=False,  # Will be set from unique constraints
                default=row["column_default"],
                generated=row["generated"] if row["generated"] else None,
                identity=identity,
            )

        return columns

    async def _get_pk_columns(
        self, conn: asyncpg.Connection, schema: str, table: str
    ) -> set[str]:
        """Get primary key column names for a table."""
        rows = await conn.fetch(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE i.indisprimary
              AND n.nspname = $1
              AND c.relname = $2
            """,
            schema,
            table,
        )
        return {row["attname"] for row in rows}

    async def _get_primary_key(
        self, conn: asyncpg.Connection, schema: str, table: str
    ) -> PrimaryKeySnapshot | None:
        """Get primary key constraint for a table."""
        row = await conn.fetchrow(
            """
            SELECT
                con.conname AS constraint_name,
                array_agg(
                  a.attname ORDER BY array_position(con.conkey, a.attnum)
                ) AS columns
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = con.conrelid 
              AND a.attnum = ANY(con.conkey)
            WHERE con.contype = 'p'
              AND n.nspname = $1
              AND c.relname = $2
            GROUP BY con.conname
            """,
            schema,
            table,
        )

        if row:
            return PrimaryKeySnapshot(
                name=row["constraint_name"],
                columns=list(row["columns"]),
            )
        return None

    async def _get_foreign_keys(
        self, conn: asyncpg.Connection, schema: str, table: str
    ) -> dict[str, ForeignKeySnapshot]:
        """Get foreign key constraints for a table."""
        foreign_keys: dict[str, ForeignKeySnapshot] = {}

        rows = await conn.fetch(
            """
            SELECT
                con.conname AS constraint_name,
                array_agg(
                  a.attname ORDER BY array_position(con.conkey, a.attnum)
                ) AS columns,
                nf.nspname AS ref_schema,
                cf.relname AS ref_table,
                array_agg(
                  af.attname ORDER BY array_position(con.confkey, af.attnum)
                ) AS ref_columns,
                con.confdeltype AS on_delete,
                con.confupdtype AS on_update,
                con.condeferrable AS deferrable,
                con.condeferred AS deferred
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = con.conrelid 
              AND a.attnum = ANY(con.conkey)
            JOIN pg_class cf ON cf.oid = con.confrelid
            JOIN pg_namespace nf ON nf.oid = cf.relnamespace
            JOIN pg_attribute af ON af.attrelid = con.confrelid 
              AND af.attnum = ANY(con.confkey)
            WHERE con.contype = 'f'
              AND n.nspname = $1
              AND c.relname = $2
            GROUP BY con.conname, nf.nspname, cf.relname, con.confdeltype,
                     con.confupdtype, con.condeferrable, con.condeferred
            """,
            schema,
            table,
        )

        for row in rows:
            foreign_keys[row["constraint_name"]] = ForeignKeySnapshot(
                name=row["constraint_name"],
                columns=list(row["columns"]),
                references_schema=row["ref_schema"],
                references_table=row["ref_table"],
                references_columns=list(row["ref_columns"]),
                on_delete=_map_fk_action(row["on_delete"]),
                on_update=_map_fk_action(row["on_update"]),
                deferrable=row["deferrable"],
                initially_deferred=row["deferred"],
            )

        return foreign_keys

    async def _get_unique_constraints(
        self, conn: asyncpg.Connection, schema: str, table: str
    ) -> dict[str, UniqueConstraintSnapshot]:
        """Get unique constraints for a table (excluding PK)."""
        unique_constraints: dict[str, UniqueConstraintSnapshot] = {}

        rows = await conn.fetch(
            """
            SELECT
                con.conname AS constraint_name,
                array_agg(
                  a.attname ORDER BY array_position(con.conkey, a.attnum)
                ) AS columns
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = con.conrelid 
              AND a.attnum = ANY(con.conkey)
            WHERE con.contype = 'u'
              AND n.nspname = $1
              AND c.relname = $2
            GROUP BY con.conname
            """,
            schema,
            table,
        )

        for row in rows:
            unique_constraints[row["constraint_name"]] = UniqueConstraintSnapshot(
                name=row["constraint_name"],
                columns=list(row["columns"]),
            )

        return unique_constraints

    async def _get_check_constraints(
        self, conn: asyncpg.Connection, schema: str, table: str
    ) -> dict[str, CheckConstraintSnapshot]:
        """Get check constraints for a table."""
        check_constraints: dict[str, CheckConstraintSnapshot] = {}

        rows = await conn.fetch(
            """
            SELECT
                con.conname AS constraint_name,
                pg_get_constraintdef(con.oid) AS definition
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE con.contype = 'c'
              AND n.nspname = $1
              AND c.relname = $2
            """,
            schema,
            table,
        )

        for row in rows:
            # Extract expression from "CHECK (expression)"
            definition = row["definition"]
            if definition.upper().startswith("CHECK (") and definition.endswith(")"):
                expression = definition[7:-1]
            else:
                expression = definition

            check_constraints[row["constraint_name"]] = CheckConstraintSnapshot(
                name=row["constraint_name"],
                expression=expression,
            )

        return check_constraints

    async def _get_indexes(
        self, conn: asyncpg.Connection, schema: str, table: str
    ) -> dict[str, IndexSnapshot]:
        """Get indexes for a table (excluding PK and unique constraint indexes)."""
        indexes: dict[str, IndexSnapshot] = {}

        rows = await conn.fetch(
            """
            SELECT
                i.relname AS index_name,
                ix.indisunique AS is_unique,
                am.amname AS index_method,
                array_agg(
                  a.attname ORDER BY array_position(ix.indkey, a.attnum)
                ) AS columns,
                pg_get_expr(ix.indpred, ix.indrelid) AS where_clause
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            JOIN pg_am am ON am.oid = i.relam
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
            WHERE n.nspname = $1
              AND t.relname = $2
              AND NOT ix.indisprimary
              AND NOT EXISTS (
                  SELECT 1 FROM pg_constraint c
                  WHERE c.conindid = ix.indexrelid AND c.contype IN ('u', 'p')
              )
            GROUP BY i.relname, ix.indisunique, am.amname, ix.indpred, ix.indrelid
            """,
            schema,
            table,
        )

        for row in rows:
            indexes[row["index_name"]] = IndexSnapshot(
                name=row["index_name"],
                columns=list(row["columns"]),
                unique=row["is_unique"],
                method=_map_index_method(row["index_method"]),
                where=row["where_clause"],
            )

        return indexes

    async def _get_policies(
        self,
        conn: asyncpg.Connection,
        schemas: Sequence[str],
        exclude_tables: Sequence[str],
    ) -> dict[str, PolicySnapshot]:
        """Get RLS policies."""
        policies: dict[str, PolicySnapshot] = {}

        rows = await conn.fetch(
            """
            SELECT
                pol.polname AS policy_name,
                n.nspname AS schema_name,
                c.relname AS table_name,
                pol.polcmd AS command,
                pol.polpermissive AS permissive,
                array_agg(r.rolname) AS roles,
                pg_get_expr(pol.polqual, pol.polrelid) AS using_expr,
                pg_get_expr(pol.polwithcheck, pol.polrelid) AS with_check_expr
            FROM pg_policy pol
            JOIN pg_class c ON c.oid = pol.polrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_roles r ON r.oid = ANY(pol.polroles)
            WHERE n.nspname = ANY($1::text[])
              AND c.relname != ALL($2::text[])
            GROUP BY pol.polname, n.nspname, c.relname, pol.polcmd,
                     pol.polpermissive, pol.polqual, pol.polwithcheck, pol.polrelid
            """,
            schemas,
            exclude_tables,
        )

        for row in rows:
            table_key = row["table_name"]
            if row["schema_name"] != "public":
                table_key = f"{row['schema_name']}.{row['table_name']}"
            key = f"{table_key}.{row['policy_name']}"

            policies[key] = PolicySnapshot(
                name=row["policy_name"],
                schema_name=row["schema_name"],
                table=row["table_name"],
                command=_map_policy_command(row["command"]),
                permissive=row["permissive"],
                roles=list(row["roles"]) if row["roles"][0] else ["public"],
                using=row["using_expr"],
                with_check=row["with_check_expr"],
            )

        return policies


def _map_fk_action(action: str) -> ForeignKeyAction | None:
    """Map PostgreSQL FK action character to enum."""
    mapping = {
        "a": ForeignKeyAction.NO_ACTION,
        "r": ForeignKeyAction.RESTRICT,
        "c": ForeignKeyAction.CASCADE,
        "n": ForeignKeyAction.SET_NULL,
        "d": ForeignKeyAction.SET_DEFAULT,
    }
    return mapping.get(action)


def _map_index_method(method: str) -> IndexMethod:
    """Map PostgreSQL index method name to enum."""
    mapping = {
        "btree": IndexMethod.BTREE,
        "hash": IndexMethod.HASH,
        "gin": IndexMethod.GIN,
        "gist": IndexMethod.GIST,
        "spgist": IndexMethod.SPGIST,
        "brin": IndexMethod.BRIN,
    }
    return mapping.get(method, IndexMethod.BTREE)


def _map_policy_command(cmd: str) -> PolicyCommand:
    """Map PostgreSQL policy command character to enum."""
    mapping = {
        "*": PolicyCommand.ALL,
        "r": PolicyCommand.SELECT,
        "a": PolicyCommand.INSERT,
        "w": PolicyCommand.UPDATE,
        "d": PolicyCommand.DELETE,
    }
    return mapping.get(cmd, PolicyCommand.ALL)
