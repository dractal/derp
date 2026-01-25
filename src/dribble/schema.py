"""Schema introspection and DDL generation for Dribble ORM."""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path

import asyncpg

from dribble.fields import FieldInfo
from dribble.table import Table


@dataclass
class ColumnInfo:
    """Information about a database column."""

    name: str
    data_type: str
    is_nullable: bool
    column_default: str | None
    is_primary_key: bool
    is_unique: bool
    foreign_key: tuple[str, str] | None  # (ref_table, ref_column)


@dataclass
class TableInfo:
    """Information about a database table."""

    name: str
    columns: dict[str, ColumnInfo]


async def introspect_database(pool: asyncpg.Pool, schema: str = "public") -> dict[str, TableInfo]:
    """Introspect the database schema and return table information.

    Args:
        pool: asyncpg connection pool
        schema: Database schema to introspect

    Returns:
        Dict mapping table names to TableInfo
    """
    tables: dict[str, TableInfo] = {}

    async with pool.acquire() as conn:
        # Get all tables
        table_rows = await conn.fetch(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = $1
              AND table_type = 'BASE TABLE'
              AND table_name NOT LIKE '_dribble_%'
            ORDER BY table_name
            """,
            schema,
        )

        for table_row in table_rows:
            table_name = table_row["table_name"]

            # Get columns for this table
            column_rows = await conn.fetch(
                """
                SELECT
                    c.column_name,
                    c.data_type,
                    c.is_nullable,
                    c.column_default,
                    c.character_maximum_length,
                    c.numeric_precision,
                    c.numeric_scale
                FROM information_schema.columns c
                WHERE c.table_schema = $1 AND c.table_name = $2
                ORDER BY c.ordinal_position
                """,
                schema,
                table_name,
            )

            # Get primary key columns
            pk_rows = await conn.fetch(
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
                table_name,
            )
            pk_columns = {row["attname"] for row in pk_rows}

            # Get unique constraints
            unique_rows = await conn.fetch(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                JOIN pg_class c ON c.oid = i.indrelid
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE i.indisunique AND NOT i.indisprimary
                  AND n.nspname = $1
                  AND c.relname = $2
                """,
                schema,
                table_name,
            )
            unique_columns = {row["attname"] for row in unique_rows}

            # Get foreign keys
            fk_rows = await conn.fetch(
                """
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name
                    AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY'
                  AND tc.table_schema = $1
                  AND tc.table_name = $2
                """,
                schema,
                table_name,
            )
            fk_map = {
                row["column_name"]: (row["foreign_table_name"], row["foreign_column_name"])
                for row in fk_rows
            }

            columns: dict[str, ColumnInfo] = {}
            for col_row in column_rows:
                col_name = col_row["column_name"]
                columns[col_name] = ColumnInfo(
                    name=col_name,
                    data_type=col_row["data_type"],
                    is_nullable=col_row["is_nullable"] == "YES",
                    column_default=col_row["column_default"],
                    is_primary_key=col_name in pk_columns,
                    is_unique=col_name in unique_columns,
                    foreign_key=fk_map.get(col_name),
                )

            tables[table_name] = TableInfo(name=table_name, columns=columns)

    return tables


def load_tables_from_module(module_path: str) -> list[type[Table]]:
    """Load Table subclasses from a Python module.

    Args:
        module_path: Path to Python file or module (e.g., "src/myapp/schema.py")

    Returns:
        List of Table subclasses found in the module
    """
    path = Path(module_path)
    if not path.exists():
        raise FileNotFoundError(f"Module not found: {module_path}")

    # Load module from file path
    spec = importlib.util.spec_from_file_location("schema_module", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {module_path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["schema_module"] = module
    spec.loader.exec_module(module)

    # Find all Table subclasses
    tables: list[type[Table]] = []
    for name in dir(module):
        obj = getattr(module, name)
        if (
            isinstance(obj, type)
            and issubclass(obj, Table)
            and obj is not Table
            and hasattr(obj, "__columns__")
        ):
            tables.append(obj)

    return tables


def generate_ddl(tables: list[type[Table]]) -> str:
    """Generate DDL for all tables.

    Args:
        tables: List of Table classes

    Returns:
        Complete DDL string for creating all tables
    """
    ddl_parts: list[str] = []
    for table in tables:
        ddl_parts.append(table.to_ddl())
    return "\n\n".join(ddl_parts)


@dataclass
class SchemaDiff:
    """Represents differences between schema and database."""

    tables_to_create: list[type[Table]]
    tables_to_drop: list[str]
    # table -> [(col_name, field_info)]
    columns_to_add: dict[str, list[tuple[str, FieldInfo]]]
    # table -> [col_name]
    columns_to_drop: dict[str, list[str]]
    # table -> [(col_name, old, new)]
    columns_to_alter: dict[str, list[tuple[str, ColumnInfo, FieldInfo]]]


def compare_schemas(
    tables: list[type[Table]],
    db_tables: dict[str, TableInfo],
) -> SchemaDiff:
    """Compare table definitions against database schema.

    Args:
        tables: List of Table classes from code
        db_tables: Dict of TableInfo from database introspection

    Returns:
        SchemaDiff with detected differences
    """
    schema_table_names = {t.get_table_name() for t in tables}
    db_table_names = set(db_tables.keys())

    # Tables to create/drop
    tables_to_create = [t for t in tables if t.get_table_name() not in db_table_names]
    tables_to_drop = [name for name in db_table_names if name not in schema_table_names]

    # Column-level changes for existing tables
    columns_to_add: dict[str, list[tuple[str, FieldInfo]]] = {}
    columns_to_drop: dict[str, list[str]] = {}
    columns_to_alter: dict[str, list[tuple[str, ColumnInfo, FieldInfo]]] = {}

    for table in tables:
        table_name = table.get_table_name()
        if table_name not in db_tables:
            continue

        db_table = db_tables[table_name]
        schema_columns = table.get_columns()
        db_column_names = set(db_table.columns.keys())
        schema_column_names = set(schema_columns.keys())

        # Columns to add
        new_cols = schema_column_names - db_column_names
        if new_cols:
            columns_to_add[table_name] = [(col, schema_columns[col]) for col in new_cols]

        # Columns to drop
        dropped_cols = db_column_names - schema_column_names
        if dropped_cols:
            columns_to_drop[table_name] = list(dropped_cols)

        # TODO: Detect column type changes (columns_to_alter)

    return SchemaDiff(
        tables_to_create=tables_to_create,
        tables_to_drop=tables_to_drop,
        columns_to_add=columns_to_add,
        columns_to_drop=columns_to_drop,
        columns_to_alter=columns_to_alter,
    )


def generate_migration_sql(diff: SchemaDiff) -> tuple[str, str]:
    """Generate up and down migration SQL from schema diff.

    Args:
        diff: SchemaDiff object

    Returns:
        Tuple of (up_sql, down_sql)
    """
    up_parts: list[str] = []
    down_parts: list[str] = []

    # Create new tables
    for table in diff.tables_to_create:
        up_parts.append(table.to_ddl())
        down_parts.insert(0, f"DROP TABLE IF EXISTS {table.get_table_name()};")

    # Drop removed tables
    for table_name in diff.tables_to_drop:
        up_parts.append(f"DROP TABLE IF EXISTS {table_name};")
        # Note: Can't generate down SQL for dropped tables without knowing their schema
        down_parts.insert(0, f"-- TODO: Recreate table {table_name}")

    # Add new columns
    for table_name, columns in diff.columns_to_add.items():
        for col_name, field_info in columns:
            col_type = field_info.field_type.sql_type()
            nullable = "NULL" if field_info.nullable else "NOT NULL"

            up_sql = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type} {nullable};"
            up_parts.append(up_sql)
            down_parts.insert(0, f"ALTER TABLE {table_name} DROP COLUMN {col_name};")

    # Drop removed columns
    for table_name, columns in diff.columns_to_drop.items():
        for col_name in columns:
            up_parts.append(f"ALTER TABLE {table_name} DROP COLUMN {col_name};")
            down_parts.insert(0, f"-- TODO: Recreate column {table_name}.{col_name}")

    return "\n".join(up_parts), "\n".join(down_parts)
