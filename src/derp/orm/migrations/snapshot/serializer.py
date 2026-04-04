"""Serialize Python Table classes to snapshot models.

This module converts derp Table definitions into JSON-serializable snapshot
models that can be compared for migration generation.
"""

from __future__ import annotations

import re
from typing import Any

from derp.orm.column.base import FK as OrmFK
from derp.orm.column.base import Column
from derp.orm.index import _expression_to_literal_sql
from derp.orm.migrations.snapshot.models import (
    ColumnSnapshot,
    EnumSnapshot,
    ForeignKeyAction,
    ForeignKeySnapshot,
    IndexMethod,
    IndexSnapshot,
    PrimaryKeySnapshot,
    SchemaSnapshot,
    SnapshotVersion,
    TableSnapshot,
    UniqueConstraintSnapshot,
)
from derp.orm.table import Table


def _map_foreign_key_action(
    action: OrmFK | None,
) -> ForeignKeyAction | None:
    """Map ORM foreign key action to snapshot action."""
    if action is None:
        return None
    mapping = {
        OrmFK.CASCADE: ForeignKeyAction.CASCADE,
        OrmFK.SET_NULL: ForeignKeyAction.SET_NULL,
        OrmFK.SET_DEFAULT: ForeignKeyAction.SET_DEFAULT,
        OrmFK.RESTRICT: ForeignKeyAction.RESTRICT,
    }
    return mapping.get(action)


def _serialize_default(default: Any) -> str | None:
    """Serialize a default value to SQL string."""
    if default is None:
        return None
    if isinstance(default, bool):
        return str(default).upper()
    if isinstance(default, int | float):
        return str(default)
    if isinstance(default, str):
        # Check if it's a SQL function or expression that should not be quoted.
        # This includes function calls like gen_random_uuid(), SQL keywords like
        # CURRENT_TIMESTAMP, expressions with parentheses, and pre-formatted
        # SQL literals with type casts like  '0'::bigint  or  '{}'::jsonb.
        if (
            default.endswith("()")
            or default.upper() in ("CURRENT_TIMESTAMP", "TRUE", "FALSE")
            or "(" in default
            or (default.startswith("'") and "::" in default)
        ):
            return default
        # Otherwise it's a string literal — wrap in SQL single-quotes
        return f"'{default}'"
    return str(default)


def _extract_array_info(sql_type: str) -> tuple[str, int]:
    """Extract base type and array dimensions from a SQL type string."""
    dimensions = 0
    base = sql_type
    while base.endswith("[]"):
        dimensions += 1
        base = base[:-2]
    return base, dimensions


def serialize_column(name: str, col: Column[Any]) -> ColumnSnapshot:
    """Serialize a single column definition to snapshot."""
    sql_type = col.sql_type()

    # Handle array types
    base_type, dimensions = _extract_array_info(sql_type)

    # Determine if primary key
    is_pk = col.primary_key

    # Determine nullability
    # Primary keys are implicitly NOT NULL
    # Serial types are implicitly NOT NULL
    is_not_null = not col.nullable or is_pk or col.is_auto_increment()

    return ColumnSnapshot(
        name=name,
        type=base_type.lower(),
        primary_key=is_pk,
        not_null=is_not_null,
        unique=col.unique and not is_pk,  # PK implies unique
        default=_serialize_default(col.default),
        generated=col.generated,
        identity=None,  # TODO: support identity columns
        array_dimensions=dimensions,
    )


def serialize_foreign_key(
    table_name: str,
    column_name: str,
    col: Column[Any],
    constraint_num: int,
) -> tuple[str, ForeignKeySnapshot]:
    """Serialize a foreign key constraint to snapshot."""
    fk_ref = col.foreign_key

    if isinstance(fk_ref, Column):
        if not fk_ref._table_name or not fk_ref._field_name:
            raise ValueError("Column in foreign_key has no table metadata.")
        ref_table = fk_ref._table_name
        ref_column = fk_ref._field_name
        ref_schema = "public"
    elif isinstance(fk_ref, str):
        ref_parts = fk_ref.split(".")
        if len(ref_parts) == 2:
            ref_table, ref_column = ref_parts
            ref_schema = "public"
        elif len(ref_parts) == 3:
            ref_schema, ref_table, ref_column = ref_parts
        else:
            raise ValueError(f"Invalid foreign key reference: {fk_ref}")
    else:
        raise ValueError(f"Invalid foreign key reference: {fk_ref}")

    constraint_name = f"{table_name}_{column_name}_fkey"

    return constraint_name, ForeignKeySnapshot(
        name=constraint_name,
        columns=[column_name],
        references_schema=ref_schema,
        references_table=ref_table,
        references_columns=[ref_column],
        on_delete=_map_foreign_key_action(col.on_delete),
        on_update=_map_foreign_key_action(col.on_update),
    )


def serialize_index(
    table_name: str,
    column_name: str,
    is_unique: bool = False,
) -> tuple[str, IndexSnapshot]:
    """Serialize an index to snapshot."""
    prefix = "uniq" if is_unique else "idx"
    index_name = f"{prefix}_{table_name}_{column_name}"

    return index_name, IndexSnapshot(
        name=index_name,
        columns=[column_name],
        unique=is_unique,
        method=IndexMethod.BTREE,
    )


def serialize_table(table_cls: type[Table], schema: str = "public") -> TableSnapshot:
    """Serialize a Table class to a TableSnapshot."""
    table_name = table_cls.get_table_name()
    columns_info = table_cls.get_columns()

    columns: dict[str, ColumnSnapshot] = {}
    foreign_keys: dict[str, ForeignKeySnapshot] = {}
    indexes: dict[str, IndexSnapshot] = {}
    unique_constraints: dict[str, UniqueConstraintSnapshot] = {}
    primary_key_columns: list[str] = []

    fk_counter = 0

    for col_name, col in columns_info.items():
        # Serialize column
        columns[col_name] = serialize_column(col_name, col)

        # Track primary key columns
        if col.primary_key:
            primary_key_columns.append(col_name)

        # Serialize foreign key if present
        if col.foreign_key:
            fk_name, fk_snapshot = serialize_foreign_key(
                table_name, col_name, col, fk_counter
            )
            foreign_keys[fk_name] = fk_snapshot
            fk_counter += 1

        # Unique constraint (if not already PK which implies unique)
        if col.unique and not col.primary_key:
            uc_name = f"{table_name}_{col_name}_key"
            unique_constraints[uc_name] = UniqueConstraintSnapshot(
                name=uc_name,
                columns=[col_name],
            )

    # Indexes
    for idx in table_cls._resolved_indexes:
        idx_name = idx.auto_name(table_name)
        where_sql = (
            _expression_to_literal_sql(idx.where) if idx.where is not None else None
        )
        indexes[idx_name] = IndexSnapshot(
            name=idx_name,
            columns=idx.column_names,
            unique=idx.unique,
            where=where_sql,
            method=IndexMethod(idx.method.value),
            concurrently=idx.concurrently,
            nulls_not_distinct=not idx.nulls_distinct,
            include=list(idx.include),
            with_options=dict(idx.with_params),
        )

    # Build primary key snapshot
    primary_key = None
    if primary_key_columns:
        pk_name = f"{table_name}_pkey" if len(primary_key_columns) > 1 else None
        primary_key = PrimaryKeySnapshot(
            name=pk_name,
            columns=primary_key_columns,
        )

    return TableSnapshot(
        name=table_name,
        schema_name=schema,
        columns=columns,
        primary_key=primary_key,
        foreign_keys=foreign_keys,
        indexes=indexes,
        unique_constraints=unique_constraints,
        check_constraints={},  # TODO: support check constraints
        rls_enabled=False,
        rls_forced=False,
    )


def extract_enums(
    tables: list[type[Table]], schema: str = "public"
) -> dict[str, EnumSnapshot]:
    """Extract enum types from table definitions."""
    enums: dict[str, EnumSnapshot] = {}

    for table_cls in tables:
        for _col_name, col in table_cls.get_columns().items():
            sql_type = col.sql_type()

            # Strip array brackets to check base type
            base_type = sql_type
            while base_type.endswith("[]"):
                base_type = base_type[:-2]

            # Check if this looks like an enum (snake_case name, not a
            # standard SQL type keyword)
            if base_type.upper() not in _SQL_BUILTIN_TYPES and base_type not in enums:
                # Try to find the enum class from the column's context
                # For now, we detect enum columns by checking if the sql_type
                # is a snake_case name (enum types are named after the Python enum)
                if re.match(r"^[a-z][a-z0-9_]*$", base_type):
                    # This is likely an enum type — but we need the values.
                    # The enum values aren't stored on the Column, so we skip
                    # for now. Enum extraction needs the original enum class.
                    pass

    return enums


# Standard SQL type names that are NOT enums
_SQL_BUILTIN_TYPES = frozenset(
    {
        "SERIAL",
        "BIGSERIAL",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "TEXT",
        "BOOLEAN",
        "TIMESTAMP",
        "TIMESTAMP WITH TIME ZONE",
        "DATE",
        "TIME",
        "TIME WITH TIME ZONE",
        "INTERVAL",
        "UUID",
        "NUMERIC",
        "REAL",
        "DOUBLE PRECISION",
        "JSON",
        "JSONB",
        "BYTEA",
    }
)


def serialize_schema(
    tables: list[type[Table]],
    schema: str = "public",
    snapshot_id: str = "",
    prev_id: str | None = None,
) -> SchemaSnapshot:
    """Serialize a list of Table classes to a complete SchemaSnapshot."""
    table_snapshots: dict[str, TableSnapshot] = {}
    for table_cls in tables:
        snap = serialize_table(table_cls, schema)
        table_snapshots[snap.name] = snap

    enums = extract_enums(tables, schema)

    return SchemaSnapshot(
        id=snapshot_id,
        prev_id=prev_id or "",
        version=SnapshotVersion.V1,
        dialect="postgresql",
        tables=table_snapshots,
        enums=enums,
        schemas=["public"],
        sequences={},
        policies={},
        roles={},
    )


def _to_snake_case(name: str) -> str:
    """Convert CamelCase to snake_case."""
    pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
    return pattern.sub("_", name).lower()
