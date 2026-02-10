"""Serialize Python Table classes to snapshot models.

This module converts derp Table definitions into JSON-serializable snapshot
models that can be compared for migration generation.
"""

from __future__ import annotations

import enum as enum_lib
import re
from typing import Any

from derp.orm.fields import (
    Array,
    BigSerial,
    Enum,
    FieldInfo,
    FieldType,
    ForeignKey,
    Serial,
)
from derp.orm.fields import (
    ForeignKeyAction as OrmForeignKeyAction,
)
from derp.orm.migrations.snapshot.models import (
    ColumnSnapshot,
    EnumSnapshot,
    ForeignKeyAction,
    ForeignKeySnapshot,
    IndexMethod,
    IndexSnapshot,
    PrimaryKeySnapshot,
    SchemaSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
)
from derp.orm.table import Table


def _map_foreign_key_action(
    action: OrmForeignKeyAction | None,
) -> ForeignKeyAction | None:
    """Map ORM foreign key action to snapshot action."""
    if action is None:
        return None
    mapping = {
        OrmForeignKeyAction.CASCADE: ForeignKeyAction.CASCADE,
        OrmForeignKeyAction.SET_NULL: ForeignKeyAction.SET_NULL,
        OrmForeignKeyAction.SET_DEFAULT: ForeignKeyAction.SET_DEFAULT,
        OrmForeignKeyAction.RESTRICT: ForeignKeyAction.RESTRICT,
        OrmForeignKeyAction.NO_ACTION: ForeignKeyAction.NO_ACTION,
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
        # Check if it's a SQL function or expression
        if (
            default.endswith("()")
            or default.upper() in ("CURRENT_TIMESTAMP", "TRUE", "FALSE")
            or "(" in default
        ):
            return default
        # Otherwise it's a string literal
        return f"'{default}'"
    return str(default)


def _extract_array_dimensions(field_type: FieldType[Any]) -> tuple[str, int]:
    """Extract base type and array dimensions from a field type."""
    dimensions = 0
    current = field_type
    while isinstance(current, Array):
        dimensions += 1
        current = current.element_type
    return current.sql_type(), dimensions


def serialize_column(name: str, field_info: FieldInfo[Any]) -> ColumnSnapshot:
    """Serialize a single column definition to snapshot."""
    field_type = field_info.field_type

    # Handle array types
    if isinstance(field_type, Array):
        base_type, dimensions = _extract_array_dimensions(field_type)
    else:
        base_type = field_type.sql_type()
        dimensions = 0

    # Determine if primary key (Serial/BigSerial implies PK behavior)
    is_pk = field_info.primary_key

    # Determine nullability
    # Primary keys are implicitly NOT NULL
    # Serial types are implicitly NOT NULL
    is_not_null = (
        not field_info.nullable or is_pk or isinstance(field_type, Serial | BigSerial)
    )

    return ColumnSnapshot(
        name=name,
        type=base_type.lower(),
        primary_key=is_pk,
        not_null=is_not_null,
        unique=field_info.unique and not is_pk,  # PK implies unique
        default=_serialize_default(field_info.default),
        generated=None,  # TODO: support generated columns
        identity=None,  # TODO: support identity columns
        array_dimensions=dimensions,
    )


def serialize_foreign_key(
    table_name: str,
    column_name: str,
    fk: ForeignKey,
    constraint_num: int,
) -> tuple[str, ForeignKeySnapshot]:
    """Serialize a foreign key constraint to snapshot."""
    # Handle class references (e.g., ForeignKey(User))
    if isinstance(fk.reference, type) and issubclass(fk.reference, Table):
        ref_table = fk.reference.get_table_name()
        primary_key = fk.reference.get_primary_key()
        if primary_key is None:
            raise ValueError(f"Table `{fk.reference.__name__}` has no primary key.")
        ref_column = primary_key[0]
        ref_schema = "public"
    else:
        # Parse string reference like "users.id" or "public.users.id"
        ref_parts = fk.reference.split(".")
        if len(ref_parts) == 2:
            ref_table, ref_column = ref_parts
            ref_schema = "public"
        elif len(ref_parts) == 3:
            ref_schema, ref_table, ref_column = ref_parts
        else:
            raise ValueError(f"Invalid foreign key reference: {fk.reference}")

    # Generate constraint name
    constraint_name = f"{table_name}_{column_name}_fkey"

    return constraint_name, ForeignKeySnapshot(
        name=constraint_name,
        columns=[column_name],
        references_schema=ref_schema,
        references_table=ref_table,
        references_columns=[ref_column],
        on_delete=_map_foreign_key_action(fk.on_delete),
        on_update=_map_foreign_key_action(fk.on_update),
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
    """Serialize a Table class to a TableSnapshot.

    Args:
        table_cls: The Table class to serialize
        schema: The database schema name (default: "public")

    Returns:
        TableSnapshot representing the table definition
    """
    table_name = table_cls.get_table_name()
    columns_info = table_cls.get_columns()

    columns: dict[str, ColumnSnapshot] = {}
    foreign_keys: dict[str, ForeignKeySnapshot] = {}
    indexes: dict[str, IndexSnapshot] = {}
    unique_constraints: dict[str, UniqueConstraintSnapshot] = {}
    primary_key_columns: list[str] = []

    fk_counter = 0

    for col_name, field_info in columns_info.items():
        # Serialize column
        columns[col_name] = serialize_column(col_name, field_info)

        # Track primary key columns
        if field_info.primary_key:
            primary_key_columns.append(col_name)

        # Serialize foreign key if present
        if field_info.foreign_key:
            fk_name, fk_snapshot = serialize_foreign_key(
                table_name, col_name, field_info.foreign_key, fk_counter
            )
            foreign_keys[fk_name] = fk_snapshot
            fk_counter += 1

        # Serialize index if present
        if field_info.index:
            idx_name, idx_snapshot = serialize_index(table_name, col_name)
            indexes[idx_name] = idx_snapshot

        # Unique constraint (if not already PK which implies unique)
        if field_info.unique and not field_info.primary_key:
            uc_name = f"{table_name}_{col_name}_unique"
            unique_constraints[uc_name] = UniqueConstraintSnapshot(
                name=uc_name,
                columns=[col_name],
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
    """Extract enum types from table definitions.

    Args:
        tables: List of Table classes
        schema: The database schema name

    Returns:
        Dict of enum name to EnumSnapshot
    """
    enums: dict[str, EnumSnapshot] = {}

    for table_cls in tables:
        for col_name, field_info in table_cls.get_columns().items():
            field_type = field_info.field_type

            # Handle arrays of enums
            while isinstance(field_type, Array):
                field_type = field_type.element_type

            if isinstance(field_type, Enum):
                enum_type: type[enum_lib.Enum] = field_type.enum  # type: ignore[assignment]
                enum_name = _to_snake_case(enum_type.__name__)

                if enum_name not in enums:
                    enums[enum_name] = EnumSnapshot(
                        name=enum_name,
                        schema_name=schema,
                        values=[e.value for e in enum_type],
                    )

    return enums


def serialize_schema(
    tables: list[type[Table]],
    schema: str = "public",
    snapshot_id: str = "",
    prev_id: str | None = None,
) -> SchemaSnapshot:
    """Serialize a list of Table classes to a complete SchemaSnapshot.

    Args:
        tables: List of Table classes to serialize
        schema: The database schema name (default: "public")
        snapshot_id: Unique identifier for this snapshot
        prev_id: ID of the previous snapshot (for diffing chain)

    Returns:
        SchemaSnapshot representing the complete schema
    """
    table_snapshots: dict[str, TableSnapshot] = {}
    schemas: list[str] = [schema] if schema else ["public"]

    for table_cls in tables:
        table_snapshot = serialize_table(table_cls, schema)
        key = (
            table_snapshot.name
            if schema == "public"
            else f"{schema}.{table_snapshot.name}"
        )
        table_snapshots[key] = table_snapshot

    # Extract enums from tables
    enums = extract_enums(tables, schema)

    return SchemaSnapshot(
        tables=table_snapshots,
        enums=enums,
        sequences={},  # TODO: extract sequences
        schemas=schemas,
        policies={},  # Policies are DB-only, not defined in code
        roles={},  # Roles are DB-only, not defined in code
        grants=[],  # Grants are DB-only, not defined in code
        id=snapshot_id,
        prev_id=prev_id,
    )


def _to_snake_case(name: str) -> str:
    """Convert CamelCase to snake_case."""
    pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
    return pattern.sub("_", name).lower()
