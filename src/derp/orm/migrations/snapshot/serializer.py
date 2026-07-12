"""Serialize Python Table classes to snapshot models.

This module converts derp Table definitions into JSON-serializable snapshot
models that can be compared for migration generation.
"""

from __future__ import annotations

from typing import Any

from derp.orm.column.base import FK as OrmFK
from derp.orm.column.base import Column
from derp.orm.column.types import Enum as EnumColumn
from derp.orm.constraint import Check as CheckConstraint
from derp.orm.constraint import Unique as UniqueConstraint
from derp.orm.index import NullsPosition, SortOrder, _expression_to_literal_sql
from derp.orm.migrations.errors import SchemaError
from derp.orm.migrations.snapshot.models import (
    CheckConstraintSnapshot,
    ColumnSnapshot,
    EnumSnapshot,
    ForeignKeyAction,
    ForeignKeySnapshot,
    IndexColumnSnapshot,
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
        collation=col.collation,
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


def serialize_table(table_cls: type[Table], schema: str = "public") -> TableSnapshot:
    """Serialize a Table class to a TableSnapshot."""
    table_name = table_cls.get_table_name()
    columns_info = table_cls.get_columns()

    columns: dict[str, ColumnSnapshot] = {}
    foreign_keys: dict[str, ForeignKeySnapshot] = {}
    indexes: dict[str, IndexSnapshot] = {}
    unique_constraints: dict[str, UniqueConstraintSnapshot] = {}
    check_constraints: dict[str, CheckConstraintSnapshot] = {}
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

        # Column-level check constraint
        if col.check is not None:
            cc_name = f"{table_name}_{col_name}_check"
            check_constraints[cc_name] = CheckConstraintSnapshot(
                name=cc_name,
                expression=col.check,
            )

    # Table-level constraints. Unique ones must land in ``unique_constraints``
    # rather than ``indexes``: PostgreSQL records them in ``pg_constraint``, and
    # the introspector's index query excludes constraint-backed indexes. A
    # multi-column uniqueness declared as ``Index(..., unique=True)`` would
    # therefore never compare equal to the live database, and every ``derp push``
    # would re-plan a DROP CONSTRAINT + CREATE UNIQUE INDEX rewrite.
    for constraint in table_cls._resolved_constraints:
        name = constraint.auto_name(table_name)
        match constraint:
            case UniqueConstraint():
                if name in unique_constraints:
                    raise SchemaError(
                        f"{table_cls.__name__}: duplicate unique constraint "
                        f"'{name}'. Pass name= to disambiguate."
                    )
                unique_constraints[name] = UniqueConstraintSnapshot(
                    name=name,
                    columns=list(constraint.columns),
                    nulls_not_distinct=not constraint.nulls_distinct,
                )
            case CheckConstraint():
                if name in check_constraints:
                    raise SchemaError(
                        f"{table_cls.__name__}: duplicate check constraint "
                        f"'{name}'. Pass name= to disambiguate."
                    )
                check_constraints[name] = CheckConstraintSnapshot(
                    name=name,
                    expression=constraint.expression,
                )
            case _:
                raise SchemaError(
                    f"{table_cls.__name__}: cannot serialize constraint "
                    f"{constraint!r} of type {type(constraint).__name__}."
                )

    # Indexes
    for idx in table_cls._resolved_indexes:
        idx_name = idx.auto_name(table_name)
        where_sql = (
            _expression_to_literal_sql(idx.where) if idx.where is not None else None
        )
        # Capture per-column metadata (opclass, ASC/DESC, NULLS FIRST/LAST,
        # collation) so it survives the snapshot round-trip. ``columns`` is
        # kept as a flat name list for backwards compatibility with older
        # snapshot consumers.
        column_specs = [
            IndexColumnSnapshot(
                name=c.name,
                expression=c.expression,
                opclass=c.opclass,
                order=SortOrder(c.order).value if c.order is not None else None,
                nulls=NullsPosition(c.nulls).value if c.nulls is not None else None,
                collation=c.collation,
            )
            for c in idx.columns
        ]
        indexes[idx_name] = IndexSnapshot(
            name=idx_name,
            columns=idx.column_names,
            column_specs=column_specs,
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
        check_constraints=check_constraints,
        rls_enabled=False,
        rls_forced=False,
    )


def extract_enums(
    tables: list[type[Table]], schema: str = "public"
) -> dict[str, EnumSnapshot]:
    """Extract enum types from table definitions.

    Walks each ``Enum[...]`` column and pulls the underlying Python enum class
    (stored on the column type via ``__class_getitem__``) to recover both the
    PostgreSQL type name and its values. Without this, every snapshot's
    ``enums`` field stays empty and ``derp generate`` re-emits ``CREATE TYPE``
    on every run, which fails against any database that already has the type.
    """
    enums: dict[str, EnumSnapshot] = {}

    for table_cls in tables:
        for col in table_cls.get_columns().values():
            if not isinstance(col, EnumColumn):
                continue
            enum_cls = col._enum_cls
            if enum_cls is None:
                continue
            sql_name = col.sql_type()
            # Drop array brackets — `status: Enum[Status][]` still maps to
            # the same underlying type.
            while sql_name.endswith("[]"):
                sql_name = sql_name[:-2]
            if sql_name in enums:
                continue
            enums[sql_name] = EnumSnapshot(
                name=sql_name,
                schema_name=schema,
                values=[v.value for v in enum_cls],
            )

    return enums


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
