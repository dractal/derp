"""Table base class for Derp ORM using Column descriptors."""

from __future__ import annotations

import copy
import enum as enum_lib
import json
import types as pytypes
from collections.abc import Sequence
from typing import Any, ClassVar, Self, dataclass_transform, get_args, get_origin

from derp.orm.column.base import Column, Field, FieldSpec
from derp.orm.index import Index


def _unwrap_nullable(ann: Any) -> tuple[Any, bool]:
    """Unwrap ``SomeType | None`` → ``(SomeType, True)``."""
    origin = get_origin(ann)
    if origin is pytypes.UnionType:
        args = [a for a in get_args(ann) if a is not type(None)]
        if len(args) == 1:
            return args[0], True
    return ann, False


def _resolve_sql_type(col_type: type[Column[Any]]) -> str:
    """Get the SQL type string from a PG type class, handling enums."""
    sql = getattr(col_type, "_sql_type", "")
    if sql:
        return sql

    # Check if this is Column[SomeEnum] — derive from enum class
    type_args = get_args(col_type)
    if type_args:
        arg = type_args[0]
        if isinstance(arg, type) and issubclass(arg, enum_lib.Enum):
            from derp.orm.column.types import _enum_sql_name

            return _enum_sql_name(arg)

    return ""


@dataclass_transform(kw_only_default=True, field_specifiers=(Field,))
class Table:
    """Base class for all Derp table definitions.

    Example::

        class User(Table, table="users"):
            id: Serial = Field(primary=True)
            name: Text = Field()
            email: Varchar[255] = Field(unique=True)

        # Query building — direct class access:
        db.select(User).where(User.name == "Alice")
    """

    __table_name__: ClassVar[str]
    __explicit_table__: ClassVar[bool]
    __columns__: ClassVar[dict[str, Column[Any]]]
    _resolved_indexes: ClassVar[list[Index]]

    @classmethod
    def indexes(cls) -> Sequence[Index]:
        """Override to define indexes for this table."""
        return []

    def __init_subclass__(cls, table: str | None = None, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        # Set table name
        if table is not None:
            cls.__table_name__ = table
            cls.__explicit_table__ = True

            # Enforce: if parent has __explicit_table__, child must use same
            for base in cls.__mro__[1:]:
                parent_table = getattr(base, "__table_name__", None)
                parent_explicit = getattr(base, "__explicit_table__", False)
                if (
                    parent_explicit
                    and parent_table is not None
                    and table != parent_table
                ):
                    raise TypeError(
                        f"Table '{cls.__name__}' uses table name '{table}' "
                        f"but its parent '{base.__name__}' uses "
                        f"'{parent_table}'. Inherited tables must use the "
                        f"same table name as their parent."
                    )
        elif not hasattr(
            cls, "__table_name__"
        ) or cls.__table_name__ is Table.__dict__.get("__table_name__"):
            cls.__table_name__ = cls.__name__.lower()
            cls.__explicit_table__ = False

        table_name = cls.__table_name__

        # Resolve type annotations (evaluates "Varchar[255]" strings etc.)
        hints = _get_type_hints_safe(cls)

        # Collect columns: inherited + own
        columns: dict[str, Column[Any]] = {}

        # Inherited columns (clone with this class's table name)
        for base in reversed(cls.__mro__[1:]):
            base_columns = getattr(base, "__columns__", None)
            if base_columns is not None:
                for name, col in base_columns.items():
                    if name not in cls.__dict__:
                        clone = copy.copy(col)
                        clone._table_name = table_name
                        clone._field_name = name
                        setattr(cls, name, clone)
                        columns[name] = clone

        # Own columns: FieldSpec in class dict → resolve annotation → Column
        for name in list(cls.__dict__):
            attr = cls.__dict__[name]
            if not isinstance(attr, FieldSpec):
                continue

            ann_type = hints.get(name)
            if ann_type is None:
                raise TypeError(
                    f"{cls.__name__}.{name}: has Field() but no type annotation"
                )

            # Nullable[X] sets the column to nullable
            is_nullable = getattr(ann_type, "_nullable_marker", False)

            # Construct Column from PG type class + FieldSpec
            if isinstance(ann_type, type) and issubclass(ann_type, Column):
                col = ann_type(attr)
            else:
                # Fallback: bare Column with resolved SQL type
                col = Column(attr)
                col._sql_type = _resolve_sql_type(ann_type) or ""

            if is_nullable:
                col._nullable = True
                if not col.has_default:
                    col._default = None

            col._table_name = table_name
            col._field_name = name
            setattr(cls, name, col)
            columns[name] = col

        cls.__columns__ = columns

        # Precompute col_name → "_col_name" for fast hydration
        cls.__slot_map__ = {name: f"_{name}" for name in columns}

        # Validate nullable annotations
        cls._validate_nullable_annotations(hints)

        # Resolve indexes from the indexes() classmethod.
        cls._resolved_indexes = list(cls.indexes())

    def __init__(self, **kwargs: Any) -> None:
        columns = type(self).__columns__

        for name, value in kwargs.items():
            if name not in columns:
                raise TypeError(
                    f"{type(self).__name__}() got an unexpected keyword "
                    f"argument '{name}'"
                )
            setattr(self, name, value)

        # Handle defaults for missing fields
        missing: list[str] = []
        for name, col in columns.items():
            if name not in kwargs:
                if col.has_default:
                    setattr(self, name, col.default)
                elif col.nullable:
                    setattr(self, name, None)
                else:
                    missing.append(name)

        if missing:
            raise TypeError(
                f"{type(self).__name__}() missing required keyword "
                f"arguments: {', '.join(repr(n) for n in missing)}"
            )

    @classmethod
    def _from_row(cls, data: dict[str, Any] | Any) -> Self:
        """Fast-path hydration from a database row.

        Bypasses ``__init__`` validation and descriptor ``__set__``.
        Uses precomputed slot map and ``object.__setattr__`` directly.

        *data* can be a ``dict`` or an ``asyncpg.Record`` — both
        support ``key in data`` and ``data[key]``.
        """
        obj = object.__new__(cls)
        sa = object.__setattr__
        slot_map = cls.__slot_map__
        for col_name, attr_name in slot_map.items():
            if col_name in data:
                sa(obj, attr_name, data[col_name])
        return obj

    @classmethod
    def _validate_nullable_annotations(
        cls, hints: dict[str, Any] | None = None
    ) -> None:
        """Ensure nullable columns use ``Nullable[X]`` and vice versa."""
        if hints is None:
            try:
                hints = _get_type_hints_safe(cls)
            except Exception:
                return

        for name, col in cls.__columns__.items():
            if name not in hints:
                continue
            ann = hints[name]
            is_nullable_ann = getattr(ann, "_nullable_marker", False)

            # Reject | None syntax — must use Nullable[X]
            _, has_union_none = _unwrap_nullable(ann)
            if has_union_none:
                raise TypeError(
                    f"{cls.__name__}.{name}: use Nullable[X] instead "
                    f"of 'X | None' for nullable columns"
                )

            if col.nullable and not is_nullable_ann:
                ann_str = getattr(ann, "__name__", None) or str(ann)
                raise TypeError(
                    f"{cls.__name__}.{name}: column is nullable but "
                    f"annotation {ann_str!r} is not Nullable[...]"
                )

    @classmethod
    def get_table_name(cls) -> str:
        """Get the SQL table name."""
        return cls.__table_name__

    @classmethod
    def get_columns(cls) -> dict[str, Column[Any]]:
        """Get all column definitions."""
        return getattr(cls, "__columns__", {})

    @classmethod
    def get_primary_key(cls) -> tuple[str, Column[Any]] | None:
        """Get the primary key column if any."""
        for name, col in cls.get_columns().items():
            if col.primary_key:
                return (name, col)
        return None

    def to_dict(self) -> dict[str, Any]:
        """Serialize instance to a dict."""
        ga = object.__getattribute__
        result: dict[str, Any] = {}
        for name, attr_name in type(self).__slot_map__.items():
            result[name] = ga(self, attr_name)
        return result

    def to_json(self) -> str:
        """Serialize instance to a JSON string."""
        return json.dumps(self.to_dict(), default=_json_default)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Construct an instance from a dict (ignores unknown keys)."""
        columns = cls.__columns__
        filtered = {k: v for k, v in data.items() if k in columns}
        return cls(**filtered)

    @classmethod
    def from_json(cls, data: str | bytes) -> Self:
        """Construct an instance from a JSON string."""
        return cls.from_dict(json.loads(data))

    @classmethod
    def to_ddl(cls) -> str:
        """Generate CREATE TABLE DDL statement."""
        columns = cls.get_columns()
        if not columns:
            raise ValueError(f"Table {cls.__name__} has no columns defined")

        table_name = cls.get_table_name()
        column_defs: list[str] = []
        constraints: list[str] = []
        indexes: list[str] = []

        for col_name, col in columns.items():
            col_def = f"    {col_name} {col.sql_type()}"

            if col.primary_key:
                col_def += " PRIMARY KEY"
            if not col.nullable and not col.primary_key:
                col_def += " NOT NULL"
            if col.unique and not col.primary_key:
                col_def += " UNIQUE"
            if col.generated is not None:
                col_def += f" GENERATED ALWAYS AS ({col.generated}) STORED"
            elif col.default is not None:
                default = col.default
                if isinstance(default, str) and (
                    default.endswith("()")
                    or default.upper() in ("CURRENT_TIMESTAMP", "TRUE", "FALSE")
                ):
                    col_def += f" DEFAULT {default}"
                elif isinstance(default, bool):
                    col_def += f" DEFAULT {str(default).upper()}"
                elif isinstance(default, (int, float)):
                    col_def += f" DEFAULT {default}"
                else:
                    escaped = str(default).replace("'", "''")
                    col_def += f" DEFAULT '{escaped}'"

            column_defs.append(col_def)

            # Foreign key constraints
            fk_sql = col.foreign_key_sql()
            if fk_sql:
                constraints.append(f"    FOREIGN KEY ({col_name}) {fk_sql}")

        # Indexes
        for idx in cls._resolved_indexes:
            indexes.append(idx.to_ddl(table_name) + ";")

        all_defs = column_defs + constraints
        ddl = f"CREATE TABLE {table_name} (\n"
        ddl += ",\n".join(all_defs)
        ddl += "\n);"

        if indexes:
            ddl += "\n\n" + "\n\n".join(indexes)

        return ddl


def get_column_ref(table: type[Table], column_name: str) -> Column[Any]:
    """Get a column reference for query building."""
    columns = table.get_columns()
    if column_name not in columns:
        raise ValueError(f"Column {column_name} not found in table {table.__name__}")
    return columns[column_name]


def _json_default(obj: Any) -> Any:
    """JSON serializer for types not natively supported."""
    import datetime
    import uuid

    if isinstance(obj, uuid.UUID):
        return obj.hex
    if isinstance(obj, datetime.datetime | datetime.date | datetime.time):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return obj.total_seconds()
    if hasattr(obj, "value"):  # Enum
        return obj.value
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _get_type_hints_safe(cls: type) -> dict[str, Any]:
    """Get type hints for column fields only.

    Instead of ``typing.get_type_hints`` (which tries to evaluate ALL
    annotations including ClassVar), we manually evaluate only the
    annotations that have a corresponding ``FieldSpec`` in the class dict.
    """
    import sys

    # Build namespace for eval
    ns: dict[str, Any] = {}
    # Include typing module for Union, ClassVar etc.
    import typing as _typing

    ns.update(vars(_typing))
    # Include the column types module
    from derp.orm.column import types as _col_types

    ns.update(vars(_col_types))
    # Include the column base module (for Column itself)
    from derp.orm.column import base as _col_base

    ns.update(vars(_col_base))
    # Include the module where the class is defined
    module = sys.modules.get(cls.__module__)
    if module is not None:
        ns.update(vars(module))

    result: dict[str, Any] = {}
    # Walk MRO to collect annotations
    for klass in reversed(cls.__mro__):
        for name, ann in getattr(klass, "__annotations__", {}).items():
            # Only resolve annotations that have a FieldSpec
            if isinstance(cls.__dict__.get(name), FieldSpec):
                if isinstance(ann, str):
                    try:
                        result[name] = eval(ann, ns)  # noqa: S307
                    except Exception:
                        pass
                else:
                    result[name] = ann
    return result
