"""Table base class for the ClickHouse ORM."""

from __future__ import annotations

import copy
import dataclasses
import enum as enum_lib
import json
import sys
import types as pytypes
from collections.abc import Sequence
from typing import (
    Any,
    ClassVar,
    Self,
    dataclass_transform,
    get_args,
    get_origin,
)

from derp.chorm.column.base import Column, Field, FieldSpec
from derp.chorm.engines.base import TableEngine
from derp.chorm.engines.mergetree import MergeTree
from derp.chorm.index import Index, Projection


def _unwrap_union_none(ann: Any) -> tuple[Any, bool]:
    """Detect ``X | None`` annotations and reject them later."""
    origin = get_origin(ann)
    if origin is pytypes.UnionType:
        args = [a for a in get_args(ann) if a is not type(None)]
        if len(args) == 1:
            return args[0], True
    return ann, False


@dataclass_transform(kw_only_default=True, field_specifiers=(Field,))
class Table:
    """Base class for all ClickHouse table definitions.

    Define a table by subclassing ``Table`` with a ``table`` name and
    optionally a ``cluster`` or ``database``::

        class Event(Table, table="events"):
            id: UInt64 = Field()
            user_id: UInt64 = Field()
            ts: DateTime = Field(default=Fn.now())
            type: LowCardinality[String] = Field()

            __engine__ = MergeTree(order_by=("user_id", "ts"))

    The engine is required for ``CREATE TABLE``; default is a no-op
    ``MergeTree`` over the empty key — practical for tests but not for
    production.
    """

    __table_name__: ClassVar[str]
    __explicit_table__: ClassVar[bool]
    __database__: ClassVar[str | None] = None
    __cluster__: ClassVar[str | None] = None
    __columns__: ClassVar[dict[str, Column[Any]]]
    __engine__: ClassVar[TableEngine | None] = None
    __comment__: ClassVar[str | None] = None
    _resolved_indexes: ClassVar[list[Index]]
    _resolved_projections: ClassVar[list[Projection]]

    @classmethod
    def indexes(cls) -> Sequence[Index]:
        """Override to define data-skipping indexes for this table."""
        return []

    @classmethod
    def projections(cls) -> Sequence[Projection]:
        """Override to define projections for this table."""
        return []

    def __init_subclass__(
        cls,
        table: str | None = None,
        database: str | None = None,
        cluster: str | None = None,
        engine: TableEngine | None = None,
        comment: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(**kwargs)

        if table is not None:
            cls.__table_name__ = table
            cls.__explicit_table__ = True
        elif not hasattr(
            cls, "__table_name__"
        ) or cls.__table_name__ is Table.__dict__.get("__table_name__"):
            cls.__table_name__ = cls.__name__.lower()
            cls.__explicit_table__ = False

        if database is not None:
            cls.__database__ = database
        if cluster is not None:
            cls.__cluster__ = cluster
        if engine is not None:
            cls.__engine__ = engine
        if comment is not None:
            cls.__comment__ = comment

        table_name = cls.__table_name__
        database_name = cls.__database__

        hints = _get_type_hints_safe(cls)

        columns: dict[str, Column[Any]] = {}

        # Inherited columns
        for base in reversed(cls.__mro__[1:]):
            base_columns = getattr(base, "__columns__", None)
            if base_columns is not None:
                for name, col in base_columns.items():
                    if name not in cls.__dict__:
                        clone = copy.copy(col)
                        clone._database_name = database_name
                        clone._table_name = table_name
                        clone._field_name = name
                        setattr(cls, name, clone)
                        columns[name] = clone

        # Own columns
        for name in list(cls.__dict__):
            attr = cls.__dict__[name]
            if not isinstance(attr, FieldSpec):
                continue

            ann_type = hints.get(name)
            if ann_type is None:
                raise TypeError(
                    f"{cls.__name__}.{name}: has Field() but no type annotation"
                )

            is_nullable = getattr(ann_type, "_nullable_marker", False)
            is_low_cardinality = getattr(ann_type, "_low_cardinality_marker", False)

            if isinstance(ann_type, type) and issubclass(ann_type, Column):
                col = ann_type(attr)
            else:
                col = Column(attr)

            if is_nullable:
                col._nullable = True
            if is_low_cardinality:
                col._low_cardinality = True

            col._database_name = database_name
            col._table_name = table_name
            col._field_name = name
            setattr(cls, name, col)
            columns[name] = col

        cls.__columns__ = columns
        cls.__slot_map__ = {name: f"_{name}" for name in columns}

        cls._validate_nullable_annotations(hints)
        cls._resolved_indexes = list(cls.indexes())
        cls._resolved_projections = list(cls.projections())

    def __init__(self, **kwargs: Any) -> None:
        columns = type(self).__columns__
        for name, value in kwargs.items():
            if name not in columns:
                raise TypeError(
                    f"{type(self).__name__}() got an unexpected keyword "
                    f"argument '{name}'"
                )
            setattr(self, name, value)

        missing: list[str] = []
        for name, col in columns.items():
            if name in kwargs:
                continue
            if col.has_default:
                setattr(self, name, col.default)
            elif col.is_materialized or col.is_alias:
                setattr(self, name, None)
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
    def _from_row(cls, data: dict[str, Any] | Sequence[Any]) -> Self:
        """Fast-path hydration from a result row.

        Accepts a dict (column-name → value) or a sequence (positional).
        """
        obj = object.__new__(cls)
        sa = object.__setattr__
        slot_map = cls.__slot_map__
        if isinstance(data, dict):
            for col_name, attr_name in slot_map.items():
                if col_name in data:
                    sa(obj, attr_name, data[col_name])
        else:
            for (col_name, attr_name), value in zip(
                slot_map.items(), data, strict=False
            ):
                sa(obj, attr_name, value)
        return obj

    @classmethod
    def _validate_nullable_annotations(
        cls, hints: dict[str, Any] | None = None
    ) -> None:
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

            _, has_union_none = _unwrap_union_none(ann)
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

    # -- Public class accessors ----------------------------------------------

    @classmethod
    def get_table_name(cls) -> str:
        return cls.__table_name__

    @classmethod
    def get_full_name(cls) -> str:
        """Return ``database.table`` when a database is set, else ``table``."""
        if cls.__database__:
            return f"{cls.__database__}.{cls.__table_name__}"
        return cls.__table_name__

    @classmethod
    def get_columns(cls) -> dict[str, Column[Any]]:
        return getattr(cls, "__columns__", {})

    @classmethod
    def get_engine(cls) -> TableEngine:
        """Return the engine, or a default ``MergeTree`` for unset tables."""
        if cls.__engine__ is None:
            return MergeTree(order_by="tuple()")
        return cls.__engine__

    # -- Serialization -------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        ga = object.__getattribute__
        result: dict[str, Any] = {}
        for name, attr_name in type(self).__slot_map__.items():
            try:
                result[name] = ga(self, attr_name)
            except AttributeError:
                result[name] = None
        return result

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), default=_json_default)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        cols = cls.__columns__
        filtered = {k: v for k, v in data.items() if k in cols}
        return cls(**filtered)

    @classmethod
    def from_json(cls, data: str | bytes) -> Self:
        return cls.from_dict(json.loads(data))

    @classmethod
    def to_ddl(cls, *, if_not_exists: bool = False) -> str:
        """Generate the ``CREATE TABLE`` DDL for this table."""
        from derp.chorm.ddl import build_create_table

        return build_create_table(cls, if_not_exists=if_not_exists)


def _json_default(obj: Any) -> Any:
    import datetime
    import uuid as _u

    if isinstance(obj, _u.UUID):
        return obj.hex
    if isinstance(obj, datetime.datetime | datetime.date | datetime.time):
        return obj.isoformat()
    if isinstance(obj, datetime.timedelta):
        return obj.total_seconds()
    if isinstance(obj, dataclasses.Field):  # rare; defensive
        return None
    if isinstance(obj, enum_lib.Enum):
        return obj.value
    if hasattr(obj, "value"):
        return obj.value
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _get_type_hints_safe(cls: type) -> dict[str, Any]:
    """Resolve type hints for fields with a :class:`FieldSpec`.

    Mirrors derp.orm.table._get_type_hints_safe, but pulls names from
    :mod:`derp.chorm.column.types` instead.
    """
    ns: dict[str, Any] = {}
    import typing as _typing

    ns.update(vars(_typing))
    from derp.chorm.column import types as _col_types

    ns.update(vars(_col_types))
    from derp.chorm.column import base as _col_base

    ns.update(vars(_col_base))
    module = sys.modules.get(cls.__module__)
    if module is not None:
        ns.update(vars(module))

    result: dict[str, Any] = {}
    for klass in reversed(cls.__mro__):
        for name, ann in getattr(klass, "__annotations__", {}).items():
            if isinstance(cls.__dict__.get(name), FieldSpec):
                if isinstance(ann, str):
                    try:
                        result[name] = eval(ann, ns)  # noqa: S307
                    except Exception:
                        pass
                else:
                    result[name] = ann
    return result
