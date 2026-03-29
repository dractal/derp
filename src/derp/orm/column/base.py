"""Column descriptor base class for Derp ORM."""

from __future__ import annotations

import dataclasses
import enum as enum_lib
from typing import Any, Literal, Self, overload

from derp.orm.expression_base import ComparisonOperator, Expression


class FK(enum_lib.StrEnum):
    """Actions for foreign key ON DELETE / ON UPDATE clauses."""

    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"
    RESTRICT = "RESTRICT"


class Fn(enum_lib.StrEnum):
    """Predefined SQL functions for use as column defaults."""

    GEN_RANDOM_UUID = "gen_random_uuid()"
    NOW = "now()"
    CURRENT_TIMESTAMP = "CURRENT_TIMESTAMP"

    @staticmethod
    def to_tsvector(config: str, *columns: str) -> str:
        """Build a ``to_tsvector(config, col1 || ' ' || col2)`` expression."""
        expr = " || ' ' || ".join(columns)
        return f"to_tsvector('{config}', {expr})"


class FieldSpec:
    """Column constraints returned by :func:`Field`.

    This is a placeholder that ``Table.__init_subclass__`` replaces with a
    real :class:`Column` descriptor after resolving the type annotation.
    """

    __slots__ = (
        "primary",
        "unique",
        "default",
        "generated",
        "foreign_key",
        "on_delete",
        "on_update",
    )

    def __init__(
        self,
        *,
        primary: bool = False,
        unique: bool = False,
        default: Any = dataclasses.MISSING,
        generated: str | None = None,
        foreign_key: str | Column[Any] | None = None,
        on_delete: FK | None = None,
        on_update: FK | None = None,
    ) -> None:
        if generated is not None and default is not dataclasses.MISSING:
            raise ValueError("A column cannot have both `default` and `generated`.")
        self.primary = primary
        self.unique = unique
        self.default = default
        self.generated = generated
        self.foreign_key = foreign_key
        self.on_delete = on_delete
        self.on_update = on_update


def Field(
    *,
    primary: bool = False,
    unique: bool = False,
    default: Any = dataclasses.MISSING,
    generated: str | None = None,
    foreign_key: str | Column[Any] | None = None,
    on_delete: FK
    | Literal["cascade", "set null", "set default", "restrict"]
    | None = None,
    on_update: FK
    | Literal["cascade", "set null", "set default", "restrict"]
    | None = None,
) -> Any:
    """Declare column constraints.

    Foreign keys::

        Field(foreign_key=User.id, on_delete="cascade")
        Field(foreign_key="users.id")

    Generated columns::

        Field(generated="price * quantity")
    """
    return FieldSpec(
        primary=primary,
        unique=unique,
        default=default,
        generated=generated,
        foreign_key=foreign_key,
        on_delete=(
            FK(on_delete.upper())
            if isinstance(on_delete, str)
            else on_delete
        ),
        on_update=(
            FK(on_update.upper())
            if isinstance(on_update, str)
            else on_update
        ),
    )


class Column[T](Expression):
    """Base descriptor for all table columns.

    Extends Expression so columns can be used directly in query building.
    Implements the descriptor protocol for typed class/instance access.

    Subclasses set ``_sql_type`` as a class variable. Parameterized types
    (e.g., ``Varchar[255]``) override ``sql_type()`` to include parameters.
    """

    _sql_type: str = ""
    _primary: bool
    _unique: bool
    _nullable: bool
    _default: Any
    _generated: str | None
    _foreign_key: str | Column[Any] | None
    _on_delete: FK | None
    _on_update: FK | None
    _table_name: str | None
    _field_name: str | None

    def __init__(self, spec: FieldSpec) -> None:
        # Use object.__setattr__ to bypass the Column descriptor's
        # __set__ which ty treats as governing all _-prefixed attrs.
        object.__setattr__(self, "_primary", spec.primary)
        object.__setattr__(self, "_unique", spec.unique)
        object.__setattr__(self, "_nullable", False)
        object.__setattr__(self, "_default", spec.default)
        object.__setattr__(self, "_generated", spec.generated)
        object.__setattr__(self, "_foreign_key", spec.foreign_key)
        object.__setattr__(self, "_on_delete", spec.on_delete)
        object.__setattr__(self, "_on_update", spec.on_update)
        object.__setattr__(self, "_table_name", None)
        object.__setattr__(self, "_field_name", None)

    # -- Descriptor protocol --------------------------------------------------

    @overload
    def __get__(self, obj: None, owner: type) -> Self: ...

    @overload
    def __get__(self, obj: object, owner: type) -> T: ...

    def __get__(self, obj: object | None, owner: type) -> Self | T:
        if obj is None:
            return self
        return getattr(obj, f"_{self._field_name}")

    def __set__(self, obj: object, value: T) -> None:
        setattr(obj, f"_{self._field_name}", value)

    def __set_name__(self, owner: Any, name: str) -> None:
        self._field_name = name
        # Table name is set later by Table.__init_subclass__

    # -- Metadata accessors ---------------------------------------------------

    @property
    def primary_key(self) -> bool:
        return self._primary

    @property
    def unique(self) -> bool:
        return self._unique

    @property
    def nullable(self) -> bool:
        return self._nullable

    @property
    def default(self) -> Any:
        return self._default if self._default is not dataclasses.MISSING else None

    @property
    def has_default(self) -> bool:
        return self._default is not dataclasses.MISSING

    @property
    def generated(self) -> str | None:
        return self._generated

    @property
    def foreign_key(self) -> str | Column[Any] | None:
        return self._foreign_key

    @property
    def on_delete(self) -> FK | None:
        return self._on_delete

    @property
    def on_update(self) -> FK | None:
        return self._on_update

    def foreign_key_sql(self) -> str | None:
        """Generate the REFERENCES clause, or None if no FK."""
        if self._foreign_key is None:
            return None
        if isinstance(self._foreign_key, Column):
            col: Column[Any] = self._foreign_key
            if not col._table_name or not col._field_name:
                raise ValueError(
                    "Column passed to foreign_key has no table metadata. "
                    "Use a class-level column reference like User.id."
                )
            ref = f"{col._table_name}({col._field_name})"
        else:
            ref = self._foreign_key.replace(".", "(") + ")"
        sql = f"REFERENCES {ref}"
        if self._on_delete:
            sql += f" ON DELETE {self._on_delete}"
        if self._on_update:
            sql += f" ON UPDATE {self._on_update}"
        return sql

    def sql_type(self) -> str:
        return self._sql_type

    def is_auto_increment(self) -> bool:
        return self._sql_type in ("SERIAL", "BIGSERIAL")

    # -- Expression interface -------------------------------------------------

    def to_sql(self, params: list[Any]) -> str:
        if self._table_name and self._field_name:
            return f"{self._table_name}.{self._field_name}"
        if self._field_name:
            return self._field_name
        raise ValueError("Column missing table/field name metadata")

    # -- Comparison operators (supplement Expression's dunders) ----------------

    def __invert__(self) -> Any:
        """Bitwise NOT (~col) — produces ``col = FALSE`` for boolean columns."""
        from derp.orm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.EQ, to_expr(False))

    # -- Aggregate methods (not on Expression base) ---------------------------

    def count(self) -> Any:
        from derp.orm.query.expressions import AggregateFunc

        return AggregateFunc("COUNT", self)

    def sum(self) -> Any:
        from derp.orm.query.expressions import AggregateFunc

        return AggregateFunc("SUM", self)

    def avg(self) -> Any:
        from derp.orm.query.expressions import AggregateFunc

        return AggregateFunc("AVG", self)

    def min(self) -> Any:
        from derp.orm.query.expressions import AggregateFunc

        return AggregateFunc("MIN", self)

    def max(self) -> Any:
        from derp.orm.query.expressions import AggregateFunc

        return AggregateFunc("MAX", self)

    def case(self, mapping: dict[Any, Any], *, else_: Any | None = None) -> Any:
        from derp.orm.query.expressions import CaseExpression

        return CaseExpression(self, list(mapping.items()), else_value=else_)
