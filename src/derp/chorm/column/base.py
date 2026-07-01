"""Column descriptor base class for the ClickHouse ORM."""

from __future__ import annotations

import dataclasses
from typing import Any, Self, overload

from derp.chorm.expression_base import ComparisonOperator, Expression, Params


@dataclasses.dataclass(frozen=True)
class Codec:
    """Column compression codec.

    Examples::

        Codec("ZSTD", 3)
        Codec("LZ4HC", 9)
        Codec("Delta", "ZSTD")   # codec pipeline
        Codec("DoubleDelta")
        Codec("Gorilla")
        Codec("T64", "ZSTD")
        Codec("NONE")
    """

    name: str
    args: tuple[Any, ...] = ()

    def __init__(self, name: str, *args: Any) -> None:
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "args", tuple(args))

    def to_sql(self) -> str:
        """Render a single codec clause (without the surrounding ``CODEC(...)``)."""
        if not self.args:
            return self.name
        rendered_args = ", ".join(_render_codec_arg(a) for a in self.args)
        return f"{self.name}({rendered_args})"


def _render_codec_arg(arg: Any) -> str:
    if isinstance(arg, Codec):
        return arg.to_sql()
    if isinstance(arg, str):
        # Treat bare strings as nested codec names (e.g., Delta("ZSTD"))
        return arg
    return str(arg)


class Fn:
    """Predefined SQL functions for use as column defaults."""

    @staticmethod
    def now() -> str:
        return "now()"

    @staticmethod
    def now64(precision: int = 3) -> str:
        return f"now64({precision})"

    @staticmethod
    def today() -> str:
        return "today()"

    @staticmethod
    def generate_uuidv4() -> str:
        return "generateUUIDv4()"

    @staticmethod
    def rand() -> str:
        return "rand()"

    @staticmethod
    def current_database() -> str:
        return "currentDatabase()"


class Materialized:
    """Marker for ``MATERIALIZED`` column expressions.

    Use as the value of ``Field(default=Materialized("expr"))`` to make
    the column auto-derived on insert (not user-supplied).
    """

    __slots__ = ("expression",)

    def __init__(self, expression: str) -> None:
        self.expression = expression


class Alias:
    """Marker for ``ALIAS`` columns (computed on read, not stored)."""

    __slots__ = ("expression",)

    def __init__(self, expression: str) -> None:
        self.expression = expression


class Ephemeral:
    """Marker for ``EPHEMERAL`` columns (insertable, not stored)."""

    __slots__ = ("default",)

    def __init__(self, default: Any = None) -> None:
        self.default = default


class FieldSpec:
    """Column constraints returned by :func:`Field`."""

    __slots__ = (
        "default",
        "codec",
        "ttl",
        "comment",
        "compression",
        "low_cardinality",
        "settings",
    )

    def __init__(
        self,
        *,
        default: Any = dataclasses.MISSING,
        codec: Codec | tuple[Codec, ...] | None = None,
        ttl: str | None = None,
        comment: str | None = None,
        settings: dict[str, Any] | None = None,
    ) -> None:
        self.default = default
        if isinstance(codec, Codec):
            codec = (codec,)
        self.codec = codec
        self.ttl = ttl
        self.comment = comment
        self.settings = settings


def Field(
    *,
    default: Any = dataclasses.MISSING,
    codec: Codec | tuple[Codec, ...] | None = None,
    ttl: str | None = None,
    comment: str | None = None,
    settings: dict[str, Any] | None = None,
) -> Any:
    """Declare a column with constraints.

    *default* may be a plain Python value (rendered as a literal),
    a string ending with ``()`` (rendered as a SQL function call),
    a :class:`Materialized` instance, an :class:`Alias`, or
    :class:`Ephemeral`.

    *codec* takes a single :class:`Codec` or a tuple to form a pipeline,
    e.g. ``(Codec("Delta"), Codec("ZSTD", 3))``.

    *ttl* is a TTL expression specific to this column (per-column TTL).

    *settings* are MergeTree column-level settings, rare but supported.
    """
    return FieldSpec(
        default=default,
        codec=codec,
        ttl=ttl,
        comment=comment,
        settings=settings,
    )


def _wrap_modifiers(base: str, *, nullable: bool, low_cardinality: bool) -> str:
    """Apply the ``Nullable``/``LowCardinality`` wrappers in ClickHouse's
    required nesting order: ``LowCardinality(Nullable(T))``.

    ClickHouse rejects ``Nullable(LowCardinality(T))`` — ``Nullable`` must be the
    inner wrapper — so this is the single source of truth for the order, shared by
    :meth:`Column.sql_type` and every parametrized type in ``column.types``.
    """
    if nullable:
        base = f"Nullable({base})"
    if low_cardinality:
        base = f"LowCardinality({base})"
    return base


class Column[T](Expression):
    """Base descriptor for all ClickHouse table columns.

    Mirrors :class:`derp.orm.column.base.Column`: extends Expression so a
    class-level reference (``MyTable.col``) can flow directly into the
    query builder.
    """

    _sql_type: str = ""
    _nullable: bool
    _low_cardinality: bool
    _default: Any
    _materialized: str | None
    _alias_expr: str | None
    _ephemeral: bool
    _ephemeral_default: Any
    _codec: tuple[Codec, ...] | None
    _ttl: str | None
    _comment: str | None
    _settings: dict[str, Any] | None
    _database_name: str | None
    _table_name: str | None
    _field_name: str | None

    def __init__(self, spec: FieldSpec) -> None:
        sa = object.__setattr__
        # Markers (set by Nullable[...]/LowCardinality[...]) are the single
        # source of truth for these flags, so a wrapped class renders correctly
        # however it is constructed — not only via the Table metaclass.
        cls = type(self)
        sa(self, "_nullable", bool(getattr(cls, "_nullable_marker", False)))
        sa(
            self,
            "_low_cardinality",
            bool(getattr(cls, "_low_cardinality_marker", False)),
        )
        sa(self, "_default", dataclasses.MISSING)
        sa(self, "_materialized", None)
        sa(self, "_alias_expr", None)
        sa(self, "_ephemeral", False)
        sa(self, "_ephemeral_default", None)
        sa(self, "_codec", spec.codec)
        sa(self, "_ttl", spec.ttl)
        sa(self, "_comment", spec.comment)
        sa(self, "_settings", spec.settings)
        sa(self, "_database_name", None)
        sa(self, "_table_name", None)
        sa(self, "_field_name", None)
        # Resolve default
        d = spec.default
        if isinstance(d, Materialized):
            sa(self, "_materialized", d.expression)
        elif isinstance(d, Alias):
            sa(self, "_alias_expr", d.expression)
        elif isinstance(d, Ephemeral):
            sa(self, "_ephemeral", True)
            sa(self, "_ephemeral_default", d.default)
        else:
            sa(self, "_default", d)

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

    # -- Metadata -------------------------------------------------------------

    @property
    def nullable(self) -> bool:
        return self._nullable

    @property
    def low_cardinality(self) -> bool:
        return self._low_cardinality

    @property
    def default(self) -> Any:
        return self._default if self._default is not dataclasses.MISSING else None

    @property
    def has_default(self) -> bool:
        return self._default is not dataclasses.MISSING

    @property
    def is_materialized(self) -> bool:
        return self._materialized is not None

    @property
    def is_alias(self) -> bool:
        return self._alias_expr is not None

    @property
    def is_ephemeral(self) -> bool:
        return self._ephemeral

    @property
    def codec(self) -> tuple[Codec, ...] | None:
        return self._codec

    @property
    def ttl(self) -> str | None:
        return self._ttl

    @property
    def comment(self) -> str | None:
        return self._comment

    def sql_type(self) -> str:
        """Return the ClickHouse type expression (e.g. ``Nullable(String)``)."""
        return _wrap_modifiers(
            self._sql_type,
            nullable=self._nullable,
            low_cardinality=self._low_cardinality,
        )

    # -- Expression interface -------------------------------------------------

    def to_sql(self, params: Params) -> str:
        if self._table_name and self._field_name:
            full_table = _full_table_name(self._database_name, self._table_name)
            table_ref = params.table_aliases.get(
                full_table,
                params.table_aliases.get(self._table_name, full_table),
            )
            return f"{_quote_table_ref(table_ref)}.{_quote_ident(self._field_name)}"
        if self._field_name:
            return _quote_ident(self._field_name)
        raise ValueError("Column missing table/field name metadata")

    # -- Unary negation -------------------------------------------------------

    def __invert__(self) -> Any:
        from derp.chorm.query.expressions import BinaryOp, to_expr

        return BinaryOp(self, ComparisonOperator.EQ, to_expr(False))

    # -- Aggregates / common analytics ----------------------------------------

    def count(self, *, distinct: bool = False) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("count", (self,), distinct=distinct)

    def count_distinct(self) -> Any:
        return self.count(distinct=True)

    def sum(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("sum", (self,))

    def avg(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("avg", (self,))

    def min(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("min", (self,))

    def max(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("max", (self,))

    def any(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("any", (self,))

    def any_last(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("anyLast", (self,))

    def uniq(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("uniq", (self,))

    def uniq_exact(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("uniqExact", (self,))

    def uniq_combined(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("uniqCombined", (self,))

    def uniq_hll12(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("uniqHLL12", (self,))

    def group_array(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("groupArray", (self,))

    def group_uniq_array(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("groupUniqArray", (self,))

    def quantile(self, level: float = 0.5) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("quantile", (self,), params=(level,))

    def quantiles(self, *levels: float) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("quantiles", (self,), params=levels)

    def median(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("median", (self,))

    def stddev_pop(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("stddevPop", (self,))

    def stddev_samp(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("stddevSamp", (self,))

    def var_pop(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("varPop", (self,))

    def var_samp(self) -> Any:
        from derp.chorm.query.expressions import AggregateFunc

        return AggregateFunc("varSamp", (self,))

    def argmin(self, other: Any) -> Any:
        from derp.chorm.query.expressions import AggregateFunc, to_expr

        return AggregateFunc("argMin", (self, to_expr(other)))

    def argmax(self, other: Any) -> Any:
        from derp.chorm.query.expressions import AggregateFunc, to_expr

        return AggregateFunc("argMax", (self, to_expr(other)))

    # -- CASE ----------------------------------------------------------------

    def case(self, mapping: dict[Any, Any], *, else_: Any | None = None) -> Any:
        from derp.chorm.query.expressions import CaseExpression

        return CaseExpression(self, list(mapping.items()), else_value=else_)


def _quote_ident(name: str) -> str:
    """Quote a ClickHouse identifier with backticks when needed.

    Backticks are always safe and idempotent.  Strip any existing
    backticks first to avoid double-quoting.
    """
    cleaned = name.replace("`", "")
    return f"`{cleaned}`"


def _quote_table_ref(name: str) -> str:
    return ".".join(_quote_ident(part) for part in name.split("."))


def _full_table_name(database: str | None, table: str) -> str:
    if database:
        return f"{database}.{table}"
    return table
