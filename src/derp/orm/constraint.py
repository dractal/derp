"""Table-level constraint definitions for Derp ORM."""

from __future__ import annotations

import abc
from typing import Any

from derp.orm.index import _resolve_column_name


class Constraint(abc.ABC):
    """Base class for table-level constraints.

    Constraints are declared from a table's ``constraints()`` classmethod and
    are emitted into both ``CREATE TABLE`` DDL and the migration snapshot.
    Unlike an :class:`~derp.orm.index.Index`, a constraint is recorded in
    ``pg_constraint``, so it round-trips through live-database introspection.

    Subclasses supply a name and a DDL clause; the snapshot serializer
    dispatches on the concrete type to place them in the right section of a
    ``TableSnapshot``.
    """

    __slots__ = ("name",)

    name: str | None

    @abc.abstractmethod
    def auto_name(self, table_name: str) -> str:
        """Return the constraint name, defaulting to PostgreSQL's convention."""

    @abc.abstractmethod
    def to_ddl(self, table_name: str) -> str:
        """Return the ``CONSTRAINT ...`` clause for use inside ``CREATE TABLE``."""


class Unique(Constraint):
    """A multi-column ``UNIQUE`` table constraint.

    Declared from a table's ``constraints()`` classmethod::

        class Membership(Table, table="memberships"):
            org_id: UUID = Field()
            user_id: UUID = Field()

            @classmethod
            def constraints(cls):
                return [Unique(cls.org_id, cls.user_id)]

    This is distinct from ``Index(..., unique=True)``. Postgres backs a unique
    constraint with an index, but only a constraint appears in ``pg_constraint``
    — so a table whose uniqueness is declared as an index will never compare
    equal to a live database that has a constraint, and vice versa.

    Single-column uniqueness is better expressed as ``Field(unique=True)``;
    both spellings produce the same ``{table}_{column}_key`` constraint name.
    """

    __slots__ = ("_columns", "nulls_distinct")

    def __init__(
        self,
        *columns: str | Any,  # Any covers Column[T]
        name: str | None = None,
        nulls_distinct: bool = True,
    ) -> None:
        if not columns:
            raise ValueError("Unique() requires at least one column.")
        self._columns = tuple(_resolve_column_name(c) for c in columns)
        self.name = name
        self.nulls_distinct = nulls_distinct

    @property
    def columns(self) -> tuple[str, ...]:
        return self._columns

    def auto_name(self, table_name: str) -> str:
        if self.name:
            return self.name
        return f"{table_name}_{'_'.join(self._columns)}_key"

    def to_ddl(self, table_name: str) -> str:
        parts = [f"CONSTRAINT {self.auto_name(table_name)} UNIQUE"]
        if not self.nulls_distinct:
            parts.append("NULLS NOT DISTINCT")
        parts.append(f"({', '.join(self._columns)})")
        return " ".join(parts)

    def __repr__(self) -> str:
        return f"Unique({', '.join(self._columns)})"


class Check(Constraint):
    """A ``CHECK`` table constraint.

    Declared from a table's ``constraints()`` classmethod::

        class Order(Table, table="orders"):
            qty: Integer = Field()

            @classmethod
            def constraints(cls):
                return [Check("qty >= 0 AND qty <= 100")]

    Single-column predicates read better as ``Field(check=...)``, which names
    the constraint ``{table}_{column}_check``.

    The expression is raw SQL, stored verbatim. PostgreSQL rewrites it on the
    way in (``IN (...)`` becomes ``= ANY (ARRAY[...])``, casts are made
    explicit), so ``derp push`` canonicalizes the authored text against the
    live server before diffing rather than comparing it to what the database
    reports back.
    """

    __slots__ = ("expression",)

    def __init__(self, expression: str, *, name: str | None = None) -> None:
        if not expression or not expression.strip():
            raise ValueError("Check() requires a non-empty expression.")
        self.expression = expression.strip()
        self.name = name

    def auto_name(self, table_name: str) -> str:
        if self.name:
            return self.name
        return f"{table_name}_check"

    def to_ddl(self, table_name: str) -> str:
        return f"CONSTRAINT {self.auto_name(table_name)} CHECK ({self.expression})"

    def __repr__(self) -> str:
        return f"Check({self.expression!r})"
