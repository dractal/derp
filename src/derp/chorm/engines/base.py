"""Base class for ClickHouse table engines."""

from __future__ import annotations

import abc
from typing import Any


class TableEngine(abc.ABC):
    """Base class for ClickHouse table engine specifications.

    A ``TableEngine`` knows how to render the ``ENGINE = ...`` clause
    plus the engine-specific table options that follow (``ORDER BY``,
    ``PARTITION BY``, ``SAMPLE BY``, ``TTL``, ``SETTINGS``, etc.).
    """

    @abc.abstractmethod
    def engine_clause(self) -> str:
        """Render ``ENGINE = Name(args)``."""

    def order_by_clause(self) -> str | None:  # pragma: no cover - default
        return None

    def partition_by_clause(self) -> str | None:  # pragma: no cover - default
        return None

    def primary_key_clause(self) -> str | None:  # pragma: no cover - default
        return None

    def sample_by_clause(self) -> str | None:  # pragma: no cover - default
        return None

    def ttl_clause(self) -> str | None:  # pragma: no cover - default
        return None

    def settings_clause(self) -> str | None:  # pragma: no cover - default
        return None


def _render_arg(arg: Any) -> str:
    """Render an engine argument as SQL.

    - ``str`` → unquoted identifier or function expression.  Wrap with
      ``"'..."`` quotes by passing a ``_Quoted`` instance.
    - ``int``/``float`` → numeric literal.
    - ``_Quoted`` → ``'...'``.
    """
    if isinstance(arg, _Quoted):
        escaped = arg.value.replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(arg, bool):
        return "1" if arg else "0"
    if isinstance(arg, int | float):
        return str(arg)
    return str(arg)


class _Quoted:
    """Wrapper indicating a string argument should be SQL-quoted."""

    __slots__ = ("value",)

    def __init__(self, value: str) -> None:
        self.value = value


def quoted(value: str) -> _Quoted:
    """Mark a string argument as a SQL string literal."""
    return _Quoted(value)
