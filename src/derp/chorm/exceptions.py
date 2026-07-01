"""Typed exceptions for the ClickHouse ORM.

Rooted at :class:`ChormError`, mirroring the ``auth`` and ``storage`` modules,
so callers can catch chorm failures without importing the underlying
``clickhouse_connect`` driver.
"""

from __future__ import annotations


class ChormError(Exception):
    """Base exception for all chorm errors."""


class ChormNotConnectedError(ChormError):
    """A query/command was issued before ``connect()`` or with no engine bound."""


class ChormBackendError(ChormError):
    """An underlying ClickHouse driver error, wrapped and chained via ``from``."""


class NoRowsError(ChormError):
    """A single-row accessor (``first()``) found no rows."""
