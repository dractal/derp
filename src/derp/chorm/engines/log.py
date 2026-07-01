"""Log family table engines.

Log-family engines have no parameters and no trailing clauses.
"""

from __future__ import annotations

from derp.chorm.engines.base import TableEngine


class _BareEngine(TableEngine):
    _name: str = ""

    def engine_clause(self) -> str:
        return f"ENGINE = {self._name}"


class TinyLog(_BareEngine):
    """``TinyLog`` — minimal log engine for small tables."""

    _name = "TinyLog"


class Log(_BareEngine):
    """``Log`` — log engine with mark files for parallel reads."""

    _name = "Log"


class StripeLog(_BareEngine):
    """``StripeLog`` — single-block log engine."""

    _name = "StripeLog"
