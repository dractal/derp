"""ClickHouse data-skipping indexes and projections."""

from __future__ import annotations

import dataclasses
from enum import StrEnum
from typing import Any


class IndexType(StrEnum):
    """ClickHouse data-skipping index types."""

    MINMAX = "minmax"
    SET = "set"
    BLOOM_FILTER = "bloom_filter"
    NGRAM_BF = "ngrambf_v1"
    TOKEN_BF = "tokenbf_v1"
    HYPOTHESIS = "hypothesis"


@dataclasses.dataclass
class Index:
    """A ``INDEX name expr TYPE type GRANULARITY n`` clause.

    Example::

        Index(
            name="users_idx",
            expression="user_id",
            type=IndexType.BLOOM_FILTER,
            granularity=4,
        )
    """

    name: str
    expression: str
    type: IndexType | str
    type_args: tuple[Any, ...] = ()
    granularity: int = 1

    def to_ddl(self) -> str:
        """Render the inline index clause for a CREATE TABLE statement."""
        if self.type_args:
            args = ", ".join(_render(a) for a in self.type_args)
            t = f"{self.type}({args})"
        else:
            t = f"{self.type}"
            if self.type in (IndexType.SET, "set"):
                # Default set(0) when omitted
                t = "set(0)"
        return (
            f"INDEX {self.name} {self.expression} "
            f"TYPE {t} GRANULARITY {self.granularity}"
        )


@dataclasses.dataclass
class Projection:
    """A table projection (CREATE TABLE ... PROJECTION name (SELECT ...))."""

    name: str
    select: str
    order_by: str | None = None

    def to_ddl(self) -> str:
        body = f"SELECT {self.select}"
        if self.order_by:
            body += f" ORDER BY {self.order_by}"
        return f"PROJECTION {self.name} ({body})"


def _render(v: Any) -> str:
    if isinstance(v, str):
        return f"'{v}'" if " " in v else v
    return str(v)
