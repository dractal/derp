"""Shared fixtures for chorm integration tests.

``ch_engine`` yields a :class:`ClickHouseEngine` wired to an in-process chdb
session (embedded ClickHouse), so generated SQL is validated against a real
ClickHouse engine instead of a string recorder. Tests using it are skipped
when chdb is not installed.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest

from derp.chorm import ClickHouseEngine


class _ChdbResult:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def named_results(self):
        return iter(self._rows)


def _render_value(v: Any) -> str:
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "1" if v else "0"
    if isinstance(v, int | float):
        return str(v)
    return "'" + str(v).replace("\\", "\\\\").replace("'", "\\'") + "'"


class ChdbClient:
    """Real-ClickHouse test double backed by an in-process chdb session.

    Implements the slice of the ``clickhouse_connect`` AsyncClient surface the
    engine actually calls (``query``/``command``/``insert``/``close``). The
    blocking chdb calls are wrapped in :func:`asyncio.to_thread`, matching how
    the engine must drive any synchronous driver.
    """

    def __init__(self) -> None:
        from chdb import session as chsession

        self._session = chsession.Session()

    async def query(
        self, sql: str, *, parameters: dict[str, Any] | None = None, **_: Any
    ) -> _ChdbResult:
        def run() -> list[dict[str, Any]]:
            res = self._session.query(sql, "JSON", params=parameters or {})
            text = str(res).strip()
            return json.loads(text)["data"] if text else []

        return _ChdbResult(await asyncio.to_thread(run))

    async def command(
        self, sql: str, *, parameters: dict[str, Any] | None = None, **_: Any
    ) -> str:
        await asyncio.to_thread(
            self._session.query, sql, "CSV", params=parameters or {}
        )
        return "ok"

    async def insert(
        self,
        table: str,
        data: list[list[Any]],
        *,
        column_names: list[str] | None = None,
    ) -> str:
        cols = f" ({', '.join(column_names)})" if column_names else ""
        values = ", ".join(
            "(" + ", ".join(_render_value(v) for v in row) + ")" for row in data
        )
        await self.command(f"INSERT INTO {table}{cols} VALUES {values}")
        return "ok"

    async def close(self) -> None:
        await asyncio.to_thread(self._session.close)


@pytest.fixture
async def ch_engine() -> AsyncIterator[ClickHouseEngine]:
    pytest.importorskip("chdb")
    engine = ClickHouseEngine(host="localhost")
    engine._client = ChdbClient()  # ty: ignore[invalid-assignment]
    try:
        yield engine
    finally:
        await engine.disconnect()
