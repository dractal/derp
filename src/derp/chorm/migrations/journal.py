"""Migration journal — records which migrations have been applied.

ClickHouse has no transactions across DDL, so the journal lives in a
dedicated table that the journal manages itself.  Idempotent.
"""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from derp.chorm.engine import ClickHouseEngine


DEFAULT_JOURNAL_TABLE = "_derp_chorm_migrations"


class MigrationJournal:
    """Persists which migrations have run, in the target database."""

    def __init__(
        self,
        engine: ClickHouseEngine,
        *,
        table_name: str = DEFAULT_JOURNAL_TABLE,
        database: str | None = None,
    ) -> None:
        self._engine = engine
        self._table = f"{database}.{table_name}" if database else table_name

    async def ensure(self) -> None:
        """Create the journal table if missing.

        ``ReplacingMergeTree(applied_at)`` collapses duplicate ``name`` rows to
        the most recent apply, so a re-recorded migration does not accumulate
        duplicate journal entries.
        """
        await self._engine.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self._table}
            (
                `name` String,
                `hash` String,
                `applied_at` DateTime DEFAULT now(),
                `notes` String DEFAULT ''
            )
            ENGINE = ReplacingMergeTree(applied_at)
            ORDER BY name
            """
        )

    async def applied(self) -> set[str]:
        """Return the set of names already applied."""
        rows = await self._engine.fetch(
            f"SELECT name FROM {self._table} ORDER BY applied_at"
        )
        return {r["name"] for r in rows}

    async def applied_with_hashes(self) -> dict[str, str]:
        """Return a ``name -> recorded hash`` map for applied migrations."""
        rows = await self._engine.fetch(
            f"SELECT name, hash FROM {self._table} ORDER BY applied_at"
        )
        return {r["name"]: r.get("hash", "") for r in rows}

    async def record(
        self,
        name: str,
        *,
        hash: str = "",
        notes: str = "",
    ) -> None:
        """Mark a migration as applied."""
        await self._engine.client.insert(
            self._table,
            [
                [
                    name,
                    hash,
                    datetime.datetime.now(datetime.UTC).replace(tzinfo=None),
                    notes,
                ]
            ],
            column_names=["name", "hash", "applied_at", "notes"],
        )

    async def remove(self, name: str) -> None:
        """Forget an applied migration (used by rollback)."""
        await self._engine.command(
            f"DELETE FROM {self._table} WHERE name = {{name:String}}",
            parameters={"name": name},
        )

    @property
    def table_name(self) -> str:
        return self._table
