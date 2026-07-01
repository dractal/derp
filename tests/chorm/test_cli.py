"""Tests for the `derp ch ...` ClickHouse CLI commands."""

from __future__ import annotations

import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from derp.cli.main import app

runner = CliRunner()

_SCHEMA = """\
from derp.chorm import Field, MergeTree, String, Table, UInt64


class Event(Table, table="events"):
    id: UInt64 = Field()
    name: String = Field()
    __engine__ = MergeTree(order_by="id")
"""

_TOML = """\
[database]
db_url = "postgresql://unused"
schema_path = "ch_schema.py"

[clickhouse]
host = "localhost"
schema_path = "ch_schema.py"
migrations_dir = "ch_migrations"
"""


@pytest.fixture
def ch_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Generator[Path]:
    (tmp_path / "ch_schema.py").write_text(_SCHEMA)
    (tmp_path / "derp.toml").write_text(_TOML)
    monkeypatch.chdir(tmp_path)
    try:
        yield tmp_path
    finally:
        sys.modules.pop("ch_schema", None)


class FakeEngine:
    """Stub ClickHouseEngine recording commands and a fake journal table."""

    def __init__(self) -> None:
        self.commands: list[str] = []
        self.applied: set[str] = set()
        self.hashes: dict[str, str] = {}

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    async def command(
        self, sql: str, *, parameters: dict[str, Any] | None = None
    ) -> None:
        self.commands.append(sql)
        if sql.strip().upper().startswith("DELETE FROM") and parameters:
            name = parameters.get("name")
            if isinstance(name, str):
                self.applied.discard(name)

    async def fetch(self, sql: str, **_: Any) -> list[dict[str, Any]]:
        if "applied_at" in sql:
            return [
                {"name": n, "hash": self.hashes.get(n, "")}
                for n in sorted(self.applied)
            ]
        return []

    @property
    def client(self) -> FakeEngine:
        return self

    async def insert(
        self, table: str, data: list[list[Any]], *, column_names: Any = None
    ) -> None:
        for row in data:
            self.applied.add(row[0])
            self.hashes[row[0]] = row[1]


@pytest.fixture
def fake_engine(monkeypatch: pytest.MonkeyPatch) -> FakeEngine:
    eng = FakeEngine()
    monkeypatch.setattr(
        "derp.cli.commands.ch._common.ClickHouseEngine",
        lambda *a, **k: eng,
    )
    return eng


def _generate(name: str = "initial") -> Any:
    return runner.invoke(app, ["ch", "generate", "--name", name, "--force"])


# -- generate / check / drop (no DB) -----------------------------------------


def test_generate_creates_migration(ch_project: Path) -> None:
    result = _generate()
    assert result.exit_code == 0, result.output
    folder = ch_project / "ch_migrations" / "0000_initial"
    assert (folder / "migration.sql").exists()
    assert (folder / "down.sql").exists()
    assert (folder / "snapshot.json").exists()
    assert (ch_project / "ch_migrations" / "journal.json").exists()
    assert "CREATE TABLE" in (folder / "migration.sql").read_text().upper()


def test_generate_no_changes_second_time(ch_project: Path) -> None:
    assert _generate().exit_code == 0
    result = _generate()
    assert result.exit_code == 0
    assert "No changes" in result.output


def test_check_detects_drift_then_passes(ch_project: Path) -> None:
    drift = runner.invoke(app, ["ch", "check"])
    assert drift.exit_code == 1
    assert "drift" in drift.output.lower()

    _generate()
    ok = runner.invoke(app, ["ch", "check"])
    assert ok.exit_code == 0
    assert "up to date" in ok.output.lower()


def test_drop_removes_last_migration(ch_project: Path) -> None:
    _generate()
    result = runner.invoke(app, ["ch", "drop", "--force"])
    assert result.exit_code == 0
    assert not (ch_project / "ch_migrations" / "0000_initial").exists()


def test_missing_clickhouse_section(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    (tmp_path / "derp.toml").write_text(
        '[database]\ndb_url = "postgresql://unused"\nschema_path = "x.py"\n'
    )
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["ch", "check"])
    assert result.exit_code == 1
    assert "[clickhouse]" in result.output


# -- migrate / status / rollback (fake engine) -------------------------------


def test_migrate_applies_pending(ch_project: Path, fake_engine: FakeEngine) -> None:
    _generate()
    result = runner.invoke(app, ["ch", "migrate"])
    assert result.exit_code == 0, result.output
    assert "Applied 1" in result.output
    assert "0000" in fake_engine.applied
    assert any("CREATE TABLE" in c.upper() for c in fake_engine.commands)


def test_migrate_idempotent(ch_project: Path, fake_engine: FakeEngine) -> None:
    _generate()
    runner.invoke(app, ["ch", "migrate"])
    again = runner.invoke(app, ["ch", "migrate"])
    assert again.exit_code == 0
    assert "No pending" in again.output


def test_migrate_detects_edited_applied_migration(
    ch_project: Path, fake_engine: FakeEngine
) -> None:
    _generate()
    assert runner.invoke(app, ["ch", "migrate"]).exit_code == 0

    # Edit an already-applied migration on disk, then re-run migrate.
    mig = ch_project / "ch_migrations" / "0000_initial" / "migration.sql"
    mig.write_text(mig.read_text() + "\n-- tampered\n")

    result = runner.invoke(app, ["ch", "migrate"])
    assert result.exit_code == 1
    assert "modified" in result.output.lower()


def test_migrate_dry_run_applies_nothing(
    ch_project: Path, fake_engine: FakeEngine
) -> None:
    _generate()
    result = runner.invoke(app, ["ch", "migrate", "--dry-run"])
    assert result.exit_code == 0
    assert fake_engine.applied == set()
    assert "Dry run" in result.output


def test_status_reports_applied_and_pending(
    ch_project: Path, fake_engine: FakeEngine
) -> None:
    _generate()
    before = runner.invoke(app, ["ch", "status"])
    assert "[pending] 0000" in before.output

    runner.invoke(app, ["ch", "migrate"])
    after = runner.invoke(app, ["ch", "status"])
    assert "[applied] 0000" in after.output


def test_rollback_reverts_last(ch_project: Path, fake_engine: FakeEngine) -> None:
    _generate()
    runner.invoke(app, ["ch", "migrate"])
    assert "0000" in fake_engine.applied

    result = runner.invoke(app, ["ch", "rollback", "--force"])
    assert result.exit_code == 0, result.output
    assert "0000" not in fake_engine.applied
    assert any("DROP TABLE" in c.upper() for c in fake_engine.commands)


# -- push / pull (fake engine + introspect) ----------------------------------


def test_push_applies_diff(ch_project: Path, fake_engine: FakeEngine) -> None:
    result = runner.invoke(app, ["ch", "push"])
    assert result.exit_code == 0, result.output
    assert any("CREATE TABLE" in c.upper() for c in fake_engine.commands)


def test_pull_writes_snapshot(ch_project: Path, fake_engine: FakeEngine) -> None:
    out = ch_project / "snap.json"
    result = runner.invoke(app, ["ch", "pull", "--out", str(out)])
    assert result.exit_code == 0, result.output
    assert out.exists()
    assert '"tables"' in out.read_text()
