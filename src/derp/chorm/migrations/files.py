"""On-disk layout for file-based ClickHouse migrations.

Mirrors the Postgres migration folder structure::

    ch_migrations/
      0000_initial/
        migration.sql      # forward DDL
        down.sql           # best-effort reverse DDL
        snapshot.json      # SchemaSnapshot at this revision
      journal.json         # ordered ledger of revisions

The on-disk ``journal.json`` tracks revision *order* (for diffing the
latest snapshot and ordering applies); which revisions have actually
been applied to a live server is tracked separately in the
``_derp_chorm_migrations`` table via
:class:`derp.chorm.migrations.journal.MigrationJournal`.
"""

from __future__ import annotations

import dataclasses
import json
import re
import time
from pathlib import Path

from derp.chorm.migrations.snapshot import SchemaSnapshot
from derp.chorm.migrations.statements import Statement

UP_FILE = "migration.sql"
DOWN_FILE = "down.sql"
SNAPSHOT_FILE = "snapshot.json"
JOURNAL_FILE = "journal.json"
DIALECT = "clickhouse"

#: Separator between rendered statements in a ``.sql`` file. ClickHouse
#: executes one statement per request, so we split on this marker rather
#: than parsing semicolons (which may appear inside string literals).
STATEMENT_SPLIT = "\n-- derp:statement-split\n"

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def slugify(name: str) -> str:
    """Normalize a migration name into a filesystem-safe slug."""
    slug = _SLUG_RE.sub("_", name.strip().lower()).strip("_")
    return slug or "migration"


def render_sql(statements: list[Statement]) -> str:
    """Render statements into a single reviewable ``.sql`` body."""
    return STATEMENT_SPLIT.join(s.to_sql() for s in statements)


def split_sql(text: str) -> list[str]:
    """Split a ``.sql`` body back into individual statements."""
    return [chunk.strip() for chunk in text.split(STATEMENT_SPLIT) if chunk.strip()]


@dataclasses.dataclass
class JournalEntry:
    idx: int
    version: str
    name: str
    when: int

    @property
    def dirname(self) -> str:
        return f"{self.version}_{self.name}"


class FileJournal:
    """The ordered on-disk ledger (``journal.json``)."""

    def __init__(self, entries: list[JournalEntry] | None = None) -> None:
        self.entries: list[JournalEntry] = entries or []

    @classmethod
    def load(cls, migrations_dir: Path) -> FileJournal:
        path = migrations_dir / JOURNAL_FILE
        if not path.exists():
            return cls()
        data = json.loads(path.read_text())
        return cls(
            [
                JournalEntry(
                    idx=e["idx"],
                    version=e["version"],
                    name=e["name"],
                    when=e["when"],
                )
                for e in data.get("entries", [])
            ]
        )

    def save(self, migrations_dir: Path) -> None:
        migrations_dir.mkdir(parents=True, exist_ok=True)
        path = migrations_dir / JOURNAL_FILE
        data = {
            "dialect": DIALECT,
            "entries": [dataclasses.asdict(e) for e in self.entries],
        }
        path.write_text(json.dumps(data, indent=2) + "\n")

    def next_version(self) -> str:
        return f"{len(self.entries):04d}"

    def latest(self) -> JournalEntry | None:
        return self.entries[-1] if self.entries else None

    def append(self, name: str) -> JournalEntry:
        entry = JournalEntry(
            idx=len(self.entries),
            version=self.next_version(),
            name=name,
            when=int(time.time()),
        )
        self.entries.append(entry)
        return entry


def write_migration(
    migrations_dir: Path,
    entry: JournalEntry,
    *,
    up_sql: str,
    down_sql: str,
    snapshot: SchemaSnapshot,
) -> Path:
    """Write a migration folder (up/down/snapshot) and return its path."""
    folder = migrations_dir / entry.dirname
    folder.mkdir(parents=True, exist_ok=True)
    (folder / UP_FILE).write_text(up_sql + "\n" if up_sql else "")
    (folder / DOWN_FILE).write_text(down_sql + "\n" if down_sql else "")
    (folder / SNAPSHOT_FILE).write_text(snapshot.to_json() + "\n")
    return folder


def read_snapshot(folder: Path) -> SchemaSnapshot:
    """Read the snapshot stored in a migration folder."""
    return SchemaSnapshot.from_json((folder / SNAPSHOT_FILE).read_text())


def read_latest_snapshot(migrations_dir: Path, journal: FileJournal) -> SchemaSnapshot:
    """Snapshot of the most recent revision, or an empty one if none."""
    latest = journal.latest()
    if latest is None:
        return SchemaSnapshot()
    return read_snapshot(migrations_dir / latest.dirname)


def read_up_sql(migrations_dir: Path, entry: JournalEntry) -> str:
    return (migrations_dir / entry.dirname / UP_FILE).read_text()


def read_down_sql(migrations_dir: Path, entry: JournalEntry) -> str:
    return (migrations_dir / entry.dirname / DOWN_FILE).read_text()
