"""Migration journal management for tracking applied migrations.

The journal is a JSON file that tracks all migrations and their order,
matching Drizzle's _journal.json format.
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field


class JournalEntry(BaseModel):
    """Single migration entry in the journal."""

    idx: int  # Sequential index
    version: str  # Migration version/folder name (e.g., "0000", "0001")
    tag: str  # Human-readable migration name
    when: int  # Unix timestamp when migration was created
    breakpoints: bool = False  # Whether migration has breakpoints

    class ConfigDict:
        populate_by_name = True


class MigrationJournal(BaseModel):
    """Migration journal tracking all migrations.

    This matches Drizzle's _journal.json format.
    """

    version: str = "1"
    dialect: Literal["postgresql"] = "postgresql"
    entries: list[JournalEntry] = Field(default_factory=list)

    def get_latest_idx(self) -> int:
        """Get the latest migration index, or -1 if no migrations."""
        if not self.entries:
            return -1
        return self.entries[-1].idx

    def get_latest_version(self) -> str | None:
        """Get the latest migration version, or None if no migrations."""
        if not self.entries:
            return None
        return self.entries[-1].version

    def add_entry(
        self,
        version: str,
        tag: str,
        breakpoints: bool = False,
    ) -> JournalEntry:
        """Add a new migration entry to the journal.

        Args:
            version: Migration version/folder name
            tag: Human-readable migration name
            breakpoints: Whether the migration has SQL breakpoints

        Returns:
            The created JournalEntry
        """
        entry = JournalEntry(
            idx=self.get_latest_idx() + 1,
            version=version,
            tag=tag,
            when=int(datetime.now().timestamp()),
            breakpoints=breakpoints,
        )
        self.entries.append(entry)
        return entry

    def get_entry(self, version: str) -> JournalEntry | None:
        """Get a journal entry by version."""
        for entry in self.entries:
            if entry.version == version:
                return entry
        return None

    def get_pending_entries(self, applied_versions: set[str]) -> list[JournalEntry]:
        """Get all entries that haven't been applied yet.

        Args:
            applied_versions: Set of versions that have been applied

        Returns:
            List of pending journal entries in order
        """
        return [e for e in self.entries if e.version not in applied_versions]

    def remove_entry(self, version: str) -> bool:
        """Remove a migration entry from the journal.

        Args:
            version: Version to remove

        Returns:
            True if entry was found and removed
        """
        for i, entry in enumerate(self.entries):
            if entry.version == version:
                self.entries.pop(i)
                # Re-index remaining entries
                for j, e in enumerate(self.entries):
                    if e.idx > entry.idx:
                        self.entries[j] = e.model_copy(update={"idx": e.idx - 1})
                return True
        return False


JOURNAL_FILENAME = "_journal.json"


def load_journal(migrations_dir: Path) -> MigrationJournal:
    """Load the migration journal from disk.

    Args:
        migrations_dir: Path to migrations directory

    Returns:
        MigrationJournal (empty if file doesn't exist)
    """
    journal_path = migrations_dir / JOURNAL_FILENAME

    if not journal_path.exists():
        return MigrationJournal()

    with open(journal_path) as f:
        data = json.load(f)

    return MigrationJournal.model_validate(data)


def save_journal(journal: MigrationJournal, migrations_dir: Path) -> None:
    """Save the migration journal to disk.

    Args:
        journal: The journal to save
        migrations_dir: Path to migrations directory
    """
    migrations_dir.mkdir(parents=True, exist_ok=True)
    journal_path = migrations_dir / JOURNAL_FILENAME

    with open(journal_path, "w") as f:
        json.dump(
            journal.model_dump(mode="json"),
            f,
            indent=2,
        )


def get_next_version(journal: MigrationJournal) -> str:
    """Generate the next migration version string.

    Args:
        journal: Current journal

    Returns:
        Next version string (e.g., "0000", "0001")
    """
    latest = journal.get_latest_idx()
    return f"{latest + 1:04d}"


def load_snapshot(migrations_dir: Path, version: str) -> dict | None:
    """Load a snapshot JSON file for a specific migration version.

    Args:
        migrations_dir: Path to migrations directory
        version: Migration version

    Returns:
        Snapshot dict or None if not found
    """
    # Find the migration folder
    for entry in migrations_dir.iterdir():
        if entry.is_dir() and entry.name.startswith(version):
            snapshot_path = entry / "snapshot.json"
            if snapshot_path.exists():
                with open(snapshot_path) as f:
                    return json.load(f)
    return None


def load_latest_snapshot(
    migrations_dir: Path, journal: MigrationJournal
) -> dict | None:
    """Load the most recent snapshot.

    Args:
        migrations_dir: Path to migrations directory
        journal: Current journal

    Returns:
        Latest snapshot dict or None if no migrations
    """
    latest_version = journal.get_latest_version()
    if latest_version is None:
        return None
    return load_snapshot(migrations_dir, latest_version)


def save_snapshot(
    migrations_dir: Path,
    version: str,
    tag: str,
    snapshot: dict,
) -> Path:
    """Save a snapshot to the migration folder.

    Args:
        migrations_dir: Path to migrations directory
        version: Migration version
        tag: Migration tag/name
        snapshot: Snapshot dict to save

    Returns:
        Path to the created migration folder
    """
    # Create folder name like "0001_add_users_table"
    safe_tag = "".join(c if c.isalnum() or c == "_" else "_" for c in tag.lower())
    folder_name = f"{version}_{safe_tag}"
    folder_path = migrations_dir / folder_name

    folder_path.mkdir(parents=True, exist_ok=True)

    snapshot_path = folder_path / "snapshot.json"
    with open(snapshot_path, "w") as f:
        json.dump(snapshot, f, indent=2)

    return folder_path


def save_migration_sql(
    migration_folder: Path,
    sql: str,
    filename: str = "migration.sql",
) -> Path:
    """Save migration SQL to the migration folder.

    Args:
        migration_folder: Path to migration folder
        sql: SQL content
        filename: SQL file name

    Returns:
        Path to the SQL file
    """
    sql_path = migration_folder / filename
    with open(sql_path, "w") as f:
        f.write(sql)
    return sql_path


def get_migration_folders(migrations_dir: Path) -> list[tuple[str, Path]]:
    """Get all migration folders sorted by version.

    Args:
        migrations_dir: Path to migrations directory

    Returns:
        List of (version, path) tuples sorted by version
    """
    if not migrations_dir.exists():
        return []

    folders: list[tuple[str, Path]] = []
    for entry in migrations_dir.iterdir():
        if entry.is_dir() and entry.name[0].isdigit():
            # Extract version from folder name (e.g., "0001" from "0001_add_users")
            version = entry.name.split("_")[0]
            folders.append((version, entry))

    return sorted(folders, key=lambda x: x[0])


def get_migration_sql(migration_folder: Path) -> str | None:
    """Read the migration SQL from a migration folder.

    Args:
        migration_folder: Path to migration folder

    Returns:
        SQL content or None if not found
    """
    sql_path = migration_folder / "migration.sql"
    if sql_path.exists():
        return sql_path.read_text()
    return None
