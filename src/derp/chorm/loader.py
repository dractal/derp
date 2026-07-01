"""Schema discovery for ClickHouse (chorm) tables."""

from __future__ import annotations

from derp._loader import deduplicate_tables, load_table_subclasses
from derp.chorm.table import Table


def load_tables(module_path: str) -> list[type[Table]]:
    """Load chorm Table subclasses from a file, directory, or glob pattern."""
    return load_table_subclasses(module_path, Table)


def discover_tables(schema_path: str) -> list[type[Table]]:
    """Load chorm tables from *schema_path* and collapse inheritance."""
    return deduplicate_tables(load_tables(schema_path), Table)
