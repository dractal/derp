"""Module loading utilities for Derp ORM."""

from __future__ import annotations

import importlib
import sys
from pathlib import Path

from derp.orm.table import Table


def _load_tables_from_file(path: Path) -> list[type[Table]]:
    """Load Table subclasses from a single Python file."""

    try:
        # Convert file path to dotted module name relative to CWD
        resolved = path.resolve()
        cwd = Path.cwd()
        try:
            relative = resolved.relative_to(cwd)
        except ValueError:
            # Absolute path outside CWD — use just the stem and add parent to sys.path
            relative = Path(resolved.stem + ".py")
            parent = str(resolved.parent)
            if parent not in sys.path:
                sys.path.insert(0, parent)

        module_name = ".".join(relative.with_suffix("").parts)

        # Ensure CWD is on sys.path so relative module names resolve
        cwd_str = str(cwd)
        if cwd_str not in sys.path:
            sys.path.insert(0, cwd_str)

        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Failed to import module at {path.name}.") from e

    # Find all Table subclasses
    tables: list[type[Table]] = []
    for name in dir(module):
        obj = getattr(module, name)
        if (
            isinstance(obj, type)
            and issubclass(obj, Table)
            and obj is not Table
            and hasattr(obj, "__columns__")
            and getattr(obj, "__explicit_table__", False)
        ):
            tables.append(obj)

    return tables


def load_tables(module_path: str) -> list[type[Table]]:
    """Load Table subclasses from a Python module, directory, or glob pattern.

    Args:
        module_path: One of:
            - Path to a Python file (e.g., "src/myapp/schema.py")
            - Path to a directory (loads all .py files recursively)
            - Glob pattern (e.g., "src/**/models.py", "src/**/*.py")

    Returns:
        List of Table subclasses found in matching modules
    """
    path = Path(module_path)
    tables: list[type[Table]] = []
    seen_tables: set[int] = set()  # Track by id to avoid duplicates

    # Check if it's a glob pattern
    if "*" in module_path or "?" in module_path or "[" in module_path:
        # Find the base directory (everything before the first glob character)
        parts = Path(module_path).parts
        base_parts: list[str] = []
        pattern_parts: list[str] = []
        found_glob = False

        for part in parts:
            if found_glob or "*" in part or "?" in part or "[" in part:
                found_glob = True
                pattern_parts.append(part)
            else:
                base_parts.append(part)

        base_dir = Path(*base_parts) if base_parts else Path(".")
        pattern = str(Path(*pattern_parts)) if pattern_parts else "**/*.py"

        if not base_dir.exists():
            raise FileNotFoundError(f"Base directory not found: {base_dir}")

        files = sorted(base_dir.glob(pattern))

    elif path.is_dir():
        # Directory: load all .py files recursively
        files = sorted(path.rglob("*.py"))

    elif path.is_file():
        # Single file
        files = [path]

    else:
        raise FileNotFoundError(f"Path not found: {module_path}")

    # Load tables from each file
    for file_path in files:
        if file_path.name.startswith("_"):
            continue  # Skip __init__.py, __pycache__, etc.

        try:
            for table in _load_tables_from_file(file_path):
                # Use qualified name to deduplicate
                key: int = id(table)
                if key not in seen_tables:
                    seen_tables.add(key)
                    tables.append(table)
        except Exception:
            # Skip files that fail to import (might not be valid modules)
            continue

    return tables


def _deduplicate_tables(tables: list[type[Table]]) -> list[type[Table]]:
    """Keep only the youngest table in each inheritance chain.

    If a table has a descendant in the list, drop it (the descendant
    inherits all its columns). If two tables share a common explicit-table
    ancestor but neither descends from the other, raise ValueError.
    """
    # Find tables that are ancestors of another table in the list
    to_remove: set[type[Table]] = set()
    for t in tables:
        for other in tables:
            if other is t:
                continue
            # If t is a proper ancestor of other, t is redundant
            if issubclass(other, t) and t is not Table:
                to_remove.add(t)

    result = [t for t in tables if t not in to_remove]

    # Check for branches: two result tables sharing a common explicit-table
    # ancestor (whether or not that ancestor was in the input list)
    for i, a in enumerate(result):
        for b in result[i + 1 :]:
            # Walk a's MRO looking for a shared explicit-table ancestor
            for base in type.mro(a):
                if (
                    base is a
                    or base is b
                    or base is Table
                    or not isinstance(base, type)
                ):
                    continue
                if (
                    getattr(base, "__explicit_table__", False)
                    and issubclass(a, base)
                    and issubclass(b, base)
                ):
                    raise ValueError(
                        f"Ambiguous table inheritance: {a.__name__} and "
                        f"{b.__name__} both extend {base.__name__}. "
                        f"Only one subclass per table hierarchy is allowed."
                    )

    return result


def discover_tables(
    schema_path: str,
    *,
    include_auth: bool = False,
) -> list[type[Table]]:
    """Load user tables and optionally inject framework tables, then dedup.

    Args:
        schema_path: Path/glob/directory for user schema files.
        include_auth: If True, inject AuthUser and AuthSession when no
            subclass is already present in the loaded tables.

    Returns:
        Deduplicated list of Table subclasses.
    """
    tables = load_tables(schema_path)

    if include_auth:
        from derp.auth.models import AuthSession, AuthUser

        if not any(issubclass(t, AuthUser) for t in tables):
            tables.append(AuthUser)
        if not any(issubclass(t, AuthSession) for t in tables):
            tables.append(AuthSession)

    return _deduplicate_tables(tables)
