"""Shared schema-discovery utilities for the ORM and ClickHouse loaders.

Both ``derp.orm`` and ``derp.chorm`` declare tables as subclasses of a
``Table`` base that carries ``__columns__`` and ``__explicit_table__``
markers.  The file-walking / import / dedup logic is identical for both;
only the base class differs, so it lives here parameterized by ``base``.
"""

from __future__ import annotations

import importlib
import sys
from pathlib import Path


def _is_explicit_table(obj: object, base: type) -> bool:
    return (
        isinstance(obj, type)
        and issubclass(obj, base)
        and obj is not base
        and hasattr(obj, "__columns__")
        and getattr(obj, "__explicit_table__", False)
    )


def _load_from_file[T](path: Path, base: type[T]) -> list[type[T]]:
    """Import a single Python file and return its explicit table subclasses."""
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

    cwd_str = str(cwd)
    if cwd_str not in sys.path:
        sys.path.insert(0, cwd_str)

    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        raise ImportError(f"Failed to import module at {path.name}.") from e

    return [
        getattr(module, name)
        for name in dir(module)
        if _is_explicit_table(getattr(module, name), base)
    ]


def load_table_subclasses[T](module_path: str, base: type[T]) -> list[type[T]]:
    """Load ``base`` subclasses from a Python file, directory, or glob pattern.

    Args:
        module_path: A file (``"app/schema.py"``), a directory (loads all
            ``.py`` recursively), or a glob (``"src/**/*.py"``).
        base: The table base class to filter for.

    Returns:
        Explicit-table subclasses found in matching modules, deduplicated
        by object identity (use :func:`deduplicate_tables` for inheritance
        collapsing).
    """
    path = Path(module_path)

    if "*" in module_path or "?" in module_path or "[" in module_path:
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
        files = sorted(path.rglob("*.py"))
    elif path.is_file():
        files = [path]
    else:
        raise FileNotFoundError(f"Path not found: {module_path}")

    tables: list[type[T]] = []
    seen: set[int] = set()
    for file_path in files:
        if file_path.name.startswith("_"):
            continue
        try:
            loaded = _load_from_file(file_path, base)
        except Exception:
            # Skip files that fail to import (might not be valid modules)
            continue
        for table in loaded:
            if id(table) not in seen:
                seen.add(id(table))
                tables.append(table)

    return tables


def deduplicate_tables[T](tables: list[type[T]], base: type[T]) -> list[type[T]]:
    """Keep only the youngest table in each inheritance chain.

    If a table has a descendant in the list, drop it (the descendant
    inherits all its columns). If two tables share a common explicit-table
    ancestor but neither descends from the other, raise ValueError.
    """
    to_remove: set[type[T]] = set()
    for t in tables:
        for other in tables:
            if other is t:
                continue
            if issubclass(other, t) and t is not base:
                to_remove.add(t)

    result = [t for t in tables if t not in to_remove]

    for i, a in enumerate(result):
        for b in result[i + 1 :]:
            for ancestor in type.mro(a):
                if (
                    ancestor is a
                    or ancestor is b
                    or ancestor is base
                    or not isinstance(ancestor, type)
                ):
                    continue
                if (
                    getattr(ancestor, "__explicit_table__", False)
                    and issubclass(a, ancestor)
                    and issubclass(b, ancestor)
                ):
                    raise ValueError(
                        f"Ambiguous table inheritance: {a.__name__} and "
                        f"{b.__name__} both extend {ancestor.__name__}. "
                        f"Only one subclass per table hierarchy is allowed."
                    )

    return result
