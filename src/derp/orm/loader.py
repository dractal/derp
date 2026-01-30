"""Module loading utilities for Derp ORM."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from derp.orm.table import Table


def _load_tables_from_file(path: Path) -> list[type[Table]]:
    """Load Table subclasses from a single Python file."""
    # Use unique module name to avoid conflicts
    module_name = f"derp_schema_{path.stem}_{id(path)}"

    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from {path}")

    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)

    # Find all Table subclasses
    tables: list[type[Table]] = []
    for name in dir(module):
        obj = getattr(module, name)
        if (
            isinstance(obj, type)
            and issubclass(obj, Table)
            and obj is not Table
            and hasattr(obj, "__columns__")
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
    seen_tables: set[str] = set()  # Track by qualified name to avoid duplicates

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
                key = f"{table.__module__}.{table.__name__}"
                if key not in seen_tables:
                    seen_tables.add(key)
                    tables.append(table)
        except Exception:
            # Skip files that fail to import (might not be valid modules)
            continue

    return tables
