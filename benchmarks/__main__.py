"""Unified benchmark runner.

Discovers and runs all ``bench_*.py`` files in the benchmarks package.

Usage::

    uv run python -m benchmarks
    uv run python -m benchmarks --database-url postgresql://...
"""

from __future__ import annotations

import argparse
import importlib
import pkgutil
import sys


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all derp benchmarks")
    parser.add_argument("--database-url", type=str, default=None)
    args = parser.parse_args()

    # Discover bench_* modules in this package
    package = importlib.import_module("benchmarks")
    modules: list[str] = []
    for info in pkgutil.iter_modules(package.__path__):
        if info.name.startswith("bench_"):
            modules.append(info.name)

    modules.sort()

    for name in modules:
        print(f"\n{'━' * 80}")
        print(f"  Running benchmarks.{name}")
        print(f"{'━' * 80}\n")

        try:
            mod = importlib.import_module(f"benchmarks.{name}")
        except SystemExit:
            # ImportError guards in optional benchmarks raise
            # SystemExit(1) — skip gracefully.
            print("  Skipped (missing dependency)\n")
            continue

        main_fn = getattr(mod, "main", None)
        if main_fn is None:
            print("  No main() found, skipping\n")
            continue

        # Patch sys.argv so each sub-benchmark sees --database-url
        saved_argv = sys.argv
        if args.database_url:
            sys.argv = [
                name,
                "--database-url",
                args.database_url,
            ]
        else:
            sys.argv = [name]

        try:
            main_fn()
        except KeyboardInterrupt:
            print("\n\nInterrupted.")
            raise
        except Exception as e:
            print(f"  Error: {e}\n")
        finally:
            sys.argv = saved_argv


if __name__ == "__main__":
    main()
