"""Errors raised while turning a schema into migration statements."""

from __future__ import annotations


class SchemaError(ValueError):
    """The schema, as written, cannot be turned into a migration.

    Raised for mistakes in the user's ``Table`` definitions — a duplicate
    constraint name, a collation change that PostgreSQL cannot apply in
    place — as opposed to bugs in derp. The CLI catches this and prints the
    message, rather than a traceback.

    Subclasses ``ValueError`` so existing ``except ValueError`` handlers keep
    working, while giving the CLI something precise to catch: pydantic's
    ``ValidationError`` is also a ``ValueError``, and swallowing that would
    hide real defects.
    """
