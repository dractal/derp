"""Dialect-specific snapshot normalization.

Normalizes SchemaSnapshot objects so that semantically identical schemas
produced by different paths (serializer vs introspector) compare as equal.
This module is the single place where dialect-specific type equivalences live.
"""

from __future__ import annotations

import abc
import re

from derp.orm.migrations.snapshot.models import (
    ColumnSnapshot,
    ForeignKeySnapshot,
    PrimaryKeySnapshot,
    SchemaSnapshot,
    TableSnapshot,
    UniqueConstraintSnapshot,
)

# PostgreSQL type aliases → canonical forms (what format_type() returns).
_PG_SIMPLE_TYPE_MAP: dict[str, str] = {
    "serial": "integer",
    "bigserial": "bigint",
    "smallserial": "smallint",
    "int": "integer",
    "int4": "integer",
    "int8": "bigint",
    "int2": "smallint",
    "float4": "real",
    "float8": "double precision",
    "bool": "boolean",
    "timestamp": "timestamp without time zone",
    "timestamptz": "timestamp with time zone",
    "time": "time without time zone",
    "timetz": "time with time zone",
}

# Parameterized type prefixes: varchar(n) → character varying(n)
_PG_PARAMETERIZED_TYPE_MAP: dict[str, str] = {
    "varchar": "character varying",
    "char": "character",
}

_SERIAL_TYPES = frozenset({"serial", "bigserial", "smallserial"})
_NEXTVAL_RE = re.compile(r"nextval\(.+::regclass\)")
# Matches a trailing ::type cast, e.g. 'default'::character varying
_TYPE_CAST_SUFFIX_RE = re.compile(r"^(.+?)::[\w\s]+$")


class SnapshotNormalizer(abc.ABC):
    """Abstract base for dialect-specific snapshot normalization."""

    @abc.abstractmethod
    def normalize(self, snapshot: SchemaSnapshot) -> SchemaSnapshot:
        """Return a new SchemaSnapshot with canonicalized representations."""


class PostgresNormalizer(SnapshotNormalizer):
    """PostgreSQL normalization rules.

    Handles type aliases, serial-implied defaults/sequences,
    PK names, and constraint name re-keying.
    """

    # ------------------------------------------------------------------
    # Public helpers (also used directly in tests)
    # ------------------------------------------------------------------

    def normalize_type(self, type_str: str) -> str:
        """Canonicalize a PostgreSQL type string."""
        t = type_str.strip().lower()

        # Parameterized: varchar(n) → character varying(n)
        for short, long in _PG_PARAMETERIZED_TYPE_MAP.items():
            pattern = rf"^{re.escape(short)}\s*(\(.+\))$"
            m = re.match(pattern, t)
            if m:
                return f"{long}{m.group(1)}"

        # Simple alias lookup
        return _PG_SIMPLE_TYPE_MAP.get(t, t)

    def normalize_column(self, col: ColumnSnapshot) -> ColumnSnapshot:
        """Normalize a single column's type and default."""
        original_type = col.type.strip().lower()
        canonical_type = self.normalize_type(original_type)

        default = col.default

        # Serial columns: serializer has default=None, introspector has
        # default=nextval(...).  Normalize both to None.
        if original_type in _SERIAL_TYPES:
            default = None
        elif default is not None and _NEXTVAL_RE.match(default):
            default = None

        # Normalize remaining defaults
        if default is not None:
            default = self._normalize_default(default)

        return col.model_copy(update={"type": canonical_type, "default": default})

    @staticmethod
    def _normalize_default(default: str) -> str:
        """Canonicalize a default value expression.

        - Case-normalize boolean literals: TRUE/FALSE → true/false
        - Strip trailing ::type casts added by pg_get_expr
        """
        d = default.strip()

        # Strip type cast suffix: 'value'::character varying → 'value'
        m = _TYPE_CAST_SUFFIX_RE.match(d)
        if m:
            d = m.group(1).strip()

        # Case-normalize boolean literals
        if d.upper() in ("TRUE", "FALSE"):
            d = d.lower()

        return d

    # ------------------------------------------------------------------
    # Full snapshot normalization
    # ------------------------------------------------------------------

    def normalize(self, snapshot: SchemaSnapshot) -> SchemaSnapshot:
        # Collect all sequence names referenced by columns in the snapshot
        referenced_seqs: set[str] = set()
        for table in snapshot.tables.values():
            for col in table.columns.values():
                if col.default and _NEXTVAL_RE.match(col.default):
                    m = re.search(r"nextval\('([^']+)'", col.default)
                    if m:
                        referenced_seqs.add(m.group(1))

        tables = {
            key: self._normalize_table(table) for key, table in snapshot.tables.items()
        }

        # Keep only sequences that are:
        # 1. NOT serial-implied (referenced by a column's nextval default)
        # 2. AND actually relevant to the schema (not orphaned from excluded tables)
        #
        # The serializer never emits sequences, so any introspected sequence
        # that backs a serial column or belongs to an excluded table would
        # cause a phantom DROP.  Filter both cases by only keeping sequences
        # that exist in the serializer's output (empty) or that are explicitly
        # defined and not serial-implied.
        sequences = {
            key: seq
            for key, seq in snapshot.sequences.items()
            if seq.name not in referenced_seqs
            and not self._is_orphan_sequence(seq.name, snapshot)
        }

        return snapshot.model_copy(update={"tables": tables, "sequences": sequences})

    @staticmethod
    def _is_orphan_sequence(seq_name: str, snapshot: SchemaSnapshot) -> bool:
        """Check if a sequence is not referenced by any table in the snapshot.

        Orphan sequences typically belong to excluded tables (e.g.
        derp_migrations) and should be filtered to avoid phantom diffs.
        """
        for table in snapshot.tables.values():
            for col in table.columns.values():
                if col.default and seq_name in col.default:
                    return False
        return True

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _normalize_table(self, table: TableSnapshot) -> TableSnapshot:
        columns = {
            name: self.normalize_column(col) for name, col in table.columns.items()
        }

        pk = table.primary_key
        if pk is not None:
            pk = PrimaryKeySnapshot(name=None, columns=pk.columns)

        ucs = self._rekey_unique_constraints(table.unique_constraints)
        fks = self._rekey_foreign_keys(table.foreign_keys)

        return table.model_copy(
            update={
                "columns": columns,
                "primary_key": pk,
                "unique_constraints": ucs,
                "foreign_keys": fks,
            }
        )

    @staticmethod
    def _structural_uc_key(uc: UniqueConstraintSnapshot) -> str:
        """Deterministic key from columns, independent of constraint name."""
        return ",".join(sorted(uc.columns))

    @staticmethod
    def _structural_fk_key(fk: ForeignKeySnapshot) -> str:
        """Deterministic key from columns + reference, independent of name."""
        cols = ",".join(fk.columns)
        refs = ",".join(fk.references_columns)
        return f"{cols}->{fk.references_schema}.{fk.references_table}({refs})"

    def _rekey_unique_constraints(
        self, ucs: dict[str, UniqueConstraintSnapshot]
    ) -> dict[str, UniqueConstraintSnapshot]:
        return {self._structural_uc_key(uc): uc for uc in ucs.values()}

    def _rekey_foreign_keys(
        self, fks: dict[str, ForeignKeySnapshot]
    ) -> dict[str, ForeignKeySnapshot]:
        return {self._structural_fk_key(fk): fk for fk in fks.values()}


def get_normalizer(dialect: str) -> SnapshotNormalizer:
    """Factory: return the normalizer for the given dialect."""
    if dialect == "postgresql":
        return PostgresNormalizer()
    raise ValueError(f"No normalizer for dialect: {dialect}")
