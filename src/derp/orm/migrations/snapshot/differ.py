"""Snapshot diffing to generate migration statements.

Compares two schema snapshots and produces JSON statements representing
the changes needed to migrate from the old schema to the new schema.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from derp.orm.migrations.snapshot.models import (
    ColumnSnapshot,
    EnumSnapshot,
    SchemaSnapshot,
    TableSnapshot,
)
from derp.orm.migrations.statements.types import (
    AddColumnStatement,
    AlterColumnDefaultStatement,
    AlterColumnNullableStatement,
    AlterColumnTypeStatement,
    AlterEnumAddValueStatement,
    ColumnDefinition,
    CreateEnumStatement,
    CreateForeignKeyStatement,
    CreateIndexStatement,
    CreatePolicyStatement,
    CreateSchemaStatement,
    CreateSequenceStatement,
    CreateTableStatement,
    CreateUniqueConstraintStatement,
    DisableRLSStatement,
    DropColumnStatement,
    DropEnumStatement,
    DropForeignKeyStatement,
    DropIndexStatement,
    DropPolicyStatement,
    DropSchemaStatement,
    DropSequenceStatement,
    DropTableStatement,
    DropUniqueConstraintStatement,
    EnableRLSStatement,
    ForeignKeyDefinition,
    PrimaryKeyDefinition,
    RenameColumnStatement,
    Statement,
    UniqueConstraintDefinition,
)


@dataclass
class RenameResolution:
    """Resolution for a potential rename operation."""

    old_name: str
    new_name: str
    is_rename: bool  # True = rename, False = drop + create


@dataclass
class DiffResult:
    """Result of comparing two snapshots."""

    statements: list[Statement] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _column_to_definition(col: ColumnSnapshot) -> ColumnDefinition:
    """Convert a ColumnSnapshot to a ColumnDefinition."""
    return ColumnDefinition(
        name=col.name,
        type=col.type,
        primary_key=col.primary_key,
        not_null=col.not_null,
        unique=col.unique,
        default=col.default,
        generated=col.generated,
        identity=col.identity.model_dump() if col.identity else None,
        array_dimensions=col.array_dimensions,
    )


def _table_to_create_statement(table: TableSnapshot) -> CreateTableStatement:
    """Convert a TableSnapshot to a CreateTableStatement."""
    columns = [_column_to_definition(col) for col in table.columns.values()]

    # Primary key
    pk = None
    if table.primary_key:
        pk = PrimaryKeyDefinition(
            name=table.primary_key.name,
            columns=table.primary_key.columns,
        )

    # Unique constraints
    unique_constraints = [
        UniqueConstraintDefinition(
            name=uc.name,
            columns=uc.columns,
            nulls_not_distinct=uc.nulls_not_distinct,
        )
        for uc in table.unique_constraints.values()
    ]

    # Foreign keys
    foreign_keys = [
        ForeignKeyDefinition(
            name=fk.name,
            columns=fk.columns,
            references_schema=fk.references_schema,
            references_table=fk.references_table,
            references_columns=fk.references_columns,
            on_delete=fk.on_delete.value if fk.on_delete else None,
            on_update=fk.on_update.value if fk.on_update else None,
            deferrable=fk.deferrable,
            initially_deferred=fk.initially_deferred,
        )
        for fk in table.foreign_keys.values()
    ]

    return CreateTableStatement(
        table_name=table.name,
        schema=table.schema_name,
        columns=columns,
        primary_key=pk,
        unique_constraints=unique_constraints,
        foreign_keys=foreign_keys,
    )


class SnapshotDiffer:
    """Compare two schema snapshots and generate migration statements.

    This class implements Drizzle-style snapshot diffing, comparing the
    old (previous) schema state against the new (current) schema state
    to determine what changes need to be made.
    """

    def __init__(
        self,
        old_snapshot: SchemaSnapshot,
        new_snapshot: SchemaSnapshot,
        rename_resolver: Callable[[str, str, str], bool] | None = None,
    ):
        """Initialize the differ.

        Args:
            old_snapshot: Previous schema state
            new_snapshot: Current/desired schema state
            rename_resolver: Optional callback for resolving rename ambiguities
              signature: (object_type, old_name, new_name) -> bool (True=rename)
        """
        self.old = old_snapshot
        self.new = new_snapshot
        self.rename_resolver = rename_resolver
        self._statements: list[Statement] = []
        self._warnings: list[str] = []

    def diff(self) -> list[Statement]:
        """Compare snapshots and return list of migration statements.

        Returns:
            List of Statement objects representing the migration
        """
        self._statements = []
        self._warnings = []

        # Order matters for dependency handling:
        # 1. Create new schemas first (other objects depend on them)
        # 2. Create new enums (columns depend on them)
        # 3. Create new sequences (columns may reference them)
        # 4. Create/alter tables
        # 5. Create indexes, constraints
        # 6. Enable RLS, create policies
        # 7. Drop in reverse order

        # Schemas
        self._diff_schemas()

        # Enums (must be before tables that use them)
        self._diff_enums()

        # Sequences
        self._diff_sequences()

        # Tables (includes columns, PKs, inline constraints)
        self._diff_tables()

        # Policies and RLS
        self._diff_policies()

        return self._statements

    def get_warnings(self) -> list[str]:
        """Get any warnings generated during diff."""
        return self._warnings

    def _sort_tables_by_fk_deps(self, table_keys: set[str]) -> list[str]:
        """Sort tables by foreign key dependencies (topological sort).

        Tables that are referenced by other tables come first.
        This ensures CREATE TABLE statements are in the correct order.
        """
        if not table_keys:
            return []

        # Build dependency graph
        # deps[table] = set of tables that 'table' depends on (references)
        deps: dict[str, set[str]] = {key: set() for key in table_keys}

        for table_key in table_keys:
            table = self.new.tables[table_key]
            for fk in table.foreign_keys.values():
                ref_table = fk.references_table
                ref_schema = fk.references_schema or "public"
                if ref_schema == "public":
                    ref_key = ref_table
                else:
                    ref_key = f"{ref_schema}.{ref_table}"
                # Only add dependency if referenced table is in the set being created
                if ref_key in table_keys:
                    deps[table_key].add(ref_key)

        # Topological sort using Kahn's algorithm
        result: list[str] = []
        # Count incoming edges (how many tables depend on each table)
        in_degree = {key: 0 for key in table_keys}
        for table_key, table_deps in deps.items():
            for dep in table_deps:
                in_degree[dep] = in_degree.get(dep, 0)  # ensure dep exists

        # Reverse: count how many tables each table depends on
        for table_key in table_keys:
            in_degree[table_key] = len(deps[table_key])

        # Start with tables that have no dependencies
        queue = [key for key in table_keys if in_degree[key] == 0]

        while queue:
            # Sort for deterministic ordering
            queue.sort()
            current = queue.pop(0)
            result.append(current)

            # Remove this table from dependencies of others
            for table_key in table_keys:
                if current in deps[table_key]:
                    deps[table_key].remove(current)
                    in_degree[table_key] -= 1
                    if in_degree[table_key] == 0:
                        queue.append(table_key)

        # If not all tables processed, there's a cycle - just return remaining
        remaining = table_keys - set(result)
        result.extend(sorted(remaining))

        return result

    def _diff_schemas(self) -> None:
        """Diff database schemas (not tables, the namespace)."""
        old_schemas = set(self.old.schemas)
        new_schemas = set(self.new.schemas)

        # Create new schemas
        for schema in new_schemas - old_schemas:
            if schema != "public":  # public always exists
                self._statements.append(CreateSchemaStatement(name=schema))

        # Drop removed schemas
        for schema in old_schemas - new_schemas:
            if schema != "public":
                self._statements.append(DropSchemaStatement(name=schema, cascade=True))

    def _diff_enums(self) -> None:
        """Diff enum types."""
        old_enums = set(self.old.enums.keys())
        new_enums = set(self.new.enums.keys())

        # Create new enums
        for enum_key in new_enums - old_enums:
            enum = self.new.enums[enum_key]
            self._statements.append(
                CreateEnumStatement(
                    name=enum.name,
                    schema=enum.schema_name,
                    values=enum.values,
                )
            )

        # Drop removed enums
        for enum_key in old_enums - new_enums:
            enum = self.old.enums[enum_key]
            self._statements.append(
                DropEnumStatement(
                    name=enum.name,
                    schema=enum.schema_name,
                    cascade=True,
                )
            )

        # Alter existing enums (add new values)
        for enum_key in old_enums & new_enums:
            old_enum = self.old.enums[enum_key]
            new_enum = self.new.enums[enum_key]
            self._diff_enum_values(old_enum, new_enum)

    def _diff_enum_values(self, old_enum: EnumSnapshot, new_enum: EnumSnapshot) -> None:
        """Diff values within an enum type."""
        old_values = set(old_enum.values)
        new_values = set(new_enum.values)

        # Add new values
        # Note: PostgreSQL doesn't support removing enum values easily
        for value in new_values - old_values:
            # Try to maintain order by finding position
            new_idx = new_enum.values.index(value)
            before = None
            after = None

            if new_idx > 0:
                # Add after previous value if it exists in old
                prev_value = new_enum.values[new_idx - 1]
                if prev_value in old_values:
                    after = prev_value

            self._statements.append(
                AlterEnumAddValueStatement(
                    name=new_enum.name,
                    schema=new_enum.schema_name,
                    value=value,
                    after=after,
                    before=before,
                )
            )

        # Warn about removed values (can't easily remove in PostgreSQL)
        removed = old_values - new_values
        if removed:
            self._warnings.append(
                f"Enum '{new_enum.name}' has removed values {removed}. "
                "PostgreSQL doesn't support removing enum values. "
                "Consider recreating the enum or leaving values in place."
            )

    def _diff_sequences(self) -> None:
        """Diff sequences."""
        old_seqs = set(self.old.sequences.keys())
        new_seqs = set(self.new.sequences.keys())

        # Create new sequences
        for seq_key in new_seqs - old_seqs:
            seq = self.new.sequences[seq_key]
            self._statements.append(
                CreateSequenceStatement(
                    name=seq.name,
                    schema=seq.schema_name,
                    start=seq.start,
                    increment=seq.increment,
                    min_value=seq.min_value,
                    max_value=seq.max_value,
                    cache=seq.cache,
                    cycle=seq.cycle,
                    owned_by=seq.owned_by,
                )
            )

        # Drop removed sequences
        for seq_key in old_seqs - new_seqs:
            seq = self.old.sequences[seq_key]
            self._statements.append(
                DropSequenceStatement(
                    name=seq.name,
                    schema=seq.schema_name,
                )
            )

    def _diff_tables(self) -> None:
        """Diff tables including columns and constraints."""
        old_tables = set(self.old.tables.keys())
        new_tables = set(self.new.tables.keys())

        # Sort new tables by FK dependencies (referenced tables first)
        tables_to_create = new_tables - old_tables
        sorted_tables = self._sort_tables_by_fk_deps(tables_to_create)

        # Create new tables in dependency order
        for table_key in sorted_tables:
            table = self.new.tables[table_key]
            self._statements.append(_table_to_create_statement(table))

            # Create indexes for new table
            for idx in table.indexes.values():
                self._statements.append(
                    CreateIndexStatement(
                        name=idx.name,
                        table_name=table.name,
                        schema=table.schema_name,
                        columns=idx.columns,
                        unique=idx.unique,
                        where=idx.where,
                        method=idx.method.value,
                        concurrently=idx.concurrently,
                        nulls_not_distinct=idx.nulls_not_distinct,
                        include=idx.include,
                    )
                )

        # Drop removed tables (reverse order - dependent tables first)
        for table_key in old_tables - new_tables:
            table = self.old.tables[table_key]
            self._statements.append(
                DropTableStatement(
                    table_name=table.name,
                    schema=table.schema_name,
                    cascade=True,
                )
            )

        # Diff existing tables
        for table_key in old_tables & new_tables:
            old_table = self.old.tables[table_key]
            new_table = self.new.tables[table_key]
            self._diff_table(old_table, new_table)

    def _diff_table(self, old_table: TableSnapshot, new_table: TableSnapshot) -> None:
        """Diff a single table's columns and constraints."""
        # Diff columns
        self._diff_columns(old_table, new_table)

        # Diff foreign keys
        self._diff_foreign_keys(old_table, new_table)

        # Diff unique constraints
        self._diff_unique_constraints(old_table, new_table)

        # Diff indexes
        self._diff_indexes(old_table, new_table)

        # Diff RLS settings
        self._diff_rls(old_table, new_table)

    def _columns_match(self, old_col: ColumnSnapshot, new_col: ColumnSnapshot) -> bool:
        """Check if two columns are similar enough to be a rename candidate.

        Columns match if they have the same type, nullability, and default value.
        """
        return (
            old_col.type == new_col.type
            and old_col.not_null == new_col.not_null
            and old_col.default == new_col.default
        )

    def _diff_columns(self, old_table: TableSnapshot, new_table: TableSnapshot) -> None:
        """Diff columns within a table, detecting potential renames."""
        old_cols = set(old_table.columns.keys())
        new_cols = set(new_table.columns.keys())

        dropped = old_cols - new_cols
        added = new_cols - old_cols

        # Find ALL potential rename pairs (may have duplicates for ambiguous cases)
        rename_candidates: list[tuple[str, str, ColumnSnapshot, ColumnSnapshot]] = []
        for old_name in sorted(dropped):  # sorted for deterministic order
            old_col = old_table.columns[old_name]
            for new_name in sorted(added):
                new_col = new_table.columns[new_name]
                if self._columns_match(old_col, new_col):
                    rename_candidates.append((old_name, new_name, old_col, new_col))

        # Resolve renames via callback, tracking which columns are already matched
        confirmed_renames: dict[str, str] = {}  # old_name -> new_name
        used_new_names: set[str] = set()

        for old_name, new_name, old_col, new_col in rename_candidates:
            # Skip if either column already matched
            if old_name in confirmed_renames or new_name in used_new_names:
                continue

            # Ask the resolver if this is a rename
            if self.rename_resolver is not None:
                qualified_old = f"{old_table.name}.{old_name}"
                if self.rename_resolver("column", qualified_old, new_name):
                    confirmed_renames[old_name] = new_name
                    used_new_names.add(new_name)
                    self._statements.append(
                        RenameColumnStatement(
                            table_name=new_table.name,
                            schema=new_table.schema_name,
                            from_column=old_name,
                            to_column=new_name,
                        )
                    )

        # Process remaining drops (excluding confirmed renames)
        for col_name in sorted(dropped - set(confirmed_renames.keys())):
            self._statements.append(
                DropColumnStatement(
                    table_name=old_table.name,
                    schema=old_table.schema_name,
                    column_name=col_name,
                )
            )

        # Process remaining adds (excluding confirmed renames)
        for col_name in sorted(added - used_new_names):
            col = new_table.columns[col_name]
            self._statements.append(
                AddColumnStatement(
                    table_name=new_table.name,
                    schema=new_table.schema_name,
                    column=_column_to_definition(col),
                )
            )

        # Diff existing columns (same name in both old and new)
        for col_name in old_cols & new_cols:
            old_col = old_table.columns[col_name]
            new_col = new_table.columns[col_name]
            self._diff_column(old_table.name, old_table.schema_name, old_col, new_col)

    def _diff_column(
        self,
        table_name: str,
        schema: str,
        old_col: ColumnSnapshot,
        new_col: ColumnSnapshot,
    ) -> None:
        """Diff a single column."""
        # Type change
        if old_col.type != new_col.type:
            self._statements.append(
                AlterColumnTypeStatement(
                    table_name=table_name,
                    schema=schema,
                    column_name=new_col.name,
                    old_type=old_col.type,
                    new_type=new_col.type,
                )
            )

        # Nullability change
        if old_col.not_null != new_col.not_null:
            self._statements.append(
                AlterColumnNullableStatement(
                    table_name=table_name,
                    schema=schema,
                    column_name=new_col.name,
                    nullable=not new_col.not_null,
                )
            )

        # Default change
        if old_col.default != new_col.default:
            self._statements.append(
                AlterColumnDefaultStatement(
                    table_name=table_name,
                    schema=schema,
                    column_name=new_col.name,
                    default=new_col.default,
                )
            )

    def _diff_foreign_keys(
        self, old_table: TableSnapshot, new_table: TableSnapshot
    ) -> None:
        """Diff foreign keys."""
        old_fks = set(old_table.foreign_keys.keys())
        new_fks = set(new_table.foreign_keys.keys())

        # Create new foreign keys
        for fk_name in new_fks - old_fks:
            fk = new_table.foreign_keys[fk_name]
            # Handle both enum and string values (use_enum_values=True serializes enums)
            self._statements.append(
                CreateForeignKeyStatement(
                    name=fk.name,
                    table_name=new_table.name,
                    schema=new_table.schema_name,
                    columns=fk.columns,
                    references_schema=fk.references_schema,
                    references_table=fk.references_table,
                    references_columns=fk.references_columns,
                    on_delete=fk.on_delete,
                    on_update=fk.on_update,
                    deferrable=fk.deferrable,
                    initially_deferred=fk.initially_deferred,
                )
            )

        # Drop removed foreign keys
        for fk_name in old_fks - new_fks:
            self._statements.append(
                DropForeignKeyStatement(
                    name=fk_name,
                    table_name=old_table.name,
                    schema=old_table.schema_name,
                )
            )

    def _diff_unique_constraints(
        self, old_table: TableSnapshot, new_table: TableSnapshot
    ) -> None:
        """Diff unique constraints."""
        old_ucs = set(old_table.unique_constraints.keys())
        new_ucs = set(new_table.unique_constraints.keys())

        # Create new unique constraints
        for uc_name in new_ucs - old_ucs:
            uc = new_table.unique_constraints[uc_name]
            self._statements.append(
                CreateUniqueConstraintStatement(
                    name=uc.name,
                    table_name=new_table.name,
                    schema=new_table.schema_name,
                    columns=uc.columns,
                    nulls_not_distinct=uc.nulls_not_distinct,
                )
            )

        # Drop removed unique constraints
        for uc_name in old_ucs - new_ucs:
            self._statements.append(
                DropUniqueConstraintStatement(
                    name=uc_name,
                    table_name=old_table.name,
                    schema=old_table.schema_name,
                )
            )

    def _diff_indexes(self, old_table: TableSnapshot, new_table: TableSnapshot) -> None:
        """Diff indexes."""
        old_idxs = set(old_table.indexes.keys())
        new_idxs = set(new_table.indexes.keys())

        # Create new indexes
        for idx_name in new_idxs - old_idxs:
            idx = new_table.indexes[idx_name]
            self._statements.append(
                CreateIndexStatement(
                    name=idx.name,
                    table_name=new_table.name,
                    schema=new_table.schema_name,
                    columns=idx.columns,
                    unique=idx.unique,
                    where=idx.where,
                    method=idx.method,
                    concurrently=idx.concurrently,
                    nulls_not_distinct=idx.nulls_not_distinct,
                    include=idx.include,
                )
            )

        # Drop removed indexes
        for idx_name in old_idxs - new_idxs:
            self._statements.append(
                DropIndexStatement(
                    name=idx_name,
                    schema=old_table.schema_name,
                )
            )

    def _diff_rls(self, old_table: TableSnapshot, new_table: TableSnapshot) -> None:
        """Diff Row-Level Security settings."""
        if old_table.rls_enabled != new_table.rls_enabled:
            if new_table.rls_enabled:
                self._statements.append(
                    EnableRLSStatement(
                        table_name=new_table.name,
                        schema=new_table.schema_name,
                        force=new_table.rls_forced,
                    )
                )
            else:
                self._statements.append(
                    DisableRLSStatement(
                        table_name=new_table.name,
                        schema=new_table.schema_name,
                    )
                )

    def _diff_policies(self) -> None:
        """Diff RLS policies."""
        old_policies = set(self.old.policies.keys())
        new_policies = set(self.new.policies.keys())

        # Create new policies
        for policy_key in new_policies - old_policies:
            policy = self.new.policies[policy_key]
            self._statements.append(
                CreatePolicyStatement(
                    name=policy.name,
                    table_name=policy.table,
                    schema=policy.schema_name,
                    command=policy.command,
                    permissive=policy.permissive,
                    roles=policy.roles,
                    using=policy.using,
                    with_check=policy.with_check,
                )
            )

        # Drop removed policies
        for policy_key in old_policies - new_policies:
            policy = self.old.policies[policy_key]
            self._statements.append(
                DropPolicyStatement(
                    name=policy.name,
                    table_name=policy.table,
                    schema=policy.schema_name,
                )
            )
