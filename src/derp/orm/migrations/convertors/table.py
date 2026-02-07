"""Table operation convertors."""

from __future__ import annotations

from derp.orm.migrations.convertors.base import (
    ConvertorRegistry,
    StatementConvertor,
    quote_identifier,
    quote_schema_table,
)
from derp.orm.migrations.statements.types import (
    ColumnDefinition,
    CreateTableStatement,
    DropTableStatement,
    RenameTableStatement,
)


def _column_def_to_sql(col: ColumnDefinition) -> str:
    """Convert a column definition to SQL."""
    parts = [quote_identifier(col.name)]

    # Type with array dimensions
    col_type = col.type.upper()
    if col.array_dimensions > 0:
        col_type += "[]" * col.array_dimensions
    parts.append(col_type)

    # Primary key
    if col.primary_key:
        parts.append("PRIMARY KEY")

    # NOT NULL (skip for primary keys as it's implicit)
    if col.not_null and not col.primary_key:
        parts.append("NOT NULL")

    # UNIQUE (skip for primary keys)
    if col.unique and not col.primary_key:
        parts.append("UNIQUE")

    # DEFAULT
    if col.default is not None:
        parts.append(f"DEFAULT {col.default}")

    # GENERATED
    if col.generated:
        parts.append(f"GENERATED ALWAYS AS ({col.generated}) STORED")

    return " ".join(parts)


class CreateTableConvertor(StatementConvertor[CreateTableStatement]):
    """Convert CREATE TABLE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "create_table"

    def convert(self, statement: CreateTableStatement) -> str:
        lines: list[str] = []
        table_ref = quote_schema_table(statement.schema_name, statement.table_name)

        lines.append(f"CREATE TABLE {table_ref} (")

        # Column definitions
        col_defs: list[str] = []
        for col in statement.columns:
            col_defs.append(f"    {_column_def_to_sql(col)}")

        # Composite primary key (if not inline)
        if statement.primary_key and len(statement.primary_key.columns) > 1:
            pk_cols = ", ".join(
                quote_identifier(c) for c in statement.primary_key.columns
            )
            if statement.primary_key.name:
                col_defs.append(
                    f"    CONSTRAINT {quote_identifier(statement.primary_key.name)} "
                    f"PRIMARY KEY ({pk_cols})"
                )
            else:
                col_defs.append(f"    PRIMARY KEY ({pk_cols})")

        # Unique constraints
        for uc in statement.unique_constraints:
            uc_cols = ", ".join(quote_identifier(c) for c in uc.columns)
            constraint_sql = f"    CONSTRAINT {quote_identifier(uc.name)} UNIQUE"
            if uc.nulls_not_distinct:
                constraint_sql += " NULLS NOT DISTINCT"
            constraint_sql += f" ({uc_cols})"
            col_defs.append(constraint_sql)

        # Check constraints
        for cc in statement.check_constraints:
            col_defs.append(
                f"    CONSTRAINT {quote_identifier(cc.name)} CHECK ({cc.expression})"
            )

        # Foreign keys
        for fk in statement.foreign_keys:
            fk_cols = ", ".join(quote_identifier(c) for c in fk.columns)
            ref_cols = ", ".join(quote_identifier(c) for c in fk.references_columns)
            ref_table = quote_schema_table(fk.references_schema, fk.references_table)

            fk_sql = (
                f"    CONSTRAINT {quote_identifier(fk.name)} "
                f"FOREIGN KEY ({fk_cols}) REFERENCES {ref_table}({ref_cols})"
            )
            if fk.on_delete:
                fk_sql += f" ON DELETE {fk.on_delete.upper()}"
            if fk.on_update:
                fk_sql += f" ON UPDATE {fk.on_update.upper()}"
            if fk.deferrable:
                fk_sql += " DEFERRABLE"
                if fk.initially_deferred:
                    fk_sql += " INITIALLY DEFERRED"
            col_defs.append(fk_sql)

        lines.append(",\n".join(col_defs))
        lines.append(");")

        return "\n".join(lines)


class DropTableConvertor(StatementConvertor[DropTableStatement]):
    """Convert DROP TABLE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "drop_table"

    def convert(self, statement: DropTableStatement) -> str:
        table_ref = quote_schema_table(statement.schema_name, statement.table_name)
        cascade = " CASCADE" if statement.cascade else ""
        return f"DROP TABLE IF EXISTS {table_ref}{cascade};"


class RenameTableConvertor(StatementConvertor):
    """Convert RENAME TABLE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "rename_table"

    def convert(self, statement: RenameTableStatement) -> str:
        from_ref = quote_schema_table(statement.schema_name, statement.from_table)
        to_name = quote_identifier(statement.to_table)
        return f"ALTER TABLE {from_ref} RENAME TO {to_name};"


# Register convertors
ConvertorRegistry.register(CreateTableConvertor())
ConvertorRegistry.register(DropTableConvertor())
ConvertorRegistry.register(RenameTableConvertor())
