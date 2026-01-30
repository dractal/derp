"""Column operation convertors."""

from __future__ import annotations

from derp.orm.migrations.convertors.base import (
    ConvertorRegistry,
    StatementConvertor,
    quote_identifier,
    quote_schema_table,
)
from derp.orm.migrations.statements.types import (
    AddColumnStatement,
    AlterColumnDefaultStatement,
    AlterColumnNullableStatement,
    AlterColumnTypeStatement,
    DropColumnStatement,
    RenameColumnStatement,
)


class AddColumnConvertor(StatementConvertor[AddColumnStatement]):
    """Convert ADD COLUMN statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "alter_table_add_column"

    def convert(self, statement: AddColumnStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        col = statement.column

        parts = [quote_identifier(col.name)]

        # Type with array dimensions
        col_type = col.type.upper()
        if col.array_dimensions > 0:
            col_type += "[]" * col.array_dimensions
        parts.append(col_type)

        # NOT NULL
        if col.not_null:
            parts.append("NOT NULL")

        # UNIQUE
        if col.unique:
            parts.append("UNIQUE")

        # DEFAULT
        if col.default is not None:
            parts.append(f"DEFAULT {col.default}")

        # GENERATED
        if col.generated:
            parts.append(f"GENERATED ALWAYS AS ({col.generated}) STORED")

        col_def = " ".join(parts)
        return f"ALTER TABLE {table_ref} ADD COLUMN {col_def};"


class DropColumnConvertor(StatementConvertor[DropColumnStatement]):
    """Convert DROP COLUMN statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "alter_table_drop_column"

    def convert(self, statement: DropColumnStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        col_name = quote_identifier(statement.column_name)
        cascade = " CASCADE" if statement.cascade else ""
        return f"ALTER TABLE {table_ref} DROP COLUMN {col_name}{cascade};"


class RenameColumnConvertor(StatementConvertor[RenameColumnStatement]):
    """Convert RENAME COLUMN statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "alter_table_rename_column"

    def convert(self, statement: RenameColumnStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        from_name = quote_identifier(statement.from_column)
        to_name = quote_identifier(statement.to_column)
        return f"ALTER TABLE {table_ref} RENAME COLUMN {from_name} TO {to_name};"


class AlterColumnTypeConvertor(StatementConvertor[AlterColumnTypeStatement]):
    """Convert ALTER COLUMN TYPE statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "alter_table_alter_column_set_type"

    def convert(self, statement: AlterColumnTypeStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        col_name = quote_identifier(statement.column_name)
        new_type = statement.new_type.upper()

        sql = (
            f"ALTER TABLE {table_ref} ALTER COLUMN {col_name} SET DATA TYPE {new_type}"
        )

        if statement.using:
            sql += f" USING {statement.using}"

        return sql + ";"


class AlterColumnNullableConvertor(StatementConvertor[AlterColumnNullableStatement]):
    """Convert ALTER COLUMN SET/DROP NOT NULL statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "alter_table_alter_column_set_nullable"

    def convert(self, statement: AlterColumnNullableStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        col_name = quote_identifier(statement.column_name)

        if statement.nullable:
            return f"ALTER TABLE {table_ref} ALTER COLUMN {col_name} DROP NOT NULL;"

        return f"ALTER TABLE {table_ref} ALTER COLUMN {col_name} SET NOT NULL;"


class AlterColumnDefaultConvertor(StatementConvertor[AlterColumnDefaultStatement]):
    """Convert ALTER COLUMN SET/DROP DEFAULT statements to SQL."""

    @property
    def statement_type(self) -> str:
        return "alter_table_alter_column_set_default"

    def convert(self, statement: AlterColumnDefaultStatement) -> str:
        table_ref = quote_schema_table(statement.schema, statement.table_name)
        col_name = quote_identifier(statement.column_name)

        if statement.default is None:
            return f"ALTER TABLE {table_ref} ALTER COLUMN {col_name} DROP DEFAULT;"
        return (
            f"ALTER TABLE {table_ref} ALTER COLUMN {col_name} "
            f"SET DEFAULT {statement.default};"
        )


# Register convertors
ConvertorRegistry.register(AddColumnConvertor())
ConvertorRegistry.register(DropColumnConvertor())
ConvertorRegistry.register(RenameColumnConvertor())
ConvertorRegistry.register(AlterColumnTypeConvertor())
ConvertorRegistry.register(AlterColumnNullableConvertor())
ConvertorRegistry.register(AlterColumnDefaultConvertor())
