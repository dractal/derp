"""Table base class for Dribble ORM using Pydantic."""

from __future__ import annotations

from typing import Any, ClassVar

from pydantic import BaseModel

from dribble.fields import FieldInfo


class ColumnAccessor:
    """Provides attribute access to table columns for query building."""

    def __init__(self, columns: dict[str, FieldInfo]):
        self._columns = columns

    def __getattr__(self, name: str) -> FieldInfo:
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in self._columns:
            raise AttributeError(f"Column '{name}' not found")
        return self._columns[name]

    def __iter__(self):
        return iter(self._columns.values())

    def __contains__(self, item: str) -> bool:
        return item in self._columns


class TableMeta(type(BaseModel)):
    """Metaclass for Table that processes field definitions."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
        table_name: str | None = None,
        **kwargs: Any,
    ) -> Any:
        # Store table_name in class namespace
        if table_name is not None:
            namespace["__table_name__"] = table_name
        elif "__table_name__" not in namespace:
            # Default to lowercase class name
            namespace["__table_name__"] = name.lower()

        # Extract FieldInfo objects BEFORE Pydantic processes them
        field_infos: dict[str, FieldInfo] = {}
        annotations = namespace.get("__annotations__", {})

        for field_name, type_hint in annotations.items():
            if field_name.startswith("_"):
                continue

            default = namespace.get(field_name)
            if isinstance(default, FieldInfo):
                field_infos[field_name] = default
                # Remove from namespace so Pydantic doesn't try to process it
                # Pydantic needs to see either no default or a proper Pydantic default
                del namespace[field_name]

        # Store field infos for processing after class creation
        namespace["__dribble_field_infos__"] = field_infos

        # Create the class (Pydantic will process it)
        cls = super().__new__(mcs, name, bases, namespace, **kwargs)

        # Process field definitions after class creation
        if name != "Table":  # Skip base Table class
            mcs._process_fields(cls)

        return cls

    @staticmethod
    def _process_fields(cls: Any) -> None:
        """Process FieldInfo annotations and set up column metadata."""
        columns: dict[str, FieldInfo] = {}
        table_name = getattr(cls, "__table_name__", cls.__name__.lower())

        # Get the stored field infos
        field_infos = getattr(cls, "__dribble_field_infos__", {})

        for field_name, info in field_infos.items():
            # Clone FieldInfo with table/field name set
            field_info = FieldInfo(
                field_type=info.field_type,
                primary_key=info.primary_key,
                unique=info.unique,
                nullable=info.nullable,
                default=info.default,
                foreign_key=info.foreign_key,
                index=info.index,
                _table_name=table_name,
                _field_name=field_name,
            )
            columns[field_name] = field_info

        cls.__columns__ = columns
        cls.c = ColumnAccessor(columns)


class Table(BaseModel, metaclass=TableMeta):
    """Base class for all Dribble table definitions.

    Example:
        class User(Table, table_name="users"):
            id: int = Field(Serial(), primary_key=True)
            name: str = Field(Varchar(255))
            email: str = Field(Varchar(255), unique=True)

        # Access columns via .c attribute for query building:
        db.select(User).where(eq(User.c.id, 1))
    """

    __table_name__: ClassVar[str]
    __columns__: ClassVar[dict[str, FieldInfo]]
    c: ClassVar[ColumnAccessor]

    model_config = {"from_attributes": True}

    @classmethod
    def get_table_name(cls) -> str:
        """Get the SQL table name."""
        return cls.__table_name__

    @classmethod
    def get_columns(cls) -> dict[str, FieldInfo]:
        """Get all column definitions."""
        return getattr(cls, "__columns__", {})

    @classmethod
    def get_primary_key(cls) -> tuple[str, FieldInfo] | None:
        """Get the primary key column if any."""
        for name, info in cls.get_columns().items():
            if info.primary_key:
                return (name, info)
        return None

    @classmethod
    def to_ddl(cls) -> str:
        """Generate CREATE TABLE DDL statement."""
        columns = cls.get_columns()
        if not columns:
            raise ValueError(f"Table {cls.__name__} has no columns defined")

        table_name = cls.get_table_name()
        column_defs: list[str] = []
        constraints: list[str] = []
        indexes: list[str] = []

        for col_name, info in columns.items():
            col_def = f"    {col_name} {info.field_type.sql_type()}"

            if info.primary_key:
                col_def += " PRIMARY KEY"
            if not info.nullable and not info.primary_key:
                col_def += " NOT NULL"
            if info.unique and not info.primary_key:
                col_def += " UNIQUE"
            if info.default is not None:
                if isinstance(info.default, str) and (
                    info.default.endswith("()")
                    or info.default.upper() in ("CURRENT_TIMESTAMP", "TRUE", "FALSE")
                ):
                    col_def += f" DEFAULT {info.default}"
                elif isinstance(info.default, bool):
                    col_def += f" DEFAULT {str(info.default).upper()}"
                elif isinstance(info.default, int | float):
                    col_def += f" DEFAULT {info.default}"
                else:
                    col_def += f" DEFAULT '{info.default}'"

            column_defs.append(col_def)

            # Foreign key constraints
            if info.foreign_key:
                fk = info.foreign_key
                ref_table, ref_col = fk.reference.split(".")
                constraint = f"    FOREIGN KEY ({col_name}) REFERENCES {ref_table}({ref_col})"
                if fk.on_delete:
                    constraint += f" ON DELETE {fk.on_delete}"
                if fk.on_update:
                    constraint += f" ON UPDATE {fk.on_update}"
                constraints.append(constraint)

            # Indexes
            if info.index:
                indexes.append(
                    f"CREATE INDEX idx_{table_name}_{col_name} ON {table_name}({col_name});"
                )

        all_defs = column_defs + constraints
        ddl = f"CREATE TABLE {table_name} (\n"
        ddl += ",\n".join(all_defs)
        ddl += "\n);"

        if indexes:
            ddl += "\n\n" + "\n".join(indexes)

        return ddl


def get_column_ref(table: type[Table], column_name: str) -> FieldInfo:
    """Get a column reference for query building."""
    columns = table.get_columns()
    if column_name not in columns:
        raise ValueError(f"Column {column_name} not found in table {table.__name__}")
    return columns[column_name]
