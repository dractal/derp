"""Migration statement IR.

Each statement represents a single DDL change.  The differ emits these;
the runner applies them.  They are JSON-serializable so a migration
batch can be inspected before execution.
"""

from __future__ import annotations

import abc
import dataclasses
from typing import Any


@dataclasses.dataclass
class Statement(abc.ABC):
    """Base class for migration statements."""

    @abc.abstractmethod
    def to_sql(self) -> str:
        """Render this statement as a single SQL string."""

    @abc.abstractmethod
    def kind(self) -> str:
        """Categorical kind: ``create``, ``drop``, ``alter``."""

    def is_destructive(self) -> bool:
        """Whether applying this statement loses data."""
        return False

    def to_dict(self) -> dict[str, Any]:
        return {"kind": type(self).__name__, **dataclasses.asdict(self)}


# =============================================================================
# Table-level
# =============================================================================


@dataclasses.dataclass
class CreateTable(Statement):
    name: str
    sql: str

    def to_sql(self) -> str:
        return self.sql

    def kind(self) -> str:
        return "create"


@dataclasses.dataclass
class DropTable(Statement):
    name: str
    if_exists: bool = True
    sync: bool = False

    def to_sql(self) -> str:
        sql = "DROP TABLE"
        if self.if_exists:
            sql += " IF EXISTS"
        sql += f" {self.name}"
        if self.sync:
            sql += " SYNC"
        return sql

    def kind(self) -> str:
        return "drop"

    def is_destructive(self) -> bool:
        return True


# =============================================================================
# Column-level
# =============================================================================


@dataclasses.dataclass
class AddColumn(Statement):
    table: str
    name: str
    column_sql: str

    def to_sql(self) -> str:
        return f"ALTER TABLE {self.table} ADD COLUMN {self.column_sql}"

    def kind(self) -> str:
        return "alter"


@dataclasses.dataclass
class DropColumn(Statement):
    table: str
    name: str
    if_exists: bool = True

    def to_sql(self) -> str:
        sql = f"ALTER TABLE {self.table} DROP COLUMN"
        if self.if_exists:
            sql += " IF EXISTS"
        sql += f" `{self.name}`"
        return sql

    def kind(self) -> str:
        return "alter"

    def is_destructive(self) -> bool:
        return True


@dataclasses.dataclass
class ModifyColumn(Statement):
    table: str
    name: str
    column_sql: str

    def to_sql(self) -> str:
        return f"ALTER TABLE {self.table} MODIFY COLUMN {self.column_sql}"

    def kind(self) -> str:
        return "alter"


@dataclasses.dataclass
class RenameColumn(Statement):
    table: str
    old_name: str
    new_name: str

    def to_sql(self) -> str:
        return (
            f"ALTER TABLE {self.table} RENAME COLUMN "
            f"`{self.old_name}` TO `{self.new_name}`"
        )

    def kind(self) -> str:
        return "alter"


@dataclasses.dataclass
class CommentColumn(Statement):
    table: str
    name: str
    comment: str

    def to_sql(self) -> str:
        escaped = self.comment.replace("'", "\\'")
        return f"ALTER TABLE {self.table} COMMENT COLUMN `{self.name}` '{escaped}'"

    def kind(self) -> str:
        return "alter"


# =============================================================================
# Index-level
# =============================================================================


@dataclasses.dataclass
class AddIndex(Statement):
    table: str
    index_sql: str

    def to_sql(self) -> str:
        return f"ALTER TABLE {self.table} ADD {self.index_sql}"

    def kind(self) -> str:
        return "alter"


@dataclasses.dataclass
class DropIndex(Statement):
    table: str
    name: str
    if_exists: bool = True

    def to_sql(self) -> str:
        sql = f"ALTER TABLE {self.table} DROP INDEX"
        if self.if_exists:
            sql += " IF EXISTS"
        sql += f" `{self.name}`"
        return sql

    def kind(self) -> str:
        return "alter"


# =============================================================================
# Projection-level
# =============================================================================


@dataclasses.dataclass
class AddProjection(Statement):
    table: str
    projection_sql: str

    def to_sql(self) -> str:
        return f"ALTER TABLE {self.table} ADD {self.projection_sql}"

    def kind(self) -> str:
        return "alter"


@dataclasses.dataclass
class DropProjection(Statement):
    table: str
    name: str
    if_exists: bool = True

    def to_sql(self) -> str:
        sql = f"ALTER TABLE {self.table} DROP PROJECTION"
        if self.if_exists:
            sql += " IF EXISTS"
        sql += f" `{self.name}`"
        return sql

    def kind(self) -> str:
        return "alter"


# =============================================================================
# TTL / engine settings
# =============================================================================


@dataclasses.dataclass
class AlterModifyTTL(Statement):
    table: str
    ttl: str

    def to_sql(self) -> str:
        return f"ALTER TABLE {self.table} MODIFY TTL {self.ttl}"

    def kind(self) -> str:
        return "alter"


@dataclasses.dataclass
class AlterRemoveTTL(Statement):
    table: str

    def to_sql(self) -> str:
        return f"ALTER TABLE {self.table} REMOVE TTL"

    def kind(self) -> str:
        return "alter"


@dataclasses.dataclass
class AlterModifySetting(Statement):
    table: str
    settings: dict[str, str]

    def to_sql(self) -> str:
        parts = [f"{k} = {v}" for k, v in self.settings.items()]
        return f"ALTER TABLE {self.table} MODIFY SETTING {', '.join(parts)}"

    def kind(self) -> str:
        return "alter"


@dataclasses.dataclass
class AlterResetSetting(Statement):
    table: str
    names: tuple[str, ...]

    def to_sql(self) -> str:
        return f"ALTER TABLE {self.table} RESET SETTING {', '.join(self.names)}"

    def kind(self) -> str:
        return "alter"
