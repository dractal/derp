"""Safety module for detecting potentially destructive operations.

This module analyzes migration statements and warns about operations
that could result in data loss, similar to Drizzle's safety features.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from derp.migrations.statements.types import (
    AlterColumnNullableStatement,
    AlterColumnTypeStatement,
    DropColumnStatement,
    DropEnumStatement,
    DropSchemaStatement,
    DropSequenceStatement,
    DropTableStatement,
    JsonStatement,
    Statement,
)


class DataLossRisk(StrEnum):
    """Risk level for data loss."""

    NONE = "none"
    LOW = "low"  # Reversible with some effort
    MEDIUM = "medium"  # May lose data, but recoverable
    HIGH = "high"  # Will definitely lose data


@dataclass
class DestructiveOperation:
    """Represents a potentially destructive operation."""

    statement: JsonStatement
    risk: DataLossRisk
    description: str
    suggestion: str | None = None


def detect_destructive_operations(
    statements: list[Statement],
) -> list[DestructiveOperation]:
    """Detect potentially destructive operations in statements.

    Args:
        statements: List of migration statements to analyze

    Returns:
        List of DestructiveOperation objects for dangerous operations
    """
    destructive: list[DestructiveOperation] = []

    for stmt in statements:
        match stmt:
            case DropTableStatement():
                destructive.append(
                    DestructiveOperation(
                        statement=stmt,
                        risk=DataLossRisk.HIGH,
                        description=(
                            f"Dropping table '{stmt.table_name}' will "
                            "permanently delete all data in the table"
                        ),
                        suggestion=(
                            "Consider renaming the table instead if you want to "
                            "preserve the data, or back up the data first"
                        ),
                    )
                )

            case DropColumnStatement():
                destructive.append(
                    DestructiveOperation(
                        statement=stmt,
                        risk=DataLossRisk.HIGH,
                        description=(
                            f"Dropping column '{stmt.column_name}' from "
                            f"'{stmt.table_name}' will permanently delete "
                            "all data in that column"
                        ),
                        suggestion=(
                            "Back up the column data before proceeding, or "
                            "consider renaming the column instead"
                        ),
                    )
                )

            case AlterColumnTypeStatement():
                if _is_narrowing_type_change(stmt.old_type, stmt.new_type):
                    destructive.append(
                        DestructiveOperation(
                            statement=stmt,
                            risk=DataLossRisk.MEDIUM,
                            description=(
                                f"Type change from '{stmt.old_type}' to "
                                f"'{stmt.new_type}' on "
                                f"'{stmt.table_name}.{stmt.column_name}' "
                                "may truncate or fail for existing data"
                            ),
                            suggestion=(
                                "Review existing data to ensure it fits in the "
                                "new type, or add a USING clause for conversion"
                            ),
                        )
                    )

            case AlterColumnNullableStatement():
                if not stmt.nullable:  # SET NOT NULL
                    destructive.append(
                        DestructiveOperation(
                            statement=stmt,
                            risk=DataLossRisk.LOW,
                            description=(
                                f"Setting '{stmt.table_name}."
                                f"{stmt.column_name}' to NOT NULL will "
                                "fail if any NULL values exist"
                            ),
                            suggestion=(
                                "Update NULL values to a default before applying "
                                "this change, or add a default value"
                            ),
                        )
                    )

            case DropSchemaStatement():
                if stmt.cascade:
                    destructive.append(
                        DestructiveOperation(
                            statement=stmt,
                            risk=DataLossRisk.HIGH,
                            description=(
                                f"Dropping schema '{stmt.name}' with CASCADE "
                                "will delete all objects and data within it"
                            ),
                            suggestion=(
                                "Ensure no important data exists in the schema "
                                "before dropping it"
                            ),
                        )
                    )

            case DropEnumStatement():
                destructive.append(
                    DestructiveOperation(
                        statement=stmt,
                        risk=DataLossRisk.MEDIUM,
                        description=(
                            f"Dropping enum type '{stmt.name}' may fail if "
                            "any columns are using this type"
                        ),
                        suggestion=(
                            "Ensure no columns are using this enum type, or "
                            "alter those columns first"
                        ),
                    )
                )

            case DropSequenceStatement():
                destructive.append(
                    DestructiveOperation(
                        statement=stmt,
                        risk=DataLossRisk.LOW,
                        description=(
                            f"Dropping sequence '{stmt.name}' may affect "
                            "columns that depend on it"
                        ),
                        suggestion=(
                            "Check for columns using this sequence as a default "
                            "before dropping"
                        ),
                    )
                )

    return destructive


def _is_narrowing_type_change(old_type: str, new_type: str) -> bool:
    """Check if a type change could result in data loss.

    Args:
        old_type: Original column type
        new_type: New column type

    Returns:
        True if the change could truncate or fail with existing data
    """
    old_lower = old_type.lower()
    new_lower = new_type.lower()

    # Text to varchar with length is narrowing
    if old_lower == "text" and new_lower.startswith("varchar"):
        return True
    if old_lower == "text" and new_lower.startswith("character varying"):
        return True

    # varchar(n) to varchar(m) where m < n
    if old_lower.startswith("varchar") or old_lower.startswith("character varying"):
        old_len = _extract_length(old_lower)
        if new_lower.startswith("varchar") or new_lower.startswith("character varying"):
            new_len = _extract_length(new_lower)
            if old_len is not None and new_len is not None and new_len < old_len:
                return True

    # bigint to int/smallint
    if old_lower == "bigint" and new_lower in (
        "integer",
        "int",
        "smallint",
        "int2",
        "int4",
    ):
        return True

    # integer to smallint
    if old_lower in ("integer", "int", "int4") and new_lower in ("smallint", "int2"):
        return True

    # numeric/decimal precision reduction
    if old_lower.startswith("numeric") or old_lower.startswith("decimal"):
        old_precision = _extract_precision(old_lower)
        if new_lower.startswith("numeric") or new_lower.startswith("decimal"):
            new_precision = _extract_precision(new_lower)
            if old_precision and new_precision:
                if new_precision[0] < old_precision[0]:  # precision reduced
                    return True
                if len(new_precision) > 1 and len(old_precision) > 1:
                    if new_precision[1] < old_precision[1]:  # scale reduced
                        return True

    # double precision to real
    if old_lower == "double precision" and new_lower == "real":
        return True

    # timestamp with timezone to timestamp without
    if "timestamp" in old_lower and "with time zone" in old_lower:
        if "timestamp" in new_lower and "with time zone" not in new_lower:
            return True

    return False


def _extract_length(type_str: str) -> int | None:
    """Extract length from varchar(n) or character varying(n)."""
    import re

    match = re.search(r"\((\d+)\)", type_str)
    if match:
        return int(match.group(1))
    return None


def _extract_precision(type_str: str) -> tuple[int, ...] | None:
    """Extract precision/scale from numeric(p,s) or decimal(p,s)."""
    import re

    match = re.search(r"\((\d+)(?:,\s*(\d+))?\)", type_str)
    if match:
        if match.group(2):
            return (int(match.group(1)), int(match.group(2)))
        return (int(match.group(1)),)
    return None


def format_destructive_warnings(operations: list[DestructiveOperation]) -> str:
    """Format destructive operations as a human-readable warning message.

    Args:
        operations: List of destructive operations

    Returns:
        Formatted warning string
    """
    if not operations:
        return ""

    lines = ["The following changes may result in data loss:\n"]

    for i, op in enumerate(operations, 1):
        risk_emoji = {
            DataLossRisk.HIGH: "[HIGH]",
            DataLossRisk.MEDIUM: "[MEDIUM]",
            DataLossRisk.LOW: "[LOW]",
        }.get(op.risk, "")

        lines.append(f"  {i}. {risk_emoji} {op.description}")
        if op.suggestion:
            lines.append(f"     Suggestion: {op.suggestion}")
        lines.append("")

    return "\n".join(lines)


def has_high_risk_operations(operations: list[DestructiveOperation]) -> bool:
    """Check if any operations are high risk.

    Args:
        operations: List of destructive operations

    Returns:
        True if any high-risk operations exist
    """
    return any(op.risk == DataLossRisk.HIGH for op in operations)
