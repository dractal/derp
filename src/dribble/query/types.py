"""Type definitions for query results."""

from __future__ import annotations

from typing import Any, TypeVar

from pydantic import BaseModel

# Generic type for table classes
T = TypeVar("T", bound=BaseModel)

# Result row type - can be a model instance or dict for partial selects
Row = dict[str, Any]
