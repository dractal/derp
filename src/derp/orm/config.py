"""Database configuration."""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass(kw_only=True)
class DatabaseConfig:
    """Database configuration."""

    db_url: str
    replica_url: str | None = None