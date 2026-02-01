"""Storage configuration."""

from __future__ import annotations

import dataclasses


@dataclasses.dataclass(kw_only=True)
class StorageConfig:
    """Storage configuration."""

    endpoint_url: str | None = None
    service_name: str = "s3"
    access_key_id: str | None = None
    secret_access_key: str | None = None
    session_token: str | None = None
    region: str = "auto"
    use_ssl: bool = True
    verify: bool | str = True