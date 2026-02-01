"""Derp client for interacting with database, file storage, and more."""

from __future__ import annotations

import dataclasses

from derp.auth import AuthClient, AuthConfig, BaseUser
from derp.orm import DatabaseConfig, DatabaseEngine
from derp.storage import StorageClient, StorageConfig


@dataclasses.dataclass(kw_only=True)
class DerpConfig[UserT: BaseUser]:
    """Derp configuration."""

    database: DatabaseConfig
    storage: StorageConfig | None = None
    auth: AuthConfig[UserT] | None = None


class DerpClient[UserT: BaseUser]:
    """Derp client for interacting with database, file storage, and more."""

    def __init__(self, config: DerpConfig[UserT]):
        self._config: DerpConfig[UserT] = config
        self._db: DatabaseEngine = DatabaseEngine(config.database.db_url)
        self._replica_db: DatabaseEngine | None = (
            DatabaseEngine(config.database.replica_url)
            if config.database.replica_url is not None
            else None
        )
        self._storage: StorageClient | None = (
            StorageClient(self._config.storage)
            if self._config.storage is not None
            else None
        )
        self._auth: AuthClient[UserT] | None = (
            AuthClient[UserT](self._config.auth)
            if self._config.auth is not None
            else None
        )
        self._in_session = False

    async def connect(self, storage: bool = True) -> None:
        """Start a session."""
        await self._db.connect()
        if self._replica_db is not None:
            await self._replica_db.connect()
        if storage and self._storage is not None:
            await self._storage.connect()
        if self._auth is not None:
            self._auth.set_db(self._db, replica_db=self._replica_db)

        self._in_session = True

    async def disconnect(
        self,
    ) -> None:
        """End a session."""
        await self._db.disconnect()
        if self._replica_db is not None:
            await self._replica_db.disconnect()
        if self._storage is not None:
            await self._storage.disconnect()
        if self._auth is not None:
            self._auth.set_db(None, replica_db=None)

        self._in_session = False

    @property
    def db(self) -> DatabaseEngine:
        """Get the database engine."""
        if not self._in_session:
            raise ValueError("Not in a session. Call `connect()` first.")
        return self._db

    @property
    def replica_db(self) -> DatabaseEngine:
        """Get the replica database engine."""
        if not self._in_session:
            raise ValueError("Not in a session. Call `connect()` first.")
        if self._replica_db is None:
            raise ValueError("Replica URL is not set on `DatabaseConfig`.")
        return self._replica_db

    @property
    def storage(self) -> StorageClient:
        """Get the storage client."""
        if not self._in_session:
            raise ValueError("Not in a session. Call `connect()` first.")
        if self._storage is None:
            raise ValueError("`StorageConfig` was not passed to `DerpConfig`.")
        return self._storage

    @property
    def auth(self) -> AuthClient[UserT]:
        """Get the auth service."""
        if not self._in_session:
            raise ValueError("Not in a session. Call `connect()` first.")
        if self._auth is None:
            raise ValueError("`AuthConfig` was not passed to `DerpConfig`.")
        return self._auth
