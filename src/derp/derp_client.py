"""Derp client for interacting with database, file storage, and more."""

from __future__ import annotations

from types import TracebackType
from typing import Self

from derp.auth import AuthClient, BaseUser
from derp.auth.email import EmailClient
from derp.config import DerpConfig
from derp.kv.base import KVClient
from derp.kv.valkey import ValkeyClient
from derp.orm import DatabaseEngine
from derp.payments import PaymentsClient
from derp.storage import StorageClient


class DerpClient[UserT: BaseUser]:
    """Derp client for interacting with database, file storage, and more."""

    def __init__(self, config: DerpConfig):
        self._config: DerpConfig = config
        self._db: DatabaseEngine = DatabaseEngine(
            config.database.db_url,
            min_size=config.database.pool_min_size,
            max_size=config.database.pool_max_size,
        )
        self._replica_db: DatabaseEngine | None = (
            DatabaseEngine(
                config.database.replica_url,
                min_size=(
                    config.database.replica_pool_min_size
                    or config.database.pool_min_size
                ),
                max_size=(
                    config.database.replica_pool_max_size
                    or config.database.pool_max_size
                ),
            )
            if config.database.replica_url is not None
            else None
        )
        self._email: EmailClient | None = (
            EmailClient(self._config.email) if self._config.email is not None else None
        )
        self._storage: StorageClient | None = (
            StorageClient(self._config.storage)
            if self._config.storage is not None
            else None
        )
        self._auth: AuthClient[UserT] | None = (
            AuthClient[UserT](self._config.auth, self._config.database.schema_path)
            if self._config.auth is not None
            else None
        )
        self._kv: KVClient | None = (
            ValkeyClient(self._config.kv.valkey)
            if self._config.kv is not None and self._config.kv.valkey is not None
            else None
        )
        self._payments: PaymentsClient | None = (
            PaymentsClient(self._config.payments)
            if self._config.payments is not None
            else None
        )
        self._in_session = False

        if self._email is None and self._auth is not None:
            raise ValueError(
                "The email client needs to be configured for authentication to work."
                "Please make sure to configure `EmailConfig` when `AuthConfig` is "
                "configured via `derp.toml` or `DerpConfig`."
            )

    async def connect(self) -> None:
        """Start a session."""
        await self._db.connect()
        if self._replica_db is not None:
            await self._replica_db.connect()
        if self._storage is not None:
            await self._storage.connect()
        if self._kv is not None:
            await self._kv.connect()
        if self._payments is not None:
            await self._payments.connect()
        if self._auth is not None:
            self._auth.set_db(self._db)
            self._auth.set_email(self._email)
            if self._kv is not None:
                self._auth.set_kv(self._kv)
        if self._kv is not None:
            self._db.set_cache(self._kv)
            if self._replica_db is not None:
                self._replica_db.set_cache(self._kv)

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
        if self._kv is not None:
            await self._kv.disconnect()
        if self._payments is not None:
            await self._payments.disconnect()
        if self._auth is not None:
            self._auth.set_db(None)
            self._auth.set_email(None)
            self._auth.set_kv(None)
        self._db.set_cache(None)
        if self._replica_db is not None:
            self._replica_db.set_cache(None)

        self._in_session = False

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.disconnect()

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
    def email(self) -> EmailClient:
        """Get the email client."""
        if not self._in_session:
            raise ValueError("Not in a session. Call `connect()` first.")
        if self._email is None:
            raise ValueError("`EmailConfig` was not passed to `DerpConfig`.")
        return self._email

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

    @property
    def kv(self) -> KVClient:
        """Get the KV client."""
        if not self._in_session:
            raise ValueError("Not in a session. Call `connect()` first.")
        if self._kv is None:
            raise ValueError("`KVConfig` was not passed to `DerpConfig`.")
        return self._kv

    @property
    def payments(self) -> PaymentsClient:
        """Get the payments client."""
        if not self._in_session:
            raise ValueError("Not in a session. Call `connect()` first.")
        if self._payments is None:
            raise ValueError("`PaymentsConfig` was not passed to `DerpConfig`.")
        return self._payments

    @property
    def config(self) -> DerpConfig:
        """Get the Derp configuration."""
        return self._config
