"""Derp client for interacting with database, file storage, and more."""

from __future__ import annotations

import datetime
from types import TracebackType
from typing import Self

from derp.ai import AIClient
from derp.auth.base import BaseAuthClient
from derp.auth.clerk_client import ClerkAuthClient
from derp.auth.cognito_client import CognitoAuthClient
from derp.auth.email import EmailClient
from derp.auth.native_client import NativeAuthClient
from derp.auth.supabase_client import SupabaseAuthClient
from derp.auth.workos_client import WorkOSAuthClient
from derp.config import DerpConfig
from derp.kv.base import KVClient
from derp.kv.valkey import ValkeyClient
from derp.orm import DatabaseEngine
from derp.orm.router import ReplicaRouter
from derp.payments import PaymentsClient
from derp.queue.base import QueueClient, Schedule, ScheduleType
from derp.queue.celery import CeleryQueueClient
from derp.queue.vercel import VercelQueueClient
from derp.storage import StorageClient


class DerpClient:
    """Derp client for interacting with database, file storage, and more."""

    def __init__(self, config: DerpConfig):
        self._config: DerpConfig = config
        self._db: DatabaseEngine = DatabaseEngine(
            config.database.db_url,
            min_size=config.database.pool_min_size,
            max_size=config.database.pool_max_size,
            statement_cache_size=config.database.statement_cache_size,
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
                statement_cache_size=config.database.replica_statement_cache_size,
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
        self._auth: BaseAuthClient | None = None
        if self._config.auth is not None:
            if self._config.auth.native is not None:
                self._auth = NativeAuthClient(self._config.auth.native)
            elif self._config.auth.clerk is not None:
                self._auth = ClerkAuthClient(self._config.auth.clerk)
            elif self._config.auth.cognito is not None:
                self._auth = CognitoAuthClient(self._config.auth.cognito)
            elif self._config.auth.supabase is not None:
                self._auth = SupabaseAuthClient(self._config.auth.supabase)
            elif self._config.auth.workos is not None:
                self._auth = WorkOSAuthClient(self._config.auth.workos)
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
        self._queue: QueueClient | None = None
        if self._config.queue is not None:
            if self._config.queue.celery is not None:
                self._queue = CeleryQueueClient(self._config.queue.celery)
            elif self._config.queue.vercel is not None:
                self._queue = VercelQueueClient(self._config.queue.vercel)
            if self._queue is not None and self._config.queue.schedules:
                self._queue.register_schedules(
                    [
                        Schedule(
                            name=sc.name,
                            task=sc.task,
                            type=(
                                ScheduleType.CRON if sc.cron else ScheduleType.INTERVAL
                            ),
                            cron=sc.cron,
                            interval=(
                                datetime.timedelta(seconds=sc.interval_seconds)
                                if sc.interval_seconds
                                else None
                            ),
                            payload=sc.payload,
                            queue=sc.queue,
                            path=sc.path,
                        )
                        for sc in self._config.queue.schedules
                    ]
                )
        self._ai: AIClient | None = (
            AIClient(self._config.ai) if self._config.ai is not None else None
        )
        self._router: ReplicaRouter | None = None
        self._in_session = False

        if (
            self._config.auth is not None
            and self._config.auth.native is not None
            and self._email is None
        ):
            raise ValueError(
                "The email client needs to be configured for native authentication "
                "to work. Please make sure to configure `EmailConfig` when "
                "`NativeAuthConfig` is configured via `derp.toml` or `DerpConfig`."
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
        if self._queue is not None:
            await self._queue.connect()
        if self._auth is not None:
            await self._auth.connect()
            self._auth.set_db(self._db)
            self._auth.set_email(self._email)
            if self._kv is not None:
                self._auth.set_kv(self._kv)
        if self._kv is not None:
            self._db.set_cache(self._kv)
        if self._ai is not None:
            await self._ai.connect()

        # Set up replica router if replica is configured
        if self._replica_db is not None:
            self._router = ReplicaRouter(
                self._db.pool,
                self._replica_db.pool,
                self._config.database,
            )
            await self._router.start()
            self._db.set_router(self._router)

        self._in_session = True

    async def disconnect(
        self,
    ) -> None:
        """End a session."""
        errors: list[Exception] = []

        # Stop router before closing pools
        if self._router is not None:
            try:
                await self._router.stop()
            except Exception as exc:
                errors.append(exc)
            self._db.set_router(None)
            self._router = None

        for client in [
            self._db,
            self._replica_db,
            self._storage,
            self._kv,
            self._payments,
            self._queue,
            self._ai,
        ]:
            if client is not None:
                try:
                    await client.disconnect()
                except Exception as exc:
                    errors.append(exc)

        if self._auth is not None:
            await self._auth.disconnect()
            self._auth.set_db(None)
            self._auth.set_email(None)
            self._auth.set_kv(None)
        self._db.set_cache(None)
        self._in_session = False

        if errors:
            raise ExceptionGroup("errors during disconnect", errors)

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
    def auth(self) -> BaseAuthClient:
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
    def queue(self) -> QueueClient:
        """Get the queue client."""
        if not self._in_session:
            raise ValueError("Not in a session. Call `connect()` first.")
        if self._queue is None:
            raise ValueError("`QueueConfig` was not passed to `DerpConfig`.")
        return self._queue

    @property
    def ai(self) -> AIClient:
        """Get the AI client."""
        if not self._in_session:
            raise ValueError("Not in a session. Call `connect()` first.")
        if self._ai is None:
            raise ValueError("`AIConfig` was not passed to `DerpConfig`.")
        return self._ai

    @property
    def config(self) -> DerpConfig:
        """Get the Derp configuration."""
        return self._config
