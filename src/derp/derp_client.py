"""Derp client for interacting with database, file storage, and more."""

from __future__ import annotations

import functools

from derp.auth import OAuth2Client
from derp.orm import DatabaseClient
from derp.storage import StorageClient


class DerpClient:
    """Derp client for interacting with database, file storage, and more."""

    def __init__(
        self,
        *,
        database_url: str | None = None,
        storage_endpoint_url: str | None = None,
        storage_service_name: str = "s3",
        storage_access_key_id: str | None = None,
        storage_secret_access_key: str | None = None,
        storage_session_token: str | None = None,
        storage_region: str = "auto",
        storage_use_ssl: bool = True,
        storage_verify: bool = True,
    ):
        self._database_url = database_url
        self._storage_endpoint_url = storage_endpoint_url
        self._storage_service_name = storage_service_name
        self._storage_access_key_id = storage_access_key_id
        self._storage_secret_access_key = storage_secret_access_key
        self._storage_session_token = storage_session_token
        self._storage_region = storage_region
        self._storage_use_ssl = storage_use_ssl
        self._storage_verify = storage_verify

    @functools.cached_property
    def db(self) -> DatabaseClient:
        """Get the database client."""
        if self._database_url is None:
            raise ValueError("Database URL is not set")
        return DatabaseClient(self._database_url)

    @functools.cached_property
    def storage(self) -> StorageClient:
        """Get the storage client."""
        if self._storage_endpoint_url is None:
            raise ValueError("Storage endpoint URL is not set")
        if self._storage_access_key_id is None:
            raise ValueError("Storage access key ID is not set")
        if self._storage_secret_access_key is None:
            raise ValueError("Storage secret access key is not set")

        return StorageClient(
            endpoint_url=self._storage_endpoint_url,
            access_key_id=self._storage_access_key_id,
            service_name=self._storage_service_name,
            secret_access_key=self._storage_secret_access_key,
            session_token=self._storage_session_token,
            region=self._storage_region,
            use_ssl=self._storage_use_ssl,
            verify=self._storage_verify,
        )

    @functools.cached_property
    def auth(self) -> OAuth2Client:
        """Get the auth client."""
        raise NotImplementedError("Derived class must implement auth property.")
