"""S3-compatible storage client using aiobotocore."""

from __future__ import annotations

from types import TracebackType
from typing import Any

import aiobotocore.session
from aiobotocore.client import AioBaseClient
from botocore.config import Config
from botocore.exceptions import ClientError

from derp.config import StorageConfig


class StorageClient:
    """S3-compatible storage client for uploading and fetching files.

    Example:
        config = StorageConfig(
            endpoint_url="https://s3.amazonaws.com",
            access_key_id="key",
            secret_access_key="secret",
        )
        storage = StorageClient(config)

        async with storage:
            await storage.upload_file("local.txt", "remote.txt")
            content = await storage.fetch_file("remote.txt")

        # Or manual lifecycle
        await storage.connect()
        await storage.upload_file("local.txt", "remote.txt")
        await storage.disconnect()
    """

    def __init__(self, config: StorageConfig):
        """Initialize Storage client.

        Args:
            config: Storage configuration.
        """
        self._config = config
        self._session: aiobotocore.session.AioSession | None = None
        self._client: AioBaseClient | None = None

    async def connect(self) -> None:
        """Establish connection to S3."""
        if self._client is not None:
            return

        self._session = aiobotocore.session.get_session()

        config = Config(
            region_name=self._config.region,
            signature_version="s3v4",
        )

        self._client = await self._session.create_client(
            self._config.service_name,
            endpoint_url=self._config.endpoint_url,
            aws_access_key_id=self._config.access_key_id,
            aws_secret_access_key=self._config.secret_access_key,
            aws_session_token=self._config.session_token,
            use_ssl=self._config.use_ssl,
            verify=self._config.verify,
            config=config,
        ).__aenter__()

    async def disconnect(self) -> None:
        """Close connection to S3."""
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None
        if self._session is not None:
            self._session = None

    async def __aenter__(self) -> StorageClient:
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
    def client(self) -> AioBaseClient:
        """Get the S3 client."""
        if self._client is None:
            raise RuntimeError(
                "Storage not connected. Call connect() or use async context manager."
            )
        return self._client
    
    def get_url(self, *, bucket: str, key: str) -> str:
        """Get the URL for a file in S3.

        Args:
            bucket: Name of the S3 bucket.
            key: S3 object key (path in bucket).

        Returns:
            URL for the file.
        """
        return f"{self._config.endpoint_url}/{bucket}/{key}"

    async def upload_file(
        self,
        *,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str | None = None,
        metadata: dict[str, str] | None = None,
        extra_args: dict[str, Any] | None = None,
    ) -> None:
        """Upload a file to S3.

        Args:
            bucket: Name of the S3 bucket.
            key: S3 object key (path in bucket).
            data: Bytes to upload.
            content_type: MIME type of the file.
            metadata: Metadata to attach to the object.
            extra_args: Additional arguments to pass to put_object.

        Example:
            await storage.upload_file(
                bucket="my-bucket",
                key="remote/file.txt",
                data=b"Hello, World!",
                content_type="text/plain",
                metadata={"author": "user123"},
            )
        """
        put_kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Key": key,
            "Body": data,
        }

        if content_type:
            put_kwargs["ContentType"] = content_type

        if metadata:
            put_kwargs["Metadata"] = metadata

        if extra_args:
            put_kwargs.update(extra_args)

        await self.client.put_object(**put_kwargs)

    async def fetch_file(self, *, bucket: str, key: str) -> bytes:
        """Fetch a file from S3.

        Args:
            bucket: Name of the S3 bucket.
            key: S3 object key (path in bucket).

        Returns:
            File content as bytes

        Example:
            # Get content as bytes
            content = await storage.fetch_file("remote/file.txt")

            # Save to local file
            await storage.fetch_file("remote/file.txt")
        """
        response = await self.client.get_object(
            Bucket=bucket,
            Key=key,
        )

        async with response["Body"] as stream:
            content = await stream.read()

        return content

    async def delete_file(self, *, bucket: str, key: str) -> None:
        """Delete a file from S3.

        Args:
            key: S3 object key (path in bucket)

        Example:
            await storage.delete_file("remote/file.txt")
        """
        await self.client.delete_object(Bucket=bucket, Key=key)

    async def file_exists(self, *, bucket: str, key: str) -> bool:
        """Check if a file exists in S3.

        Args:
            key: S3 object key (path in bucket)

        Returns:
            True if file exists, False otherwise

        Example:
            exists = await storage.file_exists("remote/file.txt")
        """
        try:
            await self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "")
            if error_code in ("404", "NoSuchKey"):
                return False
            # Re-raise other client errors
            raise

    async def list_files(
        self, *, bucket: str, prefix: str = "", max_keys: int | None = None
    ) -> list[str]:
        """List files in S3 bucket.

        Args:
            bucket: Name of the S3 bucket.
            prefix: Prefix to filter files by.
            max_keys: Maximum number of keys to return.

        Returns:
            List of object keys.

        Example:
            files = await storage.list_files(bucket="my-bucket", prefix="folder/")
        """
        list_kwargs: dict[str, Any] = {
            "Bucket": bucket,
        }

        if prefix:
            list_kwargs["Prefix"] = prefix

        if max_keys:
            list_kwargs["MaxKeys"] = max_keys

        response = await self.client.list_objects_v2(**list_kwargs)

        if "Contents" not in response:
            return []

        return [obj["Key"] for obj in response["Contents"]]
