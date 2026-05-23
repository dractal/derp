"""S3-compatible storage client using aiobotocore."""

from __future__ import annotations

from types import TracebackType
from typing import Any

from botocore.config import Config
from botocore.exceptions import ClientError
from etils import epy

from derp.config import StorageConfig
from derp.storage.exceptions import (
    StorageAccessDeniedError,
    StorageBackendError,
    StorageBucketNotFoundError,
    StorageNotConnectedError,
    StorageObjectNotFoundError,
    StoragePartialDeleteError,
)

with epy.lazy_imports():
    import aiobotocore.client as aio_client
    import aiobotocore.session as aio_session


_NOT_FOUND_CODES = frozenset({"404", "NoSuchKey", "NotFound"})
_NO_SUCH_BUCKET_CODES = frozenset({"NoSuchBucket"})
_ACCESS_DENIED_CODES = frozenset({"403", "AccessDenied", "Forbidden"})


def _join_url(base_url: str, *parts: str) -> str:
    return "/".join([base_url.rstrip("/"), *(part.strip("/") for part in parts)])


def _error_code(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "") or ""


class StorageClient:
    """S3-compatible storage client for uploading and fetching files.

    Example::

        config = StorageConfig(
            endpoint_url="https://s3.amazonaws.com",
            access_key_id="key",
            secret_access_key="secret",
        )
        storage = StorageClient(config)
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
        self._session: aio_session.AioSession | None = None
        self._client: aio_client.AioBaseClient | None = None

    async def connect(self) -> None:
        """Establish connection to S3."""
        if self._client is not None:
            return

        self._session = aio_session.get_session()

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

    async def _call(
        self,
        op_name: str,
        *,
        bucket: str | None = None,
        key: str | None = None,
        **kwargs: Any,
    ) -> Any:
        """Invoke an aiobotocore operation with normalised error handling.

        ``bucket`` / ``key`` are used to build informative
        ``StorageObjectNotFoundError`` / ``StorageBucketNotFoundError``
        exceptions; they do not get passed through to the operation.
        Pass S3 arguments (``Bucket=...``, ``Key=...``) explicitly.
        """
        if self._client is None:
            raise StorageNotConnectedError()

        op = getattr(self._client, op_name)
        try:
            return await op(**kwargs)
        except ClientError as exc:
            code = _error_code(exc)
            if code in _NO_SUCH_BUCKET_CODES and bucket is not None:
                raise StorageBucketNotFoundError(bucket) from exc
            if code in _NOT_FOUND_CODES:
                if key is not None and bucket is not None:
                    raise StorageObjectNotFoundError(bucket, key) from exc
                if bucket is not None:
                    raise StorageBucketNotFoundError(bucket) from exc
            if code in _ACCESS_DENIED_CODES:
                raise StorageAccessDeniedError() from exc
            raise StorageBackendError(str(exc), code=code or None) from exc

    def get_url(self, *, bucket: str, key: str) -> str:
        """Gets the URL for a file in S3 (public or private).

        Args:
            bucket: Name of the S3 bucket.
            key: S3 object key (path in bucket).

        Returns:
            URL for the file.

        Raises:
            ValueError: If no public endpoint URL is configured for this bucket
                and endpoint_url is not configured.
        """
        public_url = self._config.public_urls.get(bucket)
        if public_url is not None:
            return _join_url(public_url, key)

        if self._config.endpoint_url is None:
            raise ValueError(
                "Cannot construct URL: no public URL is configured for bucket "
                f"{bucket!r} in `public_urls`, and `endpoint_url` is not "
                "configured in StorageConfig."
            )
        return _join_url(self._config.endpoint_url, bucket, key)

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

        Example::

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

        await self._call("put_object", bucket=bucket, key=key, **put_kwargs)

    async def fetch_file(self, *, bucket: str, key: str) -> bytes:
        """Fetch a file from S3.

        Args:
            bucket: Name of the S3 bucket.
            key: S3 object key (path in bucket).

        Returns:
            File content as bytes.

        Raises:
            StorageObjectNotFoundError: If ``key`` does not exist.
            StorageBucketNotFoundError: If ``bucket`` does not exist.
            StorageAccessDeniedError: If the backend denies the request.
            StorageBackendError: For any other backend failure.

        Example:
            content = await storage.fetch_file(
                bucket="my-bucket", key="remote/file.txt"
            )
        """
        response = await self._call(
            "get_object", bucket=bucket, key=key, Bucket=bucket, Key=key
        )

        async with response["Body"] as stream:
            content = await stream.read()

        return content

    async def delete_file(self, *, bucket: str, key: str) -> None:
        """Delete a file from S3.

        S3 deletes are idempotent: calling this for a missing key
        succeeds silently rather than raising.

        Args:
            bucket: Name of the S3 bucket.
            key: S3 object key (path in bucket).
        """
        await self._call(
            "delete_object", bucket=bucket, key=key, Bucket=bucket, Key=key
        )

    async def delete_files(self, *, bucket: str, keys: list[str]) -> list[str]:
        """Delete multiple files from S3 in a single request.

        Args:
            bucket: Name of the S3 bucket.
            keys: List of S3 object keys to delete.

        Returns:
            List of keys that were successfully deleted.

        Raises:
            StoragePartialDeleteError: If S3 reports per-key failures. The
                exception carries both the failed entries and the keys
                that succeeded in the same batch.
        """
        if not keys:
            return []

        response = await self._call(
            "delete_objects",
            bucket=bucket,
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in keys]},
        )

        deleted = [obj["Key"] for obj in response.get("Deleted", [])]
        errors_raw = response.get("Errors") or []
        if errors_raw:
            errors = [
                {
                    "key": str(e.get("Key", "")),
                    "code": str(e.get("Code", "")),
                    "message": str(e.get("Message", "")),
                }
                for e in errors_raw
            ]
            raise StoragePartialDeleteError(errors=errors, deleted=deleted)

        return deleted

    async def copy_file(
        self,
        *,
        src_bucket: str,
        src_key: str,
        dst_bucket: str | None = None,
        dst_key: str,
    ) -> None:
        """Copy an object between keys or buckets (server-side).

        Args:
            src_bucket: Source bucket name.
            src_key: Source object key.
            dst_bucket: Destination bucket name. Defaults to src_bucket.
            dst_key: Destination object key.

        Raises:
            StorageObjectNotFoundError: If the source object does not exist.
            StorageBucketNotFoundError: If either bucket does not exist.
        """
        await self._call(
            "copy_object",
            bucket=src_bucket,
            key=src_key,
            Bucket=dst_bucket or src_bucket,
            Key=dst_key,
            CopySource={"Bucket": src_bucket, "Key": src_key},
        )

    async def file_exists(self, *, bucket: str, key: str) -> bool:
        """Check if a file exists in S3.

        Returns ``False`` for a missing key; only true backend errors
        (denied, network, 5xx) raise.

        Args:
            bucket: Name of the S3 bucket.
            key: S3 object key (path in bucket).

        Returns:
            True if file exists, False otherwise.
        """
        try:
            await self._call(
                "head_object", bucket=bucket, key=key, Bucket=bucket, Key=key
            )
            return True
        except StorageObjectNotFoundError:
            return False

    async def head_object(self, *, bucket: str, key: str) -> dict[str, Any]:
        """Get object metadata without downloading the content.

        Args:
            bucket: Name of the S3 bucket.
            key: S3 object key (path in bucket).

        Returns:
            Dict with 'content_type', 'content_length', 'last_modified',
            'etag', and 'metadata' keys.

        Raises:
            StorageObjectNotFoundError: If ``key`` does not exist.
        """
        response = await self._call(
            "head_object", bucket=bucket, key=key, Bucket=bucket, Key=key
        )
        return {
            "content_type": response.get("ContentType", "application/octet-stream"),
            "content_length": response.get("ContentLength", 0),
            "last_modified": response["LastModified"].isoformat(),
            "etag": response.get("ETag", "").strip('"'),
            "metadata": response.get("Metadata", {}),
        }

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

        response = await self._call("list_objects_v2", bucket=bucket, **list_kwargs)

        if "Contents" not in response:
            return []

        return [obj["Key"] for obj in response["Contents"]]

    async def signed_download_url(
        self,
        *,
        bucket: str,
        key: str,
        expires_in: int = 3600,
        response_content_disposition: str | None = None,
        response_content_type: str | None = None,
    ) -> str:
        """Generate a presigned URL for downloading (GET) an object.

        Args:
            bucket: Name of the S3 bucket.
            key: S3 object key (path in bucket).
            expires_in: URL expiry in seconds (default 3600).
            response_content_disposition: Value for the ``Content-Disposition``
                response header the storage backend returns for this URL, e.g.
                ``'attachment; filename="report.pdf"'`` to force a download. The
                value is signed into the URL, so it can't be tampered with.
            response_content_type: Value for the ``Content-Type`` response header
                the storage backend returns for this URL.

        Returns:
            Presigned URL string.
        """
        params: dict[str, str] = {"Bucket": bucket, "Key": key}
        if response_content_disposition is not None:
            params["ResponseContentDisposition"] = response_content_disposition
        if response_content_type is not None:
            params["ResponseContentType"] = response_content_type

        return await self._call(
            "generate_presigned_url",
            bucket=bucket,
            key=key,
            ClientMethod="get_object",
            Params=params,
            ExpiresIn=expires_in,
        )

    async def signed_upload_url(
        self,
        *,
        bucket: str,
        key: str,
        expires_in: int = 3600,
        content_type: str | None = None,
    ) -> str:
        """Generate a presigned URL for uploading (PUT) an object.

        Args:
            bucket: Name of the S3 bucket.
            key: S3 object key (path in bucket).
            expires_in: URL expiry in seconds (default 3600).
            content_type: MIME type the uploader must use.

        Returns:
            Presigned URL string.
        """
        params: dict[str, str] = {"Bucket": bucket, "Key": key}
        if content_type:
            params["ContentType"] = content_type

        return await self._call(
            "generate_presigned_url",
            bucket=bucket,
            key=key,
            ClientMethod="put_object",
            Params=params,
            ExpiresIn=expires_in,
        )

    async def list_buckets(self) -> list[dict[str, Any]]:
        """List all S3 buckets.

        Returns:
            List of dicts with 'name' and 'creation_date' keys.
        """
        response = await self._call("list_buckets")
        return [
            {
                "name": b["Name"],
                "creation_date": b["CreationDate"].isoformat(),
            }
            for b in response.get("Buckets", [])
        ]

    async def list_objects(
        self,
        *,
        bucket: str,
        prefix: str = "",
        max_keys: int = 1000,
    ) -> dict[str, Any]:
        """List objects and common prefixes in a bucket.

        Uses ``Delimiter='/'`` for folder-like browsing.

        Args:
            bucket: Name of the S3 bucket.
            prefix: Prefix to filter by (use trailing ``/`` for folders).
            max_keys: Maximum number of keys to return.

        Returns:
            Dict with 'objects' (list of object metadata dicts) and
            'prefixes' (list of prefix strings representing folders).
        """
        list_kwargs: dict[str, Any] = {
            "Bucket": bucket,
            "Prefix": prefix,
            "Delimiter": "/",
            "MaxKeys": max_keys,
        }

        response = await self._call("list_objects_v2", bucket=bucket, **list_kwargs)

        objects = [
            {
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"].isoformat(),
            }
            for obj in response.get("Contents", [])
            if obj["Key"] != prefix
        ]

        prefixes = [p["Prefix"] for p in response.get("CommonPrefixes", [])]

        return {"objects": objects, "prefixes": prefixes}
