"""Tests for the storage client."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError
from moto.moto_server.threaded_moto_server import ThreadedMotoServer

from derp.config import StorageConfig
from derp.storage.client import StorageClient
from derp.storage.exceptions import StorageNotConnectedError

REGION = "us-east-1"
BUCKET = "test-bucket"


@pytest.fixture(scope="session")
def moto_server():
    """Start a moto server for the entire test session."""
    server = ThreadedMotoServer(port=0, verbose=False)
    server.start()
    yield f"http://localhost:{server._server.server_address[1]}"  # ty:ignore[possibly-missing-attribute]
    server.stop()


@pytest.fixture(autouse=True)
def _reset_moto(moto_server):
    """Reset moto state before each test."""
    import requests

    requests.post(f"{moto_server}/moto-api/reset")
    yield


@pytest.fixture
def storage_config(moto_server: str) -> StorageConfig:
    return StorageConfig(
        endpoint_url=moto_server,
        access_key_id="testing",
        secret_access_key="testing",
        region=REGION,
        use_ssl=False,
        verify=False,
    )


@pytest.fixture
async def storage(storage_config: StorageConfig):
    """Connected storage client with a test bucket created."""
    async with StorageClient(storage_config) as client:
        await client._client.create_bucket(Bucket=BUCKET)  # type: ignore[union-attr]
        yield client


# ── Lifecycle ────────────────────────────────────────────────────


class TestLifecycle:
    @pytest.mark.asyncio
    async def test_connect_and_disconnect(self, storage_config: StorageConfig) -> None:
        client = StorageClient(storage_config)
        assert client._client is None

        await client.connect()
        assert client._client is not None

        await client.disconnect()
        assert client._client is None
        assert client._session is None

    @pytest.mark.asyncio
    async def test_connect_is_idempotent(self, storage_config: StorageConfig) -> None:
        client = StorageClient(storage_config)
        await client.connect()
        first_client = client._client

        await client.connect()
        assert client._client is first_client

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        await client.disconnect()  # should not raise

    @pytest.mark.asyncio
    async def test_async_context_manager(self, storage_config: StorageConfig) -> None:
        async with StorageClient(storage_config) as client:
            assert client._client is not None
        assert client._client is None


# ── get_url ──────────────────────────────────────────────────────


class TestGetUrl:
    def test_returns_url(self, storage_config: StorageConfig) -> None:
        client = StorageClient(storage_config)
        url = client.get_url(bucket="my-bucket", key="path/to/file.txt")
        assert url == f"{storage_config.endpoint_url}/my-bucket/path/to/file.txt"

    def test_raises_without_endpoint_url(self) -> None:
        config = StorageConfig(
            access_key_id="key",
            secret_access_key="secret",
        )
        client = StorageClient(config)
        with pytest.raises(ValueError, match="endpoint_url"):
            client.get_url(bucket="b", key="k")


# ── upload_file / fetch_file ─────────────────────────────────────


class TestUploadAndFetchFile:
    @pytest.mark.asyncio
    async def test_upload_and_fetch_roundtrip(self, storage: StorageClient) -> None:
        data = b"hello world"
        await storage.upload_file(bucket=BUCKET, key="doc.txt", data=data)

        result = await storage.fetch_file(bucket=BUCKET, key="doc.txt")
        assert result == data

    @pytest.mark.asyncio
    async def test_upload_with_content_type_and_metadata(
        self, storage: StorageClient
    ) -> None:
        await storage.upload_file(
            bucket=BUCKET,
            key="image.png",
            data=b"\x89PNG",
            content_type="image/png",
            metadata={"author": "alice"},
        )

        meta = await storage.head_object(bucket=BUCKET, key="image.png")
        assert meta["content_type"] == "image/png"
        assert meta["metadata"]["author"] == "alice"

    @pytest.mark.asyncio
    async def test_upload_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.upload_file(bucket=BUCKET, key="k", data=b"x")

    @pytest.mark.asyncio
    async def test_fetch_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.fetch_file(bucket=BUCKET, key="k")

    @pytest.mark.asyncio
    async def test_fetch_nonexistent_key_raises(self, storage: StorageClient) -> None:
        with pytest.raises(ClientError):
            await storage.fetch_file(bucket=BUCKET, key="no-such-key")


# ── delete_file ──────────────────────────────────────────────────


class TestDeleteFile:
    @pytest.mark.asyncio
    async def test_delete_removes_object(self, storage: StorageClient) -> None:
        await storage.upload_file(bucket=BUCKET, key="tmp.txt", data=b"data")
        assert await storage.file_exists(bucket=BUCKET, key="tmp.txt") is True

        await storage.delete_file(bucket=BUCKET, key="tmp.txt")
        assert await storage.file_exists(bucket=BUCKET, key="tmp.txt") is False

    @pytest.mark.asyncio
    async def test_delete_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.delete_file(bucket=BUCKET, key="k")


# ── delete_files ─────────────────────────────────────────────────


class TestDeleteFiles:
    @pytest.mark.asyncio
    async def test_deletes_multiple_objects(self, storage: StorageClient) -> None:
        for key in ["a.txt", "b.txt", "c.txt"]:
            await storage.upload_file(bucket=BUCKET, key=key, data=b"x")

        deleted = await storage.delete_files(bucket=BUCKET, keys=["a.txt", "b.txt"])
        assert sorted(deleted) == ["a.txt", "b.txt"]
        assert await storage.file_exists(bucket=BUCKET, key="a.txt") is False
        assert await storage.file_exists(bucket=BUCKET, key="b.txt") is False
        assert await storage.file_exists(bucket=BUCKET, key="c.txt") is True

    @pytest.mark.asyncio
    async def test_empty_keys_returns_empty_list(self, storage: StorageClient) -> None:
        deleted = await storage.delete_files(bucket=BUCKET, keys=[])
        assert deleted == []

    @pytest.mark.asyncio
    async def test_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.delete_files(bucket=BUCKET, keys=["k"])


# ── copy_file ────────────────────────────────────────────────────


class TestCopyFile:
    @pytest.mark.asyncio
    async def test_copies_within_same_bucket(self, storage: StorageClient) -> None:
        await storage.upload_file(bucket=BUCKET, key="orig.txt", data=b"data")

        await storage.copy_file(
            src_bucket=BUCKET, src_key="orig.txt", dst_key="copy.txt"
        )

        assert await storage.fetch_file(bucket=BUCKET, key="copy.txt") == b"data"
        # original still exists
        assert await storage.file_exists(bucket=BUCKET, key="orig.txt") is True

    @pytest.mark.asyncio
    async def test_copies_across_buckets(self, storage: StorageClient) -> None:
        other_bucket = "other-bucket"
        await storage._client.create_bucket(Bucket=other_bucket)  # type: ignore[union-attr]
        await storage.upload_file(bucket=BUCKET, key="src.txt", data=b"cross")

        await storage.copy_file(
            src_bucket=BUCKET,
            src_key="src.txt",
            dst_bucket=other_bucket,
            dst_key="dst.txt",
        )

        assert await storage.fetch_file(bucket=other_bucket, key="dst.txt") == b"cross"

    @pytest.mark.asyncio
    async def test_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.copy_file(
                src_bucket=BUCKET,
                src_key="a",
                dst_bucket=BUCKET,
                dst_key="b",
            )


# ── file_exists ──────────────────────────────────────────────────


class TestFileExists:
    @pytest.mark.asyncio
    async def test_returns_true_for_existing_key(self, storage: StorageClient) -> None:
        await storage.upload_file(bucket=BUCKET, key="exists.txt", data=b"yes")
        assert await storage.file_exists(bucket=BUCKET, key="exists.txt") is True

    @pytest.mark.asyncio
    async def test_returns_false_for_missing_key(self, storage: StorageClient) -> None:
        assert await storage.file_exists(bucket=BUCKET, key="nope.txt") is False

    @pytest.mark.asyncio
    async def test_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.file_exists(bucket=BUCKET, key="k")


# ── head_object ──────────────────────────────────────────────────


class TestHeadObject:
    @pytest.mark.asyncio
    async def test_returns_metadata_dict(self, storage: StorageClient) -> None:
        await storage.upload_file(
            bucket=BUCKET,
            key="info.txt",
            data=b"content",
            content_type="text/plain",
            metadata={"tag": "test"},
        )

        meta = await storage.head_object(bucket=BUCKET, key="info.txt")
        assert meta["content_type"] == "text/plain"
        assert meta["content_length"] == 7
        assert isinstance(meta["last_modified"], str)
        assert isinstance(meta["etag"], str)
        assert meta["metadata"]["tag"] == "test"

    @pytest.mark.asyncio
    async def test_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.head_object(bucket=BUCKET, key="k")


# ── list_files ───────────────────────────────────────────────────


class TestListFiles:
    @pytest.mark.asyncio
    async def test_lists_all_keys(self, storage: StorageClient) -> None:
        await storage.upload_file(bucket=BUCKET, key="a.txt", data=b"a")
        await storage.upload_file(bucket=BUCKET, key="b.txt", data=b"b")

        keys = await storage.list_files(bucket=BUCKET)
        assert sorted(keys) == ["a.txt", "b.txt"]

    @pytest.mark.asyncio
    async def test_filters_by_prefix(self, storage: StorageClient) -> None:
        await storage.upload_file(bucket=BUCKET, key="dir/a.txt", data=b"a")
        await storage.upload_file(bucket=BUCKET, key="dir/b.txt", data=b"b")
        await storage.upload_file(bucket=BUCKET, key="other.txt", data=b"c")

        keys = await storage.list_files(bucket=BUCKET, prefix="dir/")
        assert sorted(keys) == ["dir/a.txt", "dir/b.txt"]

    @pytest.mark.asyncio
    async def test_respects_max_keys(self, storage: StorageClient) -> None:
        for i in range(5):
            await storage.upload_file(bucket=BUCKET, key=f"f{i}.txt", data=b"x")

        keys = await storage.list_files(bucket=BUCKET, max_keys=2)
        assert len(keys) == 2

    @pytest.mark.asyncio
    async def test_empty_bucket_returns_empty_list(
        self, storage: StorageClient
    ) -> None:
        keys = await storage.list_files(bucket=BUCKET)
        assert keys == []

    @pytest.mark.asyncio
    async def test_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.list_files(bucket=BUCKET)


# ── list_buckets ─────────────────────────────────────────────────


class TestListBuckets:
    @pytest.mark.asyncio
    async def test_lists_created_buckets(self, storage: StorageClient) -> None:
        buckets = await storage.list_buckets()
        names = [b["name"] for b in buckets]
        assert BUCKET in names
        assert all("creation_date" in b for b in buckets)

    @pytest.mark.asyncio
    async def test_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.list_buckets()


# ── list_objects ─────────────────────────────────────────────────


class TestListObjects:
    @pytest.mark.asyncio
    async def test_returns_objects_and_prefixes(self, storage: StorageClient) -> None:
        await storage.upload_file(bucket=BUCKET, key="root.txt", data=b"r")
        await storage.upload_file(bucket=BUCKET, key="dir/nested.txt", data=b"n")

        result = await storage.list_objects(bucket=BUCKET)
        keys = [o["key"] for o in result["objects"]]
        assert "root.txt" in keys
        assert "dir/" in result["prefixes"]

    @pytest.mark.asyncio
    async def test_prefix_scoping(self, storage: StorageClient) -> None:
        await storage.upload_file(bucket=BUCKET, key="a/1.txt", data=b"1")
        await storage.upload_file(bucket=BUCKET, key="a/2.txt", data=b"2")
        await storage.upload_file(bucket=BUCKET, key="b/3.txt", data=b"3")

        result = await storage.list_objects(bucket=BUCKET, prefix="a/")
        keys = [o["key"] for o in result["objects"]]
        assert sorted(keys) == ["a/1.txt", "a/2.txt"]
        assert result["prefixes"] == []

    @pytest.mark.asyncio
    async def test_empty_bucket(self, storage: StorageClient) -> None:
        result = await storage.list_objects(bucket=BUCKET)
        assert result == {"objects": [], "prefixes": []}

    @pytest.mark.asyncio
    async def test_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.list_objects(bucket=BUCKET)


# ── signed_download_url ─────────────────────────────────────────


class TestSignedDownloadUrl:
    @pytest.mark.asyncio
    async def test_returns_url_string(self, storage: StorageClient) -> None:
        await storage.upload_file(bucket=BUCKET, key="file.txt", data=b"hello")

        url = await storage.signed_download_url(bucket=BUCKET, key="file.txt")

        assert isinstance(url, str)
        assert BUCKET in url
        assert "file.txt" in url

    @pytest.mark.asyncio
    async def test_default_expiry_is_3600(self, storage: StorageClient) -> None:
        await storage.upload_file(bucket=BUCKET, key="file.txt", data=b"hello")

        url = await storage.signed_download_url(bucket=BUCKET, key="file.txt")

        # moto embeds the expiry as X-Amz-Expires in the query string
        assert "X-Amz-Expires=3600" in url

    @pytest.mark.asyncio
    async def test_custom_expiry(self, storage: StorageClient) -> None:
        await storage.upload_file(bucket=BUCKET, key="file.txt", data=b"hello")

        url = await storage.signed_download_url(
            bucket=BUCKET, key="file.txt", expires_in=600
        )

        assert "X-Amz-Expires=600" in url

    @pytest.mark.asyncio
    async def test_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.signed_download_url(bucket=BUCKET, key="file.txt")


# ── signed_upload_url ───────────────────────────────────────────


class TestSignedUploadUrl:
    @pytest.mark.asyncio
    async def test_returns_url_string(self, storage: StorageClient) -> None:
        url = await storage.signed_upload_url(bucket=BUCKET, key="new-file.txt")

        assert isinstance(url, str)
        assert BUCKET in url
        assert "new-file.txt" in url

    @pytest.mark.asyncio
    async def test_default_expiry_is_3600(self, storage: StorageClient) -> None:
        url = await storage.signed_upload_url(bucket=BUCKET, key="file.txt")

        assert "X-Amz-Expires=3600" in url

    @pytest.mark.asyncio
    async def test_custom_expiry(self, storage: StorageClient) -> None:
        url = await storage.signed_upload_url(
            bucket=BUCKET, key="file.txt", expires_in=300
        )

        assert "X-Amz-Expires=300" in url

    @pytest.mark.asyncio
    async def test_with_content_type(self, storage: StorageClient) -> None:
        url = await storage.signed_upload_url(
            bucket=BUCKET,
            key="image.png",
            content_type="image/png",
        )

        assert isinstance(url, str)
        assert "image.png" in url

    @pytest.mark.asyncio
    async def test_raises_when_not_connected(
        self, storage_config: StorageConfig
    ) -> None:
        client = StorageClient(storage_config)
        with pytest.raises(StorageNotConnectedError):
            await client.signed_upload_url(bucket=BUCKET, key="file.txt")
