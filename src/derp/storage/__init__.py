"""Storage wrapper for S3-compatible object storage.

Example usage:

    from storage import Storage

    async with Storage(
        bucket_name="my-bucket",
        endpoint_url="https://s3.amazonaws.com",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
    ) as storage:
        # Upload a file
        await storage.upload_file("path/to/local/file.txt", "remote/file.txt")

        # Fetch a file
        content = await storage.fetch_file("remote/file.txt")
"""

from .client import StorageClient, StorageConfig

__all__ = ["StorageClient", "StorageConfig"]
