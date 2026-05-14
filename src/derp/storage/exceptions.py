"""Custom exceptions for storage integration.

The hierarchy is rooted at ``StorageError``. The S3 client normalises
``botocore.ClientError`` into these types so callers don't need to import
botocore to handle common failures (missing key, missing bucket, denied).

Conventions:

* Object-level lookups that don't find anything raise
  ``StorageObjectNotFoundError``. ``delete_file`` is the exception — S3
  deletes are idempotent, so it returns successfully for a missing key
  rather than raising.
* Bucket-level operations on a missing bucket raise
  ``StorageBucketNotFoundError``.
* 403 / ``AccessDenied`` collapses to ``StorageAccessDeniedError``.
* ``delete_files`` partial failures surface as
  ``StoragePartialDeleteError`` carrying the per-key reasons.
* Anything else from the backend (network, 5xx, unrecognised 4xx) is
  wrapped in ``StorageBackendError`` with the original exception chained.
"""

from __future__ import annotations


class StorageError(Exception):
    """Base exception for all storage errors."""

    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.message = message
        self.code = code or "storage_error"


class StorageNotConnectedError(StorageError):
    """Raised when storage client is used before connect()."""

    def __init__(self, message: str = "Storage not connected. Call connect() first."):
        super().__init__(message, code="storage_not_connected")


class StorageObjectNotFoundError(StorageError):
    """Raised when an S3 object lookup targets a missing key.

    The bucket and key are exposed as attributes for logging.
    """

    def __init__(self, bucket: str, key: str, message: str | None = None):
        super().__init__(
            message or f"Object {bucket!r}/{key!r} not found",
            code="storage_object_not_found",
        )
        self.bucket = bucket
        self.key = key


class StorageBucketNotFoundError(StorageError):
    """Raised when an operation targets a missing bucket."""

    def __init__(self, bucket: str, message: str | None = None):
        super().__init__(
            message or f"Bucket {bucket!r} not found",
            code="storage_bucket_not_found",
        )
        self.bucket = bucket


class StorageAccessDeniedError(StorageError):
    """Raised when S3 rejects a call with AccessDenied / 403."""

    def __init__(self, message: str = "Access denied by storage backend"):
        super().__init__(message, code="storage_access_denied")


class StoragePartialDeleteError(StorageError):
    """Raised when ``delete_objects`` reports per-key failures.

    The ``errors`` attribute is a list of ``{"key", "code", "message"}``
    dicts mirroring the S3 response. The ``deleted`` attribute lists keys
    that succeeded in the same batch.
    """

    def __init__(self, errors: list[dict[str, str]], deleted: list[str]):
        super().__init__(
            f"{len(errors)} of {len(errors) + len(deleted)} deletes failed",
            code="storage_partial_delete",
        )
        self.errors = errors
        self.deleted = deleted


class StorageBackendError(StorageError):
    """Wraps an unrecognised backend failure (network, 5xx, unknown 4xx).

    Always chains the original exception via ``raise ... from exc`` so the
    backend-specific type and traceback are preserved for debugging. The
    ``code`` attribute carries the S3 error code where available.
    """

    def __init__(self, message: str = "Storage backend error", code: str | None = None):
        super().__init__(message, code=code or "storage_backend_error")
