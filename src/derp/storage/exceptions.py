"""Custom exceptions for storage integration."""


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
