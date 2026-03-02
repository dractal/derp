"""Custom exceptions for queue integration."""

from __future__ import annotations


class QueueError(Exception):
    """Base exception for all queue errors."""

    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.message = message
        self.code = code or "queue_error"


class QueueNotConnectedError(QueueError):
    """Raised when queue client is used before connect()."""

    def __init__(self, message: str = "Queue not connected. Call connect() first."):
        super().__init__(message, code="queue_not_connected")


class QueueProviderError(QueueError):
    """Raised when the queue backend returns an error."""

    def __init__(
        self,
        message: str = "Queue provider request failed",
        code: str | None = None,
    ):
        super().__init__(message, code=code or "queue_provider_error")
