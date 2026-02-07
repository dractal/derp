"""Custom exceptions for payments integration."""

from __future__ import annotations


class PaymentsError(Exception):
    """Base exception for all payments errors."""

    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.message = message
        self.code = code or "payments_error"


class PaymentsProviderError(PaymentsError):
    """Raised when Stripe returns an error."""

    def __init__(
        self,
        message: str = "Payments provider request failed",
        code: str | None = None,
    ):
        super().__init__(message, code=code or "payments_provider_error")


class PaymentsNotConnectedError(PaymentsError):
    """Raised when payments client is used before connect()."""

    def __init__(self, message: str = "Payments not connected. Call connect() first."):
        super().__init__(message, code="payments_not_connected")


class WebhookSignatureError(PaymentsError):
    """Raised when webhook verification fails."""

    def __init__(self, message: str = "Invalid webhook signature"):
        super().__init__(message, code="webhook_signature_error")
