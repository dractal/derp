"""Custom exceptions for the ai module."""

from __future__ import annotations


class AIError(Exception):
    """Base exception for all ai errors."""

    def __init__(self, message: str, code: str | None = None):
        super().__init__(message)
        self.message = message
        self.code = code or "ai_error"


class FalMissingCredentialsError(AIError):
    """Raised when FAL credentials are missing."""

    def __init__(self, message: str = "FAL credentials are missing"):
        super().__init__(message, code="fal_missing_credentials")


class FalJobAlreadyCompletedError(AIError):
    """Raised when trying to cancel a fal job that already completed."""

    def __init__(self, message: str = "Job already completed."):
        super().__init__(message, code="fal_job_already_completed")


class FalJobNotFoundError(AIError):
    """Raised when a fal job is not found."""

    def __init__(self, message: str = "Job not found."):
        super().__init__(message, code="fal_job_not_found")


class FalJobFailedError(AIError):
    """Raised when a fal job fails."""

    def __init__(self, message: str = "Fal job failed."):
        super().__init__(message, code="fal_job_failed")


class ModalNotConnectedError(AIError):
    """Raised when Modal client is used before connect()."""

    def __init__(
        self,
        message: str = (
            "Modal client not connected. Call connect() first and "
            "make sure `[ai.modal]` is set in the configuration."
        ),
    ):
        super().__init__(message, code="modal_not_connected")
