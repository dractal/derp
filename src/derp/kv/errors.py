"""Errors for KV stores."""

from __future__ import annotations


class KVError(Exception):
    """Base error for KV operations."""


class NotSupportedError(KVError):
    """Raised when a backend does not support an operation."""
