"""Password hashing and validation."""

from __future__ import annotations

import abc
import re
import secrets
from dataclasses import dataclass

from derp.config import PasswordConfig


class PasswordHasher(abc.ABC):
    """Abstract base class for password hashers."""

    @abc.abstractmethod
    def hash(self, password: str) -> str:
        """Hash a password."""

    @abc.abstractmethod
    def verify(self, password: str, hashed: str) -> bool:
        """Verify a password against a hash."""

    @abc.abstractmethod
    def needs_rehash(self, hashed: str) -> bool:
        """Check if a hash needs to be rehashed (e.g., algorithm upgrade)."""


class Argon2Hasher(PasswordHasher):
    """Password hasher using Argon2id (recommended)."""

    def __init__(
        self,
        time_cost: int = 3,
        memory_cost: int = 65536,
        parallelism: int = 4,
    ):
        try:
            from argon2 import PasswordHasher as Argon2PasswordHasher
            from argon2.exceptions import InvalidHashError, VerifyMismatchError
        except ImportError as e:
            raise ImportError(
                "argon2-cffi is required for Argon2 hashing. "
                "Install it with: pip install argon2-cffi"
            ) from e

        self._hasher = Argon2PasswordHasher(
            time_cost=time_cost,
            memory_cost=memory_cost,
            parallelism=parallelism,
        )
        self._verify_mismatch_error = VerifyMismatchError
        self._invalid_hash_error = InvalidHashError

    def hash(self, password: str) -> str:
        """Hash a password using Argon2id."""
        return self._hasher.hash(password)

    def verify(self, password: str, hashed: str) -> bool:
        """Verify a password against an Argon2 hash."""
        try:
            self._hasher.verify(hashed, password)
            return True
        except (self._verify_mismatch_error, self._invalid_hash_error):
            return False

    def needs_rehash(self, hashed: str) -> bool:
        """Check if the hash needs to be updated with new parameters."""
        return self._hasher.check_needs_rehash(hashed)


@dataclass
class PasswordValidationResult:
    """Result of password validation."""

    valid: bool
    errors: list[str]


def validate_password(
    config: PasswordConfig, password: str
) -> PasswordValidationResult:
    errors: list[str] = []
    if len(password) < config.min_length:
        errors.append(f"Password must be at least {config.min_length} characters")

    if len(password) > config.max_length:
        errors.append(f"Password must be at most {config.max_length} characters")

    if config.require_uppercase and not re.search(r"[A-Z]", password):
        errors.append("Password must contain at least one uppercase letter")

    if config.require_lowercase and not re.search(r"[a-z]", password):
        errors.append("Password must contain at least one lowercase letter")

    if config.require_digit and not re.search(r"\d", password):
        errors.append("Password must contain at least one digit")

    if config.require_special and not re.search(r"[!@#$%^&*(),.?\":{}|<>]", password):
        errors.append("Password must contain at least one special character")

    return PasswordValidationResult(valid=len(errors) == 0, errors=errors)


def generate_secure_token(length: int = 32) -> str:
    """Generate a cryptographically secure random token."""
    return secrets.token_urlsafe(length)
