"""Password hashing and validation."""

from __future__ import annotations

import abc
import asyncio
import concurrent.futures
import dataclasses
import re
import secrets

from etils import epy

from derp.config import PasswordConfig

with epy.lazy_imports():
    import argon2
    import argon2.exceptions as argon2_exceptions


class PasswordHasher(abc.ABC):
    """Abstract base class for password hashers."""

    def __init__(self, *, max_workers: int = 8) -> None:
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="password-hasher"
        )

    @abc.abstractmethod
    def hash(self, password: str) -> str:
        """Hash a password synchronously."""

    @abc.abstractmethod
    def verify(self, password: str, hashed: str) -> bool:
        """Verify a password synchronously."""

    @abc.abstractmethod
    def needs_rehash(self, hashed: str) -> bool:
        """Check if a hash needs to be rehashed (e.g., algorithm upgrade)."""

    async def async_hash(self, password: str) -> str:
        """Hash a password without blocking the event loop."""
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, self.hash, password
        )

    async def async_verify(self, password: str, hashed: str) -> bool:
        """Verify a password without blocking the event loop."""
        return await asyncio.get_running_loop().run_in_executor(
            self._executor, self.verify, password, hashed
        )


class Argon2Hasher(PasswordHasher):
    """Password hasher using Argon2id (recommended)."""

    def __init__(
        self,
        time_cost: int = 3,
        memory_cost: int = 65536,
        parallelism: int = 4,
        *,
        max_workers: int = 8,
    ):
        super().__init__(max_workers=max_workers)
        self._hasher = argon2.PasswordHasher(
            time_cost=time_cost,
            memory_cost=memory_cost,
            parallelism=parallelism,
        )

    def hash(self, password: str) -> str:
        """Hash a password using Argon2id."""
        return self._hasher.hash(password)

    def verify(self, password: str, hashed: str) -> bool:
        """Verify a password against an Argon2 hash."""
        try:
            self._hasher.verify(hashed, password)
            return True
        except (
            argon2_exceptions.VerifyMismatchError,
            argon2_exceptions.InvalidHashError,
        ):
            return False

    def needs_rehash(self, hashed: str) -> bool:
        """Check if the hash needs to be updated with new parameters."""
        return self._hasher.check_needs_rehash(hashed)


@dataclasses.dataclass
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
