"""Tests for password hashing and validation."""

from __future__ import annotations

from derp.auth.password import Argon2Hasher, generate_secure_token, validate_password
from derp.config import PasswordConfig


class TestArgon2Hasher:
    """Tests for Argon2 password hasher."""

    def test_hash_password(self) -> None:
        """Test that hash produces a valid hash string."""
        hasher = Argon2Hasher()
        password = "test_password_123"
        hashed = hasher.hash(password)

        assert hashed != password
        assert hashed.startswith("$argon2")

    def test_verify_correct_password(self) -> None:
        """Test verification of correct password."""
        hasher = Argon2Hasher()
        password = "test_password_123"
        hashed = hasher.hash(password)

        assert hasher.verify(password, hashed) is True

    def test_verify_wrong_password(self) -> None:
        """Test verification of wrong password."""
        hasher = Argon2Hasher()
        password = "test_password_123"
        hashed = hasher.hash(password)

        assert hasher.verify("wrong_password", hashed) is False

    def test_verify_invalid_hash(self) -> None:
        """Test verification with invalid hash."""
        hasher = Argon2Hasher()
        assert hasher.verify("password", "invalid_hash") is False

    def test_different_passwords_different_hashes(self) -> None:
        """Test that different passwords produce different hashes."""
        hasher = Argon2Hasher()
        hash1 = hasher.hash("password1")
        hash2 = hasher.hash("password2")

        assert hash1 != hash2

    def test_same_password_different_hashes(self) -> None:
        """Test that same password produces different hashes (salt)."""
        hasher = Argon2Hasher()
        hash1 = hasher.hash("password")
        hash2 = hasher.hash("password")

        assert hash1 != hash2


class TestValidatePassword:
    """Tests for password validation."""

    def test_valid_password(self) -> None:
        """Test validation of a valid password."""
        config = PasswordConfig(min_length=8)
        result = validate_password(config, "password123")

        assert result.valid is True
        assert result.errors == []

    def test_too_short(self) -> None:
        """Test validation of too short password."""
        config = PasswordConfig(min_length=8)
        result = validate_password(config, "short")

        assert result.valid is False
        assert len(result.errors) == 1
        assert "at least 8 characters" in result.errors[0]

    def test_too_long(self) -> None:
        """Test validation of too long password."""
        config = PasswordConfig(max_length=10)
        result = validate_password(config, "a" * 15)

        assert result.valid is False
        assert len(result.errors) == 1
        assert "at most 10 characters" in result.errors[0]

    def test_require_uppercase(self) -> None:
        """Test uppercase requirement."""
        config = PasswordConfig(min_length=1, require_uppercase=True)
        result = validate_password(config, "lowercase")

        assert result.valid is False
        assert "uppercase" in result.errors[0]

        result = validate_password(config, "Uppercase")
        assert result.valid is True

    def test_require_lowercase(self) -> None:
        """Test lowercase requirement."""
        config = PasswordConfig(min_length=1, require_lowercase=True)
        result = validate_password(config, "UPPERCASE")

        assert result.valid is False
        assert "lowercase" in result.errors[0]

        result = validate_password(config, "LOWERCASEa")
        assert result.valid is True

    def test_require_digit(self) -> None:
        """Test digit requirement."""
        config = PasswordConfig(min_length=1, require_digit=True)

        result = validate_password(config, "nodigits")
        assert result.valid is False
        assert "digit" in result.errors[0]

        result = validate_password(config, "digits123")
        assert result.valid is True

    def test_require_special(self) -> None:
        """Test special character requirement."""
        config = PasswordConfig(min_length=1, require_special=True)

        result = validate_password(config, "nospecial")
        assert result.valid is False
        assert "special" in result.errors[0]

        result = validate_password(config, "special!")
        assert result.valid is True

    def test_multiple_requirements(self) -> None:
        """Test multiple requirements at once."""
        config = PasswordConfig(
            min_length=8,
            require_uppercase=True,
            require_lowercase=True,
            require_digit=True,
        )

        result = validate_password(config, "weak")
        assert result.valid is False
        assert len(result.errors) == 3  # length, upper, digit ("weak" has lowercase)

        result = validate_password(config, "Strong123")
        assert result.valid is True


class TestGenerateSecureToken:
    """Tests for secure token generation."""

    def test_generates_token(self) -> None:
        """Test that a token is generated."""
        token = generate_secure_token()
        assert token is not None
        assert len(token) > 0

    def test_tokens_are_unique(self) -> None:
        """Test that tokens are unique."""
        tokens = [generate_secure_token() for _ in range(100)]
        assert len(set(tokens)) == 100

    def test_custom_length(self) -> None:
        """Test token generation with custom length."""
        token = generate_secure_token(64)
        # URL-safe base64 encoding produces ~4/3 the length
        assert len(token) > 64
