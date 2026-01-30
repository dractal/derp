"""Configuration handling for Derp CLI."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

import typer

CONFIG_FILE = "derp.toml"
MIGRATIONS_TABLE = "_derp_migrations"
DEFAULT_DATABASE_URL_ENV = "DATABASE_URL"
DEFAULT_MIGRATIONS_DIR = "./drizzle"


class ConfigError(Exception):
    """Configuration error."""


@dataclass
class DatabaseConfig:
    """Database configuration."""

    env: str = DEFAULT_DATABASE_URL_ENV

    def get_url(self) -> str:
        """Get database URL from the configured environment variable."""
        url = os.environ.get(self.env)
        if not url:
            typer.echo(
                f"Error: {self.env} environment variable not set.\n"
                f"Set it with: export {self.env}=postgresql://user:pass@localhost:5432/mydb",
                err=True,
            )
            raise typer.Exit(1)
        return url


@dataclass
class MigrationsConfig:
    """Migrations configuration."""

    dir: str = DEFAULT_MIGRATIONS_DIR
    schema: str | None = None
    out: str = DEFAULT_MIGRATIONS_DIR
    breakpoints: bool = True
    strict: bool = False
    verbose: bool = False

    @property
    def directory(self) -> Path:
        """Get migrations directory as Path."""
        return Path(self.dir)

    @property
    def output_directory(self) -> Path:
        """Get output directory as Path."""
        return Path(self.out)

    def get_schema_path(self) -> str:
        """Get schema modules path, raising error if not configured."""
        if not self.schema:
            typer.echo(
                "Error: migrations.schema not configured in derp.toml\n"
                "Add: schema = \"src/schema.py\" to [migrations] section",
                err=True,
            )
            raise typer.Exit(1)
        return self.schema


@dataclass
class IntrospectConfig:
    """Introspection configuration."""

    schemas: list[str] = field(default_factory=lambda: ["public"])
    tables: list[str] | None = None  # None = all tables
    exclude_tables: list[str] = field(default_factory=lambda: ["_derp_migrations"])


@dataclass
class Config:
    """Complete Derp configuration."""

    database: DatabaseConfig
    migrations: MigrationsConfig
    introspect: IntrospectConfig = field(default_factory=IntrospectConfig)

    @classmethod
    def load(cls) -> Config:
        """Load configuration from derp.toml."""
        config_path = Path(CONFIG_FILE)

        if not config_path.exists():
            typer.echo(
                f"Error: {CONFIG_FILE} not found in current directory.\n"
                "Run 'derp init' to create one, or create it manually:\n"
                "\n"
                "[database]\n"
                'env = "DATABASE_URL"\n'
                "\n"
                "[migrations]\n"
                'dir = "./drizzle"\n'
                'schema = "src/schema.py"\n',
                err=True,
            )
            raise typer.Exit(1)

        with open(config_path, "rb") as f:
            data = tomllib.load(f)

        # Parse database config
        db_data = data.get("database", {})
        database = DatabaseConfig(
            env=db_data.get("env", DEFAULT_DATABASE_URL_ENV),
        )

        # Parse migrations config
        mig_data = data.get("migrations", {})
        migrations = MigrationsConfig(
            dir=mig_data.get("dir", DEFAULT_MIGRATIONS_DIR),
            schema=mig_data.get("schema"),
            out=mig_data.get("out", mig_data.get("dir", DEFAULT_MIGRATIONS_DIR)),
            breakpoints=mig_data.get("breakpoints", True),
            strict=mig_data.get("strict", False),
            verbose=mig_data.get("verbose", False),
        )

        # Parse introspect config
        intro_data = data.get("introspect", {})
        introspect = IntrospectConfig(
            schemas=intro_data.get("schemas", ["public"]),
            tables=intro_data.get("tables"),
            exclude_tables=intro_data.get("exclude_tables", ["_derp_migrations"]),
        )

        return cls(
            database=database,
            migrations=migrations,
            introspect=introspect,
        )


def create_default_config() -> str:
    """Return default configuration file content."""
    return """\
[database]
env = "DATABASE_URL"  # Environment variable containing the database URL

[migrations]
dir = "./drizzle"      # Directory for migration files
schema = "src/schema.py"  # Path to your schema module
# out = "./drizzle"    # Output directory (defaults to dir)
# breakpoints = true   # Add SQL breakpoints
# strict = false       # Strict mode (fail on warnings)
# verbose = false      # Verbose output

[introspect]
schemas = ["public"]   # Schemas to introspect
exclude_tables = ["_derp_migrations"]  # Tables to exclude
"""
