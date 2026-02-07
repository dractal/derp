"""Integration tests for CLI commands."""

from __future__ import annotations

import json
import os
from pathlib import Path

from typer.testing import CliRunner

from derp.cli.main import app

runner = CliRunner()


class TestInitCommand:
    """Tests for the init command."""

    def test_init_creates_config(self, temp_dir: Path):
        """Test that init creates a derp.toml file."""
        os.chdir(temp_dir)

        result = runner.invoke(app, ["init"])

        assert result.exit_code == 0
        assert "Created derp.toml" in result.stdout

        config_path = temp_dir / "derp.toml"
        assert config_path.exists()

        content = config_path.read_text()
        assert "[database]" in content
        assert "[database.migrations]" in content
        assert "[payments]" in content
        assert "STRIPE_SECRET_KEY" in content

    def test_init_fails_if_exists(self, temp_dir: Path):
        """Test that init fails if config already exists."""
        os.chdir(temp_dir)

        # Create initial config
        (temp_dir / "derp.toml").write_text("[database]\n")

        result = runner.invoke(app, ["init"])

        assert result.exit_code == 1
        assert "already exists" in result.stdout

    def test_init_force_overwrites(self, temp_dir: Path):
        """Test that init --force overwrites existing config."""
        os.chdir(temp_dir)

        # Create initial config
        (temp_dir / "derp.toml").write_text("[database]\n")

        result = runner.invoke(app, ["init", "--force"])

        assert result.exit_code == 0
        assert "Created derp.toml" in result.stdout


class TestGenerateCommand:
    """Tests for the generate command."""

    def test_generate_creates_migration(self, cli_env: dict):
        """Test that generate creates migration files."""
        result = runner.invoke(app, ["generate", "--name", "initial"])

        assert result.exit_code == 0
        assert "Created migration:" in result.stdout

        # Check that migration folder was created
        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        folders = list(migrations_dir.glob("0000_*"))
        assert len(folders) == 1

        migration_folder = folders[0]
        assert (migration_folder / "migration.sql").exists()
        assert (migration_folder / "snapshot.json").exists()

        # Check journal was created
        journal_path = migrations_dir / "journal.json"
        assert journal_path.exists()

        journal = json.loads(journal_path.read_text())
        assert len(journal["entries"]) == 1
        assert journal["entries"][0]["tag"] == "initial"

    def test_generate_no_changes(self, cli_env: dict):
        """Test generate when schema is up to date."""
        # First generate
        result = runner.invoke(app, ["generate", "--name", "initial"])
        assert result.exit_code == 0

        # Second generate should show no changes
        result = runner.invoke(app, ["generate", "--name", "second"])
        assert result.exit_code == 0
        assert "No changes detected" in result.stdout

    def test_generate_custom_migration(self, cli_env: dict):
        """Test generating a custom (empty) migration."""
        result = runner.invoke(app, ["generate", "--name", "custom_change", "--custom"])

        assert result.exit_code == 0
        assert "Created custom migration:" in result.stdout

        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        folders = list(migrations_dir.glob("0000_*"))
        assert len(folders) == 1

        sql_content = (folders[0] / "migration.sql").read_text()
        assert "Custom migration" in sql_content


class TestMigrateCommand:
    """Tests for the migrate command."""

    def test_migrate_applies_migrations(self, cli_env: dict):
        """Test that migrate applies pending migrations."""
        # Generate a migration first
        result = runner.invoke(app, ["generate", "--name", "initial"])
        assert result.exit_code == 0

        # Apply migrations
        result = runner.invoke(app, ["migrate"])

        assert result.exit_code == 0
        assert "Applied" in result.stdout or "Applied 1 migration" in result.stdout

    def test_migrate_no_pending(self, cli_env: dict):
        """Test migrate when no migrations are pending."""
        # Generate and apply
        runner.invoke(app, ["generate", "--name", "initial"])
        runner.invoke(app, ["migrate"])

        # Try to migrate again
        result = runner.invoke(app, ["migrate"])

        assert result.exit_code == 0
        assert "No pending migrations" in result.stdout

    def test_migrate_dry_run(self, cli_env: dict):
        """Test migrate --dry-run shows SQL without executing."""
        runner.invoke(app, ["generate", "--name", "initial"])

        result = runner.invoke(app, ["migrate", "--dry-run"])

        assert result.exit_code == 0
        assert "Dry run complete" in result.stdout


class TestStatusCommand:
    """Tests for the status command."""

    def test_status_no_migrations(self, cli_env: dict):
        """Test status when no migrations exist."""
        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "No migrations found" in result.stdout

    def test_status_shows_pending(self, cli_env: dict):
        """Test status shows pending migrations."""
        runner.invoke(app, ["generate", "--name", "initial"])

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "[ ]" in result.stdout  # Pending marker
        assert "initial" in result.stdout
        assert "1 pending" in result.stdout

    def test_status_shows_applied(self, cli_env: dict):
        """Test status shows applied migrations."""
        runner.invoke(app, ["generate", "--name", "initial"])
        runner.invoke(app, ["migrate"])

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "[x]" in result.stdout  # Applied marker
        assert "1 applied" in result.stdout


class TestPushCommand:
    """Tests for the push command."""

    def test_push_applies_changes(self, cli_env: dict):
        """Test push applies schema changes directly."""
        result = runner.invoke(app, ["push", "--force"])

        assert result.exit_code == 0
        assert (
            "Schema pushed successfully" in result.stdout
            or "No changes" in result.stdout
        )

    def test_push_dry_run(self, cli_env: dict):
        """Test push --dry-run shows SQL without executing."""
        result = runner.invoke(app, ["push", "--dry-run"])

        assert result.exit_code == 0
        assert "Dry run complete" in result.stdout or "No changes" in result.stdout


class TestPullCommand:
    """Tests for the pull command."""

    def test_pull_creates_snapshot(self, cli_env: dict):
        """Test pull creates a snapshot from database."""
        # First push some schema
        runner.invoke(app, ["push", "--force"])

        result = runner.invoke(app, ["pull"])

        assert result.exit_code == 0
        assert "Introspected database" in result.stdout
        assert "Snapshot saved to" in result.stdout

    def test_pull_with_migration_flag(self, cli_env: dict):
        """Test pull --migration creates a migration entry."""
        runner.invoke(app, ["push", "--force"])

        result = runner.invoke(app, ["pull", "--migration", "--name", "baseline"])

        assert result.exit_code == 0
        assert "Created migration:" in result.stdout

        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        journal_path = migrations_dir / "journal.json"
        assert journal_path.exists()


class TestCheckCommand:
    """Tests for the check command."""

    def test_check_no_snapshots(self, cli_env: dict):
        """Test check fails when no snapshots exist."""
        result = runner.invoke(app, ["check"])

        assert result.exit_code == 1
        # Message is output to stderr, check combined output
        assert "No snapshots found" in result.output

    def test_check_schema_synced(self, cli_env: dict):
        """Test check passes when schema is synced."""
        runner.invoke(app, ["generate", "--name", "initial"])

        result = runner.invoke(app, ["check"])

        assert result.exit_code == 0
        assert "Schema is up to date" in result.stdout


class TestDropCommand:
    """Tests for the drop command."""

    def test_drop_migration(self, cli_env: dict):
        """Test dropping a specific migration."""
        runner.invoke(app, ["generate", "--name", "initial"])

        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        folders_before = list(migrations_dir.glob("0000_*"))
        assert len(folders_before) == 1

        result = runner.invoke(app, ["drop", "0000", "--force"])

        assert result.exit_code == 0
        assert "Deleted" in result.stdout or "dropped" in result.stdout.lower()

        folders_after = list(migrations_dir.glob("0000_*"))
        assert len(folders_after) == 0

    def test_drop_all_migrations(self, cli_env: dict):
        """Test dropping all migrations."""
        runner.invoke(app, ["generate", "--name", "first"])
        # Manually create second migration by modifying schema
        runner.invoke(app, ["generate", "--name", "second", "--custom"])

        result = runner.invoke(app, ["drop", "--all", "--force"])

        assert result.exit_code == 0
        assert "All migrations dropped" in result.stdout

        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        folders = list(migrations_dir.glob("[0-9]*_*"))
        assert len(folders) == 0


class TestVersionCommand:
    """Tests for the version command."""

    def test_version_shows_info(self, temp_dir: Path):
        """Test version command shows version info."""
        os.chdir(temp_dir)

        result = runner.invoke(app, ["version"])

        assert result.exit_code == 0
        assert "derp version" in result.stdout
        assert "Drizzle-compatible" in result.stdout
