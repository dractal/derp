"""Integration tests for CLI commands."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import asyncpg
from typer.testing import CliRunner

from derp.cli.commands.migrate import _LOCK_KEY, _LOCK_NAMESPACE, _compute_hash
from derp.cli.main import app
from derp.config import MIGRATIONS_TABLE

runner = CliRunner()


def _query(db_url: str, sql: str, *args: object) -> list[asyncpg.Record]:
    """Run a SQL query against the test database."""

    async def _run() -> list[asyncpg.Record]:
        conn = await asyncpg.connect(db_url)
        try:
            return await conn.fetch(sql, *args)
        finally:
            await conn.close()

    return asyncio.run(_run())


def _table_exists(db_url: str, table_name: str) -> bool:
    """Check if a table exists in the public schema."""
    rows = _query(
        db_url,
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = 'public' AND table_name = $1",
        table_name,
    )
    return len(rows) > 0


def _make_custom_migration_executable(migrations_dir: Path, version: str) -> None:
    """Write real SQL into a custom migration so asyncpg can execute it."""
    folders = list(migrations_dir.glob(f"{version}_*"))
    assert len(folders) == 1
    folder = folders[0]

    (folder / "migration.sql").write_text(
        "-- custom\nCREATE TABLE IF NOT EXISTS _noop (id int);\n"
        "DROP TABLE IF EXISTS _noop;\n"
    )
    (folder / "down.sql").write_text("-- rollback custom\nSELECT 1;\n")


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
        assert "migrations_dir" in content
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

    def test_push_shows_no_changes_after_migrate(self, cli_env: dict):
        """After generate + migrate, push should see zero diff."""
        runner.invoke(app, ["generate", "--name", "initial"])
        result = runner.invoke(app, ["migrate"])
        assert result.exit_code == 0

        result = runner.invoke(app, ["push", "--force", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "No changes" in result.stdout

    def test_push_ignores_rls_changes_when_configured(self, cli_env: dict):
        """Push should ignore standalone RLS drift when ignore_rls is enabled."""
        result = runner.invoke(app, ["push", "--force"])
        assert result.exit_code == 0, result.output

        db_url = cli_env["TEST_DATABASE_URL"]
        _query(db_url, 'ALTER TABLE "users" ENABLE ROW LEVEL SECURITY')

        config_path = Path(cli_env["cwd"]) / "derp.toml"
        config_path.write_text(config_path.read_text() + "ignore_rls = true\n")

        result = runner.invoke(app, ["push", "--force", "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "No changes" in result.stdout


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


class TestGenerateDownSQL:
    """Tests for down.sql generation."""

    def test_generate_creates_down_sql(self, cli_env: dict):
        """Test that generate creates a down.sql file."""
        result = runner.invoke(app, ["generate", "--name", "initial"])
        assert result.exit_code == 0
        assert "down.sql" in result.stdout

        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        folders = list(migrations_dir.glob("0000_*"))
        assert len(folders) == 1

        down_sql = (folders[0] / "down.sql").read_text()
        assert "Rollback:" in down_sql
        # Initial migration creates tables, so down should drop them
        assert "DROP TABLE" in down_sql

    def test_down_sql_is_inverse_of_up(self, cli_env: dict):
        """Test that down.sql contains inverse operations (DROP for CREATE)."""
        result = runner.invoke(app, ["generate", "--name", "initial"])
        assert result.exit_code == 0

        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        folders = list(migrations_dir.glob("0000_*"))
        assert len(folders) == 1

        up_sql = (folders[0] / "migration.sql").read_text()
        down_sql = (folders[0] / "down.sql").read_text()

        # Up creates tables, down should drop them
        assert "CREATE TABLE" in up_sql
        assert "DROP TABLE" in down_sql


class TestHashValidation:
    """Tests for migration hash validation."""

    def test_migrate_stores_correct_hash(self, cli_env: dict):
        """Hash stored in derp_migrations must match _compute_hash of the SQL file."""
        db_url = cli_env["TEST_DATABASE_URL"]
        runner.invoke(app, ["generate", "--name", "initial"])
        result = runner.invoke(app, ["migrate"])
        assert result.exit_code == 0

        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        sql = (list(migrations_dir.glob("0000_*"))[0] / "migration.sql").read_text()
        expected_hash = _compute_hash(sql)

        rows = _query(
            db_url,
            f"SELECT hash FROM {MIGRATIONS_TABLE} WHERE version = $1",
            "0000",
        )
        assert len(rows) == 1
        assert rows[0]["hash"] == expected_hash

    def test_tampered_migration_blocks_all_progress(self, cli_env: dict):
        """A tampered migration should prevent any new migrations from running."""
        db_url = cli_env["TEST_DATABASE_URL"]

        runner.invoke(app, ["generate", "--name", "initial"])
        runner.invoke(app, ["migrate"])

        # Snapshot applied versions before tampering
        rows_before = _query(db_url, f"SELECT version FROM {MIGRATIONS_TABLE}")
        versions_before = {r["version"] for r in rows_before}

        # Tamper with the applied migration file
        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        folders = list(migrations_dir.glob("0000_*"))
        (folders[0] / "migration.sql").write_text(
            "-- tampered\nDROP TABLE users CASCADE;"
        )

        runner.invoke(app, ["generate", "--name", "second", "--custom"])

        result = runner.invoke(app, ["migrate"])
        assert result.exit_code == 1
        assert "modified" in result.output.lower()

        # No new migrations should have been applied
        rows_after = _query(db_url, f"SELECT version FROM {MIGRATIONS_TABLE}")
        assert {r["version"] for r in rows_after} == versions_before

        # Tampered SQL should NOT have been executed — tables still intact
        assert _table_exists(db_url, "users")

    def test_hash_check_passes_then_applies(self, cli_env: dict):
        """When hashes match, new migrations apply and store correct hashes."""
        db_url = cli_env["TEST_DATABASE_URL"]

        runner.invoke(app, ["generate", "--name", "initial"])
        runner.invoke(app, ["migrate"])

        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        runner.invoke(app, ["generate", "--name", "second", "--custom"])
        _make_custom_migration_executable(migrations_dir, "0001")

        result = runner.invoke(app, ["migrate"])
        assert result.exit_code == 0

        # Both migrations should have correct hashes
        rows = _query(
            db_url,
            f"SELECT version, hash FROM {MIGRATIONS_TABLE} ORDER BY id",
        )
        assert len(rows) == 2

        for row in rows:
            folder = list(migrations_dir.glob(f"{row['version']}_*"))[0]
            sql = (folder / "migration.sql").read_text()
            assert row["hash"] == _compute_hash(sql)


class TestAdvisoryLock:
    """Tests for advisory lock during migration."""

    def test_lock_released_after_successful_migrate(self, cli_env: dict):
        """Advisory lock should be free after a successful migration."""
        db_url = cli_env["TEST_DATABASE_URL"]
        runner.invoke(app, ["generate", "--name", "initial"])
        result = runner.invoke(app, ["migrate"])
        assert result.exit_code == 0

        # pg_try_advisory_lock returns true if the lock is available
        rows = _query(
            db_url,
            "SELECT pg_try_advisory_lock($1, $2) AS acquired",
            _LOCK_NAMESPACE,
            _LOCK_KEY,
        )
        assert rows[0]["acquired"] is True

        # Clean up the lock we just acquired
        _query(
            db_url,
            "SELECT pg_advisory_unlock($1, $2)",
            _LOCK_NAMESPACE,
            _LOCK_KEY,
        )

    def test_lock_released_after_failed_migrate(self, cli_env: dict):
        """Advisory lock should be released even when migration fails."""
        db_url = cli_env["TEST_DATABASE_URL"]

        runner.invoke(app, ["generate", "--name", "initial"])
        runner.invoke(app, ["migrate"])

        # Tamper to trigger hash mismatch
        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        folders = list(migrations_dir.glob("0000_*"))
        (folders[0] / "migration.sql").write_text("-- tampered\nSELECT 1;")
        runner.invoke(app, ["generate", "--name", "second", "--custom"])

        result = runner.invoke(app, ["migrate"])
        assert result.exit_code == 1

        # Lock should still be free despite the failure
        rows = _query(
            db_url,
            "SELECT pg_try_advisory_lock($1, $2) AS acquired",
            _LOCK_NAMESPACE,
            _LOCK_KEY,
        )
        assert rows[0]["acquired"] is True, "Lock was not released after failed migrate"

        _query(
            db_url,
            "SELECT pg_advisory_unlock($1, $2)",
            _LOCK_NAMESPACE,
            _LOCK_KEY,
        )


class TestRollbackCommand:
    """Tests for the rollback command."""

    def test_rollback_drops_tables(self, cli_env: dict):
        """Rollback should execute down.sql, actually removing tables."""
        db_url = cli_env["TEST_DATABASE_URL"]
        runner.invoke(app, ["generate", "--name", "initial"])
        runner.invoke(app, ["migrate"])

        assert _table_exists(db_url, "users")
        assert _table_exists(db_url, "posts")

        result = runner.invoke(app, ["rollback"])
        assert result.exit_code == 0

        # Tables should be gone
        assert not _table_exists(db_url, "users")
        assert not _table_exists(db_url, "posts")

        # Tracking row should be removed
        rows = _query(db_url, f"SELECT version FROM {MIGRATIONS_TABLE}")
        assert len(rows) == 0

    def test_rollback_dry_run_preserves_state(self, cli_env: dict):
        """Dry run should leave database completely unchanged."""
        db_url = cli_env["TEST_DATABASE_URL"]
        runner.invoke(app, ["generate", "--name", "initial"])
        runner.invoke(app, ["migrate"])

        result = runner.invoke(app, ["rollback", "--dry-run"])
        assert result.exit_code == 0
        assert "Dry run complete" in result.stdout

        # Tables and tracking row should still exist
        assert _table_exists(db_url, "users")
        assert _table_exists(db_url, "posts")
        rows = _query(db_url, f"SELECT version FROM {MIGRATIONS_TABLE}")
        assert len(rows) == 1

    def test_rollback_no_applied(self, cli_env: dict):
        """Test rollback when no migrations are applied."""
        runner.invoke(app, ["generate", "--name", "initial"])

        result = runner.invoke(app, ["rollback"])

        assert result.exit_code == 0
        assert "No applied migrations" in result.stdout

    def test_rollback_all_drops_everything(self, cli_env: dict):
        """Rollback --all should drop all tables and clear tracking."""
        db_url = cli_env["TEST_DATABASE_URL"]
        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        runner.invoke(app, ["generate", "--name", "initial"])
        runner.invoke(app, ["migrate"])
        runner.invoke(app, ["generate", "--name", "second", "--custom"])
        _make_custom_migration_executable(migrations_dir, "0001")
        runner.invoke(app, ["migrate"])

        # Both applied
        rows = _query(db_url, f"SELECT version FROM {MIGRATIONS_TABLE}")
        assert len(rows) == 2

        result = runner.invoke(app, ["rollback", "--all"])
        assert result.exit_code == 0

        # All tables and tracking rows gone
        assert not _table_exists(db_url, "users")
        assert not _table_exists(db_url, "posts")
        rows = _query(db_url, f"SELECT version FROM {MIGRATIONS_TABLE}")
        assert len(rows) == 0

    def test_rollback_then_migrate_recreates_tables(self, cli_env: dict):
        """Round-trip: tables dropped by rollback are recreated by migrate."""
        db_url = cli_env["TEST_DATABASE_URL"]
        runner.invoke(app, ["generate", "--name", "initial"])
        runner.invoke(app, ["migrate"])
        assert _table_exists(db_url, "users")

        runner.invoke(app, ["rollback"])
        assert not _table_exists(db_url, "users")

        result = runner.invoke(app, ["migrate"])
        assert result.exit_code == 0
        assert _table_exists(db_url, "users")
        assert _table_exists(db_url, "posts")

        rows = _query(db_url, f"SELECT version FROM {MIGRATIONS_TABLE}")
        assert len(rows) == 1

    def test_rollback_to_version_keeps_earlier_tables(self, cli_env: dict):
        """Rollback --to should leave earlier migrations' tables intact."""
        db_url = cli_env["TEST_DATABASE_URL"]
        migrations_dir = Path(cli_env["cwd"]) / "drizzle"
        runner.invoke(app, ["generate", "--name", "initial"])
        runner.invoke(app, ["migrate"])
        runner.invoke(app, ["generate", "--name", "second", "--custom"])
        _make_custom_migration_executable(migrations_dir, "0001")
        runner.invoke(app, ["migrate"])

        result = runner.invoke(app, ["rollback", "--to", "0000"])
        assert result.exit_code == 0

        # Initial migration's tables should survive
        assert _table_exists(db_url, "users")
        assert _table_exists(db_url, "posts")

        # Only version 0000 should remain in tracking
        rows = _query(db_url, f"SELECT version FROM {MIGRATIONS_TABLE}")
        assert len(rows) == 1
        assert rows[0]["version"] == "0000"


class TestVersionCommand:
    """Tests for the version command."""

    def test_version_shows_info(self, temp_dir: Path):
        """Test version command shows version info."""
        os.chdir(temp_dir)

        result = runner.invoke(app, ["version"])

        assert result.exit_code == 0
        assert "derp version" in result.stdout
