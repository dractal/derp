"""Derp CLI - Drizzle-style migration management for Derp ORM.

This CLI provides PostgreSQL migration tools matching Drizzle's approach:
- Snapshot-based schema diffing
- JSON statement intermediate representation
- Forward-only migrations
- Interactive safety prompts for destructive operations
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from dotenv import load_dotenv

from derp.cli.commands.check import check
from derp.cli.commands.drop import drop
from derp.cli.commands.env import env_app
from derp.cli.commands.generate import generate
from derp.cli.commands.migrate import migrate
from derp.cli.commands.pull import pull
from derp.cli.commands.push import push
from derp.cli.commands.rollback import rollback
from derp.cli.commands.status import status
from derp.cli.commands.studio import studio, studio_dev
from derp.cli.scaffolding.infra import (
    WELL_LIT_SERVICES,
    InfraChoices,
    ServiceMode,
    generate_infra_scaffold,
)
from derp.cli.scaffolding.infra.specs import (
    ServiceModeChoice,
    default_yes_choices,
)
from derp.config import CONFIG_FILE, create_default_config

app = typer.Typer(
    name="derp",
    help="Derp ORM - A strongly-typed async Python ORM for PostgreSQL",
    no_args_is_help=True,
)


load_dotenv(".env")

# Register commands
app.command()(generate)
app.command()(migrate)
app.command()(push)
app.command()(pull)
app.command()(status)
app.command()(check)
app.command()(drop)
app.command()(rollback)
app.command()(studio)
app.command()(studio_dev)
app.add_typer(env_app, name="env")


def _prompt_modes(*, yes: bool, app_name: str) -> InfraChoices:
    """Drive the interactive Provision / Import / BYO / Skip prompt loop.

    With ``yes=True``, skips prompts and returns greenfield defaults.
    """
    if yes:
        return default_yes_choices(app_name)

    typer.echo("")
    typer.echo("For each service, pick how Terraform should handle it:")
    typer.echo("  provision = create new resource")
    typer.echo("  import    = adopt an existing resource into Terraform")
    typer.echo("  byo       = bring your own (Terraform only relays the credential)")
    typer.echo("  skip      = don't include this service")
    typer.echo("")

    choices: list[ServiceModeChoice] = []
    for spec in WELL_LIT_SERVICES:
        allowed = [m.value for m in spec.allowed_modes]
        default = (
            ServiceMode.PROVISION.value
            if ServiceMode.PROVISION in spec.allowed_modes
            else ServiceMode.BYO.value
        )
        prompt = f"{spec.label} [{'/'.join(allowed)}]"
        raw = typer.prompt(prompt, default=default)
        if raw not in allowed:
            raise typer.BadParameter(
                f"Invalid mode {raw!r} for {spec.label}. "
                f"Allowed: {', '.join(allowed)}.",
            )
        choices.append(ServiceModeChoice(service=spec.key, mode=ServiceMode(raw)))

    return InfraChoices(app_name=app_name, modes=tuple(choices))


@app.command()
def init(
    name: Annotated[
        str | None,
        typer.Argument(
            help=(
                "App name for the well-lit-path scaffold (used by --infra). "
                "Defaults to the current directory name."
            ),
        ),
    ] = None,
    force: Annotated[
        bool, typer.Option("--force", "-f", help="Overwrite existing files")
    ] = False,
    infra: Annotated[
        bool,
        typer.Option(
            "--infra",
            help=(
                "Also scaffold the well-lit-path Terraform tree under `infra/`. "
                "Interactive unless --yes is set."
            ),
        ),
    ] = False,
    yes: Annotated[
        bool,
        typer.Option(
            "--yes",
            "-y",
            help=(
                "Skip interactive prompts when using --infra and accept "
                "greenfield-Provision defaults."
            ),
        ),
    ] = False,
) -> None:
    """Initialize a new derp.toml configuration file.

    Without --infra, creates a vanilla config with all sections commented
    out (existing behavior). With --infra, runs an interactive prompt loop
    (or --yes for greenfield defaults), generates a tailored derp.toml that
    matches your selections, and scaffolds the dev/prod Terraform tree.
    """
    project_root = Path.cwd()
    config_path = project_root / CONFIG_FILE

    if not infra:
        if config_path.exists() and not force:
            typer.echo(f"{CONFIG_FILE} already exists. Use --force to overwrite.")
            raise typer.Exit(1)

        config_path.write_text(create_default_config())
        typer.echo(f"Created {CONFIG_FILE}")
        typer.echo("")
        typer.echo("Next steps:")
        typer.echo("")
        typer.echo("1. Set your database URL:")
        typer.echo(
            "   export DATABASE_URL=postgresql://user:pass@localhost:5432/dbname",
        )
        typer.echo("")
        typer.echo("2. Update derp.toml with your schema path:")
        typer.echo('   schema_path = "app/*"')
        typer.echo("")
        typer.echo("3. Generate your first migration:")
        typer.echo("   derp generate --name initial")
        typer.echo("")
        typer.echo("4. Apply migrations:")
        typer.echo("   derp migrate")
        return

    # --infra path
    if config_path.exists() and not force:
        typer.echo(
            f"{CONFIG_FILE} already exists. Use --force to overwrite both "
            "derp.toml and any conflicting files under infra/."
        )
        raise typer.Exit(1)

    app_name = name or project_root.name
    if not app_name or app_name == "/":
        raise typer.BadParameter(
            "Could not infer an app name from the current directory. "
            "Pass one as the first argument: `derp init my-app --infra`."
        )

    choices = _prompt_modes(yes=yes, app_name=app_name)

    try:
        written = generate_infra_scaffold(project_root, choices, force=force)
    except FileExistsError as exc:
        typer.echo(
            f"File already exists: {exc}. Use --force to overwrite.",
            err=True,
        )
        raise typer.Exit(1) from exc

    typer.echo("")
    typer.echo(f"Scaffolded well-lit-path infra for `{app_name}`:")
    for path in written:
        typer.echo(f"  {path.relative_to(project_root)}")
    typer.echo("")
    typer.echo("Next steps:")
    typer.echo("")
    typer.echo("1. Fill in vendor credentials:")
    typer.echo("   cp infra/dev/terraform.tfvars.example infra/dev/terraform.tfvars")
    typer.echo("   $EDITOR infra/dev/terraform.tfvars")
    typer.echo("")
    typer.echo("2. Provision dev resources:")
    typer.echo("   cd infra/dev && terraform init && terraform apply")
    typer.echo("")
    typer.echo("3. Sync env vars into a local file:")
    typer.echo("   derp env pull dev")
    typer.echo("")
    typer.echo("4. Run your app:")
    typer.echo("   uv run uvicorn app.main:app --env-file .env.dev")
    typer.echo("")
    typer.echo("Repeat for prod when ready (use separate live keys).")


@app.command()
def version() -> None:
    """Show version information."""
    try:
        from importlib.metadata import version as get_version

        ver = get_version("derp-py")
    except Exception:
        ver = "unknown"

    typer.echo(f"derp version {ver}")


if __name__ == "__main__":
    app()
