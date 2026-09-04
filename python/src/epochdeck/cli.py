from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path
from typing import Annotated

import typer

from epochdeck import __version__
from epochdeck._run import sync_spool
from epochdeck.backup import StorageRoots, backup_storage, restore_storage
from epochdeck.client import EpochDeckClient
from epochdeck.exporter import export_project
from epochdeck.public_api import Api
from epochdeck.wandb_importer import import_wandb_runs

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"epochdeck {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            "-V",
            callback=_version_callback,
            is_eager=True,
            help="Print version and exit.",
        ),
    ] = False,
) -> None:
    """Operate an EpochDeck client and server."""


@app.command()
def health(
    server_url: str = typer.Option(
        "http://127.0.0.1:8787",
        envvar="EPOCHDECK_SERVER_URL",
        help="EpochDeck server base URL.",
    ),
) -> None:
    """Check server and catalog health."""
    with EpochDeckClient(server_url) as client:
        result = client.health()
    typer.echo(
        json.dumps(
            {
                "service": result.service,
                "version": result.version,
                "status": result.status,
            },
            sort_keys=True,
        )
    )


@app.command("doctor")
def doctor_command(
    server_url: str = typer.Option(
        "http://127.0.0.1:8787",
        envvar="EPOCHDECK_SERVER_URL",
        help="EpochDeck server base URL.",
    ),
) -> None:
    """Show bounded server, queue, storage, and slow-request diagnostics."""
    with EpochDeckClient(server_url) as client:
        result = client.diagnostics()
    typer.echo(json.dumps(result, sort_keys=True))


@app.command()
def sync(
    directory: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            readable=True,
            help="Offline run spool directory containing run.json.",
        ),
    ],
    server_url: str | None = typer.Option(
        None,
        envvar="EPOCHDECK_SERVER_URL",
        help="Override the server URL recorded in the spool.",
    ),
    timeout: float = typer.Option(300.0, min=0.1, help="Maximum upload time in seconds."),
) -> None:
    """Upload and finish a durable offline run."""
    run_id = sync_spool(directory, server_url=server_url, timeout=timeout)
    typer.echo(run_id)


@app.command("projects")
def projects_command(
    server_url: str = typer.Option(
        "http://127.0.0.1:8787",
        envvar="EPOCHDECK_SERVER_URL",
        help="EpochDeck server base URL.",
    ),
    limit: int = typer.Option(100, min=1, max=200, help="Maximum projects to return."),
) -> None:
    """List projects as JSON."""
    with EpochDeckClient(server_url) as client:
        result = client.projects(limit=limit)
    typer.echo(json.dumps(result, sort_keys=True))


@app.command("runs")
def runs_command(
    project: str | None = typer.Option(None, help="Restrict results to one project."),
    state: str | None = typer.Option(None, help="Restrict state to running or finished."),
    name: str | None = typer.Option(None, help="Match one exact run name."),
    name_contains: str | None = typer.Option(None, help="Match a literal run-name substring."),
    config: Annotated[
        list[str] | None,
        typer.Option(help="Top-level config equality as KEY=JSON; repeat for multiple filters."),
    ] = None,
    summary: Annotated[
        list[str] | None,
        typer.Option(help="Top-level summary equality as KEY=JSON; repeat for multiple filters."),
    ] = None,
    limit: int = typer.Option(100, min=1, max=200, help="Maximum runs to return."),
    server_url: str = typer.Option(
        "http://127.0.0.1:8787",
        envvar="EPOCHDECK_SERVER_URL",
        help="EpochDeck server base URL.",
    ),
) -> None:
    """Query a bounded run page with explicit equality filters."""
    if state is not None and state not in {"running", "finished"}:
        raise typer.BadParameter("state must be 'running' or 'finished'", param_hint="--state")
    query = {
        "project": project,
        "state": state,
        "name": name,
        "name_contains": name_contains,
        "config_equals": _parse_json_filters(config or [], "config"),
        "summary_equals": _parse_json_filters(summary or [], "summary"),
        "limit": limit,
    }
    query = {key: value for key, value in query.items() if value is not None}
    with EpochDeckClient(server_url) as client:
        result = client.query_runs(query)
    typer.echo(json.dumps(result, sort_keys=True))


@app.command("get")
def get_command(
    run: str = typer.Argument(help="Run ID or project/run-id path."),
    server_url: str = typer.Option(
        "http://127.0.0.1:8787",
        envvar="EPOCHDECK_SERVER_URL",
        help="EpochDeck server base URL.",
    ),
) -> None:
    """Get one run as JSON."""
    with Api(server_url=server_url) as api:
        result = api.run(run).to_dict()
    typer.echo(json.dumps(result, sort_keys=True))


@app.command("history")
def history_command(
    run_id: Annotated[str, typer.Argument(help="Run ID.")],
    key: Annotated[
        list[str],
        typer.Option("--key", help="Metric key; repeat to select columns."),
    ],
    after: int | None = typer.Option(None, min=0, help="Exclusive sequence cursor."),
    limit: int = typer.Option(1_000, min=1, max=5_000, help="Maximum rows to return."),
    server_url: str = typer.Option(
        "http://127.0.0.1:8787",
        envvar="EPOCHDECK_SERVER_URL",
        help="EpochDeck server base URL.",
    ),
) -> None:
    """Read one bounded full-resolution metric-history page."""
    with EpochDeckClient(server_url) as client:
        result = client.history(run_id, keys=key, after=after, limit=limit)
    typer.echo(json.dumps(result, sort_keys=True))


@app.command("export")
def export_command(
    project: Annotated[str, typer.Argument(help="Project name to export.")],
    destination: Annotated[
        Path,
        typer.Argument(help="New directory to create as a portable export bundle."),
    ],
    server_url: str = typer.Option(
        "http://127.0.0.1:8787",
        envvar="EPOCHDECK_SERVER_URL",
        help="EpochDeck server base URL.",
    ),
    timeout: float = typer.Option(300.0, min=0.1, help="Per-request timeout in seconds."),
) -> None:
    """Export after every selected run is finished and project writers are quiesced.

    The opaque project mutation token is captured before traversal and verified
    afterward; any project-visible change aborts without publishing.
    """
    with EpochDeckClient(server_url, timeout=timeout) as client:
        manifest = export_project(client, project, destination)
    typer.echo(json.dumps(manifest, sort_keys=True))


@app.command("import-wandb")
def import_wandb_command(
    entity: Annotated[str, typer.Argument(help="W&B entity or team name.")],
    project: Annotated[str, typer.Argument(help="W&B project name.")],
    target_project: str | None = typer.Option(
        None,
        help="EpochDeck project name; defaults to the W&B project name.",
    ),
    checkpoint: Annotated[
        Path,
        typer.Option(help="Durable JSON checkpoint used to resume interrupted imports."),
    ] = Path("epochdeck-wandb-checkpoint.json"),
    workers: int = typer.Option(4, min=1, max=16, help="Runs imported concurrently."),
    max_runs: int | None = typer.Option(
        None,
        min=1,
        max=100_000,
        help="Stop after this many source runs.",
    ),
    include_files: bool = typer.Option(
        True,
        "--files/--no-files",
        help="Preserve W&B run files as versioned EpochDeck artifacts.",
    ),
    server_url: str = typer.Option(
        "http://127.0.0.1:8787",
        envvar="EPOCHDECK_SERVER_URL",
        help="EpochDeck server base URL.",
    ),
    timeout: float = typer.Option(300.0, min=0.1, help="Per-request timeout in seconds."),
) -> None:
    """Import with exact replay; Ctrl-C stops workers after their active SDK call."""
    try:
        wandb = import_module("wandb")
    except ModuleNotFoundError as error:
        raise typer.BadParameter(
            "the W&B importer requires the optional 'wandb' package in this environment"
        ) from error
    source_api = wandb.Api()
    with EpochDeckClient(server_url, timeout=timeout) as client:
        result = import_wandb_runs(
            source_api,
            client,
            entity=entity,
            project=project,
            target_project=target_project or project,
            checkpoint_path=checkpoint,
            workers=workers,
            max_runs=max_runs,
            include_files=include_files,
        )
    typer.echo(
        json.dumps(
            {
                "selected": result.selected,
                "completed": result.completed,
                "skipped": result.skipped,
                "failed": result.failed,
                "failures": result.failures,
            },
            sort_keys=True,
        )
    )
    if result.failed:
        raise typer.Exit(code=1)


@app.command("backup")
def backup_command(
    destination: Annotated[
        Path,
        typer.Argument(help="New physical backup directory to create."),
    ],
) -> None:
    """Back up the catalog, raw metrics, and all CAS bytes while stopped."""
    manifest = backup_storage(StorageRoots.from_environment(), destination)
    typer.echo(json.dumps(manifest, sort_keys=True))


@app.command("restore")
def restore_command(
    bundle: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=False,
            readable=True,
            help="Physical EpochDeck backup directory.",
        ),
    ],
) -> None:
    """Verify and restore a physical backup into empty storage roots while stopped."""
    manifest = restore_storage(bundle, StorageRoots.from_environment())
    typer.echo(json.dumps(manifest, sort_keys=True))


def _parse_json_filters(values: list[str], name: str) -> dict[str, object]:
    parsed: dict[str, object] = {}
    for raw in values:
        key, separator, encoded = raw.partition("=")
        if not separator or not key:
            raise typer.BadParameter(
                f"{name} filters must use KEY=JSON",
                param_hint=f"--{name}",
            )
        if key in parsed:
            raise typer.BadParameter(
                f"duplicate {name} filter key: {key}",
                param_hint=f"--{name}",
            )
        try:
            parsed[key] = json.loads(encoded)
        except json.JSONDecodeError as error:
            raise typer.BadParameter(
                f"invalid JSON for {name} key {key!r}: {error.msg}",
                param_hint=f"--{name}",
            ) from error
    return parsed
