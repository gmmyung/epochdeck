from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from runloom.client import RunloomClient
from runloom.public_api import Api
from runloom.run import sync_spool

app = typer.Typer(no_args_is_help=True, pretty_exceptions_enable=False)


@app.callback()
def main() -> None:
    """Operate a Runloom client and server."""


@app.command()
def health(
    server_url: str = typer.Option(
        "http://127.0.0.1:8787",
        envvar="RUNLOOM_SERVER_URL",
        help="Runloom server base URL.",
    ),
) -> None:
    """Check server and catalog health."""
    with RunloomClient(server_url) as client:
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
        envvar="RUNLOOM_SERVER_URL",
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
        envvar="RUNLOOM_SERVER_URL",
        help="Runloom server base URL.",
    ),
    limit: int = typer.Option(100, min=1, max=200, help="Maximum projects to return."),
) -> None:
    """List projects as JSON."""
    with RunloomClient(server_url) as client:
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
        envvar="RUNLOOM_SERVER_URL",
        help="Runloom server base URL.",
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
    with RunloomClient(server_url) as client:
        result = client.query_runs(query)
    typer.echo(json.dumps(result, sort_keys=True))


@app.command("get")
def get_command(
    run: str = typer.Argument(help="Run ID or project/run-id path."),
    server_url: str = typer.Option(
        "http://127.0.0.1:8787",
        envvar="RUNLOOM_SERVER_URL",
        help="Runloom server base URL.",
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
        envvar="RUNLOOM_SERVER_URL",
        help="Runloom server base URL.",
    ),
) -> None:
    """Read one bounded full-resolution metric-history page."""
    with RunloomClient(server_url) as client:
        result = client.history(run_id, keys=key, after=after, limit=limit)
    typer.echo(json.dumps(result, sort_keys=True))


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
