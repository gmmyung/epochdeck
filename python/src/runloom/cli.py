from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from runloom.client import RunloomClient
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
