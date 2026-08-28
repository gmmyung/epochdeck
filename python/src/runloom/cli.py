from __future__ import annotations

import json

import typer

from runloom.client import RunloomClient

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
