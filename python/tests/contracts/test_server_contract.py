"""Exercise the public SDK against the actual server binary, including recovery.

`just server-contract-check` builds the binary and enables these tests. Unit-only
Python runs do not require a Rust toolchain or a running service.
"""

from __future__ import annotations

import base64
import os
import runpy
import socket
import subprocess
import time
import tomllib
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest

import epochdeck as ed
from epochdeck.backup import StorageRoots, backup_storage, restore_storage
from epochdeck.client import EpochDeckClient

SERVER = os.environ.get("EPOCHDECK_TEST_SERVER")
pytestmark = pytest.mark.skipif(SERVER is None, reason="run just server-contract-check")


def test_release_smoke_checks_versioned_branding(tmp_path):
    assert SERVER is not None
    root = Path(__file__).resolve().parents[3]
    smoke = runpy.run_path(str(root / "scripts" / "smoke-release-server.py"))
    version = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    smoke["_run_cli"](Path(SERVER), version)
    smoke["_smoke_server"](Path(SERVER), tmp_path / "runtime", version)


@contextmanager
def running_server(roots: StorageRoots, log_path: Path):
    assert SERVER is not None
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    url = f"http://127.0.0.1:{port}"
    environment = {
        key: value for key, value in os.environ.items() if not key.startswith("EPOCHDECK_")
    }
    environment.update(
        EPOCHDECK_BIND=f"127.0.0.1:{port}",
        EPOCHDECK_DATA_DIR=str(roots.data),
        EPOCHDECK_METRICS_DIR=str(roots.metrics),
        EPOCHDECK_BLOBS_DIR=str(roots.blobs),
    )
    with log_path.open("wb") as log:
        process = subprocess.Popen([SERVER], env=environment, stdout=log, stderr=log)
        try:
            with httpx.Client(base_url=url, timeout=0.5, trust_env=False) as probe:
                deadline = time.monotonic() + 15
                while True:
                    assert process.poll() is None, log_path.read_text()
                    try:
                        if probe.get("/api/v1/health").status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    assert time.monotonic() < deadline, log_path.read_text()
                    time.sleep(0.05)
            yield url, process
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
                    raise


def test_sdk_history_media_artifacts_survive_restart_and_physical_restore(tmp_path, monkeypatch):
    monkeypatch.delenv("EPOCHDECK_HTTP_USERNAME", raising=False)
    monkeypatch.delenv("EPOCHDECK_HTTP_PASSWORD", raising=False)
    roots = StorageRoots(tmp_path / "data", tmp_path / "metrics", tmp_path / "blobs")
    image = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAusB9Y9ZQmcAAAAASUVORK5CYII="
    )
    with running_server(roots, tmp_path / "original.log") as (url, process):
        with ed.init(project="contract", config={"seed": 7}, dir=tmp_path, server_url=url) as run:
            for step in range(100):
                run.log({"loss": float(100 - step), "reward": float(step)}, step=step)
            run.log({"image": ed.Image(image)})
            source = tmp_path / "weights.txt"
            source.write_text("contract weights")
            artifact = ed.Artifact("weights", type="model")
            artifact.add_file(source)
            run.log_artifact(artifact, aliases=["latest"])
            run.alert("contract alert")
            run.summary["result"] = "complete"

        with EpochDeckClient(url) as client:
            history = client.history(run.id, keys=["loss", "reward"], limit=200)
            assert len(history["sequence"]) == 100
            assert history["metrics"]["loss"] == [float(100 - i) for i in range(100)]
            assert client.get_run(run.id)["state"] == "finished"
            value = client.rich_values(run.id, key="image")["values"][0]
            digest = value["blob"]["digest"]
            assert client.resolve_artifact("contract", "weights", "latest")["id"] == artifact.id
            assert len(client.alerts(run.id)["alerts"]) == 1
        # Kill after acknowledgement without running server shutdown handlers.
        process.kill()
        process.wait(timeout=5)

    with (
        running_server(roots, tmp_path / "restart.log") as (url, _),
        EpochDeckClient(url) as client,
    ):
        assert client.history(run.id, keys=["loss", "reward"], limit=200) == history

    bundle = tmp_path / "backup"
    backup_storage(roots, bundle)
    restored = StorageRoots(
        tmp_path / "restored-data", tmp_path / "restored-metrics", tmp_path / "restored-blobs"
    )
    restore_storage(bundle, restored)
    with (
        running_server(restored, tmp_path / "restore.log") as (url, _),
        EpochDeckClient(url) as client,
    ):
        assert client.history(run.id, keys=["loss", "reward"], limit=200) == history
        destination = tmp_path / "restored-image.png"
        client.download_blob(digest, destination)
        assert destination.read_bytes() == image
        assert client.get_artifact(artifact.id)["entries"][0]["path"] == "weights.txt"
