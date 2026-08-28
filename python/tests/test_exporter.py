from __future__ import annotations

import hashlib
import json

import httpx

from runloom.client import RunloomClient
from runloom.exporter import export_project


def test_export_project_streams_all_current_resources_and_deduplicates_blobs(tmp_path) -> None:
    content = b"checkpoint-bytes"
    digest = hashlib.sha256(content).hexdigest()
    blob = {
        "digest": digest,
        "size": len(content),
        "mime_type": "application/octet-stream",
        "file_name": "checkpoint.bin",
    }
    artifact = {
        "id": "artifact-1",
        "project": "demo",
        "name": "model",
        "type": "model",
        "version": 0,
        "entries": [{"path": "checkpoint.bin", "blob": blob}],
    }
    run = {
        "id": "run-1",
        "project": "demo",
        "name": "baseline",
        "state": "finished",
        "config": {"seed": 7},
        "summary": {"loss": 0.5},
    }
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        path = request.url.path
        if path.endswith("/reports"):
            return httpx.Response(200, json={"reports": [], "next_before": None})
        if path.endswith("/sweeps"):
            return httpx.Response(200, json={"sweeps": [], "next_before": None})
        if path == "/api/v1/projects/demo/artifacts":
            return httpx.Response(200, json={"artifacts": [artifact], "next_before": None})
        if path == "/api/v1/query/runs":
            return httpx.Response(200, json={"runs": [run], "next_before": None})
        if path == "/api/v1/runs/run-1/metrics":
            return httpx.Response(200, json={"run_id": "run-1", "keys": ["loss"]})
        if path == "/api/v1/runs/run-1/history":
            return httpx.Response(
                200,
                json={
                    "run_id": "run-1",
                    "sequence": [1],
                    "step": [0],
                    "timestamp_ms": [1000],
                    "metrics": {"loss": [0.5]},
                    "next_after": None,
                    "sampled": False,
                    "source_points": None,
                    "source_last_sequence": 1,
                },
            )
        if path.endswith("/alerts"):
            return httpx.Response(200, json={"alerts": [], "next_before": None})
        if path.endswith("/rich-values"):
            return httpx.Response(
                200,
                json={
                    "values": [
                        {
                            "id": "rich-1",
                            "kind": "video",
                            "key": "rollout",
                            "blob": blob,
                        }
                    ],
                    "next_before": None,
                },
            )
        if path.endswith("/traces"):
            return httpx.Response(200, json={"spans": [], "next_before": None})
        if path == "/api/v1/runs/run-1/artifacts":
            return httpx.Response(
                200,
                json={
                    "artifacts": [{"artifact": artifact, "relation": "output"}],
                    "next_before": None,
                },
            )
        if path == f"/api/v1/blobs/{digest}":
            return httpx.Response(200, content=content)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    destination = tmp_path / "bundle"
    with RunloomClient(transport=httpx.MockTransport(handler)) as client:
        manifest = export_project(client, "demo", destination)

    assert manifest["counts"] == {
        "alerts": 0,
        "artifact_links": 1,
        "artifacts": 1,
        "blobs": 1,
        "metric_pages": 1,
        "reports": 0,
        "rich_values": 1,
        "runs": 1,
        "sweep_trials": 0,
        "sweeps": 0,
        "traces": 0,
    }
    assert json.loads((destination / "manifest.json").read_text())["format_version"] == 1
    assert (destination / "blobs" / "sha256" / digest[:2] / digest).read_bytes() == content
    metric_page = json.loads(
        (destination / "runs" / "run-1" / "metrics" / "0000.jsonl").read_text()
    )
    assert metric_page["metrics"] == {"loss": [0.5]}
    assert sum(request.url.path == f"/api/v1/blobs/{digest}" for request in requests) == 1
