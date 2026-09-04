from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

import httpx
import pytest

import epochdeck.exporter as exporter_module
from epochdeck.client import EpochDeckClient
from epochdeck.exporter import (
    ExportConsistencyError,
    _batches,
    _cursor_records,
    _metric_keys,
    export_project,
)


def test_export_project_streams_all_current_resources_and_deduplicates_blobs(
    monkeypatch,
    tmp_path,
) -> None:
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
    artifact_summary = {key: artifact[key] for key in ("id", "project", "name", "type", "version")}
    run = {
        "id": "run-1",
        "project": "demo",
        "name": "baseline",
        "state": "finished",
        "config": {"seed": 7},
        "summary": {"loss": 0.5},
    }
    run_summary = {
        "id": "run-1",
        "project": "demo",
        "name": "baseline",
        "state": "finished",
        "metric_revision": 1,
        "rich_data_revision": 1,
    }
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        path = request.url.path
        if path == "/api/v1/projects/demo":
            return httpx.Response(200, json={"name": "demo", "mutation_token": "7"})
        if path.endswith("/reports"):
            return httpx.Response(200, json={"reports": [], "next_before": None})
        if path.endswith("/sweeps"):
            return httpx.Response(200, json={"sweeps": [], "next_before": None})
        if path == "/api/v1/projects/demo/artifacts":
            return httpx.Response(
                200,
                json={"artifacts": [artifact_summary], "next_before": None},
            )
        if path == "/api/v1/artifacts/artifact-1":
            return httpx.Response(200, json=artifact)
        if path == "/api/v1/query/runs":
            return httpx.Response(200, json={"runs": [run_summary], "next_before": None})
        if path == "/api/v1/runs/run-1":
            return httpx.Response(200, json=run)
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
        if path == "/api/v1/runs/run-1/rich-values/keys":
            return httpx.Response(
                200,
                json={
                    "keys": [
                        {
                            "key": "rollout",
                            "count": 1,
                            "latest": {"id": "rich-1", "key": "rollout"},
                        }
                    ],
                    "next_after": None,
                },
            )
        if path == "/api/v1/runs/run-1/rich-values":
            assert request.url.params["key"] == "rollout"
            return httpx.Response(
                200,
                json={
                    "values": [{"id": "rich-1", "kind": "video", "key": "rollout"}],
                    "next_before": None,
                },
            )
        if path == "/api/v1/rich-values/rich-1":
            return httpx.Response(
                200,
                json={
                    "id": "rich-1",
                    "kind": "video",
                    "key": "rollout",
                    "metadata": {"fps": 30},
                    "blob": blob,
                },
            )
        if path == "/api/v1/runs/run-1/artifacts":
            return httpx.Response(
                200,
                json={
                    "artifacts": [{"artifact": artifact, "relation": "output"}],
                    "next_before": None,
                    "next_before_relation": None,
                },
            )
        if path == f"/api/v1/blobs/{digest}":
            return httpx.Response(200, content=content)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    destination = tmp_path / "bundle"
    publish_events: list[str] = []
    sync_tree = exporter_module._sync_private_tree
    replace = exporter_module.os.replace
    fsync_directory = exporter_module._fsync_directory

    def record_sync_tree(path, *, depth=0):
        if depth == 0:
            publish_events.append("sync-tree")
        sync_tree(path, depth=depth)

    def record_replace(source, target):
        if Path(target) == destination:
            publish_events.append("rename")
        replace(source, target)

    def record_fsync_directory(path):
        publish_events.append("fsync-parent")
        fsync_directory(path)

    monkeypatch.setattr(exporter_module, "_sync_private_tree", record_sync_tree)
    monkeypatch.setattr(exporter_module.os, "replace", record_replace)
    monkeypatch.setattr(exporter_module, "_fsync_directory", record_fsync_directory)
    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        manifest = export_project(client, "demo", destination)

    if os.name != "nt":
        assert destination.stat().st_mode & 0o777 == 0o700
        for path in destination.rglob("*"):
            assert path.stat().st_mode & 0o777 == (0o700 if path.is_dir() else 0o600)
    assert publish_events == ["sync-tree", "rename", "fsync-parent"]
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
    }
    assert json.loads((destination / "manifest.json").read_text()) == manifest
    assert manifest["format"] == "epochdeck-export"
    assert (destination / "blobs" / "sha256" / digest[:2] / digest).read_bytes() == content
    metric_page = json.loads(
        (destination / "runs" / "run-1" / "metrics" / "0000.jsonl").read_text()
    )
    assert metric_page["metrics"] == {"loss": [0.5]}
    assert sum(request.url.path == f"/api/v1/blobs/{digest}" for request in requests) == 1


def test_export_rejects_a_live_project_before_writing_a_partial_bundle(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/projects/demo":
            return httpx.Response(200, json={"name": "demo", "mutation_token": "1"})
        if request.url.path.endswith("/reports"):
            return httpx.Response(200, json={"reports": [], "next_before": None})
        if request.url.path.endswith("/sweeps"):
            return httpx.Response(200, json={"sweeps": [], "next_before": None})
        if request.url.path == "/api/v1/projects/demo/artifacts":
            return httpx.Response(200, json={"artifacts": [], "next_before": None})
        if request.url.path == "/api/v1/query/runs":
            return httpx.Response(
                200,
                json={
                    "runs": [{"id": "run-1", "project": "demo", "state": "running"}],
                    "next_before": None,
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    destination = tmp_path / "bundle"
    with (
        EpochDeckClient(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(ExportConsistencyError, match="still running"),
    ):
        export_project(client, "demo", destination)
    assert not destination.exists()


def test_export_hydrates_lightweight_sweep_and_trial_pages(tmp_path) -> None:
    sweep_summary = {
        "id": "sweep-1",
        "project": "demo",
        "name": "grid",
        "parameter_count": 1,
    }
    sweep = {
        "id": "sweep-1",
        "project": "demo",
        "name": "grid",
        "parameters": {"seed": {"values": [1, 2]}},
        "early_terminate": {"min_step": 10, "min_trials": 2},
    }
    trial_summary = {
        "id": "trial-1",
        "sweep_id": "sweep-1",
        "state": "completed",
    }
    trial = {
        "id": "trial-1",
        "sweep_id": "sweep-1",
        "state": "completed",
        "config": {"seed": 1},
    }
    detail_requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/api/v1/projects/demo":
            return httpx.Response(200, json={"name": "demo", "mutation_token": "4"})
        if path == "/api/v1/projects/demo/reports":
            return httpx.Response(200, json={"reports": [], "next_before": None})
        if path == "/api/v1/projects/demo/sweeps":
            return httpx.Response(
                200,
                json={"sweeps": [sweep_summary], "next_before": None},
            )
        if path == "/api/v1/sweeps/sweep-1":
            detail_requests.append(path)
            return httpx.Response(200, json=sweep)
        if path == "/api/v1/sweeps/sweep-1/trials":
            return httpx.Response(
                200,
                json={"trials": [trial_summary], "next_before": None},
            )
        if path == "/api/v1/sweep-trials/trial-1":
            detail_requests.append(path)
            return httpx.Response(200, json=trial)
        if path == "/api/v1/projects/demo/artifacts":
            return httpx.Response(200, json={"artifacts": [], "next_before": None})
        if path == "/api/v1/query/runs":
            return httpx.Response(200, json={"runs": [], "next_before": None})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    destination = tmp_path / "bundle"
    with EpochDeckClient(transport=httpx.MockTransport(handler)) as client:
        export_project(client, "demo", destination)

    exported_sweep = json.loads((destination / "sweeps.jsonl").read_text())
    exported_trial = json.loads((destination / "sweep-trials.jsonl").read_text())
    assert exported_sweep["parameters"] == sweep["parameters"]
    assert exported_sweep["early_terminate"] == sweep["early_terminate"]
    assert exported_trial == {"sweep_id": "sweep-1", "trial": trial}
    assert detail_requests.count("/api/v1/sweeps/sweep-1") == 1
    assert detail_requests.count("/api/v1/sweep-trials/trial-1") == 1


def test_export_rejects_a_changed_project_mutation_token(tmp_path) -> None:
    run = {
        "id": "run-1",
        "project": "demo",
        "name": "finished",
        "state": "finished",
        "config": {},
        "summary": {},
    }
    run_summary = {
        "id": "run-1",
        "project": "demo",
        "name": "finished",
        "state": "finished",
        "document_revision": 1,
        "metric_revision": 1,
        "rich_data_revision": 1,
    }
    rich_detail_calls = 0
    project_detail_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal project_detail_calls, rich_detail_calls
        path = request.url.path
        if path == "/api/v1/projects/demo":
            project_detail_calls += 1
            return httpx.Response(
                200,
                json={
                    "name": "demo",
                    "mutation_token": "11" if project_detail_calls == 1 else "12",
                },
            )
        if path == "/api/v1/projects/demo/reports":
            return httpx.Response(200, json={"reports": [], "next_before": None})
        if path == "/api/v1/projects/demo/sweeps":
            return httpx.Response(200, json={"sweeps": [], "next_before": None})
        if path == "/api/v1/projects/demo/artifacts":
            return httpx.Response(200, json={"artifacts": [], "next_before": None})
        if path == "/api/v1/query/runs":
            return httpx.Response(200, json={"runs": [run_summary], "next_before": None})
        if path == "/api/v1/runs/run-1":
            return httpx.Response(200, json=run)
        if path == "/api/v1/runs/run-1/metrics":
            return httpx.Response(
                200,
                json={"run_id": "run-1", "keys": [], "next_after": None},
            )
        if path == "/api/v1/runs/run-1/alerts":
            return httpx.Response(200, json={"alerts": [], "next_before": None})
        if path == "/api/v1/runs/run-1/rich-values/keys":
            return httpx.Response(
                200,
                json={
                    "keys": [{"key": "rollout", "count": 1, "latest": {"id": "rich-1"}}],
                    "next_after": None,
                },
            )
        if path == "/api/v1/runs/run-1/rich-values":
            return httpx.Response(
                200,
                json={
                    "values": [{"id": "rich-1", "key": "rollout", "kind": "histogram"}],
                    "next_before": None,
                },
            )
        if path == "/api/v1/rich-values/rich-1":
            rich_detail_calls += 1
            return httpx.Response(
                200,
                json={
                    "id": "rich-1",
                    "key": "rollout",
                    "kind": "histogram",
                    "metadata": {"version": 1},
                    "blob": None,
                },
            )
        if path == "/api/v1/runs/run-1/artifacts":
            return httpx.Response(
                200,
                json={
                    "artifacts": [],
                    "next_before": None,
                    "next_before_relation": None,
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    destination = tmp_path / "bundle"
    with (
        EpochDeckClient(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(ExportConsistencyError, match="project changed during export"),
    ):
        export_project(client, "demo", destination)

    assert project_detail_calls == 2
    assert rich_detail_calls == 1
    assert not destination.exists()


def test_export_mutation_token_rejects_transient_create_delete_aba(tmp_path) -> None:
    project_calls = 0
    report_detail_calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal project_calls, report_detail_calls
        path = request.url.path
        if path == "/api/v1/projects/demo":
            project_calls += 1
            return httpx.Response(
                200,
                json={
                    "name": "demo",
                    "mutation_token": "20" if project_calls == 1 else "22",
                },
            )
        if path == "/api/v1/projects/demo/reports":
            return httpx.Response(
                200,
                json={"reports": [{"id": "transient-report"}], "next_before": None},
            )
        if path == "/api/v1/reports/transient-report":
            report_detail_calls += 1
            return httpx.Response(
                200,
                json={
                    "id": "transient-report",
                    "project": "demo",
                    "name": "created then deleted",
                    "layout": {"columns": 1, "panels": []},
                },
            )
        if path == "/api/v1/projects/demo/sweeps":
            return httpx.Response(200, json={"sweeps": [], "next_before": None})
        if path == "/api/v1/projects/demo/artifacts":
            return httpx.Response(200, json={"artifacts": [], "next_before": None})
        if path == "/api/v1/query/runs":
            return httpx.Response(200, json={"runs": [], "next_before": None})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    destination = tmp_path / "bundle"
    with (
        EpochDeckClient(transport=httpx.MockTransport(handler)) as client,
        pytest.raises(ExportConsistencyError, match="project changed during export"),
    ):
        export_project(client, "demo", destination)

    assert project_calls == 2
    assert report_detail_calls == 1
    assert not destination.exists()


def test_export_pagination_rejects_invalid_and_repeated_cursors() -> None:
    with pytest.raises(TypeError, match="invalid or repeated cursor"):
        list(_cursor_records(lambda _: {"items": [], "next_before": 7}, "items"))

    calls = 0

    def repeated(before):
        nonlocal calls
        calls += 1
        return {"items": [], "next_before": "same"}

    with pytest.raises(TypeError, match="invalid or repeated cursor"):
        list(_cursor_records(repeated, "items"))
    assert calls == 2


def test_metric_key_export_is_lazy_paged_and_batched_to_protocol_width() -> None:
    class MetricClient:
        def __init__(self) -> None:
            self.calls: list[str | None] = []

        def metric_keys(self, run_id, *, after, limit):
            assert run_id == "run-1"
            assert limit == 200
            self.calls.append(after)
            if after is None:
                return {
                    "keys": [f"metric-{index:03d}" for index in range(40)],
                    "next_after": "metric-039",
                }
            assert after == "metric-039"
            return {
                "keys": [f"metric-{index:03d}" for index in range(40, 65)],
                "next_after": None,
            }

    client = MetricClient()
    keys = _metric_keys(client, "run-1")
    assert client.calls == []
    batches = list(_batches(keys, 32))

    assert client.calls == [None, "metric-039"]
    assert [len(batch) for batch in batches] == [32, 32, 1]
    assert batches[-1][-1] == "metric-064"
