from __future__ import annotations

import json
import threading
import time
import uuid

import httpx
import pytest

from runloom import Histogram, Image, Table
from runloom.run import DeliveryError, create_run, sync_spool


def test_online_run_batches_nested_metrics_without_blocking_on_upload(tmp_path) -> None:
    uploaded: list[dict] = []
    upload_started = threading.Event()
    release_upload = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/runs"):
            body = json.loads(request.content)
            return httpx.Response(
                201,
                json={
                    "run": {
                        "id": body["id"],
                        "name": "training",
                    },
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/batches"):
            upload_started.set()
            assert release_upload.wait(2)
            batch = json.loads(request.content)
            uploaded.append(batch)
            return httpx.Response(
                201,
                json={
                    "run_id": "ignored",
                    "batch_sequence": batch["batch_sequence"],
                    "accepted_points": len(batch["points"]),
                    "duplicate": False,
                    "metric_revision": 1,
                },
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(200, json={"run": {"state": "finished"}})
        raise AssertionError(f"unexpected request: {request.url}")

    run = create_run(
        project="robotics",
        run_id="019c1234-5678-7000-8000-000000000001",
        mode="online",
        resume="never",
        spool_root=tmp_path,
        batch_size=64,
        flush_interval=0,
        transport=httpx.MockTransport(handler),
    )
    run.log({"loss": 2.0, "train": {"reward": 4}})
    assert upload_started.wait(2)

    started = time.monotonic()
    run.log({"loss": 1.0})
    elapsed = time.monotonic() - started
    release_upload.set()
    run.finish(timeout=2)

    assert elapsed < 0.2
    assert len(uploaded) == 2
    assert uploaded[0]["points"][0]["metrics"] == {
        "loss": 2.0,
        "train/reward": 4.0,
    }
    assert uploaded[1]["points"][0]["step"] == 1


def test_offline_run_keeps_a_durable_journal(tmp_path) -> None:
    run = create_run(
        project="robotics",
        run_id="019c1234-5678-7000-8000-000000000002",
        config={"seed": 7},
        mode="offline",
        resume="never",
        spool_root=tmp_path,
    )
    run.config.update({"optimizer": "adam"})
    with pytest.raises(ValueError, match="allow_val_change"):
        run.config.update({"seed": 8})
    run.config.update({"seed": 8}, allow_val_change=True)
    run.summary["status"] = "offline"
    run.log({"loss": 1.5}, step=7)
    run.finish(summary={"best": {"score": 1.5, "tags": ["stable", None]}})

    run_directory = tmp_path / run.id
    event = json.loads((run_directory / "events.jsonl").read_text().strip())
    metadata = json.loads((run_directory / "run.json").read_text())
    assert event["step"] == 7
    assert event["metrics"] == {"loss": 1.5}
    assert metadata["finished"] is True
    assert metadata["config"] == {"optimizer": "adam", "seed": 8}
    assert metadata["summary"] == {
        "best": {"score": 1.5, "tags": ["stable", None]},
        "loss": 1.5,
        "status": "offline",
    }

    requests: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path.endswith("/runs"):
            return httpx.Response(
                201,
                json={
                    "run": {"name": "training"},
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/batches"):
            batch = json.loads(request.content)
            return httpx.Response(
                201,
                json={
                    "run_id": run.id,
                    "batch_sequence": batch["batch_sequence"],
                    "accepted_points": len(batch["points"]),
                    "duplicate": False,
                    "metric_revision": 1,
                },
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(200, json={"run": {"state": "finished"}})
        raise AssertionError(f"unexpected request: {request.url}")

    synced_id = sync_spool(run_directory, transport=httpx.MockTransport(handler), timeout=2)
    assert synced_id == run.id
    assert requests == [
        "/api/v1/projects/robotics/runs",
        f"/api/v1/runs/{run.id}/batches",
        f"/api/v1/runs/{run.id}/finish",
    ]
    assert (
        int((run_directory / "ack").read_text()) == (run_directory / "events.jsonl").stat().st_size
    )


def test_disabled_run_does_not_touch_the_spool(tmp_path) -> None:
    run = create_run(
        project="robotics",
        mode="disabled",
        resume="never",
        spool_root=tmp_path,
    )
    run.log({"loss": 1.0})
    run.finish()
    assert run.finished
    assert list(tmp_path.iterdir()) == []


def test_online_documents_use_authoritative_server_state(tmp_path) -> None:
    server_config: dict = {"seed": 1}
    server_summary: dict = {"status": "resumed"}
    requests: list[str] = []

    def response_run() -> dict:
        return {
            "name": "resumed-run",
            "config": dict(server_config),
            "summary": dict(server_summary),
        }

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(f"{request.method} {request.url.path}")
        body = json.loads(request.content) if request.content else {}
        if request.url.path.endswith("/runs"):
            return httpx.Response(
                200,
                json={
                    "run": response_run(),
                    "resumed": True,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/config"):
            server_config.update(body["updates"])
            return httpx.Response(200, json={"run": response_run()})
        if request.url.path.endswith("/summary"):
            server_summary.update(body["updates"])
            return httpx.Response(200, json={"run": response_run()})
        if request.url.path.endswith("/finish"):
            server_summary.update(body["summary"])
            return httpx.Response(200, json={"run": response_run()})
        raise AssertionError(f"unexpected request: {request.url}")

    run = create_run(
        project="robotics",
        run_id="019c1234-5678-7000-8000-000000000003",
        config={"seed": 999},
        mode="online",
        resume="allow",
        spool_root=tmp_path,
        transport=httpx.MockTransport(handler),
    )

    assert run.config.seed == 1
    assert run.summary["status"] == "resumed"
    run.config.update({"optimizer": "adam"})
    with pytest.raises(ValueError, match="allow_val_change"):
        run.config["seed"] = 2
    run.config.update({"seed": 2}, allow_val_change=True)
    run.summary.update({"result": "running", "metadata": {"tags": ["a", None]}})
    run.finish(summary={"result": "complete"})

    assert run.config.to_dict() == {"optimizer": "adam", "seed": 2}
    assert run.summary["result"] == "complete"
    assert requests == [
        "POST /api/v1/projects/robotics/runs",
        f"PATCH /api/v1/runs/{run.id}/config",
        f"PATCH /api/v1/runs/{run.id}/config",
        f"PATCH /api/v1/runs/{run.id}/summary",
        f"POST /api/v1/runs/{run.id}/finish",
    ]


def test_documents_reject_non_json_values_before_writing(tmp_path) -> None:
    with pytest.raises(TypeError, match="unsupported JSON type"):
        create_run(
            project="robotics",
            config={"bad": object()},
            mode="offline",
            spool_root=tmp_path,
        )
    assert list(tmp_path.iterdir()) == []


def test_online_run_rejects_a_server_without_resume_positions(tmp_path) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        return httpx.Response(
            201,
            json={
                "run": {"id": body["id"], "name": "outdated-server"},
                "resumed": False,
            },
        )

    with pytest.raises(DeliveryError, match="server and SDK versions may differ"):
        create_run(
            project="robotics",
            run_id="019c1234-5678-7000-8000-000000000004",
            mode="online",
            spool_root=tmp_path,
            transport=httpx.MockTransport(handler),
        )


def test_system_metrics_are_durable_without_advancing_user_steps(tmp_path) -> None:
    sampled = threading.Event()

    def sample() -> dict[str, float]:
        sampled.set()
        return {"system/cpu_percent": 12.5, "system/process_rss_bytes": 1_024.0}

    run = create_run(
        project="robotics",
        run_id="019c1234-5678-7000-8000-000000000005",
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0.01,
        system_sampler=sample,
    )
    run.log({"loss": 1.0}, step=7)
    assert sampled.wait(1)
    time.sleep(0.02)
    run.finish()

    events = [
        json.loads(line) for line in (tmp_path / run.id / "events.jsonl").read_text().splitlines()
    ]
    system_events = [event for event in events if "system/cpu_percent" in event["metrics"]]
    assert system_events
    assert {event["step"] for event in system_events} == {7}
    assert run.summary.to_dict() == {"loss": 1.0}


def test_alert_delivery_replays_the_same_durable_record(tmp_path) -> None:
    received: list[dict] = []
    first_alert_committed = threading.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/runs"):
            body = json.loads(request.content)
            return httpx.Response(
                201,
                json={
                    "run": {"id": body["id"], "name": "training"},
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/alerts"):
            body = json.loads(request.content)
            received.append(body)
            if len(received) == 1:
                first_alert_committed.set()
                raise httpx.ReadError("response lost after commit", request=request)
            return httpx.Response(200, json={"alert": body, "duplicate": True})
        if request.url.path.endswith("/batches"):
            body = json.loads(request.content)
            return httpx.Response(
                201,
                json={
                    "run_id": run.id,
                    "batch_sequence": body["batch_sequence"],
                    "accepted_points": len(body["points"]),
                    "duplicate": False,
                    "metric_revision": 1,
                },
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(200, json={"run": {"state": "finished"}})
        raise AssertionError(f"unexpected request: {request.url}")

    run = create_run(
        project="robotics",
        run_id="019c1234-5678-7000-8000-000000000006",
        mode="online",
        spool_root=tmp_path,
        flush_interval=0,
        system_monitor_interval=0,
        transport=httpx.MockTransport(handler),
    )
    run.log({"loss": 1.0}, step=9)
    run.alert("Training diverged", "Loss became unstable", level="WARN")
    assert first_alert_committed.wait(2)
    run.finish(timeout=2)

    assert received[0] == received[1]
    assert received[0]["level"] == "warn"
    assert received[0]["step"] == 9
    assert uuid.UUID(received[0]["id"]).version == 7
    assert not (tmp_path / run.id / "alert-delivery.json").exists()


def test_alerts_validate_before_touching_the_journal(tmp_path) -> None:
    run = create_run(
        project="robotics",
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )
    with pytest.raises(ValueError, match="alert level"):
        run.alert("Bad level", level="critical")
    with pytest.raises(ValueError, match="non-control"):
        run.alert("bad\ntitle")
    assert (tmp_path / run.id / "alerts.jsonl").read_text() == ""
    run.finish()


def test_rich_only_runs_resume_at_the_next_user_step(tmp_path) -> None:
    run_id = "019c1234-5678-7000-8000-000000000008"
    first = create_run(
        project="robotics",
        run_id=run_id,
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )
    first.log({"media": {"frame": Image(b"png-bytes")}})
    del first

    resumed = create_run(
        project="robotics",
        run_id=run_id,
        mode="offline",
        resume="allow",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )
    resumed.log({"loss": 1.0})
    resumed.finish()

    directory = tmp_path / run_id
    rich_value = json.loads((directory / "rich-values.jsonl").read_text())
    metric = json.loads((directory / "events.jsonl").read_text())
    assert rich_value["key"] == "media/frame"
    assert rich_value["kind"] == "image"
    assert rich_value["step"] == 0
    assert metric["step"] == 1
    assert (directory / "blobs" / rich_value["blob"]["digest"]).read_bytes() == b"png-bytes"


def test_offline_rich_values_stream_blobs_and_sync_idempotently(tmp_path) -> None:
    run = create_run(
        project="robotics",
        run_id="019c1234-5678-7000-8000-000000000009",
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )
    run.log(
        {
            "frame": Image(b"image-content", caption="camera"),
            "results": Table(
                columns=["step", "score"], data=((index, index / 2) for index in range(3))
            ),
            "distribution": Histogram([0.0, 0.5, 1.0], num_bins=2),
        },
        step=4,
    )
    run.finish()
    directory = tmp_path / run.id
    uploaded: dict[str, bytes] = {}
    received: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/runs"):
            return httpx.Response(
                201,
                json={
                    "run": {"name": "rich-run"},
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if "/blobs/" in request.url.path:
            digest = request.url.path.rsplit("/", 1)[1]
            content = request.read()
            uploaded[digest] = content
            return httpx.Response(
                201,
                json={
                    "blob": {
                        "digest": digest,
                        "size": len(content),
                        "mime_type": request.headers["content-type"],
                        "file_name": request.headers.get("x-runloom-file-name"),
                    },
                    "duplicate": False,
                },
            )
        if request.url.path.endswith("/rich-values"):
            value = json.loads(request.content)
            received.append(value)
            return httpx.Response(201, json={"value": value, "duplicate": False})
        if request.url.path.endswith("/finish"):
            return httpx.Response(200, json={"run": {"state": "finished"}})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    sync_spool(directory, transport=httpx.MockTransport(handler), timeout=2)

    assert {value["kind"] for value in received} == {"image", "table", "histogram"}
    assert {value["step"] for value in received} == {4}
    assert len(uploaded) == 2
    table = next(value for value in received if value["kind"] == "table")
    table_payload = json.loads(uploaded[table["blob"]["digest"]])
    assert table_payload == {
        "columns": ["step", "score"],
        "data": [[0, 0.0], [1, 0.5], [2, 1.0]],
    }
    assert table["metadata"]["row_count"] == 3
    assert (
        int((directory / "rich-ack").read_text())
        == (directory / "rich-values.jsonl").stat().st_size
    )
