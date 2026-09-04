from __future__ import annotations

import json
import os
import threading
import time
import uuid

import httpx
import pytest

from epochdeck import Artifact, Audio, Histogram, Image, Table
from epochdeck import _spool as spool_module
from epochdeck._protocol import encode_json_request
from epochdeck.rich import PreparedRichValue, RichValue
from epochdeck.run import (
    _SUMMARY_CHECKPOINT_BYTE_INTERVAL,
    _SUMMARY_CHECKPOINT_RECORD_INTERVAL,
    DeliveryError,
    _DeliveryWorker,
    create_run,
    sync_spool,
)

_METRIC_REQUEST_BUDGET = 1_750_000


def _summary_fields(
    *,
    explicit: dict | None = None,
    metric: dict | None = None,
    truncated: bool = False,
) -> dict:
    explicit = dict(explicit or {})
    metric = dict(metric or {})
    return {
        "explicit_summary": explicit,
        "metric_summary": metric,
        "summary": {**metric, **explicit},
        "summary_truncated": truncated,
    }


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
                        **_summary_fields(),
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
                    "run_id": "019c1234-5678-7000-8000-000000000001",
                    "batch_sequence": batch["batch_sequence"],
                    "accepted_points": len(batch["points"]),
                    "duplicate": False,
                    "metric_revision": 1,
                    "stop_requested": False,
                },
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(
                200,
                json={
                    "run": {
                        "id": "019c1234-5678-7000-8000-000000000001",
                        "state": "finished",
                        **_summary_fields(
                            metric={
                                key: value
                                for batch in uploaded
                                for point in batch["points"]
                                for key, value in point["metrics"].items()
                            }
                        ),
                    }
                },
            )
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
    assert set(metadata) == {
        "batch_size",
        "config",
        "explicit_summary",
        "finished",
        "finishing",
        "id",
        "metric_summary",
        "name",
        "project",
        "resume",
        "server_url",
        "summary_event_offset",
        "summary_truncated",
        "sweep_trial_id",
    }
    assert metadata["finished"] is True
    assert metadata["config"] == {"optimizer": "adam", "seed": 8}
    assert {**metadata["metric_summary"], **metadata["explicit_summary"]} == {
        "best": {"score": 1.5, "tags": ["stable", None]},
        "loss": 1.5,
        "status": "offline",
    }

    requests: list[str] = []
    finish_summaries: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.path)
        if request.url.path.endswith("/runs"):
            return httpx.Response(
                201,
                json={
                    "run": {"id": run.id, "name": "training"},
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
                    "stop_requested": False,
                },
            )
        if request.url.path.endswith("/finish"):
            finish_summaries.append(json.loads(request.content)["summary"])
            return httpx.Response(200, json={"run": {"id": run.id, "state": "finished"}})
        raise AssertionError(f"unexpected request: {request.url}")

    synced_id = sync_spool(run_directory, transport=httpx.MockTransport(handler), timeout=2)
    assert synced_id == run.id
    assert requests == [
        "/api/v1/projects/robotics/runs",
        f"/api/v1/runs/{run.id}/batches",
        f"/api/v1/runs/{run.id}/finish",
    ]
    assert finish_summaries == [
        {
            "best": {"score": 1.5, "tags": ["stable", None]},
            "status": "offline",
        }
    ]
    assert (
        int((run_directory / "ack").read_text()) == (run_directory / "events.jsonl").stat().st_size
    )


def test_proxy_credentials_are_not_written_to_the_run_spool(monkeypatch, tmp_path) -> None:
    run_id = "019c1234-5678-7000-8000-000000000102"
    monkeypatch.setenv("EPOCHDECK_HTTP_USERNAME", "proxy-user")
    monkeypatch.setenv("EPOCHDECK_HTTP_PASSWORD", "proxy-password")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Basic cHJveHktdXNlcjpwcm94eS1wYXNzd29yZA=="
        if request.url.path.endswith("/runs"):
            return httpx.Response(
                201,
                json={
                    "run": {"id": run_id, "name": "private", **_summary_fields()},
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(
                200,
                json={
                    "run": {
                        "id": run_id,
                        "state": "finished",
                        **_summary_fields(),
                    }
                },
            )
        raise AssertionError(f"unexpected request: {request.url}")

    run = create_run(
        project="robotics",
        run_id=run_id,
        mode="online",
        server_url="https://epochdeck.test",
        spool_root=tmp_path,
        system_monitor_interval=0,
        transport=httpx.MockTransport(handler),
    )
    metadata_path = tmp_path / run_id / "run.json"
    metadata = json.loads(metadata_path.read_text())
    assert metadata["server_url"] == "https://epochdeck.test"
    assert "proxy-user" not in metadata_path.read_text()
    assert "proxy-password" not in metadata_path.read_text()
    run.finish(timeout=2)


def test_embedded_server_credentials_are_rejected_before_spooling(tmp_path) -> None:
    with pytest.raises(ValueError, match="server_url must not contain credentials"):
        create_run(
            project="robotics",
            mode="offline",
            server_url="https://proxy-user:proxy-password@epochdeck.test",
            spool_root=tmp_path,
        )

    assert not any(tmp_path.iterdir())


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


def test_run_id_validation_prevents_spool_traversal_and_canonicalizes_uuid(tmp_path) -> None:
    spool_root = tmp_path / "spool"
    with pytest.raises(ValueError, match="valid UUID"):
        create_run(
            project="robotics",
            run_id="../../escaped",
            mode="offline",
            spool_root=spool_root,
        )
    with pytest.raises(DeliveryError, match="canonical UUID"):
        spool_module._Spool(spool_root, "../../escaped")

    assert not spool_root.exists()
    assert not (tmp_path / "escaped").exists()

    uppercase = "019C1234-5678-7000-8000-000000000029"
    run = create_run(
        project="robotics",
        run_id=uppercase,
        mode="offline",
        spool_root=spool_root,
    )
    run.finish()
    assert run.id == uppercase.lower()
    assert (spool_root / uppercase.lower()).is_dir()


def test_sync_rejects_a_noncanonical_spool_directory_before_reading_metadata(tmp_path) -> None:
    directory = tmp_path / "not-a-run-id"
    directory.mkdir()
    (directory / "run.json").write_text("{}")

    with pytest.raises(DeliveryError, match="canonical UUID"):
        sync_spool(directory)


def test_spool_rejects_a_symlinked_journal_without_touching_its_target(tmp_path) -> None:
    run_id = "019c1234-5678-7000-8000-000000000030"
    run_directory = tmp_path / run_id
    run_directory.mkdir()
    target = tmp_path / "outside.jsonl"
    target.write_bytes(b"outside\n")
    try:
        (run_directory / "events.jsonl").symlink_to(target)
    except (NotImplementedError, OSError):
        pytest.skip("symbolic links are not available on this Windows runner")

    with pytest.raises(DeliveryError, match="regular non-symbolic file"):
        spool_module._Spool(tmp_path, run_id)

    assert target.read_bytes() == b"outside\n"


def test_spool_bounds_metadata_delivery_and_journal_recovery_reads(tmp_path) -> None:
    spool = spool_module._Spool(tmp_path, "019c1234-5678-7000-8000-000000000031")
    spool.metadata_path.write_bytes(b"{" + b" " * spool_module._MAX_RUN_METADATA_BYTES)
    with pytest.raises(DeliveryError, match="spool file exceeds"):
        spool.read_metadata()

    spool.append({"sequence": 1, "step": 0, "timestamp_ms": 0, "metrics": {"x": 1}})
    spool.delivery_path.write_bytes(b"{" + b" " * spool_module._MAX_DELIVERY_BYTES)
    with pytest.raises(DeliveryError, match="spool file exceeds"):
        spool.read_batch(1, request_byte_budget=_METRIC_REQUEST_BUDGET)

    spool.delivery_path.unlink()
    oversized = b"x" * (spool_module._MAX_JOURNAL_RECORD_BYTES + 1) + b"\n"
    spool.events_path.write_bytes(oversized)
    with pytest.raises(DeliveryError, match="journal record exceeds"):
        spool.read_batch(1, request_byte_budget=_METRIC_REQUEST_BUDGET)
    with pytest.raises(DeliveryError, match="journal record exceeds"):
        spool.last_point()
    with pytest.raises(DeliveryError, match="journal record exceeds"):
        spool.recover_summary(
            {},
            False,
            0,
            max_tail_records=1,
            max_tail_bytes=len(oversized),
        )


def test_spool_rejects_an_incomplete_journal_record(tmp_path) -> None:
    spool = spool_module._Spool(tmp_path, "019c1234-5678-7000-8000-000000000032")
    spool.events_path.write_bytes(b'{"sequence":1}')

    with pytest.raises(DeliveryError, match="journal record is incomplete"):
        spool.read_batch(1, request_byte_budget=_METRIC_REQUEST_BUDGET)

    spool.events_path.write_bytes(b"\xff\n")
    with pytest.raises(DeliveryError, match="invalid journal event"):
        spool.read_batch(1, request_byte_budget=_METRIC_REQUEST_BUDGET)


def test_dense_metric_points_validate_before_the_durable_append(tmp_path) -> None:
    astral_character = "\U0001f600"
    dense = {f"{index:03d}{astral_character * 63}x": index for index in range(256)}
    run = create_run(
        project="robotics",
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )

    run.log(dense)

    event = json.loads((tmp_path / run.id / "events.jsonl").read_text())
    assert len(event["metrics"]) == 256
    assert all(len(key.encode("utf-8")) == 256 for key in event["metrics"])
    run.finish()


@pytest.mark.parametrize(
    ("metrics", "message"),
    [
        ({f"metric-{index}": index for index in range(257)}, "1 to 256"),
        ({"k" * 257: 1.0}, "1 to 256 non-control bytes"),
        ({"bad\u0085key": 1.0}, "non-control bytes"),
        ({"loss": float("nan")}, "finite"),
        ({"loss": 10**10_000}, "finite"),
    ],
)
def test_invalid_metric_points_never_touch_the_journal(tmp_path, metrics, message) -> None:
    run = create_run(
        project="robotics",
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )

    with pytest.raises(ValueError, match=message):
        run.log(metrics)

    assert (tmp_path / run.id / "events.jsonl").read_bytes() == b""
    run.finish()


def test_metric_delivery_splits_on_the_exact_encoded_request_budget(tmp_path) -> None:
    spool = spool_module._Spool(tmp_path, "019c1234-5678-7000-8000-000000000033")
    points = [
        {
            "sequence": sequence,
            "step": sequence - 1,
            "timestamp_ms": sequence,
            "metrics": {f"{index:03d}-{'m' * 180}": float(sequence) for index in range(100)},
        }
        for sequence in range(1, 6)
    ]
    for point in points:
        spool.append(point)
    two_point_request = {
        "batch_sequence": points[0]["sequence"],
        "points": points[:2],
    }
    budget = len(encode_json_request(two_point_request))

    first, first_offset = spool.read_batch(1_024, request_byte_budget=budget)
    reopened = spool_module._Spool(
        tmp_path,
        "019c1234-5678-7000-8000-000000000033",
    )
    replay, replay_offset = reopened.read_batch(1, request_byte_budget=budget)

    assert first == points[:2]
    assert replay == first
    assert replay_offset == first_offset
    assert len(encode_json_request(two_point_request)) == budget
    delivery = json.loads(spool.delivery_path.read_text())
    assert delivery["request_bytes"] == budget
    reopened.acknowledge(first_offset)
    second, _ = reopened.read_batch(1_024, request_byte_budget=budget)
    assert second == points[2:4]


def test_single_metric_event_over_the_delivery_budget_is_not_persisted_as_a_boundary(
    tmp_path,
) -> None:
    spool = spool_module._Spool(tmp_path, "019c1234-5678-7000-8000-000000000034")
    spool.append(
        {
            "sequence": 1,
            "step": 0,
            "timestamp_ms": 1,
            "metrics": {"loss": 1.0},
        }
    )

    with pytest.raises(DeliveryError, match="request byte budget"):
        spool.read_batch(1, request_byte_budget=32)

    assert not spool.delivery_path.exists()
    assert not spool.ack_path.exists()


def test_summary_recovery_rejects_a_tail_beyond_its_record_bound(tmp_path) -> None:
    spool = spool_module._Spool(tmp_path, "019c1234-5678-7000-8000-000000000038")
    for sequence in range(1, _SUMMARY_CHECKPOINT_RECORD_INTERVAL + 2):
        spool.append(
            {
                "sequence": sequence,
                "step": sequence - 1,
                "timestamp_ms": sequence,
                "metrics": {"loss": float(sequence)},
            }
        )

    with pytest.raises(DeliveryError, match="exceeds 128 records"):
        spool.recover_summary(
            {},
            False,
            0,
            max_tail_records=_SUMMARY_CHECKPOINT_RECORD_INTERVAL,
            max_tail_bytes=spool.events_path.stat().st_size,
        )


def test_delivery_worker_retries_the_identical_durable_metric_request(tmp_path) -> None:
    spool = spool_module._Spool(tmp_path, "019c1234-5678-7000-8000-000000000037")
    for sequence in range(1, 4):
        spool.append(
            {
                "sequence": sequence,
                "step": sequence - 1,
                "timestamp_ms": sequence,
                "metrics": {"loss": 1.0 / sequence},
            }
        )
    attempts: list[bytes] = []

    class RetryClient:
        def ingest_batch(self, run_id, request):
            attempts.append(encode_json_request(request))
            if len(attempts) == 1:
                raise RuntimeError("response lost after commit")
            return {"stop_requested": False}

    worker = _DeliveryWorker(
        client=RetryClient(),  # type: ignore[arg-type]
        run_id="019c1234-5678-7000-8000-000000000037",
        spool=spool,
        batch_size=3,
        flush_interval=0,
        stop_requested=lambda: None,
    )
    worker.start()
    worker.stop()
    worker.join(2)

    assert not worker.is_alive()
    assert attempts[0] == attempts[1]
    assert len(attempts[0]) <= _METRIC_REQUEST_BUDGET
    assert not spool.pending_metrics()
    assert not spool.delivery_path.exists()


def test_summary_resume_replays_a_bounded_tail_without_consulting_ack(tmp_path) -> None:
    run_id = "019c1234-5678-7000-8000-000000000035"
    first = create_run(
        project="robotics",
        run_id=run_id,
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )
    first.log({"loss": 1.0, "reward": 1.0})
    first.summary["loss"] = 99.0
    checkpoint = json.loads((tmp_path / run_id / "run.json").read_text())["summary_event_offset"]
    first.log({"loss": 2.0, "reward": 2.0})
    journal_size = (tmp_path / run_id / "events.jsonl").stat().st_size
    (tmp_path / run_id / "ack").write_text(str(journal_size))
    del first

    resumed = create_run(
        project="robotics",
        run_id=run_id,
        mode="offline",
        resume="allow",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )

    assert checkpoint < journal_size
    assert resumed.summary.to_dict() == {"loss": 99.0, "reward": 2.0}
    assert (
        json.loads((tmp_path / run_id / "run.json").read_text())["summary_event_offset"]
        == journal_size
    )
    resumed.finish()


def test_metric_summary_checkpoint_interval_bounds_crash_recovery_tail(tmp_path) -> None:
    run = create_run(
        project="robotics",
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )
    metadata_path = tmp_path / run.id / "run.json"
    for step in range(_SUMMARY_CHECKPOINT_RECORD_INTERVAL - 1):
        run.log({"loss": float(step)})
    assert json.loads(metadata_path.read_text())["summary_event_offset"] == 0

    run.log({"loss": 999.0})

    assert (
        json.loads(metadata_path.read_text())["summary_event_offset"]
        == (tmp_path / run.id / "events.jsonl").stat().st_size
    )
    run.finish()


def test_metric_summary_byte_interval_checkpoints_before_the_record_limit(tmp_path) -> None:
    run = create_run(
        project="robotics",
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )
    metadata_path = tmp_path / run.id / "run.json"
    journal_path = tmp_path / run.id / "events.jsonl"
    dense = {f"{index:03d}-{'b' * 252}": float(index) for index in range(256)}
    record_count = 0

    while json.loads(metadata_path.read_text())["summary_event_offset"] == 0:
        run.log(dense)
        record_count += 1
        assert record_count < _SUMMARY_CHECKPOINT_RECORD_INTERVAL

    assert journal_path.stat().st_size >= _SUMMARY_CHECKPOINT_BYTE_INTERVAL
    assert (
        json.loads(metadata_path.read_text())["summary_event_offset"] == journal_path.stat().st_size
    )
    run.finish()


@pytest.mark.parametrize("offset", [None, 1, 10**9])
def test_resume_rejects_a_malformed_summary_event_offset(tmp_path, offset) -> None:
    run_id = "019c1234-5678-7000-8000-000000000036"
    run = create_run(
        project="robotics",
        run_id=run_id,
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )
    run.log({"loss": 1.0})
    metadata_path = tmp_path / run_id / "run.json"
    metadata = json.loads(metadata_path.read_text())
    metadata["summary_event_offset"] = offset
    metadata_path.write_text(json.dumps(metadata))
    del run

    with pytest.raises(DeliveryError, match="summary event offset"):
        create_run(
            project="robotics",
            run_id=run_id,
            mode="offline",
            resume="allow",
            spool_root=tmp_path,
            system_monitor_interval=0,
        )


def test_metric_summary_is_bounded_and_explicit_values_keep_precedence(tmp_path) -> None:
    run = create_run(
        project="robotics",
        mode="offline",
        spool_root=tmp_path,
        system_monitor_interval=0,
    )
    run.log({f"k{index:03d}": float(index) for index in range(256)})
    run.log({"zzzz": 1.0})
    assert run.summary_truncated
    assert len(run.summary) == 256
    assert "zzzz" not in run.summary

    run.log({"a": 7.0, "k000": 8.0})
    run.summary["k000"] = 99.0
    run.log({"k000": 10.0, "reward": 11.0})

    assert run.summary["a"] == 7.0
    assert run.summary["k000"] == 99.0
    assert "k255" not in run.summary
    run.finish()


def test_online_finish_reclaims_acknowledged_payloads_and_keeps_private_metadata(tmp_path) -> None:
    run_id = "019c1234-5678-7000-8000-000000000099"

    def handler(request: httpx.Request) -> httpx.Response:
        body = (
            {}
            if "/blobs/" in request.url.path
            else json.loads(request.content)
            if request.content
            else {}
        )
        if request.url.path.endswith("/runs"):
            return httpx.Response(
                201,
                json={
                    "run": {"id": run_id, "name": "private", **_summary_fields()},
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if request.url.path.endswith("/batches"):
            return httpx.Response(
                201,
                json={
                    "run_id": run_id,
                    "batch_sequence": body["batch_sequence"],
                    "accepted_points": len(body["points"]),
                    "duplicate": False,
                    "metric_revision": 1,
                    "stop_requested": False,
                },
            )
        if "/blobs/" in request.url.path:
            content = request.read()
            return httpx.Response(
                201,
                json={
                    "blob": {
                        "digest": request.url.path.rsplit("/", 1)[1],
                        "size": len(content),
                        "mime_type": request.headers["content-type"],
                        "file_name": request.headers.get("x-epochdeck-file-name"),
                    },
                    "duplicate": False,
                },
            )
        if request.url.path.endswith("/rich-values"):
            return httpx.Response(201, json={"value": body, "duplicate": False})
        if request.url.path.endswith("/finish"):
            return httpx.Response(
                200,
                json={
                    "run": {
                        "id": run_id,
                        "state": "finished",
                        **_summary_fields(metric={"loss": 1.0}),
                    }
                },
            )
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    run = create_run(
        project="robotics",
        run_id=run_id,
        mode="online",
        spool_root=tmp_path,
        flush_interval=0,
        system_monitor_interval=0,
        transport=httpx.MockTransport(handler),
    )
    run.log({"loss": 1.0, "preview": Image(b"image")})
    run.finish(timeout=2)

    spool = tmp_path / run_id
    if os.name != "nt":
        assert spool.stat().st_mode & 0o777 == 0o700
        assert (spool / "run.json").stat().st_mode & 0o777 == 0o600
    assert (spool / "events.jsonl").read_bytes() == b""
    assert (spool / "rich-values.jsonl").read_bytes() == b""
    assert list((spool / "blobs").iterdir()) == []


def test_mixed_log_validates_every_rich_record_before_appending_metrics(tmp_path) -> None:
    class InvalidMetadata(RichValue):
        def _prepare(self, blob_root):
            return PreparedRichValue(kind="histogram", blob=None, metadata={"bad": object()})

    run = create_run(project="robotics", mode="offline", spool_root=tmp_path)
    with pytest.raises(TypeError, match="unsupported JSON type"):
        run.log({"loss": 1.0, "invalid": InvalidMetadata()})
    assert (tmp_path / run.id / "events.jsonl").read_bytes() == b""
    assert (tmp_path / run.id / "rich-values.jsonl").read_bytes() == b""
    run.finish()


def test_audio_and_histogram_reject_ambiguous_metadata() -> None:
    with pytest.raises(ValueError, match="sample_rate"):
        Audio(b"audio", sample_rate=44.1)  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="negative"):
        Histogram(np_histogram=([-1], [0, 1]))
    with pytest.raises(ValueError, match="strictly increasing"):
        Histogram(np_histogram=([1, 2], [0, 1, 1]))


def test_run_context_preserves_the_training_exception_when_cleanup_fails(
    monkeypatch,
    tmp_path,
) -> None:
    run = create_run(project="robotics", mode="offline", spool_root=tmp_path)

    def fail_finish(**kwargs):
        raise DeliveryError("cleanup failed")

    monkeypatch.setattr(run, "finish", fail_finish)
    with pytest.raises(ValueError, match="training failed") as captured, run:
        raise ValueError("training failed")
    assert any("cleanup failed" in note for note in captured.value.__notes__)


def test_online_documents_use_authoritative_server_state(tmp_path) -> None:
    server_config: dict = {"seed": 1}
    server_summary: dict = {"status": "resumed"}
    requests: list[str] = []

    def response_run() -> dict:
        return {
            "id": "019c1234-5678-7000-8000-000000000003",
            "name": "resumed-run",
            "config": dict(server_config),
            **_summary_fields(explicit=server_summary),
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
            return httpx.Response(
                200,
                json={"run": {**response_run(), "state": "finished"}},
            )
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
                    "run": {"id": body["id"], "name": "training", **_summary_fields()},
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
                    "stop_requested": False,
                },
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(
                200,
                json={
                    "run": {
                        "id": run.id,
                        "state": "finished",
                        **_summary_fields(metric={"loss": 1.0}),
                    }
                },
            )
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
                    "run": {"id": run.id, "name": "rich-run"},
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
                        "file_name": request.headers.get("x-epochdeck-file-name"),
                    },
                    "duplicate": False,
                },
            )
        if request.url.path.endswith("/rich-values"):
            value = json.loads(request.content)
            received.append(value)
            return httpx.Response(201, json={"value": value, "duplicate": False})
        if request.url.path.endswith("/finish"):
            return httpx.Response(200, json={"run": {"id": run.id, "state": "finished"}})
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


def test_artifacts_upload_versions_and_durable_lineage_operations(tmp_path) -> None:
    checkpoint = tmp_path / "checkpoint.bin"
    checkpoint.write_bytes(b"checkpoint-content")
    artifact = Artifact(
        "policy",
        type="model",
        description="trained policy",
        metadata={"framework": "jax"},
    ).add_file(checkpoint, name="weights/checkpoint.bin")
    run = create_run(
        project="robotics",
        run_id="019c1234-5678-7000-8000-000000000010",
        mode="offline",
        spool_root=tmp_path / "spool",
        system_monitor_interval=0,
    )
    assert run.log_artifact(artifact) is artifact
    assert run.use_artifact(artifact) == artifact.id
    run.finish()
    directory = tmp_path / "spool" / run.id
    operations: list[tuple[str, dict]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/runs"):
            return httpx.Response(
                201,
                json={
                    "run": {"id": run.id, "name": "artifact-run"},
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if "/blobs/" in request.url.path:
            digest = request.url.path.rsplit("/", 1)[1]
            content = request.read()
            operations.append(("blob", {"digest": digest, "content": content}))
            return httpx.Response(
                201,
                json={
                    "blob": {
                        "digest": digest,
                        "size": len(content),
                        "mime_type": request.headers["content-type"],
                        "file_name": request.headers.get("x-epochdeck-file-name"),
                    },
                    "duplicate": False,
                },
            )
        if request.url.path.endswith("/artifacts/use"):
            body = json.loads(request.content)
            operations.append(("use", body))
            return httpx.Response(200, json={"id": body["artifact_id"]})
        if request.url.path.endswith("/artifacts"):
            body = json.loads(request.content)
            operations.append(("create", body))
            return httpx.Response(
                201,
                json={"artifact": {"id": body["id"], "version": 0}, "duplicate": False},
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(200, json={"run": {"id": run.id, "state": "finished"}})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    sync_spool(directory, transport=httpx.MockTransport(handler), timeout=2)

    assert [operation for operation, _ in operations] == ["blob", "create", "use"]
    create = operations[1][1]
    assert create["name"] == "policy"
    assert create["type"] == "model"
    assert create["aliases"] == ["latest"]
    assert create["entries"][0]["path"] == "weights/checkpoint.bin"
    assert operations[0][1]["content"] == b"checkpoint-content"
    assert operations[2][1] == {"artifact_id": artifact.id}
    assert (
        int((directory / "artifact-ack").read_text())
        == (directory / "artifacts.jsonl").stat().st_size
    )


def test_structured_traces_preserve_tree_payloads_and_search_previews(tmp_path) -> None:
    run = create_run(
        project="agents",
        run_id="019c1234-5678-7000-8000-000000000020",
        mode="offline",
        resume="never",
        spool_root=tmp_path,
    )
    run.log({"reward": 1.0}, step=12)
    root = run.trace(
        "answer-question",
        kind="agent",
        attributes={"model": "local-model"},
        inputs={"question": "What is the reward?"},
    )
    root.add_message("user", "What is the reward?")
    with run.trace("lookup-reward", kind="tool", parent=root) as child:
        child.set_inputs({"metric": "reward"})
        child.set_outputs({"value": 1.0})
    root.add_message("assistant", "The reward is 1.0")
    root.finish(outputs={"answer": "1.0"})
    run.finish()

    directory = tmp_path / run.id
    records = [json.loads(line) for line in (directory / "traces.jsonl").read_text().splitlines()]
    child_record, root_record = records
    assert child_record["trace_id"] == root.id
    assert child_record["parent_span_id"] == root.id
    assert child_record["step"] == 12
    assert root_record["preview"]["message_count"] == 2
    assert root_record["preview"]["messages"][1] == {
        "content": "The reward is 1.0",
        "role": "assistant",
    }
    payload_path = directory / "blobs" / root_record["payload"]["digest"]
    assert json.loads(payload_path.read_text()) == {
        "inputs": {"question": "What is the reward?"},
        "messages": [
            {"content": "What is the reward?", "role": "user"},
            {"content": "The reward is 1.0", "role": "assistant"},
        ],
        "outputs": {"answer": "1.0"},
    }


def test_trace_sync_replays_the_same_span_after_response_loss(tmp_path) -> None:
    run = create_run(
        project="agents",
        run_id="019c1234-5678-7000-8000-000000000021",
        mode="offline",
        resume="never",
        spool_root=tmp_path,
    )
    with run.trace("generate", kind="llm", inputs={"prompt": "hello"}) as span:
        span.add_message("assistant", "hello back")
        span.set_outputs({"tokens": 2})
    run.finish()

    attempts = 0
    uploaded_payloads: list[bytes] = []
    delivered_spans: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        if request.url.path.endswith("/runs"):
            return httpx.Response(
                201,
                json={
                    "run": {"id": run.id, "name": "trace-run"},
                    "resumed": False,
                    "next_sequence": 1,
                    "next_step": 0,
                },
            )
        if "/blobs/" in request.url.path:
            uploaded_payloads.append(request.read())
            content = uploaded_payloads[-1]
            digest = request.url.path.rsplit("/", 1)[1]
            return httpx.Response(
                201,
                json={
                    "blob": {
                        "digest": digest,
                        "size": len(content),
                        "mime_type": request.headers["content-type"],
                        "file_name": request.headers.get("x-epochdeck-file-name"),
                    },
                    "duplicate": attempts > 0,
                },
            )
        if request.url.path.endswith("/traces"):
            delivered_spans.append(json.loads(request.content))
            attempts += 1
            if attempts == 1:
                raise httpx.ReadError("response lost", request=request)
            return httpx.Response(
                200,
                json={"span": delivered_spans[-1], "duplicate": True},
            )
        if request.url.path.endswith("/finish"):
            return httpx.Response(200, json={"run": {"id": run.id, "state": "finished"}})
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    directory = tmp_path / run.id
    sync_spool(directory, transport=httpx.MockTransport(handler), timeout=3)

    assert attempts == 2
    assert delivered_spans[0] == delivered_spans[1]
    assert delivered_spans[0]["id"] == span.id
    assert len(uploaded_payloads) == 2
    assert uploaded_payloads[0] == uploaded_payloads[1]
    assert int((directory / "trace-ack").read_text()) == (directory / "traces.jsonl").stat().st_size
