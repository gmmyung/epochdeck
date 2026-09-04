from __future__ import annotations

import json
import math
import os
import threading
import time
from collections.abc import Iterator, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

import pytest

import epochdeck.wandb_importer as importer_module
from epochdeck._wandb_state import Checkpoint, ImportCancellation, ImportCancelled
from epochdeck.client import EpochDeckApiError
from epochdeck.wandb_importer import _history_page_size, import_wandb_runs


class CommError(Exception):
    __module__ = "wandb.errors.errors"


class FakeFile:
    name = "media/rollout.mp4"

    def download(self, *, root: str, replace: bool):
        assert replace is True
        destination = Path(root) / self.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"video")
        return SimpleNamespace(name=str(destination))


class FakeArtifactFile:
    name = "checkpoint/model.bin"

    def download(self, *, root: str, replace: bool):
        assert replace is True
        destination = Path(root) / self.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"checkpoint")
        return SimpleNamespace(name=str(destination))


class FakeArtifact:
    id = "artifact-1"
    name = "policy:v3"
    qualified_name = "team/demo/policy:v3"
    version = "v3"
    type = "checkpoint"
    aliases: ClassVar[list[str]] = ["latest", "best"]
    description = "best policy"
    metadata: ClassVar[dict] = {"score": 4.0}

    def files(self, *, per_page: int):
        assert per_page == 100
        return iter([FakeArtifactFile()])


class FakeSummaryValue:
    _json_dict: ClassVar[dict] = {"label": "complete"}


class FakeSummary:
    _json_dict: ClassVar[dict] = {
        "result": FakeSummaryValue(),
    }


class FakeRun:
    id = "source-1"
    name = "source run"
    url = "https://wandb.ai/team/demo/runs/source-1"
    state = "finished"
    updated_at = "2026-08-28T00:00:00Z"
    config: ClassVar[dict] = {"seed": 7}
    summary: ClassVar[FakeSummary] = FakeSummary()
    lastHistoryStep = 1_000_000
    historyLineCount = 3

    def scan_history(self, *, page_size: int):
        assert page_size == 3_907
        return iter(
            [
                {
                    "_step": 0,
                    "_timestamp": 10.0,
                    "loss": 2.0,
                    "label": "ignored",
                    "rollout": {
                        "_type": "video-file",
                        "path": FakeFile.name,
                        "caption": "policy rollout",
                        "sha256": (
                            "0cab1c9617404faf2b24e221e189ca5945813e14d3f766345b09ca13bbe28ffc"
                        ),
                    },
                },
                {"_step": 1, "_timestamp": 11.0, "loss": 1.0},
                {"_step": 2, "_timestamp": 12.0, "nested": {"reward": 4.0}},
            ]
        )

    def file(self, name: str):
        assert name == FakeFile.name
        return FakeFile()

    def files(self):
        return iter([FakeFile()])

    def logged_artifacts(self, *, per_page: int):
        assert per_page == 100
        return iter([FakeArtifact()])


class FakeSourceApi:
    def __init__(self) -> None:
        self.flush_count = 0
        self.run_paths: list[str] = []

    def runs(self, path: str):
        assert path == "team/demo"
        return iter([FakeRun()])

    def flush(self) -> None:
        self.flush_count += 1

    def run(self, path: str):
        assert path == "team/demo/source-1"
        self.run_paths.append(path)
        return FakeRun()


class MediaTracker:
    def __init__(self, *, total: int, fail_index: int | None = None) -> None:
        self.total = total
        self.fail_index = fail_index
        self.lock = threading.Lock()
        self.release_tail = threading.Event()
        self.active = 0
        self.max_active = 0
        self.finished = 0
        self.yielded = 0
        self.max_rows_ahead = 0


class ConcurrentMediaFile:
    def __init__(self, tracker: MediaTracker, index: int) -> None:
        self._tracker = tracker
        self._index = index
        self.name = f"media/rollout-{index:04d}.mp4"

    def download(self, *, root: str, replace: bool):
        assert replace is True
        tracker = self._tracker
        with tracker.lock:
            tracker.active += 1
            tracker.max_active = max(tracker.max_active, tracker.active)
        try:
            if self._index == tracker.fail_index:
                time.sleep(0.03)
                raise OSError("media download failed")
            if self._index >= tracker.total - importer_module._MEDIA_WORKERS:
                tracker.release_tail.wait(timeout=2)
            else:
                time.sleep(0.01)
            destination = Path(root) / self.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes(f"video-{self._index}".encode())
            return SimpleNamespace(name=str(destination))
        finally:
            with tracker.lock:
                tracker.active -= 1
                tracker.finished += 1


class ConcurrentMediaRun:
    name = "parallel media"
    url = "https://wandb.ai/team/demo/runs/media-source"
    state = "finished"
    updated_at = "2026-08-30T00:00:00Z"
    config: ClassVar[dict] = {"seed": 11}
    summary: ClassVar[dict] = {"result": "complete"}

    def __init__(self, tracker: MediaTracker) -> None:
        self.id = "media-source"
        self._tracker = tracker
        self.lastHistoryStep = tracker.total - 1
        self.historyLineCount = tracker.total

    def scan_history(self, *, page_size: int):
        assert page_size == 1_000
        for index in range(self._tracker.total):
            with self._tracker.lock:
                self._tracker.yielded += 1
                ahead = self._tracker.yielded - self._tracker.finished
                self._tracker.max_rows_ahead = max(self._tracker.max_rows_ahead, ahead)
            yield {
                "_step": index,
                "loss": float(index),
                "rollout": {
                    "_type": "video-file",
                    "path": f"media/rollout-{index:04d}.mp4",
                },
            }

    def file(self, name: str):
        index = int(Path(name).stem.rsplit("-", 1)[1])
        return ConcurrentMediaFile(self._tracker, index)

    def files(self):
        return iter(())

    def logged_artifacts(self, *, per_page: int):
        assert per_page == 100
        return iter(())


class OneRunSourceApi:
    def __init__(self, run: object, refreshed_runs: list[object] | None = None) -> None:
        self._run = run
        self._refreshed_runs = list(refreshed_runs) if refreshed_runs is not None else [run]
        self.flush_count = 0
        self.run_paths: list[str] = []

    def runs(self, path: str):
        assert path == "team/demo"
        return iter([self._run])

    def flush(self) -> None:
        self.flush_count += 1

    def run(self, path: str):
        assert path == f"team/demo/{self._run.id}"
        self.run_paths.append(path)
        if len(self._refreshed_runs) > 1:
            return self._refreshed_runs.pop(0)
        return self._refreshed_runs[0]


class FakeClient:
    def __init__(self) -> None:
        self.batches: list[dict] = []
        self.fail_first_batch = True
        self.expected_unsupported = 1
        self.finished = False
        self.artifacts: list[dict] = []
        self.rich_values: list[dict] = []
        self.run: dict | None = None

    def get_run(self, run_id: str):
        if self.run is None:
            raise EpochDeckApiError(404, "not_found", "missing")
        return self.run

    def create_run(self, **request):
        self.run = {
            "id": request["run_id"],
            "project": request["project"],
            "state": "running",
            "config": request["config"],
        }
        return {
            "run": self.run,
            "next_sequence": 1,
            "next_step": 0,
        }

    def ingest_batch(self, run_id: str, batch: dict):
        self.batches.append(batch)
        if self.fail_first_batch:
            self.fail_first_batch = False
            raise ConnectionError("response lost after commit")
        return {"duplicate": len(self.batches) > 1, "next_sequence": 4}

    def upload_blob(self, path: Path, blob: dict):
        assert path.read_bytes() in {b"video", b"checkpoint"}
        return {"blob": blob, "duplicate": False}

    def create_rich_value(self, run_id: str, value: dict):
        self.rich_values.append(value)
        return {"value": value, "duplicate": len(self.rich_values) > 1}

    def create_artifact(self, run_id: str, artifact: dict):
        self.artifacts.append(artifact)
        return {"artifact": artifact, "duplicate": False}

    def finish_run(self, run_id: str, summary: dict):
        self.finished = True
        assert self.run is not None
        self.run["state"] = "finished"
        self.run["summary"] = summary
        assert (
            summary["_epochdeck_wandb_source"]["unsupported_history_values"]
            == self.expected_unsupported
        )
        return {"run": {"id": run_id, "state": "finished"}}


class ParallelMediaClient(FakeClient):
    def __init__(self, tracker: MediaTracker) -> None:
        super().__init__()
        self.fail_first_batch = False
        self.expected_unsupported = 0
        self._tracker = tracker
        self._rich_lock = threading.Lock()
        self.rich_ids: set[str] = set()
        self.finished_media_at_ingest: list[int] = []

    def ingest_batch(self, run_id: str, batch: dict):
        with self._tracker.lock:
            self.finished_media_at_ingest.append(self._tracker.finished)
        self._tracker.release_tail.set()
        return super().ingest_batch(run_id, batch)

    def upload_blob(self, path: Path, blob: dict):
        assert path.is_file()
        return {"blob": blob, "duplicate": False}

    def create_rich_value(self, run_id: str, value: dict):
        with self._rich_lock:
            duplicate = value["id"] in self.rich_ids
            self.rich_ids.add(value["id"])
            self.rich_values.append(value)
        return {"value": value, "duplicate": duplicate}


class EmptySourceApi:
    def runs(self, path: str):
        assert path == "team/demo"
        return iter(())


class BlockingEmptySourceApi:
    def __init__(self) -> None:
        self.entered = threading.Event()
        self.release = threading.Event()

    def runs(self, path: str):
        assert path == "team/demo"
        self.entered.set()
        assert self.release.wait(2)
        return iter(())


class InterruptingRun(FakeRun):
    id = "interrupting-run"
    name = "interrupting run"
    url = "https://wandb.ai/team/demo/runs/interrupting-run"

    def scan_history(self, *, page_size: int):
        raise KeyboardInterrupt


class CountingSourceApi:
    def __init__(self, runs: list[object]) -> None:
        self._runs = runs
        self.yielded = 0

    def runs(self, path: str):
        assert path == "team/demo"
        for run in self._runs:
            self.yielded += 1
            yield run

    def flush(self) -> None:
        pass

    def run(self, path: str):
        source_id = path.rsplit("/", 1)[-1]
        return next(run for run in self._runs if getattr(run, "id", None) == source_id)


class TempFileTracker:
    def __init__(self) -> None:
        self.maximum_files = 0
        self.events: list[str] = []


class TrackedFile:
    def __init__(self, tracker: TempFileTracker, name: str) -> None:
        self._tracker = tracker
        self.name = name

    def download(self, *, root: str, replace: bool):
        assert replace is True
        destination = Path(root) / self.name
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(self.name.encode())
        present = sum(path.is_file() for path in Path(root).rglob("*"))
        self._tracker.maximum_files = max(self._tracker.maximum_files, present)
        self._tracker.events.append(f"download:{self.name}")
        return SimpleNamespace(name=str(destination))


class TempBoundClient(FakeClient):
    def __init__(self, tracker: TempFileTracker) -> None:
        super().__init__()
        self._tracker = tracker

    def upload_blob(self, path: Path, blob: dict):
        assert path.is_file()
        self._tracker.events.append(f"upload:{blob['file_name']}")
        return {"blob": blob, "duplicate": False}


class TrackedArtifact(FakeArtifact):
    def __init__(self, tracker: TempFileTracker) -> None:
        self._tracker = tracker

    def files(self, *, per_page: int):
        assert per_page == 100
        return iter(
            [
                TrackedFile(self._tracker, "checkpoints/one.bin"),
                TrackedFile(self._tracker, "checkpoints/two.bin"),
            ]
        )


def test_wandb_read_retries_only_transient_comm_errors(monkeypatch) -> None:
    monkeypatch.setattr(importer_module, "_WANDB_RETRY_INITIAL_SECONDS", 0.0)
    cancellation = ImportCancellation()
    attempts = 0

    def transient_read() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise CommError("the service process is busy")
        return "ready"

    assert (
        importer_module._retry_wandb_read(
            transient_read,
            cancellation,
            "W&B test read",
        )
        == "ready"
    )
    assert attempts == 3

    class AuthenticationError(CommError):
        pass

    AuthenticationError.__module__ = "wandb.errors.errors"
    authentication_attempts = 0

    def authentication_failure() -> None:
        nonlocal authentication_attempts
        authentication_attempts += 1
        raise AuthenticationError("invalid credentials")

    with pytest.raises(AuthenticationError):
        importer_module._retry_wandb_read(
            authentication_failure,
            cancellation,
            "W&B authenticated read",
        )
    assert authentication_attempts == 1


def test_wandb_retry_is_bounded_and_cancellation_aware(monkeypatch) -> None:
    monkeypatch.setattr(importer_module, "_WANDB_RETRY_INITIAL_SECONDS", 0.0)
    cancellation = ImportCancellation()
    attempts = 0

    def unavailable_read() -> None:
        nonlocal attempts
        attempts += 1
        raise CommError("the service process is busy")

    with pytest.raises(importer_module.WandbImportError, match="failed after 5 attempts"):
        importer_module._retry_wandb_read(
            unavailable_read,
            cancellation,
            "W&B unavailable read",
        )
    assert attempts == importer_module._WANDB_READ_ATTEMPTS

    attempts = 0

    def cancel_during_read() -> None:
        nonlocal attempts
        attempts += 1
        cancellation.cancel()
        raise CommError("the service process is busy")

    with pytest.raises(ImportCancelled):
        importer_module._retry_wandb_read(
            cancel_during_read,
            cancellation,
            "W&B cancelled read",
        )
    assert attempts == 1


def test_wandb_iterator_restarts_without_duplicating_emitted_items(monkeypatch) -> None:
    monkeypatch.setattr(importer_module, "_WANDB_RETRY_INITIAL_SECONDS", 0.0)
    attempts = 0

    def records() -> Iterator[int]:
        nonlocal attempts
        attempts += 1
        attempt = attempts
        yield 1
        yield 2
        if attempt == 1:
            raise CommError("the service process is busy")
        yield 3

    values = list(
        importer_module._retrying_wandb_reads(
            records,
            ImportCancellation(),
            "W&B test listing",
        )
    )

    assert values == [1, 2, 3]
    assert attempts == 2


def test_wandb_iterator_rejects_identity_changes_while_repositioning(monkeypatch) -> None:
    monkeypatch.setattr(importer_module, "_WANDB_RETRY_INITIAL_SECONDS", 0.0)
    attempts = 0

    def records() -> Iterator[str]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            yield "run-a"
            raise CommError("the service process is busy")
        yield from ("run-new", "run-a", "run-b")

    with pytest.raises(
        importer_module.WandbImportError,
        match="changed identity or order while resuming",
    ):
        list(
            importer_module._retrying_wandb_reads(
                records,
                ImportCancellation(),
                "W&B test listing",
                resume_key=lambda value: value,
            )
        )

    assert attempts == 2


def test_history_scan_resumes_in_process_after_transient_comm_error(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(importer_module, "_WANDB_RETRY_INITIAL_SECONDS", 0.0)

    class TransientHistoryRun(FakeRun):
        def __init__(self) -> None:
            self.scan_attempts = 0

        def scan_history(self, *, page_size: int):
            assert page_size == 3_907
            self.scan_attempts += 1
            attempt = self.scan_attempts
            yield {"_step": 0, "loss": 3.0}
            yield {"_step": 1, "loss": 2.0}
            if attempt == 1:
                raise CommError("the service process is busy and did not respond in time")
            yield {"_step": 2, "loss": 1.0}

    source = TransientHistoryRun()
    client = FakeClient()
    client.fail_first_batch = False
    client.expected_unsupported = 0

    result = import_wandb_runs(
        OneRunSourceApi(source),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
        include_files=False,
    )

    assert result.completed == 1
    assert source.scan_attempts == 2
    assert [point["metrics"]["loss"] for batch in client.batches for point in batch["points"]] == [
        3.0,
        2.0,
        1.0,
    ]


def test_history_scan_rejects_reordered_rows_after_transient_failure(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(importer_module, "_WANDB_RETRY_INITIAL_SECONDS", 0.0)

    class ReorderedHistoryRun(FakeRun):
        def __init__(self) -> None:
            self.scan_attempts = 0

        def scan_history(self, *, page_size: int):
            assert page_size == 3_907
            self.scan_attempts += 1
            if self.scan_attempts == 1:
                yield {"_step": 0, "loss": 3.0}
                yield {"_step": 1, "loss": 2.0}
                raise CommError("the service process is busy")
            yield {"_step": 1, "loss": 2.0}
            yield {"_step": 0, "loss": 3.0}
            yield {"_step": 2, "loss": 1.0}

    source = ReorderedHistoryRun()
    client = FakeClient()
    client.fail_first_batch = False

    result = import_wandb_runs(
        OneRunSourceApi(source),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
        include_files=False,
    )

    assert result.failed == 1
    assert "changed identity or order" in result.failures[0]
    assert client.batches == []
    assert client.finished is False


def test_history_scan_must_match_authoritative_row_count(tmp_path) -> None:
    class ShortHistoryRun(FakeRun):
        historyLineCount = 4

    client = FakeClient()
    client.fail_first_batch = False

    result = import_wandb_runs(
        OneRunSourceApi(ShortHistoryRun()),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
        include_files=False,
    )

    assert result.failed == 1
    assert "yielded 3 rows but historyLineCount declares 4" in result.failures[0]
    assert client.finished is False
    checkpoint = Checkpoint(
        tmp_path / "checkpoint.jsonl",
        entity="team",
        project="demo",
        target_project="imported",
    )
    assert checkpoint.state(FakeRun.id)["phase"] == "history"
    assert checkpoint.state(FakeRun.id)["status"] == "failed"


def test_authoritative_empty_history_does_not_start_a_step_scan(tmp_path) -> None:
    class EmptyHistoryRun(FakeRun):
        id = "empty-history-source"
        name = "empty history source"
        url = "https://wandb.ai/team/demo/runs/empty-history-source"
        historyLineCount = 0
        lastHistoryStep = 10**15

        def scan_history(self, *, page_size: int):
            raise AssertionError(f"empty history started a scan with page size {page_size}")

    client = FakeClient()
    client.fail_first_batch = False
    client.expected_unsupported = 0

    result = import_wandb_runs(
        OneRunSourceApi(EmptyHistoryRun()),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
        include_files=False,
    )

    assert result.completed == 1
    assert client.batches == []
    assert client.finished is True


def test_media_download_retries_before_any_epochdeck_write(monkeypatch) -> None:
    monkeypatch.setattr(importer_module, "_WANDB_RETRY_INITIAL_SECONDS", 0.0)

    class TransientMediaFile(FakeFile):
        def __init__(self) -> None:
            self.download_attempts = 0

        def download(self, *, root: str, replace: bool):
            self.download_attempts += 1
            if self.download_attempts == 1:
                raise CommError("the service process is busy")
            return super().download(root=root, replace=replace)

    source_file = TransientMediaFile()
    client = FakeClient()
    importer_module._import_media_reference(
        source_file,
        client,
        run_id="run-1",
        source_metadata={
            "entity": "team",
            "project": "demo",
            "run_id": "source-1",
            "url": "https://wandb.ai/team/demo/runs/source-1",
        },
        key="rollout",
        kind="video",
        step=1,
        timestamp_ms=1_000,
        artifact_path=FakeFile.name,
        reference={"path": FakeFile.name},
        row_number=1,
        occurrence=0,
        cancellation=ImportCancellation(),
    )

    assert source_file.download_attempts == 2
    assert len(client.rich_values) == 1


def test_wandb_import_replays_a_lost_batch_and_resumes_from_checkpoint(tmp_path) -> None:
    checkpoint = tmp_path / "checkpoint.json"
    client = FakeClient()
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint,
        "workers": 1,
    }

    first = import_wandb_runs(FakeSourceApi(), client, **arguments)
    assert first.failed == 1
    assert first.completed == 0

    second = import_wandb_runs(FakeSourceApi(), client, **arguments)
    assert second.failed == 0
    assert second.completed == 1
    assert client.batches[0] == client.batches[1]
    assert client.batches[1]["points"][2]["metrics"] == {"nested/reward": 4.0}
    assert client.artifacts[0]["entries"][0]["path"] == "media/rollout.mp4"
    assert "version" not in client.artifacts[0]
    assert client.artifacts[1]["name"] == "policy"
    assert client.artifacts[1]["version"] == 3
    assert client.artifacts[1]["metadata"]["wandb_source"]["version"] == "v3"
    assert client.rich_values[-1]["kind"] == "video"
    assert client.rich_values[-1]["metadata"]["caption"] == "policy rollout"
    assert client.finished is True
    assert client.run is not None
    assert client.run["config"]["_epochdeck_wandb_source"]["state"] == "finished"
    assert client.run["config"]["_epochdeck_wandb_source"]["updated_at"] == "2026-08-28T00:00:00Z"

    third = import_wandb_runs(FakeSourceApi(), client, **arguments)
    assert third.skipped == 1
    assert len(client.batches) == 2


def test_refresh_failure_leaves_a_resumable_checkpoint(tmp_path) -> None:
    class FailRefreshOnceApi(OneRunSourceApi):
        def __init__(self, run: object) -> None:
            super().__init__(run)
            self.failed = False

        def run(self, path: str):
            if not self.failed:
                self.failed = True
                raise ValueError("malformed refresh response")
            return super().run(path)

    source_api = FailRefreshOnceApi(FakeRun())
    client = FakeClient()
    client.fail_first_batch = False
    client.expected_unsupported = 2
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint_path,
        "workers": 1,
        "include_files": False,
    }

    first = import_wandb_runs(source_api, client, **arguments)

    assert first.failed == 1
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    assert checkpoint.state(FakeRun.id)["phase"] == "history"
    assert checkpoint.state(FakeRun.id)["include_files"] is False

    second = import_wandb_runs(source_api, client, **arguments)

    assert second.completed == 1


@pytest.mark.parametrize(
    ("failure_type", "expected_phase"),
    [
        ("wandb-run-files", "run_files"),
        ("checkpoint", "logged_artifacts"),
    ],
)
def test_resource_failure_resumes_after_completed_history(
    failure_type,
    expected_phase,
    tmp_path,
) -> None:
    class ResourceRun(FakeRun):
        id = "resource-source"
        name = "resource source"
        url = "https://wandb.ai/team/demo/runs/resource-source"
        lastHistoryStep = 0
        historyLineCount = 1

        def __init__(self) -> None:
            self.scan_calls = 0

        def scan_history(self, *, page_size: int):
            assert page_size == 1_000
            self.scan_calls += 1
            return iter([{"_step": 0, "loss": 1.0}])

        def files(self):
            return iter([FakeArtifactFile()])

        def logged_artifacts(self, *, per_page: int):
            assert per_page == 100
            return iter([FakeArtifact()])

    class FailResourceOnceClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.fail_first_batch = False
            self.expected_unsupported = 0
            self.failed_resource = False

        def create_artifact(self, run_id: str, artifact: dict):
            if any(existing["id"] == artifact["id"] for existing in self.artifacts):
                return {"artifact": artifact, "duplicate": True}
            response = super().create_artifact(run_id, artifact)
            if artifact["type"] == failure_type and not self.failed_resource:
                self.failed_resource = True
                raise ConnectionError("resource response was lost")
            return response

    source = ResourceRun()
    client = FailResourceOnceClient()
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint_path,
        "workers": 1,
    }

    first = import_wandb_runs(OneRunSourceApi(source), client, **arguments)

    assert first.failed == 1
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    assert checkpoint.state(source.id)["phase"] == expected_phase

    second = import_wandb_runs(OneRunSourceApi(source), client, **arguments)

    assert second.completed == 1
    assert source.scan_calls == 1
    assert len({artifact["id"] for artifact in client.artifacts}) == len(client.artifacts)
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    assert checkpoint.state(source.id)["phase"] == "complete"


@pytest.mark.parametrize(
    ("target_problem", "message"),
    [
        ("missing", "target run does not exist"),
        ("running", "target run is not finished"),
        ("project", "deterministic EpochDeck run ID collision"),
        ("source", "deterministic EpochDeck run ID collision"),
    ],
)
def test_completed_checkpoint_revalidates_target_run(
    target_problem,
    message,
    tmp_path,
) -> None:
    client = FakeClient()
    client.fail_first_batch = False
    client.expected_unsupported = 2
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint_path,
        "workers": 1,
        "include_files": False,
    }
    assert import_wandb_runs(FakeSourceApi(), client, **arguments).completed == 1
    assert client.run is not None

    if target_problem == "missing":
        client.run = None
    elif target_problem == "running":
        client.run["state"] = "running"
    elif target_problem == "project":
        client.run["project"] = "other-project"
    else:
        client.run["config"]["_epochdeck_wandb_source"]["run_id"] = "other-source"

    resumed = import_wandb_runs(FakeSourceApi(), client, **arguments)

    assert resumed.failed == 1
    assert resumed.skipped == 0
    assert message in resumed.failures[0]


def test_lost_finish_response_recovers_only_from_finalize_phase(tmp_path) -> None:
    class LostFinishResponseClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.fail_first_batch = False
            self.expected_unsupported = 2
            self.lost_finish_response = False

        def finish_run(self, run_id: str, summary: dict):
            response = super().finish_run(run_id, summary)
            if not self.lost_finish_response:
                self.lost_finish_response = True
                raise ConnectionError("finish response was lost")
            return response

    client = LostFinishResponseClient()
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint_path,
        "workers": 1,
        "include_files": False,
    }

    first = import_wandb_runs(FakeSourceApi(), client, **arguments)
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    assert first.failed == 1
    assert checkpoint.state(FakeRun.id)["phase"] == "finalize"

    second = import_wandb_runs(FakeSourceApi(), client, **arguments)

    assert second.skipped == 1
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    assert checkpoint.state(FakeRun.id)["phase"] == "complete"


def test_finalize_recovery_rejects_a_finished_run_with_the_wrong_summary(tmp_path) -> None:
    class LostFinishResponseClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.fail_first_batch = False
            self.expected_unsupported = 2

        def finish_run(self, run_id: str, summary: dict):
            super().finish_run(run_id, summary)
            raise ConnectionError("finish response was lost")

    client = LostFinishResponseClient()
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint_path,
        "workers": 1,
        "include_files": False,
    }
    assert import_wandb_runs(FakeSourceApi(), client, **arguments).failed == 1
    assert client.run is not None
    client.run["summary"] = {}

    resumed = import_wandb_runs(FakeSourceApi(), client, **arguments)

    assert resumed.failed == 1
    assert "does not contain the expected imported W&B summary" in resumed.failures[0]


def test_checkpoint_rejects_include_files_contract_change(tmp_path) -> None:
    client = FakeClient()
    client.fail_first_batch = False
    client.expected_unsupported = 2
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint_path,
        "workers": 1,
    }
    assert (
        import_wandb_runs(
            FakeSourceApi(),
            client,
            include_files=False,
            **arguments,
        ).completed
        == 1
    )

    resumed = import_wandb_runs(
        FakeSourceApi(),
        client,
        include_files=True,
        **arguments,
    )

    assert resumed.failed == 1
    assert "include_files contract does not match" in resumed.failures[0]


def test_checkpoint_requires_explicit_include_files_contract(tmp_path) -> None:
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    checkpoint.update(FakeRun.id, phase="history")

    result = import_wandb_runs(
        FakeSourceApi(),
        FakeClient(),
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=checkpoint_path,
        workers=1,
        include_files=True,
    )

    assert result.failed == 1
    assert "no valid include_files import contract" in result.failures[0]


@pytest.mark.parametrize("phase", ["history", "run_files", "logged_artifacts"])
def test_finished_target_rejects_recovery_before_finalize(phase, tmp_path) -> None:
    client = FakeClient()
    client.fail_first_batch = False
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint_path,
        "workers": 1,
        "include_files": True,
    }
    assert import_wandb_runs(FakeSourceApi(), client, **arguments).completed == 1
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    checkpoint.update(FakeRun.id, status="failed", phase=phase)

    resumed = import_wandb_runs(FakeSourceApi(), client, **arguments)

    assert resumed.failed == 1
    assert "finished target run cannot recover" in resumed.failures[0]


def test_wandb_import_no_files_skips_media_and_artifacts(tmp_path) -> None:
    client = FakeClient()
    client.fail_first_batch = False
    client.expected_unsupported = 2

    result = import_wandb_runs(
        FakeSourceApi(),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.json",
        workers=1,
        include_files=False,
    )

    assert result.completed == 1
    assert client.rich_values == []
    assert client.artifacts == []


def test_file_import_requires_logged_artifact_capability(tmp_path) -> None:
    class MissingLoggedArtifactsRun(FakeRun):
        id = "missing-artifact-api-source"
        name = "missing artifact API source"
        url = "https://wandb.ai/team/demo/runs/missing-artifact-api-source"
        logged_artifacts = None

        def files(self):
            return iter(())

    client = FakeClient()
    client.fail_first_batch = False

    result = import_wandb_runs(
        OneRunSourceApi(MissingLoggedArtifactsRun()),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
        include_files=True,
    )

    assert result.failed == 1
    assert "does not provide callable logged_artifacts" in result.failures[0]
    assert client.finished is False
    checkpoint = Checkpoint(
        tmp_path / "checkpoint.jsonl",
        entity="team",
        project="demo",
        target_project="imported",
    )
    assert checkpoint.state(MissingLoggedArtifactsRun.id)["phase"] == "logged_artifacts"


def test_wide_wandb_history_row_is_split_and_replayed_at_its_row_boundary(
    monkeypatch,
    tmp_path,
) -> None:
    class WideRun(FakeRun):
        id = "wide-source"
        name = "wide source"
        url = "https://wandb.ai/team/demo/runs/wide-source"
        lastHistoryStep = 0
        historyLineCount = 1

        def scan_history(self, *, page_size: int):
            assert page_size == 1_000
            return iter(
                [
                    {
                        "_step": 7,
                        "_timestamp": 12.5,
                        **{f"metric-{index:03d}": float(index) for index in range(300)},
                    }
                ]
            )

    monkeypatch.setattr(importer_module, "_METRIC_BATCH_SIZE", 1)
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    client = FakeClient()
    client.expected_unsupported = 0
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint_path,
        "workers": 1,
        "include_files": False,
    }

    first = import_wandb_runs(OneRunSourceApi(WideRun()), client, **arguments)
    assert first.failed == 1
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    assert checkpoint.state("wide-source").get("rows_committed", 0) == 0

    second = import_wandb_runs(OneRunSourceApi(WideRun()), client, **arguments)

    assert second.completed == 1
    assert client.batches[0] == client.batches[1]
    replayed_points = [client.batches[1]["points"][0], client.batches[2]["points"][0]]
    assert [point["sequence"] for point in replayed_points] == [1, 2]
    assert {point["step"] for point in replayed_points} == {7}
    assert {point["timestamp_ms"] for point in replayed_points} == {12_500}
    assert [len(point["metrics"]) for point in replayed_points] == [256, 44]
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    assert checkpoint.state("wide-source")["rows_committed"] == 1
    assert checkpoint.state("wide-source")["next_sequence"] == 3


def test_unsupported_count_checkpoint_matches_byte_budget_batch_cursor(
    monkeypatch,
    tmp_path,
) -> None:
    class TwoRowRun(FakeRun):
        id = "two-row-source"
        name = "two row source"
        url = "https://wandb.ai/team/demo/runs/two-row-source"
        lastHistoryStep = 1
        historyLineCount = 2

        def scan_history(self, *, page_size: int):
            assert page_size == 1_000
            return iter(
                [
                    {"_step": 0, "loss": 2.0, "label": "first"},
                    {"_step": 1, "loss": 1.0, "label": "second"},
                ]
            )

    first_point = {
        "sequence": 1,
        "step": 0,
        "timestamp_ms": 0,
        "metrics": {"loss": 2.0},
    }
    monkeypatch.setattr(
        importer_module,
        "_METRIC_BATCH_BYTES",
        importer_module._json_size(first_point) + 1,
    )

    class FailSecondBatchClient(FakeClient):
        def __init__(self) -> None:
            super().__init__()
            self.fail_first_batch = False
            self.expected_unsupported = 2
            self._failed_second = False

        def ingest_batch(self, run_id: str, batch: dict):
            self.batches.append(batch)
            if len(self.batches) == 2 and not self._failed_second:
                self._failed_second = True
                raise ConnectionError("second batch failed before commit")
            return {"duplicate": False, "next_sequence": batch["batch_sequence"] + 1}

    checkpoint_path = tmp_path / "checkpoint.jsonl"
    client = FailSecondBatchClient()
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint_path,
        "workers": 1,
        "include_files": False,
    }
    first = import_wandb_runs(OneRunSourceApi(TwoRowRun()), client, **arguments)
    assert first.failed == 1
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    assert checkpoint.state("two-row-source")["rows_committed"] == 1
    assert checkpoint.state("two-row-source")["unsupported_values"] == 1

    second = import_wandb_runs(OneRunSourceApi(TwoRowRun()), client, **arguments)
    assert second.completed == 1
    assert client.finished is True


@pytest.mark.parametrize("metric_key", ["", "bad\nkey", "x" * 257])
def test_wandb_import_reports_the_exact_unsupported_metric_key(metric_key, tmp_path) -> None:
    class InvalidMetricRun(FakeRun):
        id = "invalid-metric-source"
        name = "invalid metric source"
        url = "https://wandb.ai/team/demo/runs/invalid-metric-source"
        lastHistoryStep = 0
        historyLineCount = 1

        def scan_history(self, *, page_size: int):
            assert page_size == 1_000
            return iter([{"_step": 0, metric_key: 1.0}])

    client = FakeClient()
    client.fail_first_batch = False
    result = import_wandb_runs(
        OneRunSourceApi(InvalidMetricRun()),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
        include_files=False,
    )

    assert result.failed == 1
    assert repr(metric_key) in result.failures[0]
    assert "must contain 1 to 256 non-control bytes" in result.failures[0]
    assert client.batches == []


def test_wandb_history_row_traversal_has_explicit_metric_media_and_node_bounds(
    monkeypatch,
) -> None:
    monkeypatch.setattr(importer_module, "_MAX_SOURCE_ROW_METRICS", 2)
    with pytest.raises(importer_module.WandbImportError, match="exceeds 2 scalar metrics"):
        importer_module._history_values({"a": 1.0, "b": 2.0, "c": 3.0})

    monkeypatch.setattr(importer_module, "_MAX_SOURCE_ROW_MEDIA", 1)
    with pytest.raises(importer_module.WandbImportError, match="exceeds 1 media values"):
        importer_module._history_values(
            {
                "first": {"_type": "video-file", "path": "media/first.mp4"},
                "second": {"_type": "video-file", "path": "media/second.mp4"},
            }
        )

    monkeypatch.setattr(importer_module, "_MAX_SOURCE_ROW_NODES", 3)
    with pytest.raises(importer_module.WandbImportError, match="exceeds 3 traversed values"):
        importer_module._history_values({"a": "x", "b": "x", "c": "x", "d": "x"})


def test_wandb_history_row_rejects_excessive_depth_and_media_metadata(monkeypatch) -> None:
    monkeypatch.setattr(importer_module, "_MAX_SOURCE_ROW_DEPTH", 3)
    nested: object = 1.0
    for _ in range(5):
        nested = {"x": nested}
    with pytest.raises(importer_module.WandbImportError, match="nesting exceeds 3 levels"):
        importer_module._history_values({"root": nested})

    monkeypatch.setattr(importer_module, "_MAX_SOURCE_MEDIA_REFERENCE_BYTES", 64)
    with pytest.raises(importer_module.WandbImportError, match="media reference exceeds 64"):
        importer_module._history_values(
            {
                "rollout": {
                    "_type": "video-file",
                    "path": "media/rollout.mp4",
                    "caption": "x" * 128,
                }
            }
        )


@pytest.mark.parametrize("document", ["config", "summary"])
def test_wandb_import_rejects_oversized_run_documents_before_delivery(
    document,
    monkeypatch,
    tmp_path,
) -> None:
    source = FakeRun()
    setattr(source, document, {"payload": "x" * 1_000})
    monkeypatch.setattr(importer_module, "_MAX_RUN_DOCUMENT_BYTES", 512)
    client = FakeClient()
    client.fail_first_batch = False

    result = import_wandb_runs(
        OneRunSourceApi(source),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
        include_files=False,
    )

    assert result.failed == 1
    assert f"serialized W&B {document} exceeds 512 bytes" in result.failures[0]
    if document == "config":
        assert client.run is None
        assert client.batches == []
    else:
        assert client.run is not None
        assert client.finished is False


def test_wandb_document_stops_iterating_once_its_serialized_budget_is_exhausted() -> None:
    class LargeMapping(Mapping[str, str]):
        def __init__(self) -> None:
            self.reads = 0

        def __len__(self) -> int:
            return 100_000

        def __iter__(self) -> Iterator[str]:
            return (f"key-{index}" for index in range(len(self)))

        def __getitem__(self, key: str) -> str:
            self.reads += 1
            return "value"

    source = LargeMapping()
    with pytest.raises(importer_module.WandbImportError, match="serialized W&B config exceeds 64"):
        importer_module._wandb_document(source, "W&B config", 64)

    assert source.reads < 100


def test_wandb_history_page_and_checkpoint_writes_are_bounded(tmp_path) -> None:
    sparse_span = 10**15 + 1
    sparse_page_size = _history_page_size(
        SimpleNamespace(
            lastHistoryStep=sparse_span - 1,
            _attrs={"historyLineCount": 100_000},
        )
    )
    assert math.ceil(sparse_span / sparse_page_size) <= importer_module._TARGET_HISTORY_PAGE_COUNT

    empty_sparse_page_size = _history_page_size(
        SimpleNamespace(
            lastHistoryStep=sparse_span - 1,
            _attrs={"historyLineCount": 0},
        )
    )
    assert (
        math.ceil(sparse_span / empty_sparse_page_size)
        <= importer_module._TARGET_HISTORY_PAGE_COUNT
    )
    assert (
        _history_page_size(SimpleNamespace(lastHistoryStep=-1, _attrs={"historyLineCount": 0}))
        == importer_module._MIN_HISTORY_PAGE_SIZE
    )

    with pytest.raises(importer_module.WandbImportError, match="100000-row"):
        _history_page_size(
            SimpleNamespace(
                lastHistoryStep=100_000,
                _attrs={"historyLineCount": 100_001},
            )
        )
    with pytest.raises(importer_module.WandbImportError, match="historyLineCount is required"):
        _history_page_size(SimpleNamespace(lastHistoryStep=100_000))

    path = tmp_path / "checkpoint.jsonl"
    checkpoint = Checkpoint(
        path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    initial_size = path.stat().st_size
    checkpoint.update("run-1", status="importing", rows_committed=10)
    first_update_size = path.stat().st_size
    checkpoint.update("run-1", rows_committed=20)

    assert initial_size < first_update_size < path.stat().st_size
    if os.name != "nt":
        assert path.stat().st_mode & 0o777 == 0o600
    assert checkpoint.state("run-1")["rows_committed"] == 20
    records = [json.loads(line) for line in path.read_text().splitlines()]
    assert [record["type"] for record in records] == [
        "header",
        "snapshot",
        "update",
        "update",
    ]
    assert set(records[0]) == {
        "entity",
        "project",
        "source",
        "target_project",
        "type",
    }


def test_history_media_is_parallel_bounded_and_does_not_hold_metric_ingest(tmp_path) -> None:
    tracker = MediaTracker(total=20)
    client = ParallelMediaClient(tracker)

    result = import_wandb_runs(
        OneRunSourceApi(ConcurrentMediaRun(tracker)),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
    )

    assert result.completed == 1
    assert 2 <= tracker.max_active <= importer_module._MEDIA_WORKERS
    assert tracker.max_rows_ahead <= importer_module._MAX_PENDING_MEDIA + 1
    assert client.finished_media_at_ingest[0] < tracker.total
    assert len(client.rich_ids) == tracker.total
    assert tracker.active == 0


def test_media_failure_drains_workers_and_resumes_without_replaying_metrics(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(importer_module, "_METRIC_BATCH_SIZE", 2)
    monkeypatch.setattr(importer_module, "_MEDIA_WORKERS", 2)
    monkeypatch.setattr(importer_module, "_MAX_PENDING_MEDIA", 4)
    tracker = MediaTracker(total=10, fail_index=2)
    client = ParallelMediaClient(tracker)
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "checkpoint_path": checkpoint_path,
        "workers": 1,
    }

    first = import_wandb_runs(
        OneRunSourceApi(ConcurrentMediaRun(tracker)),
        client,
        **arguments,
    )

    assert first.failed == 1
    assert tracker.active == 0
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    first_state = checkpoint.state("media-source")
    assert first_state["rows_committed"] == 6
    assert first_state["media_rows_committed"] == 2

    tracker.fail_index = None
    tracker.release_tail.set()
    second = import_wandb_runs(
        OneRunSourceApi(ConcurrentMediaRun(tracker)),
        client,
        **arguments,
    )

    assert second.completed == 1
    sequences = [point["sequence"] for batch in client.batches for point in batch["points"]]
    assert sequences == list(range(1, 11))
    assert len(client.rich_ids) == tracker.total
    assert len(client.rich_values) > len(client.rich_ids)
    assert tracker.active == 0


def test_checkpoint_process_lock_rejects_concurrent_import_and_releases(tmp_path) -> None:
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    blocking = BlockingEmptySourceApi()
    errors: list[BaseException] = []

    def run_first_import() -> None:
        try:
            import_wandb_runs(
                blocking,
                FakeClient(),
                entity="team",
                project="demo",
                target_project="imported",
                checkpoint_path=checkpoint_path,
                workers=1,
            )
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=run_first_import)
    thread.start()
    assert blocking.entered.wait(2)
    try:
        with pytest.raises(importer_module.WandbImportError, match="another W&B import"):
            import_wandb_runs(
                EmptySourceApi(),
                FakeClient(),
                entity="team",
                project="demo",
                target_project="imported",
                checkpoint_path=checkpoint_path,
                workers=1,
            )
    finally:
        blocking.release.set()
        thread.join(2)

    assert not thread.is_alive()
    assert errors == []
    resumed = import_wandb_runs(
        EmptySourceApi(),
        FakeClient(),
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=checkpoint_path,
        workers=1,
    )
    assert resumed.selected == 0


def test_keyboard_interrupt_cancels_queued_runs_without_waiting_for_pool_shutdown(
    monkeypatch,
    tmp_path,
) -> None:
    source = CountingSourceApi([InterruptingRun(), FakeRun()])
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    shutdown_waits: list[bool] = []
    original_shutdown = importer_module.ThreadPoolExecutor.shutdown

    def record_shutdown(executor, wait=True, *, cancel_futures=False):
        shutdown_waits.append(wait)
        return original_shutdown(executor, wait=wait, cancel_futures=cancel_futures)

    monkeypatch.setattr(importer_module.ThreadPoolExecutor, "shutdown", record_shutdown)

    with pytest.raises(KeyboardInterrupt):
        import_wandb_runs(
            source,
            FakeClient(),
            entity="team",
            project="demo",
            target_project="imported",
            checkpoint_path=checkpoint_path,
            workers=1,
        )

    assert source.yielded == 1
    assert False in shutdown_waits
    resumed = import_wandb_runs(
        EmptySourceApi(),
        FakeClient(),
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=checkpoint_path,
        workers=1,
    )
    assert resumed.selected == 0


def test_unbounded_import_fails_instead_of_truncating_at_internal_run_limit(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(importer_module, "_MAX_IMPORT_RUNS", 2)
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    checkpoint.update("one", status="complete", phase="complete", include_files=True)
    checkpoint.update("two", status="complete", phase="complete", include_files=True)
    source = CountingSourceApi(
        [
            SimpleNamespace(id="one"),
            SimpleNamespace(id="two"),
            SimpleNamespace(id="three"),
        ]
    )

    with pytest.raises(importer_module.WandbImportError, match="more than 2 runs"):
        import_wandb_runs(
            source,
            FakeClient(),
            entity="team",
            project="demo",
            target_project="imported",
            checkpoint_path=checkpoint_path,
            workers=1,
        )

    assert source.yielded == 3


def test_nonterminal_wandb_run_is_not_marked_finished(tmp_path) -> None:
    running = FakeRun()
    running.state = "running"
    client = FakeClient()

    result = import_wandb_runs(
        CountingSourceApi([running]),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
    )

    assert result.failed == 1
    assert "is not terminal" in result.failures[0]
    assert client.finished is False
    assert client.run is None


def test_preempted_wandb_run_is_importable(tmp_path) -> None:
    source = FakeRun()
    source.state = "preempted"
    client = FakeClient()
    client.fail_first_batch = False
    client.expected_unsupported = 2

    result = import_wandb_runs(
        CountingSourceApi([source]),
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
        include_files=False,
    )

    assert result.completed == 1
    assert client.finished is True
    assert client.run is not None
    assert client.run["config"]["_epochdeck_wandb_source"]["state"] == "preempted"


def test_source_revision_uses_real_wandb_heartbeat_shape() -> None:
    source = SimpleNamespace(
        state="FINISHED",
        _attrs={
            "createdAt": "2026-08-27T00:00:00Z",
            "heartbeatAt": "2026-08-30T12:34:56Z",
        },
    )

    revision = importer_module._source_revision(source)

    assert revision.state == "finished"
    assert revision.updated_at == "2026-08-30T12:34:56Z"


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        (
            SimpleNamespace(
                state="finished",
                updated_at="public",
                _attrs={"updatedAt": "private", "heartbeatAt": "heartbeat"},
            ),
            "public",
        ),
        (
            SimpleNamespace(
                state="finished",
                _attrs={"updatedAt": "private", "heartbeatAt": "heartbeat"},
            ),
            "private",
        ),
    ],
)
def test_source_revision_prefers_more_direct_timestamp(source, expected) -> None:
    assert importer_module._source_revision(source).updated_at == expected


@pytest.mark.parametrize(
    ("source", "message"),
    [
        (SimpleNamespace(state="finished"), "no source revision timestamp"),
        (
            SimpleNamespace(state="finished", _attrs={"createdAt": "created"}),
            "no source revision timestamp",
        ),
        (
            SimpleNamespace(state="finished", _attrs={"heartbeatAt": ""}),
            "invalid source revision",
        ),
        (
            SimpleNamespace(state="finished", _attrs={"heartbeatAt": 42}),
            "invalid source revision",
        ),
        (
            SimpleNamespace(state="finished", _attrs={"heartbeatAt": "x" * 257}),
            "exceeds 256 bytes",
        ),
    ],
)
def test_source_revision_rejects_missing_or_invalid_timestamp(source, message) -> None:
    with pytest.raises(importer_module.WandbImportError, match=message):
        importer_module._source_revision(source)


@pytest.mark.parametrize(
    ("refreshed_state", "refreshed_updated_at"),
    [
        ("finished", "2026-08-29T00:00:00Z"),
        ("failed", FakeRun.updated_at),
    ],
)
def test_wandb_run_change_during_import_is_not_marked_finished(
    refreshed_state,
    refreshed_updated_at,
    tmp_path,
) -> None:
    source = FakeRun()
    refreshed = FakeRun()
    refreshed.state = refreshed_state
    refreshed.updated_at = refreshed_updated_at
    source_api = OneRunSourceApi(source, [source, refreshed])
    client = FakeClient()
    client.fail_first_batch = False
    client.expected_unsupported = 2

    result = import_wandb_runs(
        source_api,
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=tmp_path / "checkpoint.jsonl",
        workers=1,
        include_files=False,
    )

    assert result.failed == 1
    assert "changed during import" in result.failures[0]
    assert "remove both the partial target run and its checkpoint" in result.failures[0]
    assert client.finished is False
    assert source_api.flush_count == 2
    assert source_api.run_paths == ["team/demo/source-1", "team/demo/source-1"]


def test_completed_checkpoint_validates_an_authoritative_source_refresh(tmp_path) -> None:
    checkpoint_path = tmp_path / "checkpoint.jsonl"
    checkpoint = Checkpoint(
        checkpoint_path,
        entity="team",
        project="demo",
        target_project="imported",
    )
    checkpoint.update(
        "source-1",
        status="complete",
        phase="complete",
        include_files=False,
        source_state=FakeRun.state,
        source_updated_at=FakeRun.updated_at,
    )
    stale_listing = FakeRun()
    changed = FakeRun()
    changed.updated_at = "2026-08-29T00:00:00Z"
    source_api = OneRunSourceApi(stale_listing, [changed])
    client = FakeClient()

    result = import_wandb_runs(
        source_api,
        client,
        entity="team",
        project="demo",
        target_project="imported",
        checkpoint_path=checkpoint_path,
        workers=1,
        include_files=False,
    )

    assert result.failed == 1
    assert result.skipped == 0
    assert "changed after import began" in result.failures[0]
    assert client.run is None
    assert source_api.flush_count == 1


def test_source_revision_in_target_config_survives_checkpoint_loss(tmp_path) -> None:
    client = FakeClient()
    client.fail_first_batch = False
    client.expected_unsupported = 2
    arguments = {
        "entity": "team",
        "project": "demo",
        "target_project": "imported",
        "workers": 1,
        "include_files": False,
    }

    first = import_wandb_runs(
        FakeSourceApi(),
        client,
        checkpoint_path=tmp_path / "first.jsonl",
        **arguments,
    )
    stale_listing = FakeRun()
    changed = FakeRun()
    changed.updated_at = "2026-08-29T00:00:00Z"
    second = import_wandb_runs(
        OneRunSourceApi(stale_listing, [changed]),
        client,
        checkpoint_path=tmp_path / "replacement.jsonl",
        **arguments,
    )

    assert first.completed == 1
    assert second.failed == 1
    assert "changed since the target run was created" in second.failures[0]
    assert "remove both the partial target run and its checkpoint" in second.failures[0]


def test_media_identity_includes_source_row_and_occurrence(tmp_path) -> None:
    client = FakeClient()
    client.fail_first_batch = False
    cancellation = ImportCancellation()
    arguments = {
        "run_id": "run-1",
        "source_metadata": {
            "entity": "team",
            "project": "demo",
            "run_id": "source-1",
            "url": "https://wandb.ai/team/demo/runs/source-1",
        },
        "key": "rollout",
        "kind": "video",
        "step": 4,
        "timestamp_ms": 1000,
        "artifact_path": FakeFile.name,
        "reference": {"path": FakeFile.name},
        "occurrence": 0,
        "cancellation": cancellation,
    }

    importer_module._import_media_reference(FakeFile(), client, row_number=1, **arguments)
    importer_module._import_media_reference(FakeFile(), client, row_number=2, **arguments)
    importer_module._import_media_reference(FakeFile(), client, row_number=1, **arguments)

    assert client.rich_values[0]["id"] != client.rich_values[1]["id"]
    assert client.rich_values[0]["id"] == client.rich_values[2]["id"]


def test_artifact_import_uploads_and_unlinks_each_temporary_file_immediately() -> None:
    tracker = TempFileTracker()
    client = TempBoundClient(tracker)
    cancellation = ImportCancellation()
    metadata = {
        "entity": "team",
        "project": "demo",
        "run_id": "source-1",
        "url": "https://wandb.ai/team/demo/runs/source-1",
    }

    importer_module._import_file_chunk(
        client,
        [
            TrackedFile(tracker, "run-files/one.bin"),
            TrackedFile(tracker, "run-files/two.bin"),
        ],
        run_id="run-1",
        source_metadata=metadata,
        chunk_start=0,
        cancellation=cancellation,
    )
    importer_module._import_logged_artifact(
        client,
        TrackedArtifact(tracker),
        "run-1",
        metadata,
        cancellation,
    )

    assert tracker.maximum_files == 1
    assert tracker.events == [
        "download:run-files/one.bin",
        "upload:one.bin",
        "download:run-files/two.bin",
        "upload:two.bin",
        "download:checkpoints/one.bin",
        "upload:one.bin",
        "download:checkpoints/two.bin",
        "upload:two.bin",
    ]


def test_run_file_artifact_names_use_the_complete_run_identity() -> None:
    client = FakeClient()
    cancellation = ImportCancellation()
    run_ids = [
        "deadbeef-0000-4000-8000-000000000001",
        "deadbeef-0000-4000-8000-000000000002",
    ]

    for run_id in run_ids:
        importer_module._import_file_chunk(
            client,
            [FakeFile()],
            run_id=run_id,
            source_metadata={
                "entity": "team",
                "project": "demo",
                "run_id": run_id,
                "url": f"https://wandb.ai/team/demo/runs/{run_id}",
            },
            chunk_start=0,
            cancellation=cancellation,
        )

    assert [artifact["name"] for artifact in client.artifacts] == [
        f"wandb-run-{run_id}-files-0000" for run_id in run_ids
    ]
    assert client.artifacts[0]["name"] != client.artifacts[1]["name"]


@pytest.mark.parametrize(
    "version",
    [None, "", "3", "v", "v-1", "v01", "v\u0661", f"v{importer_module.MAX_SAFE_INTEGER + 1}"],
)
def test_logged_artifact_import_rejects_invalid_versions_before_file_download(
    version: object,
) -> None:
    class InvalidVersionArtifact(FakeArtifact):
        def __init__(self) -> None:
            self.version = version

        def files(self, *, per_page: int):
            raise AssertionError("artifact files were requested before version validation")

    with pytest.raises(importer_module.WandbImportError, match="artifact version"):
        importer_module._import_logged_artifact(
            FakeClient(),
            InvalidVersionArtifact(),
            "run-1",
            {
                "entity": "team",
                "project": "demo",
                "run_id": "source-1",
                "url": "https://wandb.ai/team/demo/runs/source-1",
            },
            ImportCancellation(),
        )


def test_logged_artifact_import_retries_the_exact_explicit_version_request() -> None:
    class LostResponseClient(FakeClient):
        def create_artifact(self, run_id: str, artifact: dict):
            response = super().create_artifact(run_id, artifact)
            if len(self.artifacts) == 1:
                raise ConnectionError("response lost after artifact commit")
            return response

    client = LostResponseClient()
    arguments = (
        client,
        FakeArtifact(),
        "run-1",
        {
            "entity": "team",
            "project": "demo",
            "run_id": "source-1",
            "url": "https://wandb.ai/team/demo/runs/source-1",
        },
        ImportCancellation(),
    )

    with pytest.raises(ConnectionError, match="response lost"):
        importer_module._import_logged_artifact(*arguments)
    importer_module._import_logged_artifact(*arguments)

    assert client.artifacts[0] == client.artifacts[1]
    assert client.artifacts[0]["name"] == "policy"
    assert client.artifacts[0]["version"] == 3


def test_artifact_import_bounds_alias_iteration_before_file_download() -> None:
    class TooManyAliasesArtifact(FakeArtifact):
        aliases = (f"alias-{index}" for index in range(importer_module.MAX_ARTIFACT_ALIASES + 1))

        def files(self, *, per_page: int):
            raise AssertionError("artifact files were requested before alias validation")

    with pytest.raises(importer_module.WandbImportError, match="more than 256 alias values"):
        importer_module._import_logged_artifact(
            FakeClient(),
            TooManyAliasesArtifact(),
            "run-1",
            {
                "entity": "team",
                "project": "demo",
                "run_id": "source-1",
                "url": "https://wandb.ai/team/demo/runs/source-1",
            },
            ImportCancellation(),
        )


def test_artifact_import_preflights_file_count_before_any_upload() -> None:
    yielded = 0

    class OversizedArtifact(FakeArtifact):
        def files(self, *, per_page: int):
            assert per_page == 100
            nonlocal yielded
            for index in range(importer_module.MAX_ARTIFACT_ENTRIES + 1):
                yielded += 1
                yield SimpleNamespace(name=f"checkpoint/{index}.bin")

    class NoUploadClient(FakeClient):
        def upload_blob(self, path: Path, blob: dict):
            raise AssertionError("oversized artifact uploaded a blob before preflight")

    with pytest.raises(importer_module.WandbImportError, match="exceeds 4096 files"):
        importer_module._import_logged_artifact(
            NoUploadClient(),
            OversizedArtifact(),
            "run-1",
            {
                "entity": "team",
                "project": "demo",
                "run_id": "source-1",
                "url": "https://wandb.ai/team/demo/runs/source-1",
            },
            ImportCancellation(),
        )
    assert yielded == importer_module.MAX_ARTIFACT_ENTRIES + 1


def test_run_file_resume_rejects_a_shorter_source_listing(tmp_path) -> None:
    checkpoint = Checkpoint(
        tmp_path / "checkpoint.jsonl",
        entity="team",
        project="demo",
        target_project="imported",
    )
    checkpoint.update(
        FakeRun.id,
        phase="run_files",
        include_files=True,
        files_committed=2,
    )

    with pytest.raises(importer_module.WandbImportError, match="file listing became shorter"):
        importer_module._import_run_files(
            FakeRun(),
            FakeClient(),
            checkpoint,
            FakeRun.id,
            "run-1",
            {"entity": "team", "project": "demo", "run_id": FakeRun.id, "url": ""},
            ImportCancellation(),
        )

    assert checkpoint.state(FakeRun.id).get("files_complete") is not True


def test_logged_artifact_resume_rejects_a_shorter_source_listing(tmp_path) -> None:
    checkpoint = Checkpoint(
        tmp_path / "checkpoint.jsonl",
        entity="team",
        project="demo",
        target_project="imported",
    )
    checkpoint.update(
        FakeRun.id,
        phase="logged_artifacts",
        include_files=True,
        logged_artifacts_committed=2,
    )

    with pytest.raises(importer_module.WandbImportError, match="artifact listing became shorter"):
        importer_module._import_logged_artifacts(
            FakeRun(),
            FakeClient(),
            checkpoint,
            FakeRun.id,
            "run-1",
            {"entity": "team", "project": "demo", "run_id": FakeRun.id, "url": ""},
            ImportCancellation(),
        )

    assert checkpoint.state(FakeRun.id).get("logged_artifacts_complete") is not True


@pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf])
def test_history_positions_reject_nonfinite_numbers(value: float) -> None:
    with pytest.raises(importer_module.WandbImportError, match="invalid _step"):
        importer_module._history_step({"_step": value}, 0)
    with pytest.raises(importer_module.WandbImportError, match="invalid _timestamp"):
        importer_module._history_timestamp_ms({"_timestamp": value}, 0)

    with pytest.raises(importer_module.WandbImportError, match="position metadata"):
        importer_module._history_row_identity({"_step": value})


def test_history_values_treat_overflowing_numbers_as_unsupported() -> None:
    huge = 10**10_000

    metrics, media, skipped = importer_module._history_values({"huge": huge})
    reference = importer_module._bounded_media_reference(
        {"path": "media/video.mp4", "width": huge},
        "media/video.mp4",
    )

    assert metrics == {}
    assert media == []
    assert skipped == 1
    assert "width" not in reference


def test_history_timestamp_rejects_an_overflowing_integer() -> None:
    with pytest.raises(importer_module.WandbImportError, match="invalid _timestamp"):
        importer_module._history_timestamp_ms({"_timestamp": 10**10_000}, 0)


def test_artifact_identity_does_not_stringify_missing_values() -> None:
    with pytest.raises(importer_module.WandbImportError, match="no stable identity"):
        importer_module._source_artifact_identity(SimpleNamespace(id=None, qualified_name=None))
