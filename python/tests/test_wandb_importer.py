from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar

from runloom.client import RunloomApiError
from runloom.wandb_importer import import_wandb_runs


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
    def runs(self, path: str):
        assert path == "team/demo"
        return iter([FakeRun()])


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
            raise RunloomApiError(404, "not_found", "missing")
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
        assert (
            summary["_runloom_wandb_source"]["unsupported_history_values"]
            == self.expected_unsupported
        )
        return {"run": {"id": run_id, "state": "finished"}}


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
    assert client.artifacts[1]["name"] == "policy"
    assert client.artifacts[1]["metadata"]["wandb_source"]["version"] == "v3"
    assert client.rich_values[-1]["kind"] == "video"
    assert client.rich_values[-1]["metadata"]["caption"] == "policy rollout"
    assert client.finished is True

    third = import_wandb_runs(FakeSourceApi(), client, **arguments)
    assert third.skipped == 1
    assert len(client.batches) == 2


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
