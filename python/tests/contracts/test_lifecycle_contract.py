from __future__ import annotations

import json
import threading
from copy import deepcopy
from pathlib import Path
from typing import Any

import httpx
import pytest

import runloom
from runloom import DeliveryError
from runloom._summary import merge_metric_preview
from runloom.run import create_run, sync_spool

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "scalar_lifecycle.json").read_text(encoding="utf-8")
)


class ContractServer:
    def __init__(
        self,
        *,
        lose_first_batch_response: bool = False,
        lose_first_finish_response: bool = False,
    ) -> None:
        self.runs: dict[str, dict[str, Any]] = {}
        self.batch_requests: list[dict[str, Any]] = []
        self.lose_first_batch_response = lose_first_batch_response
        self.lose_first_finish_response = lose_first_finish_response
        self.loss_observed = threading.Event()
        self._lost_response = False
        self._lost_finish_response = False
        self._lock = threading.Lock()

    def add_running_run(
        self,
        run_id: str,
        *,
        config: dict[str, Any],
        summary: dict[str, Any],
        last_sequence: int,
        last_step: int,
    ) -> None:
        self.runs[run_id] = self._new_run(
            run_id,
            name="resumed-run",
            config=config,
            summary=summary,
            last_sequence=last_sequence,
            last_step=last_step,
        )

    def __call__(self, request: httpx.Request) -> httpx.Response:
        with self._lock:
            body = json.loads(request.content) if request.content else {}
            path = request.url.path
            if path.endswith("/runs") and request.method == "POST":
                return self._create_run(body)
            run_id = path.split("/runs/", 1)[1].split("/", 1)[0]
            if path.endswith("/batches"):
                return self._ingest(request, run_id, body)
            if path.endswith("/config"):
                run = self.runs[run_id]
                run["config"].update(body["updates"])
                return httpx.Response(200, json={"run": self._response_run(run)})
            if path.endswith("/summary"):
                run = self.runs[run_id]
                run["explicit_summary"].update(body["updates"])
                return httpx.Response(200, json={"run": self._response_run(run)})
            if path.endswith("/finish"):
                return self._finish(request, run_id, body["summary"])
            if request.method == "GET":
                return httpx.Response(200, json=self._response_run(self.runs[run_id]))
            raise AssertionError(f"unexpected request: {request.method} {path}")

    def points(self, run_id: str) -> list[dict[str, Any]]:
        batches = self.runs[run_id]["batches"]
        return [point for key in sorted(batches) for point in batches[key]["points"]]

    def _create_run(self, body: dict[str, Any]) -> httpx.Response:
        run_id = body["id"]
        existing = self.runs.get(run_id)
        if existing is not None:
            if existing["state"] == "finished":
                return self._error(409, "conflict", "finished runs cannot be resumed")
            if body["resume"] == "never":
                return self._error(409, "conflict", "run already exists")
            return httpx.Response(
                200,
                json={
                    "run": self._response_run(existing),
                    "resumed": True,
                    "next_sequence": existing["last_sequence"] + 1,
                    "next_step": existing["last_step"] + 1,
                },
            )
        if body["resume"] == "must":
            return self._error(404, "not_found", "required run was not found")
        run = self._new_run(
            run_id,
            name=body.get("name") or "contract-run",
            config=body["config"],
        )
        self.runs[run_id] = run
        return httpx.Response(
            201,
            json={
                "run": self._response_run(run),
                "resumed": False,
                "next_sequence": 1,
                "next_step": 0,
            },
        )

    def _ingest(
        self,
        request: httpx.Request,
        run_id: str,
        body: dict[str, Any],
    ) -> httpx.Response:
        run = self.runs[run_id]
        sequence = int(body["batch_sequence"])
        duplicate = sequence in run["batches"]
        if duplicate and run["batches"][sequence] != body:
            return self._error(409, "conflict", "batch contents changed")
        if not duplicate:
            run["batches"][sequence] = deepcopy(body)
            for point in body["points"]:
                run["metric_summary"], run["summary_truncated"] = merge_metric_preview(
                    run["metric_summary"],
                    point["metrics"],
                    truncated=run["summary_truncated"],
                )
                run["last_sequence"] = point["sequence"]
                run["last_step"] = point["step"]
        self.batch_requests.append(deepcopy(body))
        if self.lose_first_batch_response and not self._lost_response:
            self._lost_response = True
            self.loss_observed.set()
            raise httpx.ReadError("response was lost after commit", request=request)
        return httpx.Response(
            200 if duplicate else 201,
            json={
                "run_id": run_id,
                "batch_sequence": sequence,
                "accepted_points": len(body["points"]),
                "duplicate": duplicate,
                "metric_revision": len(run["batches"]),
                "stop_requested": False,
            },
        )

    def _finish(
        self,
        request: httpx.Request,
        run_id: str,
        summary: dict[str, Any],
    ) -> httpx.Response:
        run = self.runs[run_id]
        if run["state"] == "finished":
            if any(run["explicit_summary"].get(key) != value for key, value in summary.items()):
                return self._error(409, "conflict", "finished summary changed")
            return httpx.Response(200, json={"run": self._response_run(run)})
        run["explicit_summary"].update(summary)
        run["state"] = "finished"
        if self.lose_first_finish_response and not self._lost_finish_response:
            self._lost_finish_response = True
            raise httpx.ReadError("finish response was lost after commit", request=request)
        return httpx.Response(200, json={"run": self._response_run(run)})

    @staticmethod
    def _new_run(
        run_id: str,
        *,
        name: str,
        config: dict[str, Any],
        summary: dict[str, Any] | None = None,
        last_sequence: int = 0,
        last_step: int = -1,
    ) -> dict[str, Any]:
        return {
            "id": run_id,
            "name": name,
            "state": "running",
            "config": deepcopy(config),
            "explicit_summary": {},
            "metric_summary": deepcopy(summary or {}),
            "summary_truncated": False,
            "last_sequence": last_sequence,
            "last_step": last_step,
            "batches": {},
        }

    @staticmethod
    def _response_run(run: dict[str, Any]) -> dict[str, Any]:
        explicit = deepcopy(run["explicit_summary"])
        metric = deepcopy(run["metric_summary"])
        return {
            "id": run["id"],
            "name": run["name"],
            "state": run["state"],
            "config": deepcopy(run["config"]),
            "explicit_summary": explicit,
            "metric_summary": metric,
            "summary": {**metric, **explicit},
            "summary_truncated": run["summary_truncated"],
        }

    @staticmethod
    def _error(status: int, code: str, message: str) -> httpx.Response:
        return httpx.Response(status, json={"code": code, "message": message})


def test_online_scalar_lifecycle_matches_golden_contract(tmp_path) -> None:
    server = ContractServer()
    run = create_run(
        project="contract",
        run_id="019c1234-5678-7000-8000-000000000101",
        config={"seed": 42},
        mode="online",
        spool_root=tmp_path,
        flush_interval=0,
        transport=httpx.MockTransport(server),
    )
    run.config.update({"optimizer": "adam"})
    run.log({"train": {"loss": 2.0}, "flag": True})
    run.log({"train": {"loss": 1.0}}, step=7)
    run.log({"reward": 3.0})
    run.summary["result"] = "running"
    run.finish(summary={"result": "complete", "tags": ["baseline", None]})
    run.finish()

    points = server.points(run.id)
    expected = FIXTURE["online"]
    assert [point["sequence"] for point in points] == expected["sequence"]
    assert [point["step"] for point in points] == expected["step"]
    assert sorted({key for point in points for key in point["metrics"]}) == expected["metric_keys"]
    assert {
        **server.runs[run.id]["metric_summary"],
        **server.runs[run.id]["explicit_summary"],
    } == expected["summary"]
    assert server.runs[run.id]["config"] == {"optimizer": "adam", "seed": 42}


def test_offline_restart_restores_spool_state_and_policies(tmp_path) -> None:
    run_id = "019c1234-5678-7000-8000-000000000102"
    first = create_run(
        project="contract",
        run_id=run_id,
        config={"seed": 7},
        mode="offline",
        spool_root=tmp_path,
    )
    first.summary["status"] = "recovered"
    first.log({"loss": 2.0}, step=5)
    del first

    with pytest.raises(DeliveryError, match="spool already exists"):
        create_run(
            project="contract",
            run_id=run_id,
            mode="offline",
            resume="never",
            spool_root=tmp_path,
        )
    resumed = create_run(
        project="contract",
        run_id=run_id,
        config={"seed": 7},
        mode="offline",
        resume="allow",
        spool_root=tmp_path,
    )
    assert resumed.config.to_dict() == {"seed": 7}
    assert resumed.summary.to_dict() == {"loss": 2.0, "status": "recovered"}
    resumed.log({"loss": 1.0, "reward": 4.0})
    resumed.finish()

    directory = tmp_path / run_id
    events = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
    metadata = json.loads((directory / "run.json").read_text())
    expected = FIXTURE["offline_restart"]
    assert [event["sequence"] for event in events] == expected["sequence"]
    assert [event["step"] for event in events] == expected["step"]
    assert {**metadata["metric_summary"], **metadata["explicit_summary"]} == expected["summary"]
    with pytest.raises(DeliveryError, match="finished run spool"):
        create_run(
            project="contract",
            run_id=run_id,
            mode="offline",
            resume="allow",
            spool_root=tmp_path,
        )
    with pytest.raises(DeliveryError, match="existing spool"):
        create_run(
            project="contract",
            run_id="019c1234-5678-7000-8000-000000000103",
            mode="offline",
            resume="must",
            spool_root=tmp_path,
        )


def test_response_loss_replays_the_exact_inflight_batch(tmp_path) -> None:
    server = ContractServer(lose_first_batch_response=True)
    run = create_run(
        project="contract",
        run_id="019c1234-5678-7000-8000-000000000104",
        mode="online",
        spool_root=tmp_path,
        flush_interval=0,
        transport=httpx.MockTransport(server),
    )
    run.log({"loss": 2.0})
    assert server.loss_observed.wait(2)
    run.log({"loss": 1.0})
    run.finish(timeout=2)

    assert server.batch_requests[0] == server.batch_requests[1]
    assert len(server.batch_requests[0]["points"]) == 1
    assert server.batch_requests[2]["points"][0]["sequence"] == 2


def test_remote_resume_uses_server_sequence_and_step(tmp_path) -> None:
    server = ContractServer()
    run_id = "019c1234-5678-7000-8000-000000000105"
    server.add_running_run(
        run_id,
        config={"seed": 1},
        summary={"loss": 2.0},
        last_sequence=8,
        last_step=42,
    )
    run = create_run(
        project="contract",
        run_id=run_id,
        config={"seed": 999},
        mode="online",
        resume="must",
        spool_root=tmp_path,
        flush_interval=0,
        transport=httpx.MockTransport(server),
    )
    run.log({"loss": 1.0})
    run.finish(timeout=2)

    point = server.points(run_id)[0]
    assert point["sequence"] == FIXTURE["remote_resume"]["sequence"]
    assert point["step"] == FIXTURE["remote_resume"]["step"]
    assert run.config.to_dict() == {"seed": 1}


def test_finished_offline_sync_is_replay_safe(tmp_path) -> None:
    server = ContractServer()
    run = create_run(
        project="contract",
        run_id="019c1234-5678-7000-8000-000000000106",
        mode="offline",
        spool_root=tmp_path,
        batch_size=1,
    )
    run.log({"loss": 2.0})
    run.log({"loss": 1.0})
    run.finish(summary={"status": "complete"})
    directory = tmp_path / run.id
    transport = httpx.MockTransport(server)

    assert sync_spool(directory, transport=transport, timeout=2) == run.id
    assert sync_spool(directory, transport=transport, timeout=2) == run.id
    assert len(server.runs[run.id]["batches"]) == 2
    assert server.runs[run.id]["state"] == "finished"


def test_restart_recovers_a_lost_finish_response(tmp_path) -> None:
    server = ContractServer(lose_first_finish_response=True)
    run_id = "019c1234-5678-7000-8000-000000000107"
    run = create_run(
        project="contract",
        run_id=run_id,
        mode="online",
        spool_root=tmp_path,
        flush_interval=0,
        transport=httpx.MockTransport(server),
    )
    run.log({"loss": 1.0})
    with pytest.raises(httpx.ReadError, match="finish response was lost"):
        run.finish(summary={"status": "complete"}, timeout=2)

    metadata_path = tmp_path / run_id / "run.json"
    interrupted_metadata = json.loads(metadata_path.read_text())
    assert interrupted_metadata["finishing"] is True
    assert interrupted_metadata["finished"] is False

    recovered = create_run(
        project="contract",
        run_id=run_id,
        mode="online",
        resume="allow",
        spool_root=tmp_path,
        transport=httpx.MockTransport(server),
    )
    assert recovered.finished
    assert recovered.summary.to_dict() == {"loss": 1.0, "status": "complete"}
    recovered_metadata = json.loads(metadata_path.read_text())
    assert recovered_metadata["finishing"] is False
    assert recovered_metadata["finished"] is True


def test_module_level_common_workflow_and_explicit_errors(tmp_path) -> None:
    assert runloom.run is None
    directly_finished = runloom.init(project="contract", mode="disabled", dir=tmp_path)
    directly_finished.finish()
    assert runloom.run is None

    run = runloom.init(project="contract", mode="disabled", dir=tmp_path)
    assert runloom.run is run
    runloom.config.update({"seed": 3})
    runloom.log({"loss": 1.0})
    runloom.summary["status"] = "complete"
    runloom.finish()

    assert runloom.run is None
    with pytest.raises(AttributeError, match=r"before runloom\.init"):
        _ = runloom.config
    with pytest.raises(RuntimeError, match="no active Runloom run"):
        runloom.log({"loss": 0.0})
    with pytest.raises(TypeError, match="unexpected keyword argument 'reinit'"):
        runloom.init(project="contract", mode="disabled", reinit=True)  # type: ignore[call-arg]
    with pytest.raises(ValueError, match="does not support resume"):
        create_run(project="contract", mode="disabled", resume="allow")
