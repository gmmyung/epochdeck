from __future__ import annotations

import json
import subprocess
import sys
import threading
import uuid

import pytest

import epochdeck as ed
from epochdeck._delivery import _DeliveryWorker
from epochdeck._protocol import DeliveryError
from epochdeck._spool import _Spool


def test_public_resume_rejects_an_active_writer_and_recovers_after_process_exit(tmp_path) -> None:
    run = ed.init(project="ownership", mode="offline", dir=tmp_path)
    run.log({"loss": 2})
    program = """
import os, sys
import epochdeck as ed
run = ed.init(project="ownership", id=sys.argv[2], mode="offline", resume="must", dir=sys.argv[1])
run.log({"loss": 1})
os._exit(0)
"""
    try:
        blocked = subprocess.run(
            [sys.executable, "-c", program, str(tmp_path), run.id],
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert blocked.returncode != 0
        assert "run spool is already active" in blocked.stderr
    finally:
        run.close()
    subprocess.run([sys.executable, "-c", program, str(tmp_path), run.id], check=True, timeout=10)
    with ed.init(
        project="ownership", id=run.id, mode="offline", resume="must", dir=tmp_path
    ) as resumed:
        resumed.log({"loss": 0})
    journal = tmp_path / ".epochdeck" / "spool" / run.id / "events.jsonl"
    points = [json.loads(line) for line in journal.read_text().splitlines()]
    assert [point["sequence"] for point in points] == [1, 2, 3]
    assert [point["metrics"]["loss"] for point in points] == [2, 1, 0]


@pytest.mark.parametrize(
    "journal_name", ["events.jsonl", "alerts.jsonl", "rich-values.jsonl", "artifacts.jsonl"]
)
def test_resume_discards_only_an_incomplete_final_append(tmp_path, journal_name) -> None:
    run = ed.init(project="recovery", mode="offline", dir=tmp_path)
    run.log({"loss": 2})
    run.close()
    directory = tmp_path / ".epochdeck" / "spool" / run.id
    journal = directory / journal_name
    original = journal.read_bytes()
    with journal.open("ab") as stream:
        stream.write(b'{"interrupted":')
    with pytest.warns(RuntimeWarning, match="recovered an interrupted journal append"):
        resumed = ed.init(
            project="recovery", id=run.id, mode="offline", resume="must", dir=tmp_path
        )
    try:
        assert journal.read_bytes() == original
        assert resumed.summary["loss"] == 2
        resumed.log({"loss": 1})
    finally:
        resumed.finish()
    points = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
    assert [point["sequence"] for point in points] == [1, 2]


@pytest.mark.parametrize("state", ["ack", "delivery", "summary", "oversized", "corrupt"])
def test_recovery_preserves_committed_or_corrupt_bytes(tmp_path, monkeypatch, state) -> None:
    run_id = str(uuid.uuid4())
    spool = _Spool(tmp_path, run_id)
    encoded = b'{"sequence":1}\n{"sequence":2'
    if state == "ack":
        spool.ack_path.write_text(str(len(encoded)))
    elif state == "delivery":
        spool.delivery_path.write_text(
            json.dumps({"start_offset": 0, "end_offset": len(encoded), "record_identity": "1"})
        )
    elif state == "summary":
        spool.write_metadata({"summary_event_offset": len(encoded)})
    elif state == "oversized":
        monkeypatch.setattr("epochdeck._spool._MAX_JOURNAL_RECORD_BYTES", 16)
        encoded = b"x" * 17
    else:
        encoded = b"invalid json\n"
    spool.events_path.write_bytes(encoded)
    spool.close()
    with pytest.raises(DeliveryError):
        reopened = _Spool(tmp_path, run_id)
        try:
            reopened.last_point()
        finally:
            reopened.close()
    assert spool.events_path.read_bytes() == encoded


def test_cancelled_delivery_holds_ownership_until_the_request_settles(tmp_path) -> None:
    run_id = str(uuid.uuid4())
    spool = _Spool(tmp_path, run_id)
    spool.append({"sequence": 1, "step": 0, "timestamp_ms": 1, "metrics": {"loss": 1}})
    started = threading.Event()
    release = threading.Event()

    class SlowClient:
        def ingest_batch(self, run_id, request):
            started.set()
            assert release.wait(5)

    worker = _DeliveryWorker(
        client=SlowClient(),  # type: ignore[arg-type]
        run_id=run_id,
        spool=spool,
        batch_size=1,
        flush_interval=0,
    )
    worker.start()
    try:
        assert started.wait(2)
        worker.cancel()
        with pytest.raises(DeliveryError, match="already active"):
            _Spool(tmp_path, run_id)
    finally:
        release.set()
        worker.join(5)
    assert not worker.is_alive()
    reopened = _Spool(tmp_path, run_id)
    reopened.close()


def test_close_releases_module_run_and_forbids_further_mutations(tmp_path) -> None:
    run = ed.init(project="closed", mode="offline", dir=tmp_path)
    run.close()
    run.close()
    assert ed.run is None
    for operation in [
        lambda: run.log({"loss": 1}),
        lambda: run.config.update({"seed": 1}),
        run.finish,
    ]:
        with pytest.raises(RuntimeError, match="closed"):
            operation()
