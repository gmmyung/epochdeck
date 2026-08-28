from __future__ import annotations

import json
import math
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, MutableMapping
from copy import deepcopy
from pathlib import Path
from typing import Any, Literal

from runloom._ids import uuid7
from runloom.artifact import Artifact
from runloom.client import RunloomApiError, RunloomClient
from runloom.rich import RichValue
from runloom.system_metrics import SystemMonitor, SystemSampler
from runloom.trace import Trace, TraceKind

Mode = Literal["online", "offline", "disabled"]
Resume = Literal["never", "allow", "must"]

_DEFAULT_BATCH_SIZE = 64
_DEFAULT_FLUSH_INTERVAL = 0.25
_DEFAULT_FINISH_TIMEOUT = 30.0
_MAX_DOCUMENT_BYTES = 256 * 1024
_SPOOL_FORMAT_VERSION = 1
_DEFAULT_SYSTEM_METRIC_INTERVAL = 15.0
_SYSTEM_METRIC_PREFIX = "system/"
_MAX_ALERT_TITLE_BYTES = 256
_MAX_ALERT_TEXT_BYTES = 4_096


class DeliveryError(RuntimeError):
    pass


class SweepEarlyStop(RuntimeError):
    pass


class _RunDocument(Mapping[str, Any]):
    def __init__(self, initial: Mapping[str, Any], lock: threading.RLock) -> None:
        self._data = deepcopy(dict(initial))
        self._lock = lock

    def __getitem__(self, key: str) -> Any:
        with self._lock:
            return deepcopy(self._data[key])

    def __iter__(self) -> Iterator[str]:
        with self._lock:
            return iter(tuple(self._data))

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)

    def __repr__(self) -> str:
        return repr(self.to_dict())

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as error:
            raise AttributeError(name) from error

    def to_dict(self) -> dict[str, Any]:
        with self._lock:
            return deepcopy(self._data)

    def _replace(self, values: Mapping[str, Any]) -> None:
        with self._lock:
            self._data.clear()
            self._data.update(deepcopy(dict(values)))

    def _merge_local(self, values: Mapping[str, Any]) -> None:
        with self._lock:
            self._data.update(deepcopy(dict(values)))


class RunConfig(_RunDocument):
    def __init__(
        self,
        initial: Mapping[str, Any],
        lock: threading.RLock,
        updater: Callable[[dict[str, Any], bool], None],
    ) -> None:
        super().__init__(initial, lock)
        self._updater = updater

    def update(
        self,
        values: Mapping[str, Any] | None = None,
        *,
        allow_val_change: bool = False,
        **kwargs: Any,
    ) -> None:
        updates = _collect_document_updates(values, kwargs, "config")
        if updates:
            self._updater(updates, allow_val_change)

    def __setitem__(self, key: str, value: Any) -> None:
        self.update({key: value})


class RunSummary(_RunDocument, MutableMapping[str, Any]):
    def __init__(
        self,
        initial: Mapping[str, Any],
        lock: threading.RLock,
        updater: Callable[[dict[str, Any]], None],
    ) -> None:
        super().__init__(initial, lock)
        self._updater = updater

    def update(
        self,
        values: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        updates = _collect_document_updates(values, kwargs, "summary")
        if updates:
            self._updater(updates)

    def __setitem__(self, key: str, value: Any) -> None:
        self.update({key: value})

    def __delitem__(self, key: str) -> None:
        raise TypeError(f"summary key deletion is not supported: {key}")


class _Spool:
    def __init__(self, root: Path, run_id: str) -> None:
        self.directory = root / run_id
        self.directory.mkdir(parents=True, exist_ok=True)
        self.events_path = self.directory / "events.jsonl"
        self.ack_path = self.directory / "ack"
        self.metadata_path = self.directory / "run.json"
        self.delivery_path = self.directory / "delivery.json"
        self.alerts_path = self.directory / "alerts.jsonl"
        self.alert_ack_path = self.directory / "alert-ack"
        self.alert_delivery_path = self.directory / "alert-delivery.json"
        self.rich_values_path = self.directory / "rich-values.jsonl"
        self.rich_ack_path = self.directory / "rich-ack"
        self.rich_delivery_path = self.directory / "rich-delivery.json"
        self.blob_root = self.directory / "blobs"
        self.artifacts_path = self.directory / "artifacts.jsonl"
        self.artifact_ack_path = self.directory / "artifact-ack"
        self.artifact_delivery_path = self.directory / "artifact-delivery.json"
        self.traces_path = self.directory / "traces.jsonl"
        self.trace_ack_path = self.directory / "trace-ack"
        self.trace_delivery_path = self.directory / "trace-delivery.json"
        self._lock = threading.Lock()
        self.events_path.touch(exist_ok=True)
        self.alerts_path.touch(exist_ok=True)
        self.rich_values_path.touch(exist_ok=True)
        self.artifacts_path.touch(exist_ok=True)
        self.traces_path.touch(exist_ok=True)
        self.blob_root.mkdir(exist_ok=True)

    def read_metadata(self) -> dict[str, Any] | None:
        with self._lock:
            if not self.metadata_path.exists():
                return None
            return self._read_json_object(self.metadata_path, "run metadata")

    def write_metadata(self, metadata: dict[str, Any]) -> None:
        with self._lock:
            _atomic_json_write(self.metadata_path, metadata)

    def update_metadata(self, updates: Mapping[str, Any]) -> None:
        with self._lock:
            metadata = self._read_json_object(self.metadata_path, "run metadata")
            metadata.update(deepcopy(dict(updates)))
            _atomic_json_write(self.metadata_path, metadata)

    def append(self, point: dict[str, Any]) -> None:
        self._append_record(self.events_path, point)

    def append_alert(self, alert: dict[str, Any]) -> None:
        self._append_record(self.alerts_path, alert)

    def append_rich_value(self, value: dict[str, Any]) -> None:
        self._append_record(self.rich_values_path, value)

    def append_artifact(self, artifact: dict[str, Any]) -> None:
        self._append_record(self.artifacts_path, artifact)

    def append_trace(self, trace: dict[str, Any]) -> None:
        self._append_record(self.traces_path, trace)

    def _append_record(self, path: Path, record: dict[str, Any]) -> None:
        encoded = json.dumps(record, separators=(",", ":"), sort_keys=True, allow_nan=False)
        with self._lock, path.open("a", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())

    def read_batch(self, limit: int) -> tuple[list[dict[str, Any]], int]:
        return self._read_record_batch(
            self.events_path,
            self.ack_path,
            self.delivery_path,
            limit,
        )

    def read_alert(self) -> tuple[dict[str, Any] | None, int]:
        alerts, offset = self._read_record_batch(
            self.alerts_path,
            self.alert_ack_path,
            self.alert_delivery_path,
            1,
        )
        return (alerts[0] if alerts else None), offset

    def read_rich_value(self) -> tuple[dict[str, Any] | None, int]:
        values, offset = self._read_record_batch(
            self.rich_values_path,
            self.rich_ack_path,
            self.rich_delivery_path,
            1,
        )
        return (values[0] if values else None), offset

    def read_artifact(self) -> tuple[dict[str, Any] | None, int]:
        artifacts, offset = self._read_record_batch(
            self.artifacts_path,
            self.artifact_ack_path,
            self.artifact_delivery_path,
            1,
        )
        return (artifacts[0] if artifacts else None), offset

    def read_trace(self) -> tuple[dict[str, Any] | None, int]:
        traces, offset = self._read_record_batch(
            self.traces_path,
            self.trace_ack_path,
            self.trace_delivery_path,
            1,
        )
        return (traces[0] if traces else None), offset

    def _read_record_batch(
        self,
        journal_path: Path,
        ack_path: Path,
        delivery_path: Path,
        limit: int,
    ) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            offset = self._read_ack(ack_path, journal_path)
            size = journal_path.stat().st_size
            delivery = self._read_delivery(delivery_path, offset, size)
            fixed_end = int(delivery["end_offset"]) if delivery is not None else None
            points: list[dict[str, Any]] = []
            with journal_path.open("rb") as stream:
                stream.seek(offset)
                while len(points) < limit or fixed_end is not None:
                    if fixed_end is not None and stream.tell() >= fixed_end:
                        break
                    line = stream.readline()
                    if not line:
                        break
                    if fixed_end is not None and stream.tell() > fixed_end:
                        raise DeliveryError(
                            f"delivery boundary splits a journal record: {delivery_path}"
                        )
                    points.append(self._decode_record(line, stream.tell(), journal_path))
                next_offset = stream.tell()
            if fixed_end is not None and next_offset != fixed_end:
                raise DeliveryError(f"delivery boundary is outside journal: {delivery_path}")
            if points and delivery is None:
                record_identity = points[0].get("sequence", points[0].get("id"))
                if record_identity is None:
                    raise DeliveryError(f"journal record has no durable identity: {journal_path}")
                delivery = {
                    "start_offset": offset,
                    "end_offset": next_offset,
                    "record_identity": str(record_identity),
                }
                _atomic_json_write(delivery_path, delivery)
            if points:
                expected_identity = points[0].get("sequence", points[0].get("id"))
                stored_identity = delivery.get("record_identity", delivery.get("batch_sequence"))
                if str(stored_identity) != str(expected_identity):
                    raise DeliveryError(
                        f"delivery identity does not match journal: {delivery_path}"
                    )
            return points, next_offset

    def acknowledge(self, offset: int) -> None:
        self._acknowledge(self.ack_path, self.delivery_path, offset)

    def acknowledge_alert(self, offset: int) -> None:
        self._acknowledge(self.alert_ack_path, self.alert_delivery_path, offset)

    def acknowledge_rich_value(self, offset: int) -> None:
        self._acknowledge(self.rich_ack_path, self.rich_delivery_path, offset)

    def acknowledge_artifact(self, offset: int) -> None:
        self._acknowledge(self.artifact_ack_path, self.artifact_delivery_path, offset)

    def acknowledge_trace(self, offset: int) -> None:
        self._acknowledge(self.trace_ack_path, self.trace_delivery_path, offset)

    def _acknowledge(self, ack_path: Path, delivery_path: Path, offset: int) -> None:
        with self._lock:
            if delivery_path.exists():
                delivery = self._read_json_object(delivery_path, "delivery state")
                if int(delivery.get("end_offset", -1)) != offset:
                    raise DeliveryError(
                        f"acknowledgement does not match delivery boundary: {delivery_path}"
                    )
            _atomic_text_write(ack_path, str(offset))
            delivery_path.unlink(missing_ok=True)

    def pending(self) -> bool:
        return (
            self.pending_metrics()
            or self.pending_alerts()
            or self.pending_rich_values()
            or self.pending_artifacts()
            or self.pending_traces()
        )

    def pending_metrics(self) -> bool:
        return self._pending(self.ack_path, self.events_path)

    def pending_alerts(self) -> bool:
        return self._pending(self.alert_ack_path, self.alerts_path)

    def pending_rich_values(self) -> bool:
        return self._pending(self.rich_ack_path, self.rich_values_path)

    def pending_artifacts(self) -> bool:
        return self._pending(self.artifact_ack_path, self.artifacts_path)

    def pending_traces(self) -> bool:
        return self._pending(self.trace_ack_path, self.traces_path)

    def _pending(self, ack_path: Path, journal_path: Path) -> bool:
        with self._lock:
            return self._read_ack(ack_path, journal_path) < journal_path.stat().st_size

    def last_point(self) -> dict[str, Any] | None:
        return self._last_record(self.events_path)

    def last_rich_value(self) -> dict[str, Any] | None:
        return self._last_record(self.rich_values_path)

    def blob_path(self, digest: str) -> Path:
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise DeliveryError("rich value blob has an invalid SHA-256 digest")
        return self.blob_root / digest

    def _last_record(self, path: Path) -> dict[str, Any] | None:
        with self._lock, path.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            end = stream.tell()
            if end == 0:
                return None
            position = end - 1
            while position > 0:
                stream.seek(position - 1)
                if stream.read(1) == b"\n" and position < end - 1:
                    break
                position -= 1
            stream.seek(position)
            line = stream.readline()
        return self._decode_record(line, end, path) if line.strip() else None

    def pending_summary(self) -> dict[str, Any]:
        with self._lock:
            offset = self._read_ack(self.ack_path, self.events_path)
            summary: dict[str, Any] = {}
            known_keys: set[str] = set()
            with self.events_path.open("rb") as stream:
                stream.seek(offset)
                while line := stream.readline():
                    event = self._decode_record(line, stream.tell(), self.events_path)
                    metrics = event.get("metrics")
                    if not isinstance(metrics, dict):
                        raise DeliveryError(
                            f"journal event has no metric object: {self.events_path}"
                        )
                    summary_metrics = {
                        key: value
                        for key, value in metrics.items()
                        if not key.startswith(_SYSTEM_METRIC_PREFIX)
                    }
                    new_keys = summary_metrics.keys() - known_keys
                    summary.update(summary_metrics)
                    if new_keys:
                        known_keys.update(new_keys)
                        summary = _normalize_document(summary, "summary")
            return summary

    def _read_delivery(
        self,
        delivery_path: Path,
        offset: int,
        size: int,
    ) -> dict[str, Any] | None:
        if not delivery_path.exists():
            return None
        delivery = self._read_json_object(delivery_path, "delivery state")
        try:
            start = int(delivery["start_offset"])
            end = int(delivery["end_offset"])
            identity = delivery.get("record_identity", delivery.get("batch_sequence"))
            if not isinstance(identity, (str, int)) or isinstance(identity, bool):
                raise TypeError
        except (KeyError, TypeError, ValueError) as error:
            raise DeliveryError(f"invalid delivery state: {delivery_path}") from error
        if end <= offset:
            delivery_path.unlink(missing_ok=True)
            return None
        if start != offset or end <= start or end > size:
            raise DeliveryError(f"delivery state is outside journal: {delivery_path}")
        return delivery

    def _read_json_object(self, path: Path, name: str) -> dict[str, Any]:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise DeliveryError(f"invalid {name}: {path}") from error
        if not isinstance(value, dict):
            raise DeliveryError(f"invalid {name}: {path}")
        return value

    def _decode_record(self, line: bytes, offset: int, path: Path) -> dict[str, Any]:
        try:
            value = json.loads(line)
        except json.JSONDecodeError as error:
            raise DeliveryError(f"invalid journal event ending at byte {offset}: {path}") from error
        if not isinstance(value, dict):
            raise DeliveryError(f"invalid journal event ending at byte {offset}: {path}")
        return value

    def _read_ack(self, ack_path: Path, journal_path: Path) -> int:
        if not ack_path.exists():
            return 0
        try:
            offset = int(ack_path.read_text(encoding="ascii"))
        except (OSError, ValueError) as error:
            raise DeliveryError(f"invalid spool acknowledgement: {ack_path}") from error
        size = journal_path.stat().st_size
        if offset < 0 or offset > size:
            raise DeliveryError(f"spool acknowledgement is outside journal: {ack_path}")
        return offset


class _DeliveryWorker(threading.Thread):
    def __init__(
        self,
        *,
        client: RunloomClient,
        run_id: str,
        spool: _Spool,
        batch_size: int,
        flush_interval: float,
        stop_requested: Callable[[], None],
    ) -> None:
        super().__init__(name=f"runloom-{run_id[:8]}", daemon=True)
        self._client = client
        self._run_id = run_id
        self._spool = spool
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._stop_requested = stop_requested
        self._wake = threading.Event()
        self._stopping = threading.Event()
        self._cancelled = threading.Event()
        self._delivery_cursor = 0
        self.last_error: Exception | None = None

    def notify(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stopping.set()
        self._wake.set()

    def cancel(self) -> None:
        self._cancelled.set()
        self._wake.set()

    def run(self) -> None:
        retry_delay = 0.25
        while True:
            if self._cancelled.is_set():
                return
            if not self._spool.pending():
                if self._stopping.is_set():
                    return
                self._wake.wait()
                self._wake.clear()
                if not self._stopping.is_set():
                    time.sleep(self._flush_interval)
            try:
                delivery = self._next_delivery()
                if delivery == "alert":
                    alert, next_offset = self._spool.read_alert()
                    if alert is None:
                        continue
                    self._client.create_alert(self._run_id, alert)
                    self._spool.acknowledge_alert(next_offset)
                elif delivery == "rich":
                    value, next_offset = self._spool.read_rich_value()
                    if value is None:
                        continue
                    blob = value.get("blob")
                    if blob is not None:
                        self._client.upload_blob(
                            self._spool.blob_path(str(blob["digest"])),
                            blob,
                        )
                    self._client.create_rich_value(self._run_id, value)
                    self._spool.acknowledge_rich_value(next_offset)
                elif delivery == "artifact":
                    artifact, next_offset = self._spool.read_artifact()
                    if artifact is None:
                        continue
                    operation = artifact.pop("operation", None)
                    if operation == "create":
                        for entry in artifact["entries"]:
                            blob = entry["blob"]
                            self._client.upload_blob(
                                self._spool.blob_path(str(blob["digest"])),
                                blob,
                            )
                        self._client.create_artifact(self._run_id, artifact)
                    elif operation == "use":
                        self._client.use_artifact(self._run_id, str(artifact["artifact_id"]))
                    else:
                        raise DeliveryError("artifact journal has an unknown operation")
                    self._spool.acknowledge_artifact(next_offset)
                elif delivery == "trace":
                    trace, next_offset = self._spool.read_trace()
                    if trace is None:
                        continue
                    blob = trace.get("payload")
                    if blob is not None:
                        self._client.upload_blob(
                            self._spool.blob_path(str(blob["digest"])),
                            blob,
                        )
                    self._client.create_trace_span(self._run_id, trace)
                    self._spool.acknowledge_trace(next_offset)
                elif delivery == "metrics":
                    points, next_offset = self._spool.read_batch(self._batch_size)
                    if not points:
                        continue
                    request = {"batch_sequence": points[0]["sequence"], "points": points}
                    response = self._client.ingest_batch(self._run_id, request)
                    if response.get("stop_requested") is True:
                        self._stop_requested()
                    self._spool.acknowledge(next_offset)
                else:
                    continue
            except Exception as error:  # The durable journal remains authoritative.
                self.last_error = error
                self._wake.wait(retry_delay)
                self._wake.clear()
                if self._cancelled.is_set():
                    return
                retry_delay = min(retry_delay * 2, 5.0)
            else:
                self.last_error = None
                retry_delay = 0.25

    def _next_delivery(self) -> str | None:
        pending = (
            self._spool.pending_metrics,
            self._spool.pending_rich_values,
            self._spool.pending_artifacts,
            self._spool.pending_traces,
            self._spool.pending_alerts,
        )
        names = ("metrics", "rich", "artifact", "trace", "alert")
        for offset in range(len(pending)):
            index = (self._delivery_cursor + offset) % len(pending)
            if pending[index]():
                self._delivery_cursor = (index + 1) % len(pending)
                return names[index]
        return None


class Run:
    def __init__(
        self,
        *,
        project: str,
        run_id: str,
        name: str | None,
        config: Mapping[str, Any],
        mode: Mode,
        resume: Resume,
        server_url: str,
        spool_root: Path,
        batch_size: int = _DEFAULT_BATCH_SIZE,
        flush_interval: float = _DEFAULT_FLUSH_INTERVAL,
        system_monitor_interval: float | None = _DEFAULT_SYSTEM_METRIC_INTERVAL,
        system_sampler: Callable[[], Mapping[str, float]] | None = None,
        transport: Any = None,
        sweep_trial_id: str | None = None,
    ) -> None:
        batch_size = _validate_batch_size(batch_size, "batch_size")
        if flush_interval < 0:
            raise ValueError("flush_interval cannot be negative")
        initial_config = _normalize_document(config, "config")
        self.project = project
        self.id = run_id
        self.name = name
        self.mode = mode
        self.resume = resume
        self.server_url = server_url
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._finished = False
        self._finishing = False
        self._log_lock = threading.Lock()
        self._document_lock = threading.RLock()
        self._client: RunloomClient | None = None
        self._worker: _DeliveryWorker | None = None
        self._spool: _Spool | None = None
        self._finish_callback: Callable[[Run], None] | None = None
        self._system_monitor: SystemMonitor | None = None
        self._system_monitor_interval = system_monitor_interval
        self._system_sampler = system_sampler
        self._sweep_trial_id = sweep_trial_id
        self._stop_requested = threading.Event()
        self.config = RunConfig(
            initial_config,
            self._document_lock,
            self._update_config,
        )
        self.summary = RunSummary(
            {},
            self._document_lock,
            self._update_summary,
        )

        if mode == "disabled":
            self._next_sequence = 1
            self._next_step = 0
            self._last_user_step: int | None = None
            return

        self._spool = _Spool(spool_root, run_id)
        stored_metadata = self._spool.read_metadata()
        stored_finishing = False
        if stored_metadata is not None:
            _validate_spool_identity(stored_metadata, project, run_id)
            stored_finished = _metadata_flag(stored_metadata, "finished")
            stored_finishing = _metadata_flag(stored_metadata, "finishing")
            if resume == "never":
                raise DeliveryError(
                    "run spool already exists; use resume='allow' or 'must': "
                    f"{self._spool.directory}"
                )
            if stored_finished:
                raise DeliveryError(
                    f"finished run spool cannot be resumed: {self._spool.directory}"
                )
            stored_config = _normalize_document(stored_metadata.get("config", {}), "config")
            if initial_config and initial_config != stored_config:
                raise DeliveryError(
                    "resume config differs from the durable spool; resume first, then use "
                    "run.config.update(..., allow_val_change=True)"
                )
            self.config._replace(stored_config)
            self.summary._replace(
                _normalize_document(stored_metadata.get("summary", {}), "summary")
            )
            stored_name = stored_metadata.get("name")
            if stored_name is not None and not isinstance(stored_name, str):
                raise DeliveryError("stored run name must be a string or null")
            self.name = stored_name if stored_name is not None else name
            self._batch_size = _validate_batch_size(
                stored_metadata.get("batch_size", batch_size),
                "stored batch_size",
            )
        elif mode == "offline" and resume == "must":
            raise DeliveryError(
                f"resume='must' requires an existing spool: {self._spool.directory}"
            )

        last_point = self._spool.last_point()
        last_rich_value = self._spool.last_rich_value()
        self._next_sequence = int(last_point["sequence"]) + 1 if last_point else 1
        last_steps = [
            int(record["step"]) for record in (last_point, last_rich_value) if record is not None
        ]
        self._last_user_step = max(last_steps) if last_steps else None
        self._next_step = self._last_user_step + 1 if self._last_user_step is not None else 0
        pending_summary = self._spool.pending_summary()
        self.summary._replace(
            _normalize_document({**self.summary.to_dict(), **pending_summary}, "summary")
        )
        metadata = {
            "format_version": _SPOOL_FORMAT_VERSION,
            "project": project,
            "id": run_id,
            "name": self.name,
            "config": self.config.to_dict(),
            "summary": self.summary.to_dict(),
            "resume": resume,
            "server_url": server_url,
            "batch_size": self._batch_size,
            "sweep_trial_id": sweep_trial_id,
            "finished": False,
            "finishing": stored_finishing,
        }
        self._spool.write_metadata(metadata)

        if mode == "offline":
            self._start_system_monitor()
            return

        self._client = RunloomClient(server_url, transport=transport)
        try:
            response = self._client.create_run(
                project=project,
                run_id=run_id,
                name=self.name,
                config=self.config.to_dict(),
                resume=resume,
                sweep_trial_id=sweep_trial_id,
            )
        except RunloomApiError as error:
            try:
                if (
                    stored_metadata is not None
                    and stored_finishing
                    and error.status_code == 409
                    and not self._spool.pending()
                ):
                    existing = self._client.get_run(run_id)
                    actual_summary = _normalize_document(existing.get("summary", {}), "summary")
                    expected_summary = self.summary.to_dict()
                    if existing.get("state") == "finished" and all(
                        actual_summary.get(key) == value for key, value in expected_summary.items()
                    ):
                        self.name = str(existing["name"])
                        self.config._replace(
                            _normalize_document(existing.get("config", {}), "config")
                        )
                        self.summary._replace(actual_summary)
                        self._spool.update_metadata(
                            {
                                "name": self.name,
                                "config": self.config.to_dict(),
                                "summary": actual_summary,
                                "finished": True,
                                "finishing": False,
                            }
                        )
                        self._finished = True
                        return
            finally:
                self._client.close()
            raise
        except Exception:
            self._client.close()
            raise
        try:
            server_run = response["run"]
            self.name = str(server_run["name"])
            self.config._replace(
                _normalize_document(server_run.get("config", self.config.to_dict()), "config")
            )
            server_summary = _normalize_document(
                server_run.get("summary", {}),
                "summary",
            )
            self.summary._replace(
                _normalize_document(
                    {**server_summary, **self.summary.to_dict(), **pending_summary},
                    "summary",
                )
            )
            server_next_sequence = _response_position(response, "next_sequence", minimum=1)
            server_next_step = _response_position(response, "next_step", minimum=0)
            self._next_sequence = max(self._next_sequence, server_next_sequence)
            self._next_step = max(self._next_step, server_next_step)
            if self._next_step > 0:
                self._last_user_step = self._next_step - 1
            self._spool.update_metadata(
                {
                    "name": self.name,
                    "config": self.config.to_dict(),
                    "summary": self.summary.to_dict(),
                    "finishing": False,
                }
            )
            self._worker = _DeliveryWorker(
                client=self._client,
                run_id=run_id,
                spool=self._spool,
                batch_size=self._batch_size,
                flush_interval=flush_interval,
                stop_requested=self._stop_requested.set,
            )
            self._worker.start()
            if self._spool.pending():
                self._worker.notify()
            self._start_system_monitor()
        except Exception:
            if self._worker is not None:
                self._worker.cancel()
                self._worker.join(1)
            self._client.close()
            raise

    def __enter__(self) -> Run:
        return self

    def __exit__(self, *_: object) -> None:
        self.finish()

    @property
    def finished(self) -> bool:
        return self._finished

    @property
    def system_monitor_error(self) -> Exception | None:
        return self._system_monitor.last_error if self._system_monitor is not None else None

    @property
    def should_stop(self) -> bool:
        return self._stop_requested.is_set()

    def _set_finish_callback(self, callback: Callable[[Run], None]) -> None:
        self._finish_callback = callback

    def _complete(self) -> None:
        self._finished = True
        self._finishing = False
        callback = self._finish_callback
        self._finish_callback = None
        if callback is not None:
            callback(self)

    def _update_config(self, updates: dict[str, Any], allow_val_change: bool) -> None:
        with self._log_lock:
            self._ensure_documents_mutable()
            current = self.config.to_dict()
            if not allow_val_change:
                for key, value in updates.items():
                    if key in current and current[key] != value:
                        raise ValueError(
                            f"config key '{key}' already exists; "
                            "pass allow_val_change=True to replace it"
                        )
            merged = _normalize_document({**current, **updates}, "config")
            authoritative = merged
            if self.mode == "online":
                assert self._client is not None
                response = self._client.update_config(
                    self.id,
                    updates,
                    allow_val_change=allow_val_change,
                )
                authoritative = _normalize_document(
                    response["run"].get("config", merged),
                    "config",
                )
            self.config._replace(authoritative)
            if self._spool is not None:
                self._spool.update_metadata({"config": authoritative})

    def _update_summary(self, updates: dict[str, Any]) -> None:
        with self._log_lock:
            self._ensure_documents_mutable()
            merged = _normalize_document({**self.summary.to_dict(), **updates}, "summary")
            authoritative = merged
            if self.mode == "online":
                assert self._client is not None
                response = self._client.update_summary(self.id, updates)
                server_summary = _normalize_document(
                    response["run"].get("summary", merged),
                    "summary",
                )
                authoritative = _normalize_document(
                    {**server_summary, **merged},
                    "summary",
                )
            self.summary._replace(authoritative)
            if self._spool is not None:
                self._spool.update_metadata({"summary": authoritative})

    def _ensure_documents_mutable(self) -> None:
        if self._finished or self._finishing:
            raise RuntimeError(
                "cannot update config or summary while a run is finishing or finished"
            )

    def log(self, data: Mapping[str, Any], *, step: int | None = None) -> None:
        with self._log_lock:
            if self._finished or self._finishing:
                raise RuntimeError("cannot log while a run is finishing or finished")
            if self._stop_requested.is_set():
                raise SweepEarlyStop("the sweep scheduler requested early termination")
            metrics, rich_values = _flatten_log_values(data)
            if not metrics and not rich_values:
                raise ValueError("log data contains no supported values")
            selected_step = self._next_step if step is None else step
            _validate_step(selected_step)
            prepared_values = []
            if rich_values and self.mode != "disabled":
                assert self._spool is not None
                for key, value in rich_values:
                    _validate_rich_key(key)
                    prepared_values.append((key, value._prepare(self._spool.blob_root)))
            if metrics:
                self._append_metrics(metrics, selected_step, advance_step=False, summarize=True)
            if prepared_values:
                assert self._spool is not None
                timestamp_ms = time.time_ns() // 1_000_000
                for key, prepared in prepared_values:
                    self._spool.append_rich_value(
                        {
                            "id": uuid7(),
                            "key": key,
                            "kind": prepared.kind,
                            "step": selected_step,
                            "timestamp_ms": timestamp_ms,
                            "blob": prepared.blob,
                            "metadata": _normalize_document(
                                prepared.metadata,
                                f"rich value '{key}' metadata",
                            ),
                        }
                    )
            self._last_user_step = selected_step
            self._next_step = selected_step + 1
        if self._worker is not None:
            self._worker.notify()

    def alert(
        self,
        title: str,
        text: str = "",
        *,
        level: str = "info",
    ) -> None:
        with self._log_lock:
            if self._finished or self._finishing:
                raise RuntimeError("cannot alert while a run is finishing or finished")
            if self.mode == "disabled":
                return
            normalized_title, normalized_text, normalized_level = _validate_alert(
                title,
                text,
                level,
            )
            assert self._spool is not None
            self._spool.append_alert(
                {
                    "id": uuid7(),
                    "title": normalized_title,
                    "text": normalized_text,
                    "level": normalized_level,
                    "step": self._last_user_step,
                    "timestamp_ms": time.time_ns() // 1_000_000,
                }
            )
        if self._worker is not None:
            self._worker.notify()

    def log_artifact(
        self,
        artifact: Artifact,
        *,
        aliases: list[str] | tuple[str, ...] | None = None,
    ) -> Artifact:
        if not isinstance(artifact, Artifact):
            raise TypeError("log_artifact expects a runloom.Artifact")
        if aliases is not None and not isinstance(aliases, (list, tuple)):
            raise TypeError("artifact aliases must be a list or tuple of strings")
        with self._log_lock:
            if self._finished or self._finishing:
                raise RuntimeError("cannot log an artifact while a run is finishing or finished")
            if self.mode == "disabled":
                return artifact
            assert self._spool is not None
            record = artifact._prepare(self._spool.blob_root, aliases or ("latest",))
            record["metadata"] = _normalize_document(record["metadata"], "artifact metadata")
            self._spool.append_artifact(record)
        if self._worker is not None:
            self._worker.notify()
        return artifact

    def use_artifact(self, artifact: Artifact | str) -> str:
        with self._log_lock:
            if self._finished or self._finishing:
                raise RuntimeError("cannot use an artifact while a run is finishing or finished")
            if isinstance(artifact, Artifact):
                artifact_id = artifact.id
            elif isinstance(artifact, str):
                if self.mode == "disabled":
                    return artifact
                artifact_id = self._resolve_artifact_reference(artifact)
            else:
                raise TypeError("use_artifact expects an Artifact, artifact ID, or 'name:alias'")
            if self.mode == "disabled":
                return artifact_id
            assert self._spool is not None
            self._spool.append_artifact(
                {"id": uuid7(), "operation": "use", "artifact_id": artifact_id}
            )
        if self._worker is not None:
            self._worker.notify()
        return artifact_id

    def trace(
        self,
        name: str,
        *,
        kind: TraceKind = "span",
        trace_id: str | None = None,
        parent: Trace | str | None = None,
        attributes: Mapping[str, Any] | None = None,
        inputs: Any = None,
        start_time_ms: int | None = None,
    ) -> Trace:
        with self._log_lock:
            if self._finished or self._finishing:
                raise RuntimeError("cannot create a trace while a run is finishing or finished")
        return Trace(
            name,
            recorder=self._record_trace,
            kind=kind,
            trace_id=trace_id,
            parent=parent,
            attributes=attributes,
            inputs=inputs,
            start_time_ms=start_time_ms,
        )

    def _record_trace(self, trace: Trace) -> None:
        with self._log_lock:
            if self._finished or self._finishing:
                raise RuntimeError("cannot finish a trace while a run is finishing or finished")
            if self.mode == "disabled":
                return
            assert self._spool is not None
            record = trace._prepare(self._spool.blob_root, self._last_user_step)
            record["attributes"] = _normalize_document(
                record["attributes"],
                "trace attributes",
            )
            record["preview"] = _normalize_document(record["preview"], "trace preview")
            self._spool.append_trace(record)
        if self._worker is not None:
            self._worker.notify()

    def _resolve_artifact_reference(self, reference: str) -> str:
        try:
            return str(uuid.UUID(reference))
        except ValueError:
            pass
        if self.mode != "online":
            raise ValueError("offline artifact references must use a concrete artifact ID")
        name, separator, alias = reference.partition(":")
        if not separator or not name or not alias:
            raise ValueError("artifact reference must be an ID or 'name:alias'")
        assert self._client is not None
        artifact = self._client.resolve_artifact(self.project, name, alias)
        return str(artifact["id"])

    def _log_system_metrics(self, data: Mapping[str, float]) -> None:
        metrics = _flatten_metrics(data)
        if not metrics:
            return
        with self._log_lock:
            if self._finished or self._finishing:
                return
            if self._last_user_step is None:
                return
            self._append_metrics(
                metrics,
                self._last_user_step,
                advance_step=False,
                summarize=False,
            )
        if self._worker is not None:
            self._worker.notify()

    def _append_metrics(
        self,
        metrics: Mapping[str, float],
        step: int,
        *,
        advance_step: bool,
        summarize: bool,
    ) -> None:
        point = {
            "sequence": self._next_sequence,
            "step": step,
            "timestamp_ms": time.time_ns() // 1_000_000,
            "metrics": dict(metrics),
        }
        if self._spool is not None:
            self._spool.append(point)
        if summarize:
            self.summary._merge_local(metrics)
        self._next_sequence += 1
        if advance_step:
            self._next_step = step + 1

    def _start_system_monitor(self) -> None:
        if self._system_monitor_interval is None or self._spool is None:
            return
        sampler = self._system_sampler or SystemSampler(self._spool.directory).sample
        self._system_monitor = SystemMonitor(
            interval=self._system_monitor_interval,
            sampler=sampler,
            recorder=self._log_system_metrics,
        )
        self._system_monitor.start()

    def _stop_system_monitor(self, timeout: float) -> None:
        monitor = self._system_monitor
        if monitor is None:
            return
        monitor.stop()
        monitor.join(timeout)
        if monitor.is_alive():
            raise DeliveryError("timed out stopping the system metric monitor")

    def finish(
        self,
        *,
        summary: Mapping[str, Any] | None = None,
        timeout: float = _DEFAULT_FINISH_TIMEOUT,
    ) -> None:
        with self._log_lock:
            if self._finished:
                return
            if timeout <= 0:
                raise ValueError("finish timeout must be positive")
            explicit_summary = _normalize_document(summary or {}, "summary")
            final_summary = _normalize_document(
                {**self.summary.to_dict(), **explicit_summary},
                "summary",
            )
            self._finishing = True
            if self._spool is not None:
                self._spool.update_metadata({"finishing": True, "summary": final_summary})
        deadline = time.monotonic() + timeout
        self._stop_system_monitor(timeout)
        if self.mode == "disabled":
            self.summary._replace(final_summary)
            self._complete()
            return
        assert self._spool is not None
        if self.mode == "offline":
            self.summary._replace(final_summary)
            self._spool.update_metadata(
                {"finished": True, "finishing": False, "summary": final_summary}
            )
            self._complete()
            return

        assert self._worker is not None
        assert self._client is not None
        self._worker.stop()
        self._worker.join(max(deadline - time.monotonic(), 0))
        if self._worker.is_alive() or self._spool.pending():
            error = self._worker.last_error
            message = f"timed out with undelivered data in {self._spool.directory}"
            if error is not None:
                message = f"{message}: {error}"
            raise DeliveryError(message)
        response = self._client.finish_run(self.id, final_summary)
        authoritative_summary = _normalize_document(
            response["run"].get("summary", final_summary),
            "summary",
        )
        self.summary._replace(authoritative_summary)
        self._spool.update_metadata(
            {"finished": True, "finishing": False, "summary": authoritative_summary}
        )
        self._client.close()
        self._complete()


def create_run(
    *,
    project: str,
    name: str | None = None,
    run_id: str | None = None,
    config: Mapping[str, Any] | None = None,
    mode: Mode = "online",
    resume: Resume = "never",
    server_url: str = "http://127.0.0.1:8787",
    spool_root: str | Path | None = None,
    batch_size: int = _DEFAULT_BATCH_SIZE,
    flush_interval: float = _DEFAULT_FLUSH_INTERVAL,
    system_monitor_interval: float | None = None,
    system_sampler: Callable[[], Mapping[str, float]] | None = None,
    transport: Any = None,
    sweep_trial_id: str | None = None,
) -> Run:
    if mode not in {"online", "offline", "disabled"}:
        raise ValueError("mode must be 'online', 'offline', or 'disabled'")
    if resume not in {"never", "allow", "must"}:
        raise ValueError("resume must be 'never', 'allow', or 'must'")
    if resume == "must" and run_id is None:
        raise ValueError("resume='must' requires an explicit run_id")
    if mode == "disabled" and resume != "never":
        raise ValueError("disabled mode does not support resume policies")
    selected_id = run_id or uuid7()
    selected_spool_root = Path(
        spool_root
        or os.environ.get("RUNLOOM_SPOOL_DIR")
        or Path.home() / ".local" / "share" / "runloom" / "spool"
    )
    selected_monitor_interval = _system_monitor_interval(system_monitor_interval)
    return Run(
        project=project,
        run_id=selected_id,
        name=name,
        config=config or {},
        mode=mode,
        resume=resume,
        server_url=server_url,
        spool_root=selected_spool_root,
        batch_size=batch_size,
        flush_interval=flush_interval,
        system_monitor_interval=selected_monitor_interval,
        system_sampler=system_sampler,
        transport=transport,
        sweep_trial_id=sweep_trial_id,
    )


def sync_spool(
    directory: str | Path,
    *,
    server_url: str | None = None,
    timeout: float = _DEFAULT_FINISH_TIMEOUT,
    transport: Any = None,
) -> str:
    spool_directory = Path(directory)
    if timeout <= 0:
        raise ValueError("sync timeout must be positive")
    metadata_path = spool_directory / "run.json"
    if not metadata_path.is_file():
        raise DeliveryError(f"offline run metadata was not found: {metadata_path}")
    spool = _Spool(spool_directory.parent, spool_directory.name)
    metadata = spool.read_metadata()
    if metadata is None:
        raise DeliveryError(f"offline run metadata was not found: {spool.metadata_path}")
    run_id = spool_directory.name
    project = metadata.get("project")
    if not isinstance(project, str) or not project:
        raise DeliveryError("run spool metadata has no valid project")
    _validate_spool_identity(metadata, project, run_id)
    finished = _metadata_flag(metadata, "finished")
    selected_server = server_url if server_url is not None else metadata.get("server_url")
    if not isinstance(selected_server, str) or not selected_server:
        raise DeliveryError("run spool metadata has no valid server URL")
    client = RunloomClient(selected_server, transport=transport)
    try:
        try:
            client.create_run(
                project=project,
                run_id=run_id,
                name=metadata.get("name"),
                config=_normalize_document(metadata.get("config", {}), "config"),
                resume="allow",
                sweep_trial_id=metadata.get("sweep_trial_id"),
            )
        except RunloomApiError as error:
            if error.status_code != 409 or not finished:
                raise
            existing = client.get_run(run_id)
            expected_summary = _normalize_document(metadata.get("summary", {}), "summary")
            actual_summary = _normalize_document(existing.get("summary", {}), "summary")
            if existing.get("state") != "finished" or any(
                actual_summary.get(key) != value for key, value in expected_summary.items()
            ):
                raise
            return run_id
        worker = _DeliveryWorker(
            client=client,
            run_id=run_id,
            spool=spool,
            batch_size=_validate_batch_size(
                metadata.get("batch_size", _DEFAULT_BATCH_SIZE),
                "stored batch_size",
            ),
            flush_interval=0,
            stop_requested=lambda: None,
        )
        worker.start()
        worker.stop()
        worker.join(timeout)
        if worker.is_alive() or spool.pending():
            worker.cancel()
            worker.join(1)
            message = f"timed out syncing undelivered data in {spool_directory}"
            if worker.last_error is not None:
                message = f"{message}: {worker.last_error}"
            raise DeliveryError(message)
        if finished:
            client.finish_run(
                run_id,
                _normalize_document(metadata.get("summary", {}), "summary"),
            )
        else:
            summary = _normalize_document(metadata.get("summary", {}), "summary")
            if summary:
                client.update_summary(run_id, summary)
    finally:
        client.close()
    return run_id


def _validate_spool_identity(metadata: Mapping[str, Any], project: str, run_id: str) -> None:
    format_version = metadata.get("format_version", _SPOOL_FORMAT_VERSION)
    stored_project = metadata.get("project")
    stored_id = metadata.get("id")
    if (
        isinstance(format_version, bool)
        or not isinstance(format_version, int)
        or not isinstance(stored_project, str)
        or not stored_project
        or not isinstance(stored_id, str)
        or not stored_id
    ):
        raise DeliveryError("run spool metadata is missing its identity")
    if format_version != _SPOOL_FORMAT_VERSION:
        raise DeliveryError(
            f"unsupported spool format version {format_version}; expected {_SPOOL_FORMAT_VERSION}"
        )
    if stored_project != project or stored_id != run_id:
        raise DeliveryError("run spool identity does not match the requested project and run ID")


def _metadata_flag(metadata: Mapping[str, Any], name: str) -> bool:
    value = metadata.get(name, False)
    if not isinstance(value, bool):
        raise DeliveryError(f"run spool metadata field '{name}' must be boolean")
    return value


def _validate_batch_size(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer between 1 and 1024")
    if value < 1 or value > 1_024:
        raise ValueError(f"{name} must be between 1 and 1024")
    return value


def _response_position(response: Mapping[str, Any], name: str, *, minimum: int) -> int:
    value = response.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise DeliveryError(
            f"server create response has no valid {name}; server and SDK versions may differ"
        )
    return value


def _system_monitor_interval(value: float | None) -> float | None:
    selected: float | str = (
        os.environ.get("RUNLOOM_SYSTEM_METRICS_INTERVAL", str(_DEFAULT_SYSTEM_METRIC_INTERVAL))
        if value is None
        else value
    )
    try:
        interval = float(selected)
    except (TypeError, ValueError) as error:
        raise ValueError("system metric interval must be a finite non-negative number") from error
    if not math.isfinite(interval) or interval < 0:
        raise ValueError("system metric interval must be a finite non-negative number")
    return None if interval == 0 else interval


def _validate_step(value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError("step must be an integer")
    if value < 0:
        raise ValueError("step cannot be negative")


def _validate_alert(title: Any, text: Any, level: Any) -> tuple[str, str, str]:
    if not isinstance(title, str):
        raise TypeError("alert title must be a string")
    if not isinstance(text, str):
        raise TypeError("alert text must be a string")
    if not isinstance(level, str):
        raise TypeError("alert level must be a string")
    title_bytes = title.encode("utf-8")
    if (
        not title_bytes
        or len(title_bytes) > _MAX_ALERT_TITLE_BYTES
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in title)
    ):
        raise ValueError(
            f"alert title must contain 1 to {_MAX_ALERT_TITLE_BYTES} non-control bytes"
        )
    if len(text.encode("utf-8")) > _MAX_ALERT_TEXT_BYTES:
        raise ValueError(f"alert text cannot exceed {_MAX_ALERT_TEXT_BYTES} bytes")
    normalized_level = level.lower()
    if normalized_level not in {"info", "warn", "error"}:
        raise ValueError("alert level must be 'info', 'warn', or 'error'")
    return title, text, normalized_level


def _flatten_metrics(data: Mapping[str, Any], prefix: str = "") -> dict[str, float]:
    flattened: dict[str, float] = {}
    for raw_key, value in data.items():
        key = f"{prefix}/{raw_key}" if prefix else str(raw_key)
        if isinstance(value, Mapping):
            flattened.update(_flatten_metrics(value, key))
        elif isinstance(value, bool):
            flattened[key] = float(value)
        elif isinstance(value, (int, float)):
            number = float(value)
            if not math.isfinite(number):
                raise ValueError(f"metric '{key}' must be finite")
            flattened[key] = number
        else:
            raise TypeError(
                f"metric '{key}' has unsupported type {type(value).__name__}; "
                "rich values are not implemented yet"
            )
    return flattened


def _flatten_log_values(
    data: Mapping[str, Any],
    prefix: str = "",
) -> tuple[dict[str, float], list[tuple[str, RichValue]]]:
    if not isinstance(data, Mapping):
        raise TypeError("log data must be a mapping")
    metrics: dict[str, float] = {}
    rich_values: list[tuple[str, RichValue]] = []
    for raw_key, value in data.items():
        key = f"{prefix}/{raw_key}" if prefix else str(raw_key)
        if isinstance(value, Mapping):
            nested_metrics, nested_rich_values = _flatten_log_values(value, key)
            metrics.update(nested_metrics)
            rich_values.extend(nested_rich_values)
        elif isinstance(value, RichValue):
            rich_values.append((key, value))
        elif isinstance(value, bool):
            metrics[key] = float(value)
        elif isinstance(value, (int, float)):
            number = float(value)
            if not math.isfinite(number):
                raise ValueError(f"metric '{key}' must be finite")
            metrics[key] = number
        else:
            raise TypeError(
                f"metric '{key}' has unsupported type {type(value).__name__}; "
                "use a native Runloom rich value"
            )
    return metrics, rich_values


def _validate_rich_key(key: str) -> None:
    encoded = key.encode("utf-8")
    if (
        not encoded
        or len(encoded) > 256
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in key)
    ):
        raise ValueError("rich value key must contain 1 to 256 non-control bytes")


def _collect_document_updates(
    values: Mapping[str, Any] | None,
    kwargs: Mapping[str, Any],
    name: str,
) -> dict[str, Any]:
    if values is not None and not isinstance(values, Mapping):
        raise TypeError(f"{name} updates must be a mapping")
    combined = dict(values or {})
    combined.update(kwargs)
    return _normalize_document(combined, name)


def _normalize_document(values: Mapping[str, Any], name: str) -> dict[str, Any]:
    if not isinstance(values, Mapping):
        raise TypeError(f"{name} must be a mapping")
    normalized: dict[str, Any] = {}
    for key, value in values.items():
        if not isinstance(key, str):
            raise TypeError(f"{name} keys must be strings, got {type(key).__name__}")
        normalized[key] = _normalize_json_value(value, f"{name}.{key}")
    encoded = json.dumps(
        normalized,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded) > _MAX_DOCUMENT_BYTES:
        raise ValueError(f"serialized {name} exceeds {_MAX_DOCUMENT_BYTES} bytes")
    return normalized


def _normalize_json_value(value: Any, path: str) -> Any:
    if value is None or isinstance(value, (str, bool)):
        return value
    if isinstance(value, int):
        if value < -(2**63) or value > 2**64 - 1:
            raise ValueError(f"{path} integer is outside the JSON server range")
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must be finite")
        return value
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        for key, nested in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} keys must be strings, got {type(key).__name__}")
            normalized[key] = _normalize_json_value(nested, f"{path}.{key}")
        return normalized
    if isinstance(value, (list, tuple)):
        return [_normalize_json_value(item, f"{path}[{index}]") for index, item in enumerate(value)]
    raise TypeError(f"{path} has unsupported JSON type {type(value).__name__}")


def _atomic_json_write(path: Path, value: dict[str, Any]) -> None:
    _atomic_text_write(path, json.dumps(value, indent=2, sort_keys=True, allow_nan=False))


def _atomic_text_write(path: Path, value: str) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
