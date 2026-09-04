from __future__ import annotations

import math
import os
import threading
import time
import uuid
import warnings
from collections.abc import Callable, Iterator, Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any, Literal

from epochdeck._ids import uuid7
from epochdeck._json_normalization import normalize_json_object
from epochdeck._limits import MAX_SAFE_INTEGER
from epochdeck._metrics import MAX_METRICS_PER_POINT, normalize_metrics
from epochdeck._protocol import DeliveryError
from epochdeck._spool import _Spool
from epochdeck._summary import MAX_DERIVED_SUMMARY_KEYS, merge_metric_preview
from epochdeck.artifact import Artifact
from epochdeck.client import EpochDeckApiError, EpochDeckClient, _normalize_server_url
from epochdeck.rich import RichValue
from epochdeck.system_metrics import SystemMonitor, SystemSampler
from epochdeck.trace import Trace, TraceKind

Mode = Literal["online", "offline", "disabled"]
Resume = Literal["never", "allow", "must"]

_DEFAULT_BATCH_SIZE = 64
_DEFAULT_FLUSH_INTERVAL = 0.25
_DEFAULT_FINISH_TIMEOUT = 30.0
_MAX_DOCUMENT_BYTES = 256 * 1024
_MAX_DOCUMENT_DEPTH = 64
_MAX_DOCUMENT_NODES = 65_536
_MAX_LOG_VALUE_DEPTH = 64
_MAX_LOG_VALUE_NODES = 65_536
_MAX_RICH_VALUES_PER_LOG = 256
_DEFAULT_SYSTEM_METRIC_INTERVAL = 15.0
_MAX_ALERT_TITLE_BYTES = 256
_MAX_ALERT_TEXT_BYTES = 4_096
_MAX_METRIC_REQUEST_BYTES = 1_750_000
_SUMMARY_CHECKPOINT_RECORD_INTERVAL = 128
_SUMMARY_CHECKPOINT_BYTE_INTERVAL = 512 * 1024
_SUMMARY_RECOVERY_MAX_TAIL_BYTES = _SUMMARY_CHECKPOINT_BYTE_INTERVAL + 2 * 1024 * 1024 + 1


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


class RunSummary(_RunDocument):
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

    def _replace_metric_layer(
        self,
        previous: Mapping[str, float],
        current: Mapping[str, float],
        explicit: Mapping[str, Any],
    ) -> None:
        with self._lock:
            for key in previous.keys() - current.keys():
                if key not in explicit:
                    self._data.pop(key, None)
            for key, value in current.items():
                if key not in explicit:
                    self._data[key] = value


class _DeliveryWorker(threading.Thread):
    def __init__(
        self,
        *,
        client: EpochDeckClient,
        run_id: str,
        spool: _Spool,
        batch_size: int,
        flush_interval: float,
        stop_requested: Callable[[], None],
    ) -> None:
        super().__init__(name=f"epochdeck-{run_id[:8]}", daemon=True)
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
                    points, next_offset = self._spool.read_batch(
                        self._batch_size,
                        request_byte_budget=_MAX_METRIC_REQUEST_BYTES,
                    )
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
        run_id = _canonical_run_id(run_id)
        batch_size = _validate_batch_size(batch_size, "batch_size")
        server_url = _normalize_server_url(server_url)
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
        self._client: EpochDeckClient | None = None
        self._worker: _DeliveryWorker | None = None
        self._spool: _Spool | None = None
        self._finish_callback: Callable[[Run], None] | None = None
        self._system_monitor: SystemMonitor | None = None
        self._system_monitor_interval = system_monitor_interval
        self._system_sampler = system_sampler
        self._sweep_trial_id = sweep_trial_id
        self._stop_requested = threading.Event()
        self._summary_event_offset = 0
        self._latest_event_offset = 0
        self._summary_tail_records = 0
        self._explicit_summary: dict[str, Any] = {}
        self._metric_summary: dict[str, float] = {}
        self._summary_truncated = False
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
        stored_explicit_summary: dict[str, Any] = {}
        stored_metric_summary: dict[str, float] = {}
        stored_summary_truncated = False
        stored_summary_event_offset = 0
        if stored_metadata is not None:
            _validate_spool_identity(stored_metadata, project, run_id)
            (
                stored_explicit_summary,
                stored_metric_summary,
                stored_summary_truncated,
                stored_summary_event_offset,
            ) = _stored_summary_snapshot(stored_metadata)
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
        (
            recovered_metric_summary,
            recovered_summary_truncated,
            recovered_event_offset,
        ) = self._spool.recover_summary(
            stored_metric_summary,
            stored_summary_truncated,
            stored_summary_event_offset,
            max_tail_records=_SUMMARY_CHECKPOINT_RECORD_INTERVAL,
            max_tail_bytes=_SUMMARY_RECOVERY_MAX_TAIL_BYTES,
        )
        self._explicit_summary = stored_explicit_summary
        self._metric_summary = recovered_metric_summary
        self._summary_truncated = recovered_summary_truncated
        self._refresh_summary_view()
        self._summary_event_offset = recovered_event_offset
        self._latest_event_offset = recovered_event_offset
        metadata = {
            "project": project,
            "id": run_id,
            "name": self.name,
            "config": self.config.to_dict(),
            "explicit_summary": deepcopy(self._explicit_summary),
            "metric_summary": deepcopy(self._metric_summary),
            "summary_truncated": self._summary_truncated,
            "summary_event_offset": self._summary_event_offset,
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

        self._client = EpochDeckClient(server_url, transport=transport)
        try:
            response = self._client.create_run(
                project=project,
                run_id=run_id,
                name=self.name,
                config=self.config.to_dict(),
                resume=resume,
                sweep_trial_id=sweep_trial_id,
            )
        except EpochDeckApiError as error:
            try:
                if (
                    stored_metadata is not None
                    and stored_finishing
                    and error.status_code == 409
                    and not self._spool.pending()
                ):
                    existing = self._client.get_run(run_id)
                    (
                        actual_explicit,
                        actual_metric,
                        actual_truncated,
                    ) = _server_summary_components(existing)
                    actual_summary = _summary_view(actual_metric, actual_explicit)
                    expected_summary = self.summary.to_dict()
                    if existing.get("state") == "finished" and all(
                        actual_summary.get(key) == value for key, value in expected_summary.items()
                    ):
                        self.name = str(existing["name"])
                        self.config._replace(
                            _normalize_document(existing.get("config", {}), "config")
                        )
                        self._explicit_summary = actual_explicit
                        self._metric_summary = actual_metric
                        self._summary_truncated = actual_truncated
                        self._refresh_summary_view()
                        self._spool.update_metadata(
                            {
                                "name": self.name,
                                "config": self.config.to_dict(),
                                "explicit_summary": deepcopy(self._explicit_summary),
                                "metric_summary": deepcopy(self._metric_summary),
                                "summary_truncated": self._summary_truncated,
                                "summary_event_offset": self._latest_event_offset,
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
            server_next_sequence = _response_position(response, "next_sequence", minimum=1)
            server_next_step = _response_position(response, "next_step", minimum=0)
            (
                server_explicit,
                server_metric,
                server_truncated,
            ) = _server_summary_components(server_run)
            self._explicit_summary = _normalize_document(
                {**server_explicit, **self._explicit_summary},
                "explicit summary",
            )
            self._metric_summary, self._summary_truncated = merge_metric_preview(
                server_metric,
                self._metric_summary,
                truncated=server_truncated or self._summary_truncated,
            )
            self._refresh_summary_view()
            self._next_sequence = max(self._next_sequence, server_next_sequence)
            self._next_step = max(self._next_step, server_next_step)
            if self._next_step > 0:
                self._last_user_step = self._next_step - 1
            self._spool.update_metadata(
                {
                    "name": self.name,
                    "config": self.config.to_dict(),
                    "explicit_summary": deepcopy(self._explicit_summary),
                    "metric_summary": deepcopy(self._metric_summary),
                    "summary_truncated": self._summary_truncated,
                    "summary_event_offset": self._latest_event_offset,
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

    def __exit__(self, exception_type: object, exception: object, traceback: object) -> None:
        if exception is None:
            self.finish()
            return
        try:
            self.finish()
        except Exception as cleanup_error:
            add_note = getattr(exception, "add_note", None)
            if callable(add_note):
                add_note(
                    "EpochDeck also failed while finishing the run: "
                    f"{type(cleanup_error).__name__}: {cleanup_error}"
                )

    @property
    def finished(self) -> bool:
        return self._finished

    @property
    def system_monitor_error(self) -> Exception | None:
        return self._system_monitor.last_error if self._system_monitor is not None else None

    @property
    def should_stop(self) -> bool:
        return self._stop_requested.is_set()

    @property
    def summary_truncated(self) -> bool:
        return self._summary_truncated

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
            merged = _normalize_document(
                {**self._explicit_summary, **updates},
                "explicit summary",
            )
            self._explicit_summary = merged
            self._refresh_summary_view()
            self._checkpoint_summary()
            if self.mode == "online":
                assert self._client is not None
                response = self._client.update_summary(self.id, updates)
                server_explicit, server_metric, server_truncated = _server_summary_components(
                    response["run"]
                )
                self._explicit_summary = _normalize_document(
                    {**server_explicit, **merged},
                    "explicit summary",
                )
                self._metric_summary, self._summary_truncated = merge_metric_preview(
                    server_metric,
                    self._metric_summary,
                    truncated=server_truncated or self._summary_truncated,
                )
                self._refresh_summary_view()
            self._checkpoint_summary()

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
            if metrics:
                metrics = normalize_metrics(metrics)
            selected_step = self._next_step if step is None else step
            _validate_step(selected_step)
            prepared_values = []
            if rich_values and self.mode != "disabled":
                assert self._spool is not None
                for key, value in rich_values:
                    _validate_rich_key(key)
                    prepared_values.append((key, value._prepare(self._spool.blob_root)))
            timestamp_ms = time.time_ns() // 1_000_000
            rich_records = [
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
                for key, prepared in prepared_values
            ]
            if metrics:
                self._append_metrics(metrics, selected_step, advance_step=False, summarize=True)
            if rich_records:
                assert self._spool is not None
                for record in rich_records:
                    self._spool.append_rich_value(record)
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
            raise TypeError("log_artifact expects an epochdeck.Artifact")
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
        normalized_metrics = normalize_metrics(metrics)
        if self._next_sequence > MAX_SAFE_INTEGER:
            raise ValueError(f"metric sequence cannot exceed {MAX_SAFE_INTEGER}")
        if self._spool is not None and self._summary_checkpoint_due():
            self._checkpoint_summary()
        point = {
            "sequence": self._next_sequence,
            "step": step,
            "timestamp_ms": time.time_ns() // 1_000_000,
            "metrics": normalized_metrics,
        }
        if self._spool is not None:
            self._latest_event_offset = self._spool.append(point)
            self._summary_tail_records += 1
        if summarize:
            previous_metric_summary = self._metric_summary
            self._metric_summary, self._summary_truncated = merge_metric_preview(
                self._metric_summary,
                normalized_metrics,
                truncated=self._summary_truncated,
            )
            self.summary._replace_metric_layer(
                previous_metric_summary,
                self._metric_summary,
                self._explicit_summary,
            )
        self._next_sequence += 1
        if self._spool is not None and self._summary_checkpoint_due():
            self._checkpoint_summary()
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
            finish_summary = _normalize_document(summary or {}, "explicit summary")
            self._explicit_summary = _normalize_document(
                {**self._explicit_summary, **finish_summary},
                "explicit summary",
            )
            self._refresh_summary_view()
            self._finishing = True
            if self._spool is not None:
                self._checkpoint_summary({"finishing": True})
        deadline = time.monotonic() + timeout
        self._stop_system_monitor(timeout)
        if self.mode == "disabled":
            self._complete()
            return
        assert self._spool is not None
        if self.mode == "offline":
            self._spool.update_metadata({"finished": True, "finishing": False})
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
        response = self._client.finish_run(self.id, self._explicit_summary)
        (
            self._explicit_summary,
            self._metric_summary,
            self._summary_truncated,
        ) = _server_summary_components(response["run"])
        self._refresh_summary_view()
        self._checkpoint_summary({"finished": True, "finishing": False})
        self._client.close()
        try:
            self._spool.reclaim_delivered()
            self._summary_event_offset = 0
            self._latest_event_offset = 0
            self._spool.update_metadata({"summary_event_offset": 0})
        except OSError as error:
            warnings.warn(
                f"EpochDeck could not reclaim the acknowledged spool: {error}",
                RuntimeWarning,
                stacklevel=2,
            )
        self._complete()

    def _checkpoint_summary(self, updates: Mapping[str, Any] | None = None) -> None:
        if self._spool is None:
            return
        metadata_updates: dict[str, Any] = {
            "explicit_summary": deepcopy(self._explicit_summary),
            "metric_summary": deepcopy(self._metric_summary),
            "summary_truncated": self._summary_truncated,
            "summary_event_offset": self._latest_event_offset,
        }
        if updates is not None:
            metadata_updates.update(dict(updates))
        self._spool.update_metadata(metadata_updates)
        self._summary_event_offset = self._latest_event_offset
        self._summary_tail_records = 0

    def _summary_checkpoint_due(self) -> bool:
        return (
            self._summary_tail_records >= _SUMMARY_CHECKPOINT_RECORD_INTERVAL
            or self._latest_event_offset - self._summary_event_offset
            >= _SUMMARY_CHECKPOINT_BYTE_INTERVAL
        )

    def _refresh_summary_view(self) -> None:
        self.summary._replace(_summary_view(self._metric_summary, self._explicit_summary))


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
    selected_id = _canonical_run_id(run_id) if run_id is not None else uuid7()
    selected_spool_root = Path(
        spool_root
        or os.environ.get("EPOCHDECK_SPOOL_DIR")
        or Path.home() / ".local" / "share" / "epochdeck" / "spool"
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
    spool_directory = Path(directory).expanduser().resolve()
    if timeout <= 0:
        raise ValueError("sync timeout must be positive")
    raw_run_id = spool_directory.name
    try:
        run_id = _canonical_run_id(raw_run_id)
    except (TypeError, ValueError) as error:
        raise DeliveryError("spool directory name must be a canonical UUID") from error
    if run_id != raw_run_id:
        raise DeliveryError("spool directory name must be a canonical UUID")
    metadata_path = spool_directory / "run.json"
    if not metadata_path.is_file():
        raise DeliveryError(f"offline run metadata was not found: {metadata_path}")
    spool = _Spool(spool_directory.parent, spool_directory.name)
    metadata = spool.read_metadata()
    if metadata is None:
        raise DeliveryError(f"offline run metadata was not found: {spool.metadata_path}")
    project = metadata.get("project")
    if not isinstance(project, str) or not project:
        raise DeliveryError("run spool metadata has no valid project")
    _validate_spool_identity(metadata, project, run_id)
    (
        explicit_summary,
        metric_summary,
        summary_truncated,
        summary_event_offset,
    ) = _stored_summary_snapshot(metadata)
    metric_summary, summary_truncated, _ = spool.recover_summary(
        metric_summary,
        summary_truncated,
        summary_event_offset,
        max_tail_records=_SUMMARY_CHECKPOINT_RECORD_INTERVAL,
        max_tail_bytes=_SUMMARY_RECOVERY_MAX_TAIL_BYTES,
    )
    expected_summary = _summary_view(metric_summary, explicit_summary)
    finished = _metadata_flag(metadata, "finished")
    if not finished:
        raise DeliveryError("sync requires an offline run that has been finished")
    selected_server = server_url if server_url is not None else metadata.get("server_url")
    if not isinstance(selected_server, str) or not selected_server:
        raise DeliveryError("run spool metadata has no valid server URL")
    client = EpochDeckClient(selected_server, transport=transport)
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
        except EpochDeckApiError as error:
            if error.status_code != 409 or not finished:
                raise
            existing = client.get_run(run_id)
            actual_explicit, actual_metric, _ = _server_summary_components(existing)
            actual_summary = _summary_view(actual_metric, actual_explicit)
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
        client.finish_run(
            run_id,
            explicit_summary,
        )
    finally:
        client.close()
    return run_id


def _validate_spool_identity(metadata: Mapping[str, Any], project: str, run_id: str) -> None:
    stored_project = metadata.get("project")
    stored_id = metadata.get("id")
    if (
        not isinstance(stored_project, str)
        or not stored_project
        or not isinstance(stored_id, str)
        or not stored_id
    ):
        raise DeliveryError("run spool metadata is missing its identity")
    if stored_project != project or stored_id != run_id:
        raise DeliveryError("run spool identity does not match the requested project and run ID")


def _metadata_flag(metadata: Mapping[str, Any], name: str) -> bool:
    value = metadata.get(name)
    if not isinstance(value, bool):
        raise DeliveryError(f"run spool metadata field '{name}' must be boolean")
    return value


def _stored_summary_snapshot(
    metadata: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, float], bool, Any]:
    explicit = metadata.get("explicit_summary")
    metric = metadata.get("metric_summary")
    truncated = metadata.get("summary_truncated")
    if not isinstance(explicit, Mapping):
        raise DeliveryError("run spool metadata has no explicit summary object")
    if not isinstance(metric, Mapping):
        raise DeliveryError("run spool metadata has no metric summary object")
    if not isinstance(truncated, bool):
        raise DeliveryError("run spool metadata has no boolean summary truncation flag")
    try:
        normalized_explicit = _normalize_document(explicit, "explicit summary")
        normalized_metric = _normalize_metric_summary(metric, "stored metric summary")
    except (TypeError, ValueError) as error:
        raise DeliveryError(f"invalid run spool summary snapshot: {error}") from error
    if truncated and len(normalized_metric) != MAX_DERIVED_SUMMARY_KEYS:
        raise DeliveryError("truncated run spool metric summary is incomplete")
    return (
        normalized_explicit,
        normalized_metric,
        truncated,
        metadata.get("summary_event_offset"),
    )


def _server_summary_components(
    run: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, float], bool]:
    explicit = run.get("explicit_summary")
    metric = run.get("metric_summary")
    merged = run.get("summary")
    truncated = run.get("summary_truncated")
    if not isinstance(explicit, Mapping) or not isinstance(metric, Mapping):
        raise DeliveryError("server run response has no summary components")
    if not isinstance(merged, Mapping) or not isinstance(truncated, bool):
        raise DeliveryError("server run response has an invalid summary view")
    try:
        normalized_explicit = _normalize_document(explicit, "server explicit summary")
        normalized_metric = _normalize_metric_summary(metric, "server metric summary")
        normalized_merged = _normalize_document(
            merged,
            "server summary view",
            maximum=2 * _MAX_DOCUMENT_BYTES,
        )
    except (TypeError, ValueError) as error:
        raise DeliveryError(f"server run response has invalid summary data: {error}") from error
    if truncated and len(normalized_metric) != MAX_DERIVED_SUMMARY_KEYS:
        raise DeliveryError("server run response has an incomplete truncated metric summary")
    if normalized_merged != _summary_view(normalized_metric, normalized_explicit):
        raise DeliveryError("server run response summary does not match its components")
    return normalized_explicit, normalized_metric, truncated


def _normalize_metric_summary(values: Mapping[str, Any], name: str) -> dict[str, float]:
    if len(values) > MAX_DERIVED_SUMMARY_KEYS:
        raise ValueError(f"{name} cannot contain more than {MAX_DERIVED_SUMMARY_KEYS} keys")
    if not values:
        return {}
    normalized = normalize_metrics(values)
    if any(key.startswith("system/") for key in values):
        raise ValueError(f"{name} cannot contain system metrics")
    return normalized


def _summary_view(
    metric_summary: Mapping[str, float],
    explicit_summary: Mapping[str, Any],
) -> dict[str, Any]:
    return _normalize_document(
        {**metric_summary, **explicit_summary},
        "summary view",
        maximum=2 * _MAX_DOCUMENT_BYTES,
    )


def _validate_batch_size(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer between 1 and 1024")
    if value < 1 or value > 1_024:
        raise ValueError(f"{name} must be between 1 and 1024")
    return value


def _canonical_run_id(value: Any) -> str:
    if not isinstance(value, str):
        raise TypeError("run_id must be a UUID string")
    try:
        return str(uuid.UUID(value))
    except ValueError as error:
        raise ValueError("run_id must be a valid UUID") from error


def _response_position(response: Mapping[str, Any], name: str, *, minimum: int) -> int:
    value = response.get(name)
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < minimum
        or value > MAX_SAFE_INTEGER
    ):
        raise DeliveryError(
            f"server create response has no valid {name}; server and SDK versions may differ"
        )
    return value


def _system_monitor_interval(value: float | None) -> float | None:
    selected: float | str = (
        os.environ.get("EPOCHDECK_SYSTEM_METRICS_INTERVAL", str(_DEFAULT_SYSTEM_METRIC_INTERVAL))
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
    if value < 0 or value > MAX_SAFE_INTEGER:
        raise ValueError(f"step must be between 0 and {MAX_SAFE_INTEGER}")


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


def _flatten_metrics(data: Mapping[str, Any]) -> dict[str, float]:
    metrics, _ = _flatten_values(data, allow_rich=False)
    return metrics


def _flatten_log_values(
    data: Mapping[str, Any],
) -> tuple[dict[str, float], list[tuple[str, RichValue]]]:
    return _flatten_values(data, allow_rich=True)


def _flatten_values(
    data: Mapping[str, Any],
    *,
    allow_rich: bool,
) -> tuple[dict[str, float], list[tuple[str, RichValue]]]:
    if not isinstance(data, Mapping):
        raise TypeError("log data must be a mapping")
    metrics: dict[str, float] = {}
    rich_values: list[tuple[str, RichValue]] = []
    seen_keys: set[str] = set()
    visited_nodes = 1
    stack: list[tuple[Iterator[tuple[Any, Any]], str, int]] = [(iter(data.items()), "", 0)]
    while stack:
        iterator, prefix, depth = stack[-1]
        try:
            raw_key, value = next(iterator)
        except StopIteration:
            stack.pop()
            continue
        visited_nodes += 1
        if visited_nodes > _MAX_LOG_VALUE_NODES:
            raise ValueError(f"log data cannot exceed {_MAX_LOG_VALUE_NODES} traversed value nodes")
        if not isinstance(raw_key, str):
            raise TypeError(f"log keys must be strings, got {type(raw_key).__name__}")
        key = f"{prefix}/{raw_key}" if prefix else raw_key
        _validate_flattened_log_key(key)
        if isinstance(value, Mapping):
            if depth >= _MAX_LOG_VALUE_DEPTH:
                raise ValueError(f"log data nesting exceeds {_MAX_LOG_VALUE_DEPTH} levels at {key}")
            stack.append((iter(value.items()), key, depth + 1))
        elif isinstance(value, RichValue):
            if not allow_rich:
                raise TypeError(f"system metric '{key}' cannot contain a rich value")
            _reserve_flattened_key(key, seen_keys)
            if len(rich_values) >= _MAX_RICH_VALUES_PER_LOG:
                raise ValueError(
                    f"one log call cannot contain more than {_MAX_RICH_VALUES_PER_LOG} rich values"
                )
            rich_values.append((key, value))
        elif isinstance(value, bool):
            _reserve_flattened_key(key, seen_keys)
            if len(metrics) >= MAX_METRICS_PER_POINT:
                raise ValueError(
                    f"a metric point must contain 1 to {MAX_METRICS_PER_POINT} metrics"
                )
            metrics[key] = float(value)
        elif isinstance(value, (int, float)):
            _reserve_flattened_key(key, seen_keys)
            if len(metrics) >= MAX_METRICS_PER_POINT:
                raise ValueError(
                    f"a metric point must contain 1 to {MAX_METRICS_PER_POINT} metrics"
                )
            metrics[key] = _finite_metric_number(value, key)
        else:
            raise TypeError(
                f"metric '{key}' has unsupported type {type(value).__name__}; "
                "use a native EpochDeck rich value"
            )
    return metrics, rich_values


def _validate_flattened_log_key(key: str) -> None:
    encoded = key.encode("utf-8")
    if (
        not encoded
        or len(encoded) > 256
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in key)
    ):
        raise ValueError("flattened log keys must contain 1 to 256 non-control bytes")


def _reserve_flattened_key(key: str, seen: set[str]) -> None:
    if key in seen:
        raise ValueError(f"flattened log key collision: {key!r}")
    seen.add(key)


def _finite_metric_number(value: int | float, key: str) -> float:
    try:
        number = float(value)
    except OverflowError as error:
        raise ValueError(f"metric '{key}' must be finite") from error
    if not math.isfinite(number):
        raise ValueError(f"metric '{key}' must be finite")
    return number


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
    combined = _normalize_document(values or {}, name)
    combined.update(kwargs)
    return _normalize_document(combined, name)


def _normalize_document(
    values: Mapping[str, Any],
    name: str,
    *,
    maximum: int = _MAX_DOCUMENT_BYTES,
) -> dict[str, Any]:
    return normalize_json_object(
        values,
        name,
        maximum,
        maximum_depth=_MAX_DOCUMENT_DEPTH,
        maximum_nodes=_MAX_DOCUMENT_NODES,
    )
