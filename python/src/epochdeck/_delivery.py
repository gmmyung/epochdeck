"""Background delivery of the bounded, durable spool records."""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from epochdeck._protocol import DeliveryError
from epochdeck._spool import _Spool
from epochdeck.client import EpochDeckClient

_MAX_METRIC_REQUEST_BYTES = 1_750_000


class _DeliveryWorker(threading.Thread):
    def __init__(
        self,
        *,
        client: EpochDeckClient,
        run_id: str,
        spool: _Spool,
        batch_size: int,
        flush_interval: float,
    ) -> None:
        super().__init__(name=f"epochdeck-{run_id[:8]}", daemon=True)
        self._client = client
        self._run_id = run_id
        self._spool = spool
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._wake = threading.Event()
        self._stopping = threading.Event()
        self._cancelled = threading.Event()
        self._exit_lock = threading.Lock()
        self._exited = False
        self._delivery_cursor = 0
        self.last_error: Exception | None = None

    def notify(self) -> None:
        self._wake.set()

    def stop(self) -> None:
        self._stopping.set()
        self._wake.set()

    def cancel(self) -> None:
        with self._exit_lock:
            self._cancelled.set()
            if self._exited:
                self._spool.close()
        self._wake.set()

    def run(self) -> None:
        try:
            self._deliver_until_stopped()
        finally:
            # A timed-out caller cannot release ownership while HTTP delivery is active.
            with self._exit_lock:
                self._exited = True
                if self._cancelled.is_set():
                    self._spool.close()

    def _deliver_until_stopped(self) -> None:
        retry_delay = 0.25
        while True:
            if self._cancelled.is_set():
                return
            try:
                pending = self._spool.pending()
            except Exception as error:
                self.last_error = error
                self._wake.wait(retry_delay)
                self._wake.clear()
                retry_delay = min(retry_delay * 2, 5.0)
                continue
            if not pending:
                if self._stopping.is_set():
                    return
                self._wake.wait()
                self._wake.clear()
                if not self._stopping.is_set():
                    self._wake.wait(self._flush_interval)
                    self._wake.clear()
                if self._cancelled.is_set():
                    return
            try:
                delivery = self._next_delivery()
                if delivery is None:
                    continue
                delivery()
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

    def _deliver_alert(self) -> None:
        alert, next_offset = self._spool.read_alert()
        if alert is None:
            return
        self._client.create_alert(self._run_id, alert)
        self._spool.acknowledge_alert(next_offset)

    def _deliver_rich_value(self) -> None:
        value, next_offset = self._spool.read_rich_value()
        if value is None:
            return
        blob = value.get("blob")
        if blob is not None:
            self._upload_blob(blob)
        self._client.create_rich_value(self._run_id, value)
        self._spool.acknowledge_rich_value(next_offset)

    def _deliver_artifact(self) -> None:
        artifact, next_offset = self._spool.read_artifact()
        if artifact is None:
            return
        operation = artifact.pop("operation", None)
        if operation == "create":
            for entry in artifact["entries"]:
                self._upload_blob(entry["blob"])
            self._client.create_artifact(self._run_id, artifact)
        elif operation == "use":
            self._client.use_artifact(self._run_id, str(artifact["artifact_id"]))
        else:
            raise DeliveryError("artifact journal has an unknown operation")
        self._spool.acknowledge_artifact(next_offset)

    def _deliver_metrics(self) -> None:
        points, next_offset = self._spool.read_batch(
            self._batch_size,
            request_byte_budget=_MAX_METRIC_REQUEST_BYTES,
        )
        if not points:
            return
        request = {"batch_sequence": points[0]["sequence"], "points": points}
        self._client.ingest_batch(self._run_id, request)
        self._spool.acknowledge(next_offset)

    def _upload_blob(self, blob: dict[str, Any]) -> None:
        self._client.upload_blob(
            self._spool.blob_path(str(blob["digest"])),
            blob,
        )

    def _next_delivery(self) -> Callable[[], None] | None:
        deliveries = (
            (self._spool.pending_metrics, self._deliver_metrics),
            (self._spool.pending_rich_values, self._deliver_rich_value),
            (self._spool.pending_artifacts, self._deliver_artifact),
            (self._spool.pending_alerts, self._deliver_alert),
        )
        for offset in range(len(deliveries)):
            index = (self._delivery_cursor + offset) % len(deliveries)
            pending, deliver = deliveries[index]
            if pending():
                self._delivery_cursor = (index + 1) % len(deliveries)
                return deliver
        return None
