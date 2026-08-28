from __future__ import annotations

import json
import math
import os
import threading
import time
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from runloom.client import RunloomClient

Mode = Literal["online", "offline", "disabled"]
Resume = Literal["never", "allow", "must"]

_DEFAULT_BATCH_SIZE = 64
_DEFAULT_FLUSH_INTERVAL = 0.25
_DEFAULT_FINISH_TIMEOUT = 30.0


class DeliveryError(RuntimeError):
    pass


class _Spool:
    def __init__(self, root: Path, run_id: str) -> None:
        self.directory = root / run_id
        self.directory.mkdir(parents=True, exist_ok=True)
        self.events_path = self.directory / "events.jsonl"
        self.ack_path = self.directory / "ack"
        self.metadata_path = self.directory / "run.json"
        self._lock = threading.Lock()
        self.events_path.touch(exist_ok=True)

    def write_metadata(self, metadata: dict[str, Any]) -> None:
        _atomic_json_write(self.metadata_path, metadata)

    def append(self, point: dict[str, Any]) -> None:
        encoded = json.dumps(point, separators=(",", ":"), sort_keys=True, allow_nan=False)
        with self._lock, self.events_path.open("a", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())

    def read_batch(self, limit: int) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            offset = self._read_ack()
            points: list[dict[str, Any]] = []
            with self.events_path.open("rb") as stream:
                stream.seek(offset)
                while len(points) < limit:
                    line = stream.readline()
                    if not line:
                        break
                    points.append(json.loads(line))
                next_offset = stream.tell()
            return points, next_offset

    def acknowledge(self, offset: int) -> None:
        with self._lock:
            _atomic_text_write(self.ack_path, str(offset))

    def pending(self) -> bool:
        with self._lock:
            return self._read_ack() < self.events_path.stat().st_size

    def last_point(self) -> dict[str, Any] | None:
        with self._lock, self.events_path.open("rb") as stream:
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
        return json.loads(line) if line.strip() else None

    def _read_ack(self) -> int:
        if not self.ack_path.exists():
            return 0
        try:
            offset = int(self.ack_path.read_text(encoding="ascii"))
        except (OSError, ValueError) as error:
            raise DeliveryError(f"invalid spool acknowledgement: {self.ack_path}") from error
        size = self.events_path.stat().st_size
        if offset < 0 or offset > size:
            raise DeliveryError(f"spool acknowledgement is outside journal: {self.ack_path}")
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
    ) -> None:
        super().__init__(name=f"runloom-{run_id[:8]}", daemon=True)
        self._client = client
        self._run_id = run_id
        self._spool = spool
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._wake = threading.Event()
        self._stopping = threading.Event()
        self._cancelled = threading.Event()
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
            points, next_offset = self._spool.read_batch(self._batch_size)
            if not points:
                continue
            request = {"batch_sequence": points[0]["sequence"], "points": points}
            try:
                self._client.ingest_batch(self._run_id, request)
            except Exception as error:  # The durable journal remains authoritative.
                self.last_error = error
                self._wake.wait(retry_delay)
                self._wake.clear()
                if self._cancelled.is_set():
                    return
                retry_delay = min(retry_delay * 2, 5.0)
            else:
                self._spool.acknowledge(next_offset)
                self.last_error = None
                retry_delay = 0.25


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
        transport: Any = None,
    ) -> None:
        if batch_size < 1 or batch_size > 1_024:
            raise ValueError("batch_size must be between 1 and 1024")
        if flush_interval < 0:
            raise ValueError("flush_interval cannot be negative")
        self.project = project
        self.id = run_id
        self.name = name
        self.config = dict(config)
        self.mode = mode
        self.resume = resume
        self.server_url = server_url
        self._batch_size = batch_size
        self._flush_interval = flush_interval
        self._finished = False
        self._finishing = False
        self._log_lock = threading.Lock()
        self._client: RunloomClient | None = None
        self._worker: _DeliveryWorker | None = None
        self._spool: _Spool | None = None

        if mode == "disabled":
            self._next_sequence = 1
            self._next_step = 0
            return

        self._spool = _Spool(spool_root, run_id)
        last_point = self._spool.last_point()
        self._next_sequence = int(last_point["sequence"]) + 1 if last_point else 1
        self._next_step = int(last_point["step"]) + 1 if last_point else 0
        metadata = {
            "project": project,
            "id": run_id,
            "name": name,
            "config": self.config,
            "resume": resume,
            "server_url": server_url,
            "batch_size": batch_size,
            "finished": False,
        }
        self._spool.write_metadata(metadata)

        if mode == "offline":
            return

        self._client = RunloomClient(server_url, transport=transport)
        try:
            response = self._client.create_run(
                project=project,
                run_id=run_id,
                name=name,
                config=self.config,
                resume=resume,
            )
        except Exception:
            self._client.close()
            raise
        server_run = response["run"]
        self.name = str(server_run["name"])
        self._worker = _DeliveryWorker(
            client=self._client,
            run_id=run_id,
            spool=self._spool,
            batch_size=batch_size,
            flush_interval=flush_interval,
        )
        self._worker.start()
        if self._spool.pending():
            self._worker.notify()

    def __enter__(self) -> Run:
        return self

    def __exit__(self, *_: object) -> None:
        self.finish()

    @property
    def finished(self) -> bool:
        return self._finished

    def log(self, data: Mapping[str, Any], *, step: int | None = None) -> None:
        with self._log_lock:
            if self._finished or self._finishing:
                raise RuntimeError("cannot log while a run is finishing or finished")
            metrics = _flatten_metrics(data)
            if not metrics:
                raise ValueError("log data contains no numeric scalar metrics")
            selected_step = self._next_step if step is None else step
            if selected_step < 0:
                raise ValueError("step cannot be negative")
            point = {
                "sequence": self._next_sequence,
                "step": selected_step,
                "timestamp_ms": time.time_ns() // 1_000_000,
                "metrics": metrics,
            }
            if self._spool is not None:
                self._spool.append(point)
            self._next_sequence += 1
            self._next_step = selected_step + 1
        if self._worker is not None:
            self._worker.notify()

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
            self._finishing = True
        if self.mode == "disabled":
            self._finished = True
            self._finishing = False
            return
        assert self._spool is not None
        if self.mode == "offline":
            metadata = json.loads(self._spool.metadata_path.read_text(encoding="utf-8"))
            metadata["finished"] = True
            metadata["summary"] = dict(summary or {})
            self._spool.write_metadata(metadata)
            self._finished = True
            self._finishing = False
            return

        assert self._worker is not None
        assert self._client is not None
        self._worker.stop()
        self._worker.join(timeout)
        if self._worker.is_alive() or self._spool.pending():
            error = self._worker.last_error
            message = f"timed out with undelivered data in {self._spool.directory}"
            if error is not None:
                message = f"{message}: {error}"
            raise DeliveryError(message)
        self._client.finish_run(self.id, dict(summary or {}))
        self._client.close()
        self._finished = True
        self._finishing = False


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
    transport: Any = None,
) -> Run:
    if mode not in {"online", "offline", "disabled"}:
        raise ValueError("mode must be 'online', 'offline', or 'disabled'")
    if resume not in {"never", "allow", "must"}:
        raise ValueError("resume must be 'never', 'allow', or 'must'")
    if resume == "must" and run_id is None:
        raise ValueError("resume='must' requires an explicit run_id")
    selected_id = run_id or str(uuid.uuid4())
    selected_spool_root = Path(
        spool_root
        or os.environ.get("RUNLOOM_SPOOL_DIR")
        or Path.home() / ".local" / "share" / "runloom" / "spool"
    )
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
        transport=transport,
    )


def sync_spool(
    directory: str | Path,
    *,
    server_url: str | None = None,
    timeout: float = _DEFAULT_FINISH_TIMEOUT,
    transport: Any = None,
) -> str:
    spool_directory = Path(directory)
    metadata_path = spool_directory / "run.json"
    if not metadata_path.is_file():
        raise DeliveryError(f"offline run metadata was not found: {metadata_path}")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    run_id = str(metadata["id"])
    if spool_directory.name != run_id:
        raise DeliveryError("spool directory name does not match its run ID")
    selected_server = server_url or str(metadata["server_url"])
    client = RunloomClient(selected_server, transport=transport)
    spool = _Spool(spool_directory.parent, run_id)
    try:
        client.create_run(
            project=str(metadata["project"]),
            run_id=run_id,
            name=metadata.get("name"),
            config=dict(metadata.get("config", {})),
            resume="allow",
        )
        worker = _DeliveryWorker(
            client=client,
            run_id=run_id,
            spool=spool,
            batch_size=int(metadata.get("batch_size", _DEFAULT_BATCH_SIZE)),
            flush_interval=0,
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
        if bool(metadata.get("finished")):
            client.finish_run(run_id, dict(metadata.get("summary", {})))
    finally:
        client.close()
    return run_id


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


def _atomic_json_write(path: Path, value: dict[str, Any]) -> None:
    _atomic_text_write(path, json.dumps(value, indent=2, sort_keys=True, allow_nan=False))


def _atomic_text_write(path: Path, value: str) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        stream.write(value)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
