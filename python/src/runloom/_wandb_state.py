from __future__ import annotations

import fcntl
import json
import os
import threading
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any

_CHECKPOINT_VERSION = 2
_MAX_IMPORT_RUNS = 100_000
_MAX_CHECKPOINT_BYTES = 64 * 1024 * 1024
_CHECKPOINT_COMPACT_BYTES = 8 * 1024 * 1024
_CHECKPOINT_COMPACT_UPDATES = 10_000


class WandbImportError(RuntimeError):
    pass


class ImportCancelled(RuntimeError):
    pass


class ImportCancellation:
    def __init__(self) -> None:
        self._event = threading.Event()

    def cancel(self) -> None:
        self._event.set()

    @property
    def cancelled(self) -> bool:
        return self._event.is_set()

    def check(self) -> None:
        if self._event.is_set():
            raise ImportCancelled("W&B import was cancelled")

    def wait(self, timeout: float) -> None:
        if self._event.wait(timeout):
            raise ImportCancelled("W&B import was cancelled")


@contextmanager
def checkpoint_process_lock(path: Path) -> Iterator[None]:
    checkpoint = path.expanduser().resolve()
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    lock_path = checkpoint.with_name(f"{checkpoint.name}.lock")
    try:
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        os.fchmod(descriptor, 0o600)
    except OSError as error:
        raise WandbImportError(f"cannot open W&B import checkpoint lock: {lock_path}") from error
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise WandbImportError(
                f"another W&B import is using checkpoint {checkpoint}"
            ) from error
        try:
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
    finally:
        os.close(descriptor)


class Checkpoint:
    def __init__(
        self,
        path: Path,
        *,
        entity: str,
        project: str,
        target_project: str,
    ) -> None:
        self.path = path.expanduser().resolve()
        self._lock = threading.RLock()
        self._updates_since_compaction = 0
        if self.path.exists():
            if self.path.stat().st_size > _MAX_CHECKPOINT_BYTES:
                raise WandbImportError("checkpoint exceeds the 64 MiB safety limit")
            data = self._load()
        else:
            data = {
                "format_version": _CHECKPOINT_VERSION,
                "source": "wandb",
                "entity": entity,
                "project": project,
                "target_project": target_project,
                "runs": {},
            }
        if not isinstance(data, dict) or data.get("format_version") != _CHECKPOINT_VERSION:
            raise WandbImportError("unsupported W&B import checkpoint")
        expected = (entity, project, target_project)
        actual = (data.get("entity"), data.get("project"), data.get("target_project"))
        if actual != expected:
            raise WandbImportError("checkpoint source or target does not match this import")
        runs = data.get("runs")
        if not isinstance(runs, dict) or len(runs) > _MAX_IMPORT_RUNS:
            raise WandbImportError("checkpoint run map is invalid or exceeds its bound")
        self._data = data
        if not self.path.exists():
            self._compact()
        else:
            self.path.chmod(0o600)

    def state(self, source_id: str) -> dict[str, Any]:
        with self._lock:
            runs = self._runs()
            value = runs.get(source_id, {})
            if not isinstance(value, dict):
                raise WandbImportError(f"checkpoint run state is invalid: {source_id}")
            return deepcopy(value)

    def update(self, source_id: str, **updates: Any) -> None:
        with self._lock:
            runs = self._runs()
            if source_id not in runs and len(runs) >= _MAX_IMPORT_RUNS:
                raise WandbImportError(
                    f"checkpoint cannot contain more than {_MAX_IMPORT_RUNS} runs"
                )
            current = runs.setdefault(source_id, {})
            if not isinstance(current, dict):
                raise WandbImportError(f"checkpoint run state is invalid: {source_id}")
            current.update(updates)
            self._append_update(source_id, updates)

    def _runs(self) -> dict[str, Any]:
        runs = self._data["runs"]
        if not isinstance(runs, dict):
            raise WandbImportError("checkpoint run map is invalid")
        return runs

    def _load(self) -> dict[str, Any]:
        try:
            raw = self.path.read_bytes()
        except OSError as error:
            raise WandbImportError(f"invalid checkpoint: {self.path}") from error
        if raw and not raw.endswith(b"\n"):
            complete_end = raw.rfind(b"\n") + 1
            if complete_end == 0:
                raise WandbImportError(f"invalid checkpoint: {self.path}")
            raw = raw[:complete_end]
            with self.path.open("r+b") as stream:
                stream.truncate(complete_end)
                stream.flush()
                os.fsync(stream.fileno())
        data: dict[str, Any] | None = None
        try:
            for line_number, line in enumerate(raw.splitlines(), start=1):
                record = json.loads(line)
                if not isinstance(record, dict):
                    raise TypeError
                record_type = record.get("type")
                if line_number == 1 and record_type == "header":
                    data = {
                        "format_version": record.get("format_version"),
                        "source": record.get("source"),
                        "entity": record.get("entity"),
                        "project": record.get("project"),
                        "target_project": record.get("target_project"),
                        "runs": {},
                    }
                elif data is not None and record_type == "snapshot":
                    runs = record.get("runs")
                    if not isinstance(runs, dict):
                        raise TypeError
                    data["runs"] = runs
                elif data is not None and record_type == "update":
                    source_id = record.get("source_id")
                    updates = record.get("updates")
                    if not isinstance(source_id, str) or not isinstance(updates, dict):
                        raise TypeError
                    current = data["runs"].setdefault(source_id, {})
                    if not isinstance(current, dict):
                        raise TypeError
                    current.update(updates)
                    self._updates_since_compaction += 1
                else:
                    raise TypeError
        except (TypeError, json.JSONDecodeError) as error:
            raise WandbImportError(f"invalid checkpoint: {self.path}") from error
        if data is None:
            raise WandbImportError(f"invalid checkpoint: {self.path}")
        return data

    def _append_update(self, source_id: str, updates: dict[str, Any]) -> None:
        encoded = _checkpoint_line({"type": "update", "source_id": source_id, "updates": updates})
        if (
            self.path.stat().st_size >= _CHECKPOINT_COMPACT_BYTES
            and self._updates_since_compaction >= _CHECKPOINT_COMPACT_UPDATES
        ):
            self._compact()
        if self.path.stat().st_size + len(encoded) > _MAX_CHECKPOINT_BYTES:
            self._compact()
        if self.path.stat().st_size + len(encoded) > _MAX_CHECKPOINT_BYTES:
            raise WandbImportError("checkpoint exceeds the 64 MiB safety limit")
        with self.path.open("ab") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        self._updates_since_compaction += 1

    def _compact(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        header = _checkpoint_line(
            {
                "type": "header",
                "format_version": self._data["format_version"],
                "source": self._data["source"],
                "entity": self._data["entity"],
                "project": self._data["project"],
                "target_project": self._data["target_project"],
            }
        )
        snapshot = _checkpoint_line({"type": "snapshot", "runs": self._data["runs"]})
        if len(header) + len(snapshot) > _MAX_CHECKPOINT_BYTES:
            raise WandbImportError("checkpoint exceeds the 64 MiB safety limit")
        temporary = self.path.with_name(f".{self.path.name}.{uuid.uuid4()}.tmp")
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(header)
                stream.write(snapshot)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)
            _fsync_directory(self.path.parent)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        self._updates_since_compaction = 0


def _checkpoint_line(value: dict[str, Any]) -> bytes:
    try:
        return (
            json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True) + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise WandbImportError(f"checkpoint update is not JSON-compatible: {error}") from error


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
