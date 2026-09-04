from __future__ import annotations

import hashlib
import json
import math
import mimetypes
import tempfile
import uuid
from collections import deque
from collections.abc import Callable, Iterable, Iterator, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from pathlib import Path, PurePosixPath
from threading import Lock
from typing import Any, Protocol, TypeVar

from epochdeck._limits import (
    MAX_ARTIFACT_ALIAS_BYTES,
    MAX_ARTIFACT_ALIASES,
    MAX_ARTIFACT_DESCRIPTION_BYTES,
    MAX_ARTIFACT_ENTRIES,
    MAX_ARTIFACT_METADATA_BYTES,
    MAX_ARTIFACT_NAME_BYTES,
    MAX_ARTIFACT_TYPE_BYTES,
    MAX_SAFE_INTEGER,
)
from epochdeck._protocol import validate_blob_file_name
from epochdeck._wandb_state import (
    Checkpoint,
    ImportCancellation,
    ImportCancelled,
    WandbImportError,
    checkpoint_process_lock,
)
from epochdeck.artifact import _validate_component, _validate_manifest_size
from epochdeck.client import EpochDeckApiError, EpochDeckClient

_MAX_IMPORT_RUNS = 100_000
_MAX_WORKERS = 16
_ARTIFACT_WORKERS = 4
_MEDIA_WORKERS = 4
_MAX_PENDING_MEDIA = 8
_MEDIA_CHECKPOINT_INTERVAL = 256
_METRIC_BATCH_SIZE = 512
_METRIC_BATCH_BYTES = 1_750_000
_MAX_METRICS_PER_POINT = 256
_MAX_METRIC_KEY_BYTES = 256
_MAX_RUN_DOCUMENT_BYTES = 256 * 1024
_MAX_SOURCE_ROW_METRICS = 4_096
_MAX_SOURCE_ROW_MEDIA = 256
_MAX_SOURCE_ROW_NODES = 65_536
_MAX_SOURCE_ROW_DEPTH = 64
_MAX_METRIC_POINTS_PER_SOURCE_ROW = _MAX_SOURCE_ROW_METRICS // _MAX_METRICS_PER_POINT
_MAX_SOURCE_MEDIA_REFERENCE_BYTES = 64 * 1024
_MAX_SOURCE_REVISION_BYTES = 256
_WANDB_READ_ATTEMPTS = 5
_WANDB_RETRY_INITIAL_SECONDS = 0.25
_WANDB_RETRY_MAX_SECONDS = 4.0
_FILE_CHUNK_SIZE = 64
_HASH_CHUNK_BYTES = 1024 * 1024
_MIN_HISTORY_PAGE_SIZE = 1_000
_TARGET_HISTORY_PAGE_COUNT = 256
_TARGET_HISTORY_ROWS_PER_PAGE = 10_000
_MAX_HISTORY_PAGE_COUNT = 10_000
_MAX_HISTORY_RESPONSE_ROWS = 100_000
_PHASE_HISTORY = "history"
_PHASE_RUN_FILES = "run_files"
_PHASE_LOGGED_ARTIFACTS = "logged_artifacts"
_PHASE_FINALIZE = "finalize"
_PHASE_COMPLETE = "complete"
_IMPORT_PHASES = frozenset(
    {
        _PHASE_HISTORY,
        _PHASE_RUN_FILES,
        _PHASE_LOGGED_ARTIFACTS,
        _PHASE_FINALIZE,
        _PHASE_COMPLETE,
    }
)
_MEDIA_KINDS = {
    "audio-file": "audio",
    "image-file": "image",
    "video-file": "video",
}
_TERMINAL_RUN_STATES = frozenset({"finished", "failed", "crashed", "killed", "preempted"})

_T = TypeVar("_T")


class _IdentityDigest(Protocol):
    def update(self, value: bytes) -> None: ...

    def digest(self) -> bytes: ...


@dataclass(frozen=True, slots=True)
class ImportResult:
    selected: int
    completed: int
    skipped: int
    failed: int
    failures: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _SourceRevision:
    state: str
    updated_at: str


@dataclass(frozen=True, slots=True)
class _HistoryProgress:
    rows_committed: int
    media_rows_committed: int
    next_sequence: int
    unsupported_values: int


@dataclass(frozen=True, slots=True)
class _PreparedRunImport:
    source: Any
    source_id: str
    phase: str
    run_id: str
    source_revision: _SourceRevision
    source_metadata: dict[str, str]


def _retry_wandb_read(
    operation: Callable[[], _T],
    cancellation: ImportCancellation,
    description: str,
) -> _T:
    failures = 0
    while True:
        cancellation.check()
        try:
            return operation()
        except Exception as error:
            if not _is_transient_wandb_read_error(error):
                raise
            failures += 1
            if failures >= _WANDB_READ_ATTEMPTS:
                raise WandbImportError(
                    f"{description} failed after {_WANDB_READ_ATTEMPTS} attempts: {error}"
                ) from error
            _wait_for_wandb_retry(cancellation, failures)


def _retrying_wandb_reads(
    factory: Callable[[], Iterable[_T]],
    cancellation: ImportCancellation,
    description: str,
    *,
    resume_key: Callable[[_T], str] | None = None,
) -> Iterator[_T]:
    emitted = 0
    emitted_identity = hashlib.sha256()
    failures = 0
    iterator: Iterator[_T] | None = None
    while True:
        cancellation.check()
        positioning = iterator is None
        try:
            if iterator is None:
                iterator = iter(factory())
                replayed_identity = hashlib.sha256()
                for _ in range(emitted):
                    cancellation.check()
                    resumed_item = next(iterator)
                    if resume_key is not None:
                        _update_identity(replayed_identity, resume_key(resumed_item))
                if (
                    resume_key is not None
                    and replayed_identity.digest() != emitted_identity.digest()
                ):
                    raise WandbImportError(
                        f"{description} changed identity or order while resuming after "
                        "a transient failure"
                    )
            positioning = False
            item = next(iterator)
        except StopIteration:
            if positioning:
                raise WandbImportError(
                    f"{description} became shorter while resuming after a transient failure"
                ) from None
            return
        except Exception as error:
            if not _is_transient_wandb_read_error(error):
                raise
            failures += 1
            if failures >= _WANDB_READ_ATTEMPTS:
                raise WandbImportError(
                    f"{description} failed after {_WANDB_READ_ATTEMPTS} attempts: {error}"
                ) from error
            iterator = None
            _wait_for_wandb_retry(cancellation, failures)
            continue
        failures = 0
        emitted += 1
        if resume_key is not None:
            _update_identity(emitted_identity, resume_key(item))
        yield item


def _update_identity(digest: _IdentityDigest, value: str) -> None:
    encoded = value.encode("utf-8")
    digest.update(len(encoded).to_bytes(8, "big"))
    digest.update(encoded)


def _is_transient_wandb_read_error(error: Exception) -> bool:
    error_type = type(error)
    return error_type.__name__ == "CommError" and error_type.__module__.startswith("wandb.")


def _wait_for_wandb_retry(
    cancellation: ImportCancellation,
    failures: int,
) -> None:
    delay = min(
        _WANDB_RETRY_INITIAL_SECONDS * (2 ** (failures - 1)),
        _WANDB_RETRY_MAX_SECONDS,
    )
    cancellation.wait(delay)


class _SourceRunLoader:
    """Refresh W&B runs without racing the public API's shared local cache."""

    def __init__(self, source_api: Any, cancellation: ImportCancellation) -> None:
        self._source_api = source_api
        self._cancellation = cancellation
        self._lock = Lock()

    def refresh(self, *, entity: str, project: str, source_id: str) -> Any:
        flush = getattr(self._source_api, "flush", None)
        load_run = getattr(self._source_api, "run", None)
        if not callable(flush) or not callable(load_run):
            raise WandbImportError(
                "W&B source API cannot authoritatively refresh a run; "
                "expected callable flush() and run() methods"
            )
        with self._lock:
            path = f"{entity}/{project}/{source_id}"

            def refresh() -> Any:
                flush()
                return load_run(path)

            source = _retry_wandb_read(
                refresh,
                self._cancellation,
                f"W&B run refresh for {source_id!r}",
            )
        refreshed_id = _required_text(source, "id")
        if refreshed_id != source_id:
            raise WandbImportError(
                f"W&B refreshed run ID {refreshed_id!r} does not match {source_id!r}"
            )
        return source


class _MediaPipeline:
    def __init__(
        self,
        source: Any,
        client: EpochDeckClient,
        checkpoint: Checkpoint,
        *,
        source_id: str,
        run_id: str,
        source_metadata: dict[str, str],
        committed_rows: int,
        cancellation: ImportCancellation,
    ) -> None:
        self._source = source
        self._client = client
        self._checkpoint = checkpoint
        self._source_id = source_id
        self._run_id = run_id
        self._source_metadata = source_metadata
        self._cancellation = cancellation
        self._executor = ThreadPoolExecutor(
            max_workers=_MEDIA_WORKERS,
            thread_name_prefix="epochdeck-wandb-media",
        )
        self._pending: deque[tuple[int, Future[None]]] = deque()
        self._remaining_by_row: dict[int, int] = {}
        self._committed_rows = committed_rows
        self._persisted_rows = committed_rows
        self._last_closed_row = committed_rows
        self._closed = False

    @property
    def committed_rows(self) -> int:
        return self._committed_rows

    def schedule(
        self,
        row_number: int,
        media: list[tuple[str, str, dict[str, Any]]],
        *,
        step: int,
        timestamp_ms: int,
    ) -> None:
        self._cancellation.check()
        if self._closed:
            raise RuntimeError("media pipeline is closed")
        if row_number <= self._committed_rows:
            return
        if row_number != self._last_closed_row + 1:
            raise WandbImportError("W&B media rows were not scheduled in order")
        for occurrence, (key, kind, reference) in enumerate(media):
            while len(self._pending) >= _MAX_PENDING_MEDIA:
                self._drain_one()
                self._cancellation.check()
            artifact_path = _safe_artifact_path(str(reference["path"]))
            self._cancellation.check()
            source_file = _retry_wandb_read(
                partial(self._source.file, artifact_path),
                self._cancellation,
                f"W&B media file lookup for {artifact_path!r}",
            )
            future = self._executor.submit(
                _import_media_reference,
                source_file,
                self._client,
                run_id=self._run_id,
                source_metadata=self._source_metadata,
                key=key,
                kind=kind,
                step=step,
                timestamp_ms=timestamp_ms,
                artifact_path=artifact_path,
                reference=reference,
                row_number=row_number,
                occurrence=occurrence,
                cancellation=self._cancellation,
            )
            self._pending.append((row_number, future))
            self._remaining_by_row[row_number] = self._remaining_by_row.get(row_number, 0) + 1
        self._last_closed_row = row_number
        self._advance_watermark()

    def finish(self) -> None:
        if self._closed:
            return
        while self._pending:
            self._cancellation.check()
            self._drain_one()
        self._advance_watermark()
        self._persist(force=True)
        self._executor.shutdown(wait=True)
        self._closed = True

    def abort(self) -> None:
        if self._closed:
            return
        for _, future in self._pending:
            future.cancel()
        interrupted = self._cancellation.cancelled
        self._executor.shutdown(wait=not interrupted, cancel_futures=True)
        if not interrupted:
            self._persist(force=True)
        self._closed = True

    def _drain_one(self) -> None:
        row_number, future = self._pending[0]
        future.result()
        self._pending.popleft()
        remaining = self._remaining_by_row[row_number] - 1
        if remaining == 0:
            del self._remaining_by_row[row_number]
        else:
            self._remaining_by_row[row_number] = remaining
        self._advance_watermark()

    def _advance_watermark(self) -> None:
        while (
            self._committed_rows < self._last_closed_row
            and self._committed_rows + 1 not in self._remaining_by_row
        ):
            self._committed_rows += 1
        self._persist(force=False)

    def _persist(self, *, force: bool) -> None:
        if self._committed_rows == self._persisted_rows:
            return
        if not force and self._committed_rows - self._persisted_rows < _MEDIA_CHECKPOINT_INTERVAL:
            return
        self._checkpoint.update(
            self._source_id,
            media_rows_committed=self._committed_rows,
        )
        self._persisted_rows = self._committed_rows


def import_wandb_runs(
    source_api: Any,
    client: EpochDeckClient,
    *,
    entity: str,
    project: str,
    target_project: str,
    checkpoint_path: Path,
    workers: int = 4,
    max_runs: int | None = None,
    include_files: bool = True,
) -> ImportResult:
    """Import with bounded workers; cancellation lets only active SDK calls finish."""
    if not 1 <= workers <= _MAX_WORKERS:
        raise ValueError(f"workers must be between 1 and {_MAX_WORKERS}")
    if max_runs is not None and not 1 <= max_runs <= _MAX_IMPORT_RUNS:
        raise ValueError(f"max_runs must be between 1 and {_MAX_IMPORT_RUNS}")
    with checkpoint_process_lock(checkpoint_path):
        checkpoint = Checkpoint(
            checkpoint_path,
            entity=entity,
            project=project,
            target_project=target_project,
        )
        cancellation = ImportCancellation()
        source_loader = _SourceRunLoader(source_api, cancellation)
        selected = completed = skipped = failed = 0
        failures: list[str] = []
        pending: deque[tuple[str, Future[str]]] = deque()
        discovered_source_ids: set[str] = set()

        def collect(source_id: str, future: Future[str]) -> None:
            nonlocal completed, skipped, failed
            try:
                outcome = future.result()
            except ImportCancelled:
                raise
            except Exception as error:
                failed += 1
                message = f"{source_id}: {type(error).__name__}: {error}"
                failures.append(message)
                checkpoint.update(source_id, status="failed", error=message)
            else:
                if outcome == "skipped":
                    skipped += 1
                else:
                    completed += 1

        executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="epochdeck-wandb")
        interrupted = False
        try:
            for source_run in _retrying_wandb_reads(
                lambda: source_api.runs(f"{entity}/{project}"),
                cancellation,
                f"W&B run listing for {entity!r}/{project!r}",
                resume_key=lambda run: _required_text(run, "id"),
            ):
                cancellation.check()
                if max_runs is not None and selected >= max_runs:
                    break
                if max_runs is None and selected >= _MAX_IMPORT_RUNS:
                    raise WandbImportError(
                        f"W&B source contains more than {_MAX_IMPORT_RUNS} runs; "
                        "split the import into smaller projects"
                    )
                source_id = _required_text(source_run, "id")
                if source_id in discovered_source_ids:
                    raise WandbImportError(
                        f"W&B run listing returned duplicate source ID {source_id!r}"
                    )
                discovered_source_ids.add(source_id)
                future = executor.submit(
                    _import_one_run,
                    source_run,
                    source_loader,
                    client,
                    checkpoint,
                    entity,
                    project,
                    target_project,
                    include_files,
                    cancellation,
                )
                pending.append((source_id, future))
                selected += 1
                if len(pending) >= workers:
                    collect(*pending.popleft())
            for item in pending:
                collect(*item)
        except BaseException as error:
            interrupted = isinstance(error, KeyboardInterrupt)
            cancellation.cancel()
            for _, future in pending:
                future.cancel()
            raise
        finally:
            executor.shutdown(wait=not interrupted, cancel_futures=True)

        return ImportResult(
            selected=selected,
            completed=completed,
            skipped=skipped,
            failed=failed,
            failures=tuple(failures),
        )


def _import_one_run(
    source: Any,
    source_loader: _SourceRunLoader,
    client: EpochDeckClient,
    checkpoint: Checkpoint,
    entity: str,
    project: str,
    target_project: str,
    include_files: bool,
    cancellation: ImportCancellation,
) -> str:
    prepared = _prepare_run_import(
        source,
        source_loader,
        client,
        checkpoint,
        entity=entity,
        project=project,
        target_project=target_project,
        include_files=include_files,
        cancellation=cancellation,
    )
    if prepared is None:
        return "skipped"
    source = prepared.source
    source_id = prepared.source_id
    phase = prepared.phase
    run_id = prepared.run_id
    source_revision = prepared.source_revision
    source_metadata = prepared.source_metadata

    progress = _import_history(
        source,
        client,
        checkpoint,
        source_id=source_id,
        run_id=run_id,
        source_metadata=source_metadata,
        phase=phase,
        include_files=include_files,
        cancellation=cancellation,
    )

    if phase == _PHASE_HISTORY:
        phase = _PHASE_RUN_FILES if include_files else _PHASE_FINALIZE
        checkpoint.update(
            source_id,
            phase=phase,
            rows_committed=progress.rows_committed,
            media_rows_committed=progress.media_rows_committed,
            next_sequence=progress.next_sequence,
            unsupported_values=progress.unsupported_values,
        )

    if include_files and phase == _PHASE_RUN_FILES:
        _import_run_files(
            source,
            client,
            checkpoint,
            source_id,
            run_id,
            source_metadata,
            cancellation,
        )
        phase = _PHASE_LOGGED_ARTIFACTS
        checkpoint.update(source_id, phase=phase)
    if include_files and phase == _PHASE_LOGGED_ARTIFACTS:
        _import_logged_artifacts(
            source,
            client,
            checkpoint,
            source_id,
            run_id,
            source_metadata,
            cancellation,
        )
        phase = _PHASE_FINALIZE
        checkpoint.update(source_id, phase=phase)

    if phase != _PHASE_FINALIZE:
        raise WandbImportError(f"cannot finalize W&B import from checkpoint phase {phase!r}")

    _finalize_import(
        source_loader,
        client,
        checkpoint,
        entity=entity,
        project=project,
        source_id=source_id,
        run_id=run_id,
        source_revision=source_revision,
        source_metadata=source_metadata,
        progress=progress,
        cancellation=cancellation,
    )
    return "completed"


def _prepare_run_import(
    source: Any,
    source_loader: _SourceRunLoader,
    client: EpochDeckClient,
    checkpoint: Checkpoint,
    *,
    entity: str,
    project: str,
    target_project: str,
    include_files: bool,
    cancellation: ImportCancellation,
) -> _PreparedRunImport | None:
    cancellation.check()
    source_id = _required_text(source, "id")
    state, phase = _resume_import_state(checkpoint, source_id, include_files)
    source = source_loader.refresh(entity=entity, project=project, source_id=source_id)
    cancellation.check()
    source_revision = _retry_wandb_read(
        lambda: _source_revision(source),
        cancellation,
        f"W&B source revision read for {source_id!r}",
    )
    checkpoint_complete = _validate_source_revision(state, phase, source_id, source_revision)

    run_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"https://wandb.ai/{entity}/{project}/runs/{source_id}",
        )
    )
    stored_run_id = state.get("run_id")
    if stored_run_id is not None and stored_run_id != run_id:
        raise WandbImportError("checkpoint has the wrong deterministic EpochDeck run ID")
    if checkpoint_complete and stored_run_id is None:
        raise WandbImportError("completed checkpoint has no deterministic EpochDeck run ID")
    source_url = _retry_wandb_read(
        lambda: _optional_text(source, "url"),
        cancellation,
        f"W&B source URL read for {source_id!r}",
    )
    source_name = _retry_wandb_read(
        lambda: _optional_text(source, "name"),
        cancellation,
        f"W&B source name read for {source_id!r}",
    )
    source_metadata = {
        "entity": entity,
        "project": project,
        "run_id": source_id,
        "url": source_url,
        "state": source_revision.state,
        "updated_at": source_revision.updated_at,
    }
    raw_config = _retry_wandb_read(
        lambda: getattr(source, "config", {}) or {},
        cancellation,
        f"W&B config read for {source_id!r}",
    )
    config = _wandb_document(
        raw_config,
        "W&B config",
        _MAX_RUN_DOCUMENT_BYTES,
    )
    if "_epochdeck_wandb_source" in config:
        raise WandbImportError("W&B config uses reserved key '_epochdeck_wandb_source'")
    config["_epochdeck_wandb_source"] = source_metadata
    config = _json_document(config, "W&B config", _MAX_RUN_DOCUMENT_BYTES)
    record = _get_or_create_target_run(
        client,
        target_project=target_project,
        run_id=run_id,
        run_name=source_name or source_id,
        config=config,
        checkpoint_complete=checkpoint_complete,
        cancellation=cancellation,
    )
    existing_source = _object(record, "config").get("_epochdeck_wandb_source")
    if record.get("project") != target_project:
        raise WandbImportError(f"deterministic EpochDeck run ID collision: {run_id}")
    if existing_source != source_metadata:
        if _same_wandb_source(existing_source, source_metadata):
            raise _source_changed_error(source_id, "since the target run was created")
        raise WandbImportError(f"deterministic EpochDeck run ID collision: {run_id}")
    if checkpoint_complete:
        if record.get("state") != "finished":
            raise WandbImportError("completed checkpoint target run is not finished")
        return None
    if record.get("state") == "finished":
        if phase != _PHASE_FINALIZE:
            raise WandbImportError(
                f"finished target run cannot recover from checkpoint phase {phase!r}"
            )
        summary = record.get("summary")
        expected_source = {
            **source_metadata,
            "unsupported_history_values": _state_int(state, "unsupported_values", 0),
        }
        if (
            not isinstance(summary, Mapping)
            or summary.get("_epochdeck_wandb_source") != expected_source
        ):
            raise WandbImportError(
                "finished target run does not contain the expected imported W&B summary"
            )
        checkpoint.update(
            source_id,
            status="complete",
            phase=_PHASE_COMPLETE,
            run_id=run_id,
            source_updated_at=source_revision.updated_at,
            source_state=source_revision.state,
        )
        return None

    checkpoint.update(
        source_id,
        status="importing",
        error=None,
        run_id=run_id,
        source_updated_at=source_revision.updated_at,
        source_state=source_revision.state,
    )
    return _PreparedRunImport(
        source=source,
        source_id=source_id,
        phase=phase,
        run_id=run_id,
        source_revision=source_revision,
        source_metadata=source_metadata,
    )


def _resume_import_state(
    checkpoint: Checkpoint,
    source_id: str,
    include_files: bool,
) -> tuple[dict[str, Any], str]:
    state = checkpoint.state(source_id)
    if not state:
        checkpoint.update(source_id, phase=_PHASE_HISTORY, include_files=include_files)
        return checkpoint.state(source_id), _PHASE_HISTORY
    phase = _checkpoint_phase(state)
    if _checkpoint_include_files(state) != include_files:
        raise WandbImportError("checkpoint include_files contract does not match this W&B import")
    return state, phase


def _validate_source_revision(
    state: Mapping[str, Any],
    phase: str,
    source_id: str,
    source_revision: _SourceRevision,
) -> bool:
    if source_revision.state not in _TERMINAL_RUN_STATES:
        raise WandbImportError(
            f"W&B run {source_id!r} is not terminal (state={source_revision.state!r})"
        )
    stored_updated_at = state.get("source_updated_at")
    stored_source_state = state.get("source_state")
    if stored_updated_at is not None or stored_source_state is not None:
        if not isinstance(stored_updated_at, str) or not isinstance(stored_source_state, str):
            raise WandbImportError("checkpoint has an incomplete W&B source revision")
        if (stored_source_state, stored_updated_at) != (
            source_revision.state,
            source_revision.updated_at,
        ):
            raise _source_changed_error(source_id, "after import began")
    checkpoint_complete = state.get("status") == "complete"
    if checkpoint_complete:
        if stored_updated_at is None:
            raise WandbImportError("completed checkpoint has no W&B source revision")
        if phase != _PHASE_COMPLETE:
            raise WandbImportError("completed checkpoint is not in the complete import phase")
    return checkpoint_complete


def _get_or_create_target_run(
    client: EpochDeckClient,
    *,
    target_project: str,
    run_id: str,
    run_name: str,
    config: dict[str, Any],
    checkpoint_complete: bool,
    cancellation: ImportCancellation,
) -> dict[str, Any]:
    try:
        cancellation.check()
        record = client.get_run(run_id)
        cancellation.check()
        return record
    except EpochDeckApiError as error:
        if error.status_code != 404:
            raise
        if checkpoint_complete:
            raise WandbImportError("completed checkpoint target run does not exist") from error
    cancellation.check()
    created = client.create_run(
        project=target_project,
        run_id=run_id,
        name=run_name,
        config=config,
        resume="never",
    )
    cancellation.check()
    return _object(created, "run")


def _import_history(
    source: Any,
    client: EpochDeckClient,
    checkpoint: Checkpoint,
    *,
    source_id: str,
    run_id: str,
    source_metadata: dict[str, str],
    phase: str,
    include_files: bool,
    cancellation: ImportCancellation,
) -> _HistoryProgress:
    state = checkpoint.state(source_id)
    rows_committed = _state_int(state, "rows_committed", 0)
    next_sequence = _state_int(state, "next_sequence", 1)
    unsupported_values = _state_int(state, "unsupported_values", 0)
    media_rows_committed = _state_int(state, "media_rows_committed", 0)
    batch: list[dict[str, Any]] = []
    batch_bytes = 0
    batch_rows_end = rows_committed
    scanned_rows = 0 if phase == _PHASE_HISTORY else max(rows_committed, media_rows_committed)
    media_pipeline = (
        _MediaPipeline(
            source,
            client,
            checkpoint,
            source_id=source_id,
            run_id=run_id,
            source_metadata=source_metadata,
            committed_rows=media_rows_committed,
            cancellation=cancellation,
        )
        if include_files and phase == _PHASE_HISTORY
        else None
    )
    history_row_limit = 0
    history: Iterable[Any]
    try:
        if phase == _PHASE_HISTORY:
            history_row_limit = _retry_wandb_read(
                lambda: _history_line_count(source),
                cancellation,
                f"W&B history row-count read for {source_id!r}",
            )
            if history_row_limit == 0:
                history = ()
            else:
                history_page_size = _retry_wandb_read(
                    lambda: _history_page_size(source),
                    cancellation,
                    f"W&B history metadata read for {source_id!r}",
                )
                history = _retrying_wandb_reads(
                    lambda: source.scan_history(page_size=history_page_size),
                    cancellation,
                    f"W&B history scan for {source_id!r}",
                    resume_key=_history_row_identity,
                )
        else:
            history = ()
        for row_index, raw_row in enumerate(history):
            cancellation.check()
            scanned_rows = row_index + 1
            if scanned_rows > history_row_limit:
                raise WandbImportError("W&B history exceeded its declared historyLineCount")
            if not isinstance(raw_row, Mapping):
                raise WandbImportError("W&B history yielded a non-object row")
            metrics, media, skipped_values = _history_values(raw_row)
            step = _history_step(raw_row, row_index)
            timestamp_ms = _history_timestamp_ms(raw_row, row_index)
            if media_pipeline is not None:
                media_pipeline.schedule(
                    scanned_rows,
                    media,
                    step=step,
                    timestamp_ms=timestamp_ms,
                )
            if scanned_rows <= rows_committed:
                continue
            if not include_files:
                skipped_values += len(media)
            unsupported_values += skipped_values
            if metrics:
                _validate_metric_keys(metrics)
                if len(metrics) > _MAX_METRICS_PER_POINT:
                    if batch:
                        _commit_metric_batch(client, run_id, batch, cancellation)
                        next_sequence += len(batch)
                        rows_committed = batch_rows_end
                        checkpoint.update(
                            source_id,
                            rows_committed=rows_committed,
                            next_sequence=next_sequence,
                            unsupported_values=unsupported_values - skipped_values,
                        )
                        batch = []
                        batch_bytes = 0
                    committed_points = _commit_split_metric_row(
                        client,
                        run_id,
                        metrics,
                        step=step,
                        timestamp_ms=timestamp_ms,
                        next_sequence=next_sequence,
                        cancellation=cancellation,
                    )
                    next_sequence += committed_points
                    rows_committed = scanned_rows
                    batch_rows_end = rows_committed
                    checkpoint.update(
                        source_id,
                        rows_committed=rows_committed,
                        next_sequence=next_sequence,
                        unsupported_values=unsupported_values,
                    )
                    continue
                sequence = next_sequence + len(batch)
                point = {
                    "sequence": sequence,
                    "step": step,
                    "timestamp_ms": timestamp_ms,
                    "metrics": metrics,
                }
                point_bytes = _json_size(point) + 1
                if point_bytes > _METRIC_BATCH_BYTES:
                    raise WandbImportError(
                        f"W&B history row {row_index} exceeds the metric request byte budget"
                    )
                if batch and batch_bytes + point_bytes > _METRIC_BATCH_BYTES:
                    _commit_metric_batch(client, run_id, batch, cancellation)
                    next_sequence += len(batch)
                    rows_committed = batch_rows_end
                    checkpoint.update(
                        source_id,
                        rows_committed=rows_committed,
                        next_sequence=next_sequence,
                        unsupported_values=unsupported_values - skipped_values,
                    )
                    batch = []
                    batch_bytes = 0
                    point["sequence"] = next_sequence
                batch.append(point)
                batch_bytes += point_bytes
            batch_rows_end = scanned_rows
            if len(batch) >= _METRIC_BATCH_SIZE:
                _commit_metric_batch(client, run_id, batch, cancellation)
                next_sequence += len(batch)
                rows_committed = batch_rows_end
                batch = []
                batch_bytes = 0
                checkpoint.update(
                    source_id,
                    rows_committed=rows_committed,
                    next_sequence=next_sequence,
                    unsupported_values=unsupported_values,
                )

        if phase == _PHASE_HISTORY and scanned_rows != history_row_limit:
            raise WandbImportError(
                f"W&B history yielded {scanned_rows} rows but historyLineCount "
                f"declares {history_row_limit}"
            )
        if scanned_rows < rows_committed or scanned_rows < media_rows_committed:
            raise WandbImportError("W&B history became shorter after import began")
        if batch:
            _commit_metric_batch(client, run_id, batch, cancellation)
            next_sequence += len(batch)
        if scanned_rows > rows_committed:
            rows_committed = scanned_rows
            checkpoint.update(
                source_id,
                rows_committed=rows_committed,
                next_sequence=next_sequence,
                unsupported_values=unsupported_values,
            )
        if media_pipeline is not None:
            media_pipeline.finish()
            media_rows_committed = media_pipeline.committed_rows
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt):
            cancellation.cancel()
        if media_pipeline is not None:
            try:
                media_pipeline.abort()
            except Exception as cleanup_error:
                add_note = getattr(error, "add_note", None)
                if callable(add_note):
                    add_note(
                        "EpochDeck also failed while draining W&B media workers: "
                        f"{type(cleanup_error).__name__}: {cleanup_error}"
                    )
        raise

    return _HistoryProgress(
        rows_committed=rows_committed,
        media_rows_committed=media_rows_committed,
        next_sequence=next_sequence,
        unsupported_values=unsupported_values,
    )


def _finalize_import(
    source_loader: _SourceRunLoader,
    client: EpochDeckClient,
    checkpoint: Checkpoint,
    *,
    entity: str,
    project: str,
    source_id: str,
    run_id: str,
    source_revision: _SourceRevision,
    source_metadata: dict[str, str],
    progress: _HistoryProgress,
    cancellation: ImportCancellation,
) -> None:
    cancellation.check()
    refreshed_source = source_loader.refresh(
        entity=entity,
        project=project,
        source_id=source_id,
    )
    cancellation.check()
    refreshed_revision = _retry_wandb_read(
        lambda: _source_revision(refreshed_source),
        cancellation,
        f"W&B final source revision read for {source_id!r}",
    )
    if refreshed_revision != source_revision:
        raise _source_changed_error(source_id, "during import")

    raw_summary = _retry_wandb_read(
        lambda: getattr(refreshed_source, "summary", {}) or {},
        cancellation,
        f"W&B summary read for {source_id!r}",
    )
    summary = _wandb_document(
        raw_summary,
        "W&B summary",
        _MAX_RUN_DOCUMENT_BYTES,
    )
    if "_epochdeck_wandb_source" in summary:
        raise WandbImportError("W&B summary uses reserved key '_epochdeck_wandb_source'")
    summary["_epochdeck_wandb_source"] = {
        **source_metadata,
        "unsupported_history_values": progress.unsupported_values,
    }
    summary = _json_document(summary, "W&B summary", _MAX_RUN_DOCUMENT_BYTES)
    cancellation.check()
    client.finish_run(run_id, summary)
    cancellation.check()
    checkpoint.update(
        source_id,
        status="complete",
        phase=_PHASE_COMPLETE,
        rows_committed=progress.rows_committed,
        media_rows_committed=progress.media_rows_committed,
        next_sequence=progress.next_sequence,
        unsupported_values=progress.unsupported_values,
        source_updated_at=source_revision.updated_at,
        source_state=source_revision.state,
        error=None,
    )


def _commit_metric_batch(
    client: EpochDeckClient,
    run_id: str,
    points: list[dict[str, Any]],
    cancellation: ImportCancellation,
) -> None:
    cancellation.check()
    request = {"batch_sequence": points[0]["sequence"], "points": points}
    client.ingest_batch(run_id, request)
    cancellation.check()


def _commit_split_metric_row(
    client: EpochDeckClient,
    run_id: str,
    metrics: dict[str, float | bool],
    *,
    step: int,
    timestamp_ms: int,
    next_sequence: int,
    cancellation: ImportCancellation,
) -> int:
    batch: list[dict[str, Any]] = []
    batch_bytes = 0
    point_count = 0
    for metric_chunk in _metric_chunks(metrics):
        if point_count >= _MAX_METRIC_POINTS_PER_SOURCE_ROW:
            raise WandbImportError(
                "W&B history row exceeds "
                f"{_MAX_METRIC_POINTS_PER_SOURCE_ROW} metric points after splitting"
            )
        point = {
            "sequence": next_sequence + point_count,
            "step": step,
            "timestamp_ms": timestamp_ms,
            "metrics": metric_chunk,
        }
        point_bytes = _json_size(point) + 1
        if point_bytes > _METRIC_BATCH_BYTES:
            raise WandbImportError("W&B metric chunk exceeds the request byte budget")
        if batch and (
            len(batch) >= _METRIC_BATCH_SIZE or batch_bytes + point_bytes > _METRIC_BATCH_BYTES
        ):
            _commit_metric_batch(client, run_id, batch, cancellation)
            batch = []
            batch_bytes = 0
        batch.append(point)
        batch_bytes += point_bytes
        point_count += 1
    if batch:
        _commit_metric_batch(client, run_id, batch, cancellation)
    return point_count


def _metric_chunks(metrics: dict[str, float | bool]) -> Iterator[dict[str, float | bool]]:
    chunk: dict[str, float | bool] = {}
    for key in sorted(metrics):
        chunk[key] = metrics[key]
        if len(chunk) == _MAX_METRICS_PER_POINT:
            yield chunk
            chunk = {}
    if chunk:
        yield chunk


def _validate_metric_keys(metrics: Mapping[str, float | bool]) -> None:
    for key in metrics:
        encoded = key.encode("utf-8")
        if (
            not encoded
            or len(encoded) > _MAX_METRIC_KEY_BYTES
            or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in key)
        ):
            raise WandbImportError(
                f"W&B metric key {key!r} must contain 1 to "
                f"{_MAX_METRIC_KEY_BYTES} non-control bytes"
            )


def _artifact_aliases(source_artifact: Any, source_name: str) -> list[str]:
    raw_aliases = getattr(source_artifact, "aliases", ()) or ()
    if isinstance(raw_aliases, (str, bytes)):
        raise WandbImportError(f"W&B artifact {source_name!r} has an invalid alias collection")
    try:
        iterator = iter(raw_aliases)
    except TypeError as error:
        raise WandbImportError(
            f"W&B artifact {source_name!r} has an invalid alias collection"
        ) from error
    aliases: list[str] = []
    known: set[str] = set()
    for index, raw_alias in enumerate(iterator):
        if index >= MAX_ARTIFACT_ALIASES:
            raise WandbImportError(
                f"W&B artifact {source_name!r} exposes more than "
                f"{MAX_ARTIFACT_ALIASES} alias values"
            )
        alias = str(raw_alias)
        if not alias or alias in known:
            continue
        _validate_component(alias, "artifact alias", MAX_ARTIFACT_ALIAS_BYTES)
        aliases.append(alias)
        known.add(alias)
    return aliases


def _artifact_version(source_artifact: Any, source_name: str) -> tuple[str, int]:
    raw_version = getattr(source_artifact, "version", None)
    if not isinstance(raw_version, str):
        raise WandbImportError(f"W&B artifact {source_name!r} has no canonical vN artifact version")
    digits = raw_version.removeprefix("v")
    if (
        not raw_version.startswith("v")
        or not digits
        or not digits.isascii()
        or not digits.isdigit()
        or (len(digits) > 1 and digits.startswith("0"))
    ):
        raise WandbImportError(
            f"W&B artifact {source_name!r} has invalid artifact version {raw_version!r}; "
            "expected canonical vN"
        )
    version = int(digits)
    if version > MAX_SAFE_INTEGER:
        raise WandbImportError(
            f"W&B artifact {source_name!r} has artifact version outside 0..{MAX_SAFE_INTEGER}"
        )
    if not source_name.endswith(f":{raw_version}"):
        raise WandbImportError(
            f"W&B artifact name {source_name!r} does not end with version {raw_version!r}"
        )
    return raw_version, version


def _import_run_files(
    source: Any,
    client: EpochDeckClient,
    checkpoint: Checkpoint,
    source_id: str,
    run_id: str,
    source_metadata: dict[str, str],
    cancellation: ImportCancellation,
) -> None:
    cancellation.check()
    state = checkpoint.state(source_id)
    if state.get("files_complete") is True:
        return
    files_committed = _state_int(state, "files_committed", 0)
    chunk: list[Any] = []
    chunk_start = files_committed
    seen = 0
    source_files = _retrying_wandb_reads(
        source.files,
        cancellation,
        f"W&B run file listing for {source_id!r}",
        resume_key=lambda source_file: _required_text(source_file, "name"),
    )
    for index, source_file in enumerate(source_files):
        cancellation.check()
        seen = index + 1
        if seen <= files_committed:
            continue
        if not chunk:
            chunk_start = index
        chunk.append(source_file)
        if len(chunk) == _FILE_CHUNK_SIZE:
            _import_file_chunk(
                client,
                chunk,
                run_id=run_id,
                source_metadata=source_metadata,
                chunk_start=chunk_start,
                cancellation=cancellation,
            )
            files_committed = seen
            checkpoint.update(source_id, files_committed=files_committed)
            chunk = []
    if chunk:
        _import_file_chunk(
            client,
            chunk,
            run_id=run_id,
            source_metadata=source_metadata,
            chunk_start=chunk_start,
            cancellation=cancellation,
        )
        files_committed = seen
    if seen < files_committed:
        raise WandbImportError("W&B run file listing became shorter after import began")
    checkpoint.update(source_id, files_committed=files_committed, files_complete=True)


def _import_file_chunk(
    client: EpochDeckClient,
    files: list[Any],
    *,
    run_id: str,
    source_metadata: dict[str, str],
    chunk_start: int,
    cancellation: ImportCancellation,
) -> None:
    entries: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="epochdeck-wandb-") as raw_root:
        root = Path(raw_root).resolve()
        for source_file in files:
            cancellation.check()
            artifact_path = _safe_artifact_path(_required_text(source_file, "name"))
            downloaded = _retry_wandb_read(
                partial(source_file.download, root=str(root), replace=True),
                cancellation,
                f"W&B run file download for {artifact_path!r}",
            )
            cancellation.check()
            local_path = _downloaded_path(downloaded, root, artifact_path)
            try:
                blob = _blob_for_file(local_path, artifact_path, cancellation)
                cancellation.check()
                client.upload_blob(local_path, blob)
                cancellation.check()
            finally:
                local_path.unlink(missing_ok=True)
            entries.append({"path": artifact_path, "blob": blob})
        artifact_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"https://wandb.ai/{source_metadata['entity']}/{source_metadata['project']}"
                f"/runs/{source_metadata['run_id']}/files/{chunk_start}",
            )
        )
        request = {
            "id": artifact_id,
            "name": f"wandb-run-{run_id}-files-{chunk_start // _FILE_CHUNK_SIZE:04d}",
            "type": "wandb-run-files",
            "description": "One shard of files preserved by the EpochDeck W&B importer.",
            "metadata": {
                "wandb_source": source_metadata,
                "chunk_start": chunk_start,
                "shard_size": len(entries),
            },
            "aliases": ["latest"],
            "entries": entries,
        }
        _validate_import_artifact(request)
        cancellation.check()
        client.create_artifact(run_id, request)
        cancellation.check()


def _import_logged_artifacts(
    source: Any,
    client: EpochDeckClient,
    checkpoint: Checkpoint,
    source_id: str,
    run_id: str,
    source_metadata: dict[str, str],
    cancellation: ImportCancellation,
) -> None:
    cancellation.check()
    state = checkpoint.state(source_id)
    if state.get("logged_artifacts_complete") is True:
        return
    list_artifacts = getattr(source, "logged_artifacts", None)
    if not callable(list_artifacts):
        raise WandbImportError(
            "W&B source API does not provide callable logged_artifacts(); "
            "cannot import requested artifacts"
        )
    committed = _state_int(state, "logged_artifacts_committed", 0)
    seen = 0
    pending: deque[tuple[int, Future[None]]] = deque()

    def collect(index: int, future: Future[None]) -> None:
        nonlocal committed
        future.result()
        cancellation.check()
        committed = index + 1
        checkpoint.update(source_id, logged_artifacts_committed=committed)

    executor = ThreadPoolExecutor(
        max_workers=_ARTIFACT_WORKERS,
        thread_name_prefix="epochdeck-wandb-artifact",
    )
    try:
        artifacts = _retrying_wandb_reads(
            lambda: list_artifacts(per_page=100),
            cancellation,
            f"W&B logged artifact listing for {source_id!r}",
            resume_key=_source_artifact_identity,
        )
        for index, artifact in enumerate(artifacts):
            cancellation.check()
            seen = index + 1
            if seen <= committed:
                continue
            pending.append(
                (
                    index,
                    executor.submit(
                        _import_logged_artifact,
                        client,
                        artifact,
                        run_id,
                        source_metadata,
                        cancellation,
                    ),
                )
            )
            if len(pending) >= _ARTIFACT_WORKERS:
                collect(*pending.popleft())
        for item in pending:
            collect(*item)
    except BaseException as error:
        if isinstance(error, KeyboardInterrupt):
            cancellation.cancel()
        for _, future in pending:
            future.cancel()
        raise
    finally:
        executor.shutdown(wait=not cancellation.cancelled, cancel_futures=True)
    if seen < committed:
        raise WandbImportError("W&B logged artifact listing became shorter after import began")
    checkpoint.update(
        source_id,
        logged_artifacts_committed=max(committed, seen),
        logged_artifacts_complete=True,
    )


def _import_logged_artifact(
    client: EpochDeckClient,
    source_artifact: Any,
    run_id: str,
    source_metadata: dict[str, str],
    cancellation: ImportCancellation,
) -> None:
    cancellation.check()
    qualified_name = _optional_text(source_artifact, "qualified_name")
    source_id = _source_artifact_identity(source_artifact)
    source_name = _required_text(source_artifact, "name")
    source_version, target_version = _artifact_version(source_artifact, source_name)
    target_name = source_name.removesuffix(f":{source_version}")
    artifact_type = _required_text(source_artifact, "type")
    _validate_component(target_name, "artifact name", MAX_ARTIFACT_NAME_BYTES)
    _validate_component(artifact_type, "artifact type", MAX_ARTIFACT_TYPE_BYTES)
    raw_metadata = _wandb_document(
        getattr(source_artifact, "metadata", {}) or {},
        "W&B artifact source metadata",
        MAX_ARTIFACT_METADATA_BYTES,
    )
    metadata = _json_document(
        {
            "wandb_source": {
                **source_metadata,
                "artifact_id": source_id,
                "qualified_name": qualified_name,
                "version": source_version,
            },
            "wandb_metadata": raw_metadata,
        },
        "W&B artifact metadata",
        MAX_ARTIFACT_METADATA_BYTES,
    )
    aliases = _artifact_aliases(source_artifact, source_name)
    description_value = getattr(source_artifact, "description", None)
    description = str(description_value) if description_value is not None else None
    if (
        description is not None
        and len(description.encode("utf-8")) > MAX_ARTIFACT_DESCRIPTION_BYTES
    ):
        raise WandbImportError(
            f"W&B artifact description exceeds {MAX_ARTIFACT_DESCRIPTION_BYTES} bytes"
        )
    artifact_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"https://wandb.ai/artifacts/{source_id}"))
    source_files: list[Any] = []
    source_artifact_files = _retrying_wandb_reads(
        lambda: source_artifact.files(per_page=100),
        cancellation,
        f"W&B artifact file listing for {source_name!r}",
        resume_key=lambda source_file: _required_text(source_file, "name"),
    )
    for source_file in source_artifact_files:
        cancellation.check()
        if len(source_files) >= MAX_ARTIFACT_ENTRIES:
            raise WandbImportError(
                f"W&B artifact {source_name!r} exceeds {MAX_ARTIFACT_ENTRIES} files"
            )
        source_files.append(source_file)
    entries: list[dict[str, Any]] = []
    with tempfile.TemporaryDirectory(prefix="epochdeck-wandb-artifact-") as raw_root:
        root = Path(raw_root).resolve()
        for source_file in source_files:
            cancellation.check()
            artifact_path = _safe_artifact_path(_required_text(source_file, "name"))
            downloaded = _retry_wandb_read(
                partial(source_file.download, root=str(root), replace=True),
                cancellation,
                f"W&B artifact file download for {artifact_path!r}",
            )
            cancellation.check()
            local_path = _downloaded_path(downloaded, root, artifact_path)
            try:
                blob = _blob_for_file(local_path, artifact_path, cancellation)
                cancellation.check()
                client.upload_blob(local_path, blob)
                cancellation.check()
            finally:
                local_path.unlink(missing_ok=True)
            entries.append({"path": artifact_path, "blob": blob})
        request = {
            "id": artifact_id,
            "name": target_name,
            "type": artifact_type,
            "version": target_version,
            "description": description,
            "metadata": metadata,
            "aliases": aliases,
            "entries": entries,
        }
        _validate_import_artifact(request)
        cancellation.check()
        client.create_artifact(run_id, request)
        cancellation.check()


def _source_artifact_identity(source_artifact: Any) -> str:
    source_id = _optional_text(source_artifact, "id") or _optional_text(
        source_artifact, "qualified_name"
    )
    if not source_id:
        raise WandbImportError("W&B artifact has no stable identity")
    return source_id


def _history_page_size(source: Any) -> int:
    history_rows = _history_line_count(source)
    last_step = getattr(source, "lastHistoryStep", None)
    if (
        isinstance(last_step, bool)
        or not isinstance(last_step, (int, float))
        or (isinstance(last_step, float) and not math.isfinite(last_step))
        or int(last_step) != last_step
        or last_step < -1
        or last_step > MAX_SAFE_INTEGER
        or (last_step == -1 and history_rows != 0)
    ):
        raise WandbImportError("W&B lastHistoryStep is invalid or outside the supported bound")
    if last_step == -1:
        return _MIN_HISTORY_PAGE_SIZE
    span = int(last_step) + 1
    page_count = max(
        _TARGET_HISTORY_PAGE_COUNT,
        math.ceil(history_rows / _TARGET_HISTORY_ROWS_PER_PAGE),
    )
    page_count = min(page_count, _MAX_HISTORY_PAGE_COUNT)
    return max(
        _MIN_HISTORY_PAGE_SIZE,
        math.ceil(span / page_count),
    )


def _history_line_count(source: Any) -> int:
    value = getattr(source, "historyLineCount", None)
    if value is None:
        attributes = getattr(source, "_attrs", None)
        if isinstance(attributes, Mapping):
            value = attributes.get("historyLineCount")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise WandbImportError("W&B historyLineCount is required to bound history scan responses")
    if isinstance(value, float) and not math.isfinite(value):
        raise WandbImportError("W&B historyLineCount is required to bound history scan responses")
    if int(value) != value or value < 0:
        raise WandbImportError("W&B historyLineCount is required to bound history scan responses")
    rows = int(value)
    if rows > _MAX_HISTORY_RESPONSE_ROWS:
        raise WandbImportError(
            f"W&B historyLineCount exceeds the supported {_MAX_HISTORY_RESPONSE_ROWS}-row "
            "history response bound"
        )
    return rows


def _validate_import_artifact(request: dict[str, Any]) -> None:
    version = request.get("version")
    if version is not None and (
        isinstance(version, bool)
        or not isinstance(version, int)
        or not 0 <= version <= MAX_SAFE_INTEGER
    ):
        raise WandbImportError(
            f"artifact version must be an integer between 0 and {MAX_SAFE_INTEGER}"
        )
    aliases = request["aliases"]
    entries = request["entries"]
    if len(aliases) > MAX_ARTIFACT_ALIASES:
        raise WandbImportError(f"artifact cannot contain more than {MAX_ARTIFACT_ALIASES} aliases")
    for alias in aliases:
        _validate_component(alias, "artifact alias", MAX_ARTIFACT_ALIAS_BYTES)
    if len(entries) > MAX_ARTIFACT_ENTRIES:
        raise WandbImportError(f"artifact cannot contain more than {MAX_ARTIFACT_ENTRIES} entries")
    if _json_size(request["metadata"]) > MAX_ARTIFACT_METADATA_BYTES:
        raise WandbImportError(f"artifact metadata exceeds {MAX_ARTIFACT_METADATA_BYTES} bytes")
    _validate_manifest_size(request)


def _history_values(
    row: Mapping[str, Any],
) -> tuple[dict[str, float | bool], list[tuple[str, str, dict[str, Any]]], int]:
    metrics: dict[str, float | bool] = {}
    media: list[tuple[str, str, dict[str, Any]]] = []
    skipped = 0
    visited_nodes = 0

    def children(
        value: Mapping[Any, Any],
        prefix: str,
        *,
        skip_private: bool,
    ) -> Iterator[tuple[str, Any]]:
        for raw_key, child in value.items():
            child_key = str(raw_key)
            if skip_private and child_key.startswith("_"):
                continue
            key = f"{prefix}/{child_key}" if prefix else child_key
            _validate_source_history_key(key)
            yield key, child

    stack: list[tuple[Iterator[tuple[str, Any]], int]] = [(children(row, "", skip_private=True), 0)]
    while stack:
        iterator, depth = stack[-1]
        try:
            prefix, value = next(iterator)
        except StopIteration:
            stack.pop()
            continue
        visited_nodes += 1
        if visited_nodes > _MAX_SOURCE_ROW_NODES:
            raise WandbImportError(
                f"W&B history row exceeds {_MAX_SOURCE_ROW_NODES} traversed values"
            )
        if isinstance(value, bool):
            if prefix not in metrics and len(metrics) >= _MAX_SOURCE_ROW_METRICS:
                raise WandbImportError(
                    f"W&B history row exceeds {_MAX_SOURCE_ROW_METRICS} scalar metrics"
                )
            metrics[prefix] = float(value)
        elif isinstance(value, (int, float)):
            try:
                number = float(value)
            except OverflowError:
                skipped += 1
                continue
            if not math.isfinite(number):
                skipped += 1
            else:
                if prefix not in metrics and len(metrics) >= _MAX_SOURCE_ROW_METRICS:
                    raise WandbImportError(
                        f"W&B history row exceeds {_MAX_SOURCE_ROW_METRICS} scalar metrics"
                    )
                metrics[prefix] = number
        elif isinstance(value, Mapping):
            value_type = value.get("_type")
            if value_type is not None:
                kind = _MEDIA_KINDS.get(str(value_type))
                path = value.get("path")
                if kind is None or not isinstance(path, str) or not path:
                    skipped += 1
                else:
                    if len(media) >= _MAX_SOURCE_ROW_MEDIA:
                        raise WandbImportError(
                            f"W&B history row exceeds {_MAX_SOURCE_ROW_MEDIA} media values"
                        )
                    reference = _bounded_media_reference(value, path)
                    media.append((prefix, kind, reference))
                continue
            if depth >= _MAX_SOURCE_ROW_DEPTH:
                raise WandbImportError(
                    f"W&B history row nesting exceeds {_MAX_SOURCE_ROW_DEPTH} levels"
                )
            stack.append((children(value, prefix, skip_private=False), depth + 1))
        elif value is not None:
            skipped += 1
    return metrics, media, skipped


def _validate_source_history_key(key: str) -> None:
    encoded = key.encode("utf-8")
    if (
        not encoded
        or len(encoded) > _MAX_METRIC_KEY_BYTES
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in key)
    ):
        raise WandbImportError(
            f"W&B metric or media key {key!r} must contain 1 to "
            f"{_MAX_METRIC_KEY_BYTES} non-control bytes"
        )


def _bounded_media_reference(value: Mapping[Any, Any], path: str) -> dict[str, Any]:
    reference: dict[str, Any] = {"path": _safe_artifact_path(path)}
    expected_digest = value.get("sha256")
    if isinstance(expected_digest, str):
        reference["sha256"] = expected_digest
    for name in ("caption", "width", "height", "duration"):
        item = value.get(name)
        if isinstance(item, str):
            reference[name] = item
        elif isinstance(item, (int, float)) and not isinstance(item, bool):
            try:
                number = float(item)
            except OverflowError:
                continue
            if math.isfinite(number):
                reference[name] = item
    if _json_size(reference) > _MAX_SOURCE_MEDIA_REFERENCE_BYTES:
        raise WandbImportError(
            "W&B media reference exceeds "
            f"{_MAX_SOURCE_MEDIA_REFERENCE_BYTES} serialized bytes for {path!r}"
        )
    return reference


def _import_media_reference(
    source_file: Any,
    client: EpochDeckClient,
    *,
    run_id: str,
    source_metadata: dict[str, str],
    key: str,
    kind: str,
    step: int,
    timestamp_ms: int,
    artifact_path: str,
    reference: dict[str, Any],
    row_number: int,
    occurrence: int,
    cancellation: ImportCancellation,
) -> None:
    cancellation.check()
    with tempfile.TemporaryDirectory(prefix="epochdeck-wandb-media-") as raw_root:
        root = Path(raw_root).resolve()
        downloaded = _retry_wandb_read(
            partial(source_file.download, root=str(root), replace=True),
            cancellation,
            f"W&B media download for {artifact_path!r}",
        )
        cancellation.check()
        local_path = _downloaded_path(downloaded, root, artifact_path)
        blob = _blob_for_file(local_path, artifact_path, cancellation)
        expected_digest = reference.get("sha256")
        if isinstance(expected_digest, str) and expected_digest != blob["digest"]:
            raise WandbImportError(
                f"W&B media digest mismatch for {artifact_path!r}: "
                f"expected {expected_digest}, received {blob['digest']}"
            )
        cancellation.check()
        client.upload_blob(local_path, blob)
        cancellation.check()
    metadata: dict[str, Any] = {
        "wandb_path": artifact_path,
        "wandb_source": source_metadata,
    }
    for name in ("caption", "width", "height", "duration"):
        value = reference.get(name)
        if isinstance(value, (str, int, float)) and not isinstance(value, bool):
            metadata[name] = value
    value_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"https://wandb.ai/{source_metadata['entity']}/{source_metadata['project']}"
            f"/runs/{source_metadata['run_id']}/history/{row_number}/media/{occurrence}"
            f"/{key}/{step}/{blob['digest']}",
        )
    )
    cancellation.check()
    client.create_rich_value(
        run_id,
        {
            "id": value_id,
            "key": key,
            "kind": kind,
            "step": step,
            "timestamp_ms": timestamp_ms,
            "blob": blob,
            "metadata": metadata,
        },
    )
    cancellation.check()


def _history_step(row: Mapping[str, Any], row_index: int) -> int:
    value = row.get("_step", row_index)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or (isinstance(value, float) and not math.isfinite(value))
        or int(value) != value
    ):
        raise WandbImportError(f"W&B history row {row_index} has an invalid _step")
    step = int(value)
    if step < 0 or step > MAX_SAFE_INTEGER:
        raise WandbImportError(
            f"W&B history row {row_index} has an _step outside 0..{MAX_SAFE_INTEGER}"
        )
    return step


def _history_timestamp_ms(row: Mapping[str, Any], row_index: int) -> int:
    value = row.get("_timestamp")
    if value is None:
        return row_index
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or (isinstance(value, float) and not math.isfinite(value))
    ):
        raise WandbImportError(f"W&B history row {row_index} has an invalid _timestamp")
    try:
        timestamp_ms = round(float(value) * 1_000)
    except OverflowError as error:
        raise WandbImportError(f"W&B history row {row_index} has an invalid _timestamp") from error
    if timestamp_ms < 0 or timestamp_ms > MAX_SAFE_INTEGER:
        raise WandbImportError(
            f"W&B history row {row_index} has an _timestamp outside 0..{MAX_SAFE_INTEGER}"
        )
    return timestamp_ms


def _history_row_identity(row: Any) -> str:
    if not isinstance(row, Mapping):
        raise WandbImportError("W&B history yielded a non-object row")
    positions = [row.get(name) for name in ("_step", "_timestamp")]
    try:
        return json.dumps(positions, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise WandbImportError("W&B history row has invalid position metadata") from error


def _downloaded_path(downloaded: Any, root: Path, artifact_path: str) -> Path:
    named = getattr(downloaded, "name", None)
    candidates = [Path(named)] if isinstance(named, str) else []
    candidates.append(root / artifact_path)
    for candidate in candidates:
        resolved = candidate.resolve()
        if resolved.is_relative_to(root) and resolved.is_file():
            return resolved
    raise WandbImportError(f"W&B file download did not produce {artifact_path!r}")


def _safe_artifact_path(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or len(value.encode()) > 1_024
        or value.startswith("/")
        or "\\" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or str(path) != value
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise WandbImportError(f"W&B file has an unsafe artifact path: {value!r}")
    return value


def _hash_file(path: Path, cancellation: ImportCancellation) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(_HASH_CHUNK_BYTES):
            cancellation.check()
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _blob_for_file(
    path: Path,
    artifact_path: str,
    cancellation: ImportCancellation,
) -> dict[str, Any]:
    file_name = PurePosixPath(artifact_path).name
    validate_blob_file_name(file_name)
    digest, size = _hash_file(path, cancellation)
    mime_type = mimetypes.guess_type(artifact_path)[0] or "application/octet-stream"
    return {
        "digest": digest,
        "size": size,
        "mime_type": mime_type,
        "file_name": file_name,
    }


def _json_document(value: dict[str, Any], name: str, maximum: int) -> dict[str, Any]:
    try:
        encoded = json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        decoded = json.loads(encoded)
    except (TypeError, ValueError) as error:
        raise WandbImportError(f"{name} is not bounded JSON: {error}") from error
    if len(encoded) > maximum:
        raise WandbImportError(f"serialized {name} exceeds {maximum} bytes")
    if not isinstance(decoded, dict):
        raise WandbImportError(f"{name} must be an object")
    return decoded


def _wandb_document(value: Any, name: str, maximum: int) -> dict[str, Any]:
    normalized, _ = _wandb_json_value(value, name=name, maximum=maximum)
    if not isinstance(normalized, dict):
        raise WandbImportError(f"{name} must be an object")
    return _json_document(normalized, name, maximum)


def _wandb_json_value(
    value: Any,
    *,
    name: str,
    maximum: int,
    depth: int = 0,
) -> tuple[Any, int]:
    if depth > 64:
        raise WandbImportError("W&B metadata nesting exceeds 64 levels")
    json_dict = getattr(value, "_json_dict", None)
    if isinstance(json_dict, Mapping):
        value = json_dict
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        size = 2
        for raw_key, child in value.items():
            key = str(raw_key)
            if key in normalized:
                raise WandbImportError(f"{name} has duplicate keys after string normalization")
            key_size = _bounded_json_fragment_size(key, name, maximum)
            normalized_child, child_size = _wandb_json_value(
                child,
                name=name,
                maximum=maximum,
                depth=depth + 1,
            )
            size += (1 if normalized else 0) + key_size + 1 + child_size
            _validate_wandb_document_size(size, name, maximum)
            normalized[key] = normalized_child
        return normalized, size
    if isinstance(value, (list, tuple)):
        normalized_list: list[Any] = []
        size = 2
        for child in value:
            normalized_child, child_size = _wandb_json_value(
                child,
                name=name,
                maximum=maximum,
                depth=depth + 1,
            )
            size += (1 if normalized_list else 0) + child_size
            _validate_wandb_document_size(size, name, maximum)
            normalized_list.append(normalized_child)
        return normalized_list, size
    return value, _bounded_json_fragment_size(value, name, maximum)


def _bounded_json_fragment_size(value: Any, name: str, maximum: int) -> int:
    if isinstance(value, str) and len(value) > maximum:
        raise WandbImportError(f"serialized {name} exceeds {maximum} bytes")
    try:
        size = len(
            json.dumps(
                value,
                allow_nan=False,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
    except (TypeError, ValueError) as error:
        raise WandbImportError(f"{name} is not bounded JSON: {error}") from error
    _validate_wandb_document_size(size, name, maximum)
    return size


def _validate_wandb_document_size(size: int, name: str, maximum: int) -> None:
    if size > maximum:
        raise WandbImportError(f"serialized {name} exceeds {maximum} bytes")


def _json_size(value: dict[str, Any]) -> int:
    return len(
        json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            sort_keys=True,
        ).encode()
    )


def _required_text(value: Any, field: str) -> str:
    result = getattr(value, field, None)
    if not isinstance(result, str) or not result:
        raise WandbImportError(f"W&B object has no non-empty {field}")
    return result


def _optional_text(value: Any, field: str) -> str:
    result = getattr(value, field, None)
    if result is None:
        return ""
    if not isinstance(result, str):
        raise WandbImportError(f"W&B object has invalid {field}")
    return result


def _source_revision(source: Any) -> _SourceRevision:
    state = _required_text(source, "state").lower()
    return _SourceRevision(
        state=state,
        updated_at=_source_updated_at(source),
    )


def _source_updated_at(source: Any) -> str:
    missing = object()
    public_value = getattr(source, "updated_at", missing)
    if public_value is not missing and public_value is not None:
        return _bounded_revision_text(public_value, "updated_at")

    attributes = getattr(source, "_attrs", None)
    if not isinstance(attributes, Mapping):
        raise WandbImportError(
            "W&B run has no source revision timestamp; expected updated_at or "
            "_attrs['updatedAt']/_attrs['heartbeatAt']"
        )
    for field in ("updatedAt", "heartbeatAt"):
        if field in attributes and attributes[field] is not None:
            return _bounded_revision_text(attributes[field], f"_attrs[{field!r}]")
    raise WandbImportError(
        "W&B run has no source revision timestamp; expected updated_at or "
        "_attrs['updatedAt']/_attrs['heartbeatAt']"
    )


def _bounded_revision_text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise WandbImportError(f"W&B run has invalid source revision {field}")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise WandbImportError(f"W&B run has invalid source revision {field}") from error
    if len(encoded) > _MAX_SOURCE_REVISION_BYTES:
        raise WandbImportError(
            f"W&B run source revision {field} exceeds {_MAX_SOURCE_REVISION_BYTES} bytes"
        )
    return value


def _source_changed_error(source_id: str, when: str) -> WandbImportError:
    return WandbImportError(
        f"W&B run {source_id!r} changed {when}; the deterministic EpochDeck target may "
        "already contain data from the prior source revision. A safe retry requires the "
        "same W&B snapshot; otherwise remove both the partial target run and its checkpoint "
        "state before restarting."
    )


def _same_wandb_source(existing: Any, expected: dict[str, str]) -> bool:
    if not isinstance(existing, Mapping):
        return False
    return all(existing.get(field) == expected[field] for field in ("entity", "project", "run_id"))


def _object(value: dict[str, Any], field: str) -> dict[str, Any]:
    result = value.get(field)
    if not isinstance(result, dict):
        raise WandbImportError(f"EpochDeck response has no {field} object")
    return result


def _state_int(state: dict[str, Any], field: str, default: int) -> int:
    value = state.get(field, default)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise WandbImportError(f"checkpoint field {field!r} is invalid")
    return value


def _checkpoint_phase(state: dict[str, Any]) -> str:
    phase = state.get("phase")
    if not isinstance(phase, str) or phase not in _IMPORT_PHASES:
        raise WandbImportError("checkpoint has no valid W&B import phase")
    return phase


def _checkpoint_include_files(state: dict[str, Any]) -> bool:
    include_files = state.get("include_files")
    if not isinstance(include_files, bool):
        raise WandbImportError("checkpoint has no valid include_files import contract")
    return include_files
