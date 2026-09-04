from __future__ import annotations

import json
import os
import shutil
import threading
import uuid
from collections.abc import Mapping
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Any

from epochdeck._metrics import normalize_metrics
from epochdeck._platform_fs import (
    ACCESS_MODE,
    open_regular_file_descriptor,
    sync_directory,
    verify_directory,
)
from epochdeck._protocol import DeliveryError, encode_json_request
from epochdeck._summary import MAX_DERIVED_SUMMARY_KEYS, merge_metric_preview

_MAX_RUN_METADATA_BYTES = 1024 * 1024
_MAX_DELIVERY_BYTES = 4 * 1024
_MAX_ACK_BYTES = 64
_MAX_JOURNAL_RECORD_BYTES = 2 * 1024 * 1024


class _Spool:
    def __init__(self, root: Path, run_id: str) -> None:
        try:
            canonical_run_id = str(uuid.UUID(run_id))
        except (AttributeError, TypeError, ValueError) as error:
            raise DeliveryError("spool run ID must be a canonical UUID") from error
        if canonical_run_id != run_id or Path(run_id).name != run_id:
            raise DeliveryError("spool run ID must be a canonical UUID")
        self.directory = root / run_id
        _ensure_private_directory(self.directory, parents=True)
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
        self._lock = threading.Lock()
        for path in (
            self.events_path,
            self.alerts_path,
            self.rich_values_path,
            self.artifacts_path,
        ):
            _ensure_private_file(path)
        _ensure_private_directory(self.blob_root, parents=False)

    def read_metadata(self) -> dict[str, Any] | None:
        with self._lock:
            if not _path_exists(self.metadata_path):
                return None
            return self._read_json_object(
                self.metadata_path,
                "run metadata",
                _MAX_RUN_METADATA_BYTES,
            )

    def write_metadata(self, metadata: dict[str, Any]) -> None:
        with self._lock:
            _atomic_json_write(self.metadata_path, metadata, _MAX_RUN_METADATA_BYTES)

    def update_metadata(self, updates: Mapping[str, Any]) -> None:
        with self._lock:
            metadata = self._read_json_object(
                self.metadata_path,
                "run metadata",
                _MAX_RUN_METADATA_BYTES,
            )
            metadata.update(deepcopy(dict(updates)))
            _atomic_json_write(self.metadata_path, metadata, _MAX_RUN_METADATA_BYTES)

    def append(self, point: dict[str, Any]) -> int:
        return self._append_record(self.events_path, point)

    def append_alert(self, alert: dict[str, Any]) -> None:
        self._append_record(self.alerts_path, alert)

    def append_rich_value(self, value: dict[str, Any]) -> None:
        self._append_record(self.rich_values_path, value)

    def append_artifact(self, artifact: dict[str, Any]) -> None:
        self._append_record(self.artifacts_path, artifact)

    def _append_record(self, path: Path, record: dict[str, Any]) -> int:
        encoded = encode_json_request(record)
        if len(encoded) > _MAX_JOURNAL_RECORD_BYTES:
            raise DeliveryError(f"journal record exceeds {_MAX_JOURNAL_RECORD_BYTES} bytes: {path}")
        with self._lock, _open_regular_file(path, os.O_WRONLY | os.O_APPEND) as stream:
            stream.write(encoded)
            stream.write(b"\n")
            stream.flush()
            os.fsync(stream.fileno())
            return stream.tell()

    def reclaim_delivered(self) -> None:
        with self._lock:
            if any(
                self._read_ack(ack_path, journal_path) < _regular_file_size(journal_path)
                for ack_path, journal_path in self._journal_pairs()
            ):
                raise DeliveryError("cannot reclaim a spool with undelivered records")
            for ack_path, journal_path in self._journal_pairs():
                with _open_regular_file(
                    journal_path,
                    os.O_WRONLY | os.O_TRUNC,
                ) as stream:
                    stream.flush()
                    os.fsync(stream.fileno())
                ack_path.unlink(missing_ok=True)
            for delivery_path in (
                self.delivery_path,
                self.alert_delivery_path,
                self.rich_delivery_path,
                self.artifact_delivery_path,
            ):
                delivery_path.unlink(missing_ok=True)
            _verify_directory(self.blob_root)
            shutil.rmtree(self.blob_root)
            _ensure_private_directory(self.blob_root, parents=False)
            _fsync_directory(self.directory)

    def _journal_pairs(self) -> tuple[tuple[Path, Path], ...]:
        return (
            (self.ack_path, self.events_path),
            (self.alert_ack_path, self.alerts_path),
            (self.rich_ack_path, self.rich_values_path),
            (self.artifact_ack_path, self.artifacts_path),
        )

    def read_batch(
        self,
        limit: int,
        *,
        request_byte_budget: int,
    ) -> tuple[list[dict[str, Any]], int]:
        return self._read_record_batch(
            self.events_path,
            self.ack_path,
            self.delivery_path,
            limit,
            metric_request_byte_budget=request_byte_budget,
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

    def _read_record_batch(
        self,
        journal_path: Path,
        ack_path: Path,
        delivery_path: Path,
        limit: int,
        *,
        metric_request_byte_budget: int | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            offset = self._read_ack(ack_path, journal_path)
            size = _regular_file_size(journal_path)
            delivery = self._read_delivery(delivery_path, offset, size)
            fixed_end = int(delivery["end_offset"]) if delivery is not None else None
            points: list[dict[str, Any]] = []
            encoded_point_bytes = 0
            with _open_regular_file(journal_path, os.O_RDONLY) as stream:
                stream.seek(offset)
                while len(points) < limit or fixed_end is not None:
                    if fixed_end is not None and stream.tell() >= fixed_end:
                        break
                    record_start = stream.tell()
                    line = _read_journal_line(stream, journal_path)
                    if not line:
                        break
                    if fixed_end is not None and stream.tell() > fixed_end:
                        raise DeliveryError(
                            f"delivery boundary splits a journal record: {delivery_path}"
                        )
                    point = self._decode_record(line, stream.tell(), journal_path)
                    point_bytes = len(line) - 1
                    if metric_request_byte_budget is not None and fixed_end is None:
                        batch_sequence = (
                            point.get("sequence") if not points else points[0].get("sequence")
                        )
                        candidate_size = _metric_request_size(
                            batch_sequence,
                            encoded_point_bytes + point_bytes,
                            len(points) + 1,
                        )
                        if candidate_size > metric_request_byte_budget:
                            if not points:
                                raise DeliveryError(
                                    "metric journal event exceeds the delivery request byte budget"
                                )
                            stream.seek(record_start)
                            break
                    points.append(point)
                    encoded_point_bytes += point_bytes
                next_offset = stream.tell()
            if fixed_end is not None and next_offset != fixed_end:
                raise DeliveryError(f"delivery boundary is outside journal: {delivery_path}")
            encoded_request: bytes | None = None
            if metric_request_byte_budget is not None and points:
                encoded_request = encode_json_request(
                    {"batch_sequence": points[0].get("sequence"), "points": points}
                )
                if len(encoded_request) > metric_request_byte_budget:
                    raise DeliveryError("persisted metric delivery exceeds the request byte budget")
            if points and delivery is None:
                record_identity = points[0].get("sequence", points[0].get("id"))
                if record_identity is None:
                    raise DeliveryError(f"journal record has no durable identity: {journal_path}")
                delivery = {
                    "start_offset": offset,
                    "end_offset": next_offset,
                    "record_identity": str(record_identity),
                }
                if encoded_request is not None:
                    delivery.update(
                        {
                            "request_bytes": len(encoded_request),
                            "request_sha256": sha256(encoded_request).hexdigest(),
                        }
                    )
                _atomic_json_write(delivery_path, delivery, _MAX_DELIVERY_BYTES)
            if points:
                assert delivery is not None
                expected_identity = points[0].get("sequence", points[0].get("id"))
                stored_identity = delivery.get("record_identity")
                if str(stored_identity) != str(expected_identity):
                    raise DeliveryError(
                        f"delivery identity does not match journal: {delivery_path}"
                    )
                if encoded_request is not None:
                    request_bytes = delivery.get("request_bytes")
                    request_sha256 = delivery.get("request_sha256")
                    if (
                        isinstance(request_bytes, bool)
                        or not isinstance(request_bytes, int)
                        or request_bytes != len(encoded_request)
                        or not isinstance(request_sha256, str)
                        or request_sha256 != sha256(encoded_request).hexdigest()
                    ):
                        raise DeliveryError(
                            f"metric delivery identity does not match journal: {delivery_path}"
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

    def _acknowledge(self, ack_path: Path, delivery_path: Path, offset: int) -> None:
        with self._lock:
            if _path_exists(delivery_path):
                delivery = self._read_json_object(
                    delivery_path,
                    "delivery state",
                    _MAX_DELIVERY_BYTES,
                )
                end_offset = delivery.get("end_offset")
                if (
                    isinstance(end_offset, bool)
                    or not isinstance(end_offset, int)
                    or end_offset != offset
                ):
                    raise DeliveryError(
                        f"acknowledgement does not match delivery boundary: {delivery_path}"
                    )
            _atomic_text_write(ack_path, str(offset), _MAX_ACK_BYTES)
            delivery_path.unlink(missing_ok=True)
            _fsync_directory(self.directory)

    def pending(self) -> bool:
        return (
            self.pending_metrics()
            or self.pending_alerts()
            or self.pending_rich_values()
            or self.pending_artifacts()
        )

    def pending_metrics(self) -> bool:
        return self._pending(self.ack_path, self.events_path)

    def pending_alerts(self) -> bool:
        return self._pending(self.alert_ack_path, self.alerts_path)

    def pending_rich_values(self) -> bool:
        return self._pending(self.rich_ack_path, self.rich_values_path)

    def pending_artifacts(self) -> bool:
        return self._pending(self.artifact_ack_path, self.artifacts_path)

    def _pending(self, ack_path: Path, journal_path: Path) -> bool:
        with self._lock:
            return self._read_ack(ack_path, journal_path) < _regular_file_size(journal_path)

    def last_point(self) -> dict[str, Any] | None:
        return self._last_record(self.events_path)

    def last_rich_value(self) -> dict[str, Any] | None:
        return self._last_record(self.rich_values_path)

    def blob_path(self, digest: str) -> Path:
        if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise DeliveryError("rich value blob has an invalid SHA-256 digest")
        _verify_directory(self.blob_root)
        path = self.blob_root / digest
        if _path_exists(path):
            _verify_regular_file(path)
        return path

    def _last_record(self, path: Path) -> dict[str, Any] | None:
        with self._lock, _open_regular_file(path, os.O_RDONLY) as stream:
            stream.seek(0, os.SEEK_END)
            end = stream.tell()
            if end == 0:
                return None
            position = end - 1
            while position > 0:
                if end - position > _MAX_JOURNAL_RECORD_BYTES + 1:
                    raise DeliveryError(
                        f"journal record exceeds {_MAX_JOURNAL_RECORD_BYTES} bytes: {path}"
                    )
                stream.seek(position - 1)
                if stream.read(1) == b"\n" and position < end - 1:
                    break
                position -= 1
            stream.seek(position)
            line = _read_journal_line(stream, path)
        return self._decode_record(line, end, path) if line.strip() else None

    def recover_summary(
        self,
        metric_snapshot: Mapping[str, float],
        summary_truncated: Any,
        summary_event_offset: Any,
        *,
        max_tail_records: int,
        max_tail_bytes: int,
    ) -> tuple[dict[str, float], bool, int]:
        with self._lock:
            size = _regular_file_size(self.events_path)
            if (
                isinstance(summary_event_offset, bool)
                or not isinstance(summary_event_offset, int)
                or summary_event_offset < 0
                or summary_event_offset > size
            ):
                raise DeliveryError(
                    f"summary event offset is outside journal: {self.metadata_path}"
                )
            if max_tail_records < 1 or max_tail_bytes < 1:
                raise ValueError("summary recovery bounds must be positive")
            tail_bytes = size - summary_event_offset
            if tail_bytes > max_tail_bytes:
                raise DeliveryError(
                    f"summary journal tail exceeds {max_tail_bytes} bytes: {self.events_path}"
                )
            if not isinstance(summary_truncated, bool):
                raise DeliveryError(f"summary truncation flag is not boolean: {self.metadata_path}")
            summary = _validate_metric_summary_snapshot(metric_snapshot, self.metadata_path)
            if summary_truncated and len(summary) != MAX_DERIVED_SUMMARY_KEYS:
                raise DeliveryError(
                    f"truncated metric summary snapshot is incomplete: {self.metadata_path}"
                )
            with _open_regular_file(self.events_path, os.O_RDONLY) as stream:
                if summary_event_offset > 0:
                    stream.seek(summary_event_offset - 1)
                    if stream.read(1) != b"\n":
                        raise DeliveryError(
                            f"summary event offset splits a journal record: {self.metadata_path}"
                        )
                stream.seek(summary_event_offset)
                record_count = 0
                while line := _read_journal_line(stream, self.events_path):
                    record_count += 1
                    if record_count > max_tail_records:
                        raise DeliveryError(
                            "summary journal tail exceeds "
                            f"{max_tail_records} records: {self.events_path}"
                        )
                    event = self._decode_record(line, stream.tell(), self.events_path)
                    metrics = event.get("metrics")
                    if not isinstance(metrics, dict):
                        raise DeliveryError(
                            f"journal event has no metric object: {self.events_path}"
                        )
                    validated_metrics = _validate_recovered_metrics(metrics, self.events_path)
                    summary, summary_truncated = merge_metric_preview(
                        summary,
                        validated_metrics,
                        truncated=summary_truncated,
                    )
            return summary, summary_truncated, size

    def _read_delivery(
        self,
        delivery_path: Path,
        offset: int,
        size: int,
    ) -> dict[str, Any] | None:
        if not _path_exists(delivery_path):
            return None
        delivery = self._read_json_object(
            delivery_path,
            "delivery state",
            _MAX_DELIVERY_BYTES,
        )
        try:
            start = delivery["start_offset"]
            end = delivery["end_offset"]
            identity = delivery.get("record_identity")
            if (
                isinstance(start, bool)
                or not isinstance(start, int)
                or isinstance(end, bool)
                or not isinstance(end, int)
                or not isinstance(identity, (str, int))
                or isinstance(identity, bool)
            ):
                raise TypeError
        except (KeyError, TypeError) as error:
            raise DeliveryError(f"invalid delivery state: {delivery_path}") from error
        if end <= offset:
            delivery_path.unlink(missing_ok=True)
            _fsync_directory(self.directory)
            return None
        if start != offset or end <= start or end > size:
            raise DeliveryError(f"delivery state is outside journal: {delivery_path}")
        return delivery

    def _read_json_object(
        self,
        path: Path,
        name: str,
        maximum: int,
    ) -> dict[str, Any]:
        try:
            encoded = _read_bounded_file(path, maximum)
            value = json.loads(encoded)
        except UnicodeDecodeError as error:
            raise DeliveryError(f"invalid {name}: {path}") from error
        except json.JSONDecodeError as error:
            raise DeliveryError(f"invalid {name}: {path}") from error
        if not isinstance(value, dict):
            raise DeliveryError(f"invalid {name}: {path}")
        return value

    def _decode_record(self, line: bytes, offset: int, path: Path) -> dict[str, Any]:
        try:
            value = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DeliveryError(f"invalid journal event ending at byte {offset}: {path}") from error
        if not isinstance(value, dict):
            raise DeliveryError(f"invalid journal event ending at byte {offset}: {path}")
        return value

    def _read_ack(self, ack_path: Path, journal_path: Path) -> int:
        if not _path_exists(ack_path):
            return 0
        try:
            offset = int(_read_bounded_file(ack_path, _MAX_ACK_BYTES).decode("ascii"))
        except (UnicodeDecodeError, ValueError) as error:
            raise DeliveryError(f"invalid spool acknowledgement: {ack_path}") from error
        size = _regular_file_size(journal_path)
        if offset < 0 or offset > size:
            raise DeliveryError(f"spool acknowledgement is outside journal: {ack_path}")
        return offset


def _validate_metric_summary_snapshot(
    snapshot: Mapping[str, float],
    metadata_path: Path,
) -> dict[str, float]:
    if len(snapshot) > MAX_DERIVED_SUMMARY_KEYS:
        raise DeliveryError(f"metric summary snapshot is not bounded: {metadata_path}")
    if not snapshot:
        return {}
    return _validate_recovered_metrics(snapshot, metadata_path)


def _validate_recovered_metrics(
    metrics: Mapping[str, Any],
    path: Path,
) -> dict[str, float]:
    try:
        return normalize_metrics(metrics)
    except (TypeError, ValueError) as error:
        raise DeliveryError(f"journal has an invalid metric point: {path}: {error}") from error


def _metric_request_size(
    batch_sequence: Any,
    encoded_point_bytes: int,
    point_count: int,
) -> int:
    empty_request = encode_json_request({"batch_sequence": batch_sequence, "points": []})
    separators = max(point_count - 1, 0)
    return len(empty_request) + encoded_point_bytes + separators


def _atomic_json_write(path: Path, value: dict[str, Any], maximum: int) -> None:
    try:
        encoded = json.dumps(
            value,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
            ensure_ascii=False,
        )
    except (TypeError, ValueError) as error:
        raise DeliveryError(f"spool metadata is not JSON-compatible: {path}") from error
    _atomic_text_write(path, encoded, maximum)


def _atomic_text_write(path: Path, value: str, maximum: int) -> None:
    encoded = value.encode("utf-8")
    if len(encoded) > maximum:
        raise DeliveryError(f"spool file exceeds {maximum} bytes: {path}")
    _verify_directory(path.parent)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = open_regular_file_descriptor(
            temporary,
            flags,
            private_mode=0o600,
        )
    except OSError as error:
        raise DeliveryError(f"cannot create private spool file: {temporary}") from error
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        _verify_regular_file(path, private=True)
        _fsync_directory(path.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def _path_exists(path: Path) -> bool:
    try:
        os.lstat(path)
    except FileNotFoundError:
        return False
    except OSError as error:
        raise DeliveryError(f"cannot inspect spool path: {path}") from error
    return True


def _ensure_private_directory(path: Path, *, parents: bool) -> None:
    if _path_exists(path):
        _verify_directory(path, private=True)
        return
    try:
        path.mkdir(mode=0o700, parents=parents, exist_ok=False)
    except FileExistsError:
        pass
    except OSError as error:
        raise DeliveryError(f"cannot create private spool directory: {path}") from error
    _verify_directory(path, private=True)


def _verify_directory(path: Path, *, private: bool = False) -> None:
    try:
        verify_directory(path, private_mode=0o700 if private else None)
    except OSError as error:
        raise DeliveryError(f"spool path must be a non-symbolic directory: {path}") from error


def _ensure_private_file(path: Path) -> None:
    flags = os.O_WRONLY | os.O_CREAT
    try:
        descriptor = open_regular_file_descriptor(path, flags, private_mode=0o600)
    except OSError as error:
        raise DeliveryError(f"spool path must be a regular non-symbolic file: {path}") from error
    os.close(descriptor)


def _verify_regular_file(path: Path, *, private: bool = False) -> None:
    try:
        descriptor = open_regular_file_descriptor(
            path,
            os.O_RDONLY,
            private_mode=0o600 if private else None,
        )
    except OSError as error:
        raise DeliveryError(f"spool path must be a regular non-symbolic file: {path}") from error
    os.close(descriptor)


def _open_regular_file(path: Path, flags: int) -> Any:
    try:
        access = flags & ACCESS_MODE
        descriptor = open_regular_file_descriptor(
            path,
            flags,
            private_mode=0o600 if access != os.O_RDONLY else None,
        )
    except OSError as error:
        raise DeliveryError(f"spool path must be a regular non-symbolic file: {path}") from error
    try:
        if access == os.O_RDONLY:
            mode = "rb"
        elif flags & os.O_APPEND:
            mode = "ab"
        else:
            mode = "wb"
        return os.fdopen(descriptor, mode)
    except BaseException:
        os.close(descriptor)
        raise


def _regular_file_size(path: Path) -> int:
    try:
        descriptor = open_regular_file_descriptor(path, os.O_RDONLY)
    except OSError as error:
        raise DeliveryError(f"spool path must be a regular non-symbolic file: {path}") from error
    try:
        return os.fstat(descriptor).st_size
    finally:
        os.close(descriptor)


def _read_bounded_file(path: Path, maximum: int) -> bytes:
    with _open_regular_file(path, os.O_RDONLY) as stream:
        encoded = stream.read(maximum + 1)
    if len(encoded) > maximum:
        raise DeliveryError(f"spool file exceeds {maximum} bytes: {path}")
    return encoded


def _read_journal_line(stream: Any, path: Path) -> bytes:
    line = stream.readline(_MAX_JOURNAL_RECORD_BYTES + 2)
    if not line:
        return b""
    if len(line) > _MAX_JOURNAL_RECORD_BYTES + 1:
        raise DeliveryError(f"journal record exceeds {_MAX_JOURNAL_RECORD_BYTES} bytes: {path}")
    if not line.endswith(b"\n"):
        raise DeliveryError(f"journal record is incomplete: {path}")
    return line


def _fsync_directory(path: Path) -> None:
    try:
        sync_directory(path)
    except OSError as error:
        raise DeliveryError(f"spool path must be a non-symbolic directory: {path}") from error
