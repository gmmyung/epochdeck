from __future__ import annotations

import hashlib
import json
import math
import mimetypes
import os
import struct
import tempfile
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO

_COPY_CHUNK_BYTES = 1024 * 1024
_MAX_TABLE_COLUMNS = 1_024
_MAX_TABLE_ROW_BYTES = 1024 * 1024
_MAX_TABLE_PREVIEW_BYTES = 64 * 1024
_MAX_HISTOGRAM_BINS = 512


@dataclass(frozen=True, slots=True)
class PreparedRichValue:
    kind: str
    blob: dict[str, Any] | None
    metadata: dict[str, Any]


class RichValue(ABC):
    @abstractmethod
    def _prepare(self, blob_root: Path) -> PreparedRichValue:
        raise NotImplementedError


class _Media(RichValue):
    kind: str
    default_mime_type: str

    def __init__(
        self,
        data: str | Path | bytes | bytearray | memoryview,
        *,
        caption: str | None = None,
        mime_type: str | None = None,
    ) -> None:
        if not isinstance(data, (str, Path, bytes, bytearray, memoryview)):
            raise TypeError(f"{self.kind} data must be a path or bytes")
        if caption is not None and not isinstance(caption, str):
            raise TypeError(f"{self.kind} caption must be a string or None")
        self._data = data
        self._caption = caption
        self._mime_type = mime_type

    def _prepare(self, blob_root: Path) -> PreparedRichValue:
        file_name: str | None = None
        if isinstance(self._data, (str, Path)):
            path = Path(self._data).expanduser()
            if not path.is_file():
                raise FileNotFoundError(f"{self.kind} file was not found: {path}")
            file_name = path.name
            guessed_mime, _ = mimetypes.guess_type(path.name)
            with path.open("rb") as source:
                digest, size = _install_stream(blob_root, source)
        else:
            data = bytes(self._data)
            digest, size = _install_bytes(blob_root, data)
            guessed_mime = None
        mime_type = self._mime_type or guessed_mime or self.default_mime_type
        _validate_mime_type(mime_type)
        metadata = {"caption": self._caption} if self._caption is not None else {}
        return PreparedRichValue(
            kind=self.kind,
            blob={
                "digest": digest,
                "size": size,
                "mime_type": mime_type,
                "file_name": file_name,
            },
            metadata=metadata,
        )


class Image(_Media):
    kind = "image"
    default_mime_type = "image/png"


class Audio(_Media):
    kind = "audio"
    default_mime_type = "audio/wav"

    def __init__(
        self,
        data: str | Path | bytes | bytearray | memoryview,
        *,
        caption: str | None = None,
        sample_rate: int | None = None,
        mime_type: str | None = None,
    ) -> None:
        super().__init__(data, caption=caption, mime_type=mime_type)
        if sample_rate is not None and (isinstance(sample_rate, bool) or sample_rate <= 0):
            raise ValueError("audio sample_rate must be a positive integer or None")
        self._sample_rate = sample_rate

    def _prepare(self, blob_root: Path) -> PreparedRichValue:
        prepared = super()._prepare(blob_root)
        if self._sample_rate is None:
            return prepared
        return PreparedRichValue(
            kind=prepared.kind,
            blob=prepared.blob,
            metadata={**prepared.metadata, "sample_rate": self._sample_rate},
        )


class Video(_Media):
    kind = "video"
    default_mime_type = "video/mp4"


class Table(RichValue):
    def __init__(self, *, columns: Sequence[str], data: Iterable[Sequence[Any]]) -> None:
        normalized_columns = tuple(columns)
        if not normalized_columns or len(normalized_columns) > _MAX_TABLE_COLUMNS:
            raise ValueError(f"table must contain 1 to {_MAX_TABLE_COLUMNS} columns")
        if any(not isinstance(column, str) or not column for column in normalized_columns):
            raise TypeError("table columns must be non-empty strings")
        self.columns = normalized_columns
        self._data = data

    def _prepare(self, blob_root: Path) -> PreparedRichValue:
        blob_root.mkdir(parents=True, exist_ok=True)
        temporary = tempfile.NamedTemporaryFile(  # noqa: SIM115 - closed before CAS install
            mode="w+b",
            prefix="table-",
            suffix=".tmp",
            dir=blob_root,
            delete=False,
        )
        temporary_path = Path(temporary.name)
        digest = hashlib.sha256()
        size = 0
        row_count = 0
        preview: list[list[Any]] = []
        preview_size = 2
        try:

            def write(value: bytes) -> None:
                nonlocal size
                temporary.write(value)
                digest.update(value)
                size += len(value)

            write(b'{"columns":')
            write(_json_bytes(list(self.columns)))
            write(b',"data":[')
            for raw_row in self._data:
                row = list(raw_row)
                if len(row) != len(self.columns):
                    raise ValueError(
                        f"table row {row_count} has {len(row)} cells; expected {len(self.columns)}"
                    )
                encoded = _json_bytes(row)
                if len(encoded) > _MAX_TABLE_ROW_BYTES:
                    raise ValueError(f"serialized table row exceeds {_MAX_TABLE_ROW_BYTES} bytes")
                if row_count:
                    write(b",")
                write(encoded)
                if preview_size + len(encoded) <= _MAX_TABLE_PREVIEW_BYTES:
                    preview.append(row)
                    preview_size += len(encoded) + 1
                row_count += 1
            write(b"]}")
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary.close()
            digest_hex = digest.hexdigest()
            _install_temporary(blob_root, temporary_path, digest_hex)
        except Exception:
            temporary.close()
            temporary_path.unlink(missing_ok=True)
            raise
        return PreparedRichValue(
            kind="table",
            blob={
                "digest": digest_hex,
                "size": size,
                "mime_type": "application/vnd.runloom.table+json",
                "file_name": None,
            },
            metadata={
                "columns": list(self.columns),
                "row_count": row_count,
                "preview": preview,
            },
        )


class Histogram(RichValue):
    def __init__(
        self,
        values: Iterable[float] | None = None,
        *,
        np_histogram: tuple[Sequence[float], Sequence[float]] | None = None,
        num_bins: int = 64,
    ) -> None:
        if (values is None) == (np_histogram is None):
            raise ValueError("histogram requires exactly one of values or np_histogram")
        if isinstance(num_bins, bool) or not 1 <= num_bins <= _MAX_HISTOGRAM_BINS:
            raise ValueError(f"histogram num_bins must be between 1 and {_MAX_HISTOGRAM_BINS}")
        if np_histogram is not None:
            counts, edges = np_histogram
            self._counts = [_finite_float(value, "histogram count") for value in counts]
            self._edges = [_finite_float(value, "histogram edge") for value in edges]
            if not self._counts or len(self._edges) != len(self._counts) + 1:
                raise ValueError("histogram edges must contain exactly one more value than counts")
            if len(self._counts) > _MAX_HISTOGRAM_BINS:
                raise ValueError(f"histogram cannot exceed {_MAX_HISTOGRAM_BINS} bins")
            self._values: Iterable[float] | None = None
            self._num_bins = len(self._counts)
        else:
            self._values = values
            self._num_bins = num_bins
            self._counts: list[float] | None = None
            self._edges: list[float] | None = None

    def _prepare(self, blob_root: Path) -> PreparedRichValue:
        if self._counts is None or self._edges is None:
            blob_root.mkdir(parents=True, exist_ok=True)
            minimum = math.inf
            maximum = -math.inf
            value_count = 0
            with tempfile.TemporaryFile(dir=blob_root) as values_file:
                assert self._values is not None
                for raw_value in self._values:
                    value = _finite_float(raw_value, "histogram value")
                    values_file.write(struct.pack("<d", value))
                    minimum = min(minimum, value)
                    maximum = max(maximum, value)
                    value_count += 1
                if value_count == 0:
                    raise ValueError("histogram values cannot be empty")
                width = (maximum - minimum) / self._num_bins if maximum != minimum else 1.0
                edges = [minimum + width * index for index in range(self._num_bins + 1)]
                if maximum == minimum:
                    edges = [
                        minimum - 0.5 + index / self._num_bins
                        for index in range(self._num_bins + 1)
                    ]
                counts = [0.0] * self._num_bins
                values_file.seek(0)
                while encoded := values_file.read(8):
                    value = struct.unpack("<d", encoded)[0]
                    index = min(
                        int((value - edges[0]) / (edges[-1] - edges[0]) * self._num_bins),
                        self._num_bins - 1,
                    )
                    counts[index] += 1.0
            self._counts = counts
            self._edges = edges
            self._values = None
        return PreparedRichValue(
            kind="histogram",
            blob=None,
            metadata={"counts": self._counts, "edges": self._edges},
        )


def _install_bytes(blob_root: Path, data: bytes) -> tuple[str, int]:
    digest = hashlib.sha256(data).hexdigest()
    blob_root.mkdir(parents=True, exist_ok=True)
    final_path = blob_root / digest
    if not final_path.exists():
        temporary = tempfile.NamedTemporaryFile(  # noqa: SIM115 - closed before CAS install
            dir=blob_root, delete=False
        )
        temporary_path = Path(temporary.name)
        try:
            temporary.write(data)
            temporary.flush()
            os.fsync(temporary.fileno())
            temporary.close()
            _install_temporary(blob_root, temporary_path, digest)
        except Exception:
            temporary.close()
            temporary_path.unlink(missing_ok=True)
            raise
    return digest, len(data)


def _install_stream(blob_root: Path, source: BinaryIO) -> tuple[str, int]:
    blob_root.mkdir(parents=True, exist_ok=True)
    temporary = tempfile.NamedTemporaryFile(  # noqa: SIM115 - closed before CAS install
        dir=blob_root, delete=False
    )
    temporary_path = Path(temporary.name)
    digest = hashlib.sha256()
    size = 0
    try:
        while chunk := source.read(_COPY_CHUNK_BYTES):
            temporary.write(chunk)
            digest.update(chunk)
            size += len(chunk)
        temporary.flush()
        os.fsync(temporary.fileno())
        temporary.close()
        digest_hex = digest.hexdigest()
        _install_temporary(blob_root, temporary_path, digest_hex)
    except Exception:
        temporary.close()
        temporary_path.unlink(missing_ok=True)
        raise
    return digest_hex, size


def _install_temporary(blob_root: Path, temporary_path: Path, digest: str) -> None:
    final_path = blob_root / digest
    try:
        os.link(temporary_path, final_path)
    except FileExistsError:
        pass
    finally:
        temporary_path.unlink(missing_ok=True)


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise TypeError(f"table data must be JSON-compatible: {error}") from error


def _validate_mime_type(value: str) -> None:
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode("utf-8")) > 256
        or "/" not in value
    ):
        raise ValueError("mime_type must be a valid non-empty media type")


def _finite_float(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be numeric")
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"{name} must be finite")
    return number
