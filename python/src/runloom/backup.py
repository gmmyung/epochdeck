from __future__ import annotations

import fcntl
import hashlib
import json
import os
import shutil
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, TextIO

_FORMAT_VERSION = 1
_COPY_CHUNK_BYTES = 1024 * 1024
_MAX_FILES = 10_000_000
_MAX_TREE_DEPTH = 128


class BackupError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class StorageRoots:
    data: Path
    metrics: Path
    blobs: Path

    @classmethod
    def from_environment(cls) -> StorageRoots:
        data = Path(os.environ.get("RUNLOOM_DATA_DIR", "./data")).expanduser().resolve()
        metrics = (
            Path(os.environ.get("RUNLOOM_METRICS_DIR", data / "metrics")).expanduser().resolve()
        )
        blobs = Path(os.environ.get("RUNLOOM_BLOBS_DIR", data / "blobs")).expanduser().resolve()
        return cls(data=data, metrics=metrics, blobs=blobs)

    @property
    def catalog(self) -> Path:
        return self.data / "catalog.sqlite3"

    @property
    def journal(self) -> Path:
        return self.data / "journal"

    @property
    def lock(self) -> Path:
        return self.data / "runloom.lock"


def backup_storage(roots: StorageRoots, destination: Path) -> dict[str, Any]:
    """Copy a stopped server's complete physical state into an atomic bundle."""
    destination = destination.expanduser().resolve()
    _validate_bundle_location(destination, roots)
    if destination.exists():
        raise FileExistsError(f"backup destination already exists: {destination}")
    if not roots.catalog.is_file():
        raise BackupError(f"Runloom catalog was not found: {roots.catalog}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.partial-{uuid.uuid4()}"
    temporary.mkdir()
    try:
        with _storage_lock(roots):
            for directory in ["journal", "metrics", "blobs"]:
                (temporary / directory).mkdir()
            _backup_sqlite(roots.catalog, temporary / "catalog.sqlite3")
            count = 0
            total_bytes = 0
            with (temporary / "files.jsonl").open("w", encoding="utf-8") as inventory:
                catalog_entry = _inventory_existing(
                    temporary / "catalog.sqlite3", "catalog", "catalog.sqlite3"
                )
                _write_json_line(inventory, catalog_entry)
                count += 1
                total_bytes += int(catalog_entry["size"])
                for category, source in [
                    ("journal", roots.journal),
                    ("metrics", roots.metrics),
                    ("blobs", roots.blobs),
                ]:
                    if not source.exists():
                        continue
                    for path, relative in _walk_files(source, skip_staging=category == "blobs"):
                        if count >= _MAX_FILES:
                            raise BackupError(f"backup cannot exceed {_MAX_FILES} files")
                        target = temporary / category / relative
                        entry = _copy_with_digest(path, target, category, relative.as_posix())
                        _write_json_line(inventory, entry)
                        count += 1
                        total_bytes += int(entry["size"])
            manifest = {
                "format": "runloom-physical-backup",
                "format_version": _FORMAT_VERSION,
                "created_at": datetime.now(UTC).isoformat(),
                "file_count": count,
                "total_bytes": total_bytes,
            }
            _write_json(temporary / "manifest.json", manifest)
        os.replace(temporary, destination)
        return manifest
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def restore_storage(bundle: Path, roots: StorageRoots) -> dict[str, Any]:
    """Verify then restore a physical bundle into empty, inactive storage roots."""
    bundle = bundle.expanduser().resolve()
    manifest = _read_manifest(bundle)
    inventory_path = bundle / "files.jsonl"
    with _storage_lock(roots):
        _require_empty_destination(roots)
        verified_count = 0
        verified_bytes = 0
        for entry in _inventory(inventory_path):
            source = _bundle_file(bundle, entry)
            size, digest = _digest_file(source)
            if size != entry["size"] or digest != entry["sha256"]:
                raise BackupError(f"backup verification failed: {source}")
            verified_count += 1
            verified_bytes += size
        if verified_count != manifest["file_count"] or verified_bytes != manifest["total_bytes"]:
            raise BackupError("backup inventory totals do not match the manifest")

        for entry in _inventory(inventory_path):
            source = _bundle_file(bundle, entry)
            destination = _restore_path(roots, entry)
            destination.parent.mkdir(parents=True, exist_ok=True)
            partial = destination.with_name(f".{destination.name}.{uuid.uuid4()}.tmp")
            _copy_plain(source, partial)
            os.replace(partial, destination)
        _verify_sqlite(roots.catalog)
    return manifest


@contextmanager
def _storage_lock(roots: StorageRoots) -> Iterator[None]:
    roots.data.mkdir(parents=True, exist_ok=True)
    with roots.lock.open("a+b") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise BackupError(
                "Runloom storage is active; stop runloom-server before backup or restore"
            ) from error
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def _backup_sqlite(source: Path, destination: Path) -> None:
    try:
        with (
            sqlite3.connect(f"file:{source}?mode=ro", uri=True) as source_database,
            sqlite3.connect(destination) as destination_database,
        ):
            source_database.backup(destination_database)
    except sqlite3.Error as error:
        raise BackupError(f"failed to snapshot SQLite catalog: {error}") from error
    _verify_sqlite(destination)


def _verify_sqlite(path: Path) -> None:
    try:
        with sqlite3.connect(f"file:{path}?mode=ro", uri=True) as database:
            result = database.execute("PRAGMA integrity_check").fetchone()
    except sqlite3.Error as error:
        raise BackupError(f"failed to verify SQLite catalog: {error}") from error
    if result != ("ok",):
        raise BackupError(f"SQLite integrity check failed: {result}")


def _walk_files(
    root: Path,
    *,
    skip_staging: bool,
    directory: Path | None = None,
    depth: int = 0,
) -> Iterator[tuple[Path, Path]]:
    if depth > _MAX_TREE_DEPTH:
        raise BackupError(f"storage tree exceeds {_MAX_TREE_DEPTH} directory levels")
    current = root if directory is None else directory
    with os.scandir(current) as entries:
        for entry in entries:
            path = Path(entry.path)
            if entry.is_symlink():
                raise BackupError(f"storage roots cannot contain symbolic links: {path}")
            relative = path.relative_to(root)
            if depth == 0 and skip_staging and entry.name == "staging":
                continue
            if entry.is_dir(follow_symlinks=False):
                yield from _walk_files(
                    root,
                    skip_staging=skip_staging,
                    directory=path,
                    depth=depth + 1,
                )
            elif entry.is_file(follow_symlinks=False):
                yield path, relative
            else:
                raise BackupError(f"storage entry is not a regular file: {path}")


def _copy_with_digest(
    source: Path,
    destination: Path,
    category: str,
    relative: str,
) -> dict[str, Any]:
    destination.parent.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    size = 0
    with source.open("rb") as input_stream, destination.open("wb") as output_stream:
        while chunk := input_stream.read(_COPY_CHUNK_BYTES):
            output_stream.write(chunk)
            digest.update(chunk)
            size += len(chunk)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    return {"category": category, "path": relative, "size": size, "sha256": digest.hexdigest()}


def _copy_plain(source: Path, destination: Path) -> None:
    with source.open("rb") as input_stream, destination.open("wb") as output_stream:
        while chunk := input_stream.read(_COPY_CHUNK_BYTES):
            output_stream.write(chunk)
        output_stream.flush()
        os.fsync(output_stream.fileno())


def _inventory_existing(path: Path, category: str, relative: str) -> dict[str, Any]:
    size, digest = _digest_file(path)
    return {"category": category, "path": relative, "size": size, "sha256": digest}


def _digest_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while chunk := stream.read(_COPY_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def _read_manifest(bundle: Path) -> dict[str, Any]:
    try:
        manifest = json.loads((bundle / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise BackupError(f"invalid backup manifest: {bundle}") from error
    if (
        not isinstance(manifest, dict)
        or manifest.get("format") != "runloom-physical-backup"
        or manifest.get("format_version") != _FORMAT_VERSION
        or not isinstance(manifest.get("file_count"), int)
        or not isinstance(manifest.get("total_bytes"), int)
    ):
        raise BackupError("unsupported or invalid physical backup manifest")
    return manifest


def _inventory(path: Path) -> Iterator[dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, start=1):
                if line_number > _MAX_FILES:
                    raise BackupError(f"backup cannot exceed {_MAX_FILES} files")
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError as error:
                    raise BackupError(f"invalid inventory line {line_number}") from error
                if not _valid_entry(entry):
                    raise BackupError(f"invalid inventory entry on line {line_number}")
                yield entry
    except OSError as error:
        raise BackupError(f"cannot read backup inventory: {path}") from error


def _valid_entry(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    category = value.get("category")
    path = value.get("path")
    size = value.get("size")
    digest = value.get("sha256")
    return (
        category in {"catalog", "journal", "metrics", "blobs"}
        and isinstance(path, str)
        and _safe_relative(path)
        and (category != "catalog" or path == "catalog.sqlite3")
        and isinstance(size, int)
        and not isinstance(size, bool)
        and size >= 0
        and isinstance(digest, str)
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest)
    )


def _bundle_file(bundle: Path, entry: dict[str, Any]) -> Path:
    if entry["category"] == "catalog":
        return bundle / "catalog.sqlite3"
    return bundle / entry["category"] / entry["path"]


def _restore_path(roots: StorageRoots, entry: dict[str, Any]) -> Path:
    category = entry["category"]
    relative = entry["path"]
    if category == "catalog":
        return roots.catalog
    root = {"journal": roots.journal, "metrics": roots.metrics, "blobs": roots.blobs}[category]
    return root / relative


def _require_empty_destination(roots: StorageRoots) -> None:
    if roots.catalog.exists():
        raise BackupError(f"restore catalog already exists: {roots.catalog}")
    for root in [roots.journal, roots.metrics, roots.blobs]:
        if root.exists() and any(root.iterdir()):
            raise BackupError(f"restore storage root is not empty: {root}")


def _validate_bundle_location(destination: Path, roots: StorageRoots) -> None:
    for root in [roots.data, roots.metrics, roots.blobs]:
        resolved = root.resolve()
        if destination == resolved or destination.is_relative_to(resolved):
            raise BackupError("backup destination cannot be inside a Runloom storage root")


def _safe_relative(value: str) -> bool:
    path = Path(value)
    return (
        bool(value)
        and not path.is_absolute()
        and all(part not in {"", ".", ".."} for part in path.parts)
    )


def _write_json(path: Path, value: Any) -> None:
    with path.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, allow_nan=False, separators=(",", ":"), sort_keys=True)
        stream.write("\n")


def _write_json_line(stream: TextIO, value: Any) -> None:
    json.dump(value, stream, allow_nan=False, separators=(",", ":"), sort_keys=True)
    stream.write("\n")
