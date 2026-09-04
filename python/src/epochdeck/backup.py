from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
import stat
import uuid
from collections.abc import Iterator
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any, BinaryIO, TextIO

from epochdeck._platform_fs import (
    DIRECTORY_DESCRIPTORS_SUPPORTED,
    FileLockUnavailable,
    acquire_file_lock,
    is_link_or_reparse,
    open_regular_file_descriptor,
    release_file_lock,
    sync_directory,
    verify_directory,
)

_COPY_CHUNK_BYTES = 1024 * 1024
_MAX_FILES = 10_000_000
_MAX_TREE_DEPTH = 128
_MAX_MANIFEST_BYTES = 64 * 1024
_MAX_INVENTORY_RECORD_BYTES = 16 * 1024
_MAX_RELATIVE_PATH_BYTES = 4 * 1024
_NO_FOLLOW = getattr(os, "O_NOFOLLOW", 0)
_DIRECTORY = getattr(os, "O_DIRECTORY", 0)
_WINDOWS_FORBIDDEN_PATH_CHARACTERS = frozenset('<>:"\\|?*')
_WINDOWS_RESERVED_PATH_NAMES = {
    "AUX",
    "CON",
    "CONIN$",
    "CONOUT$",
    "NUL",
    "PRN",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


class BackupError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class StorageRoots:
    data: Path
    metrics: Path
    blobs: Path

    @classmethod
    def from_environment(cls) -> StorageRoots:
        data = Path(os.environ.get("EPOCHDECK_DATA_DIR", "./data")).expanduser().resolve()
        metrics = (
            Path(os.environ.get("EPOCHDECK_METRICS_DIR", data / "metrics")).expanduser().resolve()
        )
        blobs = Path(os.environ.get("EPOCHDECK_BLOBS_DIR", data / "blobs")).expanduser().resolve()
        return cls(data=data, metrics=metrics, blobs=blobs)

    @property
    def catalog(self) -> Path:
        return self.data / "catalog.sqlite3"


def backup_storage(roots: StorageRoots, destination: Path) -> dict[str, Any]:
    """Copy a stopped server's complete physical state into an atomic bundle."""
    _validate_storage_roots(roots)
    destination = destination.expanduser().resolve()
    _validate_bundle_location(destination, roots)
    if destination.exists():
        raise FileExistsError(f"backup destination already exists: {destination}")
    if not roots.catalog.is_file():
        raise BackupError(f"EpochDeck catalog was not found: {roots.catalog}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.parent / f".{destination.name}.partial-{uuid.uuid4()}"
    temporary.mkdir(mode=0o700)
    temporary.chmod(0o700)
    try:
        with _storage_lock(roots):
            for directory in ["metrics", "blobs"]:
                (temporary / directory).mkdir(mode=0o700)
            _backup_sqlite(roots.catalog, temporary / "catalog.sqlite3")
            count = 0
            total_bytes = 0
            with (temporary / "files.jsonl").open("w", encoding="utf-8") as inventory:
                (temporary / "files.jsonl").chmod(0o600)
                catalog_entry = _inventory_existing(
                    temporary / "catalog.sqlite3", "catalog", "catalog.sqlite3"
                )
                _write_json_line(inventory, catalog_entry)
                count += 1
                total_bytes += int(catalog_entry["size"])
                for category, source in [
                    ("metrics", roots.metrics),
                    ("blobs", roots.blobs),
                ]:
                    if not source.exists():
                        continue
                    for path, relative in _walk_files(source, skip_staging=True):
                        if count >= _MAX_FILES:
                            raise BackupError(f"backup cannot exceed {_MAX_FILES} files")
                        target = temporary / category / relative
                        entry = _copy_with_digest(path, target, category, relative.as_posix())
                        _write_json_line(inventory, entry)
                        count += 1
                        total_bytes += int(entry["size"])
                inventory.flush()
                os.fsync(inventory.fileno())
            manifest = {
                "format": "epochdeck-physical-backup",
                "created_at": datetime.now(UTC).isoformat(),
                "file_count": count,
                "total_bytes": total_bytes,
            }
            _write_json(temporary / "manifest.json", manifest)
            _fsync_directory_tree(temporary)
        os.replace(temporary, destination)
        _fsync_directory(destination.parent)
        return manifest
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def restore_storage(bundle: Path, roots: StorageRoots) -> dict[str, Any]:
    """Verify then restore a physical bundle into empty, inactive storage roots."""
    _validate_storage_roots(roots)
    bundle = bundle.expanduser().resolve()
    _validate_bundle_location(bundle, roots)
    manifest = _read_manifest(bundle)
    inventory_path = bundle / "files.jsonl"
    with _storage_lock(roots):
        _require_empty_destination(roots)
        restore_id = str(uuid.uuid4())
        staging = {
            "data": roots.data / f".restore-{restore_id}",
            "metrics": roots.metrics.parent / f".{roots.metrics.name}.restore-{restore_id}",
            "blobs": roots.blobs.parent / f".{roots.blobs.name}.restore-{restore_id}",
        }
        committed: list[Path] = []
        try:
            for path in staging.values():
                path.mkdir(parents=True, mode=0o700)
                path.chmod(0o700)
            verified_count = 0
            verified_bytes = 0
            for entry in _inventory(inventory_path):
                destination = _staged_restore_path(staging, entry)
                destination.parent.mkdir(parents=True, exist_ok=True)
                size = _copy_verified_bundle_file(bundle, entry, destination)
                verified_count += 1
                verified_bytes += size
            if (
                verified_count != manifest["file_count"]
                or verified_bytes != manifest["total_bytes"]
            ):
                raise BackupError("backup inventory totals do not match the manifest")
            staged_catalog = staging["data"] / "catalog.sqlite3"
            _verify_sqlite(staged_catalog)

            # Each staging root shares a filesystem with its destination. Syncing
            # every directory bottom-up makes the complete subtrees durable before
            # their top-level entries are renamed and the catalog is published.
            for path in staging.values():
                _fsync_directory_tree(path)

            for source_root, destination_root in [
                (staging["metrics"], roots.metrics),
                (staging["blobs"], roots.blobs),
            ]:
                for source in sorted(source_root.iterdir()):
                    destination = destination_root / source.name
                    os.replace(source, destination)
                    committed.append(destination)
                _fsync_directory(destination_root)
            os.replace(staged_catalog, roots.catalog)
            committed.append(roots.catalog)
            _fsync_directory(roots.catalog.parent)
            _verify_sqlite(roots.catalog)
        except BaseException:
            for destination in reversed(committed):
                if destination.is_dir():
                    shutil.rmtree(destination, ignore_errors=True)
                else:
                    destination.unlink(missing_ok=True)
                _fsync_directory(destination.parent)
            raise
        finally:
            for path in staging.values():
                shutil.rmtree(path, ignore_errors=True)
    return manifest


@contextmanager
def _storage_lock(roots: StorageRoots) -> Iterator[None]:
    requested_roots = [roots.data, roots.metrics, roots.blobs]
    for root in requested_roots:
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
    lock_paths = sorted(
        {root.expanduser().resolve() / "epochdeck.lock" for root in requested_roots},
        key=os.fspath,
    )
    streams: list[BinaryIO] = []
    try:
        for lock_path in lock_paths:
            stream = lock_path.open("a+b")
            streams.append(stream)
            try:
                acquire_file_lock(stream.fileno())
            except FileLockUnavailable as error:
                raise BackupError(
                    "EpochDeck storage is active; stop epochdeck-server before backup or restore"
                ) from error
        try:
            yield
        finally:
            for acquired_stream in reversed(streams):
                release_file_lock(acquired_stream.fileno())
    finally:
        for acquired_stream in reversed(streams):
            acquired_stream.close()


def _backup_sqlite(source: Path, destination: Path) -> None:
    try:
        with (
            closing(sqlite3.connect(_readonly_sqlite_uri(source), uri=True)) as source_database,
            closing(sqlite3.connect(destination)) as destination_database,
            source_database,
            destination_database,
        ):
            source_database.backup(destination_database)
    except sqlite3.Error as error:
        raise BackupError(f"failed to snapshot SQLite catalog: {error}") from error
    destination.chmod(0o600)
    _verify_sqlite(destination)


def _verify_sqlite(path: Path) -> None:
    try:
        with closing(sqlite3.connect(_readonly_sqlite_uri(path), uri=True)) as database:
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
            if is_link_or_reparse(entry.stat(follow_symlinks=False)):
                raise BackupError(f"storage roots cannot contain symbolic links: {path}")
            relative = path.relative_to(root)
            if depth == 0 and skip_staging and entry.name in {"staging", "epochdeck.lock"}:
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
    with _open_regular_file(source) as input_stream, destination.open("wb") as output_stream:
        destination.chmod(0o600)
        while chunk := input_stream.read(_COPY_CHUNK_BYTES):
            output_stream.write(chunk)
            digest.update(chunk)
            size += len(chunk)
        output_stream.flush()
        os.fsync(output_stream.fileno())
    return {"category": category, "path": relative, "size": size, "sha256": digest.hexdigest()}


def _copy_verified_bundle_file(
    bundle: Path,
    entry: dict[str, Any],
    destination: Path,
) -> int:
    relative = _bundle_relative_path(entry)
    digest = hashlib.sha256()
    size = 0
    try:
        with (
            _open_bundle_regular_file(bundle, relative) as input_stream,
            destination.open("xb") as output_stream,
        ):
            destination.chmod(0o600)
            while chunk := input_stream.read(_COPY_CHUNK_BYTES):
                output_stream.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            output_stream.flush()
            os.fsync(output_stream.fileno())
    except FileExistsError as error:
        raise BackupError(f"duplicate backup inventory destination: {destination}") from error
    if size != entry["size"] or digest.hexdigest() != entry["sha256"]:
        destination.unlink(missing_ok=True)
        raise BackupError(f"backup verification failed: {bundle / relative}")
    return size


def _inventory_existing(path: Path, category: str, relative: str) -> dict[str, Any]:
    size, digest = _digest_file(path)
    return {"category": category, "path": relative, "size": size, "sha256": digest}


def _digest_file(path: Path) -> tuple[int, str]:
    digest = hashlib.sha256()
    size = 0
    with _open_regular_file(path) as stream:
        while chunk := stream.read(_COPY_CHUNK_BYTES):
            digest.update(chunk)
            size += len(chunk)
    return size, digest.hexdigest()


def _read_manifest(bundle: Path) -> dict[str, Any]:
    path = bundle / "manifest.json"
    try:
        manifest = json.loads(_read_bounded_file(path, _MAX_MANIFEST_BYTES))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as error:
        raise BackupError(f"invalid backup manifest: {bundle}") from error
    expected_fields = {
        "format",
        "created_at",
        "file_count",
        "total_bytes",
    }
    if not isinstance(manifest, dict) or set(manifest) != expected_fields:
        raise BackupError("invalid physical backup manifest")
    file_count = manifest.get("file_count")
    total_bytes = manifest.get("total_bytes")
    created_at = manifest.get("created_at")
    if (
        manifest.get("format") != "epochdeck-physical-backup"
        or isinstance(file_count, bool)
        or not isinstance(file_count, int)
        or not 1 <= file_count <= _MAX_FILES
        or isinstance(total_bytes, bool)
        or not isinstance(total_bytes, int)
        or total_bytes < 0
        or not isinstance(created_at, str)
        or not created_at
    ):
        raise BackupError("invalid physical backup manifest")
    return manifest


def _inventory(path: Path) -> Iterator[dict[str, Any]]:
    try:
        with _open_regular_file(path) as stream:
            line_number = 0
            while line := stream.readline(_MAX_INVENTORY_RECORD_BYTES + 2):
                line_number += 1
                if line_number > _MAX_FILES:
                    raise BackupError(f"backup cannot exceed {_MAX_FILES} files")
                if len(line) > _MAX_INVENTORY_RECORD_BYTES + 1:
                    raise BackupError(
                        f"inventory record exceeds {_MAX_INVENTORY_RECORD_BYTES} bytes"
                    )
                if not line.endswith(b"\n"):
                    raise BackupError(f"incomplete inventory line {line_number}")
                try:
                    entry = json.loads(line)
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise BackupError(f"invalid inventory line {line_number}") from error
                if not _valid_entry(entry):
                    raise BackupError(f"invalid inventory entry on line {line_number}")
                yield entry
    except OSError as error:
        raise BackupError(f"cannot read backup inventory: {path}") from error


def _valid_entry(value: Any) -> bool:
    if not isinstance(value, dict) or set(value) != {"category", "path", "size", "sha256"}:
        return False
    category = value.get("category")
    path = value.get("path")
    size = value.get("size")
    digest = value.get("sha256")
    return (
        category in {"catalog", "metrics", "blobs"}
        and isinstance(path, str)
        and _safe_relative(path)
        and (category != "catalog" or path == "catalog.sqlite3")
        and (category == "catalog" or PurePosixPath(path).parts[0] != "epochdeck.lock")
        and isinstance(size, int)
        and not isinstance(size, bool)
        and size >= 0
        and isinstance(digest, str)
        and len(digest) == 64
        and all(character in "0123456789abcdef" for character in digest)
    )


def _bundle_relative_path(entry: dict[str, Any]) -> Path:
    if entry["category"] == "catalog":
        return Path("catalog.sqlite3")
    return Path(entry["category"], *PurePosixPath(entry["path"]).parts)


@contextmanager
def _open_bundle_regular_file(bundle: Path, relative: Path) -> Iterator[BinaryIO]:
    if not DIRECTORY_DESCRIPTORS_SUPPORTED:
        current = bundle
        try:
            verify_directory(current)
            for component in relative.parts[:-1]:
                current /= component
                verify_directory(current)
            source_descriptor = open_regular_file_descriptor(
                current / relative.parts[-1], os.O_RDONLY
            )
        except OSError as error:
            raise BackupError(f"cannot safely read backup source: {bundle / relative}") from error
        try:
            with os.fdopen(source_descriptor, "rb") as stream:
                source_descriptor = -1
                yield stream
        finally:
            if source_descriptor >= 0:
                os.close(source_descriptor)
        return

    descriptors: list[int] = []
    source_descriptor = -1
    try:
        try:
            directory = os.open(bundle, os.O_RDONLY | _DIRECTORY | _NO_FOLLOW)
            descriptors.append(directory)
            for component in relative.parts[:-1]:
                directory = os.open(
                    component,
                    os.O_RDONLY | _DIRECTORY | _NO_FOLLOW,
                    dir_fd=directory,
                )
                descriptors.append(directory)
            source_descriptor = os.open(
                relative.parts[-1],
                os.O_RDONLY | _NO_FOLLOW,
                dir_fd=directory,
            )
        except OSError as error:
            raise BackupError(f"cannot safely read backup source: {bundle / relative}") from error
        if not stat.S_ISREG(os.fstat(source_descriptor).st_mode):
            raise BackupError(f"backup source is not a regular file: {bundle / relative}")
        with os.fdopen(source_descriptor, "rb") as stream:
            source_descriptor = -1
            yield stream
    finally:
        if source_descriptor >= 0:
            os.close(source_descriptor)
        for descriptor in reversed(descriptors):
            os.close(descriptor)


def _staged_restore_path(staging: dict[str, Path], entry: dict[str, Any]) -> Path:
    category = entry["category"]
    relative = entry["path"]
    if category == "catalog":
        return staging["data"] / "catalog.sqlite3"
    return staging[category].joinpath(*PurePosixPath(relative).parts)


def _require_empty_destination(roots: StorageRoots) -> None:
    if roots.catalog.exists():
        raise BackupError(f"restore catalog already exists: {roots.catalog}")
    for root in [roots.metrics, roots.blobs]:
        if root.exists() and any(path.name != "epochdeck.lock" for path in root.iterdir()):
            raise BackupError(f"restore storage root is not empty: {root}")


def _validate_bundle_location(destination: Path, roots: StorageRoots) -> None:
    for root in [roots.data, roots.metrics, roots.blobs]:
        resolved = root.resolve()
        if (
            destination == resolved
            or destination.is_relative_to(resolved)
            or resolved.is_relative_to(destination)
        ):
            raise BackupError("backup path and EpochDeck storage roots must be disjoint")


def _validate_storage_roots(roots: StorageRoots) -> None:
    data = roots.data.expanduser().resolve()
    metrics = roots.metrics.expanduser().resolve()
    blobs = roots.blobs.expanduser().resolve()
    if metrics == blobs or metrics.is_relative_to(blobs) or blobs.is_relative_to(metrics):
        raise BackupError("metric and blob storage roots must not overlap")
    if data == metrics or data.is_relative_to(metrics):
        raise BackupError("data storage root must not equal or be inside the metric storage root")
    if data == blobs or data.is_relative_to(blobs):
        raise BackupError("data storage root must not equal or be inside the blob storage root")


def _safe_relative(value: str) -> bool:
    path = PurePosixPath(value)
    return (
        bool(value)
        and len(value.encode("utf-8")) <= _MAX_RELATIVE_PATH_BYTES
        and not path.is_absolute()
        and all(part not in {"", ".", ".."} for part in path.parts)
        and all(_portable_path_component(part) for part in path.parts)
        and str(path) == value
        and not any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    )


def _portable_path_component(value: str) -> bool:
    stem = value.split(".", maxsplit=1)[0].upper()
    return (
        not any(character in _WINDOWS_FORBIDDEN_PATH_CHARACTERS for character in value)
        and not value.endswith((" ", "."))
        and stem not in _WINDOWS_RESERVED_PATH_NAMES
    )


def _write_json(path: Path, value: Any) -> None:
    with path.open("w", encoding="utf-8") as stream:
        path.chmod(0o600)
        json.dump(value, stream, allow_nan=False, separators=(",", ":"), sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def _write_json_line(stream: TextIO, value: Any) -> None:
    json.dump(value, stream, allow_nan=False, separators=(",", ":"), sort_keys=True)
    stream.write("\n")


def _read_bounded_file(path: Path, maximum: int) -> bytes:
    with _open_regular_file(path) as stream:
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise BackupError(f"backup metadata exceeds {maximum} bytes: {path}")
    return data


@contextmanager
def _open_regular_file(path: Path) -> Iterator[BinaryIO]:
    try:
        descriptor = open_regular_file_descriptor(path, os.O_RDONLY)
    except OSError as error:
        raise BackupError(f"cannot safely read regular file: {path}") from error
    try:
        with os.fdopen(descriptor, "rb") as stream:
            descriptor = -1
            yield stream
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    if not DIRECTORY_DESCRIPTORS_SUPPORTED:
        try:
            sync_directory(path)
        except OSError as error:
            raise BackupError(f"cannot safely sync directory: {path}") from error
        return
    try:
        descriptor = os.open(path, os.O_RDONLY | _DIRECTORY | _NO_FOLLOW)
    except OSError as error:
        raise BackupError(f"cannot safely sync directory: {path}") from error
    try:
        _fsync_directory_descriptor(descriptor, path)
    finally:
        os.close(descriptor)


def _fsync_directory_tree(root: Path) -> None:
    if not DIRECTORY_DESCRIPTORS_SUPPORTED:
        _fsync_directory_tree_path(root, depth=0)
        return
    try:
        descriptor = os.open(root, os.O_RDONLY | _DIRECTORY | _NO_FOLLOW)
    except OSError as error:
        raise BackupError(f"cannot safely sync directory tree: {root}") from error
    try:
        _fsync_directory_tree_descriptor(descriptor, root, depth=0)
    finally:
        os.close(descriptor)


def _fsync_directory_tree_descriptor(
    descriptor: int,
    current: Path,
    *,
    depth: int,
) -> None:
    if depth > _MAX_TREE_DEPTH:
        raise BackupError(f"directory tree exceeds {_MAX_TREE_DEPTH} levels while syncing")
    try:
        with os.scandir(descriptor) as entries:
            for entry in entries:
                child = current / entry.name
                if is_link_or_reparse(entry.stat(follow_symlinks=False)):
                    raise BackupError(f"directory tree contains a symbolic link: {child}")
                if entry.is_dir(follow_symlinks=False):
                    try:
                        child_descriptor = os.open(
                            entry.name,
                            os.O_RDONLY | _DIRECTORY | _NO_FOLLOW,
                            dir_fd=descriptor,
                        )
                    except OSError as error:
                        raise BackupError(f"cannot safely sync directory tree: {child}") from error
                    try:
                        _fsync_directory_tree_descriptor(
                            child_descriptor,
                            child,
                            depth=depth + 1,
                        )
                    finally:
                        os.close(child_descriptor)
                elif not entry.is_file(follow_symlinks=False):
                    raise BackupError(f"directory tree contains a non-regular entry: {child}")
    except BackupError:
        raise
    except OSError as error:
        raise BackupError(f"cannot safely sync directory tree: {current}") from error
    _fsync_directory_descriptor(descriptor, current)


def _fsync_directory_descriptor(descriptor: int, path: Path) -> None:
    try:
        os.fsync(descriptor)
    except OSError as error:
        raise BackupError(f"failed to sync directory: {path}") from error


def _fsync_directory_tree_path(root: Path, *, depth: int) -> None:
    if depth > _MAX_TREE_DEPTH:
        raise BackupError(f"directory tree exceeds {_MAX_TREE_DEPTH} levels while syncing")
    try:
        verify_directory(root)
        with os.scandir(root) as entries:
            for entry in entries:
                child = root / entry.name
                status = entry.stat(follow_symlinks=False)
                if is_link_or_reparse(status):
                    raise BackupError(f"directory tree contains a symbolic link: {child}")
                if stat.S_ISDIR(status.st_mode):
                    _fsync_directory_tree_path(child, depth=depth + 1)
                elif not stat.S_ISREG(status.st_mode):
                    raise BackupError(f"directory tree contains a non-regular entry: {child}")
        _fsync_directory(root)
    except BackupError:
        raise
    except OSError as error:
        raise BackupError(f"cannot safely sync directory tree: {root}") from error


def _readonly_sqlite_uri(path: Path) -> str:
    return f"{path.expanduser().resolve().as_uri()}?mode=ro"
