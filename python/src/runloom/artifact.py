from __future__ import annotations

import json
import mimetypes
import os
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

from runloom._ids import uuid7
from runloom._json_normalization import normalize_json_object
from runloom._limits import (
    MAX_ARTIFACT_ALIAS_BYTES,
    MAX_ARTIFACT_ALIASES,
    MAX_ARTIFACT_DESCRIPTION_BYTES,
    MAX_ARTIFACT_ENTRIES,
    MAX_ARTIFACT_MANIFEST_BYTES,
    MAX_ARTIFACT_METADATA_BYTES,
    MAX_ARTIFACT_NAME_BYTES,
    MAX_ARTIFACT_PATH_BYTES,
    MAX_ARTIFACT_TYPE_BYTES,
)
from runloom._protocol import validate_blob_file_name
from runloom.rich import _install_stream


class Artifact:
    def __init__(
        self,
        name: str,
        *,
        type: str,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        _validate_component(name, "artifact name", MAX_ARTIFACT_NAME_BYTES)
        _validate_component(type, "artifact type", MAX_ARTIFACT_TYPE_BYTES)
        if description is not None and not isinstance(description, str):
            raise TypeError("artifact description must be a string or None")
        if (
            description is not None
            and len(description.encode("utf-8")) > MAX_ARTIFACT_DESCRIPTION_BYTES
        ):
            raise ValueError(
                f"artifact description cannot exceed {MAX_ARTIFACT_DESCRIPTION_BYTES} bytes"
            )
        self.name = name
        self.type = type
        self.description = description
        self.metadata = _bounded_json_object(
            metadata or {},
            "artifact metadata",
            MAX_ARTIFACT_METADATA_BYTES,
        )
        self.id = uuid7()
        self._files: dict[str, Path] = {}

    def add_file(self, local_path: str | Path, *, name: str | None = None) -> Artifact:
        source = Path(local_path).expanduser()
        if not source.is_file():
            raise FileNotFoundError(f"artifact file was not found: {source}")
        artifact_path = name or source.name
        _validate_artifact_path(artifact_path)
        if artifact_path in self._files:
            raise ValueError(f"artifact path already exists: {artifact_path}")
        if len(self._files) >= MAX_ARTIFACT_ENTRIES:
            raise ValueError(f"artifact cannot contain more than {MAX_ARTIFACT_ENTRIES} entries")
        self._files[artifact_path] = source
        return self

    def add_dir(self, local_path: str | Path, *, name: str | None = None) -> Artifact:
        root = Path(local_path).expanduser()
        if not root.is_dir():
            raise FileNotFoundError(f"artifact directory was not found: {root}")
        prefix = PurePosixPath(name) if name else PurePosixPath()
        if name:
            _validate_artifact_path(name)
        discovered_directories = 1
        for directory, directory_names, file_names in os.walk(root, followlinks=False):
            safe_directories: list[str] = []
            for candidate in directory_names:
                if (Path(directory) / candidate).is_symlink():
                    continue
                discovered_directories += 1
                if discovered_directories > MAX_ARTIFACT_ENTRIES:
                    raise ValueError(
                        "artifact directory traversal cannot exceed "
                        f"{MAX_ARTIFACT_ENTRIES} directories"
                    )
                safe_directories.append(candidate)
            directory_names[:] = sorted(safe_directories)
            for file_name in sorted(file_names):
                source = Path(directory) / file_name
                if source.is_symlink() or not source.is_file():
                    continue
                relative = PurePosixPath(source.relative_to(root).as_posix())
                self.add_file(source, name=str(prefix / relative))
        return self

    def _prepare(self, blob_root: Path, aliases: Sequence[str]) -> dict[str, Any]:
        normalized_aliases: list[str] = []
        known_aliases: set[str] = set()
        for index, alias in enumerate(aliases):
            if index >= MAX_ARTIFACT_ALIASES:
                raise ValueError(
                    f"artifact cannot contain more than {MAX_ARTIFACT_ALIASES} aliases"
                )
            _validate_component(alias, "artifact alias", MAX_ARTIFACT_ALIAS_BYTES)
            if alias not in known_aliases:
                normalized_aliases.append(alias)
                known_aliases.add(alias)
        specifications: list[tuple[str, Path, str, int]] = []
        preflight_entries: list[dict[str, Any]] = []
        for artifact_path, source in sorted(self._files.items()):
            validate_blob_file_name(source.name)
            mime_type = mimetypes.guess_type(source.name)[0] or "application/octet-stream"
            size = source.stat().st_size
            specifications.append((artifact_path, source, mime_type, size))
            preflight_entries.append(
                {
                    "path": artifact_path,
                    "blob": {
                        "digest": "0" * 64,
                        "size": size,
                        "mime_type": mime_type,
                        "file_name": source.name,
                    },
                }
            )
        _validate_manifest_size(
            {
                "id": self.id,
                "name": self.name,
                "type": self.type,
                "description": self.description,
                "metadata": self.metadata,
                "aliases": normalized_aliases,
                "entries": preflight_entries,
            }
        )
        entries: list[dict[str, Any]] = []
        for artifact_path, source, mime_type, expected_size in specifications:
            with source.open("rb") as stream:
                digest, size = _install_stream(blob_root, stream)
            if size != expected_size:
                raise RuntimeError(f"artifact file changed while it was being prepared: {source}")
            entries.append(
                {
                    "path": artifact_path,
                    "blob": {
                        "digest": digest,
                        "size": size,
                        "mime_type": mime_type,
                        "file_name": source.name,
                    },
                }
            )
        return {
            "operation": "create",
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "description": self.description,
            "metadata": self.metadata,
            "aliases": normalized_aliases,
            "entries": entries,
        }


def _validate_component(value: Any, name: str, maximum: int) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    encoded = value.encode("utf-8")
    if (
        not encoded
        or len(encoded) > maximum
        or "/" in value
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise ValueError(f"{name} must contain 1 to {maximum} safe bytes without '/'")


def _validate_artifact_path(value: Any) -> None:
    if not isinstance(value, str):
        raise TypeError("artifact path must be a string")
    encoded = value.encode("utf-8")
    path = PurePosixPath(value)
    if (
        not encoded
        or len(encoded) > MAX_ARTIFACT_PATH_BYTES
        or value.startswith("/")
        or "\\" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or str(path) != value
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise ValueError(
            f"artifact path must be a relative POSIX path up to {MAX_ARTIFACT_PATH_BYTES} bytes"
        )


def _bounded_json_object(
    value: Mapping[str, Any],
    name: str,
    maximum: int,
) -> dict[str, Any]:
    return normalize_json_object(value, name, maximum)


def _validate_manifest_size(value: dict[str, Any]) -> None:
    encoded = json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    if len(encoded) > MAX_ARTIFACT_MANIFEST_BYTES:
        raise ValueError(
            f"serialized artifact manifest exceeds {MAX_ARTIFACT_MANIFEST_BYTES} bytes"
        )
