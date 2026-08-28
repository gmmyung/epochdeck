from __future__ import annotations

import mimetypes
import os
from collections.abc import Mapping, Sequence
from copy import deepcopy
from pathlib import Path, PurePosixPath
from typing import Any

from runloom._ids import uuid7
from runloom.rich import _install_stream

_MAX_ENTRIES = 4_096
_MAX_PATH_BYTES = 1_024


class Artifact:
    def __init__(
        self,
        name: str,
        *,
        type: str,
        description: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        _validate_component(name, "artifact name", 128)
        _validate_component(type, "artifact type", 64)
        if description is not None and not isinstance(description, str):
            raise TypeError("artifact description must be a string or None")
        self.name = name
        self.type = type
        self.description = description
        self.metadata = deepcopy(dict(metadata or {}))
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
        if len(self._files) >= _MAX_ENTRIES:
            raise ValueError(f"artifact cannot contain more than {_MAX_ENTRIES} entries")
        self._files[artifact_path] = source
        return self

    def add_dir(self, local_path: str | Path, *, name: str | None = None) -> Artifact:
        root = Path(local_path).expanduser()
        if not root.is_dir():
            raise FileNotFoundError(f"artifact directory was not found: {root}")
        prefix = PurePosixPath(name) if name else PurePosixPath()
        if name:
            _validate_artifact_path(name)
        for directory, directory_names, file_names in os.walk(root, followlinks=False):
            directory_names[:] = sorted(
                candidate
                for candidate in directory_names
                if not (Path(directory) / candidate).is_symlink()
            )
            for file_name in sorted(file_names):
                source = Path(directory) / file_name
                if source.is_symlink() or not source.is_file():
                    continue
                relative = PurePosixPath(source.relative_to(root).as_posix())
                self.add_file(source, name=str(prefix / relative))
        return self

    def _prepare(self, blob_root: Path, aliases: Sequence[str]) -> dict[str, Any]:
        normalized_aliases = list(dict.fromkeys(aliases))
        for alias in normalized_aliases:
            _validate_component(alias, "artifact alias", 128)
        entries: list[dict[str, Any]] = []
        for artifact_path, source in sorted(self._files.items()):
            mime_type, _ = mimetypes.guess_type(source.name)
            with source.open("rb") as stream:
                digest, size = _install_stream(blob_root, stream)
            entries.append(
                {
                    "path": artifact_path,
                    "blob": {
                        "digest": digest,
                        "size": size,
                        "mime_type": mime_type or "application/octet-stream",
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
            "metadata": deepcopy(self.metadata),
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
        or len(encoded) > _MAX_PATH_BYTES
        or value.startswith("/")
        or "\\" in value
        or any(part in {"", ".", ".."} for part in value.split("/"))
        or str(path) != value
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise ValueError(
            f"artifact path must be a relative POSIX path up to {_MAX_PATH_BYTES} bytes"
        )
