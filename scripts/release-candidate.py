#!/usr/bin/env python3
"""Create or verify the exact release-candidate manifest and checksums."""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[1]
SERVER_ARTIFACTS = (
    ("x86_64-unknown-linux-musl", "tar.gz"),
    ("aarch64-unknown-linux-musl", "tar.gz"),
    ("x86_64-apple-darwin", "zip"),
    ("aarch64-apple-darwin", "zip"),
    ("x86_64-pc-windows-msvc", "zip"),
)


class CandidateError(RuntimeError):
    """The assembled release candidate is incomplete or inconsistent."""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=("create", "check"))
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--version", required=True)
    return parser.parse_args()


def _project_versions() -> tuple[str, str]:
    cargo = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    python = tomllib.loads(
        (ROOT / "python" / "pyproject.toml").read_text(encoding="utf-8")
    )
    return cargo["workspace"]["package"]["version"], python["project"]["version"]


def _expected_names(version: str) -> tuple[str, ...]:
    cargo_version, python_version = _project_versions()
    if version != cargo_version:
        raise CandidateError(
            f"requested version {version!r} does not match Cargo version {cargo_version!r}"
        )
    return tuple(
        sorted(
            [
                *(
                    f"epochdeck-server-{version}-{target}.{extension}"
                    for target, extension in SERVER_ARTIFACTS
                ),
                f"epochdeck-{python_version}-py3-none-any.whl",
                f"epochdeck-{python_version}.tar.gz",
            ]
        )
    )


def _regular_files(directory: Path) -> tuple[str, ...]:
    entries = list(directory.iterdir())
    for entry in entries:
        if entry.is_symlink() or not entry.is_file():
            raise CandidateError(
                f"release directory contains a non-regular entry: {entry.name}"
            )
    return tuple(sorted(entry.name for entry in entries))


def _checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _checksum_text(directory: Path, names: tuple[str, ...]) -> str:
    return "".join(f"{_checksum(directory / name)}  {name}\n" for name in names)


def main() -> int:
    arguments = _parse_args()
    directory = arguments.directory.resolve(strict=True)
    expected = _expected_names(arguments.version)
    checksum_path = directory / "SHA256SUMS"
    expected_entries = (
        expected if arguments.mode == "create" else (*expected, "SHA256SUMS")
    )
    actual_entries = _regular_files(directory)
    if actual_entries != tuple(sorted(expected_entries)):
        raise CandidateError(
            "release candidate manifest differs from the exact expected set:\n"
            f"expected: {sorted(expected_entries)!r}\nactual:   {list(actual_entries)!r}"
        )
    canonical = _checksum_text(directory, expected)
    if arguments.mode == "create":
        checksum_path.write_text(canonical, encoding="ascii", newline="\n")
    elif checksum_path.read_text(encoding="ascii") != canonical:
        raise CandidateError("SHA256SUMS is missing, malformed, unsorted, or stale")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, KeyError, tomllib.TOMLDecodeError, CandidateError) as error:
        print(f"release candidate check failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
