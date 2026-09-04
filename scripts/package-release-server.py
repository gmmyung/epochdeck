#!/usr/bin/env python3
"""Build and verify a deterministic, platform-appropriate server archive."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import os
import re
import shutil
import stat
import sys
import tarfile
import tempfile
import zipfile
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import BinaryIO

ROOT = Path(__file__).resolve().parents[1]
MAX_ARCHIVE_FILES = 512
MAX_ARCHIVE_FILE_BYTES = 1024 * 1024 * 1024
LINUX_TARGETS = {
    "aarch64-unknown-linux-musl",
    "x86_64-unknown-linux-musl",
}
MACOS_TARGETS = {"aarch64-apple-darwin"}
WINDOWS_TARGETS = {"x86_64-pc-windows-msvc"}
SUPPORTED_TARGETS = LINUX_TARGETS | MACOS_TARGETS | WINDOWS_TARGETS
VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+-(?:alpha|beta|rc)\.[0-9]+$")


class PackageError(RuntimeError):
    """The release archive could not be produced or verified safely."""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", required=True, choices=sorted(SUPPORTED_TARGETS))
    parser.add_argument("--version", required=True)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


def _source_files(target: str) -> list[tuple[Path, PurePosixPath, int]]:
    entries = [
        (ROOT / "LICENSE", PurePosixPath("LICENSE"), 0o644),
        (ROOT / "README.md", PurePosixPath("README.md"), 0o644),
        (ROOT / "SECURITY.md", PurePosixPath("SECURITY.md"), 0o644),
        (
            ROOT / "THIRD_PARTY_NOTICES.txt",
            PurePosixPath("THIRD_PARTY_NOTICES.txt"),
            0o644,
        ),
    ]
    if target in LINUX_TARGETS:
        entries.extend(
            [
                (
                    ROOT / "deploy" / "epochdeck.env.example",
                    PurePosixPath("deploy/epochdeck.env.example"),
                    0o644,
                ),
                (
                    ROOT / "deploy" / "epochdeck.service",
                    PurePosixPath("deploy/epochdeck.service"),
                    0o644,
                ),
            ]
        )
    entries.extend(
        (path, PurePosixPath(path.relative_to(ROOT).as_posix()), 0o644)
        for path in sorted((ROOT / "docs").rglob("*.md"))
    )
    if len(entries) > MAX_ARCHIVE_FILES:
        raise PackageError(f"release archive exceeds {MAX_ARCHIVE_FILES} source files")
    for source, relative, _mode in entries:
        if source.is_symlink() or not source.is_file():
            raise PackageError(f"release source is not a regular file: {source}")
        if relative.is_absolute() or ".." in relative.parts:
            raise PackageError(f"unsafe release path: {relative}")
    return entries


def _materialize(
    package_root: Path,
    *,
    target: str,
    binary: Path,
) -> list[tuple[Path, PurePosixPath, int]]:
    if binary.is_symlink() or not binary.is_file():
        raise PackageError(f"release binary is not a regular file: {binary}")
    binary_name = (
        "epochdeck-server.exe" if target in WINDOWS_TARGETS else "epochdeck-server"
    )
    entries = [
        (binary, PurePosixPath(binary_name), 0o755),
        *_source_files(target),
    ]
    for source, relative, mode in entries:
        destination = package_root.joinpath(*relative.parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        destination.chmod(mode)
    return entries


def _directories(paths: Iterable[PurePosixPath]) -> list[PurePosixPath]:
    directories: set[PurePosixPath] = set()
    for path in paths:
        parent = path.parent
        while parent != PurePosixPath("."):
            directories.add(parent)
            parent = parent.parent
    return sorted(directories, key=lambda item: item.as_posix().encode())


def _tar_info(
    path: str, *, epoch: int, mode: int, is_directory: bool
) -> tarfile.TarInfo:
    info = tarfile.TarInfo(path.rstrip("/") + ("/" if is_directory else ""))
    info.mtime = epoch
    info.mode = mode
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.type = tarfile.DIRTYPE if is_directory else tarfile.REGTYPE
    return info


def _write_tar_gz(
    archive_path: Path,
    *,
    package_root: Path,
    root_name: str,
    entries: list[tuple[Path, PurePosixPath, int]],
    epoch: int,
) -> None:
    with (
        archive_path.open("wb") as raw,
        gzip.GzipFile(
            filename="", mode="wb", fileobj=raw, mtime=epoch, compresslevel=9
        ) as zipped,
        tarfile.open(fileobj=zipped, mode="w", format=tarfile.PAX_FORMAT) as archive,
    ):
        archive.addfile(
            _tar_info(root_name, epoch=epoch, mode=0o755, is_directory=True)
        )
        for directory in _directories(relative for _, relative, _ in entries):
            archive.addfile(
                _tar_info(
                    f"{root_name}/{directory.as_posix()}",
                    epoch=epoch,
                    mode=0o755,
                    is_directory=True,
                )
            )
        for _source, relative, mode in sorted(
            entries, key=lambda item: item[1].as_posix().encode()
        ):
            packaged = package_root.joinpath(*relative.parts)
            info = _tar_info(
                f"{root_name}/{relative.as_posix()}",
                epoch=epoch,
                mode=mode,
                is_directory=False,
            )
            info.size = packaged.stat().st_size
            with packaged.open("rb") as payload:
                archive.addfile(info, payload)


def _zip_timestamp(epoch: int) -> tuple[int, int, int, int, int, int]:
    timestamp = datetime.fromtimestamp(epoch, tz=UTC)
    if timestamp.year < 1980:
        timestamp = datetime(1980, 1, 1, tzinfo=UTC)
    return (
        timestamp.year,
        timestamp.month,
        timestamp.day,
        timestamp.hour,
        timestamp.minute,
        timestamp.second - timestamp.second % 2,
    )


def _zip_info(
    path: str, *, epoch: int, mode: int, is_directory: bool
) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(
        path.rstrip("/") + ("/" if is_directory else ""), _zip_timestamp(epoch)
    )
    info.create_system = 3
    kind = stat.S_IFDIR if is_directory else stat.S_IFREG
    info.external_attr = (kind | mode) << 16
    info.compress_type = zipfile.ZIP_DEFLATED
    return info


def _write_zip(
    archive_path: Path,
    *,
    package_root: Path,
    root_name: str,
    entries: list[tuple[Path, PurePosixPath, int]],
    epoch: int,
) -> None:
    with zipfile.ZipFile(archive_path, mode="w", compresslevel=9) as archive:
        archive.writestr(
            _zip_info(root_name, epoch=epoch, mode=0o755, is_directory=True), b""
        )
        for directory in _directories(relative for _, relative, _ in entries):
            archive.writestr(
                _zip_info(
                    f"{root_name}/{directory.as_posix()}",
                    epoch=epoch,
                    mode=0o755,
                    is_directory=True,
                ),
                b"",
            )
        for _source, relative, mode in sorted(
            entries, key=lambda item: item[1].as_posix().encode()
        ):
            packaged = package_root.joinpath(*relative.parts)
            info = _zip_info(
                f"{root_name}/{relative.as_posix()}",
                epoch=epoch,
                mode=mode,
                is_directory=False,
            )
            with packaged.open("rb") as source, archive.open(info, mode="w") as target:
                shutil.copyfileobj(source, target, length=1024 * 1024)


def _expected_members(
    root_name: str, entries: list[tuple[Path, PurePosixPath, int]]
) -> dict[str, tuple[str, int, int]]:
    return {
        f"{root_name}/{relative.as_posix()}": (
            _file_digest(source),
            source.stat().st_size,
            mode,
        )
        for source, relative, mode in entries
    }


def _expected_directories(
    root_name: str, entries: list[tuple[Path, PurePosixPath, int]]
) -> dict[str, int]:
    names = [
        root_name,
        *(
            f"{root_name}/{directory.as_posix()}"
            for directory in _directories(relative for _, relative, _ in entries)
        ),
    ]
    return dict.fromkeys(names, 0o755)


def _stream_digest(source: BinaryIO) -> tuple[str, int]:
    digest = hashlib.sha256()
    size = 0
    while chunk := source.read(1024 * 1024):
        size += len(chunk)
        if size > MAX_ARCHIVE_FILE_BYTES:
            raise PackageError("archive member exceeds its size bound")
        digest.update(chunk)
    return digest.hexdigest(), size


def _file_digest(path: Path) -> str:
    if path.stat().st_size > MAX_ARCHIVE_FILE_BYTES:
        raise PackageError(f"release source exceeds its size bound: {path}")
    with path.open("rb") as source:
        digest, _size = _stream_digest(source)
    return digest


def _validate_name(name: str, root_name: str) -> None:
    path = PurePosixPath(name.rstrip("/"))
    if (
        path.is_absolute()
        or ".." in path.parts
        or not path.parts
        or path.parts[0] != root_name
    ):
        raise PackageError(f"archive contains unsafe path: {name!r}")


def _verify_tar_gz(
    archive_path: Path,
    *,
    root_name: str,
    entries: list[tuple[Path, PurePosixPath, int]],
) -> None:
    expected = _expected_members(root_name, entries)
    expected_directories = _expected_directories(root_name, entries)
    found: dict[str, tuple[str, int, int]] = {}
    found_directories: dict[str, int] = {}
    with tarfile.open(archive_path, mode="r:gz") as archive:
        members = archive.getmembers()
        if len(members) > MAX_ARCHIVE_FILES * 2:
            raise PackageError("archive member count exceeds its bound")
        for member in members:
            _validate_name(member.name, root_name)
            if (
                member.issym()
                or member.islnk()
                or not (member.isdir() or member.isfile())
            ):
                raise PackageError(
                    f"archive contains unsupported member: {member.name}"
                )
            if member.isdir():
                normalized = member.name.rstrip("/")
                if normalized in found_directories:
                    raise PackageError(
                        f"archive contains duplicate member: {member.name}"
                    )
                found_directories[normalized] = member.mode & 0o777
            else:
                if member.name in found:
                    raise PackageError(
                        f"archive contains duplicate member: {member.name}"
                    )
                payload = archive.extractfile(member)
                if payload is None:
                    raise PackageError(f"could not read archive member: {member.name}")
                digest, size = _stream_digest(payload)
                found[member.name] = (digest, size, member.mode & 0o777)
    if found != expected or found_directories != expected_directories:
        raise PackageError(
            "archive file manifest or contents do not match release inputs"
        )


def _verify_zip(
    archive_path: Path,
    *,
    root_name: str,
    entries: list[tuple[Path, PurePosixPath, int]],
) -> None:
    expected = _expected_members(root_name, entries)
    expected_directories = _expected_directories(root_name, entries)
    found: dict[str, tuple[str, int, int]] = {}
    found_directories: dict[str, int] = {}
    with zipfile.ZipFile(archive_path) as archive:
        members = archive.infolist()
        if len(members) > MAX_ARCHIVE_FILES * 2:
            raise PackageError("archive member count exceeds its bound")
        for member in members:
            _validate_name(member.filename, root_name)
            mode = (member.external_attr >> 16) & 0o777
            file_type = (member.external_attr >> 16) & 0o170000
            if file_type not in {0, stat.S_IFDIR, stat.S_IFREG}:
                raise PackageError(
                    f"archive contains unsupported member: {member.filename}"
                )
            if member.is_dir():
                normalized = member.filename.rstrip("/")
                if normalized in found_directories:
                    raise PackageError(
                        f"archive contains duplicate member: {member.filename}"
                    )
                found_directories[normalized] = mode
            else:
                if member.filename in found:
                    raise PackageError(
                        f"archive contains duplicate member: {member.filename}"
                    )
                with archive.open(member) as payload:
                    digest, size = _stream_digest(payload)
                found[member.filename] = (digest, size, mode)
    if found != expected or found_directories != expected_directories:
        raise PackageError(
            "archive file manifest or contents do not match release inputs"
        )


def main() -> int:
    arguments = _parse_args()
    if not VERSION_RE.fullmatch(arguments.version):
        raise PackageError("version must be a SemVer prerelease")
    epoch_text = os.environ.get("SOURCE_DATE_EPOCH")
    if epoch_text is None or not epoch_text.isascii() or not epoch_text.isdigit():
        raise PackageError("SOURCE_DATE_EPOCH must be an unsigned decimal integer")
    epoch = int(epoch_text)
    binary = arguments.binary.resolve(strict=True)
    output_dir = arguments.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    root_name = f"epochdeck-server-{arguments.version}-{arguments.target}"
    extension = "tar.gz" if arguments.target in LINUX_TARGETS else "zip"
    archive_path = output_dir / f"{root_name}.{extension}"
    if archive_path.exists():
        raise PackageError(f"refusing to replace existing archive: {archive_path}")

    with tempfile.TemporaryDirectory(prefix="epochdeck-package-") as temporary:
        package_root = Path(temporary) / root_name
        package_root.mkdir()
        entries = _materialize(package_root, target=arguments.target, binary=binary)
        if extension == "tar.gz":
            _write_tar_gz(
                archive_path,
                package_root=package_root,
                root_name=root_name,
                entries=entries,
                epoch=epoch,
            )
            _verify_tar_gz(archive_path, root_name=root_name, entries=entries)
        else:
            _write_zip(
                archive_path,
                package_root=package_root,
                root_name=root_name,
                entries=entries,
                epoch=epoch,
            )
            _verify_zip(archive_path, root_name=root_name, entries=entries)

    print(archive_path)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, PackageError, tarfile.TarError, zipfile.BadZipFile) as error:
        print(f"server packaging failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
