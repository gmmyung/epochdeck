#!/usr/bin/env python3
"""Smoke-test the executable shipped inside a release server archive."""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import stat
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path, PurePosixPath
from typing import BinaryIO

MAX_ARCHIVE_FILES = 1_024
MAX_ARCHIVE_FILE_BYTES = 1024 * 1024 * 1024
MAX_EXTRACTED_BYTES = 2 * 1024 * 1024 * 1024
MAX_RESPONSE_BYTES = 4 * 1024 * 1024
STARTUP_TIMEOUT_SECONDS = 30
COMMAND_TIMEOUT_SECONDS = 15
TARGET_ARCHIVES = {
    "aarch64-apple-darwin": ("zip", "epochdeck-server"),
    "aarch64-unknown-linux-musl": ("tar.gz", "epochdeck-server"),
    "x86_64-pc-windows-msvc": ("zip", "epochdeck-server.exe"),
    "x86_64-unknown-linux-musl": ("tar.gz", "epochdeck-server"),
}


class SmokeError(RuntimeError):
    """A release archive did not satisfy the native smoke test."""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", required=True, type=Path)
    parser.add_argument("--target", required=True, choices=sorted(TARGET_ARCHIVES))
    parser.add_argument("--version", required=True)
    return parser.parse_args()


def _safe_path(root: Path, name: str) -> Path:
    relative = PurePosixPath(name.rstrip("/"))
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise SmokeError(f"archive contains unsafe path: {name!r}")
    destination = root.joinpath(*relative.parts)
    destination.resolve().relative_to(root.resolve())
    return destination


def _copy_bounded(source: BinaryIO, target: BinaryIO, expected_size: int) -> None:
    if expected_size < 0 or expected_size > MAX_ARCHIVE_FILE_BYTES:
        raise SmokeError("archive member exceeds its size bound")
    copied = 0
    while chunk := source.read(min(1024 * 1024, expected_size - copied + 1)):
        copied += len(chunk)
        if copied > expected_size:
            raise SmokeError("archive member exceeds its declared size")
        target.write(chunk)
    if copied != expected_size:
        raise SmokeError("archive member is shorter than its declared size")


def _extract_tar(archive_path: Path, destination: Path) -> None:
    with tarfile.open(archive_path, mode="r:gz") as archive:
        members = archive.getmembers()
        if len(members) > MAX_ARCHIVE_FILES:
            raise SmokeError("archive member count exceeds its bound")
        extracted_size = sum(member.size for member in members if member.isfile())
        if extracted_size > MAX_EXTRACTED_BYTES:
            raise SmokeError("archive contents exceed the extraction-size bound")
        for member in members:
            output = _safe_path(destination, member.name)
            if member.isdir():
                output.mkdir(parents=True, exist_ok=True)
            elif member.isfile():
                output.parent.mkdir(parents=True, exist_ok=True)
                payload = archive.extractfile(member)
                if payload is None:
                    raise SmokeError(f"could not read archive member: {member.name}")
                with output.open("xb") as target:
                    _copy_bounded(payload, target, member.size)
                output.chmod(member.mode & 0o777)
            else:
                raise SmokeError(f"archive contains unsupported member: {member.name}")


def _extract_zip(archive_path: Path, destination: Path) -> None:
    with zipfile.ZipFile(archive_path) as archive:
        members = archive.infolist()
        if len(members) > MAX_ARCHIVE_FILES:
            raise SmokeError("archive member count exceeds its bound")
        extracted_size = sum(
            member.file_size for member in members if not member.is_dir()
        )
        if extracted_size > MAX_EXTRACTED_BYTES:
            raise SmokeError("archive contents exceed the extraction-size bound")
        for member in members:
            output = _safe_path(destination, member.filename)
            raw_mode = member.external_attr >> 16
            file_type = raw_mode & 0o170000
            if file_type not in {0, stat.S_IFDIR, stat.S_IFREG}:
                raise SmokeError(
                    f"archive contains unsupported member: {member.filename}"
                )
            if member.is_dir():
                output.mkdir(parents=True, exist_ok=True)
            else:
                output.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as source, output.open("xb") as target:
                    _copy_bounded(source, target, member.file_size)
                if os.name != "nt":
                    output.chmod(raw_mode & 0o777)


def _run_cli(binary: Path, version: str) -> None:
    result = subprocess.run(
        [binary, "--version"],
        check=False,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    if result.returncode != 0 or result.stdout.strip() != f"epochdeck-server {version}":
        raise SmokeError(f"unexpected --version result: {result!r}")
    help_result = subprocess.run(
        [binary, "--help"],
        check=False,
        capture_output=True,
        text=True,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    if (
        help_result.returncode != 0
        or "Usage: epochdeck-server [OPTIONS]" not in help_result.stdout
    ):
        raise SmokeError(f"unexpected --help result: {help_result!r}")
    unknown = subprocess.run(
        [binary, "--unknown"],
        check=False,
        capture_output=True,
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    if unknown.returncode == 0:
        raise SmokeError("release binary accepted an unknown command-line argument")


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _request(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=2) as response:
        if response.status != 200:
            raise SmokeError(f"{url} returned HTTP {response.status}")
        payload = response.read(MAX_RESPONSE_BYTES + 1)
    if len(payload) > MAX_RESPONSE_BYTES:
        raise SmokeError(f"{url} response exceeds its bound")
    return payload


def _wait_for_health(
    process: subprocess.Popen[bytes], base_url: str, version: str
) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT_SECONDS
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise SmokeError(
                f"release server exited during startup with {process.returncode}"
            )
        try:
            health = json.loads(_request(f"{base_url}/api/v1/health"))
            if health.get("status") == "healthy" and health.get("version") == version:
                return
            last_error = SmokeError(f"unexpected health response: {health!r}")
        except (
            OSError,
            TimeoutError,
            urllib.error.URLError,
            json.JSONDecodeError,
            SmokeError,
        ) as error:
            last_error = error
        time.sleep(0.25)
    raise SmokeError(f"release server did not become healthy: {last_error}")


def _stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        process.send_signal(signal.CTRL_BREAK_EVENT)
    else:
        process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
        raise SmokeError("release server did not shut down gracefully")


def _smoke_server(binary: Path, runtime: Path, version: str) -> None:
    runtime.mkdir(parents=True)
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    logo = runtime / "logo.svg"
    logo_bytes = (
        b'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1 1">'
        b'<path d="M0 0h1v1z"/></svg>'
    )
    logo.write_bytes(logo_bytes)
    environment = os.environ.copy()
    environment.update(
        {
            "EPOCHDECK_BIND": f"127.0.0.1:{port}",
            "EPOCHDECK_DATA_DIR": str(runtime / "data"),
            "EPOCHDECK_METRICS_DIR": str(runtime / "metrics"),
            "EPOCHDECK_BLOBS_DIR": str(runtime / "blobs"),
            "EPOCHDECK_DASHBOARD_ACCENT_COLOR": "#8a3ffc",
            "EPOCHDECK_DASHBOARD_LOGO_PATH": str(logo),
        }
    )
    log_path = runtime / "server.log"
    with log_path.open("wb") as log:
        process = subprocess.Popen(
            [binary],
            env=environment,
            stdout=log,
            stderr=subprocess.STDOUT,
            creationflags=(
                subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
            ),
        )
        try:
            _wait_for_health(process, base_url, version)
            if b"<title>EpochDeck</title>" not in _request(f"{base_url}/"):
                raise SmokeError(
                    "embedded dashboard does not contain the EpochDeck title"
                )
            config = json.loads(_request(f"{base_url}/api/v1/dashboard/config"))
            if (
                config.get("accent_color") != "#8a3ffc"
                or config.get("logo_url") != "/api/v1/dashboard/logo"
            ):
                raise SmokeError(f"unexpected dashboard configuration: {config!r}")
            if _request(f"{base_url}/api/v1/dashboard/logo") != logo_bytes:
                raise SmokeError(
                    "served dashboard logo differs from the configured logo"
                )
        finally:
            _stop(process)
    if process.returncode != 0:
        raise SmokeError(f"release server exited with {process.returncode}")


def main() -> int:
    arguments = _parse_args()
    archive = arguments.archive.resolve(strict=True)
    root_name = f"epochdeck-server-{arguments.version}-{arguments.target}"
    extension, binary_name = TARGET_ARCHIVES[arguments.target]
    if archive.name != f"{root_name}.{extension}":
        raise SmokeError(f"unexpected release archive name: {archive.name}")
    with tempfile.TemporaryDirectory(prefix="epochdeck-smoke-") as temporary:
        destination = Path(temporary)
        if archive.name.endswith(".tar.gz"):
            _extract_tar(archive, destination)
        elif archive.suffix == ".zip":
            _extract_zip(archive, destination)
        else:
            raise SmokeError(f"unsupported archive format: {archive.name}")
        binary = destination / root_name / binary_name
        if not binary.is_file() or binary.is_symlink():
            raise SmokeError(f"archive is missing its server binary: {binary}")
        _run_cli(binary, arguments.version)
        _smoke_server(binary, destination / "runtime", arguments.version)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, SmokeError) as error:
        print(f"smoke test failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
