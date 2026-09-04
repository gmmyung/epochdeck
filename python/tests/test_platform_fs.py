from __future__ import annotations

import errno
import multiprocessing
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

import epochdeck._platform_fs as platform_fs


def _hold_process_lock(
    path: str,
    messages: multiprocessing.queues.Queue,
    release: multiprocessing.synchronize.Event,
) -> None:
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        platform_fs.acquire_file_lock(descriptor)
        messages.put("locked")
        if not release.wait(10):
            messages.put("release timed out")
            return
        platform_fs.release_file_lock(descriptor)
        messages.put("released")
    except BaseException as error:
        messages.put(f"{type(error).__name__}: {error}")
    finally:
        os.close(descriptor)


def test_native_file_lock_rejects_another_process_and_releases(tmp_path) -> None:
    context = multiprocessing.get_context("spawn")
    messages = context.Queue()
    release = context.Event()
    lock_path = tmp_path / "native.lock"
    process = context.Process(
        target=_hold_process_lock,
        args=(os.fspath(lock_path), messages, release),
    )
    process.start()
    try:
        assert messages.get(timeout=10) == "locked"
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            with pytest.raises(platform_fs.FileLockUnavailable):
                platform_fs.acquire_file_lock(descriptor)
        finally:
            os.close(descriptor)
    finally:
        release.set()
        process.join(10)
        if process.is_alive():
            process.terminate()
            process.join(5)
    assert process.exitcode == 0
    assert messages.get(timeout=2) == "released"

    descriptor = os.open(lock_path, os.O_RDWR)
    try:
        platform_fs.acquire_file_lock(descriptor)
        platform_fs.release_file_lock(descriptor)
    finally:
        os.close(descriptor)


def test_msvcrt_backend_locks_byte_zero_without_modifying_file(monkeypatch, tmp_path) -> None:
    calls: list[tuple[int, int]] = []

    class FakeMsvcrt:
        LK_NBLCK = 2
        LK_UNLCK = 0

        @staticmethod
        def locking(descriptor: int, mode: int, length: int) -> None:
            assert os.lseek(descriptor, 0, os.SEEK_CUR) == 0
            calls.append((mode, length))

    monkeypatch.setattr(platform_fs, "_fcntl", None)
    monkeypatch.setattr(platform_fs, "_msvcrt", FakeMsvcrt)
    path = tmp_path / "empty.lock"
    descriptor = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        platform_fs.acquire_file_lock(descriptor)
        platform_fs.release_file_lock(descriptor)
    finally:
        os.close(descriptor)

    assert path.read_bytes() == b""
    assert calls == [(FakeMsvcrt.LK_NBLCK, 1), (FakeMsvcrt.LK_UNLCK, 1)]


@pytest.mark.parametrize(
    "error_number",
    {
        errno.EACCES,
        getattr(errno, "EDEADLK", errno.EACCES),
        getattr(errno, "EDEADLOCK", errno.EACCES),
    },
)
def test_msvcrt_contention_has_one_platform_neutral_error(
    monkeypatch, tmp_path, error_number
) -> None:
    class ContendedMsvcrt:
        LK_NBLCK = 2
        LK_UNLCK = 0

        @staticmethod
        def locking(descriptor: int, mode: int, length: int) -> None:
            raise OSError(error_number, "locked")

    monkeypatch.setattr(platform_fs, "_fcntl", None)
    monkeypatch.setattr(platform_fs, "_msvcrt", ContendedMsvcrt)
    descriptor = os.open(tmp_path / "contended.lock", os.O_RDWR | os.O_CREAT, 0o600)
    try:
        with pytest.raises(platform_fs.FileLockUnavailable):
            platform_fs.acquire_file_lock(descriptor)
    finally:
        os.close(descriptor)


def test_windows_directory_sync_validates_without_opening_directory(monkeypatch, tmp_path) -> None:
    directory = tmp_path / "directory"
    directory.mkdir()
    verified: list[Path] = []

    monkeypatch.setattr(platform_fs, "IS_WINDOWS", True)
    monkeypatch.setattr(platform_fs, "verify_directory", verified.append)
    platform_fs.sync_directory(directory)

    assert verified == [directory]


def test_native_regular_file_sync_flushes_a_file(tmp_path) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"payload")

    platform_fs.sync_regular_file(path)

    assert path.read_bytes() == b"payload"


def test_windows_regular_file_sync_requests_write_access(monkeypatch, tmp_path) -> None:
    path = tmp_path / "payload.bin"
    path.write_bytes(b"payload")
    opened: list[tuple[Path, int]] = []
    synced: list[int] = []
    closed: list[int] = []

    monkeypatch.setattr(platform_fs, "IS_WINDOWS", True)
    monkeypatch.setattr(
        platform_fs,
        "open_regular_file_descriptor",
        lambda requested, flags: opened.append((requested, flags)) or 41,
    )
    monkeypatch.setattr(platform_fs.os, "fsync", synced.append)
    monkeypatch.setattr(platform_fs.os, "close", closed.append)

    platform_fs.sync_regular_file(path)

    assert opened == [(path, os.O_RDWR)]
    assert synced == [41]
    assert closed == [41]


def test_reparse_points_are_treated_as_links() -> None:
    status = SimpleNamespace(st_mode=0o040755, st_file_attributes=0x400)
    assert platform_fs.is_link_or_reparse(status) is True
