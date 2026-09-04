from __future__ import annotations

import errno
import os
import stat
from pathlib import Path
from typing import Any

_fcntl: Any
try:
    import fcntl as _fcntl
except ImportError:  # Windows
    _fcntl = None

_msvcrt: Any
try:
    import msvcrt as _msvcrt
except ImportError:  # POSIX
    _msvcrt = None

IS_WINDOWS = os.name == "nt"
DIRECTORY_DESCRIPTORS_SUPPORTED = not IS_WINDOWS
NO_FOLLOW = getattr(os, "O_NOFOLLOW", 0)
DIRECTORY = getattr(os, "O_DIRECTORY", 0)
ACCESS_MODE = getattr(os, "O_ACCMODE", 0b11)
_BINARY = getattr(os, "O_BINARY", 0)
_REPARSE_POINT = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
_LOCK_CONTENTION = {
    errno.EACCES,
    errno.EAGAIN,
    getattr(errno, "EDEADLK", errno.EACCES),
    getattr(errno, "EDEADLOCK", errno.EACCES),
}


class FileLockUnavailable(RuntimeError):
    pass


def acquire_file_lock(descriptor: int) -> None:
    """Acquire one non-blocking exclusive process lock on an open file."""
    if _fcntl is not None:
        try:
            _fcntl.flock(descriptor, _fcntl.LOCK_EX | _fcntl.LOCK_NB)
        except OSError as error:
            if isinstance(error, BlockingIOError) or error.errno in _LOCK_CONTENTION:
                raise FileLockUnavailable from error
            raise
        return
    if _msvcrt is None:
        raise RuntimeError("this Python platform has no supported file-locking API")

    # msvcrt.locking permits ranges beyond EOF. Locking byte zero also
    # overlaps Rust's whole-file Windows lock without modifying the lock file.
    os.lseek(descriptor, 0, os.SEEK_SET)
    try:
        _msvcrt.locking(descriptor, _msvcrt.LK_NBLCK, 1)
    except OSError as error:
        if error.errno in _LOCK_CONTENTION:
            raise FileLockUnavailable from error
        raise


def release_file_lock(descriptor: int) -> None:
    if _fcntl is not None:
        _fcntl.flock(descriptor, _fcntl.LOCK_UN)
        return
    if _msvcrt is None:
        raise RuntimeError("this Python platform has no supported file-locking API")
    os.lseek(descriptor, 0, os.SEEK_SET)
    _msvcrt.locking(descriptor, _msvcrt.LK_UNLCK, 1)


def set_descriptor_mode(descriptor: int, mode: int, *, path: Path | None = None) -> None:
    """Apply POSIX permissions when available and keep Windows files writable."""
    fchmod = getattr(os, "fchmod", None)
    if fchmod is not None:
        fchmod(descriptor, mode)
    elif path is not None:
        path.chmod(mode)


def is_link_or_reparse(status: os.stat_result) -> bool:
    return stat.S_ISLNK(status.st_mode) or bool(
        getattr(status, "st_file_attributes", 0) & _REPARSE_POINT
    )


def verify_directory(path: Path, *, private_mode: int | None = None) -> None:
    """Reject symbolic/reparse paths and verify that path is a directory."""
    before = os.lstat(path)
    if is_link_or_reparse(before):
        raise OSError(errno.ELOOP, "directory is a symbolic link or reparse point", path)
    if not stat.S_ISDIR(before.st_mode):
        raise NotADirectoryError(errno.ENOTDIR, "path is not a directory", path)

    if IS_WINDOWS:
        if private_mode is not None:
            path.chmod(private_mode)
        return

    descriptor = os.open(path, os.O_RDONLY | DIRECTORY | NO_FOLLOW)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISDIR(opened.st_mode) or not os.path.samestat(before, opened):
            raise OSError(errno.EAGAIN, "directory changed while it was opened", path)
        if private_mode is not None:
            set_descriptor_mode(descriptor, private_mode, path=path)
    finally:
        os.close(descriptor)


def open_regular_file_descriptor(
    path: Path,
    flags: int,
    *,
    create_mode: int = 0o600,
    private_mode: int | None = None,
) -> int:
    """Open a regular non-link file and reject path replacement around open."""
    try:
        before: os.stat_result | None = os.lstat(path)
    except FileNotFoundError:
        before = None
    if before is not None and is_link_or_reparse(before):
        raise OSError(errno.ELOOP, "file is a symbolic link or reparse point", path)

    descriptor = os.open(path, flags | NO_FOLLOW | _BINARY, create_mode)
    try:
        opened = os.fstat(descriptor)
        if not stat.S_ISREG(opened.st_mode):
            raise OSError(errno.EINVAL, "path is not a regular file", path)
        current = os.lstat(path)
        if is_link_or_reparse(current) or not os.path.samestat(current, opened):
            raise OSError(errno.EAGAIN, "file changed while it was opened", path)
        if before is not None and not os.path.samestat(before, opened):
            raise OSError(errno.EAGAIN, "file changed while it was opened", path)
        if private_mode is not None:
            set_descriptor_mode(descriptor, private_mode, path=path)
        return descriptor
    except BaseException:
        os.close(descriptor)
        raise


def sync_directory(path: Path) -> None:
    """Flush a directory entry on POSIX; validate it on Windows.

    CPython cannot open Windows directories with os.open, and FlushFileBuffers
    does not provide the POSIX directory-fsync contract. File contents are
    still flushed before publication; Windows rename durability is delegated
    to the filesystem.
    """
    verify_directory(path)
    if IS_WINDOWS:
        return
    descriptor = os.open(path, os.O_RDONLY | DIRECTORY | NO_FOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
