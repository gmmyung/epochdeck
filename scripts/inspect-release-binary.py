#!/usr/bin/env python3
"""Verify that a release executable has the expected native object format."""

from __future__ import annotations

import argparse
import struct
import sys
from pathlib import Path
from typing import BinaryIO

EXPECTED_FORMATS = {
    "aarch64-unknown-linux-musl": ("ELF", 183),
    "x86_64-unknown-linux-musl": ("ELF", 62),
    "aarch64-apple-darwin": ("Mach-O", 0x0100000C),
    "x86_64-pc-windows-msvc": ("PE", 0x8664),
}


class InspectionError(RuntimeError):
    """The executable format or architecture is not the expected one."""


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--target", required=True, choices=sorted(EXPECTED_FORMATS))
    return parser.parse_args()


MAX_PE_HEADER_OFFSET = 1024 * 1024


def _elf_machine(source: BinaryIO) -> int:
    payload = source.read(20)
    if len(payload) < 20 or payload[:4] != b"\x7fELF":
        raise InspectionError("binary is not ELF")
    if payload[4] != 2:
        raise InspectionError("release binary is not 64-bit ELF")
    byte_order = payload[5]
    if byte_order == 1:
        return struct.unpack_from("<H", payload, 18)[0]
    if byte_order == 2:
        return struct.unpack_from(">H", payload, 18)[0]
    raise InspectionError("ELF binary has an invalid byte order")


def _mach_machine(source: BinaryIO) -> int:
    payload = source.read(8)
    if len(payload) < 8:
        raise InspectionError("binary is too short to be Mach-O")
    if payload[:4] == b"\xcf\xfa\xed\xfe":
        return struct.unpack_from("<I", payload, 4)[0]
    if payload[:4] == b"\xfe\xed\xfa\xcf":
        return struct.unpack_from(">I", payload, 4)[0]
    raise InspectionError("binary is not a thin 64-bit Mach-O executable")


def _pe_machine(source: BinaryIO) -> int:
    payload = source.read(0x40)
    if len(payload) < 0x40 or payload[:2] != b"MZ":
        raise InspectionError("binary is not PE/COFF")
    header_offset = struct.unpack_from("<I", payload, 0x3C)[0]
    if header_offset < 0x40 or header_offset > MAX_PE_HEADER_OFFSET:
        raise InspectionError("binary has an out-of-bounds PE/COFF header offset")
    source.seek(header_offset)
    header = source.read(26)
    if len(header) != 26 or header[:4] != b"PE\0\0":
        raise InspectionError("binary has an invalid PE/COFF header")
    optional_header_size = struct.unpack_from("<H", header, 20)[0]
    characteristics = struct.unpack_from("<H", header, 22)[0]
    optional_header_magic = struct.unpack_from("<H", header, 24)[0]
    if optional_header_size < 2 or optional_header_magic != 0x20B:
        raise InspectionError("release binary is not a 64-bit PE executable")
    if characteristics & 0x0002 == 0:
        raise InspectionError("PE/COFF image is not marked executable")
    return struct.unpack_from("<H", header, 4)[0]


def main() -> int:
    arguments = _parse_args()
    binary = arguments.binary.resolve(strict=True)
    if binary.is_symlink() or not binary.is_file():
        raise InspectionError(f"binary is not a regular file: {binary}")
    expected_format, expected_machine = EXPECTED_FORMATS[arguments.target]
    with binary.open("rb") as source:
        if expected_format == "ELF":
            machine = _elf_machine(source)
        elif expected_format == "Mach-O":
            machine = _mach_machine(source)
        else:
            machine = _pe_machine(source)
    if machine != expected_machine:
        raise InspectionError(
            f"{expected_format} machine {machine:#x} does not match {expected_machine:#x}"
        )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, InspectionError, struct.error) as error:
        print(f"binary inspection failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
