from __future__ import annotations

import importlib
from collections.abc import Iterator, Mapping
from pathlib import Path

import pytest

from epochdeck import Artifact, Audio, Histogram, Image, Table

artifact_module = importlib.import_module("epochdeck.artifact")


def test_table_stops_reading_a_row_after_the_column_bound(tmp_path: Path) -> None:
    reads = 0

    def cells() -> Iterator[int]:
        nonlocal reads
        for value in range(100_000):
            reads += 1
            yield value

    table = Table(columns=["value"], data=[cells()])  # type: ignore[list-item]
    with pytest.raises(ValueError, match="expected 1"):
        table._prepare(tmp_path / "blobs")

    assert reads == 2


def test_table_stops_reading_columns_at_the_explicit_bound() -> None:
    reads = 0

    def columns() -> Iterator[str]:
        nonlocal reads
        for index in range(100_000):
            reads += 1
            yield f"column-{index}"

    with pytest.raises(ValueError, match="1 to 1024 columns"):
        Table(columns=columns(), data=[])  # type: ignore[arg-type]

    assert reads == 1_025


def test_histogram_stops_reading_counts_at_the_bin_bound() -> None:
    reads = 0

    def counts() -> Iterator[float]:
        nonlocal reads
        for _ in range(100_000):
            reads += 1
            yield 1.0

    with pytest.raises(ValueError, match="cannot exceed 512 values"):
        Histogram(np_histogram=(counts(), [0.0, 1.0]))  # type: ignore[arg-type]

    assert reads == 513


def test_artifact_stops_reading_aliases_at_the_manifest_bound(tmp_path: Path) -> None:
    reads = 0

    def aliases() -> Iterator[str]:
        nonlocal reads
        for index in range(100_000):
            reads += 1
            yield f"alias-{index}"

    artifact = Artifact("policy", type="model")
    with pytest.raises(ValueError, match="cannot contain more than 256 aliases"):
        artifact._prepare(tmp_path / "blobs", aliases())  # type: ignore[arg-type]

    assert reads == 257


def test_artifact_metadata_stops_iterating_at_its_byte_bound(monkeypatch) -> None:
    class LargeMetadata(Mapping[str, str]):
        def __init__(self) -> None:
            self.reads = 0

        def __len__(self) -> int:
            return 100_000

        def __iter__(self) -> Iterator[str]:
            return (f"key-{index}" for index in range(len(self)))

        def __getitem__(self, key: str) -> str:
            self.reads += 1
            return "value"

    monkeypatch.setattr(artifact_module, "MAX_ARTIFACT_METADATA_BYTES", 64)
    metadata = LargeMetadata()
    with pytest.raises(ValueError, match="serialized artifact metadata exceeds 64 bytes"):
        Artifact("policy", type="model", metadata=metadata)

    assert metadata.reads < 100


def test_artifact_directory_walk_has_an_explicit_directory_bound(monkeypatch, tmp_path) -> None:
    root = tmp_path / "source"
    root.mkdir()

    def oversized_walk(path: Path, *, followlinks: bool):
        assert path == root
        assert followlinks is False
        yield str(root), [f"empty-{index}" for index in range(4_096)], []

    monkeypatch.setattr(artifact_module.os, "walk", oversized_walk)
    artifact = Artifact("policy", type="model")
    with pytest.raises(ValueError, match="cannot exceed 4096 directories"):
        artifact.add_dir(root)
    assert artifact._files == {}


def test_media_descriptor_validation_precedes_blob_copy(monkeypatch, tmp_path) -> None:
    install_calls = 0

    def install_bytes(blob_root: Path, data: bytes) -> tuple[str, int]:
        nonlocal install_calls
        install_calls += 1
        return "0" * 64, len(data)

    monkeypatch.setattr("epochdeck.rich._install_bytes", install_bytes)
    with pytest.raises(ValueError, match="mime_type"):
        Image(b"large-media", mime_type="bad\n/type")
    with pytest.raises(ValueError, match="serialized image metadata exceeds"):
        Image(b"large-media", caption="x" * (256 * 1024))
    with pytest.raises(ValueError, match="sample_rate"):
        Audio(b"large-media", sample_rate=2**53)
    assert install_calls == 0
    assert not (tmp_path / "blobs").exists()
