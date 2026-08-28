from __future__ import annotations

import sqlite3

import pytest

from runloom.backup import BackupError, StorageRoots, backup_storage, restore_storage


def test_physical_backup_verifies_and_restores_split_storage(tmp_path) -> None:
    source = StorageRoots(
        data=tmp_path / "source-data",
        metrics=tmp_path / "source-metrics",
        blobs=tmp_path / "source-blobs",
    )
    source.data.mkdir()
    source.journal.mkdir()
    source.metrics.mkdir()
    (source.blobs / "sha256" / "ab").mkdir(parents=True)
    (source.blobs / "staging").mkdir()
    with sqlite3.connect(source.catalog) as database:
        database.execute("CREATE TABLE runs (id TEXT PRIMARY KEY)")
        database.execute("INSERT INTO runs VALUES ('run-1')")
    (source.journal / "pending.jsonl").write_text('{"sequence":1}\n')
    (source.metrics / "segment.parquet").write_bytes(b"parquet")
    (source.blobs / "sha256" / "ab" / "abcdef").write_bytes(b"blob")
    (source.blobs / "staging" / "orphan.tmp").write_bytes(b"partial")

    bundle = tmp_path / "backup"
    manifest = backup_storage(source, bundle)
    assert manifest["file_count"] == 4
    assert not (bundle / "blobs" / "staging").exists()

    target = StorageRoots(
        data=tmp_path / "target-data",
        metrics=tmp_path / "target-metrics",
        blobs=tmp_path / "target-blobs",
    )
    restored = restore_storage(bundle, target)
    assert restored == manifest
    with sqlite3.connect(target.catalog) as database:
        assert database.execute("SELECT id FROM runs").fetchone() == ("run-1",)
    assert (target.metrics / "segment.parquet").read_bytes() == b"parquet"
    assert (target.blobs / "sha256" / "ab" / "abcdef").read_bytes() == b"blob"

    with pytest.raises(BackupError, match="already exists"):
        restore_storage(bundle, target)


def test_backup_rejects_a_destination_inside_storage(tmp_path) -> None:
    roots = StorageRoots(
        data=tmp_path / "data",
        metrics=tmp_path / "metrics",
        blobs=tmp_path / "blobs",
    )
    roots.data.mkdir()
    with sqlite3.connect(roots.catalog) as database:
        database.execute("CREATE TABLE health (ok INTEGER)")

    with pytest.raises(BackupError, match="cannot be inside"):
        backup_storage(roots, roots.data / "backup")
