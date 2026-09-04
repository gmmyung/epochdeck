from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

import epochdeck.backup as backup_module
from epochdeck._platform_fs import acquire_file_lock, release_file_lock
from epochdeck.backup import BackupError, StorageRoots, backup_storage, restore_storage


def test_physical_backup_verifies_and_restores_split_storage(tmp_path) -> None:
    source = StorageRoots(
        data=tmp_path / "source-data",
        metrics=tmp_path / "source-metrics",
        blobs=tmp_path / "source-blobs",
    )
    source.data.mkdir()
    source.metrics.mkdir()
    (source.metrics / "staging").mkdir()
    (source.blobs / "sha256" / "ab").mkdir(parents=True)
    (source.blobs / "staging").mkdir()
    with closing(sqlite3.connect(source.catalog)) as database, database:
        database.execute("CREATE TABLE runs (id TEXT PRIMARY KEY)")
        database.execute("INSERT INTO runs VALUES ('run-1')")
    (source.metrics / "segment.parquet").write_bytes(b"parquet")
    (source.metrics / "staging" / "orphan.parquet").write_bytes(b"partial")
    (source.blobs / "sha256" / "ab" / "abcdef").write_bytes(b"blob")
    (source.blobs / "staging" / "orphan.tmp").write_bytes(b"partial")

    bundle = tmp_path / "backup"
    manifest = backup_storage(source, bundle)
    assert manifest["file_count"] == 3
    assert set(manifest) == {
        "created_at",
        "file_count",
        "format",
        "total_bytes",
    }
    assert not (bundle / "journal").exists()
    assert not (bundle / "metrics" / "staging").exists()
    assert not (bundle / "blobs" / "staging").exists()
    assert not (bundle / "metrics" / "epochdeck.lock").exists()
    assert not (bundle / "blobs" / "epochdeck.lock").exists()
    assert all(
        (root / "epochdeck.lock").is_file() for root in (source.data, source.metrics, source.blobs)
    )
    if os.name != "nt":
        assert bundle.stat().st_mode & 0o777 == 0o700
        for path in [
            bundle / "catalog.sqlite3",
            bundle / "files.jsonl",
            bundle / "manifest.json",
            bundle / "metrics" / "segment.parquet",
            bundle / "blobs" / "sha256" / "ab" / "abcdef",
        ]:
            assert path.stat().st_mode & 0o777 == 0o600

    target = StorageRoots(
        data=tmp_path / "target-data",
        metrics=tmp_path / "target-metrics",
        blobs=tmp_path / "target-blobs",
    )
    restored = restore_storage(bundle, target)
    assert restored == manifest
    with closing(sqlite3.connect(target.catalog)) as database, database:
        assert database.execute("SELECT id FROM runs").fetchone() == ("run-1",)
    assert (target.metrics / "segment.parquet").read_bytes() == b"parquet"
    assert (target.blobs / "sha256" / "ab" / "abcdef").read_bytes() == b"blob"
    if os.name != "nt":
        assert target.catalog.stat().st_mode & 0o777 == 0o600
        assert target.metrics.stat().st_mode & 0o777 == 0o700
        assert target.blobs.stat().st_mode & 0o777 == 0o700

    with pytest.raises(BackupError, match="already exists"):
        restore_storage(bundle, target)


def test_directory_tree_sync_is_bottom_up_and_depth_bounded(monkeypatch, tmp_path) -> None:
    root = tmp_path / "tree"
    leaf = root / "branch" / "leaf"
    leaf.mkdir(parents=True)
    (leaf / "payload").write_bytes(b"payload")
    synced: list[Path] = []

    if backup_module.DIRECTORY_DESCRIPTORS_SUPPORTED:
        monkeypatch.setattr(
            backup_module,
            "_fsync_directory_descriptor",
            lambda descriptor, path: synced.append(path),
        )
    else:
        monkeypatch.setattr(backup_module, "_fsync_directory", synced.append)
    backup_module._fsync_directory_tree(root)

    assert synced == [leaf, root / "branch", root]

    monkeypatch.setattr(backup_module, "_MAX_TREE_DEPTH", 8)
    deep_root = tmp_path / "deep"
    deep_root.mkdir()
    current = deep_root
    for index in range(backup_module._MAX_TREE_DEPTH + 1):
        current /= f"d{index}"
        current.mkdir()
    with pytest.raises(BackupError, match=r"exceeds .* levels while syncing"):
        backup_module._fsync_directory_tree(deep_root)


def test_windows_directory_tree_fallback_is_bottom_up(monkeypatch, tmp_path) -> None:
    root = tmp_path / "tree"
    leaf = root / "branch" / "leaf"
    leaf.mkdir(parents=True)
    (leaf / "payload").write_bytes(b"payload")
    synced: list[Path] = []

    monkeypatch.setattr(backup_module, "DIRECTORY_DESCRIPTORS_SUPPORTED", False)
    monkeypatch.setattr(backup_module, "_fsync_directory", synced.append)
    backup_module._fsync_directory_tree(root)

    assert synced == [leaf, root / "branch", root]


def test_windows_bundle_reader_fallback_verifies_each_path_component(monkeypatch, tmp_path) -> None:
    bundle = tmp_path / "bundle"
    nested = bundle / "metrics" / "nested"
    nested.mkdir(parents=True)
    payload = nested / "segment.parquet"
    payload.write_bytes(b"metrics")
    verified: list[Path] = []
    verify_directory = backup_module.verify_directory

    def record_verify(path):
        verified.append(path)
        verify_directory(path)

    monkeypatch.setattr(backup_module, "DIRECTORY_DESCRIPTORS_SUPPORTED", False)
    monkeypatch.setattr(backup_module, "verify_directory", record_verify)
    with backup_module._open_bundle_regular_file(
        bundle,
        Path("metrics", "nested", "segment.parquet"),
    ) as stream:
        assert stream.read() == b"metrics"

    assert verified == [bundle, bundle / "metrics", nested]


def test_backup_syncs_the_generated_tree_before_atomic_publish(monkeypatch, tmp_path) -> None:
    roots = StorageRoots(
        data=tmp_path / "data",
        metrics=tmp_path / "metrics",
        blobs=tmp_path / "blobs",
    )
    roots.data.mkdir()
    with closing(sqlite3.connect(roots.catalog)) as database, database:
        database.execute("CREATE TABLE health (ok INTEGER)")
    destination = tmp_path / "backup"
    events: list[tuple[str, Path]] = []
    original_sync_tree = backup_module._fsync_directory_tree
    original_replace = backup_module.os.replace

    def record_sync_tree(path):
        events.append(("sync-tree", Path(path)))
        original_sync_tree(path)

    def record_replace(source_path, destination_path):
        if Path(destination_path) == destination:
            events.append(("publish", Path(destination_path)))
        original_replace(source_path, destination_path)

    monkeypatch.setattr(backup_module, "_fsync_directory_tree", record_sync_tree)
    monkeypatch.setattr(backup_module.os, "replace", record_replace)
    backup_storage(roots, destination)

    assert [event for event, _ in events] == ["sync-tree", "publish"]
    assert events[0][1].name.startswith(".backup.partial-")


def test_backup_closes_every_internal_sqlite_connection(monkeypatch, tmp_path) -> None:
    roots = StorageRoots(
        data=tmp_path / "data",
        metrics=tmp_path / "metrics",
        blobs=tmp_path / "blobs",
    )
    roots.data.mkdir()
    with closing(sqlite3.connect(roots.catalog)) as database, database:
        database.execute("CREATE TABLE health (ok INTEGER)")
    original_connect = sqlite3.connect
    connections: list[sqlite3.Connection] = []

    def tracked_connect(*args, **kwargs):
        database = original_connect(*args, **kwargs)
        connections.append(database)
        return database

    monkeypatch.setattr(backup_module.sqlite3, "connect", tracked_connect)

    backup_storage(roots, tmp_path / "backup")

    assert len(connections) == 3
    for database in connections:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            database.execute("SELECT 1")


def test_backup_rejects_a_destination_inside_storage(tmp_path) -> None:
    roots = StorageRoots(
        data=tmp_path / "data",
        metrics=tmp_path / "metrics",
        blobs=tmp_path / "blobs",
    )
    roots.data.mkdir()
    with closing(sqlite3.connect(roots.catalog)) as database, database:
        database.execute("CREATE TABLE health (ok INTEGER)")

    with pytest.raises(BackupError, match="must be disjoint"):
        backup_storage(roots, roots.data / "backup")

    ancestor = tmp_path / "ancestor"
    nested_roots = StorageRoots(
        data=ancestor / "data",
        metrics=ancestor / "metrics",
        blobs=ancestor / "blobs",
    )
    nested_roots.data.mkdir(parents=True)
    with closing(sqlite3.connect(nested_roots.catalog)) as database, database:
        database.execute("CREATE TABLE health (ok INTEGER)")

    with pytest.raises(BackupError, match="must be disjoint"):
        backup_storage(nested_roots, ancestor)


def test_backup_and_restore_reject_an_ambiguous_shared_content_root(tmp_path) -> None:
    source = StorageRoots(
        data=tmp_path / "source-data",
        metrics=tmp_path / "source-metrics",
        blobs=tmp_path / "source-blobs",
    )
    source.data.mkdir()
    with closing(sqlite3.connect(source.catalog)) as database, database:
        database.execute("CREATE TABLE runs (id TEXT PRIMARY KEY)")
    bundle = tmp_path / "backup"
    backup_storage(source, bundle)

    shared = tmp_path / "shared"
    ambiguous = StorageRoots(data=tmp_path / "target-data", metrics=shared, blobs=shared)
    with pytest.raises(BackupError, match="must not overlap"):
        backup_storage(ambiguous, tmp_path / "other-backup")
    with pytest.raises(BackupError, match="must not overlap"):
        restore_storage(bundle, ambiguous)

    nested = StorageRoots(
        data=tmp_path / "nested-data",
        metrics=shared,
        blobs=shared / "blobs",
    )
    with pytest.raises(BackupError, match="must not overlap"):
        backup_storage(nested, tmp_path / "nested-backup")
    with pytest.raises(BackupError, match="must not overlap"):
        restore_storage(bundle, nested)

    for invalid in [
        StorageRoots(
            data=shared,
            metrics=shared,
            blobs=tmp_path / "equal-data-blob",
        ),
        StorageRoots(
            data=shared / "nested-data",
            metrics=shared,
            blobs=tmp_path / "nested-data-blob",
        ),
        StorageRoots(
            data=shared / "blob-data",
            metrics=tmp_path / "blob-data-metrics",
            blobs=shared,
        ),
    ]:
        with pytest.raises(BackupError, match="data storage root"):
            backup_storage(invalid, tmp_path / f"invalid-backup-{invalid.data.name}")
        with pytest.raises(BackupError, match="data storage root"):
            restore_storage(bundle, invalid)


def test_storage_roots_allow_data_to_contain_disjoint_metric_and_blob_roots(tmp_path) -> None:
    data = tmp_path / "data"
    backup_module._validate_storage_roots(
        StorageRoots(
            data=data,
            metrics=data / "metrics",
            blobs=data / "blobs",
        )
    )


def test_backup_acquires_every_canonical_storage_root_lock(tmp_path) -> None:
    roots = StorageRoots(
        data=tmp_path / "data",
        metrics=tmp_path / "external-metrics",
        blobs=tmp_path / "external-blobs",
    )
    for root in (roots.data, roots.metrics, roots.blobs):
        root.mkdir()
    with closing(sqlite3.connect(roots.catalog)) as database, database:
        database.execute("CREATE TABLE health (ok INTEGER)")

    with (roots.metrics / "epochdeck.lock").open("a+b") as active_metric_lock:
        acquire_file_lock(active_metric_lock.fileno())
        try:
            with pytest.raises(BackupError, match="storage is active"):
                backup_storage(roots, tmp_path / "blocked-backup")
        finally:
            release_file_lock(active_metric_lock.fileno())

    with (roots.data / "epochdeck.lock").open("a+b") as released_data_lock:
        acquire_file_lock(released_data_lock.fileno())
        release_file_lock(released_data_lock.fileno())

    backup_storage(roots, tmp_path / "backup")
    assert all(
        (root / "epochdeck.lock").is_file() for root in (roots.data, roots.metrics, roots.blobs)
    )


def test_restore_rejects_invalid_inventory_metadata(tmp_path) -> None:
    source = StorageRoots(
        data=tmp_path / "source-data",
        metrics=tmp_path / "source-metrics",
        blobs=tmp_path / "source-blobs",
    )
    source.data.mkdir()
    with closing(sqlite3.connect(source.catalog)) as database, database:
        database.execute("CREATE TABLE runs (id TEXT PRIMARY KEY)")
    bundle = tmp_path / "backup"
    backup_storage(source, bundle)
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    target = StorageRoots(
        data=tmp_path / "target-data",
        metrics=tmp_path / "target-metrics",
        blobs=tmp_path / "target-blobs",
    )
    manifest["file_count"] += 1
    manifest_path.write_text(json.dumps(manifest))
    with (bundle / "files.jsonl").open("a") as inventory:
        inventory.write(
            json.dumps(
                {
                    "category": "journal",
                    "path": "obsolete.jsonl",
                    "size": 0,
                    "sha256": "0" * 64,
                }
            )
            + "\n"
        )
    with pytest.raises(BackupError, match="invalid inventory entry"):
        restore_storage(bundle, target)


def test_failed_restore_rolls_back_every_installed_root(monkeypatch, tmp_path) -> None:
    source = StorageRoots(
        data=tmp_path / "source-data",
        metrics=tmp_path / "source-metrics",
        blobs=tmp_path / "source-blobs",
    )
    source.data.mkdir()
    source.metrics.mkdir()
    source.blobs.mkdir()
    with closing(sqlite3.connect(source.catalog)) as database, database:
        database.execute("CREATE TABLE runs (id TEXT PRIMARY KEY)")
    (source.metrics / "segment.parquet").write_bytes(b"parquet")
    bundle = tmp_path / "backup"
    backup_storage(source, bundle)

    target = StorageRoots(
        data=tmp_path / "target-data",
        metrics=tmp_path / "target-metrics",
        blobs=tmp_path / "target-blobs",
    )
    original_copy = backup_module._copy_verified_bundle_file
    copies = 0

    def fail_second_copy(bundle_path, entry, destination_path):
        nonlocal copies
        copies += 1
        if copies == 2:
            raise OSError("simulated destination failure")
        return original_copy(bundle_path, entry, destination_path)

    monkeypatch.setattr(backup_module, "_copy_verified_bundle_file", fail_second_copy)
    with pytest.raises(OSError, match="simulated"):
        restore_storage(bundle, target)

    assert not target.catalog.exists()
    assert [path.name for path in target.metrics.iterdir()] == ["epochdeck.lock"]
    assert [path.name for path in target.blobs.iterdir()] == ["epochdeck.lock"]
    assert list(target.data.glob(".restore-*")) == []


def test_restore_rejects_duplicate_inventory_destinations(tmp_path) -> None:
    source = StorageRoots(
        data=tmp_path / "source-data",
        metrics=tmp_path / "source-metrics",
        blobs=tmp_path / "source-blobs",
    )
    source.data.mkdir()
    source.metrics.mkdir()
    with closing(sqlite3.connect(source.catalog)) as database, database:
        database.execute("CREATE TABLE runs (id TEXT PRIMARY KEY)")
    (source.metrics / "segment.parquet").write_bytes(b"metrics")
    bundle = tmp_path / "backup"
    backup_storage(source, bundle)

    inventory_path = bundle / "files.jsonl"
    entries = [json.loads(line) for line in inventory_path.read_text().splitlines()]
    duplicate = next(entry for entry in entries if entry["category"] == "metrics")
    with inventory_path.open("a") as inventory:
        inventory.write(json.dumps(duplicate) + "\n")
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["file_count"] += 1
    manifest["total_bytes"] += duplicate["size"]
    manifest_path.write_text(json.dumps(manifest))

    target = StorageRoots(
        data=tmp_path / "target-data",
        metrics=tmp_path / "target-metrics",
        blobs=tmp_path / "target-blobs",
    )
    with pytest.raises(BackupError, match="duplicate backup inventory destination"):
        restore_storage(bundle, target)
    assert not target.catalog.exists()
    assert [path.name for path in target.metrics.iterdir()] == ["epochdeck.lock"]
    assert [path.name for path in target.blobs.iterdir()] == ["epochdeck.lock"]


@pytest.mark.parametrize("linked_source", ["manifest", "inventory", "metric"])
def test_restore_rejects_symbolic_link_bundle_sources(linked_source, tmp_path) -> None:
    source = StorageRoots(
        data=tmp_path / "source-data",
        metrics=tmp_path / "source-metrics",
        blobs=tmp_path / "source-blobs",
    )
    source.data.mkdir()
    source.metrics.mkdir()
    with closing(sqlite3.connect(source.catalog)) as database, database:
        database.execute("CREATE TABLE runs (id TEXT PRIMARY KEY)")
    (source.metrics / "segment.parquet").write_bytes(b"metrics")
    bundle = tmp_path / "backup"
    backup_storage(source, bundle)

    if linked_source == "manifest":
        source_path = bundle / "manifest.json"
    elif linked_source == "inventory":
        source_path = bundle / "files.jsonl"
    else:
        source_path = bundle / "metrics" / "segment.parquet"
    real_path = tmp_path / f"real-{source_path.name}"
    source_path.replace(real_path)
    try:
        source_path.symlink_to(real_path)
    except (NotImplementedError, OSError):
        pytest.skip("symbolic links are not available on this Windows runner")

    target = StorageRoots(
        data=tmp_path / "target-data",
        metrics=tmp_path / "target-metrics",
        blobs=tmp_path / "target-blobs",
    )
    with pytest.raises(BackupError, match="cannot safely read"):
        restore_storage(bundle, target)
    assert not target.catalog.exists()


def test_restore_rejects_a_bundle_inside_target_storage(tmp_path) -> None:
    source = StorageRoots(
        data=tmp_path / "source-data",
        metrics=tmp_path / "source-metrics",
        blobs=tmp_path / "source-blobs",
    )
    source.data.mkdir()
    with closing(sqlite3.connect(source.catalog)) as database, database:
        database.execute("CREATE TABLE runs (id TEXT PRIMARY KEY)")
    target = StorageRoots(
        data=tmp_path / "target-data",
        metrics=tmp_path / "target-metrics",
        blobs=tmp_path / "target-blobs",
    )
    bundle = target.data / "backup"
    backup_storage(source, bundle)

    with pytest.raises(BackupError, match="must be disjoint"):
        restore_storage(bundle, target)

    nested_target = StorageRoots(
        data=bundle / "target-data",
        metrics=bundle / "target-metrics",
        blobs=bundle / "target-blobs",
    )
    with pytest.raises(BackupError, match="must be disjoint"):
        restore_storage(bundle, nested_target)


def test_restore_publishes_the_catalog_after_content_roots(monkeypatch, tmp_path) -> None:
    source = StorageRoots(
        data=tmp_path / "source-data",
        metrics=tmp_path / "source-metrics",
        blobs=tmp_path / "source-blobs",
    )
    source.data.mkdir()
    source.metrics.mkdir()
    source.blobs.mkdir()
    with closing(sqlite3.connect(source.catalog)) as database, database:
        database.execute("CREATE TABLE runs (id TEXT PRIMARY KEY)")
    (source.metrics / "nested").mkdir()
    (source.metrics / "nested" / "segment.parquet").write_bytes(b"metrics")
    (source.blobs / "blob").write_bytes(b"blob")
    bundle = tmp_path / "backup"
    backup_storage(source, bundle)

    target = StorageRoots(
        data=tmp_path / "target-data",
        metrics=tmp_path / "target-metrics",
        blobs=tmp_path / "target-blobs",
    )
    original_replace = backup_module.os.replace
    published: list[Path] = []
    events: list[tuple[str, Path]] = []

    def record_replace(source_path, destination_path):
        destination = Path(destination_path)
        if (
            destination == target.catalog
            or destination.is_relative_to(target.metrics)
            or destination.is_relative_to(target.blobs)
        ):
            published.append(destination)
            events.append(("publish", destination))
        original_replace(source_path, destination_path)

    if backup_module.DIRECTORY_DESCRIPTORS_SUPPORTED:
        original_fsync_descriptor = backup_module._fsync_directory_descriptor

        def record_fsync_descriptor(descriptor, path):
            events.append(("sync", Path(path)))
            original_fsync_descriptor(descriptor, path)

        monkeypatch.setattr(
            backup_module,
            "_fsync_directory_descriptor",
            record_fsync_descriptor,
        )
    else:
        original_fsync_directory = backup_module._fsync_directory

        def record_fsync_directory(path):
            events.append(("sync", Path(path)))
            original_fsync_directory(path)

        monkeypatch.setattr(backup_module, "_fsync_directory", record_fsync_directory)
    monkeypatch.setattr(backup_module.os, "replace", record_replace)
    restore_storage(bundle, target)

    assert published == [
        target.metrics / "nested",
        target.blobs / "blob",
        target.catalog,
    ]
    first_publish = next(index for index, event in enumerate(events) if event[0] == "publish")
    catalog_publish = events.index(("publish", target.catalog))
    staging_syncs = [
        (index, path)
        for index, (event, path) in enumerate(events)
        if event == "sync" and any(".restore-" in part for part in path.parts)
    ]
    assert staging_syncs
    assert max(index for index, _ in staging_syncs) < first_publish
    assert any(path.name == "nested" for _, path in staging_syncs)
    assert events.index(("sync", target.metrics)) < catalog_publish
    assert events.index(("sync", target.blobs)) < catalog_publish


def test_backup_metadata_reads_reject_oversized_manifest_inventory_and_paths(tmp_path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "manifest.json").write_bytes(b" " * (backup_module._MAX_MANIFEST_BYTES + 1))
    with pytest.raises(BackupError, match="backup metadata exceeds"):
        backup_module._read_manifest(bundle)

    inventory = bundle / "files.jsonl"
    inventory.write_bytes(b"x" * (backup_module._MAX_INVENTORY_RECORD_BYTES + 1) + b"\n")
    with pytest.raises(BackupError, match="inventory record exceeds"):
        list(backup_module._inventory(inventory))

    oversized_path = "a" * (backup_module._MAX_RELATIVE_PATH_BYTES + 1)
    inventory.write_text(
        json.dumps(
            {
                "category": "metrics",
                "path": oversized_path,
                "size": 0,
                "sha256": "0" * 64,
            }
        )
        + "\n"
    )
    with pytest.raises(BackupError, match="invalid inventory entry"):
        list(backup_module._inventory(inventory))
    inventory.write_text(
        json.dumps(
            {
                "category": "metrics",
                "path": "epochdeck.lock",
                "size": 0,
                "sha256": "0" * 64,
            }
        )
        + "\n"
    )
    with pytest.raises(BackupError, match="invalid inventory entry"):
        list(backup_module._inventory(inventory))
    inventory.write_text(
        json.dumps(
            {
                "category": "metrics",
                "path": "hostile\nsegment.parquet",
                "size": 0,
                "sha256": "0" * 64,
            }
        )
        + "\n"
    )
    with pytest.raises(BackupError, match="invalid inventory entry"):
        list(backup_module._inventory(inventory))

    inventory.write_text(
        json.dumps(
            {
                "category": "metrics",
                "path": "segment.parquet",
                "size": 0,
                "sha256": "0" * 64,
                "ignored": True,
            }
        )
        + "\n"
    )
    with pytest.raises(BackupError, match="invalid inventory entry"):
        list(backup_module._inventory(inventory))


@pytest.mark.parametrize(
    "path",
    [
        "../escape",
        "nested\\escape",
        "C:/escape",
        "payload:stream",
        "CON",
        "nested/trailing.",
        "nested//empty",
    ],
)
def test_backup_inventory_paths_are_portable_across_release_targets(path) -> None:
    assert backup_module._safe_relative(path) is False


def test_backup_inventory_accepts_a_canonical_unicode_posix_path() -> None:
    assert backup_module._safe_relative("sha256/ab/정상.bin") is True
