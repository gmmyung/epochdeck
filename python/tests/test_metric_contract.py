from __future__ import annotations

import importlib
import json
from collections.abc import Iterator, Mapping
from pathlib import Path

import pytest

from runloom.run import create_run

run_module = importlib.import_module("runloom.run")


def test_boolean_metrics_are_journaled_and_summarized_as_numbers(tmp_path: Path) -> None:
    run = create_run(project="demo", mode="offline", spool_root=tmp_path)
    run.log({"ready": True, "nested": {"done": False}})

    event = json.loads((tmp_path / run.id / "events.jsonl").read_text().splitlines()[0])
    assert event["metrics"] == {"nested/done": 0.0, "ready": 1.0}
    assert run.summary["ready"] == 1.0
    assert run.summary["nested/done"] == 0.0
    run.finish()


def test_log_rejects_deep_nesting_without_recursion_error(tmp_path: Path) -> None:
    nested: object = 1.0
    for _ in range(66):
        nested = {"x": nested}
    run = create_run(project="demo", mode="offline", spool_root=tmp_path)

    with pytest.raises(ValueError, match="log data nesting exceeds 64 levels"):
        run.log({"root": nested})

    assert (tmp_path / run.id / "events.jsonl").read_bytes() == b""
    run.finish()


def test_log_rejects_non_string_and_colliding_flattened_keys(tmp_path: Path) -> None:
    run = create_run(project="demo", mode="offline", spool_root=tmp_path)

    with pytest.raises(TypeError, match="log keys must be strings"):
        run.log({1: 1.0})  # type: ignore[dict-item]
    with pytest.raises(ValueError, match="flattened log key collision: 'train/loss'"):
        run.log({"train/loss": 1.0, "train": {"loss": 2.0}})

    assert (tmp_path / run.id / "events.jsonl").read_bytes() == b""
    run.finish()


def test_log_stops_iterating_at_the_metric_count_bound(tmp_path: Path) -> None:
    class LargeMapping(Mapping[str, float]):
        def __init__(self) -> None:
            self.reads = 0

        def __len__(self) -> int:
            return 100_000

        def __iter__(self) -> Iterator[str]:
            return (f"metric-{index}" for index in range(len(self)))

        def __getitem__(self, key: str) -> float:
            self.reads += 1
            return 1.0

    values = LargeMapping()
    run = create_run(project="demo", mode="offline", spool_root=tmp_path)

    with pytest.raises(ValueError, match="1 to 256 metrics"):
        run.log(values)

    assert values.reads == 257
    assert (tmp_path / run.id / "events.jsonl").read_bytes() == b""
    run.finish()


def test_log_rejects_excessive_traversal_nodes(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(run_module, "_MAX_LOG_VALUE_NODES", 4)
    run = create_run(project="demo", mode="offline", spool_root=tmp_path)

    with pytest.raises(ValueError, match="cannot exceed 4 traversed value nodes"):
        run.log({"a": {}, "b": {}, "c": {}, "d": {}})

    assert (tmp_path / run.id / "events.jsonl").read_bytes() == b""
    run.finish()
