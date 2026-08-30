from __future__ import annotations

import importlib
from collections.abc import Iterator, Mapping

import pytest

from epochdeck.run import create_run

run_module = importlib.import_module("epochdeck.run")
api_module = importlib.import_module("epochdeck.api")


def test_config_rejects_excessive_nesting_without_recursion_error() -> None:
    nested: object = 1
    for _ in range(66):
        nested = {"child": nested}

    with pytest.raises(ValueError, match="config nesting exceeds 64 levels"):
        create_run(project="demo", config={"root": nested}, mode="disabled")


def test_document_rejects_excessive_json_nodes(monkeypatch) -> None:
    monkeypatch.setattr(run_module, "_MAX_DOCUMENT_NODES", 4)

    with pytest.raises(ValueError, match="cannot exceed 4 JSON value nodes"):
        run_module._normalize_document({"values": [1, 2, 3]}, "config")


def test_document_stops_iterating_once_its_byte_budget_is_exhausted() -> None:
    class LargeMapping(Mapping[str, str]):
        def __init__(self) -> None:
            self.reads = 0

        def __len__(self) -> int:
            return 100_000

        def __iter__(self) -> Iterator[str]:
            return (f"key-{index}" for index in range(len(self)))

        def __getitem__(self, key: str) -> str:
            self.reads += 1
            return "value"

    source = LargeMapping()
    with pytest.raises(ValueError, match="serialized config exceeds 64 bytes"):
        run_module._normalize_document(source, "config", maximum=64)

    assert source.reads < 100


def test_public_init_bounds_config_before_run_construction(monkeypatch) -> None:
    class LargeMapping(Mapping[str, str]):
        def __init__(self) -> None:
            self.reads = 0

        def __len__(self) -> int:
            return 100_000

        def __iter__(self) -> Iterator[str]:
            return (f"key-{index}" for index in range(len(self)))

        def __getitem__(self, key: str) -> str:
            self.reads += 1
            return "value"

    monkeypatch.setattr(api_module, "_MAX_RUN_DOCUMENT_BYTES", 64)
    source = LargeMapping()
    with pytest.raises(ValueError, match="serialized config exceeds 64 bytes"):
        api_module.init(project="demo", config=source, mode="disabled")
    assert source.reads < 100


@pytest.mark.parametrize("value", [-(2**53), 2**53])
def test_native_documents_reject_json_unsafe_integers(value: int) -> None:
    with pytest.raises(ValueError, match="JSON-safe range"):
        run_module._normalize_document({"value": value}, "config")


def test_native_documents_accept_json_safe_integer_boundaries() -> None:
    maximum = 2**53 - 1
    assert run_module._normalize_document(
        {"minimum": -maximum, "maximum": maximum},
        "config",
    ) == {"minimum": -maximum, "maximum": maximum}
