from __future__ import annotations

import importlib

import pytest

from epochdeck.trace import Trace

trace_module = importlib.import_module("epochdeck.trace")


def test_trace_rejects_oversized_inputs_and_attributes_at_construction(monkeypatch) -> None:
    monkeypatch.setattr(trace_module, "_MAX_TRACE_PAYLOAD_BYTES", 128)
    with pytest.raises(ValueError, match="serialized trace inputs exceeds 128 bytes"):
        Trace("generate", recorder=lambda _: None, inputs={"prompt": "x" * 256})

    monkeypatch.setattr(trace_module, "_MAX_TRACE_METADATA_BYTES", 64)
    with pytest.raises(ValueError, match="serialized trace attributes exceeds 64 bytes"):
        Trace("generate", recorder=lambda _: None, attributes={"detail": "x" * 128})


def test_trace_message_growth_is_rejected_before_mutating_the_trace(monkeypatch) -> None:
    monkeypatch.setattr(trace_module, "_MAX_TRACE_PAYLOAD_BYTES", 200)
    trace = Trace("generate", recorder=lambda _: None)
    trace.add_message("user", "x" * 60)

    with pytest.raises(ValueError, match="serialized trace payload exceeds 200 bytes"):
        trace.add_message("assistant", "y" * 60)

    assert len(trace.messages) == 1


def test_trace_final_payload_check_precedes_cas_install(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(trace_module, "_MAX_TRACE_PAYLOAD_BYTES", 200)

    def record(trace: Trace) -> None:
        trace._prepare(tmp_path / "blobs", None)

    trace = Trace("generate", recorder=record)
    trace.messages.append({"role": "user", "content": "x" * 256})

    with pytest.raises(ValueError, match="serialized trace payload exceeds 200 bytes"):
        trace.finish()

    assert not (tmp_path / "blobs").exists()
    assert not trace.finished


def test_trace_json_is_depth_bounded_and_json_safe() -> None:
    nested: object = "leaf"
    for _ in range(66):
        nested = {"child": nested}

    with pytest.raises(ValueError, match="trace inputs nesting exceeds 64 levels"):
        Trace("generate", recorder=lambda _: None, inputs=nested)
    with pytest.raises(ValueError, match="JSON-safe range"):
        Trace("generate", recorder=lambda _: None, attributes={"unsafe": 2**53})

    trace = Trace("generate", recorder=lambda _: None)
    with pytest.raises(ValueError, match="JSON-safe range"):
        trace.add_message("user", {"unsafe": -(2**53)})
    assert trace.messages == []


def test_trace_aggregate_node_budget_is_checked_before_message_append(monkeypatch) -> None:
    monkeypatch.setattr(trace_module, "_MAX_TRACE_PAYLOAD_NODES", 8)
    trace = Trace("generate", recorder=lambda _: None)
    trace.add_message("user", "first")

    with pytest.raises(ValueError, match="trace payload cannot exceed 8 JSON value nodes"):
        trace.add_message("assistant", "second")

    assert len(trace.messages) == 1
