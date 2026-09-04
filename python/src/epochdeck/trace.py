from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, Literal

from epochdeck._ids import uuid7
from epochdeck._json_normalization import (
    DEFAULT_MAX_JSON_NODES,
    NormalizedJson,
    normalize_json_object,
    normalize_json_value_with_stats,
)
from epochdeck._limits import MAX_SAFE_INTEGER
from epochdeck.rich import _install_bytes

TraceKind = Literal["span", "llm", "tool", "chain", "agent"]
TraceStatus = Literal["unset", "ok", "error"]

_TRACE_KINDS = {"span", "llm", "tool", "chain", "agent"}
_TRACE_STATUSES = {"unset", "ok", "error"}
_MAX_NAME_BYTES = 256
_MAX_TRACE_ID_BYTES = 128
_MAX_PREVIEW_MESSAGES = 8
_MAX_PREVIEW_CONTENT_BYTES = 4_096
_MAX_TRACE_METADATA_BYTES = 256 * 1024
_MAX_TRACE_PAYLOAD_BYTES = 16 * 1024 * 1024
_MAX_TRACE_PAYLOAD_NODES = DEFAULT_MAX_JSON_NODES


class Trace:
    trace_id: str

    def __init__(
        self,
        name: str,
        *,
        recorder: Callable[[Trace], None],
        kind: TraceKind = "span",
        trace_id: str | None = None,
        parent: Trace | str | None = None,
        attributes: Mapping[str, Any] | None = None,
        inputs: Any = None,
        start_time_ms: int | None = None,
    ) -> None:
        _validate_text(name, "trace name", _MAX_NAME_BYTES)
        if kind not in _TRACE_KINDS:
            raise ValueError("trace kind must be 'span', 'llm', 'tool', 'chain', or 'agent'")
        if parent is not None and not isinstance(parent, (Trace, str)):
            raise TypeError("trace parent must be a Trace, span ID, or None")
        if trace_id is not None:
            _validate_text(trace_id, "trace ID", _MAX_TRACE_ID_BYTES)
        if isinstance(start_time_ms, bool) or (
            start_time_ms is not None
            and (
                not isinstance(start_time_ms, int)
                or start_time_ms < 0
                or start_time_ms > MAX_SAFE_INTEGER
            )
        ):
            raise ValueError(
                f"start_time_ms must be an integer between 0 and {MAX_SAFE_INTEGER}, or None"
            )

        self.id = uuid7()
        self.name = name
        self.kind = kind
        self.parent_span_id = parent.id if isinstance(parent, Trace) else parent
        inherited_trace_id: str | None = parent.trace_id if isinstance(parent, Trace) else None
        self.trace_id = trace_id or inherited_trace_id or self.id
        self.attributes = normalize_json_object(
            attributes if attributes is not None else {},
            "trace attributes",
            _MAX_TRACE_METADATA_BYTES,
        )
        normalized_inputs = _normalize_json_value(
            inputs,
            "trace inputs",
            _MAX_TRACE_PAYLOAD_BYTES,
        )
        self.inputs = normalized_inputs.value
        self._inputs_bytes = normalized_inputs.size
        self._inputs_nodes = normalized_inputs.nodes
        self.outputs: Any = None
        self._outputs_bytes = len(b"null")
        self._outputs_nodes = 1
        self.messages: list[dict[str, Any]] = []
        self._messages_bytes = 0
        self._messages_nodes = 0
        _validate_payload_bounds(
            self._inputs_bytes,
            self._outputs_bytes,
            self._messages_bytes,
            self._inputs_nodes,
            self._outputs_nodes,
            self._messages_nodes,
        )
        self.start_time_ms = time.time_ns() // 1_000_000 if start_time_ms is None else start_time_ms
        self.end_time_ms: int | None = None
        self.status: TraceStatus = "unset"
        self._recorder = recorder
        self._finished = False

    def __enter__(self) -> Trace:
        return self

    def __exit__(self, exception_type: object, exception: object, _traceback: object) -> None:
        if exception is None:
            self.finish(status="ok")
        else:
            self.attributes["exception.type"] = getattr(exception_type, "__name__", "Exception")
            self.attributes["exception.message"] = str(exception)
            self.finish(status="error")

    @property
    def finished(self) -> bool:
        return self._finished

    def add_message(
        self,
        role: str,
        content: Any,
        *,
        name: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Trace:
        self._ensure_open()
        _validate_text(role, "message role", 64)
        if name is not None:
            _validate_text(name, "message name", 256)
        message: dict[str, Any] = {"role": role, "content": content}
        if name is not None:
            message["name"] = name
        if metadata is not None:
            message["metadata"] = metadata
        normalized = _normalize_json_value(
            message,
            "trace message",
            _MAX_TRACE_PAYLOAD_BYTES,
        )
        if not isinstance(normalized.value, dict):
            raise TypeError("trace message must be a JSON object")
        separator_bytes = 1 if self.messages else 0
        _validate_payload_bounds(
            self._inputs_bytes,
            self._outputs_bytes,
            self._messages_bytes + separator_bytes + normalized.size,
            self._inputs_nodes,
            self._outputs_nodes,
            self._messages_nodes + normalized.nodes,
        )
        self.messages.append(normalized.value)
        self._messages_bytes += separator_bytes + normalized.size
        self._messages_nodes += normalized.nodes
        return self

    def set_inputs(self, inputs: Any) -> Trace:
        self._ensure_open()
        normalized = _normalize_json_value(
            inputs,
            "trace inputs",
            _MAX_TRACE_PAYLOAD_BYTES,
        )
        _validate_payload_bounds(
            normalized.size,
            self._outputs_bytes,
            self._messages_bytes,
            normalized.nodes,
            self._outputs_nodes,
            self._messages_nodes,
        )
        self.inputs = normalized.value
        self._inputs_bytes = normalized.size
        self._inputs_nodes = normalized.nodes
        return self

    def set_outputs(self, outputs: Any) -> Trace:
        self._ensure_open()
        self._set_outputs(outputs)
        return self

    def set_attribute(self, key: str, value: Any) -> Trace:
        self._ensure_open()
        _validate_text(key, "trace attribute key", 256)
        self.attributes = normalize_json_object(
            {**self.attributes, key: value},
            "trace attributes",
            _MAX_TRACE_METADATA_BYTES,
        )
        return self

    def finish(
        self,
        *,
        status: TraceStatus = "ok",
        outputs: Any = None,
        end_time_ms: int | None = None,
    ) -> None:
        self._ensure_open()
        if status not in _TRACE_STATUSES:
            raise ValueError("trace status must be 'unset', 'ok', or 'error'")
        if isinstance(end_time_ms, bool) or (
            end_time_ms is not None
            and (
                not isinstance(end_time_ms, int)
                or end_time_ms < 0
                or end_time_ms > MAX_SAFE_INTEGER
            )
        ):
            raise ValueError(
                f"end_time_ms must be an integer between 0 and {MAX_SAFE_INTEGER}, or None"
            )
        selected_end = time.time_ns() // 1_000_000 if end_time_ms is None else end_time_ms
        if selected_end < self.start_time_ms:
            raise ValueError("trace end time cannot precede its start time")
        if outputs is not None:
            self._set_outputs(outputs)
        self.status = status
        self.end_time_ms = selected_end
        self._recorder(self)
        self._finished = True

    def _prepare(self, blob_root: Path, step: int | None) -> dict[str, Any]:
        if self.end_time_ms is None:
            raise RuntimeError("cannot prepare an unfinished trace")
        attributes = normalize_json_object(
            self.attributes,
            "trace attributes",
            _MAX_TRACE_METADATA_BYTES,
        )
        normalized_payload = normalize_json_value_with_stats(
            {
                "inputs": self.inputs,
                "outputs": self.outputs,
                "messages": self.messages,
            },
            "trace payload",
            _MAX_TRACE_PAYLOAD_BYTES,
            maximum_nodes=_MAX_TRACE_PAYLOAD_NODES,
        )
        if not isinstance(normalized_payload.value, dict):
            raise TypeError("trace payload must be a JSON object")
        normalized_messages = normalized_payload.value["messages"]
        if not isinstance(normalized_messages, list):
            raise TypeError("trace messages must be a JSON array")
        if not all(isinstance(message, Mapping) for message in normalized_messages):
            raise TypeError("trace messages must contain JSON objects")
        previews = [
            _preview_message(message) for message in normalized_messages[:_MAX_PREVIEW_MESSAGES]
        ]
        payload_bytes = _bounded_json_bytes(
            normalized_payload.value,
            "trace payload",
            _MAX_TRACE_PAYLOAD_BYTES,
        )
        digest, size = _install_bytes(blob_root, payload_bytes)
        return {
            "id": self.id,
            "trace_id": self.trace_id,
            "parent_span_id": self.parent_span_id,
            "name": self.name,
            "kind": self.kind,
            "status": self.status,
            "start_time_ms": self.start_time_ms,
            "end_time_ms": self.end_time_ms,
            "step": step,
            "attributes": attributes,
            "preview": {
                "messages": previews,
                "message_count": len(normalized_messages),
            },
            "payload": {
                "digest": digest,
                "size": size,
                "mime_type": "application/vnd.epochdeck.trace+json",
                "file_name": None,
            },
        }

    def _ensure_open(self) -> None:
        if self._finished:
            raise RuntimeError("trace is already finished")

    def _set_outputs(self, outputs: Any) -> None:
        normalized = _normalize_json_value(
            outputs,
            "trace outputs",
            _MAX_TRACE_PAYLOAD_BYTES,
        )
        _validate_payload_bounds(
            self._inputs_bytes,
            normalized.size,
            self._messages_bytes,
            self._inputs_nodes,
            normalized.nodes,
            self._messages_nodes,
        )
        self.outputs = normalized.value
        self._outputs_bytes = normalized.size
        self._outputs_nodes = normalized.nodes


def _preview_message(message: Mapping[str, Any]) -> dict[str, Any]:
    preview = {"role": message["role"]}
    if "name" in message:
        preview["name"] = message["name"]
    content = message.get("content")
    if isinstance(content, str):
        preview["content"] = _truncate_utf8(content, _MAX_PREVIEW_CONTENT_BYTES)
    else:
        preview["content"] = _truncate_utf8(
            json.dumps(content, ensure_ascii=False, allow_nan=False, separators=(",", ":")),
            _MAX_PREVIEW_CONTENT_BYTES,
        )
    return preview


def _bounded_json_bytes(value: Any, name: str, maximum: int) -> bytes:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (RecursionError, TypeError, ValueError) as error:
        raise TypeError(f"{name} must be JSON-compatible: {error}") from error
    if len(encoded) > maximum:
        raise ValueError(f"serialized {name} exceeds {maximum} bytes")
    return encoded


def _normalize_json_value(value: Any, name: str, maximum: int) -> NormalizedJson:
    return normalize_json_value_with_stats(
        value,
        name,
        maximum,
        maximum_nodes=_MAX_TRACE_PAYLOAD_NODES,
    )


def _validate_payload_bounds(
    inputs_bytes: int,
    outputs_bytes: int,
    messages_bytes: int,
    inputs_nodes: int,
    outputs_nodes: int,
    messages_nodes: int,
) -> None:
    size = (
        len(b'{"inputs":')
        + inputs_bytes
        + len(b',"messages":[')
        + messages_bytes
        + len(b'],"outputs":')
        + outputs_bytes
        + len(b"}")
    )
    if size > _MAX_TRACE_PAYLOAD_BYTES:
        raise ValueError(f"serialized trace payload exceeds {_MAX_TRACE_PAYLOAD_BYTES} bytes")
    payload_nodes = inputs_nodes + outputs_nodes + messages_nodes + 2
    if payload_nodes > _MAX_TRACE_PAYLOAD_NODES:
        raise ValueError(f"trace payload cannot exceed {_MAX_TRACE_PAYLOAD_NODES} JSON value nodes")


def _validate_text(value: Any, name: str, maximum: int) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    encoded = value.encode("utf-8")
    if (
        not encoded
        or len(encoded) > maximum
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise ValueError(f"{name} must contain 1 to {maximum} non-control bytes")


def _truncate_utf8(value: str, maximum: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= maximum:
        return value
    return encoded[:maximum].decode("utf-8", errors="ignore")
