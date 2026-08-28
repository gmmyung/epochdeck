from __future__ import annotations

import json
import time
from collections.abc import Callable, Mapping
from copy import deepcopy
from pathlib import Path
from typing import Any, Literal

from runloom._ids import uuid7
from runloom.rich import _install_bytes

TraceKind = Literal["span", "llm", "tool", "chain", "agent"]
TraceStatus = Literal["unset", "ok", "error"]

_TRACE_KINDS = {"span", "llm", "tool", "chain", "agent"}
_TRACE_STATUSES = {"unset", "ok", "error"}
_MAX_NAME_BYTES = 256
_MAX_TRACE_ID_BYTES = 128
_MAX_PREVIEW_MESSAGES = 8
_MAX_PREVIEW_CONTENT_BYTES = 4_096


class Trace:
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
            start_time_ms is not None and (not isinstance(start_time_ms, int) or start_time_ms < 0)
        ):
            raise ValueError("start_time_ms must be a non-negative integer or None")

        self.id = uuid7()
        self.name = name
        self.kind = kind
        self.parent_span_id = parent.id if isinstance(parent, Trace) else parent
        inherited_trace_id = parent.trace_id if isinstance(parent, Trace) else None
        self.trace_id = trace_id or inherited_trace_id or self.id
        self.attributes = deepcopy(dict(attributes or {}))
        self.inputs = deepcopy(inputs)
        self.outputs: Any = None
        self.messages: list[dict[str, Any]] = []
        self.start_time_ms = time.time_ns() // 1_000_000 if start_time_ms is None else start_time_ms
        self.end_time_ms: int | None = None
        self.status: TraceStatus = "unset"
        self._recorder = recorder
        self._finished = False

    def __enter__(self) -> Trace:
        return self

    def __exit__(self, exception_type: object, exception: object, traceback: object) -> None:
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
        message: dict[str, Any] = {"role": role, "content": deepcopy(content)}
        if name is not None:
            message["name"] = name
        if metadata is not None:
            message["metadata"] = deepcopy(dict(metadata))
        self.messages.append(message)
        return self

    def set_inputs(self, inputs: Any) -> Trace:
        self._ensure_open()
        self.inputs = deepcopy(inputs)
        return self

    def set_outputs(self, outputs: Any) -> Trace:
        self._ensure_open()
        self.outputs = deepcopy(outputs)
        return self

    def set_attribute(self, key: str, value: Any) -> Trace:
        self._ensure_open()
        _validate_text(key, "trace attribute key", 256)
        self.attributes[key] = deepcopy(value)
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
            end_time_ms is not None and (not isinstance(end_time_ms, int) or end_time_ms < 0)
        ):
            raise ValueError("end_time_ms must be a non-negative integer or None")
        selected_end = time.time_ns() // 1_000_000 if end_time_ms is None else end_time_ms
        if selected_end < self.start_time_ms:
            raise ValueError("trace end time cannot precede its start time")
        if outputs is not None:
            self.outputs = deepcopy(outputs)
        self.status = status
        self.end_time_ms = selected_end
        self._recorder(self)
        self._finished = True

    def _prepare(self, blob_root: Path, step: int | None) -> dict[str, Any]:
        if self.end_time_ms is None:
            raise RuntimeError("cannot prepare an unfinished trace")
        payload_bytes = _json_bytes(
            {
                "inputs": self.inputs,
                "outputs": self.outputs,
                "messages": self.messages,
            }
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
            "attributes": deepcopy(self.attributes),
            "preview": {
                "messages": [
                    _preview_message(message) for message in self.messages[:_MAX_PREVIEW_MESSAGES]
                ],
                "message_count": len(self.messages),
            },
            "payload": {
                "digest": digest,
                "size": size,
                "mime_type": "application/vnd.runloom.trace+json",
                "file_name": None,
            },
        }

    def _ensure_open(self) -> None:
        if self._finished:
            raise RuntimeError("trace is already finished")


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


def _json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise TypeError(f"trace payload must be JSON-compatible: {error}") from error


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
