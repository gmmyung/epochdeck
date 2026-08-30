from __future__ import annotations

import json
import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from epochdeck._limits import MAX_SAFE_INTEGER

DEFAULT_MAX_JSON_DEPTH = 64
DEFAULT_MAX_JSON_NODES = 65_536


@dataclass(frozen=True, slots=True)
class NormalizedJson:
    value: Any
    size: int
    nodes: int


class _JsonBudget:
    __slots__ = ("maximum", "maximum_depth", "maximum_nodes", "name", "nodes")

    def __init__(
        self,
        name: str,
        maximum: int,
        maximum_depth: int,
        maximum_nodes: int,
    ) -> None:
        self.name = name
        self.maximum = maximum
        self.maximum_depth = maximum_depth
        self.maximum_nodes = maximum_nodes
        self.nodes = 0

    def visit(self, path: str, depth: int) -> None:
        if depth > self.maximum_depth:
            raise ValueError(f"{self.name} nesting exceeds {self.maximum_depth} levels at {path}")
        self.nodes += 1
        if self.nodes > self.maximum_nodes:
            raise ValueError(f"{self.name} cannot exceed {self.maximum_nodes} JSON value nodes")

    def validate_size(self, size: int) -> None:
        if size > self.maximum:
            raise ValueError(f"serialized {self.name} exceeds {self.maximum} bytes")


def normalize_json_object(
    values: Mapping[str, Any],
    name: str,
    maximum: int,
    *,
    maximum_depth: int = DEFAULT_MAX_JSON_DEPTH,
    maximum_nodes: int = DEFAULT_MAX_JSON_NODES,
) -> dict[str, Any]:
    if not isinstance(values, Mapping):
        raise TypeError(f"{name} must be a mapping")
    normalized = normalize_json_value(
        values,
        name,
        maximum,
        maximum_depth=maximum_depth,
        maximum_nodes=maximum_nodes,
    )
    assert isinstance(normalized, dict)
    return normalized


def normalize_json_value(
    value: Any,
    name: str,
    maximum: int,
    *,
    maximum_depth: int = DEFAULT_MAX_JSON_DEPTH,
    maximum_nodes: int = DEFAULT_MAX_JSON_NODES,
) -> Any:
    return normalize_json_value_with_stats(
        value,
        name,
        maximum,
        maximum_depth=maximum_depth,
        maximum_nodes=maximum_nodes,
    ).value


def normalize_json_value_with_stats(
    value: Any,
    name: str,
    maximum: int,
    *,
    maximum_depth: int = DEFAULT_MAX_JSON_DEPTH,
    maximum_nodes: int = DEFAULT_MAX_JSON_NODES,
) -> NormalizedJson:
    budget = _JsonBudget(name, maximum, maximum_depth, maximum_nodes)
    normalized, size = _normalize_json_value(value, name, budget, depth=0)
    budget.validate_size(size)
    return NormalizedJson(value=normalized, size=size, nodes=budget.nodes)


def _normalize_json_value(
    value: Any,
    path: str,
    budget: _JsonBudget,
    *,
    depth: int,
) -> tuple[Any, int]:
    budget.visit(path, depth)
    if value is None or isinstance(value, (str, bool)):
        return value, _json_scalar_size(value, budget)
    if isinstance(value, int):
        if value < -MAX_SAFE_INTEGER or value > MAX_SAFE_INTEGER:
            raise ValueError(
                f"{path} integer is outside the JSON-safe range "
                f"-{MAX_SAFE_INTEGER} to {MAX_SAFE_INTEGER}"
            )
        return value, _json_scalar_size(value, budget)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError(f"{path} must be finite")
        return value, _json_scalar_size(value, budget)
    if isinstance(value, Mapping):
        normalized: dict[str, Any] = {}
        size = 2
        for key, nested in value.items():
            if not isinstance(key, str):
                raise TypeError(f"{path} keys must be strings, got {type(key).__name__}")
            if key in normalized:
                raise ValueError(f"{path} contains duplicate key {key!r}")
            key_size = _json_scalar_size(key, budget)
            size += (1 if normalized else 0) + key_size + 1
            budget.validate_size(size)
            normalized_value, nested_size = _normalize_json_value(
                nested,
                f"{path}.{key}",
                budget,
                depth=depth + 1,
            )
            size += nested_size
            budget.validate_size(size)
            normalized[key] = normalized_value
        return normalized, size
    if isinstance(value, (list, tuple)):
        normalized_list: list[Any] = []
        size = 2
        for index, item in enumerate(value):
            normalized_item, item_size = _normalize_json_value(
                item,
                f"{path}[{index}]",
                budget,
                depth=depth + 1,
            )
            size += (1 if normalized_list else 0) + item_size
            budget.validate_size(size)
            normalized_list.append(normalized_item)
        return normalized_list, size
    raise TypeError(f"{path} has unsupported JSON type {type(value).__name__}")


def _json_scalar_size(value: str | int | float | bool | None, budget: _JsonBudget) -> int:
    if isinstance(value, str) and len(value) > budget.maximum:
        budget.validate_size(len(value) + 2)
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError(f"{budget.name} contains text that is not valid UTF-8") from error
    size = len(encoded)
    budget.validate_size(size)
    return size
