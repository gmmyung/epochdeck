from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

CursorRelation = Literal["input", "output"]


def next_text_cursor(
    response: Mapping[str, Any],
    *,
    field: str,
    previous: str | None,
    context: str,
) -> str | None:
    cursor = response.get(field)
    if cursor is None:
        return None
    if not isinstance(cursor, str) or not cursor or cursor == previous:
        raise TypeError(f"{context} has an invalid or repeated cursor")
    return cursor


def next_paired_cursor(
    response: Mapping[str, Any],
    *,
    previous: tuple[str, CursorRelation] | None,
    context: str,
) -> tuple[str, CursorRelation] | None:
    cursor = response.get("next_before")
    relation = response.get("next_before_relation")
    if cursor is None and relation is None:
        return None
    if not isinstance(cursor, str) or not cursor or relation not in {"input", "output"}:
        raise TypeError(f"{context} has an invalid paired cursor")
    pair: tuple[str, CursorRelation] = (cursor, relation)
    if pair == previous:
        raise TypeError(f"{context} has a repeated paired cursor")
    return pair
