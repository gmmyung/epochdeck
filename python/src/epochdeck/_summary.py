from __future__ import annotations

from collections.abc import Mapping
from typing import Any

MAX_DERIVED_SUMMARY_KEYS = 256
SYSTEM_METRIC_PREFIX = "system/"


def merge_metric_preview(
    current: Mapping[str, float],
    metrics: Mapping[str, Any],
    *,
    truncated: bool,
) -> tuple[dict[str, float], bool]:
    candidates = dict(current)
    for key, value in metrics.items():
        if not key.startswith(SYSTEM_METRIC_PREFIX):
            candidates[key] = float(value)
    retained_keys = sorted(candidates)[:MAX_DERIVED_SUMMARY_KEYS]
    return (
        {key: candidates[key] for key in retained_keys},
        truncated or len(candidates) > MAX_DERIVED_SUMMARY_KEYS,
    )
