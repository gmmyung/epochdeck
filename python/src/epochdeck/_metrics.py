from __future__ import annotations

import math
import unicodedata
from collections.abc import Mapping
from typing import Any

MAX_METRICS_PER_POINT = 256
MAX_METRIC_KEY_BYTES = 256


def normalize_metrics(metrics: Mapping[str, Any]) -> dict[str, float]:
    if not metrics or len(metrics) > MAX_METRICS_PER_POINT:
        raise ValueError(f"a metric point must contain 1 to {MAX_METRICS_PER_POINT} metrics")
    normalized: dict[str, float] = {}
    for key, value in metrics.items():
        if not isinstance(key, str):
            raise TypeError("metric keys must be strings")
        encoded_key = key.encode("utf-8")
        if (
            not encoded_key
            or len(encoded_key) > MAX_METRIC_KEY_BYTES
            or any(unicodedata.category(character) == "Cc" for character in key)
        ):
            raise ValueError(
                f"metric keys must contain 1 to {MAX_METRIC_KEY_BYTES} non-control bytes"
            )
        if not isinstance(value, (bool, int, float)):
            raise TypeError(f"metric '{key}' must be numeric")
        try:
            number = float(value)
            finite = math.isfinite(number)
        except OverflowError:
            finite = False
        if not finite:
            raise ValueError(f"metric '{key}' must be finite")
        normalized[key] = number
    return normalized


def validate_metrics(metrics: Mapping[str, Any]) -> None:
    normalize_metrics(metrics)
