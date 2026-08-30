from __future__ import annotations

import json
from typing import Any

from runloom._limits import MAX_FILE_NAME_BYTES


class DeliveryError(RuntimeError):
    pass


class DeliveryProtocolError(DeliveryError):
    """A successful HTTP response did not acknowledge a durable request."""


def encode_json_request(value: Any) -> bytes:
    """Encode the canonical JSON body used for durable HTTP requests."""
    return json.dumps(
        value,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def validate_blob_file_name(value: Any) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError("blob file_name must be a string or None")
    encoded = value.encode("utf-8")
    if (
        not encoded
        or len(encoded) > MAX_FILE_NAME_BYTES
        or "/" in value
        or "\\" in value
        or any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value)
    ):
        raise ValueError(
            f"blob file_name must be a 1 to {MAX_FILE_NAME_BYTES} byte non-control basename"
        )
    return value


def validate_ingest_ack(
    response: dict[str, Any],
    *,
    run_id: str,
    batch: dict[str, Any],
) -> None:
    points = batch.get("points")
    batch_sequence = batch.get("batch_sequence")
    if not isinstance(points, list) or not points:
        raise DeliveryProtocolError("metric request has no point list")
    if isinstance(batch_sequence, bool) or not isinstance(batch_sequence, int):
        raise DeliveryProtocolError("metric request has no integer batch sequence")
    if require_text(response, "run_id") != run_id:
        raise DeliveryProtocolError("metric acknowledgement has the wrong run ID")
    if require_nonnegative_int(response, "batch_sequence") != batch_sequence:
        raise DeliveryProtocolError("metric acknowledgement has the wrong batch sequence")
    if require_nonnegative_int(response, "accepted_points") != len(points):
        raise DeliveryProtocolError("metric acknowledgement has the wrong accepted-point count")
    require_bool(response, "duplicate")
    require_nonnegative_int(response, "metric_revision")
    require_bool(response, "stop_requested")


def validate_record_ack(
    response: dict[str, Any],
    *,
    field: str,
    identity_field: str,
    expected_identity: str,
) -> None:
    record = require_object(response, field)
    if require_text(record, identity_field) != expected_identity:
        raise DeliveryProtocolError(f"{field} acknowledgement has the wrong identity")
    require_bool(response, "duplicate")


def validate_run_identity(response: dict[str, Any], expected_run_id: str) -> None:
    run = require_object(response, "run")
    if require_text(run, "id") != expected_run_id:
        raise DeliveryProtocolError("run response has the wrong run ID")


def required_request_identity(request: dict[str, Any], name: str) -> str:
    identity = request.get("id")
    if not isinstance(identity, str) or not identity:
        raise DeliveryProtocolError(f"{name} request has no durable identity")
    return identity


def require_object(value: dict[str, Any], field: str) -> dict[str, Any]:
    result = value.get(field)
    if not isinstance(result, dict):
        raise DeliveryProtocolError(f"successful response has no {field} object")
    return result


def require_text(value: dict[str, Any], field: str) -> str:
    result = value.get(field)
    if not isinstance(result, str) or not result:
        raise DeliveryProtocolError(f"successful response has no non-empty {field}")
    return result


def require_nonnegative_int(value: dict[str, Any], field: str) -> int:
    result = value.get(field)
    if isinstance(result, bool) or not isinstance(result, int) or result < 0:
        raise DeliveryProtocolError(f"successful response has no non-negative integer {field}")
    return result


def require_bool(value: dict[str, Any], field: str) -> bool:
    result = value.get(field)
    if not isinstance(result, bool):
        raise DeliveryProtocolError(f"successful response has no boolean {field}")
    return result
