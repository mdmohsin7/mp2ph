"""Mixpanel raw-export event → PostHog event transform.

Mirrors PostHog's open-source S3 batch-import worker:
https://github.com/PostHog/posthog/blob/master/rust/batch-import-worker/src/parse/content/mixpanel.rs
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Optional

# Same namespace bytes as PostHog's importer. Deterministic UUIDv5 from $insert_id
# means re-running this importer is idempotent — PostHog dedupes on event UUID.
MIXPANEL_INSERT_ID_NAMESPACE = uuid.UUID(bytes=b"posthog_mixpanel")

EVENT_NAME_MAP = {
    "$mp_web_page_view": "$pageview",
}

GEOIP_PROP_MAPPINGS = [
    ("$city", "$geoip_city_name"),
    ("$region", "$geoip_subdivision_1_name"),
    ("mp_country_code", "$geoip_country_code"),
]

MP_PROPS_TO_REMOVE = (
    "$mp_api_endpoint",
    "mp_processing_time_ms",
    "$insert_id",
    "$geo_source",
    "$mp_api_timestamp_ms",
)


def map_event_name(event: str) -> str:
    return EVENT_NAME_MAP.get(event, event)


def _looks_anonymous(distinct_id: str) -> bool:
    """Mixpanel anonymous-ID heuristic from the upstream Rust source."""
    if distinct_id.startswith("$device:"):
        return True
    return all(c.isascii() and (c.isupper() or c.isdigit() or c == "-") for c in distinct_id)


def get_distinct_id(properties: dict[str, Any]) -> Optional[str]:
    """Pick the distinct_id, preferring `$distinct_id_before_identity` when the
    primary distinct_id looks like an anonymous device ID."""
    distinct_id = properties.get("distinct_id")
    before_identity = properties.get("$distinct_id_before_identity")

    if not isinstance(before_identity, str):
        return distinct_id if isinstance(distinct_id, str) else None

    if not isinstance(distinct_id, str) or distinct_id == "":
        return before_identity

    if _looks_anonymous(distinct_id):
        return before_identity

    return distinct_id


def normalize_timestamp(time_value: int | float, offset_seconds: int = 0) -> str:
    """Mixpanel's `time` is seconds-since-epoch unless > 10^10, then it's ms."""
    seconds = int(time_value)
    if seconds > 10_000_000_000:
        seconds = seconds // 1000
    seconds += offset_seconds
    return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def event_uuid_from_insert_id(insert_id: Optional[str]) -> uuid.UUID:
    """Deterministic UUIDv5 from $insert_id, matching upstream namespace.
    Falls back to UUIDv4 if no insert_id (UUIDv7 in upstream — Python's uuid
    module doesn't ship v7 yet, and v4 is sufficient for non-deterministic IDs)."""
    if isinstance(insert_id, str) and insert_id:
        return uuid.uuid5(MIXPANEL_INSERT_ID_NAMESPACE, insert_id)
    return uuid.uuid4()


def map_geoip_props(properties: dict[str, Any]) -> dict[str, Any]:
    out = dict(properties)
    for src, dst in GEOIP_PROP_MAPPINGS:
        if src in out:
            out[dst] = out.pop(src)
    return out


def remove_mp_props(properties: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in properties.items() if k not in MP_PROPS_TO_REMOVE}


def add_source_data(properties: dict[str, Any], job_id: str) -> dict[str, Any]:
    out = dict(properties)
    out["historical_migration"] = True
    out["analytics_source"] = "mixpanel"
    out["$import_job_id"] = job_id
    return out


def transform_event(
    mx_event: dict[str, Any],
    *,
    job_id: str,
    skip_no_distinct_id: bool = False,
    timestamp_offset_seconds: int = 0,
) -> Optional[dict[str, Any]]:
    """Convert a single Mixpanel raw-export event dict to a PostHog `/batch/` event.

    Returns None if the event should be skipped (only when skip_no_distinct_id=True
    and no distinct_id can be derived).
    """
    event_name = mx_event.get("event")
    properties = mx_event.get("properties") or {}

    if not isinstance(event_name, str) or not isinstance(properties, dict):
        raise ValueError("Mixpanel event missing required `event` or `properties`")

    if "time" not in properties:
        raise ValueError("Mixpanel event missing `properties.time`")

    distinct_id = get_distinct_id(properties)
    if distinct_id is None:
        if skip_no_distinct_id:
            return None
        distinct_id = str(uuid.uuid4())

    insert_id = properties.get("$insert_id")
    event_uuid = event_uuid_from_insert_id(insert_id if isinstance(insert_id, str) else None)
    timestamp = normalize_timestamp(properties["time"], offset_seconds=timestamp_offset_seconds)

    other = {k: v for k, v in properties.items() if k not in ("distinct_id", "time")}
    other = map_geoip_props(other)
    other = remove_mp_props(other)
    other = add_source_data(other, job_id=job_id)
    other["distinct_id"] = distinct_id

    return {
        "event": map_event_name(event_name),
        "distinct_id": distinct_id,
        "uuid": str(event_uuid),
        "timestamp": timestamp,
        "properties": other,
    }
