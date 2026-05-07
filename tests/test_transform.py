"""Tests mirroring PostHog's mixpanel.rs transform spec."""

from __future__ import annotations

import json
import uuid

import pytest

from mp2ph.transform import (
    MIXPANEL_INSERT_ID_NAMESPACE,
    add_source_data,
    event_uuid_from_insert_id,
    get_distinct_id,
    map_event_name,
    map_geoip_props,
    normalize_timestamp,
    remove_mp_props,
    transform_event,
)


def test_pageview_remap():
    assert map_event_name("$mp_web_page_view") == "$pageview"
    assert map_event_name("custom_event") == "custom_event"


def test_distinct_id_prefers_real_over_anon_device_prefix():
    props = {
        "distinct_id": "$device:abc123",
        "$distinct_id_before_identity": "user-42",
    }
    assert get_distinct_id(props) == "user-42"


def test_distinct_id_uppercase_treated_as_anonymous():
    # All uppercase + digits + dashes → anonymous, fall back to before_identity
    props = {
        "distinct_id": "ABC-123-XYZ",
        "$distinct_id_before_identity": "user-42",
    }
    assert get_distinct_id(props) == "user-42"


def test_distinct_id_mixed_case_kept():
    props = {
        "distinct_id": "User42",
        "$distinct_id_before_identity": "user-before",
    }
    assert get_distinct_id(props) == "User42"


def test_distinct_id_empty_falls_back():
    props = {"distinct_id": "", "$distinct_id_before_identity": "user-before"}
    assert get_distinct_id(props) == "user-before"


def test_distinct_id_missing_uses_before_identity():
    assert get_distinct_id({"$distinct_id_before_identity": "user-before"}) == "user-before"


def test_distinct_id_no_before_identity_returns_distinct():
    assert get_distinct_id({"distinct_id": "user-42"}) == "user-42"


def test_distinct_id_returns_none_when_neither_present():
    assert get_distinct_id({}) is None


def test_timestamp_seconds():
    assert normalize_timestamp(1697379000) == "2023-10-15T14:10:00Z"


def test_timestamp_milliseconds_threshold():
    # > 10_000_000_000 → treated as milliseconds
    assert normalize_timestamp(1697379000000) == "2023-10-15T14:10:00Z"


def test_timestamp_offset_applied():
    assert normalize_timestamp(1697379000, offset_seconds=60) == "2023-10-15T14:11:00Z"


def test_geoip_renames():
    props = {"$city": "Bangalore", "$region": "KA", "mp_country_code": "IN"}
    out = map_geoip_props(props)
    assert out == {
        "$geoip_city_name": "Bangalore",
        "$geoip_subdivision_1_name": "KA",
        "$geoip_country_code": "IN",
    }


def test_remove_mp_props_strips_internal_fields():
    props = {
        "$mp_api_endpoint": "x",
        "mp_processing_time_ms": 1,
        "$insert_id": "abc",
        "$geo_source": "x",
        "$mp_api_timestamp_ms": 1,
        "kept": "yes",
    }
    assert remove_mp_props(props) == {"kept": "yes"}


def test_add_source_data_injects_markers():
    out = add_source_data({}, job_id="job-1")
    assert out == {
        "historical_migration": True,
        "analytics_source": "mixpanel",
        "$import_job_id": "job-1",
    }


def test_event_uuid_is_deterministic_from_insert_id():
    a = event_uuid_from_insert_id("evt-1")
    b = event_uuid_from_insert_id("evt-1")
    c = event_uuid_from_insert_id("evt-2")
    assert a == b
    assert a != c
    # Sanity-check namespace bytes match upstream
    assert MIXPANEL_INSERT_ID_NAMESPACE.bytes == b"posthog_mixpanel"


def test_event_uuid_random_when_no_insert_id():
    a = event_uuid_from_insert_id(None)
    b = event_uuid_from_insert_id(None)
    assert a != b


def test_transform_event_full_pipeline():
    raw = {
        "event": "Conversation Created",
        "properties": {
            "time": 1697379000,
            "distinct_id": "user-42",
            "$insert_id": "evt-deadbeef",
            "$city": "Bangalore",
            "mp_country_code": "IN",
            "$mp_api_endpoint": "drop-me",
            "custom_prop": "kept",
        },
    }
    out = transform_event(raw, job_id="job-1")
    assert out is not None
    assert out["event"] == "Conversation Created"
    assert out["distinct_id"] == "user-42"
    assert out["timestamp"] == "2023-10-15T14:10:00Z"

    expected_uuid = str(uuid.uuid5(MIXPANEL_INSERT_ID_NAMESPACE, "evt-deadbeef"))
    assert out["uuid"] == expected_uuid

    p = out["properties"]
    assert p["historical_migration"] is True
    assert p["analytics_source"] == "mixpanel"
    assert p["$import_job_id"] == "job-1"
    assert p["$geoip_city_name"] == "Bangalore"
    assert p["$geoip_country_code"] == "IN"
    assert p["custom_prop"] == "kept"
    assert "$insert_id" not in p
    assert "$mp_api_endpoint" not in p
    assert "time" not in p


def test_transform_event_skip_no_distinct_id():
    raw = {"event": "anon", "properties": {"time": 1697379000}}
    assert transform_event(raw, job_id="j", skip_no_distinct_id=True) is None


def test_transform_event_generates_uuid_distinct_id_when_not_skipping():
    raw = {"event": "anon", "properties": {"time": 1697379000}}
    out = transform_event(raw, job_id="j", skip_no_distinct_id=False)
    assert out is not None
    # Should be a valid UUID
    uuid.UUID(out["distinct_id"])


def test_transform_event_pageview_rename():
    raw = {
        "event": "$mp_web_page_view",
        "properties": {"time": 1697379000, "distinct_id": "u"},
    }
    out = transform_event(raw, job_id="j")
    assert out is not None
    assert out["event"] == "$pageview"


def test_transform_event_missing_time_raises():
    with pytest.raises(ValueError):
        transform_event({"event": "x", "properties": {}}, job_id="j")
