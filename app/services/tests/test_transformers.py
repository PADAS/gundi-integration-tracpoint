import pytest

from app.services.transformers import (
    _parse_timestamp,
    transform_to_events,
    transform_to_observations,
)


def _position(**overrides) -> dict:
    """Build a minimal Tracpoint Position dict, overrideable per-test."""
    base = {
        "inboundId": 1,
        "assetId": 42,
        "assetDisplayName": "Land Rover",
        "eventId": 0,
        "eventName": "",
        "timestamp": "2026-05-21 10:00:00",
        "latitude": -1.286389,
        "longitude": 36.817222,
        "speed": 45.0,
        "course": "180",
        "fix": 3,
        "lifetimeOdometer": 12345,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# _parse_timestamp
# ---------------------------------------------------------------------------

def test_parse_timestamp_normalises_naive_to_iso_with_utc():
    assert _parse_timestamp("2026-05-21 10:00:00") == "2026-05-21T10:00:00+00:00"


def test_parse_timestamp_returns_none_for_missing_or_empty():
    assert _parse_timestamp(None) is None
    assert _parse_timestamp("") is None
    assert _parse_timestamp("   ") is None


def test_parse_timestamp_returns_none_for_unparseable_format():
    # Not in Tracpoint's documented "YYYY-MM-DD HH:MM:SS" shape.
    assert _parse_timestamp("2026/05/21 10:00:00") is None
    assert _parse_timestamp("yesterday") is None
    assert _parse_timestamp("2026-13-99 99:99:99") is None  # nonsense components


# ---------------------------------------------------------------------------
# transform_to_observations
# ---------------------------------------------------------------------------

def test_transform_to_observations_minimal_record():
    obs = transform_to_observations([_position()])
    assert len(obs) == 1
    o = obs[0]
    assert o["source"] == "42"
    assert o["subject_type"] == "vehicle"  # default
    assert o["source_name"] == "Land Rover"
    assert o["recorded_at"] == "2026-05-21T10:00:00+00:00"
    assert o["location"] == {"lat": -1.286389, "lon": 36.817222}
    assert o["additional"]["inbound_id"] == 1
    assert o["additional"]["speed_kmph"] == 45.0
    assert o["additional"]["course"] == "180"
    assert o["additional"]["gps_fix"] == 3


def test_subject_type_override_propagates_into_observations():
    obs = transform_to_observations([_position()], subject_type="ranger")
    assert obs[0]["subject_type"] == "ranger"


def test_lifetime_odometer_zero_is_preserved():
    """Earlier truthy check silently dropped odometer readings of 0."""
    obs = transform_to_observations([_position(lifetimeOdometer=0)])
    assert obs[0]["additional"]["lifetime_odometer"] == 0


def test_lifetime_odometer_missing_is_omitted():
    obs = transform_to_observations([_position(lifetimeOdometer=None)])
    assert "lifetime_odometer" not in obs[0]["additional"]


def test_records_missing_required_fields_are_skipped():
    raw = [
        _position(assetId=None),                # no assetId
        _position(timestamp=None),              # no timestamp
        _position(timestamp="garbage"),         # unparseable timestamp
        _position(latitude=None),               # no lat
        _position(longitude=None),              # no lon
        _position(),                            # valid — should be the only survivor
    ]
    obs = transform_to_observations(raw)
    assert len(obs) == 1


def test_event_tagged_position_carries_event_metadata_on_observation():
    obs = transform_to_observations([_position(eventId=47, eventName="Speeding")])
    additional = obs[0]["additional"]
    assert additional["tracpoint_event_id"] == 47
    assert additional["tracpoint_event_name"] == "Speeding"


# ---------------------------------------------------------------------------
# transform_to_events
# ---------------------------------------------------------------------------

def test_events_emit_only_tagged_positions():
    raw = [
        _position(eventId=0),  # plain tracking — no event
        _position(eventId=47, eventName="Speeding"),
        _position(eventId=99, eventName="Geofence Entry"),
    ]
    events = transform_to_events(raw)
    assert len(events) == 2
    assert {e["event_type"] for e in events} == {"tracpoint_speeding", "tracpoint_geofence_entry"}


def test_event_title_uses_display_name_when_present():
    events = transform_to_events([_position(eventId=47, eventName="Speeding")])
    assert events[0]["title"] == "Land Rover: Speeding"


def test_event_title_falls_back_to_asset_id_when_display_name_is_none():
    """The default get(..., fallback) only catches a missing key — None must
    also fall through. Previously this produced 'None: Speeding'."""
    events = transform_to_events([
        _position(eventId=47, eventName="Speeding", assetDisplayName=None)
    ])
    assert events[0]["title"] == "42: Speeding"


def test_event_title_falls_back_to_unknown_when_both_are_missing():
    events = transform_to_events([
        _position(eventId=47, eventName="Speeding", assetDisplayName=None, assetId=None)
    ])
    assert events[0]["title"] == "unknown: Speeding"


def test_event_type_snake_cases_spaces_and_hyphens():
    events = transform_to_events([_position(eventId=1, eventName="Driver Door-Open Alert")])
    assert events[0]["event_type"] == "tracpoint_driver_door_open_alert"


def test_event_type_falls_back_when_event_name_missing():
    events = transform_to_events([_position(eventId=12, eventName="")])
    assert events[0]["event_type"] == "tracpoint_event_12"


def test_event_with_missing_coordinates_still_emits_but_with_no_location():
    events = transform_to_events([
        _position(eventId=1, eventName="Speeding", latitude=None, longitude=None),
    ])
    assert len(events) == 1
    assert events[0]["location"] is None


def test_event_with_unparseable_timestamp_is_skipped():
    events = transform_to_events([_position(eventId=1, eventName="Speeding", timestamp="not a ts")])
    assert events == []
