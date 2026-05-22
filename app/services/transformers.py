import logging
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# Tracpoint emits position timestamps in this format (naive, UTC assumed).
_TRACPOINT_TS_FORMAT = "%Y-%m-%d %H:%M:%S"

# Tracpoint Position schema (from WSDL):
#   inboundId, assetId, assetDisplayName,
#   eventId, eventName,
#   waypointId, waypointName,
#   timestamp (xsd:string),
#   latitude (xsd:double), longitude (xsd:double),
#   speed (xsd:float), course (xsd:string), fix (xsd:integer),
#   road, area, town, townDistance, townDirection, county, state, country, postcode,
#   lifetimeOdometer
#
# Note: "events" in Tracpoint are tags on positions (e.g. "Speeding", "Geofence Entry"),
# not a separate data type. Positions where eventId != 0 become both a Gundi observation
# AND a Gundi event. Positions where eventId == 0 are plain tracking positions.
#
# The timestamp string format from Tracpoint is typically "YYYY-MM-DD HH:MM:SS".
# Adjust _parse_timestamp() below if the server returns a different format.


def _parse_timestamp(raw_ts: Any) -> str | None:
    """
    Normalise a Tracpoint timestamp string to ISO 8601 with UTC offset.
    Tracpoint returns naive datetime strings like "2024-03-01 14:30:00" (UTC assumed).
    Returns None if the value is missing, empty, or does not parse as the
    expected `YYYY-MM-DD HH:MM:SS` format.
    """
    if not raw_ts:
        return None
    ts = str(raw_ts).strip()
    if not ts:
        return None
    try:
        datetime.strptime(ts, _TRACPOINT_TS_FORMAT)
    except (ValueError, TypeError):
        return None
    return ts.replace(" ", "T") + "+00:00"


# ---------------------------------------------------------------------------
# Observations — all positions (regular tracks + event-tagged positions)
# ---------------------------------------------------------------------------

def transform_to_observations(
    raw_records: list[dict[str, Any]],
    subject_type: str = "vehicle",
) -> list[dict[str, Any]]:
    """
    Convert raw Tracpoint Position dicts into Gundi observation dicts.
    Every position record — whether it carries an event tag or not — becomes
    an observation so that continuous tracks are preserved in EarthRanger.
    """
    observations = []
    for record in raw_records:
        try:
            obs = _transform_observation(record, subject_type=subject_type)
            if obs:
                observations.append(obs)
        except Exception as e:
            logger.warning(
                "Skipping Tracpoint position due to transformation error",
                extra={"error": str(e), "inboundId": record.get("inboundId")},
            )
    return observations


def _transform_observation(record: dict[str, Any], subject_type: str = "vehicle") -> dict[str, Any] | None:
    source = record.get("assetId")
    if source is None:
        logger.warning("Position record missing assetId, skipping")
        return None

    recorded_at = _parse_timestamp(record.get("timestamp"))
    if not recorded_at:
        logger.warning("Position record missing timestamp, skipping", extra={"assetId": source})
        return None

    lat = record.get("latitude")
    lon = record.get("longitude")
    if lat is None or lon is None:
        logger.warning("Position record missing coordinates, skipping", extra={"assetId": source})
        return None

    additional: dict[str, Any] = {
        "inbound_id": record.get("inboundId"),
    }
    if record.get("speed") is not None:
        additional["speed_kmph"] = record["speed"]
    if record.get("course"):
        additional["course"] = record["course"]
    if record.get("fix") is not None:
        additional["gps_fix"] = record["fix"]
    if record.get("lifetimeOdometer") is not None:
        additional["lifetime_odometer"] = record["lifetimeOdometer"]
    # Location context fields
    for field in ("road", "area", "town", "county", "state", "country", "postcode"):
        if record.get(field):
            additional[field] = record[field]
    # Event tag, if present
    if record.get("eventId"):
        additional["tracpoint_event_id"] = record["eventId"]
        additional["tracpoint_event_name"] = record.get("eventName")

    return {
        "source": str(source),
        "subject_type": subject_type,
        "source_name": record.get("assetDisplayName") or str(source),
        "recorded_at": recorded_at,
        "location": {"lat": float(lat), "lon": float(lon)},
        "additional": additional,
    }


# ---------------------------------------------------------------------------
# Events — only positions tagged with a Tracpoint event (eventId != 0)
# ---------------------------------------------------------------------------

def transform_to_events(raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Convert Tracpoint event-tagged positions into Gundi event dicts.
    Only records where eventId is non-zero are included; plain tracking
    positions are silently skipped.
    """
    events = []
    for record in raw_records:
        if not record.get("eventId"):
            continue  # plain position, not an alert
        try:
            event = _transform_event(record)
            if event:
                events.append(event)
        except Exception as e:
            logger.warning(
                "Skipping Tracpoint event record due to transformation error",
                extra={"error": str(e), "inboundId": record.get("inboundId")},
            )
    return events


def _transform_event(record: dict[str, Any]) -> dict[str, Any] | None:
    recorded_at = _parse_timestamp(record.get("timestamp"))
    if not recorded_at:
        return None

    event_name = record.get("eventName") or f"event_{record.get('eventId')}"
    # Gundi event_type: lowercase, underscored identifier
    event_type = f"tracpoint_{event_name.lower().replace(' ', '_').replace('-', '_')}"

    lat = record.get("latitude")
    lon = record.get("longitude")
    location = {"lat": float(lat), "lon": float(lon)} if lat is not None and lon is not None else None

    asset_label = record.get("assetDisplayName") or record.get("assetId") or "unknown"
    return {
        "title": f"{asset_label}: {event_name}",
        "event_type": event_type,
        "recorded_at": recorded_at,
        "location": location,
        "event_details": {
            "asset_id": record.get("assetId"),
            "asset_display_name": record.get("assetDisplayName"),
            "tracpoint_event_id": record.get("eventId"),
            "tracpoint_event_name": event_name,
            "inbound_id": record.get("inboundId"),
            "speed_kmph": record.get("speed"),
            "course": record.get("course"),
        },
    }
