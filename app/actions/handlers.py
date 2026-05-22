import logging
from datetime import datetime, timezone
from typing import Any, Optional, Tuple, Union

from app.services.activity_logger import activity_logger, log_action_activity
from app.services.action_scheduler import crontab_schedule
from app.services.gundi import send_observations_to_gundi, send_events_to_gundi
from app.services.state import IntegrationStateManager
from app.services.client import TracpointClient
from app.services.transformers import transform_to_observations, transform_to_events
from gundi_core.events import LogLevel

from .configurations import AuthenticateConfig, PullObservationsConfig

logger = logging.getLogger(__name__)

state_manager = IntegrationStateManager()

# Tracpoint position timestamp format: "YYYY-MM-DD HH:MM:SS" (naive, UTC).
_POSITION_TS_FORMAT = "%Y-%m-%d %H:%M:%S"

# Composite cursor = (position timestamp, inboundId tie-breaker).
# The inboundId slot may be float("+inf") for legacy state that predates the
# tie-breaker — see `_load_cursor_from_state` — which preserves the old
# drop-on-equality dedup behavior until the cursor advances.
Cursor = Tuple[datetime, Union[int, float]]


def _get_client(auth_data: dict) -> TracpointClient:
    return TracpointClient(
        wsdl_url=auth_data.get("wsdl_url", "https://www.terramarnetworks.net/v10/index.php?wsdl"),
        company=auth_data.get("company", ""),
        username=auth_data.get("username", ""),
        password=auth_data.get("password", ""),
    )


def _parse_cursor(value: str | None) -> datetime | None:
    """Parse an ISO 8601 high-water-mark string into a tz-aware datetime."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def _parse_position_ts(raw: Any) -> datetime | None:
    """Parse a Tracpoint position timestamp string into a tz-aware UTC datetime."""
    if not raw:
        return None
    try:
        return datetime.strptime(str(raw).strip(), _POSITION_TS_FORMAT).replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _load_cursor_from_state(state: dict | None) -> Optional[Cursor]:
    """Read the persisted high-water mark, handling pre-tie-breaker state.

    Modern state has both `last_cursor` (ISO timestamp) and `last_cursor_inbound_id`.
    Legacy state from before the tie-breaker has only `last_cursor`; in that
    case we use +inf for the inboundId slot so the legacy drop-on-equality
    semantics are preserved until the cursor advances to a newer timestamp.
    """
    if not state:
        return None
    ts = _parse_cursor(state.get("last_cursor"))
    if ts is None:
        return None
    inbound = state.get("last_cursor_inbound_id")
    if isinstance(inbound, int):
        return (ts, inbound)
    return (ts, float("inf"))


def _cursor_for_position(pos: dict[str, Any]) -> Optional[Cursor]:
    """Build the cursor tuple for a single position record. None if unusable."""
    pos_dt = _parse_position_ts(pos.get("timestamp"))
    if pos_dt is None:
        return None
    inbound = pos.get("inboundId")
    # Tracpoint documents inboundId as a globally unique identifier; treat a
    # missing value as -1 so positions without one still sort below those with.
    inbound_val: int = inbound if isinstance(inbound, int) else -1
    return (pos_dt, inbound_val)


def filter_new_positions(
    raw: list[dict[str, Any]],
    cursor: Optional[Cursor],
) -> tuple[list[dict[str, Any]], Optional[Cursor]]:
    """Return positions strictly newer than `cursor` plus the new high-water mark.

    `getAllPositions` returns the latest known position for every asset on
    every call, including assets that haven't reported since the last cycle.
    The cursor is a `(timestamp, inboundId)` tuple — using the inboundId as a
    tie-breaker means two assets reporting at the exact same second don't
    cause us to drop one of them. Tuples compare lexicographically in Python.
    """
    new_raw: list[dict[str, Any]] = []
    max_cursor = cursor
    for pos in raw:
        pos_cursor = _cursor_for_position(pos)
        if pos_cursor is None:
            continue
        if cursor is not None and pos_cursor <= cursor:
            continue
        new_raw.append(pos)
        if max_cursor is None or pos_cursor > max_cursor:
            max_cursor = pos_cursor
    return new_raw, max_cursor


@activity_logger()
async def action_auth(integration, action_config: AuthenticateConfig):
    """Validate credentials against the Tracpoint SOAP service."""
    client = TracpointClient(
        wsdl_url=action_config.wsdl_url,
        company=action_config.company,
        username=action_config.username,
        password=action_config.password.get_secret_value(),
    )
    try:
        assets = await client.test_connection()
    except Exception as e:
        await log_action_activity(
            integration_id=str(integration.id),
            action_id="auth",
            level=LogLevel.ERROR,
            title="Tracpoint authentication failed",
            data={"error": str(e)},
            config_data=action_config.dict(),
        )
        raise

    return {"valid_credentials": True, "assets_visible": len(assets)}


@crontab_schedule("*/2 * * * *")
@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    """
    Fetch the latest position per asset from Tracpoint and forward new ones to Gundi.

    Every cycle calls `getAllPositions` exactly once — a single SOAP call
    regardless of fleet size. Tracpoint returns the latest known position for
    every asset; we dedup client-side against the previous cycle's high-water
    mark stored in Redis, so an asset that has not reported new data is
    silently filtered out.

    Each forwarded position becomes a Gundi observation. Positions tagged with
    a Tracpoint event (eventId != 0 — speeding, geofence breach, panic alert,
    etc.) additionally become Gundi events when `action_config.emit_events`
    is True. The default is False until Gundi's dispatcher-side reference-data
    provisioning is in place — without that, EarthRanger rejects unknown event
    types on POST.

    Trade-off vs. per-asset `getSinglePositions`: if a tracker reports multiple
    positions inside one polling window we see only the most recent. At the
    configured 2-minute cadence this is rarely a concern in practice — most
    trackers transmit at intervals close to or longer than 2 min, so we
    capture essentially every fix.
    """
    integration_id = str(integration.id)

    # 1. Auth config
    auth_config = integration.get_action_config("auth")
    client = _get_client(auth_config.data)

    # 2. Persisted high-water mark
    state = await state_manager.get_state(
        integration_id=integration_id,
        action_id="pull_observations",
    )
    since = _load_cursor_from_state(state)

    # 3. Single fetch
    try:
        raw = await client.fetch_all_positions()
    except Exception as e:
        await log_action_activity(
            integration_id=integration_id,
            action_id="pull_observations",
            level=LogLevel.ERROR,
            title="Failed to fetch positions from Tracpoint",
            data={"error": str(e)},
            config_data=action_config.dict(),
        )
        raise

    # 4. Filter to positions newer than the cursor; track the new high-water mark
    new_raw, new_cursor = filter_new_positions(raw, since)

    # 5. Transform — every fresh position is an observation; tagged positions are also events
    observations = transform_to_observations(new_raw, subject_type=action_config.subject_type)
    events = transform_to_events(new_raw) if action_config.emit_events else []

    # 6. Send to Gundi
    if observations:
        await send_observations_to_gundi(
            observations=observations,
            integration_id=integration_id,
        )
    if events:
        await send_events_to_gundi(
            events=events,
            integration_id=integration_id,
        )

    # 7. Advance cursor only when we actually forwarded something. new_cursor
    #    will always have a concrete int inbound_id at this point (any legacy
    #    +inf sentinel sits in `since`, never in the max derived from new_raw).
    if new_raw and new_cursor is not None:
        new_ts, new_inbound = new_cursor
        await state_manager.set_state(
            integration_id=integration_id,
            action_id="pull_observations",
            state={
                "last_cursor": new_ts.isoformat(),
                "last_cursor_inbound_id": new_inbound,
            },
        )

    # 8. Log summary
    await log_action_activity(
        integration_id=integration_id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title=f"Processed {len(observations)} observations, {len(events)} events from Tracpoint",
        data={
            "observations_processed": len(observations),
            "events_processed": len(events),
            "raw_positions_fetched": len(raw),
            "new_positions_after_dedup": len(new_raw),
        },
        config_data=action_config.dict(),
    )

    return {
        "observations_processed": len(observations),
        "events_processed": len(events),
    }
