import logging
from datetime import datetime, timezone, timedelta

from app.services.activity_logger import activity_logger, log_action_activity
from app.services.action_scheduler import crontab_schedule
from app.services.gundi import send_observations_to_gundi, send_events_to_gundi
from app.services.state import IntegrationStateManager
from app.services.client import TracpointClient
from app.services.tracpoint_cache import fetch_assets_cached
from app.services.transformers import transform_to_observations, transform_to_events
from gundi_core.events import LogLevel

from .configurations import AuthenticateConfig, PullObservationsConfig, PullEventsConfig

logger = logging.getLogger(__name__)

state_manager = IntegrationStateManager()

# Tracpoint timestamp format for getSinglePositions parameters: "YYYY-MM-DD HH:MM:SS"
_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _to_tracpoint_ts(iso_string: str) -> str:
    """Convert an ISO 8601 string to Tracpoint's expected timestamp format."""
    # Handle both "2024-01-01T00:00:00+00:00" and "2024-01-01T00:00:00Z"
    iso_string = iso_string.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso_string).astimezone(timezone.utc)
    return dt.strftime(_TS_FORMAT)


def _now_tracpoint_ts() -> str:
    return datetime.now(timezone.utc).strftime(_TS_FORMAT)


def _get_client(auth_data: dict) -> TracpointClient:
    return TracpointClient(
        wsdl_url=auth_data.get("wsdl_url", "http://www.terramarnetworks.net/v7/index.php?wsdl"),
        company=auth_data.get("company", ""),
        username=auth_data.get("username", ""),
        password=auth_data.get("password", ""),
    )


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


@crontab_schedule("*/15 * * * *")
@activity_logger()
async def action_pull_observations(integration, action_config: PullObservationsConfig):
    """
    Fetch position records from Tracpoint for all assets and forward them to Gundi.

    Strategy:
      - First run (no state): fetch the most recent position snapshot for all assets
        via getAllPositions, then switch to incremental on the next run.
      - Subsequent runs: for each asset, call getSinglePositions with
        [last_cursor, now] to retrieve only new positions.
    """
    integration_id = str(integration.id)

    # 1. Get auth config
    auth_config = integration.get_action_config("auth")
    client = _get_client(auth_config.data)

    # 2. Determine time range using persisted state
    state = await state_manager.get_state(
        integration_id=integration_id,
        action_id="pull_observations",
    )
    since = state.get("last_cursor") if state else None

    # 3. Fetch from Tracpoint
    all_raw: list[dict] = []
    try:
        if not since:
            # First run — snapshot of current positions
            all_raw = await client.fetch_all_positions()
        else:
            # Incremental — per-asset time-range query
            end_ts = _now_tracpoint_ts()
            start_ts = _to_tracpoint_ts(since)
            assets = await fetch_assets_cached(client, integration_id)
            for asset in assets:
                asset_id = asset.get("assetId")
                if asset_id is None:
                    continue
                positions = await client.fetch_positions_for_asset(
                    asset_id=int(asset_id),
                    start_timestamp=start_ts,
                    end_timestamp=end_ts,
                )
                all_raw.extend(positions)
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

    # 4. Transform to Gundi format
    observations = transform_to_observations(all_raw, subject_type=action_config.subject_type)

    # 5. Send to Gundi
    if observations:
        await send_observations_to_gundi(
            observations=observations,
            integration_id=integration_id,
        )

    # 6. Update state — use current time as the next high-water mark
    #    (Tracpoint timestamp field is a string; we store ISO format for portability)
    if all_raw:
        new_cursor = datetime.now(timezone.utc).isoformat()
        await state_manager.set_state(
            integration_id=integration_id,
            action_id="pull_observations",
            state={"last_cursor": new_cursor},
        )

    # 7. Log summary
    await log_action_activity(
        integration_id=integration_id,
        action_id="pull_observations",
        level=LogLevel.INFO,
        title=f"Processed {len(observations)} observations from Tracpoint",
        data={"observations_processed": len(observations), "raw_positions_fetched": len(all_raw)},
        config_data=action_config.dict(),
    )

    return {"observations_processed": len(observations)}


@crontab_schedule("*/15 * * * *")
@activity_logger()
async def action_pull_events(integration, action_config: PullEventsConfig):
    """
    Fetch event-tagged positions from Tracpoint and forward them to Gundi as events.

    Tracpoint "events" are labels applied to position records (e.g. "Speeding",
    "Geofence Entry"). This handler fetches positions and filters to those where
    eventId != 0, forwarding each as a discrete Gundi event.
    """
    integration_id = str(integration.id)

    # 1. Get auth config
    auth_config = integration.get_action_config("auth")
    client = _get_client(auth_config.data)

    # 2. Determine time range
    state = await state_manager.get_state(
        integration_id=integration_id,
        action_id="pull_events",
    )
    since = state.get("last_cursor") if state else None

    # 3. Fetch from Tracpoint
    all_raw: list[dict] = []
    try:
        if not since:
            all_raw = await client.fetch_all_positions()
        else:
            end_ts = _now_tracpoint_ts()
            start_ts = _to_tracpoint_ts(since)
            assets = await fetch_assets_cached(client, integration_id)
            for asset in assets:
                asset_id = asset.get("assetId")
                if asset_id is None:
                    continue
                positions = await client.fetch_positions_for_asset(
                    asset_id=int(asset_id),
                    start_timestamp=start_ts,
                    end_timestamp=end_ts,
                )
                all_raw.extend(positions)
    except Exception as e:
        await log_action_activity(
            integration_id=integration_id,
            action_id="pull_events",
            level=LogLevel.ERROR,
            title="Failed to fetch event positions from Tracpoint",
            data={"error": str(e)},
            config_data=action_config.dict(),
        )
        raise

    # 4. Transform — only positions tagged with an event
    events = transform_to_events(all_raw)

    # 5. Send to Gundi
    if events:
        await send_events_to_gundi(
            events=events,
            integration_id=integration_id,
        )

    # 6. Update state
    if all_raw:
        new_cursor = datetime.now(timezone.utc).isoformat()
        await state_manager.set_state(
            integration_id=integration_id,
            action_id="pull_events",
            state={"last_cursor": new_cursor},
        )

    # 7. Log summary
    await log_action_activity(
        integration_id=integration_id,
        action_id="pull_events",
        level=LogLevel.INFO,
        title=f"Processed {len(events)} events from Tracpoint",
        data={
            "events_processed": len(events),
            "raw_positions_fetched": len(all_raw),
            "event_tagged_positions": len([r for r in all_raw if r.get("eventId")]),
        },
        config_data=action_config.dict(),
    )

    return {"events_processed": len(events)}
