import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Tuple, Union

from app.services.activity_logger import activity_logger, log_action_activity
from app.services.action_scheduler import crontab_schedule
from app.services.gundi import send_observations_to_gundi, send_events_to_gundi
from app.services.state import IntegrationStateManager
from app.services.client import TracpointClient
from app.services.transformers import transform_to_observations, transform_to_events
from gundi_core.events import LogLevel

from .configurations import AuthenticateConfig, PullObservationsConfig, PullTrackHistoryConfig
from app.services.tracpoint_cache import fetch_assets_cached

logger = logging.getLogger(__name__)

state_manager = IntegrationStateManager()

# Tracpoint position timestamp format: "YYYY-MM-DD HH:MM:SS" (naive, UTC).
_POSITION_TS_FORMAT = "%Y-%m-%d %H:%M:%S"

# Max dropped-position tuples inlined in the diagnostic log line emitted by
# filter_new_positions. Comfortably above our current fleet sizes (~35), but
# bounds the entry for large fleets where an unbounded list could exceed
# Cloud Logging's per-entry size limit and get truncated.
_DROPPED_LOG_SAMPLE_SIZE = 50

# Composite cursor = (position timestamp, inboundId tie-breaker).
# Dedup is PER ASSET (GUNDI-5543): each vehicle is compared only against its
# own last-forwarded tuple. The previous fleet-wide high-water mark silently
# discarded fixes that surfaced in getAllPositions late (Terramar ingests
# fixes 10-56 min after their GPS timestamps) whenever any other vehicle had
# already pushed the fleet max past them — freezing those vehicles in
# EarthRanger until the 2-hour backfill.
Cursor = Tuple[datetime, Union[int, float]]


def _get_client(auth_data: dict) -> TracpointClient:
    return TracpointClient(
        wsdl_url=auth_data.get("wsdl_url", "http://www.terramarnetworks.net/v7/index.php?wsdl"),
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


def _load_asset_cursors_from_state(state: dict | None) -> dict[int, Cursor]:
    """Read the per-asset high-water marks out of integration state.

    State written by fleet-wide-cursor deployments (only `last_cursor` /
    `last_cursor_inbound_id`) is deliberately ignored: the first post-deploy
    cycle re-forwards each asset's latest position once, and Gundi/ER dedupe
    server-side. Seeding every asset from the fleet max instead would re-drop
    any fix that was late-surfacing at the moment of migration — the very
    bug the per-asset cursor exists to fix. Malformed entries are skipped.
    """
    if not state:
        return {}
    raw_cursors = state.get("asset_cursors")
    if not isinstance(raw_cursors, dict):
        return {}
    cursors: dict[int, Cursor] = {}
    for key, value in raw_cursors.items():
        if not (isinstance(key, str) and key.lstrip("-").isdigit()):
            continue
        if not (isinstance(value, (list, tuple)) and len(value) == 2):
            continue
        ts = _parse_cursor(value[0])
        if ts is None or not isinstance(value[1], int):
            continue
        cursors[int(key)] = (ts, value[1])
    return cursors


def _serialize_asset_cursors(cursors: dict[int, Cursor]) -> dict[str, list]:
    """JSON-safe form of the per-asset cursors: {"<assetId>": [iso_ts, inboundId]}."""
    return {str(asset_id): [ts.isoformat(), inbound] for asset_id, (ts, inbound) in cursors.items()}


def _fleet_max_cursor(cursors: dict[int, Cursor]) -> Optional[Cursor]:
    """Max tuple across all assets — written alongside the per-asset map so a
    rollback to a fleet-wide-cursor build resumes from sane state."""
    return max(cursors.values()) if cursors else None


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


def _compute_track_history_window(
    cursor: datetime | None,
    now: datetime,
    max_lookback_hours: int,
    stale_cursor_days: int,
) -> tuple[str, str]:
    """Pick the (start, end) range to ask `getSinglePositions` for.

    The end is always `now`. The start is the saved cursor unless that
    cursor is missing, older than `stale_cursor_days`, or in the future,
    in which case we fall back to `now - max_lookback_hours`. The future
    case clamps to `now` so we never send Tracpoint a backwards range.

    Returns the pair as Tracpoint's wire format strings.
    """
    lookback_start = now - timedelta(hours=max_lookback_hours)
    stale_before = now - timedelta(days=stale_cursor_days)

    if cursor is None or cursor < stale_before:
        start = lookback_start
    elif cursor > now:
        start = now
    else:
        start = cursor

    return (
        start.strftime(_POSITION_TS_FORMAT),
        now.strftime(_POSITION_TS_FORMAT),
    )


# Used as the action_id slot in IntegrationStateManager keys. Centralized so
# the load/save helpers and the action handler agree on the spelling and the
# scheduler decorator below has a single source of truth.
_TRACK_HISTORY_ACTION_ID = "pull_track_history"

# Cap on observations per POST to Gundi's sensors API. A single track-history
# cycle can accumulate thousands of fixes (many assets × dense history), and the
# sensors client posts whatever it's given as one HTTP request, so we chunk here
# to keep each request inside the API's payload/timeout limits.
_GUNDI_OBSERVATION_BATCH_SIZE = 200


def _batched(items: list, size: int):
    """Yield successive `size`-length slices of `items` (size must be >= 1)."""
    for start in range(0, len(items), size):
        yield items[start:start + size]


def _load_track_history_cursor(state: dict | None) -> datetime | None:
    """Read the per-asset 'last_fetched_to' timestamp out of integration state."""
    if not state:
        return None
    return _parse_cursor(state.get("last_fetched_to"))


def _resolve_subject_type(integration) -> str:
    """Read `subject_type` from the integration's PullObservationsConfig.

    The track-history action does not own its own `subject_type` setting —
    it borrows the one from the hot-loop action so both produce observations
    that land under the same EarthRanger subject type. If the integration
    has no `pull_observations` action configured (unusual: the two actions
    are designed to coexist), fall back to the same Pydantic default that
    PullObservationsConfig declares so a misconfigured integration still
    produces sensible observations rather than crashing.
    """
    pull_obs_default = PullObservationsConfig.__fields__["subject_type"].default
    pull_obs_config = integration.get_action_config("pull_observations")
    data = getattr(pull_obs_config, "data", None) or {}
    return data.get("subject_type") or pull_obs_default


async def _save_track_history_cursor(
    state_manager,
    integration_id: str,
    asset_id: int,
    when: datetime,
) -> None:
    """Persist `when` as the new per-asset cursor for `asset_id`."""
    await state_manager.set_state(
        integration_id=integration_id,
        action_id=_TRACK_HISTORY_ACTION_ID,
        source_id=str(asset_id),
        state={"last_fetched_to": when.isoformat()},
    )


def filter_new_positions(
    raw: list[dict[str, Any]],
    asset_cursors: dict[int, Cursor],
) -> tuple[list[dict[str, Any]], dict[int, Cursor]]:
    """Return positions newer than their asset's own cursor, plus updated cursors.

    `getAllPositions` returns the latest known position for every asset on
    every call, including assets that haven't reported since the last cycle.
    Each position is compared against its OWN asset's `(timestamp, inboundId)`
    tuple only (tuples compare lexicographically; the inboundId breaks
    same-second ties). A fix that surfaces in the API late is therefore still
    forwarded — under the old fleet-wide cursor it was silently dropped
    whenever any other vehicle had already advanced the fleet max past it
    (GUNDI-5543). Cursors for assets absent from `raw` are retained.
    """
    new_raw: list[dict[str, Any]] = []
    dropped: list[dict[str, Any]] = []
    updated = dict(asset_cursors)
    for pos in raw:
        pos_cursor = _cursor_for_position(pos)
        asset_id = pos.get("assetId")
        if pos_cursor is None or asset_id is None:
            continue
        own_cursor = updated.get(asset_id)
        if own_cursor is not None and pos_cursor <= own_cursor:
            dropped.append({
                "assetId": asset_id,
                "timestamp": str(pos.get("timestamp")).strip(),
                "inboundId": pos.get("inboundId"),
                "cursor_ts": own_cursor[0].isoformat(),
            })
            continue
        new_raw.append(pos)
        updated[asset_id] = pos_cursor
    if dropped:
        # With per-asset cursors a drop only means "this asset re-reported an
        # already-forwarded fix (or went backwards)" — normal, but kept in the
        # logs for observability. Only the first _DROPPED_LOG_SAMPLE_SIZE
        # tuples are inlined so a large fleet can't bloat the entry past
        # Cloud Logging's per-entry size limit.
        omitted = len(dropped) - _DROPPED_LOG_SAMPLE_SIZE
        suffix = f" ... and {omitted} more omitted" if omitted > 0 else ""
        logger.info(
            "Dropped %d position(s) at/behind their asset's own cursor: %s%s",
            len(dropped), dropped[:_DROPPED_LOG_SAMPLE_SIZE], suffix,
        )
    return new_raw, updated


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
    every asset; we dedup client-side against PER-ASSET high-water marks
    stored in Redis, so an asset that has not reported new data is silently
    filtered out while a fix that surfaces in the API late (Terramar ingest
    lag, GUNDI-5543) is still forwarded promptly.

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

    # 2. Persisted per-asset high-water marks
    state = await state_manager.get_state(
        integration_id=integration_id,
        action_id="pull_observations",
    )
    asset_cursors = _load_asset_cursors_from_state(state)

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

    # 4. Filter to positions newer than each asset's own cursor
    new_raw, updated_cursors = filter_new_positions(raw, asset_cursors)

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

    # 7. Persist cursors only when we actually forwarded something (the map
    #    can only have changed in that case). The legacy fleet-max fields are
    #    written alongside the per-asset map so a rollback to a fleet-wide-
    #    cursor build resumes from sane state instead of a frozen cursor.
    if new_raw:
        fleet_max = _fleet_max_cursor(updated_cursors)
        max_ts, max_inbound = fleet_max
        await state_manager.set_state(
            integration_id=integration_id,
            action_id="pull_observations",
            state={
                "asset_cursors": _serialize_asset_cursors(updated_cursors),
                "last_cursor": max_ts.isoformat(),
                "last_cursor_inbound_id": max_inbound,
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


@crontab_schedule("0 */2 * * *")
@activity_logger()
async def action_pull_track_history(integration, action_config: PullTrackHistoryConfig):
    """
    Backfill full-resolution position history per asset.

    Counterpart to `action_pull_observations`. Where the hot loop calls
    `getAllPositions` once per cycle and captures only the latest fix per
    asset, this action runs every 2 hours and calls `getSinglePositions`
    per asset for a bounded time window, so intermediate fixes that the
    hot loop missed are recovered for EarthRanger's track-history view.

    Gundi and EarthRanger dedupe observations server-side, so overlap
    with the hot loop is harmless and we deliberately do not filter the
    response against any client-side "already-sent" record.

    Per-asset errors are logged and swallowed so one bad asset doesn't
    abort the cycle.
    """
    integration_id = str(integration.id)
    now = datetime.now(timezone.utc)

    # 1. Auth + client
    auth_config = integration.get_action_config("auth")
    client = _get_client(auth_config.data)

    # 2. EarthRanger subject type comes from PullObservationsConfig — both
    #    actions on one integration should produce the same subject_type so
    #    observations from hot loop and backfill end up under one subject.
    #    If pull_observations isn't configured on this integration (unusual
    #    but not catastrophic), fall back to the same default used there.
    subject_type = _resolve_subject_type(integration)

    # 3. Asset roster (cached)
    try:
        assets = await fetch_assets_cached(client, integration_id)
    except Exception as exc:
        await log_action_activity(
            integration_id=integration_id,
            action_id=_TRACK_HISTORY_ACTION_ID,
            level=LogLevel.ERROR,
            title="Failed to fetch Tracpoint asset roster",
            data={"error": str(exc)},
            config_data=action_config.dict(),
        )
        raise

    # 4. Per-asset fetch loop — accumulate observations and successful asset ids.
    #
    # Cursor advances are intentionally deferred to step 6, after the Gundi
    # send succeeds. If send_observations_to_gundi raises (transient outage,
    # network blip, 5xx), no cursors move and the next 2-hour cycle re-asks
    # for the same windows. Gundi and EarthRanger dedupe observations
    # server-side, so the resulting overlap on retry is harmless — we prefer
    # "fail safe and retry the window" over "advance and risk permanent loss".
    all_observations: list[dict[str, Any]] = []
    assets_with_data = 0
    asset_errors = 0
    asset_ids_to_advance: list[int] = []

    for asset in assets:
        asset_id = asset.get("assetId")
        if not isinstance(asset_id, int):
            continue

        state = await state_manager.get_state(
            integration_id=integration_id,
            action_id=_TRACK_HISTORY_ACTION_ID,
            source_id=str(asset_id),
        )
        cursor = _load_track_history_cursor(state)
        start_ts, end_ts = _compute_track_history_window(
            cursor=cursor,
            now=now,
            max_lookback_hours=action_config.max_lookback_hours,
            stale_cursor_days=action_config.stale_cursor_days,
        )

        try:
            positions = await client.fetch_positions_for_asset(
                asset_id=asset_id,
                start_timestamp=start_ts,
                end_timestamp=end_ts,
            )
        except Exception as exc:
            asset_errors += 1
            logger.warning(
                "Track-history fetch failed for asset %s on integration %s: %s",
                asset_id, integration_id, exc,
            )
            continue  # don't queue cursor advance — retry the same window next cycle

        if positions:
            assets_with_data += 1
            all_observations.extend(
                transform_to_observations(positions, subject_type=subject_type)
            )

        # Queue this asset for cursor advance. The advance is written in step 6,
        # after the send succeeds — see block comment above.
        asset_ids_to_advance.append(asset_id)

    # 5. Forward to Gundi in batches of at most _GUNDI_OBSERVATION_BATCH_SIZE.
    #    The sensors client posts each batch as a single HTTP request, so
    #    chunking bounds payload size and request time on large cycles. If any
    #    batch raises, the exception propagates before step 6, so no cursors
    #    advance and the next cycle re-sends every batch — harmless because
    #    Gundi and EarthRanger dedupe server-side. (An empty observation list
    #    yields no batches, so no request is made on a quiet cycle.)
    for batch in _batched(all_observations, _GUNDI_OBSERVATION_BATCH_SIZE):
        await send_observations_to_gundi(
            observations=batch,
            integration_id=integration_id,
        )

    # 6. Advance cursors for assets whose fetch succeeded (with or without data).
    #    Advancing even for silent trackers (empty fetch result) is intentional:
    #    without it a tracker that goes quiet would pin us to an ever-growing
    #    window and never catch up once it resumes reporting.
    for asset_id in asset_ids_to_advance:
        await _save_track_history_cursor(state_manager, integration_id, asset_id, now)

    # 7. Activity log
    await log_action_activity(
        integration_id=integration_id,
        action_id=_TRACK_HISTORY_ACTION_ID,
        level=LogLevel.INFO,
        title=(
            f"Backfilled {len(all_observations)} observations across "
            f"{assets_with_data}/{len(assets)} assets (errors: {asset_errors})"
        ),
        data={
            "assets_processed": len(assets),
            "assets_with_data": assets_with_data,
            "asset_errors": asset_errors,
            "observations_processed": len(all_observations),
        },
        config_data=action_config.dict(),
    )

    return {
        "assets_processed": len(assets),
        "assets_with_data": assets_with_data,
        "asset_errors": asset_errors,
        "observations_processed": len(all_observations),
    }
