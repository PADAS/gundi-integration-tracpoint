from datetime import datetime, timezone

import pytest

from app.actions.configurations import AuthenticateConfig, PullTrackHistoryConfig
from app.actions.handlers import action_pull_track_history


@pytest.fixture
def fixed_now(mocker):
    """Pin handlers.datetime.now(timezone.utc) so windows are deterministic."""
    now = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
    # `datetime` is imported at module top; patch the module's reference.
    fake_dt = mocker.MagicMock(wraps=datetime)
    fake_dt.now.return_value = now
    mocker.patch("app.actions.handlers.datetime", fake_dt)
    return now


@pytest.fixture
def integration(mocker):
    """An Integration mock that returns different configs by action_id.

    Two action configs are exposed: `auth` (for the SOAP client) and
    `pull_observations` (which the track-history action reads `subject_type`
    from). Tests that want to override per-call behavior can reassign
    `integration.get_action_config.side_effect`.
    """
    integration = mocker.MagicMock()
    integration.id = "integration-1"

    auth = mocker.MagicMock()
    auth.data = {
        "wsdl_url": "http://www.terramarnetworks.net/v7/index.php?wsdl",
        "company": "ACME",
        "username": "user",
        "password": "pw",
    }
    pull_obs = mocker.MagicMock()
    pull_obs.data = {"subject_type": "truck", "emit_events": False}

    configs = {"auth": auth, "pull_observations": pull_obs}
    integration.get_action_config.side_effect = lambda action_id: configs.get(action_id)
    return integration


@pytest.fixture
def mocked_externals(mocker):
    """Mock the Tracpoint client, asset cache, state manager, and Gundi sender."""
    client = mocker.MagicMock()
    client.fetch_positions_for_asset = mocker.AsyncMock()
    mocker.patch("app.actions.handlers._get_client", return_value=client)

    fetch_assets = mocker.patch(
        "app.actions.handlers.fetch_assets_cached",
        new_callable=mocker.AsyncMock,
    )

    state_manager = mocker.patch("app.actions.handlers.state_manager")
    state_manager.get_state = mocker.AsyncMock(return_value={})
    state_manager.set_state = mocker.AsyncMock()

    send = mocker.patch(
        "app.actions.handlers.send_observations_to_gundi",
        new_callable=mocker.AsyncMock,
    )
    log = mocker.patch(
        "app.actions.handlers.log_action_activity",
        new_callable=mocker.AsyncMock,
    )
    # The @activity_logger() decorator calls publish_event directly (start/complete/error
    # events). Mock it at the source so tests don't hit real PubSub.
    publish = mocker.patch(
        "app.services.activity_logger.publish_event",
        new_callable=mocker.AsyncMock,
    )
    return {
        "client": client,
        "fetch_assets": fetch_assets,
        "state": state_manager,
        "send": send,
        "log": log,
        "publish": publish,
    }


@pytest.mark.asyncio
async def test_happy_path_fetches_each_asset_and_advances_cursor(
    fixed_now, integration, mocked_externals,
):
    mocked_externals["fetch_assets"].return_value = [
        {"assetId": 1, "displayName": "Rover A"},
        {"assetId": 2, "displayName": "Rover B"},
    ]
    mocked_externals["client"].fetch_positions_for_asset.side_effect = [
        [{"assetId": 1, "inboundId": 100, "timestamp": "2026-05-23 11:00:00",
          "latitude": 1.0, "longitude": 2.0}],
        [{"assetId": 2, "inboundId": 200, "timestamp": "2026-05-23 11:30:00",
          "latitude": 3.0, "longitude": 4.0}],
    ]

    result = await action_pull_track_history(integration, PullTrackHistoryConfig())

    # Asset roster fetched once, positions fetched once per asset.
    mocked_externals["fetch_assets"].assert_awaited_once()
    assert mocked_externals["client"].fetch_positions_for_asset.await_count == 2

    # Two observations forwarded to Gundi in a single batch.
    mocked_externals["send"].assert_awaited_once()
    (kwargs,) = mocked_externals["send"].await_args_list
    assert len(kwargs.kwargs["observations"]) == 2

    # Cursor advanced to `now` for both assets.
    set_state_calls = mocked_externals["state"].set_state.await_args_list
    assert len(set_state_calls) == 2
    for c in set_state_calls:
        assert c.kwargs["state"] == {"last_fetched_to": "2026-05-23T12:00:00+00:00"}

    assert result["observations_processed"] == 2
    assert result["assets_processed"] == 2


@pytest.mark.asyncio
async def test_empty_roster_emits_no_observations(
    fixed_now, integration, mocked_externals,
):
    mocked_externals["fetch_assets"].return_value = []

    result = await action_pull_track_history(integration, PullTrackHistoryConfig())

    mocked_externals["client"].fetch_positions_for_asset.assert_not_called()
    mocked_externals["send"].assert_not_called()
    mocked_externals["state"].set_state.assert_not_called()
    assert result["observations_processed"] == 0
    assert result["assets_processed"] == 0


@pytest.mark.asyncio
async def test_per_asset_fetch_error_does_not_abort_batch(
    fixed_now, integration, mocked_externals,
):
    mocked_externals["fetch_assets"].return_value = [
        {"assetId": 1}, {"assetId": 2}, {"assetId": 3},
    ]
    mocked_externals["client"].fetch_positions_for_asset.side_effect = [
        [{"assetId": 1, "inboundId": 100, "timestamp": "2026-05-23 11:00:00",
          "latitude": 1.0, "longitude": 2.0}],
        RuntimeError("Tracpoint barfed"),
        [{"assetId": 3, "inboundId": 300, "timestamp": "2026-05-23 11:30:00",
          "latitude": 5.0, "longitude": 6.0}],
    ]

    result = await action_pull_track_history(integration, PullTrackHistoryConfig())

    # Two assets succeeded, one errored. Cursor only advanced for the two
    # successful ones; the errored asset will retry its window next cycle.
    assert result["assets_with_data"] == 2
    assert result["asset_errors"] == 1
    assert result["observations_processed"] == 2
    assert mocked_externals["state"].set_state.await_count == 2


@pytest.mark.asyncio
async def test_existing_cursor_used_as_window_start(
    fixed_now, integration, mocked_externals,
):
    mocked_externals["fetch_assets"].return_value = [{"assetId": 7}]
    mocked_externals["state"].get_state.return_value = {
        "last_fetched_to": "2026-05-23T10:00:00+00:00",
    }
    mocked_externals["client"].fetch_positions_for_asset.return_value = []

    await action_pull_track_history(integration, PullTrackHistoryConfig())

    fetch_call = mocked_externals["client"].fetch_positions_for_asset.await_args
    assert fetch_call.kwargs["start_timestamp"] == "2026-05-23 10:00:00"
    assert fetch_call.kwargs["end_timestamp"] == "2026-05-23 12:00:00"


@pytest.mark.asyncio
async def test_cursor_advances_even_when_no_positions_returned(
    fixed_now, integration, mocked_externals,
):
    mocked_externals["fetch_assets"].return_value = [{"assetId": 7}]
    mocked_externals["client"].fetch_positions_for_asset.return_value = []

    await action_pull_track_history(integration, PullTrackHistoryConfig())

    # Asset reported nothing but we still advance the cursor — silent
    # trackers must not pin us to ever-growing windows.
    mocked_externals["state"].set_state.assert_awaited_once()
    assert mocked_externals["state"].set_state.await_args.kwargs["state"] == {
        "last_fetched_to": "2026-05-23T12:00:00+00:00",
    }


@pytest.mark.asyncio
async def test_roster_fetch_failure_logs_and_raises(
    fixed_now, integration, mocked_externals,
):
    """If fetch_assets_cached raises, log an ERROR-level activity and re-raise.

    No per-asset work happens, no observations are sent, no cursors move. This
    is the operational boundary between "transient asset-level error (swallow
    and continue)" and "infrastructure-level error (fail loudly)".
    """
    mocked_externals["fetch_assets"].side_effect = RuntimeError("redis down")

    with pytest.raises(RuntimeError, match="redis down"):
        await action_pull_track_history(integration, PullTrackHistoryConfig())

    mocked_externals["client"].fetch_positions_for_asset.assert_not_called()
    mocked_externals["send"].assert_not_called()
    mocked_externals["state"].set_state.assert_not_called()
    mocked_externals["log"].assert_awaited_once()
    log_kwargs = mocked_externals["log"].await_args.kwargs
    assert log_kwargs["level"].name == "ERROR"
    assert log_kwargs["action_id"] == "pull_track_history"
    assert "redis down" in str(log_kwargs["data"])


def _positions(asset_id: int, count: int) -> list[dict]:
    """Build `count` distinct position records for one asset."""
    return [
        {"assetId": asset_id, "inboundId": i, "timestamp": "2026-05-23 11:00:00",
         "latitude": 1.0, "longitude": 2.0}
        for i in range(count)
    ]


@pytest.mark.asyncio
async def test_observations_sent_in_batches_of_200(
    fixed_now, integration, mocked_externals,
):
    """A cycle producing more than 200 observations is split into multiple
    sensors-API requests of at most 200 each — never one unbounded POST."""
    mocked_externals["fetch_assets"].return_value = [{"assetId": 1}]
    # 450 positions for one asset → 450 observations → 3 batches (200/200/50).
    mocked_externals["client"].fetch_positions_for_asset.return_value = _positions(1, 450)

    result = await action_pull_track_history(integration, PullTrackHistoryConfig())

    batch_sizes = [
        len(c.kwargs["observations"])
        for c in mocked_externals["send"].await_args_list
    ]
    assert batch_sizes == [200, 200, 50]
    assert result["observations_processed"] == 450
    # Cursor still advances exactly once, after all batches land.
    mocked_externals["state"].set_state.assert_awaited_once()


@pytest.mark.asyncio
async def test_exactly_200_observations_sent_as_single_batch(
    fixed_now, integration, mocked_externals,
):
    """Boundary: exactly 200 observations is one batch, not two."""
    mocked_externals["fetch_assets"].return_value = [{"assetId": 1}]
    mocked_externals["client"].fetch_positions_for_asset.return_value = _positions(1, 200)

    await action_pull_track_history(integration, PullTrackHistoryConfig())

    batch_sizes = [
        len(c.kwargs["observations"])
        for c in mocked_externals["send"].await_args_list
    ]
    assert batch_sizes == [200]


@pytest.mark.asyncio
async def test_batch_send_failure_advances_no_cursors(
    fixed_now, integration, mocked_externals,
):
    """If a later batch fails, the exception propagates before any cursor
    advances, so the next cycle re-sends every batch (safe — Gundi dedupes)."""
    mocked_externals["fetch_assets"].return_value = [{"assetId": 1}]
    mocked_externals["client"].fetch_positions_for_asset.return_value = _positions(1, 300)
    # First batch (200) succeeds, second batch (100) raises.
    mocked_externals["send"].side_effect = [None, RuntimeError("gundi 503")]

    with pytest.raises(RuntimeError, match="gundi 503"):
        await action_pull_track_history(integration, PullTrackHistoryConfig())

    assert mocked_externals["send"].await_count == 2
    mocked_externals["state"].set_state.assert_not_called()


@pytest.mark.asyncio
async def test_subject_type_inherited_from_pull_observations(
    fixed_now, integration, mocked_externals, mocker,
):
    """Forwarded observations use PullObservationsConfig.subject_type, not a
    value local to PullTrackHistoryConfig — keeps the hot loop and the
    backfill aligned under one EarthRanger subject type."""
    # Reconfigure the integration so the pull_observations config supplies
    # an explicit subject_type that we can recognise in the forwarded batch.
    auth = integration.get_action_config("auth")
    pull_obs = mocker.MagicMock()
    pull_obs.data = {"subject_type": "ranger-truck", "emit_events": False}
    integration.get_action_config.side_effect = (
        lambda action_id: {"auth": auth, "pull_observations": pull_obs}.get(action_id)
    )
    mocked_externals["fetch_assets"].return_value = [{"assetId": 1}]
    mocked_externals["client"].fetch_positions_for_asset.return_value = [
        {"assetId": 1, "inboundId": 100, "timestamp": "2026-05-23 11:00:00",
         "latitude": 1.0, "longitude": 2.0},
    ]

    await action_pull_track_history(integration, PullTrackHistoryConfig())

    observations = mocked_externals["send"].await_args.kwargs["observations"]
    assert observations[0]["subject_type"] == "ranger-truck"


@pytest.mark.asyncio
async def test_subject_type_falls_back_when_pull_observations_unconfigured(
    fixed_now, integration, mocked_externals,
):
    """If the integration has no pull_observations action configured at all,
    fall back to the default declared in PullObservationsConfig rather than
    crashing or producing an unset subject_type."""
    # Drop the pull_observations side of the lookup — only auth remains.
    auth = integration.get_action_config("auth")
    integration.get_action_config.side_effect = (
        lambda action_id: auth if action_id == "auth" else None
    )
    mocked_externals["fetch_assets"].return_value = [{"assetId": 1}]
    mocked_externals["client"].fetch_positions_for_asset.return_value = [
        {"assetId": 1, "inboundId": 100, "timestamp": "2026-05-23 11:00:00",
         "latitude": 1.0, "longitude": 2.0},
    ]

    await action_pull_track_history(integration, PullTrackHistoryConfig())

    from app.actions.configurations import PullObservationsConfig
    expected_default = PullObservationsConfig.__fields__["subject_type"].default

    observations = mocked_externals["send"].await_args.kwargs["observations"]
    assert observations[0]["subject_type"] == expected_default
