from datetime import datetime, timezone

import pytest

from app.actions.configurations import PullTrackHistoryConfig
from app.actions.handlers import (
    _compute_track_history_window,
    _fleet_max_cursor,
    _load_asset_cursors_from_state,
    _parse_cursor,
    _parse_position_ts,
    _serialize_asset_cursors,
    filter_new_positions,
)


# ---------------------------------------------------------------------------
# _parse_cursor
# ---------------------------------------------------------------------------

def test_parse_cursor_returns_none_for_empty_inputs():
    assert _parse_cursor(None) is None
    assert _parse_cursor("") is None


def test_parse_cursor_accepts_iso_with_offset():
    assert _parse_cursor("2026-05-21T10:00:00+00:00") == datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc)


def test_parse_cursor_accepts_zulu_form():
    assert _parse_cursor("2026-05-21T10:00:00Z") == datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc)


def test_parse_cursor_returns_none_on_garbage():
    assert _parse_cursor("not a date") is None


# ---------------------------------------------------------------------------
# _parse_position_ts
# ---------------------------------------------------------------------------

def test_parse_position_ts_treats_naive_as_utc():
    assert _parse_position_ts("2026-05-21 10:00:00") == datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc)


def test_parse_position_ts_returns_none_for_empty():
    assert _parse_position_ts(None) is None
    assert _parse_position_ts("") is None


def test_parse_position_ts_returns_none_on_unexpected_format():
    assert _parse_position_ts("2026-05-21T10:00:00Z") is None  # ISO 8601 isn't Tracpoint's format


# ---------------------------------------------------------------------------
# _load_asset_cursors_from_state / _serialize_asset_cursors / _fleet_max_cursor
# ---------------------------------------------------------------------------

def test_load_asset_cursors_returns_empty_for_empty_state():
    assert _load_asset_cursors_from_state(None) == {}
    assert _load_asset_cursors_from_state({}) == {}


def test_load_asset_cursors_round_trips_through_serialization():
    cursors = {
        4980958: (datetime(2026, 7, 31, 11, 48, 13, tzinfo=timezone.utc), 6119224424),
        4978857: (datetime(2026, 7, 31, 11, 18, 30, tzinfo=timezone.utc), 6119180000),
    }
    state = {"asset_cursors": _serialize_asset_cursors(cursors)}
    assert _load_asset_cursors_from_state(state) == cursors
    # JSON-safe: keys are strings, values are [iso_string, int]
    assert set(state["asset_cursors"].keys()) == {"4980958", "4978857"}
    assert state["asset_cursors"]["4980958"] == ["2026-07-31T11:48:13+00:00", 6119224424]


def test_load_asset_cursors_ignores_legacy_fleet_cursor_state():
    """State written by the fleet-wide-cursor deployments has only
    `last_cursor`/`last_cursor_inbound_id`. We deliberately start fresh:
    the first post-deploy cycle re-forwards each asset's latest position
    once, and Gundi/ER dedupe server-side. Seeding every asset from the
    fleet max would re-drop late-surfacing fixes at the moment of
    migration — the very bug this change fixes."""
    state = {"last_cursor": "2026-05-21T10:00:00+00:00", "last_cursor_inbound_id": 42}
    assert _load_asset_cursors_from_state(state) == {}


def test_load_asset_cursors_skips_malformed_entries():
    state = {"asset_cursors": {
        "4980958": ["2026-07-31T11:48:13+00:00", 6119224424],
        "not-an-id": ["2026-07-31T11:48:13+00:00", 1],
        "4978857": ["garbage", 2],
        "4978858": "not-a-list",
        "4978861": ["2026-07-31T11:00:00+00:00"],
    }}
    assert _load_asset_cursors_from_state(state) == {
        4980958: (datetime(2026, 7, 31, 11, 48, 13, tzinfo=timezone.utc), 6119224424),
    }


def test_fleet_max_cursor_returns_max_tuple_for_rollback_compat():
    cursors = {
        1: (datetime(2026, 7, 31, 11, 0, tzinfo=timezone.utc), 500),
        2: (datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc), 100),
        3: (datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc), 99),
    }
    assert _fleet_max_cursor(cursors) == (datetime(2026, 7, 31, 12, 0, tzinfo=timezone.utc), 100)
    assert _fleet_max_cursor({}) is None


# ---------------------------------------------------------------------------
# filter_new_positions
# ---------------------------------------------------------------------------

def _pos(ts: str, asset_id: int = 1, inbound_id: int = 100) -> dict:
    return {
        "assetId": asset_id,
        "inboundId": inbound_id,
        "timestamp": ts,
        "latitude": 0.0,
        "longitude": 0.0,
    }


def _c(ts: str, inbound: int) -> tuple:
    return (datetime.strptime(ts, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc), inbound)


def test_first_run_keeps_everything_and_returns_per_asset_cursors():
    raw = [
        _pos("2026-05-21 10:00:00", asset_id=1, inbound_id=10),
        _pos("2026-05-21 10:05:00", asset_id=2, inbound_id=20),
        _pos("2026-05-21 09:55:00", asset_id=3, inbound_id=5),
    ]
    new, cursors = filter_new_positions(raw, asset_cursors={})
    assert len(new) == 3
    assert cursors == {
        1: _c("2026-05-21 10:00:00", 10),
        2: _c("2026-05-21 10:05:00", 20),
        3: _c("2026-05-21 09:55:00", 5),
    }


def test_backdated_fix_for_one_asset_is_forwarded_despite_fresher_fleet():
    """THE GUNDI-5543 regression test, from the production capture of
    2026-07-31 12:46 UTC: asset 4980958's fix timestamped 11:50:13 surfaced
    in getAllPositions ~56 minutes late, by which time other vehicles had
    pushed the fleet-wide cursor past 12:43. The fleet-wide cursor dropped
    it and EarthRanger showed nothing for this vehicle until the 14:00
    backfill. Per-asset cursors must forward it: it is newer than *this
    asset's* previous fix (11:48:13)."""
    asset_cursors = {
        4980958: _c("2026-07-31 11:48:13", 6119224424),
        4978857: _c("2026-07-31 12:43:12", 6119329790),  # fleet is way ahead
    }
    raw = [
        _pos("2026-07-31 11:50:13", asset_id=4980958, inbound_id=6119331904),  # late arrival
        _pos("2026-07-31 12:43:12", asset_id=4978857, inbound_id=6119329790),  # unchanged
    ]
    new, cursors = filter_new_positions(raw, asset_cursors)
    assert [p["assetId"] for p in new] == [4980958]
    assert cursors[4980958] == _c("2026-07-31 11:50:13", 6119331904)
    assert cursors[4978857] == _c("2026-07-31 12:43:12", 6119329790)


def test_drops_positions_at_or_behind_each_assets_own_cursor():
    asset_cursors = {
        1: _c("2026-05-21 10:00:00", 50),
        2: _c("2026-05-21 10:00:00", 50),
        3: _c("2026-05-21 10:00:00", 50),
    }
    raw = [
        _pos("2026-05-21 09:59:00", asset_id=1, inbound_id=999),  # behind own cursor — drop
        _pos("2026-05-21 10:00:00", asset_id=2, inbound_id=50),   # equals own cursor — drop
        _pos("2026-05-21 10:00:00", asset_id=3, inbound_id=51),   # same ts, higher id — keep
    ]
    new, cursors = filter_new_positions(raw, asset_cursors)
    assert [p["assetId"] for p in new] == [3]
    assert cursors[1] == _c("2026-05-21 10:00:00", 50)   # unchanged
    assert cursors[3] == _c("2026-05-21 10:00:00", 51)   # advanced


def test_unknown_asset_is_always_forwarded():
    asset_cursors = {1: _c("2026-05-21 10:00:00", 50)}
    raw = [_pos("2026-05-21 09:00:00", asset_id=99, inbound_id=1)]  # old fix, new asset
    new, cursors = filter_new_positions(raw, asset_cursors)
    assert [p["assetId"] for p in new] == [99]
    assert cursors[99] == _c("2026-05-21 09:00:00", 1)


def test_concurrent_assets_at_same_second_all_forwarded():
    raw = [
        _pos("2026-05-21 10:00:00", asset_id=1, inbound_id=100),
        _pos("2026-05-21 10:00:00", asset_id=2, inbound_id=101),
        _pos("2026-05-21 10:00:00", asset_id=3, inbound_id=102),
    ]
    new, _ = filter_new_positions(raw, asset_cursors={})
    assert [p["assetId"] for p in new] == [1, 2, 3]


def test_cursors_for_absent_assets_are_retained():
    """An asset missing from this cycle's response keeps its saved cursor
    (a roster hiccup must not reset dedup for that vehicle)."""
    asset_cursors = {7: _c("2026-05-21 10:00:00", 50)}
    new, cursors = filter_new_positions(
        [_pos("2026-05-21 10:05:00", asset_id=8, inbound_id=60)], asset_cursors,
    )
    assert cursors[7] == _c("2026-05-21 10:00:00", 50)
    assert cursors[8] == _c("2026-05-21 10:05:00", 60)


def test_dropped_positions_are_logged_with_their_assets_cursor(caplog):
    """Diagnostic: every dropped position is logged with assetId, timestamp,
    inboundId, and the asset's own cursor it lost to. With per-asset cursors
    a drop only ever means "this asset re-reported an already-forwarded fix
    or went backwards" — both worth seeing in logs, neither data loss."""
    asset_cursors = {
        7: _c("2026-05-21 10:00:00", 50),
        5: _c("2026-05-21 10:00:00", 50),
    }
    raw = [
        _pos("2026-05-21 09:58:00", asset_id=7, inbound_id=999),  # behind own cursor — drop
        _pos("2026-05-21 10:01:00", asset_id=5, inbound_id=1),    # newer — keep
    ]
    with caplog.at_level("INFO", logger="app.actions.handlers"):
        filter_new_positions(raw, asset_cursors)

    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "'assetId': 7" in message
    assert "'timestamp': '2026-05-21 09:58:00'" in message
    assert "'inboundId': 999" in message
    assert "'cursor_ts': '2026-05-21T10:00:00+00:00'" in message  # this asset's own cursor
    assert "assetId': 5" not in message  # forwarded positions aren't logged


def test_drop_log_line_is_capped_for_large_fleets(caplog):
    """The log line inlines at most _DROPPED_LOG_SAMPLE_SIZE tuples and says
    how many were omitted, so a large fleet can't bloat or truncate the entry."""
    from app.actions.handlers import _DROPPED_LOG_SAMPLE_SIZE

    total = _DROPPED_LOG_SAMPLE_SIZE + 7
    asset_cursors = {i: _c("2026-05-21 10:00:00", 50) for i in range(total)}
    raw = [
        _pos("2026-05-21 09:58:00", asset_id=i, inbound_id=i)
        for i in range(total)  # all behind their own cursors — all dropped
    ]
    with caplog.at_level("INFO", logger="app.actions.handlers"):
        filter_new_positions(raw, asset_cursors)

    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert f"Dropped {total} position(s)" in message
    assert message.count("'assetId'") == _DROPPED_LOG_SAMPLE_SIZE
    assert "7 more omitted" in message


def test_no_drop_log_line_when_nothing_dropped(caplog):
    raw = [_pos("2026-05-21 10:01:00", asset_id=5, inbound_id=1)]
    with caplog.at_level("INFO", logger="app.actions.handlers"):
        filter_new_positions(raw, asset_cursors={5: _c("2026-05-21 10:00:00", 50)})
    assert caplog.records == []


def test_cursors_unchanged_when_nothing_is_new():
    asset_cursors = {1: _c("2026-05-21 10:00:00", 50)}
    new, cursors = filter_new_positions(
        [_pos("2026-05-21 09:59:00", asset_id=1, inbound_id=999)], asset_cursors,
    )
    assert new == []
    assert cursors == asset_cursors


def test_positions_with_unparseable_timestamps_are_skipped():
    raw = [
        _pos("garbage", asset_id=1, inbound_id=5),
        _pos("2026-05-21 10:00:00", asset_id=2, inbound_id=10),
    ]
    new, cursors = filter_new_positions(raw, asset_cursors={})
    assert [p["assetId"] for p in new] == [2]
    assert cursors == {2: _c("2026-05-21 10:00:00", 10)}


def test_positions_with_missing_inbound_id_get_minus_one_slot():
    """Inbound id missing from a record shouldn't crash; -1 sentinel is used."""
    raw = [{"assetId": 1, "timestamp": "2026-05-21 10:00:00", "latitude": 0, "longitude": 0}]
    new, cursors = filter_new_positions(raw, asset_cursors={})
    assert len(new) == 1
    assert cursors == {1: _c("2026-05-21 10:00:00", -1)}


def test_positions_without_asset_id_are_skipped():
    raw = [{"timestamp": "2026-05-21 10:00:00", "inboundId": 5, "latitude": 0, "longitude": 0}]
    new, cursors = filter_new_positions(raw, asset_cursors={})
    assert new == []
    assert cursors == {}


def test_empty_input_returns_empty_and_preserves_cursors():
    asset_cursors = {1: _c("2026-05-21 10:00:00", 50)}
    new, cursors = filter_new_positions([], asset_cursors)
    assert new == []
    assert cursors == asset_cursors


# ---------------------------------------------------------------------------
# PullTrackHistoryConfig
# ---------------------------------------------------------------------------

def test_pull_track_history_config_has_sensible_defaults():
    config = PullTrackHistoryConfig()
    assert config.max_lookback_hours == 24
    assert config.stale_cursor_days == 7


def test_pull_track_history_config_accepts_overrides():
    config = PullTrackHistoryConfig(
        max_lookback_hours=6,
        stale_cursor_days=3,
    )
    assert config.max_lookback_hours == 6
    assert config.stale_cursor_days == 3


def test_pull_track_history_config_does_not_own_subject_type():
    """subject_type is intentionally absent — the action borrows it from
    PullObservationsConfig at runtime via _resolve_subject_type()."""
    assert "subject_type" not in PullTrackHistoryConfig.__fields__


def test_pull_observations_config_defaults_to_truck():
    """Default subject_type is 'truck' — the typical Tracpoint customer
    fleet is patrol/ranger trucks; existing integrations that have set the
    value explicitly in the portal are unaffected."""
    from app.actions.configurations import PullObservationsConfig
    config = PullObservationsConfig()
    assert config.subject_type == "truck"


# ---------------------------------------------------------------------------
# _compute_track_history_window
# ---------------------------------------------------------------------------

def test_window_cold_start_uses_max_lookback():
    """No saved cursor → fetch the full lookback window ending at now."""
    now = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
    start, end = _compute_track_history_window(
        cursor=None, now=now, max_lookback_hours=24, stale_cursor_days=7,
    )
    assert start == "2026-05-22 12:00:00"
    assert end == "2026-05-23 12:00:00"


def test_window_recent_cursor_used_as_start():
    """Cursor within the stale threshold becomes the window start verbatim."""
    now = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
    cursor = datetime(2026, 5, 23, 10, 30, tzinfo=timezone.utc)
    start, end = _compute_track_history_window(
        cursor=cursor, now=now, max_lookback_hours=24, stale_cursor_days=7,
    )
    assert start == "2026-05-23 10:30:00"
    assert end == "2026-05-23 12:00:00"


def test_window_stale_cursor_clamped_to_lookback():
    """Cursor older than stale_cursor_days is treated as a cold start."""
    now = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
    cursor = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)  # 22 days old
    start, end = _compute_track_history_window(
        cursor=cursor, now=now, max_lookback_hours=24, stale_cursor_days=7,
    )
    assert start == "2026-05-22 12:00:00"
    assert end == "2026-05-23 12:00:00"


def test_window_future_cursor_clamped_to_now():
    """Defensive: a cursor in the future (clock skew?) becomes (now, now)."""
    now = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
    cursor = datetime(2026, 5, 24, 0, 0, tzinfo=timezone.utc)
    start, end = _compute_track_history_window(
        cursor=cursor, now=now, max_lookback_hours=24, stale_cursor_days=7,
    )
    assert start == end == "2026-05-23 12:00:00"


# ---------------------------------------------------------------------------
# _load_track_history_cursor
# ---------------------------------------------------------------------------

from app.actions.handlers import (
    _load_track_history_cursor,
    _save_track_history_cursor,
    _TRACK_HISTORY_ACTION_ID,
)


def test_load_track_history_cursor_returns_none_for_empty_state():
    assert _load_track_history_cursor(None) is None
    assert _load_track_history_cursor({}) is None


def test_load_track_history_cursor_parses_iso_timestamp():
    state = {"last_fetched_to": "2026-05-21T10:00:00+00:00"}
    assert _load_track_history_cursor(state) == datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc)


def test_load_track_history_cursor_returns_none_on_garbage():
    assert _load_track_history_cursor({"last_fetched_to": "not a date"}) is None


# ---------------------------------------------------------------------------
# _save_track_history_cursor
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_save_track_history_cursor_writes_iso_with_correct_keys(mocker):
    state_manager = mocker.MagicMock()
    state_manager.set_state = mocker.AsyncMock()
    when = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)

    await _save_track_history_cursor(state_manager, "integration-1", 42, when)

    state_manager.set_state.assert_awaited_once_with(
        integration_id="integration-1",
        action_id=_TRACK_HISTORY_ACTION_ID,
        source_id="42",
        state={"last_fetched_to": "2026-05-23T12:00:00+00:00"},
    )
