from datetime import datetime, timezone

import pytest

from app.actions.configurations import PullTrackHistoryConfig
from app.actions.handlers import (
    _compute_track_history_window,
    _load_cursor_from_state,
    _parse_cursor,
    _parse_position_ts,
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
# _load_cursor_from_state
# ---------------------------------------------------------------------------

def test_load_cursor_returns_none_when_state_empty():
    assert _load_cursor_from_state(None) is None
    assert _load_cursor_from_state({}) is None


def test_load_cursor_modern_state_returns_tuple_with_inbound_id():
    state = {"last_cursor": "2026-05-21T10:00:00+00:00", "last_cursor_inbound_id": 42}
    cursor = _load_cursor_from_state(state)
    assert cursor == (datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc), 42)


def test_load_cursor_legacy_state_uses_infinity_to_preserve_drop_on_equality():
    """Pre-tie-breaker state has only `last_cursor`; using +inf preserves
    the previous behavior where any position at the same timestamp gets
    dropped on the first post-upgrade cycle."""
    state = {"last_cursor": "2026-05-21T10:00:00+00:00"}
    cursor = _load_cursor_from_state(state)
    assert cursor[0] == datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc)
    assert cursor[1] == float("inf")


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


def test_first_run_keeps_everything_and_returns_max_cursor():
    raw = [
        _pos("2026-05-21 10:00:00", asset_id=1, inbound_id=10),
        _pos("2026-05-21 10:05:00", asset_id=2, inbound_id=20),
        _pos("2026-05-21 09:55:00", asset_id=3, inbound_id=5),
    ]
    new, cursor = filter_new_positions(raw, cursor=None)
    assert len(new) == 3
    assert cursor == (datetime(2026, 5, 21, 10, 5, tzinfo=timezone.utc), 20)


def test_drops_positions_at_or_before_cursor():
    cursor = (datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc), 50)
    raw = [
        _pos("2026-05-21 09:59:00", asset_id=1, inbound_id=999),  # older ts — drop
        _pos("2026-05-21 10:00:00", asset_id=2, inbound_id=50),   # equal cursor — drop
        _pos("2026-05-21 10:00:00", asset_id=3, inbound_id=49),   # equal ts, lower id — drop
        _pos("2026-05-21 10:00:00", asset_id=4, inbound_id=51),   # equal ts, higher id — keep
        _pos("2026-05-21 10:01:00", asset_id=5, inbound_id=1),    # newer ts — keep
    ]
    new, new_cursor = filter_new_positions(raw, cursor)
    assert [p["assetId"] for p in new] == [4, 5]
    assert new_cursor == (datetime(2026, 5, 21, 10, 1, tzinfo=timezone.utc), 1)


def test_tie_breaker_prevents_dropping_concurrent_assets():
    """Two different assets reporting at the exact same second must both
    be forwarded — this was the bug the composite cursor fixes."""
    raw = [
        _pos("2026-05-21 10:00:00", asset_id=1, inbound_id=100),
        _pos("2026-05-21 10:00:00", asset_id=2, inbound_id=101),
        _pos("2026-05-21 10:00:00", asset_id=3, inbound_id=102),
    ]
    new, cursor = filter_new_positions(raw, cursor=None)
    assert [p["assetId"] for p in new] == [1, 2, 3]
    assert cursor == (datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc), 102)


def test_legacy_infinity_cursor_drops_everything_at_legacy_ts():
    """Modeled after the post-migration first cycle: legacy state had only
    a timestamp, so +inf is used as the inbound_id slot. Records at that
    timestamp should all be dropped (matching pre-upgrade behavior)."""
    cursor = (datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc), float("inf"))
    raw = [
        _pos("2026-05-21 10:00:00", asset_id=1, inbound_id=10_000_000),
        _pos("2026-05-21 10:01:00", asset_id=2, inbound_id=1),
    ]
    new, new_cursor = filter_new_positions(raw, cursor)
    assert [p["assetId"] for p in new] == [2]
    # Cursor advances to the next-cycle value and the infinity sentinel is gone.
    assert new_cursor == (datetime(2026, 5, 21, 10, 1, tzinfo=timezone.utc), 1)


def test_dropped_positions_are_logged_with_cursor_context(caplog):
    """Diagnostic: every dropped position is logged with assetId, timestamp,
    and inboundId plus the cursor it lost to, so production logs can show
    whether an asset's *new* fixes are being dropped for having timestamps
    behind the fleet-wide high-water mark (the suspected delay mechanism)."""
    cursor = (datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc), 50)
    raw = [
        _pos("2026-05-21 09:58:00", asset_id=7, inbound_id=999),  # behind cursor — drop
        _pos("2026-05-21 10:01:00", asset_id=5, inbound_id=1),    # newer — keep
    ]
    with caplog.at_level("INFO", logger="app.actions.handlers"):
        filter_new_positions(raw, cursor)

    assert len(caplog.records) == 1
    message = caplog.records[0].getMessage()
    assert "'assetId': 7" in message
    assert "'timestamp': '2026-05-21 09:58:00'" in message
    assert "'inboundId': 999" in message
    assert "2026-05-21T10:00:00+00:00" in message  # the cursor it was compared to
    assert "assetId': 5" not in message  # forwarded positions aren't logged


def test_no_drop_log_line_when_nothing_dropped(caplog):
    raw = [_pos("2026-05-21 10:01:00", asset_id=5, inbound_id=1)]
    with caplog.at_level("INFO", logger="app.actions.handlers"):
        filter_new_positions(raw, cursor=(datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc), 50))
    assert caplog.records == []


def test_cursor_unchanged_when_nothing_is_new():
    cursor = (datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc), 50)
    raw = [_pos("2026-05-21 09:59:00", asset_id=1, inbound_id=999)]
    new, new_cursor = filter_new_positions(raw, cursor)
    assert new == []
    assert new_cursor == cursor


def test_positions_with_unparseable_timestamps_are_skipped():
    raw = [
        _pos("garbage", asset_id=1, inbound_id=5),
        _pos("2026-05-21 10:00:00", asset_id=2, inbound_id=10),
    ]
    new, cursor = filter_new_positions(raw, cursor=None)
    assert [p["assetId"] for p in new] == [2]
    assert cursor == (datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc), 10)


def test_positions_with_missing_inbound_id_get_minus_one_slot():
    """Inbound id missing from a record shouldn't crash; -1 sentinel is used."""
    raw = [{"assetId": 1, "timestamp": "2026-05-21 10:00:00", "latitude": 0, "longitude": 0}]
    new, cursor = filter_new_positions(raw, cursor=None)
    assert len(new) == 1
    assert cursor == (datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc), -1)


def test_empty_input_returns_empty_and_preserves_cursor():
    cursor = (datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc), 50)
    new, new_cursor = filter_new_positions([], cursor)
    assert new == []
    assert new_cursor == cursor


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
