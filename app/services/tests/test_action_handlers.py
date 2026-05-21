from datetime import datetime, timezone

import pytest

from app.actions.handlers import (
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
# filter_new_positions
# ---------------------------------------------------------------------------

def _pos(ts: str, asset_id: int = 1) -> dict:
    return {"assetId": asset_id, "timestamp": ts, "latitude": 0.0, "longitude": 0.0}


def test_first_run_keeps_everything_and_returns_max_timestamp():
    raw = [
        _pos("2026-05-21 10:00:00", asset_id=1),
        _pos("2026-05-21 10:05:00", asset_id=2),
        _pos("2026-05-21 09:55:00", asset_id=3),
    ]
    new, cursor = filter_new_positions(raw, cursor=None)
    assert len(new) == 3
    assert cursor == datetime(2026, 5, 21, 10, 5, tzinfo=timezone.utc)


def test_drops_positions_at_or_before_cursor():
    cursor = datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc)
    raw = [
        _pos("2026-05-21 09:59:00", asset_id=1),  # older
        _pos("2026-05-21 10:00:00", asset_id=2),  # equal — already submitted
        _pos("2026-05-21 10:01:00", asset_id=3),  # newer — keep
    ]
    new, new_cursor = filter_new_positions(raw, cursor)
    assert [p["assetId"] for p in new] == [3]
    assert new_cursor == datetime(2026, 5, 21, 10, 1, tzinfo=timezone.utc)


def test_cursor_unchanged_when_nothing_is_new():
    cursor = datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc)
    raw = [_pos("2026-05-21 09:59:00")]
    new, new_cursor = filter_new_positions(raw, cursor)
    assert new == []
    assert new_cursor == cursor


def test_positions_with_unparseable_timestamps_are_skipped():
    raw = [
        _pos("garbage", asset_id=1),
        _pos("2026-05-21 10:00:00", asset_id=2),
    ]
    new, cursor = filter_new_positions(raw, cursor=None)
    assert [p["assetId"] for p in new] == [2]
    assert cursor == datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc)


def test_empty_input_returns_empty_and_preserves_cursor():
    cursor = datetime(2026, 5, 21, 10, 0, tzinfo=timezone.utc)
    new, new_cursor = filter_new_positions([], cursor)
    assert new == []
    assert new_cursor == cursor
