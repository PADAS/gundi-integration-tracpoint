# Tracpoint track-history backfill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a second scheduled action `action_pull_track_history` that, every 2 hours, calls `getSinglePositions` per asset to backfill full-resolution track data, complementing the existing 2-minute `action_pull_observations` snapshot hot loop.

**Architecture:** A new pull action lives alongside the existing one in `app/actions/handlers.py`. It uses the dormant `TracpointAssetCache` to enumerate the fleet, persists a per-asset cursor in Redis via `IntegrationStateManager`'s `source_id` slot (one key per `(integration_id, asset_id)` pair), and emits Gundi observations only (no events). Because both Gundi and EarthRanger dedupe server-side, the windows the two actions cover may overlap freely with no client-side filtering.

**Tech Stack:** Python 3.10, FastAPI, zeep (SOAP), `redis.asyncio`, pytest + pytest-asyncio + pytest-mock. All existing — no new dependencies.

---

## Background

The current `action_pull_observations` fires every 2 min and calls `getAllPositions` once per cycle. Tracpoint's `getAllPositions` returns only the *latest* fix per asset, so if a tracker reports more than once inside a 2-minute window (e.g. "Journey Periodic" events during active driving), the intermediate fixes are lost. The customer wants those fixes for reporting purposes but doesn't need them in near-real-time.

Solution: a second action that runs less often (every 2 hours), uses `getSinglePositions(assetId, start, end)` to ask for the full position list per asset within a time window, and forwards those to Gundi. Steady-state vendor load goes from ~30 SOAP calls/hour to ~30 + (35 * 0.5) ≈ ~48 calls/hour on a 35-vehicle fleet. Gundi/EarthRanger handle deduplication, so any fix the hot loop already captured will be silently discarded downstream.

## File Structure

**Modified:**
- `app/actions/configurations.py` — add `PullTrackHistoryConfig`
- `app/actions/handlers.py` — add `action_pull_track_history` and three helpers (`_compute_track_history_window`, `_load_track_history_cursor`, `_save_track_history_cursor`)
- `app/services/tests/test_action_handlers.py` — add tests for the three helpers
- `CLAUDE.md` — document the new action in the architecture section
- `README.md` — add the new action to the Actions table

**Created:**
- `app/services/tests/test_action_pull_track_history.py` — end-to-end test for the action handler with mocked client, state, and Gundi forwarding

**Untouched (relevant but no edits needed):**
- `app/services/tracpoint_cache.py` — `fetch_assets_cached` already supports what we need
- `app/services/client.py` — `fetch_positions_for_asset` already exists
- `app/services/transformers.py` — `transform_to_observations` is reused as-is
- `app/services/state.py` — `IntegrationStateManager` accepts `source_id` already
- `app/actions/__init__.py` — handlers auto-discover via `action_` prefix; no registration change needed

## Open conventions to preserve

- The existing `action_pull_observations` log payload uses keys like `raw_positions_fetched`, `new_positions_after_dedup`, `observations_processed`. Mirror that style for the new action so the activity log stays consistent.
- Cursor timestamps are ISO-8601 with explicit UTC offset (e.g. `"2026-05-21T10:00:00+00:00"`), parsed via `datetime.fromisoformat` after Z→`+00:00` substitution. Don't invent a new format.
- Tracpoint timestamps on the wire are `"YYYY-MM-DD HH:MM:SS"` (naive, treated as UTC). The constant `_POSITION_TS_FORMAT` already exists in `handlers.py` — reuse it for the start/end window strings passed to `getSinglePositions`.

---

## Task 1: Add `PullTrackHistoryConfig`

**Files:**
- Modify: `app/actions/configurations.py`
- Test: there is no separate config test file in the repo; the config is exercised indirectly. We still want a unit test for the defaults — add it to `app/services/tests/test_action_handlers.py` alongside the existing helper tests.

- [ ] **Step 1: Write the failing test**

Add to `app/services/tests/test_action_handlers.py` near the top imports and a new section at the bottom of the file:

```python
from app.actions.configurations import PullTrackHistoryConfig


# ---------------------------------------------------------------------------
# PullTrackHistoryConfig
# ---------------------------------------------------------------------------

def test_pull_track_history_config_has_sensible_defaults():
    config = PullTrackHistoryConfig()
    assert config.subject_type == "vehicle"
    assert config.max_lookback_hours == 24
    assert config.stale_cursor_days == 7


def test_pull_track_history_config_accepts_overrides():
    config = PullTrackHistoryConfig(
        subject_type="ranger",
        max_lookback_hours=6,
        stale_cursor_days=3,
    )
    assert config.subject_type == "ranger"
    assert config.max_lookback_hours == 6
    assert config.stale_cursor_days == 3
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest app/services/tests/test_action_handlers.py::test_pull_track_history_config_has_sensible_defaults -v`
Expected: `ImportError` or `AttributeError` — `PullTrackHistoryConfig` does not exist.

- [ ] **Step 3: Write minimal implementation**

Edit `app/actions/configurations.py`. Append after the existing `PullObservationsConfig` class:

```python
class PullTrackHistoryConfig(PullActionConfiguration):
    subject_type: str = FieldWithUIOptions(
        "vehicle",
        title="Subject Type",
        description=(
            "EarthRanger subject type applied to all observations forwarded by "
            "the track-history backfill. Should normally match "
            "PullObservationsConfig.subject_type."
        ),
    )
    max_lookback_hours: int = FieldWithUIOptions(
        24,
        title="Maximum lookback (hours)",
        description=(
            "On a cold start, or when an asset's saved cursor is older than "
            "stale_cursor_days, the action will not ask Tracpoint for more "
            "than this many hours of history. Keeps the SOAP window bounded "
            "even after long outages."
        ),
    )
    stale_cursor_days: int = FieldWithUIOptions(
        7,
        title="Stale cursor threshold (days)",
        description=(
            "Saved per-asset cursors older than this are treated as cold "
            "starts and clamped to now - max_lookback_hours. Defends against "
            "asking Tracpoint for ranges it may have purged."
        ),
    )
    ui_global_options = GlobalUISchemaOptions(order=["subject_type", "max_lookback_hours", "stale_cursor_days"])
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `pytest app/services/tests/test_action_handlers.py -v -k pull_track_history_config`
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
git add app/actions/configurations.py app/services/tests/test_action_handlers.py
git commit -m "Add PullTrackHistoryConfig"
```

---

## Task 2: Add `_compute_track_history_window` helper

This is the function that decides what `(start, end)` time range to ask Tracpoint for, given a per-asset cursor. Pure function — takes `now` as a parameter so tests can pin the clock.

**Files:**
- Modify: `app/actions/handlers.py`
- Test: `app/services/tests/test_action_handlers.py`

- [ ] **Step 1: Write the failing tests**

Append to `app/services/tests/test_action_handlers.py`:

```python
from app.actions.handlers import _compute_track_history_window


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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest app/services/tests/test_action_handlers.py -v -k _compute_track_history_window`
Expected: `ImportError` — `_compute_track_history_window` does not exist.

- [ ] **Step 3: Write minimal implementation**

Edit `app/actions/handlers.py`. Add this function after the existing `_cursor_for_position`:

```python
from datetime import timedelta  # add to existing datetime imports at top of file


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
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `pytest app/services/tests/test_action_handlers.py -v -k _compute_track_history_window`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/services/tests/test_action_handlers.py
git commit -m "Add _compute_track_history_window helper"
```

---

## Task 3: Add per-asset cursor load/save helpers

The track-history action stores one cursor per asset, using `IntegrationStateManager`'s `source_id` slot (which is currently always `"no-source"` in `action_pull_observations`). Keeping the load/save logic in dedicated helpers keeps the action body short.

**Files:**
- Modify: `app/actions/handlers.py`
- Test: `app/services/tests/test_action_handlers.py`

- [ ] **Step 1: Write the failing tests**

Append to `app/services/tests/test_action_handlers.py`:

```python
from app.actions.handlers import (
    _load_track_history_cursor,
    _save_track_history_cursor,
    _TRACK_HISTORY_ACTION_ID,
)


# ---------------------------------------------------------------------------
# _load_track_history_cursor
# ---------------------------------------------------------------------------

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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest app/services/tests/test_action_handlers.py -v -k track_history_cursor`
Expected: `ImportError` — symbols not defined.

- [ ] **Step 3: Write minimal implementation**

Edit `app/actions/handlers.py`. Add after `_compute_track_history_window`:

```python
# Used as the action_id slot in IntegrationStateManager keys. Centralized so
# the load/save helpers and the action handler agree on the spelling and the
# scheduler decorator below has a single source of truth.
_TRACK_HISTORY_ACTION_ID = "pull_track_history"


def _load_track_history_cursor(state: dict | None) -> datetime | None:
    """Read the per-asset 'last_fetched_to' timestamp out of integration state."""
    if not state:
        return None
    return _parse_cursor(state.get("last_fetched_to"))


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
```

- [ ] **Step 4: Run the tests and make sure they pass**

Run: `pytest app/services/tests/test_action_handlers.py -v -k track_history_cursor`
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
git add app/actions/handlers.py app/services/tests/test_action_handlers.py
git commit -m "Add per-asset track-history cursor helpers"
```

---

## Task 4: Add `action_pull_track_history` handler

The action itself. Loads the asset roster, computes a per-asset window, fetches positions, transforms, forwards, advances the cursor. Per-asset errors are logged and swallowed so one bad asset doesn't abort the whole cycle.

**Files:**
- Modify: `app/actions/handlers.py`
- Create: `app/services/tests/test_action_pull_track_history.py`

- [ ] **Step 1: Write the failing test (happy path)**

Create `app/services/tests/test_action_pull_track_history.py`:

```python
from datetime import datetime, timezone
from unittest.mock import call

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
    integration = mocker.MagicMock()
    integration.id = "integration-1"
    auth = mocker.MagicMock()
    auth.data = {
        "wsdl_url": "http://www.terramarnetworks.net/v7/index.php?wsdl",
        "company": "ACME",
        "username": "user",
        "password": "pw",
    }
    integration.get_action_config.return_value = auth
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
    return {
        "client": client,
        "fetch_assets": fetch_assets,
        "state": state_manager,
        "send": send,
        "log": log,
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest app/services/tests/test_action_pull_track_history.py -v`
Expected: `ImportError` — `action_pull_track_history` does not exist.

- [ ] **Step 3: Write minimal implementation**

Edit `app/actions/handlers.py`. Add at the top of the file with the other imports:

```python
from app.services.tracpoint_cache import fetch_assets_cached
```

Then append after the existing `action_pull_observations`:

```python
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

    # 2. Asset roster (cached)
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

    # 3. Per-asset fetch loop
    all_observations: list[dict[str, Any]] = []
    assets_with_data = 0
    asset_errors = 0

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
            continue  # don't advance cursor, retry the same window next cycle

        if positions:
            assets_with_data += 1
            all_observations.extend(
                transform_to_observations(positions, subject_type=action_config.subject_type)
            )

        # Advance the cursor even when no positions came back — otherwise a
        # silent tracker would force us to re-ask for the same long window
        # forever and never catch up.
        await _save_track_history_cursor(state_manager, integration_id, asset_id, now)

    # 4. Forward to Gundi
    if all_observations:
        await send_observations_to_gundi(
            observations=all_observations,
            integration_id=integration_id,
        )

    # 5. Activity log
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
```

- [ ] **Step 4: Run the happy-path test and verify it passes**

Run: `pytest app/services/tests/test_action_pull_track_history.py -v`
Expected: 1 passed.

- [ ] **Step 5: Add edge-case tests**

Append to `app/services/tests/test_action_pull_track_history.py`:

```python
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
```

- [ ] **Step 6: Run all the action's tests and verify they pass**

Run: `pytest app/services/tests/test_action_pull_track_history.py -v`
Expected: 5 passed.

- [ ] **Step 7: Run the full suite as a regression check**

Run: `pytest --tb=short -q`
Expected: previously-existing 120 tests still pass plus 11 new ones (2 + 4 + 4 + 5 = 15 minus shared fixtures, look for 11 new = 131 total). Adjust the assertion to whatever the actual final count is — the point is no existing test broke.

- [ ] **Step 8: Commit**

```bash
git add app/actions/handlers.py app/services/tests/test_action_pull_track_history.py
git commit -m "Add action_pull_track_history backfill action"
```

---

## Task 5: Update docs

**Files:**
- Modify: `CLAUDE.md`
- Modify: `README.md`

- [ ] **Step 1: Update `CLAUDE.md`**

In `CLAUDE.md`, replace the existing "Action flow (this is the primary path)" subsection list so it lists three actions instead of two. The relevant existing block reads (paraphrased — locate it by the "Action handlers live in" anchor):

```
3. Action handlers live in `app/actions/handlers.py` — two are registered:
   - `action_auth` — validates SOAP creds by calling `getAllAssets`
   - `action_pull_observations` — every 2 min via `@crontab_schedule("*/2 * * * *")`...
```

Change it to:

```
3. Action handlers live in `app/actions/handlers.py` — three are registered:
   - `action_auth` — validates SOAP creds by calling `getAllAssets`
   - `action_pull_observations` — every 2 min via `@crontab_schedule("*/2 * * * *")`. Single `getAllPositions` call per cycle; emits Gundi observations for every fresh position. Can additionally emit Gundi events for positions tagged with a Tracpoint event (`eventId != 0`) when `PullObservationsConfig.emit_events` is set to True — **default is False** because event delivery to EarthRanger requires Gundi's dispatcher-side reference-data provisioning, which is not yet deployed. Keep `emit_events=False` until that capability is in place; otherwise EarthRanger will reject the unknown event types on POST.
   - `action_pull_track_history` — every 2 hours via `@crontab_schedule("0 */2 * * *")`. For each asset, calls `getSinglePositions(assetId, start, end)` over a per-asset cursor window and forwards the result as Gundi observations to recover intermediate fixes that the 2-min hot loop missed. No event emission. Per-asset cursor stored in Redis at `integration_state.{integration_id}.pull_track_history.{asset_id}`. Configurable lookback / staleness via `PullTrackHistoryConfig`.
```

Also, in the "Action configurations" section, add the new config to the bullet list:

Find:
```
- `PullObservationsConfig` — `subject_type`, `emit_events` (default `False`; do not flip on until Gundi's dispatcher-side reference-data provisioning is deployed, otherwise EarthRanger will reject unknown event types)
```

And append after it:
```
- `PullTrackHistoryConfig` — `subject_type`, `max_lookback_hours` (default 24), `stale_cursor_days` (default 7). Tunes how aggressively the every-2-hour backfill clamps its time window after long outages.
```

- [ ] **Step 2: Update `README.md`**

In `README.md`, the existing actions table reads:

```
| Action | Trigger | Purpose |
|---|---|---|
| `action_auth` | On-demand | Validates SOAP credentials by calling `getAllAssets`. |
| `action_pull_observations` | `*/2 * * * *` (every 2 min) | Fetches positions and forwards to Gundi as observations (and optionally events). |
```

Replace with:

```
| Action | Trigger | Purpose |
|---|---|---|
| `action_auth` | On-demand | Validates SOAP credentials by calling `getAllAssets`. |
| `action_pull_observations` | `*/2 * * * *` (every 2 min) | Fetches the latest position per asset and forwards to Gundi as observations (and optionally events). |
| `action_pull_track_history` | `0 */2 * * *` (every 2 hours) | Per-asset `getSinglePositions` backfill — recovers full-resolution track between hot-loop snapshots. Observations only, no events. |
```

In the same section, add a `PullTrackHistoryConfig` table mirroring the `PullObservationsConfig` one. Add immediately after the existing `PullObservationsConfig` table:

```
### `PullTrackHistoryConfig`

| Field | Type | Default | Notes |
|---|---|---|---|
| `subject_type` | str | `"vehicle"` | EarthRanger subject type applied to all backfilled observations — usually matches `PullObservationsConfig.subject_type`. |
| `max_lookback_hours` | int | `24` | On cold start (or stale cursor) the window is clamped to at most this many hours, so a long outage doesn't ask Tracpoint for ranges it may have purged. |
| `stale_cursor_days` | int | `7` | Per-asset cursors older than this are treated as cold starts. |
```

- [ ] **Step 3: Run the full test suite one final time**

Run: `pytest --tb=short -q`
Expected: all tests pass.

- [ ] **Step 4: Commit**

```bash
git add CLAUDE.md README.md
git commit -m "Document action_pull_track_history"
```

---

## Verification checklist (run after Task 5)

- [ ] `pytest` — all tests green (120 pre-existing + ~11 new)
- [ ] `python -c "from app.actions import action_handlers; assert 'pull_track_history' in action_handlers, list(action_handlers.keys())"` — confirms auto-discovery picked up the new handler
- [ ] No new top-level dependencies added — `requirements.in` untouched
- [ ] The probe script `local/probe_tracpoint_v10.py` was not modified
- [ ] Git log shows 5 commits, all atomic (config → window helper → cursor helpers → action → docs)

## Out of scope for this plan

These are intentional deferrals — record them but don't implement:

- **Smarter cursor seeding from `getAllPositions`** — could prime per-asset cursors from the latest hot-loop snapshot to avoid the cold-start lookback. Not needed for first cut.
- **Backfill-aware GET window for "Journey Periodic" only** — could filter the backfill to driving-event records only to reduce volume. Premature optimization.
- **Per-integration cron override via portal config** — for now everyone uses `0 */2 * * *`. The action_scheduler module supports overrides if a customer needs a different cadence; we don't need to expose that until someone asks.
- **Self-registration in `app/register.py`** — auto-discovery via the `action_` prefix handles registration on its own. No `register.py` edit needed.
