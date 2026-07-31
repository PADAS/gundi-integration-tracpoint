# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this integration does

This is the **Gundi v2 ↔ Tracpoint integration**. It pulls GPS position records from the Tracpoint (Terramar Networks v7) SOAP service and forwards them to [Gundi](https://gundiservice.org) as observations and events.

The upstream `gundi-integration-action-runner` template (which this repo was forked from) supports webhook ingress with JQ transforms and dynamic schemas. **None of that applies here** — this integration is pull-driven via PubSub-triggered cron actions, not webhook-driven. `app/webhooks/handlers.py` and `app/webhooks/configurations.py` are intentionally empty.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Recompile lockfile after editing any .in file
uv pip compile -o requirements.txt requirements-base.in requirements.in requirements-dev.in

# Run all tests
pytest

# Single test
pytest app/services/tests/test_action_runner.py::test_execute_action_handler -v

# Run the FastAPI server directly
uvicorn app.main:app --reload --port 8080

# Full local stack (FastAPI + Redis + PubSub emulator)
cd local && docker compose up --build
# API docs:  http://localhost:8080/docs
# (The optional web-ui service is behind a compose profile; add
#  `--profile web-ui` to the up command for the React UI on http://localhost:3000.)
```

`local/.env.local` is a symlink to `.env.stage` (points the local stack at stage Gundi). `.env.production` exists for prod-config testing. Set `KEYCLOAK_CLIENT_SECRET` before first run — get it from the Gundi team.

## Architecture

### Action flow (this is the primary path)

1. GCP PubSub delivers a message to `POST /` (`app/main.py`)
2. `app/services/action_runner.py::execute_action()` resolves the integration from Gundi, looks up the handler by `action_id`, and invokes it
3. Action handlers live in `app/actions/handlers.py` — three are registered:
   - `action_auth` — validates SOAP creds by calling `getAllAssets`
   - `action_pull_observations` — every 2 min via `@crontab_schedule("*/2 * * * *")`. Single `getAllPositions` call per cycle; emits Gundi observations for every fresh position. Can additionally emit Gundi events for positions tagged with a Tracpoint event (`eventId != 0`) when `PullObservationsConfig.emit_events` is set to True — **default is False** because event delivery to EarthRanger requires Gundi's dispatcher-side reference-data provisioning, which is not yet deployed. Keep `emit_events=False` until that capability is in place; otherwise EarthRanger will reject the unknown event types on POST.
   - `action_pull_track_history` — every 2 hours via `@crontab_schedule("0 */2 * * *")`. For each asset, calls `getSinglePositions(assetId, start, end)` over a per-asset cursor window and forwards the result as Gundi observations to recover intermediate fixes that the 2-min hot loop missed. No event emission. Per-asset cursor stored in Redis at `integration_state.{integration_id}.pull_track_history.{asset_id}`. Observations are POSTed to Gundi in batches of 200 (`_GUNDI_OBSERVATION_BATCH_SIZE`) so a dense cycle doesn't send one unbounded request; cursors advance only after all batches succeed, and a mid-batch failure re-sends everything next cycle (Gundi dedupes). Configurable lookback / staleness via `PullTrackHistoryConfig`.
4. Handlers fetch from Tracpoint via `app/services/client.py::TracpointClient`, transform with `app/services/transformers.py`, and forward via `send_observations_to_gundi` / `send_events_to_gundi`

### Tracpoint SOAP specifics (`app/services/client.py`)

- WSDL-based async SOAP via `zeep.AsyncClient` (RPC/encoded, SOAP 1.1, `strict=False`)
- **No session/token** — `userCompany`, `userName`, `userPassword` are passed in every operation's body
- Default endpoint: `http://www.terramarnetworks.net/v7/index.php?wsdl` (overridable per integration via `AuthenticateConfig.wsdl_url`). **Do not switch the default to v10 yet** — see "v10 known issue" below.
- Operations used: `getAllAssets`, `getAllPositions`, `getSinglePositions(assetId, startTimestamp, endTimestamp)`, `getEvents`
- Timestamp format Tracpoint expects/returns: `"YYYY-MM-DD HH:MM:SS"` (naive, assume UTC)
- `_check_status()` raises `RuntimeError` on non-`OK` status. `NO_POSITION_DATA` is treated as normal (empty list), not an error.

### Pull strategy

Every cycle issues a single `getAllPositions` SOAP call — one network round-trip regardless of fleet size. Tracpoint returns the latest known position for each asset on every call, so we dedup client-side against a per-integration high-water mark stored in Redis via `IntegrationStateManager`:

- The cursor is a composite `(timestamp, inboundId)` tuple persisted as two Redis fields: `last_cursor` (ISO-8601 UTC string of the newest position previously forwarded) and `last_cursor_inbound_id` (the Tracpoint `inboundId` of that record). The inboundId acts as a tie-breaker for the rare case where two assets report at the exact same second — without it we'd silently drop one of them.
- Each cycle calls `filter_new_positions()` (in `app/actions/handlers.py`) — positions whose `(timestamp, inboundId)` tuple compares `<=` the cursor are dropped; the remainder are forwarded to Gundi and the cursor advances to the new maximum tuple seen.
- Cursor only advances when something was actually forwarded, so empty cycles (no asset moved) don't lose ground.
- Pre-tie-breaker state (older deployments that only wrote `last_cursor`) is read with `+inf` in the inboundId slot, which preserves the legacy drop-on-equality semantics until the cursor naturally advances to a newer timestamp.
- Per-asset `getSinglePositions` is not used in the hot loop. Trade-off: if a tracker reports multiple times inside one 2-min window, only the most recent fix is captured. This is acceptable for the fleets we currently target. Reintroduce per-asset queries if higher-resolution tracks are needed for a specific deployment.
- `getAllAssets` is not called in the hot loop either. `app/services/tracpoint_cache.py` (`TracpointAssetCache`, `fetch_assets_cached`) remains in the codebase for future use (e.g., enriching observations with asset-type metadata) but is not invoked today.

### Observations vs. events (`app/services/transformers.py`)

Tracpoint "events" are **tags on position records** (`eventId != 0`, e.g. "Speeding", "Geofence Entry") — not a separate data type.

- `transform_to_observations()` emits every position (tagged or not) as a Gundi observation, keyed by `assetId`. `subject_type` comes from `PullObservationsConfig` (default `"vehicle"`).
- `transform_to_events()` emits only positions where `eventId` is non-zero, as Gundi events with `event_type = f"tracpoint_{snake_case(eventName)}"`.
- A record tagged with an event therefore produces **both** an observation (continuous track) and an event (alert).
- Both transformers run on the same fetched batch inside `action_pull_observations`; there is no separate "pull events" action.
- Transformers swallow per-record errors and log a warning rather than aborting the batch.

### Action configurations (`app/actions/configurations.py`)

- `AuthenticateConfig` — `wsdl_url`, `company`, `username`, `password` (`SecretStr`)
- `PullObservationsConfig` — `subject_type` (default `"truck"`), `emit_events` (default `False`; do not flip on until Gundi's dispatcher-side reference-data provisioning is deployed, otherwise EarthRanger will reject unknown event types). `subject_type` is the canonical EarthRanger subject type for the integration and is also read at runtime by `action_pull_track_history` so both actions agree.
- `PullTrackHistoryConfig` — `max_lookback_hours` (default 24), `stale_cursor_days` (default 7). Tunes how aggressively the every-2-hour backfill clamps its time window after long outages. **No `subject_type` field on purpose** — the action borrows it from `PullObservationsConfig` via `_resolve_subject_type()` so the two actions can't drift out of sync.

All three use `FieldWithUIOptions` / `UIOptions` / `GlobalUISchemaOptions` to control how the Gundi portal renders the config forms (react-jsonschema-form ui schema).

### v10 known issue

Terramar publishes a v10 WSDL alongside v7. Side-by-side the two WSDLs look wire-compatible — v10 only adds `uid` to `Position` and `year` to `Asset` on top of v7, and the operation signatures we use are identical. Based on that diff, commit `4840ed2` flipped the default `wsdl_url` to v10. **This broke production** — but not for the reasons we initially suspected.

The actual failure mode (confirmed by running `local/probe_tracpoint_v10.py` against the WCS account with v7 and v10 side-by-side on 2026-05-23):

- v7 `getAllPositions` returns 35 records, including fresh fixes within the last polling window. Status `OK`.
- v10 `getAllPositions` returns **0 records** for the same credentials, same call. Status also `OK`. The response body is literally `<positions ... arrayType="tns:Position[0]"/>`.

There is **no wire-format bug**: timestamps, `inboundId` types, and the rest of the Position schema are identical on v7 (we never observed v10's non-empty form). v10 simply does not surface this customer's fleet. Most likely: Terramar's web-services entitlement is configured per API version, and the WCS account is enabled for v7 web services but not v10. Authentication still succeeds (no `LOGIN_FAILED`) — the account just appears to have no assets visible on v10.

Symptom in production was therefore predictable: every 2-minute cycle logged `raw_positions_fetched=0`, the cursor frozen at the last v7 fetch (`2026-05-22T17:19:27+00:00`), and no observations flowing to Gundi while the portal kept showing the fleet moving.

The default is pinned back to v7 across `app/actions/configurations.py`, `app/actions/handlers.py`, and `app/services/client.py`. Before another v10 cut-over:

1. Confirm with Terramar that web-services access is **explicitly enabled on the v10 endpoint** for the customer account in question (don't assume v7 entitlement carries over).
2. Re-run `local/probe_tracpoint_v10.py` and verify v10 returns a non-empty fleet.
3. Only then change the portal's per-integration `wsdl_url`, and watch `raw_positions_fetched` on the next 2-3 polling cycles.

**Lesson:** matching WSDL diffs do not guarantee matching service behavior. The bug here wasn't in the wire shape — it was in the customer-account entitlement model on Terramar's side, which is invisible from the WSDL. Smoke-test new SOAP endpoints by actually fetching production data; `action_auth` (which only calls `getAllAssets`) is not enough on its own — that operation may return an empty list with status `OK` while v7 has a full fleet.

### Webhook path

`app/webhooks/handlers.py` and `app/webhooks/configurations.py` are empty. The `POST /webhooks` route still exists (mounted from `app/routers/webhooks.py`) but has nothing to dispatch to. Do not add webhook support unless explicitly requested.

## Key dependencies

- **`zeep==4.3.2`** — SOAP client. Pinned because Tracpoint's WSDL is RPC/encoded and newer zeep versions have regressed handling for that style. Don't bump without verifying against the live service.
- Other dependencies come from `requirements-base.in` (framework) and `requirements-dev.in` (test tooling).

## Testing

Tests use `pytest` + `pytest-asyncio` + `pytest-mock`. Fixtures are in the (very large) `app/conftest.py` — shared across every test file. External services (Gundi API, PubSub, Redis, SOAP) must be mocked.

Coverage for the Tracpoint-specific modules:

- `app/services/tests/test_zeep_cache.py` — Redis WSDL cache.
- `app/services/tests/test_tracpoint_cache.py` — asset roster cache + `fetch_assets_cached`.
- `app/services/tests/test_action_handlers.py` — cursor parsing and `filter_new_positions` dedup logic.
- `app/services/tests/test_transformers.py` — `transform_to_observations` / `transform_to_events` mapping.

`app/services/client.py` (the SOAP layer) has no direct unit tests yet — its surface is mostly thin wrappers around zeep, and exercising it meaningfully requires a stubbed SOAP server. End-to-end coverage relies on stage smoke-testing against the live Tracpoint service.
