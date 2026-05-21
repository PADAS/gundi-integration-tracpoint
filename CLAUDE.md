# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this integration does

This is the **Gundi v2 ↔ Tracpoint integration**. It pulls GPS position records from the Tracpoint (Terramar Networks v7) SOAP service and forwards them to [Gundi](https://gundiservice.org) as observations and events.

A parent `CLAUDE.md` at `/Users/chrisdo/padas/CLAUDE.md` describes the generic Gundi integration framework (webhook ingress, JQ transforms, dynamic schemas). **Most of that does not apply here** — this integration is pull-driven via PubSub-triggered cron actions, not webhook-driven. `app/webhooks/handlers.py` and `app/webhooks/configurations.py` are intentionally empty.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Recompile lockfile after editing any .in file
pip-compile --output-file=requirements.txt requirements-base.in requirements-dev.in requirements.in

# Run all tests
pytest

# Single test
pytest app/services/tests/test_action_runner.py::test_execute_action_handler -v

# Run the FastAPI server directly
uvicorn app.main:app --reload --port 8080

# Full local stack (FastAPI + Redis + PubSub emulator + web-ui)
cd local && docker compose up --build
# API docs:  http://localhost:8080/docs
# Web UI:    http://localhost:3000
```

`local/.env.local` is a symlink to `.env.stage` (points the local stack at stage Gundi). `.env.production` exists for prod-config testing. Set `KEYCLOAK_CLIENT_SECRET` before first run — get it from the Gundi team.

## Architecture

### Action flow (this is the primary path)

1. GCP PubSub delivers a message to `POST /` (`app/main.py`)
2. `app/services/action_runner.py::execute_action()` resolves the integration from Gundi, looks up the handler by `action_id`, and invokes it
3. Action handlers live in `app/actions/handlers.py` — two are registered:
   - `action_auth` — validates SOAP creds by calling `getAllAssets`
   - `action_pull_observations` — every 15 min via `@crontab_schedule("*/15 * * * *")`. One fetch per cycle; emits Gundi observations for every position and, when `PullObservationsConfig.emit_events` is True (the default), additionally emits Gundi events for positions tagged with a Tracpoint event (`eventId != 0`).
4. Handlers fetch from Tracpoint via `app/services/client.py::TracpointClient`, transform with `app/services/transformers.py`, and forward via `send_observations_to_gundi` / `send_events_to_gundi`

### Tracpoint SOAP specifics (`app/services/client.py`)

- WSDL-based async SOAP via `zeep.AsyncClient` (RPC/encoded, SOAP 1.1, `strict=False`)
- **No session/token** — `userCompany`, `userName`, `userPassword` are passed in every operation's body
- Default endpoint: `http://www.terramarnetworks.net/v7/index.php?wsdl` (overridable per integration via `AuthenticateConfig.wsdl_url`)
- Operations used: `getAllAssets`, `getAllPositions`, `getSinglePositions(assetId, startTimestamp, endTimestamp)`, `getEvents`
- Timestamp format Tracpoint expects/returns: `"YYYY-MM-DD HH:MM:SS"` (naive, assume UTC)
- `_check_status()` raises `RuntimeError` on non-`OK` status. `NO_POSITION_DATA` is treated as normal (empty list), not an error.

### Incremental pull strategy

Both pull actions use a shared pattern, persisted in Redis via `IntegrationStateManager` (key: `integration_id` + `action_id`):

- **First run** (no `last_cursor`): call `getAllPositions` once for a snapshot of all assets' latest positions
- **Subsequent runs**: enumerate assets via `getAllAssets`, then call `getSinglePositions` per asset with `[last_cursor, now]`
- `last_cursor` is stored as an ISO-8601 UTC string and converted to Tracpoint format via `_to_tracpoint_ts()` on read
- Cursor advances to "now" only when at least one raw record was returned — so empty fetches don't lose ground

### Observations vs. events (`app/services/transformers.py`)

Tracpoint "events" are **tags on position records** (`eventId != 0`, e.g. "Speeding", "Geofence Entry") — not a separate data type.

- `transform_to_observations()` emits every position (tagged or not) as a Gundi observation, keyed by `assetId`. `subject_type` comes from `PullObservationsConfig` (default `"vehicle"`).
- `transform_to_events()` emits only positions where `eventId` is non-zero, as Gundi events with `event_type = f"tracpoint_{snake_case(eventName)}"`.
- A record tagged with an event therefore produces **both** an observation (continuous track) and an event (alert).
- Both transformers run on the same fetched batch inside `action_pull_observations`; there is no separate "pull events" action.
- Transformers swallow per-record errors and log a warning rather than aborting the batch.

### Action configurations (`app/actions/configurations.py`)

- `AuthenticateConfig` — `wsdl_url`, `company`, `username`, `password` (`SecretStr`)
- `PullObservationsConfig` — `lookback_days` (1–30, unused at the moment except as UI guidance — cursor logic uses `getAllPositions` on first run), `subject_type`, `emit_events` (default `True`; flip off for tracking-only deployments that should not surface Tracpoint events in EarthRanger's alerts pane)

Both use `FieldWithUIOptions` / `UIOptions` / `GlobalUISchemaOptions` to control how the Gundi portal renders the config forms (react-jsonschema-form ui schema).

### Webhook path

`app/webhooks/handlers.py` and `app/webhooks/configurations.py` are empty. The `POST /webhooks` route still exists (mounted from `app/routers/webhooks.py`) but has nothing to dispatch to. Do not add webhook support unless explicitly requested.

## Key dependencies

- **`zeep==4.3.2`** — SOAP client. Pinned because Tracpoint's WSDL is RPC/encoded and newer zeep versions have regressed handling for that style. Don't bump without verifying against the live service.
- Other dependencies come from `requirements-base.in` (framework) and `requirements-dev.in` (test tooling).

## Testing

Tests use `pytest` + `pytest-asyncio` + `pytest-mock`. Fixtures are in the (very large) `app/conftest.py` — shared across every test file. External services (Gundi API, PubSub, Redis, SOAP) must be mocked.

**No tests exist yet** for the Tracpoint-specific modules (`app/services/client.py`, `app/services/transformers.py`, `app/actions/handlers.py`). If you add behavior in those files, add tests under `app/services/tests/` mirroring the existing pattern.
