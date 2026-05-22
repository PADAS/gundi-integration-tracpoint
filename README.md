# gundi-integration-tracpoint

Gundi v2 Action Runner for the **Tracpoint** GPS tracking service (Terramar Networks v7 SOAP).

Pulls GPS position records from a Tracpoint customer account on a 2-minute cadence and forwards them to [Gundi](https://gundiservice.org) as observations. Optionally forwards Tracpoint event tags (speeding, geofence breach, panic alert, etc.) as Gundi events for surfacing in EarthRanger's alerts pane.

## What it does

- **One** `getAllPositions` SOAP call per cycle — single network round-trip regardless of fleet size.
- **Client-side dedup** against a `(timestamp, inboundId)` high-water mark stored in Redis so each position is forwarded exactly once.
- **Three-tier caching** (process-local zeep `AsyncClient`, Redis-backed WSDL/XSD cache, dormant asset-roster cache) to minimize cold-start and vendor load.
- Runs on Cloud Run, triggered by GCP PubSub.

For the full architecture write-up, decisions, and edge-case notes, see [`CLAUDE.md`](./CLAUDE.md).

## Prerequisite — Tracpoint web-service access

Tracpoint requires that web-service access be **explicitly enabled by Terramar Networks** on the customer account before SOAP calls will authenticate. Portal login alone is not sufficient. If `action_auth` returns `LOGIN_FAILED`, contact your Terramar account manager to enable web services for the account — see the vendor docs at [`docs/tracpoint_v10_web_services_1.1.docx`](./docs/tracpoint_v10_web_services_1.1.docx).

## Actions

| Action | Trigger | Purpose |
|---|---|---|
| `action_auth` | On-demand | Validates SOAP credentials by calling `getAllAssets`. |
| `action_pull_observations` | `*/2 * * * *` (every 2 min) | Fetches positions and forwards to Gundi as observations (and optionally events). |

### `AuthenticateConfig`

| Field | Type | Notes |
|---|---|---|
| `wsdl_url` | str | Default `http://www.terramarnetworks.net/v7/index.php?wsdl`. Override if your tenant uses a different endpoint. |
| `company` | str | `userCompany` value — your Tracpoint company name or alias. |
| `username` | str | `userName` — Tracpoint service-account user. |
| `password` | `SecretStr` | `userPassword`. |

### `PullObservationsConfig`

| Field | Type | Default | Notes |
|---|---|---|---|
| `subject_type` | str | `"vehicle"` | EarthRanger subject type applied to all observations from this integration. |
| `emit_events` | bool | `False` | Keep off until Gundi's dispatcher-side reference-data provisioning is deployed — otherwise EarthRanger rejects unknown event types on POST. |

## Local development

```bash
# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest

# Full local stack (FastAPI + Redis + PubSub emulator)
cd local && docker compose up --build
# API docs at http://localhost:8080/docs
```

`local/.env.local` is a symlink to `.env.stage` and points the local stack at Gundi stage. `.env.production` exists for prod-config testing. Set `KEYCLOAK_CLIENT_SECRET` before first run — get the value from the Gundi team.

Recompile the lockfile after editing any `.in` file:

```bash
uv pip compile -o requirements.txt requirements-base.in requirements.in requirements-dev.in
```

## Diagnostics

Set `DEBUG_SOAP_ENVELOPES=1` to log the outbound SOAP envelope when Tracpoint returns a non-`OK` status. Credentials (`userCompany` / `userName` / `userPassword`) are redacted before logging.

## Project layout

```
app/
├── actions/
│   ├── configurations.py   # AuthenticateConfig, PullObservationsConfig
│   └── handlers.py         # action_auth, action_pull_observations + dedup helpers
├── services/
│   ├── client.py           # TracpointClient (zeep AsyncClient + caching)
│   ├── transformers.py     # Tracpoint Position → Gundi observation / event
│   ├── tracpoint_cache.py  # Asset-roster cache (dormant; kept for future enrichment)
│   ├── zeep_cache.py       # Redis-backed WSDL/XSD cache for zeep
│   └── …                   # framework code from the gundi-integration-action-runner template
└── webhooks/               # intentionally empty — this integration is pull-driven
docker/                     # production Dockerfile
local/                      # docker-compose stack, env files, helpers
docs/                       # Tracpoint SOAP web-services reference (.docx)
```

## Known limitations

- **Resolution ceiling.** `getAllPositions` returns the latest position per asset. If a tracker reports more than once inside a 2-minute window, only the most recent fix is captured. Reintroduce `getSinglePositions` per asset in `app/actions/handlers.py` if higher-resolution tracks become a requirement.
- **Events deferred.** `emit_events=False` is the default because EarthRanger validates event types on POST and the upstream catalog (`getEvents`) is dynamic. Flip on only after Gundi's dispatcher-side reference-data provisioning ships; see the [project plan](#) tracked by the Gundi platform team.

## Built on

[`gundi-integration-action-runner`](https://github.com/PADAS/gundi-integration-action-runner) — the upstream template for Gundi v2 integrations. Most of the framework code under `app/services/` comes from there. This repo specializes the template to Tracpoint and intentionally drops the webhook ingress path (`app/webhooks/` is empty).
