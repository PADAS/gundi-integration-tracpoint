# Tracpoint ↔ EarthRanger comparison mode for `spot_check_positions.py`

**Date:** 2026-07-30 · **Ticket:** GUNDI-5543 · **Status:** approved (design discussed in-session)

## Problem

Field users report vehicles going quiet on ER for hours while the Terramar
portal shows movement. We have three hypotheses (see GUNDI-5543): stale data
inside the v7 `getAllPositions` web service, the integration's fleet-wide
cursor dropping backdated fixes, or by-design 2-hour backfill latency. The
existing `local/spot_check_positions.py` can only see the Tracpoint side.
Comparing what Tracpoint's API serves against what actually landed in ER
distinguishes "Gundi dropped/delayed it" from "Terramar never served it".

## Approach (chosen: extend the existing script)

Extend `local/spot_check_positions.py` with an optional ER comparison layer,
active only when ER connection flags/env are provided. Without them the
script behaves exactly as today. Rejected alternatives: a separate
comparison script (duplicates the Tracpoint fetch; two tools for one
question) and an in-integration monitoring action (needs prod deploy and ER
creds in Gundi config for what is a temporary diagnosis phase).

## ER access

- Env vars `ER_SITE_URL` (e.g. `https://<site>.pamdas.org`) and `ER_TOKEN`
  (bearer token), loadable via the existing `--env-file` mechanism.
- Optional `ER_PROVIDER_KEY` / `--er-provider-key` to scope the source
  lookup to this integration's provider.
- Plain `httpx` requests; no new dependencies.

## Mapping Tracpoint → ER (no name matching)

ER subject names are hand-edited on site, so names are not used at all.
Gundi registers each vehicle as an ER **source** whose `manufacturer_id`
is the Tracpoint `assetId` (the `source` field our transformer emits).

- At startup: `GET /api/v1.0/sources?provider_key=<key>` (paginated;
  `provider_key` filters on the provider's key string and accepts
  comma-lists). Without a provider key, fetch all sources and intersect
  `manufacturer_id` with the fleet's assetIds.
- Build `assetId → source UUID`. Print a mapping summary
  (`N matched / M fleet assets unmatched`) so mapping failures are
  loudly visible on the first cycle.

## Comparison semantics

All comparisons are per-source against ER **observations** (the ingest
ground truth), not subject `last_position` (which reflects display state
and can be affected by ER-side subject configuration).

### Fleet watch cycle (default table, `--watch`)

Per cycle: one `getAllPositions` call + a latest-observation lookup per
matched source. The lookup does NOT trust server-side ordering — the live
site returned the *oldest* row in the window for
`ordering=-recorded_at&page_size=1` (see PR #10 discussion), so
`ERClient.fetch_latest_er_ts()` queries a time window and takes
`max(recorded_at)` client-side: first a minutes-wide window answering
"does ER have the newest Tracpoint fix (or newer)?", then progressively
wider lookbacks (2 h / 26 h / 7 d) only when that is empty. ~16 live
sources, trivial at 60 s cadence.

Per vehicle verdict, with `--tolerance-min` (default 10) allowing for
Gundi pipeline latency:

| Verdict | Condition | Meaning |
| --- | --- | --- |
| `OK` | ER latest within tolerance of Tracpoint latest | pipeline healthy |
| `LAGGING` | ER behind Tracpoint beyond tolerance | Gundi delayed/dropped — the target signal |
| `TP-REGRESSED` | ER observation **newer** than Tracpoint's latest fix | v7 API serving stale data — Terramar-side problem |
| `NO-MATCH` | no ER source for this assetId | mapping/provisioning gap |

`TP-REGRESSED` works because ER only receives what this integration sends
from that same API — ER being ahead is impossible unless the API's
`getAllPositions` view regressed.

### Drop detection (`--asset ID --hours H`)

Diff `getSinglePositions(window)` against
`GET /api/v1.0/observations?source_id=<uuid>` for the same window.
Match fixes by `recorded_at` within ±2 s; sanity-check lat/lon on matches.
Tracpoint fixes older than the tolerance with no ER counterpart are listed
as **candidate drops**, cross-referenceable against the integration's
`Dropped N position(s)` Cloud Run log lines.

## Logging (`--log FILE`)

JSONL, appended per watch cycle:

- one cycle-summary record: `{t, assets, matched, ok, lagging, tp_regressed, no_match}`
- one record per non-OK vehicle: `{t, assetId, verdict, tp_ts, tp_inbound_id, er_ts, lag_s}`

Small enough to run overnight; analyzable after the fact.

## Error handling

- ER or Tracpoint fetch failure: log the error, skip the cycle, keep
  watching. The tool must survive unattended overnight runs.
- ER 401/403: explicit "token invalid/expired" message (once per
  occurrence, keep running).
- Source mapping empty: hard exit with guidance (wrong provider key /
  wrong site / token lacks permissions).

## Testing

- Comparison/verdict logic is a pure function; exercised with fake data
  (same style as the existing formatter checks).
- Live verification requires a real ER token — done by the user on first
  run; the mapping summary and one known-fresh vehicle serve as the
  smoke test.

## Out of scope

- Subject `last_position` comparison (add later only if observations look
  healthy while the map still lags — that would implicate ER display, not
  Gundi).
- Any change to the integration/production code.
- Alerting/notifications; the JSONL file is the deliverable.
