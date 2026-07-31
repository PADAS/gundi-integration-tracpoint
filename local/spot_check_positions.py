#!/usr/bin/env python3
"""Spot-check the Tracpoint v7 web service against the Terramar portal.

Background (GUNDI-5543):
    Field users report vehicles going quiet on EarthRanger for hours while
    the Terramar portal shows them moving. The leading hypothesis is that
    the v7 `getAllPositions` web service serves stale data for some assets
    while the portal is current. This script lets you sit with the portal
    open in a browser and query the web service side-by-side to see what
    the API actually has for each vehicle, right now.

Modes:
    (default)      One `getAllPositions` call. Table of every asset's latest
                   fix: timestamp, age, position, maps link. Sorted freshest
                   first. Assets silent longer than --max-age-days are
                   collapsed to a one-line count.

    --watch N      Repeat every N seconds. Fixes that changed since the
                   previous poll are marked NEW — leave it running while
                   watching the portal and see how quickly (or whether)
                   portal movement shows up in the API.

    --asset ID --hours H
                   One `getSinglePositions` call for that asset over the
                   last H hours — the full track the API holds for the
                   window. If the portal showed movement in that window
                   but this returns nothing, the web service is behind
                   the portal and the gap is on Terramar's side.

Retroactive lag check:
    inboundId is globally monotonic with ingest order. If a fix timestamped
    14:00 carries an inboundId that other assets only reached at 17:00, that
    fix arrived at the web service ~3 h late — proof of ingest lag even
    after the fact. Compare the --asset output against the ids in the
    integration's "Dropped N position(s)" Cloud Run log lines.

EarthRanger comparison (optional — set ER_SITE_URL + ER_TOKEN):
    When ER access is configured, every mode also checks what actually
    landed in ER, so "Terramar never served it" and "Gundi dropped or
    delayed it" become distinguishable (GUNDI-5543):

    - The fleet table gains the latest ER observation per vehicle and a
      verdict: OK (ER has the latest fix), PENDING (fix younger than
      --tolerance-min, still in flight), LAGGING (ER missing a fix the
      Tracpoint API has had for longer than the tolerance — Gundi-side
      problem), TP-REGRESSED (ER is *newer* than the Tracpoint API, which
      is only possible if the v7 API is serving stale data — Terramar-side
      problem), NO-MATCH (no ER source for this assetId).
    - --asset mode diffs the Tracpoint track against ER's observations
      for the same window, matching fixes by inbound_id (exact) with a
      +/-2 s timestamp fallback, lists candidate drops, and reports
      Gundi->ER delivery latency (ER created_at minus fix recorded_at).
    - --log FILE appends one JSONL summary per cycle plus one record per
      non-OK vehicle: leave --watch running overnight and analyze later.

    Mapping is via ER *sources* (manufacturer_id == Tracpoint assetId),
    never subject names (those get edited on site). Scope the source
    lookup with ER_PROVIDER_KEY / --er-provider-key if the site has other
    providers with numeric manufacturer ids.

Usage:
    From the repo root, with the project venv active:

        TRACPOINT_COMPANY=acme \\
        TRACPOINT_USERNAME=foo \\
        TRACPOINT_PASSWORD=bar \\
            python local/spot_check_positions.py

        # Credentials from a .env file (same vars):
        python local/spot_check_positions.py --env-file .env.production

        # Live comparison against the portal, refresh every 60 s:
        python local/spot_check_positions.py --env-file .env.production --watch 60

        # Full API-side track for one vehicle over the last 6 hours:
        python local/spot_check_positions.py --env-file .env.production --asset 4980952 --hours 6

        # With ER comparison — add to the .env file (or export):
        #   ER_SITE_URL=https://<site>.pamdas.org
        #   ER_TOKEN=<api token>
        #   ER_PROVIDER_KEY=<provider key>   (optional but recommended)

        # Overnight monitor: fleet vs ER every 2 min, discrepancies to JSONL:
        python local/spot_check_positions.py --env-file .env.production \\
            --watch 120 --log er_compare.jsonl

        # Did Gundi drop fixes for one vehicle in the last 24 h?
        python local/spot_check_positions.py --env-file .env.production \\
            --asset 4980952 --hours 24

Timestamps are printed in UTC (what the API returns) — the portal may
render local time, so mind the offset when comparing.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.client import TracpointClient, aclose_client_cache  # noqa: E402

V7_WSDL = "http://www.terramarnetworks.net/v7/index.php?wsdl"
_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def _silence_noisy_loggers() -> None:
    logging.basicConfig(level=logging.ERROR)
    for name in ("app.services.zeep_cache", "zeep", "httpx", "urllib3"):
        logging.getLogger(name).setLevel(logging.ERROR)


def _load_env_file(path: Path) -> None:
    """Minimal .env loader — keeps the script dependency-free."""
    if not path.exists():
        sys.exit(f"env file not found: {path}")
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        sys.exit(f"missing required env var: {name}")
    return value


def _parse_ts(raw) -> datetime | None:
    try:
        return datetime.strptime(str(raw).strip(), _TS_FORMAT).replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _fmt_age(delta: timedelta) -> str:
    total = int(delta.total_seconds())
    if total < 0:
        return "future?!"
    days, rem = divmod(total, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)
    if days:
        return f"{days}d {hours:02}h"
    if hours:
        return f"{hours}h {minutes:02}m"
    return f"{minutes}m {seconds:02}s"


def _maps_link(pos: dict) -> str:
    lat, lon = pos.get("latitude"), pos.get("longitude")
    if lat is None or lon is None:
        return "-"
    return f"https://maps.google.com/?q={lat},{lon}"


# ---------------------------------------------------------------------------
# EarthRanger comparison layer (GUNDI-5543)
# ---------------------------------------------------------------------------

# Match a Tracpoint fix to an ER observation by timestamp when inbound_id
# isn't available; also the slack used when comparing latest-fix times.
_MATCH_SLACK_S = 2


def _parse_er_ts(raw) -> datetime | None:
    """Parse an ER ISO-8601 timestamp (with offset or Z) to aware UTC."""
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class ERAuthError(RuntimeError):
    pass


class ERClient:
    """Minimal read-only client for the ER v1.0 API (token auth).

    das wraps list responses as {"data": {"count", "next", "results": [...]}}
    (StandardResultsSetPagination); helpers below unwrap and follow pages.
    """

    def __init__(self, base_url: str, token: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        # No follow_redirects: this client sends a bearer token, and a
        # redirect could forward it to an unexpected host. ER API URLs
        # don't redirect; a wrong base URL should fail loudly instead.
        self._client = httpx.AsyncClient(
            timeout=timeout,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def _get(self, url: str, params: dict | None = None) -> dict:
        resp = await self._client.get(url, params=params)
        if resp.status_code in (401, 403):
            raise ERAuthError(
                f"ER returned {resp.status_code} for {url} — token invalid/expired "
                f"or missing permissions ({resp.text[:200]})"
            )
        resp.raise_for_status()
        return resp.json()

    async def _get_all_pages(self, path: str, params: dict) -> list[dict]:
        url = f"{self.base_url}{path}"
        results: list[dict] = []
        while url:
            body = (await self._get(url, params=params)).get("data")
            params = None  # the `next` URL already carries the query string
            if isinstance(body, list):  # non-paginated shape
                return results + body
            if not isinstance(body, dict):
                raise RuntimeError(f"unexpected ER response shape for {path}: {body!r:.200}")
            results.extend(body.get("results") or [])
            url = body.get("next")
        return results

    async def fetch_sources(self, provider_key: str | None) -> list[dict]:
        params = {"page_size": 500}
        if provider_key:
            params["provider_key"] = provider_key
        return await self._get_all_pages("/api/v1.0/sources", params)

    async def fetch_latest_er_ts(
        self, source_id: str, tp_ts: datetime, now: datetime,
    ) -> datetime | None:
        """Newest ER recorded_at for the source, without trusting server-side
        ordering (live sites returned the OLDEST row for ordering=-recorded_at,
        making ER look weeks stale — see PR #10 discussion).

        Query 1 asks the precise question "does ER have the newest Tracpoint
        fix (or newer)?" — a minutes-wide window for an active vehicle. Only
        when that is empty do progressively wider lookbacks quantify how far
        behind ER actually is.
        """
        async def window_max(since: datetime, until: datetime) -> datetime | None:
            obs = await self._get_all_pages(
                "/api/v1.0/observations",
                {
                    "source_id": source_id,
                    "since": since.isoformat(),
                    "until": until.isoformat(),
                    "page_size": 4000,
                },
            )
            stamps = [t for o in obs if (t := _parse_er_ts(o.get("recorded_at")))]
            return max(stamps) if stamps else None

        latest = await window_max(tp_ts - timedelta(minutes=1), now)
        if latest is not None:
            return latest
        for lookback in (timedelta(hours=2), timedelta(hours=26), timedelta(days=7)):
            latest = await window_max(now - lookback, now)
            if latest is not None:
                return latest
        return None

    async def fetch_observations_window(
        self, source_id: str, since: datetime, until: datetime,
    ) -> list[dict]:
        return await self._get_all_pages(
            "/api/v1.0/observations",
            {
                "source_id": source_id,
                "since": since.isoformat(),
                "until": until.isoformat(),
                "include_details": "true",
                "page_size": 4000,
            },
        )


async def build_source_map(
    er: ERClient, provider_key: str | None, fleet_asset_ids: set[int],
) -> dict[int, str]:
    """Map Tracpoint assetId -> ER source UUID via source manufacturer_id."""
    sources = await er.fetch_sources(provider_key)
    mapping: dict[int, str] = {}
    for src in sources:
        raw = str(src.get("manufacturer_id") or "").strip()
        if raw.isdigit():
            mapping[int(raw)] = str(src.get("id"))
    matched = fleet_asset_ids & mapping.keys()
    unmatched = fleet_asset_ids - mapping.keys()
    scope = f"provider_key={provider_key}" if provider_key else "all providers"
    print(f"\nER source mapping ({scope}): {len(sources)} sources, "
          f"{len(matched)}/{len(fleet_asset_ids)} fleet assets matched.")
    if unmatched:
        print(f"  unmatched assetIds (NO-MATCH below): {sorted(unmatched)}")
    if not matched:
        sys.exit("No fleet asset matched any ER source — wrong site, wrong provider "
                 "key, or the token lacks source-view permissions.")
    return mapping


# The pull_track_history backfill runs every 2 hours, so a healthy pipeline
# never leaves ER more than one backfill cycle behind the Tracpoint API.
_BACKFILL_ALLOWANCE = timedelta(hours=2)


def er_verdict(
    tp_ts: datetime,
    er_ts: datetime | None,
    now: datetime,
    tolerance: timedelta,
) -> str:
    """Classify one vehicle's pipeline state. Pure function — unit-testable.

    tp_ts: latest fix in the Tracpoint API.  er_ts: latest observation in ER
    (None if ER has nothing in the lookback window).
    """
    if er_ts is not None:
        behind = tp_ts - er_ts
        if behind.total_seconds() < -_MATCH_SLACK_S:
            return "TP-REGRESSED"  # ER ahead of the API: v7 served stale data
        if behind.total_seconds() <= _MATCH_SLACK_S:
            return "OK"
        if behind > _BACKFILL_ALLOWANCE + tolerance:
            # ER is more than a full backfill cycle behind — the pipeline is
            # stuck for this vehicle even if its newest fix is still in flight.
            return "LAGGING"
    # ER is somewhat behind the Tracpoint API. In flight, or actually stuck?
    if (now - tp_ts) <= tolerance:
        return "PENDING"
    return "LAGGING"


def diff_tracks(
    tp_positions: list[dict],
    er_observations: list[dict],
    now: datetime,
    tolerance: timedelta,
) -> dict:
    """Diff a Tracpoint track window against ER's observations for the source.

    Matches by inbound_id when the ER observation carries one
    (observation_details from include_details=true), else by recorded_at
    within +/-_MATCH_SLACK_S. Returns matched pairs, candidate drops
    (Tracpoint-only fixes older than the tolerance), in-flight fixes, and
    ER-only observations.
    """
    er_by_inbound: dict[int, dict] = {}
    er_by_ts: dict[int, list[dict]] = {}  # epoch-second buckets for slack matching
    for obs in er_observations:
        ts = _parse_er_ts(obs.get("recorded_at"))
        if ts is None:
            continue
        details = obs.get("observation_details") or {}
        inbound = details.get("inbound_id")
        if isinstance(inbound, int):
            er_by_inbound[inbound] = obs
        er_by_ts.setdefault(int(ts.timestamp()), []).append(obs)

    matched, candidate_drops, in_flight = [], [], []
    claimed: set[int] = set()
    for pos in tp_positions:
        ts = _parse_ts(pos.get("timestamp"))
        if ts is None:
            continue
        inbound = pos.get("inboundId")
        obs = er_by_inbound.get(inbound) if isinstance(inbound, int) else None
        if obs is None:
            # Timestamp fallback: each ER observation may satisfy only one
            # Tracpoint fix — two fixes in the same second must not both
            # match the same observation (that would hide a real drop).
            epoch = int(ts.timestamp())
            for probe in range(-_MATCH_SLACK_S, _MATCH_SLACK_S + 1):
                bucket = er_by_ts.get(epoch + probe)
                if bucket:
                    for i, candidate in enumerate(bucket):
                        if id(candidate) not in claimed:
                            obs = bucket.pop(i)
                            break
                if obs is not None:
                    break
        if obs is not None:
            claimed.add(id(obs))
            created = _parse_er_ts(obs.get("created_at"))
            delivery_s = (created - ts).total_seconds() if created else None
            matched.append({"tp": pos, "er": obs, "delivery_s": delivery_s})
        elif (now - ts) <= tolerance:
            in_flight.append(pos)
        else:
            candidate_drops.append(pos)

    er_only = [o for o in er_observations if id(o) not in claimed]
    return {
        "matched": matched,
        "candidate_drops": candidate_drops,
        "in_flight": in_flight,
        "er_only": er_only,
    }


def _print_fleet_table(
    positions: list[dict],
    max_age_days: int,
    previous: dict | None,
    er_results: dict | None = None,
) -> dict:
    """Print the per-asset latest-fix table; return {assetId: inboundId} for change tracking.

    er_results (optional): {assetId: {"verdict": str, "er_ts": datetime | None}}.
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max_age_days) if max_age_days > 0 else None

    rows, hidden = [], 0
    for pos in positions:
        ts = _parse_ts(pos.get("timestamp"))
        if ts is None:
            continue
        if cutoff and ts < cutoff:
            hidden += 1
            continue
        rows.append((ts, pos))
    rows.sort(key=lambda r: r[0], reverse=True)

    print(f"\n=== getAllPositions @ {now.strftime('%Y-%m-%d %H:%M:%S')} UTC — "
          f"{len(positions)} assets, {len(rows)} shown, {hidden} hidden (silent > {max_age_days}d) ===")
    er_cols = f"  {'ER latest (UTC)':19}  {'verdict':12}" if er_results is not None else ""
    header = (f"{'':4}{'assetId':>9}  {'name':20.20}  {'fix (UTC)':19}  {'age':>8}  "
              f"{'inboundId':>11}{er_cols}  {'event':12.12}  maps")
    print(header)
    print("-" * len(header))

    seen: dict = {}
    for ts, pos in rows:
        asset_id = pos.get("assetId")
        inbound = pos.get("inboundId")
        seen[asset_id] = inbound
        is_new = previous is not None and previous.get(asset_id) != inbound
        marker = "NEW " if is_new else "    "
        er_part = ""
        if er_results is not None:
            res = er_results.get(asset_id) or {}
            er_ts = res.get("er_ts")
            er_part = (f"  {er_ts.strftime(_TS_FORMAT) if er_ts else '-':19}  "
                       f"{res.get('verdict', '?'):12}")
        print(f"{marker}{asset_id!s:>9}  {(pos.get('assetDisplayName') or '-'):20.20}  "
              f"{ts.strftime(_TS_FORMAT):19}  {_fmt_age(now - ts):>8}  {inbound!s:>11}{er_part}  "
              f"{(pos.get('eventName') or ''):12.12}  {_maps_link(pos)}")
    return seen


async def _compare_fleet_against_er(
    er: ERClient,
    source_map: dict[int, str],
    positions: list[dict],
    max_age_days: int,
    tolerance: timedelta,
) -> dict:
    """One comparison cycle: {assetId: {verdict, er_ts, tp_ts, tp_inbound}}."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max_age_days) if max_age_days > 0 else None
    results: dict = {}
    for pos in positions:
        tp_ts = _parse_ts(pos.get("timestamp"))
        asset_id = pos.get("assetId")
        if tp_ts is None or asset_id is None or (cutoff and tp_ts < cutoff):
            continue
        source_id = source_map.get(asset_id)
        if source_id is None:
            results[asset_id] = {"verdict": "NO-MATCH", "er_ts": None,
                                 "tp_ts": tp_ts, "tp_inbound": pos.get("inboundId")}
            continue
        er_ts = await er.fetch_latest_er_ts(source_id, tp_ts, now)
        results[asset_id] = {
            "verdict": er_verdict(tp_ts, er_ts, now, tolerance),
            "er_ts": er_ts,
            "tp_ts": tp_ts,
            "tp_inbound": pos.get("inboundId"),
        }
    return results


def _append_jsonl(log_path: Path, er_results: dict) -> None:
    now = datetime.now(timezone.utc).isoformat()
    verdicts = [r["verdict"] for r in er_results.values()]
    records = [{
        "t": now,
        "type": "cycle",
        "compared": len(er_results),
        **{v.lower().replace("-", "_"): verdicts.count(v)
           for v in ("OK", "PENDING", "LAGGING", "TP-REGRESSED", "NO-MATCH")},
    }]
    for asset_id, r in sorted(er_results.items()):
        if r["verdict"] in ("OK", "PENDING"):
            continue
        records.append({
            "t": now,
            "type": "vehicle",
            "assetId": asset_id,
            "verdict": r["verdict"],
            "tp_ts": r["tp_ts"].isoformat(),
            "tp_inbound_id": r["tp_inbound"],
            "er_ts": r["er_ts"].isoformat() if r["er_ts"] else None,
            "lag_s": (r["tp_ts"] - r["er_ts"]).total_seconds() if r["er_ts"] else None,
        })
    with log_path.open("a") as fh:
        for rec in records:
            fh.write(json.dumps(rec) + "\n")


def _print_track(asset_id: int, positions: list[dict], hours: int) -> None:
    now = datetime.now(timezone.utc)
    print(f"\n=== getSinglePositions asset={asset_id} last {hours}h "
          f"(@ {now.strftime('%Y-%m-%d %H:%M:%S')} UTC) — {len(positions)} fixes ===")
    if not positions:
        print("No fixes in window. If the portal shows movement in this window,")
        print("the v7 web service is behind the portal for this vehicle.")
        return
    rows = sorted(positions, key=lambda p: str(p.get("timestamp")))
    prev_ts = None
    for pos in rows:
        ts = _parse_ts(pos.get("timestamp"))
        gap = ""
        if ts and prev_ts:
            silent = ts - prev_ts
            if silent > timedelta(minutes=30):
                gap = f"  <-- {_fmt_age(silent)} gap"
        prev_ts = ts or prev_ts
        print(f"  {pos.get('timestamp')!s:19}  inboundId={pos.get('inboundId')!s:>11}  "
              f"speed={pos.get('speed')!s:>5}  {_maps_link(pos)}{gap}")


def _print_track_diff(diff: dict, tolerance: timedelta) -> None:
    matched, drops, in_flight = diff["matched"], diff["candidate_drops"], diff["in_flight"]
    print(f"\n--- ER comparison: {len(matched)} matched, {len(drops)} candidate drops, "
          f"{len(in_flight)} in flight (< {int(tolerance.total_seconds() // 60)}m old), "
          f"{len(diff['er_only'])} ER-only ---")
    delivery = sorted(m["delivery_s"] for m in matched if m["delivery_s"] is not None)
    if delivery:
        p50 = delivery[len(delivery) // 2]
        print(f"Gundi->ER delivery latency (recorded_at -> ER created_at): "
              f"median {_fmt_age(timedelta(seconds=p50))}, "
              f"max {_fmt_age(timedelta(seconds=delivery[-1]))}")
    if drops:
        print("\nCANDIDATE DROPS — the Tracpoint API has these fixes, ER does not "
              "(cross-check the integration's 'Dropped N position(s)' log lines):")
        for pos in drops:
            print(f"  {pos.get('timestamp')!s:19}  inboundId={pos.get('inboundId')!s:>11}  "
                  f"{_maps_link(pos)}")
    if diff["er_only"]:
        print(f"\nER-only observations (in ER but not in this Tracpoint window — "
              f"usually earlier backfill of fixes the v7 API no longer returns): "
              f"{len(diff['er_only'])}")


async def _run(args, creds: dict) -> None:
    client = TracpointClient(wsdl_url=args.wsdl_url, **creds)
    er: ERClient | None = None
    if args.er_site_url and args.er_token:
        er = ERClient(args.er_site_url, args.er_token)
    tolerance = timedelta(minutes=args.tolerance_min)

    try:
        if args.asset:
            now = datetime.now(timezone.utc)
            start_dt = now - timedelta(hours=args.hours)
            positions = await client.fetch_positions_for_asset(
                asset_id=args.asset,
                start_timestamp=start_dt.strftime(_TS_FORMAT),
                end_timestamp=now.strftime(_TS_FORMAT),
            )
            _print_track(args.asset, positions, args.hours)
            if er is not None:
                source_map = await build_source_map(er, args.er_provider_key, {args.asset})
                er_obs = await er.fetch_observations_window(
                    source_map[args.asset], since=start_dt, until=now,
                )
                diff = diff_tracks(positions, er_obs, now, tolerance)
                _print_track_diff(diff, tolerance)
            return

        source_map: dict[int, str] = {}
        previous: dict | None = None
        while True:
            try:
                positions = await client.fetch_all_positions()
            except Exception as exc:
                if not args.watch:
                    raise  # one-shot mode should fail loudly
                # Watch mode must survive unattended overnight runs: log,
                # wait out the interval, try again next cycle.
                print(f"\nTracpoint fetch failed (retrying in {args.watch}s): {exc!r}")
                await asyncio.sleep(args.watch)
                continue
            er_results = None
            if er is not None:
                try:
                    if not source_map:
                        fleet_ids = {p.get("assetId") for p in positions
                                     if isinstance(p.get("assetId"), int)}
                        source_map = await build_source_map(er, args.er_provider_key, fleet_ids)
                    er_results = await _compare_fleet_against_er(
                        er, source_map, positions, args.max_age_days, tolerance,
                    )
                except ERAuthError as exc:
                    print(f"\nER AUTH ERROR (continuing without ER this cycle): {exc}")
                except (httpx.HTTPError, RuntimeError) as exc:
                    print(f"\nER fetch failed (continuing without ER this cycle): {exc!r}")
            previous = _print_fleet_table(positions, args.max_age_days, previous, er_results)
            if er_results and args.log:
                _append_jsonl(args.log, er_results)
            if not args.watch:
                return
            await asyncio.sleep(args.watch)
    finally:
        if er is not None:
            await er.aclose()
        await aclose_client_cache()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", type=Path, help="Path to a .env file with TRACPOINT_* vars.")
    parser.add_argument("--wsdl-url", default=V7_WSDL)
    parser.add_argument("--watch", type=int, metavar="SECONDS",
                        help="Re-poll getAllPositions every N seconds, marking changed fixes NEW.")
    parser.add_argument("--asset", type=int, metavar="ASSET_ID",
                        help="Query getSinglePositions for one asset instead of the fleet table.")
    parser.add_argument("--hours", type=int, default=6,
                        help="Window for --asset mode, in hours back from now (default: 6).")
    parser.add_argument("--max-age-days", type=int, default=30,
                        help="Hide assets whose latest fix is older than this many days; 0 shows all (default: 30).")
    parser.add_argument("--er-provider-key", default=None,
                        help="Scope the ER source lookup to one provider_key (or set ER_PROVIDER_KEY).")
    parser.add_argument("--tolerance-min", type=int, default=10,
                        help="Minutes of Gundi pipeline latency to allow before a missing fix counts as LAGGING (default: 10).")
    parser.add_argument("--log", type=Path, metavar="FILE",
                        help="Append per-cycle ER comparison results as JSONL (watch mode).")
    args = parser.parse_args()

    _silence_noisy_loggers()

    if args.env_file:
        _load_env_file(args.env_file)

    creds = {
        "company": _require_env("TRACPOINT_COMPANY"),
        "username": _require_env("TRACPOINT_USERNAME"),
        "password": _require_env("TRACPOINT_PASSWORD"),
    }

    # ER comparison is optional: enabled when both env vars are present.
    args.er_site_url = os.environ.get("ER_SITE_URL")
    args.er_token = os.environ.get("ER_TOKEN")
    if args.er_provider_key is None:
        args.er_provider_key = os.environ.get("ER_PROVIDER_KEY")
    if bool(args.er_site_url) != bool(args.er_token):
        sys.exit("Set both ER_SITE_URL and ER_TOKEN to enable the ER comparison (or neither).")
    if args.log and not args.er_site_url:
        sys.exit("--log records ER comparison results; set ER_SITE_URL and ER_TOKEN.")

    try:
        asyncio.run(_run(args, creds))
    except KeyboardInterrupt:
        print("\nstopped.")


if __name__ == "__main__":
    main()
