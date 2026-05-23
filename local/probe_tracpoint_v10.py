#!/usr/bin/env python3
"""Diagnostic probe for the Tracpoint v10 endpoint stall.

Background:
    Commit 4840ed2 switched the default WSDL from v7 to v10 based on a
    WSDL-level diff that looked drop-in compatible. In practice, the
    integration's cursor froze: SOAP calls kept returning OK, but
    `filter_new_positions()` in app/actions/handlers.py treated every
    record as already-seen, so observations stopped flowing to Gundi.
    Reverting the per-portal `wsdl_url` to v7 restored the feed.

    We do not yet know what differs in v10's wire output. This script
    calls `getAllPositions` against v7 and v10 with the same credentials
    and dumps both responses side-by-side so the difference can be
    spotted by eye (most likely candidates: timestamp string format,
    timestamp timezone, or inboundId type).

Usage:
    From the repo root, with the project venv active:

        # Credentials via env vars (recommended):
        TRACPOINT_COMPANY=acme \\
        TRACPOINT_USERNAME=foo \\
        TRACPOINT_PASSWORD=bar \\
            python local/probe_tracpoint_v10.py

        # Credentials from a .env file:
        python local/probe_tracpoint_v10.py --env-file .env.production

        # Probe one endpoint only:
        python local/probe_tracpoint_v10.py --only v10

The inbound envelope is printed RAW (unredacted) — it carries vehicle
positions and asset names. Outbound envelopes have credentials redacted.

Redis is not required: zeep's WSDL cache will emit warnings if Redis
isn't reachable and continue. Those warnings are suppressed below.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
from pathlib import Path

from lxml import etree

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.services.client import (  # noqa: E402
    TracpointClient,
    _histories,
    _redact_credentials,
    aclose_client_cache,
)

V7_WSDL = "http://www.terramarnetworks.net/v7/index.php?wsdl"
V10_WSDL = "https://www.terramarnetworks.net/v10/index.php?wsdl"


def _silence_noisy_loggers() -> None:
    # The Redis-backed WSDL cache logs WARNING on every miss when Redis
    # isn't reachable — fine, but it drowns out the actual probe output.
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


def _envelope(wsdl_url: str, direction: str) -> str | None:
    history = _histories.get(wsdl_url)
    if history is None:
        return None
    entry = history.last_sent if direction == "sent" else history.last_received
    if not entry:
        return None
    return etree.tostring(entry["envelope"], pretty_print=True).decode()


async def _probe(label: str, wsdl_url: str, creds: dict, n_records: int) -> None:
    print()
    print("=" * 78)
    print(f"  {label}: {wsdl_url}")
    print("=" * 78)

    client = TracpointClient(wsdl_url=wsdl_url, **creds)
    try:
        positions = await client.fetch_all_positions()
    except Exception as exc:
        print(f"\nFETCH FAILED: {exc!r}")
        # Even on failure, the outbound envelope may have been recorded.
        sent = _envelope(wsdl_url, "sent")
        if sent:
            print("\n--- OUTBOUND ENVELOPE (credentials redacted) ---")
            print(_redact_credentials(sent))
        return

    print(f"\nReturned {len(positions)} position records.")

    sent = _envelope(wsdl_url, "sent")
    if sent:
        print("\n--- OUTBOUND ENVELOPE (credentials redacted) ---")
        print(_redact_credentials(sent))

    received = _envelope(wsdl_url, "received")
    if received:
        print("\n--- INBOUND ENVELOPE (raw — may contain location data) ---")
        print(received)

    if not positions:
        return

    print(f"\n--- FIRST {min(n_records, len(positions))} RECORD(S) DESERIALIZED ---")
    for i, pos in enumerate(positions[:n_records]):
        print(f"\n[{i}] keys: {sorted(pos.keys())}")
        for k in sorted(pos.keys()):
            v = pos[k]
            print(f"    {k!s:24} = {v!r}  ({type(v).__name__})")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--env-file", type=Path, help="Path to a .env file with TRACPOINT_* vars.")
    parser.add_argument("--only", choices=["v7", "v10"], help="Probe only one endpoint (default: both).")
    parser.add_argument("--records", type=int, default=3, help="How many deserialized records to dump per endpoint (default: 3).")
    parser.add_argument("--v7-url", default=V7_WSDL)
    parser.add_argument("--v10-url", default=V10_WSDL)
    args = parser.parse_args()

    _silence_noisy_loggers()

    if args.env_file:
        _load_env_file(args.env_file)

    creds = {
        "company": _require_env("TRACPOINT_COMPANY"),
        "username": _require_env("TRACPOINT_USERNAME"),
        "password": _require_env("TRACPOINT_PASSWORD"),
    }

    async def run() -> None:
        try:
            if args.only in (None, "v7"):
                await _probe("v7", args.v7_url, creds, args.records)
            if args.only in (None, "v10"):
                await _probe("v10", args.v10_url, creds, args.records)
        finally:
            await aclose_client_cache()

    asyncio.run(run())


if __name__ == "__main__":
    main()
