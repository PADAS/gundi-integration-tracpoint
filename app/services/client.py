import logging
import os
import re
from typing import Any

import httpx
from lxml import etree
from zeep import AsyncClient
from zeep.helpers import serialize_object
from zeep.plugins import HistoryPlugin
from zeep.settings import Settings
from zeep.transports import AsyncTransport

from app.services.zeep_cache import get_default_cache

logger = logging.getLogger(__name__)

# Set DEBUG_SOAP_ENVELOPES=1 to log the SOAP envelope when Tracpoint returns
# a non-OK status. Useful when diagnosing auth or wire-format issues. The
# envelope is scrubbed of credentials before logging (see _redact_credentials).
DEBUG_SOAP_ENVELOPES = os.environ.get("DEBUG_SOAP_ENVELOPES", "").lower() in ("1", "true", "yes")

# Matches the credential elements zeep emits inside the SOAP body (no
# namespace prefix in Tracpoint's RPC/encoded style). Captures the open
# tag and close tag so the inner value can be replaced with a placeholder
# without touching attributes or whitespace.
_CREDENTIAL_ELEMENTS_RE = re.compile(
    r"(<(userCompany|userName|userPassword)>)[^<]*(</\2>)"
)


def _redact_credentials(envelope_xml: str) -> str:
    """Replace credential values in a SOAP envelope so they don't leak into logs."""
    return _CREDENTIAL_ELEMENTS_RE.sub(r"\1***REDACTED***\3", envelope_xml)

# Tracpoint SOAP service — Terramar Networks v7
# Namespace: http://www.terramarnetworks.net/v7
# Style: RPC/encoded (SOAP 1.1)
# Default endpoint: http://www.terramarnetworks.net/v7/index.php
#
# All operations require: userCompany, userName, userPassword in the SOAP body.


# Process-wide cache of built AsyncClients, keyed by WSDL URL. Building an
# AsyncClient parses the WSDL and every referenced XSD; once cached, the
# same client is reused across every action invocation on the same Cloud
# Run instance. The HistoryPlugin is shared per WSDL URL — in production
# this integration is single-tenant so there is effectively no contention,
# and the DEBUG_SOAP_ENVELOPES dump only reads the most recent envelope.
_async_clients: dict[str, AsyncClient] = {}
_histories: dict[str, HistoryPlugin] = {}
# Underlying httpx clients owned by the AsyncClient cache. Tracked here so we
# can close their connection pools at process shutdown (see aclose_client_cache).
_owned_httpx_clients: list[httpx.AsyncClient | httpx.Client] = []


def _build_async_client(wsdl_url: str, timeout: float) -> tuple[AsyncClient, HistoryPlugin]:
    """Construct a zeep AsyncClient backed by the Redis WSDL cache.

    strict=False tolerates Tracpoint's RPC/encoded SOAP style.
    follow_redirects=True is required because the WSDL references
    http://schemas.xmlsoap.org/soap/encoding/, which 307-redirects to https,
    and httpx does not follow redirects by default.
    """
    history = HistoryPlugin()
    soap_client = httpx.AsyncClient(timeout=timeout, follow_redirects=True)
    wsdl_client = httpx.Client(timeout=timeout, follow_redirects=True)
    _owned_httpx_clients.append(soap_client)
    _owned_httpx_clients.append(wsdl_client)
    transport = AsyncTransport(
        client=soap_client,
        wsdl_client=wsdl_client,
        cache=get_default_cache(),
    )
    settings_obj = Settings(strict=False, xml_huge_tree=True)
    client = AsyncClient(
        wsdl=wsdl_url,
        transport=transport,
        settings=settings_obj,
        plugins=[history],
    )
    return client, history


def reset_client_cache() -> None:
    """Drop the process-wide AsyncClient cache. Intended for tests.

    Does NOT close the underlying httpx clients — tests that create real
    sockets should use `aclose_client_cache()` instead. Sync callers that
    just want to forget cached AsyncClients (e.g., to force a rebuild) can
    use this without going async.
    """
    _async_clients.clear()
    _histories.clear()


async def aclose_client_cache() -> None:
    """Close all httpx clients owned by the AsyncClient cache and forget them.

    Call from the FastAPI lifespan shutdown hook so connection pools don't
    leak across process restarts in environments that reuse the Python
    runtime (some test harnesses, hot-reload servers, etc.). In Cloud Run
    the OS reclaims the sockets when the instance exits regardless.
    """
    for client in _owned_httpx_clients:
        try:
            if isinstance(client, httpx.AsyncClient):
                await client.aclose()
            else:
                client.close()
        except Exception as exc:
            logger.warning("Failed to close httpx client during shutdown: %s", exc)
    _owned_httpx_clients.clear()
    _async_clients.clear()
    _histories.clear()


class TracpointClient:
    """
    Async SOAP client for the Tracpoint (Terramar Networks v7) web service.

    Credentials are passed as parameters inside every SOAP request body —
    there is no session/token. The WSDL URL is stored per-integration so
    different Tracpoint deployments can be pointed at independently.

    The expensive zeep AsyncClient (parsed WSDL + XSD graph) is cached at
    module scope keyed by WSDL URL, so one Cloud Run instance only pays
    that cost once per WSDL even across many action invocations.
    """

    def __init__(self, wsdl_url: str, company: str, username: str, password: str, timeout: float = 30.0):
        self.wsdl_url = wsdl_url
        self.company = company
        self.username = username
        self.password = password
        self.timeout = timeout

    def _get_client(self) -> AsyncClient:
        client = _async_clients.get(self.wsdl_url)
        if client is None:
            client, history = _build_async_client(self.wsdl_url, self.timeout)
            _async_clients[self.wsdl_url] = client
            _histories[self.wsdl_url] = history
        return client

    @property
    def _history(self) -> HistoryPlugin | None:
        return _histories.get(self.wsdl_url)

    def _creds(self) -> dict:
        """Shared credential kwargs passed to every SOAP operation."""
        return {
            "userCompany": self.company,
            "userName": self.username,
            "userPassword": self.password,
        }

    def _check_status(self, package: Any, operation: str) -> None:
        """Raise if the Tracpoint response status is not OK."""
        status = getattr(package, "status", None)
        if status is None:
            return
        code = getattr(status, "code", None)
        description = getattr(status, "description", None)

        if code != "OK":
            if DEBUG_SOAP_ENVELOPES and self._history and self._history.last_sent:
                sent = etree.tostring(self._history.last_sent["envelope"], pretty_print=True).decode()
                logger.error("Tracpoint SOAP envelope sent:\n%s", _redact_credentials(sent))
            raise RuntimeError(
                f"Tracpoint {operation} returned status {code}: {description}"
            )

    async def test_connection(self) -> list[dict[str, Any]]:
        """
        Verify credentials by calling getAllAssets.
        A LOGIN_FAILED status raises RuntimeError; success returns the asset list.
        Called by action_auth.
        """
        client = self._get_client()
        result = await client.service.getAllAssets(**self._creds())
        self._check_status(result, "getAllAssets")
        assets = getattr(result, "assets", None)
        return serialize_object(assets) or []

    async def fetch_all_assets(self) -> list[dict[str, Any]]:
        """
        Return all assets registered in Tracpoint.
        Useful for building an asset ID → metadata lookup before fetching positions.
        """
        client = self._get_client()
        result = await client.service.getAllAssets(**self._creds())
        self._check_status(result, "getAllAssets")
        assets = getattr(result, "assets", None)
        return serialize_object(assets) or []

    async def fetch_all_positions(self) -> list[dict[str, Any]]:
        """
        Return the most recent position for every asset.

        This is the **primary** fetch method in this integration's hot loop —
        `action_pull_observations` calls it every cycle, and client-side dedup
        (`filter_new_positions` in `app/actions/handlers.py`) discards any
        positions whose `(timestamp, inboundId)` is not strictly greater than
        the previously stored high-water mark. Trade-off: if a tracker reports
        multiple times inside one polling window we capture only the most
        recent fix.

        Use `fetch_positions_for_asset()` only if you need higher-resolution
        per-asset history within an explicit time range.
        """
        client = self._get_client()
        result = await client.service.getAllPositions(**self._creds())
        self._check_status(result, "getAllPositions")
        positions = getattr(result, "positions", None)
        return serialize_object(positions) or []

    async def fetch_positions_for_asset(
        self,
        asset_id: int,
        start_timestamp: str,
        end_timestamp: str,
    ) -> list[dict[str, Any]]:
        """
        Return all positions for a single asset within a time range.

        Args:
            asset_id:        Tracpoint assetId integer.
            start_timestamp: Start of the range (format: "YYYY-MM-DD HH:MM:SS").
            end_timestamp:   End of the range (format: "YYYY-MM-DD HH:MM:SS").

        Returns:
            List of serialized Position dicts, or [] if no data / NO_POSITION_DATA status.
        """
        client = self._get_client()
        result = await client.service.getSinglePositions(
            **self._creds(),
            assetId=asset_id,
            startTimestamp=start_timestamp,
            endTimestamp=end_timestamp,
        )
        # NO_POSITION_DATA is a normal (non-error) status for assets with no data in range
        status = getattr(result, "status", None)
        if status and getattr(status, "description", None) == "NO_POSITION_DATA":
            return []
        self._check_status(result, "getSinglePositions")
        positions = getattr(result, "positions", None)
        return serialize_object(positions) or []

    async def fetch_event_types(self) -> list[dict[str, Any]]:
        """
        Return the Tracpoint event type lookup table (eventId → name).
        These are labels applied to Position records (e.g. "Speeding", "Geofence Entry").
        Useful for building a lookup dict to enrich event records.
        """
        client = self._get_client()
        result = await client.service.getEvents(**self._creds())
        self._check_status(result, "getEvents")
        events = getattr(result, "events", None)
        return serialize_object(events) or []
