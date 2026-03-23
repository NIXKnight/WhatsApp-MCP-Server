"""HTTP client for communicating with the Go WhatsApp bridge.

Wraps a single :class:`httpx.AsyncClient` instance shared across all MCP tool
calls for the lifetime of the server process.  GET requests are retried up to
three times on transient network errors; POST requests are never retried to
prevent duplicate sends.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

_BRIDGE_BASE_URL = os.getenv("BRIDGE_URL", "http://localhost:8080")
_HEALTH_ATTEMPTS = 5
_HEALTH_RETRY_DELAY_S = 2.0

_CONNECT_TIMEOUT = 5.0
_READ_TIMEOUT = 30.0
_WRITE_TIMEOUT = 30.0
_POOL_TIMEOUT = 5.0


class BridgeUnavailableError(RuntimeError):
    """Raised when the Go WhatsApp bridge cannot be reached."""


class BridgeClient:
    """Async HTTP client wrapping the Go WhatsApp bridge REST API.

    Create a single instance in the FastMCP lifespan and inject it via
    ``ctx.lifespan_context["bridge"]``.  Call :meth:`close` in the finally
    block to cleanly shut down the underlying connection pool.

    Example::

        client = BridgeClient()
        await client.verify_health()
        result = await client.get("/api/status")
        await client.post("/api/send", json={"to": "...", "text": "..."})
        await client.close()
    """

    def __init__(self, base_url: str = _BRIDGE_BASE_URL) -> None:
        self._base_url = base_url
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=httpx.Timeout(
                connect=_CONNECT_TIMEOUT,
                read=_READ_TIMEOUT,
                write=_WRITE_TIMEOUT,
                pool=_POOL_TIMEOUT,
            ),
        )

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def verify_health(self) -> None:
        """Check that the bridge is reachable before accepting tool calls.

        Polls ``GET /api/status`` up to :data:`_HEALTH_ATTEMPTS` times with
        a :data:`_HEALTH_RETRY_DELAY_S` second gap between attempts.

        Raises:
            BridgeUnavailableError: If the bridge is not reachable after all
                attempts are exhausted.
        """
        last_exc: Exception | None = None
        for attempt in range(1, _HEALTH_ATTEMPTS + 1):
            try:
                resp = await self._client.get("/api/status")
                resp.raise_for_status()
                logger.info("Bridge health check passed on attempt %d.", attempt)
                return
            except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
                last_exc = exc
                logger.warning(
                    "Bridge health check attempt %d/%d failed: %s",
                    attempt,
                    _HEALTH_ATTEMPTS,
                    exc,
                )
                if attempt < _HEALTH_ATTEMPTS:
                    await asyncio.sleep(_HEALTH_RETRY_DELAY_S)

        raise BridgeUnavailableError(
            f"Go WhatsApp bridge unreachable at {self._base_url} after "
            f"{_HEALTH_ATTEMPTS} attempts. Start the bridge first. "
            f"Last error: {last_exc}"
        )

    async def close(self) -> None:
        """Close the underlying httpx connection pool."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, max=5),
        retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException)),
        reraise=True,
    )
    async def _get_raw(self, path: str, params: dict) -> Any:
        """Execute a GET request against the bridge, retried by tenacity.

        This method must not catch :class:`httpx.ConnectError` or
        :class:`httpx.TimeoutException` — tenacity intercepts those for
        retry logic.  :class:`httpx.HTTPStatusError` is raised immediately
        so the public :meth:`get` wrapper can translate it to a
        :class:`RuntimeError`.

        Args:
            path: URL path relative to the bridge base URL.
            params: Pre-cleaned query parameters (no ``None`` values).

        Returns:
            Parsed JSON response body.
        """
        resp = await self._client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()

    async def get(self, path: str, **params: Any) -> Any:
        """Perform a GET request against the bridge, with tenacity retries.

        Only GET requests are retried — POSTs are never retried to avoid
        duplicate messages or downloads.

        Args:
            path: URL path relative to the bridge base URL (e.g. ``/api/status``).
            **params: Query string parameters forwarded to httpx as ``params=``.
                ``None`` values are stripped before sending.

        Returns:
            Parsed JSON response body (dict, list, or scalar).

        Raises:
            RuntimeError: On connection failure after all retries, on a
                non-2xx HTTP status code, or on a request timeout.
        """
        clean_params = {k: v for k, v in params.items() if v is not None}
        try:
            return await self._get_raw(path, clean_params)
        except httpx.ConnectError as exc:
            raise RuntimeError(
                f"Bridge unavailable at {self._base_url}: {exc}"
            ) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 503:
                raise RuntimeError("WhatsApp not connected.") from exc
            raise RuntimeError(
                f"Bridge HTTP {exc.response.status_code}: {exc.response.text}"
            ) from exc
        except httpx.TimeoutException as exc:
            raise RuntimeError(f"Bridge request timed out: {exc}") from exc

    async def post(self, path: str, json: dict[str, Any] | None = None) -> Any:
        """Perform a POST request against the bridge.  Never retried.

        Args:
            path: URL path relative to the bridge base URL (e.g. ``/api/send``).
            json: Request body serialised as JSON.

        Returns:
            Parsed JSON response body (dict, list, or scalar).

        Raises:
            RuntimeError: On connection failure or on a non-2xx HTTP status code.
        """
        try:
            resp = await self._client.post(path, json=json)
            resp.raise_for_status()
            return resp.json()
        except httpx.ConnectError as exc:
            raise RuntimeError(
                f"Bridge unavailable — could not connect to {self._base_url}{path}. "
                "Ensure the Go bridge is running."
            ) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 503:
                raise RuntimeError("WhatsApp not connected.") from exc
            raise RuntimeError(
                f"Bridge returned HTTP {exc.response.status_code} for POST {path}: "
                f"{exc.response.text}"
            ) from exc
        except httpx.TimeoutException as exc:
            raise RuntimeError(f"Bridge request timed out: {exc}") from exc

    async def post_multipart(self, path: str, data: dict[str, Any], files: dict[str, Any]) -> Any:
        """Perform a multipart/form-data POST request.  Never retried.

        Used by :func:`~whatsapp_mcp.tools.media.send_media` to upload media
        files alongside metadata fields.

        Args:
            path: URL path relative to the bridge base URL.
            data: Form fields (non-file).
            files: Files dict in the format expected by httpx.

        Returns:
            Parsed JSON response body.

        Raises:
            RuntimeError: On connection failure, non-2xx HTTP status, or
                request timeout.
        """
        try:
            resp = await self._client.post(path, data=data, files=files)
            resp.raise_for_status()
            return resp.json()
        except httpx.ConnectError as exc:
            raise RuntimeError(
                f"Bridge unavailable — could not connect to {self._base_url}{path}. "
                "Ensure the Go bridge is running."
            ) from exc
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 503:
                raise RuntimeError("WhatsApp not connected.") from exc
            raise RuntimeError(
                f"Bridge returned HTTP {exc.response.status_code} for POST {path}: "
                f"{exc.response.text}"
            ) from exc
        except httpx.TimeoutException as exc:
            raise RuntimeError(f"Bridge request timed out: {exc}") from exc
