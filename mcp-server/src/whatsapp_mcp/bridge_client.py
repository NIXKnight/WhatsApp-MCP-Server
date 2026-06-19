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
import random
import time
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

# Media analysis (frame sampling + audio transcription) on the bridge can run
# for up to ~3 minutes; the default 30s read timeout would abort it.  Applied
# per-call by :meth:`BridgeClient.analyze_media`.
_ANALYZE_READ_TIMEOUT = 200.0


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
        self._send_lock = asyncio.Lock()
        self._last_send_time: float = 0.0

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

    async def _throttle_send(self) -> None:
        """Enforce a randomized 2-7 second gap between send operations.

        Serialises concurrent sends through a lock so that even parallel
        tool calls respect the minimum inter-send delay.
        """
        async with self._send_lock:
            elapsed = time.monotonic() - self._last_send_time
            min_gap = random.uniform(2.0, 7.0)
            if elapsed < min_gap:
                await asyncio.sleep(min_gap - elapsed)
            self._last_send_time = time.monotonic()

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

    async def _post_raw(
        self,
        path: str,
        json: dict[str, Any] | None = None,
        timeout: httpx.Timeout | None = None,
    ) -> Any:
        """Execute a POST without the inter-send throttle.  Never retried.

        Shared request + error-translation core for both :meth:`post` (which
        wraps this with :meth:`_throttle_send` for outbound sends) and the
        read/telemetry POST endpoints (search, trigger checks, telemetry) that
        must not incur the 2-7 second anti-ban delay.

        Args:
            path: URL path relative to the bridge base URL.
            json: Request body serialised as JSON.
            timeout: Optional per-request timeout override.  When ``None`` the
                client's default timeout applies.  Used by long-running
                endpoints (e.g. media analysis) that exceed the default read
                timeout.

        Returns:
            Parsed JSON response body (dict, list, or scalar).

        Raises:
            RuntimeError: On connection failure, non-2xx HTTP status, or
                request timeout.  503 is translated to "WhatsApp not connected.".
        """
        try:
            if timeout is not None:
                resp = await self._client.post(path, json=json, timeout=timeout)
            else:
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

    async def post(self, path: str, json: dict[str, Any] | None = None) -> Any:
        """Perform a throttled POST against the bridge.  Never retried.

        Enforces the randomized 2-7 second inter-send delay before issuing the
        request — used for outbound message/media sends and message mutations
        (reaction, edit, revoke) where WhatsApp penalises bursts.  Read-only and
        telemetry POSTs bypass the throttle via :meth:`_post_raw`.

        Args:
            path: URL path relative to the bridge base URL (e.g. ``/api/send``).
            json: Request body serialised as JSON.

        Returns:
            Parsed JSON response body (dict, list, or scalar).

        Raises:
            RuntimeError: On connection failure or on a non-2xx HTTP status code.
        """
        await self._throttle_send()
        return await self._post_raw(path, json=json)

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
        await self._throttle_send()
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

    # ------------------------------------------------------------------
    # Typed endpoint wrappers
    # ------------------------------------------------------------------

    async def search(
        self,
        query: str,
        chat_jid: str | None = None,
        sender: str | None = None,
        limit: int = 20,
    ) -> Any:
        """Hybrid (RRF) message search via ``POST /api/search``.

        Routes through the bridge's hybrid search endpoint (full-text +
        optional vector RRF), never a direct database connection.  No embedding
        is supplied, so the bridge performs a full-text/lexical search.

        Args:
            query: Natural-language / keyword search string.  Required.
            chat_jid: Optional JID to restrict the search to a single chat.
            sender: Optional sender JID filter.  Forwarded to the bridge for
                forward-compatibility; the bridge ignores fields it does not
                yet support.
            limit: Maximum results (bridge clamps to 1-100, default 20).

        Returns:
            Parsed JSON: ``{"results": [...], "total": N}`` where each result
            has ``id``, ``chat_jid``, ``content``, ``timestamp``,
            ``sender_name``, ``score``, ``snippet``, ``match_type``.
        """
        body: dict[str, Any] = {"query": query, "limit": limit}
        if chat_jid:
            body["chat_jid"] = chat_jid
        if sender:
            body["sender"] = sender
        return await self._post_raw("/api/search", json=body)

    async def send_reaction(
        self,
        chat_jid: str,
        message_id: str,
        emoji: str,
        sender: str | None = None,
    ) -> Any:
        """React to a message via ``POST /api/send/reaction`` (throttled send).

        Args:
            chat_jid: Chat containing the target message.
            message_id: ID of the message being reacted to.
            emoji: Reaction emoji.  An empty string removes a prior reaction.
            sender: Author JID of the target message.  Omit for one's own
                message.

        Returns:
            Parsed JSON: ``{"success": true, "message_id": "..."}``.
        """
        body: dict[str, Any] = {
            "chat_jid": chat_jid,
            "message_id": message_id,
            "emoji": emoji,
        }
        if sender:
            body["sender"] = sender
        return await self.post("/api/send/reaction", json=body)

    async def edit_message(
        self,
        chat_jid: str,
        message_id: str,
        new_text: str,
    ) -> Any:
        """Edit a previously sent message via ``POST /api/send/edit`` (throttled).

        Only messages sent by this account can be edited.

        Args:
            chat_jid: Chat containing the message to edit.
            message_id: ID of the message to edit.
            new_text: Replacement message body.

        Returns:
            Parsed JSON: ``{"success": true, "message_id": "..."}``.
        """
        body: dict[str, Any] = {
            "chat_jid": chat_jid,
            "message_id": message_id,
            "new_text": new_text,
        }
        return await self.post("/api/send/edit", json=body)

    async def revoke_message(
        self,
        chat_jid: str,
        message_id: str,
        sender: str | None = None,
    ) -> Any:
        """Revoke (delete for everyone) a message via ``POST /api/send/revoke``.

        Throttled send route.

        Args:
            chat_jid: Chat containing the message to revoke.
            message_id: ID of the message to revoke.
            sender: Author JID of the target message.  Omit to revoke one's own
                message; supply it when a group admin revokes another member's
                message.

        Returns:
            Parsed JSON: ``{"success": true, "message_id": "..."}``.
        """
        body: dict[str, Any] = {
            "chat_jid": chat_jid,
            "message_id": message_id,
        }
        if sender:
            body["sender"] = sender
        return await self.post("/api/send/revoke", json=body)

    async def check_triggers(
        self,
        jids: list[str],
        mention_jid: str | None = None,
        sender_jids: list[str] | None = None,
        limit: int = 100,
        dry_run: bool = False,
    ) -> Any:
        """Batch trigger check via ``POST /api/check/triggers``.

        Not a send route — bypasses the inter-send throttle.  Returns, per JID,
        the inbound messages since that chat's server-side watermark and (unless
        ``dry_run``) advances the watermark.

        Args:
            jids: Chats to check (required, non-empty).
            mention_jid: When set, keep only messages mentioning this JID.
            sender_jids: When non-empty, keep only messages from these senders.
            limit: Maximum messages per chat (default 100).
            dry_run: When ``True``, report unseen messages without advancing
                watermarks.

        Returns:
            Parsed JSON: ``{"total": N, "groups": {jid: {"count": N,
            "messages": [...]}}}``.
        """
        filters: dict[str, Any] = {}
        if mention_jid:
            filters["mention_jid"] = mention_jid
        if sender_jids:
            filters["sender_jids"] = sender_jids

        body: dict[str, Any] = {"jids": jids, "limit": limit, "filters": filters}
        if dry_run:
            body["dry_run"] = True
        return await self._post_raw("/api/check/triggers", json=body)

    async def analyze_media(self, chat_jid: str, message_id: str) -> Any:
        """Analyze a message's media via ``POST /api/media/analyze``.

        Triggers bridge-side media analysis: the bridge samples video frames
        and transcribes the audio track, returning the on-disk frame image
        paths and the transcript.  Not a send route — bypasses the inter-send
        throttle via :meth:`_post_raw` and is never retried.

        Analysis can run for up to ~3 minutes, so a longer per-call read
        timeout (:data:`_ANALYZE_READ_TIMEOUT`) is applied; the connect, write,
        and pool timeouts keep their defaults.

        Args:
            chat_jid: JID of the chat the message belongs to.
            message_id: ID of the message whose media is analyzed.

        Returns:
            Parsed JSON: ``{"frame_paths": [...], "transcription": "...",
            "duration": N, "frame_count": N}``.
        """
        return await self._post_raw(
            "/api/media/analyze",
            json={"chat_jid": chat_jid, "message_id": message_id},
            timeout=httpx.Timeout(
                connect=_CONNECT_TIMEOUT,
                read=_ANALYZE_READ_TIMEOUT,
                write=_WRITE_TIMEOUT,
                pool=_POOL_TIMEOUT,
            ),
        )

    async def record_tool_call(
        self,
        tool_name: str,
        duration_ms: int,
        success: bool,
        error_msg: str = "",
    ) -> None:
        """Record one tool invocation via ``POST /api/telemetry/tool``.

        Fire-and-forget: this method **never** raises.  Any transport, HTTP, or
        serialisation error is swallowed so that telemetry can never affect a
        tool's result or surface an error to the caller.  Bypasses the
        inter-send throttle.

        Args:
            tool_name: Name of the invoked MCP tool.
            duration_ms: Wall-clock duration of the tool call in milliseconds.
            success: Whether the tool call completed without raising.
            error_msg: Error text when ``success`` is ``False``; empty otherwise.
        """
        try:
            await self._post_raw(
                "/api/telemetry/tool",
                json={
                    "tool_name": tool_name,
                    "duration_ms": duration_ms,
                    "success": success,
                    "error_msg": error_msg,
                },
            )
        except Exception as exc:  # noqa: BLE001 — telemetry must never propagate.
            logger.debug("Telemetry record_tool_call failed (ignored): %s", exc)
