"""Media-related MCP tools.

Provides two tools:

* :func:`send_media` — Send an image, video, audio, or document file.
* :func:`download_media` — Download the media attachment from a received message.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path  # used by _detect_media_type
from typing import TYPE_CHECKING, Literal

from fastmcp import Context, FastMCP

if TYPE_CHECKING:
    from whatsapp_mcp.bridge_client import BridgeClient

logger = logging.getLogger(__name__)

MediaType = Literal["image", "video", "audio", "document"]

_IMAGE_EXTS: frozenset[str] = frozenset({".jpg", ".jpeg", ".png", ".gif", ".webp"})
_VIDEO_EXTS: frozenset[str] = frozenset({".mp4", ".avi", ".mkv", ".mov"})
_AUDIO_EXTS: frozenset[str] = frozenset({".mp3", ".ogg", ".wav", ".m4a", ".opus"})


def _detect_media_type(file_path: str) -> MediaType:
    """Derive a :data:`MediaType` from a file extension.

    Falls back to ``"document"`` for unknown extensions.

    Args:
        file_path: Absolute or relative path to the file.

    Returns:
        One of ``"image"``, ``"video"``, ``"audio"``, or ``"document"``.
    """
    ext = Path(file_path).suffix.lower()
    if ext in _IMAGE_EXTS:
        return "image"
    if ext in _VIDEO_EXTS:
        return "video"
    if ext in _AUDIO_EXTS:
        return "audio"
    return "document"


def _translate_path(host_path: str) -> str:
    """Translate a host filesystem path to the container workspace path.

    When ``WORKSPACE_HOST_PATH`` is set, any file path starting with that
    prefix is rewritten to use ``WORKSPACE_DIR`` (the container mount point)
    instead.  Paths that do not match the prefix are returned unchanged.
    """
    host_prefix = os.getenv("WORKSPACE_HOST_PATH", "")
    container_dir = os.getenv("WORKSPACE_DIR", "")
    if host_prefix and container_dir and host_path.startswith(host_prefix):
        return host_path.replace(host_prefix, container_dir, 1)
    return host_path


def register_media_tools(mcp: FastMCP) -> None:
    """Register all media tools onto *mcp*.

    Args:
        mcp: The :class:`~fastmcp.FastMCP` server instance.
    """

    # ------------------------------------------------------------------
    # send_media
    # ------------------------------------------------------------------

    @mcp.tool
    async def send_media(
        ctx: Context,
        to: str,
        file_path: str,
        caption: str | None = None,
        media_type: MediaType | None = None,
        ptt: bool = False,
    ) -> str:
        """Send a media file to a WhatsApp contact or group.

        Supports images, videos, audio files, and arbitrary documents.
        The media type is auto-detected from the file extension when
        ``media_type`` is omitted:

        * ``.jpg`` / ``.jpeg`` / ``.png`` / ``.gif`` / ``.webp`` → ``image``
        * ``.mp4`` / ``.avi`` / ``.mkv`` / ``.mov`` → ``video``
        * ``.mp3`` / ``.ogg`` / ``.wav`` / ``.m4a`` / ``.opus`` → ``audio``
        * Anything else → ``document``

        To send a WhatsApp voice note (push-to-talk), set ``ptt=True`` and
        provide a file in OGG Opus format (``.ogg``).  The bridge will mark
        the audio with the PTT flag so it appears as a voice note in WhatsApp.

        Args:
            ctx: FastMCP context.
            to: Recipient JID.  For individuals: ``923001234567@s.whatsapp.net``
                or bare number ``923001234567``.  For groups:
                ``120363039783372408@g.us``.
            file_path: Absolute path to the media file.  Host paths under
                the workspace directory are automatically translated to
                container paths (e.g. ``/home/user/workspace/photo.jpg``
                becomes ``/workspace/photo.jpg``).
            caption: Optional caption text displayed below the media in
                WhatsApp.  Ignored for ``document`` and ``audio`` types.
            media_type: Explicit media type override.  One of ``"image"``,
                ``"video"``, ``"audio"``, or ``"document"``.  Auto-detected
                from the file extension when omitted.
            ptt: When ``True``, send audio as a WhatsApp voice note
                (push-to-talk).  The file must be in OGG Opus format.
                Ignored for non-audio types.

        Returns:
            JSON string with ``{"success": true, "message_id": "..."}`` on
            success.  Raises :class:`RuntimeError` when the file does not
            exist, the bridge is unavailable, or the bridge returns an error.
        """
        # Translate host paths to container workspace paths.
        file_path = _translate_path(file_path)

        # Validate file existence before making any network call.
        if not os.path.isfile(file_path):
            raise RuntimeError(
                f'File not found at path "{file_path}". '
                "Provide an absolute path to an existing, readable file."
            )

        resolved_type: MediaType = media_type or _detect_media_type(file_path)

        bridge: BridgeClient = ctx.lifespan_context["bridge"]

        # The Go bridge's POST /api/send/media endpoint expects multipart/form-data
        # with fields: to, media_type, caption (optional), ptt (optional "true"),
        # and the file upload in the "file" field.
        form_data: dict[str, Any] = {
            "to": to,
            "media_type": resolved_type,
        }
        if caption:
            form_data["caption"] = caption
        if ptt and resolved_type == "audio":
            form_data["ptt"] = "true"

        filename = os.path.basename(file_path)
        with open(file_path, "rb") as fh:
            files = {"file": (filename, fh, "application/octet-stream")}
            result = await bridge.post_multipart("/api/send/media", data=form_data, files=files)

        return json.dumps(result)

    # ------------------------------------------------------------------
    # download_media
    # ------------------------------------------------------------------

    @mcp.tool
    async def download_media(
        ctx: Context,
        message_id: str,
        chat_jid: str,
    ) -> str:
        """Download the media attachment from a received WhatsApp message.

        The bridge fetches the encrypted media from WhatsApp's CDN, decrypts
        it, and saves it to the bridge's configured media directory.  The
        local file path is returned so the caller can read or forward the
        file.

        Args:
            ctx: FastMCP context.
            message_id: The ``id`` field of the WhatsApp message that contains
                the media.  Obtain this from :func:`check_new_messages`,
                :func:`get_messages`, or :func:`get_unread_messages`.
            chat_jid: JID of the chat the message belongs to, e.g.
                ``120363039783372408@g.us`` for a group or
                ``923001234567@s.whatsapp.net`` for a direct chat.

        Returns:
            JSON string with ``{"file_path": "/abs/path/to/file",
            "media_type": "...", "file_size": N}`` on success.  Raises
            :class:`RuntimeError` when the message is not found, has no
            downloadable media, or the bridge returns an error.
        """
        bridge: BridgeClient = ctx.lifespan_context["bridge"]

        payload: dict = {
            "message_id": message_id,
            "chat_jid": chat_jid,
            "output_dir": os.getenv("WORKSPACE_DIR", "/workspace"),
        }
        result = await bridge.post("/api/download", json=payload)
        return json.dumps(result)
