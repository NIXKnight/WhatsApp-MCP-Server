"""Pydantic models mirroring Go bridge REST API response shapes.

These models document the exact JSON returned by the Go bridge.  Field names
and types are derived directly from ``api/types.go`` in the Go bridge source.
All optional bridge fields use ``default=None`` or an appropriate default so
that partial responses (e.g. omitempty fields absent from the JSON) do not
raise validation errors.

None of these models are used by the tool implementations at runtime — tools
call ``json.dumps(result)`` directly on the raw bridge response.  The models
serve as authoritative documentation and can be used for type-safe
deserialization in future tool iterations.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Shared error envelope
# ---------------------------------------------------------------------------


class BridgeError(BaseModel):
    """Standard error envelope returned by the bridge on non-2xx responses."""

    error: str
    code: str = ""


# ---------------------------------------------------------------------------
# Status  —  GET /api/status
# ---------------------------------------------------------------------------


class BridgeStatus(BaseModel):
    """Response from ``GET /api/status``.

    Go type: ``StatusResponse``.
    """

    state: str
    is_connected: bool = False
    uptime: str = ""
    message_count: int = 0
    chat_count: int = 0
    started_at: datetime | None = None


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


class Message(BaseModel):
    """A single stored WhatsApp message.

    Go type: ``MessageResponse``.
    """

    id: str
    chat_jid: str
    sender: str = ""
    sender_name: str = ""
    content: str = ""
    timestamp: datetime | None = None
    is_from_me: bool = False
    media_type: str = ""
    filename: str = ""
    url: str = ""
    file_length: int = 0
    push_name: str = ""
    quoted_message_id: str = ""
    quoted_participant: str = ""


class MessagesResponse(BaseModel):
    """Paginated list of messages.

    Go type: ``MessagesResponse``.
    """

    messages: list[Message] = Field(default_factory=list)
    total: int = 0
    limit: int = 0
    offset: int = 0


class CheckResponse(BaseModel):
    """Response from ``GET /api/check``.

    Go type: ``CheckResponse``.
    """

    messages: list[Message] = Field(default_factory=list)
    count: int = 0
    since: datetime | None = None
    timestamp: datetime | None = None


# ---------------------------------------------------------------------------
# Chats
# ---------------------------------------------------------------------------


class Chat(BaseModel):
    """A single WhatsApp chat.

    Go type: ``ChatResponse``.
    """

    jid: str
    name: str = ""
    is_group: bool = False
    unread_count: int = 0
    last_message_time: datetime | None = None
    last_message_preview: str = ""


class ChatsResponse(BaseModel):
    """Paginated list of chats.

    Go type: ``ChatsResponse``.
    """

    chats: list[Chat] = Field(default_factory=list)
    total: int = 0
    limit: int = 0
    offset: int = 0


class UnreadChatEntry(BaseModel):
    """A single entry in the unread chats response.

    Go type: ``UnreadChatEntry``.
    """

    chat: Chat
    messages: list[Message] = Field(default_factory=list)


class UnreadResponse(BaseModel):
    """Response from ``GET /api/unread`` (structured form, no flat param).

    Go type: ``UnreadResponse``.
    """

    chats: list[UnreadChatEntry] = Field(default_factory=list)
    total: int = 0


class UnreadFlatMessage(BaseModel):
    """A single entry in the flat unread messages list.

    Go type: ``UnreadFlatMessage``.
    Field names use Go's camelCase JSON tags exactly.
    """

    chatJid: str = ""
    chatName: str = ""
    isGroup: bool = False
    messageId: str = ""
    participant: str = ""
    senderName: str = ""
    text: str = ""
    timestamp: int = 0


class UnreadFlatResponse(BaseModel):
    """Response from ``GET /api/unread?flat=true``.

    Go type: ``UnreadFlatResponse``.
    """

    totalUnread: int = 0
    messages: list[UnreadFlatMessage] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Contacts
# ---------------------------------------------------------------------------


class Contact(BaseModel):
    """A single WhatsApp contact.

    Go type: ``ContactResponse``.
    """

    jid: str
    name: str = ""
    notify: str = ""
    phone: str = ""


class ContactsResponse(BaseModel):
    """List of contacts.

    Go type: ``ContactsResponse``.
    """

    contacts: list[Contact] = Field(default_factory=list)
    total: int = 0


# ---------------------------------------------------------------------------
# Groups
# ---------------------------------------------------------------------------


class GroupParticipant(BaseModel):
    """A single participant in a WhatsApp group.

    Go type: ``GroupParticipant``.
    """

    jid: str
    is_admin: bool = False
    is_super_admin: bool = False


class Group(BaseModel):
    """A WhatsApp group chat, including participant list when fetched individually.

    Go type: ``GroupResponse`` (embeds ``ChatResponse`` plus ``participants``).
    """

    jid: str
    name: str = ""
    is_group: bool = True
    unread_count: int = 0
    last_message_time: datetime | None = None
    last_message_preview: str = ""
    participants: list[GroupParticipant] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Send responses
# ---------------------------------------------------------------------------


class SendResponse(BaseModel):
    """Response from ``POST /api/send``.

    Go type: ``SendResponse``.
    """

    success: bool = False
    message_id: str = ""


class SendMediaResponse(BaseModel):
    """Response from ``POST /api/send/media``.

    Go type: ``SendMediaResponse``.
    """

    success: bool = False
    message_id: str = ""


# ---------------------------------------------------------------------------
# Media / download
# ---------------------------------------------------------------------------


class DownloadResponse(BaseModel):
    """Response from ``POST /api/download``.

    Go type: ``DownloadResponse``.
    """

    file_path: str
    media_type: str = ""
    file_size: int = 0


# ---------------------------------------------------------------------------
# Generic passthrough helper
# ---------------------------------------------------------------------------


def raw_or_model(data: Any) -> Any:
    """Return ``data`` as-is.

    Helper for tools that JSON-serialise the bridge response without mapping
    it to a typed model.  Keeps the code explicit about intent.
    """
    return data
