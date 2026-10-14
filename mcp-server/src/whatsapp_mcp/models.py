"""Pydantic models mirroring Go bridge REST API response shapes.

Selected message and text-send contracts are validated at runtime by the MCP
tools. Those models reject coercion and unknown fields so bridge/API drift
fails closed before data is returned to callers. Other models remain response
shape documentation for tools that have not yet adopted runtime validation.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Any, Literal, TypeVar

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)


ModelT = TypeVar("ModelT", bound=BaseModel)

_RFC3339 = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    r"(?:\.\d{1,9})?(?:Z|[+-]\d{2}:\d{2})$"
)


def _parse_bridge_datetime(value: Any) -> str:
    """Validate a Go RFC3339Nano timestamp without changing its text."""
    if not isinstance(value, str) or _RFC3339.fullmatch(value) is None:
        raise ValueError("bridge timestamp must be an RFC3339 string")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid bridge timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("bridge timestamp must include a timezone")
    return value


BridgeDatetime = Annotated[str, BeforeValidator(_parse_bridge_datetime)]


class StrictBridgeModel(BaseModel):
    """Base for runtime contracts with strict types and no unknown fields."""

    model_config = ConfigDict(strict=True, extra="forbid")


def validate_bridge_response(model: type[ModelT], data: Any) -> ModelT:
    """Validate one bridge payload without exposing response data in errors."""
    try:
        return model.model_validate(data)
    except ValidationError:
        raise RuntimeError(
            f"Invalid bridge response contract: {model.__name__}."
        ) from None


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


class MessageResponse(StrictBridgeModel):
    """A single stored WhatsApp message.

    Go type: ``MessageResponse``. Fields tagged ``omitempty`` in Go remain
    optional here; every field emitted unconditionally by Go is required.
    """

    id: str = Field(min_length=1)
    chat_jid: str = Field(min_length=1)
    sender: str
    sender_name: str
    content: str
    timestamp: BridgeDatetime
    is_from_me: bool
    media_type: str = Field(default="", min_length=1)
    filename: str = Field(default="", min_length=1)
    url: str = Field(default="", min_length=1)
    file_length: int = Field(default=0, gt=0)
    push_name: str = Field(default="", min_length=1)
    quoted_message_id: str = Field(default="", min_length=1)
    quoted_participant: str = Field(default="", min_length=1)
    snippet: str = Field(default="", min_length=1)
    transcription: str = Field(default="", min_length=1)
    transcribed_at: BridgeDatetime | None = None

    @model_validator(mode="before")
    @classmethod
    def reject_explicit_zero_omitempty(cls, value: Any) -> Any:
        """Reject values Go would omit instead of serializing explicitly."""
        if not isinstance(value, Mapping):
            return value
        empty_string_fields = {
            "media_type",
            "filename",
            "url",
            "push_name",
            "quoted_message_id",
            "quoted_participant",
            "snippet",
            "transcription",
        }
        if any(
            value.get(field) == ""
            for field in empty_string_fields
            if field in value
        ):
            raise ValueError("explicit empty omitempty field")
        if value.get("file_length", 1) == 0 and "file_length" in value:
            raise ValueError("explicit zero omitempty field")
        if "transcribed_at" in value and value["transcribed_at"] is None:
            raise ValueError("explicit null omitempty field")
        return value


class MessageResponseEnvelope(StrictBridgeModel):
    """Exact response from ``GET /api/messages/{id}``."""

    message: MessageResponse


class MessagesResponse(StrictBridgeModel):
    """Paginated list of messages.

    Go type: ``MessagesResponse``.
    """

    messages: list[MessageResponse]
    total: int = Field(ge=0)
    limit: int = Field(ge=0)
    offset: int = Field(ge=0)


class CheckResponse(StrictBridgeModel):
    """Response from ``GET /api/check``.

    Go type: ``CheckResponse``.
    """

    messages: list[MessageResponse]
    count: int = Field(ge=0)
    since: BridgeDatetime
    timestamp: BridgeDatetime


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
    messages: list[MessageResponse] = Field(default_factory=list)


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


class SendResponse(StrictBridgeModel):
    """Validated response from ``POST /api/send``.

    Go type: ``SendResponse``.
    """

    success: Literal[True]
    message_id: str = Field(min_length=1)


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
