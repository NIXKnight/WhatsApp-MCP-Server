"""WhatsApp transcription worker (L3 enrichment).

Polls the bridge-owned ``messages_media`` table for audio rows without a
transcription, re-downloads missing media via the bridge ``POST /api/download``
endpoint, posts the raw audio to a Whisper endpoint (which decodes ogg/opus
server-side via ``--convert``), and writes the result back. The schema is owned
by the bridge migrations; this worker never issues DDL.
"""

__version__ = "0.1.0"
