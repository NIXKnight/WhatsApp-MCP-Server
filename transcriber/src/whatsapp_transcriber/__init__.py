"""WhatsApp transcription worker (L3 enrichment).

Polls the bridge-owned ``messages_media`` table for audio rows without a
transcription, re-downloads missing media via the bridge ``POST /api/download``
endpoint, converts ogg/opus to WAV, transcribes via a Whisper endpoint, and
writes the result back. The schema is owned by the bridge migrations; this
worker never issues DDL.
"""

__version__ = "0.1.0"
