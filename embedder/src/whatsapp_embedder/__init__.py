"""WhatsApp embedding worker (L3 enrichment).

Polls the bridge-owned ``messages`` table for un-embedded text, encodes it with a
multilingual sentence-transformers model, and stores 384-dim vectors into the
``message_embeddings`` table. The schema is owned by the bridge migrations; this
worker never issues DDL.
"""

__version__ = "0.1.0"
