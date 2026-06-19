FROM python:3.13-slim AS builder

RUN pip install --no-cache-dir uv

WORKDIR /build
COPY pyproject.toml ./
COPY src/ src/

# No lockfile is committed; resolve at build time.
RUN uv sync --no-dev

FROM python:3.13-slim

# ffmpeg (+ opus-tools for opusdec) for ogg/opus -> WAV conversion that
# whisper.cpp requires. The worker probes opusdec first, then ffmpeg.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg opus-tools \
    && rm -rf /var/lib/apt/lists/*

RUN adduser --disabled-password --uid 1000 bridge

WORKDIR /app
COPY --from=builder /build/.venv /app/.venv
COPY --from=builder /build/src /app/src

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"
ENV DATABASE_URL=""
ENV BRIDGE_URL="http://bridge:8080"
ENV WHISPER_URL="http://127.0.0.1:8443"

# Auxiliary on-demand /analyze + /health HTTP server (video keyframes + audio
# transcript). Loopback by default; override ANALYZER_HTTP_ADDR to publish.
EXPOSE 8500

USER bridge

ENTRYPOINT ["python", "-m", "whatsapp_transcriber"]
