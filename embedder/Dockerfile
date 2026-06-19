FROM python:3.13-slim AS builder

RUN pip install --no-cache-dir uv

WORKDIR /build
COPY pyproject.toml ./
COPY src/ src/

# No lockfile is committed; resolve at build time.
RUN uv sync --no-dev

FROM python:3.13-slim

RUN adduser --disabled-password --uid 1000 bridge

WORKDIR /app
COPY --from=builder /build/.venv /app/.venv
COPY --from=builder /build/src /app/src

ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONPATH="/app/src"
# Pre-cache the model into the image layer so the first poll does not block on a
# network download. HF caches under /home/bridge/.cache.
ENV HF_HOME="/home/bridge/.cache/huggingface"
ENV DATABASE_URL=""

USER bridge

ENTRYPOINT ["python", "-m", "whatsapp_embedder"]
