FROM python:3.13-slim AS builder

WORKDIR /app

# Install uv via the official standalone installer
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl ca-certificates && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    apt-get purge -y curl && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/root/.local/bin:$PATH"

# Copy pyproject.toml first for layer caching
COPY pyproject.toml ./

# Copy source code
COPY src/ src/

# Install dependencies and the project itself (no lock file)
RUN uv sync


FROM python:3.13-slim

WORKDIR /app

# Create a non-root user
RUN groupadd --system app && \
    useradd --system --gid app --create-home app

# Copy the entire virtual environment and project from the builder
COPY --from=builder /app /app

# Install uv in the final image (needed for `uv run`)
COPY --from=builder /root/.local/bin/uv /usr/local/bin/uv
COPY --from=builder /root/.local/bin/uvx /usr/local/bin/uvx

# Make the app user own the working directory
RUN chown -R app:app /app

USER app

CMD ["uv", "run", "whatsapp-mcp"]
