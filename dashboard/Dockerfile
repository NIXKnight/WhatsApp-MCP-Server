FROM python:3.13-slim AS builder

RUN pip install --no-cache-dir uv

WORKDIR /build
COPY pyproject.toml uv.lock ./
COPY src/ src/

RUN uv sync --frozen --no-dev --no-editable

FROM python:3.13-slim

RUN adduser --disabled-password --uid 1000 bridge

WORKDIR /app
COPY --from=builder /build/.venv /app/.venv

ENV PATH="/app/.venv/bin:$PATH" \
    DASHBOARD_HOST=0.0.0.0 \
    DASHBOARD_PORT=9090

USER bridge

EXPOSE 9090

CMD ["/app/.venv/bin/python", "-m", "uvicorn", "whatsapp_dashboard.app:app", "--host", "0.0.0.0", "--port", "9090"]
