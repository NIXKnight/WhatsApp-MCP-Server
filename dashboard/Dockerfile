# ---------------------------------------------------------------------------
# Stage 1 — assets: build the Tailwind stylesheet with the standalone CLI.
#
# No Node, no npm. Downloads the pinned tailwindlabs standalone binary
# (glibc, linux-x64) and purges against the Jinja templates per
# tailwind.config.js. Produces static/app.css, which the python builder
# stage folds into the wheel. curl + the ~30MB binary stay in THIS stage and
# never reach the final image.
# ---------------------------------------------------------------------------
FROM python:3.13-slim AS assets

ARG TAILWIND_VERSION=3.4.17

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /assets

# Pinned standalone CLI from the tailwindlabs/tailwindcss GitHub release.
RUN curl -fsSL \
      "https://github.com/tailwindlabs/tailwindcss/releases/download/v${TAILWIND_VERSION}/tailwindcss-linux-x64" \
      -o /usr/local/bin/tailwindcss \
    && chmod +x /usr/local/bin/tailwindcss

# Build inputs: config, the @tailwind/@apply source, and the templates the
# content scanner purges against. Paths mirror the globs in tailwind.config.js
# ("./src/whatsapp_dashboard/templates/**/*.html").
COPY tailwind.config.js ./
COPY assets/ assets/
COPY src/whatsapp_dashboard/templates/ src/whatsapp_dashboard/templates/

RUN tailwindcss \
      -c tailwind.config.js \
      -i assets/input.css \
      -o src/whatsapp_dashboard/static/app.css \
      --minify

# ---------------------------------------------------------------------------
# Stage 2 — builder: resolve dependencies and build the wheel with uv.
#
# The generated app.css is dropped into the package's static/ dir BEFORE
# `uv sync` so hatchling bakes it (alongside the vendored FontAwesome that
# arrives via `COPY src/ src/`) into the installed package.
# ---------------------------------------------------------------------------
FROM python:3.13-slim AS builder

RUN pip install --no-cache-dir uv

WORKDIR /build
COPY pyproject.toml uv.lock ./
COPY src/ src/

# Authoritative, freshly-built stylesheet (the host copy is .dockerignore'd,
# so the wheel's app.css is always the one produced in the assets stage).
COPY --from=assets /assets/src/whatsapp_dashboard/static/app.css \
                   src/whatsapp_dashboard/static/app.css

RUN uv sync --frozen --no-dev --no-editable

# ---------------------------------------------------------------------------
# Stage 3 — final: lean runtime. Only the populated virtualenv ships; the
# installed package already carries static/app.css + static/fontawesome.
# ---------------------------------------------------------------------------
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
