FROM node:22.19.0-bookworm-slim AS build
WORKDIR /build/session-controller
COPY session-controller/package.json session-controller/package-lock.json ./
RUN npm ci
COPY session-controller/tsconfig.json ./
COPY session-controller/src ./src
RUN npm run build && npm prune --omit=dev

FROM node:22.19.0-bookworm-slim
ARG APP_UID=1000
ARG APP_GID=1000
ENV NODE_ENV=production \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/opt/whatsapp-mcp/.venv
RUN case "${APP_UID}" in ''|*[!0-9]*) exit 1 ;; esac \
 && case "${APP_GID}" in ''|*[!0-9]*) exit 1 ;; esac \
 && [ "${APP_UID}" -ge 1 ] && [ "${APP_UID}" -le 65535 ] \
 && [ "${APP_GID}" -ge 1 ] && [ "${APP_GID}" -le 65535 ] \
 && groupmod --non-unique --gid "${APP_GID}" node \
 && usermod --non-unique --uid "${APP_UID}" --gid "${APP_GID}" node \
 && apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv python3-pip ca-certificates \
 && python3 -m pip install --no-cache-dir --break-system-packages uv==0.9.27 \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build --chown=node:node /build/session-controller/package.json /build/session-controller/package-lock.json ./
COPY --from=build --chown=node:node /build/session-controller/node_modules ./node_modules
COPY --from=build --chown=node:node /build/session-controller/dist ./dist
COPY --chown=node:node session-controller/config ./config
COPY mcp-server/pyproject.toml mcp-server/uv.lock /opt/whatsapp-mcp/
COPY mcp-server/src /opt/whatsapp-mcp/src
RUN cd /opt/whatsapp-mcp \
 && uv sync --frozen --no-dev \
 && mkdir -p /sessions /workspace /pi-agent \
 && chown -R node:node /app /opt/whatsapp-mcp /sessions /workspace /pi-agent
USER node:node
EXPOSE 8644
ENTRYPOINT ["node", "/app/dist/src/main.js"]
