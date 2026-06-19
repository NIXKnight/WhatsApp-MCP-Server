#!/usr/bin/env bash
set -euo pipefail

# Color helpers
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()    { echo -e "${GREEN}[setup-systemd]${NC} $*"; }
warn()    { echo -e "${YELLOW}[setup-systemd]${NC} $*"; }
error()   { echo -e "${RED}[setup-systemd]${NC} $*" >&2; }

# -------------------------------------------------------------------
# Argument validation
# -------------------------------------------------------------------
if [[ $# -ne 1 ]] || [[ "$1" != "bridge" && "$1" != "mcp-server" \
      && "$1" != "embedder" && "$1" != "transcriber" && "$1" != "dashboard" \
      && "$1" != "whisper" ]]; then
  error "Usage: $(basename "$0") <bridge|mcp-server|embedder|transcriber|dashboard|whisper>"
  exit 1
fi

COMPONENT="$1"
SYSTEMD_DIR="${HOME}/.config/systemd/user"

# Absolute repo root. systemd cannot resolve repo-relative paths; only %h (home)
# is a systemd specifier. Worker units bake REPO_ROOT in as absolute paths.
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# -------------------------------------------------------------------
# Unit definitions
# -------------------------------------------------------------------
bridge_unit() {
  cat <<'EOF'
[Unit]
Description=WhatsApp Bridge
After=network.target postgresql.service

[Service]
Type=simple
ExecStart=%h/.local/bin/whatsapp-bridge
WorkingDirectory=%h/.local/share/whatsapp-bridge
Environment=BRIDGE_DATA_DIR=%h/.local/share/whatsapp-bridge/data
Environment=BRIDGE_ADDR=127.0.0.1:8080
Environment=BRIDGE_LOG_LEVEL=info
Environment=EMBEDDER_URL=http://127.0.0.1:8000
EnvironmentFile=-%h/.config/whatsapp-bridge/env
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
}

mcp_server_unit() {
  cat <<'EOF'
[Unit]
Description=WhatsApp MCP Server
After=whatsapp-bridge.service
Requires=whatsapp-bridge.service

[Service]
Type=simple
ExecStart=%h/.local/share/whatsapp-mcp-server-venv/bin/whatsapp-mcp
WorkingDirectory=%h/.local/share/whatsapp-mcp-server
Environment=MCP_TRANSPORT=http
Environment=MCP_HOST=127.0.0.1
Environment=MCP_PORT=3000
EnvironmentFile=-%h/.config/whatsapp-bridge/env
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
}

# Whisper.cpp server unit. All paths live under $HOME, so a QUOTED heredoc with
# %h specifiers suffices (no REPO_ROOT). Validated live: large-v3 on RX 6800M,
# serving /inference on 127.0.0.1:8443.
whisper_unit() {
  cat <<'EOF'
[Unit]
Description=Whisper.cpp Server (large-v3, Vulkan GPU)
After=network.target

[Service]
Type=simple
Environment=GGML_VK_VISIBLE_DEVICES=1
ExecStart=%h/whisper.cpp/build/bin/whisper-server -m %h/whisper-models/ggml-large-v3.bin -l auto -t 8 --convert --host 127.0.0.1 --port 8443
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
}

# Worker units bake absolute REPO_ROOT paths via an UNQUOTED heredoc so
# ${REPO_ROOT} expands while systemd %h stays literal.
embedder_unit() {
  cat <<EOF
[Unit]
Description=WhatsApp Embedder Worker
After=network.target

[Service]
Type=simple
ExecStart=${REPO_ROOT}/embedder/.venv/bin/python -m whatsapp_embedder
WorkingDirectory=${REPO_ROOT}/embedder
Environment=EMBEDDING_MODEL=paraphrase-multilingual-MiniLM-L12-v2
Environment=EMBED_HTTP_ADDR=127.0.0.1:8000
EnvironmentFile=-%h/.config/whatsapp-bridge/env
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
}

transcriber_unit() {
  cat <<EOF
[Unit]
Description=WhatsApp Transcriber Worker
After=whatsapp-bridge.service whatsapp-whisper.service

[Service]
Type=simple
ExecStart=${REPO_ROOT}/transcriber/.venv/bin/python -m whatsapp_transcriber
WorkingDirectory=${REPO_ROOT}/transcriber
Environment=BRIDGE_URL=http://127.0.0.1:8080
Environment=WHISPER_URL=http://127.0.0.1:8443
EnvironmentFile=-%h/.config/whatsapp-bridge/env
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
}

dashboard_unit() {
  cat <<EOF
[Unit]
Description=WhatsApp Dashboard
After=whatsapp-bridge.service

[Service]
Type=simple
ExecStart=${REPO_ROOT}/dashboard/.venv/bin/uvicorn whatsapp_dashboard.app:app --host 127.0.0.1 --port 9090
WorkingDirectory=${REPO_ROOT}/dashboard
Environment=BRIDGE_URL=http://127.0.0.1:8080
Environment=DASHBOARD_HOST=127.0.0.1
Environment=DASHBOARD_PORT=9090
EnvironmentFile=-%h/.config/whatsapp-bridge/env
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
}

# -------------------------------------------------------------------
# Resolve names
# -------------------------------------------------------------------
case "${COMPONENT}" in
  bridge)
    SERVICE_NAME="whatsapp-bridge"
    UNIT_FILE="${SYSTEMD_DIR}/whatsapp-bridge.service"
    unit_content() { bridge_unit; }
    ;;
  mcp-server)
    SERVICE_NAME="whatsapp-mcp-server"
    UNIT_FILE="${SYSTEMD_DIR}/whatsapp-mcp-server.service"
    unit_content() { mcp_server_unit; }
    ;;
  embedder)
    SERVICE_NAME="whatsapp-embedder"
    UNIT_FILE="${SYSTEMD_DIR}/whatsapp-embedder.service"
    unit_content() { embedder_unit; }
    ;;
  transcriber)
    SERVICE_NAME="whatsapp-transcriber"
    UNIT_FILE="${SYSTEMD_DIR}/whatsapp-transcriber.service"
    unit_content() { transcriber_unit; }
    ;;
  dashboard)
    SERVICE_NAME="whatsapp-dashboard"
    UNIT_FILE="${SYSTEMD_DIR}/whatsapp-dashboard.service"
    unit_content() { dashboard_unit; }
    ;;
  whisper)
    SERVICE_NAME="whatsapp-whisper"
    UNIT_FILE="${SYSTEMD_DIR}/whatsapp-whisper.service"
    unit_content() { whisper_unit; }
    ;;
esac

# -------------------------------------------------------------------
# Write unit file (only if absent)
# -------------------------------------------------------------------
mkdir -p "${SYSTEMD_DIR}"

if [[ -f "${UNIT_FILE}" ]]; then
  warn "Unit file already exists: ${UNIT_FILE} (not overwritten)"
  warn "  --> Delete it manually to regenerate."
else
  unit_content > "${UNIT_FILE}"
  info "Created unit file: ${UNIT_FILE}"
fi

# -------------------------------------------------------------------
# Reload daemon, enable, and restart
# -------------------------------------------------------------------
info "Reloading systemd user daemon..."
systemctl --user daemon-reload

info "Enabling ${SERVICE_NAME}.service..."
systemctl --user enable "${SERVICE_NAME}.service"

info "Restarting ${SERVICE_NAME}.service..."
systemctl --user restart "${SERVICE_NAME}.service"

info "setup-systemd complete for component: ${COMPONENT}"
info "  Check status with: systemctl --user status ${SERVICE_NAME}.service"
