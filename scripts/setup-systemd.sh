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
if [[ $# -ne 1 ]] || [[ "$1" != "bridge" && "$1" != "mcp-server" ]]; then
  error "Usage: $(basename "$0") <bridge|mcp-server>"
  exit 1
fi

COMPONENT="$1"
SYSTEMD_DIR="${HOME}/.config/systemd/user"

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
