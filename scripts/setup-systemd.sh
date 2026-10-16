#!/usr/bin/env bash
set -euo pipefail
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info() { echo -e "${GREEN}[setup-systemd]${NC} $*"; }
warn() { echo -e "${YELLOW}[setup-systemd]${NC} $*"; }
error() { echo -e "${RED}[setup-systemd]${NC} $*" >&2; }

if [[ $# -ne 1 ]] || [[ ! "$1" =~ ^(bridge|mcp-server|session-controller|redis|embedder|transcriber|dashboard|whisper)$ ]]; then
  error "Usage: $(basename "$0") <bridge|mcp-server|session-controller|redis|embedder|transcriber|dashboard|whisper>"
  exit 1
fi
COMPONENT="$1"
SYSTEMD_DIR="${HOME}/.config/systemd/user"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

resolve_executable() {
  local name="$1" value
  value="$(command -v "${name}" 2>/dev/null)" || { error "Missing executable: ${name}"; exit 1; }
  value="$(readlink -f "${value}")"
  [[ -x "${value}" ]] || { error "Not executable: ${name}"; exit 1; }
  printf '%s\n' "${value}"
}
version_at_least_7_2() {
  local major minor
  IFS=. read -r major minor _ <<<"$1"
  [[ "${major}" =~ ^[0-9]+$ && "${minor}" =~ ^[0-9]+$ ]] && (( major > 7 || major == 7 && minor >= 2 ))
}
NODE_BIN="$(resolve_executable node)"
REDIS_SERVER_BIN="$(resolve_executable redis-server)"
REDIS_CLI_BIN="$(resolve_executable redis-cli)"
RSYNC_BIN="$(resolve_executable rsync)"
SYSTEMCTL_BIN="$(resolve_executable systemctl)"
SYSTEMD_ANALYZE_BIN="$(resolve_executable systemd-analyze)"
[[ "$("${NODE_BIN}" --version)" == "v22.19.0" ]] || { error 'Node v22.19.0 is required'; exit 1; }
redis_version="$("${REDIS_SERVER_BIN}" --version | sed -nE 's/.*v=([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"
version_at_least_7_2 "${redis_version}" || { error 'Redis >=7.2 is required'; exit 1; }
"${REDIS_CLI_BIN}" --version >/dev/null
"${RSYNC_BIN}" --version >/dev/null
"${SYSTEMCTL_BIN}" --version >/dev/null
"${SYSTEMD_ANALYZE_BIN}" --version >/dev/null
[[ -n "${XDG_RUNTIME_DIR:-}" && -d "${XDG_RUNTIME_DIR}" ]] || { error 'A user systemd runtime directory is required'; exit 1; }

bridge_unit() { cat <<'EOF'
[Unit]
Description=WhatsApp Bridge
Wants=whatsapp-session-controller.service
Requires=whatsapp-session-redis.service
After=network.target postgresql.service whatsapp-session-redis.service whatsapp-session-controller.service

[Service]
Type=simple
ExecStart=/usr/bin/env BRIDGE_ADDR=127.0.0.1:8080 BRIDGE_LOG_LEVEL=info %h/.local/bin/whatsapp-bridge
WorkingDirectory=%h/.local/share/whatsapp-bridge
Environment=BRIDGE_DATA_DIR=%h/.local/share/whatsapp-bridge/data
Environment=BRIDGE_MANAGED=true
Environment=EMBEDDER_URL=http://127.0.0.1:8000
Environment=ANALYZER_URL=http://127.0.0.1:8500
EnvironmentFile=%h/.config/whatsapp-bridge/env
EnvironmentFile=%h/.config/whatsapp-bridge/relay.env
EnvironmentFile=%h/.config/whatsapp-notifications/env
Restart=on-failure
RestartPreventExitStatus=78
RestartSec=5
StandardOutput=null
StandardError=journal

[Install]
WantedBy=default.target
EOF
}
redis_unit() { cat <<EOF
[Unit]
Description=WhatsApp Session Controller Redis
After=network.target
Before=whatsapp-session-controller.service

[Service]
Type=simple
ExecStart=${REDIS_SERVER_BIN} %h/.config/whatsapp-session-controller/redis.conf --bind 127.0.0.1 ::1 --protected-mode yes
ExecStop=${REDIS_CLI_BIN} -h 127.0.0.1 -p 6379 shutdown save
Restart=on-failure
RestartSec=5
TimeoutStopSec=5min

[Install]
WantedBy=default.target
EOF
}
controller_unit() {
  cat <<EOF
[Unit]
Description=WhatsApp Persistent Session Controller
Requires=whatsapp-session-redis.service
After=network.target whatsapp-session-redis.service
Before=whatsapp-bridge.service

[Service]
Type=simple
UnsetEnvironment=DATABASE_URL
ExecStart=${NODE_BIN} %h/.local/share/whatsapp-session-controller/current/dist/src/main.js
WorkingDirectory=%h/.local/share/whatsapp-session-controller/current
EnvironmentFile=%h/.config/whatsapp-notifications/env
EnvironmentFile=%h/.config/whatsapp-session-controller/env
Environment=MCP_TRANSPORT=stdio
Environment=WORKSPACE_DIR=%h/workspace
Environment=BRIDGE_URL=http://127.0.0.1:8080
Environment=SESSION_CONTROLLER_BRIDGE_BASE_URL=http://127.0.0.1:8080
Restart=on-failure
RestartSec=5
TimeoutStopSec=infinity
KillMode=mixed

[Install]
WantedBy=default.target
EOF
}
mcp_server_unit() { cat <<'EOF'
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
whisper_unit() { cat <<'EOF'
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
embedder_unit() { cat <<EOF
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
transcriber_unit() { cat <<EOF
[Unit]
Description=WhatsApp Transcriber Worker
After=whatsapp-bridge.service whatsapp-whisper.service
[Service]
Type=simple
ExecStart=${REPO_ROOT}/transcriber/.venv/bin/python -m whatsapp_transcriber
WorkingDirectory=${REPO_ROOT}/transcriber
Environment=BRIDGE_URL=http://127.0.0.1:8080
Environment=WHISPER_URL=http://127.0.0.1:8443
Environment=ANALYZER_HTTP_ADDR=127.0.0.1:8500
EnvironmentFile=-%h/.config/whatsapp-bridge/env
Restart=on-failure
RestartSec=5
[Install]
WantedBy=default.target
EOF
}
dashboard_unit() { cat <<EOF
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

case "${COMPONENT}" in
  bridge) SERVICE_NAME=whatsapp-bridge; unit_content() { bridge_unit; } ;;
  mcp-server) SERVICE_NAME=whatsapp-mcp-server; unit_content() { mcp_server_unit; } ;;
  session-controller) SERVICE_NAME=whatsapp-session-controller; unit_content() { controller_unit; } ;;
  redis) SERVICE_NAME=whatsapp-session-redis; unit_content() { redis_unit; } ;;
  embedder) SERVICE_NAME=whatsapp-embedder; unit_content() { embedder_unit; } ;;
  transcriber) SERVICE_NAME=whatsapp-transcriber; unit_content() { transcriber_unit; } ;;
  dashboard) SERVICE_NAME=whatsapp-dashboard; unit_content() { dashboard_unit; } ;;
  whisper) SERVICE_NAME=whatsapp-whisper; unit_content() { whisper_unit; } ;;
esac
UNIT_FILE="${SYSTEMD_DIR}/${SERVICE_NAME}.service"
mkdir -p "${SYSTEMD_DIR}"
temporary="$(mktemp "${SYSTEMD_DIR}/.${SERVICE_NAME}.XXXXXX")"
verify_dir="$(mktemp -d)"
trap 'rm -f "${temporary}"; rm -rf "${verify_dir}"' EXIT
unit_content >"${temporary}"
chmod 0644 "${temporary}"
cp "${temporary}" "${verify_dir}/${SERVICE_NAME}.service"
for dependency in whatsapp-bridge whatsapp-mcp-server whatsapp-session-controller whatsapp-session-redis whatsapp-embedder whatsapp-transcriber whatsapp-dashboard whatsapp-whisper postgresql; do
  [[ "${dependency}" == "${SERVICE_NAME}" ]] && continue
  printf '[Unit]\nDescription=Static verification stub\n[Service]\nType=oneshot\nExecStart=/bin/true\n' >"${verify_dir}/${dependency}.service"
done
SYSTEMD_UNIT_PATH="${verify_dir}:" "${SYSTEMD_ANALYZE_BIN}" --user verify "${verify_dir}/${SERVICE_NAME}.service"
if [[ -f "${UNIT_FILE}" ]] && cmp -s "${temporary}" "${UNIT_FILE}"; then
  info "Unit unchanged: ${UNIT_FILE}"
else
  mv -f "${temporary}" "${UNIT_FILE}"
  info "Installed ${UNIT_FILE}; use systemd user drop-ins for local overrides."
fi
info "Installed and statically verified ${SERVICE_NAME}.service; no daemon reload, enable, start, restart, or stop was performed."
