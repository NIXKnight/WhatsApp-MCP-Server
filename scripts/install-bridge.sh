#!/usr/bin/env bash
set -euo pipefail
umask 077

# Color helpers
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()    { echo -e "${GREEN}[install-bridge]${NC} $*"; }
warn()    { echo -e "${YELLOW}[install-bridge]${NC} $*"; }
error()   { echo -e "${RED}[install-bridge]${NC} $*" >&2; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BRIDGE_DIR="${REPO_ROOT}/bridge"
INSTALL_BIN="${HOME}/.local/bin"
BINARY_NAME="whatsapp-bridge"
WORK_DIR="${HOME}/.local/share/whatsapp-bridge"
BRIDGE_CONFIG="${HOME}/.config/whatsapp-bridge"
NOTIFICATION_CONFIG="${HOME}/.config/whatsapp-notifications"
ENV_FILE="${BRIDGE_CONFIG}/env"
BRIDGE_RELAY_ENV="${BRIDGE_CONFIG}/relay.env"
NOTIFICATION_ENV="${NOTIFICATION_CONFIG}/env"

secure_dir() {
  local path="$1"
  [[ ! -L "${path}" && ( ! -e "${path}" || -d "${path}" ) ]] || { error 'Managed directory is not a real directory'; exit 1; }
  mkdir -p -- "${path}"
  chmod 0700 -- "${path}"
}
secure_file_or_absent() {
  local path="$1"
  [[ ! -L "${path}" && ( ! -e "${path}" || -f "${path}" ) ]] || { error 'Managed file is not a regular file'; exit 1; }
}
validate_env_keys() {
  local file="$1" allowed="$2" line trimmed key
  [[ -e "${file}" ]] || return 0
  while IFS= read -r line || [[ -n "${line}" ]]; do
    trimmed="${line#"${line%%[![:space:]]*}"}"
    [[ -z "${trimmed}" || "${trimmed}" == \#* ]] && continue
    if [[ ! "${trimmed}" =~ ^([A-Za-z_][A-Za-z0-9_]*)= ]]; then
      error 'Managed environment assignment is invalid'; exit 1
    fi
    key="${BASH_REMATCH[1]}"
    [[ " ${allowed} " == *" ${key} "* ]] || { error 'Managed environment assignment is invalid'; exit 1; }
  done <"${file}"
}

# Validate private configuration paths before reading or writing any of them.
for directory in "${BRIDGE_CONFIG}" "${NOTIFICATION_CONFIG}"; do secure_dir "${directory}"; done
for file in "${ENV_FILE}" "${BRIDGE_RELAY_ENV}" "${NOTIFICATION_ENV}"; do secure_file_or_absent "${file}"; done

BRIDGE_ENV_KEYS='DATABASE_URL PG_DSN BRIDGE_ADDR BRIDGE_DATA_DIR BRIDGE_LOG_LEVEL BRIDGE_LOG_STATUS_REQUESTS EMBEDDER_URL ANALYZER_URL MEDIA_RETRY_ENABLED MEDIA_RETRY_INTERVAL MEDIA_MAX_DOWNLOAD_ATTEMPTS MEDIA_RETRY_BATCH_SIZE BRIDGE_URL LOG_LEVEL MCP_TRANSPORT MCP_HOST MCP_PORT WORKSPACE_DIR WORKSPACE_HOST_PATH EMBEDDING_MODEL EMBED_BATCH_SIZE EMBED_POLL_INTERVAL EMBED_HTTP_ADDR WHISPER_URL TRANSCRIBE_BATCH_SIZE TRANSCRIBE_POLL_INTERVAL ANALYZER_HTTP_ADDR DASHBOARD_HOST DASHBOARD_PORT CONFIG_PATH'
RELAY_ENV_KEYS='BRIDGE_NOTIFY_ENABLED BRIDGE_NOTIFY_URL BRIDGE_NOTIFY_TIMEOUT BRIDGE_NOTIFY_POLL_INTERVAL BRIDGE_NOTIFY_MIN_INTERVAL BRIDGE_NOTIFY_BATCH_SIZE BRIDGE_NOTIFY_MAX_ATTEMPTS BRIDGE_NOTIFY_MAX_AGE BRIDGE_NOTIFY_RETENTION'
NOTIFICATION_ENV_KEYS='BRIDGE_NOTIFY_SECRET BRIDGE_NOTIFY_GROUP_ALLOWLIST BRIDGE_NOTIFY_OPERATOR_JIDS'
validate_env_keys "${ENV_FILE}" "${BRIDGE_ENV_KEYS}"
validate_env_keys "${BRIDGE_RELAY_ENV}" "${RELAY_ENV_KEYS}"
validate_env_keys "${NOTIFICATION_ENV}" "${NOTIFICATION_ENV_KEYS}"
for private_env in "${ENV_FILE}" "${BRIDGE_RELAY_ENV}" "${NOTIFICATION_ENV}"; do
  [[ ! -e "${private_env}" ]] || chmod 0600 -- "${private_env}"
done

# -------------------------------------------------------------------
# Build
# -------------------------------------------------------------------
info "Building Go bridge (CGO_ENABLED=0)..."
(
  cd "${BRIDGE_DIR}"
  CGO_ENABLED=0 go build -o "${BINARY_NAME}" .
)
info "Build complete."

# -------------------------------------------------------------------
# Install binary
# -------------------------------------------------------------------
mkdir -p "${INSTALL_BIN}"
# Unlink first so install succeeds while the bridge is running (a running ELF
# can't be overwritten in place — ETXTBSY — but the inode can be unlinked; the
# live process keeps its open inode until restart).
rm -f "${INSTALL_BIN}/${BINARY_NAME}"
cp "${BRIDGE_DIR}/${BINARY_NAME}" "${INSTALL_BIN}/${BINARY_NAME}"
info "Installed ${BINARY_NAME} -> ${INSTALL_BIN}/${BINARY_NAME}"

# -------------------------------------------------------------------
# Create bridge working directory
# -------------------------------------------------------------------
if [[ ! -d "${WORK_DIR}" ]]; then
  mkdir -p "${WORK_DIR}/data"
  info "Created working directory: ${WORK_DIR}"
else
  info "Working directory already exists: ${WORK_DIR}"
fi

# -------------------------------------------------------------------
# Create private environment templates (only if absent)
# -------------------------------------------------------------------
if [[ ! -e "${ENV_FILE}" ]]; then
  cat >"${ENV_FILE}" <<'EOF'
# Transitional compatibility environment shared by the bridge, MCP server,
# embedder, transcriber, and dashboard. Fill in DATABASE_URL before activation.
# Optional accepted names are limited to documented database, bridge/media,
# MCP, workspace, embedder, transcriber, and dashboard compatibility settings.
DATABASE_URL=
BRIDGE_LOG_STATUS_REQUESTS=true
EOF
  warn 'Created compatibility environment; values were not printed'
else
  info 'Preserved compatibility environment; values were not printed'
fi
chmod 0600 -- "${ENV_FILE}"

if [[ ! -e "${BRIDGE_RELAY_ENV}" ]]; then
  cat >"${BRIDGE_RELAY_ENV}" <<'EOF'
# Bridge-only relay activation and destination. Optional delivery tuning may
# use only documented BRIDGE_NOTIFY_TIMEOUT/POLL_INTERVAL/MIN_INTERVAL/
# BATCH_SIZE/MAX_ATTEMPTS/MAX_AGE/RETENTION keys in this file.
BRIDGE_NOTIFY_ENABLED=
BRIDGE_NOTIFY_URL=
EOF
  info 'Created bridge-only relay environment; values were not printed'
else
  info 'Preserved bridge-only relay environment; values were not printed'
fi
chmod 0600 -- "${BRIDGE_RELAY_ENV}"

if [[ ! -e "${NOTIFICATION_ENV}" ]]; then
  cat >"${NOTIFICATION_ENV}" <<'EOF'
# Shared notification admission/authentication only. Supply values privately.
BRIDGE_NOTIFY_SECRET=
BRIDGE_NOTIFY_GROUP_ALLOWLIST=
BRIDGE_NOTIFY_OPERATOR_JIDS=
EOF
  info 'Created dedicated notification environment; values were not printed'
else
  info 'Preserved dedicated notification environment; values were not printed'
fi
chmod 0600 -- "${NOTIFICATION_ENV}"

info "install-bridge complete."
info 'No service was enabled, started, restarted, stopped, or reloaded.'
