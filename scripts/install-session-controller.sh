#!/usr/bin/env bash
set -euo pipefail
umask 077

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info() { echo -e "${GREEN}[install-session-controller]${NC} $*"; }
warn() { echo -e "${YELLOW}[install-session-controller]${NC} $*"; }
error() { echo -e "${RED}[install-session-controller]${NC} $*" >&2; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC="${REPO_ROOT}/session-controller"
APP_ROOT="${HOME}/.local/share/whatsapp-session-controller"
RELEASES="${APP_ROOT}/releases"
CURRENT="${APP_ROOT}/current"
STATE="${APP_ROOT}/sessions"
CONFIG="${HOME}/.config/whatsapp-session-controller"
BRIDGE_CONFIG="${HOME}/.config/whatsapp-bridge"
NOTIFICATION_CONFIG="${HOME}/.config/whatsapp-notifications"
REDIS_STATE="${APP_ROOT}/redis"
REDIS_CONFIG="${CONFIG}/redis.conf"
ENV_FILE="${CONFIG}/env"
BRIDGE_ENV="${BRIDGE_CONFIG}/env"
BRIDGE_RELAY_ENV="${BRIDGE_CONFIG}/relay.env"
NOTIFICATION_ENV="${NOTIFICATION_CONFIG}/env"
STAGING=''
NEXT_LINK=''
cleanup_staging() {
  if [[ -n "${STAGING}" && -d "${STAGING}" ]]; then
    chmod -R u+w -- "${STAGING}" 2>/dev/null || true
    rm -rf -- "${STAGING}"
  fi
  if [[ -n "${NEXT_LINK}" && -L "${NEXT_LINK}" ]]; then rm -f -- "${NEXT_LINK}"; fi
}
trap cleanup_staging EXIT

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
NPM_CLI="$(resolve_executable npm)"
RSYNC_BIN="$(resolve_executable rsync)"
REDIS_SERVER_BIN="$(resolve_executable redis-server)"
REDIS_CLI_BIN="$(resolve_executable redis-cli)"
SYSTEMCTL_BIN="$(resolve_executable systemctl)"
SYSTEMD_ANALYZE_BIN="$(resolve_executable systemd-analyze)"
node_version="$("${NODE_BIN}" --version)"
[[ "${node_version}" == "v22.19.0" || "${node_version}" == "v24.12.0" ]] || { error 'Exact Node v22.19.0 or v24.12.0 is required'; exit 1; }
redis_version="$("${REDIS_SERVER_BIN}" --version | sed -nE 's/.*v=([0-9]+\.[0-9]+\.[0-9]+).*/\1/p')"
version_at_least_7_2 "${redis_version}" || { error 'Redis >=7.2 is required'; exit 1; }
"${REDIS_CLI_BIN}" --version >/dev/null
"${SYSTEMCTL_BIN}" --version >/dev/null
"${SYSTEMD_ANALYZE_BIN}" --version >/dev/null
[[ -n "${XDG_RUNTIME_DIR:-}" && -d "${XDG_RUNTIME_DIR}" ]] || { error 'A user systemd runtime directory is required'; exit 1; }

require_source_dir() {
  local path="$1"
  [[ -d "${path}" && ! -L "${path}" ]] || { error 'Controller source directory is invalid'; exit 1; }
}
require_source_file() {
  local path="$1"
  [[ -f "${path}" && ! -L "${path}" ]] || { error 'Controller source file is invalid'; exit 1; }
}
# Validate every direct input and both recursive roots before staging exists.
require_source_dir "${SRC}/src"
require_source_dir "${SRC}/config"
for source_input in package.json package-lock.json tsconfig.json; do
  require_source_file "${SRC}/${source_input}"
done
for template in system-prompt.md pre-compaction.md compaction-summary.md; do
  require_source_file "${SRC}/config/${template}"
done

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
# Validate every directory, including Redis state, before any configuration or
# state write. This rejects a symlink/non-directory REDIS_STATE before redis.conf.
for directory in "${APP_ROOT}" "${RELEASES}" "${STATE}" "${REDIS_STATE}" "${CONFIG}" "${BRIDGE_CONFIG}" "${NOTIFICATION_CONFIG}"; do secure_dir "${directory}"; done
for file in "${REDIS_CONFIG}" "${ENV_FILE}" "${BRIDGE_ENV}" "${BRIDGE_RELAY_ENV}" "${NOTIFICATION_ENV}"; do secure_file_or_absent "${file}"; done

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
BRIDGE_ENV_KEYS='DATABASE_URL PG_DSN BRIDGE_ADDR BRIDGE_DATA_DIR BRIDGE_LOG_LEVEL BRIDGE_LOG_STATUS_REQUESTS EMBEDDER_URL ANALYZER_URL MEDIA_RETRY_ENABLED MEDIA_RETRY_INTERVAL MEDIA_MAX_DOWNLOAD_ATTEMPTS MEDIA_RETRY_BATCH_SIZE BRIDGE_URL LOG_LEVEL MCP_TRANSPORT MCP_HOST MCP_PORT WORKSPACE_DIR WORKSPACE_HOST_PATH EMBEDDING_MODEL EMBED_BATCH_SIZE EMBED_POLL_INTERVAL EMBED_HTTP_ADDR WHISPER_URL TRANSCRIBE_BATCH_SIZE TRANSCRIBE_POLL_INTERVAL ANALYZER_HTTP_ADDR DASHBOARD_HOST DASHBOARD_PORT CONFIG_PATH'
RELAY_ENV_KEYS='BRIDGE_NOTIFY_ENABLED BRIDGE_NOTIFY_URL BRIDGE_NOTIFY_TIMEOUT BRIDGE_NOTIFY_POLL_INTERVAL BRIDGE_NOTIFY_MIN_INTERVAL BRIDGE_NOTIFY_BATCH_SIZE BRIDGE_NOTIFY_MAX_ATTEMPTS BRIDGE_NOTIFY_MAX_AGE BRIDGE_NOTIFY_RETENTION'
NOTIFICATION_ENV_KEYS='BRIDGE_NOTIFY_SECRET BRIDGE_NOTIFY_GROUP_ALLOWLIST BRIDGE_NOTIFY_OPERATOR_JIDS'
CONTROLLER_ENV_KEYS='SESSION_CONTROLLER_HOST SESSION_CONTROLLER_PORT SESSION_CONTROLLER_WEBHOOK_PATH SESSION_CONTROLLER_MAX_BODY_BYTES SESSION_CONTROLLER_REQUEST_TIMEOUT_MS SESSION_CONTROLLER_MAX_INFLIGHT SESSION_CONTROLLER_FRESHNESS_SECONDS SESSION_CONTROLLER_REDIS_URL SESSION_CONTROLLER_WAIT_AOF_MS SESSION_CONTROLLER_BLOCK_MS SESSION_CONTROLLER_BRIDGE_BASE_URL SESSION_CONTROLLER_BRIDGE_POLL_MS SESSION_CONTROLLER_BRIDGE_REQUEST_TIMEOUT_MS SESSION_CONTROLLER_CONCURRENCY SESSION_CONTROLLER_LEASE_MS SESSION_CONTROLLER_RENEW_MS SESSION_CONTROLLER_GAP_TIMEOUT_MS SESSION_CONTROLLER_PI_CWD SESSION_CONTROLLER_PI_AGENT_DIR SESSION_CONTROLLER_SESSION_DIR SESSION_CONTROLLER_SYSTEM_PROMPT_PATH SESSION_CONTROLLER_PRE_COMPACTION_PATH SESSION_CONTROLLER_COMPACTION_SUMMARY_PATH SESSION_CONTROLLER_POLICY_VERSION SESSION_CONTROLLER_TEMPLATE_VERSION SESSION_CONTROLLER_PI_PROVIDER SESSION_CONTROLLER_PI_MODEL SESSION_CONTROLLER_MCP_COMMAND SESSION_CONTROLLER_MCP_ARGS_JSON SESSION_CONTROLLER_MCP_CWD SESSION_CONTROLLER_SAFE_WORKING_WINDOW_TOKENS SESSION_CONTROLLER_DEFAULT_CONTEXT_WINDOW_TOKENS SESSION_CONTROLLER_COMPACTION_HEADROOM_PERCENT SESSION_CONTROLLER_RESPONSE_RESERVE_TOKENS SESSION_CONTROLLER_PREPARATION_RESERVE_TOKENS SESSION_CONTROLLER_SUMMARY_RESERVE_TOKENS SESSION_CONTROLLER_CHECKPOINT_MAX_BYTES'
validate_env_keys "${BRIDGE_ENV}" "${BRIDGE_ENV_KEYS}"
validate_env_keys "${BRIDGE_RELAY_ENV}" "${RELAY_ENV_KEYS}"
validate_env_keys "${NOTIFICATION_ENV}" "${NOTIFICATION_ENV_KEYS}"
validate_env_keys "${ENV_FILE}" "${CONTROLLER_ENV_KEYS}"
# Validation reads only key names; preserve bytes and normalize private modes.
for private_env in "${BRIDGE_ENV}" "${BRIDGE_RELAY_ENV}" "${NOTIFICATION_ENV}" "${ENV_FILE}"; do
  [[ ! -e "${private_env}" ]] || chmod 0600 -- "${private_env}"
done

release_version="$("${NODE_BIN}" -e 'const fs=require("node:fs");const value=JSON.parse(fs.readFileSync(process.argv[1],"utf8")).version;if(typeof value!=="string")process.exit(1);process.stdout.write(value);' "${SRC}/package.json")"
[[ "${release_version}" =~ ^[0-9A-Za-z][0-9A-Za-z._-]*$ ]] || { error 'Invalid controller package version'; exit 1; }
STAGING="$(mktemp -d "${RELEASES}/.staging-${release_version}.XXXXXXXX")"
staging_name="${STAGING##*/}"
nonce="${staging_name#".staging-${release_version}."}"
release_id="${release_version}-${nonce}"
RELEASE="${RELEASES}/${release_id}"
[[ ! -e "${RELEASE}" ]] || { error 'Release path collision'; exit 1; }

info "Building immutable controller release ${release_id} in staging..."
# Copy from a positive allowlist only. Tests, environment/auth/model files,
# runtime/session/local state, and every other repository path are unreachable.
install -m 0644 -- \
  "${SRC}/package.json" "${SRC}/package-lock.json" "${SRC}/tsconfig.json" \
  "${STAGING}/"
install -d -m 0755 -- "${STAGING}/src" "${STAGING}/config"
# Rooted at the validated source tree; do not copy or follow any symlink.
"${RSYNC_BIN}" --recursive --times --prune-empty-dirs --no-links \
  --include='*/' --include='*.ts' --exclude='*' \
  --chmod=Du=rwx,Dgo=rx,Fu=rw,Fgo=r \
  "${SRC}/src/" "${STAGING}/src/"
source_count=0
while IFS= read -r -d '' _; do ((source_count += 1)); done \
  < <(find "${STAGING}/src" -type f -name '*.ts' -print0)
(( source_count > 0 )) || { error 'No controller TypeScript sources found'; exit 1; }
for template in system-prompt.md pre-compaction.md compaction-summary.md; do
  install -m 0644 -- "${SRC}/config/${template}" "${STAGING}/config/${template}"
done
(
  cd "${STAGING}"
  "${NODE_BIN}" "${NPM_CLI}" ci
  "${NODE_BIN}" "${NPM_CLI}" run typecheck
  "${NODE_BIN}" "${NPM_CLI}" run build
  "${NODE_BIN}" "${NPM_CLI}" prune --omit=dev
)
chmod -R a-w -- "${STAGING}"
# Same-filesystem rename publishes only a complete read-only release. Nothing
# below ever writes through RELEASE or CURRENT.
mv -- "${STAGING}" "${RELEASE}"
STAGING=''

for template in system-prompt.md pre-compaction.md compaction-summary.md; do
  target="${CONFIG}/${template}"
  secure_file_or_absent "${target}"
  if [[ ! -e "${target}" ]]; then
    install -m 0600 "${RELEASE}/config/${template}" "${target}"
    info "Installed operator-editable ${template}"
  else
    info "Preserved operator-editable ${template}"
  fi
  chmod 0600 -- "${target}"
done

if [[ ! -e "${REDIS_CONFIG}" ]]; then
  cat >"${REDIS_CONFIG}" <<EOF
bind 127.0.0.1 ::1
protected-mode yes
port 6379
dir ${REDIS_STATE}
appendonly yes
appendfsync always
maxmemory-policy noeviction
daemonize no
logfile ""
EOF
  info "Created controller Redis configuration"
else
  info "Preserved controller Redis configuration"
fi
chmod 0600 -- "${REDIS_CONFIG}"

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

if [[ ! -e "${ENV_FILE}" ]]; then
  cat >"${ENV_FILE}" <<EOF
# Controller-only settings. Notification authentication/admission belongs only
# in ~/.config/whatsapp-notifications/env. DATABASE_URL never belongs here.
SESSION_CONTROLLER_REDIS_URL=redis://127.0.0.1:6379/0
SESSION_CONTROLLER_BRIDGE_BASE_URL=http://127.0.0.1:8080
SESSION_CONTROLLER_HOST=127.0.0.1
SESSION_CONTROLLER_PORT=8644
SESSION_CONTROLLER_WEBHOOK_PATH=/webhooks/whatsapp
SESSION_CONTROLLER_PI_CWD=${HOME}/workspace
SESSION_CONTROLLER_PI_AGENT_DIR=${HOME}/.pi/agent
SESSION_CONTROLLER_SESSION_DIR=${HOME}/.local/share/whatsapp-session-controller/sessions
SESSION_CONTROLLER_SYSTEM_PROMPT_PATH=${HOME}/.config/whatsapp-session-controller/system-prompt.md
SESSION_CONTROLLER_PRE_COMPACTION_PATH=${HOME}/.config/whatsapp-session-controller/pre-compaction.md
SESSION_CONTROLLER_COMPACTION_SUMMARY_PATH=${HOME}/.config/whatsapp-session-controller/compaction-summary.md
SESSION_CONTROLLER_MCP_COMMAND=${HOME}/.local/share/whatsapp-mcp-server-venv/bin/whatsapp-mcp
SESSION_CONTROLLER_MCP_ARGS_JSON=[]
SESSION_CONTROLLER_MCP_CWD=${HOME}/.local/share/whatsapp-mcp-server
# Add controller-only model selection and safe working-window tokens here
# before the separately authorized first start.
EOF
  warn 'Created controller-only environment; values were not printed'
else
  info 'Preserved controller-only environment; values were not printed'
fi
chmod 0600 -- "${ENV_FILE}"

NEXT_LINK="${APP_ROOT}/.current-${release_id}"
ln -s "releases/${release_id}" "${NEXT_LINK}"
mv -Tf -- "${NEXT_LINK}" "${CURRENT}"
NEXT_LINK=''
info "Install complete; current now references immutable release ${release_id}."
info 'No service was enabled, started, restarted, stopped, or reloaded. Old-release cleanup is manual and requires separate authorization.'
