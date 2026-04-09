#!/usr/bin/env bash
set -euo pipefail

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
ENV_DIR="${HOME}/.config/whatsapp-bridge"
ENV_FILE="${ENV_DIR}/env"

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
# Create environment file template (only if absent)
# -------------------------------------------------------------------
if [[ ! -f "${ENV_FILE}" ]]; then
  mkdir -p "${ENV_DIR}"
  cat > "${ENV_FILE}" <<'EOF'
# WhatsApp Bridge environment configuration
# Fill in DATABASE_URL before starting the service.
# Example: DATABASE_URL=postgresql://user:password@localhost/whatsapp

DATABASE_URL=
EOF
  warn "Created environment file template: ${ENV_FILE}"
  warn "  --> Set DATABASE_URL in that file before starting the bridge."
else
  info "Environment file already exists: ${ENV_FILE} (not overwritten)"
fi

info "install-bridge complete."
