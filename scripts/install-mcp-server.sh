#!/usr/bin/env bash
set -euo pipefail

# Color helpers
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()    { echo -e "${GREEN}[install-mcp-server]${NC} $*"; }
warn()    { echo -e "${YELLOW}[install-mcp-server]${NC} $*"; }
error()   { echo -e "${RED}[install-mcp-server]${NC} $*" >&2; }

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SRC_DIR="${REPO_ROOT}/mcp-server"
DEST_DIR="${HOME}/.local/share/whatsapp-mcp-server"
VENV_DIR="${HOME}/.local/share/whatsapp-mcp-server-venv"

# -------------------------------------------------------------------
# Sync source tree
# -------------------------------------------------------------------
info "Syncing mcp-server/ -> ${DEST_DIR} ..."
mkdir -p "${DEST_DIR}"
rsync -a --delete \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='*.pyo' \
  --exclude='.git' \
  --exclude='.gitignore' \
  --exclude='.venv' \
  --exclude='*.egg-info' \
  --exclude='dist/' \
  --exclude='build/' \
  "${SRC_DIR}/" "${DEST_DIR}/"
info "Sync complete."

# -------------------------------------------------------------------
# Create venv (idempotent)
# -------------------------------------------------------------------
if [[ ! -d "${VENV_DIR}" ]]; then
  info "Creating Python venv at ${VENV_DIR} ..."
  python3 -m venv "${VENV_DIR}"
  info "Venv created."
else
  info "Venv already exists: ${VENV_DIR}"
fi

# -------------------------------------------------------------------
# Install / update package inside venv
# -------------------------------------------------------------------
info "Installing/updating whatsapp-mcp package in venv..."
# shellcheck source=/dev/null
source "${VENV_DIR}/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -e "${DEST_DIR}"
deactivate
info "Package installed. Entry point: ${VENV_DIR}/bin/whatsapp-mcp"

info "install-mcp-server complete."
