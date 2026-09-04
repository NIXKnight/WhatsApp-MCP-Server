.PHONY: bridge mcp-server install bridge-unit mcp-unit embedder-unit transcriber-unit dashboard-unit whisper-unit workers start stop restart status logs clean deploy deploy-bridge deploy-mcp

REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
SCRIPTS_DIR := $(REPO_ROOT)scripts

## Build and install the Go bridge binary
bridge:
	@bash $(SCRIPTS_DIR)/install-bridge.sh

## Sync the Python MCP server and set up its venv
mcp-server:
	@bash $(SCRIPTS_DIR)/install-mcp-server.sh

## Install both components, create systemd units, and start services
install: bridge mcp-server bridge-unit mcp-unit

## Create user systemd unit for the bridge, reload daemon, and restart service
bridge-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh bridge

## Create user systemd unit for the MCP server, reload daemon, and restart service
mcp-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh mcp-server

## Create user systemd unit for the embedder worker, reload daemon, and restart service
embedder-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh embedder

## Create user systemd unit for the transcriber worker, reload daemon, and restart service
transcriber-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh transcriber

## Create user systemd unit for the dashboard, reload daemon, and restart service
dashboard-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh dashboard

## Create user systemd unit for the whisper.cpp server, reload daemon, and restart service
whisper-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh whisper

## Sync the Python worker venvs (embedder, transcriber, dashboard)
workers:
	cd $(REPO_ROOT)embedder && uv sync
	cd $(REPO_ROOT)transcriber && uv sync
	cd $(REPO_ROOT)dashboard && uv sync

## Start all services (dependency order)
start:
	systemctl --user start whatsapp-bridge.service
	systemctl --user start whatsapp-whisper.service
	systemctl --user start whatsapp-embedder.service
	systemctl --user start whatsapp-mcp-server.service
	systemctl --user start whatsapp-transcriber.service
	systemctl --user start whatsapp-dashboard.service

## Stop all services (reverse dependency order)
stop:
	systemctl --user stop whatsapp-dashboard.service
	systemctl --user stop whatsapp-transcriber.service
	systemctl --user stop whatsapp-embedder.service
	systemctl --user stop whatsapp-whisper.service
	systemctl --user stop whatsapp-mcp-server.service
	systemctl --user stop whatsapp-bridge.service

## Restart all services
restart:
	systemctl --user restart whatsapp-bridge.service
	systemctl --user restart whatsapp-whisper.service
	systemctl --user restart whatsapp-embedder.service
	systemctl --user restart whatsapp-mcp-server.service
	systemctl --user restart whatsapp-transcriber.service
	systemctl --user restart whatsapp-dashboard.service

## Show status of all services
status:
	systemctl --user status whatsapp-bridge.service whatsapp-whisper.service whatsapp-mcp-server.service whatsapp-embedder.service whatsapp-transcriber.service whatsapp-dashboard.service

## Tail logs for all services
logs:
	journalctl --user -f -u whatsapp-bridge.service -u whatsapp-whisper.service -u whatsapp-mcp-server.service -u whatsapp-embedder.service -u whatsapp-transcriber.service -u whatsapp-dashboard.service

## Rebuild and restart the bridge only (build, install binary, restart service)
deploy-bridge: bridge
	systemctl --user restart whatsapp-bridge.service
	@echo "Bridge deployed and restarted."

## Reinstall and restart the MCP server only (sync, update venv)
deploy-mcp: mcp-server
	@echo "MCP server deployed. Restart your Claude Code session or run /mcp to reload."

## Full deploy: rebuild both, restart bridge
deploy: deploy-bridge deploy-mcp

## Remove built artifacts
clean:
	rm -f bridge/whatsapp-bridge
