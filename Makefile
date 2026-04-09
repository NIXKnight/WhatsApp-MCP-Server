.PHONY: bridge mcp-server install bridge-unit mcp-unit start stop restart status logs clean deploy deploy-bridge deploy-mcp

SCRIPTS_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))scripts

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

## Start both services
start:
	systemctl --user start whatsapp-bridge.service
	systemctl --user start whatsapp-mcp-server.service

## Stop both services
stop:
	systemctl --user stop whatsapp-mcp-server.service
	systemctl --user stop whatsapp-bridge.service

## Restart both services
restart:
	systemctl --user restart whatsapp-bridge.service
	systemctl --user restart whatsapp-mcp-server.service

## Show status of both services
status:
	systemctl --user status whatsapp-bridge.service whatsapp-mcp-server.service

## Tail logs for both services
logs:
	journalctl --user -f -u whatsapp-bridge.service -u whatsapp-mcp-server.service

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
