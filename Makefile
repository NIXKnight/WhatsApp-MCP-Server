.PHONY: bridge mcp-server session-controller workers install setup bridge-unit mcp-unit controller-unit redis-unit embedder-unit transcriber-unit dashboard-unit whisper-unit reload-units start stop restart status logs start-whisper stop-whisper status-whisper logs-whisper clean deploy deploy-bridge deploy-mcp deploy-controller

REPO_ROOT := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
SCRIPTS_DIR := $(REPO_ROOT)scripts

bridge:
	@bash $(SCRIPTS_DIR)/install-bridge.sh
mcp-server:
	@bash $(SCRIPTS_DIR)/install-mcp-server.sh
session-controller:
	@bash $(SCRIPTS_DIR)/install-session-controller.sh
workers:
	cd $(REPO_ROOT)embedder && uv sync
	cd $(REPO_ROOT)transcriber && uv sync
	cd $(REPO_ROOT)dashboard && uv sync

## Idempotent artifact installation only; preserve operator state/configuration.
## Never enable, start, restart, stop, or reload a service.
install: bridge mcp-server session-controller workers
## Idempotent unit generation plus temporary static verification only.
## Never enable, start, restart, stop, or daemon-reload a service.
setup: redis-unit controller-unit bridge-unit mcp-unit embedder-unit transcriber-unit dashboard-unit

bridge-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh bridge
mcp-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh mcp-server
controller-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh session-controller
redis-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh redis
embedder-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh embedder
transcriber-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh transcriber
dashboard-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh dashboard
whisper-unit:
	@bash $(SCRIPTS_DIR)/setup-systemd.sh whisper

## Explicit operator action after reviewing newly generated units; no activation.
reload-units:
	systemctl --user daemon-reload

## Explicitly authorized activation only; dependency order is Redis/controller before bridge.
## The optional Whisper unit is deliberately excluded from the default stack.
start:
	systemctl --user start whatsapp-session-redis.service
	systemctl --user start whatsapp-session-controller.service
	systemctl --user start whatsapp-bridge.service
	systemctl --user start whatsapp-embedder.service
	systemctl --user start whatsapp-mcp-server.service
	systemctl --user start whatsapp-transcriber.service
	systemctl --user start whatsapp-dashboard.service

## Controller first closes intake and drains active Pi turns indefinitely while bridge remains available.
stop:
	systemctl --user stop whatsapp-session-controller.service
	systemctl --user stop whatsapp-dashboard.service whatsapp-transcriber.service whatsapp-embedder.service whatsapp-mcp-server.service
	systemctl --user stop whatsapp-bridge.service
	systemctl --user stop whatsapp-session-redis.service

restart:
	+$(MAKE) stop
	+$(MAKE) start

status:
	systemctl --user status whatsapp-session-redis.service whatsapp-session-controller.service whatsapp-bridge.service whatsapp-mcp-server.service whatsapp-embedder.service whatsapp-transcriber.service whatsapp-dashboard.service
logs:
	journalctl --user -f -u whatsapp-session-redis.service -u whatsapp-session-controller.service -u whatsapp-bridge.service -u whatsapp-mcp-server.service -u whatsapp-embedder.service -u whatsapp-transcriber.service -u whatsapp-dashboard.service

## Optional Whisper lifecycle is always separate and explicitly authorized.
start-whisper:
	systemctl --user start whatsapp-whisper.service
stop-whisper:
	systemctl --user stop whatsapp-whisper.service
status-whisper:
	systemctl --user status whatsapp-whisper.service
logs-whisper:
	journalctl --user -f -u whatsapp-whisper.service

deploy-bridge: bridge
	@echo "Bridge installed. Restart requires separate explicit lifecycle action."
deploy-mcp: mcp-server
	@echo "MCP server installed. Restart/reload requires separate explicit lifecycle action."
deploy-controller: session-controller
	@echo "Session controller installed. Restart requires separate explicit lifecycle action."
## Deployment installs artifacts only; activation remains an explicit lifecycle action.
deploy: deploy-bridge deploy-mcp deploy-controller
clean:
	rm -f bridge/whatsapp-bridge
	rm -rf session-controller/dist
