.PHONY: help up down clean restart-% logs logs-% status reset-keycloak

SCRIPTS_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))scripts
COMPOSE := podman compose -f $(dir $(abspath $(lastword $(MAKEFILE_LIST))))docker-compose.yml

help: ## Show this help
	@grep -E '^[a-zA-Z_%-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

up: ## Start the full FleetShift stack
	@chmod +x $(SCRIPTS_DIR)/*.sh
	$(SCRIPTS_DIR)/start.sh

down: ## Stop all containers, preserve data
	@chmod +x $(SCRIPTS_DIR)/*.sh
	$(SCRIPTS_DIR)/stop.sh

clean: ## Stop all containers and remove ALL data (volumes, network)
	@chmod +x $(SCRIPTS_DIR)/*.sh
	$(SCRIPTS_DIR)/stop.sh --clean

restart-%: ## Restart a single container (e.g., make restart-fleetshift-mock-servers)
	$(COMPOSE) restart $*

logs: ## Tail logs from all FleetShift containers
	$(COMPOSE) logs -f

logs-%: ## Tail logs from a single container (e.g., make logs-fleetshift-server)
	$(COMPOSE) logs -f $*

status: ## Show running FleetShift containers
	$(COMPOSE) ps

reset-keycloak: ## Wipe Keycloak state and re-import realm (generates new passwords)
	$(COMPOSE) --profile demo stop keycloak
	$(COMPOSE) --profile demo rm -f keycloak
	podman volume rm fleetshift_keycloak-certs 2>/dev/null || podman volume rm keycloak-certs 2>/dev/null || true
	rm -f $(dir $(abspath $(lastword $(MAKEFILE_LIST)))).realm.json
	@echo "Keycloak state wiped. Run 'make up' to restart with fresh realm and passwords."
