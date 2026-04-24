.PHONY: help up down clean restart-% logs logs-% status cli-setup reset-keycloak

SCRIPTS_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))scripts
DEPLOY_DIR := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST))))/..)

help: ## Show this help
	@grep -E '^[a-zA-Z_%-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

up: ## Start the FleetShift stack (PROFILE=demo|prod DB=sqlite|postgres AUTH=local|external)
	$(SCRIPTS_DIR)/start.sh

down: ## Stop all containers, preserve data
	$(SCRIPTS_DIR)/stop.sh

clean: ## Stop all containers and remove ALL data (volumes, network)
	$(SCRIPTS_DIR)/stop.sh --clean

restart-%: ## Restart a single container (e.g., make restart-fleetshift-mock-servers)
	@bash -c 'source $(SCRIPTS_DIR)/common.sh && load_env && resolve_profile && compose restart $*'

logs: ## Tail logs from all FleetShift containers
	@bash -c 'source $(SCRIPTS_DIR)/common.sh && load_env && resolve_profile && compose logs -f'

logs-%: ## Tail logs from a single container (e.g., make logs-fleetshift-server)
	@bash -c 'source $(SCRIPTS_DIR)/common.sh && load_env && resolve_profile && compose logs -f $*'

status: ## Show running FleetShift containers
	@bash -c 'source $(SCRIPTS_DIR)/common.sh && load_env && resolve_profile && compose ps'

cli-setup: ## Configure fleetctl CLI for local deployment
	$(SCRIPTS_DIR)/cli-setup.sh

reset-keycloak: ## Wipe Keycloak state and re-import realm (generates new passwords)
	@bash -c 'source $(SCRIPTS_DIR)/common.sh && load_env && resolve_profile && compose stop keycloak'
	@bash -c 'source $(SCRIPTS_DIR)/common.sh && load_env && resolve_profile && compose rm -f keycloak'
	podman volume rm fleetshift_keycloak-certs 2>/dev/null || podman volume rm keycloak-certs 2>/dev/null || true
	rm -f $(dir $(abspath $(lastword $(MAKEFILE_LIST)))).realm.json
	@echo "Keycloak state wiped. Run 'make up' to restart with fresh realm and passwords."
