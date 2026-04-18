.PHONY: help up down restart-% logs logs-% status

SCRIPTS_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))scripts

help: ## Show this help
	@grep -E '^[a-zA-Z_%-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

up: ## Start the full FleetShift stack
	$(SCRIPTS_DIR)/start.sh

down: ## Stop all containers (add --clean to remove volumes)
	$(SCRIPTS_DIR)/stop.sh $(ARGS)

restart-%: ## Restart a single container (e.g., make restart-fleetshift-mock-servers)
	podman restart $*

logs: ## Tail logs from all FleetShift containers
	podman logs -f fleetshift-server &
	podman logs -f fleetshift-mock-servers &
	podman logs -f fleetshift-mock-ui-plugins &
	podman logs -f fleetshift-gui &
	wait

logs-%: ## Tail logs from a single container (e.g., make logs-fleetshift-server)
	podman logs -f $*

status: ## Show running FleetShift containers
	@podman ps --filter network=fleetshift --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
