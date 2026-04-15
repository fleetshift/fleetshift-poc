.PHONY: help build build-server build-cli build-ocp-engine test test-server test-cli test-ocp-engine test-e2e generate

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: build-server build-cli build-ocp-engine ## Build all binaries

build-server: ## Build the server binary
	cd fleetshift-server && go build -o ../bin/fleetshift ./cmd/fleetshift

build-cli: ## Build the fleetctl CLI binary
	cd fleetshift-cli && go build -o ../bin/fleetctl ./cmd/fleetctl

build-ocp-engine: ## Build the ocp-engine binary
	cd ocp-engine && go build -o ../bin/ocp-engine .

test: test-server test-cli test-ocp-engine ## Run all tests

test-server: ## Run server tests
	cd fleetshift-server && go test ./...

test-cli: ## Run CLI tests
	cd fleetshift-cli && go test ./...

test-ocp-engine: ## Run ocp-engine tests
	cd ocp-engine && go test ./...

test-e2e: build ## Run E2E tests (requires .env config and interactive auth)
	@eval "$$(dbus-launch --sh-syntax)" && \
		echo "" | gnome-keyring-daemon --unlock --components=secrets >/dev/null 2>&1 && \
		cd e2e && go test -tags e2e -timeout 3h -v

generate: ## Generate protobuf and gRPC code
	buf generate --path proto/fleetshift
	buf generate --template buf.gen.ocp.yaml --path proto/ocp/v1
