.PHONY: help build build-server build-cli test test-server test-cli generate

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: build-server build-cli ## Build all binaries

build-server: ## Build the server binary
	cd fleetshift-server && go build ./...

build-cli: ## Build the fleetctl CLI binary
	cd fleetshift-cli && go build -o ../bin/fleetctl ./cmd/fleetctl

test: test-server test-cli ## Run all tests

test-server: ## Run server tests
	cd fleetshift-server && go test ./...

test-cli: ## Run CLI tests
	cd fleetshift-cli && go test ./...

generate: ## Generate protobuf and gRPC code
	buf generate
