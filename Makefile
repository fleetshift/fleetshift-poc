.PHONY: help build build-server build-cli build-ocp-engine test test-server test-cli test-ocp-engine fmt generate

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

fmt: ## Format all Go source files
	cd fleetshift-server && gofmt -w .
	cd fleetshift-cli && gofmt -w .
	cd ocp-engine && gofmt -w .

build: fmt build-server build-cli build-ocp-engine ## Build all binaries

build-server: fmt ## Build the server binary
	cd fleetshift-server && go build -o ../bin/fleetshift ./cmd/fleetshift

build-cli: fmt ## Build the fleetctl CLI binary
	cd fleetshift-cli && go build -o ../bin/fleetctl ./cmd/fleetctl

build-ocp-engine: fmt ## Build the ocp-engine binary
	cd ocp-engine && go build -o ../bin/ocp-engine .

test: fmt test-server test-cli test-ocp-engine ## Run all tests

test-server: ## Run server tests
	cd fleetshift-server && go test ./...

test-cli: ## Run CLI tests
	cd fleetshift-cli && go test ./...

test-ocp-engine: ## Run ocp-engine tests
	cd ocp-engine && go test ./...

generate: ## Generate protobuf and gRPC code
	buf generate
