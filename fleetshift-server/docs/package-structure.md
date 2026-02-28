# Package Organization

Dependencies flow downward only.

## Layers

### Layer 1: Main (`cmd/fleetshift` and `internal/main`)

CLI commands, config serialization, object graph and object lifecycle. No business logic. Thin front to layers 2 and sometimes directly to 3. Initializes objects from layer 5.

Depends on: Transport and Infrastructure, sometimes Application and Domain (when the CLI is acting as "the transport")

### Layer 2: Transport (`internal/transport`)

Wire protocols, serialization, and transport level middleware. gRPC server lives here. No business logic.

- Depends on: Application, Domain

### Layer 3: Application (`internal/application`)

Protocol-agnostic operations using domain value objects. Cross-cutting concerns like observability and transaction boundaries.

- Depends on: Domain

### Layer 4: Domain (`internal/domain`)

Core business logic: graph resolution, usersets, sharding, routing, rebalancing. Defines interfaces for external infrastructure.

- Depends on: nothing (no other layer)

### Layer 5: Infrastructure (`internal/infrastructure/{vendor}`)

Vendor-specific implementations of domain interfaces (e.g., postgres, memory).

- Depends on: Domain

## Rules

- Domain must not import from transport, application, or infrastructure
- Application must not import from transport or infrastructure
- Infrastructure implements domain interfaces; it does not define new shared abstractions (only those it needs internally)
- New external dependencies (databases, services) get their own infrastructure package