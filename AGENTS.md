# Rules for Agents

This is the monorepo for FleetShift, a fleet management platform. It contains the management-plane server, CLI, OCP engine, end-to-end tests, proto definitions, and proof-of-concept experiments.

## Understanding the domain

The architecture documentation lives in docs/design/ and is the primary source of truth for domain concepts, system design, and open questions.

- Start with docs/design/architecture.md -- it gives the system's core mental model, names the major subsystems, and contains a reading guide that routes to the detailed sub-documents in docs/design/architecture/:
  - core_model.md -- core vocabulary, strategy axes, fulfillment kernel primitive, target model, delivery contract, single-pod invariant
  - orchestration.md -- fulfillment execution, re-evaluation, rollout
  - fleetlet_and_transport.md -- fleetlets, channels, proxying, routing, data paths
  - tenancy_and_permissions.md -- provider/tenant/workspace model, generic permission boundary
  - addon_integration.md -- capability registration, addon strategy contracts, managed-resource bridging, UI/API extensions
  - resource_indexing.md -- fleet-wide indexing and search
  - platform_hierarchy.md -- recursive platforms, federation, provisioning, bootstrap, pivot
  - open_questions.md -- unresolved design areas
- For authentication and delivery authorization, see docs/design/authentication.md and poc/attestation/hybrid/README.md
- For managed resources, see docs/design/managed_resources.md

## Designing and defining APIs

- For gRPC / proto generation and linting, see docs/buf.md
- For API design conventions (AIP-aligned), see docs/api-design.md

## Cross-cutting concerns

- Never remove comments unless they are truly no longer relevant (e.g. a TODO that is now implemented or obsolete). Prefer updating out of date explanations unless new behavior is trivially obvious.
- Prefer modern stdlib abstractions and utilities where relevant (especially around crypto or low level encoding / decoding)
- Follow test-driven development. When at all possible, write failing tests **first**, then write the code to make the test pass.
- See the Taskfile (used w/ `task` cli) for common development tasks like running tests, generating proto, and building binaries (`task -l` for available tasks)

## fleetshift-server

For how to...

- decide what logic to put where, see fleetshift-server/docs/internal-architecture.md
- implement instrumentation (observability: tracing, logging, metrics), see fleetshift-server/docs/observer-pattern.md
- decide what package to use, see fleetshift-server/docs/package-structure.md
- write tests, see fleetshift-server/docs/testing.md
- write durable workflows and integrate with durable computing libraries, see fleetshift-server/docs/durable-workflows.md
- write or modify constructors, see fleetshift-server/docs/constructors.md

When running tests, iterate using `-short` tests, but always do at least one final check with the full suite even if some tests require containers.
