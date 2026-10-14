# FleetShift Documentation

## What is FleetShift

FleetShift is a multi-cluster management UI that dynamically composes its interface based on the capabilities of connected Kubernetes and OpenShift clusters.

## Goals

- **Dynamic UI composition** — the UI discovers what each cluster supports and loads only the relevant plugin UIs. Adding a new cluster with new capabilities should automatically surface new UI sections without code changes to the shell.
- **Graceful growth** — the system must work for 1 cluster and scale gracefully as the number of integrated clusters grows to tens or hundreds. The architecture (plugin loading, data fetching, navigation) should not degrade as clusters are added.
- **Plugin-based extensibility** — all cluster-specific UI is delivered through plugins loaded at runtime via Module Federation. The shell is generic; plugins bring the domain knowledge.
- **Multi-platform support** — must work with both vanilla Kubernetes and OpenShift clusters, detecting platform-specific capabilities (e.g. ConsolePlugins, Routes) where available.

## Design

- [Feature Contracts](./feature-contracts.md) — extension types, search indexing, feature graph
- [Module Groups](./module-groups.md) — nested navigation, hierarchical search, grouped routes
- [Extension Validation](./extension-validation.md) — runtime trust boundary for cross-plugin extensions

## Non-Goals

- **Authentication and authorization** — handled elsewhere in the platform, not a concern of this project. Any auth-related code in this repository (mock login, user switching, bearer tokens) exists purely for experimentation purposes and should not be taken as a design choice or requirement.
- **API aggregation** — also handled elsewhere in the platform. This project focuses exclusively on the UI layer. Any API proxy or data-fetching code in this repository exists purely for experimentation purposes to make the UI functional during development and should not be taken as a design choice or requirement.

## Requirements

- [Plugin Discovery](./plugin-discovery.md) — how FleetShift detects which UI plugins to enable for each cluster
- [OCP Plugin Re-use](./ocp-plugin-reuse.md) — priority: re-using existing OpenShift console plugins in FleetShift
- [Plugin Versioning](./plugin-versioning.md) — loading multiple versions of the same plugin from different clusters
- [API Requirements](./api-requirements.md) — aggregate data layer and per-cluster drill-down proxy

## Experimental

- [Passkey & Grant Access](./passkey-grant-access.md) — WebAuthn-based identity verification flow
