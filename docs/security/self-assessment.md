# FleetShift security self-assessment

**Status:** Incomplete draft  
**Modeled basis:** intended architecture at commit `93c8da00129ade559e476177f33c40b2231f0b5f`  
**Purpose:** review FleetShift's security posture and identify work. This is not an audit or attestation.

## Metadata

| Field | Value |
| --- | --- |
| Software | [FleetShift repository](https://github.com/sherine-k/fleetshift-poc) |
| Security provider | Yes. FleetShift is intended to protect managed infrastructure. |
| Languages | Go, TypeScript, JavaScript, Protocol Buffers |
| SBOM | Not published for this draft |
| Compliance | No compliance certification claimed |

## Security links

- [Architecture](../design/architecture.md)
- [Delivery security](../design/security.md)
- [Authentication](../design/authentication.md)
- [Provenance](../design/architecture/provenance.md)
- [Trust model v3](../design/trust_model_v3.md)
- [OpenSSF self-assessment guide](https://security-assessments.openssf.org/self-assessment/getting-started-self-assessment/)
- [CNSC catalog](https://grc.store/cncf-tag-sc/cnsc)

## Overview

FleetShift is a multi-tenant management plane for declaring, delivering, and observing infrastructure changes across many targets. It supports user and workload identity, addons, target-side delivery agents, recursive platforms, and provider/factory deployments.

## Actors

| Actor | Role and boundary |
| --- | --- |
| User or automation | Creates requests and may sign intent. It is outside the platform process. |
| Identity/provenance authority | Authenticates principals and issues or validates evidence. It is an external trust root. |
| Resource manager | Stores state, authorizes API requests, orchestrates work, and transports evidence. It is the primary platform authority, but not the sole provenance authority. |
| Addon | Provides strategy, API, UI, inventory, or delivery capabilities. It is a separate trust boundary when deployed out of process. |
| Fleetlet/transport | Routes channels between the platform and local agents. It carries privileged delivery and proxy traffic. |
| Delivery agent/verifier | Validates credentials, provenance, scope, ordering, and generation before applying work. |
| Target | Kubernetes, cloud, platform, or addon-defined infrastructure where effects occur. |
| Browser shell/UI plugin | Presents data and actions to users. A plugin is not trusted with signing keys or unrestricted shell authority. |
| Operator/trust administrator | Bootstraps and changes provider, tenant, addon, and trust configuration. |

Isolation is provided by authentication, tenant/workspace authorization, process boundaries, target-local credentials, authenticated provenance, channel profiles, and target-side verification. Exact non-authorization isolation between tenants remains unresolved.

## Actions

1. Bootstrap a platform and establish trust configuration.
2. Authenticate a caller and evaluate tenant/workspace permissions.
3. Create or update a deployment or managed resource.
4. Sign, store, derive, and deliver typed intent and supporting evidence.
5. Resolve placement, generate manifests, plan rollout, and apply changes.
6. Report target health and inventory, then serve scoped queries.
7. Rotate, revoke, recover, or update credentials and trust configuration.
8. Install addons, activate dynamic APIs, and load UI assets.
9. Provision child platforms and factory infrastructure.
10. Provide approved break-glass access where a target profile permits it.

## Security goals

- Prevent cross-tenant authorization bypass in an honest deployment.
- Bind actions to authenticated principals, tenant partitions, targets, purposes, and generations.
- Prevent a compromised resource manager from forging or altering verified provenance.
- Prevent rollback of established verifier state and stale delivery over newer work.
- Keep target credentials local or narrowly scoped where the selected mode permits it.
- Confine addons and UI plugins to their declared capabilities.
- Limit compromise of one target, addon, or tenant from spreading laterally.
- Make delivery retryable, idempotent, and crash-safe.
- Preserve useful attribution and audit evidence.
- Continue safe target operation, or fail closed, during management-plane failure.

## Non-goals

- Confidentiality of platform-held state after resource-manager and database compromise.
- Integrity or completeness of ordinary reads, inventory, and query results after resource-manager compromise.
- Reproducing all resource-manager policy at every target.
- Protecting a target after its host or verifier is compromised.
- Global fork detection or trusted wall-clock time in the continuity/v3 baseline.
- Protecting direct mutation paths that bypass the delivery verifier.
- Preventing a trusted authority from using authority intentionally granted to it.
- Treating credential-only delivery as protected against a compromised courier.

## Critical security components

- Resource-manager authorization and tenant/workspace boundary.
- Credential verification and credential presentation.
- Typed provenance and attestation verification.
- Authenticated trust configuration and verifier-retained state.
- Fleetlet channel authorization and target profiles.
- Delivery-agent generation, replay, and durability checks.
- Target-native authorization and apply controls.

## Security-relevant features

- `PausedAuth` for repairable credential and trust failures.
- Scoped delegation and short-lived credentials.
- Addon enable/connect authorization and capability registration.
- Dynamic API schema activation.
- Inventory scope and query authorization.
- Rollout, generation, idempotency, and journaling behavior.
- Break-glass approval and expiry.
- Logging, monitoring, backups, and recovery procedures.

## Development and support

The repository has Go unit tests, selected Docker-based integration tests, Kubernetes race tests, deploy-script tests, and CodeQL for Go. The UI has lint and Vitest commands. This draft does not claim signed releases, published SBOMs, reproducible builds, complete fuzz coverage, or a mature incident-response process. Those are assessment gaps.

Security reports should use the repository's private security-reporting path when one is configured. The response owner, notification timeline, embargo policy, and patch process need to be documented before this assessment is complete.

## Appendix

- Known issue history: not assembled for this draft.
- OpenSSF Scorecard: not recorded for this draft.
- Case studies: local/sandbox fleet, shared multi-tenant fleet, and provider/factory fleet are the intended scenarios.
- Related guidance: OpenSSF self-assessment, STRIDE/DREAD, CNSC, and Gemara threat catalogs.
