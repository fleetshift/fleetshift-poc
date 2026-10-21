# FleetShift threat and risk register

Scores are preliminary design scores, not measurements. DREAD uses 0–10 and fixes discoverability at 10. Risk is likelihood × impact on a 1–5 scale. Residual scores receive credit only for controls described as existing in the intended architecture; unresolved controls receive no credit.

| ID | Actor / action / goal | STRIDE | Inherent DREAD | Inherent risk | Existing treatment | Residual risk | Status |
| --- | --- | --- | ---: | ---: | --- | ---: | --- |
| T-01 | Malicious tenant submits cross-tenant resource names or queries to read another tenant. | E/I | 8.0 | 20 Critical | RM tenant/workspace authorization; target scope checks. | 12 High | Core control; grant model unresolved |
| T-02 | Compromised RM changes signed content or invents user provenance to alter a target. | T/R/E | 8.8 | 20 Critical | Target-side provenance, attestation, trust history, generation checks. | 8 High | Applies only on verifying paths |
| T-03 | Compromised RM withholds evidence, deliveries, or trust updates. | D | 8.0 | 16 High | `PausedAuth`, retry, local verifier state. | 16 High | Recovery path unresolved |
| T-04 | Network attacker replays a valid delivery or reorders old work. | T/E | 7.2 | 12 High | Signed scope, evidence log, generation fencing, replay checks. | 6 Moderate | Profile-dependent |
| T-05 | Attacker replaces trust configuration or rolls an initialized verifier back to TOFU. | S/T/R/I/E | 7.8 | 16 High | Authenticated trust updates and retained checkpoints. | 8 High | Bootstrap/recovery ceremony required |
| T-06 | Compromised tenant or provider authority issues valid credentials or trust updates. | S/T/R/I/D/E | 8.5 | 20 Critical | Authority scoping, rotation, revocation, policy separation. | 16 High | Root compromise remains root compromise |
| T-07 | Compromised addon returns malicious manifests, schemas, placement, or API behavior. | T/D/E | 7.8 | 16 High | Enable gate, separate process, addon evidence, capability model. | 12 High | UI/schema controls incomplete |
| T-08 | Malicious UI asset executes in the shell and steals tokens or manipulates approval. | S/T/R/I/E | 7.5 | 16 High | Separate signing boundary is designed; plugin has no key API. | 12 High | Asset integrity and isolation unresolved |
| T-09 | Compromised fleetlet or delivery agent uses its target access against other targets or channels. | E/T | 7.4 | 16 High | Channel profiles, local credentials, target-side authorization. | 8 High | Transport identity and profile enforcement open |
| T-10 | Compromised target host extracts credentials or pivots to RM/other targets. | E/I/T/D | 8.1 | 20 Critical | Local credential scope, network separation, no platform-wide account. | 12 High | Deployment-dependent |
| T-11 | Malicious tenant floods APIs, deliveries, inventory, or target resources. | D/E | 8.0 | 16 High | Backpressure and rollout controls are described. | 16 High | Quotas and budgets unresolved |
| T-12 | Stale or partial apply overwrites newer target state after retry or crash. | T/R/I | 7.0 | 12 High | Generation, durable work records, idempotent apply. | 4 Low | Requires conforming agent |
| T-13 | Deletion evidence is replayed to recreate a resource. | T/R/E | 6.8 | 12 High | Signed creation nonce/tombstone is proposed. | 8 High | Retention protocol unresolved |
| T-14 | Inventory or alias reports cause cross-tenant disclosure or identity confusion. | T/I | 7.0 | 12 High | Query RBAC and two-layer identity model. | 8 High | Report provenance and alias reconciliation open |
| T-15 | Factory provider uses a standing credential or protocol tunnel to bypass consumer intent. | E/T | 8.4 | 20 Critical | Restricted factory profile, signed intent, optional buffer, vault proposal. | 12 High | Several controls are proposals |
| T-16 | Break-glass access remains active or exceeds approved scope. | E/I/T | 6.8 | 12 High | Signed approval, target/scope binding, expiry/revocation proposed. | 8 High | Enforcement details unresolved |
| T-17 | Dynamic schema or service registration routes requests to attacker-controlled code. | S/E | 7.5 | 16 High | Content hashing and atomic replacement; enable/connect lifecycle. | 12 High | Registration authentication and schema policy open |
| T-18 | Build, registry, dependency, or release compromise ships a malicious FleetShift component. | T/E | 8.2 | 20 Critical | CodeQL and tests; provenance/SBOM/signing practices not established. | 20 Critical | OpenSSF SDLC gap |
| T19 | Compromised RM may remove or change evidence stored in DB | T/R | 7.8 | 10 | not accounted for in design docs | 10 | unresolved (a solution could be to export to external store to persist audit logs)|
| T20 | Malicious tenant submits cross-tenant resource mutation requests for another tenant. | T/E/D | 6.4 | 10 | provenance verification or credential presentation | 5 | Depends on the trust model |

## Score interpretation

- **Critical:** immediate design or operational blocker for the affected deployment.
- **High:** requires explicit owner, control, and deployment decision.
- **Moderate:** control should be tested and monitored.
- **Low:** ordinary correctness or bounded operational risk.

## Highest priorities for review

1. T-01/T-14: tenant isolation and inventory identity.
2. T-03/T-04: availability, replay, and recovery under RM compromise.
3. T-07/T-08/T-17: addon, UI, and dynamic-code trust boundaries.
4. T-11: quotas and cross-tenant resource exhaustion.
5. T-15/T-16: factory and break-glass paths.
6. T-18: release and dependency trust.
