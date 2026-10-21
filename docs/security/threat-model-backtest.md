# Threat-model backtest

**Status:** Pending maintainer corpus

No historical security-report corpus was available for this draft. The current backtest therefore uses synthesized cases only and must not be treated as evidence of historical routing quality.

## Synthesized cases

| Case | Expected route | Reason |
| --- | --- | --- |
| RM alters a user-signed manifest on a provenance-verifying target | `VALID` | Violates P-PROV. |
| RM withholds all deliveries | `BY-DESIGN: property-disclaimed` (escalated) | Matches D-RM-DOS, but strict policy requires maintainer confirmation. |
| Credential-only target accepts RM-fabricated content | `BY-DESIGN: property-disclaimed` (escalated) | Matches D-CRED-ONLY; security-critical disclaimer cannot close from this draft. |
| Tenant query returns another tenant's resource | `VALID` | Violates P-AUTHZ if the attacker is an in-scope tenant user. |
| Target host root changes its own applied resources | `OUT-OF-MODEL: adversary-not-in-scope` (escalated) | Target-root compromise is explicitly outside target self-integrity. |
| Addon UI steals a signing key through a general signing API | `VALID` | Violates P-CAP and the controlled-client boundary. |
| OOM from tenant flood with no quota configured | `MODEL-GAP` | Q1 must define the resource and quota contract. |
| Dependency violates its own documented contract while FleetShift uses it correctly | `OUT-OF-MODEL: dependency-contract` | Forward to the dependency; retain FleetShift usage analysis. |

## Required next step

Collect de-identified historical reports, fixes, scanner findings, and POC failures. Stratify them across tenant authorization, provenance, trust updates, delivery ordering, addons, transport, inventory, provider/factory, and release security. Re-run every affected case before accepting the model.
