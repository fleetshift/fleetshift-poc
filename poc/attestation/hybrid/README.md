# Hybrid attestation prototype

This directory is a Python proof of concept for the provenance and verification side of the authentication model in `docs/design/authentication.md`.

It is trying to answer one narrow question:

> If the management plane is only a courier, what evidence does a target need in order to accept a delivery, an update, or a removal?

The answer explored here is a prototype that combines:

- a user signs the input content (a deployment spec, a managed resource spec, or future content types)
- addons may sign opaque outputs such as rendered manifests, placement decisions, or update plans
- the target verifies the whole chain locally from a self-contained bundle plus external trust anchors

The end design document is broader than this prototype. It also covers credential presentation, transport choices, workflow states like `PausedAuth`, and operational trust distribution. This prototype mostly isolates the target-side verification core.

## What this prototype combines

At the code structure level, this directory combines a few ideas in one model:

- a single `Attestation(input, output)` shape
- content-type polymorphism: `SignedInput.content` is a typed union (`DeploymentContent | ManagedResourceContent | ...`) via the `InputContent` protocol
- explicit, signed CEL output constraints on inputs
- content-implied constraints derived from the signed input content (strategy-based for deployments, relation-based for managed resources)
- self-contained verification bundles instead of verifier-side history lookup
- data-driven input derivation from signed update outputs
- delivery-aware output types for put and remove operations
- explainable verification results instead of a bare pass/fail

### Content types

The `InputContent` protocol defines a common interface (`to_dict`, `content_id`, `content_type`) that all content variants implement. The verification pipeline works generically through this protocol; constraint derivation, identity extraction, and update mutation dispatch on the concrete type.

**`DeploymentContent`** carries deployment identity plus manifest and placement strategy specs. Built-in to the platform.

**`ManagedResourceContent`** carries a resource spec and addon reference (`addon_id`). The user signs the "what" and the "who" -- not the "how" or the trust path. The fulfillment relation -- addon-signed evidence describing how the resource maps to a fulfillment -- is external evidence in the `VerificationBundle`, not part of what the user signs. Relation types are platform-defined (verifiers have built-in logic for each), so they use strong typing. The first relation type is `RegisteredSelfTarget` (1:1 delivery to the addon itself).

### Deployment strategies

| Strategy | What it means |
| --- | --- |
| manifest `inline` | The signed input already contains the manifests. The delivered manifests must match exactly. |
| manifest `addon` | The manifests are opaque to the target. The expected addon must sign them. |
| placement `predicate` | The target self-assesses placement by evaluating a CEL predicate against its own identity. |
| placement `addon` | A placement addon signs the allowed target list. |

### Managed resource fulfillment relations

| Relation | What it means |
| --- | --- |
| `RegisteredSelfTarget` | 1:1 manifest delivery to the addon itself. Implies placement = static to addon, manifests must match the user's signed spec. |

## Core ideas

### 1. A delivery is verified as `input -> output`

The main object is `Attestation`, which pairs:

- an `Input` (whose content is a typed union via `InputContent`)
- a concrete delivery action (`PutManifests` or `RemoveByDeploymentId`)

The target does not trust the output on its own. It verifies that the output is justified by the input.

### 2. The input carries signed policy, not just content

`SignedInput` signs an envelope containing:

- the typed `InputContent` (e.g. `DeploymentContent` or `ManagedResourceContent`)
- `valid_until`
- explicit output constraints
- optional `expected_generation`

This is important: the signer is not only authorizing "this content exists". They are also authorizing the rules the eventual output must satisfy. The content type serves as its own discriminator -- no separate `kind` field is needed.

Those explicit rules are CEL expressions (`OutputConstraint`) evaluated at verification time over a context containing:

- `input`
- `output`
- `target`
- `action`
- `placement`

### 3. Content implies policy

In addition to explicitly signed CEL constraints, the verifier derives built-in constraints from the signed content in `policy.py`. Constraint derivation dispatches on the content type:

**Deployments** (`DeploymentContent`):

- inline manifests imply `output.manifests == input.manifest_strategy.manifests`
- addon manifests imply "the output must be signed by addon X"
- predicate placement implies puts are allowed only when the target matches, and removals only when it no longer matches
- addon placement implies the placement evidence must be signed, and the current target must be consistent with the signed target list

**Managed resources** (`ManagedResourceContent`):

- the verifier looks up the fulfillment relation from the `VerificationBundle` by `(addon_id, resource_type)`
- it verifies the relation cryptographically and against the `TrustStore` (signer key recognised by the claimed anchor)
- `RegisteredSelfTarget` implies placement is static to the addon and manifests must match the user's signed spec (like inline for deployments -- the content is deterministic)

This is one of the most important ideas in the prototype: the signed input is not just data, it is a compact declaration of how verification must work.

### 4. Updates are first-class attestations

`DerivedInput` models a new content version reconstructed from:

- a prior input
- an update attestation whose output contains a signed `spec_update`

The `spec_update` carries:

- a `derive_input_expression` that transforms the prior input
- optional preconditions
- optional additional output constraints

This lets the verifier rebuild content history from signed material instead of trusting the platform's stored state. `DerivedInput` carries `prior_content_id` and `prior_content_type` to unambiguously identify the prior content being updated. Reconstitution dispatches on the prior content's concrete type.

An important detail: update attestations are themselves deployments. That means update planning can have its own manifest strategy, placement strategy, and addon signatures. In the upgrade scenarios, the update planner signs the patch, and a placement addon signs which content IDs the patch is allowed to apply to.

### 5. Trust is split between users and addons

`TrustAnchor` and `TrustStore` model external trust roots.

In the tests, there are usually at least two anchors:

- a tenant/user anchor, standing in for an external user identity system
- an addon anchor, standing in for addon-local trust

That matches the design goal in `authentication.md`: the platform is not the trust root. The verifier relies on out-of-band anchors.

### 6. Verification is meant to be explainable

`verify.py` returns a tree of `VerificationResult` nodes. The verifier is not just answering yes/no; it can explain:

- which signer was accepted
- which constraint failed
- which derived step failed
- whether the failure was in trust, derivation, placement, expiry, or generation

That makes the model useful for reasoning, not just pass/fail testing.

## Typical flow

The most representative scenario in this directory is the fleet upgrade flow in `test_delivery.py`:

1. Alice signs a base deployment input for a cluster.
2. The signed input says manifests come from `capi-provisioner`.
3. Bob signs an upgrade request whose output is a signed `spec_update` from `upgrade-planner`.
4. A placement addon signs which deployment IDs that upgrade is allowed to touch.
5. The final target manifests are signed by `capi-provisioner`.
6. The target verifies the whole chain before accepting the new version.

In other words, the target can validate:

- who authorized the original deployment
- who authorized the update
- who generated the patch
- who generated the final manifests
- whether placement still allows it
- whether the final output still satisfies all inherited constraints

## What the tests are proving

The tests cover a fairly wide slice of the design space:

- inline outputs cannot be tampered with or swapped between attestations
- addon outputs must be signed by the expected addon (trust anchor is verified structurally, not by user constraint)
- placement can be self-assessed (`predicate`) or externally decided (`addon`)
- removals are verified too; they are not trusted just because they are deletions
- derived inputs inherit prior constraints and can add new ones
- signed preconditions make update ordering meaningful
- unknown strategy types fail closed
- forged keys, forged bindings, replayed outputs, and cross-deployment evidence are rejected
- optional `expected_generation` gives a simple target-side stale-write check
- managed resources are verified through the same pipeline as deployments with content-type dispatch
- fulfillment relation evidence is external (in the bundle), looked up by `(addon_id, resource_type)`, and verified against the trust store
- missing relation evidence fails closed
- managed resource and deployment attestations coexist in the same bundle

## Mapping to `docs/design/authentication.md`

This prototype is closest to the provenance half of the design.

### Directly aligned

- Every accepted delivery is justified by signed history.
- Verification happens at the target, not just in the platform.
- Addon-produced opaque artifacts can be independently trusted.
- Trust anchors are external to the platform.
- The platform can act as a courier for verification material instead of a standing authority.

### Intentionally simplified

- `KeyBinding` is only a proof-of-possession binding over a raw public key. It is not yet the real OIDC/JWT-backed binding bundle described in the design doc.
- There is no modeling of credential presentation yet:
  - run as me
  - run as workload
  - run as platform
- There is no transport model yet (`gRPC`, buffered delivery, CRD transport, etc.).
- There is no workflow state like `PausedAuth`; verification simply accepts or rejects.
- There is no real registry, discovery, or network fetch for trust roots.
- There is no apply loop or drift/status reporting. The prototype stops at "is this delivery valid?".
- `derive_constraints` in `mutation.py` accumulates prior constraints forward through derivation chains. The intended design is per-layer: each attestation's explicit constraints bind its immediate output, and trust flows through the chain because each link is independently verified. The update's constraints should govern the final delivery; the prior's were already spent. Strategy-implied constraints already follow the per-layer model (derived late from final content).

## File guide

- `model.py`: core types and most verification logic
- `policy.py`: strategy-implied constraints
- `mutation.py`: signed update preconditions and derivation
- `verify.py`: trust store, verification bundles, explanations
- `build.py`: helpers for constructing signed inputs and outputs
- `crypto.py`: minimal real Ed25519 signing and hashing
- `cel_runtime.py`: CEL compilation and evaluation helpers
- `test_hybrid.py`: focused security and model tests
- `test_delivery.py`: delivery, placement, upgrade, and generation scenarios
- `test_managed_resource.py`: managed resource attestation with fulfillment relations

## Running it

From `attestation-poc/`:

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest hybrid
```

If you just want the most representative coverage:

```bash
python3 -m pytest hybrid/test_hybrid.py hybrid/test_delivery.py
```
