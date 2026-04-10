# Hybrid attestation prototype

This directory is a Python proof of concept for the provenance and verification side of the authentication model in `docs/design/authentication.md`.

It is trying to answer one narrow question:

> If the management plane is only a courier, what evidence does a target need in order to accept a deployment, an update, or a removal?

The answer explored here is a prototype that combines:

- a user signs the deployment input
- addons may sign opaque outputs such as rendered manifests, placement decisions, or update plans
- the target verifies the whole chain locally from a self-contained bundle plus external trust anchors

The end design document is broader than this prototype. It also covers credential presentation, transport choices, workflow states like `PausedAuth`, and operational trust distribution. This prototype mostly isolates the target-side verification core.

## What this prototype combines

At the code structure level, this directory combines a few ideas in one model:

- a single `Attestation(input, output)` shape
- explicit, signed CEL output constraints on inputs
- strategy-implied constraints derived from the signed deployment content
- self-contained verification bundles instead of verifier-side history lookup
- data-driven input derivation from signed update outputs
- delivery-aware output types for put and remove operations
- explainable verification results instead of a bare pass/fail

The built-in strategy types are a good example of that composition:

| Strategy | What it means |
| --- | --- |
| manifest `inline` | The signed input already contains the manifests. The delivered manifests must match exactly. |
| manifest `addon` | The manifests are opaque to the target. The expected addon must sign them. |
| placement `predicate` | The target self-assesses placement by evaluating a CEL predicate against its own identity. |
| placement `addon` | A placement addon signs the allowed target list. |

## Core ideas

### 1. A deployment is verified as `input -> output`

The main object is `Attestation`, which pairs:

- an `Input`
- a concrete delivery action (`PutManifests` or `RemoveByDeploymentId`)

The target does not trust the output on its own. It verifies that the output is justified by the input.

### 2. The input carries signed policy, not just content

`SignedInput` signs an envelope containing:

- the `DeploymentContent`
- `valid_until`
- explicit output constraints
- optional `expected_generation`

This is important: the signer is not only authorizing "this deployment exists". They are also authorizing the rules the eventual output must satisfy.

Those explicit rules are CEL expressions (`OutputConstraint`) evaluated at verification time over a context containing:

- `input`
- `output`
- `target`
- `action`
- `placement`

### 3. Strategies imply more policy

In addition to explicitly signed CEL constraints, the verifier derives built-in constraints from the signed strategies in `policy.py`.

Examples:

- inline manifests imply `output.manifests == input.manifest_strategy.manifests`
- addon manifests imply "the output must be signed by addon X via trust anchor Y"
- predicate placement implies puts are allowed only when the target matches, and removals only when it no longer matches
- addon placement implies the placement evidence must be signed, and the current target must be consistent with the signed target list

This is one of the most important ideas in the prototype: the signed input is not just data, it is a compact declaration of how verification must work.

### 4. Updates are first-class attestations

`DerivedInput` models a new deployment version reconstructed from:

- a prior input
- an update attestation whose output contains a signed `spec_update`

The `spec_update` carries:

- a `derive_input_expression` that transforms the prior input
- optional preconditions
- optional additional output constraints

This lets the verifier rebuild deployment history from signed material instead of trusting the platform's stored state.

An important detail: update attestations are themselves deployments. That means update planning can have its own manifest strategy, placement strategy, and addon signatures. In the upgrade scenarios, the update planner signs the patch, and a placement addon signs which deployment IDs the patch is allowed to apply to.

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
- addon outputs must be signed by the expected addon and trust anchor
- placement can be self-assessed (`predicate`) or externally decided (`addon`)
- removals are verified too; they are not trusted just because they are deletions
- derived inputs inherit prior constraints and can add new ones
- signed preconditions make update ordering meaningful
- unknown strategy types fail closed
- forged keys, forged bindings, replayed outputs, and cross-deployment evidence are rejected
- optional `expected_generation` gives a simple target-side stale-write check

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
