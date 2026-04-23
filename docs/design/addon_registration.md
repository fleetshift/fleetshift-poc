# Addon registration

How addons register capabilities with the platform, how registration maps to runtime behavior, how the server recovers registration state across restarts, and the trust model that governs it.

This document builds on the addon integration model (architecture.md §8), the fleetlet channel abstraction (architecture.md §5), the addon trust anchor model (authentication.md), and the managed resource design (managed_resources.md).

## Registration as a durable contract

Addon registration is a **durable contract** between an addon and the platform. It is not a transient session artifact — it survives addon disconnection and server restart. The contract declares what the addon provides (resource types, strategy implementations, delivery targets) and what the platform can expect from it (schemas, delivery model, status projection).

This durability has two consequences:

1. **The platform can serve managed resource APIs while the addon is disconnected.** Read paths (GET /clusters, list, schema validation) depend on the stored registration, not a live addon connection. Write paths store the user's intent and derive a Fulfillment; delivery is deferred until the addon reconnects. The managed resource enters a lifecycle phase that reflects the addon's unavailability.
2. **The server can restart without requiring addons to be online.** On startup, the server loads stored registrations and reconstructs the resource type registry, schema validators, and routing tables. Addons reconnect at their own pace.

## Registration ceremony

### First-time registration

```
┌──────────┐                    ┌──────────────┐                  ┌──────────┐
│  Admin    │                    │   Platform    │                  │  Addon   │
└────┬─────┘                    └──────┬───────┘                  └────┬─────┘
     │                                 │                               │
     │  1. Enroll addon                │                               │
     │  (trust anchor + authorized     │                               │
     │   resource types, signed)       │                               │
     │────────────────────────────────>│                               │
     │                                 │  Store AddonEnrollment        │
     │                                 │──────┐                        │
     │                                 │<─────┘                        │
     │                                 │                               │
     │                                 │  2. Addon connects            │
     │                                 │  (via fleetlet or direct)     │
     │                                 │<──────────────────────────────│
     │                                 │                               │
     │                                 │  3. RegisterCapabilities      │
     │                                 │  (signed by addon identity)   │
     │                                 │<──────────────────────────────│
     │                                 │                               │
     │                                 │  Verify signature against     │
     │                                 │  enrolled trust anchor        │
     │                                 │──────┐                        │
     │                                 │<─────┘                        │
     │                                 │                               │
     │                                 │  Verify declared resource     │
     │                                 │  types ⊆ authorized scope     │
     │                                 │──────┐                        │
     │                                 │<─────┘                        │
     │                                 │                               │
     │                                 │  Store AddonRegistration      │
     │                                 │  (versioned)                  │
     │                                 │──────┐                        │
     │                                 │<─────┘                        │
     │                                 │                               │
     │                                 │  4. RegistrationAccepted      │
     │                                 │  (registration version,       │
     │                                 │   active fulfillments)        │
     │                                 │──────────────────────────────>│
     │                                 │                               │
```

**Step 1: Admin enrollment.** Before an addon can register at runtime, an admin enrolls it. Enrollment establishes:

- **Addon identity**: a name, description, and organizational owner
- **Trust anchor**: the CA bundle, SPIFFE trust domain, or public key that the platform uses to verify the addon's signatures (see authentication.md, "Addon key lifecycle")
- **Authorized resource types**: which managed resource type names this addon is allowed to claim (e.g., `["clusters", "cluster-pools"]`)
- **Authorized strategy types**: which strategy type names this addon may register implementations for (e.g., `["scored-placement"]`)

The enrollment is itself a signed administrative action. It is the root of the trust chain for everything the addon subsequently does.

> OPEN QUESTION: Should authorized resource types be a closed list, or a pattern (e.g., `clusters.*`)? A closed list is simpler and more auditable. A pattern allows addons to define sub-types without re-enrollment.

**Step 2: Addon connects.** The addon connects to the platform through a fleetlet (production) or directly (development). The connection is authenticated — the addon presents a workload identity credential that the platform can verify against the enrolled trust anchor.

**Step 3: RegisterCapabilities.** The addon sends its capability registration, signed with its workload identity key. This includes:

- Managed resource type definitions (schema, delivery model, status projection)
- Strategy implementations (manifest, placement, rollout)
- Delivery target declarations

The platform verifies:

1. The signature is valid against the enrolled trust anchor
2. Every declared resource type is within the addon's authorized scope
3. Every declared strategy type is within the addon's authorized scope
4. The schema is a valid JSON Schema (or protobuf descriptor, depending on format)

If verification passes, the platform stores the registration and activates the declared resource types.

**Step 4: RegistrationAccepted.** The platform confirms the registration and sends the addon its current state — which Fulfillments reference this addon, which deliveries are pending. This is the addon's recovery signal (see "Reconnection and recovery" below).

### Registration update

An addon can update its registration by sending a new `RegisterCapabilities` with changes. The platform treats this as a new registration version. Changes take effect immediately for new operations. For in-flight operations:

- **Schema change**: existing managed resources are not re-validated. New writes are validated against the new schema. The addon is responsible for backward compatibility.
- **New resource type**: the API endpoint becomes available immediately.
- **Removed resource type**: existing resources are not deleted. The API endpoint continues to serve reads. New writes are rejected. Existing Fulfillments continue to deliver (the addon is still the target). This is a **deprecation**, not a removal — explicit cleanup is a separate operation.
- **Strategy change**: triggers invalidation for affected Fulfillments (the existing invalidation signal from architecture.md §8).

> OPEN QUESTION: Should registration updates require re-signing by the admin, or is the addon's own signature sufficient (within the scope the admin already authorized)? The addon's signature is sufficient if the update stays within the originally authorized scope. But if the addon wants to claim new resource types not in the original enrollment, that requires admin re-enrollment.

## Storage model

### AddonEnrollment (admin-time)

```sql
CREATE TABLE addon_enrollments (
    addon_id        TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    trust_anchor    TEXT NOT NULL,        -- serialized TrustAnchor (CA bundle, SPIFFE trust domain, etc.)
    authorized_types TEXT NOT NULL,       -- JSON array of authorized resource type names
    authorized_strategies TEXT NOT NULL,  -- JSON array of authorized strategy type names
    enrolled_by     TEXT NOT NULL,        -- admin identity
    enrollment_sig  TEXT NOT NULL,        -- admin's signature over the enrollment
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL
);
```

### AddonRegistration (runtime, versioned)

```sql
CREATE TABLE addon_registrations (
    addon_id    TEXT NOT NULL REFERENCES addon_enrollments(addon_id),
    version     INTEGER NOT NULL,
    capabilities TEXT NOT NULL,           -- serialized capability registration (resource types, strategies, etc.)
    signature   TEXT NOT NULL,            -- addon's signature over the capabilities
    registered_at TEXT NOT NULL,
    PRIMARY KEY (addon_id, version)
);
```

Each registration is immutable (INSERT only), following the same versioning pattern as `resource_intents`. The platform always uses the latest version. Prior versions are retained for audit and rollback.

### ResourceTypeRegistration (derived, denormalized for query)

```sql
CREATE TABLE resource_type_registrations (
    resource_type   TEXT PRIMARY KEY,
    addon_id        TEXT NOT NULL REFERENCES addon_enrollments(addon_id),
    registration_version INTEGER NOT NULL,
    schema          TEXT NOT NULL,        -- JSON Schema for spec validation
    delivery_target TEXT NOT NULL,        -- "self" or target reference
    status_projection TEXT,              -- projected status fields
    active          BOOLEAN NOT NULL DEFAULT TRUE
);
```

This table is derived from `addon_registrations` — it's the denormalized view the API layer uses for schema validation and routing. It is rebuilt from the registration on startup and on registration update.

## Server lifecycle

### Startup

On startup, the server:

1. Loads all `addon_enrollments` — the set of known addons and their trust anchors
2. Loads the latest version of each `addon_registration` — the current capabilities
3. Rebuilds `resource_type_registrations` — the active resource type → addon mapping
4. Registers API routes for each active resource type (schema validation, CRUD endpoints)
5. Loads Fulfillments that reference addon-owned resource types — these are pending work

At this point the server can serve read requests for all managed resource types and accept writes (storing intent and deriving Fulfillments). Delivery is deferred until the addon reconnects.

### Addon connection

When an addon connects (or reconnects):

1. The addon authenticates via the fleetlet (workload identity verification against the enrolled trust anchor)
2. The addon sends `RegisterCapabilities`
3. The platform compares against the stored registration:
   - **Unchanged**: no new registration version is created. The addon receives its pending work.
   - **Changed**: a new registration version is stored. Affected resource types and Fulfillments are updated. Invalidation signals are emitted as needed.
4. The platform sends `RegistrationAccepted` with:
   - The registration version the platform is using
   - Pending Fulfillments and deliveries for the addon's targets
   - Any managed resources that need addon action

### Addon disconnection

When an addon disconnects (fleetlet reports loss of the addon's channel):

- The addon's enrollment and registration remain stored. Resource types stay active.
- New managed resource writes continue to be accepted — the intent is stored and the Fulfillment is derived. The Fulfillment enters a phase reflecting that delivery is pending the addon's return.
- Existing Fulfillments with pending deliveries are not retried until the addon reconnects.
- The platform may surface the addon's unavailability as a condition on affected Fulfillments and managed resources.

This is the same model as a fleetlet disconnection for any target — durable execution ensures pending work is retried on reconnection. The addon is not special here; it's just another delivery target that happens to also define resource types.

### Server shutdown

On graceful shutdown:

- The server closes addon connections (fleetlets handle reconnection)
- All state is already durable in the database — no flush needed
- Addons will reconnect to the restarted server and pick up where they left off

On crash:

- Same as graceful shutdown, because all mutations are transactional
- In-flight deliveries that were not acknowledged are retried on reconnect (durable execution)

## Reconnection and recovery

The key question on reconnect: **what does the addon need to know?**

The addon is a delivery agent. On reconnect, it needs to know what work is pending. The platform already tracks this — Fulfillments with unacknowledged deliveries targeting this addon. The `RegistrationAccepted` response includes this pending work set.

The addon is also a status reporter. On reconnect, it may need to re-report status for things it manages. Two approaches:

1. **Platform-initiated**: the platform asks the addon for current status of all resources the addon manages. This is a full reconciliation pass.
2. **Addon-initiated**: the addon knows what it manages (it has its own state) and proactively reports. This is the common case for addons with external state (e.g., a cluster management addon that can query cloud APIs).

The platform should support both, but the default is addon-initiated. On `RegistrationAccepted`, the platform sends the list of managed resources the addon owns. The addon reconciles against its own state and reports back.

> OPEN QUESTION: Should the platform send full managed resource specs on reconnect, or just references? Full specs are convenient but could be large. References require the addon to fetch specs it needs. A streaming approach (send references first, addon requests full specs for the ones it needs) may be best.

## Trust model

### Trust chain

```
Admin enrollment signature
  └─ authorizes addon identity (trust anchor)
       └─ addon registration signature
            └─ claims resource type ownership
                 └─ resource type schema (validates user input)
                 └─ delivery target (where fulfillments route)
                 └─ status projection (what users see)
```

Each link in the chain is independently verifiable:

1. **Admin enrollment** is signed by an admin. A verifier checks: is this admin authorized to enroll addons?
2. **Addon registration** is signed by the addon. A verifier checks: does this signature match the trust anchor in the enrollment? Are the claimed resource types within the authorized scope?
3. **Derived Fulfillment** references the registration. A verifier checks: is the Fulfillment's target consistent with the registration's `delivery_target`?
4. **Delivery attestation** chains through the Fulfillment to the managed resource to the user's signature. A verifier can walk the full chain: user → managed resource → Fulfillment → registration → enrollment → admin.

### What the platform is trusted to do

The platform is a courier and a clerk:

- It stores enrollments, registrations, managed resources, and Fulfillments
- It validates schemas and routes deliveries
- It does NOT interpret managed resource specs (the addon does)
- It does NOT sign on behalf of addons or users

A compromised platform can:

- Refuse to store or deliver (denial of service)
- Deliver to the wrong addon (but the addon will reject — it doesn't recognize the resource type)
- Serve stale or missing status (but cannot forge addon-signed status)

A compromised platform cannot:

- Forge an addon registration (it lacks the addon's signing key)
- Forge a managed resource (it lacks the user's signing key)
- Redirect delivery to an unauthorized addon (the Fulfillment's target is derived from the registration, and the registration is addon-signed)

### What the addon is trusted to do

Once enrolled and registered, the addon is trusted to:

- Faithfully interpret managed resource specs and fulfill the user's intent
- Report accurate status
- Define a valid schema for its resource types

This trust is scoped by the enrollment. The addon cannot claim resource types it wasn't authorized for. The addon cannot sign on behalf of other addons. The addon's trust anchor constrains its identity to the key material the admin enrolled.

The addon is NOT trusted by virtue of the platform — the addon's trust comes from its own identity and the admin's enrollment decision. This is the same "zero-trust management" principle from the rest of the architecture.

### Registration as attestation evidence

A stored registration serves as evidence for delivery-side verification. When a delivery agent receives a manifest for a managed resource, it can verify:

1. The managed resource was signed by a user (user provenance)
2. The Fulfillment was derived from the managed resource (platform derivation, mechanically verifiable)
3. The Fulfillment targets an addon that claims ownership of this resource type (addon registration)
4. The addon's registration is signed and authorized (enrollment)

The registration is the link between "this addon claims to handle clusters" and "this Fulfillment was correctly routed to the cluster addon." Without the registration, a verifier cannot distinguish a correctly routed Fulfillment from a misrouted one.

## Mapping to Fulfillments

When a managed resource is created or updated, the platform derives a Fulfillment. The mapping depends on the registration:

| Registration field | Fulfillment field | Notes |
|---|---|---|
| `delivery_target: "self"` | `placement_strategy: STATIC, targets: [addon]` | The common case. The addon IS the target. |
| `resource_type` + resource name | `manifest_strategy: MANAGED_RESOURCE` | The Fulfillment references the managed resource by type and name. |
| (default) | `rollout_strategy: IMMEDIATE` | Single target → immediate rollout. |

This mapping is mechanically fixed for `delivery_target: "self"`. No configurable transformation, no addon logic involved in the derivation. The platform can derive the Fulfillment without the addon being connected — all the information is in the stored registration.

> OPEN QUESTION (from managed_resources.md): Can we support targets other than the addon itself? If so, the derivation rule becomes configurable (addon-defined), and the attestation chain requires additional evidence — the addon's signed declaration of the mapping. This is deferred; `"self"` is the only supported target for now.

## Open questions

### Multi-instance addons

Can multiple instances of the same addon register? For example, two cluster management addon instances serving different regions. If so:

- Do they share a single enrollment, or each get their own?
- Do they register the same resource types, or partitioned subtypes?
- How does the platform route a managed resource to the correct instance?

The simplest model: each instance is a separate enrollment with a separate addon identity. They can register the same resource types only if the enrollment authorizes it, and placement logic determines which instance handles which resource. But this needs design.

### Registration schema versioning

When an addon updates its schema, existing managed resources validated against the old schema may no longer validate. The addon is responsible for backward compatibility, but what does the platform enforce?

- Option A: the platform re-validates existing resources against the new schema on registration update. Breaking changes are rejected.
- Option B: the platform only validates new writes against the new schema. Existing resources are grandfathered.
- Option C: the registration carries a schema version, and resources are tagged with the schema version they were validated against. The addon handles migration.

Option B is the simplest and matches Kubernetes CRD behavior. Option C is the most correct but adds complexity.

### Deregistration ceremony

How does an addon explicitly deregister? Is there a deregistration ceremony, or does the admin revoke the enrollment? What happens to existing managed resources and Fulfillments?

Likely: admin revokes enrollment → addon's registration becomes inactive → resource type stops accepting writes → existing Fulfillments are drained or explicitly cleaned up. The addon may need to perform cleanup (delete cloud resources). This needs a protocol.

### Enrollment revocation

If an admin revokes an enrollment (the trust anchor is no longer trusted), what happens to in-flight deliveries? To existing managed resources? The platform should fail-safe: stop new deliveries, surface the revocation as a condition, but not unilaterally delete managed resources (the addon may need to clean up external state).
