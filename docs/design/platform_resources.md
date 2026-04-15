# Platform resources

## Problem

How do we offer an extensible core, but allow addons to offer "managed resource" like semantics, that incorporate decade+ of best practices?

Example: imagine a full featured cluster management addon. It should handle many domain specific use cases:

- Provision & configure managed clusters directly targeting managed provider APIs, likely with passthrough auth (e.g. ROSA or ARO clusters)
- Provision & configure clusters through native self managed options (e.g. wrapping openshift assisted installer)
- Provision & configure clusters through operators like CAPI or HyperShift
- Import existing clusters
- Upgrade these clusters (either individually or through a campaign, see https://redhat.atlassian.net/jira/software/c/projects/FM/list?selectedIssue=FM-81 )
- Full exploitation of the core placement and rollout strategy abstractions for progressive delivery, maintenance windows, etc., encoding specific cluster management best practices
- Assist in knowable operational issues in the course of provisioning, upgrades, or other configuration changes
- Manage cluster pooling strategies
- View the state of clusters and their underlying nodes (inventory)
- Integration with other cluster-related addons, like ACS, MCOA, ODF, ...

You could imagine similar domain specific experiences for other managed resource types:

- VMs
- Argo instances
- Model serving
- ...

These are the consumer-facing "nouns" of the platform, in contrast to the addon-facing core abstractions. In some sense, the whole interesting architecture of FleetShift is getting these two competing halves right:

- An agnostic "fleet core" that encodes only the most generic, consistent, "meta" or cross cutting best practices. Things like durability, resilience, extensibility, pooling, fleet-awareness, placement and rollout control, inventory, IAM, metering, ...
- Thoroughly domain specific "nouns" that are opinionated and encode all of the best practice and real world experience we can muster

## Proposal

_Platform resources_ are the "consumer-facing nouns" of the platform. They are addon-driven. Addons register to provide the functions for one or more platform resources.

Platform resources are driven by the core Deployment abstraction. An addon defines how Deployments are derived from Platform resources.

> OPEN QUESTION: How should rollout be expressed? It is a cross cutting concern that probably should not be derived from arbitrary resource schema.

> OPEN QUESTION: Can placement really be derived? What if there are multiple addons for the same resource? Is there a use case for that? (What about a managed platform instance you are deploying to as an addon?)

> We may require a standard "envelope" to support these. In this case, we reexamine just using Deployment as that envelope.

Example:

TODO: Declarative Cluster resource example

TODO: Derived deployment (manifest is maintained inline, placement target is addon, rollout is immediate)

### Attestation

Provenance is maintained up to the original platform resource, which is signed by the user agent. Platform resources therefore represent another kind of verifiable input, with optionally an accompanying derived resource input (to compliment raw signed deployments and derived deployments). When verifying provenance, the verifier must know how a platform resource structurally relates to the resulting manifests and placement. Typically, the addon itself is expected to be given the manifests, and is trusted to fulfill the user's request faithfully by design. As a separate process, this should not violate the "zero-trust management" principle that the platform is only a courier.

TODO: arch diagram showing the flow from user declarative resource input to delivery agent (addon implementing the platform resource)

### State

The schema of a platform resource is defined by the addon. The platform takes care of its storage and retrieval through well-known API patterns:

(something like this, TBD)

- `POST /{resource}`: Persists an intent for the resource and begins reconciliation (subject to "rollout" open question above)
- `GET  /{resource}`: Retrieves all stored intents
- `GET  /{resource}/{name}`
- `PUT  /{resource}/{name}`

Inventory is a separate concern. Addons can report inventory. Inventory is the state of things as they are. A single intent may explode into many resources. High level intent may explode into many "lower level" decisions and resulting attributes.

It is possible that we could support platform resources backed by addons with their own state. We know some addons will have their own state (ACS in a relational db, MCOA in prometheus/thanos, ...).

### Domain specific operations

**WIP / DRAFT**

I think a reasonable path here would be to model these as platform resources / sub resources themselves. They need to be REST-friendly, anyway. The trick is that these could result in associated new platform artifacts, like other platform resources, or Deployments. So an addon could implement a smart transformation from a high level cluster upgrade campaign input to low level deployment-that-updates-resources with rollout strategy.

### Domain specific queries

**WIP / DRAFT**

These could possibly be inventory or addon-directed queries (hand waving a bit over what that is–maybe an extension of the federated query mechanism discussed in [architecture.md](./architecture.md)) that are pre-configured templates? We'd like to define a read projection up front and not have to hit the addon to process this.

### Integration (observability, security, ...)

**WIP / DRAFT**

Addons need to be able to integrate with each other.

### API (gRPC / REST)

**WIP / DRAFT**

Implementing REST extensibility is straightforward. The question is do we want to maintain a gRPC-first design and therefore support gRPC extensions as well?

This would mean extensions would be defined as proto at some level.

There is dynamic dispatch support in gRPC. We'd implement an "unimplemented server" which catches the not explicitly implemented RPCs and dispatches them dynamically.

### Durability

How does this design guarantee that (under failure conditions)...

- no resources are orphaned
- resources eventually reconcile (or explicitly fail)

When an addon [re]connects...

- it has to ask about what work it has left to do
- it has to ensure it's reported the state it knows about. This may require querying for all the things it _should_ know about and updating those.
