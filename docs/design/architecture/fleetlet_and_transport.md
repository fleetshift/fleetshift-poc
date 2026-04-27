# Fleetlet And Transport

## What this doc covers

The connectivity model of the platform:

- the fleetlet's role
- local connection modes
- channels and transport abstractions
- built-in and addon-defined channel usage
- proxy delivery
- platform-side routing and peer mesh
- control-plane versus data-plane choices

## When to read this

Read this when you need to understand how addons, targets, and users communicate through the system, or when you need the routing and failure model for platform replicas.

## What is intentionally elsewhere

- Core target and delivery contracts: [core_model.md](core_model.md)
- Orchestration and rollout execution: [orchestration.md](orchestration.md)
- Addon contracts and strategy registration: [addon_integration.md](addon_integration.md)
- Resource indexing behavior above the index channel: [resource_indexing.md](resource_indexing.md)
- Full authentication and trust model: [../authentication.md](../authentication.md)

## Related docs

- [../architecture.md](../architecture.md)
- [platform_hierarchy.md](platform_hierarchy.md)
- [../provider_consumer_model.md](../provider_consumer_model.md)

## Fleetlet role

All addon-platform communication goes through the fleetlet. Wherever an addon runs, it connects to a local fleetlet; the fleetlet connects to the platform's URL.

The fleetlet is not an application agent. It runs no business logic. It is a transport multiplexer that serves one or more targets and centralizes:

- connection lifecycle
- authentication handoff
- reconnection and backpressure
- channel routing

This preserves zero infrastructure coupling. Addons do not need to know how the platform is hosted.

In the common case, one fleetlet serves one Kubernetes target. But a fleetlet may also serve addon-registered targets or remote targets reached through proxy delivery.

## Local connection model

Local processes connect to the fleetlet using one of two interfaces:

- **UDS** for co-located processes in the same pod or host
- **TCP** for processes elsewhere in the cluster that cannot share a socket file

On Kubernetes, both are typically enabled. UDS is the preferred path for tightly coupled agents. TCP is used when addons run as separate multi-replica deployments and need cluster-network reachability.

No local process exposes its own network endpoint or manages platform credentials directly. One outbound fleetlet connection replaces many per-addon control-plane connections.

Today the control plane is fragmented: each addon's agent independently maintains its own connection to its management server, each re-implementing TLS, reconnection, heartbeats, and backpressure. The fleetlet collapses those addon-platform control-plane connections into one per fleet member while still leaving addon-specific data-plane connections available when an addon needs them.

## Channel abstraction

The fleetlet multiplexes named, bidirectional channels:

```text
Local process <-> gRPC over UDS or TCP <-> Fleetlet <-> Transport <-> Platform
```

The local protocol and the platform-facing transport are independent choices. Local connections use the same gRPC contract whether the fleetlet reaches the platform through gRPC/TLS, NATS, Kafka, HTTP/WebSocket, or some compatibility transport.

Channels are the unit of:

- multiplexing
- routing
- access control
- traffic prioritization

Strategies and platform services address channels by name rather than by transport-specific mechanics.

## Built-in channels

The fleetlet always owns five built-in channels:

1. **Control**: heartbeat, target registration, and target metadata
2. **Access**: `ResolveIdentity` and `LookupPermissions`
3. **Delivery**: manifest delivery from platform to target
4. **Status**: continuous health and delivery status back to the platform
5. **Index**: resource index deltas from target to platform

### Access-channel special treatment

The access channel is unusually latency- and reliability-sensitive because addon proxy requests depend on it. The fleetlet can therefore optimize it more aggressively than addon-defined channels:

- local caching of identity and permission results
- brief disconnection tolerance through cached results
- priority over bulk traffic

### Delivery-channel semantics

The delivery channel is request/response and per-target. The platform sends generated manifests; the delivery agent applies them and acknowledges success or error. The envelope can include attestation data that a delivery agent verifies before apply.

### Index-channel role

The index channel carries observed-state deltas to the platform. The indexing model itself is defined in [resource_indexing.md](resource_indexing.md).

## Channel isolation and connection classes

Transport implementations isolate channels however they choose, but each channel must behave like an independent stream with `Send`, `Recv`, and `Close`. For example, a gRPC transport maps each channel to a separate HTTP/2 bidirectional stream on a shared TCP/TLS connection. That gives per-stream flow control, frame interleaving, and independent stream lifecycles, so a slow or broken channel does not stall the others.

The fleetlet can also classify channels into connection classes:

- **Control class**: low-volume, latency-sensitive control traffic
- **Bulk class**: high-volume telemetry or large transfers

Local processes do not know which class they use. The fleetlet decides how to route them. This allows transport-level or load-balancer-level isolation without changing local code.

## Reconnection isolation

Local processes never implement reconnection logic against the platform. If the platform-side connection drops, the fleetlet reconnects transparently while local UDS or TCP connections remain up.

During longer outages, local processes experience backpressure rather than explicit disconnect behavior. Loss-tolerant channels may drop data when buffers fill.

When connectivity resumes, the fleetlet emits `ChannelResumed`. Different channel types respond differently:

- **Stateful-source channels** resync from their own source of truth
- **Telemetry channels** may tolerate loss
- **Delivery channels** rely on durable orchestration retries plus idempotent apply rather than a bespoke resync protocol

## Platform-facing transport interface

The fleetlet's platform-facing side is a pluggable transport. The only hard requirement is support for bidirectional ordered message streams.

Representative mappings:

- **gRPC/TLS**: each channel is a bidirectional HTTP/2 stream
- **NATS**: each channel is a subject pair
- **Kafka**: each channel is a topic pair
- **HTTP/WebSocket**: each channel is a WebSocket or HTTP/2 stream
- **Kube API**: compatibility mode using watches and writes over Kubernetes resources

Transport mode is also a security knob. Buffered transport can provide an airgap with no persistent connection, while preserving the same envelope and verification model.

## Routing capabilities

The fleetlet can do more than 1:1 stream forwarding:

- **Fan-in**: multiple local processes on the same logical channel can merge onto one platform-side stream
- **Sharding**: one high-throughput local channel can spread across multiple platform-side streams

This lets addon backends scale without re-implementing their own control-plane load balancing.

## What flows through channels

### Built-in traffic

- **Control**: heartbeats, target registration, capability declarations, target metadata
- **Access**: identity resolution and permission lookup
- **Delivery**: manifest delivery to targets
- **Status**: target health and delivery status
- **Index**: observed-state deltas for fleet-wide search

### Addon-defined traffic

- **Protocol channels**: raw TCP tunneling for target-specific APIs, especially the Kubernetes API proxy
- **Generation traffic**: `Generate` request and response flows for addon manifest generation
- **Invalidation signals**: addon requests to recompute manifests
- **Capability registration**: addon registration and related control traffic
- **Addon API traffic**: platform-authenticated user requests proxied to addon backends
- **Data streams**: agent-to-backend addon traffic
- **State synchronization**: platform-side resource watches or pushed context for addon reconcilers
- **Strategy-specific channels**: custom channels owned by specific placement or rollout strategies

### Kubernetes API proxy

The protocol-channel model can tunnel the Kubernetes API transparently enough that `kubectl`, watches, exec, attach, and log streaming all continue to work. This is the same basic pattern used by Rancher (`remotedialer` plus its Steve proxy), OCM (`cluster-proxy` plus Konnectivity), and Clusternet, but carried over the fleetlet rather than a dedicated tunnel agent.

The primary authentication mode is OIDC token passthrough. User identity flows end to end from platform to cluster API server. This is strictly more secure than impersonation because it eliminates the high-value impersonation service account that, if compromised, could act as any user. Impersonation remains a fallback for targets that do not share the same OIDC trust.

```yaml
clusters:
- cluster:
    server: https://platform.example.com/targets/cluster-abc/api
    certificate-authority-data: <platform TLS CA>
  name: cluster-abc
users:
- name: me
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: fleetshift
      args: ["auth", "token"]
```

`kubectl` sends requests to the platform URL; the platform authorizes and proxies through the fleetlet. A CLI helper such as `fleetshift kubeconfig generate cluster-abc` can generate these kubeconfig entries, while `fleetshift auth token` handles OIDC token refresh. Multiple targets become multiple kubeconfig contexts that point at different paths on the same platform URL.

This also enables a fleet console to provide single-target management for managed Kubernetes targets without direct network access to their API servers, such as viewing workloads, restarting VMs, scaling deployments, or reading logs. Existing `kubectl` workflows, scripts, and CI/CD pipelines keep working too; they simply target the platform URL instead of the cluster's direct API address.

## Bootstrapping strategy infrastructure

Some strategies require helper infrastructure on targets. That is not a special bootstrap problem: the helper infrastructure is itself just another deployment.

For example, a scored placement strategy may require scoring agents on each Kubernetes target. Those agents can be deployed using a simpler placement strategy first, after which later deployments can depend on the richer placement logic.

## Proxy delivery

> NOTE: This was originally conceived for initial bootstrapping only, when the new managed cluster cannot realistically dial back to your laptop. Whether this or any additional scope works is TBD.

The fleetlet could also be the universal delivery transport for remote targets. A fleetlet on cluster A can manage cluster B using cluster B's kubeconfig.

This is especially useful for bootstrap, air-gapped environments, VPN bridging, and temporary management during migration.

This bootstrap is not circular. Whatever creates the target installs the co-located fleetlet as part of target creation. Proxy delivery exists for the specific case where network topology prevents the target's own fleetlet from connecting to the platform. Once that network path exists, or once the co-located fleetlet reaches a reachable platform, the proxy path is retired.

## Platform-side topology

The platform runs as one or more replicas behind a load balancer. Fleetlet connections are persistent, so each fleetlet lands on one replica until it reconnects.

That creates a routing problem: a user request can land on any replica, but the target's fleetlet may be connected elsewhere.

### Peer mesh

Platform replicas therefore form a full mesh of peer connections. Each peer connection does two jobs:

1. **Registry**: replicas tell each other which targets are connected where
2. **Transport**: a replica can forward a request to the replica that owns the fleetlet connection

```text
User request -> load balancer -> any platform replica
             -> peer forward to replica that owns the target connection
             -> fleetlet tunnel to target
```

The always-on mesh avoids an external registry. An on-demand model would avoid idle peer connections, but it would still need some separate registry or gossip system to know which peer owns which target. In the always-on mesh, the peer connection is the registry: session add and remove notifications flow over the persistent connection, so every replica has a current view of fleet connection topology without external state. Peer discovery remains pluggable so the platform itself stays infrastructure-agnostic.

### Scale and failure model

The peer mesh scales with platform replica count rather than fleet size. A small replica set is therefore practical.

If a peer connection drops, requests for targets on that peer fail until the connection is restored. If a platform replica crashes, fleetlets reconnect through the load balancer and the peer mesh updates accordingly.

## Control plane versus data plane

The fleetlet is mandatory for addon-to-platform control-plane communication:

- registration
- invalidation
- manifest generation
- delivery
- status
- access

Addon-internal data flows can use one of two modes.

### Addon-managed connections

Legacy or existing backends may prefer direct agent-to-backend traffic. In this model the addon's generated manifests inject a backend address and the agent connects directly over its own chosen protocol.

### Fleetlet-channeled connections

> NOTE: This design is not yet verified and make change arbitrarily.

New addons can treat the fleetlet as a complete networking shell. Agents and backends both talk only to their local fleetlet. The fleetlets handle routing, fan-in, TLS, reconnection, discovery, and multi-tenancy-aware ingress.

When an addon registers a capability, it declares its data channels and the fleetlet that serves them. The platform records this in its routing table. When a source fleetlet sees a local process open a data channel, it asks the platform for the destination and receives the destination fleetlet's address.

If the destination fleetlet is directly reachable, the source fleetlet connects directly and the platform stays out of the data path. If direct connectivity is unavailable, such as when both sides sit behind NAT, the platform can relay the stream as a fallback. The local programming model stays the same either way.

When many source fleetlets converge on one destination, the destination fleetlet can multiplex those inbound streams onto one local channel. The addon backend reads one merged stream rather than managing hundreds of inbound connections, while still being able to distinguish sources when needed.

Why this matters for addon developers:

- **Ingress**: the platform proxies user traffic such as UI plugins, query APIs, and webhooks through the fleetlet to the addon backend. The addon reads requests from a local socket instead of running an exposed HTTP server.
- **Egress**: deployed agents write to a fleetlet data channel, and the addon backend reads from a local socket instead of building its own server infrastructure, fan-in path, or credential distribution.
- **Control plane**: register, invalidate, generate, deliver, status, and access all use the same fleetlet path.
- **Auth and multi-tenancy**: the platform authenticates and authorizes before traffic reaches the addon, so the addon receives pre-authenticated requests with tenant context.
- **Networking**: TLS, mutual auth, credential rotation, reconnection, fan-in, firewall and NAT traversal, and backend discovery stay in the fleetlet layer.

An addon that would otherwise need a custom agent, a custom server, a TLS PKI, per-cluster credential distribution, and multi-tenancy logic becomes a `ManifestGenerator` plus a local channel reader and writer.

### Capability, not mandate

The platform does not force one data-plane model. It deploys what the addon asks for. Fleetlet-channeled traffic is the recommended path for new addons, while direct addon-managed traffic remains a compatibility option for established backends.

### Evolution

The intended progression is:

1. fleetlet handles only the control plane
2. fleetlet-channeled data flows route through the platform
3. direct fleetlet-to-fleetlet data flows remove the platform from the data path

The local programming model stays the same across these stages.
