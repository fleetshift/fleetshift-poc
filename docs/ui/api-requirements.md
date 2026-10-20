# Console API Requirements and Current Surface

Platform APIs let the console query fleet-wide inventory without browser-side fan-out to every target, while keeping live-target operations on a separate access path. The console consumes those APIs without owning target connectivity, credential validation, or authorization; this reference separates the implemented browser-facing surface from established architectural direction.

## Fleet-wide resource queries

The shared resource client queries platform-held resources through:

```text
GET /apis/open-cluster-management.io/v1alpha/{scope}:queryResources
```

Current callers use `-` for whole-platform scope. The query accepts a CEL filter, page size, page token, and ordering, and returns resource results plus a continuation token. A result contains resource identity and the addon-defined resource body; it is not a universal normalized Kubernetes object.

The core pod, namespace, and node views query Kubernetes observations through this surface. Their hook follows result pages and filters by resource type, kind, and optionally cluster identity. It does not fan out to each cluster from the browser.

Sources:

- [Resource query client and response/error types](../../sdk/common/src/resourceApi.ts)
- [Core resource fetching](../../extensions/core/client/src/plugins/core-plugin/useClusterResources.ts)
- [Inventory query and live-object boundary](../design/architecture/resource_indexing.md#search-api-shape)

Fleet-wide pages need bounded queries, pagination, and enough resource identity to link results to their owning views. Addons own their resource schemas and extracted observations. The current API does not establish automatic normalization of every Kubernetes or OpenShift version into one resource shape.

## Resource operations

Plugins can call typed resource APIs as well as the cross-resource query API. For example, the Kind wizard creates a managed cluster through:

```text
POST /apis/kind.open-cluster-management.io/v1alpha/clusters?cluster_id={id}
```

Its request contains structured cluster specification fields, including nodes and networking. It is not the former Express-proxied inline deployment flow.

Sources: [Kind API client](../../extensions/kind/client/src/api.ts), [wizard submission](../../extensions/kind/client/src/CreateClusterWizard.tsx).

## Live target access: established design direction

Platform inventory contains what reporters sent to the platform. Reading a full live target object or using watch, exec, attach, and log streaming is a separate access path, not a richer variant of the inventory query.

The [fleetlet transport design](../design/architecture/fleetlet_and_transport.md#kubernetes-api-proxy) describes transparent Kubernetes API tunneling for these operations. This is architectural direction, not a claim that the console currently provides the full proxy surface. No production browser proxy URL convention is established here.

Plugins need an explicit target context and an authorized route to the relevant API. UI visibility and knowledge of a cluster ID do not themselves grant target access. Credential presentation and authorization follow the platform's [authentication design](../design/authentication.md).

## Refresh and streaming

Current resource views fetch snapshots. Some views poll: the cluster list and detail views refresh transient state, and the overview refreshes fleet data periodically. The console does not currently provide a functioning shell-wide resource-topic subscription bus.

The Go server does expose `/api/ui/events/ws` for delivery events. It is a broadcaster, not the removed prototype's informer-topic protocol or authenticated ticket handshake. Its authentication remains explicitly unfinished in bootstrap code; it is not a production-ready subscription contract.

Sources:

- [Cluster list polling](../../extensions/core/client/src/plugins/core-plugin/ClustersPage.tsx)
- [Overview refresh](../../extensions/core/client/src/plugins/overview-plugin/useFleetData.ts)
- [Delivery event stream](../../server/internal/transport/http/event_ws.go)
- [WebSocket route and authentication TODO](../../server/internal/bootstrap/ui.go)
- [Shell subscription stub](../../client/web/src/components/Root/ScalprumShell.tsx)

Live updates remain useful for status and logs. This document does not select a new browser streaming protocol, prescribe server informers, or promise a watch RPC. Any production subscription contract must define authorization, reconnect behavior, and loss/recovery semantics before consumers rely on it.
