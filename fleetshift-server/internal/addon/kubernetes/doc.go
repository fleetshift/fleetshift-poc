// Package kubernetes is the FleetShift addon for Kubernetes clusters.
//
// It declares one AddonID with two independent capabilities:
//
//   - Delivery: [DeliveryAgent] applies and removes manifests via
//     server-side apply. Registered through AddonManager Connect.
//   - Inventory: in-process indexing watches cluster objects and
//     reports them under [ObjectResourceType]. The inventory schema is
//     registered at Connect; the indexer runtime is composed separately
//     in server wiring ([IndexingRuntime] + startup replay)
//     and is not part of ConnectInput.
//
// Delivery and inventory share [TargetType] and the target property
// keys in cluster_connection.go (api_server, credentials). An absent or
// failed indexer does not block delivery routing.
//
// File naming in this package:
//
//   - delivery_* — DeliveryAgent and SSA applier
//   - inventory_* — object identity, reporter, and the
//     in-process indexing pipeline
//   - index_schema* — which GVRs/fields to watch (indexer config,
//     not the platform ExtensionResourceSchema)
package kubernetes
