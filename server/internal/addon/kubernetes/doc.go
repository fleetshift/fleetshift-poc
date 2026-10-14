// Package kubernetes is the FleetShift addon for Kubernetes clusters.
//
// It declares one AddonID with two independent capabilities:
//
//   - Delivery: [DeliveryAgent] applies and removes manifests via
//     server-side apply. Registered through AddonManager Connect.
//   - Inventory: in-process indexing watches cluster objects and
//     reports them under [ObjectResourceType]. The inventory schema is
//     registered at Connect; the indexer runtime is composed separately
//     in server wiring ([IndexingRuntime] plus a one-shot startup replay
//     of persisted targets) and is not part of ConnectInput. When an
//     IndexingRuntime is injected, Kind and GCP HCP call EnsureIndexer
//     before reporting Delivered and StopIndexer at cluster teardown.
//
// Delivery and inventory share [TargetType] and the target property
// keys in cluster_connection.go (api_server, credentials). The Kubernetes
// [DeliveryAgent] does not require a running indexer. Kind and GCP HCP
// agents that have an IndexingRuntime require EnsureIndexer success before
// reporting Delivered.
//
// File naming in this package:
//
//   - delivery_* — DeliveryAgent and SSA applier
//   - inventory_* — object identity, reporter, and the
//     in-process indexing pipeline
//   - index_schema* — which GVRs/fields to watch (indexer config,
//     not the platform ExtensionResourceSchema)
package kubernetes
