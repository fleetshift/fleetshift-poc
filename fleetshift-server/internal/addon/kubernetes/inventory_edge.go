package kubernetes

import (
	"context"
	"slices"

	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// EdgeType identifies the kind of relationship between two Kubernetes
// resources.
type EdgeType string

const (
	EdgeOwnedBy    EdgeType = "ownedBy"
	EdgeRunsOn     EdgeType = "runsOn"
	EdgeAttachedTo EdgeType = "attachedTo"
	EdgeSelects    EdgeType = "selects"
)

// Edge is a directed topology relationship between two Kubernetes
// objects, keyed by UID. Edges are computed in memory by the writer;
// they are not persisted in the first main integration.
type Edge struct {
	EdgeType
	SourceUID, DestUID   string
	SourceKind, DestKind string
}

// EdgeDelta is one flush of topology edge adds and deletes.
type EdgeDelta struct {
	Adds    []Edge
	Deletes []Edge
}

// EdgeSink receives computed topology edge deltas. The first main
// integration wires [NoopEdgeSink]; inventory reporting never carries
// edge fields.
type EdgeSink interface {
	ApplyEdgeDelta(ctx context.Context, targetID domain.TargetID, delta EdgeDelta) error
}

// NoopEdgeSink discards edge deltas. It cannot fail: edge persistence
// is disabled until the platform edge model is selected.
type NoopEdgeSink struct{}

// ApplyEdgeDelta implements [EdgeSink] as a no-op.
func (NoopEdgeSink) ApplyEdgeDelta(context.Context, domain.TargetID, EdgeDelta) error {
	return nil
}

// inventoryNode represents a resource stored in the NodeStore.
type inventoryNode struct {
	UID        string
	Kind       string
	Name       string
	Namespace  string
	OwnerUID   string
	Labels     map[string]string
	Properties map[string]any
	GVR        schema.GroupVersionResource
}

// NodeStore provides dual-indexed lookup of resources by UID and by kind/namespace/name.
type NodeStore struct {
	ByUID               map[string]inventoryNode
	ByKindNamespaceName map[string]map[string]map[string]inventoryNode
}

// buildNodeStore constructs a NodeStore from a map of nodes keyed by UID.
// Cluster-scoped resources (empty namespace) are indexed under "_NONE".
func buildNodeStore(nodes map[string]inventoryNode) NodeStore {
	byKNN := map[string]map[string]map[string]inventoryNode{}
	for _, n := range nodes {
		kind := n.Kind
		namespace := n.Namespace
		if namespace == "" {
			namespace = "_NONE"
		}

		// Initialize maps if not present
		if _, ok := byKNN[kind]; !ok {
			byKNN[kind] = map[string]map[string]inventoryNode{}
		}
		if _, ok := byKNN[kind][namespace]; !ok {
			byKNN[kind][namespace] = map[string]inventoryNode{}
		}

		// Store by name
		byKNN[kind][namespace][n.Name] = n
	}

	return NodeStore{
		ByUID:               nodes,
		ByKindNamespaceName: byKNN,
	}
}

// commonEdges recursively walks the ownership chain via OwnerUID,
// creating an "ownedBy" edge for each level. Cycle detection prevents
// infinite loops.
func commonEdges(uid string, ns NodeStore) []Edge {
	var result []Edge
	var seen []string

	// Look up the source node
	source, ok := ns.ByUID[uid]
	if !ok {
		return result
	}

	// Helper function for recursive traversal
	var walk func(ownerUID string)
	walk = func(ownerUID string) {
		// Cycle detection
		if slices.Contains(seen, ownerUID) {
			return
		}

		// Look up the owner node
		owner, ok := ns.ByUID[ownerUID]
		if !ok {
			return
		}

		// Avoid self-loops
		if uid == ownerUID {
			return
		}

		// Create edge from source to owner
		result = append(result, Edge{
			SourceUID:  uid,
			DestUID:    ownerUID,
			EdgeType:   EdgeOwnedBy,
			SourceKind: source.Kind,
			DestKind:   owner.Kind,
		})

		// Mark this owner as seen
		seen = append(seen, ownerUID)

		// Recurse on the owner's owner
		if owner.OwnerUID != "" {
			walk(owner.OwnerUID)
		}
	}

	// Start the walk with the source's owner
	if source.OwnerUID != "" {
		walk(source.OwnerUID)
	}

	return result
}
