package kubernetes

import (
	"slices"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// EdgeType identifies the kind of relationship between two resources.
type EdgeType string

const (
	EdgeOwnedBy    EdgeType = "ownedBy"
	EdgeRunsOn     EdgeType = "runsOn"
	EdgeAttachedTo EdgeType = "attachedTo"
	EdgeSelects    EdgeType = "selects"
)

// Edge represents a directed relationship between two resources.
type Edge struct {
	EdgeType
	SourceUID, DestUID   string
	SourceKind, DestKind string
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

		if ownerUID == "" {
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
