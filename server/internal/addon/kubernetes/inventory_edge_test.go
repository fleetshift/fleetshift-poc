package kubernetes

import (
	"context"
	"testing"
)

func TestBuildNodeStore(t *testing.T) {
	nodes := map[string]inventoryNode{
		"pod-1": {
			UID:       "pod-1",
			Kind:      "Pod",
			Name:      "my-pod",
			Namespace: "default",
		},
		"deploy-1": {
			UID:       "deploy-1",
			Kind:      "Deployment",
			Name:      "my-deploy",
			Namespace: "default",
		},
		"node-1": {
			UID:       "node-1",
			Kind:      "Node",
			Name:      "worker-1",
			Namespace: "", // cluster-scoped
		},
	}

	ns := buildNodeStore(nodes)

	// Verify ByUID index
	if len(ns.ByUID) != 3 {
		t.Errorf("expected 3 nodes in ByUID, got %d", len(ns.ByUID))
	}

	// Verify namespaced lookup
	pod, ok := ns.ByKindNamespaceName["Pod"]["default"]["my-pod"]
	if !ok {
		t.Error("expected to find Pod in default namespace")
	}
	if pod.UID != "pod-1" {
		t.Errorf("expected pod UID pod-1, got %s", pod.UID)
	}

	// Verify cluster-scoped lookup uses "_NONE"
	node, ok := ns.ByKindNamespaceName["Node"]["_NONE"]["worker-1"]
	if !ok {
		t.Error("expected to find cluster-scoped Node under _NONE")
	}
	if node.UID != "node-1" {
		t.Errorf("expected node UID node-1, got %s", node.UID)
	}
}

func TestCommonEdges(t *testing.T) {
	tests := []struct {
		name     string
		nodes    map[string]inventoryNode
		sourceID string
		want     int // expected number of edges
	}{
		{
			name: "no owner",
			nodes: map[string]inventoryNode{
				"pod-1": {
					UID:      "pod-1",
					Kind:     "Pod",
					OwnerUID: "",
				},
			},
			sourceID: "pod-1",
			want:     0,
		},
		{
			name: "single owner",
			nodes: map[string]inventoryNode{
				"pod-1": {
					UID:      "pod-1",
					Kind:     "Pod",
					OwnerUID: "rs-1",
				},
				"rs-1": {
					UID:      "rs-1",
					Kind:     "ReplicaSet",
					OwnerUID: "",
				},
			},
			sourceID: "pod-1",
			want:     1,
		},
		{
			name: "ownership chain",
			nodes: map[string]inventoryNode{
				"pod-1": {
					UID:      "pod-1",
					Kind:     "Pod",
					OwnerUID: "rs-1",
				},
				"rs-1": {
					UID:      "rs-1",
					Kind:     "ReplicaSet",
					OwnerUID: "deploy-1",
				},
				"deploy-1": {
					UID:      "deploy-1",
					Kind:     "Deployment",
					OwnerUID: "",
				},
			},
			sourceID: "pod-1",
			want:     2, // pod -> rs, pod -> deploy
		},
		{
			name: "cycle protection",
			nodes: map[string]inventoryNode{
				"a": {
					UID:      "a",
					Kind:     "A",
					OwnerUID: "b",
				},
				"b": {
					UID:      "b",
					Kind:     "B",
					OwnerUID: "a", // cycle!
				},
			},
			sourceID: "a",
			want:     1, // only a -> b edge
		},
		{
			name: "missing owner",
			nodes: map[string]inventoryNode{
				"pod-1": {
					UID:      "pod-1",
					Kind:     "Pod",
					OwnerUID: "nonexistent",
				},
			},
			sourceID: "pod-1",
			want:     0,
		},
		{
			name: "self-loop owner",
			nodes: map[string]inventoryNode{
				"pod-1": {
					UID:      "pod-1",
					Kind:     "Pod",
					OwnerUID: "pod-1",
				},
			},
			sourceID: "pod-1",
			want:     0,
		},
		{
			name: "cycle via already-seen owner",
			nodes: map[string]inventoryNode{
				"a": {UID: "a", Kind: "A", OwnerUID: "b"},
				"b": {UID: "b", Kind: "B", OwnerUID: "c"},
				"c": {UID: "c", Kind: "C", OwnerUID: "b"}, // back edge to already-seen b
			},
			sourceID: "a",
			want:     2, // a->b, a->c (stop when b is seen again)
		},
		{
			name:     "missing source",
			nodes:    map[string]inventoryNode{},
			sourceID: "missing",
			want:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ns := buildNodeStore(tt.nodes)
			edges := commonEdges(tt.sourceID, ns)

			if len(edges) != tt.want {
				t.Errorf("commonEdges() returned %d edges, want %d", len(edges), tt.want)
			}

			// Verify all edges have correct source
			for _, e := range edges {
				if e.SourceUID != tt.sourceID {
					t.Errorf("edge has source %s, want %s", e.SourceUID, tt.sourceID)
				}
				if e.EdgeType != EdgeOwnedBy {
					t.Errorf("edge has type %s, want %s", e.EdgeType, EdgeOwnedBy)
				}
			}
		})
	}
}

func TestCommonEdgesDetailedChain(t *testing.T) {
	// pod -> replicaset -> deployment
	nodes := map[string]inventoryNode{
		"pod-1": {
			UID:      "pod-1",
			Kind:     "Pod",
			OwnerUID: "rs-1",
		},
		"rs-1": {
			UID:      "rs-1",
			Kind:     "ReplicaSet",
			OwnerUID: "deploy-1",
		},
		"deploy-1": {
			UID:      "deploy-1",
			Kind:     "Deployment",
			OwnerUID: "",
		},
	}

	ns := buildNodeStore(nodes)
	edges := commonEdges("pod-1", ns)

	if len(edges) != 2 {
		t.Fatalf("expected 2 edges, got %d", len(edges))
	}

	// First edge: pod -> replicaset
	if edges[0].SourceUID != "pod-1" || edges[0].DestUID != "rs-1" {
		t.Errorf("first edge: want pod-1->rs-1, got %s->%s", edges[0].SourceUID, edges[0].DestUID)
	}
	if edges[0].SourceKind != "Pod" || edges[0].DestKind != "ReplicaSet" {
		t.Errorf("first edge kinds: want Pod->ReplicaSet, got %s->%s", edges[0].SourceKind, edges[0].DestKind)
	}

	// Second edge: pod -> deployment
	if edges[1].SourceUID != "pod-1" || edges[1].DestUID != "deploy-1" {
		t.Errorf("second edge: want pod-1->deploy-1, got %s->%s", edges[1].SourceUID, edges[1].DestUID)
	}
	if edges[1].SourceKind != "Pod" || edges[1].DestKind != "Deployment" {
		t.Errorf("second edge kinds: want Pod->Deployment, got %s->%s", edges[1].SourceKind, edges[1].DestKind)
	}
}

func TestNoopEdgeSink_ApplyEdgeDeltaIsNoop(t *testing.T) {
	var sink EdgeSink = NoopEdgeSink{}
	err := sink.ApplyEdgeDelta(context.Background(), "clusters/prod", EdgeDelta{
		Adds: []Edge{{
			EdgeType:   EdgeOwnedBy,
			SourceUID:  "pod-1",
			DestUID:    "rs-1",
			SourceKind: "Pod",
			DestKind:   "ReplicaSet",
		}},
		Deletes: []Edge{{
			EdgeType:  EdgeRunsOn,
			SourceUID: "pod-1",
			DestUID:   "node-1",
		}},
	})
	if err != nil {
		t.Fatalf("NoopEdgeSink.ApplyEdgeDelta: %v", err)
	}
}

func TestNoopEdgeSink_EmptyDeltaIsNoop(t *testing.T) {
	var sink NoopEdgeSink
	if err := sink.ApplyEdgeDelta(context.Background(), "clusters/prod", EdgeDelta{}); err != nil {
		t.Fatalf("NoopEdgeSink.ApplyEdgeDelta(empty): %v", err)
	}
}

func TestEdgeTypeConstants(t *testing.T) {
	cases := []struct {
		got  EdgeType
		want string
	}{
		{EdgeOwnedBy, "ownedBy"},
		{EdgeRunsOn, "runsOn"},
		{EdgeAttachedTo, "attachedTo"},
		{EdgeSelects, "selects"},
	}
	for _, tc := range cases {
		if string(tc.got) != tc.want {
			t.Errorf("EdgeType %q = %q, want %q", tc.want, tc.got, tc.want)
		}
	}
}
