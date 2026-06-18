package kubernetes

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type deltaCall struct {
	targetID   domain.TargetID
	upserts    []domain.InventoryItem
	deletedIDs []domain.InventoryItemID
	edgeAdds   []domain.InventoryEdge
	edgeDels   []domain.InventoryEdge
}

type resyncCall struct {
	targetID      domain.TargetID
	inventoryType domain.InventoryType
	items         []domain.InventoryItem
	edges         []domain.InventoryEdge
}

type mockInventoryWriter struct {
	mu             sync.Mutex
	deltas         []deltaCall
	resyncs        []resyncCall
	applyDeltaFunc func(context.Context, domain.TargetID, []domain.InventoryItem, []domain.InventoryItemID, []domain.InventoryEdge, []domain.InventoryEdge) error
}

func (m *mockInventoryWriter) ApplyDelta(ctx context.Context, targetID domain.TargetID, upserts []domain.InventoryItem, deletedIDs []domain.InventoryItemID, edgeAdds []domain.InventoryEdge, edgeDels []domain.InventoryEdge) error {
	if m.applyDeltaFunc != nil {
		err := m.applyDeltaFunc(ctx, targetID, upserts, deletedIDs, edgeAdds, edgeDels)
		if err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deltas = append(m.deltas, deltaCall{targetID: targetID, upserts: upserts, deletedIDs: deletedIDs, edgeAdds: edgeAdds, edgeDels: edgeDels})
	return nil
}

func (m *mockInventoryWriter) Resync(_ context.Context, targetID domain.TargetID, inventoryType domain.InventoryType, items []domain.InventoryItem, edges []domain.InventoryEdge) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resyncs = append(m.resyncs, resyncCall{targetID: targetID, inventoryType: inventoryType, items: items, edges: edges})
	return nil
}

func (m *mockInventoryWriter) getDeltas() []deltaCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]deltaCall, len(m.deltas))
	copy(out, m.deltas)
	return out
}

func (m *mockInventoryWriter) getResyncs() []resyncCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]resyncCall, len(m.resyncs))
	copy(out, m.resyncs)
	return out
}

var testGVR = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}

var testSchema = map[schema.GroupVersionResource]SchemaEntry{
	testGVR: {
		GVR:  testGVR,
		Kind: "Deployment",
	},
}

func makeResource(uid, name, rv string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]any{
				"uid":               uid,
				"name":              name,
				"namespace":         "default",
				"resourceVersion":   rv,
				"creationTimestamp": "2025-06-01T12:00:00Z",
			},
		},
	}
}

func TestBatching(t *testing.T) {
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Send 3 Add events within the batch window.
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-2", "deploy-2", "101"), GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-3", "deploy-3", "102"), GVR: testGVR}

	// Wait for the batch to flush.
	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	if len(deltas) == 0 {
		t.Fatal("expected at least one delta, got none")
	}

	// All 3 upserts should be in a single batch (the first delta).
	first := deltas[0]
	if got := len(first.upserts); got != 3 {
		t.Fatalf("expected 3 upserts in first delta, got %d", got)
	}

	// Verify target ID.
	if first.targetID != "target-1" {
		t.Errorf("expected targetID=target-1, got %s", first.targetID)
	}

	// Verify item names are present.
	names := make(map[string]bool)
	for _, item := range first.upserts {
		names[item.Name()] = true
	}
	for _, expected := range []string{"deploy-1", "deploy-2", "deploy-3"} {
		if !names[expected] {
			t.Errorf("expected item name %s in upserts", expected)
		}
	}
}

func TestDelete(t *testing.T) {
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: makeResource("uid-del", "deploy-del", "200"), GVR: testGVR}

	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	var found bool
	for _, d := range deltas {
		for _, id := range d.deletedIDs {
			if id == "target-1/uid-del" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected target-1/uid-del in deletedIDs, not found")
	}
}

func TestResync(t *testing.T) {
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 10*time.Second, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	w.ResyncCh() <- ResyncEvent{
		GVR: testGVR,
		Resources: []*unstructured.Unstructured{
			makeResource("uid-r1", "deploy-r1", "300"),
			makeResource("uid-r2", "deploy-r2", "301"),
		},
	}

	time.Sleep(100 * time.Millisecond)

	resyncs := mock.getResyncs()
	if len(resyncs) == 0 {
		t.Fatal("expected at least one resync, got none")
	}

	rs := resyncs[0]
	if rs.inventoryType != "apps/v1/Deployment" {
		t.Errorf("expected inventoryType=apps/v1/Deployment, got %s", rs.inventoryType)
	}
	if len(rs.items) != 2 {
		t.Fatalf("expected 2 items in resync, got %d", len(rs.items))
	}
	if rs.targetID != "target-1" {
		t.Errorf("expected targetID=target-1, got %s", rs.targetID)
	}
}

func TestDedup(t *testing.T) {
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Send two Updates for the same UID with the same resourceVersion.
	w.EventCh() <- ResourceEvent{Op: EventUpdate, Resource: makeResource("uid-dup", "deploy-dup", "500"), GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventUpdate, Resource: makeResource("uid-dup", "deploy-dup", "500"), GVR: testGVR}

	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	var upsertCount int
	for _, d := range deltas {
		for _, item := range d.upserts {
			if item.Name() == "deploy-dup" {
				upsertCount++
			}
		}
	}
	if upsertCount != 1 {
		t.Fatalf("expected 1 upsert for uid-dup (dedup), got %d", upsertCount)
	}
}

func TestResync_MissingSchemaEntry(t *testing.T) {
	mock := &mockInventoryWriter{}
	// Schema has no entry for configmaps — tests base-only extraction.
	w := NewWriter("target-1", mock, testSchema, 10*time.Second, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	cmGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
	w.ResyncCh() <- ResyncEvent{
		GVR: cmGVR,
		Resources: []*unstructured.Unstructured{
			{
				Object: map[string]any{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]any{
						"uid":               "uid-cm1",
						"name":              "my-config",
						"namespace":         "default",
						"resourceVersion":   "400",
						"creationTimestamp": "2025-06-01T12:00:00Z",
					},
				},
			},
		},
	}

	time.Sleep(100 * time.Millisecond)

	resyncs := mock.getResyncs()
	if len(resyncs) == 0 {
		t.Fatal("expected at least one resync, got none")
	}

	rs := resyncs[0]
	// Kind should be derived from the resource object, not the schema.
	if rs.inventoryType != "v1/ConfigMap" {
		t.Errorf("expected inventoryType=v1/ConfigMap, got %s", rs.inventoryType)
	}
	if len(rs.items) != 1 {
		t.Fatalf("expected 1 item in resync, got %d", len(rs.items))
	}
}

func TestLateDeleteProtection(t *testing.T) {
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Delete then Add within the same batch window — the Add should be dropped.
	w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: makeResource("uid-late", "deploy-late", "600"), GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-late", "deploy-late", "601"), GVR: testGVR}

	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	for _, d := range deltas {
		for _, item := range d.upserts {
			if item.Name() == "deploy-late" {
				t.Fatal("expected uid-late to be dropped by late-delete protection, but found in upserts")
			}
		}
	}
}

func TestEdgeComputation_OwnedBy(t *testing.T) {
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Create a Deployment with an owner reference.
	deploy := makeResource("uid-child", "child-deploy", "100")
	deploy.Object["metadata"].(map[string]any)["ownerReferences"] = []any{
		map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"name":       "parent-rs",
			"uid":        "uid-parent",
			"controller": true,
		},
	}

	// Also add the parent resource so the edge can be resolved.
	parent := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"metadata": map[string]any{
				"uid":               "uid-parent",
				"name":              "parent-rs",
				"namespace":         "default",
				"resourceVersion":   "200",
				"creationTimestamp": "2025-06-01T12:00:00Z",
			},
		},
	}

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: deploy, GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: parent, GVR: testGVR}

	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	if len(deltas) == 0 {
		t.Fatal("expected at least one delta, got none")
	}

	first := deltas[0]
	if len(first.edgeAdds) == 0 {
		t.Fatal("expected at least one edge add, got none")
	}

	// Verify ownedBy edge from child to parent.
	var found bool
	for _, e := range first.edgeAdds {
		if e.EdgeType == "ownedBy" && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
			found = true
			if e.SourceKind != "Deployment" {
				t.Errorf("expected SourceKind=Deployment, got %s", e.SourceKind)
			}
			if e.DestKind != "ReplicaSet" {
				t.Errorf("expected DestKind=ReplicaSet, got %s", e.DestKind)
			}
		}
	}
	if !found {
		t.Fatal("expected ownedBy edge from uid-child to uid-parent, not found")
	}
}

func TestEdgeComputation_DeleteRemovesEdges(t *testing.T) {
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Create a Deployment with an owner reference.
	deploy := makeResource("uid-child", "child-deploy", "100")
	deploy.Object["metadata"].(map[string]any)["ownerReferences"] = []any{
		map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"name":       "parent-rs",
			"uid":        "uid-parent",
			"controller": true,
		},
	}

	parent := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"metadata": map[string]any{
				"uid":               "uid-parent",
				"name":              "parent-rs",
				"namespace":         "default",
				"resourceVersion":   "200",
				"creationTimestamp": "2025-06-01T12:00:00Z",
			},
		},
	}

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: deploy, GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: parent, GVR: testGVR}

	time.Sleep(250 * time.Millisecond)

	// Delete the child — should remove the edge.
	w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: deploy, GVR: testGVR}

	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	if len(deltas) < 2 {
		t.Fatalf("expected at least 2 deltas, got %d", len(deltas))
	}

	// Second delta should have the edge delete.
	second := deltas[1]
	if len(second.edgeDels) == 0 {
		t.Fatal("expected at least one edge delete, got none")
	}

	var found bool
	for _, e := range second.edgeDels {
		if e.EdgeType == "ownedBy" && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected ownedBy edge delete from uid-child to uid-parent, not found")
	}
}

func TestEdgeComputation_DiffAcrossFlushes(t *testing.T) {
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Create a Deployment with an owner reference.
	deploy := makeResource("uid-child", "child-deploy", "100")
	deploy.Object["metadata"].(map[string]any)["ownerReferences"] = []any{
		map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"name":       "parent-rs",
			"uid":        "uid-parent",
			"controller": true,
		},
	}

	parent := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"metadata": map[string]any{
				"uid":               "uid-parent",
				"name":              "parent-rs",
				"namespace":         "default",
				"resourceVersion":   "200",
				"creationTimestamp": "2025-06-01T12:00:00Z",
			},
		},
	}

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: deploy, GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: parent, GVR: testGVR}

	time.Sleep(250 * time.Millisecond)

	// Send an update for the parent (no edge change).
	parent2 := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"metadata": map[string]any{
				"uid":               "uid-parent",
				"name":              "parent-rs",
				"namespace":         "default",
				"resourceVersion":   "201",
				"creationTimestamp": "2025-06-01T12:00:00Z",
			},
		},
	}
	w.EventCh() <- ResourceEvent{Op: EventUpdate, Resource: parent2, GVR: testGVR}

	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	if len(deltas) < 2 {
		t.Fatalf("expected at least 2 deltas, got %d", len(deltas))
	}

	// First delta should have the edge add.
	first := deltas[0]
	if len(first.edgeAdds) == 0 {
		t.Fatal("expected at least one edge add in first delta, got none")
	}

	// Second delta should have no edge adds (edge already exists).
	second := deltas[1]
	if len(second.edgeAdds) > 0 {
		t.Errorf("expected no edge adds in second delta (edge unchanged), got %d", len(second.edgeAdds))
	}
}

func TestHeartbeat(t *testing.T) {
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 10*time.Second, nil)
	// Override heartbeat interval for faster test.
	w.heartbeatInterval = 100 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Wait for 2 heartbeat intervals without sending any events.
	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	if len(deltas) == 0 {
		t.Fatal("expected at least one heartbeat delta, got none")
	}

	// Verify that at least one delta is empty (heartbeat).
	var foundHeartbeat bool
	for _, d := range deltas {
		if len(d.upserts) == 0 && len(d.deletedIDs) == 0 && len(d.edgeAdds) == 0 && len(d.edgeDels) == 0 {
			foundHeartbeat = true
			break
		}
	}
	if !foundHeartbeat {
		t.Fatal("expected at least one empty delta (heartbeat), got none")
	}
}

func TestErrorRecovery(t *testing.T) {
	// Mock that fails the first 2 times, then succeeds.
	var attemptCount int
	var mu sync.Mutex
	mock := &mockInventoryWriter{
		applyDeltaFunc: func(_ context.Context, targetID domain.TargetID, upserts []domain.InventoryItem, deletedIDs []domain.InventoryItemID, edgeAdds []domain.InventoryEdge, edgeDels []domain.InventoryEdge) error {
			mu.Lock()
			defer mu.Unlock()
			attemptCount++
			if attemptCount <= 2 {
				return context.DeadlineExceeded
			}
			return nil
		},
	}

	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Send an event to trigger flush.
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-retry", "deploy-retry", "100"), GVR: testGVR}

	// Wait long enough for retries (backoff: 1s, 2s).
	time.Sleep(4 * time.Second)

	mu.Lock()
	finalAttemptCount := attemptCount
	mu.Unlock()

	if finalAttemptCount < 3 {
		t.Fatalf("expected at least 3 attempts (initial + 2 retries), got %d", finalAttemptCount)
	}

	if w.consecutiveFailures != 0 {
		t.Errorf("expected consecutiveFailures=0 after success, got %d", w.consecutiveFailures)
	}
}

func TestEdgeComputation_BuildEdges(t *testing.T) {
	// Define test GVR and schema with BuildEdges factory.
	testGVRWithEdges := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

	testSchemaWithEdges := map[schema.GroupVersionResource]SchemaEntry{
		testGVRWithEdges: {
			GVR:  testGVRWithEdges,
			Kind: "Pod",
			BuildEdges: func(r *unstructured.Unstructured, uid string) func(ns NodeStore) []Edge {
				return func(ns NodeStore) []Edge {
					return []Edge{{
						EdgeType:   EdgeRunsOn,
						SourceUID:  uid,
						DestUID:    "node-1",
						SourceKind: "Pod",
						DestKind:   "Node",
					}}
				}
			},
		},
	}

	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchemaWithEdges, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Create a Pod resource.
	pod := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "v1",
			"kind":       "Pod",
			"metadata": map[string]any{
				"uid":               "uid-pod-1",
				"name":              "test-pod",
				"namespace":         "default",
				"resourceVersion":   "100",
				"creationTimestamp": "2025-06-01T12:00:00Z",
			},
		},
	}

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pod, GVR: testGVRWithEdges}

	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	if len(deltas) == 0 {
		t.Fatal("expected at least one delta, got none")
	}

	first := deltas[0]
	if len(first.edgeAdds) == 0 {
		t.Fatal("expected at least one edge add, got none")
	}

	// Verify the runsOn edge from pod to node.
	var found bool
	for _, e := range first.edgeAdds {
		if e.EdgeType == "runsOn" && e.SourceUID == "uid-pod-1" && e.DestUID == "node-1" {
			found = true
			if e.SourceKind != "Pod" {
				t.Errorf("expected SourceKind=Pod, got %s", e.SourceKind)
			}
			if e.DestKind != "Node" {
				t.Errorf("expected DestKind=Node, got %s", e.DestKind)
			}
		}
	}
	if !found {
		t.Fatal("expected runsOn edge from uid-pod-1 to node-1, not found")
	}
}
