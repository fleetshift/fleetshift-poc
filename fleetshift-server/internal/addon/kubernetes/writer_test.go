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
}

type mockInventoryWriter struct {
	mu             sync.Mutex
	deltas         []deltaCall
	resyncs        []resyncCall
	applyDeltaFunc func(context.Context, domain.TargetID, []domain.InventoryItem, []domain.InventoryItemID, []domain.InventoryEdge, []domain.InventoryEdge) error
	resyncFunc     func(context.Context, domain.TargetID, domain.InventoryType, []domain.InventoryItem) error
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

func (m *mockInventoryWriter) Resync(ctx context.Context, targetID domain.TargetID, inventoryType domain.InventoryType, items []domain.InventoryItem) error {
	if m.resyncFunc != nil {
		if err := m.resyncFunc(ctx, targetID, inventoryType, items); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resyncs = append(m.resyncs, resyncCall{targetID: targetID, inventoryType: inventoryType, items: items})
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
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

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
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

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
	w := NewWriter("target-1", mock, testSchema, 10*time.Second, discardLogger)

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
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

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
	w := NewWriter("target-1", mock, testSchema, 10*time.Second, discardLogger)

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
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

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
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

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
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

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
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

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
	w := NewWriter("target-1", mock, testSchema, 10*time.Second, discardLogger)
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

	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

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
	w := NewWriter("target-1", mock, testSchemaWithEdges, 100*time.Millisecond, discardLogger)

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

func TestResync_DoesNotClobberFlushEdges(t *testing.T) {
	// Regression test: flush correctly produces cross-GVR edges, then a
	// resync for the source GVR must NOT delete them. Resync is items-only
	// (no edge parameter), so edges are untouched in the database. Verify
	// that no edge deletions appear in subsequent flush deltas.
	pvGVR := schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}
	pvcGVR := schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}

	crossSchema := map[schema.GroupVersionResource]SchemaEntry{
		pvGVR: {GVR: pvGVR, Kind: "PersistentVolume"},
		pvcGVR: {
			GVR:  pvcGVR,
			Kind: "PersistentVolumeClaim",
			BuildEdges: func(r *unstructured.Unstructured, uid string) func(NodeStore) []Edge {
				volName, _, _ := unstructured.NestedString(r.Object, "spec", "volumeName")
				return func(ns NodeStore) []Edge {
					if volName == "" {
						return nil
					}
					if pvMap, ok := ns.ByKindNamespaceName["PersistentVolume"]["_NONE"]; ok {
						if pv, ok := pvMap[volName]; ok {
							return []Edge{{
								EdgeType:   EdgeAttachedTo,
								SourceUID:  uid,
								DestUID:    pv.UID,
								SourceKind: "PersistentVolumeClaim",
								DestKind:   "PersistentVolume",
							}}
						}
					}
					return nil
				}
			},
		},
	}

	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, crossSchema, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	pv := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolume",
		"metadata": map[string]any{
			"uid": "uid-pv", "name": "my-pv",
			"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
		},
	}}
	pvc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{
			"uid": "uid-pvc", "name": "my-pvc", "namespace": "default",
			"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
		},
		"spec": map[string]any{"volumeName": "my-pv"},
	}}

	// Step 1: Flush produces the edge.
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pv, GVR: pvGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pvc, GVR: pvcGVR}
	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	var flushHasEdge bool
	for _, d := range deltas {
		for _, e := range d.edgeAdds {
			if e.EdgeType == "attachedTo" && e.SourceUID == "uid-pvc" && e.DestUID == "uid-pv" {
				flushHasEdge = true
			}
		}
	}
	if !flushHasEdge {
		t.Fatal("flush should have produced PVC→PV edge")
	}

	// Step 2: Resync PVCs — items-only, edges untouched.
	w.ResyncCh() <- ResyncEvent{GVR: pvcGVR, Resources: []*unstructured.Unstructured{pvc}}
	time.Sleep(100 * time.Millisecond)

	resyncs := mock.getResyncs()
	if len(resyncs) == 0 {
		t.Fatal("expected resync call")
	}

	// Step 3: Verify no edge deletions were emitted in any delta after the resync.
	deltasAfter := mock.getDeltas()
	for _, d := range deltasAfter {
		for _, e := range d.edgeDels {
			if e.EdgeType == "attachedTo" && e.SourceUID == "uid-pvc" {
				t.Fatal("resync should not cause PVC→PV edge deletion in subsequent flush")
			}
		}
	}
}

func TestResync_UpdatesWriterState(t *testing.T) {
	// Verify that resources arriving via resync are visible to subsequent
	// flush edge computation (i.e. sendResync merges into w.currentNodes).
	pvGVR := schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumes"}
	pvcGVR := schema.GroupVersionResource{Version: "v1", Resource: "persistentvolumeclaims"}

	testSchemaCrossGVR := map[schema.GroupVersionResource]SchemaEntry{
		pvGVR: {GVR: pvGVR, Kind: "PersistentVolume"},
		pvcGVR: {
			GVR:  pvcGVR,
			Kind: "PersistentVolumeClaim",
			BuildEdges: func(r *unstructured.Unstructured, uid string) func(NodeStore) []Edge {
				volName, _, _ := unstructured.NestedString(r.Object, "spec", "volumeName")
				return func(ns NodeStore) []Edge {
					if volName == "" {
						return nil
					}
					if pvMap, ok := ns.ByKindNamespaceName["PersistentVolume"]["_NONE"]; ok {
						if pv, ok := pvMap[volName]; ok {
							return []Edge{{
								EdgeType:   EdgeAttachedTo,
								SourceUID:  uid,
								DestUID:    pv.UID,
								SourceKind: "PersistentVolumeClaim",
								DestKind:   "PersistentVolume",
							}}
						}
					}
					return nil
				}
			},
		},
	}

	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchemaCrossGVR, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Step 1: Resync PVs (no events, only resync).
	pv := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolume",
		"metadata": map[string]any{
			"uid": "uid-pv", "name": "my-pv",
			"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
		},
	}}
	w.ResyncCh() <- ResyncEvent{GVR: pvGVR, Resources: []*unstructured.Unstructured{pv}}
	time.Sleep(100 * time.Millisecond)

	// Step 2: Add a PVC via event — the subsequent flush must find the
	// resynced PV in w.currentNodes to build the edge.
	pvc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "PersistentVolumeClaim",
		"metadata": map[string]any{
			"uid": "uid-pvc", "name": "my-pvc", "namespace": "default",
			"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
		},
		"spec": map[string]any{"volumeName": "my-pv"},
	}}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pvc, GVR: pvcGVR}
	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	var found bool
	for _, d := range deltas {
		for _, e := range d.edgeAdds {
			if e.EdgeType == "attachedTo" && e.SourceUID == "uid-pvc" && e.DestUID == "uid-pv" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("flush after resync should see resynced PV in currentNodes and produce PVC→PV edge")
	}
}

func TestResync_OwnedByEdgesAfterCrossGVRResync(t *testing.T) {
	// Regression test for the startup ordering race: ReplicaSet arrives via
	// resync, then Pod (with ownerReference to RS) arrives via event.
	// The flush must produce the ownedBy edge even though the RS was never
	// seen through the event path.
	rsGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
	podGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}

	testSchemaOwnedBy := map[schema.GroupVersionResource]SchemaEntry{
		rsGVR:  {GVR: rsGVR, Kind: "ReplicaSet"},
		podGVR: {GVR: podGVR, Kind: "Pod"},
	}

	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchemaOwnedBy, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Step 1: ReplicaSet arrives via resync only.
	rs := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{
			"uid": "uid-rs", "name": "nginx-rs", "namespace": "default",
			"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
		},
	}}
	w.ResyncCh() <- ResyncEvent{GVR: rsGVR, Resources: []*unstructured.Unstructured{rs}}
	time.Sleep(100 * time.Millisecond)

	// Step 2: Pod with ownerReference to RS arrives via event.
	pod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": "uid-pod", "name": "nginx-pod", "namespace": "default",
			"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
			"ownerReferences": []any{
				map[string]any{
					"apiVersion": "apps/v1", "kind": "ReplicaSet",
					"name": "nginx-rs", "uid": "uid-rs", "controller": true,
				},
			},
		},
	}}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pod, GVR: podGVR}
	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	var found bool
	for _, d := range deltas {
		for _, e := range d.edgeAdds {
			if e.EdgeType == "ownedBy" && e.SourceUID == "uid-pod" && e.DestUID == "uid-rs" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("flush should produce ownedBy edge from Pod to ReplicaSet (RS arrived via resync)")
	}
}

func TestResync_DoesNotClobberOwnedByEdges(t *testing.T) {
	// Regression test: flush correctly produces Pod→RS ownedBy edge, then
	// a Pod resync fires. The ownedBy edge must survive — resync is
	// items-only and must not cause edge deletion.
	rsGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
	podGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}

	testSchemaOwnedBy := map[schema.GroupVersionResource]SchemaEntry{
		rsGVR:  {GVR: rsGVR, Kind: "ReplicaSet"},
		podGVR: {GVR: podGVR, Kind: "Pod"},
	}

	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchemaOwnedBy, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	rs := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{
			"uid": "uid-rs", "name": "nginx-rs", "namespace": "default",
			"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
		},
	}}
	pod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": "uid-pod", "name": "nginx-pod", "namespace": "default",
			"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
			"ownerReferences": []any{
				map[string]any{
					"apiVersion": "apps/v1", "kind": "ReplicaSet",
					"name": "nginx-rs", "uid": "uid-rs", "controller": true,
				},
			},
		},
	}}

	// Step 1: Both arrive via events — flush produces ownedBy edge.
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: rs, GVR: rsGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pod, GVR: podGVR}
	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	var flushHasEdge bool
	for _, d := range deltas {
		for _, e := range d.edgeAdds {
			if e.EdgeType == "ownedBy" && e.SourceUID == "uid-pod" && e.DestUID == "uid-rs" {
				flushHasEdge = true
			}
		}
	}
	if !flushHasEdge {
		t.Fatal("flush should have produced Pod→RS ownedBy edge")
	}

	// Step 2: Pod resync fires — must not destroy the edge.
	w.ResyncCh() <- ResyncEvent{GVR: podGVR, Resources: []*unstructured.Unstructured{pod}}
	time.Sleep(100 * time.Millisecond)

	// Step 3: Verify no ownedBy edge deletions in any subsequent delta.
	deltasAfter := mock.getDeltas()
	for _, d := range deltasAfter {
		for _, e := range d.edgeDels {
			if e.EdgeType == "ownedBy" && e.SourceUID == "uid-pod" {
				t.Fatal("Pod resync must not cause ownedBy edge deletion")
			}
		}
	}
}

func TestEdgeComputation_MultipleEdgeTypesToSameDest(t *testing.T) {
	// A resource can have multiple edge types to the same destination.
	// For example, a Pod owned by a Node AND running on that Node would
	// produce both ownedBy and runsOn edges to the same dest UID.
	podGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}

	schemaMultiEdge := map[schema.GroupVersionResource]SchemaEntry{
		podGVR: {
			GVR:  podGVR,
			Kind: "Pod",
			BuildEdges: func(r *unstructured.Unstructured, uid string) func(NodeStore) []Edge {
				return func(ns NodeStore) []Edge {
					return []Edge{
						{EdgeType: EdgeRunsOn, SourceUID: uid, DestUID: "node-1", SourceKind: "Pod", DestKind: "Node"},
						{EdgeType: EdgeAttachedTo, SourceUID: uid, DestUID: "node-1", SourceKind: "Pod", DestKind: "Node"},
					}
				}
			},
		},
	}

	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, schemaMultiEdge, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	pod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": "uid-pod", "name": "test-pod", "namespace": "default",
			"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
		},
	}}

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pod, GVR: podGVR}
	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	if len(deltas) == 0 {
		t.Fatal("expected at least one delta, got none")
	}

	edgeTypes := make(map[string]bool)
	for _, d := range deltas {
		for _, e := range d.edgeAdds {
			if e.SourceUID == "uid-pod" && e.DestUID == "node-1" {
				edgeTypes[e.EdgeType] = true
			}
		}
	}

	if !edgeTypes["runsOn"] {
		t.Error("expected runsOn edge from uid-pod to node-1")
	}
	if !edgeTypes["attachedTo"] {
		t.Error("expected attachedTo edge from uid-pod to node-1")
	}
	if len(edgeTypes) != 2 {
		t.Errorf("expected 2 distinct edge types, got %d: %v", len(edgeTypes), edgeTypes)
	}
}

func TestEdgeComputation_MultipleEdgeTypes_DeleteRemovesBoth(t *testing.T) {
	podGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}

	schemaMultiEdge := map[schema.GroupVersionResource]SchemaEntry{
		podGVR: {
			GVR:  podGVR,
			Kind: "Pod",
			BuildEdges: func(r *unstructured.Unstructured, uid string) func(NodeStore) []Edge {
				return func(ns NodeStore) []Edge {
					return []Edge{
						{EdgeType: EdgeRunsOn, SourceUID: uid, DestUID: "node-1", SourceKind: "Pod", DestKind: "Node"},
						{EdgeType: EdgeAttachedTo, SourceUID: uid, DestUID: "node-1", SourceKind: "Pod", DestKind: "Node"},
					}
				}
			},
		},
	}

	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, schemaMultiEdge, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	pod := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "Pod",
		"metadata": map[string]any{
			"uid": "uid-pod", "name": "test-pod", "namespace": "default",
			"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
		},
	}}

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pod, GVR: podGVR}
	time.Sleep(250 * time.Millisecond)

	// Delete the pod — both edge types should be deleted.
	w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: pod, GVR: podGVR}
	time.Sleep(250 * time.Millisecond)

	deltas := mock.getDeltas()
	if len(deltas) < 2 {
		t.Fatalf("expected at least 2 deltas, got %d", len(deltas))
	}

	deletedEdgeTypes := make(map[string]bool)
	for _, d := range deltas {
		for _, e := range d.edgeDels {
			if e.SourceUID == "uid-pod" && e.DestUID == "node-1" {
				deletedEdgeTypes[e.EdgeType] = true
			}
		}
	}

	if !deletedEdgeTypes["runsOn"] {
		t.Error("expected runsOn edge deletion")
	}
	if !deletedEdgeTypes["attachedTo"] {
		t.Error("expected attachedTo edge deletion")
	}
}

func TestShutdownFlush_PersistsPendingEvents(t *testing.T) {
	// Regression test: when the context is cancelled, the Writer must
	// flush pending events using an uncancelled context so data is
	// persisted. A long batch interval ensures the timer doesn't fire
	// before shutdown — only the shutdown flush path runs.
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 10*time.Second, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx)
	}()

	// Send events. The 10s batch interval means the timer won't fire.
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-2", "deploy-2", "101"), GVR: testGVR}
	time.Sleep(50 * time.Millisecond) // let events drain from channel

	// Cancel the context — triggers shutdown flush.
	cancel()
	<-done

	deltas := mock.getDeltas()
	if len(deltas) == 0 {
		t.Fatal("shutdown flush should have persisted pending events, got 0 deltas")
	}

	names := make(map[string]bool)
	for _, d := range deltas {
		for _, item := range d.upserts {
			names[item.Name()] = true
		}
	}
	if !names["deploy-1"] || !names["deploy-2"] {
		t.Fatalf("shutdown flush missing events: got %v", names)
	}
}

func TestMockInventoryWriter_ResyncFunc(t *testing.T) {
	// Verify that the mock's resyncFunc is called when set.
	var called bool
	var capturedTargetID domain.TargetID
	var capturedType domain.InventoryType

	mock := &mockInventoryWriter{
		resyncFunc: func(_ context.Context, targetID domain.TargetID, inventoryType domain.InventoryType, items []domain.InventoryItem) error {
			called = true
			capturedTargetID = targetID
			capturedType = inventoryType
			return nil
		},
	}

	ctx := context.Background()
	err := mock.Resync(ctx, "test-target", "test/type", []domain.InventoryItem{})
	if err != nil {
		t.Fatalf("Resync failed: %v", err)
	}

	if !called {
		t.Fatal("resyncFunc was not called")
	}
	if capturedTargetID != "test-target" {
		t.Errorf("resyncFunc targetID = %q, want %q", capturedTargetID, "test-target")
	}
	if capturedType != "test/type" {
		t.Errorf("resyncFunc inventoryType = %q, want %q", capturedType, "test/type")
	}

	// Verify that resync was also recorded in the resyncs slice.
	resyncs := mock.getResyncs()
	if len(resyncs) != 1 {
		t.Fatalf("expected 1 resync recorded, got %d", len(resyncs))
	}
}

func TestFlushFailure_ItemsRetriedOnNextTick(t *testing.T) {
	var mu sync.Mutex
	var callCount int
	mock := &mockInventoryWriter{
		applyDeltaFunc: func(_ context.Context, _ domain.TargetID, _ []domain.InventoryItem, _ []domain.InventoryItemID, _ []domain.InventoryEdge, _ []domain.InventoryEdge) error {
			mu.Lock()
			defer mu.Unlock()
			callCount++
			if callCount <= 3 {
				return context.DeadlineExceeded
			}
			return nil
		},
	}

	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}

	// First flush: 3 retries with 1s+2s+4s backoff = ~7s, then fails.
	// Second flush: next tick retries the same items and succeeds.
	time.Sleep(9 * time.Second)

	deltas := mock.getDeltas()
	var found bool
	for _, d := range deltas {
		for _, item := range d.upserts {
			if item.Name() == "deploy-1" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected deploy-1 to be retried and persisted after failed flush")
	}
}

func TestFlushFailure_EdgesRetriedOnNextTick(t *testing.T) {
	var mu sync.Mutex
	var callCount int
	mock := &mockInventoryWriter{
		applyDeltaFunc: func(_ context.Context, _ domain.TargetID, _ []domain.InventoryItem, _ []domain.InventoryItemID, _ []domain.InventoryEdge, _ []domain.InventoryEdge) error {
			mu.Lock()
			defer mu.Unlock()
			callCount++
			if callCount <= 3 {
				return context.DeadlineExceeded
			}
			return nil
		},
	}

	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	child := makeResource("uid-child", "child-deploy", "100")
	child.Object["metadata"].(map[string]any)["ownerReferences"] = []any{
		map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"name":       "parent-rs",
			"uid":        "uid-parent",
			"controller": true,
		},
	}
	parent := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "ReplicaSet",
		"metadata": map[string]any{
			"uid": "uid-parent", "name": "parent-rs", "namespace": "default",
			"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
		},
	}}

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: child, GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: parent, GVR: testGVR}

	time.Sleep(9 * time.Second)

	deltas := mock.getDeltas()
	var found bool
	for _, d := range deltas {
		for _, e := range d.edgeAdds {
			if e.EdgeType == "ownedBy" && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
				found = true
			}
		}
	}
	if !found {
		t.Fatal("expected ownedBy edge to be retried after failed flush")
	}
}

func TestFlushFailure_DeletesRetriedOnNextTick(t *testing.T) {
	var mu sync.Mutex
	var callCount int
	mock := &mockInventoryWriter{
		applyDeltaFunc: func(_ context.Context, _ domain.TargetID, _ []domain.InventoryItem, _ []domain.InventoryItemID, _ []domain.InventoryEdge, _ []domain.InventoryEdge) error {
			mu.Lock()
			defer mu.Unlock()
			callCount++
			if callCount <= 3 {
				return context.DeadlineExceeded
			}
			return nil
		},
	}

	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: makeResource("uid-del", "deploy-del", "100"), GVR: testGVR}

	// First flush: 3 retries with 1s+2s+4s backoff = ~7s, then fails.
	// Second flush: next tick retries the same delete and succeeds.
	time.Sleep(9 * time.Second)

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
		t.Fatal("expected uid-del to be retried and persisted after failed flush")
	}
}

func TestFlushFailure_NewEventsMergedBetweenRetries(t *testing.T) {
	var mu sync.Mutex
	var callCount int
	var firstFlushComplete bool
	mock := &mockInventoryWriter{
		applyDeltaFunc: func(_ context.Context, _ domain.TargetID, _ []domain.InventoryItem, _ []domain.InventoryItemID, _ []domain.InventoryEdge, _ []domain.InventoryEdge) error {
			mu.Lock()
			defer mu.Unlock()
			callCount++
			if callCount <= 3 {
				if callCount == 3 {
					firstFlushComplete = true
				}
				return context.DeadlineExceeded
			}
			return nil
		},
	}

	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Send first event.
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}

	// Wait for the first flush to complete its retries (~7s).
	time.Sleep(7500 * time.Millisecond)

	// Send a second event AFTER the first flush has failed but before the next tick.
	mu.Lock()
	waitingForRetry := firstFlushComplete
	mu.Unlock()
	if !waitingForRetry {
		t.Fatal("test timing issue: first flush did not complete retries")
	}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-2", "deploy-2", "101"), GVR: testGVR}

	// Wait for the next tick to retry (~1.5s more).
	time.Sleep(2 * time.Second)

	deltas := mock.getDeltas()
	names := make(map[string]bool)
	for _, d := range deltas {
		for _, item := range d.upserts {
			names[item.Name()] = true
		}
	}
	if !names["deploy-1"] {
		t.Fatal("expected deploy-1 to be persisted after failed flush")
	}
	if !names["deploy-2"] {
		t.Fatal("expected deploy-2 to be merged into the retry batch")
	}
}

func TestResync_RetriesOnFailure(t *testing.T) {
	var mu sync.Mutex
	var resyncAttempts int
	mock := &mockInventoryWriter{
		resyncFunc: func(_ context.Context, _ domain.TargetID, _ domain.InventoryType, _ []domain.InventoryItem) error {
			mu.Lock()
			defer mu.Unlock()
			resyncAttempts++
			if resyncAttempts <= 2 {
				return context.DeadlineExceeded
			}
			return nil
		},
	}

	w := NewWriter("target-1", mock, testSchema, 10*time.Second, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	w.ResyncCh() <- ResyncEvent{
		GVR:       testGVR,
		Resources: []*unstructured.Unstructured{makeResource("uid-1", "deploy-1", "100")},
	}

	time.Sleep(4 * time.Second)

	mu.Lock()
	attempts := resyncAttempts
	mu.Unlock()

	if attempts < 3 {
		t.Fatalf("expected at least 3 resync attempts, got %d", attempts)
	}

	resyncs := mock.getResyncs()
	if len(resyncs) == 0 {
		t.Fatal("expected resync to succeed after retries")
	}
}

func TestResync_PurgesStaleNodes(t *testing.T) {
	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, testSchema, 100*time.Millisecond, discardLogger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Step 1: Add child (ownerRef → parent) and parent via events.
	child := makeResource("uid-child", "child-deploy", "100")
	child.Object["metadata"].(map[string]any)["ownerReferences"] = []any{
		map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"name": "parent-deploy", "uid": "uid-parent", "controller": true,
		},
	}
	parent := makeResource("uid-parent", "parent-deploy", "200")

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: child, GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: parent, GVR: testGVR}
	time.Sleep(250 * time.Millisecond)

	// Verify the ownedBy edge was created.
	deltas := mock.getDeltas()
	var edgeCreated bool
	for _, d := range deltas {
		for _, e := range d.edgeAdds {
			if e.EdgeType == "ownedBy" && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
				edgeCreated = true
			}
		}
	}
	if !edgeCreated {
		t.Fatal("precondition: expected ownedBy edge from child to parent")
	}

	// Step 2: Resync with only child — parent is stale and should be purged.
	w.ResyncCh() <- ResyncEvent{GVR: testGVR, Resources: []*unstructured.Unstructured{child}}
	time.Sleep(100 * time.Millisecond)

	// Step 3: Send an update to trigger a flush with edge recomputation.
	childUpdated := makeResource("uid-child", "child-deploy", "101")
	childUpdated.Object["metadata"].(map[string]any)["ownerReferences"] = []any{
		map[string]any{
			"apiVersion": "apps/v1", "kind": "Deployment",
			"name": "parent-deploy", "uid": "uid-parent", "controller": true,
		},
	}
	w.EventCh() <- ResourceEvent{Op: EventUpdate, Resource: childUpdated, GVR: testGVR}
	time.Sleep(250 * time.Millisecond)

	// Step 4: The flush should detect the parent is gone and emit an edge deletion.
	allDeltas := mock.getDeltas()
	var edgeDeleted bool
	for _, d := range allDeltas {
		for _, e := range d.edgeDels {
			if e.EdgeType == "ownedBy" && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
				edgeDeleted = true
			}
		}
	}
	if !edgeDeleted {
		t.Fatal("expected ownedBy edge deletion after parent was purged by resync")
	}
}

func TestResync_PurgeOnlyAffectsResyncdGVR(t *testing.T) {
	// Define a second GVR for ReplicaSets
	rsGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
	extendedSchema := map[schema.GroupVersionResource]SchemaEntry{
		testGVR: {
			GVR:  testGVR,
			Kind: "Deployment",
		},
		rsGVR: {
			GVR:  rsGVR,
			Kind: "ReplicaSet",
		},
	}

	mock := &mockInventoryWriter{}
	w := NewWriter("target-1", mock, extendedSchema, 100*time.Millisecond, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go w.Run(ctx)

	// Step 1: Add two deployments (deploy-1, deploy-2) and one ReplicaSet via events
	deploy1 := makeResource("uid-deploy-1", "deploy-1", "100")
	deploy2 := makeResource("uid-deploy-2", "deploy-2", "101")
	rs := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"metadata": map[string]any{
				"uid":               "uid-rs",
				"name":              "rs-1",
				"namespace":         "default",
				"resourceVersion":   "200",
				"creationTimestamp": "2025-06-01T12:00:00Z",
			},
		},
	}

	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: deploy1, GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: deploy2, GVR: testGVR}
	w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: rs, GVR: rsGVR}
	time.Sleep(250 * time.Millisecond)

	// Step 2: Resync deployments with only deploy-1 — should purge deploy-2 but not the ReplicaSet
	w.ResyncCh() <- ResyncEvent{GVR: testGVR, Resources: []*unstructured.Unstructured{deploy1}}
	time.Sleep(100 * time.Millisecond)

	// Step 3: Send a ReplicaSet update to trigger flush
	rsUpdated := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "apps/v1",
			"kind":       "ReplicaSet",
			"metadata": map[string]any{
				"uid":               "uid-rs",
				"name":              "rs-1",
				"namespace":         "default",
				"resourceVersion":   "201",
				"creationTimestamp": "2025-06-01T12:00:00Z",
			},
		},
	}
	w.EventCh() <- ResourceEvent{Op: EventUpdate, Resource: rsUpdated, GVR: rsGVR}
	time.Sleep(250 * time.Millisecond)

	// Step 4: Verify that the ReplicaSet persisted (resync of deployments shouldn't affect it)
	deltas := mock.getDeltas()
	var rsPersisted bool
	var deploy1Persisted bool
	for _, d := range deltas {
		for _, item := range d.upserts {
			if item.ID() == "target-1/uid-rs" {
				rsPersisted = true
			}
			if item.ID() == "target-1/uid-deploy-1" {
				deploy1Persisted = true
			}
		}
	}
	if !rsPersisted {
		t.Fatal("expected ReplicaSet to persist — resync of deployments should not purge ReplicaSets")
	}
	if !deploy1Persisted {
		t.Fatal("expected deploy-1 to persist — it was included in the resync")
	}
}
