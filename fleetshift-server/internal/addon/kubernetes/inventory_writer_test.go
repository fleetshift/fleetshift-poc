package kubernetes

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type deltaCall struct {
	delta InventoryDeltaReport
}

type replaceCall struct {
	snapshot InventoryCollectionSnapshot
}

type deleteCollectionCall struct {
	ref domain.InventoryCollectionRef
}

type recordingReporter struct {
	mu sync.Mutex

	deltas            []deltaCall
	replaces          []replaceCall
	deleteCollections []deleteCollectionCall
	deleteResources   [][]domain.InventoryResourceRef
	deleteSubtrees    []domain.InventorySubtreeRef

	applyDeltaFunc        func(context.Context, InventoryDeltaReport) error
	replaceCollectionFunc func(context.Context, InventoryCollectionSnapshot) error
	deleteCollectionFunc  func(context.Context, domain.InventoryCollectionRef) error
}

func (m *recordingReporter) ApplyDelta(ctx context.Context, delta InventoryDeltaReport) error {
	if m.applyDeltaFunc != nil {
		if err := m.applyDeltaFunc(ctx, delta); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deltas = append(m.deltas, deltaCall{delta: delta})
	return nil
}

func (m *recordingReporter) ReplaceCollection(ctx context.Context, snapshot InventoryCollectionSnapshot) error {
	if m.replaceCollectionFunc != nil {
		if err := m.replaceCollectionFunc(ctx, snapshot); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replaces = append(m.replaces, replaceCall{snapshot: snapshot})
	return nil
}

func (m *recordingReporter) DeleteResources(ctx context.Context, refs []domain.InventoryResourceRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]domain.InventoryResourceRef, len(refs))
	copy(copied, refs)
	m.deleteResources = append(m.deleteResources, copied)
	return nil
}

func (m *recordingReporter) DeleteCollection(ctx context.Context, ref domain.InventoryCollectionRef) error {
	if m.deleteCollectionFunc != nil {
		if err := m.deleteCollectionFunc(ctx, ref); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteCollections = append(m.deleteCollections, deleteCollectionCall{ref: ref})
	return nil
}

func (m *recordingReporter) DeleteSubtree(_ context.Context, ref domain.InventorySubtreeRef) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleteSubtrees = append(m.deleteSubtrees, ref)
	return nil
}

func (m *recordingReporter) getDeltas() []deltaCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]deltaCall, len(m.deltas))
	copy(out, m.deltas)
	return out
}

func (m *recordingReporter) getReplaces() []replaceCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]replaceCall, len(m.replaces))
	copy(out, m.replaces)
	return out
}

func (m *recordingReporter) getDeleteCollections() []deleteCollectionCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]deleteCollectionCall, len(m.deleteCollections))
	copy(out, m.deleteCollections)
	return out
}

type recordingEdgeSink struct {
	mu     sync.Mutex
	deltas []EdgeDelta
}

func (s *recordingEdgeSink) ApplyEdgeDelta(_ context.Context, _ domain.TargetID, delta EdgeDelta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deltas = append(s.deltas, delta)
	return nil
}

func (s *recordingEdgeSink) getDeltas() []EdgeDelta {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]EdgeDelta, len(s.deltas))
	copy(out, s.deltas)
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

func mustObjectName(t *testing.T, targetID, uid string, gvr schema.GroupVersionResource) domain.ResourceName {
	t.Helper()
	name, err := ObjectResourceName(KubernetesObjectIdentity{
		TargetID: domain.TargetID(targetID),
		GVR:      gvr,
		UID:      uid,
	})
	if err != nil {
		t.Fatalf("ObjectResourceName: %v", err)
	}
	return name
}

func mustObjectCollection(t *testing.T, targetID string, gvr schema.GroupVersionResource) domain.CollectionName {
	t.Helper()
	name, err := ObjectCollectionName(domain.TargetID(targetID), gvr)
	if err != nil {
		t.Fatalf("ObjectCollectionName: %v", err)
	}
	return name
}

func reportNames(reports []InventoryObjectReport) map[string]bool {
	names := make(map[string]bool, len(reports))
	for _, r := range reports {
		if n := r.Labels["k8s.name"]; n != "" {
			names[n] = true
		}
	}
	return names
}

func newTestWriter(reporter InventoryReporter, edgeSink EdgeSink, schema map[schema.GroupVersionResource]SchemaEntry, batchInterval time.Duration) *Writer {
	if schema == nil {
		schema = testSchema
	}
	return NewWriter("target-1", reporter, edgeSink, schema, batchInterval, discardLogger)
}

// advanceAndWait advances fake time inside a synctest bubble, then waits until
// all bubble goroutines are durably blocked again (writer idle in select / timers).
func advanceAndWait(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

func TestBatching(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-2", "deploy-2", "101"), GVR: testGVR}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-3", "deploy-3", "102"), GVR: testGVR}

		advanceAndWait(250 * time.Millisecond)

		deltas := mock.getDeltas()
		if len(deltas) == 0 {
			t.Fatal("expected at least one delta, got none")
		}
		first := deltas[0]
		if got := len(first.delta.Upserts); got != 3 {
			t.Fatalf("expected 3 upserts in first delta, got %d", got)
		}
		names := reportNames(first.delta.Upserts)
		for _, expected := range []string{"deploy-1", "deploy-2", "deploy-3"} {
			if !names[expected] {
				t.Errorf("expected item name %s in upserts", expected)
			}
		}
	})
}

func TestDelete_MapsToApplyDeltaDeletesWithObjectResourceName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: makeResource("uid-del", "deploy-del", "200"), GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		wantName := mustObjectName(t, "target-1", "uid-del", testGVR)
		deltas := mock.getDeltas()
		var found bool
		for _, d := range deltas {
			for _, ref := range d.delta.Deletes {
				if ref.ResourceType == ObjectResourceType && ref.Name == wantName {
					found = true
				}
			}
		}
		if !found {
			t.Fatalf("expected delete ref %s in ApplyDelta.Deletes, not found in %+v", wantName, deltas)
		}
		if len(mock.getDeleteCollections()) != 0 {
			t.Fatal("watch delete must not call DeleteCollection")
		}
	})
}

func TestResync_MapsToReplaceCollection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.ResyncCh() <- ResyncEvent{
			GVR: testGVR,
			Resources: []*unstructured.Unstructured{
				makeResource("uid-r1", "deploy-r1", "300"),
				makeResource("uid-r2", "deploy-r2", "301"),
			},
		}
		advanceAndWait(100 * time.Millisecond)

		replaces := mock.getReplaces()
		if len(replaces) == 0 {
			t.Fatal("expected at least one ReplaceCollection, got none")
		}
		rs := replaces[0]
		wantCollection := mustObjectCollection(t, "target-1", testGVR)
		if rs.snapshot.Collection != wantCollection {
			t.Errorf("collection = %q, want %q", rs.snapshot.Collection, wantCollection)
		}
		if len(rs.snapshot.Reports) != 2 {
			t.Fatalf("expected 2 reports in resync, got %d", len(rs.snapshot.Reports))
		}
	})
}

func TestDedup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventUpdate, Resource: makeResource("uid-dup", "deploy-dup", "500"), GVR: testGVR}
		w.EventCh() <- ResourceEvent{Op: EventUpdate, Resource: makeResource("uid-dup", "deploy-dup", "500"), GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		var upsertCount int
		for _, d := range mock.getDeltas() {
			for _, item := range d.delta.Upserts {
				if item.Labels["k8s.name"] == "deploy-dup" {
					upsertCount++
				}
			}
		}
		if upsertCount != 1 {
			t.Fatalf("expected 1 upsert for uid-dup (dedup), got %d", upsertCount)
		}
	})
}

func TestResync_MissingSchemaEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, testSchema, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

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
		advanceAndWait(100 * time.Millisecond)

		replaces := mock.getReplaces()
		if len(replaces) == 0 {
			t.Fatal("expected at least one ReplaceCollection, got none")
		}
		rs := replaces[0]
		wantCollection := mustObjectCollection(t, "target-1", cmGVR)
		if rs.snapshot.Collection != wantCollection {
			t.Errorf("collection = %q, want %q", rs.snapshot.Collection, wantCollection)
		}
		if len(rs.snapshot.Reports) != 1 {
			t.Fatalf("expected 1 report in resync, got %d", len(rs.snapshot.Reports))
		}
		if got := rs.snapshot.Reports[0].Labels["k8s.kind"]; got != "ConfigMap" {
			t.Errorf("k8s.kind = %q, want ConfigMap", got)
		}
	})
}

func TestLateDeleteProtection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: makeResource("uid-late", "deploy-late", "600"), GVR: testGVR}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-late", "deploy-late", "601"), GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		for _, d := range mock.getDeltas() {
			for _, item := range d.delta.Upserts {
				if item.Labels["k8s.name"] == "deploy-late" {
					t.Fatal("expected uid-late to be dropped by late-delete protection, but found in upserts")
				}
			}
		}
	})
}

func TestEdgeComputation_OwnedBy(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

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
		advanceAndWait(250 * time.Millisecond)

		var found bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Adds {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("expected ownedBy edge from child to parent")
		}
	})
}

func TestEdgeComputation_DeleteRemovesEdges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

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

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: child, GVR: testGVR}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: parent, GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: parent, GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		var deleted bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Deletes {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
					deleted = true
				}
			}
		}
		if !deleted {
			t.Fatal("expected ownedBy edge deletion after parent delete")
		}
	})
}

func TestEdgeComputation_DiffAcrossFlushes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

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

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: child, GVR: testGVR}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: parent, GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		if got := len(mock.getDeltas()); got < 1 {
			t.Fatalf("expected at least 1 inventory delta, got %d", got)
		}
		firstEdges := edges.getDeltas()
		if len(firstEdges) == 0 || len(firstEdges[0].Adds) == 0 {
			t.Fatal("expected at least one edge add in first flush")
		}

		parent2 := parent.DeepCopy()
		parent2.SetResourceVersion("201")
		w.EventCh() <- ResourceEvent{Op: EventUpdate, Resource: parent2, GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		if got := len(mock.getDeltas()); got < 2 {
			t.Fatalf("expected at least 2 inventory deltas, got %d", got)
		}
		// Unchanged edges are not re-emitted to the sink.
		for i, d := range edges.getDeltas() {
			if i == 0 {
				continue
			}
			if len(d.Adds) > 0 {
				t.Errorf("expected no edge adds after unchanged edge, flush %d got %d", i, len(d.Adds))
			}
		}
	})
}

func TestNoIdleHeartbeat(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Idle empty ApplyDelta heartbeats are intentionally not ported.
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 50*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		advanceAndWait(250 * time.Millisecond)

		if got := len(mock.getDeltas()); got != 0 {
			t.Fatalf("idle writer must not emit ApplyDelta heartbeats, got %d deltas", got)
		}
		if got := len(mock.getReplaces()); got != 0 {
			t.Fatalf("idle writer must not emit ReplaceCollection, got %d", got)
		}
		if got := len(mock.getDeleteCollections()); got != 0 {
			t.Fatalf("idle writer must not emit DeleteCollection, got %d", got)
		}
	})
}

func TestErrorRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attemptCount int
		var mu sync.Mutex
		mock := &recordingReporter{
			applyDeltaFunc: func(_ context.Context, _ InventoryDeltaReport) error {
				mu.Lock()
				defer mu.Unlock()
				attemptCount++
				if attemptCount <= 2 {
					return context.DeadlineExceeded
				}
				return nil
			},
		}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-retry", "deploy-retry", "100"), GVR: testGVR}
		advanceAndWait(4 * time.Second)

		mu.Lock()
		finalAttemptCount := attemptCount
		mu.Unlock()
		if finalAttemptCount < 3 {
			t.Fatalf("expected at least 3 attempts (initial + 2 retries), got %d", finalAttemptCount)
		}
	})
}

func TestEdgeComputation_BuildEdges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		testGVRWithEdges := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}
		testSchemaWithEdges := map[schema.GroupVersionResource]SchemaEntry{
			testGVRWithEdges: {
				GVR:  testGVRWithEdges,
				Kind: "Pod",
				BuildEdges: func(r *unstructured.Unstructured, uid string) func(NodeStore) []Edge {
					nodeName, _, _ := unstructured.NestedString(r.Object, "spec", "nodeName")
					return func(ns NodeStore) []Edge {
						if nodeName == "" {
							return nil
						}
						if nodeMap, ok := ns.ByKindNamespaceName["Node"]["_NONE"]; ok {
							if node, ok := nodeMap[nodeName]; ok {
								return []Edge{{
									EdgeType:   EdgeRunsOn,
									SourceUID:  uid,
									DestUID:    node.UID,
									SourceKind: "Pod",
									DestKind:   "Node",
								}}
							}
						}
						return nil
					}
				},
			},
			{Group: "", Version: "v1", Resource: "nodes"}: {
				GVR:  schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"},
				Kind: "Node",
			},
		}

		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, testSchemaWithEdges, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		nodeGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
		node := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Node",
			"metadata": map[string]any{
				"uid": "uid-node", "name": "worker-1",
				"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
		}}
		pod := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{
				"uid": "uid-pod", "name": "my-pod", "namespace": "default",
				"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
			"spec": map[string]any{"nodeName": "worker-1"},
		}}

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: node, GVR: nodeGVR}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pod, GVR: testGVRWithEdges}
		advanceAndWait(250 * time.Millisecond)

		var found bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Adds {
				if e.EdgeType == EdgeRunsOn && e.SourceUID == "uid-pod" && e.DestUID == "uid-node" {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("expected runsOn edge from pod to node")
		}
	})
}

func TestResync_DoesNotClobberFlushEdges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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

		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, crossSchema, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

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

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pv, GVR: pvGVR}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pvc, GVR: pvcGVR}
		advanceAndWait(250 * time.Millisecond)

		var flushHasEdge bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Adds {
				if e.EdgeType == EdgeAttachedTo && e.SourceUID == "uid-pvc" && e.DestUID == "uid-pv" {
					flushHasEdge = true
				}
			}
		}
		if !flushHasEdge {
			t.Fatal("flush should have produced PVC→PV edge")
		}

		w.ResyncCh() <- ResyncEvent{GVR: pvcGVR, Resources: []*unstructured.Unstructured{pvc}}
		advanceAndWait(100 * time.Millisecond)
		if len(mock.getReplaces()) == 0 {
			t.Fatal("expected ReplaceCollection call")
		}

		for _, d := range edges.getDeltas() {
			for _, e := range d.Deletes {
				if e.EdgeType == EdgeAttachedTo && e.SourceUID == "uid-pvc" {
					t.Fatal("resync should not cause PVC→PV edge deletion in subsequent flush")
				}
			}
		}
	})
}

func TestResync_UpdatesWriterState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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

		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, testSchemaCrossGVR, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		pv := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "PersistentVolume",
			"metadata": map[string]any{
				"uid": "uid-pv", "name": "my-pv",
				"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
		}}
		w.ResyncCh() <- ResyncEvent{GVR: pvGVR, Resources: []*unstructured.Unstructured{pv}}
		advanceAndWait(100 * time.Millisecond)

		pvc := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "PersistentVolumeClaim",
			"metadata": map[string]any{
				"uid": "uid-pvc", "name": "my-pvc", "namespace": "default",
				"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
			"spec": map[string]any{"volumeName": "my-pv"},
		}}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pvc, GVR: pvcGVR}
		advanceAndWait(250 * time.Millisecond)

		var found bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Adds {
				if e.EdgeType == EdgeAttachedTo && e.SourceUID == "uid-pvc" && e.DestUID == "uid-pv" {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("flush after resync should see resynced PV in currentNodes and produce PVC→PV edge")
		}
	})
}

func TestResync_OwnedByEdgesAfterCrossGVRResync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rsGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
		podGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
		testSchemaOwnedBy := map[schema.GroupVersionResource]SchemaEntry{
			rsGVR:  {GVR: rsGVR, Kind: "ReplicaSet"},
			podGVR: {GVR: podGVR, Kind: "Pod"},
		}

		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, testSchemaOwnedBy, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		rs := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{
				"uid": "uid-rs", "name": "my-rs", "namespace": "default",
				"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
		}}
		w.ResyncCh() <- ResyncEvent{GVR: rsGVR, Resources: []*unstructured.Unstructured{rs}}
		advanceAndWait(100 * time.Millisecond)

		pod := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{
				"uid": "uid-pod", "name": "my-pod", "namespace": "default",
				"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
				"ownerReferences": []any{
					map[string]any{
						"apiVersion": "apps/v1", "kind": "ReplicaSet",
						"name": "my-rs", "uid": "uid-rs", "controller": true,
					},
				},
			},
		}}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pod, GVR: podGVR}
		advanceAndWait(250 * time.Millisecond)

		var found bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Adds {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-pod" && e.DestUID == "uid-rs" {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("expected ownedBy edge after cross-GVR resync + event")
		}
	})
}

func TestResync_DoesNotClobberOwnedByEdges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rsGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
		podGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
		testSchemaOwnedBy := map[schema.GroupVersionResource]SchemaEntry{
			rsGVR:  {GVR: rsGVR, Kind: "ReplicaSet"},
			podGVR: {GVR: podGVR, Kind: "Pod"},
		}

		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, testSchemaOwnedBy, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		rs := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{
				"uid": "uid-rs", "name": "my-rs", "namespace": "default",
				"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
		}}
		pod := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{
				"uid": "uid-pod", "name": "my-pod", "namespace": "default",
				"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
				"ownerReferences": []any{
					map[string]any{
						"apiVersion": "apps/v1", "kind": "ReplicaSet",
						"name": "my-rs", "uid": "uid-rs", "controller": true,
					},
				},
			},
		}}

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: rs, GVR: rsGVR}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pod, GVR: podGVR}
		advanceAndWait(250 * time.Millisecond)

		w.ResyncCh() <- ResyncEvent{GVR: podGVR, Resources: []*unstructured.Unstructured{pod}}
		advanceAndWait(100 * time.Millisecond)

		for _, d := range edges.getDeltas() {
			for _, e := range d.Deletes {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-pod" {
					t.Fatal("pod resync must not delete ownedBy edge to ReplicaSet")
				}
			}
		}
	})
}

func TestEdgeComputation_MultipleEdgeTypesToSameDest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		podGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
		schemaMultiEdge := map[schema.GroupVersionResource]SchemaEntry{
			podGVR: {
				GVR:  podGVR,
				Kind: "Pod",
				BuildEdges: func(_ *unstructured.Unstructured, uid string) func(NodeStore) []Edge {
					return func(NodeStore) []Edge {
						return []Edge{
							{EdgeType: EdgeRunsOn, SourceUID: uid, DestUID: "uid-dest", SourceKind: "Pod", DestKind: "Node"},
							{EdgeType: EdgeAttachedTo, SourceUID: uid, DestUID: "uid-dest", SourceKind: "Pod", DestKind: "Node"},
						}
					}
				},
			},
		}

		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, schemaMultiEdge, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		pod := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{
				"uid": "uid-pod", "name": "my-pod", "namespace": "default",
				"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
		}}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pod, GVR: podGVR}
		advanceAndWait(250 * time.Millisecond)

		types := map[EdgeType]bool{}
		for _, d := range edges.getDeltas() {
			for _, e := range d.Adds {
				if e.SourceUID == "uid-pod" && e.DestUID == "uid-dest" {
					types[e.EdgeType] = true
				}
			}
		}
		if !types[EdgeRunsOn] || !types[EdgeAttachedTo] {
			t.Fatalf("expected both runsOn and attachedTo edges, got %v", types)
		}
	})
}

func TestEdgeComputation_MultipleEdgeTypes_DeleteRemovesBoth(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		podGVR := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
		schemaMultiEdge := map[schema.GroupVersionResource]SchemaEntry{
			podGVR: {
				GVR:  podGVR,
				Kind: "Pod",
				BuildEdges: func(_ *unstructured.Unstructured, uid string) func(NodeStore) []Edge {
					return func(NodeStore) []Edge {
						return []Edge{
							{EdgeType: EdgeRunsOn, SourceUID: uid, DestUID: "uid-dest", SourceKind: "Pod", DestKind: "Node"},
							{EdgeType: EdgeAttachedTo, SourceUID: uid, DestUID: "uid-dest", SourceKind: "Pod", DestKind: "Node"},
						}
					}
				},
			},
		}

		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, schemaMultiEdge, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		pod := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "Pod",
			"metadata": map[string]any{
				"uid": "uid-pod", "name": "my-pod", "namespace": "default",
				"resourceVersion": "100", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
		}}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: pod, GVR: podGVR}
		advanceAndWait(250 * time.Millisecond)

		w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: pod, GVR: podGVR}
		advanceAndWait(250 * time.Millisecond)

		deleted := map[EdgeType]bool{}
		for _, d := range edges.getDeltas() {
			for _, e := range d.Deletes {
				if e.SourceUID == "uid-pod" && e.DestUID == "uid-dest" {
					deleted[e.EdgeType] = true
				}
			}
		}
		if !deleted[EdgeRunsOn] || !deleted[EdgeAttachedTo] {
			t.Fatalf("expected both edge types deleted, got %v", deleted)
		}
	})
}

func TestShutdownFlush_PersistsPendingEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx)
		}()
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-2", "deploy-2", "101"), GVR: testGVR}
		advanceAndWait(50 * time.Millisecond)

		cancel()
		<-done

		names := map[string]bool{}
		for _, d := range mock.getDeltas() {
			for _, item := range d.delta.Upserts {
				names[item.Labels["k8s.name"]] = true
			}
		}
		if !names["deploy-1"] || !names["deploy-2"] {
			t.Fatalf("shutdown flush missing events: got %v", names)
		}
		if len(mock.getDeleteCollections()) != 0 {
			t.Fatal("informer shutdown must not call DeleteCollection")
		}
	})
}

func TestShutdown_DoesNotPersistCacheEvictionDeletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// After objects are flushed, cancelling the writer (indexer StopAll /
		// StopTarget path) must not invent deletes for local cache eviction.
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx)
		}()
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)
		if len(mock.getDeltas()) == 0 {
			t.Fatal("precondition: expected initial upsert flush")
		}

		cancel()
		<-done

		for _, d := range mock.getDeltas() {
			if len(d.delta.Deletes) > 0 {
				t.Fatalf("shutdown must not persist cache-eviction deletes, got %+v", d.delta.Deletes)
			}
		}
		if len(mock.getDeleteCollections()) != 0 {
			t.Fatal("shutdown must not call DeleteCollection")
		}
	})
}

func TestFlushFailure_ItemsRetriedOnNextTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var callCount int
		mock := &recordingReporter{
			applyDeltaFunc: func(_ context.Context, _ InventoryDeltaReport) error {
				mu.Lock()
				defer mu.Unlock()
				callCount++
				if callCount <= 3 {
					return context.DeadlineExceeded
				}
				return nil
			},
		}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		advanceAndWait(9 * time.Second)

		var found bool
		for _, d := range mock.getDeltas() {
			for _, item := range d.delta.Upserts {
				if item.Labels["k8s.name"] == "deploy-1" {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("expected deploy-1 to be retried and persisted after failed flush")
		}
	})
}

func TestFlushFailure_EdgesRetriedOnNextTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var callCount int
		mock := &recordingReporter{
			applyDeltaFunc: func(_ context.Context, _ InventoryDeltaReport) error {
				mu.Lock()
				defer mu.Unlock()
				callCount++
				if callCount <= 3 {
					return context.DeadlineExceeded
				}
				return nil
			},
		}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

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
		advanceAndWait(9 * time.Second)

		var found bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Adds {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("expected ownedBy edge to be retried after failed flush")
		}
	})
}

func TestFlushFailure_DeletesRetriedOnNextTick(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var callCount int
		mock := &recordingReporter{
			applyDeltaFunc: func(_ context.Context, _ InventoryDeltaReport) error {
				mu.Lock()
				defer mu.Unlock()
				callCount++
				if callCount <= 3 {
					return context.DeadlineExceeded
				}
				return nil
			},
		}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: makeResource("uid-del", "deploy-del", "100"), GVR: testGVR}
		advanceAndWait(9 * time.Second)

		wantName := mustObjectName(t, "target-1", "uid-del", testGVR)
		var found bool
		for _, d := range mock.getDeltas() {
			for _, ref := range d.delta.Deletes {
				if ref.Name == wantName {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("expected delete to be retried after failed flush")
		}
	})
}

func TestFlushFailure_NewEventsMergedBetweenRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var callCount int
		var firstFlushComplete bool
		mock := &recordingReporter{
			applyDeltaFunc: func(_ context.Context, _ InventoryDeltaReport) error {
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
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		advanceAndWait(7500 * time.Millisecond)

		mu.Lock()
		waitingForRetry := firstFlushComplete
		mu.Unlock()
		if !waitingForRetry {
			t.Fatal("test timing issue: first flush did not complete retries")
		}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-2", "deploy-2", "101"), GVR: testGVR}
		advanceAndWait(2 * time.Second)

		names := map[string]bool{}
		for _, d := range mock.getDeltas() {
			for _, item := range d.delta.Upserts {
				names[item.Labels["k8s.name"]] = true
			}
		}
		if !names["deploy-1"] {
			t.Fatal("expected deploy-1 to be persisted after failed flush")
		}
		if !names["deploy-2"] {
			t.Fatal("expected deploy-2 to be merged into the retry batch")
		}
	})
}

func TestResync_RetriesOnFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attemptCount int
		var mu sync.Mutex
		mock := &recordingReporter{
			replaceCollectionFunc: func(_ context.Context, _ InventoryCollectionSnapshot) error {
				mu.Lock()
				defer mu.Unlock()
				attemptCount++
				if attemptCount <= 2 {
					return context.DeadlineExceeded
				}
				return nil
			},
		}
		w := newTestWriter(mock, nil, nil, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.ResyncCh() <- ResyncEvent{
			GVR:       testGVR,
			Resources: []*unstructured.Unstructured{makeResource("uid-r1", "deploy-r1", "300")},
		}
		advanceAndWait(4 * time.Second)

		if len(mock.getReplaces()) == 0 {
			t.Fatal("expected resync to succeed after retries")
		}
	})
}

func TestResync_PurgesStaleNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		edges := &recordingEdgeSink{}
		w := newTestWriter(mock, edges, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

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
		advanceAndWait(250 * time.Millisecond)

		var edgeCreated bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Adds {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
					edgeCreated = true
				}
			}
		}
		if !edgeCreated {
			t.Fatal("precondition: expected ownedBy edge from child to parent")
		}

		w.ResyncCh() <- ResyncEvent{GVR: testGVR, Resources: []*unstructured.Unstructured{child}}
		advanceAndWait(100 * time.Millisecond)

		childUpdated := makeResource("uid-child", "child-deploy", "101")
		childUpdated.Object["metadata"].(map[string]any)["ownerReferences"] = []any{
			map[string]any{
				"apiVersion": "apps/v1", "kind": "Deployment",
				"name": "parent-deploy", "uid": "uid-parent", "controller": true,
			},
		}
		w.EventCh() <- ResourceEvent{Op: EventUpdate, Resource: childUpdated, GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		var edgeDeleted bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Deletes {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
					edgeDeleted = true
				}
			}
		}
		if !edgeDeleted {
			t.Fatal("expected ownedBy edge deletion after parent was purged by resync")
		}
	})
}

func TestResync_PurgeOnlyAffectsResyncdGVR(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rsGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
		extendedSchema := map[schema.GroupVersionResource]SchemaEntry{
			testGVR: {GVR: testGVR, Kind: "Deployment"},
			rsGVR:   {GVR: rsGVR, Kind: "ReplicaSet"},
		}

		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, extendedSchema, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx)
		}()
		synctest.Wait()

		deploy1 := makeResource("uid-deploy-1", "deploy-1", "100")
		deploy2 := makeResource("uid-deploy-2", "deploy-2", "101")
		rs := &unstructured.Unstructured{
			Object: map[string]any{
				"apiVersion": "apps/v1",
				"kind":       "ReplicaSet",
				"metadata": map[string]any{
					"uid":               "uid-rs-1",
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
		advanceAndWait(250 * time.Millisecond)

		w.ResyncCh() <- ResyncEvent{GVR: testGVR, Resources: []*unstructured.Unstructured{deploy1}}
		advanceAndWait(100 * time.Millisecond)

		cancel()
		<-done

		if _, ok := w.currentNodes["uid-deploy-2"]; ok {
			t.Fatal("deploy-2 should have been purged by deployment resync")
		}
		if _, ok := w.currentNodes["uid-rs-1"]; !ok {
			t.Fatal("ReplicaSet node must survive deployment-only resync")
		}
		if _, ok := w.currentNodes["uid-deploy-1"]; !ok {
			t.Fatal("deploy-1 should remain after resync")
		}
	})
}

func TestRemoveGVR_MapsToDeleteCollection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx)
		}()
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)
		if len(mock.getDeltas()) == 0 {
			t.Fatal("precondition: expected flush before RemoveGVR")
		}

		w.RemoveCh() <- RemoveGVREvent{GVR: testGVR}
		advanceAndWait(100 * time.Millisecond)

		wantCollection := mustObjectCollection(t, "target-1", testGVR)
		calls := mock.getDeleteCollections()
		if len(calls) != 1 {
			t.Fatalf("DeleteCollection calls = %d, want 1", len(calls))
		}
		if calls[0].ref.ResourceType != ObjectResourceType {
			t.Errorf("ResourceType = %q, want %q", calls[0].ref.ResourceType, ObjectResourceType)
		}
		if calls[0].ref.Collection != wantCollection {
			t.Errorf("Collection = %q, want %q", calls[0].ref.Collection, wantCollection)
		}

		cancel()
		<-done
		if _, ok := w.currentNodes["uid-1"]; ok {
			t.Fatal("GVR removal should drop in-memory nodes for that GVR")
		}
	})
}

func TestWriter_NoopEdgeSinkKeepsInventoryWritesSuccessful(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, NoopEdgeSink{}, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		if len(mock.getDeltas()) == 0 {
			t.Fatal("expected inventory ApplyDelta with NoopEdgeSink")
		}
	})
}

func TestResync_EmptySnapshotPrunesCollection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.ResyncCh() <- ResyncEvent{GVR: testGVR, Resources: nil}
		advanceAndWait(100 * time.Millisecond)

		replaces := mock.getReplaces()
		if len(replaces) != 1 {
			t.Fatalf("ReplaceCollection calls = %d, want 1", len(replaces))
		}
		want := mustObjectCollection(t, "target-1", testGVR)
		if replaces[0].snapshot.Collection != want {
			t.Errorf("collection = %q, want %q", replaces[0].snapshot.Collection, want)
		}
		if len(replaces[0].snapshot.Reports) != 0 {
			t.Fatalf("empty LIST must replace with 0 reports, got %d", len(replaces[0].snapshot.Reports))
		}
	})
}

func TestFlush_UpsertAndDeleteDifferentUIDsSameBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-add", "deploy-add", "100"), GVR: testGVR}
		w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: makeResource("uid-del", "deploy-del", "200"), GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		wantDel := mustObjectName(t, "target-1", "uid-del", testGVR)
		var sawAdd, sawDel bool
		for _, d := range mock.getDeltas() {
			for _, u := range d.delta.Upserts {
				if u.Labels["k8s.uid"] == "uid-add" {
					sawAdd = true
				}
			}
			for _, ref := range d.delta.Deletes {
				if ref.Name == wantDel {
					sawDel = true
				}
			}
		}
		if !sawAdd || !sawDel {
			t.Fatalf("expected same-batch upsert+delete, sawAdd=%v sawDel=%v", sawAdd, sawDel)
		}
	})
}

func TestRemoveGVR_DropsPendingForRemovedGVROnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rsGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
		schemaBoth := map[schema.GroupVersionResource]SchemaEntry{
			testGVR: {GVR: testGVR, Kind: "Deployment"},
			rsGVR:   {GVR: rsGVR, Kind: "ReplicaSet"},
		}
		mock := &recordingReporter{}
		// Long batch interval so pending stays until RemoveGVR, then shutdown flush.
		w := newTestWriter(mock, nil, schemaBoth, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx)
		}()
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-deploy", "deploy-1", "100"), GVR: testGVR}
		rs := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{
				"uid": "uid-rs", "name": "rs-1", "namespace": "default",
				"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
		}}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: rs, GVR: rsGVR}
		advanceAndWait(50 * time.Millisecond)

		w.RemoveCh() <- RemoveGVREvent{GVR: testGVR}
		advanceAndWait(50 * time.Millisecond)

		wantDeleted := mustObjectCollection(t, "target-1", testGVR)
		if calls := mock.getDeleteCollections(); len(calls) != 1 || calls[0].ref.Collection != wantDeleted {
			t.Fatalf("DeleteCollection = %+v, want %q", mock.getDeleteCollections(), wantDeleted)
		}

		cancel()
		<-done

		var sawDeploy, sawRS bool
		for _, d := range mock.getDeltas() {
			for _, u := range d.delta.Upserts {
				switch u.Labels["k8s.uid"] {
				case "uid-deploy":
					sawDeploy = true
				case "uid-rs":
					sawRS = true
				}
			}
		}
		if sawDeploy {
			t.Fatal("pending deploy upsert must be dropped when its GVR is removed")
		}
		if !sawRS {
			t.Fatal("pending ReplicaSet upsert for a different GVR must still flush")
		}
	})
}

func TestRemoveGVR_RetriesDeleteCollection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var attempts int
		var mu sync.Mutex
		mock := &recordingReporter{
			deleteCollectionFunc: func(_ context.Context, _ domain.InventoryCollectionRef) error {
				mu.Lock()
				defer mu.Unlock()
				attempts++
				if attempts <= 2 {
					return context.DeadlineExceeded
				}
				return nil
			},
		}
		w := newTestWriter(mock, nil, nil, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.RemoveCh() <- RemoveGVREvent{GVR: testGVR}
		advanceAndWait(4 * time.Second)

		mu.Lock()
		got := attempts
		mu.Unlock()
		if got < 3 {
			t.Fatalf("DeleteCollection attempts = %d, want at least 3", got)
		}
		if len(mock.getDeleteCollections()) != 1 {
			t.Fatalf("successful DeleteCollection recordings = %d, want 1", len(mock.getDeleteCollections()))
		}
	})
}

func TestFlush_SkipsExtractionFailureWithoutDroppingSibling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		// Empty UID cannot form ObjectResourceName — extraction fails and is skipped.
		bad := makeResource("", "deploy-bad", "100")
		bad.Object["metadata"].(map[string]any)["uid"] = ""
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: bad, GVR: testGVR}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-ok", "deploy-ok", "101"), GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		var sawOK, sawBad bool
		for _, d := range mock.getDeltas() {
			for _, u := range d.delta.Upserts {
				switch u.Labels["k8s.name"] {
				case "deploy-ok":
					sawOK = true
				case "deploy-bad":
					sawBad = true
				}
			}
		}
		if !sawOK {
			t.Fatal("expected sibling upsert to succeed when one extraction fails")
		}
		if sawBad {
			t.Fatal("empty-UID object must not be reported")
		}
	})
}

func TestFlush_NilReporterIsNoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Join Run before reading currentNodes to avoid a data race with flush.
		w := NewWriter("target-1", nil, nil, testSchema, 100*time.Millisecond, discardLogger)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx)
		}()
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)

		cancel()
		<-done

		// No panic is success; currentNodes still advance for edge state.
		if _, ok := w.currentNodes["uid-1"]; !ok {
			t.Fatal("nil reporter should still track currentNodes after extract")
		}
	})
}

func TestDelete_UsesEventGVRForResourceName(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cmGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "configmaps"}
		schemaBoth := map[schema.GroupVersionResource]SchemaEntry{
			testGVR: {GVR: testGVR, Kind: "Deployment"},
			cmGVR:   {GVR: cmGVR, Kind: "ConfigMap"},
		}
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, schemaBoth, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		cm := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "v1", "kind": "ConfigMap",
			"metadata": map[string]any{
				"uid": "uid-cm", "name": "my-cm", "namespace": "default",
				"resourceVersion": "1", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
		}}
		w.EventCh() <- ResourceEvent{Op: EventDelete, Resource: cm, GVR: cmGVR}
		advanceAndWait(250 * time.Millisecond)

		want := mustObjectName(t, "target-1", "uid-cm", cmGVR)
		wrong := mustObjectName(t, "target-1", "uid-cm", testGVR)
		var found bool
		for _, d := range mock.getDeltas() {
			for _, ref := range d.delta.Deletes {
				if ref.Name == want {
					found = true
				}
				if ref.Name == wrong {
					t.Fatal("delete ResourceName must use event GVR, not an unrelated schema GVR")
				}
			}
		}
		if !found {
			t.Fatalf("expected delete ref %s", want)
		}
	})
}

func TestInformerReconcile_RemoveGVR_WriterDeletesCollection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// End-to-end: InformerManager reconcile drop → RemoveGVREvent → writer DeleteCollection.
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		disc := newFakeDiscovery(nil)
		mgr := NewInformerManager(nil, disc, w.EventCh(), w.ResyncCh(), w.RemoveCh(), nil, discardLogger)
		mgr.stoppers[testGVR] = func() {}
		mgr.stoppers[schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}] = func() {}

		mgr.Reconcile(ctx, []schema.GroupVersionResource{podsGVR()})
		advanceAndWait(100 * time.Millisecond)

		want := mustObjectCollection(t, "target-1", testGVR)
		calls := mock.getDeleteCollections()
		if len(calls) != 1 {
			t.Fatalf("DeleteCollection calls = %d, want 1", len(calls))
		}
		if calls[0].ref.Collection != want {
			t.Errorf("collection = %q, want %q", calls[0].ref.Collection, want)
		}
	})
}

func TestNewWriter_NilEdgeSinkAndLoggerDefaults(t *testing.T) {
	w := NewWriter("target-1", &recordingReporter{}, nil, testSchema, time.Second, nil)
	if _, ok := w.edgeSink.(NoopEdgeSink); !ok {
		t.Fatalf("nil edgeSink default = %T, want NoopEdgeSink", w.edgeSink)
	}
	if w.logger == nil {
		t.Fatal("nil logger must default to non-nil")
	}
}

func TestFlush_EdgeSinkFailureDoesNotAdvancePreviousEdges(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		sink := &failingEdgeSink{err: context.DeadlineExceeded}
		w := newTestWriter(mock, sink, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		child := makeResource("uid-child", "child-deploy", "100")
		child.Object["metadata"].(map[string]any)["ownerReferences"] = []any{
			map[string]any{
				"apiVersion": "apps/v1", "kind": "ReplicaSet",
				"name": "parent-rs", "uid": "uid-parent", "controller": true,
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
		advanceAndWait(250 * time.Millisecond)

		if len(w.previousEdges) != 0 {
			t.Fatalf("edge sink failure must not advance previousEdges, got %d", len(w.previousEdges))
		}
		if len(mock.getDeltas()) == 0 {
			t.Fatal("inventory ApplyDelta should still have been attempted before edge sink failure")
		}
	})
}

type failingEdgeSink struct {
	err error
}

func (s *failingEdgeSink) ApplyEdgeDelta(context.Context, domain.TargetID, EdgeDelta) error {
	return s.err
}

func TestResync_SkipsExtractionFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		bad := makeResource("", "deploy-bad", "100")
		bad.Object["metadata"].(map[string]any)["uid"] = ""
		w.ResyncCh() <- ResyncEvent{
			GVR: testGVR,
			Resources: []*unstructured.Unstructured{
				bad,
				makeResource("uid-ok", "deploy-ok", "101"),
			},
		}
		advanceAndWait(100 * time.Millisecond)

		replaces := mock.getReplaces()
		if len(replaces) != 1 {
			t.Fatalf("ReplaceCollection calls = %d, want 1", len(replaces))
		}
		if len(replaces[0].snapshot.Reports) != 1 {
			t.Fatalf("reports = %d, want 1 (bad UID skipped)", len(replaces[0].snapshot.Reports))
		}
		if replaces[0].snapshot.Reports[0].Labels["k8s.uid"] != "uid-ok" {
			t.Fatalf("expected uid-ok report, got %+v", replaces[0].snapshot.Reports[0].Labels)
		}
	})
}

func TestRemoveGVR_NilReporterIsNoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Drive RemoveGVR through Run, then join before reading currentNodes.
		w := NewWriter("target-1", nil, nil, testSchema, 50*time.Millisecond, discardLogger)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx)
		}()
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		advanceAndWait(150 * time.Millisecond)

		w.RemoveCh() <- RemoveGVREvent{GVR: testGVR}
		advanceAndWait(50 * time.Millisecond)

		cancel()
		<-done

		if _, ok := w.currentNodes["uid-1"]; ok {
			t.Fatal("removeGVR should drop in-memory nodes even when reporter is nil")
		}
	})
}
