package kubernetes

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	k8stesting "k8s.io/client-go/testing"
)

type deltaCall struct {
	delta InventoryDeltaReport
}

type recordingReporter struct {
	mu sync.Mutex

	deltas []deltaCall

	applyDeltaFunc func(context.Context, InventoryDeltaReport) error
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

func (m *recordingReporter) getDeltas() []deltaCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]deltaCall, len(m.deltas))
	copy(out, m.deltas)
	return out
}

type recordingEdgeSink struct {
	mu     sync.Mutex
	deltas []EdgeDelta
}

func (s *recordingEdgeSink) ApplyEdgeDelta(_ context.Context, _ domain.ResourceName, delta EdgeDelta) error {
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

func mustObjectName(t *testing.T, clusterID, uid string, gvr schema.GroupVersionResource) domain.ResourceName {
	t.Helper()
	name, err := ObjectResourceName(KubernetesObjectIdentity{
		ClusterResourceName: testClusterResourceName(clusterID),
		GVR:                 gvr,
		UID:                 uid,
	})
	if err != nil {
		t.Fatalf("ObjectResourceName: %v", err)
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
	return NewWriter("clusters/target-1", reporter, edgeSink, schema, batchInterval, discardLogger)
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
			for _, del := range d.delta.Deletes {
				if del.Name == wantName && del.IsDelete {
					found = true
				}
			}
		}
		if !found {
			t.Fatalf("expected IsDelete for %s in ApplyDelta.Deletes, not found in %+v", wantName, deltas)
		}
	})
}

func TestResync_MapsToApplyDelta(t *testing.T) {
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

		deltas := mock.getDeltas()
		if len(deltas) == 0 {
			t.Fatal("expected at least one ApplyDelta, got none")
		}
		first := deltas[0]
		if got := len(first.delta.Upserts); got != 2 {
			t.Fatalf("expected 2 upserts in resync, got %d", got)
		}
		if len(first.delta.Deletes) != 0 {
			t.Fatalf("first resync with no prior nodes must not emit deletes, got %d", len(first.delta.Deletes))
		}
		names := reportNames(first.delta.Upserts)
		for _, expected := range []string{"deploy-r1", "deploy-r2"} {
			if !names[expected] {
				t.Errorf("expected item name %s in upserts", expected)
			}
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

		deltas := mock.getDeltas()
		if len(deltas) == 0 {
			t.Fatal("expected at least one ApplyDelta, got none")
		}
		first := deltas[0]
		if len(first.delta.Upserts) != 1 {
			t.Fatalf("expected 1 upsert in resync, got %d", len(first.delta.Upserts))
		}
		if got := first.delta.Upserts[0].Labels["k8s.kind"]; got != "ConfigMap" {
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

func TestApplyDeltaWithRetry_NoSleepAfterFinalFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{
			applyDeltaFunc: func(context.Context, InventoryDeltaReport) error {
				return errors.New("permanent failure")
			},
		}
		w := newTestWriter(mock, nil, nil, time.Hour)

		start := time.Now()
		err := w.applyDeltaWithRetry(context.Background(), InventoryDeltaReport{
			Upserts: []InventoryObjectReport{{Name: domain.ResourceName("clusters/t/objects/uid-1")}},
		})
		elapsed := time.Since(start)
		if err == nil {
			t.Fatal("expected permanent ApplyDelta failure")
		}
		// Backoff sleeps only between attempts: 1s + 2s. A sleep after the
		// final attempt would push this to ~7s (extra 4s).
		if elapsed != 3*time.Second {
			t.Fatalf("elapsed = %v, want 3s (no backoff after final failed attempt)", elapsed)
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
		deltas := mock.getDeltas()
		if len(deltas) == 0 {
			t.Fatal("expected ApplyDelta call from resync")
		}
		last := deltas[len(deltas)-1]
		if len(last.delta.Upserts) != 1 {
			t.Fatalf("expected 1 upsert in resync delta, got %d", len(last.delta.Upserts))
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

		if len(mock.getDeltas()) == 0 {
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

		var edgeDeleted bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Deletes {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
					edgeDeleted = true
				}
			}
		}
		if !edgeDeleted {
			t.Fatal("expected ownedBy edge deletion from resync alone, without a follow-up object event")
		}
	})
}

func TestRemoveGVR_FlushesEdgeDeletes(t *testing.T) {
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

		w.RemoveCh() <- RemoveGVREvent{GVR: testGVR}
		synctest.Wait()

		var edgeDeleted bool
		for _, d := range edges.getDeltas() {
			for _, e := range d.Deletes {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
					edgeDeleted = true
				}
			}
		}
		if !edgeDeleted {
			t.Fatal("expected ownedBy edge deletion from RemoveGVR without a follow-up object event")
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

func TestRemoveGVR_DoesNotPersistDeletes(t *testing.T) {
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
		deltaCountBefore := len(mock.getDeltas())

		w.RemoveCh() <- RemoveGVREvent{GVR: testGVR}
		advanceAndWait(100 * time.Millisecond)

		for _, d := range mock.getDeltas()[deltaCountBefore:] {
			if len(d.delta.Deletes) > 0 {
				t.Fatalf("RemoveGVR must not persist deletes, got %+v", d.delta.Deletes)
			}
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

// TestWriter_NoopEdgeSinkSkipsEdgeDiff ensures flushEdges short-circuits
// for NoopEdgeSink: edge computation is skipped and previousEdges stays
// empty even when objects would otherwise produce OwnedBy edges.
func TestWriter_NoopEdgeSinkSkipsEdgeDiff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, NoopEdgeSink{}, nil, 100*time.Millisecond)

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

		if len(mock.getDeltas()) == 0 {
			t.Fatal("expected inventory ApplyDelta with NoopEdgeSink")
		}
		if len(w.previousEdges) != 0 {
			t.Fatalf("NoopEdgeSink must skip edge diff; previousEdges=%d want 0", len(w.previousEdges))
		}
		if len(w.currentNodes) == 0 {
			t.Fatal("expected currentNodes populated so a real sink would have edges to diff")
		}
	})
}

// TestWriter_flushEdges_NoopSkipsDiffEvenWithEdgeFuncs is a direct unit
// check: with edgeFuncs that would produce edges, flushEdges on
// NoopEdgeSink returns without advancing previousEdges.
func TestWriter_flushEdges_NoopSkipsDiffEvenWithEdgeFuncs(t *testing.T) {
	w := NewWriter("clusters/target-1", &recordingReporter{}, NoopEdgeSink{}, nil, time.Hour, discardLogger)
	w.currentNodes["uid-a"] = inventoryNode{UID: "uid-a", Kind: "Pod", Name: "a"}
	w.currentNodes["uid-b"] = inventoryNode{UID: "uid-b", Kind: "ReplicaSet", Name: "b"}
	w.edgeFuncs["uid-a"] = func(NodeStore) []Edge {
		return []Edge{{
			EdgeType:  EdgeOwnedBy,
			SourceUID: "uid-a",
			DestUID:   "uid-b",
			DestKind:  "ReplicaSet",
		}}
	}
	// Poison previousEdges so accidental diff+commit would be visible.
	w.previousEdges = map[edgeKey]Edge{}

	if err := w.flushEdges(context.Background()); err != nil {
		t.Fatalf("flushEdges: %v", err)
	}
	if len(w.previousEdges) != 0 {
		t.Fatalf("NoopEdgeSink flushEdges must not commit edges, got %d", len(w.previousEdges))
	}
}

// TestResync_EmptySnapshotDeletesReportedUIDs verifies a later empty
// LIST emits IsDelete only for UIDs acknowledged in this generation's
// ReportedUIDs (same-process omission reconciliation — not a DB
// collection wipe of unknown rows).
func TestResync_EmptySnapshotDeletesReportedUIDs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.ResyncCh() <- ResyncEvent{GVR: testGVR, Resources: nil}
		advanceAndWait(100 * time.Millisecond)
		if len(mock.getDeltas()) != 0 {
			t.Fatalf("empty first resync must not call ApplyDelta, got %d deltas", len(mock.getDeltas()))
		}

		w.ResyncCh() <- ResyncEvent{
			GVR: testGVR,
			Resources: []*unstructured.Unstructured{
				makeResource("uid-1", "deploy-1", "100"),
				makeResource("uid-2", "deploy-2", "101"),
			},
		}
		advanceAndWait(100 * time.Millisecond)
		if len(mock.getDeltas()) != 1 {
			t.Fatalf("seed resync ApplyDelta calls = %d, want 1", len(mock.getDeltas()))
		}

		w.ResyncCh() <- ResyncEvent{GVR: testGVR, Resources: nil}
		advanceAndWait(100 * time.Millisecond)

		deltas := mock.getDeltas()
		if len(deltas) != 2 {
			t.Fatalf("expected ReportedUIDs-diff ApplyDelta after seed resync, got %d deltas", len(deltas))
		}
		diff := deltas[1]
		if len(diff.delta.Upserts) != 0 {
			t.Fatalf("empty LIST omission deletes should have no upserts, got %d", len(diff.delta.Upserts))
		}
		if len(diff.delta.Deletes) != 2 {
			t.Fatalf("empty LIST omission deletes = %d, want 2", len(diff.delta.Deletes))
		}
		want1 := mustObjectName(t, "target-1", "uid-1", testGVR)
		want2 := mustObjectName(t, "target-1", "uid-2", testGVR)
		got := make(map[domain.ResourceName]bool, len(diff.delta.Deletes))
		for _, del := range diff.delta.Deletes {
			if !del.IsDelete {
				t.Fatalf("resync delete must set IsDelete, got %+v", del)
			}
			got[del.Name] = true
		}
		if !got[want1] || !got[want2] {
			t.Fatalf("expected deletes for prior UIDs, got %+v", diff.delta.Deletes)
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
			for _, del := range d.delta.Deletes {
				if del.Name == wantDel && del.IsDelete {
					sawDel = true
				}
			}
		}
		if !sawAdd || !sawDel {
			t.Fatalf("expected same-batch upsert+IsDelete, sawAdd=%v sawDel=%v", sawAdd, sawDel)
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
		w := NewWriter("clusters/target-1", nil, nil, testSchema, 100*time.Millisecond, discardLogger)
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

func TestInformerReconcile_RemoveGVR_DoesNotPersistDeletes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// End-to-end: InformerManager reconcile drop → RemoveGVREvent → writer
		// clears in-memory state only (no persisted deletes).
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"), GVR: testGVR}
		advanceAndWait(250 * time.Millisecond)
		deltaCountBefore := len(mock.getDeltas())
		if deltaCountBefore == 0 {
			t.Fatal("precondition: expected initial flush before reconcile")
		}

		disc := newFakeDiscovery(nil)
		mgr := NewInformerManager(nil, disc, w.EventCh(), w.ResyncCh(), w.RemoveCh(), nil, discardLogger)
		mgr.stoppers[testGVR] = func() {}
		mgr.stoppers[schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}] = func() {}

		mgr.Reconcile(ctx, []schema.GroupVersionResource{podsGVR()})
		advanceAndWait(100 * time.Millisecond)

		for _, d := range mock.getDeltas()[deltaCountBefore:] {
			if len(d.delta.Deletes) > 0 {
				t.Fatalf("RemoveGVR via reconcile must not persist deletes, got %+v", d.delta.Deletes)
			}
		}
		if _, ok := w.currentNodes["uid-1"]; ok {
			t.Fatal("RemoveGVR should clear in-memory nodes for dropped GVR")
		}
	})
}

func TestNewWriter_NilEdgeSinkAndLoggerDefaults(t *testing.T) {
	w := NewWriter("clusters/target-1", &recordingReporter{}, nil, testSchema, time.Second, nil)
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

func (s *failingEdgeSink) ApplyEdgeDelta(context.Context, domain.ResourceName, EdgeDelta) error {
	return s.err
}

// toggleEdgeSink fails while fail is true, then records deltas when healthy.
type toggleEdgeSink struct {
	mu     sync.Mutex
	fail   bool
	deltas []EdgeDelta
}

func (s *toggleEdgeSink) setFail(v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fail = v
}

func (s *toggleEdgeSink) ApplyEdgeDelta(_ context.Context, _ domain.ResourceName, delta EdgeDelta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return context.DeadlineExceeded
	}
	s.deltas = append(s.deltas, delta)
	return nil
}

func (s *toggleEdgeSink) getDeltas() []EdgeDelta {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]EdgeDelta, len(s.deltas))
	copy(out, s.deltas)
	return out
}

// TestResync_EdgeSinkFailureAcksAndRetriesOnTicker verifies inventory
// resync ACK is not blocked by a failing edge sink, and that edgeDirty
// causes a ticker retry once the sink recovers — even with an empty
// object batch.
func TestResync_EdgeSinkFailureAcksAndRetriesOnTicker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		sink := &toggleEdgeSink{fail: true}
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

		ack := make(chan error, 1)
		w.ResyncCh() <- ResyncEvent{
			GVR:       testGVR,
			Resources: []*unstructured.Unstructured{child, parent},
			Ack:       ack,
		}
		synctest.Wait()

		select {
		case err := <-ack:
			if err != nil {
				t.Fatalf("resync ACK = %v, want nil despite edge sink failure", err)
			}
		default:
			t.Fatal("resync must ACK after inventory commit without waiting on edges")
		}
		if !w.edgeDirty {
			t.Fatal("expected edgeDirty after failed post-resync flushEdges")
		}
		if len(w.previousEdges) != 0 {
			t.Fatalf("previousEdges must not advance on edge failure, got %d", len(w.previousEdges))
		}
		if len(mock.getDeltas()) == 0 {
			t.Fatal("expected inventory ApplyDelta from resync")
		}
		if len(sink.getDeltas()) != 0 {
			t.Fatal("edge sink must not record deltas while failing")
		}

		sink.setFail(false)
		advanceAndWait(150 * time.Millisecond)

		if w.edgeDirty {
			t.Fatal("edgeDirty must clear after successful ticker retry")
		}
		var found bool
		for _, d := range sink.getDeltas() {
			for _, e := range d.Adds {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("ticker retry must deliver OwnedBy edge after sink recovers")
		}
		if len(w.previousEdges) == 0 {
			t.Fatal("previousEdges must advance after successful edge retry")
		}
	})
}

// TestRemoveGVR_EdgeSinkFailureRetriesOnTicker verifies RemoveGVR still
// drops in-memory membership when edges fail, and edgeDirty retries the
// resulting edge deletes on the next tick.
func TestRemoveGVR_EdgeSinkFailureRetriesOnTicker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		sink := &toggleEdgeSink{}
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
		if len(sink.getDeltas()) == 0 {
			t.Fatal("precondition: expected OwnedBy edge after add flush")
		}
		if len(w.previousEdges) == 0 {
			t.Fatal("precondition: previousEdges must be set")
		}

		sink.setFail(true)
		w.RemoveCh() <- RemoveGVREvent{GVR: testGVR}
		synctest.Wait()
		if !w.edgeDirty {
			t.Fatal("expected edgeDirty after failed post-RemoveGVR flushEdges")
		}
		if _, ok := w.currentNodes["uid-child"]; ok {
			t.Fatal("RemoveGVR must drop in-memory nodes even when edge flush fails")
		}

		sink.setFail(false)
		advanceAndWait(150 * time.Millisecond)
		if w.edgeDirty {
			t.Fatal("edgeDirty must clear after successful retry")
		}
		var sawDelete bool
		for _, d := range sink.getDeltas() {
			for _, e := range d.Deletes {
				if e.EdgeType == EdgeOwnedBy && e.SourceUID == "uid-child" && e.DestUID == "uid-parent" {
					sawDelete = true
				}
			}
		}
		if !sawDelete {
			t.Fatal("ticker retry must deliver OwnedBy edge delete after RemoveGVR")
		}
	})
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

		deltas := mock.getDeltas()
		if len(deltas) != 1 {
			t.Fatalf("ApplyDelta calls = %d, want 1", len(deltas))
		}
		if len(deltas[0].delta.Upserts) != 1 {
			t.Fatalf("upserts = %d, want 1 (bad UID skipped)", len(deltas[0].delta.Upserts))
		}
		if deltas[0].delta.Upserts[0].Labels["k8s.uid"] != "uid-ok" {
			t.Fatalf("expected uid-ok report, got %+v", deltas[0].delta.Upserts[0].Labels)
		}
	})
}

// TestResync_ListedExtractFailureDoesNotDelete verifies a previously
// reported UID that is still present in the LIST but fails extraction
// is not treated as omitted: no IsDelete, ReportedUIDs and currentNodes
// keep the prior entry.
func TestResync_ListedExtractFailureDoesNotDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		var failKeep atomic.Bool
		w.extractObserved = func(r *unstructured.Unstructured, entry SchemaEntry, clusterResourceName domain.ResourceName) (InventoryObjectReport, inventoryNode, error) {
			if failKeep.Load() && string(r.GetUID()) == "uid-keep" {
				return InventoryObjectReport{}, inventoryNode{}, errors.New("forced extract failure")
			}
			return ExtractObservedResource(r, entry, clusterResourceName)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-keep", "deploy-keep", "100"), GVR: testGVR,
		}
		advanceAndWait(250 * time.Millisecond)
		if len(mock.getDeltas()) == 0 {
			t.Fatal("precondition: expected initial upsert")
		}
		if _, ok := w.currentNodes["uid-keep"]; !ok {
			t.Fatal("precondition: uid-keep must be in currentNodes")
		}
		mock.mu.Lock()
		mock.deltas = nil
		mock.mu.Unlock()

		failKeep.Store(true)
		w.ResyncCh() <- ResyncEvent{
			GVR: testGVR,
			Resources: []*unstructured.Unstructured{
				makeResource("uid-keep", "deploy-keep", "200"),
				makeResource("uid-ok", "deploy-ok", "201"),
			},
		}
		advanceAndWait(100 * time.Millisecond)

		for _, d := range mock.getDeltas() {
			for _, del := range d.delta.Deletes {
				if strings.Contains(string(del.Name), "uid-keep") {
					t.Fatalf("listed extract failure must not emit IsDelete for uid-keep, got %+v", del)
				}
			}
		}
		var sawOK bool
		for _, d := range mock.getDeltas() {
			for _, u := range d.delta.Upserts {
				if u.Labels["k8s.uid"] == "uid-ok" {
					sawOK = true
				}
				if u.Labels["k8s.uid"] == "uid-keep" {
					t.Fatal("failed extract must not upsert uid-keep")
				}
			}
		}
		if !sawOK {
			t.Fatal("sibling uid-ok must still upsert")
		}
		st := w.gvrStates[testGVR]
		if st == nil {
			t.Fatal("expected gvr state")
		}
		if _, ok := st.ReportedUIDs["uid-keep"]; !ok {
			t.Fatal("ReportedUIDs must preserve uid-keep after listed extract failure")
		}
		if _, ok := w.currentNodes["uid-keep"]; !ok {
			t.Fatal("currentNodes must preserve uid-keep after listed extract failure")
		}
	})
}

func TestRemoveGVR_NilReporterIsNoop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Drive RemoveGVR through Run, then join before reading currentNodes.
		w := NewWriter("clusters/target-1", nil, nil, testSchema, 50*time.Millisecond, discardLogger)
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

func TestWriter_RejectsClosedGenerationEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-1", "deploy-1", "100"),
			GVR: testGVR, Generation: 1,
		}
		advanceAndWait(250 * time.Millisecond)
		if len(mock.getDeltas()) != 1 {
			t.Fatalf("gen-1 upsert ApplyDelta calls = %d, want 1", len(mock.getDeltas()))
		}

		w.RemoveCh() <- RemoveGVREvent{GVR: testGVR, Generation: 1}
		advanceAndWait(50 * time.Millisecond)

		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-late", "deploy-late", "200"),
			GVR: testGVR, Generation: 1,
		}
		advanceAndWait(250 * time.Millisecond)
		if got := len(mock.getDeltas()); got != 1 {
			t.Fatalf("closed generation must not flush, ApplyDelta calls = %d", got)
		}
	})
}

func TestResync_UsesReportedUIDsNotUnackedNodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// ReportedUIDs advances only after a successful write. A pending
		// watch upsert that has not been acknowledged must not appear in
		// the baseline a later resync would delete against.
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, time.Hour)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.ResyncCh() <- ResyncEvent{
			GVR:       testGVR,
			Resources: []*unstructured.Unstructured{makeResource("uid-ack", "deploy-ack", "100")},
		}
		advanceAndWait(100 * time.Millisecond)

		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-unacked", "deploy-unacked", "200"), GVR: testGVR,
		}
		advanceAndWait(50 * time.Millisecond)

		st := w.gvrStates[testGVR]
		if st == nil {
			t.Fatal("expected gvrState after seed resync")
		}
		if _, ok := st.ReportedUIDs["uid-ack"]; !ok {
			t.Fatal("ReportedUIDs missing acknowledged uid-ack")
		}
		if _, ok := st.ReportedUIDs["uid-unacked"]; ok {
			t.Fatal("unflushed upsert must not add uid-unacked to ReportedUIDs")
		}

		// Empty resync must delete only acknowledged absences (uid-ack),
		// never the still-pending unacked watch upsert.
		w.ResyncCh() <- ResyncEvent{GVR: testGVR, Resources: nil}
		advanceAndWait(100 * time.Millisecond)

		deltas := mock.getDeltas()
		if len(deltas) < 2 {
			t.Fatalf("ApplyDelta calls = %d, want >= 2 (seed + ReportedUIDs diff)", len(deltas))
		}
		diff := deltas[len(deltas)-1].delta
		if len(diff.Deletes) != 1 {
			t.Fatalf("ReportedUIDs-diff deletes = %d, want 1 (uid-ack only)", len(diff.Deletes))
		}
		wantAck := mustObjectName(t, "target-1", "uid-ack", testGVR)
		if diff.Deletes[0].Name != wantAck || !diff.Deletes[0].IsDelete {
			t.Fatalf("ReportedUIDs-diff deleted %+v, want IsDelete for %v", diff.Deletes[0], wantAck)
		}
		wantUnacked := mustObjectName(t, "target-1", "uid-unacked", testGVR)
		for _, d := range diff.Deletes {
			if d.Name == wantUnacked {
				t.Fatal("unacked uid must not be deleted by ReportedUIDs resync")
			}
		}

		// Drop pending work so shutdown flush cannot acknowledge it.
		w.RemoveCh() <- RemoveGVREvent{GVR: testGVR}
		advanceAndWait(50 * time.Millisecond)
	})
}

func TestResync_RetainsFailedWriteAndRetriesWithoutSecondResync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		attempts := 0
		mock := &recordingReporter{
			applyDeltaFunc: func(_ context.Context, _ InventoryDeltaReport) error {
				mu.Lock()
				defer mu.Unlock()
				attempts++
				// Fail the initial applyDeltaWithRetry window (3 attempts),
				// then succeed on the retained ticker retry.
				if attempts <= 3 {
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

		ack := make(chan error, 1)
		w.ResyncCh() <- ResyncEvent{
			GVR:       testGVR,
			Resources: []*unstructured.Unstructured{makeResource("uid-r1", "deploy-r1", "300")},
			Ack:       ack,
		}
		// Exhaust sendResync's bounded retries (1s+2s+4s), then let the
		// batch ticker retry the retained pending resync.
		advanceAndWait(10 * time.Second)

		select {
		case err := <-ack:
			if err != nil {
				t.Fatalf("retained retry ack = %v, want nil", err)
			}
		default:
			t.Fatal("retained resync must ack waiter after successful retry")
		}

		mu.Lock()
		gotAttempts := attempts
		mu.Unlock()
		if gotAttempts < 4 {
			t.Fatalf("attempts = %d, want >= 4 (3 immediate + retained retry)", gotAttempts)
		}
		if len(mock.getDeltas()) == 0 {
			t.Fatal("retained resync must eventually succeed without a second ResyncEvent")
		}
		st := w.gvrStates[testGVR]
		if st == nil {
			t.Fatal("expected gvrState after successful retained resync")
		}
		if _, ok := st.ReportedUIDs["uid-r1"]; !ok {
			t.Fatal("ReportedUIDs must include uid-r1 only after successful write")
		}
	})
}

func TestResync_RemoveGVRNacksWaitingAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{
			applyDeltaFunc: func(context.Context, InventoryDeltaReport) error {
				return context.DeadlineExceeded
			},
		}
		w := newTestWriter(mock, nil, nil, time.Hour)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		ack := make(chan error, 1)
		w.ResyncCh() <- ResyncEvent{
			GVR:        testGVR,
			Resources:  []*unstructured.Unstructured{makeResource("uid-1", "deploy-1", "100")},
			Generation: 1,
			Ack:        ack,
		}
		// Exhaust bounded retries so the batch is retained pending.
		advanceAndWait(8 * time.Second)

		select {
		case <-ack:
			t.Fatal("ack must not fire while write is still failing/retained")
		default:
		}

		w.RemoveCh() <- RemoveGVREvent{GVR: testGVR, Generation: 1}
		advanceAndWait(50 * time.Millisecond)

		select {
		case err := <-ack:
			if !errors.Is(err, errResyncGenerationClosed) {
				t.Fatalf("ack err = %v, want %v", err, errResyncGenerationClosed)
			}
		default:
			t.Fatal("RemoveGVR must nack waiting ResyncEvent.Ack")
		}
		if _, ok := w.pendingResync[testGVR]; ok {
			t.Fatal("pendingResync must be cleared on generation close")
		}
	})
}

func TestResync_NewerListNacksPendingAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		attempts := 0
		mock := &recordingReporter{
			applyDeltaFunc: func(context.Context, InventoryDeltaReport) error {
				mu.Lock()
				defer mu.Unlock()
				attempts++
				// First LIST exhausts retries and retains; second LIST succeeds.
				if attempts <= 3 {
					return context.DeadlineExceeded
				}
				return nil
			},
		}
		w := newTestWriter(mock, nil, nil, time.Hour)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		ack1 := make(chan error, 1)
		w.ResyncCh() <- ResyncEvent{
			GVR:        testGVR,
			Resources:  []*unstructured.Unstructured{makeResource("uid-old", "deploy-old", "100")},
			Generation: 1,
			Ack:        ack1,
		}
		advanceAndWait(8 * time.Second)

		ack2 := make(chan error, 1)
		w.ResyncCh() <- ResyncEvent{
			GVR:        testGVR,
			Resources:  []*unstructured.Unstructured{makeResource("uid-new", "deploy-new", "200")},
			Generation: 1,
			Ack:        ack2,
		}
		advanceAndWait(100 * time.Millisecond)

		select {
		case err := <-ack1:
			if !errors.Is(err, errResyncGenerationClosed) {
				t.Fatalf("replaced pending ack = %v, want generation closed", err)
			}
		default:
			t.Fatal("newer LIST must nack the previous pending Ack")
		}
		select {
		case err := <-ack2:
			if err != nil {
				t.Fatalf("replacement LIST ack = %v, want nil", err)
			}
		default:
			t.Fatal("successful replacement LIST must ack its waiter")
		}
	})
}

func TestWriter_FastReAddAdoptsNewerGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 100*time.Millisecond)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-old", "deploy-old", "100"),
			GVR: testGVR, Generation: 1,
		}
		advanceAndWait(250 * time.Millisecond)
		if st := w.gvrStates[testGVR]; st == nil || st.Generation != 1 {
			t.Fatalf("expected gen 1 state, got %+v", st)
		}

		// Fast re-add without RemoveGVR: newer generation must replace baseline.
		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-new", "deploy-new", "200"),
			GVR: testGVR, Generation: 2,
		}
		advanceAndWait(250 * time.Millisecond)

		st := w.gvrStates[testGVR]
		if st == nil || st.Generation != 2 {
			t.Fatalf("expected gen 2 state after fast re-add, got %+v", st)
		}
		if _, ok := st.ReportedUIDs["uid-new"]; !ok {
			t.Fatal("gen-2 upsert must be acknowledged in ReportedUIDs")
		}
		if _, ok := st.ReportedUIDs["uid-old"]; ok {
			t.Fatal("fast re-add must drop prior generation ReportedUIDs")
		}

		// Late gen-1 event must be rejected after gen-2 is open.
		before := len(mock.getDeltas())
		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-late", "deploy-late", "300"),
			GVR: testGVR, Generation: 1,
		}
		advanceAndWait(250 * time.Millisecond)
		if got := len(mock.getDeltas()); got != before {
			t.Fatalf("stale gen-1 event flushed: deltas %d -> %d", before, got)
		}
	})
}

// TestWriter_FastReAddDropsPendingUpsertsFromPriorGeneration verifies that
// unflushed gen-1 upserts are discarded when a gen-2 event adopts the GVR,
// so a later flush cannot resurrect prior-generation inventory.
func TestWriter_FastReAddDropsPendingUpsertsFromPriorGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		// Long interval so gen-1 stays pending until after gen-2 adopts.
		w := newTestWriter(mock, nil, nil, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-old", "deploy-old", "100"),
			GVR: testGVR, Generation: 1,
		}
		synctest.Wait()
		if len(mock.getDeltas()) != 0 {
			t.Fatal("precondition: gen-1 upsert must still be pending")
		}

		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-new", "deploy-new", "200"),
			GVR: testGVR, Generation: 2,
		}
		advanceAndWait(11 * time.Second)

		var sawOld, sawNew bool
		for _, d := range mock.getDeltas() {
			for _, u := range d.delta.Upserts {
				switch u.Labels["k8s.uid"] {
				case "uid-old":
					sawOld = true
				case "uid-new":
					sawNew = true
				}
			}
		}
		if sawOld {
			t.Fatal("gen-1 pending upsert must not flush after gen-2 adopt")
		}
		if !sawNew {
			t.Fatal("gen-2 upsert must flush after adopt")
		}
		if st := w.gvrStates[testGVR]; st == nil || st.Generation != 2 {
			t.Fatalf("expected gen 2 state, got %+v", st)
		}
		if _, ok := w.gvrStates[testGVR].ReportedUIDs["uid-old"]; ok {
			t.Fatal("gen-1 UID must not appear in gen-2 ReportedUIDs")
		}
	})
}

// TestWriter_FastReAddDropsPendingDeletesFromPriorGeneration verifies that
// unflushed gen-1 deletes are discarded on gen-2 adopt, so a later flush
// cannot delete inventory that still belongs to the newer generation.
func TestWriter_FastReAddDropsPendingDeletesFromPriorGeneration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, nil, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		// Establish gen-1 baseline with a flushed upsert.
		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-keep", "deploy-keep", "100"),
			GVR: testGVR, Generation: 1,
		}
		advanceAndWait(11 * time.Second)
		if len(mock.getDeltas()) == 0 {
			t.Fatal("precondition: expected gen-1 upsert flush")
		}
		mock.mu.Lock()
		mock.deltas = nil
		mock.mu.Unlock()

		// Gen-1 delete stays pending under the long batch interval.
		w.EventCh() <- ResourceEvent{
			Op: EventDelete, Resource: makeResource("uid-keep", "deploy-keep", "100"),
			GVR: testGVR, Generation: 1,
		}
		synctest.Wait()

		// Gen-2 re-add of the same UID adopts and must drop the pending delete.
		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-keep", "deploy-keep", "200"),
			GVR: testGVR, Generation: 2,
		}
		advanceAndWait(11 * time.Second)

		for _, d := range mock.getDeltas() {
			for _, del := range d.delta.Deletes {
				if del.IsDelete {
					t.Fatalf("gen-1 pending delete must not flush after gen-2 adopt, got %+v", del)
				}
			}
			for _, u := range d.delta.Upserts {
				if u.Labels["k8s.uid"] == "uid-keep" && u.IsDelete {
					t.Fatal("upsert side must not carry IsDelete for uid-keep")
				}
			}
		}
		var sawUpsert bool
		for _, d := range mock.getDeltas() {
			for _, u := range d.delta.Upserts {
				if u.Labels["k8s.uid"] == "uid-keep" {
					sawUpsert = true
				}
			}
		}
		if !sawUpsert {
			t.Fatal("gen-2 upsert for uid-keep must flush after adopt")
		}
	})
}

// TestWriter_FastReAddDropsPendingOnlyForAdoptedGVR verifies other GVRs'
// pending work survives when one GVR adopts a newer generation.
func TestWriter_FastReAddDropsPendingOnlyForAdoptedGVR(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rsGVR := schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "replicasets"}
		schemaBoth := map[schema.GroupVersionResource]SchemaEntry{
			testGVR: {GVR: testGVR, Kind: "Deployment"},
			rsGVR:   {GVR: rsGVR, Kind: "ReplicaSet"},
		}
		mock := &recordingReporter{}
		w := newTestWriter(mock, nil, schemaBoth, 10*time.Second)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(ctx)
		synctest.Wait()

		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-deploy", "deploy-1", "100"),
			GVR: testGVR, Generation: 1,
		}
		rs := &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "apps/v1", "kind": "ReplicaSet",
			"metadata": map[string]any{
				"uid": "uid-rs", "name": "rs-1", "namespace": "default",
				"resourceVersion": "200", "creationTimestamp": "2025-06-01T12:00:00Z",
			},
		}}
		w.EventCh() <- ResourceEvent{Op: EventAdd, Resource: rs, GVR: rsGVR, Generation: 1}
		synctest.Wait()

		w.EventCh() <- ResourceEvent{
			Op: EventAdd, Resource: makeResource("uid-deploy-2", "deploy-2", "300"),
			GVR: testGVR, Generation: 2,
		}
		advanceAndWait(11 * time.Second)

		var sawDeploy1, sawDeploy2, sawRS bool
		for _, d := range mock.getDeltas() {
			for _, u := range d.delta.Upserts {
				switch u.Labels["k8s.uid"] {
				case "uid-deploy":
					sawDeploy1 = true
				case "uid-deploy-2":
					sawDeploy2 = true
				case "uid-rs":
					sawRS = true
				}
			}
		}
		if sawDeploy1 {
			t.Fatal("adopted GVR must drop prior-generation pending upsert")
		}
		if !sawDeploy2 {
			t.Fatal("new generation upsert must flush")
		}
		if !sawRS {
			t.Fatal("unrelated GVR pending upsert must survive adopt")
		}
	})
}

func TestFlush_FailureRestoresUncommittedNodes(t *testing.T) {
	mock := &recordingReporter{
		applyDeltaFunc: func(context.Context, InventoryDeltaReport) error {
			return context.DeadlineExceeded
		},
	}
	w := newTestWriter(mock, nil, nil, time.Hour)

	upserts := map[string]*unstructured.Unstructured{
		"uid-1": makeResource("uid-1", "deploy-1", "100"),
	}
	upsertGVRs := map[string]schema.GroupVersionResource{"uid-1": testGVR}
	deletes := map[string]schema.GroupVersionResource{}
	sent := map[string]string{}

	if err := w.flush(context.Background(), upserts, upsertGVRs, deletes, sent); err == nil {
		t.Fatal("expected flush failure")
	}
	if _, ok := w.currentNodes["uid-1"]; ok {
		t.Fatal("flush failure must restore/clear uncommitted currentNodes entry")
	}
	if _, ok := w.edgeFuncs["uid-1"]; ok {
		t.Fatal("flush failure must restore/clear uncommitted edgeFuncs entry")
	}
	if st := w.gvrStates[testGVR]; st != nil {
		if _, ok := st.ReportedUIDs["uid-1"]; ok {
			t.Fatal("ReportedUIDs must not advance after flush failure")
		}
	}
	if len(sent) != 0 {
		t.Fatalf("sentVersions must not advance after flush failure, got %#v", sent)
	}
}

func TestGenericInformer_Run_WatchBlockedUntilWriterRetainedRetryAcks(t *testing.T) {
	// End-to-end Option A: failed LIST write retains; watch stays down until
	// the writer's ticker retry succeeds and acks.
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		dyn := newFakeDynamicClient(gvr)
		pod := makePod("uid-1", "pod-1", "default", "1")
		list := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{*pod}}
		list.Object = map[string]any{"metadata": map[string]any{"resourceVersion": "1"}}
		dyn.PrependReactor("list", "pods", func(k8stesting.Action) (handled bool, ret runtime.Object, err error) {
			return true, list, nil
		})

		watchStarted := make(chan struct{})
		dyn.PrependWatchReactor("pods", func(k8stesting.Action) (bool, watch.Interface, error) {
			select {
			case <-watchStarted:
			default:
				close(watchStarted)
			}
			return true, watch.NewFake(), nil
		})

		var mu sync.Mutex
		attempts := 0
		reporter := &recordingReporter{
			applyDeltaFunc: func(context.Context, InventoryDeltaReport) error {
				mu.Lock()
				defer mu.Unlock()
				attempts++
				if attempts <= 3 {
					return context.DeadlineExceeded
				}
				return nil
			},
		}
		schema := map[schema.GroupVersionResource]SchemaEntry{
			gvr: {GVR: gvr, Kind: "Pod"},
		}
		w := NewWriter("clusters/target-1", reporter, NoopEdgeSink{}, schema, 100*time.Millisecond, discardLogger)

		inf := NewInformerGeneration(dyn, gvr, 1, w.EventCh(), w.ResyncCh(), nil, slog.Default())

		runCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go w.Run(runCtx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			inf.Run(runCtx)
		}()
		synctest.Wait()

		select {
		case <-watchStarted:
			t.Fatal("watch must not start while LIST write is still failing")
		default:
		}

		// Exhaust immediate retries, then ticker retained retry succeeds and acks.
		time.Sleep(10 * time.Second)
		synctest.Wait()

		select {
		case <-watchStarted:
		default:
			t.Fatal("watch should start after retained LIST write succeeds")
		}
		cancel()
		synctest.Wait()
		<-done
	})
}
