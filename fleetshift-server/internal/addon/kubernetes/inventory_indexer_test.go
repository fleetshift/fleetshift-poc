package kubernetes

import (
	"context"
	"log/slog"
	"testing"
	"testing/synctest"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestNewIndexerDelegate_Defaults(t *testing.T) {
	ic := newIndexerDelegate(
		"clusters/target-1",
		nil,
		nil,
		&recordingReporter{},
		nil,           // edge sink
		IndexConfig{}, // zero BatchInterval
		discardLogger,
	)
	if ic.cfg.BatchInterval != 5*time.Second {
		t.Fatalf("BatchInterval = %v, want 5s default", ic.cfg.BatchInterval)
	}
	if _, ok := ic.edgeSink.(NoopEdgeSink); !ok {
		t.Fatalf("nil edgeSink should default to NoopEdgeSink, got %T", ic.edgeSink)
	}
	if ic.clusterResourceName != "clusters/target-1" {
		t.Fatalf("clusterResourceName = %q, want clusters/target-1", ic.clusterResourceName)
	}
	if ic.done == nil {
		t.Fatal("done channel must be created")
	}
}

func TestNewIndexerDelegate_PreservesExplicitBatchIntervalAndEdgeSink(t *testing.T) {
	edges := &recordingEdgeSink{}
	ic := newIndexerDelegate(
		"clusters/target-1",
		nil,
		nil,
		&recordingReporter{},
		edges,
		IndexConfig{BatchInterval: 250 * time.Millisecond},
		discardLogger,
	)
	if ic.cfg.BatchInterval != 250*time.Millisecond {
		t.Fatalf("BatchInterval = %v, want 250ms", ic.cfg.BatchInterval)
	}
	if ic.edgeSink != edges {
		t.Fatal("explicit edgeSink must be preserved")
	}
}

func TestIndexerDelegate_Start_ReportsCollectionAndShutdownDoesNotDelete(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gvr := podsGVR()
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(gvr, crdGVR)
		pod := makePod("uid-pod-1", "pod-1", "default", "1")
		if _, err := dyn.Resource(gvr).Namespace("default").Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatalf("create pod: %v", err)
		}

		reporter := &recordingReporter{}
		edges := &recordingEdgeSink{}
		ic := newIndexerDelegate(
			"clusters/target-1",
			dyn,
			disc,
			reporter,
			edges,
			IndexConfig{
				Schema: IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
					gvr: {GVR: gvr, Kind: "Pod"},
				}},
				AllowList:     []Resource{{ApiGroups: []string{""}, Resources: []string{"pods"}}},
				BatchInterval: 100 * time.Millisecond,
			},
			slog.Default(),
		)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			ic.start(ctx)
		}()

		awaitRunContinuousReady()
		synctest.Wait()
		// Allow writer batch flush after LIST/watch events.
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()

		foundPod := false
		for _, d := range reporter.getDeltas() {
			for _, report := range d.delta.Upserts {
				if report.Labels["k8s.uid"] == "uid-pod-1" {
					foundPod = true
				}
			}
		}
		if !foundPod {
			t.Fatal("expected pod uid-pod-1 in ApplyDelta upserts from initial LIST resync")
		}

		cancel()
		synctest.Wait()
		<-done
		<-ic.done

		for _, d := range reporter.getDeltas() {
			if len(d.delta.Deletes) > 0 {
				t.Fatalf("indexer shutdown must not persist object deletes, got %+v", d.delta.Deletes)
			}
		}
	})
}

func TestIndexerDelegate_Start_AllowListExcludesOtherGVRs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pods := podsGVR()
		svcs := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "services"}
		disc := newFakeDiscovery([]*metav1.APIResourceList{{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Verbs: metav1.Verbs{"get", "list", "watch"}},
				{Name: "services", Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		}})
		dyn := newFakeDynamicClient(pods, svcs, crdGVR)
		reporter := &recordingReporter{}
		ic := newIndexerDelegate(
			"clusters/target-1",
			dyn,
			disc,
			reporter,
			NoopEdgeSink{},
			IndexConfig{
				Schema: IndexSchema{Entries: map[schema.GroupVersionResource]SchemaEntry{
					pods: {GVR: pods, Kind: "Pod"},
					svcs: {GVR: svcs, Kind: "Service"},
				}},
				AllowList:     []Resource{{ApiGroups: []string{""}, Resources: []string{"pods"}}},
				BatchInterval: 100 * time.Millisecond,
			},
			slog.Default(),
		)

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			ic.start(ctx)
		}()
		awaitRunContinuousReady()
		synctest.Wait()
		time.Sleep(300 * time.Millisecond)
		synctest.Wait()

		for _, d := range reporter.getDeltas() {
			for _, report := range d.delta.Upserts {
				if report.Labels["k8s.kind"] == "Service" {
					t.Fatal("services GVR must not be indexed when allow-list is pods-only")
				}
			}
		}

		cancel()
		synctest.Wait()
		<-done
	})
}
