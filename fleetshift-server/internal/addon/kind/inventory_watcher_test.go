package kind

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type recordingInventoryReporter struct {
	mu      sync.Mutex
	batches []domain.InventoryDeltaBatch
	err     error  // returned from ApplyDeltaBatch when set
	before  func() // optional hook invoked before applying (outside reporter lock)
}

func (r *recordingInventoryReporter) ApplyDeltaBatch(_ context.Context, batch domain.InventoryDeltaBatch) error {
	r.mu.Lock()
	before := r.before
	err := r.err
	r.mu.Unlock()

	if before != nil {
		before()
	}
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	cp := domain.InventoryDeltaBatch{Reports: append([]domain.InventoryDeltaReport(nil), batch.Reports...)}
	r.batches = append(r.batches, cp)
	return nil
}

func TestInventoryWatcher_CoalescesPendingIntoOneBatch(t *testing.T) {
	fixed := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	reporter := &recordingInventoryReporter{}
	w := NewInventoryWatcher(reporter,
		WithInventoryDebounce(0),
		WithInventoryClock(func() time.Time { return fixed }),
	)

	clusterName := domain.ResourceName("clusters/demo")
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "demo-control-plane",
			Labels: map[string]string{"kubernetes.io/hostname": "demo-control-plane"},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{
				Type:               corev1.NodeReady,
				Status:             corev1.ConditionTrue,
				Reason:             "KubeletReady",
				Message:            "ok",
				LastTransitionTime: metav1.NewTime(fixed),
			}},
			Addresses: []corev1.NodeAddress{{Type: corev1.NodeInternalIP, Address: "10.0.0.1"}},
			NodeInfo:  corev1.NodeSystemInfo{KubeletVersion: "v1.31.0"},
		},
	}

	w.enqueueNode(clusterName, node)
	w.enqueueCluster(clusterName, true)
	// Supersede the node delta with updated labels before flush.
	node.Labels["extra"] = "1"
	w.enqueueNode(clusterName, node)

	if err := w.flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.batches) != 1 {
		t.Fatalf("batches = %d, want 1 coalesced ApplyDeltaBatch", len(reporter.batches))
	}
	reports := reporter.batches[0].Reports
	if len(reports) != 2 {
		t.Fatalf("reports in batch = %d, want 2 (node + cluster)", len(reports))
	}

	byName := map[domain.ResourceName]domain.InventoryDeltaReport{}
	for _, r := range reports {
		byName[r.Name] = r
	}
	nodeReport, ok := byName[domain.ResourceName("nodes/demo-control-plane")]
	if !ok {
		t.Fatal("missing node report")
	}
	if nodeReport.ReplaceLabels["extra"] != "1" {
		t.Errorf("superseded ReplaceLabels = %+v, want extra=1", nodeReport.ReplaceLabels)
	}
	if nodeReport.ReplaceLabels["kubernetes.io/hostname"] != "demo-control-plane" {
		t.Errorf("ReplaceLabels missing hostname: %+v", nodeReport.ReplaceLabels)
	}
	if nodeReport.Observation == nil {
		t.Fatal("node Observation is nil")
	}
	var obs map[string]any
	if err := json.Unmarshal(*nodeReport.Observation, &obs); err != nil {
		t.Fatalf("unmarshal observation: %v", err)
	}
	if obs["cluster"] != "clusters/demo" {
		t.Errorf("observation.cluster = %v, want clusters/demo", obs["cluster"])
	}
	if _, ok := byName[clusterName]; !ok {
		t.Fatal("missing cluster report")
	}
}

func TestInventoryWatcher_FlushRequeuesOnBatchFailure(t *testing.T) {
	fixed := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	clusterName := domain.ResourceName("clusters/demo")
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "n1",
			Labels: map[string]string{"v": "1"},
		},
	}

	var w *InventoryWatcher
	reporter := &recordingInventoryReporter{
		err: errors.New("batch failed"),
		before: func() {
			// Newer delta arrives while ApplyDeltaBatch is in flight.
			node.Labels["v"] = "2"
			w.enqueueNode(clusterName, node)
		},
	}
	w = NewInventoryWatcher(reporter,
		WithInventoryDebounce(0),
		WithInventoryClock(func() time.Time { return fixed }),
	)

	w.enqueueNode(clusterName, node)
	w.enqueueCluster(clusterName, true)

	if err := w.flush(context.Background()); err == nil {
		t.Fatal("flush: want error from ApplyDeltaBatch")
	}

	reporter.mu.Lock()
	reporter.err = nil
	reporter.before = nil
	reporter.mu.Unlock()

	if err := w.flush(context.Background()); err != nil {
		t.Fatalf("flush after recovery: %v", err)
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	if len(reporter.batches) != 1 {
		t.Fatalf("batches = %d, want 1 after successful retry", len(reporter.batches))
	}
	byName := map[domain.ResourceName]domain.InventoryDeltaReport{}
	for _, r := range reporter.batches[0].Reports {
		byName[r.Name] = r
	}
	if len(byName) != 2 {
		t.Fatalf("reports = %d, want 2 (requeued cluster + newer node)", len(byName))
	}
	nodeReport, ok := byName[domain.ResourceName("nodes/n1")]
	if !ok {
		t.Fatal("missing node report after requeue")
	}
	if nodeReport.ReplaceLabels["v"] != "2" {
		t.Errorf("ReplaceLabels[v] = %q, want 2 (newer pending must win)", nodeReport.ReplaceLabels["v"])
	}
	if _, ok := byName[clusterName]; !ok {
		t.Fatal("missing cluster report after requeue")
	}
}

func TestInventoryWatcher_DeleteDropsPending(t *testing.T) {
	fixed := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	reporter := &recordingInventoryReporter{}
	w := NewInventoryWatcher(reporter,
		WithInventoryDebounce(0),
		WithInventoryClock(func() time.Time { return fixed }),
	)

	clusterName := domain.ResourceName("clusters/c1")
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n1", Labels: map[string]string{"a": "1"}},
	}
	w.enqueueNode(clusterName, node)
	w.dropPending(nodeResourceName("n1"))
	if err := w.flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	reporter.mu.Lock()
	defer reporter.mu.Unlock()
	for _, b := range reporter.batches {
		for _, r := range b.Reports {
			if r.Name == domain.ResourceName("nodes/n1") {
				t.Fatalf("node report should have been dropped, got %+v", r)
			}
		}
	}
}

func TestMapNodeDelta_MirrorsKubeLabelsAndConditions(t *testing.T) {
	fixed := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "worker",
			Labels: map[string]string{"node-role.kubernetes.io/worker": ""},
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{
				Type:               corev1.NodeReady,
				Status:             corev1.ConditionTrue,
				Reason:             "Ready",
				Message:            "kubelet is posting ready",
				LastTransitionTime: metav1.NewTime(fixed),
			}},
		},
	}
	got, err := mapNodeDelta(domain.ResourceName("clusters/c1"), node, fixed)
	if err != nil {
		t.Fatalf("mapNodeDelta: %v", err)
	}
	if got.ResourceType != NodeResourceType {
		t.Errorf("ResourceType = %q, want %q", got.ResourceType, NodeResourceType)
	}
	if got.Name != domain.ResourceName("nodes/worker") {
		t.Errorf("Name = %q, want nodes/worker", got.Name)
	}
	if _, ok := got.ReplaceLabels["node-role.kubernetes.io/worker"]; !ok {
		t.Errorf("ReplaceLabels = %+v, want worker role key", got.ReplaceLabels)
	}
	if len(got.ReplaceConditions) != 1 || got.ReplaceConditions[0].Type() != "Ready" {
		t.Errorf("ReplaceConditions = %+v, want Ready", got.ReplaceConditions)
	}
}

func TestMapClusterDelta_SetsReadyConditions(t *testing.T) {
	fixed := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	got, err := mapClusterDelta(domain.ResourceName("clusters/c1"), true, fixed)
	if err != nil {
		t.Fatalf("mapClusterDelta: %v", err)
	}
	if got.ResourceType != ClusterResourceType {
		t.Errorf("ResourceType = %q", got.ResourceType)
	}
	if len(got.ReplaceConditions) != 2 {
		t.Fatalf("ReplaceConditions len = %d, want 2", len(got.ReplaceConditions))
	}
	for _, c := range got.ReplaceConditions {
		if c.Status() != domain.ConditionTrue {
			t.Errorf("condition %s status = %s, want True", c.Type(), c.Status())
		}
	}
}
