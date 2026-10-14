package kind_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// recordingIndexingRuntime records EnsureIndexer and StopIndexer calls for
// Kind agent tests.
type recordingIndexingRuntime struct {
	mu        sync.Mutex
	ensures   []kubernetes.IndexRuntimeInput
	stops     []domain.TargetID
	ensureErr error
	stopErr   error
}

// EnsureIndexer records input and returns ensureErr.
func (r *recordingIndexingRuntime) EnsureIndexer(_ context.Context, input kubernetes.IndexRuntimeInput) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensures = append(r.ensures, input)
	return r.ensureErr
}

// StopIndexer records targetID and returns stopErr.
func (r *recordingIndexingRuntime) StopIndexer(_ context.Context, targetID domain.TargetID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stops = append(r.stops, targetID)
	return r.stopErr
}

// StopAll implements [kubernetes.IndexingRuntime].
func (r *recordingIndexingRuntime) StopAll(context.Context) error { return nil }

// ensureCount returns how many EnsureIndexer calls were recorded.
func (r *recordingIndexingRuntime) ensureCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ensures)
}

// stopIDs returns a copy of recorded StopIndexer target IDs.
func (r *recordingIndexingRuntime) stopIDs() []domain.TargetID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.TargetID, len(r.stops))
	copy(out, r.stops)
	return out
}

// lastEnsure returns the most recent EnsureIndexer input, or zero if none.
func (r *recordingIndexingRuntime) lastEnsure() kubernetes.IndexRuntimeInput {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.ensures) == 0 {
		return kubernetes.IndexRuntimeInput{}
	}
	return r.ensures[len(r.ensures)-1]
}

func TestAgent_Deliver_EnsureIndexerBeforeDelivered(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	runtime := &recordingIndexingRuntime{}
	agent, _ := newTestAgent(reporter, provider, kind.WithIndexingRuntime(runtime))

	err := agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"}),
		"d1:k1",
		[]domain.Manifest{{
			ManifestType: kind.ClusterManifestType,
			Raw:          json.RawMessage(`{"name":"idx-cluster"}`),
		}},
		domain.DeliveryAuth{},
		nil,
		7,
	)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q, want %q; message = %q", result.State, domain.DeliveryStateDelivered, result.Message)
	}
	if runtime.ensureCount() != 1 {
		t.Fatalf("EnsureIndexer calls = %d, want 1", runtime.ensureCount())
	}
	got := runtime.lastEnsure()
	if got.TargetID != "k8s-idx-cluster" {
		t.Fatalf("Ensure TargetID = %q, want k8s-idx-cluster", got.TargetID)
	}
	if got.ClusterResourceName != "clusters/idx-cluster" {
		t.Fatalf("Ensure ClusterResourceName = %q, want clusters/idx-cluster", got.ClusterResourceName)
	}
	if got.Generation != 7 {
		t.Fatalf("Ensure Generation = %d, want 7", got.Generation)
	}
	if string(got.Credential) != "test-sa-token" {
		t.Fatalf("Ensure Credential = %q, want test-sa-token", got.Credential)
	}
	if got.APIServer == "" {
		t.Fatal("Ensure APIServer is empty")
	}
}

func TestAgent_Deliver_EnsureIndexerFailureFailsDelivery(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	runtime := &recordingIndexingRuntime{ensureErr: kubernetes.ErrStaleIndexerGeneration}
	agent, _ := newTestAgent(reporter, provider, kind.WithIndexingRuntime(runtime))

	err := agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"}),
		"d1:k1",
		[]domain.Manifest{{
			ManifestType: kind.ClusterManifestType,
			Raw:          json.RawMessage(`{"name":"fail-idx"}`),
		}},
		domain.DeliveryAuth{},
		nil,
		1,
	)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateFailed {
		t.Fatalf("State = %q, want %q; message = %q", result.State, domain.DeliveryStateFailed, result.Message)
	}
	if runtime.ensureCount() != 1 {
		t.Fatalf("EnsureIndexer calls = %d, want 1 (permanent fail-fast)", runtime.ensureCount())
	}
}

// TestAgent_Deliver_EnsureIndexerRetriesTransientThenSucceeds verifies the
// local EnsureIndexer envelope retries a transient error and still reports
// Delivered once EnsureIndexer succeeds.
func TestAgent_Deliver_EnsureIndexerRetriesTransientThenSucceeds(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	runtime := &flakyEnsureIndexingRuntime{failTimes: 2}
	agent, _ := newTestAgent(reporter, provider, kind.WithIndexingRuntime(runtime))

	err := agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"}),
		"d1:k1",
		[]domain.Manifest{{
			ManifestType: kind.ClusterManifestType,
			Raw:          json.RawMessage(`{"name":"retry-idx"}`),
		}},
		domain.DeliveryAuth{},
		nil,
		4,
	)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q, want %q; message = %q", result.State, domain.DeliveryStateDelivered, result.Message)
	}
	if got := runtime.ensureCount(); got != 3 {
		t.Fatalf("EnsureIndexer calls = %d, want 3 (2 transient + 1 success)", got)
	}
}

// TestAgent_Deliver_ReportResultRetriesTransientThenSucceeds verifies terminal
// ReportResult is retried on transient errors before the delivery completes.
func TestAgent_Deliver_ReportResultRetriesTransientThenSucceeds(t *testing.T) {
	provider := newFakeProvider()
	base := newChannelReporter()
	reporter := &flakyReportResultReporter{channelReporter: base, failTimes: 2}
	runtime := &recordingIndexingRuntime{}
	agent, _ := newTestAgent(reporter, provider, kind.WithIndexingRuntime(runtime))

	err := agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"}),
		"d1:k1",
		[]domain.Manifest{{
			ManifestType: kind.ClusterManifestType,
			Raw:          json.RawMessage(`{"name":"report-retry"}`),
		}},
		domain.DeliveryAuth{},
		nil,
		5,
	)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := awaitDone(t, base.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q, want %q; message = %q", result.State, domain.DeliveryStateDelivered, result.Message)
	}
	if got := reporter.reportResultCalls.Load(); got != 3 {
		t.Fatalf("ReportResult calls = %d, want 3 (2 transient + 1 success)", got)
	}
}

// flakyEnsureIndexingRuntime fails EnsureIndexer failTimes times with a
// transient error, then succeeds.
type flakyEnsureIndexingRuntime struct {
	mu        sync.Mutex
	calls     int
	failTimes int
}

// EnsureIndexer fails until failTimes transient failures have been recorded.
func (r *flakyEnsureIndexingRuntime) EnsureIndexer(_ context.Context, _ kubernetes.IndexRuntimeInput) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if r.calls <= r.failTimes {
		return errors.New("temporary discovery unavailable")
	}
	return nil
}

// StopIndexer implements [kubernetes.IndexingRuntime].
func (r *flakyEnsureIndexingRuntime) StopIndexer(context.Context, domain.TargetID) error {
	return nil
}

// StopAll implements [kubernetes.IndexingRuntime].
func (r *flakyEnsureIndexingRuntime) StopAll(context.Context) error { return nil }

// ensureCount returns how many EnsureIndexer calls were recorded.
func (r *flakyEnsureIndexingRuntime) ensureCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// flakyReportResultReporter fails ReportResult failTimes times, then delegates.
type flakyReportResultReporter struct {
	*channelReporter
	failTimes         int
	reportResultCalls atomic.Int32
}

// ReportResult fails until failTimes transient failures have been recorded.
func (r *flakyReportResultReporter) ReportResult(
	ctx context.Context,
	id domain.DeliveryID,
	gen domain.Generation,
	result domain.DeliveryResult,
) error {
	n := int(r.reportResultCalls.Add(1))
	if n <= r.failTimes {
		return errors.New("temporary report unavailable")
	}
	return r.channelReporter.ReportResult(ctx, id, gen, result)
}

func TestAgent_Remove_StopIndexer(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["fs--stop-me"] = nil
	reporter := newChannelReporter()
	runtime := &recordingIndexingRuntime{stopErr: domain.ErrNotFound} // best-effort: Remove continues
	agent, store := newTestAgent(reporter, provider, kind.WithIndexingRuntime(runtime))
	store.SetForTest("fs--stop-me", 1)

	err := agent.Remove(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		"d1:t1",
		[]domain.Manifest{{
			ManifestType: kind.ClusterManifestType,
			Raw:          json.RawMessage(`{"name":"stop-me"}`),
		}},
		domain.DeliveryAuth{},
		nil,
		1,
	)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q, want %q", result.State, domain.DeliveryStateDelivered)
	}
	stops := runtime.stopIDs()
	if len(stops) != 1 || stops[0] != "k8s-stop-me" {
		t.Fatalf("StopIndexer calls = %v, want [k8s-stop-me]", stops)
	}
	if len(provider.deleted) != 1 || provider.deleted[0] != "fs--stop-me" {
		t.Fatalf("deleted = %v, want [fs--stop-me]", provider.deleted)
	}
}

func TestAgent_Remove_StaleGenerationDoesNotStopIndexer(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["fs--demo"] = nil
	reporter := newChannelReporter()
	runtime := &recordingIndexingRuntime{}
	agent, store := newTestAgent(reporter, provider, kind.WithIndexingRuntime(runtime))
	store.SetForTest("fs--demo", 2)

	err := agent.Remove(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}),
		"d1:t1",
		[]domain.Manifest{{
			ManifestType: kind.ClusterManifestType,
			Raw:          json.RawMessage(`{"name":"demo"}`),
		}},
		domain.DeliveryAuth{},
		nil,
		1, // lower than recorded gen 2
	)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateFailed {
		t.Fatalf("State = %q, want Failed; message = %q", result.State, result.Message)
	}
	if len(runtime.stopIDs()) != 0 {
		t.Fatalf("StopIndexer calls = %v, want none on stale remove", runtime.stopIDs())
	}
	if !provider.hasCluster("fs--demo") {
		t.Fatal("cluster should remain after stale remove")
	}
}

func TestAgent_Deliver_Recreate_StopIndexerBeforeDelete(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["fs--recreate-me"] = nil
	reporter := newChannelReporter()
	runtime := &recordingIndexingRuntime{}
	agent, store := newTestAgent(reporter, provider, kind.WithIndexingRuntime(runtime))
	// Lower stored generation forces recreateOwnedCluster on deliver gen 2.
	store.SetForTest("fs--recreate-me", 1)

	err := agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"}),
		"d1:k1",
		[]domain.Manifest{{
			ManifestType: kind.ClusterManifestType,
			Raw:          json.RawMessage(`{"name":"recreate-me"}`),
		}},
		domain.DeliveryAuth{},
		nil,
		2,
	)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q, want %q; message = %q", result.State, domain.DeliveryStateDelivered, result.Message)
	}
	stops := runtime.stopIDs()
	if len(stops) != 1 || stops[0] != "k8s-recreate-me" {
		t.Fatalf("StopIndexer on recreate = %v, want [k8s-recreate-me]", stops)
	}
	if runtime.ensureCount() != 1 {
		t.Fatalf("EnsureIndexer calls = %d, want 1 after recreate", runtime.ensureCount())
	}
}

// TestAgent_Deliver_PartialEnsureLeavesReadySiblings verifies that when
// EnsureIndexer fails for a later cluster in a multi-cluster delivery, the
// agent reports Failed without outputs and does not call StopIndexer for
// clusters that already became ready.
func TestAgent_Deliver_PartialEnsureLeavesReadySiblings(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	runtime := &selectiveFailIndexingRuntime{
		failTarget: "k8s-cluster-b",
		failErr:    kubernetes.ErrStaleIndexerGeneration,
	}
	agent, _ := newTestAgent(reporter, provider, kind.WithIndexingRuntime(runtime))

	err := agent.Deliver(
		context.Background(),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"}),
		"d1:k1",
		[]domain.Manifest{
			{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"cluster-a"}`)},
			{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"cluster-b"}`)},
		},
		domain.DeliveryAuth{},
		nil,
		3,
	)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateFailed {
		t.Fatalf("State = %q, want %q; message = %q", result.State, domain.DeliveryStateFailed, result.Message)
	}
	if len(result.ProvisionedTargets) != 0 || len(result.ProducedSecrets) != 0 {
		t.Fatalf("Failed delivery must omit outputs; got targets=%v secrets=%v",
			result.ProvisionedTargets, result.ProducedSecrets)
	}
	ensures := runtime.ensureIDs()
	if len(ensures) != 2 || ensures[0] != "k8s-cluster-a" || ensures[1] != "k8s-cluster-b" {
		t.Fatalf("EnsureIndexer order = %v, want [k8s-cluster-a k8s-cluster-b]", ensures)
	}
	if stops := runtime.stopIDs(); len(stops) != 0 {
		t.Fatalf("StopIndexer calls = %v, want none (ready siblings left running)", stops)
	}
}

// selectiveFailIndexingRuntime records EnsureIndexer and StopIndexer calls
// and returns failErr from EnsureIndexer when the target matches failTarget.
type selectiveFailIndexingRuntime struct {
	mu         sync.Mutex
	ensures    []domain.TargetID
	stops      []domain.TargetID
	failTarget domain.TargetID
	failErr    error
}

// EnsureIndexer records the target ID and returns failErr for failTarget.
func (r *selectiveFailIndexingRuntime) EnsureIndexer(_ context.Context, input kubernetes.IndexRuntimeInput) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensures = append(r.ensures, input.TargetID)
	if input.TargetID == r.failTarget {
		return r.failErr
	}
	return nil
}

// StopIndexer records targetID.
func (r *selectiveFailIndexingRuntime) StopIndexer(_ context.Context, targetID domain.TargetID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stops = append(r.stops, targetID)
	return nil
}

// StopAll implements [kubernetes.IndexingRuntime].
func (r *selectiveFailIndexingRuntime) StopAll(context.Context) error { return nil }

// ensureIDs returns a copy of recorded EnsureIndexer target IDs.
func (r *selectiveFailIndexingRuntime) ensureIDs() []domain.TargetID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.TargetID, len(r.ensures))
	copy(out, r.ensures)
	return out
}

// stopIDs returns a copy of recorded StopIndexer target IDs.
func (r *selectiveFailIndexingRuntime) stopIDs() []domain.TargetID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.TargetID, len(r.stops))
	copy(out, r.stops)
	return out
}
