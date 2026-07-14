package kind_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type recordingIndexingRuntime struct {
	mu        sync.Mutex
	ensures   []kubernetes.IndexRuntimeInput
	stops     []domain.TargetID
	ensureErr error
	stopErr   error
}

func (r *recordingIndexingRuntime) EnsureIndexer(_ context.Context, input kubernetes.IndexRuntimeInput) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensures = append(r.ensures, input)
	return r.ensureErr
}

func (r *recordingIndexingRuntime) StopIndexer(_ context.Context, targetID domain.TargetID) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stops = append(r.stops, targetID)
	return r.stopErr
}

func (r *recordingIndexingRuntime) StopAll(context.Context) error { return nil }

func (r *recordingIndexingRuntime) ensureCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.ensures)
}

func (r *recordingIndexingRuntime) stopIDs() []domain.TargetID {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.TargetID, len(r.stops))
	copy(out, r.stops)
	return out
}

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
