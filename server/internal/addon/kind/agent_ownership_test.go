package kind_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestAgent_Deliver_RestartRecoversOwnership(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	store := kind.NewMemoryGenerationStore()

	agent1 := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(store), stubPlatformSA())
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})

	if err := agent1.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("first Deliver: %v", err)
	}
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("first State = %q", r.State)
	}
	creates := provider.createCount()

	// New agent process, same provider state + generation store (durable CM).
	agent2 := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(store), stubPlatformSA())
	if err := agent2.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("restart Deliver: %v", err)
	}
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("restart State = %q want Delivered: %s", r.State, r.Message)
	}
	if provider.createCount() != creates {
		t.Fatalf("Create called again after restart recovery")
	}
}

func TestAgent_Deliver_HigherGenerationRecreates(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	agent, store := newTestAgent(reporter, provider)
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})

	if err := agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("gen1: %v", err)
	}
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("gen1 State = %q", r.State)
	}

	manifests2 := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo","nodes":[{"role":"control-plane","image":"kindest/node:v1.30.0"}]}`),
	}}
	if err := agent.Deliver(context.Background(), target, "d1:t1", manifests2, domain.DeliveryAuth{}, nil, 2); err != nil {
		t.Fatalf("gen2: %v", err)
	}
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("gen2 State = %q: %s", r.State, r.Message)
	}
	if provider.deleteCount() != 1 {
		t.Fatalf("Delete count = %d, want 1", provider.deleteCount())
	}
	if provider.createCount() != 2 {
		t.Fatalf("Create count = %d, want 2", provider.createCount())
	}
	if g, found, _ := store.Get(context.Background(), "fs--demo", nil); !found || g != 2 {
		t.Fatalf("replacement generation = %d found=%v, want 2", g, found)
	}
}

func TestAgent_Deliver_HigherGenerationDeleteFailsThenRetryRecreates(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	store := kind.NewMemoryGenerationStore()
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(store), stubPlatformSA())
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})

	if err := agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("gen1: %v", err)
	}
	awaitDone(t, reporter.done)

	provider.deleteErr = errors.New("delete failed")
	if err := agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 2); err != nil {
		t.Fatalf("gen2 fail: %v", err)
	}
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateFailed {
		t.Fatalf("expected failed delete, got %q", r.State)
	}
	if g, found, _ := store.Get(context.Background(), "fs--demo", nil); !found || g != 1 {
		t.Fatalf("after failed delete gen=%d found=%v, want still 1", g, found)
	}

	provider.deleteErr = nil
	createsBefore := provider.createCount()
	if err := agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 2); err != nil {
		t.Fatalf("gen2 retry: %v", err)
	}
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("retry State = %q: %s", r.State, r.Message)
	}
	if provider.createCount() <= createsBefore {
		t.Fatal("retry must recreate (Create again)")
	}
	if g, found, _ := store.Get(context.Background(), "fs--demo", nil); !found || g != 2 {
		t.Fatalf("after retry gen=%d found=%v, want 2", g, found)
	}
}

func TestAgent_Deliver_LowerGenerationRejected(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	agent, _ := newTestAgent(reporter, provider)
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})

	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 2)
	awaitDone(t, reporter.done)

	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	r := awaitDone(t, reporter.done)
	if r.State != domain.DeliveryStateFailed || !strings.Contains(r.Message, "stale") {
		t.Fatalf("want stale fail, got %q %q", r.State, r.Message)
	}
}

func TestAgent_Remove_LowerGenerationRejected(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	agent, store := newTestAgent(reporter, provider)
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})

	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 2)
	awaitDone(t, reporter.done)

	_ = agent.Remove(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	r := awaitDone(t, reporter.done)
	if r.State != domain.DeliveryStateFailed || !strings.Contains(r.Message, "stale") {
		t.Fatalf("want stale remove fail, got %q %q", r.State, r.Message)
	}
	if !provider.hasCluster("fs--demo") {
		t.Fatal("cluster should remain after stale remove")
	}
	if g, _, _ := store.Get(context.Background(), "fs--demo", nil); g != 2 {
		t.Fatalf("gen = %d, want 2", g)
	}
}

func TestAgent_Deliver_OwnedWithoutCMRecreates(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["fs--demo"] = nil
	reporter := newChannelReporter()
	agent, store := newTestAgent(reporter, provider)

	_ = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1",
		[]domain.Manifest{{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		domain.DeliveryAuth{}, nil, 1)
	r := awaitDone(t, reporter.done)
	if r.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q: %s", r.State, r.Message)
	}
	if provider.deleteCount() != 1 {
		t.Fatalf("Delete count = %d, want 1", provider.deleteCount())
	}
	if provider.createCount() != 1 {
		t.Fatalf("Create count = %d, want 1", provider.createCount())
	}
	if g, found, _ := store.Get(context.Background(), "fs--demo", nil); !found || g != 1 {
		t.Fatalf("replacement generation = %d found=%v, want 1", g, found)
	}
}

// failAdvanceOnceStore fails the first CheckAndAdvance, then delegates.
type failAdvanceOnceStore struct {
	inner *kind.MemoryGenerationStore
	n     atomic.Int32
}

func (s *failAdvanceOnceStore) Get(ctx context.Context, name string, kc []byte) (domain.Generation, bool, error) {
	return s.inner.Get(ctx, name, kc)
}

func (s *failAdvanceOnceStore) CheckAndAdvance(ctx context.Context, name string, kc []byte, proposed domain.Generation) (kind.GenerationDisposition, domain.Generation, error) {
	if s.n.Add(1) == 1 {
		return 0, 0, errors.New("persist failed")
	}
	return s.inner.CheckAndAdvance(ctx, name, kc, proposed)
}

func (s *failAdvanceOnceStore) Forget(name string) { s.inner.Forget(name) }

func TestAgent_Deliver_MissingCMPersistFailThenRetryRecreates(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["fs--demo"] = nil
	reporter := newChannelReporter()
	inner := kind.NewMemoryGenerationStore()
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(&failAdvanceOnceStore{inner: inner}), stubPlatformSA())

	_ = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1",
		[]domain.Manifest{{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		domain.DeliveryAuth{}, nil, 1)
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateFailed {
		t.Fatalf("first State = %q, want Failed: %s", r.State, r.Message)
	}
	if provider.deleteCount() != 1 || provider.createCount() != 1 {
		t.Fatalf("after persist fail: deletes=%d creates=%d, want 1 each", provider.deleteCount(), provider.createCount())
	}
	if _, found, _ := inner.Get(context.Background(), "fs--demo", nil); found {
		t.Fatal("generation must not be recorded after persist failure")
	}
	if !provider.hasCluster("fs--demo") {
		t.Fatal("replacement cluster should exist after create-then-persist-fail")
	}

	createsBefore := provider.createCount()
	deletesBefore := provider.deleteCount()
	_ = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1",
		[]domain.Manifest{{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		domain.DeliveryAuth{}, nil, 1)
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("retry State = %q: %s", r.State, r.Message)
	}
	if provider.createCount() <= createsBefore || provider.deleteCount() <= deletesBefore {
		t.Fatalf("retry must recreate again; deletes=%d→%d creates=%d→%d",
			deletesBefore, provider.deleteCount(), createsBefore, provider.createCount())
	}
	if g, found, _ := inner.Get(context.Background(), "fs--demo", nil); !found || g != 1 {
		t.Fatalf("after retry gen=%d found=%v, want 1", g, found)
	}
}

func TestAgent_Deliver_MissingCMDeleteFailsRetainsClusterAndWatch(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	inv := inventoryTestWatcher(t, nil)
	agent, store := newTestAgent(reporter, provider, kind.WithInventoryWatcher(inv))
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})
	rn := domain.ResourceName("clusters/demo")

	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("deliver: %q %s", r.State, r.Message)
	}
	if !inv.IsWatchingForTest(rn) {
		t.Fatal("expected inventory watch after deliver")
	}

	// Simulate missing ownership ConfigMap while the cluster and watch remain.
	store.Forget("fs--demo")
	provider.deleteErr = errors.New("delete failed")

	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateFailed {
		t.Fatalf("want failed delete, got %q %s", r.State, r.Message)
	}
	if !provider.hasCluster("fs--demo") {
		t.Fatal("failed deletion must leave the existing cluster intact")
	}
	if !inv.IsWatchingForTest(rn) {
		t.Fatal("failed deletion must leave the inventory watch intact")
	}
	if provider.createCount() != 1 {
		t.Fatalf("Create must not run after failed delete; creates=%d", provider.createCount())
	}
}

func TestAgent_Deliver_MissingCMReplacementRecordsGenAndWatch(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	var watchStarts atomic.Int32
	inv := inventoryTestWatcher(t, &watchStarts)
	agent, store := newTestAgent(reporter, provider, kind.WithInventoryWatcher(inv))
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})
	rn := domain.ResourceName("clusters/demo")

	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	awaitDone(t, reporter.done)
	if got := watchStarts.Load(); got != 1 {
		t.Fatalf("watch starts after first deliver = %d, want 1", got)
	}

	store.Forget("fs--demo")
	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 3)
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("recreate: %q %s", r.State, r.Message)
	}
	if g, found, _ := store.Get(context.Background(), "fs--demo", nil); !found || g != 3 {
		t.Fatalf("replacement generation = %d found=%v, want 3", g, found)
	}
	if got := watchStarts.Load(); got != 2 {
		t.Fatalf("watch starts after replacement = %d, want 2", got)
	}
	if !inv.IsWatchingForTest(rn) {
		t.Fatal("expected watch on replacement cluster")
	}
}

func TestAgent_Deliver_GetGenerationErrorFailsWithoutDelete(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["fs--demo"] = nil
	reporter := newChannelReporter()
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(&errGetStore{err: errors.New("api unavailable")}), stubPlatformSA())

	_ = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1",
		[]domain.Manifest{{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		domain.DeliveryAuth{}, nil, 1)
	r := awaitDone(t, reporter.done)
	if r.State != domain.DeliveryStateFailed || !strings.Contains(r.Message, "ownership generation") {
		t.Fatalf("got %q %q", r.State, r.Message)
	}
	if provider.deleteCount() != 0 || provider.createCount() != 0 {
		t.Fatalf("Get error must not delete/recreate; deletes=%d creates=%d", provider.deleteCount(), provider.createCount())
	}
	if !provider.hasCluster("fs--demo") {
		t.Fatal("cluster must remain")
	}
}

type errGetStore struct {
	err error
}

func (s *errGetStore) Get(context.Context, string, []byte) (domain.Generation, bool, error) {
	return 0, false, s.err
}

func (s *errGetStore) CheckAndAdvance(context.Context, string, []byte, domain.Generation) (kind.GenerationDisposition, domain.Generation, error) {
	return 0, 0, errors.New("unexpected CheckAndAdvance")
}

func (s *errGetStore) Forget(string) {}

func TestAgent_Deliver_InvalidResourceIDRejected(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	agent, _ := newTestAgent(reporter, provider)
	_ = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1",
		[]domain.Manifest{{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"Bad_Name"}`)}},
		domain.DeliveryAuth{}, nil, 1)
	r := awaitDone(t, reporter.done)
	if r.State != domain.DeliveryStateFailed {
		t.Fatalf("State = %q, want Failed", r.State)
	}
	if provider.createCount() != 0 {
		t.Fatal("Create must not be called for invalid id")
	}
}

func TestAgent_Deliver_ListErrorFailsDelivery(t *testing.T) {
	provider := newFakeProvider()
	provider.listErr = errors.New("list failed")
	reporter := newChannelReporter()
	agent, _ := newTestAgent(reporter, provider)
	_ = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1",
		[]domain.Manifest{{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		domain.DeliveryAuth{}, nil, 1)
	r := awaitDone(t, reporter.done)
	if r.State != domain.DeliveryStateFailed || !strings.Contains(r.Message, "list") {
		t.Fatalf("got %q %q", r.State, r.Message)
	}
}

func TestAgent_Deliver_KubeConfigErrorFailsDelivery(t *testing.T) {
	provider := newFakeProvider()
	provider.kubeconfigErr = errors.New("kc failed")
	reporter := newChannelReporter()
	agent, _ := newTestAgent(reporter, provider)
	_ = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1",
		[]domain.Manifest{{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		domain.DeliveryAuth{}, nil, 1)
	r := awaitDone(t, reporter.done)
	if r.State != domain.DeliveryStateFailed || !strings.Contains(r.Message, "kubeconfig") {
		t.Fatalf("got %q %q", r.State, r.Message)
	}
}

// staleOnAdvanceStore returns not-found from Get (missing ConfigMap),
// then plants a higher generation before CheckAndAdvance on the
// replacement so the proposed gen is Stale.
type staleOnAdvanceStore struct {
	inner *kind.MemoryGenerationStore
	plant domain.Generation
}

func (s *staleOnAdvanceStore) Get(context.Context, string, []byte) (domain.Generation, bool, error) {
	return 0, false, nil
}

func (s *staleOnAdvanceStore) CheckAndAdvance(ctx context.Context, name string, kc []byte, proposed domain.Generation) (kind.GenerationDisposition, domain.Generation, error) {
	s.inner.SetForTest(name, s.plant)
	return s.inner.CheckAndAdvance(ctx, name, kc, proposed)
}

func (s *staleOnAdvanceStore) Forget(name string) { s.inner.Forget(name) }

func TestAgent_Deliver_CheckAndAdvanceStaleOnReplacementAfterMissingCM(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["fs--demo"] = nil
	reporter := newChannelReporter()
	inner := kind.NewMemoryGenerationStore()
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(&staleOnAdvanceStore{inner: inner, plant: 2}), stubPlatformSA())
	_ = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1",
		[]domain.Manifest{{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name":"demo"}`)}},
		domain.DeliveryAuth{}, nil, 1)
	r := awaitDone(t, reporter.done)
	if r.State != domain.DeliveryStateFailed || !strings.Contains(r.Message, "stale") {
		t.Fatalf("want stale fail, got %q %q", r.State, r.Message)
	}
	if provider.deleteCount() != 1 || provider.createCount() != 1 {
		t.Fatalf("missing CM must recreate before CheckAndAdvance; deletes=%d creates=%d",
			provider.deleteCount(), provider.createCount())
	}
}

func inventoryTestWatcher(t *testing.T, watchStarts *atomic.Int32) *kind.InventoryWatcher {
	t.Helper()
	return kind.NewInventoryWatcher(nopInventoryReporter{},
		kind.WithInventoryDebounce(0),
		kind.WithInventoryClientFactory(func([]byte) (kubernetes.Interface, error) {
			if watchStarts != nil {
				watchStarts.Add(1)
			}
			return fake.NewSimpleClientset(), nil
		}),
	)
}

type nopInventoryReporter struct{}

func (nopInventoryReporter) ApplyDeltaBatch(context.Context, domain.InventoryDeltaBatch) error {
	return nil
}

func TestAgent_Remove_StaleKeepsInventoryWatch(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	var watchStarts atomic.Int32
	inv := inventoryTestWatcher(t, &watchStarts)
	agent, _ := newTestAgent(reporter, provider, kind.WithInventoryWatcher(inv))
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})
	rn := domain.ResourceName("clusters/demo")

	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 2)
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("deliver: %q %s", r.State, r.Message)
	}
	if !inv.IsWatchingForTest(rn) {
		t.Fatal("expected inventory watch after deliver")
	}

	_ = agent.Remove(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateFailed || !strings.Contains(r.Message, "stale") {
		t.Fatalf("want stale remove, got %q %q", r.State, r.Message)
	}
	if !inv.IsWatchingForTest(rn) {
		t.Fatal("stale remove must not Unwatch; cluster still running")
	}
	if !provider.hasCluster("fs--demo") {
		t.Fatal("cluster should remain")
	}
}

func TestAgent_Remove_UnwatchAfterSuccessfulDelete(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	inv := inventoryTestWatcher(t, nil)
	agent, _ := newTestAgent(reporter, provider, kind.WithInventoryWatcher(inv))
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})
	rn := domain.ResourceName("clusters/demo")

	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	awaitDone(t, reporter.done)
	if !inv.IsWatchingForTest(rn) {
		t.Fatal("expected watch after deliver")
	}

	_ = agent.Remove(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("remove: %q %s", r.State, r.Message)
	}
	if inv.IsWatchingForTest(rn) {
		t.Fatal("expected Unwatch after confirmed delete")
	}
}

func TestAgent_Deliver_HigherGenerationUnwatchesBeforeRecreate(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	var watchStarts atomic.Int32
	inv := inventoryTestWatcher(t, &watchStarts)
	agent, _ := newTestAgent(reporter, provider, kind.WithInventoryWatcher(inv))
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo"}`),
	}}
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{})
	rn := domain.ResourceName("clusters/demo")

	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	awaitDone(t, reporter.done)
	if got := watchStarts.Load(); got != 1 {
		t.Fatalf("watch starts after gen1 = %d, want 1", got)
	}
	if !inv.IsWatchingForTest(rn) {
		t.Fatal("expected watch after gen1")
	}

	manifests2 := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"demo","nodes":[{"role":"control-plane","image":"kindest/node:v1.30.0"}]}`),
	}}
	_ = agent.Deliver(context.Background(), target, "d1:t1", manifests2, domain.DeliveryAuth{}, nil, 2)
	if r := awaitDone(t, reporter.done); r.State != domain.DeliveryStateDelivered {
		t.Fatalf("gen2: %q %s", r.State, r.Message)
	}
	if got := watchStarts.Load(); got != 2 {
		t.Fatalf("watch starts after recreate = %d, want 2 (Unwatch then Watch replacement)", got)
	}
	if !inv.IsWatchingForTest(rn) {
		t.Fatal("expected watch on replacement cluster")
	}
}
