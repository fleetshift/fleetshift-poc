package kind_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/log"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// fakeProvider is a reusable in-memory implementation of
// [kind.ClusterProvider] for tests. It signals on created after each
// Create call (success or failure), enabling deterministic waits for
// the async delivery goroutine.
type fakeProvider struct {
	mu        sync.Mutex
	clusters  map[string][]byte // name → raw config
	createErr error
	logger    log.Logger
	created   chan string // receives cluster name after each Create; buffered
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{
		clusters: make(map[string][]byte),
		created:  make(chan string, 10),
	}
}

func (p *fakeProvider) Create(name string, opts ...cluster.CreateOption) error {
	if p.logger != nil {
		p.logger.V(0).Infof("Creating cluster %q", name)
	}
	defer func() { p.created <- name }()
	if p.createErr != nil {
		return p.createErr
	}
	p.mu.Lock()
	p.clusters[name] = nil
	p.mu.Unlock()
	return nil
}

func (p *fakeProvider) Delete(name, _ string) error {
	p.mu.Lock()
	delete(p.clusters, name)
	p.mu.Unlock()
	return nil
}

func (p *fakeProvider) List() ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.clusters))
	for n := range p.clusters {
		out = append(out, n)
	}
	return out, nil
}

func (p *fakeProvider) hasCluster(name string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.clusters[name]
	return ok
}

func (p *fakeProvider) clusterCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.clusters)
}

func fakeFactory(p *fakeProvider) kind.ClusterProviderFactory {
	return func(logger log.Logger) kind.ClusterProvider {
		p.logger = logger
		return p
	}
}

// channelObserver collects events and signals each one on a channel,
// enabling deterministic waits in tests with async delivery.
type channelObserver struct {
	mu     sync.Mutex
	events []domain.DeliveryEvent
	ch     chan domain.DeliveryEvent
	done   chan domain.DeliveryResult
}

func newChannelObserver() *channelObserver {
	return &channelObserver{
		ch:   make(chan domain.DeliveryEvent, 100),
		done: make(chan domain.DeliveryResult, 1),
	}
}

func (o *channelObserver) Emit(e domain.DeliveryEvent) {
	o.mu.Lock()
	o.events = append(o.events, e)
	o.mu.Unlock()
	o.ch <- e
}

func (o *channelObserver) Done(result domain.DeliveryResult) {
	o.done <- result
}

var nop = domain.NopDeliveryObserver{}

func TestAgent_Deliver_CreatesCluster(t *testing.T) {
	provider := newFakeProvider()
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, nop)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	<-provider.created
	if !provider.hasCluster("dev-cluster") {
		t.Error("expected cluster 'dev-cluster' to exist")
	}
}

func TestAgent_Deliver_RecreatesExistingCluster(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["dev-cluster"] = nil
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, nop)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	<-provider.created
	if !provider.hasCluster("dev-cluster") {
		t.Error("expected cluster 'dev-cluster' to exist after recreate")
	}
}

func TestAgent_Deliver_MissingNameReturnsError(t *testing.T) {
	provider := newFakeProvider()
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, nop)
	if err == nil {
		t.Fatal("expected error for missing cluster name")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("expected ErrInvalidArgument, got: %v", err)
	}
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
}

func TestAgent_Deliver_CreateFailureEmitsError(t *testing.T) {
	provider := newFakeProvider()
	provider.createErr = errors.New("docker not available")
	observer := newChannelObserver()
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, observer)
	if err != nil {
		t.Fatalf("Deliver should not return error after ack: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	// The fake provider emits a V(0) log line in Create (via observer
	// logger) before returning the error. Then deliverAsync emits an
	// error event.
	progress := <-observer.ch
	if progress.Kind != domain.DeliveryEventProgress {
		t.Errorf("first event kind = %q, want %q", progress.Kind, domain.DeliveryEventProgress)
	}
	errEvent := <-observer.ch
	if errEvent.Kind != domain.DeliveryEventError {
		t.Errorf("second event kind = %q, want %q", errEvent.Kind, domain.DeliveryEventError)
	}
}

func TestAgent_Remove_IsNoopForNow(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["dev-cluster"] = nil
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	if err := agent.Remove(context.Background(), target, "d1:k1", nop); err != nil {
		t.Fatalf("Remove: %v", err)
	}
}

func TestAgent_Deliver_MultipleManifests(t *testing.T) {
	provider := newFakeProvider()
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{
		{ResourceType: kind.ClusterResourceType, Raw: json.RawMessage(`{"name": "cluster-a"}`)},
		{ResourceType: kind.ClusterResourceType, Raw: json.RawMessage(`{"name": "cluster-b"}`)},
	}

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, nop)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	<-provider.created
	<-provider.created
	if provider.clusterCount() != 2 {
		t.Errorf("expected 2 clusters, got %d", provider.clusterCount())
	}
}

func TestAgent_Deliver_WiresObserverLogger(t *testing.T) {
	provider := newFakeProvider()
	observer := newChannelObserver()
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, observer)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	// The fake provider calls logger.V(0).Infof inside Create, which
	// flows through the observer logger to the observer as a progress event.
	event := <-observer.ch
	if event.Kind != domain.DeliveryEventProgress {
		t.Errorf("event kind = %q, want %q", event.Kind, domain.DeliveryEventProgress)
	}
}

// recordingObserver is a simple synchronous observer used by
// observer_logger tests. See [channelObserver] for async agent tests.
type recordingObserver struct {
	events *[]domain.DeliveryEvent
}

func (o *recordingObserver) Emit(e domain.DeliveryEvent) {
	*o.events = append(*o.events, e)
}

func (o *recordingObserver) Done(domain.DeliveryResult) {}
