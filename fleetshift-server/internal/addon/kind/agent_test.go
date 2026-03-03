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

// channelDeliveryObserver collects events and completion results on
// channels, enabling deterministic waits in tests with async delivery.
// It implements [domain.DeliveryObserver].
type channelDeliveryObserver struct {
	mu     sync.Mutex
	events []domain.DeliveryEvent
	ch     chan domain.DeliveryEvent
	done   chan domain.DeliveryResult
}

func newChannelDeliveryObserver() *channelDeliveryObserver {
	return &channelDeliveryObserver{
		ch:   make(chan domain.DeliveryEvent, 100),
		done: make(chan domain.DeliveryResult, 1),
	}
}

func (o *channelDeliveryObserver) EventEmitted(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, e domain.DeliveryEvent) (context.Context, domain.EventEmittedProbe) {
	o.mu.Lock()
	o.events = append(o.events, e)
	o.mu.Unlock()
	o.ch <- e
	return ctx, domain.NoOpEventEmittedProbe{}
}

func (o *channelDeliveryObserver) Completed(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, result domain.DeliveryResult) (context.Context, domain.CompletedProbe) {
	o.done <- result
	return ctx, domain.NoOpCompletedProbe{}
}

func newChannelSignaler(obs *channelDeliveryObserver) *domain.DeliverySignaler {
	return domain.NewDeliverySignaler("", "", domain.TargetInfo{}, nil, nil, obs)
}

var nop = &domain.DeliverySignaler{}

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
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, signaler)
	if err != nil {
		t.Fatalf("Deliver should not return error after ack: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	// The fake provider emits a V(0) log line in Create (via observer
	// logger) before returning the error. Then deliverAsync emits an
	// error event.
	progress := <-obs.ch
	if progress.Kind != domain.DeliveryEventProgress {
		t.Errorf("first event kind = %q, want %q", progress.Kind, domain.DeliveryEventProgress)
	}
	errEvent := <-obs.ch
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
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, signaler)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	// The fake provider calls logger.V(0).Infof inside Create, which
	// flows through the observer logger to the signaler as a progress event.
	event := <-obs.ch
	if event.Kind != domain.DeliveryEventProgress {
		t.Errorf("event kind = %q, want %q", event.Kind, domain.DeliveryEventProgress)
	}
}

// recordingSignaler creates a *DeliverySignaler that appends emitted
// events to the provided slice. Used by observer_logger tests.
func recordingSignaler(events *[]domain.DeliveryEvent) *domain.DeliverySignaler {
	obs := &recordingDeliveryObserver{events: events}
	return domain.NewDeliverySignaler("", "", domain.TargetInfo{}, nil, nil, obs)
}

// recordingDeliveryObserver implements [domain.DeliveryObserver] by
// appending events to a slice. Used by observer_logger tests.
type recordingDeliveryObserver struct {
	events *[]domain.DeliveryEvent
}

func (o *recordingDeliveryObserver) EventEmitted(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, e domain.DeliveryEvent) (context.Context, domain.EventEmittedProbe) {
	*o.events = append(*o.events, e)
	return ctx, domain.NoOpEventEmittedProbe{}
}

func (o *recordingDeliveryObserver) Completed(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, _ domain.DeliveryResult) (context.Context, domain.CompletedProbe) {
	return ctx, domain.NoOpCompletedProbe{}
}
