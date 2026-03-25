package kind_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func (p *fakeProvider) KubeConfig(name string, _ bool) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.clusters[name]; !ok {
		return "", fmt.Errorf("cluster %q not found", name)
	}
	return "apiVersion: v1\nkind: Config\nclusters:\n- cluster:\n    server: https://127.0.0.1:6443\n  name: kind-" + name + "\n", nil
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

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nop)
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

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nop)
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

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nop)
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

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, signaler)
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

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nop)
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

	result, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, signaler)
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

func TestAgent_Deliver_ProducesTargetOutputs(t *testing.T) {
	provider := newFakeProvider()
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	_, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, signaler)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := <-obs.done

	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q, want %q", result.State, domain.DeliveryStateDelivered)
	}
	if len(result.ProvisionedTargets) != 1 {
		t.Fatalf("ProvisionedTargets count = %d, want 1", len(result.ProvisionedTargets))
	}
	if len(result.ProducedSecrets) != 0 {
		t.Fatalf("ProducedSecrets count = %d, want 0 (no stored credentials)", len(result.ProducedSecrets))
	}

	pt := result.ProvisionedTargets[0]
	if pt.ID != "k8s-dev-cluster" {
		t.Errorf("target ID = %q, want %q", pt.ID, "k8s-dev-cluster")
	}
	if pt.Type != kind.KubernetesTargetType {
		t.Errorf("target Type = %q, want %q", pt.Type, kind.KubernetesTargetType)
	}
	if pt.Name != "dev-cluster" {
		t.Errorf("target Name = %q, want %q", pt.Name, "dev-cluster")
	}
	apiServer, ok := pt.Properties["api_server"]
	if !ok || apiServer == "" {
		t.Fatal("target Properties missing api_server")
	}
}

func TestAgent_Deliver_MultipleManifests_ProducesMultipleOutputs(t *testing.T) {
	provider := newFakeProvider()
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)
	agent := kind.NewAgent(fakeFactory(provider))

	target := domain.TargetInfo{ID: "k1", Type: kind.TargetType, Name: "local-kind"}
	manifests := []domain.Manifest{
		{ResourceType: kind.ClusterResourceType, Raw: json.RawMessage(`{"name": "cluster-a"}`)},
		{ResourceType: kind.ClusterResourceType, Raw: json.RawMessage(`{"name": "cluster-b"}`)},
	}

	_, err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, signaler)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := <-obs.done

	if len(result.ProvisionedTargets) != 2 {
		t.Errorf("ProvisionedTargets count = %d, want 2", len(result.ProvisionedTargets))
	}
	if len(result.ProducedSecrets) != 0 {
		t.Errorf("ProducedSecrets count = %d, want 0", len(result.ProducedSecrets))
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

// recordingAgentObserver captures [kind.ClusterDeliverProbe] events.
type recordingAgentObserver struct {
	kind.NoOpAgentObserver
	mu      sync.Mutex
	probes  []*recordingClusterProbe
}

func (o *recordingAgentObserver) ClusterDeliverStarted(ctx context.Context, clusterName string) (context.Context, kind.ClusterDeliverProbe) {
	p := &recordingClusterProbe{clusterName: clusterName}
	o.mu.Lock()
	o.probes = append(o.probes, p)
	o.mu.Unlock()
	return ctx, p
}

type recordingClusterProbe struct {
	kind.NoOpClusterDeliverProbe
	clusterName string
	source      kind.ConfigSource
	issuerURL   domain.IssuerURL
	audience    domain.Audience
	rbacSubject domain.SubjectID
	rbacUser    string
	err         error
	ended       bool
}

func (p *recordingClusterProbe) ConfigResolved(source kind.ConfigSource, issuerURL domain.IssuerURL, audience domain.Audience) {
	p.source = source
	p.issuerURL = issuerURL
	p.audience = audience
}

func (p *recordingClusterProbe) RBACBootstrapped(subjectID domain.SubjectID, username string) {
	p.rbacSubject = subjectID
	p.rbacUser = username
}

func (p *recordingClusterProbe) Error(err error) { p.err = err }
func (p *recordingClusterProbe) End()            { p.ended = true }

func TestAgent_Observer_DefaultConfig(t *testing.T) {
	provider := newFakeProvider()
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	agentObs := &recordingAgentObserver{}
	agent := kind.NewAgent(fakeFactory(provider), kind.WithObserver(agentObs))

	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "default-cfg"}`),
	}}

	_, err := agent.Deliver(context.Background(), domain.TargetInfo{}, "d1:k1", manifests, domain.DeliveryAuth{}, signaler)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	<-obs.done

	agentObs.mu.Lock()
	defer agentObs.mu.Unlock()

	if len(agentObs.probes) != 1 {
		t.Fatalf("expected 1 probe, got %d", len(agentObs.probes))
	}
	p := agentObs.probes[0]
	if p.clusterName != "default-cfg" {
		t.Errorf("clusterName = %q, want %q", p.clusterName, "default-cfg")
	}
	if p.source != kind.ConfigSourceDefault {
		t.Errorf("source = %q, want %q", p.source, kind.ConfigSourceDefault)
	}
	if !p.ended {
		t.Error("probe.End() was not called")
	}
}

func TestAgent_Observer_CustomConfig(t *testing.T) {
	provider := newFakeProvider()
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	agentObs := &recordingAgentObserver{}
	agent := kind.NewAgent(fakeFactory(provider), kind.WithObserver(agentObs))

	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "custom-cfg", "config": "kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4"}`),
	}}

	_, err := agent.Deliver(context.Background(), domain.TargetInfo{}, "d1:k1", manifests, domain.DeliveryAuth{}, signaler)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	<-obs.done

	agentObs.mu.Lock()
	defer agentObs.mu.Unlock()

	if len(agentObs.probes) != 1 {
		t.Fatalf("expected 1 probe, got %d", len(agentObs.probes))
	}
	if agentObs.probes[0].source != kind.ConfigSourceCustom {
		t.Errorf("source = %q, want %q", agentObs.probes[0].source, kind.ConfigSourceCustom)
	}
}

// fakeTokenVerifier implements [domain.OIDCTokenVerifier] for unit tests.
type fakeTokenVerifier struct {
	err error // if non-nil, Verify returns this error
}

func (v *fakeTokenVerifier) Verify(_ context.Context, _ domain.OIDCConfig, _ string) (domain.SubjectClaims, error) {
	if v.err != nil {
		return domain.SubjectClaims{}, v.err
	}
	return domain.SubjectClaims{ID: "alice", Issuer: "https://issuer.example.com"}, nil
}

func TestAgent_Deliver_WithTokenVerifier_ValidToken(t *testing.T) {
	provider := newFakeProvider()
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	verifier := &fakeTokenVerifier{}
	cfg := domain.OIDCConfig{
		IssuerURL: "https://issuer.example.com",
		Audience:  "fleetshift",
	}

	agent := kind.NewAgent(fakeFactory(provider), kind.WithTokenVerifier(verifier, cfg))

	auth := domain.DeliveryAuth{
		Token: "valid-token",
	}

	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "verified-cluster"}`),
	}}

	result, err := agent.Deliver(context.Background(), domain.TargetInfo{}, "d1:k1", manifests, auth, signaler)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	doneResult := <-obs.done
	if doneResult.State != domain.DeliveryStateDelivered {
		t.Errorf("async State = %q, want %q", doneResult.State, domain.DeliveryStateDelivered)
	}
}

func TestAgent_Deliver_WithTokenVerifier_ExpiredToken(t *testing.T) {
	provider := newFakeProvider()

	verifier := &fakeTokenVerifier{err: errors.New("token expired")}
	cfg := domain.OIDCConfig{
		IssuerURL: "https://issuer.example.com",
		Audience:  "fleetshift",
	}

	agent := kind.NewAgent(fakeFactory(provider), kind.WithTokenVerifier(verifier, cfg))

	auth := domain.DeliveryAuth{
		Token: "expired-token",
		Caller: &domain.SubjectClaims{
			ID:     "alice",
			Issuer: "https://issuer.example.com",
		},
		Audience: []domain.Audience{"fleetshift"},
	}

	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "rejected-cluster"}`),
	}}

	result, err := agent.Deliver(context.Background(), domain.TargetInfo{}, "d1:k1", manifests, auth, nop)
	if err != nil {
		t.Fatalf("Deliver should not return an error (auth failure is a result, not an error): %v", err)
	}
	if result.State != domain.DeliveryStateAuthFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAuthFailed)
	}
	if result.Message == "" {
		t.Error("expected a message describing the verification failure")
	}

	if provider.clusterCount() != 0 {
		t.Error("no cluster should have been created when token verification fails")
	}
}

func TestAgent_Deliver_WithTokenVerifier_NoToken_SkipsVerification(t *testing.T) {
	provider := newFakeProvider()
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	verifier := &fakeTokenVerifier{err: errors.New("should not be called")}
	cfg := domain.OIDCConfig{
		IssuerURL: "https://issuer.example.com",
		Audience:  "fleetshift",
	}

	agent := kind.NewAgent(fakeFactory(provider), kind.WithTokenVerifier(verifier, cfg))

	manifests := []domain.Manifest{{
		ResourceType: kind.ClusterResourceType,
		Raw:          json.RawMessage(`{"name": "no-token-cluster"}`),
	}}

	result, err := agent.Deliver(context.Background(), domain.TargetInfo{}, "d1:k1", manifests, domain.DeliveryAuth{}, signaler)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if result.State != domain.DeliveryStateAccepted {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateAccepted)
	}

	doneResult := <-obs.done
	if doneResult.State != domain.DeliveryStateDelivered {
		t.Errorf("async State = %q, want %q", doneResult.State, domain.DeliveryStateDelivered)
	}
}

func TestAgent_Observer_MultipleSpecs(t *testing.T) {
	provider := newFakeProvider()
	obs := newChannelDeliveryObserver()
	signaler := newChannelSignaler(obs)

	agentObs := &recordingAgentObserver{}
	agent := kind.NewAgent(fakeFactory(provider), kind.WithObserver(agentObs))

	manifests := []domain.Manifest{
		{ResourceType: kind.ClusterResourceType, Raw: json.RawMessage(`{"name": "a"}`)},
		{ResourceType: kind.ClusterResourceType, Raw: json.RawMessage(`{"name": "b"}`)},
	}

	_, err := agent.Deliver(context.Background(), domain.TargetInfo{}, "d1:k1", manifests, domain.DeliveryAuth{}, signaler)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	<-obs.done

	agentObs.mu.Lock()
	defer agentObs.mu.Unlock()

	if len(agentObs.probes) != 2 {
		t.Fatalf("expected 2 probes (one per cluster), got %d", len(agentObs.probes))
	}
	if agentObs.probes[0].clusterName != "a" {
		t.Errorf("probes[0].clusterName = %q, want %q", agentObs.probes[0].clusterName, "a")
	}
	if agentObs.probes[1].clusterName != "b" {
		t.Errorf("probes[1].clusterName = %q, want %q", agentObs.probes[1].clusterName, "b")
	}
}
