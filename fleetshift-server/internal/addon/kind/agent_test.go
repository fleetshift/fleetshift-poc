package kind_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"sigs.k8s.io/kind/pkg/cluster"
	"sigs.k8s.io/kind/pkg/log"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testutil"
)

// awaitDone drains one result from ch with a safety-net timeout so
// that a regression in the fake delivery pipeline hangs for at most
// [testutil.UnitTimeout] rather than the global go-test deadline.
func awaitDone(t *testing.T, ch <-chan domain.DeliveryResult) domain.DeliveryResult {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(testutil.UnitTimeout):
		t.Fatal("timed out waiting for delivery result")
		return domain.DeliveryResult{}
	}
}

// fakeProvider is a reusable in-memory implementation of
// [kind.ClusterProvider] for tests. It signals on created after each
// Create call (success or failure), enabling deterministic waits for
// the async delivery goroutine.
type fakeProvider struct {
	mu            sync.Mutex
	clusters      map[string][]byte // name → raw config
	createErr     error
	deleteErr     error
	listErr       error
	kubeconfigErr error
	logger        log.Logger
	created       chan string // receives cluster name after each Create; buffered
	deleted       []string    // tracks deleted cluster names
	createCalls   int
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
	p.mu.Lock()
	p.createCalls++
	p.mu.Unlock()
	if p.createErr != nil {
		return p.createErr
	}
	p.mu.Lock()
	p.clusters[name] = nil
	p.mu.Unlock()
	return nil
}

func (p *fakeProvider) Delete(name, _ string) error {
	if p.logger != nil {
		p.logger.V(0).Infof("Deleting cluster %q", name)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.deleteErr != nil {
		return p.deleteErr
	}
	delete(p.clusters, name)
	p.deleted = append(p.deleted, name)
	return nil
}

func (p *fakeProvider) List() ([]string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.listErr != nil {
		return nil, p.listErr
	}
	out := make([]string, 0, len(p.clusters))
	for n := range p.clusters {
		out = append(out, n)
	}
	return out, nil
}

func (p *fakeProvider) KubeConfig(name string, _ bool) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.kubeconfigErr != nil {
		return "", p.kubeconfigErr
	}
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

func (p *fakeProvider) deleteCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.deleted)
}

func (p *fakeProvider) createCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.createCalls
}

func fakeFactory(p *fakeProvider) kind.ClusterProviderFactory {
	return func(logger log.Logger) kind.ClusterProvider {
		p.logger = logger
		return p
	}
}

func stubPlatformSA() kind.AgentOption {
	return kind.WithPlatformSABootstrap(func(_ context.Context, _ []byte, targetID domain.TargetID) (domain.SecretRef, []byte, error) {
		return domain.SecretRef(fmt.Sprintf("targets/%s/sa-token", targetID)), []byte("test-sa-token"), nil
	})
}

// channelReporter implements [domain.DeliveryReporter] for tests,
// routing events and results to channels for deterministic waits.
type channelReporter struct {
	mu     sync.Mutex
	events []domain.DeliveryEvent
	ch     chan domain.DeliveryEvent
	done   chan domain.DeliveryResult
}

func newChannelReporter() *channelReporter {
	return &channelReporter{
		ch:   make(chan domain.DeliveryEvent, 100),
		done: make(chan domain.DeliveryResult, 10),
	}
}

func (r *channelReporter) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.Generation, event domain.DeliveryEvent) error {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
	r.ch <- event
	return nil
}

func (r *channelReporter) ReportResult(_ context.Context, _ domain.DeliveryID, _ domain.Generation, result domain.DeliveryResult) error {
	r.done <- result
	return nil
}

func (r *channelReporter) ListActiveDeliveries(_ context.Context, _ []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

func newTestAgent(reporter domain.DeliveryReporter, p *fakeProvider, opts ...kind.AgentOption) (*kind.Agent, *kind.MemoryGenerationStore) {
	store := kind.NewMemoryGenerationStore()
	all := append([]kind.AgentOption{kind.WithGenerationStore(store), stubPlatformSA()}, opts...)
	return kind.NewAgent(reporter, fakeFactory(p), all...), store
}

type nopReporter struct{}

func (nopReporter) ReportEvent(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryEvent) error {
	return nil
}
func (nopReporter) ReportResult(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryResult) error {
	return nil
}
func (nopReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

func TestAgent_Deliver_CreatesCluster(t *testing.T) {
	provider := newFakeProvider()
	agent, _ := newTestAgent(nopReporter{}, provider)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	<-provider.created
	if !provider.hasCluster("fs--dev-cluster") {
		t.Error("expected cluster 'fs--dev-cluster' to exist")
	}
}

func TestAgent_Deliver_RejectsUnmanagedExistingCluster(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["dev-cluster"] = nil
	reporter := newChannelReporter()
	agent, _ := newTestAgent(reporter, provider)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
	if !strings.Contains(result.Message, "not managed") {
		t.Errorf("Message = %q, want not managed", result.Message)
	}
	if provider.deleteCount() != 0 {
		t.Errorf("Delete called %d times, want 0 (reject, do not recreate)", provider.deleteCount())
	}
	if !provider.hasCluster("dev-cluster") {
		t.Error("expected existing cluster to remain")
	}
}

func TestAgent_Deliver_SameGenerationRetrySkipsCreate(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	agent, store := newTestAgent(reporter, provider)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	if err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("first Deliver: %v", err)
	}
	first := awaitDone(t, reporter.done)
	if first.State != domain.DeliveryStateDelivered {
		t.Fatalf("first State = %q, want %q", first.State, domain.DeliveryStateDelivered)
	}
	if got := provider.createCount(); got != 1 {
		t.Fatalf("Create called %d times after first deliver, want 1", got)
	}
	if g, found, _ := store.Get(context.Background(), "fs--dev-cluster", nil); !found || g != 1 {
		t.Fatalf("store gen=%d found=%v, want 1 true", g, found)
	}

	// Same delivery ID + generation: at-least-once retry.
	if err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("second Deliver: %v", err)
	}
	second := awaitDone(t, reporter.done)
	if second.State != domain.DeliveryStateDelivered {
		t.Fatalf("second State = %q, want %q", second.State, domain.DeliveryStateDelivered)
	}
	if got := provider.createCount(); got != 1 {
		t.Fatalf("Create called %d times after retry, want 1 (skip create)", got)
	}
	if provider.deleteCount() != 0 {
		t.Errorf("Delete called %d times, want 0", provider.deleteCount())
	}
	if len(second.ProvisionedTargets) != 1 {
		t.Fatalf("second ProvisionedTargets count = %d, want 1", len(second.ProvisionedTargets))
	}
}

// TestAgent_Deliver_InflightClearedBeforeTerminalReport pins the ordering
// that at-least-once redelivery of the same delivery ID must be able to
// start as soon as ReportResult is observed. A reporter that synchronously
// re-Delivers from ReportResult would hang if inflight were still set
// (the CI flake in SameGenerationRetrySkipsCreate).
func TestAgent_Deliver_InflightClearedBeforeTerminalReport(t *testing.T) {
	provider := newFakeProvider()
	base := newChannelReporter()
	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}
	const deliveryID domain.DeliveryID = "d1:k1"

	var agent *kind.Agent
	reporter := &redeliverOnResultReporter{channelReporter: base}
	reporter.redeliver = func() {
		if err := agent.Deliver(context.Background(), target, deliveryID, manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
			t.Errorf("nested Deliver: %v", err)
		}
	}
	agent, _ = newTestAgent(reporter, provider)

	if err := agent.Deliver(context.Background(), target, deliveryID, manifests, domain.DeliveryAuth{}, nil, 1); err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	first := awaitDone(t, reporter.done)
	if first.State != domain.DeliveryStateDelivered {
		t.Fatalf("first State = %q, want %q", first.State, domain.DeliveryStateDelivered)
	}
	second := awaitDone(t, reporter.done)
	if second.State != domain.DeliveryStateDelivered {
		t.Fatalf("second State = %q, want %q", second.State, domain.DeliveryStateDelivered)
	}
	if got := provider.createCount(); got != 1 {
		t.Fatalf("Create called %d times, want 1", got)
	}
}

// redeliverOnResultReporter invokes redeliver once from ReportResult after
// forwarding the result, simulating an at-least-once retry observed as
// soon as the terminal report is delivered.
type redeliverOnResultReporter struct {
	*channelReporter
	once      sync.Once
	redeliver func()
}

func (r *redeliverOnResultReporter) ReportResult(
	ctx context.Context,
	id domain.DeliveryID,
	gen domain.Generation,
	result domain.DeliveryResult,
) error {
	err := r.channelReporter.ReportResult(ctx, id, gen, result)
	r.once.Do(func() {
		if r.redeliver != nil {
			r.redeliver()
		}
	})
	return err
}

func TestAgent_Deliver_MissingNameReturnsFailedResult(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	agent, _ := newTestAgent(reporter, provider)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return dispatch error: %v", err)
	}
	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateFailed {
		t.Errorf("State = %q, want %q", result.State, domain.DeliveryStateFailed)
	}
}

func TestAgent_Deliver_CreateFailureEmitsError(t *testing.T) {
	provider := newFakeProvider()
	provider.createErr = errors.New("docker not available")
	reporter := newChannelReporter()
	agent, _ := newTestAgent(reporter, provider)

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return error: %v", err)
	}

	// The fake provider emits a V(0) log line in Create (via observer
	// logger) before returning the error. Then deliverAsync emits an
	// error event.
	progress := <-reporter.ch
	if progress.Kind != domain.DeliveryEventProgress {
		t.Errorf("first event kind = %q, want %q", progress.Kind, domain.DeliveryEventProgress)
	}
	errEvent := <-reporter.ch
	if errEvent.Kind != domain.DeliveryEventError {
		t.Errorf("second event kind = %q, want %q", errEvent.Kind, domain.DeliveryEventError)
	}
}

func TestAgent_Remove_DeletesCluster(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["fs--my-cluster"] = nil
	reporter := newChannelReporter()
	agent, store := newTestAgent(reporter, provider)
	store.SetForTest("fs--my-cluster", 1)

	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"my-cluster"}`),
	}}

	err := agent.Remove(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("result.State = %q, want %q", result.State, domain.DeliveryStateDelivered)
	}

	if len(provider.deleted) != 1 || provider.deleted[0] != "fs--my-cluster" {
		t.Errorf("deleted = %v, want [fs--my-cluster]", provider.deleted)
	}
}

func TestAgent_Remove_ClusterAlreadyGone(t *testing.T) {
	provider := newFakeProvider()
	// cluster doesn't exist
	reporter := newChannelReporter()
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA())

	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"gone-cluster"}`),
	}}

	err := agent.Remove(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Remove should succeed for non-existent cluster: %v", err)
	}

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("result.State = %q, want %q", result.State, domain.DeliveryStateDelivered)
	}

	if len(provider.deleted) != 0 {
		t.Errorf("should not have called Delete, but deleted = %v", provider.deleted)
	}
}

func TestAgent_Deliver_MultipleManifests(t *testing.T) {
	provider := newFakeProvider()
	agent := kind.NewAgent(nopReporter{}, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA())

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{
		{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name": "cluster-a"}`)},
		{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name": "cluster-b"}`)},
	}

	err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	<-provider.created
	<-provider.created
	if provider.clusterCount() != 2 {
		t.Errorf("expected 2 clusters, got %d", provider.clusterCount())
	}
}

func TestAgent_Deliver_WiresObserverLogger(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA())

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	// The fake provider calls logger.V(0).Infof inside Create, which
	// flows through the observer logger to the reporter as a progress event.
	event := <-reporter.ch
	if event.Kind != domain.DeliveryEventProgress {
		t.Errorf("event kind = %q, want %q", event.Kind, domain.DeliveryEventProgress)
	}
}

func TestAgent_Remove_WiresObserverLogger(t *testing.T) {
	provider := newFakeProvider()
	provider.clusters["fs--my-cluster"] = nil
	reporter := newChannelReporter()
	agent, store := newTestAgent(reporter, provider)
	store.SetForTest("fs--my-cluster", 1)

	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"my-cluster"}`),
	}}

	err := agent.Remove(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Remove: %v", err)
	}

	// The fake provider calls logger.V(0).Infof inside Delete, which
	// flows through the observer logger to the reporter as a progress event.
	// A nil logger would skip that Infof and hang here — and on a real
	// kind provider, ProviderWithLogger(nil) panics during detection.
	event := <-reporter.ch
	if event.Kind != domain.DeliveryEventProgress {
		t.Errorf("event kind = %q, want %q", event.Kind, domain.DeliveryEventProgress)
	}
	if provider.logger == nil {
		t.Fatal("Remove passed nil logger to provider factory")
	}
	awaitDone(t, reporter.done)
}

func TestAgent_Deliver_ProducesTargetOutputs(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA())

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "dev-cluster"}`),
	}}

	err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := awaitDone(t, reporter.done)

	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q, want %q", result.State, domain.DeliveryStateDelivered)
	}
	if len(result.ProvisionedTargets) != 1 {
		t.Fatalf("ProvisionedTargets count = %d, want 1", len(result.ProvisionedTargets))
	}
	if len(result.ProducedSecrets) != 1 {
		t.Fatalf("ProducedSecrets count = %d, want 1", len(result.ProducedSecrets))
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
	if pt.Properties[kubernetes.PropClusterResourceName] != "clusters/dev-cluster" {
		t.Fatalf("cluster_resource_name = %q, want clusters/dev-cluster", pt.Properties[kubernetes.PropClusterResourceName])
	}
}

func TestAgent_Deliver_MultipleManifests_ProducesMultipleOutputs(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA())

	target := domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k1", Type: kind.TargetType, Name: "local-kind"})
	manifests := []domain.Manifest{
		{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name": "cluster-a"}`)},
		{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name": "cluster-b"}`)},
	}

	err := agent.Deliver(context.Background(), target, "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	result := awaitDone(t, reporter.done)

	if len(result.ProvisionedTargets) != 2 {
		t.Errorf("ProvisionedTargets count = %d, want 2", len(result.ProvisionedTargets))
	}
	if len(result.ProducedSecrets) != 2 {
		t.Errorf("ProducedSecrets count = %d, want 2", len(result.ProducedSecrets))
	}
}

// recordingReporter implements [domain.DeliveryReporter] by appending
// events to a slice. Used by observer_logger tests.
type recordingReporter struct {
	events *[]domain.DeliveryEvent
}

func (r *recordingReporter) ReportEvent(_ context.Context, _ domain.DeliveryID, _ domain.Generation, event domain.DeliveryEvent) error {
	*r.events = append(*r.events, event)
	return nil
}

func (r *recordingReporter) ReportResult(context.Context, domain.DeliveryID, domain.Generation, domain.DeliveryResult) error {
	return nil
}

func (r *recordingReporter) ListActiveDeliveries(context.Context, []domain.TargetID) ([]domain.ActiveDelivery, error) {
	return nil, nil
}

// recordingAgentObserver captures [kind.ClusterDeliverProbe] events.
type recordingAgentObserver struct {
	kind.NoOpAgentObserver
	mu     sync.Mutex
	probes []*recordingClusterProbe
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
	reporter := newChannelReporter()

	agentObs := &recordingAgentObserver{}
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA(), kind.WithObserver(agentObs))

	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "default-cfg"}`),
	}}

	err := agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	awaitDone(t, reporter.done)

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
	reporter := newChannelReporter()

	agentObs := &recordingAgentObserver{}
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA(), kind.WithObserver(agentObs))

	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "custom-cfg", "nodes": [{"role": "control-plane"}, {"role": "worker"}]}`),
	}}

	err := agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	awaitDone(t, reporter.done)

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
	return domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "alice",
			Issuer:  "https://issuer.example.com",
		},
	}, nil
}

func TestAgent_Deliver_WithTokenVerifier_ValidToken(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()

	verifier := &fakeTokenVerifier{}
	cfg := domain.OIDCConfig{
		IssuerURL: "https://issuer.example.com",
		Audience:  "fleetshift",
	}

	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA(), kind.WithTokenVerifier(verifier, cfg))

	auth := domain.DeliveryAuth{
		Token: "valid-token",
	}

	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "verified-cluster"}`),
	}}

	err := agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:k1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	doneResult := awaitDone(t, reporter.done)
	if doneResult.State != domain.DeliveryStateDelivered {
		t.Errorf("async State = %q, want %q", doneResult.State, domain.DeliveryStateDelivered)
	}
}

func TestAgent_Deliver_WithTokenVerifier_ExpiredToken(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()

	verifier := &fakeTokenVerifier{err: errors.New("token expired")}
	cfg := domain.OIDCConfig{
		IssuerURL: "https://issuer.example.com",
		Audience:  "fleetshift",
	}

	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA(), kind.WithTokenVerifier(verifier, cfg))

	auth := domain.DeliveryAuth{
		Token: "expired-token",
		Caller: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "alice",
				Issuer:  "https://issuer.example.com",
			},
		},
		Audience: []domain.Audience{"fleetshift"},
	}

	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "rejected-cluster"}`),
	}}

	err := agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:k1", manifests, auth, nil, 1)
	if err != nil {
		t.Fatalf("Deliver should not return a dispatch error: %v", err)
	}
	result := awaitDone(t, reporter.done)
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
	reporter := newChannelReporter()

	verifier := &fakeTokenVerifier{err: errors.New("should not be called")}
	cfg := domain.OIDCConfig{
		IssuerURL: "https://issuer.example.com",
		Audience:  "fleetshift",
	}

	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA(), kind.WithTokenVerifier(verifier, cfg))

	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "no-token-cluster"}`),
	}}

	err := agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	doneResult := awaitDone(t, reporter.done)
	if doneResult.State != domain.DeliveryStateDelivered {
		t.Errorf("async State = %q, want %q", doneResult.State, domain.DeliveryStateDelivered)
	}
}

func TestAgent_Observer_MultipleSpecs(t *testing.T) {
	provider := newFakeProvider()
	reporter := newChannelReporter()

	agentObs := &recordingAgentObserver{}
	agent := kind.NewAgent(reporter, fakeFactory(provider), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA(), kind.WithObserver(agentObs))

	manifests := []domain.Manifest{
		{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name": "a"}`)},
		{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name": "b"}`)},
	}

	err := agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1:k1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	awaitDone(t, reporter.done)

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

func TestAgent_Deliver_TrustBundle_StoresAndCompletes(t *testing.T) {
	fp := newFakeProvider()
	reporter := newChannelReporter()
	agent := kind.NewAgent(reporter, fakeFactory(fp), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA())

	trustEntry := domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks",
		EnrollmentAudience: "fleetshift-enroll",
		RegistrySubjectMapping: &domain.RegistrySubjectMapping{
			RegistryID: "github.com",
			Expression: "claims.preferred_username",
		},
	}
	raw, err := json.Marshal(trustEntry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	manifests := []domain.Manifest{{
		ManifestType: domain.TrustBundleManifestType,
		Raw:          raw,
	}}

	err = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	done := awaitDone(t, reporter.done)
	if done.State != domain.DeliveryStateDelivered {
		t.Fatalf("async State = %q, want Delivered", done.State)
	}

	bundles := agent.TrustBundles()
	if len(bundles) != 1 {
		t.Fatalf("trust bundles len = %d, want 1", len(bundles))
	}
	if bundles[0].IssuerURL != "https://issuer.example.com" {
		t.Errorf("issuer = %q", bundles[0].IssuerURL)
	}
	if bundles[0].RegistrySubjectMapping == nil || bundles[0].RegistrySubjectMapping.Expression != "claims.preferred_username" {
		t.Errorf("registry subject mapping = %+v", bundles[0].RegistrySubjectMapping)
	}
}

func TestAgent_Deliver_TrustBundle_IncludedInProvisionedTarget(t *testing.T) {
	fp := newFakeProvider()
	reporter := newChannelReporter()
	agent := kind.NewAgent(reporter, fakeFactory(fp), kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA())

	trustEntry := domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks",
		EnrollmentAudience: "enroll",
	}
	trustRaw, _ := json.Marshal(trustEntry)

	// First deliver a trust bundle.
	trustManifests := []domain.Manifest{{
		ManifestType: domain.TrustBundleManifestType,
		Raw:          trustRaw,
	}}
	_ = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d-trust", trustManifests, domain.DeliveryAuth{}, nil, 1)
	awaitDone(t, reporter.done)

	// Now deliver a cluster spec. The same agent retains the trust
	// bundle in memory, and the done channel has been drained.
	spec := kind.ClusterSpec{Name: "trust-test"}
	specRaw, _ := json.Marshal(spec)
	clusterManifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          specRaw,
	}}
	err := agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d-cluster", clusterManifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	done := awaitDone(t, reporter.done)
	if done.State != domain.DeliveryStateDelivered {
		t.Fatalf("async State = %q (message: %s)", done.State, done.Message)
	}

	if len(done.ProvisionedTargets) != 1 {
		t.Fatalf("provisioned targets len = %d, want 1", len(done.ProvisionedTargets))
	}
	pt := done.ProvisionedTargets[0]
	trustJSON := pt.Properties["trust_bundle"]
	if trustJSON == "" {
		t.Fatal("provisioned target missing trust_bundle property")
	}

	var entries []domain.TrustBundleEntry
	if err := json.Unmarshal([]byte(trustJSON), &entries); err != nil {
		t.Fatalf("unmarshal trust_bundle: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("entries len = %d, want 1", len(entries))
	}
	if entries[0].IssuerURL != "https://issuer.example.com" {
		t.Errorf("issuer = %q", entries[0].IssuerURL)
	}
}

// blockingProvider wraps fakeProvider but blocks Create until unblocked.
type blockingProvider struct {
	*fakeProvider
	gate        chan struct{} // close to unblock Create
	entered     chan struct{} // closed when first Create is entered
	enteredOnce sync.Once
	createCount int32
}

func newBlockingProvider() *blockingProvider {
	return &blockingProvider{
		fakeProvider: newFakeProvider(),
		gate:         make(chan struct{}),
		entered:      make(chan struct{}),
	}
}

func (p *blockingProvider) Create(name string, opts ...cluster.CreateOption) error {
	atomic.AddInt32(&p.createCount, 1)
	if p.logger != nil {
		p.logger.V(0).Infof("Creating cluster %q", name)
	}
	p.enteredOnce.Do(func() { close(p.entered) })
	<-p.gate
	return p.fakeProvider.Create(name, opts...)
}

func TestAgent_Deliver_RetryWhileInFlight_Skipped(t *testing.T) {
	bp := newBlockingProvider()
	reporter := newChannelReporter()
	agent := kind.NewAgent(reporter, func(logger log.Logger) kind.ClusterProvider {
		bp.fakeProvider.logger = logger
		return bp
	}, kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA())

	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name": "retry-cluster"}`),
	}}

	// First deliver — enters Create and blocks.
	err := agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d-retry:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("first Deliver: %v", err)
	}
	<-bp.entered // wait until goroutine is inside Create

	// Second deliver with same delivery ID — should be a no-op.
	err = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d-retry:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("second Deliver: %v", err)
	}

	// Unblock the first goroutine.
	close(bp.gate)

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q, want %q", result.State, domain.DeliveryStateDelivered)
	}

	if n := atomic.LoadInt32(&bp.createCount); n != 1 {
		t.Fatalf("Create called %d times, want 1", n)
	}
}

func TestAgent_Remove_RetryWhileInFlight_Skipped(t *testing.T) {
	fp := newFakeProvider()
	fp.clusters["fs--rm-cluster"] = nil
	reporter := newChannelReporter()
	store := kind.NewMemoryGenerationStore()
	store.SetForTest("fs--rm-cluster", 1)

	bdp := &blockingDeleteProvider{
		fakeProvider: fp,
		gate:         make(chan struct{}),
		entered:      make(chan struct{}),
		count:        new(int32),
	}
	agent := kind.NewAgent(reporter, func(_ log.Logger) kind.ClusterProvider {
		return bdp
	}, kind.WithGenerationStore(store))

	manifests := []domain.Manifest{{
		ManifestType: kind.ClusterManifestType,
		Raw:          json.RawMessage(`{"name":"rm-cluster"}`),
	}}

	err := agent.Remove(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d-rm:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("first Remove: %v", err)
	}
	<-bdp.entered

	err = agent.Remove(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d-rm:t1", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("second Remove: %v", err)
	}

	close(bdp.gate)

	result := awaitDone(t, reporter.done)
	if result.State != domain.DeliveryStateDelivered {
		t.Fatalf("State = %q, want %q", result.State, domain.DeliveryStateDelivered)
	}

	if n := atomic.LoadInt32(bdp.count); n != 1 {
		t.Fatalf("Delete called %d times, want 1", n)
	}
}

func TestAgent_Deliver_TrustBundle_RetryDoesNotDuplicate(t *testing.T) {
	bp := newBlockingProvider()
	reporter := newChannelReporter()
	agent := kind.NewAgent(reporter, func(logger log.Logger) kind.ClusterProvider {
		bp.fakeProvider.logger = logger
		return bp
	}, kind.WithGenerationStore(kind.NewMemoryGenerationStore()), stubPlatformSA())

	trustEntry := domain.TrustBundleEntry{
		IssuerURL:          "https://issuer.example.com",
		JWKSURI:            "https://issuer.example.com/jwks",
		EnrollmentAudience: "fleetshift-enroll",
	}
	trustRaw, _ := json.Marshal(trustEntry)

	manifests := []domain.Manifest{
		{ManifestType: domain.TrustBundleManifestType, Raw: trustRaw},
		{ManifestType: kind.ClusterManifestType, Raw: json.RawMessage(`{"name": "tb-retry-cluster"}`)},
	}

	// First deliver — trust bundle is stored, cluster Create blocks.
	err := agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d-tb-retry", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("first Deliver: %v", err)
	}
	<-bp.entered

	// Retry with same delivery ID while first is in-flight.
	// The inflight gate must prevent a second storeTrustBundle call.
	err = agent.Deliver(context.Background(), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{}), "d-tb-retry", manifests, domain.DeliveryAuth{}, nil, 1)
	if err != nil {
		t.Fatalf("second Deliver: %v", err)
	}

	bundles := agent.TrustBundles()
	if len(bundles) != 1 {
		t.Fatalf("trust bundles len = %d, want 1 (retry must not duplicate)", len(bundles))
	}

	close(bp.gate)
	awaitDone(t, reporter.done)
}

// blockingDeleteProvider blocks Delete until gate is closed.
type blockingDeleteProvider struct {
	*fakeProvider
	gate    chan struct{}
	entered chan struct{}
	count   *int32
	once    sync.Once
}

func (p *blockingDeleteProvider) Delete(name, kc string) error {
	atomic.AddInt32(p.count, 1)
	p.once.Do(func() { close(p.entered) })
	<-p.gate
	return p.fakeProvider.Delete(name, kc)
}
