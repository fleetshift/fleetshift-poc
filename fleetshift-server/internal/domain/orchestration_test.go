package domain_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// ---------------------------------------------------------------------------
// Test helpers: real SQLite store + seeding
// ---------------------------------------------------------------------------

func setupStore(t *testing.T) (domain.Store, domain.Vault) {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	return &sqlite.Store{DB: db}, &sqlite.VaultStore{DB: db}
}

func seedDeployment(t *testing.T, store domain.Store, dep domain.Deployment) {
	t.Helper()
	if dep.UID == "" {
		dep.UID = "test-uid"
	}
	if dep.Etag == "" {
		dep.Etag = "test-etag"
	}
	if dep.CreatedAt.IsZero() {
		dep.CreatedAt = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	if dep.UpdatedAt.IsZero() {
		dep.UpdatedAt = dep.CreatedAt
	}
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if err := tx.Deployments().Create(context.Background(), dep); err != nil {
		t.Fatalf("seed deployment %q: %v", dep.ID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func seedTargets(t *testing.T, store domain.Store, targets ...domain.TargetInfo) {
	t.Helper()
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	for _, tgt := range targets {
		if err := tx.Targets().Create(context.Background(), tgt); err != nil {
			t.Fatalf("seed target %q: %v", tgt.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func getDeployment(t *testing.T, store domain.Store, id domain.DeploymentID) domain.Deployment {
	t.Helper()
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	dep, err := tx.Deployments().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get deployment %q: %v", id, err)
	}
	return dep
}

func getTarget(t *testing.T, store domain.Store, id domain.TargetID) domain.TargetInfo {
	t.Helper()
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	tgt, err := tx.Targets().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get target %q: %v", id, err)
	}
	return tgt
}

func getDeliveries(t *testing.T, store domain.Store, depID domain.DeploymentID) []domain.Delivery {
	t.Helper()
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	records, err := tx.Deliveries().ListByDeployment(context.Background(), depID)
	if err != nil {
		t.Fatalf("list deliveries for %q: %v", depID, err)
	}
	return records
}

func getDelivery(t *testing.T, store domain.Store, id domain.DeliveryID) domain.Delivery {
	t.Helper()
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	d, err := tx.Deliveries().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get delivery %q: %v", id, err)
	}
	return d
}

// ---------------------------------------------------------------------------
// Recording observer for intermediate state assertions
// ---------------------------------------------------------------------------

type recordingObserver struct {
	domain.NoOpDeploymentObserver
	mu       sync.Mutex
	states   []domain.DeploymentState
	filtered []filteredEvent
	outputs  []outputsEvent
}

type filteredEvent struct {
	TargetID domain.TargetID
	Total    int
	Accepted int
}

func (o *recordingObserver) RunStarted(ctx context.Context, _ domain.DeploymentID) (context.Context, domain.DeploymentRunProbe) {
	return ctx, &recordingProbe{observer: o}
}

type recordingProbe struct {
	domain.NoOpDeploymentRunProbe
	observer *recordingObserver
}

func (p *recordingProbe) StateChanged(state domain.DeploymentState) {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	p.observer.states = append(p.observer.states, state)
}

type outputsEvent struct {
	TargetIDs []domain.TargetID
	Secrets   int
}

func (p *recordingProbe) DeliveryOutputsProcessed(targets []domain.ProvisionedTarget, secrets int) {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	ids := make([]domain.TargetID, len(targets))
	for i, t := range targets {
		ids[i] = t.ID
	}
	p.observer.outputs = append(p.observer.outputs, outputsEvent{TargetIDs: ids, Secrets: secrets})
}

func (p *recordingProbe) ManifestsFiltered(target domain.TargetInfo, total, accepted int) {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	p.observer.filtered = append(p.observer.filtered, filteredEvent{
		TargetID: target.ID,
		Total:    total,
		Accepted: accepted,
	})
}

// ---------------------------------------------------------------------------
// Workflow record fakes (simulate the workflow engine, not the data layer)
// ---------------------------------------------------------------------------

// recordingRecord wraps a [domain.Record] and records activity names
// and target-related inputs so tests can assert execution sequence.
type recordingRecord struct {
	ctx      context.Context
	records  []activityRecord
	delegate domain.Record
}

type activityRecord struct {
	Name string
	// TargetID is set for remove-from-target, generate-manifests, deliver-to-target.
	TargetID domain.TargetID
}

func (r *recordingRecord) ID() string              { return r.delegate.ID() }
func (r *recordingRecord) Context() context.Context { return r.ctx }

func (r *recordingRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	name := activity.Name()
	var targetID domain.TargetID
	switch v := in.(type) {
	case domain.RemoveInput:
		targetID = v.Target.ID
	case domain.GenerateManifestsInput:
		targetID = v.Target.ID
	case domain.DeliverInput:
		targetID = v.Target.ID
	}
	r.records = append(r.records, activityRecord{Name: name, TargetID: targetID})
	return r.delegate.Run(activity, in)
}

func (r *recordingRecord) Await(signalName string) (any, error) {
	return r.delegate.Await(signalName)
}

func (r *recordingRecord) activityNames() []string {
	names := make([]string, len(r.records))
	for i, rec := range r.records {
		names[i] = rec.Name
	}
	return names
}

// singleEventRecord is a minimal Record that runs activities
// synchronously. Await delivers one scripted event and then signals
// delete. Delivery-completion events injected via the shared events
// channel are drained before the scripted event sequence.
type singleEventRecord struct {
	ctx       context.Context
	event     domain.DeploymentEvent
	delivered bool
	events    <-chan domain.DeploymentEvent
}

func (r *singleEventRecord) ID() string              { return "test-single" }
func (r *singleEventRecord) Context() context.Context { return r.ctx }
func (r *singleEventRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(r.ctx, in)
}

func (r *singleEventRecord) Await(_ string) (any, error) {
	select {
	case e := <-r.events:
		return e, nil
	default:
	}
	if !r.delivered {
		r.delivered = true
		return r.event, nil
	}
	return domain.DeploymentEvent{Delete: true}, nil
}

// asyncRecord is a Record for testing async delivery agents. Await
// blocks until a signal arrives on the events channel, then sends
// a Delete on the next call.
type asyncRecord struct {
	ctx    context.Context
	events chan domain.DeploymentEvent
	sawAll bool
}

func (r *asyncRecord) ID() string              { return "test-async" }
func (r *asyncRecord) Context() context.Context { return r.ctx }
func (r *asyncRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(r.ctx, in)
}

func (r *asyncRecord) Await(_ string) (any, error) {
	if r.sawAll {
		return domain.DeploymentEvent{Delete: true}, nil
	}
	e := <-r.events
	return e, nil
}

// ---------------------------------------------------------------------------
// Signal routing (just a channel forwarder, not a data stub)
// ---------------------------------------------------------------------------

// stubRegistry implements [domain.Registry] for domain unit tests.
// It routes SignalDeploymentEvent to a shared events channel that test
// records read from via Await.
type stubRegistry struct {
	events chan domain.DeploymentEvent
}

func (r *stubRegistry) SignalDeploymentEvent(_ context.Context, _ domain.DeploymentID, event domain.DeploymentEvent) error {
	r.events <- event
	return nil
}

func (r *stubRegistry) RegisterOrchestration(_ *domain.OrchestrationWorkflowSpec) (domain.OrchestrationWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterCreateDeployment(_ *domain.CreateDeploymentWorkflowSpec) (domain.CreateDeploymentWorkflow, error) {
	return nil, nil
}

// ---------------------------------------------------------------------------
// Delivery agent fakes (simulate delivery behaviour, not the data layer)
// ---------------------------------------------------------------------------

// noopDelivery implements DeliveryService with no-op Deliver and Remove.
// It calls signaler.Done synchronously, which is safe in domain unit
// tests where workflows execute in a single goroutine without locks.
type noopDelivery struct{}

func (noopDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	signaler.Done(ctx, result)
	return result, nil
}

func (noopDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

// asyncDelivery returns Accepted immediately and calls signaler.Done
// in a background goroutine, simulating how real delivery agents
// (e.g. kind) operate.
type asyncDelivery struct {
	done chan struct{}
}

func (a *asyncDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	go func() {
		signaler.Done(ctx, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		if a.done != nil {
			close(a.done)
		}
	}()
	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

// emittingAsyncDelivery is like [asyncDelivery] but also calls
// [domain.DeliverySignaler.Emit] before completion, simulating how the
// kind agent emits progress events during cluster creation.
type emittingAsyncDelivery struct {
	done chan struct{}
}

func (a *emittingAsyncDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	go func() {
		signaler.Emit(ctx, domain.DeliveryEvent{
			Kind:    domain.DeliveryEventProgress,
			Message: "creating cluster",
		})
		signaler.Done(ctx, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		if a.done != nil {
			close(a.done)
		}
	}()
	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

func (emittingAsyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

func (asyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

// outputProducingDelivery is a DeliveryService that produces outputs
// in the delivery result (provisioned targets and secrets).
type outputProducingDelivery struct {
	targets []domain.ProvisionedTarget
	secrets []domain.ProducedSecret
}

func (d *outputProducingDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	result := domain.DeliveryResult{
		State:              domain.DeliveryStateDelivered,
		ProvisionedTargets: d.targets,
		ProducedSecrets:    d.secrets,
	}
	signaler.Done(ctx, result)
	return result, nil
}

func (d *outputProducingDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestOrchestration_RemoveStepsRunBeforeDeliverSteps(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("d1")
	seedDeployment(t, store, domain.Deployment{
		ID:              deploymentID,
		ResolvedTargets: []domain.TargetID{"old1"},
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"new1", "new2"},
		},
		State: domain.DeploymentStateCreating,
	})

	pool := []domain.TargetInfo{
		{ID: "old1", Name: "old1", Type: "test"},
		{ID: "new1", Name: "new1", Type: "test"},
		{ID: "new2", Name: "new2", Type: "test"},
	}
	seedTargets(t, store, pool...)

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	rec := &singleEventRecord{
		ctx:    context.Background(),
		event:  domain.DeploymentEvent{PoolChange: &domain.PoolChange{Set: pool}},
		events: events,
	}

	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   noopDelivery{},
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
	}

	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var removeOld1At, generateNew1At int
	removeOld1At = -1
	generateNew1At = -1
	for i, rec := range recorder.records {
		if rec.Name == "remove-from-target" && rec.TargetID == "old1" {
			removeOld1At = i
			break
		}
	}
	for i, rec := range recorder.records {
		if rec.Name == "generate-manifests" && rec.TargetID == "new1" {
			generateNew1At = i
			break
		}
	}
	if removeOld1At < 0 {
		t.Fatal("remove-from-target for old1 never recorded")
	}
	if generateNew1At < 0 {
		t.Fatal("generate-manifests for new1 never recorded")
	}
	if removeOld1At >= generateNew1At {
		t.Errorf("removals must run before delivery: remove(old1) at %d, generate(new1) at %d",
			removeOld1At, generateNew1At)
	}
}

func TestOrchestration_PlacementAndRolloutRunAsActivities(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("d1")
	seedDeployment(t, store, domain.Deployment{
		ID:                deploymentID,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	rec := &singleEventRecord{
		ctx:    context.Background(),
		event:  domain.DeploymentEvent{PoolChange: &domain.PoolChange{Set: []domain.TargetInfo{{ID: "t1", Name: "t1", Type: "test"}}}},
		events: events,
	}

	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   noopDelivery{},
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
	}

	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	names := recorder.activityNames()
	hasResolvePlacement := false
	hasPlanRollout := false
	for _, n := range names {
		if n == "resolve-placement" {
			hasResolvePlacement = true
		}
		if n == "plan-rollout" {
			hasPlanRollout = true
		}
	}
	if !hasResolvePlacement {
		t.Errorf("workflow must invoke resolve-placement activity; got names: %v", names)
	}
	if !hasPlanRollout {
		t.Errorf("workflow must invoke plan-rollout activity; got names: %v", names)
	}
}

func TestOrchestration_EmptyPool_FailsDeployment(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("empty-pool")
	seedDeployment(t, store, domain.Deployment{
		ID: deploymentID,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
		State: domain.DeploymentStateCreating,
	})

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	rec := &singleEventRecord{
		ctx:    context.Background(),
		events: events,
	}

	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   noopDelivery{},
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
	}

	_, err := wf.Run(rec, deploymentID)
	if err == nil {
		t.Fatal("expected error from empty pool, got nil")
	}
	if !strings.Contains(err.Error(), "zero targets") {
		t.Errorf("error should mention zero targets, got: %v", err)
	}

	dep := getDeployment(t, store, deploymentID)
	if dep.State != domain.DeploymentStateFailed {
		t.Errorf("deployment state = %q, want %q", dep.State, domain.DeploymentStateFailed)
	}
}

func TestOrchestration_DeliveryOutputs_RegistersTargetAndStoresSecret(t *testing.T) {
	store, vault := setupStore(t)

	deploymentID := domain.DeploymentID("output-test")
	seedDeployment(t, store, domain.Deployment{
		ID: deploymentID,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"kind-local"},
		},
		State: domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "kind-local", Type: "kind", Name: "Local Kind"})

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	rec := &singleEventRecord{
		ctx:    context.Background(),
		event:  domain.DeploymentEvent{Delete: true},
		events: events,
	}

	kubeconfig := []byte("apiVersion: v1\nkind: Config")
	delivery := &outputProducingDelivery{
		targets: []domain.ProvisionedTarget{{
			ID:   "k8s-test-cluster",
			Type: "kubernetes",
			Name: "test-cluster",
			Properties: map[string]string{
				"kubeconfig_ref": "targets/k8s-test-cluster/kubeconfig",
			},
		}},
		secrets: []domain.ProducedSecret{{
			Ref:   "targets/k8s-test-cluster/kubeconfig",
			Value: kubeconfig,
		}},
	}

	obs := &recordingObserver{}
	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   delivery,
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
		Vault:      vault,
		Observer:   obs,
	}

	_, err := wf.Run(rec, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := vault.Get(context.Background(), "targets/k8s-test-cluster/kubeconfig")
	if err != nil {
		t.Fatalf("vault.Get: %v", err)
	}
	if string(got) != string(kubeconfig) {
		t.Errorf("vault value = %q, want %q", got, kubeconfig)
	}

	_ = getTarget(t, store, "k8s-test-cluster")

	obs.mu.Lock()
	defer obs.mu.Unlock()
	if len(obs.outputs) != 1 {
		t.Fatalf("expected 1 outputs event, got %d", len(obs.outputs))
	}
	if len(obs.outputs[0].TargetIDs) != 1 || obs.outputs[0].TargetIDs[0] != "k8s-test-cluster" {
		t.Errorf("outputs target IDs = %v, want [k8s-test-cluster]", obs.outputs[0].TargetIDs)
	}
	if obs.outputs[0].Secrets != 1 {
		t.Errorf("outputs secrets = %d, want 1", obs.outputs[0].Secrets)
	}
}

func TestOrchestration_AsyncDelivery_ReachesActive(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("async-test")
	seedDeployment(t, store, domain.Deployment{
		ID: deploymentID,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1"},
		},
		State: domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	asyncDel := &asyncDelivery{done: make(chan struct{})}

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	rec := &asyncRecord{
		ctx:    context.Background(),
		events: events,
	}

	obs := &recordingObserver{}

	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   asyncDel,
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
		Observer:   obs,
	}

	go func() {
		<-asyncDel.done
		events <- domain.DeploymentEvent{Delete: true}
	}()

	_, err := wf.Run(rec, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	sawActive := false
	for _, s := range obs.states {
		if s == domain.DeploymentStateActive {
			sawActive = true
		}
	}
	if !sawActive {
		states := make([]string, len(obs.states))
		for i, s := range obs.states {
			states[i] = string(s)
		}
		t.Fatalf("workflow never reached Active; state transitions: %v", states)
	}

	dep := getDeployment(t, store, deploymentID)
	if dep.State != domain.DeploymentStateDeleting {
		t.Errorf("final deployment state = %q, want %q", dep.State, domain.DeploymentStateDeleting)
	}

	d := getDelivery(t, store, "async-test:t1")
	if d.State != domain.DeliveryStateDelivered {
		t.Errorf("delivery state = %q, want %q", d.State, domain.DeliveryStateDelivered)
	}
}

func TestOrchestration_ResourceTypeFiltering_SkipsIncompatibleTargets(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("rt-filter")
	seedDeployment(t, store, domain.Deployment{
		ID: deploymentID,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{
				{ResourceType: "api.kind.cluster", Raw: json.RawMessage(`{"name":"c1"}`)},
			},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
		State: domain.DeploymentStateCreating,
	})

	pool := []domain.TargetInfo{
		{ID: "kind-local", Name: "Kind Provider", Type: "kind", AcceptedResourceTypes: []domain.ResourceType{"api.kind.cluster"}},
		{ID: "k8s-existing", Name: "Existing K8s", Type: "kubernetes", AcceptedResourceTypes: []domain.ResourceType{"kubernetes"}},
	}
	seedTargets(t, store, pool...)

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	rec := &singleEventRecord{
		ctx:    context.Background(),
		event:  domain.DeploymentEvent{Delete: true},
		events: events,
	}

	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   noopDelivery{},
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
	}

	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var deliveredTo []domain.TargetID
	for _, rec := range recorder.records {
		if rec.Name == "deliver-to-target" {
			deliveredTo = append(deliveredTo, rec.TargetID)
		}
	}
	if len(deliveredTo) != 1 {
		t.Fatalf("expected delivery to 1 target, got %d: %v", len(deliveredTo), deliveredTo)
	}
	if deliveredTo[0] != "kind-local" {
		t.Errorf("expected delivery to kind-local, got %s", deliveredTo[0])
	}
}

func TestOrchestration_ResourceTypeFiltering_ObserverReportsFiltering(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("rt-obs")
	seedDeployment(t, store, domain.Deployment{
		ID: deploymentID,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{
				{ResourceType: "api.kind.cluster", Raw: json.RawMessage(`{"name":"c1"}`)},
			},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
		State: domain.DeploymentStateCreating,
	})

	pool := []domain.TargetInfo{
		{ID: "kind-local", Name: "Kind Provider", Type: "kind", AcceptedResourceTypes: []domain.ResourceType{"api.kind.cluster"}},
		{ID: "k8s-existing", Name: "Existing K8s", Type: "kubernetes", AcceptedResourceTypes: []domain.ResourceType{"kubernetes"}},
	}
	seedTargets(t, store, pool...)

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	rec := &singleEventRecord{
		ctx:    context.Background(),
		event:  domain.DeploymentEvent{Delete: true},
		events: events,
	}

	obs := &recordingObserver{}
	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   noopDelivery{},
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
		Observer:   obs,
	}

	_, err := wf.Run(rec, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()

	if len(obs.filtered) != 2 {
		t.Fatalf("expected 2 filtered events (one per target), got %d", len(obs.filtered))
	}

	byTarget := make(map[domain.TargetID]filteredEvent)
	for _, f := range obs.filtered {
		byTarget[f.TargetID] = f
	}

	kindEvt := byTarget["kind-local"]
	if kindEvt.Total != 1 || kindEvt.Accepted != 1 {
		t.Errorf("kind-local: total=%d accepted=%d, want 1/1", kindEvt.Total, kindEvt.Accepted)
	}

	k8sEvt := byTarget["k8s-existing"]
	if k8sEvt.Total != 1 || k8sEvt.Accepted != 0 {
		t.Errorf("k8s-existing: total=%d accepted=%d, want 1/0", k8sEvt.Total, k8sEvt.Accepted)
	}
}

func TestOrchestration_ResourceTypeFiltering_UnconstrainedTargetReceivesAll(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("rt-unconstrained")
	seedDeployment(t, store, domain.Deployment{
		ID: deploymentID,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{
				{ResourceType: "api.kind.cluster", Raw: json.RawMessage(`{"name":"c1"}`)},
			},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
		State: domain.DeploymentStateCreating,
	})

	pool := []domain.TargetInfo{
		{ID: "constrained", Name: "K8s Only", Type: "kubernetes", AcceptedResourceTypes: []domain.ResourceType{"kubernetes"}},
		{ID: "unconstrained", Name: "Legacy Target", Type: "test"},
	}
	seedTargets(t, store, pool...)

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	rec := &singleEventRecord{
		ctx:    context.Background(),
		event:  domain.DeploymentEvent{Delete: true},
		events: events,
	}

	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   noopDelivery{},
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
	}

	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var deliveredTo []domain.TargetID
	for _, rec := range recorder.records {
		if rec.Name == "deliver-to-target" {
			deliveredTo = append(deliveredTo, rec.TargetID)
		}
	}
	if len(deliveredTo) != 1 {
		t.Fatalf("expected delivery to 1 target, got %d: %v", len(deliveredTo), deliveredTo)
	}
	if deliveredTo[0] != "unconstrained" {
		t.Errorf("expected delivery to unconstrained, got %s", deliveredTo[0])
	}
}

// authFailingDelivery simulates a delivery agent that reports an
// authentication failure (e.g. 401 Unauthorized from the target API).
type authFailingDelivery struct{}

func (authFailingDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	result := domain.DeliveryResult{
		State:   domain.DeliveryStateAuthFailed,
		Message: "401 Unauthorized",
	}
	signaler.Done(ctx, result)
	return result, nil
}

func (authFailingDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

// authFailThenSucceedDelivery fails the first delivery with
// DeliveryStateAuthFailed, then succeeds on all subsequent deliveries.
// This simulates the resume scenario.
type authFailThenSucceedDelivery struct {
	mu      sync.Mutex
	attempt int
}

func (d *authFailThenSucceedDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	d.mu.Lock()
	d.attempt++
	n := d.attempt
	d.mu.Unlock()

	if n == 1 {
		result := domain.DeliveryResult{
			State:   domain.DeliveryStateAuthFailed,
			Message: "401 Unauthorized",
		}
		signaler.Done(ctx, result)
		return result, nil
	}
	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	signaler.Done(ctx, result)
	return result, nil
}

func (d *authFailThenSucceedDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

// recordingDelivery records the manifests delivered to each target, allowing
// tests to assert per-target manifest filtering.
type recordingDelivery struct {
	mu        sync.Mutex
	delivered map[domain.TargetID][]domain.Manifest
}

func (d *recordingDelivery) Deliver(ctx context.Context, target domain.TargetInfo, _ domain.DeliveryID, manifests []domain.Manifest, _ domain.DeliveryAuth, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	d.mu.Lock()
	if d.delivered == nil {
		d.delivered = make(map[domain.TargetID][]domain.Manifest)
	}
	d.delivered[target.ID] = append(d.delivered[target.ID], manifests...)
	d.mu.Unlock()
	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	signaler.Done(ctx, result)
	return result, nil
}

func (d *recordingDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

func TestOrchestration_ResourceTypeFiltering_MixedManifestsFilteredPerTarget(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("rt-mixed")
	seedDeployment(t, store, domain.Deployment{
		ID: deploymentID,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{
				{ResourceType: "api.kind.cluster", Raw: json.RawMessage(`{"name":"c1"}`)},
				{ResourceType: "kubernetes", Raw: json.RawMessage(`{"kind":"ConfigMap"}`)},
			},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type: domain.PlacementStrategyAll,
		},
		State: domain.DeploymentStateCreating,
	})

	pool := []domain.TargetInfo{
		{ID: "kind-target", Name: "Kind", Type: "kind", AcceptedResourceTypes: []domain.ResourceType{"api.kind.cluster"}},
		{ID: "k8s-target", Name: "K8s", Type: "kubernetes", AcceptedResourceTypes: []domain.ResourceType{"kubernetes"}},
	}
	seedTargets(t, store, pool...)

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	rec := &singleEventRecord{
		ctx:    context.Background(),
		event:  domain.DeploymentEvent{Delete: true},
		events: events,
	}

	delivery := &recordingDelivery{}
	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   delivery,
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
	}

	_, err := wf.Run(rec, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	kindManifests := delivery.delivered["kind-target"]
	if len(kindManifests) != 1 {
		t.Fatalf("kind-target: expected 1 manifest, got %d", len(kindManifests))
	}
	if kindManifests[0].ResourceType != "api.kind.cluster" {
		t.Errorf("kind-target: expected api.kind.cluster, got %s", kindManifests[0].ResourceType)
	}

	k8sManifests := delivery.delivered["k8s-target"]
	if len(k8sManifests) != 1 {
		t.Fatalf("k8s-target: expected 1 manifest, got %d", len(k8sManifests))
	}
	if k8sManifests[0].ResourceType != "kubernetes" {
		t.Errorf("k8s-target: expected kubernetes, got %s", k8sManifests[0].ResourceType)
	}
}

func TestOrchestration_AsyncDelivery_DeliveryObserverReceivesEvents(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("obs-test")
	seedDeployment(t, store, domain.Deployment{
		ID: deploymentID,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1"},
		},
		State: domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	emitting := &emittingAsyncDelivery{done: make(chan struct{})}
	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	rec := &asyncRecord{
		ctx:    context.Background(),
		events: events,
	}

	delObs := &recordingDeliveryObserver{}
	wf := &domain.OrchestrationWorkflowSpec{
		Store:            store,
		Delivery:         emitting,
		Strategies:       domain.DefaultStrategyFactory{},
		Registry:         reg,
		DeliveryObserver: delObs,
	}

	go func() {
		<-emitting.done
		events <- domain.DeploymentEvent{Delete: true}
	}()

	_, err := wf.Run(rec, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	emittedEvents, completions := delObs.snapshot()
	if len(emittedEvents) == 0 {
		t.Fatal("delivery observer received no progress events; Emit → observer chain is broken")
	}
	if emittedEvents[0].Kind != domain.DeliveryEventProgress {
		t.Errorf("event kind = %q, want %q", emittedEvents[0].Kind, domain.DeliveryEventProgress)
	}
	if emittedEvents[0].Message != "creating cluster" {
		t.Errorf("event message = %q, want %q", emittedEvents[0].Message, "creating cluster")
	}
	if len(completions) == 0 {
		t.Fatal("delivery observer received no completion results; Done → observer chain is broken")
	}
	if completions[0].State != domain.DeliveryStateDelivered {
		t.Errorf("completion state = %q, want %q", completions[0].State, domain.DeliveryStateDelivered)
	}
}

// pausedAuthRecord is a Record for testing the PausedAuth flow. It
// drains delivery-completion events from the events channel (placed
// there by signaler.Done via stubRegistry) and interleaves scripted
// events (AuthResumed, Delete) at the right points.
type pausedAuthRecord struct {
	ctx        context.Context
	events     chan domain.DeploymentEvent
	scripted   []domain.DeploymentEvent
	scriptIdx  int
}

func (r *pausedAuthRecord) ID() string              { return "test-paused-auth" }
func (r *pausedAuthRecord) Context() context.Context { return r.ctx }
func (r *pausedAuthRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(r.ctx, in)
}

func (r *pausedAuthRecord) Await(_ string) (any, error) {
	select {
	case e := <-r.events:
		return e, nil
	default:
	}
	if r.scriptIdx < len(r.scripted) {
		e := r.scripted[r.scriptIdx]
		r.scriptIdx++
		return e, nil
	}
	return domain.DeploymentEvent{Delete: true}, nil
}

func TestOrchestration_AuthFailed_TransitionsToPausedAuth_ThenResumes(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("auth-pause-test")
	seedDeployment(t, store, domain.Deployment{
		ID: deploymentID,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1"},
		},
		State: domain.DeploymentStateCreating,
		Auth: domain.DeliveryAuth{
			Token: "expired-token",
		},
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	delivery := &authFailThenSucceedDelivery{}
	obs := &recordingObserver{}

	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   delivery,
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
		Observer:   obs,
	}

	freshAuth := domain.DeliveryAuth{
		Token: "fresh-token",
		Caller: &domain.SubjectClaims{
			ID:     "alice",
			Issuer: "https://issuer.example.com",
		},
	}

	rec := &pausedAuthRecord{
		ctx:    context.Background(),
		events: events,
		scripted: []domain.DeploymentEvent{
			{AuthResumed: &domain.AuthResumedEvent{Auth: freshAuth}},
			{Delete: true},
		},
	}

	_, err := wf.Run(rec, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()

	sawPausedAuth := false
	sawActive := false
	for _, s := range obs.states {
		if s == domain.DeploymentStatePausedAuth {
			sawPausedAuth = true
		}
		if s == domain.DeploymentStateActive {
			sawActive = true
		}
	}
	if !sawPausedAuth {
		t.Errorf("workflow never reached PausedAuth; state transitions: %v", obs.states)
	}
	if !sawActive {
		t.Errorf("workflow never reached Active after resume; state transitions: %v", obs.states)
	}
}

func TestOrchestration_AuthFailed_DeleteWhilePaused(t *testing.T) {
	store, _ := setupStore(t)

	deploymentID := domain.DeploymentID("auth-delete-test")
	seedDeployment(t, store, domain.Deployment{
		ID: deploymentID,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1"},
		},
		State: domain.DeploymentStateCreating,
		Auth: domain.DeliveryAuth{
			Token: "expired-token",
		},
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	reg := &stubRegistry{events: events}

	obs := &recordingObserver{}
	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   authFailingDelivery{},
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
		Observer:   obs,
	}

	rec := &pausedAuthRecord{
		ctx:    context.Background(),
		events: events,
		scripted: []domain.DeploymentEvent{
			{Delete: true},
		},
	}

	_, err := wf.Run(rec, deploymentID)
	if err == nil {
		t.Fatal("expected error from delete while paused, got nil")
	}
	if !strings.Contains(err.Error(), "deleted while paused") {
		t.Errorf("error should mention 'deleted while paused', got: %v", err)
	}

	obs.mu.Lock()
	defer obs.mu.Unlock()

	sawPausedAuth := false
	for _, s := range obs.states {
		if s == domain.DeploymentStatePausedAuth {
			sawPausedAuth = true
		}
	}
	if !sawPausedAuth {
		t.Errorf("workflow never reached PausedAuth; state transitions: %v", obs.states)
	}
}
