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

func seedKeyBinding(t *testing.T, store domain.Store, kb domain.SigningKeyBinding) {
	t.Helper()
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if err := tx.SigningKeyBindings().Create(context.Background(), kb); err != nil {
		t.Fatalf("seed key binding: %v", err)
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
	tx, err := store.BeginReadOnly(context.Background())
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
	tx, err := store.BeginReadOnly(context.Background())
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
	tx, err := store.BeginReadOnly(context.Background())
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
	tx, err := store.BeginReadOnly(context.Background())
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
// Workflow record fakes
// ---------------------------------------------------------------------------

// recordingRecord wraps a [domain.Record] and records activity names
// and target-related inputs so tests can assert execution sequence.
type recordingRecord struct {
	ctx      context.Context
	records  []activityRecord
	delegate domain.Record
}

type activityRecord struct {
	Name     string
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

// simpleRecord runs activities synchronously and delivers delivery
// completion events from the events channel. Used by most tests.
type simpleRecord struct {
	ctx    context.Context
	events <-chan domain.DeploymentEvent
}

func (r *simpleRecord) ID() string              { return "test-simple" }
func (r *simpleRecord) Context() context.Context { return r.ctx }
func (r *simpleRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(r.ctx, in)
}
func (r *simpleRecord) Await(_ string) (any, error) {
	e := <-r.events
	return e, nil
}

// ---------------------------------------------------------------------------
// Signal routing
// ---------------------------------------------------------------------------

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
// Delivery agent fakes
// ---------------------------------------------------------------------------

type noopDelivery struct{}

func (noopDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	signaler.Done(ctx, result)
	return result, nil
}

func (noopDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.DeliverySignaler) error {
	return nil
}

type asyncDelivery struct {
	done chan struct{}
}

func (a *asyncDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	go func() {
		signaler.Done(ctx, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		if a.done != nil {
			close(a.done)
		}
	}()
	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

func (asyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.DeliverySignaler) error {
	return nil
}

type emittingAsyncDelivery struct {
	done chan struct{}
}

func (a *emittingAsyncDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
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

func (emittingAsyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.DeliverySignaler) error {
	return nil
}

type outputProducingDelivery struct {
	targets []domain.ProvisionedTarget
	secrets []domain.ProducedSecret
}

func (d *outputProducingDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	result := domain.DeliveryResult{
		State:              domain.DeliveryStateDelivered,
		ProvisionedTargets: d.targets,
		ProducedSecrets:    d.secrets,
	}
	signaler.Done(ctx, result)
	return result, nil
}

func (d *outputProducingDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.DeliverySignaler) error {
	return nil
}

type authFailingDelivery struct{}

func (authFailingDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	result := domain.DeliveryResult{
		State:   domain.DeliveryStateAuthFailed,
		Message: "401 Unauthorized",
	}
	signaler.Done(ctx, result)
	return result, nil
}

func (authFailingDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.DeliverySignaler) error {
	return nil
}

type recordingDelivery struct {
	mu        sync.Mutex
	delivered []domain.TargetID
}

func (d *recordingDelivery) Deliver(ctx context.Context, target domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	d.mu.Lock()
	d.delivered = append(d.delivered, target.ID)
	d.mu.Unlock()
	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	signaler.Done(ctx, result)
	return result, nil
}

func (d *recordingDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.DeliverySignaler) error {
	return nil
}

// ---------------------------------------------------------------------------
// Helper to build a standard workflow spec for tests
// ---------------------------------------------------------------------------

func newTestWorkflow(store domain.Store, delivery domain.DeliveryService, events chan domain.DeploymentEvent, opts ...func(*domain.OrchestrationWorkflowSpec)) *domain.OrchestrationWorkflowSpec {
	reg := &stubRegistry{events: events}
	wf := &domain.OrchestrationWorkflowSpec{
		Store:      store,
		Delivery:   delivery,
		Strategies: domain.DefaultStrategyFactory{},
		Registry:   reg,
	}
	for _, opt := range opts {
		opt(wf)
	}
	return wf
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestOrchestration_BasicPipeline_ReachesActive(t *testing.T) {
	store, _ := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:                "d1",
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"}, domain.TargetInfo{ID: "t2", Name: "t2", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, "d1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getDeployment(t, store, "d1")
	if dep.State != domain.DeploymentStateActive {
		t.Errorf("State = %q, want active", dep.State)
	}
	if dep.ObservedGeneration != 1 {
		t.Errorf("ObservedGeneration = %d, want 1", dep.ObservedGeneration)
	}

	deliveries := getDeliveries(t, store, "d1")
	if len(deliveries) != 2 {
		t.Fatalf("expected 2 deliveries, got %d", len(deliveries))
	}
}

func TestOrchestration_RemoveStepsRunBeforeDeliverSteps(t *testing.T) {
	store, _ := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:              "d1",
		Generation:      1,
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
	seedTargets(t, store,
		domain.TargetInfo{ID: "old1", Name: "old1", Type: "test"},
		domain.TargetInfo{ID: "new1", Name: "new1", Type: "test"},
		domain.TargetInfo{ID: "new2", Name: "new2", Type: "test"},
	)

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, "d1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var removeOld1At, generateNew1At int = -1, -1
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
	seedDeployment(t, store, domain.Deployment{
		ID:                "d1",
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, "d1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	names := recorder.activityNames()
	if !contains(names, "resolve-placement") {
		t.Error("resolve-placement not recorded as activity")
	}
	if !contains(names, "plan-rollout") {
		t.Error("plan-rollout not recorded as activity")
	}
}

func TestOrchestration_EmptyPool_FailsDeployment(t *testing.T) {
	store, _ := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:                "d1",
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategySelector, TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}}},
		State:             domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test", Labels: map[string]string{"env": "dev"}})

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, "d1")
	if err == nil {
		t.Fatal("expected error for empty pool")
	}
	if !strings.Contains(err.Error(), "zero targets") {
		t.Errorf("error = %q, want 'zero targets'", err.Error())
	}

	dep := getDeployment(t, store, "d1")
	if dep.State != domain.DeploymentStateFailed {
		t.Errorf("State = %q, want failed", dep.State)
	}
}

func TestOrchestration_DeliveryOutputs_RegistersTargetAndStoresSecret(t *testing.T) {
	store, vault := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:                "d1",
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"name":"new-cluster"}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"provisioner"}},
		State:             domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "provisioner", Name: "provisioner", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, &outputProducingDelivery{
		targets: []domain.ProvisionedTarget{{
			ID: "k8s-new-cluster", Type: "kubernetes", Name: "new-cluster",
			Properties: map[string]string{"kubeconfig_ref": "targets/k8s-new-cluster/kubeconfig"},
		}},
		secrets: []domain.ProducedSecret{{
			Ref: "targets/k8s-new-cluster/kubeconfig", Value: []byte("fake-kubeconfig-data"),
		}},
	}, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.Vault = vault
	})

	rec := &simpleRecord{ctx: context.Background(), events: events}
	obs := &recordingObserver{}
	wf.Observer = obs

	_, err := wf.Run(rec, "d1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tgt := getTarget(t, store, "k8s-new-cluster")
	if tgt.Type != "kubernetes" {
		t.Errorf("target type = %q, want kubernetes", tgt.Type)
	}

	if vault != nil {
		secret, err := vault.Get(context.Background(), "targets/k8s-new-cluster/kubeconfig")
		if err != nil {
			t.Fatalf("vault get: %v", err)
		}
		if string(secret) != "fake-kubeconfig-data" {
			t.Errorf("secret = %q, want fake-kubeconfig-data", secret)
		}
	}

	if len(obs.outputs) != 1 {
		t.Fatalf("expected 1 outputs event, got %d", len(obs.outputs))
	}
}

func TestOrchestration_AsyncDelivery_ReachesActive(t *testing.T) {
	store, _ := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:                "d1",
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, &asyncDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, "d1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getDeployment(t, store, "d1")
	if dep.State != domain.DeploymentStateActive {
		t.Errorf("State = %q, want active", dep.State)
	}
}

func TestOrchestration_AsyncDelivery_DeliveryObserverReceivesEvents(t *testing.T) {
	store, _ := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:                "d1",
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	deliveryObs := &recordingDeliveryObserver{}
	wf := newTestWorkflow(store, &emittingAsyncDelivery{}, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.DeliveryObserver = deliveryObs
	})

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, "d1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	events2, _ := deliveryObs.snapshot()
	if len(events2) == 0 {
		t.Error("expected at least one delivery event")
	}
}

func TestOrchestration_AuthFailure_SetsPausedAuth(t *testing.T) {
	store, _ := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:                "d1",
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, authFailingDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, "d1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getDeployment(t, store, "d1")
	if dep.State != domain.DeploymentStatePausedAuth {
		t.Errorf("State = %q, want paused_auth", dep.State)
	}
}

func TestOrchestration_DeletePipeline_RemovesFromTargets(t *testing.T) {
	store, _ := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:                "d1",
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.DeploymentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"},
		domain.TargetInfo{ID: "t2", Name: "t2", Type: "test"},
	)

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, "d1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	names := recorder.activityNames()
	removeCount := 0
	for _, name := range names {
		if name == "remove-from-target" {
			removeCount++
		}
	}
	if removeCount != 2 {
		t.Errorf("expected 2 remove-from-target, got %d (activities: %v)", removeCount, names)
	}

	dep := getDeployment(t, store, "d1")
	if dep.State != domain.DeploymentStateDeleting {
		t.Errorf("State = %q, want deleting", dep.State)
	}
	if len(dep.ResolvedTargets) != 0 {
		t.Errorf("ResolvedTargets = %v, want empty", dep.ResolvedTargets)
	}
}

func TestOrchestration_CompleteReconciliation_LoopsOnNewGeneration(t *testing.T) {
	store, _ := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:                "d1",
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	// Intercepting record bumps generation after the first load,
	// simulating a concurrent external mutation. The workflow should
	// loop: first iteration reconciles gen 1 and sees gen 3 has
	// arrived, second iteration reconciles gen 3 and exits.
	rec := &simpleRecord{ctx: context.Background(), events: events}
	interceptor := &afterLoadBumpGenRecord{
		delegate: rec,
		store:    store,
		depID:    "d1",
		bumps:    2,
	}

	_, err := wf.Run(interceptor, "d1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getDeployment(t, store, "d1")
	if dep.ObservedGeneration != 3 {
		t.Errorf("ObservedGeneration = %d, want 3 (loop should reconcile up to bumped generation)", dep.ObservedGeneration)
	}
}

// afterLoadBumpGenRecord wraps a record and bumps the deployment's
// generation after the load-deployment-and-pool activity runs. This
// simulates a concurrent mutation arriving mid-workflow.
type afterLoadBumpGenRecord struct {
	delegate domain.Record
	store    domain.Store
	depID    domain.DeploymentID
	bumps    int
	loaded   bool
}

func (r *afterLoadBumpGenRecord) ID() string              { return r.delegate.ID() }
func (r *afterLoadBumpGenRecord) Context() context.Context { return r.delegate.Context() }
func (r *afterLoadBumpGenRecord) Await(sig string) (any, error) { return r.delegate.Await(sig) }

func (r *afterLoadBumpGenRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	out, err := r.delegate.Run(activity, in)
	if err != nil {
		return out, err
	}
	if !r.loaded && activity.Name() == "load-deployment-and-pool" {
		r.loaded = true
		tx, txErr := r.store.Begin(context.Background())
		if txErr != nil {
			return out, txErr
		}
		dep, txErr := tx.Deployments().Get(context.Background(), r.depID)
		if txErr != nil {
			tx.Rollback()
			return out, txErr
		}
		for i := 0; i < r.bumps; i++ {
			dep.BumpGeneration()
		}
		if txErr = tx.Deployments().Update(context.Background(), dep); txErr != nil {
			tx.Rollback()
			return out, txErr
		}
		tx.Commit()
	}
	return out, nil
}

func TestOrchestration_ResourceTypeFiltering(t *testing.T) {
	store, _ := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:         "d1",
		Generation: 1,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{
				{Raw: json.RawMessage(`{}`), ResourceType: "kubernetes.manifest"},
			},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"k8s", "plain"},
		},
		State: domain.DeploymentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "k8s", Name: "k8s", Type: "kubernetes", AcceptedResourceTypes: []domain.ResourceType{"kubernetes.manifest"}},
		domain.TargetInfo{ID: "plain", Name: "plain", Type: "test"},
	)

	events := make(chan domain.DeploymentEvent, 16)
	obs := &recordingObserver{}
	rd := &recordingDelivery{}
	wf := newTestWorkflow(store, rd, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.Observer = obs
	})

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, "d1")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(obs.filtered) < 2 {
		t.Fatalf("expected 2 filter events, got %d", len(obs.filtered))
	}

	rd.mu.Lock()
	deliveredTo := rd.delivered
	rd.mu.Unlock()

	if len(deliveredTo) != 2 {
		t.Errorf("expected 2 deliveries, got %d: %v", len(deliveredTo), deliveredTo)
	}
}

// ---------------------------------------------------------------------------
// Attestation assembly
// ---------------------------------------------------------------------------

// attestationCapturingRecord wraps a Record and captures the full
// DeliverInput and RemoveInput passed to delivery/removal activities.
type attestationCapturingRecord struct {
	delegate domain.Record
	mu       sync.Mutex
	delivers []domain.DeliverInput
	removes  []domain.RemoveInput
}

func (r *attestationCapturingRecord) ID() string              { return r.delegate.ID() }
func (r *attestationCapturingRecord) Context() context.Context { return r.delegate.Context() }
func (r *attestationCapturingRecord) Await(sig string) (any, error) {
	return r.delegate.Await(sig)
}

func (r *attestationCapturingRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	switch v := in.(type) {
	case domain.DeliverInput:
		r.mu.Lock()
		r.delivers = append(r.delivers, v)
		r.mu.Unlock()
	case domain.RemoveInput:
		r.mu.Lock()
		r.removes = append(r.removes, v)
		r.mu.Unlock()
	}
	return r.delegate.Run(activity, in)
}

func testProvenance() *domain.Provenance {
	return &domain.Provenance{
		Sig: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "test-signer", Issuer: "https://issuer.example.com"},
			PublicKey:      []byte("pub-key-bytes"),
			ContentHash:    []byte("content-hash"),
			SignatureBytes: []byte("sig-bytes"),
		},
		ValidUntil:         time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpectedGeneration: 1,
	}
}

func testKeyBinding(id domain.SigningKeyBindingID, subjectID domain.SubjectID) domain.SigningKeyBinding {
	return domain.SigningKeyBinding{
		ID: id,
		FederatedIdentity: domain.FederatedIdentity{
			Subject: subjectID,
			Issuer:  "https://issuer.example.com",
		},
		PublicKeyJWK:        json.RawMessage(`{"kty":"EC","crv":"P-256","x":"x","y":"y"}`),
		Algorithm:           "ES256",
		KeyBindingDoc:       []byte("test-binding-doc"),
		KeyBindingSignature: []byte("test-binding-sig"),
		IdentityToken:       "test-id-token",
		CreatedAt:           time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:           time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestOrchestration_DeliverWithProvenance_AssemblesAttestation(t *testing.T) {
	store, _ := setupStore(t)
	prov := testProvenance()
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{ResourceType: "test.resource", Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}

	seedKeyBinding(t, store, testKeyBinding("kb-test", "test-signer"))
	seedDeployment(t, store, domain.Deployment{
		ID:                "attested-dep",
		Generation:        1,
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Auth: domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "test-signer"}},
		},
		Provenance: prov,
		State:      domain.DeploymentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	simple := &simpleRecord{ctx: context.Background(), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, "attested-dep")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	capRec.mu.Lock()
	delivers := capRec.delivers
	capRec.mu.Unlock()

	if len(delivers) != 1 {
		t.Fatalf("expected 1 deliver input, got %d", len(delivers))
	}

	att := delivers[0].Attestation
	if att == nil {
		t.Fatal("Attestation is nil; expected it to be assembled from Provenance")
	}

	if att.Input.Content.DeploymentID != "attested-dep" {
		t.Errorf("Input.Content.DeploymentID = %q, want %q", att.Input.Content.DeploymentID, "attested-dep")
	}
	if att.Input.Sig.Signer != prov.Sig.Signer {
		t.Errorf("Input.Sig.Signer = %v, want %v", att.Input.Sig.Signer, prov.Sig.Signer)
	}
	if att.Input.KeyBinding.ID != "kb-test" {
		t.Errorf("Input.KeyBinding.ID = %q, want %q", att.Input.KeyBinding.ID, "kb-test")
	}
	if string(att.Input.Content.ManifestStrategy.Type) != string(ms.Type) {
		t.Errorf("Input.Content.ManifestStrategy.Type = %q, want %q",
			att.Input.Content.ManifestStrategy.Type, ms.Type)
	}
	if string(att.Input.Content.PlacementStrategy.Type) != string(ps.Type) {
		t.Errorf("Input.Content.PlacementStrategy.Type = %q, want %q",
			att.Input.Content.PlacementStrategy.Type, ps.Type)
	}
	put, ok := att.Output.(*domain.PutManifests)
	if !ok {
		t.Fatalf("Attestation.Output is %T, want *PutManifests", att.Output)
	}
	if len(put.Manifests) == 0 {
		t.Error("PutManifests.Manifests is empty")
	}
}

func TestOrchestration_DeliverWithoutProvenance_NilAttestation(t *testing.T) {
	store, _ := setupStore(t)
	seedDeployment(t, store, domain.Deployment{
		ID:         "no-prov-dep",
		Generation: 1,
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

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	simple := &simpleRecord{ctx: context.Background(), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, "no-prov-dep")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	capRec.mu.Lock()
	delivers := capRec.delivers
	capRec.mu.Unlock()

	if len(delivers) != 1 {
		t.Fatalf("expected 1 deliver input, got %d", len(delivers))
	}
	if delivers[0].Attestation != nil {
		t.Error("Attestation should be nil for token-passthrough deployments (no provenance)")
	}
}

func TestOrchestration_RemoveWithProvenance_AssemblesRemoveAttestation(t *testing.T) {
	store, _ := setupStore(t)
	prov := testProvenance()
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"new1"},
	}

	seedKeyBinding(t, store, testKeyBinding("kb-rm", "test-signer"))
	seedDeployment(t, store, domain.Deployment{
		ID:              "rm-attested",
		Generation:      2,
		ResolvedTargets: []domain.TargetID{"old1"},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Auth: domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "test-signer"}},
		},
		Provenance: prov,
		State:      domain.DeploymentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "old1", Name: "old1", Type: "test"},
		domain.TargetInfo{ID: "new1", Name: "new1", Type: "test"},
	)

	events := make(chan domain.DeploymentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	simple := &simpleRecord{ctx: context.Background(), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, "rm-attested")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	capRec.mu.Lock()
	removes := capRec.removes
	capRec.mu.Unlock()

	if len(removes) != 1 {
		t.Fatalf("expected 1 remove input, got %d", len(removes))
	}

	att := removes[0].Attestation
	if att == nil {
		t.Fatal("Attestation is nil; expected remove attestation")
	}
	if att.Input.Content.DeploymentID != "rm-attested" {
		t.Errorf("Input.Content.DeploymentID = %q, want %q", att.Input.Content.DeploymentID, "rm-attested")
	}
	rm, ok := att.Output.(*domain.RemoveByDeploymentId)
	if !ok {
		t.Fatalf("Attestation.Output is %T, want *RemoveByDeploymentId", att.Output)
	}
	if rm.DeploymentID != "rm-attested" {
		t.Errorf("RemoveByDeploymentId.DeploymentID = %q, want %q", rm.DeploymentID, "rm-attested")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}
