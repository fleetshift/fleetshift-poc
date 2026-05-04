package domain_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func seedFulfillmentAndDeployment(t *testing.T, store domain.Store, depID domain.DeploymentID, f domain.Fulfillment) {
	t.Helper()
	defaultTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if f.CreatedAt.IsZero() {
		f.CreatedAt = defaultTime
	}
	if f.UpdatedAt.IsZero() {
		f.UpdatedAt = f.CreatedAt
	}
	f.ID = domain.FulfillmentID(depID)

	// Use Advance* to populate pending strategy records so the version
	// tables get rows on Create. Preserve the caller's desired generation.
	wantGen := f.Generation
	ms, ps, rs := f.ManifestStrategy, f.PlacementStrategy, f.RolloutStrategy
	f.ManifestStrategy = domain.ManifestStrategySpec{}
	f.PlacementStrategy = domain.PlacementStrategySpec{}
	f.RolloutStrategy = nil
	f.ManifestStrategyVersion = 0
	f.PlacementStrategyVersion = 0
	f.RolloutStrategyVersion = 0
	f.Generation = 0

	f.AdvanceManifestStrategy(ms, f.CreatedAt)
	f.AdvancePlacementStrategy(ps, f.CreatedAt)
	if rs != nil {
		f.AdvanceRolloutStrategy(rs, f.CreatedAt)
	}
	if wantGen > 0 {
		f.Generation = wantGen
	}

	dep := domain.Deployment{
		ID:            depID,
		UID:           "test-uid",
		FulfillmentID: f.ID,
		CreatedAt:     f.CreatedAt,
		UpdatedAt:     f.UpdatedAt,
		Etag:          "test-etag",
	}
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if err := tx.Fulfillments().Create(context.Background(), f); err != nil {
		t.Fatalf("seed fulfillment %q: %v", f.ID, err)
	}
	if err := tx.Deployments().Create(context.Background(), dep); err != nil {
		t.Fatalf("seed deployment %q: %v", depID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func seedSignerEnrollment(t *testing.T, store domain.Store, se domain.SignerEnrollment) {
	t.Helper()
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if err := tx.SignerEnrollments().Create(context.Background(), se); err != nil {
		t.Fatalf("seed signer enrollment: %v", err)
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

func getFulfillment(t *testing.T, store domain.Store, id domain.DeploymentID) domain.Fulfillment {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID(id))
	if err != nil {
		t.Fatalf("get fulfillment for deployment %q: %v", id, err)
	}
	return f
}

func getThinDeployment(t *testing.T, store domain.Store, id domain.DeploymentID) domain.Deployment {
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
	records, err := tx.Deliveries().ListByFulfillment(context.Background(), domain.FulfillmentID(depID))
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

func seedDelivery(t *testing.T, store domain.Store, d domain.Delivery) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.Deliveries().Put(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------
// Recording observer for intermediate state assertions
// ---------------------------------------------------------------------------

type recordingObserver struct {
	domain.NoOpFulfillmentObserver
	mu       sync.Mutex
	states   []domain.FulfillmentState
	filtered []filteredEvent
	outputs  []outputsEvent
}

type filteredEvent struct {
	TargetID domain.TargetID
	Total    int
	Accepted int
}

func (o *recordingObserver) RunStarted(ctx context.Context, _ domain.FulfillmentID) (context.Context, domain.FulfillmentRunProbe) {
	return ctx, &recordingProbe{observer: o}
}

type recordingProbe struct {
	domain.NoOpFulfillmentRunProbe
	observer *recordingObserver
}

func (p *recordingProbe) StateChanged(state domain.FulfillmentState) {
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

func (r *recordingRecord) ID() string               { return r.delegate.ID() }
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
func (r *recordingRecord) Sleep(d time.Duration) error {
	return r.delegate.Sleep(d)
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
	events <-chan domain.FulfillmentEvent
}

func (r *simpleRecord) ID() string               { return "test-simple" }
func (r *simpleRecord) Context() context.Context { return r.ctx }
func (r *simpleRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(r.ctx, in)
}
func (r *simpleRecord) Await(_ string) (any, error) {
	e := <-r.events
	return e, nil
}
func (r *simpleRecord) Sleep(_ time.Duration) error {
	return nil
}

// ---------------------------------------------------------------------------
// Signal routing
// ---------------------------------------------------------------------------

type stubRegistry struct {
	events chan domain.FulfillmentEvent
}

func (r *stubRegistry) SignalFulfillmentEvent(_ context.Context, _ domain.FulfillmentID, event domain.FulfillmentEvent) error {
	r.events <- event
	return nil
}

func (r *stubRegistry) RegisterOrchestration(_ *domain.OrchestrationWorkflowSpec) (domain.OrchestrationWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterCreateDeployment(_ *domain.CreateDeploymentWorkflowSpec) (domain.CreateDeploymentWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterDeleteDeployment(_ *domain.DeleteDeploymentWorkflowSpec) (domain.DeleteDeploymentWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterDeleteCleanup(_ *domain.DeleteCleanupWorkflowSpec) (domain.DeleteCleanupWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterResumeDeployment(_ *domain.ResumeDeploymentWorkflowSpec) (domain.ResumeDeploymentWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterProvisionIdP(_ *domain.ProvisionIdPWorkflowSpec) (domain.ProvisionIdPWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) SignalDeleteCleanupComplete(_ context.Context, _ domain.FulfillmentID, _ domain.DeleteCleanupCompleteEvent) error {
	return nil
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

func (noopDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
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

func (asyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
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

func (emittingAsyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
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

func (d *outputProducingDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
	return nil
}

type failingRemoveDelivery struct {
	err error
}

func (f *failingRemoveDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	signaler.Done(ctx, result)
	return result, nil
}

func (f *failingRemoveDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
	return f.err
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

func (authFailingDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
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

func (d *recordingDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
	return nil
}

// ---------------------------------------------------------------------------
// Helper to build a standard workflow spec for tests
// ---------------------------------------------------------------------------

func newTestWorkflow(store domain.Store, delivery domain.DeliveryService, events chan domain.FulfillmentEvent, opts ...func(*domain.OrchestrationWorkflowSpec)) *domain.OrchestrationWorkflowSpec {
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
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"}, domain.TargetInfo{ID: "t2", Name: "t2", Type: "test"})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State != domain.FulfillmentStateActive {
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
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
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
		State: domain.FulfillmentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "old1", Name: "old1", Type: "test"},
		domain.TargetInfo{ID: "new1", Name: "new1", Type: "test"},
		domain.TargetInfo{ID: "new2", Name: "new2", Type: "test"},
	)

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, domain.FulfillmentID("d1"))
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
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, domain.FulfillmentID("d1"))
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

func TestOrchestration_ZeroTargets_ActiveWithEmptySet(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategySelector, TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test", Labels: map[string]string{"env": "dev"}})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State)
	}
	if len(dep.ResolvedTargets) != 0 {
		t.Errorf("ResolvedTargets = %v, want empty", dep.ResolvedTargets)
	}
	if dep.ActiveWorkflowGen != nil {
		t.Errorf("ActiveWorkflowGen should be nil, got %v", dep.ActiveWorkflowGen)
	}
	if dep.ObservedGeneration != 1 {
		t.Errorf("ObservedGeneration = %d, want 1", dep.ObservedGeneration)
	}
}

func TestOrchestration_DeliveryOutputs_RegistersTargetAndStoresSecret(t *testing.T) {
	store, vault := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"name":"new-cluster"}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"provisioner"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "provisioner", Name: "provisioner", Type: "test"})

	events := make(chan domain.FulfillmentEvent, 16)
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

	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
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
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, &asyncDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State)
	}
}

func TestOrchestration_AsyncDelivery_DeliveryObserverReceivesEvents(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.FulfillmentEvent, 16)
	deliveryObs := &recordingDeliveryObserver{}
	wf := newTestWorkflow(store, &emittingAsyncDelivery{}, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.DeliveryObserver = deliveryObs
	})

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
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
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, authFailingDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State != domain.FulfillmentStatePausedAuth {
		t.Errorf("State = %q, want paused_auth", dep.State)
	}
}

func TestOrchestration_DeletePipeline_RemovesFromTargets(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"},
		domain.TargetInfo{ID: "t2", Name: "t2", Type: "test"},
	)
	seedDelivery(t, store, domain.Delivery{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	})
	seedDelivery(t, store, domain.Delivery{
		ID: "d1:t2", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t2",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	recorder := &recordingRecord{ctx: rec.ctx, delegate: rec}

	_, err := wf.Run(recorder, domain.FulfillmentID("d1"))
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

	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State)
	}
}

func TestOrchestration_DeletePipeline_HardDeletesRecord(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"},
		domain.TargetInfo{ID: "t2", Name: "t2", Type: "test"},
	)
	seedDelivery(t, store, domain.Delivery{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	})
	seedDelivery(t, store, domain.Delivery{
		ID: "d1:t2", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t2",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State)
	}
	deliveries, err := tx.Deliveries().ListByFulfillment(context.Background(), domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatal(err)
	}
	if len(deliveries) != 0 {
		t.Errorf("expected 0 delivery records, got %d", len(deliveries))
	}
}

func TestOrchestration_DeletePipeline_NoTargets_HardDeletes(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        2,
		ResolvedTargets:   nil,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic},
		State:             domain.FulfillmentStateDeleting,
	})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State)
	}
}

func TestOrchestration_DeletePipeline_MissingDeliveryRecord_Skips(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"},
		domain.TargetInfo{ID: "t2", Name: "t2", Type: "test"},
	)
	seedDelivery(t, store, domain.Delivery{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State)
	}
}

func TestOrchestration_DeletePipeline_RemoveFailure_KeepsRecord(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"},
	)
	seedDelivery(t, store, domain.Delivery{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	})

	failingAgent := &failingRemoveDelivery{err: fmt.Errorf("network timeout")}
	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, failingAgent, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err == nil {
		t.Fatal("expected error from Remove failure")
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State != domain.FulfillmentStateDeleting {
		t.Errorf("State = %q, want deleting", dep.State)
	}
}

func TestOrchestration_CompleteReconciliation_LoopsOnNewGeneration(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.FulfillmentEvent, 16)
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

	_, err := wf.Run(interceptor, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
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

func (r *afterLoadBumpGenRecord) ID() string                    { return r.delegate.ID() }
func (r *afterLoadBumpGenRecord) Context() context.Context      { return r.delegate.Context() }
func (r *afterLoadBumpGenRecord) Await(sig string) (any, error) { return r.delegate.Await(sig) }
func (r *afterLoadBumpGenRecord) Sleep(d time.Duration) error   { return r.delegate.Sleep(d) }

func (r *afterLoadBumpGenRecord) Run(activity domain.Activity[any, any], in any) (any, error) {
	out, err := r.delegate.Run(activity, in)
	if err != nil {
		return out, err
	}
	if !r.loaded && activity.Name() == "acquire-lock-and-load" {
		r.loaded = true
		tx, txErr := r.store.Begin(context.Background())
		if txErr != nil {
			return out, txErr
		}
		thinDep, txErr := tx.Deployments().Get(context.Background(), r.depID)
		if txErr != nil {
			tx.Rollback()
			return out, txErr
		}
		fulf, txErr := tx.Fulfillments().Get(context.Background(), thinDep.FulfillmentID)
		if txErr != nil {
			tx.Rollback()
			return out, txErr
		}
		for i := 0; i < r.bumps; i++ {
			fulf.BumpGeneration()
		}
		fulf.UpdatedAt = time.Now().UTC()
		if txErr = tx.Fulfillments().Update(context.Background(), fulf); txErr != nil {
			tx.Rollback()
			return out, txErr
		}
		tx.Commit()
	}
	return out, nil
}

func TestOrchestration_ResourceTypeFiltering(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
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
		State: domain.FulfillmentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "k8s", Name: "k8s", Type: "kubernetes", AcceptedResourceTypes: []domain.ResourceType{"kubernetes.manifest"}},
		domain.TargetInfo{ID: "plain", Name: "plain", Type: "test"},
	)

	events := make(chan domain.FulfillmentEvent, 16)
	obs := &recordingObserver{}
	rd := &recordingDelivery{}
	wf := newTestWorkflow(store, rd, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.Observer = obs
	})

	rec := &simpleRecord{ctx: context.Background(), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
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

func (r *attestationCapturingRecord) ID() string               { return r.delegate.ID() }
func (r *attestationCapturingRecord) Context() context.Context { return r.delegate.Context() }
func (r *attestationCapturingRecord) Await(sig string) (any, error) {
	return r.delegate.Await(sig)
}
func (r *attestationCapturingRecord) Sleep(d time.Duration) error {
	return r.delegate.Sleep(d)
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

func testProvenance(depID domain.DeploymentID, ms domain.ManifestStrategySpec, ps domain.PlacementStrategySpec) *domain.Provenance {
	return &domain.Provenance{
		Content: domain.DeploymentContent{
			DeploymentID:      depID,
			ManifestStrategy:  ms,
			PlacementStrategy: ps,
		},
		Sig: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "test-signer", Issuer: "https://issuer.example.com"},
			ContentHash:    []byte("content-hash"),
			SignatureBytes: []byte("sig-bytes"),
		},
		ValidUntil:         time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpectedGeneration: 1,
	}
}

func testSignerEnrollment(id domain.SignerEnrollmentID, subjectID domain.SubjectID) domain.SignerEnrollment {
	return domain.SignerEnrollment{
		ID: id,
		FederatedIdentity: domain.FederatedIdentity{
			Subject: subjectID,
			Issuer:  "https://issuer.example.com",
		},
		IdentityToken:   "test-id-token",
		RegistrySubject: domain.RegistrySubject("gh-" + string(subjectID)),
		RegistryID:      "github.com",
		CreatedAt:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:       time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestOrchestration_DeliverWithProvenance_AssemblesAttestation(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{ResourceType: "test.resource", Raw: json.RawMessage(`{"kind":"ConfigMap"}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}
	prov := testProvenance("attested-dep", ms, ps)

	seedSignerEnrollment(t, store, testSignerEnrollment("se-test", "test-signer"))
	seedFulfillmentAndDeployment(t, store, "attested-dep", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Auth: domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "test-signer"}},
		},
		Provenance: prov,
		State:      domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	simple := &simpleRecord{ctx: context.Background(), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, domain.FulfillmentID("attested-dep"))
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

	depContent, ok := att.Input.Provenance.Content.(domain.DeploymentContent)
	if !ok {
		t.Fatalf("Input.Provenance.Content is %T, want DeploymentContent", att.Input.Provenance.Content)
	}
	if depContent.DeploymentID != "attested-dep" {
		t.Errorf("DeploymentContent.DeploymentID = %q, want %q", depContent.DeploymentID, "attested-dep")
	}
	if att.Input.Provenance.Sig.Signer != prov.Sig.Signer {
		t.Errorf("Input.Provenance.Sig.Signer = %v, want %v", att.Input.Provenance.Sig.Signer, prov.Sig.Signer)
	}
	if att.Input.Signer.RegistrySubject != "gh-test-signer" {
		t.Errorf("Input.Signer.RegistrySubject = %q, want %q", att.Input.Signer.RegistrySubject, "gh-test-signer")
	}
	if string(depContent.ManifestStrategy.Type) != string(ms.Type) {
		t.Errorf("DeploymentContent.ManifestStrategy.Type = %q, want %q",
			depContent.ManifestStrategy.Type, ms.Type)
	}
	if string(depContent.PlacementStrategy.Type) != string(ps.Type) {
		t.Errorf("DeploymentContent.PlacementStrategy.Type = %q, want %q",
			depContent.PlacementStrategy.Type, ps.Type)
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
	seedFulfillmentAndDeployment(t, store, "no-prov-dep", domain.Fulfillment{
		Generation: 1,
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1"},
		},
		State: domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	simple := &simpleRecord{ctx: context.Background(), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, domain.FulfillmentID("no-prov-dep"))
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

// authFailingNoSignalDelivery returns [domain.DeliveryStateAuthFailed]
// without calling signaler.Done. This reproduces the bug where the
// kubernetes agent returns an early auth failure (e.g. missing
// trust_bundle) without notifying the workflow, causing awaitDeliveries
// to block indefinitely.
type authFailingNoSignalDelivery struct{}

func (authFailingNoSignalDelivery) Deliver(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	return domain.DeliveryResult{
		State:   domain.DeliveryStateAuthFailed,
		Message: "attestation verification failed: target has no trust_bundle property",
	}, nil
}

func (authFailingNoSignalDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, _ *domain.DeliverySignaler) error {
	return nil
}

func TestOrchestration_AuthFailureNoSignal_DoesNotHang(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, authFailingNoSignalDelivery{}, events)

	rec := &simpleRecord{ctx: context.Background(), events: events}

	done := make(chan error, 1)
	go func() {
		_, err := wf.Run(rec, domain.FulfillmentID("d1"))
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned unexpected error: %v", err)
		}
		dep := getFulfillment(t, store, "d1")
		if dep.State != domain.FulfillmentStatePausedAuth {
			t.Errorf("State = %q, want paused_auth", dep.State)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("orchestration hung: deliver-to-target returned auth_failed without signaling Done, awaitDeliveries blocked forever")
	}
}

func TestOrchestration_RemoveWithProvenance_AssemblesRemoveAttestation(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"new1"},
	}
	prov := testProvenance("rm-attested", ms, ps)

	seedSignerEnrollment(t, store, testSignerEnrollment("se-rm", "test-signer"))
	seedFulfillmentAndDeployment(t, store, "rm-attested", domain.Fulfillment{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"old1"},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Auth: domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "test-signer"}},
		},
		Provenance: prov,
		State:      domain.FulfillmentStateCreating,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "old1", Name: "old1", Type: "test"},
		domain.TargetInfo{ID: "new1", Name: "new1", Type: "test"},
	)

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	simple := &simpleRecord{ctx: context.Background(), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, domain.FulfillmentID("rm-attested"))
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
	depContent, ok := att.Input.Provenance.Content.(domain.DeploymentContent)
	if !ok {
		t.Fatalf("Input.Provenance.Content is %T, want DeploymentContent", att.Input.Provenance.Content)
	}
	if depContent.DeploymentID != "rm-attested" {
		t.Errorf("DeploymentContent.DeploymentID = %q, want %q", depContent.DeploymentID, "rm-attested")
	}
	rm, ok := att.Output.(*domain.RemoveByDeploymentId)
	if !ok {
		t.Fatalf("Attestation.Output is %T, want *RemoveByDeploymentId", att.Output)
	}
	if rm.DeploymentID != "rm-attested" {
		t.Errorf("RemoveByDeploymentId.DeploymentID = %q, want %q", rm.DeploymentID, "rm-attested")
	}
}

func TestOrchestration_DeleteWithProvenance_AssemblesRemoveAttestation(t *testing.T) {
	store, _ := setupStore(t)
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1", "t2"},
	}
	prov := testProvenance("del-attested", ms, ps)

	seedSignerEnrollment(t, store, testSignerEnrollment("se-del", "test-signer"))
	seedFulfillmentAndDeployment(t, store, "del-attested", domain.Fulfillment{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Auth: domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "test-signer"}},
		},
		Provenance: prov,
		State:      domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"},
		domain.TargetInfo{ID: "t2", Name: "t2", Type: "test"},
	)
	seedDelivery(t, store, domain.Delivery{
		ID: "del-attested:t1", FulfillmentID: domain.FulfillmentID("del-attested"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	})
	seedDelivery(t, store, domain.Delivery{
		ID: "del-attested:t2", FulfillmentID: domain.FulfillmentID("del-attested"), TargetID: "t2",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	simple := &simpleRecord{ctx: context.Background(), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, domain.FulfillmentID("del-attested"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	capRec.mu.Lock()
	removes := capRec.removes
	capRec.mu.Unlock()

	if len(removes) != 2 {
		t.Fatalf("expected 2 remove inputs (one per target), got %d", len(removes))
	}

	for i, rm := range removes {
		if rm.Attestation == nil {
			t.Errorf("remove[%d]: Attestation is nil; expected attestation with signer assertion", i)
			continue
		}
		att := rm.Attestation
		depContent, ok := att.Input.Provenance.Content.(domain.DeploymentContent)
		if !ok {
			t.Errorf("remove[%d]: Input.Provenance.Content is %T, want DeploymentContent", i, att.Input.Provenance.Content)
			continue
		}
		if depContent.DeploymentID != "del-attested" {
			t.Errorf("remove[%d]: DeploymentContent.DeploymentID = %q, want %q", i, depContent.DeploymentID, "del-attested")
		}
		if att.Input.Signer.RegistrySubject != "gh-test-signer" {
			t.Errorf("remove[%d]: RegistrySubject = %q, want %q", i, att.Input.Signer.RegistrySubject, "gh-test-signer")
		}
		rmOut, ok := att.Output.(*domain.RemoveByDeploymentId)
		if !ok {
			t.Errorf("remove[%d]: Output is %T, want *RemoveByDeploymentId", i, att.Output)
			continue
		}
		if rmOut.DeploymentID != "del-attested" {
			t.Errorf("remove[%d]: RemoveByDeploymentId.DeploymentID = %q, want %q",
				i, rmOut.DeploymentID, "del-attested")
		}
	}

	// Orchestration cleans up delivery data but leaves the fulfillment
	// row; the DeleteCleanupWorkflow deletes both rows after receiving
	// the signal.
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("del-attested"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist after orchestration cleanup, got: %v", err)
	}
	if f.State != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State)
	}
	deliveries, err := tx.Deliveries().ListByFulfillment(context.Background(), domain.FulfillmentID("del-attested"))
	if err != nil {
		t.Fatalf("list deliveries: %v", err)
	}
	if len(deliveries) != 0 {
		t.Errorf("expected delivery records cleaned up, still have %d", len(deliveries))
	}
}

func TestOrchestration_DeleteWithoutProvenance_NilAttestation(t *testing.T) {
	store, _ := setupStore(t)

	seedFulfillmentAndDeployment(t, store, "del-no-prov", domain.Fulfillment{
		Generation:      2,
		ResolvedTargets: []domain.TargetID{"t1"},
		ManifestStrategy: domain.ManifestStrategySpec{
			Type:      domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"t1"},
		},
		State: domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfo{ID: "t1", Name: "t1", Type: "test"})
	seedDelivery(t, store, domain.Delivery{
		ID: "del-no-prov:t1", FulfillmentID: domain.FulfillmentID("del-no-prov"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{}, events)

	simple := &simpleRecord{ctx: context.Background(), events: events}
	capRec := &attestationCapturingRecord{delegate: simple}

	_, err := wf.Run(capRec, domain.FulfillmentID("del-no-prov"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	capRec.mu.Lock()
	removes := capRec.removes
	capRec.mu.Unlock()

	if len(removes) != 1 {
		t.Fatalf("expected 1 remove input, got %d", len(removes))
	}
	if removes[0].Attestation != nil {
		t.Error("Attestation should be nil for deployments without provenance")
	}
}

// ---------------------------------------------------------------------------
// Fault-injection store wrapper
// ---------------------------------------------------------------------------

// commitFaultStore wraps a [domain.Store] and injects a single transient
// commit failure on the first write-transaction commit after [Arm] is
// called. Once the fault fires it is permanently disarmed.
type commitFaultStore struct {
	domain.Store
	mu      sync.Mutex
	armed   bool
	tripped bool
	err     error
}

// Arm enables the fault. The very next write-tx Commit will fail.
func (s *commitFaultStore) Arm() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.tripped {
		s.armed = true
	}
}

func (s *commitFaultStore) Begin(ctx context.Context) (domain.Tx, error) {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &commitFaultTx{Tx: tx, store: s}, nil
}

type commitFaultTx struct {
	domain.Tx
	store *commitFaultStore
}

func (tx *commitFaultTx) Commit() error {
	tx.store.mu.Lock()
	shouldFault := tx.store.armed && !tx.store.tripped
	if shouldFault {
		tx.store.armed = false
		tx.store.tripped = true
	}
	tx.store.mu.Unlock()

	if shouldFault {
		tx.Tx.Rollback()
		return tx.store.err
	}
	return tx.Tx.Commit()
}

// faultArmingObserver arms a [commitFaultStore] when delivery outputs
// are processed. This creates a precise fault point: outputs have been
// committed, and the very next write (reconciliation completion) will
// fail.
type faultArmingObserver struct {
	domain.NoOpFulfillmentObserver
	store *commitFaultStore
}

func (o *faultArmingObserver) RunStarted(ctx context.Context, _ domain.FulfillmentID) (context.Context, domain.FulfillmentRunProbe) {
	return ctx, &faultArmingProbe{store: o.store}
}

type faultArmingProbe struct {
	domain.NoOpFulfillmentRunProbe
	store *commitFaultStore
}

func (p *faultArmingProbe) DeliveryOutputsProcessed(_ []domain.ProvisionedTarget, _ int) {
	p.store.Arm()
}

// ---------------------------------------------------------------------------
// Replay-safety regression
// ---------------------------------------------------------------------------

func TestOrchestration_DeliveryOutputs_ReplayAfterTransientFailure_ErrAlreadyExists(t *testing.T) {
	realStore, vault := setupStore(t)
	seedFulfillmentAndDeployment(t, realStore, "d1", domain.Fulfillment{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"name":"new-cluster"}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"provisioner"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, realStore, domain.TargetInfo{ID: "provisioner", Name: "provisioner", Type: "test"})

	deliveryAgent := &outputProducingDelivery{
		targets: []domain.ProvisionedTarget{{
			ID: "k8s-new-cluster", Type: "kubernetes", Name: "new-cluster",
			Properties: map[string]string{"kubeconfig_ref": "targets/k8s-new-cluster/kubeconfig"},
		}},
		secrets: []domain.ProducedSecret{{
			Ref: "targets/k8s-new-cluster/kubeconfig", Value: []byte("fake-kubeconfig-data"),
		}},
	}

	// The faultArmingObserver arms the store right after delivery
	// outputs are committed. The next write-tx commit (reconciliation
	// completion) hits a transient failure, causing ContinueAsNew. The
	// fault fires once; retries see a healthy store.
	store := &commitFaultStore{
		Store: realStore,
		err:   fmt.Errorf("transient DB error"),
	}
	obs := &faultArmingObserver{store: store}

	// Simulate the engine's ContinueAsNew loop: re-run the workflow on
	// each restart, up to a bounded number of attempts. A healthy
	// pipeline should converge within two attempts (one fault + one
	// successful retry).
	const maxAttempts = 3
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		events := make(chan domain.FulfillmentEvent, 16)
		wf := newTestWorkflow(store, deliveryAgent, events, func(wf *domain.OrchestrationWorkflowSpec) {
			wf.Vault = vault
			wf.Observer = obs
		})
		rec := &simpleRecord{ctx: context.Background(), events: events}
		_, lastErr = wf.Run(rec, domain.FulfillmentID("d1"))
		if lastErr == nil {
			break
		}
		var canErr *domain.ContinueAsNewError
		if !errors.As(lastErr, &canErr) {
			t.Fatalf("attempt %d: unexpected non-ContinueAsNew error: %v", attempt+1, lastErr)
		}
	}
	if lastErr != nil {
		t.Fatalf("workflow did not converge after %d attempts "+
			"(stuck in ContinueAsNew loop from replayed output registration): %v",
			maxAttempts, lastErr)
	}

	dep := getFulfillment(t, realStore, "d1")
	if dep.State != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active after replay recovery", dep.State)
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
