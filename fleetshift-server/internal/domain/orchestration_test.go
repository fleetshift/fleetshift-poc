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

// testContext returns a context with a 5-second deadline for
// orchestration tests. These tests run against in-memory stores and
// synchronous records, so they complete in milliseconds under normal
// conditions. A short deadline ensures quick failure (rather than
// hanging for the global go test timeout) when a signal is lost.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func seedFulfillmentAndDeployment(t *testing.T, store domain.Store, depID domain.DeploymentID, seed domain.FulfillmentSnapshot) {
	t.Helper()
	defaultTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if seed.CreatedAt.IsZero() {
		seed.CreatedAt = defaultTime
	}
	if seed.UpdatedAt.IsZero() {
		seed.UpdatedAt = seed.CreatedAt
	}
	seed.ID = domain.FulfillmentID(depID)

	// Use Advance* to populate pending strategy records so the version
	// tables get rows on Create. The initial create always lands at
	// generation 1 (advanceGeneration bumps once per loaded state).
	wantGen := seed.Generation
	ms, ps, rs := seed.ManifestStrategy, seed.PlacementStrategy, seed.RolloutStrategy
	seed.ManifestStrategy = domain.ManifestStrategySpec{}
	seed.PlacementStrategy = domain.PlacementStrategySpec{}
	seed.RolloutStrategy = nil
	seed.ManifestStrategyVersion = 0
	seed.PlacementStrategyVersion = 0
	seed.RolloutStrategyVersion = 0
	seed.Generation = 0

	f := domain.FulfillmentFromSnapshot(seed)
	f.AdvanceManifestStrategy(ms, seed.CreatedAt)
	f.AdvancePlacementStrategy(ps, seed.CreatedAt)
	if rs != nil {
		f.AdvanceRolloutStrategy(rs, seed.CreatedAt)
	}

	dep := domain.DeploymentFromSnapshot(domain.DeploymentSnapshot{
		ID:            depID,
		UID:           "test-uid",
		FulfillmentID: f.ID(),
		CreatedAt:     seed.CreatedAt,
		UpdatedAt:     seed.UpdatedAt,
	})
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if err := tx.Fulfillments().Create(context.Background(), f); err != nil {
		t.Fatalf("seed fulfillment %q: %v", f.ID(), err)
	}
	if err := tx.Deployments().Create(context.Background(), dep); err != nil {
		t.Fatalf("seed deployment %q: %v", depID, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Reach the desired generation through real update cycles. Each
	// transaction loads, touches, and saves — exactly what would happen
	// in production. This keeps the test grounded in reachable state.
	for gen := f.Generation(); gen < wantGen; gen++ {
		tx2, err := store.Begin(context.Background())
		if err != nil {
			t.Fatalf("begin touch tx: %v", err)
		}
		defer tx2.Rollback()
		loaded, err := tx2.Fulfillments().Get(context.Background(), f.ID())
		if err != nil {
			t.Fatalf("reload fulfillment: %v", err)
		}
		loaded.Touch(seed.CreatedAt)
		if err := tx2.Fulfillments().Update(context.Background(), loaded); err != nil {
			t.Fatalf("touch fulfillment: %v", err)
		}
		if err := tx2.Commit(); err != nil {
			t.Fatalf("commit touch: %v", err)
		}
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
			t.Fatalf("seed target %q: %v", tgt.ID(), err)
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
	return *f
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

func getInventoryItem(t *testing.T, store domain.Store, id domain.InventoryItemID) domain.InventoryItem {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	item, err := tx.Inventory().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get inventory item %q: %v", id, err)
	}
	return item
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

func (o *recordingObserver) ProcessOutputsStarted(ctx context.Context) (context.Context, domain.ProcessOutputsProbe) {
	return ctx, &recordingOutputsProbe{observer: o}
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

func (p *recordingProbe) ManifestsFiltered(target domain.TargetInfo, total, accepted int) {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	p.observer.filtered = append(p.observer.filtered, filteredEvent{
		TargetID: target.ID(),
		Total:    total,
		Accepted: accepted,
	})
}

type outputsEvent struct {
	TargetIDs []domain.TargetID
	Secrets   int
}

type recordingOutputsProbe struct {
	domain.NoOpProcessOutputsProbe
	observer *recordingObserver
	targets  int
	secrets  int
}

func (p *recordingOutputsProbe) SecretsStored(count int) {
	p.secrets = count
}

func (p *recordingOutputsProbe) TargetsRegistered(count int) {
	p.targets = count
}

func (p *recordingOutputsProbe) End() {
	if p.targets == 0 && p.secrets == 0 {
		return
	}
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	p.observer.outputs = append(p.observer.outputs, outputsEvent{
		Secrets: p.secrets,
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
		targetID = v.Target.ID()
	case domain.GenerateManifestsInput:
		targetID = v.Target.ID()
	case domain.DeliverInput:
		targetID = v.Target.ID()
	}
	r.records = append(r.records, activityRecord{Name: name, TargetID: targetID})
	return r.delegate.Run(activity, in)
}

func (r *recordingRecord) Await(signalName string) (any, error) {
	return r.delegate.Await(signalName)
}
func (r *recordingRecord) AwaitWithTimeout(signalName string, timeout time.Duration) (any, error) {
	return r.delegate.AwaitWithTimeout(signalName, timeout)
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
	select {
	case e := <-r.events:
		return e, nil
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
}
func (r *simpleRecord) AwaitWithTimeout(_ string, timeout time.Duration) (any, error) {
	if timeout == 0 {
		select {
		case e := <-r.events:
			return e, nil
		default:
			return nil, domain.ErrSignalTimeout
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case e := <-r.events:
		return e, nil
	case <-timer.C:
		return nil, domain.ErrSignalTimeout
	case <-r.ctx.Done():
		return nil, r.ctx.Err()
	}
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

func (r *stubRegistry) RegisterDeleteDeploymentCleanup(_ *domain.DeleteDeploymentCleanupWorkflowSpec) (domain.DeleteDeploymentCleanupWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterDeleteManagedResourceCleanup(_ *domain.DeleteManagedResourceCleanupWorkflowSpec) (domain.DeleteManagedResourceCleanupWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterResumeDeployment(_ *domain.ResumeDeploymentWorkflowSpec) (domain.ResumeDeploymentWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterProvisionIdP(_ *domain.ProvisionIdPWorkflowSpec) (domain.ProvisionIdPWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterCreateManagedResource(_ *domain.CreateManagedResourceWorkflowSpec) (domain.CreateManagedResourceWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) RegisterDeleteManagedResource(_ *domain.DeleteManagedResourceWorkflowSpec) (domain.DeleteManagedResourceWorkflow, error) {
	return nil, nil
}

func (r *stubRegistry) SignalDeleteCleanupComplete(_ context.Context, _ domain.FulfillmentID, _ domain.DeleteCleanupCompleteEvent) error {
	return nil
}

// ---------------------------------------------------------------------------
// Delivery agent fakes
// ---------------------------------------------------------------------------

type noopDelivery struct {
	events chan<- domain.FulfillmentEvent
}

func (d noopDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (d noopDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// asyncDelivery signals the workflow via the events channel in a
// goroutine and optionally closes a done channel for test
// synchronization.
type asyncDelivery struct {
	events chan<- domain.FulfillmentEvent
	done   chan struct{}
}

func (a *asyncDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
		if a.done != nil {
			close(a.done)
		}
	}()
	return nil
}

func (a *asyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// emittingAsyncDelivery is like asyncDelivery but also emits a
// progress event (ignored by the domain workflow, exercised
// indirectly). The signal is the important part.
type emittingAsyncDelivery struct {
	events chan<- domain.FulfillmentEvent
	done   chan struct{}
}

func (a *emittingAsyncDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
		if a.done != nil {
			close(a.done)
		}
	}()
	return nil
}

func (a *emittingAsyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

type outputProducingDelivery struct {
	events  chan<- domain.FulfillmentEvent
	targets []domain.ProvisionedTarget
	secrets []domain.ProducedSecret
}

func (d *outputProducingDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	result := domain.DeliveryResult{
		State:              domain.DeliveryStateDelivered,
		ProvisionedTargets: d.targets,
		ProducedSecrets:    d.secrets,
	}
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     result,
			},
		}
	}()
	return nil
}

func (d *outputProducingDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

type failingRemoveDelivery struct {
	events chan<- domain.FulfillmentEvent
	err    error
}

func (f *failingRemoveDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		f.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (f *failingRemoveDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	state := domain.DeliveryStateFailed
	if domain.IsAuthExpired(f.err) {
		state = domain.DeliveryStateAuthFailed
	}
	go func() {
		f.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: state, Message: f.err.Error()},
			},
		}
	}()
	return nil
}

type authFailingDelivery struct {
	events chan<- domain.FulfillmentEvent
}

func (d authFailingDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result: domain.DeliveryResult{
					State:   domain.DeliveryStateAuthFailed,
					Message: "401 Unauthorized",
				},
			},
		}
	}()
	return nil
}

func (d authFailingDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

type recordingDelivery struct {
	events chan<- domain.FulfillmentEvent
	mu     sync.Mutex
	// delivered tracks which targets received deliveries.
	delivered []domain.TargetID
}

func (d *recordingDelivery) Deliver(_ context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	d.mu.Lock()
	d.delivered = append(d.delivered, target.ID())
	d.mu.Unlock()
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (d *recordingDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// ---------------------------------------------------------------------------
// Helper to build a standard workflow spec for tests
// ---------------------------------------------------------------------------

func newTestWorkflow(store domain.Store, delivery domain.DeliveryAgent, events chan domain.FulfillmentEvent, opts ...func(*domain.OrchestrationWorkflowSpec)) *domain.OrchestrationWorkflowSpec {
	reg := &stubRegistry{events: events}
	wf := domain.NewOrchestrationWorkflowSpec(
		store, delivery, domain.StrategyFactory{Store: store}, reg,
		domain.WithAckRetryInterval(5*time.Second),
	)
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
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}), domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "t2", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
	if dep.ObservedGeneration() != 1 {
		t.Errorf("ObservedGeneration = %d, want 1", dep.ObservedGeneration())
	}

	deliveries := getDeliveries(t, store, "d1")
	if len(deliveries) != 2 {
		t.Fatalf("expected 2 deliveries, got %d", len(deliveries))
	}
}

func TestOrchestration_RemoveStepsRunBeforeDeliverSteps(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
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
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "old1", Name: "old1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new1", Name: "new1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new2", Name: "new2", Type: "test"}),
	)

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
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
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
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
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategySelector, TargetSelector: &domain.TargetSelector{MatchLabels: map[string]string{"env": "prod"}}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test", Labels: map[string]string{"env": "dev"}}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
	if len(dep.ResolvedTargets()) != 0 {
		t.Errorf("ResolvedTargets = %v, want empty", dep.ResolvedTargets())
	}
	if dep.ActiveWorkflowGen() != nil {
		t.Errorf("ActiveWorkflowGen should be nil, got %v", dep.ActiveWorkflowGen())
	}
	if dep.ObservedGeneration() != 1 {
		t.Errorf("ObservedGeneration = %d, want 1", dep.ObservedGeneration())
	}
}

func TestOrchestration_DeliveryOutputs_RegistersTargetAndStoresSecret(t *testing.T) {
	store, vault := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"name":"new-cluster"}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"provisioner"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "provisioner", Name: "provisioner", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, &outputProducingDelivery{
		events: events,
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

	rec := &simpleRecord{ctx: testContext(t), events: events}
	obs := &recordingObserver{}
	wf.Observer = obs

	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	tgt := getTarget(t, store, "k8s-new-cluster")
	if tgt.Type() != "kubernetes" {
		t.Errorf("target type = %q, want kubernetes", tgt.Type())
	}

	item := getInventoryItem(t, store, "target:k8s-new-cluster")
	if item.SourceDeliveryID() == nil || *item.SourceDeliveryID() != "d1:provisioner" {
		t.Fatalf("inventory SourceDeliveryID = %v, want d1:provisioner", item.SourceDeliveryID())
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
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, &asyncDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
}

func TestOrchestration_EmittingAsyncDelivery_ReachesActive(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, &emittingAsyncDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}
}

func TestOrchestration_AuthFailure_SetsPausedAuth(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, authFailingDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if !dep.Paused() {
		t.Error("expected fulfillment to be paused after auth failure")
	}
	if dep.State() != domain.FulfillmentStateCreating {
		t.Errorf("State = %q, want creating (pause must preserve lifecycle state)", dep.State())
	}
}

func TestOrchestration_DeletePipeline_RemovesFromTargets(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "t2", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t2", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t2",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
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
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
}

func TestOrchestration_DeletePipeline_ResetsDeliveryForRemove(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})

	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var observedState domain.DeliveryState
	var removeCalled bool
	delivery := &deliveryStateChecker{
		store:  store,
		events: events,
		onRemove: func(deliveryID domain.DeliveryID) {
			removeCalled = true
			tx, err := store.BeginReadOnly(context.Background())
			if err != nil {
				t.Fatalf("read delivery in Remove: %v", err)
			}
			defer tx.Rollback()
			d, err := tx.Deliveries().Get(context.Background(), deliveryID)
			if err != nil {
				t.Fatalf("get delivery in Remove: %v", err)
			}
			observedState = d.State()
		},
	}
	wf := newTestWorkflow(store, delivery, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !removeCalled {
		t.Fatal("Remove was never called")
	}
	if observedState != domain.DeliveryStatePending {
		t.Errorf("delivery state inside Remove = %q, want %q", observedState, domain.DeliveryStatePending)
	}
}

// deliveryStateChecker is a DeliveryService stub that calls onRemove
// inside Remove so the test can inspect delivery record state.
type deliveryStateChecker struct {
	store    domain.Store
	events   chan<- domain.FulfillmentEvent
	onRemove func(deliveryID domain.DeliveryID)
}

func (d *deliveryStateChecker) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (d *deliveryStateChecker) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	if d.onRemove != nil {
		d.onRemove(deliveryID)
	}
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// TestOrchestration_DeletePipeline_WaitsForProgressingDelivery seeds a
// delivery in Progressing state (acked but not completed — as happens after a
// crash) and verifies that RemoveFromTarget does NOT re-dispatch via
// Agent.Remove. Instead it returns Dispatched=true so dispatchAndAwait
// waits for the completion signal from RecoverActiveDeliveries.
func TestOrchestration_DeletePipeline_WaitsForProgressingDelivery(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})

	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 2,
		State:      domain.DeliveryStateProgressing,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var removeCalled bool
	delivery := &deliveryStateChecker{
		store:  store,
		events: events,
		onRemove: func(_ domain.DeliveryID) {
			removeCalled = true
		},
	}
	wf := newTestWorkflow(store, delivery, events)

	// Simulate RecoverActiveDeliveries sending the completion signal
	// before the ack-timeout fires: inject a completion event into the
	// channel so dispatchAndAwait can pick it up.
	go func() {
		events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: "d1:t1",
				Generation: 2,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()

	rec := &simpleRecord{ctx: testContext(t), events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("d1")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if removeCalled {
		t.Fatal("Remove was called — Progressing delivery should wait for completion signal, not re-dispatch")
	}

	// The completion signal must have been consumed by dispatchAndAwait.
	// If RemoveFromTarget returned Dispatched=false the delivery would be
	// skipped, the signal would remain unconsumed, and cleanup would run
	// prematurely without the addon finishing the real work.
	select {
	case <-events:
		t.Fatal("completion signal not consumed — delivery was likely skipped (Dispatched=false)")
	default:
	}
}

func TestOrchestration_DeletePipeline_HardDeletesRecord(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateDeleting,
	})

	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "t2", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t2", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t2",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
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
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
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
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   nil,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic},
		State:             domain.FulfillmentStateDeleting,
	})

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
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
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
}

func TestOrchestration_DeletePipeline_CleansUpOwnedOutputsAndSecrets(t *testing.T) {
	store, vault := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:      2,
		ResolvedTargets: []domain.TargetID{"gcphcp-provider"},
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{
				ResourceType: "api.gcphcp.cluster",
				Raw:          json.RawMessage(`{"name":"guest-cluster"}`),
			}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"gcphcp-provider"},
		},
		State: domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "gcphcp-provider", Name: "gcphcp-provider", Type: "gcphcp"}))

	deliveryID := domain.DeliveryID("d1:gcphcp-provider")
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID:            deliveryID,
		FulfillmentID: domain.FulfillmentID("d1"),
		TargetID:      "gcphcp-provider",
		Manifests: []domain.Manifest{{
			ResourceType: "api.gcphcp.cluster",
			Raw:          json.RawMessage(`{"name":"guest-cluster"}`),
		}},
		State: domain.DeliveryStateDelivered,
	}))

	ctx := testContext(t)
	if err := vault.Put(ctx, "targets/k8s-guest-cluster/sa-token", []byte("fake-sa-token")); err != nil {
		t.Fatalf("vault put: %v", err)
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	if err := tx.Inventory().Create(ctx, domain.InventoryItemFromSnapshot(domain.InventoryItemSnapshot{
		ID:               "target:k8s-guest-cluster",
		Type:             "kubernetes",
		Name:             "guest-cluster",
		Properties:       json.RawMessage(`{"api_server":"https://guest.example:6443","service_account_token_ref":"targets/k8s-guest-cluster/sa-token"}`),
		SourceDeliveryID: &deliveryID,
		CreatedAt:        now,
		UpdatedAt:        now,
	})); err != nil {
		t.Fatalf("seed inventory item: %v", err)
	}
	if err := tx.Targets().Create(ctx, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{
		ID:              "k8s-guest-cluster",
		InventoryItemID: "target:k8s-guest-cluster",
		Type:            "kubernetes",
		Name:            "guest-cluster",
		Properties: map[string]string{
			"api_server":                "https://guest.example:6443",
			"service_account_token_ref": "targets/k8s-guest-cluster/sa-token",
		},
	})); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seeded output: %v", err)
	}

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.Vault = vault
	})

	rec := &simpleRecord{ctx: ctx, events: events}
	_, err = wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	_ = getTarget(t, store, "gcphcp-provider")

	readTx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read tx: %v", err)
	}

	if _, err := readTx.Targets().Get(ctx, "k8s-guest-cluster"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected guest target to be deleted, got err=%v", err)
	}
	if _, err := readTx.Inventory().Get(ctx, "target:k8s-guest-cluster"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected guest inventory item to be deleted, got err=%v", err)
	}
	deliveries, err := readTx.Deliveries().ListByFulfillment(ctx, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("list deliveries: %v", err)
	}
	if len(deliveries) != 0 {
		t.Fatalf("expected delivery records to be deleted, got %d", len(deliveries))
	}
	f, err := readTx.Fulfillments().Get(ctx, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist, got: %v", err)
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
	if err := readTx.Rollback(); err != nil {
		t.Fatalf("close read tx: %v", err)
	}

	if _, err := vault.Get(ctx, "targets/k8s-guest-cluster/sa-token"); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected owned vault secret to be deleted, got err=%v", err)
	}
}

func TestOrchestration_DeletePipeline_MissingDeliveryRecord_Skips(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1", "t2"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1", "t2"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "t2", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
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
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
	}
}

func TestOrchestration_DeletePipeline_RemoveFailure_KeepsRecord(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store,
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	failingAgent := &failingRemoveDelivery{events: events, err: fmt.Errorf("network timeout")}
	wf := newTestWorkflow(store, failingAgent, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err == nil {
		t.Fatal("expected error from Remove failure")
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State() != domain.FulfillmentStateDeleting {
		t.Errorf("State = %q, want deleting", dep.State())
	}
}

func TestOrchestration_CompleteReconciliation_LoopsOnNewGeneration(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	// Intercepting record bumps generation after the first load,
	// simulating a concurrent external mutation. The workflow should
	// loop: first iteration reconciles gen 1 and sees gen 3 has
	// arrived, second iteration reconciles gen 3 and exits.
	rec := &simpleRecord{ctx: testContext(t), events: events}
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
	if dep.ObservedGeneration() != 3 {
		t.Errorf("ObservedGeneration = %d, want 3 (loop should reconcile up to bumped generation)", dep.ObservedGeneration())
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
func (r *afterLoadBumpGenRecord) AwaitWithTimeout(sig string, timeout time.Duration) (any, error) {
	return r.delegate.AwaitWithTimeout(sig, timeout)
}
func (r *afterLoadBumpGenRecord) Sleep(d time.Duration) error { return r.delegate.Sleep(d) }

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
		// Simulate N separate transactions each advancing generation.
		// Each iteration re-loads (hydrating loadedGeneration from the
		// repo) then calls Touch to bump generation via the real domain
		// contract.
		for range r.bumps {
			fulf, txErr := tx.Fulfillments().Get(context.Background(), thinDep.FulfillmentID())
			if txErr != nil {
				tx.Rollback()
				return out, txErr
			}
			fulf.Touch(time.Now().UTC())
			if txErr = tx.Fulfillments().Update(context.Background(), fulf); txErr != nil {
				tx.Rollback()
				return out, txErr
			}
		}
		tx.Commit()
	}
	return out, nil
}

func TestOrchestration_ResourceTypeFiltering(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
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
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "k8s", Name: "k8s", Type: "kubernetes", AcceptedResourceTypes: []domain.ResourceType{"kubernetes.manifest"}}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "plain", Name: "plain", Type: "test"}),
	)

	events := make(chan domain.FulfillmentEvent, 16)
	obs := &recordingObserver{}
	rd := &recordingDelivery{events: events}
	wf := newTestWorkflow(store, rd, events, func(wf *domain.OrchestrationWorkflowSpec) {
		wf.Observer = obs
	})

	rec := &simpleRecord{ctx: testContext(t), events: events}
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
func (r *attestationCapturingRecord) AwaitWithTimeout(sig string, timeout time.Duration) (any, error) {
	return r.delegate.AwaitWithTimeout(sig, timeout)
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
	return domain.SignerEnrollmentFromSnapshot(domain.SignerEnrollmentSnapshot{
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
	})
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
	seedFulfillmentAndDeployment(t, store, "attested-dep", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Auth: domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "test-signer"}},
		},
		Provenance: prov,
		State:      domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	simple := &simpleRecord{ctx: testContext(t), events: events}
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
	seedFulfillmentAndDeployment(t, store, "no-prov-dep", domain.FulfillmentSnapshot{
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
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	simple := &simpleRecord{ctx: testContext(t), events: events}
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

// authFailingNoSignalDelivery signals [domain.DeliveryStateAuthFailed]
// through the events channel. This verifies that auth failures flowing
// through the async path (dispatchAndAwait) correctly pause the
// fulfillment. In the previous architecture this fake returned auth
// failure synchronously without signaling; the async delivery model
// eliminated that dual-path.
type authFailingNoSignalDelivery struct {
	events chan<- domain.FulfillmentEvent
}

func (d authFailingNoSignalDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result: domain.DeliveryResult{
					State:   domain.DeliveryStateAuthFailed,
					Message: "attestation verification failed: target has no trust_bundle property",
				},
			},
		}
	}()
	return nil
}

func (d authFailingNoSignalDelivery) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func TestOrchestration_AuthFailureNoSignal_DoesNotHang(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, authFailingNoSignalDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}

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
		if !dep.Paused() {
			t.Error("expected fulfillment to be paused after auth failure")
		}
		if dep.State() != domain.FulfillmentStateCreating {
			t.Errorf("State = %q, want creating (pause must preserve lifecycle state)", dep.State())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("orchestration hung: deliver-to-target returned auth_failed without signaling Done, dispatchAndAwait blocked forever")
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
	seedFulfillmentAndDeployment(t, store, "rm-attested", domain.FulfillmentSnapshot{
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
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "old1", Name: "old1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "new1", Name: "new1", Type: "test"}),
	)

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	simple := &simpleRecord{ctx: testContext(t), events: events}
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
	seedFulfillmentAndDeployment(t, store, "del-attested", domain.FulfillmentSnapshot{
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
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}),
		domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t2", Name: "t2", Type: "test"}),
	)
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-attested:t1", FulfillmentID: domain.FulfillmentID("del-attested"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-attested:t2", FulfillmentID: domain.FulfillmentID("del-attested"), TargetID: "t2",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	simple := &simpleRecord{ctx: testContext(t), events: events}
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
	// row; the DeleteDeploymentCleanupWorkflow deletes both rows after
	// receiving the signal.
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	f, err := tx.Fulfillments().Get(context.Background(), domain.FulfillmentID("del-attested"))
	if err != nil {
		t.Fatalf("expected fulfillment to still exist after orchestration cleanup, got: %v", err)
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("fulfillment state = %q, want deleting", f.State())
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

	seedFulfillmentAndDeployment(t, store, "del-no-prov", domain.FulfillmentSnapshot{
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

	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "del-no-prov:t1", FulfillmentID: domain.FulfillmentID("del-no-prov"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	simple := &simpleRecord{ctx: testContext(t), events: events}
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

func (o *faultArmingObserver) ProcessOutputsStarted(ctx context.Context) (context.Context, domain.ProcessOutputsProbe) {
	return ctx, &faultArmingOutputsProbe{store: o.store}
}

type faultArmingOutputsProbe struct {
	domain.NoOpProcessOutputsProbe
	store      *commitFaultStore
	hadOutputs bool
}

func (p *faultArmingOutputsProbe) TargetsRegistered(_ int) {
	p.hadOutputs = true
}

func (p *faultArmingOutputsProbe) End() {
	if p.hadOutputs {
		p.store.Arm()
	}
}

// ---------------------------------------------------------------------------
// Replay-safety regression
// ---------------------------------------------------------------------------

func TestOrchestration_DeliveryOutputs_ReplayAfterTransientFailure_ErrAlreadyExists(t *testing.T) {
	realStore, vault := setupStore(t)
	seedFulfillmentAndDeployment(t, realStore, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{"name":"new-cluster"}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"provisioner"}},
		State:             domain.FulfillmentStateCreating,
	})

	seedTargets(t, realStore, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "provisioner", Name: "provisioner", Type: "test"}))

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
		deliveryAgent.events = events
		wf := newTestWorkflow(store, deliveryAgent, events, func(wf *domain.OrchestrationWorkflowSpec) {
			wf.Vault = vault
			wf.Observer = obs
		})
		rec := &simpleRecord{ctx: testContext(t), events: events}
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
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active after replay recovery", dep.State())
	}
}

// ---------------------------------------------------------------------------
// ResolvedTargets preservation across failure/auth-pause
// ---------------------------------------------------------------------------

func TestOrchestration_AuthFailure_PreservesResolvedTargets(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, authFailingDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if !dep.Paused() {
		t.Fatalf("expected fulfillment to be paused after auth failure")
	}
	if dep.State() != domain.FulfillmentStateCreating {
		t.Fatalf("State = %q, want creating (pause must preserve lifecycle state)", dep.State())
	}
	if len(dep.ResolvedTargets()) != 1 || dep.ResolvedTargets()[0] != "t1" {
		t.Errorf("ResolvedTargets = %v, want [t1]; placement resolved before auth failure so targets must be preserved", dep.ResolvedTargets())
	}
}

func TestOrchestration_DeleteAfterAuthPause_CallsRemove(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	// Phase 1: delivery auth-fails → PausedAuth with ResolvedTargets preserved.
	ctx := testContext(t)
	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, authFailingDelivery{events: events}, events)
	rec := &simpleRecord{ctx: ctx, events: events}
	if _, err := wf.Run(rec, domain.FulfillmentID("d1")); err != nil {
		t.Fatalf("Run (create): %v", err)
	}

	// Phase 2: transition to deleting (simulates user calling Delete).
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f, err := tx.Fulfillments().Get(ctx, domain.FulfillmentID("d1"))
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	f.TransitionToDeleting(f.Auth())
	if err := tx.Fulfillments().Update(ctx, f); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Phase 3: run orchestration again — should call Remove on t1.
	events2 := make(chan domain.FulfillmentEvent, 16)
	removeAgent := &recordingRemoveDelivery{events: events2}
	wf2 := newTestWorkflow(store, removeAgent, events2)
	rec2 := &simpleRecord{ctx: ctx, events: events2}
	recorder := &recordingRecord{ctx: rec2.ctx, delegate: rec2}
	if _, err := wf2.Run(recorder, domain.FulfillmentID("d1")); err != nil {
		t.Fatalf("Run (delete): %v", err)
	}

	removeAgent.mu.Lock()
	removed := removeAgent.removed
	removeAgent.mu.Unlock()

	if len(removed) != 1 || removed[0] != "t1" {
		t.Errorf("Remove called for targets %v, want [t1]; delete must clean up targets resolved before auth pause", removed)
	}
}

// recordingRemoveDelivery tracks which targets had Remove called.
type recordingRemoveDelivery struct {
	events  chan<- domain.FulfillmentEvent
	mu      sync.Mutex
	removed []domain.TargetID
}

func (d *recordingRemoveDelivery) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (d *recordingRemoveDelivery) Remove(_ context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	d.mu.Lock()
	d.removed = append(d.removed, target.ID())
	d.mu.Unlock()
	go func() {
		d.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func TestOrchestration_DeletePipeline_AuthExpired_SetsPausedAuth(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	authErr := fmt.Errorf("%w: STS token exchange failed: invalid_grant", domain.ErrAuthExpired)
	wf := newTestWorkflow(store, &failingRemoveDelivery{events: events, err: authErr}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if !dep.Paused() {
		t.Errorf("expected fulfillment to be paused after auth failure")
	}
	if dep.State() != domain.FulfillmentStateDeleting {
		t.Errorf("State = %q, want deleting (pause must preserve lifecycle state)", dep.State())
	}
	if len(dep.ResolvedTargets()) != 1 || dep.ResolvedTargets()[0] != "t1" {
		t.Errorf("ResolvedTargets = %v, want [t1]; targets must be preserved across auth pause", dep.ResolvedTargets())
	}
}

// ---------------------------------------------------------------------------
// Delete-pause-resume: state preserved, resume takes delete path
// ---------------------------------------------------------------------------

// TestOrchestration_DeletePausedResume_StaysDeleting verifies that a
// fulfillment paused during the delete pipeline correctly re-enters
// the delete path (not the create/update path) after resume.
func TestOrchestration_DeletePausedResume_StaysDeleting(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        2,
		ResolvedTargets:   []domain.TargetID{"t1"},
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateDeleting,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		State:     domain.DeliveryStateDelivered,
	}))

	ctx := testContext(t)

	// Phase 1: orchestration attempts delete, auth fails → paused.
	events1 := make(chan domain.FulfillmentEvent, 16)
	authErr := fmt.Errorf("%w: STS token exchange failed: invalid_grant", domain.ErrAuthExpired)
	wf1 := newTestWorkflow(store, &failingRemoveDelivery{events: events1, err: authErr}, events1)
	rec1 := &simpleRecord{ctx: ctx, events: events1}
	if _, err := wf1.Run(rec1, domain.FulfillmentID("d1")); err != nil {
		t.Fatalf("Run (delete attempt): %v", err)
	}

	f := getFulfillment(t, store, "d1")
	if !f.Paused() {
		t.Fatalf("expected fulfillment to be paused after auth failure")
	}
	if f.State() != domain.FulfillmentStateDeleting {
		t.Fatalf("State = %q, want %q; pause must not overwrite lifecycle state",
			f.State(), domain.FulfillmentStateDeleting)
	}

	// Phase 2: resume with fresh auth.
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fPtr, err := tx.Fulfillments().Get(ctx, domain.FulfillmentID("d1"))
	if err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := fPtr.Resume(domain.DeliveryAuth{Token: "fresh-token"}, nil); err != nil {
		tx.Rollback()
		t.Fatalf("Resume: %v", err)
	}
	if err := tx.Fulfillments().Update(ctx, fPtr); err != nil {
		tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	// Phase 3: re-run orchestration. It must take the delete path (Remove),
	// not the default create/update path (Deliver).
	events2 := make(chan domain.FulfillmentEvent, 16)
	removeAgent := &recordingRemoveDelivery{events: events2}
	wf2 := newTestWorkflow(store, removeAgent, events2)
	rec2 := &simpleRecord{ctx: ctx, events: events2}
	if _, err := wf2.Run(rec2, domain.FulfillmentID("d1")); err != nil {
		t.Fatalf("Run (after resume): %v", err)
	}

	removeAgent.mu.Lock()
	removed := removeAgent.removed
	removeAgent.mu.Unlock()

	if len(removed) != 1 || removed[0] != "t1" {
		t.Errorf("Remove called for targets %v, want [t1]; resumed delete must take the delete path", removed)
	}

	f = getFulfillment(t, store, "d1")
	if f.State() != domain.FulfillmentStateDeleting {
		t.Errorf("final State = %q, want deleting", f.State())
	}
	if f.Paused() {
		t.Errorf("fulfillment should not be paused after successful reconciliation")
	}
}

// ---------------------------------------------------------------------------
// Async delivery failure → reset-to-Pending → retry
// ---------------------------------------------------------------------------

func TestOrchestration_DeliverTerminalDelivery_ResetsAndRedispatches(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStateFailed,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active (terminal delivery should be reset and re-dispatched)", dep.State())
	}
}

// TestOrchestration_DeliverProgressingDelivery_WaitsForCompletion seeds a
// delivery in Progressing state at the same generation (acked but not
// completed — as happens after a crash) and verifies that DeliverToTarget
// does NOT re-dispatch. Instead dispatchAndAwait waits for the completion
// signal from RecoverActiveDeliveries.
func TestOrchestration_DeliverProgressingDelivery_WaitsForCompletion(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))
	seedDelivery(t, store, domain.DeliveryFromSnapshot(domain.DeliverySnapshot{
		ID: "d1:t1", FulfillmentID: domain.FulfillmentID("d1"), TargetID: "t1",
		Manifests:  []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
		Generation: 1,
		State:      domain.DeliveryStateProgressing,
	}))

	events := make(chan domain.FulfillmentEvent, 16)
	var deliverCalled bool
	delivery := &deliveryStateChecker{
		store:  store,
		events: events,
		onRemove: func(_ domain.DeliveryID) {
			// Remove should not be called for a create flow
			t.Fatal("unexpected Remove call")
		},
	}
	// Wrap Deliver to detect re-dispatch
	agent := &deliverCallTracker{inner: delivery, called: &deliverCalled}

	wf := newTestWorkflow(store, agent, events)

	// Simulate RecoverActiveDeliveries sending the completion signal.
	go func() {
		events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: "d1:t1",
				Generation: 1,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("d1"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if deliverCalled {
		t.Error("Deliver was called — Progressing delivery should wait for completion signal, not re-dispatch")
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active", dep.State())
	}

	// Verify delivery was NOT reset to Pending — it should still be
	// Progressing because orchestration waited for the signal instead
	// of mutating the delivery state.
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	d, err := tx.Deliveries().Get(context.Background(), "d1:t1")
	if err != nil {
		t.Fatalf("get delivery: %v", err)
	}
	if d.State() != domain.DeliveryStateProgressing {
		t.Errorf("delivery state = %q, want %q (should not be reset to Pending)", d.State(), domain.DeliveryStateProgressing)
	}
}

// deliverCallTracker wraps a DeliveryAgent and tracks whether Deliver was called.
type deliverCallTracker struct {
	inner  domain.DeliveryAgent
	called *bool
}

func (d *deliverCallTracker) Deliver(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, attestation *domain.Attestation, generation domain.Generation) error {
	*d.called = true
	return d.inner.Deliver(ctx, target, deliveryID, manifests, auth, attestation, generation)
}

func (d *deliverCallTracker) Remove(ctx context.Context, target domain.TargetInfo, deliveryID domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, attestation *domain.Attestation, generation domain.Generation) error {
	return d.inner.Remove(ctx, target, deliveryID, manifests, auth, attestation, generation)
}

func TestOrchestration_AsyncDeliveryFailure_ConvergesViaRetry(t *testing.T) {
	store, _ := setupStore(t)
	seedFulfillmentAndDeployment(t, store, "d1", domain.FulfillmentSnapshot{
		Generation:        1,
		ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
		PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
		State:             domain.FulfillmentStateCreating,
	})
	seedTargets(t, store, domain.TargetInfoFromSnapshot(domain.TargetInfoSnapshot{ID: "t1", Name: "t1", Type: "test"}))

	agent := &asyncFailOnceThenSucceedAgent{store: store}

	const maxAttempts = 3
	var lastErr error
	for attempt := range maxAttempts {
		events := make(chan domain.FulfillmentEvent, 16)
		agent.events = events
		wf := newTestWorkflow(store, agent, events)
		rec := &simpleRecord{ctx: testContext(t), events: events}
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
		t.Fatalf("workflow did not converge after %d attempts: %v", maxAttempts, lastErr)
	}

	dep := getFulfillment(t, store, "d1")
	if dep.State() != domain.FulfillmentStateActive {
		t.Errorf("State = %q, want active (async failure should be retried via reset-to-Pending)", dep.State())
	}
}

// asyncFailOnceThenSucceedAgent simulates a delivery agent that fails
// on the first attempt (transitioning the delivery to Failed in the DB
// and signaling the workflow, like production's [DeliveryReportService])
// then succeeds on subsequent attempts. This tests the full ContinueAsNew
// recovery path: first run fails → ContinueAsNew → second run resets
// the terminal delivery to Pending → re-dispatches → addon succeeds.
type asyncFailOnceThenSucceedAgent struct {
	store  domain.Store
	events chan<- domain.FulfillmentEvent
	mu     sync.Mutex
	calls  int
}

func (a *asyncFailOnceThenSucceedAgent) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	a.mu.Lock()
	a.calls++
	n := a.calls
	a.mu.Unlock()

	if n == 1 {
		ctx := context.Background()
		tx, err := a.store.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		d, err := tx.Deliveries().Get(ctx, deliveryID)
		if err != nil {
			return err
		}
		if err := d.TransitionTo(domain.DeliveryStateFailed, time.Now()); err != nil {
			return err
		}
		if err := tx.Deliveries().Put(ctx, d); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		go func() {
			a.events <- domain.FulfillmentEvent{
				DeliveryCompleted: &domain.DeliveryCompletionEvent{
					DeliveryID: deliveryID,
					Generation: generation,
					Result: domain.DeliveryResult{
						State:   domain.DeliveryStateFailed,
						Message: "async delivery failed",
					},
				},
			}
		}()
		return nil
	}

	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

func (a *asyncFailOnceThenSucceedAgent) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		a.events <- domain.FulfillmentEvent{
			DeliveryCompleted: &domain.DeliveryCompletionEvent{
				DeliveryID: deliveryID,
				Generation: generation,
				Result:     domain.DeliveryResult{State: domain.DeliveryStateDelivered},
			},
		}
	}()
	return nil
}

// ---------------------------------------------------------------------------
// Deleted fulfillment → workflow stops cleanly
// ---------------------------------------------------------------------------

func TestOrchestration_DeletedFulfillment_StopsCleanly(t *testing.T) {
	store, _ := setupStore(t)
	// No fulfillment seeded — simulates a hard-deleted record.
	// The workflow should terminate cleanly, not loop via ContinueAsNew.

	events := make(chan domain.FulfillmentEvent, 16)
	wf := newTestWorkflow(store, noopDelivery{events: events}, events)

	rec := &simpleRecord{ctx: testContext(t), events: events}
	_, err := wf.Run(rec, domain.FulfillmentID("nonexistent"))
	if err != nil {
		t.Fatalf("Run should return nil for deleted fulfillment, got: %v", err)
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
