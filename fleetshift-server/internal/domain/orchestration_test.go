package domain_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// recordingJournal wraps a [domain.Journal] and records activity
// names and target-related inputs so tests can assert execution sequence.
type recordingJournal struct {
	ctx      context.Context
	records  []activityRecord
	delegate domain.Journal
}

type activityRecord struct {
	Name string
	// TargetID is set for remove-from-target, generate-manifests, deliver-to-target.
	TargetID domain.TargetID
}

func (j *recordingJournal) ID() string              { return j.delegate.ID() }
func (j *recordingJournal) Context() context.Context { return j.ctx }

func (j *recordingJournal) Run(activity domain.Activity[any, any], in any) (any, error) {
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
	j.records = append(j.records, activityRecord{Name: name, TargetID: targetID})
	return j.delegate.Run(activity, in)
}

func (j *recordingJournal) activityNames() []string {
	names := make([]string, len(j.records))
	for i, rec := range j.records {
		names[i] = rec.Name
	}
	return names
}

// stubDeploymentRepo returns a fixed deployment for Get and accepts Update.
// It tracks the full update history for tests that need to assert
// intermediate states (e.g. Active before Deleting).
type stubDeploymentRepo struct {
	deployment domain.Deployment
	updated    *domain.Deployment
	updates    []domain.Deployment
}

func (s *stubDeploymentRepo) Create(_ context.Context, d domain.Deployment) error {
	s.deployment = d
	return nil
}

func (s *stubDeploymentRepo) Get(_ context.Context, id domain.DeploymentID) (domain.Deployment, error) {
	if id != s.deployment.ID {
		return domain.Deployment{}, domain.ErrNotFound
	}
	if s.updated != nil {
		return *s.updated, nil
	}
	return s.deployment, nil
}

func (s *stubDeploymentRepo) List(_ context.Context) ([]domain.Deployment, error) {
	return []domain.Deployment{s.deployment}, nil
}

func (s *stubDeploymentRepo) Update(_ context.Context, d domain.Deployment) error {
	s.updated = &d
	s.updates = append(s.updates, d)
	return nil
}

func (s *stubDeploymentRepo) Delete(_ context.Context, _ domain.DeploymentID) error { return nil }

// stubTargetRepo returns a fixed list for List.
type stubTargetRepo struct {
	targets []domain.TargetInfo
}

func (s *stubTargetRepo) Create(_ context.Context, t domain.TargetInfo) error {
	s.targets = append(s.targets, t)
	return nil
}

func (s *stubTargetRepo) Get(_ context.Context, id domain.TargetID) (domain.TargetInfo, error) {
	for _, t := range s.targets {
		if t.ID == id {
			return t, nil
		}
	}
	return domain.TargetInfo{}, domain.ErrNotFound
}

func (s *stubTargetRepo) List(_ context.Context) ([]domain.TargetInfo, error) {
	return s.targets, nil
}

func (s *stubTargetRepo) Delete(_ context.Context, _ domain.TargetID) error { return nil }

// stubStore implements domain.Store backed by in-memory stub repos.
type stubStore struct {
	deployments *stubDeploymentRepo
	targets     *stubTargetRepo
	deliveries  *stubDeliveryRepo
}

func (s *stubStore) Begin(_ context.Context) (domain.Tx, error) {
	return &stubTx{store: s}, nil
}

type stubTx struct {
	store *stubStore
}

func (t *stubTx) Targets() domain.TargetRepository        { return t.store.targets }
func (t *stubTx) Deployments() domain.DeploymentRepository { return t.store.deployments }
func (t *stubTx) Deliveries() domain.DeliveryRepository    { return t.store.deliveries }
func (t *stubTx) Inventory() domain.InventoryRepository    { return nil }
func (t *stubTx) Commit() error                            { return nil }
func (t *stubTx) Rollback() error                          { return nil }

// stubDeliveryRepo implements DeliveryRepository with an in-memory map.
type stubDeliveryRepo struct {
	deliveries map[domain.DeliveryID]domain.Delivery
}

func newStubDeliveryRepo() *stubDeliveryRepo {
	return &stubDeliveryRepo{deliveries: make(map[domain.DeliveryID]domain.Delivery)}
}

func (s *stubDeliveryRepo) Put(_ context.Context, d domain.Delivery) error {
	s.deliveries[d.ID] = d
	return nil
}

func (s *stubDeliveryRepo) Get(_ context.Context, id domain.DeliveryID) (domain.Delivery, error) {
	d, ok := s.deliveries[id]
	if !ok {
		return domain.Delivery{}, domain.ErrNotFound
	}
	return d, nil
}

func (s *stubDeliveryRepo) GetByDeploymentTarget(_ context.Context, depID domain.DeploymentID, tgtID domain.TargetID) (domain.Delivery, error) {
	for _, d := range s.deliveries {
		if d.DeploymentID == depID && d.TargetID == tgtID {
			return d, nil
		}
	}
	return domain.Delivery{}, domain.ErrNotFound
}

func (s *stubDeliveryRepo) ListByDeployment(_ context.Context, depID domain.DeploymentID) ([]domain.Delivery, error) {
	var result []domain.Delivery
	for _, d := range s.deliveries {
		if d.DeploymentID == depID {
			result = append(result, d)
		}
	}
	return result, nil
}

func (s *stubDeliveryRepo) DeleteByDeployment(_ context.Context, depID domain.DeploymentID) error {
	for id, d := range s.deliveries {
		if d.DeploymentID == depID {
			delete(s.deliveries, id)
		}
	}
	return nil
}

// noopDelivery implements DeliveryService with no-op Deliver and Remove.
// It calls signaler.Done synchronously so that the workflow's
// awaitDeliveries loop can proceed.
type noopDelivery struct{}

func (noopDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	result := domain.DeliveryResult{State: domain.DeliveryStateDelivered}
	signaler.Done(ctx, result)
	return result, nil
}

func (noopDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

// singleEventJournal is a minimal Journal that runs activities
// synchronously. awaitEvent delivers one scripted event and then
// signals delete. The extra channel receives delivery-completion
// events injected via the workflow's SignalDeploymentEvent field;
// they are drained before the scripted event sequence.
type singleEventJournal struct {
	ctx       context.Context
	event     domain.DeploymentEvent
	delivered bool
	extra     chan domain.DeploymentEvent
}

func (j *singleEventJournal) ID() string              { return "test-single" }
func (j *singleEventJournal) Context() context.Context { return j.ctx }
func (j *singleEventJournal) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(j.ctx, in)
}

func (j *singleEventJournal) awaitEvent() (domain.DeploymentEvent, error) {
	select {
	case e := <-j.extra:
		return e, nil
	default:
	}
	if !j.delivered {
		j.delivered = true
		return j.event, nil
	}
	return domain.DeploymentEvent{Delete: true}, nil
}

func (j *singleEventJournal) signal(_ context.Context, _ domain.DeploymentID, event domain.DeploymentEvent) error {
	j.extra <- event
	return nil
}

func TestOrchestration_RemoveStepsRunBeforeDeliverSteps(t *testing.T) {
	deploymentID := domain.DeploymentID("d1")
	depRepo := &stubDeploymentRepo{
		deployment: domain.Deployment{
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
			RolloutStrategy: nil,
			State:           domain.DeploymentStateCreating,
		},
	}
	pool := []domain.TargetInfo{
		{ID: "old1"},
		{ID: "new1"},
		{ID: "new2"},
	}

	targetRepo := &stubTargetRepo{targets: pool}

	sej := &singleEventJournal{
		ctx:   context.Background(),
		event: domain.DeploymentEvent{PoolChange: &domain.PoolChange{Set: pool}},
		extra: make(chan domain.DeploymentEvent, 16),
	}

	wf := &domain.OrchestrationWorkflow{
		Store:                 &stubStore{deployments: depRepo, targets: targetRepo, deliveries: newStubDeliveryRepo()},
		Delivery:              noopDelivery{},
		Strategies:            domain.DefaultStrategyFactory{},
		SignalDeploymentEvent: sej.signal,
	}

	recorder := &recordingJournal{ctx: sej.ctx, delegate: sej}

	_, err := wf.Run(recorder, sej.awaitEvent, deploymentID)
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
	deploymentID := domain.DeploymentID("d1")
	depRepo := &stubDeploymentRepo{
		deployment: domain.Deployment{
			ID:                deploymentID,
			ResolvedTargets:   nil,
			ManifestStrategy:  domain.ManifestStrategySpec{Type: domain.ManifestStrategyInline, Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}}},
			PlacementStrategy: domain.PlacementStrategySpec{Type: domain.PlacementStrategyStatic, Targets: []domain.TargetID{"t1"}},
			RolloutStrategy:   nil,
			State:             domain.DeploymentStateCreating,
		},
	}
	pool := []domain.TargetInfo{{ID: "t1"}}

	targetRepo := &stubTargetRepo{targets: pool}

	sej := &singleEventJournal{
		ctx:   context.Background(),
		event: domain.DeploymentEvent{PoolChange: &domain.PoolChange{Set: pool}},
		extra: make(chan domain.DeploymentEvent, 16),
	}

	wf := &domain.OrchestrationWorkflow{
		Store:                 &stubStore{deployments: depRepo, targets: targetRepo, deliveries: newStubDeliveryRepo()},
		Delivery:              noopDelivery{},
		Strategies:            domain.DefaultStrategyFactory{},
		SignalDeploymentEvent: sej.signal,
	}

	recorder := &recordingJournal{ctx: sej.ctx, delegate: sej}

	_, err := wf.Run(recorder, sej.awaitEvent, deploymentID)
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
	deploymentID := domain.DeploymentID("empty-pool")
	depRepo := &stubDeploymentRepo{
		deployment: domain.Deployment{
			ID: deploymentID,
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type: domain.PlacementStrategyAll,
			},
			State: domain.DeploymentStateCreating,
		},
	}

	targetRepo := &stubTargetRepo{targets: nil}

	journal := &singleEventJournal{
		ctx:   context.Background(),
		extra: make(chan domain.DeploymentEvent, 16),
	}

	wf := &domain.OrchestrationWorkflow{
		Store:                 &stubStore{deployments: depRepo, targets: targetRepo, deliveries: newStubDeliveryRepo()},
		Delivery:              noopDelivery{},
		Strategies:            domain.DefaultStrategyFactory{},
		SignalDeploymentEvent: journal.signal,
	}

	_, err := wf.Run(journal, journal.awaitEvent, deploymentID)
	if err == nil {
		t.Fatal("expected error from empty pool, got nil")
	}
	if !strings.Contains(err.Error(), "zero targets") {
		t.Errorf("error should mention zero targets, got: %v", err)
	}

	if depRepo.updated == nil {
		t.Fatal("deployment should have been updated to Failed state")
	}
	if depRepo.updated.State != domain.DeploymentStateFailed {
		t.Errorf("deployment state = %q, want %q", depRepo.updated.State, domain.DeploymentStateFailed)
	}
}

// asyncDelivery returns Accepted immediately and calls signaler.Done
// in a background goroutine, simulating how real delivery agents
// (e.g. kind) operate.
type asyncDelivery struct {
	done chan struct{}
}

func (a *asyncDelivery) Deliver(ctx context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ []domain.Manifest, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	go func() {
		signaler.Done(ctx, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
		if a.done != nil {
			close(a.done)
		}
	}()
	return domain.DeliveryResult{State: domain.DeliveryStateAccepted}, nil
}

func (asyncDelivery) Remove(_ context.Context, _ domain.TargetInfo, _ domain.DeliveryID, _ *domain.DeliverySignaler) error {
	return nil
}

// asyncJournal is a Journal for testing async delivery agents. awaitEvent
// blocks until a signal arrives on the events channel, then sends
// a Delete on the next call.
type asyncJournal struct {
	ctx    context.Context
	events chan domain.DeploymentEvent
	sawAll bool
}

func (j *asyncJournal) ID() string              { return "test-async" }
func (j *asyncJournal) Context() context.Context { return j.ctx }
func (j *asyncJournal) Run(activity domain.Activity[any, any], in any) (any, error) {
	return activity.Run(j.ctx, in)
}

func (j *asyncJournal) awaitEvent() (domain.DeploymentEvent, error) {
	if j.sawAll {
		return domain.DeploymentEvent{Delete: true}, nil
	}
	e := <-j.events
	return e, nil
}

func (j *asyncJournal) signal(_ context.Context, _ domain.DeploymentID, event domain.DeploymentEvent) error {
	j.events <- event
	if event.DeliveryCompleted != nil {
		j.sawAll = true
	}
	return nil
}

func TestOrchestration_AsyncDelivery_ReachesActive(t *testing.T) {
	deploymentID := domain.DeploymentID("async-test")
	depRepo := &stubDeploymentRepo{
		deployment: domain.Deployment{
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
		},
	}
	pool := []domain.TargetInfo{{ID: "t1"}}
	targetRepo := &stubTargetRepo{targets: pool}
	deliveryRepo := newStubDeliveryRepo()
	store := &stubStore{deployments: depRepo, targets: targetRepo, deliveries: deliveryRepo}
	asyncDel := &asyncDelivery{done: make(chan struct{})}

	journal := &asyncJournal{
		ctx:    context.Background(),
		events: make(chan domain.DeploymentEvent, 16),
	}

	wf := &domain.OrchestrationWorkflow{
		Store:                 store,
		Delivery:              asyncDel,
		Strategies:            domain.DefaultStrategyFactory{},
		SignalDeploymentEvent: journal.signal,
	}

	_, err := wf.Run(journal, journal.awaitEvent, deploymentID)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	<-asyncDel.done

	// The workflow transitions Active → (Delete event) → Deleting.
	// Reaching Deleting proves Active was reached, since Delete is
	// only processed from the Active event loop.
	sawActive := false
	for _, u := range depRepo.updates {
		if u.State == domain.DeploymentStateActive {
			sawActive = true
		}
	}
	if !sawActive {
		states := make([]string, len(depRepo.updates))
		for i, u := range depRepo.updates {
			states[i] = string(u.State)
		}
		t.Fatalf("workflow never reached Active; state transitions: %v", states)
	}
	if depRepo.updated.State != domain.DeploymentStateDeleting {
		t.Errorf("final deployment state = %q, want %q", depRepo.updated.State, domain.DeploymentStateDeleting)
	}

	did := domain.DeliveryID("async-test:t1")
	d, err := deliveryRepo.Get(context.Background(), did)
	if err != nil {
		t.Fatalf("delivery record not found: %v", err)
	}
	if d.State != domain.DeliveryStateDelivered {
		t.Errorf("delivery state = %q, want %q", d.State, domain.DeliveryStateDelivered)
	}
}
