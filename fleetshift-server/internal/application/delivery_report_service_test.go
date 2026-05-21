package application_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func setupDeliveryReportService(t *testing.T) (*application.DeliveryReportService, domain.Store, *recordingSignal, *testDeliveryObserver) {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	sig := &recordingSignal{}
	obs := &testDeliveryObserver{}
	svc := application.NewDeliveryReportService(store, sig, application.WithDeliveryObserver(obs))
	return svc, store, sig, obs
}

func seedDeliveryRecord(t *testing.T, store domain.Store, d domain.Delivery) {
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

func seedTarget(t *testing.T, store domain.Store, target domain.TargetInfo) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.Targets().Create(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func getDeliveryState(t *testing.T, store domain.Store, id domain.DeliveryID) domain.DeliveryState {
	t.Helper()
	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	d, err := tx.Deliveries().Get(context.Background(), id)
	if err != nil {
		t.Fatalf("get delivery %q: %v", id, err)
	}
	return d.State
}

// recordingSignal implements [domain.FulfillmentSignaler] and captures
// fulfillment events for test assertions.
type recordingSignal struct {
	mu     sync.Mutex
	events []signalRecord
}

type signalRecord struct {
	FulfillmentID domain.FulfillmentID
	Event         domain.FulfillmentEvent
}

func (r *recordingSignal) SignalFulfillmentEvent(_ context.Context, fID domain.FulfillmentID, event domain.FulfillmentEvent) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, signalRecord{FulfillmentID: fID, Event: event})
	return nil
}

func (r *recordingSignal) snapshot() []signalRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]signalRecord, len(r.events))
	copy(out, r.events)
	return out
}

// testDeliveryObserver captures observer calls.
type testDeliveryObserver struct {
	domain.NoOpDeliveryObserver
	mu      sync.Mutex
	events  []domain.DeliveryEvent
	results []domain.DeliveryResult
}

func (o *testDeliveryObserver) EventEmitted(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, event domain.DeliveryEvent) (context.Context, domain.EventEmittedProbe) {
	o.mu.Lock()
	o.events = append(o.events, event)
	o.mu.Unlock()
	return ctx, domain.NoOpEventEmittedProbe{}
}

func (o *testDeliveryObserver) Completed(ctx context.Context, _ domain.DeliveryID, _ domain.TargetInfo, result domain.DeliveryResult) (context.Context, domain.CompletedProbe) {
	o.mu.Lock()
	o.results = append(o.results, result)
	o.mu.Unlock()
	return ctx, domain.NoOpCompletedProbe{}
}

func (o *testDeliveryObserver) snapshotEvents() []domain.DeliveryEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]domain.DeliveryEvent, len(o.events))
	copy(out, o.events)
	return out
}

func (o *testDeliveryObserver) snapshotResults() []domain.DeliveryResult {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make([]domain.DeliveryResult, len(o.results))
	copy(out, o.results)
	return out
}

// ---------------------------------------------------------------------------
// ReportEvent tests
// ---------------------------------------------------------------------------

func TestDeliveryReportService_ReportEvent_TransitionsToProgressing(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfo{ID: "t1", Type: "test"})
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
	})

	err := svc.ReportEvent(ctx, "del-1", domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "step 1",
	})
	if err != nil {
		t.Fatalf("ReportEvent: %v", err)
	}

	state := getDeliveryState(t, store, "del-1")
	if state != domain.DeliveryStateProgressing {
		t.Errorf("state = %q, want %q", state, domain.DeliveryStateProgressing)
	}
}

func TestDeliveryReportService_ReportEvent_AlreadyProgressing_NoStateChange(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfo{ID: "t1", Type: "test"})
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	})

	err := svc.ReportEvent(ctx, "del-1", domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "step 2",
	})
	if err != nil {
		t.Fatalf("ReportEvent: %v", err)
	}

	state := getDeliveryState(t, store, "del-1")
	if state != domain.DeliveryStateProgressing {
		t.Errorf("state = %q, want %q", state, domain.DeliveryStateProgressing)
	}
}

func TestDeliveryReportService_ReportEvent_CallsObserver(t *testing.T) {
	svc, store, _, obs := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfo{ID: "t1", Type: "test"})
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
	})

	_ = svc.ReportEvent(ctx, "del-1", domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "applying",
	})

	events := obs.snapshotEvents()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Message != "applying" {
		t.Errorf("message = %q, want %q", events[0].Message, "applying")
	}
}

// ---------------------------------------------------------------------------
// ReportResult tests
// ---------------------------------------------------------------------------

func TestDeliveryReportService_ReportResult_UpdatesStateAndSignals(t *testing.T) {
	svc, store, sig, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfo{ID: "t1", Type: "test"})
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	})

	err := svc.ReportResult(ctx, "del-1", domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
	})
	if err != nil {
		t.Fatalf("ReportResult: %v", err)
	}

	state := getDeliveryState(t, store, "del-1")
	if state != domain.DeliveryStateDelivered {
		t.Errorf("state = %q, want %q", state, domain.DeliveryStateDelivered)
	}

	signals := sig.snapshot()
	if len(signals) != 1 {
		t.Fatalf("expected 1 signal, got %d", len(signals))
	}
	if signals[0].FulfillmentID != "f1" {
		t.Errorf("fulfillment ID = %q, want %q", signals[0].FulfillmentID, "f1")
	}
	completed := signals[0].Event.DeliveryCompleted
	if completed == nil {
		t.Fatal("expected DeliveryCompleted event")
	}
	if completed.DeliveryID != "del-1" {
		t.Errorf("delivery ID = %q, want %q", completed.DeliveryID, "del-1")
	}
	if completed.Result.State != domain.DeliveryStateDelivered {
		t.Errorf("result state = %q, want %q", completed.Result.State, domain.DeliveryStateDelivered)
	}
}

func TestDeliveryReportService_ReportResult_CallsObserver(t *testing.T) {
	svc, store, _, obs := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfo{ID: "t1", Type: "test"})
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	})

	_ = svc.ReportResult(ctx, "del-1", domain.DeliveryResult{
		State: domain.DeliveryStateFailed, Message: "connection refused",
	})

	results := obs.snapshotResults()
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].State != domain.DeliveryStateFailed {
		t.Errorf("state = %q, want %q", results[0].State, domain.DeliveryStateFailed)
	}
}

// ---------------------------------------------------------------------------
// ListActiveDeliveries tests
// ---------------------------------------------------------------------------

func seedFulfillment(t *testing.T, store domain.Store, f *domain.Fulfillment) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.Fulfillments().Create(ctx, f); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func newFulfillment(id domain.FulfillmentID, gen domain.Generation, auth domain.DeliveryAuth, now time.Time) *domain.Fulfillment {
	f := &domain.Fulfillment{
		ID:        id,
		Auth:      auth,
		CreatedAt: now,
		UpdatedAt: now,
	}
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type: domain.ManifestStrategyInline,
	}, now)
	f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
		Type: domain.PlacementStrategyStatic,
	}, now)
	f.AdvanceRolloutStrategy(nil, now)
	// AdvanceManifestStrategy + AdvancePlacementStrategy + AdvanceRolloutStrategy
	// each call BumpGeneration, so generation is 3 after construction.
	// Adjust to the desired generation.
	for f.Generation < gen {
		f.BumpGeneration()
	}
	return f
}

func TestDeliveryReportService_ListActiveDeliveries(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	auth1 := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://idp.test"}},
	}
	auth2 := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-2", Issuer: "https://idp.test"}},
	}

	seedTarget(t, store, domain.TargetInfo{ID: "t1", Type: "test", Name: "target-1"})
	seedTarget(t, store, domain.TargetInfo{ID: "t2", Type: "test", Name: "target-2"})
	seedFulfillment(t, store, newFulfillment("f1", 3, auth1, now))
	seedFulfillment(t, store, newFulfillment("f2", 3, auth2, now))
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-active-1", FulfillmentID: "f1", TargetID: "t1",
		Generation: 3,
		State:      domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	})
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-active-2", FulfillmentID: "f2", TargetID: "t2",
		Generation: 3,
		State:      domain.DeliveryStateAccepted, CreatedAt: now, UpdatedAt: now,
	})

	t.Run("nil targets returns all active", func(t *testing.T) {
		active, err := svc.ListActiveDeliveries(ctx, nil)
		if err != nil {
			t.Fatalf("ListActiveDeliveries(nil): %v", err)
		}
		if len(active) != 2 {
			t.Fatalf("expected 2 active deliveries, got %d", len(active))
		}
	})

	t.Run("filter by single target", func(t *testing.T) {
		active, err := svc.ListActiveDeliveries(ctx, []domain.TargetID{"t1"})
		if err != nil {
			t.Fatalf("ListActiveDeliveries([t1]): %v", err)
		}
		if len(active) != 1 {
			t.Fatalf("expected 1 active delivery, got %d", len(active))
		}
		if active[0].Delivery.ID != "del-active-1" {
			t.Errorf("ID = %q, want %q", active[0].Delivery.ID, "del-active-1")
		}
	})

	t.Run("filter by multiple targets", func(t *testing.T) {
		active, err := svc.ListActiveDeliveries(ctx, []domain.TargetID{"t1", "t2"})
		if err != nil {
			t.Fatalf("ListActiveDeliveries([t1,t2]): %v", err)
		}
		if len(active) != 2 {
			t.Fatalf("expected 2 active deliveries, got %d", len(active))
		}
	})

	t.Run("filter by unknown target returns empty", func(t *testing.T) {
		active, err := svc.ListActiveDeliveries(ctx, []domain.TargetID{"t-unknown"})
		if err != nil {
			t.Fatalf("ListActiveDeliveries([t-unknown]): %v", err)
		}
		if len(active) != 0 {
			t.Errorf("expected 0 active deliveries, got %d", len(active))
		}
	})

	t.Run("enriches target and auth", func(t *testing.T) {
		active, err := svc.ListActiveDeliveries(ctx, []domain.TargetID{"t1"})
		if err != nil {
			t.Fatalf("ListActiveDeliveries: %v", err)
		}
		if len(active) != 1 {
			t.Fatalf("expected 1, got %d", len(active))
		}
		ad := active[0]
		if ad.Target.ID != "t1" {
			t.Errorf("Target.ID = %q, want %q", ad.Target.ID, "t1")
		}
		if ad.Target.Name != "target-1" {
			t.Errorf("Target.Name = %q, want %q", ad.Target.Name, "target-1")
		}
		if ad.Auth.Caller == nil || ad.Auth.Caller.Subject != "user-1" {
			t.Errorf("Auth.Caller.Subject = %v, want user-1", ad.Auth.Caller)
		}
		if ad.Attestation != nil {
			t.Errorf("expected nil Attestation for unsigned fulfillment, got %v", ad.Attestation)
		}
	})
}

func TestDeliveryReportService_ListActiveDeliveries_Empty(t *testing.T) {
	svc, _, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()

	active, err := svc.ListActiveDeliveries(ctx, nil)
	if err != nil {
		t.Fatalf("ListActiveDeliveries: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected 0 active deliveries, got %d", len(active))
	}
}

func TestDeliveryReportService_ListActiveDeliveries_SkipsStale(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	auth := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-a", Issuer: "https://idp.test"}},
	}

	seedTarget(t, store, domain.TargetInfo{ID: "t1", Type: "test", Name: "target-1"})
	seedFulfillment(t, store, newFulfillment("f-stale", 5, auth, now))
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-stale", FulfillmentID: "f-stale", TargetID: "t1",
		Generation: 3, // older than fulfillment gen 5
		State:      domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	})

	active, err := svc.ListActiveDeliveries(ctx, nil)
	if err != nil {
		t.Fatalf("ListActiveDeliveries: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected stale delivery to be filtered, got %d", len(active))
	}
}

func TestDeliveryReportService_ListActiveDeliveries_SkipsMissingFulfillment(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfo{ID: "t1", Type: "test"})
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-orphan", FulfillmentID: "f-missing", TargetID: "t1",
		Generation: 1,
		State:      domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	})

	active, err := svc.ListActiveDeliveries(ctx, nil)
	if err != nil {
		t.Fatalf("ListActiveDeliveries: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected delivery with missing fulfillment to be filtered, got %d", len(active))
	}
}

func TestDeliveryReportService_ListActiveDeliveries_SkipsMissingTarget(t *testing.T) {
	svc, store, _, _ := setupDeliveryReportService(t)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)
	auth := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-a", Issuer: "https://idp.test"}},
	}

	seedFulfillment(t, store, newFulfillment("f1", 3, auth, now))
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-no-target", FulfillmentID: "f1", TargetID: "t-gone",
		Generation: 3,
		State:      domain.DeliveryStateProgressing, CreatedAt: now, UpdatedAt: now,
	})

	active, err := svc.ListActiveDeliveries(ctx, nil)
	if err != nil {
		t.Fatalf("ListActiveDeliveries: %v", err)
	}
	if len(active) != 0 {
		t.Errorf("expected delivery with missing target to be filtered, got %d", len(active))
	}
}

// ---------------------------------------------------------------------------
// NilObserver tests
// ---------------------------------------------------------------------------

func TestDeliveryReportService_NilObserver_DoesNotPanic(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}
	sig := &recordingSignal{}
	svc := application.NewDeliveryReportService(store, sig)

	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	seedTarget(t, store, domain.TargetInfo{ID: "t1", Type: "test"})
	seedDeliveryRecord(t, store, domain.Delivery{
		ID: "del-1", FulfillmentID: "f1", TargetID: "t1",
		State: domain.DeliveryStatePending, CreatedAt: now, UpdatedAt: now,
	})

	if err := svc.ReportEvent(ctx, "del-1", domain.DeliveryEvent{
		Kind: domain.DeliveryEventProgress, Message: "ok",
	}); err != nil {
		t.Fatalf("ReportEvent: %v", err)
	}

	if err := svc.ReportResult(ctx, "del-1", domain.DeliveryResult{
		State: domain.DeliveryStateDelivered,
	}); err != nil {
		t.Fatalf("ReportResult: %v", err)
	}
}
