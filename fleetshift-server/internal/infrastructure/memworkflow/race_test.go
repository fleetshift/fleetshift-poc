package memworkflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// raceTestOrchestration creates a minimal orchestration workflow backed by an
// in-memory SQLite store, a recording delivery agent, and a single target.
// Returns the registered workflow and the seeded fulfillment ID.
func raceTestOrchestration(t *testing.T) (domain.OrchestrationWorkflow, domain.FulfillmentID) {
	t.Helper()

	db := sqlite.OpenTestDB(t)
	store := &sqlite.Store{DB: db}

	recordingAgent := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 2, 27, 12, 0, 0, 0, time.UTC) },
	}
	router := delivery.NewRoutingDeliveryService()
	router.Register("test", recordingAgent)

	reg := &memworkflow.Registry{}

	reporter := application.NewDeliveryReportService(store, reg)
	recordingAgent.Reporter = reporter

	orchSpec := &domain.OrchestrationWorkflowSpec{
		Store:            store,
		Delivery:         router,
		Strategies:       domain.StrategyFactory{Store: store},
		CleanupSignaler:  reg,
		AckRetryInterval: 5 * time.Second,
	}
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	ctx := context.Background()

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if err := tx.Targets().Create(ctx, domain.TargetInfo{
		ID: "t1", Type: "test", Name: "cluster-t1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	fID := domain.FulfillmentID(uuid.New().String())
	f := domain.Fulfillment{
		ID:        fID,
		State:     domain.FulfillmentStateCreating,
		CreatedAt: now,
		UpdatedAt: now,
	}
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{Raw: json.RawMessage(`{}`)}},
	}, now)
	f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}, now)
	f.AdvanceRolloutStrategy(nil, now)

	tx2, err := store.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx2.Rollback()
	if err := tx2.Fulfillments().Create(ctx, &f); err != nil {
		t.Fatal(err)
	}
	if err := tx2.Commit(); err != nil {
		t.Fatal(err)
	}

	return orchWf, fID
}

// TestCleanupBeforeResult exercises the Start → AwaitResult → Start cycle
// rapidly. With the race bug, the previous workflow's running-map entry
// has not been cleaned up by the time AwaitResult returns, causing the
// next Start to return ErrAlreadyRunning.
func TestCleanupBeforeResult(t *testing.T) {
	orchWf, fID := raceTestOrchestration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := range 50 {
		exec, err := orchWf.Start(ctx, fID)
		if err != nil {
			t.Fatalf("iteration %d: Start: %v", i, err)
		}
		if _, err := exec.AwaitResult(ctx); err != nil {
			t.Fatalf("iteration %d: AwaitResult: %v", i, err)
		}
	}
}

// TestDeferDoesNotClobberNewStart verifies that the cleanup from a
// completed workflow does not delete a running-map entry created by a
// subsequent Start for the same ID.
func TestDeferDoesNotClobberNewStart(t *testing.T) {
	orchWf, fID := raceTestOrchestration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Run the first workflow to completion.
	exec1, err := orchWf.Start(ctx, fID)
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}
	if _, err := exec1.AwaitResult(ctx); err != nil {
		t.Fatalf("first AwaitResult: %v", err)
	}

	// Start a second workflow immediately. With the race bug, this may
	// return ErrAlreadyRunning due to stale cleanup.
	exec2, err := orchWf.Start(ctx, fID)
	if err != nil {
		t.Fatalf("second Start: %v", err)
	}

	// A third Start while the second is still running must be rejected.
	_, err = orchWf.Start(ctx, fID)
	if !errors.Is(err, domain.ErrAlreadyRunning) {
		t.Fatalf("third Start: got %v, want ErrAlreadyRunning", err)
	}

	// Drain the second workflow so it doesn't leak.
	if _, err := exec2.AwaitResult(ctx); err != nil {
		t.Fatalf("second AwaitResult: %v", err)
	}
}

// TestConcurrentStartAfterComplete hammers Start/AwaitResult from
// multiple goroutines to surface data races under -race.
func TestConcurrentStartAfterComplete(t *testing.T) {
	orchWf, fID := raceTestOrchestration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Warm up: run once so the fulfillment is in Active state.
	exec, err := orchWf.Start(ctx, fID)
	if err != nil {
		t.Fatalf("warmup Start: %v", err)
	}
	if _, err := exec.AwaitResult(ctx); err != nil {
		t.Fatalf("warmup AwaitResult: %v", err)
	}

	const goroutines = 5
	const iterations = 20
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*iterations)

	for g := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range iterations {
				exec, err := orchWf.Start(ctx, fID)
				if errors.Is(err, domain.ErrAlreadyRunning) {
					continue
				}
				if err != nil {
					errs <- fmt.Errorf("goroutine %d iter %d: Start: %w", g, i, err)
					return
				}
				if _, err := exec.AwaitResult(ctx); err != nil {
					errs <- fmt.Errorf("goroutine %d iter %d: AwaitResult: %w", g, i, err)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
