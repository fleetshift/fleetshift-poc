//go:build stress

package application_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	wfpostgres "github.com/cschleiden/go-workflows/backend/postgres"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/goworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
)

// ---------------------------------------------------------------------------
// Direct store mutation for deployment "updates"
// ---------------------------------------------------------------------------

// updateDeploymentManifest directly mutates the fulfillment's manifest
// strategy in the store (bypassing the service layer) and then kicks
// off an async convergence loop to ensure orchestration picks up the
// new generation.
//
// The mutation may race with concurrent orchestration activities that
// read/write the same fulfillment row (e.g. duplicate key on manifest
// strategy insert). These conflicts are transient and self-resolving,
// so the mutation is retried up to maxMutationRetries times.
//
// The two-step approach mirrors domain mutation workflows (e.g.
// ResumeDeploymentWorkflowSpec): commit the mutation first, then use a
// convergence loop that handles the race where orchestration may
// already be running. Like real mutation workflows, the convergence
// runs asynchronously — the caller does not block on orchestration
// start. The goroutine exits when the generation is reconciled,
// superseded, or the context is cancelled.
func updateDeploymentManifest(
	ctx context.Context,
	t *testing.T,
	observer *stressObserver,
	store domain.Store,
	orchWf domain.OrchestrationWorkflow,
	depName domain.ResourceName,
	newManifest domain.Manifest,
) error {
	// Step 1: Mutate and commit (synchronous, with retry).
	const maxMutationRetries = 5
	var fID domain.FulfillmentID
	var myGen domain.Generation
	var err error
	for attempt := 0; attempt <= maxMutationRetries; attempt++ {
		fID, myGen, err = mutateManifest(ctx, store, depName, newManifest)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return err // context cancelled, no point retrying
		}
		// Brief backoff before retrying transient conflicts.
		select {
		case <-time.After(50 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err != nil {
		return err
	}

	observer.MutationRecorded(fID, myGen, time.Now())

	// Step 2: Convergence loop (async) — mirrors how real mutation
	// workflows run convergence inside the durable workflow engine.
	// Superseded generations exit immediately (Generation > myGen),
	// so at most one goroutine per fulfillment stays active.
	go func() {
		if err := stressConvergenceLoop(ctx, store, orchWf, fID, myGen); err != nil {
			// Context cancellation is expected during test shutdown.
			if ctx.Err() != nil {
				return
			}
			n := observer.convergenceErrors.Add(1)
			if n <= 5 {
				t.Logf("convergence(%s, gen=%d): %v", fID, myGen, err)
			}
		}
	}()
	return nil
}

// mutateManifest advances the fulfillment's manifest strategy in a
// single transaction, returning the fulfillment ID and the new
// generation that was written.
func mutateManifest(
	ctx context.Context,
	store domain.Store,
	depName domain.ResourceName,
	newManifest domain.Manifest,
) (domain.FulfillmentID, domain.Generation, error) {
	tx, err := store.Begin(ctx)
	if err != nil {
		return "", 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, depName)
	if err != nil {
		return "", 0, fmt.Errorf("get deployment %s: %w", depName, err)
	}

	f, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID())
	if err != nil {
		return "", 0, fmt.Errorf("get fulfillment: %w", err)
	}

	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{newManifest},
	}, time.Now())

	if err := tx.Fulfillments().Update(ctx, f); err != nil {
		return "", 0, fmt.Errorf("update fulfillment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return "", 0, fmt.Errorf("commit: %w", err)
	}
	return dep.FulfillmentID(), f.Generation(), nil
}

// stressConvergenceLoop mirrors domain.convergenceLoop for use outside
// the durable workflow engine. It ensures orchestration eventually
// reconciles (or supersedes) the generation written by a mutation.
//
// See domain/mutation_workflow.go for the canonical implementation.
func stressConvergenceLoop(
	ctx context.Context,
	store domain.Store,
	orchWf domain.OrchestrationWorkflow,
	fID domain.FulfillmentID,
	myGen domain.Generation,
) error {
	for {
		// Load fulfillment to check convergence state.
		tx, err := store.BeginReadOnly(ctx)
		if err != nil {
			return fmt.Errorf("begin read tx: %w", err)
		}
		f, err := tx.Fulfillments().Get(ctx, fID)
		tx.Rollback()
		if err != nil {
			return fmt.Errorf("load fulfillment %q: %w", fID, err)
		}

		// Already reconciled to at least our generation.
		if f.ObservedGeneration() >= myGen {
			return nil
		}
		// A newer generation superseded ours; that mutation's
		// convergence loop is responsible for starting orchestration.
		if f.Generation() > myGen {
			return nil
		}

		// Try to start orchestration.
		_, err = orchWf.Start(ctx, fID)
		if err != nil && !errors.Is(err, domain.ErrAlreadyRunning) {
			return fmt.Errorf("start orchestration: %w", err)
		}
		if err == nil {
			return nil // successfully started
		}

		// Already running — wait and retry.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ---------------------------------------------------------------------------
// Workload types and runner
// ---------------------------------------------------------------------------

// depState tracks per-slot state during the workload.
type depState struct {
	created       bool
	version       int
	fulfillmentID domain.FulfillmentID
}

// workloadStats summarizes what the workload did.
type workloadStats struct {
	createCount  int
	updateCount  int
	createErrors int
	updateErrors int
	depStates    []depState
	maxVersion   int
	endedAt      time.Time // wall-clock time when the workload loop finished
}

// runStressWorkload drives the Pareto-distributed create/update loop.
// The loop fires exactly totalOps = rate × duration operations, then
// stops. STRESS_DEPLOYMENTS is the pool size — the Pareto distribution
// selects from it, but not every slot is guaranteed to be touched.
//
// Pareto (alpha=7.21) concentrates 80% of picks on the bottom 20% of
// indices, so hotspot deployments get updated repeatedly while their
// earlier generations are still in-flight.
func runStressWorkload(
	ctx context.Context,
	t *testing.T,
	cfg stressConfig,
	h stressHarness,
	observer *stressObserver,
	targetIDs []domain.TargetID,
	depNames []domain.ResourceName,
) workloadStats {
	t.Helper()

	totalOps := cfg.ratePerSecond * int(cfg.duration.Seconds())
	logEvery := totalOps / 10
	if logEvery < 1 {
		logEvery = 1
	}
	depStates := make([]depState, cfg.totalDeployments)
	var (
		createCount  int
		updateCount  int
		createErrors int
		updateErrors int
		createdSoFar int // sequential counter for round-robin target assignment
	)

	// α = ln(1−0.8) / ln(1−0.2) ≈ 7.21: 80% of selections hit the
	// bottom 20% of indices (hotspot deployments that get updated
	// repeatedly while their earlier generations are still in-flight).
	const paretoAlpha = 7.21

	t.Logf("starting main loop: %d ops at %d/s (Pareto alpha=%.2f across %d deployments)...",
		totalOps, cfg.ratePerSecond, paretoAlpha, cfg.totalDeployments)

	startTime := time.Now()

	ticker := time.NewTicker(time.Second / time.Duration(cfg.ratePerSecond))
	defer ticker.Stop()

workloop:
	for i := 0; i < totalOps; i++ {
		select {
		case <-ctx.Done():
			t.Logf("context expired at op %d/%d: creates=%d updates=%d", i, totalOps, createCount, updateCount)
			break workloop
		case <-ticker.C:
		}

		idx := paretoIndex(cfg.totalDeployments, paretoAlpha)
		depName := depNames[idx]

		if !depStates[idx].created {
			// --- Create ---
			target := targetIDs[createdSoFar%stressNumTargets]
			manifest := stressManifest(string(depName), 1)
			view, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
				Name: depName,
				ManifestStrategy: domain.ManifestStrategySpec{
					Type:      domain.ManifestStrategyInline,
					Manifests: []domain.Manifest{manifest},
				},
				PlacementStrategy: domain.PlacementStrategySpec{
					Type:    domain.PlacementStrategyStatic,
					Targets: []domain.TargetID{target},
				},
			})
			mutatedAt := time.Now()
			if err != nil {
				createErrors++
				if createErrors <= 5 {
					t.Logf("Create(%s): %v", depName, err)
				}
				continue
			}
			observer.MutationRecorded(view.Fulfillment.ID(), domain.Generation(1), mutatedAt)
			depStates[idx] = depState{
				created:       true,
				version:       1,
				fulfillmentID: view.Fulfillment.ID(),
			}
			createCount++
			createdSoFar++
		} else {
			// --- Update (direct store mutation) ---
			depStates[idx].version++
			manifest := stressManifest(string(depName), depStates[idx].version)
			if err := updateDeploymentManifest(ctx, t, observer, h.store, h.orchWf, depName, manifest); err != nil {
				updateErrors++
				if updateErrors <= 5 {
					t.Logf("Update(%s): %v", depName, err)
				}
				continue
			}
			updateCount++
		}

		if (i+1)%logEvery == 0 || i+1 == totalOps {
			t.Logf("  ops %d/%d (creates=%d, updates=%d, convergence_errors=%d)",
				i+1, totalOps, createCount, updateCount,
				observer.convergenceErrors.Load())
		}
	}

	elapsed := time.Since(startTime)

	t.Logf("operation phase complete in %v: creates=%d (errors=%d), updates=%d (errors=%d), total_ops=%d",
		elapsed, createCount, createErrors, updateCount, updateErrors, totalOps)

	maxVersion := 0
	for _, ds := range depStates {
		if ds.version > maxVersion {
			maxVersion = ds.version
		}
	}

	return workloadStats{
		createCount:  createCount,
		updateCount:  updateCount,
		createErrors: createErrors,
		updateErrors: updateErrors,
		depStates:    depStates,
		maxVersion:   maxVersion,
		endedAt:      time.Now(),
	}
}

// ---------------------------------------------------------------------------
// Drain phase
// ---------------------------------------------------------------------------

// drainFulfillments waits until the observer has recorded at least one
// completed run for every unique fulfillment. The caller should adjust
// observer.target to the actual number of created deployments before
// calling this function.
func drainFulfillments(ctx context.Context, t *testing.T, observer *stressObserver) {
	t.Helper()

	observer.mu.Lock()
	target := observer.target
	observer.mu.Unlock()

	t.Logf("awaiting reconciliation of %d fulfillments...", target)
	select {
	case <-observer.done:
		t.Logf("all %d fulfillments reconciled at least once", target)
	case <-ctx.Done():
		observer.mu.Lock()
		t.Logf("timeout: %d/%d fulfillments reconciled", len(observer.completedFulfillments), target)
		observer.mu.Unlock()
	}
}

// ---------------------------------------------------------------------------
// Stress test
// ---------------------------------------------------------------------------

func TestStress_OrchestrationLoop(t *testing.T) {
	cfg := loadStressConfig()

	totalOps := cfg.ratePerSecond * int(cfg.duration.Seconds())
	t.Logf("config: deployments=%d rate=%d/s duration=%v total_ops=%d",
		cfg.totalDeployments, cfg.ratePerSecond, cfg.duration, totalOps)
	t.Logf("  ack delay:        %v..%v (gaussian)", cfg.ackDelayMin, cfg.ackDelayMax)
	t.Logf("  completion delay: %v..%v (gaussian)", cfg.completionDelayMin, cfg.completionDelayMax)
	t.Logf("  failure rate:     %.1f%%", cfg.failureRate*100)
	t.Logf("  pool budget:      %d (app=%d, wf=%d)", cfg.poolBudget, cfg.appPoolSize(), cfg.wfPoolSize())
	t.Logf("  pollers:          %d", cfg.pollers)

	// -----------------------------------------------------------------------
	// Database + go-workflows setup (shared Postgres, testcontainer)
	// -----------------------------------------------------------------------

	dbResult := postgres.OpenStressDB(t, postgres.StressDBConfig{
		MaxConnections: cfg.poolBudget + 20, // headroom for admin/migration
	})
	db := dbResult.DB
	appPool := cfg.appPoolSize()
	db.SetMaxOpenConns(appPool)
	db.SetMaxIdleConns(appPool / 2)
	t.Logf("  app store pool:   max_open=%d, max_idle=%d", appPool, appPool/2)

	store := &postgres.Store{DB: db}

	// go-workflows backend — same database as the app store.
	// Capture the workflow pool's *sql.DB for stats sampling.
	wfPool := cfg.wfPoolSize()
	var wfDB *sql.DB
	b := wfpostgres.NewPostgresBackend(
		dbResult.Host, dbResult.Port,
		dbResult.User, dbResult.Password, dbResult.DBName,
		wfpostgres.WithPostgresOptions(func(db *sql.DB) {
			db.SetMaxOpenConns(wfPool)
			db.SetMaxIdleConns(wfPool / 2)
			wfDB = db
		}),
	)
	t.Cleanup(func() { b.Close() })
	t.Logf("  workflow DB pool: max_open=%d, max_idle=%d (same database)", wfPool, wfPool/2)

	w := worker.New(b, &worker.Options{
		WorkflowWorkerOptions: worker.WorkflowWorkerOptions{
			WorkflowPollers:         cfg.pollers,
			WorkflowPollingInterval: 20 * time.Millisecond,
		},
		ActivityWorkerOptions: worker.ActivityWorkerOptions{
			ActivityPollers:         cfg.pollers,
			ActivityPollingInterval: 20 * time.Millisecond,
		},
		SingleWorkerMode: true,
	})
	c := client.New(b)

	reg := &goworkflows.Registry{
		Worker:  w,
		Client:  c,
		Timeout: 30 * time.Minute,
		ActivityOptions: &workflow.ActivityOptions{
			RetryOptions: workflow.RetryOptions{
				MaxAttempts:        20,
				FirstRetryInterval: 500 * time.Millisecond,
				MaxRetryInterval:   10 * time.Second,
				BackoffCoefficient: 2,
			},
		},
	}
	t.Cleanup(func() { _ = w.WaitForCompletion() })

	// -----------------------------------------------------------------------
	// Delivery agent + observer + harness
	// -----------------------------------------------------------------------

	agent := &delayedDeliveryAgent{
		ackDelayMin:        cfg.ackDelayMin,
		ackDelayMax:        cfg.ackDelayMax,
		completionDelayMin: cfg.completionDelayMin,
		completionDelayMax: cfg.completionDelayMax,
		failureRate:        cfg.failureRate,
	}

	// We don't know the exact fulfillment count upfront (some creates
	// may fail). We'll set a generous target and let the drain phase
	// handle convergence. The target is the deployment pool size since
	// at most that many fulfillments can be created.
	observer := newStressObserver(cfg.totalDeployments)

	h := setupStress(t, store, reg, agent, domain.WithFulfillmentObserver(observer))
	agent.reporter = h.reporter

	// Worker must be started after all workflows/activities are registered.
	startGoWorkflowsWorker(t, reg)

	ctx, cancel := context.WithTimeout(context.Background(), cfg.testTimeout())
	defer cancel()

	// -----------------------------------------------------------------------
	// Register targets
	// -----------------------------------------------------------------------

	targetIDs := make([]domain.TargetID, stressNumTargets)
	targetNames := make([]string, stressNumTargets)
	for i := range targetIDs {
		name := fmt.Sprintf("stress-t%02d", i)
		targetIDs[i] = domain.TargetID(name)
		targetNames[i] = name
	}
	t.Logf("registering %d targets...", stressNumTargets)
	registerTargets(t, h.testHarness, targetNames...)
	t.Log("targets registered")

	// -----------------------------------------------------------------------
	// Pre-generate deployment names
	// -----------------------------------------------------------------------

	depNames := make([]domain.ResourceName, cfg.totalDeployments)
	for i := range depNames {
		depNames[i] = domain.ResourceName(fmt.Sprintf("stress-dep-%06d", i))
	}

	// -----------------------------------------------------------------------
	// Pool stats sampler — samples both DB pools every second.
	// -----------------------------------------------------------------------

	poolStats := startPoolStatsSampler(ctx, 1*time.Second, db, wfDB)

	// -----------------------------------------------------------------------
	// Run workload, drain, and report
	// -----------------------------------------------------------------------

	stats := runStressWorkload(ctx, t, cfg, h, observer, targetIDs, depNames)

	if stats.createCount == 0 {
		t.Fatal("no deployments were created successfully")
	}

	// Not all deployments in the pool may have been touched by the
	// Pareto distribution within the allotted duration. Report pool
	// utilisation and adjust the observer's drain target to the
	// actual number of creates.
	t.Logf("pool utilisation: %d/%d deployments created (%.0f%%)",
		stats.createCount, cfg.totalDeployments,
		100*float64(stats.createCount)/float64(cfg.totalDeployments))

	observer.mu.Lock()
	observer.target = stats.createCount
	alreadyDone := len(observer.completedFulfillments) >= stats.createCount
	observer.mu.Unlock()
	if alreadyDone {
		observer.signalDone()
	}

	drainFulfillments(ctx, t, observer)
	reportStressResults(ctx, t, cfg, observer, agent, store, stats, poolStats)
}
