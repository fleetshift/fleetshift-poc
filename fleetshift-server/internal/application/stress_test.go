//go:build stress

package application_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/memworkflow"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// ---------------------------------------------------------------------------
// Environment-driven configuration
// ---------------------------------------------------------------------------

type stressConfig struct {
	numTargets    int
	ratePerSecond int
	duration      time.Duration
	deliveryDelay time.Duration
	testTimeout   time.Duration
}

func loadStressConfig() stressConfig {
	cfg := stressConfig{
		numTargets:    1000,
		ratePerSecond: 10,
		duration:      5 * time.Minute,
		deliveryDelay: 50 * time.Millisecond,
		testTimeout:   10 * time.Minute,
	}
	if v := os.Getenv("STRESS_TARGETS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.numTargets = n
		}
	}
	if v := os.Getenv("STRESS_RATE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.ratePerSecond = n
		}
	}
	if v := os.Getenv("STRESS_DURATION"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.duration = d
		}
	}
	if v := os.Getenv("STRESS_DELIVERY_DELAY"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			cfg.deliveryDelay = d
		}
	}
	cfg.testTimeout = cfg.duration + 5*time.Minute
	return cfg
}

// ---------------------------------------------------------------------------
// Mock delivery agent
// ---------------------------------------------------------------------------

type delayedDeliveryAgent struct {
	reporter domain.DeliveryReporter
	delay    time.Duration
}

func (a *delayedDeliveryAgent) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		if a.delay > 0 {
			time.Sleep(a.delay)
		}
		_ = a.reporter.ReportResult(context.Background(), deliveryID, generation, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
	}()
	return nil
}

func (a *delayedDeliveryAgent) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	go func() {
		if a.delay > 0 {
			time.Sleep(a.delay)
		}
		_ = a.reporter.ReportResult(context.Background(), deliveryID, generation, domain.DeliveryResult{State: domain.DeliveryStateDelivered})
	}()
	return nil
}

// ---------------------------------------------------------------------------
// Fulfillment observer — records per-fulfillment reconciliation latency
// ---------------------------------------------------------------------------

type reconcileRecord struct {
	started   time.Time
	completed time.Time
	state     domain.FulfillmentState
}

// stressObserver implements FulfillmentObserver to measure the time
// between orchestration run start and completion per fulfillment.
type stressObserver struct {
	domain.NoOpFulfillmentObserver

	mu      sync.Mutex
	records map[domain.FulfillmentID]*reconcileRecord
	done    chan struct{} // closed when expectedCount reconciliations complete
	target  int
	count   int
}

func newStressObserver(expectedCount int) *stressObserver {
	return &stressObserver{
		records: make(map[domain.FulfillmentID]*reconcileRecord),
		done:    make(chan struct{}),
		target:  expectedCount,
	}
}

func (o *stressObserver) RunStarted(ctx context.Context, fID domain.FulfillmentID) (context.Context, domain.FulfillmentRunProbe) {
	o.mu.Lock()
	if _, exists := o.records[fID]; !exists {
		o.records[fID] = &reconcileRecord{started: time.Now()}
	}
	o.mu.Unlock()
	return ctx, &stressRunProbe{observer: o, fulfillmentID: fID}
}

type stressRunProbe struct {
	domain.NoOpFulfillmentRunProbe
	observer      *stressObserver
	fulfillmentID domain.FulfillmentID
}

func (p *stressRunProbe) StateChanged(state domain.FulfillmentState) {
	p.observer.mu.Lock()
	if rec, ok := p.observer.records[p.fulfillmentID]; ok {
		rec.state = state
	}
	p.observer.mu.Unlock()
}

func (p *stressRunProbe) End() {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()
	if rec, ok := p.observer.records[p.fulfillmentID]; ok {
		if rec.completed.IsZero() {
			rec.completed = time.Now()
			p.observer.count++
			if p.observer.count >= p.observer.target {
				select {
				case <-p.observer.done:
				default:
					close(p.observer.done)
				}
			}
		}
	}
}

func (p *stressRunProbe) DispatchCycleStarted(deliveryCount int, expectedGen domain.Generation) domain.DispatchCycleProbe {
	return domain.NoOpDispatchCycleProbe{}
}

// latencies returns reconciliation durations for all completed runs.
func (o *stressObserver) latencies() []time.Duration {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []time.Duration
	for _, rec := range o.records {
		if !rec.completed.IsZero() {
			out = append(out, rec.completed.Sub(rec.started))
		}
	}
	return out
}

// ---------------------------------------------------------------------------
// Test harness setup (variant that accepts orchestration options)
// ---------------------------------------------------------------------------

func setupStress(t *testing.T, store domain.Store, agent domain.DeliveryAgent, orchOpts ...domain.OrchestrationWorkflowOption) testHarness {
	t.Helper()

	router := delivery.NewRoutingDeliveryService()
	router.Register(testTargetType, agent)

	reg := &memworkflow.Registry{}
	reporter := application.NewDeliveryReportService(store, reg)

	opts := append([]domain.OrchestrationWorkflowOption{
		domain.WithAckRetryInterval(5 * time.Second),
	}, orchOpts...)
	orchSpec := domain.NewOrchestrationWorkflowSpec(
		store, router, domain.StrategyFactory{Store: store}, reg,
		opts...,
	)
	orchWf, err := reg.RegisterOrchestration(orchSpec)
	if err != nil {
		t.Fatalf("RegisterOrchestration: %v", err)
	}

	cwfSpec := &domain.CreateDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
	}
	createWf, err := reg.RegisterCreateDeployment(cwfSpec)
	if err != nil {
		t.Fatalf("RegisterCreateDeployment: %v", err)
	}

	fakeReg := keyregistry.NewFake()
	keyResolver := &domain.KeyResolver{
		Registries: domain.BuiltInKeyRegistries(),
		Clients: map[domain.KeyRegistryType]domain.RegistryClient{
			domain.KeyRegistryTypeGitHub: fakeReg,
		},
	}
	provenanceSvc := &domain.ProvenanceService{KeyResolver: keyResolver}

	cleanupSpec := &domain.DeleteDeploymentCleanupWorkflowSpec{Store: store}
	cleanupWf, err := reg.RegisterDeleteDeploymentCleanup(cleanupSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteDeploymentCleanup: %v", err)
	}

	deleteSpec := &domain.DeleteDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		Cleanup:       cleanupWf,
	}
	deleteWf, err := reg.RegisterDeleteDeployment(deleteSpec)
	if err != nil {
		t.Fatalf("RegisterDeleteDeployment: %v", err)
	}

	resumeSpec := &domain.ResumeDeploymentWorkflowSpec{
		Store:         store,
		Orchestration: orchWf,
		ProvenanceSvc: provenanceSvc,
	}
	resumeWf, err := reg.RegisterResumeDeployment(resumeSpec)
	if err != nil {
		t.Fatalf("RegisterResumeDeployment: %v", err)
	}

	return testHarness{
		targets: &application.TargetService{Store: store},
		deployments: &application.DeploymentService{
			Store:         store,
			CreateWF:      createWf,
			DeleteWF:      deleteWf,
			ResumeWF:      resumeWf,
			ProvenanceSvc: provenanceSvc,
		},
		store:    store,
		reporter: reporter,
		fakeReg:  fakeReg,
	}
}

// ---------------------------------------------------------------------------
// Database store
// ---------------------------------------------------------------------------

func stressStore(t *testing.T) domain.Store {
	t.Helper()
	switch os.Getenv("STRESS_DB") {
	case "postgres":
		t.Log("using Postgres (testcontainer)")
		return &postgres.Store{DB: postgres.OpenTestDB(t)}
	default:
		t.Log("using file-backed SQLite (WAL)")
		dbPath := filepath.Join(t.TempDir(), "stress.db")
		db, err := sqlite.Open(dbPath)
		if err != nil {
			t.Fatalf("open stress db: %v", err)
		}
		// SQLite serializes writers even with WAL. Funneling through a
		// single connection avoids SQLITE_BUSY entirely by queuing at
		// the Go pool level.
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { db.Close() })
		return &sqlite.Store{DB: db}
	}
}

// ---------------------------------------------------------------------------
// Percentile computation
// ---------------------------------------------------------------------------

type latencyReport struct {
	count int
	mean  time.Duration
	p50   time.Duration
	p80   time.Duration
	p90   time.Duration
	p95   time.Duration
	p99   time.Duration
	max   time.Duration
}

func computeLatencies(durations []time.Duration) latencyReport {
	n := len(durations)
	if n == 0 {
		return latencyReport{}
	}
	sorted := make([]time.Duration, n)
	copy(sorted, durations)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	var total time.Duration
	for _, d := range sorted {
		total += d
	}
	pct := func(p float64) time.Duration {
		idx := int(math.Ceil(p/100.0*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return sorted[idx]
	}
	return latencyReport{
		count: n,
		mean:  total / time.Duration(n),
		p50:   pct(50),
		p80:   pct(80),
		p90:   pct(90),
		p95:   pct(95),
		p99:   pct(99),
		max:   sorted[n-1],
	}
}

// ---------------------------------------------------------------------------
// Stress test
// ---------------------------------------------------------------------------

func TestStress_OrchestrationLoop(t *testing.T) {
	cfg := loadStressConfig()

	totalDeployments := cfg.ratePerSecond * int(cfg.duration.Seconds())

	t.Logf("config: targets=%d rate=%d/s duration=%v delivery_delay=%v (total deployments: %d)",
		cfg.numTargets, cfg.ratePerSecond, cfg.duration, cfg.deliveryDelay, totalDeployments)

	store := stressStore(t)
	agent := &delayedDeliveryAgent{delay: cfg.deliveryDelay}
	observer := newStressObserver(totalDeployments)

	h := setupStress(t, store, agent, domain.WithFulfillmentObserver(observer))
	agent.reporter = h.reporter

	ctx, cancel := context.WithTimeout(context.Background(), cfg.testTimeout)
	defer cancel()

	// Register targets.
	targetIDs := make([]domain.TargetID, cfg.numTargets)
	targetNames := make([]string, cfg.numTargets)
	for i := range targetIDs {
		name := fmt.Sprintf("stress-t%04d", i)
		targetIDs[i] = domain.TargetID(name)
		targetNames[i] = name
	}
	t.Logf("registering %d targets...", cfg.numTargets)
	registerTargets(t, h, targetNames...)
	t.Logf("targets registered")

	// ~1KB manifest payload.
	manifest := domain.Manifest{Raw: json.RawMessage(`{
		"apiVersion": "v1",
		"kind": "ConfigMap",
		"metadata": {
			"name": "stress-config",
			"namespace": "default",
			"labels": {
				"app": "stress-test",
				"component": "orchestration",
				"managed-by": "fleetshift"
			}
		},
		"data": {
			"config.yaml": "server:\n  port: 8080\n  host: 0.0.0.0\n  read_timeout: 30s\n  write_timeout: 30s\n  idle_timeout: 120s\nlogging:\n  level: info\n  format: json\n  output: stdout\ndatabase:\n  driver: postgres\n  host: db.internal\n  port: 5432\n  name: app\n  pool_size: 25\n  idle_timeout: 300s\ncache:\n  enabled: true\n  ttl: 600s\n  max_entries: 10000\nfeatures:\n  rate_limiting: true\n  circuit_breaker: true\n  retry_backoff: exponential\n",
			"version": "1.0.0",
			"checksum": "sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
		}
	}`)}

	// Fire deployments at the configured rate for the configured duration.
	// Each deployment targets a single target, round-robining across
	// all registered targets (deployment i → target i % numTargets).
	t.Logf("sending %d deployments at %d/s for %v (round-robin across %d targets)...",
		totalDeployments, cfg.ratePerSecond, cfg.duration, cfg.numTargets)

	ticker := time.NewTicker(time.Second / time.Duration(cfg.ratePerSecond))
	defer ticker.Stop()

	var createErrors int
	for i := 0; i < totalDeployments; i++ {
		<-ticker.C
		depID := fmt.Sprintf("stress-dep-%06d", i)
		target := targetIDs[i%cfg.numTargets]
		_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
			Name: domain.ResourceName(depID),
			ManifestStrategy: domain.ManifestStrategySpec{
				Type:      domain.ManifestStrategyInline,
				Manifests: []domain.Manifest{manifest},
			},
			PlacementStrategy: domain.PlacementStrategySpec{
				Type:    domain.PlacementStrategyStatic,
				Targets: []domain.TargetID{target},
			},
		})
		if err != nil {
			createErrors++
			if createErrors <= 5 {
				t.Logf("Create(%s): %v", depID, err)
			}
			continue
		}
		if (i+1)%(totalDeployments/10) == 0 || i+1 == totalDeployments {
			t.Logf("  created %d/%d deployments", i+1, totalDeployments)
		}
	}
	ticker.Stop()

	created := totalDeployments - createErrors
	t.Logf("creation phase complete: %d created, %d errors", created, createErrors)

	if created == 0 {
		t.Fatal("no deployments were created successfully")
	}

	// Wait for all orchestration runs to complete.
	t.Logf("awaiting reconciliation of %d fulfillments...", created)
	select {
	case <-observer.done:
		t.Logf("all %d fulfillments reconciled", created)
	case <-ctx.Done():
		observer.mu.Lock()
		t.Logf("timeout: %d/%d fulfillments reconciled", observer.count, created)
		observer.mu.Unlock()
	}

	// ---------------------------------------------------------------------------
	// Latency report (observer-measured: RunStarted → RunProbe.End)
	// ---------------------------------------------------------------------------
	durations := observer.latencies()
	if len(durations) == 0 {
		t.Fatal("no latency data collected")
	}
	report := computeLatencies(durations)

	t.Logf("\n=== Reconciliation Latency (RunStarted → Run.End) ===")
	t.Logf("Samples:        %d", report.count)
	t.Logf("Mean: %v", report.mean)
	t.Logf("P50:  %v", report.p50)
	t.Logf("P80:  %v", report.p80)
	t.Logf("P90:  %v", report.p90)
	t.Logf("P95:  %v", report.p95)
	t.Logf("P99:  %v", report.p99)
	t.Logf("Max:  %v", report.max)

	// ---------------------------------------------------------------------------
	// Final state summary — query all deployments and report state distribution
	// ---------------------------------------------------------------------------
	t.Logf("\n=== Final Fulfillment State Summary ===")
	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Logf("could not query final state: %v", err)
	} else {
		views, err := tx.Deployments().ListView(ctx)
		tx.Rollback()
		if err != nil {
			t.Logf("could not list deployments: %v", err)
		} else {
			stateCounts := make(map[domain.FulfillmentState]int)
			var pausedCount int
			var reconcilingCount int
			for _, v := range views {
				stateCounts[v.Fulfillment.State()]++
				if v.Fulfillment.Paused() {
					pausedCount++
				}
				if v.Fulfillment.Reconciling() {
					reconcilingCount++
				}
			}
			t.Logf("Total deployments: %d", len(views))
			for state, count := range stateCounts {
				t.Logf("  %-10s %d", state, count)
			}
			if pausedCount > 0 {
				t.Logf("  paused:    %d", pausedCount)
			}
			if reconcilingCount > 0 {
				t.Logf("  reconciling: %d (still in progress)", reconcilingCount)
			}
		}
	}

	// ---------------------------------------------------------------------------
	// Test configuration summary
	// ---------------------------------------------------------------------------
	t.Logf("\n=== Configuration ===")
	t.Logf("Targets:        %d", cfg.numTargets)
	t.Logf("Rate:           %d/sec", cfg.ratePerSecond)
	t.Logf("Duration:       %v", cfg.duration)
	t.Logf("Delivery delay: %v", cfg.deliveryDelay)
	t.Logf("DB backend:     %s", dbBackendName())
	t.Logf("Create errors:  %d", createErrors)
}

func dbBackendName() string {
	if os.Getenv("STRESS_DB") == "postgres" {
		return "postgres (testcontainer)"
	}
	return "sqlite (file-backed WAL)"
}
