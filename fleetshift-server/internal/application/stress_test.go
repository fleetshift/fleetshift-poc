//go:build stress

package application_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	wfpostgres "github.com/cschleiden/go-workflows/backend/postgres"
	"github.com/cschleiden/go-workflows/client"
	"github.com/cschleiden/go-workflows/worker"
	"github.com/cschleiden/go-workflows/workflow"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/goworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
)

// ---------------------------------------------------------------------------
// Environment-driven configuration
// ---------------------------------------------------------------------------

const stressNumTargets = 50

type stressConfig struct {
	totalDeployments int           // unique deployment pool size
	ratePerSecond    int           // operations per second
	duration         time.Duration // main loop duration

	// Delivery agent delays (separate ack vs completion, Gaussian).
	ackDelayMin        time.Duration
	ackDelayMax        time.Duration
	completionDelayMin time.Duration
	completionDelayMax time.Duration

	// Fraction of deliveries that report DeliveryStateFailed [0.0, 1.0].
	failureRate float64

	// Postgres / go-workflows sizing.
	poolBudget int // STRESS_POSTGRES_MAX_CONNS: total Go-side connection budget
	pollers    int // STRESS_WORKFLOW_POLLERS: workflow + activity pollers
}

func (c stressConfig) appPoolSize() int { return c.poolBudget * 60 / 100 }
func (c stressConfig) wfPoolSize() int  { return c.poolBudget * 40 / 100 }

// testTimeout returns a conservative upper bound on how long the test
// should be allowed to run, accounting for the main loop plus draining
// in-flight deliveries plus convergence polling.
func (c stressConfig) testTimeout() time.Duration {
	return c.duration + c.completionDelayMax + 10*time.Minute
}

func loadStressConfig() stressConfig {
	cfg := stressConfig{
		totalDeployments:   1000,
		ratePerSecond:      10,
		duration:           5 * time.Minute,
		ackDelayMin:        50 * time.Millisecond,
		ackDelayMax:        50 * time.Millisecond,
		completionDelayMin: 50 * time.Millisecond,
		completionDelayMax: 50 * time.Millisecond,
		failureRate:        0,
		poolBudget:         200,
		pollers:            4,
	}
	if v := os.Getenv("STRESS_DEPLOYMENTS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.totalDeployments = n
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
	if v := os.Getenv("STRESS_ACK_DELAY_MIN"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			cfg.ackDelayMin = d
		}
	}
	if v := os.Getenv("STRESS_ACK_DELAY_MAX"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			cfg.ackDelayMax = d
		}
	}
	if v := os.Getenv("STRESS_COMPLETION_DELAY_MIN"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			cfg.completionDelayMin = d
		}
	}
	if v := os.Getenv("STRESS_COMPLETION_DELAY_MAX"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d >= 0 {
			cfg.completionDelayMax = d
		}
	}
	if v := os.Getenv("STRESS_FAILURE_RATE"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			cfg.failureRate = f
		}
	}
	if v := os.Getenv("STRESS_POSTGRES_MAX_CONNS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.poolBudget = n
		}
	}
	if v := os.Getenv("STRESS_WORKFLOW_POLLERS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.pollers = n
		}
	}

	// Ensure min <= max for delay ranges.
	if cfg.ackDelayMin > cfg.ackDelayMax {
		cfg.ackDelayMin, cfg.ackDelayMax = cfg.ackDelayMax, cfg.ackDelayMin
	}
	if cfg.completionDelayMin > cfg.completionDelayMax {
		cfg.completionDelayMin, cfg.completionDelayMax = cfg.completionDelayMax, cfg.completionDelayMin
	}
	return cfg
}

// ---------------------------------------------------------------------------
// Mock delivery agent
// ---------------------------------------------------------------------------

// agentDelayRecord holds the delays the agent chose for a single delivery,
// so the test can compute orchestration overhead (observed - agent delay).
type agentDelayRecord struct {
	ackDelay        time.Duration
	completionDelay time.Duration
}

type delayedDeliveryAgent struct {
	reporter domain.DeliveryReporter

	ackDelayMin        time.Duration
	ackDelayMax        time.Duration
	completionDelayMin time.Duration
	completionDelayMax time.Duration
	failureRate        float64

	// delays records the chosen delays per delivery for overhead measurement.
	delays sync.Map // domain.DeliveryID → agentDelayRecord
}

// gaussianDelay returns a duration sampled from a truncated Gaussian
// distribution with mean = (min+max)/2 and sigma = (max-min)/6 (so
// 99.7% of samples fall within [min, max]). Values are clamped to
// [min, max]. If min == max the delay is fixed (no randomness).
func gaussianDelay(min, max time.Duration) time.Duration {
	if min >= max {
		return min
	}
	mean := float64(min+max) / 2
	sigma := float64(max-min) / 6
	d := time.Duration(rand.NormFloat64()*sigma + mean)
	if d < min {
		d = min
	}
	if d > max {
		d = max
	}
	return d
}

func (a *delayedDeliveryAgent) deliver(deliveryID domain.DeliveryID, generation domain.Generation) {
	ackDelay := gaussianDelay(a.ackDelayMin, a.ackDelayMax)
	completionDelay := gaussianDelay(a.completionDelayMin, a.completionDelayMax)
	// Completion delay is total time from dispatch to result; must be >= ack delay.
	if completionDelay < ackDelay {
		completionDelay = ackDelay
	}
	a.delays.Store(deliveryID, agentDelayRecord{
		ackDelay:        ackDelay,
		completionDelay: completionDelay,
	})

	go func() {
		if ackDelay > 0 {
			time.Sleep(ackDelay)
		}
		_ = a.reporter.ReportEvent(context.Background(), deliveryID, generation, domain.DeliveryEvent{Message: "accepted"})

		remaining := completionDelay - ackDelay
		if remaining > 0 {
			time.Sleep(remaining)
		}

		state := domain.DeliveryStateDelivered
		if a.failureRate > 0 && rand.Float64() < a.failureRate {
			state = domain.DeliveryStateFailed
		}
		_ = a.reporter.ReportResult(context.Background(), deliveryID, generation, domain.DeliveryResult{State: state})
	}()
}

func (a *delayedDeliveryAgent) Deliver(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	a.deliver(deliveryID, generation)
	return nil
}

func (a *delayedDeliveryAgent) Remove(_ context.Context, _ domain.TargetInfo, deliveryID domain.DeliveryID, _ []domain.Manifest, _ domain.DeliveryAuth, _ *domain.Attestation, generation domain.Generation) error {
	a.deliver(deliveryID, generation)
	return nil
}

// ---------------------------------------------------------------------------
// Fulfillment observer — per-fulfillment reconciliation + per-delivery metrics
// ---------------------------------------------------------------------------

type reconcileRecord struct {
	started   time.Time
	completed time.Time
	state     domain.FulfillmentState
}

type deliveryMetrics struct {
	generation    domain.Generation
	dispatchedAt  time.Time
	ackReceivedAt time.Time
	completedAt   time.Time
	state         domain.DeliveryState
}

// stressObserver implements FulfillmentObserver. It measures:
//   - per-fulfillment reconciliation latency (RunStarted → End)
//   - per-delivery ack latency, completion latency (via DispatchCycleProbe)
type stressObserver struct {
	domain.NoOpFulfillmentObserver

	mu      sync.Mutex
	records map[domain.FulfillmentID]*reconcileRecord
	done    chan struct{} // closed when expectedCount reconciliations complete
	target  int
	count   int

	deliveryMu sync.Mutex
	deliveries map[domain.DeliveryID]*deliveryMetrics
}

func newStressObserver(expectedCount int) *stressObserver {
	return &stressObserver{
		records:    make(map[domain.FulfillmentID]*reconcileRecord),
		done:       make(chan struct{}),
		target:     expectedCount,
		deliveries: make(map[domain.DeliveryID]*deliveryMetrics),
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
	return &stressDispatchCycleProbe{observer: p.observer, expectedGen: expectedGen}
}

// stressDispatchCycleProbe tracks per-delivery dispatch, ack, and
// completion timestamps, along with the generation being dispatched.
type stressDispatchCycleProbe struct {
	domain.NoOpDispatchCycleProbe
	observer    *stressObserver
	expectedGen domain.Generation
}

func (p *stressDispatchCycleProbe) Dispatched(deliveryID domain.DeliveryID, isRedispatch bool) {
	if isRedispatch {
		return // only record the first dispatch
	}
	p.observer.deliveryMu.Lock()
	p.observer.deliveries[deliveryID] = &deliveryMetrics{
		generation:   p.expectedGen,
		dispatchedAt: time.Now(),
	}
	p.observer.deliveryMu.Unlock()
}

func (p *stressDispatchCycleProbe) AckReceived(deliveryID domain.DeliveryID) {
	now := time.Now()
	p.observer.deliveryMu.Lock()
	if m, ok := p.observer.deliveries[deliveryID]; ok {
		m.ackReceivedAt = now
	}
	p.observer.deliveryMu.Unlock()
}

func (p *stressDispatchCycleProbe) Completed(deliveryID domain.DeliveryID, state domain.DeliveryState) {
	now := time.Now()
	p.observer.deliveryMu.Lock()
	if m, ok := p.observer.deliveries[deliveryID]; ok {
		m.completedAt = now
		m.state = state
	}
	p.observer.deliveryMu.Unlock()
}

// reconciliationLatencies returns the duration from RunStarted to End
// for all completed fulfillment runs.
func (o *stressObserver) reconciliationLatencies() []time.Duration {
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

// ackLatencies returns the ack latency (dispatch → ack received) for
// all deliveries where both timestamps are present.
func (o *stressObserver) ackLatencies() []time.Duration {
	o.deliveryMu.Lock()
	defer o.deliveryMu.Unlock()
	var out []time.Duration
	for _, m := range o.deliveries {
		if !m.dispatchedAt.IsZero() && !m.ackReceivedAt.IsZero() {
			out = append(out, m.ackReceivedAt.Sub(m.dispatchedAt))
		}
	}
	return out
}

// completionLatencies returns the completion latency (dispatch →
// completed) for all deliveries where both timestamps are present.
func (o *stressObserver) completionLatencies() []time.Duration {
	o.deliveryMu.Lock()
	defer o.deliveryMu.Unlock()
	var out []time.Duration
	for _, m := range o.deliveries {
		if !m.dispatchedAt.IsZero() && !m.completedAt.IsZero() {
			out = append(out, m.completedAt.Sub(m.dispatchedAt))
		}
	}
	return out
}

// overheads joins observer delivery metrics with agent delay records
// to compute system overhead = observed latency - agent delay.
func (o *stressObserver) overheads(agent *delayedDeliveryAgent) (ackOverheads, completionOverheads []time.Duration) {
	o.deliveryMu.Lock()
	defer o.deliveryMu.Unlock()
	for did, m := range o.deliveries {
		v, ok := agent.delays.Load(did)
		if !ok {
			continue
		}
		rec := v.(agentDelayRecord)
		if !m.dispatchedAt.IsZero() && !m.ackReceivedAt.IsZero() {
			ackOverheads = append(ackOverheads, m.ackReceivedAt.Sub(m.dispatchedAt)-rec.ackDelay)
		}
		if !m.dispatchedAt.IsZero() && !m.completedAt.IsZero() {
			completionOverheads = append(completionOverheads, m.completedAt.Sub(m.dispatchedAt)-rec.completionDelay)
		}
	}
	return
}

// ---------------------------------------------------------------------------
// Test harness setup
// ---------------------------------------------------------------------------

// stressHarness extends the base testHarness with fields needed for
// direct store mutations (deployment updates) and orchestration nudges.
type stressHarness struct {
	testHarness
	orchWf domain.OrchestrationWorkflow
}

func setupStress(t *testing.T, store domain.Store, reg domain.Registry, agent domain.DeliveryAgent, orchOpts ...domain.OrchestrationWorkflowOption) stressHarness {
	t.Helper()

	router := delivery.NewRoutingDeliveryService()
	router.Register(testTargetType, agent)

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

	return stressHarness{
		testHarness: testHarness{
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
		},
		orchWf: orchWf,
	}
}

// startGoWorkflowsWorker starts the go-workflows worker. Must be
// called after all workflows and activities have been registered
// (i.e., after setupStress).
func startGoWorkflowsWorker(t *testing.T, reg domain.Registry) {
	t.Helper()
	gwReg, ok := reg.(*goworkflows.Registry)
	if !ok {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	if err := gwReg.Worker.Start(ctx); err != nil {
		t.Fatalf("start go-workflows worker: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Pareto distribution
// ---------------------------------------------------------------------------

// paretoIndex returns an index in [0, n) following a power-law
// distribution. The alpha parameter controls skew: with alpha ≈ 7.21,
// 80% of selections hit the bottom 20% of indices (classic 80/20
// selection frequency). Lower indices are "hotter" (selected more
// often).
//
// The CDF is P(idx < k) = 1 − (1 − k/n)^α. To find the alpha for a
// desired hotspot ratio (e.g. fraction p of selections in the bottom
// fraction q of indices): α = ln(1−p) / ln(1−q).
func paretoIndex(n int, alpha float64) int {
	u := rand.Float64()
	if u == 0 {
		u = 1e-15
	}
	idx := int(float64(n) * (1 - math.Pow(u, 1.0/alpha)))
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return idx
}

// ---------------------------------------------------------------------------
// Manifest factory
// ---------------------------------------------------------------------------

func stressManifest(deploymentName string, version int) domain.Manifest {
	data := map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]any{
			"name":      deploymentName,
			"namespace": "default",
			"labels": map[string]any{
				"app":        "stress-test",
				"managed-by": "fleetshift",
			},
		},
		"data": map[string]any{
			"version":    strconv.Itoa(version),
			"deployment": deploymentName,
			"config":     "server:\n  port: 8080\n  host: 0.0.0.0\n  read_timeout: 30s\n  write_timeout: 30s\ndatabase:\n  driver: postgres\n  pool_size: 25\ncache:\n  enabled: true\n  ttl: 600s\n",
		},
	}
	raw, _ := json.Marshal(data)
	return domain.Manifest{Raw: json.RawMessage(raw)}
}

// ---------------------------------------------------------------------------
// Direct store mutation for deployment "updates"
// ---------------------------------------------------------------------------

// updateDeploymentManifest directly mutates the fulfillment's manifest
// strategy in the store (bypassing the service layer) and nudges the
// orchestration workflow. This simulates a deployment update: the
// generation advances and the running orchestration detects the change
// on its next PersistAndCompleteReconciliation call.
func updateDeploymentManifest(
	ctx context.Context,
	store domain.Store,
	orchWf domain.OrchestrationWorkflow,
	depName domain.ResourceName,
	newManifest domain.Manifest,
) error {
	tx, err := store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, depName)
	if err != nil {
		return fmt.Errorf("get deployment %s: %w", depName, err)
	}

	f, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID())
	if err != nil {
		return fmt.Errorf("get fulfillment: %w", err)
	}

	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{newManifest},
	}, time.Now())

	if err := tx.Fulfillments().Update(ctx, f); err != nil {
		return fmt.Errorf("update fulfillment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// Nudge orchestration. If a run is in progress it will detect the
	// generation change when it completes; if idle, this starts a new run.
	_, err = orchWf.Start(ctx, dep.FulfillmentID())
	if err != nil && !errors.Is(err, domain.ErrAlreadyRunning) {
		return fmt.Errorf("start orchestration: %w", err)
	}
	return nil
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

func printReport(t *testing.T, title string, report latencyReport) {
	t.Logf("\n=== %s ===", title)
	t.Logf("Samples: %d", report.count)
	t.Logf("Mean:    %v", report.mean)
	t.Logf("P50:     %v", report.p50)
	t.Logf("P80:     %v", report.p80)
	t.Logf("P90:     %v", report.p90)
	t.Logf("P95:     %v", report.p95)
	t.Logf("P99:     %v", report.p99)
	t.Logf("Max:     %v", report.max)
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
	wfPool := cfg.wfPoolSize()
	b := wfpostgres.NewPostgresBackend(
		dbResult.Host, dbResult.Port,
		dbResult.User, dbResult.Password, dbResult.DBName,
		wfpostgres.WithPostgresOptions(func(db *sql.DB) {
			db.SetMaxOpenConns(wfPool)
			db.SetMaxIdleConns(wfPool / 2)
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
	// Delivery agent + observer
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
	// Pre-generate deployment IDs
	// -----------------------------------------------------------------------

	depNames := make([]domain.ResourceName, cfg.totalDeployments)
	for i := range depNames {
		depNames[i] = domain.ResourceName(fmt.Sprintf("stress-dep-%06d", i))
	}

	// Tracking state per deployment slot.
	type depState struct {
		created       bool
		version       int
		fulfillmentID domain.FulfillmentID
	}
	depStates := make([]depState, cfg.totalDeployments)

	var (
		createCount  int
		updateCount  int
		createErrors int
		updateErrors int
		createdSoFar int // sequential counter for round-robin target assignment
	)

	// -----------------------------------------------------------------------
	// Main loop: Pareto-driven create / update
	// -----------------------------------------------------------------------

	// α = ln(1−0.8) / ln(1−0.2) ≈ 7.21: 80% of selections hit the
	// bottom 20% of indices (hotspot deployments that get updated
	// repeatedly while their earlier generations are still in-flight).
	const paretoAlpha = 7.21

	t.Logf("starting main loop: %d ops at %d/s (Pareto alpha=%.2f across %d deployments)...",
		totalOps, cfg.ratePerSecond, paretoAlpha, cfg.totalDeployments)

	ticker := time.NewTicker(time.Second / time.Duration(cfg.ratePerSecond))
	defer ticker.Stop()

	for i := 0; i < totalOps; i++ {
		<-ticker.C
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
			if err != nil {
				createErrors++
				if createErrors <= 5 {
					t.Logf("Create(%s): %v", depName, err)
				}
				continue
			}
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
			if err := updateDeploymentManifest(ctx, store, h.orchWf, depName, manifest); err != nil {
				updateErrors++
				if updateErrors <= 5 {
					t.Logf("Update(%s): %v", depName, err)
				}
				continue
			}
			updateCount++
		}

		if (i+1)%(totalOps/10) == 0 || i+1 == totalOps {
			t.Logf("  ops %d/%d (creates=%d, updates=%d)", i+1, totalOps, createCount, updateCount)
		}
	}
	ticker.Stop()

	// -----------------------------------------------------------------------
	// Extension phase: create any deployments that were never picked
	// -----------------------------------------------------------------------

	var uncreatedCount int
	for _, ds := range depStates {
		if !ds.created {
			uncreatedCount++
		}
	}
	if uncreatedCount > 0 {
		t.Logf("WARNING: %d/%d deployments were never created during main loop; creating them now at %d/s...",
			uncreatedCount, cfg.totalDeployments, cfg.ratePerSecond)
		ticker = time.NewTicker(time.Second / time.Duration(cfg.ratePerSecond))
		for idx := range depStates {
			if depStates[idx].created {
				continue
			}
			<-ticker.C
			depName := depNames[idx]
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
			if err != nil {
				createErrors++
				if createErrors <= 10 {
					t.Logf("Create(%s) [extension]: %v", depName, err)
				}
				continue
			}
			depStates[idx] = depState{
				created:       true,
				version:       1,
				fulfillmentID: view.Fulfillment.ID(),
			}
			createCount++
			createdSoFar++
		}
		ticker.Stop()
		t.Logf("extension phase complete: %d total creates", createCount)
	}

	t.Logf("operation phase complete: creates=%d (errors=%d), updates=%d (errors=%d)",
		createCount, createErrors, updateCount, updateErrors)

	if createCount == 0 {
		t.Fatal("no deployments were created successfully")
	}

	// -----------------------------------------------------------------------
	// Drain: wait for all created fulfillments to reconcile at least once
	// -----------------------------------------------------------------------

	// Adjust the observer target to the actual number of creates.
	observer.mu.Lock()
	observer.target = createCount
	alreadyDone := observer.count >= createCount
	observer.mu.Unlock()
	if alreadyDone {
		select {
		case <-observer.done:
		default:
			close(observer.done)
		}
	}

	t.Logf("awaiting reconciliation of %d fulfillments...", createCount)
	select {
	case <-observer.done:
		t.Logf("all %d fulfillments reconciled at least once", createCount)
	case <-ctx.Done():
		observer.mu.Lock()
		t.Logf("timeout: %d/%d fulfillments reconciled", observer.count, createCount)
		observer.mu.Unlock()
	}

	// -----------------------------------------------------------------------
	// Reports
	// -----------------------------------------------------------------------

	// 1. Reconciliation latency (RunStarted → End)
	reconDurations := observer.reconciliationLatencies()
	if len(reconDurations) > 0 {
		printReport(t, "Reconciliation Latency (RunStarted → End)", computeLatencies(reconDurations))
	}

	// 2. Ack latency (dispatch → ack received)
	ackDurations := observer.ackLatencies()
	if len(ackDurations) > 0 {
		printReport(t, "Ack Latency (Dispatch → AckReceived)", computeLatencies(ackDurations))
	}

	// 3. Completion latency (dispatch → completed)
	completionDurations := observer.completionLatencies()
	if len(completionDurations) > 0 {
		printReport(t, "Completion Latency (Dispatch → Completed)", computeLatencies(completionDurations))
	}

	// 4. Overhead = observed latency - agent delay
	ackOverheads, completionOverheads := observer.overheads(agent)
	if len(ackOverheads) > 0 {
		printReport(t, "Ack Overhead (AckLatency − AgentAckDelay)", computeLatencies(ackOverheads))
	}
	if len(completionOverheads) > 0 {
		printReport(t, "Completion Overhead (CompletionLatency − AgentCompletionDelay)", computeLatencies(completionOverheads))
	}

	// 5. Generation convergence summary
	t.Logf("\n=== Generation Convergence ===")
	var convergedCount, laggedCount int
	var totalGenSkips int
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
			for _, v := range views {
				stateCounts[v.Fulfillment.State()]++
				gen := v.Fulfillment.Generation()
				obsGen := v.Fulfillment.ObservedGeneration()
				if obsGen >= gen {
					convergedCount++
				} else {
					laggedCount++
				}
				// Generations skipped = gen - obsGen - 1 (if positive);
				// each skip means an intermediate generation was never
				// independently reconciled.
				if skip := int(gen) - int(obsGen) - 1; skip > 0 {
					totalGenSkips += skip
				}
			}
			t.Logf("Fulfillments:   %d total", len(views))
			t.Logf("Converged:      %d (observedGen >= generation)", convergedCount)
			t.Logf("Lagged:         %d (observedGen < generation)", laggedCount)
			t.Logf("Gen skips:      %d (intermediate generations never independently reconciled)", totalGenSkips)
			for state, count := range stateCounts {
				t.Logf("  state %-10s %d", state, count)
			}
		}
	}

	// 6. Configuration summary
	t.Logf("\n=== Configuration ===")
	t.Logf("Targets:            %d", stressNumTargets)
	t.Logf("Deployment pool:    %d", cfg.totalDeployments)
	t.Logf("Rate:               %d/sec", cfg.ratePerSecond)
	t.Logf("Duration:           %v", cfg.duration)
	t.Logf("Ack delay:          %v..%v (gaussian)", cfg.ackDelayMin, cfg.ackDelayMax)
	t.Logf("Completion delay:   %v..%v (gaussian)", cfg.completionDelayMin, cfg.completionDelayMax)
	t.Logf("Failure rate:       %.1f%%", cfg.failureRate*100)
	t.Logf("Pool budget:        %d (app=%d, wf=%d, pg_max_connections=%d)",
		cfg.poolBudget, cfg.appPoolSize(), cfg.wfPoolSize(), cfg.poolBudget+20)
	t.Logf("Pollers:            %d", cfg.pollers)

	// 7. Operation summary
	t.Logf("\n=== Operations ===")
	t.Logf("Creates:            %d (errors: %d)", createCount, createErrors)
	t.Logf("Updates:            %d (errors: %d)", updateCount, updateErrors)
	t.Logf("Total ops:          %d", createCount+updateCount+createErrors+updateErrors)

	// Find the max version (most-updated deployment)
	maxVersion := 0
	for _, ds := range depStates {
		if ds.version > maxVersion {
			maxVersion = ds.version
		}
	}
	t.Logf("Max gen bumps:      %d (highest version for a single deployment)", maxVersion)
}
