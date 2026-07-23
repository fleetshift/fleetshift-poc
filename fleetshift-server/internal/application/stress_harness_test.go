//go:build stress

package application_test

import (
	"context"
	"encoding/json"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/delivery"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/goworkflows"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
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
// should be allowed to run. The main loop runs until all deployments
// are created; with the force-create mechanism every N/10 ops, worst
// case is ~10*N ops. We double that for safety and add time for
// draining in-flight deliveries and convergence polling.
// STRESS_DURATION serves as a floor for the loop bound.
func (c stressConfig) testTimeout() time.Duration {
	worstOps := c.totalDeployments * 20
	loopBound := time.Duration(worstOps/c.ratePerSecond+1) * time.Second
	if c.duration > loopBound {
		loopBound = c.duration
	}
	return loopBound + c.completionDelayMax + 10*time.Minute
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
// Fulfillment observer — per-run reconciliation + per-delivery metrics
// ---------------------------------------------------------------------------

// reconcileRun tracks a single orchestration workflow execution. Each
// RunStarted/End pair produces one run; a fulfillment may have many
// runs over its lifetime (e.g. after generation bumps or ContinueAsNew).
type reconcileRun struct {
	fulfillmentID domain.FulfillmentID
	started       time.Time
	completed     time.Time
	state         domain.FulfillmentState
	reconciledGen domain.Generation // first expectedGen seen via DispatchCycleStarted
	restarts      int               // inner-loop restarts (ReconciliationRestarting count)
	genAdvances   []genAdvance      // mid-rollout generation advances detected
}

// genAdvance records a single GenerationAdvancedMidRollout event.
type genAdvance struct {
	fromGen domain.Generation
	toGen   domain.Generation
}

type deliveryMetrics struct {
	fulfillmentID domain.FulfillmentID
	generation    domain.Generation
	dispatchedAt  time.Time
	ackReceivedAt time.Time
	completedAt   time.Time
	state         domain.DeliveryState
}

// stressObserver implements FulfillmentObserver. It measures:
//   - per-run reconciliation latency (RunStarted → End) across all runs
//   - per-delivery ack latency, completion latency (via DispatchCycleProbe)
//   - mid-rollout generation advances and reconciliation restarts
type stressObserver struct {
	domain.NoOpFulfillmentObserver

	mu   sync.Mutex
	runs []*reconcileRun // all runs, append-only

	// Drain tracking: unique fulfillments that completed at least one run.
	completedFulfillments map[domain.FulfillmentID]struct{}
	done                  chan struct{} // closed when len(completedFulfillments) >= target
	target                int

	// Convergence loop error tracking. Context-cancellation errors are
	// excluded since they are expected during test shutdown.
	convergenceErrors int64 // accessed atomically

	deliveryMu sync.Mutex
	deliveries map[domain.DeliveryID]*deliveryMetrics
}

func newStressObserver(expectedCount int) *stressObserver {
	return &stressObserver{
		completedFulfillments: make(map[domain.FulfillmentID]struct{}),
		done:                  make(chan struct{}),
		target:                expectedCount,
		deliveries:            make(map[domain.DeliveryID]*deliveryMetrics),
	}
}

func (o *stressObserver) RunStarted(ctx context.Context, fID domain.FulfillmentID) (context.Context, domain.FulfillmentRunProbe) {
	run := &reconcileRun{fulfillmentID: fID, started: time.Now()}
	o.mu.Lock()
	o.runs = append(o.runs, run)
	o.mu.Unlock()
	return ctx, &stressRunProbe{observer: o, run: run}
}

type stressRunProbe struct {
	domain.NoOpFulfillmentRunProbe
	observer *stressObserver
	run      *reconcileRun
}

// StateChanged, GenerationAdvancedMidRollout, ReconciliationRestarting,
// and DispatchCycleStarted write only to the probe's own run. They are
// called from the owning workflow goroutine and do not need locking;
// the report reads happen after all runs complete.

func (p *stressRunProbe) StateChanged(state domain.FulfillmentState) {
	p.run.state = state
}

func (p *stressRunProbe) GenerationAdvancedMidRollout(startGen, currentGen domain.Generation) {
	p.run.genAdvances = append(p.run.genAdvances, genAdvance{fromGen: startGen, toGen: currentGen})
}

func (p *stressRunProbe) ReconciliationRestarting(_ domain.Generation) {
	p.run.restarts++
}

func (p *stressRunProbe) End() {
	p.observer.mu.Lock()
	defer p.observer.mu.Unlock()

	p.run.completed = time.Now()

	fID := p.run.fulfillmentID
	if _, seen := p.observer.completedFulfillments[fID]; !seen {
		p.observer.completedFulfillments[fID] = struct{}{}
		if len(p.observer.completedFulfillments) >= p.observer.target {
			select {
			case <-p.observer.done:
			default:
				close(p.observer.done)
			}
		}
	}
}

func (p *stressRunProbe) DispatchCycleStarted(deliveryCount int, expectedGen domain.Generation) domain.DispatchCycleProbe {
	// Record the generation being reconciled (first cycle sets it;
	// subsequent cycles within the same run share the same generation
	// unless the inner loop restarts).
	if p.run.reconciledGen == 0 {
		p.run.reconciledGen = expectedGen
	}
	return &stressDispatchCycleProbe{
		observer:      p.observer,
		fulfillmentID: p.run.fulfillmentID,
		expectedGen:   expectedGen,
	}
}

// stressDispatchCycleProbe tracks per-delivery dispatch, ack, and
// completion timestamps, along with the generation being dispatched
// and the owning fulfillment.
type stressDispatchCycleProbe struct {
	domain.NoOpDispatchCycleProbe
	observer      *stressObserver
	fulfillmentID domain.FulfillmentID
	expectedGen   domain.Generation
}

func (p *stressDispatchCycleProbe) Dispatched(deliveryID domain.DeliveryID, isRedispatch bool) {
	if isRedispatch {
		return // only record the first dispatch
	}
	p.observer.deliveryMu.Lock()
	p.observer.deliveries[deliveryID] = &deliveryMetrics{
		fulfillmentID: p.fulfillmentID,
		generation:    p.expectedGen,
		dispatchedAt:  time.Now(),
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

// ---------------------------------------------------------------------------
// Observer accessors — called after all runs complete
// ---------------------------------------------------------------------------

// reconciliationLatencies returns the duration from RunStarted to End
// for every completed run (not just the first per fulfillment).
func (o *stressObserver) reconciliationLatencies() []time.Duration {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []time.Duration
	for _, run := range o.runs {
		if !run.completed.IsZero() {
			out = append(out, run.completed.Sub(run.started))
		}
	}
	return out
}

// runsPerFulfillment returns the count of completed runs per unique
// fulfillment, as a slice of counts (one entry per fulfillment).
func (o *stressObserver) runsPerFulfillment() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	counts := make(map[domain.FulfillmentID]int)
	for _, run := range o.runs {
		if !run.completed.IsZero() {
			counts[run.fulfillmentID]++
		}
	}
	out := make([]int, 0, len(counts))
	for _, c := range counts {
		out = append(out, c)
	}
	return out
}

// generationAdvanceStats returns the total number of mid-rollout
// generation advance events and the skip sizes (toGen − fromGen).
func (o *stressObserver) generationAdvanceStats() (totalAdvances int, skipSizes []int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	for _, run := range o.runs {
		for _, adv := range run.genAdvances {
			totalAdvances++
			skipSizes = append(skipSizes, int(adv.toGen-adv.fromGen))
		}
	}
	return
}

// totalRestarts returns the total number of inner-loop reconciliation
// restarts across all completed runs (ReconciliationRestarting events).
func (o *stressObserver) totalRestarts() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	var n int
	for _, run := range o.runs {
		n += run.restarts
	}
	return n
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
