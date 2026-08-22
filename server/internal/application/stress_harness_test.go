//go:build stress

package application_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/scripted"
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
// should be allowed to run, accounting for the main loop plus draining
// in-flight deliveries plus convergence polling.
func (c stressConfig) testTimeout() time.Duration {
	return c.duration + c.completionDelayMax + 10*time.Minute
}

// envInt reads an env var as a positive integer, returning fallback on
// missing/invalid values.
func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return fallback
}

// envDuration reads an env var as a time.Duration, returning fallback
// on missing/invalid values. allowZero controls whether 0 is accepted.
func envDuration(key string, fallback time.Duration, allowZero bool) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil && (d > 0 || (allowZero && d >= 0)) {
			return d
		}
	}
	return fallback
}

func loadStressConfig() stressConfig {
	cfg := stressConfig{
		totalDeployments:   envInt("STRESS_DEPLOYMENTS", 1000),
		ratePerSecond:      envInt("STRESS_RATE", 10),
		duration:           envDuration("STRESS_DURATION", 5*time.Minute, false),
		ackDelayMin:        envDuration("STRESS_ACK_DELAY_MIN", 50*time.Millisecond, true),
		ackDelayMax:        envDuration("STRESS_ACK_DELAY_MAX", 50*time.Millisecond, true),
		completionDelayMin: envDuration("STRESS_COMPLETION_DELAY_MIN", 50*time.Millisecond, true),
		completionDelayMax: envDuration("STRESS_COMPLETION_DELAY_MAX", 50*time.Millisecond, true),
		poolBudget:         envInt("STRESS_POSTGRES_MAX_CONNS", 200),
		pollers:            envInt("STRESS_WORKFLOW_POLLERS", 4),
	}
	if v := os.Getenv("STRESS_FAILURE_RATE"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			cfg.failureRate = f
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
// Scripted delay recorder for stress testing
// ---------------------------------------------------------------------------

// scriptedDelayRecorder implements scripted.DelayRecorder using sync.Map
// to capture the delays resolved by the scripted agent per delivery,
// enabling overhead computation (observed − agent delay).
type scriptedDelayRecorder struct {
	delays sync.Map // domain.DeliveryID → scripted.DelayRecord
}

func newScriptedDelayRecorder() *scriptedDelayRecorder {
	return &scriptedDelayRecorder{}
}

func (r *scriptedDelayRecorder) RecordDelay(deliveryID domain.DeliveryID, record scripted.DelayRecord) {
	r.delays.Store(deliveryID, record)
}

func (r *scriptedDelayRecorder) getDelays() map[domain.DeliveryID]scripted.DelayRecord {
	delays := make(map[domain.DeliveryID]scripted.DelayRecord)
	r.delays.Range(func(key, value interface{}) bool {
		delays[key.(domain.DeliveryID)] = value.(scripted.DelayRecord)
		return true
	})
	return delays
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
//   - mutation-to-ack latency (mutation commit → ack received)
//   - mid-rollout generation advances and reconciliation restarts
type stressObserver struct {
	domain.NoOpFulfillmentObserver

	mu   sync.Mutex
	runs []*reconcileRun // all runs, append-only

	// Drain tracking: unique fulfillments that completed at least one run.
	completedFulfillments map[domain.FulfillmentID]struct{}
	done                  chan struct{} // closed when len(completedFulfillments) >= target
	closeOnce             sync.Once
	target                int

	// Convergence loop error tracking. Context-cancellation errors are
	// excluded since they are expected during test shutdown.
	convergenceErrors atomic.Int64

	// deliveryMu protects deliveries and mutationTimes. These are
	// grouped under a single lock because reporting joins them
	// (mutation-to-ack latency, overhead computation).
	deliveryMu    sync.Mutex
	deliveries    map[domain.DeliveryID]*deliveryMetrics
	mutationTimes map[domain.FulfillmentID]map[domain.Generation]time.Time
}

func newStressObserver(expectedCount int) *stressObserver {
	return &stressObserver{
		completedFulfillments: make(map[domain.FulfillmentID]struct{}),
		done:                  make(chan struct{}),
		target:                expectedCount,
		deliveries:            make(map[domain.DeliveryID]*deliveryMetrics),
		mutationTimes:         make(map[domain.FulfillmentID]map[domain.Generation]time.Time),
	}
}

func (o *stressObserver) signalDone() {
	o.closeOnce.Do(func() { close(o.done) })
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
			p.observer.signalDone()
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

// MutationRecorded records the wall-clock time at which the workload
// committed a mutation for the given (fulfillmentID, generation) pair.
func (o *stressObserver) MutationRecorded(fID domain.FulfillmentID, gen domain.Generation, at time.Time) {
	o.deliveryMu.Lock()
	defer o.deliveryMu.Unlock()
	perGen, ok := o.mutationTimes[fID]
	if !ok {
		perGen = make(map[domain.Generation]time.Time)
		o.mutationTimes[fID] = perGen
	}
	perGen[gen] = at
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

// restartsPerRun returns the restart count for each completed run,
// one entry per run. This enables percentile analysis of how many
// times individual runs were interrupted by generation advancement.
func (o *stressObserver) restartsPerRun() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	var out []int
	for _, run := range o.runs {
		if !run.completed.IsZero() {
			out = append(out, run.restarts)
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
func (o *stressObserver) overheads(recorder *scriptedDelayRecorder) (ackOverheads, completionOverheads []time.Duration) {
	o.deliveryMu.Lock()
	defer o.deliveryMu.Unlock()
	delays := recorder.getDelays()
	for did, m := range o.deliveries {
		rec, ok := delays[did]
		if !ok {
			continue
		}
		if !m.dispatchedAt.IsZero() && !m.ackReceivedAt.IsZero() {
			ackOverheads = append(ackOverheads, m.ackReceivedAt.Sub(m.dispatchedAt)-rec.AckLatency)
		}
		if !m.dispatchedAt.IsZero() && !m.completedAt.IsZero() {
			completionOverheads = append(completionOverheads, m.completedAt.Sub(m.dispatchedAt)-rec.CompletionLatency)
		}
	}
	return
}

// mutationToAckLatencies joins mutation timestamps with delivery
// metrics to compute the end-to-end latency from mutation commit
// to ack received. Only deliveries with both a matching mutation
// timestamp and a recorded ack are included.
func (o *stressObserver) mutationToDispatchedLatencies() []time.Duration {
	o.deliveryMu.Lock()
	defer o.deliveryMu.Unlock()

	var out []time.Duration
	for _, m := range o.deliveries {
		if m.ackReceivedAt.IsZero() {
			continue
		}
		perGen, ok := o.mutationTimes[m.fulfillmentID]
		if !ok {
			continue
		}
		mutatedAt, ok := perGen[m.generation]
		if !ok {
			continue
		}
		out = append(out, m.dispatchedAt.Sub(mutatedAt))
	}
	return out
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

func setupStress(t *testing.T, store domain.Store, reg domain.Registry, agent domain.DeliveryAgent, reporter *application.DeliveryReportService, orchOpts ...domain.OrchestrationWorkflowOption) stressHarness {
	t.Helper()

	router := delivery.NewRoutingDeliveryService()
	router.Register(scripted.TargetType, agent)

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
// Connection pool stats sampler
// ---------------------------------------------------------------------------

// poolStatsSample is a point-in-time snapshot of sql.DBStats for one pool.
type poolStatsSample struct {
	TimestampUnixMs int64 `json:"ts_unix_ms"`
	MaxOpen         int   `json:"max_open"`
	Open            int   `json:"open"`
	InUse           int   `json:"in_use"`
	Idle            int   `json:"idle"`
	WaitCount       int64 `json:"wait_count"`       // cumulative
	WaitDurationMs  int64 `json:"wait_duration_ms"` // cumulative
}

// poolStatsTimeSeries holds periodic samples from one or more pools.
type poolStatsTimeSeries struct {
	mu       sync.Mutex
	App      []poolStatsSample `json:"app"`
	Workflow []poolStatsSample `json:"workflow"`
}

// samplePoolStats samples sql.DBStats from the given pool and appends
// to the time series. Either pool may be nil (skipped).
func (ts *poolStatsTimeSeries) sample(appDB, wfDB *sql.DB) {
	now := time.Now().UnixMilli()
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if appDB != nil {
		s := appDB.Stats()
		ts.App = append(ts.App, poolStatsSample{
			TimestampUnixMs: now,
			MaxOpen:         s.MaxOpenConnections,
			Open:            s.OpenConnections,
			InUse:           s.InUse,
			Idle:            s.Idle,
			WaitCount:       s.WaitCount,
			WaitDurationMs:  s.WaitDuration.Milliseconds(),
		})
	}
	if wfDB != nil {
		s := wfDB.Stats()
		ts.Workflow = append(ts.Workflow, poolStatsSample{
			TimestampUnixMs: now,
			MaxOpen:         s.MaxOpenConnections,
			Open:            s.OpenConnections,
			InUse:           s.InUse,
			Idle:            s.Idle,
			WaitCount:       s.WaitCount,
			WaitDurationMs:  s.WaitDuration.Milliseconds(),
		})
	}
}

// startPoolStatsSampler launches a goroutine that samples pool stats
// at the given interval until ctx is cancelled. Returns the time
// series that accumulates results.
func startPoolStatsSampler(ctx context.Context, interval time.Duration, appDB, wfDB *sql.DB) *poolStatsTimeSeries {
	ts := &poolStatsTimeSeries{}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Final sample to capture end state.
				ts.sample(appDB, wfDB)
				return
			case <-ticker.C:
				ts.sample(appDB, wfDB)
			}
		}
	}()
	return ts
}

// ---------------------------------------------------------------------------
// Manifest factory
// ---------------------------------------------------------------------------

// scriptedResourceSpec creates a scripted resource spec with the given stress
// configuration (bounded normal latency and probabilistic failure).
func scriptedResourceSpec(cfg stressConfig) json.RawMessage {
	// Convert delays to protobuf Duration format (seconds with 's' suffix, e.g., "0.05s")
	formatDuration := func(d time.Duration) string {
		if d == 0 {
			return "0s"
		}
		seconds := float64(d) / float64(time.Second)
		return fmt.Sprintf("%.9gs", seconds)
	}

	spec := map[string]any{
		"behavior": map[string]any{
			"delivery": map[string]any{
				"acknowledgement": map[string]any{
					"latency": map[string]any{
						"bounded_normal": map[string]any{
							"min": formatDuration(cfg.ackDelayMin),
							"max": formatDuration(cfg.ackDelayMax),
						},
					},
				},
				"completion": map[string]any{
					"latency": map[string]any{
						"bounded_normal": map[string]any{
							"min": formatDuration(cfg.completionDelayMin),
							"max": formatDuration(cfg.completionDelayMax),
						},
					},
					"outcome": map[string]any{
						"probabilistic": map[string]any{
							"failure_rate": cfg.failureRate,
						},
					},
				},
			},
		},
	}
	raw, _ := json.Marshal(spec)
	return json.RawMessage(raw)
}

// stressScriptedManifest creates a manifest for the scripted delivery agent
// with the given stress configuration.
func stressScriptedManifest(deploymentName string, cfg stressConfig) domain.Manifest {
	spec := scriptedResourceSpec(cfg)
	// Use a unique UID
	uid := domain.NewExtensionResourceUID()
	raw, err := domain.WrapManagedResourceSpec(domain.ResourceName(deploymentName), uid, spec)
	if err != nil {
		// This should not happen in test, but defensive coding
		return domain.Manifest{Raw: json.RawMessage("{}")}
	}
	return domain.Manifest{
		ManifestType: scripted.ManagedManifestType,
		Raw:          raw,
	}
}

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
