//go:build stress

package application_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ---------------------------------------------------------------------------
// Unified distribution computation
// ---------------------------------------------------------------------------

// distribution holds percentile statistics for a set of values.
// This is the single computation type for both duration-based and
// integer-based metrics; callers convert from their domain types
// using the convenience constructors below.
type distribution struct {
	count int
	mean  float64
	min   float64
	p50   float64
	p80   float64
	p90   float64
	p95   float64
	p99   float64
	max   float64
}

func computeDistribution(values []float64) distribution {
	n := len(values)
	if n == 0 {
		return distribution{}
	}
	sorted := make([]float64, n)
	copy(sorted, values)
	sort.Float64s(sorted)

	var total float64
	for _, v := range sorted {
		total += v
	}
	pct := func(p float64) float64 {
		idx := int(math.Ceil(p/100.0*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return sorted[idx]
	}
	return distribution{
		count: n,
		mean:  total / float64(n),
		min:   sorted[0],
		p50:   pct(50),
		p80:   pct(80),
		p90:   pct(90),
		p95:   pct(95),
		p99:   pct(99),
		max:   sorted[n-1],
	}
}

func durationDistribution(durations []time.Duration) distribution {
	values := make([]float64, len(durations))
	for i, d := range durations {
		values[i] = float64(d)
	}
	return computeDistribution(values)
}

func intDistribution(counts []int) distribution {
	values := make([]float64, len(counts))
	for i, c := range counts {
		values[i] = float64(c)
	}
	return computeDistribution(values)
}

// ---------------------------------------------------------------------------
// Report printing helpers
// ---------------------------------------------------------------------------

// printDurationReport logs a distribution with time.Duration formatting.
func printDurationReport(t *testing.T, title string, d distribution) {
	t.Helper()
	t.Logf("\n=== %s ===", title)
	t.Logf("Samples: %d", d.count)
	t.Logf("Mean:    %v", time.Duration(d.mean))
	t.Logf("P50:     %v", time.Duration(d.p50))
	t.Logf("P80:     %v", time.Duration(d.p80))
	t.Logf("P90:     %v", time.Duration(d.p90))
	t.Logf("P95:     %v", time.Duration(d.p95))
	t.Logf("P99:     %v", time.Duration(d.p99))
	t.Logf("Max:     %v", time.Duration(d.max))
}

// printIntReport logs a distribution with integer formatting.
func printIntReport(t *testing.T, title string, extra string, d distribution) {
	t.Helper()
	t.Logf("\n=== %s ===", title)
	t.Logf("Samples: %d", d.count)
	if extra != "" {
		t.Log(extra)
	}
	t.Logf("Min:     %d", int(d.min))
	t.Logf("Mean:    %.1f", d.mean)
	t.Logf("P50:     %d", int(d.p50))
	t.Logf("P80:     %d", int(d.p80))
	t.Logf("P90:     %d", int(d.p90))
	t.Logf("P95:     %d", int(d.p95))
	t.Logf("P99:     %d", int(d.p99))
	t.Logf("Max:     %d", int(d.max))
}

// ---------------------------------------------------------------------------
// JSON serialization types for stress results
// ---------------------------------------------------------------------------

// durationDistJSON is the JSON representation of a duration-valued
// distribution, with all values in milliseconds.
type durationDistJSON struct {
	Samples int     `json:"samples"`
	MeanMs  float64 `json:"mean_ms"`
	P50Ms   float64 `json:"p50_ms"`
	P80Ms   float64 `json:"p80_ms"`
	P90Ms   float64 `json:"p90_ms"`
	P95Ms   float64 `json:"p95_ms"`
	P99Ms   float64 `json:"p99_ms"`
	MaxMs   float64 `json:"max_ms"`
}

func (d distribution) toMillisJSON() *durationDistJSON {
	if d.count == 0 {
		return nil
	}
	ms := 1.0 / float64(time.Millisecond)
	return &durationDistJSON{
		Samples: d.count,
		MeanMs:  d.mean * ms,
		P50Ms:   d.p50 * ms,
		P80Ms:   d.p80 * ms,
		P90Ms:   d.p90 * ms,
		P95Ms:   d.p95 * ms,
		P99Ms:   d.p99 * ms,
		MaxMs:   d.max * ms,
	}
}

// intDistJSON is the JSON representation of an integer-valued
// distribution (e.g. restart counts, run counts).
type intDistJSON struct {
	Samples int     `json:"samples"`
	Mean    float64 `json:"mean"`
	Min     int     `json:"min"`
	P50     int     `json:"p50"`
	P80     int     `json:"p80"`
	P90     int     `json:"p90"`
	P95     int     `json:"p95"`
	P99     int     `json:"p99"`
	Max     int     `json:"max"`
}

func (d distribution) toIntJSON() *intDistJSON {
	if d.count == 0 {
		return nil
	}
	return &intDistJSON{
		Samples: d.count,
		Mean:    d.mean,
		Min:     int(d.min),
		P50:     int(d.p50),
		P80:     int(d.p80),
		P90:     int(d.p90),
		P95:     int(d.p95),
		P99:     int(d.p99),
		Max:     int(d.max),
	}
}

type deploymentDetail struct {
	Name            string `json:"name"`
	Version         int    `json:"version"`
	Generation      int    `json:"generation"`
	ObservedGen     int    `json:"observed_generation"`
	State           string `json:"state"`
	Converged       bool   `json:"converged"`
	GenerationDelta int    `json:"generation_delta"`
}

type deliveryDetail struct {
	DeliveryID           string  `json:"delivery_id"`
	FulfillmentID        string  `json:"fulfillment_id"`
	Generation           int     `json:"generation"`
	AckLatencyMs         float64 `json:"ack_latency_ms,omitempty"`
	CompLatencyMs        float64 `json:"completion_latency_ms,omitempty"`
	AckOverheadMs        float64 `json:"ack_overhead_ms,omitempty"`
	CompOverheadMs       float64 `json:"completion_overhead_ms,omitempty"`
	AgentAckDelayMs      float64 `json:"agent_ack_delay_ms,omitempty"`
	AgentCompDelayMs     float64 `json:"agent_completion_delay_ms,omitempty"`
	MutationAtUnixMs     int64   `json:"mutation_at_unix_ms,omitempty"`
	MutationToDispatchMs float64 `json:"mutation_to_dispatch_ms,omitempty"`
	State                string  `json:"state,omitempty"`
}

// ---------------------------------------------------------------------------
// Consolidated stress-test results
// ---------------------------------------------------------------------------

// reportStressResults prints all latency, convergence, configuration,
// and operation summaries at the end of a stress run.
func reportStressResults(
	ctx context.Context,
	t *testing.T,
	cfg stressConfig,
	observer *stressObserver,
	delayRecorder *scriptedDelayRecorder,
	store domain.Store,
	stats workloadStats,
	poolStats *poolStatsTimeSeries,
) {
	t.Helper()

	// 1. Reconciliation latency (RunStarted → End)
	reconDurations := observer.reconciliationLatencies()
	if len(reconDurations) > 0 {
		printDurationReport(t, "Reconciliation Latency (RunStarted → End)", durationDistribution(reconDurations))
	}

	// 2. Ack latency (dispatch → ack received)
	ackDurations := observer.ackLatencies()
	if len(ackDurations) > 0 {
		printDurationReport(t, "Ack Latency (Dispatch → AckReceived)", durationDistribution(ackDurations))
	}

	// 3. Completion latency (dispatch → completed)
	completionDurations := observer.completionLatencies()
	if len(completionDurations) > 0 {
		printDurationReport(t, "Completion Latency (Dispatch → Completed)", durationDistribution(completionDurations))
	}

	// 4. Overhead = observed latency - agent delay
	ackOverheads, completionOverheads := observer.overheads(delayRecorder)
	if len(ackOverheads) > 0 {
		printDurationReport(t, "Ack Overhead (AckLatency − AgentAckDelay)", durationDistribution(ackOverheads))
	}
	if len(completionOverheads) > 0 {
		printDurationReport(t, "Completion Overhead (CompletionLatency − AgentCompletionDelay)", durationDistribution(completionOverheads))
	}

	// 4b. Mutation-to-ack latency (mutation commit → dispatched to delivery Agent)
	mutationToDispatchDurations := observer.mutationToDispatchedLatencies()
	if len(mutationToDispatchDurations) > 0 {
		printDurationReport(t, "Mutation-to-Dispatch Latency (MutationCommit → Dispatched)", durationDistribution(mutationToDispatchDurations))
	}

	// 5. Runs per fulfillment
	reportRunsPerFulfillment(t, observer)

	// 5b. Restarts per run
	reportRestartsPerRun(t, observer)

	// 6. Generation advancement (observed via probes)
	reportGenerationAdvancement(t, observer)

	// 7. Generation convergence (final store snapshot)
	views, convergedCount, laggedCount := reportConvergence(ctx, t, store)

	// 8. Configuration summary
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

	// 9. Operation summary
	totalOps := stats.createCount + stats.updateCount + stats.createErrors + stats.updateErrors
	t.Logf("\n=== Operations ===")
	t.Logf("Creates:            %d (errors: %d)", stats.createCount, stats.createErrors)
	t.Logf("Updates:            %d (errors: %d)", stats.updateCount, stats.updateErrors)
	t.Logf("Total ops:          %d", totalOps)
	t.Logf("Max gen bumps:      %d (highest version for a single deployment)", stats.maxVersion)
	if ce := observer.convergenceErrors.Load(); ce > 0 {
		t.Logf("Convergence errors: %d (async convergence loops that failed)", ce)
	}

	dumpResults(t, cfg, observer, delayRecorder, views, stats, convergedCount, laggedCount, poolStats)
}

// reportRunsPerFulfillment logs a distribution of how many completed
// orchestration runs each fulfillment accumulated.
func reportRunsPerFulfillment(t *testing.T, observer *stressObserver) {
	t.Helper()
	counts := observer.runsPerFulfillment()
	if len(counts) == 0 {
		return
	}
	var total int
	for _, c := range counts {
		total += c
	}
	printIntReport(t, "Runs Per Fulfillment", fmt.Sprintf("Total runs:   %d", total), intDistribution(counts))
}

// reportRestartsPerRun logs a distribution of how many inner-loop
// restarts each completed orchestration run accumulated.
func reportRestartsPerRun(t *testing.T, observer *stressObserver) {
	t.Helper()
	counts := observer.restartsPerRun()
	if len(counts) == 0 {
		return
	}
	var total int
	for _, c := range counts {
		total += c
	}
	printIntReport(t, "Restarts Per Run", fmt.Sprintf("Total:        %d", total), intDistribution(counts))
}

// reportGenerationAdvancement logs reconciliation restarts and
// mid-rollout generation advancement events observed via probes.
func reportGenerationAdvancement(t *testing.T, observer *stressObserver) {
	t.Helper()
	restarts := observer.totalRestarts()
	totalAdvances, skipSizes := observer.generationAdvanceStats()

	t.Logf("\n=== Generation Advancement ===")
	t.Logf("Reconciliation restarts:  %d (inner-loop restarts due to generation advance)", restarts)
	t.Logf("Mid-rollout advances:     %d (detected between rollout steps)", totalAdvances)

	if len(skipSizes) > 0 {
		d := intDistribution(skipSizes)
		t.Logf("Mid-rollout skip sizes:   mean=%.1f, p50=%d, p95=%d, max=%d",
			d.mean, int(d.p50), int(d.p95), int(d.max))
	}
}

// reportConvergence queries the store for final fulfillment state,
// logs a generation convergence summary, and returns the snapshot
// so callers (e.g. dumpResults) can reuse it without a second query.
func reportConvergence(ctx context.Context, t *testing.T, store domain.Store) (views []domain.DeploymentView, convergedCount, laggedCount int) {
	t.Helper()
	t.Logf("\n=== Generation Convergence ===")

	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Logf("could not query final state: %v", err)
		return nil, 0, 0
	}
	views, err = tx.Deployments().ListView(ctx)
	tx.Rollback()
	if err != nil {
		t.Logf("could not list deployments: %v", err)
		return nil, 0, 0
	}

	var totalGenSkips int
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
	return views, convergedCount, laggedCount
}

// ---------------------------------------------------------------------------
// JSON dump
// ---------------------------------------------------------------------------

// stressResultsJSON is the top-level structure serialized to the
// stress-test results file. It combines configuration, operation
// counts, latency distributions, per-deployment/delivery details,
// and connection pool time series.
type stressResultsJSON struct {
	Config struct {
		Targets         int     `json:"targets"`
		Deployments     int     `json:"deployments"`
		Rate            int     `json:"rate_per_sec"`
		Duration        string  `json:"duration"`
		AckDelay        string  `json:"ack_delay"`
		CompletionDelay string  `json:"completion_delay"`
		FailureRate     float64 `json:"failure_rate"`
		PoolBudget      int     `json:"pool_budget"`
		Pollers         int     `json:"pollers"`
	} `json:"config"`
	Operations struct {
		TotalOps     int `json:"total_ops"`
		Creates      int `json:"creates"`
		CreateErrors int `json:"create_errors"`
		Updates      int `json:"updates"`
		UpdateErrors int `json:"update_errors"`
		MaxVersion   int `json:"max_version"`
	} `json:"operations"`
	Elapsed           string `json:"elapsed"`
	OrchestraRestarts int    `json:"orchestration_restarts"`
	Converged         int    `json:"converged"`
	Lagged            int    `json:"lagged"`
	Latencies         struct {
		Reconciliation     *durationDistJSON `json:"reconciliation,omitempty"`
		Ack                *durationDistJSON `json:"ack,omitempty"`
		Completion         *durationDistJSON `json:"completion,omitempty"`
		AckOverhead        *durationDistJSON `json:"ack_overhead,omitempty"`
		CompOverhead       *durationDistJSON `json:"completion_overhead,omitempty"`
		MutationToDispatch *durationDistJSON `json:"mutation_to_dispatch,omitempty"`
	} `json:"latencies"`
	RestartsPerRun  *intDistJSON         `json:"restarts_per_run,omitempty"`
	WorkloadEndedAt int64                `json:"workload_ended_at_unix_ms"`
	PoolStats       *poolStatsTimeSeries `json:"pool_stats,omitempty"`
	Deployments     []deploymentDetail   `json:"deployments"`
	Deliveries      []deliveryDetail     `json:"deliveries"`
}

// buildDeliveryDetails joins observer delivery metrics with agent delay
// records and mutation timestamps to produce per-delivery JSON details.
// Acquires observer.deliveryMu internally.
func buildDeliveryDetails(observer *stressObserver, delayRecorder *scriptedDelayRecorder) []deliveryDetail {
	observer.deliveryMu.Lock()
	defer observer.deliveryMu.Unlock()

	delays := delayRecorder.getDelays()

	details := make([]deliveryDetail, 0, len(observer.deliveries))
	for did, m := range observer.deliveries {
		dd := deliveryDetail{
			DeliveryID:    string(did),
			FulfillmentID: string(m.fulfillmentID),
			Generation:    int(m.generation),
			State:         string(m.state),
		}
		if !m.dispatchedAt.IsZero() && !m.ackReceivedAt.IsZero() {
			dd.AckLatencyMs = float64(m.ackReceivedAt.Sub(m.dispatchedAt)) / float64(time.Millisecond)
		}
		if !m.dispatchedAt.IsZero() && !m.completedAt.IsZero() {
			dd.CompLatencyMs = float64(m.completedAt.Sub(m.dispatchedAt)) / float64(time.Millisecond)
		}
		if rec, ok := delays[did]; ok {
			dd.AgentAckDelayMs = float64(rec.AckLatency) / float64(time.Millisecond)
			dd.AgentCompDelayMs = float64(rec.CompletionLatency) / float64(time.Millisecond)
			if dd.AckLatencyMs > 0 {
				dd.AckOverheadMs = dd.AckLatencyMs - dd.AgentAckDelayMs
			}
			if dd.CompLatencyMs > 0 {
				dd.CompOverheadMs = dd.CompLatencyMs - dd.AgentCompDelayMs
			}
		}
		// Join with mutation timestamps for mutation-to-dispatch latency.
		if perGen, ok := observer.mutationTimes[m.fulfillmentID]; ok {
			if mutatedAt, ok := perGen[m.generation]; ok {
				dd.MutationAtUnixMs = mutatedAt.UnixMilli()
				if !m.dispatchedAt.IsZero() {
					dd.MutationToDispatchMs = float64(m.dispatchedAt.Sub(mutatedAt)) / float64(time.Millisecond)
				}
			}
		}
		details = append(details, dd)
	}
	return details
}

func dumpResults(
	t *testing.T, cfg stressConfig,
	observer *stressObserver, delayRecorder *scriptedDelayRecorder,
	views []domain.DeploymentView, stats workloadStats,
	convergedCount, laggedCount int,
	poolStats *poolStatsTimeSeries,
) {
	outputPath := os.Getenv("STRESS_OUTPUT")
	if outputPath == "" {
		outputPath = fmt.Sprintf("/tmp/stress-results-%d.json", time.Now().Unix())
	}

	var out stressResultsJSON

	// Config
	out.Config.Targets = stressNumTargets
	out.Config.Deployments = cfg.totalDeployments
	out.Config.Rate = cfg.ratePerSecond
	out.Config.Duration = cfg.duration.String()
	out.Config.AckDelay = fmt.Sprintf("%v..%v", cfg.ackDelayMin, cfg.ackDelayMax)
	out.Config.CompletionDelay = fmt.Sprintf("%v..%v", cfg.completionDelayMin, cfg.completionDelayMax)
	out.Config.FailureRate = cfg.failureRate
	out.Config.PoolBudget = cfg.poolBudget
	out.Config.Pollers = cfg.pollers

	// Operations
	out.Operations.Creates = stats.createCount
	out.Operations.CreateErrors = stats.createErrors
	out.Operations.Updates = stats.updateCount
	out.Operations.UpdateErrors = stats.updateErrors
	out.Operations.MaxVersion = stats.maxVersion
	out.Operations.TotalOps = stats.createCount + stats.updateCount + stats.createErrors + stats.updateErrors

	// Summary stats
	out.OrchestraRestarts = observer.totalRestarts()
	out.Converged = convergedCount
	out.Lagged = laggedCount
	out.WorkloadEndedAt = stats.endedAt.UnixMilli()

	// Latency distributions — computed directly from observer.
	out.Latencies.Reconciliation = durationDistribution(observer.reconciliationLatencies()).toMillisJSON()
	out.Latencies.Ack = durationDistribution(observer.ackLatencies()).toMillisJSON()
	out.Latencies.Completion = durationDistribution(observer.completionLatencies()).toMillisJSON()

	ackOH, compOH := observer.overheads(delayRecorder)
	out.Latencies.AckOverhead = durationDistribution(ackOH).toMillisJSON()
	out.Latencies.CompOverhead = durationDistribution(compOH).toMillisJSON()
	out.Latencies.MutationToDispatch = durationDistribution(observer.mutationToDispatchedLatencies()).toMillisJSON()

	out.RestartsPerRun = intDistribution(observer.restartsPerRun()).toIntJSON()

	// Per-deployment details from the convergence snapshot (same
	// ListView result used for converged/lagged counts).
	for _, v := range views {
		gen := int(v.Fulfillment.Generation())
		obsGen := int(v.Fulfillment.ObservedGeneration())
		out.Deployments = append(out.Deployments, deploymentDetail{
			Name:            string(v.Deployment.Name()),
			Version:         gen, // version == generation for stress test
			Generation:      gen,
			ObservedGen:     obsGen,
			State:           string(v.Fulfillment.State()),
			Converged:       obsGen >= gen,
			GenerationDelta: gen - obsGen,
		})
	}

	// Per-delivery details (joins delivery metrics, agent delays,
	// and mutation timestamps under observer.deliveryMu).
	out.Deliveries = buildDeliveryDetails(observer, delayRecorder)

	// Connection pool time series — copy the slices under the lock so
	// json.MarshalIndent serialises a stable snapshot, not the
	// sampler-owned data that may still be appended to.
	if poolStats != nil {
		poolStats.mu.Lock()
		snapshot := &poolStatsTimeSeries{
			App:      make([]poolStatsSample, len(poolStats.App)),
			Workflow: make([]poolStatsSample, len(poolStats.Workflow)),
		}
		copy(snapshot.App, poolStats.App)
		copy(snapshot.Workflow, poolStats.Workflow)
		poolStats.mu.Unlock()
		out.PoolStats = snapshot
	}

	jsonData, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		t.Logf("could not marshal results JSON: %v", err)
	} else {
		if err := os.WriteFile(outputPath, jsonData, 0o644); err != nil {
			t.Logf("could not write results file: %v", err)
		} else {
			t.Logf("\n=== Results written to %s ===", outputPath)
		}
	}
}
