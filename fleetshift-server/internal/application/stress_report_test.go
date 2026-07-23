//go:build stress

package application_test

import (
	"context"
	"math"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

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
// Consolidated stress-test results
// ---------------------------------------------------------------------------

// reportStressResults prints all latency, convergence, configuration,
// and operation summaries at the end of a stress run.
func reportStressResults(
	t *testing.T,
	cfg stressConfig,
	observer *stressObserver,
	agent *delayedDeliveryAgent,
	store domain.Store,
	stats workloadStats,
) {
	t.Helper()

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

	// 5. Runs per fulfillment
	reportRunsPerFulfillment(t, observer)

	// 6. Generation advancement (observed via probes)
	reportGenerationAdvancement(t, observer)

	// 7. Generation convergence (final store snapshot)
	reportConvergence(t, store)

	// 8. Configuration summary
	t.Logf("\n=== Configuration ===")
	t.Logf("Targets:            %d", stressNumTargets)
	t.Logf("Deployment pool:    %d", cfg.totalDeployments)
	t.Logf("Rate:               %d/sec", cfg.ratePerSecond)
	t.Logf("Duration limit:     %v", cfg.duration)
	t.Logf("Ack delay:          %v..%v (gaussian)", cfg.ackDelayMin, cfg.ackDelayMax)
	t.Logf("Completion delay:   %v..%v (gaussian)", cfg.completionDelayMin, cfg.completionDelayMax)
	t.Logf("Failure rate:       %.1f%%", cfg.failureRate*100)
	t.Logf("Pool budget:        %d (app=%d, wf=%d, pg_max_connections=%d)",
		cfg.poolBudget, cfg.appPoolSize(), cfg.wfPoolSize(), cfg.poolBudget+20)
	t.Logf("Pollers:            %d", cfg.pollers)

	// 9. Operation summary
	t.Logf("\n=== Operations ===")
	t.Logf("Creates:            %d (errors: %d)", stats.createCount, stats.createErrors)
	t.Logf("Updates:            %d (errors: %d)", stats.updateCount, stats.updateErrors)
	t.Logf("Total ops:          %d", stats.createCount+stats.updateCount+stats.createErrors+stats.updateErrors)
	t.Logf("Max gen bumps:      %d (highest version for a single deployment)", stats.maxVersion)
	if ce := atomic.LoadInt64(&observer.convergenceErrors); ce > 0 {
		t.Logf("Convergence errors: %d (async convergence loops that failed)", ce)
	}
}

// reportRunsPerFulfillment logs a distribution of how many completed
// orchestration runs each fulfillment accumulated.
func reportRunsPerFulfillment(t *testing.T, observer *stressObserver) {
	t.Helper()
	counts := observer.runsPerFulfillment()
	if len(counts) == 0 {
		return
	}
	sort.Ints(counts)

	var total int
	for _, c := range counts {
		total += c
	}
	n := len(counts)
	pct := func(p float64) int {
		idx := int(math.Ceil(p/100.0*float64(n))) - 1
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return counts[idx]
	}

	t.Logf("\n=== Runs Per Fulfillment ===")
	t.Logf("Fulfillments: %d", n)
	t.Logf("Total runs:   %d", total)
	t.Logf("Min:          %d", counts[0])
	t.Logf("Mean:         %.1f", float64(total)/float64(n))
	t.Logf("P50:          %d", pct(50))
	t.Logf("P95:          %d", pct(95))
	t.Logf("P99:          %d", pct(99))
	t.Logf("Max:          %d", counts[n-1])
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
		sort.Ints(skipSizes)
		n := len(skipSizes)
		var total int
		for _, s := range skipSizes {
			total += s
		}
		pct := func(p float64) int {
			idx := int(math.Ceil(p/100.0*float64(n))) - 1
			if idx < 0 {
				idx = 0
			}
			if idx >= n {
				idx = n - 1
			}
			return skipSizes[idx]
		}
		t.Logf("Mid-rollout skip sizes:   mean=%.1f, p50=%d, p95=%d, max=%d",
			float64(total)/float64(n), pct(50), pct(95), skipSizes[n-1])
	}
}

// reportConvergence queries the store for final fulfillment state and
// logs a generation convergence summary.
func reportConvergence(t *testing.T, store domain.Store) {
	t.Helper()
	t.Logf("\n=== Generation Convergence ===")

	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Logf("could not query final state: %v", err)
		return
	}
	views, err := tx.Deployments().ListView(context.Background())
	tx.Rollback()
	if err != nil {
		t.Logf("could not list deployments: %v", err)
		return
	}

	var convergedCount, laggedCount, totalGenSkips int
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
