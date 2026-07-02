package postgres

// This file benchmarks application.InventoryReportService --
// ReplaceBatch/ApplyDeltaBatch -- on top of the exact same corpus and
// input shapes inventory_bench_test.go's BenchmarkCTE_* family uses
// for ExtensionResourceRepo directly, so the two are apples-to-apples:
// any delta between an app-layer number here and its repo-layer
// counterpart there is orchestration overhead (BEGIN/COMMIT, the
// GetType lookup, resolveBatch's bookkeeping, CandidateUID/ReceivedAt
// assembly), not a difference in what SQL actually runs.
//
// Lives in `package postgres` (not `package application`) purely for
// mechanical reasons: Go test files aren't visible outside their own
// package's test binary, so benchState and everything it seeds
// (corpus, label/condition/observation generators, the shared bench
// container) are only reachable from here. application depends on
// domain but never on postgres, so importing application from a
// postgres-package test file doesn't create an import cycle. Nothing
// about the service under test is postgres-specific.
//
// Run with: go test ./internal/infrastructure/postgres/ -bench=BenchmarkApp -benchtime=1s

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ---------------------------------------------------------------------------
// Input builders -- application.InventoryReportService shapes
// ---------------------------------------------------------------------------

// buildAppUpdateReplacements is buildUpdateReplacements's
// application-layer counterpart: same pool draw (steadyAliasPool or
// neverAliasPool, depending on withAlias), same label/observation/condition
// generators, but as
// application.InventoryReplacementInput (identified by Name/Aliases,
// not a pre-resolved domain.InventoryReplacement) since resolving that
// identity is exactly the orchestration this file measures.
func buildAppUpdateReplacements(st *benchState, n int, withAlias bool) []application.InventoryReplacementInput {
	// See buildUpdateReplacements's matching comment: withAlias picks
	// the pool, not just the payload.
	var indices []int
	if withAlias {
		indices = st.nextSteadyAliasIndices(n)
	} else {
		indices = st.nextNeverAliasIndices(n)
	}
	gen := st.nextGen()
	now := time.Now().UTC()
	reps := make([]application.InventoryReplacementInput, n)
	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(gen*int64(benchCorpusSize) + int64(idx)))
		name := benchResourceName(idx)
		rep := application.InventoryReplacementInput{
			ResourceType: benchResourceType,
			Name:         &name,
			Labels:       benchLabels(gen, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, gen),
			ObservedAt:   now,
		}
		if withAlias {
			rep.Aliases = []domain.Alias{{Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchAliasValue(idx))}}
		}
		reps[i] = rep
	}
	return reps
}

// buildAppUpdateByAliasOnly is the one input shape with no
// repo-layer counterpart at all: every report is identified purely by
// its already-resolved alias (Name left nil), forcing
// reportResolver.resolveBatch through ResolveAliasesBatch's real round
// trip and resolveByAliases's Go-side matching -- the two things a
// by-Name report skips entirely. seedAliasCorpus already gave every
// resource in [0, benchCorpusSize) exactly one resolvable alias, so
// every report here resolves successfully on the first try.
func buildAppUpdateByAliasOnly(st *benchState, n int) []application.InventoryReplacementInput {
	indices := st.nextSteadyAliasIndices(n)
	gen := st.nextGen()
	now := time.Now().UTC()
	reps := make([]application.InventoryReplacementInput, n)
	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(gen*int64(benchCorpusSize) + int64(idx)))
		reps[i] = application.InventoryReplacementInput{
			ResourceType: benchResourceType,
			Aliases:      []domain.Alias{{Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchAliasValue(idx))}},
			Labels:       benchLabels(gen, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, gen),
			ObservedAt:   now,
		}
	}
	return reps
}

// buildAppInsertReplacements is buildInsertReplacements's
// application-layer counterpart.
func buildAppInsertReplacements(st *benchState, n int) []application.InventoryReplacementInput {
	indices := st.nextNewIndices(n)
	now := time.Now().UTC()
	reps := make([]application.InventoryReplacementInput, n)
	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(int64(idx)))
		name := benchResourceName(idx)
		reps[i] = application.InventoryReplacementInput{
			ResourceType: benchResourceType,
			Name:         &name,
			Labels:       benchLabels(0, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, 0),
			ObservedAt:   now,
		}
	}
	return reps
}

// buildAppHeartbeatDeltas is buildHeartbeatDeltas's application-layer
// counterpart.
func buildAppHeartbeatDeltas(st *benchState, n int) []application.InventoryDeltaInput {
	indices := st.nextNeverAliasIndices(n)
	now := time.Now().UTC()
	deltas := make([]application.InventoryDeltaInput, n)
	for i, idx := range indices {
		name := benchResourceName(idx)
		deltas[i] = application.InventoryDeltaInput{
			ResourceType: benchResourceType,
			Name:         &name,
			ObservedAt:   now,
		}
	}
	return deltas
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

// BenchmarkApp_ReplaceBatch_UpdateExisting is
// BenchmarkCTE_ReplaceInventory_UpdateExisting's application-layer
// counterpart, run through InventoryReportService.ReplaceBatch instead
// of ExtensionResourceRepo.ReplaceInventory directly. Note that
// InventoryReportService internally splits a batch larger than
// defaultReportChunkSize (1000) into multiple chunks, each its own
// round trip inside the same transaction -- so unlike the repo-layer
// benchmark, this one's batch=2500 case is not one statement but three
// (1000+1000+500), which is real production behavior this benchmark
// intentionally does not hide.
func BenchmarkApp_ReplaceBatch_UpdateExisting(b *testing.B) {
	st := setupBenchOnce(b)
	svc := application.NewInventoryReportService(&Store{DB: st.db})
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				in := application.InventoryReplacementBatchInput{Reports: buildAppUpdateReplacements(st, batchSize, false)}
				if err := svc.ReplaceBatch(ctx, in); err != nil {
					b.Fatalf("ReplaceBatch: %v", err)
				}
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkApp_ReplaceBatch_InsertNew is
// BenchmarkCTE_ReplaceInventory_InsertNew's application-layer
// counterpart.
func BenchmarkApp_ReplaceBatch_InsertNew(b *testing.B) {
	st := setupBenchOnce(b)
	svc := application.NewInventoryReportService(&Store{DB: st.db})
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				in := application.InventoryReplacementBatchInput{Reports: buildAppInsertReplacements(st, batchSize)}
				if err := svc.ReplaceBatch(ctx, in); err != nil {
					b.Fatalf("ReplaceBatch: %v", err)
				}
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkApp_ReplaceBatch_ResolveByAlias has no repo-layer
// counterpart: every report in the batch is identified purely by an
// already-resolved alias (see buildAppUpdateByAliasOnly), so this
// isolates reportResolver's ResolveAliasesBatch round trip plus its
// Go-side resolveByAliases matching -- cost a by-Name report (every
// other benchmark in this file and inventory_bench_test.go) never
// pays at all.
func BenchmarkApp_ReplaceBatch_ResolveByAlias(b *testing.B) {
	st := setupBenchOnce(b)
	svc := application.NewInventoryReportService(&Store{DB: st.db})
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				in := application.InventoryReplacementBatchInput{Reports: buildAppUpdateByAliasOnly(st, batchSize)}
				if err := svc.ReplaceBatch(ctx, in); err != nil {
					b.Fatalf("ReplaceBatch: %v", err)
				}
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkApp_ApplyDeltaBatch_Heartbeat is
// BenchmarkCTE_ApplyInventoryDeltas_Heartbeat's application-layer
// counterpart.
func BenchmarkApp_ApplyDeltaBatch_Heartbeat(b *testing.B) {
	st := setupBenchOnce(b)
	svc := application.NewInventoryReportService(&Store{DB: st.db})
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			b.ResetTimer()
			for range b.N {
				in := application.InventoryDeltaBatchInput{Reports: buildAppHeartbeatDeltas(st, batchSize)}
				if err := svc.ApplyDeltaBatch(ctx, in); err != nil {
					b.Fatalf("ApplyDeltaBatch: %v", err)
				}
			}
			reportPerItem(b, batchSize)
		})
	}
}
