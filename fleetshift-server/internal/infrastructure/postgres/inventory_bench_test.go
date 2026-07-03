package postgres

// This file benchmarks the "massive CTE" inventory write path
// (ReplaceInventory/ApplyInventoryDeltas -- see extension_resource_repo.go's
// section doc comment) against a much simpler baseline modeled on Red Hat
// ACM Search's write path: a single (uid, cluster, data jsonb) table,
// upserted with a change-guarded `ON CONFLICT ... WHERE data IS DISTINCT
// FROM` statement, pipelined via native pgx.Batch instead of database/sql
// (see docs/design/reference/acm_search_indexing.md). Relationship
// edges are intentionally out of scope for the baseline, per request --
// this is strictly a single-table write-path comparison.
//
// Lives in `package postgres` (not `postgres_test`) so it can reference
// the production CTE constants (replaceInventorySQL etc.) directly for
// EXPLAIN capture, with zero risk of the benchmark's copy of the SQL
// drifting from what ReplaceInventory/ApplyInventoryDeltas actually run.
//
// Run with: go test ./internal/infrastructure/postgres/ -bench=. -benchtime=1s -v
// (the -v is what surfaces the EXPLAIN ANALYZE plans printed to stdout;
// omit it to see just the benchmark timing table).

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
)

// ---------------------------------------------------------------------------
// Corpus shape
// ---------------------------------------------------------------------------

const (
	benchServiceName    = "bench.fleetshift.io"
	benchTypeName       = "Widget"
	benchCollectionName = "widgets"

	// benchCorpusSize is the number of pre-existing resources seeded
	// before any benchmark runs, so every benchmark measures against a
	// "steady state" fleet-sized table rather than an empty one -- the
	// planner needs real row counts (via ANALYZE, below) to make
	// realistic choices between e.g. a nested-loop index lookup and a
	// sequential scan for the batch's natural-key joins.
	benchCorpusSize = 100_000

	benchLabelsPerResource     = 4
	benchConditionsPerResource = 2
	benchSeedChunkRows         = 5_000

	// benchExplainBatchSize is the one batch size, of benchBatchSizes
	// below, that EXPLAIN ANALYZE plans are captured for -- chosen to
	// match InventoryReportService's real default chunk size so the
	// captured plan is the one production actually runs.
	benchExplainBatchSize = 1000
)

const benchResourceType = domain.ResourceType(benchServiceName + "/" + benchTypeName)

// benchServiceNameB/benchResourceTypeB is a *second* contributor,
// registered as its own extension_resource_types row (see
// buildBenchState) purely so buildCrossContributorConflictReplacements
// and buildMixedAliasReplacements's cross-contributor bucket can
// report as a genuinely different source_extension_resource_uid than
// benchResourceType's contributor for the very same platform resource
// (same Name -- extension_resources' natural key is
// (service_name, collection_name, resource_id), so two service names
// reporting the same collection_name/resource_id resolve to two
// different extension_resources rows sharing one platform_resources
// row). Without a second real contributor, there's no way to exercise
// AliasConflictResourceHasDifferentValue (sibling_holds=true) at
// all -- every other alias scenario in this file, including
// self-replace, only ever involves one contributor.
const (
	benchServiceNameB  = "bench-b.fleetshift.io"
	benchResourceTypeB = domain.ResourceType(benchServiceNameB + "/" + benchTypeName)
)

const (
	benchAliasNamespace domain.AliasNamespace = "ext-id"
	benchAliasKey       domain.AliasKey       = "source-id"

	// benchSecondaryAliasKey is never seeded for anyone -- it exists
	// purely so buildMixedAliasReplacements/buildValueClaimedByOtherReplacements
	// can report a "genuinely new alias" for an already-existing
	// resource without that report colliding with the resource's own
	// steady-state benchAliasKey alias (which would make it a no-op
	// instead).
	benchSecondaryAliasKey domain.AliasKey = "secondary-id"

	// benchConflictVictimCount is the number of dedicated platform
	// resources seeded (see seedConflictVictims) purely to be the
	// "other, pre-existing owner" in buildValueClaimedByOtherReplacements's
	// and buildMixedAliasReplacements's AliasConflictValueClaimedByOther
	// rows. It must be at least the largest number of such rows any
	// single call can produce -- which, now that
	// buildValueClaimedByOtherReplacements exists to report this
	// outcome for 100% of a batch, is the largest entry in
	// benchBatchSizes (2500) -- so that every row gets a distinct
	// victim: reusing a victim within one batch would make two
	// different targets claim the same alias value in the same call,
	// which checkAliasBatchConsistency rejects as an intra-batch
	// contradiction before the SQL ever runs, rather than exercising
	// the against-pre-existing-state conflict path these benchmarks
	// want to measure.
	benchConflictVictimCount = 2500
)

// benchBatchSizes are the report-batch sizes every benchmark family
// sweeps: 100 (well below any chunking), 1000 (InventoryReportService's
// default chunk size), and 2500 (ACM search-indexer's default batch
// size, per docs/design/reference/acm_search_indexing.md).
var benchBatchSizes = []int{100, 1000, 2500}

func benchResourceID(idx int) string { return fmt.Sprintf("r-%08d", idx) }
func benchACMUID(idx int) string     { return fmt.Sprintf("acm-%08d", idx) }
func benchAliasValue(idx int) string { return fmt.Sprintf("ext-%08d", idx) }
func benchClusterOf(idx int) string  { return fmt.Sprintf("cluster-%d", idx%500) }

// benchCrossContribAliasValue is the value contributor B
// (benchResourceTypeB) claims under benchAliasKey in
// buildCrossContributorConflictReplacements/buildMixedAliasReplacements's
// cross-contributor bucket -- a distinct prefix from benchAliasValue
// guarantees it can never collide with contributor A's own value for
// some *other* idx, which would misclassify the row as
// AliasConflictValueClaimedByOther (phase 1) instead of the intended
// AliasConflictResourceHasDifferentValue (phase 2).
func benchCrossContribAliasValue(idx int) string { return fmt.Sprintf("cross-contrib-%08d", idx) }

// benchConflictVictimResourceID/benchConflictVictimAliasValue name
// the dedicated platform resources seedConflictVictims creates -- a
// naming scheme distinct from benchResourceID/benchAliasValue so a
// victim can never accidentally be the same identity/value pair a
// normal corpus resource already owns.
func benchConflictVictimResourceID(i int) string { return fmt.Sprintf("conflict-victim-%08d", i) }
func benchConflictVictimAliasValue(i int) string { return fmt.Sprintf("victim-alias-%08d", i) }

func benchResourceName(idx int) domain.ResourceName {
	return domain.ResourceName(benchCollectionName + "/" + benchResourceID(idx))
}

// benchObservation mirrors a small Kubernetes-style status payload --
// realistic-ish size (a few hundred bytes) rather than a trivial
// {"n":1}, since payload size affects JSONB TOAST/inline behavior and
// thus write cost.
type benchObservation struct {
	Phase     string            `json:"phase"`
	Gen       int64             `json:"gen"`
	NodeCount int               `json:"nodeCount"`
	Version   string            `json:"version"`
	Metadata  map[string]string `json:"metadata"`
}

func benchObservationJSON(gen int64) []byte {
	obs := benchObservation{
		Phase:     "Running",
		Gen:       gen,
		NodeCount: 3,
		Version:   "1.29.4",
		Metadata: map[string]string{
			"region":   "us-east-1",
			"zone":     "us-east-1a",
			"provider": "aws",
		},
	}
	b, _ := json.Marshal(obs)
	return b
}

// benchACMKinds/benchACMNamespaces are a small, realistic-ish spread
// of Kubernetes object kinds/namespaces -- enough value diversity that
// the GIN indexes below (see benchACMDataJSON) build real, non-trivial
// posting lists rather than one giant list for a single repeated
// value, which would understate real index-maintenance cost.
var (
	benchACMKinds      = []string{"Pod", "Deployment", "ConfigMap", "Service", "Secret", "ReplicaSet"}
	benchACMNamespaces = []string{"default", "kube-system", "openshift-monitoring", "app-team-a", "app-team-b"}
)

// benchACMDataJSON builds the `data` payload for the ACM baseline's
// bench_acm_resources.data column, shaped like a real
// search-indexer sync entry rather than reusing benchObservationJSON's
// extension-resource-inventory shape: it carries the same
// kind/namespace/name/apigroup/kind_plural keys the real
// search.resources GIN indexes are built over (see
// docs/design/reference/acm_search_indexing.md's schema section), so
// seeding/updating this table pays the same index-maintenance cost a
// real search-indexer write would. _hubClusterResource is set true
// for idx < 500 only -- exactly one row per cluster (there are 500
// distinct clusters, see benchClusterOf) -- mirroring ACM's
// one-pseudo-node-per-cluster convention that the partial
// data_hubCluster_idx index is sized around.
func benchACMDataJSON(gen int64, idx int) []byte {
	kind := benchACMKinds[idx%len(benchACMKinds)]
	data := map[string]any{
		"kind":            kind,
		"namespace":       benchACMNamespaces[idx%len(benchACMNamespaces)],
		"name":            fmt.Sprintf("%s-%d", strings.ToLower(kind), idx),
		"apigroup":        "apps",
		"kind_plural":     strings.ToLower(kind) + "s",
		"phase":           "Running",
		"gen":             gen,
		"resourceVersion": fmt.Sprintf("%d", gen),
	}
	if idx < 500 {
		data["_hubClusterResource"] = true
	}
	b, _ := json.Marshal(data)
	return b
}

func benchLabels(gen int64, idx int) map[string]string {
	return map[string]string{
		"label-0": fmt.Sprintf("v-%d", (int64(idx)+gen)%97),
		"label-1": fmt.Sprintf("v-%d", (int64(idx)+gen)%53),
		"label-2": fmt.Sprintf("v-%d", (int64(idx)+gen)%31),
		"label-3": fmt.Sprintf("v-%d", (int64(idx)+gen)%17),
	}
}

// benchConditionFlapPeriod controls how often benchConditions reports
// a genuine status transition for the "Ready" condition instead of
// repeating its previous steady-state value: on average 1 in
// benchConditionFlapPeriod calls for a given resource flips status.
//
// Before this existed, every UpdateExisting/UpdateWithAlias call
// reported the exact same (type, status, reason, message) tuple every
// time, so hist_conditions's change-detection WHERE clause (see
// replaceInventoryCoreCTEs/applyInventoryDeltasCoreCTEs in
// extension_resource_repo.go -- it compares status/reason/message,
// deliberately ignoring the caller-supplied last_transition_time)
// always evaluated false. Its INSERT branch -- and the write to
// extension_resource_inventory_condition_events, plus that table's
// index maintenance -- was therefore never exercised at all, which
// understated the write path's realistic cost: a real fleet always
// has some small background rate of resources transitioning between
// healthy/unhealthy even though most heartbeats really are
// condition-wise no-ops. 1-in-20 is not a measured real-world rate,
// just a "clearly non-zero but still a small minority" stand-in.
const benchConditionFlapPeriod = 20

// benchConditionFlaps decides, deterministically but without the
// periodicity artifacts a plain modulo on idx or gen alone would have
// (each benchState pool's drawFromPool revisits the same idx roughly
// every poolLen/batchSize calls, a gap that shares small common
// factors with plausible flap periods), whether resource idx's Ready
// condition flips on this particular call. Multiplying idx and gen by
// unrelated large odd constants before combining spreads the result
// across residues well enough for benchmark purposes.
func benchConditionFlaps(idx int, gen int64) bool {
	h := uint64(idx)*2654435761 ^ uint64(gen)*0x9E3779B97F4A7C15
	return h%benchConditionFlapPeriod == 0
}

// benchConditions returns the pair of conditions a report for
// resource idx at generation gen carries. Healthy always reports
// steady True; Ready flips to False on roughly 1 in
// benchConditionFlapPeriod calls (see benchConditionFlaps), so
// hist_conditions's INSERT branch actually fires for a small,
// deterministic fraction of any given batch instead of never firing.
func benchConditions(now time.Time, idx int, gen int64) []domain.Condition {
	readyStatus, reason, message := domain.ConditionTrue, "AllGood", "steady"
	if benchConditionFlaps(idx, gen) {
		readyStatus, reason, message = domain.ConditionFalse, "ProbeFailed", "liveness probe failing"
	}
	ready, _ := domain.NewCondition("Ready", readyStatus, reason, message, now)
	healthy, _ := domain.NewCondition("Healthy", domain.ConditionTrue, "AllGood", "steady", now)
	return []domain.Condition{ready, healthy}
}

// ---------------------------------------------------------------------------
// Shared benchmark state -- seeded exactly once for the whole test
// binary run, regardless of how many Benchmark functions or
// calibration rounds touch it. A bare sync.Once around a *testing.B's
// t.Cleanup-registering OpenTestDB would drop/close the corpus after
// the first sub-benchmark that happened to trigger setup returned;
// openBenchDB below intentionally skips that registration and instead
// piggybacks on the shared container's process-lifetime teardown in
// main_test.go's TestMain.
// ---------------------------------------------------------------------------

type benchState struct {
	db *sql.DB

	// neverAliasPool, steadyAliasPool, mixedAliasPool, retractPool,
	// selfReplacePool, crossContribPool, and valueConflictPool are
	// disjoint slices of a single fixed permutation of
	// [0, benchCorpusSize), each drawn from by exactly one alias
	// scenario -- see buildBenchState's doc comment for why the split
	// exists and how the seven pools are carved up, and each pool's
	// own nextXIndices doc comment below for what scenario it's
	// reserved for and whether wraparound is safe for it. Each pool
	// has its own independent, monotonically-advancing atomic cursor
	// (via drawFromPool) so concurrent sub-benchmarks never collide,
	// and rotates (wraps back to its own start) once its own cursor
	// exceeds its own length -- never another pool's.
	//
	// Drawing distinct, scattered indices this way (rather than
	// re-shuffling per call) keeps Go-side allocation cost out of the
	// very SQL timing these benchmarks are trying to isolate.
	neverAliasPool    []int
	neverAliasPos     atomic.Int64
	steadyAliasPool   []int
	steadyAliasPos    atomic.Int64
	mixedAliasPool    []int
	mixedAliasPos     atomic.Int64
	retractPool       []int
	retractPos        atomic.Int64
	selfReplacePool   []int
	selfReplacePos    atomic.Int64
	crossContribPool  []int
	crossContribPos   atomic.Int64
	valueConflictPool []int
	valueConflictPos  atomic.Int64

	// newSeq hands out resource indices beyond benchCorpusSize that have
	// never been used before, for "always a genuine INSERT" scenarios.
	newSeq atomic.Int64

	// genSeq gives every "update" call a fresh generation number so its
	// observation/labels are guaranteed to differ from whatever was
	// written last -- otherwise IS DISTINCT FROM would short-circuit the
	// write after the first iteration and we'd end up benchmarking the
	// no-op path by accident.
	genSeq atomic.Int64
}

// drawFromPool returns n indices from pool, advancing *pos
// atomically so it's safe under `go test -bench`'s (potentially
// parallel, always at-least-reentrant-across-calibration-rounds)
// benchmark execution. It wraps back to the start of pool once pos
// exceeds len(pool); see the caller-specific nextXIndices doc comments
// for whether that wraparound is safe for a given pool.
func drawFromPool(pool []int, pos *atomic.Int64, n int) []int {
	p := pos.Add(int64(n)) - int64(n)
	start := int(p % int64(len(pool)))
	out := make([]int, n)
	for i := range n {
		out[i] = pool[(start+i)%len(pool)]
	}
	return out
}

// drawFromPoolNoWrap is drawFromPool's non-wrapping counterpart, for
// pools whose scenario is only meaningful the *first* time a given
// idx is drawn -- see retractPool's and mixedAliasPool's own doc
// comments for why revisiting an idx there would silently measure a
// cheaper, different scenario than the one the benchmark claims to
// measure (retracting an already-retracted alias is a no-op; a
// second visit to mixedAliasPool's new-claim/retract buckets is too).
// Rather than let that happen silently, this panics -- which `go
// test` catches and reports as a failed benchmark, correctly
// attributed to whichever call exhausted the pool.
//
// Sizing these pools by the *final* reported b.N alone is not enough
// headroom: `go test -bench`'s calibration re-invokes a sub-benchmark's
// entire body, including everything before b.ResetTimer, once per
// candidate N as it escalates toward -benchtime -- so a single
// b.Run("batch=100", ...) can draw from its pool several times more
// than its own final N*100 before settling. A pool sized generously
// enough to absorb that, for every batch size in benchBatchSizes plus
// one extra draw for the EXPLAIN capture, is what makes a normal run
// not trip this guard; if it does trip, that's this benchmark file
// telling you honestly that its data no longer means what its name
// says, rather than quietly reporting a number for the wrong thing.
func drawFromPoolNoWrap(pool []int, pos *atomic.Int64, n int, scenario string) []int {
	p := pos.Add(int64(n)) - int64(n)
	if int(p)+n > len(pool) {
		panic(fmt.Sprintf("%s: exhausted its one-shot pool (%d indices, %d already drawn, %d more requested) -- "+
			"see drawFromPoolNoWrap's doc comment: either shrink -benchtime/-count or grow this pool", scenario, len(pool), p, n))
	}
	out := make([]int, n)
	for i := range n {
		out[i] = pool[int(p)+i]
	}
	return out
}

// nextNeverAliasIndices draws from neverAliasPool -- resources
// seedAliasCorpus deliberately never gives an alias to (see its skip
// parameter) -- for benchmarks measuring the genuine "no alias, never
// had one, while resource_alias_claims/resource_alias_contributions
// are still large from everyone else's aliases" baseline:
// buildUpdateReplacements's withAlias=false,
// buildAppUpdateReplacements's withAlias=false, buildHeartbeatDeltas,
// and the ACM baseline builders (which don't care about aliases at
// all, but drawing from here rather than a pool something else
// depends on is free). Reporting zero aliases for an idx that never
// had one is a true no-op under aliasRetractAbsentCTE's "absence =
// removal" contract (there's nothing to retract), so this pool is
// always safe to wrap around indefinitely -- unlike retractPool/
// selfReplacePool/mixedAliasPool below.
//
// This pool used to be shared with what's now retractPool -- see that
// pool's doc comment for why conflating "never had an alias" with
// "explicit retraction" was a real bug, not just an imprecise label.
func (st *benchState) nextNeverAliasIndices(n int) []int {
	return drawFromPool(st.neverAliasPool, &st.neverAliasPos, n)
}

// nextSteadyAliasIndices draws from steadyAliasPool, reserved for
// benchmarks that report the exact same single alias every time they
// visit a given idx (buildUpdateReplacements's withAlias=true and
// buildAppUpdateByAliasOnly): always re-asserting the identical
// (namespace, key, value) tuple is idempotent under the per-contributor
// replace contract (see aliasFoldCTEs's doc comment), so this pool is
// safe to wrap around indefinitely -- unlike the pools below, no
// benchmark drawing from here ever changes what seedAliasCorpus wrote,
// at any pool size.
func (st *benchState) nextSteadyAliasIndices(n int) []int {
	return drawFromPool(st.steadyAliasPool, &st.steadyAliasPos, n)
}

// nextMixedAliasIndices draws from mixedAliasPool, reserved
// exclusively for buildMixedAliasReplacements. Isolating it from the
// other pools prevents it from corrupting (or being corrupted by) any
// of them. Unlike selfReplacePool (see nextSelfReplaceIndices), this
// pool cannot be made safe to revisit just by varying the reported
// value: its new-claim bucket's "nothing has ever claimed this
// value" premise and its retraction bucket's "there's still something
// to retract" premise are both true only the first time a given idx
// is drawn -- see buildMixedAliasReplacements's doc comment. So this
// draws via drawFromPoolNoWrap, not drawFromPool: a benchmark run
// that would have silently wrapped now fails loudly instead, telling
// you to grow mixedAliasShare (in buildBenchState) rather than
// quietly reporting a number for a workload that isn't the blend this
// benchmark's name claims.
func (st *benchState) nextMixedAliasIndices(n int) []int {
	return drawFromPoolNoWrap(st.mixedAliasPool, &st.mixedAliasPos, n, "mixed-alias workload")
}

// nextRetractIndices draws from retractPool, reserved exclusively for
// BenchmarkCTE_ReplaceInventory_RetractAlias. Unlike selfReplacePool,
// there's no reported *value* to vary that would make a repeat
// retraction of the same idx meaningful on its own (retracting an
// already-retracted alias is just a no-op, indistinguishable from
// neverAliasPool's scenario) -- so instead, safety comes from the
// benchmark itself: every timed retraction call is immediately
// preceded, with the timer stopped, by a fresh
// buildRetractSeedReplacementsFor call that re-establishes exactly
// the alias seedAliasCorpus originally seeded (see
// BenchmarkCTE_ReplaceInventory_RetractAlias). That makes retractPool
// safe to wrap around indefinitely, same as steadyAliasPool, since
// there's always something genuine to retract regardless of how many
// times a given idx has been visited before.
func (st *benchState) nextRetractIndices(n int) []int {
	return drawFromPool(st.retractPool, &st.retractPos, n)
}

// nextSelfReplaceIndices draws from selfReplacePool, reserved
// exclusively for buildSelfReplaceAliasReplacements: every idx here
// starts out with a genuine seeded alias under benchAliasKey, and the
// benchmark reports a *different* value under the same key from the
// same contributor -- alias_safe's del_aliases_replaced DELETE-then-
// alias_upsert INSERT branch (a legitimate replace, not a conflict --
// see aliasFoldCTEs's doc comment on sibling_holds). Unlike
// retractPool, this pool *is* safe to wrap around indefinitely:
// buildSelfReplaceAliasReplacements folds the calling generation
// number into the reported value (see benchSelfReplaceAliasValue), so
// a revisit always reports a value that's never been reported for
// that idx before -- a genuine replace of whatever the previous visit
// (this pool's own or, before wraparound, seedAliasCorpus's original
// seed) left behind, never an accidental no-op repeat.
func (st *benchState) nextSelfReplaceIndices(n int) []int {
	return drawFromPool(st.selfReplacePool, &st.selfReplacePos, n)
}

// nextCrossContribIndices draws from crossContribPool, reserved
// exclusively for buildCrossContributorConflictReplacements. Every idx
// here has contributor A's (benchResourceType's) ordinary seeded
// alias on file, *and* its own pre-existing contributor B
// (benchResourceTypeB) extension_resources/inventory row (see
// seedCrossContributorCorpus) -- so reporting AS contributor B with a
// colliding value is a genuine steady-state AliasConflictResourceHasDifferentValue
// (sibling_holds=true) from the very first call. Contributor B's
// claim is always rejected (never written), so unlike retractPool/
// selfReplacePool this pool's state never changes -- it's safe to
// wrap around indefinitely.
func (st *benchState) nextCrossContribIndices(n int) []int {
	return drawFromPool(st.crossContribPool, &st.crossContribPos, n)
}

// nextValueConflictIndices draws from valueConflictPool, reserved
// exclusively for buildValueClaimedByOtherReplacements. Every row
// claims a distinct seedConflictVictims victim's value under
// benchSecondaryAliasKey, which that victim owns permanently -- the
// claim is always rejected (AliasConflictValueClaimedByOther), so
// this pool's state never changes either and is safe to wrap around
// indefinitely.
func (st *benchState) nextValueConflictIndices(n int) []int {
	return drawFromPool(st.valueConflictPool, &st.valueConflictPos, n)
}

func (st *benchState) nextNewIndices(n int) []int {
	base := st.newSeq.Add(int64(n)) - int64(n)
	out := make([]int, n)
	for i := range n {
		out[i] = benchCorpusSize + int(base) + i
	}
	return out
}

func (st *benchState) nextGen() int64 { return st.genSeq.Add(1) }

var (
	benchSetupOnce sync.Once
	benchShared    *benchState
	benchSetupErr  error
)

func setupBenchOnce(b *testing.B) *benchState {
	b.Helper()
	benchSetupOnce.Do(func() {
		benchShared, benchSetupErr = buildBenchState()
	})
	if benchSetupErr != nil {
		b.Fatalf("bench setup: %v", benchSetupErr)
	}
	return benchShared
}

// openBenchDB starts (once) and reuses its own dedicated testcontainers
// Postgres instance -- startBenchContainer, not startContainer/
// containerOnce, so the benchmark's 1GB-shared_buffers container stays
// independent of the lean one contract tests share via OpenTestDB --
// but otherwise follows the same testDBMu/replaceDBName pattern as
// OpenTestDB, from testdb.go. Unlike OpenTestDB, it does not register
// a t.Cleanup to drop the database, since this database must outlive
// whichever single *testing.B happens to trigger benchSetupOnce first.
// It's cleaned up along with the rest of the container by
// TerminateTestContainer in this package's TestMain when the whole
// test binary exits.
func openBenchDB() (*sql.DB, error) {
	benchContainerOnce.Do(func() {
		benchContainerCtr, benchContainerConn, benchContainerErr = startBenchContainer()
	})
	if benchContainerErr != nil {
		return nil, fmt.Errorf("postgres container: %w", benchContainerErr)
	}

	adminDB, err := sql.Open("pgx", benchContainerConn)
	if err != nil {
		return nil, fmt.Errorf("open admin connection: %w", err)
	}
	defer adminDB.Close()

	testDBMu.Lock()
	testDBCounter++
	dbName := fmt.Sprintf("bench_%d", testDBCounter)
	testDBMu.Unlock()

	if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
		return nil, fmt.Errorf("create bench database: %w", err)
	}
	return Open(replaceDBName(benchContainerConn, dbName))
}

func buildBenchState() (*benchState, error) {
	db, err := openBenchDB()
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	repo := &ExtensionResourceRepo{DB: db}

	now := time.Now().UTC()
	def := domain.NewExtensionResourceType(benchResourceType, "v1", "widgets", now, domain.WithInventory())
	if err := repo.CreateType(ctx, def); err != nil {
		return nil, fmt.Errorf("create bench resource type: %w", err)
	}
	defB := domain.NewExtensionResourceType(benchResourceTypeB, "v1", "widgets", now, domain.WithInventory())
	if err := repo.CreateType(ctx, defB); err != nil {
		return nil, fmt.Errorf("create bench resource type B: %w", err)
	}

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE bench_acm_resources (
			uid     TEXT PRIMARY KEY,
			cluster TEXT NOT NULL,
			data    JSONB NOT NULL
		)`); err != nil {
		return nil, fmt.Errorf("create bench_acm_resources: %w", err)
	}
	// Same 7-index set as the real search.resources schema (PK btree +
	// 1 plain btree + 5 GIN over `data` expressions), per
	// docs/design/reference/acm_search_indexing.md section 3.6 --
	// without these, the ACM baseline would understate real
	// search-indexer write cost: every write touches `data`, so with
	// these indexes present Postgres can never use a HOT update here
	// (any indexed column changing forces a new index entry in every
	// index on the table), and GIN maintenance is materially more
	// CPU-costly per entry than btree.
	for _, stmt := range []string{
		`CREATE INDEX bench_acm_data_kind_idx ON bench_acm_resources USING GIN ((data -> 'kind'))`,
		`CREATE INDEX bench_acm_data_namespace_idx ON bench_acm_resources USING GIN ((data -> 'namespace'))`,
		`CREATE INDEX bench_acm_data_name_idx ON bench_acm_resources USING GIN ((data -> 'name'))`,
		`CREATE INDEX bench_acm_data_cluster_idx ON bench_acm_resources USING btree (cluster)`,
		`CREATE INDEX bench_acm_data_composite_idx ON bench_acm_resources USING GIN (
			(data -> '_hubClusterResource'), (data -> 'namespace'), (data -> 'apigroup'), (data -> 'kind_plural')
		)`,
		`CREATE INDEX bench_acm_data_hubcluster_idx ON bench_acm_resources USING GIN ((data -> '_hubClusterResource'))
			WHERE data ? '_hubClusterResource'`,
	} {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return nil, fmt.Errorf("create bench_acm_resources index: %w", err)
		}
	}

	// A single permutation of every seeded resource idx, carved into
	// seven disjoint ranges -- one per benchState pool field -- up
	// front, *before* any alias seeding runs, so seedAliasCorpus knows
	// which indices to deliberately skip (neverAliasPool) and
	// seedCrossContributorCorpus knows which to give a second
	// contributor to (crossContribPool, mixedAliasPool). Each
	// benchmark scenario gets its own dedicated range so it can never
	// observe (or corrupt) another scenario's alias state -- see each
	// nextXIndices doc comment on benchState for exactly which
	// scenario a given pool belongs to, why it needs to be separate
	// from the others, and whether wrapping around it is safe. Before
	// this split existed, a single shared pool meant a benchmark that
	// reports zero aliases (meant to measure "no alias" traffic) would
	// instead retract whatever alias a *different* benchmark's
	// steady-state case depended on -- see this file's git
	// history/PR discussion for the "no alias" vs. "explicit
	// retraction" conflation that motivated today's finer-grained
	// split.
	//
	// mixedAliasShare is sized well beyond its final reported
	// b.N * batch size specifically to survive `go test -bench`'s
	// calibration overhead (see drawFromPoolNoWrap's doc comment)
	// across all of benchBatchSizes plus one EXPLAIN draw, since
	// nextMixedAliasIndices fails fast rather than wrap if that
	// headroom turns out to be wrong for a given host/benchtime/count
	// -- its retraction bucket (see buildMixedAliasReplacements) has
	// no re-seeding step to make it safe to revisit, unlike the
	// dedicated retractShare pool below. retractShare and
	// selfReplaceShare don't need that same headroom -- both wrap
	// safely (see nextRetractIndices's and nextSelfReplaceIndices's
	// doc comments) -- so they stay modest, like the other
	// safe-to-wrap pools.
	perm := rand.New(rand.NewSource(42)).Perm(benchCorpusSize)
	const (
		neverAliasShare   = 10_000
		steadyAliasShare  = 10_000
		mixedAliasShare   = 40_000
		retractShare      = 15_000
		selfReplaceShare  = 10_000
		crossContribShare = 5_000
		// valueConflictShare is whatever's left: 100_000 - sum(above) = 10_000.
	)
	o := 0
	neverAliasPool := perm[o : o+neverAliasShare]
	o += neverAliasShare
	steadyAliasPool := perm[o : o+steadyAliasShare]
	o += steadyAliasShare
	mixedAliasPool := perm[o : o+mixedAliasShare]
	o += mixedAliasShare
	retractPool := perm[o : o+retractShare]
	o += retractShare
	selfReplacePool := perm[o : o+selfReplaceShare]
	o += selfReplaceShare
	crossContribPool := perm[o : o+crossContribShare]
	o += crossContribShare
	valueConflictPool := perm[o:]

	hasAlias := make([]bool, benchCorpusSize)
	for i := range hasAlias {
		hasAlias[i] = true
	}
	for _, idx := range neverAliasPool {
		hasAlias[idx] = false
	}

	fmt.Println("\n[bench] seeding corpus (this happens once for the whole run)...")
	seedStart := time.Now()

	corpusUIDs, err := seedExtensionResourceCorpus(ctx, db, benchCorpusSize, hasAlias)
	if err != nil {
		return nil, err
	}
	if err := seedAliasCorpus(ctx, db, benchCorpusSize, corpusUIDs, hasAlias); err != nil {
		return nil, err
	}
	if err := seedCrossContributorCorpus(ctx, db, crossContribPool); err != nil {
		return nil, err
	}
	// mixedAliasPool's own cross-contributor bucket (see
	// buildMixedAliasReplacements) needs the exact same steady-state
	// pre-seeding crossContribPool gets, for the same reason: without
	// it, every draw's 5% slice would pay a one-time contributor-B
	// insert cost indistinguishable from (and inflating) the mixed
	// workload's measured latency, rather than the steady-state
	// conflict cost BenchmarkCTE_ReplaceInventory_CrossContributorConflict
	// isolates on its own.
	if err := seedCrossContributorCorpus(ctx, db, mixedAliasPool); err != nil {
		return nil, err
	}
	if err := seedConflictVictims(ctx, db, benchConflictVictimCount); err != nil {
		return nil, err
	}
	if err := seedACMCorpus(ctx, db, benchCorpusSize); err != nil {
		return nil, err
	}
	if err := analyzeBenchTables(ctx, db); err != nil {
		return nil, err
	}
	fmt.Printf("[bench] seeded %d resources (+ labels/conditions/aliases + ACM baseline table) in %s\n\n",
		benchCorpusSize, time.Since(seedStart))

	return &benchState{
		db:                db,
		neverAliasPool:    neverAliasPool,
		steadyAliasPool:   steadyAliasPool,
		mixedAliasPool:    mixedAliasPool,
		retractPool:       retractPool,
		selfReplacePool:   selfReplacePool,
		crossContribPool:  crossContribPool,
		valueConflictPool: valueConflictPool,
	}, nil
}

// ---------------------------------------------------------------------------
// Seeding: bulk multi-row inserts (UNNEST-backed, chunked at
// benchSeedChunkRows resources per statement) rather than routing
// through ReplaceInventory -- this is corpus setup, not part of what
// we're measuring, so it should be as fast as possible and not itself
// exercise the code under test.
// ---------------------------------------------------------------------------

// seedExtensionResourceCorpus returns every seeded resource's
// persistent extension_resources.uid, indexed by idx (0..n-1), so
// seedAliasCorpus can attribute its seeded aliases to the same
// contributor a later steady-state report for that idx resolves to
// (see seedAliasCorpus's doc comment for why that attribution
// matters under the per-contributor alias model). It also seeds
// extension_resources.alias_fingerprint to match the alias state
// seedAliasCorpus is about to write: aliased resources get the hash
// of their one seeded alias, and never-alias resources get the hash
// of the empty set. Because benchmark setup intentionally bypasses
// ReplaceInventory, this is what makes the steady-state no-op alias
// benchmarks exercise the fingerprint skip instead of paying one
// artificial cold-cache classification pass.
func seedExtensionResourceCorpus(ctx context.Context, db *sql.DB, n int, hasAlias []bool) ([]string, error) {
	allUIDs := make([]string, n)
	for start := 0; start < n; start += benchSeedChunkRows {
		end := min(start+benchSeedChunkRows, n)
		size := end - start

		uids := make([]string, size)
		serviceNames := make([]string, size)
		typeNames := make([]string, size)
		collectionNames := make([]string, size)
		resourceIDs := make([]string, size)
		labelsJSON := make([]string, size)
		aliasFingerprints := make([][]byte, size)
		createdAts := make([]time.Time, size)
		observations := make([]*string, size)

		for i := range size {
			idx := start + i
			uids[i] = domain.NewExtensionResourceUID().String()
			allUIDs[idx] = uids[i]
			serviceNames[i] = benchServiceName
			typeNames[i] = benchTypeName
			collectionNames[i] = benchCollectionName
			resourceIDs[i] = benchResourceID(idx)
			labelsJSON[i] = "{}"
			if hasAlias[idx] {
				aliasFingerprints[i] = domain.AliasSetFingerprint([]domain.Alias{{
					Namespace: benchAliasNamespace,
					Key:       benchAliasKey,
					Value:     domain.AliasValue(benchAliasValue(idx)),
				}})
			} else {
				aliasFingerprints[i] = domain.AliasSetFingerprint(nil)
			}
			createdAts[i] = time.Now().UTC()
			obs := string(benchObservationJSON(0))
			observations[i] = &obs
		}

		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, alias_fingerprint, created_at, updated_at)
			SELECT u, s, t, c, r, l::jsonb, fp, ts, ts
			FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::bytea[], $8::timestamptz[])
				AS x(u, s, t, c, r, l, fp, ts)
		`, uids, serviceNames, typeNames, collectionNames, resourceIDs, labelsJSON, aliasFingerprints, createdAts); err != nil {
			return nil, fmt.Errorf("seed extension_resources[%d:%d]: %w", start, end, err)
		}

		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resource_inventory (extension_resource_uid, observation, observed_at, updated_at)
			SELECT u, o::jsonb, ts, ts
			FROM UNNEST($1::uuid[], $2::text[], $3::timestamptz[]) AS x(u, o, ts)
		`, uids, observations, createdAts); err != nil {
			return nil, fmt.Errorf("seed extension_resource_inventory[%d:%d]: %w", start, end, err)
		}

		labelUIDs := make([]string, 0, size*benchLabelsPerResource)
		labelKeys := make([]string, 0, size*benchLabelsPerResource)
		labelValues := make([]string, 0, size*benchLabelsPerResource)
		for i := range size {
			idx := start + i
			for k := range benchLabelsPerResource {
				labelUIDs = append(labelUIDs, uids[i])
				labelKeys = append(labelKeys, fmt.Sprintf("label-%d", k))
				labelValues = append(labelValues, fmt.Sprintf("v-%d", (idx+k)%97))
			}
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resource_inventory_labels (extension_resource_uid, key, value)
			SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::text[])
		`, labelUIDs, labelKeys, labelValues); err != nil {
			return nil, fmt.Errorf("seed extension_resource_inventory_labels[%d:%d]: %w", start, end, err)
		}

		condTypeNames := [benchConditionsPerResource]string{"Ready", "Healthy"}
		condUIDs := make([]string, 0, size*benchConditionsPerResource)
		condTypes := make([]string, 0, size*benchConditionsPerResource)
		condStatuses := make([]string, 0, size*benchConditionsPerResource)
		condLastTransitions := make([]time.Time, 0, size*benchConditionsPerResource)
		for i := range size {
			for k := range benchConditionsPerResource {
				condUIDs = append(condUIDs, uids[i])
				condTypes = append(condTypes, condTypeNames[k])
				condStatuses = append(condStatuses, "True")
				condLastTransitions = append(condLastTransitions, createdAts[i])
			}
		}
		// reason/message must match benchConditions's steady-state
		// values ("AllGood"/"steady"), not empty strings: hist_conditions
		// treats any reason/message difference as a genuine transition
		// (see benchConditions's doc comment), so a mismatched seed here
		// would make every resource's first post-seed report look like a
		// transition regardless of benchConditionFlaps, swamping the
		// intentional ~1-in-benchConditionFlapPeriod flap rate with a
		// one-time "never touched since seeding" artifact.
		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resource_inventory_conditions
				(extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, updated_at)
			SELECT u, t, s, 'AllGood', 'steady', lt, lt, lt
			FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::timestamptz[]) AS x(u, t, s, lt)
		`, condUIDs, condTypes, condStatuses, condLastTransitions); err != nil {
			return nil, fmt.Errorf("seed extension_resource_inventory_conditions[%d:%d]: %w", start, end, err)
		}
	}
	return allUIDs, nil
}

// seedAliasCorpus gives every resource in [0, n) *except* those with
// hasAlias[idx] == false exactly one already-resolved alias, so the
// "UpdateWithAlias" benchmark measures the realistic steady-state
// case: an addon that always reports the same alias on every poll,
// hitting ReplaceInventory's alias_fingerprint skip rather than
// reclassifying a no-op alias or writing a first-time claim. uids (from
// seedExtensionResourceCorpus) attributes each seeded contribution to
// the very same extension_resources.uid a later steady-state report
// for that idx resolves to via natural-key resolution -- required
// under the per-contributor alias model (see the migration's
// resource_alias_claims doc comment): without it, a "repeat" report's
// real contributor uid would never match this seed's contributor, so
// what's meant to exercise a no-op would instead exercise a genuine
// (if one-time per idx) new claim/contribution.
//
// hasAlias[idx] == false is exactly benchState.neverAliasPool's
// membership: those resources get neither a resource_alias_claims/
// resource_alias_contributions row nor a platform_resources row:
// claims no longer require a physical platform_resources row, and a
// resource that never contributes an alias has no other reason here
// to get one either. A benchmark drawing from neverAliasPool
// therefore measures a resource type that has genuinely never
// touched either table, not one whose alias was seeded and is merely
// never read back.
//
// Each qualifying idx needs its own brand-new claim row (this
// function never corroborates a pre-existing one), so the claim
// insert always returns exactly one row per input row; claimIDByValue
// correlates the two INSERTs by value rather than assuming the
// RETURNING order matches the UNNEST input order, which Postgres
// doesn't guarantee for a set-returning INSERT ... SELECT.
func seedAliasCorpus(ctx context.Context, db *sql.DB, n int, uids []string, hasAlias []bool) error {
	for start := 0; start < n; start += benchSeedChunkRows {
		end := min(start+benchSeedChunkRows, n)

		var qualifying []int
		for idx := start; idx < end; idx++ {
			if hasAlias[idx] {
				qualifying = append(qualifying, idx)
			}
		}
		if len(qualifying) == 0 {
			continue
		}
		size := len(qualifying)

		collectionNames := make([]string, size)
		resourceIDs := make([]string, size)
		createdAts := make([]time.Time, size)
		now := time.Now().UTC()
		for i, idx := range qualifying {
			collectionNames[i] = benchCollectionName
			resourceIDs[i] = benchResourceID(idx)
			createdAts[i] = now
		}

		namespaces := make([]string, size)
		keys := make([]string, size)
		values := make([]string, size)
		sourceUIDs := make([]string, size)
		for i, idx := range qualifying {
			namespaces[i] = string(benchAliasNamespace)
			keys[i] = string(benchAliasKey)
			values[i] = benchAliasValue(idx)
			sourceUIDs[i] = uids[idx]
		}

		claimRows, err := db.QueryContext(ctx, `
			INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id, created_at)
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])
			RETURNING value, id
		`, namespaces, keys, values, collectionNames, resourceIDs, createdAts)
		if err != nil {
			return fmt.Errorf("seed resource_alias_claims[%d:%d]: %w", start, end, err)
		}
		claimIDByValue := make(map[string]int64, size)
		for claimRows.Next() {
			var value string
			var id int64
			if err := claimRows.Scan(&value, &id); err != nil {
				claimRows.Close()
				return fmt.Errorf("scan seeded resource_alias_claims[%d:%d]: %w", start, end, err)
			}
			claimIDByValue[value] = id
		}
		if err := claimRows.Err(); err != nil {
			claimRows.Close()
			return fmt.Errorf("seed resource_alias_claims[%d:%d]: %w", start, end, err)
		}
		claimRows.Close()

		claimIDs := make([]int64, size)
		for i, v := range values {
			id, ok := claimIDByValue[v]
			if !ok {
				return fmt.Errorf("seed resource_alias_contributions[%d:%d]: no claim id for value %q", start, end, v)
			}
			claimIDs[i] = id
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO resource_alias_contributions (source_extension_resource_uid, namespace, key, claim_id, created_at)
			SELECT * FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::bigint[], $5::timestamptz[])
		`, sourceUIDs, namespaces, keys, claimIDs, createdAts); err != nil {
			return fmt.Errorf("seed resource_alias_contributions[%d:%d]: %w", start, end, err)
		}
	}
	return nil
}

// seedCrossContributorCorpus registers a second contributor
// (benchResourceTypeB) on every platform resource in indices, giving
// each its own pre-existing extension_resources + extension_resource_inventory
// row -- so buildCrossContributorConflictReplacements measures a
// genuine steady-state second-contributor update (whose alias claim
// is rejected as a cross-contributor conflict) from its very first
// call, rather than conflating "cold insert of B's own row" with
// "cross-contributor alias conflict" cost on a pool's first pass.
// indices must be a subset of [0, benchCorpusSize) that already has
// contributor A's ordinary seeded alias (see seedAliasCorpus) -- the
// "sibling already holds a different value" state the conflict
// benchmark depends on -- which buildBenchState guarantees by carving
// crossContribPool disjoint from neverAliasPool alone, not from the
// contributor-A-seeded majority of the corpus.
func seedCrossContributorCorpus(ctx context.Context, db *sql.DB, indices []int) error {
	for start := 0; start < len(indices); start += benchSeedChunkRows {
		end := min(start+benchSeedChunkRows, len(indices))
		chunk := indices[start:end]
		size := len(chunk)

		uids := make([]string, size)
		serviceNames := make([]string, size)
		typeNames := make([]string, size)
		collectionNames := make([]string, size)
		resourceIDs := make([]string, size)
		aliasFingerprints := make([][]byte, size)
		createdAts := make([]time.Time, size)
		observations := make([]*string, size)
		now := time.Now().UTC()
		for i, idx := range chunk {
			uids[i] = domain.NewExtensionResourceUID().String()
			serviceNames[i] = benchServiceNameB
			typeNames[i] = benchTypeName
			collectionNames[i] = benchCollectionName
			resourceIDs[i] = benchResourceID(idx)
			aliasFingerprints[i] = domain.AliasSetFingerprint(nil)
			createdAts[i] = now
			obs := string(benchObservationJSON(0))
			observations[i] = &obs
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, alias_fingerprint, created_at, updated_at)
			SELECT u, s, t, c, r, '{}'::jsonb, fp, ts, ts
			FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[], $6::bytea[], $7::timestamptz[])
				AS x(u, s, t, c, r, fp, ts)
		`, uids, serviceNames, typeNames, collectionNames, resourceIDs, aliasFingerprints, createdAts); err != nil {
			return fmt.Errorf("seed cross-contributor extension_resources[%d:%d]: %w", start, end, err)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resource_inventory (extension_resource_uid, observation, observed_at, updated_at)
			SELECT u, o::jsonb, ts, ts
			FROM UNNEST($1::uuid[], $2::text[], $3::timestamptz[]) AS x(u, o, ts)
		`, uids, observations, createdAts); err != nil {
			return fmt.Errorf("seed cross-contributor extension_resource_inventory[%d:%d]: %w", start, end, err)
		}
	}
	return nil
}

// seedConflictVictims seeds n dedicated platform resources, each
// already owning one benchSecondaryAliasKey alias, purely so
// buildValueClaimedByOtherReplacements (and buildMixedAliasReplacements's
// matching bucket) has pre-existing "other owners" to generate genuine
// AliasConflictValueClaimedByOther rows against. These are
// platform_resources/resource_alias_claims rows only -- no
// extension_resources/inventory rows and no resource_alias_contributions
// rows at all -- since alias resolution depends on the platform
// natural key alone (see aliasFoldCTEs's doc comment), not on an
// extension resource existing; each claim is written with
// platform_owned = true and zero contributions accordingly (the same
// "platform-direct" shape resource_identity_repo.go's reconcileAliases
// writes -- see the migration's resource_alias_claims doc comment),
// which is fine here since none of the conflict shapes this seeds
// depend on contributor identity at all.
//
// This deliberately uses benchSecondaryAliasKey, not benchAliasKey:
// every corpus resource already owns a benchAliasKey alias (see
// seedAliasCorpus), so a victim seeded under benchAliasKey would make
// the "value claimed by other" bucket ambiguous with "resource has a
// different value for this key" -- whichever check the CTE runs first
// (by-resource in the small/one-phase version, by-value in the
// two-phase version) would "win" and mask the other outcome entirely,
// which is itself a genuine, interesting divergence between the two
// designs (see buildValueClaimedByOtherReplacements's doc comment) but
// not one a clean per-outcome performance comparison can share a
// bucket with. Keying victims off the otherwise-untouched
// benchSecondaryAliasKey keeps the "value claimed by other" bucket a
// pure case: the reporting resource has no existing entry under that
// key at all.
func seedConflictVictims(ctx context.Context, db *sql.DB, n int) error {
	collectionNames := make([]string, n)
	resourceIDs := make([]string, n)
	namespaces := make([]string, n)
	keys := make([]string, n)
	values := make([]string, n)
	createdAts := make([]time.Time, n)
	now := time.Now().UTC()
	for i := range n {
		collectionNames[i] = benchCollectionName
		resourceIDs[i] = benchConflictVictimResourceID(i)
		namespaces[i] = string(benchAliasNamespace)
		keys[i] = string(benchSecondaryAliasKey)
		values[i] = benchConflictVictimAliasValue(i)
		createdAts[i] = now
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO platform_resources (collection_name, resource_id, labels, created_at, updated_at)
		SELECT c, r, '{}'::jsonb, t, t
		FROM UNNEST($1::text[], $2::text[], $3::timestamptz[]) AS x(c, r, t)
	`, collectionNames, resourceIDs, createdAts); err != nil {
		return fmt.Errorf("seed conflict victim platform_resources: %w", err)
	}
	if _, err := db.ExecContext(ctx, `
		INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id, platform_owned, created_at)
		SELECT n, k, v, c, r, true, t
		FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[]) AS x(n, k, v, c, r, t)
	`, namespaces, keys, values, collectionNames, resourceIDs, createdAts); err != nil {
		return fmt.Errorf("seed conflict victim resource_alias_claims: %w", err)
	}
	return nil
}

func seedACMCorpus(ctx context.Context, db *sql.DB, n int) error {
	for start := 0; start < n; start += benchSeedChunkRows {
		end := min(start+benchSeedChunkRows, n)
		size := end - start

		uids := make([]string, size)
		clusters := make([]string, size)
		datas := make([]string, size)
		for i := range size {
			idx := start + i
			uids[i] = benchACMUID(idx)
			clusters[i] = benchClusterOf(idx)
			datas[i] = string(benchACMDataJSON(0, idx))
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO bench_acm_resources (uid, cluster, data)
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::jsonb[])
		`, uids, clusters, datas); err != nil {
			return fmt.Errorf("seed bench_acm_resources[%d:%d]: %w", start, end, err)
		}
	}
	return nil
}

func analyzeBenchTables(ctx context.Context, db *sql.DB) error {
	tables := []string{
		"extension_resources",
		"extension_resource_inventory",
		"extension_resource_inventory_labels",
		"extension_resource_inventory_conditions",
		"extension_resource_inventory_observations",
		"extension_resource_inventory_condition_events",
		"platform_resources",
		"resource_alias_claims",
		"resource_alias_contributions",
		"bench_acm_resources",
	}
	for _, t := range tables {
		if _, err := db.ExecContext(ctx, "ANALYZE "+t); err != nil {
			return fmt.Errorf("analyze %s: %w", t, err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Input builders for the CTE path
// ---------------------------------------------------------------------------

// buildUpdateReplacements's withAlias=false path is the *genuine*
// "no alias, never had one" baseline (nextNeverAliasIndices), not an
// explicit retraction -- see buildRetractAliasReplacementsFor for
// that scenario, isolated on its own dedicated pool.
func buildUpdateReplacements(st *benchState, n int, withAlias bool) []domain.InventoryReplacement {
	// withAlias picks the pool, not just the payload: see
	// nextSteadyAliasIndices/nextNeverAliasIndices's doc comments for
	// why a withAlias=false (no Aliases at all) report must never land
	// on an idx some other benchmark's alias state depends on.
	var indices []int
	if withAlias {
		indices = st.nextSteadyAliasIndices(n)
	} else {
		indices = st.nextNeverAliasIndices(n)
	}
	gen := st.nextGen()
	now := time.Now().UTC()
	reps := make([]domain.InventoryReplacement, n)
	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(gen*int64(benchCorpusSize) + int64(idx)))
		rep := domain.InventoryReplacement{
			ResourceType: benchResourceType,
			Name:         benchResourceName(idx),
			CandidateUID: domain.NewExtensionResourceUID(),
			Labels:       benchLabels(gen, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, gen),
			ObservedAt:   now,
			ReceivedAt:   now,
		}
		if withAlias {
			rep.Aliases = []domain.Alias{{Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchAliasValue(idx))}}
		}
		reps[i] = rep
	}
	return reps
}

// buildRetractSeedReplacementsFor re-establishes a benchAliasKey
// alias (the same value seedAliasCorpus originally gave every
// resource) for the given indices -- same shape as
// buildUpdateReplacements's withAlias=true case, but for a
// caller-supplied index list rather than drawing fresh ones from a
// pool. BenchmarkCTE_ReplaceInventory_RetractAlias runs this,
// timer stopped, immediately before every timed call to
// buildRetractAliasReplacementsFor against the very same indices, so
// the retraction being timed always has something genuine to
// retract -- see nextRetractIndices's doc comment.
func buildRetractSeedReplacementsFor(st *benchState, indices []int) []domain.InventoryReplacement {
	gen := st.nextGen()
	now := time.Now().UTC()
	reps := make([]domain.InventoryReplacement, len(indices))
	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(gen*int64(benchCorpusSize) + int64(idx)))
		reps[i] = domain.InventoryReplacement{
			ResourceType: benchResourceType,
			Name:         benchResourceName(idx),
			CandidateUID: domain.NewExtensionResourceUID(),
			Labels:       benchLabels(gen, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, gen),
			ObservedAt:   now,
			ReceivedAt:   now,
			Aliases:      []domain.Alias{{Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchAliasValue(idx))}},
		}
	}
	return reps
}

// buildRetractAliasReplacementsFor isolates aliasRetractAbsentCTE's
// del_aliases_absent DELETE branch: every row targets a resource that
// -- thanks to buildRetractSeedReplacementsFor having just
// (re-)established it -- genuinely holds a benchAliasKey alias, but
// reports zero Aliases, so under the per-contributor "absence =
// removal" replace contract it must be retracted for real -- unlike
// buildUpdateReplacements's withAlias=false, which draws from
// neverAliasPool and never had anything to retract in the first
// place.
func buildRetractAliasReplacementsFor(st *benchState, indices []int) []domain.InventoryReplacement {
	gen := st.nextGen()
	now := time.Now().UTC()
	reps := make([]domain.InventoryReplacement, len(indices))
	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(gen*int64(benchCorpusSize) + int64(idx)))
		reps[i] = domain.InventoryReplacement{
			ResourceType: benchResourceType,
			Name:         benchResourceName(idx),
			CandidateUID: domain.NewExtensionResourceUID(),
			Labels:       benchLabels(gen, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, gen),
			ObservedAt:   now,
			ReceivedAt:   now,
			// Aliases deliberately left nil/empty: explicit retraction.
		}
	}
	return reps
}

// benchSelfReplaceAliasValue is the value
// buildSelfReplaceAliasReplacements/buildMixedAliasReplacements's
// self-replace bucket report for idx on generation gen. gen strictly
// increases (st.nextGen()) for the life of the process, so this value
// has never been reported for idx before on any prior visit --
// including one from an earlier wrap around selfReplacePool (see
// nextSelfReplaceIndices's doc comment) -- making every call a
// genuine replace of whatever idx held before, never an accidental
// no-op repeat of the exact same value.
func benchSelfReplaceAliasValue(idx int, gen int64) string {
	return fmt.Sprintf("%s-changed-%d", benchAliasValue(idx), gen)
}

// benchMixedNewClaimAliasValue is buildMixedAliasReplacements' new-claim
// bucket's counterpart to benchSelfReplaceAliasValue: gen-suffixed for
// the same reason, so that a revisit to the same idx (whether from
// mixedAliasPool wrapping, or simply a later draw landing on the same
// idx again before that) always claims a benchSecondaryAliasKey value
// nothing has claimed before, rather than idempotently re-asserting
// whatever this bucket claimed for idx last time.
func benchMixedNewClaimAliasValue(idx int, gen int64) string {
	return fmt.Sprintf("%s-new-%d", benchAliasValue(idx), gen)
}

// buildSelfReplaceAliasReplacements isolates alias_safe's
// del_aliases_replaced DELETE-then-alias_upsert INSERT branch: every
// row's own contributor already holds benchAliasKey (selfReplacePool,
// seeded exactly like steadyAliasPool), and the report replaces it
// with a new value from that very same contributor -- sibling_holds
// is false (see aliasFoldCTEs's doc comment), so this is a legitimate
// replace, not AliasConflictResourceHasDifferentValue.
func buildSelfReplaceAliasReplacements(st *benchState, n int) []domain.InventoryReplacement {
	indices := st.nextSelfReplaceIndices(n)
	gen := st.nextGen()
	now := time.Now().UTC()
	reps := make([]domain.InventoryReplacement, n)
	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(gen*int64(benchCorpusSize) + int64(idx)))
		reps[i] = domain.InventoryReplacement{
			ResourceType: benchResourceType,
			Name:         benchResourceName(idx),
			CandidateUID: domain.NewExtensionResourceUID(),
			Labels:       benchLabels(gen, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, gen),
			ObservedAt:   now,
			ReceivedAt:   now,
			Aliases: []domain.Alias{{
				Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchSelfReplaceAliasValue(idx, gen)),
			}},
		}
	}
	return reps
}

// buildCrossContributorConflictReplacements isolates
// alias_resource_conflicts' AliasConflictResourceHasDifferentValue
// outcome (sibling_holds=true): every row reports AS a *second*
// contributor (benchResourceTypeB) targeting a platform resource
// crossContribPool dedicates to this scenario, where the *first*
// contributor (benchResourceType) already holds a different value
// under benchAliasKey (the ordinary steady-state alias every corpus
// resource gets from seedAliasCorpus) and contributor B has its own
// pre-existing extension_resources/inventory row (seedCrossContributorCorpus)
// but has never claimed this alias itself. benchCrossContribAliasValue
// is fixed per idx and never matches anything contributor A or any
// other benchmark ever claims, so -- unlike retractPool/selfReplacePool --
// this scenario never actually writes anything and is safe to
// revisit indefinitely (see nextCrossContribIndices's doc comment).
func buildCrossContributorConflictReplacements(st *benchState, n int) []domain.InventoryReplacement {
	indices := st.nextCrossContribIndices(n)
	gen := st.nextGen()
	now := time.Now().UTC()
	reps := make([]domain.InventoryReplacement, n)
	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(gen*int64(benchCorpusSize) + int64(idx)))
		reps[i] = domain.InventoryReplacement{
			ResourceType: benchResourceTypeB,
			Name:         benchResourceName(idx),
			CandidateUID: domain.NewExtensionResourceUID(),
			Labels:       benchLabels(gen, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, gen),
			ObservedAt:   now,
			ReceivedAt:   now,
			Aliases: []domain.Alias{{
				Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchCrossContribAliasValue(idx)),
			}},
		}
	}
	return reps
}

// buildValueClaimedByOtherReplacements isolates alias_value_conflicts'
// AliasConflictValueClaimedByOther outcome (phase 1, by_value): every
// row reports its own unchanged
// benchAliasKey alias (preserving it -- otherwise this report's
// silence on benchAliasKey would retract it under replace semantics,
// confounding a "new key rejected" measurement with an unrelated
// "existing key retracted" one) alongside benchSecondaryAliasKey set
// to one of seedConflictVictims's dedicated victims' values, which
// that victim owns permanently. Every row in a batch must claim a
// distinct victim (see benchConflictVictimCount's doc comment for why
// a repeat within one batch would be rejected before this even
// reaches SQL), so n must not exceed benchConflictVictimCount -- true
// for every entry in benchBatchSizes.
func buildValueClaimedByOtherReplacements(st *benchState, n int) []domain.InventoryReplacement {
	indices := st.nextValueConflictIndices(n)
	gen := st.nextGen()
	now := time.Now().UTC()
	reps := make([]domain.InventoryReplacement, n)
	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(gen*int64(benchCorpusSize) + int64(idx)))
		reps[i] = domain.InventoryReplacement{
			ResourceType: benchResourceType,
			Name:         benchResourceName(idx),
			CandidateUID: domain.NewExtensionResourceUID(),
			Labels:       benchLabels(gen, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, gen),
			ObservedAt:   now,
			ReceivedAt:   now,
			Aliases: []domain.Alias{
				{Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchAliasValue(idx))},
				{Namespace: benchAliasNamespace, Key: benchSecondaryAliasKey, Value: domain.AliasValue(benchConflictVictimAliasValue(i))},
			},
		}
	}
	return reps
}

// buildMixedAliasReplacements builds a batch of n existing-resource
// reports whose Aliases exercise all six alias outcomes this file's
// isolated benchmarks measure separately (buildUpdateReplacements/
// buildRetractAliasReplacementsFor/buildSelfReplaceAliasReplacements/
// buildCrossContributorConflictReplacements/buildValueClaimedByOtherReplacements),
// blended into one batch at illustrative-not-measured ratios modeling
// a fleet where most polls repeat an already-resolved alias and
// everything else -- new claims, replacements, retractions, and
// genuine conflicts from either a second contributor or a value
// collision -- shows up as a small trickle:
//   - 70% (remainder): steady-state no-op, same as buildUpdateReplacements'
//     withAlias=true -- the resource re-sends the exact alias it
//     already owns.
//   - 8%: a genuinely new alias under benchSecondaryAliasKey, gen-suffixed
//     (see benchMixedNewClaimAliasValue) so no idx ever repeats a value
//     it -- or, before this bucket's own wraparound, any earlier
//     visit to this same idx -- has already claimed, exercising
//     alias_safe's actual write every time rather than only the
//     first. Also re-sends the resource's own benchAliasKey alias
//     unchanged, for the same "don't accidentally retract the other
//     key" reason buildValueClaimedByOtherReplacements's doc comment
//     gives.
//   - 5%: a legitimate self-replacement, same shape as
//     buildSelfReplaceAliasReplacements.
//   - 5%: explicit retraction, same shape as
//     buildRetractAliasReplacementsFor (Aliases left empty) -- unlike
//     the dedicated BenchmarkCTE_ReplaceInventory_RetractAlias, this
//     bucket does not re-seed before retracting, so (see
//     nextMixedAliasIndices's doc comment) it remains one-shot per
//     idx.
//   - 7%: AliasConflictValueClaimedByOther, same shape as
//     buildValueClaimedByOtherReplacements (a seedConflictVictims
//     victim's value under benchSecondaryAliasKey, plus the
//     unchanged benchAliasKey alias). Each row in this bucket gets a
//     distinct victim (via its position within the bucket, not its
//     resource idx) so two rows in the same batch never claim the
//     same value -- see benchConflictVictimCount's doc comment.
//   - 5%: AliasConflictResourceHasDifferentValue, same shape as
//     buildCrossContributorConflictReplacements (reported as
//     benchResourceTypeB against benchCrossContribAliasValue).
//     mixedAliasPool gets the same seedCrossContributorCorpus
//     pre-seeding crossContribPool does (see buildBenchState), so
//     this bucket measures the same steady-state conflict cost
//     BenchmarkCTE_ReplaceInventory_CrossContributorConflict isolates
//     on its own, not a one-time cold-insert cost for contributor B's
//     own fields.
//
// nextMixedAliasIndices draws via drawFromPoolNoWrap, not
// drawFromPool: the retraction bucket above is only meaningful the
// first time a given idx is drawn (see that function's doc comment),
// so this pool must not silently wrap. The new-claim bucket alone
// would not have required this -- benchMixedNewClaimAliasValue's
// gen-suffix makes it safe to revisit indefinitely, the same trick
// benchSelfReplaceAliasValue uses for the self-replace bucket -- but
// retraction still forces the guard for the pool as a whole.
func buildMixedAliasReplacements(st *benchState, n int) []domain.InventoryReplacement {
	indices := st.nextMixedAliasIndices(n)
	gen := st.nextGen()
	now := time.Now().UTC()
	reps := make([]domain.InventoryReplacement, n)

	newCount := n * 8 / 100
	selfReplaceCount := n * 5 / 100
	retractCount := n * 5 / 100
	valueConflictCount := n * 7 / 100
	crossContribCount := n * 5 / 100

	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(gen*int64(benchCorpusSize) + int64(idx)))
		rep := domain.InventoryReplacement{
			ResourceType: benchResourceType,
			Name:         benchResourceName(idx),
			CandidateUID: domain.NewExtensionResourceUID(),
			Labels:       benchLabels(gen, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, gen),
			ObservedAt:   now,
			ReceivedAt:   now,
		}
		switch {
		case i < newCount:
			rep.Aliases = []domain.Alias{
				{Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchAliasValue(idx))},
				{Namespace: benchAliasNamespace, Key: benchSecondaryAliasKey, Value: domain.AliasValue(benchMixedNewClaimAliasValue(idx, gen))},
			}
		case i < newCount+selfReplaceCount:
			rep.Aliases = []domain.Alias{{
				Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchSelfReplaceAliasValue(idx, gen)),
			}}
		case i < newCount+selfReplaceCount+retractCount:
			// Aliases deliberately left nil/empty: explicit retraction.
		case i < newCount+selfReplaceCount+retractCount+valueConflictCount:
			victim := i - newCount - selfReplaceCount - retractCount
			rep.Aliases = []domain.Alias{
				{Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchAliasValue(idx))},
				{Namespace: benchAliasNamespace, Key: benchSecondaryAliasKey, Value: domain.AliasValue(benchConflictVictimAliasValue(victim))},
			}
		case i < newCount+selfReplaceCount+retractCount+valueConflictCount+crossContribCount:
			rep.ResourceType = benchResourceTypeB
			rep.Aliases = []domain.Alias{{
				Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchCrossContribAliasValue(idx)),
			}}
		default:
			rep.Aliases = []domain.Alias{{
				Namespace: benchAliasNamespace, Key: benchAliasKey, Value: domain.AliasValue(benchAliasValue(idx)),
			}}
		}
		reps[i] = rep
	}
	return reps
}

func buildInsertReplacements(st *benchState, n int) []domain.InventoryReplacement {
	indices := st.nextNewIndices(n)
	now := time.Now().UTC()
	reps := make([]domain.InventoryReplacement, n)
	for i, idx := range indices {
		obs := json.RawMessage(benchObservationJSON(int64(idx)))
		reps[i] = domain.InventoryReplacement{
			ResourceType: benchResourceType,
			Name:         benchResourceName(idx),
			CandidateUID: domain.NewExtensionResourceUID(),
			Labels:       benchLabels(0, idx),
			Observation:  &obs,
			Conditions:   benchConditions(now, idx, 0),
			ObservedAt:   now,
			ReceivedAt:   now,
		}
	}
	return reps
}

func buildHeartbeatDeltas(st *benchState, n int) []domain.InventoryDelta {
	indices := st.nextNeverAliasIndices(n)
	now := time.Now().UTC()
	deltas := make([]domain.InventoryDelta, n)
	for i, idx := range indices {
		deltas[i] = domain.InventoryDelta{
			ResourceType: benchResourceType,
			Name:         benchResourceName(idx),
			CandidateUID: domain.NewExtensionResourceUID(),
			ObservedAt:   now,
			ReceivedAt:   now,
		}
	}
	return deltas
}

// ---------------------------------------------------------------------------
// Input builders for the ACM baseline
// ---------------------------------------------------------------------------

func buildACMUpdateBatch(st *benchState, n int) (uids, clusters, datas []string) {
	// ACM's table has no aliases at all, so which pool this draws from
	// is arbitrary; neverAliasPool is as good as any.
	indices := st.nextNeverAliasIndices(n)
	gen := st.nextGen()
	uids = make([]string, n)
	clusters = make([]string, n)
	datas = make([]string, n)
	for i, idx := range indices {
		uids[i] = benchACMUID(idx)
		clusters[i] = benchClusterOf(idx)
		datas[i] = string(benchACMDataJSON(gen*int64(benchCorpusSize)+int64(idx), idx))
	}
	return uids, clusters, datas
}

func buildACMInsertBatch(st *benchState, n int) (uids, clusters, datas []string) {
	indices := st.nextNewIndices(n)
	uids = make([]string, n)
	clusters = make([]string, n)
	datas = make([]string, n)
	for i, idx := range indices {
		uids[i] = benchACMUID(idx)
		clusters[i] = benchClusterOf(idx)
		datas[i] = string(benchACMDataJSON(int64(idx), idx))
	}
	return uids, clusters, datas
}

// acmUpsertSQL is ACM search-indexer's per-resource write, verbatim
// per docs/design/reference/acm_search_indexing.md: a plain
// change-guarded upsert against a single (uid, cluster, data) table --
// no natural-key resolution, no normalized label/condition tables, no
// history, no aliases. Pipelined N times per batch via pgx.Batch
// rather than folded into one statement.
const acmUpsertSQL = `
INSERT INTO bench_acm_resources AS r (uid, cluster, data) VALUES ($1, $2, $3::jsonb)
ON CONFLICT (uid) DO UPDATE SET data = $3::jsonb
WHERE r.data IS DISTINCT FROM $3::jsonb`

// acmBatchUpsert pipelines the whole batch as one round trip using
// native pgx (via sql.Conn.Raw to reach the underlying *pgx.Conn) --
// per the user's point that there's no inherent reason a repository
// has to be built on database/sql just because ReplaceInventory/
// ApplyInventoryDeltas are: pgx.Batch's wire-level pipelining is the
// natural fit for "N independent single-row statements", whereas the
// CTE approach is the natural fit for "one statement, N rows batched
// via UNNEST".
func acmBatchUpsert(ctx context.Context, db *sql.DB, uids, clusters, datas []string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	return conn.Raw(func(driverConn any) error {
		pgxConn := driverConn.(*stdlib.Conn).Conn()
		batch := &pgx.Batch{}
		for i := range uids {
			batch.Queue(acmUpsertSQL, uids[i], clusters[i], datas[i])
		}
		br := pgxConn.SendBatch(ctx, batch)
		for range uids {
			if _, err := br.Exec(); err != nil {
				br.Close()
				return fmt.Errorf("acm batch upsert: %w", err)
			}
		}
		return br.Close()
	})
}

// ---------------------------------------------------------------------------
// EXPLAIN capture
// ---------------------------------------------------------------------------

// explainCapturingDB wraps a real *sql.DB and, on the first
// QueryContext call only, runs `EXPLAIN (ANALYZE, BUFFERS)` against
// the exact query and args a repository method is about to issue
// before letting the real call through -- so the plan reflects
// production's actual statement text and a production-realistic batch,
// not a hand-copied approximation of it. EXPLAIN ANALYZE really
// executes the (data-modifying) statement, so the batch used to
// trigger this is single-use and discarded rather than reused by the
// timed benchmark loop.
type explainCapturingDB struct {
	inner *sql.DB
	b     *testing.B
	label string
}

func (e *explainCapturingDB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return e.inner.ExecContext(ctx, query, args...)
}

func (e *explainCapturingDB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	logExplain(e.inner, e.label, query, args...)
	return e.inner.QueryContext(ctx, query, args...)
}

func (e *explainCapturingDB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return e.inner.QueryRowContext(ctx, query, args...)
}

func logExplain(db *sql.DB, label, query string, args ...any) {
	rows, err := db.QueryContext(context.Background(), "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) "+query, args...)
	if err != nil {
		fmt.Printf("\n=== EXPLAIN %s: error: %v ===\n", label, err)
		return
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			fmt.Printf("\n=== EXPLAIN %s: scan error: %v ===\n", label, err)
			return
		}
		sb.WriteString(line)
		sb.WriteByte('\n')
	}
	fmt.Printf("\n=== EXPLAIN: %s ===\n%s\n", label, sb.String())
}

func explainACMUpsert(db *sql.DB, label string, uid, cluster, data string) {
	logExplain(db, label, acmUpsertSQL, uid, cluster, data)
}

var (
	explainReplaceUpdateOnce        sync.Once
	explainReplaceInsertOnce        sync.Once
	explainReplaceUpdateAliasOnce   sync.Once
	explainReplaceRetractAliasOnce  sync.Once
	explainReplaceSelfReplaceOnce   sync.Once
	explainReplaceCrossContribOnce  sync.Once
	explainReplaceValueConflictOnce sync.Once
	explainReplaceMixedAliasOnce    sync.Once
	explainDeltaHeartbeatOnce       sync.Once
	explainACMUpdateOnce            sync.Once
	explainACMInsertOnce            sync.Once
)

// ---------------------------------------------------------------------------
// Benchmarks: CTE path
// ---------------------------------------------------------------------------

// BenchmarkCTE_ReplaceInventory_UpdateExisting is the dominant
// real-world case: a poller re-reporting resources that already exist,
// with a genuinely changed observation each time, and a resource type
// that has never contributed an alias at all (neverAliasPool -- see
// its doc comment). Exercises replaceInventorySQL's UPDATE branches
// throughout (upsert_inv, hist_obs, upsert_labels, upsert_conditions)
// with an empty input_aliases (see replaceInventorySQL's doc comment
// for why there's no separate alias-free statement variant to
// exercise instead) against a resource that never had anything for
// aliasRetractAbsentCTE's del_aliases_absent to retract, plus
// hist_conditions's actual INSERT branch for the benchConditionFlapPeriod-th
// of the batch whose Ready condition genuinely transitions -- see
// benchConditions's doc comment for why that matters.
//
// This is deliberately *not* the same as reporting zero aliases for a
// resource that has one on file -- see
// BenchmarkCTE_ReplaceInventory_RetractAlias for that (materially
// more expensive) scenario.
func BenchmarkCTE_ReplaceInventory_UpdateExisting(b *testing.B) {
	st := setupBenchOnce(b)
	repo := &ExtensionResourceRepo{DB: st.db}
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			if batchSize == benchExplainBatchSize {
				explainReplaceUpdateOnce.Do(func() {
					explainRepo := &ExtensionResourceRepo{DB: &explainCapturingDB{inner: st.db, b: b,
						label: fmt.Sprintf("ReplaceInventory, update existing, no alias, batch=%d", batchSize)}}
					if _, err := explainRepo.ReplaceInventory(ctx, buildUpdateReplacements(st, batchSize, false)); err != nil {
						b.Fatalf("explain replace update: %v", err)
					}
				})
			}
			preflight, err := repo.ReplaceInventory(ctx, buildUpdateReplacements(st, batchSize, false))
			if err != nil {
				b.Fatalf("ReplaceInventory: %v", err)
			}
			assertConflictCount(b, len(preflight), 0, "update existing, no alias")
			b.ResetTimer()
			for range b.N {
				if _, err := repo.ReplaceInventory(ctx, buildUpdateReplacements(st, batchSize, false)); err != nil {
					b.Fatalf("ReplaceInventory: %v", err)
				}
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkCTE_ReplaceInventory_InsertNew is the cold-start case: every
// report's resource is genuinely new, exercising resolved_er's INSERT
// branch (rather than ON CONFLICT DO NOTHING falling through to a
// lookup) for every row in the batch.
func BenchmarkCTE_ReplaceInventory_InsertNew(b *testing.B) {
	st := setupBenchOnce(b)
	repo := &ExtensionResourceRepo{DB: st.db}
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			if batchSize == benchExplainBatchSize {
				explainReplaceInsertOnce.Do(func() {
					explainRepo := &ExtensionResourceRepo{DB: &explainCapturingDB{inner: st.db, b: b,
						label: fmt.Sprintf("ReplaceInventory, insert new, no alias, batch=%d", batchSize)}}
					if _, err := explainRepo.ReplaceInventory(ctx, buildInsertReplacements(st, batchSize)); err != nil {
						b.Fatalf("explain replace insert: %v", err)
					}
				})
			}
			preflight, err := repo.ReplaceInventory(ctx, buildInsertReplacements(st, batchSize))
			if err != nil {
				b.Fatalf("ReplaceInventory: %v", err)
			}
			assertConflictCount(b, len(preflight), 0, "insert new, no alias")
			b.ResetTimer()
			for range b.N {
				if _, err := repo.ReplaceInventory(ctx, buildInsertReplacements(st, batchSize)); err != nil {
					b.Fatalf("ReplaceInventory: %v", err)
				}
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkCTE_ReplaceInventory_UpdateWithAlias is the steady-state
// alias case this session's optimization targeted: every report
// re-sends the same alias it sent last time, from the very same
// contributor (see seedAliasCorpus's doc comment for why that
// attribution matters), with extension_resources.alias_fingerprint
// already warmed to match that seeded alias state. That means this
// benchmark should exercise the fingerprint skip: needs_alias_processing
// filters the batch before aliasFoldCTEs/aliasRetractAbsentCTE do real
// work. The cost this benchmark quantifies is the residual overhead of
// carrying alias-capable SQL for warmed steady-state same-alias traffic.
func BenchmarkCTE_ReplaceInventory_UpdateWithAlias(b *testing.B) {
	st := setupBenchOnce(b)
	repo := &ExtensionResourceRepo{DB: st.db}
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			if batchSize == benchExplainBatchSize {
				explainReplaceUpdateAliasOnce.Do(func() {
					explainRepo := &ExtensionResourceRepo{DB: &explainCapturingDB{inner: st.db, b: b,
						label: fmt.Sprintf("ReplaceInventory, update existing, WITH alias (steady-state no-op), batch=%d", batchSize)}}
					if _, err := explainRepo.ReplaceInventory(ctx, buildUpdateReplacements(st, batchSize, true)); err != nil {
						b.Fatalf("explain replace update+alias: %v", err)
					}
				})
			}
			preflight, err := repo.ReplaceInventory(ctx, buildUpdateReplacements(st, batchSize, true))
			if err != nil {
				b.Fatalf("ReplaceInventory: %v", err)
			}
			assertConflictCount(b, len(preflight), 0, "update existing, WITH alias (steady-state no-op)")
			b.ResetTimer()
			for range b.N {
				if _, err := repo.ReplaceInventory(ctx, buildUpdateReplacements(st, batchSize, true)); err != nil {
					b.Fatalf("ReplaceInventory: %v", err)
				}
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkCTE_ReplaceInventory_RetractAlias isolates
// aliasRetractAbsentCTE's del_aliases_absent DELETE branch: every
// call retracts a real, pre-existing alias (see
// buildRetractAliasReplacementsFor). Contrast against
// BenchmarkCTE_ReplaceInventory_UpdateExisting, which measures the
// cheaper "never had one" case that same CTE also has to run for, but
// with nothing to actually delete.
//
// Each iteration draws its indices once, then -- with the timer
// stopped -- re-seeds a fresh alias for them via
// buildRetractSeedReplacementsFor before the timed call retracts it;
// see nextRetractIndices's doc comment for why this, not a bigger
// pool, is what makes retractPool safe to draw from indefinitely.
func BenchmarkCTE_ReplaceInventory_RetractAlias(b *testing.B) {
	st := setupBenchOnce(b)
	repo := &ExtensionResourceRepo{DB: st.db}
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			if batchSize == benchExplainBatchSize {
				explainReplaceRetractAliasOnce.Do(func() {
					indices := st.nextRetractIndices(batchSize)
					if _, err := repo.ReplaceInventory(ctx, buildRetractSeedReplacementsFor(st, indices)); err != nil {
						b.Fatalf("reseed before explain replace retract alias: %v", err)
					}
					explainRepo := &ExtensionResourceRepo{DB: &explainCapturingDB{inner: st.db, b: b,
						label: fmt.Sprintf("ReplaceInventory, explicit alias retraction, batch=%d", batchSize)}}
					if _, err := explainRepo.ReplaceInventory(ctx, buildRetractAliasReplacementsFor(st, indices)); err != nil {
						b.Fatalf("explain replace retract alias: %v", err)
					}
				})
			}
			preflightIndices := st.nextRetractIndices(batchSize)
			if _, err := repo.ReplaceInventory(ctx, buildRetractSeedReplacementsFor(st, preflightIndices)); err != nil {
				b.Fatalf("reseed before preflight replace retract alias: %v", err)
			}
			preflight, err := repo.ReplaceInventory(ctx, buildRetractAliasReplacementsFor(st, preflightIndices))
			if err != nil {
				b.Fatalf("ReplaceInventory: %v", err)
			}
			assertConflictCount(b, len(preflight), 0, "explicit alias retraction")
			b.ResetTimer()
			for range b.N {
				indices := st.nextRetractIndices(batchSize)
				b.StopTimer()
				if _, err := repo.ReplaceInventory(ctx, buildRetractSeedReplacementsFor(st, indices)); err != nil {
					b.Fatalf("reseed before replace retract alias: %v", err)
				}
				b.StartTimer()
				conflicts, err := repo.ReplaceInventory(ctx, buildRetractAliasReplacementsFor(st, indices))
				if err != nil {
					b.Fatalf("ReplaceInventory: %v", err)
				}
				assertConflictCount(b, len(conflicts), 0, "explicit alias retraction")
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkCTE_ReplaceInventory_SelfReplaceAlias isolates alias_safe's
// del_aliases_replaced DELETE-then-alias_upsert INSERT branch: every
// call replaces a contributor's own pre-existing alias with a new
// value it also owns exclusively (see
// buildSelfReplaceAliasReplacements) -- sibling_holds is false, so
// this is accepted, not AliasConflictResourceHasDifferentValue.
func BenchmarkCTE_ReplaceInventory_SelfReplaceAlias(b *testing.B) {
	st := setupBenchOnce(b)
	repo := &ExtensionResourceRepo{DB: st.db}
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			if batchSize == benchExplainBatchSize {
				explainReplaceSelfReplaceOnce.Do(func() {
					explainRepo := &ExtensionResourceRepo{DB: &explainCapturingDB{inner: st.db, b: b,
						label: fmt.Sprintf("ReplaceInventory, same-contributor self-replace, batch=%d", batchSize)}}
					conflicts, err := explainRepo.ReplaceInventory(ctx, buildSelfReplaceAliasReplacements(st, batchSize))
					if err != nil {
						b.Fatalf("explain replace self-replace alias: %v", err)
					}
					assertConflictCount(b, len(conflicts), 0, "same-contributor self-replace (EXPLAIN capture)")
				})
			}
			preflight, err := repo.ReplaceInventory(ctx, buildSelfReplaceAliasReplacements(st, batchSize))
			if err != nil {
				b.Fatalf("ReplaceInventory: %v", err)
			}
			assertConflictCount(b, len(preflight), 0, "same-contributor self-replace")
			b.ResetTimer()
			for range b.N {
				conflicts, err := repo.ReplaceInventory(ctx, buildSelfReplaceAliasReplacements(st, batchSize))
				if err != nil {
					b.Fatalf("ReplaceInventory: %v", err)
				}
				assertConflictCount(b, len(conflicts), 0, "same-contributor self-replace")
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkCTE_ReplaceInventory_CrossContributorConflict isolates
// alias_resource_conflicts' AliasConflictResourceHasDifferentValue
// outcome: every call reports from a second, sibling contributor
// whose claim always collides with the first contributor's
// pre-existing value for the same platform resource (see
// buildCrossContributorConflictReplacements) -- the one alias scenario
// in this file that genuinely requires two different contributors,
// not just one reporting differently over time.
func BenchmarkCTE_ReplaceInventory_CrossContributorConflict(b *testing.B) {
	st := setupBenchOnce(b)
	repo := &ExtensionResourceRepo{DB: st.db}
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			if batchSize == benchExplainBatchSize {
				explainReplaceCrossContribOnce.Do(func() {
					explainRepo := &ExtensionResourceRepo{DB: &explainCapturingDB{inner: st.db, b: b,
						label: fmt.Sprintf("ReplaceInventory, cross-contributor conflict, batch=%d", batchSize)}}
					conflicts, err := explainRepo.ReplaceInventory(ctx, buildCrossContributorConflictReplacements(st, batchSize))
					if err != nil {
						b.Fatalf("explain replace cross-contributor conflict: %v", err)
					}
					assertConflictCount(b, len(conflicts), batchSize, "cross-contributor conflict (EXPLAIN capture)")
				})
			}
			preflight, err := repo.ReplaceInventory(ctx, buildCrossContributorConflictReplacements(st, batchSize))
			if err != nil {
				b.Fatalf("ReplaceInventory: %v", err)
			}
			assertConflictCount(b, len(preflight), batchSize, "cross-contributor conflict")
			b.ResetTimer()
			for range b.N {
				conflicts, err := repo.ReplaceInventory(ctx, buildCrossContributorConflictReplacements(st, batchSize))
				if err != nil {
					b.Fatalf("ReplaceInventory: %v", err)
				}
				assertConflictCount(b, len(conflicts), batchSize, "cross-contributor conflict")
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkCTE_ReplaceInventory_ValueClaimedByOtherConflict isolates
// alias_value_conflicts' AliasConflictValueClaimedByOther outcome
// (phase 1, resolved without ever reaching phase 2): every call
// reports a value one of seedConflictVictims's dedicated victims
// already owns (see buildValueClaimedByOtherReplacements).
func BenchmarkCTE_ReplaceInventory_ValueClaimedByOtherConflict(b *testing.B) {
	st := setupBenchOnce(b)
	repo := &ExtensionResourceRepo{DB: st.db}
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			if batchSize == benchExplainBatchSize {
				explainReplaceValueConflictOnce.Do(func() {
					explainRepo := &ExtensionResourceRepo{DB: &explainCapturingDB{inner: st.db, b: b,
						label: fmt.Sprintf("ReplaceInventory, value claimed by other, batch=%d", batchSize)}}
					conflicts, err := explainRepo.ReplaceInventory(ctx, buildValueClaimedByOtherReplacements(st, batchSize))
					if err != nil {
						b.Fatalf("explain replace value claimed by other: %v", err)
					}
					assertConflictCount(b, len(conflicts), batchSize, "value claimed by other (EXPLAIN capture)")
				})
			}
			preflight, err := repo.ReplaceInventory(ctx, buildValueClaimedByOtherReplacements(st, batchSize))
			if err != nil {
				b.Fatalf("ReplaceInventory: %v", err)
			}
			assertConflictCount(b, len(preflight), batchSize, "value claimed by other")
			b.ResetTimer()
			for range b.N {
				conflicts, err := repo.ReplaceInventory(ctx, buildValueClaimedByOtherReplacements(st, batchSize))
				if err != nil {
					b.Fatalf("ReplaceInventory: %v", err)
				}
				assertConflictCount(b, len(conflicts), batchSize, "value claimed by other")
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkCTE_ReplaceInventory_UpdateWithMixedAlias contrasts with
// UpdateWithAlias's 100%-no-op traffic: see
// buildMixedAliasReplacements's doc comment for the mix (all six
// outcomes the isolated benchmarks above measure separately, blended
// into one batch at illustrative-not-measured ratios). It exists to
// answer a specific question about aliasFoldCTEs' two-phase,
// read-by-value-then-by-resource design (phase 1 = by_value, phase 2
// = by_resource/sibling, gated on by_value's value_claim_id being
// NULL): the no-op case never reaches either phase at all (self_claim/
// changed filters it out first), but AliasConflictValueClaimedByOther
// is resolved by phase 1 alone (the (namespace, key, value) tuple it
// looks up already exists either way, so ownership is unambiguous
// without ever checking what else this resource holds) -- while a
// genuinely new alias, a self-replacement's new value, or
// AliasConflictResourceHasDifferentValue has never been seen (or is
// deliberately not looked for) under that tuple before, so those fall
// through to phase 2. Whether the value-first design's two-reads-for-some-rows
// trade nets out favorably against the simpler by-resource-only design
// (this session's "small version" -- see git history, which paid for
// exactly one read per row) depends on the real mix of these outcomes
// in production traffic, which is exactly what this benchmark can't
// determine on its own (the percentages in buildMixedAliasReplacements's
// doc comment are illustrative, not measured) -- it's here so that
// question can be re-asked, with real query plans, if that mix ever
// becomes known.
func BenchmarkCTE_ReplaceInventory_UpdateWithMixedAlias(b *testing.B) {
	st := setupBenchOnce(b)
	repo := &ExtensionResourceRepo{DB: st.db}
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			// Deterministic for a given batchSize: buildMixedAliasReplacements
			// carves its conflict-producing buckets (valueConflictCount,
			// crossContribCount) as fixed integer-truncated fractions of n,
			// so this is an exact expectation, not an approximation --
			// despite the "expect ~N" wording this replaced used to hedge.
			expectConflicts := batchSize*7/100 + batchSize*5/100
			if batchSize == benchExplainBatchSize {
				explainReplaceMixedAliasOnce.Do(func() {
					explainRepo := &ExtensionResourceRepo{DB: &explainCapturingDB{inner: st.db, b: b,
						label: fmt.Sprintf("ReplaceInventory, update existing, WITH alias (mixed outcomes), batch=%d", batchSize)}}
					conflicts, err := explainRepo.ReplaceInventory(ctx, buildMixedAliasReplacements(st, batchSize))
					if err != nil {
						b.Fatalf("explain replace update+mixed alias: %v", err)
					}
					assertConflictCount(b, len(conflicts), expectConflicts, "mixed-alias workload (EXPLAIN capture)")
				})
			}
			preflight, err := repo.ReplaceInventory(ctx, buildMixedAliasReplacements(st, batchSize))
			if err != nil {
				b.Fatalf("ReplaceInventory: %v", err)
			}
			assertConflictCount(b, len(preflight), expectConflicts, "mixed-alias workload")
			b.ResetTimer()
			for range b.N {
				conflicts, err := repo.ReplaceInventory(ctx, buildMixedAliasReplacements(st, batchSize))
				if err != nil {
					b.Fatalf("ReplaceInventory: %v", err)
				}
				assertConflictCount(b, len(conflicts), expectConflicts, "mixed-alias workload")
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkCTE_ApplyInventoryDeltas_Heartbeat is the cheapest possible
// real call: no label/condition/observation changes at all, just an
// observed-at bump (still a genuine write to extension_resource_inventory
// for every row) -- isolates the statement's fixed per-round-trip
// planning/UNNEST/join overhead from any actual change-guarded write
// cost.
func BenchmarkCTE_ApplyInventoryDeltas_Heartbeat(b *testing.B) {
	st := setupBenchOnce(b)
	repo := &ExtensionResourceRepo{DB: st.db}
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			if batchSize == benchExplainBatchSize {
				explainDeltaHeartbeatOnce.Do(func() {
					explainRepo := &ExtensionResourceRepo{DB: &explainCapturingDB{inner: st.db, b: b,
						label: fmt.Sprintf("ApplyInventoryDeltas, heartbeat (no field changes), batch=%d", batchSize)}}
					if _, err := explainRepo.ApplyInventoryDeltas(ctx, buildHeartbeatDeltas(st, batchSize)); err != nil {
						b.Fatalf("explain delta heartbeat: %v", err)
					}
				})
			}
			preflight, err := repo.ApplyInventoryDeltas(ctx, buildHeartbeatDeltas(st, batchSize))
			if err != nil {
				b.Fatalf("ApplyInventoryDeltas: %v", err)
			}
			assertConflictCount(b, len(preflight), 0, "heartbeat (no field changes)")
			b.ResetTimer()
			for range b.N {
				if _, err := repo.ApplyInventoryDeltas(ctx, buildHeartbeatDeltas(st, batchSize)); err != nil {
					b.Fatalf("ApplyInventoryDeltas: %v", err)
				}
			}
			reportPerItem(b, batchSize)
		})
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: ACM-like baseline
// ---------------------------------------------------------------------------

// BenchmarkACM_BatchUpsert_UpdateExisting is the ACM-model counterpart
// of BenchmarkCTE_ReplaceInventory_UpdateExisting: same corpus size,
// same batch sizes, same "genuinely changed payload every call"
// semantics, but against a single flat table with no natural-key
// resolution, no normalized label/condition tables, and no aliases --
// pipelined via pgx.Batch instead of folded into one CTE statement.
func BenchmarkACM_BatchUpsert_UpdateExisting(b *testing.B) {
	st := setupBenchOnce(b)
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			if batchSize == benchExplainBatchSize {
				explainACMUpdateOnce.Do(func() {
					uids, clusters, datas := buildACMUpdateBatch(st, 1)
					explainACMUpsert(st.db, "ACM upsert, update existing (single pipelined statement shown; batch repeats this N times)",
						uids[0], clusters[0], datas[0])
				})
			}
			b.ResetTimer()
			for range b.N {
				uids, clusters, datas := buildACMUpdateBatch(st, batchSize)
				if err := acmBatchUpsert(ctx, st.db, uids, clusters, datas); err != nil {
					b.Fatalf("acm batch upsert: %v", err)
				}
			}
			reportPerItem(b, batchSize)
		})
	}
}

// BenchmarkACM_BatchUpsert_InsertNew is the ACM-model counterpart of
// BenchmarkCTE_ReplaceInventory_InsertNew.
func BenchmarkACM_BatchUpsert_InsertNew(b *testing.B) {
	st := setupBenchOnce(b)
	ctx := context.Background()

	for _, batchSize := range benchBatchSizes {
		b.Run(fmt.Sprintf("batch=%d", batchSize), func(b *testing.B) {
			if batchSize == benchExplainBatchSize {
				explainACMInsertOnce.Do(func() {
					uids, clusters, datas := buildACMInsertBatch(st, 1)
					explainACMUpsert(st.db, "ACM upsert, insert new (single pipelined statement shown; batch repeats this N times)",
						uids[0], clusters[0], datas[0])
				})
			}
			b.ResetTimer()
			for range b.N {
				uids, clusters, datas := buildACMInsertBatch(st, batchSize)
				if err := acmBatchUpsert(ctx, st.db, uids, clusters, datas); err != nil {
					b.Fatalf("acm batch upsert: %v", err)
				}
			}
			reportPerItem(b, batchSize)
		})
	}
}

// assertConflictCount fails b immediately if got (the length of a
// ReplaceInventory/ApplyInventoryDeltas call's returned
// []domain.AliasConflict) doesn't match want, identifying which
// scenario/batch size tripped it. Every alias benchmark below calls
// this twice: once on a pre-flight call before b.ResetTimer, so a
// repository regression that flips this scenario's outcome (a
// conflict silently becoming an accepted write, or vice versa) fails
// before any time is spent measuring the wrong thing, and again
// inside the timed loop itself for the conflict-heavy scenarios,
// where a mid-run regression -- including this file's own
// pool-exhaustion bugs, see drawFromPoolNoWrap -- would otherwise go
// undetected: previously these counts were only ever printed, on the
// batch=1000 EXPLAIN path alone, never asserted, so a benchmark could
// keep passing and reporting timings for a scenario it had silently
// stopped measuring.
func assertConflictCount(b *testing.B, got, want int, scenario string) {
	b.Helper()
	if got != want {
		b.Fatalf("%s: got %d alias conflicts, want %d -- this benchmark's timing no longer reflects the scenario it claims to measure",
			scenario, got, want)
	}
}

// reportPerItem adds a ns/item custom metric alongside the standard
// ns/op (which is per-*call*, i.e. per whole batch) -- ns/item is what
// lets the CTE path's O(1)-round-trips-per-chunk design be compared
// apples-to-apples against the ACM baseline's O(n)-statements-per-batch
// pipelining across different batch sizes.
func reportPerItem(b *testing.B, batchSize int) {
	if b.N == 0 {
		return
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(batchSize), "ns/item")
}
