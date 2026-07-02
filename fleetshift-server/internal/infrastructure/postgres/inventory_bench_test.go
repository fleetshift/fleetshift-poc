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
// the production CTE constants (replaceInventorySQLNoAliases etc.)
// directly for EXPLAIN capture, with zero risk of the benchmark's copy
// of the SQL drifting from what ReplaceInventory/ApplyInventoryDeltas
// actually run.
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

const (
	benchAliasNamespace domain.AliasNamespace = "ext-id"
	benchAliasKey       domain.AliasKey       = "source-id"
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

func benchConditions(now time.Time) []domain.Condition {
	ready, _ := domain.NewCondition("Ready", domain.ConditionTrue, "AllGood", "steady", now)
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

	// updatePool is a fixed permutation of [0, benchCorpusSize) used to
	// draw distinct, scattered "existing resource" indices for a given
	// batch without re-shuffling per call (which would put Go-side
	// allocation cost into the very SQL timing we're trying to
	// isolate). poolPos rotates through it.
	updatePool []int
	poolPos    atomic.Int64

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

func (st *benchState) nextUpdateIndices(n int) []int {
	pos := st.poolPos.Add(int64(n)) - int64(n)
	start := int(pos % int64(len(st.updatePool)))
	out := make([]int, n)
	for i := range n {
		out[i] = st.updatePool[(start+i)%len(st.updatePool)]
	}
	return out
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

	fmt.Println("\n[bench] seeding corpus (this happens once for the whole run)...")
	seedStart := time.Now()

	if err := seedExtensionResourceCorpus(ctx, db, benchCorpusSize); err != nil {
		return nil, err
	}
	if err := seedAliasCorpus(ctx, db, benchCorpusSize); err != nil {
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

	pool := rand.New(rand.NewSource(42)).Perm(benchCorpusSize)
	return &benchState{db: db, updatePool: pool}, nil
}

// ---------------------------------------------------------------------------
// Seeding: bulk multi-row inserts (UNNEST-backed, chunked at
// benchSeedChunkRows resources per statement) rather than routing
// through ReplaceInventory -- this is corpus setup, not part of what
// we're measuring, so it should be as fast as possible and not itself
// exercise the code under test.
// ---------------------------------------------------------------------------

func seedExtensionResourceCorpus(ctx context.Context, db *sql.DB, n int) error {
	for start := 0; start < n; start += benchSeedChunkRows {
		end := min(start+benchSeedChunkRows, n)
		size := end - start

		uids := make([]string, size)
		serviceNames := make([]string, size)
		typeNames := make([]string, size)
		collectionNames := make([]string, size)
		resourceIDs := make([]string, size)
		labelsJSON := make([]string, size)
		createdAts := make([]time.Time, size)
		observations := make([]*string, size)

		for i := range size {
			idx := start + i
			uids[i] = domain.NewExtensionResourceUID().String()
			serviceNames[i] = benchServiceName
			typeNames[i] = benchTypeName
			collectionNames[i] = benchCollectionName
			resourceIDs[i] = benchResourceID(idx)
			labelsJSON[i] = "{}"
			createdAts[i] = time.Now().UTC()
			obs := string(benchObservationJSON(0))
			observations[i] = &obs
		}

		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, created_at, updated_at)
			SELECT u, s, t, c, r, l::jsonb, ts, ts
			FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::timestamptz[])
				AS x(u, s, t, c, r, l, ts)
		`, uids, serviceNames, typeNames, collectionNames, resourceIDs, labelsJSON, createdAts); err != nil {
			return fmt.Errorf("seed extension_resources[%d:%d]: %w", start, end, err)
		}

		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resource_inventory (extension_resource_uid, observation, observed_at, updated_at)
			SELECT u, o::jsonb, ts, ts
			FROM UNNEST($1::uuid[], $2::text[], $3::timestamptz[]) AS x(u, o, ts)
		`, uids, observations, createdAts); err != nil {
			return fmt.Errorf("seed extension_resource_inventory[%d:%d]: %w", start, end, err)
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
			return fmt.Errorf("seed extension_resource_inventory_labels[%d:%d]: %w", start, end, err)
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
		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resource_inventory_conditions
				(extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, updated_at)
			SELECT u, t, s, '', '', lt, lt, lt
			FROM UNNEST($1::uuid[], $2::text[], $3::text[], $4::timestamptz[]) AS x(u, t, s, lt)
		`, condUIDs, condTypes, condStatuses, condLastTransitions); err != nil {
			return fmt.Errorf("seed extension_resource_inventory_conditions[%d:%d]: %w", start, end, err)
		}
	}
	return nil
}

// seedAliasCorpus gives every resource in [0, n) exactly one
// already-resolved alias, so the "UpdateWithAlias" benchmark measures
// the realistic steady-state case: an addon that always reports the
// same alias on every poll, hitting resource_aliases' "already
// resolved, no-op" path rather than a first-time claim.
func seedAliasCorpus(ctx context.Context, db *sql.DB, n int) error {
	for start := 0; start < n; start += benchSeedChunkRows {
		end := min(start+benchSeedChunkRows, n)
		size := end - start

		collectionNames := make([]string, size)
		resourceIDs := make([]string, size)
		createdAts := make([]time.Time, size)
		for i := range size {
			collectionNames[i] = benchCollectionName
			resourceIDs[i] = benchResourceID(start + i)
			createdAts[i] = time.Now().UTC()
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO platform_resources (collection_name, resource_id, labels, created_at, updated_at)
			SELECT c, r, '{}'::jsonb, t, t
			FROM UNNEST($1::text[], $2::text[], $3::timestamptz[]) AS x(c, r, t)
		`, collectionNames, resourceIDs, createdAts); err != nil {
			return fmt.Errorf("seed platform_resources[%d:%d]: %w", start, end, err)
		}

		namespaces := make([]string, size)
		keys := make([]string, size)
		values := make([]string, size)
		for i := range size {
			namespaces[i] = string(benchAliasNamespace)
			keys[i] = string(benchAliasKey)
			values[i] = benchAliasValue(start + i)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO resource_aliases (namespace, key, value, platform_collection_name, platform_resource_id, created_at)
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::timestamptz[])
		`, namespaces, keys, values, collectionNames, resourceIDs, createdAts); err != nil {
			return fmt.Errorf("seed resource_aliases[%d:%d]: %w", start, end, err)
		}
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
		"resource_aliases",
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

func buildUpdateReplacements(st *benchState, n int, withAlias bool) []domain.InventoryReplacement {
	indices := st.nextUpdateIndices(n)
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
			Conditions:   benchConditions(now),
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
			Conditions:   benchConditions(now),
			ObservedAt:   now,
			ReceivedAt:   now,
		}
	}
	return reps
}

func buildHeartbeatDeltas(st *benchState, n int) []domain.InventoryDelta {
	indices := st.nextUpdateIndices(n)
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
	indices := st.nextUpdateIndices(n)
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
	explainReplaceUpdateOnce      sync.Once
	explainReplaceInsertOnce      sync.Once
	explainReplaceUpdateAliasOnce sync.Once
	explainDeltaHeartbeatOnce     sync.Once
	explainACMUpdateOnce          sync.Once
	explainACMInsertOnce          sync.Once
)

// ---------------------------------------------------------------------------
// Benchmarks: CTE path
// ---------------------------------------------------------------------------

// BenchmarkCTE_ReplaceInventory_UpdateExisting is the dominant
// real-world case: a poller re-reporting resources that already exist,
// with a genuinely changed observation each time, no aliases. Exercises
// replaceInventorySQLNoAliases's UPDATE branches throughout (upsert_inv,
// hist_obs, upsert_labels, upsert_conditions, hist_conditions).
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
// re-sends the same alias it sent last time (already resolved, no
// conflict), so it exercises replaceInventorySQLWithAliases's five
// extra joins/anti-joins purely for a result that turns out to be a
// no-op -- the cost this benchmark quantifies is exactly what
// replaceInventorySQLNoAliases now avoids for the (much more common)
// no-alias case.
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
