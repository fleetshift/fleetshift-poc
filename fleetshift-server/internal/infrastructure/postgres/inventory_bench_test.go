package postgres

// This file benchmarks the current, simplified inventory write path
// (ReplaceInventory/ApplyInventoryDeltas -- see extension_resource_repo.go's
// "Inventory methods" section doc comment) against a realistic,
// tall (~100k row) corpus so the planner has real row counts to work
// with, and against a much simpler baseline modeled on Red Hat ACM
// Search's write path: a single (uid, cluster, data jsonb) table,
// upserted with a change-guarded `ON CONFLICT ... WHERE data IS
// DISTINCT FROM` statement, pipelined via native pgx.Batch instead of
// database/sql (see docs/design/reference/acm_search_indexing.md).
// Relationship edges (ACM's search.edges) are intentionally out of
// scope -- this is strictly a single-table write-path comparison, the
// same scope the original request asked for.
//
// It replaces an earlier, much larger benchmark (same file name, see
// git history) written against the previous design: normalized
// observation/condition history tables plus a claims/contributions
// alias model with synchronous cross-resource conflict detection. That
// design no longer exists on the hot path (see
// docs/design/architecture/open_questions.md's "Alias write path"
// entry and [domain.InventoryReplacement.Aliases]'s doc), so its
// benchmark's conflict-classification scenarios (cross-contributor
// disputes, value-claimed-by-other, etc.) no longer have anything to
// measure -- there is no synchronous alias classification left to
// exercise. This version measures what the current write path
// actually does: a single CTE-chained upsert per ReplaceInventory
// call, with reported aliases stored verbatim and skipped entirely
// when unchanged. The ACM baseline comparison, and the corpus/data
// shape it needs (kind/namespace spread, hub-cluster pseudo-nodes,
// etc.), carries over from that earlier benchmark largely unchanged --
// it never depended on the alias/history model that changed.
//
// Lives in `package postgres` (not `postgres_test`) so it can
// reference the production CTE constants (replaceInventorySQL et al.)
// directly for EXPLAIN capture, with zero risk of the benchmark's copy
// of the SQL drifting from what ReplaceInventory/ApplyInventoryDeltas
// actually run.
//
// Run with:
//
//	FLEETSHIFT_INVENTORY_BENCH=1 go test ./internal/infrastructure/postgres/ -run TestInventoryWritePathBenchmark -v -timeout 20m
//
// (skipped by default -- it spins up its own Postgres container,
// separate from the contract tests' shared one, and seeds a
// ~100k-row corpus, so it's too slow for the default `go test ./...`
// loop.)

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
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
	ipbServiceName    = "bench.fleetshift.io"
	ipbTypeName       = "Widget"
	ipbCollectionName = "widgets"

	// ipbPoolSize is the number of pre-existing resources seeded into
	// each of the three steady-state pools below (100,002 total,
	// matching the ~100k scale the previous, now-historical version
	// of this benchmark used), large enough that the planner sees
	// real, non-trivial row counts (via ANALYZE, after seeding) for
	// the natural-key lookups replaceInventorySQL/applyInventoryDeltasSQL
	// join against, and that the ACM baseline's GIN indexes (see this
	// file's package doc comment) build real, multi-page posting
	// lists rather than a handful of trivially-cached ones.
	ipbPoolSize = 33_334

	// ipbRounds is how many timed calls each scenario/batch-size
	// combination makes; timings report mean/p50/p95 across these.
	// 15 rather than a smaller number specifically because the
	// alias-cost breakdown (printAliasCostBreakdown) compares p50
	// across scenarios at small batch sizes, where a single slow
	// round (background checkpoint/vacuum activity in the
	// container) skews a mean noticeably more than it skews a
	// median -- more rounds narrows that gap further.
	ipbRounds = 15
)

// ipbBatchSizes are the report-batch sizes every scenario sweeps: 100
// (well below any chunking), 1000 (InventoryReportService's
// defaultReportChunkSize -- see inventory_report_service.go), 2500
// (ACM search-indexer's own default DB_BATCH_SIZE, per
// docs/design/reference/acm_search_indexing.md section 3.7), and 5000
// (well above either, to see how each design scales past its own
// production default).
var ipbBatchSizes = []int{100, 1000, 2500, 5000}

const ipbResourceType = domain.ResourceType(ipbServiceName + "/" + ipbTypeName)

const (
	ipbAliasNamespace domain.AliasNamespace = "ext-id"
	ipbAliasKey       domain.AliasKey       = "source-id"
)

// ipbNoAliasStart/ipbSameAliasStart/ipbChangedAliasStart carve the
// seeded corpus into three disjoint index ranges, each reserved for
// exactly one steady-state scenario below -- see seedIPBCorpus.
const (
	ipbNoAliasStart      = 0
	ipbSameAliasStart    = ipbNoAliasStart + ipbPoolSize
	ipbChangedAliasStart = ipbSameAliasStart + ipbPoolSize
	ipbCorpusSize        = ipbChangedAliasStart + ipbPoolSize

	// ipbNewStart is where indices for "always a genuine new
	// resource" scenarios begin, well clear of the seeded corpus
	// above so a "new" draw can never collide with it.
	ipbNewStart = 1_000_000
)

func ipbResourceID(idx int) string { return fmt.Sprintf("r-%08d", idx) }

func ipbResourceName(idx int) domain.ResourceName {
	name, _ := domain.NewResourceName(domain.CollectionName(ipbCollectionName), domain.ResourceID(ipbResourceID(idx)))
	return name
}

func ipbAliasValue(idx int, gen int64) domain.AliasValue {
	return domain.AliasValue(fmt.Sprintf("ext-%08d-gen%d", idx, gen))
}

// ipbSteadyAliasValue is ipbSameAliasStart's fixed, gen-independent
// value: reporting the exact same value every round is what exercises
// replaceInventorySQL's needs_alias_payload_write fingerprint skip.
func ipbSteadyAliasValue(idx int) domain.AliasValue {
	return domain.AliasValue(fmt.Sprintf("steady-%08d", idx))
}

// ipbObservation mirrors a small Kubernetes-style status payload --
// realistic-ish size (a few hundred bytes) rather than a trivial
// {"n":1}, since payload size affects JSONB TOAST/inline behavior and
// thus write cost.
type ipbObservation struct {
	Phase     string            `json:"phase"`
	Gen       int64             `json:"gen"`
	NodeCount int               `json:"nodeCount"`
	Version   string            `json:"version"`
	Metadata  map[string]string `json:"metadata"`
}

func ipbObservationJSON(gen int64) json.RawMessage {
	obs := ipbObservation{
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

func ipbLabels(idx int, gen int64) map[string]string {
	return map[string]string{
		"label-0": fmt.Sprintf("v-%d", (int64(idx)+gen)%97),
		"label-1": fmt.Sprintf("v-%d", (int64(idx)+gen)%53),
		"label-2": fmt.Sprintf("v-%d", (int64(idx)+gen)%31),
		"label-3": fmt.Sprintf("v-%d", (int64(idx)+gen)%17),
	}
}

// ipbConditionFlapPeriod controls how often ipbConditions reports a
// genuine status transition instead of repeating the previous
// steady-state value -- see the now-historical
// benchConditionFlapPeriod this mirrors (git history) for why a
// constant-every-time condition would be an unrealistic workload; the
// reasoning still applies even though condition *history* is no
// longer written by this write path; the JSONB conditions column
// itself still needs a realistic mix of "same value" and "changed
// value" writes.
const ipbConditionFlapPeriod = 20

func ipbConditionFlaps(idx int, gen int64) bool {
	h := uint64(idx)*2654435761 ^ uint64(gen)*0x9E3779B97F4A7C15
	return h%ipbConditionFlapPeriod == 0
}

func ipbConditions(now time.Time, idx int, gen int64) []domain.Condition {
	readyStatus, reason, message := domain.ConditionTrue, "AllGood", "steady"
	if ipbConditionFlaps(idx, gen) {
		readyStatus, reason, message = domain.ConditionFalse, "ProbeFailed", "liveness probe failing"
	}
	ready, _ := domain.NewCondition("Ready", readyStatus, reason, message, now)
	healthy, _ := domain.NewCondition("Healthy", domain.ConditionTrue, "AllGood", "steady", now)
	return []domain.Condition{ready, healthy}
}

// ---------------------------------------------------------------------------
// Seeding
// ---------------------------------------------------------------------------

func seedIPBType(t *testing.T, db *sql.DB) {
	t.Helper()
	def := domain.NewExtensionResourceType(
		ipbResourceType, "v1", domain.CollectionID(ipbCollectionName), fixedIPBTime,
		domain.WithManagement(
			domain.NewRegisteredSelfTarget(domain.TargetID("addon-widget"), domain.ManifestType("api.test.widget")),
			domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
				ContentHash:    []byte("hash"),
				SignatureBytes: []byte("sig"),
			},
		),
	)
	repo := &ExtensionResourceRepo{DB: db}
	if err := repo.CreateType(context.Background(), def); err != nil {
		t.Fatalf("seed type: %v", err)
	}
}

var fixedIPBTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

// seedIPBCorpus populates the three steady-state pools
// (ipbNoAliasStart/ipbSameAliasStart/ipbChangedAliasStart) at
// generation 0, chunked through the real ReplaceInventory path so the
// seeded rows are byte-for-byte what a real first report would have
// written.
func seedIPBCorpus(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	repo := &ExtensionResourceRepo{DB: db}
	const chunk = 5000

	seedRange := func(start, count int, withAlias, steady bool) {
		for base := 0; base < count; base += chunk {
			n := min(chunk, count-base)
			reps := make([]domain.InventoryReplacement, n)
			for i := 0; i < n; i++ {
				idx := start + base + i
				obs := ipbObservationJSON(0)
				rep := domain.InventoryReplacement{
					ResourceType: ipbResourceType,
					Name:         ipbResourceName(idx),
					CandidateUID: domain.NewExtensionResourceUID(),
					Labels:       ipbLabels(idx, 0),
					Observation:  &obs,
					Conditions:   ipbConditions(fixedIPBTime, idx, 0),
					ObservedAt:   fixedIPBTime,
					ReceivedAt:   fixedIPBTime,
				}
				if withAlias {
					var val domain.AliasValue
					if steady {
						val = ipbSteadyAliasValue(idx)
					} else {
						val = ipbAliasValue(idx, 0)
					}
					alias, err := domain.NewAlias(ipbAliasNamespace, ipbAliasKey, val)
					if err != nil {
						t.Fatalf("seed alias: %v", err)
					}
					rep.Aliases = []domain.Alias{alias}
				}
				reps[i] = rep
			}
			if err := repo.ReplaceInventory(ctx, reps); err != nil {
				t.Fatalf("seed chunk [%d,%d): %v", start+base, start+base+n, err)
			}
		}
	}

	seedRange(ipbNoAliasStart, ipbPoolSize, false, false)
	seedRange(ipbSameAliasStart, ipbPoolSize, true, true)
	seedRange(ipbChangedAliasStart, ipbPoolSize, true, false)

	if _, err := db.ExecContext(ctx, `ANALYZE extension_resources, extension_resource_inventory`); err != nil {
		t.Fatalf("analyze: %v", err)
	}
}

func ipbTableSizes(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT relname, pg_size_pretty(pg_total_relation_size(oid))
		FROM pg_class
		WHERE relname IN ('extension_resources', 'extension_resource_inventory', 'bench_acm_resources')
		ORDER BY relname`)
	if err != nil {
		t.Logf("table size query failed (non-fatal): %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name, size string
		if err := rows.Scan(&name, &size); err != nil {
			continue
		}
		t.Logf("  %-32s %s", name, size)
	}
}

// ---------------------------------------------------------------------------
// ACM baseline -- see this file's package doc comment for the model
// this reproduces. Kept as close as possible to
// docs/design/reference/acm_search_indexing.md's actual schema/SQL
// (section 3.6/3.2) rather than a simplified approximation.
// ---------------------------------------------------------------------------

// ipbACMKinds/ipbACMNamespaces are a small, realistic-ish spread of
// Kubernetes object kinds/namespaces -- enough value diversity that
// the GIN indexes below (see ipbACMDataJSON) build real, non-trivial
// posting lists rather than one giant list for a single repeated
// value, which would understate real index-maintenance cost.
var (
	ipbACMKinds      = []string{"Pod", "Deployment", "ConfigMap", "Service", "Secret", "ReplicaSet"}
	ipbACMNamespaces = []string{"default", "kube-system", "openshift-monitoring", "app-team-a", "app-team-b"}
)

func ipbACMUID(idx int) string    { return fmt.Sprintf("acm-%08d", idx) }
func ipbClusterOf(idx int) string { return fmt.Sprintf("cluster-%d", idx%500) }

// ipbACMDataJSON builds the `data` payload for the ACM baseline's
// bench_acm_resources.data column, shaped like a real search-indexer
// sync entry: it carries the same kind/namespace/name/apigroup/
// kind_plural keys the real search.resources GIN indexes are built
// over (see acm_search_indexing.md's schema section), so seeding/
// updating this table pays the same index-maintenance cost a real
// search-indexer write would. _hubClusterResource is set true for
// idx < 500 only -- exactly one row per cluster (there are 500
// distinct clusters, see ipbClusterOf) -- mirroring ACM's
// one-pseudo-node-per-cluster convention that the partial
// bench_acm_data_hubcluster_idx index is sized around.
func ipbACMDataJSON(gen int64, idx int) []byte {
	kind := ipbACMKinds[idx%len(ipbACMKinds)]
	data := map[string]any{
		"kind":            kind,
		"namespace":       ipbACMNamespaces[idx%len(ipbACMNamespaces)],
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

// seedIPBACMTable creates bench_acm_resources -- schema and all 7
// indexes lifted directly from acm_search_indexing.md section 3.6 --
// and seeds it with the same row count as the CTE-path corpus
// (ipbCorpusSize), so both sides of the comparison maintain
// similarly-sized indexes over similarly-sized tables.
func seedIPBACMTable(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE bench_acm_resources (
			uid     TEXT PRIMARY KEY,
			cluster TEXT NOT NULL,
			data    JSONB NOT NULL
		)`); err != nil {
		t.Fatalf("create bench_acm_resources: %v", err)
	}
	// Same 7-index set as the real search.resources schema (PK btree +
	// 1 plain btree + 5 GIN over `data` expressions) -- without these,
	// the ACM baseline would understate real search-indexer write
	// cost: every write touches `data`, so with these indexes present
	// Postgres can never use a HOT update here (any indexed column
	// changing forces a new index entry in every index on the table),
	// and GIN maintenance is materially more CPU-costly per entry than
	// btree.
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
			t.Fatalf("create bench_acm_resources index: %v", err)
		}
	}

	const chunk = 5000
	for start := 0; start < ipbCorpusSize; start += chunk {
		end := min(start+chunk, ipbCorpusSize)
		n := end - start
		uids := make([]string, n)
		clusters := make([]string, n)
		datas := make([]string, n)
		for i := 0; i < n; i++ {
			idx := start + i
			uids[i] = ipbACMUID(idx)
			clusters[i] = ipbClusterOf(idx)
			datas[i] = string(ipbACMDataJSON(0, idx))
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO bench_acm_resources (uid, cluster, data)
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::jsonb[])
		`, uids, clusters, datas); err != nil {
			t.Fatalf("seed bench_acm_resources[%d:%d]: %v", start, end, err)
		}
	}
	if _, err := db.ExecContext(ctx, `ANALYZE bench_acm_resources`); err != nil {
		t.Fatalf("analyze bench_acm_resources: %v", err)
	}
}

// acmUpsertSQL is ACM search-indexer's per-resource write, verbatim
// per acm_search_indexing.md section 3.2: a plain change-guarded
// upsert against a single (uid, cluster, data) table -- no
// natural-key resolution, no normalized label/condition tables, no
// history, no aliases. Pipelined N times per batch via pgx.Batch
// rather than folded into one statement (see acmBatchUpsert).
const acmUpsertSQL = `
INSERT INTO bench_acm_resources AS r (uid, cluster, data) VALUES ($1, $2, $3::jsonb)
ON CONFLICT (uid) DO UPDATE SET data = $3::jsonb
WHERE r.data IS DISTINCT FROM $3::jsonb`

// acmBatchUpsert pipelines the whole batch as one round trip using
// native pgx (via sql.Conn.Raw to reach the underlying *pgx.Conn),
// per acm_search_indexing.md section 3.4/3.8: a pgx.Batch sent via
// SendBatch executes as a single implicit transaction, the same
// "one commit per chunk" shape ReplaceInventory's single CTE
// statement has -- so timeACMUpsert's per-call wall-clock is directly
// comparable to timeReplace's, with no explicit BEGIN/COMMIT needed
// on either side.
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

// explainACMUpsert runs EXPLAIN (ANALYZE, BUFFERS) against a single
// acmUpsertSQL call -- the batch's plan doesn't depend on batch size
// at all (it's N independent copies of this same per-row statement,
// unlike replaceInventorySQL's one statement over the whole batch),
// so one row is all EXPLAIN needs to show.
func explainACMUpsert(t *testing.T, db *sql.DB, uid, cluster, data string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "EXPLAIN (ANALYZE, BUFFERS) "+acmUpsertSQL, uid, cluster, data)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("explain scan: %v", err)
		}
		t.Log(line)
	}
}

// ---------------------------------------------------------------------------
// Scenario builders -- one per named scenario in
// TestInventoryWritePathBenchmark, each returning a fresh batch for a
// given round so IS DISTINCT FROM/fingerprint short-circuits are
// exercised the same way a real repeated call would, not accidentally
// bypassed by reusing identical Go values across rounds.
// ---------------------------------------------------------------------------

// ipbCycle returns n indices starting at pool start+ (pos advanced by
// n), wrapping back to the start of [poolStart, poolStart+poolSize)
// once exhausted. Every steady-state scenario below folds the calling
// round's generation number into whatever it reports, so a wrapped
// revisit is always a genuine change from the previous visit, never
// an accidental no-op repeat.
func ipbCycle(poolStart, poolSize int, pos *int, n int) []int {
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = poolStart + (*pos+i)%poolSize
	}
	*pos += n
	return out
}

type ipbScenarioState struct {
	noAliasPos      int
	sameAliasPos    int
	changedAliasPos int
	newPos          int

	acmUpdatePos int
	acmNewPos    int
}

func (s *ipbScenarioState) nextNew(n int) []int {
	out := make([]int, n)
	for i := 0; i < n; i++ {
		out[i] = ipbNewStart + s.newPos + i
	}
	s.newPos += n
	return out
}

func ipbBaseReplacement(idx int, gen int64) domain.InventoryReplacement {
	obs := ipbObservationJSON(gen)
	return domain.InventoryReplacement{
		ResourceType: ipbResourceType,
		Name:         ipbResourceName(idx),
		CandidateUID: domain.NewExtensionResourceUID(),
		Labels:       ipbLabels(idx, gen),
		Observation:  &obs,
		Conditions:   ipbConditions(fixedIPBTime, idx, gen),
		ObservedAt:   fixedIPBTime,
		ReceivedAt:   fixedIPBTime,
	}
}

// buildNewNoAlias reports n never-before-seen resources with no
// aliases: the pure onboarding/import case, always taking
// replaceInventorySQL's resolved_er INSERT branch.
func (s *ipbScenarioState) buildNewNoAlias(round, n int) []domain.InventoryReplacement {
	indices := s.nextNew(n)
	reps := make([]domain.InventoryReplacement, n)
	for i, idx := range indices {
		reps[i] = ipbBaseReplacement(idx, int64(round))
	}
	return reps
}

// buildNewWithAlias is buildNewNoAlias's alias-bearing counterpart:
// every resource is new *and* reports one alias -- still a single
// INSERT per resource (resolved_er's own INSERT already carries the
// alias payload; see replaceInventorySQL's doc comment on why
// needs_alias_payload_write excludes these rows).
func (s *ipbScenarioState) buildNewWithAlias(round, n int) []domain.InventoryReplacement {
	indices := s.nextNew(n)
	reps := make([]domain.InventoryReplacement, n)
	for i, idx := range indices {
		rep := ipbBaseReplacement(idx, int64(round))
		alias, _ := domain.NewAlias(ipbAliasNamespace, ipbAliasKey, ipbAliasValue(idx, int64(round)))
		rep.Aliases = []domain.Alias{alias}
		reps[i] = rep
	}
	return reps
}

// buildSteadyNoAlias reports on already-existing, never-aliased
// resources with changed observation/labels/conditions each round --
// the common "heartbeat with real data drift" case. Every call hits
// replaceInventorySQL's UPDATE branch for extension_resource_inventory,
// and skips the alias payload write entirely (empty fingerprint always
// matches empty fingerprint).
func (s *ipbScenarioState) buildSteadyNoAlias(round, n int) []domain.InventoryReplacement {
	indices := ipbCycle(ipbNoAliasStart, ipbPoolSize, &s.noAliasPos, n)
	reps := make([]domain.InventoryReplacement, n)
	for i, idx := range indices {
		reps[i] = ipbBaseReplacement(idx, int64(round+1))
	}
	return reps
}

// buildSteadySameAlias re-reports the exact same alias value every
// round for already-existing resources -- the case
// needs_alias_payload_write's fingerprint comparison exists to make
// cheap: the alias UPDATE is skipped entirely once the fingerprint
// matches (i.e. from round 2 onward; round 1's seeding already wrote
// it once).
func (s *ipbScenarioState) buildSteadySameAlias(round, n int) []domain.InventoryReplacement {
	indices := ipbCycle(ipbSameAliasStart, ipbPoolSize, &s.sameAliasPos, n)
	reps := make([]domain.InventoryReplacement, n)
	for i, idx := range indices {
		rep := ipbBaseReplacement(idx, int64(round+1))
		alias, _ := domain.NewAlias(ipbAliasNamespace, ipbAliasKey, ipbSteadyAliasValue(idx))
		rep.Aliases = []domain.Alias{alias}
		reps[i] = rep
	}
	return reps
}

// buildSteadyChangedAlias reports a *different* alias value every
// round for already-existing resources -- the worst case for
// needs_alias_payload_write, which must perform the UPDATE on every
// single call since the fingerprint never matches.
func (s *ipbScenarioState) buildSteadyChangedAlias(round, n int) []domain.InventoryReplacement {
	indices := ipbCycle(ipbChangedAliasStart, ipbPoolSize, &s.changedAliasPos, n)
	reps := make([]domain.InventoryReplacement, n)
	for i, idx := range indices {
		rep := ipbBaseReplacement(idx, int64(round+1))
		alias, _ := domain.NewAlias(ipbAliasNamespace, ipbAliasKey, ipbAliasValue(idx, int64(round+1)))
		rep.Aliases = []domain.Alias{alias}
		reps[i] = rep
	}
	return reps
}

// buildSteadyHeartbeatDelta is buildSteadyNoAlias's ApplyInventoryDeltas
// analog: a pure observation-only heartbeat (no label/condition/alias
// change at all), the cheapest realistic delta shape and the one most
// heartbeat traffic in a real fleet would actually send.
func (s *ipbScenarioState) buildSteadyHeartbeatDelta(round, n int) []domain.InventoryDelta {
	indices := ipbCycle(ipbNoAliasStart, ipbPoolSize, &s.noAliasPos, n)
	deltas := make([]domain.InventoryDelta, n)
	for i, idx := range indices {
		obs := ipbObservationJSON(int64(round + 1))
		deltas[i] = domain.InventoryDelta{
			ResourceType: ipbResourceType,
			Name:         ipbResourceName(idx),
			CandidateUID: domain.NewExtensionResourceUID(),
			Observation:  &obs,
			ObservedAt:   fixedIPBTime,
			ReceivedAt:   fixedIPBTime,
		}
	}
	return deltas
}

// buildACMUpdateExisting is the ACM-model counterpart of
// buildSteadyNoAlias: cycles through the *entire* seeded
// bench_acm_resources corpus (ACM has no alias concept at all, so
// there's only one steady-state pool, not three) reporting a
// genuinely changed `data` payload every round -- the case
// acmUpsertSQL's IS DISTINCT FROM guard cannot skip.
func (s *ipbScenarioState) buildACMUpdateExisting(round, n int) (uids, clusters, datas []string) {
	indices := ipbCycle(0, ipbCorpusSize, &s.acmUpdatePos, n)
	uids = make([]string, n)
	clusters = make([]string, n)
	datas = make([]string, n)
	for i, idx := range indices {
		uids[i] = ipbACMUID(idx)
		clusters[i] = ipbClusterOf(idx)
		datas[i] = string(ipbACMDataJSON(int64(round+1), idx))
	}
	return uids, clusters, datas
}

// buildACMInsertNew is buildNewNoAlias's ACM-model counterpart: always
// a never-before-seen uid, so acmUpsertSQL always takes its INSERT
// branch.
func (s *ipbScenarioState) buildACMInsertNew(round, n int) (uids, clusters, datas []string) {
	base := s.acmNewPos
	s.acmNewPos += n
	uids = make([]string, n)
	clusters = make([]string, n)
	datas = make([]string, n)
	for i := 0; i < n; i++ {
		idx := ipbNewStart + base + i
		uids[i] = ipbACMUID(idx)
		clusters[i] = ipbClusterOf(idx)
		datas[i] = string(ipbACMDataJSON(int64(round), idx))
	}
	return uids, clusters, datas
}

// ---------------------------------------------------------------------------
// Timing
// ---------------------------------------------------------------------------

type ipbTimings struct {
	scenario  string
	batchSize int
	durations []time.Duration
}

// stats returns mean/p50/p95/min/max across r.durations.
func (r ipbTimings) stats() (mean, p50, p95, minD, maxD time.Duration) {
	sorted := append([]time.Duration(nil), r.durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var sum time.Duration
	for _, d := range sorted {
		sum += d
	}
	mean = sum / time.Duration(len(sorted))
	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[min(len(sorted)*95/100, len(sorted)-1)]
	return mean, p50, p95, sorted[0], sorted[len(sorted)-1]
}

// perThousand normalizes a duration (typically one of stats's return
// values) to a "per 1000 items" rate, the unit
// printAliasCostBreakdown/this type's own String compare across
// different batch sizes.
func (r ipbTimings) perThousand(d time.Duration) time.Duration {
	return d * 1000 / time.Duration(r.batchSize)
}

func (r ipbTimings) String() string {
	mean, p50, p95, minD, maxD := r.stats()
	return fmt.Sprintf("batch=%-5d  n=%-2d  mean=%-10s  p50=%-10s  p95=%-10s  min=%-10s  max=%-10s  (%s/1000 items)",
		r.batchSize, len(r.durations), mean, p50, p95, minD, maxD, r.perThousand(mean))
}

// printAliasCostBreakdown isolates the marginal cost of aliases
// within our own design -- the only meaningful axis, since the ACM
// baseline has no alias/identity concept at all to compare against
// (see this file's package doc comment). Compares p50, not mean:
// mean is what String reports for headline numbers, but a handful of
// slow rounds (container checkpoint/vacuum activity) skews a small
// batch size's mean far more than its median -- see ipbRounds's doc
// comment for why this mattered enough to bump rounds up. base and
// variant must share the same set of batch-size keys (both drawn
// from ipbBatchSizes here).
func printAliasCostBreakdown(t *testing.T, label string, base, variant map[int]ipbTimings) {
	t.Helper()
	t.Log(label)
	for _, bs := range ipbBatchSizes {
		_, baseP50, _, _, _ := base[bs].stats()
		_, varP50, _, _, _ := variant[bs].stats()
		basePerK := base[bs].perThousand(baseP50)
		varPerK := variant[bs].perThousand(varP50)
		pct := 100 * (float64(varPerK) - float64(basePerK)) / float64(basePerK)
		t.Logf("  batch=%-5d  baseline(p50)=%-10s  with-alias(p50)=%-10s  overhead=%+.1f%%",
			bs, basePerK, varPerK, pct)
	}
}

// timeReplace runs build/ReplaceInventory once per round inside its
// own transaction (mirroring InventoryReportService.ReplaceBatch's
// one-transaction-per-chunk shape), timing the whole
// begin/exec/commit span.
func timeReplace(t *testing.T, db *sql.DB, scenario string, batchSize int, build func(round, n int) []domain.InventoryReplacement) ipbTimings {
	t.Helper()
	ctx := context.Background()
	durs := make([]time.Duration, ipbRounds)
	for round := 0; round < ipbRounds; round++ {
		reps := build(round, batchSize)
		start := time.Now()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		repo := &ExtensionResourceRepo{DB: tx}
		if err := repo.ReplaceInventory(ctx, reps); err != nil {
			tx.Rollback()
			t.Fatalf("%s round %d: %v", scenario, round, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		durs[round] = time.Since(start)
	}
	return ipbTimings{scenario: scenario, batchSize: batchSize, durations: durs}
}

func timeApplyDeltas(t *testing.T, db *sql.DB, scenario string, batchSize int, build func(round, n int) []domain.InventoryDelta) ipbTimings {
	t.Helper()
	ctx := context.Background()
	durs := make([]time.Duration, ipbRounds)
	for round := 0; round < ipbRounds; round++ {
		deltas := build(round, batchSize)
		start := time.Now()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatalf("begin: %v", err)
		}
		repo := &ExtensionResourceRepo{DB: tx}
		if err := repo.ApplyInventoryDeltas(ctx, deltas); err != nil {
			tx.Rollback()
			t.Fatalf("%s round %d: %v", scenario, round, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		durs[round] = time.Since(start)
	}
	return ipbTimings{scenario: scenario, batchSize: batchSize, durations: durs}
}

// timeACMUpsert times acmBatchUpsert the same way timeReplace times
// ReplaceInventory: one round trip (pgx.Batch's implicit transaction)
// per round, wall-clock only.
func timeACMUpsert(t *testing.T, db *sql.DB, scenario string, batchSize int, build func(round, n int) (uids, clusters, datas []string)) ipbTimings {
	t.Helper()
	ctx := context.Background()
	durs := make([]time.Duration, ipbRounds)
	for round := 0; round < ipbRounds; round++ {
		uids, clusters, datas := build(round, batchSize)
		start := time.Now()
		if err := acmBatchUpsert(ctx, db, uids, clusters, datas); err != nil {
			t.Fatalf("%s round %d: %v", scenario, round, err)
		}
		durs[round] = time.Since(start)
	}
	return ipbTimings{scenario: scenario, batchSize: batchSize, durations: durs}
}

// ---------------------------------------------------------------------------
// Main benchmark
// ---------------------------------------------------------------------------

func TestInventoryWritePathBenchmark(t *testing.T) {
	if os.Getenv("FLEETSHIFT_INVENTORY_BENCH") == "" {
		t.Skip("set FLEETSHIFT_INVENTORY_BENCH=1 to run (spins up a dedicated Postgres container and seeds a ~100k-resource corpus)")
	}

	db := openBenchDB(t)
	seedIPBType(t, db)

	t.Log("seeding corpus...")
	seedStart := time.Now()
	seedIPBCorpus(t, db)
	t.Logf("seeded %d resources in %s", ipbCorpusSize, time.Since(seedStart))
	seedACMStart := time.Now()
	seedIPBACMTable(t, db)
	t.Logf("seeded %d ACM baseline rows in %s", ipbCorpusSize, time.Since(seedACMStart))
	ipbTableSizes(t, db)

	state := &ipbScenarioState{}

	replaceScenarios := []struct {
		name  string
		build func(round, n int) []domain.InventoryReplacement
	}{
		{"new/no-alias (onboarding)", state.buildNewNoAlias},
		{"new/with-alias (onboarding)", state.buildNewWithAlias},
		{"steady/no-alias (heartbeat, data drift)", state.buildSteadyNoAlias},
		{"steady/same-alias (fingerprint skip)", state.buildSteadySameAlias},
		{"steady/changed-alias (payload rewrite every call)", state.buildSteadyChangedAlias},
	}

	t.Log("")
	t.Log("=== ReplaceInventory ===")
	results := make(map[string]map[int]ipbTimings, len(replaceScenarios))
	for _, sc := range replaceScenarios {
		perBatch := make(map[int]ipbTimings, len(ipbBatchSizes))
		t.Run(sc.name, func(t *testing.T) {
			for _, bs := range ipbBatchSizes {
				timing := timeReplace(t, db, sc.name, bs, sc.build)
				t.Log(timing.String())
				perBatch[bs] = timing
			}
		})
		results[sc.name] = perBatch
	}

	t.Log("")
	t.Log("=== Alias cost breakdown (marginal cost within our own design; ACM has no alias concept to compare against) ===")
	printAliasCostBreakdown(t, "steady/same-alias (fingerprint skip)  vs  steady/no-alias",
		results["steady/no-alias (heartbeat, data drift)"], results["steady/same-alias (fingerprint skip)"])
	printAliasCostBreakdown(t, "steady/changed-alias (payload rewrite every call)  vs  steady/no-alias",
		results["steady/no-alias (heartbeat, data drift)"], results["steady/changed-alias (payload rewrite every call)"])
	printAliasCostBreakdown(t, "new/with-alias (onboarding)  vs  new/no-alias (onboarding)",
		results["new/no-alias (onboarding)"], results["new/with-alias (onboarding)"])

	t.Log("")
	t.Log("=== ApplyInventoryDeltas ===")
	t.Run("steady/heartbeat-delta (observation only)", func(t *testing.T) {
		for _, bs := range ipbBatchSizes {
			timing := timeApplyDeltas(t, db, "steady/heartbeat-delta", bs, state.buildSteadyHeartbeatDelta)
			t.Log(timing.String())
		}
	})

	// ACM baseline: buildACMUpdateExisting/buildACMInsertNew are the
	// direct counterparts of buildSteadyNoAlias/buildNewNoAlias above
	// -- same corpus scale, same batch sizes, same "genuinely changed
	// payload every call" semantics -- just against ACM's single flat
	// table with 7 indexes, pipelined via pgx.Batch instead of one
	// CTE statement. Compare this section's numbers directly against
	// "steady/no-alias" and "new/no-alias" above at the same batch
	// size.
	t.Log("")
	t.Log("=== ACM-style baseline (single-table upsert, pgx.Batch pipelined) ===")
	t.Run("ACM/update-existing", func(t *testing.T) {
		for _, bs := range ipbBatchSizes {
			timing := timeACMUpsert(t, db, "ACM/update-existing", bs, state.buildACMUpdateExisting)
			t.Log(timing.String())
		}
	})
	t.Run("ACM/insert-new", func(t *testing.T) {
		for _, bs := range ipbBatchSizes {
			timing := timeACMUpsert(t, db, "ACM/insert-new", bs, state.buildACMInsertNew)
			t.Log(timing.String())
		}
	})

	// EXPLAIN capture for the shapes that matter most: a pure-update
	// heartbeat (the dominant steady-state traffic pattern) and a
	// pure-insert onboarding batch, both at InventoryReportService's
	// real default chunk size, plus ACM's single-row upsert (its plan
	// is batch-size-independent -- see explainACMUpsert's doc).
	t.Log("")
	t.Log("=== EXPLAIN (ANALYZE, BUFFERS) at batch=1000 ===")
	t.Run("EXPLAIN/steady-no-alias", func(t *testing.T) {
		explainReplaceInventory(t, db, state.buildSteadyNoAlias(1000, 1000))
	})
	t.Run("EXPLAIN/new-no-alias", func(t *testing.T) {
		explainReplaceInventory(t, db, state.buildNewNoAlias(1000, 1000))
	})
	t.Run("EXPLAIN/ACM-update-existing", func(t *testing.T) {
		uids, clusters, datas := state.buildACMUpdateExisting(1000, 1)
		explainACMUpsert(t, db, uids[0], clusters[0], datas[0])
	})
	t.Run("EXPLAIN/ACM-insert-new", func(t *testing.T) {
		uids, clusters, datas := state.buildACMInsertNew(1000, 1)
		explainACMUpsert(t, db, uids[0], clusters[0], datas[0])
	})
}

// explainReplaceInventory runs EXPLAIN (ANALYZE, BUFFERS) directly
// against replaceInventorySQL with the same argument-building logic
// [ExtensionResourceRepo.ReplaceInventory] uses, and logs the plan.
// This genuinely executes (and commits) the write -- EXPLAIN ANALYZE
// always does -- so callers should treat reps as consumed, not just
// inspected.
func explainReplaceInventory(t *testing.T, db *sql.DB, reps []domain.InventoryReplacement) {
	t.Helper()
	n := len(reps)
	idx := make([]int32, n)
	serviceNames := make([]string, n)
	typeNames := make([]string, n)
	collectionNames := make([]string, n)
	resourceIDs := make([]string, n)
	candidateUIDs := make([]string, n)
	observations := make([]*string, n)
	labels := make([]string, n)
	conditions := make([]string, n)
	observedAts := make([]time.Time, n)
	receivedAts := make([]time.Time, n)
	aliasFingerprints := make([][]byte, n)
	reportedAliases := make([]string, n)

	for i, rep := range reps {
		idx[i] = int32(i)
		serviceNames[i] = string(rep.ResourceType.ServiceName())
		typeNames[i] = rep.ResourceType.TypeName()
		collectionNames[i] = string(rep.Name.Collection())
		resourceIDs[i] = string(rep.Name.ID())
		candidateUIDs[i] = rep.CandidateUID.String()
		if obs := normalizeObservation(rep.Observation); obs != nil {
			s := string(*obs)
			observations[i] = &s
		}
		labelsJSON, _ := json.Marshal(nonNilLabels(rep.Labels))
		labels[i] = string(labelsJSON)
		conditionsJSON, _ := conditionsToJSON(rep.Conditions)
		conditions[i] = string(conditionsJSON)
		observedAts[i] = rep.ObservedAt.UTC()
		receivedAts[i] = rep.ReceivedAt.UTC()
		aliasFingerprints[i] = domain.AliasSetFingerprint(rep.Aliases)
		aliasesJSON, _ := domain.CanonicalAliasPayload(rep.Aliases)
		reportedAliases[i] = string(aliasesJSON)
	}

	rows, err := db.QueryContext(context.Background(), "EXPLAIN (ANALYZE, BUFFERS) "+replaceInventorySQL,
		idx, serviceNames, typeNames, collectionNames, resourceIDs, candidateUIDs,
		observations, labels, conditions, observedAts, receivedAts,
		aliasFingerprints, reportedAliases,
	)
	if err != nil {
		t.Fatalf("explain: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("explain scan: %v", err)
		}
		t.Log(line)
	}
}

// openBenchDB opens a fresh database on the shared benchmark-only
// Postgres container (see testdb.go's benchContainerOnce doc comment
// for why this is a separate container from OpenTestDB's), migrated
// and ready to use. Reuses OpenTestDB's counter/mutex for database
// naming since both draw from the same "test_N"/"bench_N" numbering
// concern -- collisions are impossible either way since the names
// carry different prefixes.
func openBenchDB(t *testing.T) *sql.DB {
	t.Helper()
	benchContainerOnce.Do(func() {
		benchContainerCtr, benchContainerConn, benchContainerErr = startBenchContainer()
	})
	if benchContainerErr != nil {
		t.Fatalf("bench postgres container: %v", benchContainerErr)
	}

	adminDB, err := sql.Open("pgx", benchContainerConn)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer adminDB.Close()

	testDBMu.Lock()
	testDBCounter++
	dbName := fmt.Sprintf("bench_%d", testDBCounter)
	testDBMu.Unlock()

	if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatalf("create bench database: %v", err)
	}

	db, err := Open(replaceDBName(benchContainerConn, dbName))
	if err != nil {
		t.Fatalf("open bench db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
