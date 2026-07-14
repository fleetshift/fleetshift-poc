package postgres

// This file gets an EXPLAIN (ANALYZE, BUFFERS) plan for
// buildQueryResourcesSQL (see query_sql.go) against a
// realistic-scale corpus, so query_sql.go's indexing notes are
// backed by an actual plan rather than a guess.
//
// Platform rows are still seeded for comparison only -- QueryResources
// itself ignores them in this extension-only iteration.
//
// It follows inventory_bench_test.go's own EXPLAIN-driven
// investigation pattern: a dedicated bench container/corpus (see
// openBenchDB), skipped by default, with bulk raw-SQL seeding rather
// than routing every row through the full domain/repository API.
// Unlike inventory_bench_test.go's write-path corpus (which seeds
// through ReplaceInventory specifically to measure that path's own
// cost), what matters here is just realistic row counts, key
// cardinalities, and JSONB payload shapes for the *planner* to reason
// about -- so raw bulk INSERT ... SELECT * FROM UNNEST(...) (the same
// technique seedIPBACMTable already uses in this file's package for
// its ACM baseline table) is both sufficient and far faster at this
// scale.
//
// Run with:
//
//	FLEETSHIFT_QUERY_BENCH=1 go test ./internal/infrastructure/postgres/ -run 'TestQueryResources(ExplainPlan|Benchmark)$' -v -timeout 10m
//
// TestQueryResourcesExplainPlan logs one-shot EXPLAIN (ANALYZE,
// BUFFERS) plans. TestQueryResourcesBenchmark times the real
// QueryRepo.QueryResources path over repeated rounds (mean/p50/p95)
// against the same corpus. Both cover selective filters plus
// worst-case non-matching spec/observation paths and timestamp()
// conversion. Both are skipped by default -- they spin up a
// dedicated Postgres container and seed a multi-tens-of-thousands-row
// corpus, so they're too slow for the default `go test ./...` loop.

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// ---------------------------------------------------------------------------
// Corpus shape
// ---------------------------------------------------------------------------

const (
	qrbClusterService    = "kind.fleetshift.io"
	qrbClusterType       = "Cluster"
	qrbClusterCollection = "clusters"

	qrbNodeService    = "kubernetes.fleetshift.io"
	qrbNodeType       = "Node"
	qrbNodeCollection = "nodes"

	// qrbPlatformOnlyCollection holds physical-only platform
	// resources -- no extension resource ever shares this
	// collection. Seeded only so we can confirm QueryResources
	// ignores them; the query itself never reads platform_resources.
	qrbPlatformOnlyCollection = "assets"

	// qrbClusterCount/qrbNodeCount/qrbPlatformOnlyCount pick a scale
	// an order of magnitude below inventory_bench_test.go's ~100k
	// (this investigation only needs the planner to see "not
	// trivially small", not a production-scale corpus) while still
	// being large enough that a full table scan is visibly costly in
	// EXPLAIN ANALYZE's actual timings, not just its row-count
	// estimate.
	qrbClusterCount      = 20_000
	qrbNodeCount         = 20_000
	qrbPlatformOnlyCount = 5_000

	qrbChunk = 5000

	// qrbRounds is how many timed QueryResources calls each scenario
	// makes; timings report mean/p50/p95 across these. Matches the
	// inventory write-path bench's rationale for a double-digit
	// sample (container noise skews a single EXPLAIN ANALYZE more
	// than a median over many rounds).
	qrbRounds = 15

	// qrbWarmupRounds discards cold-cache / first-plan cost before
	// the timed sample.
	qrbWarmupRounds = 2
)

var qrbFixedTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

// ---------------------------------------------------------------------------
// Seeding
// ---------------------------------------------------------------------------

func seedQRBTypes(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO extension_resource_types (service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at)
		VALUES
			($1, $2, 'v1', $3, '{}'::jsonb, NULL, $4, $4),
			($5, $6, 'v1', $7, NULL, '{}'::jsonb, $4, $4)
	`, qrbClusterService, qrbClusterType, qrbClusterCollection, qrbFixedTime,
		qrbNodeService, qrbNodeType, qrbNodeCollection); err != nil {
		t.Fatalf("seed extension resource types: %v", err)
	}
}

// seedQRBPlatformOnly bulk-inserts qrbPlatformOnlyCount physical
// platform_resources rows with no corresponding extension resource,
// alternating env=prod/dev labels.
func seedQRBPlatformOnly(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	for start := 0; start < qrbPlatformOnlyCount; start += qrbChunk {
		n := min(qrbChunk, qrbPlatformOnlyCount-start)
		resourceIDs := make([]string, n)
		labels := make([]string, n)
		createdAts := make([]time.Time, n)
		for i := 0; i < n; i++ {
			idx := start + i
			resourceIDs[i] = fmt.Sprintf("asset-%08d", idx)
			env := "prod"
			if idx%2 == 0 {
				env = "dev"
			}
			labels[i] = fmt.Sprintf(`{"env":%q}`, env)
			createdAts[i] = qrbFixedTime
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO platform_resources (collection_name, resource_id, labels, created_at, updated_at)
			SELECT $1, r, l::jsonb, c, c
			FROM UNNEST($2::text[], $3::jsonb[], $4::timestamptz[]) AS t(r, l, c)
		`, qrbPlatformOnlyCollection, resourceIDs, labels, createdAts); err != nil {
			t.Fatalf("seed platform-only [%d:%d]: %v", start, start+n, err)
		}
	}
}

// seedQRBClusters bulk-inserts qrbClusterCount managed extension
// resources (kind.fleetshift.io/Cluster): extension_resources plus
// the extension_resource_managed/resource_intents/fulfillments rows
// query_sql.go's filtered_page CTE joins against, so
// resource.spec.*/resource.state-shaped filters have real rows to
// match.
func seedQRBClusters(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	teams := []string{"platform", "apps", "data"}
	providers := []string{"aws", "gcp", "azure"}
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1"}
	createdAtText := qrbFixedTime.Format(time.RFC3339)

	for start := 0; start < qrbClusterCount; start += qrbChunk {
		n := min(qrbChunk, qrbClusterCount-start)
		uids := make([]string, n)
		resourceIDs := make([]string, n)
		labels := make([]string, n)
		specs := make([]string, n)
		fulfillmentIDs := make([]string, n)
		createdAts := make([]time.Time, n)
		for i := 0; i < n; i++ {
			idx := start + i
			uids[i] = domain.NewExtensionResourceUID().String()
			resourceIDs[i] = fmt.Sprintf("cluster-%08d", idx)
			labels[i] = fmt.Sprintf(`{"team":%q}`, teams[idx%len(teams)])
			specs[i] = fmt.Sprintf(`{"provider":%q,"region":%q}`, providers[idx%len(providers)], regions[idx%len(regions)])
			fulfillmentIDs[i] = fmt.Sprintf("qrb-fulfillment-%08d", idx)
			createdAts[i] = qrbFixedTime
		}

		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, reported_aliases, created_at, updated_at)
			SELECT u, $1, $2, $3, r, l::jsonb, '{}'::jsonb, c, c
			FROM UNNEST($4::uuid[], $5::text[], $6::jsonb[], $7::timestamptz[]) AS t(u, r, l, c)
		`, qrbClusterService, qrbClusterType, qrbClusterCollection, uids, resourceIDs, labels, createdAts); err != nil {
			t.Fatalf("seed cluster extension_resources [%d:%d]: %v", start, start+n, err)
		}

		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resource_managed (extension_resource_uid, current_version, fulfillment_id)
			SELECT u, 1, f FROM UNNEST($1::uuid[], $2::text[]) AS t(u, f)
		`, uids, fulfillmentIDs); err != nil {
			t.Fatalf("seed extension_resource_managed [%d:%d]: %v", start, start+n, err)
		}

		if _, err := db.ExecContext(ctx, `
			INSERT INTO resource_intents (extension_resource_uid, version, spec, created_at)
			SELECT u, 1, s::jsonb, $1 FROM UNNEST($2::uuid[], $3::jsonb[]) AS t(u, s)
		`, createdAtText, uids, specs); err != nil {
			t.Fatalf("seed resource_intents [%d:%d]: %v", start, start+n, err)
		}

		if _, err := db.ExecContext(ctx, `
			INSERT INTO fulfillments (id, state, created_at, updated_at)
			SELECT f, 'active', $1, $1 FROM UNNEST($2::text[]) AS t(f)
		`, createdAtText, fulfillmentIDs); err != nil {
			t.Fatalf("seed fulfillments [%d:%d]: %v", start, start+n, err)
		}
	}
}

// seedQRBNodes bulk-inserts qrbNodeCount inventory-only extension
// resources (kubernetes.fleetshift.io/Node): extension_resources plus
// extension_resource_inventory, with a numeric capacity.cpu
// observation spread evenly across [1,64] so a `> 32` filter is
// roughly 50% selective -- a realistic mid-selectivity numeric JSON
// filter, not a trivially-cheap or trivially-empty one.
func seedQRBNodes(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	roles := []string{"worker", "control-plane"}

	for start := 0; start < qrbNodeCount; start += qrbChunk {
		n := min(qrbChunk, qrbNodeCount-start)
		uids := make([]string, n)
		resourceIDs := make([]string, n)
		labels := make([]string, n)
		invLabels := make([]string, n)
		observations := make([]string, n)
		conditions := make([]string, n)
		observedAts := make([]time.Time, n)
		for i := 0; i < n; i++ {
			idx := start + i
			uids[i] = domain.NewExtensionResourceUID().String()
			resourceIDs[i] = fmt.Sprintf("node-%08d", idx)
			labels[i] = "{}"
			role := roles[idx%len(roles)]
			invLabels[i] = fmt.Sprintf(`{"node-role":%q}`, role)
			cpu := idx%64 + 1
			observations[i] = fmt.Sprintf(`{"capacity":{"cpu":%d},"allocatable":{"cpu":%d}}`, cpu, max(cpu-2, 1))
			ready := "True"
			if idx%20 == 0 {
				ready = "False"
			}
			conditions[i] = fmt.Sprintf(
				`{"Ready":{"status":%q,"reason":"Probe","message":"steady","lastTransitionTime":"2026-06-01T12:00:00Z","_lastTransitionTimeNorm":"2026-06-01T12:00:00.000000000Z"}}`, ready)
			observedAts[i] = qrbFixedTime
		}

		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, reported_aliases, created_at, updated_at)
			SELECT u, $1, $2, $3, r, l::jsonb, '{}'::jsonb, c, c
			FROM UNNEST($4::uuid[], $5::text[], $6::jsonb[], $7::timestamptz[]) AS t(u, r, l, c)
		`, qrbNodeService, qrbNodeType, qrbNodeCollection, uids, resourceIDs, labels, observedAts); err != nil {
			t.Fatalf("seed node extension_resources [%d:%d]: %v", start, start+n, err)
		}

		if _, err := db.ExecContext(ctx, `
			INSERT INTO extension_resource_inventory (extension_resource_uid, observation, labels, conditions, observed_at, updated_at)
			SELECT u, o::jsonb, l::jsonb, c::jsonb, t, t
			FROM UNNEST($1::uuid[], $2::jsonb[], $3::jsonb[], $4::jsonb[], $5::timestamptz[]) AS x(u, o, l, c, t)
		`, uids, observations, invLabels, conditions, observedAts); err != nil {
			t.Fatalf("seed extension_resource_inventory [%d:%d]: %v", start, start+n, err)
		}
	}
}

// seedQRBAliasesAndRelationships gives every qrbPlatformOnlyCount
// asset one alias claim and one outgoing relationship, so
// platformResourceAggregateSelectPostgres's three LATERAL sub-joins
// (reps/aliases/relationships -- see resource_identity_repo.go) have
// non-trivial, non-empty tables to join against when a query page
// hydrates platform rows.
func seedQRBAliasesAndRelationships(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	n := qrbPlatformOnlyCount
	resourceIDs := make([]string, n)
	values := make([]string, n)
	targets := make([]string, n)
	createdAts := make([]time.Time, n)
	for i := 0; i < n; i++ {
		resourceIDs[i] = fmt.Sprintf("asset-%08d", i)
		values[i] = fmt.Sprintf("ext-%08d", i)
		targets[i] = fmt.Sprintf("asset-%08d", (i+1)%n)
		createdAts[i] = qrbFixedTime
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id, platform_owned, created_at)
		SELECT 'ext-id', 'source-id', v, $1, r, true, c
		FROM UNNEST($2::text[], $3::text[], $4::timestamptz[]) AS t(r, v, c)
	`, qrbPlatformOnlyCollection, resourceIDs, values, createdAts); err != nil {
		t.Fatalf("seed resource_alias_claims: %v", err)
	}

	if _, err := db.ExecContext(ctx, `
		INSERT INTO resource_relationships (source_collection_name, source_resource_id, type, target_collection_name, target_resource_id, source_service, created_at)
		SELECT $1, r, 'depends-on', $1, tgt, 'bench.fleetshift.io', c
		FROM UNNEST($2::text[], $3::text[], $4::timestamptz[]) AS t(r, tgt, c)
	`, qrbPlatformOnlyCollection, resourceIDs, targets, createdAts); err != nil {
		t.Fatalf("seed resource_relationships: %v", err)
	}
}

func qrbTableSizes(t *testing.T, db *sql.DB) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `
		SELECT relname, reltuples::bigint, pg_size_pretty(pg_total_relation_size(oid))
		FROM pg_class
		WHERE relname IN (
			'platform_resources', 'extension_resources', 'extension_resource_types',
			'extension_resource_managed', 'resource_intents', 'fulfillments',
			'extension_resource_inventory', 'resource_alias_claims', 'resource_relationships'
		)
		ORDER BY relname`)
	if err != nil {
		t.Logf("table size query failed (non-fatal): %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var estRows int64
		var size string
		if err := rows.Scan(&name, &estRows, &size); err != nil {
			continue
		}
		t.Logf("  %-32s ~%-8d %s", name, estRows, size)
	}
}

// ---------------------------------------------------------------------------
// EXPLAIN driver
// ---------------------------------------------------------------------------

// explainQueryResources compiles filter through the real
// querysql.Compiler/queryFieldResolver pair -- exactly what
// QueryRepo.QueryResources runs (see query_repo.go) -- builds the
// same buildQueryResourcesSQL page query, and logs its EXPLAIN
// (ANALYZE, BUFFERS) plan. keysetTok, if non-nil, exercises the
// second-page keyset predicate the same way a real PageToken would;
// the exact values don't need to correspond to a real first page for
// planning purposes, only their types matter.
func explainQueryResources(t *testing.T, db *sql.DB, label, filter, orderBy string, pageSize int, keysetTok *queryPageToken) {
	t.Helper()
	order, err := resolveQueryOrder(orderBy)
	if err != nil {
		t.Fatalf("%s: resolve order %q: %v", label, orderBy, err)
	}
	compiler := querysql.Compiler{Fields: queryFieldResolver{}, Params: querysql.DollarParams{}}
	predicate, err := compiler.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter})
	if err != nil {
		t.Fatalf("%s: compile filter %q: %v", label, filter, err)
	}
	args := append([]any{}, predicate.Args...)

	keysetSQL := "TRUE"
	if keysetTok != nil {
		keysetSQL, args = keysetPredicateSQL(order, *keysetTok, args)
	}

	limitPlaceholder := len(args) + 1
	args = append(args, pageSize+1)

	query := buildQueryResourcesSQL(predicate.SQL, keysetSQL, order, limitPlaceholder)

	t.Logf("=== %s ===", label)
	t.Logf("filter: %q  order_by: %q  page_size: %d  keyset: %v", filter, orderBy, pageSize, keysetTok != nil)
	rows, err := db.QueryContext(context.Background(), "EXPLAIN (ANALYZE, BUFFERS) "+query, args...)
	if err != nil {
		t.Fatalf("%s: explain: %v", label, err)
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			t.Fatalf("%s: explain scan: %v", label, err)
		}
		t.Log(line)
	}
	t.Log("")
}

// ---------------------------------------------------------------------------
// Main investigation
// ---------------------------------------------------------------------------

func TestQueryResourcesExplainPlan(t *testing.T) {
	if os.Getenv("FLEETSHIFT_QUERY_BENCH") == "" {
		t.Skip("set FLEETSHIFT_QUERY_BENCH=1 to run (spins up a dedicated Postgres container and seeds a realistic-scale corpus)")
	}

	db := openBenchDB(t)
	seedQRBCorpus(t, db)

	// Empty-filter first page should seek idx_extension_resources_query_order.
	explainQueryResources(t, db, "empty filter (default first page)", "", "", defaultQueryPageSize, nil)
	// Second page should keyset-seek rather than full rescan/sort.
	explainQueryResources(t, db, "empty filter (default second page keyset)", "", "", defaultQueryPageSize, &queryPageToken{
		CollectionName: qrbClusterCollection,
		ResourceID:     "cluster-00000049",
		ServiceName:    qrbClusterService,
		TypeName:       qrbClusterType,
	})
	explainQueryResources(t, db, "resourceType,name order first page", "", "resource_type,name", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "selective resourceType equality (constituent columns)",
		fmt.Sprintf(`resourceType == "%s/%s"`, qrbClusterService, qrbClusterType), "", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "extension label equality (GIN containment)",
		`resource.labels["team"] == "platform"`, "", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "guarded spec filter",
		fmt.Sprintf(`resourceType == "%s/%s" && resource.spec.provider == "aws"`, qrbClusterService, qrbClusterType),
		"", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "inventory label equality (GIN containment)",
		`resource.localLabels["node-role"] == "worker"`, "", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "inventory condition equality (GIN containment)",
		`resource.conditions["Ready"].status == "True"`, "", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "guarded numeric observation filter (safeJSONCast)",
		fmt.Sprintf(`resourceType == "%s/%s" && resource.observation.capacity.cpu > 32`, qrbNodeService, qrbNodeType),
		"", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "max page size (500), empty filter", "", "", maxQueryPageSize, nil)
	// Worst-case non-matching JSON paths force a full typed scan
	// (matching pages can stop at LIMIT once enough rows qualify).
	explainQueryResources(t, db, "worst-case non-matching guarded spec",
		fmt.Sprintf(`resourceType == "%s/%s" && resource.spec.provider == "oracle"`, qrbClusterService, qrbClusterType),
		"", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "worst-case non-matching guarded observation",
		fmt.Sprintf(`resourceType == "%s/%s" && resource.observation.capacity.cpu > 100`, qrbNodeService, qrbNodeType),
		"", defaultQueryPageSize, nil)
	// timestamp() conversion: native TIMESTAMPTZ and dual-stored
	// condition lastTransitionTime (ProtoJSON + fixed-width norm; GIN
	// equality on the norm sibling).
	explainQueryResources(t, db, "timestamp conversion native matching",
		fmt.Sprintf(`resourceType == "%s/%s" && timestamp(resource.localUpdateTime) == timestamp("2026-06-01T08:00:00-04:00")`,
			qrbNodeService, qrbNodeType),
		"", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "timestamp conversion native non-matching",
		fmt.Sprintf(`resourceType == "%s/%s" && timestamp(resource.localUpdateTime) == timestamp("2099-01-01T00:00:00Z")`,
			qrbNodeService, qrbNodeType),
		"", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "timestamp conversion condition non-matching",
		fmt.Sprintf(`resourceType == "%s/%s" && timestamp(resource.conditions["Ready"].lastTransitionTime) == timestamp("2099-01-01T00:00:00Z")`,
			qrbNodeService, qrbNodeType),
		"", defaultQueryPageSize, nil)
}

// ---------------------------------------------------------------------------
// Absolute timings (repeated QueryResources rounds)
// ---------------------------------------------------------------------------

type qrbTimings struct {
	scenario  string
	pageSize  int
	rows      int
	durations []time.Duration
}

func (r qrbTimings) stats() (mean, p50, p95, minD, maxD time.Duration) {
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

func (r qrbTimings) String() string {
	mean, p50, p95, minD, maxD := r.stats()
	return fmt.Sprintf("n=%-2d  mean=%-10s  p50=%-10s  p95=%-10s  min=%-10s  max=%-10s",
		len(r.durations), mean, p50, p95, minD, maxD)
}

// timeQueryResources times QueryRepo.QueryResources over warmup +
// timed rounds. Each round uses its own read-only transaction
// (begin/query/rollback), matching the store's one-tx-per-call shape.
func timeQueryResources(t *testing.T, db *sql.DB, scenario string, req domain.QueryResourcesRequest) qrbTimings {
	t.Helper()
	ctx := context.Background()

	runOnce := func() (domain.QueryResourcesPage, time.Duration, error) {
		start := time.Now()
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return domain.QueryResourcesPage{}, 0, fmt.Errorf("begin: %w", err)
		}
		page, err := (&QueryRepo{DB: tx}).QueryResources(ctx, req)
		_ = tx.Rollback()
		return page, time.Since(start), err
	}

	for i := 0; i < qrbWarmupRounds; i++ {
		if _, _, err := runOnce(); err != nil {
			t.Fatalf("%s warmup %d: %v", scenario, i, err)
		}
	}

	durs := make([]time.Duration, qrbRounds)
	var lastPage domain.QueryResourcesPage
	for round := 0; round < qrbRounds; round++ {
		page, d, err := runOnce()
		if err != nil {
			t.Fatalf("%s round %d: %v", scenario, round, err)
		}
		durs[round] = d
		lastPage = page
	}
	t.Logf("%s  page_size=%d  rows=%d  next_token=%v  %s",
		scenario, req.PageSize, len(lastPage.Resources), lastPage.NextPageToken != "",
		qrbTimings{scenario: scenario, pageSize: int(req.PageSize), rows: len(lastPage.Resources), durations: durs})
	return qrbTimings{scenario: scenario, pageSize: int(req.PageSize), rows: len(lastPage.Resources), durations: durs}
}

// seedQRBCorpus seeds the shared QueryResources bench corpus and
// ANALYZEs. Used by both the EXPLAIN and absolute-timing drivers.
func seedQRBCorpus(t *testing.T, db *sql.DB) {
	t.Helper()
	t.Log("seeding corpus...")
	seedStart := time.Now()
	seedQRBTypes(t, db)
	seedQRBPlatformOnly(t, db)
	seedQRBClusters(t, db)
	seedQRBNodes(t, db)
	seedQRBAliasesAndRelationships(t, db)
	if _, err := db.ExecContext(context.Background(), `ANALYZE
		platform_resources, extension_resources, extension_resource_types,
		extension_resource_managed, resource_intents, fulfillments,
		extension_resource_inventory, resource_alias_claims, resource_relationships`); err != nil {
		t.Fatalf("analyze: %v", err)
	}
	t.Logf("seeded corpus in %s", time.Since(seedStart))
	qrbTableSizes(t, db)
	t.Log("")
}

// TestQueryResourcesBenchmark times QueryRepo.QueryResources against
// the same corpus/scenarios as TestQueryResourcesExplainPlan, but as
// repeated wall-clock samples (mean/p50/p95) rather than one-shot
// EXPLAIN ANALYZE. EXPLAIN's "Execution Time" is useful for plan
// shape; this is the absolute latency number for the full Go path
// (compile + begin + query + scan + rollback).
//
// In addition to selective / page-filling filters, the suite includes
// worst-case non-matching JSON paths (spec / observation) that must
// scan every typed candidate before returning empty, plus timestamp()
// conversion scenarios for native inventory columns and dynamic
// condition strings — matching-page benches alone can stop early and
// hide that cost.
func TestQueryResourcesBenchmark(t *testing.T) {
	if os.Getenv("FLEETSHIFT_QUERY_BENCH") == "" {
		t.Skip("set FLEETSHIFT_QUERY_BENCH=1 to run (spins up a dedicated Postgres container and seeds a realistic-scale corpus)")
	}

	db := openBenchDB(t)
	seedQRBCorpus(t, db)

	t.Log("=== QueryResources absolute timings (warmup discarded) ===")
	scenarios := []struct {
		name      string
		req       domain.QueryResourcesRequest
		wantEmpty bool
	}{
		{"empty filter (default first page)", domain.QueryResourcesRequest{PageSize: int32(defaultQueryPageSize)}, false},
		{"resourceType,name order first page", domain.QueryResourcesRequest{
			PageSize: int32(defaultQueryPageSize),
			OrderBy:  "resource_type,name",
		}, false},
		{"selective resourceType equality", domain.QueryResourcesRequest{
			Filter:   fmt.Sprintf(`resourceType == "%s/%s"`, qrbClusterService, qrbClusterType),
			PageSize: int32(defaultQueryPageSize),
		}, false},
		{"extension label equality", domain.QueryResourcesRequest{
			Filter:   `resource.labels["team"] == "platform"`,
			PageSize: int32(defaultQueryPageSize),
		}, false},
		{"guarded spec filter", domain.QueryResourcesRequest{
			Filter: fmt.Sprintf(`resourceType == "%s/%s" && resource.spec.provider == "aws"`,
				qrbClusterService, qrbClusterType),
			PageSize: int32(defaultQueryPageSize),
		}, false},
		{"inventory label equality", domain.QueryResourcesRequest{
			Filter:   `resource.localLabels["node-role"] == "worker"`,
			PageSize: int32(defaultQueryPageSize),
		}, false},
		{"inventory condition equality", domain.QueryResourcesRequest{
			Filter:   `resource.conditions["Ready"].status == "True"`,
			PageSize: int32(defaultQueryPageSize),
		}, false},
		{"guarded numeric observation filter", domain.QueryResourcesRequest{
			Filter: fmt.Sprintf(`resourceType == "%s/%s" && resource.observation.capacity.cpu > 32`,
				qrbNodeService, qrbNodeType),
			PageSize: int32(defaultQueryPageSize),
		}, false},
		{"max page size (500), empty filter", domain.QueryResourcesRequest{PageSize: int32(maxQueryPageSize)}, false},
		// Worst-case non-matching JSON paths: no row matches, so the
		// engine cannot stop at pageSize and must evaluate the path
		// across every typed candidate.
		{"worst-case non-matching guarded spec", domain.QueryResourcesRequest{
			Filter: fmt.Sprintf(`resourceType == "%s/%s" && resource.spec.provider == "oracle"`,
				qrbClusterService, qrbClusterType),
			PageSize: int32(defaultQueryPageSize),
		}, true},
		{"worst-case non-matching guarded observation", domain.QueryResourcesRequest{
			Filter: fmt.Sprintf(`resourceType == "%s/%s" && resource.observation.capacity.cpu > 100`,
				qrbNodeService, qrbNodeType),
			PageSize: int32(defaultQueryPageSize),
		}, true},
		// timestamp() conversion: native TIMESTAMPTZ (localUpdateTime)
		// and dual-stored condition lastTransitionTime (GIN on norm).
		{"timestamp conversion native matching", domain.QueryResourcesRequest{
			Filter: fmt.Sprintf(
				`resourceType == "%s/%s" && timestamp(resource.localUpdateTime) == timestamp("2026-06-01T08:00:00-04:00")`,
				qrbNodeService, qrbNodeType),
			PageSize: int32(defaultQueryPageSize),
		}, false},
		{"timestamp conversion native non-matching", domain.QueryResourcesRequest{
			Filter: fmt.Sprintf(
				`resourceType == "%s/%s" && timestamp(resource.localUpdateTime) == timestamp("2099-01-01T00:00:00Z")`,
				qrbNodeService, qrbNodeType),
			PageSize: int32(defaultQueryPageSize),
		}, true},
		{"timestamp conversion condition non-matching", domain.QueryResourcesRequest{
			Filter: fmt.Sprintf(
				`resourceType == "%s/%s" && timestamp(resource.conditions["Ready"].lastTransitionTime) == timestamp("2099-01-01T00:00:00Z")`,
				qrbNodeService, qrbNodeType),
			PageSize: int32(defaultQueryPageSize),
		}, true},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			got := timeQueryResources(t, db, sc.name, sc.req)
			if sc.wantEmpty && got.rows != 0 {
				t.Fatalf("%s: expected empty result (full candidate scan), got %d rows", sc.name, got.rows)
			}
			if !sc.wantEmpty && got.rows == 0 {
				t.Fatalf("%s: expected matching rows, got 0", sc.name)
			}
		})
	}

	// Second-page keyset: first call establishes a real token, then
	// time resumed pages against that token.
	t.Run("empty filter (default second page keyset)", func(t *testing.T) {
		first, err := func() (domain.QueryResourcesPage, error) {
			tx, err := db.BeginTx(context.Background(), nil)
			if err != nil {
				return domain.QueryResourcesPage{}, err
			}
			defer tx.Rollback()
			return (&QueryRepo{DB: tx}).QueryResources(context.Background(), domain.QueryResourcesRequest{
				PageSize: int32(defaultQueryPageSize),
			})
		}()
		if err != nil {
			t.Fatalf("first page: %v", err)
		}
		if first.NextPageToken == "" {
			t.Fatal("expected next page token from first page")
		}
		timeQueryResources(t, db, "empty filter (default second page keyset)", domain.QueryResourcesRequest{
			PageSize:  int32(defaultQueryPageSize),
			PageToken: first.NextPageToken,
		})
	})
}
