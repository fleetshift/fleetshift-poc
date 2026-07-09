package sqlite

// Comparable QueryResources bench to postgres/query_repo_bench_test.go:
// same corpus shape (20k clusters + 20k nodes + 5k platform-only),
// same scenarios, same warmup/round counts, same mean/p50/p95 reporting.
//
// SQLite differences:
//   - file-backed temp DB (no container); WAL + busy_timeout via Open
//   - bulk multi-row INSERT instead of UNNEST
//   - EXPLAIN QUERY PLAN instead of EXPLAIN (ANALYZE, BUFFERS)
//   - label/condition filters use json_extract (no GIN)
//
// Run with:
//
//	FLEETSHIFT_QUERY_BENCH=1 go test ./internal/infrastructure/sqlite/ -run 'TestQueryResources(ExplainPlan|Benchmark)$' -v -timeout 10m

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// ---------------------------------------------------------------------------
// Corpus shape (kept in lockstep with postgres/query_repo_bench_test.go)
// ---------------------------------------------------------------------------

const (
	qrbClusterService    = "kind.fleetshift.io"
	qrbClusterType       = "Cluster"
	qrbClusterCollection = "clusters"

	qrbNodeService    = "kubernetes.fleetshift.io"
	qrbNodeType       = "Node"
	qrbNodeCollection = "nodes"

	qrbPlatformOnlyCollection = "assets"

	qrbClusterCount      = 20_000
	qrbNodeCount         = 20_000
	qrbPlatformOnlyCount = 5_000

	// Smaller chunks than Postgres UNNEST batches: SQLite binds every
	// value as a separate "?" and has a default variable limit of 999
	// (or higher when compiled with a larger SQLITE_MAX_VARIABLE_NUMBER;
	// modernc uses 32766). Keep well under that for multi-column rows.
	qrbChunk = 200

	qrbRounds       = 15
	qrbWarmupRounds = 2
)

var qrbFixedTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

func openQRBBenchDB(t *testing.T) *sql.DB {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "query_bench.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("open bench db: %v", err)
	}
	// Single writer connection for bulk seed + query; avoids pool
	// contention on a file-backed DB during the timed rounds.
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// ---------------------------------------------------------------------------
// Seeding
// ---------------------------------------------------------------------------

func seedQRBTypes(t *testing.T, db *sql.DB) {
	t.Helper()
	createdAt := qrbFixedTime.UTC().Format(time.RFC3339Nano)
	if _, err := db.ExecContext(context.Background(), `
		INSERT INTO extension_resource_types (service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at)
		VALUES
			(?, ?, 'v1', ?, '{}', NULL, ?, ?),
			(?, ?, 'v1', ?, NULL, '{}', ?, ?)
	`, qrbClusterService, qrbClusterType, qrbClusterCollection, createdAt, createdAt,
		qrbNodeService, qrbNodeType, qrbNodeCollection, createdAt, createdAt); err != nil {
		t.Fatalf("seed extension resource types: %v", err)
	}
}

func seedQRBPlatformOnly(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	createdAt := qrbFixedTime.UTC().Format(time.RFC3339Nano)
	for start := 0; start < qrbPlatformOnlyCount; start += qrbChunk {
		n := min(qrbChunk, qrbPlatformOnlyCount-start)
		placeholders := make([]string, n)
		args := make([]any, 0, n*4)
		for i := 0; i < n; i++ {
			idx := start + i
			env := "prod"
			if idx%2 == 0 {
				env = "dev"
			}
			placeholders[i] = "(?, ?, ?, ?, ?)"
			args = append(args, qrbPlatformOnlyCollection, fmt.Sprintf("asset-%08d", idx),
				fmt.Sprintf(`{"env":%q}`, env), createdAt, createdAt)
		}
		if _, err := db.ExecContext(ctx, `
			INSERT INTO platform_resources (collection_name, resource_id, labels, created_at, updated_at)
			VALUES `+strings.Join(placeholders, ", "), args...); err != nil {
			t.Fatalf("seed platform-only [%d:%d]: %v", start, start+n, err)
		}
	}
}

func seedQRBClusters(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	teams := []string{"platform", "apps", "data"}
	providers := []string{"aws", "gcp", "azure"}
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-south-1"}
	createdAt := qrbFixedTime.UTC().Format(time.RFC3339Nano)

	for start := 0; start < qrbClusterCount; start += qrbChunk {
		n := min(qrbChunk, qrbClusterCount-start)
		uids := make([]string, n)
		resourceIDs := make([]string, n)
		labels := make([]string, n)
		specs := make([]string, n)
		fulfillmentIDs := make([]string, n)
		for i := 0; i < n; i++ {
			idx := start + i
			uids[i] = domain.NewExtensionResourceUID().String()
			resourceIDs[i] = fmt.Sprintf("cluster-%08d", idx)
			labels[i] = fmt.Sprintf(`{"team":%q}`, teams[idx%len(teams)])
			specs[i] = fmt.Sprintf(`{"provider":%q,"region":%q}`, providers[idx%len(providers)], regions[idx%len(regions)])
			fulfillmentIDs[i] = fmt.Sprintf("qrb-fulfillment-%08d", idx)
		}

		{
			placeholders := make([]string, n)
			args := make([]any, 0, n*9)
			for i := 0; i < n; i++ {
				placeholders[i] = "(?, ?, ?, ?, ?, ?, '{}', ?, ?)"
				args = append(args, uids[i], qrbClusterService, qrbClusterType, qrbClusterCollection,
					resourceIDs[i], labels[i], createdAt, createdAt)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, reported_aliases, created_at, updated_at)
				VALUES `+strings.Join(placeholders, ", "), args...); err != nil {
				t.Fatalf("seed cluster extension_resources [%d:%d]: %v", start, start+n, err)
			}
		}
		{
			placeholders := make([]string, n)
			args := make([]any, 0, n*3)
			for i := 0; i < n; i++ {
				placeholders[i] = "(?, 1, ?)"
				args = append(args, uids[i], fulfillmentIDs[i])
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO extension_resource_managed (extension_resource_uid, current_version, fulfillment_id)
				VALUES `+strings.Join(placeholders, ", "), args...); err != nil {
				t.Fatalf("seed extension_resource_managed [%d:%d]: %v", start, start+n, err)
			}
		}
		{
			placeholders := make([]string, n)
			args := make([]any, 0, n*3)
			for i := 0; i < n; i++ {
				placeholders[i] = "(?, 1, ?, ?)"
				args = append(args, uids[i], specs[i], createdAt)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO resource_intents (extension_resource_uid, version, spec, created_at)
				VALUES `+strings.Join(placeholders, ", "), args...); err != nil {
				t.Fatalf("seed resource_intents [%d:%d]: %v", start, start+n, err)
			}
		}
		{
			placeholders := make([]string, n)
			args := make([]any, 0, n*3)
			for i := 0; i < n; i++ {
				placeholders[i] = "(?, 'active', ?, ?)"
				args = append(args, fulfillmentIDs[i], createdAt, createdAt)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO fulfillments (id, state, created_at, updated_at)
				VALUES `+strings.Join(placeholders, ", "), args...); err != nil {
				t.Fatalf("seed fulfillments [%d:%d]: %v", start, start+n, err)
			}
		}
	}
}

func seedQRBNodes(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	roles := []string{"worker", "control-plane"}
	createdAt := qrbFixedTime.UTC().Format(time.RFC3339Nano)

	for start := 0; start < qrbNodeCount; start += qrbChunk {
		n := min(qrbChunk, qrbNodeCount-start)
		uids := make([]string, n)
		resourceIDs := make([]string, n)
		invLabels := make([]string, n)
		observations := make([]string, n)
		conditions := make([]string, n)
		for i := 0; i < n; i++ {
			idx := start + i
			uids[i] = domain.NewExtensionResourceUID().String()
			resourceIDs[i] = fmt.Sprintf("node-%08d", idx)
			role := roles[idx%len(roles)]
			invLabels[i] = fmt.Sprintf(`{"node-role":%q}`, role)
			cpu := idx%64 + 1
			observations[i] = fmt.Sprintf(`{"capacity":{"cpu":%d},"allocatable":{"cpu":%d}}`, cpu, max(cpu-2, 1))
			ready := "True"
			if idx%20 == 0 {
				ready = "False"
			}
			conditions[i] = fmt.Sprintf(
				`{"Ready":{"status":%q,"reason":"Probe","message":"steady","lastTransitionTime":"2026-06-01T12:00:00Z"}}`, ready)
		}

		{
			placeholders := make([]string, n)
			args := make([]any, 0, n*9)
			for i := 0; i < n; i++ {
				placeholders[i] = "(?, ?, ?, ?, ?, '{}', '{}', ?, ?)"
				args = append(args, uids[i], qrbNodeService, qrbNodeType, qrbNodeCollection,
					resourceIDs[i], createdAt, createdAt)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, reported_aliases, created_at, updated_at)
				VALUES `+strings.Join(placeholders, ", "), args...); err != nil {
				t.Fatalf("seed node extension_resources [%d:%d]: %v", start, start+n, err)
			}
		}
		{
			placeholders := make([]string, n)
			args := make([]any, 0, n*6)
			for i := 0; i < n; i++ {
				placeholders[i] = "(?, ?, ?, ?, ?, ?)"
				args = append(args, uids[i], observations[i], invLabels[i], conditions[i], createdAt, createdAt)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO extension_resource_inventory (extension_resource_uid, observation, labels, conditions, observed_at, updated_at)
				VALUES `+strings.Join(placeholders, ", "), args...); err != nil {
				t.Fatalf("seed extension_resource_inventory [%d:%d]: %v", start, start+n, err)
			}
		}
	}
}

func seedQRBAliasesAndRelationships(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	createdAt := qrbFixedTime.UTC().Format(time.RFC3339Nano)
	for start := 0; start < qrbPlatformOnlyCount; start += qrbChunk {
		n := min(qrbChunk, qrbPlatformOnlyCount-start)
		{
			placeholders := make([]string, n)
			args := make([]any, 0, n*5)
			for i := 0; i < n; i++ {
				idx := start + i
				placeholders[i] = "('ext-id', 'source-id', ?, ?, ?, 1, ?)"
				args = append(args, fmt.Sprintf("ext-%08d", idx), qrbPlatformOnlyCollection,
					fmt.Sprintf("asset-%08d", idx), createdAt)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id, platform_owned, created_at)
				VALUES `+strings.Join(placeholders, ", "), args...); err != nil {
				t.Fatalf("seed resource_alias_claims [%d:%d]: %v", start, start+n, err)
			}
		}
		{
			placeholders := make([]string, n)
			args := make([]any, 0, n*4)
			for i := 0; i < n; i++ {
				idx := start + i
				placeholders[i] = "(?, ?, 'depends-on', ?, ?, 'bench.fleetshift.io', ?)"
				args = append(args, qrbPlatformOnlyCollection, fmt.Sprintf("asset-%08d", idx),
					qrbPlatformOnlyCollection, fmt.Sprintf("asset-%08d", (idx+1)%qrbPlatformOnlyCount), createdAt)
			}
			if _, err := db.ExecContext(ctx, `
				INSERT INTO resource_relationships (source_collection_name, source_resource_id, type, target_collection_name, target_resource_id, source_service, created_at)
				VALUES `+strings.Join(placeholders, ", "), args...); err != nil {
				t.Fatalf("seed resource_relationships [%d:%d]: %v", start, start+n, err)
			}
		}
	}
}

func qrbTableSizes(t *testing.T, db *sql.DB) {
	t.Helper()
	tables := []string{
		"platform_resources", "extension_resources", "extension_resource_types",
		"extension_resource_managed", "resource_intents", "fulfillments",
		"extension_resource_inventory", "resource_alias_claims", "resource_relationships",
	}
	for _, name := range tables {
		var n int64
		if err := db.QueryRowContext(context.Background(),
			fmt.Sprintf(`SELECT COUNT(*) FROM %s`, name)).Scan(&n); err != nil {
			t.Logf("  %-32s count failed: %v", name, err)
			continue
		}
		t.Logf("  %-32s %d", name, n)
	}
}

func seedQRBCorpus(t *testing.T, db *sql.DB) {
	t.Helper()
	t.Log("seeding corpus...")
	seedStart := time.Now()
	seedQRBTypes(t, db)
	seedQRBPlatformOnly(t, db)
	seedQRBClusters(t, db)
	seedQRBNodes(t, db)
	seedQRBAliasesAndRelationships(t, db)
	if _, err := db.ExecContext(context.Background(), `ANALYZE`); err != nil {
		t.Fatalf("analyze: %v", err)
	}
	t.Logf("seeded corpus in %s", time.Since(seedStart))
	qrbTableSizes(t, db)
	t.Log("")
}

// ---------------------------------------------------------------------------
// EXPLAIN driver
// ---------------------------------------------------------------------------

func explainQueryResources(t *testing.T, db *sql.DB, label, filter, orderBy string, pageSize int, keysetTok *queryPageToken) {
	t.Helper()
	order, err := resolveQueryOrder(orderBy)
	if err != nil {
		t.Fatalf("%s: resolve order %q: %v", label, orderBy, err)
	}
	compiler := querysql.Compiler{Fields: queryFieldResolver{}, Params: querysql.QuestionParams{}}
	predicate, err := compiler.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter})
	if err != nil {
		t.Fatalf("%s: compile filter %q: %v", label, filter, err)
	}
	args := append([]any{}, predicate.Args...)

	keysetSQL := "TRUE"
	if keysetTok != nil {
		keysetSQL, args = keysetPredicateSQL(order, *keysetTok, args)
	}
	args = append(args, pageSize+1)

	query := buildQueryResourcesSQL(predicate.SQL, keysetSQL, order)

	t.Logf("=== %s ===", label)
	t.Logf("filter: %q  order_by: %q  page_size: %d  keyset: %v", filter, orderBy, pageSize, keysetTok != nil)
	rows, err := db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		t.Fatalf("%s: explain: %v", label, err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			t.Fatalf("%s: explain scan: %v", label, err)
		}
		t.Logf("  %s", detail)
	}
	t.Log("")
}

func TestQueryResourcesExplainPlan(t *testing.T) {
	if os.Getenv("FLEETSHIFT_QUERY_BENCH") == "" {
		t.Skip("set FLEETSHIFT_QUERY_BENCH=1 to run (seeds a realistic-scale corpus on a temp SQLite file)")
	}

	db := openQRBBenchDB(t)
	seedQRBCorpus(t, db)

	explainQueryResources(t, db, "empty filter (default first page)", "", "", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "empty filter (default second page keyset)", "", "", defaultQueryPageSize, &queryPageToken{
		CollectionName: qrbClusterCollection,
		ResourceID:     "cluster-00000049",
		ServiceName:    qrbClusterService,
		TypeName:       qrbClusterType,
	})
	explainQueryResources(t, db, "resource_type,name order first page", "", "resource_type,name", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "selective resource_type equality (constituent columns)",
		fmt.Sprintf(`resource_type == "%s/%s"`, qrbClusterService, qrbClusterType), "", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "extension label equality (json_extract)",
		`resource.labels["team"] == "platform"`, "", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "guarded spec filter",
		fmt.Sprintf(`resource_type == "%s/%s" && resource.spec.provider == "aws"`, qrbClusterService, qrbClusterType),
		"", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "inventory label equality (json_extract)",
		`resource.inventory.labels["node-role"] == "worker"`, "", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "inventory condition equality (json_extract)",
		`resource.inventory.conditions["Ready"].status == "True"`, "", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "guarded numeric observation filter (safeJSONNumberCast)",
		fmt.Sprintf(`resource_type == "%s/%s" && resource.inventory.observation.capacity.cpu > 32`, qrbNodeService, qrbNodeType),
		"", defaultQueryPageSize, nil)
	explainQueryResources(t, db, "max page size (500), empty filter", "", "", maxQueryPageSize, nil)
}

// ---------------------------------------------------------------------------
// Absolute timings (repeated QueryResources rounds)
// ---------------------------------------------------------------------------

type qrbTimings struct {
	scenario  string
	pageSize  int
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
		qrbTimings{scenario: scenario, pageSize: int(req.PageSize), durations: durs})
	return qrbTimings{scenario: scenario, pageSize: int(req.PageSize), durations: durs}
}

func TestQueryResourcesBenchmark(t *testing.T) {
	if os.Getenv("FLEETSHIFT_QUERY_BENCH") == "" {
		t.Skip("set FLEETSHIFT_QUERY_BENCH=1 to run (seeds a realistic-scale corpus on a temp SQLite file)")
	}

	db := openQRBBenchDB(t)
	seedQRBCorpus(t, db)

	t.Log("=== QueryResources absolute timings (warmup discarded) ===")
	scenarios := []struct {
		name string
		req  domain.QueryResourcesRequest
	}{
		{"empty filter (default first page)", domain.QueryResourcesRequest{PageSize: int32(defaultQueryPageSize)}},
		{"resource_type,name order first page", domain.QueryResourcesRequest{
			PageSize: int32(defaultQueryPageSize),
			OrderBy:  "resource_type,name",
		}},
		{"selective resource_type equality", domain.QueryResourcesRequest{
			Filter:   fmt.Sprintf(`resource_type == "%s/%s"`, qrbClusterService, qrbClusterType),
			PageSize: int32(defaultQueryPageSize),
		}},
		{"extension label equality", domain.QueryResourcesRequest{
			Filter:   `resource.labels["team"] == "platform"`,
			PageSize: int32(defaultQueryPageSize),
		}},
		{"guarded spec filter", domain.QueryResourcesRequest{
			Filter: fmt.Sprintf(`resource_type == "%s/%s" && resource.spec.provider == "aws"`,
				qrbClusterService, qrbClusterType),
			PageSize: int32(defaultQueryPageSize),
		}},
		{"inventory label equality", domain.QueryResourcesRequest{
			Filter:   `resource.inventory.labels["node-role"] == "worker"`,
			PageSize: int32(defaultQueryPageSize),
		}},
		{"inventory condition equality", domain.QueryResourcesRequest{
			Filter:   `resource.inventory.conditions["Ready"].status == "True"`,
			PageSize: int32(defaultQueryPageSize),
		}},
		{"guarded numeric observation filter", domain.QueryResourcesRequest{
			Filter: fmt.Sprintf(`resource_type == "%s/%s" && resource.inventory.observation.capacity.cpu > 32`,
				qrbNodeService, qrbNodeType),
			PageSize: int32(defaultQueryPageSize),
		}},
		{"max page size (500), empty filter", domain.QueryResourcesRequest{PageSize: int32(maxQueryPageSize)}},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			timeQueryResources(t, db, sc.name, sc.req)
		})
	}

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
