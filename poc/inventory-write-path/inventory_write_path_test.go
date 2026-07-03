package inventorywritepath

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const corpusSize = 100_000

func isPodmanAvailable() bool {
	_, err := exec.LookPath("podman")
	return err == nil
}

func init() {
	if os.Getenv("TESTCONTAINERS_PROVIDER") != "docker" && isPodmanAvailable() {
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
}

func TestInventoryWritePathPlans(t *testing.T) {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("inventory_write_path_poc"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp")),
		detectProvider(),
		testcontainers.WithCmd("postgres", "-c", "shared_buffers=1GB", "-c", "max_wal_size=4GB"),
	)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() {
		if err := ctr.Terminate(context.Background()); err != nil {
			t.Logf("terminate postgres: %v", err)
		}
	})

	conn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	db, err := sql.Open("pgx", conn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if err := execSQL(ctx, db, schemaSQL); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if err := execSQL(ctx, db, seedSQL); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var resourceCount, inventoryCount, claimCount, contributionCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM extension_resources`).Scan(&resourceCount); err != nil {
		t.Fatalf("count extension resources: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM extension_resource_inventory`).Scan(&inventoryCount); err != nil {
		t.Fatalf("count inventory: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM resource_alias_claims`).Scan(&claimCount); err != nil {
		t.Fatalf("count alias claims: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM resource_alias_contributions`).Scan(&contributionCount); err != nil {
		t.Fatalf("count alias contributions: %v", err)
	}
	t.Logf("seeded %d corpus resources, %d total resources, %d inventory rows, %d claims, %d contributions",
		corpusSize, resourceCount, inventoryCount, claimCount, contributionCount)

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			plan, err := explain(ctx, db, scenario.sql)
			if err != nil {
				t.Fatalf("explain %s: %v", scenario.name, err)
			}
			t.Logf("\n%s", summarizePlan(plan))
		})
	}

	assertCount(ctx, t, db, "delta patched labels",
		`SELECT count(*) FROM extension_resource_inventory inv JOIN extension_resources er ON er.uid = inv.extension_resource_uid WHERE er.service_name = 'bench.fleetshift.io' AND er.resource_id BETWEEN 'r-00003001' AND 'r-00004000' AND inv.labels @> '{"patched":"true"}'::jsonb`,
		1_000,
	)
	assertCount(ctx, t, db, "replace changed observations",
		`SELECT count(*) FROM extension_resource_inventory inv JOIN extension_resources er ON er.uid = inv.extension_resource_uid WHERE er.service_name = 'bench.fleetshift.io' AND er.resource_id BETWEEN 'r-00004001' AND 'r-00005000' AND inv.observation @> '{"generation":2}'::jsonb`,
		1_000,
	)
	assertCount(ctx, t, db, "new secondary alias claims",
		`SELECT count(*) FROM resource_alias_claims WHERE namespace = 'ext-id' AND key = 'secondary-id' AND value LIKE 'new-secondary-apply-%'`,
		1_000,
	)
	assertCount(ctx, t, db, "new secondary alias contributions",
		`SELECT count(*) FROM resource_alias_contributions c JOIN resource_alias_claims cl ON cl.id = c.claim_id WHERE cl.namespace = 'ext-id' AND cl.key = 'secondary-id' AND cl.value LIKE 'new-secondary-apply-%'`,
		1_000,
	)
	assertCount(ctx, t, db, "self-replaced alias old claims",
		`SELECT count(*)
		 FROM generate_series(67001, 68000) AS g
		 JOIN resource_alias_claims cl
		   ON cl.namespace = 'ext-id'
		  AND cl.key = 'source-id'
		  AND cl.value = 'ext-' || lpad(g::text, 8, '0')`,
		0,
	)
	assertCount(ctx, t, db, "self-replaced alias new claims",
		`SELECT count(*) FROM resource_alias_claims WHERE namespace = 'ext-id' AND key = 'source-id' AND value LIKE 'ext-changed-apply-%'`,
		1_000,
	)
	assertCount(ctx, t, db, "self-replaced alias contributions",
		`SELECT count(*)
		 FROM resource_alias_contributions c
		 JOIN resource_alias_claims cl ON cl.id = c.claim_id
		 JOIN extension_resources er ON er.uid = c.source_extension_resource_uid
		 WHERE er.service_name = 'bench.fleetshift.io'
		   AND er.resource_id BETWEEN 'r-00067001' AND 'r-00068000'
		   AND cl.namespace = 'ext-id'
		   AND cl.key = 'source-id'
		   AND cl.value LIKE 'ext-changed-apply-%'`,
		1_000,
	)
	assertCount(ctx, t, db, "sibling-conflict old claims remain",
		`SELECT count(*)
		 FROM generate_series(82001, 83000) AS g
		 JOIN resource_alias_claims cl
		   ON cl.namespace = 'ext-id'
		  AND cl.key = 'source-id'
		  AND cl.value = 'ext-' || lpad(g::text, 8, '0')`,
		1_000,
	)
	assertCount(ctx, t, db, "sibling-conflict new claims absent",
		`SELECT count(*) FROM resource_alias_claims WHERE namespace = 'ext-id' AND key = 'source-id' AND value LIKE 'ext-conflict-%'`,
		0,
	)
	assertCount(ctx, t, db, "sibling-conflict primary contributor unchanged",
		`SELECT count(*)
		 FROM resource_alias_contributions c
		 JOIN resource_alias_claims cl ON cl.id = c.claim_id
		 JOIN extension_resources er ON er.uid = c.source_extension_resource_uid
		 WHERE er.service_name = 'bench.fleetshift.io'
		   AND er.resource_id BETWEEN 'r-00082001' AND 'r-00083000'
		   AND cl.namespace = 'ext-id'
		   AND cl.key = 'source-id'
		   AND cl.value = 'ext-' || substring(er.resource_id from 3)`,
		1_000,
	)
	assertCount(ctx, t, db, "sibling-conflict secondary contributor unchanged",
		`SELECT count(*)
		 FROM resource_alias_contributions c
		 JOIN resource_alias_claims cl ON cl.id = c.claim_id
		 JOIN extension_resources er ON er.uid = c.source_extension_resource_uid
		 WHERE er.service_name = 'bench-b.fleetshift.io'
		   AND er.resource_id BETWEEN 'r-00082001' AND 'r-00083000'
		   AND cl.namespace = 'ext-id'
		   AND cl.key = 'source-id'
		   AND cl.value = 'ext-' || substring(er.resource_id from 3)`,
		1_000,
	)
	assertCount(ctx, t, db, "full replace mixed latest observations",
		`SELECT count(*)
		 FROM extension_resource_inventory inv
		 JOIN extension_resources er ON er.uid = inv.extension_resource_uid
		 WHERE er.service_name = 'bench.fleetshift.io'
		   AND er.resource_id BETWEEN 'r-00084001' AND 'r-00084600'
		   AND inv.observation @> '{"generation":3}'::jsonb`,
		600,
	)
	assertCount(ctx, t, db, "full replace mixed secondary claims",
		`SELECT count(*) FROM resource_alias_claims WHERE namespace = 'ext-id' AND key = 'secondary-id' AND value LIKE 'full-secondary-%'`,
		100,
	)
	assertCount(ctx, t, db, "full replace mixed old self-replaced claims",
		`SELECT count(*)
		 FROM generate_series(84301, 84400) AS g
		 JOIN resource_alias_claims cl
		   ON cl.namespace = 'ext-id'
		  AND cl.key = 'source-id'
		  AND cl.value = 'ext-' || lpad(g::text, 8, '0')`,
		0,
	)
	assertCount(ctx, t, db, "full replace mixed new self-replaced claims",
		`SELECT count(*) FROM resource_alias_claims WHERE namespace = 'ext-id' AND key = 'source-id' AND value LIKE 'full-replaced-%'`,
		100,
	)
	assertCount(ctx, t, db, "full replace mixed retracted claims",
		`SELECT count(*) FROM resource_alias_claims WHERE namespace = 'ext-id' AND key = 'source-id' AND platform_resource_id BETWEEN 'r-00084401' AND 'r-00084500'`,
		0,
	)
	assertCount(ctx, t, db, "full replace mixed reuse primary contributions",
		`SELECT count(*)
		 FROM resource_alias_contributions c
		 JOIN resource_alias_claims cl ON cl.id = c.claim_id
		 JOIN extension_resources er ON er.uid = c.source_extension_resource_uid
		 WHERE er.service_name = 'bench.fleetshift.io'
		   AND er.resource_id BETWEEN 'r-00084501' AND 'r-00084600'
		   AND cl.namespace = 'ext-id'
		   AND cl.key = 'reuse-id'
		   AND cl.value = 'reuse-' || substring(er.resource_id from 3)`,
		100,
	)
	assertCount(ctx, t, db, "full replace conflict new claims absent",
		`SELECT count(*) FROM resource_alias_claims WHERE namespace = 'ext-id' AND key = 'source-id' AND value LIKE 'full-conflict-%'`,
		0,
	)
	assertCount(ctx, t, db, "full replace conflict old claims remain",
		`SELECT count(*)
		 FROM generate_series(83001, 83100) AS g
		 JOIN resource_alias_claims cl
		   ON cl.namespace = 'ext-id'
		  AND cl.key = 'source-id'
		  AND cl.value = 'ext-' || lpad(g::text, 8, '0')`,
		100,
	)
	assertCount(ctx, t, db, "full replace conflict latest observations unchanged",
		`SELECT count(*)
		 FROM extension_resource_inventory inv
		 JOIN extension_resources er ON er.uid = inv.extension_resource_uid
		 WHERE er.service_name = 'bench.fleetshift.io'
		   AND er.resource_id BETWEEN 'r-00083001' AND 'r-00083100'
		   AND inv.observation @> '{"generation":1}'::jsonb`,
		100,
	)
	assertCount(ctx, t, db, "retracted alias claims",
		`SELECT count(*) FROM resource_alias_claims WHERE namespace = 'ext-id' AND key = 'source-id' AND platform_resource_id BETWEEN 'r-00059001' AND 'r-00060000'`,
		0,
	)
}

func detectProvider() testcontainers.ContainerCustomizer {
	if os.Getenv("TESTCONTAINERS_PROVIDER") == "docker" {
		return testcontainers.WithProvider(testcontainers.ProviderDefault)
	}
	if isPodmanAvailable() {
		return testcontainers.WithProvider(testcontainers.ProviderPodman)
	}
	return testcontainers.WithProvider(testcontainers.ProviderDefault)
}

func execSQL(ctx context.Context, db *sql.DB, query string) error {
	_, err := db.ExecContext(ctx, query)
	return err
}

func explain(ctx context.Context, db *sql.DB, query string) (string, error) {
	rows, err := db.QueryContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) "+query)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var out strings.Builder
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return "", err
		}
		out.WriteString(line)
		out.WriteByte('\n')
	}
	return out.String(), rows.Err()
}

func assertCount(ctx context.Context, t *testing.T, db *sql.DB, label, query string, want int) {
	t.Helper()

	var got int
	if err := db.QueryRowContext(ctx, query).Scan(&got); err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if got != want {
		t.Fatalf("%s: got %d, want %d", label, got, want)
	}
}

func summarizePlan(plan string) string {
	var out []string
	for _, line := range strings.Split(plan, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		switch {
		case strings.HasPrefix(trimmed, "Planning Time:"):
			out = append(out, trimmed)
		case strings.HasPrefix(trimmed, "Execution Time:"):
			out = append(out, trimmed)
		case strings.Contains(trimmed, "Insert on extension_resource_inventory"):
			out = append(out, line)
		case strings.Contains(trimmed, "Update on extension_resource_inventory"):
			out = append(out, line)
		case strings.Contains(trimmed, "Insert on resource_alias"):
			out = append(out, line)
		case strings.Contains(trimmed, "Insert on bench_acm_resources"):
			out = append(out, line)
		case strings.Contains(trimmed, "Update on resource_alias"):
			out = append(out, line)
		case strings.Contains(trimmed, "Delete on resource_alias"):
			out = append(out, line)
		case strings.Contains(trimmed, "Index Scan using extension_resources"):
			out = append(out, line)
		case strings.Contains(trimmed, "Index Scan using extension_resource_inventory"):
			out = append(out, line)
		case strings.Contains(trimmed, "Index Only Scan using resource_alias"):
			out = append(out, line)
		case strings.Contains(trimmed, "Index Scan using resource_alias"):
			out = append(out, line)
		case strings.Contains(trimmed, "Nested Loop"):
			out = append(out, line)
		case strings.Contains(trimmed, "Hash Join"):
			out = append(out, line)
		case strings.Contains(trimmed, "Merge Join"):
			out = append(out, line)
		}
	}
	if len(out) == 0 {
		return plan
	}
	return strings.Join(out, "\n")
}

type scenario struct {
	name string
	sql  string
}

var scenarios = []scenario{
	{
		name: "acm_batch_update_existing_single_statement",
		sql:  acmBatchUpdateExistingSQL(1_001, 2_000, 2),
	},
	{
		name: "replace_latest_same_no_aliases",
		sql:  replaceLatestSQL(1_001, 2_000, "bench.fleetshift.io", 1),
	},
	{
		name: "delta_heartbeat_existing_no_payload_changes",
		sql:  deltaHeartbeatSQL(2_001, 3_000, "bench.fleetshift.io"),
	},
	{
		name: "delta_heartbeat_upsert_existing_no_payload_changes",
		sql:  deltaHeartbeatUpsertSQL(7_001, 8_000, "bench.fleetshift.io"),
	},
	{
		name: "delta_patch_existing_labels_conditions_observation",
		sql:  deltaPatchExistingSQL(3_001, 4_000, "bench.fleetshift.io", 2),
	},
	{
		name: "replace_latest_changed_no_aliases",
		sql:  replaceLatestSQL(4_001, 5_000, "bench.fleetshift.io", 2),
	},
	{
		name: "replace_never_alias_empty_fingerprint_skip",
		sql:  replaceLatestWithEmptyAliasFingerprintSkipSQL(6_001, 7_000, "bench.fleetshift.io", 1),
	},
	{
		name: "replace_same_alias_fingerprint_skip",
		sql:  replaceLatestWithAliasFingerprintSkipSQL(15_001, 16_000, "bench.fleetshift.io", 1),
	},
	{
		name: "replace_same_alias_source_first_classification",
		sql:  replaceLatestWithSameAliasClassificationSQL(16_001, 17_000, "bench.fleetshift.io", 1),
	},
	{
		name: "replace_changed_state_and_new_alias_apply",
		sql:  replaceLatestWithNewAliasApplySQL(45_001, 46_000, "bench.fleetshift.io", 2),
	},
	{
		name: "replace_changed_state_and_alias_self_replace_in_place",
		sql:  replaceLatestWithAliasSelfReplaceSQL(67_001, 68_000, "bench.fleetshift.io", 2),
	},
	{
		name: "alias_self_replace_sibling_conflict",
		sql:  aliasSelfReplaceSiblingConflictSQL(82_001, 83_000, "bench.fleetshift.io", 2),
	},
	{
		name: "full_replace_mixed_success",
		sql:  fullReplaceMixedSuccessSQL(),
	},
	{
		name: "full_replace_sibling_conflict_write_gated",
		sql:  fullReplaceSiblingConflictSQL(),
	},
	{
		name: "replace_changed_state_and_retract_alias_direct_cleanup",
		sql:  replaceLatestWithAliasRetractSQL(59_001, 60_000, "bench.fleetshift.io", 2),
	},
}

func replaceLatestSQL(start, end int, serviceName string, generation int) string {
	return inputReportsSQL(start, end, serviceName, generation) + `
, upsert_inventory AS (
	INSERT INTO extension_resource_inventory (
		extension_resource_uid,
		observation,
		labels,
		conditions,
		observed_at,
		updated_at
	)
	SELECT
		source_uid,
		observation,
		labels,
		conditions,
		observed_at,
		received_at
	FROM input_reports
	ON CONFLICT (extension_resource_uid)
	DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = EXCLUDED.labels,
		conditions = EXCLUDED.conditions,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
)
SELECT count(*) FROM upsert_inventory`
}

func acmBatchUpdateExistingSQL(start, end int, generation int) string {
	return fmt.Sprintf(`WITH input_acm AS (
	SELECT
		('acm-' || lpad(g::text, 8, '0'))::text AS uid,
		('cluster-' || (g %% 500)::text)::text AS cluster,
		jsonb_build_object(
			'kind', (ARRAY['Pod', 'Deployment', 'ConfigMap', 'Service', 'Secret', 'ReplicaSet'])[1 + (g %% 6)],
			'namespace', (ARRAY['default', 'kube-system', 'openshift-monitoring', 'app-team-a', 'app-team-b'])[1 + (g %% 5)],
			'name', 'resource-' || g::text,
			'apigroup', CASE WHEN g %% 6 = 0 THEN 'apps' ELSE '' END,
			'kind_plural', lower((ARRAY['pods', 'deployments', 'configmaps', 'services', 'secrets', 'replicasets'])[1 + (g %% 6)]),
			'_hubClusterResource', false,
			'generation', %[3]d,
			'payload', jsonb_build_object(
				'cpu', (g %% 16),
				'memoryGiB', 16 + (g %% 128),
				'zone', 'zone-' || (g %% 16)::text
			)
		) AS data
	FROM generate_series(%[1]d, %[2]d) AS g
),
upsert_acm AS (
	INSERT INTO bench_acm_resources AS r (uid, cluster, data)
	SELECT uid, cluster, data
	FROM input_acm
	ON CONFLICT (uid)
	DO UPDATE SET data = EXCLUDED.data
	WHERE r.data IS DISTINCT FROM EXCLUDED.data
	RETURNING 1
)
SELECT count(*) FROM upsert_acm`, start, end, generation)
}

func deltaHeartbeatSQL(start, end int, serviceName string) string {
	return fmt.Sprintf(`WITH input_heartbeats AS (
	SELECT
		er.uid AS source_uid,
		('2026-01-01T00:00:00Z'::timestamptz + make_interval(secs => g)) AS observed_at,
		clock_timestamp() AS received_at
	FROM generate_series(%[1]d, %[2]d) AS g
	JOIN extension_resources er
	  ON er.service_name = %[3]s
	 AND er.collection_name = 'widgets'
	 AND er.resource_id = 'r-' || lpad(g::text, 8, '0')
),
updated_inventory AS (
	UPDATE extension_resource_inventory inv
	SET observed_at = ih.observed_at,
	    updated_at = ih.received_at
	FROM input_heartbeats ih
	WHERE inv.extension_resource_uid = ih.source_uid
	RETURNING 1
)
SELECT count(*) FROM updated_inventory`, start, end, sqlQuote(serviceName))
}

func deltaHeartbeatUpsertSQL(start, end int, serviceName string) string {
	return fmt.Sprintf(`WITH input_heartbeats AS (
	SELECT
		er.uid AS source_uid,
		('2026-01-01T00:00:00Z'::timestamptz + make_interval(secs => g)) AS observed_at,
		clock_timestamp() AS received_at
	FROM generate_series(%[1]d, %[2]d) AS g
	JOIN extension_resources er
	  ON er.service_name = %[3]s
	 AND er.collection_name = 'widgets'
	 AND er.resource_id = 'r-' || lpad(g::text, 8, '0')
),
upsert_inventory AS (
	INSERT INTO extension_resource_inventory (
		extension_resource_uid,
		labels,
		conditions,
		observed_at,
		updated_at
	)
	SELECT source_uid, '{}'::jsonb, '{}'::jsonb, observed_at, received_at
	FROM input_heartbeats
	ON CONFLICT (extension_resource_uid)
	DO UPDATE SET
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
)
SELECT count(*) FROM upsert_inventory`, start, end, sqlQuote(serviceName))
}

func deltaPatchExistingSQL(start, end int, serviceName string, generation int) string {
	return fmt.Sprintf(`WITH input_patches AS (
	SELECT
		er.uid AS source_uid,
		%[5]s AS observation,
		jsonb_build_object(
			'patched', 'true',
			'generation', %[4]d::text
		) AS set_labels,
		ARRAY['owner']::text[] AS delete_label_keys,
		jsonb_build_object(
			'Ready', jsonb_build_object(
				'status', CASE WHEN g %% 10 = 0 THEN 'False' ELSE 'True' END,
				'reason', CASE WHEN g %% 10 = 0 THEN 'PatchFailed' ELSE 'Patched' END,
				'message', CASE WHEN g %% 10 = 0 THEN 'patched with warning' ELSE 'patched' END,
				'lastTransitionTime', '2026-01-02T00:00:00Z'
			)
		) AS upsert_conditions,
		ARRAY['Synced']::text[] AS delete_condition_types,
		('2026-01-01T00:00:00Z'::timestamptz + make_interval(secs => g)) AS observed_at,
		clock_timestamp() AS received_at
	FROM generate_series(%[1]d, %[2]d) AS g
	JOIN extension_resources er
	  ON er.service_name = %[3]s
	 AND er.collection_name = 'widgets'
	 AND er.resource_id = 'r-' || lpad(g::text, 8, '0')
),
updated_inventory AS (
	UPDATE extension_resource_inventory inv
	SET observation = COALESCE(p.observation, inv.observation),
	    labels = (inv.labels - p.delete_label_keys) || p.set_labels,
	    conditions = (inv.conditions - p.delete_condition_types) || p.upsert_conditions,
	    observed_at = p.observed_at,
	    updated_at = p.received_at
	FROM input_patches p
	WHERE inv.extension_resource_uid = p.source_uid
	RETURNING 1
)
SELECT count(*) FROM updated_inventory`, start, end, sqlQuote(serviceName), generation, observationExpr(generation))
}

func replaceLatestWithEmptyAliasFingerprintSkipSQL(start, end int, serviceName string, generation int) string {
	return inputReportsWithoutAliasesSQL(start, end, serviceName, generation) + `
, upsert_inventory AS (
	INSERT INTO extension_resource_inventory (
		extension_resource_uid,
		observation,
		labels,
		conditions,
		observed_at,
		updated_at
	)
	SELECT source_uid, observation, labels, conditions, observed_at, received_at
	FROM input_reports
	ON CONFLICT (extension_resource_uid)
	DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = EXCLUDED.labels,
		conditions = EXCLUDED.conditions,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
),
needs_alias_processing AS (
	SELECT source_uid
	FROM input_reports
	WHERE reported_alias_fingerprint IS DISTINCT FROM stored_alias_fingerprint
)
SELECT
	(SELECT count(*) FROM upsert_inventory) AS updated_inventory,
	(SELECT count(*) FROM needs_alias_processing) AS aliases_to_process`
}

func replaceLatestWithAliasFingerprintSkipSQL(start, end int, serviceName string, generation int) string {
	return inputReportsSQL(start, end, serviceName, generation) + `
, upsert_inventory AS (
	INSERT INTO extension_resource_inventory (
		extension_resource_uid,
		observation,
		labels,
		conditions,
		observed_at,
		updated_at
	)
	SELECT source_uid, observation, labels, conditions, observed_at, received_at
	FROM input_reports
	ON CONFLICT (extension_resource_uid)
	DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = EXCLUDED.labels,
		conditions = EXCLUDED.conditions,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
),
needs_alias_processing AS (
	SELECT source_uid
	FROM input_reports
	WHERE reported_alias_fingerprint IS DISTINCT FROM stored_alias_fingerprint
)
SELECT
	(SELECT count(*) FROM upsert_inventory) AS updated_inventory,
	(SELECT count(*) FROM needs_alias_processing) AS aliases_to_process`
}

func replaceLatestWithSameAliasClassificationSQL(start, end int, serviceName string, generation int) string {
	return inputReportsSQL(start, end, serviceName, generation) + `
, upsert_inventory AS (
	INSERT INTO extension_resource_inventory (
		extension_resource_uid,
		observation,
		labels,
		conditions,
		observed_at,
		updated_at
	)
	SELECT source_uid, observation, labels, conditions, observed_at, received_at
	FROM input_reports
	ON CONFLICT (extension_resource_uid)
	DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = EXCLUDED.labels,
		conditions = EXCLUDED.conditions,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
),
self_claim AS (
	SELECT ir.*, c.claim_id, cl.value AS existing_value, cl.platform_collection_name, cl.platform_resource_id
	FROM input_reports ir
	LEFT JOIN LATERAL (
		SELECT claim_id
		FROM resource_alias_contributions
		WHERE source_extension_resource_uid = ir.source_uid
		  AND namespace = ir.alias_namespace AND key = ir.alias_key
		LIMIT 1 OFFSET 0
	) c ON true
	LEFT JOIN LATERAL (
		SELECT value, platform_collection_name, platform_resource_id
		FROM resource_alias_claims
		WHERE id = c.claim_id
		LIMIT 1 OFFSET 0
	) cl ON true
),
changed_aliases AS (
	SELECT *
	FROM self_claim
	WHERE claim_id IS NULL
	   OR existing_value IS DISTINCT FROM alias_value
	   OR platform_collection_name IS DISTINCT FROM collection_name
	   OR platform_resource_id IS DISTINCT FROM resource_id
)
SELECT
	(SELECT count(*) FROM upsert_inventory) AS updated_inventory,
	(SELECT count(*) FROM changed_aliases) AS aliases_changed`
}

func replaceLatestWithNewAliasApplySQL(start, end int, serviceName string, generation int) string {
	return inputReportsSQL(start, end, serviceName, generation) + `
, input_aliases AS (
	SELECT
		source_uid,
		alias_namespace AS namespace,
		'secondary-id'::text AS key,
		('new-secondary-apply-' || lpad(idx::text, 8, '0'))::text AS value,
		collection_name,
		resource_id
	FROM input_reports
),
upsert_inventory AS (
	INSERT INTO extension_resource_inventory (
		extension_resource_uid,
		observation,
		labels,
		conditions,
		observed_at,
		updated_at
	)
	SELECT source_uid, observation, labels, conditions, observed_at, received_at
	FROM input_reports
	ON CONFLICT (extension_resource_uid)
	DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = EXCLUDED.labels,
		conditions = EXCLUDED.conditions,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
),
inserted_claims AS (
	INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id)
	SELECT namespace, key, value, collection_name, resource_id
	FROM input_aliases
	ON CONFLICT (namespace, key, value) DO UPDATE SET
		platform_collection_name = resource_alias_claims.platform_collection_name
	RETURNING id, namespace, key, value
),
upserted_contributions AS (
	INSERT INTO resource_alias_contributions (source_extension_resource_uid, namespace, key, claim_id)
	SELECT ia.source_uid, ia.namespace, ia.key, ic.id
	FROM input_aliases ia
	JOIN inserted_claims ic
	  ON ic.namespace = ia.namespace AND ic.key = ia.key AND ic.value = ia.value
	ON CONFLICT (source_extension_resource_uid, namespace, key)
	DO UPDATE SET claim_id = EXCLUDED.claim_id
	RETURNING 1
)
SELECT
	(SELECT count(*) FROM upsert_inventory) AS updated_inventory,
	(SELECT count(*) FROM upserted_contributions) AS upserted_contributions`
}

func replaceLatestWithAliasSelfReplaceSQL(start, end int, serviceName string, generation int) string {
	return inputReportsWithAliasValuePrefixSQL(start, end, serviceName, generation, "ext-changed-apply") + `
, upsert_inventory AS (
	INSERT INTO extension_resource_inventory (
		extension_resource_uid,
		observation,
		labels,
		conditions,
		observed_at,
		updated_at
	)
	SELECT source_uid, observation, labels, conditions, observed_at, received_at
	FROM input_reports
	ON CONFLICT (extension_resource_uid)
	DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = EXCLUDED.labels,
		conditions = EXCLUDED.conditions,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
),
self_claim AS (
	SELECT ir.*, c.claim_id
	FROM input_reports ir
	JOIN LATERAL (
		SELECT claim_id
		FROM resource_alias_contributions
		WHERE source_extension_resource_uid = ir.source_uid
		  AND namespace = ir.alias_namespace
		  AND key = ir.alias_key
		LIMIT 1 OFFSET 0
	) c ON true
),
safe_replace AS (
	SELECT sc.*
	FROM self_claim sc
	LEFT JOIN LATERAL (
		SELECT 1 AS found
		FROM resource_alias_contributions other
		WHERE other.claim_id = sc.claim_id
		  AND other.source_extension_resource_uid <> sc.source_uid
		LIMIT 1 OFFSET 0
	) sibling ON true
	WHERE sibling.found IS NULL
),
updated_claims AS (
	UPDATE resource_alias_claims cl
	SET value = sr.alias_value
	FROM safe_replace sr
	WHERE cl.id = sr.claim_id
	RETURNING 1
)
SELECT
	(SELECT count(*) FROM upsert_inventory) AS updated_inventory,
	(SELECT count(*) FROM updated_claims) AS updated_claims`
}

func aliasSelfReplaceSiblingConflictSQL(start, end int, serviceName string, generation int) string {
	return inputReportsWithAliasValuePrefixSQL(start, end, serviceName, generation, "ext-conflict") + `
, self_claim AS (
	SELECT ir.*, c.claim_id, cl.value AS self_value
	FROM input_reports ir
	JOIN LATERAL (
		SELECT claim_id
		FROM resource_alias_contributions
		WHERE source_extension_resource_uid = ir.source_uid
		  AND namespace = ir.alias_namespace
		  AND key = ir.alias_key
		LIMIT 1 OFFSET 0
	) c ON true
	JOIN LATERAL (
		SELECT value
		FROM resource_alias_claims
		WHERE id = c.claim_id
		LIMIT 1 OFFSET 0
	) cl ON true
),
sibling_state AS (
	SELECT sc.*, sibling.found IS NOT NULL AS sibling_holds
	FROM self_claim sc
	LEFT JOIN LATERAL (
		SELECT 1 AS found
		FROM resource_alias_contributions other
		WHERE other.claim_id = sc.claim_id
		  AND other.source_extension_resource_uid <> sc.source_uid
		LIMIT 1 OFFSET 0
	) sibling ON true
),
resource_conflicts AS (
	SELECT *
	FROM sibling_state
	WHERE self_value IS DISTINCT FROM alias_value
	  AND sibling_holds
),
safe_replace AS (
	SELECT *
	FROM sibling_state
	WHERE self_value IS DISTINCT FROM alias_value
	  AND NOT sibling_holds
),
updated_claims AS (
	UPDATE resource_alias_claims cl
	SET value = sr.alias_value
	FROM safe_replace sr
	WHERE cl.id = sr.claim_id
	RETURNING 1
)
SELECT
	(SELECT count(*) FROM resource_conflicts) AS resource_conflicts,
	(SELECT count(*) FROM updated_claims) AS updated_claims`
}

func replaceLatestWithAliasRetractSQL(start, end int, serviceName string, generation int) string {
	return inputReportsSQL(start, end, serviceName, generation) + `
, upsert_inventory AS (
	INSERT INTO extension_resource_inventory (
		extension_resource_uid,
		observation,
		labels,
		conditions,
		observed_at,
		updated_at
	)
	SELECT source_uid, observation, labels, conditions, observed_at, received_at
	FROM input_reports
	ON CONFLICT (extension_resource_uid)
	DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = EXCLUDED.labels,
		conditions = EXCLUDED.conditions,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
),
deleted_contributions AS (
	DELETE FROM resource_alias_contributions c
	USING input_reports ir
	WHERE c.source_extension_resource_uid = ir.source_uid
	  AND c.namespace = ir.alias_namespace
	  AND c.key = ir.alias_key
	RETURNING c.claim_id
),
deleted_ref_counts AS MATERIALIZED (
	SELECT claim_id, count(*)::bigint AS deleted_refs
	FROM deleted_contributions
	GROUP BY claim_id
),
orphan_claim_ids AS MATERIALIZED (
	SELECT drc.claim_id
	FROM deleted_ref_counts drc
	JOIN LATERAL (
		SELECT count(*)::bigint AS baseline_ct
		FROM resource_alias_contributions c
		WHERE c.claim_id = drc.claim_id
	) cc ON true
	WHERE cc.baseline_ct = drc.deleted_refs
),
deleted_orphan_claims AS (
	DELETE FROM resource_alias_claims cl
	USING orphan_claim_ids orphaned
	WHERE cl.id = orphaned.claim_id
	  AND NOT cl.platform_owned
	RETURNING 1
)
SELECT
	(SELECT count(*) FROM upsert_inventory) AS updated_inventory,
	(SELECT count(*) FROM deleted_contributions) AS deleted_contributions,
	(SELECT count(*) FROM deleted_orphan_claims) AS deleted_claims`
}

func fullReplaceMixedSuccessSQL() string {
	return fullReplaceCoreSQL(`WITH input_reports AS MATERIALIZED (
	SELECT
		row_number() OVER (ORDER BY g)::int AS idx,
		g,
		er.uid AS source_uid,
		er.alias_fingerprint AS stored_alias_fingerprint,
		CASE
			WHEN g BETWEEN 84201 AND 84300 THEN digest(('source-id=ext-' || lpad(g::text, 8, '0') || ';secondary-id=full-secondary-' || lpad((g - 84200)::text, 8, '0'))::bytea, 'sha256')
			WHEN g BETWEEN 84301 AND 84400 THEN digest(('source-id=full-replaced-' || lpad((g - 84300)::text, 8, '0'))::bytea, 'sha256')
			WHEN g BETWEEN 84401 AND 84500 THEN digest(''::bytea, 'sha256')
			WHEN g BETWEEN 84501 AND 84600 THEN digest(('source-id=ext-' || lpad(g::text, 8, '0') || ';reuse-id=reuse-' || lpad(g::text, 8, '0'))::bytea, 'sha256')
			ELSE digest(('source-id=ext-' || lpad(g::text, 8, '0'))::bytea, 'sha256')
		END AS reported_alias_fingerprint,
		'widgets'::text AS collection_name,
		('r-' || lpad(g::text, 8, '0'))::text AS resource_id,
		jsonb_build_object(
			'generation', 3,
			'payload', jsonb_build_object(
				'cpu', (g % 16),
				'memoryGiB', 16 + (g % 128),
				'zone', 'zone-' || (g % 16)::text
			)
		) AS observation,
		jsonb_build_object(
			'env', CASE WHEN g % 2 = 0 THEN 'prod' ELSE 'dev' END,
			'region', 'region-' || (g % 10)::text,
			'owner', 'team-' || (g % 20)::text,
			'shard', (g % 64)::text,
			'generation', '3'
		) AS labels,
		jsonb_build_object(
			'Ready', jsonb_build_object(
				'status', 'True',
				'reason', 'FullReplace',
				'message', 'full replace mixed success',
				'lastTransitionTime', '2026-01-03T00:00:00Z'
			)
		) AS conditions,
		('2026-01-03T00:00:00Z'::timestamptz + make_interval(secs => g)) AS observed_at,
		clock_timestamp() AS received_at
	FROM generate_series(84001, 84600) AS g
	JOIN extension_resources er
	  ON er.service_name = 'bench.fleetshift.io'
	 AND er.collection_name = 'widgets'
	 AND er.resource_id = 'r-' || lpad(g::text, 8, '0')
),
reported_aliases AS MATERIALIZED (
	SELECT
		idx,
		source_uid,
		'ext-id'::text AS namespace,
		'source-id'::text AS key,
		CASE
			WHEN g BETWEEN 84301 AND 84400 THEN 'full-replaced-' || lpad((g - 84300)::text, 8, '0')
			ELSE 'ext-' || lpad(g::text, 8, '0')
		END AS value,
		collection_name,
		resource_id,
		received_at
	FROM input_reports
	WHERE g NOT BETWEEN 84401 AND 84500
	UNION ALL
	SELECT
		idx,
		source_uid,
		'ext-id',
		'secondary-id',
		'full-secondary-' || lpad((g - 84200)::text, 8, '0'),
		collection_name,
		resource_id,
		received_at
	FROM input_reports
	WHERE g BETWEEN 84201 AND 84300
	UNION ALL
	SELECT
		idx,
		source_uid,
		'ext-id',
		'reuse-id',
		'reuse-' || lpad(g::text, 8, '0'),
		collection_name,
		resource_id,
		received_at
	FROM input_reports
	WHERE g BETWEEN 84501 AND 84600
)`)
}

func fullReplaceSiblingConflictSQL() string {
	return fullReplaceCoreSQL(`WITH input_reports AS MATERIALIZED (
	SELECT
		row_number() OVER (ORDER BY g)::int AS idx,
		g,
		er.uid AS source_uid,
		er.alias_fingerprint AS stored_alias_fingerprint,
		digest(('source-id=full-conflict-' || lpad((g - 83000)::text, 8, '0'))::bytea, 'sha256') AS reported_alias_fingerprint,
		'widgets'::text AS collection_name,
		('r-' || lpad(g::text, 8, '0'))::text AS resource_id,
		jsonb_build_object('generation', 4, 'payload', jsonb_build_object('conflict', true)) AS observation,
		jsonb_build_object('generation', '4', 'conflict', 'true') AS labels,
		jsonb_build_object(
			'Ready', jsonb_build_object(
				'status', 'False',
				'reason', 'AliasConflict',
				'message', 'should not be written',
				'lastTransitionTime', '2026-01-04T00:00:00Z'
			)
		) AS conditions,
		('2026-01-04T00:00:00Z'::timestamptz + make_interval(secs => g)) AS observed_at,
		clock_timestamp() AS received_at
	FROM generate_series(83001, 83100) AS g
	JOIN extension_resources er
	  ON er.service_name = 'bench.fleetshift.io'
	 AND er.collection_name = 'widgets'
	 AND er.resource_id = 'r-' || lpad(g::text, 8, '0')
),
reported_aliases AS MATERIALIZED (
	SELECT
		idx,
		source_uid,
		'ext-id'::text AS namespace,
		'source-id'::text AS key,
		'full-conflict-' || lpad((g - 83000)::text, 8, '0') AS value,
		collection_name,
		resource_id,
		received_at
	FROM input_reports
)`)
}

func fullReplaceCoreSQL(inputCTEs string) string {
	return inputCTEs + `
, needs_alias_processing AS MATERIALIZED (
	SELECT *
	FROM input_reports
	WHERE reported_alias_fingerprint IS DISTINCT FROM stored_alias_fingerprint
),
input_aliases AS MATERIALIZED (
	SELECT ra.*
	FROM reported_aliases ra
	JOIN needs_alias_processing nr ON nr.idx = ra.idx
),
self_claim AS (
	SELECT ia.idx, ia.namespace, ia.key, ia.value, ia.collection_name, ia.resource_id, ia.received_at, ia.source_uid,
	       c.claim_id AS self_claim_id, cl.value AS self_value,
	       cl.platform_collection_name AS self_collection_name, cl.platform_resource_id AS self_resource_id
	FROM input_aliases ia
	LEFT JOIN LATERAL (
		SELECT claim_id
		FROM resource_alias_contributions
		WHERE source_extension_resource_uid = ia.source_uid
		  AND namespace = ia.namespace
		  AND key = ia.key
		LIMIT 1 OFFSET 0
	) c ON true
	LEFT JOIN LATERAL (
		SELECT value, platform_collection_name, platform_resource_id
		FROM resource_alias_claims
		WHERE id = c.claim_id
		LIMIT 1 OFFSET 0
	) cl ON true
),
changed AS (
	SELECT idx, namespace, key, value, collection_name, resource_id, received_at, source_uid, self_claim_id
	FROM self_claim
	WHERE self_claim_id IS NULL
	   OR self_value IS DISTINCT FROM value
	   OR self_collection_name IS DISTINCT FROM collection_name
	   OR self_resource_id IS DISTINCT FROM resource_id
),
by_value AS (
	SELECT ch.idx, vc.id AS value_claim_id, vc.platform_collection_name AS value_collection_name, vc.platform_resource_id AS value_resource_id
	FROM changed ch
	LEFT JOIN LATERAL (
		SELECT id, platform_collection_name, platform_resource_id
		FROM resource_alias_claims
		WHERE namespace = ch.namespace AND key = ch.key AND value = ch.value
		LIMIT 1 OFFSET 0
	) vc ON true
),
by_resource AS (
	SELECT ch.idx, rc.id AS resource_claim_id, rc.value AS resource_value, rc.platform_owned AS resource_platform_owned
	FROM changed ch
	LEFT JOIN LATERAL (
		SELECT id, value, platform_owned
		FROM resource_alias_claims
		WHERE namespace = ch.namespace AND key = ch.key
		  AND platform_collection_name = ch.collection_name
		  AND platform_resource_id = ch.resource_id
		LIMIT 1 OFFSET 0
	) rc ON true
),
sibling AS (
	SELECT br.idx,
	       br.resource_claim_id IS NOT NULL
	       AND (
		 br.resource_platform_owned
		 OR EXISTS (
			SELECT 1
			FROM resource_alias_contributions other
			JOIN changed ch ON ch.idx = br.idx
			WHERE other.claim_id = br.resource_claim_id
			  AND other.source_extension_resource_uid <> ch.source_uid
		 )
	       ) AS sibling_holds
	FROM by_resource br
),
alias_value_conflicts AS (
	SELECT ch.idx, ch.namespace, ch.key, ch.value,
	       bv.value_collection_name AS actual_collection_name, bv.value_resource_id AS actual_resource_id
	FROM changed ch
	JOIN by_value bv ON bv.idx = ch.idx
	WHERE bv.value_claim_id IS NOT NULL
	  AND (bv.value_collection_name <> ch.collection_name OR bv.value_resource_id <> ch.resource_id)
),
alias_resource_conflicts AS (
	SELECT ch.idx, ch.namespace, ch.key, ch.value, br.resource_value AS existing_value
	FROM changed ch
	JOIN by_value bv ON bv.idx = ch.idx
	JOIN by_resource br ON br.idx = ch.idx
	JOIN sibling s ON s.idx = ch.idx
	WHERE bv.value_claim_id IS NULL
	  AND br.resource_claim_id IS NOT NULL
	  AND s.sibling_holds
),
conflict_state AS MATERIALIZED (
	SELECT EXISTS (
		SELECT 1 FROM alias_value_conflicts
		UNION ALL
		SELECT 1 FROM alias_resource_conflicts
	) AS has_conflicts
),
safe AS (
	SELECT ch.idx, ch.namespace, ch.key, ch.value, ch.collection_name, ch.resource_id, ch.received_at, ch.source_uid, ch.self_claim_id,
	       bv.value_claim_id, br.resource_claim_id
	FROM changed ch
	JOIN by_value bv ON bv.idx = ch.idx
	JOIN by_resource br ON br.idx = ch.idx
	JOIN sibling s ON s.idx = ch.idx
	WHERE NOT (bv.value_claim_id IS NOT NULL AND (bv.value_collection_name <> ch.collection_name OR bv.value_resource_id <> ch.resource_id))
	  AND NOT (bv.value_claim_id IS NULL AND br.resource_claim_id IS NOT NULL AND s.sibling_holds)
),
claim_creates AS (
	SELECT namespace, key, value, collection_name, resource_id, received_at, source_uid
	FROM safe
	WHERE value_claim_id IS NULL
	  AND resource_claim_id IS NULL
),
claim_self_replace AS (
	SELECT value, received_at, resource_claim_id
	FROM safe
	WHERE value_claim_id IS NULL
	  AND resource_claim_id IS NOT NULL
	  AND self_claim_id = resource_claim_id
),
claim_reuse AS (
	SELECT namespace, key, received_at, source_uid, value_claim_id
	FROM safe
	WHERE value_claim_id IS NOT NULL
	  AND self_claim_id IS NULL
),
upsert_inventory AS (
	INSERT INTO extension_resource_inventory (
		extension_resource_uid,
		observation,
		labels,
		conditions,
		observed_at,
		updated_at
	)
	SELECT source_uid, observation, labels, conditions, observed_at, received_at
	FROM input_reports
	WHERE NOT (SELECT has_conflicts FROM conflict_state)
	ON CONFLICT (extension_resource_uid)
	DO UPDATE SET
		observation = COALESCE(EXCLUDED.observation, extension_resource_inventory.observation),
		labels = EXCLUDED.labels,
		conditions = EXCLUDED.conditions,
		observed_at = EXCLUDED.observed_at,
		updated_at = EXCLUDED.updated_at
	RETURNING 1
),
inserted_claims AS (
	INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id, created_at)
	SELECT DISTINCT namespace, key, value, collection_name, resource_id, received_at
	FROM claim_creates
	WHERE NOT (SELECT has_conflicts FROM conflict_state)
	RETURNING id, namespace, key, value
),
updated_claims AS (
	UPDATE resource_alias_claims cl
	SET value = sr.value
	FROM claim_self_replace sr
	WHERE cl.id = sr.resource_claim_id
	  AND NOT (SELECT has_conflicts FROM conflict_state)
	RETURNING 1
),
claim_targets AS (
	SELECT namespace, key, received_at, source_uid, value_claim_id AS claim_id
	FROM claim_reuse
	UNION ALL
	SELECT cc.namespace, cc.key, cc.received_at, cc.source_uid, ic.id AS claim_id
	FROM claim_creates cc
	JOIN inserted_claims ic ON ic.namespace = cc.namespace AND ic.key = cc.key AND ic.value = cc.value
),
upserted_contributions AS (
	INSERT INTO resource_alias_contributions (source_extension_resource_uid, namespace, key, claim_id, created_at)
	SELECT DISTINCT ON (source_uid, namespace, key) source_uid, namespace, key, claim_id, received_at
	FROM claim_targets
	WHERE NOT (SELECT has_conflicts FROM conflict_state)
	ORDER BY source_uid, namespace, key
	ON CONFLICT (source_extension_resource_uid, namespace, key)
	DO UPDATE SET claim_id = EXCLUDED.claim_id
	RETURNING claim_id
),
del_aliases_absent AS (
	DELETE FROM resource_alias_contributions c
	USING needs_alias_processing nr
	WHERE c.source_extension_resource_uid = nr.source_uid
	  AND NOT (SELECT has_conflicts FROM conflict_state)
	  AND NOT EXISTS (
		SELECT 1
		FROM reported_aliases ra
		WHERE ra.source_uid = c.source_extension_resource_uid
		  AND ra.namespace = c.namespace
		  AND ra.key = c.key
	  )
	RETURNING c.claim_id
),
touched_claims AS (
	SELECT DISTINCT claim_id FROM del_aliases_absent
),
baseline_contrib_counts AS (
	SELECT tc.claim_id, cc.baseline_ct
	FROM touched_claims tc
	JOIN LATERAL (
		SELECT count(*)::bigint AS baseline_ct
		FROM resource_alias_contributions c
		WHERE c.claim_id = tc.claim_id
	) cc ON true
),
refcount_deltas AS (
	SELECT claim_id, -count(*)::bigint AS delta_refs
	FROM del_aliases_absent
	GROUP BY claim_id
	UNION ALL
	SELECT claim_id, count(*)::bigint AS delta_refs
	FROM upserted_contributions
	GROUP BY claim_id
),
net_refcount_deltas AS (
	SELECT claim_id, sum(delta_refs)::bigint AS delta_refs
	FROM refcount_deltas
	GROUP BY claim_id
),
remaining_refs AS (
	SELECT tc.claim_id,
	       COALESCE(bcc.baseline_ct, 0) + COALESCE(nrd.delta_refs, 0) AS net_refs
	FROM touched_claims tc
	LEFT JOIN baseline_contrib_counts bcc ON bcc.claim_id = tc.claim_id
	LEFT JOIN net_refcount_deltas nrd ON nrd.claim_id = tc.claim_id
),
deleted_orphan_claims AS (
	DELETE FROM resource_alias_claims cl
	USING remaining_refs rr
	WHERE cl.id = rr.claim_id
	  AND rr.net_refs = 0
	  AND NOT cl.platform_owned
	  AND NOT (SELECT has_conflicts FROM conflict_state)
	RETURNING 1
),
updated_fingerprints AS (
	UPDATE extension_resources er
	SET alias_fingerprint = nr.reported_alias_fingerprint,
	    updated_at = nr.received_at
	FROM needs_alias_processing nr
	WHERE er.uid = nr.source_uid
	  AND NOT (SELECT has_conflicts FROM conflict_state)
	RETURNING 1
)
SELECT
	(SELECT count(*) FROM alias_value_conflicts) AS value_conflicts,
	(SELECT count(*) FROM alias_resource_conflicts) AS resource_conflicts,
	(SELECT count(*) FROM upsert_inventory) AS updated_inventory,
	(SELECT count(*) FROM inserted_claims) AS inserted_claims,
	(SELECT count(*) FROM updated_claims) AS updated_claims,
	(SELECT count(*) FROM upserted_contributions) AS upserted_contributions,
	(SELECT count(*) FROM del_aliases_absent) AS deleted_contributions,
	(SELECT count(*) FROM deleted_orphan_claims) AS deleted_claims,
	(SELECT count(*) FROM updated_fingerprints) AS updated_fingerprints`
}

func inputReportsSQL(start, end int, serviceName string, generation int) string {
	return fmt.Sprintf(`WITH input_reports AS (
	SELECT
		row_number() OVER ()::int AS idx,
		er.uid AS source_uid,
		er.alias_fingerprint AS stored_alias_fingerprint,
		digest(('source-id=ext-' || lpad(g::text, 8, '0'))::bytea, 'sha256') AS reported_alias_fingerprint,
		'widgets'::text AS collection_name,
		('r-' || lpad(g::text, 8, '0'))::text AS resource_id,
		%[5]s AS observation,
		%[6]s AS labels,
		%[7]s AS conditions,
		'ext-id'::text AS alias_namespace,
		'source-id'::text AS alias_key,
		('ext-' || lpad(g::text, 8, '0'))::text AS alias_value,
		('2026-01-01T00:00:00Z'::timestamptz + make_interval(secs => g)) AS observed_at,
		clock_timestamp() AS received_at
	FROM generate_series(%[1]d, %[2]d) AS g
	JOIN extension_resources er
	  ON er.service_name = %[3]s
	 AND er.collection_name = 'widgets'
	 AND er.resource_id = 'r-' || lpad(g::text, 8, '0')
)`, start, end, sqlQuote(serviceName), generation, observationExpr(generation), labelsExpr(generation), conditionsExpr(generation))
}

func inputReportsWithAliasValuePrefixSQL(start, end int, serviceName string, generation int, valuePrefix string) string {
	valueExpr := fmt.Sprintf("(%s || '-' || lpad((g - %d + 1)::text, 8, '0'))", sqlQuote(valuePrefix), start)
	return fmt.Sprintf(`WITH input_reports AS (
	SELECT
		row_number() OVER ()::int AS idx,
		er.uid AS source_uid,
		er.alias_fingerprint AS stored_alias_fingerprint,
		digest((%[8]s)::bytea, 'sha256') AS reported_alias_fingerprint,
		'widgets'::text AS collection_name,
		('r-' || lpad(g::text, 8, '0'))::text AS resource_id,
		%[5]s AS observation,
		%[6]s AS labels,
		%[7]s AS conditions,
		'ext-id'::text AS alias_namespace,
		'source-id'::text AS alias_key,
		%[8]s::text AS alias_value,
		('2026-01-01T00:00:00Z'::timestamptz + make_interval(secs => g)) AS observed_at,
		clock_timestamp() AS received_at
	FROM generate_series(%[1]d, %[2]d) AS g
	JOIN extension_resources er
	  ON er.service_name = %[3]s
	 AND er.collection_name = 'widgets'
	 AND er.resource_id = 'r-' || lpad(g::text, 8, '0')
)`, start, end, sqlQuote(serviceName), generation, observationExpr(generation), labelsExpr(generation), conditionsExpr(generation), valueExpr)
}

func inputReportsWithoutAliasesSQL(start, end int, serviceName string, generation int) string {
	return fmt.Sprintf(`WITH input_reports AS (
	SELECT
		row_number() OVER ()::int AS idx,
		er.uid AS source_uid,
		er.alias_fingerprint AS stored_alias_fingerprint,
		digest(''::bytea, 'sha256') AS reported_alias_fingerprint,
		'widgets'::text AS collection_name,
		('r-' || lpad(g::text, 8, '0'))::text AS resource_id,
		%[5]s AS observation,
		%[6]s AS labels,
		%[7]s AS conditions,
		NULL::text AS alias_namespace,
		NULL::text AS alias_key,
		NULL::text AS alias_value,
		('2026-01-01T00:00:00Z'::timestamptz + make_interval(secs => g)) AS observed_at,
		clock_timestamp() AS received_at
	FROM generate_series(%[1]d, %[2]d) AS g
	JOIN extension_resources er
	  ON er.service_name = %[3]s
	 AND er.collection_name = 'widgets'
	 AND er.resource_id = 'r-' || lpad(g::text, 8, '0')
)`, start, end, sqlQuote(serviceName), generation, observationExpr(generation), labelsExpr(generation), conditionsExpr(generation))
}

func observationExpr(generation int) string {
	return fmt.Sprintf(`jsonb_build_object(
			'generation', %d,
			'payload', jsonb_build_object(
				'cpu', (g %% 16),
				'memoryGiB', 16 + (g %% 128),
				'zone', 'zone-' || (g %% 16)::text
			)
		)`, generation)
}

func labelsExpr(generation int) string {
	return fmt.Sprintf(`jsonb_build_object(
			'env', CASE WHEN g %% 2 = 0 THEN 'prod' ELSE 'dev' END,
			'region', 'region-' || (g %% 10)::text,
			'owner', 'team-' || (g %% 20)::text,
			'shard', (g %% 64)::text,
			'generation', %d::text
		)`, generation)
}

func conditionsExpr(generation int) string {
	return fmt.Sprintf(`jsonb_build_object(
			'Ready', jsonb_build_object(
				'status', CASE WHEN g %% 17 = 0 THEN 'False' ELSE 'True' END,
				'reason', CASE WHEN g %% 17 = 0 THEN 'Reconciling' ELSE 'Ready' END,
				'message', CASE WHEN g %% 17 = 0 THEN 'waiting for dependency' ELSE 'ready' END,
				'lastTransitionTime', '2026-01-01T00:00:00Z'
			),
			'Synced', jsonb_build_object(
				'status', 'True',
				'reason', 'Reported',
				'message', 'report generation %d',
				'lastTransitionTime', '2026-01-01T00:00:00Z'
			)
		)`, generation)
}

func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

const schemaSQL = `
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE extension_resources (
	uid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	service_name text NOT NULL,
	type_name text NOT NULL,
	collection_name text NOT NULL,
	resource_id text NOT NULL,
	labels jsonb NOT NULL DEFAULT '{}',
	alias_fingerprint bytea,
	created_at timestamptz NOT NULL DEFAULT now(),
	updated_at timestamptz NOT NULL DEFAULT now(),
	UNIQUE (service_name, collection_name, resource_id)
);

CREATE INDEX extension_resources_collection_resource_idx
	ON extension_resources(collection_name, resource_id);

CREATE TABLE extension_resource_inventory (
	extension_resource_uid uuid PRIMARY KEY
		REFERENCES extension_resources(uid) ON DELETE CASCADE,
	observation jsonb,
	labels jsonb NOT NULL DEFAULT '{}',
	conditions jsonb NOT NULL DEFAULT '{}',
	observed_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL
);

CREATE INDEX extension_resource_inventory_labels_gin
	ON extension_resource_inventory USING GIN (labels);

CREATE INDEX extension_resource_inventory_conditions_gin
	ON extension_resource_inventory USING GIN (conditions);

CREATE TABLE resource_alias_claims (
	id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	namespace text NOT NULL,
	key text NOT NULL,
	value text NOT NULL,
	platform_collection_name text NOT NULL,
	platform_resource_id text NOT NULL,
	platform_owned boolean NOT NULL DEFAULT false,
	created_at timestamptz NOT NULL DEFAULT now(),
	UNIQUE (namespace, key, value),
	UNIQUE (namespace, key, platform_collection_name, platform_resource_id),
	UNIQUE (id, namespace, key)
);

CREATE TABLE resource_alias_contributions (
	source_extension_resource_uid uuid NOT NULL
		REFERENCES extension_resources(uid) ON DELETE CASCADE,
	namespace text NOT NULL,
	key text NOT NULL,
	claim_id bigint NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (source_extension_resource_uid, namespace, key),
	FOREIGN KEY (claim_id, namespace, key)
		REFERENCES resource_alias_claims(id, namespace, key)
);

CREATE INDEX resource_alias_contributions_claim_idx
	ON resource_alias_contributions(claim_id);

CREATE INDEX resource_alias_contributions_claim_source_idx
	ON resource_alias_contributions(claim_id, source_extension_resource_uid);

CREATE TABLE bench_acm_resources (
	uid text PRIMARY KEY,
	cluster text NOT NULL,
	data jsonb NOT NULL
);

CREATE INDEX bench_acm_data_kind_idx
	ON bench_acm_resources USING GIN ((data -> 'kind'));

CREATE INDEX bench_acm_data_namespace_idx
	ON bench_acm_resources USING GIN ((data -> 'namespace'));

CREATE INDEX bench_acm_data_name_idx
	ON bench_acm_resources USING GIN ((data -> 'name'));

CREATE INDEX bench_acm_data_cluster_idx
	ON bench_acm_resources USING btree (cluster);

CREATE INDEX bench_acm_data_composite_idx
	ON bench_acm_resources USING GIN (
		(data -> '_hubClusterResource'), (data -> 'namespace'), (data -> 'apigroup'), (data -> 'kind_plural')
	);

CREATE INDEX bench_acm_data_hubcluster_idx
	ON bench_acm_resources USING GIN ((data -> '_hubClusterResource'))
	WHERE data ? '_hubClusterResource';
`

const seedSQL = `
INSERT INTO extension_resources (
	uid,
	service_name,
	type_name,
	collection_name,
	resource_id,
	labels,
	alias_fingerprint,
	created_at,
	updated_at
)
SELECT
	gen_random_uuid(),
	'bench.fleetshift.io',
	'Widget',
	'widgets',
	'r-' || lpad(g::text, 8, '0'),
	jsonb_build_object('fleet', 'primary', 'seed', g::text),
	CASE
		WHEN g BETWEEN 84101 AND 84200 THEN NULL
		WHEN g >= 15001 THEN digest(('source-id=ext-' || lpad(g::text, 8, '0'))::bytea, 'sha256')
		ELSE digest(''::bytea, 'sha256')
	END,
	'2026-01-01T00:00:00Z'::timestamptz,
	'2026-01-01T00:00:00Z'::timestamptz
FROM generate_series(1, 100000) AS g;

INSERT INTO extension_resources (
	uid,
	service_name,
	type_name,
	collection_name,
	resource_id,
	labels,
	created_at,
	updated_at
)
SELECT
	gen_random_uuid(),
	'bench-b.fleetshift.io',
	'Widget',
	'widgets',
	'r-' || lpad(g::text, 8, '0'),
	jsonb_build_object('fleet', 'secondary', 'seed', g::text),
	'2026-01-01T00:00:00Z'::timestamptz,
	'2026-01-01T00:00:00Z'::timestamptz
FROM generate_series(75001, 85000) AS g;

INSERT INTO extension_resource_inventory (
	extension_resource_uid,
	observation,
	labels,
	conditions,
	observed_at,
	updated_at
)
SELECT
	er.uid,
	jsonb_build_object(
		'generation', 1,
		'payload', jsonb_build_object(
			'cpu', (g % 16),
			'memoryGiB', 16 + (g % 128),
			'zone', 'zone-' || (g % 16)::text
		)
	),
	jsonb_build_object(
		'env', CASE WHEN g % 2 = 0 THEN 'prod' ELSE 'dev' END,
		'region', 'region-' || (g % 10)::text,
		'owner', 'team-' || (g % 20)::text,
		'shard', (g % 64)::text,
		'generation', '1'
	),
	jsonb_build_object(
		'Ready', jsonb_build_object(
			'status', CASE WHEN g % 17 = 0 THEN 'False' ELSE 'True' END,
			'reason', CASE WHEN g % 17 = 0 THEN 'Reconciling' ELSE 'Ready' END,
			'message', CASE WHEN g % 17 = 0 THEN 'waiting for dependency' ELSE 'ready' END,
			'lastTransitionTime', '2026-01-01T00:00:00Z'
		),
		'Synced', jsonb_build_object(
			'status', 'True',
			'reason', 'Reported',
			'message', 'report generation 1',
			'lastTransitionTime', '2026-01-01T00:00:00Z'
		)
	),
	'2026-01-01T00:00:00Z'::timestamptz + make_interval(secs => g),
	'2026-01-01T00:00:00Z'::timestamptz
FROM generate_series(1, 100000) AS g
JOIN extension_resources er
  ON er.service_name = 'bench.fleetshift.io'
 AND er.collection_name = 'widgets'
 AND er.resource_id = 'r-' || lpad(g::text, 8, '0');

INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id)
SELECT 'ext-id', 'source-id', 'ext-' || lpad(g::text, 8, '0'), 'widgets', 'r-' || lpad(g::text, 8, '0')
FROM generate_series(15001, 100000) AS g;

INSERT INTO resource_alias_contributions (source_extension_resource_uid, namespace, key, claim_id)
SELECT er.uid, cl.namespace, cl.key, cl.id
FROM resource_alias_claims cl
JOIN extension_resources er
  ON er.service_name = 'bench.fleetshift.io'
 AND er.collection_name = cl.platform_collection_name
 AND er.resource_id = cl.platform_resource_id
WHERE cl.key = 'source-id';

INSERT INTO resource_alias_contributions (source_extension_resource_uid, namespace, key, claim_id)
SELECT er.uid, cl.namespace, cl.key, cl.id
FROM generate_series(82001, 83000) AS g
JOIN resource_alias_claims cl
  ON cl.namespace = 'ext-id'
 AND cl.key = 'source-id'
 AND cl.platform_collection_name = 'widgets'
 AND cl.platform_resource_id = 'r-' || lpad(g::text, 8, '0')
JOIN extension_resources er
  ON er.service_name = 'bench-b.fleetshift.io'
 AND er.collection_name = cl.platform_collection_name
 AND er.resource_id = cl.platform_resource_id;

INSERT INTO resource_alias_contributions (source_extension_resource_uid, namespace, key, claim_id)
SELECT er.uid, cl.namespace, cl.key, cl.id
FROM generate_series(83001, 83100) AS g
JOIN resource_alias_claims cl
  ON cl.namespace = 'ext-id'
 AND cl.key = 'source-id'
 AND cl.platform_collection_name = 'widgets'
 AND cl.platform_resource_id = 'r-' || lpad(g::text, 8, '0')
JOIN extension_resources er
  ON er.service_name = 'bench-b.fleetshift.io'
 AND er.collection_name = cl.platform_collection_name
 AND er.resource_id = cl.platform_resource_id;

INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id)
SELECT 'ext-id', 'reuse-id', 'reuse-' || lpad(g::text, 8, '0'), 'widgets', 'r-' || lpad(g::text, 8, '0')
FROM generate_series(84501, 84600) AS g;

INSERT INTO resource_alias_contributions (source_extension_resource_uid, namespace, key, claim_id)
SELECT er.uid, cl.namespace, cl.key, cl.id
FROM generate_series(84501, 84600) AS g
JOIN resource_alias_claims cl
  ON cl.namespace = 'ext-id'
 AND cl.key = 'reuse-id'
 AND cl.platform_collection_name = 'widgets'
 AND cl.platform_resource_id = 'r-' || lpad(g::text, 8, '0')
JOIN extension_resources er
  ON er.service_name = 'bench-b.fleetshift.io'
 AND er.collection_name = cl.platform_collection_name
 AND er.resource_id = cl.platform_resource_id;

INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id, platform_owned)
SELECT 'ext-id', 'secondary-id', 'victim-' || lpad(g::text, 8, '0'), 'widgets', 'victim-' || lpad(g::text, 8, '0'), true
FROM generate_series(1, 2500) AS g;

INSERT INTO bench_acm_resources (uid, cluster, data)
SELECT
	'acm-' || lpad(g::text, 8, '0'),
	'cluster-' || (g % 500)::text,
	jsonb_build_object(
		'kind', (ARRAY['Pod', 'Deployment', 'ConfigMap', 'Service', 'Secret', 'ReplicaSet'])[1 + (g % 6)],
		'namespace', (ARRAY['default', 'kube-system', 'openshift-monitoring', 'app-team-a', 'app-team-b'])[1 + (g % 5)],
		'name', 'resource-' || g::text,
		'apigroup', CASE WHEN g % 6 = 0 THEN 'apps' ELSE '' END,
		'kind_plural', lower((ARRAY['pods', 'deployments', 'configmaps', 'services', 'secrets', 'replicasets'])[1 + (g % 6)]),
		'_hubClusterResource', false,
		'generation', 1,
		'payload', jsonb_build_object(
			'cpu', (g % 16),
			'memoryGiB', 16 + (g % 128),
			'zone', 'zone-' || (g % 16)::text
		)
	)
FROM generate_series(1, 100000) AS g;

ANALYZE extension_resources;
ANALYZE extension_resource_inventory;
ANALYZE resource_alias_claims;
ANALYZE resource_alias_contributions;
ANALYZE bench_acm_resources;
`
