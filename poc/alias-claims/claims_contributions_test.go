package aliasclaims

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

func TestClaimsContributionsPlans(t *testing.T) {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("alias_claims_poc"),
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

	var claimCount, contribCount int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM resource_alias_claims`).Scan(&claimCount); err != nil {
		t.Fatalf("count claims: %v", err)
	}
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM resource_alias_contributions`).Scan(&contribCount); err != nil {
		t.Fatalf("count contributions: %v", err)
	}
	t.Logf("seeded %d resources, %d claims, %d contributions", corpusSize, claimCount, contribCount)

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			plan, err := explain(ctx, db, scenario.sql)
			if err != nil {
				t.Fatalf("explain %s: %v", scenario.name, err)
			}
			t.Logf("\n%s", summarizePlan(plan))
		})
	}
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
		case strings.Contains(trimmed, "Seq Scan on resource_alias"):
			out = append(out, line)
		case strings.Contains(trimmed, "Index Scan using resource_alias"):
			out = append(out, line)
		case strings.Contains(trimmed, "Index Only Scan using resource_alias"):
			out = append(out, line)
		case strings.Contains(trimmed, "Index Scan using extension_resources"):
			out = append(out, line)
		case strings.Contains(trimmed, "Insert on resource_alias"):
			out = append(out, line)
		case strings.Contains(trimmed, "Update on resource_alias"):
			out = append(out, line)
		case strings.Contains(trimmed, "Delete on resource_alias"):
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
		name: "steady_source_first_noop_classification",
		sql:  inputAliasesSQL(15_001, 16_000, "bench.fleetshift.io", "source-id", "ext") + `
, self_claim AS (
	SELECT ia.*, c.claim_id, cl.value AS existing_value, cl.platform_collection_name, cl.platform_resource_id
	FROM input_aliases ia
	LEFT JOIN LATERAL (
		SELECT claim_id
		FROM resource_alias_contributions
		WHERE source_extension_resource_uid = ia.source_uid
		  AND namespace = ia.namespace AND key = ia.key
		LIMIT 1 OFFSET 0
	) c ON true
	LEFT JOIN LATERAL (
		SELECT value, platform_collection_name, platform_resource_id
		FROM resource_alias_claims
		WHERE id = c.claim_id
		LIMIT 1 OFFSET 0
	) cl ON true
)
SELECT count(*) AS changed
FROM self_claim
WHERE claim_id IS NULL
   OR existing_value IS DISTINCT FROM value
   OR platform_collection_name IS DISTINCT FROM collection_name
   OR platform_resource_id IS DISTINCT FROM resource_id`,
	},
	{
		name: "new_secondary_alias_classification",
		sql:  classifyChangedAliasesSQL(25_001, 26_000, "bench.fleetshift.io", "secondary-id", "new-secondary"),
	},
	{
		name: "value_claimed_by_other_classification",
		sql: inputAliasesSQL(35_001, 36_000, "bench.fleetshift.io", "secondary-id", "victim") + `
, by_value AS (
	SELECT ia.idx, vc.id, vc.platform_collection_name, vc.platform_resource_id
	FROM input_aliases ia
	LEFT JOIN LATERAL (
		SELECT id, platform_collection_name, platform_resource_id
		FROM resource_alias_claims
		WHERE namespace = ia.namespace AND key = ia.key AND value = ia.value
		LIMIT 1 OFFSET 0
	) vc ON true
)
SELECT count(*) AS value_conflicts
FROM input_aliases ia
JOIN by_value bv ON bv.idx = ia.idx
WHERE bv.id IS NOT NULL
  AND (bv.platform_collection_name <> ia.collection_name OR bv.platform_resource_id <> ia.resource_id)`,
	},
	{
		name: "cross_contributor_resource_conflict_classification",
		sql:  classifyChangedAliasesSQL(75_001, 76_000, "bench-b.fleetshift.io", "source-id", "ext-b"),
	},
	{
		name: "self_replace_allowed_classification",
		sql:  classifyChangedAliasesSQL(65_001, 66_000, "bench.fleetshift.io", "source-id", "ext-changed"),
	},
	{
		name: "new_secondary_alias_apply",
		sql: inputAliasesSQL(45_001, 46_000, "bench.fleetshift.io", "secondary-id", "new-secondary-apply") + `
, inserted_claims AS (
	INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id)
	SELECT namespace, key, value, collection_name, resource_id
	FROM input_aliases
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
SELECT count(*) FROM upserted_contributions`,
	},
	{
		name: "self_replace_apply",
		sql: inputAliasesSQL(67_001, 68_000, "bench.fleetshift.io", "source-id", "ext-changed-apply") + `
, self_claim AS (
	SELECT ia.*, c.claim_id
	FROM input_aliases ia
	JOIN LATERAL (
		SELECT claim_id
		FROM resource_alias_contributions
		WHERE source_extension_resource_uid = ia.source_uid
		  AND namespace = ia.namespace AND key = ia.key
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
	SET value = sr.value
	FROM safe_replace sr
	WHERE cl.id = sr.claim_id
	RETURNING 1
)
SELECT count(*) FROM updated_claims`,
	},
	{
		name: "retract_alias_apply",
		sql: inputSourcesSQL(55_001, 56_000, "bench.fleetshift.io") + `
, deleted_contributions AS (
	DELETE FROM resource_alias_contributions c
	USING input_sources s
	WHERE c.source_extension_resource_uid = s.source_uid
	  AND c.namespace = 'ext-id' AND c.key = 'source-id'
	RETURNING c.claim_id
)
SELECT count(*) FROM deleted_contributions`,
	},
	{
		name: "retract_alias_cleanup_orphan_claims_second_statement",
		sql: `WITH candidate_claims AS (
	SELECT cl.id
	FROM generate_series(55001, 56000) AS g
	JOIN LATERAL (
		SELECT id
		FROM resource_alias_claims
		WHERE namespace = 'ext-id' AND key = 'source-id'
		  AND platform_collection_name = 'widgets'
		  AND platform_resource_id = 'r-' || lpad(g::text, 8, '0')
		LIMIT 1 OFFSET 0
	) cl ON true
),
deleted_orphan_claims AS (
	DELETE FROM resource_alias_claims cl
	USING candidate_claims cc
	WHERE cl.id = cc.id
	  AND NOT cl.platform_owned
	  AND NOT EXISTS (
		SELECT 1 FROM resource_alias_contributions c WHERE c.claim_id = cl.id
	  )
	RETURNING 1
)
SELECT count(*) FROM deleted_orphan_claims`,
	},
}

func inputAliasesSQL(start, end int, serviceName, key, valuePrefix string) string {
	valueExpr := fmt.Sprintf("(%s || '-' || lpad((g - %d + 1)::text, 8, '0'))", sqlQuote(valuePrefix), start)
	if valuePrefix == "ext" {
		valueExpr = "('ext-' || lpad(g::text, 8, '0'))"
	}
	return fmt.Sprintf(`WITH input_aliases AS (
	SELECT row_number() OVER ()::int AS idx,
	       er.uid AS source_uid,
	       'ext-id'::text AS namespace,
	       %[4]s::text AS key,
	       %[5]s::text AS value,
	       'widgets'::text AS collection_name,
	       ('r-' || lpad(g::text, 8, '0'))::text AS resource_id
	FROM generate_series(%[1]d, %[2]d) AS g
	JOIN extension_resources er
	  ON er.service_name = %[3]s
	 AND er.collection_name = 'widgets'
	 AND er.resource_id = 'r-' || lpad(g::text, 8, '0')
)`, start, end, sqlQuote(serviceName), sqlQuote(key), valueExpr)
}

func inputSourcesSQL(start, end int, serviceName string) string {
	return fmt.Sprintf(`WITH input_sources AS (
	SELECT er.uid AS source_uid
	FROM generate_series(%[1]d, %[2]d) AS g
	JOIN extension_resources er
	  ON er.service_name = %[3]s
	 AND er.collection_name = 'widgets'
	 AND er.resource_id = 'r-' || lpad(g::text, 8, '0')
)`, start, end, sqlQuote(serviceName))
}

func sqlQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func classifyChangedAliasesSQL(start, end int, serviceName, key, valuePrefix string) string {
	return inputAliasesSQL(start, end, serviceName, key, valuePrefix) + `
, self_claim AS (
	SELECT ia.*, c.claim_id, cl.value AS existing_value, cl.platform_collection_name, cl.platform_resource_id
	FROM input_aliases ia
	LEFT JOIN LATERAL (
		SELECT claim_id
		FROM resource_alias_contributions
		WHERE source_extension_resource_uid = ia.source_uid
		  AND namespace = ia.namespace AND key = ia.key
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
	SELECT *
	FROM self_claim
	WHERE claim_id IS NULL
	   OR existing_value IS DISTINCT FROM value
	   OR platform_collection_name IS DISTINCT FROM collection_name
	   OR platform_resource_id IS DISTINCT FROM resource_id
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
	SELECT ch.idx, rc.id AS resource_claim_id, rc.value AS resource_value, rc.platform_owned
	FROM changed ch
	LEFT JOIN LATERAL (
		SELECT id, value, platform_owned
		FROM resource_alias_claims
		WHERE namespace = ch.namespace AND key = ch.key
		  AND platform_collection_name = ch.collection_name AND platform_resource_id = ch.resource_id
		LIMIT 1 OFFSET 0
	) rc ON true
),
sibling AS (
	SELECT br.idx,
	       br.resource_claim_id IS NOT NULL
	       AND (
		 br.platform_owned
		 OR EXISTS (
			SELECT 1
			FROM resource_alias_contributions other
			JOIN changed ch ON ch.idx = br.idx
			WHERE other.claim_id = br.resource_claim_id
			  AND other.source_extension_resource_uid <> ch.source_uid
		 )
	       ) AS sibling_holds
	FROM by_resource br
)
SELECT count(*) FILTER (
		WHERE bv.value_claim_id IS NOT NULL
		  AND (bv.value_collection_name <> ch.collection_name OR bv.value_resource_id <> ch.resource_id)
	) AS value_conflicts,
	count(*) FILTER (
		WHERE bv.value_claim_id IS NULL AND br.resource_claim_id IS NOT NULL AND s.sibling_holds
	) AS resource_conflicts,
	count(*) FILTER (
		WHERE NOT (
			bv.value_claim_id IS NOT NULL
			AND (bv.value_collection_name <> ch.collection_name OR bv.value_resource_id <> ch.resource_id)
		)
		AND NOT (bv.value_claim_id IS NULL AND br.resource_claim_id IS NOT NULL AND s.sibling_holds)
	) AS safe
FROM changed ch
LEFT JOIN by_value bv ON bv.idx = ch.idx
LEFT JOIN by_resource br ON br.idx = ch.idx
LEFT JOIN sibling s ON s.idx = ch.idx`
}

const schemaSQL = `
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE platform_resources (
	collection_name text NOT NULL,
	resource_id text NOT NULL,
	created_at timestamptz NOT NULL DEFAULT now(),
	PRIMARY KEY (collection_name, resource_id)
);

CREATE TABLE extension_resources (
	uid uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	service_name text NOT NULL,
	collection_name text NOT NULL,
	resource_id text NOT NULL,
	alias_fingerprint bytea,
	created_at timestamptz NOT NULL DEFAULT now(),
	UNIQUE (service_name, collection_name, resource_id)
);

CREATE INDEX extension_resources_collection_resource_idx
	ON extension_resources(collection_name, resource_id);

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
	UNIQUE (id, namespace, key),
	FOREIGN KEY (platform_collection_name, platform_resource_id)
		REFERENCES platform_resources(collection_name, resource_id) ON DELETE CASCADE
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
		REFERENCES resource_alias_claims(id, namespace, key) ON DELETE CASCADE
);

CREATE INDEX resource_alias_contributions_claim_idx
	ON resource_alias_contributions(claim_id);

CREATE INDEX resource_alias_contributions_claim_source_idx
	ON resource_alias_contributions(claim_id, source_extension_resource_uid);
`

const seedSQL = `
INSERT INTO platform_resources (collection_name, resource_id)
SELECT 'widgets', 'r-' || lpad(g::text, 8, '0')
FROM generate_series(1, 100000) AS g;

INSERT INTO extension_resources (uid, service_name, collection_name, resource_id, alias_fingerprint)
SELECT gen_random_uuid(), 'bench.fleetshift.io', 'widgets', 'r-' || lpad(g::text, 8, '0'), digest(('source-id=ext-' || lpad(g::text, 8, '0'))::bytea, 'sha256')
FROM generate_series(1, 100000) AS g;

INSERT INTO extension_resources (uid, service_name, collection_name, resource_id, alias_fingerprint)
SELECT gen_random_uuid(), 'bench-b.fleetshift.io', 'widgets', 'r-' || lpad(g::text, 8, '0'), NULL
FROM generate_series(75001, 85000) AS g;

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

INSERT INTO platform_resources (collection_name, resource_id)
SELECT 'widgets', 'victim-' || lpad(g::text, 8, '0')
FROM generate_series(1, 2500) AS g;

INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id, platform_owned)
SELECT 'ext-id', 'secondary-id', 'victim-' || lpad(g::text, 8, '0'), 'widgets', 'victim-' || lpad(g::text, 8, '0'), true
FROM generate_series(1, 2500) AS g;

ANALYZE platform_resources;
ANALYZE extension_resources;
ANALYZE resource_alias_claims;
ANALYZE resource_alias_contributions;
`
