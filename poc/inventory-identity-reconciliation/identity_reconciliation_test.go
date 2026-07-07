package inventoryidentityreconciliation

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func isPodmanAvailable() bool {
	_, err := exec.LookPath("podman")
	return err == nil
}

func init() {
	if os.Getenv("TESTCONTAINERS_PROVIDER") != "docker" && isPodmanAvailable() {
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
}

func TestInventoryIdentityReconciliation(t *testing.T) {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("inventory_identity_reconciliation_poc"),
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

	t.Run("pending_then_accepted", func(t *testing.T) {
		report := buildBatch(0, rangeSpec{
			start: 1, count: 1, serviceName: "kubernetes.fleetshift.io", collectionName: "clusters", generation: 1,
			aliasesFor: func(_ int, g int) []alias {
				return []alias{{namespace: "cloud", key: "provider-id", value: "cluster-provider-001"}}
			},
		})

		got := execHotPath(ctx, t, db, report)
		assertHotResult(t, got, hotPathResult{
			updatedInventory:    1,
			storedAliasPayloads: 1,
			queuedSources:       1,
			markedPending:       1,
		})
		assertStatus(ctx, t, db, "kubernetes.fleetshift.io", "clusters", "r-00000001", "pending")
		assertAcceptedRepresentations(ctx, t, db, "clusters", "r-00000001", 0)

		reconciled := execReconcile(ctx, t, db)
		assertReconcileResult(t, reconciled, reconcileResult{
			queuedSources:         1,
			acceptedSources:       1,
			conflictedSources:     0,
			insertedConflicts:     0,
			acceptedAliases:       1,
			acceptedRepresentions: 1,
		})
		assertStatus(ctx, t, db, "kubernetes.fleetshift.io", "clusters", "r-00000001", "accepted")
		assertAcceptedRepresentations(ctx, t, db, "clusters", "r-00000001", 1)
		assertResolvedAlias(ctx, t, db, "cloud", "provider-id", "cluster-provider-001", "clusters", "r-00000001")
	})

	t.Run("corroborating_inventory_source_is_accepted", func(t *testing.T) {
		report := buildBatch(0, rangeSpec{
			start: 1, count: 1, serviceName: "observer.fleetshift.io", collectionName: "clusters", generation: 1,
			aliasesFor: func(_ int, g int) []alias {
				return []alias{{namespace: "cloud", key: "provider-id", value: "cluster-provider-001"}}
			},
		})

		got := execHotPath(ctx, t, db, report)
		if got.queuedSources != 1 {
			t.Fatalf("queued sources = %d, want 1; result %s", got.queuedSources, got)
		}
		reconciled := execReconcile(ctx, t, db)
		if reconciled.conflictedSources != 0 || reconciled.acceptedSources != 1 {
			t.Fatalf("reconcile result = %s, want one accepted source and no conflicts", reconciled)
		}
		assertAcceptedRepresentations(ctx, t, db, "clusters", "r-00000001", 2)
		assertConflictCount(ctx, t, db, "clusters", "r-00000001", 0)
	})

	t.Run("conflicting_report_succeeds_but_is_not_accepted", func(t *testing.T) {
		report := buildBatch(0, rangeSpec{
			start: 1, count: 1, serviceName: "miswired.fleetshift.io", collectionName: "clusters", generation: 1,
			aliasesFor: func(_ int, g int) []alias {
				return []alias{{namespace: "cloud", key: "provider-id", value: "cluster-provider-999"}}
			},
		})

		got := execHotPath(ctx, t, db, report)
		if got.queuedSources != 1 || got.updatedInventory != 1 {
			t.Fatalf("hot path result = %s, want report accepted and source queued", got)
		}
		assertStatus(ctx, t, db, "miswired.fleetshift.io", "clusters", "r-00000001", "pending")

		reconciled := execReconcile(ctx, t, db)
		assertReconcileResult(t, reconciled, reconcileResult{
			queuedSources:         1,
			acceptedSources:       0,
			conflictedSources:     1,
			insertedConflicts:     1,
			acceptedAliases:       0,
			acceptedRepresentions: 0,
		})
		assertStatus(ctx, t, db, "miswired.fleetshift.io", "clusters", "r-00000001", "conflict")
		assertAcceptedRepresentations(ctx, t, db, "clusters", "r-00000001", 2)
		assertConflictCount(ctx, t, db, "clusters", "r-00000001", 1)
		assertResolvedAliasMissing(ctx, t, db, "cloud", "provider-id", "cluster-provider-999")
	})

	t.Run("accepted_source_becomes_pending_then_conflict_on_alias_change", func(t *testing.T) {
		report := buildBatch(0, rangeSpec{
			start: 1, count: 1, serviceName: "observer.fleetshift.io", collectionName: "clusters", generation: 2,
			aliasesFor: func(_ int, g int) []alias {
				return []alias{{namespace: "cloud", key: "provider-id", value: "cluster-provider-002"}}
			},
		})

		got := execHotPath(ctx, t, db, report)
		if got.storedAliasPayloads != 1 || got.markedPending != 1 {
			t.Fatalf("hot path result = %s, want alias payload stored and source marked pending", got)
		}
		assertStatus(ctx, t, db, "observer.fleetshift.io", "clusters", "r-00000001", "pending")
		assertAcceptedRepresentations(ctx, t, db, "clusters", "r-00000001", 1)

		reconciled := execReconcile(ctx, t, db)
		if reconciled.conflictedSources != 1 {
			t.Fatalf("reconcile result = %s, want conflict", reconciled)
		}
		assertStatus(ctx, t, db, "observer.fleetshift.io", "clusters", "r-00000001", "conflict")
		assertConflictCount(ctx, t, db, "clusters", "r-00000001", 2)
		assertResolvedAliasMissing(ctx, t, db, "cloud", "provider-id", "cluster-provider-002")
	})

	t.Run("conflict_resolves_after_corrected_report", func(t *testing.T) {
		report := buildBatch(0, rangeSpec{
			start: 1, count: 1, serviceName: "observer.fleetshift.io", collectionName: "clusters", generation: 3,
			aliasesFor: func(_ int, g int) []alias {
				return []alias{{namespace: "cloud", key: "provider-id", value: "cluster-provider-001"}}
			},
		})

		got := execHotPath(ctx, t, db, report)
		if got.clearedConflicts != 1 || got.markedPending != 1 {
			t.Fatalf("hot path result = %s, want old source conflict cleared and source pending", got)
		}
		reconciled := execReconcile(ctx, t, db)
		if reconciled.acceptedSources != 1 || reconciled.conflictedSources != 0 {
			t.Fatalf("reconcile result = %s, want accepted corrected source", reconciled)
		}
		assertStatus(ctx, t, db, "observer.fleetshift.io", "clusters", "r-00000001", "accepted")
		assertAcceptedRepresentations(ctx, t, db, "clusters", "r-00000001", 2)
		assertConflictCount(ctx, t, db, "clusters", "r-00000001", 1)
	})

	t.Run("accepted_source_can_remove_alias", func(t *testing.T) {
		initial := buildBatch(0, rangeSpec{
			start: 2, count: 1, serviceName: "alias-removal.fleetshift.io", collectionName: "clusters", generation: 1,
			aliasesFor: func(_ int, g int) []alias {
				return []alias{{namespace: "cloud", key: "provider-id", value: "removable-provider-002"}}
			},
		})

		got := execHotPath(ctx, t, db, initial)
		if got.storedAliasPayloads != 1 || got.queuedSources != 1 {
			t.Fatalf("initial hot path result = %s, want source queued", got)
		}
		reconciled := execReconcile(ctx, t, db)
		if reconciled.acceptedSources != 1 || reconciled.acceptedAliases != 1 {
			t.Fatalf("initial reconcile result = %s, want accepted alias", reconciled)
		}
		assertResolvedAlias(ctx, t, db, "cloud", "provider-id", "removable-provider-002", "clusters", "r-00000002")

		removed := buildBatch(0, rangeSpec{
			start: 2, count: 1, serviceName: "alias-removal.fleetshift.io", collectionName: "clusters", generation: 2,
			aliasesFor: func(int, int) []alias { return nil },
		})

		got = execHotPath(ctx, t, db, removed)
		if got.storedAliasPayloads != 1 || got.markedPending != 1 {
			t.Fatalf("removal hot path result = %s, want alias payload replaced and source pending", got)
		}
		assertResolvedAliasMissing(ctx, t, db, "cloud", "provider-id", "removable-provider-002")

		reconciled = execReconcile(ctx, t, db)
		if reconciled.acceptedSources != 1 || reconciled.acceptedAliases != 0 {
			t.Fatalf("removal reconcile result = %s, want accepted source with no aliases", reconciled)
		}
		assertStatus(ctx, t, db, "alias-removal.fleetshift.io", "clusters", "r-00000002", "accepted")
		assertAcceptedRepresentations(ctx, t, db, "clusters", "r-00000002", 1)
		assertResolvedAliasMissing(ctx, t, db, "cloud", "provider-id", "removable-provider-002")
	})

	t.Run("batch_timings", func(t *testing.T) {
		runBatchTimings(ctx, t, db)
	})

	if os.Getenv("FLEETSHIFT_LONG_INVENTORY_IDENTITY_BENCH") == "1" {
		t.Run("long_benchmark", func(t *testing.T) {
			runLongBenchmark(ctx, t, db)
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

type alias struct {
	namespace string
	key       string
	value     string
}

type batch struct {
	idx               []int32
	serviceNames      []string
	typeNames         []string
	collectionNames   []string
	resourceIDs       []string
	candidateUIDs     []string
	observations      []string
	labels            []string
	conditions        []string
	observedAts       []time.Time
	receivedAts       []time.Time
	aliasFingerprints [][]byte
	aliasPayloads     []string
}

func (b batch) args() []any {
	return []any{
		b.idx,
		b.serviceNames,
		b.typeNames,
		b.collectionNames,
		b.resourceIDs,
		b.candidateUIDs,
		b.observations,
		b.labels,
		b.conditions,
		b.observedAts,
		b.receivedAts,
		b.aliasFingerprints,
		b.aliasPayloads,
	}
}

type rangeSpec struct {
	start          int
	count          int
	serviceName    string
	collectionName string
	generation     int
	aliasesFor     func(iteration, g int) []alias
}

func buildBatch(iteration int, specs ...rangeSpec) batch {
	var b batch
	baseTime := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)

	for _, spec := range specs {
		for g := spec.start; g < spec.start+spec.count; g++ {
			aliases := spec.aliasesFor(iteration, g)
			receivedAt := baseTime.Add(time.Duration(iteration)*time.Minute + time.Duration(g)*time.Millisecond)
			idx := int32(len(b.idx) + 1)
			resourceID := resourceID(g)

			b.idx = append(b.idx, idx)
			b.serviceNames = append(b.serviceNames, spec.serviceName)
			b.typeNames = append(b.typeNames, "Cluster")
			b.collectionNames = append(b.collectionNames, spec.collectionName)
			b.resourceIDs = append(b.resourceIDs, resourceID)
			b.candidateUIDs = append(b.candidateUIDs, candidateUID(spec.serviceName, spec.collectionName, g))
			b.observations = append(b.observations, observationJSON(g, spec.generation))
			b.labels = append(b.labels, labelsJSON(g, spec.generation))
			b.conditions = append(b.conditions, conditionsJSON(spec.generation))
			b.observedAts = append(b.observedAts, baseTime.Add(time.Duration(g)*time.Second))
			b.receivedAts = append(b.receivedAts, receivedAt)
			b.aliasFingerprints = append(b.aliasFingerprints, aliasSetFingerprint(aliases))
			b.aliasPayloads = append(b.aliasPayloads, aliasSetJSON(aliases))
		}
	}

	return b
}

func candidateUID(serviceName, collectionName string, g int) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%08d", serviceName, collectionName, g)))
	return fmt.Sprintf("%x-%x-%x-%x-%x", h[0:4], h[4:6], h[6:8], h[8:10], h[10:16])
}

func resourceID(g int) string {
	return "r-" + fmt.Sprintf("%08d", g)
}

func aliasSetFingerprint(aliases []alias) []byte {
	sorted := sortedAliases(aliases)
	h := sha256.New()
	for _, a := range sorted {
		hashString(h, a.namespace)
		hashString(h, a.key)
		hashString(h, a.value)
	}
	return h.Sum(nil)
}

func aliasSetJSON(aliases []alias) string {
	type encodedAlias struct {
		Namespace string `json:"namespace"`
		Key       string `json:"key"`
		Value     string `json:"value"`
	}

	sorted := sortedAliases(aliases)
	encoded := make([]encodedAlias, len(sorted))
	for i, a := range sorted {
		encoded[i] = encodedAlias{
			Namespace: a.namespace,
			Key:       a.key,
			Value:     a.value,
		}
	}
	out, err := json.Marshal(encoded)
	if err != nil {
		panic(err)
	}
	return string(out)
}

func sortedAliases(aliases []alias) []alias {
	sorted := make([]alias, len(aliases))
	copy(sorted, aliases)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].namespace != sorted[j].namespace {
			return sorted[i].namespace < sorted[j].namespace
		}
		if sorted[i].key != sorted[j].key {
			return sorted[i].key < sorted[j].key
		}
		return sorted[i].value < sorted[j].value
	})
	return sorted
}

func hashString(h hash.Hash, s string) {
	_ = binary.Write(h, binary.BigEndian, int64(len(s)))
	_, _ = h.Write([]byte(s))
}

func observationJSON(g, generation int) string {
	return fmt.Sprintf(`{"generation":%d,"nodeCount":%d,"zone":"zone-%d"}`, generation, 3+(g%20), g%8)
}

func labelsJSON(g, generation int) string {
	return fmt.Sprintf(`{"region":"region-%d","generation":"%d"}`, g%10, generation)
}

func conditionsJSON(generation int) string {
	return fmt.Sprintf(`{"Ready":{"status":"True","reason":"Reported","message":"report generation %d","lastTransitionTime":"2026-01-01T00:00:00Z"}}`, generation)
}

type hotPathResult struct {
	updatedInventory    int
	storedAliasPayloads int
	clearedConflicts    int
	markedPending       int
	queuedSources       int
}

func (r hotPathResult) String() string {
	return fmt.Sprintf("inventory=%d stored_alias_payloads=%d cleared_conflicts=%d marked_pending=%d queued=%d",
		r.updatedInventory,
		r.storedAliasPayloads,
		r.clearedConflicts,
		r.markedPending,
		r.queuedSources,
	)
}

type reconcileResult struct {
	queuedSources         int
	acceptedSources       int
	conflictedSources     int
	insertedConflicts     int
	acceptedAliases       int
	acceptedRepresentions int
}

func (r reconcileResult) String() string {
	return fmt.Sprintf("queued=%d accepted=%d conflicted=%d inserted_conflicts=%d accepted_aliases=%d accepted_representations=%d",
		r.queuedSources,
		r.acceptedSources,
		r.conflictedSources,
		r.insertedConflicts,
		r.acceptedAliases,
		r.acceptedRepresentions,
	)
}

func execHotPath(ctx context.Context, t *testing.T, db *sql.DB, b batch) hotPathResult {
	t.Helper()

	var out hotPathResult
	err := db.QueryRowContext(ctx, replaceInventoryAssertionsSQL(), b.args()...).Scan(
		&out.updatedInventory,
		&out.storedAliasPayloads,
		&out.clearedConflicts,
		&out.markedPending,
		&out.queuedSources,
	)
	if err != nil {
		t.Fatalf("hot path: %v", err)
	}
	return out
}

func execReconcile(ctx context.Context, t *testing.T, db *sql.DB) reconcileResult {
	t.Helper()

	var out reconcileResult
	err := db.QueryRowContext(ctx, reconcileIdentitySQL()).Scan(
		&out.queuedSources,
		&out.acceptedSources,
		&out.conflictedSources,
		&out.insertedConflicts,
		&out.acceptedAliases,
		&out.acceptedRepresentions,
	)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return out
}

func assertHotResult(t *testing.T, got, want hotPathResult) {
	t.Helper()
	if got != want {
		t.Fatalf("hot path result = %s, want %s", got, want)
	}
}

func assertReconcileResult(t *testing.T, got, want reconcileResult) {
	t.Helper()
	if got != want {
		t.Fatalf("reconcile result = %s, want %s", got, want)
	}
}

func assertStatus(ctx context.Context, t *testing.T, db *sql.DB, serviceName, collectionName, resourceID, want string) {
	t.Helper()

	var got string
	err := db.QueryRowContext(ctx, `
		SELECT st.state
		FROM extension_resource_identity_status st
		JOIN extension_resources er ON er.uid = st.source_uid
		WHERE er.service_name = $1 AND er.collection_name = $2 AND er.resource_id = $3
	`, serviceName, collectionName, resourceID).Scan(&got)
	if err != nil {
		t.Fatalf("status query: %v", err)
	}
	if got != want {
		t.Fatalf("status = %q, want %q", got, want)
	}
}

func assertAcceptedRepresentations(ctx context.Context, t *testing.T, db *sql.DB, collectionName, resourceID string, want int) {
	t.Helper()

	var got int
	err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM accepted_platform_representations r
		JOIN extension_resource_identity_status st ON st.source_uid = r.source_uid
		WHERE r.platform_collection_name = $1
		  AND r.platform_resource_id = $2
		  AND st.state = 'accepted'
	`, collectionName, resourceID).Scan(&got)
	if err != nil {
		t.Fatalf("accepted representations query: %v", err)
	}
	if got != want {
		t.Fatalf("accepted representations = %d, want %d", got, want)
	}
}

func assertConflictCount(ctx context.Context, t *testing.T, db *sql.DB, collectionName, resourceID string, want int) {
	t.Helper()

	var got int
	err := db.QueryRowContext(ctx, `
		SELECT count(*)
		FROM identity_conflicts
		WHERE platform_collection_name = $1 AND platform_resource_id = $2
	`, collectionName, resourceID).Scan(&got)
	if err != nil {
		t.Fatalf("conflict count query: %v", err)
	}
	if got != want {
		t.Fatalf("conflict count = %d, want %d", got, want)
	}
}

func assertResolvedAlias(ctx context.Context, t *testing.T, db *sql.DB, namespace, key, value, wantCollection, wantResource string) {
	t.Helper()

	var gotCollection, gotResource string
	err := db.QueryRowContext(ctx, resolveAliasSQL, namespace, key, value).Scan(&gotCollection, &gotResource)
	if err != nil {
		t.Fatalf("resolve alias: %v", err)
	}
	if gotCollection != wantCollection || gotResource != wantResource {
		t.Fatalf("resolved alias = %s/%s, want %s/%s", gotCollection, gotResource, wantCollection, wantResource)
	}
}

func assertResolvedAliasMissing(ctx context.Context, t *testing.T, db *sql.DB, namespace, key, value string) {
	t.Helper()

	var gotCollection, gotResource string
	err := db.QueryRowContext(ctx, resolveAliasSQL, namespace, key, value).Scan(&gotCollection, &gotResource)
	if err == nil {
		t.Fatalf("resolved alias = %s/%s, want missing", gotCollection, gotResource)
	}
	if err != sql.ErrNoRows {
		t.Fatalf("resolve alias: %v", err)
	}
}

func runBatchTimings(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()

	const batchItems = 1_000

	preseedAccepted := func(name string, seed batch) {
		t.Helper()
		hot := execHotPath(ctx, t, db, seed)
		reconciled := execReconcile(ctx, t, db)
		t.Logf("%s seed hot path: %s", name, hot)
		t.Logf("%s seed reconciliation: %s", name, reconciled)
	}

	shapes := []struct {
		name       string
		setup      func()
		build      func(iteration int) batch
		assertLast func(t *testing.T, hot hotPathResult, reconciled reconcileResult)
	}{
		{
			name: "new_no_aliases",
			build: func(iteration int) batch {
				return buildBatch(iteration, rangeSpec{
					start: 10_001 + iteration*1_000, count: 1_000, serviceName: "noalias.fleetshift.io", collectionName: "clusters", generation: 1,
					aliasesFor: func(int, int) []alias { return nil },
				})
			},
			assertLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.queuedSources != 1_000 || reconciled.acceptedSources != 1_000 {
					t.Fatalf("new no-alias shape = hot %s reconcile %s, want all sources queued then accepted", hot, reconciled)
				}
			},
		},
		{
			name: "steady_no_aliases",
			setup: func() {
				preseedAccepted("steady_no_aliases", buildBatch(0, rangeSpec{
					start: 15_001, count: 1_000, serviceName: "steady-noalias.fleetshift.io", collectionName: "clusters", generation: 1,
					aliasesFor: func(int, int) []alias { return nil },
				}))
			},
			build: func(iteration int) batch {
				return buildBatch(iteration, rangeSpec{
					start: 15_001, count: 1_000, serviceName: "steady-noalias.fleetshift.io", collectionName: "clusters", generation: 2 + iteration,
					aliasesFor: func(int, int) []alias { return nil },
				})
			},
			assertLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.queuedSources != 0 || hot.markedPending != 0 || reconciled.queuedSources != 0 {
					t.Fatalf("steady no-alias shape = hot %s reconcile %s, want no identity work", hot, reconciled)
				}
			},
		},
		{
			name: "new_aliases",
			build: func(iteration int) batch {
				return buildBatch(iteration, rangeSpec{
					start: 20_001 + iteration*1_000, count: 1_000, serviceName: "newalias.fleetshift.io", collectionName: "clusters", generation: 1,
					aliasesFor: func(_ int, g int) []alias {
						return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("new-provider-%08d", g)}}
					},
				})
			},
			assertLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.storedAliasPayloads != 1_000 || hot.queuedSources != 1_000 || reconciled.acceptedAliases != 1_000 {
					t.Fatalf("new alias shape = hot %s reconcile %s, want alias payloads queued then accepted", hot, reconciled)
				}
			},
		},
		{
			name: "steady_same_aliases",
			setup: func() {
				preseedAccepted("steady_same_aliases", buildBatch(0, rangeSpec{
					start: 30_001, count: 1_000, serviceName: "steady-alias.fleetshift.io", collectionName: "clusters", generation: 1,
					aliasesFor: func(_ int, g int) []alias {
						return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("steady-provider-%08d", g)}}
					},
				}))
			},
			build: func(iteration int) batch {
				return buildBatch(iteration, rangeSpec{
					start: 30_001, count: 1_000, serviceName: "steady-alias.fleetshift.io", collectionName: "clusters", generation: 2 + iteration,
					aliasesFor: func(_ int, g int) []alias {
						return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("steady-provider-%08d", g)}}
					},
				})
			},
			assertLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.storedAliasPayloads != 0 || hot.queuedSources != 0 || reconciled.queuedSources != 0 {
					t.Fatalf("steady same-alias shape = hot %s reconcile %s, want no alias or identity work", hot, reconciled)
				}
			},
		},
		{
			name: "conflicting_aliases",
			setup: func() {
				preseedAccepted("conflicting_aliases", buildBatch(0, rangeSpec{
					start: 50_001, count: 1_000, serviceName: "identity-owner.fleetshift.io", collectionName: "clusters", generation: 1,
					aliasesFor: func(_ int, g int) []alias {
						return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("owner-provider-%08d", g)}}
					},
				}))
			},
			build: func(iteration int) batch {
				return buildBatch(iteration, rangeSpec{
					start: 50_001, count: 1_000, serviceName: fmt.Sprintf("conflict-%02d.fleetshift.io", iteration), collectionName: "clusters", generation: 1,
					aliasesFor: func(iteration, g int) []alias {
						return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("conflicting-provider-%02d-%08d", iteration, g)}}
					},
				})
			},
			assertLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.queuedSources != 1_000 || reconciled.conflictedSources != 1_000 || reconciled.acceptedSources != 0 {
					t.Fatalf("conflicting alias shape = hot %s reconcile %s, want all sources queued then conflicted", hot, reconciled)
				}
			},
		},
	}

	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			if shape.setup != nil {
				shape.setup()
			}
			var hotDurations []time.Duration
			var reconcileDurations []time.Duration
			var lastHot hotPathResult
			var lastReconcile reconcileResult
			for i := 0; i < 4; i++ {
				b := shape.build(i)
				start := time.Now()
				lastHot = execHotPath(ctx, t, db, b)
				hotDurations = append(hotDurations, time.Since(start))

				start = time.Now()
				lastReconcile = execReconcile(ctx, t, db)
				reconcileDurations = append(reconcileDurations, time.Since(start))
			}
			t.Logf("%s hot path: %s; last %s", shape.name, formatDurations(hotDurations, batchItems), lastHot)
			t.Logf("%s reconciliation: %s; last %s", shape.name, formatDurations(reconcileDurations, batchItems), lastReconcile)
			if shape.assertLast != nil {
				shape.assertLast(t, lastHot, lastReconcile)
			}
		})
	}
}

func formatDurations(durations []time.Duration, items int) string {
	parts := make([]string, len(durations))
	for i, d := range durations {
		parts[i] = fmt.Sprintf(
			"#%d=%s %.3fms/item (%d items)",
			i+1,
			d.Round(time.Microsecond),
			float64(d.Microseconds())/1000.0/float64(items),
			items,
		)
	}
	return strings.Join(parts, ", ")
}

func TestFormatDurations(t *testing.T) {
	t.Parallel()

	got := formatDurations([]time.Duration{
		2 * time.Second,
		1500 * time.Millisecond,
	}, 1_000)

	want := "#1=2s 2.000ms/item (1000 items), #2=1.5s 1.500ms/item (1000 items)"
	if got != want {
		t.Fatalf("formatDurations() = %q, want %q", got, want)
	}
}

func runLongBenchmark(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()

	const (
		batchSize          = 5_000
		aliasCount         = 1_000
		mixedNoAliasCount  = 4_800
		mixedAliasCount    = 190
		mixedChangingCount = 10
		warmupIterations   = 5
		measureIterations  = 20
	)

	preseedAccepted := func(name string, seed batch) {
		t.Helper()
		hot := execHotPath(ctx, t, db, seed)
		reconciled := execReconcile(ctx, t, db)
		t.Logf("%s seed hot path: %s", name, hot)
		t.Logf("%s seed reconciliation: %s", name, reconciled)
	}

	type longShape struct {
		name           string
		itemsPerBatch  int
		setup          func()
		build          func(iteration int) batch
		expectLast     func(t *testing.T, hot hotPathResult, reconciled reconcileResult)
		reconcileEvery bool
		logExpectation string
	}

	shapes := []longShape{
		{
			name:          "steady_no_aliases_5k",
			itemsPerBatch: batchSize,
			setup: func() {
				preseedAccepted("long steady_no_aliases", buildBatch(0, rangeSpec{
					start: 100_001, count: batchSize, serviceName: "long-steady-noalias.fleetshift.io", collectionName: "clusters", generation: 1,
					aliasesFor: func(int, int) []alias { return nil },
				}))
			},
			build: func(iteration int) batch {
				return buildBatch(iteration, rangeSpec{
					start: 100_001, count: batchSize, serviceName: "long-steady-noalias.fleetshift.io", collectionName: "clusters", generation: 2 + iteration,
					aliasesFor: func(int, int) []alias { return nil },
				})
			},
			expectLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.queuedSources != 0 || hot.storedAliasPayloads != 0 || reconciled.queuedSources != 0 {
					t.Fatalf("steady no-alias long shape = hot %s reconcile %s, want inventory-only writes", hot, reconciled)
				}
			},
			reconcileEvery: true,
			logExpectation: "accepted sources, unchanged alias fingerprint, inventory-only hot path",
		},
		{
			name:          "steady_same_aliases_5k",
			itemsPerBatch: batchSize,
			setup: func() {
				preseedAccepted("long steady_same_aliases", buildBatch(0, rangeSpec{
					start: 110_001, count: batchSize, serviceName: "long-steady-alias.fleetshift.io", collectionName: "clusters", generation: 1,
					aliasesFor: func(_ int, g int) []alias {
						return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("long-steady-provider-%08d", g)}}
					},
				}))
			},
			build: func(iteration int) batch {
				return buildBatch(iteration, rangeSpec{
					start: 110_001, count: batchSize, serviceName: "long-steady-alias.fleetshift.io", collectionName: "clusters", generation: 2 + iteration,
					aliasesFor: func(_ int, g int) []alias {
						return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("long-steady-provider-%08d", g)}}
					},
				})
			},
			expectLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.queuedSources != 0 || hot.storedAliasPayloads != 0 || reconciled.queuedSources != 0 {
					t.Fatalf("steady same-alias long shape = hot %s reconcile %s, want inventory-only writes", hot, reconciled)
				}
			},
			reconcileEvery: true,
			logExpectation: "accepted sources with one alias each, unchanged alias fingerprint",
		},
		{
			name:          "mixed_realistic_5k",
			itemsPerBatch: batchSize,
			setup: func() {
				preseedAccepted("long mixed_realistic", buildBatch(0,
					rangeSpec{
						start: 120_001, count: mixedNoAliasCount, serviceName: "long-mixed-noalias.fleetshift.io", collectionName: "clusters", generation: 1,
						aliasesFor: func(int, int) []alias { return nil },
					},
					rangeSpec{
						start: 130_001, count: mixedAliasCount, serviceName: "long-mixed-samealias.fleetshift.io", collectionName: "clusters", generation: 1,
						aliasesFor: func(_ int, g int) []alias {
							return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("long-mixed-provider-%08d", g)}}
						},
					},
					rangeSpec{
						start: 140_001, count: mixedChangingCount, serviceName: "long-mixed-changing.fleetshift.io", collectionName: "clusters", generation: 1,
						aliasesFor: func(_ int, g int) []alias {
							return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("long-changing-provider-%08d-v000", g)}}
						},
					},
				))
			},
			build: func(iteration int) batch {
				return buildBatch(iteration,
					rangeSpec{
						start: 120_001, count: mixedNoAliasCount, serviceName: "long-mixed-noalias.fleetshift.io", collectionName: "clusters", generation: 2 + iteration,
						aliasesFor: func(int, int) []alias { return nil },
					},
					rangeSpec{
						start: 130_001, count: mixedAliasCount, serviceName: "long-mixed-samealias.fleetshift.io", collectionName: "clusters", generation: 2 + iteration,
						aliasesFor: func(_ int, g int) []alias {
							return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("long-mixed-provider-%08d", g)}}
						},
					},
					rangeSpec{
						start: 140_001, count: mixedChangingCount, serviceName: "long-mixed-changing.fleetshift.io", collectionName: "clusters", generation: 2 + iteration,
						aliasesFor: func(iteration, g int) []alias {
							return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("long-changing-provider-%08d-v%03d", g, iteration+1)}}
						},
					},
				)
			},
			expectLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.queuedSources != mixedChangingCount || hot.storedAliasPayloads != mixedChangingCount || reconciled.acceptedSources != mixedChangingCount {
					t.Fatalf("mixed realistic long shape = hot %s reconcile %s, want only changed aliases queued and accepted", hot, reconciled)
				}
			},
			reconcileEvery: true,
			logExpectation: "96% no aliases, 3.8% unchanged aliases, 0.2% changed aliases",
		},
		{
			name:          "new_no_aliases_5k",
			itemsPerBatch: batchSize,
			build: func(iteration int) batch {
				return buildBatch(iteration, rangeSpec{
					start: 200_001 + iteration*batchSize, count: batchSize, serviceName: "long-new-noalias.fleetshift.io", collectionName: "clusters", generation: 1,
					aliasesFor: func(int, int) []alias { return nil },
				})
			},
			expectLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.queuedSources != batchSize || reconciled.acceptedSources != batchSize {
					t.Fatalf("new no-alias long shape = hot %s reconcile %s, want all sources queued and accepted", hot, reconciled)
				}
			},
			reconcileEvery: true,
			logExpectation: "initial import of inventory-only resources",
		},
		{
			name:          "new_aliases_1k",
			itemsPerBatch: aliasCount,
			build: func(iteration int) batch {
				return buildBatch(iteration, rangeSpec{
					start: 300_001 + iteration*aliasCount, count: aliasCount, serviceName: "long-new-alias.fleetshift.io", collectionName: "clusters", generation: 1,
					aliasesFor: func(_ int, g int) []alias {
						return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("long-new-provider-%08d", g)}}
					},
				})
			},
			expectLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.queuedSources != aliasCount || hot.storedAliasPayloads != aliasCount || reconciled.acceptedAliases != aliasCount {
					t.Fatalf("new alias long shape = hot %s reconcile %s, want all aliases queued and accepted", hot, reconciled)
				}
			},
			reconcileEvery: true,
			logExpectation: "initial import of resources with one alias each",
		},
		{
			name:          "conflicting_aliases_1k",
			itemsPerBatch: aliasCount,
			setup: func() {
				preseedAccepted("long conflicting_aliases", buildBatch(0, rangeSpec{
					start: 400_001, count: aliasCount, serviceName: "long-conflict-owner.fleetshift.io", collectionName: "clusters", generation: 1,
					aliasesFor: func(_ int, g int) []alias {
						return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("long-owner-provider-%08d", g)}}
					},
				}))
			},
			build: func(iteration int) batch {
				return buildBatch(iteration, rangeSpec{
					start: 400_001, count: aliasCount, serviceName: fmt.Sprintf("long-conflict-%03d.fleetshift.io", iteration), collectionName: "clusters", generation: 1,
					aliasesFor: func(iteration, g int) []alias {
						return []alias{{namespace: "cloud", key: "provider-id", value: fmt.Sprintf("long-conflicting-provider-%03d-%08d", iteration, g)}}
					},
				})
			},
			expectLast: func(t *testing.T, hot hotPathResult, reconciled reconcileResult) {
				t.Helper()
				if hot.queuedSources != aliasCount || reconciled.conflictedSources != aliasCount || reconciled.acceptedSources != 0 {
					t.Fatalf("conflicting alias long shape = hot %s reconcile %s, want all sources conflicted", hot, reconciled)
				}
			},
			reconcileEvery: true,
			logExpectation: "worst-case batch where every reported alias conflicts",
		},
	}

	t.Logf("long benchmark config: warmup=%d measured=%d batch_size=%d", warmupIterations, measureIterations, batchSize)
	for _, shape := range shapes {
		t.Run(shape.name, func(t *testing.T) {
			if shape.setup != nil {
				shape.setup()
			}

			var hotMeasured []time.Duration
			var reconcileMeasured []time.Duration
			var lastHot hotPathResult
			var lastReconcile reconcileResult
			totalIterations := warmupIterations + measureIterations
			for i := 0; i < totalIterations; i++ {
				b := shape.build(i)
				start := time.Now()
				lastHot = execHotPath(ctx, t, db, b)
				hotDuration := time.Since(start)

				start = time.Now()
				if shape.reconcileEvery {
					lastReconcile = execReconcile(ctx, t, db)
				}
				reconcileDuration := time.Since(start)

				if i >= warmupIterations {
					hotMeasured = append(hotMeasured, hotDuration)
					reconcileMeasured = append(reconcileMeasured, reconcileDuration)
				}
			}

			t.Logf("%s expectation: %s", shape.name, shape.logExpectation)
			t.Logf("%s hot path summary: %s; last %s", shape.name, summarizeDurations(hotMeasured, shape.itemsPerBatch), lastHot)
			t.Logf("%s reconciliation summary: %s; last %s", shape.name, summarizeDurations(reconcileMeasured, shape.itemsPerBatch), lastReconcile)
			if shape.expectLast != nil {
				shape.expectLast(t, lastHot, lastReconcile)
			}
		})
	}
}

func summarizeDurations(durations []time.Duration, items int) string {
	if len(durations) == 0 {
		return "n=0"
	}

	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	var total time.Duration
	for _, d := range durations {
		total += d
	}

	minimum := sorted[0]
	maximum := sorted[len(sorted)-1]
	median := percentileDuration(sorted, 0.50)
	p90 := percentileDuration(sorted, 0.90)
	p95 := percentileDuration(sorted, 0.95)
	mean := total / time.Duration(len(durations))

	return fmt.Sprintf(
		"n=%d min=%s mean=%s median=%s p90=%s p95=%s max=%s mean/item=%.4fms p95/item=%.4fms",
		len(durations),
		minimum.Round(time.Microsecond),
		mean.Round(time.Microsecond),
		median.Round(time.Microsecond),
		p90.Round(time.Microsecond),
		p95.Round(time.Microsecond),
		maximum.Round(time.Microsecond),
		float64(mean.Microseconds())/1000.0/float64(items),
		float64(p95.Microseconds())/1000.0/float64(items),
	)
}

func percentileDuration(sorted []time.Duration, percentile float64) time.Duration {
	if len(sorted) == 1 {
		return sorted[0]
	}
	index := int(percentile * float64(len(sorted)-1))
	if index < 0 {
		return sorted[0]
	}
	if index >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	return sorted[index]
}

const resolveAliasSQL = `
WITH candidates AS (
	SELECT DISTINCT a.platform_collection_name, a.platform_resource_id
	FROM accepted_alias_assertions a
	JOIN extension_resource_identity_status st ON st.source_uid = a.source_uid
	WHERE a.namespace = $1
	  AND a.key = $2
	  AND a.value = $3
	  AND st.state = 'accepted'
),
resolved AS (
	SELECT
		min(platform_collection_name) AS platform_collection_name,
		min(platform_resource_id) AS platform_resource_id,
		count(*) AS target_count
	FROM candidates
)
SELECT platform_collection_name, platform_resource_id
FROM resolved
WHERE target_count = 1`

func replaceInventoryAssertionsSQL() string {
	return `WITH raw_reports AS MATERIALIZED (
	SELECT
		idx,
		service_name,
		type_name,
		collection_name,
		resource_id,
		candidate_uid::uuid AS candidate_uid,
		observation::jsonb AS observation,
		labels::jsonb AS labels,
		conditions::jsonb AS conditions,
		observed_at,
		received_at,
		reported_alias_fingerprint,
		reported_aliases::jsonb AS reported_aliases
	FROM UNNEST(
		$1::int[],
		$2::text[],
		$3::text[],
		$4::text[],
		$5::text[],
		$6::text[],
		$7::text[],
		$8::text[],
		$9::text[],
		$10::timestamptz[],
		$11::timestamptz[],
		$12::bytea[],
		$13::text[]
	) AS x(idx, service_name, type_name, collection_name, resource_id, candidate_uid, observation, labels, conditions, observed_at, received_at, reported_alias_fingerprint, reported_aliases)
),
resolved_er AS (
	INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, alias_fingerprint, reported_aliases, created_at, updated_at)
	SELECT candidate_uid, service_name, type_name, collection_name, resource_id, reported_alias_fingerprint, reported_aliases, received_at, received_at
	FROM raw_reports
	ON CONFLICT (service_name, collection_name, resource_id) DO NOTHING
	RETURNING uid, service_name, collection_name, resource_id
),
input_reports AS MATERIALIZED (
	SELECT
		rr.idx,
		COALESCE(res.uid, ext.uid) AS source_uid,
		ext.alias_fingerprint AS stored_alias_fingerprint,
		rr.reported_alias_fingerprint,
		rr.reported_aliases,
		rr.collection_name,
		rr.resource_id,
		rr.observation,
		rr.labels,
		rr.conditions,
		rr.observed_at,
		rr.received_at
	FROM raw_reports rr
	LEFT JOIN resolved_er res
	  ON res.service_name = rr.service_name
	 AND res.collection_name = rr.collection_name
	 AND res.resource_id = rr.resource_id
	LEFT JOIN extension_resources ext
	  ON ext.service_name = rr.service_name
	 AND ext.collection_name = rr.collection_name
	 AND ext.resource_id = rr.resource_id
),
upsert_inventory AS (
	INSERT INTO extension_resource_inventory (extension_resource_uid, observation, labels, conditions, observed_at, updated_at)
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
needs_identity_write AS MATERIALIZED (
	SELECT *
	FROM input_reports
	WHERE reported_alias_fingerprint IS DISTINCT FROM stored_alias_fingerprint
),
cleared_conflicts AS (
	DELETE FROM identity_conflicts c
	USING needs_identity_write nr
	WHERE c.source_uid = nr.source_uid
	RETURNING 1
),
marked_pending AS (
	INSERT INTO extension_resource_identity_status (source_uid, state, conflict_count, updated_at)
	SELECT source_uid, 'pending', 0, received_at
	FROM needs_identity_write
	ON CONFLICT (source_uid)
	DO UPDATE SET state = 'pending', conflict_count = 0, updated_at = EXCLUDED.updated_at
	RETURNING 1
),
queued_sources AS (
	INSERT INTO identity_reconciliation_queue (source_uid, enqueued_at)
	SELECT source_uid, received_at
	FROM needs_identity_write
	ON CONFLICT (source_uid)
	DO UPDATE SET enqueued_at = EXCLUDED.enqueued_at
	RETURNING 1
),
updated_alias_payloads AS (
	UPDATE extension_resources er
	SET alias_fingerprint = nr.reported_alias_fingerprint,
	    reported_aliases = nr.reported_aliases,
	    updated_at = nr.received_at
	FROM needs_identity_write nr
	WHERE er.uid = nr.source_uid
	  AND NOT EXISTS (SELECT 1 FROM resolved_er res WHERE res.uid = nr.source_uid)
	RETURNING 1
)
SELECT
	(SELECT count(*) FROM upsert_inventory) AS updated_inventory,
	((SELECT count(*) FROM resolved_er) + (SELECT count(*) FROM updated_alias_payloads)) AS stored_alias_payloads,
	(SELECT count(*) FROM cleared_conflicts) AS cleared_conflicts,
	(SELECT count(*) FROM marked_pending) AS marked_pending,
	(SELECT count(*) FROM queued_sources) AS queued_sources`
}

func reconcileIdentitySQL() string {
	return `WITH queued AS MATERIALIZED (
	SELECT q.source_uid
	FROM identity_reconciliation_queue q
	ORDER BY q.enqueued_at, q.source_uid
	FOR UPDATE SKIP LOCKED
),
source_targets AS MATERIALIZED (
	SELECT
		q.source_uid,
		er.collection_name AS platform_collection_name,
		er.resource_id AS platform_resource_id,
		er.reported_aliases
	FROM queued q
	JOIN extension_resources er ON er.uid = q.source_uid
),
source_aliases AS MATERIALIZED (
	SELECT
		st.source_uid,
		alias.namespace,
		alias.key,
		alias.value,
		st.platform_collection_name,
		st.platform_resource_id
	FROM source_targets st
	CROSS JOIN LATERAL jsonb_to_recordset(st.reported_aliases) AS alias(namespace text, key text, value text)
),
alias_value_conflicts AS (
	SELECT
		sa.source_uid,
		sa.namespace,
		sa.key,
		sa.value,
		sa.platform_collection_name,
		sa.platform_resource_id,
		'alias_value_claimed_by_other_resource'::text AS conflict_kind,
		accepted.platform_collection_name AS conflicting_collection_name,
		accepted.platform_resource_id AS conflicting_resource_id,
		accepted.value AS conflicting_value
	FROM source_aliases sa
	JOIN accepted_alias_assertions accepted
	  ON accepted.namespace = sa.namespace
	 AND accepted.key = sa.key
	 AND accepted.value = sa.value
	 AND accepted.source_uid <> sa.source_uid
	JOIN extension_resource_identity_status accepted_status
	  ON accepted_status.source_uid = accepted.source_uid
	 AND accepted_status.state = 'accepted'
	WHERE accepted.platform_collection_name <> sa.platform_collection_name
	   OR accepted.platform_resource_id <> sa.platform_resource_id
),
resource_key_conflicts AS (
	SELECT
		sa.source_uid,
		sa.namespace,
		sa.key,
		sa.value,
		sa.platform_collection_name,
		sa.platform_resource_id,
		'resource_has_different_alias_value'::text AS conflict_kind,
		accepted.platform_collection_name AS conflicting_collection_name,
		accepted.platform_resource_id AS conflicting_resource_id,
		accepted.value AS conflicting_value
	FROM source_aliases sa
	JOIN accepted_alias_assertions accepted
	  ON accepted.namespace = sa.namespace
	 AND accepted.key = sa.key
	 AND accepted.platform_collection_name = sa.platform_collection_name
	 AND accepted.platform_resource_id = sa.platform_resource_id
	 AND accepted.source_uid <> sa.source_uid
	JOIN extension_resource_identity_status accepted_status
	  ON accepted_status.source_uid = accepted.source_uid
	 AND accepted_status.state = 'accepted'
	WHERE accepted.value <> sa.value
),
raw_conflicts AS MATERIALIZED (
	SELECT * FROM alias_value_conflicts
	UNION ALL
	SELECT * FROM resource_key_conflicts
),
all_conflicts AS MATERIALIZED (
	SELECT DISTINCT ON (source_uid, namespace, key, conflict_kind)
		source_uid,
		namespace,
		key,
		value,
		platform_collection_name,
		platform_resource_id,
		conflict_kind,
		conflicting_collection_name,
		conflicting_resource_id,
		conflicting_value
	FROM raw_conflicts
	ORDER BY source_uid, namespace, key, conflict_kind, conflicting_collection_name, conflicting_resource_id, conflicting_value
),
conflicted_sources AS MATERIALIZED (
	SELECT DISTINCT source_uid FROM all_conflicts
),
accepted_sources AS MATERIALIZED (
	SELECT source_uid FROM queued
	EXCEPT
	SELECT source_uid FROM conflicted_sources
),
inserted_platform_resources AS (
	INSERT INTO platform_resources (collection_name, resource_id, created_at)
	SELECT DISTINCT platform_collection_name, platform_resource_id, now()
	FROM source_targets
	WHERE source_uid IN (SELECT source_uid FROM accepted_sources)
	ON CONFLICT DO NOTHING
	RETURNING 1
),
accepted_representations AS (
	INSERT INTO accepted_platform_representations (platform_collection_name, platform_resource_id, source_uid, accepted_at)
	SELECT platform_collection_name, platform_resource_id, source_uid, now()
	FROM source_targets
	WHERE source_uid IN (SELECT source_uid FROM accepted_sources)
	ON CONFLICT (source_uid)
	DO UPDATE SET
		platform_collection_name = EXCLUDED.platform_collection_name,
		platform_resource_id = EXCLUDED.platform_resource_id,
		accepted_at = EXCLUDED.accepted_at
	RETURNING 1
),
deleted_accepted_aliases AS (
	DELETE FROM accepted_alias_assertions a
	USING accepted_sources s
	WHERE a.source_uid = s.source_uid
	RETURNING 1
),
accepted_aliases AS (
	INSERT INTO accepted_alias_assertions (
		source_uid,
		namespace,
		key,
		value,
		platform_collection_name,
		platform_resource_id,
		accepted_at
	)
	SELECT source_uid, namespace, key, value, platform_collection_name, platform_resource_id, now()
	FROM source_aliases
	WHERE source_uid IN (SELECT source_uid FROM accepted_sources)
	  AND (SELECT count(*) FROM accepted_representations) >= 0
	  AND (SELECT count(*) FROM deleted_accepted_aliases) >= 0
	ON CONFLICT (source_uid, namespace, key)
	DO UPDATE SET
		value = EXCLUDED.value,
		platform_collection_name = EXCLUDED.platform_collection_name,
		platform_resource_id = EXCLUDED.platform_resource_id,
		accepted_at = EXCLUDED.accepted_at
	RETURNING 1
),
inserted_conflicts AS (
	INSERT INTO identity_conflicts (
		source_uid,
		namespace,
		key,
		value,
		platform_collection_name,
		platform_resource_id,
		conflict_kind,
		conflicting_collection_name,
		conflicting_resource_id,
		conflicting_value,
		detected_at
	)
	SELECT
		source_uid,
		namespace,
		key,
		value,
		platform_collection_name,
		platform_resource_id,
		conflict_kind,
		conflicting_collection_name,
		conflicting_resource_id,
		conflicting_value,
		now()
	FROM all_conflicts
	ON CONFLICT (source_uid, namespace, key, conflict_kind)
	DO UPDATE SET
		value = EXCLUDED.value,
		platform_collection_name = EXCLUDED.platform_collection_name,
		platform_resource_id = EXCLUDED.platform_resource_id,
		conflicting_collection_name = EXCLUDED.conflicting_collection_name,
		conflicting_resource_id = EXCLUDED.conflicting_resource_id,
		conflicting_value = EXCLUDED.conflicting_value,
		detected_at = EXCLUDED.detected_at
	RETURNING 1
),
accepted_status AS (
	INSERT INTO extension_resource_identity_status (source_uid, state, conflict_count, updated_at)
	SELECT source_uid, 'accepted', 0, now()
	FROM accepted_sources
	ON CONFLICT (source_uid)
	DO UPDATE SET state = 'accepted', conflict_count = 0, updated_at = EXCLUDED.updated_at
	RETURNING 1
),
conflict_counts AS (
	SELECT source_uid, count(*)::int AS conflict_count
	FROM all_conflicts
	GROUP BY source_uid
),
conflict_status AS (
	INSERT INTO extension_resource_identity_status (source_uid, state, conflict_count, updated_at)
	SELECT source_uid, 'conflict', conflict_count, now()
	FROM conflict_counts
	ON CONFLICT (source_uid)
	DO UPDATE SET state = 'conflict', conflict_count = EXCLUDED.conflict_count, updated_at = EXCLUDED.updated_at
	RETURNING 1
),
deleted_queue AS (
	DELETE FROM identity_reconciliation_queue q
	USING queued
	WHERE q.source_uid = queued.source_uid
	RETURNING 1
)
SELECT
	(SELECT count(*) FROM queued) AS queued_sources,
	(SELECT count(*) FROM accepted_sources) AS accepted_sources,
	(SELECT count(*) FROM conflicted_sources) AS conflicted_sources,
	(SELECT count(*) FROM inserted_conflicts) AS inserted_conflicts,
	(SELECT count(*) FROM accepted_aliases) AS accepted_aliases,
	(SELECT count(*) FROM accepted_representations) AS accepted_representations`
}

const schemaSQL = `
CREATE TABLE extension_resources (
	uid uuid PRIMARY KEY,
	service_name text NOT NULL,
	type_name text NOT NULL,
	collection_name text NOT NULL,
	resource_id text NOT NULL,
	alias_fingerprint bytea,
	reported_aliases jsonb NOT NULL DEFAULT '[]'::jsonb,
	created_at timestamptz NOT NULL,
	updated_at timestamptz NOT NULL,
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

CREATE TABLE extension_resource_identity_status (
	source_uid uuid PRIMARY KEY
		REFERENCES extension_resources(uid) ON DELETE CASCADE,
	state text NOT NULL CHECK (state IN ('pending', 'accepted', 'conflict')),
	conflict_count int NOT NULL DEFAULT 0,
	updated_at timestamptz NOT NULL
);

CREATE TABLE identity_reconciliation_queue (
	source_uid uuid PRIMARY KEY
		REFERENCES extension_resources(uid) ON DELETE CASCADE,
	enqueued_at timestamptz NOT NULL
);

CREATE TABLE platform_resources (
	collection_name text NOT NULL,
	resource_id text NOT NULL,
	created_at timestamptz NOT NULL,
	PRIMARY KEY (collection_name, resource_id)
);

CREATE TABLE accepted_platform_representations (
	platform_collection_name text NOT NULL,
	platform_resource_id text NOT NULL,
	source_uid uuid NOT NULL
		REFERENCES extension_resources(uid) ON DELETE CASCADE,
	accepted_at timestamptz NOT NULL,
	PRIMARY KEY (platform_collection_name, platform_resource_id, source_uid),
	UNIQUE (source_uid),
	FOREIGN KEY (platform_collection_name, platform_resource_id)
		REFERENCES platform_resources(collection_name, resource_id) ON DELETE CASCADE
);

CREATE TABLE accepted_alias_assertions (
	source_uid uuid NOT NULL
		REFERENCES extension_resources(uid) ON DELETE CASCADE,
	namespace text NOT NULL,
	key text NOT NULL,
	value text NOT NULL,
	platform_collection_name text NOT NULL,
	platform_resource_id text NOT NULL,
	accepted_at timestamptz NOT NULL,
	PRIMARY KEY (source_uid, namespace, key),
	FOREIGN KEY (platform_collection_name, platform_resource_id, source_uid)
		REFERENCES accepted_platform_representations(platform_collection_name, platform_resource_id, source_uid) ON DELETE CASCADE
);

CREATE INDEX accepted_alias_assertions_value_idx
	ON accepted_alias_assertions(namespace, key, value);

CREATE INDEX accepted_alias_assertions_resource_key_idx
	ON accepted_alias_assertions(namespace, key, platform_collection_name, platform_resource_id);

CREATE TABLE identity_conflicts (
	source_uid uuid NOT NULL
		REFERENCES extension_resources(uid) ON DELETE CASCADE,
	namespace text NOT NULL,
	key text NOT NULL,
	value text NOT NULL,
	platform_collection_name text NOT NULL,
	platform_resource_id text NOT NULL,
	conflict_kind text NOT NULL,
	conflicting_collection_name text NOT NULL,
	conflicting_resource_id text NOT NULL,
	conflicting_value text NOT NULL,
	detected_at timestamptz NOT NULL,
	PRIMARY KEY (source_uid, namespace, key, conflict_kind)
);

CREATE INDEX identity_conflicts_platform_resource_idx
	ON identity_conflicts(platform_collection_name, platform_resource_id);
`
