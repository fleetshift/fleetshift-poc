package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"sync"
	"testing"

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

var (
	containerOnce sync.Once
	containerCtr  *tcpostgres.PostgresContainer
	containerConn string
	containerErr  error

	// benchContainerOnce/benchContainerCtr and friends back a second,
	// independent Postgres container used only by
	// inventory_bench_test.go's openBenchDB, kept separate from
	// containerOnce's container: contract tests don't care about
	// buffer-pool size and get the postgres:18 image's lean default,
	// while the benchmark gets a production-representative one via
	// startBenchContainer.
	benchContainerOnce sync.Once
	benchContainerCtr  *tcpostgres.PostgresContainer
	benchContainerConn string
	benchContainerErr  error
)

func detectProvider() testcontainers.ContainerCustomizer {
	if os.Getenv("TESTCONTAINERS_PROVIDER") == "docker" {
		return testcontainers.WithProvider(testcontainers.ProviderDefault)
	}
	if isPodmanAvailable() {
		return testcontainers.WithProvider(testcontainers.ProviderPodman)
	}
	return testcontainers.WithProvider(testcontainers.ProviderDefault)
}

func startContainer(extraOpts ...testcontainers.ContainerCustomizer) (*tcpostgres.PostgresContainer, string, error) {
	ctx := context.Background()
	opts := append([]testcontainers.ContainerCustomizer{
		tcpostgres.WithDatabase("fleetshift_test"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp")),
		detectProvider(),
	}, extraOpts...)
	ctr, err := tcpostgres.Run(ctx, "postgres:18", opts...)
	if err != nil {
		return nil, "", fmt.Errorf("start postgres container: %w", err)
	}
	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, "", fmt.Errorf("get connection string: %w", err)
	}
	return ctr, connStr, nil
}

// startBenchContainer is startContainer's benchmark-only sibling:
// same image and credentials, but with shared_buffers raised from the
// postgres:18 image's 128MB default to 1GB (and max_wal_size raised
// to match, so checkpoint frequency doesn't become the new
// bottleneck). inventory_bench_test.go's corpus spans a dozen tables
// at ~100k rows each; at the 128MB default, a single CTE-chained
// statement's own later CTEs can evict buffer-pool pages its earlier
// CTEs just touched, adding a real but test-container-specific cost
// that a properly-sized production Postgres (typically a much larger
// fraction of system RAM) wouldn't pay -- 1GB was enough headroom to
// make that effect go away in this corpus at the benchmark's batch
// sizes. See extension_resource_repo.go's aliasFoldCTEs doc comment
// for the EXPLAIN-driven diagnosis that led here.
func startBenchContainer() (*tcpostgres.PostgresContainer, string, error) {
	return startContainer(testcontainers.WithCmd("postgres", "-c", "shared_buffers=1GB", "-c", "max_wal_size=4GB"))
}

var (
	testDBCounter int
	testDBMu      sync.Mutex
)

func OpenTestDB(t *testing.T) *sql.DB {
	t.Helper()
	containerOnce.Do(func() {
		containerCtr, containerConn, containerErr = startContainer()
	})
	if containerErr != nil {
		t.Fatalf("postgres container: %v", containerErr)
	}

	adminDB, err := sql.Open("pgx", containerConn)
	if err != nil {
		t.Fatalf("open admin connection: %v", err)
	}
	defer adminDB.Close()

	testDBMu.Lock()
	testDBCounter++
	dbName := fmt.Sprintf("test_%d", testDBCounter)
	testDBMu.Unlock()

	if _, err := adminDB.Exec("CREATE DATABASE " + dbName); err != nil {
		t.Fatalf("create test database: %v", err)
	}

	testConnStr := replaceDBName(containerConn, dbName)
	db, err := Open(testConnStr)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
		cleanupDB, err := sql.Open("pgx", containerConn)
		if err == nil {
			cleanupDB.Exec("DROP DATABASE IF EXISTS " + dbName + " WITH (FORCE)")
			cleanupDB.Close()
		}
	})
	return db
}

func TerminateTestContainer() {
	if containerCtr != nil {
		containerCtr.Terminate(context.Background())
	}
	if benchContainerCtr != nil {
		benchContainerCtr.Terminate(context.Background())
	}
}

func replaceDBName(connStr, dbName string) string {
	u, err := url.Parse(connStr)
	if err != nil {
		return connStr
	}
	u.Path = "/" + dbName
	return u.String()
}
