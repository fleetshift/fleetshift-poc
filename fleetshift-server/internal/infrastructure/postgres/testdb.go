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
	if isPodmanAvailable() {
		os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
}

var (
	containerOnce sync.Once
	containerCtr  *tcpostgres.PostgresContainer
	containerConn string
	containerErr  error
)

func detectProvider() testcontainers.ContainerCustomizer {
	if isPodmanAvailable() {
		return testcontainers.WithProvider(testcontainers.ProviderPodman)
	}
	return testcontainers.WithProvider(testcontainers.ProviderDefault)
}

func startContainer() (*tcpostgres.PostgresContainer, string, error) {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("fleetshift_test"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp")),
		detectProvider(),
	)
	if err != nil {
		return nil, "", fmt.Errorf("start postgres container: %w", err)
	}
	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, "", fmt.Errorf("get connection string: %w", err)
	}
	return ctr, connStr, nil
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
}

func replaceDBName(connStr, dbName string) string {
	u, err := url.Parse(connStr)
	if err != nil {
		return connStr
	}
	u.Path = "/" + dbName
	return u.String()
}
