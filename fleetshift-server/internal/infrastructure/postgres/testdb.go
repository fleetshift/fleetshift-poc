package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	containerOnce sync.Once
	containerConn string
	containerErr  error
)

func startContainer() (string, error) {
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:18",
		tcpostgres.WithDatabase("fleetshift_test"),
		tcpostgres.WithUsername("test"),
		tcpostgres.WithPassword("test"),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("5432/tcp")),
	)
	if err != nil {
		return "", fmt.Errorf("start postgres container: %w", err)
	}
	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return "", fmt.Errorf("get connection string: %w", err)
	}
	return connStr, nil
}

var (
	testDBCounter int
	testDBMu      sync.Mutex
)

func OpenTestDB(t *testing.T) *sql.DB {
	t.Helper()
	containerOnce.Do(func() {
		containerConn, containerErr = startContainer()
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

func replaceDBName(connStr, dbName string) string {
	// connStr format: postgres://user:pass@host:port/dbname?params
	lastSlash := strings.LastIndex(connStr, "/")
	if lastSlash < 0 {
		return connStr
	}
	rest := connStr[lastSlash+1:]
	qmark := strings.Index(rest, "?")
	if qmark >= 0 {
		return connStr[:lastSlash+1] + dbName + rest[qmark:]
	}
	return connStr[:lastSlash+1] + dbName
}
