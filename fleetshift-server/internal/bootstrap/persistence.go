package bootstrap

import (
	"database/sql"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	pgstore "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/extensionresource"
)

// persistence holds opened storage dependencies for Start.
type persistence struct {
	db              *sql.DB
	store           domain.Store
	vault           domain.Vault
	authMethodRepo  domain.AuthMethodRepository
	activeResources *extensionresource.ActiveResourceRegistry
}

// openPersistence opens the selected database and constructs concrete stores.
// activeResources starts empty and is populated as managed schemas activate.
func openPersistence(database Database) (persistence, error) {
	activeResources := extensionresource.NewActiveResourceRegistry()
	switch db := database.(type) {
	case Postgres:
		sqlDB, err := pgstore.Open(db.DriverDSN)
		if err != nil {
			return persistence{}, fmt.Errorf("open database: %w", err)
		}
		return persistence{
			db:              sqlDB,
			store:           &pgstore.Store{DB: sqlDB, SchemaProvider: activeResources},
			vault:           &pgstore.VaultStore{DB: sqlDB},
			authMethodRepo:  &pgstore.AuthMethodRepo{DB: sqlDB},
			activeResources: activeResources,
		}, nil
	case SQLite:
		sqlDB, err := sqlite.Open(db.Path)
		if err != nil {
			return persistence{}, fmt.Errorf("open database: %w", err)
		}
		return persistence{
			db:              sqlDB,
			store:           &sqlite.Store{DB: sqlDB, SchemaProvider: activeResources},
			vault:           &sqlite.VaultStore{DB: sqlDB},
			authMethodRepo:  &sqlite.AuthMethodRepo{DB: sqlDB},
			activeResources: activeResources,
		}, nil
	default:
		return persistence{}, fmt.Errorf("unsupported database config %T", database)
	}
}
