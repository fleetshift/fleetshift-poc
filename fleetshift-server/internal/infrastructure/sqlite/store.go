package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Store implements [domain.Store] backed by SQLite.
type Store struct {
	DB *sql.DB
}

func (s *Store) Begin(ctx context.Context) (domain.Tx, error) {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	return &storeTx{tx: tx}, nil
}

func (s *Store) BeginReadOnly(ctx context.Context) (domain.Tx, error) {
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("begin read-only tx: %w", err)
	}
	return &storeTx{tx: tx}, nil
}

type storeTx struct {
	tx   *sql.Tx
	done bool
}

func (t *storeTx) Targets() domain.TargetRepository        { return &TargetRepo{DB: t.tx} }
func (t *storeTx) Deployments() domain.DeploymentRepository { return &DeploymentRepo{DB: t.tx} }
func (t *storeTx) Deliveries() domain.DeliveryRepository    { return &DeliveryRepo{DB: t.tx} }
func (t *storeTx) Inventory() domain.InventoryRepository    { return &InventoryRepo{DB: t.tx} }
func (t *storeTx) SignerEnrollments() domain.SignerEnrollmentRepository {
	return &SignerEnrollmentRepo{DB: t.tx}
}

func (t *storeTx) Commit() error {
	if t.done {
		return nil
	}
	t.done = true
	return t.tx.Commit()
}

func (t *storeTx) Rollback() error {
	if t.done {
		return nil
	}
	t.done = true
	return t.tx.Rollback()
}
