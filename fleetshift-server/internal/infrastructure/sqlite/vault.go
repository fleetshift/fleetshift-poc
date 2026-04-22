package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var _ domain.Vault = (*VaultStore)(nil)

// VaultStore implements [domain.Vault] backed by SQLite. It operates
// on *sql.DB directly (not within the transactional [Store]) because
// vault operations are independent of domain entity transactions.
type VaultStore struct {
	DB *sql.DB
}

func (v *VaultStore) Put(ctx context.Context, ref domain.SecretRef, value []byte) error {
	_, err := v.DB.ExecContext(ctx,
		`INSERT INTO vault_secrets (ref, val) VALUES (?, ?) ON CONFLICT(ref) DO UPDATE SET val = excluded.val`,
		string(ref), value,
	)
	if err != nil {
		return fmt.Errorf("vault put %q: %w", ref, err)
	}
	return nil
}

func (v *VaultStore) Get(ctx context.Context, ref domain.SecretRef) ([]byte, error) {
	var val []byte
	err := v.DB.QueryRowContext(ctx,
		`SELECT val FROM vault_secrets WHERE ref = ?`,
		string(ref),
	).Scan(&val)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("vault %q: %w", ref, domain.ErrNotFound)
		}
		return nil, fmt.Errorf("vault get %q: %w", ref, err)
	}
	return val, nil
}

func (v *VaultStore) Delete(ctx context.Context, ref domain.SecretRef) error {
	res, err := v.DB.ExecContext(ctx,
		`DELETE FROM vault_secrets WHERE ref = ?`,
		string(ref),
	)
	if err != nil {
		return fmt.Errorf("vault delete %q: %w", ref, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("vault %q: %w", ref, domain.ErrNotFound)
	}
	return nil
}
