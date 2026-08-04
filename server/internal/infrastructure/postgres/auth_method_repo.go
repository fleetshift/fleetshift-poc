package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// AuthMethodRepo implements [domain.AuthMethodRepository] backed by Postgres.
// Unlike other repos in this package, it operates on [*sql.DB] directly
// because auth method operations do not participate in cross-repo
// transactions.
type AuthMethodRepo struct {
	DB *sql.DB
}

func (r *AuthMethodRepo) Save(ctx context.Context, method domain.AuthMethod) error {
	s := method.Snapshot()
	configJSON, err := marshalAuthMethodConfig(s)
	if err != nil {
		return err
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO auth_methods (id, type, config_json) VALUES ($1, $2, $3)
		 ON CONFLICT(id) DO UPDATE SET type = excluded.type, config_json = excluded.config_json`,
		s.ID, s.Type, string(configJSON),
	)
	if err != nil {
		return fmt.Errorf("save auth method: %w", err)
	}
	return nil
}

func (r *AuthMethodRepo) Get(ctx context.Context, id domain.AuthMethodID) (domain.AuthMethod, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, type, config_json FROM auth_methods WHERE id = $1`,
		id,
	)
	return scanAuthMethod(row)
}

func (r *AuthMethodRepo) List(ctx context.Context) ([]domain.AuthMethod, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id, type, config_json FROM auth_methods`)
	if err != nil {
		return nil, fmt.Errorf("list auth methods: %w", err)
	}
	return collectRows(rows, scanAuthMethod)
}

func marshalAuthMethodConfig(s domain.AuthMethodSnapshot) ([]byte, error) {
	switch s.Type {
	case domain.AuthMethodTypeOIDC:
		return json.Marshal(s.OIDC)
	default:
		return nil, fmt.Errorf("unknown auth method type: %s", s.Type)
	}
}

func scanAuthMethod(s scanner) (domain.AuthMethod, error) {
	snap, err := scanAuthMethodSnapshot(s)
	if err != nil {
		return domain.AuthMethod{}, err
	}
	return domain.AuthMethodFromSnapshot(snap), nil
}

func scanAuthMethodSnapshot(s scanner) (domain.AuthMethodSnapshot, error) {
	var snap domain.AuthMethodSnapshot
	var id, methodType, configJSON string
	if err := s.Scan(&id, &methodType, &configJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan auth method: %w", err)
	}

	snap.ID = domain.AuthMethodID(id)
	snap.Type = domain.AuthMethodType(methodType)

	switch snap.Type {
	case domain.AuthMethodTypeOIDC:
		var cfg domain.OIDCConfig
		if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
			return snap, fmt.Errorf("unmarshal OIDC config: %w", err)
		}
		snap.OIDC = &cfg
	default:
		return snap, fmt.Errorf("unknown auth method type: %s", snap.Type)
	}

	return snap, nil
}
