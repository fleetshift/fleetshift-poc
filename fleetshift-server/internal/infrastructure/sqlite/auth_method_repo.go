package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// AuthMethodRepo implements [domain.AuthMethodRepository] backed by SQLite.
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
		`INSERT INTO auth_methods (id, type, config_json) VALUES (?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET type = excluded.type, config_json = excluded.config_json`,
		string(s.ID), string(s.Type), string(configJSON),
	)
	if err != nil {
		return fmt.Errorf("save auth method: %w", err)
	}
	return nil
}

func (r *AuthMethodRepo) Get(ctx context.Context, id domain.AuthMethodID) (domain.AuthMethod, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, type, config_json FROM auth_methods WHERE id = ?`,
		string(id),
	)
	s, err := scanAuthMethodSnapshot(row)
	if err != nil {
		return domain.AuthMethod{}, err
	}
	return domain.AuthMethodFromSnapshot(s), nil
}

func (r *AuthMethodRepo) List(ctx context.Context) ([]domain.AuthMethod, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id, type, config_json FROM auth_methods`)
	if err != nil {
		return nil, fmt.Errorf("list auth methods: %w", err)
	}
	defer rows.Close()

	var methods []domain.AuthMethod
	for rows.Next() {
		s, err := scanAuthMethodSnapshot(rows)
		if err != nil {
			return nil, err
		}
		methods = append(methods, domain.AuthMethodFromSnapshot(s))
	}
	return methods, rows.Err()
}

func marshalAuthMethodConfig(s domain.AuthMethodSnapshot) ([]byte, error) {
	switch s.Type {
	case domain.AuthMethodTypeOIDC:
		return json.Marshal(s.OIDC)
	default:
		return nil, fmt.Errorf("unknown auth method type: %s", s.Type)
	}
}

func scanAuthMethodSnapshot(s scanner) (domain.AuthMethodSnapshot, error) {
	var id, methodType, configJSON string
	if err := s.Scan(&id, &methodType, &configJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.AuthMethodSnapshot{}, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return domain.AuthMethodSnapshot{}, fmt.Errorf("scan auth method: %w", err)
	}

	snap := domain.AuthMethodSnapshot{
		ID:   domain.AuthMethodID(id),
		Type: domain.AuthMethodType(methodType),
	}

	switch snap.Type {
	case domain.AuthMethodTypeOIDC:
		var cfg domain.OIDCConfig
		if err := json.Unmarshal([]byte(configJSON), &cfg); err != nil {
			return domain.AuthMethodSnapshot{}, fmt.Errorf("unmarshal OIDC config: %w", err)
		}
		snap.OIDC = &cfg
	default:
		return domain.AuthMethodSnapshot{}, fmt.Errorf("unknown auth method type: %s", snap.Type)
	}

	return snap, nil
}
