package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeliveryRepo implements [domain.DeliveryRepository] backed by Postgres.
type DeliveryRepo struct {
	DB *sql.Tx
}

func (r *DeliveryRepo) Put(ctx context.Context, d domain.Delivery) error {
	manifests, err := json.Marshal(d.Manifests)
	if err != nil {
		return fmt.Errorf("marshal manifests: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO delivery_records (id, deployment_id, target_id, manifests, state, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 ON CONFLICT (deployment_id, target_id) DO UPDATE SET
		   id = excluded.id,
		   manifests = excluded.manifests,
		   state = excluded.state,
		   updated_at = excluded.updated_at`,
		d.ID, d.DeploymentID, d.TargetID,
		string(manifests), d.State,
		d.CreatedAt.UTC().Format(time.RFC3339),
		d.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("upsert delivery: %w", err)
	}
	return nil
}

func (r *DeliveryRepo) Get(ctx context.Context, id domain.DeliveryID) (domain.Delivery, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, deployment_id, target_id, manifests, state, created_at, updated_at
		 FROM delivery_records WHERE id = $1`,
		id,
	)
	return scanDelivery(row)
}

func (r *DeliveryRepo) GetByDeploymentTarget(ctx context.Context, depID domain.DeploymentID, tgtID domain.TargetID) (domain.Delivery, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, deployment_id, target_id, manifests, state, created_at, updated_at
		 FROM delivery_records WHERE deployment_id = $1 AND target_id = $2`,
		depID, tgtID,
	)
	return scanDelivery(row)
}

func (r *DeliveryRepo) ListByDeployment(ctx context.Context, depID domain.DeploymentID) ([]domain.Delivery, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, deployment_id, target_id, manifests, state, created_at, updated_at
		 FROM delivery_records WHERE deployment_id = $1`,
		depID,
	)
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	return collectRows(rows, scanDelivery)
}

func (r *DeliveryRepo) DeleteByDeployment(ctx context.Context, depID domain.DeploymentID) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM delivery_records WHERE deployment_id = $1`,
		depID,
	)
	if err != nil {
		return fmt.Errorf("delete deliveries: %w", err)
	}
	return nil
}

func scanDelivery(s scanner) (domain.Delivery, error) {
	var d domain.Delivery
	var id, depID, tgtID, manifestsJSON, stateStr, createdAtStr, updatedAtStr string
	if err := s.Scan(&id, &depID, &tgtID, &manifestsJSON, &stateStr, &createdAtStr, &updatedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return d, domain.ErrNotFound
		}
		return d, fmt.Errorf("scan delivery: %w", err)
	}
	d.ID = domain.DeliveryID(id)
	d.DeploymentID = domain.DeploymentID(depID)
	d.TargetID = domain.TargetID(tgtID)
	d.State = domain.DeliveryState(stateStr)
	if err := json.Unmarshal([]byte(manifestsJSON), &d.Manifests); err != nil {
		return d, fmt.Errorf("unmarshal manifests: %w", err)
	}
	t, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return d, fmt.Errorf("parse created_at: %w", err)
	}
	d.CreatedAt = t
	t, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return d, fmt.Errorf("parse updated_at: %w", err)
	}
	d.UpdatedAt = t
	return d, nil
}
