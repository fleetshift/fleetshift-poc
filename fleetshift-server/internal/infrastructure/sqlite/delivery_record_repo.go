package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeliveryRecordRepo implements [domain.DeliveryRecordRepository] backed by SQLite.
type DeliveryRecordRepo struct {
	DB *sql.DB
}

func (r *DeliveryRecordRepo) Put(ctx context.Context, rec domain.DeliveryRecord) error {
	manifests, err := json.Marshal(rec.Manifests)
	if err != nil {
		return fmt.Errorf("marshal manifests: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO delivery_records (deployment_id, target_id, manifests, state, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (deployment_id, target_id) DO UPDATE SET
		   manifests = excluded.manifests,
		   state = excluded.state,
		   updated_at = excluded.updated_at`,
		string(rec.DeploymentID), string(rec.TargetID),
		string(manifests), string(rec.State), rec.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("upsert delivery record: %w", err)
	}
	return nil
}

func (r *DeliveryRecordRepo) Get(ctx context.Context, depID domain.DeploymentID, tgtID domain.TargetID) (domain.DeliveryRecord, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT deployment_id, target_id, manifests, state, updated_at
		 FROM delivery_records WHERE deployment_id = ? AND target_id = ?`,
		string(depID), string(tgtID),
	)
	return scanDeliveryRecord(row)
}

func (r *DeliveryRecordRepo) ListByDeployment(ctx context.Context, depID domain.DeploymentID) ([]domain.DeliveryRecord, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT deployment_id, target_id, manifests, state, updated_at
		 FROM delivery_records WHERE deployment_id = ?`,
		string(depID),
	)
	if err != nil {
		return nil, fmt.Errorf("list delivery records: %w", err)
	}
	defer rows.Close()

	var records []domain.DeliveryRecord
	for rows.Next() {
		rec, err := scanDeliveryRecord(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

func (r *DeliveryRecordRepo) DeleteByDeployment(ctx context.Context, depID domain.DeploymentID) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM delivery_records WHERE deployment_id = ?`,
		string(depID),
	)
	if err != nil {
		return fmt.Errorf("delete delivery records: %w", err)
	}
	return nil
}

func scanDeliveryRecord(s scanner) (domain.DeliveryRecord, error) {
	var rec domain.DeliveryRecord
	var depID, tgtID, manifestsJSON, stateStr, updatedAtStr string
	if err := s.Scan(&depID, &tgtID, &manifestsJSON, &stateStr, &updatedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return rec, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return rec, fmt.Errorf("scan delivery record: %w", err)
	}
	rec.DeploymentID = domain.DeploymentID(depID)
	rec.TargetID = domain.TargetID(tgtID)
	rec.State = domain.DeliveryState(stateStr)
	if err := json.Unmarshal([]byte(manifestsJSON), &rec.Manifests); err != nil {
		return rec, fmt.Errorf("unmarshal manifests: %w", err)
	}
	t, err := time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return rec, fmt.Errorf("parse updated_at: %w", err)
	}
	rec.UpdatedAt = t
	return rec, nil
}
