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
	s := d.Snapshot()
	manifests, err := json.Marshal(s.Manifests)
	if err != nil {
		return fmt.Errorf("marshal manifests: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO delivery_records (id, fulfillment_id, target_id, manifests, generation, state, operation, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (fulfillment_id, target_id) DO UPDATE SET
		   id = excluded.id,
		   manifests = excluded.manifests,
		   generation = excluded.generation,
		   state = excluded.state,
		   operation = excluded.operation,
		   updated_at = excluded.updated_at`,
		string(s.ID), string(s.FulfillmentID), string(s.TargetID),
		string(manifests), int64(s.Generation), s.State, string(s.Operation),
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("upsert delivery: %w", err)
	}
	return nil
}

func (r *DeliveryRepo) Get(ctx context.Context, id domain.DeliveryID) (domain.Delivery, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, fulfillment_id, target_id, manifests, generation, state, operation, created_at, updated_at
		 FROM delivery_records WHERE id = $1`,
		string(id),
	)
	return scanDelivery(row)
}

func (r *DeliveryRepo) GetByFulfillmentTarget(ctx context.Context, fID domain.FulfillmentID, tgtID domain.TargetID) (domain.Delivery, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, fulfillment_id, target_id, manifests, generation, state, operation, created_at, updated_at
		 FROM delivery_records WHERE fulfillment_id = $1 AND target_id = $2`,
		string(fID), string(tgtID),
	)
	return scanDelivery(row)
}

func (r *DeliveryRepo) ListByFulfillment(ctx context.Context, fID domain.FulfillmentID) ([]domain.Delivery, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, fulfillment_id, target_id, manifests, generation, state, operation, created_at, updated_at
		 FROM delivery_records WHERE fulfillment_id = $1`,
		string(fID),
	)
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	return collectRows(rows, scanDelivery)
}

func (r *DeliveryRepo) ListActive(ctx context.Context, targetIDs []domain.TargetID) ([]domain.Delivery, error) {
	args := []any{
		string(domain.DeliveryStatePending),
		string(domain.DeliveryStateAccepted),
		string(domain.DeliveryStateProgressing),
	}

	q := `SELECT id, fulfillment_id, target_id, manifests, generation, state, operation, created_at, updated_at
	      FROM delivery_records WHERE state IN ($1, $2, $3)`
	if len(targetIDs) > 0 {
		q += ` AND target_id IN (`
		for i, tid := range targetIDs {
			if i > 0 {
				q += `, `
			}
			q += fmt.Sprintf(`$%d`, len(args)+1)
			args = append(args, string(tid))
		}
		q += `)`
	}

	rows, err := r.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list active deliveries: %w", err)
	}
	return collectRows(rows, scanDelivery)
}

func (r *DeliveryRepo) DeleteByFulfillment(ctx context.Context, fID domain.FulfillmentID) error {
	_, err := r.DB.ExecContext(ctx,
		`DELETE FROM delivery_records WHERE fulfillment_id = $1`,
		string(fID),
	)
	if err != nil {
		return fmt.Errorf("delete deliveries: %w", err)
	}
	return nil
}

func scanDelivery(s scanner) (domain.Delivery, error) {
	snap, err := scanDeliverySnapshot(s)
	if err != nil {
		return domain.Delivery{}, err
	}
	return domain.DeliveryFromSnapshot(snap), nil
}

func scanDeliverySnapshot(s scanner) (domain.DeliverySnapshot, error) {
	var snap domain.DeliverySnapshot
	var id, fID, tgtID, manifestsJSON, stateStr, operationStr, createdAtStr, updatedAtStr string
	var generation int64
	if err := s.Scan(&id, &fID, &tgtID, &manifestsJSON, &generation, &stateStr, &operationStr, &createdAtStr, &updatedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan delivery: %w", err)
	}
	snap.ID = domain.DeliveryID(id)
	snap.FulfillmentID = domain.FulfillmentID(fID)
	snap.TargetID = domain.TargetID(tgtID)
	snap.Generation = domain.Generation(generation)
	snap.State = domain.DeliveryState(stateStr)
	snap.Operation = domain.DeliveryOperation(operationStr)
	if err := json.Unmarshal([]byte(manifestsJSON), &snap.Manifests); err != nil {
		return snap, fmt.Errorf("unmarshal manifests: %w", err)
	}
	t, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return snap, fmt.Errorf("parse created_at: %w", err)
	}
	snap.CreatedAt = t
	t, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return snap, fmt.Errorf("parse updated_at: %w", err)
	}
	snap.UpdatedAt = t
	return snap, nil
}
