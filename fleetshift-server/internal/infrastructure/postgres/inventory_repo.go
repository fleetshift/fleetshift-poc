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

// InventoryRepo implements [domain.InventoryRepository] backed by Postgres.
type InventoryRepo struct {
	DB *sql.Tx
}

func (r *InventoryRepo) Create(ctx context.Context, item domain.InventoryItem) error {
	s := item.Snapshot()
	props := s.Properties
	if props == nil {
		props = json.RawMessage("{}")
	}
	labels, err := json.Marshal(s.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	var srcDeliveryID *string
	if s.SourceDeliveryID != nil {
		id := string(*s.SourceDeliveryID)
		srcDeliveryID = &id
	}

	var observedAt *string
	if s.ObservedAt != nil {
		oa := s.ObservedAt.UTC().Format(time.RFC3339)
		observedAt = &oa
	}

	observed := s.Observed
	if observed == nil {
		observed = json.RawMessage("{}")
	}

	conditions, err := marshalConditions(s.Conditions)
	if err != nil {
		return fmt.Errorf("marshal conditions: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO inventory_items (id, type, name, properties, labels, source_delivery_id, created_at, updated_at, target_id, observed_at, observed, conditions)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		s.ID, s.Type, s.Name,
		string(props), string(labels), srcDeliveryID,
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339),
		s.TargetID, observedAt, string(observed), string(conditions),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("inventory item %q: %w", s.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert inventory item: %w", err)
	}
	return nil
}

func (r *InventoryRepo) CreateOrUpdate(ctx context.Context, item domain.InventoryItem) error {
	s := item.Snapshot()
	props := s.Properties
	if props == nil {
		props = json.RawMessage("{}")
	}
	labels, err := json.Marshal(s.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	var srcDeliveryID *string
	if s.SourceDeliveryID != nil {
		id := string(*s.SourceDeliveryID)
		srcDeliveryID = &id
	}

	var observedAt *string
	if s.ObservedAt != nil {
		oa := s.ObservedAt.UTC().Format(time.RFC3339)
		observedAt = &oa
	}

	observed := s.Observed
	if observed == nil {
		observed = json.RawMessage("{}")
	}

	conditions, err := marshalConditions(s.Conditions)
	if err != nil {
		return fmt.Errorf("marshal conditions: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO inventory_items (id, type, name, properties, labels, source_delivery_id, created_at, updated_at, target_id, observed_at, observed, conditions)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
		 ON CONFLICT(id) DO UPDATE SET
		   type = EXCLUDED.type,
		   name = EXCLUDED.name,
		   properties = EXCLUDED.properties,
		   labels = EXCLUDED.labels,
		   source_delivery_id = EXCLUDED.source_delivery_id,
		   updated_at = EXCLUDED.updated_at,
		   target_id = EXCLUDED.target_id,
		   observed_at = EXCLUDED.observed_at,
		   observed = EXCLUDED.observed,
		   conditions = EXCLUDED.conditions`,
		s.ID, s.Type, s.Name,
		string(props), string(labels), srcDeliveryID,
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339),
		s.TargetID, observedAt, string(observed), string(conditions),
	)
	if err != nil {
		return fmt.Errorf("upsert inventory item: %w", err)
	}
	return nil
}

func (r *InventoryRepo) Get(ctx context.Context, id domain.InventoryItemID) (domain.InventoryItem, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, type, name, properties, labels, source_delivery_id, created_at, updated_at, target_id, observed_at, observed, conditions
		 FROM inventory_items WHERE id = $1`,
		id,
	)
	return scanInventoryItem(row)
}

func (r *InventoryRepo) List(ctx context.Context) ([]domain.InventoryItem, error) {
	return r.queryItems(ctx,
		`SELECT id, type, name, properties, labels, source_delivery_id, created_at, updated_at, target_id, observed_at, observed, conditions
		 FROM inventory_items`)
}

func (r *InventoryRepo) ListByType(ctx context.Context, t domain.InventoryType) ([]domain.InventoryItem, error) {
	return r.queryItems(ctx,
		`SELECT id, type, name, properties, labels, source_delivery_id, created_at, updated_at, target_id, observed_at, observed, conditions
		 FROM inventory_items WHERE type = $1`,
		t)
}

func (r *InventoryRepo) Update(ctx context.Context, item domain.InventoryItem) error {
	s := item.Snapshot()
	props := s.Properties
	if props == nil {
		props = json.RawMessage("{}")
	}
	labels, err := json.Marshal(s.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	var srcDeliveryID *string
	if s.SourceDeliveryID != nil {
		id := string(*s.SourceDeliveryID)
		srcDeliveryID = &id
	}

	var observedAt *string
	if s.ObservedAt != nil {
		oa := s.ObservedAt.UTC().Format(time.RFC3339)
		observedAt = &oa
	}

	observed := s.Observed
	if observed == nil {
		observed = json.RawMessage("{}")
	}

	conditions, err := marshalConditions(s.Conditions)
	if err != nil {
		return fmt.Errorf("marshal conditions: %w", err)
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE inventory_items
		 SET type = $1, name = $2, properties = $3, labels = $4, source_delivery_id = $5, updated_at = $6, target_id = $7, observed_at = $8, observed = $9, conditions = $10
		 WHERE id = $11`,
		s.Type, s.Name, string(props), string(labels), srcDeliveryID,
		s.UpdatedAt.UTC().Format(time.RFC3339), s.TargetID, observedAt, string(observed), string(conditions),
		s.ID,
	)
	if err != nil {
		return fmt.Errorf("update inventory item: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("inventory item %q: %w", s.ID, domain.ErrNotFound)
	}
	return nil
}

func (r *InventoryRepo) Delete(ctx context.Context, id domain.InventoryItemID) error {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM inventory_items WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete inventory item: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("inventory item %q: %w", id, domain.ErrNotFound)
	}
	return nil
}

func (r *InventoryRepo) queryItems(ctx context.Context, query string, args ...any) ([]domain.InventoryItem, error) {
	rows, err := r.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query inventory items: %w", err)
	}
	return collectRows(rows, scanInventoryItem)
}

func scanInventoryItem(s scanner) (domain.InventoryItem, error) {
	snap, err := scanInventoryItemSnapshot(s)
	if err != nil {
		return domain.InventoryItem{}, err
	}
	return domain.InventoryItemFromSnapshot(snap), nil
}

func scanInventoryItemSnapshot(s scanner) (domain.InventoryItemSnapshot, error) {
	var snap domain.InventoryItemSnapshot
	var id, itemType, name, propsJSON, labelsJSON, createdAtStr, updatedAtStr string
	var targetID, observedJSON, conditionsJSON string
	var srcDeliveryID, observedAtStr sql.NullString

	if err := s.Scan(&id, &itemType, &name, &propsJSON, &labelsJSON, &srcDeliveryID, &createdAtStr, &updatedAtStr, &targetID, &observedAtStr, &observedJSON, &conditionsJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan inventory item: %w", err)
	}
	snap.ID = domain.InventoryItemID(id)
	snap.Type = domain.InventoryType(itemType)
	snap.Name = name
	snap.Properties = json.RawMessage(propsJSON)
	if err := json.Unmarshal([]byte(labelsJSON), &snap.Labels); err != nil {
		return snap, fmt.Errorf("unmarshal labels: %w", err)
	}
	if srcDeliveryID.Valid {
		did := domain.DeliveryID(srcDeliveryID.String)
		snap.SourceDeliveryID = &did
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

	if targetID != "" {
		snap.TargetID = domain.TargetID(targetID)
	}
	if observedAtStr.Valid {
		oa, err := time.Parse(time.RFC3339, observedAtStr.String)
		if err != nil {
			return snap, fmt.Errorf("parse observed_at: %w", err)
		}
		snap.ObservedAt = &oa
	}
	if observedJSON != "" {
		snap.Observed = json.RawMessage(observedJSON)
	}
	if conditionsJSON != "" && conditionsJSON != "[]" {
		if err := json.Unmarshal([]byte(conditionsJSON), &snap.Conditions); err != nil {
			return snap, fmt.Errorf("unmarshal conditions: %w", err)
		}
	}

	return snap, nil
}

func marshalConditions(conditions []domain.InventoryCondition) (json.RawMessage, error) {
	if conditions == nil {
		return json.RawMessage("[]"), nil
	}
	data, err := json.Marshal(conditions)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

func (r *InventoryRepo) DeleteByTarget(ctx context.Context, targetID domain.TargetID) error {
	_, err := r.DB.ExecContext(ctx, `DELETE FROM inventory_items WHERE target_id = $1`, targetID)
	if err != nil {
		return fmt.Errorf("delete items by target: %w", err)
	}
	return nil
}

func (r *InventoryRepo) ReplaceByTargetAndType(ctx context.Context, targetID domain.TargetID, t domain.InventoryType, items []domain.InventoryItem) error {
	_, err := r.DB.ExecContext(ctx, `DELETE FROM inventory_items WHERE target_id = $1 AND type = $2`, targetID, t)
	if err != nil {
		return fmt.Errorf("delete for replace: %w", err)
	}
	for _, item := range items {
		if err := r.Create(ctx, item); err != nil {
			return fmt.Errorf("insert replacement: %w", err)
		}
	}
	return nil
}
