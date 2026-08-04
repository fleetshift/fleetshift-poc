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

// InventoryRepo implements [domain.InventoryRepository] backed by SQLite.
type InventoryRepo struct {
	DB *sql.Tx
}

func (r *InventoryRepo) Create(ctx context.Context, item domain.InventoryItem) error {
	s := item.Snapshot()
	props, err := marshalOrDefault(s.Properties)
	if err != nil {
		return fmt.Errorf("marshal properties: %w", err)
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

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO inventory_items (id, type, name, properties, labels, source_delivery_id, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		string(s.ID), string(s.Type), s.Name,
		string(props), string(labels), srcDeliveryID,
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339),
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
	props, err := marshalOrDefault(s.Properties)
	if err != nil {
		return fmt.Errorf("marshal properties: %w", err)
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

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO inventory_items (id, type, name, properties, labels, source_delivery_id, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   type = excluded.type,
		   name = excluded.name,
		   properties = excluded.properties,
		   labels = excluded.labels,
		   source_delivery_id = excluded.source_delivery_id,
		   updated_at = excluded.updated_at`,
		string(s.ID), string(s.Type), s.Name,
		string(props), string(labels), srcDeliveryID,
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("upsert inventory item: %w", err)
	}
	return nil
}

func (r *InventoryRepo) Get(ctx context.Context, id domain.InventoryItemID) (domain.InventoryItem, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, type, name, properties, labels, source_delivery_id, created_at, updated_at
		 FROM inventory_items WHERE id = ?`,
		string(id),
	)
	s, err := scanInventoryItemSnapshot(row)
	if err != nil {
		return domain.InventoryItem{}, err
	}
	return domain.InventoryItemFromSnapshot(s), nil
}

func (r *InventoryRepo) List(ctx context.Context) ([]domain.InventoryItem, error) {
	return r.queryItems(ctx,
		`SELECT id, type, name, properties, labels, source_delivery_id, created_at, updated_at
		 FROM inventory_items`)
}

func (r *InventoryRepo) ListByType(ctx context.Context, t domain.InventoryItemType) ([]domain.InventoryItem, error) {
	return r.queryItems(ctx,
		`SELECT id, type, name, properties, labels, source_delivery_id, created_at, updated_at
		 FROM inventory_items WHERE type = ?`,
		string(t))
}

func (r *InventoryRepo) Update(ctx context.Context, item domain.InventoryItem) error {
	s := item.Snapshot()
	props, err := marshalOrDefault(s.Properties)
	if err != nil {
		return fmt.Errorf("marshal properties: %w", err)
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

	res, err := r.DB.ExecContext(ctx,
		`UPDATE inventory_items
		 SET type = ?, name = ?, properties = ?, labels = ?, source_delivery_id = ?, updated_at = ?
		 WHERE id = ?`,
		string(s.Type), s.Name, string(props), string(labels), srcDeliveryID,
		s.UpdatedAt.UTC().Format(time.RFC3339), string(s.ID),
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
	res, err := r.DB.ExecContext(ctx, `DELETE FROM inventory_items WHERE id = ?`, string(id))
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
	defer rows.Close()

	var items []domain.InventoryItem
	for rows.Next() {
		s, err := scanInventoryItemSnapshot(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, domain.InventoryItemFromSnapshot(s))
	}
	return items, rows.Err()
}

func scanInventoryItemSnapshot(s scanner) (domain.InventoryItemSnapshot, error) {
	var snap domain.InventoryItemSnapshot
	var id, itemType, name, propsJSON, labelsJSON, createdAtStr, updatedAtStr string
	var srcDeliveryID sql.NullString

	if err := s.Scan(&id, &itemType, &name, &propsJSON, &labelsJSON, &srcDeliveryID, &createdAtStr, &updatedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return snap, fmt.Errorf("scan inventory item: %w", err)
	}
	snap.ID = domain.InventoryItemID(id)
	snap.Type = domain.InventoryItemType(itemType)
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
	return snap, nil
}

func marshalOrDefault(raw json.RawMessage) (json.RawMessage, error) {
	if raw == nil {
		return json.RawMessage("{}"), nil
	}
	return raw, nil
}
