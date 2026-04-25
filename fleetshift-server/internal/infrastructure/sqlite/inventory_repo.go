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
	props, err := marshalOrDefault(item.Properties)
	if err != nil {
		return fmt.Errorf("marshal properties: %w", err)
	}
	labels, err := json.Marshal(item.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	var srcDeliveryID *string
	if item.SourceDeliveryID != nil {
		s := string(*item.SourceDeliveryID)
		srcDeliveryID = &s
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO inventory_items (id, type, name, properties, labels, source_delivery_id, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		string(item.ID), string(item.Type), item.Name,
		string(props), string(labels), srcDeliveryID,
		item.CreatedAt.UTC().Format(time.RFC3339),
		item.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("inventory item %q: %w", item.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert inventory item: %w", err)
	}
	return nil
}

func (r *InventoryRepo) CreateOrUpdate(ctx context.Context, item domain.InventoryItem) error {
	props, err := marshalOrDefault(item.Properties)
	if err != nil {
		return fmt.Errorf("marshal properties: %w", err)
	}
	labels, err := json.Marshal(item.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	var srcDeliveryID *string
	if item.SourceDeliveryID != nil {
		s := string(*item.SourceDeliveryID)
		srcDeliveryID = &s
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
		string(item.ID), string(item.Type), item.Name,
		string(props), string(labels), srcDeliveryID,
		item.CreatedAt.UTC().Format(time.RFC3339),
		item.UpdatedAt.UTC().Format(time.RFC3339),
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
	return scanInventoryItem(row)
}

func (r *InventoryRepo) List(ctx context.Context) ([]domain.InventoryItem, error) {
	return r.queryItems(ctx,
		`SELECT id, type, name, properties, labels, source_delivery_id, created_at, updated_at
		 FROM inventory_items`)
}

func (r *InventoryRepo) ListByType(ctx context.Context, t domain.InventoryType) ([]domain.InventoryItem, error) {
	return r.queryItems(ctx,
		`SELECT id, type, name, properties, labels, source_delivery_id, created_at, updated_at
		 FROM inventory_items WHERE type = ?`,
		string(t))
}

func (r *InventoryRepo) Update(ctx context.Context, item domain.InventoryItem) error {
	props, err := marshalOrDefault(item.Properties)
	if err != nil {
		return fmt.Errorf("marshal properties: %w", err)
	}
	labels, err := json.Marshal(item.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	var srcDeliveryID *string
	if item.SourceDeliveryID != nil {
		s := string(*item.SourceDeliveryID)
		srcDeliveryID = &s
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE inventory_items
		 SET type = ?, name = ?, properties = ?, labels = ?, source_delivery_id = ?, updated_at = ?
		 WHERE id = ?`,
		string(item.Type), item.Name, string(props), string(labels), srcDeliveryID,
		item.UpdatedAt.UTC().Format(time.RFC3339), string(item.ID),
	)
	if err != nil {
		return fmt.Errorf("update inventory item: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("inventory item %q: %w", item.ID, domain.ErrNotFound)
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
		item, err := scanInventoryItem(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func scanInventoryItem(s scanner) (domain.InventoryItem, error) {
	var item domain.InventoryItem
	var id, itemType, name, propsJSON, labelsJSON, createdAtStr, updatedAtStr string
	var srcDeliveryID sql.NullString

	if err := s.Scan(&id, &itemType, &name, &propsJSON, &labelsJSON, &srcDeliveryID, &createdAtStr, &updatedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return item, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return item, fmt.Errorf("scan inventory item: %w", err)
	}
	item.ID = domain.InventoryItemID(id)
	item.Type = domain.InventoryType(itemType)
	item.Name = name
	item.Properties = json.RawMessage(propsJSON)
	if err := json.Unmarshal([]byte(labelsJSON), &item.Labels); err != nil {
		return item, fmt.Errorf("unmarshal labels: %w", err)
	}
	if srcDeliveryID.Valid {
		did := domain.DeliveryID(srcDeliveryID.String)
		item.SourceDeliveryID = &did
	}
	t, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return item, fmt.Errorf("parse created_at: %w", err)
	}
	item.CreatedAt = t
	t, err = time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return item, fmt.Errorf("parse updated_at: %w", err)
	}
	item.UpdatedAt = t
	return item, nil
}

func marshalOrDefault(raw json.RawMessage) (json.RawMessage, error) {
	if raw == nil {
		return json.RawMessage("{}"), nil
	}
	return raw, nil
}
