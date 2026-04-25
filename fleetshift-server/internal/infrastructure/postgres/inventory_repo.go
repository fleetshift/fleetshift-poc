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
	props := item.Properties
	if props == nil {
		props = json.RawMessage("{}")
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
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		item.ID, item.Type, item.Name,
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
	props := item.Properties
	if props == nil {
		props = json.RawMessage("{}")
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
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT(id) DO UPDATE SET
		   type = EXCLUDED.type,
		   name = EXCLUDED.name,
		   properties = EXCLUDED.properties,
		   labels = EXCLUDED.labels,
		   source_delivery_id = EXCLUDED.source_delivery_id,
		   updated_at = EXCLUDED.updated_at`,
		item.ID, item.Type, item.Name,
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
		 FROM inventory_items WHERE id = $1`,
		id,
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
		 FROM inventory_items WHERE type = $1`,
		t)
}

func (r *InventoryRepo) Update(ctx context.Context, item domain.InventoryItem) error {
	props := item.Properties
	if props == nil {
		props = json.RawMessage("{}")
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
		 SET type = $1, name = $2, properties = $3, labels = $4, source_delivery_id = $5, updated_at = $6
		 WHERE id = $7`,
		item.Type, item.Name, string(props), string(labels), srcDeliveryID,
		item.UpdatedAt.UTC().Format(time.RFC3339), item.ID,
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
	var item domain.InventoryItem
	var id, itemType, name, propsJSON, labelsJSON, createdAtStr, updatedAtStr string
	var srcDeliveryID sql.NullString

	if err := s.Scan(&id, &itemType, &name, &propsJSON, &labelsJSON, &srcDeliveryID, &createdAtStr, &updatedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return item, domain.ErrNotFound
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
