package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetRepo implements [domain.TargetRepository] backed by Postgres.
type TargetRepo struct {
	DB *sql.Tx
}

func (r *TargetRepo) Create(ctx context.Context, t domain.TargetInfo) error {
	labels, err := json.Marshal(t.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}
	props, err := json.Marshal(t.Properties)
	if err != nil {
		return fmt.Errorf("marshal properties: %w", err)
	}
	art, err := marshalResourceTypes(t.AcceptedResourceTypes)
	if err != nil {
		return fmt.Errorf("marshal accepted_resource_types: %w", err)
	}

	state := t.State
	if state == "" {
		state = domain.TargetStateReady
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO targets (id, type, name, state, labels, properties, inventory_item_id, accepted_resource_types) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		t.ID, t.Type, t.Name, state, string(labels), string(props), t.InventoryItemID, art,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("target %q: %w", t.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert target: %w", err)
	}
	return nil
}

func (r *TargetRepo) Get(ctx context.Context, id domain.TargetID) (domain.TargetInfo, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, type, name, state, labels, properties, inventory_item_id, accepted_resource_types FROM targets WHERE id = $1`,
		id,
	)
	return scanTarget(row)
}

func (r *TargetRepo) List(ctx context.Context) ([]domain.TargetInfo, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id, type, name, state, labels, properties, inventory_item_id, accepted_resource_types FROM targets`)
	if err != nil {
		return nil, fmt.Errorf("list targets: %w", err)
	}
	return collectRows(rows, scanTarget)
}

func (r *TargetRepo) Delete(ctx context.Context, id domain.TargetID) error {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM targets WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete target: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("target %q: %w", id, domain.ErrNotFound)
	}
	return nil
}

func scanTarget(s scanner) (domain.TargetInfo, error) {
	var t domain.TargetInfo
	var id, targetType, stateStr, labelsJSON, propsJSON, inventoryItemID, artJSON string
	if err := s.Scan(&id, &targetType, &t.Name, &stateStr, &labelsJSON, &propsJSON, &inventoryItemID, &artJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t, domain.ErrNotFound
		}
		return t, fmt.Errorf("scan target: %w", err)
	}
	t.ID = domain.TargetID(id)
	t.Type = domain.TargetType(targetType)
	t.State = domain.TargetState(stateStr)
	t.InventoryItemID = domain.InventoryItemID(inventoryItemID)
	if err := json.Unmarshal([]byte(labelsJSON), &t.Labels); err != nil {
		return t, fmt.Errorf("unmarshal labels: %w", err)
	}
	if err := json.Unmarshal([]byte(propsJSON), &t.Properties); err != nil {
		return t, fmt.Errorf("unmarshal properties: %w", err)
	}
	art, err := unmarshalResourceTypes(artJSON)
	if err != nil {
		return t, fmt.Errorf("unmarshal accepted_resource_types: %w", err)
	}
	t.AcceptedResourceTypes = art
	return t, nil
}

func marshalResourceTypes(rts []domain.ResourceType) (string, error) {
	if len(rts) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(rts)
	return string(b), err
}

func unmarshalResourceTypes(s string) ([]domain.ResourceType, error) {
	var rts []domain.ResourceType
	if err := json.Unmarshal([]byte(s), &rts); err != nil {
		return nil, err
	}
	return rts, nil
}
