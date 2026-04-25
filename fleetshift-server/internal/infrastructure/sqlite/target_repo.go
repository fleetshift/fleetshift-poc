package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetRepo implements [domain.TargetRepository] backed by SQLite.
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
		`INSERT INTO targets (id, type, name, state, labels, properties, inventory_item_id, accepted_resource_types) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		string(t.ID), string(t.Type), t.Name, string(state), string(labels), string(props), string(t.InventoryItemID), art,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("target %q: %w", t.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert target: %w", err)
	}
	return nil
}

func (r *TargetRepo) CreateOrUpdate(ctx context.Context, t domain.TargetInfo) error {
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
		`INSERT INTO targets (id, type, name, state, labels, properties, inventory_item_id, accepted_resource_types)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(id) DO UPDATE SET
		   type = excluded.type,
		   name = excluded.name,
		   state = excluded.state,
		   labels = excluded.labels,
		   properties = excluded.properties,
		   inventory_item_id = excluded.inventory_item_id,
		   accepted_resource_types = excluded.accepted_resource_types`,
		string(t.ID), string(t.Type), t.Name, string(state), string(labels), string(props), string(t.InventoryItemID), art,
	)
	if err != nil {
		return fmt.Errorf("upsert target: %w", err)
	}
	return nil
}

func (r *TargetRepo) Get(ctx context.Context, id domain.TargetID) (domain.TargetInfo, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, type, name, state, labels, properties, inventory_item_id, accepted_resource_types FROM targets WHERE id = ?`,
		string(id),
	)
	return scanTarget(row)
}

func (r *TargetRepo) List(ctx context.Context) ([]domain.TargetInfo, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id, type, name, state, labels, properties, inventory_item_id, accepted_resource_types FROM targets`)
	if err != nil {
		return nil, fmt.Errorf("list targets: %w", err)
	}
	defer rows.Close()

	var targets []domain.TargetInfo
	for rows.Next() {
		t, err := scanTarget(rows)
		if err != nil {
			return nil, err
		}
		targets = append(targets, t)
	}
	return targets, rows.Err()
}

func (r *TargetRepo) Delete(ctx context.Context, id domain.TargetID) error {
	res, err := r.DB.ExecContext(ctx, `DELETE FROM targets WHERE id = ?`, string(id))
	if err != nil {
		return fmt.Errorf("delete target: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("target %q: %w", id, domain.ErrNotFound)
	}
	return nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanTarget(s scanner) (domain.TargetInfo, error) {
	var t domain.TargetInfo
	var id, targetType, stateStr, labelsJSON, propsJSON, inventoryItemID, artJSON string
	if err := s.Scan(&id, &targetType, &t.Name, &stateStr, &labelsJSON, &propsJSON, &inventoryItemID, &artJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t, fmt.Errorf("%w", domain.ErrNotFound)
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
	raw := make([]string, len(rts))
	for i, rt := range rts {
		raw[i] = string(rt)
	}
	b, err := json.Marshal(raw)
	return string(b), err
}

func unmarshalResourceTypes(s string) ([]domain.ResourceType, error) {
	var raw []string
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	out := make([]domain.ResourceType, len(raw))
	for i, v := range raw {
		out[i] = domain.ResourceType(v)
	}
	return out, nil
}
