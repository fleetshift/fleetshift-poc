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
	s := t.Snapshot()
	labels, err := json.Marshal(s.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}
	props, err := json.Marshal(s.Properties)
	if err != nil {
		return fmt.Errorf("marshal properties: %w", err)
	}
	art, err := marshalManifestTypes(s.AcceptedManifestTypes)
	if err != nil {
		return fmt.Errorf("marshal accepted_manifest_types: %w", err)
	}

	state := s.State
	if state == "" {
		state = domain.TargetStateReady
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO targets (id, type, name, state, labels, properties, inventory_item_id, accepted_manifest_types) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		s.ID, s.Type, s.Name, state, string(labels), string(props), s.InventoryItemID, art,
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("target %q: %w", s.ID, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert target: %w", err)
	}
	return nil
}

func (r *TargetRepo) CreateOrUpdate(ctx context.Context, t domain.TargetInfo) error {
	s := t.Snapshot()
	labels, err := json.Marshal(s.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}
	props, err := json.Marshal(s.Properties)
	if err != nil {
		return fmt.Errorf("marshal properties: %w", err)
	}
	art, err := marshalManifestTypes(s.AcceptedManifestTypes)
	if err != nil {
		return fmt.Errorf("marshal accepted_manifest_types: %w", err)
	}

	state := s.State
	if state == "" {
		state = domain.TargetStateReady
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO targets (id, type, name, state, labels, properties, inventory_item_id, accepted_manifest_types)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT(id) DO UPDATE SET
		   type = EXCLUDED.type,
		   name = EXCLUDED.name,
		   state = EXCLUDED.state,
		   labels = EXCLUDED.labels,
		   properties = EXCLUDED.properties,
		   inventory_item_id = EXCLUDED.inventory_item_id,
		   accepted_manifest_types = EXCLUDED.accepted_manifest_types`,
		s.ID, s.Type, s.Name, state, string(labels), string(props), s.InventoryItemID, art,
	)
	if err != nil {
		return fmt.Errorf("upsert target: %w", err)
	}
	return nil
}

func (r *TargetRepo) Get(ctx context.Context, id domain.TargetID) (domain.TargetInfo, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT id, type, name, state, labels, properties, inventory_item_id, accepted_manifest_types FROM targets WHERE id = $1`,
		id,
	)
	return scanTarget(row)
}

func (r *TargetRepo) List(ctx context.Context) ([]domain.TargetInfo, error) {
	rows, err := r.DB.QueryContext(ctx, `SELECT id, type, name, state, labels, properties, inventory_item_id, accepted_manifest_types FROM targets`)
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
	snap, err := scanTargetSnapshot(s)
	if err != nil {
		return domain.TargetInfo{}, err
	}
	return domain.TargetInfoFromSnapshot(snap), nil
}

func scanTargetSnapshot(s scanner) (domain.TargetInfoSnapshot, error) {
	var snap domain.TargetInfoSnapshot
	var id, targetType, stateStr, labelsJSON, propsJSON, inventoryItemID, artJSON string
	if err := s.Scan(&id, &targetType, &snap.Name, &stateStr, &labelsJSON, &propsJSON, &inventoryItemID, &artJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return snap, domain.ErrNotFound
		}
		return snap, fmt.Errorf("scan target: %w", err)
	}
	snap.ID = domain.TargetID(id)
	snap.Type = domain.TargetType(targetType)
	snap.State = domain.TargetState(stateStr)
	snap.InventoryItemID = domain.InventoryItemID(inventoryItemID)
	if err := json.Unmarshal([]byte(labelsJSON), &snap.Labels); err != nil {
		return snap, fmt.Errorf("unmarshal labels: %w", err)
	}
	if err := json.Unmarshal([]byte(propsJSON), &snap.Properties); err != nil {
		return snap, fmt.Errorf("unmarshal properties: %w", err)
	}
	art, err := unmarshalManifestTypes(artJSON)
	if err != nil {
		return snap, fmt.Errorf("unmarshal accepted_manifest_types: %w", err)
	}
	snap.AcceptedManifestTypes = art
	return snap, nil
}

func marshalManifestTypes(mts []domain.ManifestType) (string, error) {
	if len(mts) == 0 {
		return "[]", nil
	}
	b, err := json.Marshal(mts)
	return string(b), err
}

func unmarshalManifestTypes(s string) ([]domain.ManifestType, error) {
	var mts []domain.ManifestType
	if err := json.Unmarshal([]byte(s), &mts); err != nil {
		return nil, err
	}
	return mts, nil
}
