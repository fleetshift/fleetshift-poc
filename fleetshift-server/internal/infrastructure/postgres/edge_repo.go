package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// EdgeRepo implements [domain.EdgeRepository] backed by Postgres.
type EdgeRepo struct {
	DB *sql.Tx
}

func (r *EdgeRepo) CreateOrUpdate(ctx context.Context, targetID domain.TargetID, edges []domain.InventoryEdge) error {
	for _, e := range edges {
		_, err := r.DB.ExecContext(ctx,
			`INSERT INTO inventory_edges (target_id, source_uid, dest_uid, edge_type, source_kind, dest_kind)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (target_id, source_uid, dest_uid, edge_type) DO UPDATE SET
			   source_kind = EXCLUDED.source_kind,
			   dest_kind = EXCLUDED.dest_kind`,
			string(targetID), e.SourceUID, e.DestUID, e.EdgeType, e.SourceKind, e.DestKind)
		if err != nil {
			return fmt.Errorf("upsert edge: %w", err)
		}
	}
	return nil
}

func (r *EdgeRepo) Delete(ctx context.Context, targetID domain.TargetID, edges []domain.InventoryEdge) error {
	for _, e := range edges {
		_, err := r.DB.ExecContext(ctx,
			`DELETE FROM inventory_edges WHERE target_id = $1 AND source_uid = $2 AND dest_uid = $3 AND edge_type = $4`,
			string(targetID), e.SourceUID, e.DestUID, e.EdgeType)
		if err != nil {
			return fmt.Errorf("delete edge: %w", err)
		}
	}
	return nil
}

func (r *EdgeRepo) DeleteBySourceUIDs(ctx context.Context, targetID domain.TargetID, sourceUIDs []string) error {
	if len(sourceUIDs) == 0 {
		return nil
	}
	placeholders := make([]string, len(sourceUIDs))
	args := make([]any, 0, len(sourceUIDs)+1)
	args = append(args, string(targetID))
	for i, uid := range sourceUIDs {
		placeholders[i] = fmt.Sprintf("$%d", i+2)
		args = append(args, uid)
	}
	query := fmt.Sprintf(
		`DELETE FROM inventory_edges WHERE target_id = $1 AND source_uid IN (%s)`,
		strings.Join(placeholders, ","))
	_, err := r.DB.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("delete edges by source UIDs: %w", err)
	}
	return nil
}

func (r *EdgeRepo) DeleteByTarget(ctx context.Context, targetID domain.TargetID) error {
	_, err := r.DB.ExecContext(ctx, `DELETE FROM inventory_edges WHERE target_id = $1`, targetID)
	if err != nil {
		return fmt.Errorf("delete edges by target: %w", err)
	}
	return nil
}

func (r *EdgeRepo) ListBySourceUID(ctx context.Context, targetID domain.TargetID, sourceUID string) ([]domain.InventoryEdge, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT edge_type, source_uid, dest_uid, source_kind, dest_kind
		 FROM inventory_edges WHERE target_id = $1 AND source_uid = $2`,
		string(targetID), sourceUID)
	if err != nil {
		return nil, fmt.Errorf("list edges by source UID: %w", err)
	}
	defer rows.Close()
	return scanEdges(rows)
}

func (r *EdgeRepo) ListByDestUID(ctx context.Context, targetID domain.TargetID, destUID string) ([]domain.InventoryEdge, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT edge_type, source_uid, dest_uid, source_kind, dest_kind
		 FROM inventory_edges WHERE target_id = $1 AND dest_uid = $2`,
		string(targetID), destUID)
	if err != nil {
		return nil, fmt.Errorf("list edges by dest UID: %w", err)
	}
	defer rows.Close()
	return scanEdges(rows)
}

func scanEdges(rows *sql.Rows) ([]domain.InventoryEdge, error) {
	var edges []domain.InventoryEdge
	for rows.Next() {
		var e domain.InventoryEdge
		if err := rows.Scan(&e.EdgeType, &e.SourceUID, &e.DestUID, &e.SourceKind, &e.DestKind); err != nil {
			return nil, fmt.Errorf("scan edge: %w", err)
		}
		edges = append(edges, e)
	}
	return edges, rows.Err()
}
