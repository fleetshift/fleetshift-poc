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

// ResourceIdentityRepo implements [domain.ResourceIdentityRepository]
// backed by SQLite.
type ResourceIdentityRepo struct {
	DB *sql.Tx
}

// ---------------------------------------------------------------------------
// Create -- insert resource + all child entities from aggregate state
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) Create(ctx context.Context, pr *domain.PlatformResource) error {
	s := pr.Snapshot()
	labels, err := json.Marshal(s.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO platform_resources (uid, collection_id, relative_name, labels, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		string(s.UID), string(s.CollectionID), string(s.RelativeName), string(labels),
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("platform resource %q: %w", s.RelativeName, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert platform resource: %w", err)
	}

	if err := r.reconcileRepresentations(ctx, s); err != nil {
		return err
	}
	if err := r.reconcileAliases(ctx, s); err != nil {
		return err
	}
	if err := r.reconcileRelationships(ctx, s); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// Get / GetByName -- load resource + join all children
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) Get(ctx context.Context, uid domain.PlatformResourceUID) (*domain.PlatformResource, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT uid, collection_id, relative_name, labels, created_at, updated_at, deleted_at
		 FROM platform_resources WHERE uid = ?`,
		string(uid),
	)
	snap, err := scanPlatformResourceSnapshot(row)
	if err != nil {
		return nil, err
	}
	return r.loadChildren(ctx, snap)
}

func (r *ResourceIdentityRepo) GetByName(ctx context.Context, name domain.RelativeResourceName) (*domain.PlatformResource, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT uid, collection_id, relative_name, labels, created_at, updated_at, deleted_at
		 FROM platform_resources WHERE relative_name = ?`,
		string(name),
	)
	snap, err := scanPlatformResourceSnapshot(row)
	if err != nil {
		return nil, err
	}
	return r.loadChildren(ctx, snap)
}

// ---------------------------------------------------------------------------
// Update -- reconcile aggregate state to storage
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) Update(ctx context.Context, pr *domain.PlatformResource) error {
	s := pr.Snapshot()
	labels, err := json.Marshal(s.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE platform_resources SET labels = ?, updated_at = ? WHERE uid = ?`,
		string(labels),
		s.UpdatedAt.UTC().Format(time.RFC3339),
		string(s.UID),
	)
	if err != nil {
		return fmt.Errorf("update platform resource: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("platform resource %q: %w", s.UID, domain.ErrNotFound)
	}

	if err := r.reconcileRepresentations(ctx, s); err != nil {
		return err
	}
	if err := r.reconcileAliases(ctx, s); err != nil {
		return err
	}
	if err := r.reconcileRelationships(ctx, s); err != nil {
		return err
	}
	return nil
}

// ---------------------------------------------------------------------------
// ListByCollection
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) ListByCollection(ctx context.Context, collection domain.CollectionID) ([]*domain.PlatformResource, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT uid, collection_id, relative_name, labels, created_at, updated_at, deleted_at
		 FROM platform_resources WHERE collection_id = ? ORDER BY relative_name`,
		string(collection),
	)
	if err != nil {
		return nil, fmt.Errorf("list platform resources: %w", err)
	}
	defer rows.Close()

	var snaps []domain.PlatformResourceSnapshot
	for rows.Next() {
		snap, err := scanPlatformResourceSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, snap)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows.Close()

	result := make([]*domain.PlatformResource, 0, len(snaps))
	for _, snap := range snaps {
		pr, err := r.loadChildren(ctx, snap)
		if err != nil {
			return nil, err
		}
		result = append(result, pr)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Cross-resource lookups
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) ResolveAlias(ctx context.Context, alias domain.Alias) (domain.PlatformResourceUID, error) {
	var uid string
	err := r.DB.QueryRowContext(ctx,
		`SELECT platform_uid FROM resource_aliases
		 WHERE namespace = ? AND key = ? AND value = ?`,
		string(alias.Namespace), string(alias.Key), string(alias.Value),
	).Scan(&uid)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("alias %s/%s/%s: %w", alias.Namespace, alias.Key, alias.Value, domain.ErrNotFound)
		}
		return "", fmt.Errorf("resolve alias: %w", err)
	}
	return domain.PlatformResourceUID(uid), nil
}

func (r *ResourceIdentityRepo) GetRepresentation(ctx context.Context, name domain.FullResourceName) (domain.ResourceRepresentation, error) {
	service := name.ServiceName()
	relative := name.RelativeName()
	row := r.DB.QueryRowContext(ctx,
		`SELECT platform_uid, service_name, version, collection_id, relative_name, roles, labels, created_at, updated_at, deleted_at
		 FROM resource_representations
		 WHERE service_name = ? AND relative_name = ?`,
		string(service), string(relative),
	)
	return scanRepresentation(row)
}

// ---------------------------------------------------------------------------
// Reconciliation helpers -- upsert child entities from aggregate state
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) reconcileRepresentations(ctx context.Context, s domain.PlatformResourceSnapshot) error {
	for _, rep := range s.Representations {
		roles, err := json.Marshal(rep.Roles)
		if err != nil {
			return fmt.Errorf("marshal roles: %w", err)
		}
		labels, err := json.Marshal(rep.Labels)
		if err != nil {
			return fmt.Errorf("marshal labels: %w", err)
		}

		var existingUID string
		checkErr := r.DB.QueryRowContext(ctx,
			`SELECT platform_uid FROM resource_representations
			 WHERE service_name = ? AND collection_id = ? AND relative_name = ?`,
			string(rep.ServiceName), string(rep.CollectionID), string(rep.RelativeName),
		).Scan(&existingUID)
		if checkErr == nil && domain.PlatformResourceUID(existingUID) != rep.PlatformUID {
			return fmt.Errorf("representation %s/%s/%s owned by %s, not %s: %w",
				rep.ServiceName, rep.CollectionID, rep.RelativeName,
				existingUID, rep.PlatformUID, domain.ErrAlreadyExists)
		}

		var deletedAtStr *string
		if rep.DeletedAt != nil {
			s := rep.DeletedAt.UTC().Format(time.RFC3339)
			deletedAtStr = &s
		}

		_, err = r.DB.ExecContext(ctx,
			`INSERT INTO resource_representations
			 (platform_uid, service_name, version, collection_id, relative_name, roles, labels, created_at, updated_at, deleted_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(service_name, collection_id, relative_name) DO UPDATE SET
			   version = excluded.version,
			   roles = excluded.roles,
			   labels = excluded.labels,
			   updated_at = excluded.updated_at,
			   deleted_at = excluded.deleted_at`,
			string(rep.PlatformUID), string(rep.ServiceName), string(rep.Version),
			string(rep.CollectionID), string(rep.RelativeName),
			string(roles), string(labels),
			rep.CreatedAt.UTC().Format(time.RFC3339),
			rep.UpdatedAt.UTC().Format(time.RFC3339),
			deletedAtStr,
		)
		if err != nil {
			return fmt.Errorf("upsert representation: %w", err)
		}
	}
	return nil
}

func (r *ResourceIdentityRepo) reconcileAliases(ctx context.Context, s domain.PlatformResourceSnapshot) error {
	for _, alias := range s.Aliases {
		var existingUID string
		err := r.DB.QueryRowContext(ctx,
			`SELECT platform_uid FROM resource_aliases
			 WHERE namespace = ? AND key = ? AND value = ?`,
			string(alias.Namespace), string(alias.Key), string(alias.Value),
		).Scan(&existingUID)
		if err == nil {
			if domain.PlatformResourceUID(existingUID) == s.UID {
				continue
			}
			return fmt.Errorf("alias %s/%s/%s owned by %s, not %s: %w",
				alias.Namespace, alias.Key, alias.Value,
				existingUID, s.UID, domain.ErrAlreadyExists)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("check existing alias: %w", err)
		}

		_, err = r.DB.ExecContext(ctx,
			`INSERT INTO resource_aliases (namespace, key, value, platform_uid, created_at)
			 VALUES (?, ?, ?, ?, ?)`,
			string(alias.Namespace), string(alias.Key), string(alias.Value),
			string(s.UID), time.Now().UTC().Format(time.RFC3339),
		)
		if err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("alias %s/%s/%s: %w", alias.Namespace, alias.Key, alias.Value, domain.ErrAlreadyExists)
			}
			return fmt.Errorf("insert alias: %w", err)
		}
	}
	return nil
}

func (r *ResourceIdentityRepo) reconcileRelationships(ctx context.Context, s domain.PlatformResourceSnapshot) error {
	for _, rel := range s.Relationships {
		_, err := r.DB.ExecContext(ctx,
			`INSERT INTO resource_relationships (source_uid, type, target_uid, source_service, created_at)
			 VALUES (?, ?, ?, ?, ?)
			 ON CONFLICT(source_uid, type, target_uid) DO UPDATE SET
			   source_service = excluded.source_service`,
			string(rel.SourceUID), string(rel.Type), string(rel.TargetUID),
			string(rel.SourceService), rel.CreatedAt.UTC().Format(time.RFC3339),
		)
		if err != nil {
			return fmt.Errorf("upsert relationship: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Load helpers -- join child entities into snapshot and hydrate aggregate
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) loadChildren(ctx context.Context, snap domain.PlatformResourceSnapshot) (*domain.PlatformResource, error) {
	reps, err := r.loadRepresentations(ctx, snap.UID)
	if err != nil {
		return nil, err
	}
	snap.Representations = reps

	aliases, err := r.loadAliases(ctx, snap.UID)
	if err != nil {
		return nil, err
	}
	snap.Aliases = aliases

	rels, err := r.loadRelationships(ctx, snap.UID)
	if err != nil {
		return nil, err
	}
	snap.Relationships = rels

	return domain.PlatformResourceFromSnapshot(snap), nil
}

func (r *ResourceIdentityRepo) loadRepresentations(ctx context.Context, uid domain.PlatformResourceUID) ([]domain.ResourceRepresentationSnapshot, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT platform_uid, service_name, version, collection_id, relative_name, roles, labels, created_at, updated_at, deleted_at
		 FROM resource_representations
		 WHERE platform_uid = ?
		 ORDER BY service_name`,
		string(uid),
	)
	if err != nil {
		return nil, fmt.Errorf("load representations: %w", err)
	}
	defer rows.Close()

	var result []domain.ResourceRepresentationSnapshot
	for rows.Next() {
		rep, err := scanRepresentation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, rep.Snapshot())
	}
	return result, rows.Err()
}

func (r *ResourceIdentityRepo) loadAliases(ctx context.Context, uid domain.PlatformResourceUID) ([]domain.ResourceAliasSnapshot, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT namespace, key, value FROM resource_aliases
		 WHERE platform_uid = ? ORDER BY namespace, key`,
		string(uid),
	)
	if err != nil {
		return nil, fmt.Errorf("load aliases: %w", err)
	}
	defer rows.Close()

	var result []domain.ResourceAliasSnapshot
	for rows.Next() {
		var ns, k, v string
		if err := rows.Scan(&ns, &k, &v); err != nil {
			return nil, fmt.Errorf("scan alias: %w", err)
		}
		result = append(result, domain.ResourceAliasSnapshot{
			Namespace: domain.AliasNamespace(ns),
			Key:       domain.AliasKey(k),
			Value:     domain.AliasValue(v),
		})
	}
	return result, rows.Err()
}

func (r *ResourceIdentityRepo) loadRelationships(ctx context.Context, uid domain.PlatformResourceUID) ([]domain.ResourceRelationshipSnapshot, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT source_uid, type, target_uid, source_service, created_at
		 FROM resource_relationships
		 WHERE source_uid = ? ORDER BY type, target_uid`,
		string(uid),
	)
	if err != nil {
		return nil, fmt.Errorf("load relationships: %w", err)
	}
	defer rows.Close()

	var result []domain.ResourceRelationshipSnapshot
	for rows.Next() {
		var srcUID, relType, tgtUID, svc, createdAtStr string
		if err := rows.Scan(&srcUID, &relType, &tgtUID, &svc, &createdAtStr); err != nil {
			return nil, fmt.Errorf("scan relationship: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339, createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse created_at: %w", err)
		}
		result = append(result, domain.ResourceRelationshipSnapshot{
			SourceUID:     domain.PlatformResourceUID(srcUID),
			Type:          domain.RelationshipType(relType),
			TargetUID:     domain.PlatformResourceUID(tgtUID),
			SourceService: domain.ServiceName(svc),
			CreatedAt:     createdAt,
		})
	}
	return result, rows.Err()
}

// ---------------------------------------------------------------------------
// Scan helpers
// ---------------------------------------------------------------------------

func scanPlatformResourceSnapshot(s scanner) (domain.PlatformResourceSnapshot, error) {
	var uid, collectionID, relativeName, labelsJSON, createdAtStr, updatedAtStr string
	var deletedAtStr sql.NullString

	if err := s.Scan(&uid, &collectionID, &relativeName, &labelsJSON, &createdAtStr, &updatedAtStr, &deletedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.PlatformResourceSnapshot{}, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return domain.PlatformResourceSnapshot{}, fmt.Errorf("scan platform resource: %w", err)
	}

	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		return domain.PlatformResourceSnapshot{}, fmt.Errorf("unmarshal labels: %w", err)
	}

	createdAt, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return domain.PlatformResourceSnapshot{}, fmt.Errorf("parse created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return domain.PlatformResourceSnapshot{}, fmt.Errorf("parse updated_at: %w", err)
	}

	snap := domain.PlatformResourceSnapshot{
		UID:          domain.PlatformResourceUID(uid),
		CollectionID: domain.CollectionID(collectionID),
		RelativeName: domain.RelativeResourceName(relativeName),
		Labels:       labels,
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}
	if deletedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, deletedAtStr.String)
		if err != nil {
			return domain.PlatformResourceSnapshot{}, fmt.Errorf("parse deleted_at: %w", err)
		}
		snap.DeletedAt = &t
	}

	return snap, nil
}

func scanRepresentation(s scanner) (domain.ResourceRepresentation, error) {
	var platformUID, serviceName, version, collectionID, relativeName string
	var rolesJSON, labelsJSON, createdAtStr, updatedAtStr string
	var deletedAtStr sql.NullString

	if err := s.Scan(&platformUID, &serviceName, &version, &collectionID, &relativeName, &rolesJSON, &labelsJSON, &createdAtStr, &updatedAtStr, &deletedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ResourceRepresentation{}, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return domain.ResourceRepresentation{}, fmt.Errorf("scan representation: %w", err)
	}

	var roles []domain.RepresentationRole
	if err := json.Unmarshal([]byte(rolesJSON), &roles); err != nil {
		return domain.ResourceRepresentation{}, fmt.Errorf("unmarshal roles: %w", err)
	}

	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		return domain.ResourceRepresentation{}, fmt.Errorf("unmarshal labels: %w", err)
	}

	createdAt, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return domain.ResourceRepresentation{}, fmt.Errorf("parse created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return domain.ResourceRepresentation{}, fmt.Errorf("parse updated_at: %w", err)
	}

	rep := domain.ResourceRepresentation{
		PlatformUID:  domain.PlatformResourceUID(platformUID),
		ServiceName:  domain.ServiceName(serviceName),
		Version:      domain.APIVersion(version),
		CollectionID: domain.CollectionID(collectionID),
		RelativeName: domain.RelativeResourceName(relativeName),
		Roles:        roles,
		Labels:       labels,
		CreatedAt:    createdAt,
		UpdatedAt:    updatedAt,
	}
	if deletedAtStr.Valid {
		t, err := time.Parse(time.RFC3339, deletedAtStr.String)
		if err != nil {
			return domain.ResourceRepresentation{}, fmt.Errorf("parse deleted_at: %w", err)
		}
		rep.DeletedAt = &t
	}

	return rep, nil
}
