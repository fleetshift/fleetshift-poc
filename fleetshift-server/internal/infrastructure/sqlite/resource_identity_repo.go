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
// Platform resources
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) CreatePlatformResource(ctx context.Context, pr *domain.PlatformResource) error {
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
	return nil
}

func (r *ResourceIdentityRepo) GetPlatformResourceByUID(ctx context.Context, uid domain.PlatformResourceUID) (*domain.PlatformResource, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT uid, collection_id, relative_name, labels, created_at, updated_at, deleted_at
		 FROM platform_resources WHERE uid = ?`,
		string(uid),
	)
	return scanPlatformResource(row)
}

func (r *ResourceIdentityRepo) GetPlatformResourceByName(ctx context.Context, name domain.RelativeResourceName) (*domain.PlatformResource, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT uid, collection_id, relative_name, labels, created_at, updated_at, deleted_at
		 FROM platform_resources WHERE relative_name = ?`,
		string(name),
	)
	return scanPlatformResource(row)
}

func (r *ResourceIdentityRepo) ListPlatformResourcesByCollection(ctx context.Context, collection domain.CollectionID) ([]domain.PlatformResource, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT uid, collection_id, relative_name, labels, created_at, updated_at, deleted_at
		 FROM platform_resources WHERE collection_id = ? ORDER BY relative_name`,
		string(collection),
	)
	if err != nil {
		return nil, fmt.Errorf("list platform resources: %w", err)
	}
	defer rows.Close()

	var result []domain.PlatformResource
	for rows.Next() {
		pr, err := scanPlatformResource(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *pr)
	}
	return result, rows.Err()
}

func (r *ResourceIdentityRepo) UpdatePlatformResourceLabels(ctx context.Context, pr *domain.PlatformResource) error {
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
		return fmt.Errorf("update platform resource labels: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("platform resource %q: %w", s.UID, domain.ErrNotFound)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Representations
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) PutRepresentation(ctx context.Context, rep domain.ResourceRepresentation) error {
	roles, err := json.Marshal(rep.Roles)
	if err != nil {
		return fmt.Errorf("marshal roles: %w", err)
	}
	labels, err := json.Marshal(rep.Labels)
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	// Check-first: if the representation exists with a different owner,
	// reject before attempting the INSERT to keep behavior consistent
	// with Postgres.
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

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO resource_representations
		 (platform_uid, service_name, version, collection_id, relative_name, roles, labels, created_at, updated_at, deleted_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
		 ON CONFLICT(service_name, collection_id, relative_name) DO UPDATE SET
		   version = excluded.version,
		   roles = excluded.roles,
		   labels = excluded.labels,
		   updated_at = excluded.updated_at,
		   deleted_at = NULL`,
		string(rep.PlatformUID), string(rep.ServiceName), string(rep.Version),
		string(rep.CollectionID), string(rep.RelativeName),
		string(roles), string(labels),
		rep.CreatedAt.UTC().Format(time.RFC3339),
		rep.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("representation %s/%s/%s: %w", rep.ServiceName, rep.CollectionID, rep.RelativeName, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("put representation: %w", err)
	}
	return nil
}

func (r *ResourceIdentityRepo) TombstoneRepresentation(ctx context.Context, service domain.ServiceName, collection domain.CollectionID, name domain.RelativeResourceName, now time.Time) error {
	res, err := r.DB.ExecContext(ctx,
		`UPDATE resource_representations SET deleted_at = ?, updated_at = ?
		 WHERE service_name = ? AND collection_id = ? AND relative_name = ? AND deleted_at IS NULL`,
		now.UTC().Format(time.RFC3339),
		now.UTC().Format(time.RFC3339),
		string(service), string(collection), string(name),
	)
	if err != nil {
		return fmt.Errorf("tombstone representation: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("representation %s/%s/%s: %w", service, collection, name, domain.ErrNotFound)
	}
	return nil
}

func (r *ResourceIdentityRepo) ListRepresentationsByPlatformUID(ctx context.Context, uid domain.PlatformResourceUID) ([]domain.ResourceRepresentation, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT platform_uid, service_name, version, collection_id, relative_name, roles, labels, created_at, updated_at, deleted_at
		 FROM resource_representations
		 WHERE platform_uid = ? AND deleted_at IS NULL
		 ORDER BY service_name`,
		string(uid),
	)
	if err != nil {
		return nil, fmt.Errorf("list representations: %w", err)
	}
	defer rows.Close()

	var result []domain.ResourceRepresentation
	for rows.Next() {
		rep, err := scanRepresentation(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, rep)
	}
	return result, rows.Err()
}

func (r *ResourceIdentityRepo) GetRepresentation(ctx context.Context, service domain.ServiceName, collection domain.CollectionID, name domain.RelativeResourceName) (domain.ResourceRepresentation, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT platform_uid, service_name, version, collection_id, relative_name, roles, labels, created_at, updated_at, deleted_at
		 FROM resource_representations
		 WHERE service_name = ? AND collection_id = ? AND relative_name = ?`,
		string(service), string(collection), string(name),
	)
	return scanRepresentation(row)
}

// ---------------------------------------------------------------------------
// Aliases
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) PutAlias(ctx context.Context, uid domain.PlatformResourceUID, alias domain.Alias, now time.Time) error {
	// Check-first to keep behavior consistent with Postgres (which
	// aborts the transaction on a unique constraint violation).
	var existingUID string
	err := r.DB.QueryRowContext(ctx,
		`SELECT platform_uid FROM resource_aliases
		 WHERE namespace = ? AND key = ? AND value = ?`,
		string(alias.Namespace), string(alias.Key), string(alias.Value),
	).Scan(&existingUID)
	if err == nil {
		if domain.PlatformResourceUID(existingUID) == uid {
			return nil
		}
		return fmt.Errorf("alias %s/%s/%s owned by %s, not %s: %w",
			alias.Namespace, alias.Key, alias.Value,
			existingUID, uid, domain.ErrAlreadyExists)
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("check existing alias: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO resource_aliases (namespace, key, value, platform_uid, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		string(alias.Namespace), string(alias.Key), string(alias.Value),
		string(uid), now.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("alias %s/%s/%s: %w", alias.Namespace, alias.Key, alias.Value, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("put alias: %w", err)
	}
	return nil
}

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

func (r *ResourceIdentityRepo) ListAliasesByPlatformUID(ctx context.Context, uid domain.PlatformResourceUID) ([]domain.Alias, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT namespace, key, value FROM resource_aliases
		 WHERE platform_uid = ? ORDER BY namespace, key`,
		string(uid),
	)
	if err != nil {
		return nil, fmt.Errorf("list aliases: %w", err)
	}
	defer rows.Close()

	var result []domain.Alias
	for rows.Next() {
		var ns, k, v string
		if err := rows.Scan(&ns, &k, &v); err != nil {
			return nil, fmt.Errorf("scan alias: %w", err)
		}
		result = append(result, domain.Alias{
			Namespace: domain.AliasNamespace(ns),
			Key:       domain.AliasKey(k),
			Value:     domain.AliasValue(v),
		})
	}
	return result, rows.Err()
}

// ---------------------------------------------------------------------------
// Relationships
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) PutRelationship(ctx context.Context, rel domain.ResourceRelationship) error {
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO resource_relationships (source_uid, type, target_uid, source_service, created_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(source_uid, type, target_uid) DO UPDATE SET
		   source_service = excluded.source_service`,
		string(rel.SourceUID), string(rel.Type), string(rel.TargetUID),
		string(rel.SourceService), rel.CreatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		return fmt.Errorf("put relationship: %w", err)
	}
	return nil
}

func (r *ResourceIdentityRepo) ListRelationshipsBySourceUID(ctx context.Context, uid domain.PlatformResourceUID) ([]domain.ResourceRelationship, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT source_uid, type, target_uid, source_service, created_at
		 FROM resource_relationships
		 WHERE source_uid = ? ORDER BY type, target_uid`,
		string(uid),
	)
	if err != nil {
		return nil, fmt.Errorf("list relationships: %w", err)
	}
	defer rows.Close()

	var result []domain.ResourceRelationship
	for rows.Next() {
		var srcUID, relType, tgtUID, svc, createdAtStr string
		if err := rows.Scan(&srcUID, &relType, &tgtUID, &svc, &createdAtStr); err != nil {
			return nil, fmt.Errorf("scan relationship: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339, createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse created_at: %w", err)
		}
		result = append(result, domain.ResourceRelationship{
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

func scanPlatformResource(s scanner) (*domain.PlatformResource, error) {
	var uid, collectionID, relativeName, labelsJSON, createdAtStr, updatedAtStr string
	var deletedAtStr sql.NullString

	if err := s.Scan(&uid, &collectionID, &relativeName, &labelsJSON, &createdAtStr, &updatedAtStr, &deletedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return nil, fmt.Errorf("scan platform resource: %w", err)
	}

	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		return nil, fmt.Errorf("unmarshal labels: %w", err)
	}

	createdAt, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse updated_at: %w", err)
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
			return nil, fmt.Errorf("parse deleted_at: %w", err)
		}
		snap.DeletedAt = &t
	}

	return domain.PlatformResourceFromSnapshot(snap), nil
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
