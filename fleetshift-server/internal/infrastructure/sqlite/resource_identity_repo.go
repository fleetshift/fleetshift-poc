package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
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

	collectionName := string(s.Name.Collection())
	resourceID := string(s.Name.ID())

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO platform_resources (collection_name, resource_id, labels, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)`,
		collectionName, resourceID, string(labels),
		s.CreatedAt.UTC().Format(time.RFC3339),
		s.UpdatedAt.UTC().Format(time.RFC3339),
	)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("platform resource %q: %w", s.Name, domain.ErrAlreadyExists)
		}
		return fmt.Errorf("insert platform resource: %w", err)
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
// GetByName -- load resource + join all children, falling back to a
// virtual (no physical row) resource derived purely from
// representations.
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) GetByName(ctx context.Context, name domain.ResourceName) (*domain.PlatformResource, error) {
	collectionName := string(name.Collection())
	resourceID := string(name.ID())
	row := r.DB.QueryRowContext(ctx,
		`SELECT collection_name, resource_id, labels, created_at, updated_at
		 FROM platform_resources WHERE collection_name = ? AND resource_id = ?`,
		collectionName, resourceID,
	)
	snap, err := scanPlatformResourceSnapshot(row)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return r.virtualByName(ctx, name)
		}
		return nil, err
	}
	return r.loadChildren(ctx, snap)
}

// virtualByName synthesizes a [domain.PlatformResource] with no
// physical platform_resources row, for a name that has extension
// resource representations but has never needed its own labels,
// aliases, or relationships (see repository.go's virtual-resource
// doc). Aliases/relationships can never exist without a physical row
// (they're FK'd to platform_resources), so representations are the
// only signal checked here. Returns [domain.ErrNotFound] if the name
// has no representations either -- i.e. it truly doesn't exist.
func (r *ResourceIdentityRepo) virtualByName(ctx context.Context, name domain.ResourceName) (*domain.PlatformResource, error) {
	collectionName := string(name.Collection())
	resourceID := string(name.ID())

	var minCreated, maxUpdated sql.NullString
	err := r.DB.QueryRowContext(ctx,
		`SELECT MIN(created_at), MAX(updated_at) FROM extension_resources
		 WHERE collection_name = ? AND resource_id = ?`,
		collectionName, resourceID,
	).Scan(&minCreated, &maxUpdated)
	if err != nil {
		return nil, fmt.Errorf("check virtual platform resource: %w", err)
	}
	if !minCreated.Valid {
		return nil, fmt.Errorf("platform resource %q: %w", name, domain.ErrNotFound)
	}

	createdAt, err := time.Parse(time.RFC3339, minCreated.String)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, maxUpdated.String)
	if err != nil {
		return nil, fmt.Errorf("parse updated_at: %w", err)
	}

	snap := domain.PlatformResourceSnapshot{
		Name:      name,
		Labels:    map[string]string{},
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
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

	collectionName := string(s.Name.Collection())
	resourceID := string(s.Name.ID())

	res, err := r.DB.ExecContext(ctx,
		`UPDATE platform_resources SET labels = ?, updated_at = ? WHERE collection_name = ? AND resource_id = ?`,
		string(labels),
		s.UpdatedAt.UTC().Format(time.RFC3339),
		collectionName, resourceID,
	)
	if err != nil {
		return fmt.Errorf("update platform resource: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("platform resource %q: %w", s.Name, domain.ErrNotFound)
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
// ListByCollection -- physical rows plus virtual-only names
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) ListByCollection(ctx context.Context, collection domain.CollectionName) ([]*domain.PlatformResource, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT collection_name, resource_id, labels, created_at, updated_at
		 FROM platform_resources WHERE collection_name = ? ORDER BY resource_id`,
		string(collection),
	)
	if err != nil {
		return nil, fmt.Errorf("list platform resources: %w", err)
	}

	var snaps []domain.PlatformResourceSnapshot
	physical := make(map[domain.ResourceName]bool)
	for rows.Next() {
		snap, err := scanPlatformResourceSnapshot(rows)
		if err != nil {
			rows.Close()
			return nil, err
		}
		snaps = append(snaps, snap)
		physical[snap.Name] = true
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	virtual, err := r.listVirtualByCollection(ctx, collection, physical)
	if err != nil {
		return nil, err
	}
	snaps = append(snaps, virtual...)

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

// listVirtualByCollection finds names under collection that have
// extension resource representations but no physical platform_resources
// row (see virtualByName's doc for why representations are the only
// signal that matters).
func (r *ResourceIdentityRepo) listVirtualByCollection(ctx context.Context, collection domain.CollectionName, physical map[domain.ResourceName]bool) ([]domain.PlatformResourceSnapshot, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT resource_id, MIN(created_at), MAX(updated_at)
		 FROM extension_resources WHERE collection_name = ?
		 GROUP BY resource_id`,
		string(collection),
	)
	if err != nil {
		return nil, fmt.Errorf("list virtual platform resources: %w", err)
	}
	defer rows.Close()

	var result []domain.PlatformResourceSnapshot
	for rows.Next() {
		var resourceID, createdAtStr, updatedAtStr string
		if err := rows.Scan(&resourceID, &createdAtStr, &updatedAtStr); err != nil {
			return nil, fmt.Errorf("scan virtual platform resource: %w", err)
		}
		name := domain.ResourceName(string(collection) + "/" + resourceID)
		if physical[name] {
			continue
		}
		createdAt, err := time.Parse(time.RFC3339, createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse created_at: %w", err)
		}
		updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse updated_at: %w", err)
		}
		result = append(result, domain.PlatformResourceSnapshot{
			Name:      name,
			Labels:    map[string]string{},
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		})
	}
	return result, rows.Err()
}

// ---------------------------------------------------------------------------
// Cross-resource lookups
// ---------------------------------------------------------------------------

// ResolveAlias reads by (namespace, key, value) alone, with no regard
// for source_extension_resource_uid: multiple contributors can hold a
// row for the very same claim (see the migration's resource_aliases
// doc comment), but resolveAliasesForWrite's own enforcement (the
// SQLite counterpart to Postgres's EXCLUDE constraints) guarantees
// they all agree on which resource it belongs to, so LIMIT 1 -- and
// therefore which contributor's row SQLite happens to pick -- is safe
// here regardless.
func (r *ResourceIdentityRepo) ResolveAlias(ctx context.Context, alias domain.Alias) (domain.ResourceName, error) {
	var collectionName, resourceID string
	err := r.DB.QueryRowContext(ctx,
		`SELECT platform_collection_name, platform_resource_id FROM resource_aliases
		 WHERE namespace = ? AND key = ? AND value = ? LIMIT 1`,
		string(alias.Namespace), string(alias.Key), string(alias.Value),
	).Scan(&collectionName, &resourceID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", fmt.Errorf("alias %s/%s/%s: %w", alias.Namespace, alias.Key, alias.Value, domain.ErrNotFound)
		}
		return "", fmt.Errorf("resolve alias: %w", err)
	}
	return domain.ResourceName(collectionName + "/" + resourceID), nil
}

// GetRepresentation derives a representation on read from
// extension_resources alone: it exists iff an extension_resources row
// matching (service, relative name) exists. There is no dependency on
// a platform_resources row -- representations for a virtual platform
// resource are derived exactly the same way. There is nothing to
// reconcile on Create/Update/Delete -- the representation appears and
// disappears exactly when the extension_resources row is
// created/deleted.
func (r *ResourceIdentityRepo) GetRepresentation(ctx context.Context, name domain.FullResourceName) (domain.ResourceRepresentation, error) {
	service := name.ServiceName()
	relative := name.ResourceName()
	collectionName := string(relative.Collection())
	resourceID := string(relative.ID())
	row := r.DB.QueryRowContext(ctx, representationDerivationQuery+`
		WHERE er.service_name = ? AND er.collection_name = ? AND er.resource_id = ?`,
		string(service), collectionName, resourceID,
	)
	return scanRepresentation(row)
}

// representationDerivationQuery joins extension_resources to
// extension_resource_types for the representation's declared API
// version. Callers append a WHERE clause to scope by (service, name)
// or by (collection_name, resource_id) alone.
const representationDerivationQuery = `
	SELECT er.service_name, ert.api_version, er.collection_name, er.resource_id, er.uid, er.created_at, er.updated_at
	FROM extension_resources er
	JOIN extension_resource_types ert ON ert.service_name = er.service_name AND ert.type_name = er.type_name
`

// ---------------------------------------------------------------------------
// Reconciliation helpers -- upsert child entities from aggregate state
// ---------------------------------------------------------------------------

// reconcileAliases inserts s.Aliases as platform-direct claims --
// source_extension_resource_uid explicitly NULL, since these come
// straight from the PlatformResource aggregate itself rather than
// from any extension resource's inventory report (see the migration's
// resource_aliases doc comment for why that column is nullable at
// all). It does not retract a previously-set alias that's absent from
// s.Aliases on Update -- see the Postgres sibling's identical doc
// comment for why that's specific to the inventory report path. LIMIT
// 1 on the existence check is safe for the same reason it is in
// ResolveAlias -- see that method's doc comment.
func (r *ResourceIdentityRepo) reconcileAliases(ctx context.Context, s domain.PlatformResourceSnapshot) error {
	collectionName := string(s.Name.Collection())
	resourceID := string(s.Name.ID())
	for _, alias := range s.Aliases {
		var existingCollection, existingResourceID string
		err := r.DB.QueryRowContext(ctx,
			`SELECT platform_collection_name, platform_resource_id FROM resource_aliases
			 WHERE namespace = ? AND key = ? AND value = ? LIMIT 1`,
			string(alias.Namespace), string(alias.Key), string(alias.Value),
		).Scan(&existingCollection, &existingResourceID)
		if err == nil {
			if existingCollection == collectionName && existingResourceID == resourceID {
				continue
			}
			return fmt.Errorf("alias %s/%s/%s owned by %s/%s, not %s: %w",
				alias.Namespace, alias.Key, alias.Value,
				existingCollection, existingResourceID, s.Name, domain.ErrAlreadyExists)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("check existing alias: %w", err)
		}

		_, err = r.DB.ExecContext(ctx,
			`INSERT INTO resource_aliases (namespace, key, value, platform_collection_name, platform_resource_id, source_extension_resource_uid, created_at)
			 VALUES (?, ?, ?, ?, ?, NULL, ?)`,
			string(alias.Namespace), string(alias.Key), string(alias.Value),
			collectionName, resourceID, time.Now().UTC().Format(time.RFC3339),
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
	sourceCollection := string(s.Name.Collection())
	sourceResourceID := string(s.Name.ID())
	for _, rel := range s.Relationships {
		targetCollection := string(rel.TargetName.Collection())
		targetResourceID := string(rel.TargetName.ID())
		_, err := r.DB.ExecContext(ctx,
			`INSERT INTO resource_relationships
			   (source_collection_name, source_resource_id, type, target_collection_name, target_resource_id, source_service, created_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(source_collection_name, source_resource_id, type, target_collection_name, target_resource_id) DO UPDATE SET
			   source_service = excluded.source_service`,
			sourceCollection, sourceResourceID, string(rel.Type), targetCollection, targetResourceID,
			string(rel.SourceService), rel.CreatedAt.UTC().Format(time.RFC3339),
		)
		if err != nil {
			return fmt.Errorf("upsert relationship: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Batch alias resolution (see repository.go's doc comments)
// ---------------------------------------------------------------------------

// ResolveAliasesBatch implements
// [domain.ResourceIdentityRepository.ResolveAliasesBatch] as a single
// round trip against resource_aliases, whose rows already carry the
// owning resource's name directly -- no join needed. DISTINCT
// collapses the possibly-many contributor rows behind a single
// (namespace, key, value) -- see the migration's resource_aliases doc
// comment -- back down to the one result map entry each represents;
// they're guaranteed to agree on platform_collection_name/
// platform_resource_id by the same enforcement ResolveAlias's doc
// comment relies on, so which contributor's row survives the collapse
// doesn't matter.
func (r *ResourceIdentityRepo) ResolveAliasesBatch(ctx context.Context, aliases []domain.Alias) (map[domain.Alias]domain.ResourceName, error) {
	if len(aliases) == 0 {
		return map[domain.Alias]domain.ResourceName{}, nil
	}

	placeholders := make([]string, len(aliases))
	args := make([]any, 0, len(aliases)*3)
	for i, a := range aliases {
		placeholders[i] = "(?, ?, ?)"
		args = append(args, string(a.Namespace), string(a.Key), string(a.Value))
	}

	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT DISTINCT namespace, key, value, platform_collection_name, platform_resource_id
			FROM resource_aliases
			WHERE (namespace, key, value) IN (%s)`, strings.Join(placeholders, ", ")),
		args...,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve aliases batch: %w", err)
	}
	defer rows.Close()

	result := make(map[domain.Alias]domain.ResourceName, len(aliases))
	for rows.Next() {
		var ns, key, value, collectionName, resourceID string
		if err := rows.Scan(&ns, &key, &value, &collectionName, &resourceID); err != nil {
			return nil, fmt.Errorf("scan resolve aliases result: %w", err)
		}
		result[domain.Alias{Namespace: domain.AliasNamespace(ns), Key: domain.AliasKey(key), Value: domain.AliasValue(value)}] =
			domain.ResourceName(collectionName + "/" + resourceID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("resolve aliases batch: %w", err)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Load helpers -- join child entities into snapshot and hydrate aggregate
// ---------------------------------------------------------------------------

func (r *ResourceIdentityRepo) loadChildren(ctx context.Context, snap domain.PlatformResourceSnapshot) (*domain.PlatformResource, error) {
	reps, err := r.loadRepresentations(ctx, snap.Name)
	if err != nil {
		return nil, err
	}
	snap.Representations = reps

	aliases, err := r.loadAliases(ctx, snap.Name)
	if err != nil {
		return nil, err
	}
	snap.Aliases = aliases

	rels, err := r.loadRelationships(ctx, snap.Name)
	if err != nil {
		return nil, err
	}
	snap.Relationships = rels

	return domain.PlatformResourceFromSnapshot(snap), nil
}

func (r *ResourceIdentityRepo) loadRepresentations(ctx context.Context, name domain.ResourceName) ([]domain.ResourceRepresentationSnapshot, error) {
	rows, err := r.DB.QueryContext(ctx, representationDerivationQuery+`
		WHERE er.collection_name = ? AND er.resource_id = ?
		ORDER BY er.service_name`,
		string(name.Collection()), string(name.ID()),
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

// loadAliases returns one snapshot per distinct (namespace, key,
// value) claimed for name, regardless of how many contributors' rows
// back it -- see the migration's resource_aliases doc comment for why
// there can be more than one row per claim, and ResolveAliasesBatch's
// doc comment for why DISTINCT is enough to collapse them (they're
// never allowed to disagree on anything DISTINCT would need to
// preserve).
func (r *ResourceIdentityRepo) loadAliases(ctx context.Context, name domain.ResourceName) ([]domain.ResourceAliasSnapshot, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT DISTINCT namespace, key, value FROM resource_aliases
		 WHERE platform_collection_name = ? AND platform_resource_id = ? ORDER BY namespace, key`,
		string(name.Collection()), string(name.ID()),
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

func (r *ResourceIdentityRepo) loadRelationships(ctx context.Context, name domain.ResourceName) ([]domain.ResourceRelationshipSnapshot, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT target_collection_name, target_resource_id, type, source_service, created_at
		 FROM resource_relationships
		 WHERE source_collection_name = ? AND source_resource_id = ? ORDER BY type, target_collection_name, target_resource_id`,
		string(name.Collection()), string(name.ID()),
	)
	if err != nil {
		return nil, fmt.Errorf("load relationships: %w", err)
	}
	defer rows.Close()

	var result []domain.ResourceRelationshipSnapshot
	for rows.Next() {
		var targetCollection, targetResourceID, relType, svc, createdAtStr string
		if err := rows.Scan(&targetCollection, &targetResourceID, &relType, &svc, &createdAtStr); err != nil {
			return nil, fmt.Errorf("scan relationship: %w", err)
		}
		createdAt, err := time.Parse(time.RFC3339, createdAtStr)
		if err != nil {
			return nil, fmt.Errorf("parse created_at: %w", err)
		}
		result = append(result, domain.ResourceRelationshipSnapshot{
			SourceName:    name,
			Type:          domain.RelationshipType(relType),
			TargetName:    domain.ResourceName(targetCollection + "/" + targetResourceID),
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
	var collectionName, resourceID, labelsJSON, createdAtStr, updatedAtStr string

	if err := s.Scan(&collectionName, &resourceID, &labelsJSON, &createdAtStr, &updatedAtStr); err != nil {
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

	return domain.PlatformResourceSnapshot{
		Name:      domain.ResourceName(collectionName + "/" + resourceID),
		Labels:    labels,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func scanRepresentation(s scanner) (domain.ResourceRepresentation, error) {
	var serviceName, version, collectionName, resourceID string
	var extensionResourceUIDStr string
	var createdAtStr, updatedAtStr string

	if err := s.Scan(&serviceName, &version, &collectionName, &resourceID, &extensionResourceUIDStr, &createdAtStr, &updatedAtStr); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ResourceRepresentation{}, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return domain.ResourceRepresentation{}, fmt.Errorf("scan representation: %w", err)
	}

	erUID, err := domain.ParseExtensionResourceUID(extensionResourceUIDStr)
	if err != nil {
		return domain.ResourceRepresentation{}, fmt.Errorf("parse extension_resource_uid: %w", err)
	}

	createdAt, err := time.Parse(time.RFC3339, createdAtStr)
	if err != nil {
		return domain.ResourceRepresentation{}, fmt.Errorf("parse created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339, updatedAtStr)
	if err != nil {
		return domain.ResourceRepresentation{}, fmt.Errorf("parse updated_at: %w", err)
	}

	snap := domain.ResourceRepresentationSnapshot{
		ServiceName:          domain.ServiceName(serviceName),
		Version:              domain.APIVersion(version),
		Name:                 domain.ResourceName(collectionName + "/" + resourceID),
		ExtensionResourceUID: erUID,
		CreatedAt:            createdAt,
		UpdatedAt:            updatedAt,
	}
	return domain.ResourceRepresentationFromSnapshot(snap), nil
}
