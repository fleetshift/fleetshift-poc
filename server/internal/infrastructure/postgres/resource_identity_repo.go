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

// ResourceIdentityRepo implements [domain.ResourceIdentityRepository]
// backed by Postgres.
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
		 VALUES ($1, $2, $3, $4, $5)`,
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
	row := r.DB.QueryRowContext(ctx,
		platformResourceByNameQuery,
		string(name.Collection()), string(name.ID()),
	)
	return scanPlatformResourceAggregate(row)
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
		`UPDATE platform_resources SET labels = $1, updated_at = $2 WHERE collection_name = $3 AND resource_id = $4`,
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
		platformResourcesByCollectionQuery,
		string(collection),
	)
	if err != nil {
		return nil, fmt.Errorf("list platform resources: %w", err)
	}
	defer rows.Close()

	var result []*domain.PlatformResource
	for rows.Next() {
		pr, err := scanPlatformResourceAggregate(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, pr)
	}
	return result, rows.Err()
}

const platformResourceAggregateSelectPostgres = `
SELECT b.collection_name, b.resource_id, b.labels::text,
       to_char(b.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
       to_char(b.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
       COALESCE(reps.representations, '[]'::jsonb)::text,
       COALESCE(aliases.aliases, '[]'::jsonb)::text,
       COALESCE(relationships.relationships, '[]'::jsonb)::text
FROM base b
LEFT JOIN LATERAL (
	SELECT jsonb_agg(jsonb_build_object(
		'ServiceName', er.service_name,
		'Version', ert.api_version,
		'Name', er.collection_name || '/' || er.resource_id,
		'ExtensionResourceUID', er.uid::text,
		'CreatedAt', to_char(er.created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"'),
		'UpdatedAt', to_char(er.updated_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
	) ORDER BY er.service_name) AS representations
	FROM extension_resources er
	JOIN extension_resource_types ert ON ert.service_name = er.service_name AND ert.type_name = er.type_name
	WHERE er.collection_name = b.collection_name AND er.resource_id = b.resource_id
) reps ON true
LEFT JOIN LATERAL (
	SELECT jsonb_agg(jsonb_build_object(
		'Namespace', namespace,
		'Key', key,
		'Value', value,
		'CreatedAt', to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
	) ORDER BY namespace, key) AS aliases
	FROM resource_alias_claims
	WHERE platform_collection_name = b.collection_name AND platform_resource_id = b.resource_id
) aliases ON true
LEFT JOIN LATERAL (
	SELECT jsonb_agg(jsonb_build_object(
		'SourceName', source_collection_name || '/' || source_resource_id,
		'Type', type,
		'TargetName', target_collection_name || '/' || target_resource_id,
		'SourceService', source_service,
		'CreatedAt', to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')
	) ORDER BY type, target_collection_name, target_resource_id) AS relationships
	FROM resource_relationships
	WHERE source_collection_name = b.collection_name AND source_resource_id = b.resource_id
) relationships ON true
`

const platformResourceByNameQuery = `
WITH input(collection_name, resource_id) AS (VALUES ($1::text, $2::text)),
physical AS (
	SELECT pr.collection_name, pr.resource_id, pr.labels, pr.created_at, pr.updated_at
	FROM platform_resources pr
	JOIN input i ON i.collection_name = pr.collection_name AND i.resource_id = pr.resource_id
),
virtual AS (
	SELECT i.collection_name, i.resource_id, '{}'::jsonb AS labels, MIN(er.created_at) AS created_at, MAX(er.updated_at) AS updated_at
	FROM input i
	JOIN extension_resources er ON er.collection_name = i.collection_name AND er.resource_id = i.resource_id
	GROUP BY i.collection_name, i.resource_id
),
base AS (
	SELECT * FROM physical
	UNION ALL
	SELECT * FROM virtual WHERE NOT EXISTS (SELECT 1 FROM physical)
)
` + platformResourceAggregateSelectPostgres

const platformResourcesByCollectionQuery = `
WITH physical AS (
	SELECT collection_name, resource_id, labels, created_at, updated_at
	FROM platform_resources
	WHERE collection_name = $1
),
virtual AS (
	SELECT collection_name, resource_id, '{}'::jsonb AS labels, MIN(created_at) AS created_at, MAX(updated_at) AS updated_at
	FROM extension_resources
	WHERE collection_name = $1
	GROUP BY collection_name, resource_id
),
base AS (
	SELECT * FROM physical
	UNION ALL
	SELECT v.* FROM virtual v
	WHERE NOT EXISTS (
		SELECT 1 FROM physical p
		WHERE p.collection_name = v.collection_name AND p.resource_id = v.resource_id
	)
)
` + platformResourceAggregateSelectPostgres + `
ORDER BY b.resource_id`

// ---------------------------------------------------------------------------
// Reconciliation helpers
// ---------------------------------------------------------------------------

// reconcileAliases reconciles resource_alias_claims against s.Aliases
// -- the PlatformResource aggregate's own complete, current view of
// the aliases it asserts directly (as opposed to ones merely
// contributed by some extension resource's inventory report; see the
// migration's resource_alias_claims doc comment for the platform_owned
// flag this distinction hinges on). Like [domain.InventoryReplacement.Aliases]'s
// contract for ReplaceInventory, s.Aliases is treated as the complete
// current set: an entry present marks (or creates) its claim
// platform_owned; a platform_owned claim absent from it gets
// unmarked, and deleted outright if no contributor is left holding it
// up either. This mirrors ReplaceInventory's own fold-in/retract
// split in spirit, just scoped to platform_owned instead of
// per-contributor rows, and run as a plain per-alias loop rather than
// a batch statement -- reconcileAliases only runs from Create/Update,
// which are already one-row-at-a-time by nature (a single aggregate),
// so there's no batch to fold into a single round trip the way
// ReplaceInventory's chunk of many reports has.
func (r *ResourceIdentityRepo) reconcileAliases(ctx context.Context, s domain.PlatformResourceSnapshot) error {
	collectionName := string(s.Name.Collection())
	resourceID := string(s.Name.ID())

	asserted := make(map[domain.Alias]bool, len(s.Aliases))
	for _, alias := range s.Aliases {
		a := domain.AliasFromSnapshot(domain.AliasSnapshot{
			Namespace: alias.Namespace,
			Key:       alias.Key,
			Value:     alias.Value,
		})
		asserted[a] = true

		var claimID int64
		var existingCollection, existingResourceID string
		var platformOwned bool
		err := r.DB.QueryRowContext(ctx,
			`SELECT id, platform_collection_name, platform_resource_id, platform_owned FROM resource_alias_claims
			 WHERE namespace = $1 AND key = $2 AND value = $3`,
			alias.Namespace, alias.Key, alias.Value,
		).Scan(&claimID, &existingCollection, &existingResourceID, &platformOwned)
		if err == nil {
			if existingCollection != collectionName || existingResourceID != resourceID {
				return fmt.Errorf("alias %s/%s/%s owned by %s/%s, not %s: %w",
					alias.Namespace, alias.Key, alias.Value,
					existingCollection, existingResourceID, s.Name, domain.ErrAlreadyExists)
			}
			if platformOwned {
				continue
			}
			// An extension resource already contributed this exact
			// claim; the platform now corroborates it too.
			if _, err := r.DB.ExecContext(ctx,
				`UPDATE resource_alias_claims SET platform_owned = true WHERE id = $1`, claimID,
			); err != nil {
				return fmt.Errorf("corroborate alias: %w", err)
			}
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("check existing alias: %w", err)
		}

		_, err = r.DB.ExecContext(ctx,
			`INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id, platform_owned, created_at)
			 VALUES ($1, $2, $3, $4, $5, true, $6)`,
			alias.Namespace, alias.Key, alias.Value,
			collectionName, resourceID, time.Now().UTC().Format(time.RFC3339),
		)
		if err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("alias %s/%s/%s: %w", alias.Namespace, alias.Key, alias.Value, domain.ErrAlreadyExists)
			}
			return fmt.Errorf("insert alias: %w", err)
		}
	}

	return r.retractAbsentPlatformOwnedAliases(ctx, collectionName, resourceID, asserted)
}

// retractAbsentPlatformOwnedAliases is reconcileAliases's other half:
// any claim this platform resource currently owns (platform_owned)
// but which is no longer in its asserted set is un-owned, or deleted
// outright if that leaves it with no contributors either -- the same
// "delete only once truly unreferenced" rule ReplaceInventory's own
// orphan cleanup (aliasRetractAbsentCTE) applies, just evaluated
// per-claim here rather than via a batch refcount. A claim that's
// merely un-owned (contributors remain) stays fully resolvable; the
// [domain.ErrAlreadyExists] path above never has to worry about it,
// since a claim with contributors but no platform_owned flag is
// exactly the "extension-contributed" case reconcileAliases already
// knows how to corroborate later if re-asserted.
func (r *ResourceIdentityRepo) retractAbsentPlatformOwnedAliases(ctx context.Context, collectionName, resourceID string, asserted map[domain.Alias]bool) error {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, namespace, key, value FROM resource_alias_claims
		 WHERE platform_collection_name = $1 AND platform_resource_id = $2 AND platform_owned`,
		collectionName, resourceID,
	)
	if err != nil {
		return fmt.Errorf("list platform-owned aliases: %w", err)
	}
	type ownedClaim struct {
		id                    int64
		namespace, key, value string
	}
	var owned []ownedClaim
	for rows.Next() {
		var oc ownedClaim
		if err := rows.Scan(&oc.id, &oc.namespace, &oc.key, &oc.value); err != nil {
			rows.Close()
			return fmt.Errorf("scan platform-owned alias: %w", err)
		}
		owned = append(owned, oc)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("list platform-owned aliases: %w", err)
	}
	rows.Close()

	for _, oc := range owned {
		a := domain.AliasFromSnapshot(domain.AliasSnapshot{
			Namespace: domain.AliasNamespace(oc.namespace),
			Key:       domain.AliasKey(oc.key),
			Value:     domain.AliasValue(oc.value),
		})
		if asserted[a] {
			continue
		}
		var contributorCount int
		if err := r.DB.QueryRowContext(ctx,
			`SELECT count(*) FROM resource_alias_contributions WHERE claim_id = $1`, oc.id,
		).Scan(&contributorCount); err != nil {
			return fmt.Errorf("count alias contributions: %w", err)
		}
		if contributorCount == 0 {
			if _, err := r.DB.ExecContext(ctx, `DELETE FROM resource_alias_claims WHERE id = $1`, oc.id); err != nil {
				return fmt.Errorf("delete orphaned alias claim: %w", err)
			}
			continue
		}
		if _, err := r.DB.ExecContext(ctx,
			`UPDATE resource_alias_claims SET platform_owned = false WHERE id = $1`, oc.id,
		); err != nil {
			return fmt.Errorf("un-own alias claim: %w", err)
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
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 ON CONFLICT(source_collection_name, source_resource_id, type, target_collection_name, target_resource_id) DO UPDATE SET
			   source_service = EXCLUDED.source_service`,
			sourceCollection, sourceResourceID, rel.Type, targetCollection, targetResourceID,
			rel.SourceService, rel.CreatedAt.UTC().Format(time.RFC3339),
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
// round trip against resource_alias_claims, whose rows already carry
// the owning resource's name directly -- no join needed, and no
// DISTINCT needed either: UNIQUE(namespace, key, value) (see the
// migration's doc comment) guarantees at most one row per requested
// alias regardless of how many contributors back it.
//
// This never consults extension_resources.reported_aliases -- an alias
// absent here simply isn't in the map ResolveAliasesBatch returns,
// whether it was never reported or is still pending reconciliation.
func (r *ResourceIdentityRepo) ResolveAliasesBatch(ctx context.Context, aliases []domain.Alias) (map[domain.Alias]domain.ResourceName, error) {
	if len(aliases) == 0 {
		return map[domain.Alias]domain.ResourceName{}, nil
	}

	namespaces := make([]string, len(aliases))
	keys := make([]string, len(aliases))
	values := make([]string, len(aliases))
	for i, a := range aliases {
		namespaces[i] = string(a.Namespace())
		keys[i] = string(a.Key())
		values[i] = string(a.Value())
	}

	rows, err := r.DB.QueryContext(ctx,
		`SELECT namespace, key, value, platform_collection_name, platform_resource_id
		 FROM resource_alias_claims
		 WHERE (namespace, key, value) IN (
			SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[])
		 )`,
		namespaces, keys, values)
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
		result[domain.AliasFromSnapshot(domain.AliasSnapshot{
			Namespace: domain.AliasNamespace(ns),
			Key:       domain.AliasKey(key),
			Value:     domain.AliasValue(value),
		})] =
			domain.ResourceName(collectionName + "/" + resourceID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("resolve aliases batch: %w", err)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Scan helpers
// ---------------------------------------------------------------------------

func scanPlatformResourceAggregate(s scanner) (*domain.PlatformResource, error) {
	var collectionName, resourceID, labelsJSON, createdAtStr, updatedAtStr string
	var representationsJSON, aliasesJSON, relationshipsJSON string

	if err := s.Scan(
		&collectionName, &resourceID, &labelsJSON, &createdAtStr, &updatedAtStr,
		&representationsJSON, &aliasesJSON, &relationshipsJSON,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w", domain.ErrNotFound)
		}
		return nil, fmt.Errorf("scan platform resource: %w", err)
	}

	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		return nil, fmt.Errorf("unmarshal labels: %w", err)
	}

	createdAt, err := time.Parse(time.RFC3339Nano, createdAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse created_at: %w", err)
	}
	updatedAt, err := time.Parse(time.RFC3339Nano, updatedAtStr)
	if err != nil {
		return nil, fmt.Errorf("parse updated_at: %w", err)
	}

	snap := domain.PlatformResourceSnapshot{
		Name:      domain.ResourceName(collectionName + "/" + resourceID),
		Labels:    labels,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}
	if err := json.Unmarshal([]byte(representationsJSON), &snap.Representations); err != nil {
		return nil, fmt.Errorf("unmarshal representations: %w", err)
	}
	if err := json.Unmarshal([]byte(aliasesJSON), &snap.Aliases); err != nil {
		return nil, fmt.Errorf("unmarshal aliases: %w", err)
	}
	if err := json.Unmarshal([]byte(relationshipsJSON), &snap.Relationships); err != nil {
		return nil, fmt.Errorf("unmarshal relationships: %w", err)
	}
	return domain.PlatformResourceFromSnapshot(snap), nil
}
