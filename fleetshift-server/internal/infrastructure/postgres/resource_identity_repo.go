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
	collectionName := string(name.Collection())
	resourceID := string(name.ID())
	row := r.DB.QueryRowContext(ctx,
		`SELECT collection_name, resource_id, labels, created_at, updated_at
		 FROM platform_resources WHERE collection_name = $1 AND resource_id = $2`,
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
// resource representations but has never needed its own labels or
// relationships (see repository.go's virtual-resource doc).
// Relationships can never exist without a physical row (they're FK'd
// to platform_resources), so representations are the only signal
// checked here. Returns [domain.ErrNotFound] if the name has no
// representations either -- i.e. it truly doesn't exist.
//
// Aliases are the one exception: resource_alias_claims has no FK to
// platform_resources (see the migration's doc comment), so a virtual
// name absolutely can have aliases -- any extension resource's own
// contributed alias implies one. That's not a gap here, though:
// reconcileAliases only ever creates a platform_owned claim as part
// of Create/Update, both of which insert a physical platform_resources
// row first, so a purely platform-owned claim (zero contributors)
// can only exist for a name that already has one. A contributed-only
// claim, meanwhile, always has at least one extension resource behind
// it -- the very contributor asserting it -- which the
// extension_resources check below already catches. So checking for
// representations here remains sufficient without a separate alias
// check.
func (r *ResourceIdentityRepo) virtualByName(ctx context.Context, name domain.ResourceName) (*domain.PlatformResource, error) {
	collectionName := string(name.Collection())
	resourceID := string(name.ID())

	var minCreated, maxUpdated sql.NullString
	err := r.DB.QueryRowContext(ctx,
		`SELECT MIN(created_at), MAX(updated_at) FROM extension_resources
		 WHERE collection_name = $1 AND resource_id = $2`,
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
		`SELECT collection_name, resource_id, labels, created_at, updated_at
		 FROM platform_resources WHERE collection_name = $1 ORDER BY resource_id`,
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
		 FROM extension_resources WHERE collection_name = $1
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

// ResolveAlias reads resource_alias_claims by (namespace, key, value)
// alone -- its own UNIQUE(namespace, key, value) constraint (see the
// migration's doc comment) guarantees at most one row can ever match,
// regardless of how many contributors' resource_alias_contributions
// rows point at it, so there's no contributor-side ambiguity to
// resolve here at all.
//
// This never reads extension_resources.reported_aliases: those are
// reporter assertions pending future asynchronous reconciliation, not
// accepted platform identity (see [domain.InventoryReplacement.Aliases]'s
// doc), so a newly reported alias that hasn't yet gone through that
// reconciliation resolves as [domain.ErrNotFound] here, same as if it
// had never been reported at all.
func (r *ResourceIdentityRepo) ResolveAlias(ctx context.Context, alias domain.Alias) (domain.ResourceName, error) {
	var collectionName, resourceID string
	err := r.DB.QueryRowContext(ctx,
		`SELECT platform_collection_name, platform_resource_id FROM resource_alias_claims
		 WHERE namespace = $1 AND key = $2 AND value = $3`,
		alias.Namespace, alias.Key, alias.Value,
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
//
// This is unconditional on extension_resources.reported_aliases: an
// extension resource with a non-empty pending alias set still
// represents the platform resource of its own declarative name, the
// same as one reporting none. Nothing in this branch's read model
// gates representation on reported_aliases being empty -- there is no
// explicit accepted-identity projection yet to trust instead (see
// [domain.InventoryReplacement.Aliases]'s doc), so "derive from
// extension_resources by name alone" already *is* this branch's
// no-alias-implicit-acceptance rule in its simplest form: with zero
// explicit-identity rows to ever contradict it, every representation
// falls through to the implicit-accepted case.

func (r *ResourceIdentityRepo) GetRepresentation(ctx context.Context, name domain.FullResourceName) (domain.ResourceRepresentation, error) {
	service := name.ServiceName()
	relative := name.ResourceName()
	collectionName := string(relative.Collection())
	resourceID := string(relative.ID())
	row := r.DB.QueryRowContext(ctx, representationDerivationQuery+`
		WHERE er.service_name = $1 AND er.collection_name = $2 AND er.resource_id = $3`,
		service, collectionName, resourceID,
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
		a := domain.Alias{Namespace: alias.Namespace, Key: alias.Key, Value: alias.Value}
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
		a := domain.Alias{Namespace: domain.AliasNamespace(oc.namespace), Key: domain.AliasKey(oc.key), Value: domain.AliasValue(oc.value)}
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
// Like [ResourceIdentityRepo.ResolveAlias], this never consults
// extension_resources.reported_aliases -- an alias absent here simply
// isn't in the map ResolveAliasesBatch returns, whether it was never
// reported or is still pending reconciliation.
func (r *ResourceIdentityRepo) ResolveAliasesBatch(ctx context.Context, aliases []domain.Alias) (map[domain.Alias]domain.ResourceName, error) {
	if len(aliases) == 0 {
		return map[domain.Alias]domain.ResourceName{}, nil
	}

	namespaces := make([]string, len(aliases))
	keys := make([]string, len(aliases))
	values := make([]string, len(aliases))
	for i, a := range aliases {
		namespaces[i] = string(a.Namespace)
		keys[i] = string(a.Key)
		values[i] = string(a.Value)
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
		result[domain.Alias{Namespace: domain.AliasNamespace(ns), Key: domain.AliasKey(key), Value: domain.AliasValue(value)}] =
			domain.ResourceName(collectionName + "/" + resourceID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("resolve aliases batch: %w", err)
	}
	return result, nil
}

// ---------------------------------------------------------------------------
// Load helpers
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
		WHERE er.collection_name = $1 AND er.resource_id = $2
		ORDER BY er.service_name`,
		string(name.Collection()), string(name.ID()),
	)
	if err != nil {
		return nil, fmt.Errorf("load representations: %w", err)
	}
	reps, err := collectRows(rows, scanRepresentation)
	if err != nil {
		return nil, err
	}
	snaps := make([]domain.ResourceRepresentationSnapshot, len(reps))
	for i, rep := range reps {
		snaps[i] = rep.Snapshot()
	}
	return snaps, nil
}

// loadAliases returns one snapshot per resource_alias_claims row
// claimed for name, regardless of how many resource_alias_contributions
// rows back it -- UNIQUE(namespace, key, platform_collection_name,
// platform_resource_id) (see the migration's doc comment) guarantees
// there's exactly one claim row per (namespace, key) for a given
// name, so no collapsing is needed here the way ResolveAliasesBatch's
// old DISTINCT once did.
func (r *ResourceIdentityRepo) loadAliases(ctx context.Context, name domain.ResourceName) ([]domain.ResourceAliasSnapshot, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT namespace, key, value FROM resource_alias_claims
		 WHERE platform_collection_name = $1 AND platform_resource_id = $2 ORDER BY namespace, key`,
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
		 WHERE source_collection_name = $1 AND source_resource_id = $2 ORDER BY type, target_collection_name, target_resource_id`,
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
