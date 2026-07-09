package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

var _ domain.ExtensionResourceRepository = (*ExtensionResourceRepo)(nil)

// ExtensionResourceRepo implements [domain.ExtensionResourceRepository] for SQLite.
type ExtensionResourceRepo struct {
	DB interface {
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	}
}

// ---------------------------------------------------------------------------
// Type CRUD
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) CreateType(ctx context.Context, def domain.ExtensionResourceType) error {
	snap := def.Snapshot()
	var mgmtJSON sql.NullString
	if snap.Management != nil {
		mt, _ := domain.NewManagementType(snap.Management.Relation, snap.Management.Signature)
		b, err := json.Marshal(mt)
		if err != nil {
			return fmt.Errorf("marshal management: %w", err)
		}
		mgmtJSON = sql.NullString{String: string(b), Valid: true}
	}

	var invJSON sql.NullString
	if snap.Inventory != nil {
		invJSON = sql.NullString{String: "{}", Valid: true}
	}

	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_types (service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		string(snap.ResourceType.ServiceName()), snap.ResourceType.TypeName(),
		string(snap.APIVersion),
		string(snap.CollectionID),
		mgmtJSON,
		invJSON,
		snap.CreatedAt.UTC().Format(time.RFC3339Nano),
		snap.UpdatedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: extension resource type %q", domain.ErrAlreadyExists, snap.ResourceType)
		}
		return err
	}
	return nil
}

func (r *ExtensionResourceRepo) UpdateType(ctx context.Context, def domain.ExtensionResourceType) error {
	snap := def.Snapshot()

	var mgmtJSON sql.NullString
	if snap.Management != nil {
		mt, _ := domain.NewManagementType(snap.Management.Relation, snap.Management.Signature)
		b, err := json.Marshal(mt)
		if err != nil {
			return fmt.Errorf("marshal management: %w", err)
		}
		mgmtJSON = sql.NullString{String: string(b), Valid: true}
	}

	var invJSON sql.NullString
	if snap.Inventory != nil {
		invJSON = sql.NullString{String: "{}", Valid: true}
	}

	res, err := r.DB.ExecContext(ctx,
		`UPDATE extension_resource_types
		 SET management = ?, inventory = ?, updated_at = ?
		 WHERE service_name = ? AND type_name = ?`,
		mgmtJSON,
		invJSON,
		snap.UpdatedAt.UTC().Format(time.RFC3339Nano),
		string(snap.ResourceType.ServiceName()), snap.ResourceType.TypeName())
	if err != nil {
		return fmt.Errorf("update extension resource type: %w", err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: extension resource type %q", domain.ErrNotFound, snap.ResourceType)
	}
	return nil
}

func (r *ExtensionResourceRepo) GetType(ctx context.Context, rt domain.ResourceType) (domain.ExtensionResourceType, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at
		 FROM extension_resource_types WHERE service_name = ? AND type_name = ?`,
		string(rt.ServiceName()), rt.TypeName())
	return r.scanType(row)
}

func (r *ExtensionResourceRepo) ListTypes(ctx context.Context) ([]domain.ExtensionResourceType, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT service_name, type_name, api_version, collection_id, management, inventory, created_at, updated_at
		 FROM extension_resource_types ORDER BY service_name, type_name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var defs []domain.ExtensionResourceType
	for rows.Next() {
		def, err := r.scanType(rows)
		if err != nil {
			return nil, err
		}
		defs = append(defs, def)
	}
	return defs, rows.Err()
}

func (r *ExtensionResourceRepo) DeleteType(ctx context.Context, rt domain.ResourceType) error {
	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM extension_resource_types WHERE service_name = ? AND type_name = ?`,
		string(rt.ServiceName()), rt.TypeName())
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: extension resource type %q", domain.ErrNotFound, rt)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Instance aggregate
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) Create(ctx context.Context, er *domain.ExtensionResource) error {
	s := er.Snapshot()

	labelsJSON, err := json.Marshal(nonNilLabels(s.Labels))
	if err != nil {
		return fmt.Errorf("marshal labels: %w", err)
	}

	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		s.UID.String(), string(s.ResourceType.ServiceName()), s.ResourceType.TypeName(),
		string(s.Name.Collection()), string(s.Name.ID()),
		string(labelsJSON),
		s.CreatedAt.UTC().Format(time.RFC3339Nano),
		s.UpdatedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("%w: extension resource %s/%s", domain.ErrAlreadyExists, s.ResourceType.ServiceName(), s.Name)
		}
		return err
	}

	// Flush pending intents keyed by extension resource UID.
	for _, intent := range s.PendingIntents {
		if _, err := r.DB.ExecContext(ctx,
			`INSERT INTO resource_intents (extension_resource_uid, version, spec, created_at)
			 VALUES (?, ?, ?, ?)`,
			intent.ExtensionResourceUID.String(), intent.Version, string(intent.Spec),
			intent.CreatedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			if isUniqueViolation(err) {
				return fmt.Errorf("%w: intent %s v%d", domain.ErrAlreadyExists, intent.ExtensionResourceUID, intent.Version)
			}
			return err
		}
	}

	// Insert managed state row if present.
	if s.Managed != nil {
		_, err = r.DB.ExecContext(ctx,
			`INSERT INTO extension_resource_managed (extension_resource_uid, current_version, fulfillment_id)
			 VALUES (?, ?, ?)`,
			s.UID.String(), int64(s.Managed.CurrentVersion), string(s.Managed.FulfillmentID))
		if err != nil {
			return fmt.Errorf("insert managed state: %w", err)
		}
	}

	// Insert inventory state row if present.
	if s.Inventory != nil {
		if err := r.insertInventory(ctx, s.UID, s.Inventory); err != nil {
			return fmt.Errorf("insert inventory state: %w", err)
		}
	}

	return nil
}

// erInstanceQuerySQLite is the shared SELECT + FROM + JOINs for
// instance aggregate reads. Callers append a WHERE clause.
//
// inv.labels/inv.conditions are read straight off
// extension_resource_inventory's own JSON columns (see that table's
// migration doc comment for why they're JSON text rather than
// normalized out into their own tables, mirroring the Postgres
// sibling's JSONB choice one-for-one). er.reported_aliases is this
// extension resource's own pending, unreconciled alias payload -- see
// [domain.InventoryReplacement.Aliases]'s doc.
var erInstanceQuerySQLite = `SELECT er.uid, er.service_name, er.type_name, er.collection_name, er.resource_id, er.labels, er.reported_aliases, er.created_at, er.updated_at,
	m.current_version, m.fulfillment_id,
	inv.labels, inv.observation, inv.observed_at, inv.updated_at, inv.conditions
FROM extension_resources er
LEFT JOIN extension_resource_managed m ON m.extension_resource_uid = er.uid
LEFT JOIN extension_resource_inventory inv ON inv.extension_resource_uid = er.uid
`

func (r *ExtensionResourceRepo) Get(ctx context.Context, name domain.FullResourceName) (*domain.ExtensionResource, error) {
	relative := name.ResourceName()
	row := r.DB.QueryRowContext(ctx,
		erInstanceQuerySQLite+`WHERE er.service_name = ? AND er.collection_name = ? AND er.resource_id = ?`,
		string(name.ServiceName()), string(relative.Collection()), string(relative.ID()))
	return r.scanInstance(row)
}

func (r *ExtensionResourceRepo) GetByUID(ctx context.Context, uid domain.ExtensionResourceUID) (*domain.ExtensionResource, error) {
	row := r.DB.QueryRowContext(ctx,
		erInstanceQuerySQLite+`WHERE er.uid = ?`, uid.String())
	return r.scanInstance(row)
}

func (r *ExtensionResourceRepo) ListByResourceType(ctx context.Context, rt domain.ResourceType) ([]*domain.ExtensionResource, error) {
	rows, err := r.DB.QueryContext(ctx,
		erInstanceQuerySQLite+`WHERE er.service_name = ? AND er.type_name = ? ORDER BY er.collection_name, er.resource_id`,
		string(rt.ServiceName()), rt.TypeName())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var results []*domain.ExtensionResource
	for rows.Next() {
		inst, err := r.scanInstance(rows)
		if err != nil {
			return nil, err
		}
		results = append(results, inst)
	}
	return results, rows.Err()
}

// Delete implements [domain.ExtensionResourceRepository.Delete].
// resource_alias_contributions cascades away with the extension
// resource on ON DELETE CASCADE, but resource_alias_claims has no FK
// to it at all (see the migration's doc comment), so any claim this
// leaves with no contributors -- and not platform_owned -- needs
// explicit cleanup, via [ExtensionResourceRepo.deleteOrphanedClaims].
// The claim ids to check are read *before* the delete (SQLite's
// cascade doesn't hand them back the way Postgres's DELETE ...
// RETURNING does). Note that inventory reporting
// (ReplaceInventory/ApplyInventoryDeltas) no longer populates
// resource_alias_claims/resource_alias_contributions at all (see
// those tables' own doc comments), so in practice this cleanup only
// ever has work to do for platform-owned claims added via
// [domain.ResourceIdentityRepository]'s AddAlias path -- kept anyway
// since that path remains reachable and this repository has no way
// to know which mechanism produced a given claim.
func (r *ExtensionResourceRepo) Delete(ctx context.Context, name domain.FullResourceName) error {
	relative := name.ResourceName()

	rows, err := r.DB.QueryContext(ctx,
		`SELECT c.claim_id FROM resource_alias_contributions c
		 JOIN extension_resources er ON er.uid = c.source_extension_resource_uid
		 WHERE er.service_name = ? AND er.collection_name = ? AND er.resource_id = ?`,
		string(name.ServiceName()), string(relative.Collection()), string(relative.ID()))
	if err != nil {
		return fmt.Errorf("find alias claims for delete: %w", err)
	}
	var claimIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan alias claim id for delete: %w", err)
		}
		claimIDs = append(claimIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("find alias claims for delete: %w", err)
	}
	rows.Close()

	res, err := r.DB.ExecContext(ctx,
		`DELETE FROM extension_resources WHERE service_name = ? AND collection_name = ? AND resource_id = ?`,
		string(name.ServiceName()), string(relative.Collection()), string(relative.ID()))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: extension resource %s", domain.ErrNotFound, name)
	}

	if err := r.deleteOrphanedClaims(ctx, claimIDs); err != nil {
		return fmt.Errorf("clean up orphaned alias claims: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Views
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) GetView(ctx context.Context, name domain.FullResourceName) (domain.ExtensionResourceView, error) {
	relative := name.ResourceName()
	q := erViewQuerySQLite + `
	WHERE er.service_name = ? AND er.collection_name = ? AND er.resource_id = ?`
	row := r.DB.QueryRowContext(ctx, q, string(name.ServiceName()), string(relative.Collection()), string(relative.ID()))
	return r.scanView(row)
}

func (r *ExtensionResourceRepo) ListViewsByType(ctx context.Context, rt domain.ResourceType) ([]domain.ExtensionResourceView, error) {
	q := erViewQuerySQLite + `
	WHERE er.service_name = ? AND er.type_name = ? ORDER BY er.collection_name, er.resource_id`
	rows, err := r.DB.QueryContext(ctx, q, string(rt.ServiceName()), rt.TypeName())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var views []domain.ExtensionResourceView
	for rows.Next() {
		v, err := r.scanView(rows)
		if err != nil {
			return nil, err
		}
		views = append(views, v)
	}
	return views, rows.Err()
}

var erViewQuerySQLite = `SELECT
	er.uid, er.service_name, er.type_name, er.collection_name, er.resource_id, er.labels, er.reported_aliases, er.created_at, er.updated_at,
	m.current_version, m.fulfillment_id,
	ri.spec, ri.created_at,
	` + fulfillmentColumnsJoined("f") + `,
	inv.labels, inv.observation, inv.observed_at, inv.updated_at, inv.conditions
FROM extension_resources er
LEFT JOIN extension_resource_managed m ON m.extension_resource_uid = er.uid
LEFT JOIN resource_intents ri
  ON ri.extension_resource_uid = er.uid AND ri.version = m.current_version
LEFT JOIN fulfillments f ON f.id = m.fulfillment_id
` + strategyJoins("f") + `
LEFT JOIN extension_resource_inventory inv ON inv.extension_resource_uid = er.uid
`

// ---------------------------------------------------------------------------
// Natural-key resolution (used by ReplaceInventory/ApplyInventoryDeltas)
// ---------------------------------------------------------------------------

// resolveOrCreateExtensionResources resolves every candidate's
// extension_resources row by natural key (service_name,
// collection_name, resource_id), creating it with candidateUIDs[i] --
// and reportedAliasPayloads[i], the pending alias payload to store on
// a brand-new row (see [domain.InventoryReplacement.Aliases]'s doc) --
// if it doesn't exist yet. SQLite has no writable CTEs, so this is
// two statements rather than the Postgres sibling's one CTE-chained
// lookup: a blind multi-row INSERT ... ON CONFLICT DO NOTHING, then a
// single tuple-IN SELECT that resolves every input row's UID and
// current reported_aliases, whether just inserted or already there.
//
// Because the SELECT runs *after* the INSERT as its own statement
// (unlike Postgres, where both live in one statement and a CTE can
// never see another CTE's own writes), storedAliasPayloads[i] for a
// row this same call just created already reflects the value that
// row's own INSERT wrote. Callers that compare it against their own
// freshly reported payload therefore see "unchanged" for a brand-new
// resource whose creating report's aliases were written directly in
// the INSERT, with no separate write-back step needed.
//
// ApplyInventoryDeltas has no aliases to seed a new row with (see
// [domain.InventoryDelta]'s doc: it never resolves a brand-new alias
// payload of its own), so it passes the deterministic empty-set
// payload for every row -- exactly what the column default would
// produce anyway, made explicit here for a single shared insert
// statement.
//
// Both returned slices are in the same order as the input slices.
func (r *ExtensionResourceRepo) resolveOrCreateExtensionResources(
	ctx context.Context,
	resourceTypes []domain.ResourceType, names []domain.ResourceName, candidateUIDs []domain.ExtensionResourceUID, receivedAts []time.Time,
	reportedAliasPayloads []string,
) ([]domain.ExtensionResourceUID, []string, error) {
	n := len(resourceTypes)
	if n == 0 {
		return nil, nil, nil
	}

	insertPlaceholders := make([]string, n)
	insertArgs := make([]any, 0, n*8)
	selectPlaceholders := make([]string, n)
	selectArgs := make([]any, 0, n*3)
	for i := range resourceTypes {
		insertPlaceholders[i] = "(?, ?, ?, ?, ?, '{}', ?, ?, ?)"
		receivedAt := receivedAts[i].UTC().Format(time.RFC3339Nano)
		insertArgs = append(insertArgs,
			candidateUIDs[i].String(), string(resourceTypes[i].ServiceName()), resourceTypes[i].TypeName(),
			string(names[i].Collection()), string(names[i].ID()),
			reportedAliasPayloads[i], receivedAt, receivedAt)

		selectPlaceholders[i] = "(?, ?, ?)"
		selectArgs = append(selectArgs,
			string(resourceTypes[i].ServiceName()), string(names[i].Collection()), string(names[i].ID()))
	}

	if _, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, reported_aliases, created_at, updated_at)
			VALUES %s
			ON CONFLICT (service_name, collection_name, resource_id) DO NOTHING`, strings.Join(insertPlaceholders, ", ")),
		insertArgs...,
	); err != nil {
		return nil, nil, fmt.Errorf("resolve-or-create extension resources (insert): %w", err)
	}

	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT service_name, collection_name, resource_id, uid, reported_aliases FROM extension_resources
			WHERE (service_name, collection_name, resource_id) IN (%s)`, strings.Join(selectPlaceholders, ", ")),
		selectArgs...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve-or-create extension resources (resolve): %w", err)
	}
	defer rows.Close()

	type resolvedRow struct {
		uid          domain.ExtensionResourceUID
		aliasPayload string
	}
	resolved := make(map[domain.FullResourceName]resolvedRow, n)
	for rows.Next() {
		var serviceName, collectionName, resourceID, uidStr string
		var aliasPayload string
		if err := rows.Scan(&serviceName, &collectionName, &resourceID, &uidStr, &aliasPayload); err != nil {
			return nil, nil, fmt.Errorf("scan resolved extension resource: %w", err)
		}
		uid, err := domain.ParseExtensionResourceUID(uidStr)
		if err != nil {
			return nil, nil, fmt.Errorf("parse resolved uid: %w", err)
		}
		fullName := domain.NewFullResourceName(domain.ServiceName(serviceName), domain.ResourceName(collectionName+"/"+resourceID))
		resolved[fullName] = resolvedRow{uid: uid, aliasPayload: aliasPayload}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("resolve-or-create extension resources (resolve): %w", err)
	}

	uids := make([]domain.ExtensionResourceUID, n)
	storedAliasPayloads := make([]string, n)
	for i := range resourceTypes {
		fullName := domain.NewFullResourceName(resourceTypes[i].ServiceName(), names[i])
		row, ok := resolved[fullName]
		if !ok {
			return nil, nil, fmt.Errorf("resolve-or-create extension resources: no result for %s", fullName)
		}
		uids[i] = row.uid
		storedAliasPayloads[i] = row.aliasPayload
	}
	return uids, storedAliasPayloads, nil
}

// ---------------------------------------------------------------------------
// Intents
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) GetIntent(ctx context.Context, uid domain.ExtensionResourceUID, version domain.IntentVersion) (domain.ResourceIntent, error) {
	row := r.DB.QueryRowContext(ctx,
		`SELECT extension_resource_uid, version, spec, created_at
		 FROM resource_intents WHERE extension_resource_uid = ? AND version = ?`,
		uid.String(), version)
	var ri domain.ResourceIntent
	var uidStr, specStr, createdAt string
	if err := row.Scan(&uidStr, &ri.Version, &specStr, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ResourceIntent{}, fmt.Errorf("%w: intent %s v%d", domain.ErrNotFound, uid, version)
		}
		return domain.ResourceIntent{}, err
	}
	parsedUID, err := domain.ParseExtensionResourceUID(uidStr)
	if err != nil {
		return domain.ResourceIntent{}, err
	}
	ri.ExtensionResourceUID = parsedUID
	ri.Spec = json.RawMessage(specStr)
	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		ri.CreatedAt = t
	}
	return ri, nil
}

// ---------------------------------------------------------------------------
// Scan helpers
// ---------------------------------------------------------------------------

func (r *ExtensionResourceRepo) scanType(s interface{ Scan(...any) error }) (domain.ExtensionResourceType, error) {
	var serviceName, typeName, apiVersion, collectionID, createdAt, updatedAt string
	var mgmtJSON, invJSON sql.NullString
	if err := s.Scan(&serviceName, &typeName, &apiVersion, &collectionID, &mgmtJSON, &invJSON, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExtensionResourceType{}, domain.ErrNotFound
		}
		return domain.ExtensionResourceType{}, err
	}

	snap := domain.ExtensionResourceTypeSnapshot{
		ResourceType: domain.ResourceType(serviceName + "/" + typeName),
		APIVersion:   domain.APIVersion(apiVersion),
		CollectionID: domain.CollectionID(collectionID),
	}
	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		snap.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, updatedAt); err == nil {
		snap.UpdatedAt = t
	}
	if mgmtJSON.Valid {
		var mt domain.ManagementType
		if err := json.Unmarshal([]byte(mgmtJSON.String), &mt); err != nil {
			return domain.ExtensionResourceType{}, fmt.Errorf("unmarshal management: %w", err)
		}
		snap.Management = &domain.ManagementTypeSnapshot{
			Relation:  mt.Relation(),
			Signature: mt.Signature(),
		}
	}
	if invJSON.Valid {
		snap.Inventory = &domain.InventoryTypeSnapshot{}
	}
	return domain.ExtensionResourceTypeFromSnapshot(snap), nil
}

func (r *ExtensionResourceRepo) scanInstance(s interface{ Scan(...any) error }) (*domain.ExtensionResource, error) {
	var uidStr, serviceName, typeName, collectionName, resourceID, labelsJSON, reportedAliasesJSON, createdAt, updatedAt string
	var mVersion sql.NullInt64
	var mFulfillmentID sql.NullString
	var invLabels, invObservation sql.NullString
	var invObservedAt, invUpdatedAt sql.NullString
	var invConditionsJSON sql.NullString

	if err := s.Scan(&uidStr, &serviceName, &typeName, &collectionName, &resourceID, &labelsJSON, &reportedAliasesJSON, &createdAt, &updatedAt,
		&mVersion, &mFulfillmentID,
		&invLabels, &invObservation, &invObservedAt, &invUpdatedAt,
		&invConditionsJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}

	uid, err := domain.ParseExtensionResourceUID(uidStr)
	if err != nil {
		return nil, err
	}

	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		return nil, fmt.Errorf("unmarshal labels: %w", err)
	}
	reportedAliases, err := unmarshalReportedAliasesPayload([]byte(reportedAliasesJSON))
	if err != nil {
		return nil, fmt.Errorf("unmarshal reported aliases: %w", err)
	}

	snap := domain.ExtensionResourceSnapshot{
		UID:             uid,
		ResourceType:    domain.ResourceType(serviceName + "/" + typeName),
		Name:            domain.ResourceName(collectionName + "/" + resourceID),
		Labels:          labels,
		ReportedAliases: reportedAliases,
	}
	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		snap.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, updatedAt); err == nil {
		snap.UpdatedAt = t
	}
	if mVersion.Valid {
		snap.Managed = &domain.ManagedStateSnapshot{
			CurrentVersion: domain.IntentVersion(mVersion.Int64),
			FulfillmentID:  domain.FulfillmentID(mFulfillmentID.String),
		}
	}
	if invObservedAt.Valid {
		invSnap := domain.InventoryResourceSnapshot{
			Labels: map[string]string{},
		}
		if invLabels.Valid {
			json.Unmarshal([]byte(invLabels.String), &invSnap.Labels)
		}
		if invObservation.Valid {
			invSnap.Observation = json.RawMessage(invObservation.String)
		}
		if t, err := time.Parse(time.RFC3339Nano, invObservedAt.String); err == nil {
			invSnap.ObservedAt = t
		}
		if invUpdatedAt.Valid {
			if t, err := time.Parse(time.RFC3339Nano, invUpdatedAt.String); err == nil {
				invSnap.UpdatedAt = t
			}
		}
		if invConditionsJSON.Valid {
			invSnap.Conditions, _ = unmarshalConditionSnapshots([]byte(invConditionsJSON.String))
		}
		snap.Inventory = &invSnap
	}
	return domain.ExtensionResourceFromSnapshot(snap), nil
}

func (r *ExtensionResourceRepo) scanView(s interface{ Scan(...any) error }) (domain.ExtensionResourceView, error) {
	var uidStr, serviceName, typeName, collectionName, resourceID, labelsJSON, reportedAliasesJSON, erCreatedAt, erUpdatedAt string
	var mVersion sql.NullInt64
	var mFulfillmentID sql.NullString
	var riSpec, riCreatedAt sql.NullString
	var fCols nullableFulfillmentScanColumns

	// Inventory columns (all nullable)
	var invLabels, invObservation sql.NullString
	var invObservedAt, invUpdatedAt sql.NullString
	var invConditionsJSON sql.NullString

	if err := s.Scan(append(append([]any{
		&uidStr, &serviceName, &typeName, &collectionName, &resourceID, &labelsJSON, &reportedAliasesJSON, &erCreatedAt, &erUpdatedAt,
		&mVersion, &mFulfillmentID,
		&riSpec, &riCreatedAt,
	}, fCols.dests()...),
		&invLabels, &invObservation, &invObservedAt, &invUpdatedAt,
		&invConditionsJSON,
	)...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExtensionResourceView{}, domain.ErrNotFound
		}
		return domain.ExtensionResourceView{}, fmt.Errorf("scan extension resource view: %w", err)
	}

	uid, err := domain.ParseExtensionResourceUID(uidStr)
	if err != nil {
		return domain.ExtensionResourceView{}, err
	}

	view, err := extensionResourceViewFromColumns(
		uid, serviceName, typeName, collectionName, resourceID,
		labelsJSON, reportedAliasesJSON,
		erCreatedAt, erUpdatedAt,
		mVersion, mFulfillmentID,
		riSpec, riCreatedAt,
		fCols,
		invLabels, invObservation, invObservedAt, invUpdatedAt,
		invConditionsJSON,
	)
	if err != nil {
		return domain.ExtensionResourceView{}, err
	}
	return view, nil
}

// extensionResourceViewFromColumns builds a [domain.ExtensionResourceView]
// from erViewQuerySQLite's already-scanned column values. Factored out of
// scanView so the query repository's extension-only projection
// (query_repo.go) can reuse the exact same construction logic against
// a row it scanned itself, without hydrating each result with a
// follow-up per-row GetView call.
func extensionResourceViewFromColumns(
	uid domain.ExtensionResourceUID,
	serviceName, typeName, collectionName, resourceID string,
	labelsJSON, reportedAliasesJSON string,
	erCreatedAt, erUpdatedAt string,
	mVersion sql.NullInt64,
	mFulfillmentID sql.NullString,
	riSpec, riCreatedAt sql.NullString,
	fCols nullableFulfillmentScanColumns,
	invLabels, invObservation sql.NullString,
	invObservedAt, invUpdatedAt sql.NullString,
	invConditionsJSON sql.NullString,
) (domain.ExtensionResourceView, error) {
	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("unmarshal labels: %w", err)
	}
	reportedAliases, err := unmarshalReportedAliasesPayload([]byte(reportedAliasesJSON))
	if err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("unmarshal reported aliases: %w", err)
	}

	snap := domain.ExtensionResourceSnapshot{
		UID:             uid,
		ResourceType:    domain.ResourceType(serviceName + "/" + typeName),
		Name:            domain.ResourceName(collectionName + "/" + resourceID),
		Labels:          labels,
		ReportedAliases: reportedAliases,
	}
	if t, err := time.Parse(time.RFC3339Nano, erCreatedAt); err == nil {
		snap.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, erUpdatedAt); err == nil {
		snap.UpdatedAt = t
	}
	if mVersion.Valid {
		snap.Managed = &domain.ManagedStateSnapshot{
			CurrentVersion: domain.IntentVersion(mVersion.Int64),
			FulfillmentID:  domain.FulfillmentID(mFulfillmentID.String),
		}
	}

	// Inventory: include in snapshot so ExtensionResourceFromSnapshot
	// hydrates Resource.Inventory().
	if invObservedAt.Valid {
		invSnap := domain.InventoryResourceSnapshot{
			Labels: map[string]string{},
		}
		if invLabels.Valid {
			json.Unmarshal([]byte(invLabels.String), &invSnap.Labels)
		}
		if invObservation.Valid {
			invSnap.Observation = json.RawMessage(invObservation.String)
		}
		if t, err := time.Parse(time.RFC3339Nano, invObservedAt.String); err == nil {
			invSnap.ObservedAt = t
		}
		if invUpdatedAt.Valid {
			if t, err := time.Parse(time.RFC3339Nano, invUpdatedAt.String); err == nil {
				invSnap.UpdatedAt = t
			}
		}
		if invConditionsJSON.Valid {
			invSnap.Conditions, _ = unmarshalConditionSnapshots([]byte(invConditionsJSON.String))
		}
		snap.Inventory = &invSnap
	}

	resource := domain.ExtensionResourceFromSnapshot(snap)

	var v domain.ExtensionResourceView
	v.Resource = *resource

	// Intent and fulfillment are only populated for managed resources.
	if riSpec.Valid {
		intent := &domain.ResourceIntent{
			ExtensionResourceUID: resource.UID(),
			Spec:                 json.RawMessage(riSpec.String),
		}
		if resource.Managed() != nil {
			intent.Version = resource.Managed().CurrentVersion()
		}
		if riCreatedAt.Valid {
			if t, err := time.Parse(time.RFC3339Nano, riCreatedAt.String); err == nil {
				intent.CreatedAt = t
			}
		}
		v.Intent = intent
	}

	if fCols.isPresent() {
		fs, err := fCols.snapshot()
		if err != nil {
			return domain.ExtensionResourceView{}, err
		}
		v.Fulfillment = domain.FulfillmentFromSnapshot(fs)
	}

	return v, nil
}

// ConditionJSON is the JSON shape of a single entry within
// extension_resource_inventory.conditions, which stores a *map* of
// these keyed by condition type rather than an array -- mirrors the
// Postgres sibling's identical type one-for-one, including field
// names, so the two backends' stored JSON shapes agree even though
// nothing forces that beyond convention.
type ConditionJSON struct {
	Status             domain.ConditionStatus `json:"status"`
	Reason             string                 `json:"reason"`
	Message            string                 `json:"message"`
	LastTransitionTime time.Time              `json:"lastTransitionTime"`
}

// conditionsToJSON marshals conds into the JSON object -- keyed by
// condition type -- that extension_resource_inventory.conditions
// stores. A nil or empty conds still marshals to `{}`, never `null`,
// since the column defaults to '{}'.
func conditionsToJSON(conds []domain.Condition) ([]byte, error) {
	byType := make(map[string]ConditionJSON, len(conds))
	for _, c := range conds {
		byType[string(c.Type())] = ConditionJSON{
			Status:             c.Status(),
			Reason:             c.Reason(),
			Message:            c.Message(),
			LastTransitionTime: c.LastTransitionTime(),
		}
	}
	return json.Marshal(byType)
}

// conditionSnapshotsToJSON is conditionsToJSON's
// [domain.ConditionSnapshot] counterpart, used by
// [ExtensionResourceRepo.insertInventory] which works with snapshot
// DTOs rather than the [domain.Condition] value objects
// ReplaceInventory/ApplyInventoryDeltas receive.
func conditionSnapshotsToJSON(conds []domain.ConditionSnapshot) ([]byte, error) {
	byType := make(map[string]ConditionJSON, len(conds))
	for _, c := range conds {
		byType[string(c.Type)] = ConditionJSON{
			Status:             c.Status,
			Reason:             c.Reason,
			Message:            c.Message,
			LastTransitionTime: c.LastTransitionTime,
		}
	}
	return json.Marshal(byType)
}

// unmarshalConditionSnapshots parses the JSON object produced by
// conditionsToJSON back into [domain.ConditionSnapshot]s, sorted by
// type for deterministic ordering (map iteration order is otherwise
// unspecified) -- matches the old normalized table's `ORDER BY type`.
func unmarshalConditionSnapshots(data []byte) ([]domain.ConditionSnapshot, error) {
	var byType map[string]ConditionJSON
	if err := json.Unmarshal(data, &byType); err != nil {
		return nil, err
	}
	snaps := make([]domain.ConditionSnapshot, 0, len(byType))
	for t, c := range byType {
		snaps = append(snaps, domain.ConditionSnapshot{
			Type:               domain.ConditionType(t),
			Status:             c.Status,
			Reason:             c.Reason,
			Message:            c.Message,
			LastTransitionTime: c.LastTransitionTime.UTC(),
		})
	}
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Type < snaps[j].Type })
	return snaps, nil
}

// nonNilLabels normalizes a nil label map to a non-nil empty one, so
// json.Marshal produces `{}` rather than `null` -- the labels columns
// this feeds default to '{}'.
func nonNilLabels(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

// ---------------------------------------------------------------------------
// Inventory methods
// ---------------------------------------------------------------------------
//
// ReplaceInventory and ApplyInventoryDeltas are each a fixed, small
// number of round trips for their *entire* input slice, not one round
// trip per item: resolveOrCreateExtensionResources resolves every
// replacement/delta's extension_resources row by natural key in two
// statements total, and batchUpsertInventoryRows below writes every
// resource's latest labels/observation/conditions in one multi-row
// upsert, regardless of batch size.
//
// Unlike the Postgres sibling, SQLite has no writable CTEs, so
// ApplyInventoryDeltas's ReplaceLabels/ReplaceConditions are whole-
// column assigns; UpsertLabels/DeleteLabels and
// UpsertConditions/DeleteConditions can't be expressed as a single
// INSERT ... SELECT the way Postgres's jsonb `-`/`||` operators allow;
// instead this reads every affected resource's current
// labels/conditions in one query, merges in Go when replace is absent,
// and writes the complete result back through the same
// batchUpsertInventoryRows primitive ReplaceInventory uses. This is
// still a fixed number of round trips per batch, not one per item.
//
// Neither method writes observation/condition history any more:
// extension_resource_inventory_observations/
// extension_resource_inventory_condition_events are populated by a
// future asynchronous writer, not this hot path -- see those tables'
// own migration doc comments.
//
// Aliases are handled with no synchronous cross-resource conflict
// detection at all -- see [domain.InventoryReplacement.Aliases]'s doc
// -- so there is nothing for either method to report beyond error.
// ReplaceInventory stores the reported set after [domain.AliasSet]
// canonicalization, skipping the write entirely when the canonical
// alias payload matches what's
// already stored. ApplyInventoryDeltas's UpsertAliases instead merges
// into the existing payload in Go using the domain's canonical
// [domain.AliasSet] representation -- a genuine read-modify-write that
// can't be avoided without knowing the *result* of the merge ahead of
// time, so it becomes its own small pass scoped to only the resources
// with alias work.

// normalizeObservation collapses the two "no real observation" input
// shapes -- a nil pointer and a non-nil pointer to the JSON literal
// null -- to a single nil result, so the rest of the repository only
// has to handle one "untouched" case. Per the observation contract
// (see [domain.InventoryReplacement.Observation]), there is no
// explicit "clear" operation; only "untouched" and "replace".
func normalizeObservation(obs *json.RawMessage) *json.RawMessage {
	if obs == nil {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(*obs), []byte("null")) {
		return nil
	}
	return obs
}

// reportedAliasObjectPayload encodes an extension resource's pending
// aliases in the same object payload shape Postgres uses: a JSON
// object keyed by the JSON-encoded [namespace, key] pair, with the
// alias value as the object value. Keeping both backends on the same
// storage shape lets the domain stay on snapshot/value-object
// boundaries rather than carrying backend-specific JSON concerns.
func reportedAliasObjectPayload(aliases domain.AliasSet) ([]byte, error) {
	payload := make(map[string]string, aliases.Len())
	for alias := range aliases.All() {
		key, err := reportedAliasObjectKey(alias.Namespace(), alias.Key())
		if err != nil {
			return nil, err
		}
		payload[key] = string(alias.Value())
	}
	return json.Marshal(payload)
}

func reportedAliasObjectKey(namespace domain.AliasNamespace, key domain.AliasKey) (string, error) {
	encoded, err := json.Marshal([2]string{string(namespace), string(key)})
	if err != nil {
		return "", fmt.Errorf("marshal alias object key: %w", err)
	}
	return string(encoded), nil
}

func unmarshalReportedAliasesPayload(payload []byte) (domain.AliasSetSnapshot, error) {
	payload = bytes.TrimSpace(payload)
	if len(payload) == 0 || bytes.Equal(payload, []byte("null")) {
		return domain.AliasSetSnapshot{}, nil
	}
	if payload[0] != '{' {
		return domain.AliasSetSnapshot{}, fmt.Errorf("reported aliases payload must be JSON object")
	}
	var encoded map[string]string
	if err := json.Unmarshal(payload, &encoded); err != nil {
		return nil, err
	}
	aliases := make(domain.AliasSetSnapshot, 0, len(encoded))
	for encodedKey, value := range encoded {
		var parts [2]string
		if err := json.Unmarshal([]byte(encodedKey), &parts); err != nil {
			return nil, fmt.Errorf("unmarshal alias object key %q: %w", encodedKey, err)
		}
		aliases = append(aliases, domain.AliasSnapshot{
			Namespace: domain.AliasNamespace(parts[0]),
			Key:       domain.AliasKey(parts[1]),
			Value:     domain.AliasValue(value),
		})
	}
	return aliases, nil
}

// inventoryRowInput is the flattened per-resource input row
// [ExtensionResourceRepo.batchUpsertInventoryRows] writes. observation
// == nil means "untouched" (see [normalizeObservation]); labels/
// conditions are always the complete latest JSON to store (a full
// replace for ReplaceInventory, the merged result for
// ApplyInventoryDeltas -- see this section's own doc comment).
type inventoryRowInput struct {
	uid                   domain.ExtensionResourceUID
	observation           *json.RawMessage
	labelsJSON            string
	conditionsJSON        string
	observedAt, updatedAt time.Time
}

// batchUpsertInventoryRows is the single low-level "write latest
// inventory rows" primitive shared by
// [ExtensionResourceRepo.ReplaceInventory] and
// [ExtensionResourceRepo.ApplyInventoryDeltas]. observation == nil
// for an item means "untouched": the ON CONFLICT clause COALESCEs the
// observation column so an untouched observation preserves whatever
// is already latest, entirely at the SQL level -- this is also what
// makes the statement double as ApplyInventoryDeltas's "ensure a
// latest row exists for every reported resource" step, since every
// item is always upserted regardless of whether its own observation
// is real.
func (r *ExtensionResourceRepo) batchUpsertInventoryRows(ctx context.Context, items []inventoryRowInput) error {
	if len(items) == 0 {
		return nil
	}
	placeholders := make([]string, len(items))
	args := make([]any, 0, len(items)*6)
	for i, it := range items {
		var obsArg any
		if it.observation != nil {
			obsArg = string(*it.observation)
		}
		placeholders[i] = "(?, ?, ?, ?, ?, ?)"
		args = append(args, it.uid.String(), obsArg, it.labelsJSON, it.conditionsJSON,
			it.observedAt.UTC().Format(time.RFC3339Nano), it.updatedAt.UTC().Format(time.RFC3339Nano))
	}
	_, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO extension_resource_inventory (extension_resource_uid, observation, labels, conditions, observed_at, updated_at)
			VALUES %s
			ON CONFLICT(extension_resource_uid) DO UPDATE SET
				observation = COALESCE(excluded.observation, extension_resource_inventory.observation),
				labels = excluded.labels,
				conditions = excluded.conditions,
				observed_at = excluded.observed_at,
				updated_at = excluded.updated_at`, strings.Join(placeholders, ", ")),
		args...)
	if err != nil {
		return fmt.Errorf("batch upsert inventory rows: %w", err)
	}
	return nil
}

// insertInventory writes a resource's initial inventory state as part
// of [ExtensionResourceRepo.Create]. There's nothing pre-existing to
// reconcile against (the resource itself was just created in the same
// call), so this is a plain INSERT rather than
// [ExtensionResourceRepo.batchUpsertInventoryRows]'s upsert.
func (r *ExtensionResourceRepo) insertInventory(ctx context.Context, uid domain.ExtensionResourceUID, inv *domain.InventoryResourceSnapshot) error {
	labelsJSON, err := json.Marshal(nonNilLabels(inv.Labels))
	if err != nil {
		return fmt.Errorf("marshal inventory labels: %w", err)
	}
	conditionsJSON, err := conditionSnapshotsToJSON(inv.Conditions)
	if err != nil {
		return fmt.Errorf("marshal inventory conditions: %w", err)
	}
	var obsArg any
	if inv.Observation != nil {
		obsArg = string(inv.Observation)
	}
	_, err = r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory (extension_resource_uid, observation, labels, conditions, observed_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		uid.String(), obsArg, string(labelsJSON), string(conditionsJSON),
		inv.ObservedAt.UTC().Format(time.RFC3339Nano), inv.UpdatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

// deleteOrphanedClaims checks each of claimIDs (deduplicated) for
// remaining resource_alias_contributions, deleting the claim itself
// if none remain and it isn't platform_owned. Shared by
// [ExtensionResourceRepo.Delete]; inventory reporting no longer
// produces contributions to orphan (see this file's own doc comment
// on the Inventory methods section), so in practice this only ever
// has work to do following a platform-owned claim's removal.
func (r *ExtensionResourceRepo) deleteOrphanedClaims(ctx context.Context, claimIDs []int64) error {
	seen := make(map[int64]bool, len(claimIDs))
	for _, id := range claimIDs {
		if seen[id] {
			continue
		}
		seen[id] = true

		var contributorCount int
		var platformOwned bool
		err := r.DB.QueryRowContext(ctx,
			`SELECT (SELECT count(*) FROM resource_alias_contributions WHERE claim_id = ?), platform_owned
			 FROM resource_alias_claims WHERE id = ?`, id, id,
		).Scan(&contributorCount, &platformOwned)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return fmt.Errorf("check orphaned alias claim: %w", err)
		}
		if contributorCount > 0 || platformOwned {
			continue
		}
		if _, err := r.DB.ExecContext(ctx, `DELETE FROM resource_alias_claims WHERE id = ?`, id); err != nil {
			return fmt.Errorf("delete orphaned alias claim: %w", err)
		}
	}
	return nil
}

// ReplaceInventory implements [domain.ExtensionResourceRepository.ReplaceInventory]
// as a fixed number of round trips for the whole batch: it resolves-
// or-creates every replacement's extension_resources row by natural
// key (resolveOrCreateExtensionResources, seeding a brand-new row's
// reported_aliases directly from that same replacement), writes every
// resource's complete latest labels/conditions/observation in one
// upsert, then writes only the pending alias payload for resources
// whose canonical payload has actually changed since the last
// successful write.
func (r *ExtensionResourceRepo) ReplaceInventory(ctx context.Context, replacements []domain.InventoryReplacement) error {
	if len(replacements) == 0 {
		return nil
	}

	n := len(replacements)
	resourceTypes := make([]domain.ResourceType, n)
	names := make([]domain.ResourceName, n)
	candidateUIDs := make([]domain.ExtensionResourceUID, n)
	receivedAts := make([]time.Time, n)
	reportedAliasPayloads := make([]string, n)
	for i, rep := range replacements {
		resourceTypes[i] = rep.ResourceType
		names[i] = rep.Name
		candidateUIDs[i] = rep.CandidateUID
		receivedAts[i] = rep.ReceivedAt
		aliasesJSON, err := reportedAliasObjectPayload(rep.Aliases)
		if err != nil {
			return fmt.Errorf("marshal reported aliases: %w", err)
		}
		reportedAliasPayloads[i] = string(aliasesJSON)
	}
	uids, storedAliasPayloads, err := r.resolveOrCreateExtensionResources(
		ctx, resourceTypes, names, candidateUIDs, receivedAts, reportedAliasPayloads)
	if err != nil {
		return fmt.Errorf("replace inventory: %w", err)
	}

	invItems := make([]inventoryRowInput, n)
	for i, rep := range replacements {
		labelsJSON, err := json.Marshal(nonNilLabels(rep.Labels))
		if err != nil {
			return fmt.Errorf("marshal labels: %w", err)
		}
		conditionsJSON, err := conditionsToJSON(rep.Conditions)
		if err != nil {
			return fmt.Errorf("marshal conditions: %w", err)
		}
		invItems[i] = inventoryRowInput{
			uid:            uids[i],
			observation:    normalizeObservation(rep.Observation),
			labelsJSON:     string(labelsJSON),
			conditionsJSON: string(conditionsJSON),
			observedAt:     rep.ObservedAt,
			updatedAt:      rep.ReceivedAt,
		}
	}
	if err := r.batchUpsertInventoryRows(ctx, invItems); err != nil {
		return fmt.Errorf("replace inventory: %w", err)
	}

	// needs_alias_payload_write, mirroring the Postgres sibling's CTE
	// of the same purpose: a resource whose canonical reported alias
	// payload differs from what's already stored. A row this same call
	// just created already has the right value stored by
	// resolveOrCreateExtensionResources's own INSERT (see that
	// method's doc comment), so it naturally compares equal here and
	// is skipped -- no separate exclusion needed.
	for i := range replacements {
		if reportedAliasPayloads[i] == storedAliasPayloads[i] {
			continue
		}
		if _, err := r.DB.ExecContext(ctx,
			`UPDATE extension_resources SET reported_aliases = ?, updated_at = ? WHERE uid = ?`,
			reportedAliasPayloads[i], receivedAts[i].UTC().Format(time.RFC3339Nano), uids[i].String(),
		); err != nil {
			return fmt.Errorf("replace inventory: write alias payload: %w", err)
		}
	}
	return nil
}

// ApplyInventoryDeltas implements [domain.ExtensionResourceRepository.ApplyInventoryDeltas]
// as a fixed number of round trips for the whole batch, following the
// same natural-key-resolve-then-write shape as ReplaceInventory
// above. A delta never seeds a brand-new row's alias payload -- see
// [domain.InventoryDelta]'s doc -- so every row here resolves-or-
// creates with the deterministic empty-set payload.
func (r *ExtensionResourceRepo) ApplyInventoryDeltas(ctx context.Context, deltas []domain.InventoryDelta) error {
	if len(deltas) == 0 {
		return nil
	}
	for _, d := range deltas {
		if err := domain.ValidateInventoryDelta(d); err != nil {
			return err
		}
	}

	n := len(deltas)
	resourceTypes := make([]domain.ResourceType, n)
	names := make([]domain.ResourceName, n)
	candidateUIDs := make([]domain.ExtensionResourceUID, n)
	receivedAts := make([]time.Time, n)
	emptyAliasPayloads := make([]string, n)
	emptyPayload, err := reportedAliasObjectPayload(domain.AliasSet{})
	if err != nil {
		return fmt.Errorf("marshal empty alias payload: %w", err)
	}
	for i, d := range deltas {
		resourceTypes[i] = d.ResourceType
		names[i] = d.Name
		candidateUIDs[i] = d.CandidateUID
		receivedAts[i] = d.ReceivedAt
		emptyAliasPayloads[i] = string(emptyPayload)
	}
	uids, _, err := r.resolveOrCreateExtensionResources(
		ctx, resourceTypes, names, candidateUIDs, receivedAts, emptyAliasPayloads)
	if err != nil {
		return fmt.Errorf("apply inventory deltas: %w", err)
	}

	prevLabels, prevConditions, err := r.batchReadCurrentLabelsAndConditions(ctx, uids)
	if err != nil {
		return fmt.Errorf("apply inventory deltas: %w", err)
	}

	invItems := make([]inventoryRowInput, n)
	var aliasWorkIdx []int
	for i, d := range deltas {
		var labelsJSON []byte
		var err error
		if d.ReplaceLabels != nil {
			labelsJSON, err = json.Marshal(nonNilLabels(d.ReplaceLabels))
			if err != nil {
				return fmt.Errorf("marshal replace labels: %w", err)
			}
		} else {
			labels := map[string]string{}
			for k, v := range prevLabels[uids[i]] {
				labels[k] = v
			}
			for _, k := range d.DeleteLabels {
				delete(labels, k)
			}
			for k, v := range d.UpsertLabels {
				labels[k] = v
			}
			labelsJSON, err = json.Marshal(labels)
			if err != nil {
				return fmt.Errorf("marshal merged labels: %w", err)
			}
		}

		var conditionsJSON []byte
		if d.ReplaceConditions != nil {
			conditionsJSON, err = conditionsToJSON(d.ReplaceConditions)
			if err != nil {
				return fmt.Errorf("marshal replace conditions: %w", err)
			}
		} else {
			conditions := map[string]ConditionJSON{}
			for t, c := range prevConditions[uids[i]] {
				conditions[t] = c
			}
			for _, t := range d.DeleteConditions {
				delete(conditions, string(t))
			}
			for _, c := range d.UpsertConditions {
				conditions[string(c.Type())] = ConditionJSON{
					Status: c.Status(), Reason: c.Reason(), Message: c.Message(), LastTransitionTime: c.LastTransitionTime(),
				}
			}
			conditionsJSON, err = json.Marshal(conditions)
			if err != nil {
				return fmt.Errorf("marshal merged conditions: %w", err)
			}
		}

		invItems[i] = inventoryRowInput{
			uid:            uids[i],
			observation:    normalizeObservation(d.Observation),
			labelsJSON:     string(labelsJSON),
			conditionsJSON: string(conditionsJSON),
			observedAt:     d.ObservedAt,
			updatedAt:      d.ReceivedAt,
		}

		if d.UpsertAliases.Len() > 0 {
			aliasWorkIdx = append(aliasWorkIdx, i)
		}
	}
	if err := r.batchUpsertInventoryRows(ctx, invItems); err != nil {
		return fmt.Errorf("apply inventory deltas: %w", err)
	}

	if len(aliasWorkIdx) == 0 {
		return nil
	}
	aliasUIDs := make([]domain.ExtensionResourceUID, len(aliasWorkIdx))
	for i, idx := range aliasWorkIdx {
		aliasUIDs[i] = uids[idx]
	}
	currentAliases, err := r.batchReadCurrentReportedAliases(ctx, aliasUIDs)
	if err != nil {
		return fmt.Errorf("apply inventory deltas: %w", err)
	}
	for _, idx := range aliasWorkIdx {
		uid := uids[idx]
		merged := currentAliases[uid].Merge(deltas[idx].UpsertAliases)
		if merged.Equal(currentAliases[uid]) {
			continue
		}
		payload, err := reportedAliasObjectPayload(merged)
		if err != nil {
			return fmt.Errorf("apply inventory deltas: marshal merged aliases: %w", err)
		}
		if _, err := r.DB.ExecContext(ctx,
			`UPDATE extension_resources SET reported_aliases = ?, updated_at = ? WHERE uid = ?`,
			string(payload), deltas[idx].ReceivedAt.UTC().Format(time.RFC3339Nano), uid.String(),
		); err != nil {
			return fmt.Errorf("apply inventory deltas: write merged alias payload: %w", err)
		}
	}
	return nil
}

// batchReadCurrentLabelsAndConditions reads every uid's current
// latest labels/conditions in one round trip each, for
// [ExtensionResourceRepo.ApplyInventoryDeltas]'s Go-side merge. A uid
// with no existing extension_resource_inventory row (freshly created
// by this same call's resolveOrCreateExtensionResources) is simply
// absent from both maps; callers treat that the same as present-but-
// empty.
func (r *ExtensionResourceRepo) batchReadCurrentLabelsAndConditions(ctx context.Context, uids []domain.ExtensionResourceUID) (map[domain.ExtensionResourceUID]map[string]string, map[domain.ExtensionResourceUID]map[string]ConditionJSON, error) {
	labels := make(map[domain.ExtensionResourceUID]map[string]string, len(uids))
	conditions := make(map[domain.ExtensionResourceUID]map[string]ConditionJSON, len(uids))
	if len(uids) == 0 {
		return labels, conditions, nil
	}
	placeholders := make([]string, len(uids))
	args := make([]any, len(uids))
	for i, u := range uids {
		placeholders[i] = "?"
		args[i] = u.String()
	}
	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT extension_resource_uid, labels, conditions FROM extension_resource_inventory
			WHERE extension_resource_uid IN (%s)`, strings.Join(placeholders, ", ")),
		args...)
	if err != nil {
		return nil, nil, fmt.Errorf("read current labels and conditions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var uidStr, labelsJSON, conditionsJSON string
		if err := rows.Scan(&uidStr, &labelsJSON, &conditionsJSON); err != nil {
			return nil, nil, fmt.Errorf("scan current labels and conditions: %w", err)
		}
		uid, err := domain.ParseExtensionResourceUID(uidStr)
		if err != nil {
			return nil, nil, fmt.Errorf("parse uid: %w", err)
		}
		var l map[string]string
		if err := json.Unmarshal([]byte(labelsJSON), &l); err != nil {
			return nil, nil, fmt.Errorf("unmarshal current labels: %w", err)
		}
		labels[uid] = l
		var c map[string]ConditionJSON
		if err := json.Unmarshal([]byte(conditionsJSON), &c); err != nil {
			return nil, nil, fmt.Errorf("unmarshal current conditions: %w", err)
		}
		conditions[uid] = c
	}
	return labels, conditions, rows.Err()
}

// batchReadCurrentReportedAliases reads every uid's current
// reported_aliases in one round trip, for
// [ExtensionResourceRepo.ApplyInventoryDeltas]'s UpsertAliases merge.
func (r *ExtensionResourceRepo) batchReadCurrentReportedAliases(ctx context.Context, uids []domain.ExtensionResourceUID) (map[domain.ExtensionResourceUID]domain.AliasSet, error) {
	result := make(map[domain.ExtensionResourceUID]domain.AliasSet, len(uids))
	if len(uids) == 0 {
		return result, nil
	}
	placeholders := make([]string, len(uids))
	args := make([]any, len(uids))
	for i, u := range uids {
		placeholders[i] = "?"
		args[i] = u.String()
	}
	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT uid, reported_aliases FROM extension_resources WHERE uid IN (%s)`, strings.Join(placeholders, ", ")),
		args...)
	if err != nil {
		return nil, fmt.Errorf("read current reported aliases: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var uidStr, aliasesJSON string
		if err := rows.Scan(&uidStr, &aliasesJSON); err != nil {
			return nil, fmt.Errorf("scan current reported aliases: %w", err)
		}
		uid, err := domain.ParseExtensionResourceUID(uidStr)
		if err != nil {
			return nil, fmt.Errorf("parse uid: %w", err)
		}
		aliases, err := unmarshalReportedAliasesPayload([]byte(aliasesJSON))
		if err != nil {
			return nil, fmt.Errorf("unmarshal current reported aliases: %w", err)
		}
		result[uid] = domain.AliasSetFromSnapshot(aliases)
	}
	return result, rows.Err()
}

// ---------------------------------------------------------------------------
// Inventory delete/prune
// ---------------------------------------------------------------------------

// deleteInventoryResourcesByPredicate deletes extension_resources rows
// matching whereSQL/args, running the same orphaned-alias-claim cleanup
// [ExtensionResourceRepo.Delete] does, but treating zero matching rows
// as success rather than [domain.ErrNotFound]. This is the shared
// shape [ExtensionResourceRepo.DeleteInventoryResources],
// [ExtensionResourceRepo.PruneInventoryCollection], and
// [ExtensionResourceRepo.DeleteInventorySubtree] all need: source-
// driven deletes, where a duplicate or already-absent delete must not
// fail. whereSQL must reference extension_resources columns
// unqualified, since it is reused verbatim against both the
// unaliased DELETE and the alias-claim lookup JOIN below.
func (r *ExtensionResourceRepo) deleteInventoryResourcesByPredicate(ctx context.Context, whereSQL string, args []any) error {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT c.claim_id FROM resource_alias_contributions c
		 JOIN extension_resources ON extension_resources.uid = c.source_extension_resource_uid
		 WHERE `+whereSQL, args...)
	if err != nil {
		return fmt.Errorf("find alias claims for delete: %w", err)
	}
	var claimIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan alias claim id for delete: %w", err)
		}
		claimIDs = append(claimIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("find alias claims for delete: %w", err)
	}
	rows.Close()

	if _, err := r.DB.ExecContext(ctx, `DELETE FROM extension_resources WHERE `+whereSQL, args...); err != nil {
		return err
	}

	if err := r.deleteOrphanedClaims(ctx, claimIDs); err != nil {
		return fmt.Errorf("clean up orphaned alias claims: %w", err)
	}
	return nil
}

// DeleteInventoryResources implements
// [domain.ExtensionResourceRepository.DeleteInventoryResources] with a
// single row-value IN match across every ref's natural key.
func (r *ExtensionResourceRepo) DeleteInventoryResources(ctx context.Context, refs []domain.InventoryResourceRef) error {
	if len(refs) == 0 {
		return nil
	}
	placeholders := make([]string, len(refs))
	args := make([]any, 0, len(refs)*3)
	for i, ref := range refs {
		placeholders[i] = "(?, ?, ?)"
		args = append(args, string(ref.ResourceType.ServiceName()), string(ref.Name.Collection()), string(ref.Name.ID()))
	}
	whereSQL := fmt.Sprintf("(service_name, collection_name, resource_id) IN (%s)", strings.Join(placeholders, ", "))
	return r.deleteInventoryResourcesByPredicate(ctx, whereSQL, args)
}

// PruneInventoryCollection implements
// [domain.ExtensionResourceRepository.PruneInventoryCollection]. A nil
// keepIDs is rejected before any SQL runs; a non-nil empty keepIDs
// omits the NOT IN clause entirely, deleting every row in the exact
// collection.
func (r *ExtensionResourceRepo) PruneInventoryCollection(ctx context.Context, scope domain.InventoryCollectionRef, keepIDs []domain.ResourceID) error {
	if keepIDs == nil {
		return fmt.Errorf("%w: keepIDs must not be nil", domain.ErrInvalidArgument)
	}
	whereSQL := "service_name = ? AND type_name = ? AND collection_name = ?"
	args := []any{string(scope.ResourceType.ServiceName()), scope.ResourceType.TypeName(), string(scope.Collection)}
	if len(keepIDs) > 0 {
		placeholders := make([]string, len(keepIDs))
		for i, id := range keepIDs {
			placeholders[i] = "?"
			args = append(args, string(id))
		}
		whereSQL += fmt.Sprintf(" AND resource_id NOT IN (%s)", strings.Join(placeholders, ", "))
	}
	return r.deleteInventoryResourcesByPredicate(ctx, whereSQL, args)
}

// DeleteInventorySubtree implements
// [domain.ExtensionResourceRepository.DeleteInventorySubtree] using a
// length-bounded substr comparison rather than LIKE, so a parent
// segment containing '%' or '_' can never be misinterpreted as a
// wildcard.
func (r *ExtensionResourceRepo) DeleteInventorySubtree(ctx context.Context, ref domain.InventorySubtreeRef) error {
	prefix := string(ref.Parent) + "/"
	whereSQL := "service_name = ? AND type_name = ? AND (collection_name = ? OR substr(collection_name, 1, ?) = ?)"
	args := []any{string(ref.ResourceType.ServiceName()), ref.ResourceType.TypeName(), string(ref.Parent), len(prefix), prefix}
	return r.deleteInventoryResourcesByPredicate(ctx, whereSQL, args)
}

func (r *ExtensionResourceRepo) ListObservations(ctx context.Context, uid domain.ExtensionResourceUID, limit int) ([]domain.Observation, error) {
	rows, err := r.DB.QueryContext(ctx,
		`SELECT id, extension_resource_uid, observation, observed_at, created_at
		 FROM extension_resource_inventory_observations
		 WHERE extension_resource_uid = ?
		 ORDER BY observed_at DESC
		 LIMIT ?`,
		uid.String(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.Observation
	for rows.Next() {
		var idStr, erUID, obsJSON, observedAt, createdAt string
		if err := rows.Scan(&idStr, &erUID, &obsJSON, &observedAt, &createdAt); err != nil {
			return nil, err
		}
		parsedUID, err := domain.ParseExtensionResourceUID(erUID)
		if err != nil {
			return nil, err
		}
		snap := domain.ObservationSnapshot{
			ID:                   domain.ObservationID(idStr),
			ExtensionResourceUID: parsedUID,
			Observation:          json.RawMessage(obsJSON),
		}
		if t, err := time.Parse(time.RFC3339Nano, observedAt); err == nil {
			snap.ObservedAt = t
		}
		if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
			snap.CreatedAt = t
		}
		result = append(result, domain.ObservationFromSnapshot(snap))
	}
	return result, rows.Err()
}

func (r *ExtensionResourceRepo) ListConditionTransitions(ctx context.Context, uid domain.ExtensionResourceUID, conditionType *domain.ConditionType, limit int) ([]domain.ConditionTransition, error) {
	var q string
	var args []any
	if conditionType != nil {
		q = `SELECT id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at
			 FROM extension_resource_inventory_condition_events
			 WHERE extension_resource_uid = ? AND type = ?
			 ORDER BY observed_at DESC
			 LIMIT ?`
		args = []any{uid.String(), string(*conditionType), limit}
	} else {
		q = `SELECT id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at
			 FROM extension_resource_inventory_condition_events
			 WHERE extension_resource_uid = ?
			 ORDER BY observed_at DESC
			 LIMIT ?`
		args = []any{uid.String(), limit}
	}
	rows, err := r.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []domain.ConditionTransition
	for rows.Next() {
		var idStr, erUID, ctStr, statusStr, reason, message, ltt, observedAt, createdAt string
		if err := rows.Scan(&idStr, &erUID, &ctStr, &statusStr, &reason, &message, &ltt, &observedAt, &createdAt); err != nil {
			return nil, err
		}
		parsedUID, err := domain.ParseExtensionResourceUID(erUID)
		if err != nil {
			return nil, err
		}
		snap := domain.ConditionTransitionSnapshot{
			ID:                   domain.ConditionTransitionID(idStr),
			ExtensionResourceUID: parsedUID,
			ConditionType:        domain.ConditionType(ctStr),
			Status:               domain.ConditionStatus(statusStr),
			Reason:               reason,
			Message:              message,
		}
		if t, err := time.Parse(time.RFC3339Nano, ltt); err == nil {
			snap.LastTransitionTime = t
		}
		if t, err := time.Parse(time.RFC3339Nano, observedAt); err == nil {
			snap.ObservedAt = t
		}
		if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
			snap.CreatedAt = t
		}
		result = append(result, domain.ConditionTransitionFromSnapshot(snap))
	}
	return result, rows.Err()
}

// nullableFulfillmentScanColumns is like [fulfillmentScanColumns] but
// uses sql.Null* types for all fields so it can handle LEFT JOIN rows
// where the fulfillment is NULL.
type nullableFulfillmentScanColumns struct {
	id, rtJSON, stateStr, pauseReason, statusReason, authJSON, createdAtStr, updatedAtStr sql.NullString
	msSpec, psSpec, rsSpec, provJSON, attestRefJSON                                       sql.NullString
	msVer, psVer, rsVer, generation, observedGeneration                                   sql.NullInt64
	activeWorkflowGen                                                                     sql.NullInt64
}

func (c *nullableFulfillmentScanColumns) dests() []any {
	return []any{
		&c.id, &c.msVer, &c.msSpec, &c.psVer, &c.psSpec, &c.rsVer, &c.rsSpec,
		&c.rtJSON, &c.stateStr, &c.pauseReason, &c.statusReason, &c.authJSON, &c.provJSON, &c.attestRefJSON,
		&c.generation, &c.observedGeneration, &c.activeWorkflowGen,
		&c.createdAtStr, &c.updatedAtStr,
	}
}

func (c *nullableFulfillmentScanColumns) isPresent() bool {
	return c.id.Valid
}

func (c *nullableFulfillmentScanColumns) snapshot() (domain.FulfillmentSnapshot, error) {
	fc := fulfillmentScanColumns{
		id:                 c.id.String,
		rtJSON:             c.rtJSON.String,
		stateStr:           c.stateStr.String,
		pauseReason:        c.pauseReason.String,
		statusReason:       c.statusReason.String,
		authJSON:           c.authJSON.String,
		createdAtStr:       c.createdAtStr.String,
		updatedAtStr:       c.updatedAtStr.String,
		msSpec:             c.msSpec,
		psSpec:             c.psSpec,
		rsSpec:             c.rsSpec,
		provJSON:           c.provJSON,
		attestRefJSON:      c.attestRefJSON,
		msVer:              c.msVer.Int64,
		psVer:              c.psVer.Int64,
		rsVer:              c.rsVer.Int64,
		generation:         c.generation.Int64,
		observedGeneration: c.observedGeneration.Int64,
		activeWorkflowGen:  c.activeWorkflowGen,
	}
	return fc.snapshot()
}
