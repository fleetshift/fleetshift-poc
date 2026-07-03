package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/google/uuid"
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

	labelsJSON, err := json.Marshal(s.Labels)
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
// invLabelsSubquerySQLite derives the latest inventory label set for
// an extension resource from the normalized
// extension_resource_inventory_labels table rather than a JSON
// column, so batch label writes can be blind multi-row
// upserts/deletes against a real table (see [ExtensionResourceRepo.batchReplaceLabels]
// etc.) instead of a read-modify-write on a JSON blob. Returns SQL
// NULL when a resource has no labels (json_group_object, like every
// other aggregate, returns NULL over zero input rows); scan helpers
// already treat a NULL/invalid labels column as an empty map.
const invLabelsSubquerySQLite = `(SELECT json_group_object(l.key, l.value) FROM extension_resource_inventory_labels l WHERE l.extension_resource_uid = er.uid) AS inv_labels`

var erInstanceQuerySQLite = `SELECT er.uid, er.service_name, er.type_name, er.collection_name, er.resource_id, er.labels, er.created_at, er.updated_at,
	m.current_version, m.fulfillment_id,
	` + invLabelsSubquerySQLite + `,
	inv.observation, inv.observed_at, inv.updated_at,
	(SELECT json_group_array(json_object(
		'type', c.type,
		'status', c.status,
		'reason', c.reason,
		'message', c.message,
		'last_transition_time', c.last_transition_time
	))
	 FROM (SELECT type, status, reason, message, last_transition_time
	       FROM extension_resource_inventory_conditions
	       WHERE extension_resource_uid = er.uid
	       ORDER BY type) c) AS inv_conditions
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
// explicit cleanup, via the same [ExtensionResourceRepo.deleteOrphanedClaims]
// helper retractAbsentAliases uses. The claim ids to check are read
// *before* the delete (SQLite's cascade doesn't hand them back the
// way Postgres's DELETE ... RETURNING does); this is a plain
// multi-statement sequence, not optimized the way ReplaceInventory/
// ApplyInventoryDeltas are, since Delete isn't a hot batch path.
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
	er.uid, er.service_name, er.type_name, er.collection_name, er.resource_id, er.labels, er.created_at, er.updated_at,
	m.current_version, m.fulfillment_id,
	ri.spec, ri.created_at,
	` + fulfillmentColumnsJoined("f") + `,
	` + invLabelsSubquerySQLite + `,
	inv.observation, inv.observed_at, inv.updated_at,
	(SELECT json_group_array(json_object(
		'type', c.type,
		'status', c.status,
		'reason', c.reason,
		'message', c.message,
		'last_transition_time', c.last_transition_time
	))
	 FROM (SELECT type, status, reason, message, last_transition_time
	       FROM extension_resource_inventory_conditions
	       WHERE extension_resource_uid = er.uid
	       ORDER BY type) c) AS inv_conditions
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
// collection_name, resource_id), creating it with candidateUIDs[i] if
// it doesn't exist yet, in the same two statements
// [domain.ExtensionResourceRepository.ReplaceInventory]'s deleted
// UpsertBatch predecessor used: SQLite has no writable CTEs, so this
// is two statements rather than Postgres's one CTE-chained lookup --
// a blind multi-row INSERT ... ON CONFLICT DO NOTHING, then a single
// tuple-IN SELECT that resolves every input row's UID, whether just
// inserted or already there. Neither statement loops per row, and
// neither ever touches an existing row's own UID/labels/timestamps.
//
// The resolve SELECT also reads each row's current alias_fingerprint,
// returned alongside uids in the same order -- for a row just
// inserted by this same call it's always nil (the INSERT never sets
// that column, so it defaults to NULL), and for a pre-existing row
// it's whatever [ExtensionResourceRepo.ReplaceInventory]'s alias
// fingerprint fast path last wrote there, if anything (see the
// migration's doc comment on that column). Callers use this to skip
// alias reclassification entirely for a report whose complete alias
// set hasn't changed since the last successful write; see the
// Postgres sibling's aliasFingerprintGateCTEs for the equivalent SQL-
// side gate this mirrors.
//
// Both returned slices are in the same order as the input slices.
func (r *ExtensionResourceRepo) resolveOrCreateExtensionResources(
	ctx context.Context,
	resourceTypes []domain.ResourceType, names []domain.ResourceName, candidateUIDs []domain.ExtensionResourceUID, receivedAts []time.Time,
) ([]domain.ExtensionResourceUID, [][]byte, error) {
	n := len(resourceTypes)
	if n == 0 {
		return nil, nil, nil
	}

	insertPlaceholders := make([]string, n)
	insertArgs := make([]any, 0, n*7)
	selectPlaceholders := make([]string, n)
	selectArgs := make([]any, 0, n*3)
	for i := range resourceTypes {
		insertPlaceholders[i] = "(?, ?, ?, ?, ?, '{}', ?, ?)"
		receivedAt := receivedAts[i].UTC().Format(time.RFC3339Nano)
		insertArgs = append(insertArgs,
			candidateUIDs[i].String(), string(resourceTypes[i].ServiceName()), resourceTypes[i].TypeName(),
			string(names[i].Collection()), string(names[i].ID()),
			receivedAt, receivedAt)

		selectPlaceholders[i] = "(?, ?, ?)"
		selectArgs = append(selectArgs,
			string(resourceTypes[i].ServiceName()), string(names[i].Collection()), string(names[i].ID()))
	}

	if _, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO extension_resources (uid, service_name, type_name, collection_name, resource_id, labels, created_at, updated_at)
			VALUES %s
			ON CONFLICT (service_name, collection_name, resource_id) DO NOTHING`, strings.Join(insertPlaceholders, ", ")),
		insertArgs...,
	); err != nil {
		return nil, nil, fmt.Errorf("resolve-or-create extension resources (insert): %w", err)
	}

	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT service_name, collection_name, resource_id, uid, alias_fingerprint FROM extension_resources
			WHERE (service_name, collection_name, resource_id) IN (%s)`, strings.Join(selectPlaceholders, ", ")),
		selectArgs...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("resolve-or-create extension resources (resolve): %w", err)
	}
	defer rows.Close()

	type resolvedRow struct {
		uid              domain.ExtensionResourceUID
		aliasFingerprint []byte
	}
	resolved := make(map[domain.FullResourceName]resolvedRow, n)
	for rows.Next() {
		var serviceName, collectionName, resourceID, uidStr string
		var aliasFingerprint []byte
		if err := rows.Scan(&serviceName, &collectionName, &resourceID, &uidStr, &aliasFingerprint); err != nil {
			return nil, nil, fmt.Errorf("scan resolved extension resource: %w", err)
		}
		uid, err := domain.ParseExtensionResourceUID(uidStr)
		if err != nil {
			return nil, nil, fmt.Errorf("parse resolved uid: %w", err)
		}
		fullName := domain.NewFullResourceName(domain.ServiceName(serviceName), domain.ResourceName(collectionName+"/"+resourceID))
		resolved[fullName] = resolvedRow{uid: uid, aliasFingerprint: aliasFingerprint}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("resolve-or-create extension resources (resolve): %w", err)
	}

	uids := make([]domain.ExtensionResourceUID, n)
	storedAliasFingerprints := make([][]byte, n)
	for i := range resourceTypes {
		fullName := domain.NewFullResourceName(resourceTypes[i].ServiceName(), names[i])
		row, ok := resolved[fullName]
		if !ok {
			return nil, nil, fmt.Errorf("resolve-or-create extension resources: no result for %s", fullName)
		}
		uids[i] = row.uid
		storedAliasFingerprints[i] = row.aliasFingerprint
	}
	return uids, storedAliasFingerprints, nil
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
	var uidStr, serviceName, typeName, collectionName, resourceID, labelsJSON, createdAt, updatedAt string
	var mVersion sql.NullInt64
	var mFulfillmentID sql.NullString
	var invLabels, invObservation, invObservedAt, invUpdatedAt sql.NullString
	var invConditionsJSON sql.NullString

	if err := s.Scan(&uidStr, &serviceName, &typeName, &collectionName, &resourceID, &labelsJSON, &createdAt, &updatedAt,
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

	snap := domain.ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: domain.ResourceType(serviceName + "/" + typeName),
		Name:         domain.ResourceName(collectionName + "/" + resourceID),
		Labels:       labels,
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
	var uidStr, serviceName, typeName, collectionName, resourceID, labelsJSON, erCreatedAt, erUpdatedAt string
	var mVersion sql.NullInt64
	var mFulfillmentID sql.NullString
	var riSpec, riCreatedAt sql.NullString
	var fCols nullableFulfillmentScanColumns

	// Inventory columns (all nullable)
	var invLabels, invObservation, invObservedAt, invUpdatedAt sql.NullString
	var invConditionsJSON sql.NullString

	if err := s.Scan(append(append([]any{
		&uidStr, &serviceName, &typeName, &collectionName, &resourceID, &labelsJSON, &erCreatedAt, &erUpdatedAt,
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

	var labels map[string]string
	if err := json.Unmarshal([]byte(labelsJSON), &labels); err != nil {
		return domain.ExtensionResourceView{}, fmt.Errorf("unmarshal labels: %w", err)
	}

	snap := domain.ExtensionResourceSnapshot{
		UID:          uid,
		ResourceType: domain.ResourceType(serviceName + "/" + typeName),
		Name:         domain.ResourceName(collectionName + "/" + resourceID),
		Labels:       labels,
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

// conditionRow mirrors the JSON shape produced by the json_group_array subquery.
type conditionRow struct {
	Type               string `json:"type"`
	Status             string `json:"status"`
	Reason             string `json:"reason"`
	Message            string `json:"message"`
	LastTransitionTime string `json:"last_transition_time"`
}

func unmarshalConditionSnapshots(data []byte) ([]domain.ConditionSnapshot, error) {
	var rows []conditionRow
	if err := json.Unmarshal(data, &rows); err != nil {
		return nil, err
	}
	snaps := make([]domain.ConditionSnapshot, len(rows))
	for i, r := range rows {
		snaps[i] = domain.ConditionSnapshot{
			Type:    domain.ConditionType(r.Type),
			Status:  domain.ConditionStatus(r.Status),
			Reason:  r.Reason,
			Message: r.Message,
		}
		if t, err := time.Parse(time.RFC3339Nano, r.LastTransitionTime); err == nil {
			snaps[i].LastTransitionTime = t
		}
	}
	return snaps, nil
}

// ---------------------------------------------------------------------------
// Inventory methods
// ---------------------------------------------------------------------------
//
// ReplaceInventory and ApplyInventoryDeltas are each a fixed, small
// number of round trips for their *entire* input slice, not one round
// trip per item: every per-item helper below (batchUpsertLatestRows,
// batchReplaceLabels, batchSetLabels, batchDeleteLabels,
// batchDeleteConditionsAbsentFrom, batchDeleteConditionsByType,
// batchRecordConditions) takes a flattened slice spanning every
// resource in the call and issues one or more multi-row statements
// built with dynamically generated placeholders.
//
// Unlike the Postgres sibling file, SQLite has no writable CTEs, so a
// step that needs to know prior state to detect a genuine change
// (observation history, condition transitions) reads that state with
// its own SELECT before the write rather than chaining both into one
// statement -- the same multi-step technique the single-row
// recordCondition this replaced already used, just applied across the
// whole batch at once. This is still a fixed number of round trips
// per batch, not one per item.
//
// SQLite's per-statement placeholder count grows with input size here
// (one or more "?" per row), unlike Postgres's fixed-size UNNEST
// array parameters, so very large batches may eventually need
// chunking against SQLite's placeholder limit; that's tracked
// separately.

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

// latestRowInput is the flattened per-resource input row shared by
// [ExtensionResourceRepo.insertInventoryLatestRow] and
// [ExtensionResourceRepo.batchUpsertLatestRows]. observation == nil
// means "untouched" (see [normalizeObservation]).
type latestRowInput struct {
	uid         domain.ExtensionResourceUID
	observation *json.RawMessage
	observedAt  time.Time
	updatedAt   time.Time
}

// insertInventoryLatestRow inserts a brand-new latest-inventory row
// for a resource that has just been created and cannot yet have one,
// so unlike [ExtensionResourceRepo.batchUpsertLatestRows] this is a
// plain INSERT: no conflict is possible, and (matching the prior
// behavior of this Create-only path) no observation history row is
// appended for the resource's initial observation.
func (r *ExtensionResourceRepo) insertInventoryLatestRow(ctx context.Context, uid domain.ExtensionResourceUID, observation *json.RawMessage, observedAt, updatedAt time.Time) error {
	var obsArg any
	if observation != nil {
		obsArg = string(*observation)
	}
	_, err := r.DB.ExecContext(ctx,
		`INSERT INTO extension_resource_inventory (extension_resource_uid, observation, observed_at, updated_at)
		 VALUES (?, ?, ?, ?)`,
		uid.String(), obsArg, observedAt.UTC().Format(time.RFC3339Nano), updatedAt.UTC().Format(time.RFC3339Nano))
	return err
}

// batchUpsertLatestRows is the single low-level "write latest
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
//
// It reads every uid's current observation up front (one query) to
// determine which resources' new observation is a genuine change,
// then does the upsert and, for the changed subset, appends
// observation history -- three round trips total, not per item.
func (r *ExtensionResourceRepo) batchUpsertLatestRows(ctx context.Context, items []latestRowInput) error {
	if len(items) == 0 {
		return nil
	}

	uidPlaceholders := make([]string, len(items))
	uidArgs := make([]any, len(items))
	for i, it := range items {
		uidPlaceholders[i] = "?"
		uidArgs[i] = it.uid.String()
	}
	prevObservations := make(map[string]sql.NullString, len(items))
	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT extension_resource_uid, observation FROM extension_resource_inventory
			WHERE extension_resource_uid IN (%s)`, strings.Join(uidPlaceholders, ", ")),
		uidArgs...)
	if err != nil {
		return fmt.Errorf("read previous observations: %w", err)
	}
	for rows.Next() {
		var uidStr string
		var obs sql.NullString
		if err := rows.Scan(&uidStr, &obs); err != nil {
			rows.Close()
			return fmt.Errorf("scan previous observation: %w", err)
		}
		prevObservations[uidStr] = obs
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read previous observations: %w", err)
	}
	rows.Close()

	upsertPlaceholders := make([]string, len(items))
	upsertArgs := make([]any, 0, len(items)*4)
	type historyRow struct {
		id, uid, observation, observedAt, createdAt string
	}
	var history []historyRow
	for i, it := range items {
		uidStr := it.uid.String()
		observedAtStr := it.observedAt.UTC().Format(time.RFC3339Nano)
		updatedAtStr := it.updatedAt.UTC().Format(time.RFC3339Nano)
		var obsArg any
		if it.observation != nil {
			obsArg = string(*it.observation)
		}
		upsertPlaceholders[i] = "(?, ?, ?, ?)"
		upsertArgs = append(upsertArgs, uidStr, obsArg, observedAtStr, updatedAtStr)

		if it.observation != nil {
			prev, existed := prevObservations[uidStr]
			differs := !existed || !prev.Valid || prev.String != string(*it.observation)
			if differs {
				history = append(history, historyRow{
					id:          string(domain.NewObservationID()),
					uid:         uidStr,
					observation: string(*it.observation),
					observedAt:  observedAtStr,
					createdAt:   updatedAtStr,
				})
			}
		}
	}

	if _, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO extension_resource_inventory (extension_resource_uid, observation, observed_at, updated_at)
			VALUES %s
			ON CONFLICT(extension_resource_uid) DO UPDATE SET
				observation = COALESCE(excluded.observation, extension_resource_inventory.observation),
				observed_at = excluded.observed_at,
				updated_at = excluded.updated_at`, strings.Join(upsertPlaceholders, ", ")),
		upsertArgs...); err != nil {
		return fmt.Errorf("batch upsert latest inventory rows: %w", err)
	}

	if len(history) > 0 {
		histPlaceholders := make([]string, len(history))
		histArgs := make([]any, 0, len(history)*5)
		for i, h := range history {
			histPlaceholders[i] = "(?, ?, ?, ?, ?)"
			histArgs = append(histArgs, h.id, h.uid, h.observation, h.observedAt, h.createdAt)
		}
		if _, err := r.DB.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO extension_resource_inventory_observations
				(id, extension_resource_uid, observation, observed_at, created_at)
				VALUES %s`, strings.Join(histPlaceholders, ", ")),
			histArgs...); err != nil {
			return fmt.Errorf("batch append observation history: %w", err)
		}
	}
	return nil
}

// labelTriple is a flattened (resource, key, value) input row shared
// by the inventory label batch primitives below.
type labelTriple struct {
	uid   domain.ExtensionResourceUID
	key   string
	value string
}

// labelKey is a flattened (resource, key) input row for
// [ExtensionResourceRepo.batchDeleteLabels].
type labelKey struct {
	uid domain.ExtensionResourceUID
	key string
}

// batchReplaceLabels implements ReplaceInventory's "Labels is the
// complete latest label set" semantics for a whole batch. uids is
// every resource in the call (including ones with zero supplied
// labels, whose existing labels are entirely cleared); labels is the
// flattened union of every replacement's supplied labels. The delete
// and the guarded upsert are two independent statements (no ordering
// dependency): the delete only ever removes rows outside the keep set
// the insert is about to write, and the insert only ever touches rows
// inside it.
func (r *ExtensionResourceRepo) batchReplaceLabels(ctx context.Context, uids []domain.ExtensionResourceUID, labels []labelTriple) error {
	if len(uids) == 0 {
		return nil
	}
	uidPlaceholders := make([]string, len(uids))
	uidArgs := make([]any, len(uids))
	for i, u := range uids {
		uidPlaceholders[i] = "?"
		uidArgs[i] = u.String()
	}

	if len(labels) == 0 {
		_, err := r.DB.ExecContext(ctx,
			fmt.Sprintf(`DELETE FROM extension_resource_inventory_labels WHERE extension_resource_uid IN (%s)`,
				strings.Join(uidPlaceholders, ", ")),
			uidArgs...)
		if err != nil {
			return fmt.Errorf("batch replace inventory labels (clear): %w", err)
		}
		return nil
	}

	keepPlaceholders := make([]string, len(labels))
	keepArgs := make([]any, 0, len(labels)*2)
	upsertPlaceholders := make([]string, len(labels))
	upsertArgs := make([]any, 0, len(labels)*3)
	for i, l := range labels {
		keepPlaceholders[i] = "(?, ?)"
		keepArgs = append(keepArgs, l.uid.String(), l.key)
		upsertPlaceholders[i] = "(?, ?, ?)"
		upsertArgs = append(upsertArgs, l.uid.String(), l.key, l.value)
	}

	deleteArgs := append(append([]any{}, uidArgs...), keepArgs...)
	if _, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM extension_resource_inventory_labels
			WHERE extension_resource_uid IN (%s)
			  AND (extension_resource_uid, key) NOT IN (%s)`,
			strings.Join(uidPlaceholders, ", "), strings.Join(keepPlaceholders, ", ")),
		deleteArgs...); err != nil {
		return fmt.Errorf("batch replace inventory labels (delete absent): %w", err)
	}

	if _, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO extension_resource_inventory_labels (extension_resource_uid, key, value)
			VALUES %s
			ON CONFLICT (extension_resource_uid, key) DO UPDATE SET value = excluded.value
			WHERE extension_resource_inventory_labels.value IS NOT excluded.value`,
			strings.Join(upsertPlaceholders, ", ")),
		upsertArgs...); err != nil {
		return fmt.Errorf("batch replace inventory labels (upsert): %w", err)
	}
	return nil
}

// batchSetLabels guarded-upserts the flattened union of every input
// resource's labels in one statement, used both by
// [ExtensionResourceRepo.insertInventory] (a resource's initial
// labels) and [ExtensionResourceRepo.ApplyInventoryDeltas] (the
// flattened union of every delta's SetLabels). Unlike
// [ExtensionResourceRepo.batchReplaceLabels], there's no "keep set"
// bookkeeping: independent per-resource deltas say nothing about
// labels outside the named keys, so nothing here is ever deleted.
func (r *ExtensionResourceRepo) batchSetLabels(ctx context.Context, labels []labelTriple) error {
	if len(labels) == 0 {
		return nil
	}
	placeholders := make([]string, len(labels))
	args := make([]any, 0, len(labels)*3)
	for i, l := range labels {
		placeholders[i] = "(?, ?, ?)"
		args = append(args, l.uid.String(), l.key, l.value)
	}
	_, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO extension_resource_inventory_labels (extension_resource_uid, key, value)
			VALUES %s
			ON CONFLICT (extension_resource_uid, key) DO UPDATE SET value = excluded.value
			WHERE extension_resource_inventory_labels.value IS NOT excluded.value`,
			strings.Join(placeholders, ", ")),
		args...)
	if err != nil {
		return fmt.Errorf("batch set inventory labels: %w", err)
	}
	return nil
}

// batchDeleteLabels deletes the flattened union of every
// ApplyInventoryDeltas delta's DeleteLabels in one statement.
func (r *ExtensionResourceRepo) batchDeleteLabels(ctx context.Context, keys []labelKey) error {
	if len(keys) == 0 {
		return nil
	}
	placeholders := make([]string, len(keys))
	args := make([]any, 0, len(keys)*2)
	for i, k := range keys {
		placeholders[i] = "(?, ?)"
		args = append(args, k.uid.String(), k.key)
	}
	_, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM extension_resource_inventory_labels WHERE (extension_resource_uid, key) IN (%s)`,
			strings.Join(placeholders, ", ")),
		args...)
	if err != nil {
		return fmt.Errorf("batch delete inventory labels: %w", err)
	}
	return nil
}

// conditionKey is a flattened (resource, condition type) input row
// for [ExtensionResourceRepo.batchDeleteConditionsAbsentFrom] and
// [ExtensionResourceRepo.batchDeleteConditionsByType].
type conditionKey struct {
	uid      domain.ExtensionResourceUID
	condType domain.ConditionType
}

// batchDeleteConditionsAbsentFrom implements ReplaceInventory's "the
// replacement is the complete current condition set" semantics for a
// whole batch. uids is every resource in the call (including ones
// with zero supplied conditions, whose existing conditions are
// entirely cleared); keep is the flattened union of every
// replacement's supplied condition types. No transition row is
// recorded for the removal (per the rework doc's condition
// transition rules).
func (r *ExtensionResourceRepo) batchDeleteConditionsAbsentFrom(ctx context.Context, uids []domain.ExtensionResourceUID, keep []conditionKey) error {
	if len(uids) == 0 {
		return nil
	}
	uidPlaceholders := make([]string, len(uids))
	uidArgs := make([]any, len(uids))
	for i, u := range uids {
		uidPlaceholders[i] = "?"
		uidArgs[i] = u.String()
	}
	if len(keep) == 0 {
		_, err := r.DB.ExecContext(ctx,
			fmt.Sprintf(`DELETE FROM extension_resource_inventory_conditions WHERE extension_resource_uid IN (%s)`,
				strings.Join(uidPlaceholders, ", ")),
			uidArgs...)
		if err != nil {
			return fmt.Errorf("batch delete absent conditions (clear): %w", err)
		}
		return nil
	}
	keepPlaceholders := make([]string, len(keep))
	keepArgs := make([]any, 0, len(keep)*2)
	for i, k := range keep {
		keepPlaceholders[i] = "(?, ?)"
		keepArgs = append(keepArgs, k.uid.String(), string(k.condType))
	}
	args := append(append([]any{}, uidArgs...), keepArgs...)
	_, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM extension_resource_inventory_conditions
			WHERE extension_resource_uid IN (%s)
			  AND (extension_resource_uid, type) NOT IN (%s)`,
			strings.Join(uidPlaceholders, ", "), strings.Join(keepPlaceholders, ", ")),
		args...)
	if err != nil {
		return fmt.Errorf("batch delete absent conditions: %w", err)
	}
	return nil
}

// batchDeleteConditionsByType deletes the flattened union of every
// ApplyInventoryDeltas delta's DeleteConditions in one statement. No
// transition row is recorded (per the rework doc's condition
// transition rules: deletion isn't a transition in this pass).
func (r *ExtensionResourceRepo) batchDeleteConditionsByType(ctx context.Context, keys []conditionKey) error {
	if len(keys) == 0 {
		return nil
	}
	placeholders := make([]string, len(keys))
	args := make([]any, 0, len(keys)*2)
	for i, k := range keys {
		placeholders[i] = "(?, ?)"
		args = append(args, k.uid.String(), string(k.condType))
	}
	_, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM extension_resource_inventory_conditions WHERE (extension_resource_uid, type) IN (%s)`,
			strings.Join(placeholders, ", ")),
		args...)
	if err != nil {
		return fmt.Errorf("batch delete conditions: %w", err)
	}
	return nil
}

// conditionInput is the flattened per-(resource, condition) input row
// for [ExtensionResourceRepo.batchRecordConditions].
type conditionInput struct {
	uid                   domain.ExtensionResourceUID
	condType              domain.ConditionType
	status                domain.ConditionStatus
	reason, message       string
	lastTransitionTime    time.Time
	observedAt, updatedAt time.Time
}

// batchRecordConditions is the batched form of the single-row
// recordCondition path this replaced, applied to every
// (resource, condition) pair in the input:
//  1. Reads the current latest condition state for every pair (one
//     query)
//  2. UPSERTs the latest condition state for every pair (always, for
//     staleness tracking)
//  3. INSERTs a transition record only for pairs whose state actually
//     changed
//
// Three round trips total, not one (or three) per pair.
func (r *ExtensionResourceRepo) batchRecordConditions(ctx context.Context, items []conditionInput) error {
	if len(items) == 0 {
		return nil
	}

	type key struct{ uid, condType string }
	type prevState struct{ status, reason, message string }

	keyPlaceholders := make([]string, len(items))
	keyArgs := make([]any, 0, len(items)*2)
	for i, it := range items {
		keyPlaceholders[i] = "(?, ?)"
		keyArgs = append(keyArgs, it.uid.String(), string(it.condType))
	}
	prev := make(map[key]prevState, len(items))
	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT extension_resource_uid, type, status, reason, message
			FROM extension_resource_inventory_conditions
			WHERE (extension_resource_uid, type) IN (%s)`, strings.Join(keyPlaceholders, ", ")),
		keyArgs...)
	if err != nil {
		return fmt.Errorf("read previous conditions: %w", err)
	}
	for rows.Next() {
		var uidStr, ctStr, status, reason, message string
		if err := rows.Scan(&uidStr, &ctStr, &status, &reason, &message); err != nil {
			rows.Close()
			return fmt.Errorf("scan previous condition: %w", err)
		}
		prev[key{uidStr, ctStr}] = prevState{status: status, reason: reason, message: message}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("read previous conditions: %w", err)
	}
	rows.Close()

	upsertPlaceholders := make([]string, len(items))
	upsertArgs := make([]any, 0, len(items)*8)
	type transitionRow struct {
		id, uid, condType, status, reason, message, lastTransitionTime, observedAt, createdAt string
	}
	var transitions []transitionRow
	for i, it := range items {
		uidStr := it.uid.String()
		ctStr := string(it.condType)
		statusStr := string(it.status)
		lttStr := it.lastTransitionTime.UTC().Format(time.RFC3339Nano)
		obsStr := it.observedAt.UTC().Format(time.RFC3339Nano)
		updStr := it.updatedAt.UTC().Format(time.RFC3339Nano)

		upsertPlaceholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?)"
		upsertArgs = append(upsertArgs, uidStr, ctStr, statusStr, it.reason, it.message, lttStr, obsStr, updStr)

		p, existed := prev[key{uidStr, ctStr}]
		changed := !existed || p.status != statusStr || p.reason != it.reason || p.message != it.message
		if changed {
			transitions = append(transitions, transitionRow{
				id: uuid.New().String(), uid: uidStr, condType: ctStr, status: statusStr,
				reason: it.reason, message: it.message, lastTransitionTime: lttStr, observedAt: obsStr, createdAt: updStr,
			})
		}
	}

	if _, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO extension_resource_inventory_conditions
			(extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, updated_at)
			VALUES %s
			ON CONFLICT (extension_resource_uid, type) DO UPDATE SET
				status = excluded.status,
				reason = excluded.reason,
				message = excluded.message,
				last_transition_time = excluded.last_transition_time,
				observed_at = excluded.observed_at,
				updated_at = excluded.updated_at`, strings.Join(upsertPlaceholders, ", ")),
		upsertArgs...); err != nil {
		return fmt.Errorf("batch upsert conditions: %w", err)
	}

	if len(transitions) > 0 {
		tPlaceholders := make([]string, len(transitions))
		tArgs := make([]any, 0, len(transitions)*9)
		for i, tr := range transitions {
			tPlaceholders[i] = "(?, ?, ?, ?, ?, ?, ?, ?, ?)"
			tArgs = append(tArgs, tr.id, tr.uid, tr.condType, tr.status, tr.reason, tr.message, tr.lastTransitionTime, tr.observedAt, tr.createdAt)
		}
		if _, err := r.DB.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO extension_resource_inventory_condition_events
				(id, extension_resource_uid, type, status, reason, message, last_transition_time, observed_at, created_at)
				VALUES %s`, strings.Join(tPlaceholders, ", ")),
			tArgs...); err != nil {
			return fmt.Errorf("batch insert condition transitions: %w", err)
		}
	}
	return nil
}

// insertInventory writes a resource's initial inventory state as part
// of [ExtensionResourceRepo.Create]. There's nothing pre-existing to
// reconcile against (the resource itself was just created in the same
// call), so this reuses the guarded-upsert/upsert-conditions batch
// primitives with single-element inputs rather than the ReplaceInventory
// delete-absent path.
func (r *ExtensionResourceRepo) insertInventory(ctx context.Context, uid domain.ExtensionResourceUID, inv *domain.InventoryResourceSnapshot) error {
	var obs *json.RawMessage
	if inv.Observation != nil {
		obs = &inv.Observation
	}
	if err := r.insertInventoryLatestRow(ctx, uid, obs, inv.ObservedAt, inv.UpdatedAt); err != nil {
		return fmt.Errorf("insert latest inventory row: %w", err)
	}
	if len(inv.Labels) > 0 {
		triples := make([]labelTriple, 0, len(inv.Labels))
		for k, v := range inv.Labels {
			triples = append(triples, labelTriple{uid: uid, key: k, value: v})
		}
		if err := r.batchSetLabels(ctx, triples); err != nil {
			return fmt.Errorf("insert inventory labels: %w", err)
		}
	}
	if len(inv.Conditions) > 0 {
		items := make([]conditionInput, len(inv.Conditions))
		for i, c := range inv.Conditions {
			items[i] = conditionInput{
				uid: uid, condType: c.Type, status: c.Status, reason: c.Reason, message: c.Message,
				lastTransitionTime: c.LastTransitionTime, observedAt: inv.ObservedAt, updatedAt: inv.UpdatedAt,
			}
		}
		if err := r.batchRecordConditions(ctx, items); err != nil {
			return fmt.Errorf("insert inventory conditions: %w", err)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Alias fold-in (see aliasCandidateInput's doc)
// ---------------------------------------------------------------------------

// aliasCandidateInput is a flattened (report position, resolved
// target, alias) input row for ReplaceInventory/ApplyInventoryDeltas's
// alias fold-in. receivedAt carries its report's ReceivedAt so
// writeAliases doesn't need a separate lookup back to the report.
// sourceUID is the contributing extension resource's own persistent
// uid -- unlike the Postgres sibling, which has to join it in via SQL
// (see that file's flattenAliasCandidates doc comment for why), it's
// simply uids[i] here: resolveOrCreateExtensionResources's own
// round trip has already resolved it by the time ReplaceInventory/
// ApplyInventoryDeltas build this slice, no CTE-style deferral needed.
type aliasCandidateInput struct {
	idx        int
	target     domain.ResourceName
	alias      domain.Alias
	receivedAt time.Time
	sourceUID  domain.ExtensionResourceUID
}

// checkAliasBatchConsistency rejects, in pure Go and with zero SQL,
// aliases that contradict each other purely within this one batch --
// the same two invariants resource_alias_claims' pair of unique
// constraints enforce against *pre-existing* rows (see the
// migration's doc comment): the same (namespace, key, value) claimed
// by two different targets, or the same target given two different
// values for the same (namespace, key). See the Postgres sibling's
// identical helper for the full rationale (SQLite has no writable
// CTEs, so a contradiction introduced entirely within one batch would
// otherwise either slip through silently or raise a hard
// constraint-violation error here just the same as there).
func checkAliasBatchConsistency(candidates []aliasCandidateInput) (safe []aliasCandidateInput, conflicts []domain.AliasConflict) {
	type valueKey struct {
		ns    domain.AliasNamespace
		key   domain.AliasKey
		value domain.AliasValue
	}
	type resourceKey struct {
		ns     domain.AliasNamespace
		key    domain.AliasKey
		target domain.ResourceName
	}

	ownerByValue := make(map[valueKey]domain.ResourceName, len(candidates))
	valueByResource := make(map[resourceKey]domain.AliasValue, len(candidates))

	for _, c := range candidates {
		vk := valueKey{c.alias.Namespace, c.alias.Key, c.alias.Value}
		rk := resourceKey{c.alias.Namespace, c.alias.Key, c.target}

		if owner, ok := ownerByValue[vk]; ok && owner != c.target {
			conflicts = append(conflicts, domain.AliasConflict{
				Alias:      c.alias,
				Kind:       domain.AliasConflictValueClaimedByOther,
				TargetName: c.target,
				ActualName: owner,
			})
			continue
		}
		if val, ok := valueByResource[rk]; ok && val != c.alias.Value {
			conflicts = append(conflicts, domain.AliasConflict{
				Alias:       c.alias,
				Kind:        domain.AliasConflictResourceHasDifferentValue,
				TargetName:  c.target,
				ActualValue: val,
			})
			continue
		}

		ownerByValue[vk] = c.target
		valueByResource[rk] = c.alias.Value
		safe = append(safe, c)
	}
	return safe, conflicts
}

// selfClaimKey identifies a candidate's own (source, namespace, key)
// -- [ExtensionResourceRepo.resolveAliasesForWrite]'s map key for
// "what claim does this exact contributor currently assert for this
// key," read from resource_alias_contributions joined to
// resource_alias_claims. The SQLite equivalent of the Postgres
// sibling's aliasFoldCTEs self_claim CTE.
type selfClaimKey struct {
	sourceUID domain.ExtensionResourceUID
	ns        domain.AliasNamespace
	key       domain.AliasKey
}

// selfClaimState is the claim a candidate's own pre-existing
// contribution (if any) currently points to.
type selfClaimState struct {
	claimID                           int64
	value, collectionName, resourceID string
}

// valueClaimKey identifies a claim by its (namespace, key, value) --
// UNIQUE(namespace, key, value) (see the migration's doc comment)
// guarantees at most one resource_alias_claims row can ever match.
type valueClaimKey struct {
	ns    domain.AliasNamespace
	key   domain.AliasKey
	value domain.AliasValue
}

type valueClaimState struct {
	claimID                    int64
	collectionName, resourceID string
}

// resourceClaimKey identifies a claim by its (namespace, key, target)
// -- UNIQUE(namespace, key, platform_collection_name,
// platform_resource_id) (see the migration's doc comment) guarantees
// at most one resource_alias_claims row can ever match.
type resourceClaimKey struct {
	ns     domain.AliasNamespace
	key    domain.AliasKey
	target domain.ResourceName
}

type resourceClaimState struct {
	claimID       int64
	value         string
	platformOwned bool
}

// aliasWritePlan is [ExtensionResourceRepo.resolveAliasesForWrite]'s
// output: every changed, non-conflicting candidate classified into
// exactly one of the three disjoint write shapes the Postgres
// sibling's aliasFoldCTEs doc comment derives a closed-form argument
// for (see that constant's doc comment -- the same argument applies
// unchanged here, since it rests purely on the schema's uniqueness
// constraints and aliases being self-referential, neither of which
// differ between backends).
type aliasWritePlan struct {
	creates      []aliasCandidateInput
	selfReplaces []selfReplaceWrite
	reuses       []aliasContributionWrite
}

// selfReplaceWrite is a claim_self_replace candidate: the contributor
// already owns claimID outright (see aliasFoldCTEs' doc comment for
// why this can never be a different claim than its own prior one),
// and is just changing its value in place.
type selfReplaceWrite struct {
	claimID int64
	value   domain.AliasValue
}

// aliasContributionWrite is a claim_reuse candidate: a brand-new
// contribution (this contributor had none for this (namespace, key)
// before) joining a claim some other contributor -- or the platform,
// via platform_owned -- already established for this exact target.
type aliasContributionWrite struct {
	sourceUID  domain.ExtensionResourceUID
	ns         domain.AliasNamespace
	key        domain.AliasKey
	claimID    int64
	receivedAt time.Time
}

// resolveAliasesForWrite runs checkAliasBatchConsistency, then
// classifies the survivors against pre-existing resource_alias_claims/
// resource_alias_contributions state via three batch reads --
// self-claim (the fast no-op path), by-value, and by-resource --
// mirroring the Postgres sibling's aliasFoldCTEs exactly (see that
// constant's doc comment for the full reasoning, which applies
// unchanged: SQLite has no writable CTEs to fold these into one
// statement, so this costs its own round trips instead, in exchange
// for the same graceful [domain.AliasConflict] behavior rather than a
// hard constraint-violation error).
func (r *ExtensionResourceRepo) resolveAliasesForWrite(ctx context.Context, candidates []aliasCandidateInput) (aliasWritePlan, []domain.AliasConflict, error) {
	safe, conflicts := checkAliasBatchConsistency(candidates)
	if len(safe) == 0 {
		return aliasWritePlan{}, conflicts, nil
	}

	selfClaims, err := r.batchReadSelfClaims(ctx, safe)
	if err != nil {
		return aliasWritePlan{}, nil, err
	}

	var changed []aliasCandidateInput
	for _, c := range safe {
		sc, ok := selfClaims[selfClaimKey{c.sourceUID, c.alias.Namespace, c.alias.Key}]
		if ok && sc.value == string(c.alias.Value) &&
			sc.collectionName == string(c.target.Collection()) && sc.resourceID == string(c.target.ID()) {
			continue
		}
		changed = append(changed, c)
	}
	if len(changed) == 0 {
		return aliasWritePlan{}, conflicts, nil
	}

	byValue, err := r.batchReadClaimsByValue(ctx, changed)
	if err != nil {
		return aliasWritePlan{}, nil, err
	}
	byResource, err := r.batchReadClaimsByResource(ctx, changed)
	if err != nil {
		return aliasWritePlan{}, nil, err
	}
	resourceClaimIDs := make([]int64, 0, len(byResource))
	for _, rc := range byResource {
		resourceClaimIDs = append(resourceClaimIDs, rc.claimID)
	}
	contributorsByClaim, err := r.batchReadContributorsByClaim(ctx, resourceClaimIDs)
	if err != nil {
		return aliasWritePlan{}, nil, err
	}

	var plan aliasWritePlan
	for _, c := range changed {
		vk := valueClaimKey{c.alias.Namespace, c.alias.Key, c.alias.Value}
		if vc, ok := byValue[vk]; ok {
			if vc.collectionName != string(c.target.Collection()) || vc.resourceID != string(c.target.ID()) {
				conflicts = append(conflicts, domain.AliasConflict{
					Alias: c.alias, Kind: domain.AliasConflictValueClaimedByOther, TargetName: c.target,
					ActualName: domain.ResourceName(vc.collectionName + "/" + vc.resourceID),
				})
				continue
			}
			// Value already belongs to this target, from any
			// contributor: a corroboration -- this contributor had
			// no prior claim for the key at all (see aliasFoldCTEs'
			// doc comment for why that's the only way to reach
			// here), so it's a brand-new contribution joining an
			// existing claim.
			plan.reuses = append(plan.reuses, aliasContributionWrite{
				sourceUID: c.sourceUID, ns: c.alias.Namespace, key: c.alias.Key, claimID: vc.claimID, receivedAt: c.receivedAt,
			})
			continue
		}

		rk := resourceClaimKey{c.alias.Namespace, c.alias.Key, c.target}
		rc, ok := byResource[rk]
		if !ok {
			plan.creates = append(plan.creates, c)
			continue
		}
		siblingHolds := rc.platformOwned
		if !siblingHolds {
			for sourceUID := range contributorsByClaim[rc.claimID] {
				if sourceUID != c.sourceUID.String() {
					siblingHolds = true
					break
				}
			}
		}
		if siblingHolds {
			conflicts = append(conflicts, domain.AliasConflict{
				Alias: c.alias, Kind: domain.AliasConflictResourceHasDifferentValue, TargetName: c.target, ActualValue: domain.AliasValue(rc.value),
			})
			continue
		}
		// This target's own claim for the key, held by nobody else
		// (extension resource or platform) -- free to change its
		// value in place.
		plan.selfReplaces = append(plan.selfReplaces, selfReplaceWrite{claimID: rc.claimID, value: c.alias.Value})
	}
	return plan, conflicts, nil
}

// batchReadSelfClaims reads every candidate's own current
// resource_alias_contributions row, joined to its claim, in one
// round trip.
func (r *ExtensionResourceRepo) batchReadSelfClaims(ctx context.Context, candidates []aliasCandidateInput) (map[selfClaimKey]selfClaimState, error) {
	placeholders := make([]string, len(candidates))
	args := make([]any, 0, len(candidates)*3)
	for i, c := range candidates {
		placeholders[i] = "(?, ?, ?)"
		args = append(args, c.sourceUID.String(), string(c.alias.Namespace), string(c.alias.Key))
	}
	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT c.source_extension_resource_uid, c.namespace, c.key, cl.id, cl.value, cl.platform_collection_name, cl.platform_resource_id
			FROM resource_alias_contributions c
			JOIN resource_alias_claims cl ON cl.id = c.claim_id
			WHERE (c.source_extension_resource_uid, c.namespace, c.key) IN (%s)`, strings.Join(placeholders, ", ")),
		args...)
	if err != nil {
		return nil, fmt.Errorf("read self claims: %w", err)
	}
	defer rows.Close()

	result := make(map[selfClaimKey]selfClaimState, len(candidates))
	for rows.Next() {
		var sourceUIDStr, ns, key, value, collectionName, resourceID string
		var claimID int64
		if err := rows.Scan(&sourceUIDStr, &ns, &key, &claimID, &value, &collectionName, &resourceID); err != nil {
			return nil, fmt.Errorf("scan self claim: %w", err)
		}
		sourceUID, err := domain.ParseExtensionResourceUID(sourceUIDStr)
		if err != nil {
			return nil, fmt.Errorf("parse self claim source uid: %w", err)
		}
		result[selfClaimKey{sourceUID, domain.AliasNamespace(ns), domain.AliasKey(key)}] = selfClaimState{
			claimID: claimID, value: value, collectionName: collectionName, resourceID: resourceID,
		}
	}
	return result, rows.Err()
}

// batchReadClaimsByValue is the SQLite equivalent of the Postgres
// sibling's aliasFoldCTEs by_value CTE: for every changed candidate's
// (namespace, key, value), is there already a claim, and for whom.
func (r *ExtensionResourceRepo) batchReadClaimsByValue(ctx context.Context, changed []aliasCandidateInput) (map[valueClaimKey]valueClaimState, error) {
	placeholders := make([]string, len(changed))
	args := make([]any, 0, len(changed)*3)
	for i, c := range changed {
		placeholders[i] = "(?, ?, ?)"
		args = append(args, string(c.alias.Namespace), string(c.alias.Key), string(c.alias.Value))
	}
	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT namespace, key, value, id, platform_collection_name, platform_resource_id
			FROM resource_alias_claims WHERE (namespace, key, value) IN (%s)`, strings.Join(placeholders, ", ")),
		args...)
	if err != nil {
		return nil, fmt.Errorf("read claims by value: %w", err)
	}
	defer rows.Close()

	result := make(map[valueClaimKey]valueClaimState, len(changed))
	for rows.Next() {
		var ns, key, value, collectionName, resourceID string
		var id int64
		if err := rows.Scan(&ns, &key, &value, &id, &collectionName, &resourceID); err != nil {
			return nil, fmt.Errorf("scan claim by value: %w", err)
		}
		result[valueClaimKey{domain.AliasNamespace(ns), domain.AliasKey(key), domain.AliasValue(value)}] = valueClaimState{
			claimID: id, collectionName: collectionName, resourceID: resourceID,
		}
	}
	return result, rows.Err()
}

// batchReadClaimsByResource is the SQLite equivalent of the Postgres
// sibling's aliasFoldCTEs by_resource CTE: for every changed
// candidate's own (namespace, key, target), is there already a claim.
func (r *ExtensionResourceRepo) batchReadClaimsByResource(ctx context.Context, changed []aliasCandidateInput) (map[resourceClaimKey]resourceClaimState, error) {
	placeholders := make([]string, len(changed))
	args := make([]any, 0, len(changed)*4)
	for i, c := range changed {
		placeholders[i] = "(?, ?, ?, ?)"
		args = append(args, string(c.alias.Namespace), string(c.alias.Key), string(c.target.Collection()), string(c.target.ID()))
	}
	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT namespace, key, platform_collection_name, platform_resource_id, id, value, platform_owned
			FROM resource_alias_claims WHERE (namespace, key, platform_collection_name, platform_resource_id) IN (%s)`, strings.Join(placeholders, ", ")),
		args...)
	if err != nil {
		return nil, fmt.Errorf("read claims by resource: %w", err)
	}
	defer rows.Close()

	result := make(map[resourceClaimKey]resourceClaimState, len(changed))
	for rows.Next() {
		var ns, key, collectionName, resourceID, value string
		var id int64
		var platformOwned bool
		if err := rows.Scan(&ns, &key, &collectionName, &resourceID, &id, &value, &platformOwned); err != nil {
			return nil, fmt.Errorf("scan claim by resource: %w", err)
		}
		rk := resourceClaimKey{domain.AliasNamespace(ns), domain.AliasKey(key), domain.ResourceName(collectionName + "/" + resourceID)}
		result[rk] = resourceClaimState{claimID: id, value: value, platformOwned: platformOwned}
	}
	return result, rows.Err()
}

// batchReadContributorsByClaim reads every resource_alias_contributions
// row for claimIDs, in one round trip, so resolveAliasesForWrite can
// tell "some other contributor (or the platform) still holds this
// claim" from "only this candidate's own contributor ever did"
// without a query per candidate.
func (r *ExtensionResourceRepo) batchReadContributorsByClaim(ctx context.Context, claimIDs []int64) (map[int64]map[string]bool, error) {
	result := make(map[int64]map[string]bool, len(claimIDs))
	if len(claimIDs) == 0 {
		return result, nil
	}
	placeholders := make([]string, len(claimIDs))
	args := make([]any, len(claimIDs))
	for i, id := range claimIDs {
		placeholders[i] = "?"
		args[i] = id
	}
	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT claim_id, source_extension_resource_uid FROM resource_alias_contributions WHERE claim_id IN (%s)`,
			strings.Join(placeholders, ", ")),
		args...)
	if err != nil {
		return nil, fmt.Errorf("read contributors by claim: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var claimID int64
		var sourceUID string
		if err := rows.Scan(&claimID, &sourceUID); err != nil {
			return nil, fmt.Errorf("scan contributor by claim: %w", err)
		}
		if result[claimID] == nil {
			result[claimID] = make(map[string]bool)
		}
		result[claimID][sourceUID] = true
	}
	return result, rows.Err()
}

// writeAliases writes an [aliasWritePlan] resolveAliasesForWrite has
// already proven safe: claim_creates get a brand-new
// resource_alias_claims row, claim_self_replace updates a
// contributor's own claim's value in place (no contribution write
// needed -- the contribution already points at that same claim id;
// see aliasFoldCTEs' doc comment), and claim_reuse joins an existing
// claim via a fresh resource_alias_contributions row. Creates and
// reuses both land in one final contributions upsert.
func (r *ExtensionResourceRepo) writeAliases(ctx context.Context, plan aliasWritePlan) error {
	if len(plan.creates) == 0 && len(plan.selfReplaces) == 0 && len(plan.reuses) == 0 {
		return nil
	}

	type claimKey struct{ ns, key, value string }
	uniqueCreates := make(map[claimKey]aliasCandidateInput, len(plan.creates))
	for _, c := range plan.creates {
		uniqueCreates[claimKey{string(c.alias.Namespace), string(c.alias.Key), string(c.alias.Value)}] = c
	}

	claimIDByValue := make(map[claimKey]int64, len(uniqueCreates))
	if len(uniqueCreates) > 0 {
		insertPlaceholders := make([]string, 0, len(uniqueCreates))
		insertArgs := make([]any, 0, len(uniqueCreates)*6)
		selectPlaceholders := make([]string, 0, len(uniqueCreates))
		selectArgs := make([]any, 0, len(uniqueCreates)*3)
		for k, c := range uniqueCreates {
			ts := c.receivedAt.UTC().Format(time.RFC3339Nano)
			insertPlaceholders = append(insertPlaceholders, "(?, ?, ?, ?, ?, ?)")
			insertArgs = append(insertArgs, k.ns, k.key, k.value, string(c.target.Collection()), string(c.target.ID()), ts)
			selectPlaceholders = append(selectPlaceholders, "(?, ?, ?)")
			selectArgs = append(selectArgs, k.ns, k.key, k.value)
		}
		if _, err := r.DB.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO resource_alias_claims (namespace, key, value, platform_collection_name, platform_resource_id, created_at)
				VALUES %s`, strings.Join(insertPlaceholders, ", ")),
			insertArgs...); err != nil {
			return fmt.Errorf("insert alias claims: %w", err)
		}

		rows, err := r.DB.QueryContext(ctx,
			fmt.Sprintf(`SELECT namespace, key, value, id FROM resource_alias_claims WHERE (namespace, key, value) IN (%s)`,
				strings.Join(selectPlaceholders, ", ")),
			selectArgs...)
		if err != nil {
			return fmt.Errorf("resolve newly created alias claims: %w", err)
		}
		for rows.Next() {
			var ns, key, value string
			var id int64
			if err := rows.Scan(&ns, &key, &value, &id); err != nil {
				rows.Close()
				return fmt.Errorf("scan newly created alias claim: %w", err)
			}
			claimIDByValue[claimKey{ns, key, value}] = id
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return fmt.Errorf("resolve newly created alias claims: %w", err)
		}
		rows.Close()
	}

	for _, sr := range plan.selfReplaces {
		if _, err := r.DB.ExecContext(ctx,
			`UPDATE resource_alias_claims SET value = ? WHERE id = ?`, string(sr.value), sr.claimID,
		); err != nil {
			return fmt.Errorf("self-replace alias claim: %w", err)
		}
	}

	type contributionKey struct{ sourceUID, ns, key string }
	type contributionRow struct {
		sourceUID, ns, key, receivedAt string
		claimID                        int64
	}
	// Deduplicated by (sourceUID, ns, key), the only key INSERT below
	// cares about being unique: the same alias reported twice within
	// one report (see DuplicateAliasWithinReportIsNotAConflict) can
	// legitimately produce two identical plan.creates/plan.reuses
	// entries for the same contributor's same key, and unlike
	// Postgres's SELECT DISTINCT ON in upserted_contributions, nothing
	// upstream of this map already collapses that. Iteration order
	// over creates-then-reuses doesn't matter for correctness here --
	// duplicates always carry the same claim id (see the comment
	// below on why this is a plain INSERT).
	contributionsByKey := make(map[contributionKey]contributionRow, len(plan.creates)+len(plan.reuses))
	for _, c := range plan.creates {
		id, ok := claimIDByValue[claimKey{string(c.alias.Namespace), string(c.alias.Key), string(c.alias.Value)}]
		if !ok {
			return fmt.Errorf("resolve newly created alias claim: %s/%s/%s", c.alias.Namespace, c.alias.Key, c.alias.Value)
		}
		contributionsByKey[contributionKey{c.sourceUID.String(), string(c.alias.Namespace), string(c.alias.Key)}] = contributionRow{
			sourceUID: c.sourceUID.String(), ns: string(c.alias.Namespace), key: string(c.alias.Key),
			claimID: id, receivedAt: c.receivedAt.UTC().Format(time.RFC3339Nano),
		}
	}
	for _, ru := range plan.reuses {
		contributionsByKey[contributionKey{ru.sourceUID.String(), string(ru.ns), string(ru.key)}] = contributionRow{
			sourceUID: ru.sourceUID.String(), ns: string(ru.ns), key: string(ru.key),
			claimID: ru.claimID, receivedAt: ru.receivedAt.UTC().Format(time.RFC3339Nano),
		}
	}
	if len(contributionsByKey) > 0 {
		placeholders := make([]string, 0, len(contributionsByKey))
		args := make([]any, 0, len(contributionsByKey)*5)
		for _, c := range contributionsByKey {
			placeholders = append(placeholders, "(?, ?, ?, ?, ?)")
			args = append(args, c.sourceUID, c.ns, c.key, c.claimID, c.receivedAt)
		}
		// Plain INSERT, not an upsert: resolveAliasesForWrite only
		// ever places a candidate in plan.creates/plan.reuses when
		// its self_claim lookup found no pre-existing contribution
		// row for (sourceUID, ns, key) -- one with an existing
		// contribution always routes through plan.selfReplaces
		// instead, which never touches resource_alias_contributions
		// (see writeAliases's own doc comment, mirroring aliasFoldCTEs'
		// closed-form argument on the Postgres side). So every row
		// here is, by construction, for a key with no pre-existing
		// row, and an ON CONFLICT ... DO UPDATE SET claim_id would
		// only ever fire if that invariant were violated by a future
		// bug -- silently moving a contribution's claim_id that way
		// is exactly the kind of change that needs compensating
		// cleanup on the claim it moved *from*, which nothing here
		// would perform. Letting the insert fail loudly on a real
		// primary-key collision surfaces that bug immediately instead
		// of quietly orphaning a claim with no error at all.
		if _, err := r.DB.ExecContext(ctx,
			fmt.Sprintf(`INSERT INTO resource_alias_contributions (source_extension_resource_uid, namespace, key, claim_id, created_at)
				VALUES %s`, strings.Join(placeholders, ", ")),
			args...); err != nil {
			return fmt.Errorf("insert alias contributions: %w", err)
		}
	}
	return nil
}

// deleteOrphanedClaims checks each of claimIDs (deduplicated) for
// remaining resource_alias_contributions, deleting the claim itself
// if none remain and it isn't platform_owned. Unlike the Postgres
// sibling's single-statement refcount algebra (needed there to work
// around one statement's snapshot seeing every CTE's *pre-statement*
// state uniformly, regardless of execution order -- see
// aliasRetractAbsentCTE's doc comment), SQLite has no writable CTEs
// to begin with, so every write here is already its own separate
// statement in the same transaction; a plain COUNT read after a prior
// statement's DELETE simply sees that delete's effect directly, with
// no algebra needed to compensate for isolation this code was never
// going to have anyway. Shared by
// [ExtensionResourceRepo.retractAbsentAliases] (ReplaceInventory) and
// [ExtensionResourceRepo.Delete].
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

// retractAbsentAliases implements ReplaceInventory's "absence
// retracts" half of the per-contributor replace contract
// [domain.InventoryReplacement.Aliases] documents -- see the Postgres
// sibling's aliasRetractAbsentCTE for the full reasoning behind
// *what* gets retracted, which applies here unchanged: reported
// carries every (uid, namespace, key) pair originally in this
// chunk's reports, before checkAliasBatchConsistency's Go-side
// filtering, so a key rejected as an intra-batch contradiction still
// counts as "reported" and isn't retracted out from under a good
// prior value. uids is every resource in the call that
// needsAliasProcessing this round -- see ReplaceInventory's own doc
// comment on the alias_fingerprint fast path -- including ones with
// zero reported aliases, whose existing aliases -- if any -- are
// entirely retracted, per the same "absence means empty" rule
// labels/conditions already follow. A resource excluded from uids
// because its fingerprint matched is, by construction, one whose
// alias set hasn't changed at all, so skipping it here is exactly as
// safe as skipping it in aliasCandidates above.
//
// Deleting a contribution can orphan its claim, so this also cleans
// up via [ExtensionResourceRepo.deleteOrphanedClaims] -- see that
// method's doc comment for why SQLite needs no refcount algebra to
// do so correctly, unlike the Postgres sibling's single-statement
// aliasRetractAbsentCTE. RETURNING claim_id off the DELETE is what
// lets this find exactly the claims this call's own retraction
// touched, without a separate read.
//
// ApplyInventoryDeltas has no counterpart: [domain.InventoryDelta.UpsertAliases]
// is additive-only, so a delta never calls this for its own upserts
// (DeleteAliases/ReplaceAliases aren't folded into ApplyInventoryDeltas
// yet at all -- see its loop building aliasCandidates below).
func (r *ExtensionResourceRepo) retractAbsentAliases(ctx context.Context, uids []domain.ExtensionResourceUID, reported []resourceAliasKeyByUID) error {
	if len(uids) == 0 {
		return nil
	}
	uidPlaceholders := make([]string, len(uids))
	uidArgs := make([]any, len(uids))
	for i, u := range uids {
		uidPlaceholders[i] = "?"
		uidArgs[i] = u.String()
	}

	var rows *sql.Rows
	var err error
	if len(reported) == 0 {
		rows, err = r.DB.QueryContext(ctx,
			fmt.Sprintf(`DELETE FROM resource_alias_contributions WHERE source_extension_resource_uid IN (%s) RETURNING claim_id`,
				strings.Join(uidPlaceholders, ", ")),
			uidArgs...)
	} else {
		keepPlaceholders := make([]string, len(reported))
		keepArgs := make([]any, 0, len(reported)*3)
		for i, k := range reported {
			keepPlaceholders[i] = "(?, ?, ?)"
			keepArgs = append(keepArgs, k.uid.String(), string(k.ns), string(k.key))
		}
		args := append(append([]any{}, uidArgs...), keepArgs...)
		rows, err = r.DB.QueryContext(ctx,
			fmt.Sprintf(`DELETE FROM resource_alias_contributions
				WHERE source_extension_resource_uid IN (%s)
				  AND (source_extension_resource_uid, namespace, key) NOT IN (%s)
				RETURNING claim_id`,
				strings.Join(uidPlaceholders, ", "), strings.Join(keepPlaceholders, ", ")),
			args...)
	}
	if err != nil {
		return fmt.Errorf("retract absent aliases: %w", err)
	}

	var touchedClaimIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan retracted alias claim id: %w", err)
		}
		touchedClaimIDs = append(touchedClaimIDs, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("retract absent aliases: %w", err)
	}
	rows.Close()

	return r.deleteOrphanedClaims(ctx, touchedClaimIDs)
}

// resourceAliasKeyByUID is a flattened (contributor, namespace, key)
// input row for [ExtensionResourceRepo.retractAbsentAliases]'s keep
// set -- see that method's doc comment for why it's built from every
// reported alias, not just the ones checkAliasBatchConsistency lets
// through.
type resourceAliasKeyByUID struct {
	uid domain.ExtensionResourceUID
	ns  domain.AliasNamespace
	key domain.AliasKey
}

// ReplaceInventory implements [domain.ExtensionResourceRepository.ReplaceInventory]
// as a fixed number of round trips for the whole batch: it first
// resolves-or-creates every replacement's extension_resources row by
// natural key (resolveOrCreateExtensionResources), then flattens every
// replacement's labels/conditions/aliases into the shared slices the
// batch primitives above expect and calls each primitive exactly
// once. See the nameless-platform-identity plan's cost-model section
// for why this is one more round trip than the Postgres sibling
// (SQLite has no writable CTEs to fold the resolution into).
//
// Aliases go through extension_resources.alias_fingerprint's fast
// path first (see resolveOrCreateExtensionResources's doc comment and
// the migration's doc comment on that column, plus the Postgres
// sibling's aliasFingerprintGateCTEs/aliasFingerprintWriteCTE for the
// SQL-side equivalent this mirrors in Go): a replacement whose
// complete Aliases hashes the same as storedAliasFingerprints[i]
// hasn't changed since the last time this resource's aliases were
// fully, successfully applied, so it's excluded from aliasCandidates/
// reportedAliasKeys entirely rather than reclassified against the
// database for no behavioral change. needsAliasProcessing tracks
// which replacements were *not* skipped, both to scope
// retractAbsentAliases to only them (a fingerprint-matched
// replacement supplying zero aliasCandidates rows must never be
// mistaken for "this resource now has zero aliases") and to know
// which ones are eligible for a fingerprint write-back afterward.
func (r *ExtensionResourceRepo) ReplaceInventory(ctx context.Context, replacements []domain.InventoryReplacement) ([]domain.AliasConflict, error) {
	if len(replacements) == 0 {
		return nil, nil
	}

	resourceTypes := make([]domain.ResourceType, len(replacements))
	names := make([]domain.ResourceName, len(replacements))
	candidateUIDs := make([]domain.ExtensionResourceUID, len(replacements))
	receivedAts := make([]time.Time, len(replacements))
	for i, rep := range replacements {
		resourceTypes[i] = rep.ResourceType
		names[i] = rep.Name
		candidateUIDs[i] = rep.CandidateUID
		receivedAts[i] = rep.ReceivedAt
	}
	uids, storedAliasFingerprints, err := r.resolveOrCreateExtensionResources(ctx, resourceTypes, names, candidateUIDs, receivedAts)
	if err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}

	latestItems := make([]latestRowInput, len(replacements))
	var labels []labelTriple
	var conditionKeeps []conditionKey
	var conditionItems []conditionInput
	var aliasCandidates []aliasCandidateInput
	// reportedAliasKeys feeds retractAbsentAliases's keep set and,
	// unlike aliasCandidates, is never filtered by
	// checkAliasBatchConsistency -- see retractAbsentAliases's doc
	// comment for why a candidate this batch goes on to reject must
	// still count as "reported" for retraction purposes.
	var reportedAliasKeys []resourceAliasKeyByUID
	// aliasProcessingUIDs is retractAbsentAliases's uids parameter,
	// scoped to only the replacements needsAliasProcessing[i] left
	// in play -- see this method's own doc comment.
	var aliasProcessingUIDs []domain.ExtensionResourceUID
	reportedAliasFingerprints := make([][]byte, len(replacements))
	needsAliasProcessing := make([]bool, len(replacements))

	for i, rep := range replacements {
		latestItems[i] = latestRowInput{
			uid:         uids[i],
			observation: normalizeObservation(rep.Observation),
			observedAt:  rep.ObservedAt,
			updatedAt:   rep.ReceivedAt,
		}
		for k, v := range rep.Labels {
			labels = append(labels, labelTriple{uid: uids[i], key: k, value: v})
		}
		for _, c := range rep.Conditions {
			conditionKeeps = append(conditionKeeps, conditionKey{uid: uids[i], condType: c.Type()})
			conditionItems = append(conditionItems, conditionInput{
				uid: uids[i], condType: c.Type(), status: c.Status(), reason: c.Reason(), message: c.Message(),
				lastTransitionTime: c.LastTransitionTime(), observedAt: rep.ObservedAt, updatedAt: rep.ReceivedAt,
			})
		}

		reportedAliasFingerprints[i] = domain.AliasSetFingerprint(rep.Aliases)
		needsAliasProcessing[i] = !bytes.Equal(reportedAliasFingerprints[i], storedAliasFingerprints[i])
		if !needsAliasProcessing[i] {
			continue
		}
		aliasProcessingUIDs = append(aliasProcessingUIDs, uids[i])
		for _, a := range rep.Aliases {
			aliasCandidates = append(aliasCandidates, aliasCandidateInput{idx: i, target: rep.Name, alias: a, receivedAt: rep.ReceivedAt, sourceUID: uids[i]})
			reportedAliasKeys = append(reportedAliasKeys, resourceAliasKeyByUID{uid: uids[i], ns: a.Namespace, key: a.Key})
		}
	}

	if err := r.batchUpsertLatestRows(ctx, latestItems); err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}
	if err := r.batchReplaceLabels(ctx, uids, labels); err != nil {
		return nil, fmt.Errorf("replace inventory labels: %w", err)
	}
	if err := r.batchDeleteConditionsAbsentFrom(ctx, uids, conditionKeeps); err != nil {
		return nil, fmt.Errorf("delete absent conditions: %w", err)
	}
	if err := r.batchRecordConditions(ctx, conditionItems); err != nil {
		return nil, fmt.Errorf("record conditions: %w", err)
	}

	plan, conflicts, err := r.resolveAliasesForWrite(ctx, aliasCandidates)
	if err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}
	if err := r.writeAliases(ctx, plan); err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}
	if err := r.retractAbsentAliases(ctx, aliasProcessingUIDs, reportedAliasKeys); err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}

	if err := r.updateAliasFingerprints(ctx, fingerprintUpdatesFor(replacements, uids, reportedAliasFingerprints, needsAliasProcessing, conflicts)); err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}
	return conflicts, nil
}

// fingerprintUpdate is a single [ExtensionResourceRepo.updateAliasFingerprints]
// write: a resource whose complete alias set should be remembered
// under its fingerprint's hash so a later, byte-for-byte identical
// report can skip alias reclassification (see ReplaceInventory's own
// doc comment).
type fingerprintUpdate struct {
	uid         domain.ExtensionResourceUID
	fingerprint []byte
}

// fingerprintUpdatesFor computes ReplaceInventory's fingerprint
// write-back set: replacement i is included only if
// needsAliasProcessing[i] (a fingerprint-matched replacement already
// has the right value stored -- nothing to write) and none of its own
// reported aliases appear in conflicts.
//
// Excluding a conflicted replacement mirrors the Postgres sibling's
// aliasFingerprintWriteCTE, which gates fingerprint_updates on zero
// conflicts for the same idx: writing the fingerprint anyway would
// let a later, still-conflicting repeat of the exact same report
// compare equal to the (wrongly) stored fingerprint and skip
// reprocessing entirely, silently swallowing a conflict that's still
// true. See RepeatedReportWithPersistentAliasConflictKeepsReturningConflict.
//
// Correlates conflicts back to their originating replacement by
// (Alias, TargetName) rather than by index, since neither
// [domain.AliasConflict] nor resolveAliasesForWrite's return value
// carries the original replacement index back out -- safe because a
// replacement's own (alias, target) pair can only ever produce a
// conflict naming that same target, whether the conflict came from
// checkAliasBatchConsistency's intra-batch check or
// resolveAliasesForWrite's database-backed one.
func fingerprintUpdatesFor(
	replacements []domain.InventoryReplacement,
	uids []domain.ExtensionResourceUID,
	reportedAliasFingerprints [][]byte,
	needsAliasProcessing []bool,
	conflicts []domain.AliasConflict,
) []fingerprintUpdate {
	type conflictKey struct {
		alias  domain.Alias
		target domain.ResourceName
	}
	conflicted := make(map[conflictKey]bool, len(conflicts))
	for _, c := range conflicts {
		conflicted[conflictKey{c.Alias, c.TargetName}] = true
	}

	var updates []fingerprintUpdate
	for i, rep := range replacements {
		if !needsAliasProcessing[i] {
			continue
		}
		hasConflict := false
		for _, a := range rep.Aliases {
			if conflicted[conflictKey{a, rep.Name}] {
				hasConflict = true
				break
			}
		}
		if hasConflict {
			continue
		}
		updates = append(updates, fingerprintUpdate{uid: uids[i], fingerprint: reportedAliasFingerprints[i]})
	}
	return updates
}

// updateAliasFingerprints writes each update's reported alias
// fingerprint to its resource's extension_resources.alias_fingerprint
// column, completing ReplaceInventory's fast path (see that method's
// own doc comment and fingerprintUpdatesFor for how updates is
// computed). Looped rather than batched into one statement, like
// writeAliases's plan.selfReplaces above -- SQLite has no "UPDATE
// different rows to different values" primitive as cheap as the
// single-row form, and this set is already bounded by how many
// replacements in this one call actually needed alias processing.
func (r *ExtensionResourceRepo) updateAliasFingerprints(ctx context.Context, updates []fingerprintUpdate) error {
	for _, u := range updates {
		if _, err := r.DB.ExecContext(ctx,
			`UPDATE extension_resources SET alias_fingerprint = ? WHERE uid = ?`,
			u.fingerprint, u.uid.String(),
		); err != nil {
			return fmt.Errorf("update alias fingerprint: %w", err)
		}
	}
	return nil
}

// ApplyInventoryDeltas implements [domain.ExtensionResourceRepository.ApplyInventoryDeltas]
// as a fixed number of round trips for the whole batch, following the
// same natural-key-resolve-then-flatten shape as ReplaceInventory
// above.
func (r *ExtensionResourceRepo) ApplyInventoryDeltas(ctx context.Context, deltas []domain.InventoryDelta) ([]domain.AliasConflict, error) {
	if len(deltas) == 0 {
		return nil, nil
	}
	for _, d := range deltas {
		if err := domain.ValidateInventoryDelta(d); err != nil {
			return nil, err
		}
	}

	resourceTypes := make([]domain.ResourceType, len(deltas))
	names := make([]domain.ResourceName, len(deltas))
	candidateUIDs := make([]domain.ExtensionResourceUID, len(deltas))
	receivedAts := make([]time.Time, len(deltas))
	for i, d := range deltas {
		resourceTypes[i] = d.ResourceType
		names[i] = d.Name
		candidateUIDs[i] = d.CandidateUID
		receivedAts[i] = d.ReceivedAt
	}
	uids, _, err := r.resolveOrCreateExtensionResources(ctx, resourceTypes, names, candidateUIDs, receivedAts)
	if err != nil {
		return nil, fmt.Errorf("apply inventory deltas: %w", err)
	}

	latestItems := make([]latestRowInput, len(deltas))
	var setLabels []labelTriple
	var deleteLabels []labelKey
	var upsertConditions []conditionInput
	var deleteConditions []conditionKey
	var aliasCandidates []aliasCandidateInput

	for i, d := range deltas {
		latestItems[i] = latestRowInput{
			uid:         uids[i],
			observation: normalizeObservation(d.Observation),
			observedAt:  d.ObservedAt,
			updatedAt:   d.ReceivedAt,
		}
		for k, v := range d.SetLabels {
			setLabels = append(setLabels, labelTriple{uid: uids[i], key: k, value: v})
		}
		for _, k := range d.DeleteLabels {
			deleteLabels = append(deleteLabels, labelKey{uid: uids[i], key: k})
		}
		for _, c := range d.UpsertConditions {
			upsertConditions = append(upsertConditions, conditionInput{
				uid: uids[i], condType: c.Type(), status: c.Status(), reason: c.Reason(), message: c.Message(),
				lastTransitionTime: c.LastTransitionTime(), observedAt: d.ObservedAt, updatedAt: d.ReceivedAt,
			})
		}
		for _, t := range d.DeleteConditions {
			deleteConditions = append(deleteConditions, conditionKey{uid: uids[i], condType: t})
		}
		// DeleteAliases/ReplaceAliases are not yet folded in here --
		// see [domain.InventoryDelta]'s doc for the target contract
		// and extensionresourcerepotest's delta alias tests, which
		// pin it down ahead of this landing.
		for _, a := range d.UpsertAliases {
			aliasCandidates = append(aliasCandidates, aliasCandidateInput{idx: i, target: d.Name, alias: a, receivedAt: d.ReceivedAt, sourceUID: uids[i]})
		}
	}

	// batchUpsertLatestRows always processes every item -- including
	// deltas with no observation change -- so this both writes
	// observations and doubles as "ensure a latest inventory row
	// exists" for every uid in the batch.
	if err := r.batchUpsertLatestRows(ctx, latestItems); err != nil {
		return nil, fmt.Errorf("apply inventory deltas: %w", err)
	}
	if err := r.batchSetLabels(ctx, setLabels); err != nil {
		return nil, fmt.Errorf("set inventory labels: %w", err)
	}
	if err := r.batchDeleteLabels(ctx, deleteLabels); err != nil {
		return nil, fmt.Errorf("delete inventory labels: %w", err)
	}
	if err := r.batchRecordConditions(ctx, upsertConditions); err != nil {
		return nil, fmt.Errorf("upsert conditions: %w", err)
	}
	if err := r.batchDeleteConditionsByType(ctx, deleteConditions); err != nil {
		return nil, fmt.Errorf("delete conditions: %w", err)
	}

	plan, conflicts, err := r.resolveAliasesForWrite(ctx, aliasCandidates)
	if err != nil {
		return nil, fmt.Errorf("apply inventory deltas: %w", err)
	}
	if err := r.writeAliases(ctx, plan); err != nil {
		return nil, fmt.Errorf("apply inventory deltas: %w", err)
	}

	// touchedUIDs mirrors the Postgres sibling's
	// aliasFingerprintInvalidateCTE scope exactly: every resource
	// with at least one UpsertAliases candidate that survived
	// checkAliasBatchConsistency, regardless of whether
	// resolveAliasesForWrite's own self-claim check above finds it
	// unchanged or it later conflicts there -- see
	// invalidateAliasFingerprints's own doc comment for why
	// over-invalidating is always safe. Computed via its own
	// checkAliasBatchConsistency call (its conflicts are discarded
	// here; resolveAliasesForWrite's internal call above already
	// produced the conflicts this method returns) rather than
	// plumbing resolveAliasesForWrite's plan back out, since a plan
	// only covers *changed* candidates and would under-invalidate one
	// resolveAliasesForWrite's self-claim check finds already up to
	// date.
	safeAliases, _ := checkAliasBatchConsistency(aliasCandidates)
	touchedUIDSet := make(map[domain.ExtensionResourceUID]bool, len(safeAliases))
	for _, c := range safeAliases {
		touchedUIDSet[c.sourceUID] = true
	}
	touchedUIDs := make([]domain.ExtensionResourceUID, 0, len(touchedUIDSet))
	for uid := range touchedUIDSet {
		touchedUIDs = append(touchedUIDs, uid)
	}
	if err := r.invalidateAliasFingerprints(ctx, touchedUIDs); err != nil {
		return nil, fmt.Errorf("apply inventory deltas: %w", err)
	}
	return conflicts, nil
}

// invalidateAliasFingerprints clears alias_fingerprint for every
// resource in uids -- the delta side of extension_resources.
// alias_fingerprint's bookkeeping. ReplaceInventory's
// updateAliasFingerprints is the only place that *sets* a
// fingerprint, so this is the only place that needs to *clear* one: a
// delta that upserts an alias changes what "this resource's complete
// alias set" truthfully is out from under whatever ReplaceInventory
// last fingerprinted, and a later replace reporting that same old set
// again must not mistake it for unchanged (see
// DeltaAddedAliasIsRetractedByLaterReplaceWithOriginalFingerprintedSet).
// Mirrors the Postgres sibling's aliasFingerprintInvalidateCTE, and
// unlike updateAliasFingerprints is batched into one statement since
// every row here gets the same NULL, not a distinct per-row value.
func (r *ExtensionResourceRepo) invalidateAliasFingerprints(ctx context.Context, uids []domain.ExtensionResourceUID) error {
	if len(uids) == 0 {
		return nil
	}
	placeholders := make([]string, len(uids))
	args := make([]any, len(uids))
	for i, u := range uids {
		placeholders[i] = "?"
		args[i] = u.String()
	}
	if _, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`UPDATE extension_resources SET alias_fingerprint = NULL WHERE uid IN (%s)`, strings.Join(placeholders, ", ")),
		args...,
	); err != nil {
		return fmt.Errorf("invalidate alias fingerprints: %w", err)
	}
	return nil
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
