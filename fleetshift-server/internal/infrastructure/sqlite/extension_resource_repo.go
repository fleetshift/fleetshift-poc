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

func (r *ExtensionResourceRepo) Delete(ctx context.Context, name domain.FullResourceName) error {
	relative := name.ResourceName()
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
// The returned slice is in the same order as the input slices.
func (r *ExtensionResourceRepo) resolveOrCreateExtensionResources(
	ctx context.Context,
	resourceTypes []domain.ResourceType, names []domain.ResourceName, candidateUIDs []domain.ExtensionResourceUID, receivedAts []time.Time,
) ([]domain.ExtensionResourceUID, error) {
	n := len(resourceTypes)
	if n == 0 {
		return nil, nil
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
		return nil, fmt.Errorf("resolve-or-create extension resources (insert): %w", err)
	}

	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT service_name, collection_name, resource_id, uid FROM extension_resources
			WHERE (service_name, collection_name, resource_id) IN (%s)`, strings.Join(selectPlaceholders, ", ")),
		selectArgs...,
	)
	if err != nil {
		return nil, fmt.Errorf("resolve-or-create extension resources (resolve): %w", err)
	}
	defer rows.Close()

	resolved := make(map[domain.FullResourceName]domain.ExtensionResourceUID, n)
	for rows.Next() {
		var serviceName, collectionName, resourceID, uidStr string
		if err := rows.Scan(&serviceName, &collectionName, &resourceID, &uidStr); err != nil {
			return nil, fmt.Errorf("scan resolved extension resource: %w", err)
		}
		uid, err := domain.ParseExtensionResourceUID(uidStr)
		if err != nil {
			return nil, fmt.Errorf("parse resolved uid: %w", err)
		}
		fullName := domain.NewFullResourceName(domain.ServiceName(serviceName), domain.ResourceName(collectionName+"/"+resourceID))
		resolved[fullName] = uid
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("resolve-or-create extension resources (resolve): %w", err)
	}

	uids := make([]domain.ExtensionResourceUID, n)
	for i := range resourceTypes {
		fullName := domain.NewFullResourceName(resourceTypes[i].ServiceName(), names[i])
		uid, ok := resolved[fullName]
		if !ok {
			return nil, fmt.Errorf("resolve-or-create extension resources: no result for %s", fullName)
		}
		uids[i] = uid
	}
	return uids, nil
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
// the same two invariants resource_aliases' pair of unique indexes
// enforce against *pre-existing* rows (see the migration's doc
// comment): the same (namespace, key, value) claimed by two different
// targets, or the same target given two different values for the same
// (namespace, key). See the Postgres sibling's identical helper for
// the full rationale (SQLite has no writable CTEs, so a contradiction
// introduced entirely within one batch would otherwise either slip
// through silently or raise a hard constraint-violation error here
// just the same as there).
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

// resourceAliasKey identifies a (namespace, key, target) triple --
// resolveAliasesForWrite's map key for "what does resource_aliases
// currently say about this resource's value for this key."
type resourceAliasKey struct {
	ns     domain.AliasNamespace
	key    domain.AliasKey
	target domain.ResourceName
}

// existingAliasRow is one pre-existing resource_aliases row read back
// by [ExtensionResourceRepo.resolveAliasesForWrite]'s two queries --
// either keyed by (namespace, key, value), where every row sharing
// that key is guaranteed (by this fold-in's own enforcement) to agree
// on target, or keyed by (namespace, key, target), where every row
// sharing that key is guaranteed to agree on value. sourceUID lets
// resolveAliasesForWrite tell "one of these rows is mine" from "every
// one of these rows belongs to some other contributor" without a
// third query.
type existingAliasRow struct {
	target    domain.ResourceName
	sourceUID domain.ExtensionResourceUID
}

// resolveAliasesForWrite runs checkAliasBatchConsistency, then reads
// pre-existing resource_aliases state for the survivors' (namespace,
// key, value) and (namespace, key, target) pairs -- across every
// contributor, not just this candidate's own, since resource_aliases
// can now hold one row per contributor for the very same claim (see
// the migration's resource_aliases doc comment). SQLite has no
// writable CTEs, so this can't fold into one blind INSERT the way the
// Postgres sibling's alias fold-in does -- it costs its own reads
// instead, in exchange for never letting a candidate that would
// violate either invariant reach the INSERT (see writeAliases), which
// is what keeps a genuine cross-contributor disagreement a graceful
// [domain.AliasConflict] instead of a hard constraint-violation error.
//
// A candidate is safe if either read comes back clean for it, per the
// same two-phase logic as the Postgres sibling's aliasFoldCTEs (see
// that constant's doc comment for the full reasoning): phase 1
// (byValue) accepts a candidate whose exact value already belongs to
// its own target, from any contributor (a no-op or a corroboration);
// phase 2 (byResource, only reached once phase 1 proves the value is
// otherwise unclaimed) accepts a candidate replacing a value only its
// own contributor ever held for that key, and rejects one where a
// different contributor holds a different value for it.
func (r *ExtensionResourceRepo) resolveAliasesForWrite(ctx context.Context, candidates []aliasCandidateInput) (safe []aliasCandidateInput, conflicts []domain.AliasConflict, err error) {
	safe, conflicts = checkAliasBatchConsistency(candidates)
	if len(safe) == 0 {
		return safe, conflicts, nil
	}

	valuePlaceholders := make([]string, len(safe))
	valueArgs := make([]any, 0, len(safe)*3)
	resourcePlaceholders := make([]string, len(safe))
	resourceArgs := make([]any, 0, len(safe)*4)
	for i, c := range safe {
		valuePlaceholders[i] = "(?, ?, ?)"
		valueArgs = append(valueArgs, string(c.alias.Namespace), string(c.alias.Key), string(c.alias.Value))
		resourcePlaceholders[i] = "(?, ?, ?, ?)"
		resourceArgs = append(resourceArgs, string(c.alias.Namespace), string(c.alias.Key), string(c.target.Collection()), string(c.target.ID()))
	}

	prevByValue := make(map[domain.Alias][]existingAliasRow, len(safe))
	rows, err := r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT namespace, key, value, platform_collection_name, platform_resource_id, source_extension_resource_uid FROM resource_aliases
			WHERE (namespace, key, value) IN (%s)`, strings.Join(valuePlaceholders, ", ")),
		valueArgs...)
	if err != nil {
		return nil, nil, fmt.Errorf("read existing aliases by value: %w", err)
	}
	for rows.Next() {
		var ns, key, value, collectionName, resourceID string
		var sourceUIDStr sql.NullString
		if err := rows.Scan(&ns, &key, &value, &collectionName, &resourceID, &sourceUIDStr); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("scan existing alias by value: %w", err)
		}
		row, err := scanExistingAliasRow(collectionName, resourceID, sourceUIDStr)
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		a := domain.Alias{Namespace: domain.AliasNamespace(ns), Key: domain.AliasKey(key), Value: domain.AliasValue(value)}
		prevByValue[a] = append(prevByValue[a], row)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, fmt.Errorf("read existing aliases by value: %w", err)
	}
	rows.Close()

	prevByResource := make(map[resourceAliasKey][]existingAliasRow, len(safe))
	prevByResourceValue := make(map[resourceAliasKey]domain.AliasValue, len(safe))
	rows, err = r.DB.QueryContext(ctx,
		fmt.Sprintf(`SELECT namespace, key, platform_collection_name, platform_resource_id, value, source_extension_resource_uid FROM resource_aliases
			WHERE (namespace, key, platform_collection_name, platform_resource_id) IN (%s)`, strings.Join(resourcePlaceholders, ", ")),
		resourceArgs...)
	if err != nil {
		return nil, nil, fmt.Errorf("read existing aliases by resource: %w", err)
	}
	for rows.Next() {
		var ns, key, collectionName, resourceID, value string
		var sourceUIDStr sql.NullString
		if err := rows.Scan(&ns, &key, &collectionName, &resourceID, &value, &sourceUIDStr); err != nil {
			rows.Close()
			return nil, nil, fmt.Errorf("scan existing alias by resource: %w", err)
		}
		row, err := scanExistingAliasRow(collectionName, resourceID, sourceUIDStr)
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		rk := resourceAliasKey{domain.AliasNamespace(ns), domain.AliasKey(key), row.target}
		prevByResource[rk] = append(prevByResource[rk], row)
		prevByResourceValue[rk] = domain.AliasValue(value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, nil, fmt.Errorf("read existing aliases by resource: %w", err)
	}
	rows.Close()

	var stillSafe []aliasCandidateInput
	for _, c := range safe {
		if valueRows := prevByValue[c.alias]; len(valueRows) > 0 {
			// Every row sharing (namespace, key, value) is
			// guaranteed to agree on target -- see
			// checkAliasBatchConsistency and this fold-in's own
			// invariant enforcement -- so valueRows[0] speaks for
			// all of them.
			if owner := valueRows[0].target; owner != c.target {
				conflicts = append(conflicts, domain.AliasConflict{
					Alias: c.alias, Kind: domain.AliasConflictValueClaimedByOther, TargetName: c.target, ActualName: owner,
				})
				continue
			}
			// Value already belongs to this target, from any
			// contributor: a no-op (mine) or a corroboration
			// (a sibling's) -- either way, safe to (re)assert.
			stillSafe = append(stillSafe, c)
			continue
		}

		rk := resourceAliasKey{c.alias.Namespace, c.alias.Key, c.target}
		resourceRows := prevByResource[rk]
		if len(resourceRows) == 0 {
			stillSafe = append(stillSafe, c)
			continue
		}
		siblingHolds := false
		for _, row := range resourceRows {
			if row.sourceUID != c.sourceUID {
				siblingHolds = true
				break
			}
		}
		if siblingHolds {
			conflicts = append(conflicts, domain.AliasConflict{
				Alias: c.alias, Kind: domain.AliasConflictResourceHasDifferentValue, TargetName: c.target, ActualValue: prevByResourceValue[rk],
			})
			continue
		}
		// Every existing row for this (namespace, key, target) is
		// this same contributor's own -- a legitimate replace of a
		// value only it ever held.
		stillSafe = append(stillSafe, c)
	}
	return stillSafe, conflicts, nil
}

// scanExistingAliasRow parses the (target, sourceUID) portion shared
// by resolveAliasesForWrite's two queries.
func scanExistingAliasRow(collectionName, resourceID string, sourceUIDStr sql.NullString) (existingAliasRow, error) {
	row := existingAliasRow{target: domain.ResourceName(collectionName + "/" + resourceID)}
	if sourceUIDStr.Valid {
		uid, err := domain.ParseExtensionResourceUID(sourceUIDStr.String)
		if err != nil {
			return existingAliasRow{}, fmt.Errorf("parse source_extension_resource_uid: %w", err)
		}
		row.sourceUID = uid
	}
	return row, nil
}

// writeAliases lazily ensures a (uid-less) platform_resources row for
// every distinct target among safe (see resource_identity_repo.go's
// virtual-resource doc), then blind-inserts every alias, then deletes
// each candidate's own stale row for the same (namespace, key) if its
// value just changed. resolveAliasesForWrite has already excluded
// anything that would violate either invariant, so the insert is
// unconditional INSERT OR IGNORE -- no read-modify-write -- and is
// idempotent against a repeat report of the very same (namespace,
// key, value) from the very same contributor by the UNIQUE (namespace,
// key, value, source_extension_resource_uid) constraint (see the
// migration's doc comment).
func (r *ExtensionResourceRepo) writeAliases(ctx context.Context, safe []aliasCandidateInput) error {
	if len(safe) == 0 {
		return nil
	}

	targets := make(map[domain.ResourceName]time.Time, len(safe))
	for _, c := range safe {
		targets[c.target] = c.receivedAt
	}
	platformPlaceholders := make([]string, 0, len(targets))
	platformArgs := make([]any, 0, len(targets)*4)
	for name, receivedAt := range targets {
		ts := receivedAt.UTC().Format(time.RFC3339Nano)
		platformPlaceholders = append(platformPlaceholders, "(?, ?, '{}', ?, ?)")
		platformArgs = append(platformArgs, string(name.Collection()), string(name.ID()), ts, ts)
	}
	if _, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO platform_resources (collection_name, resource_id, labels, created_at, updated_at)
			VALUES %s
			ON CONFLICT (collection_name, resource_id) DO NOTHING`, strings.Join(platformPlaceholders, ", ")),
		platformArgs...); err != nil {
		return fmt.Errorf("ensure platform resources for aliases: %w", err)
	}

	aliasPlaceholders := make([]string, len(safe))
	aliasArgs := make([]any, 0, len(safe)*7)
	for i, c := range safe {
		aliasPlaceholders[i] = "(?, ?, ?, ?, ?, ?, ?)"
		aliasArgs = append(aliasArgs,
			string(c.alias.Namespace), string(c.alias.Key), string(c.alias.Value),
			string(c.target.Collection()), string(c.target.ID()), c.sourceUID.String(), c.receivedAt.UTC().Format(time.RFC3339Nano))
	}
	if _, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO resource_aliases (namespace, key, value, platform_collection_name, platform_resource_id, source_extension_resource_uid, created_at)
			VALUES %s
			ON CONFLICT (namespace, key, value, source_extension_resource_uid) DO NOTHING`, strings.Join(aliasPlaceholders, ", ")),
		aliasArgs...); err != nil {
		return fmt.Errorf("upsert aliases: %w", err)
	}

	// A contributor legitimately replacing its own value for a key
	// leaves its old row behind unless explicitly cleaned up here --
	// the insert above is an unconditional add, not an upsert, since
	// (namespace, key, value, source_extension_resource_uid) as a
	// whole is what's unique, not (namespace, key,
	// source_extension_resource_uid) alone. Scoped by (namespace,
	// key, source_extension_resource_uid) so this never touches a
	// sibling contributor's row for the same claim, and NOT IN
	// the just-written (namespace, key, source_extension_resource_uid,
	// value) tuples so it never deletes the row(s) this same
	// statement just inserted -- same shape as batchReplaceLabels's
	// delete-absent/upsert pair above, just scoped by contributor
	// instead of by resource alone.
	scopePlaceholders := make([]string, len(safe))
	scopeArgs := make([]any, 0, len(safe)*3)
	keepPlaceholders := make([]string, len(safe))
	keepArgs := make([]any, 0, len(safe)*4)
	for i, c := range safe {
		scopePlaceholders[i] = "(?, ?, ?)"
		scopeArgs = append(scopeArgs, string(c.alias.Namespace), string(c.alias.Key), c.sourceUID.String())
		keepPlaceholders[i] = "(?, ?, ?, ?)"
		keepArgs = append(keepArgs, string(c.alias.Namespace), string(c.alias.Key), c.sourceUID.String(), string(c.alias.Value))
	}
	deleteArgs := append(append([]any{}, scopeArgs...), keepArgs...)
	if _, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM resource_aliases
			WHERE (namespace, key, source_extension_resource_uid) IN (%s)
			  AND (namespace, key, source_extension_resource_uid, value) NOT IN (%s)`,
			strings.Join(scopePlaceholders, ", "), strings.Join(keepPlaceholders, ", ")),
		deleteArgs...); err != nil {
		return fmt.Errorf("retract replaced aliases: %w", err)
	}
	return nil
}

// retractAbsentAliases implements ReplaceInventory's "absence
// retracts" half of the per-contributor replace contract
// [domain.InventoryReplacement.Aliases] documents -- see the Postgres
// sibling's aliasRetractAbsentCTE for the full reasoning, which
// applies here unchanged: reported carries every (uid, namespace,
// key) pair originally in this chunk's reports, before
// checkAliasBatchConsistency's Go-side filtering, so a key rejected
// as an intra-batch contradiction still counts as "reported" and
// isn't retracted out from under a good prior value. uids is every
// resource in the call (including ones with zero reported aliases,
// whose existing aliases -- if any -- are entirely retracted, per the
// same "absence means empty" rule labels/conditions already follow).
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
	if len(reported) == 0 {
		_, err := r.DB.ExecContext(ctx,
			fmt.Sprintf(`DELETE FROM resource_aliases WHERE source_extension_resource_uid IN (%s)`,
				strings.Join(uidPlaceholders, ", ")),
			uidArgs...)
		if err != nil {
			return fmt.Errorf("retract absent aliases (clear): %w", err)
		}
		return nil
	}
	keepPlaceholders := make([]string, len(reported))
	keepArgs := make([]any, 0, len(reported)*3)
	for i, k := range reported {
		keepPlaceholders[i] = "(?, ?, ?)"
		keepArgs = append(keepArgs, k.uid.String(), string(k.ns), string(k.key))
	}
	args := append(append([]any{}, uidArgs...), keepArgs...)
	_, err := r.DB.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM resource_aliases
			WHERE source_extension_resource_uid IN (%s)
			  AND (source_extension_resource_uid, namespace, key) NOT IN (%s)`,
			strings.Join(uidPlaceholders, ", "), strings.Join(keepPlaceholders, ", ")),
		args...)
	if err != nil {
		return fmt.Errorf("retract absent aliases: %w", err)
	}
	return nil
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
	uids, err := r.resolveOrCreateExtensionResources(ctx, resourceTypes, names, candidateUIDs, receivedAts)
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

	safeAliases, conflicts, err := r.resolveAliasesForWrite(ctx, aliasCandidates)
	if err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}
	if err := r.writeAliases(ctx, safeAliases); err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}
	if err := r.retractAbsentAliases(ctx, uids, reportedAliasKeys); err != nil {
		return nil, fmt.Errorf("replace inventory: %w", err)
	}
	return conflicts, nil
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
	uids, err := r.resolveOrCreateExtensionResources(ctx, resourceTypes, names, candidateUIDs, receivedAts)
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

	safeAliases, conflicts, err := r.resolveAliasesForWrite(ctx, aliasCandidates)
	if err != nil {
		return nil, fmt.Errorf("apply inventory deltas: %w", err)
	}
	if err := r.writeAliases(ctx, safeAliases); err != nil {
		return nil, fmt.Errorf("apply inventory deltas: %w", err)
	}
	return conflicts, nil
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
