package domain

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// ExtensionResourceUID is the opaque, stable identifier for an
// extension resource instance. Generated once at creation time and
// never changes. The underlying type is [uuid.UUID] so structural
// validity is encoded in the type system.
type ExtensionResourceUID uuid.UUID

// NewExtensionResourceUID generates a new random [ExtensionResourceUID].
func NewExtensionResourceUID() ExtensionResourceUID {
	return ExtensionResourceUID(uuid.New())
}

// ParseExtensionResourceUID parses a string into an [ExtensionResourceUID].
func ParseExtensionResourceUID(s string) (ExtensionResourceUID, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return ExtensionResourceUID{}, fmt.Errorf("extension resource uid: %w", err)
	}
	return ExtensionResourceUID(u), nil
}

// String returns the canonical UUID string representation.
func (u ExtensionResourceUID) String() string { return uuid.UUID(u).String() }

// MarshalText implements [encoding.TextMarshaler] for JSON string encoding.
func (u ExtensionResourceUID) MarshalText() ([]byte, error) { return uuid.UUID(u).MarshalText() }

// UnmarshalText implements [encoding.TextUnmarshaler] for JSON string decoding.
func (u *ExtensionResourceUID) UnmarshalText(data []byte) error {
	return (*uuid.UUID)(u).UnmarshalText(data)
}

// Value implements [driver.Valuer] for SQL persistence.
func (u ExtensionResourceUID) Value() (driver.Value, error) { return uuid.UUID(u).String(), nil }

// Scan implements [sql.Scanner] for SQL hydration.
func (u *ExtensionResourceUID) Scan(src any) error { return (*uuid.UUID)(u).Scan(src) }

// IsZero returns true when the UID is the zero (nil) UUID.
func (u ExtensionResourceUID) IsZero() bool { return uuid.UUID(u) == uuid.Nil }

// ---------------------------------------------------------------------------
// ManagementType -- management metadata value object
// ---------------------------------------------------------------------------

// ManagementType holds management-specific metadata for an extension
// resource type. When present on an [ExtensionResourceType], it
// indicates that instances of the type are managed resources with
// fulfillment relations and addon attestation.
type ManagementType struct {
	relation  FulfillmentRelation
	signature Signature
}

// NewManagementType constructs a [ManagementType]. The relation must
// be non-nil; the signature attests the addon's authority for the
// relation.
func NewManagementType(relation FulfillmentRelation, sig Signature) (ManagementType, error) {
	if relation == nil {
		return ManagementType{}, fmt.Errorf("management type: %w: relation is required", ErrInvalidArgument)
	}
	return ManagementType{relation: relation, signature: sig}, nil
}

// Relation returns the fulfillment relation.
func (m ManagementType) Relation() FulfillmentRelation { return m.relation }

// Signature returns the addon's cryptographic signature over the relation.
func (m ManagementType) Signature() Signature { return m.signature }

// MarshalJSON implements json.Marshaler for ManagementType. Uses the
// discriminated union serialization for [FulfillmentRelation].
func (m ManagementType) MarshalJSON() ([]byte, error) {
	rel, err := marshalFulfillmentRelation(m.relation)
	if err != nil {
		return nil, err
	}
	return json.Marshal(managementTypeJSON{Relation: rel, Signature: m.signature})
}

// UnmarshalJSON implements json.Unmarshaler for ManagementType.
func (m *ManagementType) UnmarshalJSON(data []byte) error {
	var j managementTypeJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	rel, err := unmarshalFulfillmentRelation(j.Relation)
	if err != nil {
		return err
	}
	m.relation = rel
	m.signature = j.Signature
	return nil
}

type managementTypeJSON struct {
	Relation  fulfillmentRelJSON `json:"Relation"`
	Signature Signature          `json:"Signature"`
}

// ---------------------------------------------------------------------------
// ConditionType, ConditionStatus, Condition -- inventory condition value objects
// ---------------------------------------------------------------------------

// ConditionType identifies a category of condition (e.g. "Ready",
// "Provisioned"). Non-empty, free-form string value object.
type ConditionType string

// NewConditionType validates and returns a [ConditionType]. Rejects
// empty values.
func NewConditionType(s string) (ConditionType, error) {
	if s == "" {
		return "", fmt.Errorf("condition type: %w: must not be empty", ErrInvalidArgument)
	}
	return ConditionType(s), nil
}

// ConditionStatus represents the status of a condition. Uses the
// Kubernetes-standard True/False/Unknown trichotomy.
type ConditionStatus string

const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// validConditionStatuses is the closed set accepted by [NewCondition].
var validConditionStatuses = map[ConditionStatus]struct{}{
	ConditionTrue:    {},
	ConditionFalse:   {},
	ConditionUnknown: {},
}

// Condition represents an observed condition on an inventory resource,
// following the Kubernetes conditions convention (type, status, reason,
// message, lastTransitionTime).
type Condition struct {
	conditionType      ConditionType
	status             ConditionStatus
	reason             string
	message            string
	lastTransitionTime time.Time
}

// NewCondition constructs a [Condition], validating that the status
// is one of True/False/Unknown. Reason and message are informational
// and may be empty.
func NewCondition(ct ConditionType, status ConditionStatus, reason, message string, transitionTime time.Time) (Condition, error) {
	if _, ok := validConditionStatuses[status]; !ok {
		return Condition{}, fmt.Errorf("condition status %q: %w: must be True, False, or Unknown", status, ErrInvalidArgument)
	}
	return Condition{
		conditionType:      ct,
		status:             status,
		reason:             reason,
		message:            message,
		lastTransitionTime: transitionTime,
	}, nil
}

func (c Condition) Type() ConditionType           { return c.conditionType }
func (c Condition) Status() ConditionStatus       { return c.status }
func (c Condition) Reason() string                { return c.reason }
func (c Condition) Message() string               { return c.message }
func (c Condition) LastTransitionTime() time.Time { return c.lastTransitionTime }

// ---------------------------------------------------------------------------
// InventoryType -- type-level inventory metadata
// ---------------------------------------------------------------------------

// InventoryType is a capability marker for an extension resource type.
// When present on an [ExtensionResourceType], it indicates that
// instances of the type support inventory reporting.
type InventoryType struct{}

// NewInventoryType constructs an [InventoryType].
func NewInventoryType() InventoryType { return InventoryType{} }

// ---------------------------------------------------------------------------
// InventoryResource -- instance-level inventory state
// ---------------------------------------------------------------------------

// InventoryResource holds the latest inventory state for an extension
// resource instance. Reconstituted from persistence via snapshot.
type InventoryResource struct {
	labels      map[string]string
	observation json.RawMessage
	conditions  []Condition
	observedAt  time.Time
	updatedAt   time.Time
}

func (ir *InventoryResource) Labels() map[string]string    { return ir.labels }
func (ir *InventoryResource) Observation() json.RawMessage { return ir.observation }
func (ir *InventoryResource) Conditions() []Condition      { return ir.conditions }
func (ir *InventoryResource) ObservedAt() time.Time        { return ir.observedAt }
func (ir *InventoryResource) UpdatedAt() time.Time         { return ir.updatedAt }

// ---------------------------------------------------------------------------
// Observation -- inventory observation history record
// ---------------------------------------------------------------------------

// ObservationID uniquely identifies an observation history record.
type ObservationID string

// Observation is a single observation history record for an extension
// resource instance. It captures the raw observation payload and the
// time it was observed. Observations are append-only; once persisted
// they are never modified.
type Observation struct {
	id                   ObservationID
	extensionResourceUID ExtensionResourceUID
	observation          json.RawMessage
	observedAt           time.Time
	createdAt            time.Time
}

// NewObservation constructs an [Observation].
func NewObservation(
	id ObservationID,
	erUID ExtensionResourceUID,
	observation json.RawMessage,
	observedAt time.Time,
	createdAt time.Time,
) Observation {
	return Observation{
		id:                   id,
		extensionResourceUID: erUID,
		observation:          observation,
		observedAt:           observedAt,
		createdAt:            createdAt,
	}
}

func (o Observation) ID() ObservationID                          { return o.id }
func (o Observation) ExtensionResourceUID() ExtensionResourceUID { return o.extensionResourceUID }
func (o Observation) Observation() json.RawMessage               { return o.observation }
func (o Observation) ObservedAt() time.Time                      { return o.observedAt }
func (o Observation) CreatedAt() time.Time                       { return o.createdAt }

// ---------------------------------------------------------------------------
// ConditionReport -- observed condition state submitted by reporters
// ---------------------------------------------------------------------------

// ConditionReport is the observed state of a single condition on an
// extension resource. Reporters submit reports without knowing whether
// they represent a genuine transition; that determination is made by
// the repository when it records the condition (see
// [ExtensionResourceRepository.RecordConditions]).
type ConditionReport struct {
	extensionResourceUID ExtensionResourceUID
	conditionType        ConditionType
	status               ConditionStatus
	reason               string
	message              string
	lastTransitionTime   time.Time
	observedAt           time.Time
}

// NewConditionReport constructs a [ConditionReport].
func NewConditionReport(
	erUID ExtensionResourceUID,
	conditionType ConditionType,
	status ConditionStatus,
	reason, message string,
	lastTransitionTime time.Time,
	observedAt time.Time,
) ConditionReport {
	return ConditionReport{
		extensionResourceUID: erUID,
		conditionType:        conditionType,
		status:               status,
		reason:               reason,
		message:              message,
		lastTransitionTime:   lastTransitionTime,
		observedAt:           observedAt,
	}
}

func (r ConditionReport) ExtensionResourceUID() ExtensionResourceUID { return r.extensionResourceUID }
func (r ConditionReport) ConditionType() ConditionType               { return r.conditionType }
func (r ConditionReport) Status() ConditionStatus                    { return r.status }
func (r ConditionReport) Reason() string                             { return r.reason }
func (r ConditionReport) Message() string                            { return r.message }
func (r ConditionReport) LastTransitionTime() time.Time              { return r.lastTransitionTime }
func (r ConditionReport) ObservedAt() time.Time                      { return r.observedAt }

// ---------------------------------------------------------------------------
// ConditionTransition -- realized condition state change
// ---------------------------------------------------------------------------

// ConditionTransitionID uniquely identifies a recorded condition
// transition. Generated by the repository when a [ConditionReport]
// survives the deduplication constraint.
type ConditionTransitionID string

// ConditionTransition is a persisted condition state change. It is
// produced by the repository when a [ConditionReport] represents a
// genuine transition (the (status, reason, message) tuple differs from
// the latest entry for the same (resource, condition type) pair).
// Callers never construct transitions directly; they are returned by
// [ExtensionResourceRepository.ListConditionTransitions].
type ConditionTransition struct {
	id                   ConditionTransitionID
	extensionResourceUID ExtensionResourceUID
	conditionType        ConditionType
	status               ConditionStatus
	reason               string
	message              string
	lastTransitionTime   time.Time
	observedAt           time.Time
	createdAt            time.Time
}

func (t ConditionTransition) ID() ConditionTransitionID { return t.id }
func (t ConditionTransition) ExtensionResourceUID() ExtensionResourceUID {
	return t.extensionResourceUID
}
func (t ConditionTransition) ConditionType() ConditionType  { return t.conditionType }
func (t ConditionTransition) Status() ConditionStatus       { return t.status }
func (t ConditionTransition) Reason() string                { return t.reason }
func (t ConditionTransition) Message() string               { return t.message }
func (t ConditionTransition) LastTransitionTime() time.Time { return t.lastTransitionTime }
func (t ConditionTransition) ObservedAt() time.Time         { return t.observedAt }
func (t ConditionTransition) CreatedAt() time.Time          { return t.createdAt }

// ---------------------------------------------------------------------------
// ExtensionResourceType -- type definition for extension resources
// ---------------------------------------------------------------------------

// ExtensionResourceType is the type definition that describes an
// extension resource kind. It carries API identity fields (service
// name, version, collection) and optional capability metadata for
// management (fulfillment relation and attestation signature) and/or
// inventory (observation schema).
//
// Unlike the former ManagedResourceTypeDef which used public fields,
// this type uses private fields with accessors per domain.md
// conventions.
//
// Both management and inventory are modeled as optional pointers so a
// type can be managed-only, inventory-only, or both.
type ExtensionResourceType struct {
	resourceType ResourceType
	apiVersion   APIVersion
	collectionID CollectionID
	management   *ManagementType
	inventory    *InventoryType
	createdAt    time.Time
	updatedAt    time.Time
}

// ExtensionResourceTypeOption configures optional fields on
// [ExtensionResourceType].
type ExtensionResourceTypeOption func(*ExtensionResourceType)

// WithManagement sets management metadata on an extension resource
// type. The relation and signature describe the fulfillment behavior
// and the addon's proof of ownership.
func WithManagement(relation FulfillmentRelation, sig Signature) ExtensionResourceTypeOption {
	return func(t *ExtensionResourceType) {
		t.management = &ManagementType{relation: relation, signature: sig}
	}
}

// WithInventory marks an extension resource type as supporting
// inventory reporting.
func WithInventory() ExtensionResourceTypeOption {
	return func(t *ExtensionResourceType) {
		it := NewInventoryType()
		t.inventory = &it
	}
}

// NewExtensionResourceType constructs an [ExtensionResourceType] with
// the given API identity fields. The API service name is derived from
// the [ResourceType]'s service component per AIP-123. Use
// [WithManagement] to attach management metadata.
func NewExtensionResourceType(
	rt ResourceType,
	version APIVersion,
	collectionID CollectionID,
	now time.Time,
	opts ...ExtensionResourceTypeOption,
) ExtensionResourceType {
	t := ExtensionResourceType{
		resourceType: rt,
		apiVersion:   version,
		collectionID: collectionID,
		createdAt:    now,
		updatedAt:    now,
	}
	for _, opt := range opts {
		opt(&t)
	}
	return t
}

// Accessor methods.

// ResourceType returns the extension resource type identifier.
func (t ExtensionResourceType) ResourceType() ResourceType { return t.resourceType }

// APIServiceName returns the API service name derived from the
// [ResourceType]'s service component (e.g. "kind.fleetshift.io").
func (t ExtensionResourceType) APIServiceName() ServiceName { return t.resourceType.ServiceName() }

// APIVersion returns the API version (e.g. "v1").
func (t ExtensionResourceType) APIVersion() APIVersion { return t.apiVersion }

// CollectionID returns the collection identifier (e.g. "clusters").
func (t ExtensionResourceType) CollectionID() CollectionID { return t.collectionID }

// Management returns the management metadata, or nil for
// inventory-only types.
func (t ExtensionResourceType) Management() *ManagementType { return t.management }

// Inventory returns the inventory metadata, or nil for types that do
// not support inventory reporting.
func (t ExtensionResourceType) Inventory() *InventoryType { return t.inventory }

// CreatedAt returns the creation timestamp.
func (t ExtensionResourceType) CreatedAt() time.Time { return t.createdAt }

// UpdatedAt returns the last-updated timestamp.
func (t ExtensionResourceType) UpdatedAt() time.Time { return t.updatedAt }

// MarshalJSON implements json.Marshaler for ExtensionResourceType.
// Uses custom serialization to handle the [FulfillmentRelation]
// interface within management metadata.
func (t ExtensionResourceType) MarshalJSON() ([]byte, error) {
	var mgmt *managementTypeJSON
	if t.management != nil {
		rel, err := marshalFulfillmentRelation(t.management.relation)
		if err != nil {
			return nil, err
		}
		mgmt = &managementTypeJSON{Relation: rel, Signature: t.management.signature}
	}
	return json.Marshal(extensionResourceTypeJSON{
		ResourceType: t.resourceType,
		APIVersion:   t.apiVersion,
		CollectionID: t.collectionID,
		Management:   mgmt,
		Inventory:    t.inventory,
		CreatedAt:    t.createdAt,
		UpdatedAt:    t.updatedAt,
	})
}

// UnmarshalJSON implements json.Unmarshaler for ExtensionResourceType.
func (t *ExtensionResourceType) UnmarshalJSON(data []byte) error {
	var j extensionResourceTypeJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return err
	}
	t.resourceType = j.ResourceType
	t.apiVersion = j.APIVersion
	t.collectionID = j.CollectionID
	t.inventory = j.Inventory
	t.createdAt = j.CreatedAt
	t.updatedAt = j.UpdatedAt
	if j.Management != nil {
		rel, err := unmarshalFulfillmentRelation(j.Management.Relation)
		if err != nil {
			return err
		}
		t.management = &ManagementType{relation: rel, signature: j.Management.Signature}
	}
	return nil
}

type extensionResourceTypeJSON struct {
	ResourceType ResourceType        `json:"ResourceType"`
	APIVersion   APIVersion          `json:"APIVersion"`
	CollectionID CollectionID        `json:"CollectionID"`
	Management   *managementTypeJSON `json:"Management,omitempty"`
	Inventory    *InventoryType      `json:"Inventory,omitempty"`
	CreatedAt    time.Time           `json:"CreatedAt"`
	UpdatedAt    time.Time           `json:"UpdatedAt"`
}

// ---------------------------------------------------------------------------
// ManagedState -- managed lifecycle state on an extension resource
// ---------------------------------------------------------------------------

// ManagedState holds the managed lifecycle state for an extension
// resource instance. It tracks the current intent version and the
// linked fulfillment.
//
// This is not a separate aggregate; it is state on [ExtensionResource].
// Root timestamps (createdAt, updatedAt) remain on the parent resource.
type ManagedState struct {
	currentVersion IntentVersion
	fulfillmentID  FulfillmentID
}

// CurrentVersion returns the current intent version.
func (s ManagedState) CurrentVersion() IntentVersion { return s.currentVersion }

// FulfillmentID returns the linked fulfillment's identifier.
func (s ManagedState) FulfillmentID() FulfillmentID { return s.fulfillmentID }

// ---------------------------------------------------------------------------
// ExtensionResource -- the primary extension-owned aggregate
// ---------------------------------------------------------------------------

// ExtensionResource is the primary extension-owned resource instance.
// It is the single extension-side aggregate for a fully qualified
// extension resource name such as //kind.fleetshift.io/clusters/dev.
//
// A resource may have managed state (fulfillment lifecycle),
// inventory state (observed conditions and observations), or both.
//
// Construct new instances with [NewExtensionResource]; reconstitute
// from persistence with [ExtensionResourceFromSnapshot]. Intent
// recording goes through [ExtensionResource.RecordIntent].
type ExtensionResource struct {
	uid          ExtensionResourceUID
	resourceType ResourceType
	name         ResourceName
	labels       map[string]string

	managed   *ManagedState
	inventory *InventoryResource

	createdAt time.Time
	updatedAt time.Time

	pendingIntents []ResourceIntent
}

// ExtensionResourceOption configures optional fields on
// [ExtensionResource].
type ExtensionResourceOption func(*ExtensionResource)

// WithExtensionLabels sets the labels on a new extension resource.
func WithExtensionLabels(labels map[string]string) ExtensionResourceOption {
	return func(r *ExtensionResource) {
		if labels != nil {
			r.labels = labels
		}
	}
}

// WithManagedState attaches managed lifecycle state to the extension
// resource. The fulfillmentID links the resource to its fulfillment.
// currentVersion starts at 0.
func WithManagedState(fulfillmentID FulfillmentID) ExtensionResourceOption {
	return func(r *ExtensionResource) {
		r.managed = &ManagedState{fulfillmentID: fulfillmentID}
	}
}

// NewExtensionResource creates a brand-new [ExtensionResource]. Use
// this on creation paths; use [ExtensionResourceFromSnapshot] only for
// reconstituting from persistence.
//
// After construction, call [ExtensionResource.RecordIntent] (if
// managed) to attach the initial spec version.
func NewExtensionResource(
	uid ExtensionResourceUID,
	rt ResourceType,
	name ResourceName,
	now time.Time,
	opts ...ExtensionResourceOption,
) *ExtensionResource {
	r := &ExtensionResource{
		uid:          uid,
		resourceType: rt,
		name:         name,
		labels:       map[string]string{},
		createdAt:    now,
		updatedAt:    now,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// RecordIntent advances the intent version within the managed state
// and collects a pending [ResourceIntent] record for the repository
// to flush. Returns the recorded intent for use in downstream
// derivation (e.g. [FulfillmentRelation.DeriveStrategies]).
//
// Returns an error if the resource has no managed state because intent
// versioning requires a managed lifecycle.
func (r *ExtensionResource) RecordIntent(spec json.RawMessage, now time.Time) (ResourceIntent, error) {
	if r.managed == nil {
		return ResourceIntent{}, fmt.Errorf(
			"extension resource %s: %w: cannot record intent without managed state",
			r.name, ErrInvalidArgument)
	}
	r.managed.currentVersion++
	intent := ResourceIntent{
		ExtensionResourceUID: r.uid,
		Version:              r.managed.currentVersion,
		Spec:                 spec,
		CreatedAt:            now,
	}
	r.pendingIntents = append(r.pendingIntents, intent)
	return intent, nil
}

// Accessor methods -- read-only getters for private fields.

// UID returns the resource's stable unique identifier.
func (r *ExtensionResource) UID() ExtensionResourceUID { return r.uid }

// ResourceType returns the extension resource type.
func (r *ExtensionResource) ResourceType() ResourceType { return r.resourceType }

// Name returns the resource instance name.
func (r *ExtensionResource) Name() ResourceName { return r.name }

// ServiceName returns the service name derived from the resource type.
func (r *ExtensionResource) ServiceName() ServiceName { return r.resourceType.ServiceName() }

// FullResourceName returns the globally unique name of the form
// "//{service}/{name}".
func (r *ExtensionResource) FullResourceName() FullResourceName {
	return r.name.FullName(r.ServiceName())
}

// Labels returns the user-defined extension resource labels.
func (r *ExtensionResource) Labels() map[string]string { return r.labels }

// Managed returns the managed lifecycle state, or nil for
// inventory-only resources.
func (r *ExtensionResource) Managed() *ManagedState { return r.managed }

// Inventory returns the latest inventory state, or nil if no inventory
// has been reported.
func (r *ExtensionResource) Inventory() *InventoryResource { return r.inventory }

// CreatedAt returns the creation timestamp.
func (r *ExtensionResource) CreatedAt() time.Time { return r.createdAt }

// UpdatedAt returns the last-updated timestamp.
func (r *ExtensionResource) UpdatedAt() time.Time { return r.updatedAt }

// ---------------------------------------------------------------------------
// ExtensionResourceView -- read DTO
// ---------------------------------------------------------------------------

// ExtensionResourceView is the read model that joins an
// [ExtensionResource] with its current [ResourceIntent] and
// [Fulfillment] when the resource is managed. Constructed by the
// repository via joins; never written directly.
//
// Intent and Fulfillment are populated when the resource has managed
// state and are nil for non-managed resources. Inventory is populated
// from the resource's embedded inventory state.
type ExtensionResourceView struct {
	Resource    ExtensionResource
	Intent      *ResourceIntent
	Fulfillment *Fulfillment
	Inventory   *InventoryResource
}
